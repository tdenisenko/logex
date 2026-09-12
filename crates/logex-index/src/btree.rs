use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use roaring::RoaringBitmap;

/// Magic bytes identifying a LogEx B+ tree index file.
const INDEX_MAGIC: &[u8; 4] = b"LXIX";

/// Current index file format version.
const INDEX_VERSION: u32 = 2;
const INDEX_HEADER_LEN: usize = 20;
const INDEX_V2_ENTRY_TRAILER_LEN: usize = 8 + 4;

/// An in-memory B+ tree index mapping fixed-size byte keys to roaring bitmaps
/// of row IDs. Used during index construction and for the hot partition.
///
/// Version 1 on disk format:
///   [magic: 4B] [version: 4B] [key_size: 4B] [entry_count: u64]
///   For each entry:
///     [key: key_size bytes] [bitmap_len: u32] [bitmap: serialized RoaringBitmap]
///
/// Version 2 on disk format:
///   [magic: 4B] [version: 4B] [key_size: 4B] [entry_count: u64]
///   For each entry:
///     [key: key_size bytes] [bitmap_offset: u64] [bitmap_len: u32]
///   Then the serialized bitmap payloads. This keeps exact point lookups
///   logarithmic without loading or scanning the whole file.
///
/// Entries are sorted by key for binary search on read.
#[derive(Debug)]
pub struct BTreeIndex {
    key_size: usize,
    entries: BTreeMap<Vec<u8>, RoaringBitmap>,
}

impl BTreeIndex {
    pub fn new(key_size: usize) -> Self {
        Self {
            key_size,
            entries: BTreeMap::new(),
        }
    }

    /// Insert a row ID for the given key.
    pub fn insert(&mut self, key: &[u8], row_id: u32) {
        debug_assert_eq!(key.len(), self.key_size);
        self.entries.entry(key.to_vec()).or_default().insert(row_id);
    }

    /// Look up all row IDs for a given key.
    pub fn get(&self, key: &[u8]) -> Option<&RoaringBitmap> {
        self.entries.get(key)
    }

    /// Range scan: returns all bitmaps for keys in [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> RoaringBitmap {
        if start >= end {
            return RoaringBitmap::new();
        }
        let mut result = RoaringBitmap::new();
        for (_key, bitmap) in self.entries.range(start.to_vec()..end.to_vec()) {
            result |= bitmap;
        }
        result
    }

    /// Number of unique keys.
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// Write the index to a file.
    pub fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let file = File::create(path)?;
        let mut w = BufWriter::new(file);

        w.write_all(INDEX_MAGIC)?;
        w.write_all(&INDEX_VERSION.to_le_bytes())?;
        w.write_all(&(self.key_size as u32).to_le_bytes())?;
        w.write_all(&(self.entries.len() as u64).to_le_bytes())?;

        let table_len = self
            .entries
            .len()
            .saturating_mul(self.key_size + INDEX_V2_ENTRY_TRAILER_LEN);
        let mut bitmap_offset = (INDEX_HEADER_LEN + table_len) as u64;
        let mut payloads = Vec::with_capacity(self.entries.len());

        for (key, bitmap) in &self.entries {
            w.write_all(key)?;
            let mut bitmap_buf = Vec::new();
            bitmap.serialize_into(&mut bitmap_buf)?;
            w.write_all(&bitmap_offset.to_le_bytes())?;
            w.write_all(&(bitmap_buf.len() as u32).to_le_bytes())?;
            bitmap_offset = bitmap_offset.saturating_add(bitmap_buf.len() as u64);
            payloads.push(bitmap_buf);
        }

        for bitmap_buf in payloads {
            w.write_all(&bitmap_buf)?;
        }

        w.flush()?;
        Ok(())
    }
}

/// Read-only index loaded from disk.
pub struct BTreeIndexReader {
    key_size: usize,
    /// Sorted array of (key, bitmap) pairs for binary search.
    entries: Vec<(Vec<u8>, RoaringBitmap)>,
}

impl BTreeIndexReader {
    /// Load an index from a file.
    pub fn open(path: &Path) -> io::Result<Self> {
        let data = fs::read(path)?;
        if data.len() < 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index file too short",
            ));
        }

        if &data[0..4] != INDEX_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid index magic",
            ));
        }

        let parse_err = |msg| io::Error::new(io::ErrorKind::InvalidData, msg);
        let version = u32::from_le_bytes(
            data[4..8]
                .try_into()
                .map_err(|_| parse_err("bad version"))?,
        );
        let key_size = u32::from_le_bytes(
            data[8..12]
                .try_into()
                .map_err(|_| parse_err("bad key_size"))?,
        ) as usize;
        let entry_count = u64::from_le_bytes(
            data[12..20]
                .try_into()
                .map_err(|_| parse_err("bad entry_count"))?,
        ) as usize;

        match version {
            2 => Self::open_v2(data, key_size, entry_count),
            _ => Self::open_v1(data, key_size, entry_count),
        }
    }

    fn open_v1(data: Vec<u8>, key_size: usize, entry_count: usize) -> io::Result<Self> {
        let mut reader = BufReader::new(&data[INDEX_HEADER_LEN..]);
        let mut entries = Vec::with_capacity(entry_count);

        for _ in 0..entry_count {
            let mut key = vec![0u8; key_size];
            reader.read_exact(&mut key)?;

            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let bitmap_len = u32::from_le_bytes(len_buf) as usize;

            let mut bitmap_buf = vec![0u8; bitmap_len];
            reader.read_exact(&mut bitmap_buf)?;
            let bitmap = RoaringBitmap::deserialize_from(&bitmap_buf[..])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

            entries.push((key, bitmap));
        }

        Ok(Self { key_size, entries })
    }

    fn open_v2(data: Vec<u8>, key_size: usize, entry_count: usize) -> io::Result<Self> {
        let entry_size = key_size + INDEX_V2_ENTRY_TRAILER_LEN;
        let table_len = entry_count
            .checked_mul(entry_size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "index table too large"))?;
        let table_end = INDEX_HEADER_LEN
            .checked_add(table_len)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "index table too large"))?;
        if data.len() < table_end {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index table truncated",
            ));
        }

        let mut entries = Vec::with_capacity(entry_count);
        for entry_index in 0..entry_count {
            let entry_start = INDEX_HEADER_LEN + entry_index * entry_size;
            let key = data[entry_start..entry_start + key_size].to_vec();
            let offset_start = entry_start + key_size;
            let bitmap_offset = u64::from_le_bytes(
                data[offset_start..offset_start + 8]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad bitmap offset"))?,
            ) as usize;
            let len_start = offset_start + 8;
            let bitmap_len = u32::from_le_bytes(
                data[len_start..len_start + 4]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad bitmap length"))?,
            ) as usize;
            let bitmap_end = bitmap_offset.checked_add(bitmap_len).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "bitmap payload too large")
            })?;
            if bitmap_end > data.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bitmap payload truncated",
                ));
            }
            let bitmap = RoaringBitmap::deserialize_from(&data[bitmap_offset..bitmap_end])
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entries.push((key, bitmap));
        }

        Ok(Self { key_size, entries })
    }

    /// Point lookup without loading the full index into memory.
    ///
    /// Version 2 indexes use a binary search over the fixed-width key table.
    /// Version 1 indexes fall back to a linear key scan while skipping bitmap
    /// payloads for earlier keys without deserializing them.
    pub fn get_from_file(path: &Path, key: &[u8]) -> io::Result<Option<RoaringBitmap>> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != INDEX_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid index magic",
            ));
        }

        let mut version_buf = [0u8; 4];
        reader.read_exact(&mut version_buf)?;
        let version = u32::from_le_bytes(version_buf);

        let mut key_size_buf = [0u8; 4];
        reader.read_exact(&mut key_size_buf)?;
        let key_size = u32::from_le_bytes(key_size_buf) as usize;
        if key.len() != key_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("lookup key has length {}, expected {key_size}", key.len()),
            ));
        }

        let mut entry_count_buf = [0u8; 8];
        reader.read_exact(&mut entry_count_buf)?;
        let entry_count = u64::from_le_bytes(entry_count_buf);

        if version == 2 {
            return Self::get_v2_from_reader(reader, key_size, entry_count, key);
        }

        let mut current_key = vec![0u8; key_size];
        for _ in 0..entry_count {
            reader.read_exact(&mut current_key)?;

            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let bitmap_len = u32::from_le_bytes(len_buf) as usize;

            match current_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => {
                    reader.seek(SeekFrom::Current(bitmap_len as i64))?;
                }
                std::cmp::Ordering::Equal => {
                    let mut bitmap_buf = vec![0u8; bitmap_len];
                    reader.read_exact(&mut bitmap_buf)?;
                    let bitmap = RoaringBitmap::deserialize_from(&bitmap_buf[..])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    return Ok(Some(bitmap));
                }
                std::cmp::Ordering::Greater => return Ok(None),
            }
        }

        Ok(None)
    }

    fn get_v2_from_reader(
        mut reader: BufReader<File>,
        key_size: usize,
        entry_count: u64,
        key: &[u8],
    ) -> io::Result<Option<RoaringBitmap>> {
        let entry_size = key_size + INDEX_V2_ENTRY_TRAILER_LEN;
        let mut lo = 0u64;
        let mut hi = entry_count;
        let mut current_key = vec![0u8; key_size];

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry_offset = INDEX_HEADER_LEN as u64 + mid * entry_size as u64;
            reader.seek(SeekFrom::Start(entry_offset))?;
            reader.read_exact(&mut current_key)?;

            match current_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let mut offset_buf = [0u8; 8];
                    reader.read_exact(&mut offset_buf)?;
                    let bitmap_offset = u64::from_le_bytes(offset_buf);
                    let mut len_buf = [0u8; 4];
                    reader.read_exact(&mut len_buf)?;
                    let bitmap_len = u32::from_le_bytes(len_buf) as usize;
                    reader.seek(SeekFrom::Start(bitmap_offset))?;
                    let mut bitmap_buf = vec![0u8; bitmap_len];
                    reader.read_exact(&mut bitmap_buf)?;
                    let bitmap = RoaringBitmap::deserialize_from(&bitmap_buf[..])
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    return Ok(Some(bitmap));
                }
            }
        }

        Ok(None)
    }

    /// Point lookup: find row IDs for an exact key.
    pub fn get(&self, key: &[u8]) -> Option<&RoaringBitmap> {
        self.entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
            .map(|idx| &self.entries[idx].1)
    }

    /// Range scan: return union of all bitmaps for keys in [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> RoaringBitmap {
        if start >= end {
            return RoaringBitmap::new();
        }
        // Find the first key >= start
        let lo = self.entries.partition_point(|(k, _)| k.as_slice() < start);
        let hi = self.entries.partition_point(|(k, _)| k.as_slice() < end);

        let mut result = RoaringBitmap::new();
        for (_, bitmap) in &self.entries[lo..hi] {
            result |= bitmap;
        }
        result
    }

    /// Range scan including both endpoints. The upper bound need not have a
    /// representable successor, including an all-0xff numeric/composite key.
    pub fn range_inclusive(&self, start: &[u8], end: &[u8]) -> RoaringBitmap {
        if start > end {
            return RoaringBitmap::new();
        }
        let lo = self.entries.partition_point(|(k, _)| k.as_slice() < start);
        let hi = self.entries.partition_point(|(k, _)| k.as_slice() <= end);
        let mut result = RoaringBitmap::new();
        for (_, bitmap) in &self.entries[lo..hi] {
            result |= bitmap;
        }
        result
    }

    /// Number of unique keys.
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// Key size in bytes.
    pub fn key_size(&self) -> usize {
        self.key_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn index_ranges_match_independent_oracle_including_reversed_bounds() {
        let mut values = vec![0, 1, 2, u64::MAX - 1, u64::MAX];
        let mut seed = 0xa76d_221f_d830_c941_u64;
        for _ in 0..128 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            values.push(seed);
        }
        let mut index = BTreeIndex::new(8);
        for (row, value) in values.iter().enumerate() {
            index.insert(&value.to_be_bytes(), row as u32);
        }
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("ranges.bptree");
        index.write_to_file(&path).unwrap();
        let reader = BTreeIndexReader::open(&path).unwrap();
        for (i, &start) in values.iter().enumerate() {
            for end in [start, 0, u64::MAX, values[(i + 17) % values.len()]] {
                let exclusive: RoaringBitmap = values
                    .iter()
                    .enumerate()
                    .filter(|(_, value)| start <= **value && **value < end)
                    .map(|(row, _)| row as u32)
                    .collect();
                let inclusive: RoaringBitmap = values
                    .iter()
                    .enumerate()
                    .filter(|(_, value)| start <= **value && **value <= end)
                    .map(|(row, _)| row as u32)
                    .collect();
                assert_eq!(
                    index.range(&start.to_be_bytes(), &end.to_be_bytes()),
                    exclusive
                );
                assert_eq!(
                    reader.range(&start.to_be_bytes(), &end.to_be_bytes()),
                    exclusive
                );
                assert_eq!(
                    reader.range_inclusive(&start.to_be_bytes(), &end.to_be_bytes()),
                    inclusive
                );
            }
        }
    }

    #[test]
    fn test_btree_index_insert_and_get() {
        let mut idx = BTreeIndex::new(4);
        idx.insert(&[0, 0, 0, 1], 10);
        idx.insert(&[0, 0, 0, 1], 20);
        idx.insert(&[0, 0, 0, 2], 30);

        let bitmap = idx.get(&[0, 0, 0, 1]).unwrap();
        assert!(bitmap.contains(10));
        assert!(bitmap.contains(20));
        assert!(!bitmap.contains(30));

        let bitmap2 = idx.get(&[0, 0, 0, 2]).unwrap();
        assert!(bitmap2.contains(30));
        assert_eq!(bitmap2.len(), 1);

        assert!(idx.get(&[0, 0, 0, 3]).is_none());
    }

    #[test]
    fn test_btree_index_range() {
        let mut idx = BTreeIndex::new(4);
        // Keys: 1, 2, 3, 4, 5
        for i in 1u32..=5 {
            idx.insert(&i.to_be_bytes(), i * 10);
        }

        // Range [2, 4) should include keys 2 and 3
        let result = idx.range(&2u32.to_be_bytes(), &4u32.to_be_bytes());
        assert!(result.contains(20));
        assert!(result.contains(30));
        assert!(!result.contains(10));
        assert!(!result.contains(40));
    }

    #[test]
    fn test_btree_index_file_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bptree");

        let mut idx = BTreeIndex::new(20); // address-sized keys
        let addr1 = [1u8; 20];
        let addr2 = [2u8; 20];
        idx.insert(&addr1, 0);
        idx.insert(&addr1, 5);
        idx.insert(&addr1, 100);
        idx.insert(&addr2, 50);
        idx.insert(&addr2, 75);

        idx.write_to_file(&path).unwrap();

        let reader = BTreeIndexReader::open(&path).unwrap();
        assert_eq!(reader.key_count(), 2);
        assert_eq!(reader.key_size(), 20);

        let bitmap1 = reader.get(&addr1).unwrap();
        assert_eq!(bitmap1.len(), 3);
        assert!(bitmap1.contains(0));
        assert!(bitmap1.contains(5));
        assert!(bitmap1.contains(100));

        let bitmap2 = reader.get(&addr2).unwrap();
        assert_eq!(bitmap2.len(), 2);
        assert!(bitmap2.contains(50));
        assert!(bitmap2.contains(75));

        assert!(reader.get(&[3u8; 20]).is_none());
    }

    #[test]
    fn test_btree_reader_point_lookup_from_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bptree");

        let mut idx = BTreeIndex::new(4);
        idx.insert(&1u32.to_be_bytes(), 10);
        idx.insert(&3u32.to_be_bytes(), 30);
        idx.insert(&3u32.to_be_bytes(), 31);
        idx.write_to_file(&path).unwrap();

        let bitmap = BTreeIndexReader::get_from_file(&path, &3u32.to_be_bytes())
            .unwrap()
            .unwrap();
        assert!(bitmap.contains(30));
        assert!(bitmap.contains(31));

        let missing = BTreeIndexReader::get_from_file(&path, &2u32.to_be_bytes()).unwrap();
        assert!(missing.is_none());
    }

    #[test]
    fn test_btree_reader_range() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bptree");

        let mut idx = BTreeIndex::new(8); // u64 keys
        for block in [100u64, 200, 300, 400, 500] {
            idx.insert(&block.to_be_bytes(), block as u32);
        }
        idx.write_to_file(&path).unwrap();

        let reader = BTreeIndexReader::open(&path).unwrap();

        // Range [200, 400) => blocks 200, 300
        let result = reader.range(&200u64.to_be_bytes(), &400u64.to_be_bytes());
        assert!(result.contains(200));
        assert!(result.contains(300));
        assert!(!result.contains(100));
        assert!(!result.contains(400));
    }

    #[test]
    fn test_btree_reader_range_large_block_numbers() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bptree");

        // Simulate real Ethereum block numbers (~22M)
        let mut idx = BTreeIndex::new(8);
        for block in 22_100_000u64..22_100_100 {
            // Multiple rows per block (like real data)
            for row in 0..10u32 {
                let row_id = ((block - 22_100_000) as u32) * 10 + row;
                idx.insert(&block.to_be_bytes(), row_id);
            }
        }
        idx.write_to_file(&path).unwrap();

        let reader = BTreeIndexReader::open(&path).unwrap();
        assert_eq!(reader.key_count(), 100);

        // Range [22100050, u64::MAX) — simulates `block_number >= 22100050`
        let result = reader.range(&22_100_050u64.to_be_bytes(), &u64::MAX.to_be_bytes());
        // Should contain rows for blocks 22100050..22100099 = 50 blocks * 10 rows = 500 rows
        assert_eq!(
            result.len(),
            500,
            "range [22100050, MAX) should return 500 rows"
        );

        // Point lookup
        let exact = reader.get(&22_100_050u64.to_be_bytes()).unwrap();
        assert_eq!(exact.len(), 10);

        // Range [22100050, 22100061) — simulates BETWEEN 22100050 AND 22100060
        let result = reader.range(&22_100_050u64.to_be_bytes(), &22_100_061u64.to_be_bytes());
        assert_eq!(
            result.len(),
            110,
            "range [22100050, 22100061) should return 110 rows"
        );
    }

    #[test]
    fn test_btree_large_bitmap() {
        let mut idx = BTreeIndex::new(4);
        let key = [0u8; 4];
        for i in 0..10_000u32 {
            idx.insert(&key, i);
        }

        let bitmap = idx.get(&key).unwrap();
        assert_eq!(bitmap.len(), 10_000);

        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.bptree");
        idx.write_to_file(&path).unwrap();

        let reader = BTreeIndexReader::open(&path).unwrap();
        let bitmap2 = reader.get(&key).unwrap();
        assert_eq!(bitmap2.len(), 10_000);
    }
}
