use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use roaring::RoaringBitmap;

/// Magic bytes identifying a LogEx B+ tree index file.
const INDEX_MAGIC: &[u8; 4] = b"LXIX";

/// Current index file format version.
const INDEX_VERSION: u32 = 1;

/// An in-memory B+ tree index mapping fixed-size byte keys to roaring bitmaps
/// of row IDs. Used during index construction and for the hot partition.
///
/// On disk, the format is:
///   [magic: 4B] [version: 4B] [key_size: 4B] [entry_count: u64]
///   For each entry:
///     [key: key_size bytes] [bitmap_len: u32] [bitmap: serialized RoaringBitmap]
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

        // Header
        w.write_all(INDEX_MAGIC)?;
        w.write_all(&INDEX_VERSION.to_le_bytes())?;
        w.write_all(&(self.key_size as u32).to_le_bytes())?;
        w.write_all(&(self.entries.len() as u64).to_le_bytes())?;

        // Entries (already sorted by BTreeMap)
        for (key, bitmap) in &self.entries {
            w.write_all(key)?;
            let mut bitmap_buf = Vec::new();
            bitmap.serialize_into(&mut bitmap_buf)?;
            w.write_all(&(bitmap_buf.len() as u32).to_le_bytes())?;
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
        let _version = u32::from_le_bytes(
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

        let mut reader = BufReader::new(&data[20..]);
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

    /// Point lookup: find row IDs for an exact key.
    pub fn get(&self, key: &[u8]) -> Option<&RoaringBitmap> {
        self.entries
            .binary_search_by(|(k, _)| k.as_slice().cmp(key))
            .ok()
            .map(|idx| &self.entries[idx].1)
    }

    /// Range scan: return union of all bitmaps for keys in [start, end).
    pub fn range(&self, start: &[u8], end: &[u8]) -> RoaringBitmap {
        // Find the first key >= start
        let lo = self.entries.partition_point(|(k, _)| k.as_slice() < start);
        let hi = self.entries.partition_point(|(k, _)| k.as_slice() < end);

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
