use std::collections::BTreeMap;
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use roaring::{MultiOps, RoaringBitmap};

use crate::index_file::{IndexFile, write_index_file};

/// Magic bytes identifying a LogEx B+ tree index file.
const INDEX_MAGIC: &[u8; 4] = b"LXIX";

/// Current index file format version.
const INDEX_VERSION: u32 = 2;
const INDEX_HEADER_LEN: usize = 20;
const INDEX_V2_ENTRY_TRAILER_LEN: usize = 8 + 4;
const SINGLE_ENTRY_BULK_MAX_LOGICAL_BYTES: u64 = 1024 * 1024;

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
/// New files wrap this logical encoding in the checked index-file container.
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
        let key_size = u32::try_from(self.key_size)
            .map_err(|_| invalid_index("index key width exceeds format limit"))?;
        if key_size == 0 || self.entries.keys().any(|key| key.len() != self.key_size) {
            return Err(invalid_index("invalid index key width"));
        }
        let entry_count = u64::try_from(self.entries.len())
            .map_err(|_| invalid_index("too many index entries"))?;
        let table_end = table_end(self.key_size, entry_count)?;
        let mut logical_len = table_end;
        for bitmap in self.entries.values() {
            let size = u32::try_from(bitmap.serialized_size())
                .map_err(|_| invalid_index("bitmap exceeds format limit"))?;
            logical_len = logical_len
                .checked_add(u64::from(size))
                .ok_or_else(|| invalid_index("index file too large"))?;
        }
        write_index_file(path, logical_len, |writer| {
            // Roaring serializes individual integers. A concrete, bounded
            // buffer combines those writes before the checked file writer,
            // without retaining every serialized bitmap in memory.
            let capacity = logical_len.min(64 * 1024) as usize;
            let mut writer = BufWriter::with_capacity(capacity, writer);
            writer.write_all(INDEX_MAGIC)?;
            writer.write_all(&INDEX_VERSION.to_le_bytes())?;
            writer.write_all(&key_size.to_le_bytes())?;
            writer.write_all(&entry_count.to_le_bytes())?;
            let mut bitmap_offset = table_end;
            for (key, bitmap) in &self.entries {
                let size = bitmap.serialized_size() as u32;
                writer.write_all(key)?;
                writer.write_all(&bitmap_offset.to_le_bytes())?;
                writer.write_all(&size.to_le_bytes())?;
                bitmap_offset += u64::from(size);
            }
            for bitmap in self.entries.values() {
                bitmap.serialize_into(&mut writer)?;
            }
            // Drain only this serialization buffer. The container still has
            // its final page/footer to append and owns the final file flush.
            writer
                .into_inner()
                .map(|_| ())
                .map_err(|error| error.into_error())
        })
    }
}

/// Read-only index loaded from disk.
pub struct BTreeIndexReader {
    key_size: usize,
    /// Sorted array of (key, bitmap) pairs for binary search.
    entries: Vec<(Vec<u8>, RoaringBitmap)>,
}

impl BTreeIndexReader {
    /// Load and structurally validate every entry and bitmap in an index.
    pub fn open(path: &Path) -> io::Result<Self> {
        let data = IndexFile::read_all_from_path(path)?;
        Self::open_data(data.as_slice())
    }

    /// Open a complete published index and reject an artifact substituted
    /// beneath its caller-held publication checkpoint guard.
    pub fn open_bound(path: &Path, expected_file_id: [u8; 16]) -> io::Result<Self> {
        let data = IndexFile::read_all_from_path_bound(path, expected_file_id)?;
        Self::open_data(data.as_slice())
    }

    fn open_file(file: IndexFile) -> io::Result<Self> {
        Self::open_data(&file.read_all()?)
    }

    fn open_data(data: &[u8]) -> io::Result<Self> {
        // The actual file extent bounds this slice; validate counts before
        // allocating keys, entry descriptors or decoded bitmaps.
        let header = IndexHeader::parse(data)?;
        header.validate_geometry(data.len() as u64)?;
        let count = usize::try_from(header.entry_count)
            .map_err(|_| invalid_index("too many index entries"))?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(count)
            .map_err(|_| invalid_index("index entry allocation failed"))?;
        let mut position = INDEX_HEADER_LEN;
        let mut payload_position = if header.version == 2 {
            usize::try_from(table_end(header.key_size, header.entry_count)?)
                .map_err(|_| invalid_index("index table too large"))?
        } else {
            INDEX_HEADER_LEN
        };
        for _ in 0..count {
            let key = take_bytes(data, &mut position, header.key_size)?;
            if entries
                .last()
                .is_some_and(|(previous, _): &(Vec<u8>, RoaringBitmap)| previous.as_slice() >= key)
            {
                return Err(invalid_index("index keys are not strictly increasing"));
            }
            if header.version == 2 {
                let offset =
                    u64::from_le_bytes(take_bytes(data, &mut position, 8)?.try_into().unwrap());
                if offset != payload_position as u64 {
                    return Err(invalid_index("noncontiguous index bitmap payload"));
                }
            }
            let len = u32::from_le_bytes(take_bytes(data, &mut position, 4)?.try_into().unwrap())
                as usize;
            let bitmap_data = if header.version == 2 {
                take_bytes(data, &mut payload_position, len)?
            } else {
                take_bytes(data, &mut position, len)?
            };
            let bitmap = decode_bitmap(bitmap_data)?;
            let mut owned_key = Vec::new();
            owned_key
                .try_reserve_exact(key.len())
                .map_err(|_| invalid_index("index key allocation failed"))?;
            owned_key.extend_from_slice(key);
            entries.push((owned_key, bitmap));
        }
        let consumed = if header.version == 2 {
            payload_position
        } else {
            position
        };
        if consumed != data.len() {
            return Err(invalid_index("trailing index file bytes"));
        }
        Ok(Self {
            key_size: header.key_size,
            entries,
        })
    }

    /// Protected version 2 files use logarithmic point lookup. Legacy raw
    /// files require full structural validation before their keys are searched.
    /// Structural validation alone cannot detect valid-looking data mutations.
    pub fn get_from_file(path: &Path, key: &[u8]) -> io::Result<Option<RoaringBitmap>> {
        Self::get_from_index_file(IndexFile::open(path)?, key)
    }

    /// Read a published point result only after matching its registered file
    /// identity. The caller must retain the publication checkpoint guard.
    pub fn get_from_file_bound(
        path: &Path,
        expected_file_id: [u8; 16],
        key: &[u8],
    ) -> io::Result<Option<RoaringBitmap>> {
        Self::get_from_index_file(IndexFile::open_bound(path, expected_file_id)?, key)
    }

    fn get_from_index_file(mut file: IndexFile, key: &[u8]) -> io::Result<Option<RoaringBitmap>> {
        if single_entry_bulk_route_hint(&file, key) {
            let reader = Self::open_file(file)?;
            if reader.key_size != key.len() || reader.entries.len() != 1 {
                return Err(invalid_index(
                    "single-entry route changed during validation",
                ));
            }
            let (validated_key, bitmap) = reader.entries.into_iter().next().unwrap();
            if validated_key != key {
                return Err(invalid_index("single-entry route key mismatch"));
            }
            return Ok(Some(bitmap));
        }
        let mut bytes = [0u8; INDEX_HEADER_LEN];
        file.read_exact(&mut bytes)?;
        let header = IndexHeader::parse(&bytes)?;
        header.validate_geometry(file.logical_len())?;
        if key.len() != header.key_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "lookup key has length {}, expected {}",
                    key.len(),
                    header.key_size
                ),
            ));
        }
        if !file.is_protected() || header.version == 1 {
            return Ok(Self::open_file(file)?.get(key).cloned());
        }
        if header.entry_count == 0 {
            return Ok(None);
        }
        let table_end = table_end(header.key_size, header.entry_count)?;
        // The protected writer establishes global ordering and contiguity.
        // Check endpoint geometry without rescanning the table on each lookup.
        let first = read_descriptor(&mut file, &header, 0, table_end)?;
        let last = if header.entry_count == 1 {
            first
        } else {
            read_descriptor(&mut file, &header, header.entry_count - 1, table_end)?
        };
        if first.0 != table_end || last.0 + u64::from(last.1) != file.logical_len() {
            return Err(invalid_index("invalid index payload extent"));
        }
        let mut lo = 0;
        let mut hi = header.entry_count;
        let mut current_key = vec![0u8; key.len()];
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            file.seek(SeekFrom::Start(entry_offset(&header, mid)?))?;
            file.read_exact(&mut current_key)?;
            match current_key.as_slice().cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let (offset, len) = read_descriptor(&mut file, &header, mid, table_end)?;
                    let len =
                        usize::try_from(len).map_err(|_| invalid_index("bitmap too large"))?;
                    let mut payload = Vec::new();
                    payload
                        .try_reserve_exact(len)
                        .map_err(|_| invalid_index("bitmap allocation failed"))?;
                    payload.resize(len, 0);
                    file.seek(SeekFrom::Start(offset))?;
                    file.read_exact(&mut payload)?;
                    return decode_bitmap(&payload).map(Some);
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

        union_entries(&self.entries[lo..hi])
    }

    /// Range scan including both endpoints. The upper bound need not have a
    /// representable successor, including an all-0xff numeric/composite key.
    pub fn range_inclusive(&self, start: &[u8], end: &[u8]) -> RoaringBitmap {
        if start > end {
            return RoaringBitmap::new();
        }
        let lo = self.entries.partition_point(|(k, _)| k.as_slice() < start);
        let hi = self.entries.partition_point(|(k, _)| k.as_slice() <= end);
        union_entries(&self.entries[lo..hi])
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

fn single_entry_bulk_route_hint(file: &IndexFile, requested_key: &[u8]) -> bool {
    let Some(prefix) =
        file.pending_unverified_prefix_for_bounded_route(SINGLE_ENTRY_BULK_MAX_LOGICAL_BYTES)
    else {
        return false;
    };
    let Ok(header) = IndexHeader::parse(prefix) else {
        return false;
    };
    if header.version != 2
        || header.entry_count != 1
        || header.key_size != requested_key.len()
        || header.validate_geometry(file.logical_len()).is_err()
    {
        return false;
    }
    let Ok(table_end) = table_end(header.key_size, 1) else {
        return false;
    };
    let Ok(table_end) = usize::try_from(table_end) else {
        return false;
    };
    let Some(entry) = prefix.get(INDEX_HEADER_LEN..table_end) else {
        return false;
    };
    let (hinted_key, descriptor) = entry.split_at(header.key_size);
    if hinted_key != requested_key {
        return false;
    }
    let offset = u64::from_le_bytes(descriptor[..8].try_into().unwrap());
    let len = u64::from(u32::from_le_bytes(descriptor[8..].try_into().unwrap()));
    offset == table_end as u64 && len >= 8 && offset.checked_add(len) == Some(file.logical_len())
}

fn union_entries(entries: &[(Vec<u8>, RoaringBitmap)]) -> RoaringBitmap {
    match entries {
        [] => return RoaringBitmap::new(),
        [(_, bitmap)] => return bitmap.clone(),
        _ => {}
    }
    // MultiOps avoids repeatedly growing and normalizing array containers, but
    // may temporarily promote sparse containers to 8 KiB bitmaps. Limit that
    // promoted payload to 64 KiB; widely separated rows retain pairwise union.
    let mut first_container = u32::MAX;
    let mut last_container = 0;
    for (_, bitmap) in entries {
        if let (Some(first), Some(last)) = (bitmap.min(), bitmap.max()) {
            first_container = first_container.min(first >> 16);
            last_container = last_container.max(last >> 16);
            if last_container - first_container >= 8 {
                let mut result = RoaringBitmap::new();
                for (_, bitmap) in entries {
                    result |= bitmap;
                }
                return result;
            }
        }
    }
    entries.iter().map(|(_, bitmap)| bitmap).union()
}

fn invalid_index(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct IndexHeader {
    version: u32,
    key_size: usize,
    entry_count: u64,
}

impl IndexHeader {
    fn parse(data: &[u8]) -> io::Result<Self> {
        if data.len() < INDEX_HEADER_LEN || &data[..4] != INDEX_MAGIC {
            return Err(invalid_index("invalid or truncated index header"));
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if !matches!(version, 1 | 2) {
            return Err(invalid_index("unsupported index version"));
        }
        let key_size = usize::try_from(u32::from_le_bytes(data[8..12].try_into().unwrap()))
            .map_err(|_| invalid_index("index key width too large"))?;
        if key_size == 0 {
            return Err(invalid_index("zero index key width"));
        }
        Ok(Self {
            version,
            key_size,
            entry_count: u64::from_le_bytes(data[12..20].try_into().unwrap()),
        })
    }

    fn validate_geometry(&self, logical_len: u64) -> io::Result<()> {
        // Every supported Roaring payload occupies at least eight bytes.
        let entry_min = (self.key_size as u64)
            .checked_add(if self.version == 2 { 20 } else { 12 })
            .ok_or_else(|| invalid_index("index entry too large"))?;
        let min_len = self
            .entry_count
            .checked_mul(entry_min)
            .and_then(|len| len.checked_add(INDEX_HEADER_LEN as u64))
            .ok_or_else(|| invalid_index("index file geometry overflows"))?;
        if logical_len < min_len
            || (self.entry_count == 0 && logical_len != INDEX_HEADER_LEN as u64)
        {
            return Err(invalid_index("invalid index file extent"));
        }
        Ok(())
    }
}

fn table_end(key_size: usize, entry_count: u64) -> io::Result<u64> {
    (key_size as u64)
        .checked_add(INDEX_V2_ENTRY_TRAILER_LEN as u64)
        .and_then(|size| size.checked_mul(entry_count))
        .and_then(|len| len.checked_add(INDEX_HEADER_LEN as u64))
        .ok_or_else(|| invalid_index("index table too large"))
}

fn entry_offset(header: &IndexHeader, entry: u64) -> io::Result<u64> {
    table_end(header.key_size, entry)
}

fn read_descriptor(
    file: &mut IndexFile,
    header: &IndexHeader,
    entry: u64,
    table_end: u64,
) -> io::Result<(u64, u32)> {
    let descriptor_offset = entry_offset(header, entry)?
        .checked_add(header.key_size as u64)
        .ok_or_else(|| invalid_index("index descriptor offset overflows"))?;
    file.seek(SeekFrom::Start(descriptor_offset))?;
    let mut descriptor = [0u8; INDEX_V2_ENTRY_TRAILER_LEN];
    file.read_exact(&mut descriptor)?;
    let offset = u64::from_le_bytes(descriptor[..8].try_into().unwrap());
    let len = u32::from_le_bytes(descriptor[8..].try_into().unwrap());
    let end = offset
        .checked_add(u64::from(len))
        .ok_or_else(|| invalid_index("bitmap extent overflows"))?;
    if offset < table_end || len < 8 || end > file.logical_len() {
        return Err(invalid_index("invalid bitmap payload extent"));
    }
    Ok((offset, len))
}

fn take_bytes<'a>(data: &'a [u8], position: &mut usize, len: usize) -> io::Result<&'a [u8]> {
    let end = position
        .checked_add(len)
        .ok_or_else(|| invalid_index("index extent overflows"))?;
    let bytes = data
        .get(*position..end)
        .ok_or_else(|| invalid_index("truncated index entry"))?;
    *position = end;
    Ok(bytes)
}

fn decode_bitmap(data: &[u8]) -> io::Result<RoaringBitmap> {
    // Most index values have one ordinary array/bitmap container. Its fixed
    // header proves the container count and makes key ordering vacuous; retain
    // the canonical offset, exact extent, and dependency content validation.
    const SINGLE_CONTAINER_COOKIE_AND_COUNT: u64 = 12346 | (1 << 32);
    if let Some(header) = data.get(..16)
        && u64::from_le_bytes(header[..8].try_into().unwrap()) == SINGLE_CONTAINER_COOKIE_AND_COUNT
    {
        let cardinality = usize::from(u16::from_le_bytes(header[10..12].try_into().unwrap())) + 1;
        let payload_len = if cardinality <= 4096 {
            cardinality * 2
        } else {
            8192
        };
        if header[12..16] != 16u32.to_le_bytes() || data.len() != 16 + payload_len {
            return Err(invalid_index("invalid single Roaring container extent"));
        }
        if cardinality > 4096 {
            return decode_dense_bitmaps(&header[8..12], &data[16..]);
        }
        return deserialize_bitmap(data);
    }
    // Inspect borrowed descriptors and run ranges before the dependency allocates.
    // Its decoder checks array/bitmap contents but ignores serialized offsets and
    // run cardinality, and normalizes unordered/overlapping runs into a set.
    let mut position = 0;
    let cookie = u32::from_le_bytes(take_bytes(data, &mut position, 4)?.try_into().unwrap());
    let (count, has_offsets, run_containers) = if cookie == 12346 {
        (
            u32::from_le_bytes(take_bytes(data, &mut position, 4)?.try_into().unwrap()) as usize,
            true,
            None,
        )
    } else if cookie as u16 == 12347 {
        let count = ((cookie >> 16) + 1) as usize;
        let runs = take_bytes(data, &mut position, count.div_ceil(8))?;
        (count, count >= 4, Some(runs))
    } else {
        return Err(invalid_index("invalid Roaring bitmap cookie"));
    };
    if count > 65536 {
        return Err(invalid_index("too many Roaring containers"));
    }
    let descriptions = take_bytes(data, &mut position, count * 4)?;
    let offsets = if has_offsets {
        Some(take_bytes(data, &mut position, count * 4)?)
    } else {
        None
    };
    let payload_start = position;
    let mut dense_only = count != 0;
    let mut previous = None;
    for (index, description) in descriptions.as_chunks::<4>().0.iter().enumerate() {
        let key = u16::from_le_bytes(description[..2].try_into().unwrap());
        if previous.is_some_and(|previous| previous >= key) {
            return Err(invalid_index(
                "Roaring container keys are not strictly increasing",
            ));
        }
        previous = Some(key);
        if let Some(offsets) = offsets {
            let offset = u32::from_le_bytes(offsets[index * 4..index * 4 + 4].try_into().unwrap());
            if u64::from(offset) != position as u64 {
                return Err(invalid_index("noncontiguous Roaring container offset"));
            }
        }
        let cardinality =
            usize::from(u16::from_le_bytes(description[2..4].try_into().unwrap())) + 1;
        if run_containers.is_some_and(|runs| runs[index / 8] & (1 << (index % 8)) != 0) {
            dense_only = false;
            let run_count = usize::from(u16::from_le_bytes(
                take_bytes(data, &mut position, 2)?.try_into().unwrap(),
            ));
            let runs = take_bytes(data, &mut position, run_count * 4)?;
            let mut previous_end = None;
            let mut actual_cardinality = 0;
            for run in runs.as_chunks::<4>().0 {
                let start = u16::from_le_bytes(run[..2].try_into().unwrap());
                let length_minus_one = u16::from_le_bytes(run[2..].try_into().unwrap());
                let end = start
                    .checked_add(length_minus_one)
                    .ok_or_else(|| invalid_index("Roaring run range exceeds its container"))?;
                if previous_end.is_some_and(|previous_end| start <= previous_end) {
                    return Err(invalid_index("Roaring runs overlap or are not ordered"));
                }
                previous_end = Some(end);
                // Ordered, disjoint u16 ranges contain at most 65,536 values.
                actual_cardinality += usize::from(length_minus_one) + 1;
            }
            if actual_cardinality != cardinality {
                return Err(invalid_index("Roaring run cardinality mismatch"));
            }
        } else {
            let bytes = if cardinality <= 4096 {
                dense_only = false;
                cardinality * 2
            } else {
                8192
            };
            take_bytes(data, &mut position, bytes)?;
        }
    }
    if position != data.len() {
        return Err(invalid_index("trailing bitmap payload bytes"));
    }
    if dense_only {
        return decode_dense_bitmaps(descriptions, &data[payload_start..]);
    }
    deserialize_bitmap(data)
}

fn decode_dense_bitmaps(descriptions: &[u8], payload: &[u8]) -> io::Result<RoaringBitmap> {
    // Both callers prove nonempty ordered descriptors and exactly 8192 payload
    // bytes each. The public constructor counts set bits and avoids the general
    // decoder's zero-fill before copying a complete dense container.
    let descriptions = descriptions.as_chunks::<4>().0;
    if let [description] = descriptions {
        return decode_dense_container(description, payload);
    }
    // Owned MultiOps moves distinct containers and uses cached cardinalities.
    // Repeated `|=` would rescan the growing result's lengths for each merge.
    descriptions
        .iter()
        .enumerate()
        .map(|(index, description)| {
            let start = index * 8192;
            decode_dense_container(description, &payload[start..start + 8192])
        })
        .union()
}

fn decode_dense_container(description: &[u8; 4], payload: &[u8]) -> io::Result<RoaringBitmap> {
    let key = u16::from_le_bytes(description[..2].try_into().unwrap());
    let expected = u64::from(u16::from_le_bytes(description[2..].try_into().unwrap())) + 1;
    // One container's aligned start plus 65535 fits u32, including key65535.
    let bitmap = RoaringBitmap::from_lsb0_bytes(u32::from(key) << 16, payload);
    if bitmap.len() != expected {
        return Err(invalid_index(
            "Roaring dense container cardinality mismatch",
        ));
    }
    Ok(bitmap)
}

fn deserialize_bitmap(data: &[u8]) -> io::Result<RoaringBitmap> {
    // Both decode_bitmap paths prove exact serialized extent first. Their
    // cookie/description/offset and run/array/dense byte counts correspond to
    // roaring 0.10.12's decoder; recheck that correspondence on dependency updates.
    // Pass the slice by value to preserve the dependency's direct-slice read path.
    RoaringBitmap::deserialize_from(data)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn integrity_fixture(version: u32, entries: &[(u32, u32)]) -> Vec<u8> {
        assert!(matches!(version, 1 | 2));
        let mut data = b"LXIX".to_vec();
        data.extend_from_slice(&version.to_le_bytes());
        data.extend_from_slice(&4u32.to_le_bytes());
        data.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        let payloads: Vec<Vec<u8>> = entries
            .iter()
            .map(|&(_, row)| {
                let bitmap: RoaringBitmap = [row].into_iter().collect();
                let mut payload = Vec::new();
                bitmap.serialize_into(&mut payload).unwrap();
                payload
            })
            .collect();
        let mut offset = 20 + entries.len() * 16;
        for (&(key, _), payload) in entries.iter().zip(&payloads) {
            data.extend_from_slice(&key.to_be_bytes());
            if version == 2 {
                data.extend_from_slice(&(offset as u64).to_le_bytes());
                offset += payload.len();
            }
            data.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            if version == 1 {
                data.extend_from_slice(payload);
            }
        }
        if version == 2 {
            for payload in payloads {
                data.extend_from_slice(&payload);
            }
        }
        data
    }

    fn single_dense_integrity_fixture(key: u32) -> (Vec<u8>, RoaringBitmap) {
        let bitmap: RoaringBitmap = (0..5000).collect();
        let mut serialized = Vec::new();
        bitmap.serialize_into(&mut serialized).unwrap();
        let table_end = INDEX_HEADER_LEN + 4 + INDEX_V2_ENTRY_TRAILER_LEN;
        let mut data = b"LXIX".to_vec();
        data.extend_from_slice(&2u32.to_le_bytes());
        data.extend_from_slice(&4u32.to_le_bytes());
        data.extend_from_slice(&1u64.to_le_bytes());
        data.extend_from_slice(&key.to_be_bytes());
        data.extend_from_slice(&(table_end as u64).to_le_bytes());
        data.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
        data.extend_from_slice(&serialized);
        (data, bitmap)
    }

    fn synthetic_single_entry_logical(key: u32, logical_len: usize) -> Vec<u8> {
        let table_end = INDEX_HEADER_LEN + 4 + INDEX_V2_ENTRY_TRAILER_LEN;
        assert!(logical_len >= table_end + 8 && logical_len <= u32::MAX as usize);
        let mut data = Vec::new();
        data.try_reserve_exact(logical_len).unwrap();
        data.resize(logical_len, 0);
        data[..4].copy_from_slice(INDEX_MAGIC);
        data[4..8].copy_from_slice(&2u32.to_le_bytes());
        data[8..12].copy_from_slice(&4u32.to_le_bytes());
        data[12..20].copy_from_slice(&1u64.to_le_bytes());
        data[20..24].copy_from_slice(&key.to_be_bytes());
        data[24..32].copy_from_slice(&(table_end as u64).to_le_bytes());
        data[32..36].copy_from_slice(&((logical_len - table_end) as u32).to_le_bytes());
        data
    }

    fn write_protected_fixture(path: &Path, data: &[u8]) {
        write_index_file(path, data.len() as u64, |writer| writer.write_all(data)).unwrap();
    }

    #[test]
    fn protected_index_detects_changed_logical_header_table_and_payload() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("protected.bptree");
        let logical = integrity_fixture(2, &[(1, 10), (3, 30)]);
        write_protected_fixture(&path, &logical);
        assert!(IndexFile::open(&path).unwrap().is_protected());
        assert_eq!(BTreeIndexReader::open(&path).unwrap().key_count(), 2);
        assert_eq!(
            BTreeIndexReader::get_from_file(&path, &1u32.to_be_bytes()).unwrap(),
            Some([10].into_iter().collect())
        );
        assert!(
            BTreeIndexReader::get_from_file(&path, &2u32.to_be_bytes())
                .unwrap()
                .is_none()
        );
        let pristine = fs::read(&path).unwrap();
        let logical_start = pristine
            .windows(logical.len())
            .position(|window| window == logical)
            .unwrap();
        for offset in [4, 12, 23, 24, logical.len() - 1] {
            let mut damaged = pristine.clone();
            damaged[logical_start + offset] ^= 1;
            fs::write(&path, damaged).unwrap();
            assert!(BTreeIndexReader::open(&path).is_err(), "offset {offset}");
            for key in [1u32, 2, 4] {
                assert!(
                    BTreeIndexReader::get_from_file(&path, &key.to_be_bytes()).is_err(),
                    "offset {offset}, lookup {key}"
                );
            }
        }
        let mut damaged = pristine;
        damaged[0] ^= 1;
        fs::write(&path, damaged).unwrap();
        assert!(BTreeIndexReader::open(&path).is_err());
        assert!(BTreeIndexReader::get_from_file(&path, &1u32.to_be_bytes()).is_err());
    }

    #[test]
    fn matched_single_entry_bulk_route_returns_exact_rows_after_full_validation() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("single-dense.bptree");
        let key = 7u32.to_be_bytes();
        let (logical, expected) = single_dense_integrity_fixture(7);
        assert!(logical.len() > 4096);
        write_protected_fixture(&path, &logical);

        let file = IndexFile::open(&path).unwrap();
        assert!(single_entry_bulk_route_hint(&file, &key));
        assert!(!single_entry_bulk_route_hint(&file, &8u32.to_be_bytes()));
        assert!(!single_entry_bulk_route_hint(&file, &[0; 3]));
        assert_eq!(
            BTreeIndexReader::get_from_file(&path, &key).unwrap(),
            Some(expected)
        );
    }

    #[test]
    fn absent_and_wrong_width_keys_do_not_take_single_entry_bulk_route() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("single-dense-damaged-footer.bptree");
        let key = 7u32.to_be_bytes();
        let (logical, _) = single_dense_integrity_fixture(7);
        write_protected_fixture(&path, &logical);
        let mut physical = fs::read(&path).unwrap();
        *physical.last_mut().unwrap() ^= 1;
        fs::write(&path, physical).unwrap();

        assert!(
            BTreeIndexReader::get_from_file(&path, &8u32.to_be_bytes())
                .unwrap()
                .is_none()
        );
        assert_eq!(
            BTreeIndexReader::get_from_file(&path, &[0; 3])
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(BTreeIndexReader::get_from_file(&path, &key).is_err());
    }

    #[test]
    fn single_entry_bulk_route_never_trusts_misleading_prefix_or_damaged_payload() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("single-dense-damaged.bptree");
        let key = 7u32.to_be_bytes();
        let (logical, _) = single_dense_integrity_fixture(7);
        write_protected_fixture(&path, &logical);
        let pristine = fs::read(&path).unwrap();
        let logical_start = pristine
            .windows(logical.len())
            .position(|window| window == logical)
            .unwrap();

        for position in [12, 20, 24, 32] {
            let mut damaged = pristine.clone();
            damaged[logical_start + position] ^= 1;
            fs::write(&path, damaged).unwrap();
            assert!(
                BTreeIndexReader::get_from_file(&path, &key).is_err(),
                "hint position {position}"
            );
        }

        let mut damaged = pristine;
        damaged[logical_start + 4096 + 1] ^= 1;
        fs::write(&path, damaged).unwrap();
        assert!(BTreeIndexReader::get_from_file(&path, &key).is_err());
    }

    #[test]
    fn single_entry_bulk_route_is_bounded_and_requires_more_than_one_page() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("single-route-bound.bptree");
        let key = 7u32.to_be_bytes();

        write_protected_fixture(&path, &integrity_fixture(2, &[(7, 10)]));
        assert!(!single_entry_bulk_route_hint(
            &IndexFile::open(&path).unwrap(),
            &key
        ));

        for (logical_len, expected) in [
            (SINGLE_ENTRY_BULK_MAX_LOGICAL_BYTES as usize, true),
            (SINGLE_ENTRY_BULK_MAX_LOGICAL_BYTES as usize + 1, false),
        ] {
            let logical = synthetic_single_entry_logical(7, logical_len);
            write_protected_fixture(&path, &logical);
            assert_eq!(
                single_entry_bulk_route_hint(&IndexFile::open(&path).unwrap(), &key),
                expected,
                "logical length {logical_len}"
            );
            if expected {
                assert!(BTreeIndexReader::get_from_file(&path, &key).is_err());
            }
        }
    }

    #[test]
    fn index_readers_bound_declared_sizes_before_allocating() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("sizes.bptree");
        for version in [1, 2] {
            let original = integrity_fixture(version, &[(1, 10)]);
            let mut excessive_count = original.clone();
            excessive_count[12..20].copy_from_slice(&u64::MAX.to_le_bytes());
            let mut excessive_key = original.clone();
            excessive_key[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
            let mut excessive_payload = original;
            let length_start = if version == 1 { 24 } else { 32 };
            excessive_payload[length_start..length_start + 4]
                .copy_from_slice(&u32::MAX.to_le_bytes());
            for data in [excessive_count, excessive_key, excessive_payload] {
                for protected in [false, true] {
                    if protected {
                        write_protected_fixture(&path, &data);
                    } else {
                        fs::write(&path, &data).unwrap();
                    }
                    assert!(
                        BTreeIndexReader::open(&path).is_err(),
                        "version {version}, protected {protected}"
                    );
                    assert!(
                        BTreeIndexReader::get_from_file(&path, &1u32.to_be_bytes()).is_err(),
                        "version {version}, protected {protected}"
                    );
                }
            }
        }
    }

    #[test]
    fn index_writer_rejects_invalid_key_width_without_replacing_the_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("width.bptree");
        fs::write(&path, b"existing file").unwrap();
        let mut index = BTreeIndex::new(4);
        index.entries.insert(vec![1], [10].into_iter().collect());
        assert!(index.write_to_file(&path).is_err());
        assert!(BTreeIndex::new(0).write_to_file(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"existing file");
    }

    #[test]
    fn bitmap_decode_rejects_excessive_count_and_duplicate_container_keys() {
        let mut excessive_count = 12346u32.to_le_bytes().to_vec();
        excessive_count.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_bitmap(&excessive_count).is_err());

        let bitmap: RoaringBitmap = [1, 65537].into_iter().collect();
        let mut payload = Vec::new();
        bitmap.serialize_into(&mut payload).unwrap();
        assert_eq!(decode_bitmap(&payload).unwrap(), bitmap);
        payload[12..14].copy_from_slice(&0u16.to_le_bytes());
        assert!(decode_bitmap(&payload).is_err());
    }

    type RunFixtureContainer<'a> = (u16, u16, &'a [(u16, u16)]);

    fn run_bitmap_fixture(containers: &[RunFixtureContainer<'_>]) -> Vec<u8> {
        // Fewer than four run containers have no offset table in this encoding.
        assert!(!containers.is_empty() && containers.len() <= 4);
        let cookie = 12347 | (((containers.len() - 1) as u32) << 16);
        let mut payload = cookie.to_le_bytes().to_vec();
        payload.push((1 << containers.len()) - 1);
        for &(key, cardinality, _) in containers {
            assert!(cardinality > 0);
            payload.extend_from_slice(&key.to_le_bytes());
            payload.extend_from_slice(&(cardinality - 1).to_le_bytes());
        }
        if containers.len() >= 4 {
            let mut offset = payload.len() + containers.len() * 4;
            for &(_, _, runs) in containers {
                payload.extend_from_slice(&(offset as u32).to_le_bytes());
                offset += 2 + runs.len() * 4;
            }
        }
        for &(_, _, runs) in containers {
            payload.extend_from_slice(&(runs.len() as u16).to_le_bytes());
            for &(start, length_minus_one) in runs {
                payload.extend_from_slice(&start.to_le_bytes());
                payload.extend_from_slice(&length_minus_one.to_le_bytes());
            }
        }
        payload
    }

    fn dense_bitmap_fixture(containers: &[(u16, usize, usize)]) -> Vec<u8> {
        assert!(!containers.is_empty() && containers.len() <= 64);
        let mut payload = 12346u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&(containers.len() as u32).to_le_bytes());
        for &(key, declared_cardinality, _) in containers {
            assert!((4097..=65536).contains(&declared_cardinality));
            payload.extend_from_slice(&key.to_le_bytes());
            payload.extend_from_slice(&((declared_cardinality - 1) as u16).to_le_bytes());
        }
        let mut offset = 8 + containers.len() * 8;
        for _ in containers {
            payload.extend_from_slice(&(offset as u32).to_le_bytes());
            offset += 8192;
        }
        for &(_, _, actual_cardinality) in containers {
            assert!(actual_cardinality <= 65536);
            let mut words = [0u64; 1024];
            for low in 0..actual_cardinality {
                words[low / 64] |= 1 << (low % 64);
            }
            for word in words {
                payload.extend_from_slice(&word.to_le_bytes());
            }
        }
        payload
    }

    #[test]
    fn bitmap_decode_dense_container_layouts_match_exact_rows() {
        for containers in [
            vec![(u16::MAX, 65536)],
            vec![(1, 4097), (4, 5000), (19, 8193), (u16::MAX, 65536)],
            vec![
                (0, 4097),
                (2, 5000),
                (5, 6000),
                (9, 7000),
                (14, 8000),
                (20, 9000),
                (31, 10000),
                (u16::MAX, 65536),
            ],
        ] {
            let fixture = dense_bitmap_fixture(
                &containers
                    .iter()
                    .map(|&(key, cardinality)| (key, cardinality, cardinality))
                    .collect::<Vec<_>>(),
            );
            let decoded = decode_bitmap(&fixture).unwrap();
            assert!(
                decoded.iter().eq(containers
                    .iter()
                    .flat_map(|&(key, cardinality)| (0..cardinality)
                        .map(move |low| (u32::from(key) << 16) | low as u32)))
            );
        }
    }

    #[test]
    fn bitmap_decode_larger_dense_layouts_match_exact_rows() {
        for count in [9, 49, 50, 51, 64] {
            let containers = (0..count)
                .map(|index| ((index * 3) as u16, 4097 + index))
                .collect::<Vec<_>>();
            let fixture = dense_bitmap_fixture(
                &containers
                    .iter()
                    .map(|&(key, cardinality)| (key, cardinality, cardinality))
                    .collect::<Vec<_>>(),
            );
            let decoded = decode_bitmap(&fixture).unwrap();
            assert!(
                decoded.iter().eq(containers
                    .iter()
                    .flat_map(|&(key, cardinality)| (0..cardinality)
                        .map(move |low| (u32::from(key) << 16) | low as u32)))
            );
        }
    }

    #[test]
    fn bitmap_decode_mixed_sparse_and_dense_containers_falls_back_with_exact_rows() {
        let mut expected = RoaringBitmap::new();
        expected.extend([3, 17, 41]);
        expected.extend((0..5000).map(|low| (11 << 16) | low));
        expected.insert((u32::from(u16::MAX) << 16) | 65535);
        let mut fixture = Vec::new();
        expected.serialize_into(&mut fixture).unwrap();
        assert_eq!(decode_bitmap(&fixture).unwrap(), expected);
    }

    #[test]
    fn bitmap_decode_checks_each_dense_container_cardinality() {
        let fixture = dense_bitmap_fixture(&[(1, 4999, 5000), (8, 5001, 5000)]);
        assert!(decode_bitmap(&fixture).is_err());

        let containers = (0..64)
            .map(|index| {
                let declared = match index {
                    52 => 4999,
                    63 => 5001,
                    _ => 5000,
                };
                (index as u16, declared, 5000)
            })
            .collect::<Vec<_>>();
        assert!(decode_bitmap(&dense_bitmap_fixture(&containers)).is_err());
    }

    #[test]
    fn bitmap_decode_rejects_declared_dense_containers_with_sparse_payloads() {
        for actual_cardinality in [0, 17, 4096] {
            let fixture = dense_bitmap_fixture(&[(3, 4097, actual_cardinality)]);
            assert!(
                decode_bitmap(&fixture).is_err(),
                "actual cardinality {actual_cardinality}"
            );
        }
    }

    #[test]
    fn bitmap_decode_rejects_run_container_cardinality_mismatch() {
        let valid = run_bitmap_fixture(&[(0, 1, &[(7, 0)])]);
        assert_eq!(decode_bitmap(&valid).unwrap(), [7].into_iter().collect());

        let empty_run = run_bitmap_fixture(&[(0, 1, &[])]);
        assert_eq!(empty_run.len(), 11);
        assert!(decode_bitmap(&empty_run).is_err());
    }

    #[test]
    fn bitmap_decode_checks_each_run_container_cardinality() {
        let valid = run_bitmap_fixture(&[(0, 1, &[(7, 0)]), (1, 2, &[(9, 1)])]);
        assert_eq!(
            decode_bitmap(&valid).unwrap(),
            [7, 65545, 65546].into_iter().collect()
        );
        // The overall cardinality remains three: a total-only comparison would
        // miss the disagreement in both individual container descriptions.
        let mismatch = run_bitmap_fixture(&[(0, 2, &[(7, 0)]), (1, 1, &[(9, 1)])]);
        assert!(decode_bitmap(&mismatch).is_err());
    }

    #[test]
    fn bitmap_decode_rejects_misdirected_container_offsets() {
        let bitmap: RoaringBitmap = [7].into_iter().collect();
        let mut payload = Vec::new();
        bitmap.serialize_into(&mut payload).unwrap();
        assert_eq!(payload.len(), 18);
        assert_eq!(&payload[12..16], &16u32.to_le_bytes());
        assert_eq!(decode_bitmap(&payload).unwrap(), bitmap);
        // Point the container back into the cookie instead of its two-byte data.
        payload[12..16].copy_from_slice(&0u32.to_le_bytes());
        assert!(decode_bitmap(&payload).is_err());
    }

    #[test]
    fn single_container_fast_path_preserves_array_and_bitmap_content_checks() {
        let array: RoaringBitmap = [7, 9].into_iter().collect();
        let mut payload = Vec::new();
        array.serialize_into(&mut payload).unwrap();
        assert_eq!(decode_bitmap(&payload).unwrap(), array);
        payload.copy_within(16..18, 18);
        assert!(decode_bitmap(&payload).is_err(), "duplicate array values");

        let dense: RoaringBitmap = (0..5000).collect();
        let mut payload = Vec::new();
        dense.serialize_into(&mut payload).unwrap();
        assert_eq!(decode_bitmap(&payload).unwrap(), dense);
        payload[16] ^= 1;
        assert!(
            decode_bitmap(&payload).is_err(),
            "dense cardinality mismatch"
        );
    }

    #[test]
    fn bitmap_decode_accepts_empty_array_dense_and_mixed_run_encodings() {
        for bitmap in [
            RoaringBitmap::new(),
            [1, 9, 65537].into_iter().collect(),
            (0..5000).collect(),
        ] {
            let mut payload = Vec::new();
            bitmap.serialize_into(&mut payload).unwrap();
            assert_eq!(decode_bitmap(&payload).unwrap(), bitmap);
            payload.pop();
            assert!(decode_bitmap(&payload).is_err());
        }

        // Two containers, one run and one array; this run-cookie encoding has
        // no offset table. The container payloads have different byte lengths.
        let mut mixed = (12347u32 | (1 << 16)).to_le_bytes().to_vec();
        mixed.push(1);
        for value in [0u16, 0, 1, 2, 1, 7, 0, 3, 9, 12] {
            mixed.extend_from_slice(&value.to_le_bytes());
        }
        assert_eq!(
            decode_bitmap(&mixed).unwrap(),
            [7, 65539, 65545, 65548].into_iter().collect()
        );

        let with_offsets = run_bitmap_fixture(&[
            (0, 1, &[(7, 0)]),
            (1, 1, &[(9, 0)]),
            (2, 1, &[(11, 0)]),
            (3, 1, &[(13, 0)]),
        ]);
        assert_eq!(
            decode_bitmap(&with_offsets).unwrap(),
            [7, 65545, 131083, 196621].into_iter().collect()
        );
    }

    #[test]
    fn bitmap_decode_rejects_unordered_overlapping_and_overflowing_runs() {
        for runs in [
            vec![(7, 0), (5, 0)],
            vec![(5, 2), (7, 1)],
            vec![(u16::MAX, 1)],
        ] {
            let cardinality = runs.iter().map(|(_, length)| length + 1).sum();
            let payload = run_bitmap_fixture(&[(0, cardinality, &runs)]);
            assert!(decode_bitmap(&payload).is_err(), "runs {runs:?}");
        }
        let adjacent = run_bitmap_fixture(&[(0, 2, &[(5, 0), (6, 0)])]);
        assert_eq!(
            decode_bitmap(&adjacent).unwrap(),
            [5, 6].into_iter().collect()
        );
    }

    #[test]
    fn explicit_legacy_index_fixtures_support_point_and_range_lookups() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("legacy.bptree");
        for version in [1, 2] {
            fs::write(&path, integrity_fixture(version, &[(1, 10), (3, 30)])).unwrap();
            let reader = BTreeIndexReader::open(&path).unwrap();
            let expected: RoaringBitmap = [10, 30].into_iter().collect();
            assert_eq!(
                reader.range_inclusive(&1u32.to_be_bytes(), &3u32.to_be_bytes()),
                expected,
                "version {version}"
            );
            assert_eq!(
                BTreeIndexReader::get_from_file(&path, &1u32.to_be_bytes()).unwrap(),
                Some([10].into_iter().collect()),
                "version {version}"
            );
            assert!(
                BTreeIndexReader::get_from_file(&path, &2u32.to_be_bytes())
                    .unwrap()
                    .is_none()
            );
        }
    }

    #[test]
    fn index_readers_reject_unsupported_versions() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("unsupported.bptree");
        for version in [0, 3, u32::MAX] {
            let mut data = integrity_fixture(1, &[(2, 20)]);
            data[4..8].copy_from_slice(&version.to_le_bytes());
            fs::write(&path, data).unwrap();
            assert!(BTreeIndexReader::open(&path).is_err(), "version {version}");
            assert!(
                BTreeIndexReader::get_from_file(&path, &2u32.to_be_bytes()).is_err(),
                "version {version}"
            );
        }
    }

    #[test]
    fn index_point_lookup_rejects_partial_entries_even_for_absent_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("partial.bptree");
        for version in [1, 2] {
            let fixture = integrity_fixture(version, &[(2, 20)]);
            let payload_start = if version == 1 { 28 } else { 36 };
            for end in [24, payload_start, payload_start + 1] {
                let mut data = fixture[..end].to_vec();
                if end >= payload_start {
                    // Keep the declared length tiny, even before bounds checks exist.
                    data[payload_start - 4..payload_start].copy_from_slice(&8u32.to_le_bytes());
                }
                fs::write(&path, data).unwrap();
                assert!(
                    BTreeIndexReader::open(&path).is_err(),
                    "version {version}, truncated at {end}"
                );
                for key in [1u32, 2, 3] {
                    assert!(
                        BTreeIndexReader::get_from_file(&path, &key.to_be_bytes()).is_err(),
                        "version {version}, truncated at {end}, lookup {key}"
                    );
                }
            }
        }
    }

    #[test]
    fn index_open_rejects_unsorted_and_duplicate_keys() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("ordering.bptree");
        for version in [1, 2] {
            for entries in [[(2, 20), (1, 10)], [(1, 10), (1, 20)]] {
                fs::write(&path, integrity_fixture(version, &entries)).unwrap();
                assert!(
                    BTreeIndexReader::open(&path).is_err(),
                    "version {version}, entries {entries:?}"
                );
            }
        }
    }

    #[test]
    fn index_v2_open_rejects_aliased_bitmap_descriptors() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("alias.bptree");
        let mut data = integrity_fixture(2, &[(1, 10), (2, 20)]);
        // Each table entry is a four-byte key followed by a twelve-byte descriptor.
        data.copy_within(40..52, 24);
        fs::write(&path, data).unwrap();
        assert!(BTreeIndexReader::open(&path).is_err());
    }

    #[test]
    fn index_v2_readers_reject_bitmap_offsets_into_the_key_table() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("table-offset.bptree");
        let mut data = integrity_fixture(2, &[(1, 10)]);
        data[24..32].copy_from_slice(&20u64.to_le_bytes());
        fs::write(&path, data).unwrap();
        assert!(BTreeIndexReader::open(&path).is_err());
        assert!(BTreeIndexReader::get_from_file(&path, &1u32.to_be_bytes()).is_err());
    }

    #[test]
    fn index_readers_reject_unconsumed_bytes_inside_bitmap_payloads() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("payload-trailing.bptree");
        for version in [1, 2] {
            let mut data = integrity_fixture(version, &[(1, 10)]);
            let length_start = if version == 1 { 24 } else { 32 };
            let length =
                u32::from_le_bytes(data[length_start..length_start + 4].try_into().unwrap());
            data[length_start..length_start + 4].copy_from_slice(&(length + 1).to_le_bytes());
            data.push(0);
            fs::write(&path, data).unwrap();
            assert!(BTreeIndexReader::open(&path).is_err(), "version {version}");
            assert!(
                BTreeIndexReader::get_from_file(&path, &1u32.to_be_bytes()).is_err(),
                "version {version}"
            );
        }
    }

    #[test]
    fn index_open_rejects_trailing_bytes_after_declared_entries() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("file-trailing.bptree");
        for version in [1, 2] {
            let mut data = integrity_fixture(version, &[(1, 10)]);
            data.push(0);
            fs::write(&path, data).unwrap();
            assert!(BTreeIndexReader::open(&path).is_err(), "version {version}");
        }
    }

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
    fn range_unions_match_row_pairs_for_compact_and_wide_row_spans() {
        use std::collections::BTreeSet;

        for wide in [false, true] {
            let mut pairs = Vec::new();
            for key in 0u32..128 {
                let container = if wide { key * 64 } else { key % 2 };
                for row in [7, 65535, 65536, (container << 16) + key] {
                    pairs.push((key, row));
                }
            }
            pairs.extend((0..4100).map(|offset| (7, (2 << 16) + offset)));
            pairs.push((127, u32::MAX));
            let mut index = BTreeIndex::new(4);
            for &(key, row) in &pairs {
                index.insert(&key.to_be_bytes(), row);
            }
            let dir = TempDir::new().unwrap();
            let path = dir.path().join("union.bptree");
            index.write_to_file(&path).unwrap();
            let reader = BTreeIndexReader::open(&path).unwrap();
            for start in [0u32, 1, 49, 127, 128, u32::MAX] {
                for end in [0u32, 1, 49, 50, 51, 128, u32::MAX] {
                    let expected = |inclusive| {
                        pairs
                            .iter()
                            .filter(|&&(key, _)| {
                                start <= key && (key < end || inclusive && key == end)
                            })
                            .map(|&(_, row)| row)
                            .collect::<BTreeSet<_>>()
                    };
                    let exclusive = reader.range(&start.to_be_bytes(), &end.to_be_bytes());
                    let inclusive =
                        reader.range_inclusive(&start.to_be_bytes(), &end.to_be_bytes());
                    assert_eq!(exclusive.iter().collect::<BTreeSet<_>>(), expected(false));
                    assert_eq!(inclusive.iter().collect::<BTreeSet<_>>(), expected(true));
                }
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
    fn buffered_file_roundtrip_preserves_multiple_dense_containers() {
        let key = 7u32.to_be_bytes();
        let expected: Vec<u32> = (0..9)
            .flat_map(|container| (0..5000).map(move |offset| (container << 16) + offset))
            .collect();
        let mut index = BTreeIndex::new(key.len());
        for &row in &expected {
            index.insert(&key, row);
        }
        // Nine dense containers cross the serializer's 64 KiB buffer and leave
        // a partial final write. Compare complete row IDs, not just cardinality.
        assert!(index.get(&key).unwrap().serialized_size() > 64 * 1024);
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("buffered.bptree");
        index.write_to_file(&path).unwrap();
        let reader = BTreeIndexReader::open(&path).unwrap();
        assert_eq!(
            reader.get(&key).unwrap().iter().collect::<Vec<_>>(),
            expected
        );
        let point = BTreeIndexReader::get_from_file(&path, &key)
            .unwrap()
            .unwrap();
        assert_eq!(point.iter().collect::<Vec<_>>(), expected);
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
