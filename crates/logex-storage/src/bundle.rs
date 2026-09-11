//! Immutable compressed-artifact snapshots, committed by the storage catalog.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

const FILE_MAGIC: &[u8; 8] = b"LXBND002";
const TABLE_MAGIC: &[u8; 8] = b"LXBT0002";
// Column payloads and page-index entries are append-only; nullable bitmaps replace.
pub(crate) const DATA_STREAMS: u8 = 28;
const STREAMS: u8 = 32;
pub(crate) const MAX_EXTENT_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_EXTENTS: usize = 4096;
const MAX_TABLE_BYTES: u32 = 4 * 1024 * 1024;
const MAX_TABLE_DEPTH: u32 = 32;
pub(crate) const MAX_ROWS: u64 = MAX_EXTENTS as u64 * 16_384;
const INDEX_BYTES: usize = MAX_EXTENTS * crate::page::PAGE_INDEX_ENTRY_BYTES;

fn inline_index(id: u8) -> bool {
    (14..28).contains(&id)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleReference {
    pub row_count: u64,
    pub table_offset: u64,
    pub table_len: u32,
    pub checksum: u32,
    pub depth: u32,
    pub chain_bytes: u32,
}

impl BundleReference {
    pub(crate) fn end(&self) -> io::Result<u64> {
        if self.row_count > MAX_ROWS
            || self.table_offset < FILE_MAGIC.len() as u64
            || self.table_len < 24
            || self.table_len > MAX_TABLE_BYTES
            || self.depth == 0
            || self.depth > MAX_TABLE_DEPTH
            || self.chain_bytes < self.table_len
            || self.chain_bytes > MAX_TABLE_BYTES
        {
            return Err(invalid("invalid bundle reference"));
        }
        self.table_offset
            .checked_add(u64::from(self.table_len))
            .ok_or_else(|| invalid("bundle reference overflow"))
    }
}

#[derive(Debug, Clone)]
struct Extent {
    offset: u64,
    len: u32,
    checksum: u32,
}

#[derive(Debug, Clone, Default)]
struct Stream {
    len: u64,
    extents: Vec<Extent>,
    inline: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct BundleReader {
    file: Arc<Mutex<File>>,
    reference: BundleReference,
    streams: Arc<BTreeMap<u8, Stream>>,
}

impl BundleReader {
    pub(crate) fn remaining_data_extents(&self) -> io::Result<[usize; DATA_STREAMS as usize]> {
        let mut remaining = [0; DATA_STREAMS as usize];
        for (id, count) in remaining.iter_mut().enumerate() {
            let stream = self.stream(id as u8)?;
            *count = if inline_index(id as u8) {
                MAX_EXTENTS
                    - stream
                        .inline
                        .len()
                        .div_ceil(crate::page::PAGE_INDEX_ENTRY_BYTES)
            } else {
                MAX_EXTENTS - stream.extents.len()
            };
        }
        Ok(remaining)
    }
    pub(crate) fn stream_len(&self, id: u8) -> io::Result<u64> {
        Ok(self.stream(id)?.len)
    }

    pub(crate) fn row_count(&self) -> u64 {
        self.reference.row_count
    }

    pub(crate) fn has_complete_schema(&self) -> bool {
        self.streams.len() == usize::from(STREAMS)
    }

    pub(crate) fn open(path: &Path, reference: &BundleReference) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let end = reference.end()?;
        if end > file.metadata()?.len() {
            return Err(invalid("bundle snapshot is truncated"));
        }
        let mut magic = [0; 8];
        file.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(invalid("unsupported bundle format"));
        }
        // References strictly decrease in offset, depth and total read budget.
        // Decode each bounded table once and apply deltas oldest first.
        let mut current = reference.clone();
        let mut tables = Vec::with_capacity(reference.depth as usize);
        loop {
            file.seek(SeekFrom::Start(current.table_offset))?;
            let mut bytes = buffer(current.table_len as usize)?;
            file.read_exact(&mut bytes)?;
            if crc32fast::hash(&bytes) != current.checksum {
                return Err(invalid("bundle table checksum mismatch"));
            }
            let (streams, parent) = decode_table(&bytes, &current)?;
            tables.push(streams);
            match parent {
                Some(parent) => current = parent,
                None => break,
            }
        }
        let mut streams: BTreeMap<u8, Stream> = BTreeMap::new();
        for table in tables.into_iter().rev() {
            for (id, mut update) in table {
                if id < DATA_STREAMS {
                    let stream = streams.entry(id).or_default();
                    if stream.extents.len() + update.extents.len() > MAX_EXTENTS {
                        return Err(invalid("bundle extent chain exceeds its bound"));
                    }
                    if stream.inline.len() + update.inline.len() > INDEX_BYTES {
                        return Err(invalid("bundle index chain exceeds its bound"));
                    }
                    stream.len = stream
                        .len
                        .checked_add(update.len)
                        .ok_or_else(|| invalid("bundle stream length overflow"))?;
                    stream.extents.append(&mut update.extents);
                    stream.inline.append(&mut update.inline);
                } else {
                    streams.insert(id, update);
                }
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            reference: reference.clone(),
            streams: Arc::new(streams),
        })
    }

    pub(crate) fn read_stream(&self, id: u8) -> io::Result<Vec<u8>> {
        self.read_range(id, 0..self.stream(id)?.len)
    }

    pub(crate) fn read_range(&self, id: u8, range: Range<u64>) -> io::Result<Vec<u8>> {
        let stream = self.stream(id)?;
        if range.start > range.end || range.end > stream.len {
            return Err(invalid("bundle stream range is out of bounds"));
        }
        let len = usize::try_from(range.end - range.start)
            .map_err(|_| invalid("bundle range exceeds address space"))?;
        if inline_index(id) {
            return Ok(stream.inline[range.start as usize..range.end as usize].to_vec());
        }
        let mut output = buffer(len)?;
        let mut logical = 0;
        for extent in &stream.extents {
            let next = logical + u64::from(extent.len); // validated by decode_table
            let start = range.start.max(logical);
            let end = range.end.min(next);
            if start < end {
                let bytes = self.read_extent(extent)?;
                let destination = (start - range.start) as usize;
                let local = (start - logical) as usize;
                let count = (end - start) as usize;
                output[destination..destination + count]
                    .copy_from_slice(&bytes[local..local + count]);
            }
            logical = next;
            if logical >= range.end {
                break;
            }
        }
        Ok(output)
    }

    pub(crate) fn verify_all(&self) -> io::Result<()> {
        for stream in self.streams.values() {
            for extent in &stream.extents {
                self.read_extent(extent)?;
            }
        }
        Ok(())
    }

    fn stream(&self, id: u8) -> io::Result<&Stream> {
        self.streams
            .get(&id)
            .ok_or_else(|| invalid("missing bundle stream"))
    }

    fn read_extent(&self, extent: &Extent) -> io::Result<Vec<u8>> {
        // Tables bound each extent before allocation. Verify even a partial
        // selection against the whole extent, at most MAX_EXTENT_BYTES.
        let mut bytes = buffer(extent.len as usize)?;
        let mut file = self
            .file
            .lock()
            .map_err(|_| invalid("bundle reader lock poisoned"))?;
        file.seek(SeekFrom::Start(extent.offset))?;
        file.read_exact(&mut bytes)?;
        if crc32fast::hash(&bytes) != extent.checksum {
            return Err(invalid("bundle extent checksum mismatch"));
        }
        Ok(bytes)
    }
}

struct WriteState {
    file: BufWriter<File>,
    offset: u64,
    initial_rows: u64,
    streams: BTreeMap<u8, Stream>,
    updates: BTreeMap<u8, Stream>,
    parent: Option<BundleReference>,
    failed: bool,
    path: std::path::PathBuf,
}

pub(crate) struct BundleWriter(Mutex<WriteState>);

impl BundleWriter {
    pub(crate) fn create(path: &Path) -> io::Result<Self> {
        let mut file = BufWriter::with_capacity(
            64 * 1024,
            OpenOptions::new().write(true).create_new(true).open(path)?,
        );
        file.write_all(FILE_MAGIC)?;
        Ok(Self(Mutex::new(WriteState {
            file,
            offset: FILE_MAGIC.len() as u64,
            initial_rows: 0,
            streams: BTreeMap::new(),
            updates: BTreeMap::new(),
            parent: None,
            failed: false,
            path: path.to_owned(),
        })))
    }

    pub(crate) fn append(path: &Path, reference: &BundleReference) -> io::Result<Self> {
        let reader = BundleReader::open(path, reference)?;
        let file = OpenOptions::new().append(true).open(path)?;
        let offset = reference.end()?;
        if file.metadata()?.len() != offset {
            return Err(invalid("unpublished bundle tail; recover before appending"));
        }
        Ok(Self(Mutex::new(WriteState {
            file: BufWriter::with_capacity(64 * 1024, file),
            offset,
            initial_rows: reader.reference.row_count,
            streams: (*reader.streams).clone(),
            updates: BTreeMap::new(),
            parent: Some(reference.clone()),
            failed: false,
            path: path.to_owned(),
        })))
    }

    pub(crate) fn append_data(&self, id: u8, bytes: &[u8]) -> io::Result<()> {
        if id >= DATA_STREAMS {
            return Err(invalid("invalid bundle data stream"));
        }
        self.write_stream(id, bytes, false)
    }

    pub(crate) fn replace_metadata(&self, id: u8, bytes: &[u8]) -> io::Result<()> {
        if !(DATA_STREAMS..STREAMS).contains(&id) {
            return Err(invalid("invalid bundle metadata stream"));
        }
        self.write_stream(id, bytes, true)
    }

    fn write_stream(&self, id: u8, bytes: &[u8], replace: bool) -> io::Result<()> {
        let mut state = self
            .0
            .lock()
            .map_err(|_| invalid("bundle writer lock poisoned"))?;
        if state.failed {
            return Err(invalid(
                "bundle write failed; reopen at the committed snapshot",
            ));
        }
        if inline_index(id) {
            let current = state
                .streams
                .get(&id)
                .map_or(0, |stream| stream.inline.len());
            if bytes.len() > INDEX_BYTES - current {
                return Err(invalid("bundle index limit; rotate before appending"));
            }
            // Index bytes live in the checksummed table. Keeping them inline
            // avoids one seek/read for every tiny previous append on each open.
            let stream = state.streams.entry(id).or_default();
            stream.inline.extend_from_slice(bytes);
            stream.len += bytes.len() as u64;
            let update = state.updates.entry(id).or_default();
            update.inline.extend_from_slice(bytes);
            update.len += bytes.len() as u64;
            return Ok(());
        }
        let previous = if replace {
            None
        } else {
            state.streams.get(&id)
        };
        let count = previous.map_or(0, |stream| stream.extents.len());
        if count.saturating_add(bytes.len().div_ceil(MAX_EXTENT_BYTES)) > MAX_EXTENTS {
            return Err(invalid("bundle extent limit; rotate before appending"));
        }
        let len = previous
            .map_or(0, |stream| stream.len)
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("bundle stream length overflow"))?;
        state
            .offset
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("bundle offset overflow"))?;
        // A partial I/O failure makes this writer unusable. The previous table
        // and all of its extents remain intact for recovery.
        state.failed = true;
        let mut appended = Vec::with_capacity(bytes.len().div_ceil(MAX_EXTENT_BYTES));
        for chunk in bytes.chunks(MAX_EXTENT_BYTES) {
            let extent = Extent {
                offset: state.offset,
                len: chunk.len() as u32,
                checksum: crc32fast::hash(chunk),
            };
            crate::durability::checkpoint("bundle_append_extent", &state.path)?;
            state.file.write_all(chunk)?;
            state.offset += chunk.len() as u64;
            appended.push(extent);
        }
        let stream = state.streams.entry(id).or_default();
        if replace {
            stream.extents.clear();
        }
        stream.extents.extend_from_slice(&appended);
        stream.len = len;
        let update = state.updates.entry(id).or_default();
        if replace {
            update.extents.clear();
            update.len = 0;
        }
        update.extents.extend(appended);
        update.len += bytes.len() as u64; // bounded by the complete stream above
        state.failed = false;
        Ok(())
    }

    /// Flush userspace buffers and return an immutable table reference. This
    /// does not synchronize the device: the containing publication must order
    /// and persist the artifact before committing this reference to the catalog.
    pub(crate) fn finish(self, row_count: u64) -> io::Result<BundleReference> {
        let mut state = self
            .0
            .into_inner()
            .map_err(|_| invalid("bundle writer lock poisoned"))?;
        if state.failed || row_count < state.initial_rows || row_count > MAX_ROWS {
            return Err(invalid("invalid bundle snapshot completion"));
        }
        let delta = state
            .parent
            .as_ref()
            .filter(|parent| parent.depth < MAX_TABLE_DEPTH)
            .map(|parent| encode_table(row_count, &state.updates, Some(parent)))
            .transpose()?;
        let (bytes, depth, chain_bytes) = match (delta, state.parent.as_ref()) {
            (Some(bytes), Some(parent))
                if u64::from(parent.chain_bytes) + bytes.len() as u64
                    <= u64::from(MAX_TABLE_BYTES) =>
            {
                let chain_bytes = parent.chain_bytes + bytes.len() as u32;
                (bytes, parent.depth + 1, chain_bytes)
            }
            _ => {
                let bytes = encode_table(row_count, &state.streams, None)?;
                let len = bytes.len() as u32;
                (bytes, 1, len)
            }
        };
        let reference = BundleReference {
            row_count,
            table_offset: state.offset,
            table_len: u32::try_from(bytes.len())
                .map_err(|_| invalid("bundle table is too large"))?,
            checksum: crc32fast::hash(&bytes),
            depth,
            chain_bytes,
        };
        reference.end()?;
        crate::durability::checkpoint("bundle_append_table", &state.path)?;
        state.file.write_all(&bytes)?;
        state.file.flush()?;
        Ok(reference)
    }
}

fn encode_table(
    rows: u64,
    streams: &BTreeMap<u8, Stream>,
    parent: Option<&BundleReference>,
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(TABLE_MAGIC);
    bytes.extend_from_slice(&rows.to_le_bytes());
    bytes.extend_from_slice(&(streams.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&u32::from(parent.is_some()).to_le_bytes());
    if let Some(parent) = parent {
        bytes.extend_from_slice(&parent.table_offset.to_le_bytes());
        bytes.extend_from_slice(&parent.table_len.to_le_bytes());
        bytes.extend_from_slice(&parent.checksum.to_le_bytes());
        bytes.extend_from_slice(&parent.row_count.to_le_bytes());
        bytes.extend_from_slice(&parent.depth.to_le_bytes());
        bytes.extend_from_slice(&parent.chain_bytes.to_le_bytes());
    }
    for (&id, stream) in streams {
        bytes.extend_from_slice(&[id, u8::from(inline_index(id)), 0, 0]);
        bytes.extend_from_slice(&stream.len.to_le_bytes());
        bytes.extend_from_slice(&(stream.extents.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&stream.inline);
        for extent in &stream.extents {
            bytes.extend_from_slice(&extent.offset.to_le_bytes());
            bytes.extend_from_slice(&extent.len.to_le_bytes());
            bytes.extend_from_slice(&extent.checksum.to_le_bytes());
        }
    }
    if bytes.len() > MAX_TABLE_BYTES as usize {
        return Err(invalid("bundle table exceeds its bound"));
    }
    Ok(bytes)
}

fn decode_table(
    bytes: &[u8],
    reference: &BundleReference,
) -> io::Result<(BTreeMap<u8, Stream>, Option<BundleReference>)> {
    let mut cursor = Cursor(bytes);
    if &cursor.take::<8>()? != TABLE_MAGIC || cursor.u64()? != reference.row_count {
        return Err(invalid("bundle table identity mismatch"));
    }
    let count = cursor.u32()?;
    let flags = cursor.u32()?;
    if count > u32::from(STREAMS) || flags > 1 {
        return Err(invalid("invalid bundle stream count or flags"));
    }
    let parent = if flags == 1 {
        let parent = BundleReference {
            table_offset: cursor.u64()?,
            table_len: cursor.u32()?,
            checksum: cursor.u32()?,
            row_count: cursor.u64()?,
            depth: cursor.u32()?,
            chain_bytes: cursor.u32()?,
        };
        if parent.end()? > reference.table_offset
            || parent.row_count > reference.row_count
            || parent.depth + 1 != reference.depth
            || parent.chain_bytes.checked_add(reference.table_len) != Some(reference.chain_bytes)
        {
            return Err(invalid("invalid bundle parent reference"));
        }
        Some(parent)
    } else {
        if reference.depth != 1 || reference.chain_bytes != reference.table_len {
            return Err(invalid("invalid full bundle table reference"));
        }
        None
    };
    let payload_start = parent
        .as_ref()
        .map(BundleReference::end)
        .transpose()?
        .unwrap_or(FILE_MAGIC.len() as u64);
    let mut streams = BTreeMap::new();
    let mut physical = Vec::new();
    for _ in 0..count {
        let [id, a, b, c] = cursor.take::<4>()?;
        if id >= STREAMS
            || a != u8::from(inline_index(id))
            || b != 0
            || c != 0
            || streams.contains_key(&id)
        {
            return Err(invalid("invalid or duplicated bundle stream"));
        }
        let len = cursor.u64()?;
        let count = cursor.u32()? as usize;
        if inline_index(id) {
            if count != 0 || len > INDEX_BYTES as u64 {
                return Err(invalid("invalid inline bundle index bound"));
            }
            let (bytes, tail) = cursor
                .0
                .split_at_checked(len as usize)
                .ok_or_else(|| invalid("truncated inline bundle index"))?;
            cursor.0 = tail;
            streams.insert(
                id,
                Stream {
                    len,
                    extents: Vec::new(),
                    inline: bytes.to_vec(),
                },
            );
            continue;
        }
        if count > MAX_EXTENTS || count > cursor.0.len() / 16 {
            return Err(invalid("bundle extent count exceeds its bound"));
        }
        let mut extents = Vec::with_capacity(count);
        let mut total = 0u64;
        let mut previous_end = payload_start;
        for _ in 0..count {
            let offset = cursor.u64()?;
            let len = cursor.u32()?;
            let checksum = cursor.u32()?;
            let end = offset
                .checked_add(u64::from(len))
                .ok_or_else(|| invalid("bundle extent overflow"))?;
            if offset < previous_end
                || len == 0
                || len as usize > MAX_EXTENT_BYTES
                || end > reference.table_offset
            {
                return Err(invalid("bundle extent is out of bounds or out of order"));
            }
            previous_end = end;
            total = total
                .checked_add(u64::from(len))
                .ok_or_else(|| invalid("bundle logical length overflow"))?;
            extents.push(Extent {
                offset,
                len,
                checksum,
            });
            physical.push(offset..end);
        }
        if total != len {
            return Err(invalid("bundle stream length mismatch"));
        }
        streams.insert(
            id,
            Stream {
                len,
                extents,
                inline: Vec::new(),
            },
        );
    }
    if !cursor.0.is_empty() {
        return Err(invalid("trailing bundle table bytes"));
    }
    physical.sort_unstable_by_key(|range| range.start);
    if physical.windows(2).any(|pair| pair[0].end > pair[1].start) {
        return Err(invalid("bundle extents overlap"));
    }
    Ok((streams, parent))
}

struct Cursor<'a>(&'a [u8]);
impl Cursor<'_> {
    fn take<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let (head, tail) = self
            .0
            .split_at_checked(N)
            .ok_or_else(|| invalid("truncated bundle table"))?;
        self.0 = tail;
        head.try_into()
            .map_err(|_| invalid("truncated bundle field"))
    }
    fn u32(&mut self) -> io::Result<u32> {
        self.take().map(u32::from_le_bytes)
    }
    fn u64(&mut self) -> io::Result<u64> {
        self.take().map(u64::from_le_bytes)
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn buffer(len: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(len).map_err(io::Error::other)?;
    bytes.resize(len, 0);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create(path: &Path) -> BundleReference {
        let writer = BundleWriter::create(path).unwrap();
        writer.append_data(0, b"first").unwrap();
        writer.replace_metadata(28, b"old index").unwrap();
        writer.finish(2).unwrap()
    }

    #[test]
    fn snapshots_preserve_data_and_metadata_across_appends() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let first = create(&path);
        let before = fs::read(&path).unwrap();
        let old = BundleReader::open(&path, &first).unwrap();
        let writer = BundleWriter::append(&path, &first).unwrap();
        writer.append_data(0, b" second").unwrap();
        writer.replace_metadata(28, b"new index").unwrap();
        let second = writer.finish(3).unwrap();
        let new = BundleReader::open(&path, &second).unwrap();
        assert!(fs::read(&path).unwrap().starts_with(&before));
        assert_eq!(old.read_stream(0).unwrap(), b"first");
        assert_eq!(old.read_stream(28).unwrap(), b"old index");
        assert_eq!(new.read_stream(0).unwrap(), b"first second");
        assert_eq!(new.read_range(0, 3..8).unwrap(), b"st se");
        assert_eq!(new.read_stream(28).unwrap(), b"new index");
        assert!(new.read_stream(32).is_err());
        assert!(new.read_range(0, 0..u64::MAX).is_err());
        old.verify_all().unwrap();
        new.verify_all().unwrap();
        // Reopen the old snapshot and explicitly discard only the newer suffix.
        assert!(BundleWriter::append(&path, &first).is_err());
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(first.end().unwrap())
            .unwrap();
        BundleReader::open(&path, &first)
            .unwrap()
            .verify_all()
            .unwrap();
        BundleWriter::append(&path, &first)
            .unwrap()
            .finish(2)
            .unwrap();
    }

    #[test]
    fn tiny_appends_do_not_repeat_all_previous_metadata() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let mut writer = BundleWriter::create(&path).unwrap();
        let mut reference = None;
        for rows in 1..=1024 {
            writer.append_data(0, &[7; 24]).unwrap();
            writer.append_data(1, &[9; 24]).unwrap();
            writer.replace_metadata(28, &[0; 16]).unwrap();
            let snapshot = writer.finish(rows).unwrap();
            writer = BundleWriter::append(&path, &snapshot).unwrap();
            reference = Some(snapshot);
        }
        let reader = BundleReader::open(&path, &reference.unwrap()).unwrap();
        assert_eq!(reader.read_stream(0).unwrap(), vec![7; 1024 * 24]);
        assert_eq!(reader.read_stream(1).unwrap(), vec![9; 1024 * 24]);
        reader.verify_all().unwrap();
        assert!(fs::metadata(path).unwrap().len() < 1024 * 1024);
    }

    #[test]
    fn table_chains_flatten_without_losing_old_snapshots() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let mut writer = BundleWriter::create(&path).unwrap();
        let mut snapshots = Vec::new();
        for rows in 1..=u64::from(MAX_TABLE_DEPTH * 2 + 3) {
            writer.append_data(0, &[rows as u8]).unwrap();
            writer.append_data(14, &[rows as u8; 24]).unwrap();
            writer.replace_metadata(28, &[rows as u8]).unwrap();
            let reference = writer.finish(rows).unwrap();
            assert_eq!(reference.depth, (rows as u32 - 1) % MAX_TABLE_DEPTH + 1);
            assert!(reference.chain_bytes <= MAX_TABLE_BYTES);
            writer = BundleWriter::append(&path, &reference).unwrap();
            snapshots.push(reference);
        }
        for reference in snapshots {
            let reader = BundleReader::open(&path, &reference).unwrap();
            let expected: Vec<_> = (1..=reference.row_count).map(|row| row as u8).collect();
            assert_eq!(reader.read_stream(0).unwrap(), expected);
            let index: Vec<_> = (1..=reference.row_count)
                .flat_map(|row| [row as u8; 24])
                .collect();
            assert_eq!(reader.read_stream(14).unwrap(), index);
            assert_eq!(reader.read_stream(28).unwrap(), [reference.row_count as u8]);
            reader.verify_all().unwrap();
        }
    }

    #[test]
    fn inline_indexes_enforce_encoding_and_cumulative_bounds() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let writer = BundleWriter::create(&path).unwrap();
        assert!(writer.append_data(14, &vec![0; INDEX_BYTES + 1]).is_err());
        writer.append_data(14, &vec![7; INDEX_BYTES]).unwrap();
        assert!(writer.append_data(14, b"x").is_err());
        let first = writer.finish(1).unwrap();
        let original = fs::read(&path).unwrap();
        let reader = BundleReader::open(&path, &first).unwrap();
        assert_eq!(reader.read_stream(14).unwrap(), vec![7; INDEX_BYTES]);
        for case in 0..3 {
            let mut damaged = original.clone();
            let table = &mut damaged[first.table_offset as usize..];
            match case {
                0 => table[25] = 0,
                1 => table[28..36].copy_from_slice(&(INDEX_BYTES as u64 + 1).to_le_bytes()),
                2 => table[36..40].copy_from_slice(&1u32.to_le_bytes()),
                _ => unreachable!(),
            }
            let mut forged = first.clone();
            forged.checksum = crc32fast::hash(table);
            fs::write(&path, &damaged).unwrap();
            assert!(BundleReader::open(&path, &forged).is_err(), "case {case}");
        }
        fs::write(&path, &original).unwrap();
        // Each table is individually bounded, but the appended index would
        // exceed the complete stream's limit. Recompute checksums to exercise
        // the chain check independently of accidental corruption detection.
        let updates = BTreeMap::from([(
            14,
            Stream {
                len: 1,
                extents: vec![],
                inline: vec![9],
            },
        )]);
        let bytes = encode_table(2, &updates, Some(&first)).unwrap();
        let second = BundleReference {
            row_count: 2,
            table_offset: first.end().unwrap(),
            table_len: bytes.len() as u32,
            checksum: crc32fast::hash(&bytes),
            depth: 2,
            chain_bytes: first.chain_bytes + bytes.len() as u32,
        };
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(BundleReader::open(&path, &second).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        assert_eq!(reader.read_stream(14).unwrap(), vec![7; INDEX_BYTES]);
    }

    #[test]
    fn parent_tables_and_valid_checksum_delta_bounds_are_verified() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let first = create(&path);
        let writer = BundleWriter::append(&path, &first).unwrap();
        writer.append_data(0, b"next").unwrap();
        let second = writer.finish(3).unwrap();
        let original = fs::read(&path).unwrap();
        // Parent table bytes remain required even when the newest table's CRC
        // is intact. Corruption is reported without repairing or truncating.
        for byte in first.table_offset..first.end().unwrap() {
            let mut damaged = original.clone();
            damaged[byte as usize] ^= 1;
            fs::write(&path, &damaged).unwrap();
            assert!(BundleReader::open(&path, &second).is_err());
            assert_eq!(fs::read(&path).unwrap(), damaged);
        }
        for case in 0..10 {
            let mut damaged = original.clone();
            let table = &mut damaged[second.table_offset as usize..];
            match case {
                0 => table[24..32].copy_from_slice(&second.table_offset.to_le_bytes()),
                1 => table[32..36].copy_from_slice(&u32::MAX.to_le_bytes()),
                2 => table[36] ^= 1, // parent checksum
                3 => table[40..48].copy_from_slice(&4u64.to_le_bytes()),
                4 => table[48..52].copy_from_slice(&second.depth.to_le_bytes()),
                5 => table[52..56].copy_from_slice(&u32::MAX.to_le_bytes()),
                6 => table[72..80].copy_from_slice(&(first.end().unwrap() - 1).to_le_bytes()),
                7 => table[80..84].copy_from_slice(&u32::MAX.to_le_bytes()),
                8 => table[20] = 2,
                9 => table[60..68].copy_from_slice(&u64::MAX.to_le_bytes()),
                _ => unreachable!(),
            }
            let mut forged = second.clone();
            forged.checksum = crc32fast::hash(table);
            fs::write(&path, &damaged).unwrap();
            assert!(BundleReader::open(&path, &forged).is_err(), "case {case}");
            assert_eq!(fs::read(&path).unwrap(), damaged);
        }
        for case in 0..5 {
            let mut forged = second.clone();
            match case {
                0 => forged.depth = 0,
                1 => forged.depth = MAX_TABLE_DEPTH + 1,
                2 => forged.chain_bytes = MAX_TABLE_BYTES + 1,
                3 => forged.chain_bytes = forged.table_len - 1,
                4 => forged.row_count = MAX_ROWS + 1,
                _ => unreachable!(),
            }
            assert!(forged.end().is_err(), "reference case {case}");
        }
    }

    #[test]
    fn selections_cross_bounded_extents_and_concurrent_columns() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let writer = BundleWriter::create(&path).unwrap();
        let data: Vec<_> = (0..MAX_EXTENT_BYTES + 31)
            .map(|i| (i % 251) as u8)
            .collect();
        std::thread::scope(|scope| {
            for id in 0..14 {
                let writer = &writer;
                let data = &data;
                scope.spawn(move || writer.append_data(id, data).unwrap());
            }
        });
        let reference = writer.finish(5).unwrap();
        let reader = BundleReader::open(&path, &reference).unwrap();
        for id in 0..14 {
            assert_eq!(reader.read_stream(id).unwrap(), data);
            let start = MAX_EXTENT_BYTES - 9;
            let end = MAX_EXTENT_BYTES + 9;
            assert_eq!(
                reader.read_range(id, start as u64..end as u64).unwrap(),
                data[start..end]
            );
        }
        reader.verify_all().unwrap();
    }

    #[test]
    fn truncations_and_mutations_fail_without_changing_the_artifact() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let reference = create(&path);
        let original = fs::read(&path).unwrap();
        for end in 0..original.len() {
            fs::write(&path, &original[..end]).unwrap();
            assert!(
                BundleReader::open(&path, &reference).is_err(),
                "length {end}"
            );
            assert_eq!(fs::read(&path).unwrap(), original[..end]);
        }
        for byte in 0..original.len() {
            let mut damaged = original.clone();
            damaged[byte] ^= 1;
            fs::write(&path, &damaged).unwrap();
            let result =
                BundleReader::open(&path, &reference).and_then(|reader| reader.verify_all());
            assert!(result.is_err(), "byte {byte}");
            assert_eq!(fs::read(&path).unwrap(), damaged);
        }
    }

    #[test]
    fn valid_checksums_do_not_bypass_table_bounds() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let reference = create(&path);
        let original = fs::read(&path).unwrap();
        for case in 0..10 {
            let mut bytes = original.clone();
            let table = &mut bytes[reference.table_offset as usize..];
            match case {
                0 => table[16..20].copy_from_slice(&u32::MAX.to_le_bytes()),
                1 => table[24] = STREAMS,
                2 => table[25] = 1,
                3 => table[28..36].copy_from_slice(&u64::MAX.to_le_bytes()),
                4 => table[36..40].copy_from_slice(&u32::MAX.to_le_bytes()),
                5 => table[40..48].copy_from_slice(&0u64.to_le_bytes()),
                6 => table[48..52].copy_from_slice(&u32::MAX.to_le_bytes()),
                7 => table[56] = 0, // duplicated stream id
                8 => table[72..80].copy_from_slice(&8u64.to_le_bytes()), // overlapping payload
                9 => table[20] = 1,
                _ => unreachable!(),
            }
            let mut forged = reference.clone();
            forged.checksum = crc32fast::hash(table);
            fs::write(&path, &bytes).unwrap();
            assert!(BundleReader::open(&path, &forged).is_err(), "case {case}");
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
        let mut forged = reference;
        forged.table_offset = u64::MAX;
        assert!(forged.end().is_err());
    }

    #[test]
    fn interrupted_extent_and_table_writes_leave_the_checkpoint_readable() {
        for failure in 0..3 {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("bundle");
            let first = create(&path);
            let old_bytes = fs::read(&path).unwrap();
            let writer = BundleWriter::append(&path, &first).unwrap();
            crate::durability::inject_failure(failure);
            let result = writer.append_data(0, &vec![17; MAX_EXTENT_BYTES + 9]);
            if failure < 2 {
                assert!(result.is_err());
                assert!(writer.append_data(0, b"retry").is_err());
            } else {
                result.unwrap();
            }
            assert!(writer.finish(3).is_err());
            let events = crate::durability::take_events();
            assert_eq!(events.len(), failure + 1);
            let old = BundleReader::open(&path, &first).unwrap();
            old.verify_all().unwrap();
            assert_eq!(old.read_stream(0).unwrap(), b"first");
            assert!(fs::read(&path).unwrap().starts_with(&old_bytes));
            // Storage recovery must publish the old manifest before this trim.
            OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(first.end().unwrap())
                .unwrap();
            let writer = BundleWriter::append(&path, &first).unwrap();
            writer.append_data(0, b" retry").unwrap();
            let recovered = writer.finish(3).unwrap();
            assert_eq!(
                BundleReader::open(&path, &recovered)
                    .unwrap()
                    .read_stream(0)
                    .unwrap(),
                b"first retry"
            );
        }
    }

    #[test]
    fn snapshots_match_an_independent_model_under_mixed_updates() {
        for seed in [1u64, 17, 919, u64::MAX] {
            let tmp = TempDir::new().unwrap();
            let path = tmp.path().join("bundle");
            let mut rng = seed;
            let mut next = || {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                rng
            };
            let mut writer = BundleWriter::create(&path).unwrap();
            let mut expected: BTreeMap<u8, Vec<u8>> = BTreeMap::new();
            let mut snapshots = Vec::new();
            for step in 0..70 {
                for _ in 0..5 {
                    let id = (next() % u64::from(STREAMS)) as u8;
                    let bytes: Vec<_> = (0..next() % 65).map(|_| next() as u8).collect();
                    if id < DATA_STREAMS {
                        writer.append_data(id, &bytes).unwrap();
                        expected.entry(id).or_default().extend(bytes);
                    } else {
                        writer.replace_metadata(id, &bytes).unwrap();
                        expected.insert(id, bytes);
                    }
                }
                let reference = writer.finish(step + 1).unwrap();
                snapshots.push((
                    BundleReader::open(&path, &reference).unwrap(),
                    expected.clone(),
                ));
                for (reader, model) in &snapshots {
                    for (&id, bytes) in model {
                        assert_eq!(reader.read_stream(id).unwrap(), *bytes);
                        let middle = bytes.len() / 2;
                        assert_eq!(
                            reader
                                .read_range(id, middle as u64..bytes.len() as u64)
                                .unwrap(),
                            bytes[middle..]
                        );
                    }
                }
                writer = BundleWriter::append(&path, &reference).unwrap();
            }
        }
    }

    #[test]
    fn a_reordered_data_prefix_is_invalid_even_with_a_valid_table_checksum() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let writer = BundleWriter::create(&path).unwrap();
        writer.append_data(0, b"first").unwrap();
        writer.append_data(0, b"second").unwrap();
        let mut reference = writer.finish(2).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        let table = &mut bytes[reference.table_offset as usize..];
        let first = table[40..56].to_vec();
        table.copy_within(56..72, 40);
        table[56..72].copy_from_slice(&first);
        reference.checksum = crc32fast::hash(table);
        fs::write(&path, &bytes).unwrap();
        assert!(BundleReader::open(&path, &reference).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[cfg(unix)]
    #[test]
    fn an_open_snapshot_keeps_its_file_after_path_replacement() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let first = create(&path);
        let reader = BundleReader::open(&path, &first).unwrap();
        fs::rename(&path, tmp.path().join("old-bundle")).unwrap();
        let writer = BundleWriter::create(&path).unwrap();
        writer.append_data(0, b"replacement").unwrap();
        let second = writer.finish(1).unwrap();
        fs::remove_file(tmp.path().join("old-bundle")).unwrap();
        assert_eq!(reader.read_stream(0).unwrap(), b"first");
        assert_eq!(
            BundleReader::open(&path, &second)
                .unwrap()
                .read_stream(0)
                .unwrap(),
            b"replacement"
        );
    }

    #[test]
    fn invalid_appends_keep_the_writer_usable_and_prefix_intact() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let first = create(&path);
        let bytes = fs::read(&path).unwrap();
        assert!(BundleWriter::create(&path).is_err());
        let writer = BundleWriter::append(&path, &first).unwrap();
        assert!(writer.append_data(DATA_STREAMS, b"bad").is_err());
        assert!(writer.replace_metadata(0, b"bad").is_err());
        assert!(writer.replace_metadata(STREAMS, b"bad").is_err());
        for _ in 1..MAX_EXTENTS {
            writer.append_data(0, b"x").unwrap();
        }
        assert!(writer.append_data(0, b"overflow").is_err());
        let last = writer.finish(3).unwrap();
        assert!(fs::read(&path).unwrap().starts_with(&bytes));
        let reader = BundleReader::open(&path, &last).unwrap();
        assert_eq!(reader.read_stream(0).unwrap().len(), 5 + MAX_EXTENTS - 1);
        reader.verify_all().unwrap();
    }
}
