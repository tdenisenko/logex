//! Immutable compressed-artifact snapshots, committed by the storage catalog.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

const FILE_MAGIC: &[u8; 8] = b"LXBND005";
const TABLE_MAGIC: &[u8; 8] = b"LXBT0005";
// Column payloads and page-index entries are append-only; nullable bitmaps replace.
pub(crate) const DATA_STREAMS: u8 = 28;
const STREAMS: u8 = 33;
pub(crate) const MAX_EXTENT_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_EXTENTS: usize = 4096;
const MAX_TABLE_BYTES: u32 = 4 * 1024 * 1024;
const GROUP_BITS: u32 = 3;
const MAX_TABLE_DEPTH: u32 =
    (64 / GROUP_BITS) * ((1 << GROUP_BITS) - 1) + (1 << (64 % GROUP_BITS)) - 1;
pub(crate) const MAX_ROWS: u64 = MAX_EXTENTS as u64 * 16_384;
const INDEX_BYTES: usize = MAX_EXTENTS * crate::page::PAGE_INDEX_ENTRY_BYTES;
const READ_AHEAD_BYTES: usize = 64 * 1024;

fn inline_index(id: u8) -> bool {
    (14..28).contains(&id)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleReference {
    pub sequence: u64,
    pub row_count: u64,
    pub table_offset: u64,
    /// Stored record bytes, including the codec and decoded-length header.
    pub table_len: u32,
    pub checksum: u32,
    pub depth: u32,
    /// Sum of decoded table bytes; compression never expands the parse budget.
    pub chain_bytes: u32,
}

impl BundleReference {
    pub(crate) fn end(&self) -> io::Result<u64> {
        if self.sequence == 0
            || self.row_count > MAX_ROWS
            || self.table_offset < FILE_MAGIC.len() as u64
            || self.table_len < 5
            || self.table_len > MAX_TABLE_BYTES + 5
            || self.depth == 0
            || self.depth > MAX_TABLE_DEPTH
            || self.depth != table_depth(self.sequence)
            || self.chain_bytes < 32
            || self.chain_bytes > MAX_TABLE_BYTES
        {
            return Err(invalid("invalid bundle reference"));
        }
        self.table_offset
            .checked_add(u64::from(self.table_len))
            .ok_or_else(|| invalid("bundle reference overflow"))
    }
}

// Tables form the base-8 decomposition of the publication sequence. A carry
// summarizes only its group, leaving older groups and snapshots immutable.
fn table_depth(mut sequence: u64) -> u32 {
    let mut depth = 0;
    while sequence != 0 {
        depth += (sequence & ((1 << GROUP_BITS) - 1)) as u32;
        sequence >>= GROUP_BITS;
    }
    depth
}

fn table_span(sequence: u64) -> u64 {
    1 << (sequence.trailing_zeros() / GROUP_BITS * GROUP_BITS)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Extent {
    offset: u64,
    len: u32,
    checksum: u32,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Stream {
    len: u64,
    extents: Vec<Extent>,
    inline: Vec<u8>,
}

#[derive(Debug)]
struct StreamBoundary {
    len: u64,
    extents: usize,
    inline: usize,
}

#[derive(Debug)]
struct GroupBase {
    reference: BundleReference,
    data: BTreeMap<u8, StreamBoundary>,
    metadata: BTreeMap<u8, Stream>,
}

impl GroupBase {
    fn capture(reference: BundleReference, streams: &BTreeMap<u8, Stream>) -> Self {
        let mut data = BTreeMap::new();
        let mut metadata = BTreeMap::new();
        for (&id, stream) in streams {
            if id < DATA_STREAMS {
                data.insert(
                    id,
                    StreamBoundary {
                        len: stream.len,
                        extents: stream.extents.len(),
                        inline: stream.inline.len(),
                    },
                );
            } else {
                metadata.insert(id, stream.clone());
            }
        }
        Self {
            reference,
            data,
            metadata,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BundleReader {
    file: Arc<Mutex<ReadWindow>>,
    reference: BundleReference,
    streams: Arc<BTreeMap<u8, Stream>>,
    group_base: Option<Arc<GroupBase>>,
}

#[derive(Debug)]
struct ReadWindow {
    file: File,
    offset: u64,
    bytes: Vec<u8>,
    logical_ends: BTreeMap<u8, Vec<u64>>,
}

impl ReadWindow {
    fn read_extent(
        &mut self,
        extent: &Extent,
        snapshot_end: u64,
        read_ahead: bool,
    ) -> io::Result<Vec<u8>> {
        if !read_ahead {
            // Preserve the direct path for large or physically isolated pages.
            // Reading unrelated columns would add copying without saving I/O.
            let mut bytes = buffer(extent.len as usize)?;
            self.file.seek(SeekFrom::Start(extent.offset))?;
            self.file.read_exact(&mut bytes)?;
            if crc32fast::hash(&bytes) != extent.checksum {
                return Err(invalid("bundle extent checksum mismatch"));
            }
            return Ok(bytes);
        }
        let end = extent.offset + u64::from(extent.len); // validated by decode_table
        if extent.offset < self.offset || end > self.offset + self.bytes.len() as u64 {
            // Small pages are interleaved with other columns and table records.
            // Read a bounded physical window instead of seeking for every page
            // header and payload. Large extents read exactly their own bytes.
            let offset = extent.offset / READ_AHEAD_BYTES as u64 * READ_AHEAD_BYTES as u64;
            let end = offset
                .saturating_add(READ_AHEAD_BYTES as u64)
                .max(end)
                .min(snapshot_end);
            let len = (end - offset) as usize;
            // Invalidate before I/O: a short read must not expose a partly
            // overwritten old cache on retry. Never read an unpublished suffix.
            self.bytes.clear();
            self.offset = offset;
            let mut bytes = buffer(len)?;
            self.file.seek(SeekFrom::Start(offset))?;
            self.file.read_exact(&mut bytes)?;
            self.bytes = bytes;
        }
        let start = (extent.offset - self.offset) as usize;
        let bytes = &self.bytes[start..start + extent.len as usize];
        if crc32fast::hash(bytes) != extent.checksum {
            return Err(invalid("bundle extent checksum mismatch"));
        }
        Ok(bytes.to_vec())
    }
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

    pub(crate) fn reference(&self) -> &BundleReference {
        &self.reference
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
            let decoded = decode_record(&bytes, current.chain_bytes)?;
            let (streams, parent) = decode_table(&decoded, &current)?;
            tables.push((current.clone(), streams));
            match parent {
                Some(parent) => current = parent,
                None => break,
            }
        }
        let next_sequence = reference.sequence.checked_add(1).unwrap_or(1);
        let base_sequence = next_sequence - table_span(next_sequence);
        let mut group_base = None;
        let mut streams: BTreeMap<u8, Stream> = BTreeMap::new();
        for (table_reference, table) in tables.into_iter().rev() {
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
            if base_sequence != reference.sequence && table_reference.sequence == base_sequence {
                // Capture only the lengths at the next carry's already-verified
                // boundary. Reopening the old prefix would duplicate table I/O
                // and would need another file identity check after inspection.
                group_base = Some(Arc::new(GroupBase::capture(table_reference, &streams)));
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(ReadWindow {
                file,
                offset: 0,
                bytes: Vec::new(),
                logical_ends: BTreeMap::new(),
            })),
            reference: reference.clone(),
            streams: Arc::new(streams),
            group_base,
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
        let (first, mut logical) = if range.start != 0 && stream.extents.len() >= 32 {
            // Page selections must not rescan every preceding extent. Build
            // offsets only for selected streams; full scans and startup retain
            // the compact extent layout and allocate no lookup table.
            let mut file = self
                .file
                .lock()
                .map_err(|_| invalid("bundle reader lock poisoned"))?;
            let ends = match file.logical_ends.entry(id) {
                std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::btree_map::Entry::Vacant(entry) => {
                    let mut ends = Vec::new();
                    ends.try_reserve_exact(stream.extents.len())
                        .map_err(|_| invalid("bundle lookup allocation failed"))?;
                    let mut end = 0;
                    for extent in &stream.extents {
                        // The immutable table already bounds the sum and count.
                        end += u64::from(extent.len);
                        ends.push(end);
                    }
                    entry.insert(ends)
                }
            };
            let first = ends.partition_point(|&end| end <= range.start);
            let logical = first.checked_sub(1).map_or(0, |previous| ends[previous]);
            (first, logical)
        } else {
            (0, 0)
        };
        for (index, extent) in stream.extents.iter().enumerate().skip(first) {
            let next = logical + u64::from(extent.len); // validated by decode_table
            let start = range.start.max(logical);
            let end = range.end.min(next);
            if start < end {
                let bytes = self.read_extent(&stream.extents, index)?;
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
        // An explicit integrity check must inspect the backing file again,
        // even if a previous query cached valid bytes from this snapshot.
        self.file
            .lock()
            .map_err(|_| invalid("bundle reader lock poisoned"))?
            .bytes
            .clear();
        for stream in self.streams.values() {
            for index in 0..stream.extents.len() {
                self.read_extent(&stream.extents, index)?;
            }
        }
        Ok(())
    }

    fn stream(&self, id: u8) -> io::Result<&Stream> {
        self.streams
            .get(&id)
            .ok_or_else(|| invalid("missing bundle stream"))
    }

    fn read_extent(&self, extents: &[Extent], index: usize) -> io::Result<Vec<u8>> {
        // Tables bound each extent before allocation. Verify even a partial
        // selection against the whole extent, at most MAX_EXTENT_BYTES.
        let extent = &extents[index];
        // Table validation orders physical extents. Read ahead only where
        // nearby extents of this column can reuse the physical window.
        let read_ahead = extents
            .get(index + 1)
            .is_some_and(|next| next.offset - extent.offset < 4 * 1024);
        let mut file = self
            .file
            .lock()
            .map_err(|_| invalid("bundle reader lock poisoned"))?;
        file.read_extent(extent, self.reference.end()?, read_ahead)
    }
}

struct WriteState {
    file: BufWriter<File>,
    offset: u64,
    initial_rows: u64,
    streams: BTreeMap<u8, Stream>,
    updates: BTreeMap<u8, Stream>,
    parent: Option<BundleReference>,
    group_base: Option<Arc<GroupBase>>,
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
            group_base: None,
            failed: false,
            path: path.to_owned(),
        })))
    }

    #[cfg(test)]
    pub(crate) fn append(path: &Path, reference: &BundleReference) -> io::Result<Self> {
        let reader = BundleReader::open(path, reference)?;
        Self::append_inspected(path, reader)
    }

    /// Reuse the immutable table checked during this append's preflight. Bind
    /// the writable handle to that same file before modifying any bytes.
    pub(crate) fn append_inspected(path: &Path, reader: BundleReader) -> io::Result<Self> {
        // Unix supplies stable open-file identity. Keep a fresh table check on
        // other platforms rather than assuming a pathname still names the file.
        #[cfg(not(unix))]
        let reader = BundleReader::open(path, &reader.reference)?;
        let file = OpenOptions::new().append(true).open(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let inspected = reader
                .file
                .lock()
                .map_err(|_| invalid("bundle reader lock poisoned"))?
                .file
                .metadata()?;
            let opened = file.metadata()?;
            if inspected.dev() != opened.dev() || inspected.ino() != opened.ino() {
                return Err(invalid("bundle file changed after inspection"));
            }
        }
        let offset = reader.reference.end()?;
        if file.metadata()?.len() != offset {
            return Err(invalid("unpublished bundle tail; recover before appending"));
        }
        Ok(Self(Mutex::new(WriteState {
            file: BufWriter::with_capacity(64 * 1024, file),
            offset,
            initial_rows: reader.reference.row_count,
            streams: Arc::try_unwrap(reader.streams).unwrap_or_else(|streams| (*streams).clone()),
            updates: BTreeMap::new(),
            parent: Some(reader.reference),
            group_base: reader.group_base,
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
        let mut sequence = state
            .parent
            .as_ref()
            .map_or(1, |parent| parent.sequence.checked_add(1).unwrap_or(1));
        let base_sequence = sequence - table_span(sequence);
        let parent = if base_sequence == 0 {
            None
        } else if state
            .parent
            .as_ref()
            .is_some_and(|parent| parent.sequence == base_sequence)
        {
            state.parent.as_ref()
        } else {
            Some(
                state
                    .group_base
                    .as_ref()
                    .map(|base| &base.reference)
                    .filter(|parent| parent.sequence == base_sequence)
                    .ok_or_else(|| invalid("bundle group boundary is missing"))?,
            )
        };
        let delta = parent
            .map(|parent| {
                if state.parent.as_ref() == Some(parent) {
                    encode_table(row_count, sequence, &state.updates, Some(parent))
                } else {
                    let base = state
                        .group_base
                        .as_ref()
                        .ok_or_else(|| invalid("bundle group boundary is missing"))?;
                    let updates = group_updates(&state.streams, base)?;
                    encode_table(row_count, sequence, &updates, Some(parent))
                }
            })
            .transpose()?;
        let (bytes, depth, chain_bytes) = match (delta, parent) {
            (Some(bytes), Some(parent))
                if u64::from(parent.chain_bytes) + bytes.len() as u64
                    <= u64::from(MAX_TABLE_BYTES) =>
            {
                let chain_bytes = parent.chain_bytes + bytes.len() as u32;
                (bytes, parent.depth + 1, chain_bytes)
            }
            _ => {
                if parent.is_some() {
                    // The decoded-table budget is independent of grouping.
                    // Restart the local counter when a full snapshot is needed.
                    sequence = 1;
                }
                let bytes = encode_table(row_count, sequence, &state.streams, None)?;
                let len = bytes.len() as u32;
                (bytes, 1, len)
            }
        };
        let bytes = encode_record(&bytes);
        let reference = BundleReference {
            sequence,
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

fn group_updates(
    streams: &BTreeMap<u8, Stream>,
    base: &GroupBase,
) -> io::Result<BTreeMap<u8, Stream>> {
    let mut updates = BTreeMap::new();
    for (&id, stream) in streams {
        if id < DATA_STREAMS {
            let Some(previous) = base.data.get(&id) else {
                updates.insert(id, stream.clone());
                continue;
            };
            let len = stream
                .len
                .checked_sub(previous.len)
                .ok_or_else(|| invalid("bundle group shortened an append-only stream"))?;
            if len != 0 {
                updates.insert(
                    id,
                    Stream {
                        len,
                        extents: stream
                            .extents
                            .get(previous.extents..)
                            .ok_or_else(|| invalid("bundle group shortened its extent prefix"))?
                            .to_vec(),
                        inline: stream
                            .inline
                            .get(previous.inline..)
                            .ok_or_else(|| invalid("bundle group shortened its index prefix"))?
                            .to_vec(),
                    },
                );
            }
        } else if base.metadata.get(&id) != Some(stream) {
            updates.insert(id, stream.clone());
        }
    }
    Ok(updates)
}

fn encode_table(
    rows: u64,
    sequence: u64,
    streams: &BTreeMap<u8, Stream>,
    parent: Option<&BundleReference>,
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(TABLE_MAGIC);
    bytes.extend_from_slice(&rows.to_le_bytes());
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&(streams.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&u32::from(parent.is_some()).to_le_bytes());
    if let Some(parent) = parent {
        bytes.extend_from_slice(&parent.table_offset.to_le_bytes());
        bytes.extend_from_slice(&parent.table_len.to_le_bytes());
        bytes.extend_from_slice(&parent.checksum.to_le_bytes());
        bytes.extend_from_slice(&parent.row_count.to_le_bytes());
        bytes.extend_from_slice(&parent.depth.to_le_bytes());
        bytes.extend_from_slice(&parent.chain_bytes.to_le_bytes());
        bytes.extend_from_slice(&parent.sequence.to_le_bytes());
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

// The checksummed record bounds decompression before allocating. Chain budgets
// count decoded bytes, so compression cannot expand the accepted metadata limit.
fn encode_record(bytes: &[u8]) -> Vec<u8> {
    let compressed = lz4_flex::block::compress(bytes);
    let (codec, payload) = if compressed.len() < bytes.len() {
        (1, compressed.as_slice())
    } else {
        (0, bytes)
    };
    let mut record = Vec::with_capacity(5 + payload.len());
    record.push(codec);
    record.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    record.extend_from_slice(payload);
    record
}

fn decode_record(record: &[u8], budget: u32) -> io::Result<Vec<u8>> {
    let mut cursor = Cursor(record);
    let [codec] = cursor.take::<1>()?;
    let len = cursor.u32()?;
    if !(32..=MAX_TABLE_BYTES).contains(&len) || len > budget {
        return Err(invalid("bundle decoded table exceeds its bound"));
    }
    match codec {
        0 if cursor.0.len() == len as usize => Ok(cursor.0.to_vec()),
        1 if cursor.0.len() < len as usize => {
            let mut bytes = buffer(len as usize)?;
            let actual = lz4_flex::block::decompress_into(cursor.0, &mut bytes)
                .map_err(|_| invalid("invalid compressed bundle table"))?;
            if actual != bytes.len() {
                return Err(invalid("bundle decoded table length mismatch"));
            }
            Ok(bytes)
        }
        _ => Err(invalid("invalid bundle table record")),
    }
}

fn decode_table(
    bytes: &[u8],
    reference: &BundleReference,
) -> io::Result<(BTreeMap<u8, Stream>, Option<BundleReference>)> {
    let mut cursor = Cursor(bytes);
    if &cursor.take::<8>()? != TABLE_MAGIC
        || cursor.u64()? != reference.row_count
        || cursor.u64()? != reference.sequence
    {
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
            sequence: cursor.u64()?,
        };
        if parent.end()? > reference.table_offset
            || parent.row_count > reference.row_count
            || parent.sequence.checked_add(table_span(reference.sequence))
                != Some(reference.sequence)
            || parent.depth + 1 != reference.depth
            || parent.chain_bytes.checked_add(bytes.len() as u32) != Some(reference.chain_bytes)
        {
            return Err(invalid("invalid bundle parent reference"));
        }
        Some(parent)
    } else {
        if reference.depth != 1
            || reference.chain_bytes != bytes.len() as u32
            || table_span(reference.sequence) != reference.sequence
        {
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
        assert!(new.read_stream(STREAMS).is_err());
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
        assert!(fs::metadata(path).unwrap().len() < 512 * 1024);
    }

    #[test]
    fn sparse_reads_preserve_snapshot_bounds_and_recheck_backing_integrity() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let writer = BundleWriter::create(&path).unwrap();
        let expected: Vec<u8> = (0..2 * READ_AHEAD_BYTES + 31)
            .map(|n| ((n * 17) ^ (n / READ_AHEAD_BYTES)) as u8)
            .collect();
        for bytes in expected.chunks(97) {
            writer.append_data(0, bytes).unwrap();
        }
        let first = writer.finish(1).unwrap();
        let reader = BundleReader::open(&path, &first).unwrap();
        // Selection spans read-ahead boundaries and partial extents. A warm
        // second read must match the independently generated bytes exactly.
        for _ in 0..2 {
            assert_eq!(reader.read_stream(0).unwrap(), expected);
            for start in [0, 96, READ_AHEAD_BYTES - 17, 2 * READ_AHEAD_BYTES - 17] {
                assert_eq!(
                    reader
                        .read_range(0, start as u64..(start + 29) as u64)
                        .unwrap(),
                    expected[start..start + 29]
                );
            }
        }
        let writer = BundleWriter::append(&path, &first).unwrap();
        writer.append_data(0, b"unpublished suffix").unwrap();
        writer.finish(2).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(first.end().unwrap())
            .unwrap();
        // A fresh window must never depend on bytes after its pinned snapshot.
        let reader = BundleReader::open(&path, &first).unwrap();
        assert_eq!(reader.read_stream(0).unwrap(), expected);
        let mut bytes = fs::read(&path).unwrap();
        reader.read_range(0, 0..29).unwrap();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(READ_AHEAD_BYTES as u64)
            .unwrap();
        let start = READ_AHEAD_BYTES + 100;
        let range = start as u64..(start + 29) as u64;
        assert!(reader.read_range(0, range.clone()).is_err());
        fs::write(&path, &bytes).unwrap();
        assert_eq!(
            reader.read_range(0, range).unwrap(),
            expected[start..start + 29]
        );
        bytes[FILE_MAGIC.len()] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(reader.verify_all().is_err());
        assert!(
            BundleReader::open(&path, &first)
                .unwrap()
                .read_stream(0)
                .is_err()
        );
    }

    #[test]
    fn selected_ranges_keep_each_stream_and_snapshot_independent() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let mut writer = BundleWriter::create(&path).unwrap();
        let mut expected = vec![Vec::new(); 3];
        let mut snapshots = Vec::new();
        for row in 1..=128 {
            for (id, bytes) in expected.iter_mut().enumerate() {
                let chunk: Vec<_> = (0..1 + row as usize * (id + 1) % 101)
                    .map(|n| (n ^ id ^ row as usize) as u8)
                    .collect();
                writer.append_data(id as u8, &chunk).unwrap();
                bytes.extend(chunk);
            }
            let reference = writer.finish(row).unwrap();
            if [63, 128].contains(&row) {
                let reader = BundleReader::open(&path, &reference).unwrap();
                // Warm selections before later appends carry the table group.
                for (id, bytes) in expected.iter().enumerate() {
                    let middle = bytes.len() / 2;
                    assert_eq!(
                        reader
                            .read_range(id as u8, middle as u64..bytes.len() as u64)
                            .unwrap(),
                        bytes[middle..]
                    );
                }
                snapshots.push((reader, expected.clone()));
            }
            writer = BundleWriter::append(&path, &reference).unwrap();
        }
        std::thread::scope(|scope| {
            for (reader, model) in &snapshots {
                for (id, bytes) in model.iter().enumerate() {
                    let reader = reader.clone();
                    scope.spawn(move || {
                        let mut rng = 919u64 + id as u64;
                        for _ in 0..200 {
                            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let start = rng as usize % (bytes.len() + 1);
                            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                            let end = start + rng as usize % (bytes.len() - start + 1);
                            assert_eq!(
                                reader
                                    .read_range(id as u8, start as u64..end as u64)
                                    .unwrap(),
                                bytes[start..end]
                            );
                        }
                        let end = bytes.len() as u64;
                        assert!(reader.read_range(id as u8, end..end).unwrap().is_empty());
                        assert!(reader.read_range(id as u8, end..end + 1).is_err());
                    });
                }
            }
        });
    }

    #[test]
    fn table_groups_carry_without_losing_old_snapshots() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let mut writer = BundleWriter::create(&path).unwrap();
        let mut snapshots = Vec::new();
        for rows in 1..=1057 {
            writer.append_data(0, &[rows as u8]).unwrap();
            writer.append_data(14, &[rows as u8; 24]).unwrap();
            writer.replace_metadata(28, &[rows as u8]).unwrap();
            let reference = writer.finish(rows).unwrap();
            assert_eq!(reference.sequence, rows);
            assert_eq!(
                reference.depth,
                (rows % 8 + rows / 8 % 8 + rows / 64 % 8 + rows / 512) as u32
            );
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
    fn group_carries_survive_failed_writes_and_suffix_rollback() {
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("source");
        let mut writer = BundleWriter::create(&source).unwrap();
        let mut boundaries = Vec::new();
        for rows in 1..=1024 {
            writer.append_data(0, &[rows as u8]).unwrap();
            writer.append_data(14, &[rows as u8; 24]).unwrap();
            writer.replace_metadata(28, &[rows as u8]).unwrap();
            let reference = writer.finish(rows).unwrap();
            if [7, 8, 63, 64, 511, 512, 1023, 1024].contains(&rows) {
                boundaries.push(reference.clone());
            }
            writer = BundleWriter::append(&source, &reference).unwrap();
        }
        let source_bytes = fs::read(&source).unwrap();
        for reference in boundaries {
            let prefix = &source_bytes[..reference.end().unwrap() as usize];
            for failure in 0..3 {
                let path = tmp.path().join(format!("{}-{failure}", reference.sequence));
                fs::write(&path, prefix).unwrap();
                let old = BundleReader::open(&path, &reference).unwrap();
                let writer = BundleWriter::append(&path, &reference).unwrap();
                crate::durability::inject_failure(failure);
                let result = writer
                    .append_data(0, b"x")
                    .and_then(|()| writer.append_data(14, &[9; 24]))
                    .and_then(|()| writer.replace_metadata(28, b"new"));
                assert_eq!(result.is_err(), failure < 2);
                assert!(writer.finish(reference.row_count + 1).is_err());
                assert_eq!(crate::durability::take_events().len(), failure + 1);
                assert!(fs::read(&path).unwrap().starts_with(prefix));
                old.verify_all().unwrap();
                OpenOptions::new()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_len(reference.end().unwrap())
                    .unwrap();
                let writer = BundleWriter::append(&path, &reference).unwrap();
                writer.append_data(0, b"retry").unwrap();
                writer.append_data(14, &[8; 24]).unwrap();
                writer.replace_metadata(28, b"").unwrap();
                let recovered = writer.finish(reference.row_count + 1).unwrap();
                let reader = BundleReader::open(&path, &recovered).unwrap();
                let mut expected: Vec<_> = (1..=reference.row_count).map(|n| n as u8).collect();
                assert_eq!(old.read_stream(0).unwrap(), expected);
                expected.extend_from_slice(b"retry");
                assert_eq!(reader.read_stream(0).unwrap(), expected);
                let mut index: Vec<_> = (1..=reference.row_count)
                    .flat_map(|n| [n as u8; 24])
                    .collect();
                index.extend_from_slice(&[8; 24]);
                assert_eq!(reader.read_stream(14).unwrap(), index);
                assert!(reader.read_stream(28).unwrap().is_empty());
                assert_eq!(old.read_stream(28).unwrap(), [reference.row_count as u8]);
                reader.verify_all().unwrap();
            }
        }
    }

    #[test]
    fn a_valid_checksum_cannot_skip_a_group_boundary() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let first = create(&path);
        let sequence = 2 << GROUP_BITS;
        let bytes = encode_table(3, sequence, &BTreeMap::new(), Some(&first)).unwrap();
        let decoded_len = bytes.len() as u32;
        let bytes = encode_record(&bytes);
        let forged = BundleReference {
            sequence,
            row_count: 3,
            table_offset: first.end().unwrap(),
            table_len: bytes.len() as u32,
            checksum: crc32fast::hash(&bytes),
            depth: 2,
            chain_bytes: first.chain_bytes + decoded_len,
        };
        forged.end().unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&bytes)
            .unwrap();
        let before = fs::read(&path).unwrap();
        assert!(BundleReader::open(&path, &forged).is_err());
        assert_eq!(fs::read(&path).unwrap(), before);
        BundleReader::open(&path, &first)
            .unwrap()
            .verify_all()
            .unwrap();
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
            let mut table =
                decode_record(&damaged[first.table_offset as usize..], first.chain_bytes).unwrap();
            match case {
                0 => table[33] = 0,
                1 => table[36..44].copy_from_slice(&(INDEX_BYTES as u64 + 1).to_le_bytes()),
                2 => table[44..48].copy_from_slice(&1u32.to_le_bytes()),
                _ => unreachable!(),
            }
            let mut forged = first.clone();
            let record = encode_record(&table);
            forged.table_len = record.len() as u32;
            forged.checksum = crc32fast::hash(&record);
            damaged.truncate(forged.table_offset as usize);
            damaged.extend_from_slice(&record);
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
        let bytes = encode_table(2, 2, &updates, Some(&first)).unwrap();
        let decoded_len = bytes.len() as u32;
        let bytes = encode_record(&bytes);
        let second = BundleReference {
            sequence: 2,
            row_count: 2,
            table_offset: first.end().unwrap(),
            table_len: bytes.len() as u32,
            checksum: crc32fast::hash(&bytes),
            depth: 2,
            chain_bytes: first.chain_bytes + decoded_len,
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
            let mut table =
                decode_record(&damaged[second.table_offset as usize..], second.chain_bytes)
                    .unwrap();
            match case {
                0 => table[32..40].copy_from_slice(&second.table_offset.to_le_bytes()),
                1 => table[40..44].copy_from_slice(&u32::MAX.to_le_bytes()),
                2 => table[44] ^= 1, // parent checksum
                3 => table[48..56].copy_from_slice(&4u64.to_le_bytes()),
                4 => table[56..60].copy_from_slice(&second.depth.to_le_bytes()),
                5 => table[60..64].copy_from_slice(&u32::MAX.to_le_bytes()),
                6 => table[88..96].copy_from_slice(&(first.end().unwrap() - 1).to_le_bytes()),
                7 => table[96..100].copy_from_slice(&u32::MAX.to_le_bytes()),
                8 => table[28] = 2,
                9 => table[76..84].copy_from_slice(&u64::MAX.to_le_bytes()),
                _ => unreachable!(),
            }
            let mut forged = second.clone();
            let record = encode_record(&table);
            forged.table_len = record.len() as u32;
            forged.checksum = crc32fast::hash(&record);
            damaged.truncate(forged.table_offset as usize);
            damaged.extend_from_slice(&record);
            fs::write(&path, &damaged).unwrap();
            assert!(BundleReader::open(&path, &forged).is_err(), "case {case}");
            assert_eq!(fs::read(&path).unwrap(), damaged);
        }
        for case in 0..7 {
            let mut forged = second.clone();
            match case {
                0 => forged.depth = 0,
                1 => forged.depth = MAX_TABLE_DEPTH + 1,
                2 => forged.chain_bytes = MAX_TABLE_BYTES + 1,
                3 => forged.chain_bytes = 31,
                4 => forged.row_count = MAX_ROWS + 1,
                5 => forged.sequence = 0,
                6 => forged.sequence = 32,
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
            let mut table = decode_record(
                &bytes[reference.table_offset as usize..],
                reference.chain_bytes,
            )
            .unwrap();
            match case {
                0 => table[24..28].copy_from_slice(&u32::MAX.to_le_bytes()),
                1 => table[32] = STREAMS,
                2 => table[33] = 1,
                3 => table[36..44].copy_from_slice(&u64::MAX.to_le_bytes()),
                4 => table[44..48].copy_from_slice(&u32::MAX.to_le_bytes()),
                5 => table[48..56].copy_from_slice(&0u64.to_le_bytes()),
                6 => table[56..60].copy_from_slice(&u32::MAX.to_le_bytes()),
                7 => table[64] = 0, // duplicated stream id
                8 => table[80..88].copy_from_slice(&8u64.to_le_bytes()), // overlapping payload
                9 => table[28] = 1,
                _ => unreachable!(),
            }
            let mut forged = reference.clone();
            let record = encode_record(&table);
            forged.table_len = record.len() as u32;
            forged.checksum = crc32fast::hash(&record);
            bytes.truncate(forged.table_offset as usize);
            bytes.extend_from_slice(&record);
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
        let mut table = decode_record(
            &bytes[reference.table_offset as usize..],
            reference.chain_bytes,
        )
        .unwrap();
        let first = table[48..64].to_vec();
        table.copy_within(64..80, 48);
        table[64..80].copy_from_slice(&first);
        let record = encode_record(&table);
        reference.table_len = record.len() as u32;
        reference.checksum = crc32fast::hash(&record);
        bytes.truncate(reference.table_offset as usize);
        bytes.extend_from_slice(&record);
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

    #[cfg(unix)]
    #[test]
    fn a_group_carry_uses_the_inspected_file_after_path_replacement() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let original = tmp.path().join("original");
        let mut writer = BundleWriter::create(&path).unwrap();
        let mut reference = None;
        for rows in 1..16 {
            writer.append_data(0, &[rows as u8]).unwrap();
            let snapshot = writer.finish(rows).unwrap();
            writer = BundleWriter::append(&path, &snapshot).unwrap();
            reference = Some(snapshot);
        }
        let reader = BundleReader::open(&path, &reference.unwrap()).unwrap();
        fs::rename(&path, &original).unwrap();
        fs::write(&path, b"unrelated replacement").unwrap();
        writer.append_data(0, &[16]).unwrap();
        let next = writer.finish(16).unwrap();
        assert_eq!(reader.read_stream(0).unwrap(), (1..16).collect::<Vec<u8>>());
        let appended = BundleReader::open(&original, &next).unwrap();
        assert_eq!(
            appended.read_stream(0).unwrap(),
            (1..=16).collect::<Vec<u8>>()
        );
        appended.verify_all().unwrap();
        assert_eq!(fs::read(path).unwrap(), b"unrelated replacement");
    }

    #[cfg(unix)]
    #[test]
    fn inspected_append_rejects_file_replacement_even_with_identical_bytes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let original = tmp.path().join("original");
        let first = create(&path);
        let reader = BundleReader::open(&path, &first).unwrap();
        let bytes = fs::read(&path).unwrap();
        fs::rename(&path, &original).unwrap();
        fs::copy(&original, &path).unwrap();
        assert!(BundleWriter::append_inspected(&path, reader.clone()).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(fs::read(&original).unwrap(), bytes);
        assert_eq!(reader.read_stream(0).unwrap(), b"first");
        let current = BundleReader::open(&path, &first).unwrap();
        let writer = BundleWriter::append_inspected(&path, current.clone()).unwrap();
        writer.append_data(0, b" next").unwrap();
        let second = writer.finish(3).unwrap();
        assert_eq!(
            BundleReader::open(&path, &second)
                .unwrap()
                .read_stream(0)
                .unwrap(),
            b"first next"
        );
        assert_eq!(reader.read_stream(0).unwrap(), b"first");
        assert_eq!(current.read_stream(0).unwrap(), b"first");
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

    #[test]
    fn table_records_bound_decoding_and_round_trip_both_codecs() {
        let mut rng = 919u64;
        let mut codecs = [false; 2];
        for len in [32, 127, 4096, MAX_TABLE_BYTES as usize] {
            for random in [false, true] {
                let bytes: Vec<_> = (0..len)
                    .map(|_| {
                        rng ^= rng << 13;
                        rng ^= rng >> 7;
                        rng ^= rng << 17;
                        if random { rng as u8 } else { 0 }
                    })
                    .collect();
                let record = encode_record(&bytes);
                codecs[record[0] as usize] = true;
                assert!(record.len() <= len + 5);
                assert_eq!(decode_record(&record, MAX_TABLE_BYTES).unwrap(), bytes);
                assert!(decode_record(&record, len as u32 - 1).is_err());
                for declared in [0, 31, MAX_TABLE_BYTES + 1, u32::MAX] {
                    let mut forged = record.clone();
                    forged[1..5].copy_from_slice(&declared.to_le_bytes());
                    assert!(decode_record(&forged, MAX_TABLE_BYTES).is_err());
                }
            }
        }
        assert_eq!(codecs, [true, true]);
        let record = encode_record(&vec![7; 2048]);
        assert_eq!(record[0], 1);
        for len in 0..record.len() {
            assert!(decode_record(&record[..len], MAX_TABLE_BYTES).is_err());
        }
        let mut forged = record.clone();
        forged[0] = 2;
        assert!(decode_record(&forged, MAX_TABLE_BYTES).is_err());
        forged = record;
        forged[1..5].copy_from_slice(&2049u32.to_le_bytes());
        assert!(decode_record(&forged, MAX_TABLE_BYTES).is_err());
    }

    #[test]
    fn valid_checksums_cannot_bypass_table_record_bounds() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let reference = create(&path);
        let original = fs::read(&path).unwrap();
        for case in 0..7 {
            let mut bytes = original.clone();
            let record = &mut bytes[reference.table_offset as usize..];
            match case {
                0 => record[0] = 2,
                1 => record[1..5].copy_from_slice(&u32::MAX.to_le_bytes()),
                2 => record[1..5].copy_from_slice(&(MAX_TABLE_BYTES + 1).to_le_bytes()),
                3 => record[1..5].copy_from_slice(&31u32.to_le_bytes()),
                4 => record[1..5].copy_from_slice(&(reference.chain_bytes + 1).to_le_bytes()),
                5 => record[5..].fill(0),
                6 => record[1..5].copy_from_slice(&(reference.chain_bytes - 1).to_le_bytes()),
                _ => unreachable!(),
            }
            let mut forged = reference.clone();
            forged.checksum = crc32fast::hash(record);
            fs::write(&path, &bytes).unwrap();
            assert!(BundleReader::open(&path, &forged).is_err(), "case {case}");
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }
}
