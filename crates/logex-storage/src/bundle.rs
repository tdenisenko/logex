//! Prototype immutable compressed-artifact snapshots. This module is test-only
//! until the manifest/catalog integration and performance gates are complete.
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

const FILE_MAGIC: &[u8; 8] = b"LXBND001";
const TABLE_MAGIC: &[u8; 8] = b"LXBT0001";
const DATA_STREAMS: u8 = 14;
const STREAMS: u8 = 32;
const MAX_EXTENT_BYTES: usize = 1024 * 1024;
const MAX_EXTENTS: usize = 4096;
const MAX_TABLE_BYTES: u32 = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BundleReference {
    pub(crate) row_count: u64,
    pub(crate) table_offset: u64,
    pub(crate) table_len: u32,
    pub(crate) checksum: u32,
}

impl BundleReference {
    pub(crate) fn end(&self) -> io::Result<u64> {
        if self.row_count > u64::from(u32::MAX)
            || self.table_offset < FILE_MAGIC.len() as u64
            || self.table_len < 24
            || self.table_len > MAX_TABLE_BYTES
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
}

#[derive(Debug, Clone)]
pub(crate) struct BundleReader {
    file: Arc<Mutex<File>>,
    reference: BundleReference,
    streams: Arc<BTreeMap<u8, Stream>>,
}

impl BundleReader {
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
        file.seek(SeekFrom::Start(reference.table_offset))?;
        let mut bytes = buffer(reference.table_len as usize)?;
        file.read_exact(&mut bytes)?;
        if crc32fast::hash(&bytes) != reference.checksum {
            return Err(invalid("bundle table checksum mismatch"));
        }
        let streams = decode_table(&bytes, reference)?;
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
        stream.extents.extend(appended);
        stream.len = len;
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
        if state.failed || row_count < state.initial_rows || row_count > u64::from(u32::MAX) {
            return Err(invalid("invalid bundle snapshot completion"));
        }
        let bytes = encode_table(row_count, &state.streams)?;
        let reference = BundleReference {
            row_count,
            table_offset: state.offset,
            table_len: u32::try_from(bytes.len())
                .map_err(|_| invalid("bundle table is too large"))?,
            checksum: crc32fast::hash(&bytes),
        };
        reference.end()?;
        crate::durability::checkpoint("bundle_append_table", &state.path)?;
        state.file.write_all(&bytes)?;
        state.file.flush()?;
        Ok(reference)
    }
}

fn encode_table(rows: u64, streams: &BTreeMap<u8, Stream>) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(TABLE_MAGIC);
    bytes.extend_from_slice(&rows.to_le_bytes());
    bytes.extend_from_slice(&(streams.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    for (&id, stream) in streams {
        bytes.extend_from_slice(&[id, 0, 0, 0]);
        bytes.extend_from_slice(&stream.len.to_le_bytes());
        bytes.extend_from_slice(&(stream.extents.len() as u32).to_le_bytes());
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

fn decode_table(bytes: &[u8], reference: &BundleReference) -> io::Result<BTreeMap<u8, Stream>> {
    let mut cursor = Cursor(bytes);
    if &cursor.take::<8>()? != TABLE_MAGIC || cursor.u64()? != reference.row_count {
        return Err(invalid("bundle table identity mismatch"));
    }
    let count = cursor.u32()?;
    if count > u32::from(STREAMS) || cursor.u32()? != 0 {
        return Err(invalid("invalid bundle stream count or reserved field"));
    }
    let mut streams = BTreeMap::new();
    let mut physical = Vec::new();
    for _ in 0..count {
        let [id, a, b, c] = cursor.take::<4>()?;
        if id >= STREAMS || a != 0 || b != 0 || c != 0 || streams.contains_key(&id) {
            return Err(invalid("invalid or duplicated bundle stream"));
        }
        let len = cursor.u64()?;
        let count = cursor.u32()? as usize;
        if count > MAX_EXTENTS || count > cursor.0.len() / 16 {
            return Err(invalid("bundle extent count exceeds its bound"));
        }
        let mut extents = Vec::with_capacity(count);
        let mut total = 0u64;
        let mut previous_end = FILE_MAGIC.len() as u64;
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
        streams.insert(id, Stream { len, extents });
    }
    if !cursor.0.is_empty() {
        return Err(invalid("trailing bundle table bytes"));
    }
    physical.sort_unstable_by_key(|range| range.start);
    if physical.windows(2).any(|pair| pair[0].end > pair[1].start) {
        return Err(invalid("bundle extents overlap"));
    }
    Ok(streams)
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
        writer.replace_metadata(14, b"old index").unwrap();
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
        writer.replace_metadata(14, b"new index").unwrap();
        let second = writer.finish(3).unwrap();
        let new = BundleReader::open(&path, &second).unwrap();
        assert!(fs::read(&path).unwrap().starts_with(&before));
        assert_eq!(old.read_stream(0).unwrap(), b"first");
        assert_eq!(old.read_stream(14).unwrap(), b"old index");
        assert_eq!(new.read_stream(0).unwrap(), b"first second");
        assert_eq!(new.read_range(0, 3..8).unwrap(), b"st se");
        assert_eq!(new.read_stream(14).unwrap(), b"new index");
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
    fn selections_cross_bounded_extents_and_concurrent_columns() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("bundle");
        let writer = BundleWriter::create(&path).unwrap();
        let data: Vec<_> = (0..MAX_EXTENT_BYTES + 31)
            .map(|i| (i % 251) as u8)
            .collect();
        std::thread::scope(|scope| {
            for id in 0..DATA_STREAMS {
                let writer = &writer;
                let data = &data;
                scope.spawn(move || writer.append_data(id, data).unwrap());
            }
        });
        let reference = writer.finish(5).unwrap();
        let reader = BundleReader::open(&path, &reference).unwrap();
        for id in 0..DATA_STREAMS {
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
            for step in 0..32 {
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
