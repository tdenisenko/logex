use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeSet, BinaryHeap},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
};

use alloy_primitives::B256;
use logex_storage::native::{PrimaryAuditReport, PrimaryAuditSnapshot};
use logex_types::LogRow;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;

const RECORD_BYTES: u64 = 96;
const IO_BUFFER_BYTES: usize = 64 * 1024;
const RUN_DOMAIN: &[u8] = b"logex.history-audit.event-run.v1\0";
const FILE_DOMAIN: &[u8] = b"logex.history-audit.sorted-runs.v1\0";

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn check_cancelled(cancelled: &dyn Fn() -> bool) -> io::Result<()> {
    if cancelled() {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "history audit cancelled",
        ));
    }
    Ok(())
}

/// An inclusive comparison range. The coordinator must authenticate its upper
/// bound; constructing this value establishes no consensus provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditRange {
    pub from: u64,
    pub through: u64,
}

/// Explicit work allowances, not a promise about whole-process RSS. The source
/// snapshot has separate per-segment decode/retained-artifact allowances.
#[derive(Clone, Copy, Debug)]
pub struct AuditManifestLimits {
    pub sort_records: usize,
    pub merge_fan_in: usize,
    /// Peak scratch record bytes, including merge input and output together.
    pub max_scratch_bytes: u64,
    pub max_runs: u64,
}

/// Local source correspondence only. No receipt/ancestry acceptance is implied.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditManifestSummary {
    pub version: u32,
    pub range: AuditRange,
    pub source_prefix_fingerprint: B256,
    /// Selected segment namespaces, stable across tail appends and local index
    /// builds. Resume additionally requires identical selected occurrence bytes.
    pub selected_namespace_fingerprint: B256,
    pub physical_rows: u64,
    pub canonical_rows: u64,
    pub selected_rows: u64,
    pub nonempty_blocks: u64,
    pub runs: u64,
    pub record_bytes: u64,
    pub records_digest: B256,
    pub peak_scratch_bytes: u64,
}

/// A complete physical scan reduced to bounded, externally sorted event runs.
/// Keeps both the source owner/view and its private scratch alive. No source
/// files, catalog, canonical membership or query indexes are modified.
pub struct AuditManifest {
    _scratch: TempDir,
    source: PrimaryAuditSnapshot,
    source_report: PrimaryAuditReport,
    part: Part,
    summary: AuditManifestSummary,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct EventRun {
    pub block: u64,
    pub block_hash: B256,
    pub first_log: u32,
    pub rows: u64,
    pub segment: u64,
    pub first_physical_row: u32,
    pub digest: B256,
}

impl Ord for EventRun {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .block
            .cmp(&self.block)
            .then(self.first_log.cmp(&other.first_log))
            .then(self.segment.cmp(&other.segment))
            .then(self.first_physical_row.cmp(&other.first_physical_row))
            .then(self.block_hash.cmp(&other.block_hash))
            .then(self.rows.cmp(&other.rows))
            .then(self.digest.cmp(&other.digest))
    }
}

impl PartialOrd for EventRun {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl EventRun {
    fn encode(self) -> [u8; RECORD_BYTES as usize] {
        let mut out = [0; RECORD_BYTES as usize];
        out[..8].copy_from_slice(&self.block.to_le_bytes());
        out[8..40].copy_from_slice(self.block_hash.as_slice());
        out[40..44].copy_from_slice(&self.first_log.to_le_bytes());
        out[44..52].copy_from_slice(&self.rows.to_le_bytes());
        out[52..60].copy_from_slice(&self.segment.to_le_bytes());
        out[60..64].copy_from_slice(&self.first_physical_row.to_le_bytes());
        out[64..96].copy_from_slice(self.digest.as_slice());
        out
    }

    fn decode(bytes: &[u8; RECORD_BYTES as usize]) -> io::Result<Self> {
        let run = Self {
            block: u64::from_le_bytes(bytes[..8].try_into().expect("fixed slice")),
            block_hash: B256::from_slice(&bytes[8..40]),
            first_log: u32::from_le_bytes(bytes[40..44].try_into().expect("fixed slice")),
            rows: u64::from_le_bytes(bytes[44..52].try_into().expect("fixed slice")),
            segment: u64::from_le_bytes(bytes[52..60].try_into().expect("fixed slice")),
            first_physical_row: u32::from_le_bytes(bytes[60..64].try_into().expect("fixed slice")),
            digest: B256::from_slice(&bytes[64..96]),
        };
        run.end_log()?;
        Ok(run)
    }

    pub(super) fn end_log(&self) -> io::Result<u64> {
        let end = u64::from(self.first_log)
            .checked_add(self.rows)
            .ok_or_else(|| invalid("audit event-run index overflow"))?;
        if self.rows == 0 || end > u64::from(u32::MAX) + 1 {
            return Err(invalid("audit event-run indices are not representable"));
        }
        Ok(end)
    }
}

/// Exhaustive destructuring makes adding a LogRow field a compile-time encoding
/// decision. Every nullable topic and both data lengths have distinct framing.
pub(super) fn hash_row(hash: &mut blake3::Hasher, row: &LogRow) -> io::Result<()> {
    let LogRow {
        block_number,
        block_hash,
        timestamp,
        tx_hash,
        tx_index,
        log_index,
        address,
        topic0,
        topic1,
        topic2,
        topic3,
        data,
        data_len,
        source,
    } = row;
    if data.len() != *data_len as usize {
        return Err(invalid("audit row has inconsistent data length"));
    }
    hash.update(&block_number.to_le_bytes());
    hash.update(block_hash.as_slice());
    hash.update(&timestamp.to_le_bytes());
    hash.update(tx_hash.as_slice());
    hash.update(&tx_index.to_le_bytes());
    hash.update(&log_index.to_le_bytes());
    hash.update(address.as_slice());
    for topic in [topic0, topic1, topic2, topic3] {
        match topic {
            Some(value) => {
                hash.update(&[1]);
                hash.update(value.as_slice());
            }
            None => {
                hash.update(&[0]);
            }
        }
    }
    hash.update(&data_len.to_le_bytes());
    hash.update(&(data.len() as u64).to_le_bytes());
    hash.update(data);
    hash.update(&[*source as u8]);
    Ok(())
}

pub(super) fn run_hasher() -> blake3::Hasher {
    let mut hash = blake3::Hasher::new();
    hash.update(RUN_DOMAIN);
    hash
}

fn file_hasher() -> blake3::Hasher {
    let mut hash = blake3::Hasher::new();
    hash.update(FILE_DOMAIN);
    hash
}

struct PendingRun {
    run: EventRun,
    hash: blake3::Hasher,
}

impl PendingRun {
    fn new(segment: u64, physical_row: u32, row: &LogRow) -> Self {
        Self {
            run: EventRun {
                block: row.block_number,
                block_hash: row.block_hash,
                first_log: row.log_index,
                rows: 0,
                segment,
                first_physical_row: physical_row,
                digest: B256::ZERO,
            },
            hash: run_hasher(),
        }
    }

    fn can_append(&self, segment: u64, row: &LogRow) -> bool {
        self.run.segment == segment
            && self.run.block == row.block_number
            && self.run.block_hash == row.block_hash
            && u64::from(self.run.first_log).checked_add(self.run.rows)
                == Some(row.log_index.into())
    }

    fn append(&mut self, row: &LogRow) -> io::Result<()> {
        hash_row(&mut self.hash, row)?;
        self.run.rows = self
            .run
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("audit run length overflow"))?;
        self.run.end_log()?;
        Ok(())
    }

    fn finish(mut self) -> EventRun {
        self.run.digest = B256::from(*self.hash.finalize().as_bytes());
        self.run
    }
}

struct DiskBudget {
    used: u64,
    peak: u64,
    limit: u64,
}
impl DiskBudget {
    fn reserve_record(&mut self) -> io::Result<()> {
        let next = self
            .used
            .checked_add(RECORD_BYTES)
            .ok_or_else(|| invalid("audit scratch byte overflow"))?;
        if next > self.limit {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "audit scratch needs {next} record bytes, limit is {}",
                    self.limit
                ),
            ));
        }
        self.used = next;
        self.peak = self.peak.max(next);
        Ok(())
    }
    fn remove(&mut self, part: &Part) -> io::Result<()> {
        fs::remove_file(&part.path)?;
        self.used = self
            .used
            .checked_sub(part.bytes()?)
            .ok_or_else(|| invalid("audit scratch accounting underflow"))?;
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct Part {
    path: PathBuf,
    records: u64,
    digest: B256,
    level: usize,
}
impl Part {
    fn bytes(&self) -> io::Result<u64> {
        self.records
            .checked_mul(RECORD_BYTES)
            .ok_or_else(|| invalid("audit file size overflow"))
    }
    fn reader(&self) -> io::Result<RunReader> {
        let file = File::open(&self.path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() != self.bytes()? {
            return Err(invalid(
                "audit run file length differs from committed records",
            ));
        }
        Ok(RunReader {
            read: BufReader::with_capacity(IO_BUFFER_BYTES, file),
            remaining: self.records,
            expected_digest: self.digest,
            hash: file_hasher(),
            previous: None,
            finished: false,
            failed: false,
        })
    }
}

pub(super) struct RunReader {
    read: BufReader<File>,
    remaining: u64,
    expected_digest: B256,
    hash: blake3::Hasher,
    previous: Option<EventRun>,
    finished: bool,
    failed: bool,
}
impl RunReader {
    pub(super) fn next_run(&mut self) -> io::Result<Option<EventRun>> {
        if self.failed {
            return Err(invalid("audit run reader is terminal after failure"));
        }
        let result = self.read_next();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn read_next(&mut self) -> io::Result<Option<EventRun>> {
        if self.finished {
            return Ok(None);
        }
        if self.remaining == 0 {
            let mut trailing = [0];
            if self.read.read(&mut trailing)? != 0
                || self.hash.finalize().as_bytes() != self.expected_digest.as_slice()
            {
                return Err(invalid("audit run file digest or trailing bytes changed"));
            }
            self.finished = true;
            return Ok(None);
        }
        let mut bytes = [0; RECORD_BYTES as usize];
        self.read.read_exact(&mut bytes)?;
        self.hash.update(&bytes);
        let run = EventRun::decode(&bytes)?;
        if self.previous.is_some_and(|previous| previous > run) {
            return Err(invalid("audit run file is not sorted"));
        }
        self.previous = Some(run);
        self.remaining -= 1;
        Ok(Some(run))
    }
}

struct PartWriter {
    path: PathBuf,
    write: BufWriter<File>,
    records: u64,
    hash: blake3::Hasher,
}
impl PartWriter {
    fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        Ok(Self {
            path,
            write: BufWriter::with_capacity(IO_BUFFER_BYTES, file),
            records: 0,
            hash: file_hasher(),
        })
    }
    fn push(&mut self, run: EventRun, budget: &mut DiskBudget) -> io::Result<()> {
        budget.reserve_record()?;
        let bytes = run.encode();
        self.write.write_all(&bytes)?;
        self.hash.update(&bytes);
        self.records = self
            .records
            .checked_add(1)
            .ok_or_else(|| invalid("audit record count overflow"))?;
        Ok(())
    }
    fn finish(mut self) -> io::Result<Part> {
        self.write.flush()?;
        self.write.get_ref().sync_all()?;
        Ok(Part {
            path: self.path,
            records: self.records,
            digest: B256::from(*self.hash.finalize().as_bytes()),
            level: 0,
        })
    }
}

struct Sorter {
    directory: PathBuf,
    limits: AuditManifestLimits,
    buffer: Vec<EventRun>,
    parts: Vec<Part>,
    sequence: u64,
    total_runs: u64,
    budget: DiskBudget,
}
impl Sorter {
    fn new(directory: PathBuf, limits: AuditManifestLimits) -> io::Result<Self> {
        if limits.sort_records == 0
            || !(2..=64).contains(&limits.merge_fan_in)
            || limits.max_runs == 0
            || limits.max_scratch_bytes < RECORD_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "audit sort/disk allowances must be positive; merge fan-in must be 2..=64",
            ));
        }
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(limits.sort_records)
            .map_err(io::Error::other)?;
        Ok(Self {
            directory,
            limits,
            buffer,
            parts: Vec::new(),
            sequence: 0,
            total_runs: 0,
            budget: DiskBudget {
                used: 0,
                peak: 0,
                limit: limits.max_scratch_bytes,
            },
        })
    }
    fn next_path(&mut self) -> io::Result<PathBuf> {
        let path = self.directory.join(format!("part-{}.bin", self.sequence));
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("audit scratch file sequence overflow"))?;
        Ok(path)
    }
    fn push(&mut self, run: EventRun, cancelled: &dyn Fn() -> bool) -> io::Result<()> {
        check_cancelled(cancelled)?;
        self.total_runs = self
            .total_runs
            .checked_add(1)
            .ok_or_else(|| invalid("audit total run count overflow"))?;
        if self.total_runs > self.limits.max_runs {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "audit event-run allowance exceeded",
            ));
        }
        self.buffer.push(run);
        if self.buffer.len() == self.limits.sort_records {
            self.flush(cancelled)?;
        }
        Ok(())
    }
    fn flush(&mut self, cancelled: &dyn Fn() -> bool) -> io::Result<()> {
        check_cancelled(cancelled)?;
        if self.buffer.is_empty() {
            return Ok(());
        }
        self.buffer.sort_unstable();
        check_cancelled(cancelled)?;
        let mut writer = PartWriter::new(self.next_path()?)?;
        for run in self.buffer.drain(..) {
            check_cancelled(cancelled)?;
            writer.push(run, &mut self.budget)?;
        }
        self.parts.push(writer.finish()?);
        // Merge equal levels incrementally: tiny sort buffers cannot leave an
        // unbounded list of scratch files or directory entries behind.
        while let Some(start) = self.parts.len().checked_sub(self.limits.merge_fan_in) {
            let level = self.parts[start].level;
            if !self.parts[start..].iter().all(|part| part.level == level) {
                break;
            }
            let parents = self.parts.drain(start..).collect::<Vec<_>>();
            let merged = self.merge(&parents, cancelled)?;
            self.parts.push(merged);
        }
        Ok(())
    }
    fn merge(&mut self, parts: &[Part], cancelled: &dyn Fn() -> bool) -> io::Result<Part> {
        let mut readers = parts
            .iter()
            .map(Part::reader)
            .collect::<io::Result<Vec<_>>>()?;
        let mut heap = BinaryHeap::new();
        for (index, reader) in readers.iter_mut().enumerate() {
            if let Some(run) = reader.next_run()? {
                heap.push(Reverse((run, index)));
            }
        }
        let mut writer = PartWriter::new(self.next_path()?)?;
        while let Some(Reverse((run, index))) = heap.pop() {
            check_cancelled(cancelled)?;
            writer.push(run, &mut self.budget)?;
            if let Some(next) = readers[index].next_run()? {
                heap.push(Reverse((next, index)));
            }
        }
        // All readers reached and verified their exact end before parents retire.
        let mut result = writer.finish()?;
        result.level = parts
            .iter()
            .map(|part| part.level)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| invalid("audit merge level overflow"))?;
        drop(readers);
        for part in parts {
            self.budget.remove(part)?;
        }
        Ok(result)
    }
    fn finish(mut self, cancelled: &dyn Fn() -> bool) -> io::Result<(Part, u64)> {
        self.flush(cancelled)?;
        if self.parts.is_empty() {
            let part = PartWriter::new(self.next_path()?)?.finish()?;
            self.parts.push(part);
        }
        while self.parts.len() > 1 {
            let old = std::mem::take(&mut self.parts);
            for chunk in old.chunks(self.limits.merge_fan_in) {
                if chunk.len() == 1 {
                    self.parts.push(chunk[0].clone());
                } else {
                    let merged = self.merge(chunk, cancelled)?;
                    self.parts.push(merged);
                }
            }
        }
        Ok((
            self.parts.pop().expect("at least an empty part"),
            self.budget.peak,
        ))
    }
}

impl AuditManifest {
    /// Visit the complete captured physical prefix, selecting actual canonical
    /// row values in `range`. Stored/query bounds and indexes never skip rows.
    /// Any source/read/cancellation/limit error discards the private scratch.
    pub fn build(
        source: PrimaryAuditSnapshot,
        range: AuditRange,
        scratch_parent: &Path,
        limits: AuditManifestLimits,
        cancelled: &dyn Fn() -> bool,
    ) -> io::Result<Self> {
        let result = Self::build_inner(&source, range, scratch_parent, limits, cancelled);
        // A raced reorg/close is unavailable, even if a local comparison also
        // failed. It must not be reported as a confirmed integrity failure.
        source.validate()?;
        result
    }
    fn build_inner(
        source: &PrimaryAuditSnapshot,
        range: AuditRange,
        scratch_parent: &Path,
        limits: AuditManifestLimits,
        cancelled: &dyn Fn() -> bool,
    ) -> io::Result<Self> {
        if range.from > range.through {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "audit range is inverted",
            ));
        }
        check_cancelled(cancelled)?;
        source.validate()?;
        let scratch = tempfile::Builder::new()
            .prefix("logex-history-audit-")
            .tempdir_in(scratch_parent)?;
        let mut sorter = Sorter::new(scratch.path().to_owned(), limits)?;
        let mut pending: Option<PendingRun> = None;
        let mut physical = 0u64;
        let mut canonical = 0u64;
        let mut selected = 0u64;
        let mut selected_segments = BTreeSet::new();
        let source_report = source.scan(cancelled, |batch| {
            for item in batch.rows() {
                check_cancelled(cancelled)?;
                physical = physical
                    .checked_add(1)
                    .ok_or_else(|| invalid("audit physical count overflow"))?;
                if !item.canonical {
                    continue;
                }
                canonical = canonical
                    .checked_add(1)
                    .ok_or_else(|| invalid("audit canonical count overflow"))?;
                let row = item.row;
                if !(range.from..=range.through).contains(&row.block_number) {
                    continue;
                }
                selected = selected
                    .checked_add(1)
                    .ok_or_else(|| invalid("audit selected count overflow"))?;
                selected_segments.insert(item.segment_id);
                if pending
                    .as_ref()
                    .is_some_and(|run| !run.can_append(item.segment_id, row))
                {
                    sorter.push(
                        pending.take().expect("checked pending run").finish(),
                        cancelled,
                    )?;
                }
                let run = pending
                    .get_or_insert_with(|| PendingRun::new(item.segment_id, item.row_id, row));
                run.append(row)?;
            }
            Ok(())
        })?;
        if physical != source_report.physical_rows || canonical != source_report.canonical_rows {
            return Err(invalid(
                "audit callback did not account for the entire physical source",
            ));
        }
        if let Some(run) = pending {
            sorter.push(run.finish(), cancelled)?;
        }
        let (part, peak) = sorter.finish(cancelled)?;
        let (rows, blocks) = validate_order(&part, range, cancelled)?;
        if rows != selected {
            return Err(invalid("audit merge changed physical occurrence count"));
        }
        source.validate()?;
        check_cancelled(cancelled)?;
        let mut namespaces = blake3::Hasher::new();
        namespaces.update(b"logex.history-audit.selected-namespaces.v1\0");
        namespaces.update(&(selected_segments.len() as u64).to_le_bytes());
        let mut bound = 0usize;
        // Scan reports follow captured catalog order. Sort identities so a
        // representation-only catalog reordering does not change this binding.
        let mut identities = source_report
            .segments
            .iter()
            .filter(|s| selected_segments.contains(&s.segment_id))
            .collect::<Vec<_>>();
        identities.sort_unstable_by_key(|s| s.segment_id);
        for segment in identities {
            namespaces.update(&segment.segment_id.to_le_bytes());
            namespaces.update(
                &segment
                    .source_namespace
                    .ok_or_else(|| invalid("selected audit segment lacks source namespace"))?,
            );
            bound += 1;
        }
        if bound != selected_segments.len() {
            return Err(invalid(
                "selected audit segment is missing from scan report",
            ));
        }
        let summary = AuditManifestSummary {
            version: 1,
            range,
            source_prefix_fingerprint: source_report.source_prefix_fingerprint,
            selected_namespace_fingerprint: B256::from(*namespaces.finalize().as_bytes()),
            physical_rows: physical,
            canonical_rows: canonical,
            selected_rows: selected,
            nonempty_blocks: blocks,
            runs: part.records,
            record_bytes: part.bytes()?,
            records_digest: part.digest,
            peak_scratch_bytes: peak,
        };
        Ok(Self {
            _scratch: scratch,
            source: source.clone(),
            source_report,
            part,
            summary,
        })
    }
    pub fn summary(&self) -> &AuditManifestSummary {
        &self.summary
    }
    pub fn source_report(&self) -> &PrimaryAuditReport {
        &self.source_report
    }
    /// Must be checked again at final result admission. A successful old scan
    /// cannot turn a later invalidated source view into a current result.
    pub fn validate(&self) -> io::Result<()> {
        self.source.validate()
    }
    pub(super) fn reader(&self) -> io::Result<RunReader> {
        self.validate()?;
        self.part.reader()
    }
}

fn validate_order(
    part: &Part,
    range: AuditRange,
    cancelled: &dyn Fn() -> bool,
) -> io::Result<(u64, u64)> {
    let mut reader = part.reader()?;
    let mut previous: Option<EventRun> = None;
    let mut rows = 0u64;
    let mut blocks = 0u64;
    while let Some(run) = reader.next_run()? {
        check_cancelled(cancelled)?;
        if !(range.from..=range.through).contains(&run.block) {
            return Err(invalid("audit run is outside selected range"));
        }
        let next = match previous {
            Some(old) if old.block == run.block => {
                if old.block_hash != run.block_hash {
                    return Err(invalid(format!(
                        "multiple canonical hashes at audit block {}",
                        run.block
                    )));
                }
                old.end_log()?
            }
            _ => {
                blocks = blocks
                    .checked_add(1)
                    .ok_or_else(|| invalid("audit block count overflow"))?;
                0
            }
        };
        if u64::from(run.first_log) != next {
            return Err(invalid(format!(
                "missing or overlapping canonical audit events at block {}: expected index {next}, found {} (segment {}, physical row {})",
                run.block, run.first_log, run.segment, run.first_physical_row
            )));
        }
        rows = rows
            .checked_add(run.rows)
            .ok_or_else(|| invalid("audit merged row count overflow"))?;
        previous = Some(run);
    }
    Ok((rows, blocks))
}

#[cfg(test)]
mod tests;
