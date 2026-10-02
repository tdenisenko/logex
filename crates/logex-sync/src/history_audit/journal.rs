//! Immutable, checksummed checkpoint chunks for an explicit one-time job.
//!
//! These are private local records of completed receipt comparisons, not portable
//! receipt proofs. Resume trusts previously produced local evidence, verifies its
//! complete header/checksum chain and independently rescans the current physical
//! input. It does not reinterpret a checkpoint checksum as an Ethereum root.
use super::{
    AuditManifest, AuditManifestSummary, AuditRange, ReceiptComparison, ReceiptComparisonReport,
};
use crate::repair::{RepairFetchLimits, RepairFetcher, RepairRange, VerifiedRepairBlock};
use alloy_consensus::Header;
use alloy_primitives::B256;
use alloy_rlp::{Decodable, Encodable};
use logex_cl::FinalizedAuditAnchor;
use logex_types::{ExecutionAnchor, WeakSubjectivityCheckpoint};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

const IDENTITY: &str = "identity";
const ID_MAGIC: &[u8; 8] = b"LXAUD001";
const CHUNK_MAGIC: &[u8; 8] = b"LXACH001";
const MAX_IDENTITY: usize = 16 * 1024;
const MAX_HEADER: usize = 8192;
const MAX_CHECKPOINT_BLOCKS: usize = 1024;
const MAX_CHUNK: usize = 128 + MAX_CHECKPOINT_BLOCKS * (MAX_HEADER + 12);
const MAX_UNPUBLISHED: usize = 32;
const MAX_CHUNKS: u64 = 1_000_000;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn cancelled(check: &dyn Fn() -> bool) -> io::Result<()> {
    if check() {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "audit journal cancelled",
        ));
    }
    Ok(())
}
fn digest(bytes: &[u8]) -> B256 {
    let mut h = blake3::Hasher::new();
    h.update(b"logex.history-audit.journal.v1\0");
    h.update(bytes);
    B256::from(*h.finalize().as_bytes())
}

/// Logical journal bytes and entry counts; filesystem allocation and source
/// manifest scratch have separate allowances. No implicit unbounded defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditJournalLimits {
    pub max_bytes: u64,
    pub max_chunks: u64,
    pub checkpoint_blocks: usize,
}
impl AuditJournalLimits {
    fn validate(&self) -> io::Result<()> {
        if self.max_bytes == 0
            || !(1..=MAX_CHUNKS).contains(&self.max_chunks)
            || !(1..=MAX_CHECKPOINT_BLOCKS).contains(&self.checkpoint_blocks)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid audit journal work allowance",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuditJobIdentity {
    version: u32,
    job_id: B256,
    anchor: ExecutionAnchor,
    checkpoint: WeakSubjectivityCheckpoint,
    manifest: AuditManifestSummary,
    limits: AuditJournalLimits,
}
impl AuditJobIdentity {
    /// Reading metadata does not authenticate its consensus anchor or source.
    pub fn read(directory: &Path) -> io::Result<Self> {
        read_identity(directory).map(|v| v.0)
    }
    pub fn job_id(&self) -> B256 {
        self.job_id
    }
    pub fn anchor(&self) -> ExecutionAnchor {
        self.anchor
    }
    pub fn checkpoint(&self) -> WeakSubjectivityCheckpoint {
        self.checkpoint
    }
    pub fn range(&self) -> AuditRange {
        self.manifest.range
    }
    pub fn limits(&self) -> AuditJournalLimits {
        self.limits
    }
}

struct DirectoryOwner(File);
impl DirectoryOwner {
    fn open(directory: &Path) -> io::Result<Self> {
        if !fs::symlink_metadata(directory)?.is_dir() {
            return Err(invalid("audit job is not an ordinary directory"));
        }
        let file = File::open(directory)?;
        file.try_lock().map_err(|e| match e {
            std::fs::TryLockError::WouldBlock => {
                io::Error::new(io::ErrorKind::WouldBlock, "audit job already has an owner")
            }
            std::fs::TryLockError::Error(error) => error,
        })?;
        Ok(Self(file))
    }
}
impl Drop for DirectoryOwner {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error,"failed to release audit journal ownership");
        }
    }
}

/// A source-bound comparison plus a finite append-only checkpoint journal.
/// All methods perform bounded synchronous CPU/I/O and belong on an owned
/// maintenance worker, never under the live execution storage write guard.
pub struct AuditSession<'a> {
    directory: PathBuf,
    owner: DirectoryOwner,
    identity: AuditJobIdentity,
    manifest: &'a AuditManifest,
    comparison: ReceiptComparison<'a>,
    previous: B256,
    chunks: u64,
    disk_bytes: u64,
    blocks: u64,
    events: u64,
    empty_blocks: u64,
    pending: Vec<(Header, u64)>,
    last_header: Option<Header>,
    failed: bool,
}

/// Sealed continuation of a checkpointed receipt-comparison prefix. Only an
/// AuditSession can construct it after source/identity/header-chain checks.
pub(crate) struct AuditedContinuation {
    anchor: ExecutionAnchor,
    range: AuditRange,
    last_header: Header,
    blocks: u64,
}
impl AuditedContinuation {
    pub(crate) fn anchor(&self) -> ExecutionAnchor {
        self.anchor
    }
    pub(crate) fn range(&self) -> AuditRange {
        self.range
    }
    pub(crate) fn last_header(&self) -> &Header {
        &self.last_header
    }
    pub(crate) fn blocks(&self) -> u64 {
        self.blocks
    }
}

impl<'a> AuditSession<'a> {
    pub fn create(
        parent: &Path,
        manifest: &'a AuditManifest,
        anchor: FinalizedAuditAnchor,
        limits: AuditJournalLimits,
    ) -> io::Result<Self> {
        Self::create_inner(
            parent,
            manifest,
            anchor.execution(),
            anchor.checkpoint(),
            limits,
        )
    }

    fn create_inner(
        parent: &Path,
        manifest: &'a AuditManifest,
        anchor: ExecutionAnchor,
        checkpoint: WeakSubjectivityCheckpoint,
        limits: AuditJournalLimits,
    ) -> io::Result<Self> {
        limits.validate()?;
        let comparison = ReceiptComparison::new(manifest, anchor)?;
        let mut id = [0; 32];
        rand::rngs::OsRng
            .try_fill_bytes(&mut id)
            .map_err(io::Error::other)?;
        let identity = AuditJobIdentity {
            version: 1,
            job_id: B256::from(id),
            anchor,
            checkpoint,
            manifest: manifest.summary().clone(),
            limits,
        };
        let mut bytes = ID_MAGIC.to_vec();
        bytes.extend(serde_json::to_vec(&identity).map_err(io::Error::other)?);
        if bytes.len() + 32 > MAX_IDENTITY {
            return Err(invalid("audit identity is oversized"));
        }
        let previous = digest(&bytes);
        bytes.extend(previous.as_slice());
        if bytes.len() as u64 > limits.max_bytes {
            return Err(invalid("audit identity exceeds journal byte allowance"));
        }
        let staged = logex_fs::StagedDirectory::new_in(parent, "audit-")?;
        let directory = staged.path().to_owned();
        let owner = DirectoryOwner::open(&directory)?;
        publish(&directory, IDENTITY, ".audit-identity-", &bytes)?;
        File::open(parent)?.sync_all()?;
        staged.keep();
        manifest.validate()?;
        Ok(Self {
            directory,
            owner,
            identity,
            manifest,
            comparison,
            previous,
            chunks: 0,
            disk_bytes: bytes.len() as u64,
            blocks: 0,
            events: 0,
            empty_blocks: 0,
            pending: Vec::new(),
            last_header: None,
            failed: false,
        })
    }

    /// Rescan the entire current physical source before calling. A changed tail
    /// is allowed only if the selected namespaces, every occurrence/field digest
    /// and range counts match. A checkpoint from another source cannot advance
    /// a cursor merely because block numbers or aggregate counts happen to agree.
    pub fn resume(
        directory: &Path,
        manifest: &'a AuditManifest,
        anchor: FinalizedAuditAnchor,
        check: &dyn Fn() -> bool,
    ) -> io::Result<Self> {
        manifest.validate()?;
        let result = Self::resume_inner(
            directory,
            manifest,
            anchor.execution(),
            anchor.checkpoint(),
            check,
        );
        manifest.validate()?;
        result
    }

    fn resume_inner(
        directory: &Path,
        manifest: &'a AuditManifest,
        anchor: ExecutionAnchor,
        checkpoint: WeakSubjectivityCheckpoint,
        check: &dyn Fn() -> bool,
    ) -> io::Result<Self> {
        cancelled(check)?;
        let owner = DirectoryOwner::open(directory)?;
        let (identity, previous, identity_bytes) = read_identity(directory)?;
        if identity.anchor != anchor
            || identity.checkpoint != checkpoint
            || !same_selection(&identity.manifest, manifest.summary())
        {
            return Err(invalid(
                "audit checkpoint anchor, trust root or physical source changed",
            ));
        }
        let mut paths = Vec::new();
        let mut unpublished = 0usize;
        let mut disk_bytes = 0u64;
        for entry in fs::read_dir(directory)? {
            cancelled(check)?;
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| invalid("non-UTF8 audit journal entry"))?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() {
                return Err(invalid("audit journal contains a non-regular entry"));
            }
            disk_bytes = disk_bytes
                .checked_add(metadata.len())
                .ok_or_else(|| invalid("audit journal size overflow"))?;
            if disk_bytes > identity.limits.max_bytes {
                return Err(invalid("audit journal exceeds byte allowance"));
            }
            if name == IDENTITY {
                continue;
            }
            if let Some(n) = name
                .strip_prefix("chunk-")
                .and_then(|v| v.strip_suffix(".bin"))
            {
                let sequence = n
                    .parse::<u64>()
                    .map_err(|_| invalid("invalid audit chunk sequence"))?;
                if name != chunk_name(sequence) || paths.len() as u64 >= identity.limits.max_chunks
                {
                    return Err(invalid("invalid or excessive audit chunk names"));
                }
                paths.push(sequence);
            } else if valid_staging_name(name) {
                unpublished += 1;
                if unpublished > MAX_UNPUBLISHED
                    || metadata.len() > MAX_CHUNK.max(MAX_IDENTITY) as u64
                {
                    return Err(invalid("unpublished audit scratch allowance exceeded"));
                }
                // Never promote partially staged work. Retain it for diagnostics;
                // its size/count still consumes the explicit job allowance.
            } else {
                return Err(invalid("unknown artifact in audit job"));
            }
        }
        if disk_bytes < identity_bytes {
            return Err(invalid("audit identity disappeared during enumeration"));
        }
        cancelled(check)?;
        paths.sort_unstable();
        let comparison = ReceiptComparison::new(manifest, anchor)?;
        let mut session = Self {
            directory: directory.to_owned(),
            owner,
            identity,
            manifest,
            comparison,
            previous,
            chunks: 0,
            disk_bytes,
            blocks: 0,
            events: 0,
            empty_blocks: 0,
            pending: Vec::new(),
            last_header: None,
            failed: false,
        };
        for sequence in paths {
            cancelled(check)?;
            if sequence != session.chunks {
                return Err(invalid("audit checkpoint chain has a missing chunk"));
            }
            let raw = read_bounded(&directory.join(chunk_name(sequence)), MAX_CHUNK)?;
            let (previous, entries) = decode_chunk(
                &raw,
                sequence,
                session.previous,
                session.identity.limits.checkpoint_blocks,
            )?;
            for (header, events) in entries {
                cancelled(check)?;
                session.comparison.restore_compared_block(&header, events)?;
                session.count(&header, events)?;
            }
            session.previous = previous;
            session.chunks += 1;
        }
        session.manifest.validate()?;
        cancelled(check)?;
        Ok(session)
    }

    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn identity(&self) -> &AuditJobIdentity {
        &self.identity
    }
    pub fn compared_blocks(&self) -> u64 {
        self.blocks
    }
    pub fn compared_events(&self) -> u64 {
        self.events
    }
    pub fn next_block_number(&self) -> Option<u64> {
        self.comparison.next_block_number()
    }

    /// Reuse a durable locally verified prefix without inventing a new consensus
    /// anchor at the next block. Its complete recorded ancestry remains bound to
    /// the original finalized anchor. No fetch begins until next_block is called.
    pub fn fetcher(
        &self,
        limits: RepairFetchLimits,
        cancellation: CancellationToken,
    ) -> eyre::Result<RepairFetcher> {
        match self.continuation()? {
            Some(prefix) => RepairFetcher::resume_audit(prefix, limits, cancellation),
            None => RepairFetcher::new(
                RepairRange {
                    start: self.identity.manifest.range.from,
                    end: self.identity.manifest.range.through,
                },
                self.identity.anchor,
                limits,
                cancellation,
            ),
        }
    }

    pub fn compare(&mut self, block: &VerifiedRepairBlock) -> io::Result<()> {
        if self.failed {
            return Err(invalid("audit session is terminal after failure"));
        }
        self.failed = true;
        self.comparison.compare(block)?;
        self.count(block.header(), block.rows().len() as u64)?;
        self.pending
            .push((block.header().clone(), block.rows().len() as u64));
        self.failed = false;
        if self.pending.len() >= self.identity.limits.checkpoint_blocks {
            self.checkpoint()?;
        }
        Ok(())
    }
    fn count(&mut self, header: &Header, events: u64) -> io::Result<()> {
        self.blocks = self
            .blocks
            .checked_add(1)
            .ok_or_else(|| invalid("audit checkpoint block overflow"))?;
        self.events = self
            .events
            .checked_add(events)
            .ok_or_else(|| invalid("audit checkpoint event overflow"))?;
        self.empty_blocks += u64::from(events == 0);
        self.last_header = Some(header.clone());
        Ok(())
    }

    pub fn checkpoint(&mut self) -> io::Result<()> {
        if self.failed {
            return Err(invalid("audit session is terminal after failure"));
        }
        self.manifest.validate()?;
        if self.pending.is_empty() {
            return Ok(());
        }
        self.failed = true;
        if self.chunks >= self.identity.limits.max_chunks {
            return Err(invalid("audit chunk allowance exhausted"));
        }
        let bytes = encode_chunk(self.chunks, self.previous, &self.pending)?;
        let next_bytes = self
            .disk_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid("audit journal size overflow"))?;
        if next_bytes > self.identity.limits.max_bytes {
            return Err(invalid("audit journal byte allowance exhausted"));
        }
        publish(
            &self.directory,
            &chunk_name(self.chunks),
            ".audit-chunk-",
            &bytes,
        )?;
        self.owner.0.sync_all()?;
        self.manifest.validate()?;
        self.previous = B256::from_slice(&bytes[bytes.len() - 32..]);
        self.disk_bytes = next_bytes;
        self.chunks += 1;
        self.pending.clear();
        self.failed = false;
        Ok(())
    }

    pub(crate) fn continuation(&self) -> io::Result<Option<AuditedContinuation>> {
        if self.failed || !self.pending.is_empty() {
            return Err(invalid(
                "audit continuation requires a successful checkpoint",
            ));
        }
        self.manifest.validate()?;
        Ok(self
            .last_header
            .clone()
            .map(|last_header| AuditedContinuation {
                anchor: self.identity.anchor,
                range: self.identity.manifest.range,
                last_header,
                blocks: self.blocks,
            }))
    }

    /// Provisional comparison result. The caller must re-admit the captured
    /// finalized anchor and current source before publishing acceptance. A fully
    /// checkpointed job never needs to download its original range again.
    pub fn finish(mut self) -> io::Result<ReceiptComparisonReport> {
        self.checkpoint()?;
        self.comparison.finish()
    }
}

fn same_selection(a: &AuditManifestSummary, b: &AuditManifestSummary) -> bool {
    a.version == b.version
        && a.range == b.range
        && a.selected_namespace_fingerprint == b.selected_namespace_fingerprint
        && a.selected_rows == b.selected_rows
        && a.nonempty_blocks == b.nonempty_blocks
        && a.runs == b.runs
        && a.record_bytes == b.record_bytes
        && a.records_digest == b.records_digest
}
fn chunk_name(sequence: u64) -> String {
    format!("chunk-{sequence:020}.bin")
}
fn valid_staging_name(name: &str) -> bool {
    [".audit-identity-", ".audit-chunk-"].iter().any(|p| {
        name.strip_prefix(p)
            .is_some_and(|v| v.len() == 32 && v.bytes().all(|b| b.is_ascii_hexdigit()))
    })
}
fn publish(directory: &Path, name: &str, prefix: &str, bytes: &[u8]) -> io::Result<()> {
    let destination = directory.join(name);
    match fs::symlink_metadata(&destination) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Ok(_) => return Err(invalid("audit publication already exists")),
        Err(e) => return Err(e),
    }
    let mut staged = logex_fs::StagedFile::new_in(directory, prefix)?;
    staged.as_file_mut().write_all(bytes)?;
    staged.as_file().sync_all()?;
    staged.persist(&destination)?;
    File::open(directory)?.sync_all()
}
fn read_bounded(path: &Path, max: usize) -> io::Result<Vec<u8>> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(invalid("audit artifact is not an ordinary file"));
    }
    let mut file = File::open(path)?;
    let size = usize::try_from(file.metadata()?.len()).map_err(io::Error::other)?;
    if size > max {
        return Err(invalid("audit artifact exceeds decode allowance"));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(size).map_err(io::Error::other)?;
    (&mut file).take(size as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() != size {
        return Err(invalid("audit artifact changed during read"));
    }
    Ok(bytes)
}
fn checked_payload<'a>(bytes: &'a [u8], magic: &[u8; 8]) -> io::Result<(&'a [u8], B256)> {
    if bytes.len() < 40 || &bytes[..8] != magic {
        return Err(invalid("audit artifact framing/version mismatch"));
    }
    let body = &bytes[..bytes.len() - 32];
    let hash = digest(body);
    if hash.as_slice() != &bytes[bytes.len() - 32..] {
        return Err(invalid("audit artifact checksum mismatch"));
    }
    Ok((&body[8..], hash))
}
fn read_identity(directory: &Path) -> io::Result<(AuditJobIdentity, B256, u64)> {
    let bytes = read_bounded(&directory.join(IDENTITY), MAX_IDENTITY)?;
    let (payload, hash) = checked_payload(&bytes, ID_MAGIC)?;
    let id: AuditJobIdentity = serde_json::from_slice(payload).map_err(io::Error::other)?;
    if id.version != 1
        || id.manifest.version != 1
        || id.anchor.block_number != id.manifest.range.through
        || id.manifest.range.from > id.manifest.range.through
    {
        return Err(invalid("invalid audit job identity"));
    }
    id.limits.validate()?;
    Ok((id, hash, bytes.len() as u64))
}
fn encode_chunk(sequence: u64, previous: B256, entries: &[(Header, u64)]) -> io::Result<Vec<u8>> {
    if entries.is_empty() || entries.len() > MAX_CHECKPOINT_BLOCKS {
        return Err(invalid("invalid audit checkpoint block count"));
    }
    let mut bytes = CHUNK_MAGIC.to_vec();
    bytes.extend(sequence.to_le_bytes());
    bytes.extend(previous.as_slice());
    bytes.extend((entries.len() as u32).to_le_bytes());
    for (header, events) in entries {
        let length = header.length();
        if length > MAX_HEADER {
            return Err(invalid("audit header exceeds journal allowance"));
        }
        bytes.try_reserve(length + 12).map_err(io::Error::other)?;
        bytes.extend((length as u32).to_le_bytes());
        bytes.extend(events.to_le_bytes());
        header.encode(&mut bytes);
    }
    let hash = digest(&bytes);
    bytes.extend(hash.as_slice());
    Ok(bytes)
}
fn decode_chunk(
    bytes: &[u8],
    sequence: u64,
    previous: B256,
    max_blocks: usize,
) -> io::Result<(B256, Vec<(Header, u64)>)> {
    let (mut payload, hash) = checked_payload(bytes, CHUNK_MAGIC)?;
    if payload.len() < 44 {
        return Err(invalid("audit checkpoint is truncated"));
    }
    let seq = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let prev = B256::from_slice(&payload[8..40]);
    let count = u32::from_le_bytes(payload[40..44].try_into().unwrap()) as usize;
    payload = &payload[44..];
    if seq != sequence || prev != previous || count == 0 || count > max_blocks {
        return Err(invalid("audit checkpoint chain/order/count mismatch"));
    }
    let mut entries = Vec::new();
    entries.try_reserve_exact(count).map_err(io::Error::other)?;
    for _ in 0..count {
        if payload.len() < 12 {
            return Err(invalid("audit checkpoint header frame is truncated"));
        }
        let length = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
        let events = u64::from_le_bytes(payload[4..12].try_into().unwrap());
        payload = &payload[12..];
        if length > MAX_HEADER || length > payload.len() || events > u64::from(u32::MAX) + 1 {
            return Err(invalid("audit checkpoint header/event allowance exceeded"));
        }
        let (mut encoded, rest) = payload.split_at(length);
        let header = Header::decode(&mut encoded).map_err(io::Error::other)?;
        if !encoded.is_empty() {
            return Err(invalid("trailing audit header bytes"));
        }
        entries.push((header, events));
        payload = rest;
    }
    if !payload.is_empty() {
        return Err(invalid("trailing audit checkpoint records"));
    }
    Ok((hash, entries))
}

#[cfg(test)]
mod tests;
