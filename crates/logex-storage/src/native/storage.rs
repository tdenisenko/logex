use std::collections::BTreeSet;
use std::fs::{self, File, TryLockError};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use alloy_consensus::{BlockHeader, Header};
use alloy_primitives::B256;
use logex_types::{ChainAnchors, ExecutionAnchor, ExecutionBlockMarker, LogRow, PartitionMeta};

use super::recovery::{IngestRoute, RecoveryJournal};
#[cfg(test)]
use super::segment::append_rows;
use crate::durability::{self, Publication};
use crate::state::SyncHead;
use crate::wal::{EncodedWalBatch, WriteAheadLog};
use crate::{ColumnFile, NullBitmap, SegmentReader};

use super::catalog::{
    NativeStorageCatalog, NativeStorageConfig, SegmentDescriptor, SegmentKind, SegmentManifest,
    StorageCatalogPaths, StorageState, validate_cached_headers,
};
use super::segment::{
    append_compacted_rows, append_ingest_rows, apply_ordered_rows_to_descriptor,
    apply_rows_to_descriptor, compact_ingest_segment, compact_segment, persist_ingest_manifest,
    persist_ingest_manifest_with_columns, persist_segment_manifest,
    segment_uses_current_compaction_profile, verify_raw_segment_files_complete, write_bundled_rows,
};

const HISTORICAL_STAGING_MAX_BLOCK_SPAN: u64 = 65_536;

#[derive(Debug, Clone, Copy)]
enum CompactionMode {
    RawOnly,
    ProfileRewrite,
    CurrentProfile,
}

#[derive(Debug, Clone, Copy)]
enum CompactionOrder {
    OldestFirst,
    NewestFirst,
}

#[derive(Debug)]
struct DataDirectoryLock(File);

impl Drop for DataDirectoryLock {
    fn drop(&mut self) {
        // Closing this fd alone can leave a lock alive in a descriptor inherited
        // during another thread's fork/exec. Release it when the last managed
        // storage/compaction owner disappears, regardless of such duplicates.
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to explicitly unlock data directory before closing it");
        }
    }
}

#[derive(Debug, Clone)]
pub struct SegmentCompactionTask {
    // A background plan can outlive the storage handle that created it.
    _directory_lock: Arc<DataDirectoryLock>,
    paths: StorageCatalogPaths,
    descriptor: SegmentDescriptor,
}

impl SegmentCompactionTask {
    pub fn segment_id(&self) -> u64 {
        self.descriptor.id
    }

    pub fn compact(&self) -> std::io::Result<()> {
        compact_segment(&self.paths, &self.descriptor).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to compact storage segment {} rows={} blocks=[{}, {}]: {error}",
                    self.descriptor.id,
                    self.descriptor.row_count,
                    self.descriptor
                        .min_block
                        .map_or_else(|| "?".to_owned(), |block| block.to_string()),
                    self.descriptor
                        .max_block
                        .map_or_else(|| "?".to_owned(), |block| block.to_string())
                ),
            )
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct SegmentCompactionPlan {
    tasks: Vec<SegmentCompactionTask>,
}

impl SegmentCompactionPlan {
    fn new(
        paths: StorageCatalogPaths,
        descriptors: Vec<SegmentDescriptor>,
        directory_lock: Arc<DataDirectoryLock>,
    ) -> Self {
        let tasks = descriptors
            .into_iter()
            .map(|descriptor| SegmentCompactionTask {
                _directory_lock: Arc::clone(&directory_lock),
                paths: paths.clone(),
                descriptor,
            })
            .collect();
        Self { tasks }
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub fn compact(&self) -> std::io::Result<usize> {
        for task in &self.tasks {
            task.compact()?;
        }
        Ok(self.tasks.len())
    }
}

// Bound accumulated ingestion payload, whether retained in the generic WAL or
// re-fetchable by sync. A valid oversized caller batch checkpoints before return.
const CHECKPOINT_INGEST_BYTES: u64 = 32 * 1024 * 1024;

#[cfg(test)]
thread_local! {
    static CHECKPOINT_CLOCK: std::cell::Cell<Option<std::time::Instant>> = const { std::cell::Cell::new(None) };
}

fn checkpoint_now() -> std::time::Instant {
    #[cfg(test)]
    if let Some(now) = CHECKPOINT_CLOCK.get() {
        return now;
    }
    std::time::Instant::now()
}

struct PendingCheckpoint {
    journal: RecoveryJournal,
    bytes: u64,
    rows: u32,
    checksum: crc32fast::Hasher,
    started_at: std::time::Instant,
}

struct IngestionOrigin {
    route: IngestRoute,
    start: Option<SegmentDescriptor>,
    next_segment_id: u64,
}

impl IngestionOrigin {
    fn can_defer(&self, id: u64) -> bool {
        id >= self.next_segment_id
            || self
                .start
                .as_ref()
                .is_some_and(|start| start.id == id && start.row_count == 0)
    }

    fn includes(&self, id: u64) -> bool {
        self.start.as_ref().is_some_and(|start| start.id == id) || id >= self.next_segment_id
    }
}

struct PendingIngestion {
    origin: IngestionOrigin,
    bytes: u64,
    batches: u32,
    started_at: std::time::Instant,
}

pub struct NativeStorage {
    config: NativeStorageConfig,
    paths: StorageCatalogPaths,
    catalog: NativeStorageCatalog,
    wal: WriteAheadLog,
    recovery_required: bool,
    pending_checkpoint: Option<PendingCheckpoint>,
    pending_ingestion: Option<PendingIngestion>,
    published_ingestion: Option<PendingIngestion>,
    directory_lock: Arc<DataDirectoryLock>,
}

impl NativeStorage {
    pub fn open(config: NativeStorageConfig) -> std::io::Result<Self> {
        durability::create_dir_all(&config.data_dir)?;
        // Lock the directory inode without creating a lock file. Alternative
        // path spellings that resolve to the same directory must also conflict.
        let directory_lock = File::open(&config.data_dir)?;
        directory_lock.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::WouldBlock,
                format!(
                    "data directory {} is already in use",
                    config.data_dir.display()
                ),
            ),
            TryLockError::Error(error) => io::Error::new(
                error.kind(),
                format!(
                    "cannot lock data directory {}: {error}",
                    config.data_dir.display()
                ),
            ),
        })?;
        let directory_lock = Arc::new(DataDirectoryLock(directory_lock));
        let (catalog, paths) = NativeStorageCatalog::open_or_create(&config)?;
        verify_recent_headers(&catalog.state)?;
        let wal = WriteAheadLog::open(config.data_dir.join("wal").join("pending.wal"))?;

        let mut storage = Self {
            config,
            paths,
            catalog,
            wal,
            recovery_required: false,
            pending_checkpoint: None,
            pending_ingestion: None,
            published_ingestion: None,
            directory_lock,
        };

        storage.verify_recovery_evidence()?;
        // A prior process may have published a sync catalog without completing
        // its device flush. Make the catalog observed on this open durable
        // before recovery or maintenance can retire any of its predecessors.
        durability::sync_directory(storage.paths.root())?;
        storage.restore_catalog_checkpoint()?;
        storage.ensure_active_hot_segment()?;
        storage.replay_wal()?;
        storage.repair_recoverable_hot_segment_artifacts()?;
        storage.repair_recoverable_historical_segment_artifacts()?;
        storage.verify_integrity()?;
        Ok(storage)
    }

    pub fn data_dir(&self) -> &Path {
        self.paths.root()
    }

    pub fn sync_head(&self) -> Option<SyncHead> {
        self.catalog.state.sync_head
    }

    pub fn recent_headers(&self) -> &[Header] {
        &self.catalog.state.recent_headers
    }

    pub fn historical_floor_header(&self) -> Option<&Header> {
        self.catalog.state.historical_floor_header.as_ref()
    }

    pub fn historical_anchor_header(&self) -> Option<&Header> {
        self.catalog.state.historical_anchor_header.as_ref()
    }

    pub fn historical_floor(&self) -> Option<ExecutionBlockMarker> {
        self.catalog
            .state
            .historical_floor_header
            .as_ref()
            .map(execution_marker_from_header)
    }

    pub fn historical_anchor(&self) -> Option<ExecutionBlockMarker> {
        self.catalog
            .state
            .historical_anchor_header
            .as_ref()
            .map(execution_marker_from_header)
    }

    pub fn record_sync_head(
        &mut self,
        block_number: u64,
        block_hash: B256,
        timestamp: u64,
    ) -> std::io::Result<()> {
        self.ensure_writable()?;
        if self.pending_ingestion.is_some() || self.published_ingestion.is_some() {
            self.checkpoint_durable()?;
        }
        let next = SyncHead {
            block_number,
            block_hash,
            timestamp,
        };
        if self.catalog.state.sync_head == Some(next) {
            return Ok(());
        }

        self.catalog.state.sync_head = Some(next);
        self.persist_state()
    }

    pub fn record_canonical_state(
        &mut self,
        header: &Header,
        recent_headers: &[Header],
    ) -> std::io::Result<()> {
        self.ensure_writable()?;
        validate_cached_headers(recent_headers)?;
        if self.pending_ingestion.is_some() || self.published_ingestion.is_some() {
            self.checkpoint_durable()?;
        }
        let next_sync_head = SyncHead {
            block_number: header.number(),
            block_hash: header.hash_slow(),
            timestamp: header.timestamp(),
        };
        let next_recent_headers = recent_headers.to_vec();

        if self.catalog.state.sync_head == Some(next_sync_head)
            && self.catalog.state.recent_headers == next_recent_headers
        {
            return Ok(());
        }

        self.catalog.state.sync_head = Some(next_sync_head);
        self.catalog.state.recent_headers = next_recent_headers;
        self.persist_state()
    }

    pub fn record_verified_canonical_state(
        &mut self,
        anchor: &ExecutionAnchor,
        header: &Header,
        recent_headers: &[Header],
    ) -> std::io::Result<()> {
        self.ensure_writable()?;
        validate_cached_headers(recent_headers)?;
        if self.pending_ingestion.is_some() || self.published_ingestion.is_some() {
            self.checkpoint_durable()?;
        }
        self.record_canonical_state(header, recent_headers)?;

        let mut anchors = self.catalog.anchors.clone();
        anchors.indexed_head = Some(*anchor);
        self.record_chain_anchors(anchors)
    }

    pub fn record_historical_floor(&mut self, header: &Header) -> std::io::Result<()> {
        validate_cached_headers(std::slice::from_ref(header))?;
        self.ensure_writable()?;
        if self.pending_ingestion.is_some() || self.published_ingestion.is_some() {
            self.checkpoint_durable()?;
        }
        if let Some(current) = self.catalog.state.historical_floor_header.as_ref()
            && current.number() <= header.number()
        {
            return Ok(());
        }

        self.catalog.state.historical_floor_header = Some(header.clone());
        if self.catalog.state.historical_anchor_header.is_none() {
            self.catalog.state.historical_anchor_header = Some(header.clone());
        }
        self.persist_state()
    }

    pub fn chain_anchors(&self) -> ChainAnchors {
        self.catalog.anchors.clone()
    }

    pub fn record_chain_anchors(&mut self, anchors: ChainAnchors) -> std::io::Result<()> {
        self.ensure_writable()?;
        if self.pending_ingestion.is_some() || self.published_ingestion.is_some() {
            self.checkpoint_durable()?;
        }
        if self.catalog.anchors == anchors {
            return Ok(());
        }

        self.catalog.anchors = anchors;
        self.persist_catalog()
    }

    pub fn rewind_canonical_state(
        &mut self,
        recent_headers: &[Header],
        indexed_head: Option<ExecutionAnchor>,
    ) -> std::io::Result<()> {
        self.ensure_writable()?;
        validate_cached_headers(recent_headers)?;
        self.checkpoint_durable()?;
        let next_sync_head = recent_headers.last().map(|header| SyncHead {
            block_number: header.number(),
            block_hash: header.hash_slow(),
            timestamp: header.timestamp(),
        });
        let next_recent_headers = recent_headers.to_vec();
        let mut next_anchors = self.catalog.anchors.clone();
        next_anchors.indexed_head = indexed_head;

        let state_changed = self.catalog.state.sync_head != next_sync_head
            || self.catalog.state.recent_headers != next_recent_headers;
        let anchors_changed = self.catalog.anchors != next_anchors;

        self.catalog.state.sync_head = next_sync_head;
        self.catalog.state.recent_headers = next_recent_headers;
        self.catalog.anchors = next_anchors;

        if state_changed {
            self.persist_state()?;
        }
        if anchors_changed {
            self.persist_catalog()?;
        }

        Ok(())
    }

    /// Publish validated canonical rows and their restart marker together.
    /// Restart may re-fetch the bounded sync window since the last full flush.
    /// Call `checkpoint_durable` before requiring
    /// the latest progress to survive power loss.
    pub fn ingest_canonical_batch(
        &mut self,
        rows: &[LogRow],
        header: &Header,
        recent_headers: &[Header],
        anchor: Option<&ExecutionAnchor>,
    ) -> io::Result<()> {
        validate_cached_headers(recent_headers)?;
        let hash = header.hash_slow();
        if recent_headers.last() != Some(header)
            || rows.iter().any(|row| row.block_number > header.number)
            || anchor.is_some_and(|anchor| {
                anchor.block_number != header.number || anchor.block_hash != hash
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "canonical ingestion progress does not match its batch",
            ));
        }
        self.begin_ingestion(rows, IngestRoute::Live)?;
        self.commit_rows_to_segments(rows)?;
        durability::checkpoint("ingestion_rows_applied", self.paths.root())?;
        self.catalog.state.sync_head = Some(SyncHead {
            block_number: header.number,
            block_hash: hash,
            timestamp: header.timestamp,
        });
        self.catalog.state.recent_headers = recent_headers.to_vec();
        if let Some(anchor) = anchor {
            self.catalog.anchors.indexed_head = Some(*anchor);
            self.advance_historical_floor(header);
        }
        self.complete_ingestion_batch()
    }

    /// Publish a complete validated historical chunk and its floor together.
    /// Uncheckpointed chunks may be re-fetched after restart, including empty blocks.
    pub fn ingest_historical_batch(&mut self, rows: &[LogRow], floor: &Header) -> io::Result<()> {
        validate_cached_headers(std::slice::from_ref(floor))?;
        if rows.iter().any(|row| row.block_number < floor.number) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "historical rows precede their ingestion floor",
            ));
        }
        self.begin_ingestion(rows, IngestRoute::Historical)?;
        if !rows.is_empty() {
            self.write_historical_rows(rows)?;
        }
        durability::checkpoint("ingestion_rows_applied", self.paths.root())?;
        self.advance_historical_floor(floor);
        self.complete_ingestion_batch()
    }

    fn advance_historical_floor(&mut self, header: &Header) {
        if self
            .catalog
            .state
            .historical_floor_header
            .as_ref()
            .is_none_or(|current| current.number > header.number)
        {
            self.catalog.state.historical_floor_header = Some(header.clone());
            if self.catalog.state.historical_anchor_header.is_none() {
                self.catalog.state.historical_anchor_header = Some(header.clone());
            }
        }
    }

    fn begin_ingestion(&mut self, rows: &[LogRow], route: IngestRoute) -> io::Result<()> {
        self.ensure_writable()?;
        let bytes = crate::wal::validated_payload_len(rows)? as u64;
        if self.pending_checkpoint.is_some()
            || self.ingestion_checkpoint_due(bytes)
            || [&self.pending_ingestion, &self.published_ingestion]
                .into_iter()
                .flatten()
                .any(|pending| pending.origin.route != route)
        {
            self.checkpoint_durable()?;
        }
        self.recovery_required = true;
        if self.pending_ingestion.is_none() {
            let start_id = match route {
                IngestRoute::Live => self.catalog.active_hot_segment,
                IngestRoute::Historical => self.catalog.active_historical_segment,
            };
            let start = start_id
                .map(|id| {
                    self.catalog
                        .segments
                        .iter()
                        .find(|segment| segment.id == id)
                        .cloned()
                        .ok_or_else(|| io::Error::other("ingestion starting segment is missing"))
                })
                .transpose()?;
            self.pending_ingestion = Some(PendingIngestion {
                origin: IngestionOrigin {
                    route,
                    start,
                    next_segment_id: self.catalog.next_segment_id,
                },
                bytes: 0,
                batches: 0,
                started_at: checkpoint_now(),
            });
        }
        let pending = self
            .pending_ingestion
            .as_mut()
            .ok_or_else(|| io::Error::other("ingestion checkpoint disappeared"))?;
        pending.bytes = pending
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("ingestion byte count overflow"))?;
        pending.batches += 1;
        Ok(())
    }

    fn segment_publication(&self, id: u64) -> Publication {
        if self.pending_ingestion.as_ref().is_some_and(|pending| {
            pending.origin.can_defer(id)
                || (pending.origin.includes(id)
                    && self
                        .catalog
                        .segments
                        .iter()
                        .any(|segment| segment.id == id && segment.column_bundle.is_some()))
        }) {
            Publication::Deferred
        } else if self.pending_checkpoint.is_some() || self.pending_ingestion.is_some() {
            Publication::Ordered
        } else {
            Publication::Durable
        }
    }

    /// Include every published epoch since the last full flush. Resetting these
    /// bounds on each catalog rename would allow an unbounded recovery window.
    fn ingestion_checkpoint_due(&self, additional_bytes: u64) -> bool {
        let mut bytes = additional_bytes;
        let mut batches = 0_u32;
        for pending in [&self.pending_ingestion, &self.published_ingestion]
            .into_iter()
            .flatten()
        {
            bytes = bytes.saturating_add(pending.bytes);
            batches = batches.saturating_add(pending.batches);
            if checkpoint_now().saturating_duration_since(pending.started_at)
                >= std::time::Duration::from_secs(5)
            {
                return true;
            }
        }
        bytes >= CHECKPOINT_INGEST_BYTES || batches >= 64
    }

    fn complete_ingestion_batch(&mut self) -> io::Result<()> {
        self.recovery_required = false;
        if self.ingestion_checkpoint_due(0) {
            self.checkpoint_durable()?;
        }
        Ok(())
    }

    fn checkpoint_ingestion(
        &mut self,
        pending: PendingIngestion,
        progress_durable: bool,
    ) -> io::Result<()> {
        self.recovery_required = true;
        let mut repacked = Vec::new();
        for segment in &mut self.catalog.segments {
            if pending.origin.includes(segment.id)
                && let Some(replacement) =
                    super::segment::repack_sparse_bundle(&self.paths, segment)?
            {
                repacked.push(replacement);
            }
        }
        let deferred = self
            .catalog
            .segments
            .iter()
            .filter(|segment| {
                pending.origin.can_defer(segment.id)
                    || (pending.origin.includes(segment.id) && segment.column_bundle.is_some())
            })
            .map(|segment| self.paths.segment_dir(segment.id))
            .collect::<Vec<_>>();
        let bytes = self.catalog.encode()?;
        if progress_durable || !repacked.is_empty() {
            durability::publish_catalog_after_trees(
                deferred.iter().map(PathBuf::as_path),
                &self.paths.catalog_path(),
                &bytes,
            )?;
            self.published_ingestion = None;
            // A preceding catalog can still select the old file until this
            // strong publication returns successfully. Keep ownership throughout.
            for replacement in repacked {
                replacement.retire()?;
            }
        } else {
            durability::publish_ingestion_catalog_after_trees(
                deferred.iter().map(PathBuf::as_path),
                &self.paths.catalog_path(),
                &bytes,
            )?;
            if let Some(published) = self.published_ingestion.as_mut() {
                // Preserve the earliest origin and deadline: an older catalog
                // can still be the last complete checkpoint on stable media.
                published.bytes = published
                    .bytes
                    .checked_add(pending.bytes)
                    .ok_or_else(|| io::Error::other("published ingestion byte count overflow"))?;
                published.batches = published
                    .batches
                    .checked_add(pending.batches)
                    .ok_or_else(|| io::Error::other("published ingestion batch count overflow"))?;
            } else {
                self.published_ingestion = Some(pending);
            }
        }
        self.recovery_required = false;
        Ok(())
    }

    /// Validate recovery evidence before rollback can remove any uncommitted
    /// artifacts. An unreadable/mismatched WAL must remain inspectable intact.
    fn verify_recovery_evidence(&self) -> io::Result<()> {
        let journal = RecoveryJournal::load(&self.paths)?;
        let rows = self.wal.read_all()?;
        if let Some(journal) = journal {
            self.verify_wal_transaction(&journal, &rows)?;
        } else if !rows.is_empty() {
            self.reject_ambiguous_legacy_wal(&rows)?;
        }
        Ok(())
    }

    fn verify_wal_transaction(
        &self,
        journal: &RecoveryJournal,
        rows: &[LogRow],
    ) -> io::Result<Vec<LogRow>> {
        let max_rows = if journal.is_active_checkpoint() {
            u32::try_from(rows.len())
                .map_err(|_| io::Error::other("checkpoint row count exceeds u32"))?
        } else {
            journal.row_count
        };
        let applied = self.read_applied_wal_rows(journal, max_rows)?;
        if rows.is_empty() {
            for segment in self.catalog.segments.iter().filter(|segment| {
                segment.id == journal.start.id || segment.id >= journal.next_segment_id
            }) {
                verify_segment_integrity(&self.paths, segment)?;
                let dir = self.paths.segment_dir(segment.id);
                // Compacted segments retain canonical.bitmap at the root; it
                // does not imply that the removed raw columns still exist.
                if !super::segment::segment_is_compacted(&self.paths, segment.id)?
                    && has_raw_segment_artifacts(&dir)?
                    && hot_segment_physical_row_counts(segment, &dir)?
                        .iter()
                        .any(|(_, count)| *count != segment.row_count)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "WAL is empty but its segment contains unexplained physical rows",
                    ));
                }
            }
            if applied.is_empty() {
                if journal.is_complete_checkpoint() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "completed checkpoint has an empty WAL and missing committed rows",
                    ));
                }
                // Journal publication preceded the WAL append; no rows committed.
                return Ok(applied);
            }
            let batch = EncodedWalBatch::new(&applied)?;
            if batch.row_count != journal.row_count || batch.checksum != journal.payload_checksum {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL is empty but its recovery journal describes an incomplete or mismatched commit",
                ));
            }
            // Crash after durable WAL truncation, before journal removal.
            return Ok(applied);
        }
        let batch = EncodedWalBatch::new(rows)?;
        if !journal.is_active_checkpoint()
            && (batch.row_count != journal.row_count || batch.checksum != journal.payload_checksum)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL payload does not match its recovery journal",
            ));
        }
        if rows.get(..applied.len()) != Some(applied.as_slice()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "committed segment rows do not match the journaled WAL prefix",
            ));
        }
        Ok(applied)
    }

    /// The checksummed catalog alone identifies durable rows and progress.
    /// Never adopt manifests/new segments left by an unfinished ingestion epoch.
    fn restore_catalog_checkpoint(&mut self) -> io::Result<()> {
        self.recovery_required = true;
        for segment in &self.catalog.segments {
            let dir = self.paths.segment_dir(segment.id);
            if !dir.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("committed segment {} is unavailable", segment.id),
                ));
            }
            if segment.column_bundle.is_some() {
                super::segment::restore_bundled_checkpoint(&self.paths, segment)?;
                super::segment::retire_unreferenced_bundles(&self.paths, segment)?;
                continue;
            }
            let active = Some(segment.id) == self.catalog.active_hot_segment
                || Some(segment.id) == self.catalog.active_historical_segment;
            let manifest = match fs::read(self.paths.segment_manifest_path(segment.id)) {
                Ok(bytes) => match serde_json::from_slice::<SegmentManifest>(&bytes) {
                    Ok(manifest) => Some(manifest),
                    Err(_) if active && segment.row_count == 0 => None,
                    Err(error) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
                },
                Err(error) if error.kind() == io::ErrorKind::NotFound && active => None,
                Err(error) => return Err(error),
            };
            if manifest
                .as_ref()
                .is_some_and(|manifest| manifest.segment_id != segment.id)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "catalog and manifest segment identities disagree",
                ));
            }
            // A first bundle may have reached disk before its derived manifest.
            // The catalog's empty prefix remains authoritative in either case.
            if active
                && segment.row_count == 0
                && (manifest.is_none()
                    || manifest
                        .as_ref()
                        .is_some_and(|manifest| manifest.column_bundle.is_some())
                    || dir.join(crate::column_artifact::BUNDLE_PATH).try_exists()?)
            {
                self.restore_committed_prefix(segment)?;
                continue;
            }
            let compacted = super::segment::segment_is_compacted(&self.paths, segment.id)?;
            let mut restore = active
                && manifest.as_ref().is_none_or(|manifest| {
                    manifest.row_count != segment.row_count || manifest.kind != segment.kind
                });
            if active && compacted {
                let mut prefix = manifest.clone().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing compacted manifest")
                })?;
                prefix.row_count = segment.row_count;
                restore |= super::segment::compacted_segment_has_uncommitted_tail(&dir, &prefix)?;
            }
            if active && !compacted && has_raw_segment_artifacts(&dir)? {
                match hot_segment_physical_row_counts(segment, &dir) {
                    Ok(counts) => {
                        if counts.iter().any(|(_, count)| *count < segment.row_count) {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "missing committed raw segment rows; verified repair is required",
                            ));
                        }
                        restore |= counts.iter().any(|(_, count)| *count != segment.row_count);
                    }
                    Err(_) if segment.row_count == 0 => restore = true,
                    Err(error) => return Err(error),
                }
            }
            if restore {
                self.restore_committed_prefix(segment)?;
            }
        }
        let mut removed = false;
        for entry in fs::read_dir(self.paths.segments_dir())? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(id) = parse_segment_dir_name(&name) else {
                continue;
            };
            if name != format!("s_{id:016}").as_str() {
                continue;
            }
            if id >= self.catalog.next_segment_id {
                durability::checkpoint("ingestion_remove_uncommitted_segment", &entry.path())?;
                fs::remove_dir_all(entry.path())?;
                removed = true;
            } else if !self.catalog.segments.iter().any(|segment| segment.id == id) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexplained segment below catalog allocation boundary; verified repair is required",
                ));
            }
        }
        if removed {
            durability::sync_directory(&self.paths.segments_dir())?;
        }
        self.recovery_required = false;
        Ok(())
    }

    fn restore_committed_prefix(&self, start: &SegmentDescriptor) -> io::Result<()> {
        if start.column_bundle.is_some() {
            return super::segment::restore_bundled_checkpoint(&self.paths, start);
        }
        let dir = self.paths.segment_dir(start.id);
        let mut rows = Vec::new();
        let mut canonical = NullBitmap::new();
        if start.row_count > 0 {
            let reader = SegmentReader::open(&dir)?;
            let previous = reader.read_canonical()?;
            if previous.len() < start.row_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ingestion rollback cannot recover missing committed canonical bits",
                ));
            }
            let end = u32::try_from(start.row_count)
                .map_err(|_| io::Error::other("ingestion prefix exceeds row addressing"))?;
            let mut first = 0;
            while first < end {
                let next = first.saturating_add(8192).min(end);
                let ids = (first..next).collect::<Vec<_>>();
                let chunk = reader.read_log_rows(Some(&ids))?;
                if chunk.len() != ids.len() {
                    return Err(io::Error::other("incomplete committed ingestion prefix"));
                }
                rows.try_reserve(chunk.len()).map_err(io::Error::other)?;
                rows.extend(chunk);
                first = next;
            }
            for row in 0..start.row_count {
                canonical.push(previous.is_present(row));
            }
        }
        ColumnFile::write_batch_with_canonical(&dir, &rows, Some(&canonical))?;
        let indexes = dir.join("indexes");
        if indexes.exists() {
            fs::remove_dir_all(indexes)?;
        }
        super::segment::persist_segment_manifest_with_columns(
            &self.paths,
            start,
            super::segment::default_columns(),
        )?;
        // These pages no longer have a published reader after restoring the raw
        // prefix; remove superseded derived representations only after it is durable.
        for name in ["columns", "columns_profile_v2"] {
            let obsolete = dir.join(name);
            if obsolete.exists() {
                fs::remove_dir_all(obsolete)?;
            }
        }
        durability::sync_directory(&dir)?;
        Ok(())
    }

    pub fn write_batch(&mut self, rows: &[logex_types::LogRow]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        self.ensure_writable()?;
        self.begin_checkpoint_batch(rows, IngestRoute::Live)?;
        self.commit_rows_to_segments(rows)?;
        self.complete_checkpoint_batch()
    }

    pub fn write_historical_batch(
        &mut self,
        rows: &[logex_types::LogRow],
    ) -> std::io::Result<Vec<PartitionMeta>> {
        self.ensure_writable()?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        self.begin_checkpoint_batch(rows, IngestRoute::Historical)?;
        let appended = self.write_historical_rows(rows)?;
        self.complete_checkpoint_batch()?;
        Ok(appended)
    }

    fn write_historical_rows(&mut self, rows: &[LogRow]) -> io::Result<Vec<PartitionMeta>> {
        let target_rows = self.config.hot_target_rows.max(1) as usize;
        let dense_threshold = dense_historical_batch_row_threshold(target_rows);
        if rows.len() >= dense_threshold {
            self.finalize_historical_segment()?;

            let compacted_len = compacted_historical_row_prefix_len(rows.len(), target_rows);
            let mut appended =
                self.write_compacted_historical_segments(&rows[..compacted_len], target_rows)?;
            if compacted_len < rows.len() {
                appended.extend(
                    self.write_staged_historical_rows(&rows[compacted_len..], target_rows)?,
                );
            }
            return Ok(appended);
        }

        self.write_staged_historical_rows(rows, target_rows)
    }

    fn write_compacted_historical_segments(
        &mut self,
        rows: &[logex_types::LogRow],
        target_rows: usize,
    ) -> std::io::Result<Vec<PartitionMeta>> {
        let mut appended = Vec::new();
        let mut remaining = rows;
        while !remaining.is_empty() {
            let mut descriptor = self.catalog.allocate_segment(SegmentKind::Sealed)?;
            let segment_dir = self.paths.segment_dir(descriptor.id);
            let candidate = &remaining[..remaining.len().min(target_rows)];
            let (take, _) = super::segment::bundled_row_capacity(&segment_dir, None, 0, candidate)?;
            if take == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "row exceeds bundle capacity",
                ));
            }
            let chunk = &candidate[..take];
            if segment_dir.exists() {
                fs::remove_dir_all(&segment_dir)?;
            }
            let columns = write_bundled_rows(&segment_dir, chunk)?;
            apply_ordered_rows_to_descriptor(&mut descriptor, chunk);
            let columns = columns.apply_to(&mut descriptor);
            persist_ingest_manifest_with_columns(
                &self.paths,
                &descriptor,
                columns,
                self.segment_publication(descriptor.id),
            )?;
            appended.push(self.partition_meta(&descriptor));
            self.catalog.segments.push(descriptor);
            remaining = &remaining[take..];
        }
        if !appended.is_empty() {
            self.persist_catalog()?;
        }
        Ok(appended)
    }

    fn write_staged_historical_rows(
        &mut self,
        rows: &[logex_types::LogRow],
        target_rows: usize,
    ) -> std::io::Result<Vec<PartitionMeta>> {
        let mut touched = BTreeSet::new();
        let mut offset = 0usize;
        while offset < rows.len() {
            let segment_id = self.ensure_active_historical_segment()?;
            let segment_index = self
                .catalog
                .segments
                .iter()
                .position(|segment| segment.id == segment_id)
                .ok_or_else(|| std::io::Error::other("active historical segment is missing"))?;

            let existing_rows = self.catalog.segments[segment_index].row_count;
            let remaining_capacity = target_rows.saturating_sub(existing_rows as usize);
            if remaining_capacity == 0 {
                self.finalize_historical_segment()?;
                continue;
            }

            let remaining_rows = rows.len() - offset;
            let mut take = remaining_capacity.min(remaining_rows);
            let segment_dir = self.paths.segment_dir(segment_id);
            let reference = self.catalog.segments[segment_index].column_bundle.as_ref();
            let mut inspected = None;
            if existing_rows == 0 || reference.is_some() {
                (take, inspected) = super::segment::bundled_row_capacity(
                    &segment_dir,
                    reference,
                    self.catalog.segments[segment_index].generation,
                    &rows[offset..offset + take],
                )?;
                if take == 0 {
                    if existing_rows == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "row exceeds bundle capacity",
                        ));
                    }
                    self.finalize_historical_segment()?;
                    continue;
                }
            }
            let chunk = &rows[offset..offset + take];
            if existing_rows > 0
                && historical_segment_would_exceed_block_span(
                    &self.catalog.segments[segment_index],
                    chunk,
                )
            {
                self.finalize_historical_segment()?;
                continue;
            }

            let publication = self.segment_publication(segment_id);
            let columns = if existing_rows == 0 {
                write_bundled_rows(&segment_dir, chunk)?
            } else {
                if !super::segment::segment_is_compacted(&self.paths, segment_id)? {
                    compact_ingest_segment(
                        &self.paths,
                        &self.catalog.segments[segment_index],
                        publication,
                    )?;
                }
                append_compacted_rows(&segment_dir, existing_rows, chunk, publication, inspected)?
            };
            {
                let descriptor = &mut self.catalog.segments[segment_index];
                apply_ordered_rows_to_descriptor(descriptor, chunk);
                let columns = columns.apply_to(descriptor);
                persist_ingest_manifest_with_columns(
                    &self.paths,
                    descriptor,
                    columns,
                    publication,
                )?;
                touched.insert(descriptor.id);
            }
            self.persist_catalog()?;
            offset += take;

            let should_finalize = {
                let descriptor = &self.catalog.segments[segment_index];
                descriptor.row_count >= target_rows as u64
                    || historical_segment_block_span(descriptor)
                        .is_some_and(|span| span >= HISTORICAL_STAGING_MAX_BLOCK_SPAN)
            };
            if should_finalize {
                self.finalize_historical_segment()?;
            }
        }

        Ok(touched
            .into_iter()
            .filter_map(|segment_id| {
                self.catalog
                    .segments
                    .iter()
                    .find(|segment| segment.id == segment_id)
                    .map(|segment| self.partition_meta(segment))
            })
            .collect())
    }

    pub fn finalize_active_historical_segment(&mut self) -> std::io::Result<bool> {
        self.ensure_writable()?;
        self.recovery_required = true;
        let finalized = self.finalize_historical_segment()?;
        self.recovery_required = false;
        self.checkpoint()?;
        Ok(finalized)
    }

    fn finalize_historical_segment(&mut self) -> std::io::Result<bool> {
        let Some(segment_id) = self.catalog.active_historical_segment else {
            return Ok(false);
        };
        let descriptor = self
            .catalog
            .segments
            .iter()
            .find(|segment| segment.id == segment_id)
            .cloned()
            .ok_or_else(|| std::io::Error::other("active historical segment is missing"))?;

        if descriptor.row_count > 0
            && !segment_uses_current_compaction_profile(&self.paths, segment_id)?
        {
            compact_ingest_segment(
                &self.paths,
                &descriptor,
                self.segment_publication(descriptor.id),
            )?;
        }
        self.catalog.active_historical_segment = None;
        self.persist_catalog()?;
        Ok(true)
    }

    pub fn active_historical_segment_id(&self) -> Option<u64> {
        self.catalog.active_historical_segment
    }

    fn commit_rows_to_segments(&mut self, rows: &[logex_types::LogRow]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let target_rows = self.config.hot_target_rows.max(1);
        let mut offset = 0usize;
        while offset < rows.len() {
            let hot_id = self.ensure_active_hot_segment()?;
            let segment_index = self
                .catalog
                .segments
                .iter()
                .position(|segment| segment.id == hot_id)
                .ok_or_else(|| std::io::Error::other("active hot segment is missing"))?;
            let descriptor = &self.catalog.segments[segment_index];
            let existing_rows = descriptor.row_count;
            let remaining_capacity = target_rows.saturating_sub(existing_rows);
            if remaining_capacity == 0 {
                self.seal_hot_segment()?;
                continue;
            }
            let mut take = remaining_capacity.min((rows.len() - offset) as u64) as usize;
            let hot_dir = self.paths.segment_dir(hot_id);
            // Sync creates immutable bundles directly. Keep an existing raw
            // prefix in its current representation, including row-only WAL users.
            let bundled = descriptor.column_bundle.is_some()
                || (existing_rows == 0 && self.pending_ingestion.is_some());
            let mut inspected = None;
            if bundled {
                (take, inspected) = super::segment::bundled_row_capacity(
                    &hot_dir,
                    descriptor.column_bundle.as_ref(),
                    descriptor.generation,
                    &rows[offset..offset + take],
                )?;
                if take == 0 {
                    if existing_rows == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "row exceeds bundle capacity",
                        ));
                    }
                    self.seal_hot_segment()?;
                    continue;
                }
            }
            let chunk = &rows[offset..offset + take];
            let publication = self.segment_publication(hot_id);
            let columns = if bundled {
                Some(if existing_rows == 0 {
                    write_bundled_rows(&hot_dir, chunk)?
                } else {
                    append_compacted_rows(&hot_dir, existing_rows, chunk, publication, inspected)?
                })
            } else {
                append_ingest_rows(&hot_dir, existing_rows, chunk, publication)?;
                None
            };
            let descriptor = &mut self.catalog.segments[segment_index];
            apply_rows_to_descriptor(descriptor, chunk);
            let should_seal = descriptor.row_count >= target_rows;
            if let Some(columns) = columns {
                let columns = columns.apply_to(descriptor);
                persist_ingest_manifest_with_columns(
                    &self.paths,
                    descriptor,
                    columns,
                    publication,
                )?;
            } else {
                persist_ingest_manifest(&self.paths, descriptor, publication)?;
            }
            self.persist_catalog()?;
            offset += take;
            if should_seal {
                self.seal_hot_segment()?;
            }
        }
        Ok(())
    }

    pub fn compact_eligible_segments(&mut self) -> std::io::Result<usize> {
        self.compact_eligible_segments_limit(usize::MAX)
    }

    pub fn compact_raw_segments_limit(&mut self, limit: usize) -> std::io::Result<usize> {
        let plan = self.raw_segment_compaction_plan(limit)?;
        plan.compact()
    }

    pub fn compact_eligible_segments_limit(&mut self, limit: usize) -> std::io::Result<usize> {
        self.checkpoint_durable()?;
        let plan = self.segment_compaction_plan(limit)?;
        plan.compact()
    }

    pub fn raw_segment_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.ensure_writable()?;
        self.compaction_plan(limit, CompactionMode::RawOnly)
    }

    pub fn recent_raw_segment_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.ensure_writable()?;
        self.compaction_plan_with_order(
            limit,
            CompactionMode::RawOnly,
            CompactionOrder::NewestFirst,
        )
    }

    pub fn segment_compaction_plan(&self, limit: usize) -> std::io::Result<SegmentCompactionPlan> {
        self.ensure_writable()?;
        self.compaction_plan(limit, CompactionMode::CurrentProfile)
    }

    pub fn profile_rewrite_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.ensure_writable()?;
        self.compaction_plan(limit, CompactionMode::ProfileRewrite)
    }

    fn compaction_plan(
        &self,
        limit: usize,
        mode: CompactionMode,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.compaction_plan_with_order(limit, mode, CompactionOrder::OldestFirst)
    }

    fn compaction_plan_with_order(
        &self,
        limit: usize,
        mode: CompactionMode,
        order: CompactionOrder,
    ) -> std::io::Result<SegmentCompactionPlan> {
        if limit == 0 {
            return Ok(SegmentCompactionPlan::default());
        }

        let mut eligible = Vec::new();
        match order {
            CompactionOrder::OldestFirst => {
                for segment in &self.catalog.segments {
                    if eligible.len() >= limit {
                        break;
                    }
                    if self.segment_matches_compaction_mode(segment, mode)? {
                        eligible.push(segment.clone());
                    }
                }
            }
            CompactionOrder::NewestFirst => {
                for segment in self.catalog.segments.iter().rev() {
                    if eligible.len() >= limit {
                        break;
                    }
                    if self.segment_matches_compaction_mode(segment, mode)? {
                        eligible.push(segment.clone());
                    }
                }
            }
        }

        Ok(SegmentCompactionPlan::new(
            self.paths.clone(),
            eligible,
            Arc::clone(&self.directory_lock),
        ))
    }

    fn segment_matches_compaction_mode(
        &self,
        segment: &SegmentDescriptor,
        mode: CompactionMode,
    ) -> std::io::Result<bool> {
        if Some(segment.id) == self.catalog.active_historical_segment
            || self
                .pending_ingestion
                .as_ref()
                .is_some_and(|pending| pending.origin.includes(segment.id))
            || self
                .published_ingestion
                .as_ref()
                .is_some_and(|published| published.origin.includes(segment.id))
            || self.pending_checkpoint.as_ref().is_some_and(|pending| {
                segment.id == pending.journal.start.id
                    || segment.id >= pending.journal.next_segment_id
            })
        {
            return Ok(false);
        }

        match mode {
            CompactionMode::RawOnly => self.segment_needs_raw_compaction(segment),
            CompactionMode::ProfileRewrite => self.segment_needs_profile_rewrite(segment),
            CompactionMode::CurrentProfile => self.segment_needs_compaction(segment),
        }
    }

    pub fn raw_compaction_backlog_count(&self) -> std::io::Result<usize> {
        let mut count = 0usize;
        for segment in &self.catalog.segments {
            if Some(segment.id) == self.catalog.active_historical_segment {
                continue;
            }
            if self.segment_needs_raw_compaction(segment)? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn compaction_backlog_count(&self) -> std::io::Result<usize> {
        let mut count = 0usize;
        for segment in &self.catalog.segments {
            if Some(segment.id) == self.catalog.active_historical_segment {
                continue;
            }
            if self.segment_needs_compaction(segment)? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn profile_rewrite_backlog_count(&self) -> std::io::Result<usize> {
        let mut count = 0usize;
        for segment in &self.catalog.segments {
            if Some(segment.id) == self.catalog.active_historical_segment {
                continue;
            }
            if self.segment_needs_profile_rewrite(segment)? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn mark_non_canonical(&mut self, block_hash: B256) -> std::io::Result<u64> {
        self.ensure_writable()?;
        self.checkpoint_durable()?;
        self.recovery_required = true;
        let mut total_marked = 0u64;
        let mut changed_bundles = Vec::new();

        for descriptor in &mut self.catalog.segments {
            if descriptor.row_count == 0 {
                continue;
            }

            let dir = self.paths.segment_dir(descriptor.id);
            let reader = SegmentReader::open(&dir)?;
            if reader.bundle_reference() != descriptor.column_bundle.as_ref() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical update manifest differs from catalog; reopen to restore metadata",
                ));
            }
            let hashes = reader.read_b256("block_hash", None)?;
            let mut canonical = reader.read_canonical()?;
            let mut modified = false;

            for (row_id, hash) in hashes.iter().enumerate() {
                if *hash == block_hash && canonical.is_present(row_id as u64) {
                    canonical.set(row_id as u64, false);
                    modified = true;
                    total_marked += 1;
                }
            }

            if modified {
                if let Some(reference) = &descriptor.column_bundle {
                    descriptor.column_bundle = Some(super::segment::append_bundled_canonical(
                        &dir,
                        reference,
                        descriptor.generation,
                        &canonical,
                    )?);
                    persist_ingest_manifest(&self.paths, descriptor, Publication::Deferred)?;
                    changed_bundles.push(dir);
                } else {
                    ColumnFile::replace_canonical_bitmap(&dir, &canonical)?;
                }
            }
        }

        if !changed_bundles.is_empty() {
            durability::publish_catalog_after_trees(
                changed_bundles.iter().map(PathBuf::as_path),
                &self.paths.catalog_path(),
                &self.catalog.encode()?,
            )?;
        }
        self.recovery_required = false;
        Ok(total_marked)
    }

    pub fn total_rows(&self) -> u64 {
        self.catalog
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum()
    }

    pub fn sealed_count(&self) -> usize {
        self.catalog
            .segments
            .iter()
            .filter(|segment| segment.kind == SegmentKind::Sealed)
            .count()
    }

    pub fn indexed_head_block(&self) -> Option<u64> {
        self.catalog
            .segments
            .iter()
            .filter_map(|segment| segment.max_block)
            .max()
    }

    pub fn head_block(&self) -> Option<u64> {
        self.catalog
            .state
            .sync_head
            .map(|head| head.block_number)
            .or_else(|| self.indexed_head_block())
    }

    pub fn sealed_partition_metas(&self) -> Vec<PartitionMeta> {
        self.catalog
            .segments
            .iter()
            .filter(|segment| segment.kind == SegmentKind::Sealed)
            .map(|segment| self.partition_meta(segment))
            .collect()
    }

    pub fn hot_partition_meta(&self) -> PartitionMeta {
        let descriptor = self
            .catalog
            .active_hot_segment()
            .expect("native storage always maintains an active hot segment");
        self.partition_meta(descriptor)
    }

    pub fn segments(&self) -> &[SegmentDescriptor] {
        &self.catalog.segments
    }

    pub fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.paths.segment_dir(segment_id)
    }

    fn ensure_active_hot_segment(&mut self) -> std::io::Result<u64> {
        if let Some(active) = self.catalog.active_hot_segment() {
            let path = self.paths.segment_dir(active.id);
            if !path.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "active hot segment directory is missing",
                ));
            }
            return Ok(active.id);
        }

        let descriptor = self.catalog.register_segment(SegmentKind::Hot)?;
        let path = self.paths.segment_dir(descriptor.id);
        fs::create_dir_all(&path)?;
        persist_ingest_manifest(
            &self.paths,
            &descriptor,
            self.segment_publication(descriptor.id),
        )?;
        self.persist_catalog()?;
        Ok(descriptor.id)
    }

    fn ensure_active_historical_segment(&mut self) -> std::io::Result<u64> {
        if let Some(segment_id) = self.catalog.active_historical_segment {
            if let Some(descriptor) = self
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == segment_id && segment.kind == SegmentKind::Sealed)
            {
                let path = self.paths.segment_dir(descriptor.id);
                if !path.is_dir() {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "active historical segment directory is missing",
                    ));
                }
                return Ok(descriptor.id);
            }
            self.catalog.active_historical_segment = None;
        }

        let descriptor = self.catalog.allocate_segment(SegmentKind::Sealed)?;
        let segment_dir = self.paths.segment_dir(descriptor.id);
        if segment_dir.exists() {
            fs::remove_dir_all(&segment_dir)?;
        }
        fs::create_dir_all(&segment_dir)?;
        // This allocation belongs to the pending row transaction. Publish its
        // first complete manifest with the rows; an empty placeholder adds an
        // unnecessary replacement and cannot represent a successful ingest.
        let segment_id = descriptor.id;
        self.catalog.segments.push(descriptor);
        self.catalog.active_historical_segment = Some(segment_id);
        Ok(segment_id)
    }

    fn seal_hot_segment(&mut self) -> std::io::Result<()> {
        let hot_id = self
            .catalog
            .active_hot_segment
            .ok_or_else(|| std::io::Error::other("missing hot segment"))?;
        let publication = self.segment_publication(hot_id);
        if let Some(descriptor) = self
            .catalog
            .segments
            .iter_mut()
            .find(|segment| segment.id == hot_id)
        {
            descriptor.kind = SegmentKind::Sealed;
            persist_ingest_manifest(&self.paths, descriptor, publication)?;
            tracing::info!(
                segment_id = descriptor.id,
                row_count = descriptor.row_count,
                "sealed storage segment"
            );
        }

        self.catalog.active_hot_segment = None;
        let new_hot = self.catalog.register_segment(SegmentKind::Hot)?;
        let path = self.paths.segment_dir(new_hot.id);
        fs::create_dir_all(&path)?;
        persist_ingest_manifest(&self.paths, &new_hot, self.segment_publication(new_hot.id))?;
        self.persist_catalog()?;
        Ok(())
    }

    fn ensure_writable(&self) -> io::Result<()> {
        if self.recovery_required {
            return Err(io::Error::other(
                "storage has an unfinished transaction; close and reopen it before further writes",
            ));
        }
        Ok(())
    }

    #[cfg(test)]
    fn begin_wal_batch(&mut self, rows: &[LogRow]) -> io::Result<()> {
        self.checkpoint_durable()?;
        let batch = EncodedWalBatch::new(rows)?;
        if !self.wal.is_empty()? {
            return Err(io::Error::other(
                "WAL is not empty; close and reopen storage before starting a new batch",
            ));
        }
        let journal = self.journal_for_batch(&batch)?;
        // Even a failed metadata publication may have renamed its new file.
        // Keep this instance closed to further mutations until recovery.
        self.recovery_required = true;
        journal.persist(&self.paths)?;
        self.wal.append_encoded(&batch)
    }

    fn begin_checkpoint_batch(&mut self, rows: &[LogRow], route: IngestRoute) -> io::Result<()> {
        // Validate the entire caller batch before checkpointing or changing files.
        let batch = EncodedWalBatch::new(rows)?;
        if self.pending_ingestion.is_some() || self.published_ingestion.is_some() {
            self.checkpoint_durable()?;
        }
        if self.pending_checkpoint.as_ref().is_some_and(|pending| {
            pending.journal.route() != route
                || pending.bytes.saturating_add(batch.encoded_len()) > CHECKPOINT_INGEST_BYTES
                || pending.rows.checked_add(batch.row_count).is_none()
                || checkpoint_now().saturating_duration_since(pending.started_at)
                    >= std::time::Duration::from_secs(5)
        }) {
            self.checkpoint()?;
        }
        self.recovery_required = true;
        if self.pending_checkpoint.is_none() {
            if !self.wal.is_empty()? {
                return Err(io::Error::other(
                    "WAL is not empty; reopen storage before beginning a checkpoint",
                ));
            }
            let start_id = match route {
                IngestRoute::Live => self.ensure_active_hot_segment()?,
                IngestRoute::Historical => match self.catalog.active_historical_segment {
                    Some(id) => id,
                    // An unchanged hot descriptor anchors an epoch that starts
                    // with newly allocated history; do not create an empty
                    // historical segment just to carry a journal position.
                    None => self.ensure_active_hot_segment()?,
                },
            };
            let start = self
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == start_id)
                .cloned()
                .ok_or_else(|| io::Error::other("checkpoint starting segment missing"))?;
            let journal =
                RecoveryJournal::new_checkpoint(start, self.catalog.next_segment_id, route)?;
            journal.persist(&self.paths)?;
            self.pending_checkpoint = Some(PendingCheckpoint {
                journal,
                bytes: 0,
                rows: 0,
                checksum: EncodedWalBatch::checkpoint_checksum(),
                started_at: checkpoint_now(),
            });
        }
        self.wal.append_encoded(&batch)?;
        let pending = self
            .pending_checkpoint
            .as_mut()
            .ok_or_else(|| io::Error::other("checkpoint disappeared"))?;
        pending.bytes += batch.encoded_len();
        pending.rows = pending
            .rows
            .checked_add(batch.row_count)
            .ok_or_else(|| io::Error::other("checkpoint row count overflow"))?;
        batch.extend_checkpoint_checksum(&mut pending.checksum);
        Ok(())
    }

    fn complete_checkpoint_batch(&mut self) -> io::Result<()> {
        self.recovery_required = false;
        if self
            .pending_checkpoint
            .as_ref()
            .is_some_and(|pending| pending.bytes >= CHECKPOINT_INGEST_BYTES)
        {
            self.checkpoint()?;
        }
        Ok(())
    }

    /// Publish a complete sync checkpoint, or retire WAL recovery metadata.
    /// Sync data and catalog publication are ordered. Power loss can undo the
    /// bounded window of sync work since the last full device flush; use
    /// `checkpoint_durable` when the latest progress must survive power loss.
    /// Generic row batches retain their WAL-backed durable contract.
    pub fn checkpoint(&mut self) -> io::Result<()> {
        self.ensure_writable()?;
        let progress_durable = self.ingestion_checkpoint_due(0);
        if let Some(pending) = self.pending_ingestion.take() {
            return self.checkpoint_ingestion(pending, progress_durable);
        }
        let Some(mut pending) = self.pending_checkpoint.take() else {
            if progress_durable && self.published_ingestion.is_some() {
                self.recovery_required = true;
                durability::sync_directory(self.paths.root())?;
                self.published_ingestion = None;
                self.recovery_required = false;
            }
            return Ok(());
        };
        self.recovery_required = true;
        pending
            .journal
            .complete_checkpoint(pending.rows, pending.checksum.finalize())?;
        pending.journal.persist(&self.paths)?;
        self.persist_checkpoint_catalog()?;
        self.finish_wal_batch()
    }

    /// Make the latest rows and progress durable, including the published sync
    /// catalog name. Use before operations that cannot rewind with ingestion.
    pub fn checkpoint_durable(&mut self) -> io::Result<()> {
        self.ensure_writable()?;
        if let Some(pending) = self.pending_ingestion.take() {
            return self.checkpoint_ingestion(pending, true);
        }
        self.checkpoint()?;
        if self.published_ingestion.is_some() {
            self.recovery_required = true;
            durability::sync_directory(self.paths.root())?;
            self.published_ingestion = None;
            self.recovery_required = false;
        }
        Ok(())
    }

    /// Let the runtime retire small pending epochs even when ingestion is idle.
    pub fn checkpoint_if_due(&mut self) -> io::Result<bool> {
        self.ensure_writable()?;
        if self.pending_ingestion.as_ref().is_some_and(|pending| {
            checkpoint_now().saturating_duration_since(pending.started_at)
                >= std::time::Duration::from_secs(5)
        }) {
            self.checkpoint_durable()?;
            return Ok(true);
        }
        if self.pending_checkpoint.as_ref().is_some_and(|pending| {
            checkpoint_now().saturating_duration_since(pending.started_at)
                >= std::time::Duration::from_secs(5)
        }) {
            self.checkpoint_durable()?;
            return Ok(true);
        }
        if self.published_ingestion.as_ref().is_some_and(|published| {
            checkpoint_now().saturating_duration_since(published.started_at)
                >= std::time::Duration::from_secs(5)
        }) {
            self.checkpoint_durable()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn persist_checkpoint_catalog(&self) -> io::Result<()> {
        // Each WAL-backed manifest already ordered its columns and submitted its
        // directory entries. External devices were fully persisted before that
        // publication returned. Retire_after orders this catalog before clearing
        // the WAL, and journal removal supplies the final same-device full sync.
        let bytes = self.catalog.encode()?;
        durability::write_bytes_ordered(&self.paths.catalog_path(), &bytes)
    }

    fn journal_for_batch(&self, batch: &EncodedWalBatch) -> io::Result<RecoveryJournal> {
        let start = self.catalog.active_hot_segment().cloned().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "missing WAL starting segment")
        })?;
        RecoveryJournal::new(start, self.catalog.next_segment_id, batch)
    }

    fn finish_wal_batch(&mut self) -> io::Result<()> {
        durability::checkpoint("rows_committed", self.paths.root())?;
        // Artifact/name ordering makes the catalog recoverable before the WAL
        // can disappear. Journal removal completes the final full device sync.
        self.wal.retire_after(self.paths.root())?;
        RecoveryJournal::remove(&self.paths)?;
        self.recovery_required = false;
        Ok(())
    }

    fn replay_wal(&mut self) -> io::Result<()> {
        let journal = RecoveryJournal::load(&self.paths)?;
        let rows = self.wal.read_all()?;
        let mut journal = match journal {
            Some(journal) => journal,
            None if rows.is_empty() => return self.wal.truncate(),
            None => {
                // Old WALs carry no starting position. Overlap is ambiguous:
                // these could be committed rows or a new intentionally repeated
                // batch. Never infer a transaction boundary from row equality.
                self.reject_ambiguous_legacy_wal(&rows)?;
                let batch = EncodedWalBatch::new(&rows)?;
                let journal = self.journal_for_batch(&batch)?;
                self.recovery_required = true;
                journal.persist(&self.paths)?;
                journal
            }
        };
        self.recovery_required = true;
        let applied = self.verify_wal_transaction(&journal, &rows)?;
        if rows.is_empty() {
            return self.finish_wal_batch();
        }
        // Incomplete column writes beyond the last manifest are uncommitted.
        // Restore that prefix before appending the remainder, preserving its
        // canonical bits and using atomic file replacement for every column.
        match journal.route() {
            IngestRoute::Live => {
                self.rebuild_partial_hot_segment_before_wal_replay()?;
            }
            IngestRoute::Historical => {
                // The catalog already identifies the appendable historical
                // segment. Compressed pages no longer imply finalization.
                self.repair_recoverable_historical_segment_artifacts()?;
            }
        }
        tracing::info!(
            committed_rows = applied.len(),
            remaining_rows = rows.len() - applied.len(),
            "resuming journaled WAL transaction"
        );
        match journal.route() {
            IngestRoute::Live => self.commit_rows_to_segments(&rows[applied.len()..])?,
            IngestRoute::Historical => {
                self.write_historical_rows(&rows[applied.len()..])?;
            }
        }
        if journal.is_active_checkpoint() {
            let batch = EncodedWalBatch::new(&rows)?;
            journal.complete_checkpoint(batch.row_count, batch.checksum)?;
            journal.persist(&self.paths)?;
        }
        self.persist_checkpoint_catalog()?;
        self.finish_wal_batch()
    }

    fn read_applied_wal_rows(
        &self,
        journal: &RecoveryJournal,
        max_rows: u32,
    ) -> io::Result<Vec<LogRow>> {
        let start = self
            .catalog
            .segments
            .iter()
            .find(|segment| segment.id == journal.start.id)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL starting segment is missing",
                )
            })?;
        if start.generation != journal.start.generation
            || start.row_count < journal.start.row_count
            || self.catalog.next_segment_id < journal.next_segment_id
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL starting position no longer matches the catalog",
            ));
        }
        let mut segments = self
            .catalog
            .segments
            .iter()
            .filter(|segment| {
                segment.id == journal.start.id || segment.id >= journal.next_segment_id
            })
            .collect::<Vec<_>>();
        segments.sort_by_key(|segment| segment.id);
        let mut applied = Vec::new();
        for segment in segments {
            let first = if segment.id == journal.start.id {
                journal.start.row_count
            } else {
                0
            };
            let count = segment.row_count.checked_sub(first).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "WAL segment row count decreased",
                )
            })?;
            if count > u64::from(max_rows).saturating_sub(applied.len() as u64) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "segments contain more rows than the journaled WAL batch",
                ));
            }
            if count == 0 {
                continue;
            }
            let reader = SegmentReader::open(&self.paths.segment_dir(segment.id))?;
            let canonical = reader.read_canonical()?;
            if canonical.len() < segment.row_count
                || (first..segment.row_count).any(|row| !canonical.is_present(row))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "journaled committed rows have missing or non-canonical bitmap entries",
                ));
            }
            let first = u32::try_from(first).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "WAL row address exceeds u32")
            })?;
            let end = u32::try_from(segment.row_count).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "WAL row address exceeds u32")
            })?;
            let mut first = first;
            while first < end {
                let chunk_end = first.saturating_add(8192).min(end);
                let ids = (first..chunk_end).collect::<Vec<_>>();
                applied.try_reserve(ids.len()).map_err(io::Error::other)?;
                applied.extend(reader.read_log_rows(Some(&ids))?);
                first = chunk_end;
            }
        }
        Ok(applied)
    }

    fn reject_ambiguous_legacy_wal(&self, rows: &[LogRow]) -> io::Result<()> {
        use std::collections::HashSet;
        let identities = rows
            .iter()
            .map(|row| (row.block_hash, row.log_index, row.source as u8))
            .collect::<HashSet<_>>();
        let min_block = rows.iter().map(|row| row.block_number).min().unwrap_or(0);
        let max_block = rows
            .iter()
            .map(|row| row.block_number)
            .max()
            .unwrap_or(u64::MAX);
        for segment in &self.catalog.segments {
            if segment.row_count == 0
                || segment.min_block.is_some_and(|min| min > max_block)
                || segment.max_block.is_some_and(|max| max < min_block)
            {
                continue;
            }
            let reader = SegmentReader::open(&self.paths.segment_dir(segment.id))?;
            // Bound each selection independently of the total segment size.
            let mut first = 0;
            while first < segment.row_count {
                let end = first.saturating_add(8192).min(segment.row_count);
                let ids = (first..end)
                    .map(|id| {
                        u32::try_from(id).map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "legacy WAL segment exceeds row addressing",
                            )
                        })
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                let hashes = reader.read_b256("block_hash", Some(&ids))?;
                let indexes = reader.read_u32("log_index", Some(&ids))?;
                let sources = reader.read_u8("source", Some(&ids))?;
                if hashes
                    .into_iter()
                    .zip(indexes)
                    .zip(sources)
                    .any(|((hash, index), source)| identities.contains(&(hash, index, source)))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "legacy WAL overlaps committed rows but has no recovery journal; preserve the data directory for verified recovery",
                    ));
                }
                first = end;
            }
        }
        Ok(())
    }

    fn rebuild_partial_hot_segment_before_wal_replay(&mut self) -> std::io::Result<bool> {
        let Some(hot_id) = self.catalog.active_hot_segment else {
            return Ok(false);
        };
        let Some(segment_index) = self
            .catalog
            .segments
            .iter()
            .position(|segment| segment.id == hot_id)
        else {
            return Ok(false);
        };

        self.rebuild_partial_raw_segment(segment_index, "hot")
    }

    fn repair_recoverable_hot_segment_artifacts(&self) -> std::io::Result<()> {
        let Some(hot_id) = self.catalog.active_hot_segment else {
            return Ok(());
        };
        let Some(descriptor) = self
            .catalog
            .segments
            .iter()
            .find(|segment| segment.id == hot_id)
        else {
            return Ok(());
        };
        if descriptor.row_count == 0 {
            return Ok(());
        }

        let segment_dir = self.paths.segment_dir(hot_id);
        let reader = SegmentReader::open(&segment_dir)?;
        let physical_rows = reader.read_row_count()?;
        if physical_rows != descriptor.row_count {
            return Ok(());
        }

        let canonical_len = reader.read_canonical_len()?;
        if canonical_len != descriptor.row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment {} canonical bitmap length does not match committed rows: bitmap={canonical_len} rows={}; verified recovery is required",
                    descriptor.id, descriptor.row_count
                ),
            ));
        }
        Ok(())
    }

    fn repair_recoverable_historical_segment_artifacts(&mut self) -> std::io::Result<()> {
        let Some(segment_id) = self.catalog.active_historical_segment else {
            return Ok(());
        };
        let Some(segment_index) =
            self.catalog.segments.iter().position(|segment| {
                segment.id == segment_id && segment.kind == SegmentKind::Sealed
            })
        else {
            return Ok(());
        };

        if self.rebuild_partial_raw_segment(segment_index, "historical")? {
            tracing::warn!(
                segment_id,
                "rebuilt partially-applied active historical segment"
            );
        }

        Ok(())
    }

    fn rebuild_partial_raw_segment(
        &mut self,
        segment_index: usize,
        label: &'static str,
    ) -> std::io::Result<bool> {
        let descriptor = self.catalog.segments[segment_index].clone();
        let segment_dir = self.paths.segment_dir(descriptor.id);
        if descriptor.row_count == 0 {
            // An interrupted first write may have created any subset of columns.
            // There is no committed prefix to read; replace the partial files
            // with a complete empty prefix before replaying the verified WAL.
            if !has_raw_segment_artifacts(&segment_dir)? {
                return Ok(false);
            }
        } else {
            if !segment_dir.join("address.col").exists() {
                return Ok(false);
            }

            let column_counts = hot_segment_physical_row_counts(&descriptor, &segment_dir)?;
            if column_counts
                .iter()
                .all(|(_, row_count)| *row_count == descriptor.row_count)
            {
                return Ok(false);
            }
            if let Some((name, row_count)) = column_counts
                .iter()
                .find(|(_, row_count)| *row_count < descriptor.row_count)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "{label} segment {} column {name} has fewer rows than descriptor: {row_count} < {}",
                        descriptor.id, descriptor.row_count
                    ),
                ));
            }
        }

        let committed_rows = if descriptor.row_count == 0 {
            Vec::new()
        } else {
            let row_ids = (0..descriptor.row_count)
                .map(|row| {
                    u32::try_from(row).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("segment {} exceeds supported row addressing", descriptor.id),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            SegmentReader::open(&segment_dir)?.read_log_rows(Some(&row_ids))?
        };

        let mut canonical = NullBitmap::new();
        if descriptor.row_count != 0 {
            let previous = SegmentReader::open(&segment_dir)?.read_canonical()?;
            if previous.len() < descriptor.row_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cannot recover missing committed canonical bits",
                ));
            }
            for row in 0..descriptor.row_count {
                canonical.push(previous.is_present(row));
            }
        }
        ColumnFile::write_batch_with_canonical(&segment_dir, &committed_rows, Some(&canonical))?;
        persist_segment_manifest(&self.paths, &descriptor)?;
        self.catalog.segments[segment_index] = descriptor;
        self.persist_catalog()?;
        Ok(true)
    }

    fn partition_meta(&self, descriptor: &SegmentDescriptor) -> PartitionMeta {
        PartitionMeta {
            id: descriptor.id,
            min_block: descriptor.min_block.unwrap_or(u64::MAX),
            max_block: descriptor.max_block.unwrap_or(0),
            min_timestamp: descriptor.min_timestamp,
            max_timestamp: descriptor.max_timestamp,
            row_count: descriptor.row_count,
            sealed: descriptor.kind == SegmentKind::Sealed,
            path: self.paths.segment_dir(descriptor.id),
        }
    }

    fn persist_catalog(&self) -> std::io::Result<()> {
        // Rows and progress remain at the durable catalog checkpoint until the
        // containing ingestion operation or generic WAL checkpoint is committed.
        if self.pending_ingestion.is_some()
            || (self.pending_checkpoint.is_some() && self.recovery_required)
        {
            return Ok(());
        }
        self.catalog.persist(&self.paths)
    }

    fn persist_state(&self) -> io::Result<()> {
        self.persist_catalog()
    }

    fn should_compact_segment(&self, descriptor: &SegmentDescriptor) -> bool {
        if descriptor.kind != SegmentKind::Sealed || descriptor.row_count == 0 {
            return false;
        }

        let Some(max_block) = descriptor.max_block else {
            return false;
        };
        if let Some(finalized_head) = self.catalog.anchors.finalized_head {
            return max_block <= finalized_head.block_number;
        }

        let Some(head_block) = self.head_block() else {
            return false;
        };
        max_block.saturating_add(self.config.compaction_safety_margin_blocks) <= head_block
    }

    fn segment_needs_compaction(&self, descriptor: &SegmentDescriptor) -> std::io::Result<bool> {
        if !self.should_compact_segment(descriptor) {
            return Ok(false);
        }

        Ok(!segment_uses_current_compaction_profile(
            &self.paths,
            descriptor.id,
        )?)
    }

    fn segment_needs_raw_compaction(
        &self,
        descriptor: &SegmentDescriptor,
    ) -> std::io::Result<bool> {
        if !self.should_compact_segment(descriptor) {
            return Ok(false);
        }

        Ok(!super::segment::segment_is_compacted(
            &self.paths,
            descriptor.id,
        )?)
    }

    fn segment_needs_profile_rewrite(
        &self,
        descriptor: &SegmentDescriptor,
    ) -> std::io::Result<bool> {
        if !self.should_compact_segment(descriptor) {
            return Ok(false);
        }
        if !super::segment::segment_is_compacted(&self.paths, descriptor.id)? {
            return Ok(false);
        }

        Ok(!segment_uses_current_compaction_profile(
            &self.paths,
            descriptor.id,
        )?)
    }

    fn verify_integrity(&self) -> io::Result<()> {
        verify_segments_integrity_parallel(&self.paths, &self.catalog.segments)?;

        tracing::info!(
            segments = self.catalog.segments.len(),
            recent_headers = self.catalog.state.recent_headers.len(),
            "storage integrity check passed"
        );
        Ok(())
    }
}

fn verify_segments_integrity_parallel(
    paths: &StorageCatalogPaths,
    descriptors: &[SegmentDescriptor],
) -> io::Result<()> {
    if descriptors.is_empty() {
        return Ok(());
    }

    let worker_count = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .min(8)
        .min(descriptors.len());
    if worker_count <= 1 || descriptors.len() < 64 {
        for descriptor in descriptors {
            verify_segment_integrity(paths, descriptor)?;
        }
        return Ok(());
    }

    let chunk_size = descriptors.len().div_ceil(worker_count);
    thread::scope(|scope| {
        let mut handles = Vec::with_capacity(worker_count);
        for chunk in descriptors.chunks(chunk_size) {
            handles.push(scope.spawn(move || -> io::Result<()> {
                for descriptor in chunk {
                    verify_segment_integrity(paths, descriptor)?;
                }
                Ok(())
            }));
        }

        for handle in handles {
            handle
                .join()
                .map_err(|_| io::Error::other("segment integrity worker panicked"))??;
        }
        Ok(())
    })
}

fn verify_segment_integrity(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> io::Result<()> {
    let dir = paths.segment_dir(descriptor.id);
    if !dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("segment {} directory is missing", descriptor.id),
        ));
    }

    let reader = SegmentReader::open_projected(&dir, &["block_number"])?;
    let row_count = reader.read_row_count()?;
    if row_count != descriptor.row_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment {} row-count mismatch: descriptor={} storage={row_count}",
                descriptor.id, descriptor.row_count
            ),
        ));
    }

    if row_count == 0 {
        if descriptor.min_block.is_some() || descriptor.max_block.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment {} is empty but still has block metadata",
                    descriptor.id
                ),
            ));
        }
        return Ok(());
    }

    let canonical_len = reader.read_canonical_len()?;
    if canonical_len != row_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment {} canonical bitmap length mismatch: bitmap={canonical_len} rows={row_count}",
                descriptor.id
            ),
        ));
    }

    // Once the manifest references compacted pages, raw files are obsolete and
    // their interrupted deletion must not make the committed representation
    // unreadable. Raw manifests still require the complete raw file set.
    if !super::segment::segment_is_compacted(paths, descriptor.id)? {
        verify_raw_segment_files_complete(descriptor, &dir)?;
        for (name, physical_rows) in hot_segment_physical_row_counts(descriptor, &dir)? {
            if physical_rows != row_count {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "segment {} raw column {name} row-count mismatch: manifest={row_count} storage={physical_rows}",
                        descriptor.id
                    ),
                ));
            }
        }
    }

    let last_row = u32::try_from(row_count - 1).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment {} exceeds supported per-segment row addressing",
                descriptor.id
            ),
        )
    })?;
    let boundary_blocks = reader.read_u64("block_number", Some(&[0, last_row]))?;
    if boundary_blocks.len() != 2 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment {} failed to read boundary block numbers",
                descriptor.id
            ),
        ));
    }

    let Some(min_block) = descriptor.min_block else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment {} has rows but no minimum block metadata",
                descriptor.id,
            ),
        ));
    };
    let Some(max_block) = descriptor.max_block else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment {} has rows but no maximum block metadata",
                descriptor.id,
            ),
        ));
    };

    if min_block > max_block {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "segment {} block metadata is inverted: min={min_block} max={max_block}",
                descriptor.id
            ),
        ));
    }

    for block_number in boundary_blocks {
        if block_number < min_block || block_number > max_block {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment {} boundary block {block_number} is outside descriptor range [{min_block}, {max_block}]",
                    descriptor.id
                ),
            ));
        }
    }

    Ok(())
}

fn historical_segment_would_exceed_block_span(
    descriptor: &SegmentDescriptor,
    rows: &[LogRow],
) -> bool {
    let Some((min_block, max_block)) = rows_block_range(rows) else {
        return false;
    };
    let min_block = descriptor
        .min_block
        .map(|current| current.min(min_block))
        .unwrap_or(min_block);
    let max_block = descriptor
        .max_block
        .map(|current| current.max(max_block))
        .unwrap_or(max_block);
    max_block.saturating_sub(min_block) >= HISTORICAL_STAGING_MAX_BLOCK_SPAN
}

fn historical_segment_block_span(descriptor: &SegmentDescriptor) -> Option<u64> {
    Some(descriptor.max_block?.saturating_sub(descriptor.min_block?))
}

fn dense_historical_batch_row_threshold(target_rows: usize) -> usize {
    if target_rows <= 1024 {
        target_rows.max(1)
    } else {
        (target_rows / 4).max(1)
    }
}

fn compacted_historical_row_prefix_len(row_count: usize, target_rows: usize) -> usize {
    if row_count <= target_rows {
        return row_count;
    }

    let dense_threshold = dense_historical_batch_row_threshold(target_rows);
    let remainder = row_count % target_rows;
    if remainder > 0 && remainder < dense_threshold {
        row_count - remainder
    } else {
        row_count
    }
}

fn rows_block_range(rows: &[LogRow]) -> Option<(u64, u64)> {
    let mut iter = rows.iter().map(|row| row.block_number);
    let first = iter.next()?;
    let mut min_block = first;
    let mut max_block = first;
    for block_number in iter {
        min_block = min_block.min(block_number);
        max_block = max_block.max(block_number);
    }
    Some((min_block, max_block))
}

fn has_raw_segment_artifacts(dir: &Path) -> io::Result<bool> {
    fs::read_dir(dir)?.try_fold(false, |found, entry| {
        let path = entry?.path();
        Ok(found
            || path
                .extension()
                .is_some_and(|ext| ext == "col" || ext == "null")
            || path
                .file_name()
                .is_some_and(|name| name == "canonical.bitmap"))
    })
}

fn hot_segment_physical_row_counts(
    descriptor: &SegmentDescriptor,
    segment_dir: &Path,
) -> io::Result<Vec<(&'static str, u64)>> {
    const COLUMN_FILES: &[&str] = &[
        "address.col",
        "block_number.col",
        "block_hash.col",
        "timestamp.col",
        "tx_hash.col",
        "tx_index.col",
        "log_index.col",
        "data_len.col",
        "source.col",
        "topic0.col",
        "topic1.col",
        "topic2.col",
        "topic3.col",
        "data.col",
    ];
    const BITMAP_FILES: &[&str] = &[
        "topic0.null",
        "topic1.null",
        "topic2.null",
        "topic3.null",
        "canonical.bitmap",
    ];

    let mut counts = Vec::with_capacity(COLUMN_FILES.len() + BITMAP_FILES.len());
    for name in COLUMN_FILES {
        let path = segment_dir.join(name);
        // Only the fixed header contributes a count. Payload sizes and the final
        // data offset are checked separately by raw integrity validation.
        let header = super::segment::read_raw_column_header(descriptor, &path, name)?;
        counts.push((*name, header.row_count));
    }

    for name in BITMAP_FILES {
        let path = segment_dir.join(name);
        let data = fs::read(&path)?;
        let bitmap = NullBitmap::read_from(&data).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt hot segment bitmap {}", path.display()),
            )
        })?;
        counts.push((*name, bitmap.len()));
    }

    Ok(counts)
}

fn parse_segment_dir_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let id = name.strip_prefix("s_")?;
    id.parse().ok()
}

fn execution_marker_from_header(header: &Header) -> ExecutionBlockMarker {
    ExecutionBlockMarker {
        block_number: header.number(),
        block_hash: header.hash_slow(),
        timestamp: header.timestamp(),
    }
}

fn verify_recent_headers(state: &StorageState) -> io::Result<()> {
    for headers in state.recent_headers.windows(2) {
        let parent = &headers[0];
        let child = &headers[1];
        if child.number() != parent.number().saturating_add(1) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recent header window is not contiguous: {} followed by {}",
                    parent.number(),
                    child.number()
                ),
            ));
        }
        if child.parent_hash() != parent.hash_slow() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recent header window has a broken parent link at block {}",
                    child.number()
                ),
            ));
        }
    }

    if let (Some(sync_head), Some(last_header)) = (state.sync_head, state.recent_headers.last()) {
        let last_hash = last_header.hash_slow();
        if last_header.number() != sync_head.block_number || last_hash != sync_head.block_hash {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recent header window tip ({}, {}) does not match persisted sync head ({}, {})",
                    last_header.number(),
                    last_hash,
                    sync_head.block_number,
                    sync_head.block_hash
                ),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::ColumnFileHeader;
    use std::collections::BTreeMap;
    use std::fs;

    use alloy_consensus::Header;
    use alloy_primitives::{Address, B256, Bytes, bytes};
    use logex_types::{LogRow, Source};
    use tempfile::TempDir;

    use super::*;

    fn make_rows(count: usize, start_block: u64) -> Vec<LogRow> {
        (0..count)
            .map(|idx| LogRow {
                block_number: start_block + idx as u64 / 5,
                block_hash: B256::repeat_byte((idx % 255) as u8),
                timestamp: 1_700_000_000 + idx as u64 * 12,
                tx_hash: B256::repeat_byte(((idx + 1) % 255) as u8),
                tx_index: (idx % 4) as u32,
                log_index: idx as u32,
                address: Address::repeat_byte(0x11),
                topic0: Some(B256::repeat_byte(0x22)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("deadbeef"),
                data_len: 4,
                source: Source::Receipt,
            })
            .collect()
    }

    fn ingestion_header(number: u64, parent_hash: B256) -> Header {
        Header {
            number,
            parent_hash,
            timestamp: 1_700_000_000 + number * 12,
            ..Default::default()
        }
    }

    fn ingestion_rows(count: usize, header: &Header) -> Vec<LogRow> {
        make_rows(count, header.number)
            .into_iter()
            .map(|mut row| {
                row.block_number = header.number;
                row.block_hash = header.hash_slow();
                row.timestamp = header.timestamp;
                row
            })
            .collect()
    }

    fn read_ingestion_rows(storage: &NativeStorage) -> Vec<LogRow> {
        let mut rows = Vec::new();
        for segment in &storage.catalog.segments {
            if segment.row_count > 0 {
                rows.extend(
                    SegmentReader::open(&storage.paths.segment_dir(segment.id))
                        .unwrap()
                        .read_log_rows(None)
                        .unwrap(),
                );
            }
        }
        rows.sort_by_key(|row| (row.block_number, row.log_index));
        rows
    }

    fn ingest_test_batch(
        storage: &mut NativeStorage,
        historical: bool,
        rows: &[LogRow],
        header: &Header,
        prior: &Header,
    ) -> io::Result<()> {
        if historical {
            storage.ingest_historical_batch(rows, header)
        } else {
            storage.ingest_canonical_batch(rows, header, &[prior.clone(), header.clone()], None)
        }
    }

    fn ingest_test_initial_batch(
        storage: &mut NativeStorage,
        historical: bool,
        rows: &[LogRow],
        header: &Header,
    ) -> io::Result<()> {
        if historical {
            storage.ingest_historical_batch(rows, header).map(|_| ())
        } else {
            storage.ingest_canonical_batch(rows, header, std::slice::from_ref(header), None)
        }
    }

    fn sparse_repack_fixture(dir: &Path, historical: bool) -> (NativeStorage, Vec<LogRow>, Header) {
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: dir.to_owned(),
            ..Default::default()
        })
        .unwrap();
        let mut expected = Vec::new();
        let mut previous = ingestion_header(256, B256::ZERO);
        for step in 0..127 {
            let header = ingestion_header(
                if historical { 256 - step } else { 256 + step },
                previous.hash_slow(),
            );
            let rows = ingestion_rows(1, &header);
            ingest_test_batch(&mut storage, historical, &rows, &header, &previous).unwrap();
            expected.extend(rows);
            previous = header;
        }
        storage.checkpoint_durable().unwrap();
        storage.mark_non_canonical(expected[12].block_hash).unwrap();
        (storage, expected, previous)
    }

    fn trigger_repack(
        storage: &mut NativeStorage,
        historical: bool,
        expected: &mut Vec<LogRow>,
        previous: &Header,
    ) {
        let header = ingestion_header(
            if historical {
                previous.number - 1
            } else {
                previous.number + 1
            },
            previous.hash_slow(),
        );
        let rows = ingestion_rows(1, &header);
        ingest_test_batch(storage, historical, &rows, &header, previous).unwrap();
        expected.extend(rows);
        // Coverage of an empty block is part of the same atomic catalog change.
        let empty = ingestion_header(
            if historical {
                header.number - 1
            } else {
                header.number + 1
            },
            header.hash_slow(),
        );
        ingest_test_batch(storage, historical, &[], &empty, &header).unwrap();
    }

    #[test]
    fn sparse_repacking_preserves_readers_indexes_coverage_and_future_writes() {
        for historical in [false, true] {
            let tmp = TempDir::new().unwrap();
            let (mut storage, mut expected, previous) =
                sparse_repack_fixture(tmp.path(), historical);
            trigger_repack(&mut storage, historical, &mut expected, &previous);
            let index = storage
                .catalog
                .segments
                .iter()
                .position(|segment| segment.row_count != 0)
                .unwrap();
            let old_descriptor = storage.catalog.segments[index].clone();
            let dir = storage.paths.segment_dir(old_descriptor.id);
            let old = SegmentReader::open(&dir).unwrap();
            let canonical = old.read_canonical().unwrap();
            let state = storage.catalog.state.clone();
            let sentinel = dir.join("columns/keep-me");
            fs::write(&sentinel, b"unrelated").unwrap();
            crate::IndexBuildCheckpoint::begin(&dir)
                .unwrap()
                .publish()
                .unwrap();
            assert!(
                crate::IndexReadCheckpoint::open(&dir, &old)
                    .unwrap()
                    .is_some()
            );
            let stale_builder = crate::IndexBuildCheckpoint::begin(&dir).unwrap();
            storage.checkpoint().unwrap();
            let descriptor = &storage.catalog.segments[index];
            assert_eq!(descriptor.generation, 1);
            assert_eq!(storage.catalog.state, state);
            assert!(!dir.join(crate::column_artifact::BUNDLE_PATH).exists());
            assert_eq!(fs::read(&sentinel).unwrap(), b"unrelated");
            let new = SegmentReader::open(&dir).unwrap();
            assert_eq!(new.read_log_rows(None).unwrap(), expected);
            assert_eq!(old.clone().read_log_rows(None).unwrap(), expected);
            for row in 0..128 {
                assert_eq!(
                    new.read_canonical().unwrap().is_present(row),
                    canonical.is_present(row)
                );
            }
            assert!(!canonical.is_present(12));
            assert_eq!(
                stale_builder.publish().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
            assert!(
                crate::IndexReadCheckpoint::open(&dir, &new)
                    .unwrap()
                    .is_none()
            );
            crate::IndexBuildCheckpoint::begin(&dir)
                .unwrap()
                .publish()
                .unwrap();
            assert!(
                crate::IndexReadCheckpoint::open(&dir, &new)
                    .unwrap()
                    .is_some()
            );
            assert_eq!(
                persist_segment_manifest(&storage.paths, &old_descriptor)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(
                compact_segment(&storage.paths, &old_descriptor)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            storage.mark_non_canonical(expected[20].block_hash).unwrap();
            assert!(old.read_canonical().unwrap().is_present(20));
            assert!(
                !SegmentReader::open(&dir)
                    .unwrap()
                    .read_canonical()
                    .unwrap()
                    .is_present(20)
            );
            let config = storage.config.clone();
            drop(storage);
            let mut reopened = NativeStorage::open(config).unwrap();
            assert_eq!(reopened.catalog.state, state);
            let next = ingestion_header(
                if historical {
                    previous.number - 3
                } else {
                    previous.number + 3
                },
                B256::ZERO,
            );
            let rows = ingestion_rows(2, &next);
            ingest_test_initial_batch(&mut reopened, historical, &rows, &next).unwrap();
            expected.extend(rows);
            reopened.checkpoint_durable().unwrap();
            assert_eq!(
                SegmentReader::open(&dir)
                    .unwrap()
                    .read_log_rows(None)
                    .unwrap(),
                expected
            );
            assert_eq!(old.read_log_rows(None).unwrap().len(), 128);
        }
    }

    #[test]
    fn sparse_repacking_interruption_keeps_an_exact_catalog_generation() {
        // Inject on the owning thread at every replacement/publication/retirement
        // boundary. Compression workers have separate byte-corruption tests.
        for historical in [false, true] {
            let tmp = TempDir::new().unwrap();
            let (mut storage, mut expected, previous) =
                sparse_repack_fixture(tmp.path(), historical);
            trigger_repack(&mut storage, historical, &mut expected, &previous);
            durability::inject_failure(usize::MAX);
            storage.checkpoint().unwrap();
            let events = durability::take_events();
            assert!(
                events
                    .iter()
                    .any(|(op, _)| *op == "repack_retire_generation")
            );
            let retirement = events
                .iter()
                .position(|(op, _)| *op == "repack_retire_generation")
                .unwrap();
            for fail_at in 0..events.len() {
                for previous_catalog in [false, true] {
                    if previous_catalog && fail_at >= retirement {
                        continue;
                    }
                    let tmp = TempDir::new().unwrap();
                    let (mut storage, mut expected, previous) =
                        sparse_repack_fixture(tmp.path(), historical);
                    let old_catalog = fs::read(storage.paths.catalog_path()).unwrap();
                    let old_state = storage.catalog.state.clone();
                    trigger_repack(&mut storage, historical, &mut expected, &previous);
                    let new_state = storage.catalog.state.clone();
                    durability::inject_failure(fail_at);
                    let result = storage.checkpoint();
                    let observed = durability::take_events();
                    assert!(result.is_err(), "{historical}/{fail_at}: {observed:?}");
                    assert!(storage.checkpoint().is_err());
                    if previous_catalog {
                        fs::write(storage.paths.catalog_path(), old_catalog).unwrap();
                    }
                    let config = storage.config.clone();
                    drop(storage);
                    for _ in 0..2 {
                        let reopened = NativeStorage::open(config.clone()).unwrap_or_else(|error| panic!("{historical}/{fail_at}/{previous_catalog}: {error}; {observed:?}"));
                        let rows = read_ingestion_rows(&reopened);
                        let count = rows.len();
                        assert!(count == 127 || count == 128);
                        if previous_catalog {
                            assert_eq!(count, 127);
                        }
                        let mut wanted = expected[..count].to_vec();
                        wanted.sort_by_key(|row| (row.block_number, row.log_index));
                        assert_eq!(rows, wanted);
                        assert_eq!(
                            reopened.catalog.state,
                            if count == 127 {
                                old_state.clone()
                            } else {
                                new_state.clone()
                            }
                        );
                        let descriptor = reopened
                            .catalog
                            .segments
                            .iter()
                            .find(|segment| segment.row_count != 0)
                            .unwrap();
                        let dir = reopened.paths.segment_dir(descriptor.id);
                        let reader = SegmentReader::open(&dir).unwrap();
                        assert!(!reader.read_canonical().unwrap().is_present(12));
                        for generation in 0..=1 {
                            assert_eq!(
                                crate::column_artifact::bundle_path(&dir, generation).exists(),
                                generation == descriptor.generation
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn sparse_repacking_retries_busy_ownership_and_cleans_only_reserved_orphans() {
        let tmp = TempDir::new().unwrap();
        let (mut storage, mut expected, previous) = sparse_repack_fixture(tmp.path(), true);
        trigger_repack(&mut storage, true, &mut expected, &previous);
        let id = storage.catalog.active_historical_segment.unwrap();
        let dir = storage.paths.segment_dir(id);
        let owner = File::open(&dir).unwrap();
        owner.try_lock().unwrap();
        storage.checkpoint_durable().unwrap();
        assert_eq!(
            storage
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == id)
                .unwrap()
                .generation,
            0
        );
        owner.unlock().unwrap();
        let empty = ingestion_header(previous.number - 3, B256::ZERO);
        storage.ingest_historical_batch(&[], &empty).unwrap();
        storage.checkpoint_durable().unwrap();
        assert_eq!(
            storage
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == id)
                .unwrap()
                .generation,
            1
        );
        let orphan = crate::column_artifact::bundle_path(&dir, 2);
        fs::create_dir(orphan.parent().unwrap()).unwrap();
        fs::write(&orphan, b"interrupted generation").unwrap();
        let sentinel = orphan.parent().unwrap().join("keep-me");
        fs::write(&sentinel, b"unrelated").unwrap();
        // Also cover interruption after creating a generation directory but
        // before creating its file; this must not block reuse after recovery.
        fs::create_dir(
            crate::column_artifact::bundle_path(&dir, 3)
                .parent()
                .unwrap(),
        )
        .unwrap();
        let config = storage.config.clone();
        drop(storage);
        let mut reopened = NativeStorage::open(config).unwrap();
        assert!(!orphan.exists());
        assert!(
            !crate::column_artifact::bundle_path(&dir, 3)
                .parent()
                .unwrap()
                .exists()
        );
        assert_eq!(fs::read(&sentinel).unwrap(), b"unrelated");
        assert_eq!(
            SegmentReader::open(&dir)
                .unwrap()
                .read_log_rows(None)
                .unwrap(),
            expected
        );
        assert_eq!(reopened.historical_floor_header(), Some(&empty));
        for number in (1..=128).rev() {
            let header = ingestion_header(number, B256::ZERO);
            let rows = ingestion_rows(1, &header);
            reopened.ingest_historical_batch(&rows, &header).unwrap();
            expected.extend(rows);
        }
        reopened.checkpoint_durable().unwrap();
        assert_eq!(
            reopened
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == id)
                .unwrap()
                .generation,
            3
        );
        assert_eq!(
            SegmentReader::open(&dir)
                .unwrap()
                .read_log_rows(None)
                .unwrap(),
            expected
        );
        assert_eq!(fs::read(sentinel).unwrap(), b"unrelated");
    }

    #[test]
    fn live_ingestion_bundles_fresh_segments_and_preserves_reader_snapshots() {
        let dir = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            hot_target_rows: 10,
            ..Default::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        let prior = ingestion_header(99, B256::ZERO);
        let first = ingestion_header(100, prior.hash_slow());
        let second = ingestion_header(101, first.hash_slow());
        let initial = ingestion_rows(3, &first);
        let incoming = ingestion_rows(13, &second);
        ingest_test_batch(&mut storage, false, &initial, &first, &prior).unwrap();
        storage.checkpoint().unwrap();
        let id = storage.catalog.active_hot_segment.unwrap();
        assert!(
            storage
                .catalog
                .active_hot_segment()
                .unwrap()
                .column_bundle
                .is_some()
        );
        let snapshot = SegmentReader::open(&storage.segment_path(id)).unwrap();
        ingest_test_batch(&mut storage, false, &incoming, &second, &first).unwrap();
        storage.checkpoint().unwrap();
        assert_eq!(snapshot.read_log_rows(None).unwrap(), initial);
        assert_eq!(snapshot.read_canonical_len().unwrap(), 3);
        assert!(snapshot.read_log_rows(Some(&[3])).is_err());
        assert_eq!(storage.mark_non_canonical(first.hash_slow()).unwrap(), 3);
        assert!((0..3).all(|row| snapshot.read_canonical().unwrap().is_present(row)));
        let mut expected = initial;
        expected.extend(incoming);
        drop(storage);
        for _ in 0..2 {
            let storage = NativeStorage::open(config.clone()).unwrap();
            assert_eq!(read_ingestion_rows(&storage), expected);
            assert_eq!(storage.sync_head().unwrap().block_number, second.number);
            for segment in storage
                .segments()
                .iter()
                .filter(|segment| segment.row_count > 0)
            {
                assert!(segment.column_bundle.is_some());
                let path = storage.segment_path(segment.id);
                assert!(!path.join("address.col").exists());
                assert!(!path.join("canonical.bitmap").exists());
                let reader = SegmentReader::open(&path).unwrap();
                let bits = reader.read_canonical().unwrap();
                for (id, row) in reader.read_log_rows(None).unwrap().iter().enumerate() {
                    assert_eq!(bits.is_present(id as u64), row.block_number != first.number);
                }
            }
        }
    }

    #[test]
    fn live_first_bundle_recovers_each_publication_and_empty_raw_cleanup_failure() {
        for recovered_empty in [false, true] {
            let mut steps = 0;
            for failure in std::iter::once(usize::MAX).chain(0..) {
                if failure != usize::MAX && failure >= steps {
                    break;
                }
                let dir = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: dir.path().to_owned(),
                    ..Default::default()
                };
                let prior = ingestion_header(99, B256::ZERO);
                let header = ingestion_header(100, prior.hash_slow());
                let rows = ingestion_rows(3, &header);
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                if recovered_empty {
                    ingest_test_batch(&mut storage, false, &rows, &header, &prior).unwrap();
                    drop(storage);
                    storage = NativeStorage::open(config.clone()).unwrap();
                    assert_eq!(storage.total_rows(), 0);
                    let id = storage.catalog.active_hot_segment.unwrap();
                    assert!(storage.segment_path(id).join("address.col").exists());
                }
                durability::inject_failure(failure);
                let result = ingest_test_batch(&mut storage, false, &rows, &header, &prior)
                    .and_then(|_| storage.checkpoint());
                let events = durability::take_events();
                if failure == usize::MAX {
                    result.unwrap();
                    steps = events.len();
                    assert!(steps > 0);
                    assert_eq!(
                        events
                            .iter()
                            .any(|(op, _)| *op == "bundle_remove_empty_raw_artifact"),
                        recovered_empty
                    );
                } else {
                    assert!(result.is_err(), "{failure}: {events:?}");
                    assert!(storage.ensure_writable().is_err());
                }
                drop(storage);
                let mut recovered = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                    panic!("empty={recovered_empty}, failure={failure}: {events:?}: {error}")
                });
                if recovered.sync_head().is_none() {
                    assert!(read_ingestion_rows(&recovered).is_empty());
                    ingest_test_batch(&mut recovered, false, &rows, &header, &prior).unwrap();
                    recovered.checkpoint().unwrap();
                }
                assert_eq!(read_ingestion_rows(&recovered), rows);
                assert_eq!(
                    recovered.sync_head().unwrap().block_hash,
                    header.hash_slow()
                );
                let id = recovered.catalog.active_hot_segment.unwrap();
                let path = recovered.segment_path(id);
                assert!(!path.join("address.col").exists());
                assert!(!path.join("canonical.bitmap").exists());
                drop(recovered);
                for _ in 0..2 {
                    assert_eq!(
                        read_ingestion_rows(&NativeStorage::open(config.clone()).unwrap()),
                        rows
                    );
                }
            }
        }
    }

    #[test]
    fn wal_append_to_live_bundle_recovers_each_publication_failure() {
        for incoming_count in [2, 13] {
            let mut steps = 0;
            for failure in std::iter::once(usize::MAX).chain(0..) {
                if failure != usize::MAX && failure >= steps {
                    break;
                }
                let dir = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: dir.path().to_owned(),
                    hot_target_rows: 10,
                    ..Default::default()
                };
                let prior = ingestion_header(99, B256::ZERO);
                let header = ingestion_header(100, prior.hash_slow());
                let initial = ingestion_rows(3, &header);
                let next = ingestion_header(101, header.hash_slow());
                let incoming = ingestion_rows(incoming_count, &next);
                let mut expected = initial.clone();
                expected.extend(incoming.clone());
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                ingest_test_batch(&mut storage, false, &initial, &header, &prior).unwrap();
                storage.checkpoint_durable().unwrap();
                storage.mark_non_canonical(header.hash_slow()).unwrap();
                assert!(
                    storage
                        .catalog
                        .active_hot_segment()
                        .unwrap()
                        .column_bundle
                        .is_some()
                );
                durability::inject_failure(failure);
                let result = storage
                    .write_batch(&incoming)
                    .and_then(|_| storage.checkpoint());
                let events = durability::take_events();
                if failure == usize::MAX {
                    assert!(result.is_ok(), "{events:?}");
                    steps = events.len();
                    assert!(steps > 0);
                } else {
                    assert!(result.is_err(), "{failure}: {events:?}");
                    assert!(storage.ensure_writable().is_err());
                }
                let wal = storage.wal.read_all().unwrap();
                drop(storage);
                let mut recovered = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                    panic!("rows={incoming_count}, failure={failure}: {events:?}: {error}")
                });
                let actual = read_ingestion_rows(&recovered);
                if result.is_ok() || !wal.is_empty() {
                    assert_eq!(actual, expected, "failure={failure}");
                } else {
                    assert!(actual == initial || actual == expected, "failure={failure}");
                }
                if actual == initial {
                    recovered.write_batch(&incoming).unwrap();
                    recovered.checkpoint().unwrap();
                }
                drop(recovered);
                for _ in 0..2 {
                    let recovered = NativeStorage::open(config.clone()).unwrap();
                    assert_eq!(read_ingestion_rows(&recovered), expected);
                    assert!(recovered.wal.is_empty().unwrap());
                    for segment in recovered
                        .segments()
                        .iter()
                        .filter(|segment| segment.row_count > 0)
                    {
                        let reader =
                            SegmentReader::open(&recovered.segment_path(segment.id)).unwrap();
                        let bits = reader.read_canonical().unwrap();
                        for (id, row) in reader.read_log_rows(None).unwrap().iter().enumerate() {
                            assert_eq!(
                                bits.is_present(id as u64),
                                row.block_number != header.number
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn published_ingestion_orders_data_and_catalog_before_subsequent_writes() {
        for historical in [false, true] {
            let dir = TempDir::new().unwrap();
            let mut storage = NativeStorage::open(NativeStorageConfig {
                data_dir: dir.path().to_owned(),
                ..Default::default()
            })
            .unwrap();
            let prior = ingestion_header(99, B256::ZERO);
            let header = ingestion_header(100, prior.hash_slow());
            ingest_test_batch(
                &mut storage,
                historical,
                &ingestion_rows(3, &header),
                &header,
                &prior,
            )
            .unwrap();
            durability::inject_failure(usize::MAX);
            storage.checkpoint().unwrap();
            let events = durability::take_events();
            let position = |operation, path: &Path| {
                events
                    .iter()
                    .position(|(op, p)| *op == operation && p == path)
                    .unwrap()
            };
            let catalog = storage.paths.catalog_path();
            let data = position("ingestion_data_ordered", &catalog);
            let rename = position("rename_temporary", &catalog);
            let directory = position("order_ingestion_catalog_directory", storage.paths.root());
            assert!(data < rename && rename < directory, "{events:?}");
            #[cfg(target_vendor = "apple")]
            assert!(events[..data].iter().any(|(op, _)| *op == "order_device"));
            assert!(storage.published_ingestion.is_some());
            assert!(storage.pending_ingestion.is_none());
            storage.checkpoint_durable().unwrap();
            assert!(storage.published_ingestion.is_none());
        }
    }

    fn assert_published_epoch_rewind(
        config: NativeStorageConfig,
        historical: bool,
        counts: (usize, usize, usize),
        publish_second: bool,
        keep_first: bool,
    ) {
        let (initial_count, first_count, second_count) = counts;
        let prior = ingestion_header(100, B256::ZERO);
        let first = ingestion_header(if historical { 99 } else { 101 }, prior.hash_slow());
        let second = ingestion_header(if historical { 98 } else { 102 }, first.hash_slow());
        let initial = ingestion_rows(initial_count, &prior);
        let first_rows = ingestion_rows(first_count, &first);
        let second_rows = ingestion_rows(second_count, &second);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        if historical {
            storage.ingest_historical_batch(&initial, &prior).unwrap();
        } else {
            storage
                .ingest_canonical_batch(&initial, &prior, std::slice::from_ref(&prior), None)
                .unwrap();
        }
        storage.checkpoint_durable().unwrap();
        storage.mark_non_canonical(prior.hash_slow()).unwrap();
        let previous = fs::read(storage.paths.catalog_path()).unwrap();
        ingest_test_batch(&mut storage, historical, &first_rows, &first, &prior).unwrap();
        storage.checkpoint().unwrap();
        let first_catalog = fs::read(storage.paths.catalog_path()).unwrap();
        ingest_test_batch(&mut storage, historical, &second_rows, &second, &first).unwrap();
        if publish_second {
            storage.checkpoint().unwrap();
        }
        let mut expected = initial.clone();
        if keep_first {
            expected.extend(first_rows.clone());
        }
        expected.sort_by_key(|r| (r.block_number, r.log_index));
        drop(storage);
        // Model the newest catalog rename not reaching stable media.
        // Ordering preserves either predecessor, but does not promise which
        // published catalog has reached stable media before a full flush.
        fs::write(
            config.data_dir.join("catalog.json"),
            if keep_first { first_catalog } else { previous },
        )
        .unwrap();
        for _ in 0..2 {
            let recovered = NativeStorage::open(config.clone()).unwrap();
            assert_eq!(read_ingestion_rows(&recovered), expected);
            let number = if historical {
                recovered.historical_floor().unwrap().block_number
            } else {
                recovered.sync_head().unwrap().block_number
            };
            assert_eq!(
                number,
                if keep_first {
                    first.number
                } else {
                    prior.number
                }
            );
            for segment in recovered.segments().iter().filter(|s| s.row_count > 0) {
                let reader = SegmentReader::open(&recovered.segment_path(segment.id)).unwrap();
                let hashes = reader.read_b256("block_hash", None).unwrap();
                let flags = reader.read_canonical().unwrap();
                for (row, hash) in hashes.iter().enumerate() {
                    assert_eq!(flags.is_present(row as u64), *hash != prior.hash_slow());
                }
            }
        }
        let mut recovered = NativeStorage::open(config.clone()).unwrap();
        if !keep_first {
            ingest_test_batch(&mut recovered, historical, &first_rows, &first, &prior).unwrap();
        }
        ingest_test_batch(&mut recovered, historical, &second_rows, &second, &first).unwrap();
        recovered.checkpoint_durable().unwrap();
        drop(recovered);
        let recovered = NativeStorage::open(config).unwrap();
        let mut expected = initial;
        expected.extend(first_rows);
        expected.extend(second_rows);
        expected.sort_by_key(|r| (r.block_number, r.log_index));
        assert_eq!(read_ingestion_rows(&recovered), expected);
    }

    #[test]
    fn published_ingestion_rewinds_any_complete_checkpoint_in_the_window() {
        for historical in [false, true] {
            for counts in [
                (0, 0, 0),
                (0, 3, 2),
                (0, 25, 25),
                (3, 0, 0),
                (3, 3, 2),
                (3, 25, 25),
            ] {
                for (publish_second, keep_first) in
                    [(false, false), (false, true), (true, false), (true, true)]
                {
                    let dir = TempDir::new().unwrap();
                    assert_published_epoch_rewind(
                        NativeStorageConfig {
                            data_dir: dir.path().to_owned(),
                            hot_target_rows: 10,
                            ..Default::default()
                        },
                        historical,
                        counts,
                        publish_second,
                        keep_first,
                    );
                }
            }
        }
    }

    #[test]
    fn published_ingestion_hardens_before_wal_reorg_and_metadata_mutation() {
        for action in 0..7 {
            let dir = TempDir::new().unwrap();
            let mut storage = NativeStorage::open(NativeStorageConfig {
                data_dir: dir.path().to_owned(),
                ..Default::default()
            })
            .unwrap();
            let prior = ingestion_header(99, B256::ZERO);
            let header = ingestion_header(100, prior.hash_slow());
            ingest_test_batch(
                &mut storage,
                false,
                &ingestion_rows(3, &header),
                &header,
                &prior,
            )
            .unwrap();
            storage.checkpoint().unwrap();
            assert!(storage.published_ingestion.is_some());
            durability::inject_failure(usize::MAX);
            match action {
                0 => storage.write_batch(&ingestion_rows(2, &header)).unwrap(),
                1 => {
                    storage
                        .write_historical_batch(&ingestion_rows(2, &prior))
                        .unwrap();
                }
                2 => {
                    storage.mark_non_canonical(header.hash_slow()).unwrap();
                }
                3 => storage.record_historical_floor(&prior).unwrap(),
                4 => storage
                    .record_chain_anchors(storage.chain_anchors())
                    .unwrap(),
                5 => {
                    storage.compact_eligible_segments_limit(1).unwrap();
                }
                _ => storage
                    .record_sync_head(header.number, header.hash_slow(), header.timestamp)
                    .unwrap(),
            }
            let events = durability::take_events();
            assert_eq!(
                events.first(),
                Some(&("sync_directory", storage.paths.root().to_owned())),
                "action {action}: {events:?}"
            );
            assert!(storage.published_ingestion.is_none());
        }
    }

    #[test]
    fn published_ingestion_blocks_compaction_until_hardened_or_reopened() {
        for reopen in [false, true] {
            let dir = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: dir.path().to_owned(),
                hot_target_rows: 10,
                compaction_safety_margin_blocks: 0,
            };
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            let prior = ingestion_header(99, B256::ZERO);
            let header = ingestion_header(100, prior.hash_slow());
            // Keep a real raw compaction candidate as sync now creates bundles.
            storage.write_batch(&ingestion_rows(1, &prior)).unwrap();
            storage.checkpoint().unwrap();
            ingest_test_batch(
                &mut storage,
                false,
                &ingestion_rows(13, &header),
                &header,
                &prior,
            )
            .unwrap();
            storage.checkpoint().unwrap();
            assert!(storage.raw_segment_compaction_plan(8).unwrap().is_empty());
            let next = ingestion_header(101, header.hash_slow());
            ingest_test_batch(
                &mut storage,
                false,
                &ingestion_rows(13, &next),
                &next,
                &header,
            )
            .unwrap();
            storage.checkpoint().unwrap();
            assert!(storage.raw_segment_compaction_plan(8).unwrap().is_empty());
            durability::inject_failure(usize::MAX);
            if reopen {
                drop(storage);
                storage = NativeStorage::open(config).unwrap();
            } else {
                storage.checkpoint_durable().unwrap();
            }
            let events = durability::take_events();
            assert!(
                events
                    .iter()
                    .any(|(op, path)| *op == "sync_directory" && path == storage.paths.root())
            );
            assert!(storage.published_ingestion.is_none());
            assert!(!storage.raw_segment_compaction_plan(8).unwrap().is_empty());
        }
    }

    #[test]
    fn published_ingestion_bounds_survive_frequent_checkpoints() {
        struct FrozenClock;
        impl Drop for FrozenClock {
            fn drop(&mut self) {
                CHECKPOINT_CLOCK.set(None);
            }
        }
        // This case isolates the count bound. A slow/loaded filesystem can
        // legitimately trigger the independently tested five-second bound first.
        CHECKPOINT_CLOCK.set(Some(
            std::time::Instant::now() - std::time::Duration::from_secs(60),
        ));
        let _clock = FrozenClock;
        for historical in [false, true] {
            let dir = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: dir.path().to_owned(),
                ..Default::default()
            };
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            let mut prior = ingestion_header(100, B256::ZERO);
            let mut origin = None;
            for batch in 1..=64 {
                let next = ingestion_header(
                    if historical {
                        prior.number - 1
                    } else {
                        prior.number + 1
                    },
                    prior.hash_slow(),
                );
                ingest_test_batch(&mut storage, historical, &[], &next, &prior).unwrap();
                storage.checkpoint().unwrap();
                if batch < 64 {
                    let published = storage.published_ingestion.as_ref().unwrap();
                    assert_eq!(published.batches, batch);
                    assert_eq!(
                        published.bytes,
                        u64::from(batch) * crate::wal::validated_payload_len(&[]).unwrap() as u64
                    );
                    let original = origin
                        .get_or_insert((published.origin.next_segment_id, published.started_at));
                    assert_eq!(
                        (published.origin.next_segment_id, published.started_at),
                        *original,
                    );
                } else {
                    assert!(storage.pending_ingestion.is_none());
                    assert!(storage.published_ingestion.is_none());
                }
                prior = next;
            }
            drop(storage);
            let recovered = NativeStorage::open(config).unwrap();
            assert_eq!(recovered.total_rows(), 0);
            if historical {
                assert_eq!(recovered.historical_floor_header(), Some(&prior));
            } else {
                assert_eq!(recovered.sync_head().unwrap().block_hash, prior.hash_slow());
            }
        }
    }

    #[test]
    fn published_ingestion_hardens_before_byte_deadline_or_route_boundaries() {
        for historical in [false, true] {
            for boundary in 0..3 {
                let dir = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: dir.path().to_owned(),
                    ..Default::default()
                };
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                let prior = ingestion_header(100, B256::ZERO);
                if historical {
                    storage.ingest_historical_batch(&[], &prior).unwrap();
                } else {
                    storage
                        .ingest_canonical_batch(&[], &prior, std::slice::from_ref(&prior), None)
                        .unwrap();
                }
                storage.checkpoint().unwrap();
                let next_historical = if boundary == 2 {
                    !historical
                } else {
                    historical
                };
                let next =
                    ingestion_header(if next_historical { 99 } else { 101 }, prior.hash_slow());
                let rows = ingestion_rows(3, &next);
                let payload = crate::wal::validated_payload_len(&rows).unwrap() as u64;
                let published = storage.published_ingestion.as_mut().unwrap();
                match boundary {
                    // Exercise accumulated-byte accounting without allocating a
                    // 32 MiB fixture; oversized real batches are tested separately.
                    0 => published.bytes = CHECKPOINT_INGEST_BYTES - payload,
                    1 => published.started_at -= std::time::Duration::from_secs(6),
                    _ => {}
                }
                durability::inject_failure(usize::MAX);
                ingest_test_batch(
                    &mut storage,
                    if boundary == 2 {
                        !historical
                    } else {
                        historical
                    },
                    &rows,
                    &next,
                    &prior,
                )
                .unwrap();
                let events = durability::take_events();
                assert_eq!(
                    events.first(),
                    Some(&("sync_directory", storage.paths.root().to_owned())),
                    "boundary {boundary}: {events:?}",
                );
                assert!(storage.published_ingestion.is_none());
                let pending = storage.pending_ingestion.as_ref().unwrap();
                assert_eq!(pending.batches, 1);
                assert_eq!(pending.bytes, payload);
                storage.checkpoint_durable().unwrap();
                drop(storage);
                assert_eq!(
                    read_ingestion_rows(&NativeStorage::open(config).unwrap()),
                    rows
                );
            }
        }
    }

    #[test]
    fn published_ingestion_idle_hardening_and_failure_are_explicit() {
        let dir = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        let prior = ingestion_header(99, B256::ZERO);
        let header = ingestion_header(100, prior.hash_slow());
        ingest_test_batch(
            &mut storage,
            false,
            &ingestion_rows(3, &header),
            &header,
            &prior,
        )
        .unwrap();
        storage.checkpoint().unwrap();
        assert!(!storage.checkpoint_if_due().unwrap());
        storage.published_ingestion.as_mut().unwrap().started_at -=
            std::time::Duration::from_secs(6);
        assert!(storage.checkpoint_if_due().unwrap());
        assert!(!storage.checkpoint_if_due().unwrap());
        assert!(storage.published_ingestion.is_none());
        let next = ingestion_header(101, header.hash_slow());
        ingest_test_batch(
            &mut storage,
            false,
            &ingestion_rows(2, &next),
            &next,
            &header,
        )
        .unwrap();
        storage.checkpoint().unwrap();
        durability::inject_failure(0);
        assert!(storage.checkpoint_durable().is_err());
        durability::take_events();
        assert!(storage.published_ingestion.is_some());
        assert!(storage.ensure_writable().is_err());
        drop(storage);
        let recovered = NativeStorage::open(config).unwrap();
        assert_eq!(recovered.total_rows(), 5);
        assert_eq!(recovered.sync_head().unwrap().block_number, next.number);
    }

    #[test]
    fn ingestion_restart_rewinds_rows_and_progress_before_retry() {
        for historical in [false, true] {
            for incoming_count in [0, 3, 25] {
                let dir = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: dir.path().to_owned(),
                    hot_target_rows: 10,
                    ..Default::default()
                };
                let prior = ingestion_header(100, B256::ZERO);
                let incoming =
                    ingestion_header(if historical { 99 } else { 101 }, prior.hash_slow());
                let initial = ingestion_rows(3, &prior);
                let next = ingestion_rows(incoming_count, &incoming);
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                if historical {
                    storage.ingest_historical_batch(&initial, &prior).unwrap();
                } else {
                    storage
                        .ingest_canonical_batch(
                            &initial,
                            &prior,
                            std::slice::from_ref(&prior),
                            None,
                        )
                        .unwrap();
                }
                storage.checkpoint().unwrap();
                let catalog_before = fs::read(storage.paths.catalog_path()).unwrap();
                ingest_test_batch(&mut storage, historical, &next, &incoming, &prior).unwrap();
                assert_eq!(
                    fs::read(storage.paths.catalog_path()).unwrap(),
                    catalog_before
                );
                assert!(storage.wal.is_empty().unwrap());
                drop(storage);
                let mut reopened = NativeStorage::open(config.clone()).unwrap();
                assert_eq!(read_ingestion_rows(&reopened), initial);
                if historical {
                    assert_eq!(reopened.historical_floor().unwrap().block_number, 100);
                } else {
                    assert_eq!(reopened.sync_head().unwrap().block_number, 100);
                }
                ingest_test_batch(&mut reopened, historical, &next, &incoming, &prior).unwrap();
                reopened.checkpoint().unwrap();
                drop(reopened);
                let reopened = NativeStorage::open(config).unwrap();
                let mut expected = initial;
                expected.extend(next);
                expected.sort_by_key(|row| (row.block_number, row.log_index));
                assert_eq!(read_ingestion_rows(&reopened), expected);
                if historical {
                    assert_eq!(
                        reopened.historical_floor().unwrap().block_number,
                        incoming.number
                    );
                } else {
                    assert_eq!(reopened.sync_head().unwrap().block_number, incoming.number);
                }
            }
        }
    }

    #[test]
    fn ingestion_checkpoint_recovers_each_main_thread_failure_atomically() {
        for historical in [false, true] {
            for next_count in [5, 25] {
                for fail_after in 0..500 {
                    let dir = TempDir::new().unwrap();
                    let config = NativeStorageConfig {
                        data_dir: dir.path().to_owned(),
                        hot_target_rows: 10,
                        ..Default::default()
                    };
                    let prior = ingestion_header(100, B256::ZERO);
                    let incoming =
                        ingestion_header(if historical { 99 } else { 101 }, prior.hash_slow());
                    let initial = ingestion_rows(3, &prior);
                    let next = ingestion_rows(next_count, &incoming);
                    let mut storage = NativeStorage::open(config.clone()).unwrap();
                    if historical {
                        storage.ingest_historical_batch(&initial, &prior).unwrap();
                    } else {
                        storage
                            .ingest_canonical_batch(
                                &initial,
                                &prior,
                                std::slice::from_ref(&prior),
                                None,
                            )
                            .unwrap();
                    }
                    storage.checkpoint().unwrap();
                    storage.mark_non_canonical(prior.hash_slow()).unwrap();
                    durability::inject_failure(fail_after);
                    let result =
                        ingest_test_batch(&mut storage, historical, &next, &incoming, &prior)
                            .and_then(|_| storage.checkpoint());
                    let events = durability::take_events();
                    if result.is_err() {
                        assert!(
                            storage.ensure_writable().is_err(),
                            "failure {fail_after}: {events:?}"
                        );
                    }
                    drop(storage);
                    let mut recovered = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                    panic!(
                        "historical={historical}, failure={fail_after}, events={events:?}: {error}"
                    )
                });
                    let marker = if historical {
                        recovered.historical_floor().unwrap().block_number
                    } else {
                        recovered.sync_head().unwrap().block_number
                    };
                    if marker == 100 {
                        assert_eq!(
                            read_ingestion_rows(&recovered),
                            initial,
                            "failure={fail_after}"
                        );
                        ingest_test_batch(&mut recovered, historical, &next, &incoming, &prior)
                            .unwrap();
                        recovered.checkpoint().unwrap();
                    } else {
                        assert_eq!(marker, incoming.number);
                    }
                    let mut expected = initial.clone();
                    expected.extend(next);
                    expected.sort_by_key(|row| (row.block_number, row.log_index));
                    assert_eq!(read_ingestion_rows(&recovered), expected);
                    for segment in &recovered.catalog.segments {
                        if segment.row_count == 0 {
                            continue;
                        }
                        let reader =
                            SegmentReader::open(&recovered.paths.segment_dir(segment.id)).unwrap();
                        let rows = reader.read_log_rows(None).unwrap();
                        let canonical = reader.read_canonical().unwrap();
                        for (i, row) in rows.iter().enumerate() {
                            assert_eq!(
                                canonical.is_present(i as u64),
                                row.block_number != prior.number
                            );
                        }
                    }
                    drop(recovered);
                    assert_eq!(
                        read_ingestion_rows(&NativeStorage::open(config).unwrap()),
                        expected
                    );
                    if result.is_ok() {
                        assert!(
                            events.iter().any(|(operation, path)| {
                                *operation == "rename_temporary"
                                    && *path == dir.path().join("catalog.json")
                            }),
                            "successful matrix must publish the durable catalog: {events:?}"
                        );
                        break;
                    }
                    assert!(fail_after < 499, "failure matrix did not finish");
                }
            }
        }
    }

    #[test]
    fn ingestion_rollback_itself_is_resumable() {
        for historical in [false, true] {
            for next_count in [5, 25] {
                for fail_after in 0..200 {
                    let dir = TempDir::new().unwrap();
                    let config = NativeStorageConfig {
                        data_dir: dir.path().to_owned(),
                        hot_target_rows: 10,
                        ..Default::default()
                    };
                    let prior = ingestion_header(100, B256::ZERO);
                    let incoming =
                        ingestion_header(if historical { 99 } else { 101 }, prior.hash_slow());
                    let initial = ingestion_rows(3, &prior);
                    let next = ingestion_rows(next_count, &incoming);
                    let mut storage = NativeStorage::open(config.clone()).unwrap();
                    if historical {
                        storage.ingest_historical_batch(&initial, &prior).unwrap();
                    } else {
                        storage
                            .ingest_canonical_batch(
                                &initial,
                                &prior,
                                std::slice::from_ref(&prior),
                                None,
                            )
                            .unwrap();
                    }
                    storage.checkpoint().unwrap();
                    ingest_test_batch(&mut storage, historical, &next, &incoming, &prior).unwrap();
                    drop(storage);
                    durability::inject_failure(fail_after);
                    let result = NativeStorage::open(config.clone());
                    let events = durability::take_events();
                    let success = result.is_ok();
                    drop(result);
                    let recovered = NativeStorage::open(config).unwrap_or_else(|error| panic!("historical={historical}, rollback failure={fail_after}, events={events:?}: {error}"));
                    assert_eq!(read_ingestion_rows(&recovered), initial);
                    if success {
                        if historical && next_count == 5 {
                            assert!(
                                events
                                    .iter()
                                    .any(|(op, _)| *op == "bundle_trim_uncommitted_tail"),
                                "bundled rollback must trim its append suffix: {events:?}"
                            );
                        }
                        assert!(
                            events.iter().any(|(op, _)| matches!(
                                *op,
                                "ingestion_remove_uncommitted_segment" | "rename_temporary"
                            )),
                            "rollback must exercise publication or deletion: {events:?}"
                        );
                        break;
                    }
                    assert!(fail_after < 199, "rollback matrix did not finish");
                }
            }
        }
    }

    #[test]
    fn ingestion_origin_damage_is_rejected_without_discarding_rows() {
        let damaged = "catalog.json";
        let dir = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        };
        let header = ingestion_header(100, B256::ZERO);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage
            .ingest_canonical_batch(
                &ingestion_rows(3, &header),
                &header,
                std::slice::from_ref(&header),
                None,
            )
            .unwrap();
        storage.checkpoint().unwrap();
        let next = ingestion_header(101, header.hash_slow());
        ingest_test_batch(
            &mut storage,
            false,
            &ingestion_rows(3, &next),
            &next,
            &header,
        )
        .unwrap();
        let hot = storage.segment_path(storage.catalog.active_hot_segment.unwrap());
        let artifact = hot.join(crate::column_artifact::BUNDLE_PATH);
        let before = fs::read(&artifact).unwrap();
        drop(storage);
        let original = fs::read(dir.path().join(damaged)).unwrap();
        fs::write(dir.path().join(damaged), b"{}").unwrap();
        assert!(NativeStorage::open(config.clone()).is_err());
        assert_eq!(fs::read(&artifact).unwrap(), before);
        fs::write(dir.path().join(damaged), original).unwrap();
        assert_eq!(NativeStorage::open(config).unwrap().total_rows(), 3);
    }

    #[test]
    fn ingestion_discards_damaged_segments_that_have_no_committed_rows() {
        for historical in [false, true] {
            let damaged = ["segment.json", crate::column_artifact::BUNDLE_PATH];
            for damage in damaged {
                let dir = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: dir.path().to_owned(),
                    hot_target_rows: 10,
                    ..Default::default()
                };
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                let prior = ingestion_header(99, B256::ZERO);
                let header = ingestion_header(100, prior.hash_slow());
                let rows = ingestion_rows(13, &header);
                ingest_test_batch(&mut storage, historical, &rows, &header, &prior).unwrap();
                let catalog_before = fs::read(storage.paths.catalog_path()).unwrap();
                for segment in storage
                    .segments()
                    .iter()
                    .filter(|segment| segment.row_count > 0)
                {
                    fs::write(storage.segment_path(segment.id).join(damage), b"broken").unwrap();
                }
                drop(storage);
                let mut reopened = NativeStorage::open(config.clone()).unwrap();
                assert_eq!(reopened.total_rows(), 0);
                assert!(reopened.sync_head().is_none());
                assert!(reopened.historical_floor().is_none());
                assert_eq!(
                    fs::read(reopened.paths.catalog_path()).unwrap(),
                    catalog_before
                );
                ingest_test_batch(&mut reopened, historical, &rows, &header, &prior).unwrap();
                reopened.checkpoint().unwrap();
                drop(reopened);
                assert_eq!(
                    read_ingestion_rows(&NativeStorage::open(config).unwrap()),
                    rows
                );
            }
        }
    }

    #[test]
    fn ingestion_rejects_unencodable_headers_before_publishing_rows() {
        let dir = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        let before = fs::read(storage.paths.catalog_path()).unwrap();
        let mut header = ingestion_header(100, B256::ZERO);
        header.extra_data = vec![0; 33].into();
        let rows = ingestion_rows(3, &header);
        assert!(
            storage
                .ingest_canonical_batch(&rows, &header, std::slice::from_ref(&header), None)
                .is_err()
        );
        assert!(
            storage
                .record_canonical_state(&header, std::slice::from_ref(&header))
                .is_err()
        );
        assert!(
            storage
                .rewind_canonical_state(std::slice::from_ref(&header), None)
                .is_err()
        );
        assert!(storage.ingest_historical_batch(&rows, &header).is_err());
        assert!(storage.record_historical_floor(&header).is_err());
        assert_eq!(storage.total_rows(), 0);
        assert!(storage.sync_head().is_none());
        assert!(storage.pending_ingestion.is_none());
        assert_eq!(fs::read(storage.paths.catalog_path()).unwrap(), before);
    }

    #[test]
    fn ingestion_empty_epochs_have_bounded_replay_and_config_change_recovers() {
        let dir = TempDir::new().unwrap();
        let mut config = NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        let mut headers = Vec::new();
        let mut parent_hash = B256::ZERO;
        for number in 1..=65 {
            let header = ingestion_header(number, parent_hash);
            parent_hash = header.hash_slow();
            headers.push(header.clone());
            storage
                .ingest_canonical_batch(&[], &header, &headers, None)
                .unwrap();
        }
        assert_eq!(storage.pending_ingestion.as_ref().unwrap().batches, 1);
        drop(storage);
        config.hot_target_rows = 10;
        let reopened = NativeStorage::open(config).unwrap();
        assert_eq!(reopened.sync_head().unwrap().block_number, 64);
        assert_eq!(reopened.total_rows(), 0);
        assert_eq!(reopened.catalog.hot_target_rows, 10);
    }

    #[test]
    fn ingestion_route_and_durable_api_switches_checkpoint_before_writes() {
        let dir = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        let header = ingestion_header(100, B256::ZERO);
        let prior = ingestion_rows(3, &header);
        storage
            .ingest_canonical_batch(&prior, &header, std::slice::from_ref(&header), None)
            .unwrap();
        let historical = ingestion_header(99, B256::ZERO);
        storage
            .ingest_historical_batch(&ingestion_rows(3, &historical), &historical)
            .unwrap();
        drop(storage);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        assert_eq!(read_ingestion_rows(&storage), prior);
        assert_eq!(storage.sync_head().unwrap().block_number, 100);
        assert!(storage.historical_floor().is_none());
        storage
            .ingest_historical_batch(&ingestion_rows(3, &historical), &historical)
            .unwrap();
        let mut invalid = ingestion_rows(3, &header);
        invalid[0].data_len += 1;
        assert!(storage.write_batch(&invalid).is_err());
        assert!(storage.pending_ingestion.is_some());
        storage.write_batch(&prior).unwrap();
        drop(storage);
        let recovered = NativeStorage::open(config).unwrap();
        assert_eq!(recovered.total_rows(), 9);
        assert_eq!(recovered.historical_floor().unwrap().block_number, 99);
    }

    #[test]
    fn ingestion_actual_process_exit_discards_unfinished_first_batch() {
        const CHILD: &str = "LOGEX_INGESTION_EXIT_CHILD";
        if let Some(path) = std::env::var_os(CHILD) {
            let mut storage = NativeStorage::open(NativeStorageConfig {
                data_dir: path.into(),
                ..Default::default()
            })
            .unwrap();
            let header = ingestion_header(100, B256::ZERO);
            storage
                .ingest_canonical_batch(
                    &ingestion_rows(3, &header),
                    &header,
                    std::slice::from_ref(&header),
                    None,
                )
                .unwrap();
            std::process::exit(0);
        }
        let dir = TempDir::new().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact").arg("native::storage::tests::ingestion_actual_process_exit_discards_unfinished_first_batch")
            .env(CHILD, dir.path()).output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let recovered = NativeStorage::open(NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(recovered.total_rows(), 0);
        assert!(recovered.sync_head().is_none());
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires LOGEX_TEST_VOLUME_A and LOGEX_TEST_VOLUME_B on isolated distinct mounts"]
    fn sparse_repacking_recovery_across_distinct_mounts() {
        use std::os::unix::fs::{MetadataExt, symlink};
        let roots = ["LOGEX_TEST_VOLUME_A", "LOGEX_TEST_VOLUME_B"].map(|name| {
            PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("set {name}")))
        });
        assert_ne!(
            fs::metadata(&roots[0]).unwrap().dev(),
            fs::metadata(&roots[1]).unwrap().dev()
        );
        for (root, other) in [(&roots[0], &roots[1]), (&roots[1], &roots[0])] {
            for historical in [false, true] {
                for committed in [false, true] {
                    let data = TempDir::new_in(root).unwrap();
                    let external = TempDir::new_in(other).unwrap();
                    symlink(external.path(), data.path().join("segments")).unwrap();
                    let (mut storage, mut expected, previous) =
                        sparse_repack_fixture(data.path(), historical);
                    let old_catalog = fs::read(storage.paths.catalog_path()).unwrap();
                    trigger_repack(&mut storage, historical, &mut expected, &previous);
                    let descriptor = storage
                        .catalog
                        .segments
                        .iter_mut()
                        .find(|segment| segment.row_count != 0)
                        .unwrap();
                    let replacement =
                        super::super::segment::repack_sparse_bundle(&storage.paths, descriptor)
                            .unwrap()
                            .unwrap();
                    let dir = storage.paths.segment_dir(descriptor.id);
                    if committed {
                        durability::publish_catalog_after_trees(
                            [dir.as_path()],
                            &storage.paths.catalog_path(),
                            &storage.catalog.encode().unwrap(),
                        )
                        .unwrap();
                        replacement.retire().unwrap();
                    } else {
                        // Simulate an interruption after complete replacement/manifest
                        // writes with the previous catalog still authoritative.
                        assert_eq!(fs::read(storage.paths.catalog_path()).unwrap(), old_catalog);
                        drop(replacement);
                        expected.pop();
                    }
                    let config = storage.config.clone();
                    drop(storage);
                    let reopened = NativeStorage::open(config).unwrap();
                    let reader = SegmentReader::open(&dir).unwrap();
                    assert_eq!(reader.read_log_rows(None).unwrap(), expected);
                    assert!(!reader.read_canonical().unwrap().is_present(12));
                    assert_eq!(reader.generation(), u64::from(committed));
                    assert_eq!(reopened.total_rows(), expected.len() as u64);
                    for generation in 0..=1 {
                        assert_eq!(
                            crate::column_artifact::bundle_path(&dir, generation).exists(),
                            generation == u64::from(committed)
                        );
                    }
                }
            }
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires LOGEX_TEST_VOLUME_A and LOGEX_TEST_VOLUME_B on isolated distinct mounts"]
    fn checkpoint_recovery_across_distinct_mounts() {
        use std::os::unix::fs::{MetadataExt, symlink};

        let roots = ["LOGEX_TEST_VOLUME_A", "LOGEX_TEST_VOLUME_B"].map(|name| {
            PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("set {name}")))
        });
        assert_ne!(
            fs::metadata(&roots[0]).unwrap().dev(),
            fs::metadata(&roots[1]).unwrap().dev(),
            "the supplied test roots must be on different mounted filesystems"
        );
        for (root, other) in [(&roots[0], &roots[1]), (&roots[1], &roots[0])] {
            for alias in ["wal", "segments"] {
                for historical in [false, true] {
                    // Only these newly allocated directories are changed or removed.
                    let data = TempDir::new_in(root).unwrap();
                    let external = TempDir::new_in(other).unwrap();
                    let config = NativeStorageConfig {
                        data_dir: data.path().to_path_buf(),
                        hot_target_rows: 6,
                        compaction_safety_margin_blocks: 2_048,
                    };
                    if alias == "wal" {
                        // Establish an empty catalog before installing the WAL
                        // alias. A WAL artifact without a catalog deliberately
                        // fails closed instead of initializing a new dataset.
                        drop(NativeStorage::open(config.clone()).unwrap());
                        let wal = data.path().join("wal/pending.wal");
                        assert!(!wal.exists()); // Created lazily by the first append.
                        // The first append must persist the newly created target
                        // name as well as ordering the journal on another device.
                        symlink(
                            external.path().join("pending.wal"),
                            data.path().join("wal/pending.wal"),
                        )
                        .unwrap();
                    } else {
                        symlink(external.path(), data.path().join("segments")).unwrap();
                    }
                    let mut storage = NativeStorage::open(config.clone()).unwrap();
                    let mut expected = Vec::new();
                    for (count, block) in [(3, 200), (12, 100)] {
                        let rows = make_rows(count, block);
                        durability::inject_failure(usize::MAX);
                        if historical {
                            storage.write_historical_batch(&rows).unwrap();
                        } else {
                            storage.write_batch(&rows).unwrap();
                        }
                        let events = durability::take_events();
                        #[cfg(target_vendor = "apple")]
                        assert!(events.iter().any(|(op, _)| *op == "cross_device_sync"));
                        #[cfg(not(target_vendor = "apple"))]
                        let _ = events;
                        expected.extend(rows);
                        // Close with a live checkpoint; recovery must handle both
                        // directions and historical segment rotation.
                        drop(storage);
                        storage = NativeStorage::open(config.clone()).unwrap();
                    }
                    storage.checkpoint().unwrap();
                    drop(storage);
                    let recovered = NativeStorage::open(config).unwrap();
                    let mut actual: Vec<_> = recovered
                        .segments()
                        .iter()
                        .filter(|segment| segment.row_count > 0)
                        .flat_map(|segment| {
                            SegmentReader::open(&recovered.segment_path(segment.id))
                                .unwrap()
                                .read_log_rows(None)
                                .unwrap()
                        })
                        .collect();
                    actual.sort_by_key(|row| (row.block_number, row.log_index));
                    expected.sort_by_key(|row| (row.block_number, row.log_index));
                    assert_eq!(actual, expected);
                    assert!(recovered.wal.is_empty().unwrap());
                    assert!(!RecoveryJournal::path(&recovered.paths).exists());
                }
            }
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires LOGEX_TEST_VOLUME_A and LOGEX_TEST_VOLUME_B on isolated distinct mounts"]
    fn sync_checkpoint_recovery_across_distinct_mounts() {
        use std::os::unix::fs::{MetadataExt, symlink};

        let roots = ["LOGEX_TEST_VOLUME_A", "LOGEX_TEST_VOLUME_B"].map(|name| {
            PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("set {name}")))
        });
        assert_ne!(
            fs::metadata(&roots[0]).unwrap().dev(),
            fs::metadata(&roots[1]).unwrap().dev()
        );
        for (root, other) in [(&roots[0], &roots[1]), (&roots[1], &roots[0])] {
            for historical in [false, true] {
                for count in [0, 3, 12] {
                    for commit in [false, true] {
                        // Only these new temporary directories are modified.
                        let data = TempDir::new_in(root).unwrap();
                        let columns = TempDir::new_in(other).unwrap();
                        symlink(columns.path(), data.path().join("segments")).unwrap();
                        let config = NativeStorageConfig {
                            data_dir: data.path().to_owned(),
                            hot_target_rows: 6,
                            ..Default::default()
                        };
                        let prior = ingestion_header(100, B256::ZERO);
                        let incoming =
                            ingestion_header(if historical { 99 } else { 101 }, prior.hash_slow());
                        let initial = ingestion_rows(3, &prior);
                        let next = ingestion_rows(count, &incoming);
                        let mut storage = NativeStorage::open(config.clone()).unwrap();
                        if historical {
                            storage.ingest_historical_batch(&initial, &prior).unwrap();
                        } else {
                            storage
                                .ingest_canonical_batch(
                                    &initial,
                                    &prior,
                                    std::slice::from_ref(&prior),
                                    None,
                                )
                                .unwrap();
                        }
                        storage.checkpoint().unwrap();
                        durability::inject_failure(usize::MAX);
                        ingest_test_batch(&mut storage, historical, &next, &incoming, &prior)
                            .unwrap();
                        if commit {
                            storage.checkpoint().unwrap();
                        }
                        let events = durability::take_events();
                        #[cfg(target_vendor = "apple")]
                        if commit && count > 0 {
                            let catalog = storage.paths.catalog_path();
                            let publish = events
                                .iter()
                                .position(|(op, path)| {
                                    *op == "rename_temporary" && path == &catalog
                                })
                                .unwrap();
                            assert!(events[..publish].iter().any(|(op, path)| {
                                matches!(*op, "cross_device_sync" | "sync_device")
                                    && fs::metadata(path).is_ok_and(|metadata| {
                                        metadata.dev() == fs::metadata(other).unwrap().dev()
                                    })
                            }));
                        }
                        #[cfg(not(target_vendor = "apple"))]
                        let _ = events;
                        drop(storage);
                        let mut expected = initial.clone();
                        if commit {
                            expected.extend(next.clone());
                        }
                        expected.sort_by_key(|row| (row.block_number, row.log_index));
                        for _ in 0..2 {
                            let storage = NativeStorage::open(config.clone()).unwrap();
                            assert_eq!(read_ingestion_rows(&storage), expected);
                            let head = if commit { &incoming } else { &prior };
                            if historical {
                                assert_eq!(storage.historical_floor_header(), Some(head));
                            } else {
                                assert_eq!(
                                    storage.sync_head().unwrap().block_hash,
                                    head.hash_slow()
                                );
                            }
                        }
                        if !commit {
                            let mut storage = NativeStorage::open(config.clone()).unwrap();
                            ingest_test_batch(&mut storage, historical, &next, &incoming, &prior)
                                .unwrap();
                            storage.checkpoint().unwrap();
                            drop(storage);
                            let storage = NativeStorage::open(config).unwrap();
                            expected.extend(next);
                            expected.sort_by_key(|row| (row.block_number, row.log_index));
                            assert_eq!(read_ingestion_rows(&storage), expected);
                        }
                    }
                }
            }
        }
    }

    #[cfg(unix)]
    #[test]
    #[ignore = "requires LOGEX_TEST_VOLUME_A and LOGEX_TEST_VOLUME_B on isolated distinct mounts"]
    fn published_checkpoint_recovery_across_distinct_mounts() {
        use std::os::unix::fs::{MetadataExt, symlink};
        let roots = ["LOGEX_TEST_VOLUME_A", "LOGEX_TEST_VOLUME_B"].map(|name| {
            PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("set {name}")))
        });
        assert_ne!(
            fs::metadata(&roots[0]).unwrap().dev(),
            fs::metadata(&roots[1]).unwrap().dev()
        );
        for (root, other) in [(&roots[0], &roots[1]), (&roots[1], &roots[0])] {
            for historical in [false, true] {
                for counts in [
                    (0, 0, 0),
                    (0, 3, 2),
                    (0, 25, 25),
                    (3, 0, 0),
                    (3, 3, 2),
                    (3, 25, 25),
                ] {
                    for (publish_second, keep_first) in
                        [(false, false), (false, true), (true, false), (true, true)]
                    {
                        let data = TempDir::new_in(root).unwrap();
                        let columns = TempDir::new_in(other).unwrap();
                        symlink(columns.path(), data.path().join("segments")).unwrap();
                        assert_published_epoch_rewind(
                            NativeStorageConfig {
                                data_dir: data.path().to_owned(),
                                hot_target_rows: 10,
                                ..Default::default()
                            },
                            historical,
                            counts,
                            publish_second,
                            keep_first,
                        );
                    }
                }
            }
        }
    }

    fn header(number: u64, parent_hash: B256, marker: u8) -> Header {
        let mut header = Header {
            number,
            parent_hash,
            gas_limit: 30_000_000,
            timestamp: 1_700_000_000 + number,
            ..Default::default()
        };
        header.extra_data = vec![marker].into();
        header
    }

    #[test]
    fn native_storage_bootstraps_and_rotates_segments() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        assert_eq!(storage.sealed_count(), 0);
        storage.write_batch(&make_rows(12, 100)).unwrap();
        assert_eq!(storage.sealed_count(), 1);
        assert_eq!(storage.total_rows(), 12);
        assert_eq!(storage.sealed_partition_metas()[0].row_count, 10);
        assert_eq!(storage.hot_partition_meta().row_count, 2);
    }

    #[test]
    fn native_storage_splits_oversized_batches_at_hot_target() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        storage.write_batch(&make_rows(25, 100)).unwrap();

        let sealed = storage.sealed_partition_metas();
        assert_eq!(sealed.len(), 2);
        assert_eq!(sealed[0].row_count, 10);
        assert_eq!(sealed[1].row_count, 10);
        assert_eq!(storage.hot_partition_meta().row_count, 5);
        assert_eq!(storage.total_rows(), 25);
    }

    #[test]
    fn native_storage_writes_historical_batches_as_compacted_sealed_segments() {
        let tmp = TempDir::new().unwrap();
        let rows = make_rows(25, 100);
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        storage.write_historical_batch(&rows).unwrap();

        let sealed = storage.sealed_partition_metas();
        assert_eq!(sealed.len(), 3);
        assert_eq!(
            sealed.iter().map(|meta| meta.row_count).collect::<Vec<_>>(),
            vec![10, 10, 5]
        );
        assert_eq!(storage.hot_partition_meta().row_count, 0);
        assert_eq!(storage.total_rows(), 25);

        let first_segment = storage.segment_path(sealed[0].id);
        assert!(!first_segment.join("address.col").exists());
        assert!(
            first_segment
                .join(crate::column_artifact::BUNDLE_PATH)
                .exists()
        );

        let reader = SegmentReader::open(&first_segment).unwrap();
        assert_eq!(reader.read_log_rows(None).unwrap(), rows[..10].to_vec());

        storage
            .record_sync_head(10_000, B256::repeat_byte(0xAA), 999)
            .unwrap();
        assert_eq!(storage.raw_compaction_backlog_count().unwrap(), 0);
        assert!(!first_segment.join("address.col").exists());
        assert!(
            first_segment
                .join(crate::column_artifact::BUNDLE_PATH)
                .exists()
        );
        assert!(storage.active_historical_segment_id().is_some());

        drop(storage);
        let reloaded = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        assert_eq!(reloaded.total_rows(), 25);
        assert_eq!(reloaded.sealed_count(), 3);
        assert_eq!(reloaded.hot_partition_meta().row_count, 0);
        assert!(reloaded.active_historical_segment_id().is_some());
    }

    #[test]
    fn native_storage_writes_dense_subtarget_historical_batches_compacted() {
        let tmp = TempDir::new().unwrap();
        let rows = make_rows(600, 100);
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 2_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        storage.write_historical_batch(&rows).unwrap();

        let sealed = storage.sealed_partition_metas();
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].row_count, 600);
        assert_eq!(storage.active_historical_segment_id(), None);

        let segment_dir = storage.segment_path(sealed[0].id);
        assert!(!segment_dir.join("address.col").exists());
        assert!(
            segment_dir
                .join(crate::column_artifact::BUNDLE_PATH)
                .exists()
        );
        let reader = SegmentReader::open(&segment_dir).unwrap();
        assert_eq!(reader.read_log_rows(None).unwrap(), rows);
    }

    #[test]
    fn committed_bundle_corruption_is_preserved_before_rollback() {
        for historical in [false, true] {
            for damage in 0..3 {
                let tmp = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: tmp.path().to_owned(),
                    hot_target_rows: 1_000,
                    ..Default::default()
                };
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                let prior = ingestion_header(100, B256::ZERO);
                let next = ingestion_header(if historical { 99 } else { 101 }, prior.hash_slow());
                ingest_test_initial_batch(
                    &mut storage,
                    historical,
                    &ingestion_rows(3, &prior),
                    &prior,
                )
                .unwrap();
                storage.checkpoint().unwrap();
                let id = if historical {
                    storage.active_historical_segment_id().unwrap()
                } else {
                    storage.catalog.active_hot_segment.unwrap()
                };
                let reference = storage
                    .segments()
                    .iter()
                    .find(|segment| segment.id == id)
                    .unwrap()
                    .column_bundle
                    .clone()
                    .unwrap();
                ingest_test_batch(
                    &mut storage,
                    historical,
                    &ingestion_rows(3, &next),
                    &next,
                    &prior,
                )
                .unwrap();
                let dir = storage.segment_path(id);
                let bundle_path = dir.join(crate::column_artifact::BUNDLE_PATH);
                let mut bytes = fs::read(&bundle_path).unwrap();
                match damage {
                    // The canonical bitmap is the first payload written to a new
                    // bundle, before the column workers. Its CRC must fail closed.
                    0 => bytes[8] ^= 1,
                    1 => bytes[reference.table_offset as usize] ^= 1,
                    2 => bytes.truncate(reference.end().unwrap() as usize - 1),
                    _ => unreachable!(),
                }
                fs::write(&bundle_path, &bytes).unwrap();
                let paths = [
                    bundle_path,
                    dir.join("segment.json"),
                    storage.paths.catalog_path(),
                ];
                let before: Vec<_> = paths.iter().map(|path| fs::read(path).unwrap()).collect();
                drop(storage);
                assert!(NativeStorage::open(config).is_err(), "damage {damage}");
                for (path, bytes) in paths.iter().zip(before) {
                    assert_eq!(
                        fs::read(path).unwrap(),
                        bytes,
                        "damage {damage}: {}",
                        path.display()
                    );
                }
            }
        }
    }

    #[test]
    fn bundled_readers_keep_their_snapshot_across_historical_appends() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            hot_target_rows: 1_000_000,
            ..Default::default()
        })
        .unwrap();
        let mut rows = make_rows(32_773, 100);
        for (index, row) in rows.iter_mut().enumerate() {
            row.topic1 = (index % 3 == 0).then(|| B256::repeat_byte(index as u8));
            row.topic2 = (index % 5 == 0).then(|| B256::repeat_byte((index >> 8) as u8));
        }
        storage.write_historical_batch(&rows[..16_385]).unwrap();
        storage.checkpoint().unwrap();
        let id = storage.active_historical_segment_id().unwrap();
        let snapshot = SegmentReader::open(&storage.segment_path(id)).unwrap();
        storage.write_historical_batch(&rows[16_385..]).unwrap();
        assert_eq!(storage.active_historical_segment_id(), Some(id));
        assert_eq!(snapshot.read_log_rows(None).unwrap(), rows[..16_385]);
        let selected = [16_384, 0, 16_383, 16_384, 7];
        assert_eq!(
            snapshot.read_log_rows(Some(&selected)).unwrap(),
            selected
                .iter()
                .map(|id| rows[*id as usize].clone())
                .collect::<Vec<_>>()
        );
        assert!(snapshot.read_log_rows(Some(&[16_385])).is_err());
        assert_eq!(snapshot.read_canonical_len().unwrap(), 16_385);
        let prior = SegmentReader::open(&storage.segment_path(id)).unwrap();
        let hash = rows[0].block_hash;
        let marked = rows.iter().filter(|row| row.block_hash == hash).count() as u64;
        assert_eq!(storage.mark_non_canonical(hash).unwrap(), marked);
        let current = SegmentReader::open(&storage.segment_path(id)).unwrap();
        let flags = current.read_canonical().unwrap();
        let old_flags = prior.read_canonical().unwrap();
        for (id, row) in rows.iter().enumerate() {
            assert!(old_flags.is_present(id as u64));
            assert_eq!(flags.is_present(id as u64), row.block_hash != hash);
        }
        assert!(!storage.segment_path(id).join("canonical.bitmap").exists());
        assert_eq!(
            SegmentReader::open(&storage.segment_path(id))
                .unwrap()
                .read_log_rows(None)
                .unwrap(),
            rows
        );
    }

    #[test]
    fn bundles_rotate_before_their_extent_capacity_is_exhausted() {
        for historical in [false, true] {
            use crate::bundle::{BundleWriter, MAX_EXTENTS};
            use crate::column_artifact::{BUNDLE_PATH, COLUMN_NAMES, ColumnArtifacts};
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_owned(),
                hot_target_rows: 1_000_000,
                ..Default::default()
            };
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            let header = ingestion_header(100, B256::ZERO);
            let mut initial = ingestion_rows(1, &header);
            let mut seed = 37u64;
            initial[0].data = (0..8192)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (seed >> 32) as u8
                })
                .collect::<Vec<_>>()
                .into();
            initial[0].data_len = 8192;
            ingest_test_initial_batch(&mut storage, historical, &initial, &header).unwrap();
            storage.checkpoint().unwrap();
            let id = if historical {
                storage.active_historical_segment_id().unwrap()
            } else {
                storage.catalog.active_hot_segment.unwrap()
            };
            let dir = storage.segment_path(id);
            let manifest: SegmentManifest =
                serde_json::from_slice(&fs::read(dir.join("segment.json")).unwrap()).unwrap();
            let artifacts = ColumnArtifacts::open(&dir, Some(&manifest)).unwrap();
            let staged = dir.join("fragmented.bundle");
            let writer = BundleWriter::create(&staged).unwrap();
            for (id, name) in COLUMN_NAMES.iter().enumerate() {
                let data = artifacts.read(&format!("columns/{name}.pages")).unwrap();
                if id == 13 {
                    assert!(data.len() > MAX_EXTENTS);
                    for byte in &data[..MAX_EXTENTS - 1] {
                        writer.append_data(id as u8, &[*byte]).unwrap();
                    }
                    writer
                        .append_data(id as u8, &data[MAX_EXTENTS - 1..])
                        .unwrap();
                } else {
                    writer.append_data(id as u8, &data).unwrap();
                }
                writer
                    .append_data(
                        id as u8 + 14,
                        &artifacts
                            .read(&format!("columns/{name}.pages.idx"))
                            .unwrap()[crate::page::PAGE_INDEX_HEADER_BYTES..],
                    )
                    .unwrap();
                if (9..13).contains(&id) {
                    writer
                        .replace_metadata(
                            id as u8 + 19,
                            &crate::column_artifact::encode_bitmap(
                                &artifacts.read(&format!("columns/{name}.null")).unwrap(),
                                manifest.row_count,
                            )
                            .unwrap(),
                        )
                        .unwrap();
                }
            }
            writer
                .replace_metadata(
                    crate::column_artifact::CANONICAL_STREAM,
                    &crate::column_artifact::encode_bitmap(
                        &artifacts.read("canonical.bitmap").unwrap(),
                        manifest.row_count,
                    )
                    .unwrap(),
                )
                .unwrap();
            let reference = writer.finish(1).unwrap();
            drop(artifacts);
            fs::rename(staged, dir.join(BUNDLE_PATH)).unwrap();
            let descriptor = storage
                .catalog
                .segments
                .iter_mut()
                .find(|segment| segment.id == id)
                .unwrap();
            descriptor.column_bundle = Some(reference);
            // This fixture deliberately replaces the transport before publishing
            // its new identity; standalone refresh correctly rejects a stale one.
            super::super::segment::persist_segment_manifest_with_columns(
                &storage.paths,
                descriptor,
                manifest.columns,
            )
            .unwrap();
            storage.persist_catalog().unwrap();
            drop(storage);
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            let next = ingestion_header(if historical { 99 } else { 101 }, header.hash_slow());
            let incoming = ingestion_rows(2, &next);
            ingest_test_batch(&mut storage, historical, &incoming, &next, &header).unwrap();
            storage.checkpoint().unwrap();
            let active = if historical {
                storage.active_historical_segment_id()
            } else {
                storage.catalog.active_hot_segment
            };
            assert_ne!(active, Some(id));
            assert_eq!(
                storage
                    .segments()
                    .iter()
                    .filter(|s| s.row_count > 0)
                    .count(),
                2
            );
            let mut actual = Vec::new();
            for segment in storage.segments() {
                if segment.row_count > 0 {
                    actual.extend(
                        SegmentReader::open(&storage.segment_path(segment.id))
                            .unwrap()
                            .read_log_rows(None)
                            .unwrap(),
                    );
                }
            }
            initial.extend(incoming);
            assert_eq!(actual, initial);
            drop(storage);
            assert_eq!(NativeStorage::open(config).unwrap().total_rows(), 3);
        }
    }

    #[test]
    fn bundled_append_recovery_preserves_the_checkpoint_at_each_phase() {
        for historical in [false, true] {
            for phase in 0..10 {
                let tmp = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: tmp.path().to_owned(),
                    hot_target_rows: 1_000,
                    ..Default::default()
                };
                let prior = ingestion_header(100, B256::ZERO);
                let next = ingestion_header(if historical { 99 } else { 101 }, prior.hash_slow());
                let initial = ingestion_rows(3, &prior);
                let incoming = ingestion_rows(5, &next);
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                ingest_test_initial_batch(&mut storage, historical, &initial, &prior).unwrap();
                storage.checkpoint().unwrap();
                storage.mark_non_canonical(prior.hash_slow()).unwrap();
                let id = if historical {
                    storage.active_historical_segment_id().unwrap()
                } else {
                    storage.catalog.active_hot_segment.unwrap()
                };
                let dir = storage.segment_path(id);
                let manifest_bytes = fs::read(dir.join("segment.json")).unwrap();
                let bundle_path = dir.join(crate::column_artifact::BUNDLE_PATH);
                let old_bundle = fs::read(&bundle_path).unwrap();
                assert!(!dir.join("canonical.bitmap").exists());
                ingest_test_batch(&mut storage, historical, &incoming, &next, &prior).unwrap();
                let current: SegmentManifest =
                    serde_json::from_slice(&fs::read(dir.join("segment.json")).unwrap()).unwrap();
                let new_reference = current.column_bundle.as_ref().unwrap();
                assert!(fs::read(&bundle_path).unwrap().starts_with(&old_bundle));
                if phase == 9 {
                    storage.checkpoint().unwrap();
                }
                drop(storage);
                // The catalog alone commits rows and canonical state. The
                // derived manifest may be absent, malformed or newer than a torn
                // bundle suffix without changing the retained checkpoint.
                if phase < 4 {
                    fs::write(dir.join("segment.json"), &manifest_bytes).unwrap();
                } else if phase == 4 {
                    fs::remove_file(dir.join("segment.json")).unwrap();
                } else if phase == 5 {
                    fs::write(dir.join("segment.json"), b"{torn metadata").unwrap();
                } else if phase == 6 {
                    let mut forged = current.clone();
                    forged.segment_id += 1;
                    forged.canonical_rows_path = "../unrelated".into();
                    fs::write(
                        dir.join("segment.json"),
                        serde_json::to_vec(&forged).unwrap(),
                    )
                    .unwrap();
                }
                let tail_end = match phase {
                    0 => Some(old_bundle.len() as u64 + 1),
                    1 => Some(new_reference.table_offset),
                    2 | 7 => {
                        Some(new_reference.table_offset + u64::from(new_reference.table_len) / 2)
                    }
                    _ => None,
                };
                if let Some(end) = tail_end {
                    fs::OpenOptions::new()
                        .write(true)
                        .open(&bundle_path)
                        .unwrap()
                        .set_len(end)
                        .unwrap();
                }
                for restart in 0..2 {
                    let recovered = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                        panic!("phase {phase}, restart {restart}: {error}")
                    });
                    let reader = SegmentReader::open(&recovered.segment_path(id)).unwrap();
                    let mut expected = initial.clone();
                    if phase == 9 {
                        expected.extend(incoming.clone());
                    }
                    assert_eq!(
                        reader.read_log_rows(None).unwrap(),
                        expected,
                        "phase {phase}"
                    );
                    let canonical = reader.read_canonical().unwrap();
                    for row in 0..expected.len() {
                        assert_eq!(canonical.is_present(row as u64), row >= initial.len());
                    }
                    assert_eq!(
                        if historical {
                            recovered.historical_floor().unwrap().block_number
                        } else {
                            recovered.sync_head().unwrap().block_number
                        },
                        if phase == 9 { next.number } else { 100 }
                    );
                    if phase < 9 {
                        assert_eq!(fs::read(&bundle_path).unwrap(), old_bundle);
                    }
                    if phase == 9 {
                        // A clean restart must preserve the compressed representation.
                        assert!(bundle_path.exists());
                        assert!(!dir.join("address.col").exists());
                    }
                }
                if phase < 9 {
                    let mut recovered = NativeStorage::open(config.clone()).unwrap();
                    ingest_test_batch(&mut recovered, historical, &incoming, &next, &prior)
                        .unwrap();
                    recovered.checkpoint().unwrap();
                    drop(recovered);
                    let recovered = NativeStorage::open(config).unwrap();
                    assert_eq!(recovered.total_rows(), 8);
                    let mut expected = initial;
                    expected.extend(incoming);
                    assert_eq!(
                        SegmentReader::open(&recovered.segment_path(id))
                            .unwrap()
                            .read_log_rows(None)
                            .unwrap(),
                        expected
                    );
                }
            }
        }
    }

    #[test]
    fn bundled_reorg_recovers_each_publication_failure_and_preserves_snapshots() {
        for historical in [false, true] {
            let mut steps = 0;
            for failure in std::iter::once(usize::MAX).chain(0..) {
                if failure != usize::MAX && failure >= steps {
                    break;
                }
                let tmp = TempDir::new().unwrap();
                let config = NativeStorageConfig {
                    data_dir: tmp.path().to_owned(),
                    hot_target_rows: 1_000,
                    ..Default::default()
                };
                let header = ingestion_header(100, B256::ZERO);
                let rows = ingestion_rows(3, &header);
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                ingest_test_initial_batch(&mut storage, historical, &rows, &header).unwrap();
                storage.checkpoint().unwrap();
                let id = if historical {
                    storage.active_historical_segment_id().unwrap()
                } else {
                    storage.catalog.active_hot_segment.unwrap()
                };
                let snapshot = SegmentReader::open(&storage.segment_path(id)).unwrap();
                let catalog = storage.paths.catalog_path();
                let before = fs::read(&catalog).unwrap();
                durability::inject_failure(failure);
                let result = storage.mark_non_canonical(header.hash_slow());
                let events = durability::take_events();
                if failure == usize::MAX {
                    assert_eq!(result.unwrap(), 3);
                    steps = events.len();
                    assert!(steps > 0);
                } else {
                    assert!(result.is_err(), "failure {failure}: {events:?}");
                    assert!(storage.ensure_writable().is_err());
                }
                let committed = fs::read(&catalog).unwrap() != before;
                drop(storage);
                for restart in 0..2 {
                    let recovered = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                        panic!("failure {failure}, restart {restart}: {events:?}: {error}")
                    });
                    let reader = SegmentReader::open(&recovered.segment_path(id)).unwrap();
                    assert_eq!(reader.read_log_rows(None).unwrap(), rows);
                    let bits = reader.read_canonical().unwrap();
                    assert_eq!(bits.len(), 3);
                    for row in 0..3 {
                        assert_eq!(bits.is_present(row), !committed);
                        assert!(snapshot.read_canonical().unwrap().is_present(row));
                    }
                }
                let mut recovered = NativeStorage::open(config.clone()).unwrap();
                assert_eq!(
                    recovered.mark_non_canonical(header.hash_slow()).unwrap(),
                    if committed { 0 } else { 3 }
                );
                drop(recovered);
                let recovered = NativeStorage::open(config).unwrap();
                let reader = SegmentReader::open(&recovered.segment_path(id)).unwrap();
                let bits = reader.read_canonical().unwrap();
                assert_eq!(reader.read_log_rows(None).unwrap(), rows);
                assert!((0..3).all(|row| !bits.is_present(row)));
            }
        }
    }

    #[test]
    fn stale_bundle_manifest_cannot_recanonicalize_previously_removed_rows() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            hot_target_rows: 1_000,
            ..Default::default()
        };
        let first = ingestion_header(100, B256::ZERO);
        let second = ingestion_header(99, B256::ZERO);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        for header in [&first, &second] {
            storage
                .ingest_historical_batch(&ingestion_rows(3, header), header)
                .unwrap();
        }
        storage.checkpoint().unwrap();
        let id = storage.active_historical_segment_id().unwrap();
        let dir = storage.segment_path(id);
        let manifest = dir.join("segment.json");
        let older = fs::read(&manifest).unwrap();
        assert_eq!(storage.mark_non_canonical(first.hash_slow()).unwrap(), 3);
        // The old manifest is syntactically valid and references the same rows,
        // but predates the first canonical update. It must not seed another one.
        fs::write(&manifest, &older).unwrap();
        let paths = [
            manifest,
            dir.join(crate::column_artifact::BUNDLE_PATH),
            storage.paths.catalog_path(),
        ];
        let before: Vec<_> = paths.iter().map(|path| fs::read(path).unwrap()).collect();
        assert!(storage.mark_non_canonical(second.hash_slow()).is_err());
        assert!(storage.ensure_writable().is_err());
        for (path, bytes) in paths.iter().zip(before) {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
        drop(storage);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        let flags = SegmentReader::open(&dir).unwrap().read_canonical().unwrap();
        for row in 0..6 {
            assert_eq!(flags.is_present(row), row >= 3);
        }
        assert_eq!(storage.mark_non_canonical(second.hash_slow()).unwrap(), 3);
        drop(storage);
        let storage = NativeStorage::open(config).unwrap();
        let flags = SegmentReader::open(&dir).unwrap().read_canonical().unwrap();
        assert_eq!(storage.total_rows(), 6);
        assert!((0..6).all(|row| !flags.is_present(row)));
    }

    #[test]
    fn historical_ingestion_coalesces_medium_chunks() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            hot_target_rows: 1_000_000,
            ..Default::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        for number in (100..116).rev() {
            let header = ingestion_header(number, B256::ZERO);
            let hash = header.hash_slow();
            let rows: Vec<_> = make_rows(15_360, number)
                .into_iter()
                .map(|mut row| {
                    row.block_number = number;
                    row.block_hash = hash;
                    row.timestamp = header.timestamp;
                    row
                })
                .collect();
            storage.ingest_historical_batch(&rows, &header).unwrap();
        }
        storage.finalize_active_historical_segment().unwrap();
        drop(storage);
        let storage = NativeStorage::open(config).unwrap();
        assert_eq!(storage.total_rows(), 16 * 15_360);
        assert_eq!(storage.historical_floor().unwrap().block_number, 100);
        // Medium caller batches must fill the existing segment, rather than
        // multiplying segment files and downstream index/query work.
        assert_eq!(
            storage
                .segments()
                .iter()
                .filter(|s| s.row_count > 0)
                .count(),
            1
        );
    }

    #[test]
    fn native_storage_coalesces_sparse_historical_writes_across_batches() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        storage.write_historical_batch(&make_rows(3, 100)).unwrap();
        let active_segment = storage.active_historical_segment_id().unwrap();
        storage.write_historical_batch(&make_rows(4, 90)).unwrap();

        let sealed = storage.sealed_partition_metas();
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].id, active_segment);
        assert_eq!(sealed[0].row_count, 7);
        assert_eq!(storage.total_rows(), 7);
        assert!(
            storage
                .segment_path(active_segment)
                .join(crate::column_artifact::BUNDLE_PATH)
                .exists()
        );

        storage.write_historical_batch(&make_rows(3, 80)).unwrap();
        assert_eq!(storage.active_historical_segment_id(), None);
        let sealed = storage.sealed_partition_metas();
        assert_eq!(sealed.len(), 1);
        assert_eq!(sealed[0].row_count, 10);
        assert!(
            !storage
                .segment_path(active_segment)
                .join("address.col")
                .exists()
        );
        assert!(
            storage
                .segment_path(active_segment)
                .join(crate::column_artifact::BUNDLE_PATH)
                .exists()
        );
    }

    #[cfg(unix)]
    #[test]
    fn catalog_reopens_symlinked_segment_directories() {
        let tmp = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        let rows = make_rows(10, 100);
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        storage.write_historical_batch(&rows).unwrap();
        let segment_id = storage
            .segments()
            .iter()
            .find(|segment| segment.kind == SegmentKind::Sealed)
            .map(|segment| segment.id)
            .unwrap();
        let segment_path = storage.segment_path(segment_id);
        let external_path = external
            .path()
            .join(segment_path.file_name().expect("segment path has a name"));
        fs::rename(&segment_path, &external_path).unwrap();
        std::os::unix::fs::symlink(&external_path, &segment_path).unwrap();
        storage.checkpoint().unwrap();
        drop(storage);

        let reloaded = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        assert_eq!(reloaded.total_rows(), 10);
        assert_eq!(reloaded.sealed_count(), 1);
        assert!(reloaded.segment_path(segment_id).is_dir());
    }

    #[test]
    fn historical_batch_replaces_abandoned_segment_directory() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        let stale_segment_dir = storage.paths.segment_dir(storage.catalog.next_segment_id);
        fs::create_dir_all(stale_segment_dir.join("columns")).unwrap();
        fs::write(stale_segment_dir.join("stale"), b"stale").unwrap();

        storage.write_historical_batch(&make_rows(12, 100)).unwrap();

        assert!(!stale_segment_dir.join("stale").exists());
        drop(storage);
        let reloaded = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        assert_eq!(reloaded.total_rows(), 12);
        assert_eq!(reloaded.sealed_count(), 2);
    }

    #[test]
    fn missing_catalog_never_adopts_segment_manifests() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            hot_target_rows: 10,
            ..Default::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&make_rows(25, 100)).unwrap();
        storage.checkpoint().unwrap();
        let rows_before = read_ingestion_rows(&storage);
        let descriptors = storage.segments().to_vec();
        fs::remove_file(storage.paths.catalog_path()).unwrap();
        drop(storage);
        let error = NativeStorage::open(config.clone()).err().unwrap();
        assert!(error.to_string().contains("missing catalog"));
        assert!(!config.data_dir.join("catalog.json").exists());
        let actual = descriptors
            .iter()
            .filter(|segment| segment.row_count > 0)
            .flat_map(|segment| {
                SegmentReader::open(&config.data_dir.join(&segment.relative_path))
                    .unwrap()
                    .read_log_rows(None)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, rows_before);
    }

    #[test]
    fn native_storage_reopen_preserves_wal_on_complete_entry_corruption() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        };
        let path = tmp.path().join("wal/pending.wal");
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&make_rows(1, 50)).unwrap();
        storage.checkpoint().unwrap();
        storage.wal.append(&make_rows(1, 100)).unwrap();
        let first_end = fs::metadata(&path).unwrap().len() as usize;
        storage.wal.append(&make_rows(1, 200)).unwrap();
        storage.wal.append(&make_rows(1, 300)).unwrap();
        drop(storage);
        let mut bytes = fs::read(&path).unwrap();
        bytes[first_end + 16] ^= 1;
        fs::write(&path, &bytes).unwrap();
        for _ in 0..2 {
            let error = NativeStorage::open(config.clone()).err().unwrap();
            assert!(error.to_string().contains("pending.wal"));
            assert!(error.to_string().contains(&format!("byte {first_end}")));
            assert_eq!(fs::read(&path).unwrap(), bytes);
            let (catalog, _) = NativeStorageCatalog::open_or_create(&config).unwrap();
            assert_eq!(
                catalog
                    .segments
                    .iter()
                    .map(|segment| segment.row_count)
                    .sum::<u64>(),
                1
            );
        }
    }

    #[test]
    fn native_storage_clears_empty_wal_recovery_tail_before_next_append() {
        for tail in [vec![1, 0, 0], {
            let mut frame = 0u32.to_le_bytes().to_vec();
            frame.extend_from_slice(&8u32.to_le_bytes());
            frame.extend_from_slice(b"LXWL");
            frame.extend_from_slice(&1u32.to_le_bytes());
            frame // Empty, valid payload with a missing checksum.
        }] {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 100,
                compaction_safety_margin_blocks: 2_048,
            };
            drop(NativeStorage::open(config.clone()).unwrap());
            let path = tmp.path().join("wal/pending.wal");
            fs::write(&path, tail).unwrap();
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            assert!(fs::read(&path).unwrap().is_empty());
            let rows = make_rows(3, 100);
            storage.wal.append(&rows).unwrap();
            drop(storage);
            let storage = NativeStorage::open(config.clone()).unwrap();
            assert_eq!(storage.total_rows(), 3);
            let hot_id = storage.catalog.active_hot_segment.unwrap();
            let reader = SegmentReader::open(&storage.segment_path(hot_id)).unwrap();
            assert_eq!(reader.read_log_rows(None).unwrap(), rows);
            drop(storage);
            assert_eq!(NativeStorage::open(config).unwrap().total_rows(), 3);
        }
    }

    #[test]
    fn data_directory_lock_is_exclusive_and_retained_by_compaction() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 5,
            compaction_safety_margin_blocks: 0,
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        assert_eq!(
            NativeStorage::open(config.clone()).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        fs::create_dir(tmp.path().join("child")).unwrap();
        let alias = NativeStorageConfig {
            data_dir: tmp.path().join("child/.."),
            ..config.clone()
        };
        assert_eq!(
            NativeStorage::open(alias).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        storage.write_batch(&make_rows(10, 100)).unwrap();
        storage.checkpoint().unwrap();
        let plan = storage.raw_segment_compaction_plan(1).unwrap();
        assert!(!plan.is_empty());
        drop(storage);
        assert_eq!(
            NativeStorage::open(config.clone()).err().unwrap().kind(),
            io::ErrorKind::WouldBlock
        );
        drop(plan);
        NativeStorage::open(config).expect("directory lock released after compaction plan drops");
    }

    #[test]
    fn checkpoint_replays_multiple_batches_and_continues_both_ingestion_routes() {
        for route in [IngestRoute::Live, IngestRoute::Historical] {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 10,
                compaction_safety_margin_blocks: 0,
            };
            let mut expected = make_rows(3, 100);
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&expected).unwrap();
            storage.checkpoint().unwrap();
            for (count, first) in [(3, 900), (12, 800), (2, 700)] {
                let rows = make_rows(count, first);
                match route {
                    IngestRoute::Live => storage.write_batch(&rows).unwrap(),
                    IngestRoute::Historical => {
                        storage.write_historical_batch(&rows).unwrap();
                    }
                }
                expected.extend(rows);
            }
            assert_eq!(storage.wal.read_all().unwrap().len(), expected.len() - 3);
            assert!(
                RecoveryJournal::load(&storage.paths)
                    .unwrap()
                    .unwrap()
                    .is_active_checkpoint()
            );
            drop(storage); // No explicit final checkpoint: exercise actual WAL recovery.
            for _ in 0..2 {
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                let actual = storage
                    .segments()
                    .iter()
                    .filter(|s| s.row_count > 0)
                    .flat_map(|s| {
                        SegmentReader::open(&storage.segment_path(s.id))
                            .unwrap()
                            .read_log_rows(None)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{route:?}");
                assert!(storage.wal.is_empty().unwrap());
                let rows = make_rows(2, 600);
                match route {
                    IngestRoute::Live => storage.write_batch(&rows).unwrap(),
                    IngestRoute::Historical => {
                        storage.write_historical_batch(&rows).unwrap();
                    }
                }
                expected.extend(rows);
                // Leave another pending epoch to exercise recovered staging state.
            }
        }
    }

    #[test]
    fn last_directory_owner_unlocks_even_with_a_duplicated_descriptor() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            ..NativeStorageConfig::default()
        };
        let storage = NativeStorage::open(config.clone()).unwrap();
        // dup and a transient fork/exec inherit references to the same OS lock.
        // Such a descriptor is not a managed storage or compaction owner.
        let duplicate = storage.directory_lock.0.try_clone().unwrap();
        drop(storage);
        let reopened = NativeStorage::open(config).unwrap();
        drop(duplicate);
        assert_eq!(reopened.total_rows(), 0);
    }

    #[test]
    fn route_switch_and_reorg_retire_previous_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        for (route, first) in [
            (IngestRoute::Live, 100),
            (IngestRoute::Historical, 90),
            (IngestRoute::Live, 110),
            (IngestRoute::Historical, 80),
        ] {
            let rows = make_rows(3, first);
            match route {
                IngestRoute::Live => storage.write_batch(&rows).unwrap(),
                IngestRoute::Historical => {
                    storage.write_historical_batch(&rows).unwrap();
                }
            }
            assert_eq!(storage.wal.read_all().unwrap(), rows);
            assert_eq!(
                RecoveryJournal::load(&storage.paths)
                    .unwrap()
                    .unwrap()
                    .route(),
                route
            );
        }
        assert_eq!(storage.total_rows(), 12);
        storage.mark_non_canonical(B256::ZERO).unwrap();
        assert!(storage.wal.is_empty().unwrap());
        assert!(RecoveryJournal::load(&storage.paths).unwrap().is_none());
        assert!(storage.pending_checkpoint.is_none());
    }

    #[test]
    fn checkpoint_age_is_controlled_without_sleeping() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            ..NativeStorageConfig::default()
        })
        .unwrap();
        storage.write_batch(&make_rows(2, 100)).unwrap();
        assert!(!storage.checkpoint_if_due().unwrap());
        storage.pending_checkpoint.as_mut().unwrap().started_at =
            std::time::Instant::now() - std::time::Duration::from_secs(6);
        assert!(storage.checkpoint_if_due().unwrap());
        assert!(storage.wal.is_empty().unwrap());
        assert!(!storage.checkpoint_if_due().unwrap());
    }

    #[test]
    fn checkpoint_bounds_accumulated_wal_bytes() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 128,
            compaction_safety_margin_blocks: 0,
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        let payload = Bytes::from(vec![0x5a; 512 * 1024]);
        let mut checkpointed = false;
        let mut previous_bytes = 0;
        for batch in 0..9 {
            let mut rows = make_rows(8, 100 + batch * 8);
            for row in &mut rows {
                row.data = payload.clone();
                row.data_len = payload.len() as u32;
            }
            storage.write_batch(&rows).unwrap();
            let bytes = fs::metadata(tmp.path().join("wal/pending.wal"))
                .unwrap()
                .len();
            assert!(bytes <= CHECKPOINT_INGEST_BYTES);
            checkpointed |= bytes < previous_bytes;
            previous_bytes = bytes;
        }
        assert!(checkpointed);
        drop(storage);
        let recovered = NativeStorage::open(config).unwrap();
        assert_eq!(recovered.total_rows(), 72);
        assert!(recovered.wal.is_empty().unwrap());
    }

    #[test]
    fn oversized_caller_batch_is_checkpointed_before_success() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            ..NativeStorageConfig::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        let mut rows = make_rows(1, 100);
        rows[0].data = Bytes::from(vec![0x53; CHECKPOINT_INGEST_BYTES as usize + 1]);
        rows[0].data_len = u32::try_from(rows[0].data.len()).unwrap();
        storage.write_batch(&rows).unwrap();
        assert!(storage.pending_checkpoint.is_none());
        assert!(storage.wal.is_empty().unwrap());
        assert!(!RecoveryJournal::path(&storage.paths).exists());
        drop(storage);
        let recovered = NativeStorage::open(config).unwrap();
        let actual = SegmentReader::open(&recovered.hot_partition_meta().path)
            .unwrap()
            .read_log_rows(None)
            .unwrap();
        assert_eq!(actual, rows);
    }

    #[test]
    fn completed_checkpoint_with_missing_rows_preserves_its_journal() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            ..NativeStorageConfig::default()
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&make_rows(3, 100)).unwrap();
        let mut pending = storage.pending_checkpoint.take().unwrap();
        pending
            .journal
            .complete_checkpoint(pending.rows, pending.checksum.finalize())
            .unwrap();
        pending.journal.persist(&storage.paths).unwrap();
        // Model loss of the committed data/metadata after WAL retirement. The
        // complete checkpoint marker must distinguish this from preparation
        // interrupted before its first WAL append.
        let start = pending.journal.start.clone();
        ColumnFile::write_batch(&storage.segment_path(start.id), &[]).unwrap();
        persist_segment_manifest(&storage.paths, &start).unwrap();
        storage.catalog.segments = vec![start];
        storage.persist_checkpoint_catalog().unwrap();
        storage.wal.truncate().unwrap();
        let journal_path = RecoveryJournal::path(&storage.paths);
        let journal_bytes = fs::read(&journal_path).unwrap();
        drop(storage);
        for _ in 0..2 {
            let error = NativeStorage::open(config.clone()).err().unwrap();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(fs::read(&journal_path).unwrap(), journal_bytes);
        }
    }

    #[test]
    fn historical_checkpoint_recovers_after_each_io_failure_with_prior_wal_batches() {
        fn setup() -> (TempDir, NativeStorageConfig, NativeStorage) {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 10,
                compaction_safety_margin_blocks: 0,
            };
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&make_rows(3, 100)).unwrap();
            storage.checkpoint().unwrap();
            storage.write_historical_batch(&make_rows(3, 900)).unwrap();
            storage.write_historical_batch(&make_rows(2, 800)).unwrap();
            (tmp, config, storage)
        }
        let pending = make_rows(13, 700);
        let (_tmp, _config, mut storage) = setup();
        durability::inject_failure(usize::MAX);
        storage.write_historical_batch(&pending).unwrap();
        storage.checkpoint().unwrap();
        let events = durability::take_events();
        for failure in 0..events.len() {
            let (_tmp, config, mut storage) = setup();
            durability::inject_failure(failure);
            let result = storage
                .write_historical_batch(&pending)
                .and_then(|_| storage.checkpoint());
            let observed = durability::take_events();
            assert!(result.is_err(), "{failure}: {observed:?}");
            assert!(storage.write_batch(&pending).is_err());
            let wal = storage.wal.read_all().unwrap();
            let mut expected = make_rows(3, 100);
            if wal.is_empty() {
                expected.extend(make_rows(3, 900));
                expected.extend(make_rows(2, 800));
                expected.extend(pending.clone());
            } else {
                expected.extend(wal);
            }
            drop(storage);
            for restart in 0..2 {
                let storage = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                    panic!("{failure}, restart {restart}, {observed:?}: {error}")
                });
                let actual = storage
                    .segments()
                    .iter()
                    .filter(|s| s.row_count > 0)
                    .flat_map(|s| {
                        SegmentReader::open(&storage.segment_path(s.id))
                            .unwrap()
                            .read_log_rows(None)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected, "{failure}, restart {restart}");
                assert!(storage.wal.is_empty().unwrap());
            }
        }
    }

    #[test]
    fn compacted_checkpoint_ignores_partially_removed_raw_columns() {
        for journal_remains in [false, true] {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 10,
                compaction_safety_margin_blocks: 0,
            };
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            let rows = make_rows(3, 100);
            storage.write_historical_batch(&rows).unwrap();
            let id = storage.active_historical_segment_id().unwrap();
            let address_path = storage.segment_path(id).join("address.col");
            let raw_dir = TempDir::new().unwrap();
            ColumnFile::write_batch(raw_dir.path(), &rows).unwrap();
            let raw_address = fs::read(raw_dir.path().join("address.col")).unwrap();
            storage.finalize_historical_segment().unwrap();
            if journal_remains {
                let mut pending = storage.pending_checkpoint.take().unwrap();
                pending
                    .journal
                    .complete_checkpoint(pending.rows, pending.checksum.finalize())
                    .unwrap();
                pending.journal.persist(&storage.paths).unwrap();
                storage.persist_checkpoint_catalog().unwrap();
                storage.wal.truncate().unwrap();
            } else {
                storage.checkpoint().unwrap();
            }
            // Deletions may reach disk in a different order: a raw address
            // file can survive after other raw columns have disappeared.
            fs::write(&address_path, raw_address).unwrap();
            drop(storage);
            for _ in 0..2 {
                let recovered = NativeStorage::open(config.clone()).unwrap();
                let actual = SegmentReader::open(&recovered.segment_path(id))
                    .unwrap()
                    .read_log_rows(None)
                    .unwrap();
                assert_eq!(actual, rows);
            }
        }
    }

    #[test]
    fn journaled_replay_allows_identical_new_batches() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        };
        let rows = make_rows(2, 100);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&rows).unwrap();
        storage.begin_wal_batch(&rows).unwrap();
        drop(storage);
        let recovered = NativeStorage::open(config).unwrap();
        assert_eq!(recovered.total_rows(), 4);
        let reader = SegmentReader::open(
            &recovered.segment_path(recovered.catalog.active_hot_segment.unwrap()),
        )
        .unwrap();
        assert_eq!(
            reader.read_log_rows(None).unwrap(),
            [rows.as_slice(), rows.as_slice()].concat()
        );
    }

    #[test]
    fn legacy_committed_overlap_requires_explicit_recovery() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        };
        let rows = make_rows(2, 100);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&rows).unwrap();
        storage.checkpoint().unwrap();
        storage.wal.append(&rows).unwrap();
        let wal_path = tmp.path().join("wal/pending.wal");
        let bytes = fs::read(&wal_path).unwrap();
        drop(storage);
        let error = NativeStorage::open(config).err().unwrap();
        assert!(error.to_string().contains("legacy WAL overlaps"));
        assert_eq!(fs::read(wal_path).unwrap(), bytes);
        assert!(!tmp.path().join("wal/recovery.json").exists());
    }

    #[test]
    fn journal_recovery_preserves_prior_noncanonical_rows_during_partial_column_rebuild() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 2_048,
        };
        let initial = make_rows(3, 100);
        let pending = make_rows(2, 200);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&initial).unwrap();
        storage.mark_non_canonical(initial[0].block_hash).unwrap();
        let hot = storage.catalog.active_hot_segment.unwrap();
        let dir = storage.segment_path(hot);
        storage.begin_wal_batch(&pending).unwrap();
        append_rows(&dir, 3, &pending).unwrap();
        // Restore one column to the old prefix, modelling an interrupted append.
        let data = fs::read(dir.join("address.col")).unwrap();
        let mut prefix = data[..ColumnFileHeader::SIZE + 3 * 20].to_vec();
        prefix[8..16].copy_from_slice(&3u64.to_le_bytes());
        fs::write(dir.join("address.col"), prefix).unwrap();
        drop(storage);
        durability::inject_failure(0);
        let interrupted = NativeStorage::open(config.clone());
        let events = durability::take_events();
        assert!(interrupted.is_err(), "{events:?}");
        let recovered = NativeStorage::open(config).unwrap();
        let reader = SegmentReader::open(&dir).unwrap();
        assert_eq!(
            reader.read_log_rows(None).unwrap(),
            [initial.as_slice(), pending.as_slice()].concat()
        );
        let flags = reader.read_canonical().unwrap();
        assert!(!flags.is_present(0));
        assert!((1..5).all(|row| flags.is_present(row)));
        assert_eq!(recovered.total_rows(), 5);
    }

    #[test]
    fn journal_recovery_rejects_damage_and_missing_wal_before_commit_completion() {
        for damage in 0..4 {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 20,
                compaction_safety_margin_blocks: 2_048,
            };
            let pending = make_rows(3, 200);
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.begin_wal_batch(&pending).unwrap();
            if damage == 3 {
                storage.commit_rows_to_segments(&pending[..1]).unwrap();
            }
            let journal_path = RecoveryJournal::path(&storage.paths);
            let wal_path = tmp.path().join("wal/pending.wal");
            drop(storage);
            match damage {
                0 => fs::write(&journal_path, b"{").unwrap(),
                1 => {
                    let mut json: serde_json::Value =
                        serde_json::from_slice(&fs::read(&journal_path).unwrap()).unwrap();
                    json["journal"]["row_count"] = serde_json::json!(999);
                    fs::write(&journal_path, serde_json::to_vec(&json).unwrap()).unwrap();
                }
                2 => fs::write(&journal_path, vec![b' '; 16 * 1024 + 1]).unwrap(),
                3 => fs::write(&wal_path, []).unwrap(),
                _ => unreachable!(),
            }
            let journal = fs::read(&journal_path).unwrap();
            let wal = fs::read(&wal_path).unwrap();
            assert!(NativeStorage::open(config).is_err(), "damage {damage}");
            assert_eq!(fs::read(journal_path).unwrap(), journal);
            assert_eq!(fs::read(wal_path).unwrap(), wal);
        }
    }

    #[test]
    fn journal_replay_excludes_preexisting_historical_segments() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        };
        let initial = make_rows(3, 100);
        let historical = make_rows(12, 10);
        let pending = make_rows(25, 200);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&initial).unwrap();
        storage.write_historical_batch(&historical).unwrap();
        storage.begin_wal_batch(&pending).unwrap();
        storage.commit_rows_to_segments(&pending[..12]).unwrap();
        drop(storage);
        let recovered = NativeStorage::open(config).unwrap();
        let mut actual = recovered
            .segments()
            .iter()
            .filter(|s| s.row_count > 0)
            .flat_map(|segment| {
                SegmentReader::open(&recovered.segment_path(segment.id))
                    .unwrap()
                    .read_log_rows(None)
                    .unwrap()
            })
            .collect::<Vec<_>>();
        let mut expected = [initial, historical, pending].concat();
        actual.sort_by_key(|row| (row.block_number, row.log_index));
        expected.sort_by_key(|row| (row.block_number, row.log_index));
        assert_eq!(actual, expected);
    }

    #[test]
    fn journal_replay_rejects_mismatched_payload_and_committed_prefix() {
        for damage in 0..4 {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 20,
                compaction_safety_margin_blocks: 2_048,
            };
            let pending = make_rows(3, 200);
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.begin_wal_batch(&pending).unwrap();
            match damage {
                0 | 1 => {
                    storage.wal.truncate().unwrap();
                    let replacement = make_rows(if damage == 0 { 2 } else { 3 }, 300);
                    storage.wal.append(&replacement).unwrap();
                }
                2 => storage.commit_rows_to_segments(&make_rows(1, 400)).unwrap(),
                3 => {
                    // Missing WAL cannot explain physical rows outside the manifest.
                    let hot = storage.catalog.active_hot_segment.unwrap();
                    append_rows(&storage.segment_path(hot), 0, &pending).unwrap();
                    storage.wal.truncate().unwrap();
                }
                _ => unreachable!(),
            }
            let journal_path = RecoveryJournal::path(&storage.paths);
            let wal_path = tmp.path().join("wal/pending.wal");
            let journal = fs::read(&journal_path).unwrap();
            let wal = fs::read(&wal_path).unwrap();
            drop(storage);
            assert!(NativeStorage::open(config).is_err(), "damage {damage}");
            assert_eq!(fs::read(journal_path).unwrap(), journal);
            assert_eq!(fs::read(wal_path).unwrap(), wal);
        }
    }

    #[test]
    fn journal_recovers_after_process_exit_and_releases_directory_lock() {
        const CHILD_ROOT: &str = "LOGEX_JOURNAL_TEST_CHILD_ROOT";
        const CHILD_MODE: &str = "LOGEX_JOURNAL_TEST_CHILD_MODE";
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let config = NativeStorageConfig {
                data_dir: PathBuf::from(root),
                hot_target_rows: 5,
                compaction_safety_margin_blocks: 2_048,
            };
            if std::env::var(CHILD_MODE).unwrap() == "locked" {
                assert_eq!(
                    NativeStorage::open(config).err().unwrap().kind(),
                    io::ErrorKind::WouldBlock
                );
                return;
            }
            let mut storage = NativeStorage::open(config).unwrap();
            let pending = make_rows(6, 200);
            storage.begin_wal_batch(&pending).unwrap();
            storage.commit_rows_to_segments(&pending[..4]).unwrap();
            // No Rust destructors run: the OS must release the directory lock.
            std::process::exit(0);
        }
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 5,
            compaction_safety_margin_blocks: 2_048,
        };
        let run_child = |mode: &str| {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["native::storage::tests::journal_recovers_after_process_exit_and_releases_directory_lock", "--exact", "--nocapture"])
                .env(CHILD_ROOT, tmp.path()).env(CHILD_MODE, mode).output().unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&make_rows(3, 100)).unwrap();
        run_child("locked");
        drop(storage);
        run_child("commit");
        for _ in 0..2 {
            let recovered = NativeStorage::open(config.clone()).unwrap();
            let actual = recovered
                .segments()
                .iter()
                .filter(|s| s.row_count > 0)
                .flat_map(|segment| {
                    SegmentReader::open(&recovered.segment_path(segment.id))
                        .unwrap()
                        .read_log_rows(None)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            assert_eq!(actual, [make_rows(3, 100), make_rows(6, 200)].concat());
        }
    }

    #[test]
    fn journal_replays_when_first_batch_left_only_some_column_files() {
        for missing in ["address.col", "topic1.null", "canonical.bitmap", "data.col"] {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 10,
                compaction_safety_margin_blocks: 2_048,
            };
            let pending = make_rows(3, 200);
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.begin_wal_batch(&pending).unwrap();
            let dir = storage.segment_path(storage.catalog.active_hot_segment.unwrap());
            // The first parallel column write did not finish all its files. The
            // manifest still has zero rows; the WAL contains the entire batch.
            ColumnFile::write_batch(&dir, &pending).unwrap();
            fs::remove_file(dir.join(missing)).unwrap();
            drop(storage);
            for restart in 0..2 {
                let recovered = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                    panic!("missing {missing}, restart {restart}: {error}")
                });
                assert_eq!(recovered.total_rows(), pending.len() as u64);
                assert_eq!(
                    SegmentReader::open(&dir)
                        .unwrap()
                        .read_log_rows(None)
                        .unwrap(),
                    pending
                );
            }
        }
    }

    #[test]
    fn journaled_commit_recovers_after_every_main_thread_io_checkpoint() {
        fn setup() -> (TempDir, NativeStorageConfig, NativeStorage) {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 5,
                compaction_safety_margin_blocks: 2_048,
            };
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&make_rows(3, 100)).unwrap();
            storage.checkpoint().unwrap();
            (tmp, config, storage)
        }
        let pending = make_rows(6, 200);
        let (_tmp, _config, mut storage) = setup();
        durability::inject_failure(usize::MAX);
        storage.write_batch(&pending).unwrap();
        storage.checkpoint().unwrap();
        let events = durability::take_events();
        assert!(events.iter().any(|(op, _)| *op == "rows_committed"));
        assert!(events.iter().any(|(op, _)| *op == "truncate_wal"));
        let clear = events
            .iter()
            .position(|(op, _)| *op == "truncate_wal")
            .unwrap();
        let remove = events
            .iter()
            .position(|(op, _)| *op == "remove_file")
            .unwrap();
        assert!(clear < remove);
        for failure in 0..events.len() {
            let (_tmp, config, mut storage) = setup();
            durability::inject_failure(failure);
            let result = storage
                .write_batch(&pending)
                .and_then(|()| storage.checkpoint());
            let observed = durability::take_events();
            assert!(result.is_err(), "checkpoint {failure} was not exercised");
            assert!(storage.write_batch(&pending).is_err());
            assert!(storage.write_historical_batch(&pending).is_err());
            assert!(storage.mark_non_canonical(B256::ZERO).is_err());
            assert!(
                storage
                    .record_chain_anchors(ChainAnchors::default())
                    .is_err()
            );
            assert!(storage.segment_compaction_plan(1).is_err());
            let expected =
                if storage.wal.read_all().unwrap().is_empty() && storage.total_rows() != 9 {
                    3
                } else {
                    9
                };
            drop(storage);
            for restart in 0..2 {
                let recovered = NativeStorage::open(config.clone()).unwrap_or_else(|error| {
                    panic!("checkpoint {failure}, restart {restart}, events {observed:?}: {error}")
                });
                assert_eq!(recovered.total_rows(), expected, "checkpoint {failure}");
                let actual = recovered
                    .segments()
                    .iter()
                    .flat_map(|segment| {
                        if segment.row_count == 0 {
                            return Vec::new();
                        }
                        SegmentReader::open(&recovered.segment_path(segment.id))
                            .unwrap()
                            .read_log_rows(None)
                            .unwrap()
                    })
                    .collect::<Vec<_>>();
                let mut rows = make_rows(3, 100);
                if expected == 9 {
                    rows.extend(pending.clone());
                }
                assert_eq!(actual, rows, "checkpoint {failure}");
                assert!(recovered.wal.read_all().unwrap().is_empty());
                assert!(!RecoveryJournal::path(&recovered.paths).exists());
            }
        }
    }

    #[test]
    fn native_storage_wal_replay_skips_committed_rows_across_rotation() {
        let mut recovered_counts = Vec::new();
        for applied in [1, 7, 12, 25] {
            let tmp = TempDir::new().unwrap();
            let config = NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 10,
                compaction_safety_margin_blocks: 2_048,
            };
            let initial = make_rows(3, 100);
            let pending = make_rows(25, 200);
            {
                let mut storage = NativeStorage::open(config.clone()).unwrap();
                storage.write_batch(&initial).unwrap();
                storage.begin_wal_batch(&pending).unwrap();
                storage
                    .commit_rows_to_segments(&pending[..applied])
                    .unwrap();
                // Simulate exit after some/all segment commits, before WAL clear.
            }
            for restart in 0..2 {
                let storage = NativeStorage::open(NativeStorageConfig {
                    hot_target_rows: 6,
                    ..config.clone()
                })
                .unwrap();
                recovered_counts.push((applied, restart, storage.total_rows()));
                if storage.total_rows() == 28 {
                    let actual = storage
                        .segments()
                        .iter()
                        .filter(|segment| segment.row_count > 0)
                        .flat_map(|segment| {
                            SegmentReader::open(&storage.segment_path(segment.id))
                                .unwrap()
                                .read_log_rows(None)
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(actual, [initial.as_slice(), pending.as_slice()].concat());
                }
            }
        }
        assert!(
            recovered_counts.iter().all(|(_, _, count)| *count == 28),
            "expected 28 rows after every crash/restart: {recovered_counts:?}"
        );
    }

    #[test]
    fn legacy_wal_bytes_cannot_distinguish_new_rows_from_committed_replay() {
        fn snapshot(root: &Path, dir: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    snapshot(root, &path, files);
                } else {
                    files.insert(
                        path.strip_prefix(root).unwrap().to_path_buf(),
                        fs::read(path).unwrap(),
                    );
                }
            }
        }
        let new_batch = TempDir::new().unwrap();
        let committed_batch = TempDir::new().unwrap();
        let rows = make_rows(1, 100);
        for (dir, already_committed) in [(new_batch.path(), false), (committed_batch.path(), true)]
        {
            let config = NativeStorageConfig {
                data_dir: dir.to_path_buf(),
                hot_target_rows: 10,
                compaction_safety_margin_blocks: 2_048,
            };
            let mut storage = NativeStorage::open(config).unwrap();
            if !already_committed {
                storage.write_batch(&rows).unwrap();
                storage.checkpoint().unwrap();
            }
            storage.wal.append(&rows).unwrap();
            if already_committed {
                storage.commit_rows_to_segments(&rows).unwrap();
            }
        }
        let mut new_files = BTreeMap::new();
        let mut committed_files = BTreeMap::new();
        snapshot(new_batch.path(), new_batch.path(), &mut new_files);
        snapshot(
            committed_batch.path(),
            committed_batch.path(),
            &mut committed_files,
        );
        // The first history needs two rows after recovery; the second needs one.
        // Equal on-disk bytes prove that row matching alone cannot decide safely.
        assert_eq!(new_files, committed_files);
    }

    #[test]
    fn native_storage_replay_wal_is_idempotent_after_partial_hot_commit() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 2_048,
        };
        let initial_rows = make_rows(5, 100);
        let wal_rows = make_rows(3, 200);

        {
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&initial_rows).unwrap();

            storage.wal.append(&wal_rows).unwrap();
            let hot_id = storage.catalog.active_hot_segment.unwrap();
            let descriptor = storage
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == hot_id)
                .cloned()
                .unwrap();
            append_rows(
                &storage.paths.segment_dir(hot_id),
                descriptor.row_count,
                &wal_rows,
            )
            .unwrap();
            ColumnFile::write_canonical_bitmap(
                &storage.paths.segment_dir(hot_id),
                descriptor.row_count,
            )
            .unwrap();
        }

        let recovered = NativeStorage::open(config).unwrap();
        assert_eq!(recovered.total_rows(), 8);
        assert_eq!(recovered.hot_partition_meta().row_count, 8);

        let hot = recovered.hot_partition_meta();
        let reader = SegmentReader::open(&recovered.segment_path(hot.id)).unwrap();
        let mut expected = initial_rows;
        expected.extend(wal_rows);
        assert_eq!(reader.read_log_rows(None).unwrap(), expected);
        assert!(recovered.wal.read_all().unwrap().is_empty());
    }

    #[test]
    fn native_storage_rebuilds_partial_hot_segment_before_wal_replay() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 2_048,
        };
        let initial_rows = make_rows(5, 100);
        let wal_rows = make_rows(3, 200);

        {
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&initial_rows).unwrap();

            storage.wal.append(&wal_rows).unwrap();
            let hot_id = storage.catalog.active_hot_segment.unwrap();
            let descriptor = storage
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == hot_id)
                .cloned()
                .unwrap();
            let segment_dir = storage.paths.segment_dir(hot_id);
            let data_col = fs::read(segment_dir.join("data.col")).unwrap();
            let topic3_col = fs::read(segment_dir.join("topic3.col")).unwrap();
            let topic3_null = fs::read(segment_dir.join("topic3.null")).unwrap();
            let canonical_bitmap = fs::read(segment_dir.join("canonical.bitmap")).unwrap();

            append_rows(&segment_dir, descriptor.row_count, &wal_rows).unwrap();

            fs::write(segment_dir.join("data.col"), data_col).unwrap();
            fs::write(segment_dir.join("topic3.col"), topic3_col).unwrap();
            fs::write(segment_dir.join("topic3.null"), topic3_null).unwrap();
            fs::write(segment_dir.join("canonical.bitmap"), canonical_bitmap).unwrap();
        }

        let recovered = NativeStorage::open(config).unwrap();
        assert_eq!(recovered.total_rows(), 8);
        assert_eq!(recovered.hot_partition_meta().row_count, 8);

        let hot = recovered.hot_partition_meta();
        let reader = SegmentReader::open(&recovered.segment_path(hot.id)).unwrap();
        let mut expected = initial_rows;
        expected.extend(wal_rows);
        assert_eq!(reader.read_log_rows(None).unwrap(), expected);
        assert!(recovered.wal.read_all().unwrap().is_empty());
    }

    #[test]
    fn native_storage_rebuilds_partial_active_historical_segment_on_open() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 2_048,
        };
        let committed_rows = make_rows(4, 100);
        let uncommitted_rows = make_rows(3, 90);

        {
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_historical_batch(&committed_rows).unwrap();
            let segment_id = storage.active_historical_segment_id().unwrap();
            let descriptor = storage
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == segment_id)
                .cloned()
                .unwrap();

            append_compacted_rows(
                &storage.paths.segment_dir(segment_id),
                descriptor.row_count,
                &uncommitted_rows,
                Publication::Ordered,
                None,
            )
            .unwrap();
        }

        let recovered = NativeStorage::open(config).unwrap();
        let segment_id = recovered.active_historical_segment_id().unwrap();
        let descriptor = recovered
            .segments()
            .iter()
            .find(|segment| segment.id == segment_id)
            .unwrap();
        assert_eq!(descriptor.row_count, committed_rows.len() as u64);

        let reader = SegmentReader::open(&recovered.segment_path(segment_id)).unwrap();
        assert_eq!(reader.read_log_rows(None).unwrap(), committed_rows);
        assert_eq!(reader.read_canonical_len().unwrap(), 4);
    }

    #[test]
    fn committed_canonical_bitmap_damage_does_not_recanonicalize_rows() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 2_048,
        };
        let rows = make_rows(3, 100);
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        storage.write_batch(&rows).unwrap();
        storage.mark_non_canonical(rows[0].block_hash).unwrap();
        let hot = storage.catalog.active_hot_segment.unwrap();
        let path = storage.segment_path(hot).join("canonical.bitmap");
        drop(storage);
        let mut short = NullBitmap::new();
        short.push(false);
        let mut bytes = Vec::new();
        short.write_to(&mut bytes).unwrap();
        fs::write(&path, &bytes).unwrap();
        assert!(NativeStorage::open(config).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn native_storage_rejects_missing_canonical_bits_after_committed_rows() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 2_048,
        };
        let initial_rows = make_rows(5, 100);
        let applied_rows = make_rows(3, 200);

        {
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&initial_rows).unwrap();
            storage.checkpoint().unwrap();

            let hot_id = storage.catalog.active_hot_segment.unwrap();
            let segment_index = storage
                .catalog
                .segments
                .iter()
                .position(|segment| segment.id == hot_id)
                .unwrap();
            let descriptor = storage.catalog.segments[segment_index].clone();
            let segment_dir = storage.paths.segment_dir(hot_id);
            append_rows(&segment_dir, descriptor.row_count, &applied_rows).unwrap();

            {
                let descriptor = &mut storage.catalog.segments[segment_index];
                apply_rows_to_descriptor(descriptor, &applied_rows);
                persist_segment_manifest(&storage.paths, descriptor).unwrap();
            }
            storage.persist_catalog().unwrap();
            ColumnFile::write_canonical_bitmap(&segment_dir, descriptor.row_count).unwrap();
            storage.wal.truncate().unwrap();
        }

        let error = NativeStorage::open(config).err().unwrap();
        assert!(
            error
                .to_string()
                .contains("missing committed raw segment rows")
        );
    }

    #[test]
    fn native_storage_reopens_after_reverse_historical_append() {
        let tmp = TempDir::new().unwrap();
        {
            let mut storage = NativeStorage::open(NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 100,
                compaction_safety_margin_blocks: 2_048,
            })
            .unwrap();

            storage.write_batch(&make_rows(5, 200)).unwrap();
            storage.write_batch(&make_rows(5, 190)).unwrap();

            let meta = storage.hot_partition_meta();
            assert_eq!(meta.min_block, 190);
            assert_eq!(meta.max_block, 200);
            assert_eq!(meta.row_count, 10);
        }

        let reloaded = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        let meta = reloaded.hot_partition_meta();
        assert_eq!(meta.min_block, 190);
        assert_eq!(meta.max_block, 200);
        assert_eq!(meta.row_count, 10);
    }

    #[test]
    fn native_storage_persists_sync_state() {
        let tmp = TempDir::new().unwrap();
        {
            let mut storage = NativeStorage::open(NativeStorageConfig {
                data_dir: tmp.path().to_path_buf(),
                hot_target_rows: 100,
                compaction_safety_margin_blocks: 2_048,
            })
            .unwrap();
            storage
                .record_sync_head(123, B256::repeat_byte(0xAA), 999)
                .unwrap();
            storage
                .record_canonical_state(
                    &Header {
                        number: 123,
                        timestamp: 999,
                        ..Default::default()
                    },
                    &[],
                )
                .unwrap();
        }

        let reloaded = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        assert_eq!(
            reloaded.sync_head().map(|head| head.block_number),
            Some(123)
        );
    }

    #[test]
    fn rewind_canonical_state_rewinds_sync_head_and_indexed_anchor() {
        let tmp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        storage
            .record_chain_anchors(ChainAnchors {
                indexed_head: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0xAA),
                    beacon_slot: 1,
                    block_number: 101,
                    block_hash: second.hash_slow(),
                    receipts_root: B256::repeat_byte(0xBB),
                }),
                finalized_head: None,
                optimistic_head: None,
            })
            .unwrap();
        storage
            .record_canonical_state(&second, &[first.clone(), second.clone()])
            .unwrap();

        storage
            .rewind_canonical_state(
                std::slice::from_ref(&first),
                Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0xCC),
                    beacon_slot: 2,
                    block_number: 100,
                    block_hash: first.hash_slow(),
                    receipts_root: B256::repeat_byte(0xDD),
                }),
            )
            .unwrap();

        assert_eq!(storage.sync_head().map(|head| head.block_number), Some(100));
        assert_eq!(storage.recent_headers(), std::slice::from_ref(&first));
        assert_eq!(
            storage
                .chain_anchors()
                .indexed_head
                .map(|anchor| anchor.block_number),
            Some(100)
        );
    }

    #[test]
    fn startup_integrity_rejects_broken_recent_header_window() {
        let tmp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let bad_second = header(102, B256::repeat_byte(0xFF), 0x02);
        let bad_state = StorageState {
            sync_head: Some(SyncHead {
                block_number: bad_second.number(),
                block_hash: bad_second.hash_slow(),
                timestamp: bad_second.timestamp(),
            }),
            recent_headers: vec![first, bad_second],
            ..Default::default()
        };

        let config = NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            ..Default::default()
        };
        let (mut catalog, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
        catalog.state = bad_state;
        catalog.persist(&paths).unwrap();

        let err = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        })
        .err()
        .expect("broken recent-header window should fail integrity checks");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn startup_integrity_rejects_truncated_raw_column_body() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 5,
            compaction_safety_margin_blocks: 2_048,
        };
        let sealed_id;

        {
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&make_rows(6, 100)).unwrap();
            storage.checkpoint().unwrap();
            let sealed = storage
                .segments()
                .iter()
                .find(|segment| segment.kind == SegmentKind::Sealed)
                .cloned()
                .unwrap();
            sealed_id = sealed.id;

            let path = storage.segment_path(sealed.id).join("topic2.col");
            let mut data = fs::read(&path).unwrap();
            data.truncate(data.len() - 32);
            fs::write(path, data).unwrap();
        }

        let err = NativeStorage::open(config)
            .err()
            .expect("truncated raw column should fail startup integrity");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let message = err.to_string();
        assert!(message.contains(&format!("segment {sealed_id} raw column topic2.col")));
        assert!(message.contains("length mismatch"));
    }

    #[test]
    fn compaction_error_identifies_truncated_raw_column() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 5,
            compaction_safety_margin_blocks: 100,
        })
        .unwrap();

        storage.write_batch(&make_rows(6, 100)).unwrap();
        storage.checkpoint().unwrap();
        let sealed = storage
            .segments()
            .iter()
            .find(|segment| segment.kind == SegmentKind::Sealed)
            .cloned()
            .unwrap();
        storage
            .record_sync_head(
                sealed.max_block.unwrap() + 200,
                B256::repeat_byte(0xAA),
                999,
            )
            .unwrap();

        let path = storage.segment_path(sealed.id).join("topic2.col");
        let mut data = fs::read(&path).unwrap();
        data.truncate(data.len() - 32);
        fs::write(path, data).unwrap();

        let err = storage
            .segment_compaction_plan(1)
            .unwrap()
            .compact()
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let message = err.to_string();
        assert!(message.contains(&format!("failed to compact storage segment {}", sealed.id)));
        assert!(message.contains("raw column topic2.col"));
        assert!(message.contains("length mismatch"));
    }

    #[test]
    fn historical_floor_only_moves_toward_older_blocks() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        let anchor = header(200, B256::repeat_byte(0xAA), 0x01);
        let older = header(199, B256::repeat_byte(0xBB), 0x02);
        let newer = header(201, B256::repeat_byte(0xCC), 0x03);

        storage.record_historical_floor(&anchor).unwrap();
        storage.record_historical_floor(&newer).unwrap();
        assert_eq!(
            storage.historical_floor().map(|marker| marker.block_number),
            Some(200)
        );
        assert_eq!(
            storage
                .historical_anchor()
                .map(|marker| marker.block_number),
            Some(200)
        );

        storage.record_historical_floor(&older).unwrap();
        assert_eq!(
            storage.historical_floor().map(|marker| marker.block_number),
            Some(199)
        );
        assert_eq!(
            storage
                .historical_anchor()
                .map(|marker| marker.block_number),
            Some(200)
        );

        drop(storage);
        let reloaded = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        assert_eq!(
            reloaded
                .historical_floor()
                .map(|marker| marker.block_number),
            Some(199)
        );
        assert_eq!(
            reloaded
                .historical_anchor()
                .map(|marker| marker.block_number),
            Some(200)
        );
    }

    #[test]
    fn compaction_waits_until_segment_is_safely_behind_head() {
        let tmp = TempDir::new().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 100,
        })
        .unwrap();

        storage.write_batch(&make_rows(12, 100)).unwrap();
        let sealed = storage
            .segments()
            .iter()
            .find(|segment| segment.kind == SegmentKind::Sealed)
            .cloned()
            .unwrap();
        let sealed_path = storage.segment_path(sealed.id);

        assert_eq!(storage.compact_eligible_segments().unwrap(), 0);
        assert!(sealed_path.join("address.col").exists());
        assert!(!sealed_path.join("columns/address.pages").exists());
        assert_eq!(storage.compaction_backlog_count().unwrap(), 0);

        storage
            .record_sync_head(
                sealed.max_block.unwrap() + 200,
                B256::repeat_byte(0xAA),
                999,
            )
            .unwrap();
        assert_eq!(storage.compaction_backlog_count().unwrap(), 1);
        let plan = storage.segment_compaction_plan(10).unwrap();
        assert_eq!(plan.len(), 1);
        assert_eq!(plan.compact().unwrap(), 1);
        assert!(!sealed_path.join("address.col").exists());
        assert!(sealed_path.join("columns/address.pages").exists());
        assert_eq!(storage.compaction_backlog_count().unwrap(), 0);
        assert_eq!(storage.segment_compaction_plan(10).unwrap().len(), 0);
        assert_eq!(storage.compact_eligible_segments().unwrap(), 0);
    }
}
