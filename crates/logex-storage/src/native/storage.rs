use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::thread;

use alloy_consensus::{BlockHeader, Header};
use alloy_primitives::B256;
use logex_types::{ChainAnchors, ExecutionAnchor, ExecutionBlockMarker, PartitionMeta};
use serde::{Deserialize, Serialize};

use crate::SegmentReader;
use crate::state::SyncHead;
use crate::wal::WriteAheadLog;

use super::catalog::{
    NativeStorageCatalog, NativeStorageConfig, SegmentDescriptor, SegmentKind, SegmentManifest,
    StorageCatalogPaths,
};
use super::segment::{
    append_rows, apply_ordered_rows_to_descriptor, apply_rows_to_descriptor, compact_segment,
    persist_segment_manifest, persist_segment_manifest_with_columns,
    segment_uses_current_compaction_profile, write_compacted_rows,
};

const STORAGE_STATE_FILE: &str = "storage_state.json";

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StorageState {
    #[serde(default)]
    sync_head: Option<SyncHead>,
    #[serde(default)]
    recent_headers: Vec<Header>,
    #[serde(default)]
    historical_floor_header: Option<Header>,
    #[serde(default)]
    historical_anchor_header: Option<Header>,
}

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

#[derive(Debug, Clone)]
pub struct SegmentCompactionTask {
    paths: StorageCatalogPaths,
    descriptor: SegmentDescriptor,
}

impl SegmentCompactionTask {
    pub fn segment_id(&self) -> u64 {
        self.descriptor.id
    }

    pub fn compact(&self) -> std::io::Result<()> {
        compact_segment(&self.paths, &self.descriptor)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SegmentCompactionPlan {
    tasks: Vec<SegmentCompactionTask>,
}

impl SegmentCompactionPlan {
    fn new(paths: StorageCatalogPaths, descriptors: Vec<SegmentDescriptor>) -> Self {
        let tasks = descriptors
            .into_iter()
            .map(|descriptor| SegmentCompactionTask {
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

pub struct NativeStorage {
    config: NativeStorageConfig,
    paths: StorageCatalogPaths,
    catalog: NativeStorageCatalog,
    wal: WriteAheadLog,
    state: StorageState,
}

impl NativeStorage {
    pub fn open(config: NativeStorageConfig) -> std::io::Result<Self> {
        let (catalog, paths) = NativeStorageCatalog::open_or_create(&config)?;
        let wal = WriteAheadLog::open(config.data_dir.join("wal").join("pending.wal"))?;
        let state = load_state(&paths)?;

        let mut storage = Self {
            config,
            paths,
            catalog,
            wal,
            state,
        };

        storage.repair_catalog_from_manifests()?;
        storage.ensure_active_hot_segment()?;
        storage.replay_wal()?;
        storage.verify_integrity()?;
        Ok(storage)
    }

    pub fn data_dir(&self) -> &Path {
        self.paths.root()
    }

    pub fn sync_head(&self) -> Option<SyncHead> {
        self.state.sync_head
    }

    pub fn recent_headers(&self) -> &[Header] {
        &self.state.recent_headers
    }

    pub fn historical_floor_header(&self) -> Option<&Header> {
        self.state.historical_floor_header.as_ref()
    }

    pub fn historical_anchor_header(&self) -> Option<&Header> {
        self.state.historical_anchor_header.as_ref()
    }

    pub fn historical_floor(&self) -> Option<ExecutionBlockMarker> {
        self.state
            .historical_floor_header
            .as_ref()
            .map(execution_marker_from_header)
    }

    pub fn historical_anchor(&self) -> Option<ExecutionBlockMarker> {
        self.state
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
        let next = SyncHead {
            block_number,
            block_hash,
            timestamp,
        };
        if self.state.sync_head == Some(next) {
            return Ok(());
        }

        self.state.sync_head = Some(next);
        self.persist_state()
    }

    pub fn record_canonical_state(
        &mut self,
        header: &Header,
        recent_headers: &[Header],
    ) -> std::io::Result<()> {
        let next_sync_head = SyncHead {
            block_number: header.number(),
            block_hash: header.hash_slow(),
            timestamp: header.timestamp(),
        };
        let next_recent_headers = recent_headers.to_vec();

        if self.state.sync_head == Some(next_sync_head)
            && self.state.recent_headers == next_recent_headers
        {
            return Ok(());
        }

        self.state.sync_head = Some(next_sync_head);
        self.state.recent_headers = next_recent_headers;
        self.persist_state()
    }

    pub fn record_verified_canonical_state(
        &mut self,
        anchor: &ExecutionAnchor,
        header: &Header,
        recent_headers: &[Header],
    ) -> std::io::Result<()> {
        self.record_canonical_state(header, recent_headers)?;

        let mut anchors = self.catalog.anchors.clone();
        anchors.indexed_head = Some(*anchor);
        self.record_chain_anchors(anchors)
    }

    pub fn record_historical_floor(&mut self, header: &Header) -> std::io::Result<()> {
        if let Some(current) = self.state.historical_floor_header.as_ref()
            && current.number() <= header.number()
        {
            return Ok(());
        }

        self.state.historical_floor_header = Some(header.clone());
        if self.state.historical_anchor_header.is_none() {
            self.state.historical_anchor_header = Some(header.clone());
        }
        self.persist_state()
    }

    pub fn chain_anchors(&self) -> ChainAnchors {
        self.catalog.anchors.clone()
    }

    pub fn record_chain_anchors(&mut self, anchors: ChainAnchors) -> std::io::Result<()> {
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
        let next_sync_head = recent_headers.last().map(|header| SyncHead {
            block_number: header.number(),
            block_hash: header.hash_slow(),
            timestamp: header.timestamp(),
        });
        let next_recent_headers = recent_headers.to_vec();
        let mut next_anchors = self.catalog.anchors.clone();
        next_anchors.indexed_head = indexed_head;

        let state_changed = self.state.sync_head != next_sync_head
            || self.state.recent_headers != next_recent_headers;
        let anchors_changed = self.catalog.anchors != next_anchors;

        self.state.sync_head = next_sync_head;
        self.state.recent_headers = next_recent_headers;
        self.catalog.anchors = next_anchors;

        if state_changed {
            self.persist_state()?;
        }
        if anchors_changed {
            self.persist_catalog()?;
        }

        Ok(())
    }

    pub fn write_batch(&mut self, rows: &[logex_types::LogRow]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        self.wal.append(rows)?;
        self.commit_rows_to_segments(rows)?;
        self.wal.truncate()?;

        Ok(())
    }

    pub fn write_historical_batch(&mut self, rows: &[logex_types::LogRow]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let target_rows = self.config.hot_target_rows.max(1) as usize;
        let mut wrote_segment = false;
        for chunk in rows.chunks(target_rows) {
            let mut descriptor = self.catalog.allocate_segment(SegmentKind::Sealed);
            let segment_dir = self.paths.segment_dir(descriptor.id);
            if segment_dir.exists() {
                fs::remove_dir_all(&segment_dir)?;
            }
            let columns = write_compacted_rows(&segment_dir, chunk)?;
            apply_ordered_rows_to_descriptor(&mut descriptor, chunk);
            persist_segment_manifest_with_columns(&self.paths, &descriptor, columns)?;
            self.catalog.segments.push(descriptor);
            wrote_segment = true;
        }
        if wrote_segment {
            self.persist_catalog()?;
        }

        Ok(())
    }

    fn commit_rows_to_segments(&mut self, rows: &[logex_types::LogRow]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let target_rows = self.config.hot_target_rows.max(1);
        let mut offset = 0usize;
        while offset < rows.len() {
            let hot_id = self.ensure_active_hot_segment()?;

            let remaining_capacity = {
                let descriptor = self
                    .catalog
                    .segments
                    .iter()
                    .find(|segment| segment.id == hot_id)
                    .ok_or_else(|| std::io::Error::other("active hot segment is missing"))?;
                target_rows.saturating_sub(descriptor.row_count)
            };

            if remaining_capacity == 0 {
                self.seal_hot_segment()?;
                continue;
            }

            let take = remaining_capacity.min((rows.len() - offset) as u64) as usize;
            let chunk = &rows[offset..offset + take];
            let hot_dir = self.paths.segment_dir(hot_id);

            let should_seal = {
                let descriptor = self
                    .catalog
                    .segments
                    .iter_mut()
                    .find(|segment| segment.id == hot_id)
                    .ok_or_else(|| std::io::Error::other("active hot segment is missing"))?;

                append_rows(&hot_dir, descriptor.row_count, chunk)?;
                apply_rows_to_descriptor(descriptor, chunk);
                let should_seal = descriptor.row_count >= target_rows;
                persist_segment_manifest(&self.paths, descriptor)?;
                should_seal
            };

            self.persist_catalog()?;
            offset += take;

            if should_seal {
                self.seal_hot_segment()?;
            }
        }

        Ok(())
    }

    pub fn refresh_segment_indexes(&mut self, segment_id: u64) -> std::io::Result<()> {
        let descriptor = self
            .catalog
            .segments
            .iter()
            .find(|segment| segment.id == segment_id)
            .cloned()
            .ok_or_else(|| std::io::Error::other("segment not found"))?;
        if self.should_compact_segment(&descriptor) {
            compact_segment(&self.paths, &descriptor)
        } else {
            persist_segment_manifest(&self.paths, &descriptor)
        }
    }

    pub fn compact_eligible_segments(&mut self) -> std::io::Result<usize> {
        self.compact_eligible_segments_limit(usize::MAX)
    }

    pub fn compact_raw_segments_limit(&mut self, limit: usize) -> std::io::Result<usize> {
        let plan = self.raw_segment_compaction_plan(limit)?;
        plan.compact()
    }

    pub fn compact_eligible_segments_limit(&mut self, limit: usize) -> std::io::Result<usize> {
        let plan = self.segment_compaction_plan(limit)?;
        plan.compact()
    }

    pub fn raw_segment_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.compaction_plan(limit, CompactionMode::RawOnly)
    }

    pub fn recent_raw_segment_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.compaction_plan_with_order(
            limit,
            CompactionMode::RawOnly,
            CompactionOrder::NewestFirst,
        )
    }

    pub fn segment_compaction_plan(&self, limit: usize) -> std::io::Result<SegmentCompactionPlan> {
        self.compaction_plan(limit, CompactionMode::CurrentProfile)
    }

    pub fn profile_rewrite_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
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

        Ok(SegmentCompactionPlan::new(self.paths.clone(), eligible))
    }

    fn segment_matches_compaction_mode(
        &self,
        segment: &SegmentDescriptor,
        mode: CompactionMode,
    ) -> std::io::Result<bool> {
        match mode {
            CompactionMode::RawOnly => self.segment_needs_raw_compaction(segment),
            CompactionMode::ProfileRewrite => self.segment_needs_profile_rewrite(segment),
            CompactionMode::CurrentProfile => self.segment_needs_compaction(segment),
        }
    }

    pub fn raw_compaction_backlog_count(&self) -> std::io::Result<usize> {
        let mut count = 0usize;
        for segment in &self.catalog.segments {
            if self.segment_needs_raw_compaction(segment)? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn compaction_backlog_count(&self) -> std::io::Result<usize> {
        let mut count = 0usize;
        for segment in &self.catalog.segments {
            if self.segment_needs_compaction(segment)? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn profile_rewrite_backlog_count(&self) -> std::io::Result<usize> {
        let mut count = 0usize;
        for segment in &self.catalog.segments {
            if self.segment_needs_profile_rewrite(segment)? {
                count += 1;
            }
        }
        Ok(count)
    }

    pub fn mark_non_canonical(&self, block_hash: B256) -> std::io::Result<u64> {
        let mut total_marked = 0u64;

        for descriptor in &self.catalog.segments {
            if descriptor.row_count == 0 {
                continue;
            }

            let dir = self.paths.segment_dir(descriptor.id);
            let reader = SegmentReader::open(&dir)?;
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
                let path = dir.join("canonical.bitmap");
                let file = std::fs::File::create(&path)?;
                let mut writer = std::io::BufWriter::new(file);
                canonical.write_to(&mut writer)?;
                std::io::Write::flush(&mut writer)?;
            }
        }

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
        self.state
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
            fs::create_dir_all(&path)?;
            persist_segment_manifest(&self.paths, active)?;
            return Ok(active.id);
        }

        let descriptor = self.catalog.register_segment(SegmentKind::Hot);
        let path = self.paths.segment_dir(descriptor.id);
        fs::create_dir_all(&path)?;
        persist_segment_manifest(&self.paths, &descriptor)?;
        self.persist_catalog()?;
        Ok(descriptor.id)
    }

    fn repair_catalog_from_manifests(&mut self) -> std::io::Result<()> {
        let mut descriptors = load_manifest_descriptors(&self.paths)?;
        if descriptors.is_empty() {
            return Ok(());
        }

        let mut repaired_segments =
            Vec::with_capacity(self.catalog.segments.len().max(descriptors.len()));
        let mut changed = false;

        for segment in &self.catalog.segments {
            if let Some(descriptor) = descriptors.remove(&segment.id) {
                if &descriptor != segment {
                    tracing::warn!(
                        segment_id = segment.id,
                        catalog_rows = segment.row_count,
                        manifest_rows = descriptor.row_count,
                        "repairing storage catalog segment metadata from manifest"
                    );
                    changed = true;
                }
                repaired_segments.push(descriptor);
            } else {
                repaired_segments.push(segment.clone());
            }
        }

        for descriptor in descriptors.into_values() {
            tracing::warn!(
                segment_id = descriptor.id,
                row_count = descriptor.row_count,
                "recovering storage segment missing from catalog"
            );
            repaired_segments.push(descriptor);
            changed = true;
        }

        repaired_segments.sort_by_key(|segment| segment.id);

        let active_hot_segment = repaired_segments
            .iter()
            .filter(|segment| segment.kind == SegmentKind::Hot)
            .map(|segment| segment.id)
            .max();
        if self.catalog.active_hot_segment != active_hot_segment {
            self.catalog.active_hot_segment = active_hot_segment;
            changed = true;
        }

        if let Some(max_id) = repaired_segments.iter().map(|segment| segment.id).max() {
            let next_segment_id = max_id.saturating_add(1);
            if self.catalog.next_segment_id < next_segment_id {
                self.catalog.next_segment_id = next_segment_id;
                changed = true;
            }
        }

        if self.catalog.segments != repaired_segments {
            self.catalog.segments = repaired_segments;
            changed = true;
        }

        if changed {
            self.persist_catalog()?;
        }

        Ok(())
    }

    fn seal_hot_segment(&mut self) -> std::io::Result<()> {
        let hot_id = self
            .catalog
            .active_hot_segment
            .ok_or_else(|| std::io::Error::other("missing hot segment"))?;
        if let Some(descriptor) = self
            .catalog
            .segments
            .iter_mut()
            .find(|segment| segment.id == hot_id)
        {
            descriptor.kind = SegmentKind::Sealed;
            persist_segment_manifest(&self.paths, descriptor)?;
            tracing::info!(
                segment_id = descriptor.id,
                row_count = descriptor.row_count,
                "sealed storage segment"
            );
        }

        self.catalog.active_hot_segment = None;
        let new_hot = self.catalog.register_segment(SegmentKind::Hot);
        let path = self.paths.segment_dir(new_hot.id);
        fs::create_dir_all(&path)?;
        persist_segment_manifest(&self.paths, &new_hot)?;
        self.persist_catalog()?;
        Ok(())
    }

    fn replay_wal(&mut self) -> std::io::Result<()> {
        let rows = self.wal.read_all()?;
        if rows.is_empty() {
            return Ok(());
        }

        tracing::info!(rows = rows.len(), "replaying WAL entries");

        self.commit_rows_to_segments(&rows)?;
        self.wal.truncate()?;
        Ok(())
    }

    fn partition_meta(&self, descriptor: &SegmentDescriptor) -> PartitionMeta {
        PartitionMeta {
            id: descriptor.id,
            min_block: descriptor.min_block.unwrap_or(u64::MAX),
            max_block: descriptor.max_block.unwrap_or(0),
            row_count: descriptor.row_count,
            sealed: descriptor.kind == SegmentKind::Sealed,
            path: self.paths.segment_dir(descriptor.id),
        }
    }

    fn persist_catalog(&self) -> std::io::Result<()> {
        self.catalog.persist(&self.paths)
    }

    fn persist_state(&self) -> std::io::Result<()> {
        let path = self.state_path();
        let tmp = path.with_extension("json.tmp");
        let json = serde_json::to_vec(&self.state).map_err(std::io::Error::other)?;
        fs::write(&tmp, json)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    fn state_path(&self) -> PathBuf {
        self.paths.root().join(STORAGE_STATE_FILE)
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
        verify_recent_headers(&self.state)?;

        verify_segments_integrity_parallel(&self.paths, &self.catalog.segments)?;

        tracing::info!(
            segments = self.catalog.segments.len(),
            recent_headers = self.state.recent_headers.len(),
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

    let reader = SegmentReader::open(&dir)?;
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

fn load_manifest_descriptors(
    paths: &StorageCatalogPaths,
) -> std::io::Result<BTreeMap<u64, SegmentDescriptor>> {
    let mut descriptors = BTreeMap::new();
    let segments_dir = paths.segments_dir();
    if !segments_dir.exists() {
        return Ok(descriptors);
    }

    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        if !entry.path().is_dir() {
            continue;
        }

        let Some(segment_id) = parse_segment_dir_name(&entry.file_name()) else {
            continue;
        };
        let manifest_path = entry.path().join("segment.json");
        if !manifest_path.exists() {
            continue;
        }

        let json = fs::read_to_string(&manifest_path)?;
        let manifest: SegmentManifest = serde_json::from_str(&json).map_err(io::Error::other)?;
        if manifest.segment_id != segment_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment directory id {segment_id} does not match manifest id {}",
                    manifest.segment_id
                ),
            ));
        }

        let relative_path = PathBuf::from("segments").join(format!("s_{segment_id:016}"));
        descriptors.insert(
            segment_id,
            SegmentDescriptor {
                id: segment_id,
                generation: manifest.generation,
                kind: manifest.kind,
                relative_path: relative_path.clone(),
                manifest_relative_path: relative_path.join("segment.json"),
                min_block: manifest.min_block,
                max_block: manifest.max_block,
                row_count: manifest.row_count,
            },
        );
    }

    Ok(descriptors)
}

fn parse_segment_dir_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let id = name.strip_prefix("s_")?;
    id.parse().ok()
}

fn load_state(paths: &StorageCatalogPaths) -> std::io::Result<StorageState> {
    let path = paths.root().join(STORAGE_STATE_FILE);
    if !path.exists() {
        return Ok(StorageState::default());
    }

    let json = fs::read_to_string(path)?;
    serde_json::from_str(&json).map_err(std::io::Error::other)
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
    use std::fs;

    use alloy_consensus::Header;
    use alloy_primitives::{Address, B256, bytes};
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
        assert!(first_segment.join("columns/address.pages").exists());

        let reader = SegmentReader::open(&first_segment).unwrap();
        assert_eq!(reader.read_log_rows(None).unwrap(), rows[..10].to_vec());

        storage
            .record_sync_head(10_000, B256::repeat_byte(0xAA), 999)
            .unwrap();
        assert_eq!(storage.raw_compaction_backlog_count().unwrap(), 0);
        assert!(!first_segment.join("address.col").exists());
        assert!(first_segment.join("columns/address.pages").exists());

        let reloaded = NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        assert_eq!(reloaded.total_rows(), 25);
        assert_eq!(reloaded.sealed_count(), 3);
        assert_eq!(reloaded.hot_partition_meta().row_count, 0);
    }

    #[cfg(unix)]
    #[test]
    fn catalog_repair_recovers_symlinked_segment_directories() {
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
        fs::remove_file(storage.paths.catalog_path()).unwrap();
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
    fn native_storage_recovers_catalog_from_segment_manifests() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        };

        {
            let mut storage = NativeStorage::open(config.clone()).unwrap();
            storage.write_batch(&make_rows(25, 100)).unwrap();
        }

        {
            let (mut catalog, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
            catalog.segments.truncate(1);
            catalog.segments[0].kind = SegmentKind::Hot;
            catalog.segments[0].row_count = 4;
            catalog.next_segment_id = 1;
            catalog.active_hot_segment = Some(catalog.segments[0].id);
            catalog.persist(&paths).unwrap();
        }

        let recovered = NativeStorage::open(config).unwrap();
        assert_eq!(recovered.total_rows(), 25);
        assert_eq!(recovered.sealed_count(), 2);
        assert_eq!(recovered.hot_partition_meta().row_count, 5);
        assert_eq!(
            recovered.segments().last().map(|segment| segment.id),
            Some(2)
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

        fs::write(
            tmp.path().join(STORAGE_STATE_FILE),
            serde_json::to_vec_pretty(&bad_state).unwrap(),
        )
        .unwrap();

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

        storage.refresh_segment_indexes(sealed.id).unwrap();
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
