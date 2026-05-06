use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use alloy_consensus::{BlockHeader, Header};
use alloy_primitives::B256;
use logex_types::{ChainAnchors, ExecutionAnchor, ExecutionBlockMarker, PartitionMeta};
use serde::{Deserialize, Serialize};

use crate::SegmentReader;
use crate::state::SyncHead;
use crate::wal::WriteAheadLog;

use super::catalog::{
    NativeStorageCatalog, NativeStorageConfig, SegmentDescriptor, SegmentKind, StorageCatalogPaths,
};
use super::segment::{
    append_rows, apply_rows_to_descriptor, compact_segment, persist_segment_manifest,
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

        let hot_id = self.ensure_active_hot_segment()?;
        let hot_dir = self.paths.segment_dir(hot_id);
        let descriptor = self
            .catalog
            .segments
            .iter_mut()
            .find(|segment| segment.id == hot_id)
            .ok_or_else(|| std::io::Error::other("active hot segment is missing"))?;

        append_rows(&hot_dir, descriptor.row_count, rows)?;
        apply_rows_to_descriptor(descriptor, rows);
        let should_seal = descriptor.row_count >= self.config.hot_target_rows;
        persist_segment_manifest(&self.paths, descriptor)?;
        let _ = descriptor;
        self.persist_catalog()?;

        self.wal.truncate()?;

        if should_seal {
            self.seal_hot_segment()?;
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
        let eligible: Vec<_> = self
            .catalog
            .segments
            .iter()
            .filter(|segment| self.should_compact_segment(segment))
            .cloned()
            .collect();

        for descriptor in &eligible {
            compact_segment(&self.paths, descriptor)?;
        }

        Ok(eligible.len())
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

        let hot_id = self.ensure_active_hot_segment()?;
        let hot_dir = self.paths.segment_dir(hot_id);
        let descriptor = self
            .catalog
            .segments
            .iter_mut()
            .find(|segment| segment.id == hot_id)
            .ok_or_else(|| std::io::Error::other("active hot segment is missing"))?;

        append_rows(&hot_dir, descriptor.row_count, &rows)?;
        apply_rows_to_descriptor(descriptor, &rows);
        persist_segment_manifest(&self.paths, descriptor)?;
        self.persist_catalog()?;
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
        let json = serde_json::to_vec_pretty(&self.state).map_err(std::io::Error::other)?;
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

    fn verify_integrity(&self) -> io::Result<()> {
        verify_recent_headers(&self.state)?;

        for descriptor in &self.catalog.segments {
            self.verify_segment_integrity(descriptor)?;
        }

        tracing::info!(
            segments = self.catalog.segments.len(),
            recent_headers = self.state.recent_headers.len(),
            "storage integrity check passed"
        );
        Ok(())
    }

    fn verify_segment_integrity(&self, descriptor: &SegmentDescriptor) -> io::Result<()> {
        let dir = self.paths.segment_dir(descriptor.id);
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

        let canonical = reader.read_canonical()?;
        if canonical.len() != row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "segment {} canonical bitmap length mismatch: bitmap={} rows={row_count}",
                    descriptor.id,
                    canonical.len()
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
        assert!(storage.hot_partition_meta().row_count == 0);
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

        storage
            .record_sync_head(
                sealed.max_block.unwrap() + 200,
                B256::repeat_byte(0xAA),
                999,
            )
            .unwrap();
        assert_eq!(storage.compact_eligible_segments().unwrap(), 1);
        assert!(!sealed_path.join("address.col").exists());
        assert!(sealed_path.join("columns/address.pages").exists());
    }
}
