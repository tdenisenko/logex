use std::path::{Path, PathBuf};

use alloy_consensus::Header;
use alloy_primitives::B256;
use logex_types::{ChainAnchors, ExecutionAnchor, ExecutionBlockMarker, LogRow, PartitionMeta};

use crate::native::{NativeStorage, NativeStorageConfig, SegmentCompactionPlan};
use crate::state::SyncHead;

/// A read-only compatibility view over a storage segment.
#[derive(Debug, Clone)]
pub struct Partition {
    pub meta: PartitionMeta,
}

/// Configuration for the storage engine.
#[derive(Debug, Clone)]
pub struct PartitionManagerConfig {
    /// Base data directory.
    pub data_dir: PathBuf,
    /// Target row count before sealing the active hot segment.
    pub partition_target_rows: u64,
    /// Delay permanent compaction of sealed history until it is sufficiently
    /// behind the current head.
    pub compaction_safety_margin_blocks: u64,
}

impl Default for PartitionManagerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        }
    }
}

/// Compatibility facade that keeps the rest of the node stable while the
/// underlying storage engine is rewritten around native sealed/hot segments.
pub struct PartitionManager {
    inner: NativeStorage,
    sealed_partitions: Vec<Partition>,
    hot_partition: Partition,
}

impl PartitionManager {
    /// Return the configured base data directory for this storage instance.
    pub fn data_dir(&self) -> &Path {
        self.inner.data_dir()
    }

    /// Open or create the storage engine at the given data directory.
    pub fn open(config: PartitionManagerConfig) -> std::io::Result<Self> {
        let inner = NativeStorage::open(NativeStorageConfig {
            data_dir: config.data_dir,
            hot_target_rows: config.partition_target_rows,
            compaction_safety_margin_blocks: config.compaction_safety_margin_blocks,
        })?;

        let mut manager = Self {
            inner,
            sealed_partitions: Vec::new(),
            hot_partition: Partition {
                meta: PartitionMeta {
                    id: 0,
                    min_block: u64::MAX,
                    max_block: 0,
                    min_timestamp: None,
                    max_timestamp: None,
                    row_count: 0,
                    sealed: false,
                    path: PathBuf::new(),
                },
            },
        };
        manager.refresh_views();
        Ok(manager)
    }

    /// Ingest a batch of log rows.
    pub fn write_batch(&mut self, rows: &[LogRow]) -> std::io::Result<()> {
        self.inner.write_batch(rows)?;
        self.refresh_views();
        Ok(())
    }

    /// Ingest immutable historical rows using compacted dense segments and sparse staging.
    pub fn write_historical_batch(&mut self, rows: &[LogRow]) -> std::io::Result<()> {
        self.inner.write_historical_batch(rows)?;
        self.refresh_views();
        Ok(())
    }

    /// Atomically associate canonical rows with their restart progress.
    /// Uncheckpointed work can be re-fetched after restart.
    pub fn ingest_canonical_batch(
        &mut self,
        rows: &[LogRow],
        header: &Header,
        recent_headers: &[Header],
        anchor: Option<&ExecutionAnchor>,
    ) -> std::io::Result<()> {
        self.inner
            .ingest_canonical_batch(rows, header, recent_headers, anchor)?;
        self.refresh_views();
        Ok(())
    }

    /// Associate a whole historical chunk, including empty blocks, with its floor.
    pub fn ingest_historical_batch(
        &mut self,
        rows: &[LogRow],
        floor: &Header,
    ) -> std::io::Result<()> {
        self.inner.ingest_historical_batch(rows, floor)?;
        self.refresh_views();
        Ok(())
    }

    /// Compact and close any sparse historical staging segment.
    pub fn finalize_historical_segment(&mut self) -> std::io::Result<bool> {
        let finalized = self.inner.finalize_active_historical_segment()?;
        if finalized {
            self.refresh_views();
        }
        Ok(finalized)
    }

    /// Make current rows and sync progress durable and retire recovery metadata.
    pub fn checkpoint(&mut self) -> std::io::Result<()> {
        self.inner.checkpoint()?;
        self.refresh_views();
        Ok(())
    }

    pub fn checkpoint_if_due(&mut self) -> std::io::Result<bool> {
        self.inner.checkpoint_if_due()
    }

    /// Return the segment currently absorbing sparse historical writes.
    pub fn active_historical_segment_id(&self) -> Option<u64> {
        self.inner.active_historical_segment_id()
    }

    /// Refresh manifest metadata after indexes are rebuilt externally.
    pub fn refresh_segment_indexes(&mut self, segment_id: u64) -> std::io::Result<()> {
        self.inner.refresh_segment_indexes(segment_id)
    }

    /// Refresh manifest metadata after external index writes without compacting
    /// or rewriting segment columns.
    pub fn refresh_segment_manifest(&mut self, segment_id: u64) -> std::io::Result<()> {
        self.inner.refresh_segment_manifest(segment_id)
    }

    /// Compact sealed segments that are safely behind the current head.
    pub fn compact_eligible_segments(&mut self) -> std::io::Result<usize> {
        self.inner.compact_eligible_segments()
    }

    /// Compact up to `limit` sealed segments that are safely behind the current head.
    pub fn compact_eligible_segments_limit(&mut self, limit: usize) -> std::io::Result<usize> {
        let compacted = self.inner.compact_eligible_segments_limit(limit)?;
        if compacted > 0 {
            self.refresh_views();
        }
        Ok(compacted)
    }

    /// Compact raw sealed segments without migrating stale compacted profiles.
    pub fn compact_raw_segments_limit(&mut self, limit: usize) -> std::io::Result<usize> {
        let compacted = self.inner.compact_raw_segments_limit(limit)?;
        if compacted > 0 {
            self.refresh_views();
        }
        Ok(compacted)
    }

    /// Select sealed segments that can be compacted outside the storage lock.
    pub fn segment_compaction_plan(&self, limit: usize) -> std::io::Result<SegmentCompactionPlan> {
        self.inner.segment_compaction_plan(limit)
    }

    /// Select raw sealed segments that can be compacted outside the storage lock.
    pub fn raw_segment_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.inner.raw_segment_compaction_plan(limit)
    }

    /// Select recent raw sealed segments that can be compacted outside the storage lock.
    pub fn recent_raw_segment_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.inner.recent_raw_segment_compaction_plan(limit)
    }

    /// Select compacted sealed segments that only need migration to the current profile.
    pub fn profile_rewrite_compaction_plan(
        &self,
        limit: usize,
    ) -> std::io::Result<SegmentCompactionPlan> {
        self.inner.profile_rewrite_compaction_plan(limit)
    }

    /// Count sealed segments that are eligible for compaction.
    pub fn compaction_backlog_count(&self) -> std::io::Result<usize> {
        self.inner.compaction_backlog_count()
    }

    /// Count raw sealed segments that need first-time compaction.
    pub fn raw_compaction_backlog_count(&self) -> std::io::Result<usize> {
        self.inner.raw_compaction_backlog_count()
    }

    /// Count compacted sealed segments that need migration to the current profile.
    pub fn profile_rewrite_backlog_count(&self) -> std::io::Result<usize> {
        self.inner.profile_rewrite_backlog_count()
    }

    /// Persist the latest fully-validated block, even when it produced no logs.
    pub fn record_sync_head(
        &mut self,
        block_number: u64,
        block_hash: B256,
        timestamp: u64,
    ) -> std::io::Result<()> {
        self.inner
            .record_sync_head(block_number, block_hash, timestamp)
    }

    /// Return the current sync head, which may include uncheckpointed ingestion.
    pub fn sync_head(&self) -> Option<SyncHead> {
        self.inner.sync_head()
    }

    /// Return the current canonical header window, including uncheckpointed work.
    pub fn recent_headers(&self) -> &[Header] {
        self.inner.recent_headers()
    }

    /// Return the lowest verified historical EL header, if any.
    pub fn historical_floor_header(&self) -> Option<&Header> {
        self.inner.historical_floor_header()
    }

    /// Return the anchor header where EL reverse backfill started, if any.
    pub fn historical_anchor_header(&self) -> Option<&Header> {
        self.inner.historical_anchor_header()
    }

    /// Return the lowest verified historical EL block marker, if any.
    pub fn historical_floor(&self) -> Option<ExecutionBlockMarker> {
        self.inner.historical_floor()
    }

    /// Return the historical reverse backfill anchor marker, if any.
    pub fn historical_anchor(&self) -> Option<ExecutionBlockMarker> {
        self.inner.historical_anchor()
    }

    /// Persist the latest canonical head and recent canonical header window.
    pub fn record_canonical_state(
        &mut self,
        header: &Header,
        recent_headers: &[Header],
    ) -> std::io::Result<()> {
        self.inner.record_canonical_state(header, recent_headers)
    }

    /// Persist the latest verified execution anchor together with the
    /// canonical EL header window.
    pub fn record_verified_canonical_state(
        &mut self,
        anchor: &ExecutionAnchor,
        header: &Header,
        recent_headers: &[Header],
    ) -> std::io::Result<()> {
        self.inner
            .record_verified_canonical_state(anchor, header, recent_headers)
    }

    /// Persist the lowest verified historical EL header without moving the live sync head.
    pub fn record_historical_floor(&mut self, header: &Header) -> std::io::Result<()> {
        self.inner.record_historical_floor(header)
    }

    /// Return current chain anchors, including uncheckpointed indexed progress.
    pub fn chain_anchors(&self) -> ChainAnchors {
        self.inner.chain_anchors()
    }

    /// Persist the compact execution-facing chain anchors.
    pub fn record_chain_anchors(&mut self, anchors: ChainAnchors) -> std::io::Result<()> {
        self.inner.record_chain_anchors(anchors)
    }

    /// Rewind the persisted canonical head after a consensus-driven reorg.
    pub fn rewind_canonical_state(
        &mut self,
        recent_headers: &[Header],
        indexed_head: Option<ExecutionAnchor>,
    ) -> std::io::Result<()> {
        self.inner
            .rewind_canonical_state(recent_headers, indexed_head)
    }

    /// Mark rows in a given block as non-canonical during a reorg.
    pub fn mark_non_canonical(&mut self, block_hash: B256) -> std::io::Result<u64> {
        self.inner.mark_non_canonical(block_hash)
    }

    /// Highest block number that produced at least one stored log row.
    pub fn indexed_head_block(&self) -> Option<u64> {
        self.inner.indexed_head_block()
    }

    /// Current sync head, falling back to the indexed head when metadata has
    /// not been recorded yet.
    pub fn head_block(&self) -> Option<u64> {
        self.inner.head_block()
    }

    /// Total number of rows across all segments.
    pub fn total_rows(&self) -> u64 {
        self.inner.total_rows()
    }

    /// Number of sealed segments.
    pub fn sealed_count(&self) -> usize {
        self.inner.sealed_count()
    }

    /// Access to sealed segment metadata.
    pub fn sealed_partitions(&self) -> &[Partition] {
        &self.sealed_partitions
    }

    /// Access to the active hot segment metadata.
    pub fn hot_partition(&self) -> &Partition {
        &self.hot_partition
    }

    fn refresh_views(&mut self) {
        self.sealed_partitions = self
            .inner
            .sealed_partition_metas()
            .into_iter()
            .map(|meta| Partition { meta })
            .collect();
        self.hot_partition = Partition {
            meta: self.inner.hot_partition_meta(),
        };
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::Header;
    use alloy_primitives::{Address, B256, bytes};
    use logex_types::Source;
    use tempfile::TempDir;

    use super::*;

    fn make_test_rows(count: usize, start_block: u64) -> Vec<LogRow> {
        (0..count)
            .map(|i| LogRow {
                block_number: start_block + i as u64 / 10,
                block_hash: B256::repeat_byte((i % 250) as u8),
                timestamp: 1_700_000_000 + i as u64 * 12,
                tx_hash: B256::repeat_byte(((i + 1) % 250) as u8),
                tx_index: (i % 8) as u32,
                log_index: i as u32,
                address: Address::repeat_byte((i % 250) as u8),
                topic0: Some(B256::repeat_byte(0x10)),
                topic1: if i % 2 == 0 {
                    Some(B256::repeat_byte(0x20))
                } else {
                    None
                },
                topic2: None,
                topic3: None,
                data: bytes!("deadbeef"),
                data_len: 4,
                source: Source::Receipt,
            })
            .collect()
    }

    #[test]
    fn manager_persists_rows_and_rotates_segments() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        mgr.write_batch(&make_test_rows(150, 1000)).unwrap();

        assert_eq!(mgr.total_rows(), 150);
        assert_eq!(mgr.sealed_count(), 1);
        assert_eq!(mgr.sealed_partitions()[0].meta.row_count, 100);
        assert_eq!(mgr.hot_partition().meta.row_count, 50);
    }

    #[test]
    fn manager_coalesces_sparse_historical_partitions_across_writes() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        mgr.write_historical_batch(&make_test_rows(25, 1000))
            .unwrap();
        mgr.write_historical_batch(&make_test_rows(5, 900)).unwrap();

        assert_eq!(mgr.total_rows(), 30);
        assert_eq!(mgr.sealed_count(), 3);
        assert_eq!(
            mgr.sealed_partitions()
                .iter()
                .map(|partition| partition.meta.row_count)
                .collect::<Vec<_>>(),
            vec![10, 10, 10]
        );
        assert_eq!(mgr.hot_partition().meta.row_count, 0);
        assert_eq!(mgr.active_historical_segment_id(), None);
    }

    #[test]
    fn manager_recovers_sync_head_and_recent_headers() {
        let tmp = TempDir::new().unwrap();

        {
            let mut mgr = PartitionManager::open(PartitionManagerConfig {
                data_dir: tmp.path().to_path_buf(),
                partition_target_rows: 1_000,
                compaction_safety_margin_blocks: 2_048,
            })
            .unwrap();

            let headers = vec![Header {
                number: 55,
                timestamp: 777,
                ..Default::default()
            }];

            mgr.record_canonical_state(&headers[0], &headers).unwrap();
        }

        let mgr = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        assert_eq!(mgr.sync_head().map(|head| head.block_number), Some(55));
        assert_eq!(mgr.recent_headers().len(), 1);
    }

    #[test]
    fn manager_persists_chain_anchors() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        mgr.record_chain_anchors(ChainAnchors {
            indexed_head: Some(ExecutionAnchor {
                beacon_root: B256::repeat_byte(0x11),
                beacon_slot: 100,
                block_number: 55,
                block_hash: B256::repeat_byte(0x22),
                receipts_root: B256::repeat_byte(0x33),
            }),
            finalized_head: None,
            optimistic_head: None,
        })
        .unwrap();

        drop(mgr);
        let reloaded = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();

        assert_eq!(
            reloaded
                .chain_anchors()
                .indexed_head
                .map(|anchor| anchor.block_number),
            Some(55)
        );
    }
}
