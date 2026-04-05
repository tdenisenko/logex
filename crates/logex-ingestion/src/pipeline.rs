use std::path::Path;

use alloy_primitives::{B256, Log};

use logex_index::IndexBuilder;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::BlockContext;

use crate::extract;

/// A block for reorg processing: (block_number, block_hash, timestamp, txs).
pub type ReorgBlock = (u64, B256, u64, Vec<(B256, Vec<Log>)>);

/// The ingestion pipeline: receives blocks, extracts logs, writes to storage,
/// and builds indexes on sealed partitions.
pub struct Pipeline {
    storage: PartitionManager,
}

impl Pipeline {
    /// Open a pipeline with the given data directory.
    pub fn open(data_dir: &Path) -> std::io::Result<Self> {
        let config = PartitionManagerConfig {
            data_dir: data_dir.to_path_buf(),
            ..Default::default()
        };
        Self::open_with_config(config)
    }

    /// Open a pipeline with a custom config.
    pub fn open_with_config(config: PartitionManagerConfig) -> std::io::Result<Self> {
        let storage = PartitionManager::open(config)?;
        Ok(Self { storage })
    }

    /// Ingest a committed block: extract logs from receipts and write to storage.
    ///
    /// `txs` is a list of (tx_hash, logs) pairs from the block's receipts.
    pub fn ingest_block(
        &mut self,
        block_number: u64,
        block_hash: B256,
        timestamp: u64,
        txs: &[(B256, Vec<Log>)],
    ) -> std::io::Result<u64> {
        let ctx = BlockContext {
            block_number,
            block_hash,
            timestamp,
        };
        let rows = extract::extract_logs(&ctx, txs);
        let count = rows.len() as u64;

        if !rows.is_empty() {
            let sealed_before = self.storage.sealed_count();
            self.storage.write_batch(&rows)?;
            let sealed_after = self.storage.sealed_count();

            // Build indexes for any newly sealed partitions
            for partition in &self.storage.sealed_partitions()[sealed_before..sealed_after] {
                IndexBuilder::build_all_indexes(&partition.meta.path)?;
                tracing::info!(
                    partition_id = partition.meta.id,
                    "built indexes for sealed partition"
                );
            }
        }

        tracing::debug!(
            block_number,
            logs = count,
            %block_hash,
            "ingested block"
        );

        Ok(count)
    }

    /// Handle a chain reorg: mark reverted blocks as non-canonical,
    /// then ingest the new canonical blocks.
    pub fn handle_reorg(
        &mut self,
        reverted_blocks: &[B256],
        new_blocks: &[ReorgBlock],
    ) -> std::io::Result<()> {
        // Mark all reverted blocks as non-canonical
        let mut total_reverted = 0u64;
        for block_hash in reverted_blocks {
            total_reverted += self.storage.mark_non_canonical(*block_hash)?;
        }

        if total_reverted > 0 {
            tracing::info!(
                blocks = reverted_blocks.len(),
                rows = total_reverted,
                "marked reverted blocks non-canonical"
            );
        }

        // Ingest new canonical blocks
        for (block_number, block_hash, timestamp, txs) in new_blocks {
            self.ingest_block(*block_number, *block_hash, *timestamp, txs)?;
        }

        Ok(())
    }

    /// Current head block number.
    pub fn head_block(&self) -> Option<u64> {
        self.storage.head_block()
    }

    /// Total row count across all partitions.
    pub fn total_rows(&self) -> u64 {
        self.storage.total_rows()
    }

    /// Access the underlying storage manager.
    pub fn storage(&self) -> &PartitionManager {
        &self.storage
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, bytes};
    use logex_storage::ColumnReader;
    use tempfile::TempDir;

    fn make_test_txs(addr: Address, topic0: B256) -> Vec<(B256, Vec<Log>)> {
        vec![(
            B256::repeat_byte(0x11),
            vec![Log::new(addr, vec![topic0], bytes!("cafe")).unwrap()],
        )]
    }

    #[test]
    fn test_pipeline_ingest_block() {
        let tmp = TempDir::new().unwrap();
        let mut pipeline = Pipeline::open(tmp.path()).unwrap();

        let txs = make_test_txs(Address::repeat_byte(0xAA), B256::repeat_byte(0xDD));
        let count = pipeline
            .ingest_block(100, B256::repeat_byte(0x01), 1_700_000_000, &txs)
            .unwrap();

        assert_eq!(count, 1);
        assert_eq!(pipeline.total_rows(), 1);
        assert_eq!(pipeline.head_block(), Some(100));
    }

    #[test]
    fn test_pipeline_multiple_blocks() {
        let tmp = TempDir::new().unwrap();
        let mut pipeline = Pipeline::open(tmp.path()).unwrap();

        for block_num in 100..110 {
            let txs = make_test_txs(Address::repeat_byte(0xAA), B256::repeat_byte(0xDD));
            pipeline
                .ingest_block(
                    block_num,
                    B256::repeat_byte(block_num as u8),
                    1_700_000_000 + block_num * 12,
                    &txs,
                )
                .unwrap();
        }

        assert_eq!(pipeline.total_rows(), 10);
        assert_eq!(pipeline.head_block(), Some(109));
    }

    #[test]
    fn test_pipeline_reorg() {
        let tmp = TempDir::new().unwrap();
        let mut pipeline = Pipeline::open(tmp.path()).unwrap();

        // Ingest blocks 100, 101, 102
        for block_num in 100..103 {
            let txs = make_test_txs(Address::repeat_byte(0xAA), B256::repeat_byte(0xDD));
            pipeline
                .ingest_block(
                    block_num,
                    B256::repeat_byte(block_num as u8),
                    1_700_000_000 + block_num * 12,
                    &txs,
                )
                .unwrap();
        }
        assert_eq!(pipeline.total_rows(), 3);

        // Reorg: revert blocks 101 and 102, replace with new versions
        let reverted = vec![B256::repeat_byte(101), B256::repeat_byte(102)];
        let new_blocks = vec![
            (
                101,
                B256::repeat_byte(0xF1),
                1_700_001_212,
                make_test_txs(Address::repeat_byte(0xBB), B256::repeat_byte(0xEE)),
            ),
            (
                102,
                B256::repeat_byte(0xF2),
                1_700_001_224,
                make_test_txs(Address::repeat_byte(0xCC), B256::repeat_byte(0xFF)),
            ),
        ];

        pipeline.handle_reorg(&reverted, &new_blocks).unwrap();

        // Total rows: 3 original + 2 new = 5 (reverted ones still exist but non-canonical)
        assert_eq!(pipeline.total_rows(), 5);

        // Verify canonical status: read the hot partition
        let hot_dir = &pipeline.storage().hot_partition().meta.path;
        let canonical = ColumnReader::read_canonical(hot_dir).unwrap();

        // Rows 0 (block 100) should be canonical
        assert!(canonical.is_present(0));
        // Rows 1,2 (old block 101,102) should be non-canonical
        assert!(!canonical.is_present(1));
        assert!(!canonical.is_present(2));
        // Rows 3,4 (new block 101,102) should be canonical
        assert!(canonical.is_present(3));
        assert!(canonical.is_present(4));
    }

    #[test]
    fn test_pipeline_empty_block() {
        let tmp = TempDir::new().unwrap();
        let mut pipeline = Pipeline::open(tmp.path()).unwrap();

        let count = pipeline
            .ingest_block(100, B256::repeat_byte(0x01), 1_700_000_000, &[])
            .unwrap();

        assert_eq!(count, 0);
        assert_eq!(pipeline.total_rows(), 0);
    }
}
