use std::fs;
use std::path::Path;

use logex_storage::ColumnReader;

use crate::btree::BTreeIndex;
use crate::composite::CompositeIndexBuilder;

/// Builds per-partition indexes from column data.
pub struct IndexBuilder;

impl IndexBuilder {
    /// Build all indexes (primary + composite) for a partition.
    pub fn build_all_indexes(partition_dir: &Path) -> std::io::Result<()> {
        Self::build_primary_indexes(partition_dir)?;
        CompositeIndexBuilder::build_composite_indexes(partition_dir)?;
        Ok(())
    }

    /// Build all primary indexes for a partition and write them to the indexes/ subdirectory.
    pub fn build_primary_indexes(partition_dir: &Path) -> std::io::Result<()> {
        let index_dir = partition_dir.join("indexes");
        fs::create_dir_all(&index_dir)?;

        Self::build_address_index(partition_dir, &index_dir)?;
        Self::build_topic0_index(partition_dir, &index_dir)?;
        Self::build_block_number_index(partition_dir, &index_dir)?;

        Ok(())
    }

    /// Build address index: Address (20 bytes) -> RoaringBitmap of row IDs.
    fn build_address_index(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let addresses = ColumnReader::read_address(partition_dir, None)?;
        let mut index = BTreeIndex::new(20);

        for (row_id, addr) in addresses.iter().enumerate() {
            index.insert(addr.as_slice(), row_id as u32);
        }

        index.write_to_file(&index_dir.join("address.bptree"))?;
        tracing::debug!(keys = index.key_count(), "built address index");
        Ok(())
    }

    /// Build topic0 index: B256 (32 bytes) -> RoaringBitmap of row IDs.
    /// Only indexes rows where topic0 is present (non-null).
    fn build_topic0_index(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let topics = ColumnReader::read_nullable_b256(partition_dir, "topic0", None)?;
        let mut index = BTreeIndex::new(32);

        for (row_id, topic) in topics.iter().enumerate() {
            if let Some(t) = topic {
                index.insert(t.as_slice(), row_id as u32);
            }
        }

        index.write_to_file(&index_dir.join("topic0.bptree"))?;
        tracing::debug!(keys = index.key_count(), "built topic0 index");
        Ok(())
    }

    /// Build block_number index: u64 as big-endian 8 bytes -> RoaringBitmap of row IDs.
    /// Uses big-endian so lexicographic ordering matches numeric ordering (for range scans).
    fn build_block_number_index(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let blocks = ColumnReader::read_u64(partition_dir, "block_number.col", None)?;
        let mut index = BTreeIndex::new(8);

        for (row_id, &block) in blocks.iter().enumerate() {
            index.insert(&block.to_be_bytes(), row_id as u32);
        }

        index.write_to_file(&index_dir.join("block_number.bptree"))?;
        tracing::debug!(keys = index.key_count(), "built block_number index");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::BTreeIndexReader;
    use alloy_primitives::{Address, B256, bytes};
    use logex_storage::ColumnFile;
    use logex_types::{LogRow, Source};
    use tempfile::TempDir;

    fn make_test_rows() -> Vec<LogRow> {
        vec![
            LogRow {
                block_number: 100,
                block_hash: B256::repeat_byte(0x01),
                timestamp: 1_700_000_000,
                tx_hash: B256::repeat_byte(0x11),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            },
            LogRow {
                block_number: 100,
                block_hash: B256::repeat_byte(0x01),
                timestamp: 1_700_000_000,
                tx_hash: B256::repeat_byte(0x11),
                tx_index: 0,
                log_index: 1,
                address: Address::repeat_byte(0xBB),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Receipt,
            },
            LogRow {
                block_number: 200,
                block_hash: B256::repeat_byte(0x02),
                timestamp: 1_700_001_200,
                tx_hash: B256::repeat_byte(0x22),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0xEE)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("beef"),
                data_len: 2,
                source: Source::Receipt,
            },
        ]
    }

    #[test]
    fn test_build_primary_indexes() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows();

        ColumnFile::write_batch(&dir, &rows).unwrap();
        IndexBuilder::build_primary_indexes(&dir).unwrap();

        // Verify address index
        let addr_idx = BTreeIndexReader::open(&dir.join("indexes/address.bptree")).unwrap();
        assert_eq!(addr_idx.key_count(), 2); // 0xAA and 0xBB

        let aa = addr_idx.get(Address::repeat_byte(0xAA).as_slice()).unwrap();
        assert!(aa.contains(0)); // row 0
        assert!(aa.contains(2)); // row 2
        assert_eq!(aa.len(), 2);

        let bb = addr_idx.get(Address::repeat_byte(0xBB).as_slice()).unwrap();
        assert!(bb.contains(1)); // row 1
        assert_eq!(bb.len(), 1);

        // Verify topic0 index
        let topic_idx = BTreeIndexReader::open(&dir.join("indexes/topic0.bptree")).unwrap();
        assert_eq!(topic_idx.key_count(), 2); // 0xDD and 0xEE

        let dd = topic_idx.get(B256::repeat_byte(0xDD).as_slice()).unwrap();
        assert!(dd.contains(0));
        assert!(dd.contains(1));
        assert_eq!(dd.len(), 2);

        // Verify block_number index
        let block_idx = BTreeIndexReader::open(&dir.join("indexes/block_number.bptree")).unwrap();
        assert_eq!(block_idx.key_count(), 2); // blocks 100 and 200

        let b100 = block_idx.get(&100u64.to_be_bytes()).unwrap();
        assert!(b100.contains(0));
        assert!(b100.contains(1));
        assert_eq!(b100.len(), 2);

        let b200 = block_idx.get(&200u64.to_be_bytes()).unwrap();
        assert!(b200.contains(2));
        assert_eq!(b200.len(), 1);
    }

    #[test]
    fn test_block_number_range_scan() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows();

        ColumnFile::write_batch(&dir, &rows).unwrap();
        IndexBuilder::build_primary_indexes(&dir).unwrap();

        let block_idx = BTreeIndexReader::open(&dir.join("indexes/block_number.bptree")).unwrap();

        // Range [100, 200) => block 100 only
        let result = block_idx.range(&100u64.to_be_bytes(), &200u64.to_be_bytes());
        assert!(result.contains(0));
        assert!(result.contains(1));
        assert!(!result.contains(2));

        // Range [100, 201) => blocks 100 and 200
        let result = block_idx.range(&100u64.to_be_bytes(), &201u64.to_be_bytes());
        assert_eq!(result.len(), 3);
    }
}
