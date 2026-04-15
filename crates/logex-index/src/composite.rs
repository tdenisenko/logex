use std::fs;
use std::path::Path;

use logex_storage::SegmentReader;

use crate::btree::{BTreeIndex, BTreeIndexReader};

/// Composite index definitions for common query patterns.
///
/// Each composite index concatenates multiple column values into a single
/// fixed-size key. Because keys are stored in lexicographic order, the
/// composite index supports prefix lookups (e.g., all entries for a given
/// address regardless of topic0).
///
/// Key layout: `[address: 20B][topic0: 32B]` = 52 bytes.
const ADDR_TOPIC0_KEY_SIZE: usize = 20 + 32;

/// Key layout: `[address: 20B][topic0: 32B][block_number BE: 8B]` = 60 bytes.
const ADDR_TOPIC0_BLOCK_KEY_SIZE: usize = 20 + 32 + 8;

/// Key layout: `[topic0: 32B][topic1: 32B]` = 64 bytes.
const TOPIC0_TOPIC1_KEY_SIZE: usize = 32 + 32;

/// Builds composite indexes for a partition.
pub struct CompositeIndexBuilder;

impl CompositeIndexBuilder {
    /// Build all composite indexes for a partition.
    pub fn build_composite_indexes(partition_dir: &Path) -> std::io::Result<()> {
        let index_dir = partition_dir.join("indexes");
        fs::create_dir_all(&index_dir)?;

        Self::build_address_topic0(partition_dir, &index_dir)?;
        Self::build_address_topic0_block(partition_dir, &index_dir)?;
        Self::build_topic0_topic1(partition_dir, &index_dir)?;

        Ok(())
    }

    /// Build (address, topic0) composite index.
    /// Only indexes rows where topic0 is present.
    fn build_address_topic0(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let reader = SegmentReader::open(partition_dir)?;
        let addresses = reader.read_address(None)?;
        let topic0s = reader.read_nullable_b256("topic0", None)?;
        let mut index = BTreeIndex::new(ADDR_TOPIC0_KEY_SIZE);

        let mut key = [0u8; ADDR_TOPIC0_KEY_SIZE];
        for (row_id, (addr, topic)) in addresses.iter().zip(topic0s.iter()).enumerate() {
            if let Some(t) = topic {
                key[..20].copy_from_slice(addr.as_slice());
                key[20..].copy_from_slice(t.as_slice());
                index.insert(&key, row_id as u32);
            }
        }

        index.write_to_file(&index_dir.join("address_topic0.bptree"))?;
        tracing::debug!(
            keys = index.key_count(),
            "built address+topic0 composite index"
        );
        Ok(())
    }

    /// Build (address, topic0, block_number) composite index.
    /// Only indexes rows where topic0 is present.
    fn build_address_topic0_block(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let reader = SegmentReader::open(partition_dir)?;
        let addresses = reader.read_address(None)?;
        let topic0s = reader.read_nullable_b256("topic0", None)?;
        let blocks = reader.read_u64("block_number", None)?;
        let mut index = BTreeIndex::new(ADDR_TOPIC0_BLOCK_KEY_SIZE);

        let mut key = [0u8; ADDR_TOPIC0_BLOCK_KEY_SIZE];
        for (row_id, ((addr, topic), &block)) in addresses
            .iter()
            .zip(topic0s.iter())
            .zip(blocks.iter())
            .enumerate()
        {
            if let Some(t) = topic {
                key[..20].copy_from_slice(addr.as_slice());
                key[20..52].copy_from_slice(t.as_slice());
                key[52..].copy_from_slice(&block.to_be_bytes());
                index.insert(&key, row_id as u32);
            }
        }

        index.write_to_file(&index_dir.join("address_topic0_block.bptree"))?;
        tracing::debug!(
            keys = index.key_count(),
            "built address+topic0+block composite index"
        );
        Ok(())
    }

    /// Build (topic0, topic1) composite index.
    /// Only indexes rows where both topic0 and topic1 are present.
    fn build_topic0_topic1(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let reader = SegmentReader::open(partition_dir)?;
        let topic0s = reader.read_nullable_b256("topic0", None)?;
        let topic1s = reader.read_nullable_b256("topic1", None)?;
        let mut index = BTreeIndex::new(TOPIC0_TOPIC1_KEY_SIZE);

        let mut key = [0u8; TOPIC0_TOPIC1_KEY_SIZE];
        for (row_id, (t0, t1)) in topic0s.iter().zip(topic1s.iter()).enumerate() {
            if let (Some(t0), Some(t1)) = (t0, t1) {
                key[..32].copy_from_slice(t0.as_slice());
                key[32..].copy_from_slice(t1.as_slice());
                index.insert(&key, row_id as u32);
            }
        }

        index.write_to_file(&index_dir.join("topic0_topic1.bptree"))?;
        tracing::debug!(
            keys = index.key_count(),
            "built topic0+topic1 composite index"
        );
        Ok(())
    }
}

/// Helpers for querying composite indexes using prefix lookups.
pub struct CompositeQuery;

impl CompositeQuery {
    /// Look up (address, topic0) in the composite index.
    pub fn get_address_topic0(
        reader: &BTreeIndexReader,
        address: &[u8; 20],
        topic0: &[u8; 32],
    ) -> Option<roaring::RoaringBitmap> {
        let mut key = [0u8; ADDR_TOPIC0_KEY_SIZE];
        key[..20].copy_from_slice(address);
        key[20..].copy_from_slice(topic0);
        reader.get(&key).cloned()
    }

    /// Prefix scan: all rows for a given address across all topic0 values.
    /// Uses range [address ++ 0x00..00, address ++ 0xFF..FF + 1).
    pub fn scan_by_address(
        reader: &BTreeIndexReader,
        address: &[u8; 20],
    ) -> roaring::RoaringBitmap {
        let mut start = [0u8; ADDR_TOPIC0_KEY_SIZE];
        start[..20].copy_from_slice(address);
        // end = address + 1 (increment the address portion)
        let mut end = [0u8; ADDR_TOPIC0_KEY_SIZE];
        end[..20].copy_from_slice(address);
        increment_prefix(&mut end[..20]);
        reader.range(&start, &end)
    }

    /// Look up (address, topic0, block_number) in the triple composite index.
    pub fn get_address_topic0_block(
        reader: &BTreeIndexReader,
        address: &[u8; 20],
        topic0: &[u8; 32],
        block_number: u64,
    ) -> Option<roaring::RoaringBitmap> {
        let mut key = [0u8; ADDR_TOPIC0_BLOCK_KEY_SIZE];
        key[..20].copy_from_slice(address);
        key[20..52].copy_from_slice(topic0);
        key[52..].copy_from_slice(&block_number.to_be_bytes());
        reader.get(&key).cloned()
    }

    /// Range scan on (address, topic0) prefix with block_number range [from_block, to_block).
    pub fn range_address_topic0_blocks(
        reader: &BTreeIndexReader,
        address: &[u8; 20],
        topic0: &[u8; 32],
        from_block: u64,
        to_block: u64,
    ) -> roaring::RoaringBitmap {
        let mut start = [0u8; ADDR_TOPIC0_BLOCK_KEY_SIZE];
        start[..20].copy_from_slice(address);
        start[20..52].copy_from_slice(topic0);
        start[52..].copy_from_slice(&from_block.to_be_bytes());

        let mut end = [0u8; ADDR_TOPIC0_BLOCK_KEY_SIZE];
        end[..20].copy_from_slice(address);
        end[20..52].copy_from_slice(topic0);
        end[52..].copy_from_slice(&to_block.to_be_bytes());

        reader.range(&start, &end)
    }

    /// Look up (topic0, topic1) in the composite index.
    pub fn get_topic0_topic1(
        reader: &BTreeIndexReader,
        topic0: &[u8; 32],
        topic1: &[u8; 32],
    ) -> Option<roaring::RoaringBitmap> {
        let mut key = [0u8; TOPIC0_TOPIC1_KEY_SIZE];
        key[..32].copy_from_slice(topic0);
        key[32..].copy_from_slice(topic1);
        reader.get(&key).cloned()
    }

    /// Prefix scan: all rows for a given topic0 across all topic1 values.
    pub fn scan_by_topic0(reader: &BTreeIndexReader, topic0: &[u8; 32]) -> roaring::RoaringBitmap {
        let mut start = [0u8; TOPIC0_TOPIC1_KEY_SIZE];
        start[..32].copy_from_slice(topic0);
        let mut end = [0u8; TOPIC0_TOPIC1_KEY_SIZE];
        end[..32].copy_from_slice(topic0);
        increment_prefix(&mut end[..32]);
        reader.range(&start, &end)
    }
}

/// Increment a big-endian byte slice by 1 (for exclusive upper bound).
/// Wraps around on overflow (all 0xFF becomes all 0x00).
fn increment_prefix(bytes: &mut [u8]) {
    for byte in bytes.iter_mut().rev() {
        let (val, overflow) = byte.overflowing_add(1);
        *byte = val;
        if !overflow {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
                topic1: Some(B256::repeat_byte(0xF1)),
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
                topic1: Some(B256::repeat_byte(0xF2)),
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
            // Row with no topics at all
            LogRow {
                block_number: 300,
                block_hash: B256::repeat_byte(0x03),
                timestamp: 1_700_002_400,
                tx_hash: B256::repeat_byte(0x33),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            },
        ]
    }

    fn setup_partition() -> (TempDir, std::path::PathBuf) {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows();
        ColumnFile::write_batch(&dir, &rows).unwrap();
        CompositeIndexBuilder::build_composite_indexes(&dir).unwrap();
        (tmp, dir)
    }

    #[test]
    fn test_address_topic0_index() {
        let (_tmp, dir) = setup_partition();
        let reader = BTreeIndexReader::open(&dir.join("indexes/address_topic0.bptree")).unwrap();

        // (0xAA, 0xDD) -> row 0
        let bm = CompositeQuery::get_address_topic0(
            &reader,
            &[0xAA; 20],
            B256::repeat_byte(0xDD).as_slice().try_into().unwrap(),
        )
        .unwrap();
        assert!(bm.contains(0));
        assert!(!bm.contains(1));
        assert_eq!(bm.len(), 1);

        // (0xBB, 0xDD) -> row 1
        let bm = CompositeQuery::get_address_topic0(
            &reader,
            &[0xBB; 20],
            B256::repeat_byte(0xDD).as_slice().try_into().unwrap(),
        )
        .unwrap();
        assert!(bm.contains(1));
        assert_eq!(bm.len(), 1);

        // (0xAA, 0xEE) -> row 2
        let bm = CompositeQuery::get_address_topic0(
            &reader,
            &[0xAA; 20],
            B256::repeat_byte(0xEE).as_slice().try_into().unwrap(),
        )
        .unwrap();
        assert!(bm.contains(2));
        assert_eq!(bm.len(), 1);

        // Row 3 has no topic0, should not appear
        assert_eq!(reader.key_count(), 3);
    }

    #[test]
    fn test_address_topic0_prefix_scan() {
        let (_tmp, dir) = setup_partition();
        let reader = BTreeIndexReader::open(&dir.join("indexes/address_topic0.bptree")).unwrap();

        // Scan all rows for address 0xAA -> rows 0, 2
        let bm = CompositeQuery::scan_by_address(&reader, &[0xAA; 20]);
        assert!(bm.contains(0));
        assert!(bm.contains(2));
        assert!(!bm.contains(1));
        assert_eq!(bm.len(), 2);

        // Scan all rows for address 0xBB -> row 1
        let bm = CompositeQuery::scan_by_address(&reader, &[0xBB; 20]);
        assert!(bm.contains(1));
        assert_eq!(bm.len(), 1);
    }

    #[test]
    fn test_address_topic0_block_index() {
        let (_tmp, dir) = setup_partition();
        let reader =
            BTreeIndexReader::open(&dir.join("indexes/address_topic0_block.bptree")).unwrap();

        // Exact lookup: (0xAA, 0xDD, 100) -> row 0
        let bm = CompositeQuery::get_address_topic0_block(
            &reader,
            &[0xAA; 20],
            B256::repeat_byte(0xDD).as_slice().try_into().unwrap(),
            100,
        )
        .unwrap();
        assert!(bm.contains(0));
        assert_eq!(bm.len(), 1);

        // Block range: (0xAA, 0xDD, [100, 300)) -> row 0 only (block 100)
        let bm = CompositeQuery::range_address_topic0_blocks(
            &reader,
            &[0xAA; 20],
            B256::repeat_byte(0xDD).as_slice().try_into().unwrap(),
            100,
            300,
        );
        assert!(bm.contains(0));
        assert_eq!(bm.len(), 1);
    }

    #[test]
    fn test_topic0_topic1_index() {
        let (_tmp, dir) = setup_partition();
        let reader = BTreeIndexReader::open(&dir.join("indexes/topic0_topic1.bptree")).unwrap();

        // Only rows 0 and 1 have both topic0 and topic1
        assert_eq!(reader.key_count(), 2);

        // (0xDD, 0xF1) -> row 0
        let bm = CompositeQuery::get_topic0_topic1(
            &reader,
            B256::repeat_byte(0xDD).as_slice().try_into().unwrap(),
            B256::repeat_byte(0xF1).as_slice().try_into().unwrap(),
        )
        .unwrap();
        assert!(bm.contains(0));
        assert_eq!(bm.len(), 1);

        // (0xDD, 0xF2) -> row 1
        let bm = CompositeQuery::get_topic0_topic1(
            &reader,
            B256::repeat_byte(0xDD).as_slice().try_into().unwrap(),
            B256::repeat_byte(0xF2).as_slice().try_into().unwrap(),
        )
        .unwrap();
        assert!(bm.contains(1));
        assert_eq!(bm.len(), 1);
    }

    #[test]
    fn test_topic0_prefix_scan() {
        let (_tmp, dir) = setup_partition();
        let reader = BTreeIndexReader::open(&dir.join("indexes/topic0_topic1.bptree")).unwrap();

        // All rows with topic0=0xDD (that also have topic1) -> rows 0, 1
        let bm = CompositeQuery::scan_by_topic0(
            &reader,
            B256::repeat_byte(0xDD).as_slice().try_into().unwrap(),
        );
        assert!(bm.contains(0));
        assert!(bm.contains(1));
        assert_eq!(bm.len(), 2);
    }

    #[test]
    fn test_increment_prefix() {
        let mut bytes = [0x00, 0x00, 0xFF];
        increment_prefix(&mut bytes);
        assert_eq!(bytes, [0x00, 0x01, 0x00]);

        let mut bytes = [0xFF, 0xFF];
        increment_prefix(&mut bytes);
        assert_eq!(bytes, [0x00, 0x00]); // overflow wraps
    }
}
