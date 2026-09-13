use std::fs;
use std::path::Path;

use logex_storage::{IndexBuildCheckpoint, IndexReadCheckpoint, SegmentReader};

use crate::btree::BTreeIndex;
use crate::composite::CompositeIndexBuilder;
use crate::index_file::IndexFile;
use crate::transfer_bloom::{ERC20_EVENTS_BLOOM_FILE, Erc20EventBloom};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexBuildProfile {
    All,
    LogQuery,
    Erc20Transfer,
}

/// Builds per-partition indexes from column data.
pub struct IndexBuilder;

impl IndexBuilder {
    pub fn indexes_missing(
        partition_dir: &Path,
        profile: IndexBuildProfile,
    ) -> std::io::Result<bool> {
        let reader = SegmentReader::open_projected(partition_dir, &[])?;
        let Some(checkpoint) = IndexReadCheckpoint::open(partition_dir, &reader)? else {
            return Ok(true);
        };
        let index_dir = partition_dir.join("indexes");
        for name in Self::required_index_files(profile) {
            let Some(expected) = checkpoint.artifact_id(name) else {
                return Ok(true);
            };
            match IndexFile::protected_file_id(&index_dir.join(name)) {
                Ok(actual) if actual == expected => {}
                Ok(_) => return Ok(true),
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound
                            | std::io::ErrorKind::InvalidData
                            | std::io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    return Ok(true);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(false)
    }

    /// Build all indexes (primary + composite) for a partition.
    pub fn build_all_indexes(partition_dir: &Path) -> std::io::Result<()> {
        Self::build_indexes(partition_dir, IndexBuildProfile::All)
    }

    /// Build indexes for a partition using the requested index profile.
    pub fn build_indexes(partition_dir: &Path, profile: IndexBuildProfile) -> std::io::Result<()> {
        let checkpoint = Self::begin_publication(partition_dir)?;
        match profile {
            IndexBuildProfile::All => {
                Self::build_primary_indexes_unpublished(partition_dir)?;
                CompositeIndexBuilder::build_composite_indexes(partition_dir)?;
                let index_dir = partition_dir.join("indexes");
                Erc20EventBloom::build(partition_dir, &index_dir)?;
            }
            IndexBuildProfile::LogQuery => {
                Self::build_log_query_primary_indexes_unpublished(partition_dir)?;
                let index_dir = partition_dir.join("indexes");
                CompositeIndexBuilder::build_log_query_indexes(partition_dir, &index_dir)?;
                Erc20EventBloom::build(partition_dir, &index_dir)?;
            }
            IndexBuildProfile::Erc20Transfer => {
                let index_dir = partition_dir.join("indexes");
                fs::create_dir_all(&index_dir)?;
                Erc20EventBloom::build(partition_dir, &index_dir)?;
            }
        }
        Self::publish(partition_dir, checkpoint)?;
        Ok(())
    }

    /// Build only the missing index files for the requested profile.
    pub fn build_missing_indexes(
        partition_dir: &Path,
        profile: IndexBuildProfile,
    ) -> std::io::Result<()> {
        let checkpoint = Self::begin_publication(partition_dir)?;
        let index_dir = partition_dir.join("indexes");
        fs::create_dir_all(&index_dir)?;

        match profile {
            IndexBuildProfile::All => {
                Self::build_missing_primary_indexes(partition_dir, &index_dir, true)?;
                Self::build_missing_log_query_composites(partition_dir, &index_dir)?;
            }
            IndexBuildProfile::LogQuery => {
                Self::build_missing_primary_indexes(partition_dir, &index_dir, false)?;
                Self::build_missing_log_query_composites(partition_dir, &index_dir)?;
            }
            IndexBuildProfile::Erc20Transfer => {
                Self::build_missing_erc20_transfer_indexes(partition_dir, &index_dir)?;
            }
        }

        Self::publish(partition_dir, checkpoint)?;
        Ok(())
    }

    fn begin_publication(partition_dir: &Path) -> std::io::Result<IndexBuildCheckpoint> {
        let checkpoint = IndexBuildCheckpoint::begin(partition_dir)?;
        // Unpublished, unregistered, replaced, and malformed known artifacts
        // are never adopted into a new publication merely because they exist.
        for name in Self::required_index_files(IndexBuildProfile::All)
            .iter()
            .copied()
            .chain([crate::TRANSFER_BLOOM_FILE])
        {
            let path = partition_dir.join("indexes").join(name);
            let reusable = if !checkpoint.can_reuse_existing() {
                false
            } else if let Some(expected) = checkpoint.previous_artifact_id(name) {
                match IndexFile::protected_file_id(&path) {
                    Ok(actual) => actual == expected,
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::NotFound
                                | std::io::ErrorKind::InvalidData
                                | std::io::ErrorKind::UnexpectedEof
                        ) =>
                    {
                        false
                    }
                    Err(error) => return Err(error),
                }
            } else {
                false
            };
            if !reusable {
                match fs::remove_file(partition_dir.join("indexes").join(name)) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(checkpoint)
    }

    fn publish(partition_dir: &Path, mut checkpoint: IndexBuildCheckpoint) -> std::io::Result<()> {
        let index_dir = partition_dir.join("indexes");
        for name in Self::required_index_files(IndexBuildProfile::All)
            .iter()
            .copied()
            .chain([crate::TRANSFER_BLOOM_FILE])
        {
            let path = index_dir.join(name);
            match IndexFile::protected_file_id(&path) {
                Ok(file_id) => checkpoint.register_artifact(name, file_id)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        checkpoint.publish()
    }

    pub fn required_index_files(profile: IndexBuildProfile) -> &'static [&'static str] {
        match profile {
            IndexBuildProfile::All => &[
                "address.bptree",
                "topic0.bptree",
                "block_number.bptree",
                "timestamp.bptree",
                "block_hash.bptree",
                "address_topic0.bptree",
                "address_topic0_block.bptree",
                "topic0_topic1.bptree",
                "address_topic0_topic1.bptree",
                "address_topic0_topic2.bptree",
                ERC20_EVENTS_BLOOM_FILE,
            ],
            IndexBuildProfile::LogQuery => &[
                "block_number.bptree",
                "timestamp.bptree",
                "address_topic0.bptree",
                "address_topic0_block.bptree",
                "topic0_topic1.bptree",
                "address_topic0_topic1.bptree",
                "address_topic0_topic2.bptree",
                ERC20_EVENTS_BLOOM_FILE,
            ],
            IndexBuildProfile::Erc20Transfer => &[ERC20_EVENTS_BLOOM_FILE],
        }
    }

    /// Build the primary indexes needed by common log-query access paths.
    pub fn build_log_query_primary_indexes(partition_dir: &Path) -> std::io::Result<()> {
        let checkpoint = Self::begin_publication(partition_dir)?;
        Self::build_log_query_primary_indexes_unpublished(partition_dir)?;
        Self::publish(partition_dir, checkpoint)?;
        Ok(())
    }

    fn build_log_query_primary_indexes_unpublished(partition_dir: &Path) -> std::io::Result<()> {
        let index_dir = partition_dir.join("indexes");
        fs::create_dir_all(&index_dir)?;

        Self::build_block_number_index(partition_dir, &index_dir)?;
        Self::build_timestamp_index(partition_dir, &index_dir)?;

        Ok(())
    }

    fn build_missing_primary_indexes(
        partition_dir: &Path,
        index_dir: &Path,
        include_full_primary: bool,
    ) -> std::io::Result<()> {
        if include_full_primary && !index_dir.join("address.bptree").is_file() {
            Self::build_address_index(partition_dir, index_dir)?;
        }
        if include_full_primary && !index_dir.join("topic0.bptree").is_file() {
            Self::build_topic0_index(partition_dir, index_dir)?;
        }
        if !index_dir.join("block_number.bptree").is_file() {
            Self::build_block_number_index(partition_dir, index_dir)?;
        }
        if !index_dir.join("timestamp.bptree").is_file() {
            Self::build_timestamp_index(partition_dir, index_dir)?;
        }
        if include_full_primary && !index_dir.join("block_hash.bptree").is_file() {
            Self::build_block_hash_index(partition_dir, index_dir)?;
        }
        Ok(())
    }

    fn build_missing_log_query_composites(
        partition_dir: &Path,
        index_dir: &Path,
    ) -> std::io::Result<()> {
        if !index_dir.join("address_topic0.bptree").is_file() {
            CompositeIndexBuilder::build_address_topic0(partition_dir, index_dir)?;
        }
        if !index_dir.join("address_topic0_block.bptree").is_file() {
            CompositeIndexBuilder::build_address_topic0_block(partition_dir, index_dir)?;
        }
        if !index_dir.join("topic0_topic1.bptree").is_file() {
            CompositeIndexBuilder::build_topic0_topic1(partition_dir, index_dir)?;
        }
        if !index_dir.join("address_topic0_topic1.bptree").is_file() {
            CompositeIndexBuilder::build_address_topic0_topic1(partition_dir, index_dir)?;
        }
        if !index_dir.join("address_topic0_topic2.bptree").is_file() {
            CompositeIndexBuilder::build_address_topic0_topic2(partition_dir, index_dir)?;
        }
        Self::build_missing_erc20_transfer_indexes(partition_dir, index_dir)
    }

    fn build_missing_erc20_transfer_indexes(
        partition_dir: &Path,
        index_dir: &Path,
    ) -> std::io::Result<()> {
        if !index_dir.join(ERC20_EVENTS_BLOOM_FILE).is_file() {
            Erc20EventBloom::build(partition_dir, index_dir)?;
        }
        Ok(())
    }

    /// Build all primary indexes for a partition and write them to the indexes/ subdirectory.
    pub fn build_primary_indexes(partition_dir: &Path) -> std::io::Result<()> {
        let checkpoint = Self::begin_publication(partition_dir)?;
        Self::build_primary_indexes_unpublished(partition_dir)?;
        Self::publish(partition_dir, checkpoint)?;
        Ok(())
    }

    fn build_primary_indexes_unpublished(partition_dir: &Path) -> std::io::Result<()> {
        let index_dir = partition_dir.join("indexes");
        fs::create_dir_all(&index_dir)?;

        Self::build_address_index(partition_dir, &index_dir)?;
        Self::build_topic0_index(partition_dir, &index_dir)?;
        Self::build_block_number_index(partition_dir, &index_dir)?;
        Self::build_timestamp_index(partition_dir, &index_dir)?;
        Self::build_block_hash_index(partition_dir, &index_dir)?;

        Ok(())
    }

    /// Build address index: Address (20 bytes) -> RoaringBitmap of row IDs.
    fn build_address_index(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let reader = SegmentReader::open_projected(partition_dir, &["address"])?;
        let addresses = reader.read_address(None)?;
        validate_source_rows(&reader, &[("address", addresses.len())])?;
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
        let reader = SegmentReader::open_projected(partition_dir, &["topic0"])?;
        let topics = reader.read_nullable_b256("topic0", None)?;
        validate_source_rows(&reader, &[("topic0", topics.len())])?;
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
        let reader = SegmentReader::open_projected(partition_dir, &["block_number"])?;
        let blocks = reader.read_u64("block_number", None)?;
        validate_source_rows(&reader, &[("block_number", blocks.len())])?;
        let mut index = BTreeIndex::new(8);

        for (row_id, &block) in blocks.iter().enumerate() {
            index.insert(&block.to_be_bytes(), row_id as u32);
        }

        index.write_to_file(&index_dir.join("block_number.bptree"))?;
        tracing::debug!(keys = index.key_count(), "built block_number index");
        Ok(())
    }

    /// Build timestamp index: u64 as big-endian 8 bytes -> RoaringBitmap of row IDs.
    /// Uses big-endian so lexicographic ordering matches numeric ordering (for range scans).
    fn build_timestamp_index(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let reader = SegmentReader::open_projected(partition_dir, &["timestamp"])?;
        let timestamps = reader.read_u64("timestamp", None)?;
        validate_source_rows(&reader, &[("timestamp", timestamps.len())])?;
        let mut index = BTreeIndex::new(8);

        for (row_id, &timestamp) in timestamps.iter().enumerate() {
            index.insert(&timestamp.to_be_bytes(), row_id as u32);
        }

        index.write_to_file(&index_dir.join("timestamp.bptree"))?;
        tracing::debug!(keys = index.key_count(), "built timestamp index");
        Ok(())
    }

    /// Build block_hash index: B256 (32 bytes) -> RoaringBitmap of row IDs.
    fn build_block_hash_index(partition_dir: &Path, index_dir: &Path) -> std::io::Result<()> {
        let reader = SegmentReader::open_projected(partition_dir, &["block_hash"])?;
        let hashes = reader.read_b256("block_hash", None)?;
        validate_source_rows(&reader, &[("block_hash", hashes.len())])?;
        let mut index = BTreeIndex::new(32);

        for (row_id, hash) in hashes.iter().enumerate() {
            index.insert(hash.as_slice(), row_id as u32);
        }

        index.write_to_file(&index_dir.join("block_hash.bptree"))?;
        tracing::debug!(keys = index.key_count(), "built block_hash index");
        Ok(())
    }
}

/// Each builder must cover the same captured row boundary before zipping
/// columns or converting ordinal row positions into the bitmap's u32 IDs.
pub(crate) fn validate_source_rows(
    reader: &SegmentReader,
    columns: &[(&str, usize)],
) -> std::io::Result<()> {
    let rows = reader.read_row_count()?;
    if rows > u64::from(u32::MAX) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "index source exceeds row ID limit",
        ));
    }
    for &(column, length) in columns {
        if length as u64 != rows {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("index source column {column} has {length} rows, expected {rows}"),
            ));
        }
    }
    Ok(())
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
    fn profiles_rebuild_stale_and_incomplete_indexes_before_publication() {
        for profile in [
            IndexBuildProfile::All,
            IndexBuildProfile::LogQuery,
            IndexBuildProfile::Erc20Transfer,
        ] {
            let dir = TempDir::new().unwrap();
            let rows = make_test_rows();
            ColumnFile::write_batch(dir.path(), &rows[..1]).unwrap();
            assert!(IndexBuilder::indexes_missing(dir.path(), profile).unwrap());
            IndexBuilder::build_indexes(dir.path(), profile).unwrap();
            assert!(!IndexBuilder::indexes_missing(dir.path(), profile).unwrap());
            ColumnFile::write_batch(dir.path(), &rows).unwrap();
            assert!(IndexBuilder::indexes_missing(dir.path(), profile).unwrap());
            IndexBuilder::build_missing_indexes(dir.path(), profile).unwrap();
            assert!(!IndexBuilder::indexes_missing(dir.path(), profile).unwrap());
            // A different profile must not resurrect stale files it did not rebuild.
            IndexBuilder::build_missing_indexes(dir.path(), IndexBuildProfile::All).unwrap();
            assert!(!IndexBuilder::indexes_missing(dir.path(), IndexBuildProfile::All).unwrap());
            let checkpoint = IndexBuildCheckpoint::begin(dir.path()).unwrap();
            fs::write(dir.path().join("indexes/address.bptree"), b"partial").unwrap();
            drop(checkpoint);
            assert!(IndexBuilder::indexes_missing(dir.path(), IndexBuildProfile::All).unwrap());
            IndexBuilder::build_missing_indexes(dir.path(), IndexBuildProfile::All).unwrap();
            let reader =
                BTreeIndexReader::open(&dir.path().join("indexes/address.bptree")).unwrap();
            let matches = reader.get(rows[0].address.as_slice()).unwrap();
            assert_eq!(matches.len(), 2);
        }
    }

    #[test]
    fn missing_replaced_and_unregistered_artifacts_are_repaired_before_publication() {
        let dir = TempDir::new().unwrap();
        let rows = make_test_rows();
        ColumnFile::write_batch(dir.path(), &rows).unwrap();
        IndexBuilder::build_all_indexes(dir.path()).unwrap();
        let index_dir = dir.path().join("indexes");

        fs::copy(
            index_dir.join("timestamp.bptree"),
            index_dir.join("block_number.bptree"),
        )
        .unwrap();
        fs::remove_file(index_dir.join("address.bptree")).unwrap();
        assert!(IndexBuilder::indexes_missing(dir.path(), IndexBuildProfile::All).unwrap());
        IndexBuilder::build_missing_indexes(dir.path(), IndexBuildProfile::All).unwrap();
        assert!(!IndexBuilder::indexes_missing(dir.path(), IndexBuildProfile::All).unwrap());
        let blocks = BTreeIndexReader::open(&index_dir.join("block_number.bptree")).unwrap();
        assert_eq!(
            blocks
                .get(&100u64.to_be_bytes())
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let profile_dir = TempDir::new().unwrap();
        ColumnFile::write_batch(profile_dir.path(), &rows).unwrap();
        IndexBuilder::build_indexes(profile_dir.path(), IndexBuildProfile::Erc20Transfer).unwrap();
        let profile_indexes = profile_dir.path().join("indexes");
        let mut unregistered = BTreeIndex::new(20);
        unregistered.insert(Address::repeat_byte(0x44).as_slice(), 0);
        unregistered
            .write_to_file(&profile_indexes.join("address.bptree"))
            .unwrap();
        IndexBuilder::build_missing_indexes(profile_dir.path(), IndexBuildProfile::All).unwrap();
        let addresses = BTreeIndexReader::open(&profile_indexes.join("address.bptree")).unwrap();
        assert!(
            addresses
                .get(Address::repeat_byte(0x44).as_slice())
                .is_none()
        );
        assert_eq!(
            addresses
                .get(Address::repeat_byte(0xAA).as_slice())
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 2]
        );
    }

    #[test]
    fn index_publication_rejects_different_complete_source_column_lengths() {
        let mut accepted = Vec::new();
        for profile in [
            IndexBuildProfile::All,
            IndexBuildProfile::LogQuery,
            IndexBuildProfile::Erc20Transfer,
        ] {
            for different_rows in [2, 4] {
                let dir = TempDir::new().unwrap();
                let replacement = TempDir::new().unwrap();
                let mut rows = make_test_rows();
                ColumnFile::write_batch(dir.path(), &rows).unwrap();
                rows.push(rows[0].clone());
                ColumnFile::write_batch(replacement.path(), &rows[..different_rows]).unwrap();
                // Each replacement file is internally complete. Only its row
                // boundary differs from the address column for this segment.
                for name in ["topic0.col", "topic0.null"] {
                    fs::copy(replacement.path().join(name), dir.path().join(name)).unwrap();
                }
                if IndexBuilder::build_indexes(dir.path(), profile).is_ok() {
                    accepted.push((profile, different_rows));
                } else {
                    let reader = SegmentReader::open_projected(dir.path(), &[]).unwrap();
                    assert!(
                        IndexReadCheckpoint::open(dir.path(), &reader)
                            .unwrap()
                            .is_none()
                    );
                }
            }
        }
        assert!(
            accepted.is_empty(),
            "inconsistent source rows were published: {accepted:?}"
        );
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

        // Verify timestamp index
        let timestamp_idx = BTreeIndexReader::open(&dir.join("indexes/timestamp.bptree")).unwrap();
        assert_eq!(timestamp_idx.key_count(), 2);

        let t0 = timestamp_idx.get(&1_700_000_000u64.to_be_bytes()).unwrap();
        assert!(t0.contains(0));
        assert!(t0.contains(1));
        assert_eq!(t0.len(), 2);

        let t1 = timestamp_idx.get(&1_700_001_200u64.to_be_bytes()).unwrap();
        assert!(t1.contains(2));
        assert_eq!(t1.len(), 1);

        // Verify block_hash index
        let hash_idx = BTreeIndexReader::open(&dir.join("indexes/block_hash.bptree")).unwrap();
        assert_eq!(hash_idx.key_count(), 2);
        let hash_1 = hash_idx.get(B256::repeat_byte(0x01).as_slice()).unwrap();
        assert!(hash_1.contains(0));
        assert!(hash_1.contains(1));
        assert_eq!(hash_1.len(), 2);
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
