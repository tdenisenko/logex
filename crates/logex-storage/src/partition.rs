use std::fs;
use std::path::{Path, PathBuf};

use logex_types::{LogRow, PartitionMeta};

use crate::column::ColumnFile;
use crate::reader::ColumnReader;
use crate::wal::WriteAheadLog;

/// A single partition — either the writable hot partition or a sealed immutable one.
pub struct Partition {
    pub meta: PartitionMeta,
}

impl Partition {
    /// Create a new empty hot partition.
    pub fn new_hot(id: u64, path: PathBuf) -> Self {
        Self {
            meta: PartitionMeta {
                id,
                min_block: u64::MAX,
                max_block: 0,
                row_count: 0,
                sealed: false,
                path,
            },
        }
    }

    /// Write a batch of rows to this partition.
    pub fn write_batch(&mut self, rows: &[LogRow]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        let existing = self.meta.row_count;
        if existing == 0 {
            ColumnFile::write_batch(&self.meta.path, rows)?;
        } else {
            ColumnFile::append_batch(&self.meta.path, rows, existing)?;
        }

        // Update metadata
        for row in rows {
            if row.block_number < self.meta.min_block {
                self.meta.min_block = row.block_number;
            }
            if row.block_number > self.meta.max_block {
                self.meta.max_block = row.block_number;
            }
        }
        self.meta.row_count += rows.len() as u64;

        Ok(())
    }

    /// Seal this partition: write metadata.json and mark as immutable.
    pub fn seal(&mut self) -> std::io::Result<()> {
        self.meta.sealed = true;
        self.write_metadata()?;
        Ok(())
    }

    /// Write partition metadata to disk.
    pub fn write_metadata(&self) -> std::io::Result<()> {
        fs::create_dir_all(&self.meta.path)?;
        let meta_path = self.meta.path.join("metadata.json");
        let json = serde_json::to_string_pretty(&self.meta)?;
        fs::write(meta_path, json)?;
        Ok(())
    }

    /// Load partition metadata from a directory.
    pub fn load_from_dir(path: &Path) -> std::io::Result<Self> {
        let meta_path = path.join("metadata.json");
        let json = fs::read_to_string(meta_path)?;
        let meta: PartitionMeta = serde_json::from_str(&json)?;
        Ok(Self { meta })
    }
}

/// Configuration for the partition manager.
pub struct PartitionManagerConfig {
    /// Base data directory.
    pub data_dir: PathBuf,
    /// Target row count before sealing a partition.
    pub partition_target_rows: u64,
}

impl Default for PartitionManagerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            partition_target_rows: 50_000_000,
        }
    }
}

/// Manages all partitions: the hot (writable) partition and sealed (immutable) ones.
pub struct PartitionManager {
    config: PartitionManagerConfig,
    sealed_partitions: Vec<Partition>,
    hot_partition: Partition,
    next_partition_id: u64,
    wal: WriteAheadLog,
}

impl PartitionManager {
    /// Open or create a partition manager at the given data directory.
    pub fn open(config: PartitionManagerConfig) -> std::io::Result<Self> {
        let partitions_dir = config.data_dir.join("partitions");
        let wal_dir = config.data_dir.join("wal");
        fs::create_dir_all(&partitions_dir)?;
        fs::create_dir_all(&wal_dir)?;

        let mut sealed_partitions = Vec::new();
        let mut max_id: u64 = 0;

        // Scan for existing sealed partitions
        if partitions_dir.exists() {
            let mut entries: Vec<_> = fs::read_dir(&partitions_dir)?
                .filter_map(|e| e.ok())
                .filter(|e| {
                    e.file_name().to_str().is_some_and(|n| {
                        n.starts_with("p_") && e.path().join("metadata.json").exists()
                    })
                })
                .collect();
            entries.sort_by_key(|e| e.file_name());

            for entry in entries {
                let partition = Partition::load_from_dir(&entry.path())?;
                if partition.meta.id >= max_id {
                    max_id = partition.meta.id + 1;
                }
                if partition.meta.sealed {
                    sealed_partitions.push(partition);
                }
            }
        }

        // Open or create hot partition
        let hot_path = partitions_dir.join("latest");
        let hot_partition = if hot_path.join("metadata.json").exists() {
            let p = Partition::load_from_dir(&hot_path)?;
            if p.meta.id >= max_id {
                max_id = p.meta.id + 1;
            }
            p
        } else {
            Partition::new_hot(max_id, hot_path.clone())
        };
        if hot_partition.meta.id >= max_id {
            max_id = hot_partition.meta.id + 1;
        }

        let wal = WriteAheadLog::open(wal_dir.join("pending.wal"))?;

        let mut manager = Self {
            config,
            sealed_partitions,
            hot_partition,
            next_partition_id: max_id,
            wal,
        };

        // Replay any pending WAL entries
        manager.replay_wal()?;

        Ok(manager)
    }

    /// Ingest a batch of log rows. Writes to WAL first, then to the hot partition.
    pub fn write_batch(&mut self, rows: &[LogRow]) -> std::io::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }

        // Write to WAL first for crash safety
        self.wal.append(rows)?;

        // Write to hot partition
        self.hot_partition.write_batch(rows)?;
        self.hot_partition.write_metadata()?;

        // Truncate WAL after successful write
        self.wal.truncate()?;

        // Check if hot partition should be sealed
        if self.hot_partition.meta.row_count >= self.config.partition_target_rows {
            self.seal_hot_partition()?;
        }

        Ok(())
    }

    /// Seal the current hot partition and create a new one.
    fn seal_hot_partition(&mut self) -> std::io::Result<()> {
        let partitions_dir = self.config.data_dir.join("partitions");

        // Seal the hot partition
        self.hot_partition.seal()?;

        // Move it to a numbered directory
        let sealed_name = format!("p_{:06}", self.hot_partition.meta.id);
        let sealed_path = partitions_dir.join(&sealed_name);
        fs::rename(&self.hot_partition.meta.path, &sealed_path)?;
        self.hot_partition.meta.path = sealed_path;

        let sealed = std::mem::replace(
            &mut self.hot_partition,
            Partition::new_hot(self.next_partition_id, partitions_dir.join("latest")),
        );
        self.next_partition_id += 1;
        self.sealed_partitions.push(sealed);

        tracing::info!(
            partition_id = self.sealed_partitions.last().unwrap().meta.id,
            row_count = self.sealed_partitions.last().unwrap().meta.row_count,
            "sealed partition"
        );

        Ok(())
    }

    /// Replay WAL entries that weren't committed before a crash.
    fn replay_wal(&mut self) -> std::io::Result<()> {
        let rows = self.wal.read_all()?;
        if rows.is_empty() {
            return Ok(());
        }

        tracing::info!(rows = rows.len(), "replaying WAL entries");
        self.hot_partition.write_batch(&rows)?;
        self.hot_partition.write_metadata()?;
        self.wal.truncate()?;

        Ok(())
    }

    /// Mark rows in a given block as non-canonical (during reorg).
    ///
    /// Scans all partitions (sealed + hot) whose block range could contain the
    /// given block hash. For each matching row, flips the canonical bit to 0.
    /// Returns the number of rows marked non-canonical.
    pub fn mark_non_canonical(&self, block_hash: alloy_primitives::B256) -> std::io::Result<u64> {
        let mut total_marked = 0u64;

        let all_partitions = self
            .sealed_partitions
            .iter()
            .chain(std::iter::once(&self.hot_partition));

        for partition in all_partitions {
            if partition.meta.row_count == 0 {
                continue;
            }

            let dir = &partition.meta.path;
            let hashes = ColumnReader::read_b256(dir, "block_hash.col", None)?;
            let mut canonical = ColumnReader::read_canonical(dir)?;
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
                let mut w = std::io::BufWriter::new(file);
                canonical.write_to(&mut w)?;
                std::io::Write::flush(&mut w)?;
                tracing::debug!(
                    partition_id = partition.meta.id,
                    marked = total_marked,
                    block_hash = %block_hash,
                    "marked rows non-canonical"
                );
            }
        }

        Ok(total_marked)
    }

    /// Get the current head block number (highest block in hot partition).
    pub fn head_block(&self) -> Option<u64> {
        if self.hot_partition.meta.row_count > 0 {
            Some(self.hot_partition.meta.max_block)
        } else {
            self.sealed_partitions.last().map(|p| p.meta.max_block)
        }
    }

    /// Total number of rows across all partitions.
    pub fn total_rows(&self) -> u64 {
        let sealed: u64 = self
            .sealed_partitions
            .iter()
            .map(|p| p.meta.row_count)
            .sum();
        sealed + self.hot_partition.meta.row_count
    }

    /// Number of sealed partitions.
    pub fn sealed_count(&self) -> usize {
        self.sealed_partitions.len()
    }

    /// Access to sealed partition metadata (for query planning).
    pub fn sealed_partitions(&self) -> &[Partition] {
        &self.sealed_partitions
    }

    /// Access to the hot partition.
    pub fn hot_partition(&self) -> &Partition {
        &self.hot_partition
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_types::Source;
    use tempfile::TempDir;

    fn make_test_rows(count: usize, start_block: u64) -> Vec<LogRow> {
        (0..count)
            .map(|i| LogRow {
                block_number: start_block + i as u64 / 10,
                block_hash: B256::repeat_byte((i % 256) as u8),
                timestamp: 1_700_000_000 + i as u64 * 12,
                tx_hash: B256::repeat_byte(((i + 1) % 256) as u8),
                tx_index: (i % 100) as u32,
                log_index: i as u32,
                address: Address::repeat_byte((i % 256) as u8),
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
    fn test_partition_write_and_metadata() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test_partition");

        let mut partition = Partition::new_hot(0, path.clone());
        let rows = make_test_rows(100, 1000);
        partition.write_batch(&rows).unwrap();
        partition.write_metadata().unwrap();

        assert_eq!(partition.meta.row_count, 100);
        assert_eq!(partition.meta.min_block, 1000);
        assert_eq!(partition.meta.max_block, 1009);

        // Reload metadata
        let loaded = Partition::load_from_dir(&path).unwrap();
        assert_eq!(loaded.meta.row_count, 100);
        assert_eq!(loaded.meta.min_block, 1000);
    }

    #[test]
    fn test_partition_append() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test_partition");

        let mut partition = Partition::new_hot(0, path);
        let rows1 = make_test_rows(50, 1000);
        partition.write_batch(&rows1).unwrap();

        let rows2 = make_test_rows(50, 2000);
        partition.write_batch(&rows2).unwrap();

        assert_eq!(partition.meta.row_count, 100);
        assert_eq!(partition.meta.min_block, 1000);
        assert_eq!(partition.meta.max_block, 2004);
    }

    #[test]
    fn test_partition_manager_basic() {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1000,
        };

        let mut mgr = PartitionManager::open(config).unwrap();
        assert_eq!(mgr.total_rows(), 0);
        assert_eq!(mgr.sealed_count(), 0);

        let rows = make_test_rows(500, 0);
        mgr.write_batch(&rows).unwrap();
        assert_eq!(mgr.total_rows(), 500);
        assert_eq!(mgr.head_block(), Some(49));
    }

    #[test]
    fn test_partition_manager_sealing() {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 100,
        };

        let mut mgr = PartitionManager::open(config).unwrap();

        // Write enough rows to trigger sealing
        let rows = make_test_rows(150, 0);
        mgr.write_batch(&rows).unwrap();

        assert_eq!(mgr.sealed_count(), 1);
        assert_eq!(mgr.total_rows(), 150);
        assert_eq!(mgr.sealed_partitions()[0].meta.row_count, 150);
    }

    #[test]
    fn test_partition_manager_persistence() {
        let tmp = TempDir::new().unwrap();
        let data_dir = tmp.path().to_path_buf();

        // Write some data
        {
            let config = PartitionManagerConfig {
                data_dir: data_dir.clone(),
                partition_target_rows: 50,
            };
            let mut mgr = PartitionManager::open(config).unwrap();
            let rows = make_test_rows(100, 0);
            mgr.write_batch(&rows).unwrap();
            // Should have sealed one partition
            assert_eq!(mgr.sealed_count(), 1);

            // Write hot partition metadata so it persists
            mgr.hot_partition.write_metadata().unwrap();
        }

        // Reopen and verify
        {
            let config = PartitionManagerConfig {
                data_dir,
                partition_target_rows: 50,
            };
            let mgr = PartitionManager::open(config).unwrap();
            assert_eq!(mgr.sealed_count(), 1);
            assert_eq!(mgr.sealed_partitions()[0].meta.row_count, 100);
        }
    }
}
