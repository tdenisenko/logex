use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::catalog::{SegmentDescriptor, SegmentKind, StorageCatalogPaths};
use crate::durability;
use crate::wal::EncodedWalBatch;

const JOURNAL_VERSION: u32 = 1;
const MAX_JOURNAL_BYTES: u64 = 16 * 1024;

/// A WAL batch's origin, persisted before the WAL or any segment is modified.
/// Existing segments between `start.id` and `next_segment_id` can be historical
/// and must never be counted as part of the batch's newly allocated segments.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryJournal {
    version: u32,
    pub(super) start: SegmentDescriptor,
    pub(super) next_segment_id: u64,
    pub(super) row_count: u32,
    pub(super) payload_checksum: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedJournal {
    journal: RecoveryJournal,
    checksum: u32,
}

impl RecoveryJournal {
    pub(super) fn new(
        start: SegmentDescriptor,
        next_segment_id: u64,
        batch: &EncodedWalBatch,
    ) -> io::Result<Self> {
        let journal = Self {
            version: JOURNAL_VERSION,
            start,
            next_segment_id,
            row_count: batch.row_count,
            payload_checksum: batch.checksum,
        };
        journal.validate()?;
        Ok(journal)
    }

    fn validate(&self) -> io::Result<()> {
        if self.version != JOURNAL_VERSION
            || self.start.kind != SegmentKind::Hot
            || self.start.id >= self.next_segment_id
            || self.row_count == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid or unsupported WAL recovery journal",
            ));
        }
        Ok(())
    }

    pub(super) fn path(paths: &StorageCatalogPaths) -> PathBuf {
        paths.root().join("wal/recovery.json")
    }

    pub(super) fn persist(&self, paths: &StorageCatalogPaths) -> io::Result<()> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(io::Error::other)?;
        let checked = CheckedJournal {
            journal: self.clone(),
            checksum: crc32fast::hash(&bytes),
        };
        let bytes = serde_json::to_vec(&checked).map_err(io::Error::other)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL recovery journal exceeds size limit",
            ));
        }
        durability::write_bytes_ordered(&Self::path(paths), &bytes)
    }

    pub(super) fn load(paths: &StorageCatalogPaths) -> io::Result<Option<Self>> {
        let path = Self::path(paths);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        file.take(MAX_JOURNAL_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL recovery journal exceeds size limit",
            ));
        }
        let checked: CheckedJournal = serde_json::from_slice(&bytes).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid recovery journal {}: {error}", path.display()),
            )
        })?;
        let payload = serde_json::to_vec(&checked.journal).map_err(io::Error::other)?;
        if crc32fast::hash(&payload) != checked.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL recovery journal checksum mismatch",
            ));
        }
        checked.journal.validate()?;
        Ok(Some(checked.journal))
    }

    pub(super) fn remove(paths: &StorageCatalogPaths) -> io::Result<()> {
        durability::remove_file(&Self::path(paths))
    }
}

#[cfg(test)]
mod tests {
    use super::super::catalog::{NativeStorageCatalog, NativeStorageConfig};
    use super::*;
    use crate::native::storage::NativeStorage;

    #[test]
    fn valid_checksum_does_not_bypass_journal_version_or_position_checks() {
        let dir = tempfile::tempdir().unwrap();
        let config = NativeStorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..NativeStorageConfig::default()
        };
        let storage = NativeStorage::open(config.clone()).unwrap();
        drop(storage);
        let (catalog, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
        let valid = RecoveryJournal {
            version: JOURNAL_VERSION,
            start: catalog.active_hot_segment().unwrap().clone(),
            next_segment_id: catalog.next_segment_id,
            row_count: 3,
            payload_checksum: 123,
        };
        for damage in 0..4 {
            let mut journal = valid.clone();
            match damage {
                0 => journal.version += 1,
                1 => journal.start.kind = SegmentKind::Sealed,
                2 => journal.next_segment_id = journal.start.id,
                3 => journal.row_count = 0,
                _ => unreachable!(),
            }
            let checksum = crc32fast::hash(&serde_json::to_vec(&journal).unwrap());
            let bytes = serde_json::to_vec(&CheckedJournal { journal, checksum }).unwrap();
            std::fs::write(RecoveryJournal::path(&paths), &bytes).unwrap();
            assert!(RecoveryJournal::load(&paths).is_err(), "damage {damage}");
            assert_eq!(std::fs::read(RecoveryJournal::path(&paths)).unwrap(), bytes);
        }
    }
}
