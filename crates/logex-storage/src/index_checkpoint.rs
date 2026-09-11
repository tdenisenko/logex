//! Bind derived indexes to the segment state they describe, without ingestion writes.
use std::fs::{self, File, TryLockError};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use alloy_primitives::keccak256;
use serde::{Deserialize, Serialize};

use crate::{BundleReference, SegmentReader, durability};

pub(crate) const INDEX_CHECKPOINT_FILE: &str = "index-checkpoint";
const MAGIC: &[u8; 8] = b"LXICP002";
const MAX_CHECKPOINT_BYTES: usize = 1_024;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    rows: u64,
    generation: u64,
    bundle: Option<BundleReference>,
}

impl Identity {
    fn read(reader: &SegmentReader) -> io::Result<Self> {
        Ok(Self {
            rows: reader.read_row_count()?,
            generation: reader.generation(),
            bundle: reader.bundle_reference().cloned(),
        })
    }
}

struct DirectoryLock(File);

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to release index directory lock");
        }
    }
}

/// Keep a current index set stable while reading its files. If a build is in
/// progress, callers can scan columns immediately instead of waiting for it.
pub struct IndexReadCheckpoint {
    _lock: DirectoryLock,
}

impl IndexReadCheckpoint {
    pub fn open(dir: &Path, reader: &SegmentReader) -> io::Result<Option<Self>> {
        let index_dir = dir.join("indexes");
        let file = match File::open(&index_dir) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        match file.try_lock_shared() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(None),
            Err(TryLockError::Error(error)) => return Err(error),
        }
        let lock = DirectoryLock(file);
        let Some(identity) = read_checkpoint(&index_dir)? else {
            return Ok(None);
        };
        if identity != Identity::read(reader)? {
            return Ok(None);
        }
        Ok(Some(Self { _lock: lock }))
    }
}

/// Serialize index builders and durably withdraw their old publication before
/// any file is overwritten. Publishing requires unchanged source rows/state.
pub struct IndexBuildCheckpoint {
    _lock: DirectoryLock,
    dir: PathBuf,
    identity: Identity,
    reuse_existing: bool,
}

impl IndexBuildCheckpoint {
    pub fn begin(dir: &Path) -> io::Result<Self> {
        let index_dir = dir.join("indexes");
        fs::create_dir_all(&index_dir)?;
        let file = File::open(&index_dir)?;
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::WouldBlock,
                "index directory is in use; retry the build",
            ),
            TryLockError::Error(error) => error,
        })?;
        let lock = DirectoryLock(file);
        let identity = Identity::read(&SegmentReader::open_projected(dir, &[])?)?;
        let previous = match read_checkpoint(&index_dir) {
            Ok(previous) => previous,
            Err(error) if error.kind() == io::ErrorKind::InvalidData => None,
            Err(error) => return Err(error),
        };
        let marker = index_dir.join(INDEX_CHECKPOINT_FILE);
        if marker.try_exists()? {
            durability::remove_file(&marker)?;
        }
        Ok(Self {
            _lock: lock,
            dir: dir.to_owned(),
            reuse_existing: previous.as_ref() == Some(&identity),
            identity,
        })
    }

    pub fn can_reuse_existing(&self) -> bool {
        self.reuse_existing
    }

    /// If ingestion advanced while building, leave indexes unpublished and
    /// report WouldBlock so the caller can retry against the new source state.
    pub fn publish(self) -> io::Result<()> {
        if Identity::read(&SegmentReader::open_projected(&self.dir, &[])?)? != self.identity {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "segment changed while building indexes; retry the build",
            ));
        }
        let payload = serde_json::to_vec(&self.identity).map_err(io::Error::other)?;
        let mut bytes = MAGIC.to_vec();
        bytes.extend_from_slice(keccak256(&payload).as_slice());
        bytes.extend_from_slice(&payload);
        let index_dir = self.dir.join("indexes");
        durability::publish_tree(&index_dir, &index_dir.join(INDEX_CHECKPOINT_FILE), &bytes)?;
        Ok(())
    }
}

fn read_checkpoint(index_dir: &Path) -> io::Result<Option<Identity>> {
    let file = match File::open(index_dir.join(INDEX_CHECKPOINT_FILE)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut bytes = Vec::new();
    file.take(MAX_CHECKPOINT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_CHECKPOINT_BYTES
        || bytes.len() < 40
        || &bytes[..8] != MAGIC
        || keccak256(&bytes[40..]).as_slice() != &bytes[8..40]
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid index checkpoint",
        ));
    }
    serde_json::from_slice(&bytes[40..])
        .map(Some)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ColumnFile;
    use crate::native::{NativeStorage, NativeStorageConfig};
    use alloy_consensus::Header;
    use alloy_primitives::{Address, B256, Bytes};
    use logex_types::{LogRow, Source};

    fn row() -> LogRow {
        LogRow {
            block_number: 100,
            block_hash: B256::repeat_byte(1),
            timestamp: 1_700_000_000,
            tx_hash: B256::repeat_byte(2),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(3),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::new(),
            data_len: 0,
            source: Source::Receipt,
        }
    }

    #[test]
    fn indexes_require_a_current_checkpoint_and_exclude_concurrent_builders() {
        let dir = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(dir.path(), &[row()]).unwrap();
        let reader = SegmentReader::open(dir.path()).unwrap();
        assert!(
            IndexReadCheckpoint::open(dir.path(), &reader)
                .unwrap()
                .is_none()
        );
        assert!(!dir.path().join("indexes").exists());
        let build = IndexBuildCheckpoint::begin(dir.path()).unwrap();
        assert!(!build.can_reuse_existing());
        assert_eq!(
            IndexBuildCheckpoint::begin(dir.path())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(
            IndexReadCheckpoint::open(dir.path(), &reader)
                .unwrap()
                .is_none()
        );
        build.publish().unwrap();
        let first = IndexReadCheckpoint::open(dir.path(), &reader)
            .unwrap()
            .unwrap();
        let second = IndexReadCheckpoint::open(dir.path(), &reader)
            .unwrap()
            .unwrap();
        assert_eq!(
            IndexBuildCheckpoint::begin(dir.path())
                .err()
                .unwrap()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(first);
        assert!(IndexBuildCheckpoint::begin(dir.path()).is_err());
        drop(second);
        let build = IndexBuildCheckpoint::begin(dir.path()).unwrap();
        assert!(build.can_reuse_existing());
        // Dropping an incomplete build cannot restore its former publication.
        drop(build);
        assert!(
            IndexReadCheckpoint::open(dir.path(), &reader)
                .unwrap()
                .is_none()
        );
        let build = IndexBuildCheckpoint::begin(dir.path()).unwrap();
        assert!(!build.can_reuse_existing());
        build.publish().unwrap();
        ColumnFile::write_batch(dir.path(), &[row(), row()]).unwrap();
        let reader = SegmentReader::open(dir.path()).unwrap();
        assert!(
            IndexReadCheckpoint::open(dir.path(), &reader)
                .unwrap()
                .is_none()
        );
        let build = IndexBuildCheckpoint::begin(dir.path()).unwrap();
        ColumnFile::write_batch(dir.path(), &[row(), row(), row()]).unwrap();
        assert_eq!(
            build.publish().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let reader = SegmentReader::open(dir.path()).unwrap();
        assert!(
            IndexReadCheckpoint::open(dir.path(), &reader)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn bundle_index_checkpoint_matches_canonical_state_without_changing_row_count() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        let header = Header {
            number: 100,
            timestamp: 1_700_000_000,
            ..Default::default()
        };
        let mut row = row();
        row.block_hash = header.hash_slow();
        storage
            .ingest_canonical_batch(&[row], &header, std::slice::from_ref(&header), None)
            .unwrap();
        storage.checkpoint_durable().unwrap();
        let path = storage.segment_path(storage.segments()[0].id);
        let snapshot = SegmentReader::open(&path).unwrap();
        IndexBuildCheckpoint::begin(&path)
            .unwrap()
            .publish()
            .unwrap();
        storage.mark_non_canonical(header.hash_slow()).unwrap();
        let current = SegmentReader::open(&path).unwrap();
        assert_eq!(
            snapshot.read_row_count().unwrap(),
            current.read_row_count().unwrap()
        );
        assert!(
            IndexReadCheckpoint::open(&path, &current)
                .unwrap()
                .is_none()
        );
        assert!(
            IndexReadCheckpoint::open(&path, &snapshot)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn malformed_index_checkpoints_are_bounded_and_rebuildable() {
        let dir = tempfile::tempdir().unwrap();
        ColumnFile::write_batch(dir.path(), &[row()]).unwrap();
        IndexBuildCheckpoint::begin(dir.path())
            .unwrap()
            .publish()
            .unwrap();
        let path = dir.path().join("indexes").join(INDEX_CHECKPOINT_FILE);
        let complete = fs::read(&path).unwrap();
        let reader = SegmentReader::open(dir.path()).unwrap();
        for end in 0..complete.len() {
            fs::write(&path, &complete[..end]).unwrap();
            assert_eq!(
                IndexReadCheckpoint::open(dir.path(), &reader)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(fs::read(&path).unwrap(), complete[..end]);
        }
        for position in 0..complete.len() {
            let mut bytes = complete.clone();
            bytes[position] ^= 1;
            fs::write(&path, &bytes).unwrap();
            assert!(IndexReadCheckpoint::open(dir.path(), &reader).is_err());
        }
        fs::write(&path, vec![0; MAX_CHECKPOINT_BYTES + 1]).unwrap();
        assert!(IndexReadCheckpoint::open(dir.path(), &reader).is_err());
        let build = IndexBuildCheckpoint::begin(dir.path()).unwrap();
        assert!(!build.can_reuse_existing());
        build.publish().unwrap();
        assert!(
            IndexReadCheckpoint::open(dir.path(), &reader)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn interrupted_index_rebuild_never_publishes_partial_files() {
        let mut steps = 0;
        for failure in std::iter::once(usize::MAX).chain(0..) {
            if failure != usize::MAX && failure >= steps {
                break;
            }
            let dir = tempfile::tempdir().unwrap();
            ColumnFile::write_batch(dir.path(), &[row()]).unwrap();
            let initial = IndexBuildCheckpoint::begin(dir.path()).unwrap();
            let path = dir.path().join("indexes").join("address.bptree");
            fs::write(&path, b"original complete index").unwrap();
            initial.publish().unwrap();
            durability::inject_failure(failure);
            let result = (|| {
                let build = IndexBuildCheckpoint::begin(dir.path())?;
                durability::checkpoint("test_index_partial_write", &path)?;
                fs::write(&path, b"partial")?;
                durability::checkpoint("test_index_complete_write", &path)?;
                fs::write(&path, b"replacement complete index")?;
                build.publish()
            })();
            let events = durability::take_events();
            if failure == usize::MAX {
                result.unwrap();
                steps = events.len();
                assert!(steps > 0);
            } else {
                assert!(result.is_err(), "{failure}: {events:?}");
            }
            let reader = SegmentReader::open(dir.path()).unwrap();
            if IndexReadCheckpoint::open(dir.path(), &reader)
                .unwrap()
                .is_some()
            {
                assert_ne!(
                    fs::read(&path).unwrap(),
                    b"partial",
                    "{failure}: {events:?}"
                );
            }
            let retry = IndexBuildCheckpoint::begin(dir.path()).unwrap();
            fs::write(&path, b"replacement complete index").unwrap();
            retry.publish().unwrap();
            assert!(
                IndexReadCheckpoint::open(dir.path(), &reader)
                    .unwrap()
                    .is_some()
            );
        }
    }
}
