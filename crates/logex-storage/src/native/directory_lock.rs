use std::fs::{File, TryLockError};
use std::io;
use std::path::Path;

/// Exclusive ownership of an existing data directory, without a lock file or
/// filesystem writes. Normal startup and offline inspection use the same inode
/// lock, including when the directory is reached through another path spelling.
#[derive(Debug)]
pub(super) struct DataDirectoryLock(File);

impl DataDirectoryLock {
    #[cfg(test)]
    pub(super) fn duplicate_file_for_test(&self) -> io::Result<File> {
        self.0.try_clone()
    }

    pub(super) fn acquire_existing(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        if !file.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                format!("data directory {} is not a directory", path.display()),
            ));
        }
        file.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("data directory {} is already in use", path.display()),
            ),
            TryLockError::Error(error) => io::Error::new(
                error.kind(),
                format!("cannot lock data directory {}: {error}", path.display()),
            ),
        })?;
        Ok(Self(file))
    }
}

impl Drop for DataDirectoryLock {
    fn drop(&mut self) {
        // Closing this fd alone can leave a lock alive in a descriptor inherited
        // during another thread's fork/exec. Release it when the last managed
        // storage/compaction owner disappears, regardless of such duplicates.
        if let Err(error) = self.0.unlock() {
            tracing::warn!(%error, "failed to explicitly unlock data directory before closing it");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn existing_lock_never_creates_a_directory_or_sidecar() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("missing");
        assert_eq!(
            DataDirectoryLock::acquire_existing(&missing)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert!(!missing.exists());
        let before = tmp.path().metadata().unwrap().modified().unwrap();
        let owner = DataDirectoryLock::acquire_existing(tmp.path()).unwrap();
        assert!(std::fs::read_dir(tmp.path()).unwrap().next().is_none());
        drop(owner);
        assert_eq!(tmp.path().metadata().unwrap().modified().unwrap(), before);
    }

    #[test]
    fn ownership_conflicts_until_every_shared_owner_is_dropped() {
        let tmp = tempfile::tempdir().unwrap();
        let owner = std::sync::Arc::new(DataDirectoryLock::acquire_existing(tmp.path()).unwrap());
        let retained = std::sync::Arc::clone(&owner);
        drop(owner);
        assert_eq!(
            DataDirectoryLock::acquire_existing(&tmp.path().join("."))
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(retained);
        assert!(DataDirectoryLock::acquire_existing(tmp.path()).is_ok());
    }

    #[test]
    fn a_regular_file_cannot_be_a_data_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("ordinary-file");
        std::fs::write(&path, b"preserved").unwrap();
        assert_eq!(
            DataDirectoryLock::acquire_existing(&path)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotADirectory
        );
        assert_eq!(std::fs::read(path).unwrap(), b"preserved");
    }
}
