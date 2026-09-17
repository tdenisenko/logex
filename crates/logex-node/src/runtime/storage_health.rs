use crate::volume::{MIN_FREE_BYTES, MonitorHandle, StorageMonitor};
use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub(super) enum StorageHealthFailure {
    #[error(
        "disk space below safety threshold at {path}: {free_bytes} free bytes, minimum {min_free_bytes}"
    )]
    LowSpace {
        path: PathBuf,
        free_bytes: u64,
        min_free_bytes: u64,
    },
    #[error("failed to check storage at {path}: {source}")]
    Probe {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
}

/// Called after PartitionManager has initialized both writable roots. Expected
/// volume supervision, when configured, already owns the stronger probe.
pub(super) fn monitor_initialized_storage(
    owner: &mut Option<StorageMonitor>,
    path: &Path,
) -> io::Result<MonitorHandle> {
    let monitor = match owner {
        Some(monitor) => monitor,
        None => {
            let path = path.to_owned();
            owner.insert(StorageMonitor::start_storage(move || {
                check_paths(&path, MIN_FREE_BYTES, free_space_bytes).map_err(io::Error::other)
            })?)
        }
    };
    Ok(monitor.handle())
}

pub(super) async fn wait_for_failure(
    path: PathBuf,
    mut failure: tokio::sync::watch::Receiver<Option<String>>,
) -> StorageHealthFailure {
    let reason = loop {
        if let Some(reason) = failure.borrow_and_update().clone() {
            break reason;
        }
        if failure.changed().await.is_err() {
            break "storage monitor notification channel closed".into();
        }
    };
    StorageHealthFailure::Probe {
        path,
        source: io::Error::other(reason),
    }
}

fn check_paths(
    data_dir: &Path,
    min_free_bytes: u64,
    mut free_space: impl FnMut(&Path) -> io::Result<u64>,
) -> Result<(), StorageHealthFailure> {
    for path in disk_space_probe_paths(data_dir) {
        let free_bytes = free_space(&path).map_err(|source| StorageHealthFailure::Probe {
            path: path.clone(),
            source,
        })?;
        if disk_space_is_low(free_bytes, min_free_bytes) {
            return Err(StorageHealthFailure::LowSpace {
                path,
                free_bytes,
                min_free_bytes,
            });
        }
    }
    Ok(())
}

fn disk_space_probe_paths(data_dir: &Path) -> Vec<PathBuf> {
    let mut probes = BTreeSet::new();
    for path in [data_dir.to_path_buf(), data_dir.join("segments")] {
        // A failed canonicalization must still reach the fallible probe of the
        // configured path. Never substitute an existing ancestor on another disk.
        let path = path.canonicalize().unwrap_or(path);
        probes.insert(path);
    }
    probes.into_iter().collect()
}

fn disk_space_is_low(free_bytes: u64, min_free_bytes: u64) -> bool {
    free_bytes < min_free_bytes
}

#[cfg(unix)]
fn free_space_bytes(path: &Path) -> io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: the NUL-terminated path remains alive; stat points to writable,
    // correctly aligned storage for the target platform's statvfs structure.
    let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: statvfs returned success and initialized the structure.
    let stat = unsafe { stat.assume_init() };
    let free = (stat.f_bavail as u128).saturating_mul(stat.f_frsize as u128);
    Ok(free.min(u64::MAX as u128) as u64)
}

#[cfg(not(unix))]
fn free_space_bytes(_path: &Path) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "free-space reporting is not implemented on this platform",
    ))
}

#[cfg(test)]
mod tests;
