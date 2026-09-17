use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::task::JoinSet;

const POLL_INTERVAL: Duration = Duration::from_secs(10);
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
use crate::volume::MIN_FREE_BYTES;

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

pub(super) async fn wait_for_failure(
    path: PathBuf,
    volume: Option<tokio::sync::watch::Receiver<Option<String>>>,
) -> StorageHealthFailure {
    if let Some(mut volume) = volume {
        let reason = loop {
            if let Some(reason) = volume.borrow_and_update().clone() {
                break reason;
            }
            if volume.changed().await.is_err() {
                break "volume monitor notification channel closed".into();
            }
        };
        return StorageHealthFailure::Probe {
            path,
            source: io::Error::other(reason),
        };
    }
    let mut interval = tokio::time::interval(POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if let Err(failure) = run_probe(path.clone(), PROBE_TIMEOUT, |path| {
            check_paths(path, MIN_FREE_BYTES, free_space_bytes)
        })
        .await
        {
            return failure;
        }
    }
}

pub(super) async fn run_probe(
    path: PathBuf,
    timeout: Duration,
    probe: impl FnOnce(&Path) -> Result<(), StorageHealthFailure> + Send + 'static,
) -> Result<(), StorageHealthFailure> {
    let deadline = tokio::time::Instant::now()
        .checked_add(timeout)
        .ok_or_else(|| StorageHealthFailure::Probe {
            path: path.clone(),
            source: io::Error::new(io::ErrorKind::InvalidInput, "probe timeout is too large"),
        })?;
    // Canonicalization and statvfs can both block on an unavailable filesystem.
    // Keep them off the supervisor's async worker, with only one probe in flight.
    let mut work = JoinSet::new();
    let probe_path = path.clone();
    work.spawn_blocking(move || {
        let result = probe(&probe_path);
        (tokio::time::Instant::now(), result)
    });
    await_probe(path, timeout, deadline, work).await
}

async fn await_probe(
    path: PathBuf,
    timeout: Duration,
    deadline: tokio::time::Instant,
    mut work: JoinSet<(tokio::time::Instant, Result<(), StorageHealthFailure>)>,
) -> Result<(), StorageHealthFailure> {
    let source = match tokio::time::timeout_at(deadline, work.join_next()).await {
        // Timeout polls a ready join before its timer. Check when the worker
        // actually completed so delayed polling cannot accept late success.
        Ok(Some(Ok((completed, result)))) if completed <= deadline => return result,
        Ok(Some(Err(error))) => io::Error::other(error),
        Ok(None) => unreachable!("the health guard owns one probe"),
        Ok(Some(Ok(_))) | Err(_) => io::Error::new(
            io::ErrorKind::TimedOut,
            format!("filesystem probe exceeded {timeout:?}"),
        ),
    };
    // JoinSet cancels queued work on drop. A started blocking syscall cannot be
    // interrupted: return failure without retrying and let bounded node/runtime
    // shutdown cover it. Do not await the timed-out job a second time.
    Err(StorageHealthFailure::Probe { path, source })
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
