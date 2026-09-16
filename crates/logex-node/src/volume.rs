//! Optional removable-volume protection for the standalone executable.
//!
//! Enter the verified directory before starting workers, and retain that working
//! directory until process exit. All database paths then remain relative to the
//! opened directory even if its former mount path exposes another filesystem.

use std::fs::File;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, mpsc};
use std::time::Duration;

#[path = "volume/deadline.rs"]
mod deadline;

pub(crate) const MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[cfg(any(target_os = "linux", all(unix, test)))]
#[path = "volume/linux.rs"]
mod linux;
#[cfg(target_os = "macos")]
#[path = "volume/macos.rs"]
mod macos;
#[cfg(any(target_os = "macos", target_os = "linux"))]
#[path = "volume/unix.rs"]
mod unix;

#[derive(Debug)]
pub(crate) struct ExpectedVolume {
    mount: File,
    data: File,
    mount_path: PathBuf,
    data_path: PathBuf,
    uuid: String,
}

impl ExpectedVolume {
    /// Changes the process working directory. Only main, before runtime/worker
    /// creation, may call this. Failure is terminal; never continue startup with
    /// the original absolute data path after this function returns an error.
    pub(crate) fn prepare(
        mount: &Path,
        uuid: &str,
        data: &Path,
        checkpoint: &mut Option<String>,
    ) -> io::Result<Self> {
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            let deadline = deadline::Deadline::start(Duration::from_secs(180))?;
            let result = preserve_checkpoint_path(checkpoint)
                .and_then(|()| unix::prepare(mount, uuid, data, MIN_FREE_BYTES));
            deadline.complete()?;
            result
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = (mount, uuid, data, checkpoint);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "expected-volume protection requires macOS or Linux",
            ))
        }
    }

    pub(crate) fn check(&self) -> io::Result<()> {
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            unix::check(self, MIN_FREE_BYTES)
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "expected-volume protection requires macOS or Linux",
            ))
        }
    }
}

/// Startup and offline commands have no async service supervisor. Keep one owned
/// monitor until the async supervisor takes over, or until maintenance finishes.
/// Interrupted writes retain the same recovery guarantees as process loss.
pub(crate) struct VolumeMonitor {
    stop: Option<mpsc::Sender<()>>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl VolumeMonitor {
    pub(crate) fn start(volume: Arc<ExpectedVolume>) -> io::Result<Self> {
        Self::start_with(
            Duration::from_secs(10),
            Duration::from_secs(10),
            move || volume.check(),
        )
    }

    fn start_with(
        interval: Duration,
        timeout: Duration,
        mut check: impl FnMut() -> io::Result<()> + Send + 'static,
    ) -> io::Result<Self> {
        let (stop, receiver) = mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("volume-monitor".into())
            .spawn(move || {
                loop {
                    match receiver.recv_timeout(interval) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    let deadline = deadline::Deadline::start(timeout)
                        .unwrap_or_else(|_| std::process::exit(1));
                    if let Err(error) = check() {
                        // The independent deadline stays armed even if stderr or
                        // tracing stalls. No further probe is queued after failure.
                        eprintln!("Error: storage volume became unavailable: {error}");
                        std::process::exit(1);
                    }
                    if deadline.complete().is_err() {
                        std::process::exit(1);
                    }
                }
            })?;
        Ok(Self {
            stop: Some(stop),
            worker: Some(worker),
        })
    }
}

impl Drop for VolumeMonitor {
    fn drop(&mut self) {
        // Dropping the sender wakes an idle monitor immediately. An in-flight
        // probe is bounded by its independent deadline before this join starts.
        self.stop.take();
        if self
            .worker
            .take()
            .is_some_and(|worker| worker.join().is_err())
        {
            std::process::exit(1);
        }
    }
}

pub(crate) fn configured(
    mount: Option<PathBuf>,
    uuid: Option<String>,
) -> io::Result<Option<(PathBuf, String)>> {
    match (mount, uuid) {
        (None, None) => Ok(None),
        (Some(mount), Some(uuid)) => {
            Ok(Some((absolute_normalized(&mount)?, normalize_uuid(&uuid)?)))
        }
        _ => Err(invalid(
            "expected-volume-mount and expected-volume-uuid must both be configured",
        )),
    }
}

fn normalize_uuid(uuid: &str) -> io::Result<String> {
    if uuid.is_empty()
        || uuid.len() > 128
        || !uuid
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || byte == b'-')
        || !uuid
            .bytes()
            .any(|byte| byte.is_ascii_hexdigit() && byte != b'0')
    {
        return Err(invalid(
            "expected volume UUID must be a nonzero hexadecimal filesystem UUID",
        ));
    }
    Ok(uuid.to_ascii_lowercase())
}

fn absolute_normalized(path: &Path) -> io::Result<PathBuf> {
    if !path.is_absolute() {
        return Err(invalid(
            "expected-volume mount and data paths must be absolute",
        ));
    }
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => {
                return Err(invalid("expected-volume paths cannot contain '..'"));
            }
            Component::CurDir => {}
            component => result.push(component.as_os_str()),
        }
    }
    Ok(result)
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}

pub(crate) fn preserve_checkpoint_path(checkpoint: &mut Option<String>) -> io::Result<()> {
    if let Some(value) = checkpoint {
        let path = Path::new(value);
        if path.is_relative() && path.try_exists()? {
            *value = std::env::current_dir()?
                .join(path)
                .into_os_string()
                .into_string()
                .map_err(|_| invalid("absolute checkpoint descriptor path is not valid UTF-8"))?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "volume/tests.rs"]
mod tests;
