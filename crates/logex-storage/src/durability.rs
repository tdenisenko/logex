use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// Flush this file to the device without draining its hardware queue. A later
/// ordering barrier or full sync on the SAME device is required on Apple.
fn flush_file(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        retry_interrupted(|| {
            // SAFETY: the borrowed File keeps its descriptor open throughout
            // this call. fsync takes no pointers and does not consume the fd.
            let result = unsafe { libc::fsync(file.as_raw_fd()) };
            if result == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        })
    }
    #[cfg(not(target_vendor = "apple"))]
    file.sync_all()
}

/// Order the complete replacement before its rename. Apple's barrier orders
/// earlier fsync'd data without waiting for persistence; the caller must still
/// perform a full sync before acknowledging the containing commit. Unsupported
/// filesystems/devices fall back to the stronger full sync, never plain fsync.
pub(crate) fn order_file(file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::fd::AsRawFd;
        order_or_sync(
            || {
                // SAFETY: this command ignores its third argument, takes no pointers,
                // and borrows the valid descriptor for the duration of the call.
                let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_BARRIERFSYNC) };
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            },
            || file.sync_all(),
        )
    }
    #[cfg(not(target_vendor = "apple"))]
    file.sync_all()
}

#[cfg(target_vendor = "apple")]
fn order_or_sync(
    order: impl FnMut() -> io::Result<()>,
    full_sync: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    match retry_interrupted(order) {
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::EINVAL | libc::ENOTSUP | libc::EOPNOTSUPP | libc::ENOTTY)
            ) =>
        {
            full_sync()
        }
        result => result,
    }
}

#[cfg(target_vendor = "apple")]
fn retry_interrupted(mut operation: impl FnMut() -> io::Result<()>) -> io::Result<()> {
    loop {
        match operation() {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => return result,
        }
    }
}

/// Persist a directory entry change as well as the referenced file contents.
/// The pinned standard library uses F_FULLFSYNC on Apple and fsync on Linux.
pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
    checkpoint("sync_directory", path)?;
    File::open(path)?.sync_all()
}

pub(crate) fn create_dir_all(path: &Path) -> io::Result<()> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::metadata(current) {
            Ok(_) => break,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                missing.push(current);
                current = current
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."));
            }
            Err(error) => return Err(error),
        }
    }
    fs::create_dir_all(path)?;
    for dir in missing.into_iter().rev() {
        sync_directory(dir)?;
        sync_directory(
            dir.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )?;
    }
    Ok(())
}

/// Write and synchronize the replacement before publishing its name. Exclusive
/// temporary-file creation avoids following stale symlinks or clobbering another
/// writer's temporary artifact. A crash may leave an unreferenced temporary file.
pub(crate) fn atomic_write(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
) -> io::Result<()> {
    atomic_replace_ordered(path, write)?;
    sync_directory(parent(path))
}

/// Atomically replace a column while ordering its contents before its name.
/// This is NOT a durable commit: the caller must sync the containing tree before
/// publishing its manifest. A failure still leaves a complete old or new file.
pub(crate) fn atomic_replace_ordered(
    path: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
) -> io::Result<()> {
    static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing file name"))?;
    checkpoint("write_temporary", path)?;
    let mut created = None;
    for _ in 0..32 {
        let mut temporary_name = std::ffi::OsString::from(".");
        temporary_name.push(name);
        temporary_name.push(format!(
            ".{}.{}.tmp",
            std::process::id(),
            NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
        ));
        let temporary = path.with_file_name(temporary_name);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
        {
            Ok(file) => {
                created = Some((temporary, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    let (temporary, file) = created.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::AlreadyExists,
            "cannot allocate temporary file",
        )
    })?;
    let result = (|| {
        let mut writer = BufWriter::new(file);
        write(&mut writer)?;
        writer.flush()?;
        checkpoint("sync_temporary", path)?;
        order_file(writer.get_ref())?;
        checkpoint("rename_temporary", path)?;
        fs::rename(&temporary, path)
    })();
    if result.is_err()
        && let Err(error) = fs::remove_file(&temporary)
        && error.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!(path = %temporary.display(), %error, "could not remove failed temporary write");
    }
    result
}

pub(crate) fn write_bytes(path: &Path, bytes: &[u8]) -> io::Result<()> {
    atomic_write(path, |writer| writer.write_all(bytes))
}

/// Publish a manifest after ordering all its artifacts. Complete the directory
/// changes and persist them together, once per device, before returning success.
pub(crate) fn publish_tree(tree: &Path, manifest: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut group = SyncGroup::default();
    flush_tree(tree, &mut group)?;
    group.order()?;
    atomic_replace_ordered(manifest, |writer| writer.write_all(bytes))?;
    group.flush_directory(tree)?;
    group.flush_directory(parent(tree))?;
    group.finish()
}

/// Order journal publication before any WAL or column writes. The WAL's full
/// sync supplies persistence before ingestion proceeds; no commit is acknowledged
/// here. If the journal's rename persists, its contents are already ordered ahead.
pub(crate) fn write_bytes_ordered(path: &Path, bytes: &[u8]) -> io::Result<()> {
    atomic_replace_ordered(path, |writer| writer.write_all(bytes))?;
    checkpoint("order_directory", parent(path))?;
    order_file(&File::open(parent(path))?)
}

/// Persist a just-written file and its directory entry with one device flush.
/// Account for a file symlink pointing to a different device from its parent.
pub(crate) fn sync_file_and_directory(file: &File, path: &Path) -> io::Result<()> {
    let mut group = SyncGroup::default();
    group.flush_handle(file.try_clone()?, path)?;
    group.flush_directory(parent(path))?;
    group.finish()
}

fn flush_tree(path: &Path, group: &mut SyncGroup) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            flush_tree(&entry.path(), group)?;
        } else {
            checkpoint("sync_file", &entry.path())?;
            group.flush(&entry.path())?;
        }
    }
    checkpoint("sync_directory", path)?;
    group.flush(path)
}

/// Apple documents that a full sync persists all previously fsync'd data on the
/// same device. Retain one descriptor per device, including nested mounts and
/// symlink targets; never assume every artifact is on the root's device.
#[derive(Default)]
struct SyncGroup {
    #[cfg(target_vendor = "apple")]
    devices: std::collections::BTreeMap<u64, (File, std::path::PathBuf)>,
}

impl SyncGroup {
    fn flush(&mut self, path: &Path) -> io::Result<()> {
        self.flush_handle(File::open(path)?, path)
    }

    fn flush_directory(&mut self, path: &Path) -> io::Result<()> {
        checkpoint("sync_directory", path)?;
        self.flush(path)
    }

    fn flush_handle(&mut self, file: File, _path: &Path) -> io::Result<()> {
        flush_file(&file)?;
        #[cfg(target_vendor = "apple")]
        {
            use std::os::unix::fs::MetadataExt;
            self.devices
                .entry(file.metadata()?.dev())
                .or_insert_with(|| (file, _path.to_path_buf()));
        }
        Ok(())
    }

    fn order(&self) -> io::Result<()> {
        #[cfg(target_vendor = "apple")]
        for (file, path) in self.devices.values() {
            checkpoint("order_device", path)?;
            order_file(file)?;
        }
        Ok(())
    }

    fn finish(self) -> io::Result<()> {
        #[cfg(target_vendor = "apple")]
        for (file, path) in self.devices.into_values() {
            checkpoint("sync_device", &path)?;
            file.sync_all()?;
        }
        Ok(())
    }
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

pub(crate) fn remove_file(path: &Path) -> io::Result<()> {
    checkpoint("remove_file", path)?;
    fs::remove_file(path)?;
    sync_directory(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
}

#[cfg(not(test))]
#[inline]
pub(crate) fn checkpoint(_operation: &'static str, _path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
struct Failure {
    remaining: usize,
    events: Vec<(&'static str, std::path::PathBuf)>,
}

#[cfg(test)]
thread_local! {
    static FAILURE: std::cell::RefCell<Option<Failure>> = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(crate) fn checkpoint(operation: &'static str, path: &Path) -> io::Result<()> {
    FAILURE.with_borrow_mut(|state| {
        if let Some(Failure { remaining, events }) = state {
            events.push((operation, path.to_path_buf()));
            if *remaining == 0 {
                return Err(io::Error::other(format!(
                    "injected {operation} failure: {}",
                    path.display()
                )));
            }
            *remaining -= 1;
        }
        Ok(())
    })
}

#[cfg(test)]
pub(crate) fn inject_failure(after: usize) {
    FAILURE.with_borrow_mut(|state| {
        *state = Some(Failure {
            remaining: after,
            events: Vec::new(),
        })
    });
}

#[cfg(test)]
pub(crate) fn take_events() -> Vec<(&'static str, std::path::PathBuf)> {
    FAILURE.with_borrow_mut(|state| {
        state
            .take()
            .map(|failure| failure.events)
            .unwrap_or_default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_vendor = "apple")]
    #[test]
    fn ordering_fallback_preserves_io_errors_and_retries_interruptions() {
        let mut attempts = 0;
        order_or_sync(
            || {
                attempts += 1;
                if attempts < 3 {
                    Err(io::Error::from_raw_os_error(libc::EINTR))
                } else {
                    Ok(())
                }
            },
            || panic!("a supported barrier must not need a full flush"),
        )
        .unwrap();
        assert_eq!(attempts, 3);
        for code in [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP, libc::ENOTTY] {
            let mut fallback = false;
            order_or_sync(
                || Err(io::Error::from_raw_os_error(code)),
                || {
                    fallback = true;
                    Ok(())
                },
            )
            .unwrap();
            assert!(fallback);
            let error = order_or_sync(
                || Err(io::Error::from_raw_os_error(code)),
                || Err(io::Error::from_raw_os_error(libc::EIO)),
            )
            .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::EIO));
        }
        for code in [libc::EIO, libc::EBADF, libc::EACCES, libc::ENOSPC] {
            let error = order_or_sync(
                || Err(io::Error::from_raw_os_error(code)),
                || panic!("must not mask a real I/O error"),
            )
            .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(code));
        }
    }

    #[test]
    fn grouped_publication_orders_artifacts_before_manifest_and_persists_names() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("segment");
        let nested = tree.join("columns");
        fs::create_dir_all(&nested).unwrap();
        let artifacts = [
            tree.join("canonical.bitmap"),
            nested.join("address.col"),
            nested.join("data.col"),
        ];
        for path in &artifacts {
            fs::write(path, b"complete column").unwrap();
        }
        let manifest = tree.join("segment.json");
        fs::write(&manifest, b"old manifest").unwrap();
        inject_failure(usize::MAX);
        publish_tree(&tree, &manifest, b"new manifest").unwrap();
        let events = take_events();
        let rename = events
            .iter()
            .position(|(op, path)| *op == "rename_temporary" && path == &manifest)
            .unwrap();
        for path in &artifacts {
            assert!(
                events[..rename]
                    .iter()
                    .any(|(op, p)| *op == "sync_file" && p == path)
            );
        }
        for path in [&tree, dir.path()] {
            assert!(
                events[rename + 1..]
                    .iter()
                    .any(|(op, p)| *op == "sync_directory" && p == path)
            );
        }
        #[cfg(target_vendor = "apple")]
        {
            let ordering = events
                .iter()
                .position(|(op, _)| *op == "order_device")
                .unwrap();
            assert!(ordering < rename);
            // All artifacts are on one device: one final full flush, after every
            // rename/directory flush, must persist the entire publication.
            assert_eq!(
                events.iter().filter(|(op, _)| *op == "sync_device").count(),
                1
            );
            assert_eq!(events.last().unwrap().0, "sync_device");
        }
        for failure in 0..events.len() {
            fs::write(&manifest, b"old manifest").unwrap();
            inject_failure(failure);
            let result = publish_tree(&tree, &manifest, b"new manifest");
            let observed = take_events();
            assert!(result.is_err(), "{observed:?}");
            let expected: &[u8] = if failure <= rename {
                b"old manifest"
            } else {
                b"new manifest"
            };
            assert_eq!(fs::read(&manifest).unwrap(), expected, "{observed:?}");
            for path in &artifacts {
                assert_eq!(fs::read(path).unwrap(), b"complete column");
            }
        }
    }

    #[test]
    fn atomic_replacement_failures_leave_a_complete_old_or_new_file() {
        for failure in 0..4 {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("column");
            fs::write(&path, b"original").unwrap();
            inject_failure(failure);
            let result = write_bytes(&path, b"replacement");
            let events = take_events();
            assert!(result.is_err(), "{events:?}");
            let expected: &[u8] = if failure == 3 {
                b"replacement"
            } else {
                b"original"
            };
            assert_eq!(fs::read(&path).unwrap(), expected);
            assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
            write_bytes(&path, b"recovered").unwrap();
            assert_eq!(fs::read(&path).unwrap(), b"recovered");
        }
    }

    #[test]
    fn partial_write_error_preserves_original_and_removes_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("column");
        fs::write(&path, b"original").unwrap();
        let result = atomic_write(&path, |writer| {
            writer.write_all(b"partial")?;
            Err(io::Error::other("interrupted writer"))
        });
        assert!(result.is_err());
        assert_eq!(fs::read(path).unwrap(), b"original");
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn stale_temporary_symlink_is_never_followed() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("unrelated");
        fs::write(&victim, b"preserved").unwrap();
        std::os::unix::fs::symlink(&victim, dir.path().join(".column.tmp")).unwrap();
        write_bytes(&dir.path().join("column"), b"replacement").unwrap();
        assert_eq!(fs::read(victim).unwrap(), b"preserved");
        assert_eq!(fs::read(dir.path().join("column")).unwrap(), b"replacement");
    }
}
