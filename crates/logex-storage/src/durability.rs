use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

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
        writer.get_ref().sync_all()?;
        checkpoint("rename_temporary", path)?;
        fs::rename(&temporary, path)?;
        sync_directory(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or(Path::new(".")),
        )
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

/// Synchronize a segment's artifacts before publishing a manifest that names
/// them. This also covers compacted columns and derived indexes in subdirectories.
pub(crate) fn sync_tree(path: &Path) -> io::Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            checkpoint("sync_file", &entry.path())?;
            File::open(entry.path())?.sync_all()?;
        }
    }
    sync_directory(path)
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
