use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

// Column replacements receive many small fixed-width writes. Buffer them in
// larger chunks; the bounded replacement set retains at most 2 MiB of buffers.
const REPLACEMENT_BUFFER_BYTES: usize = 64 * 1024;

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
/// perform a full sync before promising power-loss durability. Unsupported
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
    let replacement = Replacement::prepare(path, write)?;
    order_file(replacement.writer.get_ref())?;
    replacement.publish()
}

/// Stage the fixed set of column replacements on the existing scoped workers,
/// then order all their contents with one barrier per device before any rename.
/// Publication of the segment still supplies the final durability guarantee.
#[derive(Default)]
pub(crate) struct ReplacementBatch {
    pending: std::sync::Mutex<Vec<Replacement>>,
    publication: Publication,
}

/// Deferred writes must not overwrite catalog-pinned data: they create new files,
/// append immutable bundle extents, or replace derived metadata. Flush the complete
/// affected trees before the catalog can reference them. Ordered replacements
/// preserve a committed prefix; durable publication completes the device flush.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum Publication {
    #[default]
    Durable,
    Ordered,
    Deferred,
}

impl ReplacementBatch {
    pub(crate) fn new(publication: Publication) -> Self {
        Self {
            publication,
            ..Self::default()
        }
    }

    pub(crate) fn write(
        &self,
        path: &Path,
        write: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
    ) -> io::Result<()> {
        let replacement = Replacement::prepare_with_publication(path, write, self.publication)?;
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| io::Error::other("column replacement lock poisoned"))?;
        if pending.len() >= 32 || pending.iter().any(|r| r.destination == path) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid column replacement set",
            ));
        }
        pending.push(replacement);
        Ok(())
    }

    pub(crate) fn publish(self) -> io::Result<()> {
        let pending = self
            .pending
            .into_inner()
            .map_err(|_| io::Error::other("column replacement lock poisoned"))?;
        if self.publication != Publication::Deferred {
            let mut group = SyncGroup::default();
            for replacement in &pending {
                group.include_flushed(
                    replacement.writer.get_ref().try_clone()?,
                    &replacement.destination,
                )?;
            }
            group.order()?;
        }
        for replacement in pending {
            replacement.publish()?;
        }
        Ok(())
    }
}

struct Replacement {
    temporary: Option<std::path::PathBuf>,
    destination: std::path::PathBuf,
    writer: BufWriter<File>,
}

impl Replacement {
    fn prepare(
        path: &Path,
        write: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
    ) -> io::Result<Self> {
        Self::prepare_with_publication(path, write, Publication::Ordered)
    }

    fn prepare_with_publication(
        path: &Path,
        write: impl FnOnce(&mut BufWriter<File>) -> io::Result<()>,
        publication: Publication,
    ) -> io::Result<Self> {
        static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "missing file name"))?;
        if publication == Publication::Deferred {
            // No catalog or earlier reader can reference a file that does not
            // yet exist in this wholly uncommitted segment. Create it directly,
            // exclusively; existing files still use atomic replacement below.
            // A failed first write leaves an uncommitted artifact for rollback.
            checkpoint("write_unpublished", path)?;
            match OpenOptions::new().write(true).create_new(true).open(path) {
                Ok(file) => {
                    let mut replacement = Self {
                        temporary: None,
                        destination: path.to_path_buf(),
                        writer: BufWriter::with_capacity(REPLACEMENT_BUFFER_BYTES, file),
                    };
                    write(&mut replacement.writer)?;
                    replacement.writer.flush()?;
                    return Ok(replacement);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error),
            }
        }
        checkpoint("write_temporary", path)?;
        for _ in 0..32 {
            let mut temporary_name = std::ffi::OsString::from(".");
            temporary_name.push(name);
            temporary_name.push(format!(
                ".{}.{}.tmp",
                std::process::id(),
                NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
            ));
            let temporary = path.with_file_name(temporary_name);
            let file = match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            };
            let mut replacement = Self {
                temporary: Some(temporary),
                destination: path.to_path_buf(),
                writer: BufWriter::with_capacity(REPLACEMENT_BUFFER_BYTES, file),
            };
            write(&mut replacement.writer)?;
            replacement.writer.flush()?;
            if publication != Publication::Deferred {
                checkpoint("sync_temporary", path)?;
                flush_file(replacement.writer.get_ref())?;
            }
            return Ok(replacement);
        }
        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "cannot allocate temporary file for {}",
                name.to_string_lossy()
            ),
        ))
    }

    fn publish(mut self) -> io::Result<()> {
        let Some(temporary) = &self.temporary else {
            // Exclusive first creation in a wholly uncommitted segment.
            return Ok(());
        };
        checkpoint("rename_temporary", &self.destination)?;
        fs::rename(temporary, &self.destination)?;
        self.temporary = None;
        Ok(())
    }
}

impl Drop for Replacement {
    fn drop(&mut self) {
        if let Some(path) = &self.temporary
            && let Err(error) = fs::remove_file(path)
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(path = %path.display(), %error, "could not remove failed temporary write");
        }
    }
}

pub(crate) fn write_bytes(path: &Path, bytes: &[u8]) -> io::Result<()> {
    atomic_write(path, |writer| writer.write_all(bytes))
}

/// Publish a manifest after ordering all its artifacts. Complete the directory
/// changes and persist them together, once per device, before returning success.
pub(crate) fn publish_tree(tree: &Path, manifest: &Path, bytes: &[u8]) -> io::Result<()> {
    prepare_tree_publication(tree, manifest, bytes)?.finish()
}

/// WAL-backed publication can defer the full device flush until checkpoint, but
/// every artifact and name must remain ordered before subsequent publications.
pub(crate) fn publish_tree_ordered(
    tree: &Path,
    manifest: &Path,
    bytes: &[u8],
    catalog: &Path,
) -> io::Result<()> {
    // A later catalog/state write can persist independently of a segment mounted
    // on another device, so that segment needs a full flush before returning.
    prepare_tree_publication(tree, manifest, bytes)?.persist_external_devices(catalog)
}

fn prepare_tree_publication(tree: &Path, manifest: &Path, bytes: &[u8]) -> io::Result<SyncGroup> {
    let mut group = SyncGroup::default();
    flush_tree(tree, &mut group)?;
    let replacement = Replacement::prepare(manifest, |writer| writer.write_all(bytes))?;
    group.include_flushed(replacement.writer.get_ref().try_clone()?, manifest)?;
    group.order_before_manifest(manifest)?;
    replacement.publish()?;
    group.flush_directory(tree)?;
    group.flush_directory(parent(tree))?;
    Ok(group)
}

/// Order journal publication before any WAL or column writes. The WAL's full
/// sync supplies persistence before ingestion proceeds; no commit is acknowledged
/// here. If the journal's rename persists, its contents are already ordered ahead.
pub(crate) fn write_bytes_ordered(path: &Path, bytes: &[u8]) -> io::Result<()> {
    atomic_replace_ordered(path, |writer| writer.write_all(bytes))?;
    checkpoint("order_directory", parent(path))?;
    order_file(&File::open(parent(path))?)
}

pub(crate) fn write_bytes_deferred(path: &Path, bytes: &[u8]) -> io::Result<()> {
    Replacement::prepare_with_publication(
        path,
        |writer| writer.write_all(bytes),
        Publication::Deferred,
    )?
    .publish()
}

/// Order complete new segment trees and the unpublished catalog together before
/// its rename. One barrier covers both on the catalog's device; other devices
/// require their own full flush before that publication can become visible.
pub(crate) fn publish_catalog_after_trees<'a>(
    trees: impl IntoIterator<Item = &'a Path>,
    catalog: &Path,
    bytes: &[u8],
) -> io::Result<()> {
    let mut group = SyncGroup::default();
    for tree in trees {
        flush_tree(tree, &mut group)?;
        group.flush_directory(parent(tree))?;
    }
    let replacement = Replacement::prepare(catalog, |writer| writer.write_all(bytes))?;
    group.include_flushed(replacement.writer.get_ref().try_clone()?, catalog)?;
    group.order_before_manifest(catalog)?;
    replacement.publish()?;
    sync_directory(parent(catalog))
}

/// Order complete ingestion data before its restart marker, and the marker
/// before subsequent writes. Apple barriers preserve a recoverable disk state
/// without promising which complete checkpoint has reached stable media. The
/// caller must bound the entire window since its last full sync and harden it
/// before WAL writes, reorgs or destructive maintenance. External devices are
/// fully synchronized before publishing a catalog that can persist independently.
pub(crate) fn publish_ingestion_catalog_after_trees<'a>(
    trees: impl IntoIterator<Item = &'a Path>,
    catalog: &Path,
    bytes: &[u8],
) -> io::Result<()> {
    let mut group = SyncGroup::default();
    for tree in trees {
        flush_tree(tree, &mut group)?;
        group.flush_directory(parent(tree))?;
    }
    let replacement = Replacement::prepare(catalog, |writer| writer.write_all(bytes))?;
    group.include_flushed(replacement.writer.get_ref().try_clone()?, catalog)?;
    group.order_before_manifest(catalog)?;
    checkpoint("ingestion_data_ordered", catalog)?;
    replacement.publish()?;
    // Order namespace changes before later writes can reuse the old catalog's
    // blocks. Plain directory fsync does not supply that ordering on Apple.
    checkpoint("order_ingestion_catalog_directory", parent(catalog))?;
    order_file(&File::open(parent(catalog))?)
}

/// Persist a just-written file and its directory entry with one device flush.
/// Account for a file symlink pointing to a different device from its parent.
pub(crate) fn sync_file_and_directory(file: &File, path: &Path) -> io::Result<()> {
    let mut group = SyncGroup::default();
    group.flush_handle(file.try_clone()?, path)?;
    group.flush_directory(parent(path))?;
    // OpenOptions::create follows a dangling file symlink. In that case the
    // newly created name belongs to the target's directory, not the alias's.
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        let resolved = fs::canonicalize(path)?;
        if fs::canonicalize(parent(path))? != parent(&resolved) {
            group.flush_directory(parent(&resolved))?;
        }
    }
    group.finish()
}

pub(crate) fn persist_directory_before_file(directory: &Path, file: &File) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::unix::fs::MetadataExt;
        let parent = File::open(directory)?;
        if parent.metadata()?.dev() != file.metadata()?.dev() {
            checkpoint("cross_device_sync", directory)?;
            parent.sync_all()?;
        }
    }
    // On other platforms ordered journal publication already uses full fsync.
    #[cfg(not(target_vendor = "apple"))]
    let _ = (directory, file);
    Ok(())
}

pub(crate) fn order_directories_before_file(paths: &[&Path], file: &File) -> io::Result<()> {
    let mut group = SyncGroup::default();
    for path in paths {
        group.flush_directory(path)?;
    }
    #[cfg(target_vendor = "apple")]
    {
        use std::os::unix::fs::MetadataExt;
        group.order_before_device(file.metadata()?.dev())?;
    }
    #[cfg(not(target_vendor = "apple"))]
    let _ = file;
    Ok(())
}

pub(crate) fn order_file_before_directory(file: &File, directory: &Path) -> io::Result<()> {
    #[cfg(target_vendor = "apple")]
    {
        use std::os::unix::fs::MetadataExt;
        if file.metadata()?.dev() != File::open(directory)?.metadata()?.dev() {
            checkpoint("cross_device_sync", directory)?;
            return file.sync_all();
        }
    }
    #[cfg(not(target_vendor = "apple"))]
    let _ = directory;
    order_file(file)
}

fn flush_tree(path: &Path, group: &mut SyncGroup) -> io::Result<()> {
    flush_tree_inner(path, group, &mut Vec::new())
}

#[cfg(unix)]
type DirectoryIdentity = (u64, u64);
#[cfg(not(unix))]
type DirectoryIdentity = std::path::PathBuf;

fn flush_tree_inner(
    path: &Path,
    group: &mut SyncGroup,
    ancestors: &mut Vec<DirectoryIdentity>,
) -> io::Result<()> {
    #[cfg(unix)]
    let identity = {
        use std::os::unix::fs::MetadataExt;
        let metadata = fs::metadata(path)?;
        (metadata.dev(), metadata.ino())
    };
    #[cfg(not(unix))]
    let identity = fs::canonicalize(path)?;
    // Normal segment layouts have only a few levels. Bound recursion as well
    // as detecting aliases and bind-mount/symlink cycles by directory identity.
    if ancestors.len() >= 64 || ancestors.contains(&identity) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "cyclic or excessively nested storage directory: {}",
                path.display()
            ),
        ));
    }
    ancestors.push(identity);
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() || (file_type.is_symlink() && fs::metadata(entry.path())?.is_dir()) {
            flush_tree_inner(&entry.path(), group, ancestors)?;
        } else {
            checkpoint("sync_file", &entry.path())?;
            group.flush(&entry.path())?;
        }
    }
    checkpoint("sync_directory", path)?;
    group.flush(path)?;
    ancestors.pop();
    Ok(())
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
        self.include_flushed(file, _path)
    }

    fn include_flushed(&mut self, _file: File, _path: &Path) -> io::Result<()> {
        #[cfg(target_vendor = "apple")]
        {
            use std::os::unix::fs::MetadataExt;
            self.devices
                .entry(_file.metadata()?.dev())
                .or_insert_with(|| (_file, _path.to_path_buf()));
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

    fn order_before_manifest(&self, _manifest: &Path) -> io::Result<()> {
        #[cfg(target_vendor = "apple")]
        {
            use std::os::unix::fs::MetadataExt;
            let device = File::open(parent(_manifest))?.metadata()?.dev();
            self.order_before_device(device)?;
        }
        Ok(())
    }

    fn persist_external_devices(&self, _catalog: &Path) -> io::Result<()> {
        #[cfg(target_vendor = "apple")]
        {
            use std::os::unix::fs::MetadataExt;
            let destination = File::open(parent(_catalog))?.metadata()?.dev();
            for (&device, (file, path)) in &self.devices {
                if device != destination {
                    checkpoint("cross_device_sync", path)?;
                    file.sync_all()?;
                }
            }
        }
        // The directory fsyncs already submitted names to the device. The next
        // manifest/catalog ordering barrier, WAL fsync or final checkpoint flush
        // orders/persists them on this same device; another barrier here adds no
        // dependency. Other devices above must complete before that next phase.
        Ok(())
    }

    #[cfg(target_vendor = "apple")]
    fn order_before_device(&self, destination_device: u64) -> io::Result<()> {
        // Ordering barriers do not order another device. Those columns must be
        // fully durable before publishing a manifest that references them.
        for (&device, (file, path)) in &self.devices {
            if device != destination_device {
                checkpoint("cross_device_sync", path)?;
                file.sync_all()?;
            } else {
                checkpoint("order_device", path)?;
                order_file(file)?;
            }
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

    fn assert_only_test_files(dir: &Path, expected: &[&str]) {
        let mut names: std::collections::BTreeSet<_> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        for &name in expected {
            assert!(
                names.remove(std::ffi::OsStr::new(name)),
                "missing {name}: {names:?}"
            );
            #[cfg(target_vendor = "apple")]
            {
                // macOS may store metadata in an AppleDouble companion on
                // ExFAT. Permit only a valid companion of an expected file;
                // leaked replacement names (and their companions) still fail.
                let companion = format!("._{name}");
                if names.remove(std::ffi::OsStr::new(&companion)) {
                    let metadata = fs::read(dir.join(&companion)).unwrap();
                    assert!(
                        metadata.starts_with(&[0, 5, 22, 7, 0, 2, 0, 0]),
                        "invalid companion {companion}"
                    );
                }
            }
        }
        assert!(names.is_empty(), "unexpected files: {names:?}");
    }

    #[test]
    fn deferred_creation_preserves_existing_files_when_writes_fail() {
        let dir = tempfile::tempdir().unwrap();
        let existing = dir.path().join("existing");
        fs::write(&existing, b"previous rows").unwrap();
        let fail = |writer: &mut BufWriter<File>| {
            writer.write_all(b"incomplete")?;
            writer.flush()?;
            Err(io::Error::other("write interrupted"))
        };
        assert!(
            Replacement::prepare_with_publication(&existing, fail, Publication::Deferred).is_err()
        );
        assert_eq!(fs::read(&existing).unwrap(), b"previous rows");

        let new = dir.path().join("new");
        assert!(Replacement::prepare_with_publication(&new, fail, Publication::Deferred).is_err());
        assert_eq!(fs::read(&new).unwrap(), b"incomplete");
        // Recovery must discard this unpublished first-write artifact; no
        // previously published bytes were overwritten by either failure.
        assert_only_test_files(dir.path(), &["existing", "new"]);
    }

    #[cfg(target_vendor = "apple")]
    #[test]
    fn cross_device_dependencies_require_full_sync_before_publication() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("column");
        fs::write(&path, b"complete column").unwrap();
        let mut group = SyncGroup::default();
        group.flush(&path).unwrap();
        let actual_device = fs::metadata(&path).unwrap().dev();
        // Exercise the cross-device branch deterministically without requiring
        // privileged mount operations. Real mount/device failure tests are still
        // needed for platform integration; this checks the publication contract.
        let different_device = actual_device ^ 1;
        inject_failure(usize::MAX);
        group.order_before_device(different_device).unwrap();
        let events = take_events();
        assert_eq!(events, [("cross_device_sync", path.clone())]);
        inject_failure(0);
        let result = group.order_before_device(different_device);
        take_events();
        assert!(result.is_err());
        inject_failure(usize::MAX);
        group.order_before_device(actual_device).unwrap();
        assert_eq!(take_events(), [("order_device", path)]);
    }

    #[test]
    fn column_replacements_are_staged_and_failure_keeps_complete_files() {
        let dir = tempfile::tempdir().unwrap();
        let paths = [dir.path().join("first"), dir.path().join("second")];
        for path in &paths {
            fs::write(path, b"old").unwrap();
        }
        let abandoned = ReplacementBatch::default();
        abandoned.write(&paths[0], |w| w.write_all(b"new")).unwrap();
        assert_eq!(fs::read(&paths[0]).unwrap(), b"old");
        let result = abandoned.write(&paths[1], |w| {
            w.write_all(b"partial")?;
            Err(io::Error::other("writer failed"))
        });
        assert!(result.is_err());
        drop(abandoned);
        assert_only_test_files(dir.path(), &["first", "second"]);
        for path in &paths {
            assert_eq!(fs::read(path).unwrap(), b"old");
        }

        let stage = || {
            let batch = ReplacementBatch::default();
            for path in &paths {
                batch.write(path, |w| w.write_all(b"new")).unwrap();
            }
            batch
        };
        let batch = stage();
        inject_failure(usize::MAX);
        batch.publish().unwrap();
        let events = take_events();
        for failure in 0..events.len() {
            for path in &paths {
                fs::write(path, b"old").unwrap();
            }
            let batch = stage();
            inject_failure(failure);
            let result = batch.publish();
            let observed = take_events();
            assert!(result.is_err(), "{observed:?}");
            for path in &paths {
                let was_renamed = observed[..observed.len() - 1]
                    .iter()
                    .any(|(op, p)| *op == "rename_temporary" && p == path);
                assert_eq!(
                    fs::read(path).unwrap(),
                    if was_renamed { b"new" } else { b"old" }
                );
            }
            assert_only_test_files(dir.path(), &["first", "second"]);
        }
    }

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
            assert_only_test_files(dir.path(), &["column"]);
            write_bytes(&path, b"recovered").unwrap();
            assert_eq!(fs::read(&path).unwrap(), b"recovered");
        }
    }

    #[cfg(unix)]
    #[test]
    fn publication_follows_directory_symlinks_and_rejects_cycles() {
        let dir = tempfile::tempdir().unwrap();
        let tree = dir.path().join("segment");
        let columns = dir.path().join("column-files");
        fs::create_dir(&tree).unwrap();
        fs::create_dir(&columns).unwrap();
        fs::write(columns.join("data.pages"), b"column payload").unwrap();
        std::os::unix::fs::symlink(&columns, tree.join("columns")).unwrap();
        let manifest = tree.join("segment.json");
        inject_failure(usize::MAX);
        publish_tree(&tree, &manifest, b"committed").unwrap();
        let events = take_events();
        let file = tree.join("columns/data.pages");
        let rename = events
            .iter()
            .position(|(operation, path)| *operation == "rename_temporary" && path == &manifest)
            .unwrap();
        assert!(
            events[..rename]
                .iter()
                .any(|(operation, path)| *operation == "sync_file" && path == &file)
        );

        std::os::unix::fs::symlink(&tree, columns.join("cycle")).unwrap();
        assert_eq!(
            publish_tree(&tree, &manifest, b"must not publish")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(&manifest).unwrap(), b"committed");
    }

    #[cfg(unix)]
    #[test]
    fn new_file_through_symlink_syncs_its_actual_parent() {
        let dir = tempfile::tempdir().unwrap();
        let target_dir = dir.path().join("targets");
        fs::create_dir(&target_dir).unwrap();
        let target = target_dir.join("pending.wal");
        let alias = dir.path().join("pending.wal");
        std::os::unix::fs::symlink(&target, &alias).unwrap();
        let mut file = File::create(&alias).unwrap();
        file.write_all(b"durable WAL bytes").unwrap();
        inject_failure(usize::MAX);
        sync_file_and_directory(&file, &alias).unwrap();
        let events = take_events();
        let actual_parent = fs::canonicalize(&target_dir).unwrap();
        assert!(
            events
                .iter()
                .any(|(operation, path)| *operation == "sync_directory" && path == &actual_parent)
        );
        assert_eq!(fs::read(target).unwrap(), b"durable WAL bytes");
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
        assert_only_test_files(dir.path(), &["column"]);
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
