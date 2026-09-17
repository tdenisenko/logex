//! Owned staging artifacts that preserve the caller's path namespace.
//!
//! Relative paths stay relative: converting them through `current_dir` would
//! undo the node's opened-directory anchor after a mounted volume disappears.
//! Callers must keep the working directory fixed for these artifacts' lifetimes,
//! just as for their other relative storage paths. Parent directories must exist
//! and are trusted against concurrent changes by unrelated writers.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Component, Path, PathBuf};

/// An exclusively created private file, removed on drop unless published.
/// Durability barriers are explicit caller responsibilities.
pub struct StagedFile {
    path: Option<PathBuf>,
    file: File,
}

impl StagedFile {
    pub fn new_in(parent: &Path, prefix: &str) -> io::Result<Self> {
        let (path, file) = create_unique(parent, prefix, |path| {
            let mut options = OpenOptions::new();
            options.read(true).write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            options.open(path)
        })?;
        Ok(Self {
            path: Some(path),
            file,
        })
    }

    pub fn as_file(&self) -> &File {
        &self.file
    }

    pub fn as_file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Remove an unpublished artifact and report a failed cleanup.
    pub fn discard(mut self) -> io::Result<()> {
        fs::remove_file(self.path.as_ref().expect("unpublished staging path"))?;
        self.path = None;
        Ok(())
    }

    /// Atomically replace the destination in the same filesystem. On failure,
    /// only this unpublished staging file is removed; the destination is kept.
    pub fn persist(mut self, destination: &Path) -> io::Result<()> {
        fs::rename(
            self.path.as_ref().expect("unpublished staging path"),
            destination,
        )?;
        self.path = None;
        Ok(())
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            // Best-effort removal of our exclusively created scratch entry.
            // Interrupted cleanup may leave an unreferenced staging file.
            let _ = fs::remove_file(path);
        }
    }
}

/// An exclusively created private directory. Drop only removes it if empty:
/// an original artifact moved into it must never be discarded by cleanup.
pub struct StagedDirectory {
    path: Option<PathBuf>,
}

impl StagedDirectory {
    pub fn new_in(parent: &Path, prefix: &str) -> io::Result<Self> {
        let (path, ()) = create_unique(parent, prefix, |path| {
            let mut builder = fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(path)
        })?;
        Ok(Self { path: Some(path) })
    }

    pub fn path(&self) -> &Path {
        self.path.as_deref().expect("unretained staging directory")
    }

    pub fn keep(mut self) -> PathBuf {
        self.path.take().expect("unretained staging directory")
    }
}

impl Drop for StagedDirectory {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = fs::remove_dir(path);
        }
    }
}

fn create_unique<T>(
    parent: &Path,
    prefix: &str,
    mut create: impl FnMut(&Path) -> io::Result<T>,
) -> io::Result<(PathBuf, T)> {
    if prefix.is_empty()
        || prefix.as_bytes().contains(&0)
        || !matches!(
            Path::new(prefix).components().next(),
            Some(Component::Normal(_))
        )
        || Path::new(prefix).components().count() != 1
        || prefix.contains(['/', '\\'])
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid staging prefix",
        ));
    }
    for _ in 0..32 {
        let mut random = [0; 16];
        getrandom::fill(&mut random).map_err(|error| io::Error::other(error.to_string()))?;
        let suffix = u128::from_ne_bytes(random);
        let path = parent.join(format!("{prefix}{suffix:032x}"));
        match create(&path) {
            Ok(value) => return Ok((path, value)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "staging name attempts exhausted",
    ))
}

#[cfg(test)]
mod tests;
