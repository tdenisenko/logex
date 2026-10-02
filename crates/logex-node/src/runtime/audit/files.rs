//! Private bounded audit artifacts and local cancellation markers.
use alloy_primitives::B256;
use logex_sync::history_audit::AuditJobIdentity;
use serde::Serialize;
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};

pub(super) fn session_directory(root: &Path) -> io::Result<Option<PathBuf>> {
    let mut result = None;
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(suffix) = name.strip_prefix("audit-") {
            let valid = suffix.len() == 32 && suffix.bytes().all(|b| b.is_ascii_hexdigit());
            if !valid || result.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "ambiguous audit session directory",
                ));
            }
            ordinary_directory(&entry.path())?;
            AuditJobIdentity::read(&entry.path())?;
            result = Some(entry.path());
        }
    }
    Ok(result)
}
pub(super) fn ordinary_directory(path: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit directory is not an ordinary directory",
        ));
    }
    Ok(())
}
pub(super) fn create_private_directory(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}
pub(super) fn ensure_directory(path: &Path) -> io::Result<()> {
    match create_private_directory(path) {
        Ok(()) => {
            File::open(path)?.sync_all()?;
            File::open(path.parent().unwrap())?.sync_all()?;
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => ordinary_directory(path),
        Err(e) => Err(e),
    }
}
pub(super) fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<T> {
    if !fs::symlink_metadata(path)?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit metadata is not an ordinary file",
        ));
    }
    let file = File::open(path)?;
    let mut bytes = Vec::new();
    file.take(16 * 1024 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > 16 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit metadata exceeds decode allowance",
        ));
    }
    serde_json::from_slice(&bytes).map_err(io::Error::other)
}
pub(super) fn save_json(
    directory: &Path,
    name: &str,
    value: &impl Serialize,
    replace: bool,
) -> io::Result<()> {
    let path = directory.join(name);
    match fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Ok(m) if replace && m.is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "audit artifact already exists or is not a regular file",
            ));
        }
        Err(e) => return Err(e),
    }
    let mut file = logex_fs::StagedFile::new_in(directory, ".audit-report-")?;
    serde_json::to_writer_pretty(file.as_file_mut(), value).map_err(io::Error::other)?;
    file.as_file_mut().write_all(b"\n")?;
    file.as_file().sync_all()?;
    file.persist(&path)?;
    File::open(directory)?.sync_all()
}

pub(super) fn cancel_path(data_dir: &Path, id: B256) -> PathBuf {
    data_dir
        .join("history-audits")
        .join(format!("cancel-{id:x}"))
}
pub(super) fn cancellation_recorded(path: &Path, id: B256) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(e),
        Ok(m) if m.is_file() && m.len() == 32 => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "audit cancellation marker has invalid shape",
            ));
        }
    }
    let mut raw = [0; 32];
    File::open(path)?.read_exact(&mut raw)?;
    if raw != id.0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit cancellation marker belongs to another request",
        ));
    }
    Ok(true)
}
/// A local cancellation request never acquires or modifies execution storage.
/// The stable ID can also cancel an invocation still waiting for healthy sync.
pub(crate) fn cancel_request(data_dir: &Path, id: B256) -> io::Result<()> {
    if id == B256::ZERO {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "audit request ID must be nonzero",
        ));
    }
    ordinary_directory(data_dir)?;
    let path = cancel_path(data_dir, id);
    let parent = path.parent().unwrap();
    ensure_directory(parent)?;
    if cancellation_recorded(&path, id)? {
        return Ok(());
    }
    let mut file = logex_fs::StagedFile::new_in(parent, ".audit-cancel-")?;
    file.as_file_mut().write_all(id.as_slice())?;
    file.as_file().sync_all()?;
    file.persist(&path)?;
    File::open(parent)?.sync_all()
}
