//! Bounded private artifacts for explicit local maintenance operations.
use serde::Serialize;
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::Path,
};

fn ordinary_directory(path: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(path)?.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "audit directory is not an ordinary directory",
        ));
    }
    Ok(())
}
pub(in crate::runtime) fn create_private_directory(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)
}
pub(in crate::runtime) fn ensure_directory(path: &Path) -> io::Result<()> {
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
pub(in crate::runtime) fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> io::Result<T> {
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
pub(in crate::runtime) fn save_json(
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn publication_preserves_immutable_requests_and_replaces_progress() {
        let root = tempfile::tempdir().unwrap();
        let request = json!({"request_id": "retained"});
        save_json(root.path(), "request.json", &request, false).unwrap();
        let original = fs::read(root.path().join("request.json")).unwrap();
        assert!(save_json(root.path(), "request.json", &json!({}), false).is_err());
        assert_eq!(
            fs::read(root.path().join("request.json")).unwrap(),
            original
        );
        for n in [1, 2] {
            save_json(root.path(), "progress.json", &json!({"n": n}), true).unwrap();
        }
        assert_eq!(
            read_json::<Value>(&root.path().join("progress.json")).unwrap()["n"],
            2
        );
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn private_metadata_refuses_directory_and_file_aliases() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let directory = root.path().join("metadata");
        symlink(outside.path(), &directory).unwrap();
        assert!(ensure_directory(&directory).is_err());
        assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
        fs::remove_file(&directory).unwrap();
        ensure_directory(&directory).unwrap();
        assert_eq!(
            fs::metadata(&directory).unwrap().permissions().mode() & 0o077,
            0
        );
        let target = outside.path().join("record.json");
        fs::write(&target, b"original").unwrap();
        symlink(&target, directory.join("record.json")).unwrap();
        assert!(save_json(&directory, "record.json", &json!({}), true).is_err());
        assert!(read_json::<Value>(&directory.join("record.json")).is_err());
        assert_eq!(fs::read(target).unwrap(), b"original");
    }

    #[test]
    fn metadata_reads_reject_nonfiles_and_oversized_inputs() {
        let root = tempfile::tempdir().unwrap();
        assert!(read_json::<Value>(root.path()).is_err());
        let path = root.path().join("oversized.json");
        let bytes = vec![b' '; 16 * 1024 + 1];
        fs::write(&path, &bytes).unwrap();
        assert!(read_json::<Value>(&path).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}
