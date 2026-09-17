use super::*;
use std::io::Write;

#[test]
fn file_publish_replaces_complete_contents_and_cleans_only_owned_staging() {
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("state");
    fs::write(&target, b"previous").unwrap();
    fs::write(directory.path().join(".state-unrelated"), b"retained").unwrap();
    let mut staged = StagedFile::new_in(directory.path(), ".state-").unwrap();
    staged.as_file_mut().write_all(b"replacement").unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"previous");
    staged.persist(&target).unwrap();
    assert_eq!(fs::read(&target).unwrap(), b"replacement");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
    assert_eq!(
        fs::read(directory.path().join(".state-unrelated")).unwrap(),
        b"retained"
    );
}

#[test]
fn abandoned_file_is_removed_and_failed_publish_preserves_destination() {
    let directory = tempfile::tempdir().unwrap();
    let staged = StagedFile::new_in(directory.path(), ".state-").unwrap();
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    drop(staged);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    let target = directory.path().join("occupied");
    fs::create_dir(&target).unwrap();
    fs::write(target.join("original"), b"retained").unwrap();
    let staged = StagedFile::new_in(directory.path(), ".state-").unwrap();
    assert!(staged.persist(&target).is_err());
    assert_eq!(fs::read(target.join("original")).unwrap(), b"retained");
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
}

#[test]
fn directory_cleanup_removes_empty_scratch_and_preserves_archived_content() {
    let parent = tempfile::tempdir().unwrap();
    let empty = StagedDirectory::new_in(parent.path(), ".archive-").unwrap();
    let empty_path = empty.path().to_owned();
    drop(empty);
    assert!(!empty_path.exists());
    let occupied = StagedDirectory::new_in(parent.path(), ".archive-").unwrap();
    let retained = occupied.path().join("original");
    fs::write(&retained, b"retained").unwrap();
    drop(occupied);
    assert_eq!(fs::read(retained).unwrap(), b"retained");
    let kept = StagedDirectory::new_in(parent.path(), ".archive-")
        .unwrap()
        .keep();
    assert!(kept.is_dir());
}

#[test]
fn missing_parent_is_never_created() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing");
    assert!(StagedFile::new_in(&missing, ".state-").is_err());
    assert!(StagedDirectory::new_in(&missing, ".archive-").is_err());
    assert!(!missing.exists());
}

#[test]
fn unique_names_do_not_overwrite_other_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let files: Vec<_> = (0..64)
        .map(|_| StagedFile::new_in(directory.path(), ".state-").unwrap())
        .collect();
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), files.len());
    drop(files);
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn invalid_prefixes_cannot_change_the_parent_path() {
    let directory = tempfile::tempdir().unwrap();
    for prefix in [
        "", ".", "..", "../state", "/state", "a/b", "a\\b", "state\0",
    ] {
        assert!(StagedFile::new_in(directory.path(), prefix).is_err());
        assert!(StagedDirectory::new_in(directory.path(), prefix).is_err());
    }
    assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn collisions_are_bounded_and_other_errors_are_not_retried() {
    let mut calls = 0;
    let result = create_unique(Path::new("."), ".state-", |_| -> io::Result<()> {
        calls += 1;
        Err(io::ErrorKind::AlreadyExists.into())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::AlreadyExists);
    assert_eq!(calls, 32);
    calls = 0;
    let result = create_unique(Path::new("."), ".state-", |_| -> io::Result<()> {
        calls += 1;
        Err(io::ErrorKind::PermissionDenied.into())
    });
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
    assert_eq!(calls, 1);
}

#[cfg(unix)]
#[test]
fn staging_permissions_are_private_where_posix_modes_apply() {
    use std::os::unix::fs::PermissionsExt;
    let directory = tempfile::tempdir().unwrap();
    let file = StagedFile::new_in(directory.path(), ".state-").unwrap();
    assert_eq!(
        file.as_file().metadata().unwrap().permissions().mode() & 0o077,
        0
    );
    let dir = StagedDirectory::new_in(directory.path(), ".archive-").unwrap();
    assert_eq!(
        fs::metadata(dir.path()).unwrap().permissions().mode() & 0o077,
        0
    );
}

#[cfg(unix)]
#[test]
fn relative_staging_survives_parent_rename_without_using_replacement_path() {
    let directory = tempfile::tempdir().unwrap();
    let original = directory.path().join("original");
    fs::create_dir(&original).unwrap();
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "tests::relative_paths_child", "--nocapture"])
        .current_dir(&original)
        .env("LOGEX_STAGING_TEST_ROOT", directory.path())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fs::read(original.join("unrelated")).unwrap(), b"retained");
    assert_eq!(fs::read_dir(original).unwrap().count(), 1);
    assert_eq!(
        fs::read(directory.path().join("moved/state")).unwrap(),
        b"replacement"
    );
}

#[cfg(unix)]
#[test]
fn relative_paths_child() {
    let Some(root) = std::env::var_os("LOGEX_STAGING_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mut file = StagedFile::new_in(Path::new("."), ".state-").unwrap();
    assert!(file.path.as_ref().unwrap().is_relative());
    file.as_file_mut().write_all(b"replacement").unwrap();
    let archive = StagedDirectory::new_in(Path::new("."), ".archive-").unwrap();
    assert!(archive.path().is_relative());
    fs::rename(root.join("original"), root.join("moved")).unwrap();
    fs::create_dir(root.join("original")).unwrap();
    fs::write(root.join("original/unrelated"), b"retained").unwrap();
    file.persist(Path::new("state")).unwrap();
    let after_move = StagedFile::new_in(Path::new("."), ".state-").unwrap();
    assert!(after_move.path.as_ref().unwrap().is_relative());
    drop(after_move);
    drop(archive);
    assert_eq!(fs::read_dir(".").unwrap().count(), 1);
}
