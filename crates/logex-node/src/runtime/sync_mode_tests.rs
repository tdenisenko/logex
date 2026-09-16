use super::sync_mode::{
    MAX_SYNC_MODE_BYTES, RemoveStep, SyncModeState, WriteStep, read_sync_mode_state,
    remove_sync_mode_state, remove_with_checkpoints, sync_mode_state_path, write_sync_mode_state,
    write_with_checkpoints,
};

#[test]
fn absent_storage_is_not_an_absent_mode_marker() {
    let directory = tempfile::tempdir().unwrap();
    assert!(read_sync_mode_state(&directory.path().join("absent-data")).is_err());
}

#[test]
fn mode_write_does_not_recreate_missing_storage() {
    let directory = tempfile::tempdir().unwrap();
    let data = directory.path().join("absent-data");
    let result = write_sync_mode_state(
        &data,
        &SyncModeState {
            historical_sync_disabled: true,
        },
    );
    assert!(result.is_err());
    assert!(!data.exists());
}

#[test]
fn mode_removal_does_not_report_success_for_missing_storage() {
    let directory = tempfile::tempdir().unwrap();
    assert!(remove_sync_mode_state(&directory.path().join("absent-data")).is_err());
}

#[test]
fn oversized_mode_file_is_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let mut contents = br#"{"historical_sync_disabled":true}"#.to_vec();
    contents.resize(4097, b' ');
    std::fs::write(sync_mode_state_path(directory.path()), contents).unwrap();
    assert!(read_sync_mode_state(directory.path()).is_err());
}

#[cfg(unix)]
#[test]
fn mode_write_preserves_an_aliased_fixture() {
    let directory = tempfile::tempdir().unwrap();
    let reference = directory.path().join("reference.json");
    let original = br#"{"historical_sync_disabled":false}"#;
    std::fs::write(&reference, original).unwrap();
    std::os::unix::fs::symlink(&reference, sync_mode_state_path(directory.path())).unwrap();
    let result = write_sync_mode_state(
        directory.path(),
        &SyncModeState {
            historical_sync_disabled: true,
        },
    );
    assert_eq!(std::fs::read(reference).unwrap(), original);
    assert!(result.is_err());
}

#[test]
fn valid_mode_round_trips_and_conversion_removes_it() {
    let directory = tempfile::tempdir().unwrap();
    write_sync_mode_state(
        directory.path(),
        &SyncModeState {
            historical_sync_disabled: true,
        },
    )
    .unwrap();
    assert!(
        read_sync_mode_state(directory.path())
            .unwrap()
            .unwrap()
            .historical_sync_disabled
    );
    remove_sync_mode_state(directory.path()).unwrap();
    assert!(read_sync_mode_state(directory.path()).unwrap().is_none());
}

#[test]
fn initialized_storage_with_no_marker_remains_valid() {
    let directory = tempfile::tempdir().unwrap();
    assert!(read_sync_mode_state(directory.path()).unwrap().is_none());
    remove_sync_mode_state(directory.path()).unwrap();
}

#[test]
fn write_failures_keep_a_complete_old_or_new_state_and_allow_retry() {
    for fail_at in [
        WriteStep::Staged,
        WriteStep::Written,
        WriteStep::FileSynced,
        WriteStep::Replaced,
        WriteStep::DirectorySynced,
    ] {
        let directory = tempfile::tempdir().unwrap();
        write_sync_mode_state(
            directory.path(),
            &SyncModeState {
                historical_sync_disabled: false,
            },
        )
        .unwrap();
        let result = write_with_checkpoints(
            directory.path(),
            &SyncModeState {
                historical_sync_disabled: true,
            },
            |step| {
                if step == fail_at {
                    Err(std::io::Error::other("fixture publication interruption"))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        let published = matches!(fail_at, WriteStep::Replaced | WriteStep::DirectorySynced);
        assert_eq!(
            read_sync_mode_state(directory.path())
                .unwrap()
                .unwrap()
                .historical_sync_disabled,
            published
        );
        // Ordinary returned failures clean only this operation's temporary file.
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        write_sync_mode_state(
            directory.path(),
            &SyncModeState {
                historical_sync_disabled: true,
            },
        )
        .unwrap();
        assert!(
            read_sync_mode_state(directory.path())
                .unwrap()
                .unwrap()
                .historical_sync_disabled
        );
    }
}

#[test]
fn first_publication_failures_do_not_expose_partial_json() {
    for fail_at in [
        WriteStep::Staged,
        WriteStep::Written,
        WriteStep::FileSynced,
        WriteStep::Replaced,
        WriteStep::DirectorySynced,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let result = write_with_checkpoints(
            directory.path(),
            &SyncModeState {
                historical_sync_disabled: true,
            },
            |step| {
                if step == fail_at {
                    Err(std::io::Error::other(
                        "fixture first-publication interruption",
                    ))
                } else {
                    Ok(())
                }
            },
        );
        assert!(result.is_err());
        let state = read_sync_mode_state(directory.path()).unwrap();
        assert_eq!(
            state.is_some(),
            matches!(fail_at, WriteStep::Replaced | WriteStep::DirectorySynced)
        );
        assert!(state.is_none_or(|state| state.historical_sync_disabled));
    }
}

#[test]
fn removal_failures_preserve_errors_and_conversion_is_retryable() {
    for fail_at in [
        RemoveStep::BeforeRemove,
        RemoveStep::Removed,
        RemoveStep::DirectorySynced,
    ] {
        let directory = tempfile::tempdir().unwrap();
        write_sync_mode_state(
            directory.path(),
            &SyncModeState {
                historical_sync_disabled: true,
            },
        )
        .unwrap();
        let result = remove_with_checkpoints(directory.path(), |step| {
            if step == fail_at {
                Err(std::io::Error::other("fixture conversion interruption"))
            } else {
                Ok(())
            }
        });
        assert!(result.is_err());
        assert_eq!(
            read_sync_mode_state(directory.path()).unwrap().is_some(),
            fail_at == RemoveStep::BeforeRemove
        );
        remove_sync_mode_state(directory.path()).unwrap();
        remove_sync_mode_state(directory.path()).unwrap();
        assert!(read_sync_mode_state(directory.path()).unwrap().is_none());
    }
}

#[test]
fn maximum_sized_valid_marker_is_accepted() {
    let directory = tempfile::tempdir().unwrap();
    let mut contents = br#"{"historical_sync_disabled":false}"#.to_vec();
    contents.resize(MAX_SYNC_MODE_BYTES, b' ');
    std::fs::write(sync_mode_state_path(directory.path()), contents).unwrap();
    assert!(
        !read_sync_mode_state(directory.path())
            .unwrap()
            .unwrap()
            .historical_sync_disabled
    );
}

#[test]
fn malformed_marker_is_preserved_and_reported() {
    for contents in [
        b"{".as_slice(),
        b"{}",
        b"{\"historical_sync_disabled\":1}",
        b"{\"historical_sync_disabled\":true,\"unknown\":0}",
        b"\xff",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let path = sync_mode_state_path(directory.path());
        std::fs::write(&path, contents).unwrap();
        assert!(read_sync_mode_state(directory.path()).is_err());
        assert_eq!(std::fs::read(path).unwrap(), contents);
    }
}

#[test]
fn occupied_marker_directory_is_preserved() {
    let directory = tempfile::tempdir().unwrap();
    let path = sync_mode_state_path(directory.path());
    std::fs::create_dir(&path).unwrap();
    assert!(read_sync_mode_state(directory.path()).is_err());
    assert!(
        write_sync_mode_state(
            directory.path(),
            &SyncModeState {
                historical_sync_disabled: true
            }
        )
        .is_err()
    );
    assert!(remove_sync_mode_state(directory.path()).is_err());
    assert!(path.is_dir());
}

#[cfg(unix)]
#[test]
fn marker_alias_is_not_read_or_removed() {
    let directory = tempfile::tempdir().unwrap();
    let reference = directory.path().join("reference.json");
    let contents = br#"{"historical_sync_disabled":true}"#;
    std::fs::write(&reference, contents).unwrap();
    let path = sync_mode_state_path(directory.path());
    std::os::unix::fs::symlink(&reference, &path).unwrap();
    assert!(read_sync_mode_state(directory.path()).is_err());
    assert!(remove_sync_mode_state(directory.path()).is_err());
    assert!(
        std::fs::symlink_metadata(path)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(std::fs::read(reference).unwrap(), contents);
}

#[test]
fn unrelated_staged_artifacts_are_never_promoted_or_removed() {
    let directory = tempfile::tempdir().unwrap();
    let staged = directory.path().join(".sync-mode-unfinished-fixture");
    std::fs::write(&staged, b"{").unwrap();
    assert!(read_sync_mode_state(directory.path()).unwrap().is_none());
    write_sync_mode_state(
        directory.path(),
        &SyncModeState {
            historical_sync_disabled: true,
        },
    )
    .unwrap();
    remove_sync_mode_state(directory.path()).unwrap();
    assert_eq!(std::fs::read(staged).unwrap(), b"{");
}

#[test]
fn actual_publication_error_preserves_the_occupied_destination() {
    let directory = tempfile::tempdir().unwrap();
    let path = sync_mode_state_path(directory.path());
    let result = write_with_checkpoints(
        directory.path(),
        &SyncModeState {
            historical_sync_disabled: true,
        },
        |step| {
            if step == WriteStep::FileSynced {
                // A local fixture makes the actual rename fail after staging.
                std::fs::create_dir(&path)?;
            }
            Ok(())
        },
    );
    assert!(result.is_err());
    assert!(path.is_dir());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
}
