//! Disposable writer-backed controls for interruption and retained artifacts.
use super::super::tests::{data_id, fixture, inspect, inspection_limits, tree};
use super::*;
use crate::native::{NativeStorage, NativeStorageConfig, RepairInspection, inspect_repair};
use crate::{IndexBuildCheckpoint, IndexReadCheckpoint, SegmentReader};

const ARTIFACT: &str = "control.index";

fn build(source: &Path, indexes: &Path) -> io::Result<()> {
    let mut publication = IndexBuildCheckpoint::begin_fresh_at(source, indexes)?;
    fs::write(indexes.join(ARTIFACT), b"derived control")?;
    publication.register_artifact(ARTIFACT, [7; 16])?;
    publication.publish()
}

fn verify(source: &Path, indexes: &Path) -> io::Result<()> {
    let reader = SegmentReader::open_for_inspection(source)?;
    let checkpoint = IndexReadCheckpoint::open_at(indexes, &reader)?
        .ok_or_else(|| invalid("missing source-bound control checkpoint"))?;
    if checkpoint.artifact_id(ARTIFACT) != Some([7; 16])
        || fs::read(indexes.join(ARTIFACT))? != b"derived control"
    {
        return Err(invalid("control artifact differs"));
    }
    Ok(())
}

fn start(root: &Path) -> IndexRepair {
    match inspect_repair(root, inspection_limits()).unwrap() {
        RepairInspection::PendingIndexes(pending) => pending.resume(inspection_limits()).unwrap(),
        RepairInspection::Primary(primary) => {
            let id = data_id(&primary);
            primary
                .begin_index_repair(&[id], &[ARTIFACT], inspection_limits(), 0)
                .unwrap()
        }
        RepairInspection::Pending(_) => panic!("unexpected primary repair"),
    }
}

fn source_files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    tree(root)
        .into_iter()
        .filter_map(|(path, (_, bytes))| {
            if path.starts_with("indexes") {
                None
            } else {
                bytes.map(|bytes| (path, bytes))
            }
        })
        .collect()
}

#[test]
fn index_repair_preserves_primary_and_retains_originals_with_one_owner() {
    for bundled in [false, true] {
        for old in [false, true] {
            let (temp, _) = fixture(bundled, 6, true);
            let report = inspect(temp.path());
            let catalog = report.catalog.clone();
            let id = data_id(&report);
            let source = report.paths.segment_dir(id);
            if old {
                fs::create_dir(source.join("indexes")).unwrap();
                fs::write(source.join("indexes/old.index"), b"original derived data").unwrap();
            }
            let primary = source_files(&source);
            let original = old.then(|| tree(&source.join("indexes")));
            let mut repair = report
                .begin_index_repair(&[id], &[ARTIFACT], inspection_limits(), 0)
                .unwrap();
            assert!(crate::native::inspect_primary_data(temp.path(), inspection_limits()).is_err());
            let quarantine = repair.execute(0, build, verify).unwrap();
            assert_eq!(source_files(&source), primary);
            assert_eq!(
                NativeStorageCatalog::load_existing(&repair.inspection.paths).unwrap(),
                catalog
            );
            if let Some(original) = original {
                assert_eq!(tree(&quarantine.join(format!("s_{id:016}"))), original);
            }
            verify(&source, &source.join("indexes")).unwrap();
            let refreshed = repair.into_inspection().unwrap();
            assert!(refreshed.recovery_prerequisites.is_empty());
            drop(refreshed);
            drop(
                NativeStorage::open(NativeStorageConfig {
                    data_dir: temp.path().to_owned(),
                    ..Default::default()
                })
                .unwrap(),
            );
        }
    }
}

#[test]
fn index_repair_interlock_and_partial_build_resume_without_source_mutation() {
    let (temp, _) = fixture(false, 100, false);
    let mut repair = start(temp.path());
    let id = repair.journal.metadata.entries[0].id;
    let source = repair.inspection.paths.segment_dir(id);
    let before = source_files(&source);
    let result = repair.execute(
        0,
        |_, stage| {
            fs::create_dir(stage)?;
            fs::write(stage.join("partial"), b"unfinished index")?;
            Err(io::Error::other("interrupted build"))
        },
        verify,
    );
    assert!(result.is_err());
    assert_eq!(source_files(&source), before);
    let root = operation_root(&repair.inspection.paths, &repair.journal);
    drop(repair);
    let snapshot = tree(temp.path());
    assert!(
        NativeStorage::open(NativeStorageConfig {
            data_dir: temp.path().to_owned(),
            hot_target_rows: 7,
            ..Default::default()
        })
        .is_err()
    );
    assert_eq!(tree(temp.path()), snapshot);
    let RepairInspection::PendingIndexes(pending) =
        inspect_repair(temp.path(), inspection_limits()).unwrap()
    else {
        panic!("missing index repair intent");
    };
    assert_eq!(
        pending.segment_row_counts().collect::<Vec<_>>(),
        vec![(id, 6)]
    );
    let mut repair = pending.resume(inspection_limits()).unwrap();
    repair.execute(0, build, verify).unwrap();
    assert_eq!(source_files(&source), before);
    let retained: Vec<_> = fs::read_dir(root.join("attempts"))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        fs::read(retained[0].path().join("partial")).unwrap(),
        b"unfinished index"
    );
}

#[test]
fn index_repair_preflight_blocks_limits_changed_reports_and_headroom_without_writes() {
    for case in 0..9 {
        let (temp, _) = fixture(false, 100, false);
        let mut report = inspect(temp.path());
        let id = data_id(&report);
        let mut limits = inspection_limits();
        let mut ids = vec![id];
        let long_name = "x".repeat(65);
        let mut artifacts = vec![ARTIFACT];
        let mut space = 0;
        match case {
            0 => limits.max_segment_rows = 0,
            1 => report.catalog.next_segment_id += 1,
            2 => ids.push(id),
            3 => space = u64::MAX,
            4 => ids.clear(),
            5 => ids = vec![id; journal::MAX_ENTRIES + 1],
            6 => artifacts = vec![ARTIFACT; MAX_ARTIFACTS + 1],
            7 => artifacts.clear(),
            8 => artifacts = vec![&long_name],
            _ => unreachable!(),
        }
        let before = tree(temp.path());
        assert!(
            report
                .begin_index_repair(&ids, &artifacts, limits, space)
                .is_err()
        );
        assert_eq!(tree(temp.path()), before, "case {case}");
    }
}

#[test]
fn index_repair_retries_each_persistence_boundary_and_preserves_originals() {
    fn fixture_with_original() -> (tempfile::TempDir, u64, PathBuf) {
        let (temp, _) = fixture(false, 100, false);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let source = report.paths.segment_dir(id);
        fs::create_dir(source.join("indexes")).unwrap();
        fs::write(source.join("indexes/original"), b"retain this artifact").unwrap();
        drop(report);
        (temp, id, source)
    }
    let (temp, _, _) = fixture_with_original();
    let mut repair = start(temp.path());
    durability::inject_failure(usize::MAX);
    repair.execute(0, build, verify).unwrap();
    let count = durability::take_events().len();
    drop(repair);
    for boundary in 0..count {
        let (temp, id, source) = fixture_with_original();
        let primary = source_files(&source);
        let mut repair = start(temp.path());
        let quarantine = repair.quarantine_dir();
        durability::inject_failure(boundary);
        assert!(
            repair.execute(0, build, verify).is_err(),
            "boundary {boundary}"
        );
        durability::take_events();
        drop(repair);
        // A failed final parent barrier can leave no active journal. The
        // completed archive and installed checkpoint then already describe it.
        if temp.path().join(JOURNAL_FILE).exists() {
            let mut resumed = start(temp.path());
            resumed
                .execute(0, build, verify)
                .unwrap_or_else(|error| panic!("boundary {boundary}: {error}"));
        }
        verify(&source, &source.join("indexes")).unwrap();
        assert_eq!(source_files(&source), primary, "boundary {boundary}");
        assert_eq!(
            fs::read(quarantine.join(format!("s_{id:016}/original"))).unwrap(),
            b"retain this artifact",
            "boundary {boundary}"
        );
        assert!(!temp.path().join(JOURNAL_FILE).exists());
    }
}

#[test]
fn index_repair_unreadable_or_conflicting_journals_are_read_only_blockers() {
    for case in 0..3 {
        let (temp, _) = fixture(false, 100, false);
        let repair = start(temp.path());
        let paths = repair.inspection.paths.clone();
        drop(repair);
        match case {
            0 => fs::write(temp.path().join(JOURNAL_FILE), b"incomplete journal").unwrap(),
            1 => fs::write(
                temp.path().join(super::super::journal::JOURNAL_FILE),
                b"another intent",
            )
            .unwrap(),
            2 => {
                let mut catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
                catalog.hot_target_rows += 1;
                catalog.persist(&paths).unwrap();
            }
            _ => unreachable!(),
        }
        let before = tree(temp.path());
        assert!(inspect_repair(temp.path(), inspection_limits()).is_err());
        assert_eq!(tree(temp.path()), before);
    }
}

fn journal_removal_boundary() -> usize {
    let (temp, _) = fixture(false, 100, false);
    let mut repair = start(temp.path());
    durability::inject_failure(usize::MAX);
    repair.execute(0, build, verify).unwrap();
    durability::take_events()
        .iter()
        .position(|(operation, path)| {
            *operation == "remove_file"
                && path.file_name() == Some(std::ffi::OsStr::new(JOURNAL_FILE))
        })
        .unwrap()
}

fn completion_archives(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(root)
        .unwrap()
        .map(Result::unwrap)
        .filter_map(|entry| {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            (name.starts_with("completed-") && name.ends_with(".journal"))
                .then(|| (entry.path(), fs::read(entry.path()).unwrap()))
        })
        .collect()
}

#[test]
fn interrupted_completion_retains_previous_evidence_after_an_installed_index_rebuild() {
    let boundary = journal_removal_boundary();
    let (temp, _) = fixture(false, 100, false);
    let mut repair = start(temp.path());
    let id = repair.journal.metadata.entries[0].id;
    let source = repair.inspection.paths.segment_dir(id);
    let primary = source_files(&source);
    let root = operation_root(&repair.inspection.paths, &repair.journal);
    durability::inject_failure(boundary);
    assert!(repair.execute(0, build, verify).is_err());
    let events = durability::take_events();
    assert_eq!(events.last().unwrap().0, "remove_file");
    assert!(temp.path().join(JOURNAL_FILE).is_file());
    let previous = completion_archives(&root);
    assert_eq!(previous.len(), 1);
    fs::write(
        source.join("indexes").join(ARTIFACT),
        b"damaged after installation",
    )
    .unwrap();
    drop(repair);
    let mut resumed = start(temp.path());
    // A real index rebuild chooses a new opaque artifact identity. Keep that
    // property in this small storage-layer fixture to change the checkpoint.
    let build_next = |source: &Path, indexes: &Path| -> io::Result<()> {
        let mut publication = IndexBuildCheckpoint::begin_fresh_at(source, indexes)?;
        fs::write(indexes.join(ARTIFACT), b"derived control")?;
        publication.register_artifact(ARTIFACT, [8; 16])?;
        publication.publish()
    };
    let verify_next = |source: &Path, indexes: &Path| -> io::Result<()> {
        let reader = SegmentReader::open_for_inspection(source)?;
        let checkpoint = IndexReadCheckpoint::open_at(indexes, &reader)?
            .ok_or_else(|| invalid("missing rebuilt control checkpoint"))?;
        if checkpoint.artifact_id(ARTIFACT) != Some([8; 16])
            || fs::read(indexes.join(ARTIFACT))? != b"derived control"
        {
            return Err(invalid("rebuilt control artifact differs"));
        }
        Ok(())
    };
    resumed.execute(0, build_next, verify_next).unwrap();
    let completed = completion_archives(&root);
    assert_eq!(completed.len(), 2);
    for (path, bytes) in previous {
        assert_eq!(completed.get(&path), Some(&bytes));
    }
    assert_eq!(source_files(&source), primary);
    assert!(!temp.path().join(JOURNAL_FILE).exists());
    let retained: Vec<_> = fs::read_dir(root.join("attempts"))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(retained.len(), 1);
    assert_eq!(
        fs::read(retained[0].path().join(ARTIFACT)).unwrap(),
        b"damaged after installation"
    );
}

#[cfg(unix)]
#[test]
fn resumed_outputs_reject_unexpected_special_paths_without_following_or_changing_them() {
    use std::os::unix::ffi::OsStrExt;
    type Snapshot = std::collections::BTreeMap<
        PathBuf,
        (std::time::SystemTime, Option<Vec<u8>>, Option<PathBuf>),
    >;
    fn snapshot(root: &Path) -> Snapshot {
        fn visit(root: &Path, path: &Path, entries: &mut Snapshot) {
            let metadata = fs::symlink_metadata(path).unwrap();
            entries.insert(
                path.strip_prefix(root).unwrap().to_owned(),
                (
                    metadata.modified().unwrap(),
                    metadata.is_file().then(|| fs::read(path).unwrap()),
                    metadata
                        .file_type()
                        .is_symlink()
                        .then(|| fs::read_link(path).unwrap()),
                ),
            );
            if metadata.is_dir() {
                for entry in fs::read_dir(path).unwrap() {
                    visit(root, &entry.unwrap().path(), entries);
                }
            }
        }
        let mut entries = Snapshot::new();
        visit(root, root, &mut entries);
        entries
    }
    for state in ["unprepared", "prepared", "installed"] {
        for kind in ["symlink", "fifo", "directory"] {
            let (temp, _) = fixture(false, 100, false);
            let mut repair = start(temp.path());
            let id = repair.journal.metadata.entries[0].id;
            let source = repair.inspection.paths.segment_dir(id);
            let stage = repair.stage_path(id);
            if state == "unprepared" {
                build(&source, &stage).unwrap();
            } else {
                repair.prepare(0, &mut build, &mut verify).unwrap();
            }
            if state == "installed" {
                repair.install(0, &mut verify).unwrap();
            }
            let output = if state == "installed" {
                source.join("indexes")
            } else {
                stage
            };
            let extra = output.join("unexpected");
            let external = tempfile::tempdir().unwrap();
            let victim = external.path().join("unrelated");
            fs::write(&victim, b"external data must remain untouched").unwrap();
            match kind {
                "symlink" => std::os::unix::fs::symlink(&victim, &extra).unwrap(),
                "fifo" => {
                    let name = std::ffi::CString::new(extra.as_os_str().as_bytes()).unwrap();
                    // SAFETY: name is a valid NUL-terminated owned pathname in a
                    // disposable fixture; no existing path is replaced by mkfifo.
                    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
                }
                _ => fs::create_dir(&extra).unwrap(),
            }
            let before = snapshot(temp.path());
            let external_before = snapshot(external.path());
            drop(repair);
            let mut resumed = start(temp.path());
            durability::inject_failure(usize::MAX);
            let error = resumed.execute(0, build, verify).unwrap_err();
            let events = durability::take_events();
            assert_eq!(
                error.kind(),
                io::ErrorKind::Unsupported,
                "{state}/{kind}: {error}"
            );
            assert!(
                events
                    .iter()
                    .all(|(_, path)| !path.starts_with(external.path()))
            );
            assert_eq!(snapshot(temp.path()), before);
            assert_eq!(snapshot(external.path()), external_before);
        }
    }
}

#[test]
fn prepared_output_preserves_regular_auxiliary_metadata() {
    let (temp, _) = fixture(false, 100, false);
    let mut repair = start(temp.path());
    let id = repair.journal.metadata.entries[0].id;
    repair.prepare(0, &mut build, &mut verify).unwrap();
    let companion = format!("._{ARTIFACT}");
    let metadata = [0, 5, 22, 7, 0, 2, 0, 0];
    fs::write(repair.stage_path(id).join(&companion), metadata).unwrap();
    repair.execute(0, build, verify).unwrap();
    assert_eq!(
        fs::read(
            repair
                .inspection
                .paths
                .segment_dir(id)
                .join("indexes")
                .join(companion)
        )
        .unwrap(),
        metadata
    );
}
