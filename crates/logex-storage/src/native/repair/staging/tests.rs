//! Disposable storage fixtures; original directories are never replaced.
use super::super::tests::{data_id, fixture, inspect, inspection_limits, limits, tree};
use super::*;
use crate::native::{NativeStorage, NativeStorageConfig};

#[test]
fn staging_preserves_raw_and_bundled_rows_flags_identity_and_original_files() {
    for bundled in [false, true] {
        let (original, rows) = fixture(bundled, if bundled { 6 } else { 100 }, true);
        let before = tree(original.path());
        let report = inspect(original.path());
        let id = data_id(&report);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let mut verifier = plan.begin_candidate(id).unwrap();
        verifier.append(&rows).unwrap();
        let candidate = verifier.finish().unwrap();
        let parent = tempfile::tempdir().unwrap();
        let destination = parent.path().join("replacement");
        let staged = candidate
            .stage(&destination, &rows, inspection_limits())
            .unwrap();
        assert_eq!(
            staged.descriptor.source_namespace,
            candidate.descriptor().source_namespace
        );
        assert_eq!(
            staged.descriptor.source_commitment,
            candidate.descriptor().source_commitment
        );
        assert_eq!(
            staged.descriptor.source_state,
            candidate.descriptor().source_state
        );
        assert_eq!(staged.descriptor.id, id);
        assert_eq!(staged.descriptor.kind, candidate.descriptor().kind);
        assert_eq!(staged.descriptor.generation, 0);
        assert!(staged.descriptor.column_bundle.is_some());
        let reader = SegmentReader::open_for_inspection(&staged.segment_dir()).unwrap();
        let flags = reader.read_canonical().unwrap();
        let ids: Vec<_> = (0..rows.len() as u32).collect();
        let actual: Vec<_> = reader
            .log_row_batches(&ids)
            .unwrap()
            .flat_map(Result::unwrap)
            .collect();
        assert_eq!(actual, rows);
        assert_eq!(
            (0..flags.len())
                .map(|i| flags.is_present(i))
                .collect::<Vec<_>>(),
            vec![false, false, true, true, true, true]
        );
        drop(candidate);
        staged.verify().unwrap();
        assert!(
            NativeStorage::open(NativeStorageConfig {
                data_dir: original.path().to_owned(),
                ..Default::default()
            })
            .is_err()
        );
        assert!(!destination.join("catalog.json").exists());
        assert_eq!(tree(original.path()), before);
        drop(staged);
        assert!(
            destination.exists(),
            "staging retention belongs to the coordinator"
        );
    }
}

#[test]
fn staging_rechecks_detached_input_before_creating_a_destination() {
    let (original, rows) = fixture(false, 100, false);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    for case in 0..3 {
        let mut altered = rows.clone();
        match case {
            0 => {
                altered.pop();
            }
            1 => altered.swap(0, 1),
            _ => altered[0].timestamp += 1,
        }
        let destination = parent.path().join(format!("case-{case}"));
        assert!(
            candidate
                .stage(&destination, &altered, inspection_limits())
                .is_err()
        );
        assert!(!destination.exists());
    }
    assert_eq!(tree(original.path()), before);
}

#[test]
fn staging_refuses_existing_destinations_and_does_not_create_ancestors() {
    let (original, rows) = fixture(false, 100, false);
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    fs::create_dir(parent.path().join("directory")).unwrap();
    fs::write(parent.path().join("directory/retained"), b"existing stage").unwrap();
    fs::write(parent.path().join("file"), b"existing file").unwrap();
    let before = tree(parent.path());
    for name in ["directory", "file", "missing/child"] {
        assert!(
            candidate
                .stage(&parent.path().join(name), &rows, inspection_limits())
                .is_err()
        );
        assert_eq!(tree(parent.path()), before);
    }
}

#[test]
fn staging_read_limits_refuse_work_and_retain_partial_outputs() {
    let (original, rows) = fixture(false, 100, false);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    for case in 0..3 {
        let mut read = inspection_limits();
        match case {
            0 => read.max_segment_rows = rows.len() as u64 - 1,
            1 => read.max_retained_artifact_bytes = 0,
            _ => read.max_decoded_payload_bytes = 0,
        }
        let destination = parent.path().join(format!("case-{case}"));
        assert!(candidate.stage(&destination, &rows, read).is_err());
        assert_eq!(destination.exists(), case != 0);
        if case != 0 {
            let retained = tree(&destination);
            assert!(
                candidate
                    .stage(&destination, &rows, inspection_limits())
                    .is_err()
            );
            assert_eq!(tree(&destination), retained);
        }
    }
    assert_eq!(tree(original.path()), before);
}

#[test]
fn staging_reread_detects_changed_manifest_bounds_and_incomplete_bundle() {
    let (original, rows) = fixture(false, 100, false);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let staged = candidate
        .stage(
            &parent.path().join("replacement"),
            &rows,
            inspection_limits(),
        )
        .unwrap();
    let manifest_path = staged.paths.segment_manifest_path(id);
    let original_manifest = fs::read(&manifest_path).unwrap();
    let mut manifest = staged.manifest.clone();
    manifest.max_block = Some(100);
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(
        staged
            .verify()
            .unwrap_err()
            .to_string()
            .contains("manifest differs")
    );
    fs::write(&manifest_path, original_manifest).unwrap();
    staged.verify().unwrap();
    let bundle = crate::column_artifact::bundle_path(&staged.segment_dir(), 0);
    let file = fs::OpenOptions::new().write(true).open(bundle).unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    assert!(staged.verify().is_err());
    assert_eq!(tree(original.path()), before);
}

#[test]
fn staging_an_empty_original_segment_keeps_its_empty_identity() {
    let original = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: original.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = report.catalog.active_hot_segment.unwrap();
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let candidate = plan.begin_candidate(id).unwrap().finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let staged = candidate
        .stage(&parent.path().join("empty"), &[], inspection_limits())
        .unwrap();
    staged.verify().unwrap();
    assert_eq!(staged.descriptor.row_count, 0);
    assert!(staged.descriptor.min_block.is_none());
    assert!(staged.descriptor.max_block.is_none());
    assert_eq!(tree(original.path()), before);
}
