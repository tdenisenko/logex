//! Local writer-backed transaction controls. Index validation is exercised by sync.
use super::super::tests::{data_id, fixture, inspect, inspection_limits, limits, tree};
use super::*;
use crate::native::{NativeStorage, NativeStorageConfig};

fn prepared<'a>(plan: &'a RepairOwnershipPlan, rows: &[LogRow]) -> PreparedRepairPublication<'a> {
    let id = plan.segment_ids()[0];
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(rows).unwrap();
    let proof = verifier.finish().unwrap();
    let mut publication = plan.begin_publication(0).unwrap();
    let stage = publication
        .stage_candidate(&proof, rows, inspection_limits())
        .unwrap();
    publication.prepare(vec![stage], |_| Ok(())).unwrap()
}
fn finish_pending(root: &Path, rows: &[LogRow]) {
    if let Some(pending) = inspect_pending_repair(root).unwrap() {
        match pending.state() {
            RepairCatalogState::BeforePublication => {
                let plan = pending.into_plan(inspection_limits(), limits()).unwrap();
                prepared(&plan, rows)
                    .commit()
                    .unwrap()
                    .finish(inspection_limits(), |_| Ok(()))
                    .unwrap();
            }
            RepairCatalogState::AfterPublication => {
                pending.finish(inspection_limits(), |_| Ok(())).unwrap();
            }
        }
    }
}
fn verify_replacement(root: &Path, rows: &[LogRow], original: &SegmentDescriptor) {
    let report = inspect(root);
    let descriptor = report
        .catalog
        .segments
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap();
    assert_ne!(descriptor.id, original.id);
    assert_eq!(descriptor.kind, original.kind);
    assert_eq!(descriptor.source_namespace, original.source_namespace);
    assert_eq!(descriptor.source_commitment, original.source_commitment);
    assert_eq!(descriptor.source_state, original.source_state);
    let dir = StorageCatalogPaths::new(root.to_owned()).segment_dir(descriptor.id);
    let reader = SegmentReader::open_for_inspection(&dir).unwrap();
    let ids: Vec<_> = (0..rows.len() as u32).collect();
    let actual: Vec<_> = reader
        .log_row_batches(&ids)
        .unwrap()
        .flat_map(Result::unwrap)
        .collect();
    assert_eq!(actual, rows);
    let flags = reader.read_canonical().unwrap();
    assert_eq!(
        (0..flags.len())
            .map(|i| flags.is_present(i))
            .collect::<Vec<_>>(),
        vec![false, false, true, true, true, true]
    );
    assert!(!root.join(JOURNAL_FILE).exists());
    drop(report);
    drop(
        NativeStorage::open(NativeStorageConfig {
            data_dir: root.to_owned(),
            hot_target_rows: 100,
            ..Default::default()
        })
        .unwrap(),
    );
}

#[test]
fn publication_replaces_hot_and_historical_with_fresh_ids_and_preserves_state() {
    for bundled in [false, true] {
        let (temp, rows) = fixture(bundled, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let before = report.catalog.clone();
        let original = before.segments.iter().find(|s| s.id == id).unwrap().clone();
        let original_dir = StorageCatalogPaths::new(temp.path().to_owned()).segment_dir(id);
        let original_tree = tree(&original_dir);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let committed = prepared(&plan, &rows).commit().unwrap();
        assert_eq!(tree(&original_dir), original_tree);
        let after = NativeStorageCatalog::load_existing(&plan.inspection.paths).unwrap();
        assert_eq!(after.state, before.state);
        assert_eq!(after.anchors, before.anchors);
        let new_id = before.next_segment_id;
        assert_eq!(after.next_segment_id, new_id + 1);
        if before.active_hot_segment == Some(id) {
            assert_eq!(after.active_hot_segment, Some(new_id));
        }
        if before.active_historical_segment == Some(id) {
            assert_eq!(after.active_historical_segment, Some(new_id));
        }
        let quarantine = committed.finish(inspection_limits(), |_| Ok(())).unwrap();
        assert!(!original_dir.exists());
        assert_eq!(tree(&quarantine.join(format!("s_{id:016}"))), original_tree);
        drop(plan);
        verify_replacement(temp.path(), &rows, &original);
    }
}

#[test]
fn pending_intent_blocks_startup_before_config_rewrite_or_cleanup() {
    let (temp, _) = fixture(false, 100, true);
    let report = inspect(temp.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let publication = plan.begin_publication(0).unwrap();
    let before = tree(temp.path());
    assert!(plan.begin_publication(0).is_err());
    assert_eq!(tree(temp.path()), before);
    drop(publication);
    drop(plan);
    assert!(
        NativeStorage::open(NativeStorageConfig {
            data_dir: temp.path().to_owned(),
            hot_target_rows: 7,
            ..Default::default()
        })
        .is_err()
    );
    assert_eq!(tree(temp.path()), before);
    let pending = inspect_pending_repair(temp.path()).unwrap().unwrap();
    assert_eq!(pending.state(), RepairCatalogState::BeforePublication);
    assert_eq!(tree(temp.path()), before);
}

#[test]
fn prepared_installation_and_committed_quarantine_resume_without_losing_originals() {
    for committed_first in [false, true] {
        let (temp, rows) = fixture(false, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let original = report
            .catalog
            .segments
            .iter()
            .find(|s| s.id == id)
            .unwrap()
            .clone();
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let ready = prepared(&plan, &rows);
        let original_dir = plan.inspection.paths.segment_dir(id);
        let retained = tree(&original_dir);
        if committed_first {
            drop(ready.commit().unwrap());
        } else {
            // Actual interrupted-install state: prepared evidence, new tree moved,
            // but the original catalog remains authoritative.
            let stage = &ready.stages[0];
            rename_owned(
                &stage.segment_dir(),
                &plan.inspection.paths.segment_dir(stage.descriptor.id),
            )
            .unwrap();
            drop(ready);
        }
        assert_eq!(tree(&original_dir), retained);
        drop(plan);
        finish_pending(temp.path(), &rows);
        verify_replacement(temp.path(), &rows, &original);
    }
}

#[test]
fn malformed_and_third_catalog_states_block_without_mutation() {
    for malformed in [false, true] {
        let (temp, _) = fixture(false, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        drop(plan.begin_publication(0).unwrap());
        let paths = plan.inspection.paths.clone();
        drop(plan);
        if malformed {
            fs::write(temp.path().join(JOURNAL_FILE), b"broken retained intent").unwrap();
        } else {
            let mut catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
            catalog.hot_target_rows += 1;
            catalog.persist(&paths).unwrap();
        }
        let before = tree(temp.path());
        assert!(inspect_pending_repair(temp.path()).is_err());
        assert_eq!(tree(temp.path()), before);
    }
}

#[test]
fn headroom_refusal_and_destination_conflict_preserve_all_artifacts() {
    let (temp, rows) = fixture(false, 100, true);
    let report = inspect(temp.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let before = tree(temp.path());
    assert!(plan.begin_publication(u64::MAX).is_err());
    assert_eq!(tree(temp.path()), before);
    let ready = prepared(&plan, &rows);
    let destination = plan
        .inspection
        .paths
        .segment_dir(ready.stages[0].descriptor.id);
    fs::create_dir(&destination).unwrap();
    fs::write(destination.join("unrelated"), b"preserve").unwrap();
    let conflict = tree(temp.path());
    assert!(ready.commit().is_err());
    assert_eq!(tree(temp.path()), conflict);
}

#[test]
fn quarantine_conflict_never_overwrites_an_original_or_destination() {
    let (temp, rows) = fixture(false, 100, true);
    let report = inspect(temp.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let committed = prepared(&plan, &rows).commit().unwrap();
    let quarantine =
        operation_root(&plan.inspection.paths, &committed.publication.journal).join("quarantine");
    ensure_directory(&quarantine).unwrap();
    let collision = quarantine.join(format!("s_{id:016}"));
    fs::create_dir(&collision).unwrap();
    fs::write(collision.join("retained"), b"do not overwrite").unwrap();
    let before = tree(temp.path());
    assert!(committed.finish(inspection_limits(), |_| Ok(())).is_err());
    assert_eq!(tree(temp.path()), before);
}

#[test]
fn every_commit_and_quarantine_durability_boundary_resumes_recognized_state() {
    let event_count = {
        let (temp, rows) = fixture(false, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let ready = prepared(&plan, &rows);
        durability::inject_failure(usize::MAX);
        let result = ready
            .commit()
            .and_then(|committed| committed.finish(inspection_limits(), |_| Ok(())));
        let events = durability::take_events();
        result.unwrap();
        assert!(
            events
                .iter()
                .any(|(name, _)| *name == "repair_publish_catalog")
        );
        assert!(events.iter().any(|(name, _)| *name == "repair_rename"));
        events.len()
    };
    assert!(event_count > 0 && event_count < 256);
    for failure in 0..event_count {
        let (temp, rows) = fixture(false, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let original = report
            .catalog
            .segments
            .iter()
            .find(|s| s.id == id)
            .unwrap()
            .clone();
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let ready = prepared(&plan, &rows);
        durability::inject_failure(failure);
        let result = ready
            .commit()
            .and_then(|committed| committed.finish(inspection_limits(), |_| Ok(())));
        let events = durability::take_events();
        assert!(result.is_err(), "failure boundary {failure}: {events:?}");
        drop(plan);
        finish_pending(temp.path(), &rows);
        verify_replacement(temp.path(), &rows, &original);
    }
}

#[test]
fn changed_original_manifest_blocks_quarantine_and_preserves_evidence() {
    let (temp, rows) = fixture(false, 100, true);
    let report = inspect(temp.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let committed = prepared(&plan, &rows).commit().unwrap();
    let manifest = plan.inspection.paths.segment_manifest_path(id);
    fs::write(&manifest, b"unexpected original manifest").unwrap();
    // Create the quarantine container first so the failed ownership check itself
    // can be compared byte-for-byte without unrelated directory preparation.
    ensure_directory(
        &operation_root(&plan.inspection.paths, &committed.publication.journal).join("quarantine"),
    )
    .unwrap();
    let before = tree(temp.path());
    assert!(committed.finish(inspection_limits(), |_| Ok(())).is_err());
    assert_eq!(tree(temp.path()), before);
}

#[test]
fn repaired_active_hot_and_historical_owners_accept_later_rows_after_reopen() {
    for historical in [false, true] {
        let (temp, mut rows) = fixture(historical, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let replacement = plan.catalog().next_segment_id;
        prepared(&plan, &rows)
            .commit()
            .unwrap()
            .finish(inspection_limits(), |_| Ok(()))
            .unwrap();
        drop(plan);

        let mut appended = rows.last().unwrap().clone();
        appended.block_number += 1;
        appended.block_hash = alloy_primitives::B256::repeat_byte(13);
        appended.timestamp += 1;
        appended.tx_hash = alloy_primitives::B256::repeat_byte(99);
        appended.tx_index = 0;
        appended.log_index = 0;
        appended.data = alloy_primitives::Bytes::from(vec![99; 4]);
        let config = NativeStorageConfig {
            data_dir: temp.path().to_owned(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 0,
        };
        let mut storage = NativeStorage::open(config.clone()).unwrap();
        if historical {
            storage
                .write_historical_batch(std::slice::from_ref(&appended))
                .unwrap();
        } else {
            storage
                .write_batch(std::slice::from_ref(&appended))
                .unwrap();
        }
        storage.checkpoint_durable().unwrap();
        drop(storage);
        rows.push(appended);
        // Reopen through actual startup before inspection, so append prefix and
        // replacement-ID ownership must survive the normal recovery path.
        drop(NativeStorage::open(config).unwrap());
        let report = inspect(temp.path());
        assert_eq!(
            if historical {
                report.catalog.active_historical_segment
            } else {
                report.catalog.active_hot_segment
            },
            Some(replacement)
        );
        let descriptor = report
            .catalog
            .segments
            .iter()
            .find(|s| s.id == replacement)
            .unwrap();
        assert_eq!(descriptor.row_count, 7);
        let dir = StorageCatalogPaths::new(temp.path().to_owned()).segment_dir(replacement);
        let reader = SegmentReader::open_for_inspection(&dir).unwrap();
        let ids: Vec<_> = (0..7).collect();
        let actual: Vec<_> = reader
            .log_row_batches(&ids)
            .unwrap()
            .flat_map(Result::unwrap)
            .collect();
        assert_eq!(actual, rows);
        let flags = reader.read_canonical().unwrap();
        assert_eq!(
            (0..flags.len())
                .map(|row| flags.is_present(row))
                .collect::<Vec<_>>(),
            vec![false, false, true, true, true, true, true]
        );
        assert!(report.segments.iter().any(|s| s.id == replacement
            && matches!(s.disposition, PrimaryDataDisposition::CommitmentVerified)));
    }
}

#[test]
fn interrupted_directory_creation_retries_both_durability_barriers() {
    let temp = tempfile::tempdir().unwrap();
    let child = temp.path().join("quarantine");
    durability::inject_failure(0);
    let first = ensure_directory(&child);
    let first_events = durability::take_events();
    assert!(first.is_err());
    assert!(
        child.is_dir(),
        "creation precedes the injected first sync failure"
    );
    assert_eq!(first_events, vec![("sync_directory", child.clone())]);

    durability::inject_failure(usize::MAX);
    let retry = ensure_directory(&child);
    let retry_events = durability::take_events();
    retry.unwrap();
    assert_eq!(
        retry_events,
        vec![
            ("sync_directory", child),
            ("sync_directory", temp.path().to_owned()),
        ]
    );
}

// Propagate every injected failure; no assertions or unwraps while instrumentation
// is enabled. Encoding workers have separate thread-local hooks: this sweep only
// covers the observable root-thread ordering/publication checkpoints.
fn prepare_fallibly<'a>(
    plan: &'a RepairOwnershipPlan,
    rows: &[LogRow],
) -> io::Result<PreparedRepairPublication<'a>> {
    let id = plan.segment_ids()[0];
    let mut verifier = plan.begin_candidate(id)?;
    verifier.append(rows)?;
    let proof = verifier.finish()?;
    let mut publication = plan.begin_publication(0)?;
    let stage = publication.stage_candidate(&proof, rows, inspection_limits())?;
    publication.prepare(vec![stage], |_| Ok(()))
}

#[test]
fn every_observed_preparation_boundary_preserves_originals_and_resumes() {
    let event_count = {
        let (temp, rows) = fixture(false, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        durability::inject_failure(usize::MAX);
        let result = prepare_fallibly(&plan, &rows);
        let events = durability::take_events();
        drop(result.unwrap());
        events.len()
    };
    assert!(event_count > 0 && event_count < 512);
    for failure in 0..event_count {
        let (temp, rows) = fixture(false, 100, true);
        let report = inspect(temp.path());
        let id = data_id(&report);
        let before = report.catalog.clone();
        let original = before.segments.iter().find(|s| s.id == id).unwrap().clone();
        let paths = StorageCatalogPaths::new(temp.path().to_owned());
        let catalog_bytes = fs::read(paths.catalog_path()).unwrap();
        let original_tree = tree(&paths.segment_dir(id));
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        durability::inject_failure(failure);
        let result = prepare_fallibly(&plan, &rows);
        let events = durability::take_events();
        assert!(
            result.is_err(),
            "preparation boundary {failure}: {events:?}"
        );
        drop(result);
        assert_eq!(fs::read(paths.catalog_path()).unwrap(), catalog_bytes);
        assert_eq!(NativeStorageCatalog::load_existing(&paths).unwrap(), before);
        assert_eq!(tree(&paths.segment_dir(id)), original_tree);
        drop(plan);

        let pending = inspect_pending_repair(temp.path()).unwrap();
        let plan = match pending {
            Some(pending) => {
                assert_eq!(pending.state(), RepairCatalogState::BeforePublication);
                pending.into_plan(inspection_limits(), limits()).unwrap()
            }
            None => inspect(temp.path())
                .into_repair_plan(&[id], limits())
                .unwrap(),
        };
        prepared(&plan, &rows)
            .commit()
            .unwrap()
            .finish(inspection_limits(), |_| Ok(()))
            .unwrap();
        drop(plan);
        verify_replacement(temp.path(), &rows, &original);
    }
}
