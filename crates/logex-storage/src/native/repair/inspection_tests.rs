//! Unified inspection preserves artifacts and transfers the existing owner.
use super::super::tests::{data_id, fixture, inspect, inspection_limits, limits, tree};
use super::*;

fn assert_owned(root: &Path) {
    assert_eq!(
        DataDirectoryLock::acquire_existing(root)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn primary_inspection_and_plan_retain_owner_without_writes() {
    for bundled in [false, true] {
        let (tmp, _) = fixture(bundled, if bundled { 6 } else { 100 }, true);
        let before = tree(tmp.path());
        let RepairInspection::Primary(report) =
            inspect_repair(tmp.path(), inspection_limits()).unwrap()
        else {
            panic!("unexpected pending publication");
        };
        assert_owned(tmp.path());
        assert_eq!(tree(tmp.path()), before);
        let id = data_id(&report);
        assert_eq!(
            report
                .catalog
                .segments
                .iter()
                .find(|s| s.id == id)
                .unwrap()
                .column_bundle
                .is_some(),
            bundled
        );
        let plan = (*report).into_repair_plan(&[id], limits()).unwrap();
        assert_owned(tmp.path());
        assert_eq!(tree(tmp.path()), before);
        drop(plan);
        assert!(DataDirectoryLock::acquire_existing(tmp.path()).is_ok());
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn pending_inspection_and_plan_retain_owner_without_writes() {
    let (tmp, _) = fixture(false, 100, true);
    let report = inspect(tmp.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let publication = plan.begin_publication(0).unwrap();
    let operation = publication.operation_id();
    drop(publication);
    drop(plan);
    let before = tree(tmp.path());
    let RepairInspection::Pending(pending) =
        inspect_repair(tmp.path(), inspection_limits()).unwrap()
    else {
        panic!("pending publication was not classified");
    };
    assert_eq!(pending.state(), RepairCatalogState::BeforePublication);
    assert_eq!(pending.operation_id(), operation);
    assert_owned(tmp.path());
    assert_eq!(tree(tmp.path()), before);
    let plan = (*pending).into_plan(inspection_limits(), limits()).unwrap();
    assert_owned(tmp.path());
    assert_eq!(tree(tmp.path()), before);
    drop(plan);
    let pending = inspect_pending_repair(tmp.path()).unwrap().unwrap();
    assert_eq!(pending.operation_id(), operation);
    assert_owned(tmp.path());
    drop(pending);
    assert!(DataDirectoryLock::acquire_existing(tmp.path()).is_ok());
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn published_pending_inspection_checks_catalog_relation_without_writes() {
    let (tmp, rows) = fixture(false, 100, true);
    let report = inspect(tmp.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let proof = verifier.finish().unwrap();
    let mut publication = plan.begin_publication(0).unwrap();
    let staged = publication
        .stage_candidate(&proof, &rows, inspection_limits())
        .unwrap();
    drop(
        publication
            .prepare(vec![staged], |_| Ok(()))
            .unwrap()
            .commit()
            .unwrap(),
    );
    drop(proof);
    drop(plan);
    let before = tree(tmp.path());
    let RepairInspection::Pending(pending) =
        inspect_repair(tmp.path(), inspection_limits()).unwrap()
    else {
        panic!("committed publication was not classified");
    };
    assert_eq!(pending.state(), RepairCatalogState::AfterPublication);
    assert_owned(tmp.path());
    assert_eq!(tree(tmp.path()), before);
    assert!((*pending).into_plan(inspection_limits(), limits()).is_err());
    assert!(DataDirectoryLock::acquire_existing(tmp.path()).is_ok());
    assert_eq!(tree(tmp.path()), before);

    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    let mut catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
    catalog.hot_target_rows += 1;
    catalog.persist(&paths).unwrap();
    let before = tree(tmp.path());
    assert!(inspect_repair(tmp.path(), inspection_limits()).is_err());
    assert!(DataDirectoryLock::acquire_existing(tmp.path()).is_ok());
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn missing_and_unreadable_inspection_evidence_never_creates_or_changes_files() {
    let tmp = tempfile::tempdir().unwrap();
    let before = tree(tmp.path());
    for root in [tmp.path().join("missing"), tmp.path().to_owned()] {
        assert_eq!(
            inspect_repair(&root, inspection_limits())
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(tree(tmp.path()), before);
    }
    // Preserve the existing pending-only API's absent-journal behavior.
    assert!(inspect_pending_repair(tmp.path()).unwrap().is_none());
    assert_eq!(tree(tmp.path()), before);

    let (tmp, _) = fixture(false, 100, false);
    let journal = tmp.path().join(JOURNAL_FILE);
    fs::create_dir(&journal).unwrap();
    let before = tree(tmp.path());
    assert!(inspect_repair(tmp.path(), inspection_limits()).is_err());
    assert_eq!(tree(tmp.path()), before);
    fs::remove_dir(&journal).unwrap();
    fs::write(&journal, b"damaged retained journal").unwrap();
    let before = tree(tmp.path());
    assert!(inspect_repair(tmp.path(), inspection_limits()).is_err());
    assert!(DataDirectoryLock::acquire_existing(tmp.path()).is_ok());
    assert_eq!(tree(tmp.path()), before);
}
