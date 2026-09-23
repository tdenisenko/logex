//! Small disposable writer-backed datasets; assessment never opens networking.
use super::*;
use alloy_primitives::{Address, B256, Bytes};
use logex_storage::{
    ColumnFileHeader,
    native::{NativeStorage, NativeStorageCatalog, NativeStorageConfig, RepairPlanLimits},
};
use logex_types::{LogRow, Source};
use std::{collections::BTreeMap, fs, time::SystemTime};

fn limits() -> RepairAssessmentLimits {
    RepairAssessmentLimits {
        primary: InspectionLimits {
            max_segment_rows: 100,
            max_retained_artifact_bytes: 16 * 1024 * 1024,
            max_decoded_payload_bytes: 1024 * 1024,
        },
        max_index_logical_bytes_per_segment: 1024 * 1024,
    }
}

fn row(number: u64) -> LogRow {
    LogRow {
        block_number: number,
        block_hash: B256::repeat_byte(number as u8),
        timestamp: 100 + number,
        tx_hash: B256::repeat_byte(42),
        tx_index: 0,
        log_index: 0,
        address: Address::repeat_byte(7),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: Bytes::from_static(b"row"),
        data_len: 3,
        source: Source::Receipt,
    }
}

fn fixture() -> (tempfile::TempDir, NativeStorageCatalog, StorageCatalogPaths) {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: 4,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage
        .write_batch(&(10..17).map(row).collect::<Vec<_>>())
        .unwrap();
    storage.write_historical_batch(&[row(3)]).unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    let RepairInspection::Primary(primary) = inspect_repair(tmp.path(), limits().primary).unwrap()
    else {
        panic!("fixture unexpectedly has pending publication");
    };
    let catalog = primary.catalog.clone();
    // Index construction is fixture setup; inspection below must be read-only.
    for segment in &catalog.segments {
        IndexBuilder::build_all_indexes(&paths.segment_dir(segment.id)).unwrap();
    }
    (tmp, catalog, paths)
}

type Tree = BTreeMap<PathBuf, (SystemTime, Option<Vec<u8>>)>;
fn tree(root: &Path) -> Tree {
    fn visit(root: &Path, path: &Path, out: &mut Tree) {
        let metadata = fs::symlink_metadata(path).unwrap();
        out.insert(
            path.strip_prefix(root).unwrap().to_owned(),
            (
                metadata.modified().unwrap(),
                metadata.is_file().then(|| fs::read(path).unwrap()),
            ),
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), out);
            }
        }
    }
    let mut result = Tree::new();
    visit(root, root, &mut result);
    result
}

fn assess(root: &Path) -> RepairAssessment {
    assess_repair(root, limits(), IndexBuildProfile::All).unwrap()
}

fn segments(assessment: &RepairAssessment) -> &[SegmentRepairAssessment] {
    let RepairAssessmentReport::Inspected {
        index_profile,
        segments,
    } = assessment.report()
    else {
        panic!("expected segment findings: {:?}", assessment.report());
    };
    assert_eq!(*index_profile, IndexBuildProfile::All);
    segments
}

fn disposition(assessment: &RepairAssessment, id: u64) -> &SegmentRepairDisposition {
    &segments(assessment)
        .iter()
        .find(|segment| segment.id == id)
        .unwrap()
        .disposition
}

fn assert_owned(root: &Path) {
    assert_eq!(
        assess_repair(root, limits(), IndexBuildProfile::All)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn complete_assessment_preserves_all_segment_roles_and_owner() {
    let (tmp, _, _) = fixture();
    let before = tree(tmp.path());
    let assessment = assess(tmp.path());
    let findings = segments(&assessment);
    assert_eq!(findings.len(), 3);
    for role in [
        InspectedSegmentRole::ActiveHot,
        InspectedSegmentRole::ActiveHistorical,
        InspectedSegmentRole::CompletedSealed,
    ] {
        assert!(findings.iter().any(|finding| finding.role == role));
    }
    assert!(
        findings
            .iter()
            .all(|finding| finding.disposition == SegmentRepairDisposition::Verified)
    );
    assert_owned(tmp.path());
    assert_eq!(tree(tmp.path()), before);
    let inspection = assessment.into_inspection();
    assert_owned(tmp.path());
    assert_eq!(tree(tmp.path()), before);
    drop(inspection);
    drop(assess(tmp.path()));
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn absent_stale_and_damaged_indexes_require_only_local_rebuild() {
    for name in [
        "indexes",
        "indexes/index-checkpoint",
        "indexes/address.bptree",
    ] {
        let (tmp, catalog, paths) = fixture();
        let id = catalog.active_hot_segment.unwrap();
        let path = paths.segment_dir(id).join(name);
        match name {
            "indexes" => fs::remove_dir_all(path).unwrap(),
            "indexes/index-checkpoint" => fs::remove_file(path).unwrap(),
            _ => {
                let mut bytes = fs::read(&path).unwrap();
                *bytes.last_mut().unwrap() ^= 1;
                fs::write(path, bytes).unwrap();
            }
        }
        let before = tree(tmp.path());
        let assessment = assess(tmp.path());
        assert!(matches!(
            disposition(&assessment, id),
            SegmentRepairDisposition::IndexRebuildRequired(_)
        ));
        assert!(
            segments(&assessment)
                .iter()
                .filter(|segment| segment.id != id)
                .all(|segment| segment.disposition == SegmentRepairDisposition::Verified)
        );
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn primary_damage_takes_precedence_over_missing_indexes() {
    for missing in [false, true] {
        let (tmp, catalog, paths) = fixture();
        let id = catalog.active_hot_segment.unwrap();
        let dir = paths.segment_dir(id);
        if missing {
            fs::remove_file(dir.join("data.col")).unwrap();
        } else {
            let path = dir.join("block_hash.col");
            let mut bytes = fs::read(&path).unwrap();
            bytes[ColumnFileHeader::SIZE + 32] ^= 1;
            fs::write(path, bytes).unwrap();
        }
        fs::remove_dir_all(dir.join("indexes")).unwrap();
        let before = tree(tmp.path());
        let assessment = assess(tmp.path());
        assert!(matches!(
            disposition(&assessment, id),
            SegmentRepairDisposition::PrimaryRepairRequired(_)
        ));
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn primary_and_index_limits_are_not_corruption_verdicts() {
    let (tmp, catalog, _) = fixture();
    let id = catalog.active_hot_segment.unwrap();
    let before = tree(tmp.path());
    for (restricted, expected_resource) in [
        (
            RepairAssessmentLimits {
                primary: InspectionLimits {
                    max_segment_rows: 1,
                    ..limits().primary
                },
                ..limits()
            },
            "segment rows",
        ),
        (
            RepairAssessmentLimits {
                max_index_logical_bytes_per_segment: 0,
                ..limits()
            },
            "index logical payload bytes",
        ),
    ] {
        let assessment = assess_repair(tmp.path(), restricted, IndexBuildProfile::All).unwrap();
        assert!(
            matches!(disposition(&assessment, id), SegmentRepairDisposition::LimitExceeded {resource, required, limit} if *resource == expected_resource && required > limit)
        );
        assert_eq!(tree(tmp.path()), before);
    }
}

#[cfg(unix)]
#[test]
fn nonordinary_paths_block_assessment_without_guessing_damage() {
    for name in ["indexes", "indexes/address.bptree", ""] {
        let (tmp, catalog, paths) = fixture();
        let id = catalog.active_hot_segment.unwrap();
        let path = if name.is_empty() {
            paths.segment_dir(id)
        } else {
            paths.segment_dir(id).join(name)
        };
        let retained = tmp.path().join("retained-fixture");
        fs::rename(&path, &retained).unwrap();
        std::os::unix::fs::symlink(&retained, &path).unwrap();
        if name == "indexes/address.bptree" {
            fs::remove_file(paths.segment_dir(id).join("indexes/index-checkpoint")).unwrap();
        }
        let before = tree(tmp.path());
        let assessment = assess(tmp.path());
        assert!(
            matches!(disposition(&assessment, id), SegmentRepairDisposition::Blocked(issue) if issue.kind == io::ErrorKind::Unsupported)
        );
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn pending_wal_evidence_does_not_assert_recovery_or_scan_verdicts() {
    for artifact in ["wal/recovery.json", "wal/ingestion.json", "wal/pending.wal"] {
        let (tmp, _, _) = fixture();
        let path = tmp.path().join(artifact);
        fs::write(&path, b"unverified retained evidence").unwrap();
        let before = tree(tmp.path());
        let assessment = assess(tmp.path());
        assert_eq!(
            assessment.report(),
            &RepairAssessmentReport::RecoveryRequired {
                artifacts: vec![path]
            }
        );
        assert_owned(tmp.path());
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn pending_publication_is_reported_without_losing_ownership() {
    let (tmp, catalog, _) = fixture();
    let assessment = assess(tmp.path());
    let RepairInspection::Primary(primary) = assessment.into_inspection() else {
        panic!("expected primary inspection");
    };
    let plan = (*primary)
        .into_repair_plan(
            &[catalog.active_hot_segment.unwrap()],
            RepairPlanLimits {
                max_segments: 10,
                max_blocks: 10,
                max_segment_rows: 100,
                max_canonical_artifact_bytes: 1024 * 1024,
                max_candidate_data_bytes: 1024,
            },
        )
        .unwrap();
    let publication = plan.begin_publication(0).unwrap();
    let operation = publication.operation_id();
    drop(publication);
    drop(plan);
    let before = tree(tmp.path());
    let assessment = assess(tmp.path());
    assert!(
        matches!(assessment.report(), RepairAssessmentReport::PendingPublication { operation: actual, state: RepairCatalogState::BeforePublication, .. } if *actual == operation)
    );
    assert_owned(tmp.path());
    let inspection = assessment.into_inspection();
    assert_owned(tmp.path());
    assert_eq!(tree(tmp.path()), before);
    drop(inspection);
    assert_eq!(tree(tmp.path()), before);
}
