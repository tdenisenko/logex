//! Bounded disposable datasets and scripted transport; retained anchors are not CL proofs.
use super::*;
use crate::repair::tests::{Scripted, anchor, fixture, limits as fetch_limits};
use crate::repair::{RepairFetchStep, RepairFetcher, RepairRange, assess_repair};
use logex_cl::AnchorRecord;
use logex_storage::{
    SegmentReader,
    native::{
        InspectionLimits, NativeStorage, NativeStorageConfig, RepairReadLimits, StorageCatalogPaths,
    },
};
use logex_types::{ExecutionAnchor, LogRow};
use std::{collections::BTreeMap, fs, time::Duration};

fn limits() -> RepairExecutionLimits {
    RepairExecutionLimits {
        assessment: RepairAssessmentLimits {
            primary: InspectionLimits {
                max_segment_rows: 100,
                max_retained_artifact_bytes: 16 * 1024 * 1024,
                max_decoded_payload_bytes: 1024 * 1024,
            },
            max_index_logical_bytes_per_segment: 1024 * 1024,
        },
        wal: WalReadLimits {
            max_bytes: 1024 * 1024,
            max_rows: 100,
        },
        plan: RepairPlanLimits {
            max_segments: 10,
            max_blocks: 10,
            max_segment_rows: 100,
            max_canonical_artifact_bytes: 1024 * 1024,
            max_candidate_data_bytes: 1024,
        },
        reconstruction: RepairReconstructionLimits {
            max_total_rows: 100,
            max_total_data_bytes: 1024,
            read: RepairReadLimits {
                max_routing_artifact_bytes: 1024 * 1024,
                max_carry_artifact_bytes: 16 * 1024 * 1024,
                max_carry_decoded_payload_bytes: 1024 * 1024,
                max_carry_data_bytes: 1024,
            },
        },
        fetch: RepairFetchLimits {
            deadline: tokio::time::Instant::now() + Duration::from_secs(60),
            ..fetch_limits()
        },
    }
}

fn consensus(anchors: &[ExecutionAnchor]) -> (tempfile::TempDir, ConsensusStore) {
    let tmp = tempfile::tempdir().unwrap();
    let store = ConsensusStore::open(
        tmp.path(),
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )
    .unwrap();
    store
        .replace_anchors(
            anchors
                .iter()
                .map(|&anchor| AnchorRecord {
                    anchor,
                    finalized: false,
                    parent_beacon_root: None,
                })
                .collect(),
        )
        .unwrap();
    (tmp, store)
}

async fn rows() -> Vec<LogRow> {
    let (mut source, headers) = fixture();
    let mut fetch = RepairFetcher::new(
        RepairRange {
            start: headers[0].number,
            end: headers[3].number,
        },
        anchor(&headers[3]),
        limits().fetch,
        CancellationToken::new(),
    )
    .unwrap();
    let mut blocks = Vec::new();
    while let RepairFetchStep::Block(block) = fetch.next_block(&mut source).await.unwrap() {
        blocks.push(block.into_parts().1);
    }
    blocks.reverse();
    blocks.into_iter().flatten().collect()
}

fn write(rows: &[LogRow]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(rows).unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    tmp
}

fn assess(root: &Path, profile: IndexBuildProfile) -> RepairAssessment {
    assess_repair(root, limits().assessment, profile).unwrap()
}

fn primary(assessment: RepairAssessment) -> PrimaryDataInspection {
    let RepairInspection::Primary(primary) = assessment.into_inspection() else {
        panic!("expected primary inspection")
    };
    *primary
}

type Tree = BTreeMap<PathBuf, Option<Vec<u8>>>;
fn tree(root: &Path) -> Tree {
    fn visit(root: &Path, path: &Path, out: &mut Tree) {
        let metadata = fs::symlink_metadata(path).unwrap();
        out.insert(
            path.strip_prefix(root).unwrap().to_owned(),
            metadata.is_file().then(|| fs::read(path).unwrap()),
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

fn no_requests(source: &Scripted) {
    assert!(source.header_calls.is_empty());
    assert!(source.body_calls.is_empty());
    assert!(source.receipt_calls.is_empty());
}

fn verified(assessment: &RepairAssessment, profile: IndexBuildProfile) {
    let RepairAssessmentReport::Inspected {
        index_profile,
        segments,
    } = assessment.report()
    else {
        panic!("expected final inspection: {:?}", assessment.report())
    };
    assert_eq!(*index_profile, profile);
    assert!(
        segments
            .iter()
            .all(|s| s.disposition == SegmentRepairDisposition::Verified)
    );
}

#[tokio::test]
async fn missing_and_corrupt_indexes_rebuild_locally_for_every_profile() {
    for profile in [
        IndexBuildProfile::Erc20Transfer,
        IndexBuildProfile::LogQuery,
        IndexBuildProfile::All,
    ] {
        for corrupt in [false, true] {
            let expected = rows().await;
            let tmp = write(&expected);
            let before = primary(assess(tmp.path(), profile)).catalog;
            let id = before.active_hot_segment.unwrap();
            let path = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(id);
            let old_indexes = if corrupt {
                IndexBuilder::build_indexes(&path, profile).unwrap();
                fs::write(path.join("indexes/index-checkpoint"), b"damaged checkpoint").unwrap();
                Some(tree(&path.join("indexes")))
            } else {
                None
            };
            let original_columns: Tree = tree(&path)
                .into_iter()
                .filter(|(p, _)| !p.starts_with("indexes"))
                .collect();
            let catalog_bytes = fs::read(tmp.path().join("catalog.json")).unwrap();
            let (mut source, _) = fixture();
            let (_consensus_dir, store) = consensus(&[]);
            let outcome = execute_repair(
                assess(tmp.path(), profile),
                limits(),
                profile,
                &mut source,
                &store,
                CancellationToken::new(),
            )
            .await
            .unwrap();
            verified(&outcome.assessment, profile);
            no_requests(&source);
            assert!(!outcome.recovered_pending_storage);
            assert_eq!(outcome.quarantine_dirs.len(), 1);
            if let Some(old) = old_indexes {
                assert_eq!(
                    tree(&outcome.quarantine_dirs[0].join(format!("s_{id:016}"))),
                    old
                );
            }
            assert_eq!(
                tree(&path)
                    .into_iter()
                    .filter(|(p, _)| !p.starts_with("indexes"))
                    .collect::<Tree>(),
                original_columns
            );
            assert_eq!(
                fs::read(tmp.path().join("catalog.json")).unwrap(),
                catalog_bytes
            );
            assert_eq!(
                assess_repair(tmp.path(), limits().assessment, profile)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::WouldBlock
            );
            assert_eq!(primary(outcome.assessment).catalog, before);
        }
    }
}

#[tokio::test]
async fn payload_reconstruction_preserves_exact_rows_catalog_and_quarantine() {
    for pending in [false, true] {
        let expected = rows().await;
        let tmp = write(&expected);
        let before = primary(assess(tmp.path(), IndexBuildProfile::All)).catalog;
        let id = before.active_hot_segment.unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_owned());
        let original = paths.segment_dir(id);
        fs::remove_file(original.join("data.col")).unwrap();
        let damaged = tree(&original);
        if pending {
            let plan = primary(assess(tmp.path(), IndexBuildProfile::All))
                .into_repair_plan(&[id], limits().plan)
                .unwrap();
            drop(plan.begin_publication(0).unwrap());
            drop(plan);
            assert!(matches!(
                assess(tmp.path(), IndexBuildProfile::All).report(),
                RepairAssessmentReport::PendingPublication {
                    state: RepairCatalogState::BeforePublication,
                    ..
                }
            ));
        }
        let (mut source, headers) = fixture();
        let (_consensus_dir, store) = consensus(&[anchor(&headers[3])]);
        let outcome = execute_repair(
            assess(tmp.path(), IndexBuildProfile::All),
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &store,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        verified(&outcome.assessment, IndexBuildProfile::All);
        assert_eq!(source.body_calls, vec![headers[2].number]);
        assert_eq!(outcome.quarantine_dirs.len(), 1);
        assert_eq!(
            tree(&outcome.quarantine_dirs[0].join(format!("s_{id:016}"))),
            damaged
        );
        assert!(!original.exists());
        assert!(!tmp.path().join("repair.journal").exists());
        let after = primary(outcome.assessment).catalog;
        assert_eq!(after.state, before.state);
        assert_eq!(after.anchors, before.anchors);
        assert_eq!(after.next_segment_id, before.next_segment_id + 1);
        assert_eq!(after.segments.len(), before.segments.len());
        assert_eq!(
            after.active_historical_segment,
            before.active_historical_segment
        );
        let old_descriptor = before.segments.iter().find(|s| s.id == id).unwrap();
        let new_descriptor = after
            .segments
            .iter()
            .find(|s| Some(s.id) == after.active_hot_segment)
            .unwrap();
        assert_eq!(new_descriptor.kind, old_descriptor.kind);
        assert_eq!(new_descriptor.row_count, old_descriptor.row_count);
        assert_eq!(
            (new_descriptor.min_block, new_descriptor.max_block),
            (old_descriptor.min_block, old_descriptor.max_block)
        );
        assert_eq!(
            (new_descriptor.min_timestamp, new_descriptor.max_timestamp),
            (old_descriptor.min_timestamp, old_descriptor.max_timestamp)
        );
        assert_eq!(
            new_descriptor.source_commitment,
            old_descriptor.source_commitment
        );
        let replacement = paths.segment_dir(after.active_hot_segment.unwrap());
        let reader = SegmentReader::open(&replacement).unwrap();
        let ids: Vec<_> = (0..expected.len() as u32).collect();
        assert_eq!(reader.read_log_rows(Some(&ids)).unwrap(), expected);
    }
}

#[tokio::test]
async fn limits_cancellation_deadline_and_missing_anchors_leave_no_mutation() {
    for case in 0..6 {
        let tmp = write(&rows().await);
        let id = primary(assess(tmp.path(), IndexBuildProfile::All))
            .catalog
            .active_hot_segment
            .unwrap();
        let path = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(id);
        if case >= 3 {
            fs::remove_file(path.join("data.col")).unwrap();
        }
        let before = tree(tmp.path());
        let mut restricted = limits();
        let cancellation = CancellationToken::new();
        match case {
            0 => restricted.assessment.primary.max_segment_rows = 0,
            1 => cancellation.cancel(),
            2 => restricted.fetch.deadline = tokio::time::Instant::now(),
            3 => restricted.plan.max_blocks = 0,
            4 => restricted.reconstruction.max_total_rows = 0,
            5 => {} // No retained anchor admits the damaged range.
            _ => unreachable!(),
        }
        let assessment =
            assess_repair(tmp.path(), restricted.assessment, IndexBuildProfile::All).unwrap();
        let (mut source, _) = fixture();
        let (_consensus_dir, store) = consensus(&[]);
        assert!(
            execute_repair(
                assessment,
                restricted,
                IndexBuildProfile::All,
                &mut source,
                &store,
                cancellation
            )
            .await
            .is_err(),
            "case {case}"
        );
        no_requests(&source);
        assert_eq!(tree(tmp.path()), before, "case {case}");
        assert!(!tmp.path().join("repair.journal").exists());
        assert!(!tmp.path().join("index-repair.journal").exists());
    }
}

#[tokio::test]
async fn pending_index_repair_requires_recorded_profile_then_resumes_without_network() {
    let expected = rows().await;
    let tmp = write(&expected);
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_historical_batch(&expected).unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let inspection = primary(assess(tmp.path(), IndexBuildProfile::All));
    let id = inspection.catalog.active_hot_segment.unwrap();
    let catalog = inspection.catalog.clone();
    let repair = inspection
        .begin_index_repair(
            &[id],
            IndexBuilder::required_index_files(IndexBuildProfile::All),
            limits().assessment.primary,
            0,
        )
        .unwrap();
    drop(repair);
    let before = tree(tmp.path());
    let (mut source, _) = fixture();
    let (_consensus_dir, store) = consensus(&[]);
    let assessment = assess(tmp.path(), IndexBuildProfile::LogQuery);
    assert!(matches!(
        assessment.report(),
        RepairAssessmentReport::PendingIndexes { .. }
    ));
    let error = execute_repair(
        assessment,
        limits(),
        IndexBuildProfile::LogQuery,
        &mut source,
        &store,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("recorded index profile"));
    assert_eq!(tree(tmp.path()), before);
    let outcome = execute_repair(
        assess(tmp.path(), IndexBuildProfile::All),
        limits(),
        IndexBuildProfile::All,
        &mut source,
        &store,
        CancellationToken::new(),
    )
    .await
    .unwrap();
    verified(&outcome.assessment, IndexBuildProfile::All);
    no_requests(&source);
    // The pending journal covers only the hot owner. The independently missing
    // historical indexes still require a second local transaction after resume.
    assert_eq!(outcome.quarantine_dirs.len(), 2);
    assert_eq!(primary(outcome.assessment).catalog, catalog);
}

#[tokio::test]
async fn changed_execution_assessment_budget_requires_reassessment_without_mutation() {
    let tmp = write(&rows().await);
    let before = tree(tmp.path());
    let assessment = assess(tmp.path(), IndexBuildProfile::All);
    let mut restricted = limits();
    restricted.assessment.max_index_logical_bytes_per_segment = 0;
    let (mut source, _) = fixture();
    let (_consensus_dir, store) = consensus(&[]);
    let error = execute_repair(
        assessment,
        restricted,
        IndexBuildProfile::All,
        &mut source,
        &store,
        CancellationToken::new(),
    )
    .await
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("limits differ from the assessment")
    );
    no_requests(&source);
    assert_eq!(tree(tmp.path()), before);
}

#[tokio::test]
async fn committed_publication_finishes_without_anchors_or_new_requests() {
    let expected = rows().await;
    let tmp = write(&expected);
    let inspection = primary(assess(tmp.path(), IndexBuildProfile::All));
    let id = inspection.catalog.active_hot_segment.unwrap();
    let replacement_id = inspection.catalog.next_segment_id;
    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    let original = tree(&paths.segment_dir(id));
    let (mut source, headers) = fixture();
    let (_consensus_dir, store) = consensus(&[anchor(&headers[3])]);
    {
        let plan = inspection.into_repair_plan(&[id], limits().plan).unwrap();
        let mut reconstruction = RepairReconstruction::new(
            &plan,
            limits().reconstruction,
            limits().fetch,
            CancellationToken::new(),
        )
        .unwrap();
        while reconstruction
            .fetch_next_range(&mut source, &store)
            .await
            .unwrap()
        {}
        let reconstruction = reconstruction.finish().unwrap();
        let mut publication = reconstruction
            .begin_publication(IndexBuildProfile::All)
            .unwrap();
        let stage = reconstruction
            .stage_publication_segment(
                &mut publication,
                id,
                limits().assessment.primary,
                IndexBuildProfile::All,
                limits().assessment.max_index_logical_bytes_per_segment,
            )
            .unwrap();
        let prepared = publication
            .prepare(vec![stage], |dir| {
                IndexBuilder::verify_indexes(dir, IndexBuildProfile::All)
            })
            .unwrap();
        drop(
            reconstruction
                .with_current_anchors(&store, || Ok(prepared.commit()?))
                .unwrap(),
        );
    }
    let catalog = fs::read(tmp.path().join("catalog.json")).unwrap();
    fs::remove_file(
        paths
            .segment_dir(replacement_id)
            .join("indexes/block_number.bptree"),
    )
    .unwrap();
    store.replace_anchors(Vec::new()).unwrap();
    let (mut source, _) = fixture();
    let assessment = assess(tmp.path(), IndexBuildProfile::All);
    assert!(matches!(
        assessment.report(),
        RepairAssessmentReport::PendingPublication {
            state: RepairCatalogState::AfterPublication,
            ..
        }
    ));
    let mut provider = RefusingConsensus::default();
    let outcome = execute_repair_with_provider(
        assessment,
        limits(),
        IndexBuildProfile::All,
        &mut source,
        &mut provider,
        CancellationToken::new(),
        |_| {},
    )
    .await
    .unwrap();
    verified(&outcome.assessment, IndexBuildProfile::All);
    assert_eq!(provider.calls, 0);
    no_requests(&source);
    assert_eq!(fs::read(tmp.path().join("catalog.json")).unwrap(), catalog);
    assert_eq!(
        tree(&outcome.quarantine_dirs[0].join(format!("s_{id:016}"))),
        original
    );
    let reader = SegmentReader::open(&paths.segment_dir(replacement_id)).unwrap();
    let ids: Vec<_> = (0..expected.len() as u32).collect();
    assert_eq!(reader.read_log_rows(Some(&ids)).unwrap(), expected);
    assert!(!tmp.path().join("repair.journal").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn unsupported_index_path_is_blocked_without_mutation() {
    let tmp = write(&rows().await);
    let id = primary(assess(tmp.path(), IndexBuildProfile::All))
        .catalog
        .active_hot_segment
        .unwrap();
    let path = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(id);
    let outside = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(outside.path(), path.join("indexes")).unwrap();
    let before = tree(tmp.path());
    let assessment = assess(tmp.path(), IndexBuildProfile::All);
    let RepairAssessmentReport::Inspected { segments, .. } = assessment.report() else {
        panic!("expected inspection")
    };
    assert!(
        segments
            .iter()
            .any(|s| matches!(s.disposition, SegmentRepairDisposition::Blocked(_)))
    );
    let (mut source, _) = fixture();
    let (_consensus_dir, store) = consensus(&[]);
    assert!(
        execute_repair(
            assessment,
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &store,
            CancellationToken::new()
        )
        .await
        .is_err()
    );
    no_requests(&source);
    assert_eq!(tree(tmp.path()), before);
    assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
}

#[derive(Default)]
struct RefusingConsensus {
    calls: usize,
}

impl RepairConsensusProvider for RefusingConsensus {
    fn consensus(&mut self) -> Result<&ConsensusStore> {
        self.calls += 1;
        Err(eyre::eyre!("retained consensus unavailable"))
    }
}

#[tokio::test]
async fn local_repair_never_resolves_consensus_and_reports_its_phases() {
    for recover_wal in [false, true] {
        let tmp = write(&rows().await);
        if recover_wal {
            // A bounded incomplete final header is safe to retire locally.
            fs::write(tmp.path().join("wal/pending.wal"), [0; 3]).unwrap();
        }
        let (mut source, _) = fixture();
        let mut provider = RefusingConsensus::default();
        let mut phases = Vec::new();
        let outcome = execute_repair_with_provider(
            assess(tmp.path(), IndexBuildProfile::All),
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &mut provider,
            CancellationToken::new(),
            |phase| phases.push(phase),
        )
        .await
        .unwrap();
        verified(&outcome.assessment, IndexBuildProfile::All);
        assert_eq!(provider.calls, 0);
        no_requests(&source);
        assert_eq!(outcome.recovered_pending_storage, recover_wal);
        assert_eq!(
            phases.contains(&RepairExecutionPhase::Recovering),
            recover_wal
        );
        assert!(phases.contains(&RepairExecutionPhase::RebuildingIndexes));
        assert!(!phases.contains(&RepairExecutionPhase::Reconstructing));
        assert_eq!(phases.last(), Some(&RepairExecutionPhase::Verifying));
    }
}

#[tokio::test]
async fn unavailable_trust_preserves_damaged_primary_and_pending_publication() {
    for pending in [false, true] {
        let tmp = write(&rows().await);
        let id = primary(assess(tmp.path(), IndexBuildProfile::All))
            .catalog
            .active_hot_segment
            .unwrap();
        let path = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(id);
        fs::remove_file(path.join("data.col")).unwrap();
        if pending {
            let plan = primary(assess(tmp.path(), IndexBuildProfile::All))
                .into_repair_plan(&[id], limits().plan)
                .unwrap();
            drop(plan.begin_publication(0).unwrap());
            drop(plan);
        }
        let before = tree(tmp.path());
        let (mut source, _) = fixture();
        let mut provider = RefusingConsensus::default();
        let mut phases = Vec::new();
        let assessment = assess(tmp.path(), IndexBuildProfile::All);
        let directory = assessment.retain_directory();
        let error = execute_repair_with_provider(
            assessment,
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &mut provider,
            CancellationToken::new(),
            |phase| phases.push(phase),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("retained consensus unavailable"));
        assert_eq!(provider.calls, 1);
        assert_eq!(
            phases,
            [
                RepairExecutionPhase::Verifying,
                RepairExecutionPhase::Reconstructing
            ]
        );
        no_requests(&source);
        assert_eq!(tree(tmp.path()), before);
        assert_eq!(
            assess_repair(tmp.path(), limits().assessment, IndexBuildProfile::All)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        drop(directory);
        drop(assess(tmp.path(), IndexBuildProfile::All));
    }
}

#[tokio::test]
async fn pending_index_owner_survives_consumed_execution_error_until_cleanup_guard_drops() {
    let tmp = write(&rows().await);
    let inspection = primary(assess(tmp.path(), IndexBuildProfile::All));
    let id = inspection.catalog.active_hot_segment.unwrap();
    drop(
        inspection
            .begin_index_repair(
                &[id],
                IndexBuilder::required_index_files(IndexBuildProfile::All),
                limits().assessment.primary,
                0,
            )
            .unwrap(),
    );
    let assessment = assess(tmp.path(), IndexBuildProfile::All);
    let directory = assessment.retain_directory();
    let before = tree(tmp.path());
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let (mut source, _) = fixture();
    let mut provider = RefusingConsensus::default();
    assert!(
        execute_repair_with_provider(
            assessment,
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &mut provider,
            cancellation,
            |_| {}
        )
        .await
        .is_err()
    );
    assert_eq!(provider.calls, 0);
    no_requests(&source);
    assert_eq!(tree(tmp.path()), before);
    assert_eq!(
        assess_repair(tmp.path(), limits().assessment, IndexBuildProfile::All)
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(directory);
    drop(assess(tmp.path(), IndexBuildProfile::All));
}
