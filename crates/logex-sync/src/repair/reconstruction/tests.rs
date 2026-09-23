//! Disposable writer/fetcher integration; scripted anchors are not CL proofs.
use super::*;
use crate::repair::tests::{anchor, fixture, fixture_with_logs, limits as fetch_limits};
use logex_cl::AnchorRecord;
use logex_storage::ColumnFileHeader;
use logex_storage::native::{
    InspectionLimits, NativeStorage, NativeStorageConfig, PrimaryDataInspection, RepairPlanLimits,
    StorageCatalogPaths, inspect_primary_data,
};
use logex_types::ExecutionAnchor;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

fn consensus(anchors: &[ExecutionAnchor]) -> (tempfile::TempDir, ConsensusStore) {
    let directory = tempfile::tempdir().unwrap();
    let store = ConsensusStore::open(
        directory.path(),
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )
    .unwrap();
    // Persist scripted retained anchors; CL proof validation has separate fixtures.
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
    (directory, store)
}

async fn fetch(
    reconstruction: &mut RepairReconstruction<'_>,
    source: &mut impl RepairSource,
    anchor: ExecutionAnchor,
) -> Result<bool> {
    let (_directory, store) = consensus(&[anchor]);
    reconstruction.fetch_next_range(source, &store).await
}

fn read_limits() -> RepairReadLimits {
    RepairReadLimits {
        max_routing_artifact_bytes: 1024 * 1024,
        max_carry_artifact_bytes: 16 * 1024 * 1024,
        max_carry_decoded_payload_bytes: 1024 * 1024,
        max_carry_data_bytes: 1024,
    }
}
fn limits() -> RepairReconstructionLimits {
    RepairReconstructionLimits {
        max_total_rows: 100,
        max_total_data_bytes: 1024,
        read: read_limits(),
    }
}
fn fetching() -> RepairFetchLimits {
    RepairFetchLimits {
        deadline: Instant::now() + Duration::from_secs(60),
        ..fetch_limits()
    }
}
fn inspect(path: &Path) -> PrimaryDataInspection {
    inspect_primary_data(
        path,
        InspectionLimits {
            max_segment_rows: 100,
            max_retained_artifact_bytes: 16 * 1024 * 1024,
            max_decoded_payload_bytes: 1024 * 1024,
        },
    )
    .unwrap()
}
fn plan(report: PrimaryDataInspection, ids: &[u64]) -> RepairOwnershipPlan {
    report
        .into_repair_plan(
            ids,
            RepairPlanLimits {
                max_segments: 10,
                max_blocks: 10,
                max_segment_rows: 100,
                max_canonical_artifact_bytes: 1024 * 1024,
                max_candidate_data_bytes: 1024,
            },
        )
        .unwrap()
}
fn write(rows: &[LogRow], target: u64, retired: &[B256]) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: target,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(rows).unwrap();
    for hash in retired {
        assert!(storage.mark_non_canonical(*hash).unwrap() > 0);
    }
    storage.checkpoint_durable().unwrap();
    drop(storage);
    tmp
}
type Tree = BTreeMap<PathBuf, (SystemTime, Option<Vec<u8>>)>;
fn tree(root: &Path) -> Tree {
    fn visit(root: &Path, path: &Path, result: &mut Tree) {
        let metadata = fs::metadata(path).unwrap();
        result.insert(
            path.strip_prefix(root).unwrap().to_owned(),
            (
                metadata.modified().unwrap(),
                metadata.is_file().then(|| fs::read(path).unwrap()),
            ),
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), result);
            }
        }
    }
    let mut result = Tree::new();
    visit(root, root, &mut result);
    result
}
async fn original_rows(logged_blocks: &[u64]) -> Vec<LogRow> {
    let (mut source, headers) = fixture_with_logs(logged_blocks);
    let mut cursor = RepairFetcher::new(
        RepairRange {
            start: headers[0].number,
            end: headers[3].number,
        },
        anchor(&headers[3]),
        fetching(),
        CancellationToken::new(),
    )
    .unwrap();
    let mut blocks = Vec::new();
    while let RepairFetchStep::Block(block) = cursor.next_block(&mut source).await.unwrap() {
        blocks.push(block.into_parts().1);
    }
    blocks.reverse();
    blocks.into_iter().flatten().collect()
}
fn kind(error: &eyre::Report) -> RepairFetchErrorKind {
    error
        .downcast_ref::<crate::repair::RepairFetchError>()
        .unwrap()
        .kind
}

fn stage_limits() -> InspectionLimits {
    InspectionLimits {
        max_segment_rows: 100,
        max_retained_artifact_bytes: 16 * 1024 * 1024,
        max_decoded_payload_bytes: 1024 * 1024,
    }
}

async fn reconstruct_for_publication<'a>(
    plan: &'a RepairOwnershipPlan,
    logged: &[u64],
    store: &ConsensusStore,
    cancellation: CancellationToken,
) -> ReconstructedRepair<'a> {
    let (mut source, _) = fixture_with_logs(logged);
    let mut reconstruction =
        RepairReconstruction::new(plan, limits(), fetching(), cancellation).unwrap();
    while reconstruction
        .fetch_next_range(&mut source, store)
        .await
        .unwrap()
    {}
    reconstruction.finish().unwrap()
}

#[tokio::test]
async fn journaled_publication_preserves_split_owners_empty_blocks_and_indexes() {
    for logged in [&[][..], &[0, 2, 3][..]] {
        let rows = original_rows(logged).await;
        let tmp = write(&rows, 3, &[]);
        let report = inspect(tmp.path());
        let before = report.catalog.clone();
        let seeds: Vec<_> = before.segments.iter().map(|s| s.id).collect();
        let originals: Vec<_> = seeds
            .iter()
            .map(|&id| {
                let path = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(id);
                (id, tree(&path))
            })
            .collect();
        let (_, headers) = fixture_with_logs(logged);
        let (_directory, store) = consensus(&[anchor(&headers[3])]);
        let quarantine = {
            let plan = plan(report, &seeds);
            let mut publication = plan.begin_publication(0).unwrap();
            let rebuilt =
                reconstruct_for_publication(&plan, logged, &store, CancellationToken::new()).await;
            if !logged.is_empty() {
                assert_eq!(
                    plan.block_ranges(),
                    &[(headers[0].number, headers[3].number)]
                );
                assert_eq!(
                    rebuilt
                        .completions()
                        .iter()
                        .map(RepairCompletion::delivered_blocks)
                        .sum::<u64>(),
                    4
                );
            }
            let stages = plan
                .segment_ids()
                .iter()
                .map(|&id| {
                    rebuilt
                        .stage_publication_segment(
                            &mut publication,
                            id,
                            stage_limits(),
                            IndexBuildProfile::All,
                        )
                        .unwrap()
                })
                .collect();
            rebuilt
                .publish_replacements(
                    publication,
                    stages,
                    &store,
                    stage_limits(),
                    IndexBuildProfile::All,
                )
                .unwrap()
        };
        for (id, retained) in originals {
            assert_eq!(tree(&quarantine.join(format!("s_{id:016}"))), retained);
        }
        let report = inspect(tmp.path());
        assert_eq!(report.catalog.state, before.state);
        assert_eq!(report.catalog.anchors, before.anchors);
        assert_eq!(
            report.catalog.next_segment_id,
            before.next_segment_id + seeds.len() as u64
        );
        let mut actual = Vec::new();
        for descriptor in &report.catalog.segments {
            assert!(descriptor.id >= before.next_segment_id);
            let dir = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(descriptor.id);
            IndexBuilder::verify_indexes(&dir, IndexBuildProfile::All).unwrap();
            let reader = logex_storage::SegmentReader::open(&dir).unwrap();
            let ids: Vec<_> = (0..descriptor.row_count as u32).collect();
            actual.extend(reader.read_log_rows(Some(&ids)).unwrap());
        }
        assert_eq!(actual, rows);
        drop(report);
        drop(
            NativeStorage::open(NativeStorageConfig {
                data_dir: tmp.path().to_owned(),
                hot_target_rows: 3,
                ..Default::default()
            })
            .unwrap(),
        );
        assert!(!tmp.path().join("repair.journal").exists());
    }
}

#[tokio::test(start_paused = true)]
async fn publication_requires_current_anchors_and_active_reconstruction_before_switch() {
    for cause in 0..3 {
        let logged = [0, 2, 3];
        let rows = original_rows(&logged).await;
        let tmp = write(&rows, 100, &[]);
        let report = inspect(tmp.path());
        let seed = report.catalog.active_hot_segment.unwrap();
        let catalog_before = fs::read(tmp.path().join("catalog.json")).unwrap();
        let original_dir = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(seed);
        let original_before = tree(&original_dir);
        let (_, headers) = fixture_with_logs(&logged);
        let (_directory, store) = consensus(&[anchor(&headers[3])]);
        {
            let plan = plan(report, &[seed]);
            let mut publication = plan.begin_publication(0).unwrap();
            let cancellation = CancellationToken::new();
            let rebuilt =
                reconstruct_for_publication(&plan, &logged, &store, cancellation.clone()).await;
            let staged = rebuilt
                .stage_publication_segment(
                    &mut publication,
                    seed,
                    stage_limits(),
                    IndexBuildProfile::All,
                )
                .unwrap();
            match cause {
                0 => store.replace_anchors(Vec::new()).unwrap(),
                1 => cancellation.cancel(),
                _ => tokio::time::advance(Duration::from_secs(60)).await,
            }
            let error = rebuilt
                .publish_replacements(
                    publication,
                    vec![staged],
                    &store,
                    stage_limits(),
                    IndexBuildProfile::All,
                )
                .unwrap_err();
            assert_eq!(
                kind(&error),
                match cause {
                    0 => RepairFetchErrorKind::Unavailable,
                    1 => RepairFetchErrorKind::Cancelled,
                    _ => RepairFetchErrorKind::Deadline,
                }
            );
        }
        assert_eq!(
            fs::read(tmp.path().join("catalog.json")).unwrap(),
            catalog_before
        );
        assert_eq!(tree(&original_dir), original_before);
        let pending = logex_storage::native::inspect_pending_repair(tmp.path())
            .unwrap()
            .unwrap();
        assert_eq!(
            pending.state(),
            logex_storage::native::RepairCatalogState::BeforePublication
        );
    }
}

#[tokio::test]
async fn committed_repair_finishes_after_restart_and_rebuilds_missing_derived_index() {
    let logged = [0, 2, 3];
    let rows = original_rows(&logged).await;
    let tmp = write(&rows, 100, &[]);
    let report = inspect(tmp.path());
    let seed = report.catalog.active_hot_segment.unwrap();
    let new_id = report.catalog.next_segment_id;
    let (_, headers) = fixture_with_logs(&logged);
    let (_directory, store) = consensus(&[anchor(&headers[3])]);
    {
        let plan = plan(report, &[seed]);
        let mut publication = plan.begin_publication(0).unwrap();
        let rebuilt =
            reconstruct_for_publication(&plan, &logged, &store, CancellationToken::new()).await;
        let stage = rebuilt
            .stage_publication_segment(
                &mut publication,
                seed,
                stage_limits(),
                IndexBuildProfile::All,
            )
            .unwrap();
        let prepared = publication
            .prepare(vec![stage], |dir| {
                IndexBuilder::verify_indexes(dir, IndexBuildProfile::All)
            })
            .unwrap();
        // Simulate interruption after the durable switch and before quarantine.
        let committed = rebuilt
            .with_current_anchors(&store, || Ok(prepared.commit()?))
            .unwrap();
        drop(committed);
    }
    let catalog_after = fs::read(tmp.path().join("catalog.json")).unwrap();
    let replacement = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(new_id);
    fs::remove_file(replacement.join("indexes/block_number.bptree")).unwrap();
    store.replace_anchors(Vec::new()).unwrap();
    let pending = logex_storage::native::inspect_pending_repair(tmp.path())
        .unwrap()
        .unwrap();
    assert_eq!(
        pending.state(),
        logex_storage::native::RepairCatalogState::AfterPublication
    );
    let quarantine =
        finish_pending_publication(pending, stage_limits(), IndexBuildProfile::All).unwrap();
    assert!(quarantine.join(format!("s_{seed:016}")).is_dir());
    assert_eq!(
        fs::read(tmp.path().join("catalog.json")).unwrap(),
        catalog_after
    );
    IndexBuilder::verify_indexes(&replacement, IndexBuildProfile::All).unwrap();
    drop(
        NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            hot_target_rows: 100,
            ..Default::default()
        })
        .unwrap(),
    );
}

#[tokio::test]
async fn reconstruction_staging_builds_bound_indexes_without_publishing() {
    let (mut source, headers) = fixture_with_logs(&[0, 2, 3]);
    let rows = original_rows(&[0, 2, 3]).await;
    let tmp = write(&rows, 100, &[]);
    let before = tree(tmp.path());
    let report = inspect(tmp.path());
    let id = report
        .catalog
        .segments
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap()
        .id;
    let plan = plan(report, &[id]);
    let mut reconstruction =
        RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new()).unwrap();
    fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
        .await
        .unwrap();
    let rebuilt = reconstruction.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let staged = rebuilt
        .stage_segment(
            id,
            &parent.path().join("replacement"),
            stage_limits(),
            IndexBuildProfile::All,
        )
        .unwrap();
    staged.verify().unwrap();
    assert!(!IndexBuilder::indexes_missing(&staged.segment_dir(), IndexBuildProfile::All).unwrap());
    assert!(staged.segment_dir().join("indexes").is_dir());
    assert_eq!(
        staged.descriptor().source_commitment,
        rebuilt.segments()[0].descriptor().source_commitment
    );
    assert_eq!(rebuilt.completions()[0].delivered_blocks(), 4);
    assert_eq!(tree(tmp.path()), before);
    let unknown = parent.path().join("unselected");
    assert_eq!(
        kind(
            &rebuilt
                .stage_segment(u64::MAX, &unknown, stage_limits(), IndexBuildProfile::All,)
                .unwrap_err()
        ),
        RepairFetchErrorKind::InvalidInput
    );
    assert!(!unknown.exists());
}

#[tokio::test(start_paused = true)]
async fn reconstruction_staging_empty_owner_and_terminal_lifetime_checks() {
    for expired in [false, true] {
        let tmp = write(&[], 100, &[]);
        let before = tree(tmp.path());
        let report = inspect(tmp.path());
        let id = report.catalog.active_hot_segment.unwrap();
        let plan = plan(report, &[id]);
        let cancellation = CancellationToken::new();
        let rebuilt = RepairReconstruction::new(&plan, limits(), fetching(), cancellation.clone())
            .unwrap()
            .finish()
            .unwrap();
        let parent = tempfile::tempdir().unwrap();
        let staged = rebuilt
            .stage_segment(
                id,
                &parent.path().join("empty"),
                stage_limits(),
                IndexBuildProfile::All,
            )
            .unwrap();
        staged.verify().unwrap();
        assert!(
            !IndexBuilder::indexes_missing(&staged.segment_dir(), IndexBuildProfile::All).unwrap()
        );
        assert_eq!(staged.descriptor().row_count, 0);
        if expired {
            tokio::time::advance(Duration::from_secs(61)).await;
        } else {
            cancellation.cancel();
        }
        let destination = parent.path().join("refused");
        let error = rebuilt
            .stage_segment(id, &destination, stage_limits(), IndexBuildProfile::All)
            .unwrap_err();
        assert_eq!(
            kind(&error),
            if expired {
                RepairFetchErrorKind::Deadline
            } else {
                RepairFetchErrorKind::Cancelled
            }
        );
        assert!(!destination.exists());
        assert_eq!(tree(tmp.path()), before);
    }
}

#[tokio::test]
async fn reconstruction_preserves_split_owners_local_rows_and_empty_block_coverage() {
    let (mut source, headers) = fixture_with_logs(&[0, 2, 3]);
    let mut rows = original_rows(&[0, 2, 3]).await;
    assert_eq!(rows.len(), 6);
    let orphan = B256::repeat_byte(0xfe);
    rows[3].block_hash = orphan;
    rows[4].source = Source::Trace;
    let tmp = write(&rows, 3, &[orphan, rows[4].block_hash]);
    let original = inspect(tmp.path());
    let descriptors: Vec<_> = original
        .catalog
        .segments
        .iter()
        .filter(|s| s.row_count > 0)
        .collect();
    assert_eq!(descriptors.len(), 2);
    let seed = descriptors[0].id;
    let ids: Vec<_> = descriptors.iter().map(|d| d.id).collect();
    let expected: BTreeMap<_, _> = ids
        .iter()
        .zip(rows.chunks(3))
        .map(|(&id, rows)| (id, rows.to_vec()))
        .collect();
    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    // Only canonical receipt slots live in this owner. Its routing/canonical
    // metadata survives an incomplete payload; the other owner remains readable.
    drop(original);
    fs::write(
        paths.segment_dir(seed).join("data.col"),
        b"incomplete fixture payload",
    )
    .unwrap();
    let before = tree(tmp.path());
    let plan = plan(inspect(tmp.path()), &[seed]);
    let original_catalog = plan.catalog().clone();
    assert_eq!(plan.segment_ids(), ids);
    assert_eq!(
        plan.block_ranges(),
        &[(headers[0].number, headers[2].number)]
    );
    let mut reconstruction =
        RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new()).unwrap();
    assert!(
        fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    assert!(
        !fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    let rebuilt = reconstruction.finish().unwrap();
    assert_eq!(rebuilt.completions().len(), 1);
    assert_eq!(rebuilt.completions()[0].delivered_blocks(), 3);
    assert_eq!(
        source.body_calls,
        vec![headers[2].number, headers[1].number, headers[0].number]
    );
    assert_eq!(rebuilt.segments().len(), 2);
    for segment in rebuilt.segments() {
        assert_eq!(segment.rows(), expected[&segment.descriptor().id]);
        for (position, row) in segment.rows().iter().enumerate() {
            assert_eq!(
                segment.canonical().is_present(position as u64),
                row.block_hash != orphan && row.block_number != headers[3].number
            );
        }
    }
    // A canonical log absent from the original physical owners was fetched but
    // never inserted; the orphan, Trace row and original multiplicity remain.
    assert_eq!(
        rebuilt
            .segments()
            .iter()
            .map(|s| s.rows().len())
            .sum::<usize>(),
        rows.len()
    );
    assert_eq!(rebuilt.plan().catalog(), &original_catalog);
    assert_eq!(
        inspect_primary_data(
            tmp.path(),
            InspectionLimits {
                max_segment_rows: 1,
                max_retained_artifact_bytes: 1,
                max_decoded_payload_bytes: 1,
            }
        )
        .unwrap_err()
        .kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(tree(tmp.path()), before);
    drop(rebuilt);
    drop(plan);
    assert_eq!(tree(tmp.path()), before);
}

#[tokio::test]
async fn reconstruction_preserves_original_order_duplicates_and_disjoint_fetch_gaps() {
    let (mut source, headers) = fixture_with_logs(&[0, 2, 3]);
    let mut rows = original_rows(&[0, 2, 3]).await;
    rows.swap(0, 1);
    rows[5] = rows[4].clone();
    let tmp = write(&rows, 2, &[]);
    let report = inspect(tmp.path());
    let ids: Vec<_> = report
        .catalog
        .segments
        .iter()
        .filter(|s| s.row_count > 0 && s.min_block != Some(headers[2].number))
        .map(|s| s.id)
        .collect();
    let plan = plan(report, &ids);
    assert_eq!(
        plan.block_ranges(),
        &[
            (headers[0].number, headers[0].number),
            (headers[3].number, headers[3].number)
        ]
    );
    let mut reconstruction =
        RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new()).unwrap();
    assert!(
        fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    assert!(
        fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    let rebuilt = reconstruction.finish().unwrap();
    assert_eq!(
        source.body_calls,
        vec![headers[0].number, headers[3].number]
    );
    assert_eq!(rebuilt.completions().len(), 2);
    assert_eq!(rebuilt.segments()[0].rows(), &rows[..2]);
    assert_eq!(rebuilt.segments()[1].rows(), &rows[4..]);
    assert!(
        rebuilt
            .completions()
            .iter()
            .all(|c| c.delivered_blocks() == 1)
    );
}

#[tokio::test]
async fn preserved_rows_still_require_fetch_completion_and_empty_owners_need_no_range() {
    let (_, headers) = fixture();
    let rows = original_rows(&[2]).await;
    let tmp = write(&rows, 100, &[rows[0].block_hash]);
    let report = inspect(tmp.path());
    let id = report.catalog.segments[0].id;
    let plan = plan(report, &[id]);
    let reconstruction =
        RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new()).unwrap();
    assert_eq!(
        kind(&reconstruction.finish().unwrap_err()),
        RepairFetchErrorKind::Local
    );
    drop(plan);

    let tmp = write(&[], 100, &[]);
    let report = inspect(tmp.path());
    let id = report.catalog.segments[0].id;
    let plan = self::plan(report, &[id]);
    let mut reconstruction = RepairReconstruction::new(
        &plan,
        RepairReconstructionLimits {
            max_total_rows: 0,
            max_total_data_bytes: 0,
            ..limits()
        },
        fetching(),
        CancellationToken::new(),
    )
    .unwrap();
    let (mut source, _) = fixture();
    assert!(
        !fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    let rebuilt = reconstruction.finish().unwrap();
    assert!(rebuilt.completions().is_empty());
    assert_eq!(rebuilt.segments().len(), 1);
    assert!(rebuilt.segments()[0].rows().is_empty());
    assert!(source.body_calls.is_empty());
}

#[tokio::test]
async fn reconstruction_enforces_aggregate_rows_and_actual_data_with_terminal_failure() {
    let rows = original_rows(&[2]).await;
    let tmp = write(&rows, 100, &[]);
    let report = inspect(tmp.path());
    let id = report.catalog.segments[0].id;
    let plan = plan(report, &[id]);
    let error = RepairReconstruction::new(
        &plan,
        RepairReconstructionLimits {
            max_total_rows: 1,
            ..limits()
        },
        fetching(),
        CancellationToken::new(),
    )
    .unwrap_err();
    assert_eq!(kind(&error), RepairFetchErrorKind::LimitExceeded);
    for budget in [7, 8] {
        let (mut source, headers) = fixture();
        let mut reconstruction = RepairReconstruction::new(
            &plan,
            RepairReconstructionLimits {
                max_total_rows: 2,
                max_total_data_bytes: budget,
                ..limits()
            },
            fetching(),
            CancellationToken::new(),
        )
        .unwrap();
        let result = fetch(&mut reconstruction, &mut source, anchor(&headers[3])).await;
        if budget == 7 {
            assert_eq!(
                kind(&result.unwrap_err()),
                RepairFetchErrorKind::LimitExceeded
            );
            assert_eq!(
                kind(&reconstruction.finish().unwrap_err()),
                RepairFetchErrorKind::Terminal
            );
        } else {
            assert!(result.unwrap());
            assert_eq!(reconstruction.finish().unwrap().segments()[0].rows(), rows);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn dropped_fetch_and_post_fetch_cancellation_or_deadline_cannot_complete() {
    let rows = original_rows(&[2]).await;
    let tmp = write(&rows, 100, &[]);
    let report = inspect(tmp.path());
    let id = report.catalog.segments[0].id;
    let plan = plan(report, &[id]);
    let (mut source, headers) = fixture();
    source.pending_headers = true;
    let mut reconstruction =
        RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new()).unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(1),
            fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
        )
        .await
        .is_err()
    );
    assert_eq!(
        kind(&reconstruction.finish().unwrap_err()),
        RepairFetchErrorKind::Terminal
    );
    for expired in [false, true] {
        let (mut source, headers) = fixture();
        let cancellation = CancellationToken::new();
        let mut reconstruction =
            RepairReconstruction::new(&plan, limits(), fetching(), cancellation.clone()).unwrap();
        fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap();
        if expired {
            tokio::time::advance(Duration::from_secs(61)).await;
        } else {
            cancellation.cancel();
        }
        assert_eq!(
            kind(&reconstruction.finish().unwrap_err()),
            if expired {
                RepairFetchErrorKind::Deadline
            } else {
                RepairFetchErrorKind::Cancelled
            }
        );
    }
}

#[tokio::test]
async fn changed_original_routing_cannot_claim_verified_reconstruction() {
    for column in ["block_hash", "log_index"] {
        let rows = original_rows(&[2]).await;
        let tmp = write(&rows, 100, &[]);
        let report = inspect(tmp.path());
        let id = report.catalog.segments[0].id;
        let paths = StorageCatalogPaths::new(tmp.path().to_owned());
        drop(report);
        let path = paths.segment_dir(id).join(format!("{column}.col"));
        let mut bytes = fs::read(&path).unwrap();
        bytes[ColumnFileHeader::SIZE] ^= 0x40;
        fs::write(path, bytes).unwrap();
        let before = tree(tmp.path());
        let plan = plan(inspect(tmp.path()), &[id]);
        let (mut source, headers) = fixture();
        let mut reconstruction =
            RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new())
                .unwrap();
        let error = fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap_err();
        assert_eq!(kind(&error), RepairFetchErrorKind::Local);
        assert_eq!(
            kind(&reconstruction.finish().unwrap_err()),
            RepairFetchErrorKind::Terminal
        );
        assert_eq!(tree(tmp.path()), before);
    }
}

#[tokio::test]
async fn completed_fetch_cannot_replace_a_different_original_commitment() {
    let mut rows = original_rows(&[2]).await;
    // The storage writer accepts rows independently of consensus validation.
    // Its published local identity is not interchangeable with chain evidence:
    // even a complete valid fetch must not silently change recorded contents.
    rows[0].address = alloy_primitives::Address::repeat_byte(0x99);
    let tmp = write(&rows, 100, &[]);
    let report = inspect(tmp.path());
    let id = report.catalog.segments[0].id;
    let plan = plan(report, &[id]);
    let before = tree(tmp.path());
    let (mut source, headers) = fixture();
    let mut reconstruction =
        RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new()).unwrap();
    assert!(
        fetch(&mut reconstruction, &mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    let error = reconstruction.finish().unwrap_err();
    assert_eq!(kind(&error), RepairFetchErrorKind::Local);
    assert!(format!("{error:#}").contains("original logical commitment"));
    assert_eq!(tree(tmp.path()), before);
}

#[tokio::test]
async fn consensus_reconstruction_uses_nearest_anchor_and_refuses_unavailable_bridge() {
    let rows = original_rows(&[2]).await;
    let tmp = write(&rows, 100, &[]);
    let before = tree(tmp.path());
    let report = inspect(tmp.path());
    let seed = report.catalog.active_hot_segment.unwrap();
    let plan = plan(report, &[seed]);
    let (_, headers) = fixture();
    for anchors in [Vec::new(), vec![anchor(&headers[3])]] {
        let (_directory, store) = consensus(&anchors);
        let (mut source, _) = fixture();
        let mut reconstruction = RepairReconstruction::new(
            &plan,
            limits(),
            RepairFetchLimits {
                max_headers: 1,
                ..fetching()
            },
            CancellationToken::new(),
        )
        .unwrap();
        assert_eq!(
            kind(
                &reconstruction
                    .fetch_next_range(&mut source, &store)
                    .await
                    .unwrap_err()
            ),
            RepairFetchErrorKind::Unavailable
        );
        assert!(source.body_calls.is_empty());
        assert_eq!(
            kind(&reconstruction.finish().unwrap_err()),
            RepairFetchErrorKind::Terminal
        );
    }
    let (_directory, store) = consensus(&[anchor(&headers[2]), anchor(&headers[3])]);
    let (mut source, _) = fixture();
    let mut reconstruction = RepairReconstruction::new(
        &plan,
        limits(),
        RepairFetchLimits {
            max_headers: 1,
            ..fetching()
        },
        CancellationToken::new(),
    )
    .unwrap();
    assert!(
        reconstruction
            .fetch_next_range(&mut source, &store)
            .await
            .unwrap()
    );
    let rebuilt = reconstruction.finish().unwrap();
    assert_eq!(rebuilt.completions()[0].anchor(), anchor(&headers[2]));
    assert_eq!(rebuilt.segments()[0].rows(), rows);
    assert_eq!(source.body_calls, vec![headers[2].number]);
    assert_eq!(rebuilt.with_current_anchors(&store, || Ok(7)).unwrap(), 7);
    assert_eq!(tree(tmp.path()), before);
}

#[tokio::test]
async fn consensus_reconstruction_admits_every_range_or_none_including_repeated_anchors() {
    let rows = original_rows(&[0, 3]).await;
    let tmp = write(&rows, 2, &[]);
    let before = tree(tmp.path());
    let report = inspect(tmp.path());
    let seeds: Vec<_> = report
        .catalog
        .segments
        .iter()
        .filter(|segment| segment.row_count != 0)
        .map(|segment| segment.id)
        .collect();
    let plan = plan(report, &seeds);
    assert_eq!(plan.block_ranges().len(), 2);
    for repeated in [false, true] {
        let (mut source, headers) = fixture_with_logs(&[0, 3]);
        let anchors = if repeated {
            vec![anchor(&headers[3])]
        } else {
            vec![anchor(&headers[0]), anchor(&headers[3])]
        };
        let (_directory, store) = consensus(&anchors);
        let mut reconstruction =
            RepairReconstruction::new(&plan, limits(), fetching(), CancellationToken::new())
                .unwrap();
        for _ in 0..2 {
            assert!(
                reconstruction
                    .fetch_next_range(&mut source, &store)
                    .await
                    .unwrap()
            );
        }
        let rebuilt = reconstruction.finish().unwrap();
        assert_eq!(rebuilt.completions().len(), 2);
        assert_eq!(rebuilt.completions()[0].anchor(), anchors[0]);
        assert_eq!(rebuilt.completions()[1].anchor(), anchor(&headers[3]));
        let calls = std::cell::Cell::new(0);
        rebuilt
            .with_current_anchors(&store, || {
                calls.set(calls.get() + 1);
                Ok(())
            })
            .unwrap();
        assert_eq!(calls.get(), 1);
        // Removing an earlier completion cannot be hidden by a valid last anchor.
        store
            .replace_anchors(store.ordered_anchors().into_iter().skip(1).collect())
            .unwrap();
        assert_eq!(
            kind(
                &rebuilt
                    .with_current_anchors(&store, || {
                        calls.set(calls.get() + 1);
                        Ok(())
                    })
                    .unwrap_err()
            ),
            RepairFetchErrorKind::Unavailable
        );
        assert_eq!(calls.get(), 1);
        assert_eq!(tree(tmp.path()), before);
    }
}

#[tokio::test(start_paused = true)]
async fn consensus_reconstruction_checks_lifetime_after_finish_before_admission() {
    let rows = original_rows(&[2]).await;
    let tmp = write(&rows, 100, &[]);
    let before = tree(tmp.path());
    let report = inspect(tmp.path());
    let seed = report.catalog.active_hot_segment.unwrap();
    let plan = plan(report, &[seed]);
    for expired in [false, true] {
        let (mut source, headers) = fixture();
        let (_directory, store) = consensus(&[anchor(&headers[2])]);
        let cancellation = CancellationToken::new();
        let mut reconstruction =
            RepairReconstruction::new(&plan, limits(), fetching(), cancellation.clone()).unwrap();
        reconstruction
            .fetch_next_range(&mut source, &store)
            .await
            .unwrap();
        let rebuilt = reconstruction.finish().unwrap();
        if expired {
            tokio::time::advance(Duration::from_secs(60)).await;
        } else {
            cancellation.cancel();
        }
        let calls = std::cell::Cell::new(0);
        let error = rebuilt
            .with_current_anchors(&store, || {
                calls.set(calls.get() + 1);
                Ok(())
            })
            .unwrap_err();
        assert_eq!(calls.get(), 0);
        assert_eq!(
            kind(&error),
            if expired {
                RepairFetchErrorKind::Deadline
            } else {
                RepairFetchErrorKind::Cancelled
            }
        );
        assert_eq!(tree(tmp.path()), before);
    }
}

#[tokio::test]
async fn consensus_reconstruction_empty_owner_needs_no_chain_anchor() {
    let tmp = write(&[], 100, &[]);
    let before = tree(tmp.path());
    let report = inspect(tmp.path());
    let seed = report.catalog.active_hot_segment.unwrap();
    let plan = plan(report, &[seed]);
    let (_directory, store) = consensus(&[]);
    let (mut source, _) = fixture();
    let cancellation = CancellationToken::new();
    let mut reconstruction =
        RepairReconstruction::new(&plan, limits(), fetching(), cancellation.clone()).unwrap();
    assert!(
        !reconstruction
            .fetch_next_range(&mut source, &store)
            .await
            .unwrap()
    );
    let rebuilt = reconstruction.finish().unwrap();
    assert!(rebuilt.completions().is_empty());
    assert_eq!(rebuilt.with_current_anchors(&store, || Ok(3)).unwrap(), 3);
    // Admission forwards publication errors; it does not claim to roll back a callback.
    let error = rebuilt
        .with_current_anchors::<()>(&store, || Err(eyre::eyre!("fixture publication error")))
        .unwrap_err();
    assert_eq!(error.to_string(), "fixture publication error");
    cancellation.cancel();
    let error = rebuilt.with_current_anchors(&store, || Ok(())).unwrap_err();
    assert_eq!(kind(&error), RepairFetchErrorKind::Cancelled);
    assert!(source.body_calls.is_empty());
    assert_eq!(tree(tmp.path()), before);
}
