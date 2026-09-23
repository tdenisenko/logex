//! Disposable writer/fetcher integration; scripted anchors are not CL proofs.
use super::*;
use crate::repair::tests::{anchor, fixture, fixture_with_logs, limits as fetch_limits};
use logex_storage::ColumnFileHeader;
use logex_storage::native::{
    InspectionLimits, NativeStorage, NativeStorageConfig, PrimaryDataInspection, RepairPlanLimits,
    StorageCatalogPaths, inspect_primary_data,
};
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

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
        reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    assert!(
        !reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
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
        reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    assert!(
        reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
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
        !reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
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
        let result = reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
            .await;
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
            reconstruction.fetch_next_range(&mut source, anchor(&headers[3]))
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
        reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
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
        let error = reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
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
        reconstruction
            .fetch_next_range(&mut source, anchor(&headers[3]))
            .await
            .unwrap()
    );
    let error = reconstruction.finish().unwrap_err();
    assert_eq!(kind(&error), RepairFetchErrorKind::Local);
    assert!(format!("{error:#}").contains("original logical commitment"));
    assert_eq!(tree(tmp.path()), before);
}
