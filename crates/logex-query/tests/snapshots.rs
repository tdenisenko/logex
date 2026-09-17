use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes};
use logex_index::IndexBuilder;
use logex_query::{
    NativeStorageSnapshot, SqlQueryError, SqlQueryPage, execute_sql_page_on_snapshot,
};
use logex_storage::{PartitionManager, PartitionManagerConfig, SegmentReader};
use logex_types::{LogRow, Source};
use serde_json::json;
use std::{fs, path::Path};
use tempfile::TempDir;

fn header(number: u64, parent: B256) -> Header {
    Header {
        number,
        parent_hash: parent,
        timestamp: number * 12,
        ..Default::default()
    }
}

fn row(header: &Header, log_index: u32) -> LogRow {
    LogRow {
        block_number: header.number,
        block_hash: header.hash_slow(),
        timestamp: header.timestamp,
        tx_hash: B256::repeat_byte(1),
        tx_index: 0,
        log_index,
        address: Address::repeat_byte(0xaa),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: Bytes::new(),
        data_len: 0,
        source: Source::Receipt,
    }
}

fn write(storage: &mut PartitionManager, bundled: bool, header: &Header, rows: &[LogRow]) {
    if bundled {
        storage
            .ingest_canonical_batch(rows, header, std::slice::from_ref(header), None)
            .unwrap();
    } else {
        storage.write_batch(rows).unwrap();
    }
    storage.checkpoint().unwrap();
}

async fn snapshot_after_change(bundled: bool, reorg: bool) {
    let tmp = TempDir::new().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let first = header(100, B256::ZERO);
    write(
        &mut storage,
        bundled,
        &first,
        &[row(&first, 0), row(&first, 1)],
    );
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    if reorg {
        assert_eq!(storage.mark_non_canonical(first.hash_slow()).unwrap(), 2);
    } else {
        let next = header(101, first.hash_slow());
        write(&mut storage, bundled, &next, &[row(&next, 0)]);
    }
    let expected = vec![
        json!({"block_number":100,"log_index":0}),
        json!({"block_number":100,"log_index":1}),
    ];
    let mut failures = Vec::new();
    for (sql, expected) in [
        (
            "SELECT block_number, log_index FROM logs ORDER BY block_number ASC, tx_index ASC, log_index ASC",
            expected.clone(),
        ),
        (
            "SELECT block_number, log_index FROM logs ORDER BY block_number + 0 ASC, tx_index ASC, log_index ASC",
            expected,
        ),
        (
            "SELECT COUNT(*) AS total FROM logs",
            vec![json!({"total":2})],
        ),
    ] {
        let result =
            execute_sql_page_on_snapshot(sql, snapshot.clone(), 100, SqlQueryPage::default(), None)
                .await;
        if reorg {
            assert!(
                matches!(result, Err(SqlQueryError::SnapshotChanged)),
                "{sql}: {result:?}"
            );
            let retry = execute_sql_page_on_snapshot(
                sql,
                NativeStorageSnapshot::from_storage(&storage),
                100,
                SqlQueryPage::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(
                retry.rows,
                if sql.contains("COUNT") {
                    vec![json!({"total":0})]
                } else {
                    vec![]
                }
            );
            continue;
        }
        let actual = result.unwrap();
        if actual.rows != expected {
            failures.push((sql, actual.rows, expected));
        }
    }
    assert!(
        failures.is_empty(),
        "bundled={bundled} reorg={reorg}: {failures:?}"
    );
}

#[tokio::test]
async fn bundled_snapshot_excludes_later_ingestion() {
    snapshot_after_change(true, false).await;
}

#[tokio::test]
async fn raw_snapshot_excludes_later_ingestion() {
    snapshot_after_change(false, false).await;
}

#[tokio::test]
async fn bundled_snapshot_rejects_changed_canonicality() {
    snapshot_after_change(true, true).await;
}

#[tokio::test]
async fn raw_snapshot_rejects_changed_canonicality() {
    snapshot_after_change(false, true).await;
}

#[tokio::test]
async fn snapshot_survives_completed_raw_compaction() {
    let tmp = TempDir::new().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let first = header(100, B256::ZERO);
    write(
        &mut storage,
        false,
        &first,
        &[row(&first, 0), row(&first, 1)],
    );
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    assert_eq!(storage.compact_eligible_segments().unwrap(), 1);
    let result = execute_sql_page_on_snapshot(
        "SELECT log_index FROM logs ORDER BY block_number ASC, tx_index ASC, log_index ASC",
        snapshot,
        100,
        SqlQueryPage::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        result.rows,
        vec![json!({"log_index":0}), json!({"log_index":1})]
    );
}

fn fixture(bundled: bool) -> (TempDir, PartitionManager, Header) {
    let tmp = TempDir::new().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let first = header(100, B256::ZERO);
    let mut rows = vec![row(&first, 0), row(&first, 1)];
    for (i, row) in rows.iter_mut().enumerate() {
        let mut data = vec![0; 32];
        data[31] = (i + 1) as u8;
        row.data = data.into();
        row.data_len = 32;
    }
    write(&mut storage, bundled, &first, &rows);
    (tmp, storage, first)
}

const QUERIES: &[&str] = &[
    "SELECT block_number, log_index FROM logs ORDER BY block_number, tx_index, log_index",
    "SELECT block_number, log_index FROM logs ORDER BY block_number + 0, tx_index, log_index",
    "SELECT COUNT(*) AS total FROM logs",
    "SELECT source, COUNT(*) AS total FROM logs GROUP BY source",
    "SELECT SUM(data) AS total FROM logs WHERE data_len = 32",
    "SELECT MIN(block_number) AS first, MAX(block_number) AS last FROM logs",
];

#[tokio::test]
async fn reorg_at_each_query_checkpoint_returns_retryable_error() {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    for bundled in [false, true] {
        for &sql in QUERIES {
            // Discover actual execution checkpoints, then mutate at each one,
            // including the final callback after rows have been materialized.
            let (tmp, storage, _) = fixture(bundled);
            let calls = Arc::new(AtomicUsize::new(0));
            let counter = calls.clone();
            execute_sql_page_on_snapshot(
                sql,
                NativeStorageSnapshot::from_storage(&storage),
                100,
                SqlQueryPage::default(),
                Some(Arc::new(move || {
                    counter.fetch_add(1, Ordering::SeqCst);
                    false
                })),
            )
            .await
            .unwrap();
            let checks = calls.load(Ordering::SeqCst);
            assert!(checks >= 2, "{sql}: {checks}");
            drop(storage);
            drop(tmp);
            for change_at in 0..checks {
                let (_tmp, storage, first) = fixture(bundled);
                let snapshot = NativeStorageSnapshot::from_storage(&storage);
                let storage = Arc::new(Mutex::new(storage));
                let writer = storage.clone();
                let calls = AtomicUsize::new(0);
                let result = execute_sql_page_on_snapshot(
                    sql,
                    snapshot,
                    100,
                    SqlQueryPage::default(),
                    Some(Arc::new(move || {
                        if calls.fetch_add(1, Ordering::SeqCst) == change_at {
                            assert_eq!(
                                writer
                                    .lock()
                                    .unwrap()
                                    .mark_non_canonical(first.hash_slow())
                                    .unwrap(),
                                2
                            );
                        }
                        false
                    })),
                )
                .await;
                assert!(
                    matches!(result, Err(SqlQueryError::SnapshotChanged)),
                    "bundled={bundled}, checkpoint={change_at}/{checks}, {sql}: {result:?}"
                );
                assert!(storage.lock().unwrap().read_view_token().is_valid());
            }
        }
    }
}

#[tokio::test]
async fn snapshots_bound_current_indexes_and_every_aggregate_path() {
    use logex_index::IndexBuilder;
    for bundled in [false, true] {
        for historical in [false, true] {
            let (_tmp, mut storage, first) = fixture(bundled);
            if historical {
                storage
                    .write_historical_batch(&[row(&header(90, B256::ZERO), 0)])
                    .unwrap();
                storage.checkpoint().unwrap();
            }
            let snapshot = NativeStorageSnapshot::from_storage(&storage);
            let mut expected = Vec::new();
            for &sql in QUERIES {
                expected.push(
                    execute_sql_page_on_snapshot(
                        sql,
                        snapshot.clone(),
                        100,
                        SqlQueryPage::default(),
                        None,
                    )
                    .await
                    .unwrap()
                    .rows,
                );
            }
            let next = header(101, first.hash_slow());
            write(&mut storage, bundled, &next, &[row(&next, 0)]);
            if historical {
                storage
                    .write_historical_batch(&[row(&header(89, B256::ZERO), 0)])
                    .unwrap();
                storage.checkpoint().unwrap();
            }
            for partition in storage.sealed_partitions() {
                IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
            }
            IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
            for (&sql, expected) in QUERIES.iter().zip(expected) {
                let result = execute_sql_page_on_snapshot(
                    sql,
                    snapshot.clone(),
                    100,
                    SqlQueryPage::default(),
                    None,
                )
                .await
                .unwrap();
                assert_eq!(
                    result.rows, expected,
                    "bundled={bundled}, historical={historical}: {sql}"
                );
            }
            // Force indexed candidates to include the new row as well as the
            // captured prefix; bounds must be applied after a valid index hit.
            let result = execute_sql_page_on_snapshot(
                "SELECT log_index FROM logs WHERE address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND block_number >= 100 ORDER BY block_number, tx_index, log_index",
                snapshot, 100, SqlQueryPage::default(), None,
            ).await.unwrap();
            assert_eq!(
                result.rows,
                vec![json!({"log_index":0}), json!({"log_index":1})]
            );
        }
    }
}

#[tokio::test]
async fn closing_storage_invalidates_old_views_even_after_reopen() {
    let (tmp, storage, _) = fixture(true);
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    drop(storage);
    let reopened = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    for &sql in QUERIES {
        assert!(matches!(
            execute_sql_page_on_snapshot(sql, snapshot.clone(), 100, SqlQueryPage::default(), None)
                .await,
            Err(SqlQueryError::SnapshotChanged)
        ));
    }
    let result = execute_sql_page_on_snapshot(
        QUERIES[2],
        NativeStorageSnapshot::from_storage(&reopened),
        100,
        SqlQueryPage::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(result.rows, vec![json!({"total":2})]);
}

#[tokio::test]
async fn captured_prefix_survives_sparse_bundle_repacking() {
    use logex_storage::native::SegmentManifest;
    for historical in [false, true] {
        let tmp = TempDir::new().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_owned(),
            partition_target_rows: 10_000,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        let mut prior = header(256, B256::ZERO);
        for i in 0..127 {
            let next = header(
                if historical { 256 - i } else { 256 + i },
                prior.hash_slow(),
            );
            if historical {
                storage
                    .ingest_historical_batch(&[row(&next, 0)], &next)
                    .unwrap();
            } else {
                storage
                    .ingest_canonical_batch(
                        &[row(&next, 0)],
                        &next,
                        &[prior.clone(), next.clone()],
                        None,
                    )
                    .unwrap();
            }
            prior = next;
        }
        storage.checkpoint().unwrap();
        let snapshot = NativeStorageSnapshot::from_storage(&storage);
        let path = snapshot.partitions_in_order(logex_storage::native::LogOrder::Ascending)[0]
            .path
            .clone();
        let manifest = || -> SegmentManifest {
            serde_json::from_slice(&std::fs::read(path.join("segment.json")).unwrap()).unwrap()
        };
        let generation = manifest().generation;
        let expected = execute_sql_page_on_snapshot(
            QUERIES[0],
            snapshot.clone(),
            500,
            SqlQueryPage::default(),
            None,
        )
        .await
        .unwrap()
        .rows;
        let next = header(
            if historical {
                prior.number - 1
            } else {
                prior.number + 1
            },
            prior.hash_slow(),
        );
        if historical {
            storage
                .ingest_historical_batch(&[row(&next, 0)], &next)
                .unwrap();
        } else {
            storage
                .ingest_canonical_batch(&[row(&next, 0)], &next, &[prior, next.clone()], None)
                .unwrap();
        }
        storage.checkpoint().unwrap();
        assert!(manifest().generation > generation, "exercise a real repack");
        for sql in [QUERIES[0], QUERIES[1]] {
            let result = execute_sql_page_on_snapshot(
                sql,
                snapshot.clone(),
                500,
                SqlQueryPage::default(),
                None,
            )
            .await
            .unwrap();
            assert_eq!(result.rows, expected);
        }
        let result =
            execute_sql_page_on_snapshot(QUERIES[2], snapshot, 500, SqlQueryPage::default(), None)
                .await
                .unwrap();
        assert_eq!(result.rows, vec![json!({"total":127})]);
    }
}

fn copy_fixture_tree(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let destination = target.join(entry.file_name());
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            copy_fixture_tree(&entry.path(), &destination);
        } else {
            assert!(kind.is_file(), "fixture contains only regular files");
            fs::copy(entry.path(), destination).unwrap();
        }
    }
}

fn copy_index_directory(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file());
        fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
    }
}

fn reopen(path: &Path) -> PartitionManager {
    PartitionManager::open(PartitionManagerConfig {
        data_dir: path.to_path_buf(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap()
}

#[tokio::test]
async fn divergent_complete_copy_indexes_preserve_public_sql_results() {
    let (source_tmp, mut source, first) = fixture(false);
    source.checkpoint_durable().unwrap();
    drop(source);

    let target_tmp = TempDir::new().unwrap();
    copy_fixture_tree(source_tmp.path(), target_tmp.path());
    let mut source = reopen(source_tmp.path());
    let mut target = reopen(target_tmp.path());
    let next = header(101, first.hash_slow());
    let mut source_append = row(&next, 0);
    source_append.address = Address::repeat_byte(0xbb);
    let mut target_append = row(&next, 0);
    target_append.address = Address::repeat_byte(0xcc);
    write(&mut source, false, &next, &[source_append]);
    write(&mut target, false, &next, &[target_append]);

    let source_dir = source.hot_partition().meta.path.clone();
    let target_dir = target.hot_partition().meta.path.clone();
    IndexBuilder::build_all_indexes(&source_dir).unwrap();
    IndexBuilder::build_all_indexes(&target_dir).unwrap();

    let mut expected = SegmentReader::open(&target_dir)
        .unwrap()
        .read_log_rows(None)
        .unwrap()
        .into_iter()
        .filter(|row| row.address == Address::repeat_byte(0xcc))
        .collect::<Vec<_>>();
    expected.sort_by_key(|row| (row.block_number, row.tx_index, row.log_index));
    assert_eq!(expected.len(), 1);
    copy_index_directory(&source_dir.join("indexes"), &target_dir.join("indexes"));

    let snapshot = NativeStorageSnapshot::from_storage(&target);
    let address = format!("0x{}", hex::encode(Address::repeat_byte(0xcc)));
    let selected = execute_sql_page_on_snapshot(
        &format!(
            "SELECT block_number, log_index FROM logs WHERE address = '{address}' \
             ORDER BY block_number, tx_index, log_index"
        ),
        snapshot.clone(),
        next.number,
        SqlQueryPage::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        selected.rows,
        expected
            .iter()
            .map(|row| json!({
                "block_number": row.block_number,
                "log_index": row.log_index,
            }))
            .collect::<Vec<_>>()
    );

    let count = execute_sql_page_on_snapshot(
        &format!("SELECT COUNT(*) AS total FROM logs WHERE address = '{address}'"),
        snapshot,
        next.number,
        SqlQueryPage::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(count.rows, vec![json!({"total": expected.len()})]);
}

#[tokio::test]
async fn old_snapshot_uses_current_source_with_stale_checkpoint() {
    for bundled in [false, true] {
        let (_tmp, mut storage, first) = fixture(bundled);
        let path = storage.hot_partition().meta.path.clone();
        IndexBuilder::build_all_indexes(&path).unwrap();
        let snapshot = NativeStorageSnapshot::from_storage(&storage);
        let captured_rows = snapshot
            .partitions_in_order(logex_storage::native::LogOrder::Ascending)
            .into_iter()
            .find(|partition| partition.path == path)
            .unwrap()
            .row_count;

        let next = header(101, first.hash_slow());
        write(&mut storage, bundled, &next, &[row(&next, 0)]);
        assert!(storage.read_view_token().is_valid());
        let current_rows = SegmentReader::open(&path)
            .unwrap()
            .read_log_rows(None)
            .unwrap();
        assert!(u64::try_from(current_rows.len()).unwrap() > captured_rows);
        let mut expected = current_rows
            .into_iter()
            .take(usize::try_from(captured_rows).unwrap())
            .filter(|row| row.address == Address::repeat_byte(0xaa))
            .collect::<Vec<_>>();
        expected.sort_by_key(|row| (row.block_number, row.tx_index, row.log_index));

        let selected = execute_sql_page_on_snapshot(
            "SELECT block_number, log_index FROM logs \
             WHERE address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
             ORDER BY block_number, tx_index, log_index",
            snapshot.clone(),
            next.number,
            SqlQueryPage::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            selected.rows,
            expected
                .iter()
                .map(|row| json!({
                    "block_number": row.block_number,
                    "log_index": row.log_index,
                }))
                .collect::<Vec<_>>(),
            "bundled={bundled}"
        );

        let count = execute_sql_page_on_snapshot(
            "SELECT COUNT(*) AS total FROM logs \
             WHERE address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'",
            snapshot,
            next.number,
            SqlQueryPage::default(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            count.rows,
            vec![json!({"total": expected.len()})],
            "bundled={bundled}"
        );
    }
}

// New API conformance controls: the original API could not execute a retained
// snapshot after releasing its borrow of the live storage manager.
#[test]
fn native_snapshot_pages_exclude_appends_and_preserve_order() {
    use logex_query::{execute_log_filter, execute_log_filter_on_snapshot_with_cancel};
    use logex_storage::native::{LogOrder, NativeLogFilter};
    for bundled in [false, true] {
        let (_tmp, mut storage, first) = fixture(bundled);
        let snapshot = NativeStorageSnapshot::from_storage(&storage);
        let next = header(101, first.hash_slow());
        write(&mut storage, bundled, &next, &[row(&next, 0)]);
        for (order, expected_index) in [(LogOrder::Ascending, 1), (LogOrder::Descending, 0)] {
            let filter = NativeLogFilter {
                order,
                limit: Some(1),
                offset: 1,
                ..Default::default()
            };
            let rows =
                execute_log_filter_on_snapshot_with_cancel(&snapshot, &filter, None).unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(
                (rows[0].block_number, rows[0].log_index),
                (100, expected_index)
            );
        }
        let filter = NativeLogFilter::default();
        std::thread::scope(|scope| {
            let one = scope
                .spawn(|| execute_log_filter_on_snapshot_with_cancel(&snapshot, &filter, None));
            let two = scope
                .spawn(|| execute_log_filter_on_snapshot_with_cancel(&snapshot, &filter, None));
            for rows in [one.join().unwrap().unwrap(), two.join().unwrap().unwrap()] {
                assert_eq!(
                    rows.iter()
                        .map(|row| (row.block_number, row.log_index))
                        .collect::<Vec<_>>(),
                    vec![(100, 0), (100, 1)]
                );
            }
        });
        assert_eq!(execute_log_filter(&storage, &filter).unwrap().len(), 3);
    }
}

#[test]
fn native_snapshot_survives_raw_compaction() {
    use logex_query::execute_log_filter_on_snapshot_with_cancel;
    let tmp = TempDir::new().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let first = header(100, B256::ZERO);
    write(
        &mut storage,
        false,
        &first,
        &[row(&first, 0), row(&first, 1)],
    );
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    assert_eq!(storage.compact_eligible_segments().unwrap(), 1);
    let rows =
        execute_log_filter_on_snapshot_with_cancel(&snapshot, &Default::default(), None).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.log_index).collect::<Vec<_>>(),
        vec![0, 1]
    );
}

#[test]
fn native_snapshot_checks_cancellation_and_invalidation_at_each_checkpoint() {
    use logex_query::{QueryCancelCheck, execute_log_filter_on_snapshot_with_cancel};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    for bundled in [false, true] {
        let (_tmp, storage, _) = fixture(bundled);
        let snapshot = NativeStorageSnapshot::from_storage(&storage);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let check: QueryCancelCheck = Arc::new(move || {
            count.fetch_add(1, Ordering::SeqCst);
            false
        });
        execute_log_filter_on_snapshot_with_cancel(&snapshot, &Default::default(), Some(&check))
            .unwrap();
        let checkpoints = calls.load(Ordering::SeqCst);
        assert!(checkpoints >= 3);
        for checkpoint in 0..checkpoints {
            for invalidate in [false, true] {
                let (_tmp, storage, first) = fixture(bundled);
                let snapshot = NativeStorageSnapshot::from_storage(&storage);
                let storage = Arc::new(Mutex::new(storage));
                let owner = storage.clone();
                let calls = AtomicUsize::new(0);
                let check: QueryCancelCheck = Arc::new(move || {
                    if calls.fetch_add(1, Ordering::SeqCst) == checkpoint {
                        if invalidate {
                            owner
                                .lock()
                                .unwrap()
                                .mark_non_canonical(first.hash_slow())
                                .unwrap();
                        }
                        // Invalid views take precedence even on an error exit.
                        true
                    } else {
                        false
                    }
                });
                let error = execute_log_filter_on_snapshot_with_cancel(
                    &snapshot,
                    &Default::default(),
                    Some(&check),
                )
                .unwrap_err();
                assert_eq!(
                    error.kind(),
                    if invalidate {
                        std::io::ErrorKind::WouldBlock
                    } else {
                        std::io::ErrorKind::Interrupted
                    },
                    "bundled={bundled}, checkpoint={checkpoint}"
                );
                if !invalidate {
                    assert_eq!(
                        execute_log_filter_on_snapshot_with_cancel(
                            &snapshot,
                            &Default::default(),
                            None
                        )
                        .unwrap()
                        .len(),
                        2
                    );
                }
            }
        }
    }
}

#[test]
fn native_snapshot_rejects_closed_or_invalid_views_even_for_zero_limit() {
    use logex_query::{QueryCancelCheck, execute_log_filter_on_snapshot_with_cancel};
    use logex_storage::native::NativeLogFilter;
    use std::sync::Arc;
    for bundled in [false, true] {
        let (_tmp, mut storage, first) = fixture(bundled);
        let snapshot = NativeStorageSnapshot::from_storage(&storage);
        let empty = NativeLogFilter {
            limit: Some(0),
            ..Default::default()
        };
        assert!(
            execute_log_filter_on_snapshot_with_cancel(&snapshot, &empty, None)
                .unwrap()
                .is_empty()
        );
        let cancel: QueryCancelCheck = Arc::new(|| true);
        assert_eq!(
            execute_log_filter_on_snapshot_with_cancel(&snapshot, &empty, Some(&cancel))
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::Interrupted
        );
        storage.mark_non_canonical(first.hash_slow()).unwrap();
        assert_eq!(
            execute_log_filter_on_snapshot_with_cancel(&snapshot, &empty, None)
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
        let snapshot = NativeStorageSnapshot::from_storage(&storage);
        drop(storage);
        for filter in [empty, NativeLogFilter::default()] {
            assert_eq!(
                execute_log_filter_on_snapshot_with_cancel(&snapshot, &filter, None)
                    .unwrap_err()
                    .kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
}
