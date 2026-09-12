use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes};
use logex_query::{
    NativeStorageSnapshot, SqlQueryError, SqlQueryPage, execute_sql_page_on_snapshot,
};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::json;
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
