//! Scan-planning regressions using unchanged, captured Ethereum event logs.
use logex_index::IndexBuilder;
use logex_query::{
    NativeStorageSnapshot, SqlQueryError, SqlQueryPage, execute_sql_page_on_snapshot_with_memory,
};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, QueryMemoryBudget, QueryMemoryLimit};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Deserialize)]
struct Fixture {
    rows: Vec<LogRow>,
}

fn fixture() -> Vec<LogRow> {
    serde_json::from_str::<Fixture>(include_str!("fixtures/mainnet-exact-sums.json"))
        .unwrap()
        .rows
}

fn storage(rows: &[LogRow]) -> (tempfile::TempDir, PartitionManager, PartitionManagerConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 4,
        compaction_safety_margin_blocks: 0,
    };
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    for block in rows.chunk_by(|a, b| a.block_number == b.block_number) {
        storage.write_batch(block).unwrap();
    }
    storage.checkpoint().unwrap();
    assert!(storage.sealed_count() >= 8);
    (tmp, storage, config)
}

fn topic(row: &LogRow, index: usize) -> Option<String> {
    [row.topic0, row.topic1, row.topic2, row.topic3][index].map(|v| v.to_string())
}

fn projected(rows: impl Iterator<Item = LogRow>) -> Vec<Value> {
    let mut rows: Vec<_> = rows.collect();
    rows.sort_by_key(|r| (r.block_number, r.log_index));
    rows.iter()
        .map(|r| json!({"block_number":r.block_number,"log_index":r.log_index}))
        .collect()
}

async fn check(storage: &PartitionManager, sql: &str, expected: &[Value], candidates: u64) {
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(64 * 1024 * 1024).unwrap());
    let result = execute_sql_page_on_snapshot_with_memory(
        sql,
        NativeStorageSnapshot::for_unverified_inspection(storage),
        storage.head_block().unwrap(),
        SqlQueryPage::default(),
        None,
        memory.clone(),
    )
    .await
    .unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(result.rows.as_slice(), expected, "{sql}");
    assert_eq!(result.total_scanned, candidates, "{sql}");
    drop(result);
    assert_eq!(memory.used(), 0, "{sql}");
}

#[tokio::test]
async fn mainnet_disjunctions_keep_exact_selection_across_storage_layouts() {
    let rows = fixture();
    let (_tmp, mut storage, config) = storage(&rows);
    for layout in 0..4 {
        if layout == 1 {
            storage.compact_eligible_segments().unwrap();
        }
        if layout == 2 {
            for partition in storage
                .sealed_partitions()
                .iter()
                .chain(std::iter::once(storage.hot_partition()))
            {
                if partition.meta.row_count > 0 {
                    IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
                }
            }
        }
        if layout == 3 {
            drop(storage);
            storage = PartitionManager::open(config.clone()).unwrap();
        }
        for column in ["address", "topic0", "topic1", "topic2", "topic3"] {
            let value = |row: &LogRow| {
                if column == "address" {
                    Some(format!("0x{}", hex::encode(row.address)))
                } else {
                    topic(row, column[5..].parse().unwrap())
                }
            };
            let mut keys: Vec<_> = rows.iter().filter_map(value).collect();
            keys.sort();
            keys.dedup();
            assert!(keys.len() >= 2);
            let (a, b) = (&keys[0], &keys[1]);
            let matching = projected(
                rows.iter()
                    .filter(|r| value(r).is_some_and(|v| v == *a || v == *b))
                    .cloned(),
            );
            for predicate in [
                format!("{column} IN ('{a}','{b}')"),
                format!("{column}='{a}' OR {column}='{b}'"),
                format!(
                    "({column}='{a}' OR {column} IN ('{b}','{a}')) AND {column} IN ('{a}','{b}')"
                ),
            ] {
                // Arithmetic projection forces the general SQL provider.
                check(&storage, &format!("SELECT block_number, CAST(log_index+0 AS BIGINT) AS log_index FROM logs WHERE {predicate} ORDER BY block_number, log_index"), &matching, matching.len() as u64).await;
            }
            let restricted = projected(
                rows.iter()
                    .filter(|r| value(r).as_ref() == Some(a))
                    .cloned(),
            );
            check(&storage, &format!("SELECT block_number, CAST(log_index+0 AS BIGINT) AS log_index FROM logs WHERE ({column}='{a}' OR {column}='{b}') AND {column}='{a}' ORDER BY block_number, log_index"), &restricted, restricted.len() as u64).await;
        }
    }
}

#[tokio::test]
async fn mainnet_multi_scan_counts_include_both_union_inputs() {
    let rows = fixture();
    let (_tmp, storage, _) = storage(&rows);
    let deposit = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c";
    let withdrawal = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65";
    let expected = projected(
        rows.iter()
            .filter(|r| topic(r, 0).is_some_and(|v| v == deposit || v == withdrawal))
            .cloned(),
    );
    assert_eq!(expected.len(), 32);
    let sql = format!(
        "WITH a AS (SELECT block_number, log_index FROM logs WHERE topic0='{deposit}'), b AS (SELECT block_number, log_index FROM logs WHERE topic0='{withdrawal}') SELECT block_number, log_index FROM a UNION ALL SELECT block_number, log_index FROM b ORDER BY block_number, log_index"
    );
    check(&storage, &sql, &expected, 32).await;
}

#[tokio::test]
async fn mainnet_parallel_selection_releases_capacity_and_canceled_work() {
    let rows = fixture();
    let (_tmp, storage, _) = storage(&rows);
    let sql = "SELECT block_number, CAST(log_index+0 AS BIGINT) AS log_index FROM logs ORDER BY block_number, log_index";
    for bytes in [1, 1024, 4096] {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(bytes).unwrap());
        let error = execute_sql_page_on_snapshot_with_memory(
            sql,
            NativeStorageSnapshot::for_unverified_inspection(&storage),
            storage.head_block().unwrap(),
            SqlQueryPage::default(),
            None,
            memory.clone(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, SqlQueryError::Capacity(_)), "{error}");
        assert_eq!(memory.used(), 0);
    }
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(64 * 1024 * 1024).unwrap());
    let canceled = Arc::new(AtomicBool::new(false));
    let seen = canceled.clone();
    let used = memory.clone();
    let cancel = Arc::new(move || {
        if used.used() > 0 {
            seen.store(true, Ordering::Relaxed);
        }
        seen.load(Ordering::Relaxed)
    });
    let error = execute_sql_page_on_snapshot_with_memory(
        sql,
        NativeStorageSnapshot::for_unverified_inspection(&storage),
        storage.head_block().unwrap(),
        SqlQueryPage::default(),
        Some(cancel),
        memory.clone(),
    )
    .await
    .unwrap_err();
    assert!(canceled.load(Ordering::Relaxed));
    assert!(error.to_string().contains("query canceled"), "{error}");
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn mainnet_mixed_topic_disjunctions_select_unique_candidates() {
    let rows = fixture();
    let (_tmp, mut storage, config) = storage(&rows);
    let wallet = rows
        .iter()
        .find_map(|row| {
            (row.topic1.is_some() && row.topic1 == row.topic2)
                .then_some(row.topic1)
                .flatten()
        })
        .expect("actual event with the same indexed participant twice");
    let deposit = rows
        .iter()
        .find(|row| row.topic2.is_none())
        .unwrap()
        .topic0
        .unwrap();
    for layout in 0..4 {
        if layout == 1 {
            storage.compact_eligible_segments().unwrap();
        }
        if layout == 2 {
            for partition in storage
                .sealed_partitions()
                .iter()
                .chain(std::iter::once(storage.hot_partition()))
            {
                if partition.meta.row_count > 0 {
                    IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
                }
            }
        }
        if layout == 3 {
            drop(storage);
            storage = PartitionManager::open(config.clone()).unwrap();
        }
        for with_deposits in [false, true] {
            let predicate = format!(
                "topic1='{wallet}' OR topic2='{wallet}'{}",
                if with_deposits {
                    format!(" OR topic0='{deposit}'")
                } else {
                    String::new()
                }
            );
            let expected = projected(
                rows.iter()
                    .filter(|row| {
                        row.topic1 == Some(wallet)
                            || row.topic2 == Some(wallet)
                            || (with_deposits && row.topic0 == Some(deposit))
                    })
                    .cloned(),
            );
            assert!(!expected.is_empty() && expected.len() < rows.len());
            let sql = format!(
                "SELECT block_number, CAST(log_index+0 AS BIGINT) AS log_index FROM logs WHERE {predicate} ORDER BY block_number, log_index"
            );
            check(&storage, &sql, &expected, expected.len() as u64).await;
            // A pushed LIMIT must not run before the retained SQL predicate.
            let limited = format!("{sql} LIMIT 2 OFFSET 1");
            check(&storage, &limited, &expected[1..3], expected.len() as u64).await;
            let residual = projected(
                rows.iter()
                    .filter(|row| {
                        (row.topic1 == Some(wallet)
                            || row.topic2 == Some(wallet)
                            || (with_deposits && row.topic0 == Some(deposit)))
                            && row.topic2.is_some()
                            && row.log_index % 2 == 0
                    })
                    .cloned(),
            );
            assert!(residual.len() > 1 && residual.len() < expected.len());
            let sql = format!(
                "SELECT block_number, CAST(log_index+0 AS BIGINT) AS log_index FROM logs WHERE ({predicate}) AND topic2 IS NOT NULL AND log_index % 2 = 0 ORDER BY block_number, log_index LIMIT 1 OFFSET 1"
            );
            check(&storage, &sql, &residual[1..2], expected.len() as u64).await;
        }
    }
}
