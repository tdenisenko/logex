//! Public-query controls for independently owned lazy scan executions.
use alloy_primitives::{Address, B256, Bytes};
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::{Value, json};

fn fixture(separate_partitions: bool) -> (tempfile::TempDir, PartitionManager) {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: if separate_partitions { 1 } else { 3 },
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    for i in 0..3 {
        storage
            .write_batch(&[LogRow {
                block_number: u64::from(i) + 1,
                block_hash: B256::repeat_byte(i as u8 + 1),
                timestamp: u64::from(i) + 1,
                tx_hash: B256::repeat_byte(i as u8 + 1),
                tx_index: 0,
                log_index: i,
                address: Address::repeat_byte(0xaa),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::new(),
                data_len: 0,
                source: Source::Receipt,
            }])
            .unwrap();
        if separate_partitions {
            storage.checkpoint().unwrap();
        }
    }
    storage.checkpoint().unwrap();
    assert_eq!(
        storage.sealed_partitions().len(),
        if separate_partitions { 3 } else { 1 }
    );
    (tmp, storage)
}

async fn check(sql: &str, expected: Vec<Value>) {
    let mut failures = Vec::new();
    for separate in [false, true] {
        let (_tmp, storage) = fixture(separate);
        let result = execute_sql(sql, &storage, storage.head_block()).await;
        match result {
            Ok(result) if result.rows == expected => {}
            result => failures.push(format!("partitions={separate}: {result:?}")),
        }
    }
    assert!(failures.is_empty(), "{sql}: {}", failures.join("\n"));
}

#[tokio::test]
async fn repeated_cte_union_scans_preserve_both_branches() {
    check(
        "WITH c AS (SELECT log_index FROM logs) SELECT log_index FROM c UNION ALL SELECT log_index FROM c ORDER BY log_index",
        (0..3)
            .flat_map(|i| [json!({"log_index":i}), json!({"log_index":i})])
            .collect(),
    )
    .await;
}

#[tokio::test]
async fn self_join_and_cross_join_preserve_each_input() {
    check(
        "SELECT COUNT(*) AS total FROM logs a JOIN logs b ON a.log_index = b.log_index",
        vec![json!({"total":3})],
    )
    .await;
    check(
        "SELECT COUNT(*) AS total FROM logs a CROSS JOIN logs b",
        vec![json!({"total":9})],
    )
    .await;
}

#[tokio::test]
async fn repeated_scalar_subqueries_preserve_each_input() {
    check(
        "SELECT (SELECT COUNT(*) FROM logs) AS a, (SELECT COUNT(*) FROM logs) AS b",
        vec![json!({"a":3,"b":3})],
    )
    .await;
}

#[tokio::test]
async fn recursive_query_rescans_log_input_at_each_iteration() {
    check(
        "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r JOIN logs ON log_index = 0 WHERE n < 4) SELECT n FROM r ORDER BY n",
        (1..=4).map(|n| json!({"n":n})).collect(),
    )
    .await;
}

#[tokio::test]
async fn concurrent_public_queries_have_independent_scan_ownership() {
    let (_tmp, storage) = fixture(true);
    let sql = "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r JOIN logs ON log_index = 0 WHERE n < 4) SELECT n FROM r ORDER BY n";
    let (first, second) = tokio::join!(
        execute_sql(sql, &storage, storage.head_block()),
        execute_sql(sql, &storage, storage.head_block()),
    );
    for result in [first, second] {
        assert_eq!(
            result.unwrap().rows,
            (1..=4).map(|n| json!({"n":n})).collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn empty_recursive_log_scan_keeps_only_the_seed() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 3,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let sql = "WITH RECURSIVE r AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM r JOIN logs ON log_index = 0 WHERE n < 4) SELECT n FROM r ORDER BY n";
    assert_eq!(
        execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap()
            .rows,
        vec![json!({"n":1})]
    );
}
