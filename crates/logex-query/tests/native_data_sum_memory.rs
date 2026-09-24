use alloy_primitives::{Address, B256, Bytes};
use logex_query::{
    NativeStorageSnapshot, SqlQueryError, SqlQueryPage, execute_sql_page_on_snapshot_with_memory,
};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, QueryMemoryBudget, QueryMemoryLimit, Source};
use num_bigint::BigUint;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const MEMORY_BYTES: usize = 2 * 1024;
const ROWS: usize = 1_024;

fn rows() -> Vec<LogRow> {
    (0..ROWS).map(row).collect()
}

fn row(index: usize) -> LogRow {
    LogRow {
        block_number: 1,
        block_hash: B256::repeat_byte(1),
        timestamp: 1,
        tx_hash: B256::repeat_byte(2),
        tx_index: 0,
        log_index: index as u32,
        address: Address::repeat_byte(3),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: Bytes::from_static(&[1]),
        data_len: 1,
        source: Source::Receipt,
    }
}

#[tokio::test]
async fn native_data_sum_rejects_working_set_that_exceeds_shared_budget() {
    let limit = QueryMemoryLimit::new(MEMORY_BYTES).unwrap();
    let expected = json!({
        "total": "1024",
        "duplicate": "1024",
        "doubled": "2048",
        "zero": "0",
    });

    // A result with the same keys and values fits comfortably. The populated
    // query below must therefore be rejected for its candidate/source/numeric
    // working set, rather than for the small retained structured result.
    let output_memory = QueryMemoryBudget::new(limit);
    let output = execute_sql_page_on_snapshot_with_memory(
        "SELECT '1024' AS total, '1024' AS duplicate, \
                '2048' AS doubled, '0' AS zero",
        NativeStorageSnapshot::default(),
        0,
        SqlQueryPage::default(),
        None,
        output_memory.clone(),
    )
    .await
    .unwrap();
    assert_eq!(output.rows.as_slice(), &[expected]);
    drop(output);
    assert_eq!(output_memory.used(), 0);

    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 2_000,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(&rows()).unwrap();
    storage.checkpoint().unwrap();

    let sql = "SELECT SUM(data) AS total, SUM(data) AS duplicate, \
                      SUM(data) + SUM(data) AS doubled, \
                      SUM(data) - SUM(data) AS zero \
               FROM logs HAVING total > 0";
    let memory = QueryMemoryBudget::new(limit);
    let error = execute_sql_page_on_snapshot_with_memory(
        sql,
        NativeStorageSnapshot::from_storage(&storage),
        storage.head_block().unwrap_or(0),
        SqlQueryPage::default(),
        None,
        memory.clone(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, SqlQueryError::Capacity(ref message) if !message.contains("structured SQL result")),
        "native SUM must reject its working set before the tiny result: {error}"
    );
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn grouped_native_data_sum_rejects_working_set_that_exceeds_shared_budget() {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 2_000,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(&rows()).unwrap();
    storage.checkpoint().unwrap();

    let sql = "SELECT address, SUM(data) AS total \
               FROM logs GROUP BY address HAVING total > 0";
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    let head = storage.head_block().unwrap_or(0);

    let generous_memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
    let result = execute_sql_page_on_snapshot_with_memory(
        sql,
        snapshot.clone(),
        head,
        SqlQueryPage::default(),
        None,
        generous_memory.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        result.rows.as_slice(),
        &[json!({
            "address": Address::repeat_byte(3).to_string().to_ascii_lowercase(),
            "total": "1024",
        })]
    );
    assert_eq!(result.total_scanned, ROWS as u64);
    drop(result);
    assert_eq!(generous_memory.used(), 0);

    // The final one-row JSON result fits in this budget. The grouped native
    // path must still reject its candidate/source/group working set before
    // structured-result materialization and release every partial owner.
    let constrained_memory = QueryMemoryBudget::new(QueryMemoryLimit::new(MEMORY_BYTES).unwrap());
    let error = execute_sql_page_on_snapshot_with_memory(
        sql,
        snapshot,
        head,
        SqlQueryPage::default(),
        None,
        constrained_memory.clone(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, SqlQueryError::Capacity(ref message) if !message.contains("structured SQL result")),
        "grouped native SUM must reject its working set before the tiny result: {error}"
    );
    assert_eq!(constrained_memory.used(), 0);
}

#[tokio::test]
async fn grouped_native_data_sum_preserves_residual_case_partition_and_cancel_semantics() {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let fixture = (0..4)
        .map(|index| {
            let mut value = row(index);
            value.address = Address::repeat_byte(if index % 2 == 0 { 3 } else { 4 });
            value.data = Bytes::from(vec![index as u8 + 1]);
            value
        })
        .collect::<Vec<_>>();
    for rows in fixture.chunks(2) {
        storage.write_batch(rows).unwrap();
    }
    storage.checkpoint().unwrap();
    let nonempty_partitions = storage
        .sealed_partitions()
        .iter()
        .chain(std::iter::once(storage.hot_partition()))
        .filter(|partition| partition.meta.row_count > 0)
        .count();
    assert!(
        nonempty_partitions >= 2,
        "fixture must exercise nonempty partition workers and merge"
    );

    let sql = "SELECT address, \
                      SUM(CASE WHEN log_index % 2 = 0 THEN data ELSE 0 END) AS total \
               FROM logs \
               WHERE data_len = 1 AND source + 0 >= 0 \
               GROUP BY address ORDER BY total DESC";
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
    let result = execute_sql_page_on_snapshot_with_memory(
        sql,
        NativeStorageSnapshot::from_storage(&storage),
        storage.head_block().unwrap_or(0),
        SqlQueryPage::default(),
        None,
        memory.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        result.rows.as_slice(),
        &[
            json!({
                "address": Address::repeat_byte(3).to_string().to_ascii_lowercase(),
                "total": "4",
            }),
            json!({
                "address": Address::repeat_byte(4).to_string().to_ascii_lowercase(),
                "total": "0",
            }),
        ]
    );
    assert_eq!(result.total_scanned, fixture.len() as u64);
    assert!(
        memory.used() > 0,
        "returned JSON rows must retain their charge"
    );
    drop(result);
    assert_eq!(memory.used(), 0);

    let observed_owned_working_set = Arc::new(AtomicBool::new(false));
    let observed = Arc::clone(&observed_owned_working_set);
    let cancel_memory = memory.clone();
    let cancel = Arc::new(move || {
        let owned = cancel_memory.used() > 0;
        observed.fetch_or(owned, Ordering::SeqCst);
        owned
    });
    let error = execute_sql_page_on_snapshot_with_memory(
        sql,
        NativeStorageSnapshot::from_storage(&storage),
        storage.head_block().unwrap_or(0),
        SqlQueryPage::default(),
        Some(cancel),
        memory.clone(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("query canceled"), "{error}");
    assert!(
        observed_owned_working_set.load(Ordering::SeqCst),
        "cancellation must observe an owned grouped SUM working allocation"
    );
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn empty_native_data_sum_accounts_projection_value_headers() {
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1).unwrap());
    let error = execute_sql_page_on_snapshot_with_memory(
        "SELECT SUM(data) AS total FROM logs",
        NativeStorageSnapshot::default(),
        0,
        SqlQueryPage::default(),
        None,
        memory.clone(),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, SqlQueryError::Capacity(ref message) if message.contains("native SUM result values")),
        "empty aggregate projection headers must be admitted: {error}"
    );
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn native_data_sum_preserves_exact_page_snapshot_and_cancellation_semantics() {
    let empty_memory = QueryMemoryBudget::new(QueryMemoryLimit::new(64 * 1024).unwrap());
    let empty = execute_sql_page_on_snapshot_with_memory(
        "SELECT SUM(data) AS total, SUM(data) + SUM(data) AS doubled FROM logs",
        NativeStorageSnapshot::default(),
        0,
        SqlQueryPage::default(),
        None,
        empty_memory.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        empty.rows.as_slice(),
        &[json!({"total": null, "doubled": null})]
    );
    drop(empty);
    assert_eq!(empty_memory.used(), 0);

    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 2_000,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(&rows()).unwrap();
    storage.checkpoint().unwrap();
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    storage.write_batch(&[row(ROWS)]).unwrap();

    let sql = "SELECT SUM(data) AS total, SUM(data) AS duplicate, \
                      SUM(data) + SUM(data) AS doubled, \
                      SUM(data) - SUM(data) AS zero \
               FROM logs HAVING total > 0";
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
    let result = execute_sql_page_on_snapshot_with_memory(
        sql,
        snapshot.clone(),
        1,
        SqlQueryPage::default(),
        None,
        memory.clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        result.rows.as_slice(),
        &[json!({
            "total": "1024",
            "duplicate": "1024",
            "doubled": "2048",
            "zero": "0",
        })]
    );
    assert_eq!(result.total_scanned, ROWS as u64);
    drop(result);

    let rejected = execute_sql_page_on_snapshot_with_memory(
        "SELECT SUM(data) AS total FROM logs HAVING total > 2000",
        snapshot.clone(),
        1,
        SqlQueryPage::default(),
        None,
        memory.clone(),
    )
    .await
    .unwrap();
    assert!(rejected.rows.is_empty());
    drop(rejected);

    let paged = execute_sql_page_on_snapshot_with_memory(
        sql,
        snapshot,
        1,
        SqlQueryPage::new(None, 1),
        None,
        memory.clone(),
    )
    .await
    .unwrap();
    assert!(paged.rows.is_empty());
    drop(paged);
    assert_eq!(memory.used(), 0);

    let canceled = execute_sql_page_on_snapshot_with_memory(
        "SELECT SUM(data) AS total FROM logs LIMIT 0",
        NativeStorageSnapshot::from_storage(&storage),
        storage.head_block().unwrap_or(0),
        SqlQueryPage::default(),
        Some(Arc::new(|| true)),
        QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(
        canceled.to_string().contains("query canceled"),
        "{canceled}"
    );
}

#[tokio::test]
async fn native_data_sum_merges_nonempty_partitions_with_limb_carry() {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let limb_bytes = std::mem::size_of::<usize>();
    let payloads = [vec![0xff; limb_bytes], vec![1], vec![2], vec![3]];
    let fixture = payloads
        .into_iter()
        .enumerate()
        .map(|(index, data)| {
            let mut row = row(index);
            row.data_len = data.len() as u32;
            row.data = Bytes::from(data);
            row
        })
        .collect::<Vec<_>>();
    for rows in fixture.chunks(2) {
        storage.write_batch(rows).unwrap();
    }
    storage.checkpoint().unwrap();
    let nonempty_partitions = storage
        .sealed_partitions()
        .iter()
        .chain(std::iter::once(storage.hot_partition()))
        .filter(|partition| partition.meta.row_count > 0)
        .count();
    assert!(
        nonempty_partitions >= 2,
        "fixture must exercise a real merge"
    );

    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
    let result = execute_sql_page_on_snapshot_with_memory(
        "SELECT SUM(data) AS total, SUM(data) + SUM(data) AS doubled FROM logs",
        NativeStorageSnapshot::from_storage(&storage),
        storage.head_block().unwrap_or(0),
        SqlQueryPage::default(),
        None,
        memory.clone(),
    )
    .await
    .unwrap();
    let total = (BigUint::from(1u8) << (limb_bytes * 8)) + BigUint::from(5u8);
    assert_eq!(
        result.rows.as_slice(),
        &[json!({"total": total.to_string(), "doubled": (&total * 2u8).to_string()})]
    );
    assert_eq!(result.total_scanned, fixture.len() as u64);
    drop(result);
    assert_eq!(memory.used(), 0);
}
