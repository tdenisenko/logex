//! SQL relation scope compared with an independent DataFusion in-memory table.

use std::sync::Arc;

use alloy_primitives::{Address, Bytes, keccak256};
use datafusion::arrow::array::{Array, Int64Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::{Map, Value, json};

fn rows() -> Vec<LogRow> {
    (1..=3_u64)
        .map(|block_number| {
            let topics = [
                Some(keccak256([block_number as u8; 32])),
                (block_number > 1).then(|| keccak256([block_number as u8 + 10; 32])),
                None,
                (block_number == 3).then(|| keccak256([33; 32])),
            ];
            let data = vec![block_number as u8; block_number as usize];
            LogRow {
                block_number,
                block_hash: keccak256(block_number.to_le_bytes()),
                timestamp: block_number * 100,
                tx_hash: keccak256([block_number as u8 + 20; 32]),
                tx_index: block_number as u32 - 1,
                log_index: block_number as u32 + 2,
                address: Address::repeat_byte(0xa0 + block_number as u8),
                topic0: topics[0],
                topic1: topics[1],
                topic2: topics[2],
                topic3: topics[3],
                data: Bytes::from(data),
                data_len: block_number as u32,
                source: if block_number == 2 {
                    Source::Trace
                } else {
                    Source::Receipt
                },
            }
        })
        .collect()
}

fn fixture(rows: &[LogRow]) -> (tempfile::TempDir, PartitionManager) {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(rows).unwrap();
    storage.checkpoint().unwrap();
    (tmp, storage)
}

fn reference(rows: &[LogRow]) -> SessionContext {
    let block_numbers = Arc::new(UInt64Array::from(
        rows.iter().map(|row| row.block_number).collect::<Vec<_>>(),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "block_number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![block_numbers]).unwrap();
    let reference = SessionContext::new();
    reference
        .register_table(
            "logs",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    reference
}

async fn reference_rows(reference: &SessionContext, sql: &str) -> Vec<Value> {
    let batches = reference.sql(sql).await.unwrap().collect().await.unwrap();
    let mut rows = Vec::new();
    for batch in batches {
        for row_index in 0..batch.num_rows() {
            let mut row = Map::new();
            for (column_index, field) in batch.schema().fields().iter().enumerate() {
                let array = batch.column(column_index);
                let value = if array.is_null(row_index) {
                    Value::Null
                } else {
                    match field.data_type() {
                        DataType::UInt64 => array
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .map(|values| json!(values.value(row_index)))
                            .unwrap(),
                        DataType::Int64 => array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .map(|values| json!(values.value(row_index)))
                            .unwrap(),
                        data_type => panic!("unsupported oracle result type {data_type:?}"),
                    }
                };
                row.insert(field.name().clone(), value);
            }
            rows.push(Value::Object(row));
        }
    }
    rows
}

async fn reference_error(reference: &SessionContext, sql: &str) -> String {
    match reference.sql(sql).await {
        Err(error) => error.to_string(),
        Ok(dataframe) => dataframe
            .collect()
            .await
            .expect_err("reference query unexpectedly succeeded")
            .to_string(),
    }
}

#[tokio::test]
async fn cte_scope_matches_datafusion_rows() {
    let rows = rows();
    let (_tmp, storage) = fixture(&rows);
    let reference = reference(&rows);
    let queries = [
        "WITH source AS (SELECT block_number FROM logs) \
         SELECT block_number FROM source ORDER BY block_number",
        "WITH MiXeD AS (SELECT block_number FROM LoGs WHERE block_number > 1) \
         SELECT block_number FROM mixed ORDER BY block_number DESC",
        "WITH \"MiXeD\" AS (SELECT block_number FROM logs WHERE block_number < 3) \
         SELECT block_number FROM \"MiXeD\" ORDER BY block_number",
        "WITH \"source.logs\" AS (SELECT block_number FROM logs WHERE block_number = 2) \
         SELECT block_number FROM \"source.logs\"",
        "WITH first AS (SELECT block_number FROM logs), \
              second AS (SELECT block_number FROM first WHERE block_number >= 2) \
         SELECT block_number FROM second ORDER BY block_number",
        "WITH outer_source AS (SELECT block_number FROM logs), \
              final_source AS (WITH inner_source AS (SELECT block_number FROM outer_source) \
                               SELECT block_number FROM inner_source WHERE block_number <> 2) \
         SELECT block_number FROM final_source ORDER BY block_number",
        "WITH logs AS (SELECT block_number FROM logs WHERE block_number = 2) \
         SELECT block_number FROM logs",
        "WITH left_source AS (SELECT block_number FROM logs WHERE block_number <= 2), \
              right_source AS (SELECT block_number FROM logs WHERE block_number >= 2) \
         SELECT left_source.block_number FROM left_source \
         JOIN right_source ON left_source.block_number = right_source.block_number",
        "WITH source AS (SELECT block_number FROM logs) \
         SELECT block_number FROM source WHERE block_number = 1 \
         UNION ALL SELECT block_number FROM source WHERE block_number = 3 \
         ORDER BY block_number",
        "WITH RECURSIVE numbers(n) AS (SELECT 1 AS n UNION ALL \
             SELECT n + 1 FROM numbers WHERE n < 3) \
         SELECT n AS block_number FROM numbers ORDER BY n",
    ];

    for sql in queries {
        let expected = reference_rows(&reference, sql).await;
        let actual = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap_or_else(|error| panic!("LogEx rejected {sql:?}: {error}"));
        assert_eq!(actual.rows, expected, "{sql}");
    }
}

#[tokio::test]
async fn invalid_cte_references_match_datafusion_rejection() {
    let rows = rows();
    let (_tmp, storage) = fixture(&rows);
    let reference = reference(&rows);
    let cases = [
        (
            "WITH \"MiXeD\" AS (SELECT block_number FROM logs) SELECT block_number FROM mixed",
            "mixed",
            true,
        ),
        (
            "WITH source AS (SELECT block_number FROM private_logs) SELECT block_number FROM source",
            "private_logs",
            false,
        ),
        (
            "WITH first AS (SELECT block_number FROM second), \
                  second AS (SELECT block_number FROM logs) \
             SELECT block_number FROM first",
            "second",
            false,
        ),
        (
            "SELECT nested.block_number FROM \
                 (WITH private AS (SELECT block_number FROM logs) \
                  SELECT block_number FROM private) AS nested \
             JOIN private ON nested.block_number = private.block_number",
            "private",
            true,
        ),
        (
            "WITH source AS (SELECT block_number FROM logs) SELECT block_number FROM public.source",
            "public.source",
            true,
        ),
    ];

    for (sql, missing_table, guarded) in cases {
        let oracle_error = reference_error(&reference, sql).await;
        assert!(
            oracle_error.contains(missing_table) && oracle_error.contains("not found"),
            "unexpected DataFusion error for {sql:?}: {oracle_error}"
        );
        let logex_error = execute_sql(sql, &storage, storage.head_block())
            .await
            .expect_err("out-of-scope relation must be rejected")
            .to_string();
        if guarded {
            assert!(
                logex_error.contains(&format!("unsupported table '{missing_table}'")),
                "unexpected LogEx error for {sql:?}: {logex_error}"
            );
        } else {
            assert!(
                logex_error.contains(missing_table) && logex_error.contains("not found"),
                "unexpected LogEx error for {sql:?}: {logex_error}"
            );
        }
    }

    let qualified_alias = "WITH \"source.logs\" AS (SELECT block_number FROM logs) \
                           SELECT block_number FROM source.logs";
    assert_eq!(
        reference_rows(&reference, qualified_alias).await,
        vec![
            json!({"block_number": 1}),
            json!({"block_number": 2}),
            json!({"block_number": 3}),
        ],
        "DataFusion stringifies this qualified relation like the quoted alias"
    );
    let logex_error = execute_sql(qualified_alias, &storage, storage.head_block())
        .await
        .expect_err("a qualified relation must not bind to a one-component CTE alias")
        .to_string();
    assert!(
        logex_error.contains("unsupported table 'source.logs'"),
        "unexpected qualified-relation error: {logex_error}"
    );

    let nested_shadow = "WITH source AS (SELECT block_number FROM logs) \
                         SELECT nested.block_number FROM \
                             (WITH source AS (SELECT block_number FROM logs) \
                              SELECT block_number FROM source) AS nested";
    let oracle_error = reference_error(&reference, nested_shadow).await;
    assert!(
        oracle_error.contains("WITH query name \"source\" specified more than once"),
        "unexpected DataFusion shadowing error: {oracle_error}"
    );
    let logex_error = execute_sql(nested_shadow, &storage, storage.head_block())
        .await
        .expect_err("nested CTE shadowing must follow DataFusion")
        .to_string();
    assert!(
        logex_error.contains("WITH query name \"source\" specified more than once"),
        "unexpected LogEx shadowing error: {logex_error}"
    );
}

#[tokio::test]
async fn supported_tables_and_read_only_rules_remain_bounded() {
    let rows = rows();
    let (_tmp, storage) = fixture(&rows);

    let logs = execute_sql(
        "SELECT block_number FROM logs ORDER BY block_number",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(
        logs.rows,
        vec![
            json!({"block_number": 1}),
            json!({"block_number": 2}),
            json!({"block_number": 3}),
        ]
    );

    let metadata = execute_sql(
        "SELECT table_name FROM information_schema.tables",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(metadata.rows, vec![json!({"table_name": "logs"})]);

    let read_only_error = execute_sql(
        "WITH copied AS (SELECT block_number INTO scratch FROM logs) \
         SELECT block_number FROM copied",
        &storage,
        storage.head_block(),
    )
    .await
    .expect_err("SELECT INTO must remain rejected")
    .to_string();
    assert!(read_only_error.contains("SELECT INTO is not allowed"));

    let unsupported_error =
        execute_sql("SELECT * FROM private_logs", &storage, storage.head_block())
            .await
            .expect_err("unregistered tables must remain rejected")
            .to_string();
    assert!(unsupported_error.contains("unsupported table 'private_logs'"));
}
