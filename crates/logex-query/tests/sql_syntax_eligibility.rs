//! Parsed SQL modifiers must not be silently dropped by LogEx execution paths.

use std::sync::Arc;

use alloy_primitives::{Address, Bytes, keccak256};
use datafusion::arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::json;

fn fixture() -> (tempfile::TempDir, PartitionManager) {
    let mut one = vec![0; 32];
    one[31] = 1;
    let mut two = vec![0; 32];
    two[31] = 2;
    let rows = [
        LogRow {
            block_number: 1,
            block_hash: keccak256([1]),
            timestamp: 100,
            tx_hash: keccak256([11]),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(0x11),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::from(one),
            data_len: 32,
            source: Source::Receipt,
        },
        LogRow {
            block_number: 2,
            block_hash: keccak256([2]),
            timestamp: 200,
            tx_hash: keccak256([22]),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(0x22),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::from(two),
            data_len: 32,
            source: Source::Trace,
        },
    ];
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(&rows).unwrap();
    storage.checkpoint().unwrap();
    (tmp, storage)
}

fn reference() -> SessionContext {
    let numeric = |values: Vec<u64>| Arc::new(UInt64Array::from(values)) as ArrayRef;
    let text = |values: Vec<&str>| Arc::new(StringArray::from(values)) as ArrayRef;
    let register = |ctx: &SessionContext, name: &str, fields, columns| {
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    };

    let ctx = SessionContext::new();
    register(
        &ctx,
        "logs",
        vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("tx_index", DataType::UInt64, false),
            Field::new("log_index", DataType::UInt64, false),
            Field::new("source", DataType::UInt64, false),
            Field::new("address", DataType::Utf8, false),
            Field::new("amount", DataType::UInt64, false),
        ],
        vec![
            numeric(vec![1, 2]),
            numeric(vec![0, 0]),
            numeric(vec![0, 0]),
            numeric(vec![0, 1]),
            text(vec![
                "0x1111111111111111111111111111111111111111",
                "0x2222222222222222222222222222222222222222",
            ]),
            numeric(vec![1, 2]),
        ],
    );
    register(
        &ctx,
        "metadata_columns",
        vec![Field::new("ordinal_position", DataType::UInt64, false)],
        vec![numeric(vec![1, 2])],
    );
    register(
        &ctx,
        "metadata_tables",
        vec![
            Field::new("table_catalog", DataType::Utf8, false),
            Field::new("table_schema", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ],
        vec![
            text(vec!["logex"]),
            text(vec!["public"]),
            text(vec!["logs"]),
            text(vec!["BASE TABLE"]),
        ],
    );
    ctx
}

#[tokio::test]
async fn order_by_interpolate_is_never_silently_ignored() {
    let (_tmp, storage) = fixture();
    let reference = reference();
    let cases = [
        (
            "native select",
            "SELECT block_number FROM logs ORDER BY block_number, tx_index, log_index",
            "SELECT block_number FROM logs ORDER BY block_number, tx_index, log_index INTERPOLATE",
        ),
        (
            "native count",
            "SELECT source, COUNT(*) AS total FROM logs GROUP BY source ORDER BY source",
            "SELECT source, COUNT(*) AS total FROM logs GROUP BY source ORDER BY source INTERPOLATE",
        ),
        (
            "native exact sum",
            "SELECT address, SUM(amount) AS total FROM logs GROUP BY address ORDER BY total",
            "SELECT address, SUM(CAST(data AS NUMERIC)) AS total \
             FROM logs GROUP BY address ORDER BY total INTERPOLATE",
        ),
        (
            "fixed metadata",
            "SELECT ordinal_position FROM metadata_columns ORDER BY ordinal_position",
            "SELECT ordinal_position FROM information_schema.columns ORDER BY ordinal_position INTERPOLATE",
        ),
    ];

    for (case, reference_sql, logex_sql) in cases {
        let baseline = reference
            .sql(reference_sql)
            .await
            .unwrap_or_else(|error| panic!("reference failed to plan {case}: {error}"));
        let batches = baseline
            .collect()
            .await
            .unwrap_or_else(|error| panic!("reference failed to collect {case}: {error}"));
        assert!(
            batches.iter().any(|batch| batch.num_rows() > 0),
            "reference baseline for {case} must exercise collection"
        );

        let reference_interpolate = format!("{reference_sql} INTERPOLATE");
        let reference_error = reference
            .sql(&reference_interpolate)
            .await
            .expect_err("DataFusion must reject ORDER BY INTERPOLATE during planning")
            .to_string();
        assert!(
            reference_error.contains("ORDER BY INTERPOLATE is not supported"),
            "unexpected reference error for {case}: {reference_error}"
        );

        let logex_error = execute_sql(logex_sql, &storage, storage.head_block())
            .await
            .expect_err("LogEx must not silently ignore ORDER BY INTERPOLATE")
            .to_string();
        assert!(
            logex_error.contains("INTERPOLATE"),
            "unexpected LogEx error for {case}: {logex_error}"
        );
    }
}

#[tokio::test]
async fn metadata_table_alias_columns_are_not_silently_ignored() {
    let (_tmp, storage) = fixture();
    let reference = reference();

    let plain_alias = reference
        .sql("SELECT table_name FROM metadata_tables AS tables_alias")
        .await
        .expect("reference must plan a plain table alias");
    let batches = plain_alias
        .collect()
        .await
        .expect("reference must collect a plain table alias");
    let reference_table_name = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(reference_table_name.value(0), "logs");
    let logex_plain_alias = execute_sql(
        "SELECT table_name FROM information_schema.tables AS tables_alias",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("LogEx must preserve plain metadata table aliases");
    assert_eq!(logex_plain_alias.rows, vec![json!({"table_name": "logs"})]);

    let renamed = reference
        .sql(
            "SELECT table_name FROM metadata_tables AS tables_alias(\
             table_type, table_name, table_schema, table_catalog)",
        )
        .await
        .expect("the full-width table alias column list must plan");
    let renamed = renamed
        .collect()
        .await
        .expect("the full-width table alias column list must collect");
    let renamed_value = renamed[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(
        renamed_value.value(0),
        "public",
        "table alias columns must rename fields by position"
    );

    let logex_error = execute_sql(
        "SELECT table_name FROM information_schema.tables AS tables_alias(\
         table_type, table_name, table_schema, table_catalog)",
        &storage,
        storage.head_block(),
    )
    .await
    .expect_err("LogEx must not silently ignore metadata table alias columns")
    .to_string();
    assert!(
        logex_error.contains("column aliases"),
        "unexpected LogEx error: {logex_error}"
    );
}

#[tokio::test]
async fn equivalent_syntax_markers_remain_supported() {
    let (_tmp, storage) = fixture();
    let reference = reference();

    let explicit_from_first = reference
        .sql("FROM logs SELECT block_number")
        .await
        .expect("reference must plan an explicit FROM-first projection")
        .collect()
        .await
        .expect("reference must collect an explicit FROM-first projection");
    let values = explicit_from_first[0]
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    assert_eq!(
        (0..values.len())
            .map(|i| values.value(i))
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let logex_from_first = execute_sql(
        "FROM logs SELECT block_number",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("LogEx must preserve an explicit FROM-first projection");
    assert_eq!(
        logex_from_first.rows,
        vec![json!({"block_number": 1}), json!({"block_number": 2})]
    );

    let empty_projection = reference
        .sql("FROM logs")
        .await
        .expect("reference must plan a FROM-first empty projection")
        .collect()
        .await
        .expect("reference must collect a FROM-first empty projection");
    assert_eq!(
        empty_projection
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>(),
        2
    );
    assert!(
        empty_projection
            .iter()
            .all(|batch| batch.num_columns() == 0)
    );
    let logex_empty_projection = execute_sql("FROM logs", &storage, storage.head_block())
        .await
        .expect("LogEx must preserve a FROM-first empty projection");
    assert_eq!(logex_empty_projection.rows, vec![json!({}), json!({})]);

    let wildcard_exclude = execute_sql(
        "SELECT * EXCLUDE (data) FROM logs ORDER BY block_number",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("wildcard EXCLUDE options must remain planner-supported");
    assert_eq!(wildcard_exclude.rows.len(), 2);
    assert!(
        wildcard_exclude
            .rows
            .iter()
            .all(|row| { row.get("block_number").is_some() && row.get("data").is_none() })
    );

    let generated = execute_sql(
        "WITH generated AS (SELECT value FROM generate_series(1, 2)) \
         SELECT value AS block_number FROM generated ORDER BY value",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("a bounded default table function must remain planner-supported");
    assert_eq!(
        generated.rows,
        vec![json!({"block_number": 1}), json!({"block_number": 2})]
    );

    reference
        .sql("SELECT {fn COUNT(*)} AS total, {fn SUM(amount)} AS amount FROM logs")
        .await
        .expect("reference must plan ODBC function escapes")
        .collect()
        .await
        .expect("reference must collect ODBC function escapes");
    let count = execute_sql(
        "SELECT {fn COUNT(*)} AS total FROM logs",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("LogEx must preserve an ODBC COUNT escape");
    assert_eq!(count.rows, vec![json!({"total": 2})]);
    let sum = execute_sql(
        "SELECT {fn SUM(data)} AS total FROM logs",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("LogEx must preserve an ODBC exact SUM escape");
    assert_eq!(sum.rows, vec![json!({"total": "3"})]);
}

#[tokio::test]
async fn semantic_query_and_select_clauses_are_rejected_recursively() {
    let (_tmp, storage) = fixture();
    let cases = [
        (
            "FETCH",
            "SELECT block_number FROM logs ORDER BY block_number FETCH FIRST 1 ROW ONLY",
            Some(1),
        ),
        (
            "FETCH",
            "SELECT block_number FROM (SELECT block_number FROM logs FETCH FIRST 1 ROW ONLY) \
             AS nested ORDER BY block_number",
            Some(1),
        ),
        (
            "PREWHERE",
            "SELECT block_number FROM logs PREWHERE block_number = 2 ORDER BY block_number",
            Some(1),
        ),
        (
            "FETCH",
            "WITH limited AS (\
                 SELECT block_number FROM logs ORDER BY block_number FETCH FIRST 1 ROW ONLY\
             ) SELECT block_number FROM limited",
            Some(1),
        ),
        (
            "PREWHERE",
            "SELECT block_number FROM logs WHERE block_number = 1 \
             UNION ALL \
             SELECT block_number FROM logs PREWHERE block_number = 2",
            Some(2),
        ),
        (
            "PREWHERE",
            "SELECT block_number FROM (\
                 SELECT block_number FROM logs PREWHERE block_number = 2\
             ) AS nested ORDER BY block_number",
            Some(1),
        ),
        (
            "FOR UPDATE",
            "SELECT block_number FROM logs FOR UPDATE",
            None,
        ),
        (
            "SETTINGS",
            "SELECT block_number FROM logs SETTINGS max_threads = 1",
            None,
        ),
        ("FORMAT", "SELECT block_number FROM logs FORMAT JSON", None),
        (
            "FOR JSON",
            "SELECT block_number FROM logs FOR JSON AUTO",
            None,
        ),
        (
            "CONNECT BY",
            "SELECT block_number FROM logs START WITH block_number = 1 \
             CONNECT BY block_number = PRIOR block_number + 1",
            None,
        ),
    ];
    let mut accepted = Vec::new();

    for (clause, sql, semantic_row_count) in cases {
        match execute_sql(sql, &storage, storage.head_block()).await {
            Ok(result) => accepted.push(format!(
                "{clause} returned {} rows{}: {sql}",
                result.rows.len(),
                semantic_row_count
                    .map(|count| format!(", but applying the clause requires {count}"))
                    .unwrap_or_default()
            )),
            Err(error) => assert!(
                error.to_string().to_ascii_uppercase().contains(clause),
                "unexpected rejection for {clause}: {error}"
            ),
        }
    }

    assert!(
        accepted.is_empty(),
        "semantic clauses were silently ignored:\n{}",
        accepted.join("\n")
    );
}

#[tokio::test]
async fn semantic_table_group_and_function_modifiers_are_not_ignored() {
    let (_tmp, storage) = fixture();
    let cases = [
        (
            "TABLESAMPLE",
            "SELECT block_number FROM logs TABLESAMPLE SYSTEM (0)",
            Some(0),
        ),
        (
            "TABLESAMPLE",
            "SELECT block_number FROM (\
                 SELECT block_number FROM logs TABLESAMPLE SYSTEM (0)\
             ) AS sampled",
            Some(0),
        ),
        (
            "PARTITION",
            "SELECT block_number FROM logs PARTITION (recent)",
            None,
        ),
        (
            "WITH ORDINALITY",
            "SELECT block_number FROM logs WITH ORDINALITY",
            None,
        ),
        (
            "TABLE HINTS",
            "SELECT block_number FROM logs WITH (NOLOCK)",
            None,
        ),
        (
            "TABLE FUNCTION SETTINGS",
            "WITH generated AS (\
                 SELECT value FROM generate_series(1, 2, SETTINGS ignored = 1)\
             ) SELECT value AS block_number FROM generated ORDER BY value",
            None,
        ),
        (
            "TABLE FUNCTION SETTINGS",
            "SELECT value AS block_number FROM (\
                 SELECT value FROM generate_series(1, 2, SETTINGS ignored = 1)\
             ) AS generated ORDER BY value",
            None,
        ),
        (
            "PARAMETRIC FUNCTION",
            "SELECT COUNT(1)(*) AS total FROM logs",
            None,
        ),
        (
            "GROUP BY WITH MODIFIERS",
            "SELECT source, COUNT(*) AS total FROM logs \
             GROUP BY source WITH ROLLUP ORDER BY source",
            Some(3),
        ),
    ];
    let mut accepted = Vec::new();

    for (clause, sql, semantic_row_count) in cases {
        match execute_sql(sql, &storage, storage.head_block()).await {
            Ok(result) => accepted.push(format!(
                "{clause} returned {} rows{}: {sql}",
                result.rows.len(),
                semantic_row_count
                    .map(|count| format!(", but applying the clause requires {count}"))
                    .unwrap_or_default()
            )),
            Err(error) => assert!(
                error.to_string().to_ascii_uppercase().contains(clause),
                "unexpected rejection for {clause}: {error}"
            ),
        }
    }

    assert!(
        accepted.is_empty(),
        "semantic modifiers were silently ignored:\n{}",
        accepted.join("\n")
    );
}

#[tokio::test]
async fn planner_supported_grouping_forms_remain_available() {
    let (_tmp, storage) = fixture();

    let rollup = execute_sql(
        "SELECT source, COUNT(*) AS total FROM logs \
         GROUP BY ROLLUP(source) ORDER BY source",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("ordinary ROLLUP expressions must remain planner-supported");
    assert_eq!(
        rollup.rows,
        vec![
            json!({"source": Source::Receipt as u8 as u64, "total": 1}),
            json!({"source": Source::Trace as u8 as u64, "total": 1}),
            json!({"source": null, "total": 2}),
        ]
    );

    let group_by_all = execute_sql(
        "SELECT source FROM logs GROUP BY ALL ORDER BY source",
        &storage,
        storage.head_block(),
    )
    .await
    .expect("GROUP BY ALL without ignored modifiers must remain planner-supported");
    assert_eq!(
        group_by_all.rows,
        vec![
            json!({"source": Source::Receipt as u8 as u64}),
            json!({"source": Source::Trace as u8 as u64}),
        ]
    );
}
