//! Grouping and aggregate equivalence against an independent in-memory table.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use alloy_primitives::{Address, B256, Bytes, keccak256};
use datafusion::arrow::array::{Array, ArrayRef, Decimal128Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::json::writer::{JsonArray, WriterBuilder};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::MemTable;
use datafusion::functions::math::expr_fn::random;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{Expr, LogicalPlan, col, lit, lit_with_metadata};
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::optimizer::{OptimizerContext, OptimizerRule};
use datafusion::prelude::SessionContext;
use datafusion::{common::ScalarValue, common::ToDFSchema};
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::{Value, json};

fn topic(byte: u8) -> B256 {
    B256::repeat_byte(byte)
}

fn rows() -> Vec<LogRow> {
    [
        (1_u64, Source::Receipt, None),
        (2, Source::Trace, Some(topic(0xaa))),
        (3, Source::Receipt, Some(topic(0xaa))),
        (4, Source::Trace, None),
        (5, Source::Receipt, Some(topic(0xbb))),
        (6, Source::Receipt, None),
    ]
    .into_iter()
    .map(|(block_number, source, topic0)| {
        let mut data = [0_u8; 32];
        data[24..].copy_from_slice(&(block_number * 10).to_be_bytes());
        LogRow {
            block_number,
            block_hash: keccak256(block_number.to_le_bytes()),
            timestamp: block_number * 100,
            tx_hash: keccak256([block_number as u8; 32]),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(block_number as u8),
            topic0,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::copy_from_slice(&data),
            data_len: 32,
            source,
        }
    })
    .collect()
}

fn storage(populated: bool) -> (tempfile::TempDir, PartitionManager) {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    if populated {
        storage.write_batch(&rows()).unwrap();
        storage.checkpoint().unwrap();
    }
    (tmp, storage)
}

fn reference(populated: bool) -> SessionContext {
    let rows = populated.then(rows).unwrap_or_default();
    let block_number = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.block_number),
    )) as ArrayRef;
    let timestamp = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.timestamp),
    )) as ArrayRef;
    let source = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.source as u64),
    )) as ArrayRef;
    let topic0 =
        Arc::new(StringArray::from_iter(rows.iter().map(|row| {
            row.topic0.map(|topic| format!("0x{}", hex::encode(topic)))
        }))) as ArrayRef;
    let amount = Arc::new(
        Decimal128Array::from_iter_values(rows.iter().map(|row| i128::from(row.block_number * 10)))
            .with_precision_and_scale(38, 0)
            .unwrap(),
    ) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![
        Field::new("block_number", block_number.data_type().clone(), false),
        Field::new("timestamp", timestamp.data_type().clone(), false),
        Field::new("source", source.data_type().clone(), false),
        Field::new("topic0", topic0.data_type().clone(), true),
        Field::new("amount", amount.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![block_number, timestamp, source, topic0, amount],
    )
    .unwrap();
    let reference = SessionContext::new();
    reference
        .register_table(
            "logs",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    reference
}

fn simplify(expr: Expr, fields: Vec<Field>) -> Expr {
    let schema = Schema::new(fields).to_dfschema_ref().unwrap();
    let props = ExecutionProps::new();
    ExprSimplifier::new(SimplifyContext::new(&props).with_schema(schema))
        .simplify(expr)
        .unwrap()
}

async fn reference_rows(reference: &SessionContext, sql: &str) -> Vec<Value> {
    let batches = reference.sql(sql).await.unwrap().collect().await.unwrap();
    let mut encoded = Vec::new();
    let mut writer = WriterBuilder::new()
        .with_explicit_nulls(true)
        .build::<_, JsonArray>(&mut encoded);
    for batch in &batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    let mut rows: Vec<Value> = serde_json::from_slice(&encoded).unwrap();
    let mut row_offset = 0;
    for batch in &batches {
        for (column_index, field) in batch.schema().fields().iter().enumerate() {
            if !matches!(
                field.data_type(),
                DataType::Decimal32(_, _)
                    | DataType::Decimal64(_, _)
                    | DataType::Decimal128(_, _)
                    | DataType::Decimal256(_, _)
            ) {
                continue;
            }
            let array = batch.column(column_index);
            let formatter =
                ArrayFormatter::try_new(array.as_ref(), &FormatOptions::default()).unwrap();
            for row_index in 0..batch.num_rows() {
                if !array.is_null(row_index) {
                    rows[row_offset + row_index][field.name()] =
                        Value::String(formatter.value(row_index).to_string());
                }
            }
        }
        row_offset += batch.num_rows();
    }
    rows
}

async fn assert_case(
    storage: &PartitionManager,
    reference: &SessionContext,
    sql: &str,
    expected: Vec<Value>,
    expected_scanned: u64,
) {
    let actual = execute_sql(sql, storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(actual.rows, expected, "LogEx result for {sql}");
    assert_eq!(
        actual.total_scanned, expected_scanned,
        "scan count for {sql}"
    );
    assert_eq!(
        reference_rows(reference, sql).await,
        expected,
        "MemTable result for {sql}"
    );
}

#[tokio::test]
async fn group_by_all_count_matches_explicit_source_grouping() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    let expected = vec![
        json!({"source": Source::Receipt as u64, "total": 4}),
        json!({"source": Source::Trace as u64, "total": 2}),
    ];
    assert_case(
        &storage,
        &reference,
        "SELECT source, COUNT(*) AS total FROM logs GROUP BY source ORDER BY source",
        expected.clone(),
        6,
    )
    .await;
    let inferred = "SELECT source, COUNT(*) AS total FROM logs GROUP BY ALL ORDER BY source";
    assert_case(&storage, &reference, inferred, expected, 6).await;

    assert_case(
        &storage,
        &reference,
        "SELECT source AS provenance, COUNT(*) AS occurrences FROM logs \
         GROUP BY ALL ORDER BY provenance DESC",
        vec![
            json!({"provenance": Source::Trace as u64, "occurrences": 2}),
            json!({"provenance": Source::Receipt as u64, "occurrences": 4}),
        ],
        6,
    )
    .await;
}

#[tokio::test]
async fn group_by_all_count_preserves_global_and_empty_results() {
    for populated in [false, true] {
        let (_tmp, storage) = storage(populated);
        let reference = reference(populated);
        let expected_count = if populated { 6 } else { 0 };
        let expected = vec![json!({"total": expected_count})];
        assert_case(
            &storage,
            &reference,
            "SELECT COUNT(*) AS total FROM logs",
            expected.clone(),
            expected_count,
        )
        .await;
        assert_case(
            &storage,
            &reference,
            "SELECT COUNT(*) AS total FROM logs GROUP BY ALL",
            expected,
            expected_count,
        )
        .await;
    }
}

#[tokio::test]
async fn group_by_all_aggregate_expressions_match_reference() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    assert_case(
        &storage,
        &reference,
        "SELECT source, COUNT(1) + 1 AS plus_one, SUM(block_number) + 10 AS shifted \
         FROM logs GROUP BY ALL ORDER BY source",
        vec![
            json!({"source": Source::Receipt as u64, "plus_one": 5, "shifted": "25"}),
            json!({"source": Source::Trace as u64, "plus_one": 3, "shifted": "16"}),
        ],
        6,
    )
    .await;
}

#[tokio::test]
async fn group_by_all_computed_and_multiple_keys_match_reference() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    assert_case(
        &storage,
        &reference,
        "SELECT source + 10 AS bucket, SUM(block_number) AS total FROM logs \
         GROUP BY ALL ORDER BY bucket",
        vec![
            json!({"bucket": "10", "total": 15}),
            json!({"bucket": "11", "total": 6}),
        ],
        6,
    )
    .await;
    assert_case(
        &storage,
        &reference,
        "SELECT source, block_number % 2 AS parity, COUNT(1) AS total FROM logs \
         GROUP BY ALL ORDER BY source, parity",
        vec![
            json!({"source": Source::Receipt as u64, "parity": "0", "total": 1}),
            json!({"source": Source::Receipt as u64, "parity": "1", "total": 3}),
            json!({"source": Source::Trace as u64, "parity": "0", "total": 2}),
        ],
        6,
    )
    .await;
}

#[tokio::test]
async fn group_by_all_null_and_empty_groups_match_reference() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    assert_case(
        &storage,
        &reference,
        "SELECT topic0, COUNT(1) AS total FROM logs GROUP BY ALL \
         ORDER BY topic0 NULLS FIRST",
        vec![
            json!({"topic0": null, "total": 3}),
            json!({"topic0": format!("0x{}", "aa".repeat(32)), "total": 2}),
            json!({"topic0": format!("0x{}", "bb".repeat(32)), "total": 1}),
        ],
        6,
    )
    .await;
    assert_case(
        &storage,
        &reference,
        "SELECT source, COUNT(1) AS total FROM logs WHERE block_number > 6 \
         GROUP BY ALL ORDER BY source",
        vec![],
        0,
    )
    .await;
}

#[tokio::test]
async fn guarded_not_in_with_null_preserves_unknown_results() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    assert_case(
        &storage,
        &reference,
        "SELECT block_number FROM logs \
         WHERE block_number IN (1, 2, 3, 4, 5, 6) \
         AND block_number NOT IN (1, 2, NULL) \
         ORDER BY block_number + 0",
        vec![],
        0,
    )
    .await;
}

#[tokio::test]
async fn inlist_set_rewrites_preserve_projection_nulls_and_runtime_values() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    assert_case(
        &storage,
        &reference,
        "SELECT block_number, \
         block_number IN (1, 2, 3, 4, 5, 6) \
         AND block_number NOT IN (1, 2, NULL) AS kept \
         FROM logs ORDER BY block_number + 0",
        vec![
            json!({"block_number": 1, "kept": false}),
            json!({"block_number": 2, "kept": false}),
            json!({"block_number": 3, "kept": null}),
            json!({"block_number": 4, "kept": null}),
            json!({"block_number": 5, "kept": null}),
            json!({"block_number": 6, "kept": null}),
        ],
        6,
    )
    .await;
    assert_case(
        &storage,
        &reference,
        "SELECT block_number, \
         block_number IN (timestamp / 100) \
         AND block_number NOT IN (block_number + 0) AS kept \
         FROM logs ORDER BY block_number + 0",
        (1..=6)
            .map(|block_number| json!({"block_number": block_number, "kept": false}))
            .collect(),
        6,
    )
    .await;
}

#[tokio::test]
async fn disjoint_nullable_inlists_preserve_nulls_and_filter_pruning() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    let aa = format!("0x{}", "aa".repeat(32));
    let bb = format!("0x{}", "bb".repeat(32));
    let predicate = format!("topic0 IN ('{aa}') AND topic0 IN ('{bb}')");
    assert_case(
        &storage,
        &reference,
        &format!("SELECT block_number, {predicate} AS kept FROM logs ORDER BY block_number + 0"),
        vec![
            json!({"block_number": 1, "kept": null}),
            json!({"block_number": 2, "kept": false}),
            json!({"block_number": 3, "kept": false}),
            json!({"block_number": 4, "kept": null}),
            json!({"block_number": 5, "kept": false}),
            json!({"block_number": 6, "kept": null}),
        ],
        6,
    )
    .await;
    assert_case(
        &storage,
        &reference,
        &format!("SELECT block_number FROM logs WHERE {predicate}"),
        vec![],
        0,
    )
    .await;

    let present_rows = vec![
        json!({"block_number": 2}),
        json!({"block_number": 3}),
        json!({"block_number": 5}),
    ];
    let projected_presence = vec![
        json!({"block_number": 1, "kept": null}),
        json!({"block_number": 2, "kept": true}),
        json!({"block_number": 3, "kept": true}),
        json!({"block_number": 4, "kept": null}),
        json!({"block_number": 5, "kept": true}),
        json!({"block_number": 6, "kept": null}),
    ];
    assert_case(
        &storage,
        &reference,
        &format!(
            "SELECT block_number, NOT ({predicate}) AS kept \
             FROM logs ORDER BY block_number + 0"
        ),
        projected_presence.clone(),
        6,
    )
    .await;
    assert_case(
        &storage,
        &reference,
        &format!("SELECT block_number FROM logs WHERE NOT ({predicate}) ORDER BY block_number + 0"),
        present_rows.clone(),
        6,
    )
    .await;

    let negative_union = format!("topic0 NOT IN ('{aa}') OR topic0 NOT IN ('{bb}')");
    assert_case(
        &storage,
        &reference,
        &format!(
            "SELECT block_number, {negative_union} AS kept \
             FROM logs ORDER BY block_number + 0"
        ),
        projected_presence,
        6,
    )
    .await;
    assert_case(
        &storage,
        &reference,
        &format!("SELECT block_number FROM logs WHERE {negative_union} ORDER BY block_number + 0"),
        present_rows,
        6,
    )
    .await;
}

#[tokio::test]
async fn literal_set_rewrite_fast_paths_match_reference() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    for (predicate, expected) in [
        (
            "block_number IN (1, 2, 3, 4) AND block_number IN (3, 4, 5)",
            vec![3, 4],
        ),
        (
            "block_number IN (1, 2, 3, 4, 5, 6) AND block_number NOT IN (1, 2)",
            vec![3, 4, 5, 6],
        ),
        (
            "block_number IN (1, 2) OR block_number IN (2, 3)",
            vec![1, 2, 3],
        ),
        (
            "block_number NOT IN (1, 2) AND block_number NOT IN (3)",
            vec![4, 5, 6],
        ),
    ] {
        assert_case(
            &storage,
            &reference,
            &format!("SELECT block_number FROM logs WHERE {predicate} ORDER BY block_number + 0"),
            expected
                .into_iter()
                .map(|block_number| json!({"block_number": block_number}))
                .collect(),
            6,
        )
        .await;
    }
}

#[tokio::test]
async fn native_exact_sum_preserves_guarded_null_predicate() {
    let (_tmp, storage) = storage(true);
    let reference = reference(true);
    let predicate = "block_number IN (1, 2, 3, 4, 5, 6) \
                     AND block_number NOT IN (1, 2, NULL)";
    let result = execute_sql(
        &format!(
            "SELECT SUM(CASE WHEN {predicate} THEN data ELSE 0 END) AS total \
             FROM logs WHERE data_len = 32"
        ),
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    let expected = vec![json!({"total": "0"})];
    assert_eq!(result.rows, expected);
    assert_eq!(result.total_scanned, 6);
    assert_eq!(
        reference_rows(
            &reference,
            &format!("SELECT SUM(CASE WHEN {predicate} THEN amount ELSE 0 END) AS total FROM logs")
        )
        .await,
        expected
    );
}

#[test]
fn set_rewrites_require_normalized_repeatable_scalar_operands() {
    let uint64 = vec![Field::new("x", DataType::UInt64, false)];
    let null_membership = col("x")
        .in_list(vec![lit(1_u64), lit(2_u64), lit(3_u64), lit(4_u64)], false)
        .and(col("x").in_list(
            vec![
                lit(1_u64),
                lit(5_u64),
                lit(6_u64),
                Expr::Literal(ScalarValue::UInt64(None), None),
            ],
            true,
        ));
    assert_eq!(
        simplify(null_membership.clone(), uint64.clone()),
        null_membership
    );

    let mismatched_types = col("x")
        .in_list(vec![lit(1_u64), lit(2_u64), lit(3_u64), lit(4_u64)], false)
        .and(col("x").in_list(vec![lit(2_i64), lit(3_i64), lit(4_i64), lit(5_i64)], false));
    assert_eq!(
        simplify(mismatched_types.clone(), uint64.clone()),
        mismatched_types
    );

    let metadata = datafusion::common::metadata::FieldMetadata::from(BTreeMap::from([(
        "source".to_owned(),
        "test".to_owned(),
    )]));
    let annotated = col("x")
        .in_list(
            vec![
                lit_with_metadata(1_u64, Some(metadata.clone())),
                lit(2_u64),
                lit(3_u64),
                lit(4_u64),
            ],
            false,
        )
        .and(col("x").in_list(vec![lit(1_u64), lit(2_u64), lit(3_u64), lit(4_u64)], false));
    assert_eq!(simplify(annotated.clone(), uint64), annotated);

    let float_fields = vec![Field::new("x", DataType::Float64, false)];
    let volatile_tested = random()
        .in_list(
            vec![lit(0.25_f64), lit(0.5_f64), lit(0.75_f64), lit(1.0_f64)],
            true,
        )
        .and(random().in_list(
            vec![lit(0.125_f64), lit(0.25_f64), lit(0.5_f64), lit(0.75_f64)],
            true,
        ));
    assert_eq!(
        simplify(volatile_tested.clone(), float_fields.clone()),
        volatile_tested
    );
    let volatile_list = col("x")
        .in_list(
            vec![random(), lit(0.25_f64), lit(0.5_f64), lit(0.75_f64)],
            false,
        )
        .or(col("x").in_list(
            vec![random(), lit(0.5_f64), lit(0.75_f64), lit(1.0_f64)],
            false,
        ));
    assert_eq!(simplify(volatile_list.clone(), float_fields), volatile_list);
}

#[test]
fn scalar_float_set_rewrite_uses_physical_bitwise_equality() {
    let expression = col("x")
        .in_list(vec![lit(0.0_f64), lit(f64::NAN)], false)
        .and(col("x").in_list(vec![lit(-0.0_f64), lit(f64::NAN)], false));
    assert_eq!(
        simplify(expression, vec![Field::new("x", DataType::Float64, false)]),
        col("x").eq(lit(f64::NAN))
    );
}

#[test]
fn null_filter_pruning_does_not_discard_volatile_or_fallible_expressions() {
    for retained in [
        random().gt(lit(0.5_f64)),
        (lit(1_i64) / lit(0_i64)).gt(lit(0_i64)),
    ] {
        let rejects_all =
            lit(1_u64).in_list(vec![Expr::Literal(ScalarValue::UInt64(None), None)], true);
        let predicate = retained.and(rejects_all);
        let plan = LogicalPlanBuilder::empty(false)
            .filter(predicate.clone())
            .unwrap()
            .build()
            .unwrap();
        let optimized = EliminateFilter::new()
            .rewrite(plan, &OptimizerContext::new())
            .unwrap()
            .data;
        let LogicalPlan::Filter(filter) = optimized else {
            panic!("observable filter branch was discarded")
        };
        assert_eq!(filter.predicate, predicate);
    }
}

#[tokio::test]
#[ignore = "explicit release IN-list set rewrite benchmark with disposable fixture"]
async fn inlist_set_rewrite_latency() {
    const ROWS: u64 = 20_000;
    let repeats = std::env::var("LOGEX_INLIST_BENCH_REPEATS")
        .map(|value| value.parse::<usize>().unwrap())
        .unwrap_or(50);
    assert!((1..=500).contains(&repeats));
    let templates = rows();
    let benchmark_rows = (1..=ROWS)
        .map(|block_number| {
            let mut row = templates[usize::try_from((block_number - 1) % 6).unwrap()].clone();
            row.block_number = block_number;
            row.block_hash = keccak256(block_number.to_le_bytes());
            row.timestamp = block_number * 100;
            row
        })
        .collect::<Vec<_>>();
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 8_192,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    for chunk in benchmark_rows.chunks(4_096) {
        storage.write_batch(chunk).unwrap();
    }
    storage.checkpoint().unwrap();

    let aa = format!("0x{}", "aa".repeat(32));
    let bb = format!("0x{}", "bb".repeat(32));
    let long_left = (1..=1_000)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let long_right = (501..=1_500)
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let cases = [
        (
            "inlist_literal_intersection",
            "SELECT block_number FROM logs \
             WHERE block_number IN (5000, 10000, 15000, 20000) \
             AND block_number IN (10000, 15000, 19999) \
             ORDER BY block_number + 0"
                .to_owned(),
            vec![
                json!({"block_number": 10000}),
                json!({"block_number": 15000}),
            ],
            ROWS,
        ),
        (
            "inlist_long_literal_intersection",
            format!(
                "SELECT COUNT(block_number + 0) AS total FROM logs \
                 WHERE block_number IN ({long_left}) AND block_number IN ({long_right}) \
                 ORDER BY total + 0"
            ),
            vec![json!({"total": 500})],
            ROWS,
        ),
        (
            "inlist_literal_difference",
            "SELECT block_number FROM logs \
             WHERE block_number IN (5000, 10000, 15000, 20000) \
             AND block_number NOT IN (5000, 20000) \
             ORDER BY block_number + 0"
                .to_owned(),
            vec![
                json!({"block_number": 10000}),
                json!({"block_number": 15000}),
            ],
            ROWS,
        ),
        (
            "inlist_nullable_disjoint_pruned",
            format!(
                "SELECT block_number FROM logs \
                 WHERE topic0 IN ('{aa}') AND topic0 IN ('{bb}')"
            ),
            vec![],
            0,
        ),
    ];
    for (_, sql, expected, expected_scanned) in &cases {
        let result = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(&result.rows, expected);
        assert_eq!(result.total_scanned, *expected_scanned);
    }
    println!(
        "{}",
        json!({"kind":"config", "rows":ROWS, "repeats":repeats, "segment_rows":8192,
               "batch_rows":4096, "indexed":false,
               "fixture_digest":keccak256(serde_json::to_vec(&benchmark_rows).unwrap()).to_string()})
    );
    for iteration in 0..repeats {
        for offset in 0..cases.len() {
            let index = (iteration + offset) % cases.len();
            let (metric, sql, expected, expected_scanned) = &cases[index];
            let start = Instant::now();
            let result = execute_sql(sql, &storage, storage.head_block())
                .await
                .unwrap();
            let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
            assert_eq!(&result.rows, expected);
            assert_eq!(result.total_scanned, *expected_scanned);
            println!(
                "{}",
                json!({"kind":"sample", "metric":metric, "iteration":iteration,
                       "elapsed_ms":elapsed_ms})
            );
        }
    }
}
