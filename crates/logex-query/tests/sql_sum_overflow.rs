//! Checked fixed-width SUM contracts with independent finite arithmetic bounds.
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use serde_json::json;

fn fixture() -> (tempfile::TempDir, PartitionManager) {
    let tmp = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 3,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    (tmp, storage)
}

#[tokio::test]
async fn fixed_width_sum_overflow_is_an_error() {
    let (_tmp, storage) = fixture();
    let i64_values =
        "(VALUES (0, CAST('9223372036854775807' AS BIGINT)), (1, CAST('1' AS BIGINT))) t(i,x)";
    let queries = [
        format!("SELECT SUM(x) AS total FROM {i64_values}"),
        "SELECT SUM(x) AS total FROM (VALUES (CAST('-9223372036854775808' AS BIGINT)), (CAST('-1' AS BIGINT))) t(x)".into(),
        format!("SELECT i % 1 AS g, SUM(x) AS total FROM {i64_values} GROUP BY i % 1"),
        format!("SELECT SUM(DISTINCT x) AS total FROM {i64_values}"),
        format!("SELECT SUM(x) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total FROM {i64_values}"),
        format!("SELECT SUM(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM {i64_values}"),
        format!("SELECT SUM(DISTINCT x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM {i64_values}"),
        "SELECT SUM(x) AS total FROM (VALUES (CAST('18446744073709551615' AS BIGINT UNSIGNED)), (CAST('1' AS BIGINT UNSIGNED))) t(x)".into(),
        "SELECT SUM(x) AS total FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))), (CAST('1' AS DECIMAL(38,0)))) t(x)".into(),
        format!("SELECT SUM(x) AS total FROM (VALUES (CAST('{}' AS DECIMAL(76,0))), (CAST('1' AS DECIMAL(76,0)))) t(x)", "9".repeat(76)),
    ];
    let mut failures = Vec::new();
    for sql in queries {
        match execute_sql(&sql, &storage, storage.head_block()).await {
            Err(error)
                if error.to_string().contains("SUM") && error.to_string().contains("overflow") => {}
            result => failures.push(format!(
                "{sql}: expected explicit SUM overflow error, got {result:?}"
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
    // A rejected query must not prevent a subsequent ordinary query.
    let result = execute_sql(
        "SELECT SUM(x) AS total FROM (VALUES (1), (2)) t(x)",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(result.rows, vec![json!({"total":3})]);
}

#[tokio::test]
async fn sliding_distinct_ignores_nulls_and_empty_frames() {
    let (_tmp, storage) = fixture();
    let cases = [
        (
            "SELECT SUM(DISTINCT x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM (VALUES (0, CAST(NULL AS BIGINT)), (1, CAST(NULL AS BIGINT))) t(i,x)",
            vec![json!({"total":null}), json!({"total":null})],
        ),
        (
            "SELECT SUM(DISTINCT x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS total FROM (VALUES (0, CAST(5 AS BIGINT)), (1, CAST(6 AS BIGINT))) t(i,x)",
            vec![json!({"total":null}), json!({"total":5})],
        ),
        (
            "SELECT SUM(DISTINCT x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM (VALUES (0, CAST(5 AS BIGINT)), (1, CAST(NULL AS BIGINT)), (2, CAST(NULL AS BIGINT))) t(i,x)",
            vec![
                json!({"total":5}),
                json!({"total":5}),
                json!({"total":null}),
            ],
        ),
    ];
    let mut failures = Vec::new();
    for (sql, expected) in cases {
        let result = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap();
        if result.rows != expected {
            failures.push(format!(
                "{sql}: expected {expected:?}, got {:?}",
                result.rows
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn checked_window_transition_subtotals_reject_overflow() {
    let (_tmp, storage) = fixture();
    let sql = "SELECT SUM(x) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS total FROM (VALUES (0, CAST('9223372036854775807' AS BIGINT)), (1, CAST('9223372036854775807' AS BIGINT))) t(i,x)";
    // Both output frames fit, but the engine adds entering rows before retracting
    // departing rows. Checked transition state deliberately rejects their union.
    // The successful pre-fix behavior is retained in the audit baseline evidence.
    let error = execute_sql(sql, &storage, storage.head_block())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("SUM overflow"), "{error}");
}

#[test]
fn sliding_distinct_state_preserves_duplicate_multiplicity() {
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::DataType;
    use datafusion::functions_aggregate::sum::SlidingDistinctSumAccumulator;
    use datafusion::logical_expr::Accumulator;
    use std::sync::Arc;
    let mut original = SlidingDistinctSumAccumulator::try_new(&DataType::Int64).unwrap();
    original
        .update_batch(&[Arc::new(Int64Array::from(vec![5, 5]))])
        .unwrap();
    let state = original.state().unwrap()[0].to_array().unwrap();
    let mut merged = SlidingDistinctSumAccumulator::try_new(&DataType::Int64).unwrap();
    merged.merge_batch(&[state]).unwrap();
    merged
        .retract_batch(&[Arc::new(Int64Array::from(vec![5]))])
        .unwrap();
    assert_eq!(
        merged.evaluate().unwrap(),
        datafusion::common::ScalarValue::Int64(Some(5))
    );
}

#[tokio::test]
async fn checked_sum_preserves_in_range_types_nulls_and_filters() {
    let (_tmp, storage) = fixture();
    let cases = [
        (
            "SELECT SUM(x) AS total FROM (VALUES (CAST('18446744073709551615' AS BIGINT UNSIGNED)), (CAST('0' AS BIGINT UNSIGNED))) t(x)",
            vec![json!({"total":u64::MAX})],
        ),
        (
            "SELECT SUM(x) AS total FROM (VALUES (CAST('-9223372036854775808' AS BIGINT)), (CAST('0' AS BIGINT))) t(x)",
            vec![json!({"total":i64::MIN})],
        ),
        (
            "SELECT SUM(DISTINCT x) AS total FROM (VALUES (CAST('9223372036854775807' AS BIGINT)), (CAST('9223372036854775807' AS BIGINT))) t(x)",
            vec![json!({"total":i64::MAX})],
        ),
        (
            "SELECT SUM(x) FILTER (WHERE keep) AS total FROM (VALUES (CAST('9223372036854775807' AS BIGINT), TRUE), (CAST('1' AS BIGINT), FALSE), (CAST('1' AS BIGINT), NULL)) t(x,keep)",
            vec![json!({"total":i64::MAX})],
        ),
        (
            "SELECT SUM(x) AS total FROM (VALUES (CAST(NULL AS BIGINT)), (CAST(NULL AS BIGINT))) t(x)",
            vec![json!({"total":null})],
        ),
        (
            "SELECT SUM(x) AS total FROM (VALUES (CAST('999.99' AS DECIMAL(10,2))), (CAST('-999.98' AS DECIMAL(10,2)))) t(x)",
            vec![json!({"total":"0.01"})],
        ),
        (
            "SELECT SUM(x) AS total, SUM(DISTINCT x) AS distinct_total, AVG(DISTINCT x) AS mean FROM (VALUES (1.25), (2.75), (1.25)) t(x)",
            vec![json!({"total":5.25,"distinct_total":4.0,"mean":2.0})],
        ),
        (
            "SELECT SUM(DISTINCT x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS total FROM (VALUES (0, CAST(5 AS BIGINT)), (1, CAST(5 AS BIGINT)), (2, CAST(6 AS BIGINT))) t(i,x)",
            vec![json!({"total":5}), json!({"total":5}), json!({"total":11})],
        ),
    ];
    for (sql, expected) in cases {
        let result = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(result.rows, expected, "{sql}");
    }
}
