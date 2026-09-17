//! Decimal AVG controls with independently known finite averages.
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
async fn decimal_average_high_scale_does_not_wrap() {
    let (_tmp, storage) = fixture();
    let values = "(VALUES (0, CAST('0.9' AS DECIMAL(38,38))), (1, CAST('0.9' AS DECIMAL(38,38))), (2, CAST('0.9' AS DECIMAL(38,38)))) t(i,x)";
    // Exact means are 0.9 (ordinary) and 0.8 (DISTINCT). The fixed-width
    // running sums cannot represent their subtotals; reject rather than wrap.
    let cases = [
        format!("SELECT AVG(x) AS a FROM {values}"),
        format!("SELECT AVG(x) AS a FROM {values} GROUP BY i % 1"),
        format!("SELECT AVG(x) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS a FROM {values} ORDER BY i"),
        "SELECT AVG(DISTINCT x) AS a FROM (VALUES (CAST('0.7' AS DECIMAL(38,38))), (CAST('0.8' AS DECIMAL(38,38))), (CAST('0.9' AS DECIMAL(38,38)))) t(x)".into(),
    ];
    let mut failures = Vec::new();
    for sql in cases {
        match execute_sql(&sql, &storage, storage.head_block()).await {
            Err(error)
                if error.to_string().contains("AVG") && error.to_string().contains("overflow") => {}
            actual => failures.push(format!(
                "{sql}: expected checked AVG overflow, got {actual:?}"
            )),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn decimal_average_small_values_and_nulls() {
    let (_tmp, storage) = fixture();
    for (sql, expected) in [
        (
            "SELECT AVG(x) AS a, AVG(DISTINCT x) AS d FROM (VALUES (CAST('1.20' AS DECIMAL(10,2))), (CAST('2.40' AS DECIMAL(10,2))), (NULL)) t(x)",
            vec![json!({"a":"1.800000","d":"1.800000"})],
        ),
        (
            "SELECT AVG(x) AS a, AVG(DISTINCT x) AS d FROM (VALUES (CAST(NULL AS DECIMAL(10,2)))) t(x)",
            vec![json!({"a":null,"d":null})],
        ),
        (
            "SELECT AVG(x) AS a, AVG(DISTINCT x) AS d FROM (VALUES (CAST('1.20' AS DECIMAL(10,2)))) t(x) WHERE FALSE",
            vec![json!({"a":null,"d":null})],
        ),
    ] {
        let result = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(result.rows, expected, "{sql}");
    }
}

#[tokio::test]
async fn decimal_average_sliding_last_value_exits() {
    let (_tmp, storage) = fixture();
    let sql = "SELECT AVG(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM (VALUES (0, CAST('1.20' AS DECIMAL(10,2))), (1, NULL), (2, NULL)) t(i,x) ORDER BY i";
    let result = execute_sql(sql, &storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"a":"1.200000"}),
            json!({"a":"1.200000"}),
            json!({"a":null})
        ]
    );
}

#[tokio::test]
async fn decimal_average_negative_scale() {
    let (_tmp, storage) = fixture();
    let table =
        "(VALUES (0, CAST(100 AS DECIMAL(10,-2))), (1, CAST(200 AS DECIMAL(10,-2)))) t(i,x)";
    for sql in [
        format!("SELECT AVG(x) AS a, AVG(DISTINCT x) AS d FROM {table}"),
        format!("SELECT AVG(x) AS a, AVG(DISTINCT x) AS d FROM {table} GROUP BY i % 1"),
    ] {
        let result = execute_sql(&sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(result.rows, vec![json!({"a":"150.00", "d":"150.00"})]);
    }
}

#[tokio::test]
async fn decimal_average_subtotal_exceeds_input_precision() {
    let (_tmp, storage) = fixture();
    for sql in [
        "SELECT AVG(x) AS a FROM (VALUES (0, CAST(90 AS DECIMAL(2,0))), (1, CAST(90 AS DECIMAL(2,0)))) t(i,x)",
        "SELECT AVG(x) AS a FROM (VALUES (0, CAST(90 AS DECIMAL(2,0))), (1, CAST(90 AS DECIMAL(2,0)))) t(i,x) GROUP BY i % 1",
    ] {
        let result = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(result.rows, vec![json!({"a":"90.0000"})]);
    }
}

#[tokio::test]
async fn checked_decimal_window_transition_is_explicit() {
    let (_tmp, storage) = fixture();
    let sql="SELECT AVG(x) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND CURRENT ROW) AS a FROM (VALUES (0, CAST('0.9' AS DECIMAL(38,38))), (1, CAST('0.9' AS DECIMAL(38,38)))) t(i,x) ORDER BY i";
    let error = execute_sql(sql, &storage, storage.head_block())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("AVG sum overflow"));
}

#[tokio::test]
async fn decimal_average_group_filter_and_sliding_controls() {
    let (_tmp, storage) = fixture();
    let table = "(VALUES (0, CAST('-1.20' AS DECIMAL(10,2))), (1, CAST('2.40' AS DECIMAL(10,2))), (2, CAST(NULL AS DECIMAL(10,2))), (3, CAST('2.40' AS DECIMAL(10,2)))) t(i,x)";
    let sql = format!("SELECT i % 2 AS g, AVG(x) FILTER (WHERE i < 3) AS a, AVG(DISTINCT x) AS d FROM {table} GROUP BY i % 2 ORDER BY g");
    let result = execute_sql(&sql, &storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"g":0,"a":"-1.200000","d":"-1.200000"}),
            json!({"g":1,"a":"2.400000","d":"2.400000"})
        ]
    );
    let sql = format!("SELECT AVG(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM {table} ORDER BY i");
    let result = execute_sql(&sql, &storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"a":"-1.200000"}),
            json!({"a":"0.600000"}),
            json!({"a":"2.400000"}),
            json!({"a":"2.400000"})
        ]
    );
    let sql = format!("SELECT AVG(DISTINCT x) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS a FROM {table} ORDER BY i");
    let result = execute_sql(&sql, &storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"a":"-1.200000"}),
            json!({"a":"0.600000"}),
            json!({"a":"0.600000"}),
            json!({"a":"0.600000"})
        ]
    );
    let sql = format!("SELECT AVG(DISTINCT x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM {table}");
    let error = execute_sql(&sql, &storage, storage.head_block())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("retract_batch"));
    // Ordinary numeric AVG remains a JSON number, and a prior error releases query ownership.
    let result = execute_sql(
        "SELECT AVG(x) AS a FROM (VALUES (1.0), (2.0)) t(x)",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(result.rows, vec![json!({"a":1.5})]);
}

#[tokio::test]
async fn float_average_empty_window_is_sql_null() {
    let (_tmp, storage) = fixture();
    let sql="SELECT a IS NULL AS absent FROM (SELECT AVG(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM (VALUES (0, CAST(5 AS DOUBLE)), (1, NULL), (2, NULL), (3, CAST(7 AS DOUBLE))) t(i,x))";
    let result = execute_sql(sql, &storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"absent":false}),
            json!({"absent":false}),
            json!({"absent":true}),
            json!({"absent":false})
        ]
    );
}
#[tokio::test]
async fn duration_average_empty_window_is_sql_null() {
    let (_tmp, storage) = fixture();
    let sql="SELECT a IS NULL AS absent FROM (SELECT AVG(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a FROM (VALUES (0, arrow_cast(5, 'Duration(Second)')), (1, NULL), (2, NULL)) t(i,x))";
    let result = execute_sql(sql, &storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(
        result.rows,
        vec![
            json!({"absent":false}),
            json!({"absent":false}),
            json!({"absent":true})
        ]
    );
}

#[tokio::test]
async fn duration_average_large_subtotals() {
    let (_tmp, storage) = fixture();
    let table = "(VALUES (0, arrow_cast(CAST('9223372036854775807' AS BIGINT), 'Duration(Second)')), (1, arrow_cast(CAST('9223372036854775807' AS BIGINT), 'Duration(Second)'))) t(i,x)";
    // The mathematical mean fits i64; its running subtotal does not.
    for sql in [
        format!("SELECT CAST(AVG(x) AS BIGINT) AS a FROM {table}"),
        format!("SELECT CAST(AVG(x) AS BIGINT) AS a FROM {table} GROUP BY i % 1"),
        format!("SELECT CAST(AVG(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS BIGINT) AS a FROM {table} ORDER BY i"),
    ] {
        let error = execute_sql(&sql, &storage, storage.head_block())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("AVG sum overflow"), "{sql}: {error}");
    }
}

#[tokio::test]
async fn duration_average_small_values_preserve_units() {
    let (_tmp, storage) = fixture();
    for unit in ["Second", "Millisecond", "Microsecond", "Nanosecond"] {
        let table = format!("(VALUES (0, arrow_cast(-4, 'Duration({unit})')), (1, arrow_cast(8, 'Duration({unit})')), (2, NULL)) t(i,x)");
        for sql in [
            format!("SELECT CAST(AVG(x) AS BIGINT) AS a FROM {table}"),
            format!("SELECT CAST(AVG(x) AS BIGINT) AS a FROM {table} GROUP BY i % 1"),
        ] {
            let result = execute_sql(&sql, &storage, storage.head_block())
                .await
                .unwrap();
            assert_eq!(result.rows, vec![json!({"a":2})], "{unit}");
        }
        let sql = format!("SELECT CAST(AVG(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS BIGINT) AS a FROM {table} ORDER BY i");
        let result = execute_sql(&sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(
            result.rows,
            vec![json!({"a":-4}), json!({"a":2}), json!({"a":8})],
            "{unit}"
        );
    }
}
