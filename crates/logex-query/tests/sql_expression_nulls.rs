//! Independent finite NULL truth tables for scalar simplification.
//!
//! These expected results are written from the functions' strict NULL behavior
//! (and array membership's treatment of NULL elements), not from another query
//! through the same optimizer. IS NULL prevents JSON's NaN encoding from hiding
//! a non-NULL result. VALUES exercises LogEx's general query path without disk data.
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use serde_json::{Value, json};

async fn assert_expression(expression: &str, expected: [Value; 3]) {
    let tmp = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let fixture = "(VALUES (0, NULL::DOUBLE, NULL::INT), (1, 2.0, 2), (2, 4.0, 4)) AS t(id, a, i)";
    let sql = format!(
        "SELECT id, {expression} AS result, ({expression}) IS NULL AS missing FROM {fixture} ORDER BY id"
    );
    let actual = execute_sql(&sql, &storage, storage.head_block())
        .await
        .unwrap();
    let rows: Vec<_> = expected
        .iter()
        .enumerate()
        .map(|(id, value)| json!({"id": id, "result": value, "missing": value.is_null()}))
        .collect();
    assert_eq!(actual.rows, rows, "projection: {sql}");
    for negate in [false, true] {
        let predicate = format!(
            "{}(({expression}) IS NULL)",
            if negate { "NOT " } else { "" }
        );
        let sql = format!("SELECT id FROM {fixture} WHERE {predicate} ORDER BY id");
        let actual = execute_sql(&sql, &storage, storage.head_block())
            .await
            .unwrap();
        let rows: Vec<_> = expected
            .iter()
            .enumerate()
            .filter(|(_, value)| value.is_null() != negate)
            .map(|(id, _)| json!({"id": id}))
            .collect();
        assert_eq!(actual.rows, rows, "filter: {sql}");
    }
}

#[tokio::test]
async fn logarithm_nullable_identities_preserve_null() {
    assert_expression("log(a, 1.0)", [json!(null), json!(0.0), json!(0.0)]).await;
    assert_expression("log(a, a)", [json!(null), json!(1.0), json!(1.0)]).await;
    assert_expression(
        "log(a, power(a, 2.0))",
        [json!(null), json!(2.0), json!(2.0)],
    )
    .await;
}

#[tokio::test]
async fn power_nullable_identities_preserve_null() {
    assert_expression("power(a, 0.0)", [json!(null), json!(1.0), json!(1.0)]).await;
    assert_expression("power(i, 0)", [json!(null), json!(1), json!(1)]).await;
    assert_expression("power(a, 1.0)", [json!(null), json!(2.0), json!(4.0)]).await;
    assert_expression(
        "power(a, log(a, 4.0))",
        [json!(null), json!(4.0), json!(4.0)],
    )
    .await;
}

#[tokio::test]
async fn xor_nullable_cancellation_preserves_null() {
    for expression in ["i ^ i", "(i ^ 7) ^ i", "i ^ (7 ^ i)", "((i ^ 7) ^ i) ^ i"] {
        let expected = match expression {
            "i ^ i" => [json!(null), json!(0), json!(0)],
            "((i ^ 7) ^ i) ^ i" => [json!(null), json!(5), json!(3)],
            _ => [json!(null), json!(7), json!(7)],
        };
        assert_expression(expression, expected).await;
    }
    assert_expression("i ^ 0", [json!(null), json!(2), json!(4)]).await;
    assert_expression("i & 0", [json!(null), json!(0), json!(0)]).await;
    assert_expression("i | 0", [json!(null), json!(2), json!(4)]).await;
}

#[tokio::test]
async fn array_membership_nullable_elements_preserve_false() {
    assert_expression(
        "array_contains([i, 1], 2)",
        [json!(false), json!(true), json!(false)],
    )
    .await;
    assert_expression(
        "array_has(CAST([] AS INT[]), i)",
        [json!(null), json!(false), json!(false)],
    )
    .await;
    assert_expression(
        "array_has(CAST(NULL AS INT[]), i)",
        [json!(null), json!(null), json!(null)],
    )
    .await;
    assert_expression(
        "array_has([i, 1], NULL::INT)",
        [json!(null), json!(null), json!(null)],
    )
    .await;

    assert_expression(
        "array_has([i, 1], 2)",
        [json!(false), json!(true), json!(false)],
    )
    .await;
    assert_expression(
        "NOT array_has([i, 1], 2)",
        [json!(true), json!(false), json!(true)],
    )
    .await;
    assert_expression(
        "array_has([i, 1], 1)",
        [json!(true), json!(true), json!(true)],
    )
    .await;
    assert_expression(
        "array_has([i, 1], i)",
        [json!(null), json!(true), json!(true)],
    )
    .await;
    assert_expression(
        "array_has([NULL::INT, 1], i)",
        [json!(null), json!(false), json!(false)],
    )
    .await;
}

#[tokio::test]
async fn exact_sum_residual_and_case_preserve_scalar_nulls() {
    use alloy_primitives::{Address, Bytes, keccak256};
    use logex_types::{LogRow, Source};

    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let rows: Vec<_> = (0_u64..3)
        .map(|i| {
            let mut data = [0; 32];
            data[24..].copy_from_slice(&((i + 1) * 10).to_be_bytes());
            LogRow {
                block_number: i + 1,
                block_hash: keccak256(i.to_le_bytes()),
                timestamp: i + 1,
                tx_hash: keccak256(i.to_be_bytes()),
                tx_index: 0,
                log_index: i as u32,
                address: Address::repeat_byte(0xaa),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::copy_from_slice(&data),
                data_len: 32,
                source: Source::Receipt,
            }
        })
        .collect();
    storage.write_batch(&rows).unwrap();
    storage.checkpoint().unwrap();
    // Non-NULL is insufficient to justify log/power algebra. Zero and unit
    // bases produce non-NULL NaN for log(x,x); JSON alone would hide this.
    let sql = "SELECT log_index, isnan(log(CAST(log_index AS DOUBLE), CAST(log_index AS DOUBLE))) AS is_nan, log(CAST(log_index AS DOUBLE), CAST(log_index AS DOUBLE)) IS NULL AS missing, power(CAST(log_index AS DOUBLE), log(CAST(log_index AS DOUBLE), 4.0)) AS inverse FROM logs ORDER BY log_index";
    let actual = execute_sql(sql, &storage, storage.head_block())
        .await
        .unwrap();
    assert_eq!(
        actual.rows,
        vec![
            json!({"log_index": 0, "is_nan": true, "missing": false, "inverse": 1.0}),
            json!({"log_index": 1, "is_nan": true, "missing": false, "inverse": 1.0}),
            json!({"log_index": 2, "is_nan": false, "missing": false, "inverse": 4.0}),
        ]
    );
    for expression in [
        "log(CAST(address AS DOUBLE), 1.0)",
        "power(CAST(address AS DOUBLE), 0.0)",
        "CAST(address AS BIGINT) ^ CAST(address AS BIGINT)",
    ] {
        let sql = format!("SELECT {expression} AS result FROM logs");
        let error = execute_sql(&sql, &storage, storage.head_block())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("Cannot cast string"),
            "{sql}: {error}"
        );
    }
    // Payloads are exactly 10, 20, 30; only log_index=0 becomes NULL.
    for predicate in [
        "power(CAST(NULLIF(log_index, 0) AS DOUBLE), 0.0) IS NULL",
        "log(CAST(NULLIF(log_index, 0) AS DOUBLE) + 2.0, 1.0) IS NULL",
        "(NULLIF(CAST(log_index AS BIGINT), 0) ^ NULLIF(CAST(log_index AS BIGINT), 0)) IS NULL",
        "NOT array_has([NULLIF(CAST(log_index AS BIGINT), 0), 7], 1) AND log_index = 0",
    ] {
        for sql in [
            format!("SELECT SUM(data) AS total FROM logs WHERE {predicate}"),
            format!("SELECT SUM(CASE WHEN {predicate} THEN data ELSE 0 END) AS total FROM logs"),
        ] {
            let actual = execute_sql(&sql, &storage, storage.head_block())
                .await
                .unwrap();
            assert_eq!(actual.rows, vec![json!({"total": "10"})], "{sql}");
        }
    }

    for (sql, expected) in [
        (
            "SELECT SUM(data) AS total FROM logs \
             WHERE repeat('', 9223372036854775807) = '' AND log_index = 0",
            "10",
        ),
        (
            "SELECT SUM(CASE WHEN repeat('', 9223372036854775807) = '' \
             THEN data ELSE 0 END) AS total FROM logs",
            "60",
        ),
    ] {
        let actual = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(actual.rows, vec![json!({"total": expected})], "{sql}");
    }

    for sql in [
        "SELECT SUM(data) AS total FROM logs \
         WHERE repeat('abcd', 4611686018427387904) = ''",
        "SELECT SUM(CASE WHEN repeat('abcd', 4611686018427387904) = '' \
         THEN data ELSE 0 END) AS total FROM logs",
    ] {
        let error = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("string size overflow"),
            "{sql}: {error}"
        );
    }
}

#[test]
fn xor_retains_volatile_and_nonnull_runtime_operands() {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ToDFSchema;
    use datafusion::functions::math::expr_fn::random;
    use datafusion::logical_expr::execution_props::ExecutionProps;
    use datafusion::logical_expr::simplify::SimplifyContext;
    use datafusion::logical_expr::{Cast, Expr, col};
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;

    let schema = Schema::new(vec![Field::new("i", DataType::Int64, false)])
        .to_dfschema_ref()
        .unwrap();
    let props = ExecutionProps::new();
    let simplifier = ExprSimplifier::new(SimplifyContext::new(&props).with_schema(schema));
    let volatile = Expr::Cast(Cast::new(Box::new(random()), DataType::Int64));
    let expression = volatile.clone() ^ volatile;
    assert_eq!(simplifier.simplify(expression.clone()).unwrap(), expression);
    assert_eq!(
        simplifier.simplify(col("i") ^ col("i")).unwrap(),
        col("i") ^ col("i")
    );
}

#[test]
fn array_membership_empty_kernel_preserves_null_needle() {
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::{ScalarValue, config::ConfigOptions};
    use datafusion::functions_nested::array_has::array_has_udf;
    use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs};
    use std::sync::Arc;

    // Invoke the function kernel directly, bypassing every optimizer rule.
    let empty = ScalarValue::List(ScalarValue::new_list(&[], &DataType::Int32, true));
    let result = array_has_udf()
        .invoke_with_args(ScalarFunctionArgs {
            arg_fields: vec![
                Arc::new(Field::new("haystack", empty.data_type(), false)),
                Arc::new(Field::new("needle", DataType::Int32, true)),
            ],
            args: vec![
                ColumnarValue::Scalar(empty),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ],
            number_rows: 1,
            return_field: Arc::new(Field::new("result", DataType::Boolean, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap();
    assert_eq!(
        ScalarValue::try_from_array(&result.into_array(1).unwrap(), 0).unwrap(),
        ScalarValue::Boolean(None)
    );
}
