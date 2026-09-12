//! Metadata predicate semantics compared with an independent DataFusion table.

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::json::ArrayWriter;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use logex_query::{NativeStorageSnapshot, SqlQueryPage, execute_sql_page_on_snapshot};
use serde_json::Value;

fn reference_metadata() -> SessionContext {
    // These values independently describe selected public LogEx columns.
    // Keeping the fixture small makes row differences easy to diagnose while
    // retaining the public Utf8/UInt64/nullable-Utf8 metadata field types.
    let columns = vec![
        (
            "table_name",
            Arc::new(StringArray::from(vec!["logs"; 7])) as ArrayRef,
        ),
        (
            "column_name",
            Arc::new(StringArray::from(vec![
                "block_number",
                "block_hash",
                "timestamp",
                "tx_hash",
                "topic2",
                "data",
                "source",
            ])) as ArrayRef,
        ),
        (
            "ordinal_position",
            Arc::new(UInt64Array::from(vec![1, 2, 3, 4, 10, 13, 15])) as ArrayRef,
        ),
        (
            "column_default",
            Arc::new(StringArray::from(vec![None::<&str>; 7])) as ArrayRef,
        ),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, values)| {
                Field::new(*name, values.data_type().clone(), *name == "column_default")
            })
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        columns.into_iter().map(|(_, values)| values).collect(),
    )
    .unwrap();
    let reference = SessionContext::new();
    reference
        .register_table(
            "metadata_columns",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    reference
}

fn reference_sql(sql: &str) -> String {
    sql.replace("information_schema.columns", "metadata_columns")
}

async fn reference_rows(reference: &SessionContext, sql: &str) -> Vec<Value> {
    // Planning and full collection are both required: DataFusion can defer
    // expression errors until physical execution.
    let batches = reference
        .sql(&reference_sql(sql))
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let mut writer = ArrayWriter::new(Vec::new());
    writer
        .write_batches(&batches.iter().collect::<Vec<_>>())
        .unwrap();
    writer.finish().unwrap();
    serde_json::from_slice(&writer.into_inner()).unwrap()
}

async fn reference_rejects(reference: &SessionContext, sql: &str) -> bool {
    match reference.sql(&reference_sql(sql)).await {
        Err(_) => true,
        Ok(dataframe) => dataframe.collect().await.is_err(),
    }
}

async fn logex_rows(sql: &str) -> Result<Vec<Value>, String> {
    execute_sql_page_on_snapshot(
        sql,
        NativeStorageSnapshot::default(),
        0,
        SqlQueryPage::default(),
        None,
    )
    .await
    .map(|result| result.rows)
    .map_err(|error| error.to_string())
}

#[tokio::test]
async fn metadata_predicates_match_datafusion_three_valued_semantics() {
    let reference = reference_metadata();
    let mut failures = Vec::new();
    let equivalent_cases = [
        (
            "membership_and_range",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND table_name = 'logs' \
               AND ordinal_position BETWEEN 1 AND 4 \
               AND column_name IN ('block_number', 'timestamp', 'tx_hash') \
             ORDER BY ordinal_position DESC",
        ),
        (
            "negated_membership",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_name NOT IN ('block_hash', 'timestamp') \
             ORDER BY ordinal_position",
        ),
        (
            "negated_range",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position NOT BETWEEN 2 AND 3 \
             ORDER BY ordinal_position",
        ),
        (
            "reversed_comparison",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND 'timestamp' <= column_name \
             ORDER BY ordinal_position",
        ),
        (
            "numeric_string_coercion",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position = '1' \
             ORDER BY ordinal_position",
        ),
        (
            "leading_zero_is_lexical",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position = '01' \
             ORDER BY ordinal_position",
        ),
        (
            "nonnumeric_text_is_lexical",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position < 'a' \
             ORDER BY ordinal_position",
        ),
        (
            "reversed_numeric_string_comparison",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND '10' < ordinal_position \
             ORDER BY ordinal_position",
        ),
        (
            "mixed_range_uses_one_common_type",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position BETWEEN '2' AND 10 \
             ORDER BY ordinal_position",
        ),
        (
            "mixed_membership_uses_one_common_type",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position IN (1, '02', 'source') \
             ORDER BY ordinal_position",
        ),
        (
            "negated_mixed_membership_with_null",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position NOT IN (1, '02', NULL) \
             ORDER BY ordinal_position",
        ),
        (
            "null_equality",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_default = NULL \
             ORDER BY ordinal_position",
        ),
        (
            "null_inequality",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_default <> 'x' \
             ORDER BY ordinal_position",
        ),
        (
            "negated_null_range",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_default NOT BETWEEN 'a' AND 'z' \
             ORDER BY ordinal_position",
        ),
        (
            "negated_membership_with_null",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_name NOT IN ('block_number', NULL) \
             ORDER BY ordinal_position",
        ),
        (
            "unknown_or_true",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND (column_default <> 'x' OR column_name = 'block_number') \
             ORDER BY ordinal_position",
        ),
        (
            "unknown_or_false",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND (column_default <> 'x' OR column_name = 'absent') \
             ORDER BY ordinal_position",
        ),
        (
            "unknown_and_true",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_default <> 'x' \
               AND table_name = 'logs' \
             ORDER BY ordinal_position",
        ),
        (
            "stable_all_null_ordering",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
             ORDER BY column_default, ordinal_position DESC",
        ),
    ];

    for (case, sql) in equivalent_cases {
        let expected = reference_rows(&reference, sql).await;
        match logex_rows(sql).await {
            Ok(actual) if actual != expected => failures.push(format!(
                "{case}: expected {expected:?}, got {actual:?}: {sql}"
            )),
            Err(error) => failures.push(format!("{case} failed: {error}: {sql}")),
            Ok(_) => {}
        }
    }

    let truth_operands = [
        ("true", "table_name = 'logs'"),
        ("false", "table_name = 'absent'"),
        ("unknown", "column_default = 'x'"),
    ];
    let truth_matrices = [
        ("AND", [[7_usize, 0, 0], [0, 0, 0], [0, 0, 0]]),
        ("OR", [[7_usize, 7, 7], [7, 0, 0], [7, 0, 0]]),
    ];
    for (operator, expected_counts) in truth_matrices {
        for (left_index, (left_name, left)) in truth_operands.iter().enumerate() {
            for (right_index, (right_name, right)) in truth_operands.iter().enumerate() {
                let case = format!(
                    "{}_{}_{}",
                    operator.to_ascii_lowercase(),
                    left_name,
                    right_name
                );
                let sql = format!(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
                       AND (({left}) {operator} ({right})) \
                     ORDER BY ordinal_position"
                );
                let expected = reference_rows(&reference, &sql).await;
                let expected_count = expected_counts[left_index][right_index];
                assert_eq!(
                    expected.len(),
                    expected_count,
                    "independent truth-matrix oracle mismatch for {case}: {sql}"
                );
                match logex_rows(&sql).await {
                    Ok(actual) if actual.len() != expected_count || actual != expected => {
                        failures.push(format!(
                            "{case}: expected count {expected_count} and rows {expected:?}, \
                             got {actual:?}: {sql}"
                        ));
                    }
                    Err(error) => failures.push(format!("{case} failed: {error}: {sql}")),
                    Ok(_) => {}
                }
            }
        }
    }

    let counted_unknown_cases = [
        (
            "not_between_null_and_twenty",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position NOT BETWEEN NULL AND 20 \
             ORDER BY ordinal_position",
            0_usize,
        ),
        (
            "not_between_null_and_zero",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND ordinal_position NOT BETWEEN NULL AND 0 \
             ORDER BY ordinal_position",
            7,
        ),
        (
            "in_match_and_null",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_name IN ('block_number', NULL) \
             ORDER BY ordinal_position",
            1,
        ),
        (
            "in_nonmatch_and_null",
            "SELECT column_name FROM information_schema.columns \
             WHERE ordinal_position IN (1, 2, 3, 4, 10, 13, 15) \
               AND column_name IN ('absent', NULL) \
             ORDER BY ordinal_position",
            0,
        ),
    ];
    for (case, sql, expected_count) in counted_unknown_cases {
        let expected = reference_rows(&reference, sql).await;
        assert_eq!(
            expected.len(),
            expected_count,
            "independent UNKNOWN oracle mismatch for {case}: {sql}"
        );
        match logex_rows(sql).await {
            Ok(actual) if actual.len() != expected_count || actual != expected => {
                failures.push(format!(
                    "{case}: expected count {expected_count} and rows {expected:?}, \
                     got {actual:?}: {sql}"
                ))
            }
            Err(error) => failures.push(format!("{case} failed: {error}: {sql}")),
            Ok(_) => {}
        }
    }

    let incompatible_type = "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'absent' AND ordinal_position = TRUE LIMIT 0";
    assert!(
        reference_rejects(&reference, incompatible_type).await,
        "reference accepted incompatible comparison: {incompatible_type}"
    );
    if logex_rows(incompatible_type).await.is_ok() {
        failures.push(format!(
            "short-circuit hid incompatible comparison: {incompatible_type}"
        ));
    }

    for sql in [
        "SELECT column_name FROM information_schema.columns \
         WHERE ordinal_position = 0 AND column_name LIKE 'block%' LIMIT 0",
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'logs' OR lower(column_name) = 'block_number' LIMIT 0",
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name IN ('logs', ordinal_position + 1) LIMIT 0",
        "SELECT column_name FROM information_schema.columns \
         WHERE table_name = 'absent' AND 1 = 1 LIMIT 0",
        "SELECT column_name FROM information_schema.columns \
         WHERE ordinal_position <= 4 ORDER BY column_default NULLS FIRST",
    ] {
        if logex_rows(sql).await.is_ok() {
            failures.push(format!(
                "invalid or unsupported metadata expression was accepted: {sql}"
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
