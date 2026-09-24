use logex_query::{NativeStorageSnapshot, SqlQueryPage, execute_sql_page_on_snapshot};
use serde_json::{Value, json};

async fn scalar(sql: &str) -> Result<Value, String> {
    execute_sql_page_on_snapshot(
        sql,
        NativeStorageSnapshot::default(),
        0,
        SqlQueryPage::default(),
        None,
    )
    .await
    .map(|result| result.rows[0]["value"].clone())
    .map_err(|error| error.to_string())
}

#[tokio::test]
async fn repeat_rejects_checked_length_overflow_without_panicking() {
    let error = scalar("SELECT repeat('abcd', 4611686018427387904) AS value")
        .await
        .unwrap_err();
    assert!(error.contains("string size overflow"), "{error}");
}

#[tokio::test]
async fn repeat_preserves_small_zero_negative_null_and_empty_semantics() {
    for (sql, expected) in [
        ("SELECT repeat('ab', 3) AS value", json!("ababab")),
        ("SELECT repeat('ab', 0) AS value", json!("")),
        ("SELECT repeat('ab', -2) AS value", json!("")),
        (
            "SELECT repeat(CAST(NULL AS VARCHAR), 3) AS value",
            Value::Null,
        ),
        ("SELECT repeat('', 9223372036854775807) AS value", json!("")),
    ] {
        assert_eq!(scalar(sql).await.unwrap(), expected, "{sql}");
    }
}
