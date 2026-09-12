use logex_query::{NativeStorageSnapshot, SqlQueryPage, execute_sql_page_on_snapshot};
use serde_json::{Value, json};

async fn value(sql: &str) -> Value {
    let result = execute_sql_page_on_snapshot(
        sql,
        NativeStorageSnapshot::default(),
        0,
        SqlQueryPage::default(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(result.rows.len(), 1, "{sql}");
    result.rows.into_iter().next().unwrap()
}

#[tokio::test]
async fn sql_decimal_results_preserve_exact_values() {
    assert_eq!(
        value("SELECT CAST('12345678901234567890.123456789012345678' AS DECIMAL(38,18)) AS value")
            .await,
        json!({"value":"12345678901234567890.123456789012345678"})
    );
}

#[tokio::test]
async fn sql_narrow_numeric_results_are_values() {
    assert_eq!(value("SELECT CAST(7 AS TINYINT) AS i8, CAST(-12 AS SMALLINT) AS i16, CAST(1.25 AS REAL) AS f32").await,
        json!({"i8":7,"i16":-12,"f32":1.25}));
}

#[tokio::test]
async fn sql_string_view_results_preserve_text() {
    assert_eq!(
        value("SELECT upper('abc') AS value").await,
        json!({"value":"ABC"})
    );
}

#[tokio::test]
async fn sql_nested_results_preserve_values_and_nulls() {
    assert_eq!(value("SELECT ARRAY[1, NULL, 3] AS numbers, named_struct('name', 'alice', 'count', 2) AS person").await,
        json!({"numbers":[1,null,3],"person":{"name":"alice","count":2}}));
}

#[tokio::test]
async fn sql_temporal_and_binary_results_are_values() {
    assert_eq!(value("SELECT DATE '2024-01-02' AS day, TIMESTAMP '2024-01-02 03:04:05' AS moment, decode('00ff', 'hex') AS bytes").await,
        json!({"day":"2024-01-02","moment":"2024-01-02T03:04:05","bytes":"00ff"}));
}

#[tokio::test]
async fn sql_rejects_duplicate_json_object_fields() {
    for sql in [
        "SELECT 1 AS item, 2 AS item",
        "SELECT named_struct('item', 1, 'item', 2) AS value",
    ] {
        let result = execute_sql_page_on_snapshot(
            sql,
            NativeStorageSnapshot::default(),
            0,
            SqlQueryPage::default(),
            None,
        )
        .await;
        assert!(
            result.is_err(),
            "duplicate JSON fields cannot be represented without losing values: {sql}: {result:?}"
        );
    }
}

#[tokio::test]
async fn information_schema_projections_cannot_silently_lose_fields() {
    for sql in [
        "SELECT column_name AS value, data_type AS value FROM information_schema.columns",
        "SELECT missing FROM information_schema.columns WHERE table_name = 'absent'",
        "SELECT missing FROM information_schema.columns LIMIT 0",
        "SELECT * EXCLUDE (table_name) FROM information_schema.tables",
    ] {
        let result = execute_sql_page_on_snapshot(
            sql,
            NativeStorageSnapshot::default(),
            0,
            SqlQueryPage::default(),
            None,
        )
        .await;
        assert!(
            result.is_err(),
            "invalid/unsupported projection must be explicit: {sql}: {result:?}"
        );
    }
    for sql in [
        "SELECT *, table_name AS extra FROM information_schema.tables",
        "SELECT table_name AS extra, * FROM information_schema.tables",
    ] {
        let row = value(sql).await;
        assert_eq!(row["extra"], row["table_name"]);
    }
}
