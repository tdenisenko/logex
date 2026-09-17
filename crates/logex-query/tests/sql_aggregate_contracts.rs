//! Public aggregate contracts checked against finite values, not another SQL engine.
use alloy_primitives::{Address, B256, Bytes};
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use num_bigint::BigUint;
use serde_json::{Value, json};

fn fixture(populated: bool) -> (tempfile::TempDir, PartitionManager) {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 3,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    if populated {
        let rows: Vec<_> = [vec![1, 0], vec![2], vec![0xff; 32]]
            .into_iter()
            .enumerate()
            .map(|(i, data)| LogRow {
                block_number: i as u64 + 1,
                block_hash: B256::repeat_byte(i as u8 + 1),
                timestamp: i as u64 + 1,
                tx_hash: B256::repeat_byte(i as u8 + 1),
                tx_index: 0,
                log_index: i as u32,
                address: Address::repeat_byte(if i < 2 { 0xaa } else { 0xbb }),
                topic0: (i == 0).then(|| B256::repeat_byte(0xcc)),
                topic1: None,
                topic2: None,
                topic3: None,
                data_len: data.len() as u32,
                data: Bytes::from(data),
                source: Source::Receipt,
            })
            .collect();
        storage.write_batch(&rows).unwrap();
        storage.checkpoint().unwrap();
    }
    (tmp, storage)
}

fn maximum_u256() -> BigUint {
    (BigUint::from(1_u8) << 256) - BigUint::from(1_u8)
}

async fn assert_rows(storage: &PartitionManager, sql: &str, expected: Vec<Value>) {
    let result = execute_sql(sql, storage, storage.head_block())
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error:?}"));
    assert_eq!(result.rows, expected, "{sql}");
}

#[tokio::test]
async fn ordinary_numeric_aggregates_keep_numbers_and_scaled_decimal_strings() {
    let (_tmp, storage) = fixture(true);
    assert_rows(
        &storage,
        "SELECT COUNT(*) AS n, COUNT(topic0) AS present, SUM(data_len) AS total, MIN(data_len) AS lo, MAX(data_len) AS hi, AVG(data_len) AS mean FROM logs",
        vec![json!({"n":3,"present":1,"total":35,"lo":1,"hi":32,"mean":35.0/3.0})],
    ).await;
    assert_rows(
        &storage,
        "SELECT SUM(CAST(log_index AS DECIMAL(10,2))) AS total, MIN(CAST(log_index AS DECIMAL(10,2))) AS lo, MAX(CAST(log_index AS DECIMAL(10,2))) AS hi, AVG(CAST(log_index AS DECIMAL(10,2))) AS mean FROM logs",
        vec![json!({"total":"3.00","lo":"0.00","hi":"2.00","mean":"1.000000"})],
    ).await;
}

#[tokio::test]
async fn exact_mixed_sum_arithmetic_retains_arbitrary_precision_strings() {
    let (_tmp, storage) = fixture(true);
    // Data bytes independently represent 256, 2 and 2^256-1. The total exceeds
    // uint256, so a floating-point or fixed-width oracle would be insufficient.
    let total = maximum_u256() + BigUint::from(258_u16);
    assert_rows(
        &storage,
        "SELECT SUM(data) AS total, SUM(1) AS ones, SUM(-2) AS negative, SUM(CASE WHEN FALSE THEN data END) AS missing, SUM(data) - SUM(1) AS difference FROM logs",
        vec![json!({"total":total.to_string(),"ones":"3","negative":"-6","missing":null,"difference":(total-BigUint::from(3_u8)).to_string()})],
    ).await;
    assert_rows(
        &storage,
        "SELECT SUM(CAST(data AS DECIMAL)) AS total, SUM(CAST(1 AS DECIMAL)) AS ones FROM logs",
        vec![json!({"total":(maximum_u256()+BigUint::from(258_u16)).to_string(),"ones":"3"})],
    )
    .await;
}

#[tokio::test]
async fn empty_and_all_null_aggregate_inputs_preserve_null_and_count_contracts() {
    for populated in [false, true] {
        let (_tmp, storage) = fixture(populated);
        assert_rows(
        &storage,
            "SELECT COUNT(*) AS n, COUNT(topic0) AS present, SUM(data_len) AS total, MIN(data_len) AS lo, MAX(data_len) AS hi, AVG(data_len) AS mean FROM logs WHERE FALSE",
            vec![json!({"n":0,"present":0,"total":null,"lo":null,"hi":null,"mean":null})],
    ).await;
        assert_rows(
        &storage,
            "SELECT SUM(data) AS total, SUM(1) AS ones, SUM(data) - SUM(1) AS difference FROM logs WHERE FALSE",
            vec![json!({"total":null,"ones":null,"difference":null})],
    ).await;
        assert_rows(
        &storage,
            "SELECT COUNT(topic1) AS n, SUM(CASE WHEN FALSE THEN log_index END) AS total, MIN(topic1) AS lo, MAX(topic1) AS hi, AVG(CASE WHEN FALSE THEN log_index END) AS mean FROM logs",
            vec![json!({"n":0,"total":null,"lo":null,"hi":null,"mean":null})],
    ).await;
        assert_rows(
        &storage,
            "SELECT SUM(CASE WHEN FALSE THEN data END) AS total, SUM(CASE WHEN FALSE THEN 1 END) AS ones FROM logs",
            vec![json!({"total":null,"ones":null})],
    ).await;
    }
}

#[tokio::test]
async fn native_mixed_group_order_and_having_compare_integer_values() {
    let (_tmp, storage) = fixture(true);
    let a = format!("0x{}", "aa".repeat(20));
    let b = format!("0x{}", "bb".repeat(20));
    assert_rows(
        &storage,
        "SELECT address, SUM(data) AS total, SUM(1) AS ones FROM logs GROUP BY address HAVING ones > 0 ORDER BY total DESC",
        vec![json!({"address":b,"total":maximum_u256().to_string(),"ones":"1"}),json!({"address":a,"total":"258","ones":"2"})],
    ).await;
    assert_rows(
        &storage,
        "SELECT address, COUNT(*) AS n, SUM(log_index) AS total, MIN(log_index) AS lo, MAX(log_index) AS hi, AVG(log_index) AS mean FROM logs GROUP BY address HAVING n > 1 ORDER BY total",
        vec![json!({"address":a,"n":2,"total":1,"lo":0,"hi":1,"mean":0.5})],
    ).await;
    assert_rows(
        &storage,
        "SELECT address, SUM(data) AS total, SUM(1) AS ones FROM logs WHERE FALSE GROUP BY address ORDER BY total",
        vec![],
    ).await;
}

#[tokio::test]
async fn general_text_aggregates_and_unsupported_exact_mixtures_are_explicit() {
    let (_tmp, storage) = fixture(true);
    assert_rows(
        &storage,
        "SELECT COUNT(data) AS n, MIN(data) AS lo, MAX(data) AS hi FROM logs",
        vec![json!({"n":3,"lo":"0x0100","hi":format!("0x{}","ff".repeat(32))})],
    )
    .await;
    // These leave the supported exact-SUM expression family. The general path
    // sees public hexadecimal text, whose numeric coercion must fail visibly.
    for companion in [
        "COUNT(*)",
        "SUM(log_index)",
        "MIN(log_index)",
        "MAX(log_index)",
        "AVG(log_index)",
        "SUM(1.5)",
        "SUM(CAST(1 AS DECIMAL(10,2)))",
    ] {
        let sql = format!("SELECT SUM(data) AS total, {companion} AS other FROM logs");
        let result = execute_sql(&sql, &storage, storage.head_block()).await;

        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.to_string().contains("Sum not supported for Utf8")),
            "unexpected successful unsupported mixture: {sql}: {result:?}"
        );
    }
}

#[tokio::test]
async fn aggregate_cast_arguments_bind_by_expression_identity() {
    let (_tmp, storage) = fixture(true);
    assert_rows(
        &storage,
        "SELECT SUM(1) AS a, SUM(1) AS b FROM logs",
        vec![json!({"a":3,"b":3})],
    )
    .await;
    assert_rows(
        &storage,
        "SELECT SUM(CAST(1 AS DECIMAL)) AS a FROM logs",
        vec![json!({"a":"3.0000000000"})],
    )
    .await;
    assert_rows(
        &storage,
        "SELECT COUNT(data) AS all_text, COUNT(TRY_CAST(data AS BIGINT)) AS numeric_text FROM logs",
        vec![json!({"all_text":3,"numeric_text":0})],
    )
    .await;
    assert_rows(
        &storage,
        "SELECT SUM(x) AS a, SUM(CAST(x AS BIGINT)) AS b FROM (VALUES (1.25), (2.75)) t(x)",
        vec![json!({"a":4.0,"b":3})],
    )
    .await;
    let error = execute_sql(
        "SELECT COUNT(data) AS all_text, COUNT(CAST(data AS BIGINT)) AS numeric_text FROM logs",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("Cast error"), "{error}");
    let cases = [
        (
            "SELECT SUM(1) AS a, SUM(CAST(1 AS DECIMAL)) AS b FROM logs",
            json!({"a":3,"b":"3.0000000000"}),
        ),
        (
            "SELECT SUM(CAST(1 AS DECIMAL)) AS b, SUM(1) AS a FROM logs",
            json!({"a":3,"b":"3.0000000000"}),
        ),
        (
            "SELECT SUM(log_index) AS a, SUM(CAST(log_index AS DECIMAL(10,2))) AS b FROM logs",
            json!({"a":3,"b":"3.00"}),
        ),
        (
            "SELECT SUM(log_index) + 1 AS a, SUM(CAST(log_index AS DECIMAL(10,2))) + 1 AS b FROM logs",
            json!({"a":"4","b":"4.00"}),
        ),
        (
            "SELECT MIN(log_index) AS a, MIN(CAST(log_index AS DECIMAL(10,2))) AS b, MAX(log_index) AS c, MAX(CAST(log_index AS DECIMAL(10,2))) AS d FROM logs",
            json!({"a":0,"b":"0.00","c":2,"d":"2.00"}),
        ),
        (
            "SELECT AVG(log_index) AS a, AVG(CAST(log_index AS DECIMAL(10,2))) AS b FROM logs",
            json!({"a":1.0,"b":"1.000000"}),
        ),
        (
            "SELECT COUNT(log_index) AS a, COUNT(CAST(log_index AS DECIMAL(10,2))) AS b FROM logs",
            json!({"a":3,"b":3}),
        ),
        (
            "SELECT SUM(log_index) AS a, SUM(CAST(log_index AS DECIMAL(10,2))) AS b FROM logs HAVING a > 0 AND b > 0",
            json!({"a":3,"b":"3.00"}),
        ),
    ];
    let mut failures = Vec::new();
    for (sql, expected) in cases {
        match execute_sql(sql, &storage, storage.head_block()).await {
            Ok(result) if result.rows == vec![expected.clone()] => {}
            result => failures.push(format!("{sql}: expected {expected}; got {result:?}")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn aggregate_binding_preserves_grouping_ordering_and_windows() {
    let (_tmp, storage) = fixture(true);
    assert_rows(
        &storage,
        "SELECT SUM(log_index) + 1 AS a FROM logs",
        vec![json!({"a":"4"})],
    )
    .await;
    for suffix in [
        "ORDER BY b DESC",
        "ORDER BY SUM(CAST(log_index AS DECIMAL(10,2))) DESC",
        "ORDER BY SUM(log_index) DESC",
    ] {
        let sql = format!(
            "SELECT address, SUM(log_index) AS a, SUM(CAST(log_index AS DECIMAL(10,2))) AS b FROM logs GROUP BY address {suffix}"
        );
        assert_rows(
            &storage,
            &sql,
            vec![
                json!({"address":format!("0x{}","bb".repeat(20)),"a":2,"b":"2.00"}),
                json!({"address":format!("0x{}","aa".repeat(20)),"a":1,"b":"1.00"}),
            ],
        )
        .await;
    }
    assert_rows(
        &storage, "SELECT SUM(log_index) AS a FROM logs GROUP BY address ORDER BY SUM(CAST(log_index AS DECIMAL(10,2))) DESC", vec![json!({"a":2}),json!({"a":1})],
    ).await;
    assert_rows(
        &storage,
        "SELECT SUM(log_index) AS a, SUM(TRY_CAST(log_index AS DECIMAL(10,2))) AS b FROM logs",
        vec![json!({"a":3,"b":"3.00"})],
    )
    .await;
    assert_rows(
        &storage, "SELECT SUM(log_index) AS a, SUM(CAST(log_index AS DECIMAL(10,2))) AS b FROM logs GROUP BY ROLLUP(address) ORDER BY a", vec![json!({"a":1,"b":"1.00"}),json!({"a":2,"b":"2.00"}),json!({"a":3,"b":"3.00"})],
    ).await;
    assert_rows(
        &storage, "SELECT SUM(log_index) AS a, SUM(CAST(log_index AS DECIMAL(10,2))) AS b, ROW_NUMBER() OVER (ORDER BY SUM(CAST(log_index AS DECIMAL(10,2))) DESC) AS rn FROM logs GROUP BY address QUALIFY rn = 1", vec![json!({"a":2,"b":"2.00","rn":1})],
    ).await;
    assert_rows(
        &storage, "SELECT SUM(log_index) AS a, SUM(CAST(log_index AS DECIMAL(10,2))) AS b FROM logs WHERE FALSE", vec![json!({"a":null,"b":null})],
    ).await;
    assert_rows(
        &storage, "SELECT SUM(x) AS a, SUM(CAST(x AS DECIMAL(10,2))) AS b FROM (SELECT log_index AS x, 1 AS __logex_aggregate_0, 2 AS \"sum(t.x)\" FROM logs) t GROUP BY __logex_aggregate_0, \"sum(t.x)\"", vec![json!({"a":3,"b":"3.00"})],
    ).await;
    assert!(
        execute_sql(
            "SELECT SUM(log_index), SUM(CAST(log_index AS DECIMAL(10,2))) FROM logs",
            &storage,
            storage.head_block()
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn cast_changed_values_drive_order_having_and_windows() {
    let (_tmp, storage) = fixture(false);
    // Truncation changes both totals and their order: A has 3.8 versus 2,
    // whereas B has 3.1 versus 3. These are independent finite sums.
    let input = "(VALUES ('a', 1.9), ('a', 1.9), ('b', 3.1)) t(g,x)";
    let sql = format!(
        "SELECT g, SUM(x) AS floats, SUM(CAST(x AS BIGINT)) AS ints FROM {input} GROUP BY g ORDER BY SUM(CAST(x AS BIGINT)) DESC"
    );
    assert_rows(
        &storage,
        &sql,
        vec![
            json!({"g":"b","floats":3.1,"ints":3}),
            json!({"g":"a","floats":3.8,"ints":2}),
        ],
    )
    .await;
    let sql = format!(
        "SELECT g, SUM(x) AS floats FROM {input} GROUP BY g ORDER BY SUM(CAST(x AS BIGINT)) DESC"
    );
    assert_rows(
        &storage,
        &sql,
        vec![json!({"g":"b","floats":3.1}), json!({"g":"a","floats":3.8})],
    )
    .await;
    let sql = format!(
        "SELECT g, SUM(x) AS floats, SUM(CAST(x AS BIGINT)) AS ints FROM {input} GROUP BY g HAVING SUM(CAST(x AS BIGINT)) > 2"
    );
    assert_rows(&storage, &sql, vec![json!({"g":"b","floats":3.1,"ints":3})]).await;
    let sql = format!(
        "SELECT g, SUM(x) AS floats, SUM(CAST(x AS BIGINT)) AS ints, ROW_NUMBER() OVER (ORDER BY SUM(CAST(x AS BIGINT)) DESC) AS rn FROM {input} GROUP BY g QUALIFY rn = 1"
    );
    assert_rows(
        &storage,
        &sql,
        vec![json!({"g":"b","floats":3.1,"ints":3,"rn":1})],
    )
    .await;
}

#[tokio::test]
async fn aggregate_binding_preserves_ordinary_ordering_and_visibility() {
    let (_tmp, storage) = fixture(true);
    for order in ["log_index DESC", "idx DESC", "1 DESC"] {
        assert_rows(
            &storage,
            &format!("SELECT log_index AS idx FROM logs ORDER BY {order}"),
            vec![json!({"idx":2}), json!({"idx":1}), json!({"idx":0})],
        )
        .await;
    }
    // These aggregate-only ORDER BY forms were unsupported before this fix.
    // Keep their explicit rejection; no extra grouping capability is promised.
    for sql in [
        "SELECT 42 AS a FROM logs ORDER BY SUM(log_index)",
        "SELECT 42 AS a FROM logs ORDER BY SUM(log_index), SUM(CAST(log_index AS BIGINT))",
        "SELECT log_index FROM logs ORDER BY SUM(log_index)",
    ] {
        assert!(
            execute_sql(sql, &storage, storage.head_block())
                .await
                .is_err()
        );
    }
}
