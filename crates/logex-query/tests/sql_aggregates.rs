//! Exact aggregate results compared with an independent in-memory SQL table.
use std::sync::Arc;
use std::time::Instant;

use alloy_primitives::{Address, Bytes, keccak256};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use logex_index::IndexBuilder;
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::{Value, json};

fn fixture() -> (tempfile::TempDir, PartitionManager, SessionContext) {
    fixture_rows(6, false)
}

#[derive(Clone, Copy, Debug)]
enum AggregateLayout {
    Raw,
    Indexed,
    Compacted,
    Reopened,
}

fn fixture_rows(
    count: u64,
    indexed: bool,
) -> (tempfile::TempDir, PartitionManager, SessionContext) {
    fixture_rows_with_layout(
        count,
        if indexed {
            AggregateLayout::Indexed
        } else {
            AggregateLayout::Raw
        },
        8192,
    )
}

fn fixture_rows_with_layout(
    count: u64,
    layout: AggregateLayout,
    partition_target_rows: u64,
) -> (tempfile::TempDir, PartitionManager, SessionContext) {
    let rows: Vec<_> = (1..=count)
        .map(|i| {
            let amount = i * 10;
            let mut data = [0_u8; 32];
            data[24..].copy_from_slice(&amount.to_be_bytes());
            LogRow {
                block_number: i,
                block_hash: keccak256(i.to_le_bytes()),
                timestamp: i * 100,
                tx_hash: keccak256([i as u8]),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xaa + (i % 2) as u8),
                topic0: (i % 3 != 0).then(|| keccak256("event")),
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::copy_from_slice(&data),
                data_len: 32,
                source: Source::Receipt,
            }
        })
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows,
        compaction_safety_margin_blocks: 0,
    };
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    for chunk in rows.chunks(4096) {
        storage.write_batch(chunk).unwrap();
    }
    storage.checkpoint().unwrap();
    if matches!(
        layout,
        AggregateLayout::Compacted | AggregateLayout::Reopened
    ) {
        assert!(
            storage.compact_eligible_segments().unwrap() > 0,
            "layout must exercise compacted segments"
        );
    }
    if !matches!(layout, AggregateLayout::Raw) {
        for partition in storage
            .sealed_partitions()
            .iter()
            .chain(std::iter::once(storage.hot_partition()))
        {
            if partition.meta.row_count > 0 {
                IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
            }
        }
    }
    if matches!(layout, AggregateLayout::Reopened) {
        drop(storage);
        storage = PartitionManager::open(config).unwrap();
    }

    // Build directly from fixture values, without the LogEx provider, indexes,
    // filters or aggregate evaluator. The reference has a separate numeric
    // amount column; the real data column keeps its public SQL string type.
    let numeric = |v: Vec<u64>| Arc::new(UInt64Array::from(v)) as ArrayRef;
    let text = |v: Vec<String>| Arc::new(StringArray::from(v)) as ArrayRef;
    let columns = vec![
        (
            "block_number",
            numeric(rows.iter().map(|r| r.block_number).collect()),
        ),
        (
            "timestamp",
            numeric(rows.iter().map(|r| r.timestamp).collect()),
        ),
        ("tx_index", numeric(vec![0; rows.len()])),
        ("log_index", numeric(vec![0; rows.len()])),
        ("data_len", numeric(vec![32; rows.len()])),
        ("source", numeric(vec![0; rows.len()])),
        (
            "block_hash",
            text(rows.iter().map(|r| r.block_hash.to_string()).collect()),
        ),
        (
            "tx_hash",
            text(rows.iter().map(|r| r.tx_hash.to_string()).collect()),
        ),
        (
            "address",
            text(
                rows.iter()
                    .map(|r| format!("0x{}", hex::encode(r.address)))
                    .collect(),
            ),
        ),
        (
            "data",
            text(
                rows.iter()
                    .map(|r| format!("0x{}", hex::encode(&r.data)))
                    .collect(),
            ),
        ),
        (
            "topic0",
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.topic0.map(|t| t.to_string()))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "amount",
            Arc::new(Int64Array::from(
                (1..=i64::try_from(count).unwrap())
                    .map(|i| i * 10)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, a)| Field::new(*name, a.data_type().clone(), *name == "topic0"))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, a)| a).collect(),
    )
    .unwrap();
    let reference = SessionContext::new();
    reference
        .register_table(
            "logs",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    (tmp, storage, reference)
}

async fn reference_total(reference: &SessionContext, sql: &str) -> Value {
    let batches = reference.sql(sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    if values.is_null(0) {
        Value::Null
    } else {
        json!(values.value(0).to_string())
    }
}

async fn reference_grouped_totals(reference: &SessionContext, sql: &str) -> Vec<Value> {
    let batches = reference.sql(sql).await.unwrap().collect().await.unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            let addresses = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let totals = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..batch.num_rows()).map(move |row| {
                json!({
                    "address":addresses.value(row),
                    "total":if totals.is_null(row) {
                        Value::Null
                    } else {
                        json!(totals.value(row).to_string())
                    },
                })
            })
        })
        .collect()
}

#[tokio::test]
async fn aggregate_predicates_match_independent_sql_results() {
    let (_tmp, storage, reference) = fixture();
    let topic = keccak256("event");
    let predicates = [
        "block_number > 2".to_owned(),
        "TRUE".to_owned(),
        "FALSE".to_owned(),
        "NULL IS NULL".to_owned(),
        "NULL = 1".to_owned(),
        "NOT (block_number = 2)".to_owned(),
        "topic0 IS NULL".to_owned(),
        "topic0 IS NOT NULL".to_owned(),
        format!("topic0 != '{topic}'"),
        "topic0 != 'different'".to_owned(),
        "block_number NOT IN (99, NULL)".to_owned(),
        "block_number IN (2, NULL)".to_owned(),
        "block_number BETWEEN NULL AND 5".to_owned(),
        "block_number NOT BETWEEN NULL AND 5".to_owned(),
        "tx_index = 0".to_owned(),
        "block_number < timestamp".to_owned(),
        "block_number % 2 = 0".to_owned(),
        "address LIKE '0xaa%'".to_owned(),
        format!(
            "address = '{}'",
            format!("0x{}", "aa".repeat(20)).to_uppercase()
        ),
        "data >= '0X00'".to_owned(),
        "topic0 IS NULL OR block_number = 2".to_owned(),
        "NOT (topic0 IS NULL OR block_number = 2)".to_owned(),
    ];
    let mut differences = Vec::new();
    for predicate in predicates {
        for (actual, expected) in [
            (
                format!("SELECT SUM(data) AS total FROM logs WHERE {predicate}"),
                format!("SELECT SUM(amount) FROM logs WHERE {predicate}"),
            ),
            (
                format!(
                    "SELECT SUM(CASE WHEN {predicate} THEN data ELSE 0 END) AS total FROM logs"
                ),
                format!("SELECT SUM(CASE WHEN {predicate} THEN amount ELSE 0 END) FROM logs"),
            ),
            (
                format!("SELECT SUM(CASE WHEN {predicate} THEN data END) AS total FROM logs"),
                format!("SELECT SUM(CASE WHEN {predicate} THEN amount END) FROM logs"),
            ),
        ] {
            let expected = reference_total(&reference, &expected).await;
            let result = execute_sql(&actual, &storage, storage.head_block()).await;
            match result {
                Ok(result) if result.rows == vec![json!({"total":expected})] => {}
                result => {
                    differences.push(format!("{actual}\nexpected {expected}; got {result:?}"))
                }
            }
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n\n"));
}

#[tokio::test]
async fn aggregate_case_uses_only_selected_branches_and_rows() {
    let (_tmp, storage, reference) = fixture();
    let cases = [
        (
            "CASE WHEN block_number > 0 THEN data WHEN 10 / (block_number - block_number) > 0 THEN data ELSE 0 END",
            "TRUE",
        ),
        (
            "CASE WHEN 10 / (block_number - 1) > 0 THEN data END",
            "block_number % 2 = 0",
        ),
        ("CASE WHEN TRUE THEN data ELSE NULL END", "TRUE"),
        ("CASE WHEN FALSE THEN data ELSE NULL END", "TRUE"),
        ("CASE WHEN block_number > 2 THEN data ELSE -1 END", "TRUE"),
        ("CASE block_number WHEN 2 THEN data ELSE 0 END", "TRUE"),
    ];
    let mut differences = Vec::new();
    for (input, predicate) in cases {
        let actual = format!("SELECT SUM({input}) AS total FROM logs WHERE {predicate}");
        let expected = reference_total(
            &reference,
            &format!(
                "SELECT SUM({}) FROM logs WHERE {predicate}",
                input.replace("data", "amount")
            ),
        )
        .await;
        let result = execute_sql(&actual, &storage, storage.head_block()).await;
        match result {
            Ok(result) if result.rows == vec![json!({"total":expected})] => {}
            result => differences.push(format!("{actual}\nexpected {expected}; got {result:?}")),
        }
    }
    assert!(differences.is_empty(), "{}", differences.join("\n\n"));
}

#[tokio::test]
async fn aggregate_results_match_reference_across_storage_layouts() {
    let queries = [
        (
            "CASE block_number WHEN 2 THEN data WHEN 4 THEN -7 ELSE +1 END",
            "block_number >= 2",
        ),
        (
            "CASE WHEN block_number % 2 = 0 THEN CASE WHEN topic0 IS NULL THEN -3 ELSE data END ELSE -1 END",
            "block_number > 1",
        ),
        (
            "CASE WHEN topic0 IS NULL THEN data ELSE 0 END",
            "topic0 IS NULL OR block_number = 2",
        ),
        (
            "CASE WHEN 10 / (block_number - 1) > 0 THEN data END",
            "block_number % 2 = 0",
        ),
    ];
    for layout in [
        AggregateLayout::Raw,
        AggregateLayout::Indexed,
        AggregateLayout::Compacted,
        AggregateLayout::Reopened,
    ] {
        let (_tmp, storage, reference) = fixture_rows_with_layout(6, layout, 2);
        for (input, predicate) in queries {
            let expected = reference_total(
                &reference,
                &format!(
                    "SELECT SUM({}) FROM logs WHERE {predicate}",
                    input.replace("data", "amount")
                ),
            )
            .await;
            let actual = execute_sql(
                &format!("SELECT SUM({input}) AS total FROM logs WHERE {predicate}"),
                &storage,
                storage.head_block(),
            )
            .await
            .unwrap();
            assert_eq!(
                actual.rows,
                vec![json!({"total":expected})],
                "layout={layout:?} input={input} predicate={predicate}"
            );
        }
    }
}

#[tokio::test]
async fn aggregate_default_ordering_matches_reference_for_null_groups() {
    let (_tmp, storage, reference) = fixture();
    for direction in ["ASC", "DESC"] {
        let actual_sql = format!(
            "SELECT address, SUM(CASE WHEN block_number = 1 THEN data END) AS total \
             FROM logs GROUP BY address ORDER BY total {direction} LIMIT 1"
        );
        let reference_sql = actual_sql.replace("data", "amount");
        let expected = reference_grouped_totals(&reference, &reference_sql).await;
        let actual = execute_sql(&actual_sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(actual.rows, expected, "direction={direction}");
    }
}

#[tokio::test]
async fn aggregate_casts_keep_exact_data_compatibility_without_erasing_sql_types() {
    let (_tmp, storage, _reference) = fixture();
    for input in [
        "CAST(data AS NUMERIC)",
        "data::NUMERIC",
        "CAST(data AS DECIMAL)",
        "data::DEC",
        "CAST((data) AS NUMERIC)",
        "CAST(CAST(data AS DECIMAL) AS NUMERIC)",
        "CAST(CASE WHEN block_number % 2 = 0 THEN data ELSE 0 END AS NUMERIC)",
        "CASE WHEN TRUE THEN CAST(data AS DECIMAL) ELSE 0 END",
    ] {
        let result = execute_sql(
            &format!("SELECT SUM({input}) AS total FROM logs"),
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();
        let expected = if input.contains("block_number % 2") {
            "120"
        } else {
            "210"
        };
        assert_eq!(result.rows, vec![json!({"total":expected})], "{input}");
    }

    let ordinary = execute_sql(
        "SELECT SUM(1) AS total FROM logs",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(ordinary.rows, vec![json!({"total":6})]);

    let ordinary_decimal = execute_sql(
        "SELECT SUM(CAST(1 AS DECIMAL)) AS total FROM logs",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(ordinary_decimal.rows, vec![json!({"total":"6.0000000000"})]);

    let try_cast = execute_sql(
        "SELECT SUM(TRY_CAST(data AS NUMERIC)) AS total FROM logs",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(try_cast.rows, vec![json!({"total":null})]);

    for input in ["CAST(data AS BIGINT)", "CAST(data AS DECIMAL(3,1))"] {
        assert!(
            execute_sql(
                &format!("SELECT SUM({input}) AS total FROM logs"),
                &storage,
                storage.head_block(),
            )
            .await
            .is_err(),
            "{input} must retain DataFusion cast semantics"
        );
    }
    assert!(
        execute_sql(
            "SELECT SUM(CASE WHEN TRUE THEN data ELSE '1' END) AS total FROM logs",
            &storage,
            storage.head_block(),
        )
        .await
        .is_err(),
        "quoted strings must not be treated as exact integer literals"
    );
}

#[tokio::test]
async fn aggregate_validation_precedes_limit_zero_and_empty_scans() {
    let (empty_tmp, empty) = {
        let tmp = tempfile::tempdir().unwrap();
        let storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_owned(),
            partition_target_rows: 2,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        (tmp, storage)
    };
    let (_tmp, populated, _reference) = fixture();
    let queries = [
        "SELECT SUM(data) AS total FROM logs WHERE missing = 1 LIMIT 0",
        "SELECT SUM(CASE WHEN missing = 1 THEN data ELSE 0 END) AS total FROM logs LIMIT 0",
        "SELECT SUM(CASE WHEN data + 1 > 0 THEN data ELSE 0 END) AS total FROM logs LIMIT 0",
    ];
    for (name, storage) in [("populated", &populated), ("empty", &empty)] {
        for sql in queries {
            assert!(
                execute_sql(sql, storage, storage.head_block())
                    .await
                    .is_err(),
                "{name} storage accepted invalid SQL: {sql}"
            );
        }
    }
    drop(empty_tmp);
}

#[tokio::test]
#[ignore = "explicit release aggregate benchmark with disposable fixtures"]
async fn aggregate_latency() {
    let count = std::env::var("LOGEX_AGGREGATE_BENCH_ROWS")
        .map(|s| s.parse::<u64>().unwrap())
        .unwrap_or(20_000);
    assert!((1..=20_000).contains(&count));
    let (_tmp, storage, reference) = fixture_rows(count, true);
    let topic = keccak256("event");
    let specs = [
        (
            "plain",
            "data".to_owned(),
            "amount".to_owned(),
            "data_len = 32".to_owned(),
        ),
        (
            "conditional",
            format!("CASE WHEN topic0 = '{topic}' THEN data ELSE 0 END"),
            format!("CASE WHEN topic0 = '{topic}' THEN amount ELSE 0 END"),
            "data_len = 32".to_owned(),
        ),
        (
            "residual",
            "data".to_owned(),
            "amount".to_owned(),
            format!(
                "(topic0 = '{topic}' OR topic0 = '{}') AND data_len = 32",
                keccak256("other")
            ),
        ),
        (
            "nested_case",
            format!(
                "CASE WHEN block_number % 2 = 0 THEN CASE WHEN topic0 = '{topic}' THEN data ELSE 0 END ELSE 1 END"
            ),
            format!(
                "CASE WHEN block_number % 2 = 0 THEN CASE WHEN topic0 = '{topic}' THEN amount ELSE 0 END ELSE 1 END"
            ),
            "data_len = 32".to_owned(),
        ),
    ];
    let mut cases = Vec::new();
    for (name, actual, expected, predicate) in specs {
        let expected = reference_total(
            &reference,
            &format!("SELECT SUM({expected}) FROM logs WHERE {predicate}"),
        )
        .await;
        cases.push((
            name,
            format!("SELECT SUM({actual}) AS total FROM logs WHERE {predicate}"),
            vec![json!({"total":expected})],
        ));
    }
    let mut grouped = (1..=count)
        .fold(std::collections::BTreeMap::new(), |mut totals, i| {
            let address = 0xaa + (i % 2) as u8;
            let amount = if i % 3 == 0 { 0 } else { i * 10 };
            *totals.entry(address).or_insert(0_u64) += amount;
            totals
        })
        .into_iter()
        .map(|(address, total)| {
            json!({
                "address":format!("0x{}", hex::encode([address; 20])),
                "total":total.to_string(),
            })
        })
        .collect::<Vec<_>>();
    grouped.sort_by(|left, right| {
        right["total"]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap()
            .cmp(&left["total"].as_str().unwrap().parse::<u64>().unwrap())
            .then_with(|| left["address"].as_str().cmp(&right["address"].as_str()))
    });
    cases.push((
        "grouped_conditional",
        format!(
            "SELECT address, SUM(CASE WHEN topic0 = '{topic}' THEN data ELSE 0 END) AS total FROM logs WHERE data_len = 32 GROUP BY address ORDER BY total DESC"
        ),
        grouped,
    ));
    println!(
        "{}",
        json!({"kind":"config","rows":count,"repeats":50,"segment_rows":8192,"batch_rows":4096,"indexed":true})
    );
    for iteration in 0..=50 {
        for offset in 0..cases.len() {
            let (metric, sql, expected) = &cases[(iteration + offset) % cases.len()];
            let start = Instant::now();
            let result = execute_sql(sql, &storage, storage.head_block())
                .await
                .unwrap();
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.;
            assert_eq!(&result.rows, expected);
            if iteration > 0 {
                println!(
                    "{}",
                    json!({"kind":"sample","metric":metric,"iteration":iteration,"elapsed_ms":elapsed_ms})
                );
            }
        }
    }
}
