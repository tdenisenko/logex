//! SQL identifier normalization compared with an independent in-memory table.
use std::sync::Arc;
use std::time::Instant;

use alloy_primitives::{Address, Bytes, keccak256};
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use logex_index::IndexBuilder;
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::json;

#[derive(Clone, Copy, Debug)]
enum Layout {
    Raw,
    Indexed,
}

fn rows() -> Vec<LogRow> {
    [(1_u64, 1_u64, 0xbb_u8), (2, 9, 0xaa)]
        .into_iter()
        .map(|(block_number, amount, address)| {
            let mut data = [0_u8; 32];
            data[24..].copy_from_slice(&amount.to_be_bytes());
            LogRow {
                block_number,
                block_hash: keccak256(block_number.to_le_bytes()),
                timestamp: block_number * 100,
                tx_hash: keccak256([block_number as u8; 32]),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(address),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::copy_from_slice(&data),
                data_len: 32,
                source: Source::Receipt,
            }
        })
        .collect()
}

fn fixture(layout: Layout, populated: bool) -> (tempfile::TempDir, PartitionManager) {
    let fixture_rows = populated.then(rows).unwrap_or_default();
    fixture_with_rows(layout, &fixture_rows)
}

fn fixture_with_rows(
    layout: Layout,
    fixture_rows: &[LogRow],
) -> (tempfile::TempDir, PartitionManager) {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    if !fixture_rows.is_empty() {
        storage.write_batch(fixture_rows).unwrap();
        storage.checkpoint().unwrap();
        if matches!(layout, Layout::Indexed) {
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
    }
    (tmp, storage)
}

fn reference() -> SessionContext {
    let fixture_rows = rows();
    let columns = vec![
        (
            "block_number",
            Arc::new(UInt64Array::from(vec![1, 2])) as ArrayRef,
        ),
        (
            "timestamp",
            Arc::new(UInt64Array::from(vec![100, 200])) as ArrayRef,
        ),
        (
            "tx_index",
            Arc::new(UInt64Array::from(vec![0, 0])) as ArrayRef,
        ),
        (
            "log_index",
            Arc::new(UInt64Array::from(vec![0, 0])) as ArrayRef,
        ),
        (
            "address",
            Arc::new(StringArray::from(
                fixture_rows
                    .iter()
                    .map(|row| format!("0x{}", hex::encode(row.address)))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "source",
            Arc::new(UInt64Array::from(vec![0, 0])) as ArrayRef,
        ),
        ("amount", Arc::new(Int64Array::from(vec![1, 9])) as ArrayRef),
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, values)| Field::new(*name, values.data_type().clone(), false))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, values)| values).collect(),
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

fn source_reference(sources: Vec<u64>) -> SessionContext {
    let values = Arc::new(UInt64Array::from(sources)) as ArrayRef;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        values.data_type().clone(),
        false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
    let reference = SessionContext::new();
    reference
        .register_table(
            "logs",
            Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
        )
        .unwrap();
    reference
}

async fn reference_schema(reference: &SessionContext, sql: &str) -> Vec<String> {
    let batches = reference.sql(sql).await.unwrap().collect().await.unwrap();
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

async fn reference_rejects(reference: &SessionContext, sql: &str) -> bool {
    match reference.sql(sql).await {
        Err(_) => true,
        Ok(dataframe) => dataframe.collect().await.is_err(),
    }
}

#[tokio::test]
async fn native_identifiers_and_aliases_follow_datafusion_normalization() {
    let reference = reference();
    let cases = [
        (
            "SELECT LoGs.BLoCk_NuMbEr AS MiXeD FROM LoGs LIMIT 1",
            "mixed",
        ),
        (
            "SELECT \"logs\".\"block_number\" AS \"MiXeD\" FROM \"logs\" LIMIT 1",
            "MiXeD",
        ),
    ];
    for (sql, output_name) in cases {
        assert_eq!(reference_schema(&reference, sql).await, [output_name]);
        for layout in [Layout::Raw, Layout::Indexed] {
            let (_tmp, storage) = fixture(layout, true);
            let actual = execute_sql(sql, &storage, storage.head_block())
                .await
                .unwrap();
            assert_eq!(
                actual.rows,
                vec![json!({output_name: 1})],
                "{layout:?}: {sql}"
            );
        }
    }

    let order_alias_collision = "SELECT address AS block_number, tx_index, log_index FROM logs \
         ORDER BY block_number, tx_index, log_index LIMIT 1";
    let reference_batches = reference
        .sql(order_alias_collision)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        reference_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    for layout in [Layout::Raw, Layout::Indexed] {
        let (_tmp, storage) = fixture(layout, true);
        let actual = execute_sql(order_alias_collision, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(
            actual.rows[0]["block_number"], "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "{layout:?}"
        );
    }

    for layout in [Layout::Raw, Layout::Indexed] {
        let (_tmp, storage) = fixture(layout, true);
        let count = execute_sql(
            "SELECT COUNT(*) AS ToTaL FROM logs",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();
        assert_eq!(count.rows, vec![json!({"total": 2})]);

        let sum = execute_sql(
            "SELECT SUM(data) AS ToTaL FROM logs",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();
        assert_eq!(sum.rows, vec![json!({"total": "10"})]);
    }
}

#[tokio::test]
async fn quoted_identifier_case_is_validated_before_empty_or_limit_zero_results() {
    let reference = reference();
    let invalid = [
        (
            "SELECT \"BLOCK_NUMBER\" FROM logs",
            "SELECT \"BLOCK_NUMBER\" FROM logs",
        ),
        (
            "SELECT logs.\"BLOCK_NUMBER\" FROM logs LIMIT 0",
            "SELECT logs.\"BLOCK_NUMBER\" FROM logs LIMIT 0",
        ),
        (
            "SELECT \"LOGS\".block_number FROM logs",
            "SELECT \"LOGS\".block_number FROM logs",
        ),
        (
            "SELECT block_number FROM \"LOGS\" LIMIT 0",
            "SELECT block_number FROM \"LOGS\" LIMIT 0",
        ),
        (
            "SELECT COUNT(*) FROM logs WHERE \"BLOCK_NUMBER\" = 1 LIMIT 0",
            "SELECT COUNT(*) FROM logs WHERE \"BLOCK_NUMBER\" = 1 LIMIT 0",
        ),
        (
            "SELECT SUM(amount) FROM logs WHERE \"BLOCK_NUMBER\" = 1 LIMIT 0",
            "SELECT SUM(data) FROM logs WHERE \"BLOCK_NUMBER\" = 1 LIMIT 0",
        ),
        (
            "SELECT SUM(\"AMOUNT\") FROM logs LIMIT 0",
            "SELECT SUM(\"DATA\") FROM logs LIMIT 0",
        ),
        (
            "SELECT \"ADDRESS\", SUM(amount) FROM logs GROUP BY \"ADDRESS\" LIMIT 0",
            "SELECT \"ADDRESS\", SUM(data) FROM logs GROUP BY \"ADDRESS\" LIMIT 0",
        ),
    ];
    for (reference_sql, logex_sql) in invalid {
        assert!(
            reference_rejects(&reference, reference_sql).await,
            "reference accepted {reference_sql}"
        );
        for populated in [false, true] {
            for layout in [Layout::Raw, Layout::Indexed] {
                let (_tmp, storage) = fixture(layout, populated);
                assert!(
                    execute_sql(logex_sql, &storage, storage.head_block())
                        .await
                        .is_err(),
                    "{layout:?}, populated={populated} accepted {logex_sql}"
                );
            }
        }
    }

    assert!(
        reference
            .sql("SELECT SUM(amount) FROM logs WHERE block_number = 1 LIMIT 0")
            .await
            .is_ok()
    );
    for layout in [Layout::Raw, Layout::Indexed] {
        let (_tmp, storage) = fixture(layout, false);
        assert!(
            execute_sql(
                "SELECT SUM(data) FROM logs WHERE block_number = 1 LIMIT 0",
                &storage,
                storage.head_block(),
            )
            .await
            .is_ok()
        );
    }
}

#[tokio::test]
async fn normalized_duplicate_aliases_are_rejected_but_distinct_quoted_aliases_survive() {
    let reference = reference();
    let duplicate = "SELECT block_number AS Dup, block_number AS dUP FROM logs LIMIT 1";
    assert!(
        reference_rejects(&reference, duplicate).await,
        "DataFusion must reject aliases that normalize to one output name"
    );

    for layout in [Layout::Raw, Layout::Indexed] {
        let (_tmp, storage) = fixture(layout, true);
        assert!(
            execute_sql(duplicate, &storage, storage.head_block())
                .await
                .is_err(),
            "{layout:?} accepted aliases that normalize to the same JSON field"
        );

        let distinct = execute_sql(
            "SELECT block_number AS \"x\", block_number AS \"X\" FROM logs LIMIT 1",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();
        assert_eq!(distinct.rows, vec![json!({"x": 1, "X": 1})]);

        for duplicate_aggregate in [
            "SELECT SUM(data) AS Dup, SUM(data) AS dUP FROM logs",
            "SELECT source AS Dup, COUNT(*) AS dUP FROM logs GROUP BY source",
        ] {
            assert!(
                execute_sql(duplicate_aggregate, &storage, storage.head_block())
                    .await
                    .is_err(),
                "{layout:?} accepted aggregate aliases that normalize to one JSON field"
            );
        }
    }
}

#[tokio::test]
async fn aggregate_having_and_order_bind_distinct_quoted_aliases_exactly() {
    let reference = reference();
    let logex_projection = "address, SUM(data) AS \"x\", SUM(CASE WHEN block_number = 1 THEN data ELSE 0 END) AS \"X\"";
    let reference_projection = "address, SUM(amount) AS \"x\", SUM(CASE WHEN block_number = 1 THEN amount ELSE 0 END) AS \"X\"";
    let suffixes = [
        "GROUP BY address HAVING \"X\" > 0 ORDER BY \"X\" DESC",
        "GROUP BY address ORDER BY \"X\" DESC",
    ];

    for suffix in suffixes {
        let reference_sql = format!("SELECT {reference_projection} FROM logs {suffix}");
        let reference_batches = reference
            .sql(&reference_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected_addresses = reference_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(reference_batches[0].schema().field(1).name(), "x");
        assert_eq!(reference_batches[0].schema().field(2).name(), "X");

        let logex_sql = format!("SELECT {logex_projection} FROM logs {suffix}");
        for layout in [Layout::Raw, Layout::Indexed] {
            let (_tmp, storage) = fixture(layout, true);
            let actual = execute_sql(&logex_sql, &storage, storage.head_block())
                .await
                .unwrap();
            let actual_addresses = actual
                .rows
                .iter()
                .map(|row| row["address"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(actual_addresses, expected_addresses, "{layout:?}: {suffix}");
            assert!(actual.rows.iter().all(|row| row.get("x").is_some()));
            assert!(actual.rows.iter().all(|row| row.get("X").is_some()));
        }
    }
}

#[tokio::test]
async fn aggregate_alias_binding_respects_qualification_and_source_name_precedence() {
    let reference = reference();
    let invalid = [
        (
            "SELECT address, SUM(amount) AS total FROM logs GROUP BY address HAVING logs.total > 0",
            "SELECT address, SUM(data) AS total FROM logs GROUP BY address HAVING logs.total > 0",
        ),
        (
            "SELECT address, SUM(amount) AS total FROM logs GROUP BY address ORDER BY logs.total",
            "SELECT address, SUM(data) AS total FROM logs GROUP BY address ORDER BY logs.total",
        ),
        (
            "SELECT address, SUM(amount) AS block_number FROM logs GROUP BY address HAVING block_number > 1",
            "SELECT address, SUM(data) AS block_number FROM logs GROUP BY address HAVING block_number > 1",
        ),
    ];
    for (reference_sql, logex_sql) in invalid {
        assert!(
            reference_rejects(&reference, reference_sql).await,
            "reference accepted {reference_sql}"
        );
        for layout in [Layout::Raw, Layout::Indexed] {
            let (_tmp, storage) = fixture(layout, true);
            assert!(
                execute_sql(logex_sql, &storage, storage.head_block())
                    .await
                    .is_err(),
                "{layout:?} accepted {logex_sql}"
            );
        }
    }

    let valid = [(
        "SELECT address, SUM(amount) AS block_number FROM logs GROUP BY address ORDER BY block_number DESC",
        "SELECT address, SUM(data) AS block_number FROM logs GROUP BY address ORDER BY block_number DESC",
    )];
    for (reference_sql, logex_sql) in valid {
        let reference_batches = reference
            .sql(reference_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let expected_addresses = reference_batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|value| value.unwrap().to_owned())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        for layout in [Layout::Raw, Layout::Indexed] {
            let (_tmp, storage) = fixture(layout, true);
            let actual = execute_sql(logex_sql, &storage, storage.head_block())
                .await
                .unwrap();
            let actual_addresses = actual
                .rows
                .iter()
                .map(|row| row["address"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>();
            assert_eq!(
                actual_addresses, expected_addresses,
                "{layout:?}: {logex_sql}"
            );
        }
    }

    for layout in [Layout::Raw, Layout::Indexed] {
        let (_tmp, storage) = fixture(layout, true);
        let qualified_source = execute_sql(
            "SELECT source AS src, COUNT(*) AS n FROM logs GROUP BY source ORDER BY logs.source",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();
        assert_eq!(qualified_source.rows, vec![json!({"src": 0, "n": 2})]);
        assert!(
            execute_sql(
                "SELECT source AS src, COUNT(*) AS n FROM logs GROUP BY source ORDER BY logs.src",
                &storage,
                storage.head_block(),
            )
            .await
            .is_err()
        );
    }

    let mut count_rows = rows();
    count_rows[1].source = Source::Trace;
    let mut third = count_rows[0].clone();
    third.block_number = 3;
    third.block_hash = keccak256(3_u64.to_le_bytes());
    third.timestamp = 300;
    third.tx_hash = keccak256([3_u8; 32]);
    third.address = Address::repeat_byte(0xcc);
    count_rows.push(third);
    let count_order =
        "SELECT source AS src, COUNT(*) AS source FROM logs GROUP BY source ORDER BY source DESC";
    let count_reference = source_reference(vec![0, 1, 0]);
    let reference_batches = count_reference
        .sql(count_order)
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        reference_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        0
    );
    for layout in [Layout::Raw, Layout::Indexed] {
        let (_tmp, storage) = fixture_with_rows(layout, &count_rows);
        let actual = execute_sql(count_order, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(actual.rows[0]["src"], 0, "{layout:?}");
        assert_eq!(actual.rows[0]["source"], 2, "{layout:?}");
    }
}

#[tokio::test]
async fn introspection_uses_the_same_identifier_and_alias_rules() {
    let (_tmp, storage) = fixture(Layout::Raw, false);
    let unquoted = execute_sql(
        "SELECT CoLuMn_NaMe AS MiXeD FROM InFoRmAtIoN_ScHeMa.CoLuMnS \
         WHERE OrDiNaL_PoSiTiOn = 1",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(unquoted.rows, vec![json!({"mixed": "block_number"})]);

    let quoted = execute_sql(
        "SELECT \"column_name\" AS \"MiXeD\" FROM \"information_schema\".\"columns\" \
         WHERE \"ordinal_position\" = 1",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(quoted.rows, vec![json!({"MiXeD": "block_number"})]);

    let alias_order = execute_sql(
        "SELECT ordinal_position AS column_name FROM information_schema.columns \
         ORDER BY column_name LIMIT 1",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    assert_eq!(alias_order.rows, vec![json!({"column_name": 1})]);

    for sql in [
        "SELECT \"COLUMN_NAME\" FROM information_schema.columns LIMIT 0",
        "SELECT column_name FROM \"INFORMATION_SCHEMA\".columns LIMIT 0",
        "SELECT column_name FROM \"information_schema.columns\" LIMIT 0",
        "SELECT logs.column_name FROM information_schema.columns LIMIT 0",
        "SELECT column_name FROM information_schema.columns ORDER BY \"ORDINAL_POSITION\"",
        "SELECT column_name FROM information_schema.columns \
         WHERE ordinal_position = 0 AND \"TABLE_NAME\" = 'logs' LIMIT 0",
        "SELECT column_name FROM information_schema.columns \
         WHERE ordinal_position = 0 AND 1 = \"TABLE_NAME\" LIMIT 0",
    ] {
        assert!(
            execute_sql(sql, &storage, storage.head_block())
                .await
                .is_err(),
            "introspection accepted {sql}"
        );
    }
}

#[tokio::test]
#[ignore = "explicit release metadata benchmark with a disposable empty fixture"]
async fn metadata_latency() {
    let (_tmp, storage) = fixture(Layout::Raw, false);
    let cases = vec![
        (
            "canonical_tables",
            "SELECT table_name, table_type \
             FROM information_schema.tables \
             WHERE table_schema = 'public' \
             ORDER BY table_name",
            vec![json!({"table_name": "logs", "table_type": "BASE TABLE"})],
            1,
        ),
        (
            "filtered_columns",
            "SELECT column_name, data_type, is_nullable, ordinal_position \
             FROM information_schema.columns \
             WHERE table_name = 'logs' \
               AND column_name IN ('block_number', 'topic2', 'data') \
             ORDER BY ordinal_position",
            vec![
                json!({
                    "column_name": "block_number",
                    "data_type": "bigint",
                    "is_nullable": "NO",
                    "ordinal_position": 1,
                }),
                json!({
                    "column_name": "topic2",
                    "data_type": "text",
                    "is_nullable": "YES",
                    "ordinal_position": 10,
                }),
                json!({
                    "column_name": "data",
                    "data_type": "text",
                    "is_nullable": "NO",
                    "ordinal_position": 13,
                }),
            ],
            3,
        ),
        (
            "aliased_columns",
            "SELECT column_name AS name, ordinal_position AS position \
             FROM information_schema.columns \
             WHERE table_name = 'logs' \
               AND column_name IN ('block_number', 'topic2', 'data') \
             ORDER BY ordinal_position DESC",
            vec![
                json!({"name": "data", "position": 13}),
                json!({"name": "topic2", "position": 10}),
                json!({"name": "block_number", "position": 1}),
            ],
            3,
        ),
    ];

    for (_, sql, expected, total_scanned) in &cases {
        let result = execute_sql(sql, &storage, storage.head_block())
            .await
            .unwrap();
        assert_eq!(&result.rows, expected);
        assert_eq!(result.total_scanned, *total_scanned);
    }

    let fixture_digest = keccak256(
        serde_json::to_vec(
            &cases
                .iter()
                .map(|(metric, sql, expected, total_scanned)| {
                    json!({
                        "metric": metric,
                        "sql": sql,
                        "expected": expected,
                        "total_scanned": total_scanned,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap(),
    );
    println!(
        "{}",
        json!({
            "kind": "config",
            "fixture_digest": fixture_digest.to_string(),
            "shapes": cases.len(),
            "samples_per_shape": 1_000,
        })
    );

    for iteration in 0..1_000 {
        for offset in 0..cases.len() {
            let (metric, sql, expected, total_scanned) = &cases[(iteration + offset) % cases.len()];
            let start = Instant::now();
            let result = execute_sql(sql, &storage, storage.head_block())
                .await
                .unwrap();
            let elapsed_ms = start.elapsed().as_secs_f64() * 1_000.0;
            assert_eq!(&result.rows, expected);
            assert_eq!(result.total_scanned, *total_scanned);
            println!(
                "{}",
                json!({
                    "kind": "sample",
                    "metric": metric,
                    "iteration": iteration,
                    "elapsed_ms": elapsed_ms,
                })
            );
        }
    }
}
