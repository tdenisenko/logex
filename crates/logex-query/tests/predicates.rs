use std::sync::Arc;

use alloy_primitives::{Address, Bytes, keccak256};
use datafusion::arrow::array::{ArrayRef, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;
use logex_index::IndexBuilder;
use logex_query::execute_sql;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::json;
use tempfile::TempDir;

#[derive(Clone, Copy, Debug)]
enum Layout {
    Raw,
    Indexed,
    Compacted,
}

fn fixture(layout: Layout) -> (TempDir, PartitionManager, Vec<LogRow>) {
    let rows: Vec<_> = (0..6_u64)
        .map(|i| {
            let data = vec![0xab; (i % 3) as usize];
            LogRow {
                block_number: i + 1,
                block_hash: keccak256((i + 1).to_le_bytes()),
                timestamp: i * 10,
                tx_hash: keccak256([i as u8; 32]),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xaa + (i % 3) as u8),
                topic0: (i % 3 != 0).then(|| keccak256((i % 3).to_le_bytes())),
                topic1: None,
                topic2: None,
                topic3: None,
                data_len: data.len() as u32,
                data: Bytes::from(data),
                source: Source::Receipt,
            }
        })
        .collect();
    let (tmp, storage) = store_rows(&rows, layout);
    (tmp, storage, rows)
}

fn store_rows(rows: &[LogRow], layout: Layout) -> (TempDir, PartitionManager) {
    let tmp = TempDir::new().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: if matches!(layout, Layout::Compacted) {
            3
        } else {
            100
        },
        compaction_safety_margin_blocks: 0,
    };
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    storage.write_batch(rows).unwrap();
    storage.checkpoint().unwrap();
    if !matches!(layout, Layout::Raw) {
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
    if matches!(layout, Layout::Compacted) {
        assert!(storage.compact_eligible_segments().unwrap() > 0);
        drop(storage);
        storage = PartitionManager::open(config).unwrap();
    }
    (tmp, storage)
}

// Build the oracle directly from fixture values. It has no LogEx provider,
// native filters, stored columns or index code and performs no filter pushdown.
fn reference(rows: &[LogRow]) -> SessionContext {
    let numeric = |values: Vec<u64>| Arc::new(UInt64Array::from(values)) as ArrayRef;
    let text = |values: Vec<String>| Arc::new(StringArray::from(values)) as ArrayRef;
    let columns = vec![
        (
            "block_number",
            numeric(rows.iter().map(|r| r.block_number).collect()),
        ),
        (
            "timestamp",
            numeric(rows.iter().map(|r| r.timestamp).collect()),
        ),
        (
            "tx_index",
            numeric(rows.iter().map(|r| u64::from(r.tx_index)).collect()),
        ),
        (
            "log_index",
            numeric(rows.iter().map(|r| u64::from(r.log_index)).collect()),
        ),
        (
            "data_len",
            numeric(rows.iter().map(|r| u64::from(r.data_len)).collect()),
        ),
        (
            "source",
            numeric(rows.iter().map(|r| r.source as u64).collect()),
        ),
        (
            "block_hash",
            text(rows.iter().map(|r| r.block_hash.to_string()).collect()),
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
    ];
    let schema = Arc::new(Schema::new(
        columns
            .iter()
            .map(|(name, values)| Field::new(*name, values.data_type().clone(), *name == "topic0"))
            .collect::<Vec<_>>(),
    ));
    let batch = RecordBatch::try_new(
        schema.clone(),
        columns.into_iter().map(|(_, values)| values).collect(),
    )
    .unwrap();
    let ctx = SessionContext::new();
    ctx.register_table(
        "logs",
        Arc::new(MemTable::try_new(schema, vec![vec![batch]]).unwrap()),
    )
    .unwrap();
    ctx
}

async fn compare_predicates(layout: Layout) {
    let (_tmp, storage, rows) = fixture(layout);
    let ctx = reference(&rows);
    let address_a = format!("0x{}", hex::encode(rows[0].address));
    let address_b = format!("0x{}", hex::encode(rows[1].address));
    let mut predicates = vec![
        "block_number != 2".to_owned(),
        "timestamp <> 10".to_owned(),
        "2 != block_number".to_owned(),
        "block_number + 1".to_owned(),
        "block_number = 2 AND block_number = 3".to_owned(),
        "block_number BETWEEN 5 AND 2".to_owned(),
        "timestamp < 0".to_owned(),
        "0 > timestamp".to_owned(),
        "data_len = 0 AND data_len = 1".to_owned(),
        format!(
            "block_hash = '{}' AND block_hash = '{}'",
            rows[0].block_hash, rows[1].block_hash
        ),
        format!("address = '{address_a}' AND address = '{address_b}'"),
        format!("address = '{address_a}' AND address = '{address_b}' AND address = '{address_a}'"),
        format!("address IN ('{address_a}') AND address IN ('{address_b}')"),
        format!(
            "address = '{}'",
            address_a.to_uppercase().replacen("0X", "0x", 1)
        ),
        format!("address = '{}'", &address_a[2..]),
        format!("address = ' {address_a} '"),
        format!(
            "address <> '{}'",
            address_a.to_uppercase().replacen("0X", "0x", 1)
        ),
        format!("block_hash = '0x{}'", hex::encode_upper(rows[0].block_hash)),
        format!(
            "topic0 IN ('0x{}')",
            hex::encode_upper(rows[1].topic0.unwrap())
        ),
        "data = '0xAB'".to_owned(),
        "data = 'ab'".to_owned(),
        "data >= '0xAB'".to_owned(),
        "data <> '0xAB'".to_owned(),
        "data = ' 0xab '".to_owned(),
        "data >= '0x0'".to_owned(),
        "data_len = -1".to_owned(),
        "(block_number >= 2 AND block_number < 5) OR data_len = 0".to_owned(),
        "topic0 IS NULL OR data_len = 1".to_owned(),
        "topic0 NOT IN (NULL)".to_owned(),
        "data_len = 4294967296".to_owned(),
        "block_number >= 2 AND block_number <= 4".to_owned(),
    ];
    let atoms = [
        "block_number != 2".to_owned(),
        "block_number >= 2".to_owned(),
        "block_number <= 4".to_owned(),
        "timestamp > 20".to_owned(),
        "timestamp < 0".to_owned(),
        "data_len = 0".to_owned(),
        "data_len = 1".to_owned(),
        format!("address = '{address_a}'"),
        format!("address = '{address_b}'"),
        format!("address IN ('{address_a}', '{address_b}')"),
        "topic0 IS NULL".to_owned(),
        format!("topic0 = '{}'", rows[1].topic0.unwrap()),
    ];
    let mut seed = 0xdc9b_a870_u32;
    for i in 0..128 {
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 17;
            seed ^= seed << 5;
            &atoms[seed as usize % atoms.len()]
        };
        let (a, b, c) = (next(), next(), next());
        predicates.push(if i % 4 == 0 {
            format!("({a} OR {b}) AND {c}")
        } else {
            format!("{a} AND {b} AND {c}")
        });
    }
    let mut failures = Vec::new();
    for predicate in predicates {
        let sql = format!(
            "SELECT block_number FROM logs WHERE {predicate} ORDER BY block_number ASC, tx_index ASC, log_index ASC"
        );
        let expected = async {
            let batches = ctx.sql(&sql).await?.collect().await?;
            Ok::<_, datafusion::error::DataFusionError>(
                batches
                    .iter()
                    .flat_map(|b| {
                        b.column(0)
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap()
                            .values()
                            .iter()
                            .copied()
                    })
                    .collect::<Vec<_>>(),
            )
        }
        .await;
        let mut queries = vec![
            sql.clone(),
            sql.replace("ORDER BY block_number ASC", "ORDER BY block_number + 0 ASC"),
        ];
        if expected.is_ok() {
            queries.push(format!(
                "SELECT COUNT(*) AS total FROM logs WHERE {predicate}"
            ));
        }
        for (shape, query) in queries.iter().enumerate() {
            let actual = execute_sql(query, &storage, storage.head_block()).await;
            let valid = match (&expected, &actual) {
                (Err(_), Err(_)) => true,
                (Ok(blocks), Ok(result)) => {
                    let values: Vec<_> = if shape == 2 {
                        vec![json!({"total":blocks.len()})]
                    } else {
                        blocks
                            .iter()
                            .map(|block| json!({"block_number":block}))
                            .collect()
                    };
                    result.rows == values
                }
                _ => false,
            };
            if !valid {
                failures.push(format!(
                    "{query}\nexpected blocks: {expected:?}\nactual: {actual:?}"
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "layout={layout:?}\n{}",
        failures.join("\n\n")
    );
}

#[tokio::test]
async fn predicates_match_independent_reference_without_indexes() {
    compare_predicates(Layout::Raw).await;
}

#[tokio::test]
async fn predicates_match_independent_reference_with_indexes() {
    compare_predicates(Layout::Indexed).await;
}

#[tokio::test]
async fn predicates_match_independent_reference_after_compaction() {
    compare_predicates(Layout::Compacted).await;
}

#[tokio::test]
async fn numeric_filters_preserve_zero_and_maximum_boundaries() {
    let (_tmp, _storage, mut rows) = fixture(Layout::Raw);
    rows[0].block_number = 0;
    rows.last_mut().unwrap().block_number = u64::MAX;
    rows.last_mut().unwrap().timestamp = u64::MAX;
    for row in &mut rows {
        row.block_hash = keccak256(row.block_number.to_le_bytes());
    }
    let mut failures = Vec::new();
    for layout in [Layout::Raw, Layout::Indexed, Layout::Compacted] {
        let (_tmp, storage) = store_rows(&rows, layout);
        for column in ["block_number", "timestamp"] {
            for operator in ["=", ">=", ">", "<=", "<"] {
                for bound in [0, u64::MAX] {
                    let expected: Vec<_> = rows
                        .iter()
                        .filter(|row| {
                            let value = if column == "block_number" {
                                row.block_number
                            } else {
                                row.timestamp
                            };
                            match operator {
                                "=" => value == bound,
                                ">=" => value >= bound,
                                ">" => value > bound,
                                "<=" => value <= bound,
                                "<" => value < bound,
                                _ => unreachable!(),
                            }
                        })
                        .map(|row| json!({"block_number": row.block_number}))
                        .collect();
                    let sql = format!(
                        "SELECT block_number FROM logs WHERE {column} {operator} {bound} ORDER BY block_number, tx_index, log_index"
                    );
                    let actual = execute_sql(&sql, &storage, storage.head_block())
                        .await
                        .unwrap();
                    if actual.rows != expected {
                        failures.push(format!(
                            "layout={layout:?}: {sql}: expected {expected:?}, actual {:?}",
                            actual.rows
                        ));
                    }
                }
            }
        }
        // Exercise the address/topic/block composite index as well as the
        // standalone numeric indexes above, including the last possible key.
        let last = rows.last().unwrap();
        let sql = format!(
            "SELECT block_number FROM logs WHERE address = '0x{}' AND topic0 = '{}' AND block_number BETWEEN 0 AND {} ORDER BY block_number, tx_index, log_index",
            hex::encode(last.address),
            last.topic0.unwrap(),
            u64::MAX,
        );
        let expected: Vec<_> = rows
            .iter()
            .filter(|r| r.address == last.address && r.topic0 == last.topic0)
            .map(|r| json!({"block_number":r.block_number}))
            .collect();
        let actual = execute_sql(&sql, &storage, storage.head_block())
            .await
            .unwrap();
        if actual.rows != expected {
            failures.push(format!(
                "composite layout={layout:?}: expected {expected:?}, actual {:?}",
                actual.rows
            ));
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
