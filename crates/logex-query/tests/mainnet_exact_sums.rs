//! Regression rows captured from Ethereum mainnet; never synthesized or altered.
//! Expected integer totals were computed independently with Python int(data, 16).
use logex_index::IndexBuilder;
use logex_query::{
    NativeStorageSnapshot, SqlQueryError, SqlQueryPage, execute_sql_page_on_snapshot_with_memory,
};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, QueryMemoryBudget, QueryMemoryLimit};
use num_bigint::BigInt;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Deserialize)]
struct Fixture {
    rows: Vec<LogRow>,
    expected_groups: BTreeMap<String, Vec<Value>>,
    catalog_cases: Vec<CatalogCase>,
}

#[derive(Deserialize)]
struct CatalogCase {
    sql: String,
    expected: Vec<Value>,
}

fn fixture() -> Fixture {
    serde_json::from_str(include_str!("fixtures/mainnet-exact-sums.json")).unwrap()
}

fn storage(rows: &[LogRow]) -> (tempfile::TempDir, PartitionManager, PartitionManagerConfig) {
    let tmp = tempfile::tempdir().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 16,
        compaction_safety_margin_blocks: 0,
    };
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    for block in rows.chunk_by(|a, b| a.block_number == b.block_number) {
        storage.write_batch(block).unwrap();
    }
    storage.checkpoint().unwrap();
    assert!(storage.sealed_count() >= 2);
    (tmp, storage, config)
}

async fn check(storage: &PartitionManager, sql: &str, page: SqlQueryPage, expected: &[Value]) {
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(8 * 1024 * 1024).unwrap());
    let result = execute_sql_page_on_snapshot_with_memory(
        sql,
        NativeStorageSnapshot::for_unverified_inspection(storage),
        storage.head_block().unwrap(),
        page,
        None,
        memory.clone(),
    )
    .await
    .unwrap_or_else(|error| panic!("{sql}: {error}"));
    assert_eq!(result.rows.as_slice(), expected, "{sql}");
    assert!(result.total_scanned <= 64);
    drop(result);
    assert_eq!(memory.used(), 0, "{sql}");
}

fn integer(value: &Value) -> BigInt {
    value.as_str().unwrap().parse().unwrap()
}

#[tokio::test]
async fn mainnet_weth_wallet_refinement_preserves_wrapping_flows() {
    let rows = fixture().rows;
    let weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2";
    let deposit = "0xe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c";
    let withdrawal = "0x7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65";
    let wallet = rows
        .iter()
        .find(|row| row.topic0.is_some_and(|topic| topic.to_string() == deposit))
        .unwrap()
        .topic1
        .unwrap()
        .to_string();
    let matching: Vec<_> = rows
        .iter()
        .filter(|row| {
            format!("0x{}", hex::encode(row.address)) == weth
                && row.topic1.is_some_and(|topic| topic.to_string() == wallet)
                && row.topic0.is_some_and(|topic| {
                    [deposit, withdrawal].contains(&topic.to_string().as_str())
                })
        })
        .collect();
    assert!(!matching.is_empty());
    let expected: BigInt = matching
        .iter()
        .map(|row| {
            let units = BigInt::from_bytes_be(num_bigint::Sign::Plus, &row.data);
            if row.topic0.unwrap().to_string() == deposit {
                units
            } else {
                -units
            }
        })
        .sum();
    let (_tmp, mut storage, config) = storage(&rows);
    for layout in 0..4 {
        if layout == 1 {
            storage.compact_eligible_segments().unwrap();
        }
        if layout == 2 {
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
        if layout == 3 {
            drop(storage);
            storage = PartitionManager::open(config.clone()).unwrap();
        }
        let sql = format!(
            "SELECT SUM(CASE WHEN topic0='{deposit}' THEN data ELSE 0 END)-SUM(CASE WHEN topic0='{withdrawal}' THEN data ELSE 0 END) AS net_units FROM logs WHERE address='{weth}' AND topic0 IN ('{deposit}','{withdrawal}') AND (topic1='{wallet}' OR topic2='{wallet}') AND data_len=32"
        );
        check(
            &storage,
            &sql,
            SqlQueryPage::default(),
            &[json!({"net_units":expected.to_string()})],
        )
        .await;
    }
}

#[tokio::test]
async fn mainnet_topic_sums_match_independent_integers_across_storage_layouts() {
    let fixture = fixture();
    assert_eq!(fixture.rows.len(), 64);
    assert!(
        fixture.expected_groups["topic3"]
            .iter()
            .any(|r| r["key"].is_null())
    );
    assert!(
        fixture.expected_groups["topic1"]
            .iter()
            .any(|r| integer(&r["total"]) > BigInt::from(u64::MAX))
    );
    let (_tmp, mut storage, config) = storage(&fixture.rows);
    for layout in 0..4 {
        if layout == 1 {
            assert!(storage.compact_eligible_segments().unwrap() > 0);
        }
        if layout == 2 {
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
        if layout == 3 {
            drop(storage);
            storage = PartitionManager::open(config.clone()).unwrap();
        }
        for case in &fixture.catalog_cases {
            check(&storage, &case.sql, SqlQueryPage::default(), &case.expected).await;
        }
        for (column, groups) in &fixture.expected_groups {
            let base = format!(
                "SELECT logs.{column} AS participant, SUM(data) AS total FROM logs GROUP BY logs.{column}"
            );
            let expected: Vec<_> = groups
                .iter()
                .map(|r| json!({"participant": r["key"], "total": r["total"]}))
                .collect();
            check(
                &storage,
                &format!("{base} ORDER BY participant"),
                SqlQueryPage::default(),
                &expected,
            )
            .await;
            let mut reversed = expected.clone();
            reversed.reverse();
            check(
                &storage,
                &format!("{base} ORDER BY participant DESC"),
                SqlQueryPage::default(),
                &reversed,
            )
            .await;
            for descending in [false, true] {
                for nulls_first in [false, true] {
                    let mut expected = expected.clone();
                    expected.sort_by(|a, b| {
                        match (a["participant"].as_str(), b["participant"].as_str()) {
                            (Some(a), Some(b)) => {
                                if descending {
                                    b.cmp(a)
                                } else {
                                    a.cmp(b)
                                }
                            }
                            (None, None) => std::cmp::Ordering::Equal,
                            (None, _) => {
                                if nulls_first {
                                    std::cmp::Ordering::Less
                                } else {
                                    std::cmp::Ordering::Greater
                                }
                            }
                            (_, None) => {
                                if nulls_first {
                                    std::cmp::Ordering::Greater
                                } else {
                                    std::cmp::Ordering::Less
                                }
                            }
                        }
                    });
                    check(
                        &storage,
                        &format!(
                            "{base} ORDER BY participant {} NULLS {}",
                            if descending { "DESC" } else { "ASC" },
                            if nulls_first { "FIRST" } else { "LAST" }
                        ),
                        SqlQueryPage::default(),
                        &expected,
                    )
                    .await;
                }
            }
            let mut amounts = expected.clone();
            amounts.sort_by(|a, b| {
                integer(&b["total"])
                    .cmp(&integer(&a["total"]))
                    .then_with(|| a["participant"].as_str().cmp(&b["participant"].as_str()))
            });
            check(
                &storage,
                &format!("{base} ORDER BY total DESC, participant NULLS FIRST LIMIT 5"),
                SqlQueryPage {
                    offset: 1,
                    limit: Some(3),
                },
                &amounts[1..amounts.len().min(4)],
            )
            .await;

            // Real mainnet parity supplies NULL sums and ties without changing
            // any event, payload or indexed topic in the captured fixture.
            let mut conditional: Vec<_> = groups.iter().map(|r| json!({"participant": r["key"], "total": r["even"], "negative": (-integer(&r["odd"])).to_string()})).collect();
            conditional.sort_by(|a, b| {
                let a_total = a["total"].as_str().map(|s| s.parse::<BigInt>().unwrap());
                let b_total = b["total"].as_str().map(|s| s.parse::<BigInt>().unwrap());
                a_total
                    .cmp(&b_total)
                    .then_with(|| b["participant"].as_str().cmp(&a["participant"].as_str()))
            });
            let sql = format!(
                "SELECT {column} AS participant, SUM(CASE WHEN log_index % 2 = 0 THEN data END) AS total, SUM(CASE WHEN log_index % 2 = 0 THEN data ELSE 0 END) - SUM(data) AS negative FROM logs WHERE data_len = 32 AND source + 0 = 0 GROUP BY {column} HAVING negative <= 0 ORDER BY total ASC NULLS FIRST, participant DESC NULLS LAST"
            );
            check(&storage, &sql, SqlQueryPage::default(), &conditional).await;
            check(
                &storage,
                &format!(
                    "SELECT {column}, SUM(data) AS total FROM logs WHERE FALSE GROUP BY {column}"
                ),
                SqlQueryPage::default(),
                &[],
            )
            .await;
        }
    }
}

#[tokio::test]
async fn mainnet_topic_sum_releases_partial_work_on_capacity_and_cancellation() {
    let fixture = fixture();
    let (_tmp, storage, _config) = storage(&fixture.rows);
    let sql =
        "SELECT topic1, SUM(data) AS total FROM logs GROUP BY topic1 ORDER BY total DESC, topic1";
    for bytes in [1, 1024, 4096] {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(bytes).unwrap());
        let error = execute_sql_page_on_snapshot_with_memory(
            sql,
            NativeStorageSnapshot::for_unverified_inspection(&storage),
            storage.head_block().unwrap(),
            SqlQueryPage::default(),
            None,
            memory.clone(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(error, SqlQueryError::Capacity(_)),
            "{bytes}: {error}"
        );
        assert_eq!(memory.used(), 0);
    }
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(8 * 1024 * 1024).unwrap());
    let observed = Arc::new(AtomicBool::new(false));
    let cancel_memory = memory.clone();
    let cancel_observed = observed.clone();
    let cancel = Arc::new(move || {
        let owned = cancel_memory.used() > 0;
        cancel_observed.fetch_or(owned, Ordering::SeqCst);
        owned
    });
    let error = execute_sql_page_on_snapshot_with_memory(
        sql,
        NativeStorageSnapshot::for_unverified_inspection(&storage),
        storage.head_block().unwrap(),
        SqlQueryPage::default(),
        Some(cancel),
        memory.clone(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("query canceled"), "{error}");
    assert!(observed.load(Ordering::SeqCst));
    assert_eq!(memory.used(), 0);
}

#[tokio::test]
async fn mainnet_topic_sum_keeps_invalid_projection_and_order_errors() {
    let fixture = fixture();
    let (_tmp, storage, _config) = storage(&fixture.rows);
    for sql in [
        "SELECT topic1 AS amount, SUM(data) AS amount FROM logs GROUP BY topic1 ORDER BY amount",
        "SELECT topic2, SUM(data) AS total FROM logs GROUP BY topic1",
        "SELECT topic1, SUM(data) AS total FROM logs GROUP BY topic1 ORDER BY missing",
        "SELECT topic1, SUM(data) AS total FROM logs GROUP BY topic1 ORDER BY total WITH FILL",
    ] {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(8 * 1024 * 1024).unwrap());
        let result = execute_sql_page_on_snapshot_with_memory(
            sql,
            NativeStorageSnapshot::for_unverified_inspection(&storage),
            storage.head_block().unwrap(),
            SqlQueryPage::default(),
            None,
            memory.clone(),
        )
        .await;
        assert!(
            result.is_err(),
            "invalid query unexpectedly accepted: {sql}"
        );
        drop(result);
        assert_eq!(memory.used(), 0);
    }
}
