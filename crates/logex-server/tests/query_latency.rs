//! Direct gRPC handler latency, independent of network transport and live peers.
use std::sync::Arc;
use std::time::Instant;

use alloy_primitives::{Address, Bytes, keccak256};
use logex_index::IndexBuilder;
use logex_server::grpc::LogExGrpcService;
use logex_server::grpc::pb::QueryRequest;
use logex_server::grpc::pb::log_ex_service_server::LogExService;
use logex_server::handler::AppState;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source, SyncStatus};
use serde_json::{Value, json};
use tonic::Request;

#[tokio::test]
#[ignore = "explicit release gRPC benchmark; see docs/audit/grpc-query-locks.md"]
async fn benchmark_grpc_query_latency() {
    let rows: Vec<_> = (0..20_000_u32)
        .map(|index| {
            let block = 15_000_000 + u64::from(index / 128);
            let log_index = index % 128;
            let block_hash = keccak256(block.to_le_bytes());
            let mut tx_key = block_hash.to_vec();
            tx_key.extend_from_slice(&(log_index / 2).to_le_bytes());
            LogRow {
                block_number: block,
                block_hash,
                timestamp: 1_700_000_000 + (block - 15_000_000) * 12,
                tx_hash: keccak256(tx_key),
                tx_index: log_index / 2,
                log_index,
                address: Address::repeat_byte(0xaa),
                topic0: Some(keccak256("event")),
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::from(vec![(index % 256) as u8; 32]),
                data_len: 32,
                source: Source::Receipt,
            }
        })
        .collect();
    let tmp = tempfile::tempdir().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_path_buf(),
        partition_target_rows: 8192,
        compaction_safety_margin_blocks: 0,
    };
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    for batch in rows.chunks(4096) {
        storage.write_batch(batch).unwrap();
    }
    storage.checkpoint().unwrap();
    for partition in storage
        .sealed_partitions()
        .iter()
        .chain(std::iter::once(storage.hot_partition()))
    {
        if partition.meta.row_count > 0 {
            IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
        }
    }
    storage.compact_eligible_segments().unwrap();
    drop(storage);
    let state = Arc::new(AppState::new(
        PartitionManager::open(config).unwrap(),
        None,
        SyncStatus::default(),
    ));
    let service = LogExGrpcService::new(state);
    let expected_page: Vec<_> = rows.iter().rev().take(1000).map(|r| json!({
        "block_number":r.block_number,"block_hash":r.block_hash.to_string(),"timestamp":r.timestamp,
        "tx_hash":r.tx_hash.to_string(),"tx_index":r.tx_index,"log_index":r.log_index,
        "address":r.address.to_string().to_ascii_lowercase(),"topic0":r.topic0.unwrap().to_string(),
        "topic1":null,"topic2":null,"topic3":null,"topics":[r.topic0.unwrap().to_string()],
        "data":format!("0x{}",hex::encode(&r.data)),"data_len":32,"source":0,
    })).collect();
    let queries = [
        (
            "native_count",
            "SELECT COUNT(*) AS total FROM logs WHERE block_number <= latest",
            vec![json!({"total":rows.len()})],
        ),
        (
            "native_wide_page",
            "SELECT * FROM logs ORDER BY block_number DESC, tx_index DESC, log_index DESC LIMIT 1000",
            expected_page,
        ),
        (
            "datafusion_aggregate",
            "SELECT MAX(block_number) AS maximum, COUNT(*) AS total FROM logs",
            vec![json!({"maximum":rows.last().unwrap().block_number,"total":rows.len()})],
        ),
    ];
    println!(
        "{}",
        json!({"kind":"config","rows":rows.len(),"repeats":50,"segment_rows":8192,"batch_rows":4096,
        "fixture_digest":keccak256(serde_json::to_vec(&rows).unwrap()).to_string()})
    );
    for iteration in 0..=50 {
        for offset in 0..queries.len() {
            let (metric, sql, expected) = &queries[(iteration + offset) % queries.len()];
            let start = Instant::now();
            let result = service
                .query(Request::new(QueryRequest {
                    sql: (*sql).to_owned(),
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner();
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.;
            assert_eq!(result.row_count as usize, expected.len());
            let values: Vec<Value> = result
                .rows
                .iter()
                .map(|row| serde_json::from_str(&row.json).unwrap())
                .collect();
            assert_eq!(values, *expected);
            if iteration > 0 {
                println!(
                    "{}",
                    json!({"kind":"sample","metric":metric,"iteration":iteration,"elapsed_ms":elapsed_ms})
                );
            }
        }
    }
}
