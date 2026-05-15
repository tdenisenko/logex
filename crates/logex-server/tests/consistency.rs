use std::sync::Arc;

use alloy_primitives::{Address, B256, bytes};
use axum::body::Body;
use axum::http::Request;
use logex_index::IndexBuilder;
use logex_query::execute_sql;
use logex_server::grpc::pb::GetLogsRequest;
use logex_server::grpc::pb::log_ex_service_server::LogExService;
use logex_server::grpc::{LogExGrpcService, pb};
use logex_server::handler::AppState;
use logex_server::rest::QueryResponse;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source, SyncStatus};
use tower::ServiceExt;

fn make_rows() -> Vec<LogRow> {
    vec![
        LogRow {
            block_number: 100,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
            tx_hash: B256::repeat_byte(0x11),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(0xAA),
            topic0: Some(B256::repeat_byte(0xDD)),
            topic1: Some(B256::repeat_byte(0x01)),
            topic2: None,
            topic3: None,
            data: bytes!("deadbeef"),
            data_len: 4,
            source: Source::Receipt,
        },
        LogRow {
            block_number: 200,
            block_hash: B256::repeat_byte(0x02),
            timestamp: 1_700_001_200,
            tx_hash: B256::repeat_byte(0x22),
            tx_index: 1,
            log_index: 2,
            address: Address::repeat_byte(0xAA),
            topic0: Some(B256::repeat_byte(0xDD)),
            topic1: Some(B256::repeat_byte(0x02)),
            topic2: None,
            topic3: None,
            data: bytes!("cafe"),
            data_len: 2,
            source: Source::Receipt,
        },
        LogRow {
            block_number: 300,
            block_hash: B256::repeat_byte(0x03),
            timestamp: 1_700_002_400,
            tx_hash: B256::repeat_byte(0x33),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(0xBB),
            topic0: Some(B256::repeat_byte(0xEE)),
            topic1: None,
            topic2: None,
            topic3: None,
            data: bytes!(""),
            data_len: 0,
            source: Source::Receipt,
        },
    ]
}

fn setup_storage() -> (tempfile::TempDir, PartitionManager) {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_path_buf(),
        partition_target_rows: 1_000_000,
        compaction_safety_margin_blocks: 2_048,
    };
    let mut storage = PartitionManager::open(config).unwrap();
    storage.write_batch(&make_rows()).unwrap();
    IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
    (tmp, storage)
}

fn normalize_sql_rows(rows: &[serde_json::Value]) -> Vec<(u64, String, u64)> {
    rows.iter()
        .map(|row| {
            (
                row["block_number"].as_u64().unwrap(),
                row["address"].as_str().unwrap().to_owned(),
                row["log_index"].as_u64().unwrap(),
            )
        })
        .collect()
}

fn normalize_eth_logs(rows: &[serde_json::Value]) -> Vec<(u64, String, u64)> {
    rows.iter()
        .map(|row| {
            (
                u64::from_str_radix(
                    row["blockNumber"]
                        .as_str()
                        .unwrap()
                        .strip_prefix("0x")
                        .unwrap(),
                    16,
                )
                .unwrap(),
                row["address"].as_str().unwrap().to_owned(),
                u64::from_str_radix(
                    row["logIndex"]
                        .as_str()
                        .unwrap()
                        .strip_prefix("0x")
                        .unwrap(),
                    16,
                )
                .unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn rest_grpc_sql_and_eth_get_logs_stay_consistent() {
    let (_tmp, storage) = setup_storage();
    let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
    let app = logex_server::build_router(Arc::clone(&state));
    let grpc = LogExGrpcService::new(Arc::clone(&state));

    let sql = format!(
        "SELECT block_number, address, log_index FROM logs WHERE address = '0x{}' ORDER BY block_number ASC, tx_index ASC, log_index ASC",
        hex::encode(Address::repeat_byte(0xAA))
    );

    let direct_sql = {
        let storage = state.storage.read().await;
        execute_sql(&sql, &storage, storage.head_block())
            .await
            .unwrap()
            .rows
    };
    let direct_sql = normalize_sql_rows(&direct_sql);

    let rest_request = Request::builder()
        .method("POST")
        .uri("/query")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::json!({ "sql": sql }).to_string()))
        .unwrap();
    let rest_response = app.clone().oneshot(rest_request).await.unwrap();
    let rest_bytes = axum::body::to_bytes(rest_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let rest_query: QueryResponse = serde_json::from_slice(&rest_bytes).unwrap();
    let rest_sql = normalize_sql_rows(&rest_query.rows);

    let grpc_logs = grpc
        .get_logs(tonic::Request::new(GetLogsRequest {
            from_block: Some(0),
            to_block: Some(500),
            block_hash: Vec::new(),
            addresses: vec![Address::repeat_byte(0xAA).as_slice().to_vec()],
            topics: vec![pb::TopicFilter {
                match_any: false,
                any_of: vec![B256::repeat_byte(0xDD).as_slice().to_vec()],
            }],
            canonical_only: Some(true),
            descending: Some(false),
            limit: None,
            offset: None,
        }))
        .await
        .unwrap()
        .into_inner()
        .logs
        .into_iter()
        .map(|row| {
            (
                row.block_number,
                format!("0x{}", hex::encode(row.address)),
                row.log_index as u64,
            )
        })
        .collect::<Vec<_>>();

    let jsonrpc_request = serde_json::json!({
        "jsonrpc": "2.0",
        "method": "eth_getLogs",
        "params": [{
            "fromBlock": "0x0",
            "toBlock": "latest",
            "address": format!("0x{}", hex::encode(Address::repeat_byte(0xAA))),
            "topics": [format!("0x{}", hex::encode(B256::repeat_byte(0xDD)))]
        }],
        "id": 1
    });
    let jsonrpc_request = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .body(Body::from(jsonrpc_request.to_string()))
        .unwrap();
    let jsonrpc_response = app.oneshot(jsonrpc_request).await.unwrap();
    let jsonrpc_bytes = axum::body::to_bytes(jsonrpc_response.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let jsonrpc_body: serde_json::Value = serde_json::from_slice(&jsonrpc_bytes).unwrap();
    let eth_logs = normalize_eth_logs(jsonrpc_body["result"].as_array().unwrap());

    assert_eq!(direct_sql, rest_sql);
    assert_eq!(direct_sql, grpc_logs);
    assert_eq!(direct_sql, eth_logs);
}
