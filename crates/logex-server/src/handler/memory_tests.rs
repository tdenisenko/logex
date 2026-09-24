use super::*;
use crate::grpc::pb::log_ex_service_server::LogExService;
use crate::grpc::{LogExGrpcService, pb};
use crate::rest::{self, QueryRequest};
use alloy_primitives::{Address, B256, bytes};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use logex_storage::PartitionManagerConfig;
use logex_types::{LogRow, Source};

const SORT_QUERY: &str = "SELECT x FROM (VALUES (2), (1)) AS values_table(x) ORDER BY x + 0";
const SCAN_QUERY: &str = "SELECT block_hash || '' AS hash FROM logs ORDER BY hash";
const VARIABLE_SCAN_QUERY: &str = "SELECT data || '' AS payload FROM logs ORDER BY payload";

fn state(memory_bytes: usize) -> (tempfile::TempDir, Arc<AppState>) {
    let temp = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: temp.path().to_owned(),
        partition_target_rows: 1_000_000,
        compaction_safety_margin_blocks: 2_048,
    })
    .unwrap();
    let state = Arc::new(AppState::with_query_limits(
        storage,
        None,
        SyncStatus::default(),
        QueryConcurrencyLimit::new(2).unwrap(),
        QueryMemoryLimit::new(memory_bytes).unwrap(),
    ));
    (temp, state)
}

fn state_with_log(memory_bytes: usize) -> (tempfile::TempDir, Arc<AppState>) {
    state_with_logs(memory_bytes, 1)
}

fn state_with_logs(memory_bytes: usize, row_count: usize) -> (tempfile::TempDir, Arc<AppState>) {
    let temp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: temp.path().to_owned(),
        partition_target_rows: 1_000_000,
        compaction_safety_margin_blocks: 2_048,
    })
    .unwrap();
    let template = LogRow {
        block_number: 1,
        block_hash: B256::repeat_byte(1),
        timestamp: 2,
        tx_hash: B256::repeat_byte(3),
        tx_index: 0,
        log_index: 0,
        address: Address::repeat_byte(4),
        topic0: Some(B256::repeat_byte(5)),
        topic1: None,
        topic2: None,
        topic3: None,
        data: bytes!("cafe"),
        data_len: 2,
        source: Source::Receipt,
    };
    let rows = (0..row_count)
        .map(|index| LogRow {
            log_index: index as u32,
            ..template.clone()
        })
        .collect::<Vec<_>>();
    storage.write_batch(&rows).unwrap();
    storage.checkpoint().unwrap();
    let state = Arc::new(AppState::with_query_limits(
        storage,
        None,
        SyncStatus::default(),
        QueryConcurrencyLimit::new(2).unwrap(),
        QueryMemoryLimit::new(memory_bytes).unwrap(),
    ));
    (temp, state)
}

fn rest_request(sql: &str) -> QueryRequest {
    QueryRequest {
        sql: sql.to_owned(),
        limit: None,
        offset: 0,
    }
}

async fn json(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn grpc_request(sql: &str) -> pb::QueryRequest {
    pb::QueryRequest {
        sql: sql.to_owned(),
        limit: None,
        offset: None,
    }
}

#[tokio::test]
async fn shared_memory_capacity_applies_to_rest_and_grpc_without_latching_storage() {
    let (_temp, state) = state(64 * 1024 * 1024);
    let service = LogExGrpcService::new(Arc::clone(&state));
    let held = state
        .query_memory
        .reserve(state.query_memory.limit(), "test fixture")
        .unwrap();

    let response = rest::handle_query(
        State(Arc::clone(&state)),
        axum::Json(rest_request(SORT_QUERY)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = json(response).await;
    assert_eq!(body["status"], "query_capacity");
    assert_eq!(body["resource"], "memory");

    let status = service
        .query(tonic::Request::new(grpc_request(SORT_QUERY)))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    let bad_rest = rest::handle_query(
        State(Arc::clone(&state)),
        axum::Json(rest_request("SELECT * FROM")),
    )
    .await;
    assert_eq!(bad_rest.status(), StatusCode::BAD_REQUEST);
    let bad_grpc = service
        .query(tonic::Request::new(grpc_request("SELECT * FROM")))
        .await
        .unwrap_err();
    assert_eq!(bad_grpc.code(), tonic::Code::InvalidArgument);

    assert!(state.storage_failure().is_none());
    assert!(
        service
            .get_head_block(tonic::Request::new(pb::Empty {}))
            .await
            .is_ok()
    );

    drop(held);
    assert_eq!(state.query_memory.used(), 0);

    let response = rest::handle_query(
        State(Arc::clone(&state)),
        axum::Json(rest_request(SORT_QUERY)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = json(response).await;
    assert_eq!(body["row_count"], 2);
    assert_eq!(state.query_memory.used(), 0);

    let response = service
        .query(tonic::Request::new(grpc_request(SORT_QUERY)))
        .await
        .unwrap();
    assert_eq!(response.get_ref().row_count, 2);
    drop(response);
    assert_eq!(state.query_memory.used(), 0);
    assert!(state.storage_failure().is_none());
}

#[tokio::test]
async fn scan_output_capacity_is_typed_across_protocols_and_recovers() {
    let limit = 1024 * 1024;
    const ROWS: usize = 128;
    // Fixed-source materialization peaks near 8 KiB (raw plus decoded values).
    // It drops the raw input before Arrow construction, which overlaps the
    // retained ~4 KiB decoded values with ~8.5 KiB of text and offsets. A 10 KiB
    // allowance therefore admits the source and rejects specifically at output.
    const ALLOWANCE: usize = 10 * 1024;
    let (_temp, state) = state_with_logs(limit, ROWS);
    let service = LogExGrpcService::new(Arc::clone(&state));
    let held = state
        .query_memory
        .reserve(limit - ALLOWANCE, "test fixture")
        .unwrap();

    let response = rest::handle_query(
        State(Arc::clone(&state)),
        axum::Json(rest_request(SCAN_QUERY)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = json(response).await;
    assert_eq!(body["status"], "query_capacity");
    assert_eq!(body["resource"], "memory");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("sql arrow scan output")
    );
    assert!(state.storage_failure().is_none());

    let status = service
        .query(tonic::Request::new(grpc_request(SCAN_QUERY)))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("sql arrow scan output"));
    assert!(state.storage_failure().is_none());

    drop(held);
    assert_eq!(state.query_memory.used(), 0);
    let response = service
        .query(tonic::Request::new(grpc_request(SCAN_QUERY)))
        .await
        .unwrap();
    assert_eq!(response.get_ref().row_count, ROWS as u64);
    drop(response);
    assert_eq!(state.query_memory.used(), 0);
}

#[tokio::test]
async fn fixed_scan_source_capacity_is_typed_and_does_not_latch_storage() {
    let limit = 1024 * 1024;
    let (_temp, state) = state_with_log(limit);
    let service = LogExGrpcService::new(Arc::clone(&state));
    let held = state
        .query_memory
        .reserve(limit - 1, "test fixture")
        .unwrap();

    let response = rest::handle_query(
        State(Arc::clone(&state)),
        axum::Json(rest_request(SCAN_QUERY)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = json(response).await;
    assert_eq!(body["status"], "query_capacity");
    assert_eq!(body["resource"], "memory");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("captured column bytes")
    );
    assert!(state.storage_failure().is_none());

    let status = service
        .query(tonic::Request::new(grpc_request(SCAN_QUERY)))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("captured column bytes"));
    assert!(state.storage_failure().is_none());

    drop(held);
    assert_eq!(state.query_memory.used(), 0);
    let response = service
        .query(tonic::Request::new(grpc_request(SCAN_QUERY)))
        .await
        .unwrap();
    assert_eq!(response.get_ref().row_count, 1);
    drop(response);
    assert_eq!(state.query_memory.used(), 0);
    assert!(state.storage_failure().is_none());
}

#[tokio::test]
async fn variable_scan_source_capacity_is_typed_and_does_not_latch_storage() {
    let limit = 1024 * 1024;
    let (_temp, state) = state_with_log(limit);
    let service = LogExGrpcService::new(Arc::clone(&state));
    let held = state
        .query_memory
        .reserve(limit - 1, "test fixture")
        .unwrap();

    let response = rest::handle_query(
        State(Arc::clone(&state)),
        axum::Json(rest_request(VARIABLE_SCAN_QUERY)),
    )
    .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = json(response).await;
    assert_eq!(body["status"], "query_capacity");
    assert_eq!(body["resource"], "memory");
    assert!(
        body["error"]
            .as_str()
            .unwrap()
            .contains("captured column bytes")
    );
    assert!(state.storage_failure().is_none());

    let status = service
        .query(tonic::Request::new(grpc_request(VARIABLE_SCAN_QUERY)))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(status.message().contains("captured column bytes"));
    assert!(state.storage_failure().is_none());

    drop(held);
    assert_eq!(state.query_memory.used(), 0);
    let response = service
        .query(tonic::Request::new(grpc_request(VARIABLE_SCAN_QUERY)))
        .await
        .unwrap();
    assert_eq!(response.get_ref().row_count, 1);
    drop(response);
    assert_eq!(state.query_memory.used(), 0);
    assert!(state.storage_failure().is_none());
}
