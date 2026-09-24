use super::*;
use crate::grpc::pb::log_ex_service_server::LogExService;
use crate::grpc::{LogExGrpcService, pb};
use crate::rest::{self, QueryRequest};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::Response;
use logex_storage::PartitionManagerConfig;

const SORT_QUERY: &str = "SELECT x FROM (VALUES (2), (1)) AS values_table(x) ORDER BY x + 0";

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
