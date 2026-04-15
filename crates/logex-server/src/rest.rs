use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};

use logex_query::{self, SqlQueryError};
use serde::Serialize;

use crate::handler::AppState;
use crate::storage_metrics;

/// Request body for POST /query.
#[derive(serde::Deserialize)]
pub struct QueryRequest {
    /// SQL query string.
    pub sql: String,
}

/// Response for a successful query.
#[derive(Serialize, serde::Deserialize)]
pub struct QueryResponse {
    pub rows: Vec<serde_json::Value>,
    pub total_scanned: u64,
    pub row_count: usize,
}

/// Error response.
#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        (StatusCode::BAD_REQUEST, Json(self)).into_response()
    }
}

/// Handle POST /query — execute a SQL query.
pub async fn handle_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> Response {
    let storage = state.storage.read().await;
    let head_block = storage.head_block();
    let result = match logex_query::execute_sql(&req.sql, &storage, head_block).await {
        Ok(r) => r,
        Err(SqlQueryError::DataFusion(e)) => {
            return ErrorResponse {
                error: format!("query error: {e}"),
            }
            .into_response();
        }
        Err(SqlQueryError::LegacySyntax(e)) => {
            return ErrorResponse {
                error: format!("query error: {e}"),
            }
            .into_response();
        }
        Err(SqlQueryError::Storage(e)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("execution error: {e}"),
                }),
            )
                .into_response();
        }
    };

    let row_count = result.rows.len();

    Json(QueryResponse {
        rows: result.rows,
        total_scanned: result.total_scanned,
        row_count,
    })
    .into_response()
}

/// Handle GET /health.
pub async fn handle_health(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let storage = state.storage.read().await;
    Json(serde_json::json!({
        "status": "ok",
        "total_rows": storage.total_rows(),
        "sealed_partitions": storage.sealed_count(),
        "head_block": storage.head_block(),
    }))
}

/// Handle GET /status — return detailed sync and storage status.
pub async fn handle_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let sync = state.sync_status.lock().unwrap().clone();
    let (total_rows, sealed_partitions, head_block, indexed_head_block, data_dir) = {
        let storage = state.storage.read().await;
        (
            storage.total_rows(),
            storage.sealed_count(),
            storage.head_block(),
            storage.indexed_head_block(),
            storage.data_dir().to_path_buf(),
        )
    };
    let storage_metrics =
        storage_metrics::load_or_refresh(Arc::clone(&state.storage_metrics), data_dir).await;
    let progress_pct = if sync.target_block > 0 {
        Some((sync.current_block as f64 / sync.target_block as f64 * 100.0).min(100.0))
    } else {
        None
    };
    Json(serde_json::json!({
        "synced": sync.node_state == logex_types::NodeState::Synced,
        "syncing": sync.syncing,
        "node_state": sync.node_state,
        "node_state_label": sync.node_state.as_label(),
        "connected_peers": sync.connected_peers,
        "serving_peers": sync.serving_peers,
        "pending_peers": sync.pending_peers,
        "current_block": sync.current_block,
        "target_block": sync.target_block,
        "blocks_per_sec": sync.blocks_per_sec,
        "blocks_per_minute": sync.blocks_per_minute,
        "logs_ingested": sync.logs_ingested,
        "total_rows": total_rows,
        "sealed_partitions": sealed_partitions,
        "head_block": head_block,
        "indexed_head_block": indexed_head_block,
        "storage_used_bytes": storage_metrics.storage_used_bytes,
        "disk_free_bytes": storage_metrics.disk_free_bytes,
        "eta_seconds": sync.eta_seconds,
        "progress_pct": progress_pct,
    }))
}

/// Handle GET / — serve the embedded web UI.
pub async fn handle_web_ui() -> Html<&'static str> {
    Html(include_str!("web_ui.html"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use axum::body::Body;
    use axum::http::Request;
    use logex_index::IndexBuilder;
    use logex_storage::{PartitionManager, PartitionManagerConfig};
    use logex_types::{LogRow, NodeState, Source, SyncStatus};
    use tempfile::TempDir;
    use tower::ServiceExt;

    fn make_test_rows() -> Vec<LogRow> {
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
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            },
            LogRow {
                block_number: 200,
                block_hash: B256::repeat_byte(0x02),
                timestamp: 1_700_001_200,
                tx_hash: B256::repeat_byte(0x22),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xBB),
                topic0: Some(B256::repeat_byte(0xEE)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Receipt,
            },
        ]
    }

    fn setup_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        };
        let mut mgr = PartitionManager::open(config).unwrap();
        mgr.write_batch(&make_test_rows()).unwrap();
        IndexBuilder::build_all_indexes(&mgr.hot_partition().meta.path).unwrap();
        (tmp, mgr)
    }

    #[tokio::test]
    async fn test_post_query() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({ "sql": "SELECT * FROM logs" });
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: QueryResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn test_post_query_with_filter() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let addr = hex::encode(Address::repeat_byte(0xAA));
        let body = serde_json::json!({
            "sql": format!("SELECT * FROM logs WHERE address = '0x{addr}'")
        });
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: QueryResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(result.row_count, 1);
    }

    #[tokio::test]
    async fn test_post_query_projection() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({
            "sql": "SELECT block_number, address AS emitter FROM logs ORDER BY block_number"
        });
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: QueryResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(result.row_count, 2);
        assert_eq!(result.rows[0]["block_number"], 100);
        assert!(result.rows[0].get("address").is_none());
        assert!(result.rows[0].get("emitter").is_some());
    }

    #[tokio::test]
    async fn test_post_query_aggregate() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({
            "sql": "SELECT COUNT(*) AS total FROM logs WHERE block_number <= latest"
        });
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: QueryResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0]["total"], 2);
    }

    #[tokio::test]
    async fn test_post_query_desc_limit() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({
            "sql": "SELECT block_number AS bn FROM logs ORDER BY block_number DESC LIMIT 1"
        });
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: QueryResponse = serde_json::from_slice(&body).unwrap();

        assert_eq!(result.row_count, 1);
        assert_eq!(result.rows[0]["bn"], 200);
    }

    #[tokio::test]
    async fn test_post_query_parse_error() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({ "sql": "NOT A QUERY" });
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_post_query_rejects_invalid_desc_without_order_by() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({ "sql": "select * from logs desc limit 10;" });
        let req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&body).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(err["error"].as_str().is_some_and(|msg| !msg.is_empty()));
    }

    #[tokio::test]
    async fn test_health_endpoint() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let health: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(health["status"], "ok");
        assert_eq!(health["total_rows"], 2);
    }

    #[tokio::test]
    async fn test_status_endpoint_reports_indexed_head() {
        let (_tmp, mut storage) = setup_storage();
        storage
            .record_sync_head(250, B256::repeat_byte(0xFE), 1_650_000_000)
            .unwrap();
        let state = Arc::new(AppState::new(
            storage,
            None,
            SyncStatus {
                node_state: NodeState::Reconnecting,
                syncing: true,
                connected_peers: 0,
                serving_peers: 0,
                pending_peers: 12,
                current_block: 250,
                target_block: 500,
                blocks_per_sec: 2.0,
                blocks_per_minute: 120.0,
                logs_ingested: 42,
                eta_seconds: Some(125.0),
            },
        ));
        let app = crate::build_router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/status")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["head_block"], 250);
        assert_eq!(status["indexed_head_block"], 200);
        assert_eq!(status["blocks_per_minute"], 120.0);
        assert_eq!(status["logs_ingested"], 42);
        assert_eq!(status["node_state"], "reconnecting");
        assert_eq!(status["connected_peers"], 0);
        assert_eq!(status["serving_peers"], 0);
        assert_eq!(status["pending_peers"], 12);
        assert!(status["storage_used_bytes"].as_u64().unwrap_or(0) > 0);
        assert!(status["disk_free_bytes"].as_u64().unwrap_or(0) > 0);
    }

    #[tokio::test]
    async fn test_status_endpoint_does_not_mark_disconnected_node_as_synced() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(
            storage,
            None,
            SyncStatus {
                node_state: NodeState::Disconnected,
                syncing: false,
                connected_peers: 0,
                serving_peers: 0,
                pending_peers: 0,
                current_block: 0,
                target_block: 0,
                blocks_per_sec: 0.0,
                blocks_per_minute: 0.0,
                logs_ingested: 0,
                eta_seconds: None,
            },
        ));
        let app = crate::build_router(state);

        let req = Request::builder()
            .method("GET")
            .uri("/status")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["synced"], false);
        assert_eq!(status["node_state"], "disconnected");
    }
}
