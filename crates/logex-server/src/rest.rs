use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};

use logex_query::{self, is_simple_select};
use logex_types::LogRow;
use serde::Serialize;

use crate::handler::AppState;

/// Request body for POST /query.
#[derive(serde::Deserialize)]
pub struct QueryRequest {
    /// LogSQL query string.
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

/// Handle POST /query — execute a LogSQL query.
pub async fn handle_query(
    State(state): State<Arc<AppState>>,
    Json(req): Json<QueryRequest>,
) -> Response {
    let query = match logex_query::parse(&req.sql) {
        Ok(q) => q,
        Err(e) => {
            return ErrorResponse {
                error: format!("parse error: {e}"),
            }
            .into_response();
        }
    };

    if !is_simple_select(&query) {
        return ErrorResponse {
            error: "only simple SELECT queries are supported (no aggregation/decode yet)"
                .to_string(),
        }
        .into_response();
    }

    let storage = state.storage.read().await;
    let head_block = storage.head_block();
    let result = match logex_query::execute(&query, &storage, head_block) {
        Ok(r) => r,
        Err(e) => {
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
    let rows: Vec<serde_json::Value> = result.rows.iter().map(log_row_to_json).collect();

    Json(QueryResponse {
        rows,
        total_scanned: result.total_scanned,
        row_count,
    })
    .into_response()
}

/// Convert a LogRow to a JSON object with hex-encoded fields.
fn log_row_to_json(row: &LogRow) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "block_number".into(),
        serde_json::Value::Number(row.block_number.into()),
    );
    obj.insert(
        "block_hash".into(),
        serde_json::Value::String(format!("0x{}", hex::encode(row.block_hash))),
    );
    obj.insert(
        "timestamp".into(),
        serde_json::Value::Number(row.timestamp.into()),
    );
    obj.insert(
        "tx_hash".into(),
        serde_json::Value::String(format!("0x{}", hex::encode(row.tx_hash))),
    );
    obj.insert(
        "tx_index".into(),
        serde_json::Value::Number(row.tx_index.into()),
    );
    obj.insert(
        "log_index".into(),
        serde_json::Value::Number(row.log_index.into()),
    );
    obj.insert(
        "address".into(),
        serde_json::Value::String(format!("0x{}", hex::encode(row.address))),
    );

    let topics: Vec<serde_json::Value> = [row.topic0, row.topic1, row.topic2, row.topic3]
        .iter()
        .filter_map(|t| t.map(|h| serde_json::Value::String(format!("0x{}", hex::encode(h)))))
        .collect();
    obj.insert("topics".into(), serde_json::Value::Array(topics));

    obj.insert(
        "data".into(),
        serde_json::Value::String(format!("0x{}", hex::encode(&row.data))),
    );
    obj.insert(
        "data_len".into(),
        serde_json::Value::Number(row.data_len.into()),
    );

    serde_json::Value::Object(obj)
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
    let storage = state.storage.read().await;
    Json(serde_json::json!({
        "syncing": sync.syncing,
        "current_block": sync.current_block,
        "target_block": sync.target_block,
        "blocks_per_sec": sync.blocks_per_sec,
        "logs_ingested": sync.logs_ingested,
        "total_rows": storage.total_rows(),
        "sealed_partitions": storage.sealed_count(),
        "head_block": storage.head_block(),
        "eta_seconds": sync.eta_seconds,
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
    use logex_types::{LogRow, Source, SyncStatus};
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
        };
        let mut mgr = PartitionManager::open(config).unwrap();
        mgr.write_batch(&make_test_rows()).unwrap();
        IndexBuilder::build_all_indexes(&mgr.hot_partition().meta.path).unwrap();
        (tmp, mgr)
    }

    #[tokio::test]
    async fn test_post_query() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState {
            storage: Arc::new(tokio::sync::RwLock::new(storage)),
            subscriptions: None,
            sync_status: Arc::new(std::sync::Mutex::new(SyncStatus::default())),
        });
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
        let state = Arc::new(AppState {
            storage: Arc::new(tokio::sync::RwLock::new(storage)),
            subscriptions: None,
            sync_status: Arc::new(std::sync::Mutex::new(SyncStatus::default())),
        });
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
    async fn test_post_query_parse_error() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState {
            storage: Arc::new(tokio::sync::RwLock::new(storage)),
            subscriptions: None,
            sync_status: Arc::new(std::sync::Mutex::new(SyncStatus::default())),
        });
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
    async fn test_health_endpoint() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState {
            storage: Arc::new(tokio::sync::RwLock::new(storage)),
            subscriptions: None,
            sync_status: Arc::new(std::sync::Mutex::new(SyncStatus::default())),
        });
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
}
