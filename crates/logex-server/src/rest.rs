use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};

use logex_query::{self, SqlQueryError};
use logex_storage::PartitionManager;
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
    let (
        total_rows,
        sealed_partitions,
        head_block,
        head_timestamp,
        indexed_head_block,
        stored_log_range,
        chain_anchors,
        data_dir,
    ) = {
        let storage = state.storage.read().await;
        let sync_head = storage.sync_head();
        (
            storage.total_rows(),
            storage.sealed_count(),
            storage.head_block(),
            sync_head.and_then(|head| (head.timestamp > 0).then_some(head.timestamp)),
            storage.indexed_head_block(),
            stored_log_range(&storage),
            storage.chain_anchors(),
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
    let canonical_top_block = sync
        .optimistic_execution_head
        .map(|anchor| anchor.block_number)
        .or((sync.target_block > 0).then_some(sync.target_block))
        .or(head_block);
    let index_lag_blocks = canonical_top_block
        .zip(
            sync.indexed_execution_head
                .map(|anchor| anchor.block_number),
        )
        .map(|(top, indexed)| top.saturating_sub(indexed));
    let finality_lag_blocks = canonical_top_block
        .zip(
            sync.finalized_execution_head
                .map(|anchor| anchor.block_number),
        )
        .map(|(top, finalized)| top.saturating_sub(finalized));
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
        "head_timestamp": head_timestamp,
        "indexed_head_block": indexed_head_block,
        "query_coverage": {
            "stored_log_from_block": stored_log_range.map(|range| range.0),
            "stored_log_to_block": stored_log_range.map(|range| range.1),
            "latest_block": head_block,
            "latest_timestamp": head_timestamp,
            "indexed_head_block": indexed_head_block,
            "stored_rows": total_rows,
        },
        "storage_used_bytes": storage_metrics.storage_used_bytes,
        "disk_free_bytes": storage_metrics.disk_free_bytes,
        "eta_seconds": sync.eta_seconds,
        "progress_pct": progress_pct,
        "canonical_top_block": canonical_top_block,
        "checkpoint_root": sync.checkpoint.map(|checkpoint| checkpoint.beacon_root),
        "checkpoint_slot": sync.checkpoint.and_then(|checkpoint| checkpoint.beacon_slot),
        "indexed_execution_head": sync.indexed_execution_head,
        "materialized_execution_floor": sync.materialized_execution_floor,
        "materialized_execution_ceiling": sync.materialized_execution_ceiling,
        "materialized_execution_anchor_count": sync.materialized_execution_anchor_count,
        "materialized_execution_anchor_gap_count": sync.materialized_execution_anchor_gap_count,
        "optimistic_execution_head": sync.optimistic_execution_head,
        "finalized_execution_head": sync.finalized_execution_head,
        "consensus_network": sync.consensus_network,
        "consensus_light_client": sync.consensus_light_client,
        "index_lag_blocks": index_lag_blocks,
        "finality_lag_blocks": finality_lag_blocks,
        "storage_chain_anchors": chain_anchors,
    }))
}

fn stored_log_range(storage: &PartitionManager) -> Option<(u64, u64)> {
    storage
        .sealed_partitions()
        .iter()
        .map(|partition| &partition.meta)
        .chain(std::iter::once(&storage.hot_partition().meta))
        .filter(|meta| meta.row_count > 0 && meta.min_block <= meta.max_block)
        .fold(None, |range, meta| match range {
            Some((min, max)) => Some((min.min(meta.min_block), max.max(meta.max_block))),
            None => Some((meta.min_block, meta.max_block)),
        })
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
    use logex_types::{
        ConsensusDataFork, ConsensusLightClientStatus, ConsensusNetworkStatus, ExecutionAnchor,
        LightClientBootstrapStatus, LightClientExecutionData, LightClientHeaderSummary, LogRow,
        NodeState, Source, SyncStatus, WeakSubjectivityCheckpoint,
    };
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
    async fn test_post_query_missing_projection_defaults_to_full_rows() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({
            "sql": "select from logs where block_number = 100"
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
        assert_eq!(result.rows[0]["block_number"], 100);
        assert!(result.rows[0].get("tx_hash").is_some());
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
                checkpoint: Some(WeakSubjectivityCheckpoint {
                    beacon_root: B256::repeat_byte(0x77),
                    beacon_slot: Some(123_456),
                }),
                indexed_execution_head: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x01),
                    beacon_slot: 1,
                    block_number: 200,
                    block_hash: B256::repeat_byte(0x02),
                    receipts_root: B256::repeat_byte(0x03),
                }),
                materialized_execution_floor: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x0A),
                    beacon_slot: 1,
                    block_number: 150,
                    block_hash: B256::repeat_byte(0x0B),
                    receipts_root: B256::repeat_byte(0x0C),
                }),
                materialized_execution_ceiling: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x0D),
                    beacon_slot: 2,
                    block_number: 460,
                    block_hash: B256::repeat_byte(0x0E),
                    receipts_root: B256::repeat_byte(0x0F),
                }),
                materialized_execution_anchor_count: 311,
                materialized_execution_anchor_gap_count: 0,
                optimistic_execution_head: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x04),
                    beacon_slot: 2,
                    block_number: 500,
                    block_hash: B256::repeat_byte(0x05),
                    receipts_root: B256::repeat_byte(0x06),
                }),
                finalized_execution_head: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x07),
                    beacon_slot: 3,
                    block_number: 480,
                    block_hash: B256::repeat_byte(0x08),
                    receipts_root: B256::repeat_byte(0x09),
                }),
                consensus_network: Some(ConsensusNetworkStatus {
                    local_enr: Some("enr:test".to_string()),
                    local_node_id: Some("node:test".to_string()),
                    discovery_port: 9_000,
                    p2p_port: 9_000,
                    local_peer_id: Some("12D3KooWtest".to_string()),
                    max_peers: 32,
                    bootnode_count: 14,
                    discovered_peers: 21,
                    dialable_peers: 13,
                    routing_table_peers: 11,
                    active_sessions: 3,
                    connected_peer_sessions: 2,
                    dialing_peer_sessions: 1,
                    preferred_peers: 3,
                    cooldown_peers: 5,
                    ignored_peers: 7,
                    deferred_until_post_bootstrap_peers: 2,
                    identified_peers: 2,
                    status_capable_peers: 2,
                    metadata_capable_peers: 2,
                    bootstrap_capable_peers: 1,
                    updates_by_range_capable_peers: 1,
                    finality_update_capable_peers: 1,
                    optimistic_update_capable_peers: 1,
                    beacon_blocks_by_range_capable_peers: 1,
                    beacon_blocks_by_root_capable_peers: 1,
                    status_peers: 2,
                    metadata_peers: 2,
                    bootstrap_peers: 1,
                    updates_by_range_peers: 1,
                    finality_update_peers: 1,
                    optimistic_update_peers: 1,
                    beacon_blocks_by_range_peers: 1,
                    beacon_blocks_by_root_peers: 1,
                    pending_rpc_requests: 4,
                    pending_status_requests: 1,
                    pending_metadata_requests: 1,
                    pending_bootstrap_requests: 1,
                    pending_updates_by_range_requests: 0,
                    pending_finality_update_requests: 1,
                    pending_optimistic_update_requests: 0,
                    pending_beacon_blocks_by_range_requests: 0,
                    pending_forward_beacon_blocks_by_range_requests: 0,
                    pending_beacon_blocks_by_root_requests: 0,
                    status_request_failures: 3,
                    metadata_request_failures: 1,
                    bootstrap_request_failures: 0,
                    updates_by_range_request_failures: 0,
                    finality_update_request_failures: 0,
                    optimistic_update_request_failures: 0,
                    beacon_blocks_by_range_request_failures: 0,
                    beacon_blocks_by_root_request_failures: 0,
                    gossip_subscriptions: 2,
                    finality_update_gossip_messages: 4,
                    optimistic_update_gossip_messages: 9,
                    gossip_decode_failures: 1,
                    last_connection_event: Some("connected peer=peer1 endpoint=Dialer".to_string()),
                    last_identify_event: Some(
                        "peer=peer1 agent=lighthouse protocols=12 status=true metadata=true bootstrap=false updates_by_range=false finality=false optimistic=false blocks_by_range=true blocks_by_root=true preview=[/eth2/beacon_chain/req/status/2/ssz_snappy]".to_string(),
                    ),
                    last_peer_policy_event: Some(
                        "peer=peer3 policy=defer_until_post_bootstrap reason=peer only advertises post-bootstrap work".to_string(),
                    ),
                    last_rpc_failure: Some(
                        "peer=peer1 request=status failure=connection closed".to_string(),
                    ),
                    last_response_send_failure: Some(
                        "peer=peer2 request=status response=Status(..)".to_string(),
                    ),
                }),
                consensus_light_client: Some(ConsensusLightClientStatus {
                    bootstrap: Some(LightClientBootstrapStatus {
                        fork: ConsensusDataFork::Electra,
                        header: LightClientHeaderSummary {
                            beacon_slot: 123_450,
                            execution: Some(LightClientExecutionData {
                                block_number: 500,
                                block_hash: B256::repeat_byte(0xAA),
                                receipts_root: B256::repeat_byte(0xBB),
                            }),
                        },
                        current_sync_committee_pubkeys: 512,
                        current_sync_committee_branch_depth: 6,
                    }),
                    finality_update: None,
                    optimistic_update: None,
                }),
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
        assert_eq!(status["head_timestamp"], 1_650_000_000);
        assert_eq!(status["indexed_head_block"], 200);
        assert_eq!(status["query_coverage"]["stored_log_from_block"], 100);
        assert_eq!(status["query_coverage"]["stored_log_to_block"], 200);
        assert_eq!(status["query_coverage"]["latest_block"], 250);
        assert_eq!(status["query_coverage"]["latest_timestamp"], 1_650_000_000);
        assert_eq!(status["query_coverage"]["indexed_head_block"], 200);
        assert_eq!(status["query_coverage"]["stored_rows"], 2);
        assert_eq!(status["blocks_per_minute"], 120.0);
        assert_eq!(status["logs_ingested"], 42);
        assert_eq!(status["node_state"], "reconnecting");
        assert_eq!(status["canonical_top_block"], 500);
        assert_eq!(status["index_lag_blocks"], 300);
        assert_eq!(status["finality_lag_blocks"], 20);
        assert_eq!(status["materialized_execution_floor"]["block_number"], 150);
        assert_eq!(
            status["materialized_execution_ceiling"]["block_number"],
            460
        );
        assert_eq!(status["materialized_execution_anchor_count"], 311);
        assert_eq!(status["materialized_execution_anchor_gap_count"], 0);
        assert_eq!(status["connected_peers"], 0);
        assert_eq!(status["serving_peers"], 0);
        assert_eq!(status["pending_peers"], 12);
        assert_eq!(status["consensus_network"]["active_sessions"], 3);
        assert_eq!(status["consensus_network"]["dialable_peers"], 13);
        assert_eq!(
            status["consensus_light_client"]["bootstrap"]["fork"],
            "electra"
        );
        assert_eq!(
            status["consensus_light_client"]["bootstrap"]["header"]["execution"]["block_number"],
            500
        );
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
                ..Default::default()
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
