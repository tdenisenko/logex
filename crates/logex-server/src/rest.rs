use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};

use logex_query::{self, SqlQueryError, SqlQueryPage};
use logex_storage::PartitionManager;
use serde::Serialize;

use crate::handler::AppState;
use crate::storage_metrics;

const HISTORICAL_RATE_STALE_AFTER_MS: u64 = 10_000;
const HISTORICAL_RATE_DECAY_HALF_LIFE_MS: f64 = 5_000.0;
const HISTORICAL_RATE_ZERO_THRESHOLD: f64 = 0.01;
const HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK: u64 = 25_093_066;
const HISTORICAL_LOG_ESTIMATE_REFERENCE_TOTAL: f64 = 6_780_563_686.0;
const HISTORICAL_LOG_ESTIMATE_RECENT_LOGS_PER_BLOCK: f64 = 733.0;

/// Request body for POST /query.
#[derive(serde::Deserialize)]
pub struct QueryRequest {
    /// SQL query string.
    pub sql: String,
    /// Optional transport page limit. When omitted, the SQL result set is returned without
    /// an extra server-side row cap.
    #[serde(default)]
    pub limit: Option<usize>,
    /// Zero-based row offset for pagination.
    #[serde(default)]
    pub offset: usize,
}

/// Response for a successful query.
#[derive(Serialize, serde::Deserialize)]
pub struct QueryResponse {
    pub rows: Vec<serde_json::Value>,
    pub total_scanned: u64,
    pub row_count: usize,
    /// Requested transport page limit. `0` means no transport limit was applied.
    pub limit: usize,
    pub offset: usize,
    pub next_offset: Option<usize>,
    /// Maximum accepted transport page limit. `0` means unlimited.
    pub max_limit: usize,
}

#[derive(Serialize, serde::Deserialize)]
pub struct QueryCancelResponse {
    pub canceled: bool,
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
    let Some(query_guard) = state.query_control.start() else {
        return (
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "another SQL query is already running; stop it before starting a new one"
                    .to_owned(),
            }),
        )
            .into_response();
    };
    let cancel_check = query_guard.cancel_check();
    let (storage_snapshot, head_block) = {
        let storage = state.storage.read().await;
        (
            logex_query::NativeStorageSnapshot::from_storage(&storage),
            storage.head_block().unwrap_or(0),
        )
    };
    let requested_limit = req.limit;
    let page = SqlQueryPage::new(requested_limit, req.offset);
    let result = match logex_query::execute_sql_page_on_snapshot(
        &req.sql,
        storage_snapshot,
        head_block,
        page,
        Some(cancel_check),
    )
    .await
    {
        Ok(r) => r,
        Err(SqlQueryError::DataFusion(e))
            if query_guard.was_canceled() || e.to_string().contains("query canceled") =>
        {
            return (
                StatusCode::CONFLICT,
                Json(ErrorResponse {
                    error: "query canceled".to_owned(),
                }),
            )
                .into_response();
        }
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
    let next_offset = requested_limit
        .filter(|limit| *limit > 0 && row_count == *limit)
        .map(|_| req.offset + row_count);

    Json(QueryResponse {
        rows: result.rows,
        total_scanned: result.total_scanned,
        row_count,
        limit: requested_limit.unwrap_or(0),
        offset: req.offset,
        next_offset,
        max_limit: 0,
    })
    .into_response()
}

/// Handle POST /query/cancel — request cancellation for the active SQL query.
pub async fn handle_query_cancel(State(state): State<Arc<AppState>>) -> Json<QueryCancelResponse> {
    Json(QueryCancelResponse {
        canceled: state.query_control.cancel_active(),
    })
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
        historical_floor,
        historical_anchor,
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
            storage.historical_floor(),
            storage.historical_anchor(),
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
    let historical_sync_disabled = sync.historical_sync_disabled;
    let historical_floor = historical_floor.or(sync.historical_execution_floor);
    let historical_anchor = historical_anchor.or(sync.historical_execution_anchor);
    let historical_incomplete = !historical_sync_disabled
        && historical_floor.is_some_and(|floor| floor.block_number > sync.historical_target_block);
    let logs_per_sec =
        effective_historical_rate(sync.logs_per_sec, sync.logs_rate_updated_at_unix_ms);
    let historical_blocks_per_sec = if historical_incomplete {
        effective_historical_rate(
            sync.historical_blocks_per_sec,
            sync.historical_rate_updated_at_unix_ms,
        )
    } else {
        0.0
    };
    let historical_logs_per_sec = if historical_incomplete {
        effective_historical_rate(
            sync.historical_logs_per_sec,
            sync.historical_rate_updated_at_unix_ms,
        )
    } else {
        0.0
    };
    let historical_log_estimate_top_block = (!historical_sync_disabled)
        .then(|| {
            historical_anchor
                .map(|anchor| anchor.block_number)
                .or(canonical_top_block)
        })
        .flatten();
    let historical_total_logs_estimate =
        estimated_total_logs_through_block(historical_log_estimate_top_block);
    let historical_remaining_logs_estimate = (!historical_sync_disabled)
        .then(|| {
            historical_total_logs_estimate.map(|estimated| (estimated - total_rows as f64).max(0.0))
        })
        .flatten();
    let historical_eta_seconds = if historical_sync_disabled {
        None
    } else {
        rest_historical_log_eta(historical_remaining_logs_estimate, historical_logs_per_sec)
            .or_else(|| {
                rest_historical_eta(
                    historical_floor,
                    sync.historical_target_block,
                    historical_blocks_per_sec,
                )
            })
    };
    let verified_from_block = historical_floor
        .map(|floor| floor.block_number)
        .or(stored_log_range.map(|range| range.0));
    let verified_to_block = head_block.or(canonical_top_block);
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
        "logs_per_sec": logs_per_sec,
        "logs_rate_updated_at_unix_ms": sync.logs_rate_updated_at_unix_ms,
        "logs_ingested": sync.logs_ingested,
        "total_rows": total_rows,
        "sealed_partitions": sealed_partitions,
        "head_block": head_block,
        "head_timestamp": head_timestamp,
        "indexed_head_block": indexed_head_block,
        "query_coverage": {
            "stored_log_from_block": stored_log_range.map(|range| range.0),
            "stored_log_to_block": stored_log_range.map(|range| range.1),
            "verified_from_block": verified_from_block,
            "verified_to_block": verified_to_block,
            "latest_block": head_block,
            "latest_timestamp": head_timestamp,
            "indexed_head_block": indexed_head_block,
            "stored_rows": total_rows,
        },
        "storage_used_bytes": storage_metrics.storage_used_bytes,
        "disk_free_bytes": storage_metrics.disk_free_bytes,
        "storage_headroom_bytes": storage_metrics.disk_free_bytes,
        "storage_write_free_bytes": storage_metrics.storage_write_free_bytes,
        "storage_write_path": storage_metrics.storage_write_path,
        "storage_free_total_bytes": storage_metrics.storage_free_total_bytes,
        "storage_free_volumes": storage_metrics.storage_free_volumes,
        "storage_limiting_path": storage_metrics.storage_limiting_path,
        "cpu_utilization_pct": storage_metrics.cpu_utilization_pct,
        "cpu_utilization_raw_pct": storage_metrics.cpu_utilization_raw_pct,
        "cpu_logical_cores": storage_metrics.cpu_logical_cores,
        "eta_seconds": sync.eta_seconds,
        "historical_sync_disabled": historical_sync_disabled,
        "historical_execution_floor": historical_floor,
        "historical_execution_anchor": historical_anchor,
        "historical_target_block": sync.historical_target_block,
        "execution_merge_block": logex_types::EXECUTION_MERGE_BLOCK,
        "execution_terminal_pow_block": logex_types::EXECUTION_TERMINAL_POW_BLOCK,
        "historical_blocks_per_sec": historical_blocks_per_sec,
        "historical_logs_per_sec": historical_logs_per_sec,
        "historical_total_logs_estimate": historical_total_logs_estimate,
        "historical_remaining_logs_estimate": historical_remaining_logs_estimate,
        "historical_rate_updated_at_unix_ms": sync.historical_rate_updated_at_unix_ms,
        "historical_eta_seconds": historical_eta_seconds,
        "raw_log_segment_backlog": sync.raw_log_segment_backlog,
        "storage_profile_rewrite_backlog": sync.storage_profile_rewrite_backlog,
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
        "execution_network": sync.execution_network,
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

fn rest_historical_eta(
    floor: Option<logex_types::ExecutionBlockMarker>,
    target_block: u64,
    blocks_per_sec: f64,
) -> Option<f64> {
    let floor = floor?;
    if blocks_per_sec <= 0.0 || floor.block_number <= target_block {
        return None;
    }

    Some((floor.block_number - target_block) as f64 / blocks_per_sec)
}

fn rest_historical_log_eta(remaining_logs: Option<f64>, logs_per_sec: f64) -> Option<f64> {
    let remaining_logs = remaining_logs?;
    if logs_per_sec <= 0.0 || remaining_logs <= 0.0 {
        return None;
    }

    Some(remaining_logs / logs_per_sec)
}

fn estimated_total_logs_through_block(block_number: Option<u64>) -> Option<f64> {
    let block_number = block_number?;
    if block_number == 0 {
        return Some(0.0);
    }

    if block_number == HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK {
        return Some(HISTORICAL_LOG_ESTIMATE_REFERENCE_TOTAL);
    }

    if block_number < HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK {
        let reference_average = HISTORICAL_LOG_ESTIMATE_REFERENCE_TOTAL
            / HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK as f64;
        return Some(reference_average * block_number as f64);
    }

    Some(
        HISTORICAL_LOG_ESTIMATE_REFERENCE_TOTAL
            + block_number.saturating_sub(HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK) as f64
                * HISTORICAL_LOG_ESTIMATE_RECENT_LOGS_PER_BLOCK,
    )
}

fn effective_historical_rate(rate: f64, updated_at_unix_ms: Option<u64>) -> f64 {
    if rate <= 0.0 {
        return 0.0;
    }

    let Some(updated_at_unix_ms) = updated_at_unix_ms else {
        return 0.0;
    };

    let now = unix_time_millis();
    historical_rate_for_age(rate, now.saturating_sub(updated_at_unix_ms))
}

fn historical_rate_for_age(rate: f64, age_ms: u64) -> f64 {
    if rate <= 0.0 {
        return 0.0;
    }
    if age_ms <= HISTORICAL_RATE_STALE_AFTER_MS {
        return rate;
    }

    let stale_ms = age_ms - HISTORICAL_RATE_STALE_AFTER_MS;
    let decayed = rate * 0.5_f64.powf(stale_ms as f64 / HISTORICAL_RATE_DECAY_HALF_LIFE_MS);
    if decayed < HISTORICAL_RATE_ZERO_THRESHOLD {
        0.0
    } else {
        decayed
    }
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

/// Handle GET / — serve the embedded web UI.
pub async fn handle_web_ui() -> Html<&'static str> {
    Html(include_str!("web_ui.html"))
}

/// Handle GET / when the dashboard has been explicitly disabled.
pub async fn handle_dashboard_disabled() -> Response {
    (StatusCode::NOT_FOUND, "dashboard disabled").into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use axum::body::Body;
    use axum::http::Request;
    use base64::Engine;
    use logex_index::IndexBuilder;
    use logex_query::DEFAULT_QUERY_PAGE_SIZE;
    use logex_storage::{PartitionManager, PartitionManagerConfig};
    use logex_types::{
        ConsensusDataFork, ConsensusLightClientStatus, ConsensusNetworkStatus, ExecutionAnchor,
        ExecutionBlockMarker, ExecutionNetworkStatus, LightClientBootstrapStatus,
        LightClientExecutionData, LightClientHeaderSummary, LogRow, NodeState, Source, SyncStatus,
        WeakSubjectivityCheckpoint,
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

    fn make_many_test_rows(count: usize) -> Vec<LogRow> {
        (0..count)
            .map(|index| LogRow {
                block_number: index as u64,
                block_hash: B256::repeat_byte((index % 251 + 1) as u8),
                timestamp: 1_700_000_000 + index as u64,
                tx_hash: B256::repeat_byte((index % 253 + 1) as u8),
                tx_index: 0,
                log_index: index as u32,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            })
            .collect()
    }

    fn setup_storage_with_rows(rows: &[LogRow]) -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        };
        let mut mgr = PartitionManager::open(config).unwrap();
        mgr.write_batch(rows).unwrap();
        IndexBuilder::build_all_indexes(&mgr.hot_partition().meta.path).unwrap();
        (tmp, mgr)
    }

    fn setup_storage() -> (TempDir, PartitionManager) {
        setup_storage_with_rows(&make_test_rows())
    }

    fn basic_auth_header(password: &str) -> String {
        let credentials = format!("logex:{password}");
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(credentials)
        )
    }

    #[test]
    fn historical_rate_decays_after_stale_progress() {
        assert_eq!(historical_rate_for_age(500.0, 9_999), 500.0);

        let decayed = historical_rate_for_age(500.0, 20_000);
        assert!(decayed > 0.0);
        assert!(decayed < 500.0);

        assert_eq!(historical_rate_for_age(500.0, 100_000), 0.0);
    }

    #[test]
    fn historical_log_estimate_extends_reference_with_recent_density() {
        assert_eq!(
            estimated_total_logs_through_block(Some(HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK)),
            Some(HISTORICAL_LOG_ESTIMATE_REFERENCE_TOTAL)
        );
        assert_eq!(
            estimated_total_logs_through_block(Some(HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK + 10)),
            Some(HISTORICAL_LOG_ESTIMATE_REFERENCE_TOTAL + 7_330.0)
        );
        assert_eq!(estimated_total_logs_through_block(Some(0)), Some(0.0));
    }

    #[tokio::test]
    async fn test_status_endpoint_decays_stale_historical_rate() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(
            storage,
            None,
            SyncStatus {
                historical_execution_floor: Some(ExecutionBlockMarker {
                    block_number: 1_000,
                    block_hash: B256::repeat_byte(0x11),
                    timestamp: 1_700_000_000,
                }),
                historical_target_block: 0,
                historical_blocks_per_sec: 500.0,
                historical_rate_updated_at_unix_ms: Some(unix_time_millis().saturating_sub(60_000)),
                historical_eta_seconds: Some(2.0),
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
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let rate = status["historical_blocks_per_sec"].as_f64().unwrap();
        assert!(rate < 1.0);
        assert!(status["historical_eta_seconds"].as_f64().unwrap() > 1_000.0);
    }

    #[tokio::test]
    async fn test_status_endpoint_reports_log_based_historical_eta() {
        let (_tmp, storage) = setup_storage();
        let top_block = HISTORICAL_LOG_ESTIMATE_REFERENCE_BLOCK + 10;
        let estimated_total = HISTORICAL_LOG_ESTIMATE_REFERENCE_TOTAL + 7_330.0;
        let state = Arc::new(AppState::new(
            storage,
            None,
            SyncStatus {
                historical_execution_floor: Some(ExecutionBlockMarker {
                    block_number: top_block,
                    block_hash: B256::repeat_byte(0x11),
                    timestamp: 1_700_000_000,
                }),
                historical_execution_anchor: Some(ExecutionBlockMarker {
                    block_number: top_block,
                    block_hash: B256::repeat_byte(0x22),
                    timestamp: 1_700_000_000,
                }),
                historical_target_block: 0,
                historical_blocks_per_sec: 1_000.0,
                historical_logs_per_sec: 7_330.0,
                historical_rate_updated_at_unix_ms: Some(unix_time_millis()),
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
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["historical_logs_per_sec"], 7_330.0);
        assert_eq!(status["historical_total_logs_estimate"], estimated_total);
        assert_eq!(
            status["historical_remaining_logs_estimate"],
            estimated_total - 2.0
        );

        let eta = status["historical_eta_seconds"].as_f64().unwrap();
        assert!((eta - ((estimated_total - 2.0) / 7_330.0)).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_status_endpoint_suppresses_historical_eta_when_disabled() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(
            storage,
            None,
            SyncStatus {
                historical_sync_disabled: true,
                historical_execution_floor: Some(ExecutionBlockMarker {
                    block_number: 1_000,
                    block_hash: B256::repeat_byte(0x11),
                    timestamp: 1_700_000_000,
                }),
                historical_target_block: 0,
                historical_blocks_per_sec: 500.0,
                historical_logs_per_sec: 7_330.0,
                historical_rate_updated_at_unix_ms: Some(unix_time_millis()),
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
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let status: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(status["historical_sync_disabled"], true);
        assert_eq!(status["historical_blocks_per_sec"], 0.0);
        assert_eq!(status["historical_logs_per_sec"], 0.0);
        assert!(status["historical_eta_seconds"].is_null());
        assert!(status["historical_remaining_logs_estimate"].is_null());
    }

    #[tokio::test]
    async fn test_dashboard_can_be_disabled() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router_with_config(
            state,
            crate::HttpServerConfig {
                dashboard_enabled: false,
                dashboard_password: None,
            },
        );

        let req = Request::builder()
            .method("GET")
            .uri("/")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn test_dashboard_password_protects_status_and_query_routes() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router_with_config(
            state,
            crate::HttpServerConfig {
                dashboard_enabled: true,
                dashboard_password: Some("secret".to_owned()),
            },
        );

        let status_req = Request::builder()
            .method("GET")
            .uri("/status")
            .body(Body::empty())
            .unwrap();
        let status_resp = app.clone().oneshot(status_req).await.unwrap();
        assert_eq!(status_resp.status(), StatusCode::UNAUTHORIZED);
        assert!(status_resp.headers().contains_key("www-authenticate"));

        let health_req = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let health_resp = app.clone().oneshot(health_req).await.unwrap();
        assert_eq!(health_resp.status(), StatusCode::OK);

        let authed_status_req = Request::builder()
            .method("GET")
            .uri("/status")
            .header("authorization", basic_auth_header("secret"))
            .body(Body::empty())
            .unwrap();
        let authed_status_resp = app.clone().oneshot(authed_status_req).await.unwrap();
        assert_eq!(authed_status_resp.status(), StatusCode::OK);

        let query_body = serde_json::json!({ "sql": "SELECT * FROM logs" });
        let query_req = Request::builder()
            .method("POST")
            .uri("/query")
            .header("authorization", basic_auth_header("secret"))
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_string(&query_body).unwrap()))
            .unwrap();
        let query_resp = app.oneshot(query_req).await.unwrap();
        assert_eq!(query_resp.status(), StatusCode::OK);
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
    async fn test_query_cancel_endpoint_reports_active_query() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let active_query = state.query_control.start().unwrap();
        let app = crate::build_router(state);

        let req = Request::builder()
            .method("POST")
            .uri("/query/cancel")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let result: QueryCancelResponse = serde_json::from_slice(&body).unwrap();
        assert!(result.canceled);
        assert!(active_query.was_canceled());
    }

    #[tokio::test]
    async fn test_post_query_without_transport_limit_returns_all_rows() {
        let rows = make_many_test_rows(DEFAULT_QUERY_PAGE_SIZE + 25);
        let (_tmp, storage) = setup_storage_with_rows(&rows);
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({
            "sql": "SELECT block_number FROM logs ORDER BY block_number ASC"
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
        assert_eq!(result.row_count, DEFAULT_QUERY_PAGE_SIZE + 25);
        assert_eq!(result.rows.len(), DEFAULT_QUERY_PAGE_SIZE + 25);
        assert_eq!(result.limit, 0);
        assert_eq!(result.max_limit, 0);
        assert_eq!(result.next_offset, None);
    }

    #[tokio::test]
    async fn test_post_query_explicit_transport_limit_pages_results() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let app = crate::build_router(state);

        let body = serde_json::json!({
            "sql": "SELECT block_number FROM logs ORDER BY block_number ASC",
            "limit": 1
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
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.limit, 1);
        assert_eq!(result.next_offset, Some(1));
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
                logs_per_sec: 7.5,
                logs_rate_updated_at_unix_ms: Some(unix_time_millis()),
                logs_ingested: 42,
                eta_seconds: Some(125.0),
                historical_sync_disabled: false,
                historical_execution_floor: None,
                historical_execution_anchor: None,
                historical_target_block: logex_types::EXECUTION_HISTORY_TARGET_BLOCK,
                historical_blocks_per_sec: 0.0,
                historical_logs_per_sec: 0.0,
                historical_rate_updated_at_unix_ms: None,
                historical_eta_seconds: None,
                raw_log_segment_backlog: Some(2),
                storage_profile_rewrite_backlog: Some(5),
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
                execution_network: Some(ExecutionNetworkStatus {
                    max_peers: 100,
                    accepted_sessions: 17,
                    rejected_zero_tip_sessions: 2,
                    disconnected_sessions: 5,
                    saturated_disconnects: 1,
                    nonserving_disconnects: 3,
                    missing_fork_id_candidates: 4,
                    fork_id_rejected_candidates: 6,
                    queued_candidates: 9,
                    pending_dials: 3,
                    productive_peers: 7,
                    known_peers: 11,
                    saturated_peers: 2,
                    receipt_quarantined_peers: 1,
                    body_request_ready_peers: 4,
                    receipt_request_ready_peers: 5,
                    body_request_paused_peers: 6,
                    receipt_request_paused_peers: 7,
                    active_body_requests: 8,
                    active_receipt_requests: 9,
                    timeout_penalized_peers: 10,
                    body_proven_peers: 11,
                    receipt_proven_peers: 12,
                    body_request_limit_avg: 13,
                    receipt_request_limit_avg: 14,
                    historical_fetch_active: 0,
                    historical_fetch_ready: 0,
                    historical_fetch_completed: 0,
                    historical_fetch_pending: 0,
                    historical_fetch_expected_sequence: 0,
                    historical_fetch_next_sequence: 0,
                    historical_fetch_head_of_line_blocked: false,
                    historical_fetch_head_of_line_completed: 0,
                    historical_fetch_expected_active: false,
                    historical_fetch_head_of_line_elapsed_ms: None,
                    historical_prepare_active: 0,
                    historical_prepare_ready: 0,
                    historical_prepare_completed: 0,
                    historical_prepare_pending: 0,
                    historical_prepare_expected_sequence: 0,
                    historical_ingest_active: false,
                    historical_ingest_sequence: None,
                    historical_ingest_elapsed_ms: None,
                    historical_scheduler_body_slot_margin: 0,
                    historical_scheduler_receipt_slot_margin: 0,
                    historical_scheduler_write_backpressure: false,
                    historical_scheduler_pipeline_depth: 0,
                    historical_scheduler_buffer_depth: 0,
                    historical_scheduler_critical_refill_limit: 0,
                    historical_scheduler_write_refill_limit: 0,
                    historical_scheduler_stale_role_retries: 0,
                    historical_scheduler_prefix_reassignments: 0,
                    historical_scheduler_body_successes: 0,
                    historical_scheduler_receipt_successes: 0,
                    historical_scheduler_body_failures: 0,
                    historical_scheduler_receipt_failures: 0,
                    historical_scheduler_body_blocks: 0,
                    historical_scheduler_receipt_blocks: 0,
                    connected_geth_peers: 2,
                    connected_nethermind_peers: 3,
                    connected_reth_peers: 1,
                    connected_other_peers: 0,
                    serving_geth_peers: 1,
                    serving_nethermind_peers: 2,
                    serving_reth_peers: 1,
                    serving_other_peers: 0,
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
        let data_dir = {
            let storage = state.storage.read().await;
            storage.data_dir().to_path_buf()
        };
        storage_metrics::refresh_for_test(Arc::clone(&state.storage_metrics), data_dir).await;
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
        assert_eq!(
            status["execution_merge_block"],
            logex_types::EXECUTION_MERGE_BLOCK
        );
        assert_eq!(
            status["execution_terminal_pow_block"],
            logex_types::EXECUTION_TERMINAL_POW_BLOCK
        );
        assert_eq!(status["blocks_per_minute"], 120.0);
        assert_eq!(status["logs_per_sec"], 7.5);
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
        assert_eq!(status["execution_network"]["queued_candidates"], 9);
        assert_eq!(status["execution_network"]["pending_dials"], 3);
        assert_eq!(status["execution_network"]["productive_peers"], 7);
        assert_eq!(status["execution_network"]["accepted_sessions"], 17);
        assert_eq!(status["execution_network"]["rejected_zero_tip_sessions"], 2);
        assert_eq!(status["execution_network"]["disconnected_sessions"], 5);
        assert_eq!(status["execution_network"]["saturated_disconnects"], 1);
        assert_eq!(status["execution_network"]["nonserving_disconnects"], 3);
        assert_eq!(status["execution_network"]["missing_fork_id_candidates"], 4);
        assert_eq!(
            status["execution_network"]["fork_id_rejected_candidates"],
            6
        );
        assert_eq!(status["execution_network"]["connected_geth_peers"], 2);
        assert_eq!(status["execution_network"]["connected_nethermind_peers"], 3);
        assert_eq!(status["execution_network"]["connected_reth_peers"], 1);
        assert_eq!(status["execution_network"]["serving_nethermind_peers"], 2);
        assert_eq!(status["execution_network"]["body_request_ready_peers"], 4);
        assert_eq!(
            status["execution_network"]["receipt_request_ready_peers"],
            5
        );
        assert_eq!(status["execution_network"]["body_request_paused_peers"], 6);
        assert_eq!(
            status["execution_network"]["receipt_request_paused_peers"],
            7
        );
        assert_eq!(status["execution_network"]["active_body_requests"], 8);
        assert_eq!(status["execution_network"]["active_receipt_requests"], 9);
        assert_eq!(status["execution_network"]["timeout_penalized_peers"], 10);
        assert_eq!(status["execution_network"]["body_proven_peers"], 11);
        assert_eq!(status["execution_network"]["receipt_proven_peers"], 12);
        assert_eq!(status["execution_network"]["body_request_limit_avg"], 13);
        assert_eq!(status["execution_network"]["receipt_request_limit_avg"], 14);
        assert_eq!(status["raw_log_segment_backlog"], 2);
        assert_eq!(status["storage_profile_rewrite_backlog"], 5);
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
        assert_eq!(status["storage_headroom_bytes"], status["disk_free_bytes"]);
        assert!(status["storage_write_free_bytes"].as_u64().unwrap_or(0) > 0);
        assert!(
            status["storage_write_path"]
                .as_str()
                .is_some_and(|path| !path.is_empty())
        );
        assert!(
            status["storage_limiting_path"]
                .as_str()
                .is_some_and(|path| !path.is_empty())
        );
        assert!(status["storage_free_total_bytes"].as_u64().unwrap_or(0) > 0);
        assert!(
            status["storage_free_volumes"]
                .as_array()
                .is_some_and(|volumes| {
                    !volumes.is_empty()
                        && volumes.iter().all(|volume| {
                            volume["path"].is_string() && volume["free_bytes"].is_u64()
                        })
                })
        );
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
