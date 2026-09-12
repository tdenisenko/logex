use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::State;
use axum::response::Json;

use logex_query::{self, DEFAULT_QUERY_PAGE_SIZE};
use logex_storage::PartitionManager;
use logex_types::{LOGEX_CLIENT_VERSION, SyncStatus};

use crate::eth_filter::{EthFilter, RpcLog};
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};
use crate::storage_metrics::CachedStorageMetrics;

use crate::ws::SubscriptionManager;

pub(crate) const MAX_LOG_FILTER_LIMIT: usize = 10_000;

/// Shared application state.
pub struct AppState {
    pub storage: Arc<tokio::sync::RwLock<PartitionManager>>,
    /// WebSocket subscription manager. None if subscriptions are disabled.
    pub subscriptions: Option<SubscriptionManager>,
    /// Live sync progress, updated by the sync task.
    pub sync_status: Arc<std::sync::Mutex<SyncStatus>>,
    pub(crate) storage_metrics: Arc<tokio::sync::Mutex<CachedStorageMetrics>>,
    pub(crate) query_control: Arc<QueryControl>,
}

impl AppState {
    pub fn new(
        storage: PartitionManager,
        subscriptions: Option<SubscriptionManager>,
        sync_status: SyncStatus,
    ) -> Self {
        Self {
            storage: Arc::new(tokio::sync::RwLock::new(storage)),
            subscriptions,
            sync_status: Arc::new(std::sync::Mutex::new(sync_status)),
            storage_metrics: Arc::new(tokio::sync::Mutex::new(CachedStorageMetrics::default())),
            query_control: Arc::new(QueryControl::default()),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct QueryControl {
    // Admission, cancellation and completion share one linearization point.
    // The query's hot cancellation checks only read its own atomic token.
    // Critical sections only replace owned pointers/read or write atomics; no
    // user code runs under the lock, so recovering its value after poison is safe.
    active: Mutex<Option<Arc<AtomicBool>>>,
}

impl QueryControl {
    pub(crate) fn start(self: &Arc<Self>) -> Option<ActiveQueryGuard> {
        let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
        if active.is_some() {
            return None;
        }
        let canceled = Arc::new(AtomicBool::new(false));
        *active = Some(Arc::clone(&canceled));
        Some(ActiveQueryGuard {
            control: Arc::clone(self),
            canceled,
        })
    }

    pub(crate) fn cancel_active(&self) -> bool {
        let active = self.active.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(canceled) = active.as_ref() {
            canceled.store(true, Ordering::Release);
        }
        active.is_some()
    }
}

pub(crate) struct ActiveQueryGuard {
    control: Arc<QueryControl>,
    canceled: Arc<AtomicBool>,
}

impl ActiveQueryGuard {
    pub(crate) fn cancel_check(&self) -> logex_query::QueryCancelCheck {
        let canceled = Arc::clone(&self.canceled);
        Arc::new(move || canceled.load(Ordering::Acquire))
    }

    pub(crate) fn was_canceled(&self) -> bool {
        self.canceled.load(Ordering::Acquire)
    }
}

impl Drop for ActiveQueryGuard {
    fn drop(&mut self) {
        let mut active = self
            .control
            .active
            .lock()
            .unwrap_or_else(|err| err.into_inner());
        // Retained checks/tasks stay canceled after their request is dropped.
        // This token is never reused or reset for a later request.
        self.canceled.store(true, Ordering::Release);
        // Admission permits exactly one non-cloneable guard until this drop.
        *active = None;
    }
}

/// Handle a JSON-RPC request.
pub async fn handle_jsonrpc(
    State(state): State<Arc<AppState>>,
    Json(request): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    let id = request.id.clone();
    let storage = state.storage.read().await;

    let response = match request.method.as_str() {
        "eth_getLogs" => handle_eth_get_logs(&storage, &request),
        "eth_blockNumber" => handle_eth_block_number(&storage, &request),
        "web3_clientVersion" => Ok(JsonRpcResponse::success(
            id.clone(),
            serde_json::Value::String(LOGEX_CLIENT_VERSION.into()),
        )),
        "net_version" => Ok(JsonRpcResponse::success(
            id.clone(),
            serde_json::Value::String("1".into()),
        )),
        _ => Ok(JsonRpcResponse::method_not_found(id.clone())),
    };

    Json(response.unwrap_or_else(|e: String| JsonRpcResponse::internal_error(id, e)))
}

fn handle_eth_get_logs(
    storage: &PartitionManager,
    req: &JsonRpcRequest,
) -> Result<JsonRpcResponse, String> {
    let id = req.id.clone();
    let params = req
        .params
        .as_ref()
        .and_then(|p| p.as_array())
        .ok_or_else(|| "params must be an array".to_string())?;

    if params.is_empty() {
        return Err("eth_getLogs requires a filter parameter".into());
    }

    let filter: EthFilter =
        serde_json::from_value(params[0].clone()).map_err(|e| format!("invalid filter: {e}"))?;

    if filter.block_hash.is_some() && (filter.from_block.is_some() || filter.to_block.is_some()) {
        return Err("blockHash is mutually exclusive with fromBlock/toBlock".into());
    }

    if let Some(limit) = filter.limit
        && limit > MAX_LOG_FILTER_LIMIT
    {
        return Err(format!("limit must be at most {MAX_LOG_FILTER_LIMIT}"));
    }
    if filter.offset >= MAX_LOG_FILTER_LIMIT {
        return Err(format!("offset must be less than {MAX_LOG_FILTER_LIMIT}"));
    }

    let mut native_filter = filter.to_native_filter(storage.head_block().unwrap_or(0));
    native_filter.limit = Some(
        filter
            .limit
            .unwrap_or(DEFAULT_QUERY_PAGE_SIZE)
            .min(MAX_LOG_FILTER_LIMIT - filter.offset),
    );
    native_filter.offset = filter.offset;
    let rows = logex_query::execute_log_filter(storage, &native_filter)
        .map_err(|error| error.to_string())?;
    let logs: Vec<RpcLog> = rows.iter().map(RpcLog::from).collect();

    let json = serde_json::to_value(&logs).map_err(|e| e.to_string())?;
    Ok(JsonRpcResponse::success(id, json))
}

fn handle_eth_block_number(
    storage: &PartitionManager,
    req: &JsonRpcRequest,
) -> Result<JsonRpcResponse, String> {
    let block = storage.head_block().unwrap_or(0);
    Ok(JsonRpcResponse::success(
        req.id.clone(),
        serde_json::Value::String(format!("0x{block:x}")),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::PartitionManagerConfig;
    use logex_types::{LOGEX_CLIENT_VERSION, LogRow, Source};
    use tempfile::TempDir;

    use crate::eth_filter::{AddressFilter, BlockId};

    #[test]
    fn acknowledged_query_cancellation_survives_concurrent_start() {
        use std::sync::Barrier;

        let control = Arc::new(QueryControl::default());
        let start = Barrier::new(2);
        let canceled = Barrier::new(2);
        let mut lost = 0;
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..20_000 {
                    start.wait();
                    while !control.cancel_active() {
                        std::hint::spin_loop();
                    }
                    canceled.wait();
                }
            });
            for _ in 0..20_000 {
                start.wait();
                let query = control.start().unwrap();
                canceled.wait();
                // Cancellation has returned true, the query still owns its
                // slot, and there is no other client that could finish it.
                if !query.was_canceled() {
                    lost += 1;
                }
                drop(query);
            }
        });
        assert_eq!(lost, 0, "acknowledged cancellations must remain observable");
    }

    #[test]
    fn finishing_previous_query_cannot_clear_new_query_cancellation() {
        use std::sync::Barrier;

        let control = Arc::new(QueryControl::default());
        let ready = Barrier::new(2);
        let finished = Barrier::new(2);
        let next = Barrier::new(2);
        let mut lost = 0;
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..20_000 {
                    let previous = control.start().unwrap();
                    ready.wait();
                    drop(previous);
                    finished.wait();
                    next.wait();
                }
            });
            for _ in 0..20_000 {
                ready.wait();
                let query = loop {
                    if let Some(query) = control.start() {
                        break query;
                    }
                    std::hint::spin_loop();
                };
                assert!(control.cancel_active());
                finished.wait();
                if !query.was_canceled() {
                    lost += 1;
                }
                drop(query);
                next.wait();
            }
        });
        assert_eq!(lost, 0, "previous completion must not erase cancellation");
    }

    #[test]
    fn query_cancellation_tokens_are_permanent_and_isolated() {
        let control = Arc::new(QueryControl::default());
        assert!(!control.cancel_active());
        let first = control.start().unwrap();
        let first_check = first.cancel_check();
        assert!(!first_check());
        assert!(control.start().is_none());
        drop(first);
        assert!(first_check(), "abandoned work must stay canceled");
        assert!(!control.cancel_active());

        let second = control.start().unwrap();
        let second_check = second.cancel_check();
        assert!(!second_check());
        assert!(first_check());
        assert!(control.cancel_active());
        assert!(control.cancel_active(), "cancellation is idempotent");
        assert!(second_check());
        assert!(control.start().is_none(), "cancel is not completion");
        drop(second);
        let third = control.start().unwrap();
        assert!(!third.was_canceled());
        assert!(first_check() && second_check());
    }

    #[test]
    fn concurrent_query_admission_has_exactly_one_owner() {
        use std::sync::Barrier;

        let control = Arc::new(QueryControl::default());
        let start = Barrier::new(9);
        let admitted = Barrier::new(9);
        let canceled = Barrier::new(9);
        let owners = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        start.wait();
                        let query = control.start();
                        admitted.wait();
                        canceled.wait();
                        if let Some(query) = query {
                            assert!(query.was_canceled());
                            1
                        } else {
                            0
                        }
                    })
                })
                .collect();
            start.wait();
            admitted.wait();
            assert!(control.cancel_active());
            canceled.wait();
            handles.into_iter().map(|h| h.join().unwrap()).sum::<u32>()
        });
        assert_eq!(owners, 1);
        assert!(!control.cancel_active());
        assert!(control.start().is_some());
    }

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
        mgr.checkpoint().unwrap();
        (tmp, mgr)
    }

    #[test]
    fn test_eth_filter_converts_to_native_filter() {
        let filter = EthFilter {
            from_block: Some(BlockId::Number(100)),
            to_block: Some(BlockId::Number(200)),
            address: AddressFilter::Single(Address::repeat_byte(0xAA)),
            ..Default::default()
        };
        let native = filter.to_native_filter(250);
        assert_eq!(native.from_block, Some(100));
        assert_eq!(native.to_block, Some(200));
        assert_eq!(native.addresses, vec![Address::repeat_byte(0xAA)]);
    }

    #[tokio::test]
    async fn test_eth_get_logs_full() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));

        let addr = hex::encode(Address::repeat_byte(0xAA));
        let req_json = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": "0x0",
                "toBlock": "latest",
                "address": format!("0x{addr}")
            }],
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(req_json).unwrap();
        let Json(response) = handle_jsonrpc(State(state), Json(request)).await;

        assert!(response.error.is_none());
        let logs: Vec<serde_json::Value> =
            serde_json::from_value(response.result.unwrap()).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0]["address"].as_str().unwrap().contains(&addr));
    }

    #[tokio::test]
    async fn test_eth_block_number() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));

        let req_json = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(req_json).unwrap();
        let Json(response) = handle_jsonrpc(State(state), Json(request)).await;

        assert!(response.error.is_none());
        let block = response.result.unwrap();
        assert_eq!(block, "0xc8"); // 200
    }

    #[tokio::test]
    async fn test_web3_client_version() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));

        let req_json = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "web3_clientVersion",
            "params": [],
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(req_json).unwrap();
        let Json(response) = handle_jsonrpc(State(state), Json(request)).await;

        assert!(response.error.is_none());
        assert_eq!(
            response.result.unwrap(),
            serde_json::Value::String(LOGEX_CLIENT_VERSION.into())
        );
    }
}
