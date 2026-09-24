use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Json, Response};

use logex_query::{self, DEFAULT_QUERY_PAGE_SIZE, NativeStorageSnapshot, QueryCancelCheck};
use logex_storage::PartitionManager;
use logex_types::{LOGEX_CLIENT_VERSION, QueryMemoryBudget, QueryMemoryLimit, SyncStatus};

use crate::eth_filter::{EthFilter, RpcLogs};
use crate::jsonrpc::{JsonRpcDocument, JsonRpcRequest, JsonRpcResponse};
use crate::query_encoding::{is_capacity_error, serialize_json};
use crate::query_response::retain_query_lease;
use crate::storage_metrics::CachedStorageMetrics;

use crate::ws::SubscriptionManager;

pub(crate) const MAX_LOG_FILTER_LIMIT: usize = 10_000;

/// Maximum database queries sharing one server, including unfinished workers
/// and retained response buffers. Metadata and subscriptions use other domains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueryConcurrencyLimit(usize);

impl QueryConcurrencyLimit {
    pub const DEFAULT: usize = 8;

    pub fn new(value: usize) -> Result<Self, String> {
        if value == 0 || value > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(format!(
                "query-max-concurrent must be between 1 and {}",
                tokio::sync::Semaphore::MAX_PERMITS
            ));
        }
        Ok(Self(value))
    }

    pub fn get(self) -> usize {
        self.0
    }
}

impl Default for QueryConcurrencyLimit {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

/// Shared application state.
pub struct AppState {
    pub storage: Arc<tokio::sync::RwLock<PartitionManager>>,
    /// WebSocket subscription manager. None if subscriptions are disabled.
    pub subscriptions: Option<SubscriptionManager>,
    /// Live sync progress, updated by the sync task.
    pub sync_status: Arc<std::sync::Mutex<SyncStatus>>,
    pub(crate) storage_metrics: Arc<tokio::sync::Mutex<CachedStorageMetrics>>,
    pub(crate) query_control: Arc<QueryControl>,
    pub(crate) query_memory: QueryMemoryBudget,
    native_query_workers: OnceLock<Arc<tokio::sync::Semaphore>>,
}

impl AppState {
    pub fn new(
        storage: PartitionManager,
        subscriptions: Option<SubscriptionManager>,
        sync_status: SyncStatus,
    ) -> Self {
        Self::with_query_concurrency(storage, subscriptions, sync_status, Default::default())
    }

    pub fn with_query_concurrency(
        storage: PartitionManager,
        subscriptions: Option<SubscriptionManager>,
        sync_status: SyncStatus,
        limit: QueryConcurrencyLimit,
    ) -> Self {
        Self::with_query_limits(
            storage,
            subscriptions,
            sync_status,
            limit,
            Default::default(),
        )
    }

    /// Configure shared admission and accounted memory for this server.
    /// DataFusion operators, index candidates, source reads, native working sets,
    /// scan output, structured results and encoded query responses participate.
    /// Manifest/control metadata and internal expression-kernel temporaries
    /// remain outside this accounting. This is not an RSS cap.
    pub fn with_query_limits(
        storage: PartitionManager,
        subscriptions: Option<SubscriptionManager>,
        sync_status: SyncStatus,
        concurrency: QueryConcurrencyLimit,
        memory: QueryMemoryLimit,
    ) -> Self {
        Self {
            storage: Arc::new(tokio::sync::RwLock::new(storage)),
            subscriptions,
            sync_status: Arc::new(std::sync::Mutex::new(sync_status)),
            storage_metrics: Arc::new(tokio::sync::Mutex::new(CachedStorageMetrics::default())),
            query_control: Arc::new(QueryControl::new(concurrency)),
            query_memory: QueryMemoryBudget::new(memory),
            native_query_workers: OnceLock::new(),
        }
    }

    /// Permanently close storage admission and cancel outstanding queries.
    /// This never accesses storage or waits for its lock. Restart after repair.
    pub fn mark_storage_unavailable(&self, reason: impl Into<String>) {
        self.query_control.fail_storage(reason.into());
    }

    pub(crate) fn storage_failure(&self) -> Option<String> {
        self.query_control.failure.borrow().clone()
    }

    pub(crate) async fn storage_unavailable(&self) -> String {
        let mut failure = self.query_control.failure.subscribe();
        loop {
            if let Some(reason) = failure.borrow_and_update().clone() {
                return reason;
            }
            failure
                .changed()
                .await
                .expect("AppState owns failure sender");
        }
    }

    pub(crate) async fn read_storage(
        &self,
    ) -> Result<tokio::sync::RwLockReadGuard<'_, PartitionManager>, String> {
        tokio::select! {
            biased;
            reason = self.storage_unavailable() => Err(reason),
            storage = self.storage.read() => {
                match self.storage_failure() {
                    Some(reason) => Err(reason),
                    None => Ok(storage),
                }
            }
        }
    }

    /// Capture the query view under the ingestion lock, then perform filesystem
    /// reads and response conversion on a blocking worker. The request owns the
    /// cancellation guard; dropping it permanently cancels any started work.
    pub(crate) async fn run_blocking_query<T, F>(
        &self,
        query: &ActiveQueryGuard,
        execute: F,
    ) -> io::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&NativeStorageSnapshot, u64, QueryCancelCheck) -> io::Result<T> + Send + 'static,
    {
        // Native scans previously occupied an async runtime worker. Size their
        // blocking gate from the serving runtime, even when AppState was built
        // outside it, rather than adopting the much larger blocking-pool limit.
        let workers = self.native_query_workers.get_or_init(|| {
            Arc::new(tokio::sync::Semaphore::new(
                tokio::runtime::Handle::current().metrics().num_workers(),
            ))
        });
        let permit = tokio::select! {
            biased;
            reason = self.storage_unavailable() => return Err(io::Error::other(reason)),
            permit = Arc::clone(workers).acquire_owned() => {
                permit.map_err(|_| io::Error::other("native query workers are unavailable"))?
            }
        };
        let (snapshot, head) = {
            let storage = self.read_storage().await.map_err(io::Error::other)?;
            (
                NativeStorageSnapshot::from_storage(&storage),
                storage.head_block().unwrap_or(0),
            )
        };
        let cancel = query.cancel_check();
        let mut worker = BlockingQueryWorker(tokio::task::spawn_blocking(move || {
            // A dropped HTTP/gRPC future cannot free this capacity while its
            // already-started filesystem operation still owns the snapshot.
            let _permit = permit;
            let result = check_native_query_canceled(&cancel)
                .and_then(|()| execute(&snapshot, head, Arc::clone(&cancel)));
            // A reorg during scan or conversion invalidates the entire result,
            // including an error result, before cancellation is interpreted.
            snapshot.validate()?;
            // Cancellation during protocol conversion must not return success.
            check_native_query_canceled(&cancel)?;
            result
        }));
        tokio::select! {
            biased;
            reason = self.storage_unavailable() => Err(io::Error::other(reason)),
            result = &mut worker.0 => {
                result.map_err(|error| io::Error::other(format!("query worker failed: {error}")))?
            }
        }
    }
}

struct BlockingQueryWorker<T>(tokio::task::JoinHandle<io::Result<T>>);

impl<T> Drop for BlockingQueryWorker<T> {
    fn drop(&mut self) {
        // Abort work that has not started. Running filesystem work cannot be
        // forcibly interrupted and observes the request's cancellation token.
        self.0.abort();
    }
}

fn check_native_query_canceled(cancel: &QueryCancelCheck) -> io::Result<()> {
    if cancel() {
        Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"))
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct QueryControl {
    // Admission, cancellation and completion share one linearization point.
    // The query's hot cancellation checks only read its own atomic token.
    // Critical sections only replace owned pointers/read or write atomics; no
    // user code runs under the lock, so recovering its value after poison is safe.
    active: Mutex<QueryAdmissions>,
    failure: tokio::sync::watch::Sender<Option<String>>,
    capacity: Arc<tokio::sync::Semaphore>,
}

#[derive(Debug, Default)]
struct QueryAdmissions {
    exclusive: Option<Arc<AtomicBool>>,
    concurrent: Vec<Weak<AtomicBool>>,
    failed: bool,
}

impl Default for QueryControl {
    fn default() -> Self {
        Self::new(QueryConcurrencyLimit::default())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum QueryAdmissionError {
    StorageUnavailable(String),
    Busy,
    Capacity,
}

impl QueryAdmissionError {
    pub(crate) fn into_grpc_status(self) -> tonic::Status {
        match self {
            Self::StorageUnavailable(reason) => tonic::Status::unavailable(reason),
            Self::Busy => tonic::Status::already_exists("another SQL query is already running"),
            Self::Capacity => tonic::Status::resource_exhausted(Self::CAPACITY_MESSAGE),
        }
    }

    pub(crate) const CAPACITY_MESSAGE: &'static str = "shared query concurrency capacity is exhausted; retry after outstanding queries or responses finish";
}

/// Separately owned from the request's cancellation guard: a disconnected
/// request cannot return its slot while a worker or response still uses it.
#[derive(Debug, Clone)]
pub(crate) struct QueryLease {
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
}

impl QueryControl {
    pub(crate) fn new(limit: QueryConcurrencyLimit) -> Self {
        Self {
            active: Mutex::new(QueryAdmissions::default()),
            failure: tokio::sync::watch::channel(None).0,
            capacity: Arc::new(tokio::sync::Semaphore::new(limit.get())),
        }
    }

    pub(crate) fn start(self: &Arc<Self>) -> Result<ActiveQueryGuard, QueryAdmissionError> {
        self.admit(true)
    }

    pub(crate) fn start_concurrent(
        self: &Arc<Self>,
    ) -> Result<ActiveQueryGuard, QueryAdmissionError> {
        self.admit(false)
    }

    fn admit(self: &Arc<Self>, exclusive: bool) -> Result<ActiveQueryGuard, QueryAdmissionError> {
        let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
        if active.failed {
            return Err(QueryAdmissionError::StorageUnavailable(
                self.failure.borrow().clone().expect("failure is latched"),
            ));
        }
        if exclusive && active.exclusive.is_some() {
            return Err(QueryAdmissionError::Busy);
        }
        // No admission queue: only bounded, admitted work can wait for storage
        // or the narrower native worker gate. The mutex also serializes failure.
        let permit = Arc::clone(&self.capacity)
            .try_acquire_owned()
            .map_err(|_| QueryAdmissionError::Capacity)?;
        let canceled = Arc::new(AtomicBool::new(false));
        if exclusive {
            active.exclusive = Some(Arc::clone(&canceled));
        } else {
            active.concurrent.retain(|token| token.strong_count() > 0);
            active.concurrent.push(Arc::downgrade(&canceled));
        }
        Ok(ActiveQueryGuard {
            control: Arc::clone(self),
            canceled,
            exclusive,
            lease: QueryLease {
                _permit: Arc::new(permit),
            },
        })
    }

    fn fail_storage(&self, reason: String) {
        let mut active = self.active.lock().unwrap_or_else(|err| err.into_inner());
        if active.failed {
            return;
        }
        active.failed = true;
        if let Some(token) = &active.exclusive {
            token.store(true, Ordering::Release);
        }
        for token in active.concurrent.iter().filter_map(Weak::upgrade) {
            token.store(true, Ordering::Release);
        }
        self.failure.send_replace(Some(reason));
    }

    pub(crate) fn cancel_active(&self) -> bool {
        let active = self.active.lock().unwrap_or_else(|err| err.into_inner());
        if let Some(canceled) = active.exclusive.as_ref() {
            canceled.store(true, Ordering::Release);
        }
        active.exclusive.is_some()
    }
}

pub(crate) struct ActiveQueryGuard {
    control: Arc<QueryControl>,
    canceled: Arc<AtomicBool>,
    exclusive: bool,
    lease: QueryLease,
}

impl ActiveQueryGuard {
    pub(crate) fn cancel_check(&self) -> logex_query::QueryCancelCheck {
        let canceled = Arc::clone(&self.canceled);
        let lease = self.lease();
        Arc::new(move || {
            let _lease = &lease;
            canceled.load(Ordering::Acquire)
        })
    }

    pub(crate) fn lease(&self) -> QueryLease {
        self.lease.clone()
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
        if self.exclusive {
            active.exclusive = None;
        }
    }
}

/// Handle a JSON-RPC request.
pub async fn handle_jsonrpc(
    State(state): State<Arc<AppState>>,
    document: Result<Json<JsonRpcDocument>, JsonRejection>,
) -> Response {
    let request = match document {
        Ok(Json(JsonRpcDocument::Request(request))) => request,
        Ok(Json(JsonRpcDocument::InvalidRequest)) | Err(JsonRejection::JsonDataError(_)) => {
            return Json(JsonRpcResponse::invalid_request()).into_response();
        }
        Ok(Json(JsonRpcDocument::UnsupportedBatch)) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                "JSON-RPC batches are not supported",
            )
                .into_response();
        }
        Err(JsonRejection::JsonSyntaxError(_)) => {
            return Json(JsonRpcResponse::parse_error()).into_response();
        }
        Err(rejection) => return rejection.into_response(),
    };
    let notification = request.id.is_none();
    let (response, lease) = dispatch_jsonrpc(state, request).await;
    if notification {
        StatusCode::NO_CONTENT.into_response()
    } else {
        match lease {
            Some(lease) => retain_query_lease(response, lease),
            None => response,
        }
    }
}

enum RpcMethod {
    GetLogs(EthFilter),
    BlockNumber,
    ClientVersion,
    NetworkVersion,
    Unknown,
}

impl RpcMethod {
    fn parse(method: &str, params: Option<serde_json::Value>) -> Result<Self, String> {
        let method = match method {
            "eth_getLogs" => {
                let Some(serde_json::Value::Array(mut params)) = params else {
                    return Err("eth_getLogs requires exactly one positional filter".into());
                };
                if params.len() != 1 {
                    return Err("eth_getLogs requires exactly one positional filter".into());
                }
                let filter: EthFilter = serde_json::from_value(params.remove(0))
                    .map_err(|error| format!("invalid filter: {error}"))?;
                filter.validate()?;
                if filter
                    .limit
                    .is_some_and(|limit| limit > MAX_LOG_FILTER_LIMIT)
                {
                    return Err(format!("limit must be at most {MAX_LOG_FILTER_LIMIT}"));
                }
                if filter.offset >= MAX_LOG_FILTER_LIMIT {
                    return Err(format!("offset must be less than {MAX_LOG_FILTER_LIMIT}"));
                }
                return Ok(Self::GetLogs(filter));
            }
            "eth_blockNumber" => Self::BlockNumber,
            "web3_clientVersion" => Self::ClientVersion,
            "net_version" => Self::NetworkVersion,
            _ => return Ok(Self::Unknown),
        };
        let empty = match params {
            None => true,
            Some(serde_json::Value::Array(values)) => values.is_empty(),
            Some(serde_json::Value::Object(values)) => values.is_empty(),
            _ => false,
        };
        if !empty {
            return Err("method takes no parameters".into());
        }
        Ok(method)
    }
}

async fn dispatch_jsonrpc(
    state: Arc<AppState>,
    request: JsonRpcRequest,
) -> (Response, Option<QueryLease>) {
    let id = request.id.unwrap_or_default();
    let method = match RpcMethod::parse(&request.method, request.params) {
        Ok(method) => method,
        Err(error) => {
            return (
                Json(JsonRpcResponse::invalid_params(id, error)).into_response(),
                None,
            );
        }
    };
    // Metadata methods remain available without opening storage.
    match method {
        RpcMethod::ClientVersion => {
            return (
                Json(JsonRpcResponse::success(id, LOGEX_CLIENT_VERSION.into())).into_response(),
                None,
            );
        }
        RpcMethod::NetworkVersion => {
            return (
                Json(JsonRpcResponse::success(id, "1".into())).into_response(),
                None,
            );
        }
        RpcMethod::Unknown => {
            return (
                Json(JsonRpcResponse::method_not_found(id)).into_response(),
                None,
            );
        }
        RpcMethod::BlockNumber => {
            let storage = match state.read_storage().await {
                Ok(storage) => storage,
                Err(reason) => {
                    return (
                        Json(JsonRpcResponse::internal_error(id, reason)).into_response(),
                        None,
                    );
                }
            };
            return (
                Json(JsonRpcResponse::success(
                    id,
                    serde_json::Value::String(format!("0x{:x}", storage.head_block().unwrap_or(0))),
                ))
                .into_response(),
                None,
            );
        }
        _ => {}
    }
    let query = match state.query_control.start_concurrent() {
        Ok(query) => query,
        Err(QueryAdmissionError::Capacity) => {
            return (
                Json(JsonRpcResponse::error(
                    id,
                    -32005,
                    QueryAdmissionError::CAPACITY_MESSAGE.into(),
                ))
                .into_response(),
                None,
            );
        }
        Err(QueryAdmissionError::StorageUnavailable(reason)) => {
            return (
                Json(JsonRpcResponse::internal_error(id, reason)).into_response(),
                None,
            );
        }
        Err(QueryAdmissionError::Busy) => {
            unreachable!("concurrent admission has no exclusive owner")
        }
    };
    // Share the exact request ID with the worker without cloning its raw JSON.
    // The request's bounded control data is separate from scalable query output.
    let id = Arc::new(id);
    let response = match method {
        RpcMethod::GetLogs(filter) => {
            let worker_id = Arc::clone(&id);
            let memory = state.query_memory.clone();
            state
                .run_blocking_query(&query, move |snapshot, head, cancel| {
                    handle_eth_get_logs(snapshot, head, filter, &cancel, &memory, &worker_id)
                })
                .await
        }
        RpcMethod::BlockNumber
        | RpcMethod::ClientVersion
        | RpcMethod::NetworkVersion
        | RpcMethod::Unknown => {
            unreachable!("metadata returned before query admission")
        }
    };
    let response = match state.storage_failure() {
        Some(reason) => Json(JsonRpcResponse::internal_error(&**id, reason)).into_response(),
        None => match response {
            Ok(bytes) => ([(header::CONTENT_TYPE, "application/json")], bytes).into_response(),
            Err(error) if is_capacity_error(&error) => {
                Json(JsonRpcResponse::error(&**id, -32005, error.to_string())).into_response()
            }
            Err(error) => {
                Json(JsonRpcResponse::internal_error(&**id, error.to_string())).into_response()
            }
        },
    };
    (response, Some(query.lease()))
}

fn handle_eth_get_logs(
    snapshot: &NativeStorageSnapshot,
    head_block: u64,
    filter: EthFilter,
    cancel: &logex_query::QueryCancelCheck,
    memory: &QueryMemoryBudget,
    id: &serde_json::value::RawValue,
) -> io::Result<bytes::Bytes> {
    let mut native_filter = filter.to_native_filter(head_block);
    native_filter.limit = Some(
        filter
            .limit
            .unwrap_or(DEFAULT_QUERY_PAGE_SIZE)
            .min(MAX_LOG_FILTER_LIMIT - filter.offset),
    );
    native_filter.offset = filter.offset;
    let rows = logex_query::execute_log_filter_on_snapshot_with_memory(
        snapshot,
        &native_filter,
        Some(cancel),
        memory,
    )?;
    serialize_json(
        &JsonRpcResponse {
            jsonrpc: "2.0",
            result: Some(RpcLogs(&rows)),
            error: None,
            id,
        },
        memory,
        Some(cancel),
    )
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

    async fn decode_rpc_response(response: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn notification_waits_for_native_work_and_drop_permanently_cancels_it() {
        use std::future::Future;
        use std::task::Poll;
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let (release_tx, release_rx) = std::sync::mpsc::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                let _ = entered_tx.send(());
                let _ = release_rx.recv();
            });
            entered_rx.await.unwrap();
            let document =
                serde_json::from_str(r#"{"jsonrpc":"2.0","method":"eth_getLogs","params":[{}]}"#)
                    .unwrap();
            let mut notification = Box::pin(handle_jsonrpc(
                State(Arc::clone(&state)),
                Ok(Json(document)),
            ));
            let first =
                std::future::poll_fn(|cx| Poll::Ready(notification.as_mut().poll(cx))).await;
            assert!(
                first.is_pending(),
                "notification acknowledged before native execution"
            );
            let token = state
                .query_control
                .active
                .lock()
                .unwrap()
                .concurrent
                .last()
                .unwrap()
                .upgrade()
                .unwrap();
            assert!(!token.load(Ordering::Acquire));
            assert!(
                state.storage.try_write().is_ok(),
                "queued notification kept ingestion lock"
            );
            drop(notification);
            assert!(token.load(Ordering::Acquire));
            release_tx.send(()).unwrap();
            occupied.await.unwrap();
            let workers = state.native_query_workers.get().unwrap();
            let _capacity = workers.acquire().await.unwrap();
            let fresh = state.query_control.start_concurrent().unwrap();
            assert!(!fresh.was_canceled());
            assert!(token.load(Ordering::Acquire));
        });
    }

    #[test]
    fn storage_failure_permanently_closes_all_admission_and_cancels_tokens() {
        let control = Arc::new(QueryControl::default());
        let exclusive = control.start().unwrap();
        let concurrent = control.start_concurrent().unwrap();
        let retained = concurrent.cancel_check();
        control.fail_storage("volume unavailable".into());
        assert!(exclusive.was_canceled() && concurrent.was_canceled() && retained());
        drop(exclusive);
        drop(concurrent);
        control.fail_storage("later failure".into());
        assert!(matches!(
            control.start(),
            Err(QueryAdmissionError::StorageUnavailable(_))
        ));
        assert_eq!(
            control.start_concurrent().err(),
            Some(QueryAdmissionError::StorageUnavailable(
                "volume unavailable".into()
            ))
        );
        assert!(retained());
    }

    #[tokio::test]
    async fn failure_wakes_requests_waiting_for_storage_and_rejects_rpc() {
        use std::task::Poll;
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let _writer = state.storage.write().await;
        let mut pending = Box::pin(state.read_storage());
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        state.mark_storage_unavailable("volume unavailable");
        let result = tokio::time::timeout(std::time::Duration::from_secs(1), pending)
            .await
            .unwrap();
        assert_eq!(result.err().as_deref(), Some("volume unavailable"));
        for method in [
            "eth_getLogs",
            "eth_blockNumber",
            "web3_clientVersion",
            "net_version",
        ] {
            let request = serde_json::from_str(&serde_json::json!({
                "jsonrpc": "2.0", "method": method, "params": if method == "eth_getLogs" { serde_json::json!([{}]) } else { serde_json::json!([]) }, "id": 1,
            }).to_string())
            .unwrap();
            let response = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                handle_jsonrpc(State(Arc::clone(&state)), Ok(Json(request))),
            )
            .await
            .unwrap();
            let response = decode_rpc_response(response).await;
            assert_eq!(response.get("error").is_some(), method.starts_with("eth_"));
        }
    }

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
                    if let Ok(query) = control.start() {
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
        assert!(matches!(control.start(), Err(QueryAdmissionError::Busy)));
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
        assert!(
            matches!(control.start(), Err(QueryAdmissionError::Busy)),
            "cancel is not completion"
        );
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
                        if let Ok(query) = query {
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
        assert!(control.start().is_ok());
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

    #[tokio::test]
    async fn dropping_native_request_cancels_started_worker() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            let query = request_state.query_control.start_concurrent().unwrap();
            request_state
                .run_blocking_query(&query, move |snapshot, _head, cancel| {
                    let _ = started_tx.send(Arc::clone(&cancel));
                    resume_rx.recv().map_err(io::Error::other)?;
                    let result = logex_query::execute_log_filter_on_snapshot_with_cancel(
                        snapshot,
                        &Default::default(),
                        Some(&cancel),
                    );
                    let _ = finished_tx.send(result.as_ref().err().map(io::Error::kind));
                    result
                })
                .await
        });
        let cancel = started_rx.await.unwrap();
        assert!(!cancel());
        let workers = state.native_query_workers.get().unwrap();
        assert_eq!(workers.available_permits(), 0);
        assert!(state.storage.try_write().is_ok());
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        assert!(cancel(), "the abandoned worker keeps its canceled token");
        assert!(workers.try_acquire().is_err());
        resume_tx.send(()).unwrap();
        assert_eq!(finished_rx.await.unwrap(), Some(io::ErrorKind::Interrupted));
        let _returned_capacity = workers.acquire().await.unwrap();
        let fresh = state.query_control.start_concurrent().unwrap();
        assert!(!fresh.was_canceled());
    }

    #[tokio::test]
    async fn native_worker_completion_checks_cancellation_after_conversion() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            let query = request_state.query_control.start().unwrap();
            request_state
                .run_blocking_query(&query, move |_snapshot, _head, _cancel| {
                    let _ = started_tx.send(());
                    resume_rx.recv().map_err(io::Error::other)?;
                    // Represents a successful response conversion that finished
                    // after cancellation; the worker must discard its result.
                    Ok(7)
                })
                .await
        });
        started_rx.await.unwrap();
        assert!(state.query_control.cancel_active());
        resume_tx.send(()).unwrap();
        assert_eq!(
            request.await.unwrap().unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
    }

    #[tokio::test]
    async fn native_worker_revalidates_after_conversion_and_prioritizes_reorg() {
        for cancel_after_conversion in [false, true] {
            let (_tmp, storage) = setup_storage();
            let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            let request_state = Arc::clone(&state);
            let request = tokio::spawn(async move {
                let query = request_state.query_control.start().unwrap();
                request_state
                    .run_blocking_query(&query, move |snapshot, _head, cancel| {
                        let rows = logex_query::execute_log_filter_on_snapshot_with_cancel(
                            snapshot,
                            &Default::default(),
                            Some(&cancel),
                        )?;
                        let _ = started_tx.send(());
                        resume_rx.recv().map_err(io::Error::other)?;
                        // The read was valid, but a concurrent reorg can occur
                        // before conversion of these rows finishes.
                        Ok(rows.len())
                    })
                    .await
            });
            started_rx.await.unwrap();
            assert_eq!(
                state
                    .storage
                    .write()
                    .await
                    .mark_non_canonical(B256::repeat_byte(2))
                    .unwrap(),
                1
            );
            if cancel_after_conversion {
                assert!(state.query_control.cancel_active());
            }
            resume_tx.send(()).unwrap();
            assert_eq!(
                request.await.unwrap().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        }
    }

    #[test]
    fn queued_native_request_never_executes_abandoned_operation() {
        use std::future::Future;
        use std::task::Poll;

        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::with_query_concurrency(
            storage,
            None,
            SyncStatus::default(),
            QueryConcurrencyLimit::new(1).unwrap(),
        ));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (resume_tx, resume_rx) = std::sync::mpsc::channel();
            let occupied = tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                let _ = resume_rx.recv();
            });
            started_rx.await.unwrap();
            let ran = Arc::new(AtomicBool::new(false));
            let worker_ran = Arc::clone(&ran);
            let (operation_tx, operation_rx) = tokio::sync::oneshot::channel();
            let mut request = Box::pin(async {
                let query = state.query_control.start_concurrent().unwrap();
                state
                    .run_blocking_query(&query, move |_snapshot, _head, _cancel| {
                        worker_ran.store(true, Ordering::Release);
                        let _ = operation_tx.send(());
                        Ok(())
                    })
                    .await
            });
            assert!(
                std::future::poll_fn(|cx| Poll::Ready(request.as_mut().poll(cx)))
                    .await
                    .is_pending()
            );
            assert!(matches!(
                state.query_control.start_concurrent(),
                Err(QueryAdmissionError::Capacity)
            ));
            drop(request);
            resume_tx.send(()).unwrap();
            occupied.await.unwrap();
            // Observe disposal of the captured operation, independent of pool
            // queue ordering. It must be dropped without being invoked.
            assert!(operation_rx.await.is_err());
            assert!(!ran.load(Ordering::Acquire));
            let returned = state.query_control.capacity.acquire().await.unwrap();
            drop(returned);
            assert!(state.query_control.start_concurrent().is_ok());
        });
    }

    #[tokio::test]
    async fn native_worker_propagates_operation_errors() {
        let (_tmp, storage) = setup_storage();
        let state = AppState::new(storage, None, SyncStatus::default());
        let query = state.query_control.start_concurrent().unwrap();
        let error = state
            .run_blocking_query(&query, |_snapshot, _head, _cancel| {
                Err::<(), _>(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "fixture read failed",
                ))
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "fixture read failed");
    }

    #[tokio::test]
    async fn storage_failure_wakes_native_capacity_waiters_and_started_requests() {
        use std::future::Future;
        use std::task::Poll;

        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let request_state = Arc::clone(&state);
        let started = tokio::spawn(async move {
            let query = request_state.query_control.start_concurrent().unwrap();
            request_state
                .run_blocking_query(&query, move |_snapshot, _head, _cancel| {
                    let _ = started_tx.send(());
                    resume_rx.recv().map_err(io::Error::other)
                })
                .await
        });
        started_rx.await.unwrap();
        let workers = state.native_query_workers.get().unwrap();
        assert_eq!(workers.available_permits(), 0);
        let writer = state.storage.try_write().unwrap();
        let waiting_query = state.query_control.start_concurrent().unwrap();
        let ran = Arc::new(AtomicBool::new(false));
        let worker_ran = Arc::clone(&ran);
        let mut waiting = Box::pin(state.run_blocking_query(
            &waiting_query,
            move |_snapshot, _head, _cancel| {
                worker_ran.store(true, Ordering::Release);
                Ok(())
            },
        ));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        state.mark_storage_unavailable("fixture storage unavailable");
        assert_eq!(
            waiting.await.unwrap_err().to_string(),
            "fixture storage unavailable"
        );
        assert_eq!(
            started.await.unwrap().unwrap_err().to_string(),
            "fixture storage unavailable"
        );
        assert!(waiting_query.was_canceled());
        assert!(!ran.load(Ordering::Acquire));
        assert_eq!(workers.available_permits(), 0);
        drop(writer);
        resume_tx.send(()).unwrap();
        let _returned_capacity = workers.acquire().await.unwrap();
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

        let request = serde_json::from_str(&req_json.to_string()).unwrap();
        let response =
            decode_rpc_response(handle_jsonrpc(State(state), Ok(Json(request))).await).await;

        assert!(response.get("error").is_none());
        let logs: Vec<serde_json::Value> =
            serde_json::from_value(response["result"].clone()).unwrap();
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

        let request = serde_json::from_str(&req_json.to_string()).unwrap();
        let response =
            decode_rpc_response(handle_jsonrpc(State(state), Ok(Json(request))).await).await;

        assert!(response.get("error").is_none());
        let block = &response["result"];
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

        let request = serde_json::from_str(&req_json.to_string()).unwrap();
        let response =
            decode_rpc_response(handle_jsonrpc(State(state), Ok(Json(request))).await).await;

        assert!(response.get("error").is_none());
        assert_eq!(
            response["result"],
            serde_json::Value::String(LOGEX_CLIENT_VERSION.into())
        );
    }
}

#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod composition_tests;
#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod response_memory_tests;
