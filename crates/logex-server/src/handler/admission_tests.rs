use super::*;
use crate::grpc::pb::log_ex_service_server::LogExService;
use crate::grpc::{LogExGrpcService, pb};
use crate::rest::{self, QueryRequest};
use logex_storage::PartitionManagerConfig;

fn state(limit: usize) -> (tempfile::TempDir, Arc<AppState>) {
    let temp = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: temp.path().to_owned(),
        partition_target_rows: 1_000_000,
        compaction_safety_margin_blocks: 2_048,
    })
    .unwrap();
    let state = Arc::new(AppState::with_query_concurrency(
        storage,
        None,
        SyncStatus::default(),
        QueryConcurrencyLimit::new(limit).unwrap(),
    ));
    (temp, state)
}

async fn rpc(state: &Arc<AppState>, method: &str, params: serde_json::Value) -> Response {
    let document = serde_json::from_str(
        &serde_json::json!({
            "jsonrpc": "2.0", "method": method, "params": params, "id": 17,
        })
        .to_string(),
    )
    .unwrap();
    handle_jsonrpc(State(Arc::clone(state)), Ok(Json(document))).await
}

async fn json(response: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), 16 * 1024)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[test]
fn shared_capacity_counts_every_owner_and_errors_do_not_take_slots() {
    let control = Arc::new(QueryControl::new(QueryConcurrencyLimit::new(2).unwrap()));
    let exclusive = control.start().unwrap();
    let concurrent = control.start_concurrent().unwrap();
    let retained = concurrent.cancel_check();
    assert!(matches!(control.start(), Err(QueryAdmissionError::Busy)));
    assert!(matches!(
        control.start_concurrent(),
        Err(QueryAdmissionError::Capacity)
    ));
    drop(concurrent);
    assert!(retained());
    assert!(matches!(
        control.start_concurrent(),
        Err(QueryAdmissionError::Capacity)
    ));
    drop(retained);
    let next = control.start_concurrent().unwrap();
    assert!(!next.was_canceled());
    drop(next);
    drop(exclusive);
    assert_eq!(control.capacity.available_permits(), 2);

    let control = Arc::new(QueryControl::new(QueryConcurrencyLimit::new(1).unwrap()));
    let held = control.start().unwrap();
    control.fail_storage("failed while full".into());
    for error in [control.start().err(), control.start_concurrent().err()] {
        assert_eq!(
            error,
            Some(QueryAdmissionError::StorageUnavailable(
                "failed while full".into()
            ))
        );
    }
    assert!(held.was_canceled());
    assert_eq!(control.capacity.available_permits(), 0);
    drop(held);
    assert_eq!(control.capacity.available_permits(), 1);
    control.fail_storage("offline fixture".into());
    assert_eq!(
        control.start().err(),
        Some(QueryAdmissionError::StorageUnavailable(
            "failed while full".into()
        ))
    );
    assert_eq!(control.capacity.available_permits(), 1);
}

#[test]
fn simultaneous_admission_cannot_exceed_shared_capacity() {
    let control = Arc::new(QueryControl::new(QueryConcurrencyLimit::new(2).unwrap()));
    let start = std::sync::Barrier::new(9);
    let admitted = std::sync::Barrier::new(9);
    let release = std::sync::Barrier::new(9);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                scope.spawn(|| {
                    start.wait();
                    let query = control.start_concurrent();
                    admitted.wait();
                    release.wait();
                    usize::from(query.is_ok())
                })
            })
            .collect();
        start.wait();
        admitted.wait();
        assert_eq!(control.capacity.available_permits(), 0);
        release.wait();
        assert_eq!(
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .sum::<usize>(),
            2
        );
    });
    assert_eq!(control.capacity.available_permits(), 2);
}

#[tokio::test]
async fn shared_capacity_rejects_all_query_protocols_and_preserves_metadata() {
    let (_temp, state) = state(1);
    // A completed but unconsumed HTTP response still owns the single slot.
    let held = rpc(&state, "eth_getLogs", serde_json::json!([{}])).await;
    assert_eq!(held.status(), StatusCode::OK);
    assert!(matches!(
        state.query_control.start_concurrent(),
        Err(QueryAdmissionError::Capacity)
    ));
    // Holding the storage writer proves overload errors never wait for a scan.
    let writer = state.storage.write().await;
    let rest = rest::handle_query(
        State(Arc::clone(&state)),
        Json(QueryRequest {
            sql: String::new(),
            limit: None,
            offset: 0,
        }),
    )
    .await;
    assert_eq!(rest.status(), StatusCode::SERVICE_UNAVAILABLE);
    let rest = json(rest).await;
    assert_eq!(rest["status"], "query_capacity");
    assert_eq!(rest["resource"], "concurrency");
    let reply = json(rpc(&state, "eth_getLogs", serde_json::json!([{}])).await).await;
    assert_eq!(reply["id"], 17);
    assert_eq!(reply["error"]["code"], -32005);
    let notification =
        serde_json::from_str(r#"{"jsonrpc":"2.0","method":"eth_getLogs","params":[{}]}"#).unwrap();
    let notification = handle_jsonrpc(State(Arc::clone(&state)), Ok(Json(notification))).await;
    assert_eq!(notification.status(), StatusCode::NO_CONTENT);
    assert!(
        axum::body::to_bytes(notification.into_body(), 1024)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(state.query_control.capacity.available_permits(), 0);
    // Invalid JSON-RPC parameters keep their existing classification.
    let invalid = json(rpc(&state, "eth_getLogs", serde_json::json!([])).await).await;
    assert_eq!(invalid["error"]["code"], -32602);
    let service = LogExGrpcService::new(Arc::clone(&state));
    assert_eq!(
        service
            .query(tonic::Request::new(pb::QueryRequest::default()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        service
            .get_logs(tonic::Request::new(pb::GetLogsRequest::default()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert_eq!(
        service
            .stream_logs(tonic::Request::new(pb::GetLogsRequest::default()))
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::ResourceExhausted
    );
    assert!(
        !rest::handle_query_cancel(State(Arc::clone(&state)))
            .await
            .0
            .canceled
    );
    drop(writer);
    for method in ["web3_clientVersion", "net_version", "eth_blockNumber"] {
        assert!(
            json(rpc(&state, method, serde_json::json!([])).await)
                .await
                .get("result")
                .is_some()
        );
    }
    assert!(
        service
            .get_head_block(tonic::Request::new(pb::Empty {}))
            .await
            .is_ok()
    );
    use tower::ServiceExt as _;
    for path in ["/health", "/status"] {
        let response = crate::build_router(Arc::clone(&state))
            .oneshot(
                axum::http::Request::builder()
                    .uri(path)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
    drop(held);
    let next = service
        .get_logs(tonic::Request::new(pb::GetLogsRequest::default()))
        .await
        .unwrap();
    assert!(next.get_ref().logs.is_empty());
    drop(next);
    assert_eq!(state.query_control.capacity.available_permits(), 1);
}

#[tokio::test]
async fn abandoned_running_worker_owns_shared_capacity_until_actual_exit() {
    let (_temp, state) = state(1);
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let request_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        let query = request_state.query_control.start_concurrent().unwrap();
        let canceled = Arc::clone(&query.canceled);
        request_state
            .run_blocking_query(&query, move |_, _, cancel| {
                started_tx.send(canceled).unwrap();
                release_rx.recv().unwrap();
                assert!(cancel());
                Ok(())
            })
            .await
    });
    let canceled = started_rx.await.unwrap();
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(canceled.load(Ordering::Acquire));
    assert!(matches!(
        state.query_control.start_concurrent(),
        Err(QueryAdmissionError::Capacity)
    ));
    assert!(state.storage.try_write().is_ok());
    release_tx.send(()).unwrap();
    let returned = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        state.query_control.capacity.acquire(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(returned);
    assert!(state.query_control.start_concurrent().is_ok());
}

#[tokio::test]
async fn worker_errors_and_panics_return_capacity_after_owners_drop() {
    let (_temp, state) = state(1);
    for panic in [false, true] {
        let query = state.query_control.start_concurrent().unwrap();
        let result = state
            .run_blocking_query(&query, move |_, _, _| -> io::Result<()> {
                assert!(!panic, "offline worker failure fixture");
                Err(io::Error::other("offline read failure fixture"))
            })
            .await;
        assert!(result.is_err());
        assert!(matches!(
            state.query_control.start_concurrent(),
            Err(QueryAdmissionError::Capacity)
        ));
        drop(query);
        assert_eq!(state.query_control.capacity.available_permits(), 1);
    }
}
