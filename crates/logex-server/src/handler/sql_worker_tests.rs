use super::*;
use crate::grpc::pb::log_ex_service_server::LogExService;
use crate::grpc::{LogExGrpcService, pb};
use crate::rest;
use logex_storage::PartitionManagerConfig;
use logex_types::LogRow;
use std::future::Future;
use std::task::Poll;
use std::time::Duration;

fn state() -> (tempfile::TempDir, Arc<AppState>, Vec<LogRow>) {
    #[derive(serde::Deserialize)]
    struct Fixture {
        rows: Vec<LogRow>,
    }
    // Reuse the small, unmodified mainnet capture and its provenance. Runtime
    // scheduling controls below do not substitute synthetic blockchain rows.
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../../../logex-query/tests/fixtures/mainnet-exact-sums.json"
    ))
    .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: temp.path().to_owned(),
        partition_target_rows: 16,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    for block in fixture
        .rows
        .chunk_by(|a, b| a.block_number == b.block_number)
    {
        storage.write_batch(block).unwrap();
    }
    storage.checkpoint().unwrap();
    let state = Arc::new(AppState::with_query_concurrency(
        storage,
        None,
        SyncStatus::default(),
        QueryConcurrencyLimit::new(1).unwrap(),
    ));
    (temp, state, fixture.rows)
}

#[tokio::test]
async fn sql_conversion_keeps_runtime_live_and_revalidates_its_snapshot() {
    // Each current-thread runtime must keep serving metadata and ingestion
    // while an admitted SQL response is still being converted on its worker.
    for outcome in 0..4 {
        let (_temp, state, rows) = state();
        let runtime_thread = std::thread::current().id();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let request_state = Arc::clone(&state);
        let request = tokio::spawn(async move {
            let query = request_state.query_control.start().unwrap();
            request_state
                .run_sql_query(
                    &query,
                    "SELECT COUNT(*) AS n, MAX(block_number) AS h FROM logs".to_owned(),
                    SqlQueryPage::default(),
                    move |result, _memory, _cancel| {
                        entered_tx.send(std::thread::current().id()).unwrap();
                        resume_rx
                            .recv_timeout(Duration::from_secs(10))
                            .map_err(io::Error::other)?;
                        if outcome == 3 {
                            return Err(io::Error::other("conversion failed after reorg"));
                        }
                        Ok(result)
                    },
                )
                .await
        });
        let worker_thread = entered_rx.await.unwrap();
        assert_ne!(worker_thread, runtime_thread);
        assert!(state.storage.try_write().is_ok());
        assert!(matches!(
            state.query_control.start_concurrent(),
            Err(QueryAdmissionError::Capacity)
        ));
        let health = rest::handle_health(State(Arc::clone(&state))).await;
        assert_eq!(health.status(), StatusCode::OK);
        assert!(state.query_memory.used() > 0);
        if outcome >= 2 {
            assert!(
                state
                    .storage
                    .write()
                    .await
                    .mark_non_canonical(rows[0].block_hash)
                    .unwrap()
                    > 0
            );
        }
        if outcome == 1 || outcome == 3 {
            assert!(
                rest::handle_query_cancel(State(Arc::clone(&state)))
                    .await
                    .canceled
            );
        }
        resume_tx.send(()).unwrap();
        let result = request.await.unwrap();
        match outcome {
            0 => {
                let result = result.unwrap();
                assert_eq!(result.rows[0]["n"], rows.len());
                assert_eq!(result.rows[0]["h"], rows.last().unwrap().block_number);
            }
            1 => assert!(matches!(
                result,
                Err(SqlQueryError::Storage(error)) if error.kind() == io::ErrorKind::Interrupted
            )),
            _ => assert!(matches!(result, Err(SqlQueryError::SnapshotChanged))),
        }
        assert_eq!(state.query_memory.used(), 0);
        assert!(state.query_control.start_concurrent().is_ok());
    }
}

#[tokio::test]
async fn abandoned_sql_worker_retains_capacity_and_canceled_token_until_exit() {
    let (_temp, state, _) = state();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let request_state = Arc::clone(&state);
    let request = tokio::spawn(async move {
        let query = request_state.query_control.start().unwrap();
        request_state
            .run_sql_query(
                &query,
                "SELECT COUNT(*) AS n FROM logs".to_owned(),
                SqlQueryPage::default(),
                move |result, _memory, cancel| {
                    assert!(entered_tx.send(Arc::clone(cancel)).is_ok());
                    resume_rx
                        .recv_timeout(Duration::from_secs(10))
                        .map_err(io::Error::other)?;
                    Ok(result)
                },
            )
            .await
    });
    let cancel = entered_rx.await.unwrap();
    assert!(!cancel());
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    assert!(cancel());
    assert!(matches!(
        state.query_control.start_concurrent(),
        Err(QueryAdmissionError::Capacity)
    ));
    drop(cancel);
    resume_tx.send(()).unwrap();
    let returned = state.query_control.capacity.acquire().await.unwrap();
    drop(returned);
    assert_eq!(state.query_memory.used(), 0);
    assert!(state.query_control.start().is_ok());
}

#[tokio::test]
async fn both_sql_protocols_share_worker_gate_and_keep_metadata_available() {
    let (_temp, state, rows) = state();
    let workers = Arc::new(tokio::sync::Semaphore::new(1));
    state
        .blocking_query_workers
        .set(Arc::clone(&workers))
        .unwrap();
    let permit = workers.acquire().await.unwrap();
    let mut rest_query = Box::pin(rest::handle_query(
        State(Arc::clone(&state)),
        Json(rest::QueryRequest {
            sql: "SELECT COUNT(*) AS n FROM logs".to_owned(),
            limit: None,
            offset: 0,
        }),
    ));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(rest_query.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(state.storage.try_write().is_ok());
    assert!(
        rest::handle_query_cancel(State(Arc::clone(&state)))
            .await
            .canceled
    );
    drop(permit);
    let response = rest_query.await;
    assert_eq!(response.status(), StatusCode::CONFLICT);
    drop(response);
    assert_eq!(state.query_memory.used(), 0);

    let permit = workers.acquire().await.unwrap();
    let grpc = LogExGrpcService::new(Arc::clone(&state));
    let mut grpc_query = Box::pin(grpc.query(tonic::Request::new(pb::QueryRequest {
        sql: "SELECT COUNT(*) AS n FROM logs".to_owned(),
        limit: None,
        offset: None,
    })));
    assert!(
        std::future::poll_fn(|cx| Poll::Ready(grpc_query.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    let head = grpc
        .get_head_block(tonic::Request::new(pb::Empty {}))
        .await
        .unwrap();
    assert_eq!(
        head.get_ref().block_number,
        rows.last().unwrap().block_number
    );
    // A service failure must wake SQL waiting for worker capacity without
    // waiting for the occupied worker or allowing its query to start.
    state.mark_storage_unavailable("test volume unavailable");
    let error = grpc_query.await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::Unavailable);
    assert_eq!(error.message(), "test volume unavailable");
    drop(permit);
    assert_eq!(state.query_memory.used(), 0);
}
