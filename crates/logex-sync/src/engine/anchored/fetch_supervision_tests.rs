use super::*;
use crate::p2p::peer_manager::{
    empty_body_receipt_outcome, empty_header_outcome, engine_peer_fixture,
};
use logex_storage::PartitionManagerConfig;
use tokio::sync::oneshot;

async fn fixture() -> (SyncEngine, impl Sized) {
    let (peers, resources) = engine_peer_fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: directory.path().to_path_buf(),
        ..Default::default()
    })
    .unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let mut engine = SyncEngine::new(
        SyncConfig::default(),
        peers,
        Arc::new(RwLock::new(storage)),
        None,
        Arc::new(std::sync::Mutex::new(SyncStatus::default())),
        None,
        receiver,
    );
    engine.historical_fetch_expected_child = Some(child());
    (engine, (resources, directory, shutdown))
}

fn child() -> Header {
    Header {
        number: 100,
        ..Default::default()
    }
}

fn install_body(engine: &mut SyncEngine, sequence: u64, attempt: u64, handle: JoinHandle<()>) {
    engine
        .historical_fetch_handles
        .entry(sequence)
        .or_insert_with(|| HistoricalFetchHandle {
            attempts: HashMap::new(),
        })
        .attempts
        .insert(
            attempt,
            HistoricalFetchAttemptHandle {
                child_header: child(),
                owner: 0,
                handle,
            },
        );
}

fn install_header(engine: &mut SyncEngine, handle: JoinHandle<()>) {
    engine.historical_header_fetch_handle = Some(HistoricalHeaderFetchHandle {
        sequence: 0,
        attempt: 0,
        child_header: child(),
        handle,
    });
}

async fn wait_finished(handle: &JoinHandle<()>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("local worker should finish");
}

async fn stopped_worker(header: bool, aborted: bool) {
    let (mut engine, _resources) = fixture().await;
    let handle = if aborted {
        let handle = tokio::spawn(std::future::pending::<()>());
        handle.abort();
        handle
    } else {
        tokio::spawn(async {})
    };
    wait_finished(&handle).await;
    if header {
        install_header(&mut engine, handle);
    } else {
        install_body(&mut engine, 0, 0, handle);
    }
    let result = tokio::time::timeout(
        Duration::from_millis(100),
        engine.wait_for_historical_fetch_outcome(&child()),
    )
    .await;
    engine.reset_historical_fetch_pipeline();
    let result = result.expect("a stopped worker must be reported instead of waiting for a result");
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("missing worker result must be an error"),
    };
    assert!(
        error.to_string().contains(if header {
            "historical header fetch"
        } else {
            "historical body/receipt fetch"
        }),
        "{error}"
    );
}

#[tokio::test]
async fn stopped_body_worker_is_reported() {
    stopped_worker(false, true).await;
}
#[tokio::test]
async fn stopped_header_worker_is_reported() {
    stopped_worker(true, true).await;
}
#[tokio::test]
async fn body_worker_return_without_outcome_is_reported() {
    stopped_worker(false, false).await;
}
#[tokio::test]
async fn header_worker_return_without_outcome_is_reported() {
    stopped_worker(true, false).await;
}

fn body_outcome(generation: u64, sequence: u64, attempt: u64) -> HistoricalFetchOutcome {
    HistoricalFetchOutcome {
        generation,
        sequence,
        attempt,
        header_batch: HistoricalHeaderBatch {
            child_header: child(),
            header_peer: PeerId::ZERO,
            headers: vec![],
            hashes: vec![],
            required_block: 99,
            header_elapsed: Duration::ZERO,
        },
        body_receipt_elapsed: Duration::ZERO,
        outcome: empty_body_receipt_outcome(),
    }
}

fn header_outcome(generation: u64) -> HistoricalHeaderFetchOutcome {
    HistoricalHeaderFetchOutcome {
        generation,
        sequence: 0,
        attempt: 0,
        child_header: child(),
        target_count: 1,
        header_elapsed: Duration::ZERO,
        outcome: empty_header_outcome(),
    }
}

async fn completed_body_worker(still_running: bool) {
    let (mut engine, _resources) = fixture().await;
    let tx = engine.historical_fetch_tx.clone();
    let (release, receiver) = oneshot::channel::<()>();
    let (sent, result_ready) = oneshot::channel();
    let handle = tokio::spawn(async move {
        tx.send(body_outcome(0, 0, 0)).ok().unwrap();
        sent.send(()).unwrap();
        if still_running {
            let _ = receiver.await;
        }
    });
    result_ready.await.unwrap();
    if !still_running {
        wait_finished(&handle).await;
    } else {
        assert!(!handle.is_finished());
    }
    install_body(&mut engine, 0, 0, handle);
    let result = engine
        .wait_for_historical_fetch_outcome(&child())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.header_batch.child_header, child());
    assert_eq!(engine.active_historical_fetch_count(), 0);
    drop(release);
}

#[tokio::test]
async fn completed_body_result_is_kept() {
    completed_body_worker(false).await;
}
#[tokio::test]
async fn body_result_before_worker_return_is_kept() {
    completed_body_worker(true).await;
}

async fn completed_header_worker(still_running: bool) {
    let (mut engine, _resources) = fixture().await;
    let tx = engine.historical_header_fetch_tx.clone();
    let (release, receiver) = oneshot::channel::<()>();
    let (sent, result_ready) = oneshot::channel();
    let handle = tokio::spawn(async move {
        tx.send(header_outcome(0)).ok().unwrap();
        sent.send(()).unwrap();
        if still_running {
            let _ = receiver.await;
        }
    });
    result_ready.await.unwrap();
    if !still_running {
        wait_finished(&handle).await;
    } else {
        assert!(!handle.is_finished());
    }
    install_header(&mut engine, handle);
    assert!(
        !engine
            .drain_historical_header_fetch_outcomes()
            .await
            .unwrap()
    );
    assert!(engine.historical_header_fetch_handle.is_none());
    assert_eq!(
        engine.historical_fetch_planned_child.as_ref(),
        Some(&child())
    );
    drop(release);
}

#[tokio::test]
async fn completed_header_result_is_kept() {
    completed_header_worker(false).await;
}
#[tokio::test]
async fn header_result_before_worker_return_is_kept() {
    completed_header_worker(true).await;
}

#[tokio::test]
async fn pending_workers_remain_active() {
    let (mut engine, _resources) = fixture().await;
    install_body(&mut engine, 0, 0, tokio::spawn(std::future::pending()));
    install_header(&mut engine, tokio::spawn(std::future::pending()));
    engine.drain_historical_fetch_outcomes().await.unwrap();
    engine
        .drain_historical_header_fetch_outcomes()
        .await
        .unwrap();
    assert_eq!(engine.active_historical_fetch_count(), 2);
    engine.reset_historical_fetch_pipeline();
}

#[tokio::test]
async fn reset_cancellation_does_not_fail_a_new_generation() {
    let (mut engine, _resources) = fixture().await;
    let body = tokio::spawn(std::future::pending());
    let body_abort = body.abort_handle();
    let header = tokio::spawn(std::future::pending());
    let header_abort = header.abort_handle();
    install_body(&mut engine, 0, 0, body);
    install_header(&mut engine, header);
    engine.reset_historical_fetch_pipeline();
    tokio::task::yield_now().await;
    assert!(body_abort.is_finished() && header_abort.is_finished());
    engine.historical_fetch_expected_child = Some(child());
    install_body(&mut engine, 0, 0, tokio::spawn(std::future::pending()));
    install_header(&mut engine, tokio::spawn(std::future::pending()));
    engine
        .historical_fetch_tx
        .send(body_outcome(0, 0, 0))
        .ok()
        .unwrap();
    engine
        .historical_header_fetch_tx
        .send(header_outcome(0))
        .ok()
        .unwrap();
    engine.drain_historical_fetch_outcomes().await.unwrap();
    engine
        .drain_historical_header_fetch_outcomes()
        .await
        .unwrap();
    assert_eq!(engine.active_historical_fetch_count(), 2);
    assert!(engine.historical_fetch_completed.is_empty());
    engine.reset_historical_fetch_pipeline();
}

#[tokio::test]
async fn queued_success_supersedes_a_stopped_competing_attempt() {
    let (mut engine, _resources) = fixture().await;
    let stopped = tokio::spawn(std::future::pending());
    stopped.abort();
    wait_finished(&stopped).await;
    install_body(&mut engine, 0, 0, stopped);
    let tx = engine.historical_fetch_tx.clone();
    let winner = tokio::spawn(async move {
        tx.send(body_outcome(0, 0, 1)).ok().unwrap();
    });
    wait_finished(&winner).await;
    install_body(&mut engine, 0, 1, winner);
    let result = engine
        .wait_for_historical_fetch_outcome(&child())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.attempt, 1);
    assert_eq!(engine.active_historical_fetch_count(), 0);
}

#[tokio::test]
async fn obsolete_workers_are_retired_without_failure() {
    let (mut engine, _resources) = fixture().await;
    let body = tokio::spawn(async {});
    let header = tokio::spawn(async {});
    wait_finished(&body).await;
    wait_finished(&header).await;
    install_body(&mut engine, 0, 0, body);
    install_header(&mut engine, header);
    engine.historical_fetch_expected_sequence = 1;
    engine.drain_historical_fetch_outcomes().await.unwrap();
    engine
        .drain_historical_header_fetch_outcomes()
        .await
        .unwrap();
    assert_eq!(engine.active_historical_fetch_count(), 0);
}

#[tokio::test]
async fn stale_results_do_not_hide_a_current_worker_failure() {
    let (mut engine, _resources) = fixture().await;
    let handle = tokio::spawn(async {});
    wait_finished(&handle).await;
    install_body(&mut engine, 0, 1, handle);
    engine
        .historical_fetch_tx
        .send(body_outcome(0, 0, 0))
        .ok()
        .unwrap();
    assert!(engine.drain_historical_fetch_outcomes().await.is_err());
    assert_eq!(engine.active_historical_fetch_count(), 0);
    assert!(engine.historical_fetch_completed.is_empty());
}

async fn stopped_real_fetch_plan(header_failure: bool) {
    let (mut engine, _resources) = fixture().await;
    let headers = (0..64)
        .map(|index| Header {
            number: 99 - index,
            ..Default::default()
        })
        .collect::<Vec<_>>();
    let body_plan = engine
        .peers
        .prepare_bodies_and_receipts_request_for_blocks(
            headers
                .iter()
                .map(ReceiptRequestContext::from_header)
                .collect(),
            None,
            99,
            &[],
        )
        .await
        .unwrap()
        .unwrap();
    engine.spawn_historical_fetch_plan_at_sequence_inner(
        0,
        HistoricalFetchPlan {
            header_batch: HistoricalHeaderBatch {
                child_header: child(),
                header_peer: PeerId::repeat_byte(1),
                hashes: headers.iter().map(Header::hash_slow).collect(),
                headers,
                required_block: 99,
                header_elapsed: Duration::ZERO,
            },
            planned_next_child_header: None,
            body_receipt_plan: body_plan,
        },
        false,
    );
    let header_plan = engine
        .peers
        .prepare_reverse_header_pages_request(100, 1, 1, 99)
        .await
        .unwrap()
        .unwrap();
    engine.spawn_historical_header_fetch_plan(child(), 1, header_plan);
    let before = engine.peers.body_receipt_request_ready_peer_counts();
    let (bodies, receipts) = engine.peers.active_body_receipt_request_counts();
    assert!(
        bodies > 0 && receipts > 0,
        "the real plan must own reservations"
    );
    let handle = if header_failure {
        &engine
            .historical_header_fetch_handle
            .as_ref()
            .unwrap()
            .handle
    } else {
        &engine.historical_fetch_handles[&0]
            .attempts
            .values()
            .next()
            .unwrap()
            .handle
    };
    handle.abort();
    wait_finished(handle).await;
    let result = engine.wait_for_historical_fetch_outcome(&child()).await;
    assert!(result.is_err());
    assert_eq!(engine.active_historical_fetch_count(), 0);
    assert_eq!(engine.peers.active_body_receipt_request_counts(), (0, 0));
    assert_eq!(engine.peers.peer_count(), 3);
    assert_eq!(
        engine.peers.body_receipt_request_ready_peer_counts(),
        before
    );
    // Late owner-retirement messages from canceled work are idempotent.
    tokio::task::yield_now().await;
    engine.drain_historical_request_accounting();
    assert_eq!(engine.peers.active_body_receipt_request_counts(), (0, 0));
}

#[tokio::test]
async fn stopped_body_plan_releases_all_reservations_without_peer_penalty() {
    stopped_real_fetch_plan(false).await;
}

#[tokio::test]
async fn stopped_header_plan_releases_all_reservations_without_peer_penalty() {
    stopped_real_fetch_plan(true).await;
}

#[tokio::test]
async fn stale_header_result_does_not_hide_a_current_worker_failure() {
    let (mut engine, _resources) = fixture().await;
    let handle = tokio::spawn(async {});
    wait_finished(&handle).await;
    install_header(&mut engine, handle);
    let mut outcome = header_outcome(0);
    outcome.attempt = 1;
    engine
        .historical_header_fetch_tx
        .send(outcome)
        .ok()
        .unwrap();
    assert!(
        engine
            .drain_historical_header_fetch_outcomes()
            .await
            .is_err()
    );
    assert_eq!(engine.active_historical_fetch_count(), 0);
}
