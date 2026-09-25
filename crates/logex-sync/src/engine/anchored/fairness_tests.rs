//! Finite local-channel controls for sharing sync time between live and history.
//! No discovery or remote connections are started; storage belongs to each test.
use super::*;
use crate::p2p::peer_manager::engine_peer_request_fixture;
use logex_storage::PartitionManagerConfig;
use reth_eth_wire::{BlockBodies, BlockHeaders, HeadersDirection, Receipts69};
use reth_network::PeerRequest;

type Requests = Vec<mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>>;

fn anchor(header: &Header) -> logex_cl::AnchorRecord {
    logex_cl::AnchorRecord {
        anchor: ExecutionAnchor {
            beacon_root: B256::repeat_byte(header.number as u8),
            beacon_slot: header.number,
            block_number: header.number,
            block_hash: header.hash_slow(),
            receipts_root: header.receipts_root,
        },
        finalized: false,
        parent_beacon_root: None,
    }
}

async fn fixture() -> (
    SyncEngine,
    Requests,
    watch::Sender<bool>,
    Vec<Header>,
    impl Sized,
) {
    let (peers, requests, resources) = engine_peer_request_fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let mut headers = Vec::new();
    let mut parent_hash = B256::ZERO;
    for number in 0..=164 {
        let header = Header {
            number,
            parent_hash,
            timestamp: number + 1,
            gas_limit: 30_000_000,
            transactions_root: EMPTY_ROOT_HASH,
            receipts_root: EMPTY_ROOT_HASH,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            ..Default::default()
        };
        parent_hash = header.hash_slow();
        headers.push(header);
    }
    validate_reverse_downloaded_headers_with_hashes(
        &headers[100],
        &headers[..100].iter().rev().cloned().collect::<Vec<_>>(),
    )
    .unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: directory.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    storage
        .ingest_canonical_batch(&[], &headers[100], &headers[100..101], None)
        .unwrap();
    storage.ingest_historical_batch(&[], &headers[100]).unwrap();
    let consensus = ConsensusStore::open(
        directory.path(),
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )
    .unwrap();
    consensus
        .replace_anchors(headers[100..].iter().map(anchor).collect())
        .unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let engine = SyncEngine::new(
        SyncConfig {
            max_peers: 3,
            ..Default::default()
        },
        peers,
        Arc::new(RwLock::new(storage)),
        None,
        Arc::new(std::sync::Mutex::new(SyncStatus {
            current_block: 100,
            ..Default::default()
        })),
        Arc::new(consensus),
        receiver,
    );
    (engine, requests, shutdown, headers, (resources, directory))
}

#[tokio::test]
async fn fairness_single_page_history_runs_in_background() {
    let (mut engine, _requests, _shutdown, headers, _resources) = fixture().await;
    let spawned = engine
        .try_spawn_historical_header_fetch_plan(headers[100].clone())
        .await
        .unwrap();
    let active = engine.historical_header_fetch_handle.is_some();
    engine.reset_historical_fetch_pipeline();
    assert!(
        spawned && active,
        "single-page history must not force a foreground network wait"
    );
}

#[tokio::test(start_paused = true)]
async fn fairness_wait_yields_without_canceling_historical_fetch() {
    let (mut engine, _requests, _shutdown, headers, _resources) = fixture().await;
    let consensus = Arc::clone(&engine.consensus);
    consensus
        .replace_anchors(vec![anchor(&headers[100])])
        .unwrap();
    let child = headers[100].clone();
    engine.historical_fetch_expected_child = Some(child.clone());
    engine.historical_fetch_handles.insert(
        0,
        HistoricalFetchHandle {
            attempts: HashMap::from([(
                0,
                HistoricalFetchAttemptHandle {
                    child_header: child.clone(),
                    owner: 0,
                    handle: AbortOnDropHandle::new(tokio::spawn(std::future::pending())),
                },
            )]),
        },
    );
    let mut waiting = Box::pin(engine.wait_for_historical_fetch_outcome(&child));
    assert!(futures_util::poll!(waiting.as_mut()).is_pending());
    consensus
        .replace_anchors(headers[100..].iter().map(anchor).collect())
        .unwrap();
    tokio::time::advance(Duration::from_millis(250)).await;
    let result = tokio::time::timeout(Duration::from_secs(1), waiting).await;
    let retained = engine.active_historical_fetch_count();
    engine.reset_historical_fetch_pipeline();
    assert!(
        matches!(result, Ok(Ok(None))),
        "new live work must end the foreground wait"
    );
    assert_eq!(
        retained, 1,
        "yielding must preserve the running historical request"
    );
}

#[tokio::test]
async fn fairness_unavailable_live_gap_keeps_history_moving() {
    let (mut engine, mut requests, shutdown, headers, _resources) = fixture().await;
    engine
        .consensus
        .replace_anchors(vec![anchor(&headers[100]), anchor(&headers[105])])
        .unwrap();
    let status = Arc::clone(&engine.sync_status);
    let mut run = Box::pin(engine.run());
    let mut live_attempts = 0;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures_util::poll!(run.as_mut()).is_pending());
            for receiver in &mut requests {
                while let Ok(request) = receiver.try_recv() {
                    if let PeerRequest::GetBlockHeaders {
                        request: request_header,
                        response,
                    } = request
                    {
                        if request_header.direction == HeadersDirection::Rising {
                            live_attempts += 1;
                            assert!(live_attempts <= 32);
                            let _ = response.send(Ok(BlockHeaders(Vec::new())));
                        } else {
                            answer(
                                PeerRequest::GetBlockHeaders {
                                    request: request_header,
                                    response,
                                },
                                &headers,
                            );
                        }
                    } else {
                        answer(request, &headers);
                    }
                }
            }
            if status
                .lock()
                .unwrap()
                .historical_execution_floor
                .is_some_and(|floor| floor.block_number == 0)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap();
    engine.reset_historical_fetch_pipeline();
    result.expect("unavailable live payloads must not prevent ready historical writes");
    assert!(live_attempts > 0);
    assert_eq!(status.lock().unwrap().current_block, 100);
    assert_eq!(
        engine
            .storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        0
    );
}

#[tokio::test]
async fn fairness_live_batch_precedes_unfinished_history() {
    let (mut engine, mut requests, shutdown, headers, _resources) = fixture().await;
    let mut run = Box::pin(engine.run());
    let first = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            assert!(futures_util::poll!(run.as_mut()).is_pending());
            for receiver in &mut requests {
                if let Ok(request) = receiver.try_recv() {
                    return request;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the local sync loop must request a batch");
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap();
    let PeerRequest::GetBlockHeaders { request, .. } = first else {
        panic!("expected the first header request");
    };
    assert_eq!(
        request.direction,
        HeadersDirection::Rising,
        "available live anchors must not wait for a historical response"
    );
    assert_eq!(
        request.start_block,
        BlockHashOrNumber::Number(headers[101].number)
    );
    assert_eq!(
        request.limit, 32,
        "amortize catch-up requests across a bounded batch"
    );
}

fn answer(request: PeerRequest<LogexNetworkPrimitives>, headers: &[Header]) {
    match request {
        PeerRequest::GetBlockHeaders { request, response } => {
            let start = match request.start_block {
                BlockHashOrNumber::Number(number) => number as usize,
                BlockHashOrNumber::Hash(hash) => {
                    headers.iter().position(|h| h.hash_slow() == hash).unwrap()
                }
            };
            let page = match request.direction {
                HeadersDirection::Rising => headers[start..]
                    .iter()
                    .take(request.limit as usize)
                    .cloned()
                    .collect(),
                HeadersDirection::Falling => headers[..=start]
                    .iter()
                    .rev()
                    .take(request.limit as usize)
                    .cloned()
                    .collect(),
            };
            let _ = response.send(Ok(BlockHeaders(page)));
        }
        PeerRequest::GetBlockBodies { request, response } => {
            let _ = response.send(Ok(BlockBodies(vec![Default::default(); request.0.len()])));
        }
        PeerRequest::GetReceipts69 { request, response } => {
            let _ = response.send(Ok(Receipts69(vec![Vec::new(); request.0.len()])));
        }
        _ => panic!("unexpected local fixture request"),
    }
}

async fn delayed_history_catchup(retry_history: bool) {
    let (mut engine, mut requests, shutdown, headers, _resources) = fixture().await;
    let status = Arc::clone(&engine.sync_status);
    let mut run = Box::pin(engine.run());
    let mut delayed = Vec::new();
    let mut live_requests = 0;
    let mut historical_requests = 0;
    let mut retried = false;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures_util::poll!(run.as_mut()).is_pending());
            let (head, floor) = {
                let status = status.lock().unwrap();
                (
                    status.current_block,
                    status
                        .historical_execution_floor
                        .map(|floor| floor.block_number),
                )
            };
            for receiver in &mut requests {
                while let Ok(request) = receiver.try_recv() {
                    if let PeerRequest::GetBlockHeaders {
                        request: header_request,
                        ..
                    } = &request
                    {
                        if header_request.direction == HeadersDirection::Falling {
                            historical_requests += 1;
                            assert!(
                                historical_requests <= 128,
                                "history must make progress without repeated refetches"
                            );
                            delayed.push(request);
                            continue;
                        }
                        live_requests += 1;
                    }
                    answer(request, &headers);
                }
            }
            if head == 164 {
                for request in delayed.drain(..) {
                    if retry_history && !retried {
                        let PeerRequest::GetBlockHeaders { response, .. } = request else {
                            panic!("only historical headers were delayed");
                        };
                        response.send(Ok(BlockHeaders(Vec::new()))).unwrap();
                        retried = true;
                    } else {
                        answer(request, &headers);
                    }
                }
                if floor == Some(0) {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), run)
        .await
        .unwrap()
        .unwrap();
    engine.reset_historical_fetch_pipeline();
    let final_head = status.lock().unwrap().current_block;
    let final_floor = engine
        .storage
        .read()
        .await
        .historical_floor()
        .map(|floor| floor.block_number);
    assert!(
        result.is_ok(),
        "live/history completion timed out: head={final_head}, floor={final_floor:?}, live_requests={live_requests}, historical_requests={historical_requests}, delayed={}",
        delayed.len()
    );
    assert_eq!(
        live_requests, 2,
        "64 blocks need two bounded header exchanges"
    );
    assert_eq!(
        historical_requests,
        if retry_history { 2 } else { 1 },
        "a single-page fetch must try another peer only when necessary"
    );
    let storage = engine.storage.read().await;
    assert_eq!(storage.sync_head().unwrap().block_number, 164);
    assert_eq!(storage.historical_floor().unwrap().block_number, 0);
    assert_eq!(
        storage.total_rows(),
        0,
        "empty blocks still advance verified coverage"
    );
}

#[tokio::test]
async fn fairness_live_catches_up_with_delayed_history_then_history_reaches_genesis() {
    delayed_history_catchup(false).await;
}

#[tokio::test]
async fn fairness_single_page_history_retries_an_unavailable_peer() {
    delayed_history_catchup(true).await;
}

fn prepared(headers: &[Header]) -> PreparedHistoricalBatch {
    let lowest = headers.first().unwrap();
    let highest = headers.last().unwrap();
    PreparedHistoricalBatch {
        requested_headers: headers.len(),
        planned_return_blocks: headers.len(),
        header_elapsed: Duration::ZERO,
        body_receipt_elapsed: Duration::ZERO,
        extracted: super::super::ingest::HistoricalExtractedBatch {
            chunks: vec![super::super::ingest::HistoricalExtractedChunk {
                rows: Vec::new(),
                row_count: 0,
                block_count: headers.len(),
                lowest_header: lowest.clone(),
                extraction_elapsed: Duration::ZERO,
            }],
        },
        peer_notes: Vec::new(),
        lowest_block: lowest.number,
        highest_block: highest.number,
        block_count: headers.len(),
        prepare_queue_elapsed: Duration::ZERO,
        validation_elapsed: Duration::ZERO,
        validation_queue_elapsed: Duration::ZERO,
        processing_elapsed: Duration::ZERO,
        residual_batch: None,
    }
}

#[tokio::test]
async fn fairness_yielded_prepare_keeps_its_worker_and_publishes_after_completion() {
    let (mut engine, _requests, _shutdown, headers, _resources) = fixture().await;
    let (release, receiver) = tokio::sync::oneshot::channel();
    let task = HistoricalPrepareTask {
        sequence: 0,
        next_child_header: None,
        handle: AbortOnDropHandle::new(tokio::spawn(async move { receiver.await.unwrap() })),
    };
    let yielded = tokio::time::timeout(
        Duration::from_secs(1),
        engine.ingest_historical_prepared_task(task, false),
    )
    .await;
    assert!(matches!(yielded, Ok(Ok(false))));
    let retained = engine
        .historical_prepare_handles
        .remove(&0)
        .expect("the pending worker remains owned");
    assert!(!retained.handle.is_finished());
    release
        .send(Ok(Ok(prepared(&headers[..100]))))
        .ok()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !retained.handle.is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        engine
            .ingest_historical_prepared_task(retained, false)
            .await
            .unwrap()
    );
    engine.reset_historical_fetch_pipeline();
    assert_eq!(
        engine
            .storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        0
    );
}

#[tokio::test]
async fn fairness_ready_history_yields_after_one_coalesced_write_and_keeps_the_rest() {
    let (mut engine, _requests, _shutdown, headers, _resources) = fixture().await;
    // Fetching has reached genesis; the completed prepares still need to commit.
    engine.historical_fetch_expected_sequence = 9;
    engine.historical_fetch_next_sequence = 9;
    engine.historical_fetch_expected_child = Some(headers[0].clone());
    engine.historical_fetch_planned_child = None;
    for sequence in 0..8 {
        let header = &headers[99 - sequence as usize];
        engine.historical_prepare_completed.insert(
            sequence,
            HistoricalCompletedPrepare {
                next_child_header: Some(header.clone()),
                result: Ok(Ok(prepared(std::slice::from_ref(header)))),
            },
        );
    }
    engine.historical_prepare_completed.insert(
        8,
        HistoricalCompletedPrepare {
            next_child_header: Some(headers[0].clone()),
            result: Ok(Ok(prepared(&headers[..92]))),
        },
    );
    let generation = engine.historical_fetch_generation;
    assert!(
        engine
            .ingest_ready_historical_backfill_batches(8)
            .await
            .unwrap()
    );
    assert_eq!(
        engine
            .storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        96
    );
    assert_eq!(engine.historical_prepare_completed.len(), 5);
    assert_eq!(engine.historical_fetch_generation, generation);
    // Once live has caught up, the remaining ordered batch can commit normally.
    engine.sync_status.lock().unwrap().current_block = 164;
    assert!(
        engine
            .ingest_ready_historical_backfill_batches(8)
            .await
            .unwrap()
    );
    engine.reset_historical_fetch_pipeline();
    assert_eq!(
        engine
            .storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        0
    );
    assert!(engine.historical_prepare_completed.is_empty());
}
