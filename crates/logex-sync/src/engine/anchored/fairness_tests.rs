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
    fixture_with_tail(100, EMPTY_OMMER_ROOT_HASH).await
}

async fn fixture_with_tail(
    floor: usize,
    tail_ommers_hash: B256,
) -> (
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
    for number in 0..=(floor + 64) as u64 {
        let header = Header {
            number,
            parent_hash,
            timestamp: number + 1,
            gas_limit: 30_000_000,
            transactions_root: EMPTY_ROOT_HASH,
            receipts_root: EMPTY_ROOT_HASH,
            ommers_hash: if number + 1 == floor as u64 {
                tail_ommers_hash
            } else {
                EMPTY_OMMER_ROOT_HASH
            },
            ..Default::default()
        };
        parent_hash = header.hash_slow();
        headers.push(header);
    }
    validate_reverse_downloaded_headers_with_hashes(
        &headers[floor],
        &headers[..floor].iter().rev().cloned().collect::<Vec<_>>(),
    )
    .unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: directory.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    storage
        .ingest_canonical_batch(&[], &headers[floor], &headers[floor..=floor], None)
        .unwrap();
    storage
        .ingest_historical_batch(&[], &headers[floor])
        .unwrap();
    let consensus = ConsensusStore::open(
        directory.path(),
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )
    .unwrap();
    consensus
        .replace_anchors(headers[floor..].iter().map(anchor).collect())
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
            current_block: floor as u64,
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

#[tokio::test]
async fn fairness_small_nonempty_historical_tail_reaches_genesis() {
    let tail_body = reth_ethereum_primitives::BlockBody {
        ommers: vec![Header::default()],
        ..Default::default()
    };
    let floor = 63;
    let (mut engine, mut requests, shutdown, headers, _resources) =
        fixture_with_tail(floor, tail_body.calculate_ommers_root()).await;
    // An ommer commitment prevents the all-empty-header shortcut, even though
    // these early fixture blocks contain no transactions or log rows.
    let tail_hash = headers[floor - 1].hash_slow();
    let status = Arc::clone(&engine.sync_status);
    let mut run = Box::pin(engine.run());
    let mut historical_requests = 0;
    let mut supplied_tail = false;
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures_util::poll!(run.as_mut()).is_pending());
            for receiver in &mut requests {
                while let Ok(request) = receiver.try_recv() {
                    match request {
                        PeerRequest::GetBlockBodies { request, response } => {
                            let bodies = request
                                .0
                                .iter()
                                .map(|hash| {
                                    if *hash == tail_hash {
                                        supplied_tail = true;
                                        tail_body.clone()
                                    } else {
                                        Default::default()
                                    }
                                })
                                .collect();
                            let _ = response.send(Ok(BlockBodies(bodies)));
                        }
                        request => {
                            if matches!(&request, PeerRequest::GetBlockHeaders { request, .. }
                                if request.direction == HeadersDirection::Falling)
                            {
                                historical_requests += 1;
                            }
                            answer(request, &headers);
                        }
                    }
                }
            }
            let complete = {
                let status = status.lock().unwrap();
                status.current_block == (floor + 64) as u64
                    && status
                        .historical_execution_floor
                        .is_some_and(|floor| floor.block_number == 0)
            };
            if complete {
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
    let storage = engine.storage.read().await;
    assert!(
        result.is_ok(),
        "small history tail stalled: floor={:?}, header_requests={historical_requests}, supplied_tail={supplied_tail}",
        storage.historical_floor()
    );
    assert!(supplied_tail, "the nonempty body must actually be fetched");
    assert_eq!(historical_requests, 1, "do not refetch the validated tail");
    assert_eq!(storage.historical_floor().unwrap().block_number, 0);
    assert_eq!(
        storage.sync_head().unwrap().block_number,
        (floor + 64) as u64
    );
    assert_eq!(storage.total_rows(), 0);
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
async fn historical_partial_payload_retains_gap_before_queued_lookahead() {
    let (mut engine, _requests, _shutdown, headers, _resources) = fixture().await;
    let requested = 64;
    let completed = 24;
    let reverse_headers = headers[36..100].iter().rev().cloned().collect::<Vec<_>>();
    let blocks = (0..completed)
        .map(|_| ((PeerId::ZERO, Default::default()), (PeerId::ZERO, vec![])))
        .collect();
    let outcome = HistoricalFetchOutcome {
        generation: engine.historical_fetch_generation,
        sequence: 0,
        attempt: 0,
        header_batch: HistoricalHeaderBatch {
            child_header: headers[100].clone(),
            header_peer: PeerId::ZERO,
            hashes: reverse_headers.iter().map(Header::hash_slow).collect(),
            headers: reverse_headers,
            required_block: 36,
            header_elapsed: Duration::ZERO,
        },
        body_receipt_elapsed: Duration::ZERO,
        outcome: crate::p2p::peer_manager::body_receipt_prefix_outcome(requested, blocks),
    };
    let (_, batch, next_child) = engine
        .materialize_historical_fetch_outcome(outcome)
        .unwrap()
        .expect("valid short payload");
    assert_eq!(batch.blocks.len(), completed);
    assert_eq!(batch.planned_return_blocks, requested);
    assert_eq!(next_child.unwrap(), headers[36]);
    let residual = batch.residual_batch.as_ref().expect("retain missing range");
    assert_eq!(residual.header_batch.child_header, headers[76]);
    assert_eq!(
        residual.header_batch.headers,
        headers[36..76].iter().rev().cloned().collect::<Vec<_>>()
    );
    let processed = process_historical_batch(batch, std::time::Instant::now())
        .await
        .unwrap()
        .ok()
        .unwrap();
    assert_eq!(processed.block_count, completed);
    assert!(!prepared_historical_batch_can_coalesce(&processed));
    let mut written = write_prepared_historical_batch(processed, Arc::clone(&engine.storage))
        .await
        .unwrap();
    assert_eq!(
        engine
            .storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        76
    );
    assert!(
        engine
            .ingest_historical_residual_batch(written.residual_batch.take().unwrap())
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
        36
    );
    write_prepared_historical_batch(prepared(&headers[..36]), Arc::clone(&engine.storage))
        .await
        .unwrap();
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
async fn historical_single_page_plan_freezes_queued_boundary() {
    let (mut engine, _requests, _shutdown, headers, _resources) =
        fixture_with_tail(1_024, B256::repeat_byte(1)).await;
    let reversed = headers[..1_024].iter().rev().cloned().collect::<Vec<_>>();
    let plan = engine
        .prepare_historical_fetch_plan_from_header_batch(HistoricalHeaderBatch {
            child_header: headers[1_024].clone(),
            header_peer: PeerId::ZERO,
            hashes: reversed.iter().map(Header::hash_slow).collect(),
            headers: reversed,
            required_block: 0,
            header_elapsed: Duration::ZERO,
        })
        .await
        .unwrap()
        .unwrap();
    let boundary = plan.planned_next_child_header.unwrap();
    assert!(plan.header_batch.headers.len() < 1_024);
    assert_eq!(plan.header_batch.headers.last(), Some(&boundary));
    assert_eq!(plan.header_batch.hashes.last(), Some(&boundary.hash_slow()));
    assert_eq!(
        plan.body_receipt_plan.planned_prefix_blocks(),
        plan.header_batch.headers.len()
    );
    assert_eq!(plan.header_batch.required_block, boundary.number());
}

#[tokio::test]
async fn historical_coalescing_rejects_a_gap_between_ready_sequences() {
    let (mut engine, _requests, _shutdown, headers, _resources) = fixture().await;
    let mut first = prepared(&headers[90..100]);
    engine.historical_prepare_completed.insert(
        1,
        HistoricalCompletedPrepare {
            next_child_header: Some(headers[0].clone()),
            result: Ok(Ok(prepared(&headers[..85]))),
        },
    );
    let (merged, child) =
        engine.coalesce_ready_historical_prepares(0, &mut first, Some(headers[90].clone()));
    assert_eq!(
        merged, 1,
        "sequence adjacency does not prove block continuity"
    );
    assert_eq!(child, Some(headers[90].clone()));
    assert_eq!(first.block_count, 10);
    assert!(engine.historical_prepare_completed.contains_key(&1));
}

#[tokio::test]
async fn historical_stale_lookahead_refills_without_publishing_a_gap() {
    let (mut engine, _requests, _shutdown, headers, _resources) = fixture().await;
    let generation = engine.historical_fetch_generation;
    let advanced = engine
        .ingest_historical_prepare_result(
            1,
            Some(headers[0].clone()),
            Ok(Ok(prepared(&headers[..85]))),
            true,
        )
        .await
        .unwrap();
    assert!(!advanced);
    assert_ne!(engine.historical_fetch_generation, generation);
    assert_eq!(
        engine
            .storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        100
    );
    assert_eq!(engine.storage.read().await.total_rows(), 0);
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

#[test]
fn historical_generation_reset_with_new_prepares_does_not_strand_history() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (mut engine, mut requests, shutdown, headers, _resources) = fixture().await;
        engine.historical_prepare_expected_sequence = 3028;
        engine.historical_fetch_expected_sequence = 3029;
        engine.historical_fetch_next_sequence = 3031;
        engine.historical_fetch_expected_child = Some(headers[99].clone());
        engine.historical_fetch_handles.insert(
            3029,
            HistoricalFetchHandle {
                attempts: HashMap::from([(
                    0,
                    HistoricalFetchAttemptHandle {
                        child_header: headers[99].clone(),
                        owner: 0,
                        handle: AbortOnDropHandle::new(tokio::spawn(std::future::pending::<()>())),
                    },
                )]),
            },
        );
        let outcome = HistoricalFetchOutcome {
            generation: engine.historical_fetch_generation,
            sequence: 3029,
            attempt: 0,
            header_batch: HistoricalHeaderBatch {
                child_header: headers[99].clone(),
                header_peer: PeerId::ZERO,
                headers: Vec::new(),
                hashes: Vec::new(),
                required_block: 0,
                header_elapsed: Duration::ZERO,
            },
            body_receipt_elapsed: Duration::ZERO,
            outcome: crate::p2p::peer_manager::empty_body_receipt_outcome(),
        };
        let original_generation = engine.historical_fetch_generation;
        let header_delivery = engine.historical_header_fetch_tx.clone();
        let (header_capture, mut header_results) = mpsc::unbounded_channel();
        engine.historical_header_fetch_tx = header_capture;
        let tx = engine.historical_fetch_tx.clone();
        // Keep the ordered write queued without holding storage. The scheduler
        // can read the old floor and prepare new-generation lookahead meanwhile.
        let (release, wait) = std::sync::mpsc::channel();
        let (started, ready) = tokio::sync::oneshot::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = wait.recv();
        });
        ready.await.unwrap();
        let mut work = Box::pin(engine.ingest_historical_prepare_result(
            3028,
            Some(headers[99].clone()),
            Ok(Ok(prepared(&headers[99..100]))),
            false,
        ));
        assert!(futures_util::poll!(work.as_mut()).is_pending());
        tx.send(outcome).ok().unwrap();
        let overlap = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(futures_util::poll!(work.as_mut()).is_pending());
                for receiver in &mut requests {
                    while let Ok(request) = receiver.try_recv() {
                        answer(request, &headers);
                    }
                }
                if let Ok(outcome) = header_results.try_recv() {
                    assert_eq!(outcome.sequence, 0);
                    assert!(outcome.generation > original_generation);
                    header_delivery.send(outcome).ok().unwrap();
                    // The real result is ready on the engine's channel while its
                    // write remains blocked. Poll through its materialization.
                    assert!(futures_util::poll!(work.as_mut()).is_pending());
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        release.send(()).unwrap();
        blocker.await.unwrap();
        let written = tokio::time::timeout(Duration::from_secs(5), work.as_mut()).await;
        drop(work);
        overlap.expect("new-generation preparation must overlap the queued write");
        assert!(written.unwrap().unwrap());
        assert_eq!(
            engine
                .storage
                .read()
                .await
                .historical_floor()
                .unwrap()
                .block_number,
            99
        );
        assert_eq!(
            engine.historical_prepare_expected_sequence, 0,
            "old write must not overwrite the new generation's ordered cursor"
        );

        engine.historical_header_fetch_tx = header_delivery;
        // Drive the same engine through live catch-up and verified genesis.
        let status = Arc::clone(&engine.sync_status);
        let mut run = Box::pin(engine.run());
        let resumed = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                assert!(futures_util::poll!(run.as_mut()).is_pending());
                for receiver in &mut requests {
                    while let Ok(request) = receiver.try_recv() {
                        answer(request, &headers);
                    }
                }
                let complete = {
                    let status = status.lock().unwrap();
                    status.current_block == 164
                        && status
                            .historical_execution_floor
                            .is_some_and(|floor| floor.block_number == 0)
                };
                if complete {
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
        resumed.expect("the same engine must complete live and historical sync after reset");
        let storage = engine.storage.read().await;
        assert_eq!(storage.historical_floor().unwrap().block_number, 0);
        assert_eq!(storage.sync_head().unwrap().block_number, 164);
    });
}
