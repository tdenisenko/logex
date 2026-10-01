//! Finite local shutdown controls with owned temporary storage and local channels.
use super::*;
use crate::p2p::peer_manager::{empty_body_receipt_outcome, engine_peer_fixture};
use logex_storage::PartitionManagerConfig;

async fn fixture() -> (SyncEngine, watch::Sender<bool>, impl Sized) {
    let (peers, resources) = engine_peer_fixture().await;
    let directory = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: directory.path().to_path_buf(),
        ..Default::default()
    })
    .unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let engine = SyncEngine::new(
        SyncConfig::default(),
        peers,
        Arc::new(RwLock::new(storage)),
        None,
        Arc::new(std::sync::Mutex::new(SyncStatus::default())),
        Arc::new(
            ConsensusStore::open(
                directory.path(),
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .unwrap(),
        ),
        receiver,
    );
    (engine, shutdown, (resources, directory))
}

fn prepared(rejected: bool) -> PreparedHistoricalBatch {
    let rows = if rejected {
        // A tiny local fixture rejected before ingestion: its row precedes the
        // stated floor. This exercises error retention, not peer validation.
        vec![logex_types::LogRow {
            block_number: 99,
            block_hash: B256::ZERO,
            timestamp: 0,
            tx_hash: B256::ZERO,
            tx_index: 0,
            log_index: 0,
            address: alloy_primitives::Address::ZERO,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Default::default(),
            data_len: 0,
            source: logex_types::Source::Receipt,
        }]
    } else {
        Vec::new()
    };
    PreparedHistoricalBatch {
        requested_headers: 1,
        planned_return_blocks: 1,
        header_elapsed: Duration::ZERO,
        body_receipt_elapsed: Duration::ZERO,
        extracted: super::super::ingest::HistoricalExtractedBatch {
            chunks: vec![super::super::ingest::HistoricalExtractedChunk {
                verified_blocks: vec![
                    logex_storage::VerifiedBlockLogs::from_empty_header(&Header {
                        number: 100,
                        ..Default::default()
                    })
                    .unwrap(),
                ],
                row_count: rows.len() as u64,
                rows,
                block_count: 1,
                lowest_header: Header {
                    number: 100,
                    ..Default::default()
                },
                extraction_elapsed: Duration::ZERO,
            }],
        },
        peer_notes: Vec::new(),
        lowest_block: 100,
        highest_block: 100,
        block_count: 1,
        prepare_queue_elapsed: Duration::ZERO,
        validation_elapsed: Duration::ZERO,
        validation_queue_elapsed: Duration::ZERO,
        processing_elapsed: Duration::ZERO,
        residual_batch: None,
    }
}

fn shutdown_write_control(rejected: bool, stop: bool) {
    // A successful non-shutdown control ends at the history target, so it does
    // not request a follow-up header from the intentionally dormant peer fixture.
    let floor = if !stop && !rejected {
        EXECUTION_HISTORY_TARGET_BLOCK
    } else {
        100
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (was_pending, finished_early, result, storage, _resources) = runtime.block_on(async {
        let (mut engine, shutdown, resources) = fixture().await;
        let storage = Arc::clone(&engine.storage);
        let mut batch = prepared(rejected);
        batch.lowest_block = floor;
        batch.highest_block = floor;
        let chunk = &mut batch.extracted.chunks[0];
        chunk.lowest_header.number = floor;
        chunk.verified_blocks = vec![
            logex_storage::VerifiedBlockLogs::from_empty_header(&chunk.lowest_header).unwrap(),
        ];
        let child = Header {
            number: floor + 1,
            parent_hash: chunk.lowest_header.hash_slow(),
            ..Default::default()
        };
        storage
            .write()
            .await
            .ingest_historical_batch(&[], &child)
            .unwrap();
        // Readers allow the pre-write status lookup, but hold the actual write.
        let reader = storage.read().await;
        let mut work =
            Box::pin(engine.ingest_historical_prepare_result(0, None, Ok(Ok(batch)), false));
        let was_pending = futures_util::poll!(work.as_mut()).is_pending();
        if stop {
            shutdown.send(true).unwrap();
        }
        let early = match futures_util::poll!(work.as_mut()) {
            std::task::Poll::Ready(result) => Some(result),
            std::task::Poll::Pending => None,
        };
        let finished_early = early.is_some();
        drop(reader); // Release the owned blocker before awaiting or asserting.
        let result = match early {
            Some(result) => result,
            None => tokio::time::timeout(Duration::from_secs(5), work.as_mut())
                .await
                .unwrap(),
        };
        drop(work);
        (was_pending, finished_early, result, storage, resources)
    });
    drop(runtime); // Observe all started blocking work before inspecting storage.
    assert!(was_pending);
    if rejected {
        let error = result.expect_err("shutdown discarded the pending storage failure");
        assert!(
            error.to_string().contains("row has no verified block"),
            "{error}"
        );
        assert_eq!(
            storage
                .blocking_read()
                .historical_floor()
                .unwrap()
                .block_number,
            floor + 1
        );
    } else {
        assert_eq!(result.unwrap(), !stop);
        assert_eq!(
            storage
                .blocking_read()
                .historical_floor()
                .unwrap()
                .block_number,
            floor
        );
        assert!(
            !finished_early,
            "ordinary shutdown must observe the current write result"
        );
    }
}

#[test]
fn cancellation_controls_shutdown_waits_for_current_write() {
    shutdown_write_control(false, true);
}

#[test]
fn cancellation_controls_shutdown_retains_write_failure() {
    shutdown_write_control(true, true);
}

#[test]
fn cancellation_controls_normal_write_publishes_and_reports_progress() {
    shutdown_write_control(false, false);
}

#[test]
fn cancellation_controls_normal_write_failure_is_retained() {
    shutdown_write_control(true, false);
}

#[tokio::test]
async fn cancellation_controls_already_stopped_does_not_start_a_batch() {
    let (mut engine, shutdown, _resources) = fixture().await;
    shutdown.send(true).unwrap();
    assert!(
        !engine
            .ingest_historical_prepare_result(0, None, Ok(Ok(prepared(false))), false)
            .await
            .unwrap()
    );
    assert_eq!(engine.storage.read().await.historical_floor(), None);
}

struct Completion(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for Completion {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn unusable_lookahead(generation: u64, sequence: u64) -> HistoricalFetchOutcome {
    HistoricalFetchOutcome {
        generation,
        sequence,
        attempt: 0,
        header_batch: HistoricalHeaderBatch {
            child_header: Header::default(),
            header_peer: PeerId::ZERO,
            headers: Vec::new(),
            hashes: Vec::new(),
            required_block: 0,
            header_elapsed: Duration::ZERO,
        },
        body_receipt_elapsed: Duration::ZERO,
        outcome: empty_body_receipt_outcome(),
    }
}

#[tokio::test]
async fn historical_generation_reset_before_write_discards_old_preparation() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    engine.historical_prepare_expected_sequence = 3028;
    engine.historical_fetch_expected_sequence = 3029;
    engine.historical_fetch_completed.insert(
        3029,
        unusable_lookahead(engine.historical_fetch_generation, 3029),
    );
    let progressed = engine
        .ingest_historical_prepare_result(3028, None, Ok(Ok(prepared(false))), false)
        .await
        .unwrap();
    assert!(
        !progressed,
        "a reset invalidates work that has not started writing"
    );
    assert_eq!(engine.storage.read().await.historical_floor(), None);
    assert_eq!(engine.historical_prepare_expected_sequence, 0);
}

#[tokio::test]
async fn historical_generation_reset_aborts_detached_prepare_worker() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    engine.historical_prepare_expected_sequence = 3028;
    engine.historical_fetch_expected_sequence = 3029;
    engine.historical_fetch_completed.insert(
        3029,
        unusable_lookahead(engine.historical_fetch_generation, 3029),
    );
    let (stopped, stopped_rx) = tokio::sync::oneshot::channel();
    let completion = Completion(Some(stopped));
    let task = HistoricalPrepareTask {
        sequence: 3028,
        next_child_header: None,
        handle: AbortOnDropHandle::new(tokio::spawn(async move {
            let _completion = completion;
            std::future::pending::<HistoricalPrepareResult>().await
        })),
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        engine.ingest_historical_prepared_task(task, false),
    )
    .await;
    assert!(
        matches!(result, Ok(Ok(false))),
        "obsolete prepare must stop promptly"
    );
    tokio::time::timeout(Duration::from_secs(2), stopped_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(engine.historical_prepare_handles.is_empty());
    assert_eq!(engine.storage.read().await.historical_floor(), None);
}

async fn generation_reset_during_write(rejected: bool) {
    let (mut engine, _shutdown, _resources) = fixture().await;
    engine.historical_prepare_expected_sequence = 3028;
    engine.historical_fetch_expected_sequence = 3029;
    let mut headers = Vec::new();
    let mut parent_hash = B256::ZERO;
    for number in 0..=101 {
        let header = Header {
            number,
            parent_hash,
            ..Default::default()
        };
        parent_hash = header.hash_slow();
        headers.push(header);
    }
    engine.historical_fetch_next_sequence = 3031;
    engine.historical_fetch_expected_child = Some(headers[100].clone());
    engine.historical_fetch_planned_child = Some(headers[99].clone());
    // Keep the next expected fetch owned so pre-write refill does not itself
    // repair a deliberately incomplete fixture before the write can start.
    engine.historical_fetch_handles.insert(
        3029,
        HistoricalFetchHandle {
            attempts: HashMap::from([(
                0,
                HistoricalFetchAttemptHandle {
                    child_header: headers[100].clone(),
                    owner: 0,
                    handle: AbortOnDropHandle::new(tokio::spawn(std::future::pending())),
                },
            )]),
        },
    );
    let generation = engine.historical_fetch_generation;
    let tx = engine.historical_fetch_tx.clone();
    let (retired, mut retired_rx) = tokio::sync::oneshot::channel();
    let completion = Completion(Some(retired));
    engine.historical_fetch_handles.insert(
        3030,
        HistoricalFetchHandle {
            attempts: HashMap::from([(
                0,
                HistoricalFetchAttemptHandle {
                    child_header: headers[99].clone(),
                    owner: 0,
                    handle: AbortOnDropHandle::new(tokio::spawn(async move {
                        let _completion = completion;
                        std::future::pending::<()>().await;
                    })),
                },
            )]),
        },
    );
    let storage = Arc::clone(&engine.storage);
    let status = Arc::clone(&engine.sync_status);
    let mut batch = prepared(rejected);
    batch.extracted.chunks[0].lowest_header = headers[100].clone();
    batch.extracted.chunks[0].verified_blocks =
        vec![logex_storage::VerifiedBlockLogs::from_empty_header(&headers[100]).unwrap()];
    storage
        .write()
        .await
        .ingest_historical_batch(&[], &headers[101])
        .unwrap();
    let reader = storage.read().await;
    let mut work =
        Box::pin(engine.ingest_historical_prepare_result(3028, None, Ok(Ok(batch)), false));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures_util::poll!(work.as_mut()).is_pending());
            if storage.try_read().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the blocking write must acquire its place in the storage queue");
    assert!(
        status
            .lock()
            .unwrap()
            .execution_network
            .as_ref()
            .unwrap()
            .historical_ingest_active
    );
    // A failed lookahead is processed while the real storage write waits for our
    // reader. Retirement confirms the channel event was consumed before release.
    tx.send(unusable_lookahead(generation, 3030)).ok().unwrap();
    let retirement = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            result = &mut retired_rx => result.unwrap(),
            result = &mut work => panic!("the owned storage write must remain pending: {result:?}"),
        }
    })
    .await;
    drop(reader);
    let result = tokio::time::timeout(Duration::from_secs(5), work.as_mut())
        .await
        .unwrap();
    drop(work);
    retirement.unwrap();
    if rejected {
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("row has no verified block")
        );
        assert_eq!(
            storage
                .read()
                .await
                .historical_floor()
                .unwrap()
                .block_number,
            101
        );
        return;
    }
    assert!(result.unwrap());
    assert!(engine.historical_fetch_generation > generation);
    assert_eq!(
        storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        100
    );
    assert_eq!(engine.historical_prepare_expected_sequence, 0);
    assert_eq!(engine.historical_fetch_expected_sequence, 0);

    // New-generation preparation must be consumable after the old write settles.
    let mut next = prepared(false);
    next.lowest_block = 0;
    next.highest_block = 99;
    next.block_count = 100;
    next.requested_headers = 100;
    next.planned_return_blocks = 100;
    next.extracted.chunks[0].block_count = 100;
    next.extracted.chunks[0].lowest_header = headers[0].clone();
    next.extracted.chunks[0].verified_blocks = headers[..100]
        .iter()
        .map(logex_storage::VerifiedBlockLogs::from_empty_header)
        .collect::<std::io::Result<_>>()
        .unwrap();
    engine.historical_prepare_completed.insert(
        0,
        HistoricalCompletedPrepare {
            next_child_header: None,
            result: Ok(Ok(next)),
        },
    );
    assert!(
        engine
            .ingest_ready_historical_backfill_batches(1)
            .await
            .unwrap()
    );
    assert_eq!(
        storage
            .read()
            .await
            .historical_floor()
            .unwrap()
            .block_number,
        0
    );
}

#[tokio::test]
async fn historical_generation_reset_during_write_keeps_commit_and_allows_new_work() {
    generation_reset_during_write(false).await;
}

#[tokio::test]
async fn historical_generation_reset_during_write_retains_storage_failure() {
    generation_reset_during_write(true).await;
}

#[tokio::test]
async fn historical_generation_reset_recognizes_only_current_prepare_owner() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    engine.historical_fetch_expected_sequence = 2;
    engine.historical_prepare_completed.insert(
        1,
        HistoricalCompletedPrepare {
            next_child_header: None,
            result: Ok(Ok(prepared(false))),
        },
    );
    let owner = HistoricalPrepareOwner {
        generation: engine.historical_fetch_generation,
        sequence: 0,
    };
    assert!(
        !engine
            .recover_historical_sequence_gap(&Header::default(), Some(owner))
            .await
            .unwrap()
    );
    assert_eq!(engine.historical_prepare_completed.len(), 1);
    assert_eq!(engine.historical_fetch_generation, owner.generation);
    // An owner from a different generation must not hide a real missing batch.
    assert!(
        engine
            .recover_historical_sequence_gap(
                &Header::default(),
                Some(HistoricalPrepareOwner {
                    generation: owner.generation.wrapping_add(1),
                    ..owner
                })
            )
            .await
            .unwrap()
    );
    assert!(engine.historical_prepare_completed.is_empty());
}

#[tokio::test]
async fn cancellation_controls_dropping_prepare_wait_cancels_its_worker() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    let (completed, completion) = tokio::sync::oneshot::channel();
    let marker = Completion(Some(completed));
    let task = HistoricalPrepareTask {
        sequence: 0,
        next_child_header: None,
        handle: AbortOnDropHandle::new(tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<HistoricalPrepareResult>().await
        })),
    };
    let cleanup = task.handle.abort_handle();
    let mut wait = Box::pin(engine.ingest_historical_prepared_task(task, false));
    let pending = futures_util::poll!(wait.as_mut()).is_pending();
    drop(wait);
    let canceled = matches!(
        tokio::time::timeout(Duration::from_secs(5), completion).await,
        Ok(Ok(()))
    );
    cleanup.abort(); // Also clean up the original implementation before asserting.
    tokio::task::yield_now().await;
    assert!(pending);
    assert!(
        canceled,
        "dropping the wait detached its primary prepare worker"
    );
}

#[tokio::test]
async fn cancellation_controls_dropping_engine_cancels_fetch_and_prepare_workers() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    let mut completions = Vec::new();
    let mut cleanups = Vec::new();
    let mut pending = || {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        completions.push(receiver);
        let marker = Completion(Some(sender));
        let handle = tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<()>().await;
        });
        cleanups.push(handle.abort_handle());
        handle
    };
    engine.historical_header_fetch_handle = Some(HistoricalHeaderFetchHandle {
        sequence: 0,
        attempt: 0,
        child_header: Header::default(),
        handle: AbortOnDropHandle::new(pending()),
    });
    engine.historical_fetch_handles.insert(
        0,
        HistoricalFetchHandle {
            attempts: HashMap::from([(
                0,
                HistoricalFetchAttemptHandle {
                    child_header: Header::default(),
                    owner: 0,
                    handle: AbortOnDropHandle::new(pending()),
                },
            )]),
        },
    );
    let (sender, receiver) = tokio::sync::oneshot::channel();
    completions.push(receiver);
    let marker = Completion(Some(sender));
    let handle = tokio::spawn(async move {
        let _marker = marker;
        std::future::pending::<HistoricalPrepareResult>().await
    });
    cleanups.push(handle.abort_handle());
    engine.historical_prepare_handles.insert(
        0,
        HistoricalPrepareTask {
            sequence: 0,
            next_child_header: None,
            handle: AbortOnDropHandle::new(handle),
        },
    );
    drop(engine); // These workers have not been polled yet.
    let stopped = matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::try_join_all(completions)
        )
        .await,
        Ok(Ok(_))
    );
    for cleanup in cleanups {
        cleanup.abort();
    }
    tokio::task::yield_now().await;
    assert!(stopped, "dropping the engine detached historical workers");
}

#[tokio::test]
async fn cancellation_controls_closed_owner_stops_prepare_wait() {
    let (mut engine, shutdown, _resources) = fixture().await;
    let task = HistoricalPrepareTask {
        sequence: 0,
        next_child_header: None,
        handle: AbortOnDropHandle::new(tokio::spawn(
            std::future::pending::<HistoricalPrepareResult>(),
        )),
    };
    let cleanup = task.handle.abort_handle();
    drop(shutdown);
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        engine.ingest_historical_prepared_task(task, false),
    )
    .await;
    cleanup.abort();
    assert!(
        !result
            .expect("closed shutdown owner must stop the prepare wait")
            .unwrap()
    );
}

#[tokio::test]
async fn required_consensus_without_execution_anchor_waits_without_ingesting() {
    let (mut engine, shutdown, _resources) = fixture().await;
    let status = Arc::clone(&engine.sync_status);
    let storage = Arc::clone(&engine.storage);
    assert!(engine.consensus.chain_anchors().optimistic_head.is_none());
    assert!(engine.consensus.chain_anchors().finalized_head.is_none());

    // Poll the real entry point until its first consensus wait. The peer fixture
    // holds a dormant localhost listener and local channels; it never polls the
    // network manager or starts discovery, peer connections, or external sync.
    let mut run = Box::pin(engine.run());
    tokio::select! {
        biased;
        result = &mut run => panic!("sync exited before waiting for consensus: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(
        status.lock().unwrap().node_state,
        NodeState::WaitingForConsensus
    );
    assert!(storage.read().await.sync_head().is_none());
    assert_eq!(storage.read().await.total_rows(), 0);
    shutdown.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(5), &mut run)
        .await
        .expect("consensus wait must remain cancellable")
        .unwrap();
}

#[tokio::test]
async fn parent_validation_tracks_partial_anchored_progress() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    let accepted = Header {
        number: 100,
        ..Default::default()
    };
    // Reproduce the helper-visible state after the first anchored block succeeds
    // and a later payload fails: the tracker advanced, but the old batch-final
    // parent cache assignment was skipped. This is not a peer request-loop test.
    engine.head_tracker.track(accepted.clone());
    assert_eq!(engine.expected_parent_for_validation(101), Some(&accepted));
}

#[tokio::test]
async fn parent_validation_follows_restore_rewind_and_number_boundaries() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    let first = Header {
        number: 100,
        ..Default::default()
    };
    let second = Header {
        number: 101,
        parent_hash: first.hash_slow(),
        ..Default::default()
    };
    engine.head_tracker.restore([first.clone(), second.clone()]);
    assert_eq!(engine.expected_parent_for_validation(102), Some(&second));
    assert!(engine.expected_parent_for_validation(101).is_none());
    engine.head_tracker.restore([first.clone()]);
    assert_eq!(engine.expected_parent_for_validation(101), Some(&first));
    assert!(engine.expected_parent_for_validation(102).is_none());
    engine.head_tracker.restore([Header {
        number: u64::MAX,
        ..Default::default()
    }]);
    assert!(engine.expected_parent_for_validation(0).is_none());
    engine.head_tracker.restore([]);
    assert!(engine.expected_parent_for_validation(1).is_none());
}

#[tokio::test]
async fn forward_tracking_rejects_discontinuity_before_mutation() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    let genesis = Header::default();
    engine.track_forward_header(genesis.clone()).unwrap();
    let before = engine.head_tracker.snapshot();
    for incoming in [
        Header {
            number: 100,
            ..Default::default()
        },
        Header {
            number: 1,
            parent_hash: B256::repeat_byte(0x99),
            ..Default::default()
        },
    ] {
        assert!(engine.track_forward_header(incoming).is_err());
        assert_eq!(engine.head_tracker.snapshot(), before);
        assert!(engine.storage.read().await.sync_head().is_none());
    }
    let child = Header {
        number: 1,
        parent_hash: genesis.hash_slow(),
        ..Default::default()
    };
    engine.track_forward_header(child.clone()).unwrap();
    assert_eq!(engine.head_tracker.tip_header(), Some(&child));
    engine.head_tracker.restore([Header {
        number: u64::MAX,
        ..Default::default()
    }]);
    let before = engine.head_tracker.snapshot();
    assert!(engine.track_forward_header(Header::default()).is_err());
    assert_eq!(engine.head_tracker.snapshot(), before);
}

#[tokio::test]
async fn forward_tracking_allows_checkpoint_bootstrap_only_without_tip() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    let checkpoint = Header {
        number: 100,
        ..Default::default()
    };
    engine.track_forward_header(checkpoint.clone()).unwrap();
    assert_eq!(engine.head_tracker.tip_header(), Some(&checkpoint));
    assert!(
        engine
            .track_forward_header(Header {
                number: 110,
                parent_hash: checkpoint.hash_slow(),
                ..Default::default()
            })
            .is_err()
    );
    assert_eq!(engine.head_tracker.tip_header(), Some(&checkpoint));
}

#[tokio::test]
async fn consensus_reorg_publishes_tracker_only_after_storage_accepts_suffix() {
    for persisted_suffix in [false, true] {
        let (mut engine, _shutdown, _resources) = fixture().await;
        let ancestor = Header {
            number: 100,
            ..Default::default()
        };
        let old_tip = Header {
            number: 101,
            parent_hash: ancestor.hash_slow(),
            ..Default::default()
        };
        let replacement = Header {
            timestamp: 1,
            ..old_tip.clone()
        };
        let anchor = |header: &Header, slot| logex_cl::AnchorRecord {
            anchor: ExecutionAnchor {
                beacon_root: B256::repeat_byte(slot as u8),
                beacon_slot: slot,
                block_number: header.number,
                block_hash: header.hash_slow(),
                receipts_root: header.receipts_root,
            },
            finalized: false,
            parent_beacon_root: None,
        };
        let ancestor_anchor = anchor(&ancestor, 1);
        engine
            .consensus
            .append_anchors(vec![ancestor_anchor, anchor(&replacement, 2)])
            .unwrap();
        {
            let mut storage = engine.storage.write().await;
            storage
                .ingest_canonical_batch(
                    &[],
                    &ancestor,
                    std::slice::from_ref(&ancestor),
                    Some(&ancestor_anchor.anchor),
                )
                .unwrap();
            if persisted_suffix {
                storage
                    .ingest_canonical_batch(
                        &[],
                        &old_tip,
                        &[ancestor.clone(), old_tip.clone()],
                        None,
                    )
                    .unwrap();
            }
        }
        engine
            .head_tracker
            .restore([ancestor.clone(), old_tip.clone()]);
        engine.progress.record_block(101, 0);
        let result = engine.reconcile_consensus_reorg().await;
        if persisted_suffix {
            assert!(result.unwrap());
            assert_eq!(engine.head_tracker.tip_header(), Some(&ancestor));
            assert_eq!(engine.current_block(), 100);
        } else {
            assert!(result.is_err());
            assert_eq!(engine.head_tracker.tip_header(), Some(&old_tip));
            assert_eq!(engine.current_block(), 101);
        }
        let storage = engine.storage.read().await;
        assert_eq!(
            storage.sync_head().unwrap().block_hash,
            ancestor.hash_slow()
        );
        assert_eq!(storage.historical_anchor_header(), Some(&ancestor));
        assert_eq!(storage.historical_floor_header(), Some(&ancestor));
    }
}

#[tokio::test]
async fn zero_batch_sizes_fail_before_sync_startup() {
    for header_batch in [false, true] {
        let (mut engine, _shutdown, _resources) = fixture().await;
        if header_batch {
            engine.config.header_batch_size = 0;
        } else {
            engine.config.fetch_batch_size = 0;
        }
        let error = tokio::time::timeout(Duration::from_secs(5), engine.run())
            .await
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains(if header_batch {
            "header_batch_size"
        } else {
            "fetch_batch_size"
        }));
        assert!(engine.head_tracker.is_empty());
        assert!(engine.storage.read().await.sync_head().is_none());
    }
}

#[tokio::test]
async fn pending_selected_lineage_waits_without_progress_and_cancels() {
    let (mut engine, shutdown, _resources) = fixture().await;
    let header = Header {
        number: 100,
        ..Default::default()
    };
    engine.head_tracker.restore([header.clone()]);
    engine
        .storage
        .write()
        .await
        .record_sync_head(100, header.hash_slow(), 0)
        .unwrap();
    engine.progress.record_block(100, 0);
    let status = Arc::clone(&engine.sync_status);
    let storage = Arc::clone(&engine.storage);
    let original_head = storage.read().await.sync_head();
    // Exercise the real pending action branch with a finite local decision;
    // CL snapshot controls separately verify when this decision is produced.
    let mut pending = Box::pin(
        engine.apply_consensus_reorg_decision(ConsensusReorgDecision::PendingMaterialization),
    );
    tokio::select! {
        biased;
        result = &mut pending => panic!("pending lineage did not wait: {result:?}"),
        _ = tokio::task::yield_now() => {}
    }
    assert_eq!(
        status.lock().unwrap().node_state,
        NodeState::WaitingForConsensus
    );
    assert_eq!(status.lock().unwrap().current_block, 100);
    assert_eq!(storage.read().await.sync_head(), original_head);
    assert_eq!(storage.read().await.total_rows(), 0);
    shutdown.send(true).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(5), &mut pending)
            .await
            .unwrap()
            .unwrap()
    );
    drop(pending);
    assert_eq!(engine.head_tracker.tip_header(), Some(&header));
    assert_eq!(storage.read().await.sync_head(), original_head);
}

#[tokio::test]
async fn selected_chain_return_during_queued_reorg_must_preserve_published_suffix() {
    let (mut engine, _shutdown, _resources) = fixture().await;
    let manager = SubscriptionManager::new();
    let mut notifications = manager.subscribe();
    engine.subscriptions = Some(manager);
    let first = Header {
        number: 100,
        timestamp: 1,
        ..Default::default()
    };
    let second = Header {
        number: 101,
        timestamp: 2,
        parent_hash: first.hash_slow(),
        ..Default::default()
    };
    let third = Header {
        number: 102,
        timestamp: 3,
        parent_hash: second.hash_slow(),
        ..Default::default()
    };
    let original = vec![first.clone(), second.clone(), third.clone()];
    let replacement_second = Header {
        timestamp: 12,
        ..second.clone()
    };
    let replacement_third = Header {
        timestamp: 13,
        parent_hash: replacement_second.hash_slow(),
        ..third.clone()
    };
    let record = |header: &Header| logex_cl::AnchorRecord {
        anchor: ExecutionAnchor {
            beacon_root: B256::repeat_byte(header.timestamp as u8),
            beacon_slot: header.timestamp,
            block_number: header.number,
            block_hash: header.hash_slow(),
            receipts_root: header.receipts_root,
        },
        finalized: false,
        parent_beacon_root: None,
    };
    // Storage/cache fixtures deliberately isolate the real reorg publication
    // path. No peer payload, EVM, signed mainnet lineage or live network claim.
    {
        let mut storage = engine.storage.write().await;
        for (index, header) in original.iter().enumerate() {
            let row = logex_types::LogRow {
                block_number: header.number,
                block_hash: header.hash_slow(),
                timestamp: header.timestamp,
                tx_hash: B256::repeat_byte(header.number as u8),
                tx_index: 0,
                log_index: 0,
                address: alloy_primitives::Address::repeat_byte(1),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: Default::default(),
                data_len: 0,
                source: logex_types::Source::Receipt,
            };
            storage
                .ingest_canonical_batch(
                    &[row],
                    header,
                    &original[..=index],
                    Some(&record(header).anchor),
                )
                .unwrap();
        }
        storage.checkpoint_durable().unwrap();
    }
    engine.head_tracker.restore(original.clone());
    engine.progress.record_blocks(102, 3, 3);
    for header in &original {
        engine
            .peers
            .cache_canonical_block(header, &Default::default(), &[]);
    }
    assert!(engine.peers.selection_cached_block(third.hash_slow()));
    let consensus = Arc::clone(&engine.consensus);
    consensus
        .replace_anchors(vec![
            record(&first),
            record(&replacement_second),
            record(&replacement_third),
        ])
        .unwrap();
    let decision = locate_consensus_reorg(&consensus, &original).unwrap();
    let ConsensusReorgDecision::Rewind(ref reorg) = decision else {
        panic!("fixture must select the real rewind path")
    };
    assert_eq!(reorg.retained_headers, vec![first.clone()]);
    assert_eq!(
        reorg.reverted_hashes,
        vec![second.hash_slow(), third.hash_slow()]
    );

    let storage = Arc::clone(&engine.storage);
    let held_read = storage.read().await;
    let mut queued = Box::pin(engine.apply_consensus_reorg_decision(decision));
    // Poll the actual action until fair RwLock admission proves its writer is
    // queued behind our owned reader; do not infer the boundary from a sleep.
    let enqueued = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut queued).is_pending());
            if storage.try_read().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if enqueued.is_err() {
        drop(held_read);
        drop(queued);
        panic!("real reorg writer never queued within finite control timeout");
    }
    // Consensus returns to the already-published original chain before storage
    // admits the old decision. No verified-store shortcut is injected.
    consensus
        .replace_anchors(original.iter().map(record).collect())
        .unwrap();
    assert!(matches!(
        locate_consensus_reorg(&consensus, &original).unwrap(),
        ConsensusReorgDecision::Unchanged
    ));
    drop(held_read);
    let outcome = tokio::time::timeout(Duration::from_secs(5), &mut queued)
        .await
        .unwrap()
        .unwrap();
    drop(queued);
    let tip_cached = engine.peers.selection_cached_block(third.hash_slow());
    let tracked = engine.head_tracker.tip_header().unwrap().number;
    let progress = engine.current_block();
    let storage = storage.read().await;
    let persisted = storage.sync_head().unwrap().block_number;
    let reader = logex_storage::SegmentReader::open(&storage.hot_partition().meta.path).unwrap();
    let rows = reader.read_log_rows(None).unwrap();
    let canonical = reader.read_canonical().unwrap();
    let selected_blocks = rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| {
            canonical
                .is_present(index as u64)
                .then_some(row.block_number)
        })
        .collect::<Vec<_>>();
    eprintln!(
        "REORG_SELECTION_CONTROL: current selected A102; action={outcome}; persisted={persisted}; tracked={tracked}; progress={progress}; canonical={selected_blocks:?}; A102_cached={tip_cached}; physical_rows={}",
        rows.len()
    );
    assert_eq!(
        rows.len(),
        3,
        "physical rows remain; this is not a permanent data loss control"
    );
    assert_eq!(
        (persisted, tracked, progress),
        (102, 102, 102),
        "obsolete queued rewind must not discard current selected progress"
    );
    assert_eq!(selected_blocks, vec![100, 101, 102]);
    assert!(
        tip_cached,
        "obsolete rewind must not evict current selected block"
    );
    assert!(
        notifications.try_recv().is_err(),
        "obsolete queued decision must not publish a removal"
    );
}

#[tokio::test]
async fn committed_reorg_publishes_exact_removal_after_storage_success() {
    for persisted_suffix in [false, true] {
        let (mut engine, _shutdown, _resources) = fixture().await;
        let manager = SubscriptionManager::new();
        let mut receiver = manager.subscribe();
        engine.subscriptions = Some(manager.clone());
        let ancestor = Header {
            number: 100,
            ..Default::default()
        };
        let (payload_header, payload) = super::checkpoint_spool_tests::payload();
        let old_tip = Header {
            number: 101,
            parent_hash: ancestor.hash_slow(),
            timestamp: 0,
            ..payload_header
        };
        let replacement = Header {
            timestamp: 1,
            ..old_tip.clone()
        };
        let record = |header: &Header, slot| logex_cl::AnchorRecord {
            anchor: ExecutionAnchor {
                beacon_root: B256::repeat_byte(slot as u8),
                beacon_slot: slot,
                block_number: header.number,
                block_hash: header.hash_slow(),
                receipts_root: header.receipts_root,
            },
            finalized: false,
            parent_beacon_root: None,
        };
        let ancestor_record = record(&ancestor, 1);
        engine
            .consensus
            .append_anchors(vec![ancestor_record, record(&replacement, 2)])
            .unwrap();
        let mut old_rows = Vec::new();
        logex_storage::VerifiedBlockLogs::verify_and_append(
            &old_tip,
            BlockBody::transactions(&payload.0.1),
            &payload.1.1,
            &mut old_rows,
        )
        .unwrap();
        let row = old_rows.remove(0);
        {
            let mut storage = engine.storage.write().await;
            storage
                .ingest_canonical_batch(
                    &[],
                    &ancestor,
                    std::slice::from_ref(&ancestor),
                    Some(&ancestor_record.anchor),
                )
                .unwrap();
            if persisted_suffix {
                storage
                    .ingest_canonical_batch(
                        std::slice::from_ref(&row),
                        &old_tip,
                        &[ancestor.clone(), old_tip.clone()],
                        None,
                    )
                    .unwrap();
            }
        }
        engine
            .head_tracker
            .restore([ancestor.clone(), old_tip.clone()]);
        engine.progress.record_block(101, 1);
        manager.notify(std::slice::from_ref(&row));
        assert!(!receiver.try_recv().unwrap().removed);
        let result = engine.reconcile_consensus_reorg().await;
        if persisted_suffix {
            assert!(result.unwrap());
            let removal = receiver
                .try_recv()
                .expect("committed canonical retirement must publish removal");
            assert!(removal.removed);
            assert_eq!(removal.rows.len(), 1);
            assert_eq!(removal.rows[0].block_hash, row.block_hash);
            assert_eq!(engine.head_tracker.tip_header(), Some(&ancestor));
            assert_eq!(
                engine.storage.read().await.sync_head().unwrap().block_hash,
                ancestor.hash_slow()
            );
            let mut replacement_rows = Vec::new();
            let verified = logex_storage::VerifiedBlockLogs::verify_and_append(
                &replacement,
                BlockBody::transactions(&payload.0.1),
                &payload.1.1,
                &mut replacement_rows,
            )
            .unwrap();
            let replacement_row = replacement_rows.remove(0);
            let replacement_anchor = record(&replacement, 2).anchor;
            assert!(
                engine
                    .publish_selected_forward_rows(
                        std::slice::from_ref(&replacement_row),
                        std::slice::from_ref(&verified),
                        std::slice::from_ref(&replacement),
                        std::slice::from_ref(&replacement_row.block_hash),
                        &replacement_anchor,
                    )
                    .await
                    .unwrap()
            );
            // Both existing forward callers notify synchronously after this
            // selected-storage helper. Peer fetching is outside this fixture.
            manager.notify(std::slice::from_ref(&replacement_row));
            let addition = receiver.try_recv().unwrap();
            assert!(!addition.removed);
            assert_eq!(addition.rows[0].tx_hash, removal.rows[0].tx_hash);
            assert_eq!(addition.rows[0].log_index, removal.rows[0].log_index);
            assert_ne!(addition.rows[0].block_hash, removal.rows[0].block_hash);
        } else {
            assert!(result.is_err());
            assert!(receiver.try_recv().is_err());
            assert_eq!(engine.head_tracker.tip_header(), Some(&old_tip));
        }
    }
}

// One blocking slot makes the pre-start cancellation boundary deterministic.
#[test]
fn reorg_worker_queued_abort_and_started_completion() {
    for started in [false, true] {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (mut engine, _shutdown, _resources) = fixture().await;
            let manager = SubscriptionManager::new();
            let mut receiver = manager.subscribe();
            engine.subscriptions = Some(manager.clone());
            let ancestor = Header {
                number: 100,
                ..Default::default()
            };
            let old_tip = Header {
                number: 101,
                parent_hash: ancestor.hash_slow(),
                ..Default::default()
            };
            let replacement = Header {
                timestamp: 1,
                ..old_tip.clone()
            };
            let record = |header: &Header, slot| logex_cl::AnchorRecord {
                anchor: ExecutionAnchor {
                    beacon_root: B256::repeat_byte(slot as u8),
                    beacon_slot: slot,
                    block_number: header.number,
                    block_hash: header.hash_slow(),
                    receipts_root: header.receipts_root,
                },
                finalized: false,
                parent_beacon_root: None,
            };
            let ancestor_record = record(&ancestor, 1);
            engine
                .consensus
                .append_anchors(vec![ancestor_record, record(&replacement, 2)])
                .unwrap();
            let row = logex_types::LogRow {
                block_number: 101,
                block_hash: old_tip.hash_slow(),
                timestamp: 0,
                tx_hash: B256::repeat_byte(9),
                tx_index: 0,
                log_index: 0,
                address: alloy_primitives::Address::repeat_byte(1),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: Default::default(),
                data_len: 0,
                source: logex_types::Source::Receipt,
            };
            {
                let mut storage = engine.storage.write().await;
                storage
                    .ingest_canonical_batch(
                        &[],
                        &ancestor,
                        std::slice::from_ref(&ancestor),
                        Some(&ancestor_record.anchor),
                    )
                    .unwrap();
                storage
                    .ingest_canonical_batch(
                        std::slice::from_ref(&row),
                        &old_tip,
                        &[ancestor.clone(), old_tip.clone()],
                        None,
                    )
                    .unwrap();
            }
            engine
                .head_tracker
                .restore([ancestor.clone(), old_tip.clone()]);
            engine.progress.record_block(101, 1);
            manager.notify(std::slice::from_ref(&row));
            assert!(!receiver.try_recv().unwrap().removed);
            let storage = Arc::clone(&engine.storage);
            let held_read = storage.read().await;
            let (release, wait) = std::sync::mpsc::channel();
            let blocker = if started {
                None
            } else {
                let (entered, entry) = tokio::sync::oneshot::channel();
                let task = tokio::task::spawn_blocking(move || {
                    entered.send(()).unwrap();
                    wait.recv().unwrap();
                });
                entry.await.unwrap();
                Some(task)
            };
            let mut action = Box::pin(engine.reconcile_consensus_reorg());
            assert!(futures::poll!(&mut action).is_pending());
            if started {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while storage.try_read().is_ok() {
                        assert!(futures::poll!(&mut action).is_pending());
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                // The blocking worker is waiting on storage, while this single
                // runtime thread continues polling timers and cancellation.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            drop(action);
            drop(held_read);
            if let Some(blocker) = blocker {
                release.send(()).unwrap();
                blocker.await.unwrap();
                // FIFO single-slot barrier confirms the aborted queued job has
                // been passed without starting the transaction.
                tokio::task::spawn_blocking(|| ()).await.unwrap();
                assert!(receiver.try_recv().is_err());
                assert_eq!(
                    storage.read().await.sync_head().unwrap().block_hash,
                    old_tip.hash_slow()
                );
            } else {
                let removal = tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(removal.removed);
                assert_eq!(removal.rows.len(), 1);
                assert_eq!(removal.rows[0].block_hash, old_tip.hash_slow());
                assert_eq!(
                    storage.read().await.sync_head().unwrap().block_hash,
                    ancestor.hash_slow()
                );
            }
        });
    }
}
