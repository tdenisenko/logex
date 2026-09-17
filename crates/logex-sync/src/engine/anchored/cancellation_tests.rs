//! Finite local shutdown controls with owned temporary storage and local channels.
use super::*;
use crate::p2p::peer_manager::engine_peer_fixture;
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
        // Readers allow the pre-write status lookup, but hold the actual write.
        let reader = storage.read().await;
        let mut batch = prepared(rejected);
        batch.lowest_block = floor;
        batch.highest_block = floor;
        batch.extracted.chunks[0].lowest_header.number = floor;
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
            error.to_string().contains("historical rows precede"),
            "{error}"
        );
        assert_eq!(storage.blocking_read().historical_floor(), None);
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
