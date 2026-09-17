//! Local fixture payloads only. The dormant peer fixture never starts discovery
//! or public network work; these controls exercise spool/pipeline publication.
use super::*;
use crate::p2p::peer_manager::engine_peer_fixture;
use alloy_consensus::{SignableTransaction, TxLegacy, proofs};
use alloy_primitives::{Address, LogData, Signature, U256};
use logex_storage::{PartitionManagerConfig, SegmentReader};
use tempfile::TempDir;

async fn fixture(initial: &Header) -> (SyncEngine, watch::Sender<bool>, impl Sized + use<>) {
    let (peers, resources) = engine_peer_fixture().await;
    let directory = TempDir::new().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: directory.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    storage
        .ingest_canonical_batch(&[], initial, std::slice::from_ref(initial), None)
        .unwrap();
    let (shutdown, receiver) = watch::channel(false);
    let consensus = ConsensusStore::open(
        directory.path(),
        Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    )
    .unwrap();
    let mut engine = SyncEngine::new(
        SyncConfig::default(),
        peers,
        Arc::new(RwLock::new(storage)),
        None,
        Arc::new(std::sync::Mutex::new(SyncStatus::default())),
        Arc::new(consensus),
        receiver,
    );
    engine.head_tracker.restore([initial.clone()]);
    (engine, shutdown, (resources, directory))
}

fn payload() -> (Header, SourcedBodyReceipts) {
    let mut body = reth_ethereum_primitives::BlockBody::default();
    body.transactions.push(
        TxLegacy::default()
            .into_signed(Signature::new(U256::from(1), U256::from(2), false))
            .into(),
    );
    let receipt = crate::primitives::LogexReceipt {
        logs: vec![Log {
            address: Address::repeat_byte(7),
            data: LogData::new_unchecked(vec![B256::repeat_byte(8)], vec![9].into()),
        }],
        ..Default::default()
    };
    let receipt = ReceiptWithBloom {
        logs_bloom: receipt.bloom(),
        receipt,
    };
    let header = Header {
        number: 4_370_000,
        gas_limit: 5000,
        timestamp: 1000,
        transactions_root: body.calculate_tx_root(),
        ommers_hash: body.calculate_ommers_root(),
        receipts_root: proofs::calculate_receipt_root(std::slice::from_ref(&receipt)),
        logs_bloom: receipt.logs_bloom,
        ..Default::default()
    };
    (
        header,
        ((PeerId::ZERO, body), (PeerId::ZERO, vec![receipt])),
    )
}

async fn spool(
    engine: &SyncEngine,
    initial: &Header,
    count: usize,
) -> (HeaderSpoolReader, Vec<Header>, ExecutionAnchor) {
    let directory = engine.storage.read().await.data_dir().to_owned();
    let mut writer = HeaderSpoolWriter::new(directory, initial.clone())
        .await
        .unwrap();
    let mut previous = initial.clone();
    let mut oracle = Vec::new();
    for _ in 0..count {
        let header = Header {
            number: previous.number + 1,
            timestamp: previous.timestamp + 1,
            parent_hash: previous.hash_slow(),
            ..previous.clone()
        };
        oracle.push(header.clone());
        previous = header;
    }
    for page in oracle.chunks(17) {
        writer = writer.append(page.to_vec()).await.unwrap();
    }
    let anchor = ExecutionAnchor {
        beacon_root: B256::repeat_byte(1),
        beacon_slot: 1,
        block_number: previous.number,
        block_hash: previous.hash_slow(),
        receipts_root: previous.receipts_root,
    };
    engine
        .consensus
        .replace_anchors(vec![
            selection_record(initial, 0),
            logex_cl::AnchorRecord {
                anchor,
                finalized: false,
                parent_beacon_root: None,
            },
        ])
        .unwrap();
    (writer.seal(anchor).await.unwrap(), oracle, anchor)
}

#[tokio::test]
async fn checkpoint_spool_payload_chunking_preserves_rows_heads_and_terminal_publication() {
    let (initial, payload) = payload();
    let mut outcomes = Vec::new();
    for (limit, rewind_candidate) in [(128, false), (13, true)] {
        let (mut engine, _shutdown, _resources) = fixture(&initial).await;
        let (mut reader, oracle, anchor) = spool(&engine, &initial, 131).await;
        if rewind_candidate {
            let (read, position, _) = reader.read_chunk(128).await.unwrap();
            reader = read.rewind(position).await.unwrap();
        }
        while reader.remaining() != 0 {
            let (next, _, chunk) = reader.read_chunk(limit).await.unwrap();
            reader = next;
            let fetched = ForwardGapFetchedChunk {
                sequence: 0,
                header_peer: PeerId::ZERO,
                blocks: vec![payload.clone(); chunk.headers.len()],
                headers: chunk.headers,
                hashes: chunk.hashes,
                body_receipt_elapsed: Duration::ZERO,
            };
            assert!(
                engine
                    .ingest_forward_gap_fetched_chunk(fetched, anchor)
                    .await
                    .unwrap()
                    .0
            );
            let storage = engine.storage.read().await;
            assert_eq!(
                storage.chain_anchors().indexed_head,
                (reader.remaining() == 0).then_some(anchor)
            );
        }
        let mut storage = engine.storage.write().await;
        storage.checkpoint_durable().unwrap();
        let rows = SegmentReader::open(&storage.hot_partition().meta.path)
            .unwrap()
            .read_log_rows(None)
            .unwrap();
        assert_eq!(rows.len(), 131);
        assert_eq!(
            rows.iter().map(|row| row.block_number).collect::<Vec<_>>(),
            oracle
                .iter()
                .map(|header| header.number)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            storage.sync_head().unwrap().block_number,
            anchor.block_number
        );
        outcomes.push((
            rows,
            storage.recent_headers().to_vec(),
            storage.chain_anchors(),
        ));
    }
    assert_eq!(outcomes[0], outcomes[1]);
}

#[tokio::test(start_paused = true)]
async fn checkpoint_spool_declined_parallel_plan_falls_back_without_publication() {
    let (initial, _) = payload();
    let (mut engine, _shutdown, _resources) = fixture(&initial).await;
    let (reader, _, anchor) = spool(&engine, &initial, 65).await;
    // No serving peers: parallel planning declines. The sequential request also
    // has no local payload source, so progress must remain at the original tip.
    let result = engine
        .ingest_checkpoint_gap_fetch_pipeline(PeerId::ZERO, reader, anchor, false)
        .await
        .unwrap();
    assert!(!result.0);
    let storage = engine.storage.read().await;
    assert_eq!(storage.sync_head().unwrap().block_number, initial.number);
    assert_eq!(storage.total_rows(), 0);
    assert!(storage.chain_anchors().indexed_head.is_none());
}

#[tokio::test]
async fn checkpoint_spool_rejected_tail_preserves_authenticated_partial_progress() {
    let (initial, payload) = payload();
    let (mut engine, _shutdown, _resources) = fixture(&initial).await;
    let (reader, oracle, anchor) = spool(&engine, &initial, 131).await;
    let (reader, _, first) = reader.read_chunk(128).await.unwrap();
    let fetched = ForwardGapFetchedChunk {
        sequence: 0,
        header_peer: PeerId::ZERO,
        blocks: vec![payload.clone(); first.headers.len()],
        headers: first.headers,
        hashes: first.hashes,
        body_receipt_elapsed: Duration::ZERO,
    };
    assert!(
        engine
            .ingest_forward_gap_fetched_chunk(fetched, anchor)
            .await
            .unwrap()
            .0
    );
    let (_, _, tail) = reader.read_chunk(128).await.unwrap();
    let mut bad_payload = payload;
    bad_payload.1.1[0].receipt.logs.clear();
    let fetched = ForwardGapFetchedChunk {
        sequence: 1,
        header_peer: PeerId::ZERO,
        blocks: vec![bad_payload; tail.headers.len()],
        headers: tail.headers,
        hashes: tail.hashes,
        body_receipt_elapsed: Duration::ZERO,
    };
    assert!(
        !engine
            .ingest_forward_gap_fetched_chunk(fetched, anchor)
            .await
            .unwrap()
            .0
    );
    let storage = engine.storage.read().await;
    assert_eq!(
        storage.sync_head().unwrap().block_number,
        oracle[127].number
    );
    assert_eq!(storage.total_rows(), 128);
    assert!(storage.chain_anchors().indexed_head.is_none());
}

fn selection_record(header: &Header, slot: u64) -> logex_cl::AnchorRecord {
    logex_cl::AnchorRecord {
        anchor: ExecutionAnchor {
            beacon_root: B256::repeat_byte(slot as u8),
            beacon_slot: slot,
            block_number: header.number,
            block_hash: header.hash_slow(),
            receipts_root: header.receipts_root,
        },
        finalized: false,
        parent_beacon_root: None,
    }
}

async fn selection_publication_control(
    depth: usize,
    during_wait: bool,
) -> (SyncEngine, Header, Header, impl Sized) {
    let (initial, payload) = payload();
    let (mut engine, shutdown, resources) = fixture(&initial).await;
    engine.head_tracker = HeadTracker::new(depth);
    engine.head_tracker.restore([initial.clone()]);
    let (reader, oracle, old_anchor) = spool(&engine, &initial, 4).await;
    let consensus = Arc::clone(&engine.consensus);
    let common = selection_record(&initial, 0);
    let old = logex_cl::AnchorRecord {
        anchor: old_anchor,
        finalized: false,
        parent_beacon_root: None,
    };
    consensus.replace_anchors(vec![common, old]).unwrap();
    let captured = consensus.next_anchor_after(initial.number).unwrap();
    assert_eq!(captured, old_anchor);

    let mut new_tip = initial.clone();
    for header in &oracle {
        new_tip = Header {
            parent_hash: new_tip.hash_slow(),
            extra_data: vec![42].into(),
            ..header.clone()
        };
    }
    let (_, _, chunk) = reader.read_chunk(128).await.unwrap();
    let fetched = ForwardGapFetchedChunk {
        sequence: 0,
        header_peer: PeerId::ZERO,
        blocks: vec![payload; chunk.headers.len()],
        headers: chunk.headers,
        hashes: chunk.hashes,
        body_receipt_elapsed: Duration::ZERO,
    };
    let storage = Arc::clone(&engine.storage);
    let held_storage = storage.read().await;
    assert!(storage.try_read().is_ok());
    let mut publication = Box::pin(engine.ingest_forward_gap_fetched_chunk(fetched, captured));
    // Tokio's fair RwLock refuses a new reader once this consumer's writer
    // is queued behind our held read. This proves storage-await placement,
    // rather than merely observing pending parallel validation.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut publication).is_pending());
            if storage.try_read().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("consumer queued for storage within the finite fixture deadline");
    if during_wait {
        consensus
            .replace_anchors(vec![common, selection_record(&new_tip, 2)])
            .unwrap();
        assert_ne!(consensus.anchor_at(captured.block_number), Some(captured));
    }
    drop(held_storage);
    assert_eq!(publication.await.unwrap().0, !during_wait);
    if !during_wait {
        consensus
            .replace_anchors(vec![common, selection_record(&new_tip, 2)])
            .unwrap();
    }
    let published = storage.read().await.sync_head().unwrap();
    eprintln!(
        "SELECTION_CONTROL depth={depth} during_wait={during_wait}: selected={} published={} shared={} retained_first={} rows={}",
        new_tip.hash_slow(),
        published.block_hash,
        initial.number,
        engine.head_tracker.snapshot()[0].number,
        storage.read().await.total_rows()
    );
    assert_eq!(
        published.block_hash,
        if during_wait {
            initial.hash_slow()
        } else {
            captured.block_hash
        }
    );
    (engine, initial, new_tip, (shutdown, resources))
}

#[tokio::test]
async fn selection_change_during_storage_wait_preserves_recoverable_ancestor() {
    let (engine, initial, _selected, _resources) = selection_publication_control(3, true).await;
    assert_eq!(engine.head_tracker.snapshot(), vec![initial.clone()]);
    assert_eq!(engine.storage.read().await.total_rows(), 0);
    let reconciliation = locate_consensus_reorg(&engine.consensus, &engine.head_tracker.snapshot());
    eprintln!("FIXED retained-window reconciliation: {reconciliation:?}");
    assert!(
        reconciliation.is_ok(),
        "obsolete work must not turn a known retained shared ancestor into a window-exceeded error"
    );
}

#[tokio::test]
async fn selection_change_after_publication_remains_recoverable() {
    let (mut engine, initial, _selected, _resources) =
        selection_publication_control(8, false).await;
    let decision =
        locate_consensus_reorg(&engine.consensus, &engine.head_tracker.snapshot()).unwrap();
    assert!(
        matches!(&decision, ConsensusReorgDecision::Rewind(reorg) if reorg.retained_headers.last() == Some(&initial) && reorg.reverted_hashes.len() == 4)
    );
    assert!(
        engine
            .apply_consensus_reorg_decision(decision)
            .await
            .unwrap()
    );
    assert_eq!(
        engine
            .storage
            .read()
            .await
            .sync_head()
            .unwrap()
            .block_number,
        initial.number
    );
}

#[tokio::test]
async fn selection_missing_anchor_after_storage_wait_rejects_single_block_publication() {
    let (initial, payload) = payload();
    let (mut engine, _shutdown, _resources) = fixture(&initial).await;
    let (_, headers, anchor) = spool(&engine, &initial, 1).await;
    let mut rows = Vec::new();
    extract::append_from_body_receipts(
        &mut rows,
        headers[0].number,
        headers[0].hash_slow(),
        headers[0].timestamp,
        &payload.0.1,
        &payload.1.1,
    )
    .unwrap();
    let hashes = vec![headers[0].hash_slow()];
    let storage = Arc::clone(&engine.storage);
    let consensus = Arc::clone(&engine.consensus);
    let held = storage.read().await;
    let mut publish =
        Box::pin(engine.publish_selected_forward_rows(&rows, &headers, &hashes, &anchor));
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            assert!(futures::poll!(&mut publish).is_pending());
            if storage.try_read().is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    consensus.replace_anchors(vec![]).unwrap();
    drop(held);
    assert!(!publish.await.unwrap());
    assert_eq!(engine.head_tracker.snapshot(), vec![initial.clone()]);
    let storage = storage.read().await;
    assert_eq!(storage.total_rows(), 0);
    assert_eq!(storage.sync_head().unwrap().block_hash, initial.hash_slow());
    assert!(!engine.peers.selection_cached_block(headers[0].hash_slow()));
}

#[tokio::test]
async fn selection_storage_rejection_leaves_tracker_unchanged() {
    let (initial, payload) = payload();
    let (mut engine, _shutdown, _resources) = fixture(&initial).await;
    let (_, headers, anchor) = spool(&engine, &initial, 1).await;
    let mut rows = Vec::new();
    extract::append_from_body_receipts(
        &mut rows,
        headers[0].number,
        headers[0].hash_slow(),
        headers[0].timestamp,
        &payload.0.1,
        &payload.1.1,
    )
    .unwrap();
    // Deterministic storage InvalidInput, not an injected physical I/O failure.
    rows[0].block_number += 1;
    assert!(
        engine
            .publish_selected_forward_rows(&rows, &headers, &[headers[0].hash_slow()], &anchor)
            .await
            .is_err()
    );
    assert_eq!(engine.head_tracker.snapshot(), vec![initial]);
    assert_eq!(engine.storage.read().await.total_rows(), 0);
    assert!(!engine.peers.selection_cached_block(headers[0].hash_slow()));
}
