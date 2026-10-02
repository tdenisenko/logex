//! Local channels only. Scripted payloads are lifecycle controls, not benchmarks.
use super::*;
use crate::{
    p2p::peer_manager::engine_peer_request_fixture,
    primitives::LogexNetworkPrimitives,
    repair::{RepairFetchLimits, RepairFetchStep, RepairFetcher, RepairRange},
};
use alloy_consensus::{EMPTY_OMMER_ROOT_HASH, EMPTY_ROOT_HASH, Header};
use alloy_eips::BlockHashOrNumber;
use logex_types::ExecutionAnchor;
use reth_eth_wire::{BlockBodies, BlockHeaders, HeadersDirection, Receipts69};
use reth_network::PeerRequest;
use std::{future::Future, task::Poll};
use tokio_util::sync::CancellationToken;

type Receivers = Vec<mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>>;
fn headers() -> Vec<Header> {
    let mut headers = Vec::new();
    for number in 0..4 {
        let header = Header {
            number,
            parent_hash: headers.last().map(Header::hash_slow).unwrap_or_default(),
            timestamp: number + 1,
            gas_limit: 30_000_000,
            transactions_root: EMPTY_ROOT_HASH,
            receipts_root: EMPTY_ROOT_HASH,
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            ..Default::default()
        };
        headers.push(header);
    }
    headers
}

async fn drive<T>(
    future: impl Future<Output = T>,
    service: &mut AuditNetworkService,
    peers: &mut PeerManager,
    receivers: &mut Receivers,
    headers: &[Header],
    counts: &mut [usize; 3],
) -> T {
    let mut future = Box::pin(future);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Poll::Ready(result) = futures_util::poll!(future.as_mut()) {
                return result;
            }
            service.poll(peers, true).await;
            for receiver in &mut *receivers {
                while let Ok(request) = receiver.try_recv() {
                    match request {
                        PeerRequest::GetBlockHeaders { request, response } => {
                            counts[0] += 1;
                            assert_eq!(request.direction, HeadersDirection::Falling);
                            let BlockHashOrNumber::Hash(hash) = request.start_block else {
                                panic!("audit must request authenticated hashes");
                            };
                            let index = headers.iter().position(|h| h.hash_slow() == hash).unwrap();
                            let page = headers[..=index]
                                .iter()
                                .rev()
                                .take(request.limit as usize)
                                .cloned()
                                .collect();
                            response.send(Ok(BlockHeaders(page))).unwrap();
                        }
                        PeerRequest::GetBlockBodies { request, response } => {
                            counts[1] += 1;
                            assert!(request.0.len() <= 4);
                            response
                                .send(Ok(BlockBodies(vec![Default::default(); request.0.len()])))
                                .unwrap();
                        }
                        PeerRequest::GetReceipts69 { request, response } => {
                            counts[2] += 1;
                            assert!(request.0.len() <= 4);
                            response
                                .send(Ok(Receipts69(vec![Vec::new(); request.0.len()])))
                                .unwrap();
                        }
                        _ => panic!("unexpected audit request"),
                    }
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("finite local audit request must complete")
}

#[tokio::test]
async fn maintenance_waits_for_admission_then_fetches_every_empty_block_in_bounded_batches() {
    let (mut peers, mut receivers, _resources) = engine_peer_request_fixture().await;
    let (mut client, mut service) = AuditNetworkService::channel(4).unwrap();
    let chain = headers();
    let tip = &chain[3];
    let mut initial = Box::pin(client.headers(tip.hash_slow(), 1, Duration::from_secs(2), 2));
    assert!(futures_util::poll!(initial.as_mut()).is_pending());
    service.poll(&mut peers, false).await;
    assert!(service.pending.is_none());
    assert!(receivers.iter_mut().all(|r| r.try_recv().is_err()));
    drop(initial);
    service.poll(&mut peers, true).await;
    assert!(
        service.pending.is_none(),
        "cancelled queued request is never dispatched"
    );
    let anchor = ExecutionAnchor {
        block_number: tip.number,
        block_hash: tip.hash_slow(),
        receipts_root: tip.receipts_root,
        beacon_slot: 4,
        beacon_root: B256::repeat_byte(8),
    };
    let mut fetch = RepairFetcher::new(
        RepairRange { start: 0, end: 3 },
        anchor,
        RepairFetchLimits {
            header_page_size: 4,
            max_headers: 4,
            max_transactions_per_block: 100,
            max_encoded_body_bytes: 1024 * 1024,
            max_rows_per_block: 100,
            max_log_data_bytes_per_block: 1024 * 1024,
            request_timeout: Duration::from_secs(2),
            max_attempts: 2,
            deadline: tokio::time::Instant::now() + Duration::from_secs(20),
        },
        CancellationToken::new(),
    )
    .unwrap();
    let mut counts = [0; 3];
    for number in (0..4).rev() {
        let step = drive(
            fetch.next_block(&mut client),
            &mut service,
            &mut peers,
            &mut receivers,
            &chain,
            &mut counts,
        )
        .await
        .unwrap();
        let RepairFetchStep::Block(block) = step else {
            panic!("missing empty block");
        };
        assert_eq!(block.header().number, number);
        assert!(block.rows().is_empty());
    }
    assert!(matches!(
        fetch.next_block(&mut client).await.unwrap(),
        RepairFetchStep::Complete(_)
    ));
    assert_eq!(
        counts,
        [2, 2, 2],
        "paired prefetch amortizes every empty block without skipping it"
    );
    let metrics = client.metrics();
    assert_eq!(
        metrics.header_handoffs, 3,
        "includes the cancelled queued handoff"
    );
    assert_eq!(metrics.payload_handoffs, 2);
    assert_eq!(metrics.delivered_headers, 4);
    assert_eq!(metrics.delivered_payload_blocks, 4);
    assert_eq!(
        metrics.delivered_header_rlp_bytes,
        chain
            .iter()
            .map(|h| alloy_rlp::Encodable::length(h) as u64)
            .sum::<u64>()
    );
    assert!(metrics.delivered_body_rlp_bytes > 0);
    assert!(metrics.delivered_receipt_rlp_bytes > 0);
    service.shutdown(&mut peers).await;
    let stats = peers.execution_network_status();
    assert_eq!(
        (stats.active_body_requests, stats.active_receipt_requests),
        (0, 0)
    );
}

#[tokio::test]
async fn cancellation_and_shutdown_join_owned_requests_and_release_only_their_accounting() {
    let (mut peers, _receivers, _resources) = engine_peer_request_fixture().await;
    let (mut client, mut service) = AuditNetworkService::channel(1).unwrap();
    let chain = headers();
    let mut normal = peers
        .prepare_audit_payloads(vec![ReceiptRequestContext::from_header(&chain[1])], 1, 1)
        .unwrap()
        .unwrap();
    let (normal_tx, mut normal_rx) = mpsc::unbounded_channel();
    let normal_owner = peers.register_body_receipt_plan(&mut normal, normal_tx, true);
    let normal_task = tokio::spawn(normal.execute());
    tokio::task::yield_now().await;
    peers.apply_body_receipt_request_accounting_events(std::iter::from_fn(|| {
        normal_rx.try_recv().ok()
    }));
    let baseline = peers.execution_network_status();
    assert!(baseline.active_body_requests > 0 && baseline.active_receipt_requests > 0);
    let request = RequestKind::Payloads {
        blocks: vec![ReceiptRequestContext::from_header(&chain[0])],
        required: 0,
    };
    let mut waiting = Box::pin(client.request(request, 1, Duration::from_secs(20)));
    assert!(futures_util::poll!(waiting.as_mut()).is_pending());
    service.poll(&mut peers, true).await;
    assert!(service.pending.is_some());
    tokio::task::yield_now().await;
    service.poll(&mut peers, false).await;
    drop(waiting);
    service.poll(&mut peers, false).await;
    let stats = peers.execution_network_status();
    assert_eq!(
        (stats.active_body_requests, stats.active_receipt_requests),
        (
            baseline.active_body_requests,
            baseline.active_receipt_requests
        )
    );
    service.shutdown(&mut peers).await;
    assert!(service.pending.is_none());
    normal_task.abort();
    let _ = normal_task.await;
    peers.apply_body_receipt_request_accounting_events(std::iter::from_fn(|| {
        normal_rx.try_recv().ok()
    }));
    peers.retire_body_receipt_owner(normal_owner);
    let stats = peers.execution_network_status();
    assert_eq!(
        (stats.active_body_requests, stats.active_receipt_requests),
        (0, 0)
    );
    assert!(
        client
            .headers(B256::ZERO, 1, Duration::from_secs(1), 1)
            .await
            .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn handoff_timeout_cancels_before_admission_and_wakes_idle_owner() {
    let (mut peers, _receivers, _resources) = engine_peer_request_fixture().await;
    let (mut client, mut service) = AuditNetworkService::channel(1).unwrap();
    let mut future = Box::pin(client.headers(B256::ZERO, 1, Duration::from_millis(10), 1));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    service.wake.notified().await;
    tokio::time::advance(Duration::from_millis(11)).await;
    assert!(future.await.is_err());
    service.wake.notified().await;
    service.poll(&mut peers, true).await;
    assert!(service.pending.is_none());
    service.shutdown(&mut peers).await;
}

#[tokio::test]
async fn invalid_requests_and_unpaired_or_wrong_receipts_are_rejected_without_network_work() {
    assert!(AuditNetworkService::channel(0).is_err());
    assert!(AuditNetworkService::channel(33).is_err());
    let (mut client, mut service) = AuditNetworkService::channel(1).unwrap();
    assert!(
        client
            .headers(B256::ZERO, 1025, Duration::from_secs(1), 1)
            .await
            .is_err()
    );
    assert!(
        client
            .headers(B256::ZERO, 1, Duration::ZERO, 1)
            .await
            .is_err()
    );
    assert!(
        client
            .headers(B256::ZERO, 1, Duration::from_secs(1), 5)
            .await
            .is_err()
    );
    assert!(
        client
            .body(B256::ZERO, 0, Duration::from_secs(1), 1)
            .await
            .is_err()
    );
    assert!(
        client
            .receipts(&Header::default(), PeerId::ZERO, Duration::ZERO, 1)
            .await
            .is_err()
    );
    client.receipt = Some((B256::repeat_byte(1), (PeerId::ZERO, vec![])));
    assert!(
        client
            .receipts(&Header::default(), PeerId::ZERO, Duration::ZERO, 1)
            .await
            .is_err()
    );
    assert!(service.rx.try_recv().is_err());
}
