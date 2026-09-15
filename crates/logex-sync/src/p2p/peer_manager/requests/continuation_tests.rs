//! Paused-clock policy controls. Fixture's loopback listener is never polled;
//! no discovery, service task, peer connection or real request is started.
use super::limit_tests::{Fixture, take_requests};
use super::*;
use std::task::Poll;

type Request = PeerRequest<LogexNetworkPrimitives>;
fn hashes() -> Vec<B256> {
    (1..=8).map(B256::repeat_byte).collect()
}

async fn fetch(
    manager: &mut PeerManager,
    bodies: bool,
    seconds: u64,
    attempts: usize,
) -> Result<Vec<PeerId>> {
    if bodies {
        manager
            .get_bodies_prefer_peers_with_limits(
                hashes(),
                1,
                &[],
                Duration::from_secs(seconds),
                attempts,
            )
            .await
            .map(|v| v.into_iter().map(|(p, _)| p).collect())
    } else {
        manager
            .get_receipts_prefer_peers_with_limits(
                hashes(),
                1,
                &[],
                Duration::from_secs(seconds),
                attempts,
            )
            .await
            .map(|v| v.into_iter().map(|(p, _)| p).collect())
    }
}
fn take_one(receivers: &mut [mpsc::Receiver<Request>]) -> (usize, Request) {
    let mut requests = take_requests(receivers);
    assert_eq!(requests.len(), 1);
    requests.pop().unwrap()
}
fn count(request: &Request) -> usize {
    match request {
        Request::GetBlockBodies { request, .. } => request.0.len(),
        Request::GetReceipts { request, .. } | Request::GetReceipts69 { request, .. } => {
            request.0.len()
        }
        Request::GetReceipts70 { request, .. } => request.block_hashes.len(),
        _ => panic!("unexpected request"),
    }
}
fn reply(request: Request, complete: bool) {
    let n = if complete { count(&request) } else { 1 };
    match request {
        Request::GetBlockBodies { response, .. } => {
            response
                .send(Ok(BlockBodies(vec![Default::default(); n])))
                .unwrap();
        }
        Request::GetReceipts { response, .. } => {
            response.send(Ok(Receipts(vec![Vec::new(); n]))).unwrap();
        }
        Request::GetReceipts69 { response, .. } => {
            response.send(Ok(Receipts69(vec![Vec::new(); n]))).unwrap();
        }
        Request::GetReceipts70 { response, .. } => {
            response
                .send(Ok(Receipts70 {
                    last_block_incomplete: !complete,
                    receipts: if complete {
                        vec![Vec::new(); n]
                    } else {
                        vec![vec![LogexReceipt::default()]]
                    },
                }))
                .unwrap();
        }
        _ => panic!("unexpected request"),
    }
}
fn closed(request: &Request) -> bool {
    match request {
        Request::GetBlockBodies { response, .. } => response.is_closed(),
        Request::GetReceipts { response, .. } => response.is_closed(),
        Request::GetReceipts69 { response, .. } => response.is_closed(),
        Request::GetReceipts70 { response, .. } => response.is_closed(),
        _ => panic!("unexpected request"),
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerSnapshot {
    timeouts: u32,
    serving: bool,
    body_limit: usize,
    receipt_limit: usize,
    body_paused: bool,
    receipt_paused: bool,
    receipt_quarantined: bool,
}
fn state(manager: &PeerManager) -> Vec<PeerSnapshot> {
    (1..=3)
        .map(|n| {
            let p = &manager.peers[&PeerId::repeat_byte(n)];
            PeerSnapshot {
                timeouts: p.consecutive_timeouts,
                serving: p.is_serving,
                body_limit: p.body_request_limit,
                receipt_limit: p.receipt_request_limit,
                body_paused: p.body_paused_until.is_some(),
                receipt_paused: p.receipt_paused_until.is_some(),
                receipt_quarantined: p.receipt_quarantined_until.is_some(),
            }
        })
        .collect()
}
async fn expiry_run(bodies: bool, version: EthVersion, expire: bool) -> Vec<PeerSnapshot> {
    let mut fixture = Fixture::new().await;
    for peer in fixture.manager.peers.values_mut() {
        peer.version = version;
    }
    let mut future = Box::pin(fetch(&mut fixture.manager, bodies, 10, 1));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let (_, mut request) = take_one(&mut fixture.receivers);
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(9)).await;
        reply(request, false);
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        (_, request) = take_one(&mut fixture.receivers);
    }
    if expire {
        tokio::time::advance(Duration::from_secs(9)).await;
        match futures_util::poll!(future.as_mut()) {
            Poll::Ready(result) => assert!(result.is_err()),
            Poll::Pending => panic!("healthy continuations exceeded the shared 45-second window"),
        }
        assert!(closed(&request));
    }
    drop(future);
    assert!(closed(&request));
    assert!(fixture.manager.receipt_quarantined_peers.is_empty());
    state(&fixture.manager)
}
async fn expiry(bodies: bool, version: EthVersion) {
    // Existing public partial-response accounting is intentionally preserved.
    // Compare expiry with cancellation after exactly the same four fragments.
    let before = expiry_run(bodies, version, false).await;
    let after = expiry_run(bodies, version, true).await;
    assert_eq!(after, before, "local expiry must not add a peer penalty");
}
#[tokio::test(start_paused = true)]
async fn continuation_public_bodies_expire_neutrally() {
    expiry(true, EthVersion::Eth69).await;
}
#[tokio::test(start_paused = true)]
async fn continuation_public_receipts68_expire_neutrally() {
    expiry(false, EthVersion::Eth68).await;
}
#[tokio::test(start_paused = true)]
async fn continuation_public_receipts69_expire_neutrally() {
    expiry(false, EthVersion::Eth69).await;
}
#[tokio::test(start_paused = true)]
async fn continuation_public_receipts70_expire_neutrally() {
    expiry(false, EthVersion::Eth70).await;
}

async fn success(bodies: bool, wire: u64, elapsed: u64) {
    let mut fixture = Fixture::new().await;
    let mut future = Box::pin(fetch(&mut fixture.manager, bodies, wire, 1));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let (index, request) = take_one(&mut fixture.receivers);
    tokio::time::advance(Duration::from_secs(elapsed)).await;
    reply(request, true);
    match futures_util::poll!(future.as_mut()) {
        Poll::Ready(result) => assert_eq!(
            result.unwrap(),
            vec![PeerId::repeat_byte(index as u8 + 1); 8]
        ),
        Poll::Pending => panic!("complete response should be ready"),
    }
}
#[tokio::test(start_paused = true)]
async fn continuation_success_and_long_explicit_wire_budget() {
    for bodies in [true, false] {
        success(bodies, 10, 9).await;
        success(bodies, 60, 50).await;
    }
}

async fn retry(bodies: bool) {
    let mut fixture = Fixture::new().await;
    let mut future = Box::pin(fetch(&mut fixture.manager, bodies, 10, 2));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let (first, mut request) = take_one(&mut fixture.receivers);
    for _ in 0..4 {
        tokio::time::advance(Duration::from_secs(9)).await;
        reply(request, false);
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let (index, next) = take_one(&mut fixture.receivers);
        assert_eq!(index, first);
        request = next;
    }
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    assert!(closed(&request));
    let (second, request) = take_one(&mut fixture.receivers);
    assert_ne!(first, second);
    assert_eq!(count(&request), if bodies { 4 } else { 8 });
    reply(request, true);
    let expected = if bodies {
        [
            vec![PeerId::repeat_byte(first as u8 + 1); 4],
            vec![PeerId::repeat_byte(second as u8 + 1); 4],
        ]
        .concat()
    } else {
        vec![PeerId::repeat_byte(second as u8 + 1); 8]
    };
    match futures_util::poll!(future.as_mut()) {
        Poll::Ready(result) => assert_eq!(result.unwrap(), expected),
        Poll::Pending => panic!("second peer must complete"),
    }
}
#[tokio::test(start_paused = true)]
async fn continuation_retry_bodies_preserves_prefix() {
    retry(true).await;
}
#[tokio::test(start_paused = true)]
async fn continuation_retry_receipts_restarts_prefix() {
    retry(false).await;
}

#[tokio::test(start_paused = true)]
async fn continuation_parallel_helpers_have_whole_attempt_deadline() {
    for (bodies, version) in [
        (true, EthVersion::Eth69),
        (false, EthVersion::Eth68),
        (false, EthVersion::Eth69),
        (false, EthVersion::Eth70),
    ] {
        let mut fixture = Fixture::new().await;
        for peer in fixture.manager.peers.values_mut() {
            peer.version = version;
        }
        let peer = PeerId::repeat_byte(1);
        let mut future = Box::pin(async {
            if bodies {
                fixture
                    .manager
                    .request_bodies_until_complete(peer, hashes())
                    .await
                    .map(|v| v.len())
            } else {
                fixture
                    .manager
                    .request_receipts_until_complete(peer, hashes())
                    .await
                    .map(|v| v.len())
            }
        });
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let (_, mut request) = take_one(&mut fixture.receivers);
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(9)).await;
            reply(request, false);
            assert!(futures_util::poll!(future.as_mut()).is_pending());
            (_, request) = take_one(&mut fixture.receivers);
        }
        tokio::time::advance(Duration::from_secs(9)).await;
        match futures_util::poll!(future.as_mut()) {
            Poll::Ready(result) => assert!(result.is_err()),
            Poll::Pending => panic!("parallel helper exceeded shared continuation window"),
        }
        assert!(closed(&request));
        drop(future);
        assert_eq!(
            state(&fixture.manager),
            vec![
                PeerSnapshot {
                    timeouts: 0,
                    serving: true,
                    body_limit: 32,
                    receipt_limit: 32,
                    body_paused: false,
                    receipt_paused: false,
                    receipt_quarantined: false
                };
                3
            ]
        );
    }
}

#[tokio::test(start_paused = true)]
async fn continuation_local_expiry_is_neutral_and_cannot_hide_real_failure() {
    let mut fixture = Fixture::new().await;
    let before = state(&fixture.manager);
    let peer = PeerId::repeat_byte(1);
    for role in [ChunkRequestRole::Bodies, ChunkRequestRole::Receipts] {
        let local = ChunkRequestFailure {
            peer_id: peer,
            role,
            requested: 8,
            kind: ChunkFailureKind::Request(RequestAttempt::ContinuationDeadline),
        };
        assert!(!request_failure_quarantines_receipt_peer(
            &RequestAttempt::ContinuationDeadline
        ));
        assert!(!chunk_failure_disables_role_peer(&local));
        assert!(disabled_chunk_peers(&[local.clone()], role).is_empty());
        assert!(!fixture.manager.on_request_error(
            peer,
            role.request_kind(),
            &RequestAttempt::ContinuationDeadline
        ));
        let mut dead = HashSet::new();
        fixture
            .manager
            .apply_parallel_chunk_failures("test", vec![local.clone()], &mut dead);
        assert!(dead.is_empty());
        assert_eq!(state(&fixture.manager), before);
        assert!(fixture.manager.receipt_quarantined_peers.is_empty());
        let real = ChunkRequestFailure {
            kind: ChunkFailureKind::Request(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::Timeout,
            )),
            ..local.clone()
        };
        for failures in [
            vec![local.clone(), real.clone()],
            vec![real.clone(), local.clone()],
        ] {
            let merged = coalesce_parallel_chunk_failures(failures);
            assert_eq!(merged.len(), 1);
            assert!(matches!(
                merged[0].kind,
                ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout
                ))
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn continuation_deadline_wins_simultaneous_wire_timeout() {
    for bodies in [true, false] {
        let mut fixture = Fixture::new().await;
        let before = state(&fixture.manager);
        // One 45s wire wait and the 45s whole-peer timer become ready together.
        let mut future = Box::pin(fetch(&mut fixture.manager, bodies, 45, 1));
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let (_, request) = take_one(&mut fixture.receivers);
        tokio::time::advance(Duration::from_secs(45)).await;
        match futures_util::poll!(future.as_mut()) {
            Poll::Ready(result) => assert!(result.is_err()),
            Poll::Pending => panic!("both deadlines are ready"),
        }
        assert!(closed(&request));
        drop(future);
        assert_eq!(state(&fixture.manager), before);
    }
}

#[tokio::test(start_paused = true)]
async fn continuation_partial_completion_within_budget_preserves_sources() {
    for bodies in [true, false] {
        let mut fixture = Fixture::new().await;
        let mut future = Box::pin(fetch(&mut fixture.manager, bodies, 10, 1));
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let (first, request) = take_one(&mut fixture.receivers);
        tokio::time::advance(Duration::from_secs(9)).await;
        reply(request, false);
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let (second, request) = take_one(&mut fixture.receivers);
        assert_eq!(first, second);
        assert_eq!(count(&request), 7);
        tokio::time::advance(Duration::from_secs(9)).await;
        reply(request, true);
        match futures_util::poll!(future.as_mut()) {
            Poll::Ready(result) => assert_eq!(
                result.unwrap(),
                vec![PeerId::repeat_byte(first as u8 + 1); 8]
            ),
            Poll::Pending => panic!("complete partial sequence within budget"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn continuation_earlier_wire_timeout_remains_a_peer_failure() {
    for bodies in [true, false] {
        let mut fixture = Fixture::new().await;
        let mut future = Box::pin(fetch(&mut fixture.manager, bodies, 10, 1));
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let (index, request) = take_one(&mut fixture.receivers);
        tokio::time::advance(Duration::from_secs(10)).await;
        match futures_util::poll!(future.as_mut()) {
            Poll::Ready(result) => assert!(result.is_err()),
            Poll::Pending => panic!("per-wire timeout must still terminate first"),
        }
        assert!(closed(&request));
        drop(future);
        let snapshots = state(&fixture.manager);
        assert_eq!(snapshots[index].timeouts, 1);
        assert!(if bodies {
            snapshots[index].body_paused
        } else {
            snapshots[index].receipt_paused
        });
        assert!(!snapshots[index].receipt_quarantined);
    }
}

#[tokio::test(start_paused = true)]
async fn continuation_parallel_collector_preserves_completed_chunk_and_stats() {
    for bodies in [true, false] {
        let mut fixture = Fixture::new().await;
        let peers = [
            PeerId::repeat_byte(1),
            PeerId::repeat_byte(2),
            PeerId::repeat_byte(3),
        ];
        let mut future = Box::pin(async {
            if bodies {
                let (items, stats, failures) = fixture
                    .manager
                    .request_bodies_parallel_chunks(&peers, limit_tests::hashes())
                    .await
                    .unwrap()
                    .unwrap();
                (
                    items.into_iter().map(|(p, _)| p).collect::<Vec<_>>(),
                    stats,
                    failures,
                )
            } else {
                let (items, stats, failures) = fixture
                    .manager
                    .request_sourced_receipts_parallel_chunks(&peers, limit_tests::hashes())
                    .await
                    .unwrap()
                    .unwrap();
                (
                    items.into_iter().map(|(p, _)| p).collect::<Vec<_>>(),
                    stats,
                    failures,
                )
            }
        });
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let mut requests = take_requests(&mut fixture.receivers);
        assert_eq!(requests.len(), 2);
        // Choose the range beginning at hash1 as the slow range, independent of scheduling order.
        requests.sort_by_key(|(_, r)| match r {
            Request::GetBlockBodies { request, .. } => request.0[0],
            Request::GetReceipts69 { request, .. } => request.0[0],
            _ => panic!("unexpected chunk request"),
        });
        let (slow_index, mut slow) = requests.remove(0);
        let slow_count = count(&slow);
        let (completed_index, completed) = requests.pop().unwrap();
        let completed_count = count(&completed);
        reply(completed, true);
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        for _ in 0..4 {
            tokio::time::advance(Duration::from_secs(9)).await;
            reply(slow, false);
            assert!(futures_util::poll!(future.as_mut()).is_pending());
            let (index, request) = take_one(&mut fixture.receivers);
            assert_eq!(index, slow_index);
            slow = request;
        }
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        assert!(closed(&slow));
        let (winner, retry) = take_one(&mut fixture.receivers);
        assert_eq!(
            count(&retry),
            slow_count,
            "parallel retry restarts only incomplete range"
        );
        reply(retry, true);
        let (sources, stats, failures) = match futures_util::poll!(future.as_mut()) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("completed chunk and successful retry must finish collector"),
        };
        assert_eq!(
            sources,
            [
                vec![PeerId::repeat_byte(winner as u8 + 1); slow_count],
                vec![PeerId::repeat_byte(completed_index as u8 + 1); completed_count]
            ]
            .concat()
        );
        assert_eq!(stats.len(), 2);
        assert_eq!(stats.iter().map(|s| s.blocks).sum::<usize>(), 64);
        assert!(stats.iter().any(
            |s| s.peer_id == PeerId::repeat_byte(completed_index as u8 + 1)
                && s.blocks == completed_count
        ));
        assert_eq!(failures.len(), 1);
        assert!(matches!(
            failures[0].kind,
            ChunkFailureKind::Request(RequestAttempt::ContinuationDeadline)
        ));
    }
}
