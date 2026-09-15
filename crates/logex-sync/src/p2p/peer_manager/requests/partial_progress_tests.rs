//! Productive partial-response policy controls. Ordinary typed channel replies;
//! the Fixture's loopback listener is dormant, with no peer/service tasks.
use super::limit_tests::{Fixture, receipt_contexts, take_requests};
use super::*;

fn request_limit(manager: &PeerManager, id: PeerId, kind: PeerRequestKind) -> usize {
    let peer = &manager.peers[&id];
    match kind {
        PeerRequestKind::Bodies => peer.body_request_limit,
        PeerRequestKind::Receipts => peer.receipt_request_limit,
        PeerRequestKind::Headers => panic!("payload role required"),
    }
}

// Exercise the same accounting method used by sequential positive partials.
// It is separate from wire fixtures so latency can be specified honestly:
// paused Tokio time does not advance the production std::Instant stopwatch.
fn account_partial(
    manager: &mut PeerManager,
    id: PeerId,
    kind: PeerRequestKind,
    elapsed: Duration,
) {
    manager.record_peer_partial_response(id, kind, "fixture", 8, 2, elapsed);
}

async fn public_positive_prefix(kind: PeerRequestKind, version: EthVersion) {
    let mut fixture = Fixture::new().await;
    for peer in fixture.manager.peers.values_mut() {
        peer.version = version;
    }
    let order_before = fixture.manager.peer_order.clone();
    let hashes: Vec<_> = (1..=8).map(B256::repeat_byte).collect();
    let mut future = Box::pin(async {
        if matches!(kind, PeerRequestKind::Bodies) {
            fixture
                .manager
                .get_bodies_prefer_peers_with_limits(
                    hashes.clone(),
                    1,
                    &[],
                    Duration::from_secs(10),
                    1,
                )
                .await
                .map(|v| v.len())
        } else {
            fixture
                .manager
                .get_receipts_prefer_peers_with_limits(
                    receipt_contexts(&hashes),
                    1,
                    &[],
                    Duration::from_secs(10),
                    1,
                )
                .await
                .map(|v| v.len())
        }
    });
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (index, request) = requests.pop().unwrap();
    match request {
        PeerRequest::GetBlockBodies { request, response } => {
            assert_eq!(request.0, hashes);
            response
                .send(Ok(BlockBodies(vec![Default::default(); 2])))
                .unwrap();
        }
        PeerRequest::GetReceipts { request, response } => {
            assert_eq!(request.0, hashes);
            response.send(Ok(Receipts(vec![Vec::new(); 2]))).unwrap();
        }
        PeerRequest::GetReceipts69 { request, response } => {
            assert_eq!(request.0, hashes);
            response.send(Ok(Receipts69(vec![Vec::new(); 2]))).unwrap();
        }
        _ => panic!("expected selected legacy payload request"),
    }
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut tails = take_requests(&mut fixture.receivers);
    assert_eq!(tails.len(), 1);
    let (tail_index, tail) = tails.pop().unwrap();
    assert_eq!(tail_index, index);
    match &tail {
        PeerRequest::GetBlockBodies { request, .. } => assert_eq!(request.0, hashes[2..]),
        PeerRequest::GetReceipts { request, .. } | PeerRequest::GetReceipts69 { request, .. } => {
            assert_eq!(request.0, hashes[2..])
        }
        _ => panic!("expected suffix request"),
    }
    drop(future);
    let id = PeerId::repeat_byte(index as u8 + 1);
    let peer = &fixture.manager.peers[&id];
    assert_eq!(
        peer.consecutive_timeouts, 0,
        "positive prefix is useful progress, not failure"
    );
    assert!(peer.body_paused_until.is_none());
    assert!(peer.receipt_paused_until.is_none());
    assert!(peer.receipt_quarantined_until.is_none());
    assert_eq!(fixture.manager.peer_order, order_before);
    assert_eq!(request_limit(&fixture.manager, id, kind), 22);
    let closed = match tail {
        PeerRequest::GetBlockBodies { response, .. } => response.is_closed(),
        PeerRequest::GetReceipts { response, .. } => response.is_closed(),
        PeerRequest::GetReceipts69 { response, .. } => response.is_closed(),
        _ => unreachable!(),
    };
    assert!(closed);
}

#[tokio::test(start_paused = true)]
async fn partial_progress_body_prefix_is_not_peer_failure() {
    public_positive_prefix(PeerRequestKind::Bodies, EthVersion::Eth69).await;
}
#[tokio::test(start_paused = true)]
async fn partial_progress_receipts68_prefix_is_not_peer_failure() {
    public_positive_prefix(PeerRequestKind::Receipts, EthVersion::Eth68).await;
}
#[tokio::test(start_paused = true)]
async fn partial_progress_receipts69_prefix_is_not_peer_failure() {
    public_positive_prefix(PeerRequestKind::Receipts, EthVersion::Eth69).await;
}

#[tokio::test(start_paused = true)]
async fn partial_progress_slow_partial_reduces_limit_only_once() {
    for kind in [PeerRequestKind::Bodies, PeerRequestKind::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        account_partial(&mut fixture.manager, id, kind, Duration::from_secs(4));
        assert_eq!(
            request_limit(&fixture.manager, id, kind),
            22,
            "one partial response must not reduce 32 to 15 via both latency and shape"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn partial_progress_fast_partial_still_adapts_limit() {
    for kind in [PeerRequestKind::Bodies, PeerRequestKind::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        account_partial(&mut fixture.manager, id, kind, Duration::from_millis(100));
        assert_eq!(request_limit(&fixture.manager, id, kind), 22);
    }
}

#[tokio::test(start_paused = true)]
async fn partial_progress_complete_success_keeps_existing_latency_policy() {
    for kind in [PeerRequestKind::Bodies, PeerRequestKind::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        fixture
            .manager
            .record_peer_request_success(id, kind, 8, Duration::from_secs(4));
        assert_eq!(request_limit(&fixture.manager, id, kind), 22);
        assert_eq!(fixture.manager.peers[&id].consecutive_timeouts, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn partial_progress_zero_and_real_timeout_remain_failures() {
    for kind in [PeerRequestKind::Bodies, PeerRequestKind::Receipts] {
        for timeout in [false, true] {
            let mut fixture = Fixture::new().await;
            let id = PeerId::repeat_byte(1);
            if timeout {
                assert!(!fixture.manager.on_request_error(
                    id,
                    kind,
                    &RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout)
                ));
            } else {
                fixture
                    .manager
                    .on_zero_progress_response(id, kind, "fixture", 8);
            }
            assert_eq!(fixture.manager.peers[&id].consecutive_timeouts, 1);
            assert_eq!(request_limit(&fixture.manager, id, kind), 22);
            if timeout {
                let peer = &fixture.manager.peers[&id];
                assert!(match kind {
                    PeerRequestKind::Bodies => peer.body_paused_until.is_some(),
                    PeerRequestKind::Receipts => peer.receipt_paused_until.is_some(),
                    _ => unreachable!(),
                });
            }
        }
    }
}

// Append to requests/partial_progress_tests.rs after the production helper exists.
// These are proposed post-change controls, not original-path baseline results.
async fn partial_progress_restores_role_state(kind: PeerRequestKind) {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    let pause = Instant::now() + Duration::from_secs(60);
    let order_before = fixture.manager.peer_order.clone();
    let peer = fixture.manager.peers.get_mut(&id).unwrap();
    peer.consecutive_timeouts = 5;
    peer.is_serving = false;
    peer.body_blocks_per_sec = 8.0;
    peer.receipt_blocks_per_sec = 8.0;
    peer.body_request_limit = 32;
    peer.receipt_request_limit = 32;
    match kind {
        PeerRequestKind::Bodies => peer.body_paused_until = Some(pause),
        PeerRequestKind::Receipts => {
            peer.receipt_paused_until = Some(pause);
            peer.receipt_quarantined_until = Some(pause);
            fixture.manager.receipt_quarantined_peers.insert(id, pause);
        }
        PeerRequestKind::Headers => unreachable!(),
    }

    // Explicit elapsed time exercises the slow-response branch without assuming
    // paused Tokio time also advances production std::Instant measurements.
    fixture.manager.record_peer_partial_response(
        id,
        kind,
        "restoration fixture",
        8,
        2,
        Duration::from_secs(4),
    );

    let peer = &fixture.manager.peers[&id];
    assert_eq!(peer.consecutive_timeouts, 0);
    assert!(peer.is_serving);
    assert!(peer.body_paused_until.is_none());
    assert!(peer.receipt_paused_until.is_none());
    assert!(peer.receipt_quarantined_until.is_none());
    assert!(!fixture.manager.receipt_quarantined_peers.contains_key(&id));
    assert_eq!(fixture.manager.peer_order, order_before);
    assert_eq!(
        request_limit(&fixture.manager, id, kind),
        22,
        "one reduction is ceil(32*2/3), not two reductions to 15"
    );
    let (updated_rate, other_rate, other_limit) = match kind {
        PeerRequestKind::Bodies => (
            peer.body_blocks_per_sec,
            peer.receipt_blocks_per_sec,
            peer.receipt_request_limit,
        ),
        PeerRequestKind::Receipts => (
            peer.receipt_blocks_per_sec,
            peer.body_blocks_per_sec,
            peer.body_request_limit,
        ),
        PeerRequestKind::Headers => unreachable!(),
    };
    // Independent fixed oracle: 0.75*8 + 0.25*(2/4) = 6.125, exactly
    // representable. Updating twice gives a different value.
    assert_eq!(updated_rate, 6.125);
    assert_eq!(other_rate, 8.0);
    assert_eq!(other_limit, 32);
}

#[tokio::test(start_paused = true)]
async fn partial_progress_body_restores_prior_failure_state_once() {
    partial_progress_restores_role_state(PeerRequestKind::Bodies).await;
}

#[tokio::test(start_paused = true)]
async fn partial_progress_receipts_restore_both_quarantines_once() {
    partial_progress_restores_role_state(PeerRequestKind::Receipts).await;
}

#[tokio::test(start_paused = true)]
async fn timeout_counter_saturation_is_defensive_arithmetic() {
    // Synthetic boundary control only: this does not demonstrate that a real
    // session can accumulate u32::MAX timeouts. Normal drop threshold is eight.
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    fixture
        .manager
        .peers
        .get_mut(&id)
        .unwrap()
        .consecutive_timeouts = u32::MAX;
    assert_eq!(fixture.manager.record_timeout(id), MAX_CONSECUTIVE_TIMEOUTS);
    assert_eq!(
        fixture.manager.peers[&id].consecutive_timeouts,
        MAX_CONSECUTIVE_TIMEOUTS
    );
    assert!(fixture.manager.on_request_error(
        id,
        PeerRequestKind::Bodies,
        &RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout),
    ));
    assert_eq!(
        fixture.manager.peers[&id].consecutive_timeouts,
        MAX_CONSECUTIVE_TIMEOUTS
    );
}

#[tokio::test(start_paused = true)]
async fn partial_progress_fast_reply_does_not_increase_before_decreasing() {
    for kind in [PeerRequestKind::Bodies, PeerRequestKind::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        fixture.manager.record_peer_partial_response(
            id,
            kind,
            "fixture",
            64,
            32,
            Duration::from_millis(100),
        );
        assert_eq!(request_limit(&fixture.manager, id, kind), 22);
    }
}
