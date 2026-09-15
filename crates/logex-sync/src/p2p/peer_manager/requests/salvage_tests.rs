use super::*;
use std::task::Poll;

// Local channels only: these decoded empty bodies/receipts exercise scheduling,
// not committed blocks. The original std::Instant checks occur BETWEEN awaits;
// paused-time failures below concern uninterrupted healthy continuation, not
// advancing the original outer elapsed clock.
struct Fixture {
    plan: BodyReceiptRequestPlan,
    bodies: mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>,
    receipts: mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>,
    accounting: mpsc::UnboundedReceiver<BodyReceiptRequestAccounting>,
}

impl Fixture {
    fn new(version: EthVersion) -> Self {
        let (body_peer, bodies) = ownership_tests::test_session(PeerId::repeat_byte(1));
        let (mut receipt_peer, receipts) = ownership_tests::test_session(PeerId::repeat_byte(2));
        receipt_peer.version = version;
        let mut plan = ownership_tests::test_plan(&body_peer);
        plan.hashes = (0..9).map(B256::repeat_byte).collect();
        plan.receipt_contexts = limit_tests::receipt_contexts(&plan.hashes);
        plan.ranges = vec![0..8, 8..9];
        plan.range_indices_by_start = HashMap::from([(0, 0), (8, 1)]);
        plan.return_blocks = 8;
        plan.receipt_peer_ids = vec![receipt_peer.sender.peer_id];
        plan.peers.insert(
            receipt_peer.sender.peer_id,
            RequestPeerSnapshot {
                sender: receipt_peer.sender.clone(),
                version,
                metrics: RequestPeerRoleMetrics::from_active_peer(&receipt_peer),
            },
        );
        let (tx, accounting) = mpsc::unbounded_channel();
        plan.accounting_tx = Some(ScopedAccountingSender::new(tx, 7));
        Self {
            plan,
            bodies,
            receipts,
            accounting,
        }
    }
}

fn suffix() -> BTreeMap<usize, Vec<SourcedBodyReceipts>> {
    BTreeMap::from([(
        8,
        vec![(
            (PeerId::repeat_byte(3), Default::default()),
            (PeerId::repeat_byte(4), Vec::new()),
        )],
    )])
}

fn body_response(request: PeerRequest<LogexNetworkPrimitives>, count: usize) {
    let PeerRequest::GetBlockBodies { request, response } = request else {
        panic!("expected body request");
    };
    assert!(count <= request.0.len());
    response
        .send(Ok(BlockBodies(vec![Default::default(); count])))
        .unwrap();
}

fn receipt_prefix(request: PeerRequest<LogexNetworkPrimitives>, count: usize) {
    match request {
        PeerRequest::GetReceipts { request, response } => {
            assert!(count <= request.0.len());
            response
                .send(Ok(Receipts(vec![Vec::new(); count])))
                .unwrap();
        }
        PeerRequest::GetReceipts69 { request, response } => {
            assert!(count <= request.0.len());
            response
                .send(Ok(Receipts69(vec![Vec::new(); count])))
                .unwrap();
        }
        PeerRequest::GetReceipts70 { response, .. } => {
            // Positive receipt cursor progress, with the last block still incomplete.
            response
                .send(Ok(Receipts70 {
                    last_block_incomplete: true,
                    receipts: vec![vec![LogexReceipt::default()]],
                }))
                .unwrap();
        }
        _ => panic!("expected receipt request"),
    }
}

fn assert_closed(request: PeerRequest<LogexNetworkPrimitives>) {
    let closed = match request {
        PeerRequest::GetBlockBodies { response, .. } => response.is_closed(),
        PeerRequest::GetReceipts { response, .. } => response.is_closed(),
        PeerRequest::GetReceipts69 { response, .. } => response.is_closed(),
        PeerRequest::GetReceipts70 { response, .. } => response.is_closed(),
        _ => panic!("unexpected request"),
    };
    assert!(
        closed,
        "salvage expiry must drop the pending response receiver"
    );
}

fn assert_accounting(
    accounting: &mut mpsc::UnboundedReceiver<BodyReceiptRequestAccounting>,
    roles: usize,
    successes: usize,
    expected_timeout_peer: Option<PeerId>,
) {
    let (mut started, mut finished, mut stats, mut failures) = (0, 0, 0, 0);
    while let Ok(event) = accounting.try_recv() {
        assert_eq!(event.owner, 7);
        assert!(!event.retired); // The fixture still owns the plan scope.
        for failure in event.failures {
            assert_eq!(Some(failure.peer_id), expected_timeout_peer);
            assert!(matches!(failure.role, ChunkRequestRole::Bodies));
            assert!(matches!(
                failure.kind,
                ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout
                ))
            ));
            failures += 1;
        }
        stats += event.stats.len();
        for active in event.active_requests {
            match active.delta {
                BodyReceiptActiveRequestDelta::Started => started += 1,
                BodyReceiptActiveRequestDelta::Finished => finished += 1,
            }
        }
    }
    assert_eq!((started, finished, stats), (roles, roles, successes));
    assert_eq!(failures, usize::from(expected_timeout_peer.is_some()));
}

#[tokio::test(start_paused = true)]
async fn salvage_body_partial_progress_cannot_extend_shared_budget() {
    let mut fixture = Fixture::new(EthVersion::Eth69);
    let mut chunks = suffix();
    let original_suffix = chunks.clone();
    let (mut failures, mut stats) = (Vec::new(), Vec::new());
    let mut future = Box::pin(fixture.plan.salvage_live_body_receipt_prefix(
        &mut chunks,
        &mut failures,
        &mut stats,
        8,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    for _ in 0..3 {
        let request = fixture.bodies.try_recv().unwrap();
        tokio::time::advance(Duration::from_secs(3)).await;
        body_response(request, 1);
        assert!(futures_util::poll!(&mut future).is_pending());
    }
    let pending = fixture.bodies.try_recv().unwrap();
    tokio::time::advance(Duration::from_secs(3) + Duration::from_millis(1)).await;
    assert!(
        matches!(futures_util::poll!(&mut future), Poll::Ready(0)),
        "healthy per-wire progress must not extend the 12-second salvage budget"
    );
    drop(future);
    assert_closed(pending);
    assert_eq!(chunks, original_suffix);
    assert!(failures.is_empty());
    assert!(stats.is_empty());
    assert!(fixture.receipts.try_recv().is_err());
    assert_accounting(&mut fixture.accounting, 1, 0, None);
}

async fn check_receipt_budget(version: EthVersion) {
    let mut fixture = Fixture::new(version);
    let mut chunks = suffix();
    let original_suffix = chunks.clone();
    let (mut failures, mut stats) = (Vec::new(), Vec::new());
    let mut future = Box::pin(fixture.plan.salvage_live_body_receipt_prefix(
        &mut chunks,
        &mut failures,
        &mut stats,
        8,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    let request = fixture.bodies.try_recv().unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    body_response(request, 8);
    assert!(futures_util::poll!(&mut future).is_pending());
    for _ in 0..2 {
        let request = fixture.receipts.try_recv().unwrap();
        tokio::time::advance(Duration::from_secs(3)).await;
        receipt_prefix(request, 1);
        assert!(futures_util::poll!(&mut future).is_pending());
    }
    let pending = fixture.receipts.try_recv().unwrap();
    tokio::time::advance(Duration::from_secs(3) + Duration::from_millis(1)).await;
    assert!(
        matches!(futures_util::poll!(&mut future), Poll::Ready(0)),
        "receipt role must not receive a fresh salvage window"
    );
    drop(future);
    assert_closed(pending);
    assert_eq!(chunks, original_suffix);
    assert!(failures.is_empty());
    assert_eq!(stats.len(), 1);
    assert_eq!(stats[0].peer_id, PeerId::repeat_byte(1));
    assert_accounting(&mut fixture.accounting, 2, 1, None);
}

#[tokio::test(start_paused = true)]
async fn salvage_legacy_receipts_share_body_budget() {
    check_receipt_budget(EthVersion::Eth68).await;
}

#[tokio::test(start_paused = true)]
async fn salvage_eth69_receipts_share_body_budget() {
    check_receipt_budget(EthVersion::Eth69).await;
}

#[tokio::test(start_paused = true)]
async fn salvage_eth70_continuation_shares_body_budget() {
    check_receipt_budget(EthVersion::Eth70).await;
}

#[tokio::test(start_paused = true)]
async fn salvage_within_budget_keeps_sources_stats_and_balanced_guards() {
    let mut fixture = Fixture::new(EthVersion::Eth69);
    let mut chunks = suffix();
    let original_suffix = chunks[&8].clone();
    let (mut failures, mut stats) = (Vec::new(), Vec::new());
    let mut future = Box::pin(fixture.plan.salvage_live_body_receipt_prefix(
        &mut chunks,
        &mut failures,
        &mut stats,
        8,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    body_response(fixture.bodies.try_recv().unwrap(), 8);
    assert!(futures_util::poll!(&mut future).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    receipt_prefix(fixture.receipts.try_recv().unwrap(), 8);
    assert!(matches!(futures_util::poll!(&mut future), Poll::Ready(1)));
    drop(future);
    assert_eq!(chunks[&8], original_suffix);
    assert_eq!(chunks[&0].len(), 8);
    for ((body_peer, _), (receipt_peer, _)) in &chunks[&0] {
        assert_eq!(*body_peer, PeerId::repeat_byte(1));
        assert_eq!(*receipt_peer, PeerId::repeat_byte(2));
    }
    assert!(failures.is_empty());
    assert_eq!(stats.len(), 2);
    assert_accounting(&mut fixture.accounting, 2, 2, None);
}

#[tokio::test(start_paused = true)]
async fn salvage_expiry_wins_when_wire_timeout_is_ready_at_same_boundary() {
    let mut fixture = Fixture::new(EthVersion::Eth69);
    let mut chunks = suffix();
    let original_suffix = chunks.clone();
    let (mut failures, mut stats) = (Vec::new(), Vec::new());
    let mut future = Box::pin(fixture.plan.salvage_live_body_receipt_prefix(
        &mut chunks,
        &mut failures,
        &mut stats,
        8,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    for seconds in [3, 3, 2] {
        let request = fixture.bodies.try_recv().unwrap();
        tokio::time::advance(Duration::from_secs(seconds)).await;
        body_response(request, 1);
        assert!(futures_util::poll!(&mut future).is_pending());
    }
    let pending = fixture.bodies.try_recv().unwrap();
    // The pending wire request started at 8s: its 4s timer and salvage 12s timer
    // become ready together. Local expiry must take priority over peer blame.
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(matches!(futures_util::poll!(&mut future), Poll::Ready(0)));
    drop(future);
    assert_closed(pending);
    assert_eq!(chunks, original_suffix);
    assert!(
        failures.is_empty(),
        "local expiry must win the simultaneous timer race"
    );
    assert!(stats.is_empty());
    assert_accounting(&mut fixture.accounting, 1, 0, None);
}

#[tokio::test(start_paused = true)]
async fn salvage_expiry_keeps_a_completed_first_round() {
    let mut fixture = Fixture::new(EthVersion::Eth69);
    fixture.plan.ranges = vec![0..1, 1..8, 8..9];
    fixture.plan.range_indices_by_start = HashMap::from([(0, 0), (1, 1), (8, 2)]);
    let mut chunks = suffix();
    let original_suffix = chunks[&8].clone();
    let (mut failures, mut stats) = (Vec::new(), Vec::new());
    let mut future = Box::pin(fixture.plan.salvage_live_body_receipt_prefix(
        &mut chunks,
        &mut failures,
        &mut stats,
        8,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    body_response(fixture.bodies.try_recv().unwrap(), 1);
    assert!(futures_util::poll!(&mut future).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    receipt_prefix(fixture.receipts.try_recv().unwrap(), 1);
    assert!(futures_util::poll!(&mut future).is_pending());
    for _ in 0..3 {
        let request = fixture.bodies.try_recv().unwrap();
        tokio::time::advance(Duration::from_secs(3)).await;
        body_response(request, 1);
        assert!(futures_util::poll!(&mut future).is_pending());
    }
    let pending = fixture.bodies.try_recv().unwrap();
    tokio::time::advance(Duration::from_secs(1) + Duration::from_millis(1)).await;
    assert!(matches!(futures_util::poll!(&mut future), Poll::Ready(1)));
    drop(future);
    assert_closed(pending);
    assert_eq!(chunks[&8], original_suffix);
    assert_eq!(chunks[&0].len(), 1);
    assert_eq!(chunks[&0][0].0.0, PeerId::repeat_byte(1));
    assert_eq!(chunks[&0][0].1.0, PeerId::repeat_byte(2));
    assert!(!chunks.contains_key(&1));
    assert!(failures.is_empty());
    assert_eq!(stats.len(), 2);
    assert_accounting(&mut fixture.accounting, 3, 2, None);
}

#[tokio::test(start_paused = true)]
async fn salvage_retains_early_wire_timeout_then_succeeds_with_next_peer() {
    let mut fixture = Fixture::new(EthVersion::Eth69);
    let failed_peer = PeerId::repeat_byte(1);
    let winner_peer = PeerId::repeat_byte(5);
    let (peer, mut winner_requests) = ownership_tests::test_session(winner_peer);
    fixture.plan.body_peer_ids.push(winner_peer);
    fixture.plan.peers.insert(
        winner_peer,
        RequestPeerSnapshot {
            sender: peer.sender.clone(),
            version: peer.version,
            metrics: RequestPeerRoleMetrics::from_active_peer(&peer),
        },
    );
    // Two ranges produce rotation2, preserving these equal-score candidates' order.
    let mut chunks = suffix();
    let original_suffix = chunks[&8].clone();
    let (mut failures, mut stats) = (Vec::new(), Vec::new());
    let mut future = Box::pin(fixture.plan.salvage_live_body_receipt_prefix(
        &mut chunks,
        &mut failures,
        &mut stats,
        8,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    let stalled = fixture.bodies.try_recv().unwrap();
    assert!(winner_requests.try_recv().is_err());
    tokio::time::advance(Duration::from_secs(4) + Duration::from_millis(1)).await;
    assert!(futures_util::poll!(&mut future).is_pending());
    assert_closed(stalled);
    let winner = winner_requests.try_recv().unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    body_response(winner, 8);
    assert!(futures_util::poll!(&mut future).is_pending());
    let receipts = fixture.receipts.try_recv().unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    receipt_prefix(receipts, 8);
    assert!(matches!(futures_util::poll!(&mut future), Poll::Ready(1)));
    drop(future);
    assert_eq!(chunks[&8], original_suffix);
    assert_eq!(chunks[&0].len(), 8);
    for ((body_peer, _), (receipt_peer, _)) in &chunks[&0] {
        assert_eq!(*body_peer, winner_peer);
        assert_eq!(*receipt_peer, PeerId::repeat_byte(2));
    }
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].peer_id, failed_peer);
    assert!(matches!(failures[0].role, ChunkRequestRole::Bodies));
    assert_eq!(failures[0].requested, 8);
    assert!(matches!(
        failures[0].kind,
        ChunkFailureKind::Request(RequestAttempt::Request(
            reth_network::p2p::error::RequestError::Timeout
        ))
    ));
    assert_eq!(stats.len(), 2);
    assert_eq!(stats[0].peer_id, winner_peer);
    assert_eq!(stats[1].peer_id, PeerId::repeat_byte(2));
    assert_accounting(&mut fixture.accounting, 3, 2, Some(failed_peer));
}
