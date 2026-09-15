use super::*;

pub(super) fn test_session(
    id: PeerId,
) -> (
    ActivePeer,
    mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>,
) {
    let (sender, receiver) = mpsc::channel(1);
    let now = Instant::now();
    let peer = ActivePeer {
        sender: PeerRequestSender::new(id, sender),
        remote_record: NodeRecord::new_with_ports("127.0.0.1".parse().unwrap(), 30303, None, id),
        remote_record_is_dialable: true,
        remote_status: UnifiedStatus::default(),
        client_version: Arc::from("ownership test"),
        version: EthVersion::Eth69,
        is_serving: false,
        consecutive_timeouts: 2,
        header_blocks_per_sec: 0.0,
        body_blocks_per_sec: 0.0,
        receipt_blocks_per_sec: 0.0,
        body_active_requests: 0,
        receipt_active_requests: 0,
        body_reserved_requests: 0,
        receipt_reserved_requests: 0,
        body_request_limit: 4,
        receipt_request_limit: 4,
        body_paused_until: Some(now + Duration::from_secs(10)),
        receipt_paused_until: Some(now + Duration::from_secs(10)),
        receipt_quarantined_until: Some(now + Duration::from_secs(10)),
        connected_at: now,
    };
    (peer, receiver)
}

fn totals(peer: &ActivePeer) -> (usize, usize, usize, usize) {
    (
        peer.body_reserved_requests,
        peer.body_active_requests,
        peer.receipt_reserved_requests,
        peer.receipt_active_requests,
    )
}

pub(super) fn test_plan(peer: &ActivePeer) -> BodyReceiptRequestPlan {
    let id = peer.sender.peer_id;
    BodyReceiptRequestPlan {
        hashes: vec![B256::ZERO],
        ranges: std::iter::once(0..1).collect(),
        range_indices_by_start: HashMap::from([(0, 0)]),
        return_blocks: 1,
        body_peer_ids: vec![id],
        receipt_peer_ids: vec![id],
        max_in_flight: 1,
        body_max_in_flight: 1,
        receipt_max_in_flight: 1,
        peer_rotation: 0,
        priority: BodyReceiptRequestPriority::Full,
        peers: HashMap::from([(
            id,
            RequestPeerSnapshot {
                sender: peer.sender.clone(),
                version: peer.version,
                metrics: RequestPeerRoleMetrics::from_active_peer(peer),
            },
        )]),
        accounting_tx: None,
    }
}

fn apply(
    ledger: &mut BodyReceiptOwnerLedger,
    peers: &mut HashMap<PeerId, ActivePeer>,
    owner: u64,
    peer_id: PeerId,
    kind: PeerRequestKind,
    delta: BodyReceiptActiveRequestDelta,
) {
    ledger.apply_delta(
        peers,
        owner,
        BodyReceiptActiveRequest {
            peer_id,
            kind,
            delta,
        },
    );
}

#[test]
fn overlapping_plans_preserve_shared_capacity_when_one_retires() {
    use BodyReceiptActiveRequestDelta::{Finished, Started};
    use PeerRequestKind::{Bodies, Receipts};

    let id = PeerId::repeat_byte(1);
    let (peer, _requests) = test_session(id);
    let mut first = test_plan(&peer);
    let mut second = test_plan(&peer);
    let mut peers = HashMap::from([(id, peer)]);
    let mut ledger = BodyReceiptOwnerLedger::default();
    let (tx, _accounting) = mpsc::unbounded_channel();
    let a = ledger.register(&mut peers, &mut first, tx.clone(), true);
    let b = ledger.register(&mut peers, &mut second, tx, true);
    assert_ne!(a, b);
    assert_eq!(totals(&peers[&id]), (2, 0, 2, 0));

    apply(&mut ledger, &mut peers, a, id, Bodies, Started);
    assert_eq!(totals(&peers[&id]), (1, 1, 2, 0));
    apply(&mut ledger, &mut peers, a, id, Bodies, Finished);
    ledger.retire(&mut peers, a);
    assert_eq!(totals(&peers[&id]), (1, 0, 1, 0));

    // Duplicate retirement and late events cannot consume the second plan.
    ledger.retire(&mut peers, a);
    apply(&mut ledger, &mut peers, a, id, Receipts, Started);
    apply(&mut ledger, &mut peers, a, id, Bodies, Finished);
    assert_eq!(totals(&peers[&id]), (1, 0, 1, 0));

    apply(&mut ledger, &mut peers, b, id, Bodies, Started);
    apply(&mut ledger, &mut peers, b, id, Receipts, Started);
    apply(&mut ledger, &mut peers, b, id, Bodies, Started);
    assert_eq!(totals(&peers[&id]), (0, 2, 0, 1));
    ledger.retire(&mut peers, b);
    assert_eq!(totals(&peers[&id]), (0, 0, 0, 0));
    assert!(ledger.owners.is_empty());
}

#[test]
fn reset_and_reconnect_preserve_replacement_charges() {
    use BodyReceiptActiveRequestDelta::{Finished, Started};
    use PeerRequestKind::Bodies;

    let id = PeerId::repeat_byte(2);
    let (peer, _old_requests) = test_session(id);
    let mut first = test_plan(&peer);
    let mut peers = HashMap::from([(id, peer)]);
    let mut ledger = BodyReceiptOwnerLedger::default();
    let (tx, _accounting) = mpsc::unbounded_channel();
    let a = ledger.register(&mut peers, &mut first, tx.clone(), true);
    apply(&mut ledger, &mut peers, a, id, Bodies, Started);
    ledger.reset(&mut peers);
    assert_eq!(totals(&peers[&id]), (0, 0, 0, 0));
    assert!(ledger.owners.is_empty());

    let mut second = test_plan(&peers[&id]);
    let b = ledger.register(&mut peers, &mut second, tx.clone(), true);
    assert_ne!(a, b);
    apply(&mut ledger, &mut peers, b, id, Bodies, Started);
    apply(&mut ledger, &mut peers, a, id, Bodies, Finished);
    ledger.retire(&mut peers, a);
    assert_eq!(totals(&peers[&id]), (0, 1, 1, 0));

    let (replacement, _new_requests) = test_session(id);
    let mut third = test_plan(&replacement);
    peers.insert(id, replacement);
    let c = ledger.register(&mut peers, &mut third, tx, true);
    apply(&mut ledger, &mut peers, c, id, Bodies, Started);
    apply(&mut ledger, &mut peers, b, id, Bodies, Finished);
    apply(&mut ledger, &mut peers, b, id, Bodies, Started);
    ledger.retire(&mut peers, b);
    assert_eq!(totals(&peers[&id]), (0, 1, 1, 0));
    assert!(peers[&id].body_paused_until.is_some());
    assert!(peers[&id].receipt_quarantined_until.is_some());
    assert!(!peers[&id].is_serving);
    ledger.retire(&mut peers, c);
    assert_eq!(totals(&peers[&id]), (0, 0, 0, 0));
    assert!(ledger.owners.is_empty());
}

#[test]
fn unpolled_registered_plan_drop_releases_its_real_reservations() {
    let id = PeerId::repeat_byte(3);
    let (peer, _requests) = test_session(id);
    let mut plan = test_plan(&peer);
    let mut peers = HashMap::from([(id, peer)]);
    let mut ledger = BodyReceiptOwnerLedger::default();
    let (tx, mut accounting) = mpsc::unbounded_channel();

    // A prepared plan has no owner until registration at execution.
    assert!(ledger.owners.is_empty());
    let owner = ledger.register(&mut peers, &mut plan, tx, true);
    assert_eq!(totals(&peers[&id]), (1, 0, 1, 0));
    let unpolled = plan.execute();
    drop(unpolled);
    let event = accounting.try_recv().unwrap();
    assert!(event.retired);
    assert_eq!(event.owner, owner);
    ledger.retire(&mut peers, event.owner);
    assert_eq!(totals(&peers[&id]), (0, 0, 0, 0));
    assert!(ledger.owners.is_empty());
    assert!(accounting.try_recv().is_err());
}

fn feedback(owner: u64, peer_id: PeerId) -> BodyReceiptRequestAccounting {
    BodyReceiptRequestAccounting {
        owner,
        stats: vec![TypedRequestStat::new(
            peer_id,
            PeerRequestKind::Bodies,
            2,
            Duration::from_secs(1),
            64,
        )],
        failures: vec![ChunkRequestFailure {
            role: ChunkRequestRole::Receipts,
            peer_id,
            requested: 2,
            kind: ChunkFailureKind::Incomplete { returned: 0 },
        }],
        ..Default::default()
    }
}

#[test]
fn streamed_direct_and_header_accounting_require_the_captured_session() {
    let id = PeerId::repeat_byte(4);
    let (peer, _old_requests) = test_session(id);
    let sessions = HashMap::from([(id, peer.sender.clone())]);
    let mut plan = test_plan(&peer);
    let mut peers = HashMap::from([(id, peer)]);
    let mut ledger = BodyReceiptOwnerLedger::default();
    let (tx, _accounting) = mpsc::unbounded_channel();
    let owner = ledger.register(&mut peers, &mut plan, tx.clone(), false);

    let mut streamed = feedback(owner, id);
    assert!(ledger.filter_accounting(&peers, &mut streamed));
    assert_eq!((streamed.stats.len(), streamed.failures.len()), (1, 1));
    let mut direct = feedback(owner, id);
    filter_session_accounting(&peers, &sessions, &mut direct.stats, &mut direct.failures);
    assert_eq!((direct.stats.len(), direct.failures.len()), (1, 1));
    assert!(captured_session_is_current(&peers, &sessions, id));

    let (replacement, _new_requests) = test_session(id);
    let mut replacement_plan = test_plan(&replacement);
    peers.insert(id, replacement);
    let replacement_owner = ledger.register(&mut peers, &mut replacement_plan, tx, false);

    let mut old_streamed = feedback(owner, id);
    assert!(ledger.filter_accounting(&peers, &mut old_streamed));
    assert!(old_streamed.stats.is_empty() && old_streamed.failures.is_empty());
    let mut old_direct = feedback(owner, id);
    filter_session_accounting(
        &peers,
        &sessions,
        &mut old_direct.stats,
        &mut old_direct.failures,
    );
    assert!(old_direct.stats.is_empty() && old_direct.failures.is_empty());
    assert!(!captured_session_is_current(&peers, &sessions, id));

    let mut current = feedback(replacement_owner, id);
    assert!(ledger.filter_accounting(&peers, &mut current));
    assert_eq!((current.stats.len(), current.failures.len()), (1, 1));
    ledger.retire(&mut peers, owner);
    assert!(!ledger.filter_accounting(&peers, &mut feedback(owner, id)));
    ledger.reset(&mut peers);
    assert!(!ledger.filter_accounting(&peers, &mut feedback(replacement_owner, id)));
}

#[test]
fn normal_fifo_retirement_preserves_terminal_feedback_and_releases_capacity() {
    let id = PeerId::repeat_byte(5);
    let (peer, _requests) = test_session(id);
    let mut plan = test_plan(&peer);
    let mut peers = HashMap::from([(id, peer)]);
    let mut ledger = BodyReceiptOwnerLedger::default();
    let (tx, mut accounting) = mpsc::unbounded_channel();
    let owner = ledger.register(&mut peers, &mut plan, tx, true);
    let terminal = feedback(owner, id);
    emit_body_receipt_request_accounting(&plan.accounting_tx, terminal.stats, terminal.failures);
    drop(plan);

    let mut terminal = accounting.try_recv().unwrap();
    assert!(!terminal.retired);
    assert!(ledger.filter_accounting(&peers, &mut terminal));
    assert_eq!((terminal.stats.len(), terminal.failures.len()), (1, 1));
    let retirement = accounting.try_recv().unwrap();
    assert!(retirement.retired && retirement.owner == owner);
    ledger.retire(&mut peers, retirement.owner);
    assert_eq!(totals(&peers[&id]), (0, 0, 0, 0));
    assert!(ledger.owners.is_empty());
    assert!(!ledger.filter_accounting(&peers, &mut feedback(owner, id)));
    assert!(accounting.try_recv().is_err());
}
