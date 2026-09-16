//! Finite in-memory retry-history sequences; the local fixture never dials peers.
use super::limit_tests::Fixture;
use super::*;

fn identity(index: u64) -> PeerId {
    let mut bytes = [0u8; 64];
    bytes[56..].copy_from_slice(&index.to_be_bytes());
    PeerId::from(bytes)
}

#[tokio::test]
async fn backoff_retention_saturated_history_has_an_entry_bound() {
    let mut fixture = Fixture::new().await;
    for index in 0..4097 {
        fixture.manager.backoff_saturated_peer(identity(index));
    }
    assert!(fixture.manager.saturated_peers.len() <= 4096);
}

#[tokio::test]
async fn backoff_retention_receipt_history_has_an_entry_bound() {
    let mut fixture = Fixture::new().await;
    for index in 0..4097 {
        let id = identity(index);
        let (peer, _receiver) = super::ownership_tests::test_session(id);
        fixture.manager.peers.insert(id, peer);
        fixture
            .manager
            .quarantine_peer_receipts_after_request_failure(
                id,
                "retention fixture",
                1,
                &RequestAttempt::Disconnected,
            );
        fixture.manager.remove_peer(id);
    }
    assert!(fixture.manager.receipt_quarantine_history.len() <= 4096);
}

#[tokio::test]
async fn backoff_retention_transport_pause_preserves_productive_restart_hint() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    let record = fixture.manager.peers[&id].remote_record;
    assert!(fixture.manager.remember_productive(record));
    assert!(
        fixture
            .manager
            .known_peers()
            .iter()
            .any(|node| node.id == id)
    );
    fixture
        .manager
        .quarantine_peer_receipts_after_request_failure(
            id,
            "restart hint fixture",
            1,
            &RequestAttempt::Disconnected,
        );
    assert!(fixture.manager.peer_receipts_quarantined(id));
    assert!(
        fixture
            .manager
            .known_peers()
            .iter()
            .any(|node| node.id == id)
    );
    fixture.manager.remove_peer(id);
    assert!(fixture.manager.peer_receipts_quarantined(id));
    assert!(
        fixture
            .manager
            .known_peers()
            .iter()
            .any(|node| node.id == id)
    );
    // Incomplete service has a different, existing restart-hint policy.
    fixture
        .manager
        .quarantine_peer_receipts(id, "incomplete fixture", 1, 0);
    assert!(
        !fixture
            .manager
            .known_peers()
            .iter()
            .any(|node| node.id == id)
    );
}

#[tokio::test]
async fn backoff_retention_disconnected_cooldown_does_not_consume_a_dial_slot() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    let removed = fixture.manager.remove_peer(id).unwrap();
    fixture.manager.network_activated = true;
    fixture.manager.requeue_disconnected_peer(&removed);
    let fresh = NodeRecord::new_with_ports(Ipv4Addr::LOCALHOST.into(), 30305, None, identity(5000));
    assert!(fixture.manager.remember_pending(fresh));
    fixture.manager.dial_pending_peers(3);
    assert_eq!(fixture.manager.session_metrics.submitted_dials_total, 1);
    assert!(fixture.manager.pending_dials.contains_key(&fresh.id));
    assert!(!fixture.manager.pending_dials.contains_key(&id));
}

#[test]
fn backoff_retention_expiry_precedes_pressure_and_refresh_preserves_deadline() {
    let now = Instant::now();
    let (first, second, third, fourth) = (identity(1), identity(2), identity(3), identity(4));
    let short = now + Duration::from_secs(10);
    let long = now + Duration::from_secs(20);
    let mut history = HashMap::from([(first, now), (second, long)]);
    state::retain_peer_backoff(&mut history, third, short, now, 2);
    assert_eq!(history, HashMap::from([(second, long), (third, short)]));
    state::retain_peer_backoff(&mut history, second, short, now, 2);
    assert_eq!(history[&second], long);
    state::retain_peer_backoff(&mut history, fourth, long, now, 2);
    assert_eq!(history, HashMap::from([(second, long), (fourth, long)]));
    state::retain_peer_backoff(&mut history, first, now, now, 2);
    assert!(!history.contains_key(&first));
    let mut disabled = HashMap::new();
    state::retain_peer_backoff(&mut disabled, first, long, now, 0);
    assert!(disabled.is_empty());
}

#[tokio::test]
async fn backoff_retention_history_pressure_preserves_active_restriction() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    let record = fixture.manager.peers[&id].remote_record;
    fixture
        .manager
        .quarantine_peer_receipts_after_request_failure(
            id,
            "active fixture",
            1,
            &RequestAttempt::Disconnected,
        );
    let until = fixture.manager.peers[&id].receipt_quarantined_until;
    assert!(!fixture.manager.receipt_quarantine_history.contains_key(&id));
    let now = Instant::now();
    for index in 0..4097 {
        state::retain_peer_backoff(
            &mut fixture.manager.receipt_quarantine_history,
            identity(index),
            now + Duration::from_secs(300),
            now,
            4096,
        );
    }
    assert_eq!(fixture.manager.receipt_quarantine_history.len(), 4096);
    assert_eq!(fixture.manager.peers[&id].receipt_quarantined_until, until);
    assert!(fixture.manager.peer_receipts_quarantined(id));
    assert!(!fixture.manager.remember_productive(record));
    assert!(
        !fixture
            .manager
            .known_peers()
            .iter()
            .any(|node| node.id == id)
    );
    assert_eq!(
        fixture
            .manager
            .execution_network_status()
            .receipt_quarantined_peers,
        4097
    );
}

#[tokio::test]
async fn backoff_retention_reconnect_moves_restriction_and_success_restores_seed() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    let record = fixture.manager.peers[&id].remote_record;
    fixture.manager.persist_known_peer_cache();
    fixture
        .manager
        .quarantine_peer_receipts_after_request_failure(
            id,
            "reconnect fixture",
            1,
            &RequestAttempt::Disconnected,
        );
    let until = fixture.manager.peers[&id]
        .receipt_quarantined_until
        .unwrap();
    fixture.manager.remove_peer(id);
    assert_eq!(fixture.manager.receipt_quarantine_history[&id], until);
    assert!(fixture.manager.remember_pending(record));
    let (sender, _receiver) = super::session_endpoint_tests::request_channel(id);
    fixture.manager.insert_peer(
        super::session_endpoint_tests::info(id, record.tcp_addr()),
        sender,
    );
    assert!(!fixture.manager.receipt_quarantine_history.contains_key(&id));
    assert_eq!(
        fixture.manager.peers[&id].receipt_quarantined_until,
        Some(until)
    );
    assert_eq!(
        fixture
            .manager
            .execution_network_status()
            .receipt_quarantined_peers,
        1
    );
    fixture.manager.persist_known_peer_cache();
    let saved =
        crate::p2p::persistence::load_known_peers(&fixture.manager.known_peers_path).unwrap();
    assert!(!saved.iter().any(|node| node.id == id));
    fixture.manager.record_peer_request_success(
        id,
        PeerRequestKind::Receipts,
        1,
        Duration::from_secs(1),
    );
    assert!(!fixture.manager.peer_receipts_quarantined(id));
    assert!(
        fixture
            .manager
            .known_peers()
            .iter()
            .any(|node| node.id == id)
    );
    assert_eq!(
        fixture
            .manager
            .execution_network_status()
            .receipt_quarantined_peers,
        0
    );
}

#[tokio::test]
async fn backoff_retention_replacement_session_inherits_active_restriction() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    fixture
        .manager
        .quarantine_peer_receipts(id, "long fixture", 1, 0);
    let until = fixture.manager.peers[&id].receipt_quarantined_until;
    fixture
        .manager
        .quarantine_peer_receipts_after_request_failure(
            id,
            "short fixture",
            1,
            &RequestAttempt::Disconnected,
        );
    assert_eq!(fixture.manager.peers[&id].receipt_quarantined_until, until);
    let (sender, _receiver) = super::session_endpoint_tests::request_channel(id);
    fixture.manager.insert_peer(
        super::session_endpoint_tests::info(id, "127.0.0.1:40303".parse().unwrap()),
        sender,
    );
    assert_eq!(fixture.manager.peers[&id].receipt_quarantined_until, until);
    assert!(fixture.manager.receipt_quarantine_history.is_empty());
    assert_eq!(
        fixture
            .manager
            .execution_network_status()
            .receipt_quarantined_peers,
        1
    );
}

#[tokio::test]
async fn backoff_retention_status_ignores_expired_history_and_forgetting_clears_state() {
    let mut fixture = Fixture::new().await;
    let active = PeerId::repeat_byte(1);
    fixture
        .manager
        .quarantine_peer_receipts_after_request_failure(
            active,
            "status fixture",
            1,
            &RequestAttempt::Disconnected,
        );
    let now = Instant::now();
    fixture
        .manager
        .receipt_quarantine_history
        .insert(identity(5), now);
    fixture
        .manager
        .receipt_quarantine_history
        .insert(identity(6), now + Duration::from_secs(60));
    assert_eq!(
        fixture
            .manager
            .execution_network_status()
            .receipt_quarantined_peers,
        2
    );
    fixture.manager.prune_receipt_quarantine_history(now);
    assert_eq!(fixture.manager.receipt_quarantine_history.len(), 1);
    fixture.manager.forget_peer(active);
    fixture.manager.forget_peer(identity(6));
    assert!(!fixture.manager.peer_receipts_quarantined(active));
    assert!(fixture.manager.receipt_quarantine_history.is_empty());
    assert_eq!(
        fixture
            .manager
            .execution_network_status()
            .receipt_quarantined_peers,
        0
    );
}

#[tokio::test]
async fn backoff_retention_disconnect_history_is_bounded_without_submitted_dials() {
    let mut fixture = Fixture::new().await;
    fixture.manager.network_activated = true;
    let mut peer = fixture.manager.peers[&PeerId::repeat_byte(1)].clone();
    for index in 0..4097 {
        peer.remote_record.id = identity(index);
        fixture.manager.requeue_disconnected_peer(&peer);
    }
    assert_eq!(fixture.manager.disconnected_retries.len(), 4096);
    assert!(fixture.manager.pending_dials.is_empty());
    assert!(fixture.manager.pending.len() <= 4096);
    assert!(
        fixture
            .manager
            .dial_is_suppressed(identity(4096), Instant::now())
    );
}

#[tokio::test]
async fn backoff_retention_retry_hint_survives_pending_eviction_and_session_acceptance() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    let removed = fixture.manager.remove_peer(id).unwrap();
    fixture.manager.network_activated = true;
    fixture.manager.requeue_disconnected_peer(&removed);
    fixture.manager.pending.remove(&id);
    let (sender, _receiver) = super::session_endpoint_tests::request_channel(id);
    fixture.manager.insert_peer(
        super::session_endpoint_tests::info(id, "127.0.0.1:40303".parse().unwrap()),
        sender,
    );
    assert_eq!(
        fixture.manager.peers[&id].remote_record,
        removed.remote_record
    );
    assert!(fixture.manager.peers[&id].remote_record_is_dialable);
    assert!(!fixture.manager.disconnected_retries.contains_key(&id));
    assert!(fixture.manager.pending_dials.is_empty());
}

#[tokio::test]
async fn backoff_retention_expired_disconnect_delay_allows_actual_dial() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    let removed = fixture.manager.remove_peer(id).unwrap();
    fixture.manager.network_activated = true;
    fixture.manager.requeue_disconnected_peer(&removed);
    // Pending admission/retention can lose an optional hint while its retry
    // deadline still owns the advertised address.
    fixture.manager.pending.remove(&id);
    let now = Instant::now();
    fixture
        .manager
        .disconnected_retries
        .get_mut(&id)
        .unwrap()
        .retry_at = now;
    fixture.manager.prune_submitted_dials(now);
    assert_eq!(
        fixture.manager.session_metrics.submitted_dial_expirations,
        0
    );
    assert!(!fixture.manager.disconnected_retries.contains_key(&id));
    fixture.manager.dial_pending_peers(3);
    assert_eq!(fixture.manager.session_metrics.submitted_dials_total, 1);
    assert!(fixture.manager.pending_dials.contains_key(&id));
    fixture.manager.forget_peer(id);
    assert!(!fixture.manager.pending_dials.contains_key(&id));
    assert!(!fixture.manager.disconnected_retries.contains_key(&id));
}
