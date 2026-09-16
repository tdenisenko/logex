//! Local failure-accounting and reconnect controls. No peer connections are made.
use super::limit_tests::Fixture;
use super::session_endpoint_tests::{info, request_channel};
use super::*;

fn pause(manager: &PeerManager, id: PeerId, role: ChunkRequestRole) -> Option<Instant> {
    let peer = &manager.peers[&id];
    match role {
        ChunkRequestRole::Bodies => peer.body_paused_until,
        ChunkRequestRole::Receipts => peer.receipt_paused_until,
    }
}

fn failure(manager: &mut PeerManager, id: PeerId, role: ChunkRequestRole, error: RequestAttempt) {
    let mut dead = HashSet::new();
    manager.apply_parallel_chunk_failures(
        "rehabilitation fixture",
        vec![ChunkRequestFailure {
            role,
            peer_id: id,
            requested: 1,
            kind: ChunkFailureKind::Request(error),
        }],
        &mut dead,
    );
    assert!(dead.is_empty());
}

#[tokio::test]
async fn rehabilitation_later_transport_failure_preserves_longer_timeout_pause() {
    for role in [ChunkRequestRole::Bodies, ChunkRequestRole::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        fixture
            .manager
            .peers
            .get_mut(&id)
            .unwrap()
            .consecutive_timeouts = 3;
        failure(
            &mut fixture.manager,
            id,
            role,
            RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout),
        );
        let longer = pause(&fixture.manager, id, role).unwrap();
        // Separate completions use separate failure batches; within-batch
        // severity coalescing cannot preserve the first completion's deadline.
        failure(&mut fixture.manager, id, role, RequestAttempt::Disconnected);
        assert!(pause(&fixture.manager, id, role).unwrap() >= longer);
    }
}

async fn reconnect_seed(quarantine_active: bool) {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(0x75);
    let record = NodeRecord::new_with_ports(Ipv4Addr::LOCALHOST.into(), 30303, Some(30304), id);
    let now = Instant::now();
    let until = if quarantine_active {
        now + Duration::from_secs(60)
    } else {
        now - Duration::from_secs(1)
    };
    fixture.manager.receipt_quarantine_history.insert(id, until);
    assert!(fixture.manager.remember_pending(record));
    let (sender, _receiver) = request_channel(id);
    fixture
        .manager
        .insert_peer(info(id, record.tcp_addr()), sender);
    assert_eq!(
        fixture.manager.peer_receipts_quarantined(id),
        quarantine_active
    );
    assert_eq!(
        fixture.manager.peers[&id].receipt_quarantined_until,
        quarantine_active.then_some(until)
    );
    assert_eq!(
        fixture.manager.known_peers.iter().any(|node| node.id == id),
        !quarantine_active
    );
    assert_eq!(
        fixture
            .manager
            .persisted_known_peers
            .iter()
            .any(|node| node.id == id),
        !quarantine_active
    );
    if !quarantine_active {
        let saved =
            crate::p2p::persistence::load_known_peers(&fixture.manager.known_peers_path).unwrap();
        assert_eq!(saved.iter().find(|node| node.id == id), Some(&record));
    }
}

#[tokio::test]
async fn rehabilitation_expired_quarantine_does_not_exclude_reconnected_seed() {
    reconnect_seed(false).await;
}

#[tokio::test]
async fn rehabilitation_active_quarantine_still_excludes_reconnected_seed() {
    reconnect_seed(true).await;
}

#[tokio::test]
async fn rehabilitation_later_timeout_extends_shorter_pause_without_pausing_other_role() {
    for role in [ChunkRequestRole::Bodies, ChunkRequestRole::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        failure(&mut fixture.manager, id, role, RequestAttempt::Disconnected);
        let shorter = pause(&fixture.manager, id, role).unwrap();
        failure(
            &mut fixture.manager,
            id,
            role,
            RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout),
        );
        assert!(pause(&fixture.manager, id, role).unwrap() > shorter);
        let other = match role {
            ChunkRequestRole::Bodies => ChunkRequestRole::Receipts,
            ChunkRequestRole::Receipts => ChunkRequestRole::Bodies,
        };
        assert!(pause(&fixture.manager, id, other).is_none());
    }
}

#[tokio::test]
async fn rehabilitation_success_clears_extended_pause_and_receipt_quarantine() {
    for role in [ChunkRequestRole::Bodies, ChunkRequestRole::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        fixture
            .manager
            .peers
            .get_mut(&id)
            .unwrap()
            .consecutive_timeouts = 3;
        failure(
            &mut fixture.manager,
            id,
            role,
            RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout),
        );
        failure(&mut fixture.manager, id, role, RequestAttempt::Disconnected);
        fixture.manager.record_peer_request_success(
            id,
            role.request_kind(),
            1,
            Duration::from_secs(1),
        );
        assert!(pause(&fixture.manager, id, role).is_none());
        assert_eq!(fixture.manager.peers[&id].consecutive_timeouts, 0);
        assert!(!fixture.manager.peer_receipts_quarantined(id));
    }
}

#[tokio::test]
async fn rehabilitation_expired_pause_is_replaced_by_new_failure() {
    for role in [ChunkRequestRole::Bodies, ChunkRequestRole::Receipts] {
        let mut fixture = Fixture::new().await;
        let id = PeerId::repeat_byte(1);
        let past = Instant::now() - Duration::from_secs(1);
        let peer = fixture.manager.peers.get_mut(&id).unwrap();
        match role {
            ChunkRequestRole::Bodies => peer.body_paused_until = Some(past),
            ChunkRequestRole::Receipts => peer.receipt_paused_until = Some(past),
        }
        let before = Instant::now();
        failure(&mut fixture.manager, id, role, RequestAttempt::Disconnected);
        assert!(pause(&fixture.manager, id, role).unwrap() >= before + REQUEST_KIND_PAUSE_DURATION);
    }
}

#[tokio::test]
async fn rehabilitation_header_failure_does_not_pause_payload_roles() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    assert!(!fixture.manager.on_request_error(
        id,
        PeerRequestKind::Headers,
        &RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout),
    ));
    assert!(pause(&fixture.manager, id, ChunkRequestRole::Bodies).is_none());
    assert!(pause(&fixture.manager, id, ChunkRequestRole::Receipts).is_none());
}
