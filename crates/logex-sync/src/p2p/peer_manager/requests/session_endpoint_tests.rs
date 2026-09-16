//! Session metadata controls using a dormant local fixture; no connections are made.
use super::limit_tests::Fixture;
use super::*;

fn info(peer_id: PeerId, remote_addr: SocketAddr) -> reth_network::events::SessionInfo {
    reth_network::events::SessionInfo {
        peer_id,
        remote_addr,
        client_version: Arc::from("session endpoint fixture"),
        capabilities: Arc::new(reth_eth_wire::Capabilities::new(vec![
            reth_eth_wire::Capability::eth(EthVersion::Eth69),
        ])),
        status: Arc::new(UnifiedStatus {
            latest_block: Some(100),
            ..Default::default()
        }),
        version: EthVersion::Eth69,
        peer_kind: PeerKind::Basic,
    }
}

fn request_channel(
    peer_id: PeerId,
) -> (
    PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>,
) {
    let (sender, receiver) = mpsc::channel(1);
    (PeerRequestSender::new(peer_id, sender), receiver)
}

#[tokio::test]
async fn session_endpoint_new_discovery_hint_is_retained_for_retry() {
    let mut fixture = Fixture::new().await;
    fixture.manager.dial_families = DialAddressFamilies::BOTH;
    let id = PeerId::repeat_byte(0x71);
    let submitted = NodeRecord::new_with_ports(Ipv4Addr::LOCALHOST.into(), 30303, Some(30304), id);
    let later_hint = NodeRecord::new_with_ports(Ipv6Addr::LOCALHOST.into(), 30305, Some(30306), id);
    fixture.manager.pending_dials.insert(
        id,
        SubmittedDial {
            node: submitted,
            submitted_at: Instant::now(),
        },
    );
    assert!(fixture.manager.remember_pending(later_hint));
    let (sender, _receiver) = request_channel(id);
    fixture
        .manager
        .insert_peer(info(id, submitted.tcp_addr()), sender);
    assert_eq!(fixture.manager.peers[&id].remote_record, later_hint);
    assert!(fixture.manager.peers[&id].remote_record_is_dialable);
    assert!(!fixture.manager.pending.contains_key(&id));
    assert!(!fixture.manager.pending_dials.contains_key(&id));
    let persisted =
        crate::p2p::persistence::load_known_peers(&fixture.manager.known_peers_path).unwrap();
    assert_eq!(
        persisted.iter().find(|node| node.id == id),
        Some(&later_hint)
    );
}

#[tokio::test]
async fn session_endpoint_metrics_follow_socket_with_a_different_family_hint() {
    for (connected_ip, hint_ip, expected_v4, expected_v6) in [
        (
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            4,
            0,
        ),
        (
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            3,
            1,
        ),
    ] {
        let mut fixture = Fixture::new().await;
        fixture.manager.dial_families = DialAddressFamilies::BOTH;
        let id = PeerId::repeat_byte(0x72);
        let hint = NodeRecord::new_with_ports(hint_ip, 30303, Some(30304), id);
        assert!(fixture.manager.remember_pending(hint));
        let connected = SocketAddr::new(connected_ip, 40303);
        let (sender, _receiver) = request_channel(id);
        fixture.manager.insert_peer(info(id, connected), sender);
        assert_eq!(fixture.manager.peers[&id].remote_record, hint);
        assert_eq!(fixture.manager.peers[&id].remote_addr, connected);
        let status = fixture.manager.execution_network_status();
        assert_eq!(status.connected_ipv4_peers, expected_v4);
        assert_eq!(status.connected_ipv6_peers, expected_v6);
        assert!(fixture.manager.report_valid_serving_peer(id));
        let status = fixture.manager.execution_network_status();
        assert_eq!(status.serving_ipv4_peers, expected_v4);
        assert_eq!(status.serving_ipv6_peers, expected_v6);
    }
}

#[tokio::test]
async fn session_endpoint_unknown_remote_socket_is_not_saved_as_a_retry_hint() {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(0x73);
    let remote = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 40303);
    let (sender, _receiver) = request_channel(id);
    fixture.manager.insert_peer(info(id, remote), sender);
    assert!(!fixture.manager.peers[&id].remote_record_is_dialable);
    assert!(
        !fixture
            .manager
            .known_peers()
            .iter()
            .any(|node| node.id == id)
    );
    let status = fixture.manager.execution_network_status();
    assert_eq!(status.connected_ipv4_peers, 3);
    assert_eq!(status.connected_ipv6_peers, 1);
}

#[tokio::test]
async fn session_endpoint_reconnect_updates_observed_family_and_keeps_retry_hint() {
    let mut fixture = Fixture::new().await;
    fixture.manager.dial_families = DialAddressFamilies::BOTH;
    let id = PeerId::repeat_byte(0x74);
    let hint = NodeRecord::new_with_ports(Ipv6Addr::LOCALHOST.into(), 30303, Some(30304), id);
    for (connected, v4, v6) in [
        (SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 40303), 4, 0),
        (SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 40304), 3, 1),
    ] {
        fixture.manager.remove_peer(id);
        assert!(fixture.manager.remember_pending(hint));
        let (sender, _receiver) = request_channel(id);
        fixture.manager.insert_peer(info(id, connected), sender);
        assert_eq!(fixture.manager.peers[&id].remote_record, hint);
        assert_eq!(fixture.manager.peers[&id].remote_addr, connected);
        let status = fixture.manager.execution_network_status();
        assert_eq!(status.connected_ipv4_peers, v4);
        assert_eq!(status.connected_ipv6_peers, v6);
    }
}
