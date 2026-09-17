use futures_util::FutureExt;

use super::*;

#[test]
fn execution_peer_admission_preserves_connection_and_pending_limits() {
    for (total, outbound, inbound, dials) in [
        (0, 0, 0, 32),
        (1, 1, 0, 32),
        (2, 1, 1, 32),
        (3, 1, 2, 32),
        (100, 33, 67, 96),
    ] {
        let (peers, sessions) = execution_peer_configs(total).unwrap();
        assert_eq!(peers.max_peers(), total);
        assert_eq!(peers.connection_info.max_outbound, outbound);
        assert_eq!(peers.connection_info.max_inbound, inbound);
        assert_eq!(peers.connection_info.max_concurrent_outbound_dials, dials);
        assert_eq!(sessions.limits.max_pending_inbound, Some(inbound as u32));
        assert_eq!(sessions.limits.max_pending_outbound, Some(dials as u32));
    }
}

#[test]
fn execution_peer_admission_preserves_dependency_buffer_scaling() {
    // The pinned dependency defaults to 130 peers, a 260-slot event buffer,
    // and a maximum scaled event buffer of 2,600 slots.
    for (total, event_buffer) in [(0, 260), (100, 260), (131, 262), (1_300, 2_600)] {
        let (_, sessions) = execution_peer_configs(total).unwrap();
        assert_eq!(sessions.session_event_buffer, event_buffer);
        assert_eq!(sessions.session_command_buffer, 32);
    }
}

#[test]
fn execution_peer_admission_accepts_largest_representable_configuration() {
    #[cfg(target_pointer_width = "64")]
    let maximum = 6_442_450_942;
    #[cfg(target_pointer_width = "32")]
    let maximum = usize::MAX / 2;

    let (peers, sessions) = execution_peer_configs(maximum).unwrap();
    assert_eq!(peers.max_peers(), maximum);
    assert_eq!(sessions.session_event_buffer, 2_600);
    assert_eq!(
        usize::try_from(sessions.limits.max_pending_inbound.unwrap()).unwrap(),
        peers.connection_info.max_inbound,
    );
    assert!(execution_peer_configs(maximum + 1).is_err());
}

#[test]
#[cfg(target_pointer_width = "64")]
fn execution_peer_admission_rejects_inbound_narrowing() {
    let error = execution_peer_configs(6_442_450_943).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("inbound session limit exceeding u32::MAX")
    );
}

#[test]
fn execution_peer_admission_rejects_event_buffer_overflow() {
    for total in [usize::MAX / 2 + 1, usize::MAX] {
        let error = execution_peer_configs(total).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("session event-buffer calculation")
        );
    }
}

#[test]
fn execution_peer_admission_public_constructor_rejects_before_async_setup() {
    // Deliberately no Tokio runtime: the public constructor must return before
    // looking up bootnodes, accessing storage or creating networking tasks.
    let config = PeerManagerConfig {
        secret_key: SecretKey::from_slice(&[1; 32]).unwrap(),
        listener_port: 0,
        discovery_port: 0,
        bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
        dial_families: DialAddressFamilies::IPV4,
        max_peers: usize::MAX,
        nat_resolver: NatResolver::None,
        our_head: Head::default(),
        known_peers: Vec::new(),
        known_peers_path: PathBuf::new(),
        execution_bootnodes: Vec::new(),
        execution_discv5_port: 0,
    };
    let Some(Err(error)) = PeerManager::new(config).now_or_never() else {
        panic!("invalid peer capacity must fail immediately");
    };
    assert!(
        error
            .to_string()
            .contains("session event-buffer calculation")
    );
}
