//! Pure record-selection controls; no sockets or discovery services are started.
use super::*;

fn record(tcp4: u16, tcp6: u16, prefer_ipv6: bool) -> DnsNodeRecordUpdate {
    record_with_udp(tcp4, tcp6, 30303, 30304, prefer_ipv6)
}

fn record_with_udp(
    tcp4: u16,
    tcp6: u16,
    udp4: u16,
    udp6: u16,
    prefer_ipv6: bool,
) -> DnsNodeRecordUpdate {
    // Select a real deterministic fixture key for each existing parity branch.
    (1..=32)
        .find_map(|seed| {
            let key = SecretKey::from_byte_array(&[seed; 32]).unwrap();
            let enr = enr::Enr::<SecretKey>::builder()
                .ip4(Ipv4Addr::LOCALHOST)
                .ip6(Ipv6Addr::LOCALHOST)
                .tcp4(tcp4)
                .tcp6(tcp6)
                .udp4(udp4)
                .udp6(udp6)
                .build(&key)
                .unwrap();
            let update = dns_node_record_update_from_event(DnsDiscoveryEvent::Enr(enr)).unwrap();
            let odd = update.peer_id.as_slice().last().unwrap() & 1 == 1;
            (odd == prefer_ipv6).then_some(update)
        })
        .expect("fixtures must cover both peer-ID parity branches")
}

#[test]
fn endpoint_selection_signed_zero_tcp_does_not_hide_usable_family() {
    for (tcp4, tcp6, expected) in [
        (0, 30304, IpAddr::V6(Ipv6Addr::LOCALHOST)),
        (30303, 0, IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ] {
        let update = record(tcp4, tcp6, false);
        let enr = EnrCombinedKeyWrapper::from(update.enr).0;
        let selected = signed_enr_node_record_for_dial_families(DialAddressFamilies::BOTH, &enr)
            .expect("a usable allowed endpoint exists");
        assert_eq!(selected.tcp_addr().ip(), expected);
        assert_ne!(selected.tcp_port, 0);
        let unavailable = if tcp4 == 0 {
            DialAddressFamilies::IPV4
        } else {
            DialAddressFamilies::IPV6
        };
        assert!(signed_enr_node_record_for_dial_families(unavailable, &enr).is_none());
    }
}

#[test]
fn endpoint_selection_dns_zero_tcp_does_not_hide_usable_family() {
    for prefer_ipv6 in [false, true] {
        for (tcp4, tcp6, expected) in [
            (0, 30304, IpAddr::V6(Ipv6Addr::LOCALHOST)),
            (30303, 0, IpAddr::V4(Ipv4Addr::LOCALHOST)),
        ] {
            let update = record(tcp4, tcp6, prefer_ipv6);
            let selected = dns_node_record_for_dial_families(DialAddressFamilies::BOTH, &update)
                .expect("a usable allowed endpoint exists");
            assert_eq!(selected.tcp_addr().ip(), expected);
            assert_ne!(selected.tcp_port, 0);
            let unavailable = if tcp4 == 0 {
                DialAddressFamilies::IPV4
            } else {
                DialAddressFamilies::IPV6
            };
            assert!(dns_node_record_for_dial_families(unavailable, &update).is_none());
        }
    }
}

#[test]
fn endpoint_selection_default_record_skips_zero_ipv4_tcp() {
    let update = record(0, 30304, false);
    let selected = update.node_record.expect("a usable IPv6 endpoint exists");
    assert!(selected.address.is_ipv6());
    assert_eq!(selected.tcp_port, 30304);
}

#[test]
fn endpoint_selection_initial_bootstrap_uses_usable_allowed_endpoint() {
    for (tcp4, tcp6, expected) in [
        (0, 30304, IpAddr::V6(Ipv6Addr::LOCALHOST)),
        (30303, 0, IpAddr::V4(Ipv4Addr::LOCALHOST)),
    ] {
        let update = record(tcp4, tcp6, false);
        let selected = dns_initial_direct_boot_node(
            expected,
            DialAddressFamilies::BOTH,
            &MAINNET.fork_filter(Head::default()),
            &update,
        )
        .expect("a usable allowed endpoint exists");
        assert_eq!(selected.tcp_addr().ip(), expected);
        assert_ne!(selected.tcp_port, 0);
    }
}

#[test]
fn endpoint_selection_zero_tcp_retains_udp_discovery_only() {
    let update = record(0, 0, false);
    let filter = MAINNET.fork_filter(Head::default());
    assert!(update.node_record.is_none());
    assert!(dns_node_record_for_dial_families(DialAddressFamilies::BOTH, &update).is_none());
    let enr = EnrCombinedKeyWrapper::from(update.enr.clone()).0;
    assert!(signed_enr_node_record_for_dial_families(DialAddressFamilies::BOTH, &enr).is_none());
    for bind in [
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    ] {
        assert!(dns_signed_boot_node_for_bind_ip(bind, &filter, &update).is_some());
        assert!(signed_enr_matches_discovery_bind_ip(bind, &enr));
    }
}

#[test]
fn endpoint_selection_discovery_keeps_udp_when_tcp_zero() {
    let update = record(0, 0, false);
    let filter = MAINNET.fork_filter(Head::default());
    for (bind, port) in [
        (IpAddr::V4(Ipv4Addr::UNSPECIFIED), 30303),
        (IpAddr::V6(Ipv6Addr::UNSPECIFIED), 30304),
    ] {
        let seed = dns_boot_node_for_bind_ip(bind, &filter, &update)
            .expect("UDP discovery does not require a TCP service");
        assert_eq!(seed.tcp_port, 0);
        assert_eq!(seed.udp_port, port);
    }
}

#[test]
fn endpoint_selection_discovery_requires_nonzero_udp() {
    let filter = MAINNET.fork_filter(Head::default());
    for (udp4, udp6, bind) in [
        (0, 30304, IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        (30303, 0, IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
    ] {
        let update = record_with_udp(30303, 30304, udp4, udp6, false);
        assert!(dns_boot_node_for_bind_ip(bind, &filter, &update).is_none());
        assert!(dns_signed_boot_node_for_bind_ip(bind, &filter, &update).is_none());
        let enr = EnrCombinedKeyWrapper::from(update.enr).0;
        assert!(!signed_enr_matches_discovery_bind_ip(bind, &enr));
        assert!(
            signed_enr_node_record_for_dial_families(DialAddressFamilies::BOTH, &enr).is_some()
        );
    }
}

#[test]
fn endpoint_selection_absent_udp_is_not_invented_from_tcp() {
    let key = SecretKey::from_byte_array(&[0x37; 32]).unwrap();
    let enr = enr::Enr::<SecretKey>::builder()
        .ip4(Ipv4Addr::LOCALHOST)
        .tcp4(30303)
        .ip6(Ipv6Addr::LOCALHOST)
        .tcp6(30304)
        .build(&key)
        .unwrap();
    let update = dns_node_record_update_from_event(DnsDiscoveryEvent::Enr(enr.clone())).unwrap();
    assert_eq!(update.node_record.unwrap().udp_port, 0);
    let signed = EnrCombinedKeyWrapper::from(enr).0;
    for family in [DialAddressFamilies::IPV4, DialAddressFamilies::IPV6] {
        assert_eq!(
            dns_node_record_for_dial_families(family, &update)
                .unwrap()
                .udp_port,
            0
        );
        assert_eq!(
            signed_enr_node_record_for_dial_families(family, &signed)
                .unwrap()
                .udp_port,
            0
        );
    }
}

#[test]
fn endpoint_selection_configured_seed_uses_discovery_bind_independently_of_tcp() {
    for (tcp4, tcp6, bind, expected_port) in [
        (0, 30304, IpAddr::V4(Ipv4Addr::LOCALHOST), 30303),
        (30303, 0, IpAddr::V6(Ipv6Addr::LOCALHOST), 30304),
    ] {
        let update = record(tcp4, tcp6, false);
        let enr = EnrCombinedKeyWrapper::from(update.enr).0;
        let direct =
            signed_enr_node_record_for_dial_families(DialAddressFamilies::BOTH, &enr).unwrap();
        assert_ne!(direct.address, bind);
        let seed = signed_enr_discovery_node_for_bind_ip(bind, &enr).unwrap();
        assert_eq!(seed.address, bind);
        assert_eq!(seed.tcp_port, 0);
        assert_eq!(seed.udp_port, expected_port);
        let reth_discv5::BootNode::Enode(address) =
            reth_discv5::BootNode::from_unsigned(seed).unwrap()
        else {
            panic!("unsigned seed should become an address");
        };
        assert!(
            address
                .to_string()
                .contains(&format!("/udp/{expected_port}/"))
        );
    }
}

#[test]
fn endpoint_selection_udp_only_generic_ipv6_seed_preserves_identity() {
    let key = SecretKey::from_byte_array(&[0x38; 32]).unwrap();
    let enr = enr::Enr::<SecretKey>::builder()
        .ip6(Ipv6Addr::LOCALHOST)
        .udp4(30304)
        .build(&key)
        .unwrap();
    let update = dns_node_record_update_from_event(DnsDiscoveryEvent::Enr(enr.clone())).unwrap();
    assert!(update.node_record.is_none());
    assert!(dns_node_record_for_dial_families(DialAddressFamilies::BOTH, &update).is_none());
    let bind = IpAddr::V6(Ipv6Addr::UNSPECIFIED);
    let filter = MAINNET.fork_filter(Head::default());
    let dns_seed = dns_boot_node_for_bind_ip(bind, &filter, &update).unwrap();
    let signed = EnrCombinedKeyWrapper::from(enr).0;
    let configured_seed = signed_enr_discovery_node_for_bind_ip(bind, &signed).unwrap();
    assert_eq!(dns_seed, configured_seed);
    assert_eq!(dns_seed.id, update.peer_id);
    assert_eq!(dns_seed.tcp_port, 0);
    assert_eq!(dns_seed.udp_port, 30304);
    // Signed and unsigned discovery now resolve the same shared UDP endpoint.
    assert!(signed_enr_matches_discovery_bind_ip(bind, &signed));
    assert_eq!(
        dns_signed_boot_node_for_bind_ip(bind, &filter, &update)
            .unwrap()
            .to_string(),
        signed.to_string()
    );
    assert!(reth_discv5::BootNode::from_unsigned(configured_seed).is_ok());
}
