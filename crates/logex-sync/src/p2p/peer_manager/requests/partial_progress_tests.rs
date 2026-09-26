//! Productive partial-response policy controls. Ordinary typed channel replies;
//! the Fixture's loopback listener is dormant, with no peer/service tasks.
use super::limit_tests::{Fixture, receipt_contexts, take_requests};
use super::*;
use alloy_consensus::Header;

async fn reverse_header_prefix_control(short_page: Option<usize>) {
    let mut fixture = Fixture::new().await;
    let mut chain = Vec::new();
    let mut parent_hash = B256::ZERO;
    for number in 100..=106 {
        let header = Header {
            number,
            parent_hash,
            ommers_hash: alloy_consensus::constants::EMPTY_OMMER_ROOT_HASH,
            transactions_root: alloy_consensus::constants::EMPTY_ROOT_HASH,
            receipts_root: alloy_consensus::constants::EMPTY_ROOT_HASH,
            gas_limit: 30_000_000,
            timestamp: 1_700_000_000 + number,
            withdrawals_root: Some(alloy_consensus::constants::EMPTY_ROOT_HASH),
            ..Default::default()
        };
        parent_hash = header.hash_slow();
        chain.push(header);
    }

    let mut collected = Vec::new();
    for attempt in 0..2 {
        let child = collected.last().unwrap_or(&chain[6]);
        let remaining = child.number - 100;
        if remaining == 0 {
            break;
        }
        let plan = fixture
            .manager
            .prepare_reverse_header_pages_request(child.number, remaining, 2, 100)
            .await
            .unwrap()
            .unwrap()
            .with_sequential_candidates();
        let mut future = Box::pin(plan.execute());
        let mut queued = Vec::new();
        // The fixture's small channels can require another poll after draining.
        for _ in 0..remaining.div_ceil(2) {
            assert!(futures_util::poll!(future.as_mut()).is_pending());
            queued.extend(take_requests(&mut fixture.receivers));
            if queued.len() as u64 == remaining.div_ceil(2) {
                break;
            }
        }
        let mut requests = queued
            .into_iter()
            .map(|(_, message)| {
                let PeerRequest::GetBlockHeaders { request, response } = message else {
                    panic!("expected a local header request");
                };
                let BlockHashOrNumber::Number(start) = request.start_block else {
                    panic!("expected a numeric page start");
                };
                assert_eq!(request.skip, 0);
                assert_eq!(request.direction, reth_eth_wire::HeadersDirection::Falling);
                (start, request.limit, response)
            })
            .collect::<Vec<_>>();
        assert_eq!(requests.len() as u64, remaining.div_ceil(2));
        // Complete older pages first: completion order must not affect the prefix.
        requests.sort_by_key(|(start, _, _)| *start);
        let request_count = requests.len();
        let bytes_before = fixture
            .manager
            .session_metrics
            .p2p_download
            .snapshot(Instant::now())
            .total_payload_bytes;
        let mut received_bytes = 0;
        for (index, (start, limit, response)) in requests.into_iter().enumerate() {
            let page_index = ((105 - start) / 2) as usize;
            let count = if attempt == 0 && short_page == Some(page_index) {
                1
            } else {
                limit
            };
            let headers = (0..count)
                .map(|offset| chain[(start - 100 - offset) as usize].clone())
                .collect::<Vec<_>>();
            received_bytes += headers_payload_bytes(&headers);
            response.send(Ok(BlockHeaders(headers))).unwrap();
            if index + 1 < request_count {
                assert!(futures_util::poll!(future.as_mut()).is_pending());
            }
        }
        let pages = fixture
            .manager
            .complete_reverse_header_pages_request(future.await)
            .unwrap();
        assert_eq!(
            fixture
                .manager
                .session_metrics
                .p2p_download
                .snapshot(Instant::now())
                .total_payload_bytes
                - bytes_before,
            received_bytes
        );
        let headers = pages
            .into_iter()
            .flat_map(|(_, headers)| headers)
            .collect::<Vec<_>>();
        let expected_len = if attempt == 0 {
            short_page.map_or(6, |page| page * 2 + 1)
        } else {
            remaining as usize
        };
        let expected = (100..child.number)
            .rev()
            .take(expected_len)
            .collect::<Vec<_>>();
        assert_eq!(
            headers
                .iter()
                .map(|header| header.number)
                .collect::<Vec<_>>(),
            expected
        );
        crate::validation::validate_reverse_downloaded_headers_with_hashes(child, &headers)
            .unwrap();
        collected.extend(headers);
        assert_eq!(fixture.manager.peers.len(), 3);
        assert!(
            fixture
                .manager
                .peers
                .values()
                .all(|peer| peer.consecutive_timeouts == 0)
        );
    }
    assert_eq!(
        collected
            .iter()
            .map(|header| header.number)
            .collect::<Vec<_>>(),
        (100..106).rev().collect::<Vec<_>>()
    );
}

#[tokio::test(start_paused = true)]
async fn reverse_header_prefix_stops_after_short_first_page() {
    reverse_header_prefix_control(Some(0)).await;
}

#[tokio::test(start_paused = true)]
async fn reverse_header_prefix_stops_after_short_middle_page() {
    reverse_header_prefix_control(Some(1)).await;
}

#[tokio::test(start_paused = true)]
async fn reverse_header_prefix_accepts_short_final_page() {
    reverse_header_prefix_control(Some(2)).await;
}

#[tokio::test(start_paused = true)]
async fn reverse_header_prefix_preserves_complete_pages() {
    reverse_header_prefix_control(None).await;
}

#[tokio::test(start_paused = true)]
async fn reverse_header_prefix_accounts_later_completed_failures() {
    let mut fixture = Fixture::new().await;
    let first = PeerId::repeat_byte(1);
    let later = PeerId::repeat_byte(2);
    fixture
        .manager
        .peers
        .get_mut(&later)
        .unwrap()
        .consecutive_timeouts = MAX_CONSECUTIVE_TIMEOUTS - 1;
    let sessions = fixture
        .manager
        .peers
        .iter()
        .map(|(id, peer)| (*id, peer.sender.clone()))
        .collect();
    let pages = fixture
        .manager
        .complete_reverse_header_pages_request(ReverseHeaderPagesRequestOutcome {
            page_results: vec![
                HeaderPageResult {
                    page_index: 0,
                    requested: 2,
                    success: Some((
                        first,
                        vec![Header {
                            number: 5,
                            ..Default::default()
                        }],
                        Duration::from_millis(10),
                    )),
                    failures: Vec::new(),
                },
                HeaderPageResult {
                    page_index: 1,
                    requested: 2,
                    success: None,
                    failures: vec![(
                        later,
                        RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout),
                    )],
                },
            ],
            sessions,
        })
        .unwrap();
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].0, first);
    assert_eq!(pages[0].1[0].number, 5);
    assert!(fixture.manager.peers.contains_key(&first));
    assert!(!fixture.manager.peers.contains_key(&later));
}

#[tokio::test(start_paused = true)]
async fn reverse_header_prefix_never_resumes_after_an_unavailable_page() {
    for (first_empty, later_empty) in [(false, false), (true, false), (false, true)] {
        let mut fixture = Fixture::new().await;
        let first = PeerId::repeat_byte(1);
        let later = PeerId::repeat_byte(2);
        let sessions = fixture
            .manager
            .peers
            .iter()
            .map(|(id, peer)| (*id, peer.sender.clone()))
            .collect();
        let later_headers = if later_empty {
            Vec::new()
        } else {
            vec![Header {
                number: 3,
                ..Default::default()
            }]
        };
        let expected_bytes = if later_empty {
            0 // Empty replies retain the existing zero-progress accounting path.
        } else {
            headers_payload_bytes(&later_headers)
        };
        let result = fixture.manager.complete_reverse_header_pages_request(
            ReverseHeaderPagesRequestOutcome {
                page_results: vec![
                    HeaderPageResult {
                        page_index: 0,
                        requested: 1,
                        success: first_empty.then_some((
                            first,
                            Vec::new(),
                            Duration::from_millis(10),
                        )),
                        failures: Vec::new(),
                    },
                    HeaderPageResult {
                        page_index: 1,
                        requested: 1,
                        success: Some((later, later_headers, Duration::from_millis(10))),
                        failures: Vec::new(),
                    },
                ],
                sessions,
            },
        );
        if first_empty {
            assert!(result.unwrap().is_empty());
        } else {
            assert!(result.is_err());
        }
        assert_eq!(
            fixture
                .manager
                .session_metrics
                .p2p_download
                .snapshot(Instant::now())
                .total_payload_bytes,
            expected_bytes
        );
    }
}

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
    assert!(!fixture.manager.receipt_quarantine_history.contains_key(&id));
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
async fn partial_progress_receipts_restore_session_quarantine_once() {
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
