use super::*;
use std::task::Poll;

/// Reuse the dormant network fixture for engine lifecycle tests. The returned
/// resources keep its local channels, listener and temporary directory alive.
pub(crate) async fn engine_peer_fixture() -> (PeerManager, impl Sized) {
    let Fixture {
        manager,
        _network,
        _directory,
        receivers,
    } = Fixture::new().await;
    (manager, (_network, _directory, receivers))
}

pub(crate) fn empty_body_receipt_outcome() -> BodyReceiptRequestOutcome {
    BodyReceiptRequestOutcome {
        total_hashes: 0,
        return_blocks: 0,
        planned_return_blocks: 0,
        chunks: BTreeMap::new(),
        failures: Default::default(),
        stats: Default::default(),
        accounting_forwarded: false,
        sessions: HashMap::new(),
    }
}

pub(crate) fn empty_header_outcome() -> ReverseHeaderPagesRequestOutcome {
    ReverseHeaderPagesRequestOutcome {
        page_results: Vec::new(),
        sessions: HashMap::new(),
    }
}

/// Reth requires a real listener to construct its public handle. This manager is
/// normally retained without polling; publication tests explicitly poll once to
/// drain local commands. A TCP listener is bound on localhost:0, but no connection
/// task is started; discovery, DNS and network service tasks are disabled.
pub(super) struct Fixture {
    pub(super) manager: PeerManager,
    _network: NetworkManager<LogexNetworkPrimitives>,
    _directory: tempfile::TempDir,
    pub(super) receivers: Vec<mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>>,
}

impl Fixture {
    pub(super) fn poll_network_status_head(&mut self) -> B256 {
        let waker = futures_util::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(
            std::future::Future::poll(std::pin::Pin::new(&mut self._network), &mut context,)
                .is_pending()
        );
        self._network.status().eth_protocol_info.head
    }

    pub(super) async fn new() -> Self {
        let serve_cache = Arc::new(ServeCacheProvider::new());
        let config = NetworkConfigBuilder::<LogexNetworkPrimitives>::new(
            SecretKey::from_slice(&[1; 32]).unwrap(),
        )
        .listener_addr("127.0.0.1:0".parse().unwrap())
        .disable_discovery()
        .disable_nat()
        .disable_tx_gossip(true)
        .build(Arc::clone(&serve_cache));
        assert!(config.boot_nodes.is_empty());
        assert!(config.required_block_hashes.is_empty());
        assert!(config.discovery_v4_config.is_none());
        assert!(config.discovery_v5_config.is_none());
        assert!(config.dns_discovery_config.is_none());
        let network = NetworkManager::new(config).await.unwrap();
        assert!(network.local_addr().ip().is_loopback());
        let directory = tempfile::tempdir().unwrap();
        let local_head = Head::default();
        let mut manager = PeerManager {
            task_monitor: crate::tasks::TaskMonitor::default(),
            network: network.handle().clone(),
            network_task: None,
            eth_request_task: None,
            dns_discovery_task: None,
            network_events: Box::pin(futures_util::stream::pending()),
            discovery_events: Box::pin(futures_util::stream::pending()),
            dns_discovery_events: None,
            peers: HashMap::new(),
            peer_order: VecDeque::new(),
            request_cursor: 0,
            pending: HashMap::new(),
            pending_dials: HashMap::new(),
            disconnected_retries: HashMap::new(),
            saturated_peers: HashMap::new(),
            receipt_quarantine_history: HashMap::new(),
            productive: VecDeque::new(),
            known_peers: Vec::new(),
            configured_peer_ids: HashSet::new(),
            known_scan_cursor: 0,
            body_receipt_owners: BodyReceiptOwnerLedger::default(),
            known_peers_path: directory.path().join("known-peers.json"),
            persisted_known_peers: Vec::new(),
            serve_cache,
            last_advertised_range: BlockRangeUpdate {
                earliest: 0,
                latest: 0,
                latest_hash: MAINNET.genesis_hash(),
            },
            fork_filter: MAINNET.fork_filter(local_head),
            local_head,
            bind_ip: "127.0.0.1".parse().unwrap(),
            dial_families: DialAddressFamilies::IPV4,
            network_activated: false,
            max_peers: 3,
            session_metrics: ExecutionPeerSessionMetrics::default(),
            body_receipt_scheduler_metrics: BodyReceiptSchedulerMetrics::default(),
        };
        let mut receivers = Vec::new();
        for byte in 1..=3 {
            let id = PeerId::repeat_byte(byte);
            let (mut peer, receiver) = ownership_tests::test_session(id);
            peer.is_serving = true;
            peer.consecutive_timeouts = 0;
            peer.body_request_limit = 32;
            peer.receipt_request_limit = 32;
            peer.body_paused_until = None;
            peer.receipt_paused_until = None;
            peer.receipt_quarantined_until = None;
            manager.peers.insert(id, peer);
            manager.peer_order.push_back(id);
            receivers.push(receiver);
        }
        let peers = manager.peer_ids_for_requests(Some(1));
        for kind in [PeerRequestKind::Bodies, PeerRequestKind::Receipts] {
            assert!(manager.request_chunk_ranges(64, &peers, kind).len() >= 2);
        }
        Self {
            manager,
            _network: network,
            _directory: directory,
            receivers,
        }
    }
}

pub(super) fn hashes() -> Vec<B256> {
    (0..64).map(|byte| B256::repeat_byte(byte + 1)).collect()
}

pub(super) fn receipt_contexts(hashes: &[B256]) -> Vec<ReceiptRequestContext> {
    hashes
        .iter()
        .map(|hash| ReceiptRequestContext::test_with_hash(*hash, 1_000_000))
        .collect()
}

async fn fetch(manager: &mut PeerManager, kind: PeerRequestKind, limited: bool) -> Result<usize> {
    match (kind, limited) {
        (PeerRequestKind::Bodies, true) => manager
            .get_bodies_prefer_peers_with_limits(hashes(), 1, &[], Duration::from_secs(2), 2)
            .await
            .map(|bodies| bodies.len()),
        (PeerRequestKind::Bodies, false) => manager
            .get_bodies(hashes(), 1)
            .await
            .map(|bodies| bodies.len()),
        (PeerRequestKind::Receipts, true) => manager
            .get_receipts_prefer_peers_with_limits(
                receipt_contexts(&hashes()),
                1,
                &[],
                Duration::from_secs(2),
                2,
            )
            .await
            .map(|receipts| receipts.len()),
        (PeerRequestKind::Receipts, false) => manager
            .get_receipts(receipt_contexts(&hashes()), 1)
            .await
            .map(|receipts| receipts.len()),
        _ => unreachable!(),
    }
}

pub(super) fn take_requests(
    receivers: &mut [mpsc::Receiver<PeerRequest<LogexNetworkPrimitives>>],
) -> Vec<(usize, PeerRequest<LogexNetworkPrimitives>)> {
    let mut requests = Vec::new();
    for (index, receiver) in receivers.iter_mut().enumerate() {
        while let Ok(request) = receiver.try_recv() {
            requests.push((index, request));
        }
    }
    requests
}

fn response_closed(request: &PeerRequest<LogexNetworkPrimitives>) -> bool {
    match request {
        PeerRequest::GetBlockBodies { response, .. } => response.is_closed(),
        PeerRequest::GetReceipts69 { response, .. } => response.is_closed(),
        _ => panic!("unexpected fixture request"),
    }
}

fn requested_hashes(request: &PeerRequest<LogexNetworkPrimitives>) -> &[B256] {
    match request {
        PeerRequest::GetBlockBodies { request, .. } => &request.0,
        PeerRequest::GetReceipts69 { request, .. } => &request.0,
        _ => panic!("unexpected fixture request"),
    }
}

fn answer(request: PeerRequest<LogexNetworkPrimitives>, empty: bool) {
    let count = if empty {
        0
    } else {
        requested_hashes(&request).len()
    };
    answer_prefix(request, count);
}

fn answer_prefix(request: PeerRequest<LogexNetworkPrimitives>, count: usize) {
    assert!(count <= requested_hashes(&request).len());
    match request {
        PeerRequest::GetBlockBodies { response, .. } => {
            let _ = response.send(Ok(BlockBodies(vec![Default::default(); count])));
        }
        PeerRequest::GetReceipts69 { response, .. } => {
            let _ = response.send(Ok(Receipts69(vec![Vec::new(); count])));
        }
        _ => panic!("unexpected fixture request"),
    }
}

async fn limited_bulk_api_uses_two_second_request_deadline(kind: PeerRequestKind) {
    let mut fixture = Fixture::new().await;
    let future = fetch(&mut fixture.manager, kind, true);
    tokio::pin!(future);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let first = take_requests(&mut fixture.receivers);
    assert!(!first.is_empty());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    assert!(
        first.iter().all(|(_, request)| response_closed(request)),
        "explicit deadline must replace the bulk default timeout"
    );
}

async fn limited_bulk_api_selects_at_most_two_peers(kind: PeerRequestKind) {
    let mut fixture = Fixture::new().await;
    let future = fetch(&mut fixture.manager, kind, true);
    tokio::pin!(future);
    let mut selected = HashSet::new();
    let mut completed = false;
    for _ in 0..32 {
        match futures_util::poll!(future.as_mut()) {
            Poll::Ready(result) => {
                assert!(result.is_err());
                completed = true;
                break;
            }
            Poll::Pending => {
                let requests = take_requests(&mut fixture.receivers);
                assert!(
                    !requests.is_empty(),
                    "fixture must progress through local replies"
                );
                for (index, request) in requests {
                    selected.insert(index);
                    answer(request, true);
                }
            }
        }
    }
    assert!(completed);
    assert_eq!(
        selected.len(),
        2,
        "explicit peer-selection cap must include all work"
    );
}

async fn default_bulk_apis_keep_concurrent_chunks(kind: PeerRequestKind) {
    let mut fixture = Fixture::new().await;
    let future = fetch(&mut fixture.manager, kind, false);
    tokio::pin!(future);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let requests = take_requests(&mut fixture.receivers);
    assert!(
        requests.len() >= 2,
        "bulk chunks must be concurrently admitted before replies"
    );
    for (_, request) in requests {
        answer(request, false);
    }
    assert_eq!(future.await.unwrap(), 64);
}

async fn limited_bulk_partial_prefix_continues_on_the_same_peer(kind: PeerRequestKind) {
    let mut fixture = Fixture::new().await;
    let future = fetch(&mut fixture.manager, kind, true);
    tokio::pin!(future);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (first_peer, first) = requests.pop().unwrap();
    assert_eq!(requested_hashes(&first), hashes());
    answer_prefix(first, 32);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (next_peer, next) = requests.pop().unwrap();
    assert_eq!(next_peer, first_peer);
    assert_eq!(requested_hashes(&next), &hashes()[32..]);
    answer_prefix(next, 32);
    assert_eq!(future.await.unwrap(), 64);
}

#[tokio::test(start_paused = true)]
async fn limited_bulk_api_uses_two_second_request_deadline_bodies() {
    limited_bulk_api_uses_two_second_request_deadline(PeerRequestKind::Bodies).await;
}

#[tokio::test(start_paused = true)]
async fn limited_bulk_api_uses_two_second_request_deadline_receipts() {
    limited_bulk_api_uses_two_second_request_deadline(PeerRequestKind::Receipts).await;
}

#[tokio::test(start_paused = true)]
async fn limited_bulk_api_selects_at_most_two_peers_bodies() {
    limited_bulk_api_selects_at_most_two_peers(PeerRequestKind::Bodies).await;
}

#[tokio::test(start_paused = true)]
async fn limited_bulk_api_selects_at_most_two_peers_receipts() {
    limited_bulk_api_selects_at_most_two_peers(PeerRequestKind::Receipts).await;
}

#[tokio::test(start_paused = true)]
async fn default_bulk_apis_keep_concurrent_chunks_bodies() {
    default_bulk_apis_keep_concurrent_chunks(PeerRequestKind::Bodies).await;
}

#[tokio::test(start_paused = true)]
async fn default_bulk_apis_keep_concurrent_chunks_receipts() {
    default_bulk_apis_keep_concurrent_chunks(PeerRequestKind::Receipts).await;
}

#[tokio::test(start_paused = true)]
async fn limited_bulk_partial_prefix_continues_on_the_same_peer_bodies() {
    limited_bulk_partial_prefix_continues_on_the_same_peer(PeerRequestKind::Bodies).await;
}

#[tokio::test(start_paused = true)]
async fn limited_bulk_partial_prefix_continues_on_the_same_peer_receipts() {
    limited_bulk_partial_prefix_continues_on_the_same_peer(PeerRequestKind::Receipts).await;
}

#[tokio::test(start_paused = true)]
async fn limited_reverse_headers_preserve_hash_direction_and_partial_response() {
    let mut fixture = Fixture::new().await;
    let start = BlockHashOrNumber::Hash(B256::repeat_byte(17));
    let mut future = Box::pin(fixture.manager.get_headers_reverse_with_limits(
        start,
        3,
        Duration::from_secs(2),
        2,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (peer_index, PeerRequest::GetBlockHeaders { request, response }) = requests.pop().unwrap()
    else {
        panic!("expected one local header request");
    };
    assert_eq!(request.start_block, start);
    assert_eq!(request.limit, 3);
    assert_eq!(request.skip, 0);
    assert_eq!(request.direction, reth_eth_wire::HeadersDirection::Falling);
    let headers = vec![alloy_consensus::Header {
        number: 12,
        ..Default::default()
    }];
    response.send(Ok(BlockHeaders(headers.clone()))).unwrap();
    let (peer, actual) = future.await.unwrap();
    assert_eq!(peer, PeerId::repeat_byte((peer_index + 1) as u8));
    assert_eq!(actual, headers);
}

#[tokio::test(start_paused = true)]
async fn limited_reverse_headers_apply_exchange_timeout_and_peer_limit() {
    let mut fixture = Fixture::new().await;
    let mut future = Box::pin(fixture.manager.get_headers_reverse_with_limits(
        BlockHashOrNumber::Number(1),
        1,
        Duration::from_secs(2),
        2,
    ));
    let mut selected = HashSet::new();
    for _ in 0..2 {
        assert!(futures_util::poll!(&mut future).is_pending());
        let mut requests = take_requests(&mut fixture.receivers);
        assert_eq!(requests.len(), 1);
        let (peer, PeerRequest::GetBlockHeaders { response, .. }) = requests.pop().unwrap() else {
            panic!("expected one local header request");
        };
        assert!(selected.insert(peer));
        tokio::time::advance(Duration::from_secs(2)).await;
        // Polling the next iteration starts the next bounded attempt. The
        // response receiver must close when its own deadline is observed.
        let outcome = futures_util::poll!(&mut future);
        assert!(response.is_closed());
        if selected.len() == 2 {
            assert!(matches!(outcome, Poll::Ready(Err(_))));
        } else {
            assert!(outcome.is_pending());
        }
    }
    assert!(take_requests(&mut fixture.receivers).is_empty());
}

#[tokio::test(start_paused = true)]
async fn canceled_limited_reverse_headers_allow_a_later_independent_request() {
    let mut fixture = Fixture::new().await;
    let mut future = Box::pin(fixture.manager.get_headers_reverse_with_limits(
        BlockHashOrNumber::Number(1),
        1,
        Duration::from_secs(2),
        1,
    ));
    assert!(futures_util::poll!(&mut future).is_pending());
    let (_, PeerRequest::GetBlockHeaders { response, .. }) =
        take_requests(&mut fixture.receivers).pop().unwrap()
    else {
        panic!("expected one local header request");
    };
    drop(future);
    assert!(response.is_closed());
    let mut next = Box::pin(fixture.manager.get_headers_reverse_with_limits(
        BlockHashOrNumber::Number(1),
        1,
        Duration::from_secs(2),
        1,
    ));
    assert!(futures_util::poll!(&mut next).is_pending());
    let (_, PeerRequest::GetBlockHeaders { response, .. }) =
        take_requests(&mut fixture.receivers).pop().unwrap()
    else {
        panic!("expected the independent local header request");
    };
    response
        .send(Ok(BlockHeaders(vec![alloy_consensus::Header::default()])))
        .unwrap();
    assert_eq!(next.await.unwrap().1.len(), 1);
}
