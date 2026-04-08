use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use eyre::{Result, bail};
use futures_util::{FutureExt, StreamExt};
use reth_chainspec::{EthChainSpec, MAINNET};
use reth_discv4::{Discv4Config, NatResolver};
use reth_eth_wire::{
    BlockBodies, BlockHeaders, DisconnectReason, EthVersion, GetBlockBodies, GetBlockHeaders,
    GetReceipts, GetReceipts70, HeadersDirection, NetworkPrimitives, Receipts, Receipts69,
    Receipts70, UnifiedStatus,
};
use reth_ethereum_forks::Head;
use reth_network::types::peers::config::PeerBackoffDurations;
use reth_network::types::{PeerKind, ReputationChangeKind};
use reth_network::{
    DiscoveredEvent, DiscoveryEvent, NetworkConfigBuilder, NetworkEvent,
    NetworkEventListenerProvider, NetworkHandle, NetworkManager, PeerRequest, PeerRequestSender,
    Peers, PeersConfig, PeersInfo, SessionsConfig,
};
use reth_network_peers::{NodeRecord, PeerId, TrustedPeer, mainnet_nodes};
use secp256k1::SecretKey;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_stream::Stream;
use tracing::{debug, info, warn};

use crate::p2p::persistence::persist_known_peers_if_changed;
use crate::p2p::serve_cache::ServeCacheProvider;
use crate::primitives::LogexNetworkPrimitives;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_WAIT: Duration = Duration::from_secs(2);
const FILL_BUDGET: Duration = Duration::from_secs(12);
const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);
const REFILL_SLOTS_INTERVAL: Duration = Duration::from_millis(800);
const NETWORK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_HANDLER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const EARLY_SESSION_DROP_THRESHOLD: Duration = Duration::from_secs(30);
const MAX_PERSISTED_PEERS: usize = 512;
const MAX_TRACKED_PENDING: usize = 4096;
const MAX_CONSECUTIVE_TIMEOUTS: u32 = 2;
const MAX_CONCURRENT_OUTBOUND_DIALS: usize = 50;
const DIAL_BACKOFF_DURATIONS: PeerBackoffDurations = PeerBackoffDurations {
    low: Duration::from_secs(5),
    medium: Duration::from_secs(30),
    high: Duration::from_secs(60 * 5),
    max: Duration::from_secs(60 * 15),
};

static MAINNET_BOOTNODE_IDS: LazyLock<HashSet<PeerId>> =
    LazyLock::new(|| mainnet_nodes().into_iter().map(|node| node.id).collect());

type NetworkEvents =
    Pin<Box<dyn Stream<Item = NetworkEvent<PeerRequest<LogexNetworkPrimitives>>> + Send>>;
type DiscoveryEvents = Pin<Box<dyn Stream<Item = DiscoveryEvent> + Send>>;

/// Manages peer sessions and request routing on top of Reth's real network stack.
pub struct PeerManager {
    network: NetworkHandle<LogexNetworkPrimitives>,
    network_task: Option<JoinHandle<()>>,
    eth_request_task: Option<JoinHandle<()>>,
    network_events: NetworkEvents,
    discovery_events: DiscoveryEvents,
    peers: HashMap<PeerId, ActivePeer>,
    peer_order: VecDeque<PeerId>,
    pending: HashMap<PeerId, NodeRecord>,
    productive: VecDeque<NodeRecord>,
    known_peers: Vec<NodeRecord>,
    known_peers_path: PathBuf,
    persisted_known_peers: Vec<NodeRecord>,
    serve_cache: Arc<ServeCacheProvider>,
}

#[derive(Clone)]
struct ActivePeer {
    sender: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    remote_record: NodeRecord,
    remote_status: UnifiedStatus,
    version: EthVersion,
    is_serving: bool,
    consecutive_timeouts: u32,
    connected_at: Instant,
}

impl PeerManager {
    /// Create a new peer manager backed by Reth's network/session stack.
    pub async fn new(
        secret_key: SecretKey,
        listener_port: u16,
        discovery_port: u16,
        max_peers: usize,
        our_head: Head,
        known_peers: Vec<NodeRecord>,
        known_peers_path: PathBuf,
    ) -> Result<Self> {
        let basic_nodes: HashSet<NodeRecord> = known_peers.iter().copied().collect();
        let trusted_nodes: Vec<TrustedPeer> =
            known_peers.iter().copied().map(TrustedPeer::from).collect();
        let serve_cache = Arc::new(ServeCacheProvider::new());
        let peer_config = PeersConfig::default()
            .with_basic_nodes(basic_nodes)
            .with_trusted_nodes(trusted_nodes)
            .with_max_outbound(max_peers)
            .with_max_inbound(max_peers.max(16))
            .with_max_concurrent_dials(max_peers.clamp(8, MAX_CONCURRENT_OUTBOUND_DIALS))
            .with_refill_slots_interval(REFILL_SLOTS_INTERVAL)
            .with_backoff_durations(DIAL_BACKOFF_DURATIONS)
            .with_enforce_enr_fork_id(false);
        let sessions_config =
            SessionsConfig::default().with_upscaled_event_buffer(peer_config.max_peers());

        let mut discovery = Discv4Config::builder();
        discovery
            .lookup_interval(DISCOVERY_LOOKUP_INTERVAL)
            .ping_interval(DISCOVERY_PING_INTERVAL);

        let listener_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), listener_port);
        let discovery_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), discovery_port);
        let network_head = normalize_network_head(our_head);

        let config = NetworkConfigBuilder::<LogexNetworkPrimitives>::new(secret_key)
            .set_head(network_head)
            .listener_addr(listener_addr)
            .discovery_addr(discovery_addr)
            .external_ip_resolver(NatResolver::Any)
            .sessions_config(sessions_config)
            .peer_config(peer_config)
            .mainnet_boot_nodes()
            .disable_tx_gossip(true)
            .discovery(discovery)
            .build(Arc::clone(&serve_cache));

        let builder = NetworkManager::builder(config)
            .await
            .map_err(|error| eyre::eyre!("failed to start p2p network: {error}"))?;
        let (handle, network, _, request_handler) = builder
            .request_handler(Arc::clone(&serve_cache))
            .split_with_handle();
        let local_record = handle.local_node_record();
        let local_enr = handle.local_enr();
        let network_events = Box::pin(handle.event_listener());
        let discovery_events = Box::pin(handle.discovery_listener());
        let network_task = tokio::spawn(network);
        let eth_request_task = tokio::spawn(request_handler);

        let mut manager = Self {
            network: handle,
            network_task: Some(network_task),
            eth_request_task: Some(eth_request_task),
            network_events,
            discovery_events,
            peers: HashMap::new(),
            peer_order: VecDeque::new(),
            pending: HashMap::new(),
            productive: VecDeque::new(),
            known_peers,
            known_peers_path,
            persisted_known_peers: Vec::new(),
            serve_cache,
        };

        manager.seed_known_peers(true);
        manager.persisted_known_peers = manager.known_peers();

        info!(
            peer_id = %manager.network.peer_id(),
            enode = %local_record,
            enr = %local_enr,
            listener = %local_record.tcp_addr(),
            discovery = %discovery_addr,
            "p2p networking started"
        );

        Ok(manager)
    }

    /// Update our local head view and propagate it into Reth's live network
    /// status so newly established sessions see the same canonical tip.
    pub fn set_head(&mut self, head: Head) {
        self.network.update_status(normalize_network_head(head));
    }

    pub fn cache_canonical_block(
        &self,
        header: <LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader,
        body: <LogexNetworkPrimitives as NetworkPrimitives>::BlockBody,
        receipts: &[alloy_consensus::ReceiptWithBloom<
            <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
        >],
    ) {
        self.serve_cache.insert_block(header, body, receipts);
    }

    pub fn remove_cached_blocks(&self, reverted_hashes: &[B256]) {
        self.serve_cache.remove_blocks(reverted_hashes);
    }

    /// Gracefully stop the network manager and wait for the background task.
    pub async fn shutdown(&mut self) {
        self.network_events = Box::pin(tokio_stream::empty());
        self.discovery_events = Box::pin(tokio_stream::empty());

        match timeout(NETWORK_SHUTDOWN_TIMEOUT, self.network.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%error, "failed to shut down p2p network cleanly");
            }
            Err(_) => {
                warn!(
                    ?NETWORK_SHUTDOWN_TIMEOUT,
                    "timed out waiting for p2p network shutdown acknowledgement"
                );
            }
        }

        if let Some(mut task) = self.network_task.take() {
            match timeout(NETWORK_SHUTDOWN_TIMEOUT, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(%error, "p2p network task exited unexpectedly");
                }
                Err(_) => {
                    warn!(
                        ?NETWORK_SHUTDOWN_TIMEOUT,
                        "timed out waiting for p2p network task to stop, aborting it"
                    );
                    task.abort();
                    if let Err(error) = task.await
                        && !error.is_cancelled()
                    {
                        warn!(%error, "p2p network task aborted with an unexpected error");
                    }
                }
            }
        }

        if let Some(mut task) = self.eth_request_task.take() {
            match timeout(REQUEST_HANDLER_SHUTDOWN_TIMEOUT, &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(%error, "eth request handler task exited unexpectedly");
                }
                Err(_) => {
                    debug!(
                        ?REQUEST_HANDLER_SHUTDOWN_TIMEOUT,
                        "timed out waiting for eth request handler task to stop, aborting it"
                    );
                    task.abort();
                    if let Err(error) = task.await
                        && !error.is_cancelled()
                    {
                        warn!(%error, "eth request handler task aborted with an unexpected error");
                    }
                }
            }
        }
    }

    /// Number of currently connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Number of connected peers that have actually served sync data.
    pub fn serving_peer_count(&self) -> usize {
        self.peers.values().filter(|peer| peer.is_serving).count()
    }

    /// Highest advertised canonical block across connected peers.
    pub fn highest_peer_block(&self) -> Option<u64> {
        self.peers
            .values()
            .filter_map(|peer| peer.remote_status.latest_block)
            .filter(|block| *block > 0)
            .max()
    }

    /// Number of queued peer candidates awaiting or undergoing connection attempts.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Snapshot of productive peers suitable for writing to disk on shutdown.
    pub fn known_peers(&self) -> Vec<NodeRecord> {
        let mut peers = Vec::with_capacity(MAX_PERSISTED_PEERS);

        for peer in &self.productive {
            if is_bootstrap_node(peer.id) {
                continue;
            }
            push_unique_peer(&mut peers, *peer);
            if peers.len() >= MAX_PERSISTED_PEERS {
                break;
            }
        }

        peers
    }

    /// Wait until we have at least `min` peers or run out of budget.
    pub async fn fill_peers(&mut self, min: usize, target: usize) {
        self.drain_events_now();
        if self.peers.len() >= min || self.peers.len() >= target {
            return;
        }

        let deadline = Instant::now() + FILL_BUDGET;

        loop {
            self.drain_events_now();

            if self.peers.len() >= min || self.peers.len() >= target {
                return;
            }

            if Instant::now() >= deadline {
                debug!(
                    connected_peers = self.peers.len(),
                    pending_peers = self.pending.len(),
                    target,
                    "fill_peers budget exhausted"
                );
                return;
            }

            let remaining = (deadline - Instant::now()).min(DISCOVERY_WAIT);
            if !self.wait_for_activity(remaining).await {
                debug!(
                    connected_peers = self.peers.len(),
                    pending_peers = self.pending.len(),
                    "no network activity while waiting for peers during this refill interval"
                );
                continue;
            }
        }
    }

    /// Request block headers starting at `start_block` for `count` blocks.
    pub async fn get_headers(
        &mut self,
        start_block: u64,
        count: u64,
    ) -> Result<Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>> {
        self.drain_events_now();

        let request = GetBlockHeaders {
            start_block: start_block.into(),
            limit: count,
            skip: 0,
            direction: HeadersDirection::Rising,
        };

        self.send_request_to_any_peer(move |peer| PeerRequest::GetBlockHeaders {
            request,
            response: peer,
        })
        .await
    }

    /// Request block bodies for the given block hashes.
    pub async fn get_bodies(
        &mut self,
        hashes: Vec<B256>,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
    )> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok((PeerId::ZERO, Vec::new()));
        }

        let requested = hashes.len();
        let peer_ids = self.peer_ids_for_requests();
        let mut dead_peers = HashSet::new();

        for peer_id in peer_ids {
            let request_hashes = hashes.clone();
            match self
                .request_with_channel(peer_id, &move |peer| PeerRequest::GetBlockBodies {
                    request: GetBlockBodies(request_hashes.clone()),
                    response: peer,
                })
                .await
            {
                Ok(response) => {
                    if response_len_matches_request(requested, response.len()) {
                        self.on_request_success(peer_id);
                        return Ok((peer_id, response));
                    }

                    self.on_incomplete_response(peer_id, "block bodies", requested, response.len());
                    dead_peers.insert(peer_id);
                }
                Err(error) => {
                    let should_drop = self.on_request_error(peer_id, &error);
                    debug!(peer = %peer_id, ?error, "body request failed");
                    if should_drop {
                        dead_peers.insert(peer_id);
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        bail!("no peers available to handle block body request")
    }

    /// Request receipts for the given block hashes.
    pub async fn get_receipts(
        &mut self,
        hashes: Vec<B256>,
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok((PeerId::ZERO, Vec::new()));
        }

        let requested = hashes.len();
        let peer_ids = self.peer_ids_for_requests();
        let mut dead_peers = HashSet::new();

        for peer_id in peer_ids {
            let version = match self.peers.get(&peer_id) {
                Some(peer) => peer.version,
                None => continue,
            };

            let attempt = if version >= EthVersion::Eth70 {
                self.request_receipts70(peer_id, hashes.clone()).await
            } else if version >= EthVersion::Eth69 {
                self.request_receipts69(peer_id, hashes.clone()).await
            } else {
                self.request_receipts(peer_id, hashes.clone()).await
            };

            match attempt {
                Ok(receipts) => {
                    if response_len_matches_request(requested, receipts.len()) {
                        if !receipts.is_empty() && self.mark_peer_serving(peer_id) {
                            self.persist_productive_peers();
                        }
                        self.on_request_success(peer_id);
                        return Ok((peer_id, receipts));
                    }

                    self.on_incomplete_response(peer_id, "receipts", requested, receipts.len());
                    dead_peers.insert(peer_id);
                }
                Err(error) => {
                    let should_drop = self.on_request_error(peer_id, &error);
                    debug!(peer = %peer_id, ?error, "receipt request failed");
                    if should_drop {
                        dead_peers.insert(peer_id);
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        bail!("no peers available to handle receipt request")
    }

    /// Disconnect and de-prioritize a peer that served invalid block data.
    pub fn report_invalid_block_data(&mut self, peer_id: PeerId, response_kind: &'static str) {
        if peer_id == PeerId::ZERO {
            return;
        }

        self.network
            .reputation_change(peer_id, ReputationChangeKind::BadMessage);
        self.network.disconnect_peer(peer_id);
        let known_changed = self.forget_peer(peer_id);
        if known_changed {
            self.persist_productive_peers();
        }
        warn!(
            peer = %peer_id,
            response_kind,
            "peer served invalid block data, disconnecting it"
        );
    }

    fn on_incomplete_response(
        &mut self,
        peer_id: PeerId,
        response_kind: &'static str,
        requested: usize,
        returned: usize,
    ) {
        self.network
            .reputation_change(peer_id, ReputationChangeKind::BadMessage);
        warn!(
            peer = %peer_id,
            response_kind,
            requested,
            returned,
            "peer returned an incomplete response, disconnecting it"
        );
    }

    async fn send_request_to_any_peer<T, W, MakeRequest>(
        &mut self,
        make_request: MakeRequest,
    ) -> Result<T>
    where
        W: IntoResponseValue<T>,
        MakeRequest: Fn(
            oneshot::Sender<reth_network::p2p::error::RequestResult<W>>,
        ) -> PeerRequest<LogexNetworkPrimitives>,
    {
        let peer_ids = self.peer_ids_for_requests();
        let mut dead_peers = HashSet::new();

        for peer_id in peer_ids {
            match self.request_with_channel(peer_id, &make_request).await {
                Ok(response) => {
                    self.on_request_success(peer_id);
                    return Ok(response);
                }
                Err(error) => {
                    let should_drop = self.on_request_error(peer_id, &error);
                    debug!(peer = %peer_id, ?error, "peer request failed");
                    if should_drop {
                        dead_peers.insert(peer_id);
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        bail!("no peers available to handle request")
    }

    async fn request_with_channel<T, W, MakeRequest>(
        &self,
        peer_id: PeerId,
        make_request: &MakeRequest,
    ) -> std::result::Result<T, RequestAttempt>
    where
        W: IntoResponseValue<T>,
        MakeRequest: Fn(
            oneshot::Sender<reth_network::p2p::error::RequestResult<W>>,
        ) -> PeerRequest<LogexNetworkPrimitives>,
    {
        let Some(peer) = self.peers.get(&peer_id) else {
            return Err(RequestAttempt::Disconnected);
        };

        let sender = peer.sender.clone();
        let (response_tx, response_rx) = oneshot::channel();
        sender
            .to_session_tx
            .send(make_request(response_tx))
            .await
            .map_err(|_| RequestAttempt::Disconnected)?;

        match timeout(REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(Ok(response))) => Ok(response.into_value()),
            Ok(Ok(Err(error))) => Err(RequestAttempt::Request(error)),
            Ok(Err(_)) => Err(RequestAttempt::Disconnected),
            Err(_) => Err(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::Timeout,
            )),
        }
    }

    async fn request_receipts(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts {
            request: GetReceipts(hashes.clone()),
            response,
        })
        .await
    }

    async fn request_receipts69(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let receipts: Vec<Vec<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>> = self
            .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts69 {
                request: GetReceipts(hashes.clone()),
                response,
            })
            .await?;
        Ok(Receipts69(receipts).into_with_bloom().0)
    }

    async fn request_receipts70(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let mut merged = Vec::with_capacity(hashes.len());
        let mut next_block_index = 0usize;
        let mut first_block_receipt_index = 0u64;

        while next_block_index < hashes.len() {
            let request_hashes = hashes[next_block_index..].to_vec();
            let response: Receipts70<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt> = self
                .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts70 {
                    request: GetReceipts70 {
                        first_block_receipt_index,
                        block_hashes: request_hashes.clone(),
                    },
                    response,
                })
                .await?;

            let (updated_block_index, updated_receipt_index) = merge_receipts70_response(
                &mut merged,
                next_block_index,
                first_block_receipt_index,
                response,
                hashes.len(),
            )
            .map_err(Receipts70MergeError::into_request_attempt)?;

            next_block_index = updated_block_index;
            first_block_receipt_index = updated_receipt_index;
        }

        Ok(merged)
    }

    fn seed_known_peers(&mut self, force: bool) {
        if !force {
            return;
        }

        for peer in self.known_peers.clone() {
            if is_bootstrap_node(peer.id)
                || peer.tcp_port == 0
                || self.peers.contains_key(&peer.id)
                || self.pending.contains_key(&peer.id)
            {
                continue;
            }

            self.pending.insert(peer.id, peer);
            self.network.connect_peer_kind(
                peer.id,
                PeerKind::Trusted,
                peer.tcp_addr(),
                Some(peer.udp_addr()),
            );
        }
    }

    fn drain_events_now(&mut self) {
        while let Some(event) = self.network_events.next().now_or_never().flatten() {
            self.handle_network_event(event);
        }
        while let Some(event) = self.discovery_events.next().now_or_never().flatten() {
            self.handle_discovery_event(event);
        }
    }

    async fn wait_for_activity(&mut self, max_wait: Duration) -> bool {
        let delay = tokio::time::sleep(max_wait);
        tokio::pin!(delay);

        tokio::select! {
            maybe_event = self.network_events.next() => {
                if let Some(event) = maybe_event {
                    self.handle_network_event(event);
                    true
                } else {
                    warn!("network event stream closed");
                    false
                }
            }
            maybe_event = self.discovery_events.next() => {
                if let Some(event) = maybe_event {
                    self.handle_discovery_event(event);
                    true
                } else {
                    warn!("discovery event stream closed");
                    false
                }
            }
            _ = &mut delay => false,
        }
    }

    fn handle_network_event(&mut self, event: NetworkEvent<PeerRequest<LogexNetworkPrimitives>>) {
        match event {
            NetworkEvent::Peer(reth_network::events::PeerEvent::SessionClosed {
                peer_id,
                reason,
            }) => {
                self.log_session_closed(peer_id, reason);
                self.remove_peer(peer_id);
            }
            NetworkEvent::Peer(reth_network::events::PeerEvent::PeerRemoved(peer_id)) => {
                self.remove_peer(peer_id);
            }
            NetworkEvent::Peer(_) => {}
            NetworkEvent::ActivePeerSession { info, messages } => {
                self.insert_peer(info, messages);
            }
        }
    }

    fn handle_discovery_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::NewNode(DiscoveredEvent::EventQueued { peer_id, addr, .. }) => {
                let node = NodeRecord::new_with_ports(
                    addr.tcp().ip(),
                    addr.tcp().port(),
                    addr.udp().map(|socket| socket.port()),
                    peer_id,
                );
                self.remember_pending(node);
            }
            DiscoveryEvent::EnrForkId(node, _) => {
                self.remember_pending(node);
            }
        }
    }

    fn insert_peer(
        &mut self,
        info: reth_network::events::SessionInfo,
        messages: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    ) {
        let mut record = self
            .pending
            .remove(&info.peer_id)
            .unwrap_or_else(|| NodeRecord::new(info.remote_addr, info.peer_id));
        record = record.with_tcp_port(info.remote_addr.port());

        let was_productive = self.productive.iter().any(|peer| peer.id == info.peer_id);
        let peer = ActivePeer {
            sender: messages,
            remote_record: record,
            remote_status: *info.status,
            version: info.version,
            is_serving: false,
            consecutive_timeouts: 0,
            connected_at: Instant::now(),
        };

        self.peers.insert(info.peer_id, peer);
        self.peer_order.retain(|peer_id| *peer_id != info.peer_id);
        if was_productive {
            self.peer_order.push_front(info.peer_id);
        } else {
            self.peer_order.push_back(info.peer_id);
        }

        debug!(
            peer = %info.peer_id,
            remote_addr = %info.remote_addr,
            latest_block = info.status.latest_block.unwrap_or_default(),
            version = ?info.version,
            "peer session established"
        );
    }

    fn remember_pending(&mut self, node: NodeRecord) {
        if is_bootstrap_node(node.id) || node.tcp_port == 0 || self.peers.contains_key(&node.id) {
            return;
        }
        if self.pending.len() >= MAX_TRACKED_PENDING && !self.pending.contains_key(&node.id) {
            return;
        }
        self.pending.insert(node.id, node);
    }

    fn peer_ids_for_requests(&self) -> Vec<PeerId> {
        self.peer_order
            .iter()
            .filter(|peer_id| self.peers.contains_key(*peer_id))
            .copied()
            .collect()
    }

    fn on_request_success(&mut self, peer_id: PeerId) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.consecutive_timeouts = 0;
        }
        self.promote_peer(peer_id);
    }

    fn on_request_error(&mut self, peer_id: PeerId, error: &RequestAttempt) -> bool {
        match error {
            RequestAttempt::Disconnected => true,
            RequestAttempt::Request(request_error) => match request_error {
                reth_network::p2p::error::RequestError::Timeout => {
                    self.network
                        .reputation_change(peer_id, ReputationChangeKind::Timeout);
                    self.record_timeout(peer_id) >= MAX_CONSECUTIVE_TIMEOUTS
                }
                reth_network::p2p::error::RequestError::BadResponse => {
                    self.network
                        .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
                    true
                }
                reth_network::p2p::error::RequestError::UnsupportedCapability => {
                    self.network
                        .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
                    true
                }
                reth_network::p2p::error::RequestError::ChannelClosed
                | reth_network::p2p::error::RequestError::ConnectionDropped => true,
            },
        }
    }

    fn record_timeout(&mut self, peer_id: PeerId) -> u32 {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return MAX_CONSECUTIVE_TIMEOUTS;
        };
        peer.consecutive_timeouts += 1;
        let consecutive_timeouts = peer.consecutive_timeouts;
        let _ = peer;
        self.demote_peer(peer_id);
        consecutive_timeouts
    }

    fn mark_peer_serving(&mut self, peer_id: PeerId) -> bool {
        let productive = if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.is_serving = true;
            Some(peer.remote_record)
        } else {
            None
        };

        if let Some(peer) = productive {
            return self.remember_productive(peer);
        }
        false
    }

    fn remember_productive(&mut self, node: NodeRecord) -> bool {
        let existing_index = self
            .productive
            .iter()
            .position(|productive| productive.id == node.id);
        if let Some(index) = existing_index {
            self.productive.remove(index);
        }

        self.productive.push_front(node);
        while self.productive.len() > MAX_PERSISTED_PEERS {
            self.productive.pop_back();
        }

        let known_changed = upsert_known_peer(&mut self.known_peers, node);
        existing_index != Some(0) || known_changed
    }

    fn persist_productive_peers(&mut self) {
        let peers = self.known_peers();
        match persist_known_peers_if_changed(
            &self.known_peers_path,
            &peers,
            &mut self.persisted_known_peers,
        ) {
            Ok(true) => {
                info!(
                    peers = peers.len(),
                    path = %self.known_peers_path.display(),
                    "persisted known peers after serving peer update"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    error = %error,
                    path = %self.known_peers_path.display(),
                    "failed to persist known peers after serving peer update"
                );
            }
        }
    }

    fn promote_peer(&mut self, peer_id: PeerId) {
        self.peer_order.retain(|id| *id != peer_id);
        self.peer_order.push_front(peer_id);
    }

    fn demote_peer(&mut self, peer_id: PeerId) {
        self.peer_order.retain(|id| *id != peer_id);
        self.peer_order.push_back(peer_id);
    }

    fn remove_dead_peers(&mut self, dead_peers: &HashSet<PeerId>) {
        if dead_peers.is_empty() {
            return;
        }

        for peer_id in dead_peers {
            self.network.disconnect_peer(*peer_id);
            self.remove_peer(*peer_id);
        }
    }

    fn forget_peer(&mut self, peer_id: PeerId) -> bool {
        self.peers.remove(&peer_id);
        self.pending.remove(&peer_id);
        self.peer_order.retain(|id| *id != peer_id);
        let productive_before = self.productive.len();
        self.productive.retain(|peer| peer.id != peer_id);
        let known_before = self.known_peers.len();
        self.known_peers.retain(|peer| peer.id != peer_id);
        productive_before != self.productive.len() || known_before != self.known_peers.len()
    }

    fn remove_peer(&mut self, peer_id: PeerId) {
        self.pending.remove(&peer_id);
        if let Some(peer) = self.peers.remove(&peer_id)
            && peer.is_serving
        {
            self.remember_productive(peer.remote_record);
        }
        self.peer_order.retain(|id| *id != peer_id);
    }

    fn log_session_closed(&self, peer_id: PeerId, reason: Option<DisconnectReason>) {
        let Some(peer) = self.peers.get(&peer_id) else {
            debug!(peer = %peer_id, ?reason, "peer session closed");
            return;
        };

        let connected_for = peer.connected_at.elapsed();
        let latest_block = peer.remote_status.latest_block.unwrap_or_default();
        let saturated_remote = matches!(reason, Some(DisconnectReason::TooManyPeers));
        let noisy_remote_rejection =
            saturated_remote && !peer.is_serving && connected_for <= EARLY_SESSION_DROP_THRESHOLD;

        if noisy_remote_rejection {
            debug!(
                peer = %peer_id,
                remote_addr = %peer.remote_record.tcp_addr(),
                ?reason,
                ?connected_for,
                serving = peer.is_serving,
                version = ?peer.version,
                latest_block,
                saturated_remote,
                "peer session closed"
            );
        } else if reason.is_some() || connected_for <= EARLY_SESSION_DROP_THRESHOLD {
            info!(
                peer = %peer_id,
                remote_addr = %peer.remote_record.tcp_addr(),
                ?reason,
                ?connected_for,
                serving = peer.is_serving,
                version = ?peer.version,
                latest_block,
                saturated_remote,
                "peer session closed"
            );
        } else {
            debug!(
                peer = %peer_id,
                remote_addr = %peer.remote_record.tcp_addr(),
                ?reason,
                ?connected_for,
                serving = peer.is_serving,
                version = ?peer.version,
                latest_block,
                saturated_remote,
                "peer session closed"
            );
        }
    }
}

#[derive(Debug, Clone)]
enum RequestAttempt {
    Disconnected,
    Request(reth_network::p2p::error::RequestError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Receipts70MergeError {
    EmptyResponse,
    ResponseOverflow,
    UnexpectedAppend,
    NoProgress,
}

impl Receipts70MergeError {
    fn into_request_attempt(self) -> RequestAttempt {
        RequestAttempt::Request(reth_network::p2p::error::RequestError::BadResponse)
    }
}

fn push_unique_peer(peers: &mut Vec<NodeRecord>, node: NodeRecord) {
    if peers.iter().any(|peer| peer.id == node.id) {
        return;
    }
    peers.push(node);
}

fn upsert_known_peer(known_peers: &mut Vec<NodeRecord>, node: NodeRecord) -> bool {
    if let Some(existing) = known_peers.iter_mut().find(|peer| peer.id == node.id) {
        if *existing == node {
            return false;
        }
        *existing = node;
        return true;
    }

    known_peers.push(node);
    true
}

fn is_bootstrap_node(id: PeerId) -> bool {
    MAINNET_BOOTNODE_IDS.contains(&id)
}

fn response_len_matches_request(requested: usize, returned: usize) -> bool {
    requested == returned
}

fn normalize_network_head(mut head: Head) -> Head {
    if head.hash.is_zero() {
        head.hash = MAINNET.genesis_hash();
    }
    if head.number == 0 && head.timestamp == 0 {
        head.timestamp = MAINNET.genesis().timestamp;
    }
    head
}

fn merge_receipts70_response<T>(
    merged: &mut Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>>,
    next_block_index: usize,
    first_block_receipt_index: u64,
    response: Receipts70<T>,
    expected_blocks: usize,
) -> std::result::Result<(usize, u64), Receipts70MergeError>
where
    T: alloy_consensus::TxReceipt,
{
    let previous_state = (next_block_index, first_block_receipt_index);
    let returned_blocks = response.receipts.len();
    if returned_blocks == 0 {
        return Err(Receipts70MergeError::EmptyResponse);
    }

    if next_block_index + returned_blocks > expected_blocks {
        return Err(Receipts70MergeError::ResponseOverflow);
    }

    let last_block_incomplete = response.last_block_incomplete;
    let receipts = response.into_with_bloom().0;
    for (offset, block_receipts) in receipts.into_iter().enumerate() {
        let target_index = next_block_index + offset;
        if target_index < merged.len() {
            if offset != 0 || first_block_receipt_index == 0 {
                return Err(Receipts70MergeError::UnexpectedAppend);
            }
            if block_receipts.is_empty() {
                return Err(Receipts70MergeError::NoProgress);
            }
            merged[target_index].extend(block_receipts);
        } else if target_index == merged.len() {
            merged.push(block_receipts);
        } else {
            return Err(Receipts70MergeError::ResponseOverflow);
        }
    }

    let (updated_block_index, updated_receipt_index) = if last_block_incomplete {
        let partial_block_index = next_block_index + returned_blocks - 1;
        let received_receipts = merged
            .get(partial_block_index)
            .map(Vec::len)
            .unwrap_or_default();
        if received_receipts == 0 {
            return Err(Receipts70MergeError::NoProgress);
        }
        (
            next_block_index + returned_blocks - 1,
            received_receipts as u64,
        )
    } else {
        (next_block_index + returned_blocks, 0)
    };

    if (updated_block_index, updated_receipt_index) == previous_state {
        return Err(Receipts70MergeError::NoProgress);
    }

    Ok((updated_block_index, updated_receipt_index))
}

trait IntoResponseValue<T> {
    fn into_value(self) -> T;
}

impl<T> IntoResponseValue<Vec<T>> for BlockHeaders<T> {
    fn into_value(self) -> Vec<T> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<T>> for BlockBodies<T> {
    fn into_value(self) -> Vec<T> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>>> for Receipts<T> {
    fn into_value(self) -> Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<T>>> for Receipts69<T> {
    fn into_value(self) -> Vec<Vec<T>> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<T>>> for Receipts70<T> {
    fn into_value(self) -> Vec<Vec<T>> {
        self.receipts
    }
}

impl<T> IntoResponseValue<Receipts70<T>> for Receipts70<T> {
    fn into_value(self) -> Receipts70<T> {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{ReceiptWithBloom, TxType};
    use alloy_primitives::Log;
    use reth_ethereum_primitives::Receipt;

    #[test]
    fn mainnet_bootnodes_are_recognized() {
        let bootnode = mainnet_nodes()
            .into_iter()
            .next()
            .expect("mainnet bootnodes should not be empty");
        assert!(is_bootstrap_node(bootnode.id));
    }

    #[test]
    fn non_bootstrap_peer_is_not_treated_as_bootstrap() {
        let non_bootstrap = NodeRecord::new(
            "203.0.113.10:30303".parse().expect("valid address"),
            PeerId::repeat_byte(0x42),
        );
        assert!(!is_bootstrap_node(non_bootstrap.id));
    }

    #[test]
    fn response_length_must_match_request() {
        assert!(response_len_matches_request(8, 8));
        assert!(!response_len_matches_request(8, 0));
        assert!(!response_len_matches_request(8, 7));
    }

    #[test]
    fn upsert_known_peer_updates_existing_record() {
        let peer_id = PeerId::repeat_byte(0x42);
        let mut known_peers = vec![NodeRecord::new_with_ports(
            "127.0.0.1".parse().unwrap(),
            30303,
            Some(30303),
            peer_id,
        )];

        let changed = upsert_known_peer(
            &mut known_peers,
            NodeRecord::new_with_ports("127.0.0.1".parse().unwrap(), 30304, Some(30305), peer_id),
        );

        assert!(changed);
        assert_eq!(known_peers.len(), 1);
        assert_eq!(known_peers[0].tcp_port, 30304);
        assert_eq!(known_peers[0].udp_port, 30305);
    }

    fn fake_receipt(gas: u64) -> Receipt {
        Receipt {
            tx_type: TxType::Legacy,
            success: true,
            cumulative_gas_used: gas,
            logs: Vec::<Log>::new(),
        }
    }

    #[test]
    fn eth70_partial_receipts_are_merged_across_requests() {
        let mut merged = Vec::<Vec<ReceiptWithBloom<Receipt>>>::new();

        let (next_block_index, first_block_receipt_index) = merge_receipts70_response(
            &mut merged,
            0,
            0,
            Receipts70 {
                last_block_incomplete: true,
                receipts: vec![
                    vec![fake_receipt(1)],
                    vec![fake_receipt(2)],
                    vec![fake_receipt(3)],
                ],
            },
            3,
        )
        .expect("first partial response should merge");

        assert_eq!(next_block_index, 2);
        assert_eq!(first_block_receipt_index, 1);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[2].len(), 1);

        let (next_block_index, first_block_receipt_index) = merge_receipts70_response(
            &mut merged,
            next_block_index,
            first_block_receipt_index,
            Receipts70 {
                last_block_incomplete: false,
                receipts: vec![vec![fake_receipt(4)]],
            },
            3,
        )
        .expect("continuation response should merge");

        assert_eq!(next_block_index, 3);
        assert_eq!(first_block_receipt_index, 0);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[2].len(), 2);
    }

    #[test]
    fn eth70_empty_response_is_rejected() {
        let mut merged = Vec::<Vec<ReceiptWithBloom<Receipt>>>::new();

        let error = merge_receipts70_response(
            &mut merged,
            0,
            0,
            Receipts70 {
                last_block_incomplete: false,
                receipts: Vec::<Vec<Receipt>>::new(),
            },
            1,
        )
        .expect_err("empty response should be rejected");

        assert_eq!(error, Receipts70MergeError::EmptyResponse);
    }
}
