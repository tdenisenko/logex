use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use eyre::Result;
use reth_discv4::{Discv4Config, NatResolver};
use reth_eth_wire::{
    DisconnectReason, EthVersion, GetReceipts, GetReceipts70, NetworkPrimitives, Receipts,
    Receipts69, Receipts70, UnifiedStatus,
};
use reth_ethereum_forks::Head;
use reth_network::p2p::bodies::client::BodiesClient;
use reth_network::p2p::headers::client::{HeadersClient, HeadersRequest};
use reth_network::types::peers::config::PeerBackoffDurations;
use reth_network::types::{PeerKind, ReputationChangeKind};
use reth_network::{
    BlockDownloaderProvider, DiscoveredEvent, DiscoveryEvent, FetchClient, NetworkConfigBuilder,
    NetworkEvent, NetworkEventListenerProvider, NetworkHandle, NetworkManager, PeerRequest,
    PeerRequestSender, Peers, PeersConfig, PeersInfo, SessionsConfig,
};
use reth_network_peers::{NodeRecord, PeerId, TrustedPeer, mainnet_nodes};
use secp256k1::SecretKey;
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tracing::info;

use crate::p2p::serve_cache::ServeCacheProvider;
use crate::primitives::LogexNetworkPrimitives;

mod lifecycle;
mod requests;
mod state;

use self::requests::RequestAttempt;
use self::state::{
    body_range_hint, disconnect_note, is_bootstrap_node, is_stale_nonserving_peer,
    normalize_network_head, seed_productive_peers,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_WAIT: Duration = Duration::from_secs(2);
const FILL_BUDGET: Duration = Duration::from_secs(12);
const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);
const REFILL_SLOTS_INTERVAL: Duration = Duration::from_millis(800);
const NETWORK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const NETWORK_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_millis(750);
const REQUEST_HANDLER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const EARLY_SESSION_DROP_THRESHOLD: Duration = Duration::from_secs(30);
const USELESS_PEER_GRACE_PERIOD: Duration = Duration::from_secs(20);
const MAX_PERSISTED_PEERS: usize = 512;
const MAX_TRACKED_PENDING: usize = 4096;
const MAX_CONSECUTIVE_TIMEOUTS: u32 = 2;
const MAX_CONCURRENT_OUTBOUND_DIALS: usize = 100;
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
pub type SourcedBlockBody = (
    PeerId,
    <LogexNetworkPrimitives as NetworkPrimitives>::BlockBody,
);

/// Manages peer sessions and request routing on top of Reth's real network stack.
pub struct PeerManager {
    network: NetworkHandle<LogexNetworkPrimitives>,
    fetch_client: FetchClient<LogexNetworkPrimitives>,
    network_task: Option<JoinHandle<()>>,
    eth_request_task: Option<JoinHandle<()>>,
    network_events: NetworkEvents,
    discovery_events: DiscoveryEvents,
    peers: HashMap<PeerId, ActivePeer>,
    peer_order: VecDeque<PeerId>,
    request_cursor: usize,
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
    client_version: Arc<str>,
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
        let productive = seed_productive_peers(&known_peers);
        let basic_nodes: HashSet<NodeRecord> = known_peers.iter().copied().collect();
        let trusted_nodes: Vec<TrustedPeer> =
            known_peers.iter().copied().map(TrustedPeer::from).collect();
        let serve_cache = Arc::new(ServeCacheProvider::new());
        let peer_config = PeersConfig::default()
            .with_basic_nodes(basic_nodes)
            .with_trusted_nodes(trusted_nodes)
            .with_max_outbound(max_peers)
            .with_max_inbound(max_peers.max(16))
            .with_max_concurrent_dials(
                max_peers
                    .saturating_mul(2)
                    .clamp(8, MAX_CONCURRENT_OUTBOUND_DIALS),
            )
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
        let fetch_client = handle
            .fetch_client()
            .await
            .map_err(|error| eyre::eyre!("failed to create reth fetch client: {error}"))?;

        let mut manager = Self {
            network: handle,
            fetch_client,
            network_task: Some(network_task),
            eth_request_task: Some(eth_request_task),
            network_events,
            discovery_events,
            peers: HashMap::new(),
            peer_order: VecDeque::new(),
            request_cursor: 0,
            pending: HashMap::new(),
            productive,
            known_peers,
            known_peers_path,
            persisted_known_peers: Vec::new(),
            serve_cache,
        };

        manager.seed_known_peers();
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
}
