use std::collections::{HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use alloy_primitives::B256;
use eyre::Result;
use logex_types::LOGEX_CLIENT_VERSION;
use reth_chainspec::{EthChainSpec, MAINNET};
use reth_discv4::{Discv4Config, NatResolver};
use reth_dns_discovery::DnsDiscoveryConfig;
use reth_eth_wire::{
    BlockBodies, BlockHeaders, BlockRangeUpdate, DisconnectReason, EthVersion, GetBlockBodies,
    GetBlockHeaders, GetReceipts, GetReceipts70, HelloMessage, NetworkPrimitives, Receipts,
    Receipts69, Receipts70, UnifiedStatus,
};
use reth_ethereum_forks::{ForkFilter, ForkId, Head};
use reth_network::p2p::headers::client::HeadersRequest;
use reth_network::types::peers::config::PeerBackoffDurations;
use reth_network::types::{PeerKind, ReputationChangeKind};
use reth_network::{
    DiscoveredEvent, DiscoveryEvent, NetworkConfigBuilder, NetworkEvent,
    NetworkEventListenerProvider, NetworkHandle, NetworkManager, NetworkSyncUpdater, PeerRequest,
    PeerRequestSender, Peers, PeersConfig, PeersInfo, SessionsConfig,
};
use reth_network_peers::{NodeRecord, PeerId, mainnet_nodes};
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
    advertised_status_range, disconnect_note, is_bootstrap_node, is_saturated_remote_rejection,
    is_stale_nonserving_peer, normalize_network_head, seed_productive_peers,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_WAIT: Duration = Duration::from_secs(2);
const FILL_BUDGET: Duration = Duration::from_secs(20);
const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);
const DNS_DISCOVERY_REQUESTS_PER_SEC: usize = 16;
const REFILL_SLOTS_INTERVAL: Duration = Duration::from_millis(800);
const NETWORK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const NETWORK_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_millis(750);
const REQUEST_HANDLER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const EARLY_SESSION_DROP_THRESHOLD: Duration = Duration::from_secs(30);
const SATURATED_PEER_RETRY_DELAY: Duration = Duration::from_secs(60);
const USELESS_PEER_GRACE_PERIOD: Duration = Duration::from_secs(5);
const SUBMITTED_DIAL_SUPPRESSION_INTERVAL: Duration = Duration::from_secs(60);
const MAX_PERSISTED_PEERS: usize = 512;
const MAX_TRACKED_PENDING: usize = 4096;
const MAX_CONSECUTIVE_TIMEOUTS: u32 = 2;
const MAX_CONCURRENT_OUTBOUND_DIALS: usize = 256;
const MAX_PENDING_DIALS_PER_REFILL: usize = 160;
const REQUEST_PEER_REFILL_ATTEMPTS: usize = 20;
const DIAL_BACKOFF_DURATIONS: PeerBackoffDurations = PeerBackoffDurations {
    low: Duration::from_secs(60),
    medium: Duration::from_secs(60 * 3),
    high: Duration::from_secs(60 * 15),
    max: Duration::from_secs(60 * 60),
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
    network_task: Option<JoinHandle<()>>,
    eth_request_task: Option<JoinHandle<()>>,
    network_events: NetworkEvents,
    discovery_events: DiscoveryEvents,
    peers: HashMap<PeerId, ActivePeer>,
    peer_order: VecDeque<PeerId>,
    request_cursor: usize,
    pending: HashMap<PeerId, NodeRecord>,
    pending_dials: HashMap<PeerId, Instant>,
    saturated_peers: HashMap<PeerId, Instant>,
    productive: VecDeque<NodeRecord>,
    known_peers: Vec<NodeRecord>,
    known_peers_path: PathBuf,
    persisted_known_peers: Vec<NodeRecord>,
    serve_cache: Arc<ServeCacheProvider>,
    fork_filter: ForkFilter,
    local_head: Head,
    network_activated: bool,
    max_peers: usize,
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
        let dns_discovery = mainnet_dns_discovery_config();
        let productive = seed_productive_peers(&known_peers);
        let basic_nodes: HashSet<NodeRecord> = known_peers.iter().copied().collect();
        let serve_cache = Arc::new(ServeCacheProvider::new());
        let peer_config = PeersConfig::default()
            .with_basic_nodes(basic_nodes)
            .with_max_outbound(max_peers)
            .with_max_inbound(max_peers.max(16))
            .with_max_concurrent_dials(
                max_peers
                    .saturating_mul(4)
                    .clamp(32, MAX_CONCURRENT_OUTBOUND_DIALS),
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
        let network_activated = network_head.number > 0;
        let fork_filter = MAINNET.fork_filter(network_head);

        let mut builder = NetworkConfigBuilder::<LogexNetworkPrimitives>::new(secret_key)
            .set_head(network_head)
            .listener_addr(listener_addr)
            .discovery_addr(discovery_addr)
            .external_ip_resolver(NatResolver::Any)
            .sessions_config(sessions_config)
            .peer_config(peer_config)
            .mainnet_boot_nodes()
            .disable_tx_gossip(true)
            .discovery(discovery);
        if let Some((_, dns_discovery_config)) = dns_discovery {
            builder = builder.dns_discovery(dns_discovery_config);
        }
        let peer_id = builder.get_peer_id();
        let hello = HelloMessage::builder(peer_id)
            .client_version(LOGEX_CLIENT_VERSION)
            .build();

        let mut config = builder.hello_message(hello).build(Arc::clone(&serve_cache));
        if let Some((earliest, latest, latest_hash)) =
            advertised_status_range(serve_cache.advertised_history_range(), network_head)
        {
            config.status.set_history_range(earliest, latest);
            config.status.blockhash = latest_hash;
        }

        let builder = NetworkManager::builder(config)
            .await
            .map_err(|error| eyre::eyre!("failed to start p2p network: {error}"))?;
        let (handle, network, _, request_handler) = builder
            .request_handler(Arc::clone(&serve_cache))
            .split_with_handle();
        if !network_activated {
            handle.set_network_hibernate();
        }
        let local_record = handle.local_node_record();
        let local_enr = handle.local_enr();
        let network_events = Box::pin(handle.event_listener());
        let discovery_events = Box::pin(handle.discovery_listener());
        let network_task = tokio::spawn(network);
        if !network_activated {
            handle.set_network_hibernate();
        }
        let eth_request_task = tokio::spawn(request_handler);
        let mut manager = Self {
            network: handle,
            network_task: Some(network_task),
            eth_request_task: Some(eth_request_task),
            network_events,
            discovery_events,
            peers: HashMap::new(),
            peer_order: VecDeque::new(),
            request_cursor: 0,
            pending: HashMap::new(),
            pending_dials: HashMap::new(),
            saturated_peers: HashMap::new(),
            productive,
            known_peers,
            known_peers_path,
            persisted_known_peers: Vec::new(),
            serve_cache,
            fork_filter,
            local_head: network_head,
            network_activated,
            max_peers,
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
        let head = normalize_network_head(head);
        self.local_head = head;
        self.fork_filter.set_head(head);
        self.network.update_status(head);
        if !self.network_activated && head.number > 0 {
            self.network_activated = true;
            self.network.set_network_active();
            self.seed_known_peers();
            info!(
                block_number = head.number,
                block_hash = %head.hash,
                "activated execution peer dialing after consensus head"
            );
        }
        self.sync_advertised_history_range();
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
        self.sync_advertised_history_range();
    }

    pub fn remove_cached_blocks(&self, reverted_hashes: &[B256]) {
        self.serve_cache.remove_blocks(reverted_hashes);
        self.sync_advertised_history_range();
    }

    fn sync_advertised_history_range(&self) {
        if let Some((earliest, latest, latest_hash)) =
            advertised_status_range(self.serve_cache.advertised_history_range(), self.local_head)
        {
            self.network.update_block_range(BlockRangeUpdate {
                earliest,
                latest,
                latest_hash,
            });
        }
    }

    fn is_compatible_fork_id(&self, fork_id: ForkId) -> bool {
        self.fork_filter.validate(fork_id).is_ok()
    }

    fn recently_submitted(&self, peer_id: PeerId, now: Instant) -> bool {
        self.pending_dials
            .get(&peer_id)
            .is_some_and(|last_submitted| {
                now.duration_since(*last_submitted) < SUBMITTED_DIAL_SUPPRESSION_INTERVAL
            })
    }

    fn prune_submitted_dials(&mut self, now: Instant) {
        self.pending_dials.retain(|_, last_submitted| {
            now.duration_since(*last_submitted) < SUBMITTED_DIAL_SUPPRESSION_INTERVAL
        });
    }
}

fn mainnet_dns_discovery_config() -> Option<(String, DnsDiscoveryConfig)> {
    let Some(dns_network) = MAINNET.chain().public_dns_network_protocol() else {
        tracing::warn!("mainnet DNS bootstrap link unavailable");
        return None;
    };

    let dns_link = match dns_network.parse() {
        Ok(link) => link,
        Err(error) => {
            tracing::warn!(%dns_network, %error, "failed to parse mainnet DNS bootstrap link");
            return None;
        }
    };

    Some((
        dns_network.to_owned(),
        DnsDiscoveryConfig {
            bootstrap_dns_networks: Some(HashSet::from([dns_link])),
            lookup_timeout: Duration::from_secs(3),
            max_requests_per_sec: NonZeroUsize::new(DNS_DISCOVERY_REQUESTS_PER_SEC)
                .expect("DNS request limit is non-zero"),
            ..DnsDiscoveryConfig::default()
        },
    ))
}
