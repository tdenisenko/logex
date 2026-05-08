use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
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
    advertised_status_range, disconnect_note, inherited_peer_request_limit, is_bootstrap_node,
    is_saturated_remote_rejection, is_stale_nonserving_peer, normalize_network_head,
    peer_receipts_are_quarantined, rotate_request_candidates, seed_productive_peers,
    should_retry_disconnected_peer,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_WAIT: Duration = Duration::from_secs(2);
const FILL_BUDGET: Duration = Duration::from_secs(2);
const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);
const DNS_DISCOVERY_REQUESTS_PER_SEC: usize = 16;
const REFILL_SLOTS_INTERVAL: Duration = Duration::from_millis(500);
const NETWORK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const NETWORK_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_millis(750);
const REQUEST_HANDLER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
const EARLY_SESSION_DROP_THRESHOLD: Duration = Duration::from_secs(30);
const SATURATED_PEER_RETRY_DELAY: Duration = Duration::from_secs(60);
const USELESS_PEER_GRACE_PERIOD: Duration = Duration::from_secs(5);
const SUBMITTED_DIAL_SUPPRESSION_INTERVAL: Duration = Duration::from_secs(15);
const MAX_PERSISTED_PEERS: usize = 512;
const MAX_TRACKED_PENDING: usize = 4096;
const MAX_CONSECUTIVE_TIMEOUTS: u32 = 8;
const OUTBOUND_DIAL_RATIO: usize = 3;
const MAX_CONCURRENT_OUTBOUND_DIALS: usize = 96;
const MAX_PENDING_DIALS_PER_REFILL: usize = 48;
const REQUEST_PEER_REFILL_ATTEMPTS: usize = 20;
pub(super) const REQUEST_LIMIT_MIN: usize = 1;
pub(super) const REQUEST_LIMIT_MAX: usize = 128;
pub(super) const BODY_REQUEST_LIMIT_INITIAL: usize = 16;
pub(super) const RECEIPT_REQUEST_LIMIT_INITIAL: usize = 16;
const REQUEST_LIMIT_LOWER_LATENCY: Duration = Duration::from_secs(2);
const REQUEST_LIMIT_UPPER_LATENCY: Duration = Duration::from_secs(3);
const REQUEST_KIND_PAUSE_DURATION: Duration = Duration::from_secs(20);
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
pub type SourcedReceiptSet = (
    PeerId,
    Vec<alloy_consensus::ReceiptWithBloom<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>>,
);
pub type SourcedBodyReceipts = (SourcedBlockBody, SourcedReceiptSet);

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
    receipt_quarantined_peers: HashMap<PeerId, Instant>,
    productive: VecDeque<NodeRecord>,
    known_peers: Vec<NodeRecord>,
    known_peers_path: PathBuf,
    persisted_known_peers: Vec<NodeRecord>,
    serve_cache: Arc<ServeCacheProvider>,
    fork_filter: ForkFilter,
    local_head: Head,
    network_activated: bool,
    max_peers: usize,
    session_metrics: ExecutionPeerSessionMetrics,
}

#[derive(Default)]
struct ExecutionPeerSessionMetrics {
    accepted_sessions: u64,
    rejected_zero_tip_sessions: u64,
    disconnected_sessions: u64,
    saturated_disconnects: u64,
    nonserving_disconnects: u64,
    missing_fork_id_candidates: u64,
    fork_id_rejected_candidates: u64,
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
    header_blocks_per_sec: f64,
    body_blocks_per_sec: f64,
    receipt_blocks_per_sec: f64,
    body_request_limit: usize,
    receipt_request_limit: usize,
    body_paused_until: Option<Instant>,
    receipt_paused_until: Option<Instant>,
    receipt_quarantined_until: Option<Instant>,
    connected_at: Instant,
}

pub struct PeerManagerConfig {
    pub secret_key: SecretKey,
    pub listener_port: u16,
    pub discovery_port: u16,
    pub max_peers: usize,
    pub nat_resolver: NatResolver,
    pub our_head: Head,
    pub known_peers: Vec<NodeRecord>,
    pub known_peers_path: PathBuf,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum PeerRequestKind {
    Headers,
    Bodies,
    Receipts,
}

impl PeerManager {
    /// Create a new peer manager backed by Reth's network/session stack.
    pub async fn new(config: PeerManagerConfig) -> Result<Self> {
        let PeerManagerConfig {
            secret_key,
            listener_port,
            discovery_port,
            max_peers,
            nat_resolver,
            our_head,
            known_peers,
            known_peers_path,
        } = config;
        let dns_discovery = mainnet_dns_discovery_config();
        let advertised_nat_resolver = resolve_startup_nat(nat_resolver.clone()).await;
        let productive = seed_productive_peers(&known_peers);
        let serve_cache = Arc::new(ServeCacheProvider::new());
        let (max_outbound, max_inbound) = peer_connection_limits(max_peers);
        let peer_config = PeersConfig::default()
            .with_max_outbound(max_outbound)
            .with_max_inbound(max_inbound)
            .with_max_concurrent_dials(
                max_outbound
                    .saturating_mul(4)
                    .clamp(32, MAX_CONCURRENT_OUTBOUND_DIALS),
            )
            .with_refill_slots_interval(REFILL_SLOTS_INTERVAL)
            .with_backoff_durations(DIAL_BACKOFF_DURATIONS)
            .with_enforce_enr_fork_id(false);
        let mut sessions_config =
            SessionsConfig::default().with_upscaled_event_buffer(peer_config.max_peers());
        sessions_config.limits.max_pending_outbound = Some(MAX_CONCURRENT_OUTBOUND_DIALS as u32);
        sessions_config.limits.max_pending_inbound = Some(max_inbound as u32);

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
            .sessions_config(sessions_config)
            .peer_config(peer_config)
            .mainnet_boot_nodes()
            .disable_tx_gossip(true)
            .discovery(discovery)
            .external_ip_resolver(advertised_nat_resolver.clone());
        if let Some((_, dns_discovery_config)) = dns_discovery {
            builder = builder.dns_discovery(dns_discovery_config);
        }
        let peer_id = builder.get_peer_id();
        let hello = HelloMessage::builder(peer_id)
            .client_version(LOGEX_CLIENT_VERSION)
            .protocol(EthVersion::Eth70)
            .protocol(EthVersion::Eth69)
            .port(listener_port)
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
            receipt_quarantined_peers: HashMap::new(),
            productive,
            known_peers,
            known_peers_path,
            persisted_known_peers: Vec::new(),
            serve_cache,
            fork_filter,
            local_head: network_head,
            network_activated,
            max_peers,
            session_metrics: ExecutionPeerSessionMetrics::default(),
        };

        manager.seed_known_peers();
        manager.persisted_known_peers = manager.known_peers();

        info!(
            peer_id = %manager.network.peer_id(),
            enode = %local_record,
            enr = %local_enr,
            listener = %local_record.tcp_addr(),
            discovery = %discovery_addr,
            nat = %advertised_nat_resolver,
            "p2p networking started"
        );
        if local_record.tcp_addr().ip().is_unspecified() {
            tracing::warn!(
                nat = %advertised_nat_resolver,
                "EL p2p is advertising an unspecified external address; inbound peer retention will be weaker unless the node is run with --nat extip:<public-ip>, --nat extaddr:<domain>, or a working public IP resolver"
            );
        }

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

    pub fn cache_canonical_headers(
        &self,
        headers: impl IntoIterator<Item = <LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    ) {
        self.serve_cache.insert_headers(headers);
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

async fn resolve_startup_nat(nat_resolver: NatResolver) -> NatResolver {
    if matches!(
        nat_resolver,
        NatResolver::ExternalIp(_) | NatResolver::ExternalAddr(_) | NatResolver::None
    ) {
        return nat_resolver;
    }

    match nat_resolver.clone().external_addr().await {
        Some(ip) => {
            info!(
                nat = %nat_resolver,
                external_ip = %ip,
                "resolved EL external IP before starting discovery"
            );
            NatResolver::ExternalIp(ip)
        }
        None => nat_resolver,
    }
}

fn peer_connection_limits(max_peers: usize) -> (usize, usize) {
    if max_peers == 0 {
        return (0, 0);
    }

    let max_outbound = (max_peers / OUTBOUND_DIAL_RATIO).clamp(1, max_peers);
    let max_inbound = max_peers.saturating_sub(max_outbound);
    (max_outbound, max_inbound)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peer_connection_limits_treat_config_as_total_capacity() {
        assert_eq!(peer_connection_limits(0), (0, 0));
        assert_eq!(peer_connection_limits(1), (1, 0));
        assert_eq!(peer_connection_limits(3), (1, 2));
        assert_eq!(peer_connection_limits(100), (33, 67));
    }
}
