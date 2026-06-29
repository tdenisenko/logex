use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
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
use reth_discv5::discv5::ListenConfig;
use reth_dns_discovery::{
    DnsDiscoveryConfig, DnsDiscoveryService, DnsNodeRecordUpdate, DnsResolver,
};
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
use tokio::time::{self, Instant as TokioInstant};
use tokio_stream::{Stream, StreamExt};
use tracing::{info, trace};

use crate::p2p::serve_cache::ServeCacheProvider;
use crate::primitives::LogexNetworkPrimitives;

mod lifecycle;
mod requests;
mod state;

use self::requests::{
    BodyReceiptActiveRequest, BodyReceiptActiveRequestDelta, BodyReceiptRequestReservations,
    RequestAttempt,
};
pub(crate) use self::requests::{
    BodyReceiptRequestAccounting, BodyReceiptRequestOutcome, BodyReceiptRequestPlan,
    BodyReceiptRequestReservations as BodyReceiptPeerReservations,
    ReverseHeaderPagesRequestOutcome, ReverseHeaderPagesRequestPlan,
};
use self::state::{
    advertised_status_range, disconnect_note, inherited_peer_request_limit, is_bootstrap_node,
    is_saturated_remote_rejection, is_stale_nonserving_peer, normalize_network_head,
    peer_receipts_are_quarantined, rotate_request_candidates, seed_productive_peers,
    should_retry_disconnected_peer, upsert_known_peer,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_WAIT: Duration = Duration::from_secs(2);
const FILL_BUDGET: Duration = Duration::from_secs(2);
const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);
const DNS_DISCOVERY_IPV4_REQUESTS_PER_SEC: usize = 16;
const DNS_DISCOVERY_IPV6_REQUESTS_PER_SEC: usize = 64;
const DNS_DISCOVERY_IPV6_BOOTNODE_TARGET: usize = 64;
const DNS_DISCOVERY_IPV6_BOOTNODE_WAIT: Duration = Duration::from_secs(10);
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
pub(super) const BODY_REQUEST_LIMIT_INITIAL: usize = 48;
pub(super) const RECEIPT_REQUEST_LIMIT_INITIAL: usize = 48;
const UNPROVEN_BODY_REQUEST_LIMIT: usize = 16;
const UNPROVEN_RECEIPT_REQUEST_LIMIT: usize = 16;
const REQUEST_LIMIT_LOWER_LATENCY: Duration = Duration::from_secs(2);
const REQUEST_LIMIT_UPPER_LATENCY: Duration = Duration::from_secs(3);
const REQUEST_KIND_PAUSE_DURATION: Duration = Duration::from_secs(8);
const REQUEST_TIMEOUT_PAUSE_MAX_DURATION: Duration = Duration::from_secs(60);
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
type DnsDiscoveryUpdates = Pin<Box<dyn Stream<Item = DnsNodeRecordUpdate> + Send>>;
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
    dns_discovery_task: Option<JoinHandle<()>>,
    network_events: NetworkEvents,
    discovery_events: DiscoveryEvents,
    dns_discovery_updates: Option<DnsDiscoveryUpdates>,
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
    bind_ip: IpAddr,
    network_activated: bool,
    max_peers: usize,
    session_metrics: ExecutionPeerSessionMetrics,
    body_receipt_scheduler_metrics: BodyReceiptSchedulerMetrics,
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
    discovered_candidates: u64,
    dns_discovered_candidates: u64,
    dns_family_rejected_candidates: u64,
    submitted_dials_total: u64,
    submitted_dial_expirations: u64,
}

#[derive(Default)]
struct BodyReceiptSchedulerMetrics {
    stale_role_retries: u64,
    prefix_reassignments: u64,
    body_successes: u64,
    receipt_successes: u64,
    body_failures: u64,
    receipt_failures: u64,
    body_blocks: u64,
    receipt_blocks: u64,
}

#[derive(Clone)]
struct ActivePeer {
    sender: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    remote_record: NodeRecord,
    remote_record_is_dialable: bool,
    remote_status: UnifiedStatus,
    client_version: Arc<str>,
    version: EthVersion,
    is_serving: bool,
    consecutive_timeouts: u32,
    header_blocks_per_sec: f64,
    body_blocks_per_sec: f64,
    receipt_blocks_per_sec: f64,
    body_active_requests: usize,
    receipt_active_requests: usize,
    body_reserved_requests: usize,
    receipt_reserved_requests: usize,
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
    pub bind_ip: IpAddr,
    pub max_peers: usize,
    pub nat_resolver: NatResolver,
    pub our_head: Head,
    pub known_peers: Vec<NodeRecord>,
    pub known_peers_path: PathBuf,
    pub execution_bootnodes: Vec<String>,
    pub execution_discv5_port: u16,
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
            bind_ip,
            max_peers,
            nat_resolver,
            our_head,
            known_peers,
            known_peers_path,
            execution_bootnodes,
            execution_discv5_port,
        } = config;
        let dns_discovery = mainnet_dns_discovery_config(bind_ip);
        let advertised_nat_resolver = resolve_startup_nat(nat_resolver.clone()).await;
        let mut known_peers = known_peers
            .into_iter()
            .filter(|node| node_matches_bind_ip(bind_ip, node))
            .collect::<Vec<_>>();
        let execution_bootnodes = parse_execution_bootnodes(&execution_bootnodes)?;
        let mut filtered_execution_bootnodes = Vec::new();
        let mut skipped_execution_bootnodes = 0usize;
        for node in execution_bootnodes {
            if node_matches_bind_ip(bind_ip, &node) {
                upsert_known_peer(&mut known_peers, node);
                filtered_execution_bootnodes.push(node);
            } else {
                skipped_execution_bootnodes += 1;
            }
        }
        if !filtered_execution_bootnodes.is_empty() || skipped_execution_bootnodes > 0 {
            tracing::info!(
                accepted = filtered_execution_bootnodes.len(),
                skipped = skipped_execution_bootnodes,
                bind_ip = %bind_ip,
                "loaded configured execution bootnodes"
            );
        }
        let productive = seed_productive_peers(&known_peers);
        let serve_cache = Arc::new(ServeCacheProvider::new());
        let (max_outbound, max_inbound) = peer_connection_limits(max_peers);
        let max_concurrent_dials = max_outbound
            .saturating_mul(4)
            .clamp(32, MAX_CONCURRENT_OUTBOUND_DIALS);
        let peer_config = PeersConfig::default()
            .with_max_outbound(max_outbound)
            .with_max_inbound(max_inbound)
            .with_max_concurrent_dials(max_concurrent_dials)
            .with_refill_slots_interval(REFILL_SLOTS_INTERVAL)
            .with_backoff_durations(DIAL_BACKOFF_DURATIONS)
            .with_enforce_enr_fork_id(false);
        let mut sessions_config =
            SessionsConfig::default().with_upscaled_event_buffer(peer_config.max_peers());
        sessions_config.limits.max_pending_outbound = Some(max_concurrent_dials as u32);
        sessions_config.limits.max_pending_inbound = Some(max_inbound as u32);

        let mut discovery = Discv4Config::builder();
        discovery
            .lookup_interval(DISCOVERY_LOOKUP_INTERVAL)
            .ping_interval(DISCOVERY_PING_INTERVAL);

        let listener_addr = SocketAddr::new(bind_ip, listener_port);
        let discovery_addr = SocketAddr::new(bind_ip, discovery_port);
        let network_head = normalize_network_head(our_head);
        let network_activated = network_head.number > 0;
        let fork_filter = MAINNET.fork_filter(network_head);

        let mut dns_discovery_task = None;
        let mut dns_discovery_updates = None;
        let mut dns_ipv6_boot_nodes = Vec::new();
        if bind_ip.is_ipv6()
            && let Some((dns_network, dns_discovery_config)) = dns_discovery.as_ref()
        {
            match DnsResolver::from_system_conf() {
                Ok(resolver) => {
                    let (mut service, _) = DnsDiscoveryService::new_pair(
                        Arc::new(resolver),
                        dns_discovery_config.clone(),
                    );
                    let mut updates = Box::pin(service.node_record_stream()) as DnsDiscoveryUpdates;
                    let task = service.spawn();
                    dns_ipv6_boot_nodes =
                        collect_initial_dns_boot_nodes(bind_ip, &fork_filter, &mut updates).await;
                    dns_discovery_updates = Some(updates);
                    dns_discovery_task = Some(task);
                    tracing::info!(
                        dns_network,
                        bind_ip = %bind_ip,
                        bootnodes = dns_ipv6_boot_nodes.len(),
                        "started family-aware execution DNS discovery for IPv6 p2p bind"
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        bind_ip = %bind_ip,
                        "failed to start family-aware execution DNS discovery"
                    );
                }
            }
        }
        if !dns_ipv6_boot_nodes.is_empty() {
            discovery.add_boot_nodes(dns_ipv6_boot_nodes.iter().copied());
        }
        if !filtered_execution_bootnodes.is_empty() {
            discovery.add_boot_nodes(filtered_execution_bootnodes.iter().copied());
        }

        let mut builder = NetworkConfigBuilder::<LogexNetworkPrimitives>::new(secret_key)
            .set_head(network_head)
            .listener_addr(listener_addr)
            .discovery_addr(discovery_addr)
            .sessions_config(sessions_config)
            .peer_config(peer_config)
            .disable_tx_gossip(true)
            .discovery(discovery)
            .external_ip_resolver(advertised_nat_resolver.clone());
        if bind_ip.is_ipv4() {
            builder = builder.mainnet_boot_nodes();
        } else {
            if let IpAddr::V6(ip) = bind_ip {
                let discv5_listen = ListenConfig::Ipv6 {
                    ip,
                    port: execution_discv5_port,
                };
                let discv5_boot_nodes = dns_ipv6_boot_nodes
                    .iter()
                    .chain(filtered_execution_bootnodes.iter())
                    .copied()
                    .collect::<Vec<_>>();
                builder = builder.discovery_v5(
                    reth_discv5::Config::builder(listener_addr)
                        .discv5_config(
                            reth_discv5::discv5::ConfigBuilder::new(discv5_listen).build(),
                        )
                        .add_unsigned_boot_nodes(discv5_boot_nodes.iter().copied()),
                );
                tracing::info!(
                    bind_ip = %bind_ip,
                    udp_port = execution_discv5_port,
                    bootnodes = discv5_boot_nodes.len(),
                    "enabled execution discv5 discovery for IPv6 p2p bind"
                );
            }
            builder = builder.disable_dns_discovery();
            tracing::info!(
                bind_ip = %bind_ip,
                bootnodes = dns_ipv6_boot_nodes.len(),
                "enabled Reth discv4 with family-aware IPv6 DNS bootnodes and disabled Reth DNS conversion"
            );
        }
        if bind_ip.is_ipv4()
            && let Some((_, dns_discovery_config)) = dns_discovery
        {
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
            dns_discovery_task,
            network_events,
            discovery_events,
            dns_discovery_updates,
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
            bind_ip,
            network_activated,
            max_peers,
            session_metrics: ExecutionPeerSessionMetrics::default(),
            body_receipt_scheduler_metrics: BodyReceiptSchedulerMetrics::default(),
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
        let before = self.pending_dials.len();
        self.pending_dials.retain(|_, last_submitted| {
            now.duration_since(*last_submitted) < SUBMITTED_DIAL_SUPPRESSION_INTERVAL
        });
        let expired = before.saturating_sub(self.pending_dials.len());
        if expired > 0 {
            self.session_metrics.submitted_dial_expirations = self
                .session_metrics
                .submitted_dial_expirations
                .saturating_add(expired as u64);
            trace!(
                expired_dials = expired,
                pending_dials = self.pending_dials.len(),
                "expired submitted execution peer dials"
            );
        }
    }
}

fn mainnet_dns_discovery_config(bind_ip: IpAddr) -> Option<(String, DnsDiscoveryConfig)> {
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

    let max_requests_per_sec = if bind_ip.is_ipv6() {
        DNS_DISCOVERY_IPV6_REQUESTS_PER_SEC
    } else {
        DNS_DISCOVERY_IPV4_REQUESTS_PER_SEC
    };

    Some((
        dns_network.to_owned(),
        DnsDiscoveryConfig {
            bootstrap_dns_networks: Some(HashSet::from([dns_link])),
            lookup_timeout: Duration::from_secs(3),
            max_requests_per_sec: NonZeroUsize::new(max_requests_per_sec)
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

fn parse_execution_bootnodes(raw_bootnodes: &[String]) -> Result<Vec<NodeRecord>> {
    let mut nodes = Vec::new();
    for raw in raw_bootnodes {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let node = raw
            .parse::<NodeRecord>()
            .map_err(|error| eyre::eyre!("invalid execution bootnode {raw:?}: {error}"))?;
        nodes.push(node);
    }
    Ok(nodes)
}

fn node_matches_bind_ip(bind_ip: IpAddr, node: &NodeRecord) -> bool {
    node.tcp_addr().ip().is_ipv4() == bind_ip.is_ipv4()
}

async fn collect_initial_dns_boot_nodes(
    bind_ip: IpAddr,
    fork_filter: &ForkFilter,
    updates: &mut DnsDiscoveryUpdates,
) -> Vec<NodeRecord> {
    if bind_ip.is_ipv4() {
        return Vec::new();
    }

    let deadline = TokioInstant::now() + DNS_DISCOVERY_IPV6_BOOTNODE_WAIT;
    let mut nodes = Vec::new();
    let mut seen = HashSet::new();
    loop {
        if nodes.len() >= DNS_DISCOVERY_IPV6_BOOTNODE_TARGET {
            break;
        }
        let Some(remaining) = deadline.checked_duration_since(TokioInstant::now()) else {
            break;
        };
        let Ok(Some(update)) = time::timeout(remaining, updates.next()).await else {
            break;
        };
        let Some(node) = dns_boot_node_for_bind_ip(bind_ip, fork_filter, &update) else {
            continue;
        };
        if seen.insert(node.id) {
            nodes.push(node);
        }
    }
    nodes
}

fn dns_boot_node_for_bind_ip(
    bind_ip: IpAddr,
    fork_filter: &ForkFilter,
    update: &DnsNodeRecordUpdate,
) -> Option<NodeRecord> {
    if let Some(fork_id) = update.fork_id
        && fork_filter.validate(fork_id).is_err()
    {
        return None;
    }
    let advertised_udp = if bind_ip.is_ipv6() {
        update.enr.udp6().or_else(|| update.enr.udp4())
    } else {
        update.enr.udp4()
    };
    advertised_udp?;
    let node = dns_node_record_for_bind_ip(bind_ip, update)?;
    Some(node)
}

fn dns_node_record_for_bind_ip(
    bind_ip: IpAddr,
    update: &DnsNodeRecordUpdate,
) -> Option<NodeRecord> {
    let peer_id = update.node_record.id;
    if bind_ip.is_ipv6() {
        let ip = update.enr.ip6().map(IpAddr::V6)?;
        let tcp_port = update.enr.tcp6().or_else(|| update.enr.tcp4())?;
        let udp_port = update.enr.udp6().or_else(|| update.enr.udp4());
        return Some(NodeRecord::new_with_ports(ip, tcp_port, udp_port, peer_id));
    }

    let ip = update.enr.ip4().map(IpAddr::V4)?;
    let tcp_port = update.enr.tcp4()?;
    let udp_port = update.enr.udp4();
    Some(NodeRecord::new_with_ports(ip, tcp_port, udp_port, peer_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn peer_connection_limits_treat_config_as_total_capacity() {
        assert_eq!(peer_connection_limits(0), (0, 0));
        assert_eq!(peer_connection_limits(1), (1, 0));
        assert_eq!(peer_connection_limits(3), (1, 2));
        assert_eq!(peer_connection_limits(100), (33, 67));
    }

    #[test]
    fn node_family_filter_matches_configured_bind_ip() {
        let v4_node = NodeRecord::new_with_ports(
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            30303,
            Some(30303),
            PeerId::repeat_byte(0x01),
        );
        let v6_node = NodeRecord::new_with_ports(
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            30303,
            Some(30303),
            PeerId::repeat_byte(0x02),
        );

        assert!(node_matches_bind_ip(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            &v4_node
        ));
        assert!(!node_matches_bind_ip(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            &v6_node
        ));
        assert!(node_matches_bind_ip(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            &v6_node
        ));
        assert!(!node_matches_bind_ip(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            &v4_node
        ));
    }

    #[test]
    fn dns_node_record_uses_ipv6_tcp6_for_ipv6_bind_ip() {
        let secret = SecretKey::from_byte_array(&[0x11; 32]).unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip6(Ipv6Addr::LOCALHOST)
            .tcp6(30304)
            .udp6(30305)
            .build(&secret)
            .unwrap();
        let expected_tcp = enr.tcp6().expect("fixture advertises an IPv6 TCP port");
        let expected_udp = enr.udp6();
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                30303,
                Some(30303),
                PeerId::repeat_byte(0x01),
            ),
            fork_id: None,
            enr,
        };

        let record = dns_node_record_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), &update)
            .expect("fixture contains IPv6 TCP endpoint fields");

        assert!(record.tcp_addr().ip().is_ipv6());
        assert_eq!(record.tcp_port, expected_tcp);
        assert_eq!(record.udp_port, expected_udp.unwrap_or_default());
    }

    #[test]
    fn dns_node_record_uses_shared_tcp_port_for_ipv6_when_tcp6_is_absent() {
        let enr: reth_network_peers::Enr<SecretKey> = "enr:-Ky4QFLajgAy-oJ6-qZAtBwJAAKJYSN1IvRz6idba7ab3U44YgtX3kwRE0yotbttzeRFCCSD8QuvjBJSzUiYNKbXAZgkg2V0aMfGhAfJRi6AgmlkgnY0gmlwhIjzWNuDaXA2kCoBBPgBcQDcAAAAAAAAAAKJc2VjcDI1NmsxoQLYYURYDijb3HRPx6MDWt3HGS-GtWwdhqudRJ4ye_9VF4N0Y3CCdyeDdWRwgncn"
            .parse()
            .unwrap();
        assert!(enr.ip6().is_some());
        assert!(enr.tcp6().is_none());
        assert!(enr.tcp4().is_some());
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                30303,
                Some(30303),
                PeerId::repeat_byte(0x01),
            ),
            fork_id: None,
            enr,
        };

        let record = dns_node_record_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), &update)
            .expect("IPv6 ENRs may use the shared tcp field when tcp6 is absent");

        assert!(record.tcp_addr().ip().is_ipv6());
        assert_eq!(record.tcp_port, update.enr.tcp4().unwrap());
    }

    #[test]
    fn dns_boot_node_accepts_ipv6_udp_seed() {
        let secret = SecretKey::from_byte_array(&[0x22; 32]).unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip6(Ipv6Addr::LOCALHOST)
            .tcp6(30303)
            .udp6(30303)
            .build(&secret)
            .unwrap();
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                30303,
                Some(30303),
                PeerId::repeat_byte(0x01),
            ),
            fork_id: None,
            enr,
        };
        let fork_filter = MAINNET.fork_filter(Head {
            number: 25_000_000,
            timestamp: 1_760_000_000,
            ..Default::default()
        });

        let record =
            dns_boot_node_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), &fork_filter, &update)
                .expect("IPv6 DNS bootnodes with UDP should seed discv4");

        assert!(record.tcp_addr().ip().is_ipv6());
        assert_eq!(record.udp_port, 30303);
    }

    #[test]
    fn parse_execution_bootnodes_accepts_ipv6_enode() {
        let nodes = parse_execution_bootnodes(&["enode://1dd9d65c4552b5eb43d5ad55a2ee3f56c6cbc1c64a5c8d659f51fcd51bace24351232b8d7821617d2b29b54b81cdefb9b3e9c37d7fd5f63270bcc9e1a6f6a439@[2001:db8:3c4d:15::abcd:ef12]:52150?discport=52151".to_owned()])
            .expect("valid IPv6 enode should parse");

        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].tcp_addr().ip().is_ipv6());
        assert_eq!(nodes[0].tcp_port, 52150);
        assert_eq!(nodes[0].udp_port, 52151);
    }

    #[test]
    fn parse_execution_bootnodes_rejects_invalid_enode() {
        let error = parse_execution_bootnodes(&["not-an-enode".to_owned()])
            .expect_err("invalid enode should fail");

        assert!(error.to_string().contains("invalid execution bootnode"));
    }

    #[test]
    fn dns_boot_node_rejects_ipv6_seed_without_udp() {
        let secret = SecretKey::from_byte_array(&[0x33; 32]).unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip6(Ipv6Addr::LOCALHOST)
            .tcp6(30303)
            .build(&secret)
            .unwrap();
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                30303,
                None,
                PeerId::repeat_byte(0x01),
            ),
            fork_id: None,
            enr,
        };
        let fork_filter = MAINNET.fork_filter(Head {
            number: 25_000_000,
            timestamp: 1_760_000_000,
            ..Default::default()
        });

        assert!(
            dns_boot_node_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), &fork_filter, &update)
                .is_none()
        );
    }

    #[test]
    fn dns_boot_node_rejects_incompatible_fork_id() {
        let secret = SecretKey::from_byte_array(&[0x44; 32]).unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip6(Ipv6Addr::LOCALHOST)
            .tcp6(30303)
            .udp6(30303)
            .build(&secret)
            .unwrap();
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                30303,
                Some(30303),
                PeerId::repeat_byte(0x01),
            ),
            fork_id: Some(ForkId {
                hash: reth_ethereum_forks::ForkHash([0xff; 4]),
                next: 0,
            }),
            enr,
        };
        let fork_filter = MAINNET.fork_filter(Head {
            number: 25_000_000,
            timestamp: 1_760_000_000,
            ..Default::default()
        });

        assert!(
            dns_boot_node_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), &fork_filter, &update)
                .is_none()
        );
    }

    #[test]
    fn mainnet_dns_discovery_uses_wider_crawl_for_ipv6() {
        let (_, ipv4_config) =
            mainnet_dns_discovery_config(IpAddr::V4(Ipv4Addr::UNSPECIFIED)).unwrap();
        let (_, ipv6_config) =
            mainnet_dns_discovery_config(IpAddr::V6(Ipv6Addr::UNSPECIFIED)).unwrap();

        assert_eq!(
            ipv4_config.max_requests_per_sec.get(),
            DNS_DISCOVERY_IPV4_REQUESTS_PER_SEC
        );
        assert_eq!(
            ipv6_config.max_requests_per_sec.get(),
            DNS_DISCOVERY_IPV6_REQUESTS_PER_SEC
        );
        assert!(
            ipv6_config.max_requests_per_sec > ipv4_config.max_requests_per_sec,
            "IPv6 startup needs to skip through mostly IPv4-only mainnet ENRs quickly"
        );
    }
}
