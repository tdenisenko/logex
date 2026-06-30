use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::{NonZeroU32, NonZeroUsize};
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
use reth_discv5::discv5::{Enr as Discv5Enr, ListenConfig};
use reth_discv5::enr::EnrCombinedKeyWrapper;
use reth_discv5::enr_to_discv4_id;
use reth_dns_discovery::{
    DnsDiscoveryConfig, DnsDiscoveryEvent, DnsDiscoveryService, DnsNodeRecordUpdate, DnsResolver,
};
use reth_eth_wire::{
    BlockBodies, BlockHeaders, BlockRangeUpdate, DisconnectReason, EthVersion, GetBlockBodies,
    GetBlockHeaders, GetReceipts, GetReceipts70, HelloMessage, NetworkPrimitives, Receipts,
    Receipts69, Receipts70, UnifiedStatus,
};
use reth_ethereum_forks::{EnrForkIdEntry, ForkFilter, ForkId, Head};
use reth_network::p2p::headers::client::HeadersRequest;
use reth_network::types::peers::config::PeerBackoffDurations;
use reth_network::types::{PeerKind, ReputationChangeKind};
use reth_network::{
    DiscoveredEvent, DiscoveryEvent, NetworkConfigBuilder, NetworkEvent,
    NetworkEventListenerProvider, NetworkHandle, NetworkManager, NetworkSyncUpdater, PeerRequest,
    PeerRequestSender, Peers, PeersConfig, PeersInfo, SessionsConfig,
};
use reth_network_peers::{NodeRecord, PeerId, mainnet_nodes, pk2id};
use secp256k1::SecretKey;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant as TokioInstant};
use tokio_stream::wrappers::ReceiverStream;
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
    is_restart_seed_peer, is_saturated_remote_rejection, is_stale_nonserving_peer,
    normalize_network_head, peer_receipts_are_quarantined, rotate_request_candidates,
    seed_productive_peers, should_retry_disconnected_peer, upsert_known_peer,
};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const DISCOVERY_WAIT: Duration = Duration::from_secs(2);
const FILL_BUDGET: Duration = Duration::from_secs(2);
const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);
const DNS_DISCOVERY_IPV4_REQUESTS_PER_SEC: usize = 16;
const DNS_DISCOVERY_IPV6_REQUESTS_PER_SEC: usize = 64;
const DNS_DISCOVERY_CACHE_LIMIT: u32 = 8_192;
const DNS_DISCOVERY_EVENT_BUFFER: usize = 8_192;
const DNS_DISCOVERY_IPV6_BOOTNODE_TARGET: usize = 128;
const DNS_DISCOVERY_IPV6_BOOTNODE_WAIT: Duration = Duration::from_secs(15);
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
type DnsDiscoveryEvents = Pin<Box<dyn Stream<Item = DnsDiscoveryEvent> + Send>>;
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
    dns_discovery_events: Option<DnsDiscoveryEvents>,
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
    dial_families: DialAddressFamilies,
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
    pub dial_families: DialAddressFamilies,
    pub max_peers: usize,
    pub nat_resolver: NatResolver,
    pub our_head: Head,
    pub known_peers: Vec<NodeRecord>,
    pub known_peers_path: PathBuf,
    pub execution_bootnodes: Vec<String>,
    pub execution_discv5_port: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DialAddressFamilies {
    ipv4: bool,
    ipv6: bool,
}

impl DialAddressFamilies {
    pub const IPV4: Self = Self {
        ipv4: true,
        ipv6: false,
    };
    pub const IPV6: Self = Self {
        ipv4: false,
        ipv6: true,
    };
    pub const BOTH: Self = Self {
        ipv4: true,
        ipv6: true,
    };

    pub const fn for_bind_ip(bind_ip: IpAddr) -> Self {
        if bind_ip.is_ipv4() {
            Self::IPV4
        } else {
            Self::IPV6
        }
    }

    const fn includes_ip(self, ip: IpAddr) -> bool {
        (ip.is_ipv4() && self.ipv4) || (ip.is_ipv6() && self.ipv6)
    }

    pub const fn allows_ipv4(self) -> bool {
        self.ipv4
    }

    pub const fn allows_ipv6(self) -> bool {
        self.ipv6
    }

    const fn includes_ipv6(self) -> bool {
        self.ipv6
    }
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
            dial_families,
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
            .filter(|node| node_matches_dial_families(dial_families, node))
            .collect::<Vec<_>>();
        let execution_bootnodes = parse_execution_bootnodes(&execution_bootnodes)?;
        let mut filtered_execution_bootnodes = Vec::new();
        let mut filtered_execution_bootnode_enrs = Vec::new();
        let mut skipped_execution_bootnodes = 0usize;
        let mut skipped_execution_bootnode_enrs = 0usize;
        for node in execution_bootnodes.node_records {
            if node_matches_dial_families(dial_families, &node) {
                upsert_known_peer(&mut known_peers, node);
                filtered_execution_bootnodes.push(node);
            } else {
                skipped_execution_bootnodes += 1;
            }
        }
        for enr in execution_bootnodes.signed_enrs {
            let mut accepted = false;
            if let Some(node) = signed_enr_node_record_for_dial_families(dial_families, &enr) {
                upsert_known_peer(&mut known_peers, node);
                filtered_execution_bootnodes.push(node);
                accepted = true;
            }
            if signed_enr_matches_discovery_bind_ip(bind_ip, &enr) {
                filtered_execution_bootnode_enrs.push(enr);
                accepted = true;
            }
            if !accepted {
                skipped_execution_bootnode_enrs += 1;
            }
        }
        if !filtered_execution_bootnodes.is_empty()
            || !filtered_execution_bootnode_enrs.is_empty()
            || skipped_execution_bootnodes > 0
            || skipped_execution_bootnode_enrs > 0
        {
            tracing::info!(
                accepted_direct = filtered_execution_bootnodes.len(),
                accepted_signed_discovery = filtered_execution_bootnode_enrs.len(),
                skipped_enodes = skipped_execution_bootnodes,
                skipped_enrs = skipped_execution_bootnode_enrs,
                bind_ip = %bind_ip,
                ?dial_families,
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

        let mut dns_discovery_events = None;
        let mut dns_discovery_task = None;
        let mut dns_initial_boot_nodes = DnsInitialBootNodes::default();
        if dial_families.includes_ipv6()
            && let Some((dns_network, dns_discovery_config)) = dns_discovery.as_ref()
        {
            match DnsResolver::from_system_conf() {
                Ok(resolver) => {
                    let mut service =
                        DnsDiscoveryService::new(Arc::new(resolver), dns_discovery_config.clone());
                    service.bootstrap();
                    let mut events = Box::pin(service) as DnsDiscoveryEvents;
                    dns_initial_boot_nodes = collect_initial_dns_boot_nodes(
                        bind_ip,
                        dial_families,
                        &fork_filter,
                        &mut events,
                    )
                    .await;
                    let (events, task) = spawn_dns_discovery_poller(events);
                    dns_discovery_events = Some(events);
                    dns_discovery_task = Some(task);
                    tracing::info!(
                        dns_network,
                        bind_ip = %bind_ip,
                        ?dial_families,
                        direct_bootnodes = dns_initial_boot_nodes.direct_node_records.len(),
                        discovery_bootnodes = dns_initial_boot_nodes.discovery_node_records.len(),
                        signed_bootnodes = dns_initial_boot_nodes.signed_enrs.len(),
                        "started family-aware execution DNS discovery for outbound p2p candidates"
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
        let dns_bind_compatible_boot_nodes = dns_initial_boot_nodes
            .discovery_node_records
            .iter()
            .copied()
            .filter(|node| node_matches_bind_ip(bind_ip, node))
            .collect::<Vec<_>>();
        if !dns_bind_compatible_boot_nodes.is_empty() {
            discovery.add_boot_nodes(dns_bind_compatible_boot_nodes.iter().copied());
        }
        let discovery_execution_bootnodes = filtered_execution_bootnodes
            .iter()
            .copied()
            .filter(|node| node_matches_bind_ip(bind_ip, node))
            .collect::<Vec<_>>();
        if !discovery_execution_bootnodes.is_empty() {
            discovery.add_boot_nodes(discovery_execution_bootnodes.iter().copied());
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
                let discv5_boot_nodes = dns_initial_boot_nodes
                    .discovery_node_records
                    .iter()
                    .chain(filtered_execution_bootnodes.iter())
                    .filter(|node| node_matches_bind_ip(bind_ip, node))
                    .copied()
                    .collect::<Vec<_>>();
                let signed_discv5_boot_nodes = filtered_execution_bootnode_enrs
                    .iter()
                    .chain(dns_initial_boot_nodes.signed_enrs.iter())
                    .filter(|enr| signed_enr_matches_discovery_bind_ip(bind_ip, enr))
                    .cloned()
                    .collect::<Vec<_>>();
                builder = builder.discovery_v5(
                    reth_discv5::Config::builder(listener_addr)
                        .discv5_config(
                            reth_discv5::discv5::ConfigBuilder::new(discv5_listen).build(),
                        )
                        .add_unsigned_boot_nodes(discv5_boot_nodes.iter().copied())
                        .add_signed_boot_nodes(signed_discv5_boot_nodes.iter().cloned()),
                );
                tracing::info!(
                    bind_ip = %bind_ip,
                    udp_port = execution_discv5_port,
                    unsigned_bootnodes = discv5_boot_nodes.len(),
                    signed_bootnodes = signed_discv5_boot_nodes.len(),
                    "enabled execution discv5 discovery for IPv6 p2p bind"
                );
            }
            builder = builder.disable_discv4_discovery().disable_dns_discovery();
            tracing::info!(
                bind_ip = %bind_ip,
                direct_bootnodes = dns_initial_boot_nodes.direct_node_records.len(),
                discovery_bootnodes = dns_initial_boot_nodes.discovery_node_records.len(),
                signed_bootnodes = dns_initial_boot_nodes.signed_enrs.len(),
                "enabled execution discv5 with family-aware IPv6 DNS bootnodes and disabled Reth discv4/DNS conversion"
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
            dns_discovery_events,
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
            dial_families,
            network_activated,
            max_peers,
            session_metrics: ExecutionPeerSessionMetrics::default(),
            body_receipt_scheduler_metrics: BodyReceiptSchedulerMetrics::default(),
        };

        let initial_dns_direct_bootnodes = dns_initial_boot_nodes.direct_node_records.clone();
        manager.seed_known_peers();
        let queued_initial_dns_bootnodes =
            manager.queue_initial_dns_boot_nodes(&initial_dns_direct_bootnodes);
        if queued_initial_dns_bootnodes > 0 {
            tracing::info!(
                queued_initial_dns_bootnodes,
                pending_peers = manager.pending.len(),
                "queued initial DNS execution bootnodes for direct RLPx dialing"
            );
            manager.fill_open_peer_slots();
        }
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

    fn queue_initial_dns_boot_nodes(&mut self, nodes: &[NodeRecord]) -> usize {
        let mut queued = 0usize;
        for node in nodes.iter().copied() {
            self.session_metrics.dns_discovered_candidates = self
                .session_metrics
                .dns_discovered_candidates
                .saturating_add(1);
            let before = self.pending.len();
            self.remember_pending(node);
            if self.pending.len() > before {
                queued = queued.saturating_add(1);
            }
        }
        queued
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
            dns_record_cache_limit: NonZeroU32::new(DNS_DISCOVERY_CACHE_LIMIT)
                .expect("DNS cache limit is non-zero"),
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

#[derive(Debug, Default)]
struct ParsedExecutionBootnodes {
    node_records: Vec<NodeRecord>,
    signed_enrs: Vec<Discv5Enr>,
}

#[derive(Debug, Default)]
struct DnsInitialBootNodes {
    direct_node_records: Vec<NodeRecord>,
    discovery_node_records: Vec<NodeRecord>,
    signed_enrs: Vec<Discv5Enr>,
}

fn parse_execution_bootnodes(raw_bootnodes: &[String]) -> Result<ParsedExecutionBootnodes> {
    let mut bootnodes = ParsedExecutionBootnodes::default();
    for raw in raw_bootnodes {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        if raw.starts_with("enr:") {
            let enr = raw
                .parse::<Discv5Enr>()
                .map_err(|error| eyre::eyre!("invalid execution bootnode ENR {raw:?}: {error}"))?;
            bootnodes.signed_enrs.push(enr);
        } else {
            let node = raw.parse::<NodeRecord>().map_err(|error| {
                eyre::eyre!(
                    "invalid execution bootnode {raw:?}: {error}; expected enode:// or enr:"
                )
            })?;
            bootnodes.node_records.push(node);
        }
    }
    Ok(bootnodes)
}

fn node_matches_bind_ip(bind_ip: IpAddr, node: &NodeRecord) -> bool {
    node.tcp_addr().ip().is_ipv4() == bind_ip.is_ipv4()
}

fn node_matches_dial_families(families: DialAddressFamilies, node: &NodeRecord) -> bool {
    families.includes_ip(node.tcp_addr().ip())
}

fn signed_enr_node_record_for_dial_families(
    families: DialAddressFamilies,
    enr: &Discv5Enr,
) -> Option<NodeRecord> {
    let peer_id = enr_to_discv4_id(enr)?;
    if families.ipv4
        && let (Some(ip), Some(tcp_port)) = (enr.ip4(), enr.tcp4())
    {
        return Some(NodeRecord::new_with_ports(
            IpAddr::V4(ip),
            tcp_port,
            enr.udp4(),
            peer_id,
        ));
    }
    if families.ipv6
        && let (Some(ip), Some(tcp_port)) = (enr.ip6(), signed_ipv6_tcp_port(enr))
    {
        return Some(NodeRecord::new_with_ports(
            IpAddr::V6(ip),
            tcp_port,
            signed_ipv6_udp_port(enr),
            peer_id,
        ));
    }
    None
}

fn signed_enr_matches_discovery_bind_ip(bind_ip: IpAddr, enr: &Discv5Enr) -> bool {
    if bind_ip.is_ipv6() {
        enr.ip6().is_some() && enr.udp6().is_some()
    } else {
        enr.ip4().is_some() && enr.udp4().is_some()
    }
}

async fn collect_initial_dns_boot_nodes(
    bind_ip: IpAddr,
    dial_families: DialAddressFamilies,
    fork_filter: &ForkFilter,
    events: &mut DnsDiscoveryEvents,
) -> DnsInitialBootNodes {
    if !dial_families.includes_ipv6() {
        return DnsInitialBootNodes::default();
    }

    let deadline = TokioInstant::now() + DNS_DISCOVERY_IPV6_BOOTNODE_WAIT;
    let mut bootnodes = DnsInitialBootNodes::default();
    let mut seen_direct_node_records = HashSet::new();
    let mut seen_discovery_node_records = HashSet::new();
    let mut seen_signed_enrs = HashSet::new();
    loop {
        let total_bootnodes = bootnodes
            .direct_node_records
            .len()
            .saturating_add(bootnodes.discovery_node_records.len())
            .saturating_add(bootnodes.signed_enrs.len());
        if total_bootnodes >= DNS_DISCOVERY_IPV6_BOOTNODE_TARGET {
            break;
        }
        let Some(remaining) = deadline.checked_duration_since(TokioInstant::now()) else {
            break;
        };
        let Ok(Some(event)) = time::timeout(remaining, events.next()).await else {
            break;
        };
        let Some(update) = dns_node_record_update_from_event(event) else {
            continue;
        };
        if let Some(node) = dns_initial_ipv6_direct_boot_node(fork_filter, &update)
            && seen_direct_node_records.insert(node.id)
        {
            bootnodes.direct_node_records.push(node);
        }
        if let Some(node) = dns_boot_node_for_bind_ip(bind_ip, fork_filter, &update)
            && seen_discovery_node_records.insert(node.id)
        {
            bootnodes.discovery_node_records.push(node);
        }
        if let Some(enr) = dns_signed_boot_node_for_bind_ip(bind_ip, fork_filter, &update)
            && seen_signed_enrs.insert(enr.to_string())
        {
            bootnodes.signed_enrs.push(enr);
        }
    }
    bootnodes
}

fn spawn_dns_discovery_poller(
    mut events: DnsDiscoveryEvents,
) -> (DnsDiscoveryEvents, JoinHandle<()>) {
    let (sender, receiver) = mpsc::channel(DNS_DISCOVERY_EVENT_BUFFER);
    let task = tokio::spawn(async move {
        let mut forwarded = 0u64;
        while let Some(event) = events.next().await {
            if sender.send(event).await.is_err() {
                break;
            }
            forwarded = forwarded.saturating_add(1);
        }
        trace!(
            forwarded,
            "execution DNS discovery background poller stopped"
        );
    });

    (Box::pin(ReceiverStream::new(receiver)), task)
}

fn dns_node_record_update_from_event(event: DnsDiscoveryEvent) -> Option<DnsNodeRecordUpdate> {
    let DnsDiscoveryEvent::Enr(enr) = event;
    let peer_id = pk2id(&enr.public_key());
    let address = enr
        .ip4()
        .map(IpAddr::V4)
        .or_else(|| enr.ip6().map(IpAddr::V6))?;
    let tcp_port = enr
        .tcp4()
        .or_else(|| enr.tcp6())
        .or_else(|| dns_ipv6_tcp_port(&enr))?;
    let udp_port = enr
        .udp4()
        .or_else(|| enr.udp6())
        .or_else(|| dns_ipv6_udp_port(&enr));
    let node_record = NodeRecord::new_with_ports(address, tcp_port, udp_port, peer_id);
    let fork_id = enr
        .get_decodable::<EnrForkIdEntry>(b"eth")
        .transpose()
        .ok()
        .flatten()
        .map(Into::into);

    Some(DnsNodeRecordUpdate {
        node_record,
        fork_id,
        enr,
    })
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
        dns_ipv6_udp_port(&update.enr)
    } else {
        update.enr.udp4()
    };
    advertised_udp?;
    let node = dns_node_record_for_bind_ip(bind_ip, update)?;
    Some(node)
}

fn dns_initial_ipv6_direct_boot_node(
    fork_filter: &ForkFilter,
    update: &DnsNodeRecordUpdate,
) -> Option<NodeRecord> {
    if let Some(fork_id) = update.fork_id
        && fork_filter.validate(fork_id).is_err()
    {
        return None;
    }

    dns_node_record_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), update)
}

fn dns_signed_boot_node_for_bind_ip(
    bind_ip: IpAddr,
    fork_filter: &ForkFilter,
    update: &DnsNodeRecordUpdate,
) -> Option<Discv5Enr> {
    if let Some(fork_id) = update.fork_id
        && fork_filter.validate(fork_id).is_err()
    {
        return None;
    }

    if bind_ip.is_ipv6() {
        update.enr.ip6()?;
        update.enr.udp6()?;
    } else {
        update.enr.ip4()?;
        update.enr.udp4()?;
    }

    Some(EnrCombinedKeyWrapper::from(update.enr.clone()).0)
}

fn dns_node_record_for_bind_ip(
    bind_ip: IpAddr,
    update: &DnsNodeRecordUpdate,
) -> Option<NodeRecord> {
    let peer_id = update.node_record.id;
    if bind_ip.is_ipv6() {
        let ip = update.enr.ip6().map(IpAddr::V6)?;
        let tcp_port = dns_ipv6_tcp_port(&update.enr)?;
        let udp_port = dns_ipv6_udp_port(&update.enr);
        return Some(NodeRecord::new_with_ports(ip, tcp_port, udp_port, peer_id));
    }

    let ip = update.enr.ip4().map(IpAddr::V4)?;
    let tcp_port = update.enr.tcp4()?;
    let udp_port = update.enr.udp4();
    Some(NodeRecord::new_with_ports(ip, tcp_port, udp_port, peer_id))
}

fn dns_node_record_for_dial_families(
    families: DialAddressFamilies,
    update: &DnsNodeRecordUpdate,
) -> Option<NodeRecord> {
    if families.ipv4
        && let Some(node) = dns_node_record_for_bind_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED), update)
    {
        return Some(node);
    }
    if families.ipv6
        && let Some(node) = dns_node_record_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), update)
    {
        return Some(node);
    }
    None
}

fn signed_ipv6_tcp_port(enr: &Discv5Enr) -> Option<u16> {
    enr.tcp6().or_else(|| enr.tcp4())
}

fn signed_ipv6_udp_port(enr: &Discv5Enr) -> Option<u16> {
    enr.udp6().or_else(|| enr.udp4())
}

fn dns_ipv6_tcp_port(enr: &reth_network_peers::Enr<SecretKey>) -> Option<u16> {
    enr.tcp6().or_else(|| enr.tcp4())
}

fn dns_ipv6_udp_port(enr: &reth_network_peers::Enr<SecretKey>) -> Option<u16> {
    enr.udp6().or_else(|| enr.udp4())
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
    fn node_family_filter_matches_configured_dial_families() {
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

        assert!(node_matches_dial_families(
            DialAddressFamilies::IPV4,
            &v4_node
        ));
        assert!(!node_matches_dial_families(
            DialAddressFamilies::IPV4,
            &v6_node
        ));
        assert!(node_matches_dial_families(
            DialAddressFamilies::IPV6,
            &v6_node
        ));
        assert!(!node_matches_dial_families(
            DialAddressFamilies::IPV6,
            &v4_node
        ));
        assert!(node_matches_dial_families(
            DialAddressFamilies::BOTH,
            &v4_node
        ));
        assert!(node_matches_dial_families(
            DialAddressFamilies::BOTH,
            &v6_node
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
    fn dns_node_record_uses_generic_tcp_udp_for_ipv6_when_specific_ports_are_absent() {
        let enr: reth_network_peers::Enr<SecretKey> = "enr:-Ky4QFLajgAy-oJ6-qZAtBwJAAKJYSN1IvRz6idba7ab3U44YgtX3kwRE0yotbttzeRFCCSD8QuvjBJSzUiYNKbXAZgkg2V0aMfGhAfJRi6AgmlkgnY0gmlwhIjzWNuDaXA2kCoBBPgBcQDcAAAAAAAAAAKJc2VjcDI1NmsxoQLYYURYDijb3HRPx6MDWt3HGS-GtWwdhqudRJ4ye_9VF4N0Y3CCdyeDdWRwgncn"
            .parse()
            .unwrap();
        assert!(enr.ip6().is_some());
        assert!(enr.tcp6().is_none());
        assert!(enr.tcp4().is_some());
        assert!(enr.udp6().is_none());
        assert!(enr.udp4().is_some());
        let expected_tcp = enr.tcp4().unwrap();
        let expected_udp = enr.udp4().unwrap();
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
            .expect("IPv6 ENR should use generic TCP/UDP ports as EIP-778 fallback");

        assert!(record.tcp_addr().ip().is_ipv6());
        assert_eq!(record.tcp_port, expected_tcp);
        assert_eq!(record.udp_port, expected_udp);
    }

    #[test]
    fn dns_event_conversion_preserves_ipv6_generic_ports_and_fork_id() {
        let secret = SecretKey::from_byte_array(&[0x17; 32]).unwrap();
        let ipv6 = "2001:db8::17".parse::<Ipv6Addr>().unwrap();
        let fork_id = MAINNET.latest_fork_id();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip6(ipv6)
            .tcp4(30303)
            .udp4(30304)
            .add_value(b"eth", &EnrForkIdEntry::from(fork_id))
            .build(&secret)
            .unwrap();

        let update = dns_node_record_update_from_event(DnsDiscoveryEvent::Enr(enr))
            .expect("valid ENR should convert into a DNS update");
        let record = dns_node_record_for_dial_families(DialAddressFamilies::IPV6, &update)
            .expect("IPv6 dial family should use generic TCP/UDP fallback");

        assert_eq!(update.fork_id, Some(fork_id));
        assert_eq!(record.tcp_addr().ip(), IpAddr::V6(ipv6));
        assert_eq!(record.tcp_port, 30303);
        assert_eq!(record.udp_port, 30304);
    }

    #[test]
    fn dns_node_record_for_dual_dial_families_prefers_ipv4_when_available() {
        let secret = SecretKey::from_byte_array(&[0x18; 32]).unwrap();
        let ipv4 = Ipv4Addr::new(198, 51, 100, 12);
        let ipv6 = "2001:db8::12".parse::<Ipv6Addr>().unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip4(ipv4)
            .tcp4(30303)
            .udp4(30303)
            .ip6(ipv6)
            .tcp6(30304)
            .udp6(30304)
            .build(&secret)
            .unwrap();
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V4(ipv4),
                30303,
                Some(30303),
                PeerId::repeat_byte(0x01),
            ),
            fork_id: None,
            enr,
        };

        let record = dns_node_record_for_dial_families(DialAddressFamilies::BOTH, &update)
            .expect("dual-family ENR should produce a dialable endpoint");

        assert_eq!(record.tcp_addr().ip(), IpAddr::V4(ipv4));
        assert_eq!(record.tcp_port, 30303);
    }

    #[test]
    fn initial_dns_direct_boot_node_prefers_ipv6_for_dual_family_startup() {
        let secret = SecretKey::from_byte_array(&[0x1a; 32]).unwrap();
        let ipv4 = Ipv4Addr::new(198, 51, 100, 22);
        let ipv6 = "2001:db8::22".parse::<Ipv6Addr>().unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip4(ipv4)
            .tcp4(30303)
            .udp4(30303)
            .ip6(ipv6)
            .tcp6(30304)
            .udp6(30304)
            .build(&secret)
            .unwrap();
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V4(ipv4),
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

        let record = dns_initial_ipv6_direct_boot_node(&fork_filter, &update)
            .expect("initial dual-family DNS bootnode collection should keep IPv6 candidates");

        assert_eq!(record.tcp_addr().ip(), IpAddr::V6(ipv6));
        assert_eq!(record.tcp_port, 30304);
    }

    #[test]
    fn dns_node_record_for_dual_dial_families_accepts_ipv6_only_record() {
        let secret = SecretKey::from_byte_array(&[0x19; 32]).unwrap();
        let ipv6 = "2001:db8::19".parse::<Ipv6Addr>().unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip6(ipv6)
            .tcp6(30304)
            .udp6(30304)
            .build(&secret)
            .unwrap();
        let update = DnsNodeRecordUpdate {
            node_record: NodeRecord::new_with_ports(
                IpAddr::V6(ipv6),
                30304,
                Some(30304),
                PeerId::repeat_byte(0x01),
            ),
            fork_id: None,
            enr,
        };

        let record = dns_node_record_for_dial_families(DialAddressFamilies::BOTH, &update)
            .expect("dual-family dialing should keep IPv6-only ENRs");

        assert_eq!(record.tcp_addr().ip(), IpAddr::V6(ipv6));
        assert_eq!(record.tcp_port, 30304);
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
                .expect("IPv6 DNS bootnodes with UDP should seed unsigned discovery");

        assert!(record.tcp_addr().ip().is_ipv6());
        assert_eq!(record.udp_port, 30303);
    }

    #[test]
    fn parse_execution_bootnodes_accepts_ipv6_enode() {
        let bootnodes = parse_execution_bootnodes(&["enode://1dd9d65c4552b5eb43d5ad55a2ee3f56c6cbc1c64a5c8d659f51fcd51bace24351232b8d7821617d2b29b54b81cdefb9b3e9c37d7fd5f63270bcc9e1a6f6a439@[2001:db8:3c4d:15::abcd:ef12]:52150?discport=52151".to_owned()])
            .expect("valid IPv6 enode should parse");

        assert_eq!(bootnodes.node_records.len(), 1);
        assert!(bootnodes.signed_enrs.is_empty());
        assert!(bootnodes.node_records[0].tcp_addr().ip().is_ipv6());
        assert_eq!(bootnodes.node_records[0].tcp_port, 52150);
        assert_eq!(bootnodes.node_records[0].udp_port, 52151);
    }

    #[test]
    fn parse_execution_bootnodes_accepts_signed_ipv6_enr() {
        let key = reth_discv5::discv5::enr::CombinedKey::generate_secp256k1();
        let ipv6 = "2001:db8:4::5".parse::<Ipv6Addr>().unwrap();
        let enr = Discv5Enr::builder()
            .ip6(ipv6)
            .tcp6(52150)
            .udp6(52151)
            .build(&key)
            .unwrap();

        let bootnodes =
            parse_execution_bootnodes(&[enr.to_string()]).expect("valid signed ENR should parse");

        assert!(bootnodes.node_records.is_empty());
        assert_eq!(bootnodes.signed_enrs.len(), 1);

        let node = signed_enr_node_record_for_dial_families(
            DialAddressFamilies::IPV6,
            &bootnodes.signed_enrs[0],
        )
        .expect("IPv6 ENR should produce a direct IPv6 candidate");
        assert_eq!(node.tcp_addr().ip(), IpAddr::V6(ipv6));
        assert_eq!(node.tcp_port, 52150);
        assert_eq!(node.udp_port, 52151);
        assert!(signed_enr_matches_discovery_bind_ip(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            &bootnodes.signed_enrs[0]
        ));
        assert!(!signed_enr_matches_discovery_bind_ip(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            &bootnodes.signed_enrs[0]
        ));
    }

    #[test]
    fn signed_enr_direct_candidate_uses_generic_ports_for_ipv6_fallback() {
        let key = reth_discv5::discv5::enr::CombinedKey::generate_secp256k1();
        let ipv6 = "2001:db8:4::66".parse::<Ipv6Addr>().unwrap();
        let enr = Discv5Enr::builder()
            .ip6(ipv6)
            .tcp4(30303)
            .udp4(30304)
            .build(&key)
            .unwrap();

        let node = signed_enr_node_record_for_dial_families(DialAddressFamilies::IPV6, &enr)
            .expect("IPv6 family should use generic TCP/UDP fallback ports");

        assert_eq!(node.tcp_addr().ip(), IpAddr::V6(ipv6));
        assert_eq!(node.tcp_port, 30303);
        assert_eq!(node.udp_port, 30304);
        assert!(!signed_enr_matches_discovery_bind_ip(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            &enr
        ));
    }

    #[test]
    fn signed_enr_direct_candidate_respects_dial_family() {
        let key = reth_discv5::discv5::enr::CombinedKey::generate_secp256k1();
        let ipv4 = Ipv4Addr::new(198, 51, 100, 77);
        let ipv6 = "2001:db8:4::77".parse::<Ipv6Addr>().unwrap();
        let enr = Discv5Enr::builder()
            .ip4(ipv4)
            .tcp4(30303)
            .udp4(30303)
            .ip6(ipv6)
            .tcp6(30304)
            .udp6(30304)
            .build(&key)
            .unwrap();

        let ipv4_node = signed_enr_node_record_for_dial_families(DialAddressFamilies::IPV4, &enr)
            .expect("IPv4 family should use IPv4 ENR fields");
        let ipv6_node = signed_enr_node_record_for_dial_families(DialAddressFamilies::IPV6, &enr)
            .expect("IPv6 family should use IPv6 ENR fields");
        let dual_node = signed_enr_node_record_for_dial_families(DialAddressFamilies::BOTH, &enr)
            .expect("dual family should prefer IPv4 when available");

        assert_eq!(ipv4_node.tcp_addr().ip(), IpAddr::V4(ipv4));
        assert_eq!(ipv4_node.tcp_port, 30303);
        assert_eq!(ipv6_node.tcp_addr().ip(), IpAddr::V6(ipv6));
        assert_eq!(ipv6_node.tcp_port, 30304);
        assert_eq!(dual_node.tcp_addr().ip(), IpAddr::V4(ipv4));
        assert_eq!(dual_node.tcp_port, 30303);
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

        let direct = dns_initial_ipv6_direct_boot_node(&fork_filter, &update)
            .expect("IPv6 DNS ENRs with TCP should still be direct RLPx candidates");
        assert_eq!(direct.tcp_addr().ip(), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(direct.tcp_port, 30303);
        assert_eq!(direct.udp_port, 30303);
    }

    #[test]
    fn dns_signed_boot_node_accepts_ipv6_udp_without_tcp() {
        let secret = SecretKey::from_byte_array(&[0x34; 32]).unwrap();
        let ipv6 = "2001:db8:34::1".parse::<Ipv6Addr>().unwrap();
        let enr = enr::Enr::<SecretKey>::builder()
            .ip6(ipv6)
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

        assert!(
            dns_boot_node_for_bind_ip(IpAddr::V6(Ipv6Addr::UNSPECIFIED), &fork_filter, &update)
                .is_none()
        );
        let signed = dns_signed_boot_node_for_bind_ip(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            &fork_filter,
            &update,
        )
        .expect("IPv6 UDP ENR should seed discv5");

        assert_eq!(signed.ip6(), Some(ipv6));
        assert_eq!(signed.udp6(), Some(30303));
        assert_eq!(signed.tcp6(), None);
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
        assert!(
            dns_signed_boot_node_for_bind_ip(
                IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                &fork_filter,
                &update
            )
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
        assert_eq!(
            ipv6_config.dns_record_cache_limit.get(),
            DNS_DISCOVERY_CACHE_LIMIT
        );
        assert!(
            ipv6_config.max_requests_per_sec > ipv4_config.max_requests_per_sec,
            "IPv6 startup needs to skip through mostly IPv4-only mainnet ENRs quickly"
        );
    }
}
