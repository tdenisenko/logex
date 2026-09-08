use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::fs;
use std::io;
use std::net::IpAddr;
use std::num::NonZeroU8;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy_primitives::{B256, hex};
use discv5::enr::{CombinedKey, CombinedPublicKey, NodeId};
use discv5::{ConfigBuilder, Discv5, Enr, Event, ListenConfig};
use futures::{StreamExt, future::Either};
use libp2p::bytes::Bytes;
use libp2p::core::{ConnectedPoint, muxing::StreamMuxerBox, transport::Boxed};
use libp2p::gossipsub;
use libp2p::identify;
use libp2p::identity;
use libp2p::multiaddr::Protocol;
use libp2p::request_response;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{DialError, NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId, SwarmBuilder, Transport, noise, tcp, yamux};
use libp2p_mplex as mplex;
use logex_types::{
    ConsensusDataFork, ConsensusNetworkStatus, NodeState, SyncStatus, WeakSubjectivityCheckpoint,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::rpc::{
    BEACON_BLOCKS_BY_RANGE_V1_PROTOCOL_ID, BEACON_BLOCKS_BY_RANGE_V2_PROTOCOL_ID,
    BEACON_BLOCKS_BY_ROOT_V1_PROTOCOL_ID, BEACON_BLOCKS_BY_ROOT_V2_PROTOCOL_ID,
    BeaconBlocksByRangeRequest, Eth2OutboundRequestId, Eth2RpcBehaviour, Eth2RpcEvent,
    Eth2RpcRequest, Eth2RpcResponse, GOODBYE_V1_PROTOCOL_ID, LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID,
    LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID, LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID,
    LIGHT_CLIENT_UPDATES_BY_RANGE_PROTOCOL_ID, LightClientUpdatesByRangeRequest,
    METADATA_V1_PROTOCOL_ID, METADATA_V2_PROTOCOL_ID, METADATA_V3_PROTOCOL_ID, MetaData,
    PING_PROTOCOL_ID, RawRpcResponse, STATUS_V1_PROTOCOL_ID, STATUS_V2_PROTOCOL_ID, StatusMessage,
    build_beacon_blocks_by_range_behaviour, build_beacon_blocks_by_root_behaviour,
    build_goodbye_behaviour, build_light_client_bootstrap_behaviour,
    build_light_client_finality_update_behaviour, build_light_client_optimistic_update_behaviour,
    build_light_client_updates_by_range_behaviour, build_metadata_behaviour, build_ping_behaviour,
    build_status_behaviour, invalid_request, rate_limited, resource_unavailable,
};
use crate::{
    ConsensusStore, LightClientVerificationError, MAINNET_CONSENSUS_CHAIN_SPEC,
    VerifiedBeaconBlock, VerifiedLightClientStore, apply_finality_update_payload,
    apply_light_client_update_payload, apply_optimistic_update_payload, decode_finality_update,
    decode_optimistic_update, decode_verified_beacon_block, force_update_light_client_store,
    verify_bootstrap_payload,
};

const CONSENSUS_STATE_DIR: &str = "cl";
const DISCOVERY_SECRET_FILE: &str = "discovery-secret";
const KNOWN_PEERS_FILE: &str = "known-peers.json";
const DISCOVERY_QUERY_INTERVAL: Duration = Duration::from_secs(15);
const DISCOVERY_QUERY_FANOUT: usize = 4;
const MIN_DISCOVERY_PEER_RESERVE: usize = 256;
const MAX_RETAINED_DISCONNECTED_PEERS: usize = 500;
const KNOWN_PEER_PERSIST_INTERVAL: Duration = Duration::from_secs(30);
const RPC_REQUEST_INTERVAL: Duration = Duration::from_secs(5);
const STATUS_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(300);
const PING_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(15);
const FINALITY_UPDATE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const CURRENT_PERIOD_UPDATE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const MAX_CONCURRENT_DEFAULT_RPC_REQUESTS: usize = 2;
const MAX_CONCURRENT_STATUS_REQUESTS: usize = 4;
const MAX_CONCURRENT_HISTORY_REQUESTS: usize = 8;
const MAX_BEACON_BLOCKS_BY_ROOT_REQUEST: usize = 128;
const MAX_LIGHT_CLIENT_UPDATES_BY_RANGE_REQUEST: u64 = 128;
const MAX_BEACON_BLOCKS_BY_RANGE_REQUEST: u64 = 128;
const FORWARD_BEACON_BLOCK_RANGE_WINDOW: u64 = 16;
const MAX_DIAL_ADDRESSES_PER_ATTEMPT: usize = 2;
const MAX_PERSISTED_KNOWN_PEERS: usize = 256;
const HEAD_RECOVERY_PROGRESSION_PEER_RESERVE: usize = 2;
const MAX_STATUS_FAILURES_BEFORE_DISCONNECT: u32 = 2;
const IDENTIFY_GRACE_PERIOD: Duration = Duration::from_secs(8);
const PEER_BACKOFF_BASE: Duration = Duration::from_secs(15);
const PEER_BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);
const GOODBYE_REASON_IRRELEVANT_NETWORK: u64 = 2;
const GOODBYE_REASON_FAULT: u64 = 3;
const IDENTIFY_PROTOCOL_VERSION: &str = "eth2/1.0.0";
const IDENTIFY_AGENT_VERSION: &str = concat!("logex/", env!("CARGO_PKG_VERSION"));
const GOSSIP_MAX_TRANSMIT_SIZE: usize = 10 * 1024 * 1024 + 1024;
const P2P_BANDWIDTH_RATE_WINDOW: Duration = Duration::from_secs(15);
const LIGHT_CLIENT_FINALITY_UPDATE_TOPIC_NAME: &str = "light_client_finality_update";
const LIGHT_CLIENT_OPTIMISTIC_UPDATE_TOPIC_NAME: &str = "light_client_optimistic_update";
const GOSSIP_ENCODING_NAME: &str = "ssz_snappy";
const MESSAGE_DOMAIN_VALID_SNAPPY: [u8; 4] = [1, 0, 0, 0];
const ATTESTATION_SUBNET_BITFIELD: [u8; 8] = [0u8; 8];
const SYNCNET_BITFIELD: [u8; 1] = [0u8; 1];
const LOCAL_CUSTODY_GROUP_COUNT: u64 = 0;
const INBOUND_RATE_LIMIT_RETENTION: Duration = Duration::from_secs(60);
const FORK_TOPIC_SUBSCRIBE_DELAY_SLOTS: u64 = 2;
const FORK_TOPIC_UNSUBSCRIBE_DELAY_EPOCHS: u64 = 2;
const CONSENSUS_NETWORK_RESTART_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct ConsensusNetworkConfig {
    pub data_dir: PathBuf,
    pub checkpoint: WeakSubjectivityCheckpoint,
    pub bind_ip: IpAddr,
    pub dial_families: ConsensusDialAddressFamilies,
    pub external_ip: Option<IpAddr>,
    pub discovery_port: u16,
    pub p2p_port: u16,
    pub max_peers: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusDialAddressFamilies {
    ipv4: bool,
    ipv6: bool,
}

impl ConsensusDialAddressFamilies {
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

    const fn includes(self, class: DialAddressClass) -> bool {
        match class {
            DialAddressClass::Tcp4 | DialAddressClass::Quic4 => self.ipv4,
            DialAddressClass::Tcp6 | DialAddressClass::Quic6 => self.ipv6,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConsensusNetworkError {
    #[error("failed to load discovery secret {path}: {source}")]
    ReadSecret { path: PathBuf, source: io::Error },
    #[error("failed to parse discovery secret {path}: {message}")]
    ParseSecret { path: PathBuf, message: String },
    #[error("failed to persist discovery secret {path}: {source}")]
    PersistSecret { path: PathBuf, source: io::Error },
    #[error("failed to load known peers {path}: {source}")]
    ReadKnownPeers { path: PathBuf, source: io::Error },
    #[error("failed to parse known peers {path}: {message}")]
    ParseKnownPeers { path: PathBuf, message: String },
    #[error("failed to persist known peers {path}: {source}")]
    PersistKnownPeers { path: PathBuf, source: io::Error },
    #[error("invalid built-in mainnet bootnode ENR: {0}")]
    InvalidBootnode(String),
    #[error("failed to construct consensus discovery service: {0}")]
    ConstructDiscovery(String),
    #[error("failed to start consensus discovery service: {0}")]
    StartDiscovery(String),
    #[error("failed to open consensus discovery event stream: {0}")]
    EventStream(String),
    #[error("failed to construct consensus libp2p transport: {0}")]
    ConstructRpcTransport(String),
    #[error("failed to construct consensus gossipsub behaviour: {0}")]
    ConstructGossip(String),
    #[error("failed to bind consensus libp2p listener: {0}")]
    ListenRpcTransport(String),
    #[error("failed to derive libp2p identity from consensus secret key: {0}")]
    Libp2pIdentity(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedPeer {
    enr: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    support: Option<PeerRpcSupport>,
    #[serde(default)]
    status_successes: u32,
    #[serde(default)]
    bootstrap_successes: u32,
    #[serde(default)]
    useful_successes: u32,
    #[serde(default)]
    dial_stats: PeerDialAddressStats,
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "ConsensusBehaviourEvent")]
struct ConsensusBehaviour {
    connection_limits: libp2p::connection_limits::Behaviour,
    identify: identify::Behaviour,
    gossip: gossipsub::Behaviour,
    status_rpc: StatusRpcBehaviour,
    goodbye_rpc: GoodbyeRpcBehaviour,
    metadata_rpc: MetadataRpcBehaviour,
    ping_rpc: PingRpcBehaviour,
    light_client_bootstrap_rpc: LightClientBootstrapRpcBehaviour,
    light_client_updates_by_range_rpc: LightClientUpdatesByRangeRpcBehaviour,
    light_client_finality_update_rpc: LightClientFinalityUpdateRpcBehaviour,
    light_client_optimistic_update_rpc: LightClientOptimisticUpdateRpcBehaviour,
    beacon_blocks_by_range_rpc: BeaconBlocksByRangeRpcBehaviour,
    beacon_blocks_by_root_rpc: BeaconBlocksByRootRpcBehaviour,
}

#[derive(Debug)]
enum ConsensusBehaviourEvent {
    ConnectionLimits(Infallible),
    Identify(Box<identify::Event>),
    Gossip(Box<gossipsub::Event>),
    StatusRpc(Eth2RpcEvent),
    GoodbyeRpc(Eth2RpcEvent),
    MetadataRpc(Eth2RpcEvent),
    PingRpc(Eth2RpcEvent),
    LightClientBootstrapRpc(Eth2RpcEvent),
    LightClientUpdatesByRangeRpc(Eth2RpcEvent),
    LightClientFinalityUpdateRpc(Eth2RpcEvent),
    LightClientOptimisticUpdateRpc(Eth2RpcEvent),
    BeaconBlocksByRangeRpc(Eth2RpcEvent),
    BeaconBlocksByRootRpc(Eth2RpcEvent),
}

impl From<Infallible> for ConsensusBehaviourEvent {
    fn from(event: Infallible) -> Self {
        Self::ConnectionLimits(event)
    }
}

impl From<identify::Event> for ConsensusBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
    }
}

impl From<gossipsub::Event> for ConsensusBehaviourEvent {
    fn from(event: gossipsub::Event) -> Self {
        Self::Gossip(Box::new(event))
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "StatusRpcWrappedEvent")]
struct StatusRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct StatusRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for StatusRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<StatusRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: StatusRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::StatusRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "GoodbyeRpcWrappedEvent")]
struct GoodbyeRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct GoodbyeRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for GoodbyeRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<GoodbyeRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: GoodbyeRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::GoodbyeRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "MetadataRpcWrappedEvent")]
struct MetadataRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct MetadataRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for MetadataRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<MetadataRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: MetadataRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::MetadataRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "PingRpcWrappedEvent")]
struct PingRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct PingRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for PingRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<PingRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: PingRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::PingRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "LightClientBootstrapRpcWrappedEvent")]
struct LightClientBootstrapRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct LightClientBootstrapRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for LightClientBootstrapRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<LightClientBootstrapRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: LightClientBootstrapRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::LightClientBootstrapRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "LightClientUpdatesByRangeRpcWrappedEvent")]
struct LightClientUpdatesByRangeRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct LightClientUpdatesByRangeRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for LightClientUpdatesByRangeRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<LightClientUpdatesByRangeRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: LightClientUpdatesByRangeRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::LightClientUpdatesByRangeRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "LightClientFinalityUpdateRpcWrappedEvent")]
struct LightClientFinalityUpdateRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct LightClientFinalityUpdateRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for LightClientFinalityUpdateRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<LightClientFinalityUpdateRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: LightClientFinalityUpdateRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::LightClientFinalityUpdateRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "LightClientOptimisticUpdateRpcWrappedEvent")]
struct LightClientOptimisticUpdateRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct LightClientOptimisticUpdateRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for LightClientOptimisticUpdateRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<LightClientOptimisticUpdateRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: LightClientOptimisticUpdateRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::LightClientOptimisticUpdateRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "BeaconBlocksByRangeRpcWrappedEvent")]
struct BeaconBlocksByRangeRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct BeaconBlocksByRangeRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for BeaconBlocksByRangeRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<BeaconBlocksByRangeRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: BeaconBlocksByRangeRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::BeaconBlocksByRangeRpc(event.0)
    }
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "BeaconBlocksByRootRpcWrappedEvent")]
struct BeaconBlocksByRootRpcBehaviour {
    inner: Eth2RpcBehaviour,
}

#[derive(Debug)]
struct BeaconBlocksByRootRpcWrappedEvent(Eth2RpcEvent);

impl From<Eth2RpcEvent> for BeaconBlocksByRootRpcWrappedEvent {
    fn from(event: Eth2RpcEvent) -> Self {
        Self(event)
    }
}

impl From<BeaconBlocksByRootRpcWrappedEvent> for ConsensusBehaviourEvent {
    fn from(event: BeaconBlocksByRootRpcWrappedEvent) -> Self {
        ConsensusBehaviourEvent::BeaconBlocksByRootRpc(event.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingRequestKey {
    kind: RpcRequestKind,
    request_id: Eth2OutboundRequestId,
}

#[derive(Debug, Clone, Copy)]
struct InboundRateLimitBucket {
    window_started_at: Instant,
    used: u64,
}

pub fn spawn_consensus_network(
    config: ConsensusNetworkConfig,
    consensus: Arc<ConsensusStore>,
    sync_status: Arc<Mutex<SyncStatus>>,
    shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>, ConsensusNetworkError> {
    let network = ConsensusNetwork::new(
        config.clone(),
        Arc::clone(&consensus),
        Arc::clone(&sync_status),
    )?;
    Ok(tokio::spawn(async move {
        let mut shutdown = shutdown;
        let mut network = network;
        loop {
            let outcome = tokio::spawn(network.run(shutdown.clone())).await;
            if *shutdown.borrow() {
                break;
            }

            mark_consensus_network_unavailable(&sync_status);
            match outcome {
                Ok(Ok(())) => {
                    tracing::error!("consensus network exited unexpectedly; restarting");
                }
                Ok(Err(error)) => {
                    tracing::error!(%error, "consensus network exited with an error; restarting");
                }
                Err(error) => {
                    tracing::error!(
                        %error,
                        panicked = error.is_panic(),
                        "consensus network task failed; restarting"
                    );
                }
            }

            loop {
                tokio::select! {
                    _ = tokio::time::sleep(CONSENSUS_NETWORK_RESTART_DELAY) => {}
                    _ = wait_for_shutdown(&mut shutdown) => return,
                }
                match ConsensusNetwork::new(
                    config.clone(),
                    Arc::clone(&consensus),
                    Arc::clone(&sync_status),
                ) {
                    Ok(next_network) => {
                        network = next_network;
                        tracing::info!("consensus network supervisor restarted the network task");
                        break;
                    }
                    Err(error) => {
                        tracing::error!(%error, "failed to reconstruct consensus network; retrying");
                    }
                }
            }
        }
    }))
}

fn mark_consensus_network_unavailable(sync_status: &Arc<Mutex<SyncStatus>>) {
    let current_slot = current_wall_clock_slot();
    let mut status = sync_status.lock().unwrap();
    let optimistic_slot = status
        .optimistic_execution_head
        .map(|anchor| anchor.beacon_slot);
    let optimistic_lag =
        optimistic_slot.map(|slot| crate::optimistic_head_lag_slots(current_slot, slot));

    status.consensus_current_slot = Some(current_slot);
    status.consensus_head_lag_slots = optimistic_lag;
    status.consensus_head_fresh = Some(false);
    status.consensus_status_updated_at_unix_ms = Some(unix_time_millis());
    status.node_state = NodeState::WaitingForConsensus;
    status.syncing = false;
    if let Some(network) = status.consensus_network.as_mut() {
        network.current_slot = current_slot;
        network.optimistic_head_lag_slots = optimistic_lag;
        network.head_recovery_active = true;
        network.connected_peer_sessions = 0;
        network.dialing_peer_sessions = 0;
        network.pending_rpc_requests = 0;
        network.p2p_download_bytes_per_sec = 0;
        network.p2p_upload_bytes_per_sec = 0;
    }
}

struct ConsensusNetwork {
    config: ConsensusNetworkConfig,
    consensus: Arc<ConsensusStore>,
    sync_status: Arc<Mutex<SyncStatus>>,
    discv5: Discv5,
    swarm: Swarm<ConsensusBehaviour>,
    bootnode_count: usize,
    bootnode_peers: HashSet<PeerId>,
    fork_digest: [u8; 4],
    known_peers_path: PathBuf,
    last_persisted: Vec<PersistedPeer>,
    observed: HashSet<PeerId>,
    dialable_peers: HashMap<PeerId, Vec<Multiaddr>>,
    dialing_peers: HashSet<PeerId>,
    connected_peers: HashSet<PeerId>,
    closing_peers: HashSet<PeerId>,
    connected_since: HashMap<PeerId, Instant>,
    peer_endpoints: HashMap<PeerId, String>,
    peer_lifecycle: HashMap<PeerId, PeerLifecycleState>,
    peer_support: HashMap<PeerId, PeerRpcSupport>,
    peer_failures: HashMap<PeerId, PeerFailureCounts>,
    inbound_status_peers: HashSet<PeerId>,
    status_peers: HashSet<PeerId>,
    metadata_peers: HashSet<PeerId>,
    ping_peers: HashSet<PeerId>,
    last_status_success_at: HashMap<PeerId, Instant>,
    last_ping_success_at: HashMap<PeerId, Instant>,
    bootstrap_peers: HashSet<PeerId>,
    updates_by_range_peers: HashSet<PeerId>,
    finality_update_peers: HashSet<PeerId>,
    optimistic_update_peers: HashSet<PeerId>,
    beacon_blocks_by_range_peers: HashSet<PeerId>,
    beacon_blocks_by_root_peers: HashSet<PeerId>,
    pending_requests: HashMap<PendingRequestKey, PeerId>,
    pending_history_root_requests: HashMap<PendingRequestKey, Vec<B256>>,
    pending_history_range_requests: HashMap<PendingRequestKey, BeaconBlocksByRangeRequest>,
    pending_peer_kinds: HashSet<(PeerId, RpcRequestKind)>,
    inbound_rate_limits: HashMap<(PeerId, RpcRequestKind), InboundRateLimitBucket>,
    last_light_client_request_at: HashMap<RpcRequestKind, Instant>,
    request_failures: RpcFailureCounts,
    gossip_topics: ConsensusGossipTopics,
    pre_subscribed_fork_digest: Option<[u8; 4]>,
    retiring_gossip_topics: Vec<RetiringGossipTopics>,
    gossip_counts: GossipMessageCounts,
    p2p_download_metrics: PayloadBandwidthWindow,
    p2p_upload_metrics: PayloadBandwidthWindow,
    gossip_subscriptions: HashSet<gossipsub::TopicHash>,
    last_connection_event: Option<String>,
    last_identify_event: Option<String>,
    last_peer_policy_event: Option<String>,
    last_rpc_failure: Option<String>,
    last_response_send_failure: Option<String>,
    verified_beacon_blocks: HashMap<B256, VerifiedBeaconBlock>,
    verified_beacon_block_children: HashMap<B256, Vec<VerifiedBeaconBlock>>,
    verified_beacon_block_payloads: HashMap<B256, RawRpcResponse>,
    active_history_target: Option<HistorySyncTarget>,
    head_recovery_attempts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HistorySyncTarget {
    checkpoint_root: B256,
    checkpoint_slot: u64,
    finalized_root: B256,
    optimistic_root: B256,
    optimistic_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CachedForwardPathProgress {
    checkpoint_slot: u64,
    target_slot: u64,
    highest_cached_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MaterializableHistoryChain {
    blocks: Vec<VerifiedBeaconBlock>,
    reaches_optimistic_head: bool,
}

fn select_history_sync_target(
    current: Option<HistorySyncTarget>,
    latest: Option<HistorySyncTarget>,
    current_complete: bool,
) -> Option<HistorySyncTarget> {
    match (current, latest) {
        (_, None) => None,
        (None, latest) => latest,
        (Some(current), Some(latest))
            if current_complete
                || current.checkpoint_root != latest.checkpoint_root
                || latest.optimistic_slot <= current.optimistic_slot
                || (current.optimistic_root == latest.optimistic_root
                    && current.optimistic_slot == latest.optimistic_slot) =>
        {
            Some(latest)
        }
        (Some(current), Some(_latest)) => Some(current),
    }
}

fn select_forward_history_range_target(
    current: Option<HistorySyncTarget>,
    latest: Option<HistorySyncTarget>,
) -> Option<HistorySyncTarget> {
    match (current, latest) {
        (None, latest) => latest,
        (Some(current), Some(latest))
            if latest.checkpoint_root == current.checkpoint_root
                && latest.checkpoint_slot == current.checkpoint_slot
                && latest.optimistic_slot > current.optimistic_slot =>
        {
            Some(latest)
        }
        (Some(current), _) => Some(current),
    }
}

fn live_head_progression_needed(target: Option<HistorySyncTarget>, current_slot: u64) -> bool {
    target.is_some_and(|target| {
        target.optimistic_slot <= target.checkpoint_slot
            || !crate::optimistic_head_is_fresh_at(current_slot, target.optimistic_slot)
    })
}

fn limited_local_status_message(fork_digest: [u8; 4]) -> StatusMessage {
    StatusMessage::genesis(fork_digest, MAINNET_CONSENSUS_CHAIN_SPEC.genesis_block_root)
}

fn consensus_data_fork_for_slot(slot: u64) -> ConsensusDataFork {
    match MAINNET_CONSENSUS_CHAIN_SPEC
        .fork_version_for_epoch(MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(slot))[0]
    {
        version if version >= 0x05 => ConsensusDataFork::Electra,
        0x04 => ConsensusDataFork::Deneb,
        _ => ConsensusDataFork::Capella,
    }
}

fn verified_beacon_blocks_from_anchor_records(
    anchors: &[crate::AnchorRecord],
) -> HashMap<B256, VerifiedBeaconBlock> {
    let mut blocks = HashMap::new();
    let mut previous_root = None;
    for record in anchors {
        let anchor = record.anchor;
        let parent_root = record
            .parent_beacon_root
            .or(previous_root)
            .unwrap_or_default();
        blocks.insert(
            anchor.beacon_root,
            VerifiedBeaconBlock {
                fork: consensus_data_fork_for_slot(anchor.beacon_slot),
                beacon_root: anchor.beacon_root,
                parent_root,
                slot: anchor.beacon_slot,
                execution_anchor: anchor,
            },
        );
        previous_root = Some(anchor.beacon_root);
    }
    blocks
}

fn verified_beacon_blocks_from_light_client_store(
    store: &VerifiedLightClientStore,
) -> Vec<VerifiedBeaconBlock> {
    [&store.finalized_header, &store.optimistic_header]
        .into_iter()
        .filter_map(|header| {
            let execution_anchor = header.execution_anchor()?;
            Some(VerifiedBeaconBlock {
                fork: header.fork,
                beacon_root: execution_anchor.beacon_root,
                parent_root: header.beacon.parent_root,
                slot: header.beacon.slot,
                execution_anchor,
            })
        })
        .collect()
}

fn finality_update_is_stale(bytes: &[u8], store: &VerifiedLightClientStore) -> bool {
    decode_finality_update(bytes).is_ok_and(|status| {
        status.attested_header.beacon_slot <= store.optimistic_header.beacon.slot
            && status.finalized_header.beacon_slot <= store.finalized_header.beacon.slot
    })
}

fn optimistic_update_is_stale(bytes: &[u8], store: &VerifiedLightClientStore) -> bool {
    decode_optimistic_update(bytes).is_ok_and(|status| {
        status.attested_header.beacon_slot <= store.optimistic_header.beacon.slot
    })
}

fn verified_beacon_block_children_from_blocks(
    blocks: &HashMap<B256, VerifiedBeaconBlock>,
) -> HashMap<B256, Vec<VerifiedBeaconBlock>> {
    let mut children: HashMap<B256, Vec<VerifiedBeaconBlock>> = HashMap::new();
    for block in blocks.values().copied() {
        children.entry(block.parent_root).or_default().push(block);
    }
    for blocks in children.values_mut() {
        blocks.sort_by_key(|block| (block.slot, block.beacon_root));
    }
    children
}

#[derive(Debug, Clone, Copy, Default)]
struct PostBootstrapRequestReadiness {
    priority_root_ready: bool,
    range_ready: bool,
    deferred_root_ready: bool,
    updates_ready: bool,
    finality_ready: bool,
    optimistic_ready: bool,
    pending_priority_root: bool,
    pending_range: bool,
    prefer_range_when_both_ready: bool,
}

fn select_post_bootstrap_request_kind(
    readiness: PostBootstrapRequestReadiness,
) -> Option<RpcRequestKind> {
    let root_ready = readiness.priority_root_ready || readiness.deferred_root_ready;
    if root_ready && readiness.range_ready {
        if !readiness.pending_range && readiness.pending_priority_root {
            Some(RpcRequestKind::BeaconBlocksByRange)
        } else if readiness.pending_range && !readiness.pending_priority_root {
            Some(RpcRequestKind::BeaconBlocksByRoot)
        } else if readiness.prefer_range_when_both_ready {
            Some(RpcRequestKind::BeaconBlocksByRange)
        } else {
            Some(RpcRequestKind::BeaconBlocksByRoot)
        }
    } else if readiness.priority_root_ready {
        Some(RpcRequestKind::BeaconBlocksByRoot)
    } else if readiness.range_ready {
        Some(RpcRequestKind::BeaconBlocksByRange)
    } else if readiness.deferred_root_ready {
        Some(RpcRequestKind::BeaconBlocksByRoot)
    } else if readiness.updates_ready {
        Some(RpcRequestKind::LightClientUpdatesByRange)
    } else if readiness.finality_ready {
        Some(RpcRequestKind::LightClientFinalityUpdate)
    } else if readiness.optimistic_ready {
        Some(RpcRequestKind::LightClientOptimisticUpdate)
    } else {
        None
    }
}

fn select_live_head_progression_request_kind(
    updates_ready: bool,
    optimistic_ready: bool,
    finality_ready: bool,
) -> Option<RpcRequestKind> {
    if optimistic_ready {
        Some(RpcRequestKind::LightClientOptimisticUpdate)
    } else if updates_ready {
        Some(RpcRequestKind::LightClientUpdatesByRange)
    } else if finality_ready {
        Some(RpcRequestKind::LightClientFinalityUpdate)
    } else {
        None
    }
}

const fn max_concurrent_requests_for_kind(kind: RpcRequestKind) -> usize {
    match kind {
        RpcRequestKind::Status => MAX_CONCURRENT_STATUS_REQUESTS,
        RpcRequestKind::BeaconBlocksByRange | RpcRequestKind::BeaconBlocksByRoot => {
            MAX_CONCURRENT_HISTORY_REQUESTS
        }
        RpcRequestKind::Goodbye
        | RpcRequestKind::MetaData
        | RpcRequestKind::Ping
        | RpcRequestKind::LightClientBootstrap
        | RpcRequestKind::LightClientUpdatesByRange
        | RpcRequestKind::LightClientFinalityUpdate
        | RpcRequestKind::LightClientOptimisticUpdate => MAX_CONCURRENT_DEFAULT_RPC_REQUESTS,
    }
}

fn next_forward_history_range_request_for_progress(
    progress: CachedForwardPathProgress,
) -> Option<BeaconBlocksByRangeRequest> {
    if progress.highest_cached_slot >= progress.target_slot {
        return None;
    }

    let start_slot = progress
        .highest_cached_slot
        .saturating_add(1)
        .max(progress.checkpoint_slot.saturating_add(1));
    if start_slot > progress.target_slot {
        return None;
    }
    let end_slot = progress
        .target_slot
        .min(start_slot.saturating_add(FORWARD_BEACON_BLOCK_RANGE_WINDOW - 1));
    let count = end_slot.saturating_sub(start_slot).saturating_add(1);
    (count > 0).then_some(BeaconBlocksByRangeRequest {
        start_slot,
        count,
        step: 1,
    })
}

fn history_range_end_slot(request: BeaconBlocksByRangeRequest) -> u64 {
    request
        .start_slot
        .saturating_add(request.step.saturating_mul(request.count.saturating_sub(1)))
}

fn forward_progress_with_pending_ranges(
    mut progress: CachedForwardPathProgress,
    pending_ranges: &[BeaconBlocksByRangeRequest],
) -> CachedForwardPathProgress {
    loop {
        let next_slot = progress.highest_cached_slot.saturating_add(1);
        let Some(request) = pending_ranges.iter().copied().find(|request| {
            request.start_slot <= next_slot && history_range_end_slot(*request) >= next_slot
        }) else {
            break;
        };
        progress.highest_cached_slot = progress.target_slot.min(
            progress
                .highest_cached_slot
                .max(history_range_end_slot(request)),
        );
        if progress.highest_cached_slot >= progress.target_slot {
            break;
        }
    }
    progress
}

fn range_request_contains_slot(request: BeaconBlocksByRangeRequest, slot: u64) -> bool {
    if request.count == 0 || request.step == 0 || slot < request.start_slot {
        return false;
    }

    let Some(relative_slot) = slot.checked_sub(request.start_slot) else {
        return false;
    };
    let Some(max_relative_slot) = request.step.checked_mul(request.count.saturating_sub(1)) else {
        return false;
    };

    relative_slot <= max_relative_slot && relative_slot % request.step == 0
}

fn push_missing_history_root(
    roots: &mut Vec<B256>,
    root: B256,
    verified_beacon_blocks: &HashMap<B256, VerifiedBeaconBlock>,
    pending_roots: &HashSet<B256>,
) {
    if roots.len() >= MAX_BEACON_BLOCKS_BY_ROOT_REQUEST
        || verified_beacon_blocks.contains_key(&root)
        || pending_roots.contains(&root)
        || roots.contains(&root)
    {
        return;
    }
    roots.push(root);
}

fn extend_missing_history_lineage(
    roots: &mut Vec<B256>,
    start_root: B256,
    checkpoint_root: B256,
    verified_beacon_blocks: &HashMap<B256, VerifiedBeaconBlock>,
    pending_roots: &HashSet<B256>,
) {
    let mut current_root = start_root;
    let mut child_slot = None;
    loop {
        if roots.len() >= MAX_BEACON_BLOCKS_BY_ROOT_REQUEST {
            return;
        }
        let Some(block) = verified_beacon_blocks.get(&current_root) else {
            push_missing_history_root(roots, current_root, verified_beacon_blocks, pending_roots);
            return;
        };
        if block.beacon_root != current_root || child_slot.is_some_and(|slot| block.slot >= slot) {
            return;
        }
        if current_root == checkpoint_root {
            return;
        }
        child_slot = Some(block.slot);
        current_root = block.parent_root;
    }
}

#[cfg(test)]
fn next_forward_history_root_request_for_target(
    target: HistorySyncTarget,
    verified_beacon_blocks: &HashMap<B256, VerifiedBeaconBlock>,
) -> Option<Vec<B256>> {
    next_forward_history_root_request_for_target_excluding(
        target,
        verified_beacon_blocks,
        &HashSet::new(),
    )
}

fn next_forward_history_root_request_for_target_excluding(
    target: HistorySyncTarget,
    verified_beacon_blocks: &HashMap<B256, VerifiedBeaconBlock>,
    pending_roots: &HashSet<B256>,
) -> Option<Vec<B256>> {
    let mut roots = Vec::new();
    push_missing_history_root(
        &mut roots,
        target.checkpoint_root,
        verified_beacon_blocks,
        pending_roots,
    );
    extend_missing_history_lineage(
        &mut roots,
        target.optimistic_root,
        target.checkpoint_root,
        verified_beacon_blocks,
        pending_roots,
    );
    if target.finalized_root != target.optimistic_root {
        extend_missing_history_lineage(
            &mut roots,
            target.finalized_root,
            target.checkpoint_root,
            verified_beacon_blocks,
            pending_roots,
        );
    }

    (!roots.is_empty()).then_some(roots)
}

fn materializable_history_chain(
    verified_beacon_blocks: &HashMap<B256, VerifiedBeaconBlock>,
    target: HistorySyncTarget,
) -> Option<MaterializableHistoryChain> {
    if let Some(blocks) = canonical_chain_blocks_to_root(
        verified_beacon_blocks,
        target.checkpoint_root,
        target.checkpoint_slot,
        target.optimistic_root,
    ) {
        return Some(MaterializableHistoryChain {
            blocks,
            reaches_optimistic_head: true,
        });
    }

    canonical_chain_blocks_to_root(
        verified_beacon_blocks,
        target.checkpoint_root,
        target.checkpoint_slot,
        target.finalized_root,
    )
    .map(|blocks| MaterializableHistoryChain {
        blocks,
        reaches_optimistic_head: false,
    })
}

fn canonical_chain_blocks_to_root(
    verified_beacon_blocks: &HashMap<B256, VerifiedBeaconBlock>,
    checkpoint_root: B256,
    checkpoint_slot: u64,
    target_root: B256,
) -> Option<Vec<VerifiedBeaconBlock>> {
    let checkpoint_block = *verified_beacon_blocks.get(&checkpoint_root)?;
    if checkpoint_block.slot != checkpoint_slot {
        return None;
    }

    let mut current_root = target_root;
    let mut child_slot = None;
    let mut reverse_chain = Vec::new();
    loop {
        let block = if current_root == checkpoint_root {
            checkpoint_block
        } else {
            *verified_beacon_blocks.get(&current_root)?
        };
        // Beacon parents must have smaller slots, including across skipped
        // slots. This also bounds traversal if imported/cached links cycle.
        if block.beacon_root != current_root || child_slot.is_some_and(|slot| block.slot >= slot) {
            return None;
        }
        reverse_chain.push(block);
        if current_root == checkpoint_root {
            break;
        }
        child_slot = Some(block.slot);
        current_root = block.parent_root;
    }
    reverse_chain.reverse();
    Some(reverse_chain)
}

fn cached_target_lineage_roots(
    verified_beacon_blocks: &HashMap<B256, VerifiedBeaconBlock>,
    target: HistorySyncTarget,
) -> HashSet<B256> {
    let mut roots = HashSet::new();
    for root in [target.finalized_root, target.optimistic_root] {
        let mut current_root = root;
        let mut child_slot = None;
        loop {
            let Some(block) = verified_beacon_blocks.get(&current_root) else {
                roots.insert(current_root);
                break;
            };
            if block.beacon_root != current_root
                || child_slot.is_some_and(|slot| block.slot >= slot)
            {
                break;
            }
            if !roots.insert(current_root) || current_root == target.checkpoint_root {
                break;
            }
            child_slot = Some(block.slot);
            current_root = block.parent_root;
        }
    }
    roots
}

fn cached_beacon_block_payloads_by_root(
    roots: &[B256],
    payloads: &HashMap<B256, RawRpcResponse>,
) -> Vec<RawRpcResponse> {
    roots
        .iter()
        .filter_map(|root| payloads.get(root).cloned())
        .collect()
}

fn cached_beacon_block_payloads_by_range(
    request: BeaconBlocksByRangeRequest,
    canonical_blocks: &[VerifiedBeaconBlock],
    payloads: &HashMap<B256, RawRpcResponse>,
) -> Vec<RawRpcResponse> {
    if request.count == 0 || request.step == 0 {
        return Vec::new();
    }

    let end_slot = request
        .start_slot
        .saturating_add(request.step.saturating_mul(request.count.saturating_sub(1)));

    canonical_blocks
        .iter()
        .filter(|block| {
            block.slot >= request.start_slot
                && block.slot <= end_slot
                && (block.slot - request.start_slot).is_multiple_of(request.step)
        })
        .filter_map(|block| payloads.get(&block.beacon_root).cloned())
        .collect()
}

fn cached_light_client_update_payloads_by_range(
    request: LightClientUpdatesByRangeRequest,
    payloads: &BTreeMap<u64, RawRpcResponse>,
) -> Vec<RawRpcResponse> {
    if request.count == 0 {
        return Vec::new();
    }

    let end_period = request.start_period.saturating_add(request.count);
    let mut range = payloads.range(request.start_period..end_period);
    let Some((&first_period, first_payload)) = range.next() else {
        return Vec::new();
    };

    let mut responses = vec![first_payload.clone()];
    let mut expected_period = first_period.saturating_add(1);
    for (&period, payload) in range {
        if period != expected_period {
            break;
        }
        responses.push(payload.clone());
        expected_period = expected_period.saturating_add(1);
    }

    responses
}

fn select_checkpoint_forward_child<I>(
    children: I,
    preferred_roots: &HashSet<B256>,
) -> Option<VerifiedBeaconBlock>
where
    I: IntoIterator<Item = VerifiedBeaconBlock>,
{
    let mut only_child = None;
    let mut child_count = 0usize;
    let mut only_preferred_child = None;
    let mut preferred_child_count = 0usize;

    for child in children {
        child_count = child_count.saturating_add(1);
        only_child = Some(child);
        if preferred_roots.contains(&child.beacon_root) {
            preferred_child_count = preferred_child_count.saturating_add(1);
            only_preferred_child = Some(child);
        }
    }

    if preferred_child_count == 1 {
        only_preferred_child
    } else if child_count == 1 {
        only_child
    } else {
        None
    }
}

#[derive(Debug, Clone)]
struct ConsensusGossipTopics {
    finality_update: gossipsub::IdentTopic,
    optimistic_update: gossipsub::IdentTopic,
}

#[derive(Debug, Clone)]
struct RetiringGossipTopics {
    unsubscribe_at_epoch: u64,
    topics: ConsensusGossipTopics,
}

#[derive(Debug, Clone, Copy, Default)]
struct GossipMessageCounts {
    finality_update: u64,
    optimistic_update: u64,
    decode_failures: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct PayloadBandwidthSnapshot {
    bytes_per_sec: u64,
    total_payload_bytes: u64,
}

#[derive(Debug, Default)]
struct PayloadBandwidthWindow {
    events: VecDeque<(Instant, u64)>,
    window_payload_bytes: u64,
    total_payload_bytes: u64,
}

impl PayloadBandwidthWindow {
    fn record(&mut self, payload_bytes: u64, now: Instant) {
        if payload_bytes == 0 {
            return;
        }
        self.events.push_back((now, payload_bytes));
        self.window_payload_bytes = self.window_payload_bytes.saturating_add(payload_bytes);
        self.total_payload_bytes = self.total_payload_bytes.saturating_add(payload_bytes);
        self.prune(now);
    }

    fn snapshot(&mut self, now: Instant) -> PayloadBandwidthSnapshot {
        self.prune(now);
        let bytes_per_sec = self
            .events
            .front()
            .map(|(first_event_at, _)| {
                let elapsed = now
                    .saturating_duration_since(*first_event_at)
                    .max(Duration::from_secs(1))
                    .min(P2P_BANDWIDTH_RATE_WINDOW);
                (self.window_payload_bytes as f64 / elapsed.as_secs_f64()).round() as u64
            })
            .unwrap_or_default();

        PayloadBandwidthSnapshot {
            bytes_per_sec,
            total_payload_bytes: self.total_payload_bytes,
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some((event_at, payload_bytes)) = self.events.front().copied() {
            if now.saturating_duration_since(event_at) <= P2P_BANDWIDTH_RATE_WINDOW {
                break;
            }
            self.events.pop_front();
            self.window_payload_bytes = self.window_payload_bytes.saturating_sub(payload_bytes);
        }
    }
}

fn usize_to_u64(value: usize) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

fn raw_rpc_response_payload_bytes(payload: &RawRpcResponse) -> u64 {
    usize_to_u64(payload.context_bytes.map(|_| 4).unwrap_or_default())
        .saturating_add(usize_to_u64(payload.bytes.len()))
}

fn raw_rpc_response_payloads_bytes(payloads: &[RawRpcResponse]) -> u64 {
    payloads.iter().fold(0u64, |total, payload| {
        total.saturating_add(raw_rpc_response_payload_bytes(payload))
    })
}

fn consensus_request_payload_bytes(request: &Eth2RpcRequest) -> u64 {
    match request {
        Eth2RpcRequest::Status(_) => 92,
        Eth2RpcRequest::Goodbye(_) | Eth2RpcRequest::Ping(_) => 8,
        Eth2RpcRequest::MetaData
        | Eth2RpcRequest::LightClientFinalityUpdate
        | Eth2RpcRequest::LightClientOptimisticUpdate => 0,
        Eth2RpcRequest::LightClientBootstrap(_) => 32,
        Eth2RpcRequest::LightClientUpdatesByRange(_) => 16,
        Eth2RpcRequest::BeaconBlocksByRange(_) => 24,
        Eth2RpcRequest::BeaconBlocksByRoot(roots) => usize_to_u64(roots.len()).saturating_mul(32),
    }
}

fn consensus_response_payload_bytes(response: &Eth2RpcResponse) -> u64 {
    match response {
        Eth2RpcResponse::Status(_) => 92,
        Eth2RpcResponse::Goodbye(_) | Eth2RpcResponse::Ping(_) => 8,
        Eth2RpcResponse::MetaData(_) => 17,
        Eth2RpcResponse::LightClientBootstrap(payload)
        | Eth2RpcResponse::LightClientFinalityUpdate(payload)
        | Eth2RpcResponse::LightClientOptimisticUpdate(payload) => {
            raw_rpc_response_payload_bytes(payload)
        }
        Eth2RpcResponse::LightClientUpdatesByRange(payloads)
        | Eth2RpcResponse::BeaconBlocksByRange(payloads)
        | Eth2RpcResponse::BeaconBlocksByRoot(payloads) => {
            raw_rpc_response_payloads_bytes(payloads)
        }
        Eth2RpcResponse::Error(error) => usize_to_u64(error.message.len()),
    }
}

#[derive(Debug)]
struct DiscoveryQueryResult {
    target: NodeId,
    result: Result<Vec<Enr>, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RpcRequestKind {
    Status,
    Goodbye,
    MetaData,
    Ping,
    LightClientBootstrap,
    LightClientUpdatesByRange,
    LightClientFinalityUpdate,
    LightClientOptimisticUpdate,
    BeaconBlocksByRange,
    BeaconBlocksByRoot,
}

impl RpcRequestKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Goodbye => "goodbye",
            Self::MetaData => "metadata",
            Self::Ping => "ping",
            Self::LightClientBootstrap => "light_client_bootstrap",
            Self::LightClientUpdatesByRange => "light_client_updates_by_range",
            Self::LightClientFinalityUpdate => "light_client_finality_update",
            Self::LightClientOptimisticUpdate => "light_client_optimistic_update",
            Self::BeaconBlocksByRange => "beacon_blocks_by_range",
            Self::BeaconBlocksByRoot => "beacon_blocks_by_root",
        }
    }
}

fn inbound_request_cost(request: &Eth2RpcRequest) -> Result<u64, &'static str> {
    match request {
        Eth2RpcRequest::LightClientUpdatesByRange(request)
            if request.count > MAX_LIGHT_CLIENT_UPDATES_BY_RANGE_REQUEST =>
        {
            Err("light-client updates request count exceeds 128")
        }
        Eth2RpcRequest::BeaconBlocksByRange(request)
            if request.count > MAX_BEACON_BLOCKS_BY_RANGE_REQUEST =>
        {
            Err("beacon blocks by range request count exceeds 128")
        }
        Eth2RpcRequest::BeaconBlocksByRange(request) if request.step == 0 => {
            Err("beacon blocks by range request step must be non-zero")
        }
        Eth2RpcRequest::BeaconBlocksByRoot(roots)
            if roots.len() > MAX_BEACON_BLOCKS_BY_ROOT_REQUEST =>
        {
            Err("beacon blocks by root request count exceeds 128")
        }
        Eth2RpcRequest::BeaconBlocksByRange(request) => Ok(request.count.max(1)),
        Eth2RpcRequest::BeaconBlocksByRoot(roots) => {
            Ok(u64::try_from(roots.len()).unwrap_or(u64::MAX).max(1))
        }
        _ => Ok(1),
    }
}

fn inbound_rate_limit_quota(kind: RpcRequestKind) -> (u64, Duration) {
    match kind {
        RpcRequestKind::Status => (5, Duration::from_secs(15)),
        RpcRequestKind::Goodbye => (1, Duration::from_secs(10)),
        RpcRequestKind::MetaData => (2, Duration::from_secs(5)),
        RpcRequestKind::Ping => (2, Duration::from_secs(10)),
        RpcRequestKind::BeaconBlocksByRange | RpcRequestKind::BeaconBlocksByRoot => {
            (128, Duration::from_secs(10))
        }
        RpcRequestKind::LightClientBootstrap
        | RpcRequestKind::LightClientUpdatesByRange
        | RpcRequestKind::LightClientFinalityUpdate
        | RpcRequestKind::LightClientOptimisticUpdate => (1, Duration::from_secs(10)),
    }
}

fn consume_inbound_rate_limit(
    bucket: &mut InboundRateLimitBucket,
    now: Instant,
    cost: u64,
    quota: u64,
    window: Duration,
) -> bool {
    if now.saturating_duration_since(bucket.window_started_at) >= window {
        bucket.window_started_at = now;
        bucket.used = 0;
    }
    if bucket.used.saturating_add(cost) > quota {
        return false;
    }
    bucket.used = bucket.used.saturating_add(cost);
    true
}

fn status_irrelevance_reason(
    local: StatusMessage,
    remote: StatusMessage,
    current_slot: u64,
) -> Option<&'static str> {
    if remote.fork_digest != local.fork_digest {
        return Some("incompatible fork digest");
    }
    if remote.head_slot > current_slot.saturating_add(1) {
        return Some("peer head is more than one slot in the future");
    }
    if remote.finalized_epoch == local.finalized_epoch
        && remote.finalized_root != B256::ZERO
        && local.finalized_root != B256::ZERO
        && remote.finalized_root != local.finalized_root
    {
        return Some("conflicting finalized root at the local finalized epoch");
    }
    None
}

fn light_client_verification_error_is_peer_fault(error: &LightClientVerificationError) -> bool {
    !matches!(
        error,
        LightClientVerificationError::UnknownSyncCommitteePeriod { .. }
            | LightClientVerificationError::IrrelevantUpdate { .. }
            | LightClientVerificationError::SignatureFromFuture { .. }
    )
}

fn rpc_context_matches_slot(payload: &RawRpcResponse, slot: u64) -> bool {
    payload.context_bytes.is_some_and(|context| {
        context
            == MAINNET_CONSENSUS_CHAIN_SPEC
                .fork_digest_for_epoch(MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(slot))
    })
}

#[derive(Debug, Clone, Copy, Default)]
struct RpcFailureCounts {
    status: u64,
    goodbye: u64,
    metadata: u64,
    ping: u64,
    bootstrap: u64,
    updates_by_range: u64,
    finality_update: u64,
    optimistic_update: u64,
    beacon_blocks_by_range: u64,
    beacon_blocks_by_root: u64,
}

impl RpcFailureCounts {
    fn increment(&mut self, kind: RpcRequestKind) {
        match kind {
            RpcRequestKind::Status => self.status += 1,
            RpcRequestKind::Goodbye => self.goodbye += 1,
            RpcRequestKind::MetaData => self.metadata += 1,
            RpcRequestKind::Ping => self.ping += 1,
            RpcRequestKind::LightClientBootstrap => self.bootstrap += 1,
            RpcRequestKind::LightClientUpdatesByRange => self.updates_by_range += 1,
            RpcRequestKind::LightClientFinalityUpdate => self.finality_update += 1,
            RpcRequestKind::LightClientOptimisticUpdate => self.optimistic_update += 1,
            RpcRequestKind::BeaconBlocksByRange => self.beacon_blocks_by_range += 1,
            RpcRequestKind::BeaconBlocksByRoot => self.beacon_blocks_by_root += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerFailureCounts {
    status: u32,
    goodbye: u32,
    metadata: u32,
    ping: u32,
    bootstrap: u32,
    updates_by_range: u32,
    finality_update: u32,
    optimistic_update: u32,
    beacon_blocks_by_range: u32,
    beacon_blocks_by_root: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum DialAddressClass {
    Tcp4,
    Quic4,
    Tcp6,
    Quic6,
}

impl DialAddressClass {
    const fn label(self) -> &'static str {
        match self {
            Self::Tcp4 => "tcp4",
            Self::Quic4 => "quic4",
            Self::Tcp6 => "tcp6",
            Self::Quic6 => "quic6",
        }
    }

    const fn default_priority(self, bootstrap_needed: bool) -> i32 {
        match (bootstrap_needed, self) {
            (true, Self::Tcp4) => 400,
            (true, Self::Quic4) => 350,
            (true, Self::Tcp6) => 200,
            (true, Self::Quic6) => 150,
            (false, Self::Quic4) => 400,
            (false, Self::Tcp4) => 350,
            (false, Self::Quic6) => 200,
            (false, Self::Tcp6) => 150,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
struct PeerDialAddressStats {
    tcp4_successes: u32,
    quic4_successes: u32,
    tcp6_successes: u32,
    quic6_successes: u32,
    tcp4_failures: u32,
    quic4_failures: u32,
    tcp6_failures: u32,
    quic6_failures: u32,
}

impl PeerDialAddressStats {
    fn record_success(&mut self, class: DialAddressClass) {
        let successes = self.successes_mut(class);
        *successes = (*successes).saturating_add(1);
        *self.failures_mut(class) = 0;
    }

    fn record_failure(&mut self, class: DialAddressClass) {
        let failures = self.failures_mut(class);
        *failures = (*failures).saturating_add(1);
    }

    fn priority(self, class: DialAddressClass, bootstrap_needed: bool) -> i32 {
        class.default_priority(bootstrap_needed) + self.successes(class) as i32 * 100
            - self.failures(class) as i32 * 125
    }

    const fn successes(self, class: DialAddressClass) -> u32 {
        match class {
            DialAddressClass::Tcp4 => self.tcp4_successes,
            DialAddressClass::Quic4 => self.quic4_successes,
            DialAddressClass::Tcp6 => self.tcp6_successes,
            DialAddressClass::Quic6 => self.quic6_successes,
        }
    }

    const fn failures(self, class: DialAddressClass) -> u32 {
        match class {
            DialAddressClass::Tcp4 => self.tcp4_failures,
            DialAddressClass::Quic4 => self.quic4_failures,
            DialAddressClass::Tcp6 => self.tcp6_failures,
            DialAddressClass::Quic6 => self.quic6_failures,
        }
    }

    fn successes_mut(&mut self, class: DialAddressClass) -> &mut u32 {
        match class {
            DialAddressClass::Tcp4 => &mut self.tcp4_successes,
            DialAddressClass::Quic4 => &mut self.quic4_successes,
            DialAddressClass::Tcp6 => &mut self.tcp6_successes,
            DialAddressClass::Quic6 => &mut self.quic6_successes,
        }
    }

    fn failures_mut(&mut self, class: DialAddressClass) -> &mut u32 {
        match class {
            DialAddressClass::Tcp4 => &mut self.tcp4_failures,
            DialAddressClass::Quic4 => &mut self.quic4_failures,
            DialAddressClass::Tcp6 => &mut self.tcp6_failures,
            DialAddressClass::Quic6 => &mut self.quic6_failures,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct PeerLifecycleState {
    remembered_support: Option<PeerRpcSupport>,
    status_successes: u32,
    bootstrap_successes: u32,
    useful_successes: u32,
    dial_stats: PeerDialAddressStats,
    transport_failures: u32,
    rpc_failures: u32,
    disconnects: u32,
    cooldown_until: Option<Instant>,
    ignored_for_run: bool,
    deferred_until_post_bootstrap: bool,
}

impl PeerLifecycleState {
    fn from_persisted(peer: &PersistedPeer) -> Self {
        Self {
            remembered_support: peer.support,
            status_successes: peer.status_successes,
            bootstrap_successes: peer.bootstrap_successes,
            useful_successes: peer.useful_successes,
            dial_stats: peer.dial_stats,
            transport_failures: 0,
            rpc_failures: 0,
            disconnects: 0,
            cooldown_until: None,
            ignored_for_run: false,
            deferred_until_post_bootstrap: false,
        }
    }

    fn persisted_support(&self) -> Option<PeerRpcSupport> {
        self.remembered_support
    }

    fn remember_support(&mut self, support: PeerRpcSupport) {
        self.remembered_support = Some(support);
        if support.status {
            self.ignored_for_run = false;
        }
        if support.supports_bootstrap_sync() {
            self.deferred_until_post_bootstrap = false;
        }
    }

    fn record_success(&mut self, kind: RpcRequestKind) {
        let useful_rpc = matches!(
            kind,
            RpcRequestKind::LightClientBootstrap
                | RpcRequestKind::LightClientUpdatesByRange
                | RpcRequestKind::LightClientFinalityUpdate
                | RpcRequestKind::LightClientOptimisticUpdate
                | RpcRequestKind::BeaconBlocksByRange
                | RpcRequestKind::BeaconBlocksByRoot
        );
        match kind {
            RpcRequestKind::Status => self.status_successes += 1,
            RpcRequestKind::LightClientBootstrap => {
                self.bootstrap_successes += 1;
                self.useful_successes += 1;
            }
            RpcRequestKind::LightClientUpdatesByRange
            | RpcRequestKind::LightClientFinalityUpdate
            | RpcRequestKind::LightClientOptimisticUpdate
            | RpcRequestKind::BeaconBlocksByRange
            | RpcRequestKind::BeaconBlocksByRoot => {
                self.useful_successes += 1;
            }
            RpcRequestKind::Goodbye | RpcRequestKind::MetaData | RpcRequestKind::Ping => {}
        }
        // Backoff tracks consecutive failures. A verified RPC response proves that the
        // connection is healthy, so old transport and disconnect streaks must not poison a
        // useful peer for the rest of a long-running process.
        self.transport_failures = 0;
        self.disconnects = 0;
        if useful_rpc {
            self.rpc_failures = 0;
        }
        if self.rpc_failures == 0 {
            self.cooldown_until = None;
        }
    }

    fn record_dial_success(&mut self, class: DialAddressClass) {
        self.dial_stats.record_success(class);
        self.transport_failures = self.transport_failures.saturating_sub(1);
        self.cooldown_until = None;
    }

    fn record_dial_failure(&mut self, class: DialAddressClass) {
        self.dial_stats.record_failure(class);
    }

    fn record_transport_failure(&mut self, now: Instant) -> Duration {
        self.transport_failures = self.transport_failures.saturating_add(1);
        let delay = peer_backoff_delay(self.transport_failures);
        self.cooldown_until = Some(now + delay);
        delay
    }

    fn record_rpc_failure(&mut self, kind: RpcRequestKind, now: Instant) -> Option<Duration> {
        match kind {
            RpcRequestKind::LightClientBootstrap
            | RpcRequestKind::LightClientUpdatesByRange
            | RpcRequestKind::LightClientFinalityUpdate
            | RpcRequestKind::LightClientOptimisticUpdate
            | RpcRequestKind::BeaconBlocksByRange
            | RpcRequestKind::BeaconBlocksByRoot => {
                self.rpc_failures = self.rpc_failures.saturating_add(1);
                let delay = peer_backoff_delay(self.rpc_failures);
                self.cooldown_until = Some(now + delay);
                Some(delay)
            }
            RpcRequestKind::Status
            | RpcRequestKind::Goodbye
            | RpcRequestKind::MetaData
            | RpcRequestKind::Ping => None,
        }
    }

    fn record_disconnect(&mut self, now: Instant) -> Duration {
        self.disconnects = self.disconnects.saturating_add(1);
        let attempts = self.transport_failures.max(self.disconnects);
        let delay = peer_backoff_delay(attempts);
        self.cooldown_until = Some(now + delay);
        delay
    }

    fn in_cooldown(&self, now: Instant) -> bool {
        self.cooldown_until.is_some_and(|until| until > now)
    }

    fn clear_expired_cooldown(&mut self, now: Instant) {
        if self.cooldown_until.is_some_and(|until| until <= now) {
            self.cooldown_until = None;
        }
    }

    fn mark_ignored_for_run(&mut self) {
        self.ignored_for_run = true;
        self.deferred_until_post_bootstrap = false;
        self.cooldown_until = None;
    }

    fn mark_deferred_until_post_bootstrap(&mut self) {
        self.deferred_until_post_bootstrap = true;
    }

    fn clear_bootstrap_deferral(&mut self) {
        self.deferred_until_post_bootstrap = false;
    }

    fn preferred(&self) -> bool {
        self.bootstrap_successes > 0 || self.useful_successes > 0 || self.status_successes > 0
    }
}

fn record_dial_error_on_lifecycle(lifecycle: &mut PeerLifecycleState, error: &DialError) -> bool {
    match error {
        DialError::LocalPeerId { address } | DialError::WrongPeerId { address, .. } => {
            if let Some(class) = dial_address_class(address) {
                lifecycle.record_dial_failure(class);
            }
            lifecycle.mark_ignored_for_run();
            true
        }
        DialError::Transport(errors) => {
            for (address, _) in errors {
                if let Some(class) = dial_address_class(address) {
                    lifecycle.record_dial_failure(class);
                }
            }
            false
        }
        DialError::NoAddresses
        | DialError::DialPeerConditionFalse(_)
        | DialError::Aborted
        | DialError::Denied { .. } => false,
    }
}

impl PeerFailureCounts {
    fn increment(&mut self, kind: RpcRequestKind) -> u32 {
        match kind {
            RpcRequestKind::Status => {
                self.status += 1;
                self.status
            }
            RpcRequestKind::Goodbye => {
                self.goodbye += 1;
                self.goodbye
            }
            RpcRequestKind::MetaData => {
                self.metadata += 1;
                self.metadata
            }
            RpcRequestKind::Ping => {
                self.ping += 1;
                self.ping
            }
            RpcRequestKind::LightClientBootstrap => {
                self.bootstrap += 1;
                self.bootstrap
            }
            RpcRequestKind::LightClientUpdatesByRange => {
                self.updates_by_range += 1;
                self.updates_by_range
            }
            RpcRequestKind::LightClientFinalityUpdate => {
                self.finality_update += 1;
                self.finality_update
            }
            RpcRequestKind::LightClientOptimisticUpdate => {
                self.optimistic_update += 1;
                self.optimistic_update
            }
            RpcRequestKind::BeaconBlocksByRange => {
                self.beacon_blocks_by_range += 1;
                self.beacon_blocks_by_range
            }
            RpcRequestKind::BeaconBlocksByRoot => {
                self.beacon_blocks_by_root += 1;
                self.beacon_blocks_by_root
            }
        }
    }

    fn reset(&mut self, kind: RpcRequestKind) {
        match kind {
            RpcRequestKind::Status => self.status = 0,
            RpcRequestKind::Goodbye => self.goodbye = 0,
            RpcRequestKind::MetaData => self.metadata = 0,
            RpcRequestKind::Ping => self.ping = 0,
            RpcRequestKind::LightClientBootstrap => self.bootstrap = 0,
            RpcRequestKind::LightClientUpdatesByRange => self.updates_by_range = 0,
            RpcRequestKind::LightClientFinalityUpdate => self.finality_update = 0,
            RpcRequestKind::LightClientOptimisticUpdate => self.optimistic_update = 0,
            RpcRequestKind::BeaconBlocksByRange => self.beacon_blocks_by_range = 0,
            RpcRequestKind::BeaconBlocksByRoot => self.beacon_blocks_by_root = 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
struct PeerRpcSupport {
    status: bool,
    goodbye: bool,
    metadata: bool,
    ping: bool,
    light_client_bootstrap: bool,
    light_client_updates_by_range: bool,
    light_client_finality_update: bool,
    light_client_optimistic_update: bool,
    beacon_blocks_by_range: bool,
    beacon_blocks_by_root: bool,
}

impl PeerRpcSupport {
    fn from_identify_info(info: &identify::Info) -> Self {
        let mut support = Self::default();
        for protocol in &info.protocols {
            match protocol.as_ref() {
                STATUS_V1_PROTOCOL_ID | STATUS_V2_PROTOCOL_ID => support.status = true,
                GOODBYE_V1_PROTOCOL_ID => support.goodbye = true,
                METADATA_V1_PROTOCOL_ID | METADATA_V2_PROTOCOL_ID | METADATA_V3_PROTOCOL_ID => {
                    support.metadata = true;
                }
                PING_PROTOCOL_ID => support.ping = true,
                LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID => support.light_client_bootstrap = true,
                LIGHT_CLIENT_UPDATES_BY_RANGE_PROTOCOL_ID => {
                    support.light_client_updates_by_range = true;
                }
                LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID => {
                    support.light_client_finality_update = true;
                }
                LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID => {
                    support.light_client_optimistic_update = true;
                }
                BEACON_BLOCKS_BY_RANGE_V1_PROTOCOL_ID | BEACON_BLOCKS_BY_RANGE_V2_PROTOCOL_ID => {
                    support.beacon_blocks_by_range = true;
                }
                BEACON_BLOCKS_BY_ROOT_V1_PROTOCOL_ID | BEACON_BLOCKS_BY_ROOT_V2_PROTOCOL_ID => {
                    support.beacon_blocks_by_root = true;
                }
                _ => {}
            }
        }
        support
    }

    const fn supports_request(self, kind: RpcRequestKind) -> bool {
        match kind {
            RpcRequestKind::Status => self.status,
            RpcRequestKind::Goodbye => self.goodbye,
            RpcRequestKind::MetaData => self.metadata,
            RpcRequestKind::Ping => self.ping,
            RpcRequestKind::LightClientBootstrap => self.light_client_bootstrap,
            RpcRequestKind::LightClientUpdatesByRange => self.light_client_updates_by_range,
            RpcRequestKind::LightClientFinalityUpdate => self.light_client_finality_update,
            RpcRequestKind::LightClientOptimisticUpdate => self.light_client_optimistic_update,
            RpcRequestKind::BeaconBlocksByRange => self.beacon_blocks_by_range,
            RpcRequestKind::BeaconBlocksByRoot => self.beacon_blocks_by_root,
        }
    }

    const fn supports_any_light_client(self) -> bool {
        self.light_client_bootstrap
            || self.light_client_updates_by_range
            || self.light_client_finality_update
            || self.light_client_optimistic_update
    }

    const fn supports_history_backfill(self) -> bool {
        self.beacon_blocks_by_range || self.beacon_blocks_by_root
    }

    const fn supports_bootstrap_sync(self) -> bool {
        self.status && self.light_client_bootstrap
    }

    const fn supports_light_client_progression(self) -> bool {
        self.light_client_updates_by_range
            || self.light_client_finality_update
            || self.light_client_optimistic_update
    }

    const fn supports_any_post_bootstrap_work(self) -> bool {
        self.supports_light_client_progression() || self.supports_history_backfill()
    }
}

impl ConsensusNetwork {
    fn new(
        config: ConsensusNetworkConfig,
        consensus: Arc<ConsensusStore>,
        sync_status: Arc<Mutex<SyncStatus>>,
    ) -> Result<Self, ConsensusNetworkError> {
        let bootnodes = mainnet_bootnodes()?;
        let local_epoch = config
            .checkpoint
            .beacon_slot
            .map(|slot| MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(slot))
            .map(|checkpoint_epoch| {
                checkpoint_epoch.max(MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_epoch())
            })
            .unwrap_or_else(|| MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_epoch());
        let fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(local_epoch);
        let fork_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(local_epoch);
        let next_fork_digest = MAINNET_CONSENSUS_CHAIN_SPEC.next_fork_digest_for_epoch(local_epoch);
        let gossip_topics = build_gossip_topics(fork_digest);
        let secret_path = discovery_secret_path(&config.data_dir);
        let known_peers_path = known_peers_path(&config.data_dir);
        let enr_key = load_or_create_secret_key(&secret_path)?;
        let local_enr = build_local_enr(
            &enr_key,
            LocalEnrConfig {
                fork_id: &fork_id,
                next_fork_digest: &next_fork_digest,
                custody_group_count: LOCAL_CUSTODY_GROUP_COUNT,
                bind_ip: config.bind_ip,
                external_ip: config.external_ip,
                discovery_port: config.discovery_port,
                p2p_port: config.p2p_port,
            },
        );
        let local_keypair = build_libp2p_keypair(&enr_key)?;
        let listen_config = ListenConfig::from_ip(config.bind_ip, config.discovery_port);
        let discovery_config = ConfigBuilder::new(listen_config)
            .enable_packet_filter()
            .build();
        let discv5 = Discv5::new(local_enr, enr_key, discovery_config)
            .map_err(|error| ConsensusNetworkError::ConstructDiscovery(error.to_string()))?;

        let known_peers = load_known_peers(&known_peers_path)?;
        let mut retained_known_peers = Vec::new();
        let mut dialable_peers = HashMap::new();
        let mut peer_lifecycle = HashMap::new();
        let mut bootnode_peers = HashSet::new();

        let mut seeded_bootnodes = 0usize;
        let mut skipped_bootnodes = 0usize;
        for enr in &bootnodes {
            if enr_has_discv5_endpoint_for_families(config.dial_families, enr) {
                if let Err(error) = discv5.add_enr(enr.clone()) {
                    tracing::warn!(%error, enr = %enr, "failed to seed consensus bootnode");
                } else {
                    seeded_bootnodes = seeded_bootnodes.saturating_add(1);
                }
            } else {
                skipped_bootnodes = skipped_bootnodes.saturating_add(1);
            }
            if let Some(peer_id) =
                observe_dialable_peer_for_families(&mut dialable_peers, config.dial_families, enr)
            {
                bootnode_peers.insert(peer_id);
            }
        }
        if skipped_bootnodes > 0 {
            tracing::debug!(
                seeded_bootnodes,
                skipped_bootnodes,
                ?config.dial_families,
                "skipped consensus bootnodes without compatible discovery endpoints"
            );
        }

        for peer in &known_peers {
            if peer.support.is_some_and(|support| !support.status) {
                tracing::debug!(
                    enr = %peer.enr,
                    "ignoring cached consensus peer that previously identified without beacon status support"
                );
                continue;
            }
            match peer.enr.parse::<Enr>() {
                Ok(enr) => {
                    if !enr_is_relevant_consensus_peer(&enr, &fork_digest) {
                        tracing::debug!(
                            enr = %peer.enr,
                            "ignoring cached consensus peer ENR that is not relevant to the expected beacon network"
                        );
                        continue;
                    }
                    let Some(peer_id) = observe_dialable_peer_for_families(
                        &mut dialable_peers,
                        config.dial_families,
                        &enr,
                    ) else {
                        tracing::debug!(
                            enr = %peer.enr,
                            ?config.dial_families,
                            "ignoring cached consensus peer without a dialable address for configured families"
                        );
                        continue;
                    };
                    if enr_has_discv5_endpoint_for_families(config.dial_families, &enr)
                        && let Err(error) = discv5.add_enr(enr.clone())
                    {
                        tracing::debug!(
                            %error,
                            enr = %peer.enr,
                            "skipping cached consensus peer that could not be inserted"
                        );
                    }
                    peer_lifecycle.insert(peer_id, PeerLifecycleState::from_persisted(peer));
                    retained_known_peers.push(peer.clone());
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        enr = %peer.enr,
                        "ignoring invalid cached consensus peer ENR"
                    );
                }
            }
        }

        let swarm = build_rpc_swarm(local_keypair, config.max_peers)?;
        let mut verified_beacon_blocks =
            verified_beacon_blocks_from_anchor_records(&consensus.ordered_anchors());
        if let Some(store) = consensus.light_client_store() {
            for block in verified_beacon_blocks_from_light_client_store(&store) {
                verified_beacon_blocks.insert(block.beacon_root, block);
            }
        }
        let verified_beacon_block_children =
            verified_beacon_block_children_from_blocks(&verified_beacon_blocks);

        Ok(Self {
            config,
            consensus,
            sync_status,
            discv5,
            swarm,
            bootnode_count: seeded_bootnodes,
            bootnode_peers,
            fork_digest,
            known_peers_path,
            last_persisted: retained_known_peers,
            observed: HashSet::new(),
            dialable_peers,
            dialing_peers: HashSet::new(),
            connected_peers: HashSet::new(),
            closing_peers: HashSet::new(),
            connected_since: HashMap::new(),
            peer_endpoints: HashMap::new(),
            peer_lifecycle,
            peer_support: HashMap::new(),
            peer_failures: HashMap::new(),
            inbound_status_peers: HashSet::new(),
            status_peers: HashSet::new(),
            metadata_peers: HashSet::new(),
            ping_peers: HashSet::new(),
            last_status_success_at: HashMap::new(),
            last_ping_success_at: HashMap::new(),
            bootstrap_peers: HashSet::new(),
            updates_by_range_peers: HashSet::new(),
            finality_update_peers: HashSet::new(),
            optimistic_update_peers: HashSet::new(),
            beacon_blocks_by_range_peers: HashSet::new(),
            beacon_blocks_by_root_peers: HashSet::new(),
            pending_requests: HashMap::new(),
            pending_history_root_requests: HashMap::new(),
            pending_history_range_requests: HashMap::new(),
            pending_peer_kinds: HashSet::new(),
            inbound_rate_limits: HashMap::new(),
            last_light_client_request_at: HashMap::new(),
            request_failures: RpcFailureCounts::default(),
            gossip_topics,
            pre_subscribed_fork_digest: None,
            retiring_gossip_topics: Vec::new(),
            gossip_counts: GossipMessageCounts::default(),
            p2p_download_metrics: PayloadBandwidthWindow::default(),
            p2p_upload_metrics: PayloadBandwidthWindow::default(),
            gossip_subscriptions: HashSet::new(),
            last_connection_event: None,
            last_identify_event: None,
            last_peer_policy_event: None,
            last_rpc_failure: None,
            last_response_send_failure: None,
            verified_beacon_blocks,
            verified_beacon_block_children,
            verified_beacon_block_payloads: HashMap::new(),
            active_history_target: None,
            head_recovery_attempts: 0,
        })
    }

    async fn run(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ConsensusNetworkError> {
        self.discv5
            .start()
            .await
            .map_err(|error| ConsensusNetworkError::StartDiscovery(error.to_string()))?;
        let tcp_listen_addr =
            multiaddr_bind_ip(self.config.bind_ip).with(Protocol::Tcp(self.config.p2p_port));
        self.swarm
            .listen_on(tcp_listen_addr)
            .map_err(|error| ConsensusNetworkError::ListenRpcTransport(error.to_string()))?;
        if self.config.discovery_port != self.config.p2p_port {
            let quic_listen_addr = multiaddr_bind_ip(self.config.bind_ip)
                .with(Protocol::Udp(self.config.p2p_port))
                .with(Protocol::QuicV1);
            self.swarm
                .listen_on(quic_listen_addr)
                .map_err(|error| ConsensusNetworkError::ListenRpcTransport(error.to_string()))?;
        } else {
            tracing::info!(
                discovery_port = self.config.discovery_port,
                p2p_port = self.config.p2p_port,
                "skipping inbound QUIC listener because consensus discovery and p2p share the same UDP port"
            );
        }
        let mut event_stream = self
            .discv5
            .event_stream()
            .await
            .map_err(|error| ConsensusNetworkError::EventStream(error.to_string()))?;

        let local_enr = self.discv5.local_enr();
        let external_ip = self
            .config
            .external_ip
            .map(|ip| ip.to_string())
            .unwrap_or_else(|| "unresolved".to_owned());
        tracing::info!(
            local_enr = %local_enr.to_base64(),
            node_id = %local_enr.node_id(),
            bind_ip = %self.config.bind_ip,
            %external_ip,
            discovery_port = self.config.discovery_port,
            p2p_port = self.config.p2p_port,
            local_peer_id = %self.swarm.local_peer_id(),
            bootnodes = self.bootnode_count,
            "consensus network started"
        );
        self.subscribe_gossip_topics();
        self.refresh_status();

        let mut query_interval = tokio::time::interval(DISCOVERY_QUERY_INTERVAL);
        query_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut persist_interval = tokio::time::interval(KNOWN_PEER_PERSIST_INTERVAL);
        persist_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut rpc_interval = tokio::time::interval(RPC_REQUEST_INTERVAL);
        rpc_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let (discovery_query_tx, mut discovery_query_rx) =
            mpsc::unbounded_channel::<DiscoveryQueryResult>();
        let mut pending_discovery_queries = 0usize;

        loop {
            tokio::select! {
                _ = wait_for_shutdown(&mut shutdown) => {
                    tracing::info!("consensus network shutting down");
                    break;
                }
                _ = query_interval.tick() => {
                    self.launch_discovery_queries(&discovery_query_tx, &mut pending_discovery_queries);
                    self.refresh_dialable_peers_from_routing_table();
                    self.prune_inactive_peer_state();
                    self.refresh_status();
                }
                _ = rpc_interval.tick() => {
                    self.maintain_fork_subscriptions();
                    self.maybe_force_light_client_store();
                    self.drive_peer_connections();
                    self.drive_rpc_requests();
                    self.refresh_status();
                }
                _ = persist_interval.tick() => {
                    if let Err(error) = self.persist_known_peers() {
                        tracing::warn!(%error, "failed to persist consensus peer cache");
                    }
                    self.refresh_status();
                }
                result = discovery_query_rx.recv() => {
                    let Some(result) = result else {
                        tracing::warn!("consensus discovery query channel ended unexpectedly");
                        break;
                    };
                    pending_discovery_queries = pending_discovery_queries.saturating_sub(1);
                    self.handle_discovery_query_result(result);
                    self.refresh_dialable_peers_from_routing_table();
                    self.prune_inactive_peer_state();
                    self.refresh_status();
                }
                event = event_stream.recv() => {
                    match event {
                        Some(event) => {
                            self.handle_event(event);
                            self.refresh_status();
                        }
                        None => {
                            tracing::warn!("consensus discovery event stream ended unexpectedly");
                            break;
                        }
                    }
                }
                event = self.swarm.select_next_some() => {
                    self.handle_swarm_event(event);
                    self.refresh_status();
                }
            }
        }

        if let Err(error) = self.persist_known_peers() {
            tracing::warn!(%error, "failed to persist consensus peer cache during shutdown");
        }
        self.discv5.shutdown();
        self.refresh_status();
        Ok(())
    }

    fn launch_discovery_queries(
        &self,
        discovery_query_tx: &mpsc::UnboundedSender<DiscoveryQueryResult>,
        pending_discovery_queries: &mut usize,
    ) {
        if !self.discovery_query_needed() {
            return;
        }
        while *pending_discovery_queries < DISCOVERY_QUERY_FANOUT {
            let target = NodeId::random();
            let tx = discovery_query_tx.clone();
            let query = self.discv5.find_node(target);
            *pending_discovery_queries += 1;
            tokio::spawn(async move {
                let result = query.await.map_err(|error| error.to_string());
                let _ = tx.send(DiscoveryQueryResult { target, result });
            });
        }
    }

    fn discovery_query_needed(&self) -> bool {
        let now = Instant::now();
        let eligible_inventory = self
            .dialable_peers
            .iter()
            .filter(|(peer, addrs)| {
                !addrs.is_empty()
                    && self.peer_lifecycle.get(peer).is_none_or(|lifecycle| {
                        !lifecycle.ignored_for_run && !lifecycle.in_cooldown(now)
                    })
            })
            .count();
        let target = self
            .config
            .max_peers
            .saturating_mul(3)
            .max(MIN_DISCOVERY_PEER_RESERVE);
        eligible_inventory < target
    }

    fn handle_discovery_query_result(&mut self, result: DiscoveryQueryResult) {
        match result.result {
            Ok(found) => {
                tracing::debug!(
                    target = %result.target,
                    discovered = found.len(),
                    "consensus discovery query completed"
                );
                for enr in found {
                    self.observe_enr(&enr);
                }
            }
            Err(error) => {
                tracing::debug!(target = %result.target, %error, "consensus discovery query failed");
            }
        }
    }

    fn refresh_dialable_peers_from_routing_table(&mut self) {
        for enr in self.discv5.table_entries_enr() {
            self.observe_enr(&enr);
        }
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Discovered(enr) => self.observe_enr(&enr),
            Event::SessionEstablished(enr, socket) => {
                tracing::debug!(%socket, node_id = %enr.node_id(), "consensus discovery session established");
                self.observe_enr(&enr);
            }
            Event::NodeInserted { node_id, replaced } => {
                tracing::debug!(%node_id, replaced = replaced.map(|id| id.to_string()), "consensus discovery routing table updated");
            }
            Event::UnverifiableEnr {
                enr,
                socket,
                node_id,
            } => {
                tracing::debug!(%socket, %node_id, enr = %enr.to_base64(), "consensus discovery received unverifiable ENR");
            }
            Event::SocketUpdated(socket) => {
                tracing::info!(%socket, "consensus discovery updated its observed socket");
            }
            Event::SessionsExpired(expired) => {
                tracing::debug!(
                    count = expired.len(),
                    "consensus discovery sessions expired"
                );
            }
            Event::TalkRequest(_) => {
                tracing::debug!("consensus discovery received an unsupported TALKREQ");
            }
            _ => {}
        }
    }

    fn observe_enr(&mut self, enr: &Enr) {
        if !enr_is_relevant_consensus_peer(enr, &self.fork_digest) {
            let remote_fork_digest = enr_fork_digest(enr).map(hex::encode);
            tracing::debug!(
                node_id = %enr.node_id(),
                local_fork_digest = %hex::encode(self.fork_digest),
                remote_fork_digest = remote_fork_digest.as_deref().unwrap_or("missing"),
                "ignoring discovered ENR that is not relevant to the expected beacon network"
            );
            return;
        }
        if let Some(peer) = observe_dialable_peer(&mut self.dialable_peers, enr) {
            self.observed.insert(peer);
        }
    }

    fn prune_inactive_peer_state(&mut self) {
        let inactive_count = self
            .dialable_peers
            .keys()
            .filter(|peer| {
                !self.connected_peers.contains(peer)
                    && !self.dialing_peers.contains(peer)
                    && !self.closing_peers.contains(peer)
            })
            .count();
        let excess = inactive_count.saturating_sub(MAX_RETAINED_DISCONNECTED_PEERS);
        if excess > 0 {
            let mut candidates = self
                .dialable_peers
                .keys()
                .copied()
                .filter(|peer| {
                    !self.connected_peers.contains(peer)
                        && !self.dialing_peers.contains(peer)
                        && !self.closing_peers.contains(peer)
                        && !self.bootnode_peers.contains(peer)
                        && self.pending_requests_for_peer(*peer) == 0
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|peer| {
                let lifecycle = self.peer_lifecycle.get(peer);
                (
                    lifecycle.is_some_and(PeerLifecycleState::preferred),
                    lifecycle.map_or(0, |state| state.useful_successes),
                    lifecycle.map_or(0, |state| state.status_successes),
                    *peer,
                )
            });
            for peer in candidates.into_iter().take(excess) {
                self.dialable_peers.remove(&peer);
                self.observed.remove(&peer);
                self.peer_lifecycle.remove(&peer);
                self.clear_peer_state(peer);
                self.inbound_rate_limits
                    .retain(|(bucket_peer, _), _| *bucket_peer != peer);
            }
        }

        let now = Instant::now();
        self.inbound_rate_limits.retain(|_, bucket| {
            now.saturating_duration_since(bucket.window_started_at) <= INBOUND_RATE_LIMIT_RETENTION
        });
    }

    fn handle_swarm_event(&mut self, event: SwarmEvent<ConsensusBehaviourEvent>) {
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                tracing::info!(%address, "consensus libp2p listening");
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                tracing::debug!(%peer_id, endpoint = ?endpoint, "consensus libp2p connection established");
                let dial_class = match &endpoint {
                    ConnectedPoint::Dialer { address, .. } => dial_address_class(address),
                    ConnectedPoint::Listener { .. } => None,
                };
                if let Some(class) = dial_class {
                    self.peer_lifecycle
                        .entry(peer_id)
                        .or_default()
                        .record_dial_success(class);
                }
                let endpoint = format!("{endpoint:?}");
                self.last_connection_event = Some(match dial_class {
                    Some(class) => {
                        format!(
                            "connected peer={peer_id} endpoint={endpoint} dial_class={}",
                            class.label()
                        )
                    }
                    None => format!("connected peer={peer_id} endpoint={endpoint}"),
                });
                self.dialing_peers.remove(&peer_id);
                self.closing_peers.remove(&peer_id);
                self.connected_peers.insert(peer_id);
                self.connected_since
                    .entry(peer_id)
                    .or_insert_with(Instant::now);
                self.peer_endpoints.insert(peer_id, endpoint);
                self.drive_rpc_requests();
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                tracing::debug!(%peer_id, cause = ?cause, "consensus libp2p connection closed");
                self.last_connection_event = Some(format!("closed peer={peer_id} cause={cause:?}"));
                self.dialing_peers.remove(&peer_id);
                self.connected_peers.remove(&peer_id);
                let planned = self.closing_peers.remove(&peer_id);
                if !planned {
                    self.record_peer_disconnect(
                        peer_id,
                        format!("connection_closed cause={cause:?}"),
                    );
                }
                self.clear_peer_state(peer_id);
                self.clear_pending_requests_for_peer(peer_id);
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                tracing::debug!(peer_id = peer_id.map(|peer| peer.to_string()), %error, "consensus libp2p dial failed");
                self.last_connection_event = Some(format!(
                    "dial_error peer={} error={error}",
                    peer_id
                        .map(|peer| peer.to_string())
                        .unwrap_or_else(|| "unknown".to_owned())
                ));
                if let Some(peer_id) = peer_id {
                    self.dialing_peers.remove(&peer_id);
                    self.clear_pending_requests_for_peer(peer_id);
                    let ignored_for_run = self.record_dial_error(peer_id, &error);
                    if !ignored_for_run {
                        self.record_transport_backoff(peer_id, format!("dial_error error={error}"));
                    }
                }
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::StatusRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::Status, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::GoodbyeRpc(event)) => {
                self.handle_goodbye_rpc_event(event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::MetadataRpc(event)) => {
                self.handle_metadata_rpc_event(event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::PingRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::Ping, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::LightClientBootstrapRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::LightClientBootstrap, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::LightClientUpdatesByRangeRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::LightClientUpdatesByRange, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::LightClientFinalityUpdateRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::LightClientFinalityUpdate, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::LightClientOptimisticUpdateRpc(
                event,
            )) => {
                self.handle_rpc_event(RpcRequestKind::LightClientOptimisticUpdate, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::BeaconBlocksByRangeRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::BeaconBlocksByRange, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::BeaconBlocksByRootRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::BeaconBlocksByRoot, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::Identify(event)) => {
                self.handle_identify_event(*event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::Gossip(event)) => {
                self.handle_gossip_event(*event);
            }
            _ => {}
        }
    }

    fn handle_identify_event(&mut self, event: identify::Event) {
        match event {
            identify::Event::Received { peer_id, info, .. } => {
                let support = PeerRpcSupport::from_identify_info(&info);
                let advertised_protocols = info
                    .protocols
                    .iter()
                    .take(6)
                    .map(|protocol| protocol.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                tracing::debug!(
                    %peer_id,
                    protocol_version = %info.protocol_version,
                    agent_version = %info.agent_version,
                    listen_addrs = info.listen_addrs.len(),
                    protocols = info.protocols.len(),
                    supports_light_client_progression = support.supports_light_client_progression(),
                    supports_any_light_client = support.supports_any_light_client(),
                    "received consensus identify info"
                );
                self.last_identify_event = Some(format!(
                    "peer={peer_id} agent={} protocols={} status={} metadata={} bootstrap={} updates_by_range={} finality={} optimistic={} blocks_by_range={} blocks_by_root={} preview=[{}]",
                    info.agent_version,
                    info.protocols.len(),
                    support.status,
                    support.metadata,
                    support.light_client_bootstrap,
                    support.light_client_updates_by_range,
                    support.light_client_finality_update,
                    support.light_client_optimistic_update,
                    support.beacon_blocks_by_range,
                    support.beacon_blocks_by_root,
                    advertised_protocols
                ));
                self.peer_lifecycle
                    .entry(peer_id)
                    .or_default()
                    .remember_support(support);
                self.peer_support.insert(peer_id, support);
                self.drive_rpc_requests();
            }
            identify::Event::Sent { peer_id, .. } => {
                tracing::debug!(%peer_id, "sent consensus identify info");
            }
            identify::Event::Pushed { peer_id, .. } => {
                tracing::debug!(%peer_id, "pushed consensus identify info");
            }
            identify::Event::Error { peer_id, error, .. } => {
                tracing::debug!(%peer_id, %error, "consensus identify exchange failed");
            }
        }
    }

    fn subscribe_gossip_topics(&mut self) {
        self.subscribe_gossip_topic_set(&self.gossip_topics.clone());
    }

    fn subscribe_gossip_topic_set(&mut self, topics: &ConsensusGossipTopics) {
        for topic in [
            topics.finality_update.clone(),
            topics.optimistic_update.clone(),
        ] {
            match self.swarm.behaviour_mut().gossip.subscribe(&topic) {
                Ok(true) | Ok(false) => {
                    self.gossip_subscriptions.insert(topic.hash());
                }
                Err(error) => {
                    tracing::warn!(topic = %topic.hash(), %error, "failed to subscribe to consensus gossip topic");
                }
            }
        }
    }

    fn unsubscribe_gossip_topic_set(&mut self, topics: &ConsensusGossipTopics) {
        for topic in [
            topics.finality_update.clone(),
            topics.optimistic_update.clone(),
        ] {
            self.swarm.behaviour_mut().gossip.unsubscribe(&topic);
            self.gossip_subscriptions.remove(&topic.hash());
        }
    }

    fn maintain_fork_subscriptions(&mut self) {
        let current_slot = current_wall_clock_slot();
        let current_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(current_slot);
        let expected_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(current_epoch);

        if let Some(next_epoch) =
            MAINNET_CONSENSUS_CHAIN_SPEC.next_scheduled_epoch_after(current_epoch)
        {
            let next_slot = next_epoch.saturating_mul(32);
            if next_slot <= current_slot.saturating_add(FORK_TOPIC_SUBSCRIBE_DELAY_SLOTS) {
                let next_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(next_epoch);
                if self.pre_subscribed_fork_digest != Some(next_digest) {
                    self.subscribe_gossip_topic_set(&build_gossip_topics(next_digest));
                    self.pre_subscribed_fork_digest = Some(next_digest);
                    tracing::info!(
                        next_epoch,
                        next_fork_digest = %hex::encode(next_digest),
                        "pre-subscribed to upcoming consensus fork gossip topics"
                    );
                }
            }
        }

        if expected_digest != self.fork_digest {
            let old_digest = self.fork_digest;
            let old_topics = self.gossip_topics.clone();
            let new_topics = build_gossip_topics(expected_digest);
            self.subscribe_gossip_topic_set(&new_topics);
            self.fork_digest = expected_digest;
            self.gossip_topics = new_topics;
            self.pre_subscribed_fork_digest = None;
            self.retiring_gossip_topics.push(RetiringGossipTopics {
                unsubscribe_at_epoch: current_epoch
                    .saturating_add(FORK_TOPIC_UNSUBSCRIBE_DELAY_EPOCHS),
                topics: old_topics,
            });

            let fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(current_epoch);
            let next_fork_digest =
                MAINNET_CONSENSUS_CHAIN_SPEC.next_fork_digest_for_epoch(current_epoch);
            if let Err(error) = self.discv5.enr_insert("eth2", &fork_id.as_slice()) {
                tracing::warn!(%error, "failed to update consensus fork ID in the local ENR");
            }
            if let Err(error) = self.discv5.enr_insert("nfd", &next_fork_digest.as_slice()) {
                tracing::warn!(%error, "failed to update next fork digest in the local ENR");
            }

            self.status_peers.clear();
            self.last_status_success_at.clear();
            tracing::info!(
                current_epoch,
                old_fork_digest = %hex::encode(old_digest),
                new_fork_digest = %hex::encode(expected_digest),
                "rotated consensus ENR and gossip topics at a scheduled fork transition"
            );
        }

        let retiring = std::mem::take(&mut self.retiring_gossip_topics);
        for topics in retiring {
            if topics.unsubscribe_at_epoch <= current_epoch {
                self.unsubscribe_gossip_topic_set(&topics.topics);
                tracing::info!(
                    current_epoch,
                    "unsubscribed from retired consensus fork gossip topics"
                );
            } else {
                self.retiring_gossip_topics.push(topics);
            }
        }
    }

    fn handle_gossip_event(&mut self, event: gossipsub::Event) {
        match event {
            gossipsub::Event::Message {
                propagation_source,
                message_id,
                message,
            } => {
                self.record_p2p_download_payload(usize_to_u64(message.data.len()));
                let acceptance = self.handle_gossip_message(propagation_source, &message);
                if !self
                    .swarm
                    .behaviour_mut()
                    .gossip
                    .report_message_validation_result(&message_id, &propagation_source, acceptance)
                {
                    tracing::debug!(
                        %propagation_source,
                        %message_id,
                        "consensus gossip message was no longer pending validation"
                    );
                }
            }
            gossipsub::Event::Subscribed { peer_id, topic } => {
                tracing::debug!(%peer_id, topic = %topic, "consensus peer subscribed to gossip topic");
            }
            gossipsub::Event::Unsubscribed { peer_id, topic } => {
                tracing::debug!(%peer_id, topic = %topic, "consensus peer unsubscribed from gossip topic");
            }
            _ => {}
        }
    }

    fn handle_gossip_message(
        &mut self,
        propagation_source: PeerId,
        message: &gossipsub::Message,
    ) -> gossipsub::MessageAcceptance {
        let Some(decoded) = decode_gossip_payload(&message.data) else {
            self.gossip_counts.decode_failures += 1;
            tracing::debug!(
                %propagation_source,
                topic = %message.topic,
                bytes = message.data.len(),
                "failed to decompress consensus gossip payload"
            );
            return gossipsub::MessageAcceptance::Reject;
        };

        if message.topic == self.gossip_topics.finality_update.hash() {
            let Some(store) = self.consensus.light_client_store() else {
                tracing::debug!(
                    %propagation_source,
                    bytes = decoded.len(),
                    "ignoring consensus finality-update gossip until a verified bootstrap exists"
                );
                return gossipsub::MessageAcceptance::Ignore;
            };
            if finality_update_is_stale(&decoded, &store) {
                tracing::trace!(
                    %propagation_source,
                    bytes = decoded.len(),
                    "ignoring stale consensus finality-update gossip"
                );
                return gossipsub::MessageAcceptance::Ignore;
            }

            match apply_finality_update_payload(&decoded, &store) {
                Ok((summary, next_store, _, _)) => {
                    self.gossip_counts.finality_update += 1;
                    tracing::debug!(
                        %propagation_source,
                        bytes = decoded.len(),
                        fork = ?summary.fork,
                        attested_slot = summary.attested_header.beacon_slot,
                        finalized_slot = summary.finalized_header.beacon_slot,
                        "received consensus light-client finality-update gossip"
                    );
                    if let Err(error) = self.consensus.record_verified_finality_update(
                        summary,
                        crate::rpc::RawRpcResponse {
                            context_bytes: None,
                            bytes: decoded,
                        },
                        next_store,
                    ) {
                        tracing::warn!(
                            %propagation_source,
                            %error,
                            "failed to persist verified finality update learned from gossip"
                        );
                    }
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                    return gossipsub::MessageAcceptance::Accept;
                }
                Err(error) => {
                    self.gossip_counts.decode_failures += 1;
                    tracing::debug!(
                        %propagation_source,
                        bytes = decoded.len(),
                        %error,
                        "failed to verify consensus finality-update gossip payload"
                    );
                    return gossipsub::MessageAcceptance::Reject;
                }
            }
        }

        if message.topic == self.gossip_topics.optimistic_update.hash() {
            let Some(store) = self.consensus.light_client_store() else {
                tracing::debug!(
                    %propagation_source,
                    bytes = decoded.len(),
                    "ignoring consensus optimistic-update gossip until a verified bootstrap exists"
                );
                return gossipsub::MessageAcceptance::Ignore;
            };
            if optimistic_update_is_stale(&decoded, &store) {
                tracing::trace!(
                    %propagation_source,
                    bytes = decoded.len(),
                    "ignoring stale consensus optimistic-update gossip"
                );
                return gossipsub::MessageAcceptance::Ignore;
            }

            match apply_optimistic_update_payload(&decoded, &store) {
                Ok((summary, next_store, _)) => {
                    self.gossip_counts.optimistic_update += 1;
                    tracing::debug!(
                        %propagation_source,
                        bytes = decoded.len(),
                        fork = ?summary.fork,
                        attested_slot = summary.attested_header.beacon_slot,
                        "received consensus light-client optimistic-update gossip"
                    );
                    if let Err(error) = self.consensus.record_verified_optimistic_update(
                        summary,
                        crate::rpc::RawRpcResponse {
                            context_bytes: None,
                            bytes: decoded,
                        },
                        next_store,
                    ) {
                        tracing::warn!(
                            %propagation_source,
                            %error,
                            "failed to persist verified optimistic update learned from gossip"
                        );
                    }
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                    return gossipsub::MessageAcceptance::Accept;
                }
                Err(error) => {
                    self.gossip_counts.decode_failures += 1;
                    tracing::debug!(
                        %propagation_source,
                        bytes = decoded.len(),
                        %error,
                        "failed to verify consensus optimistic-update gossip payload"
                    );
                    return gossipsub::MessageAcceptance::Reject;
                }
            }
        }

        gossipsub::MessageAcceptance::Ignore
    }

    fn handle_rpc_event(&mut self, kind: RpcRequestKind, event: Eth2RpcEvent) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    tracing::debug!(%peer, ?request, "received inbound consensus RPC request");
                    self.record_p2p_download_payload(consensus_request_payload_bytes(&request));
                    let status_irrelevance = match &request {
                        Eth2RpcRequest::Status(status) => status_irrelevance_reason(
                            self.local_status_message(),
                            *status,
                            current_wall_clock_slot(),
                        ),
                        _ => None,
                    };
                    let response = match self.validate_and_rate_limit_inbound_request(
                        peer,
                        kind,
                        &request,
                    ) {
                        Err(response) => response,
                        Ok(()) => match request {
                        Eth2RpcRequest::Status(status) => {
                            if status.fork_digest != self.fork_digest {
                                tracing::debug!(
                                    %peer,
                                    local = hex::encode(self.fork_digest),
                                    remote = hex::encode(status.fork_digest),
                                    "consensus peer requested status with a different fork digest"
                                );
                            }
                            Eth2RpcResponse::Status(self.local_status_message())
                        }
                        Eth2RpcRequest::MetaData => {
                            Eth2RpcResponse::MetaData(self.local_metadata())
                        }
                        Eth2RpcRequest::Ping(_) => {
                            Eth2RpcResponse::Ping(self.local_metadata().seq_number)
                        }
                        Eth2RpcRequest::Goodbye(_) => {
                            resource_unavailable("goodbye must use the goodbye RPC family")
                        }
                        Eth2RpcRequest::LightClientBootstrap(root) => {
                            if root == self.consensus.checkpoint().beacon_root {
                                self.consensus
                                    .light_client_payloads()
                                    .bootstrap
                                    .map(Eth2RpcResponse::LightClientBootstrap)
                                    .unwrap_or_else(|| {
                                        resource_unavailable(
                                            "light-client bootstrap is not yet available locally",
                                        )
                                    })
                            } else {
                                resource_unavailable(
                                    "light-client bootstrap is only available for the local trusted checkpoint root",
                                )
                            }
                        }
                        Eth2RpcRequest::LightClientFinalityUpdate => self
                            .consensus
                            .light_client_payloads()
                            .finality_update
                            .map(Eth2RpcResponse::LightClientFinalityUpdate)
                            .unwrap_or_else(|| {
                                resource_unavailable(
                                    "light-client finality update is not yet available locally",
                                )
                            }),
                        Eth2RpcRequest::LightClientOptimisticUpdate => self
                            .consensus
                            .light_client_payloads()
                            .optimistic_update
                            .map(Eth2RpcResponse::LightClientOptimisticUpdate)
                            .unwrap_or_else(|| {
                                resource_unavailable(
                                    "light-client optimistic update is not yet available locally",
                                )
                            }),
                        Eth2RpcRequest::LightClientUpdatesByRange(request) => {
                            let responses =
                                self.cached_verified_light_client_updates_by_range(request);
                            if responses.is_empty() {
                                resource_unavailable(
                                    "light-client updates by range are not yet available locally",
                                )
                            } else {
                                Eth2RpcResponse::LightClientUpdatesByRange(responses)
                            }
                        }
                        Eth2RpcRequest::BeaconBlocksByRange(request) => {
                            Eth2RpcResponse::BeaconBlocksByRange(
                                self.cached_verified_beacon_blocks_by_range(request),
                            )
                        }
                        Eth2RpcRequest::BeaconBlocksByRoot(roots) => {
                            Eth2RpcResponse::BeaconBlocksByRoot(
                                self.cached_verified_beacon_blocks_by_root(&roots),
                            )
                        }
                        },
                    };
                    if let Err(response) = self.send_rpc_response(kind, channel, response) {
                        let peer_context = self.peer_context(peer);
                        self.last_response_send_failure = Some(format!(
                            "{peer_context} request={} response={response:?}",
                            kind.as_str()
                        ));
                        tracing::debug!(
                            %peer,
                            error = ?response,
                            "failed to send consensus RPC response"
                        );
                    }
                    if let Some(reason) = status_irrelevance {
                        self.mark_peer_ignored_for_run(
                            peer,
                            format!("irrelevant inbound status: {reason}"),
                        );
                        self.disconnect_peer_with_reason(peer, GOODBYE_REASON_IRRELEVANT_NETWORK);
                    }
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => self.handle_rpc_response(kind, peer, request_id, response),
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                let _ = self.take_pending_request(kind, request_id);
                if kind == RpcRequestKind::BeaconBlocksByRoot {
                    self.take_pending_history_root_request(request_id);
                }
                if kind == RpcRequestKind::BeaconBlocksByRange {
                    self.take_pending_history_range_request(request_id);
                }
                self.request_failures.increment(kind);
                let peer_failures = self.record_peer_failure(peer, kind);
                let peer_context = self.peer_context(peer);
                self.last_rpc_failure = Some(format!(
                    "{peer_context} request={} failure={error}",
                    kind.as_str()
                ));
                match kind {
                    RpcRequestKind::LightClientBootstrap
                    | RpcRequestKind::LightClientUpdatesByRange
                    | RpcRequestKind::LightClientFinalityUpdate
                    | RpcRequestKind::LightClientOptimisticUpdate
                    | RpcRequestKind::BeaconBlocksByRange
                    | RpcRequestKind::BeaconBlocksByRoot => {
                        tracing::debug!(
                            %peer,
                            request = kind.as_str(),
                            %error,
                            "consensus light-client RPC request failed"
                        );
                    }
                    _ => {
                        tracing::debug!(
                            %peer,
                            request = kind.as_str(),
                            %error,
                            "consensus RPC request failed"
                        );
                    }
                }
                if kind == RpcRequestKind::Status
                    && peer_failures >= MAX_STATUS_FAILURES_BEFORE_DISCONNECT
                {
                    tracing::debug!(
                        %peer,
                        failures = peer_failures,
                        "disconnecting consensus peer after repeated status RPC failures"
                    );
                    self.disconnect_peer_with_reason(peer, GOODBYE_REASON_FAULT);
                }
            }
            request_response::Event::InboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                tracing::debug!(%peer, request = kind.as_str(), ?request_id, %error, "consensus RPC inbound failure");
            }
            request_response::Event::ResponseSent {
                peer, request_id, ..
            } => {
                tracing::debug!(%peer, request = kind.as_str(), ?request_id, "consensus RPC response sent");
                if kind == RpcRequestKind::Status {
                    self.inbound_status_peers.insert(peer);
                }
            }
        }
    }

    fn validate_and_rate_limit_inbound_request(
        &mut self,
        peer: PeerId,
        kind: RpcRequestKind,
        request: &Eth2RpcRequest,
    ) -> Result<(), Eth2RpcResponse> {
        let cost = inbound_request_cost(request).map_err(invalid_request)?;
        let now = Instant::now();
        let (quota, window) = inbound_rate_limit_quota(kind);
        let bucket =
            self.inbound_rate_limits
                .entry((peer, kind))
                .or_insert(InboundRateLimitBucket {
                    window_started_at: now,
                    used: 0,
                });
        if consume_inbound_rate_limit(bucket, now, cost, quota, window) {
            Ok(())
        } else {
            tracing::debug!(
                %peer,
                request = kind.as_str(),
                cost,
                quota,
                "rate limiting inbound consensus RPC request"
            );
            Err(rate_limited("rate limit exceeded"))
        }
    }

    fn handle_goodbye_rpc_event(&mut self, event: Eth2RpcEvent) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    tracing::debug!(%peer, ?request, "received inbound consensus goodbye RPC request");
                    self.record_p2p_download_payload(consensus_request_payload_bytes(&request));
                    if let Eth2RpcRequest::Goodbye(reason) = request {
                        let _ = self.validate_and_rate_limit_inbound_request(
                            peer,
                            RpcRequestKind::Goodbye,
                            &Eth2RpcRequest::Goodbye(reason),
                        );
                        tracing::debug!(%peer, reason, "consensus peer sent goodbye");
                    }
                    drop(channel);
                    self.disconnect_now(peer);
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => {
                    let Some(_) = self.take_pending_request(RpcRequestKind::Goodbye, request_id)
                    else {
                        tracing::warn!(%peer, ?request_id, "received consensus goodbye response for an unknown request");
                        return;
                    };
                    self.record_p2p_download_payload(consensus_response_payload_bytes(&response));
                    match response {
                        Eth2RpcResponse::Goodbye(reason) => {
                            tracing::debug!(%peer, reason, "received consensus goodbye response");
                        }
                        Eth2RpcResponse::Error(error) => {
                            tracing::debug!(
                                %peer,
                                error_code = error.code,
                                message = %String::from_utf8_lossy(&error.message),
                                "consensus goodbye RPC returned an error"
                            );
                        }
                        other => {
                            tracing::warn!(
                                %peer,
                                ?other,
                                "consensus goodbye RPC returned an unexpected response"
                            );
                        }
                    }
                    self.disconnect_now(peer);
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                let _ = self.take_pending_request(RpcRequestKind::Goodbye, request_id);
                let expected_no_response =
                    matches!(
                        &error,
                        request_response::OutboundFailure::Io(io_error)
                            if io_error.kind() == io::ErrorKind::UnexpectedEof
                    ) || matches!(error, request_response::OutboundFailure::ConnectionClosed);
                if expected_no_response {
                    tracing::debug!(
                        %peer,
                        ?request_id,
                        %error,
                        "consensus goodbye RPC completed without an explicit response payload"
                    );
                } else {
                    self.request_failures.increment(RpcRequestKind::Goodbye);
                    let peer_context = self.peer_context(peer);
                    self.last_rpc_failure =
                        Some(format!("{peer_context} request=goodbye failure={error}"));
                    tracing::debug!(
                        %peer,
                        ?request_id,
                        %error,
                        "consensus goodbye RPC request failed"
                    );
                }
                self.disconnect_now(peer);
            }
            request_response::Event::InboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                tracing::debug!(%peer, ?request_id, %error, "consensus goodbye RPC inbound failure");
                self.disconnect_now(peer);
            }
            request_response::Event::ResponseSent {
                peer, request_id, ..
            } => {
                tracing::debug!(%peer, ?request_id, "consensus goodbye RPC response sent");
                self.disconnect_now(peer);
            }
        }
    }

    fn handle_metadata_rpc_event(&mut self, event: Eth2RpcEvent) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    tracing::debug!(%peer, ?request, "received inbound consensus metadata RPC request");
                    self.record_p2p_download_payload(consensus_request_payload_bytes(&request));
                    let response = match self.validate_and_rate_limit_inbound_request(
                        peer,
                        RpcRequestKind::MetaData,
                        &request,
                    ) {
                        Err(response) => response,
                        Ok(()) => match request {
                            Eth2RpcRequest::MetaData => {
                                Eth2RpcResponse::MetaData(self.local_metadata())
                            }
                            _ => resource_unavailable("unsupported request on metadata RPC family"),
                        },
                    };
                    let payload_bytes = consensus_response_payload_bytes(&response);
                    let result = self
                        .swarm
                        .behaviour_mut()
                        .metadata_rpc
                        .inner
                        .send_response(channel, response);
                    if result.is_ok() {
                        self.record_p2p_upload_payload(payload_bytes);
                    }
                    if let Err(response) = result {
                        let peer_context = self.peer_context(peer);
                        self.last_response_send_failure = Some(format!(
                            "{peer_context} request=metadata response={response:?}"
                        ));
                        tracing::debug!(
                            %peer,
                            error = ?response,
                            "failed to send consensus metadata RPC response"
                        );
                    }
                }
                request_response::Message::Response {
                    request_id,
                    response,
                } => self.handle_rpc_response(RpcRequestKind::MetaData, peer, request_id, response),
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                let _ = self.take_pending_request(RpcRequestKind::MetaData, request_id);
                self.request_failures.increment(RpcRequestKind::MetaData);
                self.record_peer_failure(peer, RpcRequestKind::MetaData);
                let peer_context = self.peer_context(peer);
                self.last_rpc_failure =
                    Some(format!("{peer_context} request=metadata failure={error}"));
                tracing::debug!(%peer, ?request_id, %error, "consensus metadata RPC request failed");
            }
            request_response::Event::InboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                tracing::debug!(%peer, ?request_id, %error, "consensus metadata RPC inbound failure");
            }
            request_response::Event::ResponseSent {
                peer, request_id, ..
            } => {
                tracing::debug!(%peer, ?request_id, "consensus metadata RPC response sent");
            }
        }
    }

    fn handle_rpc_response(
        &mut self,
        kind: RpcRequestKind,
        peer: PeerId,
        request_id: Eth2OutboundRequestId,
        response: Eth2RpcResponse,
    ) {
        let Some(_) = self.take_pending_request(kind, request_id) else {
            tracing::warn!(%peer, request = kind.as_str(), ?request_id, "received consensus RPC response for an unknown request");
            return;
        };
        let requested_history_roots = if kind == RpcRequestKind::BeaconBlocksByRoot {
            self.take_pending_history_root_request(request_id)
        } else {
            Vec::new()
        };
        let requested_history_range = if kind == RpcRequestKind::BeaconBlocksByRange {
            self.take_pending_history_range_request(request_id)
        } else {
            None
        };
        self.record_p2p_download_payload(consensus_response_payload_bytes(&response));

        match (kind, response) {
            (RpcRequestKind::Status, Eth2RpcResponse::Status(status)) => {
                if let Some(reason) = status_irrelevance_reason(
                    self.local_status_message(),
                    status,
                    current_wall_clock_slot(),
                ) {
                    self.mark_peer_ignored_for_run(peer, format!("irrelevant status: {reason}"));
                    tracing::info!(
                        %peer,
                        %reason,
                        local_fork = %hex::encode(self.fork_digest),
                        remote_fork = %hex::encode(status.fork_digest),
                        remote_head_slot = status.head_slot,
                        "disconnecting consensus peer after an irrelevant status response"
                    );
                    self.disconnect_peer_with_reason(peer, GOODBYE_REASON_IRRELEVANT_NETWORK);
                    return;
                }
                tracing::debug!(
                    %peer,
                    finalized_epoch = status.finalized_epoch,
                    head_slot = status.head_slot,
                    earliest_available_slot = status.earliest_available_slot,
                    "received consensus status response"
                );
                self.record_peer_success(peer, RpcRequestKind::Status);
                self.status_peers.insert(peer);
                self.last_status_success_at.insert(peer, Instant::now());
                self.drive_rpc_requests();
            }
            (RpcRequestKind::MetaData, Eth2RpcResponse::MetaData(metadata)) => {
                tracing::debug!(
                    %peer,
                    seq_number = metadata.seq_number,
                    "received consensus metadata response"
                );
                self.record_peer_success(peer, RpcRequestKind::MetaData);
                self.metadata_peers.insert(peer);
                self.drive_rpc_requests();
            }
            (RpcRequestKind::Ping, Eth2RpcResponse::Ping(seq_number)) => {
                tracing::debug!(%peer, seq_number, "received consensus ping response");
                self.record_peer_success(peer, RpcRequestKind::Ping);
                self.ping_peers.insert(peer);
                self.last_ping_success_at.insert(peer, Instant::now());
                self.drive_rpc_requests();
            }
            (
                RpcRequestKind::LightClientBootstrap,
                Eth2RpcResponse::LightClientBootstrap(payload),
            ) => match verify_bootstrap_payload(&payload.bytes, self.config.checkpoint) {
                Ok((summary, store))
                    if rpc_context_matches_slot(&payload, summary.header.beacon_slot) =>
                {
                    tracing::info!(
                        %peer,
                        bytes = payload.bytes.len(),
                        checkpoint_root = %self.config.checkpoint.beacon_root,
                        fork = ?summary.fork,
                        beacon_slot = summary.header.beacon_slot,
                        execution_block =
                            summary.header.execution.map(|execution| execution.block_number),
                        "received and decoded light-client bootstrap payload"
                    );
                    self.record_peer_success(peer, RpcRequestKind::LightClientBootstrap);
                    self.bootstrap_peers.insert(peer);
                    if let Err(error) =
                        self.consensus
                            .record_verified_bootstrap(summary, payload.clone(), store)
                    {
                        tracing::warn!(
                            %peer,
                            %error,
                            "failed to persist verified bootstrap payload"
                        );
                    }
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                }
                Ok((summary, _)) => {
                    let detail = format!(
                        "fork context {:?} does not match bootstrap slot {}",
                        payload.context_bytes, summary.header.beacon_slot
                    );
                    tracing::warn!(%peer, %detail, "rejected light-client bootstrap response");
                    self.record_invalid_light_client_response(
                        peer,
                        RpcRequestKind::LightClientBootstrap,
                        detail,
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        %peer,
                        bytes = payload.bytes.len(),
                        %error,
                        "failed to verify light-client bootstrap payload"
                    );
                    self.record_invalid_light_client_response(
                        peer,
                        RpcRequestKind::LightClientBootstrap,
                        error.to_string(),
                    );
                }
            },
            (
                RpcRequestKind::LightClientUpdatesByRange,
                Eth2RpcResponse::LightClientUpdatesByRange(chunks),
            ) => {
                let total_bytes = chunks.iter().map(|chunk| chunk.bytes.len()).sum::<usize>();
                let Some(mut store) = self.consensus.light_client_store() else {
                    tracing::debug!(
                        %peer,
                        chunks = chunks.len(),
                        total_bytes,
                        "ignoring light-client updates by range response until a verified bootstrap exists"
                    );
                    return;
                };
                let mut applied = None;
                let mut verified_updates_by_period = Vec::new();
                let mut peer_fault = None;
                for chunk in &chunks {
                    match apply_light_client_update_payload(&chunk.bytes, &store) {
                        Ok(next) => {
                            let attested_slot = next.optimistic_status.attested_header.beacon_slot;
                            if !rpc_context_matches_slot(chunk, attested_slot) {
                                peer_fault = Some(format!(
                                    "fork context {:?} does not match update attested slot {attested_slot}",
                                    chunk.context_bytes
                                ));
                                continue;
                            }
                            let period = sync_committee_period_for_slot(attested_slot);
                            tracing::debug!(
                                %peer,
                                bytes = chunk.bytes.len(),
                                period,
                                attested_slot = next.optimistic_status.attested_header.beacon_slot,
                                finalized_slot = next
                                    .finality_status
                                    .as_ref()
                                    .map(|status| status.finalized_header.beacon_slot),
                                participants = next.optimistic_status.sync_committee_participants,
                                "received and verified light-client update payload"
                            );
                            store = next.store.clone();
                            verified_updates_by_period.push((period, chunk.clone()));
                            applied = Some(next);
                        }
                        Err(error) => {
                            tracing::warn!(
                                %peer,
                                bytes = chunk.bytes.len(),
                                %error,
                                "failed to verify light-client update payload"
                            );
                            if light_client_verification_error_is_peer_fault(&error) {
                                peer_fault = Some(error.to_string());
                            }
                        }
                    }
                }
                if let Some(applied) = applied {
                    self.record_peer_success(peer, RpcRequestKind::LightClientUpdatesByRange);
                    self.updates_by_range_peers.insert(peer);
                    if let Err(error) = self
                        .consensus
                        .record_verified_applied_update(applied, verified_updates_by_period)
                    {
                        tracing::warn!(
                            %peer,
                            %error,
                            "failed to persist verified light-client updates by range state"
                        );
                    }
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                } else {
                    tracing::warn!(
                        %peer,
                        chunks = chunks.len(),
                        total_bytes,
                        "light-client updates by range stream did not yield a usable verified update"
                    );
                    if let Some(detail) = peer_fault {
                        self.record_invalid_light_client_response(
                            peer,
                            RpcRequestKind::LightClientUpdatesByRange,
                            detail,
                        );
                    }
                }
            }
            (
                RpcRequestKind::LightClientFinalityUpdate,
                Eth2RpcResponse::LightClientFinalityUpdate(payload),
            ) => {
                let Some(store) = self.consensus.light_client_store() else {
                    tracing::debug!(
                        %peer,
                        "ignoring light-client finality update response until a verified bootstrap exists"
                    );
                    return;
                };
                if finality_update_is_stale(&payload.bytes, &store) {
                    tracing::trace!(
                        %peer,
                        bytes = payload.bytes.len(),
                        "ignoring stale light-client finality update response"
                    );
                    return;
                }
                match apply_finality_update_payload(&payload.bytes, &store) {
                    Ok((summary, next_store, _, _)) => {
                        if !rpc_context_matches_slot(&payload, summary.attested_header.beacon_slot)
                        {
                            self.record_invalid_light_client_response(
                                peer,
                                RpcRequestKind::LightClientFinalityUpdate,
                                format!(
                                    "fork context {:?} does not match finality attested slot {}",
                                    payload.context_bytes, summary.attested_header.beacon_slot
                                ),
                            );
                            return;
                        }
                        tracing::debug!(
                            %peer,
                            bytes = payload.bytes.len(),
                            fork = ?summary.fork,
                            attested_slot = summary.attested_header.beacon_slot,
                            finalized_slot = summary.finalized_header.beacon_slot,
                            execution_block = summary
                                .finalized_header
                                .execution
                                .map(|execution| execution.block_number),
                            participants = summary.sync_committee_participants,
                            "received and decoded light-client finality update payload"
                        );
                        self.record_peer_success(peer, RpcRequestKind::LightClientFinalityUpdate);
                        self.finality_update_peers.insert(peer);
                        if let Err(error) = self.consensus.record_verified_finality_update(
                            summary,
                            payload.clone(),
                            next_store,
                        ) {
                            tracing::warn!(
                                %peer,
                                %error,
                                "failed to persist verified finality update"
                            );
                        }
                        self.seed_verified_light_client_headers();
                        self.materialize_verified_anchor_segments();
                        self.drive_rpc_requests();
                    }
                    Err(error) => {
                        tracing::warn!(
                            %peer,
                            bytes = payload.bytes.len(),
                            %error,
                            "failed to verify light-client finality update payload"
                        );
                        if light_client_verification_error_is_peer_fault(&error) {
                            self.record_invalid_light_client_response(
                                peer,
                                RpcRequestKind::LightClientFinalityUpdate,
                                error.to_string(),
                            );
                        }
                    }
                }
            }
            (
                RpcRequestKind::LightClientOptimisticUpdate,
                Eth2RpcResponse::LightClientOptimisticUpdate(payload),
            ) => {
                let Some(store) = self.consensus.light_client_store() else {
                    tracing::debug!(
                        %peer,
                        "ignoring light-client optimistic update response until a verified bootstrap exists"
                    );
                    return;
                };
                if optimistic_update_is_stale(&payload.bytes, &store) {
                    tracing::trace!(
                        %peer,
                        bytes = payload.bytes.len(),
                        "ignoring stale light-client optimistic update response"
                    );
                    return;
                }
                match apply_optimistic_update_payload(&payload.bytes, &store) {
                    Ok((summary, next_store, _)) => {
                        if !rpc_context_matches_slot(&payload, summary.attested_header.beacon_slot)
                        {
                            self.record_invalid_light_client_response(
                                peer,
                                RpcRequestKind::LightClientOptimisticUpdate,
                                format!(
                                    "fork context {:?} does not match optimistic attested slot {}",
                                    payload.context_bytes, summary.attested_header.beacon_slot
                                ),
                            );
                            return;
                        }
                        tracing::debug!(
                            %peer,
                            bytes = payload.bytes.len(),
                            fork = ?summary.fork,
                            attested_slot = summary.attested_header.beacon_slot,
                            execution_block = summary
                                .attested_header
                                .execution
                                .map(|execution| execution.block_number),
                            participants = summary.sync_committee_participants,
                            "received and decoded light-client optimistic update payload"
                        );
                        self.record_peer_success(peer, RpcRequestKind::LightClientOptimisticUpdate);
                        self.optimistic_update_peers.insert(peer);
                        if let Err(error) = self.consensus.record_verified_optimistic_update(
                            summary,
                            payload.clone(),
                            next_store,
                        ) {
                            tracing::warn!(
                                %peer,
                                %error,
                                "failed to persist verified optimistic update"
                            );
                        }
                        self.seed_verified_light_client_headers();
                        self.materialize_verified_anchor_segments();
                        self.drive_rpc_requests();
                    }
                    Err(error) => {
                        tracing::warn!(
                            %peer,
                            bytes = payload.bytes.len(),
                            %error,
                            "failed to verify light-client optimistic update payload"
                        );
                        if light_client_verification_error_is_peer_fault(&error) {
                            self.record_invalid_light_client_response(
                                peer,
                                RpcRequestKind::LightClientOptimisticUpdate,
                                error.to_string(),
                            );
                        }
                    }
                }
            }
            (RpcRequestKind::BeaconBlocksByRange, Eth2RpcResponse::BeaconBlocksByRange(chunks)) => {
                let total_bytes = chunks.iter().map(|chunk| chunk.bytes.len()).sum::<usize>();
                tracing::debug!(
                    %peer,
                    chunks = chunks.len(),
                    total_bytes,
                    "received beacon blocks by range response stream"
                );
                let mut decoded_blocks = Vec::new();
                let mut invalid_response = false;
                for chunk in &chunks {
                    match decode_verified_beacon_block(chunk) {
                        Ok(block) => {
                            if requested_history_range.is_some_and(|request| {
                                !range_request_contains_slot(request, block.slot)
                            }) {
                                invalid_response = true;
                                tracing::warn!(
                                    %peer,
                                    actual_root = %block.beacon_root,
                                    actual_slot = block.slot,
                                    actual_parent_root = %block.parent_root,
                                    requested_range = ?requested_history_range,
                                    "discarding beacon block by range response outside the requested slot window"
                                );
                                continue;
                            }
                            decoded_blocks.push((block, chunk.clone()));
                        }
                        Err(error) => {
                            tracing::warn!(
                                %peer,
                                bytes = chunk.bytes.len(),
                                %error,
                                "failed to decode or verify beacon block from range response"
                            );
                        }
                    }
                }
                if invalid_response {
                    self.disconnect_faulty_history_peer(
                        peer,
                        RpcRequestKind::BeaconBlocksByRange,
                        format!(
                            "invalid_range_response requested_range={requested_history_range:?}"
                        ),
                    );
                    return;
                }
                if decoded_blocks.is_empty() {
                    self.record_unusable_history_response(
                        peer,
                        RpcRequestKind::BeaconBlocksByRange,
                        format!(
                            "no_decodable_blocks chunks={} total_bytes={} requested_range={requested_history_range:?}",
                            chunks.len(),
                            total_bytes
                        ),
                    );
                    return;
                }

                self.record_peer_success(peer, RpcRequestKind::BeaconBlocksByRange);
                self.beacon_blocks_by_range_peers.insert(peer);
                let mut inserted = 0usize;
                for (block, payload) in decoded_blocks {
                    if self.record_verified_beacon_block(block, Some(payload)) {
                        inserted += 1;
                    }
                }
                if inserted > 0 {
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                }
            }
            (RpcRequestKind::BeaconBlocksByRoot, Eth2RpcResponse::BeaconBlocksByRoot(chunks)) => {
                let total_bytes = chunks.iter().map(|chunk| chunk.bytes.len()).sum::<usize>();
                tracing::debug!(
                    %peer,
                    chunks = chunks.len(),
                    total_bytes,
                    "received beacon blocks by root response stream"
                );
                let mut decoded_blocks = Vec::new();
                let mut invalid_response = false;
                for chunk in &chunks {
                    match decode_verified_beacon_block(chunk) {
                        Ok(block) => {
                            if !requested_history_roots.is_empty()
                                && !requested_history_roots.contains(&block.beacon_root)
                            {
                                invalid_response = true;
                                tracing::warn!(
                                    %peer,
                                    actual_root = %block.beacon_root,
                                    actual_slot = block.slot,
                                    actual_parent_root = %block.parent_root,
                                    requested_roots = ?requested_history_roots,
                                    "discarding beacon block by root response that did not match the requested trusted roots"
                                );
                                continue;
                            }
                            decoded_blocks.push((block, chunk.clone()));
                        }
                        Err(error) => {
                            tracing::warn!(
                                %peer,
                                bytes = chunk.bytes.len(),
                                %error,
                                "failed to decode or verify beacon block from root response"
                            );
                        }
                    }
                }
                if invalid_response {
                    let requested_roots = requested_history_roots
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(",");
                    self.disconnect_faulty_history_peer(
                        peer,
                        RpcRequestKind::BeaconBlocksByRoot,
                        format!("invalid_root_response requested_roots=[{requested_roots}]"),
                    );
                    return;
                }
                if decoded_blocks.is_empty() {
                    self.record_unusable_history_response(
                        peer,
                        RpcRequestKind::BeaconBlocksByRoot,
                        format!(
                            "no_decodable_blocks chunks={} total_bytes={} requested_roots={requested_history_roots:?}",
                            chunks.len(),
                            total_bytes
                        ),
                    );
                    return;
                }

                self.record_peer_success(peer, RpcRequestKind::BeaconBlocksByRoot);
                self.beacon_blocks_by_root_peers.insert(peer);
                let mut inserted = 0usize;
                for (block, payload) in decoded_blocks {
                    if self.record_verified_beacon_block(block, Some(payload)) {
                        inserted += 1;
                    }
                }
                if inserted > 0 {
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                }
            }
            (kind, Eth2RpcResponse::Error(error)) => {
                self.request_failures.increment(kind);
                let peer_failures = self.record_peer_failure(peer, kind);
                let message = String::from_utf8_lossy(&error.message);
                let peer_context = self.peer_context(peer);
                self.last_rpc_failure = Some(format!(
                    "{peer_context} request={} error_code={} message={message}",
                    kind.as_str(),
                    error.code
                ));
                match kind {
                    RpcRequestKind::LightClientBootstrap
                    | RpcRequestKind::LightClientUpdatesByRange
                    | RpcRequestKind::LightClientFinalityUpdate
                    | RpcRequestKind::LightClientOptimisticUpdate
                    | RpcRequestKind::BeaconBlocksByRange
                    | RpcRequestKind::BeaconBlocksByRoot => {
                        tracing::debug!(
                            %peer,
                            request = kind.as_str(),
                            error_code = error.code,
                            message = %message,
                            "consensus peer returned a light-client RPC error"
                        );
                    }
                    _ => {
                        tracing::debug!(
                            %peer,
                            request = kind.as_str(),
                            error_code = error.code,
                            message = %message,
                            "consensus peer returned an RPC error"
                        );
                    }
                }
                if kind == RpcRequestKind::Status
                    && peer_failures >= MAX_STATUS_FAILURES_BEFORE_DISCONNECT
                {
                    tracing::debug!(
                        %peer,
                        failures = peer_failures,
                        "disconnecting consensus peer after repeated status RPC errors"
                    );
                    self.disconnect_peer_with_reason(peer, GOODBYE_REASON_FAULT);
                }
            }
            (kind, response) => {
                let peer_context = self.peer_context(peer);
                self.last_rpc_failure = Some(format!(
                    "{peer_context} request={} unexpected_response={response:?}",
                    kind.as_str()
                ));
                tracing::warn!(
                    %peer,
                    request = kind.as_str(),
                    ?response,
                    "consensus peer returned an unexpected RPC response"
                );
            }
        }
    }

    fn drive_peer_connections(&mut self) {
        let bootstrap_needed = self.consensus.light_client_status().bootstrap.is_none();
        let head_progression_needed = !bootstrap_needed
            && live_head_progression_needed(
                self.latest_history_sync_target(),
                current_wall_clock_slot(),
            );
        let now = Instant::now();
        for lifecycle in self.peer_lifecycle.values_mut() {
            lifecycle.clear_expired_cooldown(now);
        }
        let mut dialable = self
            .dialable_peers
            .iter()
            .map(|(peer, addrs)| (*peer, addrs.clone()))
            .collect::<Vec<_>>();
        for (_, addrs) in &mut dialable {
            retain_dial_addresses_for_families(self.config.dial_families, addrs);
        }
        dialable.retain(|(peer, addrs)| {
            !addrs.is_empty() && self.peer_is_dialable(*peer, bootstrap_needed, now)
        });
        tracing::debug!(
            bootstrap_needed,
            discovered = self.observed.len(),
            dialable = self.dialable_peers.len(),
            eligible = dialable.len(),
            dialing = self.dialing_peers.len(),
            connected = self.connected_peers.len(),
            max_peers = self.config.max_peers,
            "driving consensus peer connections"
        );
        dialable.sort_by(|(left, _), (right, _)| {
            self.peer_priority(*right, bootstrap_needed, head_progression_needed)
                .cmp(&self.peer_priority(*left, bootstrap_needed, head_progression_needed))
                .then_with(|| left.cmp(right))
        });

        let active_targets = self.connected_peers.len() + self.dialing_peers.len();
        if head_progression_needed {
            let active_progression_peers = self
                .connected_peers
                .iter()
                .filter(|peer| {
                    self.peer_support
                        .get(peer)
                        .is_some_and(|support| support.supports_light_client_progression())
                })
                .count()
                + self
                    .dialing_peers
                    .iter()
                    .filter(|peer| {
                        self.peer_lifecycle
                            .get(peer)
                            .and_then(|lifecycle| lifecycle.remembered_support)
                            .is_some_and(|support| support.supports_light_client_progression())
                    })
                    .count();
            let dialable_progression_candidates = dialable
                .iter()
                .filter(|(peer, _)| {
                    !self.connected_peers.contains(peer)
                        && !self.dialing_peers.contains(peer)
                        && self
                            .peer_lifecycle
                            .get(peer)
                            .and_then(|lifecycle| lifecycle.remembered_support)
                            .is_some_and(|support| support.supports_light_client_progression())
                })
                .count();
            let slots_to_reclaim = head_recovery_connection_slots_to_reclaim(
                self.config.max_peers,
                active_targets,
                active_progression_peers,
                dialable_progression_candidates,
            );

            if slots_to_reclaim > 0 {
                let mut replaceable_peers =
                    self.connected_peers
                        .iter()
                        .copied()
                        .filter(|peer| {
                            !self.closing_peers.contains(peer)
                                && self.pending_requests_for_peer(*peer) == 0
                                && self.peer_support.get(peer).is_some_and(|support| {
                                    !support.supports_light_client_progression()
                                })
                        })
                        .collect::<Vec<_>>();
                replaceable_peers.sort_by(|left, right| {
                    self.peer_priority(*left, bootstrap_needed, head_progression_needed)
                        .cmp(&self.peer_priority(*right, bootstrap_needed, head_progression_needed))
                        .then_with(|| left.cmp(right))
                });
                let replaceable_peers = replaceable_peers
                    .into_iter()
                    .take(slots_to_reclaim)
                    .collect::<Vec<_>>();
                if !replaceable_peers.is_empty() {
                    tracing::info!(
                        peers = replaceable_peers.len(),
                        active_progression_peers,
                        dialable_progression_candidates,
                        "reclaiming consensus connections for stale-head recovery"
                    );
                    for peer in replaceable_peers {
                        self.disconnect_peer_with_reason(peer, GOODBYE_REASON_IRRELEVANT_NETWORK);
                    }
                    return;
                }
            }
        }

        let mut active_targets = active_targets;
        for (peer, addrs) in dialable {
            if self.connected_peers.contains(&peer) || self.dialing_peers.contains(&peer) {
                continue;
            }
            if self.config.max_peers > 0 && active_targets >= self.config.max_peers {
                tracing::debug!(
                    active_targets,
                    max_peers = self.config.max_peers,
                    "stopping consensus dial scheduling because the active target limit was reached"
                );
                break;
            }
            self.ensure_connected(peer, addrs);
            active_targets += 1;
        }
    }

    fn drive_rpc_requests(&mut self) {
        self.seed_verified_light_client_headers();
        self.refresh_history_sync_target();
        let bootstrap_needed = self.consensus.light_client_status().bootstrap.is_none();
        let head_progression_needed = !bootstrap_needed
            && live_head_progression_needed(
                self.latest_history_sync_target(),
                current_wall_clock_slot(),
            );
        let now = Instant::now();
        if !bootstrap_needed {
            for lifecycle in self.peer_lifecycle.values_mut() {
                lifecycle.clear_bootstrap_deferral();
            }
        }
        let mut connected = self.connected_peers.iter().copied().collect::<Vec<_>>();
        connected.sort_by(|left, right| {
            self.peer_priority(*right, bootstrap_needed, head_progression_needed)
                .cmp(&self.peer_priority(*left, bootstrap_needed, head_progression_needed))
                .then_with(|| left.cmp(right))
        });
        for peer in connected {
            if self
                .peer_lifecycle
                .get(&peer)
                .is_some_and(|lifecycle| lifecycle.in_cooldown(now))
            {
                if self.pending_requests_for_peer(peer) == 0 {
                    tracing::debug!(
                        %peer,
                        "disconnecting consensus peer in cooldown to free a request slot"
                    );
                    self.disconnect_peer_with_reason(peer, GOODBYE_REASON_FAULT);
                }
                continue;
            }

            let support = self.peer_support.get(&peer).copied();
            let identify_timed_out = self.identify_timed_out(peer);

            let Some(support) = support else {
                if identify_timed_out {
                    tracing::debug!(
                        %peer,
                        "disconnecting consensus peer that never identified after connect"
                    );
                    self.record_transport_backoff(peer, "identify_timeout".to_owned());
                    self.disconnect_peer_with_reason(peer, GOODBYE_REASON_FAULT);
                }
                continue;
            };

            if !support.status {
                self.mark_peer_ignored_for_run(peer, "identify missing status rpc".to_owned());
                tracing::debug!(
                    %peer,
                    "disconnecting consensus peer that identified without advertising the Status RPC"
                );
                self.disconnect_peer_with_reason(peer, GOODBYE_REASON_IRRELEVANT_NETWORK);
                continue;
            }

            if !self.is_request_satisfied(peer, RpcRequestKind::Status) {
                if self.can_issue_request(RpcRequestKind::Status) {
                    self.ensure_request(peer, RpcRequestKind::Status);
                }
                continue;
            }

            if support.supports_request(RpcRequestKind::Ping)
                && !self.is_request_satisfied(peer, RpcRequestKind::Ping)
                && self.can_issue_request(RpcRequestKind::Ping)
            {
                self.ensure_request(peer, RpcRequestKind::Ping);
            }

            if bootstrap_needed && !support.supports_bootstrap_sync() {
                if support.supports_any_post_bootstrap_work() {
                    self.defer_peer_until_post_bootstrap(
                        peer,
                        "peer only advertises post-bootstrap work".to_owned(),
                    );
                } else {
                    self.mark_peer_ignored_for_run(
                        peer,
                        "peer cannot serve bootstrap or useful post-bootstrap work".to_owned(),
                    );
                }
                tracing::debug!(
                    %peer,
                    "disconnecting consensus peer that cannot serve bootstrap during bootstrap phase"
                );
                self.disconnect_peer_with_reason(peer, GOODBYE_REASON_IRRELEVANT_NETWORK);
                continue;
            }

            if !bootstrap_needed && !support.supports_any_post_bootstrap_work() {
                self.mark_peer_ignored_for_run(
                    peer,
                    "peer lacks useful post-bootstrap consensus RPCs".to_owned(),
                );
                tracing::debug!(
                    %peer,
                    "disconnecting consensus peer that does not advertise useful post-bootstrap consensus RPCs"
                );
                self.disconnect_peer_with_reason(peer, GOODBYE_REASON_IRRELEVANT_NETWORK);
                continue;
            }

            if bootstrap_needed {
                if support.supports_request(RpcRequestKind::LightClientBootstrap)
                    && self.can_issue_request(RpcRequestKind::LightClientBootstrap)
                {
                    self.ensure_request(peer, RpcRequestKind::LightClientBootstrap);
                }
                continue;
            }

            if self.pending_requests_for_peer(peer) > 0 {
                continue;
            }

            if let Some(kind) = self.next_post_bootstrap_request_kind(support) {
                self.ensure_request(peer, kind);
            }
        }
    }

    fn send_rpc_response(
        &mut self,
        kind: RpcRequestKind,
        channel: request_response::ResponseChannel<Eth2RpcResponse>,
        response: Eth2RpcResponse,
    ) -> Result<(), Eth2RpcResponse> {
        let payload_bytes = consensus_response_payload_bytes(&response);
        let result = match kind {
            RpcRequestKind::Status => self
                .swarm
                .behaviour_mut()
                .status_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::Goodbye => self
                .swarm
                .behaviour_mut()
                .goodbye_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::MetaData => self
                .swarm
                .behaviour_mut()
                .metadata_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::Ping => self
                .swarm
                .behaviour_mut()
                .ping_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::LightClientBootstrap => self
                .swarm
                .behaviour_mut()
                .light_client_bootstrap_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::LightClientUpdatesByRange => self
                .swarm
                .behaviour_mut()
                .light_client_updates_by_range_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::LightClientFinalityUpdate => self
                .swarm
                .behaviour_mut()
                .light_client_finality_update_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::LightClientOptimisticUpdate => self
                .swarm
                .behaviour_mut()
                .light_client_optimistic_update_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::BeaconBlocksByRange => self
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_range_rpc
                .inner
                .send_response(channel, response),
            RpcRequestKind::BeaconBlocksByRoot => self
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_root_rpc
                .inner
                .send_response(channel, response),
        };
        if result.is_ok() {
            self.record_p2p_upload_payload(payload_bytes);
        }
        result
    }

    fn record_p2p_download_payload(&mut self, payload_bytes: u64) {
        self.p2p_download_metrics
            .record(payload_bytes, Instant::now());
    }

    fn record_p2p_upload_payload(&mut self, payload_bytes: u64) {
        self.p2p_upload_metrics
            .record(payload_bytes, Instant::now());
    }

    fn ensure_connected(&mut self, peer: PeerId, addrs: Vec<Multiaddr>) {
        let bootstrap_needed = self.consensus.light_client_status().bootstrap.is_none();
        let addrs = self
            .select_dial_addresses(peer, addrs, bootstrap_needed)
            .into_iter()
            .map(strip_peer_id)
            .collect::<Vec<_>>();
        if addrs.is_empty() {
            return;
        }

        let dial_targets = addrs
            .iter()
            .map(|addr| match dial_address_class(addr) {
                Some(class) => format!("{addr}({})", class.label()),
                None => addr.to_string(),
            })
            .collect::<Vec<_>>()
            .join(", ");
        tracing::debug!(
            %peer,
            bootstrap_needed,
            targets = %dial_targets,
            "starting consensus libp2p dial"
        );
        self.last_connection_event =
            Some(format!("dial_start peer={peer} targets=[{dial_targets}]"));
        let dial = DialOpts::peer_id(peer)
            .condition(PeerCondition::Disconnected)
            .addresses(addrs)
            .build();
        match self.swarm.dial(dial) {
            Ok(()) => {
                self.dialing_peers.insert(peer);
            }
            Err(error) => {
                tracing::debug!(%peer, %error, "failed to start consensus libp2p dial");
                let ignored_for_run = self.record_dial_error(peer, &error);
                if !ignored_for_run
                    && !matches!(
                        &error,
                        DialError::DialPeerConditionFalse(_)
                            | DialError::NoAddresses
                            | DialError::Aborted
                    )
                {
                    self.record_transport_backoff(peer, format!("dial_start_error error={error}"));
                }
            }
        }
    }

    fn ensure_request(&mut self, peer: PeerId, kind: RpcRequestKind) {
        if self.is_request_satisfied(peer, kind) || self.is_request_pending(peer, kind) {
            return;
        }

        let Some(request) = self.build_request(kind) else {
            return;
        };
        let payload_bytes = consensus_request_payload_bytes(&request);
        let requested_history_roots = match &request {
            Eth2RpcRequest::BeaconBlocksByRoot(roots) => Some(roots.clone()),
            _ => None,
        };
        let requested_history_range = match &request {
            Eth2RpcRequest::BeaconBlocksByRange(request) => Some(*request),
            _ => None,
        };
        let request_id = match kind {
            RpcRequestKind::Status => self
                .swarm
                .behaviour_mut()
                .status_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::Goodbye => self
                .swarm
                .behaviour_mut()
                .goodbye_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::MetaData => self
                .swarm
                .behaviour_mut()
                .metadata_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::Ping => self
                .swarm
                .behaviour_mut()
                .ping_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::LightClientBootstrap => self
                .swarm
                .behaviour_mut()
                .light_client_bootstrap_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::LightClientUpdatesByRange => self
                .swarm
                .behaviour_mut()
                .light_client_updates_by_range_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::LightClientFinalityUpdate => self
                .swarm
                .behaviour_mut()
                .light_client_finality_update_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::LightClientOptimisticUpdate => self
                .swarm
                .behaviour_mut()
                .light_client_optimistic_update_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::BeaconBlocksByRange => self
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_range_rpc
                .inner
                .send_request(&peer, request),
            RpcRequestKind::BeaconBlocksByRoot => self
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_root_rpc
                .inner
                .send_request(&peer, request),
        };
        tracing::debug!(%peer, request = kind.as_str(), ?request_id, "sent outbound consensus RPC request");
        self.record_p2p_upload_payload(payload_bytes);
        let key = PendingRequestKey { kind, request_id };
        self.pending_requests.insert(key, peer);
        if let Some(roots) = requested_history_roots {
            self.pending_history_root_requests.insert(key, roots);
        }
        if let Some(request) = requested_history_range {
            self.pending_history_range_requests.insert(key, request);
        }
        self.pending_peer_kinds.insert((peer, kind));
        if matches!(
            kind,
            RpcRequestKind::LightClientUpdatesByRange
                | RpcRequestKind::LightClientFinalityUpdate
                | RpcRequestKind::LightClientOptimisticUpdate
        ) {
            self.last_light_client_request_at
                .insert(kind, Instant::now());
        }
        if matches!(
            kind,
            RpcRequestKind::LightClientUpdatesByRange
                | RpcRequestKind::LightClientFinalityUpdate
                | RpcRequestKind::LightClientOptimisticUpdate
        ) && live_head_progression_needed(
            self.latest_history_sync_target(),
            current_wall_clock_slot(),
        ) {
            self.head_recovery_attempts = self.head_recovery_attempts.saturating_add(1);
        }
    }

    fn build_request(&self, kind: RpcRequestKind) -> Option<Eth2RpcRequest> {
        match kind {
            RpcRequestKind::Status => Some(Eth2RpcRequest::Status(self.local_status_message())),
            RpcRequestKind::Goodbye => Some(Eth2RpcRequest::Goodbye(GOODBYE_REASON_FAULT)),
            RpcRequestKind::MetaData => Some(Eth2RpcRequest::MetaData),
            RpcRequestKind::Ping => Some(Eth2RpcRequest::Ping(0)),
            RpcRequestKind::LightClientBootstrap => Some(Eth2RpcRequest::LightClientBootstrap(
                self.consensus.checkpoint().beacon_root,
            )),
            RpcRequestKind::LightClientUpdatesByRange => self
                .next_updates_by_range_request()
                .map(Eth2RpcRequest::LightClientUpdatesByRange),
            RpcRequestKind::LightClientFinalityUpdate => {
                Some(Eth2RpcRequest::LightClientFinalityUpdate)
            }
            RpcRequestKind::LightClientOptimisticUpdate => {
                Some(Eth2RpcRequest::LightClientOptimisticUpdate)
            }
            RpcRequestKind::BeaconBlocksByRange => self
                .next_history_range_request()
                .map(Eth2RpcRequest::BeaconBlocksByRange),
            RpcRequestKind::BeaconBlocksByRoot => self
                .next_history_root_request()
                .map(Eth2RpcRequest::BeaconBlocksByRoot),
        }
    }

    fn next_post_bootstrap_request_kind(&self, support: PeerRpcSupport) -> Option<RpcRequestKind> {
        let now = Instant::now();
        let head_progression_needed = live_head_progression_needed(
            self.latest_history_sync_target(),
            current_wall_clock_slot(),
        );
        if head_progression_needed
            && let Some(kind) = select_live_head_progression_request_kind(
                support.supports_request(RpcRequestKind::LightClientUpdatesByRange)
                    && self.can_issue_request(RpcRequestKind::LightClientUpdatesByRange)
                    && self.updates_by_range_request_due(now),
                support.supports_request(RpcRequestKind::LightClientOptimisticUpdate)
                    && self.can_issue_request(RpcRequestKind::LightClientOptimisticUpdate)
                    && self.light_client_request_due(
                        RpcRequestKind::LightClientOptimisticUpdate,
                        now,
                        RPC_REQUEST_INTERVAL,
                    ),
                support.supports_request(RpcRequestKind::LightClientFinalityUpdate)
                    && self.can_issue_request(RpcRequestKind::LightClientFinalityUpdate)
                    && self.light_client_request_due(
                        RpcRequestKind::LightClientFinalityUpdate,
                        now,
                        RPC_REQUEST_INTERVAL,
                    ),
            )
        {
            return Some(kind);
        }

        let priority_root_ready = support.supports_request(RpcRequestKind::BeaconBlocksByRoot)
            && self.can_issue_request(RpcRequestKind::BeaconBlocksByRoot)
            && self.next_priority_history_root_request().is_some();
        let range_ready = support.supports_request(RpcRequestKind::BeaconBlocksByRange)
            && self.can_issue_request(RpcRequestKind::BeaconBlocksByRange)
            && self.next_history_range_request().is_some();
        let pending_priority_root =
            self.pending_requests_for_kind(RpcRequestKind::BeaconBlocksByRoot) > 0;
        let pending_range = self.pending_requests_for_kind(RpcRequestKind::BeaconBlocksByRange) > 0;
        let deferred_root_ready = support.supports_request(RpcRequestKind::BeaconBlocksByRoot)
            && self.can_issue_request(RpcRequestKind::BeaconBlocksByRoot)
            && !priority_root_ready
            && self.next_history_root_request().is_some();
        select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
            priority_root_ready,
            range_ready,
            deferred_root_ready,
            updates_ready: support.supports_request(RpcRequestKind::LightClientUpdatesByRange)
                && self.can_issue_request(RpcRequestKind::LightClientUpdatesByRange)
                && self.updates_by_range_request_due(now),
            finality_ready: support.supports_request(RpcRequestKind::LightClientFinalityUpdate)
                && self.can_issue_request(RpcRequestKind::LightClientFinalityUpdate)
                && self.light_client_request_due(
                    RpcRequestKind::LightClientFinalityUpdate,
                    now,
                    FINALITY_UPDATE_POLL_INTERVAL,
                ),
            optimistic_ready: false,
            pending_priority_root,
            pending_range,
            prefer_range_when_both_ready: true,
        })
    }

    fn light_client_request_due(
        &self,
        kind: RpcRequestKind,
        now: Instant,
        interval: Duration,
    ) -> bool {
        light_client_request_due(
            self.last_light_client_request_at.get(&kind).copied(),
            now,
            interval,
        )
    }

    fn updates_by_range_request_due(&self, now: Instant) -> bool {
        let request = match self.next_updates_by_range_request() {
            Some(request) => request,
            None => return false,
        };
        updates_by_range_request_due(
            request,
            sync_committee_period_for_slot(current_wall_clock_slot()),
            self.last_light_client_request_at
                .get(&RpcRequestKind::LightClientUpdatesByRange)
                .copied(),
            now,
        )
    }

    fn local_status_message(&self) -> StatusMessage {
        if let Some(store) = self.consensus.light_client_store() {
            return StatusMessage {
                fork_digest: self.fork_digest,
                finalized_root: store.finalized_header.beacon_root(),
                finalized_epoch: store.finalized_header.beacon.slot / 32,
                head_root: store.optimistic_header.beacon_root(),
                head_slot: store.optimistic_header.beacon.slot,
                earliest_available_slot: store.bootstrap_slot(),
            };
        }

        limited_local_status_message(self.fork_digest)
    }

    fn local_metadata(&self) -> MetaData {
        MetaData::empty()
    }

    fn next_updates_by_range_request(&self) -> Option<LightClientUpdatesByRangeRequest> {
        let store = self.consensus.light_client_store()?;
        let finalized_period = sync_committee_period_for_slot(store.finalized_header.beacon.slot);
        let current_period = sync_committee_period_for_slot(current_wall_clock_slot());
        let start_period = if store.next_sync_committee.is_none() {
            finalized_period
        } else if finalized_period + 1 < current_period {
            finalized_period + 1
        } else {
            return None;
        };
        Some(LightClientUpdatesByRangeRequest {
            start_period,
            count: 1,
        })
    }

    fn latest_history_sync_target(&self) -> Option<HistorySyncTarget> {
        let store = self.consensus.light_client_store()?;
        Some(HistorySyncTarget {
            checkpoint_root: store.checkpoint_root,
            checkpoint_slot: store.bootstrap_slot(),
            finalized_root: store.finalized_header.beacon_root(),
            optimistic_root: store.optimistic_header.beacon_root(),
            optimistic_slot: store.optimistic_header.beacon.slot,
        })
    }

    fn current_history_sync_target(&self) -> Option<HistorySyncTarget> {
        self.active_history_target
            .or_else(|| self.latest_history_sync_target())
    }

    fn refresh_history_sync_target(&mut self) {
        let latest = self.latest_history_sync_target();
        let current = self.active_history_target;
        let current_complete = current
            .and_then(|target| self.canonical_chain_blocks(target))
            .is_some();
        let next = select_history_sync_target(current, latest, current_complete);
        if next != current {
            tracing::debug!(
                previous_target = ?current,
                next_target = ?next,
                current_complete,
                "updated active consensus history sync target"
            );
        }
        self.active_history_target = next;
    }

    fn next_history_root_request(&self) -> Option<Vec<B256>> {
        let pending_roots = self.pending_history_roots();
        self.next_priority_history_root_request_excluding(&pending_roots)
    }

    fn next_priority_history_root_request(&self) -> Option<Vec<B256>> {
        let pending_roots = self.pending_history_roots();
        self.next_priority_history_root_request_excluding(&pending_roots)
    }

    fn next_priority_history_root_request_excluding(
        &self,
        pending_roots: &HashSet<B256>,
    ) -> Option<Vec<B256>> {
        let target = self.current_history_sync_target()?;
        next_forward_history_root_request_for_target_excluding(
            target,
            &self.verified_beacon_blocks,
            pending_roots,
        )
    }

    fn next_history_range_request(&self) -> Option<BeaconBlocksByRangeRequest> {
        let target = self.current_history_sync_target()?;
        let forward_target =
            select_forward_history_range_target(Some(target), self.latest_history_sync_target())?;
        let pending = self.pending_history_ranges();
        self.next_forward_history_range_request_with_pending(forward_target, &pending)
    }

    fn next_forward_history_range_request_with_pending(
        &self,
        target: HistorySyncTarget,
        pending_ranges: &[BeaconBlocksByRangeRequest],
    ) -> Option<BeaconBlocksByRangeRequest> {
        if target.optimistic_slot <= target.checkpoint_slot {
            return None;
        }
        let progress = self.cached_forward_path_progress(target)?;
        let progress = forward_progress_with_pending_ranges(progress, pending_ranges);
        next_forward_history_range_request_for_progress(progress)
    }

    fn cached_forward_path_progress(
        &self,
        target: HistorySyncTarget,
    ) -> Option<CachedForwardPathProgress> {
        let highest_cached_slot = self.checkpoint_forward_highest_cached_slot(target)?;
        Some(CachedForwardPathProgress {
            checkpoint_slot: target.checkpoint_slot,
            target_slot: target.optimistic_slot,
            highest_cached_slot,
        })
    }

    fn record_verified_beacon_block(
        &mut self,
        block: VerifiedBeaconBlock,
        payload: Option<RawRpcResponse>,
    ) -> bool {
        if let Some(payload) = payload {
            self.verified_beacon_block_payloads
                .insert(block.beacon_root, payload);
        }
        let previous = self.verified_beacon_blocks.insert(block.beacon_root, block);
        if previous == Some(block) {
            return false;
        }

        if let Some(previous) = previous {
            let remove_parent = self
                .verified_beacon_block_children
                .get_mut(&previous.parent_root)
                .is_some_and(|children| {
                    children.retain(|child| child.beacon_root != previous.beacon_root);
                    children.is_empty()
                });
            if remove_parent {
                self.verified_beacon_block_children
                    .remove(&previous.parent_root);
            }
        }

        let children = self
            .verified_beacon_block_children
            .entry(block.parent_root)
            .or_default();
        if !children
            .iter()
            .any(|child| child.beacon_root == block.beacon_root)
        {
            children.push(block);
            children.sort_by_key(|child| (child.slot, child.beacon_root));
        }

        true
    }

    fn seed_verified_light_client_headers(&mut self) -> bool {
        let Some(store) = self.consensus.light_client_store() else {
            return false;
        };
        let blocks = verified_beacon_blocks_from_light_client_store(&store);
        let mut inserted = false;
        for block in blocks {
            inserted |= self.record_verified_beacon_block(block, None);
        }
        inserted
    }

    fn cached_verified_beacon_blocks_by_root(&self, roots: &[B256]) -> Vec<RawRpcResponse> {
        cached_beacon_block_payloads_by_root(roots, &self.verified_beacon_block_payloads)
    }

    fn cached_verified_beacon_blocks_by_range(
        &self,
        request: BeaconBlocksByRangeRequest,
    ) -> Vec<RawRpcResponse> {
        let canonical_blocks = self.canonical_serving_blocks();
        cached_beacon_block_payloads_by_range(
            request,
            &canonical_blocks,
            &self.verified_beacon_block_payloads,
        )
    }

    fn cached_verified_light_client_updates_by_range(
        &self,
        request: LightClientUpdatesByRangeRequest,
    ) -> Vec<RawRpcResponse> {
        let payloads = self.consensus.light_client_payloads();
        cached_light_client_update_payloads_by_range(request, &payloads.updates_by_period)
    }

    fn pending_history_roots(&self) -> HashSet<B256> {
        self.pending_history_root_requests
            .values()
            .flat_map(|roots| roots.iter().copied())
            .collect()
    }

    fn pending_history_ranges(&self) -> Vec<BeaconBlocksByRangeRequest> {
        self.pending_history_range_requests
            .values()
            .copied()
            .collect()
    }

    fn materialize_verified_anchor_segments(&mut self) {
        self.materialize_verified_history_segment();
    }

    fn materialize_verified_history_segment(&mut self) {
        let Some(target) = self.current_history_sync_target() else {
            return;
        };
        let Some(store) = self.consensus.light_client_store() else {
            return;
        };
        let Some(materializable_chain) = self.materializable_history_chain(target) else {
            return;
        };
        let chain = materializable_chain.blocks;

        let anchor_records = chain
            .iter()
            .map(|block| crate::AnchorRecord {
                anchor: block.execution_anchor,
                finalized: block.slot <= store.finalized_header.beacon.slot,
                parent_beacon_root: Some(block.parent_root),
            })
            .collect::<Vec<_>>();
        let Some(first_anchor) = anchor_records.first().map(|record| record.anchor) else {
            return;
        };
        let Some(last_anchor) = anchor_records.last().map(|record| record.anchor) else {
            return;
        };
        let replace_end_block = if materializable_chain.reaches_optimistic_head {
            self.consensus
                .highest_anchor_block_from(first_anchor.block_number)
                .map_or(last_anchor.block_number, |existing_end| {
                    existing_end.max(last_anchor.block_number)
                })
        } else {
            last_anchor.block_number
        };

        if let Err(error) = self.consensus.replace_anchor_range(
            first_anchor.block_number,
            replace_end_block,
            anchor_records,
        ) {
            tracing::warn!(%error, "failed to persist verified beacon-block execution anchors");
            return;
        }
        self.refresh_history_sync_target();
    }

    fn canonical_chain_blocks(
        &self,
        target: HistorySyncTarget,
    ) -> Option<Vec<VerifiedBeaconBlock>> {
        self.canonical_chain_blocks_to_root(
            target.checkpoint_root,
            target.checkpoint_slot,
            target.optimistic_root,
        )
    }

    fn materializable_history_chain(
        &self,
        target: HistorySyncTarget,
    ) -> Option<MaterializableHistoryChain> {
        materializable_history_chain(&self.verified_beacon_blocks, target)
    }

    fn canonical_chain_blocks_to_root(
        &self,
        checkpoint_root: B256,
        checkpoint_slot: u64,
        target_root: B256,
    ) -> Option<Vec<VerifiedBeaconBlock>> {
        canonical_chain_blocks_to_root(
            &self.verified_beacon_blocks,
            checkpoint_root,
            checkpoint_slot,
            target_root,
        )
    }

    fn checkpoint_forward_highest_cached_slot(&self, target: HistorySyncTarget) -> Option<u64> {
        let checkpoint_block = *self.verified_beacon_blocks.get(&target.checkpoint_root)?;
        if checkpoint_block.slot != target.checkpoint_slot {
            return None;
        }

        let preferred_roots = cached_target_lineage_roots(&self.verified_beacon_blocks, target);
        let mut current = checkpoint_block;
        while let Some(child) = self.next_checkpoint_forward_child(
            current.beacon_root,
            current.slot,
            target.optimistic_slot,
            &preferred_roots,
        ) {
            current = child;
        }

        Some(current.slot)
    }

    fn serving_forward_chain_blocks(&self) -> Option<Vec<VerifiedBeaconBlock>> {
        let target = self.current_history_sync_target()?;
        self.canonical_chain_blocks_to_root(
            target.checkpoint_root,
            target.checkpoint_slot,
            target.optimistic_root,
        )
        .or_else(|| {
            self.canonical_chain_blocks_to_root(
                target.checkpoint_root,
                target.checkpoint_slot,
                target.finalized_root,
            )
        })
    }

    fn canonical_serving_blocks(&self) -> Vec<VerifiedBeaconBlock> {
        let mut blocks = self.serving_forward_chain_blocks().unwrap_or_default();
        blocks.sort_by_key(|block| (block.slot, block.beacon_root));
        blocks.dedup_by_key(|block| block.beacon_root);
        blocks
    }

    fn next_checkpoint_forward_child(
        &self,
        parent_root: B256,
        parent_slot: u64,
        target_slot: u64,
        preferred_roots: &HashSet<B256>,
    ) -> Option<VerifiedBeaconBlock> {
        select_checkpoint_forward_child(
            self.verified_beacon_block_children
                .get(&parent_root)?
                .iter()
                .copied()
                .filter(|child| child.slot > parent_slot && child.slot <= target_slot),
            preferred_roots,
        )
    }

    fn maybe_force_light_client_store(&mut self) {
        let Some(store) = self.consensus.light_client_store() else {
            return;
        };
        let Some(next_store) = force_update_light_client_store(&store) else {
            return;
        };
        tracing::info!(
            finalized_slot = next_store.finalized_header.beacon.slot,
            optimistic_slot = next_store.optimistic_header.beacon.slot,
            "forced a light-client store update after consensus update timeout"
        );
        if let Err(error) = self
            .consensus
            .replace_verified_light_client_store(next_store)
        {
            tracing::warn!(%error, "failed to persist forced light-client store update");
        }
        self.seed_verified_light_client_headers();
    }

    fn identify_timed_out(&self, peer: PeerId) -> bool {
        self.connected_since
            .get(&peer)
            .map(|connected_at| connected_at.elapsed() >= IDENTIFY_GRACE_PERIOD)
            .unwrap_or(false)
    }

    fn peer_is_dialable(&self, peer: PeerId, bootstrap_needed: bool, now: Instant) -> bool {
        let Some(lifecycle) = self.peer_lifecycle.get(&peer) else {
            return true;
        };
        if lifecycle.ignored_for_run {
            return false;
        }
        if bootstrap_needed && lifecycle.deferred_until_post_bootstrap {
            return false;
        }
        !lifecycle.in_cooldown(now)
    }

    fn peer_priority(
        &self,
        peer: PeerId,
        bootstrap_needed: bool,
        head_progression_needed: bool,
    ) -> i32 {
        let Some(lifecycle) = self.peer_lifecycle.get(&peer) else {
            return 0;
        };
        let support_score = match lifecycle.remembered_support {
            Some(support) if bootstrap_needed && support.supports_bootstrap_sync() => 4,
            Some(support)
                if head_progression_needed && support.supports_light_client_progression() =>
            {
                5
            }
            Some(support) if !bootstrap_needed && support.supports_any_post_bootstrap_work() => 4,
            Some(support) if bootstrap_needed && support.supports_any_post_bootstrap_work() => 3,
            Some(support) if support.status => 2,
            Some(_) => 0,
            None => 1,
        };
        peer_lifecycle_priority(
            lifecycle,
            support_score,
            self.bootnode_peers.contains(&peer),
        )
    }

    fn select_dial_addresses(
        &self,
        peer: PeerId,
        mut addrs: Vec<Multiaddr>,
        bootstrap_needed: bool,
    ) -> Vec<Multiaddr> {
        retain_dial_addresses_for_families(self.config.dial_families, &mut addrs);
        addrs.sort_by(|left, right| {
            self.dial_address_priority(peer, right, bootstrap_needed)
                .cmp(&self.dial_address_priority(peer, left, bootstrap_needed))
                .then_with(|| left.to_string().cmp(&right.to_string()))
        });
        if addrs.len() > MAX_DIAL_ADDRESSES_PER_ATTEMPT {
            addrs.truncate(MAX_DIAL_ADDRESSES_PER_ATTEMPT);
        }
        addrs
    }

    fn dial_address_priority(&self, peer: PeerId, addr: &Multiaddr, bootstrap_needed: bool) -> i32 {
        dial_address_class(addr)
            .map(|class| {
                self.peer_lifecycle
                    .get(&peer)
                    .map(|lifecycle| lifecycle.dial_stats.priority(class, bootstrap_needed))
                    .unwrap_or_else(|| {
                        PeerDialAddressStats::default().priority(class, bootstrap_needed)
                    })
            })
            .unwrap_or(i32::MIN / 2)
    }

    fn mark_peer_ignored_for_run(&mut self, peer: PeerId, reason: String) {
        self.peer_lifecycle
            .entry(peer)
            .or_default()
            .mark_ignored_for_run();
        self.last_peer_policy_event = Some(format!(
            "{} policy=ignore reason={reason}",
            self.peer_context(peer)
        ));
    }

    fn defer_peer_until_post_bootstrap(&mut self, peer: PeerId, reason: String) {
        self.peer_lifecycle
            .entry(peer)
            .or_default()
            .mark_deferred_until_post_bootstrap();
        self.last_peer_policy_event = Some(format!(
            "{} policy=defer_until_post_bootstrap reason={reason}",
            self.peer_context(peer)
        ));
    }

    fn record_transport_backoff(&mut self, peer: PeerId, detail: String) {
        let delay = self
            .peer_lifecycle
            .entry(peer)
            .or_default()
            .record_transport_failure(Instant::now());
        self.last_peer_policy_event = Some(format!(
            "{} policy=transport_backoff delay_secs={} detail={detail}",
            self.peer_context(peer),
            delay.as_secs()
        ));
    }

    fn record_dial_error(&mut self, peer: PeerId, error: &DialError) -> bool {
        let lifecycle = self.peer_lifecycle.entry(peer).or_default();
        let ignored_for_run = record_dial_error_on_lifecycle(lifecycle, error);
        if ignored_for_run {
            let reason = match error {
                DialError::LocalPeerId { address } => {
                    format!("local_peer_id address={address}")
                }
                DialError::WrongPeerId { obtained, address } => {
                    format!("wrong_peer_id obtained={obtained} address={address}")
                }
                _ => "unusable_peer_identity".to_owned(),
            };
            self.last_peer_policy_event = Some(format!(
                "{} policy=ignore_for_run reason={reason}",
                self.peer_context(peer)
            ));
        }
        ignored_for_run
    }

    fn record_peer_disconnect(&mut self, peer: PeerId, detail: String) {
        let delay = self
            .peer_lifecycle
            .entry(peer)
            .or_default()
            .record_disconnect(Instant::now());
        self.last_peer_policy_event = Some(format!(
            "{} policy=disconnect_backoff delay_secs={} detail={detail}",
            self.peer_context(peer),
            delay.as_secs()
        ));
    }

    fn record_peer_success(&mut self, peer: PeerId, kind: RpcRequestKind) {
        self.reset_peer_failure(peer, kind);
        self.peer_lifecycle
            .entry(peer)
            .or_default()
            .record_success(kind);
    }

    fn record_peer_failure(&mut self, peer: PeerId, kind: RpcRequestKind) -> u32 {
        let failures = self.peer_failures.entry(peer).or_default().increment(kind);
        if let Some(delay) = self
            .peer_lifecycle
            .entry(peer)
            .or_default()
            .record_rpc_failure(kind, Instant::now())
        {
            self.last_peer_policy_event = Some(format!(
                "{} policy=rpc_backoff request={} delay_secs={}",
                self.peer_context(peer),
                kind.as_str(),
                delay.as_secs()
            ));
        }
        failures
    }

    fn record_unusable_history_response(
        &mut self,
        peer: PeerId,
        kind: RpcRequestKind,
        detail: String,
    ) {
        self.request_failures.increment(kind);
        let peer_failures = self.record_peer_failure(peer, kind);
        self.last_rpc_failure = Some(format!(
            "{} request={} {detail}",
            self.peer_context(peer),
            kind.as_str()
        ));
        tracing::info!(
            %peer,
            request = kind.as_str(),
            failures = peer_failures,
            %detail,
            "consensus history RPC response did not contain usable beacon blocks"
        );
    }

    fn record_invalid_light_client_response(
        &mut self,
        peer: PeerId,
        kind: RpcRequestKind,
        detail: String,
    ) {
        self.request_failures.increment(kind);
        let failures = self.record_peer_failure(peer, kind);
        self.last_rpc_failure = Some(format!(
            "{} request={} invalid_response={detail}",
            self.peer_context(peer),
            kind.as_str()
        ));
        tracing::info!(
            %peer,
            request = kind.as_str(),
            failures,
            %detail,
            "penalized consensus peer for an invalid light-client response"
        );
    }

    fn disconnect_faulty_history_peer(
        &mut self,
        peer: PeerId,
        kind: RpcRequestKind,
        detail: String,
    ) {
        self.request_failures.increment(kind);
        let peer_failures = self.record_peer_failure(peer, kind);
        self.peer_lifecycle
            .entry(peer)
            .or_default()
            .mark_ignored_for_run();
        self.last_rpc_failure = Some(format!(
            "{} request={} {detail}",
            self.peer_context(peer),
            kind.as_str()
        ));
        tracing::info!(
            %peer,
            request = kind.as_str(),
            failures = peer_failures,
            %detail,
            "disconnecting and ignoring consensus peer after invalid beacon history response"
        );
        self.disconnect_peer_with_reason(peer, GOODBYE_REASON_FAULT);
    }

    fn reset_peer_failure(&mut self, peer: PeerId, kind: RpcRequestKind) {
        if let Some(failures) = self.peer_failures.get_mut(&peer) {
            failures.reset(kind);
        }
    }

    fn disconnect_peer_with_reason(&mut self, peer: PeerId, reason: u64) {
        if self.is_request_pending(peer, RpcRequestKind::Goodbye) {
            return;
        }
        self.closing_peers.insert(peer);
        let supports_goodbye = self
            .peer_support
            .get(&peer)
            .copied()
            .map(|support| support.supports_request(RpcRequestKind::Goodbye))
            .unwrap_or(false);
        if !supports_goodbye || !self.connected_peers.contains(&peer) {
            self.disconnect_now(peer);
            return;
        }

        let request_id = self
            .swarm
            .behaviour_mut()
            .goodbye_rpc
            .inner
            .send_request(&peer, Eth2RpcRequest::Goodbye(reason));
        self.record_p2p_upload_payload(8);
        self.pending_requests.insert(
            PendingRequestKey {
                kind: RpcRequestKind::Goodbye,
                request_id,
            },
            peer,
        );
        self.pending_peer_kinds
            .insert((peer, RpcRequestKind::Goodbye));
    }

    fn disconnect_now(&mut self, peer: PeerId) {
        self.closing_peers.insert(peer);
        let _ = self.swarm.disconnect_peer_id(peer);
    }

    fn take_pending_history_root_request(
        &mut self,
        request_id: Eth2OutboundRequestId,
    ) -> Vec<B256> {
        self.pending_history_root_requests
            .remove(&PendingRequestKey {
                kind: RpcRequestKind::BeaconBlocksByRoot,
                request_id,
            })
            .unwrap_or_default()
    }

    fn take_pending_history_range_request(
        &mut self,
        request_id: Eth2OutboundRequestId,
    ) -> Option<BeaconBlocksByRangeRequest> {
        self.pending_history_range_requests
            .remove(&PendingRequestKey {
                kind: RpcRequestKind::BeaconBlocksByRange,
                request_id,
            })
    }

    fn take_pending_request(
        &mut self,
        kind: RpcRequestKind,
        request_id: Eth2OutboundRequestId,
    ) -> Option<PeerId> {
        let peer = self
            .pending_requests
            .remove(&PendingRequestKey { kind, request_id })?;
        self.pending_peer_kinds.remove(&(peer, kind));
        Some(peer)
    }

    fn clear_pending_requests_for_peer(&mut self, peer: PeerId) {
        let stale = self
            .pending_requests
            .iter()
            .filter_map(|(key, pending_peer)| (*pending_peer == peer).then_some(*key))
            .collect::<Vec<_>>();
        for key in stale {
            let _ = self.take_pending_request(key.kind, key.request_id);
            self.pending_history_root_requests.remove(&key);
            self.pending_history_range_requests.remove(&key);
        }
    }

    fn clear_peer_state(&mut self, peer: PeerId) {
        self.connected_since.remove(&peer);
        self.peer_endpoints.remove(&peer);
        self.peer_support.remove(&peer);
        self.peer_failures.remove(&peer);
        self.inbound_status_peers.remove(&peer);
        self.status_peers.remove(&peer);
        self.metadata_peers.remove(&peer);
        self.ping_peers.remove(&peer);
        self.last_status_success_at.remove(&peer);
        self.last_ping_success_at.remove(&peer);
        self.bootstrap_peers.remove(&peer);
        self.updates_by_range_peers.remove(&peer);
        self.finality_update_peers.remove(&peer);
        self.optimistic_update_peers.remove(&peer);
        self.beacon_blocks_by_range_peers.remove(&peer);
        self.beacon_blocks_by_root_peers.remove(&peer);
    }

    fn is_request_pending(&self, peer: PeerId, kind: RpcRequestKind) -> bool {
        self.pending_peer_kinds.contains(&(peer, kind))
    }

    fn is_request_satisfied(&self, peer: PeerId, kind: RpcRequestKind) -> bool {
        match kind {
            RpcRequestKind::Status => {
                self.status_peers.contains(&peer)
                    && self
                        .last_status_success_at
                        .get(&peer)
                        .is_some_and(|last| last.elapsed() < STATUS_MAINTENANCE_INTERVAL)
            }
            RpcRequestKind::Goodbye => false,
            RpcRequestKind::MetaData => self.metadata_peers.contains(&peer),
            RpcRequestKind::Ping => {
                self.ping_peers.contains(&peer)
                    && self
                        .last_ping_success_at
                        .get(&peer)
                        .is_some_and(|last| last.elapsed() < PING_MAINTENANCE_INTERVAL)
            }
            RpcRequestKind::LightClientBootstrap
            | RpcRequestKind::LightClientUpdatesByRange
            | RpcRequestKind::LightClientFinalityUpdate
            | RpcRequestKind::LightClientOptimisticUpdate
            | RpcRequestKind::BeaconBlocksByRange
            | RpcRequestKind::BeaconBlocksByRoot => false,
        }
    }

    fn pending_requests_for_kind(&self, kind: RpcRequestKind) -> usize {
        self.pending_requests
            .keys()
            .filter(|key| key.kind == kind)
            .count()
    }

    fn pending_requests_for_peer(&self, peer: PeerId) -> usize {
        self.pending_peer_kinds
            .iter()
            .filter(|(pending_peer, _)| *pending_peer == peer)
            .count()
    }

    fn peer_context(&self, peer: PeerId) -> String {
        match self.peer_endpoints.get(&peer) {
            Some(endpoint) => format!("peer={peer} endpoint={endpoint}"),
            None => format!("peer={peer}"),
        }
    }

    fn can_issue_request(&self, kind: RpcRequestKind) -> bool {
        self.pending_requests_for_kind(kind) < max_concurrent_requests_for_kind(kind)
    }

    fn refresh_status(&mut self) {
        let table_entries = self.discv5.table_entries_enr();
        let light_client = self.consensus.light_client_status();
        let checkpoint = self.consensus.checkpoint();
        let anchors = self.consensus.chain_anchors();
        let anchor_coverage = self.consensus.anchor_coverage();
        let identified_peers = self.peer_support.len();
        let now = Instant::now();
        let preferred_peers = self
            .peer_lifecycle
            .values()
            .filter(|lifecycle| lifecycle.preferred())
            .count();
        let cooldown_peers = self
            .peer_lifecycle
            .values()
            .filter(|lifecycle| lifecycle.in_cooldown(now))
            .count();
        let ignored_peers = self
            .peer_lifecycle
            .values()
            .filter(|lifecycle| lifecycle.ignored_for_run)
            .count();
        let deferred_until_post_bootstrap_peers = self
            .peer_lifecycle
            .values()
            .filter(|lifecycle| lifecycle.deferred_until_post_bootstrap)
            .count();
        let status_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.status)
            .count();
        let metadata_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.metadata)
            .count();
        let bootstrap_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.light_client_bootstrap)
            .count();
        let updates_by_range_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.light_client_updates_by_range)
            .count();
        let finality_update_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.light_client_finality_update)
            .count();
        let optimistic_update_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.light_client_optimistic_update)
            .count();
        let beacon_blocks_by_range_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.beacon_blocks_by_range)
            .count();
        let beacon_blocks_by_root_capable_peers = self
            .peer_support
            .values()
            .filter(|support| support.beacon_blocks_by_root)
            .count();
        let pending_forward_ranges = self.pending_history_range_requests.len();
        let p2p_download = self.p2p_download_metrics.snapshot(now);
        let p2p_upload = self.p2p_upload_metrics.snapshot(now);
        let current_slot = current_wall_clock_slot();
        let optimistic_head_slot = anchors.optimistic_head.map(|anchor| anchor.beacon_slot);
        let optimistic_head_lag_slots =
            optimistic_head_slot.map(|slot| crate::optimistic_head_lag_slots(current_slot, slot));
        let head_recovery_active =
            live_head_progression_needed(self.latest_history_sync_target(), current_slot);
        let finality_update_gossip_mesh_peers = self
            .swarm
            .behaviour()
            .gossip
            .mesh_peers(&self.gossip_topics.finality_update.hash())
            .count();
        let optimistic_update_gossip_mesh_peers = self
            .swarm
            .behaviour()
            .gossip
            .mesh_peers(&self.gossip_topics.optimistic_update.hash())
            .count();
        let status = ConsensusNetworkStatus {
            local_enr: Some(self.discv5.local_enr().to_base64()),
            local_node_id: Some(self.discv5.local_enr().node_id().to_string()),
            discovery_port: self.config.discovery_port,
            p2p_port: self.config.p2p_port,
            local_peer_id: Some(self.swarm.local_peer_id().to_string()),
            max_peers: self.config.max_peers,
            bootnode_count: self.bootnode_count,
            discovered_peers: self.observed.len(),
            dialable_peers: self.dialable_peers.len(),
            routing_table_peers: table_entries.len(),
            active_sessions: self.discv5.connected_peers(),
            connected_peer_sessions: self.connected_peers.len(),
            dialing_peer_sessions: self.dialing_peers.len(),
            preferred_peers,
            cooldown_peers,
            ignored_peers,
            deferred_until_post_bootstrap_peers,
            identified_peers,
            status_capable_peers,
            metadata_capable_peers,
            bootstrap_capable_peers,
            updates_by_range_capable_peers,
            finality_update_capable_peers,
            optimistic_update_capable_peers,
            beacon_blocks_by_range_capable_peers,
            beacon_blocks_by_root_capable_peers,
            status_peers: self.status_peers.len(),
            metadata_peers: self.metadata_peers.len(),
            bootstrap_peers: self.bootstrap_peers.len(),
            updates_by_range_peers: self.updates_by_range_peers.len(),
            finality_update_peers: self.finality_update_peers.len(),
            optimistic_update_peers: self.optimistic_update_peers.len(),
            beacon_blocks_by_range_peers: self.beacon_blocks_by_range_peers.len(),
            beacon_blocks_by_root_peers: self.beacon_blocks_by_root_peers.len(),
            pending_rpc_requests: self.pending_requests.len(),
            pending_status_requests: self.pending_requests_for_kind(RpcRequestKind::Status),
            pending_metadata_requests: self.pending_requests_for_kind(RpcRequestKind::MetaData),
            pending_bootstrap_requests: self
                .pending_requests_for_kind(RpcRequestKind::LightClientBootstrap),
            pending_updates_by_range_requests: self
                .pending_requests_for_kind(RpcRequestKind::LightClientUpdatesByRange),
            pending_finality_update_requests: self
                .pending_requests_for_kind(RpcRequestKind::LightClientFinalityUpdate),
            pending_optimistic_update_requests: self
                .pending_requests_for_kind(RpcRequestKind::LightClientOptimisticUpdate),
            pending_beacon_blocks_by_range_requests: self
                .pending_requests_for_kind(RpcRequestKind::BeaconBlocksByRange),
            pending_forward_beacon_blocks_by_range_requests: pending_forward_ranges,
            pending_beacon_blocks_by_root_requests: self
                .pending_requests_for_kind(RpcRequestKind::BeaconBlocksByRoot),
            status_request_failures: self.request_failures.status,
            metadata_request_failures: self.request_failures.metadata,
            bootstrap_request_failures: self.request_failures.bootstrap,
            updates_by_range_request_failures: self.request_failures.updates_by_range,
            finality_update_request_failures: self.request_failures.finality_update,
            optimistic_update_request_failures: self.request_failures.optimistic_update,
            beacon_blocks_by_range_request_failures: self.request_failures.beacon_blocks_by_range,
            beacon_blocks_by_root_request_failures: self.request_failures.beacon_blocks_by_root,
            gossip_subscriptions: self.gossip_subscriptions.len(),
            finality_update_gossip_mesh_peers,
            optimistic_update_gossip_mesh_peers,
            finality_update_gossip_messages: self.gossip_counts.finality_update,
            optimistic_update_gossip_messages: self.gossip_counts.optimistic_update,
            gossip_decode_failures: self.gossip_counts.decode_failures,
            current_slot,
            optimistic_head_slot,
            optimistic_head_lag_slots,
            head_recovery_active,
            head_recovery_attempts: self.head_recovery_attempts,
            p2p_download_bytes_per_sec: p2p_download.bytes_per_sec,
            p2p_upload_bytes_per_sec: p2p_upload.bytes_per_sec,
            p2p_downloaded_payload_bytes: p2p_download.total_payload_bytes,
            p2p_uploaded_payload_bytes: p2p_upload.total_payload_bytes,
            last_connection_event: self.last_connection_event.clone(),
            last_identify_event: self.last_identify_event.clone(),
            last_peer_policy_event: self.last_peer_policy_event.clone(),
            last_rpc_failure: self.last_rpc_failure.clone(),
            last_response_send_failure: self.last_response_send_failure.clone(),
        };
        let mut sync_status = self.sync_status.lock().unwrap();
        sync_status.checkpoint = Some(checkpoint);
        sync_status.consensus_network = Some(status);
        sync_status.consensus_light_client = (!light_client.is_empty()).then_some(light_client);
        sync_status.optimistic_execution_head = anchors.optimistic_head;
        sync_status.consensus_current_slot = Some(current_slot);
        sync_status.consensus_head_lag_slots = optimistic_head_lag_slots;
        sync_status.consensus_head_fresh = Some(
            optimistic_head_slot
                .is_some_and(|slot| crate::optimistic_head_is_fresh_at(current_slot, slot)),
        );
        sync_status.consensus_status_updated_at_unix_ms = Some(unix_time_millis());
        if sync_status.consensus_head_fresh == Some(false)
            && sync_status.node_state == logex_types::NodeState::Synced
        {
            sync_status.node_state = logex_types::NodeState::WaitingForConsensus;
            sync_status.syncing = false;
        }
        sync_status.finalized_execution_head = anchors.finalized_head;
        sync_status.materialized_execution_floor = anchor_coverage.floor;
        sync_status.materialized_execution_ceiling = anchor_coverage.ceiling;
        sync_status.materialized_execution_anchor_count = anchor_coverage.count;
        sync_status.materialized_execution_anchor_gap_count = anchor_coverage.gap_count;
        if let Some(anchor) = anchors.optimistic_head {
            sync_status.target_block = sync_status.target_block.max(anchor.block_number);
        }
    }

    fn persist_known_peers(&mut self) -> Result<(), ConsensusNetworkError> {
        let bootstrap_needed = self.consensus.light_client_status().bootstrap.is_none();
        let head_progression_needed = !bootstrap_needed
            && live_head_progression_needed(
                self.latest_history_sync_target(),
                current_wall_clock_slot(),
            );
        let mut peers = self
            .discv5
            .table_entries_enr()
            .into_iter()
            .filter(|enr| enr_is_relevant_consensus_peer(enr, &self.fork_digest))
            .filter(|enr| {
                enr.tcp4().is_some()
                    || enr.tcp6().is_some()
                    || enr_quic4(enr).is_some()
                    || enr_quic6(enr).is_some()
            })
            .map(|enr| {
                let peer_state = peer_id_from_enr(&enr)
                    .ok()
                    .and_then(|peer| self.peer_lifecycle.get(&peer).cloned());
                PersistedPeer {
                    enr: enr.to_base64(),
                    support: peer_state
                        .as_ref()
                        .and_then(PeerLifecycleState::persisted_support),
                    status_successes: peer_state
                        .as_ref()
                        .map(|state| state.status_successes)
                        .unwrap_or_default(),
                    bootstrap_successes: peer_state
                        .as_ref()
                        .map(|state| state.bootstrap_successes)
                        .unwrap_or_default(),
                    useful_successes: peer_state
                        .as_ref()
                        .map(|state| state.useful_successes)
                        .unwrap_or_default(),
                    dial_stats: peer_state
                        .as_ref()
                        .map(|state| state.dial_stats)
                        .unwrap_or_default(),
                }
            })
            .filter(|peer| peer.support.is_none_or(|support| support.status))
            .collect::<Vec<_>>();

        peers.sort_by(|left, right| {
            let left_priority = left
                .enr
                .parse::<Enr>()
                .ok()
                .and_then(|enr| peer_id_from_enr(&enr).ok())
                .map(|peer| self.peer_priority(peer, bootstrap_needed, head_progression_needed))
                .unwrap_or_default();
            let right_priority = right
                .enr
                .parse::<Enr>()
                .ok()
                .and_then(|enr| peer_id_from_enr(&enr).ok())
                .map(|peer| self.peer_priority(peer, bootstrap_needed, head_progression_needed))
                .unwrap_or_default();
            right_priority
                .cmp(&left_priority)
                .then_with(|| left.enr.cmp(&right.enr))
        });
        if peers.len() > MAX_PERSISTED_KNOWN_PEERS {
            peers.truncate(MAX_PERSISTED_KNOWN_PEERS);
        }

        if peers == self.last_persisted {
            return Ok(());
        }

        persist_known_peers(&self.known_peers_path, &peers)?;
        self.last_persisted = peers;
        Ok(())
    }
}

struct LocalEnrConfig<'a> {
    fork_id: &'a [u8],
    next_fork_digest: &'a [u8; 4],
    custody_group_count: u64,
    bind_ip: IpAddr,
    external_ip: Option<IpAddr>,
    discovery_port: u16,
    p2p_port: u16,
}

fn build_local_enr(enr_key: &CombinedKey, config: LocalEnrConfig<'_>) -> Enr {
    let custody_group_count = trim_big_endian_u64(config.custody_group_count);
    let mut builder = Enr::builder();
    configure_local_enr_endpoints(
        &mut builder,
        config.bind_ip,
        config.external_ip,
        config.discovery_port,
        config.p2p_port,
    );
    builder
        .add_value("eth2", &config.fork_id)
        .add_value("nfd", config.next_fork_digest)
        .add_value("cgc", &custody_group_count)
        .add_value("attnets", &ATTESTATION_SUBNET_BITFIELD)
        .add_value("syncnets", &SYNCNET_BITFIELD);
    builder
        .build(enr_key)
        .expect("local consensus ENR should always be constructible")
}

fn configure_local_enr_endpoints(
    builder: &mut discv5::enr::Builder<CombinedKey>,
    bind_ip: IpAddr,
    external_ip: Option<IpAddr>,
    discovery_port: u16,
    p2p_port: u16,
) {
    let family_ip = external_ip.unwrap_or(bind_ip);
    match family_ip {
        IpAddr::V4(ip) => {
            if !ip.is_unspecified() {
                builder.ip4(ip);
            }
            builder.udp4(discovery_port).tcp4(p2p_port);
            if discovery_port != p2p_port {
                builder.add_value("quic", &p2p_port);
            }
        }
        IpAddr::V6(ip) => {
            if !ip.is_unspecified() {
                builder.ip6(ip);
            }
            builder.udp6(discovery_port).tcp6(p2p_port);
            if discovery_port != p2p_port {
                builder.add_value("quic6", &p2p_port);
            }
        }
    }
}

fn multiaddr_bind_ip(ip: IpAddr) -> Multiaddr {
    match ip {
        IpAddr::V4(ip) => Multiaddr::empty().with(Protocol::Ip4(ip)),
        IpAddr::V6(ip) => Multiaddr::empty().with(Protocol::Ip6(ip)),
    }
}

fn trim_big_endian_u64(value: u64) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    let first_non_zero = bytes
        .iter()
        .position(|byte| *byte != 0)
        .unwrap_or(bytes.len());
    bytes[first_non_zero..].to_vec()
}

fn build_libp2p_keypair(enr_key: &CombinedKey) -> Result<identity::Keypair, ConsensusNetworkError> {
    let mut secret_bytes = enr_key.encode();
    let secret = identity::secp256k1::SecretKey::try_from_bytes(&mut secret_bytes)
        .map_err(|error| ConsensusNetworkError::Libp2pIdentity(error.to_string()))?;
    let keypair = identity::secp256k1::Keypair::from(secret);
    Ok(identity::Keypair::from(keypair))
}

fn build_rpc_swarm(
    keypair: identity::Keypair,
    max_peers: usize,
) -> Result<Swarm<ConsensusBehaviour>, ConsensusNetworkError> {
    let public_key = keypair.public();
    let transport = build_rpc_transport(&keypair)?;
    SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_other_transport(|_| transport)
        .map_err(|error| ConsensusNetworkError::ConstructRpcTransport(error.to_string()))?
        .with_behaviour(move |_| {
            let gossip = build_gossip_behaviour()?;
            let identify = identify::Behaviour::new(
                identify::Config::new(IDENTIFY_PROTOCOL_VERSION.into(), public_key.clone())
                    .with_agent_version(IDENTIFY_AGENT_VERSION.to_owned())
                    .with_cache_size(0),
            );
            Ok(ConsensusBehaviour {
                connection_limits: build_connection_limits(max_peers),
                identify,
                gossip,
                status_rpc: StatusRpcBehaviour {
                    inner: build_status_behaviour(),
                },
                goodbye_rpc: GoodbyeRpcBehaviour {
                    inner: build_goodbye_behaviour(),
                },
                metadata_rpc: MetadataRpcBehaviour {
                    inner: build_metadata_behaviour(),
                },
                ping_rpc: PingRpcBehaviour {
                    inner: build_ping_behaviour(),
                },
                light_client_bootstrap_rpc: LightClientBootstrapRpcBehaviour {
                    inner: build_light_client_bootstrap_behaviour(),
                },
                light_client_updates_by_range_rpc: LightClientUpdatesByRangeRpcBehaviour {
                    inner: build_light_client_updates_by_range_behaviour(),
                },
                light_client_finality_update_rpc: LightClientFinalityUpdateRpcBehaviour {
                    inner: build_light_client_finality_update_behaviour(),
                },
                light_client_optimistic_update_rpc: LightClientOptimisticUpdateRpcBehaviour {
                    inner: build_light_client_optimistic_update_behaviour(),
                },
                beacon_blocks_by_range_rpc: BeaconBlocksByRangeRpcBehaviour {
                    inner: build_beacon_blocks_by_range_behaviour(),
                },
                beacon_blocks_by_root_rpc: BeaconBlocksByRootRpcBehaviour {
                    inner: build_beacon_blocks_by_root_behaviour(),
                },
            })
        })
        .map_err(|error| ConsensusNetworkError::ConstructRpcTransport(error.to_string()))
        .map(|builder| {
            builder
                .with_swarm_config(|config| {
                    config
                        .with_idle_connection_timeout(Duration::from_secs(10))
                        .with_dial_concurrency_factor(
                            NonZeroU8::new(1).expect("dial concurrency factor is non-zero"),
                        )
                })
                .build()
        })
}

fn build_connection_limits(max_peers: usize) -> libp2p::connection_limits::Behaviour {
    let mut limits = libp2p::connection_limits::ConnectionLimits::default()
        .with_max_pending_incoming(Some(5))
        .with_max_pending_outgoing(Some(16))
        .with_max_established_per_peer(Some(1));
    if max_peers > 0 {
        let max_peers = u32::try_from(max_peers).unwrap_or(u32::MAX);
        limits = limits
            .with_max_established_incoming(Some(max_peers.saturating_mul(9).div_ceil(10)))
            .with_max_established_outgoing(Some(max_peers.saturating_mul(11).div_ceil(10)))
            .with_max_established(Some(max_peers.saturating_mul(13).div_ceil(10)));
    }
    libp2p::connection_limits::Behaviour::new(limits)
}

fn build_rpc_transport(
    keypair: &identity::Keypair,
) -> Result<Boxed<(PeerId, StreamMuxerBox)>, ConsensusNetworkError> {
    let mut mplex_config = mplex::Config::new();
    mplex_config.set_max_buffer_size(256);
    mplex_config.set_max_buffer_behaviour(mplex::MaxBufferBehaviour::Block);

    let tcp_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(libp2p::core::upgrade::Version::V1)
        .authenticate(
            noise::Config::new(keypair)
                .map_err(|error| ConsensusNetworkError::ConstructRpcTransport(error.to_string()))?,
        )
        .multiplex(libp2p::core::upgrade::SelectUpgrade::new(
            yamux::Config::default(),
            mplex_config,
        ))
        .timeout(Duration::from_secs(10));

    let quic_transport = libp2p::quic::tokio::Transport::new(libp2p::quic::Config::new(keypair));
    let transport = tcp_transport
        .or_transport(quic_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        });

    libp2p::dns::tokio::Transport::system(transport)
        .map(Transport::boxed)
        .map_err(|error| ConsensusNetworkError::ConstructRpcTransport(error.to_string()))
}

fn build_gossip_behaviour() -> Result<gossipsub::Behaviour, ConsensusNetworkError> {
    let config = gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::Anonymous)
        .validate_messages()
        .max_transmit_size(GOSSIP_MAX_TRANSMIT_SIZE)
        .message_id_fn(eth2_message_id)
        .build()
        .map_err(|error| ConsensusNetworkError::ConstructGossip(error.to_string()))?;
    gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Anonymous, config)
        .map_err(|error| ConsensusNetworkError::ConstructGossip(error.to_string()))
}

fn build_gossip_topics(fork_digest: [u8; 4]) -> ConsensusGossipTopics {
    let fork_digest = hex::encode(fork_digest);
    ConsensusGossipTopics {
        finality_update: gossipsub::IdentTopic::new(format!(
            "/eth2/{fork_digest}/{LIGHT_CLIENT_FINALITY_UPDATE_TOPIC_NAME}/{GOSSIP_ENCODING_NAME}"
        )),
        optimistic_update: gossipsub::IdentTopic::new(format!(
            "/eth2/{fork_digest}/{LIGHT_CLIENT_OPTIMISTIC_UPDATE_TOPIC_NAME}/{GOSSIP_ENCODING_NAME}"
        )),
    }
}

fn eth2_message_id(message: &gossipsub::Message) -> gossipsub::MessageId {
    let topic = message.topic.as_str().as_bytes();
    let mut hasher = Sha256::new();
    hasher.update(MESSAGE_DOMAIN_VALID_SNAPPY);
    hasher.update(usize_to_u64(topic.len()).to_le_bytes());
    hasher.update(topic);
    hasher.update(&message.data);
    let digest = hasher.finalize();
    gossipsub::MessageId::from(digest[..20].to_vec())
}

fn decode_gossip_payload(payload: &[u8]) -> Option<Vec<u8>> {
    let decompressed_len = snap::raw::decompress_len(payload).ok()?;
    if decompressed_len > GOSSIP_MAX_TRANSMIT_SIZE {
        return None;
    }
    snap::raw::Decoder::new().decompress_vec(payload).ok()
}

fn discovery_secret_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join(CONSENSUS_STATE_DIR)
        .join(DISCOVERY_SECRET_FILE)
}

fn known_peers_path(data_dir: &Path) -> PathBuf {
    data_dir.join(CONSENSUS_STATE_DIR).join(KNOWN_PEERS_FILE)
}

fn observe_dialable_peer(peers: &mut HashMap<PeerId, Vec<Multiaddr>>, enr: &Enr) -> Option<PeerId> {
    if let Some((peer_id, addrs)) = enr_multiaddrs(enr) {
        peers.insert(peer_id, addrs);
        return Some(peer_id);
    }
    None
}

fn observe_dialable_peer_for_families(
    peers: &mut HashMap<PeerId, Vec<Multiaddr>>,
    families: ConsensusDialAddressFamilies,
    enr: &Enr,
) -> Option<PeerId> {
    let (peer_id, mut addrs) = enr_multiaddrs(enr)?;
    retain_dial_addresses_for_families(families, &mut addrs);
    if addrs.is_empty() {
        return None;
    }
    peers.insert(peer_id, addrs);
    Some(peer_id)
}

fn enr_has_discv5_endpoint_for_families(families: ConsensusDialAddressFamilies, enr: &Enr) -> bool {
    (families.ipv4 && enr.ip4().is_some() && enr.udp4().is_some())
        || (families.ipv6 && enr.ip6().is_some() && enr.udp6().is_some())
}

fn load_or_create_secret_key(secret_key_path: &Path) -> Result<CombinedKey, ConsensusNetworkError> {
    match secret_key_path.try_exists() {
        Ok(true) => {
            let contents = fs::read_to_string(secret_key_path).map_err(|source| {
                ConsensusNetworkError::ReadSecret {
                    path: secret_key_path.to_path_buf(),
                    source,
                }
            })?;
            let hex_key = contents.trim().trim_start_matches("0x");
            let mut bytes =
                hex::decode(hex_key).map_err(|error| ConsensusNetworkError::ParseSecret {
                    path: secret_key_path.to_path_buf(),
                    message: error.to_string(),
                })?;
            CombinedKey::secp256k1_from_bytes(&mut bytes).map_err(|error| {
                ConsensusNetworkError::ParseSecret {
                    path: secret_key_path.to_path_buf(),
                    message: error.to_string(),
                }
            })
        }
        Ok(false) => {
            if let Some(dir) = secret_key_path.parent() {
                fs::create_dir_all(dir).map_err(|source| ConsensusNetworkError::PersistSecret {
                    path: secret_key_path.to_path_buf(),
                    source,
                })?;
            }

            let key = CombinedKey::generate_secp256k1();
            fs::write(secret_key_path, hex::encode(key.encode())).map_err(|source| {
                ConsensusNetworkError::PersistSecret {
                    path: secret_key_path.to_path_buf(),
                    source,
                }
            })?;
            Ok(key)
        }
        Err(source) => Err(ConsensusNetworkError::ReadSecret {
            path: secret_key_path.to_path_buf(),
            source,
        }),
    }
}

fn load_known_peers(path: &Path) -> Result<Vec<PersistedPeer>, ConsensusNetworkError> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents =
        fs::read_to_string(path).map_err(|source| ConsensusNetworkError::ReadKnownPeers {
            path: path.to_path_buf(),
            source,
        })?;
    serde_json::from_str(&contents).map_err(|error| ConsensusNetworkError::ParseKnownPeers {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

fn persist_known_peers(path: &Path, peers: &[PersistedPeer]) -> Result<(), ConsensusNetworkError> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|source| ConsensusNetworkError::PersistKnownPeers {
            path: path.to_path_buf(),
            source,
        })?;
    }

    let json = serde_json::to_vec_pretty(peers).map_err(|error| {
        ConsensusNetworkError::ParseKnownPeers {
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    })?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json).map_err(|source| ConsensusNetworkError::PersistKnownPeers {
        path: tmp.clone(),
        source,
    })?;
    fs::rename(&tmp, path).map_err(|source| ConsensusNetworkError::PersistKnownPeers {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn mainnet_bootnodes() -> Result<Vec<Enr>, ConsensusNetworkError> {
    MAINNET_BOOTNODES
        .iter()
        .map(|enr| {
            enr.parse::<Enr>()
                .map_err(|_| ConsensusNetworkError::InvalidBootnode((*enr).to_string()))
        })
        .collect()
}

fn enr_fork_digest(enr: &Enr) -> Option<[u8; 4]> {
    enr.get_decodable::<Bytes>("eth2")
        .and_then(Result::ok)
        .and_then(|fork_id| {
            fork_id
                .get(0..4)
                .and_then(|fork_digest| fork_digest.try_into().ok())
        })
}

fn enr_matches_fork_digest(enr: &Enr, expected_fork_digest: &[u8; 4]) -> bool {
    let Some(remote_fork_digest) = enr_fork_digest(enr) else {
        return false;
    };
    if &remote_fork_digest == expected_fork_digest {
        return true;
    }

    let Some(remote_version) =
        MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_digest(remote_fork_digest)
    else {
        return false;
    };
    let Some(expected_version) =
        MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_digest(*expected_fork_digest)
    else {
        return false;
    };

    remote_version == expected_version || (remote_version[0] >= 0x05 && expected_version[0] >= 0x05)
}

#[allow(deprecated)]
fn enr_is_opstack_peer(enr: &Enr) -> bool {
    enr.get("opstack").is_some()
}

fn enr_is_relevant_consensus_peer(enr: &Enr, expected_fork_digest: &[u8; 4]) -> bool {
    enr_matches_fork_digest(enr, expected_fork_digest) && !enr_is_opstack_peer(enr)
}

fn enr_multiaddrs(enr: &Enr) -> Option<(PeerId, Vec<Multiaddr>)> {
    let peer_id = match peer_id_from_enr(enr) {
        Ok(peer_id) => peer_id,
        Err(error) => {
            tracing::debug!(%error, enr = %enr, "failed to derive libp2p peer id from consensus ENR");
            return None;
        }
    };

    let mut addrs = Vec::new();
    if let (Some(ip), Some(port)) = (enr.ip4(), enr_quic4(enr)) {
        addrs.push(multiaddr_from_ip_quic(IpAddr::V4(ip), port, peer_id));
    }
    if let (Some(ip), Some(port)) = (enr.ip6(), enr_quic6(enr)) {
        addrs.push(multiaddr_from_ip_quic(IpAddr::V6(ip), port, peer_id));
    }
    if let (Some(ip), Some(port)) = (enr.ip4(), enr.tcp4()) {
        addrs.push(multiaddr_from_ip(IpAddr::V4(ip), port, peer_id));
    }
    if let (Some(ip), Some(port)) = (enr.ip6(), enr.tcp6()) {
        addrs.push(multiaddr_from_ip(IpAddr::V6(ip), port, peer_id));
    }
    if addrs.is_empty() {
        return None;
    }

    Some((peer_id, addrs))
}

fn peer_id_from_enr(enr: &Enr) -> Result<PeerId, String> {
    let public_key = match enr.public_key() {
        CombinedPublicKey::Secp256k1(public_key) => {
            let public_key =
                identity::secp256k1::PublicKey::try_from_bytes(&public_key.to_sec1_bytes())
                    .map_err(|error| error.to_string())?;
            identity::PublicKey::from(public_key)
        }
        CombinedPublicKey::Ed25519(public_key) => {
            let public_key = identity::ed25519::PublicKey::try_from_bytes(&public_key.to_bytes())
                .map_err(|error| error.to_string())?;
            identity::PublicKey::from(public_key)
        }
    };
    Ok(PeerId::from_public_key(&public_key))
}

fn multiaddr_from_ip(ip: IpAddr, port: u16, peer_id: PeerId) -> Multiaddr {
    let addr = match ip {
        IpAddr::V4(ipv4) => Multiaddr::empty().with(Protocol::Ip4(ipv4)),
        IpAddr::V6(ipv6) => Multiaddr::empty().with(Protocol::Ip6(ipv6)),
    }
    .with(Protocol::Tcp(port));
    addr.with_p2p(peer_id)
        .expect("newly built multiaddr should always accept a peer id")
}

fn multiaddr_from_ip_quic(ip: IpAddr, port: u16, peer_id: PeerId) -> Multiaddr {
    let addr = match ip {
        IpAddr::V4(ipv4) => Multiaddr::empty().with(Protocol::Ip4(ipv4)),
        IpAddr::V6(ipv6) => Multiaddr::empty().with(Protocol::Ip6(ipv6)),
    }
    .with(Protocol::Udp(port))
    .with(Protocol::QuicV1);
    addr.with_p2p(peer_id)
        .expect("newly built multiaddr should always accept a peer id")
}

fn strip_peer_id(mut addr: Multiaddr) -> Multiaddr {
    match addr.pop() {
        Some(Protocol::P2p(_)) => {}
        Some(other) => addr.push(other),
        None => {}
    }
    addr
}

fn enr_quic4(enr: &Enr) -> Option<u16> {
    enr.get_decodable("quic").and_then(Result::ok)
}

fn enr_quic6(enr: &Enr) -> Option<u16> {
    enr.get_decodable("quic6").and_then(Result::ok)
}

fn dial_address_class(addr: &Multiaddr) -> Option<DialAddressClass> {
    let mut ip_version = None;
    let mut transport = None;
    for protocol in addr.iter() {
        match protocol {
            Protocol::Ip4(_) => ip_version = Some(4u8),
            Protocol::Ip6(ip) if ip.to_ipv4_mapped().is_some() => ip_version = Some(0u8),
            Protocol::Ip6(_) => ip_version = Some(6u8),
            Protocol::Tcp(_) => transport = Some("tcp"),
            Protocol::QuicV1 => transport = Some("quic"),
            _ => {}
        }
    }

    match (ip_version, transport) {
        (Some(4), Some("tcp")) => Some(DialAddressClass::Tcp4),
        (Some(4), Some("quic")) => Some(DialAddressClass::Quic4),
        (Some(6), Some("tcp")) => Some(DialAddressClass::Tcp6),
        (Some(6), Some("quic")) => Some(DialAddressClass::Quic6),
        _ => None,
    }
}

fn retain_dial_addresses_for_families(
    families: ConsensusDialAddressFamilies,
    addrs: &mut Vec<Multiaddr>,
) {
    addrs.retain(|addr| dial_address_matches_families(families, addr));
}

fn dial_address_matches_families(families: ConsensusDialAddressFamilies, addr: &Multiaddr) -> bool {
    dial_address_class(addr).is_some_and(|class| families.includes(class))
}

fn sync_committee_period_for_slot(slot: u64) -> u64 {
    const SLOTS_PER_EPOCH: u64 = 32;
    const EPOCHS_PER_SYNC_COMMITTEE_PERIOD: u64 = 256;
    slot / (SLOTS_PER_EPOCH * EPOCHS_PER_SYNC_COMMITTEE_PERIOD)
}

fn current_wall_clock_slot() -> u64 {
    MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_slot()
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

fn peer_backoff_delay(attempts: u32) -> Duration {
    let exponent = attempts.saturating_sub(1).min(6);
    let multiplier = 1u64 << exponent;
    let seconds = PEER_BACKOFF_BASE
        .as_secs()
        .saturating_mul(multiplier)
        .min(PEER_BACKOFF_MAX.as_secs());
    Duration::from_secs(seconds)
}

fn peer_lifecycle_priority(
    lifecycle: &PeerLifecycleState,
    support_score: i32,
    bootnode: bool,
) -> i32 {
    let usefulness = lifecycle
        .bootstrap_successes
        .saturating_mul(4)
        .saturating_add(lifecycle.useful_successes.saturating_mul(3))
        .saturating_add(lifecycle.status_successes)
        .min(100) as i32;
    let failure_penalty = lifecycle.transport_failures.min(4) as i32 * 2_500
        + lifecycle.rpc_failures.min(4) as i32 * 2_500
        + lifecycle.disconnects.min(8) as i32 * 1_000;
    let preferred_bonus = lifecycle.preferred() as i32 * 2_000;
    let bootnode_penalty = bootnode as i32 * 250;

    preferred_bonus + support_score * 1_000 + usefulness * 10 - failure_penalty - bootnode_penalty
}

fn light_client_request_due(
    last_request: Option<Instant>,
    now: Instant,
    interval: Duration,
) -> bool {
    last_request.is_none_or(|last_request| now.duration_since(last_request) >= interval)
}

fn updates_by_range_request_due(
    request: LightClientUpdatesByRangeRequest,
    current_period: u64,
    last_request: Option<Instant>,
    now: Instant,
) -> bool {
    request.start_period < current_period
        || light_client_request_due(last_request, now, CURRENT_PERIOD_UPDATE_POLL_INTERVAL)
}

fn head_recovery_connection_slots_to_reclaim(
    max_peers: usize,
    active_targets: usize,
    active_progression_peers: usize,
    dialable_progression_candidates: usize,
) -> usize {
    if max_peers == 0 || dialable_progression_candidates == 0 {
        return 0;
    }

    let progression_peer_reserve = HEAD_RECOVERY_PROGRESSION_PEER_RESERVE.min(max_peers);
    let progression_peer_deficit = progression_peer_reserve
        .saturating_sub(active_progression_peers)
        .min(dialable_progression_candidates);
    let available_slots = max_peers.saturating_sub(active_targets);
    progression_peer_deficit.saturating_sub(available_slots)
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

const MAINNET_BOOTNODES: &[&str] = &[
    "enr:-Iu4QLm7bZGdAt9NSeJG0cEnJohWcQTQaI9wFLu3Q7eHIDfrI4cwtzvEW3F3VbG9XdFXlrHyFGeXPn9snTCQJ9bnMRABgmlkgnY0gmlwhAOTJQCJc2VjcDI1NmsxoQIZdZD6tDYpkpEfVo5bgiU8MGRjhcOmHGD2nErK0UKRrIN0Y3CCIyiDdWRwgiMo",
    "enr:-Iu4QEDJ4Wa_UQNbK8Ay1hFEkXvd8psolVK6OhfTL9irqz3nbXxxWyKwEplPfkju4zduVQj6mMhUCm9R2Lc4YM5jPcIBgmlkgnY0gmlwhANrfESJc2VjcDI1NmsxoQJCYz2-nsqFpeEj6eov9HSi9QssIVIVNr0I89J1vXM9foN0Y3CCIyiDdWRwgiMo",
    "enr:-Ku4QImhMc1z8yCiNJ1TyUxdcfNucje3BGwEHzodEZUan8PherEo4sF7pPHPSIB1NNuSg5fZy7qFsjmUKs2ea1Whi0EBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQOVphkDqal4QzPMksc5wnpuC3gvSC8AfbFOnZY_On34wIN1ZHCCIyg",
    "enr:-Ku4QP2xDnEtUXIjzJ_DhlCRN9SN99RYQPJL92TMlSv7U5C1YnYLjwOQHgZIUXw6c-BvRg2Yc2QsZxxoS_pPRVe0yK8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMeFF5GrS7UZpAH2Ly84aLK-TyvH-dRo0JM1i8yygH50YN1ZHCCJxA",
    "enr:-Ku4QPp9z1W4tAO8Ber_NQierYaOStqhDqQdOPY3bB3jDgkjcbk6YrEnVYIiCBbTxuar3CzS528d2iE7TdJsrL-dEKoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMw5fqqkw2hHC4F5HZZDPsNmPdB1Gi8JPQK7pRc9XHh-oN1ZHCCKvg",
    "enr:-Le4QPUXJS2BTORXxyx2Ia-9ae4YqA_JWX3ssj4E_J-3z1A-HmFGrU8BpvpqhNabayXeOZ2Nq_sbeDgtzMJpLLnXFgAChGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISsaa0Zg2lwNpAkAIkHAAAAAPA8kv_-awoTiXNlY3AyNTZrMaEDHAD2JKYevx89W0CcFJFiskdcEzkH_Wdv9iW42qLK79ODdWRwgiMohHVkcDaCI4I",
    "enr:-Le4QLHZDSvkLfqgEo8IWGG96h6mxwe_PsggC20CL3neLBjfXLGAQFOPSltZ7oP6ol54OvaNqO02Rnvb8YmDR274uq8ChGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLosQxg2lwNpAqAX4AAAAAAPA8kv_-ax65iXNlY3AyNTZrMaEDBJj7_dLFACaxBfaI8KZTh_SSJUjhyAyfshimvSqo22WDdWRwgiMohHVkcDaCI4I",
    "enr:-Le4QH6LQrusDbAHPjU_HcKOuMeXfdEB5NJyXgHWFadfHgiySqeDyusQMvfphdYWOzuSZO9Uq2AMRJR5O4ip7OvVma8BhGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLY9ncg2lwNpAkAh8AgQIBAAAAAAAAAAmXiXNlY3AyNTZrMaECDYCZTZEksF-kmgPholqgVt8IXr-8L7Nu7YrZ7HUpgxmDdWRwgiMohHVkcDaCI4I",
    "enr:-Le4QIqLuWybHNONr933Lk0dcMmAB5WgvGKRyDihy1wHDIVlNuuztX62W51voT4I8qD34GcTEOTmag1bcdZ_8aaT4NUBhGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLY04ng2lwNpAkAh8AgAIBAAAAAAAAAA-fiXNlY3AyNTZrMaEDscnRV6n1m-D9ID5UsURk0jsoKNXt1TIrj8uKOGW6iluDdWRwgiMohHVkcDaCI4I",
    "enr:-Ku4QHqVeJ8PPICcWk1vSn_XcSkjOkNiTg6Fmii5j6vUQgvzMc9L1goFnLKgXqBJspJjIsB91LTOleFmyWWrFVATGngBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAMRHkWJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyg",
    "enr:-Ku4QG-2_Md3sZIAUebGYT6g0SMskIml77l6yR-M_JXc-UdNHCmHQeOiMLbylPejyJsdAPsTHJyjJB2sYGDLe0dn8uYBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhBLY-NyJc2VjcDI1NmsxoQORcM6e19T1T9gi7jxEZjk_sjVLGFscUNqAY9obgZaxbIN1ZHCCIyg",
    "enr:-Ku4QPn5eVhcoF1opaFEvg1b6JNFD2rqVkHQ8HApOKK61OIcIXD127bKWgAtbwI7pnxx6cDyk_nI88TrZKQaGMZj0q0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDayLMaJc2VjcDI1NmsxoQK2sBOLGcUb4AwuYzFuAVCaNHA-dy24UuEKkeFNgCVCsIN1ZHCCIyg",
    "enr:-Ku4QEWzdnVtXc2Q0ZVigfCGggOVB2Vc1ZCPEc6j21NIFLODSJbvNaef1g4PxhPwl_3kax86YPheFUSLXPRs98vvYsoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDZBrP2Jc2VjcDI1NmsxoQM6jr8Rb1ktLEsVcKAPa08wCsKUmvoQ8khiOl_SLozf9IN1ZHCCIyg",
    "enr:-LK4QA8FfhaAjlb_BXsXxSfiysR7R52Nhi9JBt4F8SPssu8hdE1BXQQEtVDC3qStCW60LSO7hEsVHv5zm8_6Vnjhcn0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAN4aBKJc2VjcDI1NmsxoQJerDhsJ-KxZ8sHySMOCmTO6sHM3iCFQ6VMvLTe948MyYN0Y3CCI4yDdWRwgiOM",
    "enr:-LK4QKWrXTpV9T78hNG6s8AM6IO4XH9kFT91uZtFg1GcsJ6dKovDOr1jtAAFPnS2lvNltkOGA9k29BUN7lFh_sjuc9QBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhANAdd-Jc2VjcDI1NmsxoQLQa6ai7y9PMN5hpLe5HmiJSlYzMuzP7ZhwRiwHvqNXdoN0Y3CCI4yDdWRwgiOM",
    "enr:-IS4QPi-onjNsT5xAIAenhCGTDl4z-4UOR25Uq-3TmG4V3kwB9ljLTb_Kp1wdjHNj-H8VVLRBSSWVZo3GUe3z6k0E-IBgmlkgnY0gmlwhKB3_qGJc2VjcDI1NmsxoQMvAfgB4cJXvvXeM6WbCG86CstbSxbQBSGx31FAwVtOTYN1ZHCCIyg",
    "enr:-KG4QPUf8-g_jU-KrwzG42AGt0wWM1BTnQxgZXlvCEIfTQ5hSmptkmgmMbRkpOqv6kzb33SlhPHJp7x4rLWWiVq5lSECgmlkgnY0gmlwhFPlR9KDaXA2kCoGxcAJAAAVAAAAAAAAABCJc2VjcDI1NmsxoQLdUv9Eo9sxCt0tc_CheLOWnX59yHJtkBSOL7kpxdJ6GYN1ZHCCIyiEdWRwNoIjKA",
];

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, Ipv6Addr};
    use std::sync::{Arc, Mutex};

    use super::*;
    use alloy_primitives::b256;
    use libp2p::StreamProtocol;
    use tempfile::TempDir;

    #[test]
    fn unavailable_consensus_network_invalidates_cached_head_status() {
        let sync_status = Arc::new(Mutex::new(SyncStatus {
            node_state: NodeState::Synced,
            syncing: true,
            optimistic_execution_head: Some(logex_types::ExecutionAnchor {
                beacon_root: B256::repeat_byte(0x01),
                beacon_slot: current_wall_clock_slot().saturating_sub(100),
                block_number: 1_000,
                block_hash: B256::repeat_byte(0x02),
                receipts_root: B256::repeat_byte(0x03),
            }),
            consensus_head_fresh: Some(true),
            consensus_network: Some(ConsensusNetworkStatus {
                connected_peer_sessions: 12,
                dialing_peer_sessions: 4,
                pending_rpc_requests: 3,
                p2p_download_bytes_per_sec: 1_000,
                p2p_upload_bytes_per_sec: 500,
                ..Default::default()
            }),
            ..Default::default()
        }));

        mark_consensus_network_unavailable(&sync_status);

        let status = sync_status.lock().unwrap();
        assert_eq!(status.node_state, NodeState::WaitingForConsensus);
        assert!(!status.syncing);
        assert_eq!(status.consensus_head_fresh, Some(false));
        assert!(status.consensus_status_updated_at_unix_ms.is_some());
        let network = status.consensus_network.as_ref().unwrap();
        assert!(network.head_recovery_active);
        assert_eq!(network.connected_peer_sessions, 0);
        assert_eq!(network.dialing_peer_sessions, 0);
        assert_eq!(network.pending_rpc_requests, 0);
        assert_eq!(network.p2p_download_bytes_per_sec, 0);
        assert_eq!(network.p2p_upload_bytes_per_sec, 0);
    }

    #[test]
    fn discovery_secret_is_stable_after_first_write() {
        let temp = TempDir::new().unwrap();
        let path = discovery_secret_path(temp.path());

        let first = load_or_create_secret_key(&path).unwrap();
        let second = load_or_create_secret_key(&path).unwrap();

        assert_eq!(first.encode(), second.encode());
    }

    #[test]
    fn known_peers_round_trip() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        let peers = vec![
            PersistedPeer {
                enr: MAINNET_BOOTNODES[0].to_string(),
                support: Some(PeerRpcSupport {
                    status: true,
                    light_client_bootstrap: true,
                    ..PeerRpcSupport::default()
                }),
                status_successes: 2,
                bootstrap_successes: 1,
                useful_successes: 1,
                dial_stats: PeerDialAddressStats {
                    tcp4_successes: 1,
                    quic4_successes: 0,
                    tcp6_successes: 0,
                    quic6_successes: 0,
                    tcp4_failures: 0,
                    quic4_failures: 1,
                    tcp6_failures: 0,
                    quic6_failures: 0,
                },
            },
            PersistedPeer {
                enr: MAINNET_BOOTNODES[1].to_string(),
                support: None,
                status_successes: 0,
                bootstrap_successes: 0,
                useful_successes: 0,
                dial_stats: PeerDialAddressStats::default(),
            },
        ];

        persist_known_peers(&path, &peers).unwrap();
        let loaded = load_known_peers(&path).unwrap();

        assert_eq!(loaded, peers);
    }

    #[test]
    fn known_peers_backward_compatibility_defaults_missing_state() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        let legacy = serde_json::json!([{ "enr": MAINNET_BOOTNODES[0] }]);

        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, serde_json::to_vec_pretty(&legacy).unwrap()).unwrap();
        let loaded = load_known_peers(&path).unwrap();

        assert_eq!(
            loaded,
            vec![PersistedPeer {
                enr: MAINNET_BOOTNODES[0].to_string(),
                support: None,
                status_successes: 0,
                bootstrap_successes: 0,
                useful_successes: 0,
                dial_stats: PeerDialAddressStats::default(),
            }]
        );
    }

    #[test]
    fn bundled_bootnodes_parse() {
        let bootnodes = mainnet_bootnodes().unwrap();
        let current_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_epoch();

        assert_eq!(bootnodes.len(), 17);
        assert!(bootnodes.iter().all(|enr| enr.udp4().is_some()));
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC
                .enr_fork_id_for_epoch(current_epoch)
                .len(),
            16
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC
                .fork_digest_for_epoch(current_epoch)
                .len(),
            4
        );
    }

    #[test]
    fn inbound_request_validation_enforces_consensus_limits() {
        assert!(
            inbound_request_cost(&Eth2RpcRequest::BeaconBlocksByRange(
                BeaconBlocksByRangeRequest {
                    start_slot: 1,
                    count: 128,
                    step: 1,
                }
            ))
            .is_ok()
        );
        assert!(
            inbound_request_cost(&Eth2RpcRequest::BeaconBlocksByRange(
                BeaconBlocksByRangeRequest {
                    start_slot: 1,
                    count: 129,
                    step: 1,
                }
            ))
            .is_err()
        );
        assert!(
            inbound_request_cost(&Eth2RpcRequest::BeaconBlocksByRange(
                BeaconBlocksByRangeRequest {
                    start_slot: 1,
                    count: 1,
                    step: 0,
                }
            ))
            .is_err()
        );
        assert!(
            inbound_request_cost(&Eth2RpcRequest::BeaconBlocksByRoot(vec![B256::ZERO; 129]))
                .is_err()
        );
        assert!(
            inbound_request_cost(&Eth2RpcRequest::LightClientUpdatesByRange(
                LightClientUpdatesByRangeRequest {
                    start_period: 1,
                    count: 129,
                }
            ))
            .is_err()
        );
    }

    #[test]
    fn inbound_rate_limit_replenishes_after_window() {
        let now = Instant::now();
        let mut bucket = InboundRateLimitBucket {
            window_started_at: now,
            used: 0,
        };

        assert!(consume_inbound_rate_limit(
            &mut bucket,
            now,
            64,
            128,
            Duration::from_secs(10)
        ));
        assert!(!consume_inbound_rate_limit(
            &mut bucket,
            now,
            65,
            128,
            Duration::from_secs(10)
        ));
        assert!(consume_inbound_rate_limit(
            &mut bucket,
            now + Duration::from_secs(10),
            128,
            128,
            Duration::from_secs(10)
        ));
    }

    #[test]
    fn status_relevance_rejects_wrong_fork_future_head_and_finality_conflict() {
        let local = StatusMessage {
            fork_digest: [1, 2, 3, 4],
            finalized_root: B256::repeat_byte(1),
            finalized_epoch: 10,
            head_root: B256::repeat_byte(2),
            head_slot: 330,
            earliest_available_slot: 0,
        };
        assert_eq!(status_irrelevance_reason(local, local, 330), None);
        assert_eq!(
            status_irrelevance_reason(
                local,
                StatusMessage {
                    fork_digest: [4, 3, 2, 1],
                    ..local
                },
                330
            ),
            Some("incompatible fork digest")
        );
        assert_eq!(
            status_irrelevance_reason(
                local,
                StatusMessage {
                    head_slot: 332,
                    ..local
                },
                330
            ),
            Some("peer head is more than one slot in the future")
        );
        assert_eq!(
            status_irrelevance_reason(
                local,
                StatusMessage {
                    finalized_root: B256::repeat_byte(3),
                    ..local
                },
                330
            ),
            Some("conflicting finalized root at the local finalized epoch")
        );
    }

    #[test]
    fn gossip_message_id_uses_post_altair_topic_aware_wire_hash() {
        let topic = gossipsub::TopicHash::from_raw(
            "/eth2/8c9f62fe/light_client_optimistic_update/ssz_snappy",
        );
        let message = gossipsub::Message {
            source: None,
            data: vec![1, 2, 3, 4],
            sequence_number: None,
            topic,
        };
        let topic_bytes = message.topic.as_str().as_bytes();
        let mut hasher = Sha256::new();
        hasher.update(MESSAGE_DOMAIN_VALID_SNAPPY);
        hasher.update(usize_to_u64(topic_bytes.len()).to_le_bytes());
        hasher.update(topic_bytes);
        hasher.update(&message.data);
        let expected = hasher.finalize();

        assert_eq!(
            eth2_message_id(&message),
            gossipsub::MessageId::from(expected[..20].to_vec())
        );
    }

    #[test]
    fn rpc_context_must_match_payload_slot_fork_digest() {
        let slot = 419_072 * 32;
        let expected = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(419_072);
        assert!(rpc_context_matches_slot(
            &RawRpcResponse {
                context_bytes: Some(expected),
                bytes: Vec::new(),
            },
            slot
        ));
        assert!(!rpc_context_matches_slot(
            &RawRpcResponse {
                context_bytes: Some([0xff; 4]),
                bytes: Vec::new(),
            },
            slot
        ));
        assert!(!rpc_context_matches_slot(
            &RawRpcResponse {
                context_bytes: None,
                bytes: Vec::new(),
            },
            slot
        ));
    }

    #[test]
    fn peer_backoff_delay_grows_and_caps() {
        assert_eq!(peer_backoff_delay(1), Duration::from_secs(15));
        assert_eq!(peer_backoff_delay(2), Duration::from_secs(30));
        assert_eq!(peer_backoff_delay(3), Duration::from_secs(60));
        assert_eq!(peer_backoff_delay(8), Duration::from_secs(15 * 60));
    }

    #[test]
    fn peer_priority_yields_to_discovery_after_consecutive_failures() {
        let now = Instant::now();
        let mut known_capable = PeerLifecycleState::default();
        known_capable.record_success(RpcRequestKind::Status);
        let unknown = PeerLifecycleState::default();

        assert!(
            peer_lifecycle_priority(&known_capable, 5, false)
                > peer_lifecycle_priority(&unknown, 1, false)
        );
        for _ in 0..3 {
            known_capable.record_transport_failure(now);
        }
        assert!(
            peer_lifecycle_priority(&known_capable, 5, false)
                < peer_lifecycle_priority(&unknown, 1, false)
        );

        known_capable.record_success(RpcRequestKind::Status);
        assert!(
            peer_lifecycle_priority(&known_capable, 5, false)
                > peer_lifecycle_priority(&unknown, 1, false)
        );
    }

    #[test]
    fn light_client_request_polling_is_bounded() {
        let now = Instant::now();
        assert!(light_client_request_due(None, now, RPC_REQUEST_INTERVAL));
        assert!(!light_client_request_due(
            Some(now),
            now + Duration::from_secs(4),
            RPC_REQUEST_INTERVAL
        ));
        assert!(light_client_request_due(
            Some(now),
            now + RPC_REQUEST_INTERVAL,
            RPC_REQUEST_INTERVAL
        ));
        assert!(!light_client_request_due(
            Some(now),
            now + Duration::from_secs(59),
            FINALITY_UPDATE_POLL_INTERVAL
        ));
        assert!(light_client_request_due(
            Some(now),
            now + FINALITY_UPDATE_POLL_INTERVAL,
            FINALITY_UPDATE_POLL_INTERVAL
        ));

        let current_period_request = LightClientUpdatesByRangeRequest {
            start_period: 100,
            count: 1,
        };
        assert!(!updates_by_range_request_due(
            current_period_request,
            100,
            Some(now),
            now + Duration::from_secs(59),
        ));
        assert!(updates_by_range_request_due(
            current_period_request,
            100,
            Some(now),
            now + CURRENT_PERIOD_UPDATE_POLL_INTERVAL,
        ));

        let historical_period_request = LightClientUpdatesByRangeRequest {
            start_period: 99,
            count: 1,
        };
        assert!(updates_by_range_request_due(
            historical_period_request,
            100,
            Some(now),
            now,
        ));
    }

    #[test]
    fn stale_head_reclaims_capacity_for_progression_peers() {
        assert_eq!(head_recovery_connection_slots_to_reclaim(32, 32, 0, 4), 2);
        assert_eq!(head_recovery_connection_slots_to_reclaim(32, 31, 0, 4), 1);
        assert_eq!(head_recovery_connection_slots_to_reclaim(32, 32, 1, 4), 1);
        assert_eq!(head_recovery_connection_slots_to_reclaim(32, 32, 2, 4), 0);
        assert_eq!(head_recovery_connection_slots_to_reclaim(32, 32, 0, 1), 1);
        assert_eq!(head_recovery_connection_slots_to_reclaim(0, 32, 0, 4), 0);
    }

    #[test]
    fn request_concurrency_is_wider_for_status_and_history() {
        assert_eq!(
            max_concurrent_requests_for_kind(RpcRequestKind::Status),
            MAX_CONCURRENT_STATUS_REQUESTS
        );
        assert_eq!(MAX_CONCURRENT_HISTORY_REQUESTS, 8);
        assert_eq!(
            max_concurrent_requests_for_kind(RpcRequestKind::BeaconBlocksByRange),
            MAX_CONCURRENT_HISTORY_REQUESTS
        );
        assert_eq!(
            max_concurrent_requests_for_kind(RpcRequestKind::BeaconBlocksByRoot),
            MAX_CONCURRENT_HISTORY_REQUESTS
        );
        assert_eq!(
            max_concurrent_requests_for_kind(RpcRequestKind::LightClientBootstrap),
            MAX_CONCURRENT_DEFAULT_RPC_REQUESTS
        );
    }

    #[test]
    fn lifecycle_cooldown_tracks_useful_rpc_failures_until_success() {
        let mut lifecycle = PeerLifecycleState::default();
        let now = Instant::now();

        assert_eq!(
            lifecycle.record_rpc_failure(RpcRequestKind::BeaconBlocksByRange, now),
            Some(Duration::from_secs(15))
        );
        assert!(lifecycle.in_cooldown(now + Duration::from_secs(1)));

        lifecycle.record_success(RpcRequestKind::BeaconBlocksByRange);
        assert!(!lifecycle.in_cooldown(now + Duration::from_secs(1)));
    }

    #[test]
    fn lifecycle_success_resets_consecutive_connection_penalties() {
        let mut lifecycle = PeerLifecycleState::default();
        let now = Instant::now();

        lifecycle.record_transport_failure(now);
        lifecycle.record_disconnect(now);
        lifecycle.record_disconnect(now);
        assert_eq!(lifecycle.transport_failures, 1);
        assert_eq!(lifecycle.disconnects, 2);

        lifecycle.record_success(RpcRequestKind::Status);
        assert_eq!(lifecycle.transport_failures, 0);
        assert_eq!(lifecycle.rpc_failures, 0);
        assert_eq!(lifecycle.disconnects, 0);
        assert!(!lifecycle.in_cooldown(now));
        assert_eq!(lifecycle.record_disconnect(now), Duration::from_secs(15));
    }

    #[test]
    fn status_success_does_not_hide_useful_rpc_failures() {
        let mut lifecycle = PeerLifecycleState::default();
        let now = Instant::now();

        lifecycle.record_rpc_failure(RpcRequestKind::LightClientOptimisticUpdate, now);
        lifecycle.record_success(RpcRequestKind::Status);
        assert_eq!(lifecycle.rpc_failures, 1);
        assert!(lifecycle.in_cooldown(now + Duration::from_secs(1)));

        lifecycle.record_success(RpcRequestKind::LightClientOptimisticUpdate);
        assert_eq!(lifecycle.rpc_failures, 0);
        assert!(!lifecycle.in_cooldown(now + Duration::from_secs(1)));
    }

    #[test]
    fn lifecycle_prefers_successful_bootstrap_peers() {
        let mut lifecycle = PeerLifecycleState::default();
        assert!(!lifecycle.preferred());

        lifecycle.record_success(RpcRequestKind::Status);
        assert!(lifecycle.preferred());

        lifecycle.record_success(RpcRequestKind::LightClientBootstrap);
        assert_eq!(lifecycle.bootstrap_successes, 1);
        assert_eq!(lifecycle.useful_successes, 1);
    }

    #[test]
    fn dial_address_stats_shift_priority_after_failures_and_successes() {
        let mut stats = PeerDialAddressStats::default();
        assert!(
            stats.priority(DialAddressClass::Tcp4, true)
                > stats.priority(DialAddressClass::Quic4, true)
        );

        stats.record_failure(DialAddressClass::Tcp4);
        assert!(
            stats.priority(DialAddressClass::Tcp4, true)
                < stats.priority(DialAddressClass::Quic4, true)
        );

        stats.record_success(DialAddressClass::Tcp4);
        assert!(
            stats.priority(DialAddressClass::Tcp4, true)
                > stats.priority(DialAddressClass::Quic4, true)
        );
    }

    #[test]
    fn wrong_peer_id_dial_error_ignores_peer_for_run() {
        let expected = PeerId::random();
        let obtained = PeerId::random();
        let address = multiaddr_from_ip(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000, expected);
        let mut lifecycle = PeerLifecycleState::default();

        let ignored_for_run = record_dial_error_on_lifecycle(
            &mut lifecycle,
            &DialError::WrongPeerId { obtained, address },
        );

        assert!(ignored_for_run);
        assert!(lifecycle.ignored_for_run);
        assert_eq!(lifecycle.dial_stats.tcp4_failures, 1);
        assert!(!lifecycle.in_cooldown(Instant::now()));
    }

    #[test]
    fn dial_address_class_detects_tcp_and_quic_variants() {
        let peer_id = PeerId::random();
        assert_eq!(
            dial_address_class(&multiaddr_from_ip(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                9000,
                peer_id
            )),
            Some(DialAddressClass::Tcp4)
        );
        assert_eq!(
            dial_address_class(&multiaddr_from_ip_quic(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                9000,
                peer_id
            )),
            Some(DialAddressClass::Quic4)
        );
        assert_eq!(
            dial_address_class(&multiaddr_from_ip(
                IpAddr::V6(Ipv6Addr::LOCALHOST),
                9000,
                peer_id
            )),
            Some(DialAddressClass::Tcp6)
        );
        assert_eq!(
            dial_address_class(&multiaddr_from_ip(
                IpAddr::V6(Ipv4Addr::new(51, 77, 18, 149).to_ipv6_mapped()),
                9000,
                peer_id
            )),
            None
        );
    }

    #[test]
    fn dial_address_family_filter_matches_configured_families() {
        let peer_id = PeerId::random();
        let tcp4 = multiaddr_from_ip(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000, peer_id);
        let quic4 = multiaddr_from_ip_quic(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000, peer_id);
        let tcp6 = multiaddr_from_ip(IpAddr::V6(Ipv6Addr::LOCALHOST), 9000, peer_id);
        let quic6 = multiaddr_from_ip_quic(IpAddr::V6(Ipv6Addr::LOCALHOST), 9000, peer_id);
        let mapped_tcp = multiaddr_from_ip(
            IpAddr::V6(Ipv4Addr::new(51, 77, 18, 149).to_ipv6_mapped()),
            9000,
            peer_id,
        );
        let mapped_quic = multiaddr_from_ip_quic(
            IpAddr::V6(Ipv4Addr::new(51, 77, 18, 149).to_ipv6_mapped()),
            9000,
            peer_id,
        );

        let mut ipv4_addrs = vec![
            tcp4.clone(),
            quic4.clone(),
            tcp6.clone(),
            quic6.clone(),
            mapped_tcp.clone(),
            mapped_quic.clone(),
        ];
        retain_dial_addresses_for_families(ConsensusDialAddressFamilies::IPV4, &mut ipv4_addrs);
        assert_eq!(ipv4_addrs, vec![tcp4.clone(), quic4.clone()]);

        let mut ipv6_addrs = vec![
            tcp4.clone(),
            quic4.clone(),
            tcp6.clone(),
            quic6.clone(),
            mapped_tcp.clone(),
            mapped_quic.clone(),
        ];
        retain_dial_addresses_for_families(ConsensusDialAddressFamilies::IPV6, &mut ipv6_addrs);
        assert_eq!(ipv6_addrs, vec![tcp6.clone(), quic6.clone()]);

        let mut dual_addrs = vec![tcp4, quic4, tcp6, quic6, mapped_tcp, mapped_quic];
        retain_dial_addresses_for_families(ConsensusDialAddressFamilies::BOTH, &mut dual_addrs);
        assert_eq!(
            dual_addrs
                .iter()
                .filter_map(dial_address_class)
                .collect::<Vec<_>>(),
            vec![
                DialAddressClass::Tcp4,
                DialAddressClass::Quic4,
                DialAddressClass::Tcp6,
                DialAddressClass::Quic6
            ]
        );
    }

    #[test]
    fn local_enr_uses_ipv6_fields_when_external_ip_is_ipv6() {
        let key = CombinedKey::generate_secp256k1();
        let external_ip = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let enr = build_local_enr(
            &key,
            LocalEnrConfig {
                fork_id: &[0u8; 16],
                next_fork_digest: &[1u8; 4],
                custody_group_count: 0,
                bind_ip: IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                external_ip: Some(IpAddr::V6(external_ip)),
                discovery_port: 9000,
                p2p_port: 9001,
            },
        );

        assert_eq!(enr.ip6(), Some(external_ip));
        assert_eq!(enr.udp6(), Some(9000));
        assert_eq!(enr.tcp6(), Some(9001));
        assert_eq!(enr_quic6(&enr), Some(9001));
        assert_eq!(enr.ip4(), None);
        assert_eq!(enr.udp4(), None);
        assert_eq!(enr.tcp4(), None);
        assert_eq!(enr_quic4(&enr), None);
    }

    #[test]
    fn local_enr_preserves_ipv4_fields_by_default() {
        let key = CombinedKey::generate_secp256k1();
        let enr = build_local_enr(
            &key,
            LocalEnrConfig {
                fork_id: &[0u8; 16],
                next_fork_digest: &[1u8; 4],
                custody_group_count: 0,
                bind_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                external_ip: None,
                discovery_port: 9000,
                p2p_port: 9000,
            },
        );

        assert_eq!(enr.udp4(), Some(9000));
        assert_eq!(enr.tcp4(), Some(9000));
        assert_eq!(enr.ip6(), None);
        assert_eq!(enr.udp6(), None);
        assert_eq!(enr.tcp6(), None);
    }

    #[test]
    fn strip_peer_id_removes_trailing_p2p_component() {
        let peer_id = PeerId::random();
        let addr = multiaddr_from_ip(IpAddr::V4(Ipv4Addr::LOCALHOST), 9000, peer_id);

        assert_eq!(strip_peer_id(addr).to_string(), "/ip4/127.0.0.1/tcp/9000");
    }

    #[test]
    fn bootnode_enr_yields_dialable_multiaddr() {
        let enr = MAINNET_BOOTNODES[0].parse::<Enr>().unwrap();
        let (peer_id, addrs) = enr_multiaddrs(&enr).unwrap();

        assert!(!addrs.is_empty());
        assert!(addrs.iter().all(|addr| addr.to_string().contains("/tcp/")));
        assert!(
            addrs
                .iter()
                .all(|addr| addr.to_string().contains(&peer_id.to_string()))
        );
    }

    #[test]
    fn consensus_bootnode_discovery_filter_matches_dial_family() {
        let key = CombinedKey::generate_secp256k1();
        let ipv4 = Enr::builder()
            .ip4(Ipv4Addr::LOCALHOST)
            .udp4(9000)
            .tcp4(9000)
            .build(&key)
            .unwrap();
        let ipv6 = Enr::builder()
            .ip6(Ipv6Addr::LOCALHOST)
            .udp6(9000)
            .tcp6(9000)
            .build(&key)
            .unwrap();

        assert!(enr_has_discv5_endpoint_for_families(
            ConsensusDialAddressFamilies::IPV4,
            &ipv4
        ));
        assert!(!enr_has_discv5_endpoint_for_families(
            ConsensusDialAddressFamilies::IPV6,
            &ipv4
        ));
        assert!(enr_has_discv5_endpoint_for_families(
            ConsensusDialAddressFamilies::IPV6,
            &ipv6
        ));
        assert!(enr_has_discv5_endpoint_for_families(
            ConsensusDialAddressFamilies::BOTH,
            &ipv6
        ));
    }

    #[test]
    fn observed_consensus_peer_keeps_only_configured_dial_family() {
        let key = CombinedKey::generate_secp256k1();
        let peer_enr = Enr::builder()
            .ip4(Ipv4Addr::LOCALHOST)
            .udp4(9000)
            .tcp4(9001)
            .ip6(Ipv6Addr::LOCALHOST)
            .udp6(9002)
            .tcp6(9003)
            .build(&key)
            .unwrap();
        let mut peers = HashMap::new();

        let peer_id = observe_dialable_peer_for_families(
            &mut peers,
            ConsensusDialAddressFamilies::IPV6,
            &peer_enr,
        )
        .expect("dual-stack ENR should provide an IPv6 dial address");

        let addrs = peers
            .get(&peer_id)
            .expect("filtered dial addresses should be retained");
        assert!(!addrs.is_empty());
        assert!(addrs.iter().all(|addr| matches!(
            dial_address_class(addr),
            Some(DialAddressClass::Tcp6 | DialAddressClass::Quic6)
        )));
    }

    #[test]
    fn peer_id_from_enr_matches_libp2p_identity() {
        let enr_key = CombinedKey::generate_secp256k1();
        let enr = Enr::builder().build(&enr_key).unwrap();
        let libp2p_keypair = build_libp2p_keypair(&enr_key).unwrap();

        assert_eq!(
            peer_id_from_enr(&enr).unwrap(),
            libp2p_keypair.public().to_peer_id()
        );
    }

    #[test]
    fn enr_fork_filter_accepts_matching_bootnode_and_rejects_other_fork_id() {
        let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(14_132_160);
        let expected_fork_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(epoch);
        let expected_fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(epoch);
        let enr_key = CombinedKey::generate_secp256k1();
        let matching = Enr::builder()
            .add_value("eth2", &expected_fork_id)
            .build(&enr_key)
            .unwrap();
        let mismatched = Enr::builder()
            .add_value("eth2", &[0u8; 16])
            .build(&enr_key)
            .unwrap();

        assert!(enr_matches_fork_digest(&matching, &expected_fork_digest));
        assert!(!enr_matches_fork_digest(&mismatched, &expected_fork_digest));
    }

    #[test]
    fn enr_fork_filter_accepts_compatible_post_electra_and_rejects_missing_digest() {
        let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(14_132_160);
        let expected_fork_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(epoch);
        let compatible_fork_id = {
            let mut fork_id = [0u8; 16];
            fork_id[..4].copy_from_slice(
                &MAINNET_CONSENSUS_CHAIN_SPEC
                    .plain_fork_digest_for_version([0x05, 0x00, 0x00, 0x00]),
            );
            fork_id[4..8].copy_from_slice(&[0x05, 0x00, 0x00, 0x00]);
            fork_id[8..16].copy_from_slice(&364_032u64.to_le_bytes());
            fork_id
        };
        let enr_key = CombinedKey::generate_secp256k1();
        let compatible = Enr::builder()
            .add_value("eth2", &compatible_fork_id)
            .build(&enr_key)
            .unwrap();
        let missing = Enr::builder().build(&enr_key).unwrap();

        assert!(enr_matches_fork_digest(&compatible, &expected_fork_digest));
        assert!(!enr_matches_fork_digest(&missing, &expected_fork_digest));
    }

    #[test]
    fn enr_relevance_filter_rejects_opstack_peers() {
        let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(14_132_160);
        let expected_fork_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(epoch);
        let expected_fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(epoch);
        let enr_key = CombinedKey::generate_secp256k1();
        let opstack = Enr::builder()
            .add_value("eth2", &expected_fork_id)
            .add_value("opstack", &[1u8, 2u8, 3u8])
            .build(&enr_key)
            .unwrap();

        assert!(!enr_is_relevant_consensus_peer(
            &opstack,
            &expected_fork_digest
        ));
    }

    #[test]
    fn identify_support_recognizes_light_client_protocols() {
        let info = identify::Info {
            public_key: identity::Keypair::generate_ed25519().public(),
            protocol_version: IDENTIFY_PROTOCOL_VERSION.to_owned(),
            agent_version: IDENTIFY_AGENT_VERSION.to_owned(),
            listen_addrs: vec![],
            protocols: vec![
                StreamProtocol::new(STATUS_V2_PROTOCOL_ID),
                StreamProtocol::new(GOODBYE_V1_PROTOCOL_ID),
                StreamProtocol::new(METADATA_V1_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_UPDATES_BY_RANGE_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID),
                StreamProtocol::new(BEACON_BLOCKS_BY_RANGE_V2_PROTOCOL_ID),
                StreamProtocol::new(BEACON_BLOCKS_BY_ROOT_V2_PROTOCOL_ID),
            ],
            observed_addr: Multiaddr::empty(),
            signed_peer_record: None,
        };

        let support = PeerRpcSupport::from_identify_info(&info);
        assert!(support.status);
        assert!(support.goodbye);
        assert!(support.metadata);
        assert!(support.supports_request(RpcRequestKind::Goodbye));
        assert!(support.supports_light_client_progression());
        assert!(support.supports_request(RpcRequestKind::MetaData));
        assert!(support.supports_request(RpcRequestKind::LightClientBootstrap));
        assert!(support.supports_request(RpcRequestKind::LightClientUpdatesByRange));
        assert!(support.supports_request(RpcRequestKind::LightClientFinalityUpdate));
        assert!(support.supports_request(RpcRequestKind::LightClientOptimisticUpdate));
        assert!(support.supports_request(RpcRequestKind::BeaconBlocksByRange));
        assert!(support.supports_request(RpcRequestKind::BeaconBlocksByRoot));
    }

    #[test]
    fn identify_support_accepts_partial_post_bootstrap_light_client_sets() {
        let info = identify::Info {
            public_key: identity::Keypair::generate_ed25519().public(),
            protocol_version: IDENTIFY_PROTOCOL_VERSION.to_owned(),
            agent_version: IDENTIFY_AGENT_VERSION.to_owned(),
            listen_addrs: vec![],
            protocols: vec![
                StreamProtocol::new(STATUS_V1_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID),
            ],
            observed_addr: Multiaddr::empty(),
            signed_peer_record: None,
        };

        let support = PeerRpcSupport::from_identify_info(&info);
        assert!(support.status);
        assert!(support.supports_light_client_progression());
        assert!(support.supports_any_post_bootstrap_work());
        assert!(support.supports_request(RpcRequestKind::LightClientFinalityUpdate));
        assert!(!support.supports_request(RpcRequestKind::LightClientBootstrap));
    }

    #[test]
    fn history_target_stays_pinned_while_forward_head_moves_and_advances_when_complete() {
        let checkpoint_root =
            b256!("0x0101010101010101010101010101010101010101010101010101010101010101");
        let initial_optimistic_root =
            b256!("0x0202020202020202020202020202020202020202020202020202020202020202");
        let newer_optimistic_root =
            b256!("0x0303030303030303030303030303030303030303030303030303030303030303");
        let initial = HistorySyncTarget {
            checkpoint_root,
            checkpoint_slot: 100,
            finalized_root: checkpoint_root,
            optimistic_root: initial_optimistic_root,
            optimistic_slot: 120,
        };
        let newer = HistorySyncTarget {
            checkpoint_root,
            checkpoint_slot: 100,
            finalized_root: initial_optimistic_root,
            optimistic_root: newer_optimistic_root,
            optimistic_slot: 125,
        };

        assert_eq!(
            select_history_sync_target(Some(initial), Some(newer), false),
            Some(initial)
        );
        assert_eq!(
            select_history_sync_target(Some(initial), Some(newer), true),
            Some(newer)
        );
    }

    #[test]
    fn forward_range_target_tracks_latest_head_while_materialization_target_is_pinned() {
        let checkpoint_root =
            b256!("0x0101010101010101010101010101010101010101010101010101010101010101");
        let current = HistorySyncTarget {
            checkpoint_root,
            checkpoint_slot: 100,
            finalized_root: checkpoint_root,
            optimistic_root: b256!(
                "0x0202020202020202020202020202020202020202020202020202020202020202"
            ),
            optimistic_slot: 120,
        };
        let latest = HistorySyncTarget {
            checkpoint_root,
            checkpoint_slot: 100,
            finalized_root: current.optimistic_root,
            optimistic_root: b256!(
                "0x0303030303030303030303030303030303030303030303030303030303030303"
            ),
            optimistic_slot: 125,
        };

        assert_eq!(
            select_history_sync_target(Some(current), Some(latest), false),
            Some(current)
        );
        assert_eq!(
            select_forward_history_range_target(Some(current), Some(latest)),
            Some(latest)
        );
    }

    #[test]
    fn persisted_anchor_records_seed_verified_beacon_chain_links() {
        let first_root = B256::repeat_byte(0x11);
        let second_root = B256::repeat_byte(0x22);
        let first = crate::AnchorRecord {
            anchor: logex_types::ExecutionAnchor {
                beacon_root: first_root,
                beacon_slot: 100,
                block_number: 1,
                block_hash: B256::repeat_byte(0x31),
                receipts_root: B256::repeat_byte(0x41),
            },
            finalized: true,
            parent_beacon_root: None,
        };
        let second = crate::AnchorRecord {
            anchor: logex_types::ExecutionAnchor {
                beacon_root: second_root,
                beacon_slot: 101,
                block_number: 2,
                block_hash: B256::repeat_byte(0x32),
                receipts_root: B256::repeat_byte(0x42),
            },
            finalized: false,
            parent_beacon_root: None,
        };

        let blocks = verified_beacon_blocks_from_anchor_records(&[first, second]);

        assert_eq!(blocks[&first_root].parent_root, B256::ZERO);
        assert_eq!(blocks[&second_root].parent_root, first_root);
        assert_eq!(blocks[&second_root].execution_anchor.block_number, 2);
    }

    #[test]
    fn history_target_switches_immediately_on_reorg_or_same_slot_replacement() {
        let checkpoint_root =
            b256!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let current = HistorySyncTarget {
            checkpoint_root,
            checkpoint_slot: 64,
            finalized_root: checkpoint_root,
            optimistic_root: b256!(
                "0x2222222222222222222222222222222222222222222222222222222222222222"
            ),
            optimistic_slot: 96,
        };
        let reorged = HistorySyncTarget {
            checkpoint_root,
            checkpoint_slot: 64,
            finalized_root: checkpoint_root,
            optimistic_root: b256!(
                "0x3333333333333333333333333333333333333333333333333333333333333333"
            ),
            optimistic_slot: 96,
        };

        assert_eq!(
            select_history_sync_target(Some(current), Some(reorged), false),
            Some(reorged)
        );
    }

    #[test]
    fn post_bootstrap_request_selection_skips_unbuildable_higher_priority_work() {
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                range_ready: true,
                finality_ready: true,
                optimistic_ready: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRange)
        );
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                finality_ready: true,
                optimistic_ready: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::LightClientFinalityUpdate)
        );
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                optimistic_ready: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::LightClientOptimisticUpdate)
        );
    }

    #[test]
    fn stale_head_recovery_prioritizes_optimistic_progress_over_period_maintenance() {
        assert_eq!(
            select_live_head_progression_request_kind(true, true, true),
            Some(RpcRequestKind::LightClientOptimisticUpdate)
        );
        assert_eq!(
            select_live_head_progression_request_kind(true, false, true),
            Some(RpcRequestKind::LightClientUpdatesByRange)
        );
        assert_eq!(
            select_live_head_progression_request_kind(false, false, true),
            Some(RpcRequestKind::LightClientFinalityUpdate)
        );
    }

    #[test]
    fn post_bootstrap_request_selection_prefers_ranges_before_deferred_root_chasing() {
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                range_ready: true,
                deferred_root_ready: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRange)
        );
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                priority_root_ready: true,
                range_ready: true,
                deferred_root_ready: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRange)
        );
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                deferred_root_ready: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRoot)
        );
    }

    #[test]
    fn post_bootstrap_request_selection_can_prefer_priority_root_when_range_was_just_sent() {
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                priority_root_ready: true,
                range_ready: true,
                deferred_root_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRoot)
        );
    }

    #[test]
    fn post_bootstrap_request_selection_keeps_root_and_range_flows_alive_together() {
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                priority_root_ready: true,
                range_ready: true,
                deferred_root_ready: true,
                pending_priority_root: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRange)
        );
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                priority_root_ready: true,
                range_ready: true,
                deferred_root_ready: true,
                pending_range: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRoot)
        );
    }

    #[test]
    fn post_bootstrap_request_selection_chases_deferred_root_while_range_is_pending() {
        assert_eq!(
            select_post_bootstrap_request_kind(PostBootstrapRequestReadiness {
                range_ready: true,
                deferred_root_ready: true,
                pending_range: true,
                prefer_range_when_both_ready: true,
                ..Default::default()
            }),
            Some(RpcRequestKind::BeaconBlocksByRoot)
        );
    }

    #[test]
    fn live_head_progression_is_needed_until_optimistic_head_is_fresh() {
        let target = HistorySyncTarget {
            checkpoint_root: B256::repeat_byte(0x10),
            checkpoint_slot: 100,
            finalized_root: B256::repeat_byte(0x10),
            optimistic_root: B256::repeat_byte(0x10),
            optimistic_slot: 100,
        };
        assert!(live_head_progression_needed(Some(target), 100));

        let advanced = HistorySyncTarget {
            optimistic_root: B256::repeat_byte(0x11),
            optimistic_slot: 101,
            ..target
        };
        assert!(!live_head_progression_needed(Some(advanced), 105));
        assert!(live_head_progression_needed(Some(advanced), 106));
        assert!(live_head_progression_needed(Some(advanced), 151));
        assert!(!live_head_progression_needed(None, 151));
    }

    #[test]
    fn limited_local_status_stays_genesis_shaped_until_verified_store_exists() {
        let status = limited_local_status_message([9, 8, 7, 6]);

        assert_eq!(status.fork_digest, [9, 8, 7, 6]);
        assert_eq!(status.finalized_root, B256::ZERO);
        assert_eq!(
            status.head_root,
            MAINNET_CONSENSUS_CHAIN_SPEC.genesis_block_root
        );
        assert_eq!(status.finalized_epoch, 0);
        assert_eq!(status.head_slot, 0);
        assert_eq!(status.earliest_available_slot, 0);
    }

    #[test]
    fn checkpoint_forward_child_selection_prefers_known_target_lineage() {
        let block = |slot: u64, beacon_byte: u8| VerifiedBeaconBlock {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon_root: B256::repeat_byte(beacon_byte),
            parent_root: B256::repeat_byte(0x10),
            slot,
            execution_anchor: logex_types::ExecutionAnchor {
                beacon_root: B256::repeat_byte(beacon_byte),
                beacon_slot: slot,
                block_number: slot,
                block_hash: B256::repeat_byte(beacon_byte.wrapping_add(1)),
                receipts_root: B256::repeat_byte(beacon_byte.wrapping_add(2)),
            },
        };
        let child_a = block(101, 0x11);
        let child_b = block(101, 0x12);
        let preferred_roots = HashSet::from([child_b.beacon_root]);

        assert_eq!(
            select_checkpoint_forward_child([child_a, child_b], &preferred_roots),
            Some(child_b)
        );
        assert_eq!(
            select_checkpoint_forward_child([child_a], &HashSet::new()),
            Some(child_a)
        );
        assert_eq!(
            select_checkpoint_forward_child([child_a, child_b], &HashSet::new()),
            None
        );
    }

    #[test]
    fn forward_history_range_request_caps_deneb_electra_block_counts() {
        let request = next_forward_history_range_request_for_progress(CachedForwardPathProgress {
            checkpoint_slot: 100,
            target_slot: 400,
            highest_cached_slot: 100,
        })
        .expect("range request should be available");

        assert_eq!(request.start_slot, 101);
        assert_eq!(request.count, FORWARD_BEACON_BLOCK_RANGE_WINDOW);
        assert_eq!(request.step, 1);
    }

    #[test]
    fn forward_history_range_request_starts_after_materialized_ceiling() {
        let request = next_forward_history_range_request_for_progress(CachedForwardPathProgress {
            checkpoint_slot: 100,
            target_slot: 400,
            highest_cached_slot: 140,
        })
        .expect("range request should be available");

        assert_eq!(request.start_slot, 141);
        assert_eq!(request.count, FORWARD_BEACON_BLOCK_RANGE_WINDOW);
        assert_eq!(request.step, 1);
    }

    #[test]
    fn range_request_slot_filter_respects_count_and_step() {
        let request = BeaconBlocksByRangeRequest {
            start_slot: 100,
            count: 4,
            step: 2,
        };

        assert!(!range_request_contains_slot(request, 99));
        assert!(range_request_contains_slot(request, 100));
        assert!(!range_request_contains_slot(request, 101));
        assert!(range_request_contains_slot(request, 102));
        assert!(range_request_contains_slot(request, 106));
        assert!(!range_request_contains_slot(request, 108));
        assert!(!range_request_contains_slot(
            BeaconBlocksByRangeRequest {
                start_slot: 100,
                count: 0,
                step: 1,
            },
            100,
        ));
        assert!(!range_request_contains_slot(
            BeaconBlocksByRangeRequest {
                start_slot: 100,
                count: 1,
                step: 0,
            },
            100,
        ));
    }

    #[test]
    fn pending_forward_history_ranges_reserve_requested_windows() {
        let forward_pending = BeaconBlocksByRangeRequest {
            start_slot: 101,
            count: 16,
            step: 1,
        };

        let forward = forward_progress_with_pending_ranges(
            CachedForwardPathProgress {
                checkpoint_slot: 100,
                target_slot: 400,
                highest_cached_slot: 100,
            },
            &[forward_pending],
        );
        assert_eq!(forward.highest_cached_slot, 116);
        assert_eq!(
            next_forward_history_range_request_for_progress(forward)
                .expect("next forward range should skip pending window")
                .start_slot,
            117
        );
    }

    #[test]
    fn forward_history_root_request_prioritizes_checkpoint_then_missing_lineage() {
        let target = HistorySyncTarget {
            checkpoint_root: B256::repeat_byte(0x10),
            checkpoint_slot: 100,
            finalized_root: B256::repeat_byte(0x14),
            optimistic_root: B256::repeat_byte(0x14),
            optimistic_slot: 104,
        };
        let block = |slot: u64, beacon_root: B256, parent_root: B256| VerifiedBeaconBlock {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon_root,
            parent_root,
            slot,
            execution_anchor: logex_types::ExecutionAnchor {
                beacon_root,
                beacon_slot: slot,
                block_number: slot,
                block_hash: beacon_root,
                receipts_root: parent_root,
            },
        };

        let mut verified = HashMap::new();
        assert_eq!(
            next_forward_history_root_request_for_target(target, &verified),
            Some(vec![target.checkpoint_root, target.optimistic_root])
        );

        verified.insert(
            target.checkpoint_root,
            block(100, target.checkpoint_root, B256::repeat_byte(0x09)),
        );
        assert_eq!(
            next_forward_history_root_request_for_target(target, &verified),
            Some(vec![target.optimistic_root])
        );

        let intermediate = B256::repeat_byte(0x12);
        verified.insert(
            target.optimistic_root,
            block(104, target.optimistic_root, intermediate),
        );
        assert_eq!(
            next_forward_history_root_request_for_target(target, &verified),
            Some(vec![intermediate])
        );

        let direct_child = B256::repeat_byte(0x11);
        verified.insert(intermediate, block(103, intermediate, direct_child));
        assert_eq!(
            next_forward_history_root_request_for_target(target, &verified),
            Some(vec![direct_child])
        );

        verified.insert(
            direct_child,
            block(101, direct_child, target.checkpoint_root),
        );
        assert_eq!(
            next_forward_history_root_request_for_target(target, &verified),
            None
        );
    }

    #[test]
    fn forward_history_root_request_excludes_pending_roots_and_batches_distinct_targets() {
        let target = HistorySyncTarget {
            checkpoint_root: B256::repeat_byte(0x20),
            checkpoint_slot: 200,
            finalized_root: B256::repeat_byte(0x23),
            optimistic_root: B256::repeat_byte(0x24),
            optimistic_slot: 204,
        };
        let block = |slot: u64, beacon_root: B256, parent_root: B256| VerifiedBeaconBlock {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon_root,
            parent_root,
            slot,
            execution_anchor: logex_types::ExecutionAnchor {
                beacon_root,
                beacon_slot: slot,
                block_number: slot,
                block_hash: beacon_root,
                receipts_root: parent_root,
            },
        };

        let mut verified = HashMap::new();
        verified.insert(
            target.checkpoint_root,
            block(200, target.checkpoint_root, B256::repeat_byte(0x19)),
        );

        let pending = HashSet::from([target.optimistic_root]);
        assert_eq!(
            next_forward_history_root_request_for_target_excluding(target, &verified, &pending),
            Some(vec![target.finalized_root])
        );

        let pending = HashSet::from([target.optimistic_root, target.finalized_root]);
        assert_eq!(
            next_forward_history_root_request_for_target_excluding(target, &verified, &pending),
            None
        );
    }

    #[test]
    fn cached_ancestry_rejects_non_decreasing_parent_slots() {
        let checkpoint = ancestry_test_block(100, 1, 0);
        for parent_slot in [102, 103] {
            let parent = ancestry_test_block(parent_slot, 2, 1);
            let head = ancestry_test_block(102, 3, 2);
            let cached = HashMap::from([
                (checkpoint.beacon_root, checkpoint),
                (parent.beacon_root, parent),
                (head.beacon_root, head),
            ]);
            assert!(
                canonical_chain_blocks_to_root(
                    &cached,
                    checkpoint.beacon_root,
                    checkpoint.slot,
                    head.beacon_root,
                )
                .is_none()
            );
        }
    }

    #[test]
    fn cached_ancestry_does_not_request_parents_of_invalid_lineage() {
        let parent = ancestry_test_block(103, 2, 1);
        let head = ancestry_test_block(102, 3, 2);
        let cached = HashMap::from([(parent.beacon_root, parent), (head.beacon_root, head)]);
        let mut missing = Vec::new();
        extend_missing_history_lineage(
            &mut missing,
            head.beacon_root,
            B256::repeat_byte(0x10),
            &cached,
            &HashSet::new(),
        );
        assert!(missing.is_empty());
    }

    fn ancestry_test_block(slot: u64, root: u8, parent: u8) -> VerifiedBeaconBlock {
        let beacon_root = B256::repeat_byte(root);
        let parent_root = B256::repeat_byte(parent);
        VerifiedBeaconBlock {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon_root,
            parent_root,
            slot,
            execution_anchor: logex_types::ExecutionAnchor {
                beacon_root,
                beacon_slot: slot,
                block_number: slot,
                block_hash: beacon_root,
                receipts_root: parent_root,
            },
        }
    }

    #[test]
    fn cached_ancestry_cycles_terminate_without_publication_or_requests() {
        let checkpoint = ancestry_test_block(100, 1, 0);
        let parent = ancestry_test_block(101, 2, 3);
        for head in [
            ancestry_test_block(102, 3, 3),
            ancestry_test_block(102, 3, 2),
        ] {
            let target = HistorySyncTarget {
                checkpoint_root: checkpoint.beacon_root,
                checkpoint_slot: checkpoint.slot,
                finalized_root: head.beacon_root,
                optimistic_root: head.beacon_root,
                optimistic_slot: head.slot,
            };
            let cached = HashMap::from([
                (checkpoint.beacon_root, checkpoint),
                (parent.beacon_root, parent),
                (head.beacon_root, head),
            ]);
            assert!(materializable_history_chain(&cached, target).is_none());
            assert!(next_forward_history_root_request_for_target(target, &cached).is_none());
            let roots = cached_target_lineage_roots(&cached, target);
            assert!(roots.contains(&head.beacon_root));
            assert!(!roots.contains(&checkpoint.beacon_root));
            assert!(roots.len() <= 2);
        }
    }

    #[test]
    fn cached_ancestry_preserves_gaps_shared_tails_and_slot_boundaries() {
        let checkpoint = ancestry_test_block(0, 1, 0);
        let parent = ancestry_test_block(42, 2, 1);
        let head = ancestry_test_block(u64::MAX, 3, 2);
        let target = HistorySyncTarget {
            checkpoint_root: checkpoint.beacon_root,
            checkpoint_slot: checkpoint.slot,
            finalized_root: parent.beacon_root,
            optimistic_root: head.beacon_root,
            optimistic_slot: head.slot,
        };
        let mut cached = HashMap::from([
            (checkpoint.beacon_root, checkpoint),
            (parent.beacon_root, parent),
            (head.beacon_root, head),
        ]);
        assert_eq!(
            materializable_history_chain(&cached, target)
                .unwrap()
                .blocks,
            vec![checkpoint, parent, head],
        );
        let roots = HashSet::from([checkpoint.beacon_root, parent.beacon_root, head.beacon_root]);
        assert_eq!(cached_target_lineage_roots(&cached, target), roots);
        assert!(next_forward_history_root_request_for_target(target, &cached).is_none());
        assert_eq!(
            canonical_chain_blocks_to_root(
                &cached,
                checkpoint.beacon_root,
                0,
                checkpoint.beacon_root,
            ),
            Some(vec![checkpoint])
        );
        assert!(
            canonical_chain_blocks_to_root(&cached, checkpoint.beacon_root, 1, head.beacon_root,)
                .is_none()
        );

        cached.remove(&parent.beacon_root);
        assert!(materializable_history_chain(&cached, target).is_none());
        assert_eq!(
            next_forward_history_root_request_for_target(target, &cached),
            Some(vec![parent.beacon_root])
        );
        assert_eq!(
            cached_target_lineage_roots(&cached, target),
            HashSet::from([parent.beacon_root, head.beacon_root])
        );
    }

    #[test]
    fn cached_ancestry_matches_cycle_detecting_reference_for_small_graphs() {
        let checkpoint = ancestry_test_block(0, 1, 0);
        for slots in [[1, 2, 3], [3, 2, 1], [1, 1, 2], [0, 1, u64::MAX]] {
            for parents in 0..125u16 {
                let blocks = [
                    checkpoint,
                    ancestry_test_block(slots[0], 2, (parents % 5) as u8),
                    ancestry_test_block(slots[1], 3, ((parents / 5) % 5) as u8),
                    ancestry_test_block(slots[2], 4, (parents / 25) as u8),
                ];
                let cached = blocks
                    .iter()
                    .map(|block| (block.beacon_root, *block))
                    .collect();
                // The reference bounds traversal by visited roots, then checks
                // slot ordering on the completed path independently.
                let mut visited = HashSet::new();
                let mut root = blocks[3].beacon_root;
                let mut path = Vec::new();
                let mut expected = None;
                while visited.insert(root) {
                    let Some(block) = blocks.iter().find(|block| block.beacon_root == root) else {
                        break;
                    };
                    path.push(*block);
                    if root == checkpoint.beacon_root {
                        path.reverse();
                        if path.windows(2).all(|pair| pair[0].slot < pair[1].slot) {
                            expected = Some(path);
                        }
                        break;
                    }
                    root = block.parent_root;
                }
                assert_eq!(
                    canonical_chain_blocks_to_root(
                        &cached,
                        checkpoint.beacon_root,
                        0,
                        blocks[3].beacon_root,
                    ),
                    expected,
                    "slots={slots:?}, parents={parents}"
                );
            }
        }
    }

    #[test]
    fn cached_ancestry_rejects_mismatched_root_metadata() {
        let checkpoint = ancestry_test_block(100, 1, 0);
        let head = ancestry_test_block(101, 2, 1);
        let target = HistorySyncTarget {
            checkpoint_root: checkpoint.beacon_root,
            checkpoint_slot: checkpoint.slot,
            finalized_root: head.beacon_root,
            optimistic_root: head.beacon_root,
            optimistic_slot: head.slot,
        };
        let mut cached = HashMap::from([
            (checkpoint.beacon_root, checkpoint),
            (head.beacon_root, head),
        ]);
        cached.get_mut(&head.beacon_root).unwrap().beacon_root = B256::repeat_byte(3);
        assert!(materializable_history_chain(&cached, target).is_none());
        assert!(next_forward_history_root_request_for_target(target, &cached).is_none());
        assert!(cached_target_lineage_roots(&cached, target).is_empty());
    }

    #[test]
    fn materializable_history_chain_waits_for_complete_target_lineage() {
        let block = |slot: u64, beacon_root: B256, parent_root: B256| VerifiedBeaconBlock {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon_root,
            parent_root,
            slot,
            execution_anchor: logex_types::ExecutionAnchor {
                beacon_root,
                beacon_slot: slot,
                block_number: slot,
                block_hash: beacon_root,
                receipts_root: parent_root,
            },
        };
        let checkpoint_root = B256::repeat_byte(0x30);
        let finalized_root = B256::repeat_byte(0x31);
        let optimistic_root = B256::repeat_byte(0x32);
        let target = HistorySyncTarget {
            checkpoint_root,
            checkpoint_slot: 300,
            finalized_root,
            optimistic_root,
            optimistic_slot: 302,
        };
        let mut verified = HashMap::from([(
            checkpoint_root,
            block(300, checkpoint_root, B256::repeat_byte(0x2f)),
        )]);

        assert_eq!(materializable_history_chain(&verified, target), None);

        verified.insert(finalized_root, block(301, finalized_root, checkpoint_root));
        let materializable = materializable_history_chain(&verified, target)
            .expect("finalized lineage should be enough to materialize a partial target");
        assert!(!materializable.reaches_optimistic_head);
        assert_eq!(
            materializable
                .blocks
                .iter()
                .map(|block| block.beacon_root)
                .collect::<Vec<_>>(),
            vec![checkpoint_root, finalized_root]
        );

        verified.insert(optimistic_root, block(302, optimistic_root, finalized_root));
        let materializable = materializable_history_chain(&verified, target)
            .expect("optimistic lineage should be preferred when complete");
        assert!(materializable.reaches_optimistic_head);
        assert_eq!(
            materializable
                .blocks
                .iter()
                .map(|block| block.beacon_root)
                .collect::<Vec<_>>(),
            vec![checkpoint_root, finalized_root, optimistic_root]
        );
    }

    #[test]
    fn cached_beacon_block_root_responses_preserve_requested_order() {
        let payload = |byte: u8| RawRpcResponse {
            context_bytes: Some([byte; 4]),
            bytes: vec![byte],
        };
        let payloads = HashMap::from([
            (B256::repeat_byte(0x11), payload(0x11)),
            (B256::repeat_byte(0x22), payload(0x22)),
        ]);

        let responses = cached_beacon_block_payloads_by_root(
            &[
                B256::repeat_byte(0x22),
                B256::repeat_byte(0x33),
                B256::repeat_byte(0x11),
            ],
            &payloads,
        );

        assert_eq!(responses, vec![payload(0x22), payload(0x11)]);
    }

    #[test]
    fn cached_beacon_block_range_responses_only_use_requested_canonical_slots() {
        let block = |slot: u64, beacon_byte: u8, parent_byte: u8| VerifiedBeaconBlock {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon_root: B256::repeat_byte(beacon_byte),
            parent_root: B256::repeat_byte(parent_byte),
            slot,
            execution_anchor: logex_types::ExecutionAnchor {
                beacon_root: B256::repeat_byte(beacon_byte),
                beacon_slot: slot,
                block_number: slot,
                block_hash: B256::repeat_byte(beacon_byte.wrapping_add(1)),
                receipts_root: B256::repeat_byte(beacon_byte.wrapping_add(2)),
            },
        };
        let payload = |byte: u8| RawRpcResponse {
            context_bytes: Some([byte; 4]),
            bytes: vec![byte],
        };

        let canonical_blocks = vec![
            block(100, 0x10, 0x09),
            block(101, 0x11, 0x10),
            block(103, 0x13, 0x11),
        ];
        let payloads = HashMap::from([
            (B256::repeat_byte(0x10), payload(0x10)),
            (B256::repeat_byte(0x11), payload(0x11)),
            (B256::repeat_byte(0x12), payload(0x12)),
            (B256::repeat_byte(0x13), payload(0x13)),
        ]);

        let responses = cached_beacon_block_payloads_by_range(
            BeaconBlocksByRangeRequest {
                start_slot: 100,
                count: 4,
                step: 1,
            },
            &canonical_blocks,
            &payloads,
        );

        assert_eq!(responses, vec![payload(0x10), payload(0x11), payload(0x13)]);
    }

    #[test]
    fn cached_light_client_updates_by_range_start_at_earliest_known_period() {
        let payload = |byte: u8| RawRpcResponse {
            context_bytes: Some([byte; 4]),
            bytes: vec![byte],
        };
        let payloads = BTreeMap::from([
            (10u64, payload(0x0a)),
            (12u64, payload(0x0c)),
            (13u64, payload(0x0d)),
        ]);

        let responses = cached_light_client_update_payloads_by_range(
            LightClientUpdatesByRangeRequest {
                start_period: 11,
                count: 4,
            },
            &payloads,
        );

        assert_eq!(responses, vec![payload(0x0c), payload(0x0d)]);
    }

    #[test]
    fn cached_light_client_updates_by_range_stops_on_first_gap() {
        let payload = |byte: u8| RawRpcResponse {
            context_bytes: Some([byte; 4]),
            bytes: vec![byte],
        };
        let payloads = BTreeMap::from([
            (20u64, payload(0x14)),
            (21u64, payload(0x15)),
            (23u64, payload(0x17)),
        ]);

        let responses = cached_light_client_update_payloads_by_range(
            LightClientUpdatesByRangeRequest {
                start_period: 20,
                count: 8,
            },
            &payloads,
        );

        assert_eq!(responses, vec![payload(0x14), payload(0x15)]);
    }

    #[test]
    fn sync_committee_period_uses_mainnet_slot_scale() {
        assert_eq!(sync_committee_period_for_slot(0), 0);
        assert_eq!(sync_committee_period_for_slot(8191), 0);
        assert_eq!(sync_committee_period_for_slot(8192), 1);
    }
}
