use std::collections::{HashMap, HashSet, VecDeque};
use std::convert::Infallible;
use std::fs;
use std::io::{self, Read, Write};
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

use crate::beacon_cache::{BeaconPayloadCache, CachedBeaconPayload};
use crate::rpc_memory::{RESPONSE_DECODED_BYTES, RpcMemoryError, RpcResponseBudgets, from_io};

use crate::light_client::{
    GossipUpdateMetadata, apply_finality_update_payload_at_slot,
    apply_optimistic_update_payload_at_slot, gossip_update_metadata,
};

use crate::rpc::{
    BEACON_BLOCKS_BY_RANGE_V1_PROTOCOL_ID, BEACON_BLOCKS_BY_RANGE_V2_PROTOCOL_ID,
    BEACON_BLOCKS_BY_ROOT_V1_PROTOCOL_ID, BEACON_BLOCKS_BY_ROOT_V2_PROTOCOL_ID,
    BeaconBlocksByRangeRequest, BudgetedRpcResponse, Eth2OutboundRequestId, Eth2RpcBehaviour,
    Eth2RpcEvent, Eth2RpcRequest, Eth2RpcResponse, GOODBYE_V1_PROTOCOL_ID,
    LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID, LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID,
    LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID, LIGHT_CLIENT_UPDATES_BY_RANGE_PROTOCOL_ID,
    LightClientUpdatesByRangeRequest, METADATA_V1_PROTOCOL_ID, METADATA_V2_PROTOCOL_ID,
    METADATA_V3_PROTOCOL_ID, MetaData, PING_PROTOCOL_ID, RawRpcResponse, STATUS_V1_PROTOCOL_ID,
    STATUS_V2_PROTOCOL_ID, StatusMessage, build_beacon_blocks_by_range_behaviour,
    build_beacon_blocks_by_root_behaviour, build_goodbye_behaviour,
    build_light_client_bootstrap_behaviour, build_light_client_finality_update_behaviour,
    build_light_client_optimistic_update_behaviour, build_light_client_updates_by_range_behaviour,
    build_metadata_behaviour, build_ping_behaviour, build_status_behaviour, invalid_request,
    rate_limited, resource_unavailable,
};
use crate::{
    ConsensusStore, LightClientVerificationError, MAINNET_CONSENSUS_CHAIN_SPEC,
    VerifiedBeaconBlock, VerifiedLightClientStore, apply_finality_update_payload,
    apply_light_client_update_payload, apply_optimistic_update_payload,
    decode_verified_beacon_block, force_update_light_client_store, verify_bootstrap_payload,
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
const MAX_KNOWN_PEERS_BYTES: usize = 1024 * 1024;
// ENR raw records are capped at 300 bytes, plus the base64 `enr:` prefix.
const MAX_PERSISTED_ENR_BYTES: usize = 404;
const HEAD_RECOVERY_PROGRESSION_PEER_RESERVE: usize = 2;
const MAX_STATUS_FAILURES_BEFORE_DISCONNECT: u32 = 2;
const IDENTIFY_GRACE_PERIOD: Duration = Duration::from_secs(8);
const PEER_BACKOFF_BASE: Duration = Duration::from_secs(15);
const PEER_BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);
const GOODBYE_REASON_IRRELEVANT_NETWORK: u64 = 2;
const GOODBYE_REASON_FAULT: u64 = 3;
const IDENTIFY_PROTOCOL_VERSION: &str = "eth2/1.0.0";
const IDENTIFY_AGENT_VERSION: &str = concat!("logex/", env!("CARGO_PKG_VERSION"));
const GOSSIP_MAX_PAYLOAD_SIZE: usize = 10 * 1024 * 1024;
const P2P_BANDWIDTH_RATE_WINDOW: Duration = Duration::from_secs(15);
const LIGHT_CLIENT_FINALITY_UPDATE_TOPIC_NAME: &str = "light_client_finality_update";
const LIGHT_CLIENT_OPTIMISTIC_UPDATE_TOPIC_NAME: &str = "light_client_optimistic_update";
const GOSSIP_ENCODING_NAME: &str = "ssz_snappy";
const MESSAGE_DOMAIN_VALID_SNAPPY: [u8; 4] = [1, 0, 0, 0];
const MESSAGE_DOMAIN_INVALID_SNAPPY: [u8; 4] = [0, 0, 0, 0];
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
    #[error("failed to preserve damaged known peers {path}: {source}")]
    QuarantineKnownPeers { path: PathBuf, source: io::Error },
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
    gossip: gossipsub::Behaviour<GossipSizeGuard>,
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

async fn stop_for_consensus_storage_failure(
    failure: &watch::Receiver<Option<Arc<str>>>,
    sync_status: &Arc<Mutex<SyncStatus>>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    if failure.borrow().is_none() {
        return false;
    }
    mark_consensus_network_unavailable(sync_status);
    // The node owns bounded shutdown. Do not serve more requests or restart
    // networking against a permanently failed store while it shuts down.
    wait_for_shutdown(shutdown).await;
    true
}

struct ConsensusNetwork {
    config: ConsensusNetworkConfig,
    consensus: Arc<ConsensusStore>,
    storage_failure: watch::Receiver<Option<Arc<str>>>,
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
    history_range_batch_limit: u64,
    history_root_batch_limit: usize,
    local_rpc_retry_after: HashMap<RpcRequestKind, Instant>,
    pending_light_client_range_requests:
        HashMap<PendingRequestKey, LightClientUpdatesByRangeRequest>,
    pending_history_root_requests: HashMap<PendingRequestKey, Vec<B256>>,
    pending_history_range_requests: HashMap<PendingRequestKey, BeaconBlocksByRangeRequest>,
    pending_peer_kinds: HashSet<(PeerId, RpcRequestKind)>,
    inbound_rate_limits: HashMap<(PeerId, RpcRequestKind), InboundRateLimitBucket>,
    last_light_client_request_at: HashMap<RpcRequestKind, Instant>,
    request_failures: RpcFailureCounts,
    gossip_topics: ConsensusGossipTopics,
    gossip_forwarded: GossipForwarded,
    pending_gossip_forward: Option<GossipUpdateMetadata>,
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
    verified_beacon_block_payloads: BeaconPayloadCache,
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

fn recovered_history_batch_limit(
    current: u64,
    requested: u64,
    received: usize,
    largest_body: usize,
    useful: bool,
) -> u64 {
    if !useful || received == 0 || received as u64 != requested || largest_body == 0 {
        return current;
    }
    let safe_count = (RESPONSE_DECODED_BYTES / largest_body) as u64;
    current.max(
        current
            .saturating_mul(2)
            .min(MAX_BEACON_BLOCKS_BY_RANGE_REQUEST)
            .min(safe_count),
    )
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

fn light_client_range_contains_next_period(
    request: LightClientUpdatesByRangeRequest,
    previous: Option<u64>,
    period: u64,
) -> bool {
    period
        .checked_sub(request.start_period)
        .is_some_and(|offset| offset < request.count)
        && previous.is_none_or(|previous| previous.checked_add(1) == Some(period))
}

fn valid_history_range_sequence<'a>(
    request: BeaconBlocksByRangeRequest,
    blocks: impl Iterator<Item = &'a VerifiedBeaconBlock>,
) -> bool {
    let mut previous: Option<&VerifiedBeaconBlock> = None;
    let mut roots = HashSet::new();
    for block in blocks {
        if !roots.insert(block.beacon_root)
            || previous.is_some_and(|previous| {
                block.slot <= previous.slot
                    || (request.step == 1 && block.parent_root != previous.beacon_root)
            })
        {
            return false;
        }
        previous = Some(block);
    }
    true
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
    payloads: &BeaconPayloadCache,
) -> Vec<CachedBeaconPayload> {
    roots
        .iter()
        .filter_map(|root| payloads.get(root).cloned())
        .collect()
}

fn cached_beacon_block_payloads_by_range(
    request: BeaconBlocksByRangeRequest,
    canonical_blocks: &[VerifiedBeaconBlock],
    payloads: &BeaconPayloadCache,
) -> Vec<CachedBeaconPayload> {
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
        .map_while(|block| payloads.get(&block.beacon_root).cloned())
        .collect()
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

#[derive(Debug, Default)]
struct GossipForwarded {
    finality: Option<GossipUpdateMetadata>,
    optimistic_slot: Option<u64>,
}

impl GossipForwarded {
    fn permits(
        &self,
        metadata: &GossipUpdateMetadata,
        before: &VerifiedLightClientStore,
        after: &VerifiedLightClientStore,
    ) -> bool {
        if let Some(finalized) = metadata.finalized_slot {
            let newer = self.finality.as_ref().is_none_or(|old| {
                finalized > old.finalized_slot.unwrap_or(0)
                    || (Some(finalized) == old.finalized_slot
                        && metadata.participants * 3 > 512 * 2
                        && old.participants * 3 <= 512 * 2)
            });
            newer && after.finalized_header.beacon.slot > before.finalized_header.beacon.slot
        } else {
            self.optimistic_slot
                .is_none_or(|slot| metadata.attested_slot > slot)
                && (after.optimistic_header.beacon.slot > before.optimistic_header.beacon.slot
                    || self
                        .finality
                        .as_ref()
                        .is_some_and(|old| old.optimistic_bytes == metadata.optimistic_bytes))
        }
    }

    fn record(&mut self, metadata: GossipUpdateMetadata) {
        if metadata.finalized_slot.is_some() {
            self.finality = Some(metadata);
        } else {
            self.optimistic_slot = Some(metadata.attested_slot);
        }
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
        Eth2RpcResponse::CachedBeaconBlocksByRange(payloads)
        | Eth2RpcResponse::CachedBeaconBlocksByRoot(payloads) => payloads
            .iter()
            .map(|payload| raw_rpc_response_payload_bytes(payload.as_raw()))
            .sum(),
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

    fn priority(self, class: DialAddressClass, bootstrap_needed: bool) -> i64 {
        // The complete weighted u32 history fits in i64 without losing ordering.
        i64::from(class.default_priority(bootstrap_needed)) + i64::from(self.successes(class)) * 100
            - i64::from(self.failures(class)) * 125
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

#[derive(Debug, Clone)]
struct RetainedPeerEnr {
    enr: Enr,
    bootstrap: bool,
}

#[derive(Debug, Clone, Default)]
struct PeerLifecycleState {
    latest_enr: Option<RetainedPeerEnr>,
    seen_in_discovery: bool,
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
            latest_enr: None,
            seen_in_discovery: false,
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
            RpcRequestKind::Status => {
                self.status_successes = self.status_successes.saturating_add(1);
            }
            RpcRequestKind::LightClientBootstrap => {
                self.bootstrap_successes = self.bootstrap_successes.saturating_add(1);
                self.useful_successes = self.useful_successes.saturating_add(1);
            }
            RpcRequestKind::LightClientUpdatesByRange
            | RpcRequestKind::LightClientFinalityUpdate
            | RpcRequestKind::LightClientOptimisticUpdate
            | RpcRequestKind::BeaconBlocksByRange
            | RpcRequestKind::BeaconBlocksByRoot => {
                self.useful_successes = self.useful_successes.saturating_add(1);
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
            if let Some((peer_id, _)) = retain_peer_enr(
                &mut peer_lifecycle,
                &mut dialable_peers,
                config.dial_families,
                &fork_digest,
                enr,
                true,
            ) && dialable_peers.contains_key(&peer_id)
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

        let mut restored_metadata = HashSet::new();
        for peer in &known_peers {
            match peer.enr.parse::<Enr>() {
                Ok(enr) => {
                    let Some(peer_id) = retain_cached_peer_enr(
                        &mut peer_lifecycle,
                        &mut dialable_peers,
                        &mut restored_metadata,
                        config.dial_families,
                        &fork_digest,
                        &enr,
                        peer,
                    ) else {
                        continue;
                    };
                    let state = peer_lifecycle.get(&peer_id).expect("retained peer");
                    if !dialable_peers.contains_key(&peer_id) {
                        continue;
                    }
                    let canonical = &state.latest_enr.as_ref().expect("retained ENR").enr;
                    if enr_has_discv5_endpoint_for_families(config.dial_families, canonical)
                        && let Err(error) = discv5.add_enr(canonical.clone())
                    {
                        tracing::debug!(%error, "skipping cached consensus peer that could not be inserted");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, enr = %peer.enr, "ignoring invalid cached consensus peer ENR")
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
            storage_failure: consensus.subscribe_storage_failure(),
            consensus,
            sync_status,
            discv5,
            swarm,
            bootnode_count: seeded_bootnodes,
            bootnode_peers,
            fork_digest,
            known_peers_path,
            // Compare future saves with the actual file, including stale entries.
            last_persisted: known_peers,
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
            history_range_batch_limit: MAX_BEACON_BLOCKS_BY_RANGE_REQUEST,
            history_root_batch_limit: MAX_BEACON_BLOCKS_BY_ROOT_REQUEST,
            local_rpc_retry_after: HashMap::new(),
            pending_light_client_range_requests: HashMap::new(),
            pending_history_root_requests: HashMap::new(),
            pending_history_range_requests: HashMap::new(),
            pending_peer_kinds: HashSet::new(),
            inbound_rate_limits: HashMap::new(),
            last_light_client_request_at: HashMap::new(),
            request_failures: RpcFailureCounts::default(),
            gossip_topics,
            gossip_forwarded: GossipForwarded::default(),
            pending_gossip_forward: None,
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
            verified_beacon_block_payloads: BeaconPayloadCache::default(),
            active_history_target: None,
            head_recovery_attempts: 0,
        })
    }

    async fn run(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ConsensusNetworkError> {
        if stop_for_consensus_storage_failure(
            &self.storage_failure,
            &self.sync_status,
            &mut shutdown,
        )
        .await
        {
            return Ok(());
        }
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
            if stop_for_consensus_storage_failure(
                &self.storage_failure,
                &self.sync_status,
                &mut shutdown,
            )
            .await
            {
                break;
            }
            tokio::select! {
                _ = wait_for_shutdown(&mut shutdown) => {
                    tracing::info!("consensus network shutting down");
                    break;
                }
                _ = self.storage_failure.changed() => {}
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
        if let Some((peer, _)) = retain_peer_enr(
            &mut self.peer_lifecycle,
            &mut self.dialable_peers,
            self.config.dial_families,
            &self.fork_digest,
            enr,
            false,
        ) {
            self.peer_lifecycle
                .get_mut(&peer)
                .expect("retained peer")
                .seen_in_discovery = true;
            if self.dialable_peers.contains_key(&peer) {
                self.observed.insert(peer);
            } else {
                self.observed.remove(&peer);
            }
        }
    }

    fn refresh_retained_peer_inventory(&mut self) {
        for (peer, state) in &self.peer_lifecycle {
            refresh_peer_enr_addresses(
                &mut self.dialable_peers,
                self.config.dial_families,
                &self.fork_digest,
                *peer,
                state,
            );
            if state.seen_in_discovery && self.dialable_peers.contains_key(peer) {
                self.observed.insert(*peer);
            } else {
                self.observed.remove(peer);
            }
        }
    }

    fn prune_inactive_peer_state(&mut self) {
        // Inbound peers may have lifecycle state without a dialable ENR.
        // Count each peer once across both inventories, retaining the existing
        // protection and ranking policy for inactive records.
        let mut candidates = self
            .dialable_peers
            .keys()
            .chain(
                self.peer_lifecycle
                    .keys()
                    .filter(|peer| !self.dialable_peers.contains_key(*peer)),
            )
            .copied()
            .filter(|peer| {
                !self.connected_peers.contains(peer)
                    && !self.dialing_peers.contains(peer)
                    && !self.closing_peers.contains(peer)
            })
            .collect::<Vec<_>>();
        let excess = candidates
            .len()
            .saturating_sub(MAX_RETAINED_DISCONNECTED_PEERS);
        if excess > 0 {
            candidates.retain(|peer| {
                !self.bootnode_peers.contains(peer) && self.pending_requests_for_peer(*peer) == 0
            });
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
        self.maintain_fork_subscriptions_at_slot(current_wall_clock_slot());
    }

    fn maintain_fork_subscriptions_at_slot(&mut self, current_slot: u64) {
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
            self.refresh_retained_peer_inventory();
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
                let reported = self
                    .swarm
                    .behaviour_mut()
                    .gossip
                    .report_message_validation_result(&message_id, &propagation_source, acceptance);
                self.complete_gossip_validation(reported);
                if !reported {
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

    fn complete_gossip_validation(&mut self, reported: bool) {
        if let Some(metadata) = self.pending_gossip_forward.take()
            && reported
        {
            self.gossip_forwarded.record(metadata);
        }
    }

    fn handle_gossip_message(
        &mut self,
        propagation_source: PeerId,
        message: &gossipsub::Message,
    ) -> gossipsub::MessageAcceptance {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        self.handle_gossip_message_at(propagation_source, message, now_ms)
    }

    fn handle_gossip_message_at(
        &mut self,
        propagation_source: PeerId,
        message: &gossipsub::Message,
        now_ms: u128,
    ) -> gossipsub::MessageAcceptance {
        use gossipsub::MessageAcceptance::{Accept, Ignore, Reject};
        self.pending_gossip_forward = None;
        let Some((finality, digest)) = parse_light_client_topic(&message.topic) else {
            return Reject;
        };
        let current_slot = gossip_slot_at(now_ms);
        let active = message.topic == self.gossip_topics.finality_update.hash()
            || message.topic == self.gossip_topics.optimistic_update.hash()
            || self.pre_subscribed_fork_digest == Some(digest)
            || self.retiring_gossip_topics.iter().any(|old| {
                old.unsubscribe_at_epoch > current_slot / 32
                    && (message.topic == old.topics.finality_update.hash()
                        || message.topic == old.topics.optimistic_update.hash())
            });
        if !active {
            return Ignore;
        }
        let Some(decoded) =
            decode_gossip_payload(&message.data, gossip_payload_limit(&message.topic))
        else {
            self.gossip_counts.decode_failures += 1;
            return Reject;
        };
        let metadata = match gossip_update_metadata(&decoded, finality) {
            Ok(metadata) => metadata,
            Err(_) => {
                self.gossip_counts.decode_failures += 1;
                return Reject;
            }
        };
        if digest != MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(metadata.attested_slot / 32)
        {
            return Reject;
        }
        if !gossip_update_is_due(metadata.signature_slot, now_ms) {
            return Ignore;
        }
        let Some(store) = self.consensus.light_client_store() else {
            return Ignore;
        };
        // The store no longer retains committees from older periods. Such
        // updates cannot advance it and must not be blamed for their age.
        if metadata.signature_slot / 8192 < store.finalized_header.beacon.slot / 8192 {
            return Ignore;
        }
        let result = if finality {
            apply_finality_update_payload_at_slot(&decoded, &store, current_slot).map(
                |(summary, next_store, _, _)| {
                    let forward = self
                        .gossip_forwarded
                        .permits(&metadata, &store, &next_store);
                    let headers_changed = next_store.finalized_header != store.finalized_header
                        || next_store.optimistic_header != store.optimistic_header;
                    let persisted = self.consensus.record_verified_finality_update(
                        summary,
                        RawRpcResponse {
                            context_bytes: None,
                            bytes: decoded,
                        },
                        next_store,
                    );
                    (forward, headers_changed, persisted)
                },
            )
        } else {
            apply_optimistic_update_payload_at_slot(&decoded, &store, current_slot).map(
                |(summary, next_store, _)| {
                    let forward = self
                        .gossip_forwarded
                        .permits(&metadata, &store, &next_store);
                    let headers_changed = next_store.finalized_header != store.finalized_header
                        || next_store.optimistic_header != store.optimistic_header;
                    let persisted = self.consensus.record_verified_optimistic_update(
                        summary,
                        RawRpcResponse {
                            context_bytes: None,
                            bytes: decoded,
                        },
                        next_store,
                    );
                    (forward, headers_changed, persisted)
                },
            )
        };
        match result {
            Ok((forward, headers_changed, Ok(changed))) => {
                if finality {
                    self.gossip_counts.finality_update += 1;
                } else {
                    self.gossip_counts.optimistic_update += 1;
                }
                if headers_changed {
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                }
                if changed {
                    self.drive_rpc_requests();
                }
                if forward {
                    self.pending_gossip_forward = Some(metadata);
                    Accept
                } else {
                    Ignore
                }
            }
            Ok((_, _, Err(error))) => {
                tracing::warn!(%propagation_source, %error, "failed to persist verified gossip update");
                Ignore
            }
            // Missing committees are a local catch-up condition, not evidence
            // that the sender supplied an invalid proof or signature.
            Err(
                LightClientVerificationError::IrrelevantUpdate { .. }
                | LightClientVerificationError::UnknownSyncCommitteePeriod { .. },
            ) => Ignore,
            Err(error) => {
                self.gossip_counts.decode_failures += 1;
                tracing::debug!(%propagation_source, %error, "failed to verify consensus gossip update");
                Reject
            }
        }
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
                                    .light_client_bootstrap_payload()
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
                            .light_client_finality_update_payload()
                            .map(Eth2RpcResponse::LightClientFinalityUpdate)
                            .unwrap_or_else(|| {
                                resource_unavailable(
                                    "light-client finality update is not yet available locally",
                                )
                            }),
                        Eth2RpcRequest::LightClientOptimisticUpdate => self
                            .consensus
                            .light_client_optimistic_update_payload()
                            .map(Eth2RpcResponse::LightClientOptimisticUpdate)
                            .unwrap_or_else(|| {
                                resource_unavailable(
                                    "light-client optimistic update is not yet available locally",
                                )
                            }),
                        Eth2RpcRequest::LightClientUpdatesByRange(request) => {
                            let responses =
                                self.consensus.light_client_update_payloads(request.start_period, request.count);
                            if responses.is_empty() {
                                resource_unavailable(
                                    "light-client updates by range are not yet available locally",
                                )
                            } else {
                                Eth2RpcResponse::LightClientUpdatesByRange(responses)
                            }
                        }
                        Eth2RpcRequest::BeaconBlocksByRange(request) => {
                            Eth2RpcResponse::CachedBeaconBlocksByRange(
                                self.cached_verified_beacon_blocks_by_range(request),
                            )
                        }
                        Eth2RpcRequest::BeaconBlocksByRoot(roots) => {
                            Eth2RpcResponse::CachedBeaconBlocksByRoot(
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
                } => {
                    let (response, _reservation) = response.into_parts();
                    self.handle_rpc_response(kind, peer, request_id, response);
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                if self.take_pending_request(kind, request_id).is_none() {
                    return;
                }
                self.pending_light_client_range_requests
                    .remove(&PendingRequestKey { kind, request_id });
                let failed_count = match kind {
                    RpcRequestKind::BeaconBlocksByRoot => {
                        Some(self.take_pending_history_root_request(request_id).len() as u64)
                    }
                    RpcRequestKind::BeaconBlocksByRange => self
                        .take_pending_history_range_request(request_id)
                        .map(|request| request.count),
                    _ => None,
                };
                if self.handle_local_rpc_failure(kind, &error, failed_count, Instant::now()) {
                    return;
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
                    let (response, _reservation) = response.into_parts();
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
                if self
                    .take_pending_request(RpcRequestKind::Goodbye, request_id)
                    .is_none()
                {
                    return;
                }
                if self.handle_local_rpc_failure(
                    RpcRequestKind::Goodbye,
                    &error,
                    None,
                    Instant::now(),
                ) {
                    self.disconnect_now(peer);
                    return;
                }
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
                        .send_response(channel, response.into());
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
                } => {
                    let (response, _reservation) = response.into_parts();
                    self.handle_rpc_response(RpcRequestKind::MetaData, peer, request_id, response);
                }
            },
            request_response::Event::OutboundFailure {
                peer,
                request_id,
                error,
                ..
            } => {
                if self
                    .take_pending_request(RpcRequestKind::MetaData, request_id)
                    .is_none()
                {
                    return;
                }
                if self.handle_local_rpc_failure(
                    RpcRequestKind::MetaData,
                    &error,
                    None,
                    Instant::now(),
                ) {
                    return;
                }
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
        let requested_light_client_range = self
            .pending_light_client_range_requests
            .remove(&PendingRequestKey { kind, request_id });
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
                        return;
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
                let Some(request) = requested_light_client_range else {
                    tracing::warn!(%peer, ?request_id, "missing light-client range request metadata");
                    return;
                };
                if chunks.len() as u64 > request.count {
                    self.record_invalid_light_client_response(
                        peer,
                        kind,
                        format!(
                            "range response has {} chunks for count {}",
                            chunks.len(),
                            request.count
                        ),
                    );
                    return;
                }
                let chunk_count = chunks.len();
                let total_bytes = chunks.iter().map(|chunk| chunk.bytes.len()).sum::<usize>();
                let Some(mut store) = self.consensus.light_client_store() else {
                    tracing::debug!(
                        %peer,
                        chunks = chunk_count,
                        total_bytes,
                        "ignoring light-client updates by range response until a verified bootstrap exists"
                    );
                    return;
                };
                let mut applied = None;
                let mut verified_updates_by_period = Vec::new();
                let mut previous_period = None;
                for chunk in chunks {
                    match apply_light_client_update_payload(&chunk.bytes, &store) {
                        Ok(next) => {
                            let attested_slot = next.optimistic_status.attested_header.beacon_slot;
                            if !rpc_context_matches_slot(&chunk, attested_slot) {
                                self.record_invalid_light_client_response(peer, kind, format!(
                                    "fork context {:?} does not match update attested slot {attested_slot}",
                                    chunk.context_bytes
                                ));
                                return;
                            }
                            let period = sync_committee_period_for_slot(attested_slot);
                            if !light_client_range_contains_next_period(
                                request,
                                previous_period,
                                period,
                            ) {
                                self.record_invalid_light_client_response(peer, kind,
                                    format!("invalid range response period {period} after {previous_period:?} for {request:?}"));
                                return;
                            }
                            previous_period = Some(period);
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
                            verified_updates_by_period.push((period, chunk));
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
                                self.record_invalid_light_client_response(
                                    peer,
                                    kind,
                                    error.to_string(),
                                );
                            }
                            // A concurrently advanced store can make a response
                            // unusable without peer fault. Stop without publishing
                            // an incomplete sequence or blaming stale responses.
                            return;
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
                        return;
                    }
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                } else {
                    tracing::warn!(
                        %peer,
                        chunks = chunk_count,
                        total_bytes,
                        "light-client updates by range stream did not yield a usable verified update"
                    );
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
                        let headers_changed = next_store.finalized_header != store.finalized_header
                            || next_store.optimistic_header != store.optimistic_header;
                        let changed = match self
                            .consensus
                            .record_verified_finality_update(summary, payload, next_store)
                        {
                            Ok(changed) => changed,
                            Err(error) => {
                                tracing::warn!(%peer, %error, "failed to persist verified finality update");
                                return;
                            }
                        };
                        if headers_changed {
                            self.seed_verified_light_client_headers();
                            self.materialize_verified_anchor_segments();
                        }
                        if changed {
                            self.drive_rpc_requests();
                        }
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
                        let headers_changed = next_store.finalized_header != store.finalized_header
                            || next_store.optimistic_header != store.optimistic_header;
                        let changed = match self
                            .consensus
                            .record_verified_optimistic_update(summary, payload, next_store)
                        {
                            Ok(changed) => changed,
                            Err(error) => {
                                tracing::warn!(%peer, %error, "failed to persist verified optimistic update");
                                return;
                            }
                        };
                        if headers_changed {
                            self.seed_verified_light_client_headers();
                            self.materialize_verified_anchor_segments();
                        }
                        if changed {
                            self.drive_rpc_requests();
                        }
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
                let Some(request) = requested_history_range else {
                    tracing::warn!(%peer, ?request_id, "missing beacon range request metadata");
                    return;
                };
                if chunks.len() as u64 > request.count {
                    self.disconnect_faulty_history_peer(
                        peer,
                        kind,
                        format!(
                            "range response has {} chunks for count {}",
                            chunks.len(),
                            request.count
                        ),
                    );
                    return;
                }
                let chunk_count = chunks.len();
                let total_bytes = chunks.iter().map(|chunk| chunk.bytes.len()).sum::<usize>();
                tracing::debug!(
                    %peer,
                    chunks = chunk_count,
                    total_bytes,
                    "received beacon blocks by range response stream"
                );
                let mut decoded_blocks = Vec::new();
                let mut invalid_response = false;
                for chunk in chunks {
                    match decode_verified_beacon_block(&chunk) {
                        Ok(block) => {
                            if !range_request_contains_slot(request, block.slot) {
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
                            decoded_blocks.push((block, chunk));
                        }
                        Err(error) => {
                            tracing::warn!(
                                %peer,
                                bytes = chunk.bytes.len(),
                                %error,
                                "failed to decode or verify beacon block from range response"
                            );
                            invalid_response = true;
                        }
                    }
                }
                if !valid_history_range_sequence(
                    request,
                    decoded_blocks.iter().map(|(block, _)| block),
                ) {
                    invalid_response = true;
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
                            chunk_count,
                            total_bytes
                        ),
                    );
                    return;
                }

                let largest_body = decoded_blocks
                    .iter()
                    .map(|(_, payload)| payload.bytes.len())
                    .max()
                    .unwrap_or(0);
                self.record_peer_success(peer, RpcRequestKind::BeaconBlocksByRange);
                self.beacon_blocks_by_range_peers.insert(peer);
                let mut inserted = 0usize;
                for (block, payload) in decoded_blocks {
                    if self.record_verified_beacon_block(block, Some(payload)) {
                        inserted += 1;
                    }
                }
                self.history_range_batch_limit = recovered_history_batch_limit(
                    self.history_range_batch_limit,
                    request.count,
                    chunk_count,
                    largest_body,
                    inserted > 0,
                );
                if inserted > 0 {
                    self.seed_verified_light_client_headers();
                    self.materialize_verified_anchor_segments();
                    self.drive_rpc_requests();
                }
            }
            (RpcRequestKind::BeaconBlocksByRoot, Eth2RpcResponse::BeaconBlocksByRoot(chunks)) => {
                if requested_history_roots.is_empty() {
                    tracing::warn!(%peer, ?request_id, "missing beacon root request metadata");
                    return;
                }
                if chunks.len() > requested_history_roots.len() {
                    self.disconnect_faulty_history_peer(
                        peer,
                        kind,
                        format!(
                            "root response has {} chunks for {} requested roots",
                            chunks.len(),
                            requested_history_roots.len()
                        ),
                    );
                    return;
                }
                let chunk_count = chunks.len();
                let total_bytes = chunks.iter().map(|chunk| chunk.bytes.len()).sum::<usize>();
                tracing::debug!(
                    %peer,
                    chunks = chunk_count,
                    total_bytes,
                    "received beacon blocks by root response stream"
                );
                let mut decoded_blocks = Vec::new();
                let mut invalid_response = false;
                for chunk in chunks {
                    match decode_verified_beacon_block(&chunk) {
                        Ok(block) => {
                            if !requested_history_roots.contains(&block.beacon_root) {
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
                            decoded_blocks.push((block, chunk));
                        }
                        Err(error) => {
                            tracing::warn!(
                                %peer,
                                bytes = chunk.bytes.len(),
                                %error,
                                "failed to decode or verify beacon block from root response"
                            );
                            invalid_response = true;
                        }
                    }
                }
                let mut seen_roots = HashSet::new();
                if decoded_blocks
                    .iter()
                    .any(|(block, _)| !seen_roots.insert(block.beacon_root))
                {
                    invalid_response = true;
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
                            chunk_count,
                            total_bytes
                        ),
                    );
                    return;
                }

                let largest_body = decoded_blocks
                    .iter()
                    .map(|(_, payload)| payload.bytes.len())
                    .max()
                    .unwrap_or(0);
                self.record_peer_success(peer, RpcRequestKind::BeaconBlocksByRoot);
                self.beacon_blocks_by_root_peers.insert(peer);
                let mut inserted = 0usize;
                for (block, payload) in decoded_blocks {
                    if self.record_verified_beacon_block(block, Some(payload)) {
                        inserted += 1;
                    }
                }
                self.history_root_batch_limit = recovered_history_batch_limit(
                    self.history_root_batch_limit as u64,
                    requested_history_roots.len() as u64,
                    chunk_count,
                    largest_body,
                    inserted > 0,
                ) as usize;
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
        if self.storage_failure.borrow().is_some() {
            return;
        }
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
        if self.storage_failure.borrow().is_some() {
            return;
        }
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
            if self.closing_peers.contains(&peer) {
                continue;
            }
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
        channel: request_response::ResponseChannel<BudgetedRpcResponse>,
        response: Eth2RpcResponse,
    ) -> Result<(), Eth2RpcResponse> {
        let payload_bytes = consensus_response_payload_bytes(&response);
        let result = match kind {
            RpcRequestKind::Status => self
                .swarm
                .behaviour_mut()
                .status_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::Goodbye => self
                .swarm
                .behaviour_mut()
                .goodbye_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::MetaData => self
                .swarm
                .behaviour_mut()
                .metadata_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::Ping => self
                .swarm
                .behaviour_mut()
                .ping_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::LightClientBootstrap => self
                .swarm
                .behaviour_mut()
                .light_client_bootstrap_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::LightClientUpdatesByRange => self
                .swarm
                .behaviour_mut()
                .light_client_updates_by_range_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::LightClientFinalityUpdate => self
                .swarm
                .behaviour_mut()
                .light_client_finality_update_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::LightClientOptimisticUpdate => self
                .swarm
                .behaviour_mut()
                .light_client_optimistic_update_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::BeaconBlocksByRange => self
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_range_rpc
                .inner
                .send_response(channel, response.into()),
            RpcRequestKind::BeaconBlocksByRoot => self
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_root_rpc
                .inner
                .send_response(channel, response.into()),
        };
        if result.is_ok() {
            self.record_p2p_upload_payload(payload_bytes);
        }
        result.map_err(|response| response.into_parts().0)
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
        if !self.local_rpc_retry_ready(kind, Instant::now())
            || self.is_request_satisfied(peer, kind)
            || self.is_request_pending(peer, kind)
        {
            return;
        }

        let Some(request) = self.build_request(kind) else {
            return;
        };
        let payload_bytes = consensus_request_payload_bytes(&request);
        let requested_light_client_range = match &request {
            Eth2RpcRequest::LightClientUpdatesByRange(request) => Some(*request),
            _ => None,
        };
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
        if let Some(request) = requested_light_client_range {
            self.pending_light_client_range_requests
                .insert(key, request);
        }
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
        .map(|mut roots| {
            roots.truncate(self.history_root_batch_limit);
            roots
        })
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
        next_forward_history_range_request_for_progress(progress).map(|mut request| {
            request.count = request.count.min(self.history_range_batch_limit);
            request
        })
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
        if let Some(mut payload) = payload {
            // Decoding already checked any supplied context. V1 carries none;
            // retain the canonical context so the same body can serve V2.
            payload.context_bytes.get_or_insert_with(|| {
                MAINNET_CONSENSUS_CHAIN_SPEC
                    .fork_digest_for_epoch(MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(block.slot))
            });
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

    fn cached_verified_beacon_blocks_by_root(&self, roots: &[B256]) -> Vec<CachedBeaconPayload> {
        cached_beacon_block_payloads_by_root(roots, &self.verified_beacon_block_payloads)
    }

    fn cached_verified_beacon_blocks_by_range(
        &self,
        request: BeaconBlocksByRangeRequest,
    ) -> Vec<CachedBeaconPayload> {
        let canonical_blocks = self.canonical_serving_blocks();
        cached_beacon_block_payloads_by_range(
            request,
            &canonical_blocks,
            &self.verified_beacon_block_payloads,
        )
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
            return;
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

    fn dial_address_priority(&self, peer: PeerId, addr: &Multiaddr, bootstrap_needed: bool) -> i64 {
        dial_address_class(addr)
            .map(|class| {
                self.peer_lifecycle
                    .get(&peer)
                    .map(|lifecycle| lifecycle.dial_stats.priority(class, bootstrap_needed))
                    .unwrap_or_else(|| {
                        PeerDialAddressStats::default().priority(class, bootstrap_needed)
                    })
            })
            .unwrap_or(i64::MIN / 2)
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
        if self.swarm.disconnect_peer_id(peer).is_err() {
            // No connection remains, so no later ConnectionClosed event will
            // clear this marker (e.g. a queued inbound Goodbye failure).
            self.closing_peers.remove(&peer);
        }
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
            self.pending_light_client_range_requests.remove(&key);
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

    fn local_rpc_retry_ready(&self, kind: RpcRequestKind, now: Instant) -> bool {
        self.local_rpc_retry_after
            .get(&kind)
            .is_none_or(|deadline| now >= *deadline)
    }

    fn handle_local_rpc_failure(
        &mut self,
        kind: RpcRequestKind,
        error: &request_response::OutboundFailure,
        failed_count: Option<u64>,
        now: Instant,
    ) -> bool {
        let request_response::OutboundFailure::Io(error) = error else {
            return false;
        };
        let Some(local) = from_io(error) else {
            return false;
        };
        let history = matches!(
            kind,
            RpcRequestKind::BeaconBlocksByRange | RpcRequestKind::BeaconBlocksByRoot
        );
        let deadline = now + Duration::from_secs(1);
        match local {
            RpcMemoryError::ResponseLimit { .. } => {
                if let Some(count) = failed_count.filter(|count| *count > 0) {
                    let reduced = (count / 2).max(1);
                    match kind {
                        RpcRequestKind::BeaconBlocksByRange => {
                            self.history_range_batch_limit =
                                self.history_range_batch_limit.min(reduced)
                        }
                        RpcRequestKind::BeaconBlocksByRoot => {
                            self.history_root_batch_limit = self
                                .history_root_batch_limit
                                .min(usize::try_from(reduced).unwrap_or(usize::MAX))
                        }
                        _ => {}
                    }
                }
                self.local_rpc_retry_after.insert(kind, deadline);
            }
            RpcMemoryError::Capacity { .. } | RpcMemoryError::Allocation { .. } => {
                if history {
                    self.local_rpc_retry_after
                        .insert(RpcRequestKind::BeaconBlocksByRange, deadline);
                    self.local_rpc_retry_after
                        .insert(RpcRequestKind::BeaconBlocksByRoot, deadline);
                } else {
                    self.local_rpc_retry_after.insert(kind, deadline);
                }
            }
        }
        self.last_rpc_failure = Some(format!(
            "local response resource limit request={} error={local}",
            kind.as_str()
        ));
        tracing::debug!(request = kind.as_str(), %local, "deferring consensus RPC after local resource limit");
        true
    }

    fn can_issue_request(&self, kind: RpcRequestKind) -> bool {
        self.local_rpc_retry_ready(kind, Instant::now())
            && self.pending_requests_for_kind(kind) < max_concurrent_requests_for_kind(kind)
    }

    fn refresh_status(&mut self) {
        if self.storage_failure.borrow().is_some() {
            mark_consensus_network_unavailable(&self.sync_status);
            return;
        }
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

    fn selected_known_peers(&self, enrs: Vec<Enr>) -> Vec<PersistedPeer> {
        let bootstrap_needed = self.consensus.light_client_status().bootstrap.is_none();
        let head_progression_needed = !bootstrap_needed
            && live_head_progression_needed(
                self.latest_history_sync_target(),
                current_wall_clock_slot(),
            );
        let mut peers = enrs
            .into_iter()
            .map(|enr| {
                peer_id_from_enr(&enr)
                    .ok()
                    .and_then(|peer| self.peer_lifecycle.get(&peer))
                    .and_then(|state| state.latest_enr.as_ref())
                    .filter(|record| record.enr.seq() >= enr.seq())
                    .map(|record| record.enr.clone())
                    .unwrap_or(enr)
            })
            .filter(|enr| enr_is_relevant_consensus_peer(enr, &self.fork_digest))
            .filter_map(|enr| {
                let (peer_id, _) = enr_multiaddrs_for_families(self.config.dial_families, &enr)?;
                let peer_state = self.peer_lifecycle.get(&peer_id);
                let support = peer_state.and_then(PeerLifecycleState::persisted_support);
                if support.is_some_and(|support| !support.status) {
                    return None;
                }
                let peer = PersistedPeer {
                    enr: enr.to_base64(),
                    support,
                    status_successes: peer_state
                        .map(|state| state.status_successes)
                        .unwrap_or_default(),
                    bootstrap_successes: peer_state
                        .map(|state| state.bootstrap_successes)
                        .unwrap_or_default(),
                    useful_successes: peer_state
                        .map(|state| state.useful_successes)
                        .unwrap_or_default(),
                    dial_stats: peer_state.map(|state| state.dial_stats).unwrap_or_default(),
                };
                // The routing table already holds decoded, verified records.
                // Compute priority once instead of reparsing and reverifying the
                // serialized ENR for both sides of every sort comparison.
                Some((
                    self.peer_priority(peer_id, bootstrap_needed, head_progression_needed),
                    peer,
                ))
            })
            .collect::<Vec<_>>();
        peers.sort_by(|(left_priority, left), (right_priority, right)| {
            right_priority
                .cmp(left_priority)
                .then_with(|| left.enr.cmp(&right.enr))
        });
        peers.truncate(MAX_PERSISTED_KNOWN_PEERS);
        peers.into_iter().map(|(_, peer)| peer).collect()
    }

    fn persist_known_peers(&mut self) -> Result<(), ConsensusNetworkError> {
        let peers = self.selected_known_peers(self.discv5.table_entries_enr());

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
            let response_budgets = RpcResponseBudgets::default();
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
                    inner: build_status_behaviour(response_budgets.clone()),
                },
                goodbye_rpc: GoodbyeRpcBehaviour {
                    inner: build_goodbye_behaviour(response_budgets.clone()),
                },
                metadata_rpc: MetadataRpcBehaviour {
                    inner: build_metadata_behaviour(response_budgets.clone()),
                },
                ping_rpc: PingRpcBehaviour {
                    inner: build_ping_behaviour(response_budgets.clone()),
                },
                light_client_bootstrap_rpc: LightClientBootstrapRpcBehaviour {
                    inner: build_light_client_bootstrap_behaviour(response_budgets.clone()),
                },
                light_client_updates_by_range_rpc: LightClientUpdatesByRangeRpcBehaviour {
                    inner: build_light_client_updates_by_range_behaviour(response_budgets.clone()),
                },
                light_client_finality_update_rpc: LightClientFinalityUpdateRpcBehaviour {
                    inner: build_light_client_finality_update_behaviour(response_budgets.clone()),
                },
                light_client_optimistic_update_rpc: LightClientOptimisticUpdateRpcBehaviour {
                    inner: build_light_client_optimistic_update_behaviour(response_budgets.clone()),
                },
                beacon_blocks_by_range_rpc: BeaconBlocksByRangeRpcBehaviour {
                    inner: build_beacon_blocks_by_range_behaviour(response_budgets.clone()),
                },
                beacon_blocks_by_root_rpc: BeaconBlocksByRootRpcBehaviour {
                    inner: build_beacon_blocks_by_root_behaviour(response_budgets.clone()),
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

// This transform preserves wire bytes. Global size failures are rejected by
// gossipsub before ID generation and do not reach application-event counters.
// Malformed Snappy within those bounds still receives the required INVALID ID.
#[derive(Default)]
struct GossipSizeGuard;

fn validate_gossip_wire_size(payload: &[u8], max_decoded: usize) -> io::Result<()> {
    if payload.len() > snap::raw::max_compress_len(max_decoded) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "gossip compressed payload exceeds global limit",
        ));
    }
    if snap::raw::decompress_len(payload).is_ok_and(|length| length > max_decoded) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "gossip declared payload exceeds global limit",
        ));
    }
    Ok(())
}

impl gossipsub::DataTransform for GossipSizeGuard {
    fn inbound_transform(&self, message: gossipsub::RawMessage) -> io::Result<gossipsub::Message> {
        validate_gossip_wire_size(&message.data, GOSSIP_MAX_PAYLOAD_SIZE)?;
        Ok(gossipsub::Message {
            source: message.source,
            data: message.data,
            sequence_number: message.sequence_number,
            topic: message.topic,
        })
    }

    fn outbound_transform(&self, _: &gossipsub::TopicHash, data: Vec<u8>) -> io::Result<Vec<u8>> {
        validate_gossip_wire_size(&data, GOSSIP_MAX_PAYLOAD_SIZE)?;
        Ok(data)
    }
}

fn build_gossip_config() -> Result<gossipsub::Config, ConsensusNetworkError> {
    gossipsub::ConfigBuilder::default()
        .validation_mode(gossipsub::ValidationMode::Anonymous)
        .validate_messages()
        // Phase0 networking parameters; mainnet seen_ttl is 12 * 32 * 2.
        .mesh_n(8)
        .mesh_n_low(6)
        .mesh_n_high(12)
        .gossip_lazy(6)
        .heartbeat_interval(Duration::from_millis(700))
        .fanout_ttl(Duration::from_secs(60))
        .history_length(6)
        .history_gossip(3)
        .duplicate_cache_time(Duration::from_secs(768))
        .max_transmit_size(snap::raw::max_compress_len(GOSSIP_MAX_PAYLOAD_SIZE) + 1024)
        .message_id_fn(eth2_message_id)
        .build()
        .map_err(|error| ConsensusNetworkError::ConstructGossip(error.to_string()))
}

fn build_gossip_behaviour() -> Result<gossipsub::Behaviour<GossipSizeGuard>, ConsensusNetworkError>
{
    gossipsub::Behaviour::new_with_transform(
        gossipsub::MessageAuthenticity::Anonymous,
        build_gossip_config()?,
        GossipSizeGuard,
    )
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
    let decoded = decode_gossip_payload(&message.data, GOSSIP_MAX_PAYLOAD_SIZE);
    let (domain, payload) = match &decoded {
        Some(payload) => (MESSAGE_DOMAIN_VALID_SNAPPY, payload.as_slice()),
        None => (MESSAGE_DOMAIN_INVALID_SNAPPY, message.data.as_slice()),
    };
    hasher.update(domain);
    hasher.update(usize_to_u64(topic.len()).to_le_bytes());
    hasher.update(topic);
    hasher.update(payload);
    let digest = hasher.finalize();
    gossipsub::MessageId::from(digest[..20].to_vec())
}

// Admit only canonical, supported LC topic names. Recognized old/future topics
// are classified separately from malformed or unknown topics by the caller.
fn parse_light_client_topic(topic: &gossipsub::TopicHash) -> Option<(bool, [u8; 4])> {
    let mut parts = topic.as_str().split('/');
    if parts.next() != Some("") || parts.next() != Some("eth2") {
        return None;
    }
    let encoded_digest = parts.next()?;
    let finality = match parts.next()? {
        LIGHT_CLIENT_FINALITY_UPDATE_TOPIC_NAME => true,
        LIGHT_CLIENT_OPTIMISTIC_UPDATE_TOPIC_NAME => false,
        _ => return None,
    };
    if parts.next() != Some(GOSSIP_ENCODING_NAME)
        || parts.next().is_some()
        || encoded_digest.len() != 8
    {
        return None;
    }
    let mut digest = [0u8; 4];
    hex::decode_to_slice(encoded_digest, &mut digest).ok()?;
    if !encoded_digest
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let spec = MAINNET_CONSENSUS_CHAIN_SPEC;
    let known = spec
        .fork_schedule
        .iter()
        .any(|fork| fork.version[0] >= 3 && spec.fork_digest_for_epoch(fork.epoch) == digest)
        || spec
            .blob_schedule
            .iter()
            .any(|fork| spec.fork_digest_for_epoch(fork.epoch) == digest);
    known.then_some((finality, digest))
}

fn gossip_slot_at(now_ms: u128) -> u64 {
    let elapsed =
        now_ms.saturating_sub(u128::from(MAINNET_CONSENSUS_CHAIN_SPEC.genesis_time) * 1000);
    u64::try_from(elapsed / 12_000).unwrap_or(u64::MAX)
}

fn gossip_update_is_due(signature_slot: u64, now_ms: u128) -> bool {
    let start = u128::from(MAINNET_CONSENSUS_CHAIN_SPEC.genesis_time) * 1000
        + u128::from(signature_slot) * 12_000;
    // Mainnet through Fulu: 12000 * 3333 // 10000, with 500 ms clock allowance.
    now_ms.saturating_add(500) >= start + 3_999
}

fn gossip_payload_limit(topic: &gossipsub::TopicHash) -> usize {
    let mut components = topic.as_str().rsplit('/');
    if components.next() != Some(GOSSIP_ENCODING_NAME) {
        return GOSSIP_MAX_PAYLOAD_SIZE;
    }
    let protocol = match components.next() {
        Some(LIGHT_CLIENT_FINALITY_UPDATE_TOPIC_NAME) => {
            crate::rpc::Eth2RpcProtocol::LightClientFinalityUpdateV1
        }
        Some(LIGHT_CLIENT_OPTIMISTIC_UPDATE_TOPIC_NAME) => {
            crate::rpc::Eth2RpcProtocol::LightClientOptimisticUpdateV1
        }
        _ => return GOSSIP_MAX_PAYLOAD_SIZE,
    };
    crate::rpc::response_payload_limit(&protocol, 0)
}

fn decode_gossip_payload(payload: &[u8], max_decoded: usize) -> Option<Vec<u8>> {
    let decompressed_len = snap::raw::decompress_len(payload).ok()?;
    if decompressed_len > max_decoded.min(GOSSIP_MAX_PAYLOAD_SIZE) {
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

fn retain_cached_peer_enr(
    lifecycle: &mut HashMap<PeerId, PeerLifecycleState>,
    peers: &mut HashMap<PeerId, Vec<Multiaddr>>,
    restored_metadata: &mut HashSet<PeerId>,
    families: ConsensusDialAddressFamilies,
    fork_digest: &[u8; 4],
    enr: &Enr,
    cached: &PersistedPeer,
) -> Option<PeerId> {
    let (peer, accepted) = retain_peer_enr(lifecycle, peers, families, fork_digest, enr, false)?;
    let state = lifecycle.get_mut(&peer).expect("retained peer");
    let canonical = state.latest_enr.as_ref().expect("retained ENR");
    // Equal records may restore bootnode metadata once; stale or conflicting
    // cache entries must not replace the winner's scores.
    if accepted || (canonical.enr == *enr && !restored_metadata.contains(&peer)) {
        let latest_enr = state.latest_enr.take();
        *state = PeerLifecycleState::from_persisted(cached);
        state.latest_enr = latest_enr;
        restored_metadata.insert(peer);
    }
    refresh_peer_enr_addresses(peers, families, fork_digest, peer, state);
    Some(peer)
}

// Sequence authority shares the lifecycle record's existing inactive eviction.
// Equal/stale announcements can refresh eligibility, never canonical content.
fn retain_peer_enr(
    lifecycle: &mut HashMap<PeerId, PeerLifecycleState>,
    peers: &mut HashMap<PeerId, Vec<Multiaddr>>,
    families: ConsensusDialAddressFamilies,
    fork_digest: &[u8; 4],
    enr: &Enr,
    bootstrap: bool,
) -> Option<(PeerId, bool)> {
    let peer = peer_id_from_enr(enr).ok()?;
    let state = lifecycle.entry(peer).or_default();
    let accepted = state
        .latest_enr
        .as_ref()
        .is_none_or(|record| enr.seq() > record.enr.seq());
    if accepted {
        state.latest_enr = Some(RetainedPeerEnr {
            enr: enr.clone(),
            bootstrap,
        });
    }
    refresh_peer_enr_addresses(peers, families, fork_digest, peer, state);
    Some((peer, accepted))
}

fn refresh_peer_enr_addresses(
    peers: &mut HashMap<PeerId, Vec<Multiaddr>>,
    families: ConsensusDialAddressFamilies,
    fork_digest: &[u8; 4],
    peer: PeerId,
    state: &PeerLifecycleState,
) {
    let Some(record) = &state.latest_enr else {
        return;
    };
    let addresses = (!state
        .remembered_support
        .is_some_and(|support| !support.status)
        && (record.bootstrap || enr_is_relevant_consensus_peer(&record.enr, fork_digest)))
    .then(|| enr_multiaddrs_for_families(families, &record.enr))
    .flatten();
    if let Some((_, addresses)) = addresses {
        peers.insert(peer, addresses);
    } else {
        peers.remove(&peer);
    }
}

fn enr_multiaddrs_for_families(
    families: ConsensusDialAddressFamilies,
    enr: &Enr,
) -> Option<(PeerId, Vec<Multiaddr>)> {
    let (peer_id, mut addrs) = enr_multiaddrs(enr)?;
    retain_dial_addresses_for_families(families, &mut addrs);
    (!addrs.is_empty()).then_some((peer_id, addrs))
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
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(ConsensusNetworkError::ReadKnownPeers {
                path: path.to_path_buf(),
                source,
            });
        }
    };
    let mut contents = Vec::new();
    file.take((MAX_KNOWN_PEERS_BYTES + 1) as u64)
        .read_to_end(&mut contents)
        .map_err(|source| ConsensusNetworkError::ReadKnownPeers {
            path: path.to_path_buf(),
            source,
        })?;
    let decoded = if contents.len() > MAX_KNOWN_PEERS_BYTES {
        Err("known-peer cache exceeds byte limit".to_owned())
    } else {
        let mut decoder = serde_json::Deserializer::from_slice(&contents);
        serde::de::Deserializer::deserialize_seq(&mut decoder, KnownPeersVisitor)
            .and_then(|peers| decoder.end().map(|()| peers))
            .map_err(|error| error.to_string())
    };
    match decoded {
        Ok(peers) => Ok(peers),
        Err(reason) => {
            let retained = quarantine_known_peers(path).map_err(|source| {
                ConsensusNetworkError::QuarantineKnownPeers {
                    path: path.to_path_buf(),
                    source,
                }
            })?;
            tracing::warn!(path = %path.display(), quarantine = %retained.display(), %reason,
                "preserved damaged known-peer cache; continuing without cached peers");
            Ok(Vec::new())
        }
    }
}

struct KnownPeersVisitor;

impl<'de> serde::de::Visitor<'de> for KnownPeersVisitor {
    type Value = Vec<PersistedPeer>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "at most {MAX_PERSISTED_KNOWN_PEERS} known-peer records"
        )
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut peers = Vec::new();
        while peers.len() < MAX_PERSISTED_KNOWN_PEERS {
            match seq.next_element::<PersistedPeer>()? {
                Some(peer) => peers.push(peer),
                None => return Ok(peers),
            }
        }
        if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::custom(
                "known-peer cache exceeds record limit",
            ));
        }
        Ok(peers)
    }
}

fn quarantine_known_peers(path: &Path) -> io::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let directory = tempfile::Builder::new()
        .prefix(".known-peers-quarantine-")
        .tempdir_in(parent)?;
    let retained = directory.path().join(KNOWN_PEERS_FILE);
    fs::rename(path, &retained)?;
    // Once the original moves, no subsequent cleanup may remove this evidence.
    let _ = directory.keep();
    Ok(retained)
}

fn persist_known_peers(path: &Path, peers: &[PersistedPeer]) -> Result<(), ConsensusNetworkError> {
    let write = || -> io::Result<()> {
        if peers.len() > MAX_PERSISTED_KNOWN_PEERS
            || peers
                .iter()
                .any(|peer| peer.enr.len() > MAX_PERSISTED_ENR_BYTES)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "known-peer records exceed writer limits",
            ));
        }
        let json = serde_json::to_vec_pretty(peers).map_err(io::Error::other)?;
        if json.len() > MAX_KNOWN_PEERS_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "known-peer cache exceeds byte limit",
            ));
        }
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let mut temporary = tempfile::Builder::new()
            .prefix(".known-peers-")
            .tempfile_in(parent)?;
        temporary.write_all(&json)?;
        // These derived hints need atomic visibility, not periodic power-loss barriers.
        temporary.persist(path).map_err(|error| error.error)?;
        Ok(())
    };
    write().map_err(|source| ConsensusNetworkError::PersistKnownPeers {
        path: path.to_path_buf(),
        source,
    })
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

    fn retention_peer(index: u16) -> PeerId {
        let mut seed = [1; 32];
        seed[..2].copy_from_slice(&index.to_le_bytes());
        identity::Keypair::ed25519_from_bytes(seed)
            .unwrap()
            .public()
            .to_peer_id()
    }

    fn freshness_enr(index: u16, seq: u64, port: Option<u16>, ipv6: bool, digest: [u8; 4]) -> Enr {
        let mut secret = [1; 32];
        secret[..2].copy_from_slice(&index.to_le_bytes());
        let key = CombinedKey::secp256k1_from_bytes(&mut secret).unwrap();
        let mut fork_id = [0; 16];
        fork_id[..4].copy_from_slice(&digest);
        fork_id[8..].copy_from_slice(&u64::MAX.to_le_bytes());
        let mut builder = Enr::builder();
        builder.seq(seq).add_value("eth2", &fork_id);
        if ipv6 {
            builder.ip6(Ipv6Addr::LOCALHOST).udp6(9000);
            if let Some(port) = port {
                builder.tcp6(port);
            }
        } else {
            builder.ip4(Ipv4Addr::LOCALHOST).udp4(9000);
            if let Some(port) = port {
                builder.tcp4(port);
            }
        }
        builder.build(&key).unwrap()
    }

    #[tokio::test]
    async fn peer_freshness_older_and_equal_enrs_do_not_replace_latest_endpoints() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let older = freshness_enr(1, 1, Some(9001), false, network.fork_digest);
        let newer = freshness_enr(1, 2, Some(9002), false, network.fork_digest);
        let conflict = freshness_enr(1, 2, Some(9999), false, network.fork_digest);
        let peer = peer_id_from_enr(&newer).unwrap();
        network.observe_enr(&older);
        network.observe_enr(&newer);
        let expected = vec![
            format!("/ip4/127.0.0.1/tcp/9002/p2p/{peer}")
                .parse::<Multiaddr>()
                .unwrap(),
        ];
        assert_eq!(network.dialable_peers[&peer], expected);
        for enr in [&older, &conflict, &newer] {
            network.observe_enr(enr);
            assert_eq!(network.dialable_peers[&peer], expected);
        }
    }

    #[tokio::test]
    async fn peer_freshness_newer_withdrawals_remove_inventory_without_canceling_requests() {
        for reason in 0..3 {
            let temp = TempDir::new().unwrap();
            let mut network = peer_retention_fixture(&temp);
            let older = freshness_enr(1, 1, Some(9001), false, network.fork_digest);
            let newer = freshness_enr(
                1,
                2,
                if reason == 0 { None } else { Some(9002) },
                reason == 1,
                if reason == 2 {
                    [0xff; 4]
                } else {
                    network.fork_digest
                },
            );
            let peer = peer_id_from_enr(&older).unwrap();
            network.observe_enr(&older);
            network.connected_peers.insert(peer);
            network.ensure_request(peer, RpcRequestKind::Status);
            let pending = network.pending_requests.clone();
            network.observe_enr(&newer);
            assert!(!network.dialable_peers.contains_key(&peer));
            assert!(!network.observed.contains(&peer));
            network.observe_enr(&older);
            assert!(!network.dialable_peers.contains_key(&peer));
            assert_eq!(network.pending_requests, pending);
            assert!(network.connected_peers.contains(&peer));
            assert!(!network.closing_peers.contains(&peer));
        }
    }

    #[tokio::test]
    async fn peer_freshness_saved_selection_does_not_restore_withdrawn_table_enr() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let older = freshness_enr(1, 1, Some(9001), false, network.fork_digest);
        let newer = freshness_enr(1, 2, None, false, network.fork_digest);
        network.observe_enr(&older);
        network.observe_enr(&newer);
        assert!(network.selected_known_peers(vec![older]).is_empty());
    }

    #[tokio::test]
    async fn peer_freshness_canonical_records_refresh_and_expire_with_lifecycle() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let digest = network.fork_digest;
        for index in 0..=MAX_RETAINED_DISCONNECTED_PEERS {
            network.observe_enr(&freshness_enr(index as u16, 2, None, false, digest));
        }
        assert!(network.dialable_peers.is_empty());
        assert!(network.observed.is_empty());
        network.prune_inactive_peer_state();
        assert_eq!(
            network.peer_lifecycle.len(),
            MAX_RETAINED_DISCONNECTED_PEERS
        );
        network.prune_inactive_peer_state();
        assert_eq!(
            network.peer_lifecycle.len(),
            MAX_RETAINED_DISCONNECTED_PEERS
        );

        let canonical = freshness_enr(600, 2, Some(9002), false, [0xff; 4]);
        let conflict = freshness_enr(600, 2, Some(9999), false, digest);
        let peer = peer_id_from_enr(&canonical).unwrap();
        network.observe_enr(&canonical);
        network.observe_enr(&conflict);
        assert!(!network.dialable_peers.contains_key(&peer));
        network.fork_digest = [0xff; 4];
        network.refresh_retained_peer_inventory();
        assert!(network.observed.contains(&peer));
        network.fork_digest = digest;
        network.refresh_retained_peer_inventory();
        assert!(!network.observed.contains(&peer));
        assert!(!network.dialable_peers.contains_key(&peer));
        network.fork_digest = [0xff; 4];
        network.refresh_retained_peer_inventory();
        assert!(network.observed.contains(&peer));
        assert_eq!(
            network.dialable_peers[&peer],
            vec![
                format!("/ip4/127.0.0.1/tcp/9002/p2p/{peer}")
                    .parse::<Multiaddr>()
                    .unwrap()
            ]
        );
    }

    #[tokio::test]
    async fn peer_freshness_cached_unsupported_peer_stays_excluded_after_refresh() {
        let temp = TempDir::new().unwrap();
        let network = peer_retention_fixture(&temp);
        let enr = freshness_enr(1, 2, Some(9002), false, network.fork_digest);
        let peer = peer_id_from_enr(&enr).unwrap();
        let cached = PersistedPeer {
            enr: enr.to_base64(),
            support: Some(PeerRpcSupport::default()),
            status_successes: 7,
            bootstrap_successes: 0,
            useful_successes: 0,
            dial_stats: PeerDialAddressStats::default(),
        };
        persist_known_peers(&network.known_peers_path, &[cached]).unwrap();
        let mut loaded = ConsensusNetwork::new(
            network.config.clone(),
            network.consensus.clone(),
            network.sync_status.clone(),
        )
        .unwrap();
        assert!(!loaded.dialable_peers.contains_key(&peer));
        assert_eq!(
            loaded.peer_lifecycle[&peer]
                .latest_enr
                .as_ref()
                .unwrap()
                .enr,
            enr
        );
        loaded.observe_enr(&enr);
        loaded.refresh_retained_peer_inventory();
        assert!(!loaded.dialable_peers.contains_key(&peer));
        assert!(!loaded.observed.contains(&peer));
        assert!(loaded.selected_known_peers(vec![enr]).is_empty());
        loaded.persist_known_peers().unwrap();
        assert!(
            load_known_peers(&loaded.known_peers_path)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn peer_freshness_boot_cache_sequence_and_metadata_ordering() {
        let digest = [0xee; 4];
        let boot = freshness_enr(1, 2, Some(9002), false, [0xff; 4]);
        let older = freshness_enr(1, 1, Some(9001), false, digest);
        let conflict = freshness_enr(1, 2, Some(9999), false, digest);
        let newer = freshness_enr(1, 3, Some(9003), false, digest);
        let withdrawn = freshness_enr(1, 4, None, false, digest);
        for order in [
            vec![&older, &conflict, &newer, &older, &conflict],
            vec![&newer, &conflict, &older],
        ] {
            let mut states = HashMap::new();
            let mut peers = HashMap::new();
            let mut restored = HashSet::new();
            let (peer, _) = retain_peer_enr(
                &mut states,
                &mut peers,
                ConsensusDialAddressFamilies::IPV4,
                &digest,
                &boot,
                true,
            )
            .unwrap();
            assert!(
                peers.contains_key(&peer),
                "built-in bootstrap retains its initial eligibility"
            );
            for enr in order {
                let cached = PersistedPeer {
                    enr: enr.to_base64(),
                    status_successes: enr.seq() as u32,
                    support: None,
                    bootstrap_successes: 0,
                    useful_successes: 0,
                    dial_stats: PeerDialAddressStats::default(),
                };
                retain_cached_peer_enr(
                    &mut states,
                    &mut peers,
                    &mut restored,
                    ConsensusDialAddressFamilies::IPV4,
                    &digest,
                    enr,
                    &cached,
                )
                .unwrap();
            }
            assert_eq!(states[&peer].status_successes, 3);
            assert_eq!(states[&peer].latest_enr.as_ref().unwrap().enr, newer);
            assert!(!states[&peer].latest_enr.as_ref().unwrap().bootstrap);
            assert_eq!(
                peers[&peer],
                vec![
                    format!("/ip4/127.0.0.1/tcp/9003/p2p/{peer}")
                        .parse::<Multiaddr>()
                        .unwrap()
                ]
            );
            let cached = PersistedPeer {
                enr: withdrawn.to_base64(),
                support: Some(PeerRpcSupport::default()),
                status_successes: 0,
                bootstrap_successes: 0,
                useful_successes: 0,
                dial_stats: PeerDialAddressStats::default(),
            };
            retain_cached_peer_enr(
                &mut states,
                &mut peers,
                &mut restored,
                ConsensusDialAddressFamilies::IPV4,
                &digest,
                &withdrawn,
                &cached,
            )
            .unwrap();
            assert!(!peers.contains_key(&peer));
            assert_eq!(states[&peer].latest_enr.as_ref().unwrap().enr.seq(), 4);
            retain_peer_enr(
                &mut states,
                &mut peers,
                ConsensusDialAddressFamilies::IPV4,
                &digest,
                &boot,
                true,
            )
            .unwrap();
            assert!(!peers.contains_key(&peer));
        }
    }

    #[test]
    fn peer_freshness_cache_support_follows_sequence_winner() {
        let digest = [0xee; 4];
        let older = freshness_enr(1, 1, Some(9001), false, digest);
        let newer = freshness_enr(1, 2, Some(9002), false, digest);
        for newer_supported in [false, true] {
            for reverse in [false, true] {
                let mut states = HashMap::new();
                let mut peers = HashMap::new();
                let mut restored = HashSet::new();
                let mut records = vec![(&older, !newer_supported), (&newer, newer_supported)];
                if reverse {
                    records.reverse();
                }
                for (enr, status) in records {
                    let cached = PersistedPeer {
                        enr: enr.to_base64(),
                        support: Some(PeerRpcSupport {
                            status,
                            ..PeerRpcSupport::default()
                        }),
                        status_successes: enr.seq() as u32,
                        bootstrap_successes: 0,
                        useful_successes: 0,
                        dial_stats: PeerDialAddressStats::default(),
                    };
                    retain_cached_peer_enr(
                        &mut states,
                        &mut peers,
                        &mut restored,
                        ConsensusDialAddressFamilies::IPV4,
                        &digest,
                        enr,
                        &cached,
                    )
                    .unwrap();
                }
                let peer = peer_id_from_enr(&newer).unwrap();
                assert_eq!(peers.contains_key(&peer), newer_supported);
                assert_eq!(states[&peer].status_successes, 2);
                assert_eq!(states[&peer].latest_enr.as_ref().unwrap().enr, newer);
            }
        }
    }

    #[tokio::test]
    async fn peer_freshness_saved_selection_serializes_latest_and_reloads() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let old = freshness_enr(1, 1, Some(9001), false, network.fork_digest);
        let new = freshness_enr(1, 2, Some(9002), false, network.fork_digest);
        network.discv5.add_enr(old.clone()).unwrap();
        network.observe_enr(&new);
        let conflict = freshness_enr(1, 2, Some(9999), false, network.fork_digest);
        assert_eq!(
            network.selected_known_peers(vec![conflict])[0].enr,
            new.to_base64()
        );
        let table_newer = freshness_enr(1, 3, Some(9003), false, network.fork_digest);
        assert_eq!(
            network.selected_known_peers(vec![table_newer.clone()])[0].enr,
            table_newer.to_base64()
        );
        network.persist_known_peers().unwrap();
        let saved = load_known_peers(&network.known_peers_path).unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].enr, new.to_base64());
        let reloaded = ConsensusNetwork::new(
            network.config.clone(),
            network.consensus.clone(),
            network.sync_status.clone(),
        )
        .unwrap();
        let peer = peer_id_from_enr(&new).unwrap();
        assert_eq!(
            reloaded.peer_lifecycle[&peer]
                .latest_enr
                .as_ref()
                .unwrap()
                .enr,
            new
        );
        assert_eq!(
            reloaded.dialable_peers[&peer],
            vec![
                format!("/ip4/127.0.0.1/tcp/9002/p2p/{peer}")
                    .parse::<Multiaddr>()
                    .unwrap()
            ]
        );
    }

    #[tokio::test]
    async fn peer_retention_saved_cache_requires_compatible_rpc_endpoint() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let key = CombinedKey::generate_secp256k1();
        let mut fork_id = [0; 16];
        fork_id[..4].copy_from_slice(&network.fork_digest);
        fork_id[8..].copy_from_slice(&u64::MAX.to_le_bytes());
        // Discovery works over IPv4, while this record offers RPC only over
        // IPv6. A node configured for IPv4 cannot use it for sync requests.
        let enr = Enr::builder()
            .ip4(Ipv4Addr::LOCALHOST)
            .udp4(9000)
            .ip6(Ipv6Addr::LOCALHOST)
            .tcp6(9000)
            .add_value("eth2", &fork_id)
            .build(&key)
            .unwrap();
        network.discv5.add_enr(enr.clone()).unwrap();
        network.persist_known_peers().unwrap();
        assert!(
            !load_known_peers(&network.known_peers_path)
                .unwrap()
                .iter()
                .any(|peer| peer.enr == enr.to_base64())
        );
        network.config.dial_families = ConsensusDialAddressFamilies::IPV6;
        network.persist_known_peers().unwrap();
        assert!(
            load_known_peers(&network.known_peers_path)
                .unwrap()
                .iter()
                .any(|peer| peer.enr == enr.to_base64())
        );
    }

    #[tokio::test]
    async fn peer_retention_saved_selection_preserves_priority_ties_and_limit() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let mut fork_id = [0; 16];
        fork_id[..4].copy_from_slice(&network.fork_digest);
        fork_id[8..].copy_from_slice(&u64::MAX.to_le_bytes());
        let mut enrs = Vec::new();
        let mut ordinary = Vec::new();
        for index in 0..MAX_PERSISTED_KNOWN_PEERS + 3 {
            let key = CombinedKey::generate_secp256k1();
            let enr = Enr::builder()
                .ip4(Ipv4Addr::LOCALHOST)
                .tcp4(9000)
                .add_value("eth2", &fork_id)
                .build(&key)
                .unwrap();
            let peer = peer_id_from_enr(&enr).unwrap();
            match index {
                0 => {
                    network.peer_lifecycle.insert(
                        peer,
                        PeerLifecycleState {
                            status_successes: 3,
                            bootstrap_successes: 2,
                            useful_successes: 5,
                            ..Default::default()
                        },
                    );
                }
                1 => {
                    network.peer_lifecycle.insert(
                        peer,
                        PeerLifecycleState {
                            remembered_support: Some(PeerRpcSupport::default()),
                            ..Default::default()
                        },
                    );
                }
                _ => ordinary.push(enr.to_base64()),
            }
            enrs.push(enr);
        }
        // One useful peer ranks first; a peer known to lack Status is excluded.
        // Every other peer ties, so ENR text alone decides the retained suffix.
        ordinary.sort();
        let expected = std::iter::once(enrs[0].to_base64())
            .chain(ordinary.into_iter().take(MAX_PERSISTED_KNOWN_PEERS - 1))
            .collect::<Vec<_>>();
        let selected = network.selected_known_peers(enrs.clone());
        assert_eq!(
            selected
                .iter()
                .map(|peer| peer.enr.clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(selected[0].status_successes, 3);
        assert_eq!(selected[0].bootstrap_successes, 2);
        assert_eq!(selected[0].useful_successes, 5);
        enrs.reverse();
        assert_eq!(network.selected_known_peers(enrs), selected);
        assert!(network.selected_known_peers(Vec::new()).is_empty());
    }

    fn peer_retention_fixture(temp: &TempDir) -> ConsensusNetwork {
        let (mut network, _) = request_lifecycle_fixture(temp);
        network.dialable_peers.clear();
        network.peer_lifecycle.clear();
        network.bootnode_peers.clear();
        network.observed.clear();
        network
    }

    #[tokio::test]
    async fn peer_retention_counts_mixed_inventory_once_and_cleans_evicted_state() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let peers = (0..=500).map(retention_peer).collect::<Vec<_>>();
        for (index, peer) in peers.iter().copied().enumerate() {
            if index <= 375 {
                network
                    .peer_lifecycle
                    .insert(peer, PeerLifecycleState::default());
            }
            if index <= 250 || index > 375 {
                network
                    .dialable_peers
                    .insert(peer, vec!["/ip4/127.0.0.1/tcp/1".parse().unwrap()]);
                network.observed.insert(peer);
            }
        }
        let evicted = *peers.iter().min().unwrap();
        network
            .peer_support
            .insert(evicted, PeerRpcSupport::default());
        network.metadata_peers.insert(evicted);
        network.inbound_rate_limits.insert(
            (evicted, RpcRequestKind::Status),
            InboundRateLimitBucket {
                window_started_at: Instant::now(),
                used: 1,
            },
        );
        network.prune_inactive_peer_state();
        let retained = network
            .dialable_peers
            .keys()
            .chain(network.peer_lifecycle.keys())
            .copied()
            .collect::<HashSet<_>>();
        assert_eq!(retained.len(), 500);
        assert!(!retained.contains(&evicted));
        assert!(!network.observed.contains(&evicted));
        assert!(!network.peer_support.contains_key(&evicted));
        assert!(!network.metadata_peers.contains(&evicted));
        assert!(
            !network
                .inbound_rate_limits
                .contains_key(&(evicted, RpcRequestKind::Status))
        );
        network.prune_inactive_peer_state();
        assert_eq!(
            network
                .dialable_peers
                .keys()
                .chain(network.peer_lifecycle.keys())
                .copied()
                .collect::<HashSet<_>>(),
            retained
        );
    }

    #[tokio::test]
    async fn peer_retention_preserves_protection_and_existing_priority_order() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        for index in 0..=500 {
            network.peer_lifecycle.insert(
                retention_peer(index),
                PeerLifecycleState {
                    status_successes: 2,
                    ..Default::default()
                },
            );
        }
        network
            .peer_lifecycle
            .insert(retention_peer(0), PeerLifecycleState::default());
        network.peer_lifecycle.insert(
            retention_peer(1),
            PeerLifecycleState {
                bootstrap_successes: 1,
                ..Default::default()
            },
        );
        network.peer_lifecycle.insert(
            retention_peer(2),
            PeerLifecycleState {
                status_successes: 1,
                ..Default::default()
            },
        );
        network.peer_lifecycle.insert(
            retention_peer(3),
            PeerLifecycleState {
                useful_successes: 1,
                ..Default::default()
            },
        );
        let protected = (600..605).map(retention_peer).collect::<Vec<_>>();
        for peer in &protected {
            network
                .peer_lifecycle
                .insert(*peer, PeerLifecycleState::default());
        }
        network.connected_peers.insert(protected[0]);
        network.dialing_peers.insert(protected[1]);
        network.closing_peers.insert(protected[2]);
        network.bootnode_peers.insert(protected[3]);
        network.ensure_request(protected[4], RpcRequestKind::Status);
        network.prune_inactive_peer_state();
        // Active peers are outside the inactive count; bootnodes and pending
        // requests remain protected within it, as in the previous policy.
        assert_eq!(network.peer_lifecycle.len(), 503);
        for index in 0..3 {
            assert!(!network.peer_lifecycle.contains_key(&retention_peer(index)));
        }
        assert!(network.peer_lifecycle.contains_key(&retention_peer(3)));
        for peer in &protected {
            assert!(network.peer_lifecycle.contains_key(peer));
        }
        network.prune_inactive_peer_state();
        assert_eq!(network.peer_lifecycle.len(), 503);
        network.connected_peers.clear();
        network.dialing_peers.clear();
        network.closing_peers.clear();
        network.bootnode_peers.clear();
        network.clear_pending_requests_for_peer(protected[4]);
        network.prune_inactive_peer_state();
        assert_eq!(network.peer_lifecycle.len(), 500);
        // The cap does not override bootnode protection even if the protected
        // set alone exceeds it; this remains an explicit policy limit.
        network
            .peer_lifecycle
            .insert(retention_peer(999), PeerLifecycleState::default());
        network
            .bootnode_peers
            .extend(network.peer_lifecycle.keys().copied());
        network.prune_inactive_peer_state();
        assert_eq!(network.peer_lifecycle.len(), 501);
    }

    #[tokio::test]
    async fn peer_retention_live_enr_inventory_respects_configured_family() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        let key = CombinedKey::generate_secp256k1();
        let mut fork_id = [0; 16];
        fork_id[..4].copy_from_slice(&network.fork_digest);
        fork_id[8..].copy_from_slice(&u64::MAX.to_le_bytes());
        let enr = Enr::builder()
            .ip6(Ipv6Addr::LOCALHOST)
            .udp6(9000)
            .tcp6(9000)
            .add_value("eth2", &fork_id)
            .build(&key)
            .unwrap();
        assert!(enr_is_relevant_consensus_peer(&enr, &network.fork_digest));
        network.observe_enr(&enr);
        assert!(network.dialable_peers.is_empty());
        assert!(network.observed.is_empty());
        assert!(network.discovery_query_needed());
        network.config.dial_families = ConsensusDialAddressFamilies::IPV6;
        network.observe_enr(&enr);
        let peer = peer_id_from_enr(&enr).unwrap();
        assert!(network.dialable_peers.contains_key(&peer));
        assert!(network.observed.contains(&peer));
    }

    #[tokio::test]
    async fn peer_retention_prunes_inbound_only_lifecycle_records() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        for index in 0..=500 {
            network.record_peer_disconnect(retention_peer(index), "offline fixture".to_owned());
        }
        assert!(network.dialable_peers.is_empty());
        assert_eq!(network.peer_lifecycle.len(), 501);
        network.prune_inactive_peer_state();
        assert_eq!(
            network.peer_lifecycle.len(),
            MAX_RETAINED_DISCONNECTED_PEERS
        );
        let retained = network
            .peer_lifecycle
            .keys()
            .copied()
            .collect::<HashSet<_>>();
        network.prune_inactive_peer_state();
        assert_eq!(
            network
                .peer_lifecycle
                .keys()
                .copied()
                .collect::<HashSet<_>>(),
            retained
        );
    }

    fn request_lifecycle_fixture(temp: &TempDir) -> (ConsensusNetwork, RawRpcResponse) {
        request_lifecycle_fixture_at_slot(temp, 419_072 * 32 + 16)
    }

    fn request_lifecycle_fixture_at_slot(
        temp: &TempDir,
        slot: u64,
    ) -> (ConsensusNetwork, RawRpcResponse) {
        let fixture = crate::light_client::test_cached_light_client_fixture(slot);
        let consensus = Arc::new(
            ConsensusStore::open(
                temp.path(),
                Some(&format!("{:#x}", fixture.checkpoint.beacon_root)),
            )
            .unwrap(),
        );
        let bootstrap = fixture.payloads.bootstrap.unwrap();
        let (status, store) =
            verify_bootstrap_payload(&bootstrap.bytes, fixture.checkpoint).unwrap();
        consensus
            .record_verified_bootstrap(status, bootstrap, store)
            .unwrap();
        let network = ConsensusNetwork::new(
            ConsensusNetworkConfig {
                data_dir: temp.path().to_owned(),
                checkpoint: fixture.checkpoint,
                bind_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
                dial_families: ConsensusDialAddressFamilies::IPV4,
                external_ip: None,
                discovery_port: 0,
                p2p_port: 0,
                max_peers: 1,
            },
            consensus,
            Arc::new(Mutex::new(SyncStatus::default())),
        )
        .unwrap();
        // Construction and send_request only enqueue work. Never start discovery
        // or poll the swarm: these tests open no listener and contact no peer.
        (
            network,
            fixture
                .payloads
                .updates_by_period
                .into_values()
                .next()
                .unwrap(),
        )
    }

    fn rpc_participation_slot() -> u64 {
        (current_wall_clock_slot().saturating_sub(2 * 8192) / 8192) * 8192 + 16
    }

    fn deliver_range_response(network: &mut ConsensusNetwork, payload: RawRpcResponse) {
        let peer = PeerId::random();
        let kind = RpcRequestKind::LightClientUpdatesByRange;
        network.ensure_request(peer, kind);
        let key = *network
            .pending_requests
            .iter()
            .find(|(key, pending_peer)| key.kind == kind && **pending_peer == peer)
            .unwrap()
            .0;
        network.handle_rpc_response(
            kind,
            peer,
            key.request_id,
            Eth2RpcResponse::LightClientUpdatesByRange(vec![payload]),
        );
        assert!(!network.pending_requests.contains_key(&key));
        assert_eq!(
            network.peer_lifecycle.get(&peer).unwrap().useful_successes,
            1
        );
    }

    #[tokio::test]
    async fn rpc_participation_range_summary_does_not_hide_older_singleton_cache() {
        let temp = TempDir::new().unwrap();
        let slot = rpc_participation_slot();
        let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
        deliver_singleton_response(&mut network, false, singleton_test_payload(slot, 1, false));
        let range = crate::light_client::test_cached_light_client_fixture(slot + 4)
            .payloads
            .updates_by_period
            .into_values()
            .next()
            .unwrap();
        deliver_range_response(&mut network, range);
        let summary = network
            .consensus
            .light_client_status()
            .optimistic_update
            .unwrap();
        assert_eq!(summary.attested_header.beacon_slot, slot + 5);
        let better_cache = singleton_test_payload(slot + 2, 1, false);
        deliver_singleton_response(&mut network, false, better_cache.clone());
        assert_eq!(
            network.consensus.light_client_optimistic_update_payload(),
            Some(better_cache)
        );
        assert_eq!(
            network.consensus.light_client_status().optimistic_update,
            Some(summary)
        );
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .optimistic_header
                .beacon
                .slot,
            slot + 5
        );
    }

    fn deliver_singleton_response(
        network: &mut ConsensusNetwork,
        finality: bool,
        payload: RawRpcResponse,
    ) -> PeerId {
        let peer = PeerId::random();
        let kind = if finality {
            RpcRequestKind::LightClientFinalityUpdate
        } else {
            RpcRequestKind::LightClientOptimisticUpdate
        };
        network.ensure_request(peer, kind);
        let key = *network
            .pending_requests
            .iter()
            .find(|(key, pending_peer)| key.kind == kind && **pending_peer == peer)
            .unwrap()
            .0;
        let response = if finality {
            Eth2RpcResponse::LightClientFinalityUpdate(payload)
        } else {
            Eth2RpcResponse::LightClientOptimisticUpdate(payload)
        };
        network.handle_rpc_response(kind, peer, key.request_id, response);
        assert!(!network.pending_requests.contains_key(&key));
        peer
    }

    fn singleton_test_payload(slot: u64, participants: usize, finality: bool) -> RawRpcResponse {
        let (finality_bytes, optimistic_bytes) =
            crate::light_client::test_gossip_payloads(slot, participants);
        // Mainnet BPO2 digest, independently fixed in the fork conformance fixtures.
        RawRpcResponse {
            context_bytes: Some([0x8c, 0x9f, 0x62, 0xfe]),
            bytes: if finality {
                finality_bytes
            } else {
                optimistic_bytes
            },
        }
    }

    fn assert_rpc_equal_slot_participation(finality: bool, low: usize, high: usize) {
        let temp = TempDir::new().unwrap();
        let slot = rpc_participation_slot();
        let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
        deliver_singleton_response(
            &mut network,
            finality,
            singleton_test_payload(slot, low, finality),
        );
        let before = network.consensus.light_client_store().unwrap();
        assert_eq!(before.current_max_active_participants, low);
        let stronger = singleton_test_payload(slot, high, finality);
        let expected = if finality {
            apply_finality_update_payload(&stronger.bytes, &before)
                .unwrap()
                .1
        } else {
            apply_optimistic_update_payload(&stronger.bytes, &before)
                .unwrap()
                .1
        };
        assert_eq!(expected.finalized_header, before.finalized_header);
        assert_eq!(expected.optimistic_header, before.optimistic_header);
        assert_eq!(expected.current_max_active_participants, high);
        assert_ne!(expected, before);
        let peer = deliver_singleton_response(&mut network, finality, stronger.clone());
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .current_max_active_participants,
            high
        );
        assert_eq!(network.consensus.light_client_store().unwrap(), expected);
        assert_eq!(
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .light_client_store()
                .unwrap(),
            expected
        );
        assert_eq!(
            network.peer_lifecycle.get(&peer).unwrap().useful_successes,
            1
        );
        let cached = if finality {
            network.consensus.light_client_finality_update_payload()
        } else {
            network.consensus.light_client_optimistic_update_payload()
        };
        assert_eq!(cached, Some(stronger));
        // Exactly half the improved maximum must not advance optimism. Without
        // the equal-slot improvement, this signed later update would advance it.
        let threshold = singleton_test_payload(slot + 1, high / 2, false);
        let without_improvement = apply_optimistic_update_payload(&threshold.bytes, &before)
            .unwrap()
            .1;
        assert!(
            without_improvement.optimistic_header.beacon.slot
                > before.optimistic_header.beacon.slot
        );
        deliver_singleton_response(&mut network, false, threshold);
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .optimistic_header,
            expected.optimistic_header
        );
    }

    #[tokio::test]
    async fn rpc_participation_matching_range_summary_still_refreshes_singleton_cache() {
        for finality in [false, true] {
            let temp = TempDir::new().unwrap();
            let slot = rpc_participation_slot();
            let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
            let (low, high) = if finality { (342, 344) } else { (1, 4) };
            deliver_singleton_response(
                &mut network,
                finality,
                singleton_test_payload(slot, low, finality),
            );
            let (range, finality_bytes, optimistic_bytes) =
                crate::light_client::test_rpc_update_payloads(slot, high);
            deliver_range_response(
                &mut network,
                RawRpcResponse {
                    context_bytes: Some([0x8c, 0x9f, 0x62, 0xfe]),
                    bytes: range,
                },
            );
            let range_summary = network.consensus.light_client_status();
            let payload = RawRpcResponse {
                context_bytes: Some([0x8c, 0x9f, 0x62, 0xfe]),
                bytes: if finality {
                    finality_bytes
                } else {
                    optimistic_bytes
                },
            };
            deliver_singleton_response(&mut network, finality, payload.clone());
            let cached = if finality {
                network.consensus.light_client_finality_update_payload()
            } else {
                network.consensus.light_client_optimistic_update_payload()
            };
            assert_eq!(cached, Some(payload));
            assert_eq!(network.consensus.light_client_status(), range_summary);
            let weaker_range = crate::light_client::test_rpc_update_payloads(slot, low).0;
            deliver_range_response(
                &mut network,
                RawRpcResponse {
                    context_bytes: Some([0x8c, 0x9f, 0x62, 0xfe]),
                    bytes: weaker_range,
                },
            );
            assert_eq!(network.consensus.light_client_status(), range_summary);
            let reopened = ConsensusStore::open(temp.path(), None).unwrap();
            assert_eq!(reopened.light_client_status(), range_summary);
            assert_eq!(
                reopened.light_client_finality_update_payload(),
                network.consensus.light_client_finality_update_payload()
            );
            assert_eq!(
                reopened.light_client_optimistic_update_payload(),
                network.consensus.light_client_optimistic_update_payload()
            );
        }
    }

    #[tokio::test]
    async fn rpc_participation_stable_duplicates_and_weaker_responses_do_not_write() {
        for finality in [false, true] {
            let temp = TempDir::new().unwrap();
            let slot = rpc_participation_slot();
            let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
            let (low, high) = if finality { (342, 344) } else { (1, 4) };
            deliver_singleton_response(
                &mut network,
                finality,
                singleton_test_payload(slot, low, finality),
            );
            deliver_singleton_response(
                &mut network,
                finality,
                singleton_test_payload(slot, high, finality),
            );
            let before = network.consensus.light_client_store();
            let summary = network.consensus.light_client_status();
            let saved = temp.path().join("saved-snapshot.json");
            fs::rename(network.consensus.state_path(), &saved).unwrap();
            for participants in [high, low] {
                let peer = deliver_singleton_response(
                    &mut network,
                    finality,
                    singleton_test_payload(slot, participants, finality),
                );
                assert_eq!(
                    network.peer_lifecycle.get(&peer).unwrap().useful_successes,
                    1
                );
                assert_eq!(network.consensus.light_client_store(), before);
                assert_eq!(network.consensus.light_client_status(), summary);
                assert!(!network.consensus.state_path().exists());
            }
            fs::rename(saved, network.consensus.state_path()).unwrap();
            assert_eq!(
                ConsensusStore::open(temp.path(), None)
                    .unwrap()
                    .light_client_store(),
                before
            );
        }
    }

    #[tokio::test]
    async fn rpc_participation_equal_slot_invalid_responses_receive_no_credit() {
        for finality in [false, true] {
            for invalid_context in [false, true] {
                let temp = TempDir::new().unwrap();
                let slot = rpc_participation_slot();
                let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
                deliver_singleton_response(
                    &mut network,
                    finality,
                    singleton_test_payload(slot, 342, finality),
                );
                let before = network.consensus.light_client_store();
                let mut payload = singleton_test_payload(slot, 344, finality);
                if invalid_context {
                    payload.context_bytes = Some([0; 4]);
                } else {
                    // Finality fixed section has two offsets and seven proof nodes;
                    // optimistic has one offset. Both place signature after 64 committee-bitfield bytes.
                    let signature_start = if finality { 8 + 7 * 32 + 64 } else { 4 + 64 };
                    payload.bytes[signature_start..signature_start + 96].fill(0);
                }
                let peer = deliver_singleton_response(&mut network, finality, payload);
                assert_eq!(network.consensus.light_client_store(), before);
                assert_eq!(
                    network
                        .peer_lifecycle
                        .get(&peer)
                        .map_or(0, |state| state.useful_successes),
                    0
                );
                assert!(network.peer_failures.contains_key(&peer));
            }
        }
    }

    #[tokio::test]
    async fn rpc_participation_local_save_failure_does_not_publish_or_blame_peer() {
        for finality in [false, true] {
            let temp = TempDir::new().unwrap();
            let slot = rpc_participation_slot();
            let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
            let before = network.consensus.light_client_store();
            let summary = network.consensus.light_client_status();
            let parent = network.consensus.state_path().parent().unwrap().to_owned();
            let moved = temp.path().join("saved-cl-directory");
            fs::rename(&parent, &moved).unwrap();
            let peer = deliver_singleton_response(
                &mut network,
                finality,
                singleton_test_payload(slot, 342, finality),
            );
            assert_eq!(network.consensus.light_client_store(), before);
            assert_eq!(network.consensus.light_client_status(), summary);
            assert!(
                network
                    .consensus
                    .light_client_finality_update_payload()
                    .is_none()
            );
            assert!(
                network
                    .consensus
                    .light_client_optimistic_update_payload()
                    .is_none()
            );
            assert!(
                network
                    .consensus
                    .subscribe_storage_failure()
                    .borrow()
                    .is_some()
            );
            assert_eq!(
                network.peer_lifecycle.get(&peer).unwrap().useful_successes,
                1
            );
            assert!(!network.peer_failures.contains_key(&peer));
            fs::rename(moved, parent).unwrap();
            assert_eq!(
                ConsensusStore::open(temp.path(), None)
                    .unwrap()
                    .light_client_store(),
                before
            );
        }
    }

    #[tokio::test]
    async fn rpc_participation_missing_committee_remains_local_and_new_finality_advances() {
        for finality in [false, true] {
            let temp = TempDir::new().unwrap();
            let slot = rpc_participation_slot();
            let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
            let before = network.consensus.light_client_store();
            let peer = deliver_singleton_response(
                &mut network,
                finality,
                singleton_test_payload(slot + 8192, 342, finality),
            );
            assert_eq!(network.consensus.light_client_store(), before);
            assert!(!network.peer_failures.contains_key(&peer));
            assert_eq!(
                network
                    .peer_lifecycle
                    .get(&peer)
                    .map_or(0, |state| state.useful_successes),
                0
            );
        }
        let temp = TempDir::new().unwrap();
        let slot = rpc_participation_slot();
        let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
        deliver_singleton_response(&mut network, true, singleton_test_payload(slot, 1, true));
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .finalized_header
                .beacon
                .slot,
            slot
        );
        deliver_singleton_response(&mut network, true, singleton_test_payload(slot, 342, true));
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .finalized_header
                .beacon
                .slot,
            slot + 1
        );
    }

    #[tokio::test]
    async fn rpc_participation_equal_slot_optimistic_updates_safety_threshold() {
        assert_rpc_equal_slot_participation(false, 1, 4);
    }

    #[tokio::test]
    async fn rpc_participation_equal_slot_finality_updates_safety_threshold() {
        assert_rpc_equal_slot_participation(true, 342, 344);
    }

    #[tokio::test]
    async fn request_lifecycle_stale_signed_response_is_dropped_without_peer_blame() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let peer = PeerId::random();
        let original = network.consensus.light_client_store().unwrap();
        let historical =
            crate::light_client::test_cached_light_client_fixture(419_072 * 32 - 8192 + 8);
        let update = historical
            .payloads
            .updates_by_period
            .into_values()
            .next()
            .unwrap();
        assert!(matches!(
            apply_light_client_update_payload(&update.bytes, &original),
            Err(LightClientVerificationError::UnknownSyncCommitteePeriod { .. })
        ));
        let kind = RpcRequestKind::LightClientUpdatesByRange;
        network.ensure_request(peer, kind);
        let key = *network.pending_requests.keys().next().unwrap();
        network.handle_rpc_response(
            kind,
            peer,
            key.request_id,
            Eth2RpcResponse::LightClientUpdatesByRange(vec![update]),
        );
        assert_eq!(network.consensus.light_client_store().unwrap(), original);
        assert!(!network.peer_failures.contains_key(&peer));
        assert!(network.last_rpc_failure.is_none());
        assert!(network.pending_light_client_range_requests.is_empty());
    }

    #[tokio::test]
    async fn request_lifecycle_invalid_chunk_after_valid_update_publishes_nothing() {
        for invalid_kind in [0, 1, 2] {
            let temp = TempDir::new().unwrap();
            let (mut network, update) = request_lifecycle_fixture(&temp);
            let peer = PeerId::random();
            let original = network.consensus.light_client_store().unwrap();
            let request = LightClientUpdatesByRangeRequest {
                start_period: sync_committee_period_for_slot(original.bootstrap_slot()),
                count: 2,
            };
            let kind = RpcRequestKind::LightClientUpdatesByRange;
            let request_id = network
                .swarm
                .behaviour_mut()
                .light_client_updates_by_range_rpc
                .inner
                .send_request(&peer, Eth2RpcRequest::LightClientUpdatesByRange(request));
            let key = PendingRequestKey { kind, request_id };
            network.pending_requests.insert(key, peer);
            network.pending_peer_kinds.insert((peer, kind));
            network
                .pending_light_client_range_requests
                .insert(key, request);
            let mut invalid = update.clone();
            match invalid_kind {
                0 => invalid.bytes = vec![0],
                1 => invalid.context_bytes = Some([0xff; 4]),
                _ => {} // A second valid update in the same period is not consecutive.
            }
            network.handle_rpc_response(
                kind,
                peer,
                request_id,
                Eth2RpcResponse::LightClientUpdatesByRange(vec![update, invalid]),
            );
            assert_eq!(network.consensus.light_client_store().unwrap(), original);
            assert_eq!(network.peer_lifecycle[&peer].rpc_failures, 1);
        }
    }

    #[tokio::test]
    async fn request_lifecycle_rejects_signed_update_outside_requested_period() {
        let temp = TempDir::new().unwrap();
        let (mut network, update) = request_lifecycle_fixture(&temp);
        let peer = PeerId::random();
        let original = network.consensus.light_client_store().unwrap();
        // The signed update is valid for the store; only request correlation
        // disqualifies it from this otherwise ordinary count-one response.
        assert!(apply_light_client_update_payload(&update.bytes, &original).is_ok());
        let request = LightClientUpdatesByRangeRequest {
            start_period: sync_committee_period_for_slot(original.bootstrap_slot()) + 1,
            count: 1,
        };
        let kind = RpcRequestKind::LightClientUpdatesByRange;
        let request_id = network
            .swarm
            .behaviour_mut()
            .light_client_updates_by_range_rpc
            .inner
            .send_request(&peer, Eth2RpcRequest::LightClientUpdatesByRange(request));
        let key = PendingRequestKey { kind, request_id };
        network.pending_requests.insert(key, peer);
        network.pending_peer_kinds.insert((peer, kind));
        network
            .pending_light_client_range_requests
            .insert(key, request);
        network.handle_rpc_response(
            kind,
            peer,
            request_id,
            Eth2RpcResponse::LightClientUpdatesByRange(vec![update]),
        );
        assert_eq!(network.consensus.light_client_store().unwrap(), original);
        assert!(network.pending_light_client_range_requests.is_empty());
    }

    #[test]
    fn request_lifecycle_range_shapes_allow_partial_results_and_skipped_slots() {
        let request = LightClientUpdatesByRangeRequest {
            start_period: 10,
            count: 4,
        };
        assert!(light_client_range_contains_next_period(request, None, 11));
        assert!(light_client_range_contains_next_period(
            request,
            Some(11),
            12
        ));
        for (previous, period) in [
            (None, 9),
            (None, 14),
            (Some(11), 11),
            (Some(11), 13),
            (Some(12), 11),
        ] {
            assert!(!light_client_range_contains_next_period(
                request, previous, period
            ));
        }
        assert!(light_client_range_contains_next_period(
            LightClientUpdatesByRangeRequest {
                start_period: u64::MAX,
                count: 1
            },
            None,
            u64::MAX
        ));
        assert!(!light_client_range_contains_next_period(
            LightClientUpdatesByRangeRequest {
                start_period: u64::MAX,
                count: 1
            },
            Some(u64::MAX),
            0
        ));

        let request = BeaconBlocksByRangeRequest {
            start_slot: 10,
            count: 8,
            step: 1,
        };
        let first = ancestry_test_block(10, 1, 0);
        let second = ancestry_test_block(13, 2, 1);
        let fork = ancestry_test_block(14, 3, 0);
        assert!(valid_history_range_sequence(request, [].iter()));
        assert!(valid_history_range_sequence(request, [first].iter()));
        assert!(valid_history_range_sequence(
            request,
            [first, second].iter()
        ));
        for blocks in [[second, first], [first, first], [first, fork]] {
            assert!(!valid_history_range_sequence(request, blocks.iter()));
        }
        // For step > 1, omitted intervening blocks prevent direct parent checks.
        assert!(valid_history_range_sequence(
            BeaconBlocksByRangeRequest { step: 2, ..request },
            [first, fork].iter()
        ));
    }

    #[tokio::test]
    async fn request_lifecycle_timeout_releases_metadata_and_late_response_is_ignored() {
        let temp = TempDir::new().unwrap();
        let (mut network, update) = request_lifecycle_fixture(&temp);
        let peer = PeerId::random();
        let original = network.consensus.light_client_store().unwrap();
        let kind = RpcRequestKind::LightClientUpdatesByRange;
        network.ensure_request(peer, kind);
        let key = *network.pending_requests.keys().next().unwrap();
        network.handle_rpc_event(
            kind,
            request_response::Event::OutboundFailure {
                peer,
                connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                request_id: key.request_id,
                error: request_response::OutboundFailure::Timeout,
            },
        );
        assert!(network.pending_requests.is_empty());
        assert!(network.pending_light_client_range_requests.is_empty());
        assert!(network.pending_peer_kinds.is_empty());
        assert_eq!(network.peer_lifecycle[&peer].rpc_failures, 1);
        network.handle_rpc_response(
            kind,
            peer,
            key.request_id,
            Eth2RpcResponse::LightClientUpdatesByRange(vec![update]),
        );
        assert_eq!(network.consensus.light_client_store().unwrap(), original);
        assert_eq!(network.peer_lifecycle[&peer].rpc_failures, 1);
    }

    fn memory_failure_request(
        network: &mut ConsensusNetwork,
        peer: PeerId,
        range: bool,
        count: usize,
    ) -> PendingRequestKey {
        let (kind, request_id) = if range {
            let request = BeaconBlocksByRangeRequest {
                start_slot: 100,
                count: count as u64,
                step: 1,
            };
            let id = network
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_range_rpc
                .inner
                .send_request(&peer, Eth2RpcRequest::BeaconBlocksByRange(request));
            let key = PendingRequestKey {
                kind: RpcRequestKind::BeaconBlocksByRange,
                request_id: id,
            };
            network.pending_history_range_requests.insert(key, request);
            (key.kind, id)
        } else {
            let roots = (0..count)
                .map(|value| B256::repeat_byte(value as u8))
                .collect::<Vec<_>>();
            let id = network
                .swarm
                .behaviour_mut()
                .beacon_blocks_by_root_rpc
                .inner
                .send_request(&peer, Eth2RpcRequest::BeaconBlocksByRoot(roots.clone()));
            let key = PendingRequestKey {
                kind: RpcRequestKind::BeaconBlocksByRoot,
                request_id: id,
            };
            network.pending_history_root_requests.insert(key, roots);
            (key.kind, id)
        };
        let key = PendingRequestKey { kind, request_id };
        network.pending_requests.insert(key, peer);
        network.pending_peer_kinds.insert((peer, kind));
        key
    }

    fn memory_failure_event(
        peer: PeerId,
        key: PendingRequestKey,
        error: RpcMemoryError,
    ) -> Eth2RpcEvent {
        request_response::Event::OutboundFailure {
            peer,
            connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
            request_id: key.request_id,
            error: request_response::OutboundFailure::Io(io::Error::other(error)),
        }
    }

    fn memory_root_request(
        network: &mut ConsensusNetwork,
        peer: PeerId,
        roots: Vec<B256>,
    ) -> PendingRequestKey {
        let id = network
            .swarm
            .behaviour_mut()
            .beacon_blocks_by_root_rpc
            .inner
            .send_request(&peer, Eth2RpcRequest::BeaconBlocksByRoot(roots.clone()));
        let key = PendingRequestKey {
            kind: RpcRequestKind::BeaconBlocksByRoot,
            request_id: id,
        };
        network.pending_history_root_requests.insert(key, roots);
        network.pending_requests.insert(key, peer);
        network.pending_peer_kinds.insert((peer, key.kind));
        key
    }

    #[tokio::test]
    async fn incoming_memory_actual_codec_limit_retries_valid_blocks_and_releases_queue_charge() {
        let first = RawRpcResponse {
            bytes: include_bytes!("../tests/fixtures/beacon_block_14132042.ssz").to_vec(),
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(14132042 / 32)),
        };
        let mut second = first.clone();
        // Synthetic root-membership fixture, not a canonical-chain/proposer-signature claim.
        // SignedBeaconBlock fixed section = message offset (4) + signature (96).
        // BeaconBlock starts with slot (8), then proposer_index (8).
        let message_offset = u32::from_le_bytes(second.bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(message_offset, 100);
        let proposer = message_offset + 8;
        second.bytes[proposer] ^= 1;
        let bodies = vec![first, second];
        let blocks = bodies
            .iter()
            .map(|raw| decode_verified_beacon_block(raw).unwrap())
            .collect::<Vec<_>>();
        assert_ne!(blocks[0].beacon_root, blocks[1].beacon_root);
        let roots = blocks
            .iter()
            .map(|block| block.beacon_root)
            .collect::<Vec<_>>();
        for limited in [false, true] {
            let temp = TempDir::new().unwrap();
            let (mut network, _) = request_lifecycle_fixture(&temp);
            let peer = PeerId::random();
            let max_decoded = if limited {
                bodies[0].bytes.len() + 1
            } else {
                bodies.iter().map(|raw| raw.bytes.len()).sum()
            };
            let budgets = RpcResponseBudgets::with_limits(1024 * 1024, 1024, max_decoded);
            let pool = budgets.pool(true);
            let mut codec = crate::rpc::Eth2RpcCodec::new(budgets);
            let protocol = crate::rpc::Eth2RpcProtocol::BeaconBlocksByRootV2;
            let key = memory_root_request(&mut network, peer, roots.clone());
            let mut wire = futures::io::Cursor::new(Vec::new());
            request_response::Codec::write_response(
                &mut codec,
                &protocol,
                &mut wire,
                Eth2RpcResponse::BeaconBlocksByRoot(bodies.clone()).into(),
            )
            .await
            .unwrap();
            wire.set_position(0);
            let result =
                request_response::Codec::read_response(&mut codec, &protocol, &mut wire).await;
            if limited {
                let error = result.unwrap_err();
                assert!(matches!(
                    from_io(&error),
                    Some(RpcMemoryError::ResponseLimit { .. })
                ));
                assert_eq!(pool.used(), 0);
                network.handle_rpc_event(
                    key.kind,
                    request_response::Event::OutboundFailure {
                        peer,
                        connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                        request_id: key.request_id,
                        error: request_response::OutboundFailure::Io(error),
                    },
                );
                assert_eq!(network.history_root_batch_limit, 1);
                assert!(network.peer_failures.is_empty());
                assert!(
                    roots
                        .iter()
                        .all(|root| !network.verified_beacon_blocks.contains_key(root))
                );
                for (raw, root) in bodies.iter().zip(&roots) {
                    network
                        .local_rpc_retry_after
                        .insert(key.kind, Instant::now());
                    let retry = memory_root_request(&mut network, peer, vec![*root]);
                    assert!(
                        network.pending_history_root_requests[&retry].len()
                            <= network.history_root_batch_limit
                    );
                    let mut wire = futures::io::Cursor::new(Vec::new());
                    request_response::Codec::write_response(
                        &mut codec,
                        &protocol,
                        &mut wire,
                        Eth2RpcResponse::BeaconBlocksByRoot(vec![raw.clone()]).into(),
                    )
                    .await
                    .unwrap();
                    wire.set_position(0);
                    let response =
                        request_response::Codec::read_response(&mut codec, &protocol, &mut wire)
                            .await
                            .unwrap();
                    assert!(pool.used() > 0);
                    let event = request_response::Event::Message {
                        peer,
                        connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                        message: request_response::Message::Response {
                            request_id: retry.request_id,
                            response,
                        },
                    };
                    assert!(pool.used() > 0);
                    network.handle_rpc_event(retry.kind, event);
                    assert_eq!(pool.used(), 0);
                }
            } else {
                let response = result.unwrap();
                assert!(pool.used() > 0);
                network.handle_rpc_event(
                    key.kind,
                    request_response::Event::Message {
                        peer,
                        connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                        message: request_response::Message::Response {
                            request_id: key.request_id,
                            response,
                        },
                    },
                );
                assert_eq!(pool.used(), 0);
            }
            for block in &blocks {
                assert_eq!(
                    network.verified_beacon_blocks.get(&block.beacon_root),
                    Some(block)
                );
            }
            assert_eq!(
                network.history_root_batch_limit,
                if limited { 4 } else { 128 }
            );
            assert_eq!(network.history_range_batch_limit, 128);

            assert!(
                network
                    .peer_failures
                    .get(&peer)
                    .is_none_or(|failures| failures.beacon_blocks_by_root == 0)
            );
            assert_eq!(network.request_failures.beacon_blocks_by_root, 0);
        }
    }

    #[tokio::test]
    async fn incoming_memory_response_limit_halves_actual_batch_without_peer_fault() {
        for range in [false, true] {
            let temp = TempDir::new().unwrap();
            let (mut network, _) = request_lifecycle_fixture(&temp);
            let peer = PeerId::random();
            for (count, expected) in [(16, 8), (8, 4), (1, 1)] {
                let key = memory_failure_request(&mut network, peer, range, count);
                network.handle_rpc_event(
                    key.kind,
                    memory_failure_event(peer, key, RpcMemoryError::ResponseLimit { limit: 64 }),
                );
                assert!(network.pending_requests.is_empty());
                assert!(network.pending_peer_kinds.is_empty());
                assert!(network.pending_history_range_requests.is_empty());
                assert!(network.pending_history_root_requests.is_empty());
                assert!(network.peer_failures.is_empty());
                assert!(!network.peer_lifecycle.contains_key(&peer));
                assert_eq!(network.request_failures.beacon_blocks_by_range, 0);
                assert_eq!(network.request_failures.beacon_blocks_by_root, 0);
                assert_eq!(
                    if range {
                        network.history_range_batch_limit as usize
                    } else {
                        network.history_root_batch_limit
                    },
                    expected
                );
            }
            let deadline = network.local_rpc_retry_after.clone();
            let key = memory_failure_request(&mut network, peer, range, 1);
            network.clear_pending_requests_for_peer(peer);
            network.handle_rpc_event(
                key.kind,
                memory_failure_event(
                    peer,
                    key,
                    RpcMemoryError::Capacity {
                        requested: 2,
                        used: 1,
                        limit: 1,
                    },
                ),
            );
            assert_eq!(network.local_rpc_retry_after, deadline);
        }
    }

    #[test]
    fn incoming_memory_batch_recovery_is_bounded_and_requires_complete_useful_batches() {
        let large = 10 * 1024 * 1024;
        let mut limit = 1;
        for expected in [2, 4, 6, 6] {
            limit = recovered_history_batch_limit(limit, limit, limit as usize, large, true);
            assert_eq!(limit, expected);
        }
        // Smaller later bodies recover from a previous outlier without removing the ceiling.
        for expected in [12, 24, 48, 96, 128, 128] {
            limit = recovered_history_batch_limit(limit, limit, limit as usize, 1024, true);
            assert_eq!(limit, expected);
        }
        for (requested, received, largest, useful) in [
            (8, 4, 1, true),
            (0, 0, 0, true),
            (8, 8, 1, false),
            (8, 8, 0, true),
        ] {
            assert_eq!(
                recovered_history_batch_limit(8, requested, received, largest, useful),
                8
            );
        }
        // Success never lowers an already larger cap.
        assert_eq!(recovered_history_batch_limit(8, 1, 1, large, true), 8);
    }

    #[tokio::test]
    async fn incoming_memory_capacity_backoff_is_local_bounded_and_family_scoped() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let now = Instant::now();
        let error =
            request_response::OutboundFailure::Io(io::Error::other(RpcMemoryError::Capacity {
                requested: 2,
                used: 1,
                limit: 1,
            }));
        assert!(network.handle_local_rpc_failure(
            RpcRequestKind::BeaconBlocksByRange,
            &error,
            Some(16),
            now
        ));
        for kind in [
            RpcRequestKind::BeaconBlocksByRange,
            RpcRequestKind::BeaconBlocksByRoot,
        ] {
            assert!(!network.local_rpc_retry_ready(kind, now));
            assert!(network.local_rpc_retry_ready(kind, now + Duration::from_secs(1)));
            assert!(!network.can_issue_request(kind));
            network.ensure_request(PeerId::random(), kind);
        }
        assert!(network.pending_requests.is_empty());
        assert!(network.local_rpc_retry_ready(RpcRequestKind::LightClientOptimisticUpdate, now));
        assert_eq!(network.history_range_batch_limit, 128);
        let allocation =
            request_response::OutboundFailure::Io(io::Error::other(RpcMemoryError::Allocation {
                message: "fixture".to_owned(),
            }));
        assert!(network.handle_local_rpc_failure(RpcRequestKind::MetaData, &allocation, None, now));
        assert!(!network.local_rpc_retry_ready(RpcRequestKind::MetaData, now));
        assert!(network.local_rpc_retry_ready(RpcRequestKind::Status, now));
        assert!(!network.handle_local_rpc_failure(
            RpcRequestKind::Status,
            &request_response::OutboundFailure::Timeout,
            None,
            now
        ));
    }

    #[tokio::test]
    async fn incoming_memory_builders_use_capped_actual_pending_windows() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let store = network.consensus.light_client_store().unwrap();
        let target = HistorySyncTarget {
            checkpoint_root: store.checkpoint_root,
            checkpoint_slot: store.bootstrap_slot(),
            finalized_root: B256::repeat_byte(0xaa),
            optimistic_root: B256::repeat_byte(0xbb),
            optimistic_slot: store.bootstrap_slot() + 64,
        };
        network.active_history_target = Some(target);
        network.history_range_batch_limit = 8;
        let first = network
            .next_forward_history_range_request_with_pending(target, &[])
            .unwrap();
        assert_eq!(first.count, 8);
        let second = network
            .next_forward_history_range_request_with_pending(target, &[first])
            .unwrap();
        assert_eq!(second.start_slot, first.start_slot + 8);
        assert_eq!(second.count, 8);
        assert!(network.next_history_root_request().unwrap().len() >= 2);
        network.history_root_batch_limit = 1;
        assert_eq!(network.next_history_root_request().unwrap().len(), 1);
        assert_eq!(
            network.next_priority_history_root_request().unwrap().len(),
            1
        );
        let peer = PeerId::random();
        network.ensure_request(peer, RpcRequestKind::BeaconBlocksByRoot);
        assert_eq!(
            network
                .pending_history_root_requests
                .values()
                .next()
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn request_lifecycle_closing_peer_gets_no_new_scheduler_work() {
        for closing in [true, false] {
            let temp = TempDir::new().unwrap();
            let (mut network, _) = request_lifecycle_fixture(&temp);
            let peer = PeerId::random();
            network.connected_peers.insert(peer);
            network.peer_support.insert(
                peer,
                PeerRpcSupport {
                    status: true,
                    goodbye: true,
                    light_client_updates_by_range: true,
                    ..Default::default()
                },
            );
            if closing {
                network.disconnect_peer_with_reason(peer, GOODBYE_REASON_FAULT);
                assert!(network.is_request_pending(peer, RpcRequestKind::Goodbye));
            }
            network.drive_rpc_requests();
            assert_eq!(
                network.is_request_pending(peer, RpcRequestKind::Status),
                !closing
            );
            assert_eq!(network.pending_requests.len(), 1);
        }
    }

    #[tokio::test]
    async fn request_lifecycle_rejects_extra_signed_lc_chunks_before_publication() {
        for count in [2, 1] {
            let temp = TempDir::new().unwrap();
            let (mut network, update) = request_lifecycle_fixture(&temp);
            let peer = PeerId::random();
            let original = network.consensus.light_client_store().unwrap();
            network.ensure_request(peer, RpcRequestKind::LightClientUpdatesByRange);
            let key = *network.pending_requests.keys().next().unwrap();
            network.handle_rpc_response(
                key.kind,
                peer,
                key.request_id,
                Eth2RpcResponse::LightClientUpdatesByRange(vec![update; count]),
            );
            let actual = network.consensus.light_client_store().unwrap();
            assert_eq!(actual == original, count > 1);
            assert!(network.pending_requests.is_empty());
            assert!(network.pending_peer_kinds.is_empty());
        }
    }

    #[tokio::test]
    async fn request_lifecycle_ignores_failures_after_pending_cancellation() {
        for kind in [
            RpcRequestKind::LightClientUpdatesByRange,
            RpcRequestKind::MetaData,
            RpcRequestKind::Goodbye,
        ] {
            let temp = TempDir::new().unwrap();
            let (mut network, _) = request_lifecycle_fixture(&temp);
            let peer = PeerId::random();
            network.ensure_request(peer, kind);
            let key = *network.pending_requests.keys().next().unwrap();
            network.connected_peers.insert(peer);
            network.closing_peers.insert(peer);
            network.handle_swarm_event(SwarmEvent::ConnectionClosed {
                peer_id: peer,
                connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                endpoint: ConnectedPoint::Listener {
                    local_addr: "/memory/1".parse().unwrap(),
                    send_back_addr: "/memory/2".parse().unwrap(),
                },
                num_established: 0,
                cause: None,
            });
            assert!(network.pending_requests.is_empty());
            assert!(network.pending_light_client_range_requests.is_empty());
            let event = request_response::Event::OutboundFailure {
                peer,
                connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                request_id: key.request_id,
                error: request_response::OutboundFailure::ConnectionClosed,
            };
            match kind {
                RpcRequestKind::MetaData => network.handle_metadata_rpc_event(event),
                RpcRequestKind::Goodbye => network.handle_goodbye_rpc_event(event),
                _ => network.handle_rpc_event(kind, event),
            }
            assert!(network.peer_failures.is_empty());
            assert!(network.last_rpc_failure.is_none());
            assert!(!network.closing_peers.contains(&peer));
            assert!(
                network
                    .peer_lifecycle
                    .get(&peer)
                    .is_none_or(|state| state.rpc_failures == 0)
            );
        }
    }

    #[tokio::test]
    async fn request_lifecycle_disconnect_of_absent_peer_leaves_no_closing_marker() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let peer = PeerId::random();
        network.disconnect_now(peer);
        assert!(!network.closing_peers.contains(&peer));
    }

    #[tokio::test]
    async fn request_lifecycle_history_partial_empty_and_invalid_tail_controls() {
        for range in [false, true] {
            for case in [0, 1, 2, 3] {
                let temp = TempDir::new().unwrap();
                let (mut network, _) = request_lifecycle_fixture(&temp);
                let peer = PeerId::random();
                let raw = RawRpcResponse {
                    bytes: include_bytes!("../tests/fixtures/beacon_block_14132042.ssz").to_vec(),
                    context_bytes: Some(
                        MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(14132042 / 32),
                    ),
                };
                let block = decode_verified_beacon_block(&raw).unwrap();
                let kind = if range {
                    RpcRequestKind::BeaconBlocksByRange
                } else {
                    RpcRequestKind::BeaconBlocksByRoot
                };
                let roots = vec![block.beacon_root, B256::repeat_byte(0xff)];
                let request = BeaconBlocksByRangeRequest {
                    start_slot: block.slot,
                    count: 2,
                    step: 1,
                };
                let request_id = if range {
                    network
                        .swarm
                        .behaviour_mut()
                        .beacon_blocks_by_range_rpc
                        .inner
                        .send_request(&peer, Eth2RpcRequest::BeaconBlocksByRange(request))
                } else {
                    network
                        .swarm
                        .behaviour_mut()
                        .beacon_blocks_by_root_rpc
                        .inner
                        .send_request(&peer, Eth2RpcRequest::BeaconBlocksByRoot(roots.clone()))
                };
                let key = PendingRequestKey { kind, request_id };
                network.pending_requests.insert(key, peer);
                network.pending_peer_kinds.insert((peer, kind));
                if range {
                    network.pending_history_range_requests.insert(key, request);
                } else {
                    network.pending_history_root_requests.insert(key, roots);
                }
                let chunks = match case {
                    0 => vec![],
                    1 => vec![raw],
                    2 => vec![
                        raw,
                        RawRpcResponse {
                            bytes: vec![0],
                            context_bytes: None,
                        },
                    ],
                    _ => vec![raw.clone(), raw],
                };
                let response = if range {
                    Eth2RpcResponse::BeaconBlocksByRange(chunks)
                } else {
                    Eth2RpcResponse::BeaconBlocksByRoot(chunks)
                };
                network.handle_rpc_response(kind, peer, request_id, response);
                assert_eq!(
                    network
                        .verified_beacon_blocks
                        .contains_key(&block.beacon_root),
                    case == 1
                );
                assert!(network.pending_history_root_requests.is_empty());
                assert!(network.pending_history_range_requests.is_empty());
                assert_eq!(
                    network
                        .peer_lifecycle
                        .get(&peer)
                        .is_some_and(|state| state.ignored_for_run),
                    case >= 2
                );
                if case == 0 {
                    assert!(
                        network
                            .last_rpc_failure
                            .as_ref()
                            .unwrap()
                            .contains("no_decodable_blocks")
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn request_lifecycle_rejects_duplicate_history_root_response() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let peer = PeerId::random();
        let raw = RawRpcResponse {
            bytes: include_bytes!("../tests/fixtures/beacon_block_14132042.ssz").to_vec(),
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(14132042 / 32)),
        };
        let block = decode_verified_beacon_block(&raw).unwrap();
        let roots = vec![block.beacon_root];
        let kind = RpcRequestKind::BeaconBlocksByRoot;
        let request_id = network
            .swarm
            .behaviour_mut()
            .beacon_blocks_by_root_rpc
            .inner
            .send_request(&peer, Eth2RpcRequest::BeaconBlocksByRoot(roots.clone()));
        let key = PendingRequestKey { kind, request_id };
        network.pending_requests.insert(key, peer);
        network.pending_peer_kinds.insert((peer, kind));
        network.pending_history_root_requests.insert(key, roots);
        network.handle_rpc_response(
            kind,
            peer,
            request_id,
            Eth2RpcResponse::BeaconBlocksByRoot(vec![raw.clone(), raw]),
        );
        assert!(
            !network
                .verified_beacon_blocks
                .contains_key(&block.beacon_root)
        );
        assert!(network.pending_history_root_requests.is_empty());
    }

    #[tokio::test]
    async fn consensus_storage_failure_stops_network_until_global_shutdown() {
        let (failure_tx, failure_rx) = watch::channel(None);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let status = Arc::new(Mutex::new(SyncStatus {
            syncing: true,
            consensus_head_fresh: Some(true),
            ..Default::default()
        }));
        assert!(!stop_for_consensus_storage_failure(&failure_rx, &status, &mut shutdown_rx).await);
        assert!(status.lock().unwrap().syncing);

        failure_tx
            .send(Some(Arc::from("state sync failed")))
            .unwrap();
        let mut stopping = std::pin::pin!(stop_for_consensus_storage_failure(
            &failure_rx,
            &status,
            &mut shutdown_rx,
        ));
        assert!(futures::poll!(&mut stopping).is_pending());
        {
            let status = status.lock().unwrap();
            assert!(!status.syncing);
            assert_eq!(status.consensus_head_fresh, Some(false));
        }
        shutdown_tx.send(true).unwrap();
        assert!(stopping.await);
    }

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

    fn peer_cache_test_record() -> PersistedPeer {
        PersistedPeer {
            enr: MAINNET_BOOTNODES[0].to_string(),
            support: Some(PeerRpcSupport {
                status: true,
                ..Default::default()
            }),
            status_successes: u32::MAX,
            bootstrap_successes: u32::MAX,
            useful_successes: u32::MAX,
            dial_stats: PeerDialAddressStats::default(),
        }
    }

    fn peer_cache_quarantined_contents(parent: &Path) -> Vec<Vec<u8>> {
        let mut originals = fs::read_dir(parent)
            .unwrap()
            .map(Result::unwrap)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".known-peers-quarantine-")
            })
            .map(|entry| fs::read(entry.path().join(KNOWN_PEERS_FILE)).unwrap())
            .collect::<Vec<_>>();
        originals.sort();
        originals
    }

    #[test]
    fn peer_cache_recovery_preserves_each_incomplete_original() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut truncated = serde_json::to_vec_pretty(&[peer_cache_test_record()]).unwrap();
        truncated.pop();
        let mut originals = Vec::new();
        for contents in [Vec::new(), truncated, vec![0xff], b"[] []".to_vec()] {
            fs::write(&path, &contents).unwrap();
            assert!(load_known_peers(&path).unwrap().is_empty());
            assert!(!path.exists());
            originals.push(contents);
            originals.sort();
            assert_eq!(
                peer_cache_quarantined_contents(path.parent().unwrap()),
                originals
            );
            // A second open sees an absent cache and creates no extra quarantine.
            assert!(load_known_peers(&path).unwrap().is_empty());
            assert_eq!(
                peer_cache_quarantined_contents(path.parent().unwrap()),
                originals
            );
        }
        let peers = vec![peer_cache_test_record()];
        persist_known_peers(&path, &peers).unwrap();
        assert_eq!(load_known_peers(&path).unwrap(), peers);
        assert_eq!(
            peer_cache_quarantined_contents(path.parent().unwrap()),
            originals
        );
    }

    #[test]
    fn peer_cache_recovery_bounds_file_bytes() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        // Valid JSON with whitespace, one byte above the chosen 1 MiB cache policy.
        let mut contents = vec![b' '; 1024 * 1024 + 1];
        contents[..2].copy_from_slice(b"[]");
        fs::write(&path, &contents).unwrap();
        assert!(load_known_peers(&path).unwrap().is_empty());
        assert!(!path.exists());
        assert_eq!(
            peer_cache_quarantined_contents(path.parent().unwrap()),
            vec![contents]
        );
    }

    #[test]
    fn peer_cache_recovery_enforces_the_writer_record_limit() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        let peers = vec![peer_cache_test_record(); MAX_PERSISTED_KNOWN_PEERS];
        persist_known_peers(&path, &peers).unwrap();
        assert_eq!(load_known_peers(&path).unwrap(), peers);
        let oversized = vec![peer_cache_test_record(); MAX_PERSISTED_KNOWN_PEERS + 1];
        let contents = serde_json::to_vec_pretty(&oversized).unwrap();
        fs::write(&path, &contents).unwrap();
        assert!(load_known_peers(&path).unwrap().is_empty());
        assert!(!path.exists());
        assert_eq!(
            peer_cache_quarantined_contents(path.parent().unwrap()),
            vec![contents]
        );
    }

    #[test]
    fn peer_cache_recovery_leaves_existing_staging_artifacts_untouched() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let old_staging = path.with_extension("json.tmp");
        fs::write(&old_staging, b"retained staging evidence").unwrap();
        let peers = vec![peer_cache_test_record()];
        persist_known_peers(&path, &peers).unwrap();
        assert_eq!(load_known_peers(&path).unwrap(), peers);
        assert_eq!(
            fs::read(&old_staging).unwrap(),
            b"retained staging evidence"
        );
    }

    #[test]
    fn peer_cache_recovery_propagates_real_read_errors() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        assert!(load_known_peers(&path).unwrap().is_empty());
        assert!(!path.parent().unwrap().exists());
        fs::create_dir_all(&path).unwrap();
        assert!(matches!(
            load_known_peers(&path),
            Err(ConsensusNetworkError::ReadKnownPeers { .. })
        ));
        assert!(path.is_dir());
        assert!(peer_cache_quarantined_contents(path.parent().unwrap()).is_empty());
    }

    #[tokio::test]
    async fn peer_cache_recovery_allows_network_construction_without_changing_trust() {
        let temp = TempDir::new().unwrap();
        let network = peer_retention_fixture(&temp);
        let trusted_before = network.consensus.light_client_store();
        fs::write(&network.known_peers_path, b"[").unwrap();
        let loaded = ConsensusNetwork::new(
            network.config.clone(),
            network.consensus.clone(),
            network.sync_status.clone(),
        )
        .unwrap();
        assert!(loaded.last_persisted.is_empty());
        assert_eq!(loaded.consensus.light_client_store(), trusted_before);
        assert!(loaded.bootnode_count > 0);
        assert_eq!(
            peer_cache_quarantined_contents(loaded.known_peers_path.parent().unwrap()),
            vec![b"[".to_vec()]
        );
    }

    #[test]
    fn peer_cache_recovery_preserves_artifacts_on_io_failures() {
        let temp = TempDir::new().unwrap();
        let parent_file = temp.path().join("parent-file");
        fs::write(&parent_file, b"original").unwrap();
        assert!(matches!(
            load_known_peers(&parent_file.join(KNOWN_PEERS_FILE)),
            Err(ConsensusNetworkError::ReadKnownPeers { .. })
        ));
        assert!(quarantine_known_peers(&parent_file.join(KNOWN_PEERS_FILE)).is_err());
        assert_eq!(fs::read(&parent_file).unwrap(), b"original");
        // The parent permits creating quarantine, but the source disappeared.
        assert!(quarantine_known_peers(&temp.path().join("missing.json")).is_err());
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 1);

        let destination = temp.path().join("destination");
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("retained"), b"original").unwrap();
        assert!(persist_known_peers(&destination, &[peer_cache_test_record()]).is_err());
        assert_eq!(fs::read(destination.join("retained")).unwrap(), b"original");
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
    }

    #[test]
    fn peer_cache_recovery_rejects_invalid_writer_input_without_replacement() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        let peers = vec![peer_cache_test_record()];
        persist_known_peers(&path, &peers).unwrap();
        let original = fs::read(&path).unwrap();
        assert!(
            persist_known_peers(
                &path,
                &vec![peer_cache_test_record(); MAX_PERSISTED_KNOWN_PEERS + 1]
            )
            .is_err()
        );
        let mut oversized = peer_cache_test_record();
        oversized.enr = "x".repeat(MAX_PERSISTED_ENR_BYTES + 1);
        assert!(persist_known_peers(&path, &[oversized]).is_err());
        assert_eq!(fs::read(&path).unwrap(), original);
    }

    #[tokio::test]
    async fn peer_cache_recovery_failed_save_retains_last_persisted() {
        let temp = TempDir::new().unwrap();
        let mut network = peer_retention_fixture(&temp);
        network.last_persisted = vec![peer_cache_test_record()];
        let before = network.last_persisted.clone();
        let blocked = temp.path().join("blocked");
        fs::write(&blocked, b"original").unwrap();
        network.known_peers_path = blocked.join(KNOWN_PEERS_FILE);
        assert!(network.persist_known_peers().is_err());
        assert_eq!(network.last_persisted, before);
        assert_eq!(fs::read(blocked).unwrap(), b"original");
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

    const TEST_GOSSIP_TOPIC: &str = "/eth2/8c9f62fe/light_client_optimistic_update/ssz_snappy";
    const HELLO_SNAPPY: &[u8] = &[5, 16, b'h', b'e', b'l', b'l', b'o'];
    const HELLO_SPLIT_SNAPPY: &[u8] = &[5, 4, b'h', b'e', 8, b'l', b'l', b'o'];

    fn gossip_test_message(data: &[u8], topic: &str) -> gossipsub::Message {
        gossipsub::Message {
            source: None,
            data: data.to_vec(),
            sequence_number: None,
            topic: gossipsub::TopicHash::from_raw(topic),
        }
    }

    #[test]
    fn gossip_message_id_matches_independent_decompressed_hash() {
        // Literal raw Snappy blocks use one versus two literal tags to encode
        // ASCII hello. Expected hashes were independently derived with Python
        // hashlib from Altair's domain + topic length LE64 + topic + payload.
        for encoded in [HELLO_SNAPPY, HELLO_SPLIT_SNAPPY] {
            let message = gossip_test_message(encoded, TEST_GOSSIP_TOPIC);
            assert_eq!(
                hex::encode(eth2_message_id(&message).0),
                "acdaed95fb905c932cde9a7c5f82be95a0a13999"
            );
        }
        let other = gossip_test_message(
            HELLO_SNAPPY,
            "/eth2/8c9f62fe/light_client_finality_update/ssz_snappy",
        );
        assert_eq!(
            hex::encode(eth2_message_id(&other).0),
            "72751e8394b7da80573f4351834cc8ec8eab4747"
        );
    }

    #[test]
    fn gossip_message_id_uses_invalid_domain_for_malformed_snappy() {
        let message = gossip_test_message(&[5, 16, b'h'], TEST_GOSSIP_TOPIC);
        assert_eq!(
            hex::encode(eth2_message_id(&message).0),
            "a5fda19ffa82c72dbac59ef5792012e4c5791e86"
        );
        let invalid_empty = gossip_test_message(&[], TEST_GOSSIP_TOPIC);
        assert_eq!(
            hex::encode(eth2_message_id(&invalid_empty).0),
            "23b136acdeb2da6a2fbc26747ab2ea7eddf0fa97"
        );
        let valid_empty = gossip_test_message(&[0], TEST_GOSSIP_TOPIC);
        assert_eq!(
            hex::encode(eth2_message_id(&valid_empty).0),
            "5343304ad5b71850b0ad402d4658092ced89bfac"
        );
    }

    fn gossip_raw_message(data: &[u8]) -> gossipsub::RawMessage {
        gossipsub::RawMessage {
            source: None,
            data: data.to_vec(),
            sequence_number: None,
            topic: gossipsub::TopicHash::from_raw(TEST_GOSSIP_TOPIC),
            signature: None,
            key: None,
            validated: false,
        }
    }

    #[test]
    fn gossip_size_guard_preserves_wire_and_rejects_global_oversize() {
        use gossipsub::DataTransform as _;
        for payload in [HELLO_SNAPPY, HELLO_SPLIT_SNAPPY, &[5, 16, b'h'], &[], &[0]] {
            let message = GossipSizeGuard
                .inbound_transform(gossip_raw_message(payload))
                .unwrap();
            assert_eq!(message.data, payload);
            assert_eq!(
                GossipSizeGuard
                    .outbound_transform(&message.topic, message.data)
                    .unwrap(),
                payload
            );
        }
        let mut buffer = unsigned_varint::encode::u64_buffer();
        let oversized_header =
            unsigned_varint::encode::u64((GOSSIP_MAX_PAYLOAD_SIZE + 1) as u64, &mut buffer);
        assert!(
            GossipSizeGuard
                .inbound_transform(gossip_raw_message(oversized_header))
                .is_err()
        );
        assert!(
            GossipSizeGuard
                .outbound_transform(
                    &gossipsub::TopicHash::from_raw(TEST_GOSSIP_TOPIC),
                    oversized_header.to_vec()
                )
                .is_err()
        );
        // Exercise compressed-size boundaries with a small injected budget;
        // malformed bounded bytes remain admitted for INVALID-domain IDs.
        let max_wire = snap::raw::max_compress_len(8);
        assert!(validate_gossip_wire_size(&vec![0x80; max_wire], 8).is_ok());
        assert!(validate_gossip_wire_size(&vec![0x80; max_wire + 1], 8).is_err());
        assert!(validate_gossip_wire_size(HELLO_SNAPPY, 5).is_ok());
        assert!(validate_gossip_wire_size(HELLO_SNAPPY, 4).is_err());
    }

    #[test]
    fn gossip_type_limits_do_not_change_valid_snappy_ids() {
        let topic = gossipsub::TopicHash::from_raw(TEST_GOSSIP_TOPIC);
        assert_eq!(gossip_payload_limit(&topic), 1_032);
        let finality = gossipsub::TopicHash::from_raw(
            "/eth2/8c9f62fe/light_client_finality_update/ssz_snappy",
        );
        assert_eq!(gossip_payload_limit(&finality), 2_120);
        let retired = gossipsub::TopicHash::from_raw(
            "/eth2/cb0d1acc/light_client_finality_update/ssz_snappy",
        );
        assert_eq!(gossip_payload_limit(&retired), 2_120);
        assert_eq!(
            decode_gossip_payload(HELLO_SNAPPY, 5),
            Some(b"hello".to_vec())
        );
        assert!(decode_gossip_payload(HELLO_SNAPPY, 4).is_none());
        let encoded = snap::raw::Encoder::new()
            .compress_vec(&vec![b'x'; 1_033])
            .unwrap();
        assert!(validate_gossip_wire_size(&encoded, GOSSIP_MAX_PAYLOAD_SIZE).is_ok());
        assert!(decode_gossip_payload(&encoded, gossip_payload_limit(&topic)).is_none());
        let message = gossip_test_message(&encoded, TEST_GOSSIP_TOPIC);
        // SHA256 oracle hashes the decompressed 1033 ASCII x bytes under VALID,
        // even though this payload is too large for the optimistic SSZ type.
        assert_eq!(
            hex::encode(eth2_message_id(&message).0),
            "dc816b31a7b43c062e43b6441746e7a27529c160"
        );
    }

    #[tokio::test]
    async fn gossip_admission_unknown_topic_rejected_before_decode() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let message = gossip_test_message(HELLO_SNAPPY, "/eth2/8c9f62fe/unknown/ssz_snappy");
        assert!(matches!(
            network.handle_gossip_message(PeerId::random(), &message),
            gossipsub::MessageAcceptance::Reject
        ));
        assert_eq!(network.gossip_counts.decode_failures, 0);
    }

    #[tokio::test]
    async fn gossip_admission_finality_without_advance_is_processed_not_forwarded() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let fixture = crate::light_client::test_cached_light_client_fixture(419_072 * 32 + 16);
        let before = network.consensus.light_client_store().unwrap();
        let bytes = snap::raw::Encoder::new()
            .compress_vec(&fixture.payloads.finality_update.unwrap().bytes)
            .unwrap();
        let message = gossip_test_message(
            &bytes,
            network.gossip_topics.finality_update.hash().as_str(),
        );
        assert!(matches!(
            network.handle_gossip_message(PeerId::random(), &message),
            gossipsub::MessageAcceptance::Ignore
        ));
        let after = network.consensus.light_client_store().unwrap();
        assert_eq!(after.finalized_header, before.finalized_header);
        assert!(after.optimistic_header.beacon.slot > before.optimistic_header.beacon.slot);
    }

    fn gossip_test_time(slot: u64, offset_ms: u128) -> u128 {
        u128::from(MAINNET_CONSENSUS_CHAIN_SPEC.genesis_time) * 1000
            + u128::from(slot) * 12000
            + offset_ms
    }

    fn compressed_gossip(bytes: &[u8], topic: &gossipsub::TopicHash) -> gossipsub::Message {
        gossip_test_message(
            &snap::raw::Encoder::new().compress_vec(bytes).unwrap(),
            topic.as_str(),
        )
    }

    #[test]
    fn gossip_admission_timing_uses_floor_basis_points_and_disparity() {
        let slot = 419_072 * 32 + 19;
        assert!(!gossip_update_is_due(slot, gossip_test_time(slot, 3498)));
        assert!(gossip_update_is_due(slot, gossip_test_time(slot, 3499)));
        assert!(gossip_update_is_due(slot, gossip_test_time(slot, 3500)));
        assert!(!gossip_update_is_due(
            u64::MAX,
            gossip_test_time(slot, 3499)
        ));
    }

    #[tokio::test]
    async fn gossip_admission_future_ignored_then_due_update_accepted() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let slot = 419_072 * 32 + 16;
        let (_, bytes) = crate::light_client::test_gossip_payloads(slot, 1);
        let message = compressed_gossip(&bytes, &network.gossip_topics.optimistic_update.hash());
        let before = network.consensus.light_client_store();
        for now in [gossip_test_time(slot, 0), gossip_test_time(slot + 3, 3498)] {
            assert!(matches!(
                network.handle_gossip_message_at(PeerId::random(), &message, now),
                gossipsub::MessageAcceptance::Ignore
            ));
            assert_eq!(network.consensus.light_client_store(), before);
        }
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &message,
                gossip_test_time(slot + 3, 3499)
            ),
            gossipsub::MessageAcceptance::Accept
        ));
    }

    #[tokio::test]
    async fn gossip_admission_forward_history_requires_report_and_exact_finality_match() {
        let slot = 419_072 * 32 + 16;
        for reported in [false, true] {
            let temp = TempDir::new().unwrap();
            let (mut network, _) = request_lifecycle_fixture(&temp);
            let (finality, optimistic) = crate::light_client::test_gossip_payloads(slot, 342);
            let message =
                compressed_gossip(&finality, &network.gossip_topics.finality_update.hash());
            assert!(matches!(
                network.handle_gossip_message_at(
                    PeerId::random(),
                    &message,
                    gossip_test_time(slot + 3, 3499)
                ),
                gossipsub::MessageAcceptance::Accept
            ));
            assert!(network.gossip_forwarded.finality.is_none());
            network.complete_gossip_validation(reported);
            assert_eq!(network.gossip_forwarded.finality.is_some(), reported);
            let message =
                compressed_gossip(&optimistic, &network.gossip_topics.optimistic_update.hash());
            let result = network.handle_gossip_message_at(
                PeerId::random(),
                &message,
                gossip_test_time(slot + 3, 3499),
            );
            assert_eq!(
                matches!(result, gossipsub::MessageAcceptance::Accept),
                reported
            );
            network.complete_gossip_validation(true);
            assert!(matches!(
                network.handle_gossip_message_at(
                    PeerId::random(),
                    &message,
                    gossip_test_time(slot + 3, 3499)
                ),
                gossipsub::MessageAcceptance::Ignore
            ));
        }
    }

    #[tokio::test]
    async fn gossip_admission_same_slot_participation_updates_store_without_forwarding() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let slot = 419_072 * 32 + 16;
        let (_, low) = crate::light_client::test_gossip_payloads(slot, 1);
        let (_, higher) = crate::light_client::test_gossip_payloads(slot, 2);
        let topic = network.gossip_topics.optimistic_update.hash();
        let now = gossip_test_time(slot + 3, 3499);
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &compressed_gossip(&low, &topic),
                now
            ),
            gossipsub::MessageAcceptance::Accept
        ));
        network.complete_gossip_validation(true);
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &compressed_gossip(&higher, &topic),
                now
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .current_max_active_participants,
            2
        );
    }

    #[tokio::test]
    async fn gossip_admission_low_participation_does_not_forward_newer_header() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let slot = 419_072 * 32 + 16;
        let (_, strong) = crate::light_client::test_gossip_payloads(slot, 342);
        let (_, weak) = crate::light_client::test_gossip_payloads(slot + 1, 1);
        let topic = network.gossip_topics.optimistic_update.hash();
        let now = gossip_test_time(slot + 4, 3499);
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &compressed_gossip(&strong, &topic),
                now
            ),
            gossipsub::MessageAcceptance::Accept
        ));
        network.complete_gossip_validation(true);
        let before = network.consensus.light_client_store().unwrap();
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &compressed_gossip(&weak, &topic),
                now
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .optimistic_header,
            before.optimistic_header
        );
        let mismatching = gossip_update_metadata(&weak, false).unwrap();
        let mut forwarded = GossipForwarded {
            finality: Some(
                gossip_update_metadata(
                    &crate::light_client::test_gossip_payloads(slot, 342).0,
                    true,
                )
                .unwrap(),
            ),
            optimistic_slot: None,
        };
        assert!(!forwarded.permits(&mismatching, &before, &before));
        forwarded.finality.as_mut().unwrap().optimistic_bytes =
            mismatching.optimistic_bytes.clone();
        assert!(forwarded.permits(&mismatching, &before, &before));
    }

    fn assert_gossip_unavailable_committee_ignored(periods_ahead: u64, finality: bool) {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let target_slot = 419_072 * 32 + 16 + periods_ahead * 8192;
        let (finality_bytes, optimistic_bytes) =
            crate::light_client::test_gossip_payloads(target_slot, 342);
        let bytes = if finality {
            finality_bytes
        } else {
            optimistic_bytes
        };
        let metadata = gossip_update_metadata(&bytes, finality).unwrap();
        assert_eq!(metadata.attested_slot, target_slot + 2);
        assert_eq!(metadata.signature_slot, target_slot + 3);
        assert_eq!(metadata.participants, 342);
        let before = network.consensus.light_client_store().unwrap();
        assert!(before.next_sync_committee.is_none());
        let verify = |store: &VerifiedLightClientStore| {
            if finality {
                apply_finality_update_payload_at_slot(&bytes, store, target_slot + 3).map(|_| ())
            } else {
                apply_optimistic_update_payload_at_slot(&bytes, store, target_slot + 3).map(|_| ())
            }
        };
        assert!(
            matches!(verify(&before), Err(LightClientVerificationError::UnknownSyncCommitteePeriod { signature_period, store_period }) if signature_period == store_period + periods_ahead)
        );
        // The same bytes authenticate when a bootstrap supplies their committee.
        let target = crate::light_client::test_cached_light_client_fixture(target_slot);
        let (_, target_store) =
            verify_bootstrap_payload(&target.payloads.bootstrap.unwrap().bytes, target.checkpoint)
                .unwrap();
        assert!(verify(&target_store).is_ok());
        let topic = if finality {
            network.gossip_topics.finality_update.hash()
        } else {
            network.gossip_topics.optimistic_update.hash()
        };
        let message = compressed_gossip(&bytes, &topic);
        assert_eq!(
            parse_light_client_topic(&topic).unwrap().1,
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(metadata.attested_slot / 32)
        );
        let now = gossip_test_time(target_slot + 3, 3499);
        assert!(gossip_update_is_due(metadata.signature_slot, now));
        assert!(matches!(
            network.handle_gossip_message_at(PeerId::random(), &message, now),
            gossipsub::MessageAcceptance::Ignore
        ));
        assert_eq!(network.consensus.light_client_store().unwrap(), before);
        assert_eq!(network.gossip_counts.decode_failures, 0);
        assert_eq!(network.gossip_counts.finality_update, 0);
        assert_eq!(network.gossip_counts.optimistic_update, 0);
        assert!(network.pending_gossip_forward.is_none());
        network.complete_gossip_validation(true);
        assert!(network.gossip_forwarded.finality.is_none());
        assert!(network.gossip_forwarded.optimistic_slot.is_none());
        assert!(
            network
                .consensus
                .light_client_finality_update_payload()
                .is_none()
        );
        assert!(
            network
                .consensus
                .light_client_optimistic_update_payload()
                .is_none()
        );
    }

    #[test]
    fn gossip_topic_parser_requires_exact_components_and_lowercase_digest() {
        let canonical = "/eth2/8c9f62fe/light_client_optimistic_update/ssz_snappy";
        assert!(parse_light_client_topic(&gossipsub::TopicHash::from_raw(canonical)).is_some());
        for topic in [
            format!("{canonical}/extra"),
            canonical.replace("8c9f62fe", "8C9F62FE"),
            canonical.replace("8c9f62fe", "8c9f62fe00"),
            canonical.replace("/eth2/", "/eth2//"),
        ] {
            assert!(parse_light_client_topic(&gossipsub::TopicHash::from_raw(topic)).is_none());
        }
    }

    #[tokio::test]
    async fn gossip_unavailable_committee_classification_keeps_invalid_payloads_rejected() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let slot = 419_072 * 32 + 16;
        let (_, bytes) = crate::light_client::test_gossip_payloads(slot, 342);
        let mut malformed = bytes.clone();
        malformed.truncate(5);
        let mut bad_signature = bytes.clone();
        // Optimistic fixed section: header offset (4), bits (64), signature (96).
        bad_signature[68..164].fill(0);
        let mut bad_proof = bytes.clone();
        let header_offset = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        bad_proof[header_offset + 80] ^= 1; // Beacon body root, outside execution proof.
        let store = network.consensus.light_client_store().unwrap();
        assert!(matches!(
            apply_optimistic_update_payload_at_slot(&bad_signature, &store, slot + 3),
            Err(LightClientVerificationError::InvalidSignature(_))
        ));
        assert!(matches!(
            apply_optimistic_update_payload_at_slot(&bad_proof, &store, slot + 3),
            Err(LightClientVerificationError::InvalidExecutionProof { .. })
        ));
        let topic = network.gossip_topics.optimistic_update.hash();
        for bytes in [malformed, bad_signature, bad_proof] {
            assert!(matches!(
                network.handle_gossip_message_at(
                    PeerId::random(),
                    &compressed_gossip(&bytes, &topic),
                    gossip_test_time(slot + 3, 3499)
                ),
                gossipsub::MessageAcceptance::Reject
            ));
            assert_eq!(network.consensus.light_client_store().unwrap(), store);
            assert!(network.pending_gossip_forward.is_none());
        }
        assert_eq!(network.gossip_counts.decode_failures, 3);
    }

    #[tokio::test]
    async fn gossip_unavailable_committee_missing_next_is_ignored() {
        assert_gossip_unavailable_committee_ignored(1, false);
    }

    #[tokio::test]
    async fn gossip_unavailable_committee_farther_catchup_is_ignored() {
        assert_gossip_unavailable_committee_ignored(2, true);
    }

    #[tokio::test]
    async fn gossip_admission_old_signature_period_is_not_peer_fault() {
        let temp = TempDir::new().unwrap();
        let slot = 419_328 * 32 + 16;
        let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
        let (_, old) = crate::light_client::test_gossip_payloads(slot - 8192, 1);
        let message = compressed_gossip(&old, &network.gossip_topics.optimistic_update.hash());
        let before = network.consensus.light_client_store();
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &message,
                gossip_test_time(slot + 3, 3499)
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
        assert_eq!(network.consensus.light_client_store(), before);
        assert_eq!(network.gossip_counts.decode_failures, 0);
    }

    #[tokio::test]
    async fn gossip_admission_real_bpo_transition_processes_retiring_topic_until_expiry() {
        let boundary = 419_072 * 32;
        let slot = boundary - 4;
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
        let fixture = crate::light_client::test_cached_light_client_fixture(slot);
        let bootstrap = fixture.payloads.bootstrap.unwrap();
        let (status, mut store) =
            verify_bootstrap_payload(&bootstrap.bytes, fixture.checkpoint).unwrap();
        // Model a previously proven next committee; all local fixture members use the same key.
        store.next_sync_committee = Some(store.current_sync_committee.clone());
        network
            .consensus
            .record_verified_bootstrap(status, bootstrap, store)
            .unwrap();
        let old_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch((boundary - 1) / 32);
        let new_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(boundary / 32);
        network.fork_digest = old_digest;
        network.gossip_topics = build_gossip_topics(old_digest);
        network.maintain_fork_subscriptions_at_slot(boundary - 2);
        assert_eq!(network.pre_subscribed_fork_digest, Some(new_digest));
        let bytes =
            crate::light_client::test_gossip_boundary_optimistic(slot, boundary - 1, boundary);
        let old_message = compressed_gossip(
            &bytes,
            &build_gossip_topics(old_digest).optimistic_update.hash(),
        );
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &old_message,
                gossip_test_time(boundary - 1, 0)
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
        let future_bytes =
            crate::light_client::test_gossip_boundary_optimistic(slot, boundary, boundary + 1);
        let future_message = compressed_gossip(
            &future_bytes,
            &build_gossip_topics(new_digest).optimistic_update.hash(),
        );
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &future_message,
                gossip_test_time(boundary - 1, 0)
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
        network.maintain_fork_subscriptions_at_slot(boundary);
        assert_eq!(network.fork_digest, new_digest);
        assert_eq!(network.retiring_gossip_topics.len(), 1);
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &old_message,
                gossip_test_time(boundary, 3499)
            ),
            gossipsub::MessageAcceptance::Accept
        ));
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .optimistic_header
                .beacon
                .slot,
            boundary - 1
        );
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &future_message,
                gossip_test_time(boundary + 1, 3499)
            ),
            gossipsub::MessageAcceptance::Accept
        ));
        network.maintain_fork_subscriptions_at_slot(boundary + 64);
        assert!(network.retiring_gossip_topics.is_empty());
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &gossip_test_message(&[], old_message.topic.as_str()),
                gossip_test_time(boundary + 64, 0)
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
    }

    #[tokio::test]
    async fn gossip_admission_finality_exception_requires_identical_aggregate_at_same_slot() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let slot = 419_072 * 32 + 16;
        let (finality, _) = crate::light_client::test_gossip_payloads(slot, 342);
        let (_, other_aggregate) = crate::light_client::test_gossip_payloads(slot, 343);
        let now = gossip_test_time(slot + 3, 3499);
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &compressed_gossip(&finality, &network.gossip_topics.finality_update.hash()),
                now
            ),
            gossipsub::MessageAcceptance::Accept
        ));
        network.complete_gossip_validation(true);
        assert_eq!(
            gossip_update_metadata(&other_aggregate, false)
                .unwrap()
                .attested_slot,
            network
                .gossip_forwarded
                .finality
                .as_ref()
                .unwrap()
                .attested_slot
        );
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &compressed_gossip(
                    &other_aggregate,
                    &network.gossip_topics.optimistic_update.hash()
                ),
                now
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
        assert_eq!(
            network
                .consensus
                .light_client_store()
                .unwrap()
                .current_max_active_participants,
            343
        );
    }

    #[tokio::test]
    async fn gossip_admission_lifecycle_topics_bind_attested_slot() {
        let slot = 412_672 * 32 + 16;
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture_at_slot(&temp, slot);
        let (_, bytes) = crate::light_client::test_gossip_payloads(slot, 1);
        let old_digest = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(slot / 32);
        let old_topics = build_gossip_topics(old_digest);
        let message = compressed_gossip(&bytes, &old_topics.optimistic_update.hash());
        let now = gossip_test_time(slot + 3, 3499);
        // A recognized but inactive topic is ignored before malformed Snappy decoding.
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &gossip_test_message(&[], message.topic.as_str()),
                now
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
        assert_eq!(network.gossip_counts.decode_failures, 0);
        network.pre_subscribed_fork_digest = Some(old_digest);
        assert!(matches!(
            network.handle_gossip_message_at(PeerId::random(), &message, now),
            gossipsub::MessageAcceptance::Accept
        ));
        // Valid old-fork data on the currently configured topic must not be relabeled.
        let wrong = compressed_gossip(&bytes, &network.gossip_topics.optimistic_update.hash());
        assert!(matches!(
            network.handle_gossip_message_at(PeerId::random(), &wrong, now),
            gossipsub::MessageAcceptance::Reject
        ));
        network.pre_subscribed_fork_digest = None;
        network.retiring_gossip_topics.push(RetiringGossipTopics {
            unsubscribe_at_epoch: slot / 32 + 2,
            topics: old_topics,
        });
        assert!(!matches!(
            network.handle_gossip_message_at(PeerId::random(), &message, now),
            gossipsub::MessageAcceptance::Reject
        ));
        assert!(matches!(
            network.handle_gossip_message_at(
                PeerId::random(),
                &gossip_test_message(&[], message.topic.as_str()),
                gossip_test_time((slot / 32 + 2) * 32, 0)
            ),
            gossipsub::MessageAcceptance::Ignore
        ));
    }

    #[test]
    fn gossip_config_matches_pinned_mainnet_parameters() {
        let config = build_gossip_config().unwrap();
        assert_eq!(
            (
                config.mesh_n(),
                config.mesh_n_low(),
                config.mesh_n_high(),
                config.gossip_lazy()
            ),
            (8, 6, 12, 6)
        );
        assert_eq!(config.heartbeat_interval(), Duration::from_millis(700));
        assert_eq!(config.fanout_ttl(), Duration::from_secs(60));
        assert_eq!((config.history_length(), config.history_gossip()), (6, 3));
        assert_eq!(config.duplicate_cache_time(), Duration::from_secs(768));
        assert_eq!(
            config.max_transmit_size_for_topic(&gossipsub::TopicHash::from_raw(TEST_GOSSIP_TOPIC)),
            12_234_442
        );
        assert_eq!(GOSSIP_MAX_PAYLOAD_SIZE, 10_485_760);
        assert!(config.validate_messages());
        assert!(matches!(
            config.validation_mode(),
            gossipsub::ValidationMode::Anonymous
        ));
    }

    #[tokio::test]
    async fn gossip_event_keeps_compressed_accounting_and_rejects_type_oversize() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let peer = PeerId::random();
        let topic = network.gossip_topics.optimistic_update.hash();
        let encoded = snap::raw::Encoder::new()
            .compress_vec(&vec![b'x'; 1_033])
            .unwrap();
        let message = gossip_test_message(&encoded, topic.as_str());
        let before = network.consensus.light_client_store();
        let message_id = eth2_message_id(&message);
        network.handle_gossip_event(gossipsub::Event::Message {
            propagation_source: peer,
            message_id,
            message,
        });
        assert_eq!(network.gossip_counts.decode_failures, 1);
        assert_eq!(
            network
                .p2p_download_metrics
                .snapshot(Instant::now())
                .total_payload_bytes,
            encoded.len() as u64
        );
        assert_eq!(network.consensus.light_client_store(), before);
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
    fn historical_rpc_context_uses_the_object_epoch() {
        for (epoch, correct, incorrect) in [
            (194_048, [0xbb, 0xa4, 0xda, 0x96], [0xad, 0x53, 0x2c, 0xeb]),
            (269_568, [0x6a, 0x95, 0xa1, 0xa9], [0x24, 0x43, 0x18, 0x33]),
            (364_032, [0xad, 0x53, 0x2c, 0xeb], [0xe3, 0x85, 0x95, 0x71]),
            (411_392, [0xcc, 0x2c, 0x5c, 0xdb], [0xad, 0x53, 0x2c, 0xeb]),
            (412_672, [0xcb, 0x0d, 0x1a, 0xcc], [0xcc, 0x2c, 0x5c, 0xdb]),
            (419_072, [0x8c, 0x9f, 0x62, 0xfe], [0xcb, 0x0d, 0x1a, 0xcc]),
        ] {
            let slot = epoch * 32;
            let mut response = RawRpcResponse {
                context_bytes: Some(correct),
                bytes: Vec::new(),
            };
            assert!(rpc_context_matches_slot(&response, slot), "epoch {epoch}");
            response.context_bytes = Some(incorrect);
            assert!(!rpc_context_matches_slot(&response, slot), "epoch {epoch}");
            assert!(crate::light_client::normalize_cached_context(&mut response, slot).is_err());
            response.context_bytes = None;
            crate::light_client::normalize_cached_context(&mut response, slot).unwrap();
            assert_eq!(response.context_bytes, Some(correct), "epoch {epoch}");
        }
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
    fn peer_scoring_large_dial_counters_keep_exact_scores() {
        // Literal results cover multiplication, signed-conversion and subtraction
        // boundaries of the original 32-bit score, including mixed history.
        for (successes, failures, expected) in [
            (0, 0, 400_i64),
            (21_474_833, 0, 2_147_483_700),
            (0, 17_179_870, -2_147_483_350),
            (2_147_483_648, 0, 214_748_365_200),
            (0, 2_147_483_648, -268_435_455_600),
            (u32::MAX, 0, 429_496_729_900),
            (0, u32::MAX, -536_870_911_475),
            (u32::MAX, u32::MAX, -107_374_181_975),
        ] {
            let stats = PeerDialAddressStats {
                tcp4_successes: successes,
                tcp4_failures: failures,
                ..Default::default()
            };
            assert_eq!(
                stats.priority(DialAddressClass::Tcp4, true),
                expected,
                "successes={successes}, failures={failures}"
            );
        }
    }

    #[test]
    fn peer_scoring_dial_ranking_is_monotonic_for_every_class() {
        for class in [
            DialAddressClass::Tcp4,
            DialAddressClass::Quic4,
            DialAddressClass::Tcp6,
            DialAddressClass::Quic6,
        ] {
            for bootstrap_needed in [false, true] {
                let mut previous_success = i64::MIN;
                let mut previous_failure = i64::MAX;
                for count in [0, 1, 17_179_870, 21_474_833, 2_147_483_648, u32::MAX] {
                    let mut stats = PeerDialAddressStats::default();
                    *stats.successes_mut(class) = count;
                    let success = stats.priority(class, bootstrap_needed);
                    assert!(success > previous_success);
                    previous_success = success;
                    *stats.successes_mut(class) = 0;
                    *stats.failures_mut(class) = count;
                    let failure = stats.priority(class, bootstrap_needed);
                    assert!(failure < previous_failure);
                    previous_failure = failure;
                }
            }
        }
    }

    #[test]
    fn peer_scoring_restored_success_counters_saturate() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        let cached = PersistedPeer {
            enr: MAINNET_BOOTNODES[0].to_string(),
            support: None,
            status_successes: u32::MAX,
            bootstrap_successes: u32::MAX,
            useful_successes: u32::MAX,
            dial_stats: PeerDialAddressStats::default(),
        };
        persist_known_peers(&path, &[cached]).unwrap();
        let loaded = load_known_peers(&path).unwrap();
        for kind in [
            RpcRequestKind::Status,
            RpcRequestKind::LightClientBootstrap,
            RpcRequestKind::LightClientUpdatesByRange,
            RpcRequestKind::LightClientFinalityUpdate,
            RpcRequestKind::LightClientOptimisticUpdate,
            RpcRequestKind::BeaconBlocksByRange,
            RpcRequestKind::BeaconBlocksByRoot,
            RpcRequestKind::Goodbye,
            RpcRequestKind::MetaData,
            RpcRequestKind::Ping,
        ] {
            let mut lifecycle = PeerLifecycleState::from_persisted(&loaded[0]);
            let priority = peer_lifecycle_priority(&lifecycle, 5, false);
            lifecycle.record_success(kind);
            assert_eq!(lifecycle.status_successes, u32::MAX);
            assert_eq!(lifecycle.bootstrap_successes, u32::MAX);
            assert_eq!(lifecycle.useful_successes, u32::MAX);
            assert!(lifecycle.preferred());
            assert_eq!(peer_lifecycle_priority(&lifecycle, 5, false), priority);
        }
    }

    #[tokio::test]
    async fn peer_scoring_restored_dial_history_preserves_address_selection() {
        let temp = TempDir::new().unwrap();
        let network = peer_retention_fixture(&temp);
        let enr = freshness_enr(1, 1, Some(9000), false, network.fork_digest);
        let peer = peer_id_from_enr(&enr).unwrap();
        let cached = PersistedPeer {
            enr: enr.to_base64(),
            support: None,
            status_successes: 0,
            bootstrap_successes: 0,
            useful_successes: 0,
            dial_stats: PeerDialAddressStats {
                tcp4_successes: u32::MAX,
                quic4_failures: u32::MAX,
                ..Default::default()
            },
        };
        persist_known_peers(&network.known_peers_path, &[cached]).unwrap();
        let loaded = ConsensusNetwork::new(
            network.config.clone(),
            network.consensus.clone(),
            network.sync_status.clone(),
        )
        .unwrap();
        let tcp: Multiaddr = format!("/ip4/127.0.0.1/tcp/9000/p2p/{peer}")
            .parse()
            .unwrap();
        let quic: Multiaddr = format!("/ip4/127.0.0.1/udp/9000/quic-v1/p2p/{peer}")
            .parse()
            .unwrap();
        for bootstrap_needed in [false, true] {
            assert_eq!(
                loaded.select_dial_addresses(
                    peer,
                    vec![quic.clone(), tcp.clone()],
                    bootstrap_needed
                ),
                vec![tcp.clone(), quic.clone()]
            );
            assert!(
                loaded.dial_address_priority(peer, &Multiaddr::empty(), bootstrap_needed)
                    < loaded.dial_address_priority(peer, &quic, bootstrap_needed)
            );
        }
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

        let (peer_id, _) = retain_peer_enr(
            &mut HashMap::new(),
            &mut peers,
            ConsensusDialAddressFamilies::IPV6,
            &[0; 4],
            &peer_enr,
            true,
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
        let mut payloads = BeaconPayloadCache::default();
        for (root, raw) in [
            (B256::repeat_byte(0x11), payload(0x11)),
            (B256::repeat_byte(0x22), payload(0x22)),
        ] {
            payloads.insert(root, raw);
        }

        let responses = cached_beacon_block_payloads_by_root(
            &[
                B256::repeat_byte(0x22),
                B256::repeat_byte(0x33),
                B256::repeat_byte(0x11),
            ],
            &payloads,
        );

        assert_eq!(
            responses
                .iter()
                .map(|body| body.as_raw().clone())
                .collect::<Vec<_>>(),
            vec![payload(0x22), payload(0x11)]
        );
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
        let mut payloads = BeaconPayloadCache::default();
        for (root, raw) in [
            (B256::repeat_byte(0x10), payload(0x10)),
            (B256::repeat_byte(0x11), payload(0x11)),
            (B256::repeat_byte(0x12), payload(0x12)),
            (B256::repeat_byte(0x13), payload(0x13)),
        ] {
            payloads.insert(root, raw);
        }

        let responses = cached_beacon_block_payloads_by_range(
            BeaconBlocksByRangeRequest {
                start_slot: 100,
                count: 4,
                step: 1,
            },
            &canonical_blocks,
            &payloads,
        );

        assert_eq!(
            responses
                .iter()
                .map(|body| body.as_raw().clone())
                .collect::<Vec<_>>(),
            vec![payload(0x10), payload(0x11), payload(0x13)]
        );
        // A missing body is not an empty slot: never serve past a canonical hole.
        let mut sparse = BeaconPayloadCache::default();
        sparse.insert(B256::repeat_byte(0x10), payload(0x10));
        sparse.insert(B256::repeat_byte(0x13), payload(0x13));
        let request = BeaconBlocksByRangeRequest {
            start_slot: 100,
            count: 4,
            step: 1,
        };
        let prefix = cached_beacon_block_payloads_by_range(request, &canonical_blocks, &sparse);
        assert_eq!(prefix.len(), 1);
        assert_eq!(prefix[0].as_raw(), &payload(0x10));
        let missing_first = BeaconBlocksByRangeRequest {
            start_slot: 101,
            ..request
        };
        assert!(
            cached_beacon_block_payloads_by_range(missing_first, &canonical_blocks, &sparse)
                .is_empty()
        );
    }

    #[tokio::test]
    async fn beacon_cache_v1_first_body_supports_v2_after_duplicate() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        let raw = RawRpcResponse {
            bytes: include_bytes!("../tests/fixtures/beacon_block_14132042.ssz").to_vec(),
            context_bytes: None,
        };
        let block = decode_verified_beacon_block(&raw).unwrap();
        assert!(network.record_verified_beacon_block(block, Some(raw)));
        let first = network
            .verified_beacon_block_payloads
            .get(&block.beacon_root)
            .unwrap()
            .clone();
        let context = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(block.slot / 32);
        assert_eq!(first.as_raw().context_bytes, Some(context));
        let duplicate = first.as_raw().clone();
        assert_eq!(decode_verified_beacon_block(&duplicate).unwrap(), block);
        assert!(!network.record_verified_beacon_block(block, Some(duplicate)));
        let cached = network.cached_verified_beacon_blocks_by_root(&[block.beacon_root]);
        assert!(std::ptr::eq(first.as_raw(), cached[0].as_raw()));
        let mut wire = futures::io::Cursor::new(Vec::new());
        let protocol = crate::rpc::Eth2RpcProtocol::BeaconBlocksByRootV2;
        let mut codec = crate::rpc::Eth2RpcCodec::default();
        request_response::Codec::write_response(
            &mut codec,
            &protocol,
            &mut wire,
            Eth2RpcResponse::CachedBeaconBlocksByRoot(cached).into(),
        )
        .await
        .unwrap();
        wire.set_position(0);
        let response = request_response::Codec::read_response(&mut codec, &protocol, &mut wire)
            .await
            .unwrap();
        assert_eq!(
            response.into_parts().0,
            Eth2RpcResponse::BeaconBlocksByRoot(vec![first.as_raw().clone()])
        );
        let mut wrong = first.as_raw().clone();
        wrong.context_bytes = Some([0xff; 4]);
        assert!(decode_verified_beacon_block(&wrong).is_err());
    }

    #[tokio::test]
    async fn beacon_cache_eviction_preserves_verified_metadata_and_ancestry() {
        let temp = TempDir::new().unwrap();
        let (mut network, _) = request_lifecycle_fixture(&temp);
        network.verified_beacon_block_payloads = BeaconPayloadCache::with_limits(1, 1);
        let make_block = |slot, root, parent| VerifiedBeaconBlock {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon_root: root,
            parent_root: parent,
            slot,
            execution_anchor: logex_types::ExecutionAnchor {
                beacon_root: root,
                beacon_slot: slot,
                block_number: slot,
                block_hash: root,
                receipts_root: root,
            },
        };
        let first = make_block(100, B256::repeat_byte(1), B256::ZERO);
        let second = make_block(101, B256::repeat_byte(2), first.beacon_root);
        let payload = || RawRpcResponse {
            context_bytes: Some([0; 4]),
            bytes: vec![1],
        };
        assert!(network.record_verified_beacon_block(first, Some(payload())));
        let held = network
            .verified_beacon_block_payloads
            .get(&first.beacon_root)
            .unwrap()
            .clone();
        assert!(network.record_verified_beacon_block(second, Some(payload())));
        assert!(
            network
                .verified_beacon_block_payloads
                .get(&second.beacon_root)
                .is_none()
        );
        assert_eq!(
            network.verified_beacon_blocks.get(&first.beacon_root),
            Some(&first)
        );
        assert_eq!(
            network.verified_beacon_blocks.get(&second.beacon_root),
            Some(&second)
        );
        assert!(network.verified_beacon_block_children[&first.beacon_root].contains(&second));
        drop(held);
        assert!(!network.record_verified_beacon_block(second, Some(payload())));
        assert!(
            network
                .verified_beacon_block_payloads
                .get(&second.beacon_root)
                .is_some()
        );
    }

    #[test]
    fn sync_committee_period_uses_mainnet_slot_scale() {
        assert_eq!(sync_committee_period_for_slot(0), 0);
        assert_eq!(sync_committee_period_for_slot(8191), 0);
        assert_eq!(sync_committee_period_for_slot(8192), 1);
    }
}
