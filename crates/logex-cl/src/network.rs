use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::hex;
use discv5::enr::{CombinedKey, EnrPublicKey, NodeId};
use discv5::{ConfigBuilder, Discv5, Enr, Event, ListenConfig};
use futures::StreamExt;
use libp2p::identify;
use libp2p::identity;
use libp2p::multiaddr::Protocol;
use libp2p::request_response;
use libp2p::swarm::dial_opts::{DialOpts, PeerCondition};
use libp2p::swarm::{NetworkBehaviour, Swarm, SwarmEvent};
use libp2p::{Multiaddr, PeerId, SwarmBuilder, noise, tcp, yamux};
use libp2p_mplex as mplex;
use logex_types::{ConsensusNetworkStatus, SyncStatus, WeakSubjectivityCheckpoint};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::rpc::{
    Eth2OutboundRequestId, Eth2RpcBehaviour, Eth2RpcEvent, Eth2RpcRequest, Eth2RpcResponse,
    LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID, LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID,
    LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID, METADATA_V1_PROTOCOL_ID, METADATA_V2_PROTOCOL_ID,
    METADATA_V3_PROTOCOL_ID, MetaData, PING_PROTOCOL_ID, STATUS_V1_PROTOCOL_ID,
    STATUS_V2_PROTOCOL_ID, StatusMessage, build_light_client_bootstrap_behaviour,
    build_light_client_finality_update_behaviour, build_light_client_optimistic_update_behaviour,
    build_metadata_behaviour, build_ping_behaviour, build_status_behaviour, resource_unavailable,
};
use crate::{ConsensusStore, decode_bootstrap, decode_finality_update, decode_optimistic_update};

const CONSENSUS_STATE_DIR: &str = "cl";
const DISCOVERY_SECRET_FILE: &str = "discovery-secret";
const KNOWN_PEERS_FILE: &str = "known-peers.json";
const DISCOVERY_QUERY_INTERVAL: Duration = Duration::from_secs(15);
const KNOWN_PEER_PERSIST_INTERVAL: Duration = Duration::from_secs(30);
const RPC_REQUEST_INTERVAL: Duration = Duration::from_secs(5);
const MAX_CONCURRENT_RPC_REQUESTS_PER_KIND: usize = 2;
const MAX_STATUS_FAILURES_BEFORE_DISCONNECT: u32 = 2;
const IDENTIFY_PROTOCOL_VERSION: &str = "eth2/1.0.0";
const IDENTIFY_AGENT_VERSION: &str = concat!("logex/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
pub struct ConsensusNetworkConfig {
    pub data_dir: PathBuf,
    pub checkpoint: WeakSubjectivityCheckpoint,
    pub discovery_port: u16,
    pub p2p_port: u16,
    pub max_peers: usize,
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
    #[error("built-in mainnet bootnodes did not expose an eth2 fork id")]
    MissingBootnodeForkId,
    #[error("built-in mainnet bootnodes exposed an invalid eth2 fork id")]
    InvalidBootnodeForkId,
    #[error("failed to construct consensus discovery service: {0}")]
    ConstructDiscovery(String),
    #[error("failed to start consensus discovery service: {0}")]
    StartDiscovery(String),
    #[error("failed to open consensus discovery event stream: {0}")]
    EventStream(String),
    #[error("failed to construct consensus libp2p transport: {0}")]
    ConstructRpcTransport(String),
    #[error("failed to bind consensus libp2p listener: {0}")]
    ListenRpcTransport(String),
    #[error("failed to derive libp2p identity from consensus secret key: {0}")]
    Libp2pIdentity(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedPeer {
    enr: String,
}

#[derive(NetworkBehaviour)]
#[behaviour(to_swarm = "ConsensusBehaviourEvent")]
struct ConsensusBehaviour {
    identify: identify::Behaviour,
    status_rpc: StatusRpcBehaviour,
    metadata_rpc: MetadataRpcBehaviour,
    ping_rpc: PingRpcBehaviour,
    light_client_bootstrap_rpc: LightClientBootstrapRpcBehaviour,
    light_client_finality_update_rpc: LightClientFinalityUpdateRpcBehaviour,
    light_client_optimistic_update_rpc: LightClientOptimisticUpdateRpcBehaviour,
}

#[derive(Debug)]
enum ConsensusBehaviourEvent {
    Identify(Box<identify::Event>),
    StatusRpc(Eth2RpcEvent),
    MetadataRpc(Eth2RpcEvent),
    PingRpc(Eth2RpcEvent),
    LightClientBootstrapRpc(Eth2RpcEvent),
    LightClientFinalityUpdateRpc(Eth2RpcEvent),
    LightClientOptimisticUpdateRpc(Eth2RpcEvent),
}

impl From<identify::Event> for ConsensusBehaviourEvent {
    fn from(event: identify::Event) -> Self {
        Self::Identify(Box::new(event))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingRequestKey {
    kind: RpcRequestKind,
    request_id: Eth2OutboundRequestId,
}

pub fn spawn_consensus_network(
    config: ConsensusNetworkConfig,
    consensus: Arc<ConsensusStore>,
    sync_status: Arc<Mutex<SyncStatus>>,
    shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>, ConsensusNetworkError> {
    let network = ConsensusNetwork::new(config, consensus, sync_status)?;
    Ok(tokio::spawn(async move {
        if let Err(error) = network.run(shutdown).await {
            tracing::error!(%error, "consensus discovery task exited with an error");
        }
    }))
}

struct ConsensusNetwork {
    config: ConsensusNetworkConfig,
    consensus: Arc<ConsensusStore>,
    sync_status: Arc<Mutex<SyncStatus>>,
    discv5: Discv5,
    swarm: Swarm<ConsensusBehaviour>,
    bootnode_count: usize,
    fork_digest: [u8; 4],
    known_peers_path: PathBuf,
    last_persisted: Vec<PersistedPeer>,
    observed: BTreeSet<String>,
    dialable_peers: HashMap<PeerId, Vec<Multiaddr>>,
    dialing_peers: HashSet<PeerId>,
    connected_peers: HashSet<PeerId>,
    peer_support: HashMap<PeerId, PeerRpcSupport>,
    peer_failures: HashMap<PeerId, PeerFailureCounts>,
    status_peers: HashSet<PeerId>,
    metadata_peers: HashSet<PeerId>,
    ping_peers: HashSet<PeerId>,
    bootstrap_peers: HashSet<PeerId>,
    finality_update_peers: HashSet<PeerId>,
    optimistic_update_peers: HashSet<PeerId>,
    pending_requests: HashMap<PendingRequestKey, PeerId>,
    pending_peer_kinds: HashSet<(PeerId, RpcRequestKind)>,
    request_failures: RpcFailureCounts,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RpcRequestKind {
    Status,
    MetaData,
    Ping,
    LightClientBootstrap,
    LightClientFinalityUpdate,
    LightClientOptimisticUpdate,
}

impl RpcRequestKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::MetaData => "metadata",
            Self::Ping => "ping",
            Self::LightClientBootstrap => "light_client_bootstrap",
            Self::LightClientFinalityUpdate => "light_client_finality_update",
            Self::LightClientOptimisticUpdate => "light_client_optimistic_update",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct RpcFailureCounts {
    status: u64,
    metadata: u64,
    ping: u64,
    bootstrap: u64,
    finality_update: u64,
    optimistic_update: u64,
}

impl RpcFailureCounts {
    fn increment(&mut self, kind: RpcRequestKind) {
        match kind {
            RpcRequestKind::Status => self.status += 1,
            RpcRequestKind::MetaData => self.metadata += 1,
            RpcRequestKind::Ping => self.ping += 1,
            RpcRequestKind::LightClientBootstrap => self.bootstrap += 1,
            RpcRequestKind::LightClientFinalityUpdate => self.finality_update += 1,
            RpcRequestKind::LightClientOptimisticUpdate => self.optimistic_update += 1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerFailureCounts {
    status: u32,
    metadata: u32,
    ping: u32,
    bootstrap: u32,
    finality_update: u32,
    optimistic_update: u32,
}

impl PeerFailureCounts {
    fn increment(&mut self, kind: RpcRequestKind) -> u32 {
        match kind {
            RpcRequestKind::Status => {
                self.status += 1;
                self.status
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
            RpcRequestKind::LightClientFinalityUpdate => {
                self.finality_update += 1;
                self.finality_update
            }
            RpcRequestKind::LightClientOptimisticUpdate => {
                self.optimistic_update += 1;
                self.optimistic_update
            }
        }
    }

    fn reset(&mut self, kind: RpcRequestKind) {
        match kind {
            RpcRequestKind::Status => self.status = 0,
            RpcRequestKind::MetaData => self.metadata = 0,
            RpcRequestKind::Ping => self.ping = 0,
            RpcRequestKind::LightClientBootstrap => self.bootstrap = 0,
            RpcRequestKind::LightClientFinalityUpdate => self.finality_update = 0,
            RpcRequestKind::LightClientOptimisticUpdate => self.optimistic_update = 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct PeerRpcSupport {
    status: bool,
    metadata: bool,
    ping: bool,
    light_client_bootstrap: bool,
    light_client_finality_update: bool,
    light_client_optimistic_update: bool,
}

impl PeerRpcSupport {
    fn from_identify_info(info: &identify::Info) -> Self {
        let mut support = Self::default();
        for protocol in &info.protocols {
            match protocol.as_ref() {
                STATUS_V1_PROTOCOL_ID | STATUS_V2_PROTOCOL_ID => support.status = true,
                METADATA_V1_PROTOCOL_ID | METADATA_V2_PROTOCOL_ID | METADATA_V3_PROTOCOL_ID => {
                    support.metadata = true;
                }
                PING_PROTOCOL_ID => support.ping = true,
                LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID => support.light_client_bootstrap = true,
                LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID => {
                    support.light_client_finality_update = true;
                }
                LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID => {
                    support.light_client_optimistic_update = true;
                }
                _ => {}
            }
        }
        support
    }

    const fn supports_request(self, kind: RpcRequestKind) -> bool {
        match kind {
            RpcRequestKind::Status => self.status,
            RpcRequestKind::MetaData => self.metadata,
            RpcRequestKind::Ping => self.ping,
            RpcRequestKind::LightClientBootstrap => self.light_client_bootstrap,
            RpcRequestKind::LightClientFinalityUpdate => self.light_client_finality_update,
            RpcRequestKind::LightClientOptimisticUpdate => self.light_client_optimistic_update,
        }
    }

    const fn supports_light_client(self) -> bool {
        self.light_client_bootstrap
            && self.light_client_finality_update
            && self.light_client_optimistic_update
    }
}

impl ConsensusNetwork {
    fn new(
        config: ConsensusNetworkConfig,
        consensus: Arc<ConsensusStore>,
        sync_status: Arc<Mutex<SyncStatus>>,
    ) -> Result<Self, ConsensusNetworkError> {
        let bootnodes = mainnet_bootnodes()?;
        let fork_id = current_eth2_fork_id(&bootnodes)?;
        let fork_digest = current_fork_digest(&fork_id)?;
        let secret_path = discovery_secret_path(&config.data_dir);
        let known_peers_path = known_peers_path(&config.data_dir);
        let enr_key = load_or_create_secret_key(&secret_path)?;
        let local_enr = build_local_enr(&enr_key, &fork_id, config.discovery_port, config.p2p_port);
        let local_keypair = build_libp2p_keypair(&enr_key)?;
        let listen_config =
            ListenConfig::from_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.discovery_port);
        let discovery_config = ConfigBuilder::new(listen_config)
            .enable_packet_filter()
            .build();
        let discv5 = Discv5::new(local_enr, enr_key, discovery_config)
            .map_err(|error| ConsensusNetworkError::ConstructDiscovery(error.to_string()))?;

        let known_peers = load_known_peers(&known_peers_path)?;
        let mut dialable_peers = HashMap::new();

        for enr in &bootnodes {
            if let Err(error) = discv5.add_enr(enr.clone()) {
                tracing::warn!(%error, enr = %enr, "failed to seed consensus bootnode");
            }
            observe_dialable_peer(&mut dialable_peers, enr);
        }

        for peer in &known_peers {
            match peer.enr.parse::<Enr>() {
                Ok(enr) => {
                    if let Err(error) = discv5.add_enr(enr.clone()) {
                        tracing::debug!(
                            %error,
                            enr = %peer.enr,
                            "skipping cached consensus peer that could not be inserted"
                        );
                    }
                    observe_dialable_peer(&mut dialable_peers, &enr);
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

        let swarm = build_rpc_swarm(local_keypair)?;

        Ok(Self {
            config,
            consensus,
            sync_status,
            discv5,
            swarm,
            bootnode_count: bootnodes.len(),
            fork_digest,
            known_peers_path,
            last_persisted: known_peers,
            observed: BTreeSet::new(),
            dialable_peers,
            dialing_peers: HashSet::new(),
            connected_peers: HashSet::new(),
            peer_support: HashMap::new(),
            peer_failures: HashMap::new(),
            status_peers: HashSet::new(),
            metadata_peers: HashSet::new(),
            ping_peers: HashSet::new(),
            bootstrap_peers: HashSet::new(),
            finality_update_peers: HashSet::new(),
            optimistic_update_peers: HashSet::new(),
            pending_requests: HashMap::new(),
            pending_peer_kinds: HashSet::new(),
            request_failures: RpcFailureCounts::default(),
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
        let listen_addr = Multiaddr::empty()
            .with(Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
            .with(Protocol::Tcp(self.config.p2p_port));
        self.swarm
            .listen_on(listen_addr)
            .map_err(|error| ConsensusNetworkError::ListenRpcTransport(error.to_string()))?;
        let mut event_stream = self
            .discv5
            .event_stream()
            .await
            .map_err(|error| ConsensusNetworkError::EventStream(error.to_string()))?;

        let local_enr = self.discv5.local_enr();
        tracing::info!(
            local_enr = %local_enr.to_base64(),
            node_id = %local_enr.node_id(),
            discovery_port = self.config.discovery_port,
            p2p_port = self.config.p2p_port,
            local_peer_id = %self.swarm.local_peer_id(),
            bootnodes = self.bootnode_count,
            "consensus network started"
        );
        self.refresh_status();

        let mut query_interval = tokio::time::interval(DISCOVERY_QUERY_INTERVAL);
        query_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut persist_interval = tokio::time::interval(KNOWN_PEER_PERSIST_INTERVAL);
        persist_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut rpc_interval = tokio::time::interval(RPC_REQUEST_INTERVAL);
        rpc_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = wait_for_shutdown(&mut shutdown) => {
                    tracing::info!("consensus network shutting down");
                    break;
                }
                _ = query_interval.tick() => {
                    self.drive_discovery_queries().await;
                    self.refresh_status();
                }
                _ = rpc_interval.tick() => {
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

    async fn drive_discovery_queries(&mut self) {
        let target = NodeId::random();
        match self.discv5.find_node(target).await {
            Ok(found) => {
                for enr in found {
                    self.observe_enr(&enr);
                }
            }
            Err(error) => {
                tracing::debug!(%error, "consensus discovery query failed");
            }
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
        self.observed.insert(enr.node_id().to_string());
        observe_dialable_peer(&mut self.dialable_peers, enr);
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
                self.dialing_peers.remove(&peer_id);
                self.connected_peers.insert(peer_id);
                self.drive_rpc_requests();
            }
            SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                tracing::debug!(%peer_id, cause = ?cause, "consensus libp2p connection closed");
                self.dialing_peers.remove(&peer_id);
                self.connected_peers.remove(&peer_id);
                self.clear_peer_state(peer_id);
                self.clear_pending_requests_for_peer(peer_id);
            }
            SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                tracing::debug!(peer_id = peer_id.map(|peer| peer.to_string()), %error, "consensus libp2p dial failed");
                if let Some(peer_id) = peer_id {
                    self.dialing_peers.remove(&peer_id);
                    self.clear_pending_requests_for_peer(peer_id);
                }
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::StatusRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::Status, event);
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
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::LightClientFinalityUpdateRpc(event)) => {
                self.handle_rpc_event(RpcRequestKind::LightClientFinalityUpdate, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::LightClientOptimisticUpdateRpc(
                event,
            )) => {
                self.handle_rpc_event(RpcRequestKind::LightClientOptimisticUpdate, event);
            }
            SwarmEvent::Behaviour(ConsensusBehaviourEvent::Identify(event)) => {
                self.handle_identify_event(*event);
            }
            _ => {}
        }
    }

    fn handle_identify_event(&mut self, event: identify::Event) {
        match event {
            identify::Event::Received { peer_id, info, .. } => {
                let support = PeerRpcSupport::from_identify_info(&info);
                tracing::debug!(
                    %peer_id,
                    protocol_version = %info.protocol_version,
                    agent_version = %info.agent_version,
                    listen_addrs = info.listen_addrs.len(),
                    protocols = info.protocols.len(),
                    supports_light_client = support.supports_light_client(),
                    "received consensus identify info"
                );
                self.peer_support.insert(peer_id, support);
                if !support.supports_light_client() {
                    tracing::debug!(
                        %peer_id,
                        status = support.status,
                        metadata = support.metadata,
                        bootstrap = support.light_client_bootstrap,
                        finality = support.light_client_finality_update,
                        optimistic = support.light_client_optimistic_update,
                        "disconnecting consensus peer that does not advertise the full light-client req/resp set"
                    );
                    self.disconnect_peer(peer_id);
                    return;
                }
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

    fn handle_rpc_event(&mut self, kind: RpcRequestKind, event: Eth2RpcEvent) {
        match event {
            request_response::Event::Message { peer, message, .. } => match message {
                request_response::Message::Request {
                    request, channel, ..
                } => {
                    tracing::debug!(%peer, ?request, "received inbound consensus RPC request");
                    let response = match request {
                        Eth2RpcRequest::Status(status) => {
                            if status.fork_digest != self.fork_digest {
                                tracing::debug!(
                                    %peer,
                                    local = hex::encode(self.fork_digest),
                                    remote = hex::encode(status.fork_digest),
                                    "consensus peer requested status with a different fork digest"
                                );
                            }
                            self.status_peers.insert(peer);
                            Eth2RpcResponse::Status(self.local_status_message())
                        }
                        Eth2RpcRequest::MetaData => {
                            Eth2RpcResponse::MetaData(self.local_metadata())
                        }
                        Eth2RpcRequest::Ping(_) => {
                            Eth2RpcResponse::Ping(self.local_metadata().seq_number)
                        }
                        Eth2RpcRequest::LightClientBootstrap(_)
                        | Eth2RpcRequest::LightClientFinalityUpdate
                        | Eth2RpcRequest::LightClientOptimisticUpdate => {
                            resource_unavailable("light-client data is not yet served by LogEx")
                        }
                    };
                    if let Err(response) = self.send_rpc_response(kind, channel, response) {
                        tracing::warn!(
                            %peer,
                            error = ?response,
                            "failed to send consensus RPC response"
                        );
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
                self.request_failures.increment(kind);
                let peer_failures = self.record_peer_failure(peer, kind);
                match kind {
                    RpcRequestKind::LightClientBootstrap
                    | RpcRequestKind::LightClientFinalityUpdate
                    | RpcRequestKind::LightClientOptimisticUpdate => {
                        tracing::info!(
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
                    self.disconnect_peer(peer);
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
                    self.drive_rpc_requests();
                }
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
                    let response = match request {
                        Eth2RpcRequest::MetaData => {
                            Eth2RpcResponse::MetaData(self.local_metadata())
                        }
                        _ => resource_unavailable("unsupported request on metadata RPC family"),
                    };
                    if let Err(response) = self
                        .swarm
                        .behaviour_mut()
                        .metadata_rpc
                        .inner
                        .send_response(channel, response)
                    {
                        tracing::warn!(
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
        self.reset_peer_failure(peer, kind);

        match (kind, response) {
            (RpcRequestKind::Status, Eth2RpcResponse::Status(status)) => {
                if status.fork_digest != self.fork_digest {
                    tracing::debug!(
                        %peer,
                        local = hex::encode(self.fork_digest),
                        remote = hex::encode(status.fork_digest),
                        "consensus peer replied with a different fork digest"
                    );
                }
                self.status_peers.insert(peer);
                self.drive_rpc_requests();
            }
            (RpcRequestKind::MetaData, Eth2RpcResponse::MetaData(metadata)) => {
                tracing::debug!(
                    %peer,
                    seq_number = metadata.seq_number,
                    "received consensus metadata response"
                );
                self.metadata_peers.insert(peer);
                self.drive_rpc_requests();
            }
            (RpcRequestKind::Ping, Eth2RpcResponse::Ping(seq_number)) => {
                tracing::debug!(%peer, seq_number, "received consensus ping response");
                self.ping_peers.insert(peer);
            }
            (
                RpcRequestKind::LightClientBootstrap,
                Eth2RpcResponse::LightClientBootstrap(payload),
            ) => match decode_bootstrap(&payload.bytes) {
                Ok(summary) => {
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
                    self.bootstrap_peers.insert(peer);
                    if let Err(error) = self.consensus.update_bootstrap_status(summary) {
                        tracing::warn!(%peer, %error, "failed to persist decoded bootstrap payload");
                    }
                    self.drive_rpc_requests();
                }
                Err(error) => {
                    tracing::warn!(
                        %peer,
                        bytes = payload.bytes.len(),
                        %error,
                        "failed to decode light-client bootstrap payload"
                    );
                }
            },
            (
                RpcRequestKind::LightClientFinalityUpdate,
                Eth2RpcResponse::LightClientFinalityUpdate(payload),
            ) => match decode_finality_update(&payload.bytes) {
                Ok(summary) => {
                    tracing::info!(
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
                    self.finality_update_peers.insert(peer);
                    if let Err(error) = self.consensus.update_finality_update_status(summary) {
                        tracing::warn!(%peer, %error, "failed to persist decoded finality update");
                    }
                    self.drive_rpc_requests();
                }
                Err(error) => {
                    tracing::warn!(
                        %peer,
                        bytes = payload.bytes.len(),
                        %error,
                        "failed to decode light-client finality update payload"
                    );
                }
            },
            (
                RpcRequestKind::LightClientOptimisticUpdate,
                Eth2RpcResponse::LightClientOptimisticUpdate(payload),
            ) => match decode_optimistic_update(&payload.bytes) {
                Ok(summary) => {
                    tracing::info!(
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
                    self.optimistic_update_peers.insert(peer);
                    if let Err(error) = self.consensus.update_optimistic_update_status(summary) {
                        tracing::warn!(%peer, %error, "failed to persist decoded optimistic update");
                    }
                    self.drive_rpc_requests();
                }
                Err(error) => {
                    tracing::warn!(
                        %peer,
                        bytes = payload.bytes.len(),
                        %error,
                        "failed to decode light-client optimistic update payload"
                    );
                }
            },
            (kind, Eth2RpcResponse::Error(error)) => {
                self.request_failures.increment(kind);
                let peer_failures = self.record_peer_failure(peer, kind);
                let message = String::from_utf8_lossy(&error.message);
                match kind {
                    RpcRequestKind::LightClientBootstrap
                    | RpcRequestKind::LightClientFinalityUpdate
                    | RpcRequestKind::LightClientOptimisticUpdate => {
                        tracing::info!(
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
                    self.disconnect_peer(peer);
                }
            }
            (kind, response) => {
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
        let mut dialable = self
            .dialable_peers
            .iter()
            .map(|(peer, addrs)| (*peer, addrs.clone()))
            .collect::<Vec<_>>();
        dialable.sort_by(|(left, _), (right, _)| left.to_string().cmp(&right.to_string()));

        let mut active_targets = self.connected_peers.len() + self.dialing_peers.len();
        for (peer, addrs) in dialable {
            if self.connected_peers.contains(&peer) || self.dialing_peers.contains(&peer) {
                continue;
            }
            if self.config.max_peers > 0 && active_targets >= self.config.max_peers {
                break;
            }
            self.ensure_connected(peer, addrs);
            active_targets += 1;
        }
    }

    fn drive_rpc_requests(&mut self) {
        let mut connected = self.connected_peers.iter().copied().collect::<Vec<_>>();
        connected.sort_by_key(|peer| peer.to_string());
        for peer in connected {
            if !self.status_peers.contains(&peer) {
                if self
                    .peer_support
                    .get(&peer)
                    .copied()
                    .map(|support| support.supports_request(RpcRequestKind::Status))
                    .unwrap_or(true)
                    && self.can_issue_request(RpcRequestKind::Status)
                {
                    self.ensure_request(peer, RpcRequestKind::Status);
                }
                continue;
            }

            if self
                .peer_support
                .get(&peer)
                .copied()
                .map(|support| support.supports_request(RpcRequestKind::MetaData))
                .unwrap_or(true)
                && self.can_issue_request(RpcRequestKind::MetaData)
            {
                self.ensure_request(peer, RpcRequestKind::MetaData);
            }

            let Some(support) = self.peer_support.get(&peer).copied() else {
                if self.can_issue_request(RpcRequestKind::Ping) {
                    self.ensure_request(peer, RpcRequestKind::Ping);
                }
                continue;
            };

            if support.supports_request(RpcRequestKind::LightClientFinalityUpdate)
                && self.can_issue_request(RpcRequestKind::LightClientFinalityUpdate)
            {
                self.ensure_request(peer, RpcRequestKind::LightClientFinalityUpdate);
            }
            if support.supports_request(RpcRequestKind::LightClientOptimisticUpdate)
                && self.can_issue_request(RpcRequestKind::LightClientOptimisticUpdate)
            {
                self.ensure_request(peer, RpcRequestKind::LightClientOptimisticUpdate);
            }
            if support.supports_request(RpcRequestKind::LightClientBootstrap)
                && self.can_issue_request(RpcRequestKind::LightClientBootstrap)
            {
                self.ensure_request(peer, RpcRequestKind::LightClientBootstrap);
            }
            if support.supports_request(RpcRequestKind::Ping)
                && self.can_issue_request(RpcRequestKind::Ping)
            {
                self.ensure_request(peer, RpcRequestKind::Ping);
            }
        }
    }

    fn send_rpc_response(
        &mut self,
        kind: RpcRequestKind,
        channel: request_response::ResponseChannel<Eth2RpcResponse>,
        response: Eth2RpcResponse,
    ) -> Result<(), Eth2RpcResponse> {
        match kind {
            RpcRequestKind::Status => self
                .swarm
                .behaviour_mut()
                .status_rpc
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
        }
    }

    fn ensure_connected(&mut self, peer: PeerId, addrs: Vec<Multiaddr>) {
        if addrs.is_empty() {
            return;
        }

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
            }
        }
    }

    fn ensure_request(&mut self, peer: PeerId, kind: RpcRequestKind) {
        if self.is_request_satisfied(peer, kind) || self.is_request_pending(peer, kind) {
            return;
        }

        let request = self.build_request(kind);
        let request_id = match kind {
            RpcRequestKind::Status => self
                .swarm
                .behaviour_mut()
                .status_rpc
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
        };
        self.pending_requests
            .insert(PendingRequestKey { kind, request_id }, peer);
        self.pending_peer_kinds.insert((peer, kind));
    }

    fn build_request(&self, kind: RpcRequestKind) -> Eth2RpcRequest {
        match kind {
            RpcRequestKind::Status => Eth2RpcRequest::Status(self.local_status_message()),
            RpcRequestKind::MetaData => Eth2RpcRequest::MetaData,
            RpcRequestKind::Ping => Eth2RpcRequest::Ping(0),
            RpcRequestKind::LightClientBootstrap => {
                Eth2RpcRequest::LightClientBootstrap(self.consensus.checkpoint().beacon_root)
            }
            RpcRequestKind::LightClientFinalityUpdate => Eth2RpcRequest::LightClientFinalityUpdate,
            RpcRequestKind::LightClientOptimisticUpdate => {
                Eth2RpcRequest::LightClientOptimisticUpdate
            }
        }
    }

    fn local_status_message(&self) -> StatusMessage {
        let checkpoint = self.consensus.checkpoint();
        match checkpoint.beacon_slot {
            Some(slot) => StatusMessage {
                fork_digest: self.fork_digest,
                finalized_root: checkpoint.beacon_root,
                finalized_epoch: slot / 32,
                head_root: checkpoint.beacon_root,
                head_slot: slot,
                earliest_available_slot: slot,
            },
            None => StatusMessage::genesis(self.fork_digest),
        }
    }

    fn local_metadata(&self) -> MetaData {
        MetaData::empty()
    }

    fn record_peer_failure(&mut self, peer: PeerId, kind: RpcRequestKind) -> u32 {
        self.peer_failures.entry(peer).or_default().increment(kind)
    }

    fn reset_peer_failure(&mut self, peer: PeerId, kind: RpcRequestKind) {
        if let Some(failures) = self.peer_failures.get_mut(&peer) {
            failures.reset(kind);
        }
    }

    fn disconnect_peer(&mut self, peer: PeerId) {
        let _ = self.swarm.disconnect_peer_id(peer);
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
        }
    }

    fn clear_peer_state(&mut self, peer: PeerId) {
        self.peer_support.remove(&peer);
        self.peer_failures.remove(&peer);
        self.status_peers.remove(&peer);
        self.metadata_peers.remove(&peer);
        self.ping_peers.remove(&peer);
        self.bootstrap_peers.remove(&peer);
        self.finality_update_peers.remove(&peer);
        self.optimistic_update_peers.remove(&peer);
    }

    fn is_request_pending(&self, peer: PeerId, kind: RpcRequestKind) -> bool {
        self.pending_peer_kinds.contains(&(peer, kind))
    }

    fn is_request_satisfied(&self, peer: PeerId, kind: RpcRequestKind) -> bool {
        match kind {
            RpcRequestKind::Status => self.status_peers.contains(&peer),
            RpcRequestKind::MetaData => self.metadata_peers.contains(&peer),
            RpcRequestKind::Ping => self.ping_peers.contains(&peer),
            RpcRequestKind::LightClientBootstrap => self.bootstrap_peers.contains(&peer),
            RpcRequestKind::LightClientFinalityUpdate => self.finality_update_peers.contains(&peer),
            RpcRequestKind::LightClientOptimisticUpdate => {
                self.optimistic_update_peers.contains(&peer)
            }
        }
    }

    fn pending_requests_for_kind(&self, kind: RpcRequestKind) -> usize {
        self.pending_requests
            .keys()
            .filter(|key| key.kind == kind)
            .count()
    }

    fn can_issue_request(&self, kind: RpcRequestKind) -> bool {
        self.pending_requests_for_kind(kind) < MAX_CONCURRENT_RPC_REQUESTS_PER_KIND
    }

    fn refresh_status(&self) {
        let table_entries = self.discv5.table_entries_enr();
        let light_client = self.consensus.light_client_status();
        let checkpoint = self.consensus.checkpoint();
        let identified_peers = self.peer_support.len();
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
            identified_peers,
            status_capable_peers,
            metadata_capable_peers,
            bootstrap_capable_peers,
            finality_update_capable_peers,
            optimistic_update_capable_peers,
            status_peers: self.status_peers.len(),
            metadata_peers: self.metadata_peers.len(),
            bootstrap_peers: self.bootstrap_peers.len(),
            finality_update_peers: self.finality_update_peers.len(),
            optimistic_update_peers: self.optimistic_update_peers.len(),
            pending_rpc_requests: self.pending_requests.len(),
            pending_status_requests: self.pending_requests_for_kind(RpcRequestKind::Status),
            pending_metadata_requests: self.pending_requests_for_kind(RpcRequestKind::MetaData),
            pending_bootstrap_requests: self
                .pending_requests_for_kind(RpcRequestKind::LightClientBootstrap),
            pending_finality_update_requests: self
                .pending_requests_for_kind(RpcRequestKind::LightClientFinalityUpdate),
            pending_optimistic_update_requests: self
                .pending_requests_for_kind(RpcRequestKind::LightClientOptimisticUpdate),
            status_request_failures: self.request_failures.status,
            metadata_request_failures: self.request_failures.metadata,
            bootstrap_request_failures: self.request_failures.bootstrap,
            finality_update_request_failures: self.request_failures.finality_update,
            optimistic_update_request_failures: self.request_failures.optimistic_update,
        };
        let mut sync_status = self.sync_status.lock().unwrap();
        sync_status.checkpoint = Some(checkpoint);
        sync_status.consensus_network = Some(status);
        sync_status.consensus_light_client = (!light_client.is_empty()).then_some(light_client);
    }

    fn persist_known_peers(&mut self) -> Result<(), ConsensusNetworkError> {
        let mut peers = self
            .discv5
            .table_entries_enr()
            .into_iter()
            .filter(|enr| enr.tcp4().is_some() || enr.tcp6().is_some())
            .map(|enr| PersistedPeer {
                enr: enr.to_base64(),
            })
            .collect::<Vec<_>>();

        if self.config.max_peers > 0 && peers.len() > self.config.max_peers {
            peers.truncate(self.config.max_peers);
        }
        peers.sort_by(|left, right| left.enr.cmp(&right.enr));

        if peers == self.last_persisted {
            return Ok(());
        }

        persist_known_peers(&self.known_peers_path, &peers)?;
        self.last_persisted = peers;
        Ok(())
    }
}

fn build_local_enr(
    enr_key: &CombinedKey,
    fork_id: &[u8],
    discovery_port: u16,
    p2p_port: u16,
) -> Enr {
    let mut builder = Enr::builder();
    builder
        .udp4(discovery_port)
        .tcp4(p2p_port)
        .add_value("eth2", &fork_id);
    builder
        .build(enr_key)
        .expect("local consensus ENR should always be constructible")
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
) -> Result<Swarm<ConsensusBehaviour>, ConsensusNetworkError> {
    let public_key = keypair.public();
    SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            tcp::Config::default().nodelay(true),
            noise::Config::new,
            (yamux::Config::default, mplex::Config::default),
        )
        .map_err(|error| ConsensusNetworkError::ConstructRpcTransport(error.to_string()))?
        .with_behaviour(move |_| {
            let identify = identify::Behaviour::new(
                identify::Config::new(IDENTIFY_PROTOCOL_VERSION.into(), public_key.clone())
                    .with_agent_version(IDENTIFY_AGENT_VERSION.to_owned())
                    .with_cache_size(0),
            );
            ConsensusBehaviour {
                identify,
                status_rpc: StatusRpcBehaviour {
                    inner: build_status_behaviour(),
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
                light_client_finality_update_rpc: LightClientFinalityUpdateRpcBehaviour {
                    inner: build_light_client_finality_update_behaviour(),
                },
                light_client_optimistic_update_rpc: LightClientOptimisticUpdateRpcBehaviour {
                    inner: build_light_client_optimistic_update_behaviour(),
                },
            }
        })
        .map_err(|error| ConsensusNetworkError::ConstructRpcTransport(error.to_string()))
        .map(|builder| builder.build())
}

fn discovery_secret_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join(CONSENSUS_STATE_DIR)
        .join(DISCOVERY_SECRET_FILE)
}

fn known_peers_path(data_dir: &Path) -> PathBuf {
    data_dir.join(CONSENSUS_STATE_DIR).join(KNOWN_PEERS_FILE)
}

fn observe_dialable_peer(peers: &mut HashMap<PeerId, Vec<Multiaddr>>, enr: &Enr) {
    if let Some((peer_id, addrs)) = enr_multiaddrs(enr) {
        peers.insert(peer_id, addrs);
    }
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

#[allow(deprecated)]
fn current_eth2_fork_id(bootnodes: &[Enr]) -> Result<Vec<u8>, ConsensusNetworkError> {
    bootnodes
        .iter()
        .find_map(|enr| enr.get("eth2").map(|bytes| bytes.to_vec()))
        .ok_or(ConsensusNetworkError::MissingBootnodeForkId)
}

fn current_fork_digest(fork_id: &[u8]) -> Result<[u8; 4], ConsensusNetworkError> {
    fork_id
        .get(0..4)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(ConsensusNetworkError::InvalidBootnodeForkId)
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
    let public_key = identity::secp256k1::PublicKey::try_from_bytes(&enr.public_key().encode())
        .map_err(|error| error.to_string())?;
    Ok(identity::PublicKey::from(public_key).to_peer_id())
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

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

const MAINNET_BOOTNODES: &[&str] = &[
    "enr:-KG4QNTx85fjxABbSq_Rta9wy56nQ1fHK0PewJbGjLm1M4bMGx5-3Qq4ZX2-iFJ0pys_O90sVXNNOxp2E7afBsGsBrgDhGV0aDKQu6TalgMAAAD__________4JpZIJ2NIJpcIQEnfA2iXNlY3AyNTZrMaECGXWQ-rQ2KZKRH1aOW4IlPDBkY4XDphxg9pxKytFCkayDdGNwgiMog3VkcIIjKA",
    "enr:-KG4QF4B5WrlFcRhUU6dZETwY5ZzAXnA0vGC__L1Kdw602nDZwXSTs5RFXFIFUnbQJmhNGVU6OIX7KVrCSTODsz1tK4DhGV0aDKQu6TalgMAAAD__________4JpZIJ2NIJpcIQExNYEiXNlY3AyNTZrMaECQmM9vp7KhaXhI-nqL_R0ovULLCFSFTa9CPPSdb1zPX6DdGNwgiMog3VkcIIjKA",
    "enr:-Ku4QImhMc1z8yCiNJ1TyUxdcfNucje3BGwEHzodEZUan8PherEo4sF7pPHPSIB1NNuSg5fZy7qFsjmUKs2ea1Whi0EBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQOVphkDqal4QzPMksc5wnpuC3gvSC8AfbFOnZY_On34wIN1ZHCCIyg",
    "enr:-Ku4QP2xDnEtUXIjzJ_DhlCRN9SN99RYQPJL92TMlSv7U5C1YnYLjwOQHgZIUXw6c-BvRg2Yc2QsZxxoS_pPRVe0yK8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMeFF5GrS7UZpAH2Ly84aLK-TyvH-dRo0JM1i8yygH50YN1ZHCCJxA",
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
];

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::StreamProtocol;
    use tempfile::TempDir;

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
            },
            PersistedPeer {
                enr: MAINNET_BOOTNODES[1].to_string(),
            },
        ];

        persist_known_peers(&path, &peers).unwrap();
        let loaded = load_known_peers(&path).unwrap();

        assert_eq!(loaded, peers);
    }

    #[test]
    fn bundled_bootnodes_parse() {
        let bootnodes = mainnet_bootnodes().unwrap();

        assert!(bootnodes.len() >= 10);
        assert!(bootnodes.iter().all(|enr| enr.udp4().is_some()));
        assert_eq!(current_eth2_fork_id(&bootnodes).unwrap().len(), 16);
        assert_eq!(
            current_fork_digest(&current_eth2_fork_id(&bootnodes).unwrap())
                .unwrap()
                .len(),
            4
        );
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
    fn identify_support_recognizes_light_client_protocols() {
        let info = identify::Info {
            public_key: identity::Keypair::generate_ed25519().public(),
            protocol_version: IDENTIFY_PROTOCOL_VERSION.to_owned(),
            agent_version: IDENTIFY_AGENT_VERSION.to_owned(),
            listen_addrs: vec![],
            protocols: vec![
                StreamProtocol::new(STATUS_V2_PROTOCOL_ID),
                StreamProtocol::new(METADATA_V1_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID),
                StreamProtocol::new(LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID),
            ],
            observed_addr: Multiaddr::empty(),
            signed_peer_record: None,
        };

        let support = PeerRpcSupport::from_identify_info(&info);
        assert!(support.status);
        assert!(support.metadata);
        assert!(support.supports_light_client());
        assert!(support.supports_request(RpcRequestKind::MetaData));
        assert!(support.supports_request(RpcRequestKind::LightClientBootstrap));
        assert!(support.supports_request(RpcRequestKind::LightClientFinalityUpdate));
        assert!(support.supports_request(RpcRequestKind::LightClientOptimisticUpdate));
    }

    #[test]
    fn identify_support_rejects_partial_light_client_protocol_sets() {
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
        assert!(!support.supports_light_client());
        assert!(support.supports_request(RpcRequestKind::LightClientFinalityUpdate));
        assert!(!support.supports_request(RpcRequestKind::LightClientBootstrap));
    }
}
