use crate::{ConsensusLightClientStatus, ExecutionAnchor, WeakSubjectivityCheckpoint};
use serde::Serialize;

/// High-level runtime state for the node.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    #[default]
    Starting,
    Discovering,
    Connecting,
    Syncing,
    WaitingForConsensus,
    Synced,
    Disconnected,
    Reconnecting,
}

impl NodeState {
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Starting => "Starting",
            Self::Discovering => "Discovering",
            Self::Connecting => "Connecting",
            Self::Syncing => "Syncing",
            Self::WaitingForConsensus => "Waiting For Consensus",
            Self::Synced => "Synced",
            Self::Disconnected => "Disconnected",
            Self::Reconnecting => "Reconnecting",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ConsensusNetworkStatus {
    /// Base64-encoded local ENR advertised on the consensus network.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_enr: Option<String>,
    /// Local discovery node id, derived from the ENR public key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_node_id: Option<String>,
    /// UDP discovery port bound for discv5.
    pub discovery_port: u16,
    /// TCP libp2p port advertised in the ENR for future req/resp and gossip sessions.
    pub p2p_port: u16,
    /// Local libp2p peer id derived from the consensus networking key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_peer_id: Option<String>,
    /// Maximum retained dialable consensus peers.
    pub max_peers: usize,
    /// Number of configured bootnodes.
    pub bootnode_count: usize,
    /// Number of unique peers observed by discovery in this run.
    pub discovered_peers: usize,
    /// Number of discovered peers that advertise a TCP port and are eligible as dial candidates.
    pub dialable_peers: usize,
    /// Number of ENRs currently stored in the discovery routing table.
    pub routing_table_peers: usize,
    /// Number of active UDP discovery sessions currently established.
    pub active_sessions: usize,
    /// Number of libp2p consensus peers currently connected over TCP.
    pub connected_peer_sessions: usize,
    /// Number of connected peers for which identify information has been recorded.
    pub identified_peers: usize,
    /// Number of identified peers that advertise the Status RPC.
    pub status_capable_peers: usize,
    /// Number of identified peers that advertise the MetaData RPC.
    pub metadata_capable_peers: usize,
    /// Number of identified peers that advertise the light-client bootstrap RPC.
    pub bootstrap_capable_peers: usize,
    /// Number of identified peers that advertise the light-client finality update RPC.
    pub finality_update_capable_peers: usize,
    /// Number of identified peers that advertise the light-client optimistic update RPC.
    pub optimistic_update_capable_peers: usize,
    /// Number of peers that answered the initial Status req/resp handshake.
    pub status_peers: usize,
    /// Number of peers that answered the MetaData req/resp handshake.
    pub metadata_peers: usize,
    /// Number of peers that served a light-client bootstrap payload.
    pub bootstrap_peers: usize,
    /// Number of peers that served a light-client finality update payload.
    pub finality_update_peers: usize,
    /// Number of peers that served a light-client optimistic update payload.
    pub optimistic_update_peers: usize,
    /// Number of outbound light-client RPC requests currently in flight.
    pub pending_rpc_requests: usize,
    /// Number of in-flight Status requests.
    pub pending_status_requests: usize,
    /// Number of in-flight MetaData requests.
    pub pending_metadata_requests: usize,
    /// Number of in-flight light-client bootstrap requests.
    pub pending_bootstrap_requests: usize,
    /// Number of in-flight light-client finality-update requests.
    pub pending_finality_update_requests: usize,
    /// Number of in-flight light-client optimistic-update requests.
    pub pending_optimistic_update_requests: usize,
    /// Number of failed Status request attempts since startup.
    pub status_request_failures: u64,
    /// Number of failed MetaData request attempts since startup.
    pub metadata_request_failures: u64,
    /// Number of failed light-client bootstrap request attempts since startup.
    pub bootstrap_request_failures: u64,
    /// Number of failed light-client finality-update request attempts since startup.
    pub finality_update_request_failures: u64,
    /// Number of failed light-client optimistic-update request attempts since startup.
    pub optimistic_update_request_failures: u64,
    /// Number of active CL gossipsub topic subscriptions.
    pub gossip_subscriptions: usize,
    /// Number of light-client finality-update gossip messages observed since startup.
    pub finality_update_gossip_messages: u64,
    /// Number of light-client optimistic-update gossip messages observed since startup.
    pub optimistic_update_gossip_messages: u64,
    /// Number of malformed or undecodable light-client gossip payloads observed since startup.
    pub gossip_decode_failures: u64,
}

/// Live sync progress, updated by the sync task, read by HTTP endpoints.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncStatus {
    /// High-level runtime state shown in the UI and status endpoint.
    pub node_state: NodeState,
    /// Whether the node is actively syncing blocks.
    pub syncing: bool,
    /// Number of currently connected peers.
    pub connected_peers: usize,
    /// Number of connected peers that have successfully answered validated
    /// sync requests such as headers, bodies, or receipts.
    pub serving_peers: usize,
    /// Number of pending peer candidates waiting to be dialed.
    pub pending_peers: usize,
    /// The highest block number ingested so far.
    pub current_block: u64,
    /// The latest known block on the network.
    pub target_block: u64,
    /// Current sync throughput.
    pub blocks_per_sec: f64,
    /// Average sync throughput expressed in blocks per minute.
    pub blocks_per_minute: f64,
    /// Total logs ingested since the node started.
    pub logs_ingested: u64,
    /// Estimated seconds remaining to reach target.
    pub eta_seconds: Option<f64>,
    /// Weak-subjectivity checkpoint the node bootstrapped from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<WeakSubjectivityCheckpoint>,
    /// Highest indexed execution anchor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_execution_head: Option<ExecutionAnchor>,
    /// Highest optimistic execution anchor known from CL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optimistic_execution_head: Option<ExecutionAnchor>,
    /// Highest finalized execution anchor known from CL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalized_execution_head: Option<ExecutionAnchor>,
    /// Native consensus-network discovery state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consensus_network: Option<ConsensusNetworkStatus>,
    /// Decoded native CL light-client payload summaries learned from peers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consensus_light_client: Option<ConsensusLightClientStatus>,
}
