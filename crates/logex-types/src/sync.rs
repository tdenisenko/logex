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
            Self::Synced => "Synced",
            Self::Disconnected => "Disconnected",
            Self::Reconnecting => "Reconnecting",
        }
    }
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
    /// Number of peers that have successfully answered sync requests.
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
}
