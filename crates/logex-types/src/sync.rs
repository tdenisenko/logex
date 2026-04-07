use serde::Serialize;

/// Live sync progress, updated by the sync task, read by HTTP endpoints.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncStatus {
    /// Whether the node is actively syncing blocks.
    pub syncing: bool,
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
