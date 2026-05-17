pub mod engine;
mod extract;
pub mod head_tracker;
pub mod p2p;
pub mod primitives;
pub mod progress;
pub mod validation;

pub use logex_types::EXECUTION_HISTORY_TARGET_BLOCK;

/// Configuration for the sync engine.
pub struct SyncConfig {
    /// Maximum concurrent peer connections.
    pub max_peers: usize,

    /// Disable reverse historical execution sync and only follow verified CL anchors forward.
    pub disable_historical_sync: bool,

    /// Headers to request per GetBlockHeaders message.
    pub header_batch_size: u64,

    /// Blocks to request receipts/bodies for per batch.
    pub fetch_batch_size: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            max_peers: 100,
            disable_historical_sync: false,
            header_batch_size: 1024,
            fetch_batch_size: 1024,
        }
    }
}
