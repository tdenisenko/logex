pub mod engine;
pub mod head_tracker;
pub mod p2p;
pub mod progress;
pub mod validation;

use alloy_primitives::{B256, Log};

/// Configuration for the sync engine.
pub struct SyncConfig {
    /// Trusted checkpoint: (block_number, block_hash).
    /// The sync engine trusts this anchor and validates the header chain
    /// from it to the network tip via parent_hash linkage.
    /// If None, syncs from genesis.
    pub checkpoint: Option<(u64, B256)>,

    /// Maximum concurrent peer connections.
    pub max_peers: usize,

    /// Headers to request per GetBlockHeaders message.
    pub header_batch_size: u64,

    /// Blocks to request receipts/bodies for per batch.
    pub fetch_batch_size: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            checkpoint: None,
            max_peers: 50,
            header_batch_size: 1024,
            fetch_batch_size: 128,
        }
    }
}

/// A fetched block with all data needed for ingestion.
///
/// This is the output of the P2P layer: headers provide block metadata,
/// bodies provide tx hashes, and receipts provide logs. The receipt root
/// has already been validated against the header before constructing this.
pub struct FetchedBlock {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: u64,
    pub receipts_root: B256,
    /// (tx_hash, logs) pairs — assembled by zipping body tx hashes with receipt logs.
    pub txs: Vec<(B256, Vec<Log>)>,
}
