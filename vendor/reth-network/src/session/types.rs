//! Shared types for network sessions.
// LogEx patch: publish and read complete range tuples under one lock.

use alloy_primitives::B256;
use parking_lot::RwLock;
use reth_eth_wire::BlockRangeUpdate;
use std::{ops::RangeInclusive, sync::Arc};

/// Information about the range of full blocks available from a peer.
///
/// This represents the announced `eth69` [`BlockRangeUpdate`] of a peer.
#[derive(Debug, Clone)]
pub struct BlockRangeInfo {
    /// Range numbers and hash belong to the same publication.
    inner: Arc<RwLock<BlockRangeUpdate>>,
}

impl BlockRangeInfo {
    /// Creates a new range information.
    pub fn new(earliest: u64, latest: u64, latest_hash: B256) -> Self {
        Self { inner: Arc::new(RwLock::new(BlockRangeUpdate { earliest, latest, latest_hash })) }
    }

    /// Returns true if the block number is within the range of blocks available from the peer.
    pub fn contains(&self, block_number: u64) -> bool {
        self.range().contains(&block_number)
    }

    /// Returns the range of blocks available from the peer.
    pub fn range(&self) -> RangeInclusive<u64> {
        let snapshot = self.inner.read();
        snapshot.earliest..=snapshot.latest
    }

    /// Returns the earliest full block number available from the peer.
    pub fn earliest(&self) -> u64 {
        self.inner.read().earliest
    }

    /// Returns the latest full block number available from the peer.
    pub fn latest(&self) -> u64 {
        self.inner.read().latest
    }

    /// Returns the latest block hash available from the peer.
    pub fn latest_hash(&self) -> B256 {
        self.inner.read().latest_hash
    }

    /// Returns true if the peer has the full history available.
    pub fn has_full_history(&self) -> bool {
        self.earliest() == 0
    }

    /// Updates the range information as one publication.
    pub fn update(&self, earliest: u64, latest: u64, latest_hash: B256) {
        *self.inner.write() = BlockRangeUpdate { earliest, latest, latest_hash };
    }

    /// Returns a coherent Eth69 [`BlockRangeUpdate`] snapshot.
    pub fn to_message(&self) -> BlockRangeUpdate {
        self.inner.read().clone()
    }
}
