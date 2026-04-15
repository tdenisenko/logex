use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

/// Persisted sync head metadata. This advances even for blocks that contain
/// zero logs so the node can resume sync and report head height correctly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncHead {
    pub block_number: u64,
    pub block_hash: B256,
    #[serde(default)]
    pub timestamp: u64,
}
