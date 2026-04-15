use alloy_primitives::B256;
use serde::{Deserialize, Serialize};

/// Weak-subjectivity checkpoint used to bootstrap the consensus view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeakSubjectivityCheckpoint {
    pub beacon_root: B256,
    #[serde(default)]
    pub beacon_slot: Option<u64>,
}

/// CL-authenticated execution anchor that LogEx can use to verify EL data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAnchor {
    pub beacon_root: B256,
    pub beacon_slot: u64,
    pub block_number: u64,
    pub block_hash: B256,
    pub receipts_root: B256,
}

/// Persisted execution-facing anchors exposed to the rest of the node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ChainAnchors {
    #[serde(default)]
    pub indexed_head: Option<ExecutionAnchor>,
    #[serde(default)]
    pub finalized_head: Option<ExecutionAnchor>,
    #[serde(default)]
    pub optimistic_head: Option<ExecutionAnchor>,
}
