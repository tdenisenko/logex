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

/// Execution block marker that does not imply direct CL authentication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionBlockMarker {
    pub block_number: u64,
    pub block_hash: B256,
    pub timestamp: u64,
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

/// Post-merge consensus fork used by a decoded light-client payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ConsensusDataFork {
    #[default]
    Capella,
    Deneb,
    Electra,
}

/// Execution payload fields surfaced from a decoded CL light-client header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LightClientExecutionData {
    pub block_number: u64,
    pub block_hash: B256,
    pub receipts_root: B256,
}

/// CL light-client header summary surfaced in status and persisted consensus state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct LightClientHeaderSummary {
    pub beacon_slot: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<LightClientExecutionData>,
}

/// Persisted summary of a decoded light-client bootstrap.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LightClientBootstrapStatus {
    pub fork: ConsensusDataFork,
    pub header: LightClientHeaderSummary,
    pub current_sync_committee_pubkeys: usize,
    pub current_sync_committee_branch_depth: usize,
}

/// Persisted summary of a decoded light-client finality update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LightClientFinalityUpdateStatus {
    pub fork: ConsensusDataFork,
    pub attested_header: LightClientHeaderSummary,
    pub finalized_header: LightClientHeaderSummary,
    pub signature_slot: u64,
    pub sync_committee_participants: usize,
    pub finality_branch_depth: usize,
}

/// Persisted summary of a decoded light-client optimistic update.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LightClientOptimisticUpdateStatus {
    pub fork: ConsensusDataFork,
    pub attested_header: LightClientHeaderSummary,
    pub signature_slot: u64,
    pub sync_committee_participants: usize,
}

/// Persisted CL light-client payload summaries learned from native consensus peers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConsensusLightClientStatus {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<LightClientBootstrapStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finality_update: Option<LightClientFinalityUpdateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimistic_update: Option<LightClientOptimisticUpdateStatus>,
}

impl ConsensusLightClientStatus {
    pub const fn is_empty(&self) -> bool {
        self.bootstrap.is_none()
            && self.finality_update.is_none()
            && self.optimistic_update.is_none()
    }
}
