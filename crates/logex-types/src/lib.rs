mod consensus;
mod log_row;
mod partition;
mod sync;

pub use consensus::{
    ChainAnchors, ConsensusDataFork, ConsensusLightClientStatus, ExecutionAnchor,
    ExecutionBlockMarker, LightClientBootstrapStatus, LightClientExecutionData,
    LightClientFinalityUpdateStatus, LightClientHeaderSummary, LightClientOptimisticUpdateStatus,
    WeakSubjectivityCheckpoint,
};
pub use log_row::{BlockContext, LogRow, Source};
pub use partition::PartitionMeta;
pub use sync::{ConsensusNetworkStatus, ExecutionNetworkStatus, NodeState, SyncStatus};

/// Canonical LogEx client version string used across JSON-RPC and devp2p.
pub const LOGEX_CLIENT_VERSION: &str = concat!("LogEx/v", env!("CARGO_PKG_VERSION"));

/// Terminal target for execution-layer reverse validation on Ethereum mainnet.
pub const EXECUTION_HISTORY_TARGET_BLOCK: u64 = 0;

/// First Ethereum mainnet proof-of-stake execution block.
///
/// Reverse EL sync still walks below this block to genesis. For pre-Merge
/// blocks, canonicality comes from the finalized post-Merge execution header's
/// recursive parent-hash ancestry; logs are then checked against each ancestor
/// header's receipt root.
pub const EXECUTION_MERGE_BLOCK: u64 = 15_537_394;

/// Final Ethereum mainnet proof-of-work execution block.
pub const EXECUTION_TERMINAL_POW_BLOCK: u64 = EXECUTION_MERGE_BLOCK - 1;
