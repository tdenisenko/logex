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
