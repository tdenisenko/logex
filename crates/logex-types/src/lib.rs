mod log_row;
mod partition;
mod sync;

pub use log_row::{BlockContext, LogRow, Source};
pub use partition::PartitionMeta;
pub use sync::{NodeState, SyncStatus};

/// Canonical LogEx client version string used across JSON-RPC and devp2p.
pub const LOGEX_CLIENT_VERSION: &str = concat!("LogEx/v", env!("CARGO_PKG_VERSION"));
