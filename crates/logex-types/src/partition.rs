use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Metadata for a single partition. Stored as `metadata.json` inside each
/// partition directory and also held in memory by the `PartitionManager`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    /// Unique partition identifier (sequential).
    pub id: u64,
    /// Lowest block number in this partition.
    pub min_block: u64,
    /// Highest block number in this partition.
    pub max_block: u64,
    /// Lowest block timestamp in this partition, when known.
    #[serde(default)]
    pub min_timestamp: Option<u64>,
    /// Highest block timestamp in this partition, when known.
    #[serde(default)]
    pub max_timestamp: Option<u64>,
    /// Total number of log rows in this partition.
    pub row_count: u64,
    /// Whether this partition has been sealed (immutable).
    pub sealed: bool,
    /// Path to the partition directory on disk.
    pub path: PathBuf,
}
