use std::path::PathBuf;

use clap::{Parser, Subcommand};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "logex",
    version = env!("CARGO_PKG_VERSION"),
    about = "LogEx — standalone Ethereum light node for fast event log queries"
)]
pub struct Cli {
    /// Path to the LogEx data directory.
    #[arg(long, default_value = "./logex-data", global = true)]
    pub data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info", global = true)]
    pub log_level: String,

    /// Target rows per partition before sealing.
    #[arg(long, default_value = "50000000", global = true)]
    pub partition_target_rows: u64,

    /// Path to optional TOML config file.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Weak-subjectivity checkpoint root or descriptor file path.
    #[arg(long, global = true)]
    pub checkpoint: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Start the node: sync blocks from the P2P network and serve queries.
    Sync {
        /// HTTP server port (Web UI + SQL + JSON-RPC).
        #[arg(long, default_value = "8577")]
        http_port: u16,

        /// gRPC server port.
        #[arg(long, default_value = "8578")]
        grpc_port: u16,

        /// P2P discovery port (UDP).
        #[arg(long, default_value = "30303")]
        discovery_port: u16,

        /// P2P listener port (TCP).
        #[arg(long, default_value = "30303")]
        p2p_port: u16,

        /// Maximum peer connections.
        #[arg(long, default_value = "50")]
        max_peers: usize,

        /// Consensus-layer discv5 discovery port (UDP).
        #[arg(long, default_value = "9000")]
        cl_discovery_port: u16,

        /// Consensus-layer libp2p port advertised in the local ENR.
        #[arg(long, default_value = "9000")]
        cl_p2p_port: u16,

        /// Maximum dialable CL peers to retain from discovery.
        #[arg(long, default_value = "32")]
        cl_max_peers: usize,
    },

    /// Build or rebuild indexes on the hot partition.
    BuildIndexes,

    /// Show storage statistics.
    Info,
}

/// TOML config file structure.
#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    #[serde(default)]
    pub log_level: Option<String>,
    #[serde(default)]
    pub partition_target_rows: Option<u64>,
    #[serde(default)]
    pub checkpoint: Option<String>,
}

impl Config {
    pub fn load(path: &PathBuf) -> Result<Self, String> {
        let contents =
            std::fs::read_to_string(path).map_err(|e| format!("failed to read config: {e}"))?;
        toml::from_str(&contents).map_err(|e| format!("failed to parse config: {e}"))
    }
}
