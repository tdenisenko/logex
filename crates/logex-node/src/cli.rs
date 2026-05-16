use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "logex",
    version = env!("CARGO_PKG_VERSION"),
    about = "LogEx — standalone Ethereum light node for fast event log queries"
)]
pub struct Cli {
    /// Path to the LogEx data directory. Defaults to the OS application data directory.
    #[arg(long, value_name = "PATH", global = true)]
    pub data_dir: Option<PathBuf>,

    /// Tracing filter (for example: info, debug, or info,discv5=error).
    #[arg(long, default_value = "info", global = true)]
    pub log_level: String,

    /// Target rows per partition before sealing.
    #[arg(long, default_value = "1000000", global = true)]
    pub partition_target_rows: u64,

    /// Path to optional TOML config file.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Weak-subjectivity checkpoint root or descriptor file path.
    #[arg(long, global = true)]
    pub checkpoint: Option<String>,

    /// Trusted Beacon API/checkpoint-sync URL used to fetch or validate a recent finalized checkpoint.
    #[arg(long, global = true)]
    pub checkpoint_sync_url: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

pub fn default_data_dir() -> PathBuf {
    platform_data_dir().join(platform_app_dir_name())
}

#[cfg(target_os = "windows")]
fn platform_data_dir() -> PathBuf {
    std::env::var_os("APPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("USERPROFILE")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join("AppData").join("Roaming"))
        })
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(target_os = "macos")]
fn platform_data_dir() -> PathBuf {
    std::env::var_os("HOME")
        .filter(|value| !value.is_empty())
        .map(|home| {
            PathBuf::from(home)
                .join("Library")
                .join("Application Support")
        })
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn platform_data_dir() -> PathBuf {
    std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".local").join("share"))
        })
        .unwrap_or_else(|| PathBuf::from("."))
}

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn platform_app_dir_name() -> &'static str {
    "LogEx"
}

#[cfg(all(unix, not(target_os = "macos")))]
fn platform_app_dir_name() -> &'static str {
    "logex"
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
        #[arg(long, default_value = "100")]
        max_peers: usize,

        /// EL NAT/external address resolver advertised to peers: any, none, publicip, netif, extip:<ip>, or extaddr:<domain>.
        #[arg(long, default_value = "any")]
        nat: String,

        /// Consensus-layer discv5 discovery port (UDP).
        #[arg(long, default_value = "9000")]
        cl_discovery_port: u16,

        /// Consensus-layer libp2p port advertised in the local ENR.
        #[arg(long, default_value = "9000")]
        cl_p2p_port: u16,

        /// Maximum dialable CL peers to retain from discovery.
        #[arg(long, default_value = "32")]
        cl_max_peers: usize,

        /// Disable the embedded HTTP dashboard. Query APIs remain available.
        #[arg(long)]
        disable_dashboard: bool,

        /// Require HTTP Basic authentication for dashboard, status, query, JSON-RPC, and WebSocket endpoints.
        #[arg(long, value_name = "PASSWORD")]
        dashboard_password: Option<String>,
    },

    /// Build or rebuild query indexes.
    BuildIndexes {
        /// Include sealed historical segments.
        #[arg(long)]
        sealed: bool,

        /// Include the active hot segment. When neither --hot nor --sealed is set, hot is implied.
        #[arg(long)]
        hot: bool,

        /// Index profile to build.
        #[arg(long, value_enum, default_value = "all")]
        profile: IndexProfile,

        /// Skip segments that already have every index required by the selected profile.
        #[arg(long)]
        missing_only: bool,

        /// Maximum number of matching segments to index.
        #[arg(long)]
        limit: Option<usize>,

        /// Only index segments whose block range overlaps this lower bound.
        #[arg(long)]
        from_block: Option<u64>,

        /// Only index segments whose block range overlaps this upper bound.
        #[arg(long)]
        to_block: Option<u64>,

        /// Only index segments whose timestamp range overlaps this lower bound.
        #[arg(long)]
        from_timestamp: Option<u64>,

        /// Only index segments whose timestamp range overlaps this upper bound.
        #[arg(long)]
        to_timestamp: Option<u64>,
    },

    /// Compact sealed storage segments.
    Compact {
        /// Maximum number of eligible sealed segments to compact.
        #[arg(long)]
        limit: Option<usize>,
    },

    /// Show storage statistics.
    Info,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum IndexProfile {
    All,
    LogQuery,
    Erc20Transfer,
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
    #[serde(default)]
    pub checkpoint_sync_url: Option<String>,
    #[serde(default)]
    pub nat: Option<String>,
    #[serde(default)]
    pub dashboard_enabled: Option<bool>,
    #[serde(default)]
    pub dashboard_password: Option<String>,
}

impl Config {
    pub fn load(path: &PathBuf) -> Result<Self, String> {
        let contents =
            std::fs::read_to_string(path).map_err(|e| format!("failed to read config: {e}"))?;
        toml::from_str(&contents).map_err(|e| format!("failed to parse config: {e}"))
    }
}
