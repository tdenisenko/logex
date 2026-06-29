use std::net::IpAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand, ValueEnum};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "logex",
    version = env!("CARGO_PKG_VERSION"),
    about = "Standalone Ethereum log-verification client and query server",
    long_about = "LogEx joins the Ethereum consensus-layer and execution-layer P2P networks, verifies receipt logs, stores them locally, and serves SQL, JSON-RPC, gRPC, WebSocket, and dashboard query APIs.",
    after_help = "Examples:
  logex sync
  logex --data-dir /var/lib/logex/mainnet --config /etc/logex/config.toml sync --http-host 0.0.0.0 --dashboard-password '<password>'
  logex --data-dir /var/lib/logex/mainnet build-indexes --sealed --missing-only --profile erc20-transfer --jobs 4
  logex --data-dir /var/lib/logex/mainnet info"
)]
pub struct Cli {
    /// Path to the LogEx data directory.
    ///
    /// Defaults to the OS application data directory:
    /// macOS: ~/Library/Application Support/LogEx
    /// Linux: $XDG_DATA_HOME/logex or ~/.local/share/logex
    /// Windows: %APPDATA%\LogEx
    #[arg(long, value_name = "PATH", global = true)]
    pub data_dir: Option<PathBuf>,

    /// Tracing filter.
    ///
    /// Examples: info, debug, info,logex_sync=debug,discv5=error.
    /// The default "info" filter suppresses noisy discovery warnings.
    #[arg(long, default_value = "info", global = true)]
    pub log_level: String,

    /// Target log rows per storage segment before sealing and compacting it.
    ///
    /// Larger values reduce segment count; smaller values seal sooner and can
    /// make recently written data immutable earlier.
    #[arg(long, default_value = "1000000", global = true)]
    pub partition_target_rows: u64,

    /// Path to an optional TOML config file.
    ///
    /// Supported keys: data_dir, log_level, partition_target_rows, checkpoint,
    /// checkpoint_sync_url, nat, p2p_bind_ip, execution_bootnodes, execution_discv5_port,
    /// http_host, grpc_host, allow_public_grpc, dashboard_enabled, dashboard_password.
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,

    /// Weak-subjectivity checkpoint root, slot@root pair, or descriptor file path.
    ///
    /// When supplied, the checkpoint is validated against --checkpoint-sync-url
    /// and must be recent. When omitted for a fresh data directory, LogEx
    /// resolves a recent finalized checkpoint from --checkpoint-sync-url.
    #[arg(long, global = true)]
    pub checkpoint: Option<String>,

    /// Trusted Beacon API/checkpoint-sync URL used to fetch or validate a recent finalized checkpoint.
    ///
    /// Defaults to a 2-of-3 mainnet checkpoint quorum for sync. Use
    /// comma-separated URLs to override the default sources and require
    /// multi-source checkpoint agreement.
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
    #[command(after_help = "Examples:
  logex sync
  logex --data-dir /var/lib/logex/mainnet --checkpoint <slot@root> sync --http-port 18683
  logex --config /etc/logex/config.toml sync --http-host 0.0.0.0 --dashboard-password '<password>'

Security:
  HTTP binds to 127.0.0.1 by default. Public HTTP requires --dashboard-password.
  gRPC binds to 127.0.0.1 by default. Public gRPC requires --allow-public-grpc.")]
    Sync {
        /// HTTP server host for dashboard, status, SQL, JSON-RPC, and WebSocket APIs.
        ///
        /// Defaults to loopback. Use 0.0.0.0 only when the dashboard/API is
        /// protected with --dashboard-password and the host is firewalled or
        /// behind a trusted proxy.
        #[arg(long, default_value = "127.0.0.1")]
        http_host: IpAddr,

        /// HTTP server port for dashboard, status, SQL, JSON-RPC, and WebSocket APIs.
        #[arg(long, default_value = "8577")]
        http_port: u16,

        /// gRPC server host.
        ///
        /// Defaults to loopback. gRPC is unauthenticated; public gRPC listeners
        /// require --allow-public-grpc and should be exposed only on trusted
        /// networks.
        #[arg(long, default_value = "127.0.0.1")]
        grpc_host: IpAddr,

        /// gRPC server port for LogExService.Query, GetLogs, StreamLogs, and GetHeadBlock.
        #[arg(long, default_value = "8578")]
        grpc_port: u16,

        /// Execution-layer discovery port (UDP discv4).
        #[arg(long, default_value = "30303")]
        discovery_port: u16,

        /// Execution-layer P2P listener port (TCP eth protocol).
        #[arg(long, default_value = "30303")]
        p2p_port: u16,

        /// Maximum execution-layer peer sessions.
        ///
        /// Higher values can improve throughput only when CPU, memory,
        /// bandwidth, and disk can keep up.
        #[arg(long, default_value = "100")]
        max_peers: usize,

        /// Execution-layer NAT/external address resolver advertised to peers.
        ///
        /// The default "any" auto-selects a locally owned public IPv4 address,
        /// then a locally owned public IPv6 address, and otherwise runs
        /// outbound-only without advertising a public address. Accepted explicit
        /// values include: none, publicip, netif, extip:<ip>, extaddr:<domain>.
        /// On public servers, extip:<ip> is usually the most deterministic
        /// choice.
        #[arg(long, default_value = "any")]
        nat: String,

        /// Local IP address used by execution and consensus P2P listeners.
        ///
        /// By default, LogEx uses automatic address-family selection. Use "::"
        /// with --nat extip:<ipv6> to force IPv6-only P2P.
        #[arg(long, value_name = "IP")]
        p2p_bind_ip: Option<IpAddr>,

        /// Execution-layer enode bootnode to seed discovery and direct dials.
        ///
        /// May be repeated or comma-separated. IPv6 enodes must use the standard
        /// bracketed form, for example enode://<pubkey>@[2001:db8::1]:30303?discport=30303.
        #[arg(
            long = "execution-bootnode",
            value_name = "ENODE",
            value_delimiter = ','
        )]
        execution_bootnodes: Vec<String>,

        /// Execution-layer discovery v5 UDP port.
        ///
        /// Used for IPv6 execution peer discovery in addition to discv4. The
        /// default matches Reth's execution discv5 default.
        #[arg(long = "execution-discv5-port", default_value = "9200")]
        execution_discv5_port: u16,

        /// Consensus-layer discovery port (UDP discv5).
        #[arg(long, default_value = "9000")]
        cl_discovery_port: u16,

        /// Consensus-layer libp2p TCP port advertised in the local ENR.
        #[arg(long, default_value = "9000")]
        cl_p2p_port: u16,

        /// Maximum dialable consensus-layer peers retained from discovery.
        #[arg(long, default_value = "32")]
        cl_max_peers: usize,

        /// Disable the embedded HTML dashboard. HTTP query APIs remain available.
        #[arg(long)]
        disable_dashboard: bool,

        /// Require HTTP Basic authentication for dashboard, status, query, JSON-RPC, and WebSocket endpoints.
        ///
        /// Username is "logex"; this value is the password. Required when
        /// --http-host is a public/non-loopback address.
        #[arg(long, value_name = "PASSWORD")]
        dashboard_password: Option<String>,

        /// Allow unauthenticated gRPC to listen on a non-loopback interface.
        ///
        /// This only disables LogEx's startup guard; use firewalling or a
        /// private network because the gRPC API itself is not authenticated.
        #[arg(long)]
        allow_public_grpc: bool,

        /// Disable reverse historical execution sync for a fresh data directory.
        ///
        /// This mode only follows verified consensus anchors forward from the
        /// checkpoint pivot. It can only be enabled before the data directory is
        /// initialized; restart without this flag to resume normal historical sync.
        #[arg(long)]
        disable_historical_sync: bool,
    },

    /// Build or rebuild query indexes.
    #[command(after_help = "Examples:
  logex --data-dir /var/lib/logex/mainnet build-indexes
  logex --data-dir /var/lib/logex/mainnet build-indexes --sealed --missing-only --profile erc20-transfer --jobs 4
  logex --data-dir /var/lib/logex/mainnet build-indexes --sealed --from-block 12000000 --to-block 25100000")]
    BuildIndexes {
        /// Include sealed historical segments.
        #[arg(long)]
        sealed: bool,

        /// Include the active hot segment.
        ///
        /// When neither --hot nor --sealed is set, --hot is implied.
        #[arg(long)]
        hot: bool,

        /// Index profile to build.
        ///
        /// all: every supported query index.
        /// log-query: general log filtering indexes.
        /// erc20-transfer: common ERC20 Transfer/Approval bloom indexes.
        #[arg(long, value_enum, default_value = "all")]
        profile: IndexProfile,

        /// Skip segments that already have every index required by the selected profile.
        #[arg(long)]
        missing_only: bool,

        /// Maximum number of matching segments to index.
        #[arg(long)]
        limit: Option<usize>,

        /// Number of segment index builds to run concurrently.
        ///
        /// The effective worker count is capped by available CPUs and matching
        /// segment count.
        #[arg(long, default_value = "1")]
        jobs: usize,

        /// Only index segments whose block range overlaps this lower bound.
        #[arg(long)]
        from_block: Option<u64>,

        /// Only index segments whose block range overlaps this upper bound.
        #[arg(long)]
        to_block: Option<u64>,

        /// Only index segments whose timestamp range overlaps this lower bound.
        ///
        /// Value is a Unix timestamp in UTC seconds.
        #[arg(long)]
        from_timestamp: Option<u64>,

        /// Only index segments whose timestamp range overlaps this upper bound.
        ///
        /// Value is a Unix timestamp in UTC seconds.
        #[arg(long)]
        to_timestamp: Option<u64>,
    },

    /// Compact sealed storage segments.
    Compact {
        /// Maximum number of eligible sealed segments to compact.
        ///
        /// Omit to compact all eligible sealed segments. Normal historical sync
        /// already writes compacted sealed segments; this is mainly for older
        /// data directories or changed compression profiles.
        #[arg(long)]
        limit: Option<usize>,
    },

    /// Show storage, checkpoint, and indexed coverage statistics for the data directory.
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
    pub p2p_bind_ip: Option<IpAddr>,
    #[serde(default)]
    pub execution_bootnodes: Option<Vec<String>>,
    #[serde(default)]
    pub execution_discv5_port: Option<u16>,
    #[serde(default)]
    pub http_host: Option<IpAddr>,
    #[serde(default)]
    pub grpc_host: Option<IpAddr>,
    #[serde(default)]
    pub allow_public_grpc: Option<bool>,
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

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use clap::{CommandFactory, Parser};

    use super::{Cli, Command};

    #[test]
    fn sync_defaults_bind_query_apis_to_loopback() {
        let cli = Cli::try_parse_from(["logex", "sync"]).unwrap();

        let Command::Sync {
            http_host,
            grpc_host,
            allow_public_grpc,
            ..
        } = cli.command
        else {
            panic!("expected sync command");
        };

        assert_eq!(http_host, IpAddr::from([127, 0, 0, 1]));
        assert_eq!(grpc_host, IpAddr::from([127, 0, 0, 1]));
        assert!(!allow_public_grpc);
    }

    #[test]
    fn sync_accepts_explicit_public_query_api_hosts() {
        let cli = Cli::try_parse_from([
            "logex",
            "sync",
            "--http-host",
            "0.0.0.0",
            "--grpc-host",
            "0.0.0.0",
            "--allow-public-grpc",
        ])
        .unwrap();

        let Command::Sync {
            http_host,
            grpc_host,
            allow_public_grpc,
            ..
        } = cli.command
        else {
            panic!("expected sync command");
        };

        assert_eq!(http_host, IpAddr::from([0, 0, 0, 0]));
        assert_eq!(grpc_host, IpAddr::from([0, 0, 0, 0]));
        assert!(allow_public_grpc);
    }

    #[test]
    fn sync_accepts_explicit_ipv6_p2p_bind_ip() {
        let cli = Cli::try_parse_from(["logex", "sync", "--p2p-bind-ip", "::"]).unwrap();

        let Command::Sync { p2p_bind_ip, .. } = cli.command else {
            panic!("expected sync command");
        };

        assert_eq!(p2p_bind_ip, Some("::".parse::<IpAddr>().unwrap()));
    }

    #[test]
    fn sync_accepts_execution_bootnodes() {
        let bootnode = "enode://1dd9d65c4552b5eb43d5ad55a2ee3f56c6cbc1c64a5c8d659f51fcd51bace24351232b8d7821617d2b29b54b81cdefb9b3e9c37d7fd5f63270bcc9e1a6f6a439@[2001:db8:3c4d:15::abcd:ef12]:52150?discport=52151";
        let cli = Cli::try_parse_from(["logex", "sync", "--execution-bootnode", bootnode]).unwrap();

        let Command::Sync {
            execution_bootnodes,
            ..
        } = cli.command
        else {
            panic!("expected sync command");
        };

        assert_eq!(execution_bootnodes, vec![bootnode.to_owned()]);
    }

    #[test]
    fn sync_accepts_execution_discv5_port() {
        let cli =
            Cli::try_parse_from(["logex", "sync", "--execution-discv5-port", "9201"]).unwrap();

        let Command::Sync {
            execution_discv5_port,
            ..
        } = cli.command
        else {
            panic!("expected sync command");
        };

        assert_eq!(execution_discv5_port, 9201);
    }

    #[test]
    fn sync_accepts_disable_historical_sync_flag() {
        let cli = Cli::try_parse_from(["logex", "sync", "--disable-historical-sync"]).unwrap();

        let Command::Sync {
            disable_historical_sync,
            ..
        } = cli.command
        else {
            panic!("expected sync command");
        };

        assert!(disable_historical_sync);
    }

    #[test]
    fn help_documents_public_listener_controls() {
        let mut command = Cli::command();
        let sync = command
            .find_subcommand_mut("sync")
            .expect("sync subcommand should exist");
        let help = sync.render_long_help().to_string();

        assert!(help.contains("--http-host"));
        assert!(help.contains("Public HTTP requires --dashboard-password"));
        assert!(help.contains("--grpc-host"));
        assert!(help.contains("Public gRPC requires --allow-public-grpc"));
        assert!(help.contains("--dashboard-password <PASSWORD>"));
        assert!(help.contains("--allow-public-grpc"));
        assert!(help.contains("--p2p-bind-ip <IP>"));
        assert!(help.contains("--disable-historical-sync"));
        assert!(help.contains("only follows verified consensus anchors forward"));
    }

    #[test]
    fn help_documents_index_maintenance_controls() {
        let mut command = Cli::command();
        let build_indexes = command
            .find_subcommand_mut("build-indexes")
            .expect("build-indexes subcommand should exist");
        let help = build_indexes.render_long_help().to_string();

        assert!(help.contains("--sealed"));
        assert!(help.contains("--hot"));
        assert!(help.contains("--profile <PROFILE>"));
        assert!(help.contains("--missing-only"));
        assert!(help.contains("--jobs <JOBS>"));
        assert!(help.contains("--from-block <FROM_BLOCK>"));
        assert!(help.contains("--to-timestamp <TO_TIMESTAMP>"));
    }
}
