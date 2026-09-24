use std::net::IpAddr;
use std::path::{Path, PathBuf};

use clap::{ArgMatches, Args, Parser, Subcommand, ValueEnum, parser::ValueSource};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(
    name = "logex",
    version = env!("CARGO_PKG_VERSION"),
    about = "Standalone Ethereum log-verification client and query server",
    long_about = "LogEx joins the Ethereum consensus-layer and execution-layer P2P networks, verifies receipt logs, stores them locally, and serves SQL, JSON-RPC, gRPC, WebSocket, and dashboard query APIs.",
    after_help = "Examples:
  logex sync
  logex repair --dry-run
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

    /// Required mount location for an external data volume (with --expected-volume-uuid).
    #[arg(long, value_name = "PATH", global = true)]
    pub expected_volume_mount: Option<PathBuf>,

    /// Stable filesystem UUID of the external data volume.
    #[arg(long, value_name = "UUID", global = true)]
    pub expected_volume_uuid: Option<String>,

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
    /// Explicit command-line options override file settings; unknown keys are errors.
    ///
    /// Supported keys: data_dir, expected_volume_mount, expected_volume_uuid,
    /// log_level, partition_target_rows, checkpoint,
    /// checkpoint_sync_url, nat, p2p_bind_ip, execution_bootnodes, execution_discv5_port,
    /// http_host, grpc_host, allow_public_grpc, dashboard_enabled, dashboard_password,
    /// repair_corrupt_segments, query_max_concurrent.
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

impl Cli {
    /// Resolve explicit command-line values, then file values, then CLI defaults.
    /// `matches` must be the same parse used to construct this `Cli`.
    pub fn apply_config(&mut self, file_config: Config, matches: &ArgMatches) {
        self.data_dir = self.data_dir.take().or(file_config.data_dir);
        self.expected_volume_mount = self
            .expected_volume_mount
            .take()
            .or(file_config.expected_volume_mount);
        self.expected_volume_uuid = self
            .expected_volume_uuid
            .take()
            .or(file_config.expected_volume_uuid);
        apply_file_default(
            &mut self.log_level,
            file_config.log_level,
            matches,
            "log_level",
        );
        apply_file_default(
            &mut self.partition_target_rows,
            file_config.partition_target_rows,
            matches,
            "partition_target_rows",
        );
        self.checkpoint = self.checkpoint.take().or(file_config.checkpoint);
        self.checkpoint_sync_url = self
            .checkpoint_sync_url
            .take()
            .or(file_config.checkpoint_sync_url);
        match &mut self.command {
            Command::Sync {
                http_host,
                nat,
                p2p_bind_ip,
                execution_bootnodes,
                execution_discv5_port,
                disable_dashboard,
                dashboard_password,
                ..
            }
            | Command::Repair {
                http_host,
                nat,
                p2p_bind_ip,
                execution_bootnodes,
                execution_discv5_port,
                disable_dashboard,
                dashboard_password,
                ..
            } => {
                let (_, command_matches) = matches
                    .subcommand()
                    .expect("command comes from the same CLI parse");
                apply_file_default(nat, file_config.nat, command_matches, "nat");
                apply_file_default(
                    http_host,
                    file_config.http_host,
                    command_matches,
                    "http_host",
                );
                *p2p_bind_ip = p2p_bind_ip.or(file_config.p2p_bind_ip);
                apply_file_default(
                    execution_bootnodes,
                    file_config.execution_bootnodes,
                    command_matches,
                    "execution_bootnodes",
                );
                apply_file_default(
                    execution_discv5_port,
                    file_config.execution_discv5_port,
                    command_matches,
                    "execution_discv5_port",
                );
                *disable_dashboard |= file_config.dashboard_enabled == Some(false);
                *dashboard_password = dashboard_password.take().or(file_config.dashboard_password);
            }
            _ => {}
        }
        if let Command::Sync {
            grpc_host,
            allow_public_grpc,
            repair_corrupt_segments,
            query_max_concurrent,
            ..
        } = &mut self.command
        {
            let matches = matches
                .subcommand_matches("sync")
                .expect("sync options come from the same CLI parse");
            apply_file_default(grpc_host, file_config.grpc_host, matches, "grpc_host");
            apply_file_default(
                query_max_concurrent,
                file_config.query_max_concurrent,
                matches,
                "query_max_concurrent",
            );
            apply_file_default(
                allow_public_grpc,
                file_config.allow_public_grpc,
                matches,
                "allow_public_grpc",
            );
            apply_file_default(
                repair_corrupt_segments,
                file_config.repair_corrupt_segments,
                matches,
                "repair_corrupt_segments",
            );
        }
    }
}

fn apply_file_default<T>(target: &mut T, configured: Option<T>, matches: &ArgMatches, id: &str) {
    if matches.value_source(id) != Some(ValueSource::CommandLine)
        && let Some(value) = configured
    {
        *target = value;
    }
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

/// Positive maintenance work allowances, not process-memory/RSS guarantees.
#[derive(Args, Clone, Debug)]
pub struct RepairLimitsArgs {
    /// Maintenance work deadline in seconds; shutdown and cleanup have separate bounds.
    #[arg(long, default_value = "3600", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_timeout_secs: u64,

    /// Maximum time per execution-peer repair request, in seconds.
    #[arg(long, default_value = "15", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_request_timeout_secs: u64,

    /// Maximum attempts for each execution-peer repair request.
    #[arg(long, default_value = "3", value_parser = positive_usize)]
    pub repair_max_attempts: usize,

    /// Maximum segments in the primary reconstruction plan, including overlapping owners.
    ///
    /// Does not limit the number of segments inspected or rebuilt for indexes.
    #[arg(long, default_value = "64", value_parser = positive_usize)]
    pub repair_max_segments: usize,

    /// Per-segment inspection/reconstruction row allowance and retained WAL row allowance.
    #[arg(long, default_value = "4000000", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_max_segment_rows: u64,

    /// Byte allowance for each separate per-segment source, decoded payload, index,
    /// routing, canonical, carry-row and candidate-data budget, and for retained WAL.
    ///
    /// Each budget can aggregate multiple artifacts; these allowances are not
    /// one combined memory or disk-space cap.
    #[arg(long, default_value = "1073741824", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_max_segment_bytes: u64,

    /// Maximum retained rows across the selected primary reconstruction plan.
    #[arg(long, default_value = "8000000", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_max_total_rows: u64,

    /// Maximum retained row payload bytes across the primary reconstruction plan,
    /// including preserved carry rows and fetched replacement rows.
    #[arg(long, default_value = "2147483648", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_max_total_data_bytes: u64,

    /// Maximum distinct blocks covered by the overlapping primary reconstruction ranges.
    #[arg(long, default_value = "1000000", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_max_blocks: u64,

    /// Maximum headers per fetched range, including its bridge to a retained anchor.
    /// This is not a sum across all reconstruction ranges.
    #[arg(long, default_value = "2000000", value_parser = clap::value_parser!(u64).range(1..))]
    pub repair_max_headers: u64,
}

fn positive_usize(value: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| "value must be a positive platform-sized integer".to_owned())
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
        /// Shared maximum admitted SQL and native queries across REST, JSON-RPC and gRPC.
        /// Excess requests fail immediately; this is not a query memory budget.
        #[arg(long, default_value = "8")]
        query_max_concurrent: usize,

        /// Repair corrupt segments from retained verified trust before normal sync.
        #[arg(long, default_value = "false", num_args = 0..=1, require_equals = true, default_missing_value = "true", action = clap::ArgAction::Set)]
        repair_corrupt_segments: bool,

        #[command(flatten)]
        repair_limits: RepairLimitsArgs,

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

        /// Execution-layer discv4 UDP discovery port for IPv4 execution binds.
        ///
        /// Strict IPv6 execution binds disable discv4 and use
        /// --execution-discv5-port plus direct IPv6 RLPx candidates instead.
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
        /// The default "any" auto-selects a locally owned public IPv4 address
        /// with outbound reachability, then a locally owned public IPv6 address
        /// with outbound reachability, and otherwise runs outbound-only without
        /// advertising a public address. Accepted explicit values include:
        /// none, publicip, netif, extip:<ip>, extaddr:<domain>. On public
        /// servers, extip:<ip> is usually the most deterministic choice.
        #[arg(long, default_value = "any")]
        nat: String,

        /// Local IP address used by execution and consensus P2P listeners.
        ///
        /// By default, LogEx uses automatic address-family selection. Use "::"
        /// with --nat extip:<ipv6> to select IPv6; LogEx narrows the listener
        /// to the concrete local public IPv6 address when it can verify that
        /// address locally.
        #[arg(long, value_name = "IP")]
        p2p_bind_ip: Option<IpAddr>,

        /// Execution-layer bootnode to seed discovery and direct dials.
        ///
        /// Accepts enode:// records with IP literals or DNS names, or signed
        /// enr: records. May be repeated or comma-separated. IPv6 enodes must
        /// use the standard bracketed form, for example
        /// enode://<pubkey>@[2001:db8::1]:30303?discport=30303.
        #[arg(
            long = "execution-bootnode",
            value_name = "ENODE_OR_ENR",
            value_delimiter = ','
        )]
        execution_bootnodes: Vec<String>,

        /// Execution-layer discovery v5 UDP port.
        ///
        /// Used for strict IPv6 execution peer discovery. The default matches
        /// Reth's execution discv5 default.
        #[arg(long = "execution-discv5-port", default_value = "9200")]
        execution_discv5_port: u16,

        /// Consensus-layer discovery port (UDP discv5).
        #[arg(long, default_value = "9000")]
        cl_discovery_port: u16,

        /// Consensus-layer libp2p TCP port advertised in the local ENR.
        #[arg(long, default_value = "9000")]
        cl_p2p_port: u16,

        /// Target number of consensus-layer peers.
        ///
        /// Zero disables aggregate established-connection limits; per-peer and
        /// pending-connection limits remain active.
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

    /// Inspect or repair storage using retained verified trust and execution peers.
    ///
    /// Does not establish new consensus/checkpoint trust. Limits bound maintenance
    /// work; they are not RSS guarantees or a disk reservation. Separate byte
    /// allowances can aggregate multiple source or decoded artifacts.
    #[command(after_help = "Examples:
  logex --data-dir /var/lib/logex/mainnet repair --dry-run
  logex --data-dir /var/lib/logex/mainnet repair --repair-max-segments 16

Output:
  Successful assessments emit JSON on stdout; logs and errors go to stderr.
  Dry-run exit codes: 0 = locally verified; 1 = blocked, limit exceeded, or error;
  2 = further work required. Local verification does not prove chain completeness.")]
    Repair {
        /// Inspect and report without modifying storage or starting peer networking.
        #[arg(long)]
        dry_run: bool,

        /// HTTP status/dashboard bind address; public binds require a password.
        #[arg(long, default_value = "127.0.0.1")]
        http_host: IpAddr,
        /// HTTP status/dashboard port.
        #[arg(long, default_value = "8577")]
        http_port: u16,
        /// Disable the embedded HTML dashboard.
        #[arg(long)]
        disable_dashboard: bool,
        /// HTTP Basic authentication password (username: logex).
        #[arg(long, value_name = "PASSWORD")]
        dashboard_password: Option<String>,
        /// Execution-layer discv4 UDP discovery port.
        #[arg(long, default_value = "30303")]
        discovery_port: u16,
        /// Execution-layer TCP listener port.
        #[arg(long, default_value = "30303")]
        p2p_port: u16,
        /// Maximum execution-layer peer sessions.
        #[arg(long, default_value = "100")]
        max_peers: usize,
        /// Execution-layer NAT/external address resolver advertised to peers.
        #[arg(long, default_value = "any")]
        nat: String,
        /// Local execution-layer listener IP address.
        #[arg(long, value_name = "IP")]
        p2p_bind_ip: Option<IpAddr>,
        /// Execution bootnode enode:// or enr: record; repeat or comma-separate.
        #[arg(
            long = "execution-bootnode",
            value_name = "ENODE_OR_ENR",
            value_delimiter = ','
        )]
        execution_bootnodes: Vec<String>,
        /// Execution-layer discovery v5 UDP port.
        #[arg(long = "execution-discv5-port", default_value = "9200")]
        execution_discv5_port: u16,

        #[command(flatten)]
        repair_limits: RepairLimitsArgs,
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
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub query_max_concurrent: Option<usize>,
    #[serde(default)]
    pub repair_corrupt_segments: Option<bool>,
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    #[serde(default)]
    pub expected_volume_mount: Option<PathBuf>,
    #[serde(default)]
    pub expected_volume_uuid: Option<String>,
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
    pub fn load(path: &Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|error| format!("failed to read config {path:?}: {error}"))?;
        toml::from_str(&contents).map_err(|error: toml::de::Error| {
            // Both Display and message() can contain configuration values.
            // Report a location without copying password-bearing source text.
            let location = error.span().map(|span| {
                let (line, column) = contents.char_indices()
                    .take_while(|(offset, _)| *offset < span.start)
                    .fold((1_usize, 1_usize), |(line, column), (_, character)| {
                        if character == '\n' { (line + 1, 1) } else { (line, column + 1) }
                    });
                format!(" at line {line}, column {column}")
            }).unwrap_or_default();
            format!("failed to parse config {path:?}{location}; check TOML syntax, supported keys and value types")
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use clap::{CommandFactory, Parser};

    use super::{Cli, Command};

    #[test]
    fn repair_and_sync_share_positive_maintenance_limits() {
        let flags = [
            "--repair-timeout-secs",
            "--repair-request-timeout-secs",
            "--repair-max-attempts",
            "--repair-max-segments",
            "--repair-max-segment-rows",
            "--repair-max-segment-bytes",
            "--repair-max-total-rows",
            "--repair-max-total-data-bytes",
            "--repair-max-blocks",
            "--repair-max-headers",
        ];
        for command in ["repair", "sync"] {
            for flag in flags {
                assert!(
                    Cli::try_parse_from(["logex", command, flag, "0"]).is_err(),
                    "{command} {flag}"
                );
                assert!(
                    Cli::try_parse_from(["logex", command, flag, "1"]).is_ok(),
                    "{command} {flag}"
                );
            }
            let cli = Cli::try_parse_from(["logex", command]).unwrap();
            let limits = match cli.command {
                Command::Repair {
                    repair_limits,
                    dry_run,
                    http_host,
                    ..
                } => {
                    assert!(!dry_run);
                    assert_eq!(http_host, IpAddr::from([127, 0, 0, 1]));
                    repair_limits
                }
                Command::Sync {
                    repair_limits,
                    repair_corrupt_segments,
                    ..
                } => {
                    assert!(!repair_corrupt_segments);
                    repair_limits
                }
                _ => unreachable!(),
            };
            assert_eq!(
                (
                    limits.repair_timeout_secs,
                    limits.repair_request_timeout_secs
                ),
                (3600, 15)
            );
            assert_eq!(
                (limits.repair_max_attempts, limits.repair_max_segments),
                (3, 64)
            );
            assert_eq!(
                (
                    limits.repair_max_segment_rows,
                    limits.repair_max_segment_bytes
                ),
                (4_000_000, 1_073_741_824)
            );
            assert_eq!(
                (
                    limits.repair_max_total_rows,
                    limits.repair_max_total_data_bytes
                ),
                (8_000_000, 2_147_483_648)
            );
            assert_eq!(
                (limits.repair_max_blocks, limits.repair_max_headers),
                (1_000_000, 2_000_000)
            );
        }
        assert!(Cli::try_parse_from(["logex", "repair", "--cl-p2p-port", "9001"]).is_err());
    }

    #[test]
    fn repair_config_defaults_and_explicit_cli_precedence() {
        use clap::FromArgMatches;
        for explicit in [false, true] {
            let mut args = vec!["logex", "repair", "--dry-run"];
            if explicit {
                args.extend([
                    "--http-host",
                    "127.0.0.2",
                    "--nat",
                    "none",
                    "--p2p-bind-ip",
                    "::1",
                    "--execution-bootnode",
                    "cli-node",
                    "--execution-discv5-port",
                    "9202",
                    "--dashboard-password",
                    "cli-password",
                ]);
            }
            let matches = Cli::command().try_get_matches_from(args).unwrap();
            let mut cli = Cli::from_arg_matches(&matches).unwrap();
            let config = toml::from_str(
                r#"
http_host = "127.0.0.3"
nat = "netif"
p2p_bind_ip = "127.0.0.4"
execution_bootnodes = ["file-node"]
execution_discv5_port = 9203
dashboard_enabled = false
dashboard_password = "file-password"
"#,
            )
            .unwrap();
            cli.apply_config(config, &matches);
            let Command::Repair {
                dry_run,
                http_host,
                nat,
                p2p_bind_ip,
                execution_bootnodes,
                execution_discv5_port,
                disable_dashboard,
                dashboard_password,
                ..
            } = cli.command
            else {
                unreachable!()
            };
            assert!(dry_run && disable_dashboard);
            assert_eq!(
                http_host.to_string(),
                if explicit { "127.0.0.2" } else { "127.0.0.3" }
            );
            assert_eq!(nat, if explicit { "none" } else { "netif" });
            assert_eq!(
                p2p_bind_ip.unwrap().to_string(),
                if explicit { "::1" } else { "127.0.0.4" }
            );
            assert_eq!(
                execution_bootnodes,
                vec![if explicit { "cli-node" } else { "file-node" }]
            );
            assert_eq!(execution_discv5_port, if explicit { 9202 } else { 9203 });
            assert_eq!(
                dashboard_password.as_deref(),
                Some(if explicit {
                    "cli-password"
                } else {
                    "file-password"
                })
            );
        }
    }

    #[test]
    fn sync_repair_opt_in_cli_overrides_config_in_both_directions() {
        use clap::FromArgMatches;
        for (flag, configured, expected) in [
            (None, true, true),
            (None, false, false),
            (Some("--repair-corrupt-segments"), false, true),
            (Some("--repair-corrupt-segments=false"), true, false),
        ] {
            let mut args = vec!["logex", "sync"];
            args.extend(flag);
            let matches = Cli::command().try_get_matches_from(args).unwrap();
            let mut cli = Cli::from_arg_matches(&matches).unwrap();
            cli.apply_config(
                super::Config {
                    repair_corrupt_segments: Some(configured),
                    ..Default::default()
                },
                &matches,
            );
            let Command::Sync {
                repair_corrupt_segments,
                ..
            } = cli.command
            else {
                unreachable!()
            };
            assert_eq!(repair_corrupt_segments, expected);
        }
        assert!(toml::from_str::<super::Config>("repair_corrupt_segments = true").is_ok());
        assert!(toml::from_str::<super::Config>("repair_max_segments = 4").is_err());
    }

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
        assert!(help.contains("IPv6 enodes must"));
        assert!(help.contains("--execution-bootnode <ENODE_OR_ENR>"));
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

#[cfg(test)]
mod config_tests;
