mod background;
mod checkpoint;
mod cli;
mod commands;
mod runtime;

use clap::Parser;
use std::net::IpAddr;

use checkpoint::DEFAULT_CHECKPOINT_SYNC_URL;
use cli::{Cli, Command, Config, default_data_dir};
use logex_storage::PartitionManagerConfig;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
#[global_allocator]
static GLOBAL_ALLOCATOR: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_LOG_FILTER: &str = "info,discv5=error";
#[cfg(unix)]
const MIN_FILE_DESCRIPTOR_LIMIT: u64 = 16_384;

fn main() {
    let cli = Cli::parse();

    let file_config = cli.config.as_ref().map(Config::load);
    if let Some(Err(e)) = &file_config {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    let file_config = file_config.and_then(|r| r.ok()).unwrap_or_default();

    let log_level = effective_log_filter(&cli.log_level, file_config.log_level);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.parse().unwrap_or_default()),
        )
        .init();

    raise_file_descriptor_limit();

    let data_dir = file_config
        .data_dir
        .unwrap_or_else(|| cli.data_dir.unwrap_or_else(default_data_dir));
    let partition_target_rows = file_config
        .partition_target_rows
        .unwrap_or(cli.partition_target_rows);
    let checkpoint = file_config.checkpoint.or(cli.checkpoint);
    let checkpoint_sync_url = file_config.checkpoint_sync_url.or(cli.checkpoint_sync_url);
    let config_nat = file_config.nat;

    let pm_config = PartitionManagerConfig {
        data_dir,
        partition_target_rows,
        compaction_safety_margin_blocks: PartitionManagerConfig::default()
            .compaction_safety_margin_blocks,
    };

    match cli.command {
        Command::Sync {
            http_host,
            http_port,
            grpc_host,
            grpc_port,
            discovery_port,
            p2p_port,
            max_peers,
            nat,
            p2p_bind_ip,
            execution_bootnodes,
            execution_discv5_port,
            cl_discovery_port,
            cl_p2p_port,
            cl_max_peers,
            disable_dashboard,
            dashboard_password,
            allow_public_grpc,
            disable_historical_sync,
        } => {
            let nat = if nat == "any" {
                config_nat.unwrap_or(nat)
            } else {
                nat
            };
            let http_host = file_config.http_host.unwrap_or(http_host);
            let grpc_host = file_config.grpc_host.unwrap_or(grpc_host);
            let p2p_bind_ip = file_config.p2p_bind_ip.or(p2p_bind_ip);
            let execution_bootnodes = if execution_bootnodes.is_empty() {
                file_config.execution_bootnodes.unwrap_or_default()
            } else {
                execution_bootnodes
            };
            let execution_discv5_port = file_config
                .execution_discv5_port
                .unwrap_or(execution_discv5_port);
            let allow_public_grpc = file_config.allow_public_grpc.unwrap_or(allow_public_grpc);
            let dashboard_enabled =
                file_config.dashboard_enabled.unwrap_or(true) && !disable_dashboard;
            let dashboard_password = dashboard_password.or(file_config.dashboard_password);
            let checkpoint_sync_url = checkpoint_sync_url
                .filter(|url| !url.trim().is_empty())
                .or_else(|| Some(DEFAULT_CHECKPOINT_SYNC_URL.to_owned()));
            if dashboard_password
                .as_ref()
                .is_some_and(|password| password.is_empty())
            {
                eprintln!("Error: dashboard password cannot be empty");
                std::process::exit(1);
            }
            if let Err(error) = validate_listener_policy(
                http_host,
                grpc_host,
                dashboard_password.as_deref(),
                allow_public_grpc,
            ) {
                eprintln!("Error: {error}");
                std::process::exit(1);
            }
            let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
            rt.block_on(runtime::run_sync(runtime::RunSyncOptions {
                pm_config,
                checkpoint,
                checkpoint_sync_url,
                http_host,
                http_port,
                grpc_host,
                grpc_port,
                discovery_port,
                p2p_port,
                max_peers,
                nat,
                p2p_bind_ip,
                execution_bootnodes,
                execution_discv5_port,
                cl_discovery_port,
                cl_p2p_port,
                cl_max_peers,
                dashboard_enabled,
                dashboard_password,
                disable_historical_sync,
            }));
        }
        Command::BuildIndexes {
            sealed,
            hot,
            profile,
            missing_only,
            limit,
            jobs,
            from_block,
            to_block,
            from_timestamp,
            to_timestamp,
        } => commands::run_build_indexes(
            pm_config,
            commands::BuildIndexesOptions {
                sealed,
                hot,
                profile,
                missing_only,
                limit,
                jobs,
                from_block,
                to_block,
                from_timestamp,
                to_timestamp,
            },
        ),
        Command::Compact { limit } => commands::run_compact(pm_config, limit),
        Command::Info => commands::run_info(pm_config),
    }
}

fn effective_log_filter(cli_log_level: &str, config_log_level: Option<String>) -> String {
    let requested = if cli_log_level != DEFAULT_LOG_LEVEL {
        cli_log_level.to_owned()
    } else {
        config_log_level.unwrap_or_else(|| DEFAULT_LOG_FILTER.to_owned())
    };
    normalize_info_log_filter(requested)
}

fn normalize_info_log_filter(filter: String) -> String {
    if filter.trim() == DEFAULT_LOG_LEVEL {
        DEFAULT_LOG_FILTER.to_owned()
    } else {
        filter
    }
}

fn validate_listener_policy(
    http_host: IpAddr,
    grpc_host: IpAddr,
    dashboard_password: Option<&str>,
    allow_public_grpc: bool,
) -> Result<(), String> {
    if is_public_listener(http_host) && dashboard_password.is_none() {
        return Err(
            "public HTTP listeners require --dashboard-password; use --http-host 127.0.0.1 for local-only access"
                .to_owned(),
        );
    }

    if is_public_listener(grpc_host) && !allow_public_grpc {
        return Err(
            "public gRPC listeners require --allow-public-grpc; use --grpc-host 127.0.0.1 for local-only access"
                .to_owned(),
        );
    }

    Ok(())
}

fn is_public_listener(host: IpAddr) -> bool {
    !host.is_loopback()
}

fn raise_file_descriptor_limit() {
    #[cfg(unix)]
    match raise_file_descriptor_limit_unix(MIN_FILE_DESCRIPTOR_LIMIT) {
        Ok(Some(change)) => tracing::info!(
            previous_soft_limit = change.previous_soft,
            soft_limit = change.current_soft,
            hard_limit = change.hard,
            "raised process file descriptor limit"
        ),
        Ok(None) => tracing::debug!(
            minimum = MIN_FILE_DESCRIPTOR_LIMIT,
            "process file descriptor limit is sufficient"
        ),
        Err(error) => tracing::warn!(
            %error,
            minimum = MIN_FILE_DESCRIPTOR_LIMIT,
            "failed to raise process file descriptor limit"
        ),
    }
}

#[cfg(unix)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileDescriptorLimitChange {
    previous_soft: u64,
    current_soft: u64,
    hard: u64,
}

#[cfg(unix)]
fn raise_file_descriptor_limit_unix(
    minimum: u64,
) -> std::io::Result<Option<FileDescriptorLimitChange>> {
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    // SAFETY: getrlimit initializes the provided rlimit pointer when it returns 0.
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: getrlimit succeeded, so the rlimit structure has been initialized.
    let mut limit = unsafe { limit.assume_init() };

    let current_soft = limit.rlim_cur;
    let hard = limit.rlim_max;
    let Some(target_soft) = desired_file_descriptor_soft_limit(current_soft, hard, minimum) else {
        return Ok(None);
    };

    limit.rlim_cur = target_soft;
    // SAFETY: limit was obtained from getrlimit and only rlim_cur is adjusted within rlim_max.
    if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) } != 0 {
        return Err(std::io::Error::last_os_error());
    }

    Ok(Some(FileDescriptorLimitChange {
        previous_soft: current_soft,
        current_soft: target_soft,
        hard,
    }))
}

fn desired_file_descriptor_soft_limit(current_soft: u64, hard: u64, minimum: u64) -> Option<u64> {
    let target = minimum.min(hard);
    (target > current_soft).then_some(target)
}

#[cfg(test)]
mod tests {
    use super::{
        DEFAULT_LOG_FILTER, desired_file_descriptor_soft_limit, effective_log_filter,
        validate_listener_policy,
    };
    use std::net::IpAddr;

    #[test]
    fn default_info_log_filter_suppresses_noisy_discovery_warnings() {
        assert_eq!(effective_log_filter("info", None), DEFAULT_LOG_FILTER);
        assert_eq!(
            effective_log_filter("info", Some("info".to_owned())),
            DEFAULT_LOG_FILTER
        );
    }

    #[test]
    fn explicit_log_filters_are_preserved() {
        assert_eq!(effective_log_filter("debug", None), "debug");
        assert_eq!(
            effective_log_filter("info", Some("info,discv5=warn".to_owned())),
            "info,discv5=warn"
        );
    }

    #[test]
    fn listener_policy_allows_loopback_without_auth() {
        assert!(
            validate_listener_policy(
                IpAddr::from([127, 0, 0, 1]),
                IpAddr::from([127, 0, 0, 1]),
                None,
                false,
            )
            .is_ok()
        );
    }

    #[test]
    fn listener_policy_rejects_public_http_without_auth() {
        let error = validate_listener_policy(
            IpAddr::from([0, 0, 0, 0]),
            IpAddr::from([127, 0, 0, 1]),
            None,
            false,
        )
        .unwrap_err();

        assert!(error.contains("public HTTP listeners require --dashboard-password"));
    }

    #[test]
    fn listener_policy_allows_public_http_with_auth() {
        assert!(
            validate_listener_policy(
                IpAddr::from([0, 0, 0, 0]),
                IpAddr::from([127, 0, 0, 1]),
                Some("secret"),
                false,
            )
            .is_ok()
        );
    }

    #[test]
    fn listener_policy_rejects_public_grpc_without_explicit_allow() {
        let error = validate_listener_policy(
            IpAddr::from([127, 0, 0, 1]),
            IpAddr::from([0, 0, 0, 0]),
            None,
            false,
        )
        .unwrap_err();

        assert!(error.contains("public gRPC listeners require --allow-public-grpc"));
    }

    #[test]
    fn listener_policy_allows_public_grpc_with_explicit_allow() {
        assert!(
            validate_listener_policy(
                IpAddr::from([127, 0, 0, 1]),
                IpAddr::from([0, 0, 0, 0]),
                None,
                true,
            )
            .is_ok()
        );
    }

    #[test]
    fn descriptor_limit_is_raised_to_minimum_when_possible() {
        assert_eq!(
            desired_file_descriptor_soft_limit(256, 65_536, 16_384),
            Some(16_384)
        );
    }

    #[test]
    fn descriptor_limit_respects_hard_limit() {
        assert_eq!(
            desired_file_descriptor_soft_limit(256, 4_096, 16_384),
            Some(4_096)
        );
    }

    #[test]
    fn descriptor_limit_is_unchanged_when_already_sufficient() {
        assert_eq!(
            desired_file_descriptor_soft_limit(32_768, 65_536, 16_384),
            None
        );
    }
}
