mod background;
mod checkpoint;
mod cli;
mod commands;
mod runtime;
mod volume;

use clap::{CommandFactory, FromArgMatches};
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
    let matches = Cli::command().get_matches();
    let mut cli = Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit());

    let file_config = cli.config.as_deref().map(Config::load);
    if let Some(Err(e)) = &file_config {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    let file_config = file_config.and_then(|r| r.ok()).unwrap_or_default();

    cli.apply_config(file_config, &matches);
    // Validate resolved admission settings before volume probes, path creation,
    // storage ownership or runtime startup can have side effects.
    let query_concurrency = match resolved_query_concurrency(&cli.command) {
        Ok(limit) => limit,
        Err(error) => {
            eprintln!("Error: {error}");
            std::process::exit(1);
        }
    };
    drop(matches);
    let log_level = normalize_info_log_filter(cli.log_level);

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.parse().unwrap_or_default()),
        )
        .init();

    raise_file_descriptor_limit();

    let mut data_dir = cli.data_dir.unwrap_or_else(default_data_dir);
    let partition_target_rows = cli.partition_target_rows;
    let mut checkpoint = cli.checkpoint;
    let checkpoint_sync_url = cli.checkpoint_sync_url;
    let is_repair = matches!(cli.command, Command::Repair { .. });
    let dry_run = matches!(cli.command, Command::Repair { dry_run: true, .. });
    if is_repair && (checkpoint.is_some() || checkpoint_sync_url.is_some()) {
        eprintln!(
            "Error: repair uses retained consensus trust only; omit checkpoint and checkpoint_sync_url from the command and config file"
        );
        std::process::exit(1);
    }
    let listener_policy = match &cli.command {
        Command::Sync {
            http_host,
            grpc_host,
            dashboard_password,
            allow_public_grpc,
            ..
        } => validate_listener_policy(
            *http_host,
            *grpc_host,
            dashboard_password.as_deref(),
            *allow_public_grpc,
        ),
        Command::Repair {
            dry_run: false,
            http_host,
            dashboard_password,
            ..
        } => validate_listener_policy(
            *http_host,
            std::net::Ipv4Addr::LOCALHOST.into(),
            dashboard_password.as_deref(),
            false,
        ),
        _ => Ok(()),
    };
    if let Err(error) = listener_policy {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }

    let expected_volume = match volume::configured(
        cli.expected_volume_mount,
        cli.expected_volume_uuid,
    )
    .and_then(|configured| {
        configured
            .map(|(mount, uuid)| {
                let volume = if is_repair {
                    // Repair requires an existing dataset. Even writable
                    // repair must never initialize a missing directory.
                    volume::ExpectedVolume::prepare_read_only(&mount, &uuid, &data_dir)
                } else {
                    volume::ExpectedVolume::prepare(&mount, &uuid, &data_dir, &mut checkpoint)
                }?;
                if is_repair && !dry_run {
                    volume.check()?;
                }
                Ok::<_, std::io::Error>(std::sync::Arc::new(volume))
            })
            .transpose()
    }) {
        Ok(volume) => volume,
        Err(error) => {
            eprintln!("Error: storage volume preflight failed: {error}");
            std::process::exit(1);
        }
    };
    if expected_volume.is_some() {
        data_dir = std::path::PathBuf::from(".");
    }
    if let Command::Repair {
        dry_run: true,
        ref repair_limits,
        ..
    } = cli.command
    {
        match runtime::repair::run_dry_run(&data_dir, repair_limits, expected_volume.as_deref()) {
            Ok(0) => return,
            Ok(code) => std::process::exit(code),
            Err(error) => {
                eprintln!("Error: repair inspection failed: {error:#}");
                std::process::exit(1);
            }
        }
    }
    let mut storage_monitor = expected_volume.map(|volume| {
        volume::StorageMonitor::start_volume(volume).unwrap_or_else(|error| {
            eprintln!("Error: cannot supervise storage volume: {error}");
            std::process::exit(1);
        })
    });

    let pm_config = PartitionManagerConfig {
        data_dir,
        partition_target_rows,
        compaction_safety_margin_blocks: PartitionManagerConfig::default()
            .compaction_safety_margin_blocks,
    };

    match cli.command {
        Command::Sync {
            query_max_concurrent: _,
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
            allow_public_grpc: _,
            disable_historical_sync,
            repair_corrupt_segments,
            repair_limits,
        } => {
            let dashboard_enabled = !disable_dashboard;
            let checkpoint_sync_url = checkpoint_sync_url
                .filter(|url| !url.trim().is_empty())
                .or_else(|| Some(DEFAULT_CHECKPOINT_SYNC_URL.to_owned()));
            let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
            let shutdown = rt.block_on(runtime::run_sync(runtime::RunSyncOptions {
                query_concurrency: query_concurrency
                    .expect("sync admission was validated before startup"),
                pm_config,
                storage_monitor: &mut storage_monitor,
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
                repair_corrupt_segments,
                repair_limits,
            }));
            if runtime::finish_runtime_shutdown(rt, shutdown, || drop(storage_monitor.take()))
                .is_err()
            {
                std::process::exit(1);
            }
        }
        Command::Repair {
            dry_run: _,
            http_host,
            http_port,
            disable_dashboard,
            dashboard_password,
            discovery_port,
            p2p_port,
            max_peers,
            nat,
            p2p_bind_ip,
            execution_bootnodes,
            execution_discv5_port,
            repair_limits,
        } => {
            let rt = tokio::runtime::Runtime::new().unwrap_or_else(|error| {
                eprintln!("Error: cannot create repair runtime: {error}");
                std::process::exit(1);
            });
            let (shutdown, result) = rt.block_on(runtime::run_repair_command(
                runtime::repair::RunRepairOptions {
                    root: pm_config.data_dir,
                    limits: repair_limits,
                    http_address: std::net::SocketAddr::new(http_host, http_port),
                    http: logex_server::HttpServerConfig {
                        dashboard_enabled: !disable_dashboard,
                        dashboard_password,
                    },
                    network: runtime::repair::RepairNetworkOptions {
                        discovery_port,
                        p2p_port,
                        max_peers,
                        nat,
                        p2p_bind_ip,
                        execution_bootnodes,
                        execution_discv5_port,
                    },
                },
                &mut storage_monitor,
            ));
            let cleanup =
                runtime::finish_runtime_shutdown(rt, shutdown, || drop(storage_monitor.take()));
            if let Err(error) = result {
                eprintln!("Error: repair failed: {error:#}");
                std::process::exit(1);
            }
            if let Err(error) = cleanup {
                eprintln!("Error: repair shutdown failed: {error}");
                std::process::exit(1);
            }
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
    // Offline commands also retain the monitor through all command-owned I/O.
    drop(storage_monitor);
}

fn normalize_info_log_filter(filter: String) -> String {
    if filter.trim() == DEFAULT_LOG_LEVEL {
        DEFAULT_LOG_FILTER.to_owned()
    } else {
        filter
    }
}

fn resolved_query_concurrency(
    command: &Command,
) -> Result<Option<logex_server::QueryConcurrencyLimit>, String> {
    match command {
        Command::Sync {
            query_max_concurrent,
            ..
        } => logex_server::QueryConcurrencyLimit::new(*query_max_concurrent)
            .map(Some)
            .map_err(|error| {
                format!("invalid --query-max-concurrent / query_max_concurrent: {error}")
            }),
        _ => Ok(None),
    }
}

fn validate_listener_policy(
    http_host: IpAddr,
    grpc_host: IpAddr,
    dashboard_password: Option<&str>,
    allow_public_grpc: bool,
) -> Result<(), String> {
    if dashboard_password.is_some_and(str::is_empty) {
        return Err("dashboard password cannot be empty".to_owned());
    }
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
        DEFAULT_LOG_FILTER, desired_file_descriptor_soft_limit, normalize_info_log_filter,
        validate_listener_policy,
    };
    use std::net::IpAddr;

    #[test]
    fn info_log_filter_suppresses_noisy_discovery_warnings() {
        assert_eq!(
            normalize_info_log_filter("info".to_owned()),
            DEFAULT_LOG_FILTER
        );
        assert_eq!(
            normalize_info_log_filter(" info ".to_owned()),
            DEFAULT_LOG_FILTER
        );
    }

    #[test]
    fn custom_log_filters_are_preserved() {
        for filter in ["debug", "warn", "info,discv5=warn"] {
            assert_eq!(normalize_info_log_filter(filter.to_owned()), filter);
        }
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
