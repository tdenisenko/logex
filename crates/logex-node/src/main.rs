mod background;
mod checkpoint;
mod cli;
mod commands;
mod runtime;

use clap::Parser;

use cli::{Cli, Command, Config};
use logex_storage::PartitionManagerConfig;

fn main() {
    let cli = Cli::parse();

    let file_config = cli.config.as_ref().map(Config::load);
    if let Some(Err(e)) = &file_config {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    let file_config = file_config.and_then(|r| r.ok()).unwrap_or_default();

    let log_level = if cli.log_level != "info" {
        cli.log_level.clone()
    } else {
        file_config
            .log_level
            .unwrap_or_else(|| cli.log_level.clone())
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.parse().unwrap_or_default()),
        )
        .init();

    let data_dir = file_config.data_dir.unwrap_or(cli.data_dir);
    let partition_target_rows = file_config
        .partition_target_rows
        .unwrap_or(cli.partition_target_rows);
    let checkpoint = file_config.checkpoint.or(cli.checkpoint);
    let checkpoint_sync_url = file_config.checkpoint_sync_url.or(cli.checkpoint_sync_url);

    let pm_config = PartitionManagerConfig {
        data_dir,
        partition_target_rows,
        compaction_safety_margin_blocks: PartitionManagerConfig::default()
            .compaction_safety_margin_blocks,
    };

    match cli.command {
        Command::Sync {
            http_port,
            grpc_port,
            discovery_port,
            p2p_port,
            max_peers,
            cl_discovery_port,
            cl_p2p_port,
            cl_max_peers,
        } => {
            let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
            rt.block_on(runtime::run_sync(runtime::RunSyncOptions {
                pm_config,
                checkpoint,
                checkpoint_sync_url,
                http_port,
                grpc_port,
                discovery_port,
                p2p_port,
                max_peers,
                cl_discovery_port,
                cl_p2p_port,
                cl_max_peers,
            }));
        }
        Command::BuildIndexes => commands::run_build_indexes(pm_config),
        Command::Info => commands::run_info(pm_config),
    }
}
