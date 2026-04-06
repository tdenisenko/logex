use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use serde::Deserialize;

use logex_server::{AppState, SubscriptionManager};
use logex_storage::{PartitionManager, PartitionManagerConfig};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(
    name = "logex",
    version = VERSION,
    about = "Fast, self-hosted Ethereum node for querying logs and token transfers with SQL"
)]
struct Cli {
    /// Path to the LogEx data directory.
    #[arg(long, default_value = "./data", global = true)]
    data_dir: PathBuf,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info", global = true)]
    log_level: String,

    /// Target rows per partition before sealing.
    #[arg(long, default_value = "50000000", global = true)]
    partition_target_rows: u64,

    /// Path to optional TOML config file.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the query server (JSON-RPC + REST + gRPC + WebSocket).
    Serve {
        /// HTTP server bind address.
        #[arg(long, default_value = "127.0.0.1:8545")]
        http_addr: SocketAddr,

        /// gRPC server bind address.
        #[arg(long, default_value = "127.0.0.1:8546")]
        grpc_addr: SocketAddr,
    },

    /// Ingest blocks from an Ethereum JSON-RPC endpoint.
    Ingest {
        /// Ethereum JSON-RPC URL.
        #[arg(long)]
        rpc_url: String,

        /// First block to ingest (inclusive).
        #[arg(long)]
        from_block: u64,

        /// Last block to ingest (inclusive). Defaults to latest.
        #[arg(long)]
        to_block: Option<u64>,

        /// Build indexes on the hot partition after ingestion.
        #[arg(long, default_value = "true")]
        build_indexes: bool,
    },

    /// Build or rebuild indexes on the hot partition.
    BuildIndexes,

    /// Show storage statistics.
    Info,
}

/// TOML config file structure.
#[derive(Debug, Default, Deserialize)]
struct Config {
    #[serde(default)]
    data_dir: Option<PathBuf>,
    #[serde(default)]
    log_level: Option<String>,
    #[serde(default)]
    partition_target_rows: Option<u64>,
}

impl Config {
    fn load(path: &PathBuf) -> Result<Self, String> {
        let contents =
            std::fs::read_to_string(path).map_err(|e| format!("failed to read config: {e}"))?;
        toml::from_str(&contents).map_err(|e| format!("failed to parse config: {e}"))
    }
}

#[tokio::main]
async fn main() {
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

    let pm_config = PartitionManagerConfig {
        data_dir: data_dir.clone(),
        partition_target_rows,
    };

    match cli.command.unwrap_or(Command::Serve {
        http_addr: "127.0.0.1:8545".parse().unwrap(),
        grpc_addr: "127.0.0.1:8546".parse().unwrap(),
    }) {
        Command::Serve {
            http_addr,
            grpc_addr,
        } => run_server(pm_config, http_addr, grpc_addr).await,
        Command::Ingest {
            rpc_url,
            from_block,
            to_block,
            build_indexes,
        } => run_ingest(pm_config, &rpc_url, from_block, to_block, build_indexes).await,
        Command::BuildIndexes => run_build_indexes(pm_config),
        Command::Info => run_info(pm_config),
    }
}

async fn run_server(config: PartitionManagerConfig, http_addr: SocketAddr, grpc_addr: SocketAddr) {
    tracing::info!(
        version = VERSION,
        data_dir = %config.data_dir.display(),
        %http_addr,
        %grpc_addr,
        "starting logex server"
    );

    let storage = match PartitionManager::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    tracing::info!(
        total_rows = storage.total_rows(),
        sealed_partitions = storage.sealed_count(),
        head_block = ?storage.head_block(),
        "storage ready"
    );

    let state = Arc::new(AppState {
        storage,
        subscriptions: Some(SubscriptionManager::new()),
    });

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    let grpc_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(e) = logex_server::grpc::serve_grpc(grpc_state, grpc_addr).await {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    let http_state = Arc::clone(&state);
    let http_handle = tokio::spawn(async move {
        if let Err(e) = logex_server::serve(http_state, http_addr, shutdown_rx).await {
            tracing::error!(error = %e, "HTTP server error");
        }
    });

    match tokio::signal::ctrl_c().await {
        Ok(()) => tracing::info!("received shutdown signal"),
        Err(e) => tracing::error!(error = %e, "failed to listen for shutdown signal"),
    }

    let _ = shutdown_tx.send(());
    let _ = http_handle.await;
    tracing::info!("logex stopped");
}

async fn run_ingest(
    config: PartitionManagerConfig,
    rpc_url: &str,
    from_block: u64,
    to_block: Option<u64>,
    build_indexes: bool,
) {
    let to_block = match to_block {
        Some(b) => b,
        None => {
            tracing::info!("fetching latest block number");
            match logex_ingestion::rpc::get_latest_block(rpc_url).await {
                Ok(n) => {
                    tracing::info!(latest = n, "resolved latest block");
                    n
                }
                Err(e) => {
                    tracing::error!(error = %e, "failed to get latest block");
                    std::process::exit(1);
                }
            }
        }
    };

    let mut pipeline = match logex_ingestion::Pipeline::open_with_config(config) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(error = %e, "failed to open pipeline");
            std::process::exit(1);
        }
    };

    match logex_ingestion::rpc::ingest_range(&mut pipeline, rpc_url, from_block, to_block).await {
        Ok(stats) => {
            tracing::info!(
                blocks = stats.blocks_ingested,
                logs = stats.logs_ingested,
                elapsed = ?stats.elapsed,
                "ingestion complete"
            );
        }
        Err(e) => {
            tracing::error!(error = %e, "ingestion failed");
            std::process::exit(1);
        }
    }

    if build_indexes {
        tracing::info!("building indexes on hot partition");
        let hot_path = &pipeline.storage().hot_partition().meta.path;
        if pipeline.storage().hot_partition().meta.row_count > 0 {
            if let Err(e) = logex_index::IndexBuilder::build_all_indexes(hot_path) {
                tracing::error!(error = %e, "failed to build indexes");
                std::process::exit(1);
            }
            tracing::info!("indexes built successfully");
        }
    }

    tracing::info!(
        total_rows = pipeline.storage().total_rows(),
        head_block = ?pipeline.storage().head_block(),
        "storage summary"
    );
}

fn run_build_indexes(config: PartitionManagerConfig) {
    let storage = match PartitionManager::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let hot_path = &storage.hot_partition().meta.path;
    if storage.hot_partition().meta.row_count == 0 {
        println!("Hot partition is empty, nothing to index");
        return;
    }

    tracing::info!(
        rows = storage.hot_partition().meta.row_count,
        "building indexes on hot partition"
    );
    if let Err(e) = logex_index::IndexBuilder::build_all_indexes(hot_path) {
        tracing::error!(error = %e, "failed to build indexes");
        std::process::exit(1);
    }
    tracing::info!("indexes built successfully");
}

fn run_info(config: PartitionManagerConfig) {
    let storage = match PartitionManager::open(config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    println!("LogEx Storage Info");
    println!("  Total rows:         {}", storage.total_rows());
    println!("  Sealed partitions:  {}", storage.sealed_count());
    println!(
        "  Head block:         {}",
        storage
            .head_block()
            .map_or("none".to_string(), |b| b.to_string())
    );
    println!(
        "  Hot partition rows:  {}",
        storage.hot_partition().meta.row_count
    );
}
