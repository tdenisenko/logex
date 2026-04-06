use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use serde::Deserialize;

use logex_index::IndexBuilder;
use logex_server::{AppState, SubscriptionManager};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_sync::SyncConfig;
use logex_sync::engine::SyncEngine;
use logex_sync::p2p::{discovery, peer_manager::PeerManager};
use logex_types::SyncStatus;
use reth_ethereum_forks::Head;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Parser, Debug)]
#[command(
    name = "logex",
    version = VERSION,
    about = "LogEx — standalone Ethereum light node for fast event log queries"
)]
struct Cli {
    /// Path to the LogEx data directory.
    #[arg(long, default_value = "./logex-data", global = true)]
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
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Start the node: sync blocks from the P2P network and serve queries.
    Sync {
        /// HTTP server port (Web UI + LogSQL + JSON-RPC).
        #[arg(long, default_value = "8577")]
        http_port: u16,

        /// gRPC server port.
        #[arg(long, default_value = "8578")]
        grpc_port: u16,

        /// P2P discovery port (UDP).
        #[arg(long, default_value = "30303")]
        discovery_port: u16,

        /// Maximum peer connections.
        #[arg(long, default_value = "50")]
        max_peers: usize,
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

    let pm_config = PartitionManagerConfig {
        data_dir,
        partition_target_rows,
    };

    match cli.command {
        Command::Sync {
            http_port,
            grpc_port,
            discovery_port,
            max_peers,
        } => {
            let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
            rt.block_on(run_sync(
                pm_config,
                http_port,
                grpc_port,
                discovery_port,
                max_peers,
            ));
        }
        Command::BuildIndexes => run_build_indexes(pm_config),
        Command::Info => run_info(pm_config),
    }
}

async fn run_sync(
    pm_config: PartitionManagerConfig,
    http_port: u16,
    grpc_port: u16,
    discovery_port: u16,
    max_peers: usize,
) {
    // Open storage
    let storage = match PartitionManager::open(pm_config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let head_block = storage.head_block().unwrap_or(0);
    tracing::info!(
        total_rows = storage.total_rows(),
        head_block,
        "storage ready"
    );

    // Shared state
    let state = Arc::new(AppState {
        storage: Arc::new(tokio::sync::RwLock::new(storage)),
        subscriptions: Some(SubscriptionManager::new()),
        sync_status: Arc::new(std::sync::Mutex::new(SyncStatus::default())),
    });

    // Spawn HTTP server
    let http_addr: SocketAddr = ([0, 0, 0, 0], http_port).into();
    let http_state = Arc::clone(&state);
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    tokio::spawn(async move {
        tracing::info!(%http_addr, "HTTP server starting");
        if let Err(e) = logex_server::serve(http_state, http_addr, shutdown_rx).await {
            tracing::error!(error = %e, "HTTP server error");
        }
    });

    // Spawn gRPC server
    let grpc_addr: SocketAddr = ([0, 0, 0, 0], grpc_port).into();
    let grpc_state = Arc::clone(&state);
    tokio::spawn(async move {
        tracing::info!(%grpc_addr, "gRPC server starting");
        if let Err(e) = logex_server::grpc::serve_grpc(grpc_state, grpc_addr).await {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    // Spawn background indexer
    let index_state = Arc::clone(&state);
    tokio::spawn(run_background_indexer(index_state));

    tracing::info!(
        http = %format!("http://{http_addr}"),
        grpc = %format!("http://{grpc_addr}"),
        "query endpoints ready"
    );

    // Generate node identity
    let secret_key = secp256k1::SecretKey::new(&mut rand::thread_rng());

    // Start peer discovery
    let disc = match discovery::start_discovery(secret_key, discovery_port).await {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(error = %e, "failed to start peer discovery");
            std::process::exit(1);
        }
    };

    // Create peer manager and sync engine
    let our_head = Head {
        number: head_block,
        hash: alloy_primitives::B256::ZERO,
        ..Default::default()
    };
    let peers = PeerManager::new(secret_key, disc, our_head);

    let sync_config = SyncConfig {
        max_peers,
        ..Default::default()
    };

    let mut engine = SyncEngine::new(
        sync_config,
        peers,
        Arc::clone(&state.storage),
        state.subscriptions.clone(),
        Arc::clone(&state.sync_status),
    );

    // Run sync (blocks until error or shutdown)
    if let Err(e) = engine.run().await {
        tracing::error!(error = %e, "sync engine error");
    }

    let _ = shutdown_tx.send(());
    tracing::info!("shutting down");
}

/// Background task that periodically rebuilds indexes on the hot partition.
async fn run_background_indexer(state: Arc<AppState>) {
    let mut last_indexed_rows = 0u64;

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(30)).await;

        let (hot_path, hot_rows) = {
            let storage = state.storage.read().await;
            let hp = storage.hot_partition().meta.path.clone();
            let rows = storage.hot_partition().meta.row_count;
            (hp, rows)
        };

        if hot_rows > last_indexed_rows && hot_rows > 0 {
            let path = hot_path.clone();
            match tokio::task::spawn_blocking(move || IndexBuilder::build_all_indexes(&path)).await
            {
                Ok(Ok(())) => {
                    tracing::debug!(rows = hot_rows, "indexes rebuilt");
                    last_indexed_rows = hot_rows;
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "failed to build indexes");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "index build task panicked");
                }
            }
        }
    }
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
    if let Err(e) = IndexBuilder::build_all_indexes(hot_path) {
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
