use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use serde::Deserialize;

use logex_index::IndexBuilder;
use logex_server::{AppState, SubscriptionManager};
use logex_storage::{PartitionManager, PartitionManagerConfig, SyncHead};
use logex_sync::SyncConfig;
use logex_sync::engine::SyncEngine;
use logex_sync::p2p::{
    peer_manager::PeerManager,
    persistence::{
        discovery_secret_path, known_peers_path, load_known_peers, load_or_create_secret_key,
        persist_known_peers,
    },
};
use logex_types::SyncStatus;
use reth_chainspec::{EthChainSpec, MAINNET};
use reth_ethereum_forks::Head;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

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

        /// P2P listener port (TCP).
        #[arg(long, default_value = "30303")]
        p2p_port: u16,

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
            p2p_port,
            max_peers,
        } => {
            let rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");
            rt.block_on(run_sync(
                pm_config,
                http_port,
                grpc_port,
                discovery_port,
                p2p_port,
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
    p2p_port: u16,
    max_peers: usize,
) {
    let data_dir = pm_config.data_dir.clone();
    let discovery_secret_file = discovery_secret_path(&data_dir);
    let known_peers_file = known_peers_path(&data_dir);

    // Open storage
    let storage = match PartitionManager::open(pm_config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let sync_head = storage.sync_head();
    let head_block = storage.head_block().unwrap_or(0);
    let indexed_head_block = storage.indexed_head_block();
    let resume_block = sync_head
        .map(|head| head.block_number)
        .or(indexed_head_block)
        .unwrap_or(0);
    tracing::info!(
        total_rows = storage.total_rows(),
        head_block,
        indexed_head_block,
        sync_head_block = sync_head.map(|head| head.block_number),
        "storage ready"
    );

    // Shared state
    let state = Arc::new(AppState {
        storage: Arc::new(tokio::sync::RwLock::new(storage)),
        subscriptions: Some(SubscriptionManager::new()),
        sync_status: Arc::new(std::sync::Mutex::new(initial_sync_status(resume_block))),
    });

    let known_peers = match load_known_peers(&known_peers_file) {
        Ok(peers) => peers,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %known_peers_file.display(),
                "failed to load known peers, starting with an empty peer cache"
            );
            Vec::new()
        }
    };
    tracing::info!(
        peers = known_peers.len(),
        path = %known_peers_file.display(),
        "loaded known peers"
    );

    let secret_key = match load_or_create_secret_key(&discovery_secret_file) {
        Ok(secret) => secret,
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %discovery_secret_file.display(),
                "failed to load discovery secret"
            );
            std::process::exit(1);
        }
    };
    tracing::info!(
        path = %discovery_secret_file.display(),
        "loaded discovery secret"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn HTTP server
    let http_addr: SocketAddr = ([0, 0, 0, 0], http_port).into();
    let http_state = Arc::clone(&state);
    let http_shutdown = shutdown_rx.clone();
    let http_handle = tokio::spawn(async move {
        tracing::info!(%http_addr, "HTTP server starting");
        if let Err(e) = logex_server::serve(http_state, http_addr, http_shutdown).await {
            tracing::error!(error = %e, "HTTP server error");
        }
    });

    // Spawn gRPC server
    let grpc_addr: SocketAddr = ([0, 0, 0, 0], grpc_port).into();
    let grpc_state = Arc::clone(&state);
    let grpc_shutdown = shutdown_rx.clone();
    let grpc_handle = tokio::spawn(async move {
        tracing::info!(%grpc_addr, "gRPC server starting");
        if let Err(e) = logex_server::grpc::serve_grpc(grpc_state, grpc_addr, grpc_shutdown).await {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    // Spawn background indexer
    let index_state = Arc::clone(&state);
    let index_shutdown = shutdown_rx.clone();
    let index_handle = tokio::spawn(run_background_indexer(index_state, index_shutdown));

    tracing::info!(
        http = %format!("http://{http_addr}"),
        grpc = %format!("http://{grpc_addr}"),
        "query endpoints ready"
    );

    // Start the network from the latest persisted sync head when storage has a
    // complete block hash + timestamp. Legacy metadata without timestamps falls
    // back to genesis until the next verified block updates the live status.
    let our_head = startup_network_head(sync_head);
    let peers = match PeerManager::new(
        secret_key,
        p2p_port,
        discovery_port,
        max_peers,
        our_head,
        known_peers,
        known_peers_file.clone(),
    )
    .await
    {
        Ok(peers) => peers,
        Err(e) => {
            tracing::error!(error = %e, "failed to start p2p networking");
            std::process::exit(1);
        }
    };

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
        shutdown_rx.clone(),
    );

    // Run sync until completion or an external shutdown signal arrives.
    let engine_result = {
        let mut engine_run = pin!(engine.run());
        tokio::select! {
            res = &mut engine_run => res,
            signal = wait_for_shutdown_signal() => {
                tracing::info!(signal, "shutdown requested, stopping node gracefully");
                let _ = shutdown_tx.send(true);
                engine_run.await
            }
        }
    };

    if let Err(e) = engine_result {
        tracing::error!(error = %e, "sync engine error");
    }

    engine.shutdown().await;

    let known_peers = engine.known_peers();
    if let Err(e) = persist_known_peers(&known_peers_file, &known_peers) {
        tracing::warn!(
            error = %e,
            path = %known_peers_file.display(),
            "failed to persist known peers"
        );
    } else {
        tracing::info!(
            peers = known_peers.len(),
            path = %known_peers_file.display(),
            "persisted known peers"
        );
    }

    let _ = shutdown_tx.send(true);
    tracing::info!("waiting for HTTP, gRPC, and indexing tasks to stop");
    log_task_exit("HTTP server", http_handle).await;
    log_task_exit("gRPC server", grpc_handle).await;
    log_task_exit("background indexer", index_handle).await;
    tracing::info!("shutting down");
}

fn initial_sync_status(resume_block: u64) -> SyncStatus {
    SyncStatus {
        current_block: resume_block,
        // The persisted sync head is our local resume point, not evidence of
        // the network tip. Keep the target unknown until a peer advertises a
        // credible latest block.
        target_block: 0,
        ..Default::default()
    }
}

/// Background task that periodically rebuilds indexes on the hot partition.
async fn run_background_indexer(
    state: Arc<AppState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut last_indexed: Option<HotIndexState> = None;
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = wait_for_shutdown(&mut shutdown) => {
                tracing::info!("background indexer shutting down");
                break;
            }
            _ = ticker.tick() => {}
        }

        let current = {
            let storage = state.storage.read().await;
            HotIndexState {
                partition_id: storage.hot_partition().meta.id,
                row_count: storage.hot_partition().meta.row_count,
                path: storage.hot_partition().meta.path.clone(),
            }
        };

        if should_rebuild_hot_indexes(last_indexed.as_ref(), &current) {
            let path = current.path.clone();
            match tokio::task::spawn_blocking(move || IndexBuilder::build_all_indexes(&path)).await
            {
                Ok(Ok(())) => {
                    tracing::debug!(
                        partition_id = current.partition_id,
                        rows = current.row_count,
                        "indexes rebuilt"
                    );
                    last_indexed = Some(current);
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

fn startup_network_head(sync_head: Option<SyncHead>) -> Head {
    match sync_head {
        Some(head) if head.block_number == 0 || head.timestamp > 0 => Head {
            number: head.block_number,
            hash: head.block_hash,
            timestamp: if head.block_number == 0 {
                MAINNET.genesis().timestamp
            } else {
                head.timestamp
            },
            ..Default::default()
        },
        Some(head) => {
            tracing::warn!(
                block_number = head.block_number,
                "sync metadata is missing the block timestamp, starting network status from genesis until a new verified block updates it"
            );
            genesis_network_head()
        }
        None => genesis_network_head(),
    }
}

fn genesis_network_head() -> Head {
    Head {
        number: 0,
        hash: MAINNET.genesis_hash(),
        timestamp: MAINNET.genesis().timestamp,
        ..Default::default()
    }
}

async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("failed to install SIGINT handler");
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = interrupt.recv() => "SIGINT",
            _ = terminate.recv() => "SIGTERM",
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

async fn wait_for_shutdown(shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

async fn log_task_exit(name: &str, handle: tokio::task::JoinHandle<()>) {
    let mut handle = handle;
    match tokio::time::timeout(TASK_SHUTDOWN_TIMEOUT, &mut handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(task = name, error = %e, "task exited unexpectedly");
        }
        Err(_) => {
            tracing::warn!(
                task = name,
                ?TASK_SHUTDOWN_TIMEOUT,
                "task did not stop in time, aborting it"
            );
            handle.abort();
            let _ = handle.await;
        }
    }
}

#[derive(Debug, Clone)]
struct HotIndexState {
    partition_id: u64,
    row_count: u64,
    path: PathBuf,
}

fn should_rebuild_hot_indexes(
    last_indexed: Option<&HotIndexState>,
    current: &HotIndexState,
) -> bool {
    if current.row_count == 0 {
        return false;
    }

    match last_indexed {
        None => true,
        Some(last) if last.partition_id != current.partition_id => true,
        Some(last) => current.row_count > last.row_count,
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
        "  Synced head:        {}",
        storage
            .head_block()
            .map_or("none".to_string(), |b| b.to_string())
    );
    println!(
        "  Indexed head:       {}",
        storage
            .indexed_head_block()
            .map_or("none".to_string(), |b| b.to_string())
    );
    println!(
        "  Hot partition rows:  {}",
        storage.hot_partition().meta.row_count
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebuild_hot_indexes_when_partition_rotates() {
        let last = HotIndexState {
            partition_id: 1,
            row_count: 150,
            path: PathBuf::from("/tmp/old"),
        };
        let current = HotIndexState {
            partition_id: 2,
            row_count: 1,
            path: PathBuf::from("/tmp/new"),
        };

        assert!(should_rebuild_hot_indexes(Some(&last), &current));
    }

    #[test]
    fn test_skip_rebuild_when_hot_partition_is_unchanged() {
        let last = HotIndexState {
            partition_id: 7,
            row_count: 200,
            path: PathBuf::from("/tmp/hot"),
        };
        let current = HotIndexState {
            partition_id: 7,
            row_count: 200,
            path: PathBuf::from("/tmp/hot"),
        };

        assert!(!should_rebuild_hot_indexes(Some(&last), &current));
    }

    #[test]
    fn initial_sync_status_does_not_treat_resume_block_as_network_target() {
        let status = initial_sync_status(83_714);

        assert_eq!(status.current_block, 83_714);
        assert_eq!(status.target_block, 0);
    }
}
