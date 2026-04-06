use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
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
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// HTTP server bind address (JSON-RPC + REST + WebSocket).
    #[arg(long, default_value = "127.0.0.1:8545")]
    http_addr: SocketAddr,

    /// gRPC server bind address.
    #[arg(long, default_value = "127.0.0.1:8546")]
    grpc_addr: SocketAddr,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Target rows per partition before sealing.
    #[arg(long, default_value = "50000000")]
    partition_target_rows: u64,

    /// Path to optional TOML config file.
    #[arg(long)]
    config: Option<PathBuf>,
}

/// TOML config file structure. CLI args take precedence.
#[derive(Debug, Default, Deserialize)]
struct Config {
    #[serde(default)]
    data_dir: Option<PathBuf>,
    #[serde(default)]
    http_addr: Option<SocketAddr>,
    #[serde(default)]
    grpc_addr: Option<SocketAddr>,
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

    // Load config file if specified (CLI args override config values)
    let file_config = cli.config.as_ref().map(Config::load);
    if let Some(Err(e)) = &file_config {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
    let file_config = file_config.and_then(|r| r.ok()).unwrap_or_default();

    let log_level = if cli.log_level != "info" {
        cli.log_level.clone()
    } else {
        file_config.log_level.unwrap_or_else(|| cli.log_level.clone())
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| log_level.parse().unwrap_or_default()),
        )
        .init();

    let data_dir = file_config.data_dir.unwrap_or(cli.data_dir);
    let http_addr = file_config.http_addr.unwrap_or(cli.http_addr);
    let grpc_addr = file_config.grpc_addr.unwrap_or(cli.grpc_addr);
    let partition_target_rows = file_config
        .partition_target_rows
        .unwrap_or(cli.partition_target_rows);

    tracing::info!(
        version = VERSION,
        data_dir = %data_dir.display(),
        %http_addr,
        %grpc_addr,
        "starting logex"
    );

    let config = PartitionManagerConfig {
        data_dir,
        partition_target_rows,
    };

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

    // Shutdown signal
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());

    // Start gRPC server in background
    let grpc_state = Arc::clone(&state);
    tokio::spawn(async move {
        if let Err(e) = logex_server::grpc::serve_grpc(grpc_state, grpc_addr).await {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    // Start HTTP server in background
    let http_state = Arc::clone(&state);
    let http_handle = tokio::spawn(async move {
        if let Err(e) = logex_server::serve(http_state, http_addr, shutdown_rx).await {
            tracing::error!(error = %e, "HTTP server error");
        }
    });

    // Wait for Ctrl+C
    match tokio::signal::ctrl_c().await {
        Ok(()) => {
            tracing::info!("received shutdown signal");
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to listen for shutdown signal");
        }
    }

    // Trigger graceful shutdown
    let _ = shutdown_tx.send(());
    let _ = http_handle.await;

    tracing::info!("logex stopped");
}
