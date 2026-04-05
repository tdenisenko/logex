use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

use logex_server::AppState;
use logex_storage::{PartitionManager, PartitionManagerConfig};

#[derive(Parser, Debug)]
#[command(
    name = "logex",
    about = "Fast, self-hosted Ethereum node for querying logs and token transfers with SQL"
)]
struct Cli {
    /// Path to the LogEx data directory.
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,

    /// HTTP server bind address.
    #[arg(long, default_value = "127.0.0.1:8545")]
    http_addr: SocketAddr,

    /// gRPC server bind address.
    #[arg(long, default_value = "127.0.0.1:8546")]
    grpc_addr: String,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cli.log_level.parse().unwrap_or_default()),
        )
        .init();

    tracing::info!(data_dir = %cli.data_dir.display(), "starting logex");

    let config = PartitionManagerConfig {
        data_dir: cli.data_dir.clone(),
        ..Default::default()
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
        "storage ready"
    );

    let state = Arc::new(AppState { storage });

    if let Err(e) = logex_server::serve(state, cli.http_addr).await {
        tracing::error!(error = %e, "server error");
        std::process::exit(1);
    }
}
