use clap::Parser;
use std::path::PathBuf;

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
    http_addr: String,

    /// gRPC server bind address.
    #[arg(long, default_value = "127.0.0.1:8546")]
    grpc_addr: String,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, default_value = "info")]
    log_level: String,
}

fn main() {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| cli.log_level.parse().unwrap_or_default()),
        )
        .init();

    tracing::info!(data_dir = %cli.data_dir.display(), "starting logex");
    tracing::info!("logex is not yet fully implemented — scaffold only");
}
