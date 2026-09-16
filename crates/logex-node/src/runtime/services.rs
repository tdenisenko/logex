use std::net::SocketAddr;
use std::sync::Arc;

use logex_server::{AppState, HttpServerConfig};
use logex_sync::tasks::TaskMonitor;
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub(super) fn spawn_http(
    workers: &TaskMonitor,
    state: Arc<AppState>,
    addr: SocketAddr,
    shutdown: watch::Receiver<bool>,
    config: HttpServerConfig,
) -> JoinHandle<()> {
    workers.spawn_result("HTTP server", async move {
        tracing::info!(%addr, "HTTP server starting");
        logex_server::serve_with_config(state, addr, shutdown, config).await
    })
}

pub(super) fn spawn_grpc(
    workers: &TaskMonitor,
    state: Arc<AppState>,
    addr: SocketAddr,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<()> {
    workers.spawn_result("gRPC server", async move {
        tracing::info!(%addr, "gRPC server starting");
        logex_server::grpc::serve_grpc(state, addr, shutdown).await
    })
}
