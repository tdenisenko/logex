pub mod eth_filter;
pub mod grpc;
pub mod handler;
pub mod jsonrpc;
pub mod rest;
pub mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};

pub use handler::AppState;
pub use ws::SubscriptionManager;

/// Build the axum router with all endpoints.
pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", post(handler::handle_jsonrpc))
        .route("/query", post(rest::handle_query))
        .route("/health", get(rest::handle_health))
        .route("/ws", get(ws::handle_ws_upgrade))
        .with_state(state)
}

/// Start the HTTP server on the given address with graceful shutdown support.
pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
    shutdown: tokio::sync::watch::Receiver<()>,
) -> std::io::Result<()> {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "HTTP server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await
        .map_err(std::io::Error::other)
}

async fn shutdown_signal(mut rx: tokio::sync::watch::Receiver<()>) {
    let _ = rx.changed().await;
    tracing::info!("HTTP server shutting down");
}
