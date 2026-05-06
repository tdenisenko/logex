#![recursion_limit = "256"]

pub mod eth_filter;
pub mod grpc;
pub mod handler;
pub mod jsonrpc;
pub mod rest;
mod storage_metrics;
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
        .route("/", get(rest::handle_web_ui).post(handler::handle_jsonrpc))
        .route("/status", get(rest::handle_status))
        .route("/query", post(rest::handle_query))
        .route("/health", get(rest::handle_health))
        .route("/ws", get(ws::handle_ws_upgrade))
        .with_state(state)
}

/// Start the HTTP server on the given address with graceful shutdown support.
pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "HTTP server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await
        .map_err(std::io::Error::other)
}

async fn shutdown_signal(mut rx: tokio::sync::watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            break;
        }
    }
    tracing::info!("HTTP server shutting down");
}
