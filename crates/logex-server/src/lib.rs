pub mod eth_filter;
pub mod grpc;
pub mod handler;
pub mod jsonrpc;
pub mod rest;
pub mod ws;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, routing::{get, post}};

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

/// Start the HTTP server on the given address.
pub async fn serve(state: Arc<AppState>, addr: SocketAddr) -> std::io::Result<()> {
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "JSON-RPC server listening");
    axum::serve(listener, app)
        .await
        .map_err(std::io::Error::other)
}
