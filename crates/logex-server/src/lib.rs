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

use axum::extract::Request;
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::{
    Router,
    routing::{get, post},
};
use base64::Engine;

pub use handler::AppState;
pub use ws::SubscriptionManager;

#[derive(Clone, PartialEq, Eq)]
pub struct HttpServerConfig {
    pub dashboard_enabled: bool,
    pub dashboard_password: Option<String>,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            dashboard_enabled: true,
            dashboard_password: None,
        }
    }
}

/// Build the axum router with all endpoints.
pub fn build_router(state: Arc<AppState>) -> Router {
    build_router_with_config(state, HttpServerConfig::default())
}

/// Build the axum router with dashboard/security options.
pub fn build_router_with_config(state: Arc<AppState>, config: HttpServerConfig) -> Router {
    let root = if config.dashboard_enabled {
        get(rest::handle_web_ui).post(handler::handle_jsonrpc)
    } else {
        get(rest::handle_dashboard_disabled).post(handler::handle_jsonrpc)
    };

    let protected = Router::new()
        .route("/", root)
        .route("/status", get(rest::handle_status))
        .route("/query", post(rest::handle_query))
        .route("/ws", get(ws::handle_ws_upgrade))
        .route_layer(middleware::from_fn_with_state(
            config.clone(),
            require_dashboard_auth,
        ))
        .with_state(Arc::clone(&state));

    let public = Router::new()
        .route("/health", get(rest::handle_health))
        .with_state(state);

    protected.merge(public)
}

/// Start the HTTP server on the given address with graceful shutdown support.
pub async fn serve(
    state: Arc<AppState>,
    addr: SocketAddr,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> std::io::Result<()> {
    serve_with_config(state, addr, shutdown, HttpServerConfig::default()).await
}

/// Start the HTTP server with dashboard/security options.
pub async fn serve_with_config(
    state: Arc<AppState>,
    addr: SocketAddr,
    shutdown: tokio::sync::watch::Receiver<bool>,
    config: HttpServerConfig,
) -> std::io::Result<()> {
    let app = build_router_with_config(state, config);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "HTTP server listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await
        .map_err(std::io::Error::other)
}

async fn require_dashboard_auth(
    axum::extract::State(config): axum::extract::State<HttpServerConfig>,
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let Some(password) = config.dashboard_password.as_deref() else {
        return next.run(request).await;
    };

    if dashboard_auth_is_valid(&headers, password) {
        return next.run(request).await;
    }

    let mut response = (StatusCode::UNAUTHORIZED, "authentication required").into_response();
    response.headers_mut().insert(
        WWW_AUTHENTICATE,
        HeaderValue::from_static("Basic realm=\"LogEx\", charset=\"UTF-8\""),
    );
    response
}

fn dashboard_auth_is_valid(headers: &HeaderMap, password: &str) -> bool {
    let Some(value) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(encoded) = value.strip_prefix("Basic ") else {
        return false;
    };
    let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(encoded) else {
        return false;
    };
    let Ok(credentials) = std::str::from_utf8(&decoded) else {
        return false;
    };
    let Some((username, supplied_password)) = credentials.split_once(':') else {
        return false;
    };

    username == "logex" && supplied_password == password
}

async fn shutdown_signal(mut rx: tokio::sync::watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            break;
        }
    }
    tracing::info!("HTTP server shutting down");
}
