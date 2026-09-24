#![recursion_limit = "256"]

pub mod eth_filter;
pub mod grpc;
pub mod handler;
pub mod jsonrpc;
mod maintenance;
mod origin;
mod query_encoding;
mod query_response;
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

pub use handler::{AppState, QueryConcurrencyLimit};
pub use logex_types::QueryMemoryLimit;
pub use maintenance::{
    MaintenanceState, MaintenanceStatus, MaintenanceStatusKind, RepairPhase,
    build_maintenance_router, serve_maintenance,
};
pub use origin::BrowserOrigin;
pub use ws::SubscriptionManager;

#[derive(Clone, PartialEq, Eq)]
pub struct HttpServerConfig {
    pub dashboard_enabled: bool,
    pub dashboard_password: Option<String>,
    /// Explicit browser origins. Empty permits only the direct HTTP origin.
    pub allowed_origins: Vec<BrowserOrigin>,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            dashboard_enabled: true,
            dashboard_password: None,
            allowed_origins: Vec::new(),
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
        .route("/query/cancel", post(rest::handle_query_cancel))
        .route("/ws", get(ws::handle_ws_upgrade))
        .route(
            "/live/erc20-transfers/subscriptions",
            post(ws::handle_live_transfer_subscribe),
        )
        .route(
            "/live/erc20-transfers/subscriptions/{id}",
            get(ws::handle_live_transfer_get).delete(ws::handle_live_transfer_delete),
        )
        .route(
            "/live/erc20-transfers/subscriptions/{id}/clear",
            post(ws::handle_live_transfer_clear),
        )
        .route_layer(middleware::from_fn_with_state(
            Arc::new(config),
            require_http_access,
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
    let app = build_router_with_config(Arc::clone(&state), config);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "HTTP server listening");
    let shutdown_state = Arc::clone(&state);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown_signal(shutdown).await;
            shutdown_state.query_control.cancel_active();
        })
        .await
        .map_err(std::io::Error::other)
}

async fn require_http_access(
    axum::extract::State(config): axum::extract::State<Arc<HttpServerConfig>>,
    request: Request,
    next: Next,
) -> Response {
    let headers = request.headers();
    if let Some(password) = config.dashboard_password.as_deref()
        && !dashboard_auth_is_valid(headers, password)
    {
        let mut response = (StatusCode::UNAUTHORIZED, "authentication required").into_response();
        response.headers_mut().insert(
            WWW_AUTHENTICATE,
            HeaderValue::from_static("Basic realm=\"LogEx\", charset=\"UTF-8\""),
        );
        return response;
    }
    if !origin::request_origin_is_allowed(headers, request.uri(), &config.allowed_origins) {
        return (StatusCode::FORBIDDEN, "browser origin is not allowed").into_response();
    }
    next.run(request).await
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
