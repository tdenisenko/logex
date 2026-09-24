//! HTTP visibility before storage can safely be opened. No storage handle or
//! query/subscription service is constructed by this module.
use std::sync::{Arc, Mutex};

use axum::{
    Router,
    extract::State,
    http::StatusCode,
    middleware,
    response::{IntoResponse, Json, Response},
    routing::{any, get},
};
use serde::Serialize;
use tokio::{net::TcpListener, sync::watch};

use crate::{HttpServerConfig, require_http_access, rest, shutdown_signal};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepairPhase {
    Inspecting,
    Recovering,
    RebuildingIndexes,
    Reconstructing,
    Verifying,
    Starting,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceStatusKind {
    Repairing,
    RepairFailed,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaintenanceStatus {
    pub status: MaintenanceStatusKind,
    pub phase: RepairPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diagnostic: Option<String>,
}

#[derive(Debug)]
pub struct MaintenanceState {
    status: Mutex<MaintenanceStatus>,
}

impl MaintenanceState {
    pub fn new(phase: RepairPhase) -> Self {
        Self {
            status: Mutex::new(MaintenanceStatus {
                status: if phase == RepairPhase::Failed {
                    MaintenanceStatusKind::RepairFailed
                } else {
                    MaintenanceStatusKind::Repairing
                },
                phase,
                diagnostic: None,
            }),
        }
    }

    /// Failure is terminal for this attempt; a delayed progress update cannot
    /// hide its diagnostic or make an unsuccessful repair appear healthy.
    pub fn set_phase(&self, phase: RepairPhase) {
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if status.status != MaintenanceStatusKind::RepairFailed {
            status.phase = phase;
            if phase == RepairPhase::Failed {
                status.status = MaintenanceStatusKind::RepairFailed;
            }
        }
    }

    pub fn fail(&self, diagnostic: impl Into<String>) {
        let diagnostic = diagnostic.into();
        let mut status = self
            .status
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        status.status = MaintenanceStatusKind::RepairFailed;
        status.phase = RepairPhase::Failed;
        status.diagnostic = Some(diagnostic);
    }

    pub fn status(&self) -> MaintenanceStatus {
        self.status
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
}

async fn status(State(state): State<Arc<MaintenanceState>>) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(state.status())).into_response()
}

async fn health(State(state): State<Arc<MaintenanceState>>) -> Response {
    // Health is public, like normal startup. Detailed diagnostics stay behind
    // dashboard authentication and may contain local paths or error context.
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({ "status": state.status().status })),
    )
        .into_response()
}

/// Maintenance mirrors normal HTTP authentication, origin and dashboard policy, but
/// every data route terminates here without reading bodies or upgrading sockets.
pub fn build_maintenance_router(state: Arc<MaintenanceState>, config: HttpServerConfig) -> Router {
    let root = if config.dashboard_enabled {
        get(rest::handle_web_ui).post(status)
    } else {
        get(rest::handle_dashboard_disabled).post(status)
    };
    let protected = Router::new()
        .route("/", root)
        .route("/status", get(status))
        .route("/query", any(status))
        .route("/query/cancel", any(status))
        .route("/ws", any(status))
        .route("/live/erc20-transfers/subscriptions", any(status))
        .route("/live/erc20-transfers/subscriptions/{id}", any(status))
        .route(
            "/live/erc20-transfers/subscriptions/{id}/clear",
            any(status),
        )
        .route_layer(middleware::from_fn_with_state(
            Arc::new(config),
            require_http_access,
        ))
        .with_state(Arc::clone(&state));
    protected.merge(
        Router::new()
            .route("/health", get(health))
            .with_state(state),
    )
}

/// Serve an already-bound listener so bind failures precede maintenance work.
/// Signal shutdown and join this future before normal startup binds the address.
/// The caller supervises failures and bounds graceful connection-drain time.
pub async fn serve_maintenance(
    state: Arc<MaintenanceState>,
    listener: TcpListener,
    shutdown: watch::Receiver<bool>,
    config: HttpServerConfig,
) -> std::io::Result<()> {
    axum::serve(listener, build_maintenance_router(state, config))
        .with_graceful_shutdown(shutdown_signal(shutdown))
        .await
        .map_err(std::io::Error::other)
}

#[cfg(test)]
mod tests;
