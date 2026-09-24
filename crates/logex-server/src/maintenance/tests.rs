use super::*;
use axum::{
    body::{Body, to_bytes},
    http::Request,
};
use base64::Engine;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tower::ServiceExt;

fn auth() -> String {
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode("logex:secret")
    )
}
fn config() -> HttpServerConfig {
    HttpServerConfig {
        dashboard_enabled: true,
        dashboard_password: Some("secret".into()),
        ..Default::default()
    }
}
async fn json(response: Response) -> serde_json::Value {
    serde_json::from_slice(&to_bytes(response.into_body(), 4096).await.unwrap()).unwrap()
}

#[tokio::test]
async fn maintenance_protects_all_data_routes_without_storage_or_request_parsing() {
    let state = Arc::new(MaintenanceState::new(RepairPhase::Inspecting));
    let router = build_maintenance_router(state, config());
    for (method, path) in [
        ("GET", "/status"),
        ("POST", "/"),
        ("POST", "/query"),
        ("POST", "/query/cancel"),
        ("GET", "/ws"),
        ("POST", "/live/erc20-transfers/subscriptions"),
        ("GET", "/live/erc20-transfers/subscriptions/test"),
        ("DELETE", "/live/erc20-transfers/subscriptions/test"),
        ("POST", "/live/erc20-transfers/subscriptions/test/clear"),
    ] {
        for credentials in [None, Some("Basic invalid"), Some("Basic d3Jvbmc6c2VjcmV0")] {
            let mut request = Request::builder().method(method).uri(path);
            if let Some(credentials) = credentials {
                request = request.header("authorization", credentials);
            }
            let response = router
                .clone()
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {path}"
            );
            assert!(response.headers().contains_key("www-authenticate"));
        }
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", auth())
                    .header("content-type", "application/json")
                    .body(Body::from("invalid JSON is never parsed"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "{method} {path}"
        );
        assert_eq!(json(response).await["status"], "repairing");
    }
}

#[tokio::test]
async fn status_tracks_progress_and_terminal_failure_but_health_hides_diagnostics() {
    let state = Arc::new(MaintenanceState::new(RepairPhase::Inspecting));
    let router = build_maintenance_router(state.clone(), config());
    state.set_phase(RepairPhase::RebuildingIndexes);
    assert_eq!(state.status().phase, RepairPhase::RebuildingIndexes);
    state.fail("private/path: <unexpected data>");
    state.set_phase(RepairPhase::Starting);
    let health = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json(health).await,
        serde_json::json!({"status":"repair_failed"})
    );
    let response = router
        .oneshot(
            Request::builder()
                .uri("/status")
                .header("authorization", auth())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        json(response).await,
        serde_json::json!({
            "status":"repair_failed", "phase":"failed", "diagnostic":"private/path: <unexpected data>"
        })
    );
}

#[tokio::test]
async fn dashboard_policy_and_passwordless_maintenance_match_normal_server() {
    for enabled in [true, false] {
        let router = build_maintenance_router(
            Arc::new(MaintenanceState::new(RepairPhase::Inspecting)),
            HttpServerConfig {
                dashboard_enabled: enabled,
                dashboard_password: None,
                ..Default::default()
            },
        );
        let response = router
            .clone()
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            if enabled {
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            }
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }
    let router = build_maintenance_router(
        Arc::new(MaintenanceState::new(RepairPhase::Inspecting)),
        config(),
    );
    let response = router
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn joined_maintenance_server_releases_listener_before_normal_startup() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(serve_maintenance(
            Arc::new(MaintenanceState::new(RepairPhase::Inspecting)),
            listener,
            receiver,
            HttpServerConfig::default(),
        ));
        let mut connection = tokio::net::TcpStream::connect(address).await.unwrap();
        connection
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        connection.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 503"));
        assert!(response.contains("repairing"));
        drop(connection);
        shutdown.send(true).unwrap();
        task.await.unwrap().unwrap();
        let rebound = TcpListener::bind(address).await.unwrap();
        drop(rebound);
    })
    .await
    .expect("finite maintenance shutdown/rebind");
}
