use std::future::Future;
use std::sync::Arc;
use std::task::Poll;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use logex_server::{AppState, HttpServerConfig};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::SyncStatus;
use serde_json::{Value, json};
use tower::ServiceExt;

fn setup() -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    })
    .unwrap();
    (
        dir,
        Arc::new(AppState::new(storage, None, SyncStatus::default())),
    )
}

fn request(body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .body(body.into())
        .unwrap()
}

async fn bytes(response: Response) -> (StatusCode, Vec<u8>) {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
        .await
        .unwrap();
    (status, body.to_vec())
}

async fn rpc(app: axum::Router, payload: Value) -> Value {
    let response = app.oneshot(request(payload.to_string())).await.unwrap();
    let (status, body) = bytes(response).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    serde_json::from_slice(&body).unwrap()
}

#[tokio::test]
async fn invalid_envelopes_use_protocol_errors() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for payload in [
        json!({"jsonrpc":"1.0","method":"net_version","id":1}),
        json!({"jsonrpc":2,"method":"net_version","id":1}),
        json!({"jsonrpc":"2.0","method":"net_version","id":{}}),
        json!({"jsonrpc":"2.0","method":"net_version","id":[]}),
        json!({"jsonrpc":"2.0","method":"net_version","id":true}),
        json!({"jsonrpc":"2.0","method":"net_version","params":null,"id":1}),
        json!({"jsonrpc":"2.0","method":"net_version","params":false,"id":1}),
        json!({"jsonrpc":"2.0","id":1}),
        json!({"jsonrpc":"2.0","method":false}),
        json!(5),
        json!([]),
    ] {
        let response = rpc(app.clone(), payload.clone()).await;
        assert_eq!(response["error"]["code"], -32600, "{payload}");
        assert_eq!(response["id"], Value::Null);
        assert_eq!(response["jsonrpc"], "2.0");
        assert!(response.get("result").is_none());
    }
    let (status, body) = bytes(app.oneshot(request("{")).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["error"]["code"], -32700);
    assert_eq!(response["id"], Value::Null);
}

#[tokio::test]
async fn notifications_are_silent_but_explicit_null_ids_respond() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for (method, params) in [
        ("net_version", json!([])),
        ("web3_clientVersion", json!({})),
        ("eth_getLogs", json!([{}])),
        ("eth_getLogs", json!([])),
        ("unknown", json!([])),
    ] {
        let response = app
            .clone()
            .oneshot(request(
                json!({
                    "jsonrpc":"2.0", "method":method, "params":params
                })
                .to_string(),
            ))
            .await
            .unwrap();
        let (status, body) = bytes(response).await;
        assert_eq!(
            status,
            StatusCode::NO_CONTENT,
            "{method}: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(body.is_empty());
    }
    for id in [json!(null), json!("request"), json!(-2), json!(1.5)] {
        let response = rpc(
            app.clone(),
            json!({"jsonrpc":"2.0","method":"net_version","id":id}),
        )
        .await;
        assert_eq!(response["id"], id);
        assert_eq!(response["result"], "1");
    }
}

#[tokio::test]
async fn method_argument_errors_are_invalid_params() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for (method, params) in [
        ("eth_getLogs", json!([])),
        ("eth_getLogs", json!({})),
        ("eth_getLogs", json!([5])),
        ("eth_getLogs", json!([{}, {}])),
        ("eth_getLogs", json!([{"limit":10001}])),
        ("eth_getLogs", json!([{"offset":10000}])),
        (
            "eth_getLogs",
            json!([{"blockHash":format!("0x{}", "00".repeat(32)),"fromBlock":"0x1"}]),
        ),
        ("eth_blockNumber", json!([{}])),
        ("net_version", json!({"extra":1})),
        ("web3_clientVersion", json!([1])),
    ] {
        let response = rpc(
            app.clone(),
            json!({"jsonrpc":"2.0","method":method,"params":params,"id":"args"}),
        )
        .await;
        assert_eq!(response["error"]["code"], -32602, "{method}: {response}");
        assert_eq!(response["id"], "args");
    }
}

#[tokio::test]
async fn malformed_method_arguments_do_not_wait_for_storage() {
    let (_dir, state) = setup();
    let writer = state.storage.write().await;
    let mut response = Box::pin(
        logex_server::build_router(Arc::clone(&state)).oneshot(request(
            json!({"jsonrpc":"2.0","method":"eth_getLogs","params":[],"id":1}).to_string(),
        )),
    );
    let first = std::future::poll_fn(|cx| Poll::Ready(response.as_mut().poll(cx))).await;
    drop(writer);
    assert!(first.is_ready(), "invalid arguments waited for storage");
}

#[tokio::test]
async fn transport_auth_and_unsupported_batches_remain_explicit() {
    let (_dir, state) = setup();
    for dashboard_enabled in [true, false] {
        let app = logex_server::build_router_with_config(
            Arc::clone(&state),
            HttpServerConfig {
                dashboard_enabled,
                dashboard_password: Some("secret".into()),
            },
        );
        let response = app.oneshot(request("{")).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let mut valid = request(json!({"jsonrpc":"2.0","method":"net_version","id":1}).to_string());
        valid
            .headers_mut()
            .insert("authorization", "Basic bG9nZXg6c2VjcmV0".parse().unwrap());
        let app = logex_server::build_router_with_config(
            Arc::clone(&state),
            HttpServerConfig {
                dashboard_enabled,
                dashboard_password: Some("secret".into()),
            },
        );
        assert_eq!(app.oneshot(valid).await.unwrap().status(), StatusCode::OK);
    }
    let app = logex_server::build_router(state);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let response = app
        .clone()
        .layer(axum::extract::DefaultBodyLimit::max(4))
        .oneshot(request("12345"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let response = app
        .oneshot(request(
            json!([{"jsonrpc":"2.0","method":"net_version","id":1}]).to_string(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn numeric_request_ids_are_echoed_without_rounding() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for id in [
        "18446744073709551617",
        "0.1234567890123456789012345",
        "1e400",
        "-0",
    ] {
        let payload = format!(r#"{{"jsonrpc":"2.0","method":"net_version","id":{id}}}"#);
        let (status, body) = bytes(app.clone().oneshot(request(payload)).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let response = String::from_utf8(body).unwrap();
        assert!(
            response.contains(&format!("\"id\":{id}")),
            "numeric id changed: {response}"
        );
    }
}

#[tokio::test]
async fn valid_no_argument_methods_and_unknown_method_keep_their_results() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for method in ["net_version", "web3_clientVersion", "eth_blockNumber"] {
        for params in [None, Some(json!([])), Some(json!({}))] {
            let mut payload = json!({"jsonrpc":"2.0","method":method,"id":7});
            if let Some(params) = params {
                payload["params"] = params;
            }
            let response = rpc(app.clone(), payload).await;
            assert!(response.get("error").is_none(), "{response}");
            assert!(response["result"].is_string());
        }
    }
    let response = rpc(
        app,
        json!({"jsonrpc":"2.0","method":"unknown","params":{"unknown":true},"id":7}),
    )
    .await;
    assert_eq!(response["error"]["code"], -32601);
    assert_eq!(response["id"], 7);
}

#[tokio::test]
async fn parameter_validation_precedes_storage_failure_and_execution_failures_stay_internal() {
    let (_dir, state) = setup();
    state.mark_storage_unavailable("fixture storage failure");
    let app = logex_server::build_router(state);
    let response = rpc(
        app.clone(),
        json!({"jsonrpc":"2.0","method":"eth_getLogs","params":[],"id":1}),
    )
    .await;
    assert_eq!(response["error"]["code"], -32602);
    for method in ["eth_getLogs", "eth_blockNumber"] {
        let params = if method == "eth_getLogs" {
            json!([{}])
        } else {
            json!([])
        };
        let response = rpc(
            app.clone(),
            json!({"jsonrpc":"2.0","method":method,"params":params,"id":1}),
        )
        .await;
        assert_eq!(response["error"]["code"], -32603);
        assert_eq!(response["error"]["message"], "fixture storage failure");
    }
    let (status, body) = bytes(
        app.oneshot(request(
            json!({"jsonrpc":"2.0","method":"eth_getLogs","params":[{}]}).to_string(),
        ))
        .await
        .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert!(body.is_empty());
}

#[tokio::test]
async fn unknown_members_and_recursive_parameter_limits_use_normal_json_parsing() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    let response = rpc(
        app.clone(),
        json!({"jsonrpc":"2.0","method":"net_version","id":1,"extension":{"accepted":true}}),
    )
    .await;
    assert_eq!(response["result"], "1");
    // A finite small fixture exceeds the parser's existing depth, not a new limit.
    let nested = format!("{}0{}", "[".repeat(130), "]".repeat(130));
    let payload = format!(r#"{{"jsonrpc":"2.0","method":"eth_getLogs","params":{nested},"id":1}}"#);
    let (status, body) = bytes(app.oneshot(request(payload)).await.unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    let response: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["error"]["code"], -32700);
}

#[tokio::test]
async fn method_and_version_require_wire_strings() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for payload in [
        r#"{"jsonrpc":{"$serde_json::private::RawValue":"\"2.0\""},"method":"net_version","id":1}"#,
        r#"{"jsonrpc":"2.0","method":{"$serde_json::private::RawValue":"\"net_version\""},"id":1}"#,
    ] {
        let (status, body) = bytes(app.clone().oneshot(request(payload)).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let response: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            response["error"]["code"], -32600,
            "wire object became a string: {response}"
        );
        assert_eq!(response["id"], Value::Null);
    }
}

#[tokio::test]
async fn literal_params_objects_cannot_become_positional_arrays() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for (method, embedded) in [("net_version", "[]"), ("eth_getLogs", "[{}]")] {
        let payload = format!(
            r#"{{"jsonrpc":"2.0","method":"{method}","params":{{"$serde_json::private::RawValue":"{embedded}"}},"id":1}}"#
        );
        let (status, body) = bytes(app.clone().oneshot(request(payload)).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let response: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            response["error"]["code"], -32602,
            "literal params shape changed: {response}"
        );
    }
}

#[tokio::test]
async fn malformed_complete_documents_are_parse_errors_despite_early_invalid_shapes() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for payload in [
        r#"{"jsonrpc":false,"#,
        r#"{"jsonrpc":"2.0","method":false,"params":["#,
        "true trailing",
        r#"{"jsonrpc":"2.0","jsonrpc":"2.0","method":"net_version","id":1} trailing"#,
    ] {
        let (status, body) = bytes(app.clone().oneshot(request(payload)).await.unwrap()).await;
        assert_eq!(status, StatusCode::OK);
        let response: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            response["error"]["code"], -32700,
            "syntax hidden by early shape error: {response}"
        );
        assert_eq!(response["id"], Value::Null);
    }
}

#[tokio::test]
async fn duplicate_fields_and_wrong_wire_shapes_are_consumed_before_rejection() {
    let (_dir, state) = setup();
    let app = logex_server::build_router(state);
    for field in [
        r#""jsonrpc":"2.0","jsonrpc":"2.0","method":"net_version","id":1"#,
        r#""jsonrpc":"2.0","method":"net_version","method":"net_version","id":1"#,
        r#""jsonrpc":"2.0","method":"net_version","params":[],"params":[],"id":1"#,
        r#""jsonrpc":"2.0","method":"net_version","id":1,"id":1"#,
        r#""jsonrpc":{},"method":"net_version","id":1"#,
        r#""jsonrpc":"2.0","method":[],"id":1"#,
        r#""jsonrpc":"2.0","method":"net_version","params":"invalid","id":1"#,
    ] {
        for (suffix, code) in [("", -32600), (" trailing", -32700)] {
            let payload = format!("{{{field}}}{suffix}");
            let (status, body) = bytes(app.clone().oneshot(request(payload)).await.unwrap()).await;
            assert_eq!(status, StatusCode::OK);
            let response: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(response["error"]["code"], code);
            assert_eq!(response["id"], Value::Null);
        }
    }
}
