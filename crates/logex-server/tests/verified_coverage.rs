//! Public API coverage admission with the production AppState constructor.
use std::sync::Arc;

use alloy_consensus::Header;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use logex_server::{
    AppState, build_router,
    grpc::{
        LogExGrpcService,
        pb::{GetLogsRequest, QueryRequest, log_ex_service_server::LogExService},
    },
};
use logex_storage::{PartitionManager, PartitionManagerConfig, VerifiedBlockLogs};
use logex_types::SyncStatus;
use serde_json::{Value, json};
use tower::ServiceExt;

fn fixture(verified: bool) -> (tempfile::TempDir, Arc<AppState>, Header) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: dir.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    let first = Header {
        number: 10,
        ..Default::default()
    };
    let second = Header {
        number: 11,
        parent_hash: first.hash_slow(),
        ..Default::default()
    };
    let last = Header {
        number: 12,
        parent_hash: second.hash_slow(),
        ..Default::default()
    };
    let headers = [first, second.clone(), last.clone()];
    if verified {
        let blocks: Vec<_> = headers
            .iter()
            .map(|header| VerifiedBlockLogs::from_empty_header(header).unwrap())
            .collect();
        storage
            .ingest_verified_canonical_batch(&[], &blocks, &headers, None)
            .unwrap();
    } else {
        storage.record_canonical_state(&last, &headers).unwrap();
    }
    (
        dir,
        Arc::new(AppState::new(storage, None, SyncStatus::default())),
        second,
    )
}

async fn post(state: &Arc<AppState>, path: &str, body: Value) -> (StatusCode, Value) {
    let response = build_router(Arc::clone(state))
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}

#[tokio::test]
async fn rest_rejects_uncertified_scans_but_admits_verified_empty_blocks() {
    for verified in [false, true] {
        let (_dir, state, _) = fixture(verified);
        for sql in [
            "SELECT COUNT(*) FROM logs",
            "SELECT DISTINCT address FROM logs",
            "SELECT SUM(data) FROM logs",
        ] {
            let (status, body) = post(&state, "/query", json!({"sql":sql})).await;
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{sql}: {body}");
            assert!(body["error"].as_str().unwrap().contains("verified"));
            assert!(body.get("rows").is_none());
        }
        let (status, body) = post(&state, "/query", json!({"sql":"SELECT COUNT(*) AS count FROM logs WHERE block_number BETWEEN 10 AND 12"})).await;
        if verified {
            assert_eq!(status, StatusCode::OK, "{body}");
            assert_eq!(body["rows"][0]["count"], 0);
        } else {
            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        }
    }
}

#[tokio::test]
async fn jsonrpc_reports_missing_coverage_instead_of_empty_logs() {
    let (_dir, state, empty) = fixture(true);
    for filter in [
        json!({}),
        json!({"fromBlock":"0x9","toBlock":"0xc"}),
        json!({"fromBlock":"0xa","toBlock":"0xd"}),
    ] {
        let (_, body) = post(
            &state,
            "/",
            json!({"jsonrpc":"2.0","method":"eth_getLogs","params":[filter],"id":17}),
        )
        .await;
        assert_eq!(body["error"]["code"], -32001, "{body}");
        assert_eq!(body["id"], 17);
        assert!(body.get("result").is_none());
    }
    for filter in [
        json!({"fromBlock":"0xa","toBlock":"0xc"}),
        json!({"blockHash":empty.hash_slow().to_string()}),
    ] {
        let (_, body) = post(
            &state,
            "/",
            json!({"jsonrpc":"2.0","method":"eth_getLogs","params":[filter],"id":18}),
        )
        .await;
        assert_eq!(body["result"], json!([]), "{body}");
    }
}

#[tokio::test]
async fn grpc_native_stream_and_sql_use_the_same_verified_admission() {
    let (_dir, state, _) = fixture(true);
    let service = LogExGrpcService::new(state);
    let native = service
        .get_logs(tonic::Request::new(GetLogsRequest::default()))
        .await
        .err()
        .unwrap();
    assert_eq!(native.code(), tonic::Code::FailedPrecondition);
    let stream = service
        .stream_logs(tonic::Request::new(GetLogsRequest::default()))
        .await
        .err()
        .unwrap();
    assert_eq!(stream.code(), tonic::Code::FailedPrecondition);
    for sql in [
        "SELECT COUNT(*) FROM logs",
        "SELECT DISTINCT address FROM logs",
    ] {
        let error = service
            .query(tonic::Request::new(QueryRequest {
                sql: sql.to_owned(),
                ..Default::default()
            }))
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "{sql}: {error}"
        );
    }
    let result = service
        .get_logs(tonic::Request::new(GetLogsRequest {
            from_block: Some(10),
            to_block: Some(12),
            ..Default::default()
        }))
        .await
        .unwrap();
    assert!(result.get_ref().logs.is_empty());
}
