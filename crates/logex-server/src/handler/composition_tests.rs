use super::*;
use alloy_primitives::{Address, B256};
use bytes::Bytes;
use http_body::Body as _;
use logex_storage::PartitionManagerConfig;
use logex_types::{LogRow, Source};
use prost::Message as _;
use std::{future::poll_fn, pin::Pin};
use tower::ServiceExt as _;

const MAX_FRAMES: usize = 32;
const PERMITS: usize = MAX_FRAMES + 1;
const PAYLOAD_BYTES: usize = 4097;

fn state() -> (tempfile::TempDir, Arc<AppState>) {
    let temp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: temp.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    storage
        .write_batch(&[LogRow {
            block_number: 1,
            block_hash: B256::repeat_byte(1),
            timestamp: 2,
            tx_hash: B256::repeat_byte(3),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(4),
            topic0: Some(B256::repeat_byte(5)),
            topic1: None,
            topic2: None,
            topic3: None,
            data: vec![0xab; PAYLOAD_BYTES].into(),
            data_len: PAYLOAD_BYTES as u32,
            source: Source::Receipt,
        }])
        .unwrap();
    let state = Arc::new(AppState::with_query_limits(
        storage,
        None,
        SyncStatus::default(),
        QueryConcurrencyLimit::new(PERMITS).unwrap(),
        QueryMemoryLimit::new(128 * 1024).unwrap(),
    ));
    (temp, state)
}

async fn rpc(state: &Arc<AppState>) -> Response {
    let request =
        serde_json::from_str(r#"{"jsonrpc":"2.0","method":"eth_getLogs","params":[{}],"id":7}"#)
            .unwrap();
    handle_jsonrpc(State(Arc::clone(state)), Ok(Json(request))).await
}

#[derive(Clone, Copy)]
enum Target {
    RestSql,
    RpcLogs,
}

impl Target {
    async fn response(self, state: &Arc<AppState>) -> Response {
        match self {
            Self::RestSql => {
                crate::rest::handle_query(
                    State(Arc::clone(state)),
                    Json(crate::rest::QueryRequest {
                        sql: "SELECT data AS first, data AS second FROM logs".to_owned(),
                        limit: None,
                        offset: 0,
                    }),
                )
                .await
            }
            Self::RpcLogs => rpc(state).await,
        }
    }

    async fn outcome(self, state: &Arc<AppState>) -> (bool, serde_json::Value) {
        let response = self.response(state).await;
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 128 * 1024)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let capacity = match self {
            Self::RestSql => {
                if status == StatusCode::SERVICE_UNAVAILABLE {
                    assert_eq!(value["status"], "query_capacity");
                    assert_eq!(value["resource"], "memory");
                    true
                } else {
                    assert_eq!(status, StatusCode::OK, "{value}");
                    false
                }
            }
            Self::RpcLogs => {
                assert_eq!(status, StatusCode::OK);
                if value.get("error").is_some() {
                    assert_eq!(value["error"]["code"], -32005);
                    assert!(
                        value["error"]["message"]
                            .as_str()
                            .unwrap()
                            .starts_with("query memory capacity exceeded at ")
                    );
                    assert!(value.get("result").is_none());
                    true
                } else {
                    false
                }
            }
        };
        (capacity, value)
    }
}

async fn rpc_frame(state: &Arc<AppState>) -> Bytes {
    let response = rpc(state).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut body = response.into_body();
    let bytes = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(value.get("error").is_none(), "{value}");
    assert_eq!(value["result"][0]["data"], payload());
    bytes
}

async fn tonic_frame(state: &Arc<AppState>) -> Bytes {
    let payload = crate::grpc::pb::GetLogsRequest::default().encode_to_vec();
    let mut framed = vec![0];
    framed.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    framed.extend_from_slice(&payload);
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/logex.LogExService/GetLogs")
        .header("content-type", "application/grpc")
        .body(axum::body::Body::from(framed))
        .unwrap();
    let response = crate::grpc::grpc_service(Arc::clone(state))
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    // Poll the generated service to cross lazy protocol-to-Prost encoding.
    let mut body = response.into_body();
    let bytes = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap()
        .into_data()
        .expect("successful encoded Tonic data frame");
    assert_eq!(bytes[0], 0);
    assert_eq!(
        u32::from_be_bytes(bytes[1..5].try_into().unwrap()) as usize,
        bytes.len() - 5
    );
    let decoded = crate::grpc::pb::GetLogsResponse::decode(&bytes[5..]).unwrap();
    assert_eq!(decoded.logs.len(), 1);
    assert_eq!(decoded.logs[0].data, vec![0xab; PAYLOAD_BYTES]);
    bytes
}

fn payload() -> String {
    format!("0x{}", "ab".repeat(PAYLOAD_BYTES))
}

async fn composition(target: Target) {
    let (_temp, state) = state();
    let (capacity, expected) = target.outcome(&state).await;
    assert!(!capacity);
    match target {
        Target::RestSql => assert_eq!(
            expected["rows"],
            serde_json::json!([{"first": payload(), "second": payload()}])
        ),
        Target::RpcLogs => assert_eq!(expected["result"][0]["data"], payload()),
    }
    assert_eq!(state.query_memory.used(), 0);
    assert_eq!(state.query_control.capacity.available_permits(), PERMITS);

    // Saturate using only real response owners. Probe after each frame so the
    // threshold follows actual implementation capacity, not allocator constants.
    let mut frames = Vec::new();
    let mut rejected = false;
    for _ in 0..MAX_FRAMES {
        let frame = match target {
            Target::RestSql => rpc_frame(&state).await,
            Target::RpcLogs => tonic_frame(&state).await,
        };
        frames.push(frame);
        let held = state.query_memory.used();
        assert!(held > 0);
        assert!(state.query_control.capacity.available_permits() > 0);
        rejected = target.outcome(&state).await.0;
        assert_eq!(state.query_memory.used(), held);
        assert!(state.storage_failure().is_none());
        if rejected {
            break;
        }
    }
    assert!(
        rejected,
        "bounded real response owners must create pressure"
    );
    let held = state.query_memory.used();
    let frame_count = frames.len();
    let aliases: Vec<_> = frames.iter().map(|frame| frame.slice(0..1)).collect();
    drop(frames);
    assert_eq!(state.query_memory.used(), held);
    assert_eq!(
        state.query_control.capacity.available_permits(),
        PERMITS - frame_count
    );
    assert!(target.outcome(&state).await.0);
    assert_eq!(state.query_memory.used(), held);
    assert!(state.storage_failure().is_none());
    drop(aliases);
    assert_eq!(state.query_memory.used(), 0);
    assert_eq!(state.query_control.capacity.available_permits(), PERMITS);

    let (capacity, recovered) = target.outcome(&state).await;
    assert!(!capacity);
    assert_eq!(recovered, expected);
    assert_eq!(state.query_memory.used(), 0);
    assert_eq!(state.query_control.capacity.available_permits(), PERMITS);
    assert!(state.storage_failure().is_none());
}

#[tokio::test]
async fn rpc_frame_owners_contend_with_rest_sql_and_recover() {
    composition(Target::RestSql).await;
}

#[tokio::test]
async fn tonic_frame_owners_contend_with_rpc_logs_and_recover() {
    composition(Target::RpcLogs).await;
}
