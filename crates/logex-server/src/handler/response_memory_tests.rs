use super::*;
use alloy_primitives::{Address, B256};
use http_body::Body;
use logex_storage::PartitionManagerConfig;
use logex_types::{LogRow, Source};
use std::pin::Pin;

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
            data: vec![0xab; 4097].into(),
            data_len: 4097,
            source: Source::Receipt,
        }])
        .unwrap();
    let state = Arc::new(AppState::with_query_limits(
        storage,
        None,
        SyncStatus::default(),
        QueryConcurrencyLimit::new(2).unwrap(),
        QueryMemoryLimit::new(64 * 1024).unwrap(),
    ));
    (temp, state)
}

async fn get_logs(state: &Arc<AppState>, id: &str) -> Response {
    let request = serde_json::from_str(&format!(
        r#"{{"jsonrpc":"2.0","method":"eth_getLogs","params":[{{}}],"id":{id}}}"#
    ))
    .unwrap();
    handle_jsonrpc(State(Arc::clone(state)), Ok(Json(request))).await
}

#[tokio::test]
async fn encoded_rpc_allocation_follows_retained_frame_aliases() {
    let (_temp, state) = state();
    let response = get_logs(&state, "9007199254740993.125").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    let held = state.query_memory.used();
    assert!(held > 8194);
    assert_eq!(state.query_control.capacity.available_permits(), 1);
    let mut body = response.into_body();
    let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    let bytes = frame.into_data().unwrap();
    assert!(
        std::str::from_utf8(&bytes)
            .unwrap()
            .ends_with(r#""id":9007199254740993.125}"#)
    );
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["result"].as_array().unwrap().len(), 1);
    assert_eq!(
        value["result"][0]["data"],
        format!("0x{}", "ab".repeat(4097))
    );
    let clone = bytes.clone();
    let slice = bytes.slice(0..1);
    drop(body);
    drop(bytes);
    drop(clone);
    assert_eq!(state.query_memory.used(), held);
    assert_eq!(state.query_control.capacity.available_permits(), 1);
    drop(slice);
    assert_eq!(state.query_memory.used(), 0);
    assert_eq!(state.query_control.capacity.available_permits(), 2);

    let unpolled = get_logs(&state, "null").await;
    assert!(state.query_memory.used() > 0);
    drop(unpolled);
    assert_eq!(state.query_memory.used(), 0);
    assert_eq!(state.query_control.capacity.available_permits(), 2);
}

#[tokio::test]
async fn rpc_capacity_is_explicit_preserves_id_and_recovers_without_storage_failure() {
    let (_temp, state) = state();
    for available in [127, 800] {
        // The shared source budget now rejects this request before encoding.
        let held = state
            .query_memory
            .reserve(state.query_memory.limit() - available, "competing query")
            .unwrap();
        let response = get_logs(&state, r#""capacity\u0020id""#).await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        assert!(
            std::str::from_utf8(&bytes)
                .unwrap()
                .ends_with(r#""id":"capacity\u0020id"}"#)
        );
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"]["code"], -32005);
        assert!(value.get("result").is_none());
        let message = value["error"]["message"].as_str().unwrap();
        assert!(message.starts_with("query memory capacity exceeded at "));
        assert!(!message.contains("HTTP query response"));
        assert!(state.storage_failure().is_none());
        assert_eq!(state.query_memory.used(), held.bytes());
        drop(bytes);
        let status = crate::rest::handle_health(State(Arc::clone(&state))).await;
        assert_eq!(status.status(), StatusCode::OK);
        drop(status);
        drop(held);
    }

    let recovered = get_logs(&state, "7").await;
    let bytes = axum::body::to_bytes(recovered.into_body(), 32 * 1024)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert!(value.get("error").is_none());
    assert_eq!(value["result"].as_array().unwrap().len(), 1);
    drop(bytes);
    assert_eq!(state.query_memory.used(), 0);
    assert_eq!(state.query_control.capacity.available_permits(), 2);
}

#[tokio::test]
async fn rpc_encoding_pressure_retains_native_source_and_reports_its_stage() {
    let (_temp, state) = state();
    let snapshot = {
        let storage = state.read_storage().await.unwrap();
        NativeStorageSnapshot::from_storage(&storage)
    };
    let rows = logex_query::execute_log_filter_on_snapshot_with_memory(
        &snapshot,
        &logex_storage::native::NativeLogFilter::new(),
        None,
        &state.query_memory,
    )
    .unwrap();
    let source_charge = state.query_memory.used();
    assert!(source_charge > 0);
    let id = serde_json::value::RawValue::from_string("7".to_owned()).unwrap();
    for available in [127, 800] {
        let held = state
            .query_memory
            .reserve(
                state.query_memory.limit() - usize::try_from(source_charge).unwrap() - available,
                "competing query",
            )
            .unwrap();
        let error = serialize_json(
            &JsonRpcResponse {
                jsonrpc: "2.0",
                result: Some(RpcLogs(&rows)),
                error: None,
                id: &*id,
            },
            &state.query_memory,
            None,
        )
        .unwrap_err();
        assert!(is_capacity_error(&error));
        assert!(error.to_string().contains("HTTP query response"));
        assert_eq!(state.query_memory.used(), source_charge + held.bytes());
        drop(held);
    }
    drop(rows);
    assert_eq!(state.query_memory.used(), 0);
}

#[tokio::test]
async fn rpc_notification_releases_encoded_memory_without_a_response_body() {
    let (_temp, state) = state();
    let request =
        serde_json::from_str(r#"{"jsonrpc":"2.0","method":"eth_getLogs","params":[{}]}"#).unwrap();
    let response = handle_jsonrpc(State(Arc::clone(&state)), Ok(Json(request))).await;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        axum::body::to_bytes(response.into_body(), 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(state.query_memory.used(), 0);
    assert_eq!(state.query_control.capacity.available_permits(), 2);
}
