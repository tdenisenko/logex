use std::future::Future;
use std::sync::Arc;
use std::task::Poll;

use alloy_primitives::{Address, B256, Bytes};
use axum::Json;
use axum::extract::State;
use logex_server::grpc::LogExGrpcService;
use logex_server::grpc::pb::GetLogsRequest;
use logex_server::grpc::pb::log_ex_service_server::LogExService;
use logex_server::handler::handle_jsonrpc;
use logex_server::{AppState, jsonrpc::JsonRpcRequest};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source, SyncStatus};
use tokio_stream::StreamExt;

#[derive(Clone, Copy, Debug)]
enum Protocol {
    JsonRpc,
    Grpc,
    GrpcStream,
}

fn row(block: u64) -> LogRow {
    LogRow {
        block_number: block,
        block_hash: B256::from(alloy_primitives::U256::from(block).to_be_bytes::<32>()),
        timestamp: block,
        tx_hash: B256::repeat_byte(3),
        tx_index: 0,
        log_index: 0,
        address: Address::repeat_byte(4),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: Bytes::new(),
        data_len: 0,
        source: Source::Receipt,
    }
}

async fn query(state: Arc<AppState>, protocol: Protocol) -> Result<Vec<u64>, String> {
    match protocol {
        Protocol::JsonRpc => {
            let request: JsonRpcRequest = serde_json::from_value(serde_json::json!({
                "jsonrpc": "2.0", "method": "eth_getLogs", "params": [{}], "id": 1,
            }))
            .unwrap();
            let Json(response) = handle_jsonrpc(State(state), Json(request)).await;
            if let Some(error) = response.error {
                return Err(error.message);
            }
            Ok(response
                .result
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    u64::from_str_radix(
                        row["blockNumber"]
                            .as_str()
                            .unwrap()
                            .trim_start_matches("0x"),
                        16,
                    )
                    .unwrap()
                })
                .collect())
        }
        Protocol::Grpc => Ok(LogExGrpcService::new(state)
            .get_logs(tonic::Request::new(GetLogsRequest::default()))
            .await
            .map_err(|error| error.to_string())?
            .into_inner()
            .logs
            .into_iter()
            .map(|row| row.block_number)
            .collect()),
        Protocol::GrpcStream => {
            let mut stream = LogExGrpcService::new(state)
                .stream_logs(tonic::Request::new(GetLogsRequest::default()))
                .await
                .map_err(|error| error.to_string())?
                .into_inner();
            let mut blocks = Vec::new();
            while let Some(row) = stream.next().await {
                blocks.push(row.map_err(|error| error.to_string())?.block_number);
            }
            Ok(blocks)
        }
    }
}

fn snapshot_while_worker_queued(protocol: Protocol, reorg: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    })
    .unwrap();
    storage.write_batch(&[row(100), row(200)]).unwrap();
    storage.checkpoint().unwrap();
    let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
    // Occupy the one blocking worker. This deterministically pauses real API
    // execution after snapshot capture without slowing or replacing storage I/O.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let occupied = tokio::task::spawn_blocking(move || {
            let _ = entered_tx.send(());
            // Sender drop also releases this worker if an assertion fails.
            let _ = release_rx.recv();
        });
        entered_rx.await.unwrap();
        let mut request = Box::pin(query(Arc::clone(&state), protocol));
        let first = std::future::poll_fn(|cx| Poll::Ready(request.as_mut().poll(cx))).await;
        assert!(
            first.is_pending(),
            "{protocol:?} must yield before scanning"
        );
        let mut writer = state
            .storage
            .try_write()
            .expect("queued native queries must release the ingestion lock");
        if reorg {
            assert_eq!(writer.mark_non_canonical(row(200).block_hash).unwrap(), 1);
        } else {
            writer.write_batch(&[row(300)]).unwrap();
        }
        drop(writer);
        release_tx.send(()).unwrap();
        occupied.await.unwrap();
        let result = request.await;
        if reorg {
            assert!(result.unwrap_err().contains("snapshot changed"));
            assert_eq!(query(state, protocol).await.unwrap(), vec![100]);
        } else {
            assert_eq!(result.unwrap(), vec![100, 200]);
            assert_eq!(query(state, protocol).await.unwrap(), vec![100, 200, 300]);
        }
    });
}

#[test]
fn jsonrpc_native_scan_yields_and_keeps_captured_rows() {
    snapshot_while_worker_queued(Protocol::JsonRpc, false);
}

#[test]
fn grpc_native_scan_yields_and_keeps_captured_rows() {
    snapshot_while_worker_queued(Protocol::Grpc, false);
}

#[test]
fn grpc_stream_scan_yields_and_keeps_captured_rows() {
    snapshot_while_worker_queued(Protocol::GrpcStream, false);
}

#[test]
fn jsonrpc_native_scan_rejects_obsolete_snapshot() {
    snapshot_while_worker_queued(Protocol::JsonRpc, true);
}

#[test]
fn grpc_native_scan_rejects_obsolete_snapshot() {
    snapshot_while_worker_queued(Protocol::Grpc, true);
}

#[test]
fn grpc_stream_scan_rejects_obsolete_snapshot() {
    snapshot_while_worker_queued(Protocol::GrpcStream, true);
}
