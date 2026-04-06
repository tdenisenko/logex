use std::sync::Arc;

use axum::extract::State;
use axum::response::Json;

use logex_query::{self, QueryResult};
use logex_storage::PartitionManager;

use crate::eth_filter::{AddressFilter, BlockId, EthFilter, RpcLog, TopicFilter, matches_filter};
use crate::jsonrpc::{JsonRpcRequest, JsonRpcResponse};

use crate::ws::SubscriptionManager;

/// Shared application state.
pub struct AppState {
    pub storage: PartitionManager,
    /// WebSocket subscription manager. None if subscriptions are disabled.
    pub subscriptions: Option<SubscriptionManager>,
}

/// Handle a JSON-RPC request.
pub async fn handle_jsonrpc(
    State(state): State<Arc<AppState>>,
    Json(request): Json<JsonRpcRequest>,
) -> Json<JsonRpcResponse> {
    let id = request.id.clone();

    let response = match request.method.as_str() {
        "eth_getLogs" => handle_eth_get_logs(&state, &request),
        "eth_blockNumber" => handle_eth_block_number(&state, &request),
        "web3_clientVersion" => Ok(JsonRpcResponse::success(
            id.clone(),
            serde_json::Value::String("LogEx/0.1.0".into()),
        )),
        "net_version" => Ok(JsonRpcResponse::success(
            id.clone(),
            serde_json::Value::String("1".into()),
        )),
        _ => Ok(JsonRpcResponse::method_not_found(id.clone())),
    };

    Json(response.unwrap_or_else(|e: String| JsonRpcResponse::internal_error(id, e)))
}

fn handle_eth_get_logs(state: &AppState, req: &JsonRpcRequest) -> Result<JsonRpcResponse, String> {
    let id = req.id.clone();
    let params = req
        .params
        .as_ref()
        .and_then(|p| p.as_array())
        .ok_or_else(|| "params must be an array".to_string())?;

    if params.is_empty() {
        return Err("eth_getLogs requires a filter parameter".into());
    }

    let filter: EthFilter =
        serde_json::from_value(params[0].clone()).map_err(|e| format!("invalid filter: {e}"))?;

    // Convert eth_getLogs filter to a LogSQL query
    let sql = filter_to_logsql(&filter, &state.storage);
    tracing::debug!(sql = %sql, "eth_getLogs query");

    let query = logex_query::parse(&sql).map_err(|e| format!("query parse error: {e}"))?;

    let head_block = state.storage.head_block();
    let result: QueryResult =
        logex_query::execute(&query, &state.storage, head_block).map_err(|e| e.to_string())?;

    // Apply eth_getLogs topic/address filters that go beyond what the index supports
    // (e.g., multi-address, topic1-3 filters)
    let logs: Vec<RpcLog> = result
        .rows
        .iter()
        .filter(|row| matches_filter(row, &filter))
        .map(RpcLog::from)
        .collect();

    let json = serde_json::to_value(&logs).map_err(|e| e.to_string())?;
    Ok(JsonRpcResponse::success(id, json))
}

fn handle_eth_block_number(
    state: &AppState,
    req: &JsonRpcRequest,
) -> Result<JsonRpcResponse, String> {
    let block = state.storage.head_block().unwrap_or(0);
    Ok(JsonRpcResponse::success(
        req.id.clone(),
        serde_json::Value::String(format!("0x{block:x}")),
    ))
}

/// Convert an `eth_getLogs` filter into a LogSQL WHERE clause.
fn filter_to_logsql(filter: &EthFilter, storage: &PartitionManager) -> String {
    let mut conditions = Vec::new();

    // Block range
    match (&filter.from_block, &filter.to_block) {
        (Some(from), Some(to)) => {
            let from_num = resolve_block_id(from, storage);
            let to_num = resolve_block_id(to, storage);
            conditions.push(format!("block_number BETWEEN {from_num} AND {to_num}"));
        }
        (Some(from), None) => {
            let from_num = resolve_block_id(from, storage);
            conditions.push(format!("block_number >= {from_num}"));
        }
        (None, Some(to)) => {
            let to_num = resolve_block_id(to, storage);
            conditions.push(format!("block_number <= {to_num}"));
        }
        (None, None) => {}
    }

    // Address — push single-address to index, multi-address handled via residual
    match &filter.address {
        AddressFilter::Any => {}
        AddressFilter::Single(addr) => {
            conditions.push(format!("address = '0x{}'", hex::encode(addr)));
        }
        AddressFilter::Multiple(_) => {
            // Multi-address filter is handled post-query via matches_filter
        }
    }

    // Topic0 — push single value to index
    if let Some(Some(tf)) = filter.topics.first()
        && let TopicFilter::Single(hash) = tf
    {
        conditions.push(format!("topic0 = '0x{}'", hex::encode(hash)));
    }
    // Multi-topic0 handled via matches_filter

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", conditions.join(" AND "))
    };

    format!("SELECT * FROM logs{where_clause}")
}

fn resolve_block_id(id: &BlockId, storage: &PartitionManager) -> u64 {
    match id {
        BlockId::Number(n) => *n,
        BlockId::Latest => storage.head_block().unwrap_or(0),
        BlockId::Earliest => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::PartitionManagerConfig;
    use logex_types::{LogRow, Source};
    use tempfile::TempDir;

    fn make_test_rows() -> Vec<LogRow> {
        vec![
            LogRow {
                block_number: 100,
                block_hash: B256::repeat_byte(0x01),
                timestamp: 1_700_000_000,
                tx_hash: B256::repeat_byte(0x11),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            },
            LogRow {
                block_number: 200,
                block_hash: B256::repeat_byte(0x02),
                timestamp: 1_700_001_200,
                tx_hash: B256::repeat_byte(0x22),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xBB),
                topic0: Some(B256::repeat_byte(0xEE)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Receipt,
            },
        ]
    }

    fn setup_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
        };
        let mut mgr = PartitionManager::open(config).unwrap();
        mgr.write_batch(&make_test_rows()).unwrap();
        IndexBuilder::build_all_indexes(&mgr.hot_partition().meta.path).unwrap();
        (tmp, mgr)
    }

    #[test]
    fn test_filter_to_logsql_basic() {
        let (_tmp, storage) = setup_storage();
        let filter = EthFilter {
            from_block: Some(BlockId::Number(100)),
            to_block: Some(BlockId::Number(200)),
            address: AddressFilter::Single(Address::repeat_byte(0xAA)),
            ..Default::default()
        };
        let sql = filter_to_logsql(&filter, &storage);
        assert!(sql.contains("block_number BETWEEN 100 AND 200"));
        assert!(sql.contains("address = '0x"));
    }

    #[test]
    fn test_filter_to_logsql_no_filter() {
        let (_tmp, storage) = setup_storage();
        let filter = EthFilter::default();
        let sql = filter_to_logsql(&filter, &storage);
        assert_eq!(sql, "SELECT * FROM logs");
    }

    #[tokio::test]
    async fn test_eth_get_logs_full() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState {
            storage,
            subscriptions: None,
        });

        let addr = hex::encode(Address::repeat_byte(0xAA));
        let req_json = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_getLogs",
            "params": [{
                "fromBlock": "0x0",
                "toBlock": "latest",
                "address": format!("0x{addr}")
            }],
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(req_json).unwrap();
        let Json(response) = handle_jsonrpc(State(state), Json(request)).await;

        assert!(response.error.is_none());
        let logs: Vec<serde_json::Value> =
            serde_json::from_value(response.result.unwrap()).unwrap();
        assert_eq!(logs.len(), 1);
        assert!(logs[0]["address"].as_str().unwrap().contains(&addr));
    }

    #[tokio::test]
    async fn test_eth_block_number() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState {
            storage,
            subscriptions: None,
        });

        let req_json = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        });

        let request: JsonRpcRequest = serde_json::from_value(req_json).unwrap();
        let Json(response) = handle_jsonrpc(State(state), Json(request)).await;

        assert!(response.error.is_none());
        let block = response.result.unwrap();
        assert_eq!(block, "0xc8"); // 200
    }
}
