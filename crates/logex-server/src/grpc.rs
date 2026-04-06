use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use logex_query::{self, is_simple_select};
use logex_types::LogRow;

use crate::handler::AppState;

pub mod pb {
    tonic::include_proto!("logex");
}

use pb::log_ex_service_server::{LogExService, LogExServiceServer};
use pb::{Empty, HeadBlockResponse, LogEntry, QueryRequest};

/// gRPC service implementation.
pub struct LogExGrpcService {
    state: Arc<AppState>,
}

impl LogExGrpcService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

type LogStream = Pin<Box<dyn Stream<Item = Result<LogEntry, Status>> + Send>>;

#[tonic::async_trait]
impl LogExService for LogExGrpcService {
    type QueryStream = LogStream;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let sql = &request.get_ref().sql;
        tracing::debug!(sql = %sql, "gRPC query");

        let query = logex_query::parse(sql)
            .map_err(|e| Status::invalid_argument(format!("parse error: {e}")))?;

        if !is_simple_select(&query) {
            return Err(Status::unimplemented(
                "only simple SELECT queries are supported",
            ));
        }

        let head_block = self.state.storage.head_block();
        let result = logex_query::execute(&query, &self.state.storage, head_block)
            .map_err(|e| Status::internal(format!("execution error: {e}")))?;

        let entries: Vec<LogEntry> = result.rows.iter().map(log_row_to_entry).collect();

        let stream = tokio_stream::iter(entries.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_head_block(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<HeadBlockResponse>, Status> {
        let block_number = self.state.storage.head_block().unwrap_or(0);
        Ok(Response::new(HeadBlockResponse { block_number }))
    }
}

fn log_row_to_entry(row: &LogRow) -> LogEntry {
    let mut topics = Vec::with_capacity(4);
    if let Some(t) = &row.topic0 {
        topics.push(t.as_slice().to_vec());
    }
    if let Some(t) = &row.topic1 {
        topics.push(t.as_slice().to_vec());
    }
    if let Some(t) = &row.topic2 {
        topics.push(t.as_slice().to_vec());
    }
    if let Some(t) = &row.topic3 {
        topics.push(t.as_slice().to_vec());
    }

    LogEntry {
        block_number: row.block_number,
        block_hash: row.block_hash.as_slice().to_vec(),
        timestamp: row.timestamp,
        tx_hash: row.tx_hash.as_slice().to_vec(),
        tx_index: row.tx_index,
        log_index: row.log_index,
        address: row.address.as_slice().to_vec(),
        topics,
        data: row.data.to_vec(),
    }
}

/// Start the gRPC server on the given address.
pub async fn serve_grpc(state: Arc<AppState>, addr: SocketAddr) -> Result<(), tonic::transport::Error> {
    let service = LogExGrpcService::new(state);
    tracing::info!(%addr, "gRPC server listening");

    tonic::transport::Server::builder()
        .add_service(LogExServiceServer::new(service))
        .serve(addr)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::{PartitionManager, PartitionManagerConfig};
    use logex_types::Source;
    use tempfile::TempDir;
    use tokio_stream::StreamExt;

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

    #[tokio::test]
    async fn test_grpc_query() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState { storage, subscriptions: None });
        let service = LogExGrpcService::new(state);

        let request = Request::new(QueryRequest {
            sql: "SELECT * FROM logs".into(),
        });

        let response = service.query(request).await.unwrap();
        let mut stream = response.into_inner();

        let mut entries = Vec::new();
        while let Some(entry) = stream.next().await {
            entries.push(entry.unwrap());
        }

        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].block_number, 100);
        assert_eq!(entries[1].block_number, 200);
    }

    #[tokio::test]
    async fn test_grpc_query_with_filter() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState { storage, subscriptions: None });
        let service = LogExGrpcService::new(state);

        let addr = hex::encode(Address::repeat_byte(0xAA));
        let request = Request::new(QueryRequest {
            sql: format!("SELECT * FROM logs WHERE address = '0x{addr}'"),
        });

        let response = service.query(request).await.unwrap();
        let mut stream = response.into_inner();

        let mut entries = Vec::new();
        while let Some(entry) = stream.next().await {
            entries.push(entry.unwrap());
        }

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].address, Address::repeat_byte(0xAA).as_slice());
    }

    #[tokio::test]
    async fn test_grpc_head_block() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState { storage, subscriptions: None });
        let service = LogExGrpcService::new(state);

        let response = service
            .get_head_block(Request::new(Empty {}))
            .await
            .unwrap();
        assert_eq!(response.into_inner().block_number, 200);
    }

    #[tokio::test]
    async fn test_grpc_invalid_sql() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState { storage, subscriptions: None });
        let service = LogExGrpcService::new(state);

        let request = Request::new(QueryRequest {
            sql: "NOT SQL".into(),
        });

        let result = service.query(request).await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::InvalidArgument);
    }
}
