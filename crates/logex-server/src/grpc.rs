use std::net::SocketAddr;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use logex_query::{self, SqlQueryError};

use crate::handler::AppState;

pub mod pb {
    tonic::include_proto!("logex");
}

use pb::log_ex_service_server::{LogExService, LogExServiceServer};
use pb::{Empty, HeadBlockResponse, QueryRequest, QueryResponse, QueryRow};

/// gRPC service implementation.
pub struct LogExGrpcService {
    state: Arc<AppState>,
}

impl LogExGrpcService {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl LogExService for LogExGrpcService {
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let sql = &request.get_ref().sql;
        tracing::debug!(sql = %sql, "gRPC query");

        let storage = self.state.storage.read().await;
        let head_block = storage.head_block();
        let result = match logex_query::execute_sql(sql, &storage, head_block).await {
            Ok(result) => result,
            Err(SqlQueryError::DataFusion(err)) => {
                return Err(Status::invalid_argument(format!("query error: {err}")));
            }
            Err(SqlQueryError::Storage(err)) => {
                return Err(Status::internal(format!("execution error: {err}")));
            }
        };

        let row_count = result.rows.len() as u64;
        let rows = result
            .rows
            .into_iter()
            .map(|row| QueryRow {
                json: serde_json::to_string(&row)
                    .unwrap_or_else(|_| String::from("{\"error\":\"row serialization failed\"}")),
            })
            .collect();

        Ok(Response::new(QueryResponse {
            rows,
            total_scanned: result.total_scanned,
            row_count,
        }))
    }

    async fn get_head_block(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<HeadBlockResponse>, Status> {
        let storage = self.state.storage.read().await;
        let block_number = storage.head_block().unwrap_or(0);
        Ok(Response::new(HeadBlockResponse { block_number }))
    }
}

/// Start the gRPC server on the given address.
pub async fn serve_grpc(
    state: Arc<AppState>,
    addr: SocketAddr,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<(), tonic::transport::Error> {
    let service = LogExGrpcService::new(state);
    tracing::info!(%addr, "gRPC server listening");

    tonic::transport::Server::builder()
        .add_service(LogExServiceServer::new(service))
        .serve_with_shutdown(addr, grpc_shutdown_signal(shutdown))
        .await
}

async fn grpc_shutdown_signal(mut rx: tokio::sync::watch::Receiver<bool>) {
    while !*rx.borrow_and_update() {
        if rx.changed().await.is_err() {
            break;
        }
    }
    tracing::info!("gRPC server shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::{PartitionManager, PartitionManagerConfig};
    use logex_types::{LogRow, Source, SyncStatus};
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

    #[tokio::test]
    async fn test_grpc_query() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let request = Request::new(QueryRequest {
            sql: "SELECT * FROM logs".into(),
        });

        let response = service.query(request).await.unwrap();
        let response = response.into_inner();

        assert_eq!(response.row_count, 2);
        let first: serde_json::Value = serde_json::from_str(&response.rows[0].json).unwrap();
        let second: serde_json::Value = serde_json::from_str(&response.rows[1].json).unwrap();
        assert_eq!(first["block_number"], 100);
        assert_eq!(second["block_number"], 200);
    }

    #[tokio::test]
    async fn test_grpc_query_with_filter() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let addr = hex::encode(Address::repeat_byte(0xAA));
        let request = Request::new(QueryRequest {
            sql: format!("SELECT * FROM logs WHERE address = '0x{addr}'"),
        });

        let response = service.query(request).await.unwrap();
        let response = response.into_inner();

        assert_eq!(response.row_count, 1);
        let row: serde_json::Value = serde_json::from_str(&response.rows[0].json).unwrap();
        assert_eq!(
            row["address"],
            format!("0x{}", hex::encode(Address::repeat_byte(0xAA)))
        );
    }

    #[tokio::test]
    async fn test_grpc_query_aggregate() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let request = Request::new(QueryRequest {
            sql: "SELECT COUNT(*) AS total FROM logs WHERE block_number <= latest".into(),
        });

        let response = service.query(request).await.unwrap().into_inner();
        assert_eq!(response.row_count, 1);
        let row: serde_json::Value = serde_json::from_str(&response.rows[0].json).unwrap();
        assert_eq!(row["total"], 2);
    }

    #[tokio::test]
    async fn test_grpc_query_desc_limit() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let request = Request::new(QueryRequest {
            sql: "SELECT block_number AS bn FROM logs ORDER BY block_number DESC LIMIT 1".into(),
        });

        let response = service.query(request).await.unwrap().into_inner();
        assert_eq!(response.row_count, 1);
        let row: serde_json::Value = serde_json::from_str(&response.rows[0].json).unwrap();
        assert_eq!(row["bn"], 200);
    }

    #[tokio::test]
    async fn test_grpc_head_block() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
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
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let request = Request::new(QueryRequest {
            sql: "NOT SQL".into(),
        });

        let result = service.query(request).await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::InvalidArgument);
    }
}
