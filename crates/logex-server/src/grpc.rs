use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;

use alloy_primitives::{Address, B256};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use logex_query::{self, DEFAULT_QUERY_PAGE_SIZE, MAX_QUERY_LIMIT, SqlQueryError, SqlQueryPage};
use logex_storage::native::{LogOrder, NativeLogFilter, TopicConstraint};
use logex_types::LogRow;

use crate::handler::AppState;

pub mod pb {
    tonic::include_proto!("logex");
}

use pb::log_ex_service_server::{LogExService, LogExServiceServer};
use pb::{
    Empty, GetLogsRequest, GetLogsResponse, HeadBlockResponse, LogEntry, QueryRequest,
    QueryResponse, QueryRow,
};

type BoxStatus = Box<Status>;

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
    type StreamLogsStream = Pin<Box<dyn Stream<Item = Result<LogEntry, Status>> + Send>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let sql = &request.get_ref().sql;
        tracing::debug!(sql = %sql, "gRPC query");

        let storage = self.state.storage.read().await;
        let head_block = storage.head_block();
        let query_request = request.get_ref();
        let limit = query_request
            .limit
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("limit is too large"))?
            .unwrap_or(DEFAULT_QUERY_PAGE_SIZE);
        let offset = query_request
            .offset
            .map(usize::try_from)
            .transpose()
            .map_err(|_| Status::invalid_argument("offset is too large"))?
            .unwrap_or(0);
        let result = match logex_query::execute_sql_page(
            sql,
            &storage,
            head_block,
            SqlQueryPage::new(Some(limit), offset),
        )
        .await
        {
            Ok(result) => result,
            Err(SqlQueryError::DataFusion(err)) => {
                return Err(Status::invalid_argument(format!("query error: {err}")));
            }
            Err(SqlQueryError::LegacySyntax(err)) => {
                return Err(Status::invalid_argument(format!("query error: {err}")));
            }
            Err(SqlQueryError::Storage(err)) => {
                return Err(Status::internal(format!("execution error: {err}")));
            }
        };

        let row_count = result.rows.len() as u64;
        let next_offset = (limit > 0
            && row_count as usize == limit
            && offset.saturating_add(row_count as usize) < MAX_QUERY_LIMIT)
            .then_some((offset + row_count as usize) as u64);
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
            limit: limit.min(MAX_QUERY_LIMIT.saturating_sub(offset)) as u64,
            offset: offset as u64,
            next_offset,
            max_limit: MAX_QUERY_LIMIT as u64,
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

    async fn get_logs(
        &self,
        request: Request<GetLogsRequest>,
    ) -> Result<Response<GetLogsResponse>, Status> {
        let filter = proto_filter_to_native_filter(request.get_ref()).map_err(|status| *status)?;

        let storage = self.state.storage.read().await;
        let rows = logex_query::execute_log_filter(&storage, &filter)
            .map_err(|err| Status::internal(format!("execution error: {err}")))?;
        let row_count = rows.len() as u64;
        let logs = rows.into_iter().map(log_row_to_proto).collect();

        Ok(Response::new(GetLogsResponse { logs, row_count }))
    }

    async fn stream_logs(
        &self,
        request: Request<GetLogsRequest>,
    ) -> Result<Response<Self::StreamLogsStream>, Status> {
        let filter = proto_filter_to_native_filter(request.get_ref()).map_err(|status| *status)?;

        let storage = self.state.storage.read().await;
        let rows = logex_query::execute_log_filter(&storage, &filter)
            .map_err(|err| Status::internal(format!("execution error: {err}")))?;
        let entries: Vec<Result<LogEntry, Status>> = rows
            .into_iter()
            .map(log_row_to_proto)
            .map(Ok::<_, Status>)
            .collect();
        let stream = tokio_stream::iter(entries);

        Ok(Response::new(Box::pin(stream)))
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

fn proto_filter_to_native_filter(request: &GetLogsRequest) -> Result<NativeLogFilter, BoxStatus> {
    if !request.block_hash.is_empty()
        && (request.from_block.is_some() || request.to_block.is_some())
    {
        return Err(Box::new(Status::invalid_argument(
            "block_hash is mutually exclusive with from_block/to_block",
        )));
    }

    if request.topics.len() > 4 {
        return Err(Box::new(Status::invalid_argument(
            "at most four topic positions are supported",
        )));
    }

    let mut filter = NativeLogFilter::new();
    filter.from_block = request.from_block;
    filter.to_block = request.to_block;
    filter.block_hash = parse_optional_b256(&request.block_hash, "block_hash")?;
    filter.addresses = request
        .addresses
        .iter()
        .map(|address| parse_address(address, "addresses"))
        .collect::<Result<Vec<_>, _>>()?;
    filter.canonical_only = request.canonical_only.unwrap_or(true);
    filter.order = if request.descending.unwrap_or(false) {
        LogOrder::Descending
    } else {
        LogOrder::Ascending
    };
    filter.limit = request
        .limit
        .map(|limit| {
            usize::try_from(limit)
                .map_err(|_| Box::new(Status::invalid_argument("limit is too large")))
        })
        .transpose()?;
    if let Some(limit) = filter.limit {
        if limit > MAX_QUERY_LIMIT {
            return Err(Box::new(Status::invalid_argument(format!(
                "limit must be at most {MAX_QUERY_LIMIT}"
            ))));
        }
    } else {
        filter.limit = Some(DEFAULT_QUERY_PAGE_SIZE);
    }
    filter.offset = request
        .offset
        .map(|offset| {
            usize::try_from(offset)
                .map_err(|_| Box::new(Status::invalid_argument("offset is too large")))
        })
        .transpose()?
        .unwrap_or(0);
    if filter.offset >= MAX_QUERY_LIMIT {
        return Err(Box::new(Status::invalid_argument(format!(
            "offset must be less than {MAX_QUERY_LIMIT}"
        ))));
    }
    filter.limit = filter
        .limit
        .map(|limit| limit.min(MAX_QUERY_LIMIT - filter.offset));

    for (index, topic) in request.topics.iter().enumerate() {
        filter.topics[index] = parse_topic_constraint(topic, index)?;
    }

    Ok(filter)
}

fn parse_optional_b256(bytes: &[u8], field: &str) -> Result<Option<B256>, BoxStatus> {
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.len() != 32 {
        return Err(Box::new(Status::invalid_argument(format!(
            "{field} must be exactly 32 bytes"
        ))));
    }
    Ok(Some(B256::from_slice(bytes)))
}

fn parse_address(bytes: &[u8], field: &str) -> Result<Address, BoxStatus> {
    if bytes.len() != 20 {
        return Err(Box::new(Status::invalid_argument(format!(
            "{field} entries must be exactly 20 bytes"
        ))));
    }
    Ok(Address::from_slice(bytes))
}

fn parse_topic_constraint(
    topic: &pb::TopicFilter,
    index: usize,
) -> Result<TopicConstraint, BoxStatus> {
    if topic.match_any {
        if !topic.any_of.is_empty() {
            return Err(Box::new(Status::invalid_argument(format!(
                "topic position {index} cannot set match_any and any_of together"
            ))));
        }
        return Ok(TopicConstraint::Any);
    }

    if topic.any_of.is_empty() {
        return Ok(TopicConstraint::Any);
    }

    let hashes = topic
        .any_of
        .iter()
        .map(|value| {
            if value.len() != 32 {
                return Err(Box::new(Status::invalid_argument(format!(
                    "topic position {index} values must be exactly 32 bytes"
                ))));
            }
            Ok(B256::from_slice(value))
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(if hashes.len() == 1 {
        TopicConstraint::One(hashes[0])
    } else {
        TopicConstraint::AnyOf(hashes)
    })
}

fn log_row_to_proto(row: LogRow) -> LogEntry {
    let mut topics = Vec::with_capacity(4);
    if let Some(topic) = row.topic0 {
        topics.push(topic.as_slice().to_vec());
    }
    if let Some(topic) = row.topic1 {
        topics.push(topic.as_slice().to_vec());
    }
    if let Some(topic) = row.topic2 {
        topics.push(topic.as_slice().to_vec());
    }
    if let Some(topic) = row.topic3 {
        topics.push(topic.as_slice().to_vec());
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
        data_len: row.data_len,
        source: row.source as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::{PartitionManager, PartitionManagerConfig};
    use logex_types::{LogRow, Source, SyncStatus};
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
            compaction_safety_margin_blocks: 2_048,
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
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
            ..Default::default()
        });

        let result = service.query(request).await;
        assert!(result.is_err());
        assert_eq!(result.err().unwrap().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_grpc_get_logs() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let request = Request::new(GetLogsRequest {
            from_block: Some(0),
            to_block: Some(250),
            block_hash: Vec::new(),
            addresses: vec![Address::repeat_byte(0xAA).as_slice().to_vec()],
            topics: vec![pb::TopicFilter {
                match_any: false,
                any_of: vec![B256::repeat_byte(0xDD).as_slice().to_vec()],
            }],
            canonical_only: None,
            descending: None,
            limit: None,
            offset: None,
        });

        let response = service.get_logs(request).await.unwrap().into_inner();
        assert_eq!(response.row_count, 1);
        assert_eq!(response.logs[0].block_number, 100);
        assert_eq!(
            response.logs[0].address,
            Address::repeat_byte(0xAA).as_slice()
        );
        assert_eq!(response.logs[0].topics.len(), 1);
        assert_eq!(
            response.logs[0].topics[0],
            B256::repeat_byte(0xDD).as_slice()
        );
    }

    #[tokio::test]
    async fn test_grpc_stream_logs_descending() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let request = Request::new(GetLogsRequest {
            from_block: Some(0),
            to_block: Some(250),
            block_hash: Vec::new(),
            addresses: Vec::new(),
            topics: Vec::new(),
            canonical_only: Some(true),
            descending: Some(true),
            limit: Some(2),
            offset: None,
        });

        let mut stream = service.stream_logs(request).await.unwrap().into_inner();
        let first = stream.next().await.unwrap().unwrap();
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(first.block_number, 200);
        assert_eq!(second.block_number, 100);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_grpc_get_logs_rejects_invalid_address() {
        let (_tmp, storage) = setup_storage();
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let service = LogExGrpcService::new(state);

        let request = Request::new(GetLogsRequest {
            from_block: None,
            to_block: None,
            block_hash: Vec::new(),
            addresses: vec![vec![0x01, 0x02]],
            topics: Vec::new(),
            canonical_only: None,
            descending: None,
            limit: None,
            offset: None,
        });

        let error = service
            .get_logs(request)
            .await
            .expect_err("invalid address must fail");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
