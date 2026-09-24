use std::{future::poll_fn, pin::Pin, sync::Arc};

use axum::{
    body::Body,
    http::{Request as HttpRequest, Response as HttpResponse, StatusCode},
};
use bytes::Bytes;
use http_body::{Body as _, Frame};
use logex_types::{LogRow, QueryMemoryLimit, SyncStatus};
use prost::Message as _;
use tempfile::TempDir;
use tonic::{Code, Status, body::Body as TonicBody};
use tower::ServiceExt as _;

use super::{
    grpc_service,
    pb::{
        Empty, GetLogsRequest, LogEntry, QueryRequest, QueryResponse, QueryRow,
        log_ex_service_client::LogExServiceClient,
    },
    tests::{make_test_rows, setup_storage},
};
use crate::handler::{AppState, QueryAdmissionError, QueryConcurrencyLimit};

const TONIC_FIRST_OUTPUT_CAPACITY: usize = 8 * 1024;
const COMPETING_STAGE: &str = "test competing query";

#[derive(Clone, Copy, Debug)]
enum AccountedRoute {
    Query,
    GetLogs,
    StreamLogs,
}

impl AccountedRoute {
    const ALL: [Self; 3] = [Self::Query, Self::GetLogs, Self::StreamLogs];

    fn method(self) -> &'static str {
        match self {
            Self::Query => "Query",
            Self::GetLogs => "GetLogs",
            Self::StreamLogs => "StreamLogs",
        }
    }

    fn payload(self) -> Vec<u8> {
        match self {
            Self::Query => QueryRequest {
                sql: "SELECT block_number FROM logs ORDER BY block_number".to_owned(),
                limit: None,
                offset: None,
            }
            .encode_to_vec(),
            Self::GetLogs | Self::StreamLogs => GetLogsRequest::default().encode_to_vec(),
        }
    }

    fn request(self) -> HttpRequest<Body> {
        let payload = self.payload();
        let mut framed = Vec::with_capacity(5 + payload.len());
        framed.push(0);
        framed.extend_from_slice(
            &u32::try_from(payload.len())
                .expect("small test message")
                .to_be_bytes(),
        );
        framed.extend_from_slice(&payload);
        HttpRequest::builder()
            .method("POST")
            .uri(format!("/logex.LogExService/{}", self.method()))
            .header("content-type", "application/grpc")
            .body(Body::from(framed))
            .unwrap()
    }

    async fn response(self, state: &Arc<AppState>) -> HttpResponse<TonicBody> {
        let response = grpc_service(Arc::clone(state))
            .oneshot(self.request())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{self:?}");
        response
    }
}

fn limited_state() -> (TempDir, Arc<AppState>) {
    let (tmp, storage) = setup_storage();
    let state = Arc::new(AppState::with_query_limits(
        storage,
        None,
        SyncStatus::default(),
        QueryConcurrencyLimit::new(1).unwrap(),
        QueryMemoryLimit::default(),
    ));
    (tmp, state)
}

fn expected_entry(row: &LogRow) -> LogEntry {
    LogEntry {
        block_number: row.block_number,
        block_hash: row.block_hash.as_slice().to_vec(),
        timestamp: row.timestamp,
        tx_hash: row.tx_hash.as_slice().to_vec(),
        tx_index: row.tx_index,
        log_index: row.log_index,
        address: row.address.as_slice().to_vec(),
        topics: [row.topic0, row.topic1, row.topic2, row.topic3]
            .into_iter()
            .flatten()
            .map(|topic| topic.as_slice().to_vec())
            .collect(),
        data: row.data.to_vec(),
        data_len: row.data_len,
        source: row.source as u32,
    }
}

async fn next_frame(body: &mut TonicBody) -> Option<Result<Frame<Bytes>, Status>> {
    poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await
}

async fn expect_terminal_status(body: &mut TonicBody, expected: Code) {
    let frame = next_frame(body).await.unwrap().unwrap();
    let trailers = frame
        .into_trailers()
        .expect("encoding failure must not emit successful response data");
    let status = Status::from_header_map(&trailers).expect("gRPC status trailers");
    assert_eq!(status.code(), expected);
    assert!(body.is_end_stream());
    assert!(next_frame(body).await.is_none());
    assert!(next_frame(body).await.is_none());
}

#[tokio::test]
async fn empty_stream_observes_storage_failure_before_success_trailers() {
    let (_tmp, state) = limited_state();
    let filter = GetLogsRequest {
        from_block: Some(1_000),
        to_block: Some(1_000),
        ..Default::default()
    }
    .encode_to_vec();
    let mut wire = vec![0];
    wire.extend_from_slice(&u32::try_from(filter.len()).unwrap().to_be_bytes());
    wire.extend_from_slice(&filter);
    let mut request = AccountedRoute::StreamLogs.request();
    *request.body_mut() = Body::from(wire);
    let response = grpc_service(Arc::clone(&state))
        .oneshot(request)
        .await
        .unwrap();
    state.mark_storage_unavailable("fixture volume unavailable");
    let mut body = response.into_body();
    expect_terminal_status(&mut body, Code::Unavailable).await;
    assert_eq!(state.query_memory.used(), 0);
}

#[tokio::test]
async fn generated_client_routes_match_expected_values() {
    let expected_rows = make_test_rows();
    let expected_logs = expected_rows.iter().map(expected_entry).collect::<Vec<_>>();
    let (tmp, storage) = setup_storage();
    let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
    let mut client = LogExServiceClient::new(grpc_service(Arc::clone(&state)));

    let query = client
        .query(QueryRequest {
            sql: "SELECT block_number FROM logs ORDER BY block_number".to_owned(),
            limit: Some(1),
            offset: Some(1),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        query,
        QueryResponse {
            rows: vec![QueryRow {
                json: r#"{"block_number":200}"#.to_owned(),
            }],
            total_scanned: 2,
            row_count: 1,
            limit: 1,
            offset: 1,
            next_offset: Some(2),
            max_limit: 0,
        }
    );

    let head = client.get_head_block(Empty {}).await.unwrap().into_inner();
    assert_eq!(head.block_number, 200);

    let logs = client
        .get_logs(GetLogsRequest::default())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(logs.row_count, 2);
    assert_eq!(logs.logs, expected_logs);

    let mut stream = client
        .stream_logs(GetLogsRequest::default())
        .await
        .unwrap()
        .into_inner();
    let mut streamed = Vec::new();
    while let Some(entry) = stream.message().await.unwrap() {
        streamed.push(entry);
    }
    assert_eq!(streamed, expected_logs);
    drop(stream);
    assert_eq!(state.query_memory.used(), 0);
    drop(tmp);
}

#[tokio::test]
async fn protocol_charge_exists_before_poll_and_unpolled_drop_releases_it() {
    for route in AccountedRoute::ALL {
        let (_tmp, state) = limited_state();
        let response = route.response(&state).await;
        assert!(state.query_memory.used() > 0, "{route:?}");
        assert!(matches!(
            state.query_control.start_concurrent(),
            Err(QueryAdmissionError::Capacity)
        ));

        drop(response);
        assert_eq!(state.query_memory.used(), 0, "{route:?}");
        assert!(state.query_control.start_concurrent().is_ok(), "{route:?}");
    }
}

#[tokio::test]
async fn frame_aliases_retain_full_encoding_charge_and_admission() {
    for route in AccountedRoute::ALL {
        let (_tmp, state) = limited_state();
        let response = route.response(&state).await;
        let protocol_charge = state.query_memory.used();
        assert!(protocol_charge > 0, "{route:?}");

        let mut body = response.into_body();
        let frame = next_frame(&mut body).await.unwrap().unwrap();
        let bytes = frame.into_data().expect("successful response data");
        assert!(!bytes.is_empty(), "{route:?}");
        let clone = bytes.clone();
        let slice = bytes.slice(1..);

        drop(body);
        let output_charge = state.query_memory.used();
        assert!(
            output_charge >= TONIC_FIRST_OUTPUT_CAPACITY as u128,
            "{route:?}: the complete backing capacity must remain charged"
        );
        assert!(matches!(
            state.query_control.start_concurrent(),
            Err(QueryAdmissionError::Capacity)
        ));

        drop(bytes);
        drop(clone);
        assert_eq!(state.query_memory.used(), output_charge, "{route:?}");
        assert!(matches!(
            state.query_control.start_concurrent(),
            Err(QueryAdmissionError::Capacity)
        ));
        drop(slice);
        assert_eq!(state.query_memory.used(), 0, "{route:?}");
        assert!(state.query_control.start_concurrent().is_ok(), "{route:?}");
    }
}

#[tokio::test]
async fn encoding_capacity_refusal_is_terminal_and_recoverable() {
    for route in AccountedRoute::ALL {
        let (_tmp, state) = limited_state();
        let response = route.response(&state).await;
        let protocol_charge = state.query_memory.used();
        let available = (state.query_memory.limit() as u128)
            .checked_sub(protocol_charge)
            .unwrap();
        assert!(
            available >= TONIC_FIRST_OUTPUT_CAPACITY as u128,
            "{route:?}"
        );
        let competing_bytes =
            usize::try_from(available - (TONIC_FIRST_OUTPUT_CAPACITY as u128 - 1)).unwrap();
        let competitor = state
            .query_memory
            .reserve(competing_bytes, COMPETING_STAGE)
            .unwrap();
        assert_eq!(
            state.query_memory.limit() as u128 - state.query_memory.used(),
            (TONIC_FIRST_OUTPUT_CAPACITY - 1) as u128,
            "{route:?}"
        );

        let mut body = response.into_body();
        expect_terminal_status(&mut body, Code::ResourceExhausted).await;
        assert_eq!(
            state.query_memory.used(),
            competitor.bytes(),
            "{route:?}: failed encoding must release pending protocol values"
        );
        assert!(matches!(
            state.query_control.start_concurrent(),
            Err(QueryAdmissionError::Capacity)
        ));
        drop(body);
        assert_eq!(state.query_memory.used(), competitor.bytes(), "{route:?}");
        assert!(state.query_control.start_concurrent().is_ok(), "{route:?}");
        drop(competitor);
        assert_eq!(state.query_memory.used(), 0, "{route:?}");

        let response = route.response(&state).await;
        let mut body = response.into_body();
        let frame = next_frame(&mut body).await.unwrap().unwrap();
        let bytes = frame.into_data().expect("encoding must recover");
        assert!(!bytes.is_empty(), "{route:?}");
        drop(body);
        assert!(state.query_memory.used() > 0, "{route:?}");
        drop(bytes);
        assert_eq!(state.query_memory.used(), 0, "{route:?}");
        assert!(state.query_control.start_concurrent().is_ok(), "{route:?}");
    }
}

#[tokio::test]
async fn storage_failure_after_handler_response_prevents_lazy_encoding() {
    for route in AccountedRoute::ALL {
        let (_tmp, state) = limited_state();
        let response = route.response(&state).await;
        assert!(state.query_memory.used() > 0, "{route:?}");
        assert!(matches!(
            state.query_control.start_concurrent(),
            Err(QueryAdmissionError::Capacity)
        ));

        state.mark_storage_unavailable("test storage failure");
        let mut body = response.into_body();
        expect_terminal_status(&mut body, Code::Unavailable).await;
        assert_eq!(
            state.query_memory.used(),
            0,
            "{route:?}: rejected response must release pending protocol values"
        );
    }
}
