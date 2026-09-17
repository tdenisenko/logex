use alloy_primitives::{Address, B256, Bytes};
use axum::{body::Body, http::Request};
use logex_index::IndexBuilder;
use logex_server::{
    AppState,
    grpc::{
        LogExGrpcService,
        pb::{GetLogsRequest, TopicFilter, log_ex_service_server::LogExService},
    },
};
use logex_storage::{IndexReadCheckpoint, PartitionManager, PartitionManagerConfig, SegmentReader};
use logex_types::{LogRow, Source, SyncStatus};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_stream::StreamExt;
use tower::ServiceExt;

fn rows() -> Vec<LogRow> {
    (0..5)
        .map(|n| LogRow {
            block_number: 9 + n / 2,
            block_hash: B256::repeat_byte(if n < 2 { 1 } else { 2 }),
            timestamp: 100 + n,
            tx_hash: B256::repeat_byte(n as u8 + 1),
            tx_index: n as u32,
            log_index: n as u32,
            address: Address::repeat_byte(if n % 2 == 0 { 0xAA } else { 0xBB }),
            topic0: (n > 0).then_some(B256::ZERO),
            topic1: (n > 1).then_some(B256::repeat_byte(1)),
            topic2: (n > 2).then_some(B256::repeat_byte(2)),
            topic3: (n > 3).then_some(B256::repeat_byte(3)),
            data: Bytes::new(),
            data_len: 0,
            source: Source::Receipt,
        })
        .collect()
}

#[derive(Clone, Copy, Debug)]
enum Fixture {
    HotUnindexed,
    HotIndexed,
    SealedIndexed,
}

fn setup(indexed: bool) -> (tempfile::TempDir, Arc<AppState>) {
    setup_mode(if indexed {
        Fixture::HotIndexed
    } else {
        Fixture::HotUnindexed
    })
}

fn setup_mode(mode: Fixture) -> (tempfile::TempDir, Arc<AppState>) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: dir.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    if matches!(mode, Fixture::SealedIndexed) {
        storage.write_historical_batch(&rows()).unwrap();
        storage.finalize_historical_segment().unwrap();
        assert!(!storage.sealed_partitions().is_empty());
    } else {
        storage.write_batch(&rows()).unwrap();
    }
    let paths: Vec<_> = storage
        .sealed_partitions()
        .iter()
        .chain(std::iter::once(storage.hot_partition()))
        .filter(|partition| partition.meta.row_count > 0)
        .map(|partition| partition.meta.path.clone())
        .collect();
    for path in paths {
        if !matches!(mode, Fixture::HotUnindexed) {
            IndexBuilder::build_all_indexes(&path).unwrap();
        }
        let reader = SegmentReader::open(&path).unwrap();
        let checkpoint = IndexReadCheckpoint::open(&path, &reader).unwrap();
        let indexed = checkpoint
            .as_ref()
            .and_then(|checkpoint| checkpoint.artifact_id("topic0.bptree"))
            .is_some();
        assert_eq!(
            indexed,
            !matches!(mode, Fixture::HotUnindexed),
            "{mode:?} must exercise the intended index path"
        );
    }
    (
        dir,
        Arc::new(AppState::new(storage, None, SyncStatus::default())),
    )
}

async fn rpc(state: Arc<AppState>, filter: &str) -> Value {
    let body = format!(r#"{{"jsonrpc":"2.0","method":"eth_getLogs","params":[{filter}],"id":7}}"#);
    let request = Request::builder()
        .method("POST")
        .uri("/")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    let response = logex_server::build_router(state)
        .oneshot(request)
        .await
        .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 65536)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn ids(response: &Value) -> Vec<u32> {
    response["result"]
        .as_array()
        .unwrap_or_else(|| panic!("{response}"))
        .iter()
        .map(|row| {
            u32::from_str_radix(
                row["logIndex"].as_str().unwrap().trim_start_matches("0x"),
                16,
            )
            .unwrap()
        })
        .collect()
}

#[tokio::test]
async fn literal_filter_shapes_reject_invalid_nested_members() {
    let (_dir, state) = setup(false);
    let mut mismatches = Vec::new();
    for filter in [
        "[null,null,null,[],null,null,0]".to_owned(),
        format!(r#"{{"topics":[["0x{}",true]]}}"#, "00".repeat(32)),
        r#"{"address":{"$serde_json::private::RawValue":"\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""}}"#.into(),
        r#"{"topics":[null,null,null,null,null]}"#.into(),
        r#"{"topics":[[null,true]]}"#.into(),
        r#"{"topics":[[12]]}"#.into(),
        r#"{"topics":[[{}]]}"#.into(),
        r#"{"topics":[[[]]]}"#.into(),
    ] {
        let response=rpc(Arc::clone(&state),&filter).await;
        if response["error"]["code"] != -32602 {
            mismatches.push(format!("filter={filter}, code={}",response["error"]["code"]));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[tokio::test]
async fn private_filter_extension_cannot_inject_real_constraints() {
    let (_dir, state) = setup(false);
    let filter = r#"{"$serde_json::private::RawValue":"{\"address\":\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"}"}"#;
    assert_eq!(ids(&rpc(state, filter).await), vec![0, 1, 2, 3, 4]);
}

#[tokio::test]
async fn wildcard_positions_require_topic_presence_in_http_and_grpc() {
    let mut mismatches = Vec::new();
    for indexed in [false, true] {
        let (_dir, state) = setup(indexed);
        for (topics, count) in [
            (json!([null]), 1),
            (json!([[]]), 1),
            (json!([null, null]), 2),
            (
                json!([[
                    null,
                    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
                ]]),
                1,
            ),
        ] {
            let expected: Vec<_> = (count..5).collect();
            let response = rpc(Arc::clone(&state), &json!({"topics":topics}).to_string()).await;
            let actual = response.get("result").map(|_| ids(&response));
            if actual.as_ref() != Some(&expected) {
                mismatches.push(format!(
                    "HTTP topics={topics},indexed={indexed},actual={actual:?},expected={expected:?}"
                ));
            }
            let request = GetLogsRequest {
                topics: (0..count)
                    .map(|_| TopicFilter {
                        match_any: false,
                        any_of: vec![],
                    })
                    .collect(),
                ..Default::default()
            };
            let grpc = LogExGrpcService::new(Arc::clone(&state));
            let got = grpc
                .get_logs(tonic::Request::new(request.clone()))
                .await
                .unwrap()
                .into_inner()
                .logs
                .into_iter()
                .map(|r| r.log_index)
                .collect::<Vec<_>>();
            if got != expected {
                mismatches.push(format!(
                    "gRPC topics={topics},indexed={indexed},actual={got:?},expected={expected:?}"
                ));
            }
            let mut stream = grpc
                .stream_logs(tonic::Request::new(request))
                .await
                .unwrap()
                .into_inner();
            let mut got = Vec::new();
            while let Some(row) = stream.next().await {
                got.push(row.unwrap().log_index);
            }
            if got != expected {
                mismatches.push(format!(
                    "stream topics={topics},indexed={indexed},actual={got:?},expected={expected:?}"
                ));
            }
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[tokio::test]
async fn unsupported_named_bounds_are_not_latest_aliases() {
    let (_dir, state) = setup(false);
    let mut mismatches = Vec::new();
    for tag in ["safe", "finalized", "pending"] {
        let response = rpc(Arc::clone(&state), &json!({"toBlock":tag}).to_string()).await;
        if response["error"]["code"] != -32602 {
            mismatches.push(format!("tag={tag},code={}", response["error"]["code"]));
        }
    }
    assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
}

#[derive(Default)]
struct OracleCase {
    wire: Value,
    addresses: Vec<Address>,
    positions: Vec<Option<Vec<B256>>>,
    from: Option<u64>,
    to: Option<u64>,
    hash: Option<B256>,
    offset: usize,
    limit: Option<usize>,
}

fn expected_rows(case: &OracleCase) -> Vec<u32> {
    // Test-owned Ethereum predicate, independent of production decoding/conversion/matching.
    rows()
        .into_iter()
        .filter(|row| {
            let topics = [row.topic0, row.topic1, row.topic2, row.topic3];
            (case.addresses.is_empty() || case.addresses.contains(&row.address))
                && case.from.is_none_or(|from| row.block_number >= from)
                && case.to.is_none_or(|to| row.block_number <= to)
                && case.hash.is_none_or(|hash| row.block_hash == hash)
                && case.positions.iter().enumerate().all(|(index, allowed)| {
                    topics[index].is_some_and(|topic| {
                        allowed
                            .as_ref()
                            .is_none_or(|allowed| allowed.is_empty() || allowed.contains(&topic))
                    })
                })
        })
        .skip(case.offset)
        .take(case.limit.unwrap_or(usize::MAX))
        .map(|row| row.log_index)
        .collect()
}

#[tokio::test]
async fn independent_filter_oracle_agrees_across_protocols_and_storage_plans() {
    use logex_server::eth_filter::EthFilter;
    use logex_storage::native::{NativeLogFilter, TopicConstraint};
    let cases = [
        OracleCase {
            wire: json!({}),
            ..Default::default()
        },
        OracleCase {
            wire: json!({"address":null,"topics":null}),
            ..Default::default()
        },
        OracleCase {
            wire: json!({"address":[],"topics":[]}),
            ..Default::default()
        },
        OracleCase {
            wire: json!({"topics":[null]}),
            positions: vec![None],
            ..Default::default()
        },
        OracleCase {
            wire: json!({"topics":[[]]}),
            positions: vec![Some(vec![])],
            ..Default::default()
        },
        OracleCase {
            wire: json!({"topics":[null,null,null,null]}),
            positions: vec![None; 4],
            ..Default::default()
        },
        OracleCase {
            wire: json!({"topics":[format!("{:#x}",B256::ZERO)]}),
            positions: vec![Some(vec![B256::ZERO])],
            ..Default::default()
        },
        OracleCase {
            wire: json!({"topics":[[format!("{:#x}",B256::ZERO),format!("{:#x}",B256::repeat_byte(9))]]}),
            positions: vec![Some(vec![B256::ZERO, B256::repeat_byte(9)])],
            ..Default::default()
        },
        OracleCase {
            wire: json!({"topics":[[null,format!("{:#x}",B256::repeat_byte(9))]]}),
            positions: vec![None],
            ..Default::default()
        },
        OracleCase {
            wire: json!({"address":format!("{:#x}",Address::repeat_byte(0xAA)),"topics":[null,null]}),
            addresses: vec![Address::repeat_byte(0xAA)],
            positions: vec![None; 2],
            ..Default::default()
        },
        OracleCase {
            wire: json!({"fromBlock":"0x9","toBlock":"0xa"}),
            from: Some(9),
            to: Some(10),
            ..Default::default()
        },
        OracleCase {
            wire: json!({"fromBlock":"0xb","toBlock":"0x9"}),
            from: Some(11),
            to: Some(9),
            ..Default::default()
        },
        OracleCase {
            wire: json!({"blockHash":format!("{:#x}",B256::repeat_byte(1))}),
            hash: Some(B256::repeat_byte(1)),
            ..Default::default()
        },
        OracleCase {
            wire: json!({"topics":[null],"offset":1,"limit":2}),
            positions: vec![None],
            offset: 1,
            limit: Some(2),
            ..Default::default()
        },
    ];
    for mode in [
        Fixture::HotUnindexed,
        Fixture::HotIndexed,
        Fixture::SealedIndexed,
    ] {
        let (_dir, state) = setup_mode(mode);
        for case in &cases {
            let expected = expected_rows(case);
            assert_eq!(
                ids(&rpc(Arc::clone(&state), &case.wire.to_string()).await),
                expected,
                "HTTP {mode:?} {}",
                case.wire
            );
            let request = GetLogsRequest {
                from_block: case.from,
                to_block: case.to,
                block_hash: case
                    .hash
                    .map(|hash| hash.as_slice().to_vec())
                    .unwrap_or_default(),
                addresses: case
                    .addresses
                    .iter()
                    .map(|address| address.as_slice().to_vec())
                    .collect(),
                topics: case
                    .positions
                    .iter()
                    .map(|position| TopicFilter {
                        match_any: false,
                        any_of: position
                            .as_ref()
                            .map(|hashes| {
                                hashes.iter().map(|hash| hash.as_slice().to_vec()).collect()
                            })
                            .unwrap_or_default(),
                    })
                    .collect(),
                offset: Some(case.offset as u64),
                limit: case.limit.map(|limit| limit as u64),
                ..Default::default()
            };
            let grpc = LogExGrpcService::new(Arc::clone(&state));
            let got = grpc
                .get_logs(tonic::Request::new(request.clone()))
                .await
                .unwrap()
                .into_inner()
                .logs
                .into_iter()
                .map(|row| row.log_index)
                .collect::<Vec<_>>();
            assert_eq!(got, expected, "gRPC {mode:?} {}", case.wire);
            let mut stream = grpc
                .stream_logs(tonic::Request::new(request))
                .await
                .unwrap()
                .into_inner();
            let mut got = Vec::new();
            while let Some(row) = stream.next().await {
                got.push(row.unwrap().log_index);
            }
            assert_eq!(got, expected, "stream {mode:?} {}", case.wire);
            let filter: EthFilter = serde_json::from_str(&case.wire.to_string()).unwrap();
            filter.validate().unwrap();
            let storage = state.storage.read().await;
            let mut native = filter.to_native_filter(storage.head_block().unwrap_or(0));
            native.limit = case.limit;
            native.offset = case.offset;
            let got = logex_query::execute_log_filter(&storage, &native)
                .unwrap()
                .into_iter()
                .map(|row| row.log_index)
                .collect::<Vec<_>>();
            assert_eq!(got, expected, "native {mode:?} {}", case.wire);
        }
        let storage = state.storage.read().await;
        assert_eq!(
            logex_query::execute_log_filter(&storage, &NativeLogFilter::new())
                .unwrap()
                .len(),
            5
        );
        let empty = NativeLogFilter::new().with_topic(0, TopicConstraint::AnyOf(vec![]));
        assert!(
            logex_query::execute_log_filter(&storage, &empty)
                .unwrap()
                .is_empty()
        );
    }
}

#[tokio::test]
async fn latest_http_bounds_still_resolve_from_the_query_snapshot() {
    let (_dir, state) = setup(false);
    assert_eq!(
        ids(&rpc(
            Arc::clone(&state),
            r#"{"fromBlock":"latest","toBlock":"latest"}"#
        )
        .await),
        vec![4]
    );
    assert_eq!(
        ids(&rpc(state, r#"{"fromBlock":"earliest","toBlock":"0x9"}"#).await),
        vec![0, 1]
    );
}

#[tokio::test]
async fn invalid_nested_filters_finish_before_storage_admission() {
    use std::future::Future;
    use std::task::Poll;
    let (_dir, state) = setup(false);
    let writer = state.storage.write().await;
    for filter in [
        r#"{"topics":[[null,true]]}"#,
        r#"{"topics":[["0x00"]]}"#,
        r#"{"topics":[{"$serde_json::private::RawValue":"[]"}]}"#,
        r#"{"topics":[[{"$serde_json::private::RawValue":"null"}]]}"#,
        r#"[null,null,null,[],null,null,0]"#,
    ] {
        let mut pending = Box::pin(rpc(Arc::clone(&state), filter));
        let result = std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx))).await;
        let Poll::Ready(response) = result else {
            panic!("invalid filter waited for storage: {filter}");
        };
        assert_eq!(response["error"]["code"], -32602);
        assert_eq!(response["id"], 7);
    }
    drop(writer);
}

#[tokio::test]
async fn grpc_filter_shapes_keep_existing_invalid_argument_contract() {
    let (_dir, state) = setup(false);
    let grpc = LogExGrpcService::new(state);
    for request in [
        GetLogsRequest {
            topics: vec![TopicFilter::default(); 5],
            ..Default::default()
        },
        GetLogsRequest {
            topics: vec![TopicFilter {
                match_any: true,
                any_of: vec![vec![0; 32]],
            }],
            ..Default::default()
        },
        GetLogsRequest {
            block_hash: vec![0; 32],
            from_block: Some(1),
            ..Default::default()
        },
    ] {
        assert_eq!(
            grpc.get_logs(tonic::Request::new(request.clone()))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let error = grpc
            .stream_logs(tonic::Request::new(request))
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
