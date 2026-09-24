use super::*;
use alloy_primitives::{Address, B256};
use logex_types::{QueryMemoryLimit, Source};
use tokio_stream::StreamExt;

fn memory(bytes: usize) -> QueryMemoryBudget {
    QueryMemoryBudget::new(QueryMemoryLimit::new(bytes).unwrap())
}

fn fixture() -> LogRow {
    LogRow {
        block_number: u64::MAX,
        block_hash: B256::repeat_byte(1),
        timestamp: 128,
        tx_hash: B256::repeat_byte(2),
        tx_index: u32::MAX,
        log_index: 0,
        address: Address::repeat_byte(3),
        topic0: Some(B256::repeat_byte(4)),
        topic1: None,
        topic2: None,
        topic3: Some(B256::repeat_byte(5)),
        data: vec![6; 127].into(),
        data_len: 127,
        source: Source::Trace,
    }
}

fn retained_capacity(response: &GetLogsResponse) -> usize {
    response.logs.capacity() * std::mem::size_of::<LogEntry>()
        + response
            .logs
            .iter()
            .map(|row| {
                row.block_hash.capacity()
                    + row.tx_hash.capacity()
                    + row.address.capacity()
                    + row.topics.capacity() * std::mem::size_of::<Vec<u8>>()
                    + row.topics.iter().map(Vec::capacity).sum::<usize>()
                    + row.data.capacity()
            })
            .sum::<usize>()
}

#[test]
fn protocol_values_wire_lengths_and_exact_retained_capacity() {
    let memory = memory(16 * 1024);
    let row = fixture();
    let response = log_response(std::slice::from_ref(&row), &memory, None).unwrap();
    let expected = GetLogsResponse {
        logs: vec![LogEntry {
            block_number: u64::MAX,
            block_hash: vec![1; 32],
            timestamp: 128,
            tx_hash: vec![2; 32],
            tx_index: u32::MAX,
            log_index: 0,
            address: vec![3; 20],
            topics: vec![vec![4; 32], vec![5; 32]],
            data: vec![6; 127],
            data_len: 127,
            source: 1,
        }],
        row_count: 1,
    };
    assert_eq!(*response, expected);
    assert_eq!(response.encode_to_vec(), expected.encode_to_vec());
    assert_eq!(response.encoded_len, expected.encoded_len());
    let capacity = retained_capacity(&response);
    assert_eq!(memory.used(), capacity as u128);
    drop(response);
    assert_eq!(memory.used(), 0);

    let exact = self::memory(capacity);
    let response = log_response(std::slice::from_ref(&row), &exact, None).unwrap();
    assert_eq!(exact.used(), capacity as u128);
    drop(response);
    assert_eq!(exact.used(), 0);
    let short = self::memory(capacity - 1);
    let error = log_response(&[row], &short, None).unwrap_err();
    assert!(crate::query_encoding::is_capacity_error(&error));
    assert_eq!(short.used(), 0);

    for entry in [
        LogEntry::default(),
        LogEntry {
            topics: vec![Vec::new(), vec![0; 128]],
            ..Default::default()
        },
        LogEntry {
            block_number: 127,
            timestamp: 16_384,
            tx_index: 16_383,
            log_index: u32::MAX,
            data_len: u32::MAX,
            source: 1,
            ..Default::default()
        },
    ] {
        assert_eq!(
            entry.checked_encoded_len().unwrap(),
            entry.encode_to_vec().len()
        );
        let response = GetLogsResponse {
            logs: vec![entry],
            row_count: 1,
        };
        assert_eq!(
            response.checked_encoded_len().unwrap(),
            response.encode_to_vec().len()
        );
    }
}

#[tokio::test]
async fn stream_and_returned_items_share_the_original_allocation_owner() {
    let memory = memory(16 * 1024);
    let response = log_response(&[fixture(), fixture()], &memory, None).unwrap();
    let held = memory.used();
    let mut source = response.into_stream();
    let first = source.next().await.unwrap().unwrap();
    let second = source.next().await.unwrap().unwrap();
    assert!(source.next().await.is_none());
    drop(first);
    drop(second);
    assert_eq!(
        memory.used(),
        held,
        "drained iterator still owns vector backing"
    );
    drop(source);
    assert_eq!(memory.used(), 0);

    let response = log_response(&[fixture(), fixture()], &memory, None).unwrap();
    let held = memory.used();
    let mut source = response.into_stream();
    let first = source.next().await.unwrap().unwrap();
    drop(source);
    assert_eq!(
        memory.used(),
        held,
        "a yielded message still owns its nested buffers"
    );
    drop(first);
    assert_eq!(memory.used(), 0);

    let slots = std::mem::size_of::<LogEntry>();
    let mut builder = ResponseBuilder::new(GetLogsResponse::default(), &memory, slots).unwrap();
    builder.message.logs = builder.allocate_vec(1).unwrap();
    let mut empty = builder.finish().unwrap().into_stream();
    assert!(empty.next().await.is_none());
    assert_eq!(memory.used(), slots as u128);
    drop(empty);
    assert_eq!(memory.used(), 0);
}

#[test]
fn allocator_transfers_capacity_and_reconciles_observed_excess_before_refund() {
    let mut original = Vec::with_capacity(64);
    original.extend_from_slice(b"fixture");
    let pointer = original.as_ptr();
    let capacity = original.capacity();
    let transferred = Bytes::from(original).try_into_mut().unwrap();
    assert_eq!(
        transferred.as_ptr(),
        pointer,
        "pinned conversion must not copy"
    );
    assert_eq!(transferred.capacity(), capacity);
    drop(transferred);

    let memory = memory(512);
    let allocation = OutputAllocator(memory.clone()).allocate_buffer(64).unwrap();
    assert!(allocation.capacity() >= 64);
    assert!(allocation.is_empty());
    assert_eq!(memory.used(), allocation.capacity() as u128);
    drop(allocation);
    assert_eq!(memory.used(), 0);
    let empty = OutputAllocator(memory.clone()).allocate_buffer(0).unwrap();
    assert_eq!(empty.capacity(), 0);
    drop(empty);

    let memory = self::memory(16);
    let mut allocation = Allocation {
        value: Vec::<u8>::with_capacity(32),
        charge: memory.reserve(8, ENCODING_STAGE).unwrap(),
    };
    let actual = allocation.value.capacity();
    let error = reconcile(&memory, &mut allocation.charge, 8, actual, ENCODING_STAGE).unwrap_err();
    assert!(crate::query_encoding::is_capacity_error(&error));
    assert_eq!(memory.used(), actual as u128);
    drop(allocation);
    assert_eq!(memory.used(), 0);
}

#[test]
fn cancellation_releases_partially_constructed_native_protocol() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let memory = memory(16 * 1024);
    let checks = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&checks);
    // Preflight succeeds; cancellation occurs while copying nested fields.
    let cancel: QueryCancelCheck = Arc::new(move || observed.fetch_add(1, Ordering::Relaxed) >= 5);
    let error = log_response(&[fixture()], &memory, Some(&cancel)).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(memory.used(), 0);
    assert!(checks.load(Ordering::Relaxed) > 5);
}

#[tokio::test]
async fn cancellation_at_preparation_allocation_and_encode_boundaries_discards_output() {
    use http_body::Body;
    use std::{
        future::poll_fn,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use tonic::codec::EncodeBody;
    for stop_at in 1..=4 {
        let memory = memory(16 * 1024);
        let checks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&checks);
        let response = log_response(&[fixture()], &memory, None)
            .unwrap()
            .with_encoding_check(move || {
                if observed.fetch_add(1, Ordering::Relaxed) + 1 >= stop_at {
                    Err(Status::cancelled("fixture cancellation"))
                } else {
                    Ok(())
                }
            });
        let encoder =
            OwnedProstCodec::<GetLogsResponse, super::super::pb::Empty>::default().encoder();
        let mut body = EncodeBody::new_server(
            encoder,
            tokio_stream::iter([Ok(response)]),
            None,
            Default::default(),
            None,
        );
        let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.trailers_ref().unwrap()["grpc-status"], "1");
        assert!(body.is_end_stream());
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
        assert_eq!(checks.load(Ordering::Relaxed), stop_at);
        assert_eq!(memory.used(), 0);
    }
}

#[tokio::test]
async fn empty_and_drained_streams_check_cancellation_before_completion() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for rows in [Vec::new(), vec![fixture()]] {
        let memory = memory(16 * 1024);
        let canceled = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&canceled);
        let response = log_response(&rows, &memory, None)
            .unwrap()
            .with_encoding_check(move || {
                if observed.load(Ordering::Relaxed) {
                    Err(Status::cancelled("fixture cancellation"))
                } else {
                    Ok(())
                }
            });
        let mut source = response.into_stream();
        if !rows.is_empty() {
            drop(source.next().await.unwrap().unwrap());
        }
        canceled.store(true, Ordering::Relaxed);
        assert_eq!(
            source.next().await.unwrap().unwrap_err().code(),
            tonic::Code::Cancelled
        );
        assert!(source.next().await.is_none());
        assert!(source.next().await.is_none());
        drop(source);
        assert_eq!(memory.used(), 0);
    }
}
