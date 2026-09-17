//! Public native-query checks using only owned temporary storage and fixture rows.

use alloy_primitives::{Address, B256, Bytes, keccak256};
use logex_index::IndexBuilder;
use logex_query::execute_log_filter;
use logex_storage::native::{LogOrder, NativeLogFilter};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use tempfile::TempDir;

fn row(block: u64, timestamp: u64) -> LogRow {
    LogRow {
        block_number: block,
        block_hash: keccak256(block.to_le_bytes()),
        timestamp,
        tx_hash: B256::repeat_byte(1),
        tx_index: 0,
        log_index: 0,
        address: Address::repeat_byte(0xaa),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: Bytes::new(),
        data_len: 0,
        source: Source::Receipt,
    }
}

fn verify_point_queries(storage: &PartitionManager, input: &[LogRow], by_timestamp: bool) {
    let mut expected = input.to_vec();
    expected.sort_by_key(|row| row.block_number);
    assert_eq!(
        execute_log_filter(storage, &NativeLogFilter::new()).unwrap(),
        expected,
        "an unrestricted query must confirm all fixture rows are present"
    );
    for row in input {
        let filter = if by_timestamp {
            NativeLogFilter::new().with_timestamp_range(Some(row.timestamp), Some(row.timestamp))
        } else {
            NativeLogFilter::new().with_block_range(Some(row.block_number), Some(row.block_number))
        };
        assert_eq!(
            execute_log_filter(storage, &filter).unwrap(),
            vec![row.clone()],
            "segment bounds must include interior row for {filter:?}"
        );
    }
}

fn historical_point_queries(target: u64, by_timestamp: bool) {
    let directory = TempDir::new().unwrap();
    let config = PartitionManagerConfig {
        data_dir: directory.path().to_owned(),
        partition_target_rows: target,
        compaction_safety_margin_blocks: 0,
    };
    let input = vec![row(100, 1000), row(1, 10), row(900, 9000), row(101, 1010)];
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    storage.write_historical_batch(&input).unwrap();
    verify_point_queries(&storage, &input, by_timestamp);

    storage.finalize_historical_segment().unwrap();
    for partition in storage.sealed_partitions() {
        IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
    }
    verify_point_queries(&storage, &input, by_timestamp);
    storage.checkpoint().unwrap();
    drop(storage);
    let reopened = PartitionManager::open(config).unwrap();
    verify_point_queries(&reopened, &input, by_timestamp);
}

#[test]
fn historical_dense_bounds_preserve_block_query_results() {
    historical_point_queries(4, false);
}

#[test]
fn historical_staged_bounds_preserve_block_query_results() {
    historical_point_queries(32, false);
}

#[test]
fn historical_dense_bounds_preserve_timestamp_query_results() {
    historical_point_queries(4, true);
}

#[test]
fn historical_staged_bounds_preserve_timestamp_query_results() {
    historical_point_queries(32, true);
}

fn historical_ordered_page(order: LogOrder, expected: u64) {
    let directory = TempDir::new().unwrap();
    let config = PartitionManagerConfig {
        data_dir: directory.path().to_owned(),
        partition_target_rows: 4,
        compaction_safety_margin_blocks: 0,
    };
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    for blocks in [[100, 1, 900, 101], [30, 400, 450, 500]] {
        let rows: Vec<_> = blocks
            .into_iter()
            .map(|block| row(block, block * 10))
            .collect();
        storage.write_historical_batch(&rows).unwrap();
    }
    assert_eq!(storage.sealed_count(), 2);
    for reopen in [false, true] {
        if reopen {
            storage.checkpoint().unwrap();
            drop(storage);
            storage = PartitionManager::open(config.clone()).unwrap();
        }
        let mut filter = NativeLogFilter::new();
        filter.order = order;
        filter.limit = Some(1);
        assert_eq!(
            execute_log_filter(&storage, &filter).unwrap(),
            vec![row(expected, expected * 10)],
            "partition ordering and early completion must include interior extrema"
        );
    }
}

#[test]
fn historical_bounds_preserve_global_first_row() {
    historical_ordered_page(LogOrder::Ascending, 1);
}

#[test]
fn historical_bounds_preserve_global_last_row() {
    historical_ordered_page(LogOrder::Descending, 900);
}
