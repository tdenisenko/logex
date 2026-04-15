use std::time::Instant;

use alloy_primitives::{Address, B256, bytes};
use logex_index::IndexBuilder;
use logex_query::{execute_log_filter, execute_sql};
use logex_storage::native::{NativeLogFilter, TopicConstraint};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};

const TRANSFER_TOPIC0: B256 = B256::new([
    0xdd, 0xf2, 0x52, 0xad, 0x1b, 0xe2, 0xc8, 0x9b, 0x69, 0xc2, 0xb0, 0x68, 0xfc, 0x37, 0x8d, 0xaa,
    0x95, 0x2b, 0xa7, 0xf1, 0x63, 0xc4, 0xa1, 0x16, 0x28, 0xf5, 0x5a, 0x4d, 0xf5, 0x23, 0xb3, 0xef,
]);

fn benchmark_row_count() -> usize {
    std::env::var("LOGEX_BENCH_ROWS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000)
}

fn padded_topic_address(byte: u8) -> B256 {
    let mut out = [0u8; 32];
    out[12..].copy_from_slice(Address::repeat_byte(byte).as_slice());
    B256::from(out)
}

fn make_transfer_like_rows(count: usize) -> Vec<LogRow> {
    let hot_token = Address::repeat_byte(0xAA);
    let cold_token = Address::repeat_byte(0xBB);

    (0..count)
        .map(|index| {
            let block_number = 15_000_000 + (index / 4) as u64;
            let token = if index % 5 == 0 {
                cold_token
            } else {
                hot_token
            };
            let from = padded_topic_address((index % 251) as u8);
            let to = padded_topic_address(((index + 17) % 251) as u8);
            let data = if index % 3 == 0 {
                bytes!("0000000000000000000000000000000000000000000000000000000000000064")
            } else {
                bytes!("0000000000000000000000000000000000000000000000000000000000000001")
            };

            LogRow {
                block_number,
                block_hash: B256::repeat_byte((index % 251) as u8),
                timestamp: 1_700_000_000 + (block_number - 15_000_000) * 12,
                tx_hash: B256::repeat_byte(((index + 31) % 251) as u8),
                tx_index: (index % 4) as u32,
                log_index: index as u32,
                address: token,
                topic0: Some(TRANSFER_TOPIC0),
                topic1: Some(from),
                topic2: Some(to),
                topic3: None,
                data_len: data.len() as u32,
                data,
                source: Source::Receipt,
            }
        })
        .collect()
}

fn setup_storage(count: usize) -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_path_buf(),
        partition_target_rows: 50_000,
        compaction_safety_margin_blocks: 2_048,
    };
    let mut storage = PartitionManager::open(config).unwrap();
    storage
        .write_batch(&make_transfer_like_rows(count))
        .unwrap();

    let sealed: Vec<_> = storage
        .sealed_partitions()
        .iter()
        .map(|partition| (partition.meta.id, partition.meta.path.clone()))
        .collect();
    for (segment_id, path) in sealed {
        IndexBuilder::build_all_indexes(&path).unwrap();
        storage.refresh_segment_indexes(segment_id).unwrap();
    }
    if storage.hot_partition().meta.row_count > 0 {
        IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
        storage
            .refresh_segment_indexes(storage.hot_partition().meta.id)
            .unwrap();
    }

    tmp
}

#[tokio::test]
#[ignore = "benchmark harness; run with --ignored --nocapture and optionally LOGEX_BENCH_ROWS"]
async fn benchmark_native_and_sql_queries() {
    let rows = benchmark_row_count();
    let tmp = setup_storage(rows);
    let storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_path_buf(),
        partition_target_rows: 50_000,
        compaction_safety_margin_blocks: 2_048,
    })
    .unwrap();

    let mut filter = NativeLogFilter::new();
    filter.from_block = Some(15_000_100);
    filter.to_block = Some(15_020_000);
    filter.addresses = vec![Address::repeat_byte(0xAA)];
    filter.topics[0] = TopicConstraint::One(TRANSFER_TOPIC0);

    let start = Instant::now();
    let logs = execute_log_filter(&storage, &filter).unwrap();
    let native_elapsed = start.elapsed();

    let start = Instant::now();
    let aggregate = execute_sql(
        "SELECT COUNT(*) AS total FROM logs WHERE address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND topic0 = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    let aggregate_elapsed = start.elapsed();

    let start = Instant::now();
    let ordered = execute_sql(
        "SELECT block_number, address, log_index FROM logs WHERE block_number BETWEEN 15000100 AND 15020000 ORDER BY block_number DESC, tx_index DESC, log_index DESC LIMIT 1000",
        &storage,
        storage.head_block(),
    )
    .await
    .unwrap();
    let ordered_elapsed = start.elapsed();

    eprintln!(
        "native_filter rows={} elapsed_ms={} rows_per_sec={:.2}",
        logs.len(),
        native_elapsed.as_millis(),
        logs.len() as f64 / native_elapsed.as_secs_f64()
    );
    eprintln!(
        "sql_aggregate rows={} scanned={} elapsed_ms={}",
        aggregate.rows.len(),
        aggregate.total_scanned,
        aggregate_elapsed.as_millis()
    );
    eprintln!(
        "sql_ordered rows={} scanned={} elapsed_ms={}",
        ordered.rows.len(),
        ordered.total_scanned,
        ordered_elapsed.as_millis()
    );

    assert!(!logs.is_empty());
    assert_eq!(aggregate.rows.len(), 1);
    assert!(!ordered.rows.is_empty());
}
