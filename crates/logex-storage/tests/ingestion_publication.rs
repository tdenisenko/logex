//! Storage calls made by live/historical sync, including progress publication.
//! Synthetic headers are already-validated inputs here, not consensus fixtures.
use std::time::Instant;

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bytes, keccak256};
use logex_storage::{PartitionManager, PartitionManagerConfig, SegmentReader};
use logex_types::{ExecutionAnchor, LogRow, Source};
use serde_json::json;

struct Config {
    blocks: usize,
    warm_headers: usize,
    rows_per_block: usize,
    history_batch_blocks: usize,
    segment_rows: u64,
    repeats: usize,
}

// SyncEngine's retained canonical-header window.
const RECENT_HEADER_WINDOW: usize = 8_192;

fn positive_env(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or_else(|| panic!("{name} must be a positive integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("invalid {name}: {error}"),
    }
}

fn fixture(config: &Config) -> (Vec<Header>, Vec<Vec<LogRow>>) {
    let mut headers = Vec::with_capacity(config.warm_headers + config.blocks);
    let mut blocks = Vec::with_capacity(config.blocks);
    let mut parent_hash = B256::ZERO;
    for index in 0..config.warm_headers + config.blocks {
        let header = Header {
            number: 15_000_000 + index as u64,
            parent_hash,
            timestamp: 1_700_000_000 + index as u64 * 12,
            gas_limit: 30_000_000,
            ..Default::default()
        };
        let hash = header.hash_slow();
        parent_hash = hash;
        headers.push(header.clone());
        if index < config.warm_headers {
            continue;
        }
        let count = if (index - config.warm_headers + 1).is_multiple_of(16) {
            0
        } else {
            config.rows_per_block
        };
        let rows = (0..count)
            .map(|log| {
                let mut key = hash.to_vec();
                key.extend_from_slice(&(log as u64 / 2).to_le_bytes());
                let data = Bytes::copy_from_slice(keccak256(&key).as_slice());
                LogRow {
                    block_number: header.number,
                    block_hash: hash,
                    timestamp: header.timestamp,
                    tx_hash: keccak256(&key),
                    tx_index: u32::try_from(log / 2).unwrap(),
                    log_index: u32::try_from(log).unwrap(),
                    address: Address::repeat_byte((log % 251) as u8),
                    topic0: Some(keccak256("Transfer(address,address,uint256)")),
                    topic1: (log % 2 == 0).then_some(hash),
                    topic2: None,
                    topic3: None,
                    data_len: u32::try_from(data.len()).unwrap(),
                    data,
                    source: Source::Receipt,
                }
            })
            .collect();
        blocks.push(rows);
    }
    (headers, blocks)
}

fn anchor(header: &Header) -> ExecutionAnchor {
    ExecutionAnchor {
        beacon_root: B256::repeat_byte(0x42),
        beacon_slot: header.number,
        block_number: header.number,
        block_hash: header.hash_slow(),
        receipts_root: header.receipts_root,
    }
}

fn assert_rows(storage: &PartitionManager, expected: &[LogRow]) {
    let mut actual: Vec<_> = storage
        .sealed_partitions()
        .iter()
        .chain(std::iter::once(storage.hot_partition()))
        .filter(|partition| partition.meta.row_count > 0)
        .flat_map(|partition| {
            SegmentReader::open(&partition.meta.path)
                .unwrap()
                .read_log_rows(None)
                .unwrap()
        })
        .collect();
    actual.sort_by_key(|row| (row.block_number, row.log_index));
    assert_eq!(actual, expected);
    assert_eq!(storage.total_rows(), expected.len() as u64);
}

fn run(config: Config) {
    let (headers, blocks) = fixture(&config);
    let measured_headers = &headers[config.warm_headers..];
    let expected: Vec<_> = blocks.iter().flatten().cloned().collect();
    // Extraction and historical coalescing occur before the measured storage
    // calls. Each batch includes complete blocks, including zero-log blocks.
    let history: Vec<_> = (0..measured_headers.len())
        .collect::<Vec<_>>()
        .rchunks(config.history_batch_blocks)
        .map(|indices| {
            (
                indices[0],
                indices
                    .iter()
                    .flat_map(|&index| blocks[index].iter().cloned())
                    .collect::<Vec<_>>(),
            )
        })
        .collect();
    let tip = headers.last().unwrap();
    println!(
        "{}",
        json!({"kind":"config", "fixture_version":1, "workload":"sync_storage_publication",
            "blocks":config.blocks,"rows_per_nonempty_block":config.rows_per_block,
            "empty_every_nth_block":16,"history_batch_blocks":config.history_batch_blocks,
            "segment_rows":config.segment_rows,"repeats":config.repeats,
            "rows":expected.len(),"fixture_digest":keccak256(serde_json::to_vec(&expected).unwrap()),
            "tip_hash":tip.hash_slow(),"recent_header_window":RECENT_HEADER_WINDOW,
            "warm_headers":config.warm_headers,
            "cache":"fresh directories; OS cache not evicted"})
    );
    for iteration in 0..config.repeats {
        for historical in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let storage_config = PartitionManagerConfig {
                data_dir: dir.path().to_path_buf(),
                partition_target_rows: config.segment_rows,
                compaction_safety_margin_blocks: 2_048,
            };
            let mut storage = PartitionManager::open(storage_config.clone()).unwrap();
            if historical {
                // Establish the initial backward-sync anchor outside ingestion.
                storage.record_historical_floor(tip).unwrap();
            } else if config.warm_headers > 0 {
                let previous = &headers[config.warm_headers - 1];
                storage
                    .record_verified_canonical_state(
                        &anchor(previous),
                        previous,
                        &headers[config.warm_headers.saturating_sub(RECENT_HEADER_WINDOW)
                            ..config.warm_headers],
                    )
                    .unwrap();
                storage.record_historical_floor(previous).unwrap();
            }
            let start = Instant::now();
            if historical {
                for (first, rows) in &history {
                    storage
                        .ingest_historical_batch(rows, &measured_headers[*first])
                        .unwrap();
                }
                storage.finalize_historical_segment().unwrap();
            } else {
                for (index, (header, rows)) in measured_headers.iter().zip(&blocks).enumerate() {
                    storage
                        .ingest_canonical_batch(
                            rows,
                            header,
                            &headers[(config.warm_headers + index + 1)
                                .saturating_sub(RECENT_HEADER_WINDOW)
                                ..config.warm_headers + index + 1],
                            Some(&anchor(header)),
                        )
                        .unwrap();
                }
            }
            storage.checkpoint().unwrap();
            let elapsed = start.elapsed();
            println!(
                "{}",
                json!({"kind":"sample", "iteration":iteration,
                    "metric":if historical {"historical_storage_publication"} else {"live_storage_publication"},
                    "elapsed_ms":elapsed.as_secs_f64()*1000.0,
                    "blocks_per_second":measured_headers.len() as f64/elapsed.as_secs_f64(),
                    "rows_per_second":expected.len() as f64/elapsed.as_secs_f64()})
            );
            assert_rows(&storage, &expected);
            drop(storage);
            let reopened = PartitionManager::open(storage_config).unwrap();
            assert_rows(&reopened, &expected);
            if historical {
                assert_eq!(reopened.historical_floor_header(), measured_headers.first());
                assert_eq!(reopened.historical_anchor_header(), Some(tip));
                assert!(reopened.sync_head().is_none());
            } else {
                assert_eq!(reopened.sync_head().unwrap().block_number, tip.number);
                assert_eq!(reopened.sync_head().unwrap().block_hash, tip.hash_slow());
                assert_eq!(reopened.chain_anchors().indexed_head, Some(anchor(tip)));
                assert_eq!(
                    reopened.historical_floor_header(),
                    headers.get(config.warm_headers.saturating_sub(1))
                );
                assert_eq!(reopened.recent_headers().last(), Some(tip));
            }
        }
    }
}

#[test]
fn storage_publication_preserves_rows_and_empty_block_progress() {
    run(Config {
        blocks: 16,
        warm_headers: 4,
        rows_per_block: 3,
        history_batch_blocks: 6,
        segment_rows: 11,
        repeats: 1,
    });
}

#[test]
#[ignore = "release sync-storage publication baseline; see docs/audit/benchmarks.md"]
fn benchmark_sync_storage_publication() {
    run(Config {
        blocks: positive_env("LOGEX_PUBLICATION_BLOCKS", 128),
        warm_headers: positive_env("LOGEX_PUBLICATION_WARM_HEADERS", 8_192),
        rows_per_block: positive_env("LOGEX_PUBLICATION_ROWS_PER_BLOCK", 128),
        history_batch_blocks: positive_env("LOGEX_PUBLICATION_HISTORY_BLOCKS", 2_048),
        segment_rows: positive_env("LOGEX_PUBLICATION_SEGMENT_ROWS", 1_000_000) as u64,
        repeats: positive_env("LOGEX_PUBLICATION_REPEATS", 3),
    });
}
