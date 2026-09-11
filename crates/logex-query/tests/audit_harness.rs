//! Deterministic storage/query baselines. See docs/audit/benchmarks.md.
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use alloy_primitives::{Address, B256, Bytes, keccak256};
use logex_index::IndexBuilder;
use logex_query::{execute_log_filter, execute_sql};
use logex_storage::native::{NativeLogFilter, TopicConstraint};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::{Value, json};

const FIRST_BLOCK: u64 = 15_000_000;
const FIXTURE_VERSION: u32 = 1;
const HOT_ADDRESS: Address = Address::repeat_byte(0xaa);

#[derive(Clone, Copy, Debug)]
enum Profile {
    Sparse,
    Dense,
}

impl Profile {
    fn name(self) -> &'static str {
        match self {
            Self::Sparse => "sparse",
            Self::Dense => "dense",
        }
    }
}

struct Config {
    rows: usize,
    repeats: usize,
    segment_rows: usize,
    batch_rows: usize,
    workers: usize,
    profile: Profile,
}

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

impl Config {
    fn from_env() -> Self {
        let profile = match std::env::var("LOGEX_BENCH_PROFILE")
            .unwrap_or_else(|_| "dense".into())
            .as_str()
        {
            "sparse" => Profile::Sparse,
            "dense" => Profile::Dense,
            _ => panic!("LOGEX_BENCH_PROFILE must be sparse or dense"),
        };
        Self {
            rows: positive_env("LOGEX_BENCH_ROWS", 200_000),
            repeats: positive_env("LOGEX_BENCH_REPEATS", 5),
            segment_rows: positive_env("LOGEX_BENCH_SEGMENT_ROWS", 50_000),
            batch_rows: positive_env("LOGEX_BENCH_BATCH_ROWS", 8_192),
            workers: positive_env("LOGEX_BENCH_WORKERS", 4),
            profile,
        }
    }

    fn storage(&self, path: &Path) -> PartitionManagerConfig {
        PartitionManagerConfig {
            data_dir: path.to_path_buf(),
            partition_target_rows: self.segment_rows as u64,
            // A fixture has no finality/reorg protocol; allow compaction behind its head.
            compaction_safety_margin_blocks: 0,
        }
    }
}

fn transfer_topic() -> B256 {
    keccak256("Transfer(address,address,uint256)")
}

fn fixture(count: usize, profile: Profile) -> Vec<LogRow> {
    let topic = transfer_topic();
    (0..count)
        .map(|index| {
            let (block_offset, log_index) = match profile {
                Profile::Sparse => (index as u64 * 3, 0),
                Profile::Dense => ((index / 128) as u64, (index % 128) as u32),
            };
            let block_number = FIRST_BLOCK + block_offset;
            let tx_index = log_index / 2;
            let block_hash = keccak256(block_number.to_le_bytes());
            let mut tx_key = block_hash.to_vec();
            tx_key.extend_from_slice(&tx_index.to_le_bytes());
            let is_transfer = index % 5 != 0;
            let data_len = if is_transfer {
                32
            } else {
                [0, 64, 256, 1024][index % 4]
            };
            let payload = keccak256((index as u64).to_le_bytes());
            let data = Bytes::from(
                payload
                    .as_slice()
                    .iter()
                    .copied()
                    .cycle()
                    .take(data_len)
                    .collect::<Vec<_>>(),
            );
            LogRow {
                block_number,
                block_hash,
                timestamp: 1_700_000_000 + block_offset * 12,
                tx_hash: keccak256(tx_key),
                tx_index,
                log_index,
                address: if index % 7 != 0 {
                    HOT_ADDRESS
                } else {
                    Address::repeat_byte(0xbb)
                },
                topic0: if is_transfer { Some(topic) } else { None },
                topic1: is_transfer.then_some(B256::with_last_byte((index % 251) as u8)),
                topic2: is_transfer.then_some(B256::with_last_byte(((index + 17) % 251) as u8)),
                topic3: None,
                data_len: u32::try_from(data.len()).unwrap(),
                data,
                source: Source::Receipt,
            }
        })
        .collect()
}

// Independent oracle: deliberately does not call the native filter/pushdown helpers.
fn expected_matches(rows: &[LogRow]) -> Vec<LogRow> {
    rows.iter()
        .filter(|row| row.address == HOT_ADDRESS && row.topic0 == Some(transfer_topic()))
        .cloned()
        .collect()
}

fn filter() -> NativeLogFilter {
    let mut filter = NativeLogFilter::new();
    filter.addresses = vec![HOT_ADDRESS];
    filter.topics[0] = TopicConstraint::One(transfer_topic());
    filter
}

fn assert_native(storage: &PartitionManager, expected: &[LogRow]) {
    assert_eq!(execute_log_filter(storage, &filter()).unwrap(), expected);
}

fn file_bytes(path: &Path) -> u64 {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                file_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

#[derive(Default)]
struct Samples {
    values: BTreeMap<&'static str, Vec<Duration>>,
    enabled: bool,
}

impl Samples {
    fn record(
        &mut self,
        name: &'static str,
        iteration: usize,
        elapsed: Duration,
        work_rows: usize,
    ) {
        if !self.enabled {
            return;
        }
        self.values.entry(name).or_default().push(elapsed);
        println!(
            "{}",
            json!({
                "kind": "sample", "metric": name, "iteration": iteration,
                "elapsed_ms": elapsed.as_secs_f64() * 1000.0, "work_rows": work_rows,
                "rows_per_second": work_rows as f64 / elapsed.as_secs_f64(),
            })
        );
    }

    fn summarize(&mut self) {
        for (name, values) in &mut self.values {
            values.sort();
            let middle = values.len() / 2;
            let median = if values.len() % 2 == 0 {
                (values[middle - 1].as_secs_f64() + values[middle].as_secs_f64()) / 2.0
            } else {
                values[middle].as_secs_f64()
            };
            println!(
                "{}",
                json!({
                    "kind": "summary", "metric": name, "samples": values.len(),
                    "median_ms": median * 1000.0,
                    "p95_ms": values[(values.len() * 95).div_ceil(100) - 1].as_secs_f64() * 1000.0,
                })
            );
        }
    }
}

async fn query_cases(
    storage: &PartitionManager,
    expected: &[LogRow],
    iteration: usize,
    samples: &mut Samples,
) {
    let start = Instant::now();
    let actual = execute_log_filter(storage, &filter()).unwrap();
    let elapsed = start.elapsed();
    assert_eq!(actual, expected);
    samples.record("native_filter", iteration, elapsed, expected.len());

    let predicate = format!(
        "address = '{HOT_ADDRESS}' AND topic0 = '{}'",
        transfer_topic()
    );
    let sql = format!("SELECT COUNT(*) AS total FROM logs WHERE {predicate}");
    let start = Instant::now();
    let count = execute_sql(&sql, storage, storage.head_block())
        .await
        .unwrap();
    let elapsed = start.elapsed();
    assert_eq!(count.rows, vec![json!({"total": expected.len()})]);
    samples.record("sql_count", iteration, elapsed, expected.len());

    let sql = format!(
        "SELECT block_number, tx_index, log_index FROM logs WHERE {predicate} ORDER BY block_number DESC, tx_index DESC, log_index DESC LIMIT 1000"
    );
    let start = Instant::now();
    let ordered = execute_sql(&sql, storage, storage.head_block())
        .await
        .unwrap();
    let elapsed = start.elapsed();
    let expected_order: Vec<Value> = expected.iter().rev().take(1000).map(|row| json!({
        "block_number": row.block_number, "tx_index": row.tx_index, "log_index": row.log_index,
    })).collect();
    assert_eq!(ordered.rows, expected_order);
    samples.record("sql_ordered", iteration, elapsed, expected_order.len());
}

async fn run(config: Config) {
    let rows = fixture(config.rows, config.profile);
    let expected = expected_matches(&rows);
    println!(
        "{}",
        json!({
            "kind": "config", "fixture_version": FIXTURE_VERSION, "profile": config.profile.name(),
            "rows": config.rows, "repeats": config.repeats, "segment_rows": config.segment_rows,
            "batch_rows": config.batch_rows, "workers": config.workers,
            "fixture_digest": keccak256(serde_json::to_vec(&rows).unwrap()).to_string(),
            "cache": "fresh directories for writes; OS cache not evicted; queries warmed once",
        })
    );
    let mut samples = Samples {
        enabled: true,
        ..Default::default()
    };
    for iteration in 0..config.repeats {
        let tmp = tempfile::tempdir().unwrap();
        let mut storage = PartitionManager::open(config.storage(tmp.path())).unwrap();
        let start = Instant::now();
        for batch in rows.chunks(config.batch_rows) {
            storage.write_batch(batch).unwrap();
        }
        // Include the final checkpoint: deferring column persistence must not
        // make ingestion appear faster by charging it to indexing or reopen.
        storage.checkpoint().unwrap();
        samples.record(
            "live_storage_ingest",
            iteration,
            start.elapsed(),
            rows.len(),
        );
        assert_eq!(storage.total_rows(), rows.len() as u64);
        assert_native(&storage, &expected);
        let raw_bytes = file_bytes(tmp.path());

        let targets: Vec<_> = storage
            .sealed_partitions()
            .iter()
            .chain(std::iter::once(storage.hot_partition()))
            .filter(|partition| partition.meta.row_count > 0)
            .map(|partition| (partition.meta.id, partition.meta.path.clone()))
            .collect();
        let start = Instant::now();
        for (id, path) in &targets {
            IndexBuilder::build_all_indexes(path).unwrap();
            storage.refresh_segment_manifest(*id).unwrap();
        }
        samples.record("index_build", iteration, start.elapsed(), rows.len());
        assert_native(&storage, &expected);
        let indexed_bytes = file_bytes(tmp.path());

        let start = Instant::now();
        let compacted = storage.compact_eligible_segments().unwrap();
        samples.record("compaction", iteration, start.elapsed(), rows.len());
        assert_native(&storage, &expected);
        println!(
            "{}",
            json!({"kind": "storage", "iteration": iteration,
                "raw_file_bytes": raw_bytes, "indexed_file_bytes": indexed_bytes,
                "compacted_file_bytes": file_bytes(tmp.path()), "compacted_segments": compacted,
            })
        );
        drop(storage);
        let start = Instant::now();
        let storage = PartitionManager::open(config.storage(tmp.path())).unwrap();
        samples.record("reopen", iteration, start.elapsed(), rows.len());
        assert_eq!(storage.total_rows(), rows.len() as u64);

        // Warm every measured query path once; do not add warmups to the samples.
        query_cases(&storage, &expected, iteration, &mut Samples::default()).await;
        query_cases(&storage, &expected, iteration, &mut samples).await;

        // Scoped native queries run on actual OS threads, not serial futures.
        // The barrier excludes thread creation from the measured work interval.
        let barrier = Arc::new(Barrier::new(config.workers + 1));
        let elapsed = std::thread::scope(|scope| {
            let mut handles = Vec::new();
            for _ in 0..config.workers {
                let barrier = Arc::clone(&barrier);
                let storage = &storage;
                handles.push(scope.spawn(move || {
                    barrier.wait();
                    execute_log_filter(storage, &filter()).unwrap()
                }));
            }
            let start = Instant::now();
            barrier.wait();
            let results: Vec<_> = handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect();
            let elapsed = start.elapsed();
            for result in results {
                assert_eq!(result, expected);
            }
            elapsed
        });
        samples.record(
            "concurrent_native_queries",
            iteration,
            elapsed,
            expected.len() * config.workers,
        );
        drop(storage);

        let history = tempfile::tempdir().unwrap();
        let mut storage = PartitionManager::open(config.storage(history.path())).unwrap();
        let start = Instant::now();
        for batch in rows.rchunks(config.batch_rows) {
            storage.write_historical_batch(batch).unwrap();
        }
        storage.finalize_historical_segment().unwrap();
        samples.record(
            "historical_storage_ingest",
            iteration,
            start.elapsed(),
            rows.len(),
        );
        assert_eq!(storage.total_rows(), rows.len() as u64);
        assert_native(&storage, &expected);
        drop(storage);
        let storage = PartitionManager::open(config.storage(history.path())).unwrap();
        assert_eq!(storage.total_rows(), rows.len() as u64);
        assert_native(&storage, &expected);
    }
    samples.summarize();
}

#[tokio::test]
#[ignore = "release baseline; see docs/audit/benchmarks.md"]
async fn benchmark_storage_indexes_and_queries() {
    run(Config::from_env()).await;
}

#[tokio::test]
async fn audit_fixture_round_trips_through_live_and_historical_storage() {
    // CI exercises the same oracle and paths with rotation, split blocks, and a tail.
    for profile in [Profile::Sparse, Profile::Dense] {
        run(Config {
            rows: 259,
            repeats: 1,
            segment_rows: 100,
            batch_rows: 67,
            workers: 2,
            profile,
        })
        .await;
    }
}

#[tokio::test]
async fn ordered_sql_crosses_compacted_page_boundaries() {
    let rows = fixture(17_000, Profile::Dense);
    let expected = expected_matches(&rows);
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_path_buf(),
        partition_target_rows: 50_000,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_historical_batch(&rows).unwrap();
    storage.finalize_historical_segment().unwrap();
    // LIMIT 1000 spans the tail page and the previous 16,384-row page.
    query_cases(&storage, &expected, 0, &mut Samples::default()).await;
}

#[tokio::test]
async fn historical_queries_preserve_canonical_flags_after_reorg_and_restart() {
    let rows = fixture(17_000, Profile::Dense);
    let removed_hash = rows[0].block_hash;
    let remaining: Vec<_> = rows
        .iter()
        .filter(|row| row.block_hash != removed_hash)
        .cloned()
        .collect();
    let expected = expected_matches(&remaining);
    let tmp = tempfile::tempdir().unwrap();
    let config = PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: 50_000,
        compaction_safety_margin_blocks: 0,
    };
    let mut storage = PartitionManager::open(config.clone()).unwrap();
    storage.write_historical_batch(&rows).unwrap();
    storage.finalize_historical_segment().unwrap();
    assert_eq!(
        storage.mark_non_canonical(removed_hash).unwrap(),
        (rows.len() - remaining.len()) as u64
    );
    query_cases(&storage, &expected, 0, &mut Samples::default()).await;
    drop(storage);
    let storage = PartitionManager::open(config).unwrap();
    query_cases(&storage, &expected, 0, &mut Samples::default()).await;
}
