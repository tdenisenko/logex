//! Storage calls made by live/historical sync, including progress publication.
//! Synthetic headers are already-validated inputs here, not consensus fixtures.
use std::path::Path;
use std::time::Instant;

use alloy_consensus::Header;
use alloy_primitives::{Address, B256, Bloom, Bytes, keccak256};
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
    route: Option<bool>,
    checkpoint_each_block: bool,
    durable_checkpoint: bool,
    rich_headers: bool,
    mixed_payloads: bool,
}

#[derive(Default, serde::Serialize)]
struct Footprint {
    logical_bytes: u64,
    allocated_bytes: Option<u64>,
    files: u64,
}

fn footprint(path: &Path) -> std::io::Result<Footprint> {
    let mut result = Footprint {
        allocated_bytes: cfg!(unix).then_some(0),
        ..Default::default()
    };
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            let child = footprint(&entry.path())?;
            result.logical_bytes += child.logical_bytes;
            result.files += child.files;
            result.allocated_bytes = result
                .allocated_bytes
                .zip(child.allocated_bytes)
                .map(|(left, right)| left + right);
        } else {
            result.logical_bytes += metadata.len();
            result.files += 1;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                *result.allocated_bytes.as_mut().unwrap() += metadata.blocks() * 512;
            }
        }
    }
    Ok(result)
}

// OS-attributed process disk writes, not NAND/device-wide write amplification.
// Read these counters outside timing; delayed writeback may be charged later.
#[cfg(target_os = "macos")]
fn process_written_bytes() -> std::io::Result<Option<u64>> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage_info_v2>::uninit();
    // SAFETY: RUSAGE_INFO_V2 writes exactly the matching C-layout structure.
    // Its correctly aligned buffer remains valid for this synchronous call;
    // proc_pid_rusage's pointer-to-pointer signature denotes the output buffer,
    // not a pointer value to dereference. Only a successful call initializes it.
    // See Apple's libproc.h and bsd/sys/resource.h, linked in benchmarks.md.
    let result = unsafe {
        libc::proc_pid_rusage(
            libc::getpid(),
            libc::RUSAGE_INFO_V2,
            usage.as_mut_ptr().cast(),
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: the successful call above initialized the complete v2 structure.
    Ok(Some(unsafe { usage.assume_init() }.ri_diskio_byteswritten))
}

#[cfg(target_os = "linux")]
fn process_written_bytes() -> std::io::Result<Option<u64>> {
    let counters = std::fs::read_to_string("/proc/self/io")?;
    let value = counters
        .lines()
        .find_map(|line| line.strip_prefix("write_bytes:"))
        .ok_or_else(|| std::io::Error::other("missing process write_bytes counter"))?;
    value
        .trim()
        .parse()
        .map(Some)
        .map_err(std::io::Error::other)
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn process_written_bytes() -> std::io::Result<Option<u64>> {
    Ok(None)
}

impl Config {
    fn checkpoint(&self, storage: &mut PartitionManager) {
        if self.durable_checkpoint {
            storage.checkpoint_durable().unwrap();
        } else {
            storage.checkpoint().unwrap();
        }
    }
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

fn fill_header_fields(header: &mut Header, index: usize) {
    let hash = |tag: u8| {
        let mut seed = (index as u64).to_le_bytes().to_vec();
        seed.push(tag);
        keccak256(seed)
    };
    header.beneficiary = Address::from_slice(&hash(0).as_slice()[12..]);
    header.state_root = hash(1);
    header.transactions_root = hash(2);
    header.receipts_root = hash(3);
    header.mix_hash = hash(4);
    header.extra_data = Bytes::copy_from_slice(hash(5).as_slice());
    let mut bloom = [0u8; 256];
    for (part, chunk) in bloom.as_chunks_mut::<32>().0.iter_mut().enumerate() {
        chunk.copy_from_slice(hash(10 + part as u8).as_slice());
    }
    header.logs_bloom = Bloom::from(bloom);
    header.gas_used = 15_000_000 + index as u64;
    header.base_fee_per_gas = Some(1_000_000_000 + index as u64);
    header.withdrawals_root = Some(hash(20));
    header.blob_gas_used = Some(393_216);
    header.excess_blob_gas = Some(index as u64 * 131_072);
    header.parent_beacon_block_root = Some(hash(21));
    header.requests_hash = Some(hash(22));
}

fn fixture(config: &Config) -> (Vec<Header>, Vec<Vec<LogRow>>) {
    let mut headers = Vec::with_capacity(config.warm_headers + config.blocks);
    let mut blocks = Vec::with_capacity(config.blocks);
    let mut parent_hash = B256::ZERO;
    for index in 0..config.warm_headers + config.blocks {
        let mut header = Header {
            number: 15_000_000 + index as u64,
            parent_hash,
            timestamp: 1_700_000_000 + index as u64 * 12,
            gas_limit: 30_000_000,
            ..Default::default()
        };
        if config.rich_headers {
            fill_header_fields(&mut header, index);
        }
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
                let data = if config.mixed_payloads {
                    let length = [0, 32, 32, 32, 32, 64, 128, 256, 1024, 32][(index + log) % 10];
                    let mut payload = Vec::with_capacity(length);
                    for word in 0..length / 32 {
                        let mut seed = key.clone();
                        seed.extend_from_slice(&(log as u64).to_le_bytes());
                        seed.extend_from_slice(&(word as u64).to_le_bytes());
                        payload.extend_from_slice(keccak256(seed).as_slice());
                    }
                    Bytes::from(payload)
                } else {
                    Bytes::copy_from_slice(keccak256(&key).as_slice())
                };
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

fn assert_rows(storage: &PartitionManager, expected: &[LogRow], selected: bool) {
    let mut actual: Vec<_> = storage
        .sealed_partitions()
        .iter()
        .chain(std::iter::once(storage.hot_partition()))
        .filter(|partition| partition.meta.row_count > 0)
        .flat_map(|partition| {
            let reader = SegmentReader::open(&partition.meta.path).unwrap();
            let ids = selected.then(|| {
                (0..u32::try_from(partition.meta.row_count).expect("fixture row count fits u32"))
                    .collect::<Vec<_>>()
            });
            reader.read_log_rows(ids.as_deref()).unwrap()
        })
        .collect();
    actual.sort_by_key(|row| (row.block_number, row.log_index));
    assert_eq!(actual, expected);
    assert_eq!(storage.total_rows(), expected.len() as u64);
}

fn run(config: Config) {
    let selected_reads = match std::env::var("LOGEX_PUBLICATION_READ_MODE").as_deref() {
        Err(std::env::VarError::NotPresent) | Ok("full") => false,
        Ok("selected") => true,
        value => panic!("invalid LOGEX_PUBLICATION_READ_MODE: {value:?}"),
    };
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
        json!({"kind":"config", "fixture_version":if config.mixed_payloads {4} else {3}, "workload":"sync_storage_publication",
            "blocks":config.blocks,"rows_per_nonempty_block":config.rows_per_block,
            "empty_every_nth_block":16,"history_batch_blocks":config.history_batch_blocks,
            "segment_rows":config.segment_rows,"repeats":config.repeats,
            "rows":expected.len(),"fixture_digest":keccak256(serde_json::to_vec(&expected).unwrap()),
            "tip_hash":tip.hash_slow(),"recent_header_window":RECENT_HEADER_WINDOW,
            "warm_headers":config.warm_headers,
            "route":config.route.map(|historical| if historical {"historical"} else {"live"}),
            "checkpoint_each_block":config.checkpoint_each_block,
            "durable_checkpoint":config.durable_checkpoint,
            "header_fields":if config.rich_headers {"rich"} else {"minimal"},
            "payload":if config.mixed_payloads {"mixed"} else {"transfer"},
            "read_mode":if selected_reads {"selected"} else {"full"},
            "cache":"fresh directories; OS cache not evicted"})
    );
    for iteration in 0..config.repeats {
        for historical in [false, true] {
            if config.route.is_some_and(|route| route != historical) {
                continue;
            }
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
            let writes_before = process_written_bytes().unwrap();
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
                    // Measure a publication boundary after each live block without
                    // sleeps. Report the revision's checkpoint durability contract;
                    // this is not a paced-network or power-failure test.
                    if config.checkpoint_each_block {
                        config.checkpoint(&mut storage);
                    }
                }
            }
            config.checkpoint(&mut storage);
            let elapsed = start.elapsed();
            let writes_after = process_written_bytes().unwrap();
            println!(
                "{}",
                json!({"kind":"sample", "iteration":iteration,
                    "metric":if historical {"historical_storage_publication"} else {"live_storage_publication"},
                    "elapsed_ms":elapsed.as_secs_f64()*1000.0,
                    "blocks_per_second":measured_headers.len() as f64/elapsed.as_secs_f64(),
                    "rows_per_second":expected.len() as f64/elapsed.as_secs_f64()})
            );
            let start = Instant::now();
            assert_rows(&storage, &expected, selected_reads);
            let validation_ms = start.elapsed().as_secs_f64() * 1000.0;
            let files = footprint(dir.path()).unwrap();
            let segments = storage.sealed_partitions().len()
                + usize::from(storage.hot_partition().meta.row_count > 0);
            drop(storage);
            let start = Instant::now();
            let reopened = PartitionManager::open(storage_config).unwrap();
            let reopen_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert_rows(&reopened, &expected, selected_reads);
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
            println!(
                "{}",
                json!({"kind":"lifecycle", "iteration":iteration,
                    "route":if historical {"historical"} else {"live"},
                    "files":files, "segments":segments,
                    "full_row_validation_ms":validation_ms, "warm_reopen_ms":reopen_ms,
                    "process_written_bytes":writes_before.zip(writes_after)
                        .map(|(before, after)| after.checked_sub(before).unwrap()),
                })
            );
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
        route: None,
        checkpoint_each_block: false,
        durable_checkpoint: false,
        rich_headers: false,
        mixed_payloads: true,
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
        route: match std::env::var("LOGEX_PUBLICATION_ROUTE").as_deref() {
            Ok("live") => Some(false),
            Ok("historical") => Some(true),
            Ok("both") | Err(std::env::VarError::NotPresent) => None,
            value => panic!("invalid LOGEX_PUBLICATION_ROUTE: {value:?}"),
        },
        rich_headers: match std::env::var("LOGEX_PUBLICATION_HEADER_FIELDS").as_deref() {
            Ok("rich") => true,
            Ok("minimal") | Err(std::env::VarError::NotPresent) => false,
            value => panic!("invalid LOGEX_PUBLICATION_HEADER_FIELDS: {value:?}"),
        },
        mixed_payloads: match std::env::var("LOGEX_PUBLICATION_PAYLOAD").as_deref() {
            Ok("mixed") => true,
            Ok("transfer") | Err(std::env::VarError::NotPresent) => false,
            value => panic!("invalid LOGEX_PUBLICATION_PAYLOAD: {value:?}"),
        },
        durable_checkpoint: match std::env::var("LOGEX_PUBLICATION_DURABLE_CHECKPOINT").as_deref() {
            Ok("1") => true,
            Ok("0") | Err(_) => false,
            Ok(_) => panic!("LOGEX_PUBLICATION_DURABLE_CHECKPOINT must be 0 or 1"),
        },
        checkpoint_each_block: match std::env::var("LOGEX_PUBLICATION_CHECKPOINT_EACH_BLOCK")
            .as_deref()
        {
            Ok("1") => true,
            Ok("0") | Err(std::env::VarError::NotPresent) => false,
            value => panic!("invalid LOGEX_PUBLICATION_CHECKPOINT_EACH_BLOCK: {value:?}"),
        },
    });
}

#[test]
#[ignore = "release cached-header encoding comparison; see docs/audit/benchmarks.md"]
fn benchmark_cached_header_encoding() {
    use alloy_rlp::Decodable;

    for rich in [false, true] {
        let (headers, _) = fixture(&Config {
            blocks: RECENT_HEADER_WINDOW,
            warm_headers: 0,
            rows_per_block: 0,
            history_batch_blocks: 2048,
            segment_rows: 1_000_000,
            repeats: 1,
            route: None,
            checkpoint_each_block: false,
            durable_checkpoint: false,
            rich_headers: rich,
            mixed_payloads: false,
        });
        for iteration in 0..9 {
            let start = Instant::now();
            let json_bytes = serde_json::to_vec(&headers).unwrap();
            let json_ms = start.elapsed().as_secs_f64() * 1000.0;
            let start = Instant::now();
            let rlp_bytes = alloy_rlp::encode(&headers);
            let rlp_ms = start.elapsed().as_secs_f64() * 1000.0;
            let start = Instant::now();
            let compressed = lz4_flex::compress_prepend_size(&json_bytes);
            let lz4_ms = start.elapsed().as_secs_f64() * 1000.0;
            let mut input = rlp_bytes.as_slice();
            assert_eq!(Vec::<Header>::decode(&mut input).unwrap(), headers);
            assert!(input.is_empty());
            println!(
                "{}",
                json!({"kind":"header_encoding", "rich_fields":rich,
                "iteration":iteration,"headers":headers.len(),
                "json_ms":json_ms,"json_bytes":json_bytes.len(),
                "rlp_ms":rlp_ms,"rlp_bytes":rlp_bytes.len(),
                "lz4_additional_ms":lz4_ms,"lz4_bytes":compressed.len()})
            );
        }
    }
}
