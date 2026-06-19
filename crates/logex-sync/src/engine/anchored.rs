use super::*;
use crate::EXECUTION_HISTORY_TARGET_BLOCK;
use crate::extract;
use crate::p2p::peer_manager::SourcedBodyReceipts;
use crate::primitives::LogexNetworkPrimitives;
use crate::validation::validate_header_matches_anchor;
use alloy_consensus::{EMPTY_OMMER_ROOT_HASH, EMPTY_ROOT_HASH, ReceiptWithBloom};
use alloy_eips::BlockHashOrNumber;
use logex_types::{ExecutionAnchor, NodeState};
use reth_eth_wire::NetworkPrimitives;
use std::sync::OnceLock;
use tokio::task::JoinSet;

const CONSENSUS_WAIT_INTERVAL: Duration = Duration::from_secs(2);
const CONSENSUS_ANCHOR_FORWARD_BATCH_LIMIT: u64 = 32;
const HISTORICAL_VALIDATION_TASKS_PER_CPU: usize = 16;
const HISTORICAL_VALIDATION_TASK_LIMIT: usize = 256;
const HISTORICAL_VALIDATION_LOG_WORK_WEIGHT: u64 = 4;
const HISTORICAL_VALIDATION_TX_WORK_WEIGHT: u64 = 8;
const HISTORICAL_LOW_PEER_FETCH_PIPELINE_DEPTH: usize = 2;
const HISTORICAL_MEDIUM_PEER_FETCH_PIPELINE_DEPTH: usize = 4;
const HISTORICAL_HIGH_MEMORY_MEDIUM_PEER_FETCH_PIPELINE_DEPTH: usize = 6;
const HISTORICAL_DEEP_FETCH_PIPELINE_DEPTH: usize = 6;
const HISTORICAL_WIDE_FETCH_PIPELINE_DEPTH: usize = 8;
const HISTORICAL_LOW_PEER_FETCH_WINDOW_BLOCKS: u64 = 1_024;
const HISTORICAL_MEDIUM_PEER_FETCH_WINDOW_BLOCKS: u64 = 2_048;
const HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS: u64 = 5_000;
const HISTORICAL_DEEP_FETCH_WINDOW_BLOCKS: u64 = 4_096;
const HISTORICAL_WIDE_FETCH_WINDOW_BLOCKS: u64 = 5_000;
const HISTORICAL_DENSE_FETCH_WINDOW_BLOCKS: u64 = 512;
const HISTORICAL_VERY_DENSE_FETCH_WINDOW_BLOCKS: u64 = 512;
const HISTORICAL_MEDIUM_DENSITY_TARGET_FETCH_ROWS: f64 = 750_000.0;
const HISTORICAL_MEDIUM_DENSITY_MAX_FETCH_WINDOW_BLOCKS: u64 = 10_000;
const HISTORICAL_DENSE_FETCH_PIPELINE_DEPTH: usize = 6;
const HISTORICAL_VERY_DENSE_FETCH_PIPELINE_DEPTH: usize = 6;
const HISTORICAL_SPARSE_FETCH_PIPELINE_DEPTH: usize = 5;
const HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT: usize = 12;
const HISTORICAL_DENSE_FETCH_BUFFER_EXTRA: usize = 4;
const HISTORICAL_PREPARE_LOOKAHEAD_DEPTH: usize = 2;
const HISTORICAL_SEQUENTIAL_FETCH_BATCH_LIMIT: usize = 1024;
const HISTORICAL_USE_COMBINED_BODY_RECEIPT_PIPELINE: bool = true;
const HISTORICAL_MEDIUM_LOOKAHEAD_MIN_SERVING_PEERS: usize = 8;
const HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS: usize = 8;
const HISTORICAL_DEEP_LOOKAHEAD_MIN_SERVING_PEERS: usize = 48;
const HISTORICAL_WIDE_LOOKAHEAD_MIN_SERVING_PEERS: usize = 80;
const HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS: usize = 48;
const HISTORICAL_SPARSE_ROWS_PER_BLOCK: f64 = 100.0;
const HISTORICAL_DENSE_ROWS_PER_BLOCK: f64 = 300.0;
const HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK: f64 = 1_500.0;
const HISTORICAL_DENSITY_EWMA_WEIGHT: f64 = 0.5;
const HISTORICAL_HEADER_GAS_WINDOW_MIN_BLOCKS: usize = 1_024;
const HISTORICAL_HEADER_GAS_PER_BLOCK_TARGET: u128 = 30_000_000;
#[cfg(target_os = "linux")]
const BYTES_PER_KIB: u64 = 1024;
const BYTES_PER_GIB: u64 = 1024 * 1024 * 1024;
const HISTORICAL_CRITICAL_AVAILABLE_MEMORY_BYTES: u64 = 2 * BYTES_PER_GIB;
const HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES: u64 = 4 * BYTES_PER_GIB;
const HISTORICAL_MEDIUM_PIPELINE_MIN_TOTAL_MEMORY_BYTES: u64 = 12 * BYTES_PER_GIB;
const HISTORICAL_HIGH_PIPELINE_MIN_TOTAL_MEMORY_BYTES: u64 = 14 * BYTES_PER_GIB;
const HISTORICAL_SPARSE_PIPELINE_MIN_TOTAL_MEMORY_BYTES: u64 =
    HISTORICAL_HIGH_PIPELINE_MIN_TOTAL_MEMORY_BYTES;
const HISTORICAL_DEEP_WINDOW_MIN_TOTAL_MEMORY_BYTES: u64 = 24 * BYTES_PER_GIB;
const HISTORICAL_WIDE_WINDOW_MIN_TOTAL_MEMORY_BYTES: u64 = 48 * BYTES_PER_GIB;
const HISTORICAL_ALLOCATOR_TRIM_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct ConsensusReorg {
    retained_headers: Vec<Header>,
    indexed_head: Option<ExecutionAnchor>,
    reverted_hashes: Vec<B256>,
}

struct HistoricalValidationJob {
    index: usize,
    header: Header,
    block_hash: B256,
    body_peer: PeerId,
    body: <LogexNetworkPrimitives as NetworkPrimitives>::BlockBody,
    receipt_peer: PeerId,
    receipts: Vec<ReceiptWithBloom<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>>,
}

struct HistoricalValidationExtractedChunk {
    start_index: usize,
    peer_notes: Vec<PeerId>,
    extracted: super::ingest::HistoricalExtractedChunk,
    lowest_block: u64,
    highest_block: u64,
    validation_elapsed: Duration,
}

async fn validate_historical_blocks_parallel(
    headers: &[Header],
    hashes: &[B256],
    blocks: Vec<SourcedBodyReceipts>,
) -> Result<std::result::Result<Vec<HistoricalValidatedBlock>, Box<HistoricalValidationFailure>>> {
    let jobs = build_historical_validation_jobs(headers, hashes, blocks);
    let block_count = jobs.len();
    let task_count = historical_validation_task_count(block_count);
    let chunk_ranges = historical_validation_work_ranges(
        jobs.iter().map(historical_validation_job_work),
        task_count,
    );
    let mut tasks = JoinSet::new();
    let mut jobs = jobs.into_iter();

    for range in chunk_ranges {
        let mut chunk = Vec::with_capacity(range.len());
        for _ in range {
            if let Some(job) = jobs.next() {
                chunk.push(job);
            }
        }
        if !chunk.is_empty() {
            tasks.spawn_blocking(move || validate_historical_block_chunk(chunk));
        }
    }

    let mut validated = Vec::with_capacity(block_count);
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(mut blocks)) => validated.append(&mut blocks),
            Ok(Err(failure)) => return Ok(Err(failure)),
            Err(error) => return Err(eyre::eyre!("historical validation worker failed: {error}")),
        }
    }
    validated.sort_by_key(|block| block.index);
    Ok(Ok(validated))
}

async fn validate_and_extract_historical_blocks_streaming(
    headers: &[Header],
    hashes: &[B256],
    blocks: Vec<SourcedBodyReceipts>,
) -> Result<
    std::result::Result<
        (
            super::ingest::HistoricalExtractedBatch,
            Vec<PeerId>,
            u64,
            u64,
            usize,
            Duration,
        ),
        Box<HistoricalValidationFailure>,
    >,
> {
    let jobs = build_historical_validation_jobs(headers, hashes, blocks);
    let block_count = jobs.len();
    let task_count = historical_validation_task_count(block_count);
    let chunk_ranges = historical_validation_work_ranges(
        jobs.iter().map(historical_validation_job_work),
        task_count,
    );
    let mut tasks = JoinSet::new();
    let mut jobs = jobs.into_iter();

    for range in chunk_ranges {
        let mut chunk = Vec::with_capacity(range.len());
        for _ in range {
            if let Some(job) = jobs.next() {
                chunk.push(job);
            }
        }
        if !chunk.is_empty() {
            tasks.spawn_blocking(move || validate_and_extract_historical_block_chunk(chunk));
        }
    }

    let mut pending_chunks = BTreeMap::new();
    let mut next_chunk_index = 0usize;
    let mut extracted_chunks = Vec::new();
    let mut peer_notes = Vec::new();
    let mut validation_elapsed = Duration::ZERO;
    let mut lowest_block = u64::MAX;
    let mut highest_block = 0u64;
    let mut block_count = 0usize;

    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(chunk)) => {
                pending_chunks.insert(chunk.start_index, chunk);
                while let Some(chunk) = pending_chunks.remove(&next_chunk_index) {
                    next_chunk_index = next_chunk_index.saturating_add(chunk.extracted.block_count);
                    validation_elapsed = validation_elapsed.max(chunk.validation_elapsed);
                    peer_notes.extend(chunk.peer_notes);
                    lowest_block = lowest_block.min(chunk.lowest_block);
                    highest_block = highest_block.max(chunk.highest_block);
                    block_count = block_count.saturating_add(chunk.extracted.block_count);
                    extracted_chunks.push(chunk.extracted);
                }
            }
            Ok(Err(failure)) => {
                tasks.abort_all();
                return Ok(Err(failure));
            }
            Err(error) => {
                tasks.abort_all();
                return Err(eyre::eyre!(
                    "historical validation/extraction worker failed: {error}"
                ));
            }
        }
    }

    Ok(Ok((
        super::ingest::HistoricalExtractedBatch {
            chunks: extracted_chunks,
        },
        peer_notes,
        lowest_block,
        highest_block,
        block_count,
        validation_elapsed,
    )))
}

fn build_historical_validation_jobs(
    headers: &[Header],
    hashes: &[B256],
    blocks: Vec<SourcedBodyReceipts>,
) -> Vec<HistoricalValidationJob> {
    blocks
        .into_iter()
        .enumerate()
        .filter_map(|(index, ((body_peer, body), (receipt_peer, receipts)))| {
            let header = headers.get(index)?.clone();
            let block_hash = hashes.get(index).copied().unwrap_or_default();
            Some(HistoricalValidationJob {
                index,
                header,
                block_hash,
                body_peer,
                body,
                receipt_peer,
                receipts,
            })
        })
        .collect()
}

fn historical_validation_job_work(job: &HistoricalValidationJob) -> u64 {
    let tx_work = job.body.transaction_count() as u64 * HISTORICAL_VALIDATION_TX_WORK_WEIGHT;
    let log_work = job
        .receipts
        .iter()
        .map(|receipt| receipt.logs().len() as u64)
        .sum::<u64>()
        * HISTORICAL_VALIDATION_LOG_WORK_WEIGHT;
    1 + tx_work + log_work
}

fn historical_validation_work_ranges(
    work: impl IntoIterator<Item = u64>,
    task_count: usize,
) -> Vec<std::ops::Range<usize>> {
    let work = work.into_iter().collect::<Vec<_>>();
    let block_count = work.len();
    if block_count == 0 {
        return Vec::new();
    }

    let task_count = task_count.clamp(1, block_count);
    if task_count == 1 {
        return std::iter::once(0..block_count).collect();
    }

    let total_work = work
        .iter()
        .copied()
        .fold(0u64, |total, item| total.saturating_add(item.max(1)));
    let target_work = total_work.div_ceil(task_count as u64).max(1);
    let mut ranges = Vec::with_capacity(task_count);
    let mut start = 0usize;
    let mut chunk_work = 0u64;

    for (index, item_work) in work.iter().copied().enumerate() {
        let item_work = item_work.max(1);
        let remaining_jobs = block_count - index;
        let remaining_chunks = task_count.saturating_sub(ranges.len() + 1);
        if index > start
            && chunk_work.saturating_add(item_work) > target_work
            && remaining_chunks > 0
            && remaining_jobs > remaining_chunks
        {
            ranges.push(start..index);
            start = index;
            chunk_work = 0;
        }

        chunk_work = chunk_work.saturating_add(item_work);

        let remaining_jobs_after = block_count - index - 1;
        let remaining_chunks_after = task_count.saturating_sub(ranges.len() + 1);
        if chunk_work >= target_work
            && remaining_chunks_after > 0
            && remaining_jobs_after >= remaining_chunks_after
        {
            ranges.push(start..index + 1);
            start = index + 1;
            chunk_work = 0;
        }
    }

    if start < block_count {
        ranges.push(start..block_count);
    }
    ranges
}

fn historical_validation_task_count(block_count: usize) -> usize {
    if block_count == 0 {
        return 1;
    }

    let cpu_count = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1);
    cpu_count
        .saturating_mul(HISTORICAL_VALIDATION_TASKS_PER_CPU)
        .clamp(1, HISTORICAL_VALIDATION_TASK_LIMIT)
        .min(block_count)
}

fn historical_header_window_reached_gas_target(
    header_count: usize,
    cumulative_gas: u128,
    target_count: u64,
) -> bool {
    let gas_target = HISTORICAL_HEADER_GAS_PER_BLOCK_TARGET.saturating_mul(target_count as u128);
    header_count >= HISTORICAL_HEADER_GAS_WINDOW_MIN_BLOCKS && cumulative_gas >= gas_target
}

fn historical_fetch_pipeline_depth_for_serving_peers(
    serving_peers: usize,
    total_memory_bytes: Option<u64>,
    available_memory_bytes: Option<u64>,
) -> usize {
    let depth = if serving_peers >= HISTORICAL_WIDE_LOOKAHEAD_MIN_SERVING_PEERS
        && historical_allows_wide_windows(total_memory_bytes)
    {
        HISTORICAL_WIDE_FETCH_PIPELINE_DEPTH
    } else if serving_peers >= HISTORICAL_DEEP_LOOKAHEAD_MIN_SERVING_PEERS
        && historical_allows_deep_windows(total_memory_bytes)
    {
        HISTORICAL_DEEP_FETCH_PIPELINE_DEPTH
    } else if serving_peers >= HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS
        && historical_allows_high_memory_pipeline(total_memory_bytes)
    {
        HISTORICAL_HIGH_MEMORY_MEDIUM_PEER_FETCH_PIPELINE_DEPTH
    } else if serving_peers >= HISTORICAL_MEDIUM_LOOKAHEAD_MIN_SERVING_PEERS
        && historical_allows_medium_memory_pipeline(total_memory_bytes)
    {
        HISTORICAL_MEDIUM_PEER_FETCH_PIPELINE_DEPTH
    } else {
        HISTORICAL_LOW_PEER_FETCH_PIPELINE_DEPTH
    };

    if historical_available_memory_is_critical(available_memory_bytes) {
        1
    } else if historical_available_memory_is_low(available_memory_bytes) {
        depth.min(HISTORICAL_LOW_PEER_FETCH_PIPELINE_DEPTH)
    } else {
        depth
    }
}

fn historical_fetch_window_blocks_for_serving_peers(
    serving_peers: usize,
    total_memory_bytes: Option<u64>,
    available_memory_bytes: Option<u64>,
) -> u64 {
    if historical_available_memory_is_low(available_memory_bytes) {
        return HISTORICAL_LOW_PEER_FETCH_WINDOW_BLOCKS;
    }

    if serving_peers >= HISTORICAL_WIDE_LOOKAHEAD_MIN_SERVING_PEERS
        && historical_allows_wide_windows(total_memory_bytes)
    {
        HISTORICAL_WIDE_FETCH_WINDOW_BLOCKS
    } else if serving_peers >= HISTORICAL_DEEP_LOOKAHEAD_MIN_SERVING_PEERS
        && historical_allows_deep_windows(total_memory_bytes)
    {
        HISTORICAL_DEEP_FETCH_WINDOW_BLOCKS
    } else if serving_peers >= HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS
        && historical_allows_high_memory_pipeline(total_memory_bytes)
    {
        HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS
    } else if serving_peers >= HISTORICAL_MEDIUM_LOOKAHEAD_MIN_SERVING_PEERS {
        HISTORICAL_MEDIUM_PEER_FETCH_WINDOW_BLOCKS
    } else {
        HISTORICAL_LOW_PEER_FETCH_WINDOW_BLOCKS
    }
}

fn historical_density_fetch_pipeline_depth_cap(rows_per_block: Option<f64>) -> Option<usize> {
    let rows_per_block = rows_per_block?;
    if rows_per_block >= HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK {
        Some(HISTORICAL_VERY_DENSE_FETCH_PIPELINE_DEPTH)
    } else if rows_per_block >= HISTORICAL_DENSE_ROWS_PER_BLOCK {
        Some(HISTORICAL_DENSE_FETCH_PIPELINE_DEPTH)
    } else {
        None
    }
}

fn historical_sparse_fetch_pipeline_depth_boost(
    serving_peers: usize,
    total_memory_bytes: Option<u64>,
    available_memory_bytes: Option<u64>,
    rows_per_block: Option<f64>,
) -> Option<usize> {
    if serving_peers < HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS
        || !historical_allows_sparse_pipeline(total_memory_bytes)
        || historical_available_memory_is_low(available_memory_bytes)
    {
        return None;
    }

    let rows_per_block = rows_per_block?;
    (rows_per_block <= HISTORICAL_SPARSE_ROWS_PER_BLOCK)
        .then_some(HISTORICAL_SPARSE_FETCH_PIPELINE_DEPTH)
}

fn historical_dense_fetch_pipeline_depth_boost(
    serving_peers: usize,
    total_memory_bytes: Option<u64>,
    available_memory_bytes: Option<u64>,
    rows_per_block: Option<f64>,
) -> Option<usize> {
    if serving_peers < HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS
        || !historical_allows_high_memory_pipeline(total_memory_bytes)
        || historical_available_memory_is_low(available_memory_bytes)
    {
        return None;
    }

    let rows_per_block = rows_per_block?;
    (HISTORICAL_DENSE_ROWS_PER_BLOCK..HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK)
        .contains(&rows_per_block)
        .then_some(HISTORICAL_DENSE_FETCH_PIPELINE_DEPTH)
}

fn historical_fetch_buffer_depth(
    pipeline_depth: usize,
    available_memory_bytes: Option<u64>,
    rows_per_block: Option<f64>,
) -> usize {
    if pipeline_depth <= 1 || historical_available_memory_is_low(available_memory_bytes) {
        return pipeline_depth;
    }

    let extra = match rows_per_block {
        Some(rows_per_block) if rows_per_block >= HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK => {
            HISTORICAL_DENSE_FETCH_BUFFER_EXTRA / 2
        }
        Some(rows_per_block) if rows_per_block >= HISTORICAL_DENSE_ROWS_PER_BLOCK => {
            HISTORICAL_DENSE_FETCH_BUFFER_EXTRA
        }
        _ => pipeline_depth,
    };
    pipeline_depth
        .saturating_add(extra)
        .min(HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT)
}

fn historical_fetch_budget_has_capacity(
    pending_fetches: usize,
    completed_fetches: usize,
    buffer_depth: usize,
    available_memory_bytes: Option<u64>,
) -> bool {
    if historical_available_memory_is_low(available_memory_bytes) {
        pending_fetches < buffer_depth
    } else {
        completed_fetches < buffer_depth
    }
}

fn historical_density_fetch_window_cap(rows_per_block: Option<f64>) -> Option<u64> {
    let rows_per_block = rows_per_block?;
    if rows_per_block >= HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK {
        Some(HISTORICAL_VERY_DENSE_FETCH_WINDOW_BLOCKS)
    } else if rows_per_block >= HISTORICAL_DENSE_ROWS_PER_BLOCK {
        Some(HISTORICAL_DENSE_FETCH_WINDOW_BLOCKS)
    } else if rows_per_block >= HISTORICAL_SPARSE_ROWS_PER_BLOCK {
        Some(historical_medium_density_fetch_window(rows_per_block))
    } else {
        None
    }
}

fn historical_density_fetch_window_boost(
    serving_peers: usize,
    total_memory_bytes: Option<u64>,
    available_memory_bytes: Option<u64>,
    rows_per_block: Option<f64>,
) -> Option<u64> {
    if serving_peers < HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS
        || !historical_allows_sparse_pipeline(total_memory_bytes)
        || historical_available_memory_is_low(available_memory_bytes)
    {
        return None;
    }

    let rows_per_block = rows_per_block?;
    if !(0.0..=HISTORICAL_SPARSE_ROWS_PER_BLOCK).contains(&rows_per_block) {
        return None;
    }

    let window = (HISTORICAL_MEDIUM_DENSITY_TARGET_FETCH_ROWS / rows_per_block)
        .floor()
        .max(HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS as f64) as u64;
    Some(window.min(HISTORICAL_MEDIUM_DENSITY_MAX_FETCH_WINDOW_BLOCKS))
}

fn historical_medium_density_fetch_window(rows_per_block: f64) -> u64 {
    if rows_per_block <= 0.0 {
        return HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS;
    }
    (HISTORICAL_MEDIUM_DENSITY_TARGET_FETCH_ROWS / rows_per_block)
        .floor()
        .max(HISTORICAL_LOW_PEER_FETCH_WINDOW_BLOCKS as f64)
        .min(HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS as f64) as u64
}

fn update_historical_density_ewma(current: Option<f64>, rows: u64, blocks: usize) -> Option<f64> {
    if blocks == 0 {
        return current;
    }
    let sample = rows as f64 / blocks as f64;
    Some(match current {
        Some(current) => {
            (current * (1.0 - HISTORICAL_DENSITY_EWMA_WEIGHT))
                + (sample * HISTORICAL_DENSITY_EWMA_WEIGHT)
        }
        None => sample,
    })
}

fn historical_allows_high_memory_pipeline(total_memory_bytes: Option<u64>) -> bool {
    total_memory_bytes.is_none_or(|bytes| bytes >= HISTORICAL_HIGH_PIPELINE_MIN_TOTAL_MEMORY_BYTES)
}

fn historical_allows_sparse_pipeline(total_memory_bytes: Option<u64>) -> bool {
    total_memory_bytes
        .is_none_or(|bytes| bytes >= HISTORICAL_SPARSE_PIPELINE_MIN_TOTAL_MEMORY_BYTES)
}

fn historical_allows_medium_memory_pipeline(total_memory_bytes: Option<u64>) -> bool {
    total_memory_bytes
        .is_none_or(|bytes| bytes >= HISTORICAL_MEDIUM_PIPELINE_MIN_TOTAL_MEMORY_BYTES)
}

fn historical_allows_deep_windows(total_memory_bytes: Option<u64>) -> bool {
    total_memory_bytes.is_none_or(|bytes| bytes >= HISTORICAL_DEEP_WINDOW_MIN_TOTAL_MEMORY_BYTES)
}

fn historical_allows_wide_windows(total_memory_bytes: Option<u64>) -> bool {
    total_memory_bytes.is_none_or(|bytes| bytes >= HISTORICAL_WIDE_WINDOW_MIN_TOTAL_MEMORY_BYTES)
}

fn historical_available_memory_is_low(available_memory_bytes: Option<u64>) -> bool {
    available_memory_bytes.is_some_and(|bytes| bytes < HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES)
}

fn historical_available_memory_is_critical(available_memory_bytes: Option<u64>) -> bool {
    available_memory_bytes.is_some_and(|bytes| bytes < HISTORICAL_CRITICAL_AVAILABLE_MEMORY_BYTES)
}

fn historical_allocator_trim_is_due(
    last_trimmed_at: Option<std::time::Instant>,
    now: std::time::Instant,
    available_memory_bytes: Option<u64>,
) -> bool {
    historical_available_memory_is_low(available_memory_bytes)
        && last_trimmed_at.is_none_or(|trimmed_at| {
            now.duration_since(trimmed_at) >= HISTORICAL_ALLOCATOR_TRIM_INTERVAL
        })
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn trim_process_allocator() -> bool {
    // SAFETY: glibc documents malloc_trim as process-global and thread-safe. It
    // only asks the allocator to return free arenas to the OS; live allocations
    // are not moved or invalidated.
    unsafe { libc::malloc_trim(0) != 0 }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn trim_process_allocator() -> bool {
    false
}

fn historical_total_memory_bytes() -> Option<u64> {
    static TOTAL_MEMORY_BYTES: OnceLock<Option<u64>> = OnceLock::new();
    *TOTAL_MEMORY_BYTES.get_or_init(read_platform_total_memory_bytes)
}

fn historical_available_memory_bytes() -> Option<u64> {
    read_platform_available_memory_bytes()
}

#[cfg(target_os = "linux")]
fn read_platform_available_memory_bytes() -> Option<u64> {
    read_linux_meminfo_bytes("MemAvailable:")
}

#[cfg(target_os = "linux")]
fn read_platform_total_memory_bytes() -> Option<u64> {
    read_linux_meminfo_bytes("MemTotal:")
}

#[cfg(target_os = "macos")]
fn read_platform_available_memory_bytes() -> Option<u64> {
    read_darwin_available_memory_bytes()
}

#[cfg(target_os = "macos")]
fn read_platform_total_memory_bytes() -> Option<u64> {
    read_darwin_total_memory_bytes()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_platform_available_memory_bytes() -> Option<u64> {
    None
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn read_platform_total_memory_bytes() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_linux_meminfo_bytes(prefix: &str) -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        let Some(rest) = line.strip_prefix(prefix) else {
            continue;
        };
        let kib = rest
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())?;
        return kib.checked_mul(BYTES_PER_KIB);
    }
    None
}

#[cfg(target_os = "macos")]
fn read_darwin_total_memory_bytes() -> Option<u64> {
    let name = b"hw.memsize\0";
    let mut value = 0u64;
    let mut size = std::mem::size_of::<u64>() as libc::size_t;
    let rc = unsafe {
        libc::sysctlbyname(
            name.as_ptr().cast(),
            (&mut value as *mut u64).cast(),
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };
    (rc == 0 && size == std::mem::size_of::<u64>() as libc::size_t).then_some(value)
}

#[cfg(target_os = "macos")]
fn read_darwin_available_memory_bytes() -> Option<u64> {
    let mut stats: libc::vm_statistics64_data_t = unsafe { std::mem::zeroed() };
    let mut count = libc::HOST_VM_INFO64_COUNT;
    #[allow(deprecated)]
    let host = unsafe { libc::mach_host_self() };
    let rc = unsafe {
        libc::host_statistics64(
            host,
            libc::HOST_VM_INFO64,
            (&mut stats as *mut libc::vm_statistics64_data_t).cast(),
            &mut count,
        )
    };
    if rc != libc::KERN_SUCCESS {
        return None;
    }

    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    if page_size <= 0 {
        return None;
    }

    let reclaimable_pages = u64::from(stats.free_count)
        .saturating_add(u64::from(stats.inactive_count))
        .saturating_add(u64::from(stats.speculative_count))
        .saturating_add(u64::from(stats.purgeable_count));
    reclaimable_pages.checked_mul(page_size as u64)
}

fn validate_and_extract_historical_block_chunk(
    jobs: Vec<HistoricalValidationJob>,
) -> std::result::Result<HistoricalValidationExtractedChunk, Box<HistoricalValidationFailure>> {
    let validation_started = std::time::Instant::now();
    let start_index = jobs.first().map_or(0, |job| job.index);
    let block_count = jobs.len();
    let total_log_capacity = jobs
        .iter()
        .map(|job| {
            job.receipts
                .iter()
                .map(|receipt| receipt.logs().len())
                .sum::<usize>()
        })
        .sum();
    let mut rows = Vec::with_capacity(total_log_capacity);
    let mut peer_notes = Vec::new();
    let mut lowest_header = None;
    let mut lowest_block = u64::MAX;
    let mut highest_block = 0u64;
    let mut extraction_elapsed = Duration::ZERO;

    for job in jobs {
        let block_number = job.header.number();
        if let Err(error) = validate_block_pre_execution(&job.header, job.block_hash, &job.body) {
            return Err(Box::new(HistoricalValidationFailure {
                peer: job.body_peer,
                response_kind: "block bodies",
                block_number,
                block_hash: job.block_hash,
                message: error.to_string(),
            }));
        }

        if !receipts_match_transaction_count(&job.body, &job.receipts) {
            return Err(Box::new(HistoricalValidationFailure {
                peer: job.receipt_peer,
                response_kind: "receipts",
                block_number,
                block_hash: job.block_hash,
                message: format!(
                    "transaction/receipt count mismatch: transactions={}, receipts={}",
                    job.body.transaction_count(),
                    job.receipts.len()
                ),
            }));
        }

        if let Err(error) = validate_receipts_for_header(&job.header, &job.receipts) {
            return Err(Box::new(HistoricalValidationFailure {
                peer: job.receipt_peer,
                response_kind: "receipts",
                block_number,
                block_hash: job.block_hash,
                message: error.to_string(),
            }));
        }

        let extraction_started = std::time::Instant::now();
        extract::append_from_body_receipts(
            &mut rows,
            block_number,
            job.block_hash,
            job.header.timestamp(),
            &job.body,
            &job.receipts,
        );
        extraction_elapsed += extraction_started.elapsed();

        push_unique_peer_note(&mut peer_notes, job.body_peer);
        push_unique_peer_note(&mut peer_notes, job.receipt_peer);
        lowest_block = lowest_block.min(block_number);
        highest_block = highest_block.max(block_number);
        if lowest_header
            .as_ref()
            .is_none_or(|header: &Header| block_number < header.number())
        {
            lowest_header = Some(job.header);
        }
    }

    let lowest_header = lowest_header.unwrap_or_else(|| Header {
        number: 0,
        ..Default::default()
    });
    let row_count = rows.len() as u64;
    Ok(HistoricalValidationExtractedChunk {
        start_index,
        peer_notes,
        extracted: super::ingest::HistoricalExtractedChunk {
            rows,
            row_count,
            block_count,
            lowest_header,
            extraction_elapsed,
        },
        lowest_block,
        highest_block,
        validation_elapsed: validation_started.elapsed(),
    })
}

fn push_unique_peer_note(peer_notes: &mut Vec<PeerId>, peer_id: PeerId) {
    if peer_id != PeerId::ZERO && !peer_notes.contains(&peer_id) {
        peer_notes.push(peer_id);
    }
}

fn validate_historical_block_chunk(
    jobs: Vec<HistoricalValidationJob>,
) -> std::result::Result<Vec<HistoricalValidatedBlock>, Box<HistoricalValidationFailure>> {
    let mut validated = Vec::with_capacity(jobs.len());
    for job in jobs {
        validated.push(validate_historical_block(
            job.index,
            job.header,
            job.block_hash,
            job.body_peer,
            job.body,
            job.receipt_peer,
            job.receipts,
        )?);
    }
    Ok(validated)
}

fn validate_historical_block(
    index: usize,
    header: Header,
    block_hash: B256,
    body_peer: PeerId,
    body: <LogexNetworkPrimitives as NetworkPrimitives>::BlockBody,
    receipt_peer: PeerId,
    receipts: Vec<ReceiptWithBloom<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>>,
) -> std::result::Result<HistoricalValidatedBlock, Box<HistoricalValidationFailure>> {
    let block_number = header.number();
    if let Err(error) = validate_block_pre_execution(&header, block_hash, &body) {
        return Err(Box::new(HistoricalValidationFailure {
            peer: body_peer,
            response_kind: "block bodies",
            block_number,
            block_hash,
            message: error.to_string(),
        }));
    }

    if !receipts_match_transaction_count(&body, &receipts) {
        return Err(Box::new(HistoricalValidationFailure {
            peer: receipt_peer,
            response_kind: "receipts",
            block_number,
            block_hash,
            message: format!(
                "transaction/receipt count mismatch: transactions={}, receipts={}",
                body.transaction_count(),
                receipts.len()
            ),
        }));
    }

    if let Err(error) = validate_receipts_for_header(&header, &receipts) {
        return Err(Box::new(HistoricalValidationFailure {
            peer: receipt_peer,
            response_kind: "receipts",
            block_number,
            block_hash,
            message: error.to_string(),
        }));
    }

    Ok(HistoricalValidatedBlock {
        index,
        header,
        block_hash,
        body_peer,
        body,
        receipt_peer,
        receipts,
    })
}

fn spawn_historical_prepare_task(
    sequence: u64,
    batch: HistoricalFetchedBatch,
) -> HistoricalPrepareTask {
    let next_child_header = historical_batch_next_child_header(&batch);
    let refill_child_header = next_child_header.clone();
    let handle =
        tokio::spawn(async move { process_historical_batch(batch, refill_child_header).await });

    HistoricalPrepareTask {
        sequence,
        next_child_header,
        handle,
    }
}

async fn process_historical_batch(
    batch: HistoricalFetchedBatch,
    next_child_header: Option<Header>,
) -> Result<std::result::Result<PreparedHistoricalBatch, Box<HistoricalValidationFailure>>> {
    let HistoricalFetchedBatch {
        header_peer,
        headers,
        hashes,
        blocks,
        required_block,
        header_elapsed,
        body_receipt_elapsed,
        residual_header_batch,
    } = batch;
    let requested_headers = headers.len();

    let processing_started = std::time::Instant::now();
    let (
        extracted,
        mut block_peer_notes,
        lowest_block,
        highest_block,
        block_count,
        validation_elapsed,
    ) = match validate_and_extract_historical_blocks_streaming(&headers, &hashes, blocks).await? {
        Ok(processed) => processed,
        Err(failure) => return Ok(Err(failure)),
    };
    let processing_elapsed = processing_started.elapsed();

    let mut peer_notes = Vec::with_capacity(block_peer_notes.len() + 1);
    peer_notes.push(header_peer);
    peer_notes.append(&mut block_peer_notes);

    let lowest_block = if block_count == 0 {
        required_block
    } else {
        lowest_block
    };
    let highest_block = if block_count == 0 {
        required_block
    } else {
        highest_block
    };

    Ok(Ok(PreparedHistoricalBatch {
        requested_headers,
        next_child_header,
        header_elapsed,
        body_receipt_elapsed,
        extracted,
        peer_notes,
        lowest_block,
        highest_block,
        block_count,
        validation_elapsed,
        processing_elapsed,
        residual_header_batch,
    }))
}

async fn write_prepared_historical_batch(
    prepared: PreparedHistoricalBatch,
    storage: Arc<RwLock<PartitionManager>>,
) -> Result<WrittenHistoricalBatch> {
    let PreparedHistoricalBatch {
        requested_headers,
        next_child_header,
        header_elapsed,
        body_receipt_elapsed,
        extracted,
        peer_notes,
        lowest_block,
        highest_block,
        block_count,
        validation_elapsed,
        processing_elapsed,
        residual_header_batch,
    } = prepared;
    let write_started = std::time::Instant::now();
    let outcome = super::ingest::write_extracted_historical_batch(storage, extracted).await?;
    let processing_elapsed = processing_elapsed.saturating_add(write_started.elapsed());

    Ok(WrittenHistoricalBatch {
        requested_headers,
        next_child_header,
        header_elapsed,
        body_receipt_elapsed,
        outcome,
        peer_notes,
        lowest_block,
        highest_block,
        block_count,
        validation_elapsed,
        prepare_wait_elapsed: Duration::ZERO,
        processing_elapsed,
        residual_header_batch,
    })
}

fn historical_batch_next_child_header(batch: &HistoricalFetchedBatch) -> Option<Header> {
    if let Some(residual_child) = batch
        .residual_header_batch
        .as_ref()
        .and_then(|residual| residual.headers.last().cloned())
    {
        return Some(residual_child);
    }

    batch
        .blocks
        .len()
        .checked_sub(1)
        .and_then(|index| batch.headers.get(index))
        .cloned()
}

fn historical_residual_header_batch(
    header_peer: PeerId,
    headers: &[Header],
    hashes: &[B256],
    consumed_blocks: usize,
    planned_return_blocks: usize,
) -> Option<HistoricalHeaderBatch> {
    let residual_end = planned_return_blocks.min(headers.len()).min(hashes.len());
    if consumed_blocks == 0 || consumed_blocks >= residual_end {
        return None;
    }

    let child_header = headers.get(consumed_blocks.checked_sub(1)?)?.clone();
    let residual_headers = headers.get(consumed_blocks..residual_end)?.to_vec();
    let residual_hashes = hashes.get(consumed_blocks..residual_end)?.to_vec();
    let required_block = residual_headers
        .last()
        .map(|header| header.number())
        .unwrap_or_else(|| child_header.number().saturating_sub(1));

    Some(HistoricalHeaderBatch {
        child_header,
        header_peer,
        headers: residual_headers,
        hashes: residual_hashes,
        required_block,
        header_elapsed: Duration::ZERO,
    })
}

fn historical_header_has_empty_body_and_receipts(header: &Header) -> bool {
    header.transactions_root() == EMPTY_ROOT_HASH
        && header.receipts_root() == EMPTY_ROOT_HASH
        && header.ommers_hash() == EMPTY_OMMER_ROOT_HASH
        && header
            .withdrawals_root()
            .is_none_or(|root| root == EMPTY_ROOT_HASH)
}

fn historical_header_batch_matches_child(
    batch: &HistoricalHeaderBatch,
    child_header: &Header,
) -> bool {
    batch.child_header.number() == child_header.number()
        && batch.child_header.hash_slow() == child_header.hash_slow()
}

impl SyncEngine {
    pub(super) async fn run_consensus_sync(&mut self) -> Result<()> {
        self.refresh_consensus_status().await;
        self.set_runtime_state(NodeState::Discovering);

        let mut attempt: u32 = 0;
        loop {
            if self.shutdown_requested() {
                return self.finish_shutdown();
            }
            self.refresh_consensus_status().await;
            if !self.set_peer_head_from_consensus() {
                self.set_runtime_state(NodeState::WaitingForConsensus);
                if cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(CONSENSUS_WAIT_INTERVAL),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                continue;
            }
            self.refresh_connectivity_state();
            let min_peers = peer_refill_goal(
                self.peers.peer_count(),
                self.peers.serving_peer_count(),
                self.config.max_peers,
            )
            .unwrap_or_else(|| refill_peer_floor(self.config.max_peers));
            if cancelable(
                &mut self.shutdown,
                self.peers.fill_peers(min_peers, self.config.max_peers),
            )
            .await
            .is_none()
            {
                return self.finish_shutdown();
            }
            self.refresh_connectivity_state();
            let required_block = self
                .consensus
                .as_ref()
                .and_then(|consensus| consensus.anchor_coverage().floor)
                .map_or(1, |anchor| anchor.block_number);
            if self.peers.has_block_request_peer(required_block) {
                self.connected_once = true;
                break;
            }
            attempt += 1;
            let delay = Duration::from_secs((attempt as u64).min(10));
            tracing::warn!(
                attempt,
                ?delay,
                "no execution peer eligible for consensus-anchored sync yet"
            );
            if cancelable(&mut self.shutdown, tokio::time::sleep(delay))
                .await
                .is_none()
            {
                return self.finish_shutdown();
            }
        }

        loop {
            if self.shutdown_requested() {
                return self.finish_shutdown();
            }

            if let Some(min_peers) = peer_refill_goal(
                self.peers.peer_count(),
                self.peers.serving_peer_count(),
                self.config.max_peers,
            ) {
                self.refresh_connectivity_state();
                if self.peers.serving_peer_count() < MIN_ACTIVE_SYNC_PEERS {
                    if cancelable(
                        &mut self.shutdown,
                        self.peers.fill_peers(min_peers, self.config.max_peers),
                    )
                    .await
                    .is_none()
                    {
                        return self.finish_shutdown();
                    }
                } else {
                    self.peers.drain_events_now();
                }
                self.refresh_connectivity_state();
            }

            let current = self.current_block();
            self.refresh_consensus_status().await;
            self.set_peer_head_from_consensus();
            self.refresh_historical_status().await;
            if self.reconcile_consensus_reorg().await? {
                continue;
            }

            let Some(consensus) = self.consensus.clone() else {
                return Ok(());
            };

            let anchor_batch_limit = if current == 0 {
                1
            } else {
                self.config
                    .header_batch_size
                    .min(CONSENSUS_ANCHOR_FORWARD_BATCH_LIMIT)
            };
            let anchors = self
                .next_consensus_anchor_batch(current, &consensus, anchor_batch_limit)
                .await;
            if anchors.is_empty() {
                if self.try_mark_synced("caught up to available consensus anchors") {
                    self.sync_status_peers();
                } else {
                    self.set_runtime_state(NodeState::WaitingForConsensus);
                }
                if !self.config.disable_historical_sync
                    && self.ingest_historical_backfill_batch().await?
                {
                    continue;
                }
                if cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(CONSENSUS_WAIT_INTERVAL),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                continue;
            };

            let forward_progressed = self.ingest_anchored_blocks(anchors).await?;
            let (current, target) = self.sync_cursor();
            let historical_progressed = if !self.config.disable_historical_sync
                && should_run_historical_backfill(
                    current,
                    target,
                    LIVE_LAG_HISTORICAL_BACKFILL_THRESHOLD,
                ) {
                self.ingest_historical_backfill_batch().await?
            } else {
                false
            };
            if !(forward_progressed || historical_progressed)
                && cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(CONSENSUS_WAIT_INTERVAL),
                )
                .await
                .is_none()
            {
                return self.finish_shutdown();
            }
        }
    }

    async fn ingest_anchored_blocks(&mut self, anchors: Vec<ExecutionAnchor>) -> Result<bool> {
        let Some(first_anchor) = anchors.first().copied() else {
            return Ok(false);
        };
        let request_count = anchors.len() as u64;
        let (header_peer, headers) = match cancelable(
            &mut self.shutdown,
            self.peers
                .get_headers(first_anchor.block_number, request_count),
        )
        .await
        {
            Some(Ok((peer_id, headers))) if !headers.is_empty() => (peer_id, headers),
            Some(Ok((_peer_id, _headers))) => {
                tracing::debug!(
                    block_number = first_anchor.block_number,
                    "no peer returned the anchored headers yet"
                );
                return Ok(false);
            }
            Some(Err(error)) => {
                tracing::warn!(
                    error = %error,
                    start_block = first_anchor.block_number,
                    request_count,
                    "anchored header batch request failed"
                );
                return Ok(false);
            }
            None => {
                self.finish_shutdown()?;
                return Ok(false);
            }
        };

        if headers.len() > anchors.len() {
            tracing::warn!(
                requested = anchors.len(),
                returned = headers.len(),
                header_peer = %header_peer,
                "anchored header batch returned more headers than requested"
            );
            self.peers.report_invalid_block_data(header_peer, "headers");
            return Ok(false);
        }

        let anchors = &anchors[..headers.len()];
        if let Err(error) = validate_downloaded_headers(
            first_anchor.block_number,
            self.expected_parent_for_validation(first_anchor.block_number),
            &headers,
        ) {
            tracing::warn!(
                start_block = first_anchor.block_number,
                headers = headers.len(),
                header_peer = %header_peer,
                %error,
                "anchored header batch validation failed"
            );
            self.peers.report_invalid_block_data(header_peer, "headers");
            return Ok(false);
        }

        for (anchor, header) in anchors.iter().zip(&headers) {
            let block_hash = header.hash_slow();
            if let Err(error) = validate_header_matches_anchor(anchor, header, block_hash) {
                tracing::warn!(
                    block_number = anchor.block_number,
                    expected_hash = %anchor.block_hash,
                    got_hash = %block_hash,
                    header_peer = %header_peer,
                    %error,
                    "anchored header did not match the consensus anchor"
                );
                self.peers.report_invalid_block_data(header_peer, "headers");
                return Ok(false);
            }
        }

        let mut newly_serving_peers = HashSet::new();
        let hashes: Vec<B256> = headers.iter().map(|header| header.hash_slow()).collect();
        let mut progressed = false;
        let mut last_validated_header = None;
        let mut last_head = None;

        for ((chunk_headers, chunk_hashes), chunk_anchors) in headers
            .chunks(self.config.fetch_batch_size)
            .zip(hashes.chunks(self.config.fetch_batch_size))
            .zip(anchors.chunks(self.config.fetch_batch_size))
        {
            let chunk_headers = chunk_headers.to_vec();
            let chunk_hashes = chunk_hashes.to_vec();
            let required_block = chunk_headers
                .last()
                .map(|header| header.number())
                .unwrap_or(first_anchor.block_number);

            let bodies = match cancelable(
                &mut self.shutdown,
                self.peers.get_bodies_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &[header_peer],
                ),
            )
            .await
            {
                Some(Ok(bodies)) if bodies.len() == chunk_headers.len() => bodies,
                Some(Ok(bodies)) => {
                    tracing::warn!(
                        headers = chunk_headers.len(),
                        bodies = bodies.len(),
                        "anchored block body request returned an unexpected response"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::warn!(%error, "anchored block body request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            for (i, header) in chunk_headers.iter().enumerate() {
                let block_number = header.number();
                let block_hash = chunk_hashes[i];
                let (body_peer, body) = &bodies[i];
                if let Err(error) = validate_block_pre_execution(header, block_hash, body) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        body_peer = %body_peer,
                        %error,
                        "anchored block pre-execution validation failed"
                    );
                    self.peers
                        .report_invalid_block_data(*body_peer, "block bodies");
                    return Ok(progressed);
                }
            }

            let expected_receipt_counts: Vec<usize> = bodies
                .iter()
                .map(|(_peer_id, body)| body.transaction_count())
                .collect();
            let receipt_peer_preference = preferred_body_peers(&bodies, header_peer);
            let (receipt_peer, receipts) = match cancelable(
                &mut self.shutdown,
                self.peers.get_receipts_matching_counts_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &expected_receipt_counts,
                    &receipt_peer_preference,
                ),
            )
            .await
            {
                Some(Ok((peer_id, receipts))) if receipts.len() == chunk_headers.len() => {
                    (peer_id, receipts)
                }
                Some(Ok((peer_id, receipts))) => {
                    tracing::warn!(
                        headers = chunk_headers.len(),
                        receipt_peer = %peer_id,
                        returned_receipt_sets = receipts.len(),
                        "anchored receipt request returned an unexpected response"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::warn!(%error, "anchored receipt request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            for (i, header) in chunk_headers.iter().enumerate() {
                let anchor = chunk_anchors[i];
                let block_hash = chunk_hashes[i];
                let block_number = header.number();
                let (body_peer, body) = &bodies[i];

                if !receipts_match_transaction_count(body, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        transactions = body.transaction_count(),
                        receipts = receipts[i].len(),
                        "anchored block body / receipt count mismatch"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    return Ok(progressed);
                }

                if let Err(error) = validate_receipts_for_header(header, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        %error,
                        "anchored receipt validation failed"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    return Ok(progressed);
                }

                let txs = assemble_txs(body, &receipts[i]);
                if let Some(reorg) = self.head_tracker.track(header.clone()) {
                    self.handle_reorg(reorg).await?;
                }

                let recent_headers = self.head_tracker.snapshot();
                self.peers
                    .cache_canonical_block(header.clone(), body.clone(), &receipts[i]);
                let log_count = self
                    .ingest_block(header, block_hash, &txs, &recent_headers, Some(&anchor))
                    .await?;
                self.progress.record_block(block_number, log_count);
                self.note_serving_peer(header_peer, &mut newly_serving_peers);
                self.note_serving_peer(*body_peer, &mut newly_serving_peers);
                self.note_serving_peer(receipt_peer, &mut newly_serving_peers);
                last_validated_header = Some(header.clone());
                last_head = Some(execution_head(block_number, block_hash, header.timestamp()));
                progressed = true;
            }
        }

        self.last_validated_header = last_validated_header;
        self.refresh_consensus_status().await;
        self.refresh_historical_status().await;

        if let Some(head) = last_head {
            self.peers.set_head(head);
        }

        if self.try_mark_synced("caught up to available consensus anchors") {
            self.sync_status_peers();
        } else {
            self.refresh_connectivity_state();
        }

        Ok(progressed)
    }

    async fn next_consensus_anchor_batch(
        &self,
        current: u64,
        consensus: &ConsensusStore,
        limit: u64,
    ) -> Vec<ExecutionAnchor> {
        let storage_has_head = if current == 0 {
            let storage = self.storage.read().await;
            storage.sync_head().is_some()
        } else {
            true
        };
        contiguous_anchor_batch(
            current,
            !storage_has_head,
            &consensus.ordered_anchors(),
            limit as usize,
        )
    }
}

fn contiguous_anchor_batch(
    current: u64,
    allow_bootstrap_jump: bool,
    records: &[logex_cl::AnchorRecord],
    limit: usize,
) -> Vec<ExecutionAnchor> {
    if limit == 0 {
        return Vec::new();
    }

    let mut anchors = Vec::new();
    let mut next_expected = current.saturating_add(1);
    for record in records
        .iter()
        .map(|record| record.anchor)
        .filter(|anchor| anchor.block_number > current)
    {
        if anchors.is_empty() {
            if record.block_number == next_expected
                || can_bootstrap_from_anchor(current, record.block_number) && allow_bootstrap_jump
            {
                next_expected = record.block_number.saturating_add(1);
                anchors.push(record);
            } else {
                return Vec::new();
            }
        } else if record.block_number == next_expected {
            next_expected = next_expected.saturating_add(1);
            anchors.push(record);
        } else {
            break;
        }

        if anchors.len() >= limit {
            break;
        }
    }

    anchors
}

impl SyncEngine {
    async fn ingest_historical_backfill_batch(&mut self) -> Result<bool> {
        let child_header = {
            let storage = self.storage.read().await;
            storage.historical_floor_header().cloned()
        };
        let Some(child_header) = child_header else {
            return Ok(false);
        };
        if child_header.number() == EXECUTION_HISTORY_TARGET_BLOCK {
            {
                let mut storage = self.storage.write().await;
                storage.finalize_historical_segment().map_err(|error| {
                    eyre::eyre!("historical storage finalization error: {error}")
                })?;
            }
            self.refresh_historical_status().await;
            return Ok(false);
        }

        if self.peers.peer_count() == 0 {
            self.refresh_connectivity_state();
            return Ok(false);
        }

        let connected_peer_floor = historical_backfill_peer_floor(self.config.max_peers);
        if self.peers.peer_count() < connected_peer_floor
            && !self.has_ready_historical_fetch_for(&child_header)
        {
            self.refresh_connectivity_state();
            tracing::debug!(
                connected_peers = self.peers.peer_count(),
                connected_peer_floor,
                "waiting for execution peer pool before historical backfill"
            );
            return Ok(false);
        }

        self.drain_historical_prepare_tasks().await?;
        let expected_prepare_sequence = self.historical_prepare_expected_sequence;
        if let Some(result) = self
            .historical_prepare_completed
            .remove(&expected_prepare_sequence)
        {
            return self
                .ingest_historical_prepare_result(expected_prepare_sequence, result, true)
                .await;
        }
        if let Some(task) = self
            .historical_prepare_handles
            .remove(&expected_prepare_sequence)
        {
            return self.ingest_historical_prepared_task(task, true).await;
        }

        let Some((sequence, batch, prefetched)) = self
            .fetch_historical_combined_batch(child_header.clone())
            .await?
        else {
            if self.shutdown_requested() {
                self.finish_shutdown()?;
                return Ok(false);
            }
            return self
                .ingest_historical_backfill_batch_sequential(child_header)
                .await;
        };
        let task = spawn_historical_prepare_task(sequence, batch);

        self.ingest_historical_prepared_task(task, prefetched).await
    }

    pub(super) fn reset_historical_fetch_pipeline(&mut self) {
        for (_, handle) in self.historical_fetch_handles.drain() {
            handle.abort();
        }
        self.reset_historical_prepare_pipeline();
        self.historical_fetch_generation = self.historical_fetch_generation.wrapping_add(1);
        self.historical_fetch_next_sequence = 0;
        self.historical_fetch_expected_sequence = 0;
        self.historical_prepare_expected_sequence = self.historical_fetch_expected_sequence;
        self.historical_fetch_expected_child = None;
        self.historical_fetch_planned_child = None;
        self.historical_fetch_completed.clear();
        while self.historical_fetch_rx.try_recv().is_ok() {}
    }

    fn reset_historical_prepare_pipeline(&mut self) {
        for (_, task) in std::mem::take(&mut self.historical_prepare_handles) {
            task.handle.abort();
        }
        self.historical_prepare_expected_sequence = self.historical_fetch_expected_sequence;
        self.historical_prepare_completed.clear();
    }

    fn store_historical_fetch_outcome(&mut self, mut outcome: HistoricalFetchOutcome) {
        if outcome.generation != self.historical_fetch_generation {
            return;
        }

        self.historical_fetch_handles.remove(&outcome.sequence);
        if outcome.sequence < self.historical_fetch_expected_sequence {
            return;
        }
        self.peers
            .apply_body_receipt_request_accounting(&mut outcome.outcome);
        self.historical_fetch_completed
            .insert(outcome.sequence, outcome);
    }

    fn drain_historical_fetch_outcomes(&mut self) {
        while let Ok(outcome) = self.historical_fetch_rx.try_recv() {
            self.store_historical_fetch_outcome(outcome);
        }
    }

    async fn drain_historical_prepare_tasks(&mut self) -> Result<()> {
        let finished = self
            .historical_prepare_handles
            .iter()
            .filter_map(|(sequence, task)| task.handle.is_finished().then_some(*sequence))
            .collect::<Vec<_>>();
        for sequence in finished {
            let Some(task) = self.historical_prepare_handles.remove(&sequence) else {
                continue;
            };
            let result = task
                .handle
                .await
                .map_err(|error| eyre::eyre!("historical prepare worker failed: {error}"))?;
            self.historical_prepare_completed.insert(sequence, result);
        }
        Ok(())
    }

    fn pending_historical_prepare_count(&self) -> usize {
        self.historical_prepare_handles.len() + self.historical_prepare_completed.len()
    }

    async fn spawn_ready_historical_prepare_tasks(&mut self) -> Result<()> {
        self.drain_historical_fetch_outcomes();
        self.drain_historical_prepare_tasks().await?;

        while self.pending_historical_prepare_count() < HISTORICAL_PREPARE_LOOKAHEAD_DEPTH {
            let Some(sequence) = self.next_historical_fetch_sequence_to_prepare() else {
                break;
            };
            let Some(outcome) = self.historical_fetch_completed.remove(&sequence) else {
                break;
            };
            let expected_sequence = sequence == self.historical_fetch_expected_sequence;
            let Some((sequence, batch, next_child_header)) =
                self.materialize_historical_fetch_outcome(outcome)?
            else {
                if expected_sequence {
                    self.reset_historical_fetch_pipeline();
                }
                break;
            };
            if expected_sequence {
                self.advance_historical_fetch_sequence(next_child_header);
            }
            let task = spawn_historical_prepare_task(sequence, batch);
            self.historical_prepare_handles.insert(sequence, task);

            if expected_sequence {
                let Some(next_child_header) = self.historical_fetch_expected_child.clone() else {
                    break;
                };
                self.ensure_historical_fetch_pipeline(next_child_header)
                    .await?;
            }
            self.drain_historical_fetch_outcomes();
        }

        Ok(())
    }

    fn next_historical_fetch_sequence_to_prepare(&self) -> Option<u64> {
        if self
            .historical_fetch_completed
            .contains_key(&self.historical_fetch_expected_sequence)
        {
            return Some(self.historical_fetch_expected_sequence);
        }

        self.historical_fetch_completed
            .iter()
            .find_map(|(sequence, outcome)| {
                (*sequence > self.historical_fetch_expected_sequence
                    && outcome.outcome.has_accepted_contiguous_prefix())
                .then_some(*sequence)
            })
    }

    fn has_ready_historical_fetch_for(&mut self, child_header: &Header) -> bool {
        self.drain_historical_fetch_outcomes();
        self.historical_fetch_pipeline_matches(child_header)
            && self
                .historical_fetch_completed
                .contains_key(&self.historical_fetch_expected_sequence)
    }

    fn historical_fetch_pipeline_matches(&self, child_header: &Header) -> bool {
        self.historical_fetch_expected_child
            .as_ref()
            .is_some_and(|expected| {
                expected.number() == child_header.number()
                    && expected.hash_slow() == child_header.hash_slow()
            })
    }

    fn pending_historical_fetch_count(&self) -> usize {
        self.historical_fetch_handles.len() + self.historical_fetch_completed.len()
    }

    fn active_historical_fetch_count(&self) -> usize {
        self.historical_fetch_handles.len()
    }

    fn historical_fetch_window_blocks(&self) -> u64 {
        let total_memory_bytes = historical_total_memory_bytes();
        let available_memory_bytes = historical_available_memory_bytes();
        let base_window = historical_fetch_window_blocks_for_serving_peers(
            self.peers.serving_peer_count(),
            total_memory_bytes,
            available_memory_bytes,
        );
        let capped_window =
            historical_density_fetch_window_cap(self.historical_rows_per_block_ewma)
                .map(|cap| base_window.min(cap))
                .unwrap_or(base_window);
        historical_density_fetch_window_boost(
            self.peers.serving_peer_count(),
            total_memory_bytes,
            available_memory_bytes,
            self.historical_rows_per_block_ewma,
        )
        .map(|boost| capped_window.max(boost))
        .unwrap_or(capped_window)
    }

    fn spawn_historical_fetch_plan(&mut self, plan: HistoricalFetchPlan) {
        let generation = self.historical_fetch_generation;
        let sequence = self.historical_fetch_next_sequence;
        self.historical_fetch_next_sequence = self.historical_fetch_next_sequence.saturating_add(1);
        let tx = self.historical_fetch_tx.clone();
        let handle = tokio::spawn(async move {
            let body_receipt_started = std::time::Instant::now();
            let outcome = plan.body_receipt_plan.execute().await;
            let _ = tx.send(HistoricalFetchOutcome {
                generation,
                sequence,
                header_batch: plan.header_batch,
                body_receipt_elapsed: body_receipt_started.elapsed(),
                outcome,
            });
        });
        self.historical_fetch_handles.insert(sequence, handle);
    }

    async fn ensure_historical_fetch_pipeline(&mut self, child_header: Header) -> Result<()> {
        self.drain_historical_fetch_outcomes();

        let pipeline_empty = self.pending_historical_fetch_count() == 0
            && self.historical_fetch_planned_child.is_none();
        if !self.historical_fetch_pipeline_matches(&child_header) || pipeline_empty {
            self.reset_historical_fetch_pipeline();
            self.historical_fetch_expected_child = Some(child_header.clone());
            self.historical_fetch_planned_child = Some(child_header.clone());
        }

        let available_memory_bytes = historical_available_memory_bytes();
        let base_pipeline_depth = historical_fetch_pipeline_depth_for_serving_peers(
            self.peers.serving_peer_count(),
            historical_total_memory_bytes(),
            available_memory_bytes,
        );
        let sparse_pipeline_boost = historical_sparse_fetch_pipeline_depth_boost(
            self.peers.serving_peer_count(),
            historical_total_memory_bytes(),
            available_memory_bytes,
            self.historical_rows_per_block_ewma,
        );
        let dense_pipeline_boost = historical_dense_fetch_pipeline_depth_boost(
            self.peers.serving_peer_count(),
            historical_total_memory_bytes(),
            available_memory_bytes,
            self.historical_rows_per_block_ewma,
        );
        let base_pipeline_depth = sparse_pipeline_boost
            .map(|boost| base_pipeline_depth.max(boost))
            .unwrap_or(base_pipeline_depth);
        let base_pipeline_depth = dense_pipeline_boost
            .map(|boost| base_pipeline_depth.max(boost))
            .unwrap_or(base_pipeline_depth);
        let density_pipeline_cap =
            historical_density_fetch_pipeline_depth_cap(self.historical_rows_per_block_ewma);
        let pipeline_depth = density_pipeline_cap
            .map(|cap| base_pipeline_depth.min(cap))
            .unwrap_or(base_pipeline_depth);
        let buffer_depth = historical_fetch_buffer_depth(
            pipeline_depth,
            available_memory_bytes,
            self.historical_rows_per_block_ewma,
        );
        if historical_available_memory_is_critical(available_memory_bytes)
            && self.pending_historical_fetch_count() > buffer_depth
        {
            tracing::debug!(
                available_memory_bytes,
                pending_fetches = self.pending_historical_fetch_count(),
                pipeline_depth,
                buffer_depth,
                "resetting historical fetch lookahead under memory pressure"
            );
            self.reset_historical_fetch_pipeline();
            self.historical_fetch_expected_child = Some(child_header.clone());
            self.historical_fetch_planned_child = Some(child_header.clone());
        }
        while self.active_historical_fetch_count() < pipeline_depth
            && historical_fetch_budget_has_capacity(
                self.pending_historical_fetch_count(),
                self.historical_fetch_completed.len(),
                buffer_depth,
                available_memory_bytes,
            )
        {
            let Some(planned_child) = self.historical_fetch_planned_child.take() else {
                break;
            };
            if planned_child.number() == EXECUTION_HISTORY_TARGET_BLOCK {
                break;
            }

            let Some(plan) = self.prepare_historical_fetch_plan(planned_child).await? else {
                if self.pending_historical_fetch_count() == 0 {
                    self.reset_historical_fetch_pipeline();
                }
                self.historical_fetch_planned_child = None;
                break;
            };

            self.historical_fetch_planned_child = plan.planned_next_child_header.clone();
            self.spawn_historical_fetch_plan(plan);
        }

        Ok(())
    }

    async fn wait_for_historical_fetch_outcome(
        &mut self,
        child_header: &Header,
    ) -> Result<Option<HistoricalFetchOutcome>> {
        loop {
            self.drain_historical_fetch_outcomes();

            if let Some(outcome) = self
                .historical_fetch_completed
                .remove(&self.historical_fetch_expected_sequence)
            {
                if historical_header_batch_matches_child(&outcome.header_batch, child_header) {
                    return Ok(Some(outcome));
                }

                tracing::debug!(
                    expected_child = child_header.number(),
                    fetched_child = outcome.header_batch.child_header.number(),
                    "discarding stale historical fetch outcome"
                );
                self.reset_historical_fetch_pipeline();
                return Ok(None);
            }

            if self.historical_fetch_handles.is_empty() {
                return Ok(None);
            }

            match cancelable(&mut self.shutdown, self.historical_fetch_rx.recv()).await {
                Some(Some(outcome)) => {
                    self.store_historical_fetch_outcome(outcome);
                    self.ensure_historical_fetch_pipeline(child_header.clone())
                        .await?;
                }
                Some(None) => return Ok(None),
                None => {
                    self.finish_shutdown()?;
                    return Ok(None);
                }
            }
        }
    }

    fn complete_historical_fetch_outcome(
        &mut self,
        fetch: HistoricalFetchOutcome,
    ) -> Result<Option<(u64, HistoricalFetchedBatch)>> {
        let Some((sequence, batch, next_child_header)) =
            self.materialize_historical_fetch_outcome(fetch)?
        else {
            self.reset_historical_fetch_pipeline();
            return Ok(None);
        };
        self.advance_historical_fetch_sequence(next_child_header);
        Ok(Some((sequence, batch)))
    }

    fn materialize_historical_fetch_outcome(
        &mut self,
        fetch: HistoricalFetchOutcome,
    ) -> Result<Option<(u64, HistoricalFetchedBatch, Option<Header>)>> {
        let HistoricalFetchOutcome {
            sequence,
            header_batch,
            body_receipt_elapsed,
            outcome,
            ..
        } = fetch;
        let HistoricalHeaderBatch {
            header_peer,
            headers,
            hashes,
            required_block,
            header_elapsed,
            ..
        } = header_batch;

        match self.peers.complete_bodies_and_receipts_request(outcome) {
            Ok(Some(completion))
                if !completion.blocks.is_empty() && completion.blocks.len() <= headers.len() =>
            {
                let consumed_blocks = completion.blocks.len();
                let planned_return_blocks = completion.planned_return_blocks;
                let next_child_header = consumed_blocks
                    .checked_sub(1)
                    .and_then(|index| headers.get(index))
                    .cloned();
                let residual_header_batch = historical_residual_header_batch(
                    header_peer,
                    &headers,
                    &hashes,
                    consumed_blocks,
                    planned_return_blocks,
                );
                let residual_next_child_header = residual_header_batch
                    .as_ref()
                    .and_then(|batch| batch.headers.last().cloned());
                if consumed_blocks < planned_return_blocks {
                    tracing::debug!(
                        consumed_blocks,
                        planned_return_blocks,
                        residual_blocks = residual_header_batch
                            .as_ref()
                            .map(|batch| batch.headers.len())
                            .unwrap_or_default(),
                        "historical body/receipt pipeline completed partial prefix with residual gap"
                    );
                }
                let next_child_header = residual_next_child_header.or(next_child_header.clone());
                Ok(Some((
                    sequence,
                    HistoricalFetchedBatch {
                        header_peer,
                        headers,
                        hashes,
                        blocks: completion.blocks,
                        required_block,
                        header_elapsed,
                        body_receipt_elapsed,
                        residual_header_batch,
                    },
                    next_child_header,
                )))
            }
            Ok(Some(completion)) => {
                tracing::debug!(
                    headers = headers.len(),
                    blocks = completion.blocks.len(),
                    "historical body/receipt pipeline returned unusable response, resetting lookahead"
                );
                Ok(None)
            }
            Ok(None) => Ok(None),
            Err(error) => {
                tracing::debug!(
                    error = %error,
                    "historical body/receipt pipeline failed, resetting lookahead"
                );
                self.refresh_connectivity_state();
                Ok(None)
            }
        }
    }

    fn advance_historical_fetch_sequence(&mut self, next_child_header: Option<Header>) {
        self.historical_fetch_expected_sequence =
            self.historical_fetch_expected_sequence.saturating_add(1);
        self.historical_fetch_expected_child = next_child_header;
    }

    async fn prepare_historical_fetch_plan(
        &mut self,
        child_header: Header,
    ) -> Result<Option<HistoricalFetchPlan>> {
        let Some(header_batch) = self.fetch_historical_header_batch(child_header).await? else {
            return Ok(None);
        };

        if !HISTORICAL_USE_COMBINED_BODY_RECEIPT_PIPELINE {
            return Ok(None);
        }

        if header_batch
            .headers
            .iter()
            .all(historical_header_has_empty_body_and_receipts)
        {
            tracing::debug!(
                headers = header_batch.headers.len(),
                required_block = header_batch.required_block,
                "historical header batch has empty body and receipt roots"
            );
            return Ok(None);
        }

        let body_receipt_hashes = header_batch.hashes.clone();
        let body_receipt_gas_used = header_batch
            .headers
            .iter()
            .map(|header| header.gas_used())
            .collect();
        let body_receipt_plan = self
            .peers
            .prepare_bodies_and_receipts_request_for_hashes_and_gas(
                body_receipt_hashes,
                body_receipt_gas_used,
                self.historical_rows_per_block_ewma,
                header_batch.required_block,
                &[header_batch.header_peer],
            )
            .await?;

        Ok(body_receipt_plan.map(|body_receipt_plan| {
            let planned_next_child_header = body_receipt_plan
                .return_blocks()
                .min(header_batch.headers.len())
                .checked_sub(1)
                .and_then(|index| header_batch.headers.get(index))
                .cloned();
            HistoricalFetchPlan {
                header_batch,
                planned_next_child_header,
                body_receipt_plan,
            }
        }))
    }

    async fn fetch_historical_header_batch(
        &mut self,
        child_header: Header,
    ) -> Result<Option<HistoricalHeaderBatch>> {
        if child_header.number() == EXECUTION_HISTORY_TARGET_BLOCK {
            return Ok(None);
        }

        let page_limit = self
            .config
            .header_batch_size
            .clamp(1, HISTORICAL_BACKFILL_HEADER_BATCH_LIMIT);
        let target_count = self
            .historical_fetch_window_blocks()
            .min(child_header.number() - EXECUTION_HISTORY_TARGET_BLOCK);
        let header_started = std::time::Instant::now();
        let mut remaining = target_count;
        let mut page_child_header = child_header.clone();
        let mut header_peer = PeerId::ZERO;
        let mut headers = Vec::with_capacity(target_count as usize);
        let mut hashes = Vec::with_capacity(target_count as usize);
        let mut cumulative_header_gas = 0u128;

        while remaining > 0 {
            let request_count = remaining.min(page_limit);
            let (page_peer, page_headers) = match cancelable(
                &mut self.shutdown,
                self.peers.get_headers_reverse(
                    BlockHashOrNumber::Hash(page_child_header.parent_hash()),
                    request_count,
                ),
            )
            .await
            {
                Some(Ok(response)) => response,
                Some(Err(error)) => {
                    tracing::debug!(
                        error = %error,
                        child_block = page_child_header.number(),
                        requested_headers = request_count,
                        collected_headers = headers.len(),
                        "historical reverse header request failed"
                    );
                    self.refresh_connectivity_state();
                    if headers.is_empty() {
                        return Ok(None);
                    }
                    break;
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(None);
                }
            };

            if page_headers.is_empty() {
                self.refresh_connectivity_state();
                break;
            }

            let page_hashes = match validate_reverse_downloaded_headers_with_hashes(
                &page_child_header,
                &page_headers,
            ) {
                Ok(hashes) => hashes,
                Err(error) => {
                    tracing::warn!(
                        child_block = page_child_header.number(),
                        header_peer = %page_peer,
                        %error,
                        "historical reverse header validation failed"
                    );
                    self.peers.report_invalid_block_data(page_peer, "headers");
                    self.refresh_connectivity_state();
                    if headers.is_empty() {
                        return Ok(None);
                    }
                    break;
                }
            };

            if header_peer == PeerId::ZERO {
                header_peer = page_peer;
            }
            let previous_header_count = headers.len();
            for (offset, header) in page_headers.iter().enumerate() {
                if previous_header_count + offset >= target_count as usize {
                    break;
                }
                cumulative_header_gas =
                    cumulative_header_gas.saturating_add(u128::from(header.gas_used));
            }
            remaining = remaining.saturating_sub(page_headers.len() as u64);
            if let Some(next_child) = page_headers.last().cloned() {
                page_child_header = next_child;
            }
            hashes.extend(page_hashes);
            headers.extend(page_headers);

            if headers
                .last()
                .is_some_and(|header| header.number() == EXECUTION_HISTORY_TARGET_BLOCK)
                || historical_header_window_reached_gas_target(
                    headers.len(),
                    cumulative_header_gas,
                    target_count,
                )
            {
                break;
            }
        }
        let header_elapsed = header_started.elapsed();

        if headers.is_empty() {
            return Ok(None);
        }

        let required_block = headers
            .last()
            .map(|header| header.number())
            .unwrap_or(child_header.number().saturating_sub(1));

        Ok(Some(HistoricalHeaderBatch {
            child_header,
            header_peer,
            headers,
            hashes,
            required_block,
            header_elapsed,
        }))
    }

    async fn fetch_historical_combined_batch(
        &mut self,
        child_header: Header,
    ) -> Result<Option<(u64, HistoricalFetchedBatch, bool)>> {
        self.ensure_historical_fetch_pipeline(child_header.clone())
            .await?;
        self.drain_historical_fetch_outcomes();
        let prefetched = self
            .historical_fetch_completed
            .contains_key(&self.historical_fetch_expected_sequence);
        let Some(outcome) = self
            .wait_for_historical_fetch_outcome(&child_header)
            .await?
        else {
            return Ok(None);
        };
        Ok(self
            .complete_historical_fetch_outcome(outcome)?
            .map(|(sequence, batch)| (sequence, batch, prefetched)))
    }

    async fn ingest_historical_prepared_task(
        &mut self,
        task: HistoricalPrepareTask,
        prefetched: bool,
    ) -> Result<bool> {
        let sequence = task.sequence;
        self.ensure_historical_fetch_pipeline_for_child(task.next_child_header.clone())
            .await?;

        let mut handle = task.handle;
        let prepared = loop {
            tokio::select! {
                result = &mut handle => {
                    break result
                        .map_err(|error| eyre::eyre!("historical processing worker failed: {error}"))?;
                }
                outcome = self.historical_fetch_rx.recv() => {
                    match outcome {
                        Some(outcome) => {
                            self.store_historical_fetch_outcome(outcome);
                            self.spawn_ready_historical_prepare_tasks().await?;
                        }
                        None => break Err(eyre::eyre!("historical fetch channel closed")),
                    }
                }
                changed = self.shutdown.changed() => {
                    if changed.is_ok() && self.shutdown_requested() {
                        self.finish_shutdown()?;
                        return Ok(false);
                    }
                }
            }
        };

        self.ingest_historical_prepare_result(sequence, prepared, prefetched)
            .await
    }

    async fn ensure_historical_fetch_pipeline_for_child(
        &mut self,
        next_child_header: Option<Header>,
    ) -> Result<()> {
        if let Some(next_child_header) = next_child_header
            && next_child_header.number() > EXECUTION_HISTORY_TARGET_BLOCK
            && self.peers.peer_count() > 0
        {
            self.ensure_historical_fetch_pipeline(next_child_header)
                .await?;
        }
        Ok(())
    }

    async fn ingest_historical_prepare_result(
        &mut self,
        sequence: u64,
        prepared: HistoricalPrepareResult,
        prefetched: bool,
    ) -> Result<bool> {
        let batch_started = std::time::Instant::now();
        let overlap_started = std::time::Instant::now();
        self.spawn_ready_historical_prepare_tasks().await?;
        let queued_next_fetches = self.pending_historical_fetch_count();
        let prepare_wait_started = std::time::Instant::now();
        let prepared = prepared?;
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(failure) => {
                tracing::warn!(
                    block_number = failure.block_number,
                    block_hash = %failure.block_hash,
                    peer = %failure.peer,
                    error = %failure.message,
                    "historical block validation failed"
                );
                self.peers
                    .report_invalid_block_data(failure.peer, failure.response_kind);
                self.reset_historical_fetch_pipeline();
                self.refresh_historical_status().await;
                return Ok(false);
            }
        };
        let mut written =
            write_prepared_historical_batch(prepared, Arc::clone(&self.storage)).await?;
        let next_child_header = written.next_child_header.clone();
        written.prepare_wait_elapsed = prepare_wait_started.elapsed();
        self.historical_prepare_expected_sequence = sequence.saturating_add(1);
        let overlap_elapsed = overlap_started.elapsed();
        let mut newly_serving_peers = HashSet::new();
        for peer_id in &written.peer_notes {
            self.note_serving_peer(*peer_id, &mut newly_serving_peers);
        }
        let requested_headers = written.requested_headers;
        let header_elapsed = written.header_elapsed;
        let body_receipt_elapsed = written.body_receipt_elapsed;
        let processing_elapsed = written.processing_elapsed;
        let validation_elapsed = written.validation_elapsed;
        let prepare_wait_elapsed = written.prepare_wait_elapsed;
        let lowest_block = written.lowest_block;
        let highest_block = written.highest_block;
        let block_count = written.block_count;
        let extraction_elapsed = written.outcome.extraction_elapsed;
        let write_elapsed = written.outcome.write_elapsed;
        let residual_header_batch = written.residual_header_batch.take();
        let log_count = self.record_historical_ingest_outcome(written.outcome);
        self.historical_rows_per_block_ewma = update_historical_density_ewma(
            self.historical_rows_per_block_ewma,
            log_count,
            block_count,
        );
        self.maybe_trim_historical_allocator();
        self.refresh_historical_status().await;
        let residual_blocks = residual_header_batch
            .as_ref()
            .map(|batch| batch.headers.len())
            .unwrap_or_default();
        if let Some(residual_header_batch) = residual_header_batch
            && !self
                .ingest_historical_residual_header_batch(residual_header_batch)
                .await?
        {
            self.reset_historical_fetch_pipeline();
            self.refresh_historical_status().await;
            return Ok(true);
        }
        self.ensure_historical_fetch_pipeline_for_child(next_child_header)
            .await?;

        tracing::debug!(
            requested_headers,
            sequence,
            lowest_block,
            highest_block,
            blocks = block_count,
            logs = log_count,
            residual_blocks,
            prefetched,
            prefetched_next = queued_next_fetches > 0,
            queued_next_fetches,
            active_fetches = self.active_historical_fetch_count(),
            buffered_fetches = self.pending_historical_fetch_count(),
            prepared_fetches = self.pending_historical_prepare_count(),
            rows_per_block_ewma = self.historical_rows_per_block_ewma,
            header_ms = header_elapsed.as_millis(),
            body_receipt_ms = body_receipt_elapsed.as_millis(),
            validation_ms = validation_elapsed.as_millis(),
            prepare_wait_ms = prepare_wait_elapsed.as_millis(),
            extraction_ms = extraction_elapsed.as_millis(),
            write_ms = write_elapsed.as_millis(),
            process_ms = processing_elapsed.as_millis(),
            overlap_ms = overlap_elapsed.as_millis(),
            total_ms = batch_started.elapsed().as_millis(),
            "historical body/receipt pipeline batch completed"
        );
        tracing::trace!(
            lowest_block,
            highest_block,
            blocks = block_count,
            logs = log_count,
            "historical block batch verified and ingested"
        );
        Ok(true)
    }

    async fn ingest_historical_residual_header_batch(
        &mut self,
        header_batch: HistoricalHeaderBatch,
    ) -> Result<bool> {
        let HistoricalHeaderBatch {
            child_header,
            header_peer,
            headers,
            hashes,
            required_block,
            ..
        } = header_batch;
        if headers.is_empty() {
            return Ok(true);
        }
        let residual_started = std::time::Instant::now();
        if headers.len() != hashes.len() {
            tracing::debug!(
                headers = headers.len(),
                hashes = hashes.len(),
                "historical residual gap has mismatched header/hash counts"
            );
            return Ok(false);
        }
        match validate_reverse_downloaded_headers_with_hashes(&child_header, &headers) {
            Ok(validated_hashes) if validated_hashes == hashes => {}
            Ok(_) => {
                tracing::warn!(
                    child_block = child_header.number(),
                    header_peer = %header_peer,
                    "historical residual gap hash validation failed"
                );
                self.peers.report_invalid_block_data(header_peer, "headers");
                return Ok(false);
            }
            Err(error) => {
                tracing::warn!(
                    child_block = child_header.number(),
                    header_peer = %header_peer,
                    %error,
                    "historical residual gap header validation failed"
                );
                self.peers.report_invalid_block_data(header_peer, "headers");
                return Ok(false);
            }
        }

        let mut newly_serving_peers = HashSet::new();
        if headers
            .iter()
            .all(historical_header_has_empty_body_and_receipts)
        {
            self.ingest_empty_historical_header_chunk(
                header_peer,
                &headers,
                &mut newly_serving_peers,
            )
            .await?;
            self.refresh_historical_status().await;
            return Ok(true);
        }

        let mut remaining_headers = headers;
        let mut remaining_hashes = hashes;
        while !remaining_headers.is_empty() {
            if remaining_headers
                .iter()
                .all(historical_header_has_empty_body_and_receipts)
            {
                self.ingest_empty_historical_header_chunk(
                    header_peer,
                    &remaining_headers,
                    &mut newly_serving_peers,
                )
                .await?;
                self.refresh_historical_status().await;
                break;
            }

            let body_receipt_started = std::time::Instant::now();
            let body_receipt_gas_used = remaining_headers
                .iter()
                .map(|header| header.gas_used())
                .collect();
            let blocks = match self
                .peers
                .prepare_bodies_and_receipts_request_for_hashes_and_gas(
                    remaining_hashes.clone(),
                    body_receipt_gas_used,
                    self.historical_rows_per_block_ewma,
                    required_block,
                    &[header_peer],
                )
                .await?
            {
                Some(plan) => {
                    let outcome = plan.execute().await;
                    match self
                        .peers
                        .complete_residual_bodies_and_receipts_request(outcome)
                    {
                        Ok(Some(completion))
                            if !completion.blocks.is_empty()
                                && completion.blocks.len() <= remaining_headers.len() =>
                        {
                            completion.blocks
                        }
                        Ok(Some(completion)) => {
                            tracing::debug!(
                                headers = remaining_headers.len(),
                                blocks = completion.blocks.len(),
                                "historical residual parallel body/receipt response was unusable"
                            );
                            return Ok(false);
                        }
                        Ok(None) => return Ok(false),
                        Err(error) => {
                            tracing::debug!(
                                error = %error,
                                "historical residual parallel body/receipt request failed"
                            );
                            self.refresh_connectivity_state();
                            return Ok(false);
                        }
                    }
                }
                None => return Ok(false),
            };
            let body_receipt_elapsed = body_receipt_started.elapsed();
            let block_count = blocks.len();
            if block_count == 0 || block_count > remaining_headers.len() {
                return Ok(false);
            }
            let chunk_headers = remaining_headers[..block_count].to_vec();
            let chunk_hashes = remaining_hashes[..block_count].to_vec();
            let remaining_after_chunk = remaining_headers.len().saturating_sub(block_count);
            if remaining_after_chunk > 0 {
                tracing::debug!(
                    headers = remaining_headers.len(),
                    blocks = block_count,
                    remaining_blocks = remaining_after_chunk,
                    "historical residual parallel body/receipt response made partial progress"
                );
            }

            let (
                extracted,
                mut peer_notes,
                lowest_block,
                highest_block,
                block_count,
                validation_elapsed,
            ) = match validate_and_extract_historical_blocks_streaming(
                &chunk_headers,
                &chunk_hashes,
                blocks,
            )
            .await?
            {
                Ok(processed) => processed,
                Err(failure) => {
                    tracing::warn!(
                        block_number = failure.block_number,
                        block_hash = %failure.block_hash,
                        peer = %failure.peer,
                        error = %failure.message,
                        "historical residual block validation failed"
                    );
                    self.peers
                        .report_invalid_block_data(failure.peer, failure.response_kind);
                    return Ok(false);
                }
            };

            peer_notes.push(header_peer);
            for peer_id in peer_notes {
                self.note_serving_peer(peer_id, &mut newly_serving_peers);
            }

            let write_started = std::time::Instant::now();
            let outcome = super::ingest::write_extracted_historical_batch(
                Arc::clone(&self.storage),
                extracted,
            )
            .await?;
            let extraction_elapsed = outcome.extraction_elapsed;
            let write_elapsed = outcome.write_elapsed;
            let log_count = self.record_historical_ingest_outcome(outcome);
            self.historical_rows_per_block_ewma = update_historical_density_ewma(
                self.historical_rows_per_block_ewma,
                log_count,
                block_count,
            );
            self.refresh_historical_status().await;

            tracing::debug!(
                lowest_block,
                highest_block,
                blocks = block_count,
                logs = log_count,
                remaining_blocks = remaining_after_chunk,
                body_receipt_ms = body_receipt_elapsed.as_millis(),
                validation_ms = validation_elapsed.as_millis(),
                extraction_ms = extraction_elapsed.as_millis(),
                write_ms = write_elapsed.as_millis(),
                write_total_ms = write_started.elapsed().as_millis(),
                total_ms = residual_started.elapsed().as_millis(),
                "historical residual body/receipt gap verified and ingested"
            );

            remaining_headers.drain(..block_count);
            remaining_hashes.drain(..block_count);
        }

        Ok(true)
    }

    fn maybe_trim_historical_allocator(&mut self) {
        let available_memory_bytes = historical_available_memory_bytes();
        let now = std::time::Instant::now();
        if !historical_allocator_trim_is_due(
            self.last_historical_allocator_trim,
            now,
            available_memory_bytes,
        ) {
            return;
        }

        self.last_historical_allocator_trim = Some(now);
        let trimmed = trim_process_allocator();
        tracing::debug!(
            available_memory_bytes,
            trimmed,
            "requested allocator trim after historical batch under memory pressure"
        );
    }

    async fn ingest_historical_backfill_batch_sequential(
        &mut self,
        child_header: Header,
    ) -> Result<bool> {
        let request_count = self
            .config
            .header_batch_size
            .min(HISTORICAL_BACKFILL_HEADER_BATCH_LIMIT)
            .min(child_header.number() - EXECUTION_HISTORY_TARGET_BLOCK);
        let (header_peer, headers) = match cancelable(
            &mut self.shutdown,
            self.peers.get_headers_reverse(
                BlockHashOrNumber::Hash(child_header.parent_hash()),
                request_count,
            ),
        )
        .await
        {
            Some(Ok(response)) => response,
            Some(Err(error)) => {
                tracing::debug!(
                    error = %error,
                    child_block = child_header.number(),
                    "historical reverse header request failed"
                );
                self.refresh_connectivity_state();
                return Ok(false);
            }
            None => {
                self.finish_shutdown()?;
                return Ok(false);
            }
        };

        if headers.is_empty() {
            self.refresh_connectivity_state();
            return Ok(false);
        }

        let hashes = match validate_reverse_downloaded_headers_with_hashes(&child_header, &headers)
        {
            Ok(hashes) => hashes,
            Err(error) => {
                tracing::warn!(
                    child_block = child_header.number(),
                    header_peer = %header_peer,
                    %error,
                    "historical reverse header validation failed"
                );
                self.peers.report_invalid_block_data(header_peer, "headers");
                self.refresh_connectivity_state();
                return Ok(false);
            }
        };

        let mut newly_serving_peers = HashSet::new();

        let mut progressed = false;
        let sequential_fetch_batch_size = self
            .config
            .fetch_batch_size
            .min(HISTORICAL_SEQUENTIAL_FETCH_BATCH_LIMIT);
        for (chunk_headers, chunk_hashes) in headers
            .chunks(sequential_fetch_batch_size)
            .zip(hashes.chunks(sequential_fetch_batch_size))
        {
            let chunk_headers = chunk_headers.to_vec();
            let chunk_hashes = chunk_hashes.to_vec();
            let required_block = chunk_headers
                .first()
                .map(|header| header.number())
                .unwrap_or(child_header.number().saturating_sub(1));

            if chunk_headers
                .iter()
                .all(historical_header_has_empty_body_and_receipts)
            {
                self.ingest_empty_historical_header_chunk(
                    header_peer,
                    &chunk_headers,
                    &mut newly_serving_peers,
                )
                .await?;
                progressed = true;
                continue;
            }

            let bodies = match cancelable(
                &mut self.shutdown,
                self.peers.get_bodies_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &[header_peer],
                ),
            )
            .await
            {
                Some(Ok(bodies)) if bodies.len() == chunk_headers.len() => bodies,
                Some(Ok(bodies)) => {
                    tracing::debug!(
                        headers = chunk_headers.len(),
                        bodies = bodies.len(),
                        "historical body response count mismatch"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::debug!(error = %error, "historical body request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            let expected_receipt_counts: Vec<usize> = bodies
                .iter()
                .map(|(_peer_id, body)| body.transaction_count())
                .collect();
            let receipt_peer_preference = preferred_body_peers(&bodies, header_peer);

            let (receipt_peer, receipts) = match cancelable(
                &mut self.shutdown,
                self.peers.get_receipts_matching_counts_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &expected_receipt_counts,
                    &receipt_peer_preference,
                ),
            )
            .await
            {
                Some(Ok((peer_id, receipts))) if receipts.len() == chunk_headers.len() => {
                    (peer_id, receipts)
                }
                Some(Ok((_peer_id, receipts))) => {
                    tracing::debug!(
                        headers = chunk_headers.len(),
                        receipts = receipts.len(),
                        "historical receipt response count mismatch"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::debug!(error = %error, "historical receipt request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            let blocks: Vec<SourcedBodyReceipts> = bodies
                .into_iter()
                .zip(receipts)
                .map(|((body_peer, body), receipts)| ((body_peer, body), (receipt_peer, receipts)))
                .collect();
            let validated =
                match validate_historical_blocks_parallel(&chunk_headers, &chunk_hashes, blocks)
                    .await?
                {
                    Ok(validated) => validated,
                    Err(failure) => {
                        tracing::warn!(
                            block_number = failure.block_number,
                            block_hash = %failure.block_hash,
                            peer = %failure.peer,
                            error = %failure.message,
                            "historical block validation failed"
                        );
                        self.peers
                            .report_invalid_block_data(failure.peer, failure.response_kind);
                        return Ok(progressed);
                    }
                };

            for block in &validated {
                self.note_serving_peer(header_peer, &mut newly_serving_peers);
                self.note_serving_peer(block.body_peer, &mut newly_serving_peers);
                self.note_serving_peer(block.receipt_peer, &mut newly_serving_peers);
            }

            let lowest_block = validated
                .iter()
                .map(|block| block.header.number())
                .min()
                .unwrap_or(required_block);
            let highest_block = validated
                .iter()
                .map(|block| block.header.number())
                .max()
                .unwrap_or(required_block);
            let block_count = validated.len();
            let outcome = super::ingest::write_validated_historical_blocks(
                Arc::clone(&self.storage),
                validated,
            )
            .await?;
            let log_count = self.record_historical_ingest_outcome(outcome);
            self.historical_rows_per_block_ewma = update_historical_density_ewma(
                self.historical_rows_per_block_ewma,
                log_count,
                block_count,
            );
            progressed = true;

            tracing::trace!(
                lowest_block,
                highest_block,
                blocks = block_count,
                logs = log_count,
                "historical block batch verified and ingested"
            );
        }

        self.refresh_historical_status().await;
        Ok(progressed)
    }

    async fn ingest_empty_historical_header_chunk(
        &mut self,
        header_peer: PeerId,
        headers: &[Header],
        newly_serving_peers: &mut HashSet<PeerId>,
    ) -> Result<()> {
        let Some(lowest_header) = headers.iter().min_by_key(|header| header.number()) else {
            return Ok(());
        };
        let highest_block = headers
            .iter()
            .map(|header| header.number())
            .max()
            .unwrap_or_else(|| lowest_header.number());
        let block_count = headers.len() as u64;
        let floor = super::ingest::execution_marker_from_header(lowest_header);
        let anchor = {
            let mut storage = self.storage.write().await;
            storage
                .record_historical_floor(lowest_header)
                .map_err(|error| eyre::eyre!("historical metadata error: {error}"))?;
            storage.historical_anchor()
        };
        let outcome = HistoricalIngestOutcome {
            block_count,
            row_count: 0,
            floor,
            anchor,
            extraction_elapsed: Duration::ZERO,
            write_elapsed: Duration::ZERO,
        };
        self.record_historical_ingest_outcome(outcome);
        self.historical_rows_per_block_ewma =
            update_historical_density_ewma(self.historical_rows_per_block_ewma, 0, headers.len());
        self.note_serving_peer(header_peer, newly_serving_peers);

        tracing::debug!(
            lowest_block = lowest_header.number(),
            highest_block,
            blocks = headers.len(),
            "historical empty-root header batch ingested without body/receipt requests"
        );
        Ok(())
    }

    async fn reconcile_consensus_reorg(&mut self) -> Result<bool> {
        let Some(consensus) = self.consensus.as_ref() else {
            return Ok(false);
        };

        let recent_headers = self.head_tracker.snapshot();
        let Some(reorg) = locate_consensus_reorg(consensus, &recent_headers)? else {
            return Ok(false);
        };

        self.peers.remove_cached_blocks(&reorg.reverted_hashes);

        let reverted_rows = {
            let mut storage = self.storage.write().await;
            let mut total_reverted = 0u64;
            for hash in &reorg.reverted_hashes {
                total_reverted += storage
                    .mark_non_canonical(*hash)
                    .map_err(|error| eyre::eyre!("consensus reorg error: {error}"))?;
            }
            storage
                .rewind_canonical_state(&reorg.retained_headers, reorg.indexed_head)
                .map_err(|error| eyre::eyre!("consensus reorg state rewind error: {error}"))?;
            total_reverted
        };

        self.head_tracker.restore(reorg.retained_headers.clone());
        self.last_validated_header = reorg.retained_headers.last().cloned();
        self.progress
            .rewind_to(reorg.indexed_head.map_or(0, |anchor| anchor.block_number));
        self.refresh_consensus_status().await;

        tracing::warn!(
            reverted_blocks = reorg.reverted_hashes.len(),
            reverted_rows,
            rewind_to = reorg.indexed_head.map(|anchor| anchor.block_number),
            "rewound indexed canonical state to match the latest consensus anchors"
        );

        Ok(true)
    }

    pub(super) async fn refresh_consensus_status(&self) {
        let Some(consensus) = self.consensus.as_ref() else {
            return;
        };

        let checkpoint = consensus.checkpoint();
        let mut anchors = consensus.chain_anchors();
        let indexed_head = {
            let storage = self.storage.read().await;
            storage.chain_anchors().indexed_head
        };
        anchors.indexed_head = indexed_head;
        self.progress.update_consensus_state(checkpoint, &anchors);
    }

    async fn refresh_historical_status(&self) {
        let (floor, anchor) = {
            let storage = self.storage.read().await;
            (storage.historical_floor(), storage.historical_anchor())
        };
        self.progress
            .initialize_historical_state(floor, anchor, EXECUTION_HISTORY_TARGET_BLOCK);
    }
}

fn can_bootstrap_from_anchor(current: u64, anchor_block: u64) -> bool {
    current == 0 && anchor_block > 0
}

fn locate_consensus_reorg(
    consensus: &ConsensusStore,
    recent_headers: &[Header],
) -> Result<Option<ConsensusReorg>> {
    let Some(tip) = recent_headers.last() else {
        return Ok(None);
    };

    let tip_hash = tip.hash_slow();
    if consensus
        .anchor_at(tip.number())
        .is_some_and(|anchor| anchor.block_hash == tip_hash)
    {
        return Ok(None);
    }

    for index in (0..recent_headers.len()).rev() {
        let header = &recent_headers[index];
        let header_hash = header.hash_slow();
        if let Some(anchor) = consensus.anchor_at(header.number())
            && anchor.block_hash == header_hash
        {
            return Ok(Some(ConsensusReorg {
                retained_headers: recent_headers[..=index].to_vec(),
                indexed_head: Some(anchor),
                reverted_hashes: recent_headers[index + 1..]
                    .iter()
                    .map(|header| header.hash_slow())
                    .collect(),
            }));
        }
    }

    let first_block = recent_headers
        .first()
        .map(Header::number)
        .unwrap_or_default();
    let last_block = recent_headers
        .last()
        .map(Header::number)
        .unwrap_or_default();
    Err(eyre::eyre!(
        "consensus anchor reorg exceeded the persisted recent-header window ({first_block}..{last_block}); a fresh checkpointed resync is required"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use logex_cl::AnchorRecord;
    use tempfile::TempDir;

    fn header(number: u64, parent_hash: B256, marker: u8) -> Header {
        let mut header = Header {
            number,
            parent_hash,
            gas_limit: 30_000_000,
            timestamp: 1_700_000_000 + number,
            ..Default::default()
        };
        header.extra_data = vec![marker].into();
        header
    }

    fn anchor_for(header: &Header, beacon_slot: u64) -> AnchorRecord {
        AnchorRecord {
            anchor: ExecutionAnchor {
                beacon_root: B256::repeat_byte(beacon_slot as u8),
                beacon_slot,
                block_number: header.number(),
                block_hash: header.hash_slow(),
                receipts_root: header.receipts_root(),
            },
            finalized: false,
            parent_beacon_root: None,
        }
    }

    #[test]
    fn empty_body_receipt_detection_requires_empty_commitments() {
        let mut empty = header(100, B256::ZERO, 0x01);
        empty.transactions_root = EMPTY_ROOT_HASH;
        empty.receipts_root = EMPTY_ROOT_HASH;
        empty.ommers_hash = EMPTY_OMMER_ROOT_HASH;
        empty.withdrawals_root = None;
        assert!(historical_header_has_empty_body_and_receipts(&empty));

        let mut with_receipts = empty.clone();
        with_receipts.receipts_root = B256::repeat_byte(0x22);
        assert!(!historical_header_has_empty_body_and_receipts(
            &with_receipts
        ));

        let mut with_withdrawals = empty;
        with_withdrawals.withdrawals_root = Some(B256::repeat_byte(0x33));
        assert!(!historical_header_has_empty_body_and_receipts(
            &with_withdrawals
        ));
    }

    #[test]
    fn contiguous_anchor_batch_allows_fresh_checkpoint_jump() {
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let third = header(102, second.hash_slow(), 0x03);
        let anchors = vec![
            anchor_for(&first, 1),
            anchor_for(&second, 2),
            anchor_for(&third, 3),
        ];

        let batch = contiguous_anchor_batch(0, true, &anchors, 2);

        assert_eq!(
            batch
                .iter()
                .map(|anchor| anchor.block_number)
                .collect::<Vec<_>>(),
            vec![100, 101]
        );
    }

    #[test]
    fn contiguous_anchor_batch_stops_at_first_gap() {
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let gap = header(103, second.hash_slow(), 0x03);
        let anchors = vec![
            anchor_for(&first, 1),
            anchor_for(&second, 2),
            anchor_for(&gap, 3),
        ];

        let batch = contiguous_anchor_batch(99, false, &anchors, 16);

        assert_eq!(
            batch
                .iter()
                .map(|anchor| anchor.block_number)
                .collect::<Vec<_>>(),
            vec![100, 101]
        );
    }

    #[test]
    fn contiguous_anchor_batch_rejects_noncontiguous_start_without_bootstrap() {
        let first = header(100, B256::ZERO, 0x01);
        let anchors = vec![anchor_for(&first, 1)];

        assert!(contiguous_anchor_batch(98, false, &anchors, 16).is_empty());
    }

    #[test]
    fn historical_header_gas_window_requires_minimum_dense_prefix() {
        let target_count = HISTORICAL_HEADER_GAS_WINDOW_MIN_BLOCKS as u64;
        let gas_target = HISTORICAL_HEADER_GAS_PER_BLOCK_TARGET * target_count as u128;
        assert!(!historical_header_window_reached_gas_target(
            HISTORICAL_HEADER_GAS_WINDOW_MIN_BLOCKS - 1,
            gas_target,
            target_count,
        ));
        assert!(!historical_header_window_reached_gas_target(
            HISTORICAL_HEADER_GAS_WINDOW_MIN_BLOCKS,
            gas_target - 1,
            target_count,
        ));
        assert!(historical_header_window_reached_gas_target(
            HISTORICAL_HEADER_GAS_WINDOW_MIN_BLOCKS,
            gas_target,
            target_count,
        ));
    }

    #[test]
    fn historical_residual_header_batch_covers_only_missing_partial_prefix_gap() {
        let h100 = header(100, B256::ZERO, 0x01);
        let h101 = header(101, h100.hash_slow(), 0x02);
        let h102 = header(102, h101.hash_slow(), 0x03);
        let h103 = header(103, h102.hash_slow(), 0x04);
        let h104 = header(104, h103.hash_slow(), 0x05);
        let headers = vec![
            h104.clone(),
            h103.clone(),
            h102.clone(),
            h101.clone(),
            h100.clone(),
        ];
        let hashes = headers.iter().map(Header::hash_slow).collect::<Vec<_>>();

        let residual =
            historical_residual_header_batch(PeerId::ZERO, &headers, &hashes, 2, 5).unwrap();

        assert_eq!(residual.child_header.number(), 103);
        assert_eq!(
            residual
                .headers
                .iter()
                .map(Header::number)
                .collect::<Vec<_>>(),
            vec![102, 101, 100]
        );
        assert_eq!(residual.hashes, hashes[2..5]);
        assert_eq!(residual.required_block, 100);
        assert!(historical_residual_header_batch(PeerId::ZERO, &headers, &hashes, 5, 5).is_none());
    }

    #[test]
    fn historical_batch_next_child_header_continues_below_residual_gap() {
        let h100 = header(100, B256::ZERO, 0x01);
        let h101 = header(101, h100.hash_slow(), 0x02);
        let h102 = header(102, h101.hash_slow(), 0x03);
        let h103 = header(103, h102.hash_slow(), 0x04);
        let h104 = header(104, h103.hash_slow(), 0x05);
        let headers = vec![
            h104.clone(),
            h103.clone(),
            h102.clone(),
            h101.clone(),
            h100.clone(),
        ];
        let hashes = headers.iter().map(Header::hash_slow).collect::<Vec<_>>();
        let residual_header_batch =
            historical_residual_header_batch(PeerId::ZERO, &headers, &hashes, 2, 5);

        let batch = HistoricalFetchedBatch {
            header_peer: PeerId::ZERO,
            headers,
            hashes,
            blocks: Vec::new(),
            required_block: 100,
            header_elapsed: Duration::ZERO,
            body_receipt_elapsed: Duration::ZERO,
            residual_header_batch,
        };

        assert_eq!(
            historical_batch_next_child_header(&batch).map(|header| header.number()),
            Some(100)
        );
    }

    #[test]
    fn historical_fetch_window_respects_memory_tier() {
        let small_memory = Some(8 * BYTES_PER_GIB);
        let medium_memory = Some(HISTORICAL_MEDIUM_PIPELINE_MIN_TOTAL_MEMORY_BYTES);
        let high_pipeline_memory = Some(HISTORICAL_HIGH_PIPELINE_MIN_TOTAL_MEMORY_BYTES);
        let deep_memory = Some(HISTORICAL_DEEP_WINDOW_MIN_TOTAL_MEMORY_BYTES);
        let wide_memory = Some(HISTORICAL_WIDE_WINDOW_MIN_TOTAL_MEMORY_BYTES);

        assert_eq!(
            historical_fetch_window_blocks_for_serving_peers(40, small_memory, None),
            HISTORICAL_MEDIUM_PEER_FETCH_WINDOW_BLOCKS
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(40, small_memory, None),
            HISTORICAL_LOW_PEER_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(40, medium_memory, None),
            HISTORICAL_MEDIUM_PEER_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(
                HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS - 1,
                high_pipeline_memory,
                None,
            ),
            HISTORICAL_LOW_PEER_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(
                HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS,
                high_pipeline_memory,
                None,
            ),
            HISTORICAL_HIGH_MEMORY_MEDIUM_PEER_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(40, high_pipeline_memory, None),
            HISTORICAL_HIGH_MEMORY_MEDIUM_PEER_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_window_blocks_for_serving_peers(40, high_pipeline_memory, None),
            HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(
                HISTORICAL_MEDIUM_LOOKAHEAD_MIN_SERVING_PEERS - 1,
                high_pipeline_memory,
                None,
            ),
            HISTORICAL_LOW_PEER_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_window_blocks_for_serving_peers(
                HISTORICAL_MEDIUM_LOOKAHEAD_MIN_SERVING_PEERS,
                high_pipeline_memory,
                None,
            ),
            HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS
        );
        assert_eq!(
            historical_fetch_window_blocks_for_serving_peers(48, deep_memory, None),
            HISTORICAL_DEEP_FETCH_WINDOW_BLOCKS
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(48, deep_memory, None),
            HISTORICAL_DEEP_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_window_blocks_for_serving_peers(80, wide_memory, None),
            HISTORICAL_WIDE_FETCH_WINDOW_BLOCKS
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(80, wide_memory, None),
            HISTORICAL_WIDE_FETCH_PIPELINE_DEPTH
        );
    }

    #[test]
    fn historical_fetch_window_backs_off_under_memory_pressure() {
        let total_memory = Some(16 * BYTES_PER_GIB);
        let low_available = Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1);
        let critical_available = Some(HISTORICAL_CRITICAL_AVAILABLE_MEMORY_BYTES - 1);

        assert_eq!(
            historical_fetch_window_blocks_for_serving_peers(40, total_memory, low_available),
            HISTORICAL_LOW_PEER_FETCH_WINDOW_BLOCKS
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(40, total_memory, low_available),
            HISTORICAL_LOW_PEER_FETCH_PIPELINE_DEPTH
        );
        assert_eq!(
            historical_fetch_pipeline_depth_for_serving_peers(40, total_memory, critical_available,),
            1
        );
    }

    #[test]
    fn historical_density_caps_dense_fetch_lookahead() {
        assert_eq!(historical_density_fetch_window_cap(None), None);
        assert_eq!(historical_density_fetch_pipeline_depth_cap(None), None);
        assert_eq!(
            historical_density_fetch_window_cap(Some(100.0)),
            Some(HISTORICAL_HIGH_MEMORY_FETCH_WINDOW_BLOCKS)
        );
        assert_eq!(
            historical_density_fetch_pipeline_depth_cap(Some(100.0)),
            None
        );
        assert_eq!(
            historical_density_fetch_window_cap(Some(200.0)),
            Some(3_750)
        );
        assert_eq!(
            historical_density_fetch_window_cap(Some(400.0)),
            Some(HISTORICAL_DENSE_FETCH_WINDOW_BLOCKS)
        );
        assert_eq!(
            historical_density_fetch_pipeline_depth_cap(Some(400.0)),
            Some(HISTORICAL_DENSE_FETCH_PIPELINE_DEPTH)
        );
        assert_eq!(
            historical_density_fetch_window_cap(Some(HISTORICAL_DENSE_ROWS_PER_BLOCK)),
            Some(HISTORICAL_DENSE_FETCH_WINDOW_BLOCKS)
        );
        assert_eq!(
            historical_density_fetch_pipeline_depth_cap(Some(HISTORICAL_DENSE_ROWS_PER_BLOCK)),
            Some(HISTORICAL_DENSE_FETCH_PIPELINE_DEPTH)
        );
        assert_eq!(
            historical_density_fetch_window_cap(Some(900.0)),
            Some(HISTORICAL_DENSE_FETCH_WINDOW_BLOCKS)
        );
        assert_eq!(
            historical_density_fetch_window_cap(Some(HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK)),
            Some(HISTORICAL_VERY_DENSE_FETCH_WINDOW_BLOCKS)
        );
        assert_eq!(
            historical_density_fetch_pipeline_depth_cap(Some(HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK)),
            Some(HISTORICAL_VERY_DENSE_FETCH_PIPELINE_DEPTH)
        );
    }

    #[test]
    fn historical_sparse_density_can_expand_fetch_window() {
        let high_memory = Some(HISTORICAL_SPARSE_PIPELINE_MIN_TOTAL_MEMORY_BYTES);
        let healthy_available = Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES);
        let low_available = Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1);

        assert_eq!(
            historical_density_fetch_window_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS,
                high_memory,
                healthy_available,
                Some(50.0),
            ),
            Some(10_000)
        );
        assert_eq!(
            historical_density_fetch_window_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS,
                high_memory,
                healthy_available,
                Some(100.0),
            ),
            Some(7_500)
        );
        assert_eq!(
            historical_density_fetch_window_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS - 1,
                high_memory,
                healthy_available,
                Some(50.0),
            ),
            None
        );
        assert_eq!(
            historical_density_fetch_window_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS,
                high_memory,
                low_available,
                Some(50.0),
            ),
            None
        );
        assert_eq!(
            historical_density_fetch_window_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS,
                high_memory,
                healthy_available,
                Some(HISTORICAL_SPARSE_ROWS_PER_BLOCK + 1.0),
            ),
            None
        );
    }

    #[test]
    fn historical_dense_density_can_boost_fetch_pipeline_depth() {
        let high_memory = Some(HISTORICAL_HIGH_PIPELINE_MIN_TOTAL_MEMORY_BYTES);
        let healthy_available = Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES);
        let low_available = Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1);

        assert_eq!(
            historical_dense_fetch_pipeline_depth_boost(
                HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS,
                high_memory,
                healthy_available,
                Some(HISTORICAL_DENSE_ROWS_PER_BLOCK),
            ),
            Some(HISTORICAL_DENSE_FETCH_PIPELINE_DEPTH)
        );
        assert_eq!(
            historical_dense_fetch_pipeline_depth_boost(
                HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS - 1,
                high_memory,
                healthy_available,
                Some(HISTORICAL_DENSE_ROWS_PER_BLOCK),
            ),
            None
        );
        assert_eq!(
            historical_dense_fetch_pipeline_depth_boost(
                HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS,
                high_memory,
                low_available,
                Some(HISTORICAL_DENSE_ROWS_PER_BLOCK),
            ),
            None
        );
        assert_eq!(
            historical_dense_fetch_pipeline_depth_boost(
                HISTORICAL_HIGH_PIPELINE_MIN_SERVING_PEERS,
                high_memory,
                healthy_available,
                Some(HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK),
            ),
            None
        );
    }

    #[test]
    fn historical_sparse_density_can_boost_fetch_lookahead() {
        let high_memory = Some(HISTORICAL_SPARSE_PIPELINE_MIN_TOTAL_MEMORY_BYTES);
        let healthy_available = Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES);
        let low_available = Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1);

        assert_eq!(
            historical_sparse_fetch_pipeline_depth_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS,
                high_memory,
                healthy_available,
                Some(HISTORICAL_SPARSE_ROWS_PER_BLOCK),
            ),
            Some(HISTORICAL_SPARSE_FETCH_PIPELINE_DEPTH)
        );
        assert_eq!(
            historical_sparse_fetch_pipeline_depth_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS - 1,
                high_memory,
                healthy_available,
                Some(HISTORICAL_SPARSE_ROWS_PER_BLOCK),
            ),
            None
        );
        assert_eq!(
            historical_sparse_fetch_pipeline_depth_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS,
                high_memory,
                low_available,
                Some(HISTORICAL_SPARSE_ROWS_PER_BLOCK),
            ),
            None
        );
        assert_eq!(
            historical_sparse_fetch_pipeline_depth_boost(
                HISTORICAL_SPARSE_LOOKAHEAD_MIN_SERVING_PEERS,
                high_memory,
                healthy_available,
                Some(HISTORICAL_SPARSE_ROWS_PER_BLOCK + 1.0),
            ),
            None
        );
    }

    #[test]
    fn historical_fetch_buffer_keeps_downloader_ahead_without_overbuffering_dense_ranges() {
        assert_eq!(historical_fetch_buffer_depth(1, None, None), 1);
        assert_eq!(
            historical_fetch_buffer_depth(8, Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1), None),
            8
        );
        assert_eq!(
            historical_fetch_buffer_depth(8, None, Some(HISTORICAL_DENSE_ROWS_PER_BLOCK)),
            12
        );
        assert_eq!(
            historical_fetch_buffer_depth(6, None, Some(HISTORICAL_VERY_DENSE_ROWS_PER_BLOCK)),
            8
        );
        assert_eq!(
            historical_fetch_buffer_depth(8, None, Some(HISTORICAL_SPARSE_ROWS_PER_BLOCK)),
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT
        );
    }

    #[test]
    fn historical_fetch_budget_keeps_active_downloads_full_when_memory_is_healthy() {
        assert!(historical_fetch_budget_has_capacity(
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT + 4,
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT - 1,
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT,
            Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES)
        ));
        assert!(!historical_fetch_budget_has_capacity(
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT,
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT,
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT,
            Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES)
        ));
        assert!(!historical_fetch_budget_has_capacity(
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT,
            0,
            HISTORICAL_FETCH_BUFFER_DEPTH_LIMIT,
            Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1)
        ));
    }

    #[test]
    fn historical_density_ewma_tracks_recent_batches() {
        let first = update_historical_density_ewma(None, 1000, 10).unwrap();
        assert_eq!(first, 100.0);

        let second = update_historical_density_ewma(Some(first), 9000, 10).unwrap();
        assert_eq!(second, 500.0);

        assert_eq!(
            update_historical_density_ewma(Some(second), 100, 0),
            Some(second)
        );
    }

    #[test]
    fn historical_validation_work_ranges_keep_contiguous_balanced_chunks() {
        let ranges = historical_validation_work_ranges([100, 1, 1, 100, 1, 1], 4);

        assert_eq!(ranges, vec![0..1, 1..3, 3..4, 4..6]);
    }

    #[test]
    fn historical_validation_work_ranges_leave_room_for_remaining_tasks() {
        let ranges = historical_validation_work_ranges([10, 10, 10], 8);

        assert_eq!(ranges, vec![0..1, 1..2, 2..3]);
    }

    #[test]
    fn allocator_trim_is_rate_limited_to_low_memory() {
        let now = std::time::Instant::now();
        assert!(!historical_allocator_trim_is_due(None, now, None));
        assert!(!historical_allocator_trim_is_due(
            None,
            now,
            Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES)
        ));
        assert!(historical_allocator_trim_is_due(
            None,
            now,
            Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1)
        ));
        assert!(!historical_allocator_trim_is_due(
            Some(now),
            now,
            Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1)
        ));
        assert!(historical_allocator_trim_is_due(
            Some(now - HISTORICAL_ALLOCATOR_TRIM_INTERVAL),
            now,
            Some(HISTORICAL_LOW_AVAILABLE_MEMORY_BYTES - 1)
        ));
    }

    #[test]
    fn locate_consensus_reorg_returns_none_for_matching_tip() {
        let temp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        store
            .append_anchors(vec![anchor_for(&first, 1), anchor_for(&second, 2)])
            .unwrap();

        assert!(
            locate_consensus_reorg(&store, &[first, second])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn locate_consensus_reorg_finds_common_ancestor() {
        let temp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let old_third = header(102, second.hash_slow(), 0x03);
        let new_third = header(102, second.hash_slow(), 0x13);
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )
        .unwrap();
        store
            .append_anchors(vec![
                anchor_for(&first, 1),
                anchor_for(&second, 2),
                anchor_for(&new_third, 3),
            ])
            .unwrap();

        let reorg =
            locate_consensus_reorg(&store, &[first.clone(), second.clone(), old_third.clone()])
                .unwrap()
                .expect("expected a consensus reorg");
        assert_eq!(reorg.retained_headers, vec![first, second.clone()]);
        assert_eq!(
            reorg.indexed_head.map(|anchor| anchor.block_hash),
            Some(second.hash_slow())
        );
        assert_eq!(reorg.reverted_hashes, vec![old_third.hash_slow()]);
    }

    #[test]
    fn locate_consensus_reorg_errors_when_window_has_no_common_ancestor() {
        let temp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let competing_first = header(100, B256::ZERO, 0x11);
        let competing_second = header(101, competing_first.hash_slow(), 0x12);
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
        )
        .unwrap();
        store
            .append_anchors(vec![
                anchor_for(&competing_first, 1),
                anchor_for(&competing_second, 2),
            ])
            .unwrap();

        let error = locate_consensus_reorg(&store, &[first, second]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("consensus anchor reorg exceeded the persisted recent-header window")
        );
    }
}
