use std::time::Instant;

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{B256, Log};
use alloy_provider::{Provider, ProviderBuilder};

use crate::pipeline::Pipeline;

/// Number of blocks to fetch concurrently in each batch.
const BATCH_SIZE: u64 = 10;

/// Fetch and ingest a range of blocks from an Ethereum JSON-RPC endpoint.
/// Uses batched parallel RPC fetching for throughput.
pub async fn ingest_range(
    pipeline: &mut Pipeline,
    rpc_url: &str,
    from_block: u64,
    to_block: u64,
) -> Result<IngestStats, IngestError> {
    let provider = ProviderBuilder::new().connect_http(
        rpc_url
            .parse()
            .map_err(|e| IngestError::Config(format!("{e}")))?,
    );

    let mut stats = IngestStats::default();
    let start = Instant::now();

    tracing::info!(from = from_block, to = to_block, "starting RPC ingestion");

    let mut current = from_block;
    while current <= to_block {
        let batch_end = (current + BATCH_SIZE - 1).min(to_block);
        let batch_start_time = Instant::now();

        // Fetch all blocks in this batch concurrently
        let block_nums: Vec<u64> = (current..=batch_end).collect();
        let fetched = fetch_blocks_parallel(&provider, &block_nums).await?;

        // Ingest in order (storage writes are sequential)
        for block_data in fetched {
            let log_count = pipeline
                .ingest_block(
                    block_data.block_num,
                    block_data.block_hash,
                    block_data.timestamp,
                    &block_data.txs,
                )
                .map_err(|e| IngestError::Storage(format!("{e}")))?;

            stats.blocks_ingested += 1;
            stats.logs_ingested += log_count;
        }

        let elapsed = start.elapsed();
        let bps = stats.blocks_ingested as f64 / elapsed.as_secs_f64();
        tracing::info!(
            block = batch_end,
            total_blocks = stats.blocks_ingested,
            total_logs = stats.logs_ingested,
            blocks_per_sec = format!("{bps:.1}"),
            "ingested batch"
        );

        current = batch_end + 1;

        // Rate limit between batches to stay under free-tier RPC limits
        let batch_elapsed = batch_start_time.elapsed();
        let min_batch_time = std::time::Duration::from_millis(500);
        if batch_elapsed < min_batch_time {
            tokio::time::sleep(min_batch_time - batch_elapsed).await;
        }
    }

    stats.elapsed = start.elapsed();
    tracing::info!(
        blocks = stats.blocks_ingested,
        logs = stats.logs_ingested,
        elapsed = ?stats.elapsed,
        "ingestion complete"
    );

    Ok(stats)
}

/// Data fetched from RPC for a single block.
struct FetchedBlock {
    block_num: u64,
    block_hash: B256,
    timestamp: u64,
    txs: Vec<(B256, Vec<Log>)>,
}

/// Fetch multiple blocks in parallel. Returns results in block number order.
async fn fetch_blocks_parallel(
    provider: &impl Provider,
    block_nums: &[u64],
) -> Result<Vec<FetchedBlock>, IngestError> {
    let futures: Vec<_> = block_nums
        .iter()
        .map(|&num| fetch_single_block(provider, num))
        .collect();

    let results = futures::future::join_all(futures).await;

    let mut blocks = Vec::with_capacity(results.len());
    for result in results {
        match result {
            Ok(block) => blocks.push(block),
            Err(e) => {
                // Retry the failed block once
                tracing::warn!(error = %e, "batch fetch failed, retrying individually");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let block_num = blocks.len() as u64 + block_nums[0];
                let block = fetch_single_block(provider, block_num).await?;
                blocks.push(block);
            }
        }
    }

    Ok(blocks)
}

async fn fetch_single_block(
    provider: &impl Provider,
    block_num: u64,
) -> Result<FetchedBlock, IngestError> {
    // Fetch block header and receipts concurrently
    let (block_result, receipts_result) = tokio::join!(
        provider.get_block_by_number(BlockNumberOrTag::Number(block_num)),
        provider.get_block_receipts(BlockId::Number(BlockNumberOrTag::Number(block_num))),
    );

    let block = block_result
        .map_err(|e| IngestError::Rpc(format!("get_block {block_num}: {e}")))?
        .ok_or_else(|| IngestError::Rpc(format!("block {block_num} not found")))?;

    let receipts = receipts_result
        .map_err(|e| IngestError::Rpc(format!("get_receipts {block_num}: {e}")))?
        .ok_or_else(|| IngestError::Rpc(format!("receipts for block {block_num} not found")))?;

    let txs: Vec<(B256, Vec<Log>)> = receipts
        .into_iter()
        .map(|receipt| {
            let tx_hash = receipt.transaction_hash;
            let logs: Vec<Log> = receipt
                .inner
                .logs()
                .iter()
                .map(|log| {
                    Log::new_unchecked(
                        log.address(),
                        log.topics().to_vec(),
                        log.data().data.clone(),
                    )
                })
                .collect();
            (tx_hash, logs)
        })
        .collect();

    Ok(FetchedBlock {
        block_num,
        block_hash: block.header.hash,
        timestamp: block.header.timestamp,
        txs,
    })
}

/// Fetch the latest block number from the RPC endpoint.
pub async fn get_latest_block(rpc_url: &str) -> Result<u64, IngestError> {
    let provider = ProviderBuilder::new().connect_http(
        rpc_url
            .parse()
            .map_err(|e| IngestError::Config(format!("{e}")))?,
    );

    let block_num = provider
        .get_block_number()
        .await
        .map_err(|e| IngestError::Rpc(format!("get_block_number: {e}")))?;

    Ok(block_num)
}

#[derive(Debug, Default)]
pub struct IngestStats {
    pub blocks_ingested: u64,
    pub logs_ingested: u64,
    pub elapsed: std::time::Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("config error: {0}")]
    Config(String),
    #[error("RPC error: {0}")]
    Rpc(String),
    #[error("storage error: {0}")]
    Storage(String),
}
