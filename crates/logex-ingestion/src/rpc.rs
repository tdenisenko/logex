use std::time::Instant;

use alloy_eips::{BlockId, BlockNumberOrTag};
use alloy_primitives::{B256, Log};
use alloy_provider::{Provider, ProviderBuilder};

use crate::pipeline::Pipeline;

/// Fetch and ingest a range of blocks from an Ethereum JSON-RPC endpoint.
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

    for block_num in from_block..=to_block {
        let block_start = Instant::now();

        match ingest_single_block(pipeline, &provider, block_num).await {
            Ok(log_count) => {
                stats.blocks_ingested += 1;
                stats.logs_ingested += log_count;

                if stats.blocks_ingested % 10 == 0 || block_num == to_block {
                    let elapsed = start.elapsed();
                    let bps = stats.blocks_ingested as f64 / elapsed.as_secs_f64();
                    tracing::info!(
                        block = block_num,
                        logs = log_count,
                        total_blocks = stats.blocks_ingested,
                        total_logs = stats.logs_ingested,
                        blocks_per_sec = format!("{bps:.1}"),
                        "ingested block"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(block = block_num, error = %e, "failed to ingest block, retrying");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                match ingest_single_block(pipeline, &provider, block_num).await {
                    Ok(log_count) => {
                        stats.blocks_ingested += 1;
                        stats.logs_ingested += log_count;
                    }
                    Err(e) => {
                        tracing::error!(block = block_num, error = %e, "block ingestion failed");
                        return Err(e);
                    }
                }
            }
        }

        // Adaptive rate limiting: stay under typical free-tier RPC limits
        let elapsed = block_start.elapsed();
        if elapsed.as_millis() < 100 {
            tokio::time::sleep(std::time::Duration::from_millis(100) - elapsed).await;
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

async fn ingest_single_block(
    pipeline: &mut Pipeline,
    provider: &impl Provider,
    block_num: u64,
) -> Result<u64, IngestError> {
    // Fetch block header
    let block = provider
        .get_block_by_number(BlockNumberOrTag::Number(block_num))
        .await
        .map_err(|e| IngestError::Rpc(format!("get_block {block_num}: {e}")))?
        .ok_or_else(|| IngestError::Rpc(format!("block {block_num} not found")))?;

    let block_hash = block.header.hash;
    let timestamp = block.header.timestamp;

    // Fetch receipts
    let receipts = provider
        .get_block_receipts(BlockId::Number(BlockNumberOrTag::Number(block_num)))
        .await
        .map_err(|e| IngestError::Rpc(format!("get_receipts {block_num}: {e}")))?
        .ok_or_else(|| IngestError::Rpc(format!("receipts for block {block_num} not found")))?;

    // Convert receipts to (tx_hash, Vec<Log>) pairs
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

    let log_count = pipeline
        .ingest_block(block_num, block_hash, timestamp, &txs)
        .map_err(|e| IngestError::Storage(format!("{e}")))?;

    Ok(log_count)
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
