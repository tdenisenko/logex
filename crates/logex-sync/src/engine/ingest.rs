use super::*;
use crate::extract;
use logex_types::{ExecutionAnchor, ExecutionBlockMarker, LogRow};

const HISTORICAL_WRITE_CHUNK_BLOCKS: usize = 512;

struct HistoricalChunkWriteOutcome {
    row_count: u64,
    floor: Option<ExecutionBlockMarker>,
    anchor: Option<ExecutionBlockMarker>,
    extraction_elapsed: Duration,
    write_elapsed: Duration,
}

impl SyncEngine {
    /// Write a block's logs to storage and notify subscribers.
    pub(super) async fn ingest_block(
        &self,
        header: &Header,
        block_hash: B256,
        txs: &[(B256, Vec<Log>)],
        recent_headers: &[Header],
        anchor: Option<&ExecutionAnchor>,
    ) -> Result<u64> {
        let block_number = header.number();
        let timestamp = header.timestamp();
        let rows = extract::extract_from_block(block_number, block_hash, timestamp, txs);
        let count = rows.len() as u64;

        let mut storage = self.storage.write().await;
        if !rows.is_empty() {
            storage
                .write_batch(&rows)
                .map_err(|e| eyre::eyre!("storage write error: {e}"))?;

            if let Some(ref subs) = self.subscriptions {
                subs.notify(&rows);
            }
        }
        match anchor {
            Some(anchor) => {
                storage
                    .record_verified_canonical_state(anchor, header, recent_headers)
                    .map_err(|e| eyre::eyre!("storage metadata error: {e}"))?;
                storage
                    .record_historical_floor(header)
                    .map_err(|e| eyre::eyre!("historical metadata error: {e}"))?;
            }
            None => storage
                .record_canonical_state(header, recent_headers)
                .map_err(|e| eyre::eyre!("storage metadata error: {e}"))?,
        }

        Ok(count)
    }

    pub(super) fn record_historical_ingest_outcome(
        &mut self,
        outcome: HistoricalIngestOutcome,
    ) -> u64 {
        self.progress.record_historical_blocks(
            outcome.floor,
            outcome.anchor,
            crate::EXECUTION_HISTORY_TARGET_BLOCK,
            outcome.block_count,
            outcome.row_count,
        );
        tracing::debug!(
            blocks = outcome.block_count,
            rows = outcome.row_count,
            extraction_ms = outcome.extraction_elapsed.as_millis(),
            write_ms = outcome.write_elapsed.as_millis(),
            "historical block batch extracted and written"
        );
        outcome.row_count
    }

    /// Handle a detected reorg: mark reverted blocks non-canonical.
    pub(super) async fn handle_reorg(&self, reorg: ReorgInfo) -> Result<()> {
        if reorg.reverted_hashes.is_empty() {
            return Ok(());
        }

        self.peers.remove_cached_blocks(&reorg.reverted_hashes);

        let storage = self.storage.write().await;
        let mut total_reverted = 0u64;
        for hash in &reorg.reverted_hashes {
            total_reverted += storage
                .mark_non_canonical(*hash)
                .map_err(|e| eyre::eyre!("reorg error: {e}"))?;
        }

        tracing::info!(
            fork_block = reorg.fork_block,
            reverted_blocks = reorg.reverted_hashes.len(),
            reverted_rows = total_reverted,
            "handled reorg"
        );
        Ok(())
    }
}

pub(super) async fn write_validated_historical_blocks(
    storage: Arc<RwLock<PartitionManager>>,
    subscriptions: Option<SubscriptionManager>,
    mut blocks: Vec<HistoricalValidatedBlock>,
) -> Result<HistoricalIngestOutcome> {
    if blocks.is_empty() {
        return Ok(HistoricalIngestOutcome {
            block_count: 0,
            row_count: 0,
            floor: ExecutionBlockMarker {
                block_number: 0,
                block_hash: B256::ZERO,
                timestamp: 0,
            },
            anchor: None,
            extraction_elapsed: Duration::ZERO,
            write_elapsed: Duration::ZERO,
        });
    }

    let lowest_header = blocks
        .iter()
        .min_by_key(|block| block.header.number())
        .map(|block| block.header.clone())
        .expect("non-empty historical block batch has a lowest header");
    let block_count = blocks.len() as u64;
    let mut row_count = 0u64;
    let mut extraction_elapsed = Duration::ZERO;
    let mut write_elapsed = Duration::ZERO;
    let mut floor = None;
    let mut anchor = None;

    while !blocks.is_empty() {
        let take = blocks.len().min(HISTORICAL_WRITE_CHUNK_BLOCKS);
        let chunk: Vec<_> = blocks.drain(..take).collect();
        let chunk_subscriptions = subscriptions.clone();
        let chunk_storage = Arc::clone(&storage);
        let chunk_outcome =
            tokio::task::spawn_blocking(move || -> Result<HistoricalChunkWriteOutcome> {
                let chunk_lowest_header = chunk
                    .iter()
                    .min_by_key(|block| block.header.number())
                    .map(|block| block.header.clone())
                    .expect("non-empty historical block chunk has a lowest header");

                let extraction_started = std::time::Instant::now();
                let rows = collect_validated_historical_rows(&chunk);
                let extraction_elapsed = extraction_started.elapsed();
                let row_count = rows.len() as u64;

                let write_started = std::time::Instant::now();
                let mut storage = chunk_storage.blocking_write();
                if !rows.is_empty() {
                    storage
                        .write_historical_batch(&rows)
                        .map_err(|e| eyre::eyre!("storage write error: {e}"))?;

                    if let Some(ref subs) = chunk_subscriptions {
                        subs.notify(&rows);
                    }
                }

                storage
                    .record_historical_floor(&chunk_lowest_header)
                    .map_err(|e| eyre::eyre!("historical metadata error: {e}"))?;
                Ok(HistoricalChunkWriteOutcome {
                    row_count,
                    floor: storage.historical_floor(),
                    anchor: storage.historical_anchor(),
                    extraction_elapsed,
                    write_elapsed: write_started.elapsed(),
                })
            })
            .await
            .map_err(|error| eyre::eyre!("historical storage worker failed: {error}"))??;
        extraction_elapsed += chunk_outcome.extraction_elapsed;
        write_elapsed += chunk_outcome.write_elapsed;
        row_count = row_count.saturating_add(chunk_outcome.row_count);
        floor = chunk_outcome.floor;
        anchor = chunk_outcome.anchor;
    }

    Ok(HistoricalIngestOutcome {
        block_count,
        row_count,
        floor: floor.unwrap_or_else(|| execution_marker_from_header(&lowest_header)),
        anchor,
        extraction_elapsed,
        write_elapsed,
    })
}

fn collect_validated_historical_rows(blocks: &[HistoricalValidatedBlock]) -> Vec<LogRow> {
    let total_rows = blocks
        .iter()
        .map(|block| {
            block
                .receipts
                .iter()
                .map(|receipt| receipt.logs().len())
                .sum::<usize>()
        })
        .sum();
    let mut rows = Vec::with_capacity(total_rows);
    for block in blocks {
        rows.extend(extract::extract_from_body_receipts(
            block.header.number(),
            block.block_hash,
            block.header.timestamp(),
            &block.body,
            &block.receipts,
        ));
    }
    rows
}

pub(super) fn execution_marker_from_header(header: &Header) -> ExecutionBlockMarker {
    ExecutionBlockMarker {
        block_number: header.number(),
        block_hash: header.hash_slow(),
        timestamp: header.timestamp(),
    }
}
