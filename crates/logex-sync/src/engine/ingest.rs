use super::*;
use crate::extract;
use logex_types::{ExecutionAnchor, ExecutionBlockMarker, LogRow};
use tokio::task::JoinSet;

struct HistoricalRows {
    index: usize,
    rows: Vec<LogRow>,
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

    /// Write a validated historical block batch without moving the live canonical head.
    pub(super) async fn ingest_historical_blocks(
        &mut self,
        blocks: Vec<HistoricalBlockIngest>,
    ) -> Result<u64> {
        let outcome = self.historical_write_future(blocks).await?;
        Ok(self.record_historical_ingest_outcome(outcome))
    }

    pub(super) fn historical_write_future(
        &self,
        blocks: Vec<HistoricalBlockIngest>,
    ) -> impl std::future::Future<Output = Result<HistoricalIngestOutcome>> + Send + 'static {
        let storage = Arc::clone(&self.storage);
        let subscriptions = self.subscriptions.clone();
        async move { write_historical_blocks(storage, subscriptions, blocks).await }
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

async fn write_historical_blocks(
    storage: Arc<RwLock<PartitionManager>>,
    subscriptions: Option<SubscriptionManager>,
    blocks: Vec<HistoricalBlockIngest>,
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
    let extraction_started = std::time::Instant::now();
    let rows = extract_historical_rows_parallel(blocks).await?;
    let extraction_elapsed = extraction_started.elapsed();
    let row_count = rows.len() as u64;
    let lowest_header_for_write = lowest_header.clone();
    let write_started = std::time::Instant::now();
    let (floor, anchor) = tokio::task::spawn_blocking(move || -> Result<_> {
        let mut storage = storage.blocking_write();
        if !rows.is_empty() {
            storage
                .write_historical_batch(&rows)
                .map_err(|e| eyre::eyre!("storage write error: {e}"))?;

            if let Some(ref subs) = subscriptions {
                subs.notify(&rows);
            }
        }

        storage
            .record_historical_floor(&lowest_header_for_write)
            .map_err(|e| eyre::eyre!("historical metadata error: {e}"))?;
        Ok((storage.historical_floor(), storage.historical_anchor()))
    })
    .await
    .map_err(|error| eyre::eyre!("historical storage worker failed: {error}"))??;
    let write_elapsed = write_started.elapsed();

    Ok(HistoricalIngestOutcome {
        block_count,
        row_count,
        floor: floor.unwrap_or_else(|| execution_marker_from_header(&lowest_header)),
        anchor,
        extraction_elapsed,
        write_elapsed,
    })
}

async fn extract_historical_rows_parallel(
    blocks: Vec<HistoricalBlockIngest>,
) -> Result<Vec<LogRow>> {
    let mut tasks = JoinSet::new();
    for (index, block) in blocks.into_iter().enumerate() {
        tasks.spawn_blocking(move || HistoricalRows {
            index,
            rows: extract::extract_from_block(
                block.header.number(),
                block.block_hash,
                block.header.timestamp(),
                &block.txs,
            ),
        });
    }

    let mut batches = Vec::with_capacity(tasks.len());
    while let Some(result) = tasks.join_next().await {
        let batch =
            result.map_err(|error| eyre::eyre!("historical extraction worker failed: {error}"))?;
        batches.push(batch);
    }
    batches.sort_by_key(|batch| batch.index);

    let total_rows = batches.iter().map(|batch| batch.rows.len()).sum();
    let mut rows = Vec::with_capacity(total_rows);
    for batch in batches {
        rows.extend(batch.rows);
    }
    Ok(rows)
}

pub(super) fn execution_marker_from_header(header: &Header) -> ExecutionBlockMarker {
    ExecutionBlockMarker {
        block_number: header.number(),
        block_hash: header.hash_slow(),
        timestamp: header.timestamp(),
    }
}
