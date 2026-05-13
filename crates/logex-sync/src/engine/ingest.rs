use super::*;
use crate::extract;
use logex_types::{ExecutionAnchor, ExecutionBlockMarker, LogRow};

const HISTORICAL_EXTRACT_CHUNK_BLOCKS: usize = 256;
const HISTORICAL_WRITE_CHUNK_BLOCKS: usize = 512;
const HISTORICAL_EXTRACTION_PIPELINE_DEPTH: usize = 4;

pub(super) struct HistoricalExtractedBatch {
    pub(super) block_count: u64,
    pub(super) row_count: u64,
    pub(super) lowest_header: Header,
    pub(super) chunks: Vec<HistoricalExtractedChunk>,
    pub(super) extraction_elapsed: Duration,
}

pub(super) struct HistoricalExtractedChunk {
    pub(super) rows: Vec<LogRow>,
    pub(super) row_count: u64,
    pub(super) block_count: usize,
    pub(super) lowest_header: Header,
    pub(super) extraction_elapsed: Duration,
}

struct HistoricalChunkWriteOutcome {
    floor: Option<ExecutionBlockMarker>,
    anchor: Option<ExecutionBlockMarker>,
    write_elapsed: Duration,
}

impl HistoricalExtractedChunk {
    fn merge(&mut self, mut other: Self) {
        self.rows.append(&mut other.rows);
        self.row_count = self.row_count.saturating_add(other.row_count);
        self.block_count = self.block_count.saturating_add(other.block_count);
        if other.lowest_header.number() < self.lowest_header.number() {
            self.lowest_header = other.lowest_header;
        }
        self.extraction_elapsed += other.extraction_elapsed;
    }
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
    blocks: Vec<HistoricalValidatedBlock>,
) -> Result<HistoricalIngestOutcome> {
    let extracted = extract_validated_historical_blocks(blocks).await?;
    write_extracted_historical_batch(storage, subscriptions, extracted).await
}

pub(super) async fn extract_validated_historical_blocks(
    blocks: Vec<HistoricalValidatedBlock>,
) -> Result<HistoricalExtractedBatch> {
    if blocks.is_empty() {
        return Ok(HistoricalExtractedBatch {
            block_count: 0,
            row_count: 0,
            lowest_header: Header {
                number: 0,
                ..Default::default()
            },
            chunks: Vec::new(),
            extraction_elapsed: Duration::ZERO,
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
    let mut chunks = Vec::new();

    let mut block_chunks = blocks.into_iter();
    let mut extraction_tasks = VecDeque::with_capacity(HISTORICAL_EXTRACTION_PIPELINE_DEPTH);
    let mut write_buffer: Option<HistoricalExtractedChunk> = None;
    fill_historical_extraction_pipeline(&mut block_chunks, &mut extraction_tasks);
    while let Some(extraction_task) = extraction_tasks.pop_front() {
        fill_historical_extraction_pipeline(&mut block_chunks, &mut extraction_tasks);
        let extracted = extraction_task
            .await
            .map_err(|error| eyre::eyre!("historical extraction worker failed: {error}"))??;

        match write_buffer.as_mut() {
            Some(buffer) => buffer.merge(extracted),
            None => write_buffer = Some(extracted),
        }

        let should_write = write_buffer
            .as_ref()
            .is_some_and(|buffer| buffer.block_count >= HISTORICAL_WRITE_CHUNK_BLOCKS);
        if should_write {
            let extracted = write_buffer
                .take()
                .expect("historical write buffer is present when ready");
            row_count = row_count.saturating_add(extracted.row_count);
            extraction_elapsed += extracted.extraction_elapsed;
            chunks.push(extracted);
        }
    }

    if let Some(extracted) = write_buffer.take() {
        row_count = row_count.saturating_add(extracted.row_count);
        extraction_elapsed += extracted.extraction_elapsed;
        chunks.push(extracted);
    }

    Ok(HistoricalExtractedBatch {
        block_count,
        row_count,
        lowest_header,
        chunks,
        extraction_elapsed,
    })
}

pub(super) async fn write_extracted_historical_batch(
    storage: Arc<RwLock<PartitionManager>>,
    subscriptions: Option<SubscriptionManager>,
    extracted: HistoricalExtractedBatch,
) -> Result<HistoricalIngestOutcome> {
    if extracted.block_count == 0 {
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

    let mut write_elapsed = Duration::ZERO;
    let mut floor = None;
    let mut anchor = None;

    let write_chunks = coalesce_historical_write_chunks(extracted.chunks);
    let last_chunk_index = write_chunks.len().saturating_sub(1);
    for (index, chunk) in write_chunks.into_iter().enumerate() {
        let chunk_outcome =
            write_extracted_historical_chunk(Arc::clone(&storage), subscriptions.clone(), chunk)
                .await?;
        write_elapsed += chunk_outcome.write_elapsed;
        if index == last_chunk_index {
            floor = chunk_outcome.floor;
            anchor = chunk_outcome.anchor;
        }
    }

    Ok(HistoricalIngestOutcome {
        block_count: extracted.block_count,
        row_count: extracted.row_count,
        floor: floor.unwrap_or_else(|| execution_marker_from_header(&extracted.lowest_header)),
        anchor,
        extraction_elapsed: extracted.extraction_elapsed,
        write_elapsed,
    })
}

fn fill_historical_extraction_pipeline(
    blocks: &mut std::vec::IntoIter<HistoricalValidatedBlock>,
    extraction_tasks: &mut VecDeque<tokio::task::JoinHandle<Result<HistoricalExtractedChunk>>>,
) {
    while extraction_tasks.len() < HISTORICAL_EXTRACTION_PIPELINE_DEPTH {
        let Some(task) = next_historical_extract_task(blocks) else {
            break;
        };
        extraction_tasks.push_back(task);
    }
}

fn next_historical_extract_task(
    blocks: &mut std::vec::IntoIter<HistoricalValidatedBlock>,
) -> Option<tokio::task::JoinHandle<Result<HistoricalExtractedChunk>>> {
    let mut chunk = Vec::with_capacity(HISTORICAL_EXTRACT_CHUNK_BLOCKS);
    for _ in 0..HISTORICAL_EXTRACT_CHUNK_BLOCKS {
        let Some(block) = blocks.next() else {
            break;
        };
        chunk.push(block);
    }

    if chunk.is_empty() {
        return None;
    }

    Some(tokio::task::spawn_blocking(
        move || -> Result<HistoricalExtractedChunk> {
            let lowest_header = chunk
                .iter()
                .min_by_key(|block| block.header.number())
                .map(|block| block.header.clone())
                .expect("non-empty historical block chunk has a lowest header");

            let extraction_started = std::time::Instant::now();
            let block_count = chunk.len();
            let rows = collect_validated_historical_rows(chunk);
            let extraction_elapsed = extraction_started.elapsed();
            let row_count = rows.len() as u64;

            Ok(HistoricalExtractedChunk {
                rows,
                row_count,
                block_count,
                lowest_header,
                extraction_elapsed,
            })
        },
    ))
}

async fn write_extracted_historical_chunk(
    storage: Arc<RwLock<PartitionManager>>,
    subscriptions: Option<SubscriptionManager>,
    extracted: HistoricalExtractedChunk,
) -> Result<HistoricalChunkWriteOutcome> {
    tokio::task::spawn_blocking(move || -> Result<HistoricalChunkWriteOutcome> {
        let write_started = std::time::Instant::now();
        let mut storage = storage.blocking_write();
        if !extracted.rows.is_empty() {
            storage
                .write_historical_batch(&extracted.rows)
                .map_err(|e| eyre::eyre!("storage write error: {e}"))?;

            if let Some(ref subs) = subscriptions {
                subs.notify(&extracted.rows);
            }
        }

        storage
            .record_historical_floor(&extracted.lowest_header)
            .map_err(|e| eyre::eyre!("historical metadata error: {e}"))?;
        Ok(HistoricalChunkWriteOutcome {
            floor: storage.historical_floor(),
            anchor: storage.historical_anchor(),
            write_elapsed: write_started.elapsed(),
        })
    })
    .await
    .map_err(|error| eyre::eyre!("historical storage worker failed: {error}"))?
}

fn coalesce_historical_write_chunks(
    chunks: Vec<HistoricalExtractedChunk>,
) -> Vec<HistoricalExtractedChunk> {
    let mut write_chunks = Vec::new();
    let mut write_buffer: Option<HistoricalExtractedChunk> = None;

    for chunk in chunks {
        match write_buffer.as_mut() {
            Some(buffer) => buffer.merge(chunk),
            None => write_buffer = Some(chunk),
        }

        if write_buffer
            .as_ref()
            .is_some_and(|buffer| buffer.block_count >= HISTORICAL_WRITE_CHUNK_BLOCKS)
        {
            write_chunks.push(
                write_buffer
                    .take()
                    .expect("historical write buffer is present when ready"),
            );
        }
    }

    if let Some(buffer) = write_buffer {
        write_chunks.push(buffer);
    }

    write_chunks
}

fn collect_validated_historical_rows(blocks: Vec<HistoricalValidatedBlock>) -> Vec<LogRow> {
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
        extract::append_from_body_receipts(
            &mut rows,
            block.header.number(),
            block.block_hash,
            block.header.timestamp(),
            &block.body,
            &block.receipts,
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    fn extracted_chunk(block_count: usize, lowest_block: u64) -> HistoricalExtractedChunk {
        HistoricalExtractedChunk {
            rows: Vec::new(),
            row_count: 0,
            block_count,
            lowest_header: Header {
                number: lowest_block,
                ..Default::default()
            },
            extraction_elapsed: Duration::ZERO,
        }
    }

    #[test]
    fn coalesces_fused_extraction_chunks_into_write_sized_batches() {
        let chunks = vec![
            extracted_chunk(128, 896),
            extracted_chunk(128, 768),
            extracted_chunk(128, 640),
            extracted_chunk(128, 512),
            extracted_chunk(128, 384),
            extracted_chunk(128, 256),
            extracted_chunk(128, 128),
            extracted_chunk(128, 0),
        ];

        let write_chunks = coalesce_historical_write_chunks(chunks);

        assert_eq!(write_chunks.len(), 2);
        assert_eq!(write_chunks[0].block_count, HISTORICAL_WRITE_CHUNK_BLOCKS);
        assert_eq!(write_chunks[0].lowest_header.number(), 512);
        assert_eq!(write_chunks[1].block_count, HISTORICAL_WRITE_CHUNK_BLOCKS);
        assert_eq!(write_chunks[1].lowest_header.number(), 0);
    }

    #[test]
    fn keeps_partial_historical_write_chunk() {
        let chunks = vec![extracted_chunk(300, 700), extracted_chunk(300, 400)];

        let write_chunks = coalesce_historical_write_chunks(chunks);

        assert_eq!(write_chunks.len(), 1);
        assert_eq!(write_chunks[0].block_count, 600);
        assert_eq!(write_chunks[0].lowest_header.number(), 400);
    }
}
