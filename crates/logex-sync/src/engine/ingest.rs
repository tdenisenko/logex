use super::*;
use crate::extract;
use logex_types::{ExecutionAnchor, ExecutionBlockMarker, LogRow};
use std::collections::VecDeque;

const HISTORICAL_EXTRACT_CHUNK_BLOCKS: usize = 256;
const HISTORICAL_WRITE_CHUNK_BLOCKS: usize = 2048;
const HISTORICAL_WRITE_CHUNK_ROWS: u64 = 500_000;
const HISTORICAL_WRITE_CHUNK_HIGH_MEMORY_ROWS: u64 = 1_000_000;
const HISTORICAL_WRITE_CHUNK_HIGH_MEMORY_AVAILABLE_BYTES: u64 = 6 * 1024 * 1024 * 1024;
const HISTORICAL_EXTRACTION_PIPELINE_MIN_DEPTH: usize = 2;
const HISTORICAL_EXTRACTION_PIPELINE_MAX_DEPTH: usize = 8;

pub(super) struct HistoricalExtractedBatch {
    pub(super) chunks: Vec<HistoricalExtractedChunk>,
}

pub(super) struct HistoricalExtractedChunk {
    pub(super) rows: Vec<LogRow>,
    pub(super) row_count: u64,
    pub(super) block_count: usize,
    pub(super) lowest_header: Header,
    pub(super) extraction_elapsed: Duration,
}

pub(super) struct HistoricalBatchWriter {
    storage: Arc<RwLock<PartitionManager>>,
    write_buffer: Option<HistoricalWriteBuffer>,
    write_elapsed: Duration,
    floor: Option<ExecutionBlockMarker>,
    anchor: Option<ExecutionBlockMarker>,
    lowest_header: Option<Header>,
    block_count: u64,
    row_count: u64,
    extraction_elapsed: Duration,
}

struct HistoricalChunkWriteOutcome {
    floor: Option<ExecutionBlockMarker>,
    anchor: Option<ExecutionBlockMarker>,
    write_elapsed: Duration,
}

struct HistoricalWriteBuffer {
    chunks: Vec<HistoricalExtractedChunk>,
    row_count: u64,
    block_count: usize,
    lowest_header: Header,
    extraction_elapsed: Duration,
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

impl HistoricalWriteBuffer {
    fn new(chunk: HistoricalExtractedChunk) -> Self {
        Self {
            row_count: chunk.row_count,
            block_count: chunk.block_count,
            lowest_header: chunk.lowest_header.clone(),
            extraction_elapsed: chunk.extraction_elapsed,
            chunks: vec![chunk],
        }
    }

    fn push(&mut self, chunk: HistoricalExtractedChunk) {
        self.row_count = self.row_count.saturating_add(chunk.row_count);
        self.block_count = self.block_count.saturating_add(chunk.block_count);
        if chunk.lowest_header.number() < self.lowest_header.number() {
            self.lowest_header = chunk.lowest_header.clone();
        }
        self.extraction_elapsed += chunk.extraction_elapsed;
        self.chunks.push(chunk);
    }

    fn is_ready(&self) -> bool {
        historical_write_chunk_counts_are_ready(self.block_count, self.row_count)
    }

    fn into_chunk(mut self) -> HistoricalExtractedChunk {
        if self.chunks.len() == 1 {
            return self
                .chunks
                .pop()
                .expect("single historical write chunk is present");
        }

        let mut rows = Vec::with_capacity(self.row_count.min(usize::MAX as u64) as usize);
        for mut chunk in self.chunks {
            rows.append(&mut chunk.rows);
        }

        HistoricalExtractedChunk {
            rows,
            row_count: self.row_count,
            block_count: self.block_count,
            lowest_header: self.lowest_header,
            extraction_elapsed: self.extraction_elapsed,
        }
    }
}

impl HistoricalBatchWriter {
    pub(super) fn new(storage: Arc<RwLock<PartitionManager>>) -> Self {
        Self {
            storage,
            write_buffer: None,
            write_elapsed: Duration::ZERO,
            floor: None,
            anchor: None,
            lowest_header: None,
            block_count: 0,
            row_count: 0,
            extraction_elapsed: Duration::ZERO,
        }
    }

    pub(super) async fn push_chunk(&mut self, chunk: HistoricalExtractedChunk) -> Result<()> {
        self.block_count = self.block_count.saturating_add(chunk.block_count as u64);
        self.row_count = self.row_count.saturating_add(chunk.row_count);
        self.extraction_elapsed += chunk.extraction_elapsed;
        if self
            .lowest_header
            .as_ref()
            .is_none_or(|header| chunk.lowest_header.number() < header.number())
        {
            self.lowest_header = Some(chunk.lowest_header.clone());
        }

        match self.write_buffer.as_mut() {
            Some(buffer) => buffer.push(chunk),
            None => self.write_buffer = Some(HistoricalWriteBuffer::new(chunk)),
        }

        if self
            .write_buffer
            .as_ref()
            .is_some_and(|buffer| buffer.is_ready())
        {
            self.flush_buffer().await?;
        }

        Ok(())
    }

    pub(super) async fn finish(mut self) -> Result<HistoricalIngestOutcome> {
        self.flush_buffer().await?;

        if self.block_count == 0 {
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

        let lowest_header = self
            .lowest_header
            .expect("non-empty historical writer has a lowest header");
        Ok(HistoricalIngestOutcome {
            block_count: self.block_count,
            row_count: self.row_count,
            floor: self
                .floor
                .unwrap_or_else(|| execution_marker_from_header(&lowest_header)),
            anchor: self.anchor,
            extraction_elapsed: self.extraction_elapsed,
            write_elapsed: self.write_elapsed,
        })
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        let Some(buffer) = self.write_buffer.take() else {
            return Ok(());
        };
        let chunk = buffer.into_chunk();

        let chunk_outcome =
            write_extracted_historical_chunk(Arc::clone(&self.storage), chunk).await?;
        self.write_elapsed += chunk_outcome.write_elapsed;
        self.floor = chunk_outcome.floor;
        self.anchor = chunk_outcome.anchor;
        Ok(())
    }
}

impl SyncEngine {
    /// Publish validated forward data only while its captured terminal anchor
    /// remains admitted. All preparation precedes the consensus publication lock.
    pub(super) async fn publish_selected_forward_rows(
        &mut self,
        rows: &[LogRow],
        headers: &[Header],
        hashes: &[B256],
        anchor: &ExecutionAnchor,
    ) -> Result<bool> {
        eyre::ensure!(
            !headers.is_empty() && headers.len() == hashes.len(),
            "invalid forward publication batch"
        );
        let mut tip = self.head_tracker.tip();
        for (header, hash) in headers.iter().zip(hashes) {
            if let Some((number, parent_hash)) = tip {
                eyre::ensure!(
                    number.checked_add(1) == Some(header.number)
                        && parent_hash == header.parent_hash,
                    "verified forward header {} does not extend tracked canonical tip {}",
                    header.number,
                    number
                );
            }
            tip = Some((header.number, *hash));
        }
        let recent_headers = self.head_tracker.snapshot_with_append(headers);
        let last = headers.last().expect("nonempty forward publication");
        let indexed_anchor = (last.number == anchor.block_number).then_some(anchor);
        let storage = Arc::clone(&self.storage);
        let consensus = Arc::clone(&self.consensus);
        let mut storage = storage.write().await;
        let published = consensus
            .with_current_anchor(anchor, || -> Result<()> {
                storage
                    .ingest_canonical_batch(rows, last, &recent_headers, indexed_anchor)
                    .map_err(|error| eyre::eyre!("storage ingestion error: {error}"))?;
                // Continuity was checked above; only append new headers, without
                // restoring/rehashing the retained persistence window.
                for header in headers {
                    self.track_forward_header(header.clone())?;
                }
                Ok(())
            })
            .transpose()?
            .is_some();
        drop(storage);
        Ok(published)
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

    /// Advance only along the verified forward chain. Canonical reorgs must be
    /// reconciled against consensus through the durable storage transaction.
    pub(super) fn track_forward_header(&mut self, header: Header) -> Result<()> {
        if let Some((number, hash)) = self.head_tracker.tip() {
            eyre::ensure!(
                number.checked_add(1) == Some(header.number()) && hash == header.parent_hash(),
                "verified forward header {} does not extend tracked canonical tip {}",
                header.number(),
                number,
            );
        }
        // An empty tracker allows the initial authenticated checkpoint jump.
        // A populated tracker, including genesis, must always extend its tip.
        let reorg = self.head_tracker.track(header);
        debug_assert!(
            reorg.is_none(),
            "forward continuity was checked before tracking"
        );
        Ok(())
    }
}

pub(super) async fn write_validated_historical_blocks(
    storage: Arc<RwLock<PartitionManager>>,
    blocks: Vec<HistoricalValidatedBlock>,
) -> Result<HistoricalIngestOutcome> {
    let extracted = extract_validated_historical_blocks(blocks).await?;
    write_extracted_historical_batch(storage, extracted).await
}

pub(super) async fn extract_validated_historical_blocks(
    blocks: Vec<HistoricalValidatedBlock>,
) -> Result<HistoricalExtractedBatch> {
    if blocks.is_empty() {
        return Ok(HistoricalExtractedBatch { chunks: Vec::new() });
    }

    let mut chunks = Vec::new();

    let mut block_chunks = blocks.into_iter();
    let extraction_pipeline_depth = historical_extraction_pipeline_depth();
    let mut extraction_tasks = VecDeque::with_capacity(extraction_pipeline_depth);
    let mut write_buffer: Option<HistoricalExtractedChunk> = None;
    fill_historical_extraction_pipeline(
        &mut block_chunks,
        &mut extraction_tasks,
        extraction_pipeline_depth,
    );
    while let Some(extraction_task) = extraction_tasks.pop_front() {
        fill_historical_extraction_pipeline(
            &mut block_chunks,
            &mut extraction_tasks,
            extraction_pipeline_depth,
        );
        let extracted = extraction_task
            .await
            .map_err(|error| eyre::eyre!("historical extraction worker failed: {error}"))??;

        match write_buffer.as_mut() {
            Some(buffer) => buffer.merge(extracted),
            None => write_buffer = Some(extracted),
        }

        let should_write = write_buffer
            .as_ref()
            .is_some_and(historical_write_chunk_is_ready);
        if should_write {
            let extracted = write_buffer
                .take()
                .expect("historical write buffer is present when ready");
            chunks.push(extracted);
        }
    }

    if let Some(extracted) = write_buffer.take() {
        chunks.push(extracted);
    }

    Ok(HistoricalExtractedBatch { chunks })
}

pub(super) async fn write_extracted_historical_batch(
    storage: Arc<RwLock<PartitionManager>>,
    extracted: HistoricalExtractedBatch,
) -> Result<HistoricalIngestOutcome> {
    let mut writer = HistoricalBatchWriter::new(storage);
    let write_chunks = coalesce_historical_write_chunks(extracted.chunks);
    for chunk in write_chunks {
        writer.push_chunk(chunk).await?;
    }
    writer.finish().await
}

fn fill_historical_extraction_pipeline(
    blocks: &mut std::vec::IntoIter<HistoricalValidatedBlock>,
    extraction_tasks: &mut VecDeque<AbortOnDropHandle<Result<HistoricalExtractedChunk>>>,
    pipeline_depth: usize,
) {
    while extraction_tasks.len() < pipeline_depth {
        let Some(task) = next_historical_extract_task(blocks) else {
            break;
        };
        extraction_tasks.push_back(task);
    }
}

fn historical_extraction_pipeline_depth() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(HISTORICAL_EXTRACTION_PIPELINE_MIN_DEPTH)
        .clamp(
            HISTORICAL_EXTRACTION_PIPELINE_MIN_DEPTH,
            HISTORICAL_EXTRACTION_PIPELINE_MAX_DEPTH,
        )
}

fn next_historical_extract_task(
    blocks: &mut std::vec::IntoIter<HistoricalValidatedBlock>,
) -> Option<AbortOnDropHandle<Result<HistoricalExtractedChunk>>> {
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

    Some(AbortOnDropHandle::new(tokio::task::spawn_blocking(
        move || -> Result<HistoricalExtractedChunk> {
            let lowest_header = chunk
                .iter()
                .min_by_key(|block| block.header.number())
                .map(|block| block.header.clone())
                .expect("non-empty historical block chunk has a lowest header");

            let extraction_started = std::time::Instant::now();
            let block_count = chunk.len();
            let rows = collect_validated_historical_rows(chunk)?;
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
    )))
}

async fn write_extracted_historical_chunk(
    storage: Arc<RwLock<PartitionManager>>,
    extracted: HistoricalExtractedChunk,
) -> Result<HistoricalChunkWriteOutcome> {
    // Dropping the caller cancels work that has not started. Once the blocking
    // commit starts it runs to completion; ordinary shutdown observes that
    // result, and the node's existing deadlines bound forced runtime teardown.
    AbortOnDropHandle::new(tokio::task::spawn_blocking(
        move || -> Result<HistoricalChunkWriteOutcome> {
            let write_started = std::time::Instant::now();
            let mut storage = storage.blocking_write();
            storage
                .ingest_historical_batch(&extracted.rows, &extracted.lowest_header)
                .map_err(|e| eyre::eyre!("historical storage ingestion error: {e}"))?;
            Ok(HistoricalChunkWriteOutcome {
                floor: storage.historical_floor(),
                anchor: storage.historical_anchor(),
                write_elapsed: write_started.elapsed(),
            })
        },
    ))
    .await
    .map_err(|error| eyre::eyre!("historical storage worker failed: {error}"))?
}

fn coalesce_historical_write_chunks(
    chunks: Vec<HistoricalExtractedChunk>,
) -> Vec<HistoricalExtractedChunk> {
    coalesce_historical_write_chunks_with_row_limit(chunks, historical_write_chunk_row_limit())
}

fn coalesce_historical_write_chunks_with_row_limit(
    chunks: Vec<HistoricalExtractedChunk>,
    row_limit: u64,
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
            .is_some_and(|buffer| historical_write_chunk_is_ready_with_row_limit(buffer, row_limit))
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

fn historical_write_chunk_is_ready(buffer: &HistoricalExtractedChunk) -> bool {
    historical_write_chunk_is_ready_with_row_limit(buffer, historical_write_chunk_row_limit())
}

fn historical_write_chunk_is_ready_with_row_limit(
    buffer: &HistoricalExtractedChunk,
    row_limit: u64,
) -> bool {
    historical_write_chunk_counts_are_ready_with_row_limit(
        buffer.block_count,
        buffer.row_count,
        row_limit,
    )
}

fn historical_write_chunk_counts_are_ready(block_count: usize, row_count: u64) -> bool {
    historical_write_chunk_counts_are_ready_with_row_limit(
        block_count,
        row_count,
        historical_write_chunk_row_limit(),
    )
}

fn historical_write_chunk_counts_are_ready_with_row_limit(
    block_count: usize,
    row_count: u64,
    row_limit: u64,
) -> bool {
    block_count >= HISTORICAL_WRITE_CHUNK_BLOCKS || row_count >= row_limit
}

fn historical_write_chunk_row_limit() -> u64 {
    historical_write_chunk_row_limit_for_available_memory(super::memory::available_bytes())
}

fn historical_write_chunk_row_limit_for_available_memory(
    available_memory_bytes: Option<u64>,
) -> u64 {
    if available_memory_bytes
        .is_some_and(|bytes| bytes >= HISTORICAL_WRITE_CHUNK_HIGH_MEMORY_AVAILABLE_BYTES)
    {
        HISTORICAL_WRITE_CHUNK_HIGH_MEMORY_ROWS
    } else {
        HISTORICAL_WRITE_CHUNK_ROWS
    }
}

fn collect_validated_historical_rows(
    mut blocks: Vec<HistoricalValidatedBlock>,
) -> Result<Vec<LogRow>> {
    blocks.sort_unstable_by_key(|block| block.header.number());

    let total_rows = extract::checked_row_count(
        blocks
            .iter()
            .flat_map(|block| &block.receipts)
            .map(|receipt| receipt.logs().len()),
    )?;
    let mut rows = Vec::new();
    rows.try_reserve(total_rows)
        .map_err(|error| eyre::eyre!("reserve historical log rows: {error}"))?;
    for block in blocks {
        extract::append_from_body_receipts(
            &mut rows,
            block.header.number(),
            block.block_hash,
            block.header.timestamp(),
            &block.body,
            &block.receipts,
        )?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn extracted_chunk(block_count: usize, lowest_block: u64) -> HistoricalExtractedChunk {
        extracted_chunk_with_rows(block_count, 0, lowest_block)
    }

    fn extracted_chunk_with_rows(
        block_count: usize,
        row_count: u64,
        lowest_block: u64,
    ) -> HistoricalExtractedChunk {
        HistoricalExtractedChunk {
            rows: Vec::new(),
            row_count,
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
        let chunks = (0..16)
            .map(|index| extracted_chunk(128, (15 - index) * 128))
            .collect();

        let write_chunks =
            coalesce_historical_write_chunks_with_row_limit(chunks, HISTORICAL_WRITE_CHUNK_ROWS);

        assert_eq!(write_chunks.len(), 1);
        assert_eq!(write_chunks[0].block_count, HISTORICAL_WRITE_CHUNK_BLOCKS);
        assert_eq!(write_chunks[0].lowest_header.number(), 0);
    }

    #[test]
    fn keeps_partial_historical_write_chunk() {
        let chunks = vec![extracted_chunk(300, 700), extracted_chunk(300, 400)];

        let write_chunks =
            coalesce_historical_write_chunks_with_row_limit(chunks, HISTORICAL_WRITE_CHUNK_ROWS);

        assert_eq!(write_chunks.len(), 1);
        assert_eq!(write_chunks[0].block_count, 600);
        assert_eq!(write_chunks[0].lowest_header.number(), 400);
    }

    #[test]
    fn coalesces_dense_historical_chunks_by_row_count() {
        let chunks = vec![
            extracted_chunk_with_rows(128, 300_000, 900),
            extracted_chunk_with_rows(128, 250_000, 772),
            extracted_chunk_with_rows(128, 1, 644),
        ];

        let write_chunks =
            coalesce_historical_write_chunks_with_row_limit(chunks, HISTORICAL_WRITE_CHUNK_ROWS);

        assert_eq!(write_chunks.len(), 2);
        assert_eq!(write_chunks[0].block_count, 256);
        assert_eq!(write_chunks[0].row_count, 550_000);
        assert_eq!(write_chunks[0].lowest_header.number(), 772);
        assert_eq!(write_chunks[1].block_count, 128);
    }

    #[test]
    fn write_chunk_row_limit_scales_only_with_healthy_available_memory() {
        assert_eq!(
            historical_write_chunk_row_limit_for_available_memory(Some(
                HISTORICAL_WRITE_CHUNK_HIGH_MEMORY_AVAILABLE_BYTES
            )),
            HISTORICAL_WRITE_CHUNK_HIGH_MEMORY_ROWS
        );
        assert_eq!(
            historical_write_chunk_row_limit_for_available_memory(Some(
                HISTORICAL_WRITE_CHUNK_HIGH_MEMORY_AVAILABLE_BYTES - 1
            )),
            HISTORICAL_WRITE_CHUNK_ROWS
        );
        assert_eq!(
            historical_write_chunk_row_limit_for_available_memory(None),
            HISTORICAL_WRITE_CHUNK_ROWS
        );
    }
}

#[cfg(test)]
mod cancellation_controls {
    use super::*;
    use logex_storage::PartitionManagerConfig;

    #[test]
    fn dropping_a_queued_write_does_not_publish_its_floor() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(RwLock::new(
            PartitionManager::open(PartitionManagerConfig {
                data_dir: directory.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        ));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (release, blocked) = std::sync::mpsc::channel();
        let (started, observed) = tokio::sync::oneshot::channel();
        let waiting = runtime.spawn_blocking(move || {
            started.send(()).unwrap();
            blocked.recv().unwrap();
        });
        let was_pending = runtime.block_on(async {
            observed.await.unwrap();
            let chunk = HistoricalExtractedChunk {
                rows: Vec::new(),
                row_count: 0,
                block_count: 1,
                lowest_header: Header {
                    number: 100,
                    ..Default::default()
                },
                extraction_elapsed: Duration::ZERO,
            };
            let mut write = Box::pin(write_extracted_historical_chunk(
                Arc::clone(&storage),
                chunk,
            ));
            let was_pending = futures_util::poll!(write.as_mut()).is_pending();
            drop(write);
            was_pending
        });
        release.send(()).unwrap();
        runtime.block_on(waiting).unwrap();
        drop(runtime); // Drain started/queued blocking work before inspecting the result.
        let floor = storage.blocking_read().historical_floor();
        assert!(was_pending);
        assert_eq!(
            floor, None,
            "dropped queued work must not publish after cancellation"
        );
    }

    #[test]
    fn dropping_a_started_write_still_allows_atomic_completion() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Arc::new(RwLock::new(
            PartitionManager::open(PartitionManagerConfig {
                data_dir: directory.path().to_path_buf(),
                ..Default::default()
            })
            .unwrap(),
        ));
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        let (pending, started) = runtime.block_on(async {
            let reader = storage.read().await;
            let chunk = HistoricalExtractedChunk {
                rows: Vec::new(),
                row_count: 0,
                block_count: 1,
                lowest_header: Header {
                    number: 100,
                    ..Default::default()
                },
                extraction_elapsed: Duration::ZERO,
            };
            let mut write = Box::pin(write_extracted_historical_chunk(
                Arc::clone(&storage),
                chunk,
            ));
            let pending = futures_util::poll!(write.as_mut()).is_pending();
            // Tokio's fair lock refuses new readers once the blocking writer
            // is queued. This confirms the actual commit job has started.
            let started = tokio::time::timeout(Duration::from_secs(5), async {
                while storage.try_read().is_ok() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok();
            drop(write);
            drop(reader);
            (pending, started)
        });
        drop(runtime);
        assert!(pending && started);
        assert_eq!(
            storage
                .blocking_read()
                .historical_floor()
                .unwrap()
                .block_number,
            100
        );
    }

    #[tokio::test]
    async fn owned_extraction_retains_empty_block_count_and_floor() {
        let count = HISTORICAL_EXTRACT_CHUNK_BLOCKS + 1;
        let blocks = (1..=count)
            .rev()
            .map(|number| HistoricalValidatedBlock {
                index: count - number,
                header: Header {
                    number: number as u64,
                    ..Default::default()
                },
                block_hash: B256::ZERO,
                body_peer: PeerId::ZERO,
                body: Default::default(),
                receipt_peer: PeerId::ZERO,
                receipts: Vec::new(),
            })
            .collect();
        let extracted = extract_validated_historical_blocks(blocks).await.unwrap();
        assert_eq!(extracted.chunks.len(), 1);
        assert_eq!(extracted.chunks[0].block_count, count);
        assert_eq!(extracted.chunks[0].lowest_header.number, 1);
        assert_eq!(extracted.chunks[0].row_count, 0);
        assert!(extracted.chunks[0].rows.is_empty());
    }
}
