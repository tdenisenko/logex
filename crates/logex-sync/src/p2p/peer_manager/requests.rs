use alloy_eips::BlockHashOrNumber;
use eyre::{Result, bail};
use futures_util::{FutureExt, StreamExt};
use reth_primitives_traits::BlockBody as _;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{debug, trace, warn};

use crate::primitives::{
    LogexReceipt, ReceiptBloomCache, logex_receipt_batches_with_cached_blooms,
};

use super::*;

const PIPELINED_CHUNK_REQUEST_PEERS: usize = 3;
const PIPELINED_GAP_RETRY_ROUNDS: usize = 2;
const PIPELINED_BODY_RECEIPT_HEDGE_DELAY: Duration = Duration::from_secs(3);
const PIPELINED_BODY_RECEIPT_PLAN_TIMEOUT: Duration = Duration::from_secs(45);
const PIPELINED_BODY_RECEIPT_MAX_HEDGES: usize = 16;
const PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK: usize = 2;
const PIPELINED_BODY_RECEIPT_CHUNK_BLOCKS_DEFAULT: usize = 128;
const PIPELINED_BODY_RECEIPT_CHUNK_GAS_TARGET: u64 = 960_000_000;
const PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS: usize = 1024;
const PIPELINED_BODY_RECEIPT_MIN_ACCEPTED_PREFIX_BLOCKS: usize = 384;
const PIPELINED_BODY_RECEIPT_MAX_CONTIGUOUS_RETURN_BLOCKS: usize = 10_000;
const PIPELINED_BODY_RECEIPT_RETURN_GAS_PER_BLOCK_TARGET: u128 = 30_000_000;
const PARALLEL_CHUNK_RETRY_ROUNDS: usize = 2;
const PARALLEL_REQUESTS_PER_PEER: usize = 4;
const MAX_PARALLEL_BODY_RECEIPT_REQUESTS: usize = 128;
const MAX_PARALLEL_BODY_REQUESTS: usize = 64;
const MIN_PARALLEL_BODY_REQUEST_BLOCKS: usize = 64;
const MAX_PARALLEL_RECEIPT_REQUESTS: usize = 64;
const MIN_PARALLEL_RECEIPT_REQUEST_BLOCKS: usize = 64;

type ReceiptBatch = Vec<
    Vec<alloy_consensus::ReceiptWithBloom<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>>,
>;
type RequestStats = Vec<(PeerId, usize, Duration)>;
type TypedRequestStats = Vec<(PeerId, PeerRequestKind, usize, Duration)>;
type ParallelChunkFailures = Vec<ChunkRequestFailure>;
type ParallelChunkError = (ParallelChunkFailures, RequestStats);
type ParallelBodies = (Vec<SourcedBlockBody>, RequestStats, ParallelChunkFailures);
type ParallelReceipts = (PeerId, ReceiptBatch, RequestStats, ParallelChunkFailures);

#[derive(Debug, Clone)]
struct ChunkRequestFailure {
    role: ChunkRequestRole,
    peer_id: PeerId,
    requested: usize,
    kind: ChunkFailureKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ChunkRequestRole {
    Bodies,
    Receipts,
}

impl ChunkRequestRole {
    fn request_kind(self) -> PeerRequestKind {
        match self {
            Self::Bodies => PeerRequestKind::Bodies,
            Self::Receipts => PeerRequestKind::Receipts,
        }
    }
}

struct BodyReceiptChunk {
    start: usize,
    blocks: Vec<SourcedBodyReceipts>,
    failures: ParallelChunkFailures,
    stats: TypedRequestStats,
}

struct InFlightBodyReceiptChunk {
    range: std::ops::Range<usize>,
    chunk_index: usize,
    attempts: usize,
    last_hedged_at: Instant,
    hedges: usize,
}

pub(crate) struct BodyReceiptRequestPlan {
    hashes: Vec<B256>,
    ranges: Vec<std::ops::Range<usize>>,
    range_indices_by_start: HashMap<usize, usize>,
    return_blocks: usize,
    body_peer_ids: Vec<PeerId>,
    receipt_peer_ids: Vec<PeerId>,
    max_in_flight: usize,
    peers: HashMap<PeerId, RequestPeerSnapshot>,
}

pub(crate) struct BodyReceiptRequestOutcome {
    total_hashes: usize,
    return_blocks: usize,
    chunks: BTreeMap<usize, Vec<SourcedBodyReceipts>>,
    failures: ParallelChunkFailures,
    stats: TypedRequestStats,
}

pub(crate) struct BodyReceiptRequestCompletion {
    pub(crate) blocks: Vec<SourcedBodyReceipts>,
    pub(crate) planned_return_blocks: usize,
}

#[derive(Clone)]
struct RequestPeerSnapshot {
    sender: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    version: EthVersion,
}

#[derive(Debug, Clone)]
enum ChunkFailureKind {
    Request(RequestAttempt),
    Incomplete { returned: usize },
    ReceiptCountMismatch(ReceiptCountMismatch),
}

impl PeerManager {
    /// Request block headers starting at `start_block` for `count` blocks.
    pub async fn get_headers(
        &mut self,
        start_block: u64,
        count: u64,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        let request = HeadersRequest::rising(start_block.into(), count);
        self.get_headers_from_peers(request, Some(start_block))
            .await
    }

    /// Request block headers ending at `start_block` in descending block order.
    pub async fn get_headers_reverse(
        &mut self,
        start: BlockHashOrNumber,
        count: u64,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        let required_block = match start {
            BlockHashOrNumber::Number(block) => Some(block),
            BlockHashOrNumber::Hash(_) => None,
        };
        let request = HeadersRequest::falling(start, count);
        self.get_headers_from_peers(request, required_block).await
    }

    /// Request a single block header by hash or number.
    pub async fn get_header(
        &mut self,
        id: BlockHashOrNumber,
    ) -> Result<(
        PeerId,
        Option<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        let required_block = match id {
            BlockHashOrNumber::Number(block) => Some(block),
            BlockHashOrNumber::Hash(_) => None,
        };
        let (peer_id, mut headers) = self
            .get_headers_from_peers(HeadersRequest::one(id), required_block)
            .await?;
        Ok((peer_id, headers.pop()))
    }

    /// Request a single block header by hash.
    pub async fn get_header_by_hash(
        &mut self,
        hash: B256,
    ) -> Result<(
        PeerId,
        Option<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        self.get_header(BlockHashOrNumber::Hash(hash)).await
    }

    /// Request block bodies for the given block hashes.
    pub async fn get_bodies(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
    ) -> Result<Vec<SourcedBlockBody>> {
        self.get_bodies_prefer_peers(hashes, required_block, &[])
            .await
    }

    /// Request block bodies while trying the supplied proven peers before the
    /// regular rotating candidate set.
    pub async fn get_bodies_prefer_peers(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Result<Vec<SourcedBlockBody>> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let mut remaining_hashes = hashes.clone();
        let mut collected = Vec::with_capacity(hashes.len());
        let mut peer_ids = self
            .peer_ids_for_block_requests(Some(required_block), preferred_peers)
            .await;
        self.filter_paused_request_peers(&mut peer_ids, PeerRequestKind::Bodies);
        self.sort_peer_ids_by_request_performance(&mut peer_ids, PeerRequestKind::Bodies);
        let mut dead_peers = HashSet::new();

        match self
            .request_bodies_parallel_chunks(&peer_ids, remaining_hashes.clone())
            .await
        {
            Ok(Some((bodies, stats, failures))) => {
                for (peer_id, blocks, elapsed) in stats {
                    self.record_peer_request_success(
                        peer_id,
                        PeerRequestKind::Bodies,
                        blocks,
                        elapsed,
                    );
                }
                self.apply_parallel_chunk_failures("block bodies", failures, &mut dead_peers);
                self.remove_dead_peers(&dead_peers);
                self.advance_request_cursor();
                return Ok(bodies);
            }
            Ok(None) => {}
            Err((failures, stats)) => {
                for (peer_id, blocks, elapsed) in stats {
                    self.record_peer_request_success(
                        peer_id,
                        PeerRequestKind::Bodies,
                        blocks,
                        elapsed,
                    );
                }
                self.apply_parallel_chunk_failures("block bodies", failures, &mut dead_peers);
                self.remove_dead_peers(&dead_peers);
                peer_ids = self
                    .peer_ids_for_block_requests(Some(required_block), preferred_peers)
                    .await;
                self.filter_paused_request_peers(&mut peer_ids, PeerRequestKind::Bodies);
                self.sort_peer_ids_by_request_performance(&mut peer_ids, PeerRequestKind::Bodies);
            }
        }

        for peer_id in peer_ids {
            while !remaining_hashes.is_empty() {
                let request_hashes = remaining_hashes.clone();
                let started_at = Instant::now();
                let bodies = match self.request_bodies(peer_id, request_hashes.clone()).await {
                    Ok(bodies) => bodies,
                    Err(error) => {
                        let should_drop =
                            self.on_request_error(peer_id, PeerRequestKind::Bodies, &error);
                        debug!(peer = %peer_id, ?error, "block body request failed");
                        if should_drop {
                            dead_peers.insert(peer_id);
                        }
                        break;
                    }
                };

                match classify_response_progress(request_hashes.len(), bodies.len()) {
                    ResponseProgress::Complete => {
                        self.record_peer_request_success(
                            peer_id,
                            PeerRequestKind::Bodies,
                            bodies.len(),
                            started_at.elapsed(),
                        );
                        self.advance_request_cursor();
                        collected.extend(bodies.into_iter().map(|body| (peer_id, body)));
                        self.remove_dead_peers(&dead_peers);
                        return Ok(collected);
                    }
                    ResponseProgress::Partial { returned } => {
                        self.record_peer_request_success(
                            peer_id,
                            PeerRequestKind::Bodies,
                            returned,
                            started_at.elapsed(),
                        );
                        self.on_partial_response(
                            peer_id,
                            PeerRequestKind::Bodies,
                            "block bodies",
                            request_hashes.len(),
                            returned,
                        );
                        collected.extend(bodies.into_iter().map(|body| (peer_id, body)));
                        remaining_hashes = request_hashes[returned..].to_vec();
                    }
                    ResponseProgress::Empty => {
                        self.on_zero_progress_response(
                            peer_id,
                            PeerRequestKind::Bodies,
                            "block bodies",
                            request_hashes.len(),
                        );
                        break;
                    }
                    ResponseProgress::Overflow { returned } => {
                        self.on_invalid_response_length(
                            peer_id,
                            "block bodies",
                            request_hashes.len(),
                            returned,
                        );
                        dead_peers.insert(peer_id);
                        break;
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        if remaining_hashes.is_empty() {
            Ok(collected)
        } else {
            bail!("no peers available to handle block body request")
        }
    }

    /// Request bodies and matching receipts as independently pipelined chunks.
    ///
    /// This is used by reverse historical sync, where a validated header window
    /// can be split across peers and each chunk can move from bodies to receipts
    /// without waiting for every other body chunk to complete.
    pub async fn get_bodies_and_receipts_prefer_peers(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Result<Option<Vec<SourcedBodyReceipts>>> {
        if hashes.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let Some(plan) = self
            .prepare_bodies_and_receipts_request(hashes, required_block, preferred_peers)
            .await?
        else {
            return Ok(None);
        };
        let outcome = plan.execute().await;
        self.complete_bodies_and_receipts_request(outcome)
            .map(|completion| completion.map(|completion| completion.blocks))
    }

    pub(crate) async fn prepare_bodies_and_receipts_request(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Result<Option<BodyReceiptRequestPlan>> {
        self.prepare_bodies_and_receipts_request_inner(
            hashes,
            None,
            required_block,
            preferred_peers,
        )
        .await
    }

    pub(crate) async fn prepare_bodies_and_receipts_request_for_hashes_and_gas(
        &mut self,
        hashes: Vec<B256>,
        gas_used: Vec<u64>,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Result<Option<BodyReceiptRequestPlan>> {
        self.prepare_bodies_and_receipts_request_inner(
            hashes,
            Some(gas_used),
            required_block,
            preferred_peers,
        )
        .await
    }

    async fn prepare_bodies_and_receipts_request_inner(
        &mut self,
        hashes: Vec<B256>,
        receipt_gas_used: Option<Vec<u64>>,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Result<Option<BodyReceiptRequestPlan>> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok(None);
        }
        if hashes.len() < MIN_PARALLEL_BODY_REQUEST_BLOCKS {
            return Ok(None);
        }

        let mut body_peer_ids = self
            .peer_ids_for_block_requests(Some(required_block), preferred_peers)
            .await;
        self.filter_paused_request_peers(&mut body_peer_ids, PeerRequestKind::Bodies);
        self.sort_peer_ids_by_request_performance(&mut body_peer_ids, PeerRequestKind::Bodies);
        if body_peer_ids.is_empty() {
            return Ok(None);
        }

        let mut receipt_peer_ids = self
            .peer_ids_for_receipt_requests(required_block, preferred_peers)
            .await;
        self.filter_paused_request_peers(&mut receipt_peer_ids, PeerRequestKind::Receipts);
        self.sort_peer_ids_by_request_performance(&mut receipt_peer_ids, PeerRequestKind::Receipts);
        if receipt_peer_ids.is_empty() {
            return Ok(None);
        }

        let ranges = self.body_receipt_chunk_ranges(
            hashes.len(),
            &body_peer_ids,
            &receipt_peer_ids,
            receipt_gas_used.as_deref(),
        );
        if ranges.len() < 2 {
            return Ok(None);
        }
        let return_blocks = body_receipt_return_blocks(hashes.len(), receipt_gas_used.as_deref());
        let range_indices_by_start: HashMap<usize, usize> = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect();
        let max_in_flight = paired_body_receipt_chunk_window_limit(
            body_peer_ids.len().min(receipt_peer_ids.len()),
            MAX_PARALLEL_BODY_RECEIPT_REQUESTS,
        );
        if max_in_flight == 0 {
            return Ok(None);
        }

        let peers = body_peer_ids
            .iter()
            .chain(receipt_peer_ids.iter())
            .filter_map(|peer_id| {
                self.peers.get(peer_id).map(|peer| {
                    (
                        *peer_id,
                        RequestPeerSnapshot {
                            sender: peer.sender.clone(),
                            version: peer.version,
                        },
                    )
                })
            })
            .collect();

        Ok(Some(BodyReceiptRequestPlan {
            hashes,
            ranges,
            range_indices_by_start,
            return_blocks,
            body_peer_ids,
            receipt_peer_ids,
            max_in_flight,
            peers,
        }))
    }

    pub(crate) fn complete_bodies_and_receipts_request(
        &mut self,
        outcome: BodyReceiptRequestOutcome,
    ) -> Result<Option<BodyReceiptRequestCompletion>> {
        let BodyReceiptRequestOutcome {
            total_hashes,
            return_blocks,
            chunks,
            failures,
            stats,
        } = outcome;
        let mut dead_peers = HashSet::new();
        for (peer_id, kind, blocks, elapsed) in stats {
            self.record_peer_request_success(peer_id, kind, blocks, elapsed);
        }
        self.apply_parallel_chunk_failures("body/receipt chunks", failures, &mut dead_peers);
        self.remove_dead_peers(&dead_peers);

        let blocks = take_contiguous_body_receipt_prefix(return_blocks, chunks);

        let min_accepted_prefix = body_receipt_min_accepted_prefix(return_blocks);
        if blocks.len() >= min_accepted_prefix {
            self.advance_request_cursor();
            Ok(Some(BodyReceiptRequestCompletion {
                blocks,
                planned_return_blocks: return_blocks,
            }))
        } else {
            bail!(
                "body/receipt chunk pipeline completed {}/{} blocks below accepted prefix {}",
                blocks.len(),
                total_hashes,
                min_accepted_prefix
            )
        }
    }
}

impl BodyReceiptRequestPlan {
    pub(crate) fn return_blocks(&self) -> usize {
        self.return_blocks
    }

    pub(crate) async fn execute(self) -> BodyReceiptRequestOutcome {
        let plan_started_at = Instant::now();
        let mut chunks = BTreeMap::new();
        let mut failures = Vec::new();
        let mut stats = Vec::new();
        {
            let mut attempts = futures_util::stream::FuturesUnordered::new();
            let mut pending_ranges =
                self.ranges
                    .iter()
                    .cloned()
                    .enumerate()
                    .filter_map(|(chunk_index, range)| {
                        body_receipt_prefix_range(range, self.return_blocks)
                            .map(|range| (chunk_index, range))
                    });
            let mut retry_counts = HashMap::<usize, usize>::new();
            let mut in_flight = HashMap::<usize, InFlightBodyReceiptChunk>::new();
            let mut body_bad_peers = HashSet::<PeerId>::new();
            let mut receipt_bad_peers = HashSet::<PeerId>::new();
            let mut hedge_count = 0usize;
            let min_return_blocks = self.return_blocks;
            let max_scheduled_chunks =
                body_receipt_scheduled_chunk_limit(&self.ranges, min_return_blocks)
                    .min(self.max_in_flight);
            for _ in 0..max_scheduled_chunks {
                let Some((chunk_index, range)) = pending_ranges.next() else {
                    break;
                };
                let chunk_body_peer_ids = peer_ids_excluding(&self.body_peer_ids, &body_bad_peers);
                let chunk_receipt_peer_ids =
                    peer_ids_excluding(&self.receipt_peer_ids, &receipt_bad_peers);
                schedule_body_receipt_chunk_attempt(
                    &self,
                    &mut attempts,
                    &mut in_flight,
                    range,
                    chunk_index,
                    chunk_body_peer_ids,
                    chunk_receipt_peer_ids,
                );
            }

            while !attempts.is_empty() {
                let Some(wait_timeout) =
                    PIPELINED_BODY_RECEIPT_PLAN_TIMEOUT.checked_sub(plan_started_at.elapsed())
                else {
                    debug!(
                        elapsed_ms = plan_started_at.elapsed().as_millis(),
                        chunks = chunks.len(),
                        in_flight = in_flight.len(),
                        "body/receipt chunk pipeline hit plan timeout"
                    );
                    break;
                };
                let chunk = match timeout(
                    wait_timeout.min(PIPELINED_BODY_RECEIPT_HEDGE_DELAY),
                    attempts.next(),
                )
                .await
                {
                    Ok(Some(chunk)) => chunk,
                    Ok(None) => break,
                    Err(_) => {
                        if plan_started_at.elapsed() >= PIPELINED_BODY_RECEIPT_PLAN_TIMEOUT {
                            debug!(
                                elapsed_ms = plan_started_at.elapsed().as_millis(),
                                chunks = chunks.len(),
                                in_flight = in_flight.len(),
                                "body/receipt chunk pipeline hit plan timeout"
                            );
                            break;
                        }
                        if hedge_count < PIPELINED_BODY_RECEIPT_MAX_HEDGES
                            && let Some((range, chunk_index)) = body_receipt_hedge_candidate(
                                &mut in_flight,
                                &chunks,
                                min_return_blocks,
                                Instant::now(),
                            )
                        {
                            let chunk_body_peer_ids =
                                peer_ids_excluding(&self.body_peer_ids, &body_bad_peers);
                            let chunk_receipt_peer_ids =
                                peer_ids_excluding(&self.receipt_peer_ids, &receipt_bad_peers);
                            schedule_body_receipt_chunk_attempt(
                                &self,
                                &mut attempts,
                                &mut in_flight,
                                range,
                                chunk_index
                                    + ((PIPELINED_GAP_RETRY_ROUNDS + 1 + hedge_count)
                                        * self.ranges.len()),
                                chunk_body_peer_ids,
                                chunk_receipt_peer_ids,
                            );
                            hedge_count += 1;
                        }
                        continue;
                    }
                };
                let chunk_start = chunk.start;
                complete_body_receipt_chunk_attempt(&mut in_flight, chunk_start);
                let chunk_already_completed = chunks.contains_key(&chunk_start);
                let chunk_failed = chunk.blocks.is_empty();
                if !chunk_already_completed {
                    for failure in &chunk.failures {
                        if chunk_failure_disables_role_peer(failure) {
                            match failure.role {
                                ChunkRequestRole::Bodies => {
                                    body_bad_peers.insert(failure.peer_id);
                                }
                                ChunkRequestRole::Receipts => {
                                    receipt_bad_peers.insert(failure.peer_id);
                                }
                            }
                        }
                    }
                    failures.extend(chunk.failures);
                }
                stats.extend(chunk.stats);
                if !chunk_failed && !chunk_already_completed {
                    chunks.insert(chunk_start, chunk.blocks);
                }

                let contiguous_blocks = contiguous_chunk_blocks(&chunks);
                if contiguous_blocks >= min_return_blocks || contiguous_blocks == self.hashes.len()
                {
                    break;
                }

                if chunk_failed
                    && chunk_start <= contiguous_blocks
                    && !in_flight.contains_key(&chunk_start)
                    && retry_counts.get(&chunk_start).copied().unwrap_or_default()
                        < PIPELINED_GAP_RETRY_ROUNDS
                    && let Some(base_chunk_index) =
                        self.range_indices_by_start.get(&chunk_start).copied()
                    && let Some(range) = self
                        .ranges
                        .iter()
                        .find(|range| range.start == chunk_start)
                        .cloned()
                        .and_then(|range| body_receipt_prefix_range(range, min_return_blocks))
                {
                    let retry_count = retry_counts.entry(chunk_start).or_default();
                    *retry_count += 1;
                    let chunk_index = base_chunk_index + (*retry_count * self.ranges.len());
                    let chunk_body_peer_ids =
                        peer_ids_excluding(&self.body_peer_ids, &body_bad_peers);
                    let chunk_receipt_peer_ids =
                        peer_ids_excluding(&self.receipt_peer_ids, &receipt_bad_peers);
                    schedule_body_receipt_chunk_attempt(
                        &self,
                        &mut attempts,
                        &mut in_flight,
                        range,
                        chunk_index,
                        chunk_body_peer_ids,
                        chunk_receipt_peer_ids,
                    );
                }

                while attempts.len() < max_scheduled_chunks {
                    if contiguous_chunk_blocks(&chunks) >= min_return_blocks {
                        break;
                    }
                    let Some((chunk_index, range)) = pending_ranges.next() else {
                        break;
                    };
                    let chunk_body_peer_ids =
                        peer_ids_excluding(&self.body_peer_ids, &body_bad_peers);
                    let chunk_receipt_peer_ids =
                        peer_ids_excluding(&self.receipt_peer_ids, &receipt_bad_peers);
                    schedule_body_receipt_chunk_attempt(
                        &self,
                        &mut attempts,
                        &mut in_flight,
                        range,
                        chunk_index,
                        chunk_body_peer_ids,
                        chunk_receipt_peer_ids,
                    );
                }

                while hedge_count < PIPELINED_BODY_RECEIPT_MAX_HEDGES {
                    let Some((range, chunk_index)) = body_receipt_hedge_candidate(
                        &mut in_flight,
                        &chunks,
                        min_return_blocks,
                        Instant::now(),
                    ) else {
                        break;
                    };
                    let chunk_body_peer_ids =
                        peer_ids_excluding(&self.body_peer_ids, &body_bad_peers);
                    let chunk_receipt_peer_ids =
                        peer_ids_excluding(&self.receipt_peer_ids, &receipt_bad_peers);
                    schedule_body_receipt_chunk_attempt(
                        &self,
                        &mut attempts,
                        &mut in_flight,
                        range,
                        chunk_index
                            + ((PIPELINED_GAP_RETRY_ROUNDS + 1 + hedge_count) * self.ranges.len()),
                        chunk_body_peer_ids,
                        chunk_receipt_peer_ids,
                    );
                    hedge_count += 1;
                }
            }
        }

        BodyReceiptRequestOutcome {
            total_hashes: self.hashes.len(),
            return_blocks: self.return_blocks,
            chunks,
            failures,
            stats,
        }
    }

    async fn request_body_receipt_chunk(
        &self,
        start: usize,
        hashes: Vec<B256>,
        body_peer_ids: Vec<PeerId>,
        receipt_peer_ids: Vec<PeerId>,
    ) -> BodyReceiptChunk {
        let mut failures = Vec::new();
        let mut stats = Vec::new();
        let mut cached_receipts: Option<(PeerId, ReceiptBatch)> = None;
        let body_candidates = body_peer_ids
            .into_iter()
            .take(PIPELINED_CHUNK_REQUEST_PEERS)
            .collect::<Vec<_>>();

        for body_peer in body_candidates {
            let receipt_candidates = receipt_candidates_for_body_peer(
                receipt_peer_ids.clone(),
                body_peer,
                PIPELINED_CHUNK_REQUEST_PEERS,
            );
            let Some(first_receipt_peer) = receipt_candidates.first().copied() else {
                failures.push(ChunkRequestFailure {
                    role: ChunkRequestRole::Bodies,
                    peer_id: body_peer,
                    requested: hashes.len(),
                    kind: ChunkFailureKind::Request(RequestAttempt::Disconnected),
                });
                continue;
            };
            let has_cached_receipts = cached_receipts.is_some();
            let fallback_receipt_candidates = receipt_candidates
                .into_iter()
                .skip(if has_cached_receipts { 0 } else { 1 });

            let body_hashes = hashes.clone();
            let (body_elapsed, body_result, receipt_result) = if has_cached_receipts {
                let started_at = Instant::now();
                let result = self
                    .request_bodies_until_complete(body_peer, body_hashes)
                    .await;
                (started_at.elapsed(), result, None)
            } else {
                let receipt_hashes = hashes.clone();
                let body_request = async {
                    let started_at = Instant::now();
                    let result = self
                        .request_bodies_until_complete(body_peer, body_hashes)
                        .await;
                    (started_at.elapsed(), result)
                };
                let receipt_request = async {
                    let started_at = Instant::now();
                    let result = self
                        .request_receipts_until_complete(first_receipt_peer, receipt_hashes, None)
                        .await;
                    (started_at.elapsed(), result)
                };
                let ((body_elapsed, body_result), (receipt_elapsed, receipt_result)) =
                    tokio::join!(body_request, receipt_request);
                (
                    body_elapsed,
                    body_result,
                    Some((first_receipt_peer, receipt_elapsed, receipt_result)),
                )
            };

            let bodies = match body_result {
                Ok(bodies) => {
                    stats.push((
                        body_peer,
                        PeerRequestKind::Bodies,
                        bodies.len(),
                        body_elapsed,
                    ));
                    bodies
                        .into_iter()
                        .map(|body| (body_peer, body))
                        .collect::<Vec<_>>()
                }
                Err(kind) => {
                    failures.push(ChunkRequestFailure {
                        role: ChunkRequestRole::Bodies,
                        peer_id: body_peer,
                        requested: hashes.len(),
                        kind,
                    });
                    match receipt_result {
                        Some((receipt_peer, receipt_elapsed, Ok(receipts))) => {
                            stats.push((
                                receipt_peer,
                                PeerRequestKind::Receipts,
                                receipts.len(),
                                receipt_elapsed,
                            ));
                            cached_receipts.get_or_insert((receipt_peer, receipts));
                        }
                        Some((receipt_peer, _receipt_elapsed, Err(kind))) => {
                            failures.push(ChunkRequestFailure {
                                role: ChunkRequestRole::Receipts,
                                peer_id: receipt_peer,
                                requested: hashes.len(),
                                kind,
                            });
                        }
                        None => {}
                    }
                    continue;
                }
            };

            let expected_receipt_counts = bodies
                .iter()
                .map(|(_, body)| body.transaction_count())
                .collect::<Vec<_>>();

            let mut skip_fallback_receipt_peer = None;
            if let Some((receipt_peer, receipts)) = cached_receipts.take() {
                match body_receipt_blocks_if_counts_match(
                    &bodies,
                    receipt_peer,
                    receipts,
                    hashes.len(),
                ) {
                    Ok(blocks) => {
                        return BodyReceiptChunk {
                            start,
                            blocks,
                            failures,
                            stats,
                        };
                    }
                    Err(kind) => {
                        skip_fallback_receipt_peer = Some(receipt_peer);
                        failures.push(ChunkRequestFailure {
                            role: ChunkRequestRole::Receipts,
                            peer_id: receipt_peer,
                            requested: hashes.len(),
                            kind: ChunkFailureKind::ReceiptCountMismatch(kind),
                        });
                    }
                }
            }

            match receipt_result {
                Some((receipt_peer, receipt_elapsed, Ok(receipts))) => {
                    stats.push((
                        receipt_peer,
                        PeerRequestKind::Receipts,
                        receipts.len(),
                        receipt_elapsed,
                    ));
                    match body_receipt_blocks_if_counts_match(
                        &bodies,
                        receipt_peer,
                        receipts,
                        hashes.len(),
                    ) {
                        Ok(blocks) => {
                            return BodyReceiptChunk {
                                start,
                                blocks,
                                failures,
                                stats,
                            };
                        }
                        Err(kind) => failures.push(ChunkRequestFailure {
                            role: ChunkRequestRole::Receipts,
                            peer_id: receipt_peer,
                            requested: hashes.len(),
                            kind: ChunkFailureKind::ReceiptCountMismatch(kind),
                        }),
                    }
                }
                Some((receipt_peer, _receipt_elapsed, Err(kind))) => {
                    failures.push(ChunkRequestFailure {
                        role: ChunkRequestRole::Receipts,
                        peer_id: receipt_peer,
                        requested: hashes.len(),
                        kind,
                    })
                }
                None => {}
            }

            for receipt_peer in fallback_receipt_candidates {
                if skip_fallback_receipt_peer == Some(receipt_peer) {
                    continue;
                }
                let started_at = Instant::now();
                match self
                    .request_receipts_until_complete(
                        receipt_peer,
                        hashes.clone(),
                        Some(expected_receipt_counts.clone()),
                    )
                    .await
                {
                    Ok(receipts) => {
                        stats.push((
                            receipt_peer,
                            PeerRequestKind::Receipts,
                            receipts.len(),
                            started_at.elapsed(),
                        ));
                        match body_receipt_blocks_if_counts_match(
                            &bodies,
                            receipt_peer,
                            receipts,
                            hashes.len(),
                        ) {
                            Ok(blocks) => {
                                return BodyReceiptChunk {
                                    start,
                                    blocks,
                                    failures,
                                    stats,
                                };
                            }
                            Err(kind) => failures.push(ChunkRequestFailure {
                                role: ChunkRequestRole::Receipts,
                                peer_id: receipt_peer,
                                requested: hashes.len(),
                                kind: ChunkFailureKind::ReceiptCountMismatch(kind),
                            }),
                        }
                    }
                    Err(kind) => failures.push(ChunkRequestFailure {
                        role: ChunkRequestRole::Receipts,
                        peer_id: receipt_peer,
                        requested: hashes.len(),
                        kind,
                    }),
                }
            }
        }

        BodyReceiptChunk {
            start,
            blocks: Vec::new(),
            failures,
            stats,
        }
    }

    async fn request_bodies_until_complete(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        ChunkFailureKind,
    > {
        let mut remaining_hashes = hashes;
        let mut bodies = Vec::with_capacity(remaining_hashes.len());

        while !remaining_hashes.is_empty() {
            let request_hashes = remaining_hashes.clone();
            let response = self
                .request_bodies(peer_id, request_hashes.clone())
                .await
                .map_err(ChunkFailureKind::Request)?;

            match classify_response_progress(request_hashes.len(), response.len()) {
                ResponseProgress::Complete => {
                    bodies.extend(response);
                    return Ok(bodies);
                }
                ResponseProgress::Partial { returned } => {
                    bodies.extend(response);
                    remaining_hashes = request_hashes[returned..].to_vec();
                }
                ResponseProgress::Empty => {
                    return Err(ChunkFailureKind::Incomplete { returned: 0 });
                }
                ResponseProgress::Overflow { returned } => {
                    return Err(ChunkFailureKind::Incomplete { returned });
                }
            }
        }

        Ok(bodies)
    }

    async fn request_receipts_until_complete(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
        expected_receipt_counts: Option<Vec<usize>>,
    ) -> std::result::Result<ReceiptBatch, ChunkFailureKind> {
        let Some(version) = self.peers.get(&peer_id).map(|peer| peer.version) else {
            return Err(ChunkFailureKind::Request(RequestAttempt::Disconnected));
        };

        if expected_receipt_counts
            .as_ref()
            .is_some_and(|expected| expected.len() != hashes.len())
        {
            return Err(ChunkFailureKind::Request(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::BadResponse,
            )));
        }

        if version >= EthVersion::Eth70 {
            let receipts = self
                .request_receipts70(peer_id, hashes.clone())
                .await
                .map_err(ChunkFailureKind::Request)?;
            validate_receipt_response_counts(
                "receipts70",
                hashes.len(),
                &receipts,
                expected_receipt_counts.as_deref(),
            )
            .map_err(ChunkFailureKind::ReceiptCountMismatch)?;
            return Ok(receipts);
        }

        let mut remaining_hashes = hashes.clone();
        let mut receipts = Vec::with_capacity(hashes.len());
        let mut offset = 0usize;

        while !remaining_hashes.is_empty() {
            let request_hashes = remaining_hashes.clone();
            let response = if version >= EthVersion::Eth69 {
                self.request_receipts69(peer_id, request_hashes.clone())
                    .await
            } else {
                self.request_receipts(peer_id, request_hashes.clone()).await
            }
            .map_err(ChunkFailureKind::Request)?;

            match classify_response_progress(request_hashes.len(), response.len()) {
                ResponseProgress::Complete => {
                    let returned = response.len();
                    validate_receipt_response_counts(
                        "receipts",
                        returned,
                        &response,
                        expected_receipt_counts
                            .as_deref()
                            .map(|expected| &expected[offset..offset + returned]),
                    )
                    .map_err(ChunkFailureKind::ReceiptCountMismatch)?;
                    receipts.extend(response);
                    return Ok(receipts);
                }
                ResponseProgress::Partial { returned } => {
                    validate_receipt_response_counts(
                        "receipts",
                        returned,
                        &response,
                        expected_receipt_counts
                            .as_deref()
                            .map(|expected| &expected[offset..offset + returned]),
                    )
                    .map_err(ChunkFailureKind::ReceiptCountMismatch)?;
                    receipts.extend(response);
                    offset += returned;
                    remaining_hashes = request_hashes[returned..].to_vec();
                }
                ResponseProgress::Empty => {
                    return Err(ChunkFailureKind::Incomplete { returned: 0 });
                }
                ResponseProgress::Overflow { returned } => {
                    return Err(ChunkFailureKind::Incomplete { returned });
                }
            }
        }

        Ok(receipts)
    }

    async fn request_bodies(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetBlockBodies {
            request: GetBlockBodies(hashes.clone()),
            response,
        })
        .await
    }

    async fn request_receipts(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts {
            request: GetReceipts(hashes.clone()),
            response,
        })
        .await
    }

    async fn request_receipts69(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let receipts: Vec<Vec<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>> = self
            .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts69 {
                request: GetReceipts(hashes.clone()),
                response,
            })
            .await?;
        let mut bloom_cache = ReceiptBloomCache::default();
        Ok(logex_receipt_batches_with_cached_blooms(
            receipts,
            &mut bloom_cache,
        ))
    }

    async fn request_receipts70(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let mut merged = Vec::with_capacity(hashes.len());
        let mut next_block_index = 0usize;
        let mut first_block_receipt_index = 0u64;
        let mut bloom_cache = ReceiptBloomCache::default();

        while next_block_index < hashes.len() {
            let request_hashes = hashes[next_block_index..].to_vec();
            let response: Receipts70<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt> = self
                .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts70 {
                    request: GetReceipts70 {
                        first_block_receipt_index,
                        block_hashes: request_hashes.clone(),
                    },
                    response,
                })
                .await?;

            let (updated_block_index, updated_receipt_index) = merge_receipts70_response(
                &mut merged,
                next_block_index,
                first_block_receipt_index,
                response,
                hashes.len(),
                &mut bloom_cache,
            )
            .map_err(Receipts70MergeError::into_request_attempt)?;

            next_block_index = updated_block_index;
            first_block_receipt_index = updated_receipt_index;
        }

        Ok(merged)
    }

    async fn request_with_channel<T, W, MakeRequest>(
        &self,
        peer_id: PeerId,
        make_request: &MakeRequest,
    ) -> std::result::Result<T, RequestAttempt>
    where
        W: IntoResponseValue<T>,
        MakeRequest: Fn(
            oneshot::Sender<reth_network::p2p::error::RequestResult<W>>,
        ) -> PeerRequest<LogexNetworkPrimitives>,
    {
        let Some(peer) = self.peers.get(&peer_id) else {
            return Err(RequestAttempt::Disconnected);
        };

        let sender = peer.sender.clone();
        let (response_tx, response_rx) = oneshot::channel();
        sender
            .to_session_tx
            .send(make_request(response_tx))
            .await
            .map_err(|_| RequestAttempt::Disconnected)?;

        match timeout(REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(Ok(response))) => Ok(response.into_value()),
            Ok(Ok(Err(error))) => Err(RequestAttempt::Request(error)),
            Ok(Err(_)) => Err(RequestAttempt::Disconnected),
            Err(_) => Err(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::Timeout,
            )),
        }
    }
}

impl PeerManager {
    async fn get_headers_from_peers(
        &mut self,
        request: HeadersRequest,
        required_block: Option<u64>,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        self.drain_events_now();

        let mut peer_ids = self.peer_ids_for_block_requests(required_block, &[]).await;
        self.sort_peer_ids_by_request_performance(&mut peer_ids, PeerRequestKind::Headers);
        let mut dead_peers = HashSet::new();
        let mut saw_empty_response = false;

        for peer_id in peer_ids {
            let started_at = Instant::now();
            match self.request_headers(peer_id, request.clone()).await {
                Ok(headers) => {
                    if headers.len() > request.limit as usize {
                        self.on_invalid_response_length(
                            peer_id,
                            "headers",
                            request.limit as usize,
                            headers.len(),
                        );
                        dead_peers.insert(peer_id);
                        continue;
                    }
                    if headers.is_empty() && request.limit > 0 {
                        saw_empty_response = true;
                        self.on_zero_progress_response(
                            peer_id,
                            PeerRequestKind::Headers,
                            "headers",
                            request.limit as usize,
                        );
                        continue;
                    }
                    self.record_peer_request_success(
                        peer_id,
                        PeerRequestKind::Headers,
                        headers.len(),
                        started_at.elapsed(),
                    );
                    self.advance_request_cursor();
                    self.remove_dead_peers(&dead_peers);
                    return Ok((peer_id, headers));
                }
                Err(error) => {
                    let should_drop =
                        self.on_request_error(peer_id, PeerRequestKind::Headers, &error);
                    debug!(peer = %peer_id, ?error, "header request failed");
                    if should_drop {
                        dead_peers.insert(peer_id);
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        if saw_empty_response {
            return Ok((PeerId::ZERO, Vec::new()));
        }
        bail!("no peers available to handle header request")
    }

    /// Request receipts for the given block hashes.
    pub async fn get_receipts(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.get_receipts_inner(hashes, required_block, None, &[])
            .await
    }

    /// Request receipts and only return a peer response whose per-block receipt
    /// counts match the already fetched bodies. Peers that return receipt sets
    /// inconsistent with the bodies are disconnected and the request is retried
    /// against the next eligible peer.
    pub async fn get_receipts_matching_counts(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: &[usize],
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.get_receipts_matching_counts_prefer_peers(
            hashes,
            required_block,
            expected_receipt_counts,
            &[],
        )
        .await
    }

    /// Request receipts while trying body-proven peers before rotating through
    /// the rest of the eligible peer set.
    pub async fn get_receipts_matching_counts_prefer_peers(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: &[usize],
        preferred_peers: &[PeerId],
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.get_receipts_inner(
            hashes,
            required_block,
            Some(expected_receipt_counts),
            preferred_peers,
        )
        .await
    }

    async fn get_receipts_inner(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: Option<&[usize]>,
        preferred_peers: &[PeerId],
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok((PeerId::ZERO, Vec::new()));
        }
        if let Some(expected) = expected_receipt_counts
            && expected.len() != hashes.len()
        {
            bail!(
                "receipt count expectation length mismatch: expected {} entries for {} hashes",
                expected.len(),
                hashes.len()
            );
        }

        let mut peer_ids = self
            .peer_ids_for_receipt_requests(required_block, preferred_peers)
            .await;
        self.filter_paused_request_peers(&mut peer_ids, PeerRequestKind::Receipts);
        self.sort_peer_ids_by_request_performance(&mut peer_ids, PeerRequestKind::Receipts);
        let mut dead_peers = HashSet::new();

        match self
            .request_receipts_parallel_chunks(&peer_ids, hashes.clone(), expected_receipt_counts)
            .await
        {
            Ok(Some((peer_id, receipts, stats, failures))) => {
                for (peer_id, blocks, elapsed) in stats {
                    self.record_peer_request_success(
                        peer_id,
                        PeerRequestKind::Receipts,
                        blocks,
                        elapsed,
                    );
                }
                self.apply_parallel_chunk_failures("receipts", failures, &mut dead_peers);
                self.remove_dead_peers(&dead_peers);
                self.advance_request_cursor();
                return Ok((peer_id, receipts));
            }
            Ok(None) => {}
            Err((failures, stats)) => {
                for (peer_id, blocks, elapsed) in stats {
                    self.record_peer_request_success(
                        peer_id,
                        PeerRequestKind::Receipts,
                        blocks,
                        elapsed,
                    );
                }
                self.apply_parallel_chunk_failures("receipts", failures, &mut dead_peers);
                self.remove_dead_peers(&dead_peers);
                peer_ids = self
                    .peer_ids_for_receipt_requests(required_block, preferred_peers)
                    .await;
                self.filter_paused_request_peers(&mut peer_ids, PeerRequestKind::Receipts);
                self.sort_peer_ids_by_request_performance(&mut peer_ids, PeerRequestKind::Receipts);
            }
        }

        for peer_id in peer_ids {
            let version = match self.peers.get(&peer_id) {
                Some(peer) => {
                    trace!(
                        peer = %peer_id,
                        client_version = %peer.client_version,
                        version = ?peer.version,
                        serving = peer.is_serving,
                        latest_block = ?peer.remote_status.latest_block,
                        earliest_block = ?peer.remote_status.earliest_block,
                        required_block,
                        hashes = hashes.len(),
                        preferred = preferred_peers.contains(&peer_id),
                        "requesting receipts from execution peer"
                    );
                    peer.version
                }
                None => continue,
            };

            if version >= EthVersion::Eth70 {
                let started_at = Instant::now();
                match self.request_receipts70(peer_id, hashes.clone()).await {
                    Ok(receipts) => {
                        if let Err(error) = validate_receipt_response_counts(
                            "receipts70",
                            hashes.len(),
                            &receipts,
                            expected_receipt_counts,
                        ) {
                            if self.on_receipt_count_mismatch(peer_id, error) {
                                dead_peers.insert(peer_id);
                            }
                            continue;
                        }
                        self.record_peer_request_success(
                            peer_id,
                            PeerRequestKind::Receipts,
                            receipts.len(),
                            started_at.elapsed(),
                        );
                        self.advance_request_cursor();
                        return Ok((peer_id, receipts));
                    }
                    Err(error) => {
                        let should_drop =
                            self.on_request_error(peer_id, PeerRequestKind::Receipts, &error);
                        debug!(
                            peer = %peer_id,
                            ?error,
                            "eth/70 receipt request failed"
                        );
                        if should_drop {
                            dead_peers.insert(peer_id);
                        }
                        continue;
                    }
                }
            }

            let mut remaining_hashes = hashes.clone();
            let mut collected = Vec::with_capacity(hashes.len());

            while !remaining_hashes.is_empty() {
                let request_hashes = remaining_hashes.clone();
                let started_at = Instant::now();
                let attempt = if version >= EthVersion::Eth69 {
                    self.request_receipts69(peer_id, request_hashes.clone())
                        .await
                } else {
                    self.request_receipts(peer_id, request_hashes.clone()).await
                };

                match attempt {
                    Ok(receipts) => {
                        match classify_response_progress(request_hashes.len(), receipts.len()) {
                            ResponseProgress::Complete => {
                                if let Err(error) = validate_receipt_response_counts(
                                    "receipts",
                                    request_hashes.len(),
                                    &receipts,
                                    expected_receipt_counts.map(|expected| {
                                        &expected[hashes.len() - remaining_hashes.len()..]
                                    }),
                                ) {
                                    if self.on_receipt_count_mismatch(peer_id, error) {
                                        dead_peers.insert(peer_id);
                                    }
                                    break;
                                }
                                self.record_peer_request_success(
                                    peer_id,
                                    PeerRequestKind::Receipts,
                                    receipts.len(),
                                    started_at.elapsed(),
                                );
                                self.advance_request_cursor();
                                collected.extend(receipts);
                                return Ok((peer_id, collected));
                            }
                            ResponseProgress::Partial { returned } => {
                                if let Err(error) = validate_receipt_response_counts(
                                    "receipts",
                                    returned,
                                    &receipts,
                                    expected_receipt_counts.map(|expected| {
                                        let offset = hashes.len() - remaining_hashes.len();
                                        &expected[offset..offset + returned]
                                    }),
                                ) {
                                    if self.on_receipt_count_mismatch(peer_id, error) {
                                        dead_peers.insert(peer_id);
                                    }
                                    break;
                                }
                                self.record_peer_request_success(
                                    peer_id,
                                    PeerRequestKind::Receipts,
                                    returned,
                                    started_at.elapsed(),
                                );
                                self.on_partial_response(
                                    peer_id,
                                    PeerRequestKind::Receipts,
                                    "receipts",
                                    request_hashes.len(),
                                    returned,
                                );
                                collected.extend(receipts);
                                remaining_hashes = request_hashes[returned..].to_vec();
                            }
                            ResponseProgress::Empty => {
                                self.on_zero_progress_response(
                                    peer_id,
                                    PeerRequestKind::Receipts,
                                    "receipts",
                                    request_hashes.len(),
                                );
                                break;
                            }
                            ResponseProgress::Overflow { returned } => {
                                self.on_invalid_response_length(
                                    peer_id,
                                    "receipts",
                                    request_hashes.len(),
                                    returned,
                                );
                                dead_peers.insert(peer_id);
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        let should_drop =
                            self.on_request_error(peer_id, PeerRequestKind::Receipts, &error);
                        debug!(peer = %peer_id, ?error, "receipt request failed");
                        if should_drop {
                            dead_peers.insert(peer_id);
                        }
                        break;
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        bail!("no peers available to handle receipt request")
    }

    async fn peer_ids_for_receipt_requests(
        &mut self,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Vec<PeerId> {
        let peer_ids = self
            .peer_ids_for_block_requests(Some(required_block), preferred_peers)
            .await
            .into_iter();
        let (healthy, quarantined): (Vec<_>, Vec<_>) =
            peer_ids.partition(|peer_id| !self.peer_receipts_quarantined(*peer_id));
        if healthy.is_empty() {
            quarantined
        } else {
            healthy
        }
    }

    async fn peer_ids_for_block_requests(
        &mut self,
        required_block: Option<u64>,
        preferred_peers: &[PeerId],
    ) -> Vec<PeerId> {
        let mut peer_ids = prioritize_preferred_peer_ids(
            self.peer_ids_for_requests(required_block),
            preferred_peers,
        );
        if !peer_ids.is_empty() {
            return peer_ids;
        }

        for _ in 0..REQUEST_PEER_REFILL_ATTEMPTS {
            self.dial_pending_peers(self.max_peers);
            if !self.wait_for_activity(Duration::from_millis(250)).await {
                self.drain_events_now();
            }
            peer_ids = prioritize_preferred_peer_ids(
                self.peer_ids_for_requests(required_block),
                preferred_peers,
            );
            if !peer_ids.is_empty() {
                return peer_ids;
            }
        }

        peer_ids
    }

    fn body_receipt_chunk_ranges(
        &self,
        total_items: usize,
        body_peer_ids: &[PeerId],
        receipt_peer_ids: &[PeerId],
        receipt_gas_used: Option<&[u64]>,
    ) -> Vec<std::ops::Range<usize>> {
        if body_peer_ids.is_empty() || receipt_peer_ids.is_empty() {
            return Vec::new();
        }

        let chunk_cap = body_receipt_chunk_cap(body_peer_ids.len().min(receipt_peer_ids.len()));
        chunk_ranges_with_optional_gas(
            total_items,
            |chunk_index| {
                let body_peer = body_peer_ids[chunk_index % body_peer_ids.len()];
                let receipt_peer = receipt_peer_ids[chunk_index % receipt_peer_ids.len()];
                body_receipt_chunk_limit(
                    self.peer_request_limit(body_peer, PeerRequestKind::Bodies),
                    self.peer_request_limit(receipt_peer, PeerRequestKind::Receipts),
                    chunk_cap,
                )
            },
            receipt_gas_used,
        )
    }

    fn request_chunk_ranges(
        &self,
        total_items: usize,
        peer_ids: &[PeerId],
        kind: PeerRequestKind,
    ) -> Vec<std::ops::Range<usize>> {
        if peer_ids.is_empty() {
            return Vec::new();
        }

        self.dynamic_chunk_ranges_with(total_items, |chunk_index| {
            let peer_id = peer_ids[chunk_index % peer_ids.len()];
            self.peer_request_limit(peer_id, kind)
        })
    }

    fn dynamic_chunk_ranges_with(
        &self,
        total_items: usize,
        mut limit_for_chunk: impl FnMut(usize) -> usize,
    ) -> Vec<std::ops::Range<usize>> {
        if total_items == 0 {
            return Vec::new();
        }

        let mut ranges = Vec::new();
        let mut start = 0usize;
        let mut chunk_index = 0usize;
        while start < total_items {
            let limit = limit_for_chunk(chunk_index).clamp(REQUEST_LIMIT_MIN, REQUEST_LIMIT_MAX);
            let end = start.saturating_add(limit).min(total_items);
            ranges.push(start..end);
            start = end;
            chunk_index += 1;
        }
        ranges
    }

    async fn request_bodies_parallel_chunks(
        &self,
        peer_ids: &[PeerId],
        hashes: Vec<B256>,
    ) -> std::result::Result<Option<ParallelBodies>, ParallelChunkError> {
        if hashes.len() < MIN_PARALLEL_BODY_REQUEST_BLOCKS || peer_ids.is_empty() {
            return Ok(None);
        }

        let ranges = self.request_chunk_ranges(hashes.len(), peer_ids, PeerRequestKind::Bodies);
        if ranges.len() < 2 {
            return Ok(None);
        }
        let range_count = ranges.len();
        let range_indices_by_start: HashMap<usize, usize> = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect();
        let max_in_flight = request_window_limit(peer_ids.len(), MAX_PARALLEL_BODY_REQUESTS);
        if max_in_flight == 0 {
            return Ok(None);
        }

        let mut attempts = futures_util::stream::FuturesUnordered::new();
        let mut pending_ranges = ranges.iter().cloned().enumerate();
        let mut retry_counts = HashMap::<usize, usize>::new();
        for _ in 0..max_in_flight {
            let Some((chunk_index, range)) = pending_ranges.next() else {
                break;
            };
            let peer_id = peer_ids[chunk_index % peer_ids.len()];
            let request_hashes = hashes[range.clone()].to_vec();
            attempts.push(
                async move {
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    let result = self
                        .request_bodies_until_complete(peer_id, request_hashes)
                        .await;
                    (
                        chunk_index,
                        range,
                        peer_id,
                        requested,
                        started_at.elapsed(),
                        result,
                    )
                }
                .boxed_local(),
            );
        }

        let mut chunks = BTreeMap::new();
        let mut stats = Vec::new();
        let mut failures = Vec::new();
        while let Some((chunk_index, range, peer_id, requested, elapsed, result)) =
            attempts.next().await
        {
            let request_failed = result.is_err();
            match result {
                Ok(bodies) => {
                    stats.push((peer_id, bodies.len(), elapsed));
                    chunks.insert(range.start, (peer_id, bodies));
                }
                Err(kind) => {
                    failures.push(ChunkRequestFailure {
                        role: ChunkRequestRole::Bodies,
                        peer_id,
                        requested,
                        kind: kind.clone(),
                    });
                    trace!(
                        peer = %peer_id,
                        requested,
                        ?kind,
                        "parallel body chunk request failed"
                    );
                }
            }
            let retry_range = if request_failed && peer_ids.len() > 1 {
                let retry_count = retry_counts.entry(range.start).or_default();
                if *retry_count < PARALLEL_CHUNK_RETRY_ROUNDS {
                    *retry_count += 1;
                    let base_chunk_index = range_indices_by_start
                        .get(&range.start)
                        .copied()
                        .unwrap_or(chunk_index);
                    Some((base_chunk_index + (*retry_count * range_count), range))
                } else {
                    None
                }
            } else {
                None
            };

            let next_range = retry_range.or_else(|| pending_ranges.next());
            let Some((chunk_index, range)) = next_range else {
                continue;
            };
            let peer_id = peer_ids[chunk_index % peer_ids.len()];
            let request_hashes = hashes[range.clone()].to_vec();
            attempts.push(
                async move {
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    let result = self
                        .request_bodies_until_complete(peer_id, request_hashes)
                        .await;
                    (
                        chunk_index,
                        range,
                        peer_id,
                        requested,
                        started_at.elapsed(),
                        result,
                    )
                }
                .boxed_local(),
            );
        }

        if !missing_chunk_ranges(&ranges, &chunks).is_empty() {
            let mut bad_peers = disabled_chunk_peers(&failures, ChunkRequestRole::Bodies);
            for range in missing_chunk_ranges(&ranges, &chunks) {
                let base_chunk_index = range_indices_by_start
                    .get(&range.start)
                    .copied()
                    .unwrap_or_default();
                let mut retry_peer_ids = peer_ids_excluding(peer_ids, &bad_peers);
                rotate_request_candidates(
                    &mut retry_peer_ids,
                    base_chunk_index + ((PARALLEL_CHUNK_RETRY_ROUNDS + 1) * range_count),
                );
                for peer_id in retry_peer_ids {
                    let request_hashes = hashes[range.clone()].to_vec();
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    match self
                        .request_bodies_until_complete(peer_id, request_hashes)
                        .await
                    {
                        Ok(bodies) => {
                            stats.push((peer_id, bodies.len(), started_at.elapsed()));
                            chunks.insert(range.start, (peer_id, bodies));
                            break;
                        }
                        Err(kind) => {
                            let failure = ChunkRequestFailure {
                                role: ChunkRequestRole::Bodies,
                                peer_id,
                                requested,
                                kind: kind.clone(),
                            };
                            if chunk_failure_disables_role_peer(&failure) {
                                bad_peers.insert(peer_id);
                            }
                            failures.push(failure);
                            trace!(
                                peer = %peer_id,
                                requested,
                                ?kind,
                                "parallel body chunk salvage request failed"
                            );
                        }
                    }
                }
            }
        }

        let mut bodies = Vec::with_capacity(hashes.len());
        for (_, (peer_id, chunk_bodies)) in chunks {
            bodies.extend(chunk_bodies.into_iter().map(|body| (peer_id, body)));
        }

        if bodies.len() == hashes.len() {
            Ok(Some((bodies, stats, failures)))
        } else if failures.is_empty() {
            Ok(None)
        } else {
            Err((failures, stats))
        }
    }

    async fn request_receipts_parallel_chunks(
        &self,
        peer_ids: &[PeerId],
        hashes: Vec<B256>,
        expected_receipt_counts: Option<&[usize]>,
    ) -> std::result::Result<Option<ParallelReceipts>, ParallelChunkError> {
        if hashes.len() < MIN_PARALLEL_RECEIPT_REQUEST_BLOCKS || peer_ids.is_empty() {
            return Ok(None);
        }

        let ranges = self.request_chunk_ranges(hashes.len(), peer_ids, PeerRequestKind::Receipts);
        if ranges.len() < 2 {
            return Ok(None);
        }
        let range_count = ranges.len();
        let range_indices_by_start: HashMap<usize, usize> = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect();
        let max_in_flight = request_window_limit(peer_ids.len(), MAX_PARALLEL_RECEIPT_REQUESTS);
        if max_in_flight == 0 {
            return Ok(None);
        }

        let mut attempts = futures_util::stream::FuturesUnordered::new();
        let mut pending_ranges = ranges.iter().cloned().enumerate();
        let mut retry_counts = HashMap::<usize, usize>::new();
        for _ in 0..max_in_flight {
            let Some((chunk_index, range)) = pending_ranges.next() else {
                break;
            };
            let peer_id = peer_ids[chunk_index % peer_ids.len()];
            let request_hashes = hashes[range.clone()].to_vec();
            let expected_receipt_counts =
                expected_receipt_counts.map(|expected| expected[range.clone()].to_vec());
            attempts.push(
                async move {
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    let result = self
                        .request_receipts_until_complete(
                            peer_id,
                            request_hashes,
                            expected_receipt_counts,
                        )
                        .await;
                    (
                        chunk_index,
                        range,
                        peer_id,
                        requested,
                        started_at.elapsed(),
                        result,
                    )
                }
                .boxed_local(),
            );
        }

        let mut chunks = BTreeMap::new();
        let mut stats = Vec::new();
        let mut failures = Vec::new();
        while let Some((chunk_index, range, peer_id, requested, elapsed, result)) =
            attempts.next().await
        {
            let request_failed = result.is_err();
            match result {
                Ok(receipts) => {
                    stats.push((peer_id, receipts.len(), elapsed));
                    chunks.insert(range.start, (peer_id, receipts));
                }
                Err(kind) => {
                    failures.push(ChunkRequestFailure {
                        role: ChunkRequestRole::Receipts,
                        peer_id,
                        requested,
                        kind: kind.clone(),
                    });
                    trace!(
                        peer = %peer_id,
                        requested,
                        ?kind,
                        "parallel receipt chunk request failed"
                    );
                }
            }
            let retry_range = if request_failed && peer_ids.len() > 1 {
                let retry_count = retry_counts.entry(range.start).or_default();
                if *retry_count < PARALLEL_CHUNK_RETRY_ROUNDS {
                    *retry_count += 1;
                    let base_chunk_index = range_indices_by_start
                        .get(&range.start)
                        .copied()
                        .unwrap_or(chunk_index);
                    Some((base_chunk_index + (*retry_count * range_count), range))
                } else {
                    None
                }
            } else {
                None
            };

            let next_range = retry_range.or_else(|| pending_ranges.next());
            let Some((chunk_index, range)) = next_range else {
                continue;
            };
            let peer_id = peer_ids[chunk_index % peer_ids.len()];
            let request_hashes = hashes[range.clone()].to_vec();
            let expected_receipt_counts =
                expected_receipt_counts.map(|expected| expected[range.clone()].to_vec());
            attempts.push(
                async move {
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    let result = self
                        .request_receipts_until_complete(
                            peer_id,
                            request_hashes,
                            expected_receipt_counts,
                        )
                        .await;
                    (
                        chunk_index,
                        range,
                        peer_id,
                        requested,
                        started_at.elapsed(),
                        result,
                    )
                }
                .boxed_local(),
            );
        }

        if !missing_chunk_ranges(&ranges, &chunks).is_empty() {
            let mut bad_peers = disabled_chunk_peers(&failures, ChunkRequestRole::Receipts);
            for range in missing_chunk_ranges(&ranges, &chunks) {
                let base_chunk_index = range_indices_by_start
                    .get(&range.start)
                    .copied()
                    .unwrap_or_default();
                let mut retry_peer_ids = peer_ids_excluding(peer_ids, &bad_peers);
                rotate_request_candidates(
                    &mut retry_peer_ids,
                    base_chunk_index + ((PARALLEL_CHUNK_RETRY_ROUNDS + 1) * range_count),
                );
                for peer_id in retry_peer_ids {
                    let request_hashes = hashes[range.clone()].to_vec();
                    let expected_counts =
                        expected_receipt_counts.map(|expected| expected[range.clone()].to_vec());
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    match self
                        .request_receipts_until_complete(peer_id, request_hashes, expected_counts)
                        .await
                    {
                        Ok(receipts) => {
                            stats.push((peer_id, receipts.len(), started_at.elapsed()));
                            chunks.insert(range.start, (peer_id, receipts));
                            break;
                        }
                        Err(kind) => {
                            let failure = ChunkRequestFailure {
                                role: ChunkRequestRole::Receipts,
                                peer_id,
                                requested,
                                kind: kind.clone(),
                            };
                            if chunk_failure_disables_role_peer(&failure) {
                                bad_peers.insert(peer_id);
                            }
                            failures.push(failure);
                            trace!(
                                peer = %peer_id,
                                requested,
                                ?kind,
                                "parallel receipt chunk salvage request failed"
                            );
                        }
                    }
                }
            }
        }

        let Some(first_peer) = chunks.first_key_value().map(|(_, (peer_id, _))| *peer_id) else {
            return Ok(None);
        };
        let mut receipts = Vec::with_capacity(hashes.len());
        for (_, (_, chunk_receipts)) in chunks {
            receipts.extend(chunk_receipts);
        }

        if receipts.len() == hashes.len() {
            Ok(Some((first_peer, receipts, stats, failures)))
        } else if failures.is_empty() {
            Ok(None)
        } else {
            Err((failures, stats))
        }
    }

    async fn request_bodies_until_complete(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        ChunkFailureKind,
    > {
        let mut remaining_hashes = hashes;
        let mut bodies = Vec::with_capacity(remaining_hashes.len());

        while !remaining_hashes.is_empty() {
            let request_hashes = remaining_hashes.clone();
            let response = self
                .request_bodies(peer_id, request_hashes.clone())
                .await
                .map_err(ChunkFailureKind::Request)?;

            match classify_response_progress(request_hashes.len(), response.len()) {
                ResponseProgress::Complete => {
                    bodies.extend(response);
                    return Ok(bodies);
                }
                ResponseProgress::Partial { returned } => {
                    bodies.extend(response);
                    remaining_hashes = request_hashes[returned..].to_vec();
                }
                ResponseProgress::Empty => {
                    return Err(ChunkFailureKind::Incomplete { returned: 0 });
                }
                ResponseProgress::Overflow { returned } => {
                    return Err(ChunkFailureKind::Incomplete { returned });
                }
            }
        }

        Ok(bodies)
    }

    async fn request_receipts_until_complete(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
        expected_receipt_counts: Option<Vec<usize>>,
    ) -> std::result::Result<ReceiptBatch, ChunkFailureKind> {
        let Some(version) = self.peers.get(&peer_id).map(|peer| peer.version) else {
            return Err(ChunkFailureKind::Request(RequestAttempt::Disconnected));
        };

        if expected_receipt_counts
            .as_ref()
            .is_some_and(|expected| expected.len() != hashes.len())
        {
            return Err(ChunkFailureKind::Request(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::BadResponse,
            )));
        }

        if version >= EthVersion::Eth70 {
            let receipts = self
                .request_receipts70(peer_id, hashes.clone())
                .await
                .map_err(ChunkFailureKind::Request)?;
            validate_receipt_response_counts(
                "receipts70",
                hashes.len(),
                &receipts,
                expected_receipt_counts.as_deref(),
            )
            .map_err(ChunkFailureKind::ReceiptCountMismatch)?;
            return Ok(receipts);
        }

        let mut remaining_hashes = hashes.clone();
        let mut receipts = Vec::with_capacity(hashes.len());
        let mut offset = 0usize;

        while !remaining_hashes.is_empty() {
            let request_hashes = remaining_hashes.clone();
            let response = if version >= EthVersion::Eth69 {
                self.request_receipts69(peer_id, request_hashes.clone())
                    .await
            } else {
                self.request_receipts(peer_id, request_hashes.clone()).await
            }
            .map_err(ChunkFailureKind::Request)?;

            match classify_response_progress(request_hashes.len(), response.len()) {
                ResponseProgress::Complete => {
                    let returned = response.len();
                    validate_receipt_response_counts(
                        "receipts",
                        returned,
                        &response,
                        expected_receipt_counts
                            .as_deref()
                            .map(|expected| &expected[offset..offset + returned]),
                    )
                    .map_err(ChunkFailureKind::ReceiptCountMismatch)?;
                    receipts.extend(response);
                    return Ok(receipts);
                }
                ResponseProgress::Partial { returned } => {
                    validate_receipt_response_counts(
                        "receipts",
                        returned,
                        &response,
                        expected_receipt_counts
                            .as_deref()
                            .map(|expected| &expected[offset..offset + returned]),
                    )
                    .map_err(ChunkFailureKind::ReceiptCountMismatch)?;
                    receipts.extend(response);
                    offset += returned;
                    remaining_hashes = request_hashes[returned..].to_vec();
                }
                ResponseProgress::Empty => {
                    return Err(ChunkFailureKind::Incomplete { returned: 0 });
                }
                ResponseProgress::Overflow { returned } => {
                    return Err(ChunkFailureKind::Incomplete { returned });
                }
            }
        }

        Ok(receipts)
    }

    fn apply_parallel_chunk_failures(
        &mut self,
        response_kind: &'static str,
        failures: ParallelChunkFailures,
        dead_peers: &mut HashSet<PeerId>,
    ) {
        let mut request_failures =
            HashMap::<(PeerId, ChunkRequestRole), ChunkRequestFailure>::new();
        let mut other_failures = Vec::new();
        for failure in failures {
            if matches!(failure.kind, ChunkFailureKind::Request(_)) {
                match request_failures.entry((failure.peer_id, failure.role)) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        entry.insert(failure);
                    }
                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                        if chunk_request_failure_severity(&failure)
                            > chunk_request_failure_severity(entry.get())
                        {
                            entry.insert(failure);
                        }
                    }
                }
            } else {
                other_failures.push(failure);
            }
        }

        for failure in request_failures.into_values().chain(other_failures) {
            match failure.kind {
                ChunkFailureKind::Request(error) => {
                    let quarantine_receipts = failure.role == ChunkRequestRole::Receipts
                        && request_failure_quarantines_receipt_peer(&error);
                    let should_drop =
                        self.on_request_error(failure.peer_id, failure.role.request_kind(), &error);
                    if quarantine_receipts {
                        self.quarantine_peer_receipts_after_request_failure(
                            failure.peer_id,
                            response_kind,
                            failure.requested,
                            &error,
                        );
                    }
                    debug!(
                        peer = %failure.peer_id,
                        role = ?failure.role,
                        requested = failure.requested,
                        ?error,
                        response_kind,
                        "parallel chunk request failed"
                    );
                    if should_drop {
                        dead_peers.insert(failure.peer_id);
                    }
                }
                ChunkFailureKind::Incomplete { returned } => {
                    match classify_response_progress(failure.requested, returned) {
                        ResponseProgress::Partial { returned } => {
                            self.on_partial_response(
                                failure.peer_id,
                                failure.role.request_kind(),
                                response_kind,
                                failure.requested,
                                returned,
                            );
                        }
                        ResponseProgress::Empty => {
                            self.on_zero_progress_response(
                                failure.peer_id,
                                failure.role.request_kind(),
                                response_kind,
                                failure.requested,
                            );
                        }
                        ResponseProgress::Overflow { returned } => {
                            self.on_invalid_response_length(
                                failure.peer_id,
                                response_kind,
                                failure.requested,
                                returned,
                            );
                            dead_peers.insert(failure.peer_id);
                        }
                        ResponseProgress::Complete => {}
                    }
                }
                ChunkFailureKind::ReceiptCountMismatch(error) => {
                    if self.on_receipt_count_mismatch(failure.peer_id, error) {
                        dead_peers.insert(failure.peer_id);
                    }
                }
            }
        }
    }

    pub(super) async fn request_headers(
        &self,
        peer_id: PeerId,
        request: HeadersRequest,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetBlockHeaders {
            request: GetBlockHeaders {
                start_block: request.start,
                limit: request.limit,
                skip: 0,
                direction: request.direction,
            },
            response,
        })
        .await
    }

    pub(super) async fn request_bodies(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetBlockBodies {
            request: GetBlockBodies(hashes.clone()),
            response,
        })
        .await
    }

    pub(super) async fn request_with_channel<T, W, MakeRequest>(
        &self,
        peer_id: PeerId,
        make_request: &MakeRequest,
    ) -> std::result::Result<T, RequestAttempt>
    where
        W: IntoResponseValue<T>,
        MakeRequest: Fn(
            oneshot::Sender<reth_network::p2p::error::RequestResult<W>>,
        ) -> PeerRequest<LogexNetworkPrimitives>,
    {
        let Some(peer) = self.peers.get(&peer_id) else {
            return Err(RequestAttempt::Disconnected);
        };

        let sender = peer.sender.clone();
        let (response_tx, response_rx) = oneshot::channel();
        sender
            .to_session_tx
            .send(make_request(response_tx))
            .await
            .map_err(|_| RequestAttempt::Disconnected)?;

        match timeout(REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(Ok(response))) => Ok(response.into_value()),
            Ok(Ok(Err(error))) => Err(RequestAttempt::Request(error)),
            Ok(Err(_)) => Err(RequestAttempt::Disconnected),
            Err(_) => Err(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::Timeout,
            )),
        }
    }

    pub(super) async fn request_receipts(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts {
            request: GetReceipts(hashes.clone()),
            response,
        })
        .await
    }

    pub(super) async fn request_receipts69(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let receipts: Vec<Vec<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>> = self
            .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts69 {
                request: GetReceipts(hashes.clone()),
                response,
            })
            .await?;
        let mut bloom_cache = ReceiptBloomCache::default();
        Ok(logex_receipt_batches_with_cached_blooms(
            receipts,
            &mut bloom_cache,
        ))
    }

    pub(super) async fn request_receipts70(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let mut merged = Vec::with_capacity(hashes.len());
        let mut next_block_index = 0usize;
        let mut first_block_receipt_index = 0u64;
        let mut bloom_cache = ReceiptBloomCache::default();

        while next_block_index < hashes.len() {
            let request_hashes = hashes[next_block_index..].to_vec();
            let response: Receipts70<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt> = self
                .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts70 {
                    request: GetReceipts70 {
                        first_block_receipt_index,
                        block_hashes: request_hashes.clone(),
                    },
                    response,
                })
                .await?;

            let (updated_block_index, updated_receipt_index) = merge_receipts70_response(
                &mut merged,
                next_block_index,
                first_block_receipt_index,
                response,
                hashes.len(),
                &mut bloom_cache,
            )
            .map_err(Receipts70MergeError::into_request_attempt)?;

            next_block_index = updated_block_index;
            first_block_receipt_index = updated_receipt_index;
        }

        Ok(merged)
    }

    fn on_receipt_count_mismatch(&mut self, peer_id: PeerId, error: ReceiptCountMismatch) -> bool {
        if receipt_count_mismatch_is_protocol_breach(error) {
            self.network
                .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
            warn!(
                peer = %peer_id,
                response_kind = error.response_kind,
                requested_blocks = error.requested_blocks,
                returned_blocks = error.returned_blocks,
                block_index = error.block_index,
                expected_receipts = error.expected_receipts,
                returned_receipts = error.returned_receipts,
                "peer returned receipts inconsistent with requested blocks, disconnecting it"
            );
            return true;
        }

        self.quarantine_peer_receipts(
            peer_id,
            error.response_kind,
            error.requested_blocks,
            error.returned_blocks,
        );
        debug!(
            peer = %peer_id,
            response_kind = error.response_kind,
            requested_blocks = error.requested_blocks,
            returned_blocks = error.returned_blocks,
            block_index = error.block_index,
            expected_receipts = error.expected_receipts,
            returned_receipts = error.returned_receipts,
            "peer could not serve complete receipts for requested blocks"
        );
        false
    }
}

fn chunk_request_failure_severity(failure: &ChunkRequestFailure) -> u8 {
    let ChunkFailureKind::Request(error) = &failure.kind else {
        return 0;
    };

    match error {
        RequestAttempt::Request(reth_network::p2p::error::RequestError::BadResponse)
        | RequestAttempt::Request(reth_network::p2p::error::RequestError::UnsupportedCapability) => {
            3
        }
        RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout) => 2,
        RequestAttempt::Disconnected
        | RequestAttempt::Request(reth_network::p2p::error::RequestError::ChannelClosed)
        | RequestAttempt::Request(reth_network::p2p::error::RequestError::ConnectionDropped) => 1,
    }
}

fn request_failure_quarantines_receipt_peer(error: &RequestAttempt) -> bool {
    matches!(
        error,
        RequestAttempt::Disconnected
            | RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout)
            | RequestAttempt::Request(reth_network::p2p::error::RequestError::ChannelClosed)
            | RequestAttempt::Request(reth_network::p2p::error::RequestError::ConnectionDropped)
    )
}

fn peer_ids_excluding(peer_ids: &[PeerId], bad_peers: &HashSet<PeerId>) -> Vec<PeerId> {
    if bad_peers.is_empty() {
        return peer_ids.to_vec();
    }

    let filtered: Vec<_> = peer_ids
        .iter()
        .copied()
        .filter(|peer_id| !bad_peers.contains(peer_id))
        .collect();
    if filtered.is_empty() {
        peer_ids.to_vec()
    } else {
        filtered
    }
}

fn disabled_chunk_peers(
    failures: &[ChunkRequestFailure],
    role: ChunkRequestRole,
) -> HashSet<PeerId> {
    failures
        .iter()
        .filter(|failure| failure.role == role && chunk_failure_disables_role_peer(failure))
        .map(|failure| failure.peer_id)
        .collect()
}

fn missing_chunk_ranges<T>(
    ranges: &[std::ops::Range<usize>],
    chunks: &BTreeMap<usize, (PeerId, Vec<T>)>,
) -> Vec<std::ops::Range<usize>> {
    ranges
        .iter()
        .filter(|range| {
            chunks
                .get(&range.start)
                .is_none_or(|(_, items)| items.len() != range.len())
        })
        .cloned()
        .collect()
}

fn chunk_failure_disables_role_peer(failure: &ChunkRequestFailure) -> bool {
    match &failure.kind {
        ChunkFailureKind::Request(error) => matches!(
            error,
            RequestAttempt::Disconnected
                | RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout)
                | RequestAttempt::Request(reth_network::p2p::error::RequestError::ChannelClosed)
                | RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::ConnectionDropped
                )
        ),
        ChunkFailureKind::Incomplete { .. } => true,
        ChunkFailureKind::ReceiptCountMismatch(error) => {
            receipt_count_mismatch_disables_receipt_peer(*error)
        }
    }
}

fn schedule_body_receipt_chunk_attempt<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptChunk>,
    >,
    in_flight: &mut HashMap<usize, InFlightBodyReceiptChunk>,
    range: std::ops::Range<usize>,
    chunk_index: usize,
    body_peer_ids: Vec<PeerId>,
    receipt_peer_ids: Vec<PeerId>,
) {
    let start = range.start;
    match in_flight.entry(start) {
        std::collections::hash_map::Entry::Occupied(mut entry) => {
            entry.get_mut().attempts += 1;
        }
        std::collections::hash_map::Entry::Vacant(entry) => {
            entry.insert(InFlightBodyReceiptChunk {
                range: range.clone(),
                chunk_index,
                attempts: 1,
                last_hedged_at: Instant::now(),
                hedges: 0,
            });
        }
    }

    attempts.push(body_receipt_chunk_request(
        plan,
        range,
        chunk_index,
        &plan.hashes,
        &body_peer_ids,
        &receipt_peer_ids,
    ));
}

fn complete_body_receipt_chunk_attempt(
    in_flight: &mut HashMap<usize, InFlightBodyReceiptChunk>,
    start: usize,
) {
    let std::collections::hash_map::Entry::Occupied(mut entry) = in_flight.entry(start) else {
        return;
    };
    if entry.get().attempts > 1 {
        entry.get_mut().attempts -= 1;
    } else {
        entry.remove();
    }
}

fn body_receipt_hedge_candidate(
    in_flight: &mut HashMap<usize, InFlightBodyReceiptChunk>,
    chunks: &BTreeMap<usize, Vec<SourcedBodyReceipts>>,
    min_return_blocks: usize,
    now: Instant,
) -> Option<(std::ops::Range<usize>, usize)> {
    let contiguous_blocks = contiguous_chunk_blocks(chunks);
    let start = in_flight
        .keys()
        .copied()
        .filter(|start| {
            *start <= contiguous_blocks && *start < min_return_blocks && !chunks.contains_key(start)
        })
        .min()?;
    let entry = in_flight.get_mut(&start)?;
    if entry.hedges >= PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK
        || now.duration_since(entry.last_hedged_at) < PIPELINED_BODY_RECEIPT_HEDGE_DELAY
    {
        return None;
    }

    entry.hedges += 1;
    entry.last_hedged_at = now;
    Some((entry.range.clone(), entry.chunk_index))
}

fn body_receipt_chunk_request<'a>(
    plan: &'a BodyReceiptRequestPlan,
    range: std::ops::Range<usize>,
    chunk_index: usize,
    hashes: &[B256],
    body_peer_ids: &[PeerId],
    receipt_peer_ids: &[PeerId],
) -> futures_util::future::BoxFuture<'a, BodyReceiptChunk> {
    let chunk_hashes = hashes[range.clone()].to_vec();
    let mut chunk_body_peers = body_peer_ids.to_vec();
    rotate_request_candidates(&mut chunk_body_peers, chunk_index);
    let mut chunk_receipt_peers = receipt_peer_ids.to_vec();
    rotate_request_candidates(&mut chunk_receipt_peers, chunk_index);

    async move {
        plan.request_body_receipt_chunk(
            range.start,
            chunk_hashes,
            chunk_body_peers,
            chunk_receipt_peers,
        )
        .await
    }
    .boxed()
}

fn receipt_candidates_for_body_peer(
    receipt_peer_ids: Vec<PeerId>,
    body_peer: PeerId,
    limit: usize,
) -> Vec<PeerId> {
    let mut candidates = receipt_peer_ids;
    if candidates.first() == Some(&body_peer)
        && let Some(index) = candidates.iter().position(|peer_id| *peer_id != body_peer)
    {
        candidates.swap(0, index);
    }
    candidates.truncate(limit);
    candidates
}

fn body_receipt_blocks_if_counts_match(
    bodies: &[SourcedBlockBody],
    receipt_peer: PeerId,
    receipts: ReceiptBatch,
    requested: usize,
) -> std::result::Result<Vec<SourcedBodyReceipts>, ReceiptCountMismatch> {
    let expected_receipt_counts: Vec<usize> = bodies
        .iter()
        .map(|(_, body)| body.transaction_count())
        .collect();
    validate_receipt_response_counts(
        "receipts",
        requested,
        &receipts,
        Some(&expected_receipt_counts),
    )?;
    Ok(bodies
        .iter()
        .cloned()
        .zip(
            receipts
                .into_iter()
                .map(|receipts| (receipt_peer, receipts)),
        )
        .collect())
}

fn contiguous_chunk_blocks<T>(chunks: &BTreeMap<usize, Vec<T>>) -> usize {
    let mut expected_start = 0usize;
    for (start, chunk_blocks) in chunks {
        if *start != expected_start {
            break;
        }
        expected_start += chunk_blocks.len();
    }
    expected_start
}

fn take_contiguous_body_receipt_prefix(
    return_blocks: usize,
    chunks: BTreeMap<usize, Vec<SourcedBodyReceipts>>,
) -> Vec<SourcedBodyReceipts> {
    take_contiguous_prefix(return_blocks, chunks)
}

fn take_contiguous_prefix<T>(return_blocks: usize, chunks: BTreeMap<usize, Vec<T>>) -> Vec<T> {
    let contiguous_blocks = contiguous_chunk_blocks(&chunks);
    let target_blocks = contiguous_blocks.min(return_blocks);
    let mut blocks = Vec::with_capacity(target_blocks);
    let mut expected_start = 0usize;

    for (start, mut chunk_blocks) in chunks {
        if start == expected_start && blocks.len() < target_blocks {
            let remaining_prefix = target_blocks - blocks.len();
            if chunk_blocks.len() <= remaining_prefix {
                expected_start += chunk_blocks.len();
                blocks.extend(chunk_blocks);
                continue;
            }

            chunk_blocks.truncate(remaining_prefix);
            expected_start += chunk_blocks.len();
            blocks.extend(chunk_blocks);
        }
    }

    blocks
}

#[derive(Debug, Clone)]
pub(super) enum RequestAttempt {
    Disconnected,
    Request(reth_network::p2p::error::RequestError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Receipts70MergeError {
    EmptyResponse,
    ResponseOverflow,
    UnexpectedAppend,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseProgress {
    Complete,
    Partial { returned: usize },
    Empty,
    Overflow { returned: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiptCountMismatch {
    response_kind: &'static str,
    requested_blocks: usize,
    returned_blocks: usize,
    block_index: Option<usize>,
    expected_receipts: Option<usize>,
    returned_receipts: Option<usize>,
}

impl Receipts70MergeError {
    pub(super) fn into_request_attempt(self) -> RequestAttempt {
        RequestAttempt::Request(reth_network::p2p::error::RequestError::BadResponse)
    }
}

pub(super) fn classify_response_progress(requested: usize, returned: usize) -> ResponseProgress {
    match returned.cmp(&requested) {
        std::cmp::Ordering::Equal => ResponseProgress::Complete,
        std::cmp::Ordering::Less if returned == 0 => ResponseProgress::Empty,
        std::cmp::Ordering::Less => ResponseProgress::Partial { returned },
        std::cmp::Ordering::Greater => ResponseProgress::Overflow { returned },
    }
}

pub(super) fn prioritize_preferred_peer_ids(
    mut peer_ids: Vec<PeerId>,
    preferred_peers: &[PeerId],
) -> Vec<PeerId> {
    if peer_ids.len() <= 1 || preferred_peers.is_empty() {
        return peer_ids;
    }

    let mut prioritized = Vec::with_capacity(peer_ids.len());
    for preferred in preferred_peers {
        if let Some(index) = peer_ids.iter().position(|peer_id| peer_id == preferred) {
            prioritized.push(peer_ids.remove(index));
        }
    }
    prioritized.extend(peer_ids);
    prioritized
}

fn validate_receipt_response_counts<T>(
    response_kind: &'static str,
    requested_blocks: usize,
    receipts: &[Vec<alloy_consensus::ReceiptWithBloom<T>>],
    expected_receipt_counts: Option<&[usize]>,
) -> std::result::Result<(), ReceiptCountMismatch> {
    if receipts.len() != requested_blocks {
        return Err(ReceiptCountMismatch {
            response_kind,
            requested_blocks,
            returned_blocks: receipts.len(),
            block_index: None,
            expected_receipts: None,
            returned_receipts: None,
        });
    }

    let Some(expected_receipt_counts) = expected_receipt_counts else {
        return Ok(());
    };

    for (block_index, (block_receipts, expected_count)) in receipts
        .iter()
        .zip(expected_receipt_counts.iter().copied())
        .enumerate()
    {
        if block_receipts.len() != expected_count {
            return Err(ReceiptCountMismatch {
                response_kind,
                requested_blocks,
                returned_blocks: receipts.len(),
                block_index: Some(block_index),
                expected_receipts: Some(expected_count),
                returned_receipts: Some(block_receipts.len()),
            });
        }
    }

    Ok(())
}

fn receipt_count_mismatch_is_protocol_breach(error: ReceiptCountMismatch) -> bool {
    if error.returned_blocks > error.requested_blocks {
        return true;
    }

    match (error.expected_receipts, error.returned_receipts) {
        (Some(expected), Some(returned)) => returned > expected,
        _ => false,
    }
}

fn receipt_count_mismatch_disables_receipt_peer(error: ReceiptCountMismatch) -> bool {
    !receipt_count_mismatch_is_protocol_breach(error)
}

pub(super) fn merge_receipts70_response(
    merged: &mut ReceiptBatch,
    next_block_index: usize,
    first_block_receipt_index: u64,
    response: Receipts70<LogexReceipt>,
    expected_blocks: usize,
    bloom_cache: &mut ReceiptBloomCache,
) -> std::result::Result<(usize, u64), Receipts70MergeError> {
    let previous_state = (next_block_index, first_block_receipt_index);
    let returned_blocks = response.receipts.len();
    if returned_blocks == 0 {
        return Err(Receipts70MergeError::EmptyResponse);
    }

    if next_block_index + returned_blocks > expected_blocks {
        return Err(Receipts70MergeError::ResponseOverflow);
    }

    let last_block_incomplete = response.last_block_incomplete;
    let receipts = logex_receipt_batches_with_cached_blooms(response.receipts, bloom_cache);
    for (offset, block_receipts) in receipts.into_iter().enumerate() {
        let target_index = next_block_index + offset;
        if target_index < merged.len() {
            if offset != 0 || first_block_receipt_index == 0 {
                return Err(Receipts70MergeError::UnexpectedAppend);
            }
            if block_receipts.is_empty() {
                return Err(Receipts70MergeError::NoProgress);
            }
            merged[target_index].extend(block_receipts);
        } else if target_index == merged.len() {
            merged.push(block_receipts);
        } else {
            return Err(Receipts70MergeError::ResponseOverflow);
        }
    }

    let (updated_block_index, updated_receipt_index) = if last_block_incomplete {
        let partial_block_index = next_block_index + returned_blocks - 1;
        let received_receipts = merged
            .get(partial_block_index)
            .map(Vec::len)
            .unwrap_or_default();
        if received_receipts == 0 {
            return Err(Receipts70MergeError::NoProgress);
        }
        (
            next_block_index + returned_blocks - 1,
            received_receipts as u64,
        )
    } else {
        (next_block_index + returned_blocks, 0)
    };

    if (updated_block_index, updated_receipt_index) == previous_state {
        return Err(Receipts70MergeError::NoProgress);
    }

    Ok((updated_block_index, updated_receipt_index))
}

pub(super) trait IntoResponseValue<T> {
    fn into_value(self) -> T;
}

impl<T> IntoResponseValue<Vec<T>> for BlockHeaders<T> {
    fn into_value(self) -> Vec<T> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<T>> for BlockBodies<T> {
    fn into_value(self) -> Vec<T> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>>> for Receipts<T> {
    fn into_value(self) -> Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<T>>> for Receipts69<T> {
    fn into_value(self) -> Vec<Vec<T>> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<T>>> for Receipts70<T> {
    fn into_value(self) -> Vec<Vec<T>> {
        self.receipts
    }
}

impl<T> IntoResponseValue<Receipts70<T>> for Receipts70<T> {
    fn into_value(self) -> Receipts70<T> {
        self
    }
}

#[cfg(test)]
fn chunk_ranges(total_items: usize, target_chunk_size: usize) -> Vec<std::ops::Range<usize>> {
    if total_items == 0 || target_chunk_size == 0 {
        return Vec::new();
    }

    let chunk_count = total_items.div_ceil(target_chunk_size);
    let mut ranges = Vec::with_capacity(chunk_count);
    let mut start = 0;
    while start < total_items {
        let end = start.saturating_add(target_chunk_size).min(total_items);
        ranges.push(start..end);
        start = end;
    }
    ranges
}

fn request_window_limit(peer_count: usize, max_in_flight: usize) -> usize {
    if peer_count == 0 || max_in_flight == 0 {
        return 0;
    }

    peer_count
        .saturating_mul(PARALLEL_REQUESTS_PER_PEER)
        .clamp(1, max_in_flight)
}

fn paired_body_receipt_chunk_window_limit(peer_count: usize, max_in_flight: usize) -> usize {
    let request_limit = request_window_limit(peer_count, max_in_flight);
    if request_limit == 0 {
        0
    } else {
        request_limit.div_ceil(2)
    }
}

fn chunk_ranges_with_optional_gas(
    total_items: usize,
    mut limit_for_chunk: impl FnMut(usize) -> usize,
    gas_used: Option<&[u64]>,
) -> Vec<std::ops::Range<usize>> {
    if total_items == 0 {
        return Vec::new();
    }

    let mut ranges = Vec::new();
    let mut start = 0usize;
    let mut chunk_index = 0usize;
    while start < total_items {
        let block_limit = limit_for_chunk(chunk_index).clamp(REQUEST_LIMIT_MIN, REQUEST_LIMIT_MAX);
        let mut end = start;
        let mut chunk_gas = 0u64;

        while end < total_items && end.saturating_sub(start) < block_limit {
            let next_gas = gas_used
                .and_then(|values| values.get(end))
                .copied()
                .unwrap_or_default();
            if end > start
                && gas_used.is_some()
                && chunk_gas.saturating_add(next_gas) > PIPELINED_BODY_RECEIPT_CHUNK_GAS_TARGET
            {
                break;
            }

            chunk_gas = chunk_gas.saturating_add(next_gas);
            end += 1;
        }

        if end == start {
            end = start.saturating_add(1).min(total_items);
        }
        ranges.push(start..end);
        start = end;
        chunk_index += 1;
    }
    ranges
}

fn body_receipt_chunk_cap(_peer_pair_count: usize) -> usize {
    PIPELINED_BODY_RECEIPT_CHUNK_BLOCKS_DEFAULT
}

fn body_receipt_chunk_limit(body_limit: usize, receipt_limit: usize, chunk_cap: usize) -> usize {
    body_limit.min(receipt_limit).min(chunk_cap)
}

fn body_receipt_prefix_range(
    mut range: std::ops::Range<usize>,
    return_blocks: usize,
) -> Option<std::ops::Range<usize>> {
    if range.start >= return_blocks {
        return None;
    }
    range.end = range.end.min(return_blocks);
    if range.start < range.end {
        Some(range)
    } else {
        None
    }
}

fn body_receipt_return_blocks(total_blocks: usize, gas_used: Option<&[u64]>) -> usize {
    if total_blocks <= PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS {
        return total_blocks;
    }

    let Some(gas_used) = gas_used else {
        return PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS;
    };

    let return_limit = total_blocks.min(PIPELINED_BODY_RECEIPT_MAX_CONTIGUOUS_RETURN_BLOCKS);
    let return_gas_target =
        PIPELINED_BODY_RECEIPT_RETURN_GAS_PER_BLOCK_TARGET.saturating_mul(return_limit as u128);
    let mut cumulative_gas = 0u128;
    for (index, gas) in gas_used.iter().take(total_blocks).enumerate() {
        cumulative_gas = cumulative_gas.saturating_add(u128::from(*gas));
        let returned_blocks = index + 1;
        if returned_blocks >= PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS
            && cumulative_gas >= return_gas_target
        {
            return returned_blocks;
        }
    }

    return_limit
}

fn body_receipt_min_accepted_prefix(return_blocks: usize) -> usize {
    return_blocks.min(PIPELINED_BODY_RECEIPT_MIN_ACCEPTED_PREFIX_BLOCKS)
}

fn body_receipt_scheduled_chunk_limit(
    ranges: &[std::ops::Range<usize>],
    min_return_blocks: usize,
) -> usize {
    if ranges.is_empty() || min_return_blocks == 0 {
        return 0;
    }

    let prefix_chunks = ranges
        .iter()
        .take_while(|range| range.start < min_return_blocks)
        .count()
        .max(1);
    prefix_chunks.clamp(1, ranges.len())
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Eip658Value, TxType};
    use alloy_primitives::Log;

    use super::*;

    #[test]
    fn response_progress_distinguishes_complete_partial_and_overflow() {
        assert_eq!(classify_response_progress(8, 8), ResponseProgress::Complete);
        assert_eq!(classify_response_progress(8, 0), ResponseProgress::Empty);
        assert_eq!(
            classify_response_progress(8, 7),
            ResponseProgress::Partial { returned: 7 }
        );
        assert_eq!(
            classify_response_progress(8, 9),
            ResponseProgress::Overflow { returned: 9 }
        );
    }

    #[test]
    fn preferred_peer_ids_are_tried_before_rotation_order() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let missing = PeerId::repeat_byte(0x44);

        let ordered =
            prioritize_preferred_peer_ids(vec![first, second, third], &[missing, third, first]);

        assert_eq!(ordered, vec![third, first, second]);
    }

    #[test]
    fn body_receipt_chunks_prefer_distinct_first_receipt_peer() {
        let body_peer = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);

        let ordered =
            receipt_candidates_for_body_peer(vec![body_peer, second, third], body_peer, 3);

        assert_eq!(ordered, vec![second, body_peer, third]);
    }

    #[test]
    fn body_receipt_chunks_fall_back_to_body_peer_when_needed() {
        let body_peer = PeerId::repeat_byte(0x11);

        let ordered = receipt_candidates_for_body_peer(vec![body_peer], body_peer, 3);

        assert_eq!(ordered, vec![body_peer]);
    }

    #[test]
    fn chunk_ranges_splits_by_fixed_target_size() {
        assert_eq!(chunk_ranges(128, 32), vec![0..32, 32..64, 64..96, 96..128]);
        assert_eq!(chunk_ranges(33, 16), vec![0..16, 16..32, 32..33]);
        assert_eq!(chunk_ranges(17, 32), vec![0..17]);
    }

    #[test]
    fn request_window_limit_scales_in_flight_requests_by_peer_count() {
        assert_eq!(request_window_limit(0, 16), 0);
        assert_eq!(request_window_limit(1, 16), 4);
        assert_eq!(request_window_limit(2, 16), 8);
        assert_eq!(request_window_limit(8, 16), 16);
    }

    #[test]
    fn paired_body_receipt_window_counts_both_request_types() {
        assert_eq!(paired_body_receipt_chunk_window_limit(0, 16), 0);
        assert_eq!(paired_body_receipt_chunk_window_limit(1, 16), 2);
        assert_eq!(paired_body_receipt_chunk_window_limit(2, 16), 4);
        assert_eq!(paired_body_receipt_chunk_window_limit(8, 16), 8);
        assert_eq!(paired_body_receipt_chunk_window_limit(64, 128), 64);
    }

    #[test]
    fn body_receipt_chunk_limit_caps_large_adaptive_limits() {
        assert_eq!(body_receipt_chunk_cap(31), 128);
        assert_eq!(body_receipt_chunk_cap(32), 128);
        assert_eq!(body_receipt_chunk_limit(128, 128, 128), 128);
        assert_eq!(body_receipt_chunk_limit(16, 128, 128), 16);
        assert_eq!(body_receipt_chunk_limit(128, 8, 128), 8);
    }

    #[test]
    fn gas_limited_chunk_ranges_keep_sparse_windows_wide() {
        let sparse = vec![0; 256];

        assert_eq!(
            chunk_ranges_with_optional_gas(256, |_| 128, Some(&sparse)),
            vec![0..128, 128..256]
        );
    }

    #[test]
    fn body_receipt_return_blocks_scales_dense_prefix_and_caps_sparse_windows() {
        let dense = vec![30_000_000; 4096];
        let sparse = vec![0; 12_000];

        assert_eq!(body_receipt_return_blocks(128, Some(&dense)), 128);
        assert_eq!(body_receipt_return_blocks(4096, Some(&dense)), 4096);
        assert_eq!(body_receipt_return_blocks(6000, Some(&sparse)), 6000);
        assert_eq!(body_receipt_return_blocks(12_000, Some(&sparse)), 10_000);
        assert_eq!(body_receipt_return_blocks(4096, None), 1024);
    }

    #[test]
    fn body_receipt_min_accepted_prefix_rejects_tiny_dense_progress() {
        assert_eq!(body_receipt_min_accepted_prefix(128), 128);
        assert_eq!(body_receipt_min_accepted_prefix(1024), 384);
        assert_eq!(body_receipt_min_accepted_prefix(10_000), 384);
    }

    #[test]
    fn body_receipt_scheduled_chunk_limit_caps_to_prefix_chunks() {
        let ranges = vec![0..32, 32..64, 64..96, 96..128, 128..160, 160..192];

        assert_eq!(body_receipt_scheduled_chunk_limit(&ranges, 0), 0);
        assert_eq!(body_receipt_scheduled_chunk_limit(&ranges, 32), 1);
        assert_eq!(body_receipt_scheduled_chunk_limit(&ranges, 96), 3);
        assert_eq!(body_receipt_scheduled_chunk_limit(&ranges, 4096), 6);
    }

    #[test]
    fn body_receipt_prefix_range_truncates_to_return_boundary() {
        assert_eq!(body_receipt_prefix_range(0..64, 128), Some(0..64));
        assert_eq!(body_receipt_prefix_range(64..160, 128), Some(64..128));
        assert_eq!(body_receipt_prefix_range(128..192, 128), None);
    }

    #[test]
    fn body_receipt_hedge_candidate_allows_bounded_rehedges_for_blocking_gap() {
        let start = Instant::now();
        let mut in_flight = HashMap::from([(
            0,
            InFlightBodyReceiptChunk {
                range: 0..32,
                chunk_index: 0,
                attempts: 1,
                last_hedged_at: start - PIPELINED_BODY_RECEIPT_HEDGE_DELAY,
                hedges: 0,
            },
        )]);
        let chunks = BTreeMap::<usize, Vec<SourcedBodyReceipts>>::new();

        assert_eq!(
            body_receipt_hedge_candidate(&mut in_flight, &chunks, 1024, start),
            Some((0..32, 0))
        );
        assert!(body_receipt_hedge_candidate(&mut in_flight, &chunks, 1024, start).is_none());
        assert_eq!(
            body_receipt_hedge_candidate(
                &mut in_flight,
                &chunks,
                1024,
                start + PIPELINED_BODY_RECEIPT_HEDGE_DELAY
            ),
            Some((0..32, 0))
        );
        assert!(
            body_receipt_hedge_candidate(
                &mut in_flight,
                &chunks,
                1024,
                start + (PIPELINED_BODY_RECEIPT_HEDGE_DELAY * 2)
            )
            .is_none()
        );
    }

    #[test]
    fn take_contiguous_body_receipt_prefix_returns_deterministic_prefix() {
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![0, 1, 2, 3]);
        chunks.insert(4, vec![4, 5, 6, 7]);
        chunks.insert(8, vec![8, 9, 10, 11]);

        let blocks = take_contiguous_prefix(6, chunks);

        assert_eq!(blocks, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn gas_limited_chunk_ranges_split_dense_receipt_windows() {
        let gas_used = vec![30_000_000; 20];

        assert_eq!(
            chunk_ranges_with_optional_gas(20, |_| 20, Some(&gas_used)),
            vec![0..20]
        );
        assert_eq!(
            chunk_ranges_with_optional_gas(20, |_| 8, Some(&gas_used)),
            vec![0..8, 8..16, 16..20]
        );
        let large_gas_window = vec![30_000_000; 70];
        assert_eq!(
            chunk_ranges_with_optional_gas(70, |_| 80, Some(&large_gas_window)),
            vec![0..32, 32..64, 64..70]
        );
    }

    #[test]
    fn missing_chunk_ranges_only_returns_absent_or_incomplete_ranges() {
        let peer = PeerId::repeat_byte(0x11);
        let ranges = vec![0..4, 4..8, 8..12];
        let mut chunks = BTreeMap::new();
        chunks.insert(0, (peer, vec![1u8, 2, 3, 4]));
        chunks.insert(4, (peer, vec![5u8, 6]));

        assert_eq!(missing_chunk_ranges(&ranges, &chunks), vec![4..8, 8..12]);
    }

    #[test]
    fn disabled_chunk_peers_only_tracks_retry_disabling_failures_for_role() {
        let body_peer = PeerId::repeat_byte(0x11);
        let receipt_peer = PeerId::repeat_byte(0x22);
        let failures = vec![
            ChunkRequestFailure {
                role: ChunkRequestRole::Bodies,
                peer_id: body_peer,
                requested: 16,
                kind: ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout,
                )),
            },
            ChunkRequestFailure {
                role: ChunkRequestRole::Receipts,
                peer_id: receipt_peer,
                requested: 16,
                kind: ChunkFailureKind::Incomplete { returned: 0 },
            },
        ];

        assert_eq!(
            disabled_chunk_peers(&failures, ChunkRequestRole::Bodies),
            HashSet::from([body_peer])
        );
    }

    #[test]
    fn receipt_count_mismatch_only_treats_overflow_as_protocol_breach() {
        assert!(!receipt_count_mismatch_is_protocol_breach(
            ReceiptCountMismatch {
                response_kind: "receipts",
                requested_blocks: 16,
                returned_blocks: 0,
                block_index: None,
                expected_receipts: None,
                returned_receipts: None,
            }
        ));
        assert!(!receipt_count_mismatch_is_protocol_breach(
            ReceiptCountMismatch {
                response_kind: "receipts",
                requested_blocks: 16,
                returned_blocks: 16,
                block_index: Some(3),
                expected_receipts: Some(12),
                returned_receipts: Some(0),
            }
        ));
        assert!(receipt_count_mismatch_is_protocol_breach(
            ReceiptCountMismatch {
                response_kind: "receipts",
                requested_blocks: 16,
                returned_blocks: 17,
                block_index: None,
                expected_receipts: None,
                returned_receipts: None,
            }
        ));
        assert!(receipt_count_mismatch_is_protocol_breach(
            ReceiptCountMismatch {
                response_kind: "receipts",
                requested_blocks: 16,
                returned_blocks: 16,
                block_index: Some(3),
                expected_receipts: Some(12),
                returned_receipts: Some(13),
            }
        ));
    }

    fn fake_receipt(gas: u64) -> LogexReceipt {
        LogexReceipt {
            tx_type: TxType::Legacy,
            status: Eip658Value::success(),
            cumulative_gas_used: gas,
            logs: Vec::<Log>::new(),
        }
    }

    #[test]
    fn eth70_partial_receipts_are_merged_across_requests() {
        let mut merged = ReceiptBatch::new();
        let mut bloom_cache = ReceiptBloomCache::default();

        let (next_block_index, first_block_receipt_index) = merge_receipts70_response(
            &mut merged,
            0,
            0,
            Receipts70 {
                last_block_incomplete: true,
                receipts: vec![
                    vec![fake_receipt(1)],
                    vec![fake_receipt(2)],
                    vec![fake_receipt(3)],
                ],
            },
            3,
            &mut bloom_cache,
        )
        .expect("first partial response should merge");

        assert_eq!(next_block_index, 2);
        assert_eq!(first_block_receipt_index, 1);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[2].len(), 1);

        let (next_block_index, first_block_receipt_index) = merge_receipts70_response(
            &mut merged,
            next_block_index,
            first_block_receipt_index,
            Receipts70 {
                last_block_incomplete: false,
                receipts: vec![vec![fake_receipt(4)]],
            },
            3,
            &mut bloom_cache,
        )
        .expect("continuation response should merge");

        assert_eq!(next_block_index, 3);
        assert_eq!(first_block_receipt_index, 0);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[2].len(), 2);
    }

    #[test]
    fn eth70_empty_response_is_rejected() {
        let mut merged = ReceiptBatch::new();
        let mut bloom_cache = ReceiptBloomCache::default();

        let error = merge_receipts70_response(
            &mut merged,
            0,
            0,
            Receipts70 {
                last_block_incomplete: false,
                receipts: Vec::<Vec<LogexReceipt>>::new(),
            },
            1,
            &mut bloom_cache,
        )
        .expect_err("empty response should be rejected");

        assert_eq!(error, Receipts70MergeError::EmptyResponse);
    }
}
