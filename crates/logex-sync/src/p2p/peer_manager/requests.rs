use alloy_eips::BlockHashOrNumber;
use eyre::{Result, bail};
use futures_util::{FutureExt, StreamExt};
use reth_primitives_traits::BlockBody as _;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, trace, warn};

use crate::primitives::{
    LogexReceipt, ReceiptBloomCache, logex_receipt_batches_with_cached_blooms,
};

use super::*;

const PIPELINED_CHUNK_REQUEST_PEERS: usize = 3;
const PIPELINED_GAP_RETRY_ROUNDS: usize = 2;
const PIPELINED_BODY_RECEIPT_HEDGE_DELAY: Duration = Duration::from_millis(1_500);
const PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const PIPELINED_BODY_RECEIPT_PLAN_TIMEOUT: Duration = Duration::from_secs(45);
const PIPELINED_BODY_RECEIPT_MAX_HEDGES: usize = 64;
const PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK: usize = 4;
const PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS: usize = 16;
const PIPELINED_BODY_RECEIPT_FULL_PREFIX_MIN_PEERS: usize =
    PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS;
const PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANT_CHUNKS: usize = 4;
const PIPELINED_BODY_RECEIPT_PREFIX_HEDGE_SPARE_ATTEMPTS: usize = 4;
const PIPELINED_BODY_RECEIPT_FAST_POOL_MIN_PEERS: usize = 32;
const PIPELINED_BODY_RECEIPT_FAST_POOL_SIZE: usize = 24;
const PIPELINED_BODY_RECEIPT_IDLE_POOL_MIN_PEERS: usize = 4;
const PIPELINED_BODY_RECEIPT_SERVING_POOL_MIN_PEERS: usize = 16;
const PIPELINED_BODY_RECEIPT_SERVING_POOL_PROBE_PEERS: usize = 8;
const PIPELINED_BODY_RECEIPT_DECOUPLED_DENSE: bool = true;
const PIPELINED_BODY_RECEIPT_DECOUPLED_MIN_PEERS: usize = 8;
const PIPELINED_BODY_RECEIPT_CHUNK_BLOCKS_DEFAULT: usize = 128;
const PIPELINED_BODY_RECEIPT_DENSE_CHUNK_BLOCKS: usize = 48;
const PIPELINED_BODY_RECEIPT_VERY_DENSE_CHUNK_BLOCKS: usize = 32;
const PIPELINED_BODY_RECEIPT_CHUNK_GAS_TARGET: u64 = 960_000_000;
const PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS: usize = 512;
const PIPELINED_BODY_RECEIPT_DENSE_MIN_ACCEPTED_PREFIX_BLOCKS: usize =
    PIPELINED_BODY_RECEIPT_CHUNK_BLOCKS_DEFAULT / 2;
const PIPELINED_BODY_RECEIPT_MIN_ACCEPTED_PREFIX_BLOCKS: usize =
    PIPELINED_BODY_RECEIPT_CHUNK_BLOCKS_DEFAULT;
const PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS: usize = 16;
const PIPELINED_BODY_RECEIPT_MAX_CONTIGUOUS_RETURN_BLOCKS: usize = 10_000;
const PIPELINED_BODY_RECEIPT_DENSE_RETURN_ROWS_PER_BLOCK: f64 = 100.0;
const PIPELINED_BODY_RECEIPT_VERY_DENSE_RETURN_ROWS_PER_BLOCK: f64 = 1_500.0;
const PIPELINED_BODY_RECEIPT_RETURN_GAS_PER_BLOCK_TARGET: u128 = 30_000_000;
const PARALLEL_CHUNK_RETRY_ROUNDS: usize = 2;
const PARALLEL_CHUNK_SALVAGE_PEER_LIMIT: usize = 4;
const PARALLEL_REQUESTS_PER_PEER_LOW: usize = 1;
const PARALLEL_REQUESTS_PER_PEER_HIGH: usize = 2;
const PARALLEL_HIGH_FANOUT_MIN_PEERS: usize = 16;
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
type ParallelSourcedReceipts = (Vec<SourcedReceiptSet>, RequestStats, ParallelChunkFailures);
type ParallelReceipts = (PeerId, ReceiptBatch, RequestStats, ParallelChunkFailures);
type DecoupledBodyChunkResult = (
    usize,
    std::ops::Range<usize>,
    PeerId,
    usize,
    Duration,
    std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        ChunkFailureKind,
    >,
);
type DecoupledReceiptChunkResult = (
    usize,
    std::ops::Range<usize>,
    PeerId,
    usize,
    Duration,
    std::result::Result<ReceiptBatch, ChunkFailureKind>,
);

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

struct BodyReceiptChunkAttempt {
    chunk: BodyReceiptChunk,
    body_peer: Option<PeerId>,
    receipt_peer: Option<PeerId>,
}

#[derive(Default)]
struct BodyReceiptPlanPeerState {
    body_in_flight_peers: HashMap<PeerId, usize>,
    receipt_in_flight_peers: HashMap<PeerId, usize>,
    body_bad_peers: HashSet<PeerId>,
    receipt_bad_peers: HashSet<PeerId>,
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
    peer_rotation: usize,
    peers: HashMap<PeerId, RequestPeerSnapshot>,
    accounting_tx: Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
}

pub(crate) struct BodyReceiptRequestOutcome {
    total_hashes: usize,
    return_blocks: usize,
    planned_return_blocks: usize,
    chunks: BTreeMap<usize, Vec<SourcedBodyReceipts>>,
    failures: ParallelChunkFailures,
    stats: TypedRequestStats,
    accounting_forwarded: bool,
}

pub(crate) struct BodyReceiptRequestAccounting {
    stats: TypedRequestStats,
    failures: ParallelChunkFailures,
    active_requests: Vec<BodyReceiptActiveRequest>,
}

#[derive(Debug, Clone)]
pub(super) struct BodyReceiptActiveRequest {
    pub(super) peer_id: PeerId,
    pub(super) kind: PeerRequestKind,
    pub(super) delta: BodyReceiptActiveRequestDelta,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum BodyReceiptActiveRequestDelta {
    Started,
    Finished,
}

struct BodyReceiptActiveRequestGuard {
    accounting_tx: Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
    peer_id: PeerId,
    kind: PeerRequestKind,
    active: bool,
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

struct HeaderPageResult {
    page_index: usize,
    requested: u64,
    success: Option<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
        Duration,
    )>,
    failures: Vec<(PeerId, RequestAttempt)>,
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
        self.get_headers_from_peers(request, Some(start_block), None, None)
            .await
    }

    pub async fn get_headers_with_limits(
        &mut self,
        start_block: u64,
        count: u64,
        request_timeout: Duration,
        max_attempts: usize,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        let request = HeadersRequest::rising(start_block.into(), count);
        self.get_headers_from_peers(
            request,
            Some(start_block),
            Some(request_timeout),
            Some(max_attempts),
        )
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
        self.get_headers_from_peers(request, required_block, None, None)
            .await
    }

    pub(crate) async fn get_headers_reverse_pages(
        &mut self,
        child_block: u64,
        total_count: u64,
        page_limit: u64,
        required_block: u64,
    ) -> Result<
        Vec<(
            PeerId,
            Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
        )>,
    > {
        self.drain_events_now();
        if child_block == 0 || total_count == 0 || page_limit == 0 {
            return Ok(Vec::new());
        }

        let mut peer_ids = self
            .peer_ids_for_block_requests(Some(required_block), &[])
            .await;
        self.sort_peer_ids_by_request_performance(&mut peer_ids, PeerRequestKind::Headers);
        if peer_ids.is_empty() {
            bail!("no peers available to handle reverse header page request")
        }

        let peers = peer_ids
            .iter()
            .filter_map(|peer_id| {
                self.peers
                    .get(peer_id)
                    .map(|peer| (*peer_id, peer.sender.clone()))
            })
            .collect::<Vec<_>>();
        if peers.is_empty() {
            bail!("no connected peers available to handle reverse header page request")
        }

        let mut attempts = futures_util::stream::FuturesUnordered::new();
        let mut offset = 0u64;
        let mut page_index = 0usize;
        while offset < total_count {
            let Some(start_block) = child_block
                .checked_sub(1)
                .and_then(|block| block.checked_sub(offset))
            else {
                break;
            };
            let request_count = page_limit.min(total_count - offset);
            let request =
                HeadersRequest::falling(BlockHashOrNumber::Number(start_block), request_count);
            let mut candidates = peers.clone();
            if !candidates.is_empty() {
                let shift = page_index % candidates.len();
                candidates.rotate_left(shift);
            }
            attempts
                .push(request_header_page_from_candidates(page_index, request, candidates).boxed());
            offset = offset.saturating_add(request_count);
            page_index = page_index.saturating_add(1);
        }

        let mut page_results = Vec::new();
        while let Some(result) = attempts.next().await {
            page_results.push(result);
        }
        page_results.sort_by_key(|result| result.page_index);

        let mut dead_peers = HashSet::new();
        let mut saw_empty_response = false;
        let mut pages = Vec::new();
        for result in page_results {
            for (peer_id, error) in result.failures {
                let should_drop = self.on_request_error(peer_id, PeerRequestKind::Headers, &error);
                debug!(peer = %peer_id, ?error, "reverse header page request failed");
                if should_drop {
                    dead_peers.insert(peer_id);
                }
            }

            let Some((peer_id, headers, elapsed)) = result.success else {
                break;
            };
            if headers.len() > result.requested as usize {
                self.on_invalid_response_length(
                    peer_id,
                    "headers",
                    result.requested as usize,
                    headers.len(),
                );
                dead_peers.insert(peer_id);
                break;
            }
            if headers.is_empty() && result.requested > 0 {
                saw_empty_response = true;
                self.on_zero_progress_response(
                    peer_id,
                    PeerRequestKind::Headers,
                    "headers",
                    result.requested as usize,
                );
                break;
            }
            self.record_peer_request_success(
                peer_id,
                PeerRequestKind::Headers,
                headers.len(),
                elapsed,
            );
            pages.push((peer_id, headers));
        }

        self.advance_request_cursor();
        self.remove_dead_peers(&dead_peers);
        if pages.is_empty() && saw_empty_response {
            return Ok(Vec::new());
        }
        if pages.is_empty() {
            bail!("no peers available to handle reverse header page request")
        }
        Ok(pages)
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
            .get_headers_from_peers(HeadersRequest::one(id), required_block, None, None)
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
        self.get_bodies_prefer_peers_inner(hashes, required_block, preferred_peers, None, None)
            .await
    }

    pub async fn get_bodies_prefer_peers_with_limits(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        preferred_peers: &[PeerId],
        request_timeout: Duration,
        max_attempts: usize,
    ) -> Result<Vec<SourcedBlockBody>> {
        self.get_bodies_prefer_peers_inner(
            hashes,
            required_block,
            preferred_peers,
            Some(request_timeout),
            Some(max_attempts),
        )
        .await
    }

    async fn get_bodies_prefer_peers_inner(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        preferred_peers: &[PeerId],
        request_timeout: Option<Duration>,
        max_attempts: Option<usize>,
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

        for (attempt_index, peer_id) in peer_ids.into_iter().enumerate() {
            if max_attempts.is_some_and(|limit| attempt_index >= limit.max(1)) {
                break;
            }
            while !remaining_hashes.is_empty() {
                let request_hashes = remaining_hashes.clone();
                let started_at = Instant::now();
                let bodies = match self
                    .request_bodies_with_timeout(peer_id, request_hashes.clone(), request_timeout)
                    .await
                {
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
            None,
            required_block,
            preferred_peers,
            &[],
        )
        .await
    }

    pub(crate) async fn prepare_bodies_and_receipts_request_for_hashes_and_gas(
        &mut self,
        hashes: Vec<B256>,
        gas_used: Vec<u64>,
        rows_per_block: Option<f64>,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Result<Option<BodyReceiptRequestPlan>> {
        self.prepare_bodies_and_receipts_request_inner(
            hashes,
            Some(gas_used),
            rows_per_block,
            required_block,
            preferred_peers,
            &[],
        )
        .await
    }

    pub(crate) async fn prepare_bodies_and_receipts_request_for_hashes_and_gas_excluding(
        &mut self,
        hashes: Vec<B256>,
        gas_used: Vec<u64>,
        rows_per_block: Option<f64>,
        required_block: u64,
        preferred_peers: &[PeerId],
        excluded_peers: &[PeerId],
    ) -> Result<Option<BodyReceiptRequestPlan>> {
        self.prepare_bodies_and_receipts_request_inner(
            hashes,
            Some(gas_used),
            rows_per_block,
            required_block,
            preferred_peers,
            excluded_peers,
        )
        .await
    }

    async fn prepare_bodies_and_receipts_request_inner(
        &mut self,
        hashes: Vec<B256>,
        receipt_gas_used: Option<Vec<u64>>,
        rows_per_block: Option<f64>,
        required_block: u64,
        preferred_peers: &[PeerId],
        excluded_peers: &[PeerId],
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
        body_peer_ids.retain(|peer_id| !excluded_peers.contains(peer_id));
        self.filter_paused_request_peers(&mut body_peer_ids, PeerRequestKind::Bodies);
        retain_idle_body_receipt_candidate_pool_if_enough(
            &self.peers,
            &mut body_peer_ids,
            PeerRequestKind::Bodies,
        );
        retain_serving_body_receipt_candidate_pool_if_enough(&self.peers, &mut body_peer_ids);
        self.sort_peer_ids_by_request_performance(&mut body_peer_ids, PeerRequestKind::Bodies);
        limit_body_receipt_candidate_pool(&mut body_peer_ids);
        if body_peer_ids.is_empty() {
            return Ok(None);
        }

        let mut receipt_peer_ids = self
            .peer_ids_for_receipt_requests(required_block, preferred_peers)
            .await;
        receipt_peer_ids.retain(|peer_id| !excluded_peers.contains(peer_id));
        self.filter_paused_request_peers(&mut receipt_peer_ids, PeerRequestKind::Receipts);
        retain_idle_body_receipt_candidate_pool_if_enough(
            &self.peers,
            &mut receipt_peer_ids,
            PeerRequestKind::Receipts,
        );
        retain_serving_body_receipt_candidate_pool_if_enough(&self.peers, &mut receipt_peer_ids);
        self.sort_peer_ids_by_request_performance(&mut receipt_peer_ids, PeerRequestKind::Receipts);
        limit_body_receipt_candidate_pool(&mut receipt_peer_ids);
        if receipt_peer_ids.is_empty() {
            return Ok(None);
        }

        let ranges = self.body_receipt_chunk_ranges(
            hashes.len(),
            &body_peer_ids,
            &receipt_peer_ids,
            receipt_gas_used.as_deref(),
            rows_per_block,
        );
        if ranges.len() < 2 {
            return Ok(None);
        }
        let return_blocks =
            body_receipt_return_blocks(hashes.len(), receipt_gas_used.as_deref(), rows_per_block);
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
            peer_rotation: self.request_cursor,
            peers,
            accounting_tx: None,
        }))
    }

    pub(crate) fn complete_bodies_and_receipts_request(
        &mut self,
        outcome: BodyReceiptRequestOutcome,
    ) -> Result<Option<BodyReceiptRequestCompletion>> {
        self.complete_bodies_and_receipts_request_with_min_prefix(outcome, None)
    }

    pub(crate) fn complete_residual_bodies_and_receipts_request(
        &mut self,
        outcome: BodyReceiptRequestOutcome,
    ) -> Result<Option<BodyReceiptRequestCompletion>> {
        self.complete_bodies_and_receipts_request_with_min_prefix(
            outcome,
            Some(PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS),
        )
    }

    fn complete_bodies_and_receipts_request_with_min_prefix(
        &mut self,
        mut outcome: BodyReceiptRequestOutcome,
        min_accepted_prefix_override: Option<usize>,
    ) -> Result<Option<BodyReceiptRequestCompletion>> {
        self.apply_body_receipt_request_accounting(&mut outcome);
        let BodyReceiptRequestOutcome {
            total_hashes,
            return_blocks,
            planned_return_blocks,
            chunks,
            failures: _,
            stats: _,
            accounting_forwarded: _,
        } = outcome;

        let completion_return_blocks = body_receipt_completion_return_blocks(
            return_blocks,
            planned_return_blocks,
            total_hashes,
        );
        let blocks = take_contiguous_body_receipt_prefix(completion_return_blocks, chunks);

        let min_accepted_prefix = min_accepted_prefix_override
            .map(|prefix| {
                body_receipt_min_accepted_prefix_override(completion_return_blocks, prefix)
            })
            .unwrap_or_else(|| body_receipt_min_accepted_prefix(completion_return_blocks));
        if blocks.len() >= min_accepted_prefix {
            self.advance_request_cursor();
            Ok(Some(BodyReceiptRequestCompletion {
                blocks,
                planned_return_blocks,
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

    pub(crate) fn apply_body_receipt_request_accounting(
        &mut self,
        outcome: &mut BodyReceiptRequestOutcome,
    ) {
        if outcome.accounting_forwarded {
            return;
        }
        let (stats, failures) = take_body_receipt_request_accounting(outcome);
        self.apply_body_receipt_request_accounting_parts(stats, failures);
    }

    pub(crate) fn apply_body_receipt_request_accounting_events(
        &mut self,
        accountings: impl IntoIterator<Item = BodyReceiptRequestAccounting>,
    ) {
        let mut stats = Vec::new();
        let mut failures = Vec::new();
        for accounting in accountings {
            let BodyReceiptRequestAccounting {
                stats: event_stats,
                failures: event_failures,
                active_requests,
            } = accounting;
            self.apply_body_receipt_active_request_deltas(active_requests);
            stats.extend(event_stats);
            failures.extend(event_failures);
        }
        self.apply_body_receipt_request_accounting_parts(stats, failures);
    }

    fn apply_body_receipt_request_accounting_parts(
        &mut self,
        stats: TypedRequestStats,
        failures: ParallelChunkFailures,
    ) {
        if stats.is_empty() && failures.is_empty() {
            return;
        }

        let mut dead_peers = HashSet::new();
        for (peer_id, kind, blocks, elapsed) in stats {
            self.record_peer_request_success(peer_id, kind, blocks, elapsed);
        }
        self.apply_parallel_chunk_failures("body/receipt chunks", failures, &mut dead_peers);
        self.remove_dead_peers(&dead_peers);
    }
}

fn take_body_receipt_request_accounting(
    outcome: &mut BodyReceiptRequestOutcome,
) -> (TypedRequestStats, ParallelChunkFailures) {
    (
        std::mem::take(&mut outcome.stats),
        std::mem::take(&mut outcome.failures),
    )
}

impl BodyReceiptActiveRequestGuard {
    fn new(
        accounting_tx: &Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
        peer_id: PeerId,
        kind: PeerRequestKind,
    ) -> Self {
        emit_body_receipt_active_request_delta(
            accounting_tx,
            peer_id,
            kind,
            BodyReceiptActiveRequestDelta::Started,
        );
        Self {
            accounting_tx: accounting_tx.clone(),
            peer_id,
            kind,
            active: true,
        }
    }
}

impl Drop for BodyReceiptActiveRequestGuard {
    fn drop(&mut self) {
        if self.active {
            emit_body_receipt_active_request_delta(
                &self.accounting_tx,
                self.peer_id,
                self.kind,
                BodyReceiptActiveRequestDelta::Finished,
            );
            self.active = false;
        }
    }
}

fn emit_body_receipt_active_request_delta(
    accounting_tx: &Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
    peer_id: PeerId,
    kind: PeerRequestKind,
    delta: BodyReceiptActiveRequestDelta,
) {
    let Some(accounting_tx) = accounting_tx else {
        return;
    };

    let _ = accounting_tx.send(BodyReceiptRequestAccounting {
        stats: Vec::new(),
        failures: Vec::new(),
        active_requests: vec![BodyReceiptActiveRequest {
            peer_id,
            kind,
            delta,
        }],
    });
}

fn emit_body_receipt_request_accounting(
    accounting_tx: &Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
    stats: TypedRequestStats,
    failures: ParallelChunkFailures,
) {
    if stats.is_empty() && failures.is_empty() {
        return;
    }

    let Some(accounting_tx) = accounting_tx else {
        return;
    };

    let _ = accounting_tx.send(BodyReceiptRequestAccounting {
        stats,
        failures,
        active_requests: Vec::new(),
    });
}

fn emit_body_receipt_role_success(
    accounting_tx: &Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
    peer_id: PeerId,
    kind: PeerRequestKind,
    blocks: usize,
    elapsed: Duration,
) {
    emit_body_receipt_request_accounting(
        accounting_tx,
        vec![(peer_id, kind, blocks, elapsed)],
        Vec::new(),
    );
}

fn emit_body_receipt_role_failure(
    accounting_tx: &Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
    failure: ChunkRequestFailure,
) {
    emit_body_receipt_request_accounting(accounting_tx, Vec::new(), vec![failure]);
}

impl BodyReceiptRequestPlan {
    pub(crate) fn planned_prefix_blocks(&self) -> usize {
        planned_body_receipt_prefix_blocks(&self.ranges, self.return_blocks, self.hashes.len())
    }

    pub(crate) fn with_accounting_tx(
        mut self,
        accounting_tx: mpsc::UnboundedSender<BodyReceiptRequestAccounting>,
    ) -> Self {
        self.accounting_tx = Some(accounting_tx);
        self
    }

    pub(crate) fn with_peer_rotation_offset(mut self, offset: usize) -> Self {
        self.peer_rotation = self.peer_rotation.wrapping_add(offset);
        self
    }

    pub(crate) async fn execute(self) -> BodyReceiptRequestOutcome {
        if self.should_use_decoupled_dense_pipeline() {
            let outcome = self.execute_decoupled_dense().await;
            if !outcome.chunks.is_empty() {
                return outcome;
            }
            debug!(
                total_hashes = outcome.total_hashes,
                return_blocks = outcome.return_blocks,
                failures = outcome.failures.len(),
                stats = outcome.stats.len(),
                "decoupled body/receipt pipeline did not produce a prefix, falling back"
            );
            let mut fallback = self.execute_paired().await;
            if !outcome.accounting_forwarded {
                fallback.stats.extend(outcome.stats);
                fallback.failures.extend(outcome.failures);
            }
            return fallback;
        }

        self.execute_paired().await
    }

    fn should_use_decoupled_dense_pipeline(&self) -> bool {
        PIPELINED_BODY_RECEIPT_DECOUPLED_DENSE
            && self.return_blocks <= PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS
            && self.body_peer_ids.len().min(self.receipt_peer_ids.len())
                >= PIPELINED_BODY_RECEIPT_DECOUPLED_MIN_PEERS
    }

    async fn execute_decoupled_dense(&self) -> BodyReceiptRequestOutcome {
        let accounting_forwarded = self.accounting_tx.is_some();
        let return_blocks = self.return_blocks.min(self.hashes.len());
        let hashes = self.hashes[..return_blocks].to_vec();
        let ranges = self
            .ranges
            .iter()
            .cloned()
            .filter_map(|range| body_receipt_prefix_range(range, return_blocks))
            .collect::<Vec<_>>();
        let bodies_request = self.request_decoupled_body_chunks(
            &self.body_peer_ids,
            hashes.clone(),
            ranges.clone(),
            self.peer_rotation,
        );
        let receipts_request = self.request_decoupled_sourced_receipt_chunks(
            &self.receipt_peer_ids,
            hashes,
            ranges,
            self.peer_rotation,
        );
        let (bodies_result, receipts_result) = tokio::join!(bodies_request, receipts_request);

        let mut chunks = BTreeMap::new();
        let mut failures = Vec::new();
        let mut stats = Vec::new();

        let bodies = match bodies_result {
            Ok(Some((bodies, body_stats, body_failures))) => {
                stats.extend(body_stats.into_iter().map(|(peer_id, blocks, elapsed)| {
                    (peer_id, PeerRequestKind::Bodies, blocks, elapsed)
                }));
                failures.extend(body_failures);
                Some(bodies)
            }
            Ok(None) => None,
            Err((body_failures, body_stats)) => {
                stats.extend(body_stats.into_iter().map(|(peer_id, blocks, elapsed)| {
                    (peer_id, PeerRequestKind::Bodies, blocks, elapsed)
                }));
                failures.extend(body_failures);
                None
            }
        };

        let receipts = match receipts_result {
            Ok(Some((receipts, receipt_stats, receipt_failures))) => {
                stats.extend(receipt_stats.into_iter().map(|(peer_id, blocks, elapsed)| {
                    (peer_id, PeerRequestKind::Receipts, blocks, elapsed)
                }));
                failures.extend(receipt_failures);
                Some(receipts)
            }
            Ok(None) => None,
            Err((receipt_failures, receipt_stats)) => {
                stats.extend(receipt_stats.into_iter().map(|(peer_id, blocks, elapsed)| {
                    (peer_id, PeerRequestKind::Receipts, blocks, elapsed)
                }));
                failures.extend(receipt_failures);
                None
            }
        };

        debug!(
            total_hashes = self.hashes.len(),
            return_blocks = self.return_blocks,
            accepted_prefix = decoupled_dense_accepted_prefix(
                return_blocks,
                self.body_peer_ids.len().min(self.receipt_peer_ids.len())
            ),
            body_prefix_blocks = bodies.as_ref().map_or(0, Vec::len),
            receipt_prefix_blocks = receipts.as_ref().map_or(0, Vec::len),
            failures = failures.len(),
            stats = stats.len(),
            "decoupled dense body/receipt plan completed"
        );

        if let (Some(mut bodies), Some(mut receipts)) = (bodies, receipts) {
            let prefix_len = bodies.len().min(receipts.len());
            let accepted_prefix = decoupled_dense_accepted_prefix(
                return_blocks,
                self.body_peer_ids.len().min(self.receipt_peer_ids.len()),
            );
            if prefix_len >= accepted_prefix {
                bodies.truncate(prefix_len);
                receipts.truncate(prefix_len);
                match body_receipt_blocks_if_sourced_counts_match(&bodies, receipts, prefix_len) {
                    Ok(blocks) => {
                        chunks.insert(0, blocks);
                    }
                    Err(error) => {
                        let (peer_id, kind) = *error;
                        failures.push(ChunkRequestFailure {
                            role: ChunkRequestRole::Receipts,
                            peer_id,
                            requested: prefix_len,
                            kind: ChunkFailureKind::ReceiptCountMismatch(kind),
                        });
                    }
                }
            }
        }

        BodyReceiptRequestOutcome {
            total_hashes: self.hashes.len(),
            return_blocks: self.return_blocks,
            planned_return_blocks: self.planned_prefix_blocks(),
            chunks,
            failures,
            stats,
            accounting_forwarded,
        }
    }

    async fn execute_paired(self) -> BodyReceiptRequestOutcome {
        let accounting_forwarded = self.accounting_tx.is_some();
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
            let mut peer_state = BodyReceiptPlanPeerState::default();
            let mut hedge_count = 0usize;
            let min_return_blocks = body_receipt_plan_progress_target(self.return_blocks);
            let max_scheduled_chunks =
                body_receipt_scheduled_chunk_limit(&self.ranges, min_return_blocks)
                    .min(self.max_in_flight);
            let max_hedged_attempts = self.max_in_flight.max(max_scheduled_chunks).saturating_add(
                body_receipt_prefix_hedge_spare_attempts(
                    min_return_blocks,
                    self.body_peer_ids.len().min(self.receipt_peer_ids.len()),
                ),
            );
            let mut scheduled_prefix_ranges = Vec::with_capacity(max_scheduled_chunks);
            for _ in 0..max_scheduled_chunks {
                let Some((chunk_index, range)) = pending_ranges.next() else {
                    break;
                };
                schedule_body_receipt_chunk_attempt(
                    &self,
                    &mut attempts,
                    &mut in_flight,
                    &mut peer_state,
                    range.clone(),
                    chunk_index,
                );
                scheduled_prefix_ranges.push((chunk_index, range));
            }

            let redundant_prefix_chunks = body_receipt_initial_prefix_redundancy_count(
                &self.ranges,
                min_return_blocks,
                max_scheduled_chunks,
                self.max_in_flight,
                self.body_peer_ids.len().min(self.receipt_peer_ids.len()),
            );
            for (duplicate_index, (base_chunk_index, range)) in scheduled_prefix_ranges
                .iter()
                .take(redundant_prefix_chunks)
                .cloned()
                .enumerate()
            {
                schedule_body_receipt_chunk_attempt(
                    &self,
                    &mut attempts,
                    &mut in_flight,
                    &mut peer_state,
                    range,
                    base_chunk_index
                        + ((PIPELINED_GAP_RETRY_ROUNDS + 1 + duplicate_index) * self.ranges.len()),
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
                            && attempts.len() < max_hedged_attempts
                            && let Some((range, chunk_index)) = body_receipt_hedge_candidate(
                                &mut in_flight,
                                &chunks,
                                min_return_blocks,
                                Instant::now(),
                            )
                        {
                            schedule_body_receipt_chunk_attempt(
                                &self,
                                &mut attempts,
                                &mut in_flight,
                                &mut peer_state,
                                range,
                                chunk_index
                                    + ((PIPELINED_GAP_RETRY_ROUNDS + 1 + hedge_count)
                                        * self.ranges.len()),
                            );
                            hedge_count += 1;
                        }
                        continue;
                    }
                };
                let BodyReceiptChunkAttempt {
                    chunk,
                    body_peer,
                    receipt_peer,
                } = chunk;
                release_body_receipt_attempt_peer(&mut peer_state.body_in_flight_peers, body_peer);
                release_body_receipt_attempt_peer(
                    &mut peer_state.receipt_in_flight_peers,
                    receipt_peer,
                );
                let chunk_start = chunk.start;
                complete_body_receipt_chunk_attempt(&mut in_flight, chunk_start);
                let chunk_already_completed = chunks.contains_key(&chunk_start);
                let chunk_failed = chunk.blocks.is_empty();
                emit_body_receipt_request_accounting(
                    &self.accounting_tx,
                    chunk.stats.clone(),
                    chunk.failures.clone(),
                );
                if !chunk_already_completed {
                    for failure in &chunk.failures {
                        if chunk_failure_disables_role_peer(failure) {
                            match failure.role {
                                ChunkRequestRole::Bodies => {
                                    peer_state.body_bad_peers.insert(failure.peer_id);
                                }
                                ChunkRequestRole::Receipts => {
                                    peer_state.receipt_bad_peers.insert(failure.peer_id);
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
                    schedule_body_receipt_chunk_attempt(
                        &self,
                        &mut attempts,
                        &mut in_flight,
                        &mut peer_state,
                        range,
                        chunk_index,
                    );
                }

                while attempts.len() < max_scheduled_chunks {
                    if contiguous_chunk_blocks(&chunks) >= min_return_blocks {
                        break;
                    }
                    let Some((chunk_index, range)) = pending_ranges.next() else {
                        break;
                    };
                    schedule_body_receipt_chunk_attempt(
                        &self,
                        &mut attempts,
                        &mut in_flight,
                        &mut peer_state,
                        range,
                        chunk_index,
                    );
                }

                while hedge_count < PIPELINED_BODY_RECEIPT_MAX_HEDGES
                    && attempts.len() < max_hedged_attempts
                {
                    let Some((range, chunk_index)) = body_receipt_hedge_candidate(
                        &mut in_flight,
                        &chunks,
                        min_return_blocks,
                        Instant::now(),
                    ) else {
                        break;
                    };
                    schedule_body_receipt_chunk_attempt(
                        &self,
                        &mut attempts,
                        &mut in_flight,
                        &mut peer_state,
                        range,
                        chunk_index
                            + ((PIPELINED_GAP_RETRY_ROUNDS + 1 + hedge_count) * self.ranges.len()),
                    );
                    hedge_count += 1;
                }
            }
        }

        let mut body_requests = 0usize;
        let mut receipt_requests = 0usize;
        let mut body_total_ms = 0u128;
        let mut receipt_total_ms = 0u128;
        let mut body_max_ms = 0u128;
        let mut receipt_max_ms = 0u128;
        let mut body_failures = 0usize;
        let mut receipt_failures = 0usize;
        for (_, kind, _, elapsed) in &stats {
            match kind {
                PeerRequestKind::Bodies => {
                    body_requests += 1;
                    let elapsed_ms = elapsed.as_millis();
                    body_total_ms += elapsed_ms;
                    body_max_ms = body_max_ms.max(elapsed_ms);
                }
                PeerRequestKind::Receipts => {
                    receipt_requests += 1;
                    let elapsed_ms = elapsed.as_millis();
                    receipt_total_ms += elapsed_ms;
                    receipt_max_ms = receipt_max_ms.max(elapsed_ms);
                }
                PeerRequestKind::Headers => {}
            }
        }
        for failure in &failures {
            match failure.role {
                ChunkRequestRole::Bodies => body_failures += 1,
                ChunkRequestRole::Receipts => receipt_failures += 1,
            }
        }
        let contiguous_blocks = contiguous_chunk_blocks(&chunks);
        let body_avg_ms = if body_requests == 0 {
            0
        } else {
            body_total_ms / body_requests as u128
        };
        let receipt_avg_ms = if receipt_requests == 0 {
            0
        } else {
            receipt_total_ms / receipt_requests as u128
        };
        debug!(
            total_hashes = self.hashes.len(),
            return_blocks = self.return_blocks,
            contiguous_blocks,
            completed_chunks = chunks.len(),
            failures = failures.len(),
            body_failures,
            receipt_failures,
            body_requests,
            receipt_requests,
            body_avg_ms,
            receipt_avg_ms,
            body_max_ms,
            receipt_max_ms,
            plan_ms = plan_started_at.elapsed().as_millis(),
            "body/receipt chunk pipeline plan completed"
        );

        BodyReceiptRequestOutcome {
            total_hashes: self.hashes.len(),
            return_blocks: self.return_blocks,
            planned_return_blocks: self.planned_prefix_blocks(),
            chunks,
            failures,
            stats,
            accounting_forwarded,
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
                let _active = BodyReceiptActiveRequestGuard::new(
                    &self.accounting_tx,
                    body_peer,
                    PeerRequestKind::Bodies,
                );
                let result = self
                    .request_bodies_until_complete(body_peer, body_hashes)
                    .await;
                (started_at.elapsed(), result, None)
            } else {
                let receipt_hashes = hashes.clone();
                let body_request = async {
                    let started_at = Instant::now();
                    let _active = BodyReceiptActiveRequestGuard::new(
                        &self.accounting_tx,
                        body_peer,
                        PeerRequestKind::Bodies,
                    );
                    let result = self
                        .request_bodies_until_complete(body_peer, body_hashes)
                        .await;
                    (started_at.elapsed(), result)
                };
                let receipt_request = async {
                    let started_at = Instant::now();
                    let _active = BodyReceiptActiveRequestGuard::new(
                        &self.accounting_tx,
                        first_receipt_peer,
                        PeerRequestKind::Receipts,
                    );
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
                let _active = BodyReceiptActiveRequestGuard::new(
                    &self.accounting_tx,
                    receipt_peer,
                    PeerRequestKind::Receipts,
                );
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

    async fn request_decoupled_body_chunks(
        &self,
        peer_ids: &[PeerId],
        hashes: Vec<B256>,
        ranges: Vec<std::ops::Range<usize>>,
        peer_rotation: usize,
    ) -> std::result::Result<Option<ParallelBodies>, ParallelChunkError> {
        if ranges.len() < 2 || peer_ids.is_empty() {
            return Ok(None);
        }
        let range_count = ranges.len();
        let range_indices_by_start: HashMap<usize, usize> = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect();
        let max_in_flight =
            request_window_limit(peer_ids.len(), MAX_PARALLEL_BODY_REQUESTS).min(ranges.len());
        if max_in_flight == 0 {
            return Ok(None);
        }
        let accepted_prefix = decoupled_dense_accepted_prefix(hashes.len(), peer_ids.len());

        let mut attempts = futures_util::stream::FuturesUnordered::new();
        let mut pending_ranges = ranges.iter().cloned().enumerate();
        let mut retry_counts = HashMap::<usize, usize>::new();
        let mut bad_peers = HashSet::<PeerId>::new();
        let redundant_prefix_chunks =
            decoupled_initial_prefix_redundancy_count(&ranges, hashes.len(), peer_ids.len());
        for _ in 0..max_in_flight {
            let Some((chunk_index, range)) = pending_ranges.next() else {
                break;
            };
            schedule_decoupled_body_chunk(
                self,
                &mut attempts,
                peer_ids,
                &hashes,
                rotated_chunk_index(chunk_index, peer_rotation),
                range,
            );
        }
        for (duplicate_index, range) in ranges
            .iter()
            .take(redundant_prefix_chunks)
            .cloned()
            .enumerate()
        {
            let base_chunk_index = range_indices_by_start
                .get(&range.start)
                .copied()
                .unwrap_or_default();
            schedule_decoupled_body_chunk(
                self,
                &mut attempts,
                peer_ids,
                &hashes,
                rotated_chunk_index(
                    base_chunk_index
                        + ((PARALLEL_CHUNK_RETRY_ROUNDS + 1 + duplicate_index) * range_count),
                    peer_rotation,
                ),
                range,
            );
        }

        let mut chunks = BTreeMap::new();
        let mut stats = Vec::new();
        let mut failures = Vec::new();
        while let Some((chunk_index, range, peer_id, requested, elapsed, result)) =
            attempts.next().await
        {
            let request_failed = result.is_err();
            let chunk_already_completed = chunks.contains_key(&range.start);
            match result {
                Ok(bodies) => {
                    stats.push((peer_id, bodies.len(), elapsed));
                    emit_body_receipt_role_success(
                        &self.accounting_tx,
                        peer_id,
                        PeerRequestKind::Bodies,
                        bodies.len(),
                        elapsed,
                    );
                    if !chunk_already_completed {
                        chunks.insert(range.start, (peer_id, bodies));
                    }
                }
                Err(kind) => {
                    if chunk_already_completed {
                        trace!(
                            peer = %peer_id,
                            requested,
                            ?kind,
                            "late duplicate body chunk request failed after chunk completion"
                        );
                        continue;
                    }
                    let failure = ChunkRequestFailure {
                        role: ChunkRequestRole::Bodies,
                        peer_id,
                        requested,
                        kind: kind.clone(),
                    };
                    emit_body_receipt_role_failure(&self.accounting_tx, failure.clone());
                    if chunk_failure_disables_role_peer(&failure) {
                        bad_peers.insert(peer_id);
                    }
                    failures.push(failure);
                    trace!(
                        peer = %peer_id,
                        requested,
                        ?kind,
                        "decoupled body chunk request failed"
                    );
                }
            }

            if missing_chunk_ranges(&ranges, &chunks).is_empty() {
                break;
            }
            if decoupled_dense_can_stop_early(
                contiguous_sourced_chunk_items(&chunks),
                accepted_prefix,
                hashes.len(),
            ) {
                break;
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
            let chunk_peer_ids = peer_ids_excluding(peer_ids, &bad_peers);
            if chunk_peer_ids.is_empty() {
                continue;
            }
            schedule_decoupled_body_chunk(
                self,
                &mut attempts,
                &chunk_peer_ids,
                &hashes,
                rotated_chunk_index(chunk_index, peer_rotation),
                range,
            );
        }

        let missing_prefix_ranges = missing_prefix_chunk_ranges(&ranges, &chunks, accepted_prefix);
        if !missing_prefix_ranges.is_empty() {
            bad_peers.extend(disabled_chunk_peers(&failures, ChunkRequestRole::Bodies));
            for range in missing_prefix_ranges {
                let base_chunk_index = range_indices_by_start
                    .get(&range.start)
                    .copied()
                    .unwrap_or_default();
                let mut retry_peer_ids = peer_ids_excluding(peer_ids, &bad_peers);
                rotate_request_candidates(
                    &mut retry_peer_ids,
                    rotated_chunk_index(
                        base_chunk_index + ((PARALLEL_CHUNK_RETRY_ROUNDS + 1) * range_count),
                        peer_rotation,
                    ),
                );
                for peer_id in retry_peer_ids
                    .into_iter()
                    .take(PARALLEL_CHUNK_SALVAGE_PEER_LIMIT)
                {
                    let request_hashes = hashes[range.clone()].to_vec();
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    let _active = BodyReceiptActiveRequestGuard::new(
                        &self.accounting_tx,
                        peer_id,
                        PeerRequestKind::Bodies,
                    );
                    match self
                        .request_bodies_until_complete(peer_id, request_hashes)
                        .await
                    {
                        Ok(bodies) => {
                            let elapsed = started_at.elapsed();
                            stats.push((peer_id, bodies.len(), elapsed));
                            emit_body_receipt_role_success(
                                &self.accounting_tx,
                                peer_id,
                                PeerRequestKind::Bodies,
                                bodies.len(),
                                elapsed,
                            );
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
                            emit_body_receipt_role_failure(&self.accounting_tx, failure.clone());
                            if chunk_failure_disables_role_peer(&failure) {
                                bad_peers.insert(peer_id);
                            }
                            failures.push(failure);
                            trace!(
                                peer = %peer_id,
                                requested,
                                ?kind,
                                "decoupled body chunk salvage request failed"
                            );
                        }
                    }
                }
            }
        }

        let bodies = sourced_bodies_from_chunks(hashes.len(), chunks);

        if bodies.len() >= accepted_prefix {
            Ok(Some((bodies, stats, failures)))
        } else if failures.is_empty() {
            Ok(None)
        } else {
            Err((failures, stats))
        }
    }

    async fn request_decoupled_sourced_receipt_chunks(
        &self,
        peer_ids: &[PeerId],
        hashes: Vec<B256>,
        ranges: Vec<std::ops::Range<usize>>,
        peer_rotation: usize,
    ) -> std::result::Result<Option<ParallelSourcedReceipts>, ParallelChunkError> {
        if ranges.len() < 2 || peer_ids.is_empty() {
            return Ok(None);
        }
        let range_count = ranges.len();
        let range_indices_by_start: HashMap<usize, usize> = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect();
        let max_in_flight =
            request_window_limit(peer_ids.len(), MAX_PARALLEL_RECEIPT_REQUESTS).min(ranges.len());
        if max_in_flight == 0 {
            return Ok(None);
        }
        let accepted_prefix = decoupled_dense_accepted_prefix(hashes.len(), peer_ids.len());

        let mut attempts = futures_util::stream::FuturesUnordered::new();
        let mut pending_ranges = ranges.iter().cloned().enumerate();
        let mut retry_counts = HashMap::<usize, usize>::new();
        let mut bad_peers = HashSet::<PeerId>::new();
        let redundant_prefix_chunks =
            decoupled_initial_prefix_redundancy_count(&ranges, hashes.len(), peer_ids.len());
        for _ in 0..max_in_flight {
            let Some((chunk_index, range)) = pending_ranges.next() else {
                break;
            };
            schedule_decoupled_receipt_chunk(
                self,
                &mut attempts,
                peer_ids,
                &hashes,
                rotated_chunk_index(chunk_index, peer_rotation),
                range,
            );
        }
        for (duplicate_index, range) in ranges
            .iter()
            .take(redundant_prefix_chunks)
            .cloned()
            .enumerate()
        {
            let base_chunk_index = range_indices_by_start
                .get(&range.start)
                .copied()
                .unwrap_or_default();
            schedule_decoupled_receipt_chunk(
                self,
                &mut attempts,
                peer_ids,
                &hashes,
                rotated_chunk_index(
                    base_chunk_index
                        + ((PARALLEL_CHUNK_RETRY_ROUNDS + 1 + duplicate_index) * range_count),
                    peer_rotation,
                ),
                range,
            );
        }

        let mut chunks = BTreeMap::new();
        let mut stats = Vec::new();
        let mut failures = Vec::new();
        while let Some((chunk_index, range, peer_id, requested, elapsed, result)) =
            attempts.next().await
        {
            let request_failed = result.is_err();
            let chunk_already_completed = chunks.contains_key(&range.start);
            match result {
                Ok(receipts) => {
                    stats.push((peer_id, receipts.len(), elapsed));
                    emit_body_receipt_role_success(
                        &self.accounting_tx,
                        peer_id,
                        PeerRequestKind::Receipts,
                        receipts.len(),
                        elapsed,
                    );
                    if !chunk_already_completed {
                        chunks.insert(range.start, (peer_id, receipts));
                    }
                }
                Err(kind) => {
                    if chunk_already_completed {
                        trace!(
                            peer = %peer_id,
                            requested,
                            ?kind,
                            "late duplicate receipt chunk request failed after chunk completion"
                        );
                        continue;
                    }
                    let failure = ChunkRequestFailure {
                        role: ChunkRequestRole::Receipts,
                        peer_id,
                        requested,
                        kind: kind.clone(),
                    };
                    emit_body_receipt_role_failure(&self.accounting_tx, failure.clone());
                    if chunk_failure_disables_role_peer(&failure) {
                        bad_peers.insert(peer_id);
                    }
                    failures.push(failure);
                    trace!(
                        peer = %peer_id,
                        requested,
                        ?kind,
                        "decoupled receipt chunk request failed"
                    );
                }
            }

            if missing_chunk_ranges(&ranges, &chunks).is_empty() {
                break;
            }
            if decoupled_dense_can_stop_early(
                contiguous_sourced_chunk_items(&chunks),
                accepted_prefix,
                hashes.len(),
            ) {
                break;
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
            let chunk_peer_ids = peer_ids_excluding(peer_ids, &bad_peers);
            if chunk_peer_ids.is_empty() {
                continue;
            }
            schedule_decoupled_receipt_chunk(
                self,
                &mut attempts,
                &chunk_peer_ids,
                &hashes,
                rotated_chunk_index(chunk_index, peer_rotation),
                range,
            );
        }

        let missing_prefix_ranges = missing_prefix_chunk_ranges(&ranges, &chunks, accepted_prefix);
        if !missing_prefix_ranges.is_empty() {
            bad_peers.extend(disabled_chunk_peers(&failures, ChunkRequestRole::Receipts));
            for range in missing_prefix_ranges {
                let base_chunk_index = range_indices_by_start
                    .get(&range.start)
                    .copied()
                    .unwrap_or_default();
                let mut retry_peer_ids = peer_ids_excluding(peer_ids, &bad_peers);
                rotate_request_candidates(
                    &mut retry_peer_ids,
                    rotated_chunk_index(
                        base_chunk_index + ((PARALLEL_CHUNK_RETRY_ROUNDS + 1) * range_count),
                        peer_rotation,
                    ),
                );
                for peer_id in retry_peer_ids
                    .into_iter()
                    .take(PARALLEL_CHUNK_SALVAGE_PEER_LIMIT)
                {
                    let request_hashes = hashes[range.clone()].to_vec();
                    let started_at = Instant::now();
                    let requested = request_hashes.len();
                    let _active = BodyReceiptActiveRequestGuard::new(
                        &self.accounting_tx,
                        peer_id,
                        PeerRequestKind::Receipts,
                    );
                    match self
                        .request_receipts_until_complete(peer_id, request_hashes, None)
                        .await
                    {
                        Ok(receipts) => {
                            let elapsed = started_at.elapsed();
                            stats.push((peer_id, receipts.len(), elapsed));
                            emit_body_receipt_role_success(
                                &self.accounting_tx,
                                peer_id,
                                PeerRequestKind::Receipts,
                                receipts.len(),
                                elapsed,
                            );
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
                            emit_body_receipt_role_failure(&self.accounting_tx, failure.clone());
                            if chunk_failure_disables_role_peer(&failure) {
                                bad_peers.insert(peer_id);
                            }
                            failures.push(failure);
                            trace!(
                                peer = %peer_id,
                                requested,
                                ?kind,
                                "decoupled receipt chunk salvage request failed"
                            );
                        }
                    }
                }
            }
        }

        let receipts = sourced_receipts_from_chunks(hashes.len(), chunks);
        if receipts.len() >= accepted_prefix {
            Ok(Some((receipts, stats, failures)))
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

        match timeout(PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT, response_rx).await {
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
        request_timeout: Option<Duration>,
        max_attempts: Option<usize>,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        self.drain_events_now();

        let mut peer_ids = self.peer_ids_for_block_requests(required_block, &[]).await;
        self.sort_peer_ids_by_request_performance(&mut peer_ids, PeerRequestKind::Headers);
        let mut dead_peers = HashSet::new();
        let mut saw_empty_response = false;

        for (attempt_index, peer_id) in peer_ids.into_iter().enumerate() {
            if max_attempts.is_some_and(|limit| attempt_index >= limit.max(1)) {
                break;
            }
            let started_at = Instant::now();
            match self
                .request_headers_with_timeout(peer_id, request.clone(), request_timeout)
                .await
            {
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
        self.get_receipts_inner(hashes, required_block, None, &[], None, None)
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
            None,
            None,
        )
        .await
    }

    pub async fn get_receipts_matching_counts_prefer_peers_with_limits(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: &[usize],
        preferred_peers: &[PeerId],
        request_timeout: Duration,
        max_attempts: usize,
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
            Some(request_timeout),
            Some(max_attempts),
        )
        .await
    }

    async fn get_receipts_inner(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: Option<&[usize]>,
        preferred_peers: &[PeerId],
        request_timeout: Option<Duration>,
        max_attempts: Option<usize>,
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

        for (attempt_index, peer_id) in peer_ids.into_iter().enumerate() {
            if max_attempts.is_some_and(|limit| attempt_index >= limit.max(1)) {
                break;
            }
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
                match self
                    .request_receipts70(peer_id, hashes.clone(), request_timeout)
                    .await
                {
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
                    self.request_receipts69(peer_id, request_hashes.clone(), request_timeout)
                        .await
                } else {
                    self.request_receipts_with_timeout(
                        peer_id,
                        request_hashes.clone(),
                        request_timeout,
                    )
                    .await
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
        rows_per_block: Option<f64>,
    ) -> Vec<std::ops::Range<usize>> {
        if body_peer_ids.is_empty() || receipt_peer_ids.is_empty() {
            return Vec::new();
        }

        let chunk_cap = body_receipt_chunk_cap(
            body_peer_ids.len().min(receipt_peer_ids.len()),
            rows_per_block,
        );
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
            let peer_id =
                peer_ids[rotated_chunk_index(chunk_index, self.request_cursor) % peer_ids.len()];
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
            let peer_id =
                peer_ids[rotated_chunk_index(chunk_index, self.request_cursor) % peer_ids.len()];
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
            let peer_id =
                peer_ids[rotated_chunk_index(chunk_index, self.request_cursor) % peer_ids.len()];
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
        match self
            .request_sourced_receipts_parallel_chunks(peer_ids, hashes, expected_receipt_counts)
            .await
        {
            Ok(Some((sourced_receipts, stats, failures))) => {
                let Some(first_peer) = sourced_receipts.first().map(|(peer_id, _)| *peer_id) else {
                    return Ok(None);
                };
                let receipts = sourced_receipts
                    .into_iter()
                    .map(|(_, receipts)| receipts)
                    .collect();
                Ok(Some((first_peer, receipts, stats, failures)))
            }
            Ok(None) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn request_sourced_receipts_parallel_chunks(
        &self,
        peer_ids: &[PeerId],
        hashes: Vec<B256>,
        expected_receipt_counts: Option<&[usize]>,
    ) -> std::result::Result<Option<ParallelSourcedReceipts>, ParallelChunkError> {
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
            let peer_id =
                peer_ids[rotated_chunk_index(chunk_index, self.request_cursor) % peer_ids.len()];
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
            let peer_id =
                peer_ids[rotated_chunk_index(chunk_index, self.request_cursor) % peer_ids.len()];
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

        let receipts = sourced_receipts_from_chunks(hashes.len(), chunks);

        if receipts.len() == hashes.len() {
            Ok(Some((receipts, stats, failures)))
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
                .request_receipts70(peer_id, hashes.clone(), None)
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
                self.request_receipts69(peer_id, request_hashes.clone(), None)
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
        for failure in coalesce_parallel_chunk_failures(failures) {
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

    pub(super) async fn request_headers_with_timeout(
        &self,
        peer_id: PeerId,
        request: HeadersRequest,
        request_timeout: Option<Duration>,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
        RequestAttempt,
    > {
        self.request_with_channel(
            peer_id,
            &move |response| PeerRequest::GetBlockHeaders {
                request: GetBlockHeaders {
                    start_block: request.start,
                    limit: request.limit,
                    skip: 0,
                    direction: request.direction,
                },
                response,
            },
            request_timeout,
        )
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
        self.request_bodies_with_timeout(peer_id, hashes, None)
            .await
    }

    pub(super) async fn request_bodies_with_timeout(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
        request_timeout: Option<Duration>,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        RequestAttempt,
    > {
        self.request_with_channel(
            peer_id,
            &move |response| PeerRequest::GetBlockBodies {
                request: GetBlockBodies(hashes.clone()),
                response,
            },
            request_timeout,
        )
        .await
    }

    pub(super) async fn request_with_channel<T, W, MakeRequest>(
        &self,
        peer_id: PeerId,
        make_request: &MakeRequest,
        request_timeout: Option<Duration>,
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

        match timeout(request_timeout.unwrap_or(REQUEST_TIMEOUT), response_rx).await {
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
        self.request_receipts_with_timeout(peer_id, hashes, None)
            .await
    }

    pub(super) async fn request_receipts_with_timeout(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
        request_timeout: Option<Duration>,
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
        self.request_with_channel(
            peer_id,
            &move |response| PeerRequest::GetReceipts {
                request: GetReceipts(hashes.clone()),
                response,
            },
            request_timeout,
        )
        .await
    }

    pub(super) async fn request_receipts69(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
        request_timeout: Option<Duration>,
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
            .request_with_channel(
                peer_id,
                &move |response| PeerRequest::GetReceipts69 {
                    request: GetReceipts(hashes.clone()),
                    response,
                },
                request_timeout,
            )
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
        request_timeout: Option<Duration>,
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
                .request_with_channel(
                    peer_id,
                    &move |response| PeerRequest::GetReceipts70 {
                        request: GetReceipts70 {
                            first_block_receipt_index,
                            block_hashes: request_hashes.clone(),
                        },
                        response,
                    },
                    request_timeout,
                )
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

fn coalesce_parallel_chunk_failures(failures: ParallelChunkFailures) -> ParallelChunkFailures {
    let mut request_failures = HashMap::<(PeerId, ChunkRequestRole), ChunkRequestFailure>::new();
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

    request_failures
        .into_values()
        .chain(other_failures)
        .collect()
}

fn request_failure_quarantines_receipt_peer(error: &RequestAttempt) -> bool {
    matches!(
        error,
        RequestAttempt::Disconnected
            | RequestAttempt::Request(reth_network::p2p::error::RequestError::ChannelClosed)
            | RequestAttempt::Request(reth_network::p2p::error::RequestError::ConnectionDropped)
    )
}

fn peer_ids_excluding(peer_ids: &[PeerId], bad_peers: &HashSet<PeerId>) -> Vec<PeerId> {
    if bad_peers.is_empty() {
        return peer_ids.to_vec();
    }

    peer_ids
        .iter()
        .copied()
        .filter(|peer_id| !bad_peers.contains(peer_id))
        .collect()
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

fn missing_prefix_chunk_ranges<T>(
    ranges: &[std::ops::Range<usize>],
    chunks: &BTreeMap<usize, (PeerId, Vec<T>)>,
    min_accepted_prefix: usize,
) -> Vec<std::ops::Range<usize>> {
    missing_chunk_ranges(ranges, chunks)
        .into_iter()
        .filter(|range| range.start < min_accepted_prefix)
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
        futures_util::future::BoxFuture<'a, BodyReceiptChunkAttempt>,
    >,
    in_flight: &mut HashMap<usize, InFlightBodyReceiptChunk>,
    peer_state: &mut BodyReceiptPlanPeerState,
    range: std::ops::Range<usize>,
    chunk_index: usize,
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

    let (body_peer_ids, body_peer) = body_receipt_attempt_peer_ids(
        &plan.body_peer_ids,
        &peer_state.body_bad_peers,
        &peer_state.body_in_flight_peers,
        chunk_index,
    );
    let (receipt_peer_ids, receipt_peer) = body_receipt_attempt_peer_ids(
        &plan.receipt_peer_ids,
        &peer_state.receipt_bad_peers,
        &peer_state.receipt_in_flight_peers,
        chunk_index,
    );
    record_body_receipt_attempt_peer(&mut peer_state.body_in_flight_peers, body_peer);
    record_body_receipt_attempt_peer(&mut peer_state.receipt_in_flight_peers, receipt_peer);

    attempts.push(body_receipt_chunk_request(
        plan,
        range,
        &plan.hashes,
        body_peer_ids,
        receipt_peer_ids,
        body_peer,
        receipt_peer,
    ));
}

fn body_receipt_attempt_peer_ids(
    peer_ids: &[PeerId],
    bad_peers: &HashSet<PeerId>,
    in_flight_peers: &HashMap<PeerId, usize>,
    chunk_index: usize,
) -> (Vec<PeerId>, Option<PeerId>) {
    let mut ordered = peer_ids.to_vec();
    rotate_request_candidates(&mut ordered, chunk_index);

    let mut eligible = ordered
        .iter()
        .copied()
        .filter(|peer_id| !bad_peers.contains(peer_id))
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        eligible = ordered;
    }

    let Some(min_in_flight) = eligible
        .iter()
        .map(|peer_id| in_flight_peers.get(peer_id).copied().unwrap_or_default())
        .min()
    else {
        return (Vec::new(), None);
    };

    if let Some(index) = eligible.iter().position(|peer_id| {
        in_flight_peers.get(peer_id).copied().unwrap_or_default() == min_in_flight
    }) {
        eligible.rotate_left(index);
    }

    let primary_peer = eligible.first().copied();
    (eligible, primary_peer)
}

fn record_body_receipt_attempt_peer(
    in_flight_peers: &mut HashMap<PeerId, usize>,
    peer_id: Option<PeerId>,
) {
    let Some(peer_id) = peer_id else {
        return;
    };
    *in_flight_peers.entry(peer_id).or_default() += 1;
}

fn release_body_receipt_attempt_peer(
    in_flight_peers: &mut HashMap<PeerId, usize>,
    peer_id: Option<PeerId>,
) {
    let Some(peer_id) = peer_id else {
        return;
    };
    match in_flight_peers.entry(peer_id) {
        std::collections::hash_map::Entry::Occupied(mut entry) if *entry.get() > 1 => {
            *entry.get_mut() -= 1;
        }
        std::collections::hash_map::Entry::Occupied(entry) => {
            entry.remove();
        }
        std::collections::hash_map::Entry::Vacant(_) => {}
    }
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

async fn request_header_page_from_candidates(
    page_index: usize,
    request: HeadersRequest,
    candidates: Vec<(
        PeerId,
        PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    )>,
) -> HeaderPageResult {
    let requested = request.limit;
    let mut failures = Vec::new();
    for (peer_id, sender) in candidates {
        let started_at = Instant::now();
        match request_headers_with_sender(sender, request.clone()).await {
            Ok(headers) => {
                return HeaderPageResult {
                    page_index,
                    requested,
                    success: Some((peer_id, headers, started_at.elapsed())),
                    failures,
                };
            }
            Err(error) => failures.push((peer_id, error)),
        }
    }

    HeaderPageResult {
        page_index,
        requested,
        success: None,
        failures,
    }
}

async fn request_headers_with_sender(
    sender: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    request: HeadersRequest,
) -> std::result::Result<
    Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    RequestAttempt,
> {
    let (response_tx, response_rx) = oneshot::channel();
    sender
        .to_session_tx
        .send(PeerRequest::GetBlockHeaders {
            request: GetBlockHeaders {
                start_block: request.start,
                limit: request.limit,
                skip: 0,
                direction: request.direction,
            },
            response: response_tx,
        })
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
    hashes: &[B256],
    body_peer_ids: Vec<PeerId>,
    receipt_peer_ids: Vec<PeerId>,
    body_peer: Option<PeerId>,
    receipt_peer: Option<PeerId>,
) -> futures_util::future::BoxFuture<'a, BodyReceiptChunkAttempt> {
    let chunk_hashes = hashes[range.clone()].to_vec();

    async move {
        let chunk = plan
            .request_body_receipt_chunk(range.start, chunk_hashes, body_peer_ids, receipt_peer_ids)
            .await;
        BodyReceiptChunkAttempt {
            chunk,
            body_peer,
            receipt_peer,
        }
    }
    .boxed()
}

fn schedule_decoupled_body_chunk<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, DecoupledBodyChunkResult>,
    >,
    peer_ids: &[PeerId],
    hashes: &[B256],
    chunk_index: usize,
    range: std::ops::Range<usize>,
) {
    let peer_id = peer_ids[chunk_index % peer_ids.len()];
    let request_hashes = hashes[range.clone()].to_vec();
    attempts.push(
        async move {
            let started_at = Instant::now();
            let requested = request_hashes.len();
            let _active = BodyReceiptActiveRequestGuard::new(
                &plan.accounting_tx,
                peer_id,
                PeerRequestKind::Bodies,
            );
            let result = plan
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
        .boxed(),
    );
}

fn rotated_chunk_index(chunk_index: usize, peer_rotation: usize) -> usize {
    chunk_index.wrapping_add(peer_rotation)
}

fn schedule_decoupled_receipt_chunk<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, DecoupledReceiptChunkResult>,
    >,
    peer_ids: &[PeerId],
    hashes: &[B256],
    chunk_index: usize,
    range: std::ops::Range<usize>,
) {
    let peer_id = peer_ids[chunk_index % peer_ids.len()];
    let request_hashes = hashes[range.clone()].to_vec();
    attempts.push(
        async move {
            let started_at = Instant::now();
            let requested = request_hashes.len();
            let _active = BodyReceiptActiveRequestGuard::new(
                &plan.accounting_tx,
                peer_id,
                PeerRequestKind::Receipts,
            );
            let result = plan
                .request_receipts_until_complete(peer_id, request_hashes, None)
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
        .boxed(),
    );
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

fn body_receipt_blocks_if_sourced_counts_match(
    bodies: &[SourcedBlockBody],
    receipts: Vec<SourcedReceiptSet>,
    requested: usize,
) -> std::result::Result<Vec<SourcedBodyReceipts>, Box<(PeerId, ReceiptCountMismatch)>> {
    if receipts.len() != requested {
        let peer_id = receipts
            .last()
            .map(|(peer_id, _)| *peer_id)
            .unwrap_or(PeerId::ZERO);
        return Err(Box::new((
            peer_id,
            ReceiptCountMismatch {
                response_kind: "receipts",
                requested_blocks: requested,
                returned_blocks: receipts.len(),
                block_index: None,
                expected_receipts: None,
                returned_receipts: None,
            },
        )));
    }

    for (block_index, ((receipt_peer, block_receipts), (_, body))) in
        receipts.iter().zip(bodies.iter()).enumerate()
    {
        let expected_receipts = body.transaction_count();
        if block_receipts.len() != expected_receipts {
            return Err(Box::new((
                *receipt_peer,
                ReceiptCountMismatch {
                    response_kind: "receipts",
                    requested_blocks: requested,
                    returned_blocks: receipts.len(),
                    block_index: Some(block_index),
                    expected_receipts: Some(expected_receipts),
                    returned_receipts: Some(block_receipts.len()),
                },
            )));
        }
    }

    Ok(bodies.iter().cloned().zip(receipts).collect())
}

fn sourced_bodies_from_chunks(
    total_hashes: usize,
    chunks: BTreeMap<
        usize,
        (
            PeerId,
            Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        ),
    >,
) -> Vec<SourcedBlockBody> {
    let mut bodies = Vec::with_capacity(total_hashes);
    let mut expected_start = 0usize;
    for (start, (peer_id, chunk_bodies)) in chunks {
        if start != expected_start || bodies.len() >= total_hashes {
            break;
        }
        expected_start = expected_start.saturating_add(chunk_bodies.len());
        bodies.extend(chunk_bodies.into_iter().map(|body| (peer_id, body)));
    }
    bodies.truncate(total_hashes);
    bodies
}

fn sourced_receipts_from_chunks(
    total_hashes: usize,
    chunks: BTreeMap<usize, (PeerId, ReceiptBatch)>,
) -> Vec<SourcedReceiptSet> {
    let mut receipts = Vec::with_capacity(total_hashes);
    let mut expected_start = 0usize;
    for (start, (peer_id, chunk_receipts)) in chunks {
        if start != expected_start || receipts.len() >= total_hashes {
            break;
        }
        expected_start = expected_start.saturating_add(chunk_receipts.len());
        receipts.extend(
            chunk_receipts
                .into_iter()
                .map(|receipts| (peer_id, receipts)),
        );
    }
    receipts.truncate(total_hashes);
    receipts
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
        .saturating_mul(parallel_requests_per_peer(peer_count))
        .clamp(1, max_in_flight)
}

fn parallel_requests_per_peer(peer_count: usize) -> usize {
    if peer_count >= PARALLEL_HIGH_FANOUT_MIN_PEERS {
        PARALLEL_REQUESTS_PER_PEER_HIGH
    } else {
        PARALLEL_REQUESTS_PER_PEER_LOW
    }
}

fn limit_body_receipt_candidate_pool(peer_ids: &mut Vec<PeerId>) {
    if peer_ids.len() >= PIPELINED_BODY_RECEIPT_FAST_POOL_MIN_PEERS {
        peer_ids.truncate(PIPELINED_BODY_RECEIPT_FAST_POOL_SIZE);
    }
}

fn retain_idle_body_receipt_candidate_pool_if_enough(
    peers: &HashMap<PeerId, ActivePeer>,
    peer_ids: &mut Vec<PeerId>,
    kind: PeerRequestKind,
) {
    retain_preferred_items_if_enough(
        peer_ids,
        PIPELINED_BODY_RECEIPT_IDLE_POOL_MIN_PEERS,
        |peer_id| {
            peers
                .get(peer_id)
                .is_some_and(|peer| body_receipt_active_requests(peer, kind) == 0)
        },
    );
}

fn retain_serving_body_receipt_candidate_pool_if_enough(
    peers: &HashMap<PeerId, ActivePeer>,
    peer_ids: &mut Vec<PeerId>,
) {
    retain_preferred_items_with_limited_fallbacks_if_enough(
        peer_ids,
        PIPELINED_BODY_RECEIPT_SERVING_POOL_MIN_PEERS,
        PIPELINED_BODY_RECEIPT_SERVING_POOL_PROBE_PEERS,
        |peer_id| peers.get(peer_id).is_some_and(|peer| peer.is_serving),
    );
}

fn retain_preferred_items_if_enough<T>(
    items: &mut Vec<T>,
    min_preferred: usize,
    is_preferred: impl Fn(&T) -> bool,
) {
    let preferred = items.iter().filter(|item| is_preferred(*item)).count();
    if preferred >= min_preferred {
        items.retain(is_preferred);
    }
}

fn retain_preferred_items_with_limited_fallbacks_if_enough<T>(
    items: &mut Vec<T>,
    min_preferred: usize,
    fallback_limit: usize,
    is_preferred: impl Fn(&T) -> bool,
) {
    let preferred = items.iter().filter(|item| is_preferred(*item)).count();
    if preferred < min_preferred {
        return;
    }

    let mut fallback_count = 0usize;
    items.retain(|item| {
        if is_preferred(item) {
            return true;
        }
        if fallback_count < fallback_limit {
            fallback_count += 1;
            return true;
        }
        false
    });
}

fn body_receipt_active_requests(peer: &ActivePeer, kind: PeerRequestKind) -> usize {
    match kind {
        PeerRequestKind::Headers => 0,
        PeerRequestKind::Bodies => peer.body_active_requests,
        PeerRequestKind::Receipts => peer.receipt_active_requests,
    }
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

fn body_receipt_chunk_cap(peer_pair_count: usize, rows_per_block: Option<f64>) -> usize {
    if peer_pair_count >= PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS {
        if rows_per_block
            .is_some_and(|rows| rows >= PIPELINED_BODY_RECEIPT_VERY_DENSE_RETURN_ROWS_PER_BLOCK)
        {
            return PIPELINED_BODY_RECEIPT_VERY_DENSE_CHUNK_BLOCKS;
        }
        if rows_per_block
            .is_some_and(|rows| rows >= PIPELINED_BODY_RECEIPT_DENSE_RETURN_ROWS_PER_BLOCK)
        {
            return PIPELINED_BODY_RECEIPT_DENSE_CHUNK_BLOCKS;
        }
    }

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

fn body_receipt_return_blocks(
    total_blocks: usize,
    gas_used: Option<&[u64]>,
    rows_per_block: Option<f64>,
) -> usize {
    if total_blocks <= PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS {
        return total_blocks;
    }

    let Some(gas_used) = gas_used else {
        return PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS;
    };

    let return_limit = if rows_per_block.is_some_and(|rows_per_block| {
        rows_per_block >= PIPELINED_BODY_RECEIPT_DENSE_RETURN_ROWS_PER_BLOCK
    }) {
        total_blocks.min(PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS)
    } else {
        total_blocks.min(PIPELINED_BODY_RECEIPT_MAX_CONTIGUOUS_RETURN_BLOCKS)
    };
    let return_gas_target =
        PIPELINED_BODY_RECEIPT_RETURN_GAS_PER_BLOCK_TARGET.saturating_mul(return_limit as u128);
    let mut cumulative_gas = 0u128;
    for (index, gas) in gas_used.iter().take(return_limit).enumerate() {
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
    let prefix = if return_blocks <= PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS {
        PIPELINED_BODY_RECEIPT_DENSE_MIN_ACCEPTED_PREFIX_BLOCKS
    } else {
        PIPELINED_BODY_RECEIPT_MIN_ACCEPTED_PREFIX_BLOCKS
    };
    return_blocks.min(prefix)
}

fn body_receipt_plan_progress_target(return_blocks: usize) -> usize {
    return_blocks.min(PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS)
}

fn planned_body_receipt_prefix_blocks(
    ranges: &[std::ops::Range<usize>],
    return_blocks: usize,
    total_hashes: usize,
) -> usize {
    let min_return_blocks = body_receipt_plan_progress_target(return_blocks);
    ranges
        .iter()
        .find(|range| range.end >= min_return_blocks)
        .map(|range| range.end)
        .unwrap_or(min_return_blocks)
        .min(return_blocks)
        .min(total_hashes)
}

fn body_receipt_completion_return_blocks(
    return_blocks: usize,
    planned_return_blocks: usize,
    total_hashes: usize,
) -> usize {
    planned_return_blocks.min(return_blocks).min(total_hashes)
}

fn body_receipt_min_accepted_prefix_override(return_blocks: usize, prefix: usize) -> usize {
    return_blocks.min(prefix)
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

fn body_receipt_initial_prefix_redundancy_count(
    ranges: &[std::ops::Range<usize>],
    min_return_blocks: usize,
    scheduled_chunks: usize,
    max_in_flight: usize,
    peer_count: usize,
) -> usize {
    if ranges.is_empty()
        || min_return_blocks == 0
        || min_return_blocks > PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS
        || peer_count < PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS
    {
        return 0;
    }

    let spare_attempts = max_in_flight.saturating_sub(scheduled_chunks);
    if spare_attempts == 0 {
        return 0;
    }

    let prefix_chunks = body_receipt_scheduled_chunk_limit(ranges, min_return_blocks);
    spare_attempts
        .min(prefix_chunks)
        .min(PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANT_CHUNKS)
}

fn body_receipt_prefix_hedge_spare_attempts(min_return_blocks: usize, peer_count: usize) -> usize {
    if min_return_blocks == 0
        || min_return_blocks > PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS
        || peer_count < PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS
    {
        return 0;
    }

    PIPELINED_BODY_RECEIPT_PREFIX_HEDGE_SPARE_ATTEMPTS
}

fn decoupled_initial_prefix_redundancy_count(
    ranges: &[std::ops::Range<usize>],
    min_return_blocks: usize,
    peer_count: usize,
) -> usize {
    body_receipt_scheduled_chunk_limit(ranges, min_return_blocks)
        .min(body_receipt_prefix_hedge_spare_attempts(
            min_return_blocks,
            peer_count,
        ))
        .min(PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANT_CHUNKS)
}

fn decoupled_dense_accepted_prefix(return_blocks: usize, peer_count: usize) -> usize {
    if peer_count >= PIPELINED_BODY_RECEIPT_FULL_PREFIX_MIN_PEERS {
        return return_blocks;
    }

    let target = (return_blocks / 2).max(body_receipt_min_accepted_prefix(return_blocks));
    return_blocks.min(target)
}

fn decoupled_dense_can_stop_early(
    completed_prefix: usize,
    accepted_prefix: usize,
    return_blocks: usize,
) -> bool {
    accepted_prefix >= return_blocks && completed_prefix >= accepted_prefix
}

fn contiguous_sourced_chunk_items<T>(chunks: &BTreeMap<usize, (PeerId, Vec<T>)>) -> usize {
    let mut expected_start = 0usize;
    for (start, (_, items)) in chunks {
        if *start != expected_start {
            break;
        }
        expected_start = expected_start.saturating_add(items.len());
    }
    expected_start
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
    fn body_receipt_attempt_peers_prefer_least_loaded_non_bad_peer() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let fourth = PeerId::repeat_byte(0x44);
        let peers = vec![first, second, third, fourth];
        let bad_peers = HashSet::from([third]);
        let in_flight = HashMap::from([(first, 2), (second, 1), (fourth, 3)]);

        let (ordered, primary) = body_receipt_attempt_peer_ids(&peers, &bad_peers, &in_flight, 0);

        assert_eq!(primary, Some(second));
        assert_eq!(ordered, vec![second, fourth, first]);
    }

    #[test]
    fn body_receipt_attempt_peers_preserve_rotation_among_equal_loads() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let peers = vec![first, second, third];

        let (ordered, primary) =
            body_receipt_attempt_peer_ids(&peers, &HashSet::new(), &HashMap::new(), 1);

        assert_eq!(primary, Some(second));
        assert_eq!(ordered, vec![second, third, first]);
    }

    #[test]
    fn body_receipt_attempt_peer_load_accounting_releases_to_zero() {
        let peer = PeerId::repeat_byte(0x11);
        let mut in_flight = HashMap::new();

        record_body_receipt_attempt_peer(&mut in_flight, Some(peer));
        record_body_receipt_attempt_peer(&mut in_flight, Some(peer));
        release_body_receipt_attempt_peer(&mut in_flight, Some(peer));

        assert_eq!(in_flight.get(&peer), Some(&1));

        release_body_receipt_attempt_peer(&mut in_flight, Some(peer));

        assert!(!in_flight.contains_key(&peer));
    }

    #[test]
    fn sourced_receipts_from_chunks_preserves_chunk_peer_attribution() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let chunks: BTreeMap<usize, (PeerId, ReceiptBatch)> = BTreeMap::from([
            (0, (first, vec![Vec::new(), Vec::new()])),
            (2, (second, vec![Vec::new()])),
        ]);

        let receipts = sourced_receipts_from_chunks(3, chunks);

        assert_eq!(
            receipts
                .iter()
                .map(|(peer_id, _)| *peer_id)
                .collect::<Vec<_>>(),
            vec![first, first, second]
        );
    }

    #[test]
    fn sourced_receipts_from_chunks_stops_at_first_gap() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let chunks: BTreeMap<usize, (PeerId, ReceiptBatch)> = BTreeMap::from([
            (0, (first, vec![Vec::new()])),
            (2, (second, vec![Vec::new()])),
        ]);

        let receipts = sourced_receipts_from_chunks(3, chunks);

        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].0, first);
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
        assert_eq!(request_window_limit(1, 16), 1);
        assert_eq!(request_window_limit(2, 16), 2);
        assert_eq!(request_window_limit(8, 16), 8);
        assert_eq!(
            request_window_limit(PARALLEL_HIGH_FANOUT_MIN_PEERS - 1, 128),
            15
        );
        assert_eq!(
            request_window_limit(PARALLEL_HIGH_FANOUT_MIN_PEERS, 128),
            32
        );
        assert_eq!(request_window_limit(32, 128), 64);
    }

    #[test]
    fn paired_body_receipt_window_counts_both_request_types() {
        assert_eq!(paired_body_receipt_chunk_window_limit(0, 16), 0);
        assert_eq!(paired_body_receipt_chunk_window_limit(1, 16), 1);
        assert_eq!(paired_body_receipt_chunk_window_limit(2, 16), 1);
        assert_eq!(paired_body_receipt_chunk_window_limit(8, 16), 4);
        assert_eq!(paired_body_receipt_chunk_window_limit(64, 128), 64);
    }

    #[test]
    fn body_receipt_candidate_pool_only_trims_large_peer_sets() {
        let mut peers = (0..PIPELINED_BODY_RECEIPT_FAST_POOL_MIN_PEERS - 1)
            .map(|index| PeerId::repeat_byte(index as u8))
            .collect::<Vec<_>>();
        limit_body_receipt_candidate_pool(&mut peers);
        assert_eq!(peers.len(), PIPELINED_BODY_RECEIPT_FAST_POOL_MIN_PEERS - 1);

        peers.push(PeerId::repeat_byte(0xff));
        limit_body_receipt_candidate_pool(&mut peers);
        assert_eq!(peers.len(), PIPELINED_BODY_RECEIPT_FAST_POOL_SIZE);
    }

    #[test]
    fn preferred_item_retention_keeps_fallbacks_until_threshold() {
        let mut items = vec![1, 2, 3, 4, 5];

        retain_preferred_items_if_enough(&mut items, 4, |item| item % 2 == 1);

        assert_eq!(items, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn preferred_item_retention_filters_once_threshold_is_met() {
        let mut items = vec![1, 2, 3, 4, 5];

        retain_preferred_items_if_enough(&mut items, 3, |item| item % 2 == 1);

        assert_eq!(items, vec![1, 3, 5]);
    }

    #[test]
    fn preferred_item_retention_keeps_bounded_probe_fallbacks() {
        let mut items = vec![1, 2, 3, 4, 5, 6, 7];

        retain_preferred_items_with_limited_fallbacks_if_enough(&mut items, 3, 2, |item| {
            item % 2 == 1
        });

        assert_eq!(items, vec![1, 2, 3, 4, 5, 7]);
    }

    #[test]
    fn body_receipt_chunk_limit_caps_large_adaptive_limits() {
        assert_eq!(body_receipt_chunk_cap(31, None), 128);
        assert_eq!(body_receipt_chunk_cap(32, Some(50.0)), 128);
        assert_eq!(body_receipt_chunk_cap(15, Some(250.0)), 128);
        assert_eq!(body_receipt_chunk_cap(32, Some(100.0)), 48);
        assert_eq!(body_receipt_chunk_cap(32, Some(250.0)), 48);
        assert_eq!(body_receipt_chunk_cap(32, Some(1500.0)), 32);
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

        assert_eq!(
            body_receipt_return_blocks(128, Some(&dense), Some(300.0)),
            128
        );
        assert_eq!(
            body_receipt_return_blocks(4096, Some(&dense), Some(300.0)),
            512
        );
        let medium_gas = vec![15_000_000; 4096];
        assert_eq!(
            body_receipt_return_blocks(4096, Some(&medium_gas), Some(300.0)),
            512
        );
        assert_eq!(
            body_receipt_return_blocks(4096, Some(&dense), Some(80.0)),
            4096
        );
        assert_eq!(
            body_receipt_return_blocks(6000, Some(&sparse), Some(80.0)),
            6000
        );
        assert_eq!(
            body_receipt_return_blocks(12_000, Some(&sparse), Some(80.0)),
            10_000
        );
        assert_eq!(body_receipt_return_blocks(4096, None, None), 512);
    }

    #[test]
    fn body_receipt_min_accepted_prefix_accepts_dense_half_chunk_progress() {
        assert_eq!(body_receipt_min_accepted_prefix(32), 32);
        assert_eq!(body_receipt_min_accepted_prefix(64), 64);
        assert_eq!(body_receipt_min_accepted_prefix(128), 64);
        assert_eq!(body_receipt_min_accepted_prefix(1024), 128);
        assert_eq!(body_receipt_min_accepted_prefix(10_000), 128);
    }

    #[test]
    fn body_receipt_plan_progress_target_bounds_large_windows() {
        assert_eq!(body_receipt_plan_progress_target(32), 32);
        assert_eq!(
            body_receipt_plan_progress_target(PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS),
            PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS
        );
        assert_eq!(
            body_receipt_plan_progress_target(10_000),
            PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS
        );
    }

    #[test]
    fn planned_body_receipt_prefix_blocks_uses_chunk_boundary() {
        let ranges = vec![0..300, 300..690, 690..1_100, 1_100..1_500];

        assert_eq!(
            planned_body_receipt_prefix_blocks(&ranges, 1_500, 1_500),
            690
        );
        assert_eq!(planned_body_receipt_prefix_blocks(&ranges, 512, 1024), 512);
        assert_eq!(planned_body_receipt_prefix_blocks(&ranges, 1024, 640), 640);
    }

    #[test]
    fn body_receipt_completion_return_blocks_caps_to_planned_prefix() {
        assert_eq!(
            body_receipt_completion_return_blocks(5_000, 1_024, 5_000),
            1_024
        );
        assert_eq!(body_receipt_completion_return_blocks(512, 1_024, 512), 512);
        assert_eq!(
            body_receipt_completion_return_blocks(1_024, 1_024, 640),
            640
        );
    }

    #[test]
    fn body_receipt_residual_prefix_accepts_small_verified_progress() {
        assert_eq!(
            body_receipt_min_accepted_prefix_override(
                816,
                PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS
            ),
            16
        );
        assert_eq!(
            body_receipt_min_accepted_prefix_override(
                8,
                PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS
            ),
            8
        );
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
    fn body_receipt_initial_prefix_redundancy_uses_only_spare_dense_capacity() {
        let ranges = vec![0..32, 32..64, 64..96, 96..128, 128..160, 160..192];

        assert_eq!(
            body_receipt_initial_prefix_redundancy_count(&ranges, 128, 4, 8, 16),
            4
        );
        assert_eq!(
            body_receipt_initial_prefix_redundancy_count(&ranges, 128, 4, 6, 16),
            2
        );
        assert_eq!(
            body_receipt_initial_prefix_redundancy_count(&ranges, 128, 4, 4, 16),
            0
        );
    }

    #[test]
    fn body_receipt_initial_prefix_redundancy_skips_sparse_or_underpeered_plans() {
        let ranges = vec![0..128, 128..256, 256..384, 384..512, 512..640];

        assert_eq!(
            body_receipt_initial_prefix_redundancy_count(&ranges, 2048, 5, 10, 32),
            0
        );
        assert_eq!(
            body_receipt_initial_prefix_redundancy_count(
                &ranges,
                512,
                4,
                8,
                PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS - 1
            ),
            0
        );
    }

    #[test]
    fn body_receipt_prefix_hedges_get_spare_capacity_for_dense_peer_sets() {
        assert_eq!(
            body_receipt_prefix_hedge_spare_attempts(
                512,
                PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS
            ),
            4
        );
        assert_eq!(body_receipt_prefix_hedge_spare_attempts(513, 32), 0);
        assert_eq!(
            body_receipt_prefix_hedge_spare_attempts(
                1024,
                PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS - 1
            ),
            0
        );
        assert_eq!(body_receipt_prefix_hedge_spare_attempts(0, 32), 0);
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
        for retry in 1..PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK {
            assert_eq!(
                body_receipt_hedge_candidate(
                    &mut in_flight,
                    &chunks,
                    1024,
                    start + (PIPELINED_BODY_RECEIPT_HEDGE_DELAY * retry as u32)
                ),
                Some((0..32, 0))
            );
        }
        assert!(
            body_receipt_hedge_candidate(
                &mut in_flight,
                &chunks,
                1024,
                start
                    + (PIPELINED_BODY_RECEIPT_HEDGE_DELAY
                        * PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK as u32)
            )
            .is_none()
        );
    }

    #[test]
    fn decoupled_prefix_redundancy_requires_large_peer_pool() {
        let ranges = vec![0..128, 128..256, 256..384, 384..512, 512..640];

        assert_eq!(
            decoupled_initial_prefix_redundancy_count(
                &ranges,
                PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS,
                PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS - 1,
            ),
            0
        );
        assert_eq!(
            decoupled_initial_prefix_redundancy_count(
                &ranges,
                PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS,
                PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS,
            ),
            PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANT_CHUNKS
        );
    }

    #[test]
    fn decoupled_dense_prefix_targets_full_windows_with_enough_peers() {
        assert_eq!(
            decoupled_dense_accepted_prefix(1024, PIPELINED_BODY_RECEIPT_FULL_PREFIX_MIN_PEERS - 1),
            512
        );
        assert_eq!(
            decoupled_dense_accepted_prefix(1024, PIPELINED_BODY_RECEIPT_FULL_PREFIX_MIN_PEERS),
            1024
        );
        assert_eq!(
            decoupled_dense_accepted_prefix(128, PIPELINED_BODY_RECEIPT_FULL_PREFIX_MIN_PEERS - 1),
            64
        );
        assert_eq!(
            decoupled_dense_accepted_prefix(128, PIPELINED_BODY_RECEIPT_FULL_PREFIX_MIN_PEERS),
            128
        );
        assert_eq!(
            decoupled_dense_accepted_prefix(32, PIPELINED_BODY_RECEIPT_FULL_PREFIX_MIN_PEERS),
            32
        );

        let peer = PeerId::repeat_byte(0x11);
        let mut chunks = BTreeMap::new();
        chunks.insert(0, (peer, vec![1u8; 128]));
        chunks.insert(256, (peer, vec![1u8; 128]));
        assert_eq!(contiguous_sourced_chunk_items(&chunks), 128);

        chunks.insert(128, (peer, vec![1u8; 128]));
        assert_eq!(contiguous_sourced_chunk_items(&chunks), 384);
    }

    #[test]
    fn decoupled_dense_only_stops_early_for_full_prefix_targets() {
        assert!(decoupled_dense_can_stop_early(1024, 1024, 1024));
        assert!(!decoupled_dense_can_stop_early(512, 512, 1024));
        assert!(!decoupled_dense_can_stop_early(511, 512, 1024));
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
    fn body_receipt_request_accounting_is_drained_once() {
        let peer = PeerId::repeat_byte(0x11);
        let mut outcome = BodyReceiptRequestOutcome {
            total_hashes: 4,
            return_blocks: 4,
            planned_return_blocks: 4,
            chunks: BTreeMap::new(),
            failures: vec![ChunkRequestFailure {
                role: ChunkRequestRole::Receipts,
                peer_id: peer,
                requested: 4,
                kind: ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout,
                )),
            }],
            stats: vec![(peer, PeerRequestKind::Bodies, 4, Duration::from_millis(250))],
            accounting_forwarded: false,
        };

        let (stats, failures) = take_body_receipt_request_accounting(&mut outcome);
        assert_eq!(stats.len(), 1);
        assert_eq!(failures.len(), 1);

        let (stats, failures) = take_body_receipt_request_accounting(&mut outcome);
        assert!(stats.is_empty());
        assert!(failures.is_empty());
    }

    #[test]
    fn body_receipt_request_accounting_event_emits_attempt_feedback() {
        let peer = PeerId::repeat_byte(0x11);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let failure = ChunkRequestFailure {
            role: ChunkRequestRole::Bodies,
            peer_id: peer,
            requested: 4,
            kind: ChunkFailureKind::Request(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::Timeout,
            )),
        };

        emit_body_receipt_request_accounting(
            &Some(tx),
            vec![(
                peer,
                PeerRequestKind::Receipts,
                4,
                Duration::from_millis(500),
            )],
            vec![failure],
        );

        let accounting = rx.try_recv().expect("accounting event should be emitted");
        assert_eq!(accounting.stats.len(), 1);
        assert_eq!(accounting.failures.len(), 1);
        assert!(accounting.active_requests.is_empty());
    }

    #[test]
    fn parallel_chunk_failures_coalesce_duplicate_request_penalties() {
        let peer = PeerId::repeat_byte(0x11);
        let other_peer = PeerId::repeat_byte(0x22);

        let failures = coalesce_parallel_chunk_failures(vec![
            ChunkRequestFailure {
                role: ChunkRequestRole::Receipts,
                peer_id: peer,
                requested: 64,
                kind: ChunkFailureKind::Request(RequestAttempt::Disconnected),
            },
            ChunkRequestFailure {
                role: ChunkRequestRole::Receipts,
                peer_id: peer,
                requested: 16,
                kind: ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout,
                )),
            },
            ChunkRequestFailure {
                role: ChunkRequestRole::Bodies,
                peer_id: peer,
                requested: 32,
                kind: ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::BadResponse,
                )),
            },
            ChunkRequestFailure {
                role: ChunkRequestRole::Bodies,
                peer_id: other_peer,
                requested: 32,
                kind: ChunkFailureKind::Incomplete { returned: 0 },
            },
        ]);

        assert_eq!(failures.len(), 3);
        assert!(failures.iter().any(|failure| {
            failure.peer_id == peer
                && failure.role == ChunkRequestRole::Receipts
                && matches!(
                    failure.kind,
                    ChunkFailureKind::Request(RequestAttempt::Request(
                        reth_network::p2p::error::RequestError::Timeout
                    ))
                )
        }));
        assert!(failures.iter().any(|failure| {
            failure.peer_id == peer
                && failure.role == ChunkRequestRole::Bodies
                && matches!(
                    failure.kind,
                    ChunkFailureKind::Request(RequestAttempt::Request(
                        reth_network::p2p::error::RequestError::BadResponse
                    ))
                )
        }));
        assert!(failures.iter().any(|failure| {
            failure.peer_id == other_peer
                && matches!(failure.kind, ChunkFailureKind::Incomplete { returned: 0 })
        }));
    }

    #[test]
    fn body_receipt_active_request_guard_emits_start_and_finish() {
        let peer = PeerId::repeat_byte(0x11);
        let (tx, mut rx) = mpsc::unbounded_channel();

        {
            let _guard =
                BodyReceiptActiveRequestGuard::new(&Some(tx), peer, PeerRequestKind::Receipts);
            let accounting = rx.try_recv().expect("start event should be emitted");
            assert!(accounting.stats.is_empty());
            assert!(accounting.failures.is_empty());
            assert_eq!(accounting.active_requests.len(), 1);
            assert_eq!(accounting.active_requests[0].peer_id, peer);
            assert!(matches!(
                accounting.active_requests[0].kind,
                PeerRequestKind::Receipts
            ));
            assert!(matches!(
                accounting.active_requests[0].delta,
                BodyReceiptActiveRequestDelta::Started
            ));
        }

        let accounting = rx.try_recv().expect("finish event should be emitted");
        assert!(accounting.stats.is_empty());
        assert!(accounting.failures.is_empty());
        assert_eq!(accounting.active_requests.len(), 1);
        assert_eq!(accounting.active_requests[0].peer_id, peer);
        assert!(matches!(
            accounting.active_requests[0].kind,
            PeerRequestKind::Receipts
        ));
        assert!(matches!(
            accounting.active_requests[0].delta,
            BodyReceiptActiveRequestDelta::Finished
        ));
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
    fn missing_prefix_chunk_ranges_only_repairs_required_prefix() {
        let peer = PeerId::repeat_byte(0x11);
        let ranges = vec![0..4, 4..8, 8..12, 12..16];
        let mut chunks = BTreeMap::new();
        chunks.insert(0, (peer, vec![1u8, 2, 3, 4]));
        chunks.insert(8, (peer, vec![9u8, 10, 11, 12]));

        assert_eq!(
            missing_prefix_chunk_ranges(&ranges, &chunks, 10),
            vec![4..8]
        );
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
    fn peer_ids_excluding_returns_only_available_peers() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);

        assert_eq!(
            peer_ids_excluding(&[first, second, third], &HashSet::from([second])),
            vec![first, third]
        );
    }

    #[test]
    fn peer_ids_excluding_returns_empty_when_all_peers_are_bad() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);

        assert!(peer_ids_excluding(&[first, second], &HashSet::from([first, second])).is_empty());
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
