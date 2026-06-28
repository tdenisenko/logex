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

use super::state::execution_client_family;
use super::*;

const PIPELINED_CHUNK_REQUEST_PEERS: usize = 3;
const PIPELINED_BODY_RECEIPT_HEDGE_DELAY: Duration = Duration::from_millis(1_500);
const PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT: Duration = Duration::from_secs(4);
const PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT_MAX: Duration = Duration::from_secs(8);
const PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT_SAFETY_FACTOR: f64 = 1.75;
const PIPELINED_BODY_RECEIPT_PLAN_TIMEOUT: Duration = Duration::from_secs(45);
const PIPELINED_BODY_RECEIPT_MAX_HEDGES: usize = 64;
const PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK: usize = 4;
const PIPELINED_BODY_RECEIPT_PREFIX_REASSIGN_ROUNDS: usize = 4;
const PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_EXTRA_ROLE_ATTEMPTS: usize = 8;
const PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_PEERS: usize = 8;
const PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_PEER_LIMIT: usize = 4;
const PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_CHUNKS: usize = 2;
const PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_TIMEOUT: Duration = Duration::from_secs(12);
const PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_DELAY: Duration = Duration::from_secs(4);
const PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_MIN_BLOCKS: usize = 256;
const PIPELINED_BODY_RECEIPT_BACKGROUND_DRAIN_GRACE: Duration = Duration::from_millis(250);
const PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS: usize = 16;
const PIPELINED_BODY_RECEIPT_PREFIX_EARLY_REDUNDANT_CHUNKS: usize = 2;
const PIPELINED_BODY_RECEIPT_LOW_PEER_PREFIX_REDUNDANCY_MIN_PEERS: usize = 8;
const PIPELINED_BODY_RECEIPT_PREFIX_HEDGE_SPARE_ATTEMPTS: usize = 4;
const PIPELINED_BODY_RECEIPT_LOOKAHEAD_PREFIX_CHUNK_LIMIT: usize = 8;
const PIPELINED_BODY_RECEIPT_FAST_POOL_MIN_PEERS: usize = 32;
const PIPELINED_BODY_RECEIPT_FAST_POOL_SIZE: usize = 16;
const PIPELINED_BODY_RECEIPT_IDLE_POOL_MIN_PEERS: usize = 4;
const PIPELINED_BODY_RECEIPT_IDLE_POOL_PROBE_PEERS: usize = 8;
const PIPELINED_BODY_RECEIPT_SERVING_POOL_MIN_PEERS: usize = 16;
const PIPELINED_BODY_RECEIPT_SERVING_POOL_PROBE_PEERS: usize = 8;
const PIPELINED_BODY_RECEIPT_CHUNK_BLOCKS_DEFAULT: usize = 128;
const PIPELINED_BODY_RECEIPT_DENSE_CHUNK_BLOCKS: usize = 48;
const PIPELINED_BODY_RECEIPT_VERY_DENSE_CHUNK_BLOCKS: usize = 32;
const PIPELINED_BODY_RECEIPT_CHUNK_GAS_TARGET: u64 = 960_000_000;
const PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS: usize = 512;
const PIPELINED_BODY_RECEIPT_DENSE_MIN_ACCEPTED_PREFIX_BLOCKS: usize =
    PIPELINED_BODY_RECEIPT_DENSE_CHUNK_BLOCKS * 2 / 3;
const PIPELINED_BODY_RECEIPT_MIN_ACCEPTED_PREFIX_BLOCKS: usize =
    PIPELINED_BODY_RECEIPT_CHUNK_BLOCKS_DEFAULT;
const PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS: usize = 16;
const PIPELINED_BODY_RECEIPT_MAX_CONTIGUOUS_RETURN_BLOCKS: usize = 10_000;
const PIPELINED_BODY_RECEIPT_DENSE_RETURN_ROWS_PER_BLOCK: f64 = 300.0;
const PIPELINED_BODY_RECEIPT_VERY_DENSE_RETURN_ROWS_PER_BLOCK: f64 = 1_500.0;
const PIPELINED_BODY_RECEIPT_RETURN_GAS_PER_BLOCK_TARGET: u128 = 30_000_000;
const PARALLEL_CHUNK_RETRY_ROUNDS: usize = 2;
const PARALLEL_REQUESTS_PER_PEER_LOW: usize = 1;
const PARALLEL_REQUESTS_PER_PEER_HIGH: usize = 2;
const PARALLEL_HIGH_FANOUT_MIN_PEERS: usize = 16;
const MAX_PARALLEL_BODY_RECEIPT_REQUESTS: usize = 128;
const MAX_PARALLEL_BODY_REQUESTS: usize = 64;
const MIN_PARALLEL_BODY_REQUEST_BLOCKS: usize = 64;
const MAX_PARALLEL_RECEIPT_REQUESTS: usize = 64;
const MIN_PARALLEL_RECEIPT_REQUEST_BLOCKS: usize = 64;
const REVERSE_HEADER_PAGE_PARALLEL_CANDIDATES: usize = 3;

type ReceiptBatch = Vec<
    Vec<alloy_consensus::ReceiptWithBloom<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>>,
>;
type RequestStats = Vec<(PeerId, usize, Duration)>;
type TypedRequestStats = Vec<(PeerId, PeerRequestKind, usize, Duration)>;
type ParallelChunkFailures = Vec<ChunkRequestFailure>;
type ParallelChunkError = (ParallelChunkFailures, RequestStats);
type RawBlockBodies = Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>;
type ParallelBodies = (Vec<SourcedBlockBody>, RequestStats, ParallelChunkFailures);
type ParallelSourcedReceipts = (Vec<SourcedReceiptSet>, RequestStats, ParallelChunkFailures);
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

enum BodyReceiptPlanRoleAttempt {
    Bodies {
        start: usize,
        peer_id: PeerId,
        requested: usize,
        elapsed: Duration,
        result: std::result::Result<RawBlockBodies, ChunkFailureKind>,
    },
    Receipts {
        start: usize,
        peer_id: PeerId,
        requested: usize,
        elapsed: Duration,
        result: std::result::Result<ReceiptBatch, ChunkFailureKind>,
    },
}

struct BodyReceiptChunkLiveCandidates {
    bodies: Vec<PeerId>,
    receipts: Vec<PeerId>,
}

#[derive(Default)]
struct BodyReceiptChunkLiveRoleState {
    next_index: usize,
    used_peers: HashSet<PeerId>,
    in_flight: usize,
    last_scheduled_at: Option<Instant>,
}

#[derive(Default)]
struct BodyReceiptChunkLiveState {
    bodies: BodyReceiptChunkLiveRoleState,
    receipts: BodyReceiptChunkLiveRoleState,
}

struct BodyReceiptChunkLiveStatus {
    has_bodies: bool,
    has_cached_receipts: bool,
    force: bool,
}

#[derive(Default)]
struct BodyReceiptPlanPeerState {
    body_in_flight_peers: HashMap<PeerId, usize>,
    receipt_in_flight_peers: HashMap<PeerId, usize>,
    body_bad_peers: HashSet<PeerId>,
    receipt_bad_peers: HashSet<PeerId>,
}

#[derive(Clone, Copy)]
struct BodyReceiptStaleRoleSchedule {
    min_return_blocks: usize,
    max_role_attempts: usize,
    prefix_critical: bool,
}

#[derive(Clone, Copy)]
struct BodyReceiptLiveLaneSchedule {
    min_return_blocks: usize,
    max_prefix_chunks: usize,
    max_background_chunks: usize,
    max_live_chunks: usize,
    max_role_attempts: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum BodyReceiptRequestPriority {
    #[default]
    Full,
    Lookahead,
}

struct PlanLiveBodyReceiptChunk {
    range: std::ops::Range<usize>,
    candidates: BodyReceiptChunkLiveCandidates,
    state: BodyReceiptChunkLiveState,
    bodies: Option<Vec<SourcedBlockBody>>,
    expected_receipt_counts: Option<Vec<usize>>,
    cached_receipts: Vec<(PeerId, ReceiptBatch)>,
    completed: bool,
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
    body_max_in_flight: usize,
    receipt_max_in_flight: usize,
    peer_rotation: usize,
    priority: BodyReceiptRequestPriority,
    peers: HashMap<PeerId, RequestPeerSnapshot>,
    accounting_tx: Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
}

#[derive(Clone, Default)]
pub(crate) struct BodyReceiptRequestReservations {
    entries: Vec<BodyReceiptRequestReservation>,
}

#[derive(Clone)]
pub(super) struct BodyReceiptRequestReservation {
    pub(super) peer_id: PeerId,
    pub(super) kind: PeerRequestKind,
    pub(super) count: usize,
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
    scheduler: BodyReceiptSchedulerAccounting,
}

#[derive(Debug, Clone, Copy, Default)]
struct BodyReceiptSchedulerAccounting {
    stale_role_retries: u64,
    prefix_reassignments: u64,
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
    pub(crate) residual_chunks: BTreeMap<usize, Vec<SourcedBodyReceipts>>,
}

#[derive(Clone)]
struct RequestPeerSnapshot {
    sender: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    version: EthVersion,
    metrics: RequestPeerRoleMetrics,
}

#[derive(Clone, Copy, Default)]
struct RequestPeerRoleMetrics {
    is_serving: bool,
    consecutive_timeouts: u32,
    body_blocks_per_sec: f64,
    receipt_blocks_per_sec: f64,
    body_active_requests: usize,
    receipt_active_requests: usize,
    body_reserved_requests: usize,
    receipt_reserved_requests: usize,
}

impl RequestPeerRoleMetrics {
    fn from_active_peer(peer: &ActivePeer) -> Self {
        Self {
            is_serving: peer.is_serving,
            consecutive_timeouts: peer.consecutive_timeouts,
            body_blocks_per_sec: peer.body_blocks_per_sec,
            receipt_blocks_per_sec: peer.receipt_blocks_per_sec,
            body_active_requests: peer.body_active_requests,
            receipt_active_requests: peer.receipt_active_requests,
            body_reserved_requests: peer.body_reserved_requests,
            receipt_reserved_requests: peer.receipt_reserved_requests,
        }
    }
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

pub(crate) struct ReverseHeaderPagesRequestPlan {
    pages: Vec<HeaderPageRequestPlan>,
}

struct HeaderPageRequestPlan {
    page_index: usize,
    request: HeadersRequest,
    candidates: Vec<(
        PeerId,
        PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    )>,
}

pub(crate) struct ReverseHeaderPagesRequestOutcome {
    page_results: Vec<HeaderPageResult>,
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

    pub(crate) async fn prepare_reverse_header_pages_request(
        &mut self,
        child_block: u64,
        total_count: u64,
        page_limit: u64,
        required_block: u64,
    ) -> Result<Option<ReverseHeaderPagesRequestPlan>> {
        self.drain_events_now();
        if child_block == 0 || total_count == 0 || page_limit == 0 {
            return Ok(None);
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

        let mut pages = Vec::new();
        for (page_index, request) in
            reverse_header_page_requests(child_block, total_count, page_limit)
        {
            let mut candidates = peers.clone();
            if !candidates.is_empty() {
                let shift = page_index % candidates.len();
                candidates.rotate_left(shift);
            }
            pages.push(HeaderPageRequestPlan {
                page_index,
                request,
                candidates,
            });
        }

        Ok(Some(ReverseHeaderPagesRequestPlan { pages }))
    }

    pub(crate) fn complete_reverse_header_pages_request(
        &mut self,
        mut outcome: ReverseHeaderPagesRequestOutcome,
    ) -> Result<
        Vec<(
            PeerId,
            Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
        )>,
    > {
        outcome.page_results.sort_by_key(|result| result.page_index);
        let mut dead_peers = HashSet::new();
        let mut saw_empty_response = false;
        let mut pages = Vec::new();
        for result in outcome.page_results {
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

    pub(crate) fn refresh_bodies_and_receipts_request_plan(
        &self,
        plan: &mut BodyReceiptRequestPlan,
    ) {
        let mut plan_peer_ids = plan
            .body_peer_ids
            .iter()
            .chain(plan.receipt_peer_ids.iter())
            .copied()
            .collect::<HashSet<_>>();

        for peer_id in plan_peer_ids.drain() {
            let Some(peer) = self.peers.get(&peer_id) else {
                continue;
            };
            let snapshot = RequestPeerSnapshot {
                sender: peer.sender.clone(),
                version: peer.version,
                metrics: RequestPeerRoleMetrics::from_active_peer(peer),
            };
            plan.peers.insert(peer_id, snapshot);
        }
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
        let minimum_role_window = body_receipt_scheduled_chunk_limit(
            &ranges,
            body_receipt_plan_progress_target(return_blocks),
        );
        let range_indices_by_start: HashMap<usize, usize> = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect();
        let body_max_in_flight = body_receipt_role_request_window_remaining(
            &self.peers,
            &body_peer_ids,
            PeerRequestKind::Bodies,
            MAX_PARALLEL_BODY_REQUESTS,
            minimum_role_window,
        );
        if body_max_in_flight == 0 {
            return Ok(None);
        }
        let receipt_max_in_flight = body_receipt_role_request_window_remaining(
            &self.peers,
            &receipt_peer_ids,
            PeerRequestKind::Receipts,
            MAX_PARALLEL_RECEIPT_REQUESTS,
            minimum_role_window,
        );
        if receipt_max_in_flight == 0 {
            return Ok(None);
        }
        let max_in_flight = paired_body_receipt_chunk_window_limit(
            body_peer_ids.len().min(receipt_peer_ids.len()),
            MAX_PARALLEL_BODY_RECEIPT_REQUESTS,
        )
        .min(body_max_in_flight)
        .min(receipt_max_in_flight);
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
                            metrics: RequestPeerRoleMetrics::from_active_peer(peer),
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
            body_max_in_flight,
            receipt_max_in_flight,
            peer_rotation: self.request_cursor,
            priority: BodyReceiptRequestPriority::Full,
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
        let BodyReceiptCompletionChunks {
            blocks,
            planned_return_blocks,
            residual_chunks,
            min_accepted_prefix,
        } = body_receipt_completion_chunks(
            completion_return_blocks,
            chunks,
            min_accepted_prefix_override,
        );
        if blocks.len() >= min_accepted_prefix {
            self.advance_request_cursor();
            Ok(Some(BodyReceiptRequestCompletion {
                blocks,
                planned_return_blocks,
                residual_chunks,
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
                scheduler,
            } = accounting;
            self.apply_body_receipt_active_request_deltas(active_requests);
            self.body_receipt_scheduler_metrics.stale_role_retries = self
                .body_receipt_scheduler_metrics
                .stale_role_retries
                .saturating_add(scheduler.stale_role_retries);
            self.body_receipt_scheduler_metrics.prefix_reassignments = self
                .body_receipt_scheduler_metrics
                .prefix_reassignments
                .saturating_add(scheduler.prefix_reassignments);
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
        update_body_receipt_scheduler_attempt_metrics(
            &mut self.body_receipt_scheduler_metrics,
            &stats,
            &failures,
        );
        for (peer_id, kind, blocks, elapsed) in &stats {
            self.record_peer_request_success(*peer_id, *kind, *blocks, *elapsed);
        }
        self.apply_parallel_chunk_failures("body/receipt chunks", failures, &mut dead_peers);
        self.remove_dead_peers(&dead_peers);
    }
}

fn update_body_receipt_scheduler_attempt_metrics(
    metrics: &mut BodyReceiptSchedulerMetrics,
    stats: &TypedRequestStats,
    failures: &[ChunkRequestFailure],
) {
    for (_, kind, blocks, _) in stats {
        match kind {
            PeerRequestKind::Bodies => {
                metrics.body_successes = metrics.body_successes.saturating_add(1);
                metrics.body_blocks = metrics
                    .body_blocks
                    .saturating_add((*blocks).try_into().unwrap_or(u64::MAX));
            }
            PeerRequestKind::Receipts => {
                metrics.receipt_successes = metrics.receipt_successes.saturating_add(1);
                metrics.receipt_blocks = metrics
                    .receipt_blocks
                    .saturating_add((*blocks).try_into().unwrap_or(u64::MAX));
            }
            PeerRequestKind::Headers => {}
        }
    }
    for failure in failures {
        match failure.role {
            ChunkRequestRole::Bodies => {
                metrics.body_failures = metrics.body_failures.saturating_add(1);
            }
            ChunkRequestRole::Receipts => {
                metrics.receipt_failures = metrics.receipt_failures.saturating_add(1);
            }
        }
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
        scheduler: BodyReceiptSchedulerAccounting::default(),
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
        scheduler: BodyReceiptSchedulerAccounting::default(),
    });
}

fn emit_body_receipt_scheduler_accounting(
    accounting_tx: &Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
    scheduler: BodyReceiptSchedulerAccounting,
) {
    if scheduler.stale_role_retries == 0 && scheduler.prefix_reassignments == 0 {
        return;
    }

    let Some(accounting_tx) = accounting_tx else {
        return;
    };

    let _ = accounting_tx.send(BodyReceiptRequestAccounting {
        stats: Vec::new(),
        failures: Vec::new(),
        active_requests: Vec::new(),
        scheduler,
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

    pub(crate) fn with_max_return_blocks(mut self, return_blocks: usize) -> Self {
        self.return_blocks = self.return_blocks.min(return_blocks).min(self.hashes.len());
        self
    }

    pub(crate) fn reservations(&self) -> BodyReceiptRequestReservations {
        self.paired_initial_reservations()
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

    pub(crate) fn with_lookahead_priority(mut self) -> Self {
        self.priority = BodyReceiptRequestPriority::Lookahead;
        self
    }

    pub(crate) fn with_full_priority(mut self) -> Self {
        self.priority = BodyReceiptRequestPriority::Full;
        self
    }

    pub(crate) async fn execute(self) -> BodyReceiptRequestOutcome {
        self.execute_paired().await
    }

    fn paired_initial_reservations(&self) -> BodyReceiptRequestReservations {
        let mut pending_ranges =
            self.ranges
                .iter()
                .cloned()
                .enumerate()
                .filter_map(|(chunk_index, range)| {
                    body_receipt_prefix_range(range, self.return_blocks)
                        .map(|range| (chunk_index, range))
                });
        let min_return_blocks = body_receipt_plan_progress_target(self.return_blocks);
        let max_scheduled_chunks = body_receipt_priority_prefix_chunk_limit(
            self.priority,
            body_receipt_scheduled_chunk_limit(&self.ranges, min_return_blocks),
        )
        .min(self.max_in_flight);
        let mut peer_state = BodyReceiptPlanPeerState::default();
        let mut reservations = BodyReceiptRequestReservations::default();
        for _ in 0..max_scheduled_chunks {
            let Some((chunk_index, _range)) = pending_ranges.next() else {
                break;
            };
            add_paired_initial_chunk_reservation(
                self,
                &mut peer_state,
                &mut reservations,
                chunk_index,
            );
        }

        reservations
    }

    async fn execute_paired(self) -> BodyReceiptRequestOutcome {
        self.execute_paired_plan_live().await
    }

    async fn execute_paired_plan_live(self) -> BodyReceiptRequestOutcome {
        let accounting_forwarded = self.accounting_tx.is_some();
        let plan_started_at = Instant::now();
        let mut chunks = BTreeMap::new();
        let mut failures = Vec::new();
        let mut stats = Vec::new();
        let mut hedge_count = 0usize;
        let mut prefix_completed_at: Option<Instant> = None;
        let lane_schedule: BodyReceiptLiveLaneSchedule;
        let min_return_blocks = body_receipt_plan_progress_target(self.return_blocks);
        let partial_prefix_flush_blocks =
            body_receipt_partial_prefix_flush_blocks(self.return_blocks, min_return_blocks);
        {
            let mut attempts = futures_util::stream::FuturesUnordered::new();
            let mut pending_prefix_ranges = self
                .ranges
                .iter()
                .cloned()
                .enumerate()
                .filter_map(|(chunk_index, range)| {
                    body_receipt_prefix_range(range, min_return_blocks)
                        .map(|range| (chunk_index, range))
                })
                .collect::<std::collections::VecDeque<_>>();
            let mut pending_background_ranges = self
                .ranges
                .iter()
                .cloned()
                .enumerate()
                .filter_map(|(chunk_index, range)| {
                    body_receipt_background_range(range, min_return_blocks, self.return_blocks)
                        .map(|range| (chunk_index, range))
                })
                .collect::<std::collections::VecDeque<_>>();
            let mut retry_counts = HashMap::<usize, usize>::new();
            let mut active_chunks = HashMap::<usize, PlanLiveBodyReceiptChunk>::new();
            let mut peer_state = BodyReceiptPlanPeerState::default();
            let max_prefix_chunks = body_receipt_priority_prefix_chunk_limit(
                self.priority,
                body_receipt_scheduled_chunk_limit(&self.ranges, min_return_blocks),
            )
            .min(self.max_in_flight);
            let max_background_chunks = body_receipt_background_chunk_limit(
                &self.ranges,
                min_return_blocks,
                self.return_blocks,
                max_prefix_chunks,
                self.body_max_in_flight.min(self.receipt_max_in_flight),
            );
            let max_live_chunks = max_prefix_chunks.saturating_add(max_background_chunks);
            let max_chunk_attempts = self.max_in_flight.max(max_live_chunks).saturating_add(
                body_receipt_prefix_hedge_spare_attempts(
                    min_return_blocks,
                    self.body_peer_ids.len().min(self.receipt_peer_ids.len()),
                ),
            );
            let max_role_attempts =
                body_receipt_plan_live_role_attempt_limit(max_live_chunks, max_chunk_attempts);
            lane_schedule = BodyReceiptLiveLaneSchedule {
                min_return_blocks,
                max_prefix_chunks,
                max_background_chunks,
                max_live_chunks,
                max_role_attempts,
            };
            for _ in 0..lane_schedule.max_prefix_chunks {
                let Some((chunk_index, range)) = pending_prefix_ranges.pop_front() else {
                    break;
                };
                schedule_body_receipt_plan_chunk(
                    &self,
                    &mut attempts,
                    &mut active_chunks,
                    &mut peer_state,
                    range.clone(),
                    chunk_index,
                );
            }
            schedule_body_receipt_prefix_redundancy(
                &self,
                &mut attempts,
                &mut active_chunks,
                &mut peer_state,
                &chunks,
                lane_schedule,
            );
            schedule_body_receipt_background_chunks(
                &self,
                &mut attempts,
                &mut active_chunks,
                &mut peer_state,
                &mut pending_background_ranges,
                lane_schedule,
            );

            while !attempts.is_empty() {
                if let Some(completed_at) = prefix_completed_at
                    && (body_receipt_plan_live_in_flight_background_chunk_count(
                        &active_chunks,
                        min_return_blocks,
                    ) == 0
                        || completed_at.elapsed() >= PIPELINED_BODY_RECEIPT_BACKGROUND_DRAIN_GRACE)
                {
                    break;
                }
                let Some(wait_timeout) =
                    PIPELINED_BODY_RECEIPT_PLAN_TIMEOUT.checked_sub(plan_started_at.elapsed())
                else {
                    debug!(
                        elapsed_ms = plan_started_at.elapsed().as_millis(),
                        chunks = chunks.len(),
                        in_flight = body_receipt_plan_live_active_chunk_count(&active_chunks),
                        prefix_in_flight = body_receipt_plan_live_active_prefix_chunk_count(
                            &active_chunks,
                            min_return_blocks
                        ),
                        background_in_flight = body_receipt_plan_live_active_background_chunk_count(
                            &active_chunks,
                            min_return_blocks
                        ),
                        "body/receipt chunk pipeline hit plan timeout"
                    );
                    break;
                };
                let wait_slice = prefix_completed_at
                    .and_then(|completed_at| {
                        PIPELINED_BODY_RECEIPT_BACKGROUND_DRAIN_GRACE
                            .checked_sub(completed_at.elapsed())
                    })
                    .unwrap_or(PIPELINED_BODY_RECEIPT_HEDGE_DELAY)
                    .min(PIPELINED_BODY_RECEIPT_HEDGE_DELAY);
                if wait_slice.is_zero() {
                    break;
                }
                let role_attempt = match timeout(wait_timeout.min(wait_slice), attempts.next())
                    .await
                {
                    Ok(Some(role_attempt)) => role_attempt,
                    Ok(None) => break,
                    Err(_) => {
                        if plan_started_at.elapsed() >= PIPELINED_BODY_RECEIPT_PLAN_TIMEOUT {
                            debug!(
                                elapsed_ms = plan_started_at.elapsed().as_millis(),
                                chunks = chunks.len(),
                                in_flight =
                                    body_receipt_plan_live_active_chunk_count(&active_chunks),
                                prefix_in_flight = body_receipt_plan_live_active_prefix_chunk_count(
                                    &active_chunks,
                                    min_return_blocks
                                ),
                                background_in_flight =
                                    body_receipt_plan_live_active_background_chunk_count(
                                        &active_chunks,
                                        min_return_blocks
                                    ),
                                "body/receipt chunk pipeline hit plan timeout"
                            );
                            break;
                        }
                        if prefix_completed_at.is_some() {
                            break;
                        }
                        let prefix_critical = body_receipt_prefix_critical_repair_needed(
                            &active_chunks,
                            &chunks,
                            min_return_blocks,
                        );
                        let role_attempt_limit = body_receipt_prefix_critical_role_attempt_limit(
                            lane_schedule.max_role_attempts,
                            prefix_critical,
                        );
                        if hedge_count < PIPELINED_BODY_RECEIPT_MAX_HEDGES
                            && attempts.len() < role_attempt_limit
                        {
                            let scheduled = schedule_stale_body_receipt_plan_roles(
                                &self,
                                &mut attempts,
                                &mut active_chunks,
                                &mut peer_state,
                                &chunks,
                                BodyReceiptStaleRoleSchedule {
                                    min_return_blocks,
                                    max_role_attempts: role_attempt_limit,
                                    prefix_critical,
                                },
                            );
                            hedge_count += scheduled;
                            emit_body_receipt_scheduler_accounting(
                                &self.accounting_tx,
                                BodyReceiptSchedulerAccounting {
                                    stale_role_retries: if prefix_critical {
                                        0
                                    } else {
                                        scheduled as u64
                                    },
                                    prefix_reassignments: if prefix_critical {
                                        scheduled as u64
                                    } else {
                                        0
                                    },
                                },
                            );
                        }
                        continue;
                    }
                };

                let start = body_receipt_plan_role_attempt_start(&role_attempt);
                apply_body_receipt_plan_role_attempt(
                    &mut active_chunks,
                    &mut peer_state,
                    &mut chunks,
                    &mut failures,
                    &mut stats,
                    role_attempt,
                    &self.accounting_tx,
                );

                let remove_completed = active_chunks.get(&start).is_some_and(|chunk| {
                    chunk.completed
                        && chunk.state.bodies.in_flight == 0
                        && chunk.state.receipts.in_flight == 0
                });
                if remove_completed {
                    active_chunks.remove(&start);
                }

                let contiguous_blocks = contiguous_chunk_blocks(&chunks);
                if contiguous_blocks >= min_return_blocks || contiguous_blocks == self.hashes.len()
                {
                    if contiguous_blocks == self.hashes.len()
                        || body_receipt_plan_live_in_flight_background_chunk_count(
                            &active_chunks,
                            min_return_blocks,
                        ) == 0
                    {
                        break;
                    }
                    prefix_completed_at.get_or_insert_with(Instant::now);
                    continue;
                }
                if body_receipt_partial_prefix_flush_ready(
                    contiguous_blocks,
                    partial_prefix_flush_blocks,
                    min_return_blocks,
                    plan_started_at.elapsed(),
                    &active_chunks,
                    &chunks,
                ) {
                    debug!(
                        contiguous_blocks,
                        min_return_blocks,
                        partial_prefix_flush_blocks,
                        elapsed_ms = plan_started_at.elapsed().as_millis(),
                        "body/receipt chunk pipeline flushing partial prefix before slow tail"
                    );
                    break;
                }
                let prefix_critical = body_receipt_prefix_critical_repair_needed(
                    &active_chunks,
                    &chunks,
                    min_return_blocks,
                );
                let role_attempt_limit = body_receipt_prefix_critical_role_attempt_limit(
                    lane_schedule.max_role_attempts,
                    prefix_critical,
                );
                if attempts.len() < role_attempt_limit {
                    let scheduled = schedule_stale_body_receipt_plan_roles(
                        &self,
                        &mut attempts,
                        &mut active_chunks,
                        &mut peer_state,
                        &chunks,
                        BodyReceiptStaleRoleSchedule {
                            min_return_blocks,
                            max_role_attempts: role_attempt_limit,
                            prefix_critical,
                        },
                    );
                    emit_body_receipt_scheduler_accounting(
                        &self.accounting_tx,
                        BodyReceiptSchedulerAccounting {
                            stale_role_retries: if prefix_critical { 0 } else { scheduled as u64 },
                            prefix_reassignments: if prefix_critical {
                                scheduled as u64
                            } else {
                                0
                            },
                        },
                    );
                }

                body_receipt_remove_exhausted_prefix_chunks(
                    &mut active_chunks,
                    &chunks,
                    min_return_blocks,
                );

                let missing_prefix_attempt_limit = body_receipt_prefix_critical_role_attempt_limit(
                    lane_schedule.max_role_attempts,
                    body_receipt_prefix_critical_repair_needed(
                        &active_chunks,
                        &chunks,
                        min_return_blocks,
                    ),
                );
                if attempts.len() < missing_prefix_attempt_limit
                    && let Some((range, chunk_index)) =
                        body_receipt_missing_prefix_reassign_candidate(
                            &self.ranges,
                            &self.range_indices_by_start,
                            &mut retry_counts,
                            &body_receipt_plan_live_in_flight_chunks(&active_chunks),
                            &chunks,
                            min_return_blocks,
                        )
                {
                    schedule_body_receipt_plan_chunk(
                        &self,
                        &mut attempts,
                        &mut active_chunks,
                        &mut peer_state,
                        range,
                        chunk_index,
                    );
                    emit_body_receipt_scheduler_accounting(
                        &self.accounting_tx,
                        BodyReceiptSchedulerAccounting {
                            stale_role_retries: 0,
                            prefix_reassignments: 1,
                        },
                    );
                }

                schedule_body_receipt_prefix_chunks(
                    &self,
                    &mut attempts,
                    &mut active_chunks,
                    &mut peer_state,
                    &mut pending_prefix_ranges,
                    lane_schedule,
                );
                schedule_body_receipt_prefix_redundancy(
                    &self,
                    &mut attempts,
                    &mut active_chunks,
                    &mut peer_state,
                    &chunks,
                    lane_schedule,
                );
                schedule_body_receipt_background_chunks(
                    &self,
                    &mut attempts,
                    &mut active_chunks,
                    &mut peer_state,
                    &mut pending_background_ranges,
                    lane_schedule,
                );
            }
        }

        let prefix_salvaged_chunks = self
            .salvage_live_body_receipt_prefix(
                &mut chunks,
                &mut failures,
                &mut stats,
                min_return_blocks,
            )
            .await;

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
            hedges = hedge_count,
            prefix_salvaged_chunks,
            max_prefix_chunks = lane_schedule.max_prefix_chunks,
            max_background_chunks = lane_schedule.max_background_chunks,
            max_attempts = lane_schedule.max_role_attempts,
            prefix_background_drain_ms = prefix_completed_at
                .map(|completed_at| completed_at.elapsed().as_millis())
                .unwrap_or(0),
            plan_ms = plan_started_at.elapsed().as_millis(),
            "body/receipt live role pipeline plan completed"
        );

        BodyReceiptRequestOutcome {
            total_hashes: self.hashes.len(),
            return_blocks: self.return_blocks,
            planned_return_blocks: body_receipt_completed_plan_return_blocks(
                &chunks,
                self.return_blocks,
            ),
            chunks,
            failures,
            stats,
            accounting_forwarded,
        }
    }

    async fn salvage_live_body_receipt_prefix(
        &self,
        chunks: &mut BTreeMap<usize, Vec<SourcedBodyReceipts>>,
        failures: &mut ParallelChunkFailures,
        stats: &mut TypedRequestStats,
        min_return_blocks: usize,
    ) -> usize {
        let started_at = Instant::now();
        let mut salvaged_chunks = 0usize;
        for salvage_round in 0..PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_CHUNKS {
            if started_at.elapsed() >= PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_TIMEOUT {
                break;
            }
            let Some(range) =
                body_receipt_missing_prefix_salvage_range(&self.ranges, chunks, min_return_blocks)
            else {
                break;
            };
            let base_chunk_index = self
                .range_indices_by_start
                .get(&range.start)
                .copied()
                .unwrap_or_default();
            let chunk_index = base_chunk_index + ((salvage_round + 1) * self.ranges.len().max(1));
            let Some(blocks) = self
                .salvage_live_body_receipt_prefix_range(
                    range.clone(),
                    chunk_index,
                    started_at,
                    failures,
                    stats,
                )
                .await
            else {
                break;
            };
            chunks.insert(range.start, blocks);
            salvaged_chunks += 1;
            if contiguous_chunk_blocks(chunks) >= min_return_blocks {
                break;
            }
        }

        salvaged_chunks
    }

    async fn salvage_live_body_receipt_prefix_range(
        &self,
        range: std::ops::Range<usize>,
        chunk_index: usize,
        salvage_started_at: Instant,
        failures: &mut ParallelChunkFailures,
        stats: &mut TypedRequestStats,
    ) -> Option<Vec<SourcedBodyReceipts>> {
        let request_hashes = self.hashes[range.clone()].to_vec();
        let mut body_bad_peers = disabled_chunk_peers(failures, ChunkRequestRole::Bodies);
        let mut receipt_bad_peers = disabled_chunk_peers(failures, ChunkRequestRole::Receipts);
        let empty_in_flight = HashMap::new();
        let (body_ordered, _) = body_receipt_attempt_peer_ids(
            &self.body_peer_ids,
            &body_bad_peers,
            &empty_in_flight,
            chunk_index,
            PeerRequestKind::Bodies,
            &self.peers,
        );

        for body_peer in body_ordered
            .into_iter()
            .take(PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_PEER_LIMIT)
        {
            if salvage_started_at.elapsed() >= PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_TIMEOUT {
                break;
            }

            let body_started_at = Instant::now();
            let body_result = {
                let _active = BodyReceiptActiveRequestGuard::new(
                    &self.accounting_tx,
                    body_peer,
                    PeerRequestKind::Bodies,
                );
                self.request_bodies_until_complete(body_peer, request_hashes.clone())
                    .await
            };
            let raw_bodies = match body_result {
                Ok(raw_bodies) => {
                    stats.push((
                        body_peer,
                        PeerRequestKind::Bodies,
                        raw_bodies.len(),
                        body_started_at.elapsed(),
                    ));
                    emit_body_receipt_role_success(
                        &self.accounting_tx,
                        body_peer,
                        PeerRequestKind::Bodies,
                        raw_bodies.len(),
                        body_started_at.elapsed(),
                    );
                    raw_bodies
                }
                Err(kind) => {
                    let failure = ChunkRequestFailure {
                        role: ChunkRequestRole::Bodies,
                        peer_id: body_peer,
                        requested: request_hashes.len(),
                        kind,
                    };
                    if chunk_failure_disables_role_peer(&failure) {
                        body_bad_peers.insert(body_peer);
                    }
                    emit_body_receipt_role_failure(&self.accounting_tx, failure.clone());
                    failures.push(failure);
                    continue;
                }
            };

            let sourced_bodies = raw_bodies
                .into_iter()
                .map(|body| (body_peer, body))
                .collect::<Vec<_>>();
            let expected_receipt_counts = sourced_bodies
                .iter()
                .map(|(_, body)| body.transaction_count())
                .collect::<Vec<_>>();
            let (receipt_ordered, _) = body_receipt_attempt_peer_ids(
                &self.receipt_peer_ids,
                &receipt_bad_peers,
                &empty_in_flight,
                chunk_index,
                PeerRequestKind::Receipts,
                &self.peers,
            );
            let receipt_candidates = receipt_candidates_for_body_peer(
                receipt_ordered,
                body_peer,
                PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_PEER_LIMIT,
            );
            for receipt_peer in receipt_candidates {
                if salvage_started_at.elapsed() >= PIPELINED_BODY_RECEIPT_PREFIX_SALVAGE_TIMEOUT {
                    break;
                }

                let receipt_started_at = Instant::now();
                let receipt_result = {
                    let _active = BodyReceiptActiveRequestGuard::new(
                        &self.accounting_tx,
                        receipt_peer,
                        PeerRequestKind::Receipts,
                    );
                    self.request_receipts_until_complete(
                        receipt_peer,
                        request_hashes.clone(),
                        Some(expected_receipt_counts.clone()),
                    )
                    .await
                };
                match receipt_result {
                    Ok(receipts) => {
                        stats.push((
                            receipt_peer,
                            PeerRequestKind::Receipts,
                            receipts.len(),
                            receipt_started_at.elapsed(),
                        ));
                        emit_body_receipt_role_success(
                            &self.accounting_tx,
                            receipt_peer,
                            PeerRequestKind::Receipts,
                            receipts.len(),
                            receipt_started_at.elapsed(),
                        );
                        match body_receipt_blocks_if_counts_match(
                            &sourced_bodies,
                            receipt_peer,
                            receipts,
                            range.len(),
                        ) {
                            Ok(blocks) => return Some(blocks),
                            Err(kind) => {
                                let failure = ChunkRequestFailure {
                                    role: ChunkRequestRole::Receipts,
                                    peer_id: receipt_peer,
                                    requested: range.len(),
                                    kind: ChunkFailureKind::ReceiptCountMismatch(kind),
                                };
                                if chunk_failure_disables_role_peer(&failure) {
                                    receipt_bad_peers.insert(receipt_peer);
                                }
                                emit_body_receipt_role_failure(
                                    &self.accounting_tx,
                                    failure.clone(),
                                );
                                failures.push(failure);
                            }
                        }
                    }
                    Err(kind) => {
                        let failure = ChunkRequestFailure {
                            role: ChunkRequestRole::Receipts,
                            peer_id: receipt_peer,
                            requested: request_hashes.len(),
                            kind,
                        };
                        if chunk_failure_disables_role_peer(&failure) {
                            receipt_bad_peers.insert(receipt_peer);
                        }
                        emit_body_receipt_role_failure(&self.accounting_tx, failure.clone());
                        failures.push(failure);
                    }
                }
            }
        }

        None
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
            let request_timeout =
                self.role_request_timeout(peer_id, PeerRequestKind::Bodies, request_hashes.len());
            let response = self
                .request_bodies(peer_id, request_hashes.clone(), request_timeout)
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
            let request_timeout =
                self.role_request_timeout(peer_id, PeerRequestKind::Receipts, request_hashes.len());
            let response = if version >= EthVersion::Eth69 {
                self.request_receipts69(peer_id, request_hashes.clone(), request_timeout)
                    .await
            } else {
                self.request_receipts(peer_id, request_hashes.clone(), request_timeout)
                    .await
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
        request_timeout: Duration,
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

    async fn request_receipts(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
        request_timeout: Duration,
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

    async fn request_receipts69(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
        request_timeout: Duration,
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
            let request_timeout =
                self.role_request_timeout(peer_id, PeerRequestKind::Receipts, request_hashes.len());
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

    async fn request_with_channel<T, W, MakeRequest>(
        &self,
        peer_id: PeerId,
        make_request: &MakeRequest,
        request_timeout: Duration,
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

        match timeout(request_timeout, response_rx).await {
            Ok(Ok(Ok(response))) => Ok(response.into_value()),
            Ok(Ok(Err(error))) => Err(RequestAttempt::Request(error)),
            Ok(Err(_)) => Err(RequestAttempt::Disconnected),
            Err(_) => Err(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::Timeout,
            )),
        }
    }

    fn role_request_timeout(
        &self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        requested: usize,
    ) -> Duration {
        body_receipt_role_request_timeout(
            self.peers.get(&peer_id).map(|peer| &peer.metrics),
            kind,
            requested,
        )
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

fn body_receipt_plan_live_role_attempt_limit(
    max_unique_chunks: usize,
    max_chunk_attempts: usize,
) -> usize {
    max_unique_chunks
        .saturating_mul(2)
        .saturating_add(max_chunk_attempts)
        .max(max_chunk_attempts.saturating_mul(2))
}

fn body_receipt_plan_live_active_chunk_count(
    chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
) -> usize {
    chunks.values().filter(|chunk| !chunk.completed).count()
}

fn body_receipt_plan_live_active_prefix_chunk_count(
    chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
    min_return_blocks: usize,
) -> usize {
    chunks
        .values()
        .filter(|chunk| !chunk.completed && chunk.range.start < min_return_blocks)
        .count()
}

fn body_receipt_plan_live_active_background_chunk_count(
    chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
    min_return_blocks: usize,
) -> usize {
    chunks
        .values()
        .filter(|chunk| !chunk.completed && chunk.range.start >= min_return_blocks)
        .count()
}

fn body_receipt_plan_live_in_flight_background_chunk_count(
    chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
    min_return_blocks: usize,
) -> usize {
    chunks
        .values()
        .filter(|chunk| {
            chunk.range.start >= min_return_blocks && body_receipt_chunk_has_in_flight_roles(chunk)
        })
        .count()
}

fn body_receipt_plan_live_in_flight_chunks(
    chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
) -> HashSet<usize> {
    chunks
        .iter()
        .filter(|(_, chunk)| body_receipt_chunk_has_in_flight_roles(chunk))
        .map(|(start, _)| *start)
        .collect()
}

fn body_receipt_chunk_has_in_flight_roles(chunk: &PlanLiveBodyReceiptChunk) -> bool {
    !chunk.completed && (chunk.state.bodies.in_flight > 0 || chunk.state.receipts.in_flight > 0)
}

fn body_receipt_chunk_can_schedule_missing_role(chunk: &PlanLiveBodyReceiptChunk) -> bool {
    if chunk.completed {
        return false;
    }

    let needs_bodies = chunk.bodies.is_none();
    let can_schedule_bodies =
        needs_bodies && chunk.state.bodies.next_index < chunk.candidates.bodies.len();
    let needs_receipts = chunk.bodies.is_some() || chunk.cached_receipts.is_empty();
    let can_schedule_receipts =
        needs_receipts && chunk.state.receipts.next_index < chunk.candidates.receipts.len();

    can_schedule_bodies || can_schedule_receipts
}

fn body_receipt_remove_exhausted_prefix_chunks<T>(
    active_chunks: &mut HashMap<usize, PlanLiveBodyReceiptChunk>,
    completed_chunks: &BTreeMap<usize, Vec<T>>,
    min_return_blocks: usize,
) -> usize {
    let contiguous_blocks = contiguous_chunk_blocks(completed_chunks);
    let exhausted_starts = active_chunks
        .iter()
        .filter_map(|(start, chunk)| {
            let is_missing_prefix = *start <= contiguous_blocks
                && *start < min_return_blocks
                && !completed_chunks.contains_key(start);
            if is_missing_prefix
                && !body_receipt_chunk_has_in_flight_roles(chunk)
                && !body_receipt_chunk_can_schedule_missing_role(chunk)
            {
                Some(*start)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    for start in &exhausted_starts {
        active_chunks.remove(start);
    }

    exhausted_starts.len()
}

impl BodyReceiptRequestReservations {
    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(super) fn entries(&self) -> &[BodyReceiptRequestReservation] {
        &self.entries
    }

    pub(crate) fn body_receipt_counts(&self) -> (usize, usize) {
        let mut body_requests = 0usize;
        let mut receipt_requests = 0usize;
        for entry in &self.entries {
            match entry.kind {
                PeerRequestKind::Headers => {}
                PeerRequestKind::Bodies => {
                    body_requests = body_requests.saturating_add(entry.count);
                }
                PeerRequestKind::Receipts => {
                    receipt_requests = receipt_requests.saturating_add(entry.count);
                }
            }
        }
        (body_requests, receipt_requests)
    }

    fn add(&mut self, peer_id: PeerId, kind: PeerRequestKind) {
        if matches!(kind, PeerRequestKind::Headers) {
            return;
        }
        if let Some(entry) = self
            .entries
            .iter_mut()
            .find(|entry| entry.peer_id == peer_id && matches_same_request_kind(entry.kind, kind))
        {
            entry.count = entry.count.saturating_add(1);
            return;
        }
        self.entries.push(BodyReceiptRequestReservation {
            peer_id,
            kind,
            count: 1,
        });
    }
}

fn matches_same_request_kind(left: PeerRequestKind, right: PeerRequestKind) -> bool {
    matches!(
        (left, right),
        (PeerRequestKind::Headers, PeerRequestKind::Headers)
            | (PeerRequestKind::Bodies, PeerRequestKind::Bodies)
            | (PeerRequestKind::Receipts, PeerRequestKind::Receipts)
    )
}

fn add_paired_initial_chunk_reservation(
    plan: &BodyReceiptRequestPlan,
    peer_state: &mut BodyReceiptPlanPeerState,
    reservations: &mut BodyReceiptRequestReservations,
    chunk_index: usize,
) {
    let (body_candidates, body_peer) = body_receipt_attempt_peer_ids(
        &plan.body_peer_ids,
        &peer_state.body_bad_peers,
        &peer_state.body_in_flight_peers,
        chunk_index,
        PeerRequestKind::Bodies,
        &plan.peers,
    );
    let body_peer = body_peer.or_else(|| body_candidates.first().copied());
    if let Some(peer_id) = body_peer {
        record_body_receipt_attempt_peer(&mut peer_state.body_in_flight_peers, Some(peer_id));
        reservations.add(peer_id, PeerRequestKind::Bodies);
    }

    let (receipt_ordered, _) = body_receipt_attempt_peer_ids(
        &plan.receipt_peer_ids,
        &peer_state.receipt_bad_peers,
        &peer_state.receipt_in_flight_peers,
        chunk_index,
        PeerRequestKind::Receipts,
        &plan.peers,
    );
    let receipt_candidates = if let Some(body_peer) = body_peer {
        receipt_candidates_for_body_peer(receipt_ordered, body_peer, PIPELINED_CHUNK_REQUEST_PEERS)
    } else {
        receipt_ordered
            .into_iter()
            .take(PIPELINED_CHUNK_REQUEST_PEERS)
            .collect::<Vec<_>>()
    };
    if let Some(peer_id) = receipt_candidates
        .into_iter()
        .find(|peer_id| !peer_state.receipt_bad_peers.contains(peer_id))
    {
        record_body_receipt_attempt_peer(&mut peer_state.receipt_in_flight_peers, Some(peer_id));
        reservations.add(peer_id, PeerRequestKind::Receipts);
    }
}

fn schedule_body_receipt_plan_chunk<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    active_chunks: &mut HashMap<usize, PlanLiveBodyReceiptChunk>,
    peer_state: &mut BodyReceiptPlanPeerState,
    range: std::ops::Range<usize>,
    chunk_index: usize,
) {
    if active_chunks.contains_key(&range.start) {
        return;
    }

    let (body_candidates, body_peer) = body_receipt_attempt_peer_ids(
        &plan.body_peer_ids,
        &peer_state.body_bad_peers,
        &peer_state.body_in_flight_peers,
        chunk_index,
        PeerRequestKind::Bodies,
        &plan.peers,
    );
    let (receipt_ordered, _) = body_receipt_attempt_peer_ids(
        &plan.receipt_peer_ids,
        &peer_state.receipt_bad_peers,
        &peer_state.receipt_in_flight_peers,
        chunk_index,
        PeerRequestKind::Receipts,
        &plan.peers,
    );
    let mut receipt_candidates = if let Some(body_peer) = body_peer {
        receipt_candidates_for_body_peer(receipt_ordered, body_peer, PIPELINED_CHUNK_REQUEST_PEERS)
    } else {
        receipt_ordered
            .into_iter()
            .take(PIPELINED_CHUNK_REQUEST_PEERS)
            .collect::<Vec<_>>()
    };
    receipt_candidates.retain(|peer_id| !peer_state.receipt_bad_peers.contains(peer_id));

    let mut chunk = PlanLiveBodyReceiptChunk {
        range: range.clone(),
        candidates: BodyReceiptChunkLiveCandidates {
            bodies: body_candidates
                .into_iter()
                .take(PIPELINED_CHUNK_REQUEST_PEERS)
                .collect(),
            receipts: receipt_candidates,
        },
        state: BodyReceiptChunkLiveState::default(),
        bodies: None,
        expected_receipt_counts: None,
        cached_receipts: Vec::new(),
        completed: false,
        hedges: 0,
    };

    let hashes = &plan.hashes[range.clone()];
    schedule_body_receipt_plan_body_role(
        plan,
        attempts,
        peer_state,
        range.start,
        &mut chunk,
        hashes,
    );
    schedule_body_receipt_plan_receipt_role(
        plan,
        attempts,
        peer_state,
        range.start,
        &mut chunk,
        hashes,
        None,
    );
    active_chunks.insert(range.start, chunk);
}

fn schedule_body_receipt_prefix_chunks<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    active_chunks: &mut HashMap<usize, PlanLiveBodyReceiptChunk>,
    peer_state: &mut BodyReceiptPlanPeerState,
    pending_ranges: &mut std::collections::VecDeque<(usize, std::ops::Range<usize>)>,
    schedule: BodyReceiptLiveLaneSchedule,
) -> usize {
    let mut scheduled = 0usize;
    while body_receipt_plan_live_active_prefix_chunk_count(
        active_chunks,
        schedule.min_return_blocks,
    ) < schedule.max_prefix_chunks
        && attempts.len() < schedule.max_role_attempts
    {
        let Some((chunk_index, range)) = pending_ranges.pop_front() else {
            break;
        };
        schedule_body_receipt_plan_chunk(
            plan,
            attempts,
            active_chunks,
            peer_state,
            range,
            chunk_index,
        );
        scheduled += 1;
    }

    scheduled
}

fn schedule_body_receipt_background_chunks<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    active_chunks: &mut HashMap<usize, PlanLiveBodyReceiptChunk>,
    peer_state: &mut BodyReceiptPlanPeerState,
    pending_ranges: &mut std::collections::VecDeque<(usize, std::ops::Range<usize>)>,
    schedule: BodyReceiptLiveLaneSchedule,
) -> usize {
    let mut scheduled = 0usize;
    while schedule.max_background_chunks > 0
        && body_receipt_plan_live_active_background_chunk_count(
            active_chunks,
            schedule.min_return_blocks,
        ) < schedule.max_background_chunks
        && body_receipt_plan_live_active_chunk_count(active_chunks) < schedule.max_live_chunks
        && attempts.len() < schedule.max_role_attempts
    {
        let Some((chunk_index, range)) = pending_ranges.pop_front() else {
            break;
        };
        schedule_body_receipt_plan_chunk(
            plan,
            attempts,
            active_chunks,
            peer_state,
            range,
            chunk_index,
        );
        scheduled += 1;
    }

    scheduled
}

fn schedule_body_receipt_prefix_redundancy<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    active_chunks: &mut HashMap<usize, PlanLiveBodyReceiptChunk>,
    peer_state: &mut BodyReceiptPlanPeerState,
    completed_chunks: &BTreeMap<usize, Vec<SourcedBodyReceipts>>,
    schedule: BodyReceiptLiveLaneSchedule,
) -> usize {
    if plan.priority != BodyReceiptRequestPriority::Full
        || schedule.min_return_blocks == 0
        || attempts.len() >= schedule.max_role_attempts
        || plan.body_peer_ids.len().min(plan.receipt_peer_ids.len())
            < PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS
    {
        return 0;
    }

    let mut starts = active_chunks
        .keys()
        .copied()
        .filter(|start| {
            *start < schedule.min_return_blocks && !completed_chunks.contains_key(start)
        })
        .collect::<Vec<_>>();
    starts.sort_unstable();

    let mut scheduled = 0usize;
    for start in starts
        .into_iter()
        .take(PIPELINED_BODY_RECEIPT_PREFIX_EARLY_REDUNDANT_CHUNKS)
    {
        if attempts.len() >= schedule.max_role_attempts {
            break;
        }
        let Some(chunk) = active_chunks.get_mut(&start) else {
            continue;
        };
        if chunk.completed
            || chunk.hedges > 0
            || chunk.hedges >= PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK
        {
            continue;
        }
        let chunk_index = plan
            .range_indices_by_start
            .get(&start)
            .copied()
            .unwrap_or_default()
            .saturating_add(plan.ranges.len().max(1));
        extend_body_receipt_plan_chunk_candidates(plan, peer_state, chunk_index, chunk);
        scheduled += schedule_missing_body_receipt_plan_roles(
            plan,
            attempts,
            peer_state,
            start,
            chunk,
            schedule.max_role_attempts,
            true,
        );
    }

    scheduled
}

fn schedule_body_receipt_plan_body_role<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    peer_state: &mut BodyReceiptPlanPeerState,
    start: usize,
    chunk: &mut PlanLiveBodyReceiptChunk,
    hashes: &[B256],
) -> bool {
    let Some(peer_id) = next_body_receipt_role_candidate(
        &chunk.candidates.bodies,
        &mut chunk.state.bodies,
        &peer_state.body_bad_peers,
    ) else {
        return false;
    };

    let request_hashes = hashes.to_vec();
    record_body_receipt_attempt_peer(&mut peer_state.body_in_flight_peers, Some(peer_id));
    chunk.state.bodies.in_flight += 1;
    chunk.state.bodies.last_scheduled_at = Some(Instant::now());
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
            BodyReceiptPlanRoleAttempt::Bodies {
                start,
                peer_id,
                requested,
                elapsed: started_at.elapsed(),
                result,
            }
        }
        .boxed(),
    );
    true
}

fn schedule_body_receipt_plan_receipt_role<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    peer_state: &mut BodyReceiptPlanPeerState,
    start: usize,
    chunk: &mut PlanLiveBodyReceiptChunk,
    hashes: &[B256],
    expected_receipt_counts: Option<&[usize]>,
) -> bool {
    let Some(peer_id) = next_body_receipt_role_candidate(
        &chunk.candidates.receipts,
        &mut chunk.state.receipts,
        &peer_state.receipt_bad_peers,
    ) else {
        return false;
    };

    let request_hashes = hashes.to_vec();
    let expected_receipt_counts = expected_receipt_counts.map(|counts| counts.to_vec());
    record_body_receipt_attempt_peer(&mut peer_state.receipt_in_flight_peers, Some(peer_id));
    chunk.state.receipts.in_flight += 1;
    chunk.state.receipts.last_scheduled_at = Some(Instant::now());
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
                .request_receipts_until_complete(peer_id, request_hashes, expected_receipt_counts)
                .await;
            BodyReceiptPlanRoleAttempt::Receipts {
                start,
                peer_id,
                requested,
                elapsed: started_at.elapsed(),
                result,
            }
        }
        .boxed(),
    );
    true
}

fn next_body_receipt_role_candidate(
    candidates: &[PeerId],
    state: &mut BodyReceiptChunkLiveRoleState,
    bad_peers: &HashSet<PeerId>,
) -> Option<PeerId> {
    while state.next_index < candidates.len() {
        let peer_id = candidates[state.next_index];
        state.next_index += 1;
        if bad_peers.contains(&peer_id) || !state.used_peers.insert(peer_id) {
            continue;
        }
        return Some(peer_id);
    }

    None
}

fn body_receipt_plan_role_attempt_start(attempt: &BodyReceiptPlanRoleAttempt) -> usize {
    match attempt {
        BodyReceiptPlanRoleAttempt::Bodies { start, .. }
        | BodyReceiptPlanRoleAttempt::Receipts { start, .. } => *start,
    }
}

fn apply_body_receipt_plan_role_attempt(
    active_chunks: &mut HashMap<usize, PlanLiveBodyReceiptChunk>,
    peer_state: &mut BodyReceiptPlanPeerState,
    completed_chunks: &mut BTreeMap<usize, Vec<SourcedBodyReceipts>>,
    failures: &mut ParallelChunkFailures,
    stats: &mut TypedRequestStats,
    attempt: BodyReceiptPlanRoleAttempt,
    accounting_tx: &Option<mpsc::UnboundedSender<BodyReceiptRequestAccounting>>,
) {
    match attempt {
        BodyReceiptPlanRoleAttempt::Bodies {
            start,
            peer_id,
            requested,
            elapsed,
            result,
        } => {
            release_body_receipt_attempt_peer(&mut peer_state.body_in_flight_peers, Some(peer_id));
            let Some(chunk) = active_chunks.get_mut(&start) else {
                return;
            };
            chunk.state.bodies.in_flight = chunk.state.bodies.in_flight.saturating_sub(1);
            if chunk.completed {
                return;
            }
            match result {
                Ok(raw_bodies) => {
                    stats.push((peer_id, PeerRequestKind::Bodies, raw_bodies.len(), elapsed));
                    emit_body_receipt_role_success(
                        accounting_tx,
                        peer_id,
                        PeerRequestKind::Bodies,
                        raw_bodies.len(),
                        elapsed,
                    );
                    let sourced_bodies = raw_bodies
                        .into_iter()
                        .map(|body| (peer_id, body))
                        .collect::<Vec<_>>();
                    chunk.expected_receipt_counts = Some(
                        sourced_bodies
                            .iter()
                            .map(|(_, body)| body.transaction_count())
                            .collect(),
                    );
                    for (receipt_peer, receipts) in chunk.cached_receipts.drain(..) {
                        match body_receipt_blocks_if_counts_match(
                            &sourced_bodies,
                            receipt_peer,
                            receipts,
                            chunk.range.len(),
                        ) {
                            Ok(blocks) => {
                                completed_chunks.insert(start, blocks);
                                chunk.completed = true;
                                return;
                            }
                            Err(kind) => {
                                let failure = ChunkRequestFailure {
                                    role: ChunkRequestRole::Receipts,
                                    peer_id: receipt_peer,
                                    requested: chunk.range.len(),
                                    kind: ChunkFailureKind::ReceiptCountMismatch(kind),
                                };
                                if chunk_failure_disables_role_peer(&failure) {
                                    peer_state.receipt_bad_peers.insert(receipt_peer);
                                }
                                emit_body_receipt_role_failure(accounting_tx, failure.clone());
                                failures.push(failure);
                            }
                        }
                    }
                    chunk.bodies = Some(sourced_bodies);
                }
                Err(kind) => {
                    let failure = ChunkRequestFailure {
                        role: ChunkRequestRole::Bodies,
                        peer_id,
                        requested,
                        kind,
                    };
                    if chunk_failure_disables_role_peer(&failure) {
                        peer_state.body_bad_peers.insert(peer_id);
                    }
                    emit_body_receipt_role_failure(accounting_tx, failure.clone());
                    failures.push(failure);
                }
            }
        }
        BodyReceiptPlanRoleAttempt::Receipts {
            start,
            peer_id,
            requested,
            elapsed,
            result,
        } => {
            release_body_receipt_attempt_peer(
                &mut peer_state.receipt_in_flight_peers,
                Some(peer_id),
            );
            let Some(chunk) = active_chunks.get_mut(&start) else {
                return;
            };
            chunk.state.receipts.in_flight = chunk.state.receipts.in_flight.saturating_sub(1);
            if chunk.completed {
                return;
            }
            match result {
                Ok(receipts) => {
                    stats.push((peer_id, PeerRequestKind::Receipts, receipts.len(), elapsed));
                    emit_body_receipt_role_success(
                        accounting_tx,
                        peer_id,
                        PeerRequestKind::Receipts,
                        receipts.len(),
                        elapsed,
                    );
                    if let Some(bodies) = chunk.bodies.as_ref() {
                        match body_receipt_blocks_if_counts_match(
                            bodies,
                            peer_id,
                            receipts,
                            chunk.range.len(),
                        ) {
                            Ok(blocks) => {
                                completed_chunks.insert(start, blocks);
                                chunk.completed = true;
                            }
                            Err(kind) => {
                                let failure = ChunkRequestFailure {
                                    role: ChunkRequestRole::Receipts,
                                    peer_id,
                                    requested,
                                    kind: ChunkFailureKind::ReceiptCountMismatch(kind),
                                };
                                if chunk_failure_disables_role_peer(&failure) {
                                    peer_state.receipt_bad_peers.insert(peer_id);
                                }
                                emit_body_receipt_role_failure(accounting_tx, failure.clone());
                                failures.push(failure);
                            }
                        }
                    } else {
                        chunk.cached_receipts.push((peer_id, receipts));
                    }
                }
                Err(kind) => {
                    let failure = ChunkRequestFailure {
                        role: ChunkRequestRole::Receipts,
                        peer_id,
                        requested,
                        kind,
                    };
                    if chunk_failure_disables_role_peer(&failure) {
                        peer_state.receipt_bad_peers.insert(peer_id);
                    }
                    emit_body_receipt_role_failure(accounting_tx, failure.clone());
                    failures.push(failure);
                }
            }
        }
    }
}

fn schedule_missing_body_receipt_plan_roles<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    peer_state: &mut BodyReceiptPlanPeerState,
    start: usize,
    chunk: &mut PlanLiveBodyReceiptChunk,
    max_role_attempts: usize,
    force: bool,
) -> usize {
    if chunk.completed || attempts.len() >= max_role_attempts {
        return 0;
    }

    let hashes = &plan.hashes[chunk.range.clone()];
    let expected_receipt_counts = chunk.expected_receipt_counts.clone();
    let mut scheduled = 0usize;
    let status = BodyReceiptChunkLiveStatus {
        has_bodies: chunk.bodies.is_some(),
        has_cached_receipts: !chunk.cached_receipts.is_empty(),
        force,
    };
    let should_schedule_body =
        body_receipt_chunk_should_schedule_body(&chunk.state.bodies, &status);
    let should_schedule_receipts =
        body_receipt_chunk_should_schedule_receipts(&chunk.state.receipts, &status);
    if should_schedule_body
        && schedule_body_receipt_plan_body_role(plan, attempts, peer_state, start, chunk, hashes)
    {
        scheduled += 1;
    }
    if attempts.len() < max_role_attempts
        && should_schedule_receipts
        && schedule_body_receipt_plan_receipt_role(
            plan,
            attempts,
            peer_state,
            start,
            chunk,
            hashes,
            expected_receipt_counts.as_deref(),
        )
    {
        scheduled += 1;
    }
    if scheduled > 0 {
        chunk.hedges = chunk.hedges.saturating_add(1);
    }
    scheduled
}

fn schedule_stale_body_receipt_plan_roles<'a>(
    plan: &'a BodyReceiptRequestPlan,
    attempts: &mut futures_util::stream::FuturesUnordered<
        futures_util::future::BoxFuture<'a, BodyReceiptPlanRoleAttempt>,
    >,
    active_chunks: &mut HashMap<usize, PlanLiveBodyReceiptChunk>,
    peer_state: &mut BodyReceiptPlanPeerState,
    completed_chunks: &BTreeMap<usize, Vec<SourcedBodyReceipts>>,
    schedule: BodyReceiptStaleRoleSchedule,
) -> usize {
    let contiguous_blocks = contiguous_chunk_blocks(completed_chunks);
    let Some(start) = active_chunks
        .keys()
        .copied()
        .filter(|start| {
            *start <= contiguous_blocks
                && *start < schedule.min_return_blocks
                && !completed_chunks.contains_key(start)
        })
        .min()
    else {
        return 0;
    };
    let Some(chunk) = active_chunks.get_mut(&start) else {
        return 0;
    };
    if chunk.hedges >= PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK {
        return 0;
    }

    if schedule.prefix_critical {
        let chunk_index = plan
            .range_indices_by_start
            .get(&start)
            .copied()
            .unwrap_or_default()
            .saturating_add((chunk.hedges + 1).saturating_mul(plan.ranges.len().max(1)));
        extend_body_receipt_plan_chunk_candidates(plan, peer_state, chunk_index, chunk);
    }

    schedule_missing_body_receipt_plan_roles(
        plan,
        attempts,
        peer_state,
        start,
        chunk,
        schedule.max_role_attempts,
        schedule.prefix_critical,
    )
}

fn body_receipt_prefix_critical_role_attempt_limit(
    max_role_attempts: usize,
    prefix_critical: bool,
) -> usize {
    if prefix_critical {
        max_role_attempts.saturating_add(PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_EXTRA_ROLE_ATTEMPTS)
    } else {
        max_role_attempts
    }
}

fn body_receipt_has_buffered_suffix_after_prefix<T>(
    completed_chunks: &BTreeMap<usize, Vec<T>>,
    min_return_blocks: usize,
) -> bool {
    let contiguous_blocks = contiguous_chunk_blocks(completed_chunks);
    contiguous_blocks < min_return_blocks
        && completed_chunks
            .keys()
            .any(|start| *start > contiguous_blocks)
}

fn body_receipt_prefix_critical_repair_needed<T>(
    active_chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
    completed_chunks: &BTreeMap<usize, Vec<T>>,
    min_return_blocks: usize,
) -> bool {
    body_receipt_has_buffered_suffix_after_prefix(completed_chunks, min_return_blocks)
        || body_receipt_earliest_missing_prefix_role_needs_repair(
            active_chunks,
            completed_chunks,
            min_return_blocks,
        )
}

fn body_receipt_partial_prefix_flush_blocks(
    return_blocks: usize,
    min_return_blocks: usize,
) -> usize {
    if min_return_blocks == 0 {
        return 0;
    }

    let min_accepted_prefix = body_receipt_min_accepted_prefix(return_blocks);
    min_return_blocks
        .min(PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_MIN_BLOCKS.max(min_accepted_prefix))
}

fn body_receipt_partial_prefix_flush_ready<T>(
    contiguous_blocks: usize,
    flush_blocks: usize,
    min_return_blocks: usize,
    elapsed: Duration,
    active_chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
    completed_chunks: &BTreeMap<usize, Vec<T>>,
) -> bool {
    flush_blocks > 0
        && contiguous_blocks >= flush_blocks
        && contiguous_blocks < min_return_blocks
        && elapsed >= PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_DELAY
        && body_receipt_prefix_critical_repair_needed(
            active_chunks,
            completed_chunks,
            min_return_blocks,
        )
}

fn body_receipt_earliest_missing_prefix_role_needs_repair<T>(
    active_chunks: &HashMap<usize, PlanLiveBodyReceiptChunk>,
    completed_chunks: &BTreeMap<usize, Vec<T>>,
    min_return_blocks: usize,
) -> bool {
    let contiguous_blocks = contiguous_chunk_blocks(completed_chunks);
    let Some(start) = active_chunks
        .keys()
        .copied()
        .filter(|start| {
            *start <= contiguous_blocks
                && *start < min_return_blocks
                && !completed_chunks.contains_key(start)
        })
        .min()
    else {
        return false;
    };
    let Some(chunk) = active_chunks.get(&start) else {
        return false;
    };
    if chunk.completed {
        return false;
    }

    let status = BodyReceiptChunkLiveStatus {
        has_bodies: chunk.bodies.is_some(),
        has_cached_receipts: !chunk.cached_receipts.is_empty(),
        force: false,
    };
    let body_needs_repair = body_receipt_chunk_should_schedule_body(&chunk.state.bodies, &status)
        && body_receipt_role_needs_prefix_critical_repair(
            &chunk.state.bodies,
            chunk.candidates.bodies.len(),
        );
    let receipt_needs_repair =
        body_receipt_chunk_should_schedule_receipts(&chunk.state.receipts, &status)
            && body_receipt_role_needs_prefix_critical_repair(
                &chunk.state.receipts,
                chunk.candidates.receipts.len(),
            );

    body_needs_repair || receipt_needs_repair
}

fn body_receipt_role_needs_prefix_critical_repair(
    state: &BodyReceiptChunkLiveRoleState,
    _candidate_count: usize,
) -> bool {
    state.in_flight > 0 && body_receipt_chunk_role_hedge_due(state.last_scheduled_at, false)
}

fn extend_body_receipt_plan_chunk_candidates(
    plan: &BodyReceiptRequestPlan,
    peer_state: &BodyReceiptPlanPeerState,
    chunk_index: usize,
    chunk: &mut PlanLiveBodyReceiptChunk,
) {
    let (body_ordered, _) = body_receipt_attempt_peer_ids(
        &plan.body_peer_ids,
        &peer_state.body_bad_peers,
        &peer_state.body_in_flight_peers,
        chunk_index,
        PeerRequestKind::Bodies,
        &plan.peers,
    );
    append_body_receipt_plan_candidates(
        &mut chunk.candidates.bodies,
        &chunk.state.bodies.used_peers,
        body_ordered,
        PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_PEERS,
    );

    let (receipt_ordered, _) = body_receipt_attempt_peer_ids(
        &plan.receipt_peer_ids,
        &peer_state.receipt_bad_peers,
        &peer_state.receipt_in_flight_peers,
        chunk_index,
        PeerRequestKind::Receipts,
        &plan.peers,
    );
    append_body_receipt_plan_candidates(
        &mut chunk.candidates.receipts,
        &chunk.state.receipts.used_peers,
        receipt_ordered,
        PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_PEERS,
    );
}

fn append_body_receipt_plan_candidates(
    candidates: &mut Vec<PeerId>,
    used_peers: &HashSet<PeerId>,
    ordered: Vec<PeerId>,
    target_len: usize,
) {
    if candidates.len() >= target_len {
        return;
    }

    let mut seen = candidates.iter().copied().collect::<HashSet<_>>();
    for peer_id in ordered {
        if candidates.len() >= target_len {
            break;
        }
        if used_peers.contains(&peer_id) || !seen.insert(peer_id) {
            continue;
        }
        candidates.push(peer_id);
    }
}

fn body_receipt_attempt_peer_ids(
    peer_ids: &[PeerId],
    bad_peers: &HashSet<PeerId>,
    in_flight_peers: &HashMap<PeerId, usize>,
    chunk_index: usize,
    kind: PeerRequestKind,
    peers: &HashMap<PeerId, RequestPeerSnapshot>,
) -> (Vec<PeerId>, Option<PeerId>) {
    let mut ordered = peer_ids.to_vec();
    if !ordered.is_empty() {
        let rotation = chunk_index % ordered.len();
        ordered.rotate_left(rotation);
    }

    let mut eligible = ordered
        .iter()
        .copied()
        .filter(|peer_id| !bad_peers.contains(peer_id))
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        eligible = ordered;
    }

    eligible.sort_by(|left, right| {
        let left_score = body_receipt_plan_peer_role_score(
            peers.get(left).map(|peer| &peer.metrics),
            kind,
            in_flight_peers.get(left).copied().unwrap_or_default(),
        );
        let right_score = body_receipt_plan_peer_role_score(
            peers.get(right).map(|peer| &peer.metrics),
            kind,
            in_flight_peers.get(right).copied().unwrap_or_default(),
        );
        right_score
            .partial_cmp(&left_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let primary_peer = eligible.first().copied();
    (eligible, primary_peer)
}

fn body_receipt_plan_peer_role_score(
    metrics: Option<&RequestPeerRoleMetrics>,
    kind: PeerRequestKind,
    local_in_flight: usize,
) -> f64 {
    let Some(metrics) = metrics else {
        return body_receipt_load_adjusted_peer_rate(1.0, local_in_flight);
    };
    let measured_rate = body_receipt_peer_role_rate(metrics, kind);
    let base_rate = if measured_rate > 0.0 {
        measured_rate
    } else {
        1.0
    };
    let active_load = body_receipt_peer_role_load(metrics, kind).saturating_add(local_in_flight);
    let serving_bonus = if metrics.is_serving { 4.0 } else { 0.0 };
    let timeout_penalty = f64::from(metrics.consecutive_timeouts) * 8.0;

    body_receipt_load_adjusted_peer_rate(base_rate + serving_bonus, active_load) - timeout_penalty
}

fn body_receipt_load_adjusted_peer_rate(base_rate: f64, active_requests: usize) -> f64 {
    base_rate / (1.0 + active_requests as f64)
}

fn body_receipt_role_request_timeout(
    metrics: Option<&RequestPeerRoleMetrics>,
    kind: PeerRequestKind,
    requested: usize,
) -> Duration {
    let Some(metrics) = metrics else {
        return PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT;
    };
    if requested == 0 {
        return PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT;
    }

    let measured_rate = body_receipt_peer_role_rate(metrics, kind);
    if measured_rate <= 0.0 {
        return PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT;
    }

    let active_load = body_receipt_peer_role_load(metrics, kind);
    let effective_rate = body_receipt_load_adjusted_peer_rate(measured_rate, active_load).max(0.1);
    let expected_secs = requested as f64 / effective_rate;
    if expected_secs > PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT.as_secs_f64() {
        return PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT;
    }

    let adaptive_timeout = Duration::from_secs_f64(
        expected_secs * PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT_SAFETY_FACTOR,
    );

    adaptive_timeout
        .max(PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT)
        .min(PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT_MAX)
}

fn body_receipt_peer_role_rate(metrics: &RequestPeerRoleMetrics, kind: PeerRequestKind) -> f64 {
    match kind {
        PeerRequestKind::Headers => 0.0,
        PeerRequestKind::Bodies => metrics.body_blocks_per_sec,
        PeerRequestKind::Receipts => metrics.receipt_blocks_per_sec,
    }
}

fn body_receipt_peer_role_load(metrics: &RequestPeerRoleMetrics, kind: PeerRequestKind) -> usize {
    match kind {
        PeerRequestKind::Headers => 0,
        PeerRequestKind::Bodies => metrics
            .body_active_requests
            .saturating_add(metrics.body_reserved_requests),
        PeerRequestKind::Receipts => metrics
            .receipt_active_requests
            .saturating_add(metrics.receipt_reserved_requests),
    }
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

impl ReverseHeaderPagesRequestPlan {
    pub(crate) async fn execute(self) -> ReverseHeaderPagesRequestOutcome {
        let mut attempts = futures_util::stream::FuturesUnordered::new();
        for page in self.pages {
            attempts.push(
                request_header_page_from_candidates(page.page_index, page.request, page.candidates)
                    .boxed(),
            );
        }

        let mut page_results = Vec::new();
        while let Some(result) = attempts.next().await {
            page_results.push(result);
        }

        ReverseHeaderPagesRequestOutcome { page_results }
    }
}

fn reverse_header_page_requests(
    child_block: u64,
    total_count: u64,
    page_limit: u64,
) -> Vec<(usize, HeadersRequest)> {
    let mut requests = Vec::new();
    if child_block == 0 || total_count == 0 || page_limit == 0 {
        return requests;
    }

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
        requests.push((
            page_index,
            HeadersRequest::falling(BlockHashOrNumber::Number(start_block), request_count),
        ));
        offset = offset.saturating_add(request_count);
        page_index = page_index.saturating_add(1);
    }
    requests
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
    let mut candidates = candidates.into_iter();

    loop {
        let mut attempts = futures_util::stream::FuturesUnordered::new();
        for (peer_id, sender) in candidates
            .by_ref()
            .take(REVERSE_HEADER_PAGE_PARALLEL_CANDIDATES)
        {
            attempts.push({
                let request = request.clone();
                async move {
                    let started_at = Instant::now();
                    let result = request_headers_with_sender(sender, request).await;
                    (peer_id, started_at.elapsed(), result)
                }
                .boxed()
            });
        }

        if attempts.is_empty() {
            break;
        }

        while let Some((peer_id, elapsed, result)) = attempts.next().await {
            match result {
                Ok(headers)
                    if headers.len() <= requested as usize
                        && (requested == 0 || !headers.is_empty()) =>
                {
                    return HeaderPageResult {
                        page_index,
                        requested,
                        success: Some((peer_id, headers, elapsed)),
                        failures,
                    };
                }
                Ok(_) => failures.push((
                    peer_id,
                    RequestAttempt::Request(reth_network::p2p::error::RequestError::BadResponse),
                )),
                Err(error) => failures.push((peer_id, error)),
            }
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

fn body_receipt_missing_prefix_reassign_candidate<T>(
    ranges: &[std::ops::Range<usize>],
    range_indices_by_start: &HashMap<usize, usize>,
    retry_counts: &mut HashMap<usize, usize>,
    in_flight: &HashSet<usize>,
    chunks: &BTreeMap<usize, Vec<T>>,
    min_return_blocks: usize,
) -> Option<(std::ops::Range<usize>, usize)> {
    let contiguous_blocks = contiguous_chunk_blocks(chunks);
    let range = ranges
        .iter()
        .find(|range| {
            range.start <= contiguous_blocks
                && range.start < min_return_blocks
                && !chunks.contains_key(&range.start)
                && !in_flight.contains(&range.start)
        })
        .and_then(|range| body_receipt_prefix_range(range.clone(), min_return_blocks))?;
    let retry_count = retry_counts.entry(range.start).or_default();
    if *retry_count >= PIPELINED_BODY_RECEIPT_PREFIX_REASSIGN_ROUNDS {
        return None;
    }

    *retry_count += 1;
    let base_chunk_index = range_indices_by_start
        .get(&range.start)
        .copied()
        .unwrap_or_default();
    Some((
        range,
        base_chunk_index + (*retry_count * ranges.len().max(1)),
    ))
}

fn body_receipt_missing_prefix_salvage_range<T>(
    ranges: &[std::ops::Range<usize>],
    chunks: &BTreeMap<usize, Vec<T>>,
    min_return_blocks: usize,
) -> Option<std::ops::Range<usize>> {
    if !body_receipt_has_buffered_suffix_after_prefix(chunks, min_return_blocks) {
        return None;
    }

    let contiguous_blocks = contiguous_chunk_blocks(chunks);
    ranges
        .iter()
        .find(|range| {
            range.start <= contiguous_blocks
                && range.start < min_return_blocks
                && !chunks.contains_key(&range.start)
        })
        .and_then(|range| body_receipt_prefix_range(range.clone(), min_return_blocks))
}

fn body_receipt_chunk_should_schedule_body(
    state: &BodyReceiptChunkLiveRoleState,
    status: &BodyReceiptChunkLiveStatus,
) -> bool {
    !status.has_bodies
        && (state.in_flight == 0
            || body_receipt_chunk_role_hedge_due(state.last_scheduled_at, status.force))
}

fn body_receipt_chunk_should_schedule_receipts(
    state: &BodyReceiptChunkLiveRoleState,
    status: &BodyReceiptChunkLiveStatus,
) -> bool {
    (status.has_bodies || (state.in_flight == 0 && !status.has_cached_receipts))
        && (state.in_flight == 0
            || body_receipt_chunk_role_hedge_due(state.last_scheduled_at, status.force))
}

fn body_receipt_chunk_role_hedge_due(last_scheduled_at: Option<Instant>, force: bool) -> bool {
    force
        || last_scheduled_at.is_some_and(|scheduled_at| {
            scheduled_at.elapsed() >= PIPELINED_BODY_RECEIPT_HEDGE_DELAY
        })
}

fn rotated_chunk_index(chunk_index: usize, peer_rotation: usize) -> usize {
    chunk_index.wrapping_add(peer_rotation)
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

struct BodyReceiptCompletionChunks<T> {
    blocks: Vec<T>,
    planned_return_blocks: usize,
    residual_chunks: BTreeMap<usize, Vec<T>>,
    min_accepted_prefix: usize,
}

fn body_receipt_completion_chunks<T>(
    completion_return_blocks: usize,
    chunks: BTreeMap<usize, Vec<T>>,
    min_accepted_prefix_override: Option<usize>,
) -> BodyReceiptCompletionChunks<T> {
    let preserve_residual_chunks = min_accepted_prefix_override.is_some();
    let (blocks, residual_chunks) = split_contiguous_prefix(completion_return_blocks, chunks);
    let min_accepted_prefix = body_receipt_completion_min_accepted_prefix(
        completion_return_blocks,
        min_accepted_prefix_override,
        !residual_chunks.is_empty(),
    );
    let planned_return_blocks = if preserve_residual_chunks {
        completion_return_blocks
    } else {
        blocks.len()
    };
    let residual_chunks = if preserve_residual_chunks {
        residual_chunks
    } else {
        BTreeMap::new()
    };

    BodyReceiptCompletionChunks {
        blocks,
        planned_return_blocks,
        residual_chunks,
        min_accepted_prefix,
    }
}

fn split_contiguous_prefix<T>(
    return_blocks: usize,
    chunks: BTreeMap<usize, Vec<T>>,
) -> (Vec<T>, BTreeMap<usize, Vec<T>>) {
    let contiguous_blocks = contiguous_chunk_blocks(&chunks);
    let target_blocks = contiguous_blocks.min(return_blocks);
    let mut blocks = Vec::with_capacity(target_blocks);
    let mut residual_chunks = BTreeMap::new();
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
        } else if start >= target_blocks && start < return_blocks {
            let mut chunk_blocks = chunk_blocks;
            let max_len = return_blocks - start;
            if chunk_blocks.len() > max_len {
                chunk_blocks.truncate(max_len);
            }
            residual_chunks.insert(start - target_blocks, chunk_blocks);
        }
    }

    (blocks, residual_chunks)
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

fn body_receipt_role_request_window_remaining(
    peers: &HashMap<PeerId, ActivePeer>,
    peer_ids: &[PeerId],
    kind: PeerRequestKind,
    max_in_flight: usize,
    minimum_in_flight: usize,
) -> usize {
    let active_requests = peer_ids
        .iter()
        .filter_map(|peer_id| peers.get(peer_id))
        .map(|peer| body_receipt_active_requests(peer, kind))
        .sum::<usize>();

    body_receipt_role_request_window_remaining_for_load(
        peer_ids.len(),
        active_requests,
        max_in_flight,
        minimum_in_flight,
    )
}

fn body_receipt_role_request_window_remaining_for_load(
    peer_count: usize,
    active_requests: usize,
    max_in_flight: usize,
    minimum_in_flight: usize,
) -> usize {
    let window = request_window_limit(peer_count, max_in_flight);
    if window == 0 {
        return 0;
    }

    window
        .saturating_sub(active_requests)
        .max(minimum_in_flight.min(window))
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
    retain_preferred_items_with_limited_fallbacks_if_enough(
        peer_ids,
        PIPELINED_BODY_RECEIPT_IDLE_POOL_MIN_PEERS,
        PIPELINED_BODY_RECEIPT_IDLE_POOL_PROBE_PEERS,
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
    let mut serving_families = HashSet::new();
    for peer_id in peer_ids.iter() {
        let Some(peer) = peers.get(peer_id) else {
            continue;
        };
        if peer.is_serving {
            serving_families.insert(execution_client_family(&peer.client_version));
        }
    }

    retain_preferred_items_with_limited_fallbacks_and_required_probes_if_enough(
        peer_ids,
        PIPELINED_BODY_RECEIPT_SERVING_POOL_MIN_PEERS,
        PIPELINED_BODY_RECEIPT_SERVING_POOL_PROBE_PEERS,
        |peer_id| peers.get(peer_id).is_some_and(|peer| peer.is_serving),
        |peer_id| {
            let peer = peers.get(peer_id)?;
            let family = execution_client_family(&peer.client_version);
            (!peer.is_serving && !serving_families.contains(&family)).then_some(family)
        },
    );
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

fn retain_preferred_items_with_limited_fallbacks_and_required_probes_if_enough<T, K>(
    items: &mut Vec<T>,
    min_preferred: usize,
    fallback_limit: usize,
    is_preferred: impl Fn(&T) -> bool,
    required_probe_key: impl Fn(&T) -> Option<K>,
) where
    K: Eq + std::hash::Hash,
{
    let preferred = items.iter().filter(|item| is_preferred(*item)).count();
    if preferred < min_preferred {
        return;
    }

    let mut required_probe_indexes = HashSet::new();
    let mut seen_probe_keys = HashSet::new();
    for (index, item) in items.iter().enumerate() {
        if is_preferred(item) {
            continue;
        }
        if let Some(key) = required_probe_key(item)
            && seen_probe_keys.insert(key)
        {
            required_probe_indexes.insert(index);
        }
    }

    let mut fallback_count = 0usize;
    let mut index = 0usize;
    items.retain(|item| {
        let current_index = index;
        index = index.saturating_add(1);
        if is_preferred(item) || required_probe_indexes.contains(&current_index) {
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
        PeerRequestKind::Bodies => peer
            .body_active_requests
            .saturating_add(peer.body_reserved_requests),
        PeerRequestKind::Receipts => peer
            .receipt_active_requests
            .saturating_add(peer.receipt_reserved_requests),
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

fn body_receipt_background_range(
    mut range: std::ops::Range<usize>,
    min_return_blocks: usize,
    return_blocks: usize,
) -> Option<std::ops::Range<usize>> {
    if range.end <= min_return_blocks || range.start >= return_blocks {
        return None;
    }
    range.start = range.start.max(min_return_blocks);
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

fn body_receipt_completed_plan_return_blocks<T>(
    chunks: &BTreeMap<usize, Vec<T>>,
    return_blocks: usize,
) -> usize {
    contiguous_chunk_blocks(chunks).min(return_blocks)
}

fn body_receipt_min_accepted_prefix_override(return_blocks: usize, prefix: usize) -> usize {
    return_blocks.min(prefix)
}

fn body_receipt_completion_min_accepted_prefix(
    return_blocks: usize,
    min_accepted_prefix_override: Option<usize>,
    has_residual_chunks: bool,
) -> usize {
    if let Some(prefix) = min_accepted_prefix_override {
        return body_receipt_min_accepted_prefix_override(return_blocks, prefix);
    }

    let default_prefix = body_receipt_min_accepted_prefix(return_blocks);
    if has_residual_chunks {
        default_prefix.min(body_receipt_min_accepted_prefix_override(
            return_blocks,
            PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS,
        ))
    } else {
        default_prefix
    }
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

fn body_receipt_priority_prefix_chunk_limit(
    priority: BodyReceiptRequestPriority,
    prefix_chunk_limit: usize,
) -> usize {
    match priority {
        BodyReceiptRequestPriority::Full => prefix_chunk_limit,
        BodyReceiptRequestPriority::Lookahead if prefix_chunk_limit == 0 => 0,
        BodyReceiptRequestPriority::Lookahead => prefix_chunk_limit
            .min(PIPELINED_BODY_RECEIPT_LOOKAHEAD_PREFIX_CHUNK_LIMIT)
            .max(1),
    }
}

fn body_receipt_background_chunk_limit(
    ranges: &[std::ops::Range<usize>],
    min_return_blocks: usize,
    return_blocks: usize,
    prefix_chunk_limit: usize,
    role_window: usize,
) -> usize {
    if ranges.is_empty()
        || min_return_blocks == 0
        || return_blocks <= min_return_blocks
        || prefix_chunk_limit == 0
        || role_window <= prefix_chunk_limit
    {
        return 0;
    }

    let background_chunks = ranges
        .iter()
        .filter(|range| {
            body_receipt_background_range((*range).clone(), min_return_blocks, return_blocks)
                .is_some()
        })
        .count();
    role_window
        .saturating_sub(prefix_chunk_limit)
        .min(prefix_chunk_limit)
        .min(background_chunks)
}

fn body_receipt_prefix_hedge_spare_attempts(min_return_blocks: usize, peer_count: usize) -> usize {
    if min_return_blocks == 0
        || min_return_blocks > PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS
        || peer_count < PIPELINED_BODY_RECEIPT_LOW_PEER_PREFIX_REDUNDANCY_MIN_PEERS
    {
        return 0;
    }

    PIPELINED_BODY_RECEIPT_PREFIX_HEDGE_SPARE_ATTEMPTS
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

        let (ordered, primary) = body_receipt_attempt_peer_ids(
            &peers,
            &bad_peers,
            &in_flight,
            0,
            PeerRequestKind::Bodies,
            &HashMap::new(),
        );

        assert_eq!(primary, Some(second));
        assert_eq!(ordered, vec![second, first, fourth]);
    }

    #[test]
    fn body_receipt_attempt_peers_preserve_performance_order_among_equal_loads() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let peers = vec![first, second, third];

        let (ordered, primary) = body_receipt_attempt_peer_ids(
            &peers,
            &HashSet::new(),
            &HashMap::new(),
            0,
            PeerRequestKind::Bodies,
            &HashMap::new(),
        );

        assert_eq!(primary, Some(first));
        assert_eq!(ordered, vec![first, second, third]);
    }

    #[test]
    fn body_receipt_attempt_peers_rotate_equal_score_ties_by_chunk() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let peers = vec![first, second, third];

        let (ordered, primary) = body_receipt_attempt_peer_ids(
            &peers,
            &HashSet::new(),
            &HashMap::new(),
            1,
            PeerRequestKind::Bodies,
            &HashMap::new(),
        );

        assert_eq!(primary, Some(second));
        assert_eq!(ordered, vec![second, third, first]);
    }

    #[test]
    fn body_receipt_role_candidate_skips_used_and_bad_peers() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let candidates = vec![first, second, third];
        let mut state = BodyReceiptChunkLiveRoleState {
            used_peers: HashSet::from([first]),
            ..Default::default()
        };
        let bad_peers = HashSet::from([second]);

        let selected = next_body_receipt_role_candidate(&candidates, &mut state, &bad_peers);

        assert_eq!(selected, Some(third));
        assert_eq!(state.next_index, candidates.len());
        assert!(state.used_peers.contains(&third));
        assert!(!state.used_peers.contains(&second));
    }

    #[test]
    fn body_receipt_role_candidate_returns_none_after_bad_peers_exhaust_candidates() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let candidates = vec![first, second];
        let mut state = BodyReceiptChunkLiveRoleState::default();
        let bad_peers = HashSet::from([first, second]);

        let selected = next_body_receipt_role_candidate(&candidates, &mut state, &bad_peers);

        assert_eq!(selected, None);
        assert_eq!(state.next_index, candidates.len());
        assert!(state.used_peers.is_empty());
    }

    #[test]
    fn body_receipt_plan_peer_score_uses_role_rate_and_load() {
        let fast_loaded = RequestPeerRoleMetrics {
            is_serving: true,
            body_blocks_per_sec: 120.0,
            body_active_requests: 3,
            ..Default::default()
        };
        let slow_idle = RequestPeerRoleMetrics {
            is_serving: true,
            body_blocks_per_sec: 20.0,
            ..Default::default()
        };

        assert!(
            body_receipt_plan_peer_role_score(Some(&fast_loaded), PeerRequestKind::Bodies, 0)
                > body_receipt_plan_peer_role_score(Some(&slow_idle), PeerRequestKind::Bodies, 0)
        );
    }

    #[test]
    fn body_receipt_plan_peer_score_is_role_specific() {
        let peer = RequestPeerRoleMetrics {
            is_serving: true,
            body_blocks_per_sec: 10.0,
            receipt_blocks_per_sec: 120.0,
            ..Default::default()
        };

        assert!(
            body_receipt_plan_peer_role_score(Some(&peer), PeerRequestKind::Receipts, 0)
                > body_receipt_plan_peer_role_score(Some(&peer), PeerRequestKind::Bodies, 0)
        );
    }

    #[test]
    fn body_receipt_plan_peer_score_load_adjusts_serving_bonus() {
        let overloaded_serving = RequestPeerRoleMetrics {
            is_serving: true,
            body_active_requests: 4,
            body_reserved_requests: 4,
            ..Default::default()
        };
        let idle_unproven = RequestPeerRoleMetrics::default();

        assert!(
            body_receipt_plan_peer_role_score(Some(&idle_unproven), PeerRequestKind::Bodies, 0)
                > body_receipt_plan_peer_role_score(
                    Some(&overloaded_serving),
                    PeerRequestKind::Bodies,
                    0
                )
        );
    }

    #[test]
    fn body_receipt_role_request_timeout_uses_bounded_peer_rate() {
        let unproven = RequestPeerRoleMetrics::default();
        assert_eq!(
            body_receipt_role_request_timeout(Some(&unproven), PeerRequestKind::Receipts, 128),
            PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT
        );

        let fast = RequestPeerRoleMetrics {
            receipt_blocks_per_sec: 1_000.0,
            ..Default::default()
        };
        assert_eq!(
            body_receipt_role_request_timeout(Some(&fast), PeerRequestKind::Receipts, 128),
            PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT
        );

        let medium = RequestPeerRoleMetrics {
            receipt_blocks_per_sec: 40.0,
            ..Default::default()
        };
        let timeout =
            body_receipt_role_request_timeout(Some(&medium), PeerRequestKind::Receipts, 128);
        assert!(timeout > PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT);
        assert!(timeout < PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT_MAX);

        let slow_loaded = RequestPeerRoleMetrics {
            receipt_blocks_per_sec: 16.0,
            receipt_active_requests: 1,
            ..Default::default()
        };
        assert_eq!(
            body_receipt_role_request_timeout(Some(&slow_loaded), PeerRequestKind::Receipts, 128),
            PIPELINED_BODY_RECEIPT_REQUEST_TIMEOUT
        );
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
    fn body_receipt_role_request_window_subtracts_active_load() {
        assert_eq!(
            body_receipt_role_request_window_remaining_for_load(8, 0, 64, 2),
            8
        );
        assert_eq!(
            body_receipt_role_request_window_remaining_for_load(8, 3, 64, 2),
            5
        );
        assert_eq!(
            body_receipt_role_request_window_remaining_for_load(8, 7, 64, 2),
            2
        );
        assert_eq!(
            body_receipt_role_request_window_remaining_for_load(8, 99, 64, 2),
            2
        );
        assert_eq!(
            body_receipt_role_request_window_remaining_for_load(8, 99, 64, 0),
            0
        );
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
    fn preferred_item_retention_keeps_bounded_probe_fallbacks() {
        let mut items = vec![1, 2, 3, 4, 5, 6, 7];

        retain_preferred_items_with_limited_fallbacks_if_enough(&mut items, 3, 2, |item| {
            item % 2 == 1
        });

        assert_eq!(items, vec![1, 2, 3, 4, 5, 7]);
    }

    #[test]
    fn preferred_item_retention_keeps_required_probe_families() {
        let mut items = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];

        retain_preferred_items_with_limited_fallbacks_and_required_probes_if_enough(
            &mut items,
            3,
            2,
            |item| *item <= 3,
            |item| match *item {
                7 | 8 => Some("nethermind"),
                9 => Some("reth"),
                _ => None,
            },
        );

        assert_eq!(items, vec![1, 2, 3, 4, 5, 7, 9]);
    }

    #[test]
    fn preferred_item_retention_waits_for_minimum_preferred_count() {
        let mut items = vec![1, 2, 3, 4, 5];

        retain_preferred_items_with_limited_fallbacks_and_required_probes_if_enough(
            &mut items,
            4,
            1,
            |item| *item <= 3,
            |item| (*item == 5).then_some("nethermind"),
        );

        assert_eq!(items, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn body_receipt_chunk_limit_caps_large_adaptive_limits() {
        assert_eq!(body_receipt_chunk_cap(31, None), 128);
        assert_eq!(body_receipt_chunk_cap(32, Some(50.0)), 128);
        assert_eq!(body_receipt_chunk_cap(15, Some(250.0)), 128);
        assert_eq!(body_receipt_chunk_cap(32, Some(100.0)), 128);
        assert_eq!(body_receipt_chunk_cap(32, Some(250.0)), 128);
        assert_eq!(body_receipt_chunk_cap(32, Some(300.0)), 48);
        assert_eq!(body_receipt_chunk_cap(32, Some(1500.0)), 32);
        assert_eq!(body_receipt_chunk_limit(128, 128, 128), 128);
        assert_eq!(body_receipt_chunk_limit(16, 128, 128), 16);
        assert_eq!(body_receipt_chunk_limit(128, 8, 128), 8);
    }

    #[test]
    fn reverse_header_page_requests_split_descending_pages() {
        let requests = reverse_header_page_requests(1_001, 250, 100);
        let pages = requests
            .iter()
            .map(|(index, request)| {
                let BlockHashOrNumber::Number(start) = request.start else {
                    panic!("reverse page request should use numeric starts");
                };
                (*index, start, request.limit)
            })
            .collect::<Vec<_>>();

        assert_eq!(pages, vec![(0, 1_000, 100), (1, 900, 100), (2, 800, 50)]);
        assert!(reverse_header_page_requests(0, 250, 100).is_empty());
        assert!(reverse_header_page_requests(1_001, 0, 100).is_empty());
        assert!(reverse_header_page_requests(1_001, 250, 0).is_empty());
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
            body_receipt_return_blocks(4096, Some(&dense), Some(120.0)),
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
    fn body_receipt_completed_plan_return_blocks_tracks_contiguous_progress() {
        let mut chunks = BTreeMap::new();
        assert_eq!(body_receipt_completed_plan_return_blocks(&chunks, 2048), 0);

        chunks.insert(0, vec![0u8; 512]);
        assert_eq!(
            body_receipt_completed_plan_return_blocks(&chunks, 2048),
            512
        );

        chunks.insert(512, vec![0u8; 256]);
        assert_eq!(
            body_receipt_completed_plan_return_blocks(&chunks, 2048),
            768
        );

        chunks.insert(1536, vec![0u8; 1024]);
        assert_eq!(
            body_receipt_completed_plan_return_blocks(&chunks, 2048),
            768
        );
    }

    #[test]
    fn body_receipt_min_accepted_prefix_accepts_dense_chunk_progress() {
        assert_eq!(body_receipt_min_accepted_prefix(32), 32);
        assert_eq!(body_receipt_min_accepted_prefix(64), 32);
        assert_eq!(body_receipt_min_accepted_prefix(128), 32);
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
    fn body_receipt_partial_prefix_flush_target_keeps_progress_substantial() {
        assert_eq!(body_receipt_partial_prefix_flush_blocks(32, 32), 32);
        assert_eq!(body_receipt_partial_prefix_flush_blocks(512, 512), 256);
        assert_eq!(body_receipt_partial_prefix_flush_blocks(10_000, 512), 256);
        assert_eq!(body_receipt_partial_prefix_flush_blocks(0, 0), 0);
    }

    #[test]
    fn body_receipt_partial_prefix_flush_requires_delay_and_prefix_pressure() {
        let active_chunks = HashMap::new();
        let mut chunks = BTreeMap::from([(0usize, vec![1u8; 256])]);
        assert!(!body_receipt_partial_prefix_flush_ready(
            256,
            256,
            512,
            PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_DELAY + Duration::from_millis(1),
            &active_chunks,
            &chunks,
        ));

        chunks.insert(384, vec![1u8; 32]);
        assert!(!body_receipt_partial_prefix_flush_ready(
            256,
            256,
            512,
            PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_DELAY - Duration::from_millis(1),
            &active_chunks,
            &chunks,
        ));
        assert!(!body_receipt_partial_prefix_flush_ready(
            255,
            256,
            512,
            PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_DELAY + Duration::from_millis(1),
            &active_chunks,
            &chunks,
        ));
        assert!(body_receipt_partial_prefix_flush_ready(
            256,
            256,
            512,
            PIPELINED_BODY_RECEIPT_PARTIAL_PREFIX_FLUSH_DELAY + Duration::from_millis(1),
            &active_chunks,
            &chunks,
        ));
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
    fn body_receipt_plan_max_return_blocks_caps_speculative_prefix() {
        let ranges = vec![0..300, 300..690, 690..1_100, 1_100..1_500];
        let plan = BodyReceiptRequestPlan {
            hashes: vec![B256::ZERO; 1_500],
            range_indices_by_start: ranges
                .iter()
                .enumerate()
                .map(|(index, range)| (range.start, index))
                .collect(),
            ranges,
            return_blocks: 1_500,
            body_peer_ids: Vec::new(),
            receipt_peer_ids: Vec::new(),
            max_in_flight: 0,
            body_max_in_flight: 0,
            receipt_max_in_flight: 0,
            peer_rotation: 0,
            priority: BodyReceiptRequestPriority::Full,
            peers: HashMap::new(),
            accounting_tx: None,
        };

        let capped = plan.with_max_return_blocks(512);

        assert_eq!(capped.planned_prefix_blocks(), 512);
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
    fn body_receipt_completion_prefix_uses_residual_floor_only_with_buffered_suffix() {
        assert_eq!(
            body_receipt_completion_min_accepted_prefix(512, None, false),
            body_receipt_min_accepted_prefix(512)
        );
        assert_eq!(
            body_receipt_completion_min_accepted_prefix(512, None, true),
            PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS
        );
        assert_eq!(
            body_receipt_completion_min_accepted_prefix(8, None, true),
            8
        );
        assert_eq!(
            body_receipt_completion_min_accepted_prefix(512, Some(24), true),
            24
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
    fn body_receipt_priority_prefix_limit_caps_only_lookahead() {
        assert_eq!(
            body_receipt_priority_prefix_chunk_limit(BodyReceiptRequestPriority::Full, 20),
            20
        );
        assert_eq!(
            body_receipt_priority_prefix_chunk_limit(BodyReceiptRequestPriority::Lookahead, 20),
            PIPELINED_BODY_RECEIPT_LOOKAHEAD_PREFIX_CHUNK_LIMIT
        );
        assert_eq!(
            body_receipt_priority_prefix_chunk_limit(BodyReceiptRequestPriority::Lookahead, 4),
            4
        );
        assert_eq!(
            body_receipt_priority_prefix_chunk_limit(BodyReceiptRequestPriority::Lookahead, 0),
            0
        );
    }

    #[test]
    fn body_receipt_background_range_slices_after_prefix() {
        assert_eq!(body_receipt_background_range(0..64, 32, 96), Some(32..64));
        assert_eq!(body_receipt_background_range(0..32, 32, 96), None);
        assert_eq!(body_receipt_background_range(64..128, 32, 96), Some(64..96));
        assert_eq!(body_receipt_background_range(128..160, 32, 96), None);
    }

    #[test]
    fn body_receipt_background_chunk_limit_uses_spare_role_capacity() {
        let ranges = vec![0..32, 32..64, 64..96, 96..128];

        assert_eq!(
            body_receipt_background_chunk_limit(&ranges, 64, 128, 2, 2),
            0
        );
        assert_eq!(
            body_receipt_background_chunk_limit(&ranges, 64, 128, 2, 3),
            1
        );
        assert_eq!(
            body_receipt_background_chunk_limit(&ranges, 64, 128, 2, 8),
            2
        );
        assert_eq!(
            body_receipt_background_chunk_limit(&ranges, 64, 64, 2, 8),
            0
        );
    }

    #[test]
    fn body_receipt_prefix_hedges_get_spare_capacity_for_dense_peer_sets() {
        assert_eq!(
            body_receipt_prefix_hedge_spare_attempts(
                512,
                PIPELINED_BODY_RECEIPT_LOW_PEER_PREFIX_REDUNDANCY_MIN_PEERS
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
    fn body_receipt_live_plan_role_attempt_limit_keeps_bounded_hedge_room() {
        assert_eq!(body_receipt_plan_live_role_attempt_limit(0, 0), 0);
        assert_eq!(body_receipt_plan_live_role_attempt_limit(4, 4), 12);
        assert_eq!(body_receipt_plan_live_role_attempt_limit(6, 10), 22);
    }

    #[test]
    fn body_receipt_live_plan_counts_only_incomplete_chunks_as_active() {
        let mut chunks = HashMap::new();
        chunks.insert(
            0,
            PlanLiveBodyReceiptChunk {
                range: 0..32,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: Vec::new(),
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState::default(),
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: true,
                hedges: 0,
            },
        );
        chunks.insert(
            32,
            PlanLiveBodyReceiptChunk {
                range: 32..64,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: Vec::new(),
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState {
                    bodies: BodyReceiptChunkLiveRoleState {
                        in_flight: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: 2,
            },
        );

        assert_eq!(body_receipt_plan_live_active_chunk_count(&chunks), 1);
        let in_flight = body_receipt_plan_live_in_flight_chunks(&chunks);
        assert!(!in_flight.contains(&0));
        assert!(in_flight.contains(&32));
    }

    #[test]
    fn body_receipt_live_plan_counts_prefix_and_background_lanes() {
        let mut chunks = HashMap::new();
        chunks.insert(
            0,
            PlanLiveBodyReceiptChunk {
                range: 0..32,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: Vec::new(),
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState::default(),
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: 0,
            },
        );
        chunks.insert(
            64,
            PlanLiveBodyReceiptChunk {
                range: 64..96,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: Vec::new(),
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState::default(),
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: 0,
            },
        );
        chunks.insert(
            96,
            PlanLiveBodyReceiptChunk {
                range: 96..128,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: Vec::new(),
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState::default(),
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: true,
                hedges: 0,
            },
        );

        assert_eq!(
            body_receipt_plan_live_active_prefix_chunk_count(&chunks, 64),
            1
        );
        assert_eq!(
            body_receipt_plan_live_active_background_chunk_count(&chunks, 64),
            1
        );
        assert_eq!(
            body_receipt_plan_live_in_flight_background_chunk_count(&chunks, 64),
            0
        );
        chunks.get_mut(&64).unwrap().state.receipts.in_flight = 1;
        assert_eq!(
            body_receipt_plan_live_in_flight_background_chunk_count(&chunks, 64),
            1
        );
        assert_eq!(body_receipt_plan_live_active_chunk_count(&chunks), 2);
    }

    #[test]
    fn body_receipt_exhausted_prefix_chunk_can_be_reassigned() {
        let mut active_chunks = HashMap::new();
        active_chunks.insert(
            0,
            PlanLiveBodyReceiptChunk {
                range: 0..32,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: Vec::new(),
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState::default(),
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: PIPELINED_BODY_RECEIPT_MAX_HEDGES_PER_CHUNK,
            },
        );
        active_chunks.insert(
            32,
            PlanLiveBodyReceiptChunk {
                range: 32..64,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: Vec::new(),
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState::default(),
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: true,
                hedges: 0,
            },
        );
        let completed_chunks = BTreeMap::from([(32usize, vec![1u8; 32])]);

        assert_eq!(
            body_receipt_remove_exhausted_prefix_chunks(&mut active_chunks, &completed_chunks, 64),
            1
        );
        assert!(!active_chunks.contains_key(&0));
        assert!(active_chunks.contains_key(&32));
    }

    #[test]
    fn body_receipt_prefix_chunk_with_remaining_candidate_is_not_reassigned() {
        let mut active_chunks = HashMap::new();
        active_chunks.insert(
            0,
            PlanLiveBodyReceiptChunk {
                range: 0..32,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: vec![PeerId::repeat_byte(1)],
                    receipts: Vec::new(),
                },
                state: BodyReceiptChunkLiveState::default(),
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: 0,
            },
        );

        assert_eq!(
            body_receipt_remove_exhausted_prefix_chunks(
                &mut active_chunks,
                &BTreeMap::<usize, Vec<u8>>::new(),
                32,
            ),
            0
        );
        assert!(active_chunks.contains_key(&0));
    }

    #[test]
    fn body_receipt_paired_plan_reservations_cover_initial_role_wave() {
        let body_peers = (0..PIPELINED_BODY_RECEIPT_LOW_PEER_PREFIX_REDUNDANCY_MIN_PEERS)
            .map(|index| PeerId::repeat_byte((index + 1) as u8))
            .collect::<Vec<_>>();
        let receipt_peers = (0..PIPELINED_BODY_RECEIPT_LOW_PEER_PREFIX_REDUNDANCY_MIN_PEERS)
            .map(|index| PeerId::repeat_byte((index + 21) as u8))
            .collect::<Vec<_>>();
        let ranges = vec![0..64, 64..128, 128..192, 192..256];
        let plan = BodyReceiptRequestPlan {
            hashes: vec![B256::ZERO; 256],
            range_indices_by_start: ranges
                .iter()
                .enumerate()
                .map(|(index, range)| (range.start, index))
                .collect(),
            ranges,
            return_blocks: 256,
            body_peer_ids: body_peers,
            receipt_peer_ids: receipt_peers,
            max_in_flight: 2,
            body_max_in_flight: 2,
            receipt_max_in_flight: 2,
            peer_rotation: 0,
            priority: BodyReceiptRequestPriority::Full,
            peers: HashMap::new(),
            accounting_tx: None,
        };

        let reservations = plan.reservations();
        let body_reservations = reservations
            .entries()
            .iter()
            .filter(|entry| matches!(entry.kind, PeerRequestKind::Bodies))
            .map(|entry| entry.count)
            .sum::<usize>();
        let receipt_reservations = reservations
            .entries()
            .iter()
            .filter(|entry| matches!(entry.kind, PeerRequestKind::Receipts))
            .map(|entry| entry.count)
            .sum::<usize>();

        assert_eq!(body_reservations, 2);
        assert_eq!(receipt_reservations, 2);
    }

    #[test]
    fn body_receipt_dense_plan_uses_live_scheduler_reservations() {
        let body_peers = (0..PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS)
            .map(|index| PeerId::repeat_byte((index + 1) as u8))
            .collect::<Vec<_>>();
        let receipt_peers = (0..PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS)
            .map(|index| PeerId::repeat_byte((index + 41) as u8))
            .collect::<Vec<_>>();
        let ranges = vec![0..64, 64..128, 128..192, 192..256];
        let plan = BodyReceiptRequestPlan {
            hashes: vec![B256::ZERO; 256],
            range_indices_by_start: ranges
                .iter()
                .enumerate()
                .map(|(index, range)| (range.start, index))
                .collect(),
            ranges,
            return_blocks: PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS,
            body_peer_ids: body_peers,
            receipt_peer_ids: receipt_peers,
            max_in_flight: 4,
            body_max_in_flight: 4,
            receipt_max_in_flight: 4,
            peer_rotation: 0,
            priority: BodyReceiptRequestPriority::Full,
            peers: HashMap::new(),
            accounting_tx: None,
        };

        let reservations = plan.reservations();
        let body_reservations = reservations
            .entries()
            .iter()
            .filter(|entry| matches!(entry.kind, PeerRequestKind::Bodies))
            .map(|entry| entry.count)
            .sum::<usize>();
        let receipt_reservations = reservations
            .entries()
            .iter()
            .filter(|entry| matches!(entry.kind, PeerRequestKind::Receipts))
            .map(|entry| entry.count)
            .sum::<usize>();

        assert_eq!(body_reservations, 4);
        assert_eq!(receipt_reservations, 4);
    }

    #[test]
    fn body_receipt_dense_plan_reserves_only_initial_live_wave() {
        let body_peers = (0..PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS)
            .map(|index| PeerId::repeat_byte((index + 1) as u8))
            .collect::<Vec<_>>();
        let receipt_peers = (0..PIPELINED_BODY_RECEIPT_PREFIX_REDUNDANCY_MIN_PEERS)
            .map(|index| PeerId::repeat_byte((index + 41) as u8))
            .collect::<Vec<_>>();
        let ranges = vec![0..64, 64..128, 128..192, 192..256];
        let plan = BodyReceiptRequestPlan {
            hashes: vec![B256::ZERO; 256],
            range_indices_by_start: ranges
                .iter()
                .enumerate()
                .map(|(index, range)| (range.start, index))
                .collect(),
            ranges,
            return_blocks: PIPELINED_BODY_RECEIPT_MIN_CONTIGUOUS_RETURN_BLOCKS,
            body_peer_ids: body_peers,
            receipt_peer_ids: receipt_peers,
            max_in_flight: 4,
            body_max_in_flight: 8,
            receipt_max_in_flight: 8,
            peer_rotation: 0,
            priority: BodyReceiptRequestPriority::Full,
            peers: HashMap::new(),
            accounting_tx: None,
        };

        let reservations = plan.reservations();
        let body_reservations = reservations
            .entries()
            .iter()
            .filter(|entry| matches!(entry.kind, PeerRequestKind::Bodies))
            .map(|entry| entry.count)
            .sum::<usize>();
        let receipt_reservations = reservations
            .entries()
            .iter()
            .filter(|entry| matches!(entry.kind, PeerRequestKind::Receipts))
            .map(|entry| entry.count)
            .sum::<usize>();

        assert_eq!(body_reservations, 4);
        assert_eq!(receipt_reservations, 4);
    }

    #[test]
    fn body_receipt_live_chunk_waits_for_bodies_when_receipts_are_cached() {
        let idle_receipts = BodyReceiptChunkLiveRoleState::default();
        let cached_without_bodies = BodyReceiptChunkLiveStatus {
            has_bodies: false,
            has_cached_receipts: true,
            force: true,
        };
        assert!(!body_receipt_chunk_should_schedule_receipts(
            &idle_receipts,
            &cached_without_bodies
        ));

        let missing_without_cache = BodyReceiptChunkLiveStatus {
            has_cached_receipts: false,
            ..cached_without_bodies
        };
        assert!(body_receipt_chunk_should_schedule_receipts(
            &idle_receipts,
            &missing_without_cache
        ));
    }

    #[test]
    fn body_receipt_live_chunk_hedges_only_stale_inflight_roles() {
        let fresh = BodyReceiptChunkLiveRoleState {
            in_flight: 1,
            last_scheduled_at: Some(Instant::now()),
            ..Default::default()
        };
        let stale = BodyReceiptChunkLiveRoleState {
            in_flight: 1,
            last_scheduled_at: Some(
                Instant::now() - PIPELINED_BODY_RECEIPT_HEDGE_DELAY - Duration::from_millis(1),
            ),
            ..Default::default()
        };
        let missing_bodies = BodyReceiptChunkLiveStatus {
            has_bodies: false,
            has_cached_receipts: false,
            force: false,
        };
        assert!(!body_receipt_chunk_should_schedule_body(
            &fresh,
            &missing_bodies
        ));
        assert!(body_receipt_chunk_should_schedule_body(
            &stale,
            &missing_bodies
        ));

        let missing_receipts = BodyReceiptChunkLiveStatus {
            has_bodies: true,
            ..missing_bodies
        };
        assert!(!body_receipt_chunk_should_schedule_receipts(
            &fresh,
            &missing_receipts
        ));
        assert!(body_receipt_chunk_should_schedule_receipts(
            &stale,
            &missing_receipts
        ));
    }

    #[test]
    fn body_receipt_prefix_redundancy_schedules_only_full_priority_prefix_chunks() {
        let body_peers = (0..20)
            .map(|index| PeerId::repeat_byte(index as u8 + 1))
            .collect::<Vec<_>>();
        let receipt_peers = (0..20)
            .map(|index| PeerId::repeat_byte(index as u8 + 41))
            .collect::<Vec<_>>();
        let ranges = vec![0..32, 32..64, 64..96, 96..128];
        let mut plan = BodyReceiptRequestPlan {
            hashes: vec![B256::ZERO; 128],
            range_indices_by_start: ranges
                .iter()
                .enumerate()
                .map(|(index, range)| (range.start, index))
                .collect(),
            ranges,
            return_blocks: 128,
            body_peer_ids: body_peers,
            receipt_peer_ids: receipt_peers,
            max_in_flight: 8,
            body_max_in_flight: 8,
            receipt_max_in_flight: 8,
            peer_rotation: 0,
            priority: BodyReceiptRequestPriority::Lookahead,
            peers: HashMap::new(),
            accounting_tx: None,
        };
        let schedule = BodyReceiptLiveLaneSchedule {
            min_return_blocks: 128,
            max_prefix_chunks: 4,
            max_background_chunks: 0,
            max_live_chunks: 4,
            max_role_attempts: 16,
        };
        let mut active_chunks = [0usize, 32, 64]
            .into_iter()
            .map(|start| {
                (
                    start,
                    PlanLiveBodyReceiptChunk {
                        range: start..start + 32,
                        candidates: BodyReceiptChunkLiveCandidates {
                            bodies: Vec::new(),
                            receipts: Vec::new(),
                        },
                        state: BodyReceiptChunkLiveState::default(),
                        bodies: None,
                        expected_receipt_counts: None,
                        cached_receipts: Vec::new(),
                        completed: false,
                        hedges: 0,
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        let completed_chunks: BTreeMap<usize, Vec<SourcedBodyReceipts>> = BTreeMap::new();
        let mut peer_state = BodyReceiptPlanPeerState::default();
        {
            let mut attempts = futures_util::stream::FuturesUnordered::new();
            assert_eq!(
                schedule_body_receipt_prefix_redundancy(
                    &plan,
                    &mut attempts,
                    &mut active_chunks,
                    &mut peer_state,
                    &completed_chunks,
                    schedule,
                ),
                0
            );
            assert_eq!(attempts.len(), 0);
        }

        plan.priority = BodyReceiptRequestPriority::Full;
        let mut attempts = futures_util::stream::FuturesUnordered::new();
        assert_eq!(
            schedule_body_receipt_prefix_redundancy(
                &plan,
                &mut attempts,
                &mut active_chunks,
                &mut peer_state,
                &completed_chunks,
                schedule,
            ),
            4
        );
        assert_eq!(attempts.len(), 4);
        assert_eq!(active_chunks.get(&0).map(|chunk| chunk.hedges), Some(1));
        assert_eq!(active_chunks.get(&32).map(|chunk| chunk.hedges), Some(1));
        assert_eq!(active_chunks.get(&64).map(|chunk| chunk.hedges), Some(0));
    }

    #[test]
    fn body_receipt_prefix_critical_limit_adds_bounded_retry_lane() {
        assert_eq!(
            body_receipt_prefix_critical_role_attempt_limit(12, false),
            12
        );
        assert_eq!(
            body_receipt_prefix_critical_role_attempt_limit(12, true),
            12 + PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_EXTRA_ROLE_ATTEMPTS
        );
    }

    #[test]
    fn body_receipt_detects_buffered_suffix_behind_prefix_gap() {
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![1u8; 32]);
        assert!(!body_receipt_has_buffered_suffix_after_prefix(&chunks, 96));

        chunks.insert(64, vec![1u8; 32]);
        assert!(body_receipt_has_buffered_suffix_after_prefix(&chunks, 96));

        chunks.insert(32, vec![1u8; 32]);
        assert!(!body_receipt_has_buffered_suffix_after_prefix(&chunks, 96));

        let chunks = BTreeMap::from([(32usize, vec![1u8; 32])]);
        assert!(body_receipt_has_buffered_suffix_after_prefix(&chunks, 16));
    }

    #[test]
    fn body_receipt_prefix_critical_repair_detects_stale_inflight_prefix_role() {
        let peer = PeerId::repeat_byte(0x11);
        let mut active_chunks = HashMap::new();
        active_chunks.insert(
            0usize,
            PlanLiveBodyReceiptChunk {
                range: 0..32,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: vec![peer, PeerId::repeat_byte(0x22)],
                    receipts: vec![peer],
                },
                state: BodyReceiptChunkLiveState {
                    bodies: BodyReceiptChunkLiveRoleState {
                        next_index: 1,
                        used_peers: HashSet::from([peer]),
                        in_flight: 1,
                        last_scheduled_at: Some(
                            Instant::now()
                                - PIPELINED_BODY_RECEIPT_HEDGE_DELAY
                                - Duration::from_millis(1),
                        ),
                    },
                    receipts: BodyReceiptChunkLiveRoleState {
                        next_index: 1,
                        used_peers: HashSet::from([peer]),
                        in_flight: 1,
                        last_scheduled_at: Some(Instant::now()),
                    },
                },
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: 0,
            },
        );

        assert!(body_receipt_prefix_critical_repair_needed(
            &active_chunks,
            &BTreeMap::<usize, Vec<u8>>::new(),
            32,
        ));
    }

    #[test]
    fn body_receipt_prefix_critical_repair_ignores_fresh_inflight_prefix_role() {
        let peer = PeerId::repeat_byte(0x11);
        let mut active_chunks = HashMap::new();
        active_chunks.insert(
            0usize,
            PlanLiveBodyReceiptChunk {
                range: 0..32,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: vec![peer, PeerId::repeat_byte(0x22)],
                    receipts: vec![peer],
                },
                state: BodyReceiptChunkLiveState {
                    bodies: BodyReceiptChunkLiveRoleState {
                        next_index: 1,
                        used_peers: HashSet::from([peer]),
                        in_flight: 1,
                        last_scheduled_at: Some(Instant::now()),
                    },
                    receipts: BodyReceiptChunkLiveRoleState {
                        next_index: 1,
                        used_peers: HashSet::from([peer]),
                        in_flight: 1,
                        last_scheduled_at: Some(Instant::now()),
                    },
                },
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: 0,
            },
        );

        assert!(!body_receipt_prefix_critical_repair_needed(
            &active_chunks,
            &BTreeMap::<usize, Vec<u8>>::new(),
            32,
        ));
    }

    #[test]
    fn body_receipt_prefix_critical_repair_ignores_exhausted_prefix_role_candidates() {
        let peer = PeerId::repeat_byte(0x11);
        let mut active_chunks = HashMap::new();
        active_chunks.insert(
            0usize,
            PlanLiveBodyReceiptChunk {
                range: 0..32,
                candidates: BodyReceiptChunkLiveCandidates {
                    bodies: vec![peer],
                    receipts: vec![peer],
                },
                state: BodyReceiptChunkLiveState {
                    bodies: BodyReceiptChunkLiveRoleState {
                        next_index: 1,
                        used_peers: HashSet::from([peer]),
                        in_flight: 0,
                        last_scheduled_at: Some(Instant::now()),
                    },
                    receipts: BodyReceiptChunkLiveRoleState {
                        next_index: 1,
                        used_peers: HashSet::from([peer]),
                        in_flight: 1,
                        last_scheduled_at: Some(Instant::now()),
                    },
                },
                bodies: None,
                expected_receipt_counts: None,
                cached_receipts: Vec::new(),
                completed: false,
                hedges: 0,
            },
        );

        assert!(!body_receipt_prefix_critical_repair_needed(
            &active_chunks,
            &BTreeMap::<usize, Vec<u8>>::new(),
            32,
        ));
    }

    #[test]
    fn body_receipt_prefix_critical_candidate_expansion_uses_fresh_peers() {
        let body_peers = (0..10)
            .map(|index| PeerId::repeat_byte((index + 1) as u8))
            .collect::<Vec<_>>();
        let receipt_peers = (0..10)
            .map(|index| PeerId::repeat_byte((index + 41) as u8))
            .collect::<Vec<_>>();
        let ranges = vec![0..32];
        let plan = BodyReceiptRequestPlan {
            hashes: vec![B256::ZERO; 32],
            range_indices_by_start: HashMap::from([(0usize, 0usize)]),
            ranges,
            return_blocks: 32,
            body_peer_ids: body_peers.clone(),
            receipt_peer_ids: receipt_peers.clone(),
            max_in_flight: 1,
            body_max_in_flight: 1,
            receipt_max_in_flight: 1,
            peer_rotation: 0,
            priority: BodyReceiptRequestPriority::Full,
            peers: HashMap::new(),
            accounting_tx: None,
        };
        let mut chunk = PlanLiveBodyReceiptChunk {
            range: 0..32,
            candidates: BodyReceiptChunkLiveCandidates {
                bodies: body_peers[..PIPELINED_CHUNK_REQUEST_PEERS].to_vec(),
                receipts: receipt_peers[..PIPELINED_CHUNK_REQUEST_PEERS].to_vec(),
            },
            state: BodyReceiptChunkLiveState {
                bodies: BodyReceiptChunkLiveRoleState {
                    used_peers: body_peers[..PIPELINED_CHUNK_REQUEST_PEERS]
                        .iter()
                        .copied()
                        .collect(),
                    ..Default::default()
                },
                receipts: BodyReceiptChunkLiveRoleState {
                    used_peers: receipt_peers[..PIPELINED_CHUNK_REQUEST_PEERS]
                        .iter()
                        .copied()
                        .collect(),
                    ..Default::default()
                },
            },
            bodies: None,
            expected_receipt_counts: None,
            cached_receipts: Vec::new(),
            completed: false,
            hedges: 0,
        };

        extend_body_receipt_plan_chunk_candidates(
            &plan,
            &BodyReceiptPlanPeerState::default(),
            plan.ranges.len(),
            &mut chunk,
        );

        assert_eq!(
            chunk.candidates.bodies.len(),
            PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_PEERS
        );
        assert_eq!(
            chunk.candidates.receipts.len(),
            PIPELINED_BODY_RECEIPT_PREFIX_CRITICAL_PEERS
        );
        assert!(
            chunk.candidates.bodies[PIPELINED_CHUNK_REQUEST_PEERS..]
                .iter()
                .all(|peer| !chunk.state.bodies.used_peers.contains(peer))
        );
        assert!(
            chunk.candidates.receipts[PIPELINED_CHUNK_REQUEST_PEERS..]
                .iter()
                .all(|peer| !chunk.state.receipts.used_peers.contains(peer))
        );
    }

    #[test]
    fn body_receipt_prefix_range_truncates_to_return_boundary() {
        assert_eq!(body_receipt_prefix_range(0..64, 128), Some(0..64));
        assert_eq!(body_receipt_prefix_range(64..160, 128), Some(64..128));
        assert_eq!(body_receipt_prefix_range(128..192, 128), None);
    }

    #[test]
    fn body_receipt_missing_prefix_reassigns_earliest_unowned_gap() {
        let ranges = vec![0..32, 32..64, 64..96];
        let indices = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect::<HashMap<_, _>>();
        let mut retry_counts = HashMap::new();
        let in_flight = HashSet::new();
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![1u8; 32]);
        chunks.insert(64, vec![1u8; 32]);

        assert_eq!(
            body_receipt_missing_prefix_reassign_candidate(
                &ranges,
                &indices,
                &mut retry_counts,
                &in_flight,
                &chunks,
                96,
            ),
            Some((32..64, 4))
        );
    }

    #[test]
    fn body_receipt_missing_prefix_reassign_skips_inflight_gap() {
        let ranges = vec![0..32, 32..64, 64..96];
        let indices = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect::<HashMap<_, _>>();
        let mut retry_counts = HashMap::new();
        let in_flight = HashSet::from([32]);
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![1u8; 32]);

        assert_eq!(
            body_receipt_missing_prefix_reassign_candidate(
                &ranges,
                &indices,
                &mut retry_counts,
                &in_flight,
                &chunks,
                96,
            ),
            None
        );
    }

    #[test]
    fn body_receipt_missing_prefix_reassign_is_bounded() {
        let ranges = vec![0..32, 32..64];
        let indices = ranges
            .iter()
            .enumerate()
            .map(|(index, range)| (range.start, index))
            .collect::<HashMap<_, _>>();
        let mut retry_counts = HashMap::from([(0, PIPELINED_BODY_RECEIPT_PREFIX_REASSIGN_ROUNDS)]);

        assert_eq!(
            body_receipt_missing_prefix_reassign_candidate(
                &ranges,
                &indices,
                &mut retry_counts,
                &HashSet::new(),
                &BTreeMap::<usize, Vec<u8>>::new(),
                64,
            ),
            None
        );
    }

    #[test]
    fn body_receipt_prefix_salvage_requires_buffered_suffix() {
        let ranges = vec![0..32, 32..64, 64..96];

        assert_eq!(
            body_receipt_missing_prefix_salvage_range(
                &ranges,
                &BTreeMap::<usize, Vec<u8>>::new(),
                96,
            ),
            None
        );

        let chunks = BTreeMap::from([(64usize, vec![1u8; 32])]);
        assert_eq!(
            body_receipt_missing_prefix_salvage_range(&ranges, &chunks, 96),
            Some(0..32)
        );
    }

    #[test]
    fn body_receipt_prefix_salvage_truncates_to_progress_target() {
        let ranges = vec![0..32, 32..80, 80..128];
        let chunks = BTreeMap::from([(0usize, vec![1u8; 32]), (80usize, vec![1u8; 48])]);

        assert_eq!(
            body_receipt_missing_prefix_salvage_range(&ranges, &chunks, 64),
            Some(32..64)
        );
    }

    #[test]
    fn split_contiguous_prefix_returns_deterministic_prefix() {
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![0, 1, 2, 3]);
        chunks.insert(4, vec![4, 5, 6, 7]);
        chunks.insert(8, vec![8, 9, 10, 11]);

        let (blocks, _) = split_contiguous_prefix(6, chunks);

        assert_eq!(blocks, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn split_contiguous_prefix_preserves_residual_chunks_after_gap() {
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![0, 1, 2, 3]);
        chunks.insert(8, vec![8, 9, 10, 11]);
        chunks.insert(12, vec![12, 13, 14, 15]);

        let (blocks, residual_chunks) = split_contiguous_prefix(16, chunks);

        assert_eq!(blocks, vec![0, 1, 2, 3]);
        assert_eq!(residual_chunks.get(&4), Some(&vec![8, 9, 10, 11]));
        assert_eq!(residual_chunks.get(&8), Some(&vec![12, 13, 14, 15]));
    }

    #[test]
    fn primary_body_receipt_completion_emits_only_contiguous_prefix() {
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![0u8; 128]);
        chunks.insert(256, vec![1u8; 128]);

        let completion = body_receipt_completion_chunks(512, chunks, None);

        assert_eq!(completion.blocks.len(), 128);
        assert_eq!(completion.planned_return_blocks, 128);
        assert!(completion.residual_chunks.is_empty());
        assert_eq!(
            completion.min_accepted_prefix,
            PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS
        );
    }

    #[test]
    fn residual_body_receipt_completion_preserves_suffix_chunks() {
        let mut chunks = BTreeMap::new();
        chunks.insert(0, vec![0u8; 128]);
        chunks.insert(256, vec![1u8; 128]);

        let completion = body_receipt_completion_chunks(
            512,
            chunks,
            Some(PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS),
        );

        assert_eq!(completion.blocks.len(), 128);
        assert_eq!(completion.planned_return_blocks, 512);
        assert_eq!(completion.residual_chunks.get(&128), Some(&vec![1u8; 128]));
        assert_eq!(
            completion.min_accepted_prefix,
            PIPELINED_BODY_RECEIPT_RESIDUAL_MIN_ACCEPTED_PREFIX_BLOCKS
        );
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
    fn body_receipt_scheduler_attempt_metrics_track_roles_separately() {
        let peer = PeerId::repeat_byte(0x11);
        let mut metrics = BodyReceiptSchedulerMetrics::default();
        let stats = vec![
            (peer, PeerRequestKind::Bodies, 4, Duration::from_millis(100)),
            (
                peer,
                PeerRequestKind::Receipts,
                3,
                Duration::from_millis(150),
            ),
        ];
        let failures = vec![
            ChunkRequestFailure {
                role: ChunkRequestRole::Bodies,
                peer_id: peer,
                requested: 8,
                kind: ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout,
                )),
            },
            ChunkRequestFailure {
                role: ChunkRequestRole::Receipts,
                peer_id: peer,
                requested: 8,
                kind: ChunkFailureKind::Request(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout,
                )),
            },
        ];

        update_body_receipt_scheduler_attempt_metrics(&mut metrics, &stats, &failures);

        assert_eq!(metrics.body_successes, 1);
        assert_eq!(metrics.receipt_successes, 1);
        assert_eq!(metrics.body_failures, 1);
        assert_eq!(metrics.receipt_failures, 1);
        assert_eq!(metrics.body_blocks, 4);
        assert_eq!(metrics.receipt_blocks, 3);
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
