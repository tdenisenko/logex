use alloy_consensus::{BlockHeader, Header, ReceiptWithBloom, TxReceipt, transaction::TxHashRef};
use alloy_primitives::{B256, Log};
use eyre::Result;
use logex_cl::ConsensusStore;
use reth_eth_wire::NetworkPrimitives;
use reth_ethereum_forks::Head;
use reth_network_peers::{NodeRecord, PeerId};
use reth_primitives_traits::{BlockBody, SignedTransaction};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc, watch};
use tokio::task::JoinHandle;

use logex_server::SubscriptionManager;
use logex_storage::PartitionManager;
use logex_types::{NodeState, SyncStatus};

use crate::SyncConfig;
use crate::head_tracker::{HeadTracker, ReorgInfo};
use crate::p2p::peer_manager::{
    BodyReceiptPeerReservations, BodyReceiptRequestAccounting, BodyReceiptRequestOutcome,
    BodyReceiptRequestPlan, PeerManager, ReverseHeaderPagesRequestOutcome,
    ReverseHeaderPagesRequestPlan, SourcedBodyReceipts,
};
use crate::primitives::LogexNetworkPrimitives;
use crate::progress::ProgressTracker;
use crate::validation::{
    HeaderValidationError, receipts_match_transaction_count, validate_block_pre_execution,
    validate_downloaded_headers, validate_receipts_for_header,
    validate_reverse_downloaded_headers_with_hashes,
};

mod anchored;
mod helpers;
mod historical;
mod ingest;
mod live;

use self::helpers::{
    assemble_txs, cancelable, execution_head, historical_backfill_peer_floor, peer_refill_goal,
    preferred_body_peers, refill_peer_floor, should_mark_historical_complete,
    should_switch_to_live_without_target,
};

const HISTORICAL_EMPTY_THRESHOLD: u32 = 5;
const HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES: u32 = 2;
const LIVE_SYNC_POLL_INTERVAL: Duration = Duration::from_secs(12);
const MIN_ACTIVE_SYNC_PEERS: usize = 8;
const TARGET_ACTIVE_SYNC_PEERS: usize = 80;
const PEER_REFILL_STEP: usize = 16;
const HISTORICAL_BACKFILL_CONNECTED_PEER_FLOOR_CAP: usize = 1;
const RECENT_HEADER_WINDOW: usize = 8_192;
const HISTORICAL_BACKFILL_HEADER_BATCH_LIMIT: u64 = 1024;

pub(super) struct HistoricalValidatedBlock {
    index: usize,
    header: Header,
    block_hash: B256,
    body_peer: PeerId,
    body: <LogexNetworkPrimitives as NetworkPrimitives>::BlockBody,
    receipt_peer: PeerId,
    receipts: Vec<ReceiptWithBloom<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>>,
}

pub(super) struct HistoricalFetchedBatch {
    header_peer: PeerId,
    headers: Vec<Header>,
    hashes: Vec<B256>,
    blocks: Vec<SourcedBodyReceipts>,
    planned_return_blocks: usize,
    required_block: u64,
    header_elapsed: Duration,
    body_receipt_elapsed: Duration,
    residual_batch: Option<HistoricalResidualBatch>,
}

pub(super) struct HistoricalHeaderBatch {
    child_header: Header,
    header_peer: PeerId,
    headers: Vec<Header>,
    hashes: Vec<B256>,
    required_block: u64,
    header_elapsed: Duration,
}

pub(super) struct HistoricalResidualBatch {
    header_batch: HistoricalHeaderBatch,
    prefetched_chunks: BTreeMap<usize, Vec<SourcedBodyReceipts>>,
}

pub(super) struct HistoricalFetchPlan {
    header_batch: HistoricalHeaderBatch,
    planned_next_child_header: Option<Header>,
    body_receipt_plan: BodyReceiptRequestPlan,
}

pub(super) struct HistoricalFetchHandle {
    attempt: u64,
    reservations: BodyReceiptPeerReservations,
    handle: JoinHandle<()>,
}

pub(super) struct HistoricalHeaderFetchHandle {
    sequence: u64,
    attempt: u64,
    child_header: Header,
    handle: JoinHandle<()>,
}

pub(super) struct HistoricalHeaderFetchOutcome {
    generation: u64,
    sequence: u64,
    attempt: u64,
    child_header: Header,
    target_count: u64,
    header_elapsed: Duration,
    outcome: ReverseHeaderPagesRequestOutcome,
}

pub(super) struct HistoricalFetchOutcome {
    generation: u64,
    sequence: u64,
    attempt: u64,
    header_batch: HistoricalHeaderBatch,
    body_receipt_elapsed: Duration,
    outcome: BodyReceiptRequestOutcome,
}

pub(super) struct HistoricalIngestOutcome {
    block_count: u64,
    row_count: u64,
    floor: logex_types::ExecutionBlockMarker,
    anchor: Option<logex_types::ExecutionBlockMarker>,
    extraction_elapsed: Duration,
    write_elapsed: Duration,
}

pub(super) struct PreparedHistoricalBatch {
    requested_headers: usize,
    planned_return_blocks: usize,
    header_elapsed: Duration,
    body_receipt_elapsed: Duration,
    extracted: ingest::HistoricalExtractedBatch,
    peer_notes: Vec<PeerId>,
    lowest_block: u64,
    highest_block: u64,
    block_count: usize,
    prepare_queue_elapsed: Duration,
    validation_elapsed: Duration,
    validation_queue_elapsed: Duration,
    processing_elapsed: Duration,
    residual_batch: Option<HistoricalResidualBatch>,
}

pub(super) struct WrittenHistoricalBatch {
    requested_headers: usize,
    planned_return_blocks: usize,
    header_elapsed: Duration,
    body_receipt_elapsed: Duration,
    outcome: HistoricalIngestOutcome,
    peer_notes: Vec<PeerId>,
    lowest_block: u64,
    highest_block: u64,
    block_count: usize,
    prepare_queue_elapsed: Duration,
    validation_elapsed: Duration,
    validation_queue_elapsed: Duration,
    prepare_wait_elapsed: Duration,
    processing_elapsed: Duration,
    residual_batch: Option<HistoricalResidualBatch>,
}

pub(super) struct HistoricalValidationFailure {
    peer: PeerId,
    response_kind: &'static str,
    block_number: u64,
    block_hash: B256,
    message: String,
}

pub(super) struct HistoricalPrepareTask {
    sequence: u64,
    next_child_header: Option<Header>,
    handle: JoinHandle<
        Result<std::result::Result<PreparedHistoricalBatch, Box<HistoricalValidationFailure>>>,
    >,
}

pub(super) type HistoricalPrepareResult =
    Result<std::result::Result<PreparedHistoricalBatch, Box<HistoricalValidationFailure>>>;

pub(super) struct HistoricalCompletedPrepare {
    next_child_header: Option<Header>,
    result: HistoricalPrepareResult,
}

/// The sync engine: orchestrates P2P block fetching, validation, and ingestion.
pub struct SyncEngine {
    config: SyncConfig,
    peers: PeerManager,
    storage: Arc<RwLock<PartitionManager>>,
    subscriptions: Option<SubscriptionManager>,
    sync_status: Arc<std::sync::Mutex<SyncStatus>>,
    consensus: Option<Arc<ConsensusStore>>,
    head_tracker: HeadTracker,
    progress: ProgressTracker,
    historical_header_fetch_tx: mpsc::UnboundedSender<HistoricalHeaderFetchOutcome>,
    historical_header_fetch_rx: mpsc::UnboundedReceiver<HistoricalHeaderFetchOutcome>,
    historical_fetch_tx: mpsc::UnboundedSender<HistoricalFetchOutcome>,
    historical_fetch_rx: mpsc::UnboundedReceiver<HistoricalFetchOutcome>,
    historical_request_accounting_tx: mpsc::UnboundedSender<BodyReceiptRequestAccounting>,
    historical_request_accounting_rx: mpsc::UnboundedReceiver<BodyReceiptRequestAccounting>,
    historical_fetch_generation: u64,
    historical_fetch_next_attempt: u64,
    historical_fetch_next_sequence: u64,
    historical_fetch_expected_sequence: u64,
    historical_fetch_expected_child: Option<Header>,
    historical_fetch_planned_child: Option<Header>,
    historical_fetch_head_of_line_started_at: Option<Instant>,
    historical_header_fetch_handle: Option<HistoricalHeaderFetchHandle>,
    historical_fetch_handles: HashMap<u64, HistoricalFetchHandle>,
    historical_fetch_completed: BTreeMap<u64, HistoricalFetchOutcome>,
    historical_prepare_expected_sequence: u64,
    historical_prepare_handles: BTreeMap<u64, HistoricalPrepareTask>,
    historical_prepare_completed: BTreeMap<u64, HistoricalCompletedPrepare>,
    historical_ingest_sequence: Option<u64>,
    historical_ingest_started_at: Option<Instant>,
    historical_rows_per_block_ewma: Option<f64>,
    last_historical_allocator_trim: Option<Instant>,
    connected_once: bool,
    last_validated_header: Option<Header>,
    shutdown: watch::Receiver<bool>,
}

impl SyncEngine {
    pub fn new(
        config: SyncConfig,
        peers: PeerManager,
        storage: Arc<RwLock<PartitionManager>>,
        subscriptions: Option<SubscriptionManager>,
        sync_status: Arc<std::sync::Mutex<SyncStatus>>,
        consensus: Option<Arc<ConsensusStore>>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        let progress = ProgressTracker::new(Arc::clone(&sync_status));
        let (historical_header_fetch_tx, historical_header_fetch_rx) = mpsc::unbounded_channel();
        let (historical_fetch_tx, historical_fetch_rx) = mpsc::unbounded_channel();
        let (historical_request_accounting_tx, historical_request_accounting_rx) =
            mpsc::unbounded_channel();
        Self {
            config,
            peers,
            storage,
            subscriptions,
            sync_status,
            consensus,
            head_tracker: HeadTracker::new(RECENT_HEADER_WINDOW),
            progress,
            historical_header_fetch_tx,
            historical_header_fetch_rx,
            historical_fetch_tx,
            historical_fetch_rx,
            historical_request_accounting_tx,
            historical_request_accounting_rx,
            historical_fetch_generation: 0,
            historical_fetch_next_attempt: 0,
            historical_fetch_next_sequence: 0,
            historical_fetch_expected_sequence: 0,
            historical_fetch_expected_child: None,
            historical_fetch_planned_child: None,
            historical_fetch_head_of_line_started_at: None,
            historical_header_fetch_handle: None,
            historical_fetch_handles: HashMap::new(),
            historical_fetch_completed: BTreeMap::new(),
            historical_prepare_expected_sequence: 0,
            historical_prepare_handles: BTreeMap::new(),
            historical_prepare_completed: BTreeMap::new(),
            historical_ingest_sequence: None,
            historical_ingest_started_at: None,
            historical_rows_per_block_ewma: None,
            last_historical_allocator_trim: None,
            connected_once: false,
            last_validated_header: None,
            shutdown,
        }
    }

    pub fn known_peers(&self) -> Vec<NodeRecord> {
        self.peers.known_peers()
    }

    pub async fn shutdown(&mut self) {
        self.reset_historical_fetch_pipeline();
        self.peers.shutdown().await;
    }
}
