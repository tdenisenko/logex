use alloy_consensus::{BlockHeader, Header, TxReceipt, transaction::TxHashRef};
use alloy_primitives::{B256, Log};
use eyre::Result;
use logex_cl::ConsensusStore;
use reth_ethereum_forks::Head;
use reth_network_peers::{NodeRecord, PeerId};
use reth_primitives_traits::{BlockBody, SignedTransaction};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, watch};

use logex_server::SubscriptionManager;
use logex_storage::PartitionManager;
use logex_types::{LogRow, NodeState, SyncStatus};

use crate::SyncConfig;
use crate::head_tracker::{HeadTracker, ReorgInfo};
use crate::p2p::peer_manager::{PeerManager, SourcedBodyReceipts};
use crate::progress::ProgressTracker;
use crate::validation::{
    receipts_match_transaction_count, validate_block_pre_execution, validate_downloaded_headers,
    validate_receipts_for_header, validate_reverse_downloaded_headers,
};

mod anchored;
mod helpers;
mod historical;
mod ingest;
mod live;

use self::helpers::{
    assemble_txs, cancelable, execution_head, peer_refill_goal, preferred_body_peers,
    refill_peer_floor, should_mark_historical_complete, should_run_historical_backfill,
    should_switch_to_live_without_target,
};

const HISTORICAL_EMPTY_THRESHOLD: u32 = 5;
const HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES: u32 = 2;
const LIVE_SYNC_POLL_INTERVAL: Duration = Duration::from_secs(12);
const MIN_ACTIVE_SYNC_PEERS: usize = 8;
const TARGET_ACTIVE_SYNC_PEERS: usize = 48;
const PEER_REFILL_STEP: usize = 16;
const RECENT_HEADER_WINDOW: usize = 8_192;
const HISTORICAL_BACKFILL_HEADER_BATCH_LIMIT: u64 = 1024;
const LIVE_LAG_HISTORICAL_BACKFILL_THRESHOLD: u64 = 32;

pub(super) struct HistoricalBlockIngest {
    header: Header,
    rows: Vec<LogRow>,
}

pub(super) struct HistoricalFetchedBatch {
    child_header: Header,
    header_peer: PeerId,
    headers: Vec<Header>,
    hashes: Vec<B256>,
    blocks: Vec<SourcedBodyReceipts>,
    required_block: u64,
    header_elapsed: Duration,
    body_receipt_elapsed: Duration,
}

pub(super) struct HistoricalIngestOutcome {
    block_count: u64,
    row_count: u64,
    floor: logex_types::ExecutionBlockMarker,
    anchor: Option<logex_types::ExecutionBlockMarker>,
    extraction_elapsed: Duration,
    write_elapsed: Duration,
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
    historical_prefetch: Option<HistoricalFetchedBatch>,
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
        Self {
            config,
            peers,
            storage,
            subscriptions,
            sync_status,
            consensus,
            head_tracker: HeadTracker::new(RECENT_HEADER_WINDOW),
            progress,
            historical_prefetch: None,
            connected_once: false,
            last_validated_header: None,
            shutdown,
        }
    }

    pub fn known_peers(&self) -> Vec<NodeRecord> {
        self.peers.known_peers()
    }

    pub async fn shutdown(&mut self) {
        self.peers.shutdown().await;
    }
}
