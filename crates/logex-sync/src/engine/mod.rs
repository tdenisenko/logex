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
use logex_types::{NodeState, SyncStatus};

use crate::SyncConfig;
use crate::head_tracker::{HeadTracker, ReorgInfo};
use crate::p2p::peer_manager::PeerManager;
use crate::progress::ProgressTracker;
use crate::validation::{
    receipts_match_transaction_count, validate_block_pre_execution, validate_downloaded_headers,
    validate_receipts_for_header,
};

mod anchored;
mod helpers;
mod historical;
mod ingest;
mod live;

use self::helpers::{
    assemble_txs, cancelable, desired_refill_min_peers, should_mark_historical_complete,
    should_switch_to_live_without_target,
};

const HISTORICAL_EMPTY_THRESHOLD: u32 = 5;
const HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES: u32 = 2;
const LIVE_SYNC_POLL_INTERVAL: Duration = Duration::from_secs(12);
const MIN_ACTIVE_SYNC_PEERS: usize = 4;
const RECENT_HEADER_WINDOW: usize = 8_192;

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
