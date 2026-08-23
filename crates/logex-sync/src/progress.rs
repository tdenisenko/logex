use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use logex_cl::{
    MAINNET_CONSENSUS_CHAIN_SPEC, optimistic_head_is_fresh_at, optimistic_head_lag_slots,
};
use logex_types::{
    ChainAnchors, ExecutionBlockMarker, ExecutionNetworkStatus, NodeState, SyncStatus,
    WeakSubjectivityCheckpoint,
};

const HISTORICAL_RATE_EWMA_WEIGHT: f64 = 0.35;
const LIVE_LOG_RATE_EWMA_WEIGHT: f64 = 0.35;

/// Tracks sync progress and updates the shared SyncStatus.
pub struct ProgressTracker {
    status: Arc<Mutex<SyncStatus>>,
    start: Instant,
    blocks_processed: u64,
    logs_ingested: u64,
    live_log_rate_at: Instant,
    live_recent_logs_per_sec: f64,
    /// Block number when we last logged progress.
    last_log_block: u64,
    /// Timestamp of the last terminal progress line.
    last_log_at: Instant,
    historical_blocks_processed: u64,
    historical_logs_ingested: u64,
    historical_rate_at: Instant,
    historical_recent_blocks_per_sec: f64,
    historical_recent_logs_per_sec: f64,
    last_historical_log_block: u64,
    last_historical_log_at: Instant,
}

impl ProgressTracker {
    pub fn new(status: Arc<Mutex<SyncStatus>>) -> Self {
        Self {
            status,
            start: Instant::now(),
            blocks_processed: 0,
            logs_ingested: 0,
            live_log_rate_at: Instant::now(),
            live_recent_logs_per_sec: 0.0,
            last_log_block: 0,
            last_log_at: Instant::now(),
            historical_blocks_processed: 0,
            historical_logs_ingested: 0,
            historical_rate_at: Instant::now(),
            historical_recent_blocks_per_sec: 0.0,
            historical_recent_logs_per_sec: 0.0,
            last_historical_log_block: u64::MAX,
            last_historical_log_at: Instant::now(),
        }
    }

    /// Set the network tip as the sync target.
    pub fn set_target(&self, target_block: u64) {
        if target_block == 0 {
            return;
        }

        let mut status = self.status.lock().unwrap();
        let previous_target = status.target_block;
        status.target_block = status.target_block.max(target_block);
        status.syncing = status.node_state == NodeState::Syncing;

        if status.target_block > previous_target
            && (previous_target == 0 || status.target_block.saturating_sub(previous_target) >= 1024)
        {
            tracing::info!(
                current_block = status.current_block,
                target_block = status.target_block,
                remaining_blocks = status.target_block.saturating_sub(status.current_block),
                "updated sync target from peer announcements"
            );
        }
    }

    /// Update the exposed CL checkpoint and chain-anchor snapshot.
    pub fn update_consensus_state(
        &self,
        checkpoint: WeakSubjectivityCheckpoint,
        anchors: &ChainAnchors,
    ) {
        self.update_consensus_state_at(
            checkpoint,
            anchors,
            MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_slot(),
        );
    }

    fn update_consensus_state_at(
        &self,
        checkpoint: WeakSubjectivityCheckpoint,
        anchors: &ChainAnchors,
        current_slot: u64,
    ) {
        let mut status = self.status.lock().unwrap();
        let optimistic_slot = anchors.optimistic_head.map(|anchor| anchor.beacon_slot);
        let head_fresh =
            optimistic_slot.is_some_and(|slot| optimistic_head_is_fresh_at(current_slot, slot));
        status.checkpoint = Some(checkpoint);
        status.indexed_execution_head = anchors.indexed_head;
        status.optimistic_execution_head = anchors.optimistic_head;
        status.finalized_execution_head = anchors.finalized_head;
        status.consensus_current_slot = Some(current_slot);
        status.consensus_head_lag_slots =
            optimistic_slot.map(|slot| optimistic_head_lag_slots(current_slot, slot));
        status.consensus_head_fresh = Some(head_fresh);
        status.target_block = anchors
            .optimistic_head
            .map_or(status.current_block, |anchor| anchor.block_number);
        if !head_fresh && status.node_state == NodeState::Synced {
            status.node_state = NodeState::WaitingForConsensus;
            status.syncing = false;
        }
    }

    /// Update the node's connectivity state and peer counts.
    pub fn update_network_state(
        &self,
        node_state: NodeState,
        connected_peers: usize,
        serving_peers: usize,
        pending_peers: usize,
    ) {
        let mut status = self.status.lock().unwrap();
        let previous_state = status.node_state;
        let historical_incomplete = historical_sync_is_incomplete(&status);
        let node_state = if historical_incomplete
            && matches!(
                node_state,
                NodeState::Synced | NodeState::WaitingForConsensus
            ) {
            NodeState::Syncing
        } else if node_state == NodeState::Synced
            && (status.consensus_head_fresh == Some(false)
                || (status.checkpoint.is_some() && status.consensus_head_fresh.is_none()))
        {
            NodeState::WaitingForConsensus
        } else {
            node_state
        };
        status.node_state = node_state;
        status.syncing = node_state == NodeState::Syncing || historical_incomplete;
        status.connected_peers = connected_peers;
        status.serving_peers = serving_peers;
        status.pending_peers = pending_peers;

        if previous_state != node_state {
            tracing::info!(
                from = previous_state.as_label(),
                to = node_state.as_label(),
                current_block = status.current_block,
                target_block = status.target_block,
                connected_peers,
                serving_peers,
                pending_peers,
                "node state changed"
            );
        }
    }

    pub fn update_execution_network_state(&self, execution_network: ExecutionNetworkStatus) {
        let mut status = self.status.lock().unwrap();
        status.execution_network = Some(execution_network);
    }

    /// Record that a block has been ingested.
    pub fn record_block(&mut self, block_number: u64, log_count: u64) {
        self.record_blocks(block_number, 1, log_count);
    }

    /// Record that a contiguous batch of forward blocks has been ingested.
    pub fn record_blocks(&mut self, block_number: u64, block_count: u64, log_count: u64) {
        if block_count == 0 {
            return;
        }

        self.blocks_processed += block_count;
        self.logs_ingested += log_count;

        let now = Instant::now();
        let live_interval = now.duration_since(self.live_log_rate_at).as_secs_f64();
        self.live_log_rate_at = now;
        let recent_lps = if live_interval > 0.0 {
            log_count as f64 / live_interval
        } else {
            0.0
        };
        let live_lps = smoothed_live_log_rate(self.live_recent_logs_per_sec, recent_lps);
        self.live_recent_logs_per_sec = live_lps;

        let elapsed = self.start.elapsed().as_secs_f64();
        let bps = if elapsed > 0.0 {
            self.blocks_processed as f64 / elapsed
        } else {
            0.0
        };
        let bpm = bps * 60.0;

        let mut status = self.status.lock().unwrap();
        status.node_state = NodeState::Syncing;
        status.syncing = true;
        status.current_block = block_number;
        status.blocks_per_sec = bps;
        status.blocks_per_minute = bpm;
        status.logs_per_sec = live_lps;
        status.logs_rate_updated_at_unix_ms = Some(unix_time_millis());
        status.logs_ingested = self.logs_ingested;

        if bps > 0.0 && status.target_block > block_number {
            let remaining = (status.target_block - block_number) as f64;
            status.eta_seconds = Some(remaining / bps);
        } else {
            status.eta_seconds = None;
        }

        // Log progress periodically so operators get geth/reth-style feedback
        // even when blocks are sparse or many blocks contain no logs.
        let should_log = block_number / 1000 > self.last_log_block / 1000
            || self.last_log_at.elapsed().as_secs() >= 15;
        if should_log {
            self.last_log_block = block_number;
            self.last_log_at = Instant::now();
            tracing::info!(
                current_block = block_number,
                target_block = status.target_block,
                remaining_blocks = status.target_block.saturating_sub(block_number),
                total_blocks = self.blocks_processed,
                total_logs = self.logs_ingested,
                blocks_per_sec = format!("{bps:.2}"),
                blocks_per_minute = format!("{bpm:.1}"),
                logs_per_sec = format!("{live_lps:.2}"),
                eta_seconds = status.eta_seconds.map(|eta| eta.round() as u64),
                "sync progress"
            );
        }
    }

    pub fn initialize_historical_state(
        &self,
        floor: Option<ExecutionBlockMarker>,
        anchor: Option<ExecutionBlockMarker>,
        target_block: u64,
    ) {
        let mut status = self.status.lock().unwrap();
        status.historical_execution_floor = floor;
        status.historical_execution_anchor = anchor;
        status.historical_target_block = target_block;
        status.historical_eta_seconds = if status.historical_sync_disabled {
            None
        } else {
            historical_eta(
                status.historical_execution_floor,
                status.historical_target_block,
                status.historical_blocks_per_sec,
            )
        };
    }

    pub fn record_historical_blocks(
        &mut self,
        floor: ExecutionBlockMarker,
        anchor: Option<ExecutionBlockMarker>,
        target_block: u64,
        block_count: u64,
        log_count: u64,
    ) {
        if block_count == 0 {
            return;
        }

        self.historical_blocks_processed += block_count;
        self.historical_logs_ingested += log_count;
        self.logs_ingested += log_count;

        let now = Instant::now();
        let interval = now.duration_since(self.historical_rate_at).as_secs_f64();
        self.historical_rate_at = now;
        let recent_bps = if interval > 0.0 {
            block_count as f64 / interval
        } else {
            0.0
        };
        let bps = smoothed_historical_rate(self.historical_recent_blocks_per_sec, recent_bps);
        self.historical_recent_blocks_per_sec = bps;
        let recent_lps = if interval > 0.0 {
            log_count as f64 / interval
        } else {
            0.0
        };
        let lps = smoothed_historical_rate(self.historical_recent_logs_per_sec, recent_lps);
        self.historical_recent_logs_per_sec = lps;

        let mut status = self.status.lock().unwrap();
        status.node_state = NodeState::Syncing;
        status.syncing = true;
        status.historical_execution_floor = Some(floor);
        status.historical_execution_anchor = anchor.or(status.historical_execution_anchor);
        status.historical_target_block = target_block;
        status.historical_blocks_per_sec = bps;
        status.historical_logs_per_sec = lps;
        status.historical_rate_updated_at_unix_ms = Some(unix_time_millis());
        status.historical_eta_seconds = historical_eta(Some(floor), target_block, bps);
        status.logs_ingested = self.logs_ingested;

        let should_log = floor.block_number / 1000 < self.last_historical_log_block / 1000
            || self.last_historical_log_at.elapsed().as_secs() >= 15;
        if should_log {
            self.last_historical_log_block = floor.block_number;
            self.last_historical_log_at = Instant::now();
            tracing::info!(
                historical_floor = floor.block_number,
                historical_target = target_block,
                remaining_blocks = floor.block_number.saturating_sub(target_block),
                total_historical_blocks = self.historical_blocks_processed,
                total_historical_logs = self.historical_logs_ingested,
                historical_blocks_per_sec = format!("{bps:.2}"),
                historical_logs_per_sec = format!("{lps:.2}"),
                historical_eta_seconds =
                    status.historical_eta_seconds.map(|eta| eta.round() as u64),
                "historical reverse sync progress"
            );
        }
    }

    pub fn rewind_to(&self, block_number: u64) {
        let mut status = self.status.lock().unwrap();
        status.current_block = block_number;
        if status.target_block < block_number {
            status.target_block = block_number;
        }
        status.eta_seconds = None;
    }

    /// Mark sync as complete (caught up to tip).
    pub fn mark_synced(&self) {
        let mut status = self.status.lock().unwrap();
        if status.consensus_head_fresh == Some(false)
            || (status.checkpoint.is_some() && status.consensus_head_fresh.is_none())
        {
            status.node_state = NodeState::WaitingForConsensus;
            status.syncing = false;
            status.eta_seconds = None;
            return;
        }
        status.node_state = NodeState::Synced;
        status.syncing = false;
        status.target_block = status.current_block;
        status.eta_seconds = None;
        if status.historical_sync_disabled || !historical_sync_is_incomplete(&status) {
            status.historical_blocks_per_sec = 0.0;
            status.historical_logs_per_sec = 0.0;
            status.historical_rate_updated_at_unix_ms = None;
            status.historical_eta_seconds = None;
        }
    }

    pub fn blocks_processed(&self) -> u64 {
        self.blocks_processed
    }

    pub fn logs_ingested(&self) -> u64 {
        self.logs_ingested
    }
}

fn historical_eta(
    floor: Option<ExecutionBlockMarker>,
    target_block: u64,
    blocks_per_sec: f64,
) -> Option<f64> {
    let floor = floor?;
    if blocks_per_sec <= 0.0 || floor.block_number <= target_block {
        return None;
    }

    Some((floor.block_number - target_block) as f64 / blocks_per_sec)
}

fn historical_sync_is_incomplete(status: &SyncStatus) -> bool {
    !status.historical_sync_disabled
        && status
            .historical_execution_floor
            .is_some_and(|floor| floor.block_number > status.historical_target_block)
}

fn smoothed_historical_rate(previous: f64, recent: f64) -> f64 {
    if recent <= 0.0 {
        return previous.max(0.0);
    }
    if previous <= 0.0 {
        return recent;
    }

    (previous * (1.0 - HISTORICAL_RATE_EWMA_WEIGHT)) + (recent * HISTORICAL_RATE_EWMA_WEIGHT)
}

fn smoothed_live_log_rate(previous: f64, recent: f64) -> f64 {
    if previous <= 0.0 {
        return recent.max(0.0);
    }

    (previous * (1.0 - LIVE_LOG_RATE_EWMA_WEIGHT)) + (recent.max(0.0) * LIVE_LOG_RATE_EWMA_WEIGHT)
}

fn unix_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use logex_types::{ExecutionAnchor, NodeState, SyncStatus, WeakSubjectivityCheckpoint};

    #[test]
    fn syncing_flag_stays_true_while_syncing_without_known_target() {
        let status = Arc::new(Mutex::new(SyncStatus::default()));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_network_state(NodeState::Syncing, 1, 1, 0);

        let status = status.lock().unwrap().clone();
        assert!(status.syncing);
        assert_eq!(status.target_block, 0);
        assert_eq!(status.node_state, NodeState::Syncing);
    }

    #[test]
    fn syncing_flag_turns_off_when_node_is_not_syncing() {
        let status = Arc::new(Mutex::new(SyncStatus::default()));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_network_state(NodeState::Connecting, 1, 0, 10);

        let status = status.lock().unwrap().clone();
        assert!(!status.syncing);
        assert_eq!(status.node_state, NodeState::Connecting);
    }

    #[test]
    fn historical_backfill_keeps_runtime_syncing_state_visible() {
        let status = Arc::new(Mutex::new(SyncStatus {
            historical_execution_floor: Some(ExecutionBlockMarker {
                block_number: 10,
                block_hash: B256::repeat_byte(0x10),
                timestamp: 100,
            }),
            historical_target_block: 0,
            ..Default::default()
        }));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_network_state(NodeState::Synced, 8, 8, 64);

        let status = status.lock().unwrap().clone();
        assert!(status.syncing);
        assert_eq!(status.node_state, NodeState::Syncing);
    }

    #[test]
    fn disabled_historical_sync_does_not_force_runtime_syncing_state() {
        let status = Arc::new(Mutex::new(SyncStatus {
            historical_sync_disabled: true,
            historical_execution_floor: Some(ExecutionBlockMarker {
                block_number: 10,
                block_hash: B256::repeat_byte(0x10),
                timestamp: 100,
            }),
            historical_target_block: 0,
            ..Default::default()
        }));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_network_state(NodeState::Synced, 8, 8, 64);

        let status = status.lock().unwrap().clone();
        assert!(!status.syncing);
        assert_eq!(status.node_state, NodeState::Synced);
    }

    #[test]
    fn historical_rate_smoothing_uses_recent_progress() {
        assert_eq!(smoothed_historical_rate(0.0, 128.0), 128.0);
        assert_eq!(smoothed_historical_rate(100.0, 0.0), 100.0);
        let smoothed = smoothed_historical_rate(100.0, 200.0);
        assert!(smoothed > 100.0);
        assert!(smoothed < 200.0);
    }

    #[test]
    fn live_log_rate_smoothing_decays_on_empty_blocks() {
        assert_eq!(smoothed_live_log_rate(0.0, 128.0), 128.0);
        let decayed = smoothed_live_log_rate(100.0, 0.0);
        assert!(decayed > 0.0);
        assert!(decayed < 100.0);
    }

    #[test]
    fn forward_batch_progress_records_one_live_rate_sample() {
        let status = Arc::new(Mutex::new(SyncStatus::default()));
        let mut tracker = ProgressTracker::new(Arc::clone(&status));

        std::thread::sleep(std::time::Duration::from_millis(10));
        tracker.record_blocks(128, 128, 1_280);

        let status = status.lock().unwrap().clone();
        assert_eq!(status.current_block, 128);
        assert_eq!(status.logs_ingested, 1_280);
        assert!(status.blocks_per_sec > 0.0);
        assert!(status.logs_per_sec > 0.0);
        assert!(status.logs_per_sec < 1_000_000.0);
    }

    #[test]
    fn consensus_target_can_move_backwards_after_reorg() {
        let status = Arc::new(Mutex::new(SyncStatus {
            current_block: 95,
            target_block: 100,
            ..Default::default()
        }));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_consensus_state_at(
            WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(0x11),
                beacon_slot: Some(1),
            },
            &ChainAnchors {
                indexed_head: None,
                finalized_head: None,
                optimistic_head: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x22),
                    beacon_slot: 2,
                    block_number: 97,
                    block_hash: B256::repeat_byte(0x33),
                    receipts_root: B256::repeat_byte(0x44),
                }),
            },
            2,
        );

        let status = status.lock().unwrap().clone();
        assert_eq!(status.target_block, 97);
    }

    #[test]
    fn stale_consensus_head_prevents_synced_state() {
        let status = Arc::new(Mutex::new(SyncStatus {
            node_state: NodeState::Synced,
            current_block: 97,
            target_block: 97,
            ..Default::default()
        }));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_consensus_state_at(
            WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(0x11),
                beacon_slot: Some(100),
            },
            &ChainAnchors {
                indexed_head: None,
                finalized_head: None,
                optimistic_head: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x22),
                    beacon_slot: 100,
                    block_number: 97,
                    block_hash: B256::repeat_byte(0x33),
                    receipts_root: B256::repeat_byte(0x44),
                }),
            },
            105,
        );
        tracker.mark_synced();

        let status = status.lock().unwrap().clone();
        assert_eq!(status.consensus_head_lag_slots, Some(5));
        assert_eq!(status.consensus_head_fresh, Some(false));
        assert_eq!(status.node_state, NodeState::WaitingForConsensus);
        assert!(!status.syncing);
    }

    #[test]
    fn fresh_consensus_head_allows_synced_state() {
        let status = Arc::new(Mutex::new(SyncStatus {
            node_state: NodeState::WaitingForConsensus,
            current_block: 97,
            target_block: 97,
            ..Default::default()
        }));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_consensus_state_at(
            WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(0x11),
                beacon_slot: Some(100),
            },
            &ChainAnchors {
                indexed_head: None,
                finalized_head: None,
                optimistic_head: Some(ExecutionAnchor {
                    beacon_root: B256::repeat_byte(0x22),
                    beacon_slot: 100,
                    block_number: 97,
                    block_hash: B256::repeat_byte(0x33),
                    receipts_root: B256::repeat_byte(0x44),
                }),
            },
            104,
        );
        tracker.mark_synced();

        let status = status.lock().unwrap().clone();
        assert_eq!(status.consensus_head_lag_slots, Some(4));
        assert_eq!(status.consensus_head_fresh, Some(true));
        assert_eq!(status.node_state, NodeState::Synced);
        assert!(!status.syncing);
    }

    #[test]
    fn connectivity_refresh_cannot_overwrite_stale_consensus_state() {
        let status = Arc::new(Mutex::new(SyncStatus {
            checkpoint: Some(WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(0x11),
                beacon_slot: Some(100),
            }),
            consensus_head_fresh: Some(false),
            ..Default::default()
        }));
        let tracker = ProgressTracker::new(Arc::clone(&status));

        tracker.update_network_state(NodeState::Synced, 8, 4, 16);

        let status = status.lock().unwrap().clone();
        assert_eq!(status.node_state, NodeState::WaitingForConsensus);
        assert!(!status.syncing);
    }
}
