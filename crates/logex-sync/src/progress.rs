use std::sync::{Arc, Mutex};
use std::time::Instant;

use logex_types::{NodeState, SyncStatus};

/// Tracks sync progress and updates the shared SyncStatus.
pub struct ProgressTracker {
    status: Arc<Mutex<SyncStatus>>,
    start: Instant,
    blocks_processed: u64,
    logs_ingested: u64,
    /// Block number when we last logged progress.
    last_log_block: u64,
    /// Timestamp of the last terminal progress line.
    last_log_at: Instant,
}

impl ProgressTracker {
    pub fn new(status: Arc<Mutex<SyncStatus>>) -> Self {
        Self {
            status,
            start: Instant::now(),
            blocks_processed: 0,
            logs_ingested: 0,
            last_log_block: 0,
            last_log_at: Instant::now(),
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
        status.node_state = node_state;
        status.syncing = node_state == NodeState::Syncing;
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

    /// Record that a block has been ingested.
    pub fn record_block(&mut self, block_number: u64, log_count: u64) {
        self.blocks_processed += 1;
        self.logs_ingested += log_count;

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
                eta_seconds = status.eta_seconds.map(|eta| eta.round() as u64),
                "sync progress"
            );
        }
    }

    /// Mark sync as complete (caught up to tip).
    pub fn mark_synced(&self) {
        let mut status = self.status.lock().unwrap();
        status.node_state = NodeState::Synced;
        status.syncing = false;
        status.target_block = status.current_block;
        status.eta_seconds = None;
    }

    pub fn blocks_processed(&self) -> u64 {
        self.blocks_processed
    }

    pub fn logs_ingested(&self) -> u64 {
        self.logs_ingested
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_types::{NodeState, SyncStatus};

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
}
