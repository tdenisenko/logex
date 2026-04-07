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
        let mut status = self.status.lock().unwrap();
        status.target_block = status.target_block.max(target_block);
        status.syncing = status.target_block > status.current_block;
        if status.syncing && matches!(status.node_state, NodeState::Starting | NodeState::Synced) {
            status.node_state = NodeState::Syncing;
        }
    }

    /// Update the node's connectivity state and peer counts.
    pub fn update_network_state(
        &self,
        node_state: NodeState,
        connected_peers: usize,
        pending_peers: usize,
    ) {
        let mut status = self.status.lock().unwrap();
        status.node_state = node_state;
        status.connected_peers = connected_peers;
        status.pending_peers = pending_peers;
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
