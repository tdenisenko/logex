use std::sync::{Arc, Mutex};
use std::time::Instant;

use logex_types::SyncStatus;

/// Tracks sync progress and updates the shared SyncStatus.
pub struct ProgressTracker {
    status: Arc<Mutex<SyncStatus>>,
    start: Instant,
    blocks_processed: u64,
    logs_ingested: u64,
    /// Block number when we last logged progress.
    last_log_block: u64,
}

impl ProgressTracker {
    pub fn new(status: Arc<Mutex<SyncStatus>>) -> Self {
        Self {
            status,
            start: Instant::now(),
            blocks_processed: 0,
            logs_ingested: 0,
            last_log_block: 0,
        }
    }

    /// Set the network tip as the sync target.
    pub fn set_target(&self, target_block: u64) {
        let mut status = self.status.lock().unwrap();
        status.target_block = target_block;
        status.syncing = true;
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

        let mut status = self.status.lock().unwrap();
        status.current_block = block_number;
        status.blocks_per_sec = bps;
        status.logs_ingested = self.logs_ingested;

        if bps > 0.0 && status.target_block > block_number {
            let remaining = (status.target_block - block_number) as f64;
            status.eta_seconds = Some(remaining / bps);
        } else {
            status.eta_seconds = None;
        }

        // Log progress every 1000 blocks.
        if block_number / 1000 > self.last_log_block / 1000 {
            self.last_log_block = block_number;
            tracing::info!(
                block = block_number,
                total_blocks = self.blocks_processed,
                total_logs = self.logs_ingested,
                blocks_per_sec = format!("{bps:.1}"),
                "sync progress"
            );
        }
    }

    /// Mark sync as complete (caught up to tip).
    pub fn mark_synced(&self) {
        let mut status = self.status.lock().unwrap();
        status.syncing = false;
        status.eta_seconds = None;
    }

    pub fn blocks_processed(&self) -> u64 {
        self.blocks_processed
    }

    pub fn logs_ingested(&self) -> u64 {
        self.logs_ingested
    }
}
