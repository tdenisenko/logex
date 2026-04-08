use alloy_consensus::{BlockHeader, TxReceipt, transaction::TxHashRef};
use alloy_primitives::{B256, Log};
use eyre::Result;
use reth_ethereum_forks::Head;
use reth_network_peers::NodeRecord;
use reth_primitives_traits::{BlockBody, SignedTransaction};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, watch};

use logex_index::IndexBuilder;
use logex_ingestion::extract;
use logex_server::SubscriptionManager;
use logex_storage::PartitionManager;
use logex_types::{NodeState, SyncStatus};

use crate::SyncConfig;
use crate::head_tracker::{HeadTracker, ReorgInfo};
use crate::p2p::peer_manager::PeerManager;
use crate::progress::ProgressTracker;
use crate::validation::{receipts_match_transaction_count, validate_receipts_for_header};

const HISTORICAL_EMPTY_THRESHOLD: u32 = 5;
const HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES: u32 = 2;
const LIVE_SYNC_POLL_INTERVAL: Duration = Duration::from_secs(12);

/// The sync engine: orchestrates P2P block fetching, validation, and ingestion.
pub struct SyncEngine {
    config: SyncConfig,
    peers: PeerManager,
    storage: Arc<RwLock<PartitionManager>>,
    subscriptions: Option<SubscriptionManager>,
    sync_status: Arc<std::sync::Mutex<SyncStatus>>,
    head_tracker: HeadTracker,
    progress: ProgressTracker,
    connected_once: bool,
    shutdown: watch::Receiver<bool>,
}

impl SyncEngine {
    pub fn new(
        config: SyncConfig,
        peers: PeerManager,
        storage: Arc<RwLock<PartitionManager>>,
        subscriptions: Option<SubscriptionManager>,
        sync_status: Arc<std::sync::Mutex<SyncStatus>>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        let progress = ProgressTracker::new(Arc::clone(&sync_status));
        Self {
            config,
            peers,
            storage,
            subscriptions,
            sync_status,
            head_tracker: HeadTracker::new(256),
            progress,
            connected_once: false,
            shutdown,
        }
    }

    /// Run the sync loop: historical catch-up, then live following.
    pub async fn run(&mut self) -> Result<()> {
        let start_block = {
            let storage = self.storage.read().await;
            storage
                .sync_head()
                .map(|head| head.block_number + 1)
                .or_else(|| storage.indexed_head_block().map(|block| block + 1))
                .unwrap_or(0)
        };

        tracing::info!(start_block, "starting sync");
        self.set_runtime_state(NodeState::Discovering);

        // Wait until we have at least one connected peer. Discovery's routing
        // table is empty at startup and most discovered nodes are dead, so we
        // poll fill_peers with backoff rather than bailing on first failure.
        //
        // We pass min=1 here to return as soon as a single peer is connected,
        // so the engine can issue its first request immediately. Mainnet peers
        // disconnect us if we sit idle after the eth handshake; greedily
        // filling to max_peers before doing any work was the previous bug.
        let mut attempt: u32 = 0;
        loop {
            if self.shutdown_requested() {
                return self.finish_shutdown();
            }
            self.refresh_connectivity_state();
            if cancelable(
                &mut self.shutdown,
                self.peers.fill_peers(1, self.config.max_peers),
            )
            .await
            .is_none()
            {
                return self.finish_shutdown();
            }
            self.refresh_connectivity_state();
            if self.peers.peer_count() > 0 {
                self.connected_once = true;
                if let Some(target) = self.peers.highest_peer_block() {
                    self.progress.set_target(target);
                }
                self.refresh_connectivity_state();
                break;
            }
            attempt += 1;
            let delay = Duration::from_secs((attempt as u64).min(10));
            tracing::warn!(
                attempt,
                ?delay,
                "no peers connected yet, waiting for discovery to populate"
            );
            if cancelable(&mut self.shutdown, tokio::time::sleep(delay))
                .await
                .is_none()
            {
                return self.finish_shutdown();
            }
        }
        tracing::info!(peers = self.peers.peer_count(), "connected to peers");

        // Historical sync: batch-fetch until caught up.
        //
        // P2P peers churn constantly — they disconnect us mid-request, return
        // empty bodies, or just go away. Treat each request as best-effort:
        // on any error, top up peers and retry from the same `current` block.
        // Bailing on the first failure (the previous behavior) was fatal at
        // startup when we typically have only one or two peers.
        let mut current = start_block;
        let mut consecutive_empty: u32 = 0;
        loop {
            if self.shutdown_requested() {
                return self.finish_shutdown();
            }
            // Top up peers when we drop below half the target. We pass min=1
            // so fill_peers can add at least one more usable peer without
            // stalling toward the full target. Passing `1` here was a bug:
            // once we already had a single peer, fill_peers returned
            // immediately and we never actually replenished the pool.
            if self.peers.peer_count() < self.config.max_peers / 2 {
                let min_peers = (self.peers.peer_count() + 1).min(self.config.max_peers);
                self.refresh_connectivity_state();
                if cancelable(
                    &mut self.shutdown,
                    self.peers.fill_peers(min_peers, self.config.max_peers),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                self.refresh_connectivity_state();
            }
            if self.peers.peer_count() == 0 {
                self.refresh_connectivity_state();
                tracing::warn!("no peers available, waiting for discovery");
                if cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(Duration::from_secs(2)),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                continue;
            }
            if let Some(target) = self.peers.highest_peer_block() {
                self.progress.set_target(target);
            }
            self.refresh_connectivity_state();

            let headers = match cancelable(
                &mut self.shutdown,
                self.peers
                    .get_headers(current, self.config.header_batch_size),
            )
            .await
            {
                Some(Ok(h)) => h,
                Some(Err(e)) => {
                    self.refresh_connectivity_state();
                    tracing::debug!(error = %e, current, "header request failed, retrying");
                    continue;
                }
                None => return self.finish_shutdown(),
            };
            self.refresh_connectivity_state();
            if headers.is_empty() {
                consecutive_empty += 1;
                let target_block = self.known_target_block();
                if let Some(target_block) =
                    should_mark_historical_complete(current, target_block, consecutive_empty)
                {
                    tracing::info!(
                        current_block = current.saturating_sub(1),
                        target_block,
                        consecutive_empty,
                        "historical catch-up reached advertised peer tip"
                    );
                    self.progress.mark_synced();
                    self.sync_status_peers();
                    break;
                }

                if should_switch_to_live_without_target(current, target_block, consecutive_empty) {
                    tracing::info!(
                        current_block = current.saturating_sub(1),
                        consecutive_empty,
                        "no higher historical batches available from current serving peers, switching to head polling"
                    );
                    break;
                }

                if let Some(target_block) = target_block {
                    tracing::debug!(
                        next_block = current,
                        target_block,
                        consecutive_empty,
                        "peer returned empty headers before the advertised historical target was confirmed"
                    );
                } else if consecutive_empty == 1
                    || consecutive_empty.is_multiple_of(HISTORICAL_EMPTY_THRESHOLD)
                {
                    tracing::info!(
                        next_block = current,
                        consecutive_empty,
                        connected_peers = self.peers.peer_count(),
                        serving_peers = self.peers.serving_peer_count(),
                        pending_peers = self.peers.pending_count(),
                        "waiting for a serving peer to provide a credible sync target"
                    );
                } else {
                    tracing::debug!(
                        next_block = current,
                        consecutive_empty,
                        "peer returned empty headers while the sync target is still unknown"
                    );
                }
                continue;
            }
            consecutive_empty = 0;

            let hashes: Vec<B256> = headers.iter().map(|h| h.hash_slow()).collect();

            let fetch_size = self.config.fetch_batch_size;
            let mut chunk_failed = false;
            let mut next_block = current;
            let mut last_ingested_head: Option<Head> = None;
            for chunk_start in (0..headers.len()).step_by(fetch_size) {
                let chunk_end = (chunk_start + fetch_size).min(headers.len());
                let chunk_headers = &headers[chunk_start..chunk_end];
                let chunk_hashes = hashes[chunk_start..chunk_end].to_vec();

                let (body_peer, bodies) = match cancelable(
                    &mut self.shutdown,
                    self.peers.get_bodies(chunk_hashes.clone()),
                )
                .await
                {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => {
                        tracing::debug!(error = %e, "body request failed, retrying batch");
                        chunk_failed = true;
                        break;
                    }
                    None => return self.finish_shutdown(),
                };
                let (receipt_peer, receipts) = match cancelable(
                    &mut self.shutdown,
                    self.peers.get_receipts(chunk_hashes.clone()),
                )
                .await
                {
                    Some(Ok(r)) => r,
                    Some(Err(e)) => {
                        tracing::debug!(error = %e, "receipt request failed, retrying batch");
                        chunk_failed = true;
                        break;
                    }
                    None => return self.finish_shutdown(),
                };

                if bodies.len() != chunk_headers.len() || receipts.len() != chunk_headers.len() {
                    tracing::warn!(
                        headers = chunk_headers.len(),
                        bodies = bodies.len(),
                        receipts = receipts.len(),
                        "peer returned mismatched counts, retrying from last ingested block"
                    );
                    chunk_failed = true;
                    break;
                }

                for (i, header) in chunk_headers.iter().enumerate() {
                    let block_hash = chunk_hashes[i];
                    let block_number = header.number();
                    let timestamp = header.timestamp();
                    let parent_hash = header.parent_hash();

                    if bodies[i].calculate_tx_root() != header.transactions_root() {
                        tracing::warn!(
                            block_number,
                            %block_hash,
                            body_peer = %body_peer,
                            "block body transaction root mismatch — retrying from last ingested block"
                        );
                        self.peers
                            .report_invalid_block_data(body_peer, "block bodies");
                        chunk_failed = true;
                        break;
                    }

                    if !receipts_match_transaction_count(&bodies[i], &receipts[i]) {
                        tracing::warn!(
                            block_number,
                            %block_hash,
                            receipt_peer = %receipt_peer,
                            transactions = bodies[i].transaction_count(),
                            receipts = receipts[i].len(),
                            "block body / receipt count mismatch — retrying from last ingested block"
                        );
                        self.peers
                            .report_invalid_block_data(receipt_peer, "receipts");
                        chunk_failed = true;
                        break;
                    }

                    if let Err(error) = validate_receipts_for_header(header, &receipts[i]) {
                        tracing::warn!(
                            block_number,
                            %block_hash,
                            receipt_peer = %receipt_peer,
                            %error,
                            "receipt validation failed — retrying from last ingested block"
                        );
                        self.peers
                            .report_invalid_block_data(receipt_peer, "receipts");
                        chunk_failed = true;
                        break;
                    }

                    // Extract tx hashes from block body, zip with receipt logs
                    let txs = assemble_txs(&bodies[i], &receipts[i]);

                    if let Some(reorg) =
                        self.head_tracker
                            .track(block_number, block_hash, parent_hash)
                    {
                        self.handle_reorg(reorg).await?;
                    }

                    let log_count = self
                        .ingest_block(block_number, block_hash, timestamp, &txs)
                        .await?;
                    self.progress.record_block(block_number, log_count);
                    next_block = block_number + 1;
                    last_ingested_head = Some(Head {
                        number: block_number,
                        hash: block_hash,
                        timestamp,
                        ..Default::default()
                    });
                }

                if chunk_failed {
                    break;
                }
            }

            if let Some(head) = last_ingested_head {
                self.peers.set_head(head);
            }
            current = next_block;

            if chunk_failed {
                continue;
            }
        }

        tracing::info!(
            current_block = self.current_block(),
            target_block = self.known_target_block(),
            "polling peers for new canonical blocks"
        );
        self.run_live_sync().await
    }

    /// Follow the chain head, ingesting new blocks as they arrive.
    async fn run_live_sync(&mut self) -> Result<()> {
        loop {
            if cancelable(
                &mut self.shutdown,
                tokio::time::sleep(LIVE_SYNC_POLL_INTERVAL),
            )
            .await
            .is_none()
            {
                return self.finish_shutdown();
            }

            if self.peers.peer_count() < 3 {
                let min_peers = (self.peers.peer_count() + 1).min(self.config.max_peers);
                self.refresh_connectivity_state();
                if cancelable(
                    &mut self.shutdown,
                    self.peers.fill_peers(min_peers, self.config.max_peers),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                self.refresh_connectivity_state();
            }
            if self.peers.peer_count() == 0 {
                self.refresh_connectivity_state();
                continue;
            }
            if let Some(target) = self.peers.highest_peer_block() {
                self.progress.set_target(target);
            }

            let current = {
                let status = self.sync_status.lock().unwrap();
                status.current_block
            };

            let headers = match cancelable(
                &mut self.shutdown,
                self.peers.get_headers(current + 1, 16),
            )
            .await
            {
                Some(Ok(h)) if !h.is_empty() => {
                    self.refresh_connectivity_state();
                    h
                }
                Some(Ok(_)) => {
                    if self.try_mark_synced("caught up to advertised peer tip") {
                        self.sync_status_peers();
                    } else {
                        self.refresh_connectivity_state();
                        tracing::debug!(
                            current_block = current,
                            target_block = self.known_target_block(),
                            "live head poll returned no headers while waiting for a better peer response"
                        );
                    }
                    continue;
                }
                Some(Err(e)) => {
                    self.refresh_connectivity_state();
                    tracing::debug!(error = %e, current, "live header request failed");
                    continue;
                }
                None => return self.finish_shutdown(),
            };

            let hashes: Vec<B256> = headers.iter().map(|h| h.hash_slow()).collect();

            let (body_peer, bodies) =
                match cancelable(&mut self.shutdown, self.peers.get_bodies(hashes.clone())).await {
                    Some(Ok((peer_id, bodies))) if bodies.len() == headers.len() => {
                        (peer_id, bodies)
                    }
                    Some(Ok(_)) | Some(Err(_)) => continue,
                    None => return self.finish_shutdown(),
                };
            let (receipt_peer, receipts) =
                match cancelable(&mut self.shutdown, self.peers.get_receipts(hashes.clone())).await
                {
                    Some(Ok((peer_id, receipts))) if receipts.len() == headers.len() => {
                        (peer_id, receipts)
                    }
                    Some(Ok(_)) | Some(Err(_)) => continue,
                    None => return self.finish_shutdown(),
                };

            let mut batch_failed = false;
            for (i, header) in headers.iter().enumerate() {
                let block_hash = hashes[i];
                let block_number = header.number();
                let timestamp = header.timestamp();
                let parent_hash = header.parent_hash();

                if bodies[i].calculate_tx_root() != header.transactions_root() {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        body_peer = %body_peer,
                        "block body transaction root mismatch in live sync — retrying from current head"
                    );
                    self.peers
                        .report_invalid_block_data(body_peer, "block bodies");
                    batch_failed = true;
                    break;
                }

                if !receipts_match_transaction_count(&bodies[i], &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        transactions = bodies[i].transaction_count(),
                        receipts = receipts[i].len(),
                        "block body / receipt count mismatch in live sync — retrying from current head"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    batch_failed = true;
                    break;
                }

                if let Err(error) = validate_receipts_for_header(header, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        %error,
                        "receipt validation failed in live sync — retrying from current head"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    batch_failed = true;
                    break;
                }

                let txs = assemble_txs(&bodies[i], &receipts[i]);

                if let Some(reorg) = self
                    .head_tracker
                    .track(block_number, block_hash, parent_hash)
                {
                    self.handle_reorg(reorg).await?;
                }

                let log_count = self
                    .ingest_block(block_number, block_hash, timestamp, &txs)
                    .await?;
                self.progress.record_block(block_number, log_count);

                self.peers.set_head(Head {
                    number: block_number,
                    hash: block_hash,
                    timestamp,
                    ..Default::default()
                });
            }

            if batch_failed {
                continue;
            }

            if self.try_mark_synced("caught up to advertised peer tip") {
                self.sync_status_peers();
            } else {
                self.refresh_connectivity_state();
            }
        }
    }

    /// Write a block's logs to storage and notify subscribers.
    async fn ingest_block(
        &self,
        block_number: u64,
        block_hash: B256,
        timestamp: u64,
        txs: &[(B256, Vec<Log>)],
    ) -> Result<u64> {
        let rows = extract::extract_from_block(block_number, block_hash, timestamp, txs);
        let count = rows.len() as u64;

        let mut storage = self.storage.write().await;
        if !rows.is_empty() {
            let sealed_before = storage.sealed_count();
            storage
                .write_batch(&rows)
                .map_err(|e| eyre::eyre!("storage write error: {e}"))?;
            let sealed_after = storage.sealed_count();

            for partition in &storage.sealed_partitions()[sealed_before..sealed_after] {
                if let Err(e) = IndexBuilder::build_all_indexes(&partition.meta.path) {
                    tracing::warn!(
                        error = %e,
                        partition_id = partition.meta.id,
                        "failed to build indexes for sealed partition"
                    );
                }
            }

            if let Some(ref subs) = self.subscriptions {
                subs.notify(&rows);
            }
        }
        storage
            .record_sync_head(block_number, block_hash, timestamp)
            .map_err(|e| eyre::eyre!("storage metadata error: {e}"))?;

        Ok(count)
    }

    /// Handle a detected reorg: mark reverted blocks non-canonical.
    async fn handle_reorg(&self, reorg: ReorgInfo) -> Result<()> {
        if reorg.reverted_hashes.is_empty() {
            return Ok(());
        }

        let storage = self.storage.write().await;
        let mut total_reverted = 0u64;
        for hash in &reorg.reverted_hashes {
            total_reverted += storage
                .mark_non_canonical(*hash)
                .map_err(|e| eyre::eyre!("reorg error: {e}"))?;
        }

        tracing::info!(
            fork_block = reorg.fork_block,
            reverted_blocks = reorg.reverted_hashes.len(),
            reverted_rows = total_reverted,
            "handled reorg"
        );
        Ok(())
    }

    fn sync_status_peers(&self) {
        let state = {
            let status = self.sync_status.lock().unwrap();
            status.node_state
        };
        self.progress.update_network_state(
            state,
            self.peers.peer_count(),
            self.peers.serving_peer_count(),
            self.peers.pending_count(),
        );
    }

    fn set_runtime_state(&self, state: NodeState) {
        self.progress.update_network_state(
            state,
            self.peers.peer_count(),
            self.peers.serving_peer_count(),
            self.peers.pending_count(),
        );
    }

    fn refresh_connectivity_state(&self) {
        let (current_block, target_block) = self.sync_cursor();
        let state = runtime_state_for_connectivity(
            self.connected_once,
            self.peers.peer_count(),
            self.peers.serving_peer_count(),
            self.peers.pending_count(),
            current_block,
            target_block,
        );

        self.set_runtime_state(state);
    }

    fn sync_cursor(&self) -> (u64, u64) {
        let status = self.sync_status.lock().unwrap();
        (status.current_block, status.target_block)
    }

    fn current_block(&self) -> u64 {
        self.sync_cursor().0
    }

    fn known_target_block(&self) -> Option<u64> {
        let (_, target_block) = self.sync_cursor();
        (target_block > 0).then_some(target_block)
    }

    fn try_mark_synced(&self, reason: &'static str) -> bool {
        let (current_block, target_block) = self.sync_cursor();
        if target_block == 0 || current_block < target_block {
            return false;
        }

        let already_synced = {
            let status = self.sync_status.lock().unwrap();
            status.node_state == NodeState::Synced
        };

        self.progress.mark_synced();
        if !already_synced {
            tracing::info!(current_block, target_block, reason, "sync caught up");
        }
        true
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown.has_changed().unwrap_or(true)
    }

    fn finish_shutdown(&self) -> Result<()> {
        tracing::info!("shutdown requested, stopping sync engine");
        let mut status = self.sync_status.lock().unwrap();
        status.syncing = false;
        status.eta_seconds = None;
        Ok(())
    }

    pub fn known_peers(&self) -> Vec<NodeRecord> {
        self.peers.known_peers()
    }

    pub async fn shutdown(&mut self) {
        self.peers.shutdown().await;
    }
}

fn should_mark_historical_complete(
    next_block: u64,
    target_block: Option<u64>,
    consecutive_empty: u32,
) -> Option<u64> {
    let target_block = target_block?;
    (next_block > target_block && consecutive_empty >= HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES)
        .then_some(target_block)
}

fn should_switch_to_live_without_target(
    next_block: u64,
    target_block: Option<u64>,
    consecutive_empty: u32,
) -> bool {
    target_block.is_none() && next_block > 1 && consecutive_empty >= HISTORICAL_EMPTY_THRESHOLD
}

fn runtime_state_for_connectivity(
    connected_once: bool,
    connected_peers: usize,
    serving_peers: usize,
    pending_peers: usize,
    current_block: u64,
    target_block: u64,
) -> NodeState {
    if serving_peers > 0 {
        if target_block > 0 && current_block >= target_block {
            NodeState::Synced
        } else {
            NodeState::Syncing
        }
    } else if connected_peers > 0 {
        NodeState::Connecting
    } else if connected_once {
        if pending_peers > 0 {
            NodeState::Reconnecting
        } else {
            NodeState::Disconnected
        }
    } else if pending_peers > 0 {
        NodeState::Connecting
    } else {
        NodeState::Discovering
    }
}

async fn cancelable<T>(
    shutdown: &mut watch::Receiver<bool>,
    future: impl Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        changed = shutdown.changed() => {
            let _ = changed;
            None
        }
        result = future => Some(result),
    }
}

/// Zip block body tx hashes with receipt logs into the format the ingestion pipeline expects.
fn assemble_txs<B, R>(body: &B, receipts: &[R]) -> Vec<(B256, Vec<Log>)>
where
    B: reth_primitives_traits::BlockBody,
    B::Transaction: SignedTransaction,
    R: TxReceipt<Log = Log>,
{
    body.transactions()
        .iter()
        .zip(receipts.iter())
        .map(|(tx, receipt)| {
            let tx_hash = *tx.tx_hash();
            let logs = receipt.logs().to_vec();
            (tx_hash, logs)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn historical_completion_requires_known_target_and_confirmed_empty_responses() {
        assert_eq!(
            should_mark_historical_complete(101, Some(100), HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES),
            Some(100)
        );
        assert_eq!(
            should_mark_historical_complete(100, Some(100), HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES),
            None
        );
        assert_eq!(
            should_mark_historical_complete(101, None, HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES),
            None
        );
    }

    #[test]
    fn historical_fallback_to_live_requires_real_progress_without_target() {
        assert!(should_switch_to_live_without_target(
            2,
            None,
            HISTORICAL_EMPTY_THRESHOLD
        ));
        assert!(!should_switch_to_live_without_target(
            1,
            None,
            HISTORICAL_EMPTY_THRESHOLD
        ));
        assert!(!should_switch_to_live_without_target(
            2,
            Some(10),
            HISTORICAL_EMPTY_THRESHOLD
        ));
    }

    #[test]
    fn runtime_state_only_reports_synced_when_caught_up_to_known_tip() {
        assert_eq!(
            runtime_state_for_connectivity(false, 0, 0, 0, 0, 0),
            NodeState::Discovering
        );
        assert_eq!(
            runtime_state_for_connectivity(false, 1, 0, 0, 0, 0),
            NodeState::Connecting
        );
        assert_eq!(
            runtime_state_for_connectivity(true, 0, 0, 1, 0, 0),
            NodeState::Reconnecting
        );
        assert_eq!(
            runtime_state_for_connectivity(true, 1, 1, 0, 50, 100),
            NodeState::Syncing
        );
        assert_eq!(
            runtime_state_for_connectivity(true, 1, 1, 0, 100, 100),
            NodeState::Synced
        );
    }
}
