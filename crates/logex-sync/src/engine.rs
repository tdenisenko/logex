use alloy_consensus::{BlockHeader, TxReceipt, transaction::TxHashRef};
use alloy_primitives::{B256, Log};
use eyre::Result;
use reth_ethereum_forks::Head;
use reth_network_peers::NodeRecord;
use reth_primitives_traits::SignedTransaction;
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
use crate::validation::validate_receipt_root;

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
            storage.head_block().map(|b| b + 1).unwrap_or(0)
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
                self.set_runtime_state(NodeState::Syncing);
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
        // Number of consecutive empty header responses we've gotten. A single
        // empty response doesn't mean we're caught up — peers routinely return
        // empty bodies when they're load-shedding, syncing, or just being
        // uncooperative. Only treat the chain as caught up after several
        // consecutive empty responses, ideally from different peers (the
        // peer rotation in send_request_and_receive gives us this naturally).
        const EMPTY_THRESHOLD: u32 = 5;
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
            self.set_runtime_state(NodeState::Syncing);

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
            if headers.is_empty() {
                consecutive_empty += 1;
                if consecutive_empty >= EMPTY_THRESHOLD {
                    tracing::info!(
                        block = current.saturating_sub(1),
                        consecutive_empty,
                        "historical sync complete"
                    );
                    self.progress.mark_synced();
                    break;
                }
                tracing::debug!(
                    current,
                    consecutive_empty,
                    "peer returned empty headers, retrying"
                );
                continue;
            }
            consecutive_empty = 0;

            let hashes: Vec<B256> = headers.iter().map(|h| h.hash_slow()).collect();

            let fetch_size = self.config.fetch_batch_size;
            let mut chunk_failed = false;
            for chunk_start in (0..headers.len()).step_by(fetch_size) {
                let chunk_end = (chunk_start + fetch_size).min(headers.len());
                let chunk_headers = &headers[chunk_start..chunk_end];
                let chunk_hashes = hashes[chunk_start..chunk_end].to_vec();

                let bodies = match cancelable(
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
                let receipts = match cancelable(
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
                        "peer returned mismatched counts, skipping batch"
                    );
                    continue;
                }

                for (i, header) in chunk_headers.iter().enumerate() {
                    let block_hash = chunk_hashes[i];
                    let block_number = header.number();
                    let timestamp = header.timestamp();
                    let parent_hash = header.parent_hash();

                    // Trustless verification: the header's receipts_root is consensus-attested,
                    // so recomputing the trie from the receipts a peer sent us proves they're
                    // genuine. Mismatches mean the peer is lying or buggy — drop the batch.
                    if !validate_receipt_root(&receipts[i], header.receipts_root()) {
                        tracing::warn!(
                            block_number,
                            %block_hash,
                            "receipt root mismatch — discarding batch from peer"
                        );
                        continue;
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
                }
            }

            // Only advance `current` if every chunk in the batch succeeded.
            // A failed chunk means we already top-up peers next iteration and
            // re-request from the same starting block.
            if !chunk_failed && let Some(last) = headers.last() {
                // Tell new peer handshakes how far we are. Without this we
                // keep advertising head=0 forever, which makes peers treat
                // us like a fresh useless node and disconnect us early.
                self.peers.set_head(Head {
                    number: last.number(),
                    hash: last.hash_slow(),
                    timestamp: last.timestamp(),
                    ..Default::default()
                });
                current = last.number() + 1;
            }
        }

        tracing::info!("entering live sync mode");
        self.run_live_sync().await
    }

    /// Follow the chain head, ingesting new blocks as they arrive.
    async fn run_live_sync(&mut self) -> Result<()> {
        loop {
            if cancelable(
                &mut self.shutdown,
                tokio::time::sleep(Duration::from_secs(12)),
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

            let headers =
                match cancelable(&mut self.shutdown, self.peers.get_headers(current + 1, 16)).await
                {
                    Some(Ok(h)) if !h.is_empty() => {
                        self.set_runtime_state(NodeState::Syncing);
                        h
                    }
                    Some(Ok(_)) => {
                        self.progress.mark_synced();
                        self.sync_status_peers();
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

            let bodies =
                match cancelable(&mut self.shutdown, self.peers.get_bodies(hashes.clone())).await {
                    Some(Ok(b)) if b.len() == headers.len() => b,
                    Some(Ok(_)) | Some(Err(_)) => continue,
                    None => return self.finish_shutdown(),
                };
            let receipts =
                match cancelable(&mut self.shutdown, self.peers.get_receipts(hashes.clone())).await
                {
                    Some(Ok(r)) if r.len() == headers.len() => r,
                    Some(Ok(_)) | Some(Err(_)) => continue,
                    None => return self.finish_shutdown(),
                };

            for (i, header) in headers.iter().enumerate() {
                let block_hash = hashes[i];
                let block_number = header.number();
                let timestamp = header.timestamp();
                let parent_hash = header.parent_hash();

                if !validate_receipt_root(&receipts[i], header.receipts_root()) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        "receipt root mismatch in live sync — skipping block"
                    );
                    continue;
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

            self.progress.mark_synced();
            self.sync_status_peers();
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
            .record_sync_head(block_number, block_hash)
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
            self.peers.pending_count(),
        );
    }

    fn set_runtime_state(&self, state: NodeState) {
        self.progress.update_network_state(
            state,
            self.peers.peer_count(),
            self.peers.pending_count(),
        );
    }

    fn refresh_connectivity_state(&self) {
        let state = if self.peers.peer_count() > 0 {
            NodeState::Syncing
        } else if self.connected_once {
            if self.peers.pending_count() > 0 {
                NodeState::Reconnecting
            } else {
                NodeState::Disconnected
            }
        } else if self.peers.pending_count() > 0 {
            NodeState::Connecting
        } else {
            NodeState::Discovering
        };

        self.set_runtime_state(state);
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
