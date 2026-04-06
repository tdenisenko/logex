use std::sync::Arc;
use std::time::Duration;

use alloy_consensus::{BlockHeader, TxReceipt, transaction::TxHashRef};
use alloy_primitives::{B256, Log};
use eyre::Result;
use reth_ethereum_forks::Head;
use reth_primitives_traits::SignedTransaction;
use tokio::sync::RwLock;

use logex_index::IndexBuilder;
use logex_ingestion::extract;
use logex_server::SubscriptionManager;
use logex_storage::PartitionManager;
use logex_types::SyncStatus;

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
}

impl SyncEngine {
    pub fn new(
        config: SyncConfig,
        peers: PeerManager,
        storage: Arc<RwLock<PartitionManager>>,
        subscriptions: Option<SubscriptionManager>,
        sync_status: Arc<std::sync::Mutex<SyncStatus>>,
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
        }
    }

    /// Run the sync loop: historical catch-up, then live following.
    pub async fn run(&mut self) -> Result<()> {
        let start_block = {
            let storage = self.storage.read().await;
            storage.head_block().map(|b| b + 1).unwrap_or(0)
        };

        tracing::info!(start_block, "starting sync");

        // Ensure we have peers
        self.peers.fill_peers(self.config.max_peers).await;
        if self.peers.peer_count() == 0 {
            tracing::warn!("no peers found, waiting...");
            tokio::time::sleep(Duration::from_secs(5)).await;
            self.peers.fill_peers(self.config.max_peers).await;
            if self.peers.peer_count() == 0 {
                eyre::bail!("unable to connect to any peers");
            }
        }
        tracing::info!(peers = self.peers.peer_count(), "connected to peers");

        // Historical sync: batch-fetch until caught up
        let mut current = start_block;
        loop {
            if self.peers.peer_count() < self.config.max_peers / 2 {
                self.peers.fill_peers(self.config.max_peers).await;
            }

            let headers = self
                .peers
                .get_headers(current, self.config.header_batch_size)
                .await?;
            if headers.is_empty() {
                tracing::info!(
                    block = current.saturating_sub(1),
                    "historical sync complete"
                );
                self.progress.mark_synced();
                break;
            }

            let hashes: Vec<B256> = headers.iter().map(|h| h.hash_slow()).collect();

            let fetch_size = self.config.fetch_batch_size;
            for chunk_start in (0..headers.len()).step_by(fetch_size) {
                let chunk_end = (chunk_start + fetch_size).min(headers.len());
                let chunk_headers = &headers[chunk_start..chunk_end];
                let chunk_hashes = hashes[chunk_start..chunk_end].to_vec();

                let bodies = self.peers.get_bodies(chunk_hashes.clone()).await?;
                let receipts = self.peers.get_receipts(chunk_hashes.clone()).await?;

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

            current = headers.last().map(|h| h.number() + 1).unwrap_or(current);
        }

        tracing::info!("entering live sync mode");
        self.run_live_sync().await
    }

    /// Follow the chain head, ingesting new blocks as they arrive.
    async fn run_live_sync(&mut self) -> Result<()> {
        loop {
            tokio::time::sleep(Duration::from_secs(12)).await;

            if self.peers.peer_count() < 3 {
                self.peers.fill_peers(self.config.max_peers).await;
            }

            let current = {
                let status = self.sync_status.lock().unwrap();
                status.current_block
            };

            let headers = match self.peers.get_headers(current + 1, 16).await {
                Ok(h) if !h.is_empty() => h,
                _ => continue,
            };

            let hashes: Vec<B256> = headers.iter().map(|h| h.hash_slow()).collect();

            let bodies = match self.peers.get_bodies(hashes.clone()).await {
                Ok(b) if b.len() == headers.len() => b,
                _ => continue,
            };
            let receipts = match self.peers.get_receipts(hashes.clone()).await {
                Ok(r) if r.len() == headers.len() => r,
                _ => continue,
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
                    ..Default::default()
                });
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

        if !rows.is_empty() {
            let mut storage = self.storage.write().await;
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
