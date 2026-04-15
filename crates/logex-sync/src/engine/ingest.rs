use super::*;
use crate::extract;
use logex_index::IndexBuilder;

impl SyncEngine {
    /// Write a block's logs to storage and notify subscribers.
    pub(super) async fn ingest_block(
        &self,
        header: &Header,
        block_hash: B256,
        txs: &[(B256, Vec<Log>)],
        recent_headers: &[Header],
    ) -> Result<u64> {
        let block_number = header.number();
        let timestamp = header.timestamp();
        let rows = extract::extract_from_block(block_number, block_hash, timestamp, txs);
        let count = rows.len() as u64;

        let mut storage = self.storage.write().await;
        if !rows.is_empty() {
            let sealed_before = storage.sealed_count();
            storage
                .write_batch(&rows)
                .map_err(|e| eyre::eyre!("storage write error: {e}"))?;
            let sealed_after = storage.sealed_count();

            let sealed_segments: Vec<_> = storage.sealed_partitions()[sealed_before..sealed_after]
                .iter()
                .map(|partition| (partition.meta.id, partition.meta.path.clone()))
                .collect();

            for (segment_id, segment_path) in sealed_segments {
                if let Err(e) = IndexBuilder::build_all_indexes(&segment_path) {
                    tracing::warn!(
                        error = %e,
                        partition_id = segment_id,
                        "failed to build indexes for sealed partition"
                    );
                    continue;
                }

                if let Err(e) = storage.refresh_segment_indexes(segment_id) {
                    tracing::warn!(
                        error = %e,
                        partition_id = segment_id,
                        "failed to refresh segment manifest after index build"
                    );
                }
            }

            if let Some(ref subs) = self.subscriptions {
                subs.notify(&rows);
            }
        }
        storage
            .record_canonical_state(header, recent_headers)
            .map_err(|e| eyre::eyre!("storage metadata error: {e}"))?;

        Ok(count)
    }

    /// Handle a detected reorg: mark reverted blocks non-canonical.
    pub(super) async fn handle_reorg(&self, reorg: ReorgInfo) -> Result<()> {
        if reorg.reverted_hashes.is_empty() {
            return Ok(());
        }

        self.peers.remove_cached_blocks(&reorg.reverted_hashes);

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
