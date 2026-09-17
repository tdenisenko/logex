use super::*;

impl SyncEngine {
    /// Restore the local header window and run consensus-anchored synchronization.
    pub async fn run(&mut self) -> Result<()> {
        let (start_block, recent_headers) = {
            let storage = self.storage.read().await;
            (
                storage
                    .sync_head()
                    .map(|head| head.block_number + 1)
                    .or_else(|| storage.indexed_head_block().map(|block| block + 1))
                    .unwrap_or(0),
                storage.recent_headers().to_vec(),
            )
        };

        if !recent_headers.is_empty() {
            self.head_tracker.restore(recent_headers.clone());
            self.peers.cache_canonical_headers(recent_headers.clone());
            self.last_validated_header = recent_headers.last().cloned();
            tracing::info!(
                restored_headers = recent_headers.len(),
                restored_tip = self.head_tracker.tip().map(|(number, _)| number),
                "restored recent canonical header window from storage"
            );
        }

        tracing::info!(start_block, "starting sync");

        self.run_consensus_sync().await
    }
}
