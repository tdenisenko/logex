use super::*;
use std::future::Future;

impl SyncEngine {
    pub(super) fn sync_status_peers(&self) {
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

    pub(super) fn set_runtime_state(&self, state: NodeState) {
        self.progress.update_network_state(
            state,
            self.peers.peer_count(),
            self.peers.serving_peer_count(),
            self.peers.pending_count(),
        );
    }

    pub(super) fn refresh_connectivity_state(&self) {
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

    pub(super) fn sync_cursor(&self) -> (u64, u64) {
        let status = self.sync_status.lock().unwrap();
        (status.current_block, status.target_block)
    }

    pub(super) fn current_block(&self) -> u64 {
        self.sync_cursor().0
    }

    pub(super) fn expected_parent_for_validation(
        &self,
        expected_start_block: u64,
    ) -> Option<&Header> {
        self.last_validated_header
            .as_ref()
            .filter(|header| header.number() + 1 == expected_start_block)
    }

    pub(super) fn known_target_block(&self) -> Option<u64> {
        let (_, target_block) = self.sync_cursor();
        (target_block > 0).then_some(target_block)
    }

    pub(super) fn try_mark_synced(&self, reason: &'static str) -> bool {
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

    pub(super) fn shutdown_requested(&self) -> bool {
        self.shutdown.has_changed().unwrap_or(true)
    }

    pub(super) fn finish_shutdown(&self) -> Result<()> {
        tracing::info!("shutdown requested, stopping sync engine");
        let mut status = self.sync_status.lock().unwrap();
        status.syncing = false;
        status.eta_seconds = None;
        Ok(())
    }

    pub(super) fn note_serving_peer(
        &mut self,
        peer_id: PeerId,
        newly_serving: &mut HashSet<PeerId>,
    ) {
        if !should_note_serving_peer(peer_id, newly_serving) {
            return;
        }

        if self.peers.report_valid_serving_peer(peer_id) {
            self.sync_status_peers();
        }
    }
}

pub(super) fn should_mark_historical_complete(
    next_block: u64,
    target_block: Option<u64>,
    consecutive_empty: u32,
) -> Option<u64> {
    let target_block = target_block?;
    (next_block > target_block && consecutive_empty >= HISTORICAL_TIP_CONFIRM_EMPTY_RESPONSES)
        .then_some(target_block)
}

pub(super) fn should_switch_to_live_without_target(
    next_block: u64,
    target_block: Option<u64>,
    consecutive_empty: u32,
) -> bool {
    target_block.is_none() && next_block > 1 && consecutive_empty >= HISTORICAL_EMPTY_THRESHOLD
}

pub(super) fn runtime_state_for_connectivity(
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

pub(super) async fn cancelable<T>(
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

/// Zip block body tx hashes with receipt logs into the format the engine
/// expects for log extraction.
pub(super) fn assemble_txs<B, R>(body: &B, receipts: &[R]) -> Vec<(B256, Vec<Log>)>
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

pub(super) fn refill_peer_floor(max_peers: usize) -> usize {
    max_peers.clamp(1, MIN_ACTIVE_SYNC_PEERS)
}

pub(super) fn desired_refill_min_peers(connected_peers: usize, max_peers: usize) -> usize {
    (connected_peers + 1)
        .max(refill_peer_floor(max_peers))
        .min(max_peers)
}

pub(super) fn should_note_serving_peer(
    peer_id: PeerId,
    newly_serving: &mut HashSet<PeerId>,
) -> bool {
    peer_id != PeerId::ZERO && newly_serving.insert(peer_id)
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
        assert_eq!(
            runtime_state_for_connectivity(true, 1, 1, 0, 100, 0),
            NodeState::Syncing
        );
    }

    #[test]
    fn desired_refill_min_peers_maintains_a_small_live_floor() {
        assert_eq!(desired_refill_min_peers(0, 50), 4);
        assert_eq!(desired_refill_min_peers(1, 50), 4);
        assert_eq!(desired_refill_min_peers(3, 50), 4);
        assert_eq!(desired_refill_min_peers(4, 50), 5);
        assert_eq!(desired_refill_min_peers(0, 2), 2);
    }

    #[test]
    fn serving_peer_notifications_ignore_zero_and_duplicates() {
        let first = PeerId::repeat_byte(0x11);
        let mut seen = HashSet::new();

        assert!(should_note_serving_peer(first, &mut seen));
        assert!(!should_note_serving_peer(first, &mut seen));
        assert!(!should_note_serving_peer(PeerId::ZERO, &mut seen));
    }
}
