use super::*;
use alloy_primitives::U256;
use logex_cl::MAINNET_CONSENSUS_CHAIN_SPEC;
use logex_types::ExecutionAnchor;
use reth_chainspec::{EthChainSpec, MAINNET};
use std::future::Future;

impl SyncEngine {
    fn execution_network_status(&self) -> logex_types::ExecutionNetworkStatus {
        let mut status = self.peers.execution_network_status();
        status.historical_fetch_active = self.historical_fetch_handles.len();
        status.historical_fetch_completed = self.historical_fetch_completed.len();
        status.historical_fetch_pending =
            status.historical_fetch_active + status.historical_fetch_completed;
        status.historical_fetch_expected_sequence = self.historical_fetch_expected_sequence;
        status.historical_fetch_next_sequence = self.historical_fetch_next_sequence;
        status.historical_prepare_active = self.historical_prepare_handles.len();
        status.historical_prepare_ready = self
            .historical_prepare_handles
            .values()
            .filter(|task| task.handle.is_finished())
            .count()
            + self.historical_prepare_completed.len();
        status.historical_prepare_completed = self.historical_prepare_completed.len();
        status.historical_prepare_pending =
            status.historical_prepare_active + status.historical_prepare_completed;
        status.historical_prepare_expected_sequence = self.historical_prepare_expected_sequence;
        status.historical_ingest_active = self.historical_ingest_sequence.is_some();
        status.historical_ingest_sequence = self.historical_ingest_sequence;
        status.historical_ingest_elapsed_ms = self
            .historical_ingest_started_at
            .map(|started_at| started_at.elapsed().as_millis() as u64);
        status
    }

    pub(super) fn sync_status_peers(&self) {
        let state = {
            let status = self.sync_status.lock().unwrap();
            status.node_state
        };
        self.progress
            .update_execution_network_state(self.execution_network_status());
        self.progress.update_network_state(
            state,
            self.peers.peer_count(),
            self.peers.serving_peer_count(),
            self.peers.pending_count(),
        );
    }

    pub(super) fn set_runtime_state(&self, state: NodeState) {
        self.progress
            .update_execution_network_state(self.execution_network_status());
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

    pub(super) fn set_peer_head_from_consensus(&mut self) -> bool {
        let Some(anchor) = self
            .consensus
            .as_ref()
            .and_then(|consensus| consensus.anchor_coverage().ceiling)
        else {
            return false;
        };

        self.peers.set_head(consensus_anchor_head(anchor));
        true
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
        {
            let status = self.sync_status.lock().unwrap();
            if !status.historical_sync_disabled
                && status
                    .historical_execution_floor
                    .is_some_and(|floor| floor.block_number > status.historical_target_block)
            {
                return false;
            }
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
        shutdown_requested(&self.shutdown)
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

pub(super) fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow()
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

pub(super) fn historical_backfill_peer_floor(max_peers: usize) -> usize {
    if max_peers == 0 {
        return 0;
    }

    (max_peers / 3)
        .clamp(1, HISTORICAL_BACKFILL_CONNECTED_PEER_FLOOR_CAP)
        .min(max_peers)
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
    max_peers.clamp(1, TARGET_ACTIVE_SYNC_PEERS.max(MIN_ACTIVE_SYNC_PEERS))
}

pub(super) fn nonblocking_refill_min_peers(connected_peers: usize, max_peers: usize) -> usize {
    connected_peers
        .saturating_add(PEER_REFILL_STEP)
        .clamp(1, max_peers)
}

pub(super) fn active_refill_min_peers(max_peers: usize) -> usize {
    refill_peer_floor(max_peers).min(max_peers)
}

pub(super) fn peer_refill_goal(
    connected_peers: usize,
    serving_peers: usize,
    max_peers: usize,
) -> Option<usize> {
    if max_peers == 0 {
        return None;
    }

    if serving_peers < MIN_ACTIVE_SYNC_PEERS && connected_peers < max_peers {
        return Some(active_refill_min_peers(max_peers));
    }

    if connected_peers < max_peers / 2 {
        return Some(nonblocking_refill_min_peers(connected_peers, max_peers));
    }

    None
}

pub(super) fn should_note_serving_peer(
    peer_id: PeerId,
    newly_serving: &mut HashSet<PeerId>,
) -> bool {
    peer_id != PeerId::ZERO && newly_serving.insert(peer_id)
}

pub(super) fn preferred_body_peers<B>(
    bodies: &[(PeerId, B)],
    fallback_peer: PeerId,
) -> Vec<PeerId> {
    let mut peers = Vec::with_capacity(bodies.len().saturating_add(1));
    for (peer_id, _) in bodies {
        if *peer_id != PeerId::ZERO && !peers.contains(peer_id) {
            peers.push(*peer_id);
        }
    }
    if fallback_peer != PeerId::ZERO && !peers.contains(&fallback_peer) {
        peers.push(fallback_peer);
    }
    peers
}

pub(super) fn consensus_anchor_head(anchor: ExecutionAnchor) -> Head {
    execution_head(
        anchor.block_number,
        anchor.block_hash,
        MAINNET_CONSENSUS_CHAIN_SPEC
            .genesis_time
            .saturating_add(anchor.beacon_slot.saturating_mul(12)),
    )
}

pub(super) fn execution_head(number: u64, hash: B256, timestamp: u64) -> Head {
    Head {
        number,
        hash,
        timestamp,
        difficulty: if number == 0 {
            MAINNET.genesis().difficulty
        } else {
            U256::ZERO
        },
        total_difficulty: if number == 0 {
            MAINNET.genesis().difficulty
        } else {
            MAINNET
                .final_paris_total_difficulty()
                .unwrap_or(MAINNET.genesis().difficulty)
        },
    }
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
    fn historical_backfill_peer_floor_scales_with_configured_pool() {
        assert_eq!(historical_backfill_peer_floor(0), 0);
        assert_eq!(historical_backfill_peer_floor(1), 1);
        assert_eq!(historical_backfill_peer_floor(8), 2);
        assert_eq!(historical_backfill_peer_floor(100), 4);
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
    fn nonblocking_refill_min_peers_grows_pool_incrementally() {
        assert_eq!(nonblocking_refill_min_peers(0, 50), 16);
        assert_eq!(nonblocking_refill_min_peers(1, 50), 17);
        assert_eq!(nonblocking_refill_min_peers(7, 50), 23);
        assert_eq!(nonblocking_refill_min_peers(50, 50), 50);
        assert_eq!(nonblocking_refill_min_peers(0, 2), 2);
    }

    #[test]
    fn active_refill_min_peers_uses_sync_target() {
        assert_eq!(active_refill_min_peers(100), 80);
        assert_eq!(active_refill_min_peers(50), 50);
        assert_eq!(active_refill_min_peers(8), 8);
        assert_eq!(active_refill_min_peers(2), 2);
    }

    #[test]
    fn peer_refill_goal_refills_toward_active_pool() {
        assert_eq!(peer_refill_goal(0, 0, 50), Some(50));
        assert_eq!(peer_refill_goal(2, 1, 50), Some(50));
        assert_eq!(peer_refill_goal(49, 1, 50), Some(50));
        assert_eq!(peer_refill_goal(50, 1, 50), None);
        assert_eq!(peer_refill_goal(10, 50, 50), Some(26));
        assert_eq!(peer_refill_goal(30, 50, 50), None);
        assert_eq!(peer_refill_goal(48, 48, 100), Some(64));
        assert_eq!(peer_refill_goal(52, 48, 100), None);
    }

    #[test]
    fn post_merge_execution_heads_advertise_terminal_total_difficulty() {
        let head = execution_head(25_000_000, B256::repeat_byte(0x11), 1_778_000_000);

        assert_eq!(head.difficulty, U256::ZERO);
        assert_eq!(
            head.total_difficulty,
            MAINNET.final_paris_total_difficulty().unwrap()
        );
    }

    #[test]
    fn merge_boundary_constants_match_mainnet_chainspec() {
        let (merge_block, _) = MAINNET
            .paris_block_and_final_difficulty
            .expect("mainnet Paris block must be known");

        assert_eq!(merge_block, logex_types::EXECUTION_MERGE_BLOCK);
        assert_eq!(
            logex_types::EXECUTION_TERMINAL_POW_BLOCK + 1,
            logex_types::EXECUTION_MERGE_BLOCK
        );
    }

    #[test]
    fn serving_peer_notifications_ignore_zero_and_duplicates() {
        let first = PeerId::repeat_byte(0x11);
        let mut seen = HashSet::new();

        assert!(should_note_serving_peer(first, &mut seen));
        assert!(!should_note_serving_peer(first, &mut seen));
        assert!(!should_note_serving_peer(PeerId::ZERO, &mut seen));
    }

    #[tokio::test]
    async fn shutdown_requested_stays_true_after_change_is_observed() {
        let (tx, mut rx) = watch::channel(false);

        tx.send(true).unwrap();
        rx.changed().await.unwrap();

        assert!(shutdown_requested(&rx));
    }
}
