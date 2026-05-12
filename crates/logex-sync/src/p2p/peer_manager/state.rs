use logex_types::ExecutionNetworkStatus;
use reth_chainspec::{EthChainSpec, MAINNET};
use tracing::{debug, info, trace, warn};

use super::*;
use crate::p2p::persistence::persist_known_peers_if_changed;

const PEER_RATE_EWMA_WEIGHT: f64 = 0.25;
const RECEIPT_QUARANTINE_DURATION: Duration = Duration::from_secs(5 * 60);
const RECEIPT_REQUEST_FAILURE_QUARANTINE_DURATION: Duration = Duration::from_secs(30);

impl PeerManager {
    /// Number of currently connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Number of connected peers that have actually served sync data.
    pub fn serving_peer_count(&self) -> usize {
        self.peers.values().filter(|peer| peer.is_serving).count()
    }

    /// Highest advertised canonical block across connected peers.
    pub fn highest_peer_block(&self) -> Option<u64> {
        self.peers
            .values()
            .filter_map(|peer| peer.remote_status.latest_block)
            .filter(|block| *block > 0)
            .max()
    }

    /// Whether any connected peer is eligible to try a block request.
    pub fn has_block_request_peer(&self, required_block: u64) -> bool {
        self.peers.values().any(|peer| {
            peer.remote_status
                .earliest_block
                .is_none_or(|earliest| earliest <= required_block)
                && peer_can_attempt_block_request(
                    peer.is_serving,
                    peer.remote_status.latest_block,
                    required_block,
                )
        })
    }

    /// Number of queued peer candidates awaiting or undergoing connection attempts.
    pub fn pending_count(&self) -> usize {
        self.pending.len().saturating_add(self.pending_dials.len())
    }

    /// Snapshot of execution-network peer retention and dial state for status/UI metrics.
    pub fn execution_network_status(&self) -> ExecutionNetworkStatus {
        let mut client_counts = ExecutionClientFamilyCounts::default();
        for peer in self.peers.values() {
            client_counts.record(&peer.client_version, peer.is_serving);
        }

        ExecutionNetworkStatus {
            max_peers: self.max_peers,
            accepted_sessions: self.session_metrics.accepted_sessions,
            rejected_zero_tip_sessions: self.session_metrics.rejected_zero_tip_sessions,
            disconnected_sessions: self.session_metrics.disconnected_sessions,
            saturated_disconnects: self.session_metrics.saturated_disconnects,
            nonserving_disconnects: self.session_metrics.nonserving_disconnects,
            missing_fork_id_candidates: self.session_metrics.missing_fork_id_candidates,
            fork_id_rejected_candidates: self.session_metrics.fork_id_rejected_candidates,
            queued_candidates: self.pending.len(),
            pending_dials: self.pending_dials.len(),
            productive_peers: self.productive.len(),
            known_peers: self.known_peers.len(),
            saturated_peers: self.saturated_peers.len(),
            receipt_quarantined_peers: self.receipt_quarantined_peers.len(),
            connected_geth_peers: client_counts.connected_geth,
            connected_nethermind_peers: client_counts.connected_nethermind,
            connected_reth_peers: client_counts.connected_reth,
            connected_other_peers: client_counts.connected_other,
            serving_geth_peers: client_counts.serving_geth,
            serving_nethermind_peers: client_counts.serving_nethermind,
            serving_reth_peers: client_counts.serving_reth,
            serving_other_peers: client_counts.serving_other,
        }
    }

    /// Snapshot of productive peers suitable for writing to disk on shutdown.
    pub fn known_peers(&self) -> Vec<NodeRecord> {
        let mut peers = Vec::with_capacity(MAX_PERSISTED_PEERS);

        for peer in &self.productive {
            if is_bootstrap_node(peer.id) {
                continue;
            }
            push_unique_peer(&mut peers, *peer);
            if peers.len() >= MAX_PERSISTED_PEERS {
                break;
            }
        }

        peers
    }

    /// Wait until we have at least `min` peers or run out of budget.
    pub async fn fill_peers(&mut self, min: usize, target: usize) {
        self.drain_events_now();
        if !self.network_activated {
            return;
        }
        self.queue_known_peers();
        self.dial_pending_peers(target);
        if self.peers.len() >= min || self.peers.len() >= target {
            return;
        }

        let deadline = Instant::now() + FILL_BUDGET;

        loop {
            self.drain_events_now();
            self.queue_known_peers();
            self.dial_pending_peers(target);

            if self.peers.len() >= min || self.peers.len() >= target {
                return;
            }

            if Instant::now() >= deadline {
                debug!(
                    connected_peers = self.peers.len(),
                    pending_peers = self.pending.len(),
                    target,
                    "fill_peers budget exhausted"
                );
                return;
            }

            let remaining = (deadline - Instant::now()).min(DISCOVERY_WAIT);
            if !self.wait_for_activity(remaining).await {
                trace!(
                    connected_peers = self.peers.len(),
                    pending_peers = self.pending.len(),
                    "no network activity while waiting for peers during this refill interval"
                );
                continue;
            }
        }
    }

    /// Disconnect and de-prioritize a peer that served invalid block data.
    pub fn report_invalid_block_data(&mut self, peer_id: PeerId, response_kind: &'static str) {
        if peer_id == PeerId::ZERO {
            return;
        }

        self.network
            .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
        self.network.disconnect_peer(peer_id);
        let known_changed = self.forget_peer(peer_id);
        if known_changed {
            self.persist_productive_peers();
        }
        warn!(
            peer = %peer_id,
            response_kind,
            "peer served invalid block data, disconnecting it"
        );
    }

    pub(super) fn on_invalid_response_length(
        &mut self,
        peer_id: PeerId,
        response_kind: &'static str,
        requested: usize,
        returned: usize,
    ) {
        self.network
            .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
        warn!(
            peer = %peer_id,
            response_kind,
            requested,
            returned,
            "peer returned more items than requested, disconnecting it"
        );
    }

    pub(super) fn on_partial_response(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        response_kind: &'static str,
        requested: usize,
        returned: usize,
    ) {
        self.reduce_peer_request_limit(peer_id, kind);
        self.pause_peer_requests(peer_id, kind, REQUEST_KIND_PAUSE_DURATION);
        self.record_soft_failure(peer_id);
        trace!(
            peer = %peer_id,
            response_kind,
            requested,
            returned,
            remaining = requested.saturating_sub(returned),
            "peer returned a partial response, requesting the remaining tail"
        );
    }

    pub(super) fn on_zero_progress_response(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        response_kind: &'static str,
        requested: usize,
    ) {
        self.reduce_peer_request_limit(peer_id, kind);
        self.record_soft_failure(peer_id);
        debug!(
            peer = %peer_id,
            response_kind,
            requested,
            "peer returned no data for a non-empty request"
        );
    }

    pub(super) fn peer_ids_for_requests(&self, required_block: Option<u64>) -> Vec<PeerId> {
        let mut peers: Vec<_> = self
            .peer_order
            .iter()
            .filter(|peer_id| self.peers.contains_key(*peer_id))
            .copied()
            .collect();
        rotate_request_candidates(&mut peers, self.request_cursor);

        let Some(required_block) = required_block else {
            return peers;
        };

        let (mut preferred, mut fallback) = (Vec::with_capacity(peers.len()), Vec::new());
        for peer_id in peers {
            let Some(peer) = self.peers.get(&peer_id) else {
                continue;
            };

            if peer
                .remote_status
                .earliest_block
                .is_some_and(|earliest| earliest > required_block)
            {
                continue;
            }

            if !peer_can_attempt_block_request(
                peer.is_serving,
                peer.remote_status.latest_block,
                required_block,
            ) {
                continue;
            }

            if peer_is_preferred_for_block(
                peer.is_serving,
                peer.remote_status.earliest_block,
                peer.remote_status.latest_block,
                required_block,
            ) {
                preferred.push(peer_id);
            } else {
                fallback.push(peer_id);
            }
        }
        preferred.extend(fallback);
        preferred
    }

    pub(super) fn sort_peer_ids_by_request_performance(
        &self,
        peer_ids: &mut [PeerId],
        kind: PeerRequestKind,
    ) {
        peer_ids.sort_by(|left, right| {
            let left_score = self.peer_request_score(*left, kind);
            let right_score = self.peer_request_score(*right, kind);
            right_score
                .partial_cmp(&left_score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    fn peer_request_score(&self, peer_id: PeerId, kind: PeerRequestKind) -> f64 {
        let Some(peer) = self.peers.get(&peer_id) else {
            return f64::MIN;
        };
        if peer_request_is_paused(peer, kind) {
            return f64::MIN;
        }
        if matches!(kind, PeerRequestKind::Receipts) && peer_receipts_are_quarantined(peer) {
            return f64::MIN;
        }

        let measured_rate = match kind {
            PeerRequestKind::Headers => peer.header_blocks_per_sec,
            PeerRequestKind::Bodies => peer.body_blocks_per_sec,
            PeerRequestKind::Receipts => peer.receipt_blocks_per_sec,
        };
        let base_rate = if measured_rate > 0.0 {
            measured_rate
        } else {
            1.0
        };
        let serving_bonus = if peer.is_serving { 4.0 } else { 0.0 };
        let timeout_penalty = f64::from(peer.consecutive_timeouts) * 8.0;
        base_rate + serving_bonus - timeout_penalty
    }

    pub(super) fn record_peer_request_success(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        blocks: usize,
        elapsed: Duration,
    ) {
        self.reset_peer_timeout(peer_id);
        if matches!(kind, PeerRequestKind::Receipts) {
            self.receipt_quarantined_peers.remove(&peer_id);
        }
        self.clear_peer_request_pause(peer_id, kind);
        self.adjust_peer_request_limit_after_success(peer_id, kind, blocks, elapsed);
        if blocks == 0 || elapsed.is_zero() {
            return;
        }

        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };
        peer.is_serving = true;
        if matches!(kind, PeerRequestKind::Receipts) {
            peer.receipt_quarantined_until = None;
        }
        let observed_rate = blocks as f64 / elapsed.as_secs_f64().max(0.001);
        let slot = match kind {
            PeerRequestKind::Headers => &mut peer.header_blocks_per_sec,
            PeerRequestKind::Bodies => &mut peer.body_blocks_per_sec,
            PeerRequestKind::Receipts => &mut peer.receipt_blocks_per_sec,
        };
        *slot = if *slot > 0.0 {
            (*slot * (1.0 - PEER_RATE_EWMA_WEIGHT)) + (observed_rate * PEER_RATE_EWMA_WEIGHT)
        } else {
            observed_rate
        };
    }

    pub(super) fn reset_peer_timeout(&mut self, peer_id: PeerId) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.consecutive_timeouts = 0;
        }
    }

    pub(super) fn on_request_error(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        error: &RequestAttempt,
    ) -> bool {
        match error {
            RequestAttempt::Disconnected => true,
            RequestAttempt::Request(request_error) => match request_error {
                reth_network::p2p::error::RequestError::Timeout => {
                    self.reduce_peer_request_limit(peer_id, kind);
                    self.pause_peer_requests(peer_id, kind, REQUEST_KIND_PAUSE_DURATION);
                    self.record_timeout(peer_id) >= MAX_CONSECUTIVE_TIMEOUTS
                }
                reth_network::p2p::error::RequestError::BadResponse => {
                    self.network
                        .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
                    true
                }
                reth_network::p2p::error::RequestError::UnsupportedCapability => {
                    self.network
                        .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
                    true
                }
                reth_network::p2p::error::RequestError::ChannelClosed
                | reth_network::p2p::error::RequestError::ConnectionDropped => true,
            },
        }
    }

    pub(super) fn record_timeout(&mut self, peer_id: PeerId) -> u32 {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return MAX_CONSECUTIVE_TIMEOUTS;
        };
        peer.consecutive_timeouts += 1;
        let consecutive_timeouts = peer.consecutive_timeouts;
        let _ = peer;
        self.demote_peer(peer_id);
        consecutive_timeouts
    }

    pub(super) fn record_soft_failure(&mut self, peer_id: PeerId) {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };
        peer.consecutive_timeouts = peer
            .consecutive_timeouts
            .saturating_add(1)
            .min(MAX_CONSECUTIVE_TIMEOUTS);
        let _ = peer;
        self.demote_peer(peer_id);
    }

    pub(super) fn peer_receipts_quarantined(&self, peer_id: PeerId) -> bool {
        let now = Instant::now();
        self.receipt_quarantined_peers
            .get(&peer_id)
            .is_some_and(|until| *until > now)
            || self
                .peers
                .get(&peer_id)
                .is_some_and(peer_receipts_are_quarantined)
    }

    pub(super) fn filter_paused_request_peers(
        &self,
        peer_ids: &mut Vec<PeerId>,
        kind: PeerRequestKind,
    ) {
        if matches!(kind, PeerRequestKind::Headers) || peer_ids.len() <= 1 {
            return;
        }

        let available: Vec<_> = peer_ids
            .iter()
            .copied()
            .filter(|peer_id| {
                self.peers
                    .get(peer_id)
                    .is_some_and(|peer| !peer_request_is_paused(peer, kind))
            })
            .collect();
        if !available.is_empty() {
            *peer_ids = available;
        }
    }

    pub(super) fn quarantine_peer_receipts(
        &mut self,
        peer_id: PeerId,
        response_kind: &'static str,
        requested_blocks: usize,
        returned_blocks: usize,
    ) {
        self.set_receipt_quarantine(peer_id, RECEIPT_QUARANTINE_DURATION);
        self.reduce_peer_request_limit(peer_id, PeerRequestKind::Receipts);
        self.record_soft_failure(peer_id);
        if self.remove_productive_peer(peer_id) {
            self.persist_productive_peers();
        }
        debug!(
            peer = %peer_id,
            response_kind,
            requested_blocks,
            returned_blocks,
            quarantine_seconds = RECEIPT_QUARANTINE_DURATION.as_secs(),
            "quarantined peer for receipt requests after incomplete receipt service"
        );
    }

    pub(super) fn quarantine_peer_receipts_after_request_failure(
        &mut self,
        peer_id: PeerId,
        response_kind: &'static str,
        requested_blocks: usize,
        error: &RequestAttempt,
    ) {
        self.set_receipt_quarantine(peer_id, RECEIPT_REQUEST_FAILURE_QUARANTINE_DURATION);
        debug!(
            peer = %peer_id,
            response_kind,
            requested_blocks,
            ?error,
            quarantine_seconds = RECEIPT_REQUEST_FAILURE_QUARANTINE_DURATION.as_secs(),
            "quarantined peer for receipt requests after receipt request failure"
        );
    }

    pub(super) fn mark_peer_serving(&mut self, peer_id: PeerId) -> (bool, bool) {
        let (became_serving, productive) = if let Some(peer) = self.peers.get_mut(&peer_id) {
            let became_serving = !peer.is_serving;
            peer.is_serving = true;
            (became_serving, became_serving.then_some(peer.remote_record))
        } else {
            (false, None)
        };

        let should_persist = productive
            .map(|peer| self.remember_productive(peer))
            .unwrap_or(false);
        (became_serving, should_persist)
    }

    pub fn report_valid_serving_peer(&mut self, peer_id: PeerId) -> bool {
        if peer_id == PeerId::ZERO {
            return false;
        }

        let (became_serving, should_persist) = self.mark_peer_serving(peer_id);
        if should_persist {
            self.persist_productive_peers();
        }
        became_serving
    }

    pub(super) fn remember_productive(&mut self, node: NodeRecord) -> bool {
        let now = Instant::now();
        if self
            .receipt_quarantined_peers
            .get(&node.id)
            .is_some_and(|until| *until > now)
        {
            return false;
        }

        let existing_index = self
            .productive
            .iter()
            .position(|productive| productive.id == node.id);
        if let Some(index) = existing_index {
            self.productive.remove(index);
        }

        self.productive.push_front(node);
        while self.productive.len() > MAX_PERSISTED_PEERS {
            self.productive.pop_back();
        }

        let known_changed = upsert_known_peer(&mut self.known_peers, node);
        should_persist_productive_update(existing_index, known_changed)
    }

    pub(super) fn remove_productive_peer(&mut self, peer_id: PeerId) -> bool {
        let productive_before = self.productive.len();
        self.productive.retain(|peer| peer.id != peer_id);
        let known_before = self.known_peers.len();
        self.known_peers.retain(|peer| peer.id != peer_id);
        productive_before != self.productive.len() || known_before != self.known_peers.len()
    }

    pub(super) fn persist_productive_peers(&mut self) {
        let peers = self.known_peers();
        match persist_known_peers_if_changed(
            &self.known_peers_path,
            &peers,
            &mut self.persisted_known_peers,
        ) {
            Ok(true) => {
                info!(
                    peers = peers.len(),
                    path = %self.known_peers_path.display(),
                    "persisted known peers after serving peer update"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    error = %error,
                    path = %self.known_peers_path.display(),
                    "failed to persist known peers after serving peer update"
                );
            }
        }
    }

    fn set_receipt_quarantine(&mut self, peer_id: PeerId, duration: Duration) {
        let quarantined_until = Instant::now() + duration;
        self.receipt_quarantined_peers
            .insert(peer_id, quarantined_until);
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.receipt_quarantined_until = Some(quarantined_until);
            peer.receipt_blocks_per_sec = 0.0;
        }
    }

    pub(super) fn demote_peer(&mut self, peer_id: PeerId) {
        self.peer_order.retain(|id| *id != peer_id);
        self.peer_order.push_back(peer_id);
        self.rebalance_request_cursor();
    }

    pub(super) fn peer_request_limit(&self, peer_id: PeerId, kind: PeerRequestKind) -> usize {
        let Some(peer) = self.peers.get(&peer_id) else {
            return request_limit_initial(kind);
        };

        match kind {
            PeerRequestKind::Headers => request_limit_initial(kind),
            PeerRequestKind::Bodies => peer.body_request_limit,
            PeerRequestKind::Receipts => peer.receipt_request_limit,
        }
        .clamp(REQUEST_LIMIT_MIN, REQUEST_LIMIT_MAX)
    }

    fn adjust_peer_request_limit_after_success(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        blocks: usize,
        elapsed: Duration,
    ) {
        if blocks == 0 || matches!(kind, PeerRequestKind::Headers) {
            return;
        }

        let Some(limit) = self.peer_request_limit_mut(peer_id, kind) else {
            return;
        };
        if elapsed > REQUEST_LIMIT_UPPER_LATENCY {
            *limit = decrease_request_limit(*limit);
        } else if blocks >= *limit && elapsed < REQUEST_LIMIT_LOWER_LATENCY {
            *limit = increase_request_limit(*limit);
        }
    }

    fn reduce_peer_request_limit(&mut self, peer_id: PeerId, kind: PeerRequestKind) {
        let Some(limit) = self.peer_request_limit_mut(peer_id, kind) else {
            return;
        };
        *limit = decrease_request_limit(*limit);
    }

    fn pause_peer_requests(&mut self, peer_id: PeerId, kind: PeerRequestKind, duration: Duration) {
        let until = Instant::now() + duration;
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };
        match kind {
            PeerRequestKind::Headers => {}
            PeerRequestKind::Bodies => peer.body_paused_until = Some(until),
            PeerRequestKind::Receipts => peer.receipt_paused_until = Some(until),
        }
    }

    fn clear_peer_request_pause(&mut self, peer_id: PeerId, kind: PeerRequestKind) {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return;
        };
        match kind {
            PeerRequestKind::Headers => {}
            PeerRequestKind::Bodies => peer.body_paused_until = None,
            PeerRequestKind::Receipts => peer.receipt_paused_until = None,
        }
    }

    fn peer_request_limit_mut(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
    ) -> Option<&mut usize> {
        let peer = self.peers.get_mut(&peer_id)?;
        match kind {
            PeerRequestKind::Headers => None,
            PeerRequestKind::Bodies => Some(&mut peer.body_request_limit),
            PeerRequestKind::Receipts => Some(&mut peer.receipt_request_limit),
        }
    }

    pub(super) fn advance_request_cursor(&mut self) {
        let len = self.peer_order.len();
        if len == 0 {
            self.request_cursor = 0;
        } else {
            self.request_cursor = (self.request_cursor + 1) % len;
        }
    }

    pub(super) fn rebalance_request_cursor(&mut self) {
        let len = self.peer_order.len();
        if len == 0 {
            self.request_cursor = 0;
        } else if self.request_cursor >= len {
            self.request_cursor %= len;
        }
    }
}

pub(super) fn push_unique_peer(peers: &mut Vec<NodeRecord>, node: NodeRecord) {
    if peers.iter().any(|peer| peer.id == node.id) {
        return;
    }
    peers.push(node);
}

pub(super) fn request_limit_initial(kind: PeerRequestKind) -> usize {
    match kind {
        PeerRequestKind::Headers => 1,
        PeerRequestKind::Bodies => BODY_REQUEST_LIMIT_INITIAL,
        PeerRequestKind::Receipts => RECEIPT_REQUEST_LIMIT_INITIAL,
    }
}

pub(super) fn inherited_peer_request_limit(
    limits: impl Iterator<Item = usize>,
    kind: PeerRequestKind,
) -> usize {
    let mut total = 0usize;
    let mut count = 0usize;
    for limit in limits {
        total = total.saturating_add(limit);
        count = count.saturating_add(1);
    }

    if count == 0 {
        return request_limit_initial(kind);
    }

    total
        .div_ceil(count)
        .clamp(request_limit_initial(kind), REQUEST_LIMIT_MAX)
}

fn increase_request_limit(current: usize) -> usize {
    current
        .saturating_mul(3)
        .div_ceil(2)
        .clamp(REQUEST_LIMIT_MIN, REQUEST_LIMIT_MAX)
}

fn decrease_request_limit(current: usize) -> usize {
    current
        .saturating_mul(2)
        .div_ceil(3)
        .clamp(REQUEST_LIMIT_MIN, REQUEST_LIMIT_MAX)
}

pub(super) fn seed_productive_peers(known_peers: &[NodeRecord]) -> VecDeque<NodeRecord> {
    known_peers
        .iter()
        .copied()
        .filter(|peer| !is_bootstrap_node(peer.id) && peer.tcp_port > 0)
        .take(MAX_PERSISTED_PEERS)
        .collect()
}

pub(super) fn rotate_request_candidates(peers: &mut [PeerId], request_cursor: usize) {
    if peers.len() > 1 {
        peers.rotate_left(request_cursor % peers.len());
    }
}

pub(super) fn is_stale_nonserving_peer(
    is_serving: bool,
    latest_block: Option<u64>,
    connected_for: Duration,
) -> bool {
    !is_serving
        && latest_block.is_none_or(|latest_block| latest_block == 0)
        && connected_for >= USELESS_PEER_GRACE_PERIOD
}

pub(super) fn is_saturated_remote_rejection(
    reason: Option<DisconnectReason>,
    _is_serving: bool,
    _connected_for: Duration,
) -> bool {
    matches!(reason, Some(DisconnectReason::TooManyPeers))
}

pub(super) fn should_retry_disconnected_peer(reason: Option<DisconnectReason>) -> bool {
    matches!(
        reason,
        None | Some(
            DisconnectReason::DisconnectRequested
                | DisconnectReason::TcpSubsystemError
                | DisconnectReason::PingTimeout
        )
    )
}

pub(super) fn disconnect_note(
    reason: Option<DisconnectReason>,
    is_serving: bool,
    latest_block: Option<u64>,
    connected_for: Duration,
) -> &'static str {
    match reason {
        Some(DisconnectReason::SubprotocolSpecific) => {
            "likely peer sent an invalid post-merge subprotocol message"
        }
        Some(DisconnectReason::TooManyPeers) => "remote peer was saturated",
        None if is_stale_nonserving_peer(is_serving, latest_block, connected_for) => {
            "peer never advertised a usable tip and was not useful for sync"
        }
        None if !is_serving && latest_block.is_none_or(|latest_block| latest_block == 0) => {
            "peer disconnected before advertising a usable tip"
        }
        None if !is_serving => "peer disconnected before serving sync data",
        _ => "session churn",
    }
}

pub(super) fn upsert_known_peer(known_peers: &mut Vec<NodeRecord>, node: NodeRecord) -> bool {
    if let Some(existing) = known_peers.iter_mut().find(|peer| peer.id == node.id) {
        if *existing == node {
            return false;
        }
        *existing = node;
        return true;
    }

    known_peers.push(node);
    true
}

pub(super) fn is_bootstrap_node(id: PeerId) -> bool {
    MAINNET_BOOTNODE_IDS.contains(&id)
}

pub(super) fn peer_is_preferred_for_block(
    is_serving: bool,
    earliest_block: Option<u64>,
    latest_block: Option<u64>,
    required_block: u64,
) -> bool {
    if earliest_block.is_some_and(|earliest| earliest > required_block) {
        return false;
    }
    if latest_block.is_some_and(|latest| latest < required_block) {
        return false;
    }

    is_serving || latest_block.is_some_and(|block| block > 0 && block >= required_block)
}

pub(super) fn peer_can_attempt_block_request(
    is_serving: bool,
    latest_block: Option<u64>,
    required_block: u64,
) -> bool {
    if is_serving {
        return true;
    }

    match latest_block {
        Some(latest) => latest >= required_block,
        None => true,
    }
}

pub(super) fn should_persist_productive_update(
    existing_index: Option<usize>,
    known_changed: bool,
) -> bool {
    existing_index != Some(0) || known_changed
}

pub(super) fn normalize_network_head(mut head: Head) -> Head {
    if head.hash.is_zero() {
        head.hash = MAINNET.genesis_hash();
    }
    if head.number == 0 && head.timestamp == 0 {
        head.timestamp = MAINNET.genesis().timestamp;
    }
    head
}

#[derive(Default)]
struct ExecutionClientFamilyCounts {
    connected_geth: usize,
    connected_nethermind: usize,
    connected_reth: usize,
    connected_other: usize,
    serving_geth: usize,
    serving_nethermind: usize,
    serving_reth: usize,
    serving_other: usize,
}

impl ExecutionClientFamilyCounts {
    fn record(&mut self, client_version: &str, is_serving: bool) {
        match execution_client_family(client_version) {
            ExecutionClientFamily::Geth => {
                self.connected_geth += 1;
                if is_serving {
                    self.serving_geth += 1;
                }
            }
            ExecutionClientFamily::Nethermind => {
                self.connected_nethermind += 1;
                if is_serving {
                    self.serving_nethermind += 1;
                }
            }
            ExecutionClientFamily::Reth => {
                self.connected_reth += 1;
                if is_serving {
                    self.serving_reth += 1;
                }
            }
            ExecutionClientFamily::Other => {
                self.connected_other += 1;
                if is_serving {
                    self.serving_other += 1;
                }
            }
        }
    }
}

enum ExecutionClientFamily {
    Geth,
    Nethermind,
    Reth,
    Other,
}

fn execution_client_family(client_version: &str) -> ExecutionClientFamily {
    let client_version = client_version.to_ascii_lowercase();
    if client_version.starts_with("geth/") {
        ExecutionClientFamily::Geth
    } else if client_version.starts_with("nethermind/") {
        ExecutionClientFamily::Nethermind
    } else if client_version.starts_with("reth/") {
        ExecutionClientFamily::Reth
    } else {
        ExecutionClientFamily::Other
    }
}

pub(super) fn advertised_status_range(
    cached_range: Option<(u64, u64, B256)>,
    head: Head,
) -> Option<(u64, u64, B256)> {
    if let Some((earliest, latest, latest_hash)) = cached_range
        && earliest <= latest
        && !latest_hash.is_zero()
    {
        return Some((earliest, latest, latest_hash));
    }

    let head = normalize_network_head(head);
    if !head.hash.is_zero() {
        return Some((head.number, head.number, head.hash));
    }

    None
}

pub(super) fn peer_receipts_are_quarantined(peer: &ActivePeer) -> bool {
    peer.receipt_quarantined_until
        .is_some_and(|until| until > Instant::now())
}

fn peer_request_is_paused(peer: &ActivePeer, kind: PeerRequestKind) -> bool {
    let now = Instant::now();
    match kind {
        PeerRequestKind::Headers => false,
        PeerRequestKind::Bodies => peer.body_paused_until.is_some_and(|until| until > now),
        PeerRequestKind::Receipts => peer.receipt_paused_until.is_some_and(|until| until > now),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mainnet_bootnodes_are_recognized() {
        let bootnode = mainnet_nodes()
            .into_iter()
            .next()
            .expect("mainnet bootnodes should not be empty");
        assert!(is_bootstrap_node(bootnode.id));
    }

    #[test]
    fn non_bootstrap_peer_is_not_treated_as_bootstrap() {
        let non_bootstrap = NodeRecord::new(
            "203.0.113.10:30303".parse().expect("valid address"),
            PeerId::repeat_byte(0x42),
        );
        assert!(!is_bootstrap_node(non_bootstrap.id));
    }

    #[test]
    fn upsert_known_peer_updates_existing_record() {
        let peer_id = PeerId::repeat_byte(0x42);
        let mut known_peers = vec![NodeRecord::new_with_ports(
            "127.0.0.1".parse().unwrap(),
            30303,
            Some(30303),
            peer_id,
        )];

        let changed = upsert_known_peer(
            &mut known_peers,
            NodeRecord::new_with_ports("127.0.0.1".parse().unwrap(), 30304, Some(30305), peer_id),
        );

        assert!(changed);
        assert_eq!(known_peers.len(), 1);
        assert_eq!(known_peers[0].tcp_port, 30304);
        assert_eq!(known_peers[0].udp_port, 30305);
    }

    #[test]
    fn seed_productive_peers_preserves_restart_priority_order() {
        let first = NodeRecord::new_with_ports(
            "127.0.0.1".parse().unwrap(),
            30303,
            Some(30303),
            PeerId::repeat_byte(0x11),
        );
        let second = NodeRecord::new_with_ports(
            "127.0.0.1".parse().unwrap(),
            30304,
            Some(30304),
            PeerId::repeat_byte(0x22),
        );

        let productive = seed_productive_peers(&[first, second]);
        let productive: Vec<_> = productive.into_iter().collect();

        assert_eq!(productive, vec![first, second]);
    }

    #[test]
    fn request_rotation_moves_starting_peer() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let mut peers = vec![first, second, third];

        rotate_request_candidates(&mut peers, 1);
        assert_eq!(peers, vec![second, third, first]);

        rotate_request_candidates(&mut peers, 2);
        assert_eq!(peers, vec![first, second, third]);
    }

    #[test]
    fn inherited_request_limits_use_warmed_pool_average() {
        assert_eq!(
            inherited_peer_request_limit([24, 48, 96].into_iter(), PeerRequestKind::Bodies),
            BODY_REQUEST_LIMIT_INITIAL
        );
        assert_eq!(
            inherited_peer_request_limit([1, 2].into_iter(), PeerRequestKind::Receipts),
            RECEIPT_REQUEST_LIMIT_INITIAL
        );
        assert_eq!(
            inherited_peer_request_limit(std::iter::empty(), PeerRequestKind::Bodies),
            BODY_REQUEST_LIMIT_INITIAL
        );
        assert_eq!(
            inherited_peer_request_limit(
                [usize::MAX, usize::MAX].into_iter(),
                PeerRequestKind::Bodies
            ),
            REQUEST_LIMIT_MAX
        );
    }

    #[test]
    fn stale_nonserving_peer_detection_requires_missing_or_zero_tip_and_grace_period() {
        assert!(is_stale_nonserving_peer(
            false,
            Some(0),
            USELESS_PEER_GRACE_PERIOD
        ));
        assert!(is_stale_nonserving_peer(
            false,
            None,
            USELESS_PEER_GRACE_PERIOD + Duration::from_secs(1)
        ));
        assert!(!is_stale_nonserving_peer(
            true,
            Some(0),
            USELESS_PEER_GRACE_PERIOD + Duration::from_secs(1)
        ));
        assert!(!is_stale_nonserving_peer(
            false,
            Some(1),
            USELESS_PEER_GRACE_PERIOD + Duration::from_secs(1)
        ));
        assert!(!is_stale_nonserving_peer(
            false,
            Some(0),
            USELESS_PEER_GRACE_PERIOD - Duration::from_secs(1)
        ));
    }

    #[test]
    fn saturated_remote_rejection_backs_off_any_too_many_peers_disconnect() {
        assert!(is_saturated_remote_rejection(
            Some(DisconnectReason::TooManyPeers),
            false,
            Duration::from_secs(1)
        ));
        assert!(is_saturated_remote_rejection(
            Some(DisconnectReason::TooManyPeers),
            true,
            Duration::from_secs(1)
        ));
        assert!(is_saturated_remote_rejection(
            Some(DisconnectReason::TooManyPeers),
            false,
            EARLY_SESSION_DROP_THRESHOLD + Duration::from_secs(1)
        ));
        assert!(!is_saturated_remote_rejection(
            None,
            false,
            Duration::from_secs(1)
        ));
    }

    #[test]
    fn retry_disconnected_peer_only_for_transient_session_closes() {
        assert!(should_retry_disconnected_peer(None));
        assert!(should_retry_disconnected_peer(Some(
            DisconnectReason::DisconnectRequested
        )));
        assert!(should_retry_disconnected_peer(Some(
            DisconnectReason::TcpSubsystemError
        )));
        assert!(should_retry_disconnected_peer(Some(
            DisconnectReason::PingTimeout
        )));
        assert!(!should_retry_disconnected_peer(Some(
            DisconnectReason::TooManyPeers
        )));
        assert!(!should_retry_disconnected_peer(Some(
            DisconnectReason::SubprotocolSpecific
        )));
        assert!(!should_retry_disconnected_peer(Some(
            DisconnectReason::ProtocolBreach
        )));
    }

    #[test]
    fn peer_preference_requires_serving_or_sufficient_tip() {
        assert!(peer_is_preferred_for_block(false, Some(0), Some(500), 400));
        assert!(peer_is_preferred_for_block(true, Some(350), None, 400));
        assert!(!peer_is_preferred_for_block(true, Some(350), Some(0), 400));
        assert!(!peer_is_preferred_for_block(
            true,
            Some(450),
            Some(500),
            400
        ));
        assert!(!peer_is_preferred_for_block(
            false,
            Some(450),
            Some(500),
            400
        ));
        assert!(!peer_is_preferred_for_block(false, Some(0), Some(0), 400));
        assert!(!peer_is_preferred_for_block(false, Some(0), Some(399), 400));
        assert!(!peer_is_preferred_for_block(false, None, None, 400));
    }

    #[test]
    fn block_request_candidates_require_serving_or_sufficient_tip() {
        assert!(peer_can_attempt_block_request(false, Some(500), 400));
        assert!(!peer_can_attempt_block_request(false, Some(0), 400));
        assert!(!peer_can_attempt_block_request(false, Some(399), 400));
        assert!(peer_can_attempt_block_request(false, None, 400));
        assert!(peer_can_attempt_block_request(true, None, 400));
    }

    #[test]
    fn productive_reordering_persists_when_priority_changes() {
        assert!(!should_persist_productive_update(Some(0), false));
        assert!(should_persist_productive_update(Some(3), false));
        assert!(should_persist_productive_update(None, false));
        assert!(should_persist_productive_update(Some(1), true));
    }

    #[test]
    fn disconnect_note_explains_subprotocol_specific_disconnects() {
        assert_eq!(
            disconnect_note(
                Some(DisconnectReason::SubprotocolSpecific),
                false,
                Some(0),
                Duration::from_millis(10)
            ),
            "likely peer sent an invalid post-merge subprotocol message"
        );
    }

    #[test]
    fn advertised_status_range_uses_cached_window_when_available() {
        let head = Head {
            number: 10,
            hash: B256::repeat_byte(0x10),
            ..Default::default()
        };
        let cached = Some((7, 9, B256::repeat_byte(0x09)));

        assert_eq!(
            advertised_status_range(cached, head),
            Some((7, 9, B256::repeat_byte(0x09)))
        );
    }

    #[test]
    fn advertised_status_range_falls_back_to_head_when_body_cache_is_empty() {
        let head = Head {
            number: 10,
            hash: B256::repeat_byte(0x10),
            ..Default::default()
        };

        assert_eq!(
            advertised_status_range(None, head),
            Some((10, 10, B256::repeat_byte(0x10)))
        );
    }

    #[test]
    fn advertised_status_range_allows_genesis_fallback() {
        let head = Head {
            number: 0,
            hash: B256::repeat_byte(0x10),
            ..Default::default()
        };

        assert_eq!(
            advertised_status_range(None, head),
            Some((0, 0, B256::repeat_byte(0x10)))
        );
    }

    #[test]
    fn advertised_status_range_ignores_invalid_cached_window() {
        let head = Head {
            number: 10,
            hash: B256::repeat_byte(0x10),
            ..Default::default()
        };
        let cached = Some((11, 9, B256::repeat_byte(0x09)));

        assert_eq!(
            advertised_status_range(cached, head),
            Some((10, 10, B256::repeat_byte(0x10)))
        );
    }
}
