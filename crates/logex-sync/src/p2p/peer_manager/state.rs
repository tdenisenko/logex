use std::ops::RangeInclusive;

use reth_chainspec::{EthChainSpec, MAINNET};
use tracing::{debug, info, warn};

use super::*;
use crate::p2p::persistence::persist_known_peers_if_changed;

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

    /// Number of queued peer candidates awaiting or undergoing connection attempts.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
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
        if self.peers.len() >= min || self.peers.len() >= target {
            return;
        }

        let deadline = Instant::now() + FILL_BUDGET;

        loop {
            self.drain_events_now();

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
                debug!(
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
        response_kind: &'static str,
        requested: usize,
        returned: usize,
    ) {
        self.reset_peer_timeout(peer_id);
        debug!(
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
        response_kind: &'static str,
        requested: usize,
    ) {
        self.reset_peer_timeout(peer_id);
        self.demote_peer(peer_id);
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

    pub(super) fn note_peer_success(&mut self, peer_id: PeerId) {
        self.reset_peer_timeout(peer_id);
    }

    pub(super) fn on_receipt_request_success(&mut self, peer_id: PeerId) {
        self.note_peer_success(peer_id);
        self.advance_request_cursor();
    }

    pub(super) fn reset_peer_timeout(&mut self, peer_id: PeerId) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.consecutive_timeouts = 0;
        }
    }

    pub(super) fn on_request_error(&mut self, peer_id: PeerId, error: &RequestAttempt) -> bool {
        match error {
            RequestAttempt::Disconnected => true,
            RequestAttempt::Request(request_error) => match request_error {
                reth_network::p2p::error::RequestError::Timeout => {
                    self.network
                        .reputation_change(peer_id, ReputationChangeKind::Timeout);
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

    pub(super) fn mark_peer_serving(&mut self, peer_id: PeerId) -> (bool, bool) {
        let (became_serving, productive) = if let Some(peer) = self.peers.get_mut(&peer_id) {
            let became_serving = !peer.is_serving;
            peer.is_serving = true;
            (became_serving, Some(peer.remote_record))
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

    pub(super) fn demote_peer(&mut self, peer_id: PeerId) {
        self.peer_order.retain(|id| *id != peer_id);
        self.peer_order.push_back(peer_id);
        self.rebalance_request_cursor();
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
        && latest_block.unwrap_or_default() == 0
        && connected_for >= USELESS_PEER_GRACE_PERIOD
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
        None if !is_serving && latest_block.unwrap_or_default() == 0 => {
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

    is_serving || latest_block.is_some_and(|block| block > 0 && block >= required_block)
}

pub(super) fn should_persist_productive_update(
    existing_index: Option<usize>,
    known_changed: bool,
) -> bool {
    existing_index.is_none() || known_changed
}

pub(super) fn body_range_hint(required_block: u64, requested_hashes: usize) -> RangeInclusive<u64> {
    let span = requested_hashes.saturating_sub(1) as u64;
    required_block.saturating_sub(span)..=required_block
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
    fn body_range_hint_tracks_requested_tail_range() {
        assert_eq!(body_range_hint(150, 1), 150..=150);
        assert_eq!(body_range_hint(150, 4), 147..=150);
        assert_eq!(body_range_hint(2, 8), 0..=2);
    }

    #[test]
    fn stale_nonserving_peer_detection_requires_zero_tip_and_grace_period() {
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
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn peer_preference_requires_serving_or_sufficient_tip() {
        assert!(peer_is_preferred_for_block(false, Some(0), Some(500), 400));
        assert!(peer_is_preferred_for_block(true, Some(350), Some(0), 400));
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
    fn productive_reordering_alone_does_not_force_persist() {
        assert!(!should_persist_productive_update(Some(0), false));
        assert!(!should_persist_productive_update(Some(3), false));
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
}
