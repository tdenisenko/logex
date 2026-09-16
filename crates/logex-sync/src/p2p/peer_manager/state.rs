use logex_types::ExecutionNetworkStatus;
use reth_chainspec::{EthChainSpec, MAINNET};
use tracing::{debug, info, trace, warn};

use super::*;
use crate::p2p::persistence::persist_known_peers_if_changed;

const PEER_RATE_EWMA_WEIGHT: f64 = 0.25;
const RECEIPT_QUARANTINE_DURATION: Duration = Duration::from_secs(5 * 60);
const RECEIPT_REQUEST_FAILURE_QUARANTINE_DURATION: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestErrorDisposition {
    Ignore,
    Pause,
    Timeout,
    DropBadProtocol,
}

impl PeerManager {
    /// Number of currently connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Number of connected peers that have actually served sync data.
    pub fn serving_peer_count(&self) -> usize {
        self.peers.values().filter(|peer| peer.is_serving).count()
    }

    /// Number of connected peers that are currently eligible for body and
    /// receipt requests. Unproven peers still use lower request limits.
    pub fn body_receipt_request_ready_peer_count(&self) -> usize {
        let mut body_ready = 0usize;
        let mut receipt_ready = 0usize;
        for peer in self.peers.values() {
            if !peer_request_is_paused(peer, PeerRequestKind::Bodies) {
                body_ready = body_ready.saturating_add(1);
            }
            if !peer_request_is_paused(peer, PeerRequestKind::Receipts)
                && !peer_receipts_are_quarantined(peer)
            {
                receipt_ready = receipt_ready.saturating_add(1);
            }
        }
        body_ready.min(receipt_ready)
    }

    /// Connected peers currently eligible for body and receipt requests,
    /// returned separately so scheduler backpressure can respect asymmetric
    /// receipt pauses/quarantines.
    pub fn body_receipt_request_ready_peer_counts(&self) -> (usize, usize) {
        let mut body_ready = 0usize;
        let mut receipt_ready = 0usize;
        for peer in self.peers.values() {
            if !peer_request_is_paused(peer, PeerRequestKind::Bodies) {
                body_ready = body_ready.saturating_add(1);
            }
            if !peer_request_is_paused(peer, PeerRequestKind::Receipts)
                && !peer_receipts_are_quarantined(peer)
            {
                receipt_ready = receipt_ready.saturating_add(1);
            }
        }
        (body_ready, receipt_ready)
    }

    /// Active plus reserved body/receipt request slots currently charged to
    /// peers by historical background fetch plans.
    pub fn active_body_receipt_request_counts(&self) -> (usize, usize) {
        let mut active_body_requests = 0usize;
        let mut active_receipt_requests = 0usize;
        for peer in self.peers.values() {
            active_body_requests = active_body_requests
                .saturating_add(peer.body_active_requests)
                .saturating_add(peer.body_reserved_requests);
            active_receipt_requests = active_receipt_requests
                .saturating_add(peer.receipt_active_requests)
                .saturating_add(peer.receipt_reserved_requests);
        }
        (active_body_requests, active_receipt_requests)
    }

    /// Adaptive concurrent request-slot capacity currently available for
    /// historical body/receipt fetches, derived from per-peer block limits
    /// that rise on fast complete responses and shrink on slow or partial
    /// responses.
    pub fn body_receipt_request_slot_capacity_counts(
        &self,
        per_ready_peer_target: usize,
    ) -> (usize, usize) {
        let mut body_capacity = 0usize;
        let mut receipt_capacity = 0usize;
        for peer in self.peers.values() {
            if !peer_request_is_paused(peer, PeerRequestKind::Bodies) {
                body_capacity =
                    body_capacity.saturating_add(adaptive_request_slot_capacity_for_peer(
                        PeerRequestKind::Bodies,
                        peer.is_serving,
                        peer.body_request_limit,
                        per_ready_peer_target,
                    ));
            }
            if !peer_request_is_paused(peer, PeerRequestKind::Receipts)
                && !peer_receipts_are_quarantined(peer)
            {
                receipt_capacity =
                    receipt_capacity.saturating_add(adaptive_request_slot_capacity_for_peer(
                        PeerRequestKind::Receipts,
                        peer.is_serving,
                        peer.receipt_request_limit,
                        per_ready_peer_target,
                    ));
            }
        }
        (body_capacity, receipt_capacity)
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
        let now = Instant::now();
        let mut receipt_quarantined_peers = self
            .receipt_quarantine_history
            .values()
            .filter(|until| **until > now)
            .count();
        let mut client_counts = ExecutionClientFamilyCounts::default();
        let mut body_request_ready_peers = 0usize;
        let mut receipt_request_ready_peers = 0usize;
        let mut body_request_paused_peers = 0usize;
        let mut receipt_request_paused_peers = 0usize;
        let mut active_body_requests = 0usize;
        let mut active_receipt_requests = 0usize;
        let mut timeout_penalized_peers = 0usize;
        let mut body_proven_peers = 0usize;
        let mut receipt_proven_peers = 0usize;
        let mut body_request_limit_total = 0usize;
        let mut receipt_request_limit_total = 0usize;
        let mut connected_ipv4_peers = 0usize;
        let mut connected_ipv6_peers = 0usize;
        let mut serving_ipv4_peers = 0usize;
        let mut serving_ipv6_peers = 0usize;
        for peer in self.peers.values() {
            let receipts_quarantined = peer
                .receipt_quarantined_until
                .is_some_and(|until| until > now);
            receipt_quarantined_peers =
                receipt_quarantined_peers.saturating_add(usize::from(receipts_quarantined));
            client_counts.record(&peer.client_version, peer.is_serving);
            if peer.remote_addr.ip().is_ipv4() {
                connected_ipv4_peers = connected_ipv4_peers.saturating_add(1);
                if peer.is_serving {
                    serving_ipv4_peers = serving_ipv4_peers.saturating_add(1);
                }
            } else {
                connected_ipv6_peers = connected_ipv6_peers.saturating_add(1);
                if peer.is_serving {
                    serving_ipv6_peers = serving_ipv6_peers.saturating_add(1);
                }
            }
            let body_paused = peer_request_is_paused(peer, PeerRequestKind::Bodies);
            let receipt_paused = peer_request_is_paused(peer, PeerRequestKind::Receipts);
            if peer.body_blocks_per_sec > 0.0 {
                body_proven_peers = body_proven_peers.saturating_add(1);
            }
            if peer.receipt_blocks_per_sec > 0.0 {
                receipt_proven_peers = receipt_proven_peers.saturating_add(1);
            }
            body_request_limit_total =
                body_request_limit_total.saturating_add(peer.body_request_limit);
            receipt_request_limit_total =
                receipt_request_limit_total.saturating_add(peer.receipt_request_limit);
            if body_paused {
                body_request_paused_peers = body_request_paused_peers.saturating_add(1);
            } else {
                body_request_ready_peers = body_request_ready_peers.saturating_add(1);
            }
            if receipt_paused {
                receipt_request_paused_peers = receipt_request_paused_peers.saturating_add(1);
            } else if !receipts_quarantined {
                receipt_request_ready_peers = receipt_request_ready_peers.saturating_add(1);
            }
            active_body_requests = active_body_requests
                .saturating_add(peer.body_active_requests)
                .saturating_add(peer.body_reserved_requests);
            active_receipt_requests = active_receipt_requests
                .saturating_add(peer.receipt_active_requests)
                .saturating_add(peer.receipt_reserved_requests);
            if peer.consecutive_timeouts > 0 {
                timeout_penalized_peers = timeout_penalized_peers.saturating_add(1);
            }
        }
        let body_request_limit_avg = average_peer_request_limit(
            body_request_limit_total,
            self.peers.len(),
            PeerRequestKind::Bodies,
        );
        let receipt_request_limit_avg = average_peer_request_limit(
            receipt_request_limit_total,
            self.peers.len(),
            PeerRequestKind::Receipts,
        );
        let p2p_download = self.session_metrics.p2p_download.snapshot(Instant::now());
        let (served_upload_bytes_per_sec, served_uploaded_payload_bytes) =
            self.serve_cache.p2p_upload_snapshot();

        ExecutionNetworkStatus {
            max_peers: self.max_peers,
            accepted_sessions: self.session_metrics.accepted_sessions,
            rejected_zero_tip_sessions: self.session_metrics.rejected_zero_tip_sessions,
            disconnected_sessions: self.session_metrics.disconnected_sessions,
            saturated_disconnects: self.session_metrics.saturated_disconnects,
            nonserving_disconnects: self.session_metrics.nonserving_disconnects,
            missing_fork_id_candidates: self.session_metrics.missing_fork_id_candidates,
            fork_id_rejected_candidates: self.session_metrics.fork_id_rejected_candidates,
            discovered_candidates: self.session_metrics.discovered_candidates,
            dns_discovered_candidates: self.session_metrics.dns_discovered_candidates,
            dns_family_rejected_candidates: self.session_metrics.dns_family_rejected_candidates,
            configured_bootnode_direct_candidates: self
                .session_metrics
                .configured_bootnode_direct_candidates,
            configured_bootnode_discovery_enrs: self
                .session_metrics
                .configured_bootnode_discovery_enrs,
            configured_bootnode_family_rejections: self
                .session_metrics
                .configured_bootnode_family_rejections,
            submitted_dials_total: self.session_metrics.submitted_dials_total,
            submitted_dial_expirations: self.session_metrics.submitted_dial_expirations,
            queued_candidates: self.pending.len(),
            pending_dials: self.pending_dials.len(),
            productive_peers: self.productive.len(),
            known_peers: self.known_peers.len(),
            saturated_peers: self.saturated_peers.len(),
            receipt_quarantined_peers,
            body_request_ready_peers,
            receipt_request_ready_peers,
            body_request_paused_peers,
            receipt_request_paused_peers,
            active_body_requests,
            active_receipt_requests,
            timeout_penalized_peers,
            body_proven_peers,
            receipt_proven_peers,
            body_request_limit_avg,
            receipt_request_limit_avg,
            historical_fetch_active: 0,
            historical_fetch_ready: 0,
            historical_fetch_completed: 0,
            historical_fetch_pending: 0,
            historical_fetch_expected_sequence: 0,
            historical_fetch_next_sequence: 0,
            historical_fetch_head_of_line_blocked: false,
            historical_fetch_head_of_line_completed: 0,
            historical_fetch_expected_active: false,
            historical_fetch_head_of_line_elapsed_ms: None,
            historical_prepare_active: 0,
            historical_prepare_ready: 0,
            historical_prepare_completed: 0,
            historical_prepare_pending: 0,
            historical_prepare_expected_sequence: 0,
            historical_ingest_active: false,
            historical_ingest_sequence: None,
            historical_ingest_elapsed_ms: None,
            historical_scheduler_body_slot_margin: 0,
            historical_scheduler_receipt_slot_margin: 0,
            historical_scheduler_write_backpressure: false,
            historical_scheduler_pipeline_depth: 0,
            historical_scheduler_buffer_depth: 0,
            historical_scheduler_ready_plan_depth: 0,
            historical_scheduler_critical_refill_limit: 0,
            historical_scheduler_write_refill_limit: 0,
            historical_scheduler_stale_role_retries: self
                .body_receipt_scheduler_metrics
                .stale_role_retries,
            historical_scheduler_prefix_reassignments: self
                .body_receipt_scheduler_metrics
                .prefix_reassignments,
            historical_scheduler_body_successes: self.body_receipt_scheduler_metrics.body_successes,
            historical_scheduler_receipt_successes: self
                .body_receipt_scheduler_metrics
                .receipt_successes,
            historical_scheduler_body_failures: self.body_receipt_scheduler_metrics.body_failures,
            historical_scheduler_receipt_failures: self
                .body_receipt_scheduler_metrics
                .receipt_failures,
            historical_scheduler_body_blocks: self.body_receipt_scheduler_metrics.body_blocks,
            historical_scheduler_receipt_blocks: self.body_receipt_scheduler_metrics.receipt_blocks,
            p2p_download_bytes_per_sec: p2p_download.bytes_per_sec,
            p2p_upload_bytes_per_sec: served_upload_bytes_per_sec,
            p2p_downloaded_payload_bytes: p2p_download.total_payload_bytes,
            p2p_uploaded_payload_bytes: served_uploaded_payload_bytes,
            connected_geth_peers: client_counts.connected_geth,
            connected_nethermind_peers: client_counts.connected_nethermind,
            connected_reth_peers: client_counts.connected_reth,
            connected_other_peers: client_counts.connected_other,
            connected_ipv4_peers,
            connected_ipv6_peers,
            serving_geth_peers: client_counts.serving_geth,
            serving_nethermind_peers: client_counts.serving_nethermind,
            serving_reth_peers: client_counts.serving_reth,
            serving_other_peers: client_counts.serving_other,
            serving_ipv4_peers,
            serving_ipv6_peers,
        }
    }

    /// Snapshot of restart seed peers suitable for writing to disk.
    ///
    /// Peers that already served data stay first because they are the highest
    /// value restart candidates. Dialable peers that completed Eth handshake
    /// and advertised a usable tip are retained after them so outbound-only
    /// and sparse-family modes can still build a usable known-peer table.
    pub fn known_peers(&self) -> Vec<NodeRecord> {
        let mut peers = Vec::with_capacity(MAX_PERSISTED_PEERS);

        for peer in &self.productive {
            if is_bootstrap_node(peer.id) {
                continue;
            }
            push_unique_peer(&mut peers, *peer);
            if peers.len() >= MAX_PERSISTED_PEERS {
                return peers;
            }
        }

        for peer_id in &self.peer_order {
            let Some(peer) = self.peers.get(peer_id) else {
                continue;
            };
            if !is_restart_seed_peer(
                peer.remote_record_is_dialable,
                peer.remote_status.latest_block,
                peer_receipts_are_quarantined(peer),
                peer.remote_record.id,
            ) {
                continue;
            }

            push_unique_peer(&mut peers, peer.remote_record);
            if peers.len() >= MAX_PERSISTED_PEERS {
                return peers;
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
            self.persist_known_peer_cache();
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

    pub(super) fn record_peer_partial_response(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        response_kind: &'static str,
        requested: usize,
        returned: usize,
        elapsed: Duration,
    ) {
        // A smaller reply can reflect the remote response-size limit. Record
        // useful progress, then adapt once for its size, without a second
        // latency adjustment or a failure penalty.
        self.record_peer_request_progress(peer_id, kind, returned, elapsed);
        self.reduce_peer_request_limit(peer_id, kind);
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
            super::compare_peer_scores_desc(left_score, right_score)
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
        let active_requests = peer_active_request_count(peer, kind);
        let base_rate = if measured_rate > 0.0 {
            measured_rate
        } else {
            1.0
        };
        let serving_bonus = if peer.is_serving { 4.0 } else { 0.0 };
        let timeout_penalty = f64::from(peer.consecutive_timeouts) * 8.0;
        load_adjusted_peer_rate(base_rate, active_requests) + serving_bonus - timeout_penalty
    }

    pub(crate) fn clear_body_receipt_active_requests(&mut self) {
        self.body_receipt_owners.reset(&mut self.peers);
    }

    pub(super) fn record_peer_request_success(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        blocks: usize,
        elapsed: Duration,
    ) {
        self.record_peer_request_progress(peer_id, kind, blocks, elapsed);
        self.adjust_peer_request_limit_after_success(peer_id, kind, blocks, elapsed);
    }

    fn record_peer_request_progress(
        &mut self,
        peer_id: PeerId,
        kind: PeerRequestKind,
        blocks: usize,
        elapsed: Duration,
    ) {
        self.reset_peer_timeout(peer_id);
        if matches!(kind, PeerRequestKind::Receipts) {
            self.receipt_quarantine_history.remove(&peer_id);
        }
        self.clear_peer_request_pause(peer_id, kind);
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

    pub(super) fn record_p2p_download_payload(&mut self, payload_bytes: u64, _elapsed: Duration) {
        self.session_metrics
            .p2p_download
            .record(payload_bytes, Instant::now());
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
        match request_error_disposition(error) {
            RequestErrorDisposition::Ignore => {
                if let RequestAttempt::ReceiptResourcesExceeded(resource) = error {
                    debug!(
                        peer = %peer_id,
                        block_hash = %resource.block_hash,
                        max_weight = resource.max_weight,
                        "receipt request exceeded its header-derived resource allowance"
                    );
                }
                false
            }
            RequestErrorDisposition::Pause => {
                self.pause_peer_requests(peer_id, kind, REQUEST_KIND_PAUSE_DURATION);
                self.record_soft_failure(peer_id);
                false
            }
            RequestErrorDisposition::Timeout => {
                self.reduce_peer_request_limit(peer_id, kind);
                let consecutive_timeouts = self.record_timeout(peer_id);
                self.pause_peer_requests(
                    peer_id,
                    kind,
                    request_timeout_pause_duration(consecutive_timeouts),
                );
                consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS
            }
            RequestErrorDisposition::DropBadProtocol => {
                self.network
                    .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
                true
            }
        }
    }

    pub(super) fn record_timeout(&mut self, peer_id: PeerId) -> u32 {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return MAX_CONSECUTIVE_TIMEOUTS;
        };
        peer.consecutive_timeouts = peer
            .consecutive_timeouts
            .saturating_add(1)
            .min(MAX_CONSECUTIVE_TIMEOUTS);
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
        self.receipt_quarantine_history
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
            self.persist_known_peer_cache();
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
            (
                became_serving,
                (became_serving && peer.remote_record_is_dialable).then_some(peer.remote_record),
            )
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
            self.persist_known_peer_cache();
        }
        became_serving
    }

    pub(super) fn remember_productive(&mut self, node: NodeRecord) -> bool {
        if self.peer_receipts_quarantined(node.id) {
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

        let known_changed = self.remember_known_peer(node);
        should_persist_productive_update(existing_index, known_changed)
    }

    pub(super) fn remember_known_peer(&mut self, node: NodeRecord) -> bool {
        let changed = upsert_known_peer(&mut self.known_peers, node);
        prune_known_hints(
            &mut self.known_peers,
            &self.configured_peer_ids,
            &self.productive,
            MAX_PERSISTED_PEERS,
        );
        changed && self.known_peers.iter().any(|peer| peer.id == node.id)
    }

    pub(super) fn remove_productive_peer(&mut self, peer_id: PeerId) -> bool {
        let productive_before = self.productive.len();
        self.productive.retain(|peer| peer.id != peer_id);
        let known_before = self.known_peers.len();
        self.known_peers.retain(|peer| peer.id != peer_id);
        productive_before != self.productive.len() || known_before != self.known_peers.len()
    }

    pub(super) fn persist_known_peer_cache(&mut self) {
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
                    "persisted known peer cache"
                );
            }
            Ok(false) => {}
            Err(error) => {
                warn!(
                    error = %error,
                    path = %self.known_peers_path.display(),
                    "failed to persist known peer cache"
                );
            }
        }
    }

    fn set_receipt_quarantine(&mut self, peer_id: PeerId, duration: Duration) {
        let now = Instant::now();
        let quarantined_until = now + duration;
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.receipt_quarantined_until = Some(
                peer.receipt_quarantined_until
                    .map_or(quarantined_until, |previous| {
                        previous.max(quarantined_until)
                    }),
            );
            self.receipt_quarantine_history.remove(&peer_id);
            peer.receipt_blocks_per_sec = 0.0;
        } else {
            retain_peer_backoff(
                &mut self.receipt_quarantine_history,
                peer_id,
                quarantined_until,
                now,
                MAX_RETAINED_PEER_BACKOFFS,
            );
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

        let limit = match kind {
            PeerRequestKind::Headers => request_limit_initial(kind),
            PeerRequestKind::Bodies => peer.body_request_limit,
            PeerRequestKind::Receipts => peer.receipt_request_limit,
        };
        effective_peer_request_limit(kind, peer.is_serving, limit)
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
        let paused_until = match kind {
            PeerRequestKind::Headers => return,
            PeerRequestKind::Bodies => &mut peer.body_paused_until,
            PeerRequestKind::Receipts => &mut peer.receipt_paused_until,
        };
        // Another in-flight request may fail after a longer pause was set.
        // Failures can extend that deadline; useful progress clears it.
        *paused_until = Some(paused_until.map_or(until, |previous| previous.max(until)));
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

fn unproven_request_limit(kind: PeerRequestKind) -> usize {
    match kind {
        PeerRequestKind::Headers => 1,
        PeerRequestKind::Bodies => UNPROVEN_BODY_REQUEST_LIMIT,
        PeerRequestKind::Receipts => UNPROVEN_RECEIPT_REQUEST_LIMIT,
    }
}

fn effective_peer_request_limit(kind: PeerRequestKind, is_serving: bool, limit: usize) -> usize {
    let limit = if is_serving {
        limit
    } else {
        limit.min(unproven_request_limit(kind))
    };
    limit.clamp(REQUEST_LIMIT_MIN, REQUEST_LIMIT_MAX)
}

fn average_peer_request_limit(total: usize, peers: usize, kind: PeerRequestKind) -> usize {
    if peers == 0 {
        request_limit_initial(kind)
    } else {
        total
            .div_ceil(peers)
            .clamp(REQUEST_LIMIT_MIN, REQUEST_LIMIT_MAX)
    }
}

fn adaptive_request_slot_capacity_for_peer(
    kind: PeerRequestKind,
    is_serving: bool,
    limit: usize,
    per_ready_peer_target: usize,
) -> usize {
    let effective_limit = effective_peer_request_limit(kind, is_serving, limit);
    effective_limit
        .saturating_mul(per_ready_peer_target.max(1))
        .div_ceil(request_limit_initial(kind).max(1))
        .clamp(1, REQUEST_LIMIT_MAX)
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

/// Keep finite disconnected scheduling history. Expiry precedes pressure eviction;
/// active-session restrictions must not be stored exclusively in this cache.
pub(super) fn retain_peer_backoff(
    history: &mut HashMap<PeerId, Instant>,
    peer_id: PeerId,
    until: Instant,
    now: Instant,
    limit: usize,
) {
    let until = history
        .get(&peer_id)
        .map_or(until, |previous| (*previous).max(until));
    insert_bounded_backoff(history, peer_id, until, now, limit, |deadline| *deadline);
}

pub(super) fn insert_bounded_backoff<T>(
    history: &mut HashMap<PeerId, T>,
    peer_id: PeerId,
    value: T,
    now: Instant,
    limit: usize,
    deadline: impl Fn(&T) -> Instant,
) {
    if deadline(&value) <= now || limit == 0 {
        return;
    }
    if !history.contains_key(&peer_id) {
        if history.len() >= limit {
            history.retain(|_, value| deadline(value) > now);
        }
        while history.len() >= limit {
            let Some(oldest) = history
                .iter()
                .min_by_key(|(_, value)| deadline(value))
                .map(|(id, _)| *id)
            else {
                break;
            };
            history.remove(&oldest);
        }
    }
    history.insert(peer_id, value);
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

pub(super) fn scan_known_hint_indices(
    len: usize,
    cursor: usize,
    mut admit: impl FnMut(usize) -> bool,
) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let start = cursor % len;
    let mut index = start;
    let mut next_cursor = start;
    let mut admitted = 0;
    for _ in 0..len {
        let next = if index + 1 == len { 0 } else { index + 1 };
        if admit(index) {
            admitted += 1;
            next_cursor = next;
        }
        index = next;
    }
    (admitted, next_cursor)
}

pub(super) fn retry_hint_is_eligible(families: DialAddressFamilies, node: &NodeRecord) -> bool {
    node.tcp_port != 0 && !is_bootstrap_node(node.id) && node_matches_dial_families(families, node)
}

pub(super) fn bounded_initial_known_peers(
    peers: impl IntoIterator<Item = NodeRecord>,
    families: DialAddressFamilies,
    limit: usize,
) -> Vec<NodeRecord> {
    let mut retained = Vec::new();
    let mut ids = HashSet::new();
    for peer in peers {
        if retained.len() == limit {
            break;
        }
        if retry_hint_is_eligible(families, &peer) && ids.insert(peer.id) {
            retained.push(peer);
        }
    }
    retained
}

pub(super) fn prune_known_hints(
    known: &mut Vec<NodeRecord>,
    configured: &HashSet<PeerId>,
    productive: &VecDeque<NodeRecord>,
    learned_limit: usize,
) {
    if known.len() <= learned_limit {
        return;
    }
    let mut excess = known
        .iter()
        .filter(|node| !configured.contains(&node.id))
        .count()
        .saturating_sub(learned_limit);
    if excess == 0 {
        return;
    }
    let productive_ids: HashSet<_> = productive.iter().map(|node| node.id).collect();
    known.retain(|node| {
        if excess > 0 && !configured.contains(&node.id) && !productive_ids.contains(&node.id) {
            excess -= 1;
            false
        } else {
            true
        }
    });
}

pub(super) fn admit_pending_hint(
    pending: &mut HashMap<PeerId, NodeRecord>,
    node: NodeRecord,
    configured: &HashSet<PeerId>,
    productive: &VecDeque<NodeRecord>,
    known: &[NodeRecord],
    limit: usize,
) -> bool {
    if let Some(existing) = pending.get_mut(&node.id) {
        *existing = node;
        return false;
    }
    if pending.len() < limit {
        pending.insert(node.id, node);
        return true;
    }
    let priority = if productive.iter().any(|peer| peer.id == node.id) {
        3
    } else if configured.contains(&node.id) {
        2
    } else if known.iter().any(|peer| peer.id == node.id) {
        1
    } else {
        return false;
    };
    // Build bounded lookup sets only for a full queue and a retry candidate.
    // Each victim comparison is O(1), rather than scanning all retry lists.
    let productive_ids: HashSet<_> = productive.iter().map(|peer| peer.id).collect();
    let known_ids: HashSet<_> = known.iter().map(|peer| peer.id).collect();
    let victim = pending
        .keys()
        .filter_map(|peer| {
            let rank = if productive_ids.contains(peer) {
                3
            } else if configured.contains(peer) {
                2
            } else if known_ids.contains(peer) {
                1
            } else {
                0
            };
            (rank < priority).then_some((rank, *peer))
        })
        .min();
    let Some((_, victim)) = victim else {
        return false;
    };
    pending.remove(&victim);
    pending.insert(node.id, node);
    true
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

pub(super) fn is_restart_seed_peer(
    remote_record_is_dialable: bool,
    latest_block: Option<u64>,
    receipt_quarantined: bool,
    peer_id: PeerId,
) -> bool {
    remote_record_is_dialable
        && latest_block.is_some_and(|latest| latest > 0)
        && !receipt_quarantined
        && !is_bootstrap_node(peer_id)
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ExecutionClientFamily {
    Geth,
    Nethermind,
    Reth,
    Other,
}

pub(crate) fn execution_client_family(client_version: &str) -> ExecutionClientFamily {
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

pub(super) fn advertised_status_range(cached_range: Option<(u64, u64, B256)>) -> (u64, u64, B256) {
    if let Some((earliest, latest, latest_hash)) = cached_range
        && earliest <= latest
        && !latest_hash.is_zero()
    {
        return (earliest, latest, latest_hash);
    }

    // Consensus head knowledge does not imply body/receipt availability. The
    // provider always serves mainnet genesis, even when its optional cache is empty.
    (0, 0, MAINNET.genesis_hash())
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

fn request_error_disposition(error: &RequestAttempt) -> RequestErrorDisposition {
    match error {
        RequestAttempt::ContinuationDeadline | RequestAttempt::ReceiptResourcesExceeded(_) => {
            RequestErrorDisposition::Ignore
        }
        RequestAttempt::ReceiptResponseOverflow { .. } => RequestErrorDisposition::DropBadProtocol,
        RequestAttempt::Disconnected => RequestErrorDisposition::Pause,
        RequestAttempt::Request(request_error) => match request_error {
            reth_network::p2p::error::RequestError::Timeout => RequestErrorDisposition::Timeout,
            reth_network::p2p::error::RequestError::BadResponse
            | reth_network::p2p::error::RequestError::UnsupportedCapability => {
                RequestErrorDisposition::DropBadProtocol
            }
            reth_network::p2p::error::RequestError::ChannelClosed
            | reth_network::p2p::error::RequestError::ConnectionDropped => {
                RequestErrorDisposition::Pause
            }
        },
    }
}

fn request_timeout_pause_duration(consecutive_timeouts: u32) -> Duration {
    REQUEST_KIND_PAUSE_DURATION
        .saturating_mul(consecutive_timeouts.max(1))
        .min(REQUEST_TIMEOUT_PAUSE_MAX_DURATION)
}

fn peer_active_request_count(peer: &ActivePeer, kind: PeerRequestKind) -> usize {
    match kind {
        PeerRequestKind::Headers => 0,
        PeerRequestKind::Bodies => peer
            .body_active_requests
            .saturating_add(peer.body_reserved_requests),
        PeerRequestKind::Receipts => peer
            .receipt_active_requests
            .saturating_add(peer.receipt_reserved_requests),
    }
}

fn load_adjusted_peer_rate(base_rate: f64, active_requests: usize) -> f64 {
    base_rate / (1.0 + active_requests as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn retention_node(id: u16) -> NodeRecord {
        let mut bytes = [0; 64];
        bytes[..2].copy_from_slice(&id.to_be_bytes());
        NodeRecord::new_with_ports(
            "127.0.0.1".parse().unwrap(),
            30303,
            None,
            PeerId::from_slice(&bytes),
        )
    }

    #[test]
    fn initial_retry_limit_applies_after_eligibility_and_deduplication() {
        let first = retention_node(1);
        let second = retention_node(2);
        let mut no_tcp = first;
        no_tcp.tcp_port = 0;
        let mut ipv6 = second;
        ipv6.address = "::1".parse().unwrap();
        let builtin = mainnet_nodes().into_iter().next().unwrap();
        let inputs = [no_tcp, builtin, ipv6, no_tcp, first, first, second];
        assert_eq!(
            bounded_initial_known_peers(inputs, DialAddressFamilies::IPV4, 2),
            vec![first, second]
        );
        assert_eq!(
            bounded_initial_known_peers(inputs, DialAddressFamilies::IPV6, 2),
            vec![ipv6]
        );
        assert_eq!(
            bounded_initial_known_peers(inputs, DialAddressFamilies::BOTH, 2),
            vec![ipv6, first]
        );
        assert!(!retry_hint_is_eligible(DialAddressFamilies::BOTH, &no_tcp));
        assert!(!retry_hint_is_eligible(DialAddressFamilies::BOTH, &builtin));
    }

    #[test]
    fn retry_hint_retention_preserves_productive_configured_and_first_seed_order() {
        let nodes: Vec<_> = (1..=6).map(retention_node).collect();
        assert_eq!(
            bounded_initial_known_peers(
                [nodes[0], nodes[0], nodes[1], nodes[2]],
                DialAddressFamilies::BOTH,
                2
            ),
            nodes[..2]
        );
        let mut duplicate = nodes[0];
        duplicate.tcp_port = 30304;
        assert_eq!(
            bounded_initial_known_peers(
                [nodes[0], duplicate, nodes[1]],
                DialAddressFamilies::BOTH,
                2
            ),
            nodes[..2]
        );
        assert!(
            bounded_initial_known_peers(nodes.clone(), DialAddressFamilies::BOTH, 0).is_empty()
        );
        let mut known = nodes.clone();
        let configured = HashSet::from([nodes[0].id, nodes[1].id]);
        let productive = VecDeque::from([nodes[4], nodes[5]]);
        prune_known_hints(&mut known, &configured, &productive, 2);
        assert_eq!(known, vec![nodes[0], nodes[1], nodes[4], nodes[5]]);
        prune_known_hints(&mut known, &configured, &productive, 2);
        assert_eq!(known.len(), 4);
        // Pressure protection does not recreate explicitly removed records.
        known.retain(|node| node.id != nodes[0].id);
        prune_known_hints(&mut known, &configured, &productive, 2);
        assert!(!known.contains(&nodes[0]));
        known.push(nodes[2]);
        prune_known_hints(&mut known, &configured, &productive, 2);
        assert!(
            !known.contains(&nodes[2]),
            "absent configured entries do not add learned capacity"
        );
    }

    #[test]
    fn retry_hint_retention_bounds_churn_and_releases_old_productive_hints() {
        let ipv4 = retention_node(2000);
        let mut ipv6 = retention_node(2001);
        ipv6.address = "::1".parse().unwrap();
        let configured = HashSet::from([ipv4.id, ipv6.id]);
        let mut productive = VecDeque::from([retention_node(1), retention_node(2)]);
        let mut known = vec![ipv4, ipv6, productive[0], productive[1]];
        for id in 3..=1024 {
            let node = retention_node(id);
            assert!(upsert_known_peer(&mut known, node));
            prune_known_hints(&mut known, &configured, &productive, 4);
            assert!(known.len() <= 6);
            assert!(known.contains(&ipv4) && known.contains(&ipv6));
            assert!(productive.iter().all(|peer| known.contains(peer)));
        }
        assert_eq!(
            known,
            vec![
                ipv4,
                ipv6,
                retention_node(1),
                retention_node(2),
                retention_node(1023),
                retention_node(1024)
            ]
        );
        productive.pop_front();
        productive.push_back(retention_node(1025));
        upsert_known_peer(&mut known, retention_node(1025));
        prune_known_hints(&mut known, &configured, &productive, 4);
        assert_eq!(known.len(), 6);
        assert!(!known.contains(&retention_node(1)));
        assert!(productive.iter().all(|peer| known.contains(peer)));
    }

    #[test]
    fn pending_retry_priority_replaces_only_strictly_lower_hints() {
        let nodes: Vec<_> = (1..=6).map(retention_node).collect();
        let configured = HashSet::from([nodes[3].id]);
        let productive = VecDeque::from([nodes[4]]);
        let known = vec![nodes[2], nodes[3], nodes[4]];
        let mut pending = HashMap::from([(nodes[0].id, nodes[0]), (nodes[1].id, nodes[1])]);
        assert!(!admit_pending_hint(
            &mut pending,
            nodes[5],
            &configured,
            &productive,
            &known,
            2
        ));
        assert!(admit_pending_hint(
            &mut pending,
            nodes[2],
            &configured,
            &productive,
            &known,
            2
        ));
        assert!(admit_pending_hint(
            &mut pending,
            nodes[3],
            &configured,
            &productive,
            &known,
            2
        ));
        assert!(admit_pending_hint(
            &mut pending,
            nodes[4],
            &configured,
            &productive,
            &known,
            2
        ));
        assert_eq!(pending.len(), 2);
        assert!(pending.contains_key(&nodes[3].id));
        assert!(pending.contains_key(&nodes[4].id));
        assert!(!admit_pending_hint(
            &mut pending,
            nodes[2],
            &configured,
            &productive,
            &known,
            2
        ));
        let mut updated = nodes[3];
        updated.tcp_port = 30304;
        assert!(!admit_pending_hint(
            &mut pending,
            updated,
            &configured,
            &productive,
            &known,
            2
        ));
        assert_eq!(pending[&updated.id], updated);
    }

    #[test]
    fn indexed_retry_scan_rotates_after_admissions_not_failed_full_passes() {
        let nodes: Vec<_> = (1..=4).map(retention_node).collect();
        let configured: HashSet<_> = nodes.iter().map(|node| node.id).collect();
        let mut pending = HashMap::new();
        let productive = VecDeque::new();
        let mut scan = |cursor| {
            scan_known_hint_indices(nodes.len(), cursor, |index| {
                admit_pending_hint(
                    &mut pending,
                    nodes[index],
                    &configured,
                    &productive,
                    &nodes,
                    2,
                )
            })
        };
        let (count, cursor) = scan(0);
        assert_eq!((count, cursor), (2, 2));
        assert_eq!(scan(cursor), (0, cursor));
        pending.remove(&nodes[0].id);
        let (count, cursor) = scan_known_hint_indices(nodes.len(), cursor, |index| {
            admit_pending_hint(
                &mut pending,
                nodes[index],
                &configured,
                &productive,
                &nodes,
                2,
            )
        });
        assert_eq!((count, cursor), (1, 3));
        assert!(pending.contains_key(&nodes[2].id));
        assert_eq!(nodes.len(), 4);
        assert_eq!(
            scan_known_hint_indices(0, usize::MAX, |_| panic!("empty")),
            (0, 0)
        );
        assert_eq!(scan_known_hint_indices(2, 9, |_| false), (0, 1));
    }

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
    fn restart_seed_policy_keeps_reachable_non_bootstrap_peers() {
        let peer_id = PeerId::repeat_byte(0x42);

        assert!(is_restart_seed_peer(true, Some(1), false, peer_id));
        assert!(!is_restart_seed_peer(false, Some(1), false, peer_id));
        assert!(!is_restart_seed_peer(true, Some(0), false, peer_id));
        assert!(!is_restart_seed_peer(true, None, false, peer_id));
        assert!(!is_restart_seed_peer(true, Some(1), true, peer_id));

        let bootnode = mainnet_nodes()
            .into_iter()
            .next()
            .expect("mainnet bootnodes should not be empty");
        assert!(!is_restart_seed_peer(true, Some(1), false, bootnode.id));
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
            56
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
    fn unproven_body_receipt_peers_start_with_smaller_request_limits() {
        assert_eq!(
            effective_peer_request_limit(
                PeerRequestKind::Bodies,
                false,
                BODY_REQUEST_LIMIT_INITIAL
            ),
            UNPROVEN_BODY_REQUEST_LIMIT
        );
        assert_eq!(
            effective_peer_request_limit(
                PeerRequestKind::Receipts,
                false,
                RECEIPT_REQUEST_LIMIT_INITIAL
            ),
            UNPROVEN_RECEIPT_REQUEST_LIMIT
        );
        assert_eq!(
            effective_peer_request_limit(PeerRequestKind::Bodies, true, REQUEST_LIMIT_MAX),
            REQUEST_LIMIT_MAX
        );
    }

    #[test]
    fn average_request_limit_reports_initial_without_peers() {
        assert_eq!(
            average_peer_request_limit(0, 0, PeerRequestKind::Bodies),
            BODY_REQUEST_LIMIT_INITIAL
        );
        assert_eq!(
            average_peer_request_limit(96, 2, PeerRequestKind::Receipts),
            48
        );
    }

    #[test]
    fn adaptive_request_slot_capacity_scales_relative_to_initial_limit() {
        assert_eq!(
            adaptive_request_slot_capacity_for_peer(
                PeerRequestKind::Bodies,
                true,
                BODY_REQUEST_LIMIT_INITIAL,
                6
            ),
            6
        );
        assert_eq!(
            adaptive_request_slot_capacity_for_peer(
                PeerRequestKind::Bodies,
                true,
                BODY_REQUEST_LIMIT_INITIAL / 3,
                6
            ),
            2
        );
        assert_eq!(
            adaptive_request_slot_capacity_for_peer(
                PeerRequestKind::Receipts,
                true,
                REQUEST_LIMIT_MAX,
                6
            ),
            16
        );
        assert_eq!(
            adaptive_request_slot_capacity_for_peer(
                PeerRequestKind::Receipts,
                false,
                REQUEST_LIMIT_MAX,
                6
            ),
            2
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
        assert_eq!(
            advertised_status_range(Some((7, 9, B256::repeat_byte(9)))),
            (7, 9, B256::repeat_byte(9)),
        );
    }

    #[test]
    fn advertised_status_range_empty_cache_serves_genesis() {
        assert_eq!(
            advertised_status_range(None),
            (0, 0, MAINNET.genesis_hash())
        );
    }

    #[test]
    fn advertised_status_range_ignores_invalid_cached_window() {
        for cached in [
            Some((11, 9, B256::repeat_byte(9))),
            Some((9, 9, B256::ZERO)),
        ] {
            assert_eq!(
                advertised_status_range(cached),
                (0, 0, MAINNET.genesis_hash())
            );
        }
    }

    #[test]
    fn active_request_load_reduces_peer_score_without_blacklisting() {
        assert_eq!(load_adjusted_peer_rate(120.0, 0), 120.0);
        assert_eq!(load_adjusted_peer_rate(120.0, 1), 60.0);
        assert_eq!(load_adjusted_peer_rate(120.0, 2), 40.0);
    }

    #[test]
    fn timeout_pause_scales_with_repeated_timeouts() {
        assert_eq!(
            request_timeout_pause_duration(0),
            REQUEST_KIND_PAUSE_DURATION
        );
        assert_eq!(
            request_timeout_pause_duration(2),
            REQUEST_KIND_PAUSE_DURATION * 2
        );
        assert_eq!(
            request_timeout_pause_duration(100),
            REQUEST_TIMEOUT_PAUSE_MAX_DURATION
        );
    }

    #[test]
    fn transient_transport_request_errors_pause_without_dropping_peer() {
        assert_eq!(
            request_error_disposition(&RequestAttempt::Disconnected),
            RequestErrorDisposition::Pause
        );
        assert_eq!(
            request_error_disposition(&RequestAttempt::Request(
                reth_network::p2p::error::RequestError::ChannelClosed
            )),
            RequestErrorDisposition::Pause
        );
        assert_eq!(
            request_error_disposition(&RequestAttempt::Request(
                reth_network::p2p::error::RequestError::ConnectionDropped
            )),
            RequestErrorDisposition::Pause
        );
    }

    #[test]
    fn protocol_request_errors_still_drop_peer() {
        assert_eq!(
            request_error_disposition(&RequestAttempt::Request(
                reth_network::p2p::error::RequestError::BadResponse
            )),
            RequestErrorDisposition::DropBadProtocol
        );
        assert_eq!(
            request_error_disposition(&RequestAttempt::Request(
                reth_network::p2p::error::RequestError::UnsupportedCapability
            )),
            RequestErrorDisposition::DropBadProtocol
        );
    }
}
