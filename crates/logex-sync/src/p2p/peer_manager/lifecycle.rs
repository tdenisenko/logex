use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use tokio::time::timeout;
use tracing::{debug, trace, warn};

use super::*;

impl PeerManager {
    /// Gracefully stop the network manager and wait for the background task.
    pub async fn shutdown(&mut self) {
        match timeout(NETWORK_SHUTDOWN_TIMEOUT, self.network.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                warn!(%error, "failed to shut down p2p network cleanly");
            }
            Err(_) => {
                warn!(
                    ?NETWORK_SHUTDOWN_TIMEOUT,
                    "timed out waiting for p2p network shutdown acknowledgement"
                );
            }
        }

        self.drain_shutdown_events(NETWORK_SHUTDOWN_DRAIN_TIMEOUT)
            .await;
        self.network_events = Box::pin(tokio_stream::empty());
        self.discovery_events = Box::pin(tokio_stream::empty());
        self.dns_discovery_updates = None;

        if let Some(task) = self.network_task.take() {
            debug!(
                ?NETWORK_SHUTDOWN_DRAIN_TIMEOUT,
                "aborting long-lived reth network task after graceful disconnect drain"
            );
            abort_and_wait(task, "p2p network task").await;
        }

        if let Some(task) = self.eth_request_task.take() {
            abort_and_wait(task, "eth request handler task").await;
        }

        if let Some(task) = self.dns_discovery_task.take() {
            abort_and_wait(task, "execution DNS discovery task").await;
        }
    }

    pub(super) async fn drain_shutdown_events(&mut self, max_wait: Duration) {
        let deadline = Instant::now() + max_wait;
        while Instant::now() < deadline && (!self.peers.is_empty() || !self.pending.is_empty()) {
            let remaining = (deadline - Instant::now()).min(Duration::from_millis(100));
            if !self.wait_for_activity(remaining).await {
                break;
            }
        }
    }

    pub(super) fn seed_known_peers(&mut self) {
        let queued = self.queue_known_peers();

        if self.network_activated && queued > 0 {
            self.dial_pending_peers(self.max_peers);
        }
    }

    pub(super) fn queue_known_peers(&mut self) -> usize {
        let now = Instant::now();
        self.prune_saturated_peers(now);
        self.prune_receipt_quarantined_peers(now);
        let mut queued = 0usize;
        for peer in self.known_peers.clone() {
            if is_bootstrap_node(peer.id)
                || peer.tcp_port == 0
                || self.peers.contains_key(&peer.id)
                || self.pending.contains_key(&peer.id)
                || self.recently_saturated(peer.id, now)
                || self.recently_submitted(peer.id, now)
            {
                continue;
            }

            self.remember_pending(peer);
            queued += 1;
        }

        queued
    }

    pub(crate) fn drain_events_now(&mut self) {
        while let Some(event) = self.network_events.next().now_or_never().flatten() {
            self.handle_network_event(event);
        }
        while let Some(event) = self.discovery_events.next().now_or_never().flatten() {
            self.handle_discovery_event(event);
        }
        self.drain_dns_discovery_updates_now();
        let now = Instant::now();
        self.prune_saturated_peers(now);
        self.prune_receipt_quarantined_peers(now);
        self.prune_stale_nonserving_peers();
        self.fill_open_peer_slots();
    }

    pub(super) fn dial_pending_peers(&mut self, target: usize) {
        if !self.network_activated || self.peers.len() >= target || self.pending.is_empty() {
            return;
        }

        let now = Instant::now();
        self.prune_submitted_dials(now);
        let dial_capacity = MAX_CONCURRENT_OUTBOUND_DIALS.saturating_sub(self.pending_dials.len());
        if dial_capacity == 0 {
            return;
        }
        let active_or_submitted = self.peers.len().saturating_add(self.pending_dials.len());
        let open_slots = target
            .saturating_sub(active_or_submitted)
            .min(dial_capacity);
        if open_slots == 0 {
            return;
        }
        let dial_budget = open_slots.clamp(1, MAX_PENDING_DIALS_PER_REFILL);

        let candidates: Vec<_> = self
            .pending
            .values()
            .copied()
            .filter(|node| {
                !is_bootstrap_node(node.id)
                    && node_matches_bind_ip(self.bind_ip, node)
                    && node.tcp_port > 0
                    && !self.peers.contains_key(&node.id)
                    && !self.recently_saturated(node.id, now)
                    && !self.recently_submitted(node.id, now)
            })
            .collect();
        let candidates = select_dial_candidates(candidates, &self.productive, dial_budget);

        if candidates.is_empty() {
            return;
        }

        for node in &candidates {
            self.pending.remove(&node.id);
            self.pending_dials.insert(node.id, now);
            self.network.connect_peer_kind(
                node.id,
                PeerKind::Basic,
                node.tcp_addr(),
                Some(node.udp_addr()),
            );
        }

        trace!(
            submitted_peers = candidates.len(),
            connected_peers = self.peers.len(),
            submitted_dials = self.pending_dials.len(),
            pending_peers = self.pending.len(),
            target,
            "submitted discovered execution peers to execution peer scheduler"
        );
    }

    pub(super) fn fill_open_peer_slots(&mut self) {
        let active_or_submitted = self.peers.len().saturating_add(self.pending_dials.len());
        if self.network_activated && active_or_submitted < self.max_peers {
            self.queue_known_peers();
        }
        self.dial_pending_peers(self.max_peers);
    }

    pub(super) async fn wait_for_activity(&mut self, max_wait: Duration) -> bool {
        if self.drain_dns_discovery_updates_now() > 0 {
            return true;
        }

        let delay = tokio::time::sleep(max_wait);
        tokio::pin!(delay);

        if let Some(dns_updates) = self.dns_discovery_updates.as_mut() {
            tokio::select! {
                maybe_update = dns_updates.next() => {
                    if let Some(update) = maybe_update {
                        self.handle_dns_discovery_update(update);
                        true
                    } else {
                        self.dns_discovery_updates = None;
                        warn!("execution DNS discovery update stream closed");
                        false
                    }
                }
                maybe_event = self.network_events.next() => {
                    if let Some(event) = maybe_event {
                        self.handle_network_event(event);
                        true
                    } else {
                        warn!("network event stream closed");
                        false
                    }
                }
                maybe_event = self.discovery_events.next() => {
                    if let Some(event) = maybe_event {
                        self.handle_discovery_event(event);
                        true
                    } else {
                        warn!("discovery event stream closed");
                        false
                    }
                }
                _ = &mut delay => false,
            }
        } else {
            tokio::select! {
                maybe_event = self.network_events.next() => {
                    if let Some(event) = maybe_event {
                        self.handle_network_event(event);
                        true
                    } else {
                        warn!("network event stream closed");
                        false
                    }
                }
                maybe_event = self.discovery_events.next() => {
                    if let Some(event) = maybe_event {
                        self.handle_discovery_event(event);
                        true
                    } else {
                        warn!("discovery event stream closed");
                        false
                    }
                }
                _ = &mut delay => false,
            }
        }
    }

    fn drain_dns_discovery_updates_now(&mut self) -> usize {
        let mut count = 0usize;
        loop {
            let Some(update) = self
                .dns_discovery_updates
                .as_mut()
                .and_then(|updates| updates.next().now_or_never().flatten())
            else {
                break;
            };
            self.handle_dns_discovery_update(update);
            count = count.saturating_add(1);
        }
        count
    }

    pub(super) fn handle_network_event(
        &mut self,
        event: NetworkEvent<PeerRequest<LogexNetworkPrimitives>>,
    ) {
        match event {
            NetworkEvent::Peer(reth_network::events::PeerEvent::SessionClosed {
                peer_id,
                reason,
            }) => {
                let backoff_saturated = self.should_backoff_saturated_peer(peer_id, reason);
                self.record_session_closed_metrics(peer_id, reason);
                self.log_session_closed(peer_id, reason);
                self.drop_unproductive_known_peer(peer_id, reason);
                let removed = self.remove_peer(peer_id);
                if backoff_saturated {
                    self.backoff_saturated_peer(peer_id);
                } else if should_retry_disconnected_peer(reason)
                    && let Some(peer) = removed.as_ref()
                    && self.should_requeue_disconnected_peer(peer, reason)
                {
                    self.requeue_disconnected_peer(peer);
                }
            }
            NetworkEvent::Peer(reth_network::events::PeerEvent::PeerRemoved(peer_id)) => {
                self.remove_peer(peer_id);
            }
            NetworkEvent::Peer(reth_network::events::PeerEvent::PeerAdded(_)) => {}
            NetworkEvent::Peer(_) => {}
            NetworkEvent::ActivePeerSession { info, messages } => {
                self.insert_peer(info, messages);
            }
        }
    }

    pub(super) fn handle_discovery_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::NewNode(DiscoveredEvent::EventQueued {
                peer_id,
                addr,
                fork_id,
                ..
            }) => {
                if let Some(fork_id) = fork_id
                    && !self.is_compatible_fork_id(fork_id)
                {
                    self.session_metrics.fork_id_rejected_candidates = self
                        .session_metrics
                        .fork_id_rejected_candidates
                        .saturating_add(1);
                    return;
                }
                if fork_id.is_none() {
                    self.session_metrics.missing_fork_id_candidates = self
                        .session_metrics
                        .missing_fork_id_candidates
                        .saturating_add(1);
                }
                let node = NodeRecord::new_with_ports(
                    addr.tcp().ip(),
                    addr.tcp().port(),
                    addr.udp().map(|socket| socket.port()),
                    peer_id,
                );
                trace!(
                    peer = %peer_id,
                    addr = %node.tcp_addr(),
                    ?fork_id,
                    "queued execution peer discovered for compatible or unverified fork id"
                );
                self.remember_pending(node);
            }
            DiscoveryEvent::EnrForkId(node, fork_id) => {
                if !self.is_compatible_fork_id(fork_id) {
                    self.session_metrics.fork_id_rejected_candidates = self
                        .session_metrics
                        .fork_id_rejected_candidates
                        .saturating_add(1);
                    trace!(
                        peer = %node.id,
                        ?fork_id,
                        local_fork_id = ?self.fork_filter.current(),
                        "ignoring execution peer with incompatible fork id"
                    );
                    return;
                }
                self.remember_pending(node);
            }
        }
    }

    pub(super) fn handle_dns_discovery_update(&mut self, update: DnsNodeRecordUpdate) {
        if let Some(fork_id) = update.fork_id
            && !self.is_compatible_fork_id(fork_id)
        {
            self.session_metrics.fork_id_rejected_candidates = self
                .session_metrics
                .fork_id_rejected_candidates
                .saturating_add(1);
            return;
        }
        if update.fork_id.is_none() {
            self.session_metrics.missing_fork_id_candidates = self
                .session_metrics
                .missing_fork_id_candidates
                .saturating_add(1);
        }

        let Some(node) = dns_node_record_for_bind_ip(self.bind_ip, &update) else {
            trace!(
                peer = %update.node_record.id,
                bind_ip = %self.bind_ip,
                node_addr = %update.node_record.tcp_addr(),
                has_ip4 = update.enr.ip4().is_some(),
                has_tcp4 = update.enr.tcp4().is_some(),
                has_ip6 = update.enr.ip6().is_some(),
                has_tcp6 = update.enr.tcp6().is_some(),
                "ignoring DNS execution peer without a dialable endpoint for configured p2p address family"
            );
            return;
        };
        trace!(
            peer = %node.id,
            fork_id = ?update.fork_id,
            addr = %node.tcp_addr(),
            "queued execution peer discovered from family-aware DNS"
        );
        self.remember_pending(node);
    }

    pub(super) fn insert_peer(
        &mut self,
        info: reth_network::events::SessionInfo,
        messages: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    ) {
        if info.version < EthVersion::Eth69 {
            debug!(
                peer = %info.peer_id,
                remote_addr = %info.remote_addr,
                version = ?info.version,
                "disconnecting execution peer below eth/69"
            );
            self.remove_unsupported_session(info.peer_id);
            return;
        }

        let latest_block = info.status.latest_block;
        if latest_block.is_none_or(|latest| latest == 0) {
            debug!(
                peer = %info.peer_id,
                remote_addr = %info.remote_addr,
                version = ?info.version,
                "disconnecting execution peer without a usable advertised block tip"
            );
            self.pending.remove(&info.peer_id);
            self.pending_dials.remove(&info.peer_id);
            self.session_metrics.rejected_zero_tip_sessions = self
                .session_metrics
                .rejected_zero_tip_sessions
                .saturating_add(1);
            self.network
                .reputation_change(info.peer_id, ReputationChangeKind::Dropped);
            self.network.disconnect_peer(info.peer_id);
            self.network.remove_peer(info.peer_id, PeerKind::Basic);
            return;
        }

        let advertised_record = self.pending.remove(&info.peer_id);
        let remote_record_is_dialable = advertised_record.is_some();
        let record =
            advertised_record.unwrap_or_else(|| NodeRecord::new(info.remote_addr, info.peer_id));
        self.pending_dials.remove(&info.peer_id);

        let was_productive = self.productive.iter().any(|peer| peer.id == info.peer_id);
        let receipt_quarantined_until = self.receipt_quarantined_peers.get(&info.peer_id).copied();
        let body_request_limit = inherited_peer_request_limit(
            self.peers.values().map(|peer| peer.body_request_limit),
            PeerRequestKind::Bodies,
        );
        let receipt_request_limit = inherited_peer_request_limit(
            self.peers.values().map(|peer| peer.receipt_request_limit),
            PeerRequestKind::Receipts,
        );
        let peer = ActivePeer {
            sender: messages,
            remote_record: record,
            remote_record_is_dialable,
            remote_status: *info.status,
            client_version: info.client_version,
            version: info.version,
            is_serving: false,
            consecutive_timeouts: 0,
            header_blocks_per_sec: 0.0,
            body_blocks_per_sec: 0.0,
            receipt_blocks_per_sec: 0.0,
            body_active_requests: 0,
            receipt_active_requests: 0,
            body_reserved_requests: 0,
            receipt_reserved_requests: 0,
            body_request_limit,
            receipt_request_limit,
            body_paused_until: None,
            receipt_paused_until: None,
            receipt_quarantined_until,
            connected_at: Instant::now(),
        };

        self.peers.insert(info.peer_id, peer);
        self.session_metrics.accepted_sessions =
            self.session_metrics.accepted_sessions.saturating_add(1);
        self.peer_order.retain(|peer_id| *peer_id != info.peer_id);
        if was_productive {
            self.peer_order.push_front(info.peer_id);
        } else {
            self.peer_order.push_back(info.peer_id);
        }
        self.rebalance_request_cursor();

        debug!(
            peer = %info.peer_id,
            remote_addr = %info.remote_addr,
            latest_block = ?latest_block,
            version = ?info.version,
            "peer session established"
        );
    }

    pub(super) fn remember_pending(&mut self, node: NodeRecord) {
        let now = Instant::now();
        self.prune_saturated_peers(now);
        if is_bootstrap_node(node.id) {
            return;
        }
        if node.tcp_port == 0 {
            return;
        }
        if !node_matches_bind_ip(self.bind_ip, &node) {
            trace!(
                peer = %node.id,
                addr = %node.tcp_addr(),
                bind_ip = %self.bind_ip,
                "ignoring execution peer outside configured p2p address family"
            );
            return;
        }
        if self.peers.contains_key(&node.id) {
            return;
        }
        if self.recently_saturated(node.id, now) {
            return;
        }
        if self.pending.len() >= MAX_TRACKED_PENDING && !self.pending.contains_key(&node.id) {
            return;
        }
        self.pending.insert(node.id, node);
    }

    pub(super) fn remove_dead_peers(&mut self, dead_peers: &HashSet<PeerId>) {
        if dead_peers.is_empty() {
            return;
        }

        for peer_id in dead_peers {
            self.network.disconnect_peer(*peer_id);
            self.network.remove_peer(*peer_id, PeerKind::Basic);
            self.remove_peer(*peer_id);
        }
    }

    pub(super) fn forget_peer(&mut self, peer_id: PeerId) -> bool {
        self.peers.remove(&peer_id);
        self.pending.remove(&peer_id);
        self.pending_dials.remove(&peer_id);
        self.saturated_peers.remove(&peer_id);
        self.receipt_quarantined_peers.remove(&peer_id);
        self.peer_order.retain(|id| *id != peer_id);
        self.rebalance_request_cursor();
        let productive_before = self.productive.len();
        self.productive.retain(|peer| peer.id != peer_id);
        let known_before = self.known_peers.len();
        self.known_peers.retain(|peer| peer.id != peer_id);
        productive_before != self.productive.len() || known_before != self.known_peers.len()
    }

    pub(super) fn remove_peer(&mut self, peer_id: PeerId) -> Option<ActivePeer> {
        self.pending.remove(&peer_id);
        self.pending_dials.remove(&peer_id);
        let peer = self.peers.remove(&peer_id);
        if let Some(peer) = peer.as_ref()
            && peer.is_serving
            && !peer_receipts_are_quarantined(peer)
            && peer.remote_record_is_dialable
        {
            self.remember_productive(peer.remote_record);
        }
        self.peer_order.retain(|id| *id != peer_id);
        self.rebalance_request_cursor();
        peer
    }

    pub(super) fn requeue_disconnected_peer(&mut self, peer: &ActivePeer) {
        if !self.network_activated
            || !peer.remote_record_is_dialable
            || peer.remote_record.tcp_port == 0
        {
            return;
        }

        let now = Instant::now();
        if self.recently_saturated(peer.remote_record.id, now) {
            return;
        }

        self.pending_dials.insert(peer.remote_record.id, now);
        self.remember_pending(peer.remote_record);
    }

    pub(super) fn should_requeue_disconnected_peer(
        &self,
        peer: &ActivePeer,
        _reason: Option<DisconnectReason>,
    ) -> bool {
        peer.is_serving
    }

    pub(super) fn remove_unsupported_session(&mut self, peer_id: PeerId) {
        self.network.disconnect_peer(peer_id);
        self.network.remove_peer(peer_id, PeerKind::Basic);
        let known_changed = self.forget_peer(peer_id);
        if known_changed {
            self.persist_productive_peers();
        }
    }

    pub(super) fn prune_stale_nonserving_peers(&mut self) {
        let stale_peers: Vec<_> = self
            .peers
            .iter()
            .filter_map(|(peer_id, peer)| {
                is_stale_nonserving_peer(
                    peer.is_serving,
                    peer.remote_status.latest_block,
                    peer.connected_at.elapsed(),
                )
                .then_some(*peer_id)
            })
            .collect();

        for peer_id in stale_peers {
            let Some(peer) = self.peers.get(&peer_id) else {
                continue;
            };

            self.network
                .reputation_change(peer_id, ReputationChangeKind::Dropped);
            debug!(
                peer = %peer_id,
                remote_addr = %peer.remote_record.tcp_addr(),
                client_version = %peer.client_version,
                connected_for = ?peer.connected_at.elapsed(),
                "disconnecting stale non-serving peer without an advertised tip"
            );
            self.network.disconnect_peer(peer_id);
            self.remove_peer(peer_id);
        }
    }

    pub(super) fn drop_unproductive_known_peer(
        &mut self,
        peer_id: PeerId,
        reason: Option<DisconnectReason>,
    ) {
        let Some(peer) = self.peers.get(&peer_id) else {
            return;
        };
        if peer.is_serving || is_bootstrap_node(peer_id) {
            return;
        }
        if matches!(reason, Some(DisconnectReason::TooManyPeers)) {
            return;
        }
        if !self.productive.iter().any(|node| node.id == peer_id)
            && !self.known_peers.iter().any(|node| node.id == peer_id)
        {
            return;
        }

        let connected_for = peer.connected_at.elapsed();
        let remove_from_cache =
            is_stale_nonserving_peer(false, peer.remote_status.latest_block, connected_for)
                || matches!(
                    reason,
                    Some(
                        DisconnectReason::DisconnectRequested
                            | DisconnectReason::SubprotocolSpecific
                            | DisconnectReason::TcpSubsystemError
                            | DisconnectReason::PingTimeout
                    )
                )
                || reason.is_none();

        if !remove_from_cache {
            return;
        }

        if self.remove_productive_peer(peer_id) {
            self.persist_productive_peers();
        }
    }

    pub(super) fn should_backoff_saturated_peer(
        &self,
        peer_id: PeerId,
        reason: Option<DisconnectReason>,
    ) -> bool {
        let Some(peer) = self.peers.get(&peer_id) else {
            return false;
        };
        is_saturated_remote_rejection(reason, peer.is_serving, peer.connected_at.elapsed())
    }

    pub(super) fn record_session_closed_metrics(
        &mut self,
        peer_id: PeerId,
        reason: Option<DisconnectReason>,
    ) {
        let Some(peer) = self.peers.get(&peer_id) else {
            return;
        };
        self.session_metrics.disconnected_sessions =
            self.session_metrics.disconnected_sessions.saturating_add(1);
        if matches!(reason, Some(DisconnectReason::TooManyPeers)) {
            self.session_metrics.saturated_disconnects =
                self.session_metrics.saturated_disconnects.saturating_add(1);
        }
        if !peer.is_serving {
            self.session_metrics.nonserving_disconnects = self
                .session_metrics
                .nonserving_disconnects
                .saturating_add(1);
        }
    }

    pub(super) fn backoff_saturated_peer(&mut self, peer_id: PeerId) {
        let until = Instant::now() + SATURATED_PEER_RETRY_DELAY;
        self.saturated_peers.insert(peer_id, until);
        self.pending.remove(&peer_id);
        self.pending_dials.remove(&peer_id);
        self.network.remove_peer(peer_id, PeerKind::Basic);
    }

    pub(super) fn recently_saturated(&self, peer_id: PeerId, now: Instant) -> bool {
        self.saturated_peers
            .get(&peer_id)
            .is_some_and(|until| *until > now)
    }

    pub(super) fn prune_saturated_peers(&mut self, now: Instant) {
        self.saturated_peers.retain(|_, until| *until > now);
    }

    pub(super) fn prune_receipt_quarantined_peers(&mut self, now: Instant) {
        self.receipt_quarantined_peers
            .retain(|_, until| *until > now);
    }

    pub(super) fn log_session_closed(&self, peer_id: PeerId, reason: Option<DisconnectReason>) {
        let Some(peer) = self.peers.get(&peer_id) else {
            debug!(peer = %peer_id, ?reason, "peer session closed");
            return;
        };

        let connected_for = peer.connected_at.elapsed();
        let latest_block = peer.remote_status.latest_block.unwrap_or_default();
        let saturated_remote = matches!(reason, Some(DisconnectReason::TooManyPeers));
        let noisy_remote_rejection =
            saturated_remote && !peer.is_serving && connected_for <= EARLY_SESSION_DROP_THRESHOLD;
        let disconnect_note = disconnect_note(
            reason,
            peer.is_serving,
            peer.remote_status.latest_block,
            connected_for,
        );
        let noisy_non_serving_disconnect = reason.is_none() && !peer.is_serving;

        if noisy_non_serving_disconnect {
            self.network
                .reputation_change(peer_id, ReputationChangeKind::Dropped);
        }

        if noisy_remote_rejection {
            debug!(
                peer = %peer_id,
                remote_addr = %peer.remote_record.tcp_addr(),
                client_version = %peer.client_version,
                ?reason,
                ?connected_for,
                serving = peer.is_serving,
                version = ?peer.version,
                latest_block,
                saturated_remote,
                disconnect_note,
                "peer session closed"
            );
        } else if reason.is_some() && !noisy_non_serving_disconnect {
            debug!(
                peer = %peer_id,
                remote_addr = %peer.remote_record.tcp_addr(),
                client_version = %peer.client_version,
                ?reason,
                ?connected_for,
                serving = peer.is_serving,
                version = ?peer.version,
                latest_block,
                saturated_remote,
                disconnect_note,
                "peer session closed"
            );
        } else {
            debug!(
                peer = %peer_id,
                remote_addr = %peer.remote_record.tcp_addr(),
                client_version = %peer.client_version,
                ?reason,
                ?connected_for,
                serving = peer.is_serving,
                version = ?peer.version,
                latest_block,
                saturated_remote,
                disconnect_note,
                "peer session closed"
            );
        }
    }
}

pub(super) async fn abort_and_wait(task: JoinHandle<()>, task_name: &'static str) {
    task.abort();
    match timeout(REQUEST_HANDLER_SHUTDOWN_TIMEOUT, task).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            if !error.is_cancelled() {
                warn!(%error, task = task_name, "task exited unexpectedly during shutdown");
            }
        }
        Err(_) => {
            debug!(
                task = task_name,
                "task abort is still pending after timeout"
            );
        }
    }
}

fn sort_dial_candidates_by_productivity(
    candidates: &mut [NodeRecord],
    productive: &VecDeque<NodeRecord>,
) {
    candidates.sort_by_key(|candidate| productive_peer_priority(productive, candidate.id));
}

fn select_dial_candidates(
    candidates: Vec<NodeRecord>,
    productive: &VecDeque<NodeRecord>,
    dial_budget: usize,
) -> Vec<NodeRecord> {
    if candidates.len() <= dial_budget {
        return candidates;
    }

    let mut preferred = Vec::new();
    let mut fresh = Vec::new();
    for candidate in candidates {
        if productive.iter().any(|peer| peer.id == candidate.id) {
            preferred.push(candidate);
        } else {
            fresh.push(candidate);
        }
    }

    sort_dial_candidates_by_productivity(&mut preferred, productive);
    let preferred_budget = dial_budget.saturating_mul(2).div_ceil(3).max(1);
    let mut selected = Vec::with_capacity(dial_budget);
    let mut preferred = preferred.into_iter();
    selected.extend(preferred.by_ref().take(preferred_budget.min(dial_budget)));
    selected.extend(
        fresh
            .into_iter()
            .take(dial_budget.saturating_sub(selected.len())),
    );
    selected.extend(preferred.take(dial_budget.saturating_sub(selected.len())));
    selected
}

fn productive_peer_priority(productive: &VecDeque<NodeRecord>, peer_id: PeerId) -> usize {
    productive
        .iter()
        .position(|peer| peer.id == peer_id)
        .unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn dial_candidates_prefer_recent_productive_peers() {
        let first = NodeRecord::new_with_ports(
            Ipv4Addr::LOCALHOST.into(),
            30303,
            Some(30303),
            PeerId::repeat_byte(0x01),
        );
        let second = NodeRecord::new_with_ports(
            Ipv4Addr::LOCALHOST.into(),
            30304,
            Some(30304),
            PeerId::repeat_byte(0x02),
        );
        let unranked = NodeRecord::new_with_ports(
            Ipv4Addr::LOCALHOST.into(),
            30305,
            Some(30305),
            PeerId::repeat_byte(0x03),
        );
        let productive = VecDeque::from([second, first]);
        let mut candidates = vec![unranked, first, second];

        sort_dial_candidates_by_productivity(&mut candidates, &productive);

        assert_eq!(
            candidates.iter().map(|node| node.id).collect::<Vec<_>>(),
            vec![second.id, first.id, unranked.id]
        );
    }

    #[test]
    fn dial_candidate_selection_keeps_room_for_fresh_peers() {
        let productive = (0..6)
            .map(|index| {
                NodeRecord::new_with_ports(
                    Ipv4Addr::LOCALHOST.into(),
                    30303 + index,
                    Some(30303 + index),
                    PeerId::repeat_byte(index as u8),
                )
            })
            .collect::<Vec<_>>();
        let fresh = (6..9)
            .map(|index| {
                NodeRecord::new_with_ports(
                    Ipv4Addr::LOCALHOST.into(),
                    30303 + index,
                    Some(30303 + index),
                    PeerId::repeat_byte(index as u8),
                )
            })
            .collect::<Vec<_>>();
        let selected = select_dial_candidates(
            productive
                .iter()
                .copied()
                .chain(fresh.iter().copied())
                .collect(),
            &productive.iter().copied().collect(),
            6,
        );

        assert_eq!(selected.len(), 6);
        assert_eq!(
            selected
                .iter()
                .filter(|candidate| fresh.iter().any(|fresh| fresh.id == candidate.id))
                .count(),
            2
        );
    }

    #[test]
    fn dial_candidate_selection_uses_full_budget_when_all_candidates_are_productive() {
        let productive = (0..9)
            .map(|index| {
                NodeRecord::new_with_ports(
                    Ipv4Addr::LOCALHOST.into(),
                    30303 + index,
                    Some(30303 + index),
                    PeerId::repeat_byte(index as u8),
                )
            })
            .collect::<Vec<_>>();
        let selected = select_dial_candidates(
            productive.to_vec(),
            &productive.iter().copied().collect(),
            6,
        );

        assert_eq!(selected.len(), 6);
    }
}
