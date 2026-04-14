use std::time::Duration;

use futures_util::{FutureExt, StreamExt};
use tokio::time::timeout;
use tracing::{debug, info, warn};

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
        for peer in self.known_peers.clone() {
            if is_bootstrap_node(peer.id)
                || peer.tcp_port == 0
                || self.peers.contains_key(&peer.id)
                || self.pending.contains_key(&peer.id)
            {
                continue;
            }

            self.pending.insert(peer.id, peer);
            self.network.connect_peer_kind(
                peer.id,
                PeerKind::Trusted,
                peer.tcp_addr(),
                Some(peer.udp_addr()),
            );
        }
    }

    pub(super) fn drain_events_now(&mut self) {
        while let Some(event) = self.network_events.next().now_or_never().flatten() {
            self.handle_network_event(event);
        }
        while let Some(event) = self.discovery_events.next().now_or_never().flatten() {
            self.handle_discovery_event(event);
        }
        self.prune_stale_nonserving_peers();
    }

    pub(super) async fn wait_for_activity(&mut self, max_wait: Duration) -> bool {
        let delay = tokio::time::sleep(max_wait);
        tokio::pin!(delay);

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

    pub(super) fn handle_network_event(
        &mut self,
        event: NetworkEvent<PeerRequest<LogexNetworkPrimitives>>,
    ) {
        match event {
            NetworkEvent::Peer(reth_network::events::PeerEvent::SessionClosed {
                peer_id,
                reason,
            }) => {
                self.log_session_closed(peer_id, reason);
                self.remove_peer(peer_id);
            }
            NetworkEvent::Peer(reth_network::events::PeerEvent::PeerRemoved(peer_id)) => {
                self.remove_peer(peer_id);
            }
            NetworkEvent::Peer(_) => {}
            NetworkEvent::ActivePeerSession { info, messages } => {
                self.insert_peer(info, messages);
            }
        }
    }

    pub(super) fn handle_discovery_event(&mut self, event: DiscoveryEvent) {
        match event {
            DiscoveryEvent::NewNode(DiscoveredEvent::EventQueued { peer_id, addr, .. }) => {
                let node = NodeRecord::new_with_ports(
                    addr.tcp().ip(),
                    addr.tcp().port(),
                    addr.udp().map(|socket| socket.port()),
                    peer_id,
                );
                self.remember_pending(node);
            }
            DiscoveryEvent::EnrForkId(node, _) => {
                self.remember_pending(node);
            }
        }
    }

    pub(super) fn insert_peer(
        &mut self,
        info: reth_network::events::SessionInfo,
        messages: PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>,
    ) {
        let mut record = self
            .pending
            .remove(&info.peer_id)
            .unwrap_or_else(|| NodeRecord::new(info.remote_addr, info.peer_id));
        record = record.with_tcp_port(info.remote_addr.port());

        let was_productive = self.productive.iter().any(|peer| peer.id == info.peer_id);
        let peer = ActivePeer {
            sender: messages,
            remote_record: record,
            remote_status: *info.status,
            client_version: info.client_version,
            version: info.version,
            is_serving: false,
            consecutive_timeouts: 0,
            connected_at: Instant::now(),
        };

        self.peers.insert(info.peer_id, peer);
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
            latest_block = info.status.latest_block.unwrap_or_default(),
            version = ?info.version,
            "peer session established"
        );
    }

    pub(super) fn remember_pending(&mut self, node: NodeRecord) {
        if is_bootstrap_node(node.id) || node.tcp_port == 0 || self.peers.contains_key(&node.id) {
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
            self.remove_peer(*peer_id);
        }
    }

    pub(super) fn forget_peer(&mut self, peer_id: PeerId) -> bool {
        self.peers.remove(&peer_id);
        self.pending.remove(&peer_id);
        self.peer_order.retain(|id| *id != peer_id);
        self.rebalance_request_cursor();
        let productive_before = self.productive.len();
        self.productive.retain(|peer| peer.id != peer_id);
        let known_before = self.known_peers.len();
        self.known_peers.retain(|peer| peer.id != peer_id);
        productive_before != self.productive.len() || known_before != self.known_peers.len()
    }

    pub(super) fn remove_peer(&mut self, peer_id: PeerId) {
        self.pending.remove(&peer_id);
        if let Some(peer) = self.peers.remove(&peer_id)
            && peer.is_serving
        {
            self.remember_productive(peer.remote_record);
        }
        self.peer_order.retain(|id| *id != peer_id);
        self.rebalance_request_cursor();
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

            info!(
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
        } else if reason.is_some() || connected_for <= EARLY_SESSION_DROP_THRESHOLD {
            info!(
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
