use std::collections::{HashSet, VecDeque};
use std::sync::LazyLock;
use std::time::{Duration, Instant};

use alloy_primitives::{B256, B512};
use eyre::{Result, bail};
use futures::FutureExt;
use futures::stream::FuturesUnordered;
use futures_util::{SinkExt, StreamExt};
use reth_discv4::{DiscoveryUpdate, Discv4};
use reth_eth_wire::{
    EthMessage, EthNetworkPrimitives, EthVersion, GetBlockBodies, GetBlockHeaders, GetReceipts,
    GetReceipts70, HeadersDirection, message::RequestPair,
};
use reth_eth_wire_types::NetworkPrimitives;
use reth_ethereum_forks::Head;
use reth_network_peers::{NodeRecord, PeerId, mainnet_nodes};
use secp256k1::SecretKey;
use tokio::time::timeout;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, info, warn};

use super::connection::{self, PeerConnection};
use super::discovery::Discovery;

/// Timeout for individual request/response roundtrips.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long to wait for new discovery events when we have nothing to try.
/// Short enough that startup feels responsive, long enough to let the DHT
/// emit a meaningful batch of records.
const DISCOVERY_WAIT: Duration = Duration::from_secs(2);

/// Wall-clock cap on a single `fill_peers` call. Without this, fill_peers
/// would loop until `target` is reached or candidates dry up — but on a
/// fresh start, candidates flow in faster than peers accept us, so the call
/// blocks the sync engine indefinitely. With a budget, fill_peers returns
/// whatever progress it made and lets the engine decide whether to start
/// fetching headers (any peers > 0) or wait and retry.
const FILL_BUDGET: Duration = Duration::from_secs(12);
const ACTIVE_LOOKUP_TIMEOUT: Duration = Duration::from_secs(10);
const ACTIVE_RANDOM_LOOKUP_FANOUT: usize = 1;
const MAX_PERSISTED_PEERS: usize = 512;
const MAX_CONSECUTIVE_TIMEOUTS: u32 = 2;
static MAINNET_BOOTNODE_IDS: LazyLock<HashSet<B512>> =
    LazyLock::new(|| mainnet_nodes().into_iter().map(|node| node.id).collect());

/// Manages a pool of peer connections and routes requests.
pub struct PeerManager {
    secret_key: SecretKey,
    peers: Vec<PeerConnection>,
    discovery: Discv4,
    discovery_updates: ReceiverStream<DiscoveryUpdate>,
    /// Buffered candidates we haven't tried to connect to yet. The discovery
    /// stream emits records continuously, and we want to drain everything
    /// available before sleeping.
    pending: VecDeque<NodeRecord>,
    /// Peers that have already proved useful for sync and are worth
    /// persisting across restarts.
    productive: VecDeque<NodeRecord>,
    our_head: Head,
    next_request_id: u64,
    last_responder: Option<B512>,
}

impl PeerManager {
    /// Create a new peer manager with discovery running.
    pub fn new(
        secret_key: SecretKey,
        discovery: Discovery,
        our_head: Head,
        known_peers: Vec<NodeRecord>,
    ) -> Self {
        let mut manager = Self {
            secret_key,
            peers: Vec::new(),
            discovery: discovery.handle,
            discovery_updates: discovery.updates,
            pending: VecDeque::new(),
            productive: VecDeque::new(),
            our_head,
            next_request_id: 1,
            last_responder: None,
        };

        for node in known_peers {
            manager.seed_known_node(node);
        }

        manager
    }

    /// Update our advertised head (for new peer handshakes).
    pub fn set_head(&mut self, head: Head) {
        self.our_head = head;
    }

    /// Number of currently connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Number of connected peers that have successfully answered at least one
    /// block/receipt request and are therefore actually serving sync data.
    pub fn serving_peer_count(&self) -> usize {
        self.peers.iter().filter(|peer| peer.is_serving).count()
    }

    /// Highest advertised canonical block across connected peers.
    pub fn highest_peer_block(&self) -> Option<u64> {
        self.peers
            .iter()
            .filter_map(|peer| peer.remote_status.latest_block)
            .filter(|block| *block > 0)
            .max()
    }

    /// Number of queued peer candidates awaiting connection attempts.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Snapshot of known peers suitable for writing to disk on shutdown.
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

    /// Drain whatever discovery events are immediately available into the
    /// pending queue. Non-blocking — returns the number of new candidates added.
    fn drain_discovery(&mut self) -> usize {
        let mut added = 0;
        while let Some(update) = self.discovery_updates.next().now_or_never().flatten() {
            self.handle_update(update, &mut added);
        }
        added
    }

    /// Wait up to `DISCOVERY_WAIT` for at least one new discovery event,
    /// then drain anything else that's immediately ready.
    async fn wait_for_discovery(&mut self) -> usize {
        let mut added = 0;
        match timeout(DISCOVERY_WAIT, self.discovery_updates.next()).await {
            Ok(Some(update)) => self.handle_update(update, &mut added),
            Ok(None) => {
                // Stream closed - discovery service died.
                warn!("discovery update stream closed");
            }
            Err(_) => {} // timeout, no events yet
        }
        // Pick up anything else that arrived during the same poll window.
        while let Some(update) = self.discovery_updates.next().now_or_never().flatten() {
            self.handle_update(update, &mut added);
        }
        added
    }

    /// Actively query the DHT for nodes when passive discovery has not yet
    /// produced any candidates.
    async fn lookup_candidates(&mut self) -> usize {
        let mut added = 0;
        let mut tasks = FuturesUnordered::new();

        tasks.push(run_active_lookup(self.discovery.clone(), "self", None));

        for _ in 0..ACTIVE_RANDOM_LOOKUP_FANOUT {
            tasks.push(run_active_lookup(
                self.discovery.clone(),
                "random",
                Some(PeerId::random()),
            ));
        }

        while let Some((lookup_kind, result)) = tasks.next().await {
            match result {
                Ok(Ok(records)) => {
                    let mut lookup_added = 0;
                    for record in records {
                        if self.enqueue_candidate(record) {
                            added += 1;
                            lookup_added += 1;
                        }
                    }
                    if lookup_added > 0 {
                        debug!(
                            lookup = lookup_kind,
                            added = lookup_added,
                            "active discovery lookup produced dial candidates"
                        );
                    }
                }
                Ok(Err(err)) => {
                    debug!(lookup = lookup_kind, error = %err, "discovery lookup failed");
                }
                Err(_) => {
                    debug!(lookup = lookup_kind, timeout = ?ACTIVE_LOOKUP_TIMEOUT, "discovery lookup timed out");
                }
            }
        }
        added
    }

    fn handle_update(&mut self, update: DiscoveryUpdate, added: &mut usize) {
        match update {
            DiscoveryUpdate::Added(record) | DiscoveryUpdate::DiscoveredAtCapacity(record) => {
                if self.enqueue_candidate(record) {
                    *added += 1;
                }
            }
            DiscoveryUpdate::Batch(updates) => {
                for u in updates {
                    self.handle_update(u, added);
                }
            }
            DiscoveryUpdate::EnrForkId(_, _) | DiscoveryUpdate::Removed(_) => {}
        }
    }

    /// Connect to discovered peers, returning as soon as `min` peers are
    /// connected (or `target` if `min` is already met). Bounded by
    /// [`FILL_BUDGET`].
    ///
    /// Pulls candidates from the discv4 update stream (populated by the
    /// background DHT walk) and dials them in parallel because most are
    /// firewalled, saturated, or speak a different protocol — serial dialing
    /// would burn the startup budget on dead nodes.
    ///
    /// **Why two thresholds?** Mainnet peers drop connections that go idle
    /// during the eth handshake / first request, so the wall-clock window
    /// between *connecting* and *issuing the first GetBlockHeaders* must be
    /// short. Returning eagerly at `min` lets the engine start fetching
    /// almost immediately; subsequent calls top up toward `target` while
    /// requests are already in flight. With a single `target` threshold the
    /// fill loop kept dialing for the full 20s budget, leaving the first
    /// few peers idle long enough to be disconnected.
    ///
    /// Returns early when any of: `min` reached (after the current dial
    /// batch completes), `target` reached, no new candidates within the
    /// discovery wait window, or [`FILL_BUDGET`] elapsed. The wall-clock
    /// budget is what makes this safe to call from the sync engine's main
    /// loop — without it, a steady candidate inflow combined with a low
    /// connection success rate keeps fill_peers spinning forever, blocking
    /// header fetches.
    pub async fn fill_peers(&mut self, min: usize, target: usize) {
        // Already meet the caller's minimum — return immediately so they
        // can use the peers they already have. This prevents the engine
        // from blocking inside fill_peers (with peers sitting idle) while
        // dialing toward `target`.
        if self.peers.len() >= min {
            return;
        }
        if self.peers.len() >= target {
            return;
        }

        // Kick the DHT to keep the routing table growing. send_lookup_self
        // is fire-and-forget, unlike lookup_random which would block ~40s
        // if the table is sparse.
        self.discovery.send_lookup_self();
        for _ in 0..ACTIVE_RANDOM_LOOKUP_FANOUT {
            self.discovery.send_lookup(PeerId::random());
        }

        let deadline = Instant::now() + FILL_BUDGET;

        loop {
            if self.peers.len() >= target {
                return;
            }
            if Instant::now() >= deadline {
                debug!(
                    current_peers = self.peers.len(),
                    target, "fill_peers budget exhausted"
                );
                return;
            }

            self.drain_discovery();

            if self.pending.is_empty() {
                let added = self.lookup_candidates().await;
                if added > 0 {
                    info!(added, "seeded dial queue from active discovery lookup");
                }
            }

            if self.pending.is_empty() {
                let added = self.wait_for_discovery().await;
                if added == 0 {
                    // No new candidates within the wait window — let the
                    // caller decide whether to retry or move on.
                    return;
                }
            }

            // Take a batch and dial them concurrently. Bigger batches improve
            // throughput at the cost of momentary connection-storm noise.
            let batch_size = (target - self.peers.len()).max(8);
            let batch: Vec<NodeRecord> = self
                .pending
                .drain(..self.pending.len().min(batch_size))
                .collect();

            debug!(
                batch = batch.len(),
                pending = self.pending.len(),
                current_peers = self.peers.len(),
                min,
                target,
                "dialing peer batch"
            );

            if batch.is_empty() {
                return;
            }

            let secret_key = self.secret_key;
            let our_head = self.our_head;
            let mut failed = 0usize;
            let mut tasks: FuturesUnordered<_> = batch
                .into_iter()
                .map(|node| async move {
                    let id = node.id;
                    let result = connection::connect(&node, secret_key, our_head).await;
                    (id, result)
                })
                .collect();

            while let Some((id, result)) = tasks.next().await {
                match result {
                    Ok(conn) => {
                        debug!(peer = %conn.remote_id, "new peer connected");
                        self.peers.push(conn);
                        if self.peers.len() >= min {
                            debug!(
                                current_peers = self.peers.len(),
                                min,
                                target,
                                "fill_peers reached minimum mid-batch, returning immediately"
                            );
                            return;
                        }
                        if self.peers.len() >= target {
                            return;
                        }
                    }
                    Err(e) => {
                        failed += 1;
                        debug!(peer = %id, error = %e, "failed to connect to peer");
                    }
                }
                // Re-check the deadline between completions so a slow batch
                // doesn't blow past the budget by 15s.
                if Instant::now() >= deadline {
                    debug!(
                        current_peers = self.peers.len(),
                        target, "fill_peers budget exhausted mid-batch"
                    );
                    return;
                }
            }

            if failed > 0 && self.peers.is_empty() {
                info!(
                    failed,
                    pending = self.pending.len(),
                    "peer dial batch finished without a connection"
                );
            }

            // Return as soon as we have `min` peers — letting the caller
            // start using them before they get bored and disconnect.
            // Without this, mainnet peers reliably dropped us after ~20s
            // of being connected but unused.
            if self.peers.len() >= min {
                debug!(
                    current_peers = self.peers.len(),
                    min, target, "fill_peers reached minimum, returning"
                );
                return;
            }
        }
    }

    fn is_connected(&self, id: B512) -> bool {
        self.peers.iter().any(|p| p.remote_id == id)
    }

    fn knows_node(&self, id: B512) -> bool {
        self.is_connected(id) || self.pending.iter().any(|node| node.id == id)
    }

    fn seed_known_node(&mut self, node: NodeRecord) {
        if is_bootstrap_node(node.id) {
            return;
        }
        self.discovery.add_node(node);
        self.remember_productive_with_priority(node, false);
        self.enqueue_candidate(node);
    }

    fn enqueue_candidate(&mut self, node: NodeRecord) -> bool {
        if is_bootstrap_node(node.id) {
            return false;
        }
        if self.knows_node(node.id) {
            return false;
        }

        self.pending.push_back(node);
        true
    }

    fn remember_productive(&mut self, node: NodeRecord) {
        self.remember_productive_with_priority(node, true);
    }

    fn remember_productive_with_priority(&mut self, node: NodeRecord, recent: bool) {
        if let Some(index) = self
            .productive
            .iter()
            .position(|productive| productive.id == node.id)
        {
            self.productive.remove(index);
        }

        if recent {
            self.productive.push_front(node);
        } else {
            self.productive.push_back(node);
        }

        while self.productive.len() > MAX_PERSISTED_PEERS {
            self.productive.pop_back();
        }
    }

    fn next_id(&mut self) -> u64 {
        let id = self.next_request_id;
        self.next_request_id += 1;
        id
    }

    /// Request block headers starting at `start_block` for `count` blocks.
    pub async fn get_headers(
        &mut self,
        start_block: u64,
        count: u64,
    ) -> Result<Vec<<EthNetworkPrimitives as NetworkPrimitives>::BlockHeader>> {
        let request_id = self.next_id();
        let request = GetBlockHeaders {
            start_block: start_block.into(),
            limit: count,
            skip: 0,
            direction: HeadersDirection::Rising,
        };

        let headers = self
            .send_request_and_receive(
                |_| {
                    EthMessage::GetBlockHeaders(RequestPair {
                        request_id,
                        message: request,
                    })
                },
                |msg| match msg {
                    EthMessage::BlockHeaders(pair) if pair.request_id == request_id => {
                        Some(pair.message.0)
                    }
                    _ => None,
                },
            )
            .await?;

        if !headers.is_empty() {
            self.mark_last_responder_serving();
        }

        Ok(headers)
    }

    /// Request block bodies for the given block hashes.
    pub async fn get_bodies(
        &mut self,
        hashes: Vec<B256>,
    ) -> Result<Vec<<EthNetworkPrimitives as NetworkPrimitives>::BlockBody>> {
        let request_id = self.next_id();
        let request = GetBlockBodies(hashes);

        self.send_request_and_receive(
            |_| {
                EthMessage::GetBlockBodies(RequestPair {
                    request_id,
                    message: request.clone(),
                })
            },
            |msg| match msg {
                EthMessage::BlockBodies(pair) if pair.request_id == request_id => {
                    Some(pair.message.0)
                }
                _ => None,
            },
        )
        .await
    }

    /// Request receipts for the given block hashes.
    /// Returns `Vec<Vec<ReceiptWithBloom<Receipt>>>` — one inner vec per requested block.
    pub async fn get_receipts(
        &mut self,
        hashes: Vec<B256>,
    ) -> Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <EthNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    > {
        let request_id = self.next_id();
        let request = GetReceipts(hashes.clone());

        self.send_request_and_receive(
            move |version| {
                if version >= EthVersion::Eth70 {
                    EthMessage::GetReceipts70(RequestPair {
                        request_id,
                        message: GetReceipts70 {
                            first_block_receipt_index: 0,
                            block_hashes: hashes.clone(),
                        },
                    })
                } else {
                    EthMessage::GetReceipts(RequestPair {
                        request_id,
                        message: request.clone(),
                    })
                }
            },
            |msg| match msg {
                EthMessage::Receipts(pair) if pair.request_id == request_id => Some(pair.message.0),
                EthMessage::Receipts69(pair) if pair.request_id == request_id => {
                    Some(pair.message.into_with_bloom().0)
                }
                EthMessage::Receipts70(pair) if pair.request_id == request_id => {
                    Some(pair.message.into_with_bloom().0)
                }
                _ => None,
            },
        )
        .await
    }

    /// Send a request to the first available peer and wait for the matching response.
    async fn send_request_and_receive<T>(
        &mut self,
        request_for_version: impl Fn(EthVersion) -> EthMessage<EthNetworkPrimitives>,
        extract: impl Fn(EthMessage<EthNetworkPrimitives>) -> Option<T>,
    ) -> Result<T> {
        let mut dead_peers = HashSet::new();
        self.last_responder = None;

        let peer_order: Vec<B512> = self.peers.iter().map(|peer| peer.remote_id).collect();

        for remote_id in peer_order {
            let Some(idx) = self
                .peers
                .iter()
                .position(|peer| peer.remote_id == remote_id)
            else {
                continue;
            };

            let outcome = {
                let peer = &mut self.peers[idx];
                let request = request_for_version(peer.remote_status.version);

                if peer.stream.send(request).await.is_err() {
                    Err(RequestAttempt::Disconnected)
                } else {
                    match timeout(REQUEST_TIMEOUT, async {
                        while let Some(msg_result) = peer.stream.next().await {
                            match msg_result {
                                Ok(msg) => {
                                    if let Some(result) = extract(msg) {
                                        return Ok(result);
                                    }
                                }
                                Err(e) => return Err(eyre::eyre!("stream error: {e}")),
                            }
                        }
                        Err(eyre::eyre!("peer disconnected"))
                    })
                    .await
                    {
                        Ok(Ok(result)) => Ok(result),
                        Ok(Err(e)) => Err(RequestAttempt::StreamError(e)),
                        Err(_) => Err(RequestAttempt::TimedOut),
                    }
                }
            };

            match outcome {
                Ok(result) => {
                    self.last_responder = Some(remote_id);
                    self.reset_peer_timeout(remote_id);
                    self.promote_peer(remote_id);
                    self.remove_dead_peers(&dead_peers);
                    return Ok(result);
                }
                Err(RequestAttempt::TimedOut) => {
                    debug!(peer = %remote_id, "request timed out");
                    if self.record_timeout(remote_id) >= MAX_CONSECUTIVE_TIMEOUTS {
                        dead_peers.insert(remote_id);
                    } else {
                        self.demote_peer(remote_id);
                    }
                }
                Err(RequestAttempt::StreamError(error)) => {
                    debug!(peer = %remote_id, error = %error, "peer error during request");
                    dead_peers.insert(remote_id);
                }
                Err(RequestAttempt::Disconnected) => {
                    dead_peers.insert(remote_id);
                }
            }
        }

        self.remove_dead_peers(&dead_peers);

        bail!("no peers available to handle request")
    }

    fn mark_last_responder_serving(&mut self) {
        let Some(remote_id) = self.last_responder.take() else {
            return;
        };

        let productive_peer = if let Some(peer) = self
            .peers
            .iter_mut()
            .find(|peer| peer.remote_id == remote_id)
        {
            peer.is_serving = true;
            Some(peer.remote_record)
        } else {
            None
        };

        if let Some(peer) = productive_peer {
            self.remember_productive(peer);
        }
    }

    fn reset_peer_timeout(&mut self, remote_id: B512) {
        if let Some(peer) = self
            .peers
            .iter_mut()
            .find(|peer| peer.remote_id == remote_id)
        {
            peer.consecutive_timeouts = 0;
        }
    }

    fn record_timeout(&mut self, remote_id: B512) -> u32 {
        if let Some(peer) = self
            .peers
            .iter_mut()
            .find(|peer| peer.remote_id == remote_id)
        {
            peer.consecutive_timeouts += 1;
            return peer.consecutive_timeouts;
        }
        MAX_CONSECUTIVE_TIMEOUTS
    }

    fn promote_peer(&mut self, remote_id: B512) {
        let Some(index) = self
            .peers
            .iter()
            .position(|peer| peer.remote_id == remote_id)
        else {
            return;
        };
        if index == 0 {
            return;
        }
        let peer = self.peers.remove(index);
        self.peers.insert(0, peer);
    }

    fn demote_peer(&mut self, remote_id: B512) {
        let Some(index) = self
            .peers
            .iter()
            .position(|peer| peer.remote_id == remote_id)
        else {
            return;
        };
        if index + 1 == self.peers.len() {
            return;
        }
        let peer = self.peers.remove(index);
        self.peers.push(peer);
    }

    fn remove_dead_peers(&mut self, dead_peers: &HashSet<B512>) {
        if dead_peers.is_empty() {
            return;
        }
        self.peers
            .retain(|peer| !dead_peers.contains(&peer.remote_id));
    }
}

enum RequestAttempt {
    TimedOut,
    Disconnected,
    StreamError(eyre::Error),
}

fn push_unique_peer(peers: &mut Vec<NodeRecord>, node: NodeRecord) {
    if peers.iter().any(|peer| peer.id == node.id) {
        return;
    }
    peers.push(node);
}

fn is_bootstrap_node(id: B512) -> bool {
    MAINNET_BOOTNODE_IDS.contains(&id)
}

async fn run_active_lookup(
    discovery: Discv4,
    lookup_kind: &'static str,
    target: Option<PeerId>,
) -> (
    &'static str,
    std::result::Result<
        std::result::Result<Vec<NodeRecord>, reth_discv4::error::Discv4Error>,
        tokio::time::error::Elapsed,
    >,
) {
    let result = match target {
        Some(target) => timeout(ACTIVE_LOOKUP_TIMEOUT, discovery.lookup(target)).await,
        None => timeout(ACTIVE_LOOKUP_TIMEOUT, discovery.lookup_self()).await,
    };
    (lookup_kind, result)
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
            B512::repeat_byte(0x42),
        );
        assert!(!is_bootstrap_node(non_bootstrap.id));
    }
}
