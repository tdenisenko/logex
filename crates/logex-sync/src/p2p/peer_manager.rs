use std::collections::VecDeque;
use std::time::{Duration, Instant};

use alloy_primitives::{B256, B512};
use eyre::{Result, bail};
use futures::FutureExt;
use futures::stream::FuturesUnordered;
use futures_util::{SinkExt, StreamExt};
use reth_discv4::{DiscoveryUpdate, Discv4};
use reth_eth_wire::{
    EthMessage, EthNetworkPrimitives, GetBlockBodies, GetBlockHeaders, GetReceipts,
    HeadersDirection, message::RequestPair,
};
use reth_eth_wire_types::NetworkPrimitives;
use reth_ethereum_forks::Head;
use reth_network_peers::{NodeRecord, mainnet_nodes};
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
const FILL_BUDGET: Duration = Duration::from_secs(20);
const MAX_PERSISTED_PEERS: usize = 512;

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
    /// Known peers discovered or successfully connected during this run.
    /// We keep these across queue drains so shutdown persistence does not
    /// accidentally forget useful peers after a bad reconnect cycle.
    known: VecDeque<NodeRecord>,
    our_head: Head,
    next_request_id: u64,
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
            known: VecDeque::new(),
            our_head,
            next_request_id: 1,
        };

        for node in known_peers {
            manager.seed_known_node(node);
        }

        // Seed the queue with persisted peers first, then hardcoded mainnet
        // bootnodes so startup can dial immediately instead of waiting on the
        // discovery stream to produce its first records.
        for node in mainnet_nodes() {
            manager.enqueue_candidate(node, false);
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

    /// Highest advertised canonical block across connected peers.
    pub fn highest_peer_block(&self) -> Option<u64> {
        self.peers
            .iter()
            .filter_map(|peer| peer.remote_status.latest_block)
            .max()
    }

    /// Number of queued peer candidates awaiting connection attempts.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Snapshot of known peers suitable for writing to disk on shutdown.
    pub fn known_peers(&self) -> Vec<NodeRecord> {
        self.known
            .iter()
            .copied()
            .take(MAX_PERSISTED_PEERS)
            .collect()
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
        match timeout(FILL_BUDGET / 2, self.discovery.lookup_self()).await {
            Ok(Ok(records)) => {
                for record in records {
                    if self.enqueue_candidate(record, true) {
                        added += 1;
                    }
                }
            }
            Ok(Err(err)) => {
                debug!(error = %err, "discovery lookup failed");
            }
            Err(_) => {
                debug!("discovery lookup timed out");
            }
        }
        added
    }

    fn handle_update(&mut self, update: DiscoveryUpdate, added: &mut usize) {
        match update {
            DiscoveryUpdate::Added(record) | DiscoveryUpdate::DiscoveredAtCapacity(record) => {
                if self.enqueue_candidate(record, true) {
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
                        self.remember_node(conn.remote_record);
                        self.peers.push(conn);
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
        self.remember_node_with_priority(node, false);
        self.enqueue_candidate(node, false);
    }

    fn enqueue_candidate(&mut self, node: NodeRecord, remember: bool) -> bool {
        if remember {
            self.remember_node(node);
        }

        if self.knows_node(node.id) {
            return false;
        }

        self.pending.push_back(node);
        true
    }

    fn remember_node(&mut self, node: NodeRecord) {
        self.remember_node_with_priority(node, true);
    }

    fn remember_node_with_priority(&mut self, node: NodeRecord, recent: bool) {
        if let Some(index) = self.known.iter().position(|known| known.id == node.id) {
            self.known.remove(index);
        }

        if recent {
            self.known.push_front(node);
        } else {
            self.known.push_back(node);
        }

        while self.known.len() > MAX_PERSISTED_PEERS {
            self.known.pop_back();
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

        self.send_request_and_receive(
            EthMessage::GetBlockHeaders(RequestPair {
                request_id,
                message: request,
            }),
            |msg| match msg {
                EthMessage::BlockHeaders(pair) if pair.request_id == request_id => {
                    Some(pair.message.0)
                }
                _ => None,
            },
        )
        .await
    }

    /// Request block bodies for the given block hashes.
    pub async fn get_bodies(
        &mut self,
        hashes: Vec<B256>,
    ) -> Result<Vec<<EthNetworkPrimitives as NetworkPrimitives>::BlockBody>> {
        let request_id = self.next_id();
        let request = GetBlockBodies(hashes);

        self.send_request_and_receive(
            EthMessage::GetBlockBodies(RequestPair {
                request_id,
                message: request,
            }),
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
        let request = GetReceipts(hashes);

        self.send_request_and_receive(
            EthMessage::GetReceipts(RequestPair {
                request_id,
                message: request,
            }),
            |msg| match msg {
                EthMessage::Receipts(pair) if pair.request_id == request_id => Some(pair.message.0),
                _ => None,
            },
        )
        .await
    }

    /// Send a request to the first available peer and wait for the matching response.
    async fn send_request_and_receive<T>(
        &mut self,
        request: EthMessage<EthNetworkPrimitives>,
        extract: impl Fn(EthMessage<EthNetworkPrimitives>) -> Option<T>,
    ) -> Result<T> {
        // Try each peer in order, remove dead ones
        let mut dead_peers = Vec::new();

        for (idx, peer) in self.peers.iter_mut().enumerate() {
            // Send
            if peer.stream.send(request.clone()).await.is_err() {
                dead_peers.push(idx);
                continue;
            }

            // Wait for matching response
            match timeout(REQUEST_TIMEOUT, async {
                while let Some(msg_result) = peer.stream.next().await {
                    match msg_result {
                        Ok(msg) => {
                            if let Some(result) = extract(msg) {
                                return Ok(result);
                            }
                            // Not our response, keep reading
                        }
                        Err(e) => return Err(eyre::eyre!("stream error: {e}")),
                    }
                }
                Err(eyre::eyre!("peer disconnected"))
            })
            .await
            {
                Ok(Ok(result)) => {
                    // Clean up dead peers before returning
                    for &idx in dead_peers.iter().rev() {
                        self.peers.swap_remove(idx);
                    }
                    return Ok(result);
                }
                Ok(Err(e)) => {
                    debug!(peer = %peer.remote_id, error = %e, "peer error during request");
                    dead_peers.push(idx);
                }
                Err(_) => {
                    debug!(peer = %peer.remote_id, "request timed out");
                    dead_peers.push(idx);
                }
            }
        }

        // Clean up dead peers
        for &idx in dead_peers.iter().rev() {
            self.peers.swap_remove(idx);
        }

        bail!("no peers available to handle request")
    }
}
