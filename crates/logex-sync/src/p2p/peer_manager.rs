use std::time::Duration;

use alloy_primitives::{B256, B512};
use eyre::{Result, bail};
use futures_util::{SinkExt, StreamExt};
use reth_discv4::Discv4;
use reth_eth_wire::{
    EthMessage, EthNetworkPrimitives, GetBlockBodies, GetBlockHeaders, GetReceipts,
    HeadersDirection, message::RequestPair,
};
use reth_eth_wire_types::NetworkPrimitives;
use reth_ethereum_forks::Head;
use secp256k1::SecretKey;
use tokio::time::timeout;
use tracing::{debug, warn};

use super::connection::{self, PeerConnection};

/// Timeout for individual request/response roundtrips.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Manages a pool of peer connections and routes requests.
pub struct PeerManager {
    secret_key: SecretKey,
    peers: Vec<PeerConnection>,
    discovery: Discv4,
    our_head: Head,
    next_request_id: u64,
}

impl PeerManager {
    /// Create a new peer manager with discovery running.
    pub fn new(secret_key: SecretKey, discovery: Discv4, our_head: Head) -> Self {
        Self {
            secret_key,
            peers: Vec::new(),
            discovery,
            our_head,
            next_request_id: 1,
        }
    }

    /// Update our advertised head (for new peer handshakes).
    pub fn set_head(&mut self, head: Head) {
        self.our_head = head;
    }

    /// Number of currently connected peers.
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Connect to discovered peers until we reach `target` connections.
    pub async fn fill_peers(&mut self, target: usize) {
        if self.peers.len() >= target {
            return;
        }
        let needed = target - self.peers.len();

        // Discover new peers
        let nodes = match self.discovery.lookup_random().await {
            Ok(n) => n,
            Err(e) => {
                warn!(error = %e, "peer discovery lookup failed");
                return;
            }
        };

        let mut connected = 0usize;
        for node in nodes.into_iter().take(needed * 2) {
            if self.is_connected(node.id) {
                continue;
            }
            match connection::connect(&node, self.secret_key, self.our_head).await {
                Ok(conn) => {
                    debug!(peer = %conn.remote_id, "new peer connected");
                    self.peers.push(conn);
                    connected += 1;
                    if connected >= needed {
                        break;
                    }
                }
                Err(e) => {
                    debug!(peer = %node.id, error = %e, "failed to connect to peer");
                }
            }
        }
    }

    fn is_connected(&self, id: B512) -> bool {
        self.peers.iter().any(|p| p.remote_id == id)
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
