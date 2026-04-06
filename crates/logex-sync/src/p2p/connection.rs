use std::net::SocketAddr;

use alloy_primitives::{B512, U256};
use eyre::Result;
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::{
    EthNetworkPrimitives, EthStream, EthVersion, HelloMessageWithProtocols, P2PStream,
    UnauthedEthStream, UnauthedP2PStream, UnifiedStatus,
};
use reth_ethereum_forks::{ForkId, Head};
use reth_network_peers::NodeRecord;
use secp256k1::SecretKey;
use tokio::net::TcpStream;
use tracing::{debug, trace};

use super::mainnet::{self, MAINNET_CHAIN_ID, MAINNET_GENESIS};

/// The inner P2P stream type after all handshakes complete.
pub type PeerStream = EthStream<P2PStream<ECIESStream<TcpStream>>, EthNetworkPrimitives>;

/// An established connection to an Ethereum peer.
pub struct PeerConnection {
    pub stream: PeerStream,
    pub remote_id: B512,
    pub remote_status: UnifiedStatus,
}

/// Connect to an Ethereum peer and complete all handshakes:
/// TCP → ECIES → P2P (Hello) → Eth (Status).
///
/// Returns a fully authenticated `PeerConnection` ready for protocol messages.
pub async fn connect(
    node: &NodeRecord,
    secret_key: SecretKey,
    our_head: Head,
) -> Result<PeerConnection> {
    let addr: SocketAddr = (node.address, node.tcp_port).into();
    let remote_id = node.id;

    trace!(%addr, %remote_id, "connecting to peer");

    // 1. TCP
    let tcp = TcpStream::connect(addr).await?;

    // 2. ECIES encryption
    let ecies = ECIESStream::connect(tcp, secret_key, remote_id).await?;

    // 3. P2P handshake
    let hello = HelloMessageWithProtocols::builder(remote_id).build();
    let unauthed_p2p = UnauthedP2PStream::new(ecies);
    let (p2p_stream, _their_hello) = unauthed_p2p.handshake(hello).await?;

    // 4. Eth handshake
    let fork_filter = mainnet::mainnet_fork_filter(our_head);
    let fork_id = fork_filter.current();

    let status = UnifiedStatus {
        version: EthVersion::Eth68,
        chain: alloy_chains::Chain::from_id(MAINNET_CHAIN_ID),
        genesis: MAINNET_GENESIS,
        forkid: ForkId {
            hash: fork_id.hash,
            next: fork_id.next,
        },
        blockhash: our_head.hash,
        total_difficulty: Some(U256::ZERO),
        earliest_block: None,
        latest_block: Some(our_head.number),
    };

    let unauthed_eth = UnauthedEthStream::new(p2p_stream);
    let (eth_stream, their_status) = unauthed_eth.handshake(status, fork_filter).await?;

    debug!(
        %remote_id,
        chain = %their_status.chain,
        "peer connected"
    );

    Ok(PeerConnection {
        stream: eth_stream,
        remote_id,
        remote_status: their_status,
    })
}
