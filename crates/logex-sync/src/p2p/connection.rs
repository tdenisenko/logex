use std::net::SocketAddr;
use std::time::Duration;

use alloy_primitives::B512;
use eyre::{Result, eyre};
use reth_ecies::stream::ECIESStream;
use reth_eth_wire::{
    EthNetworkPrimitives, EthStream, EthVersion, HelloMessageWithProtocols, P2PStream,
    UnauthedEthStream, UnauthedP2PStream, UnifiedStatus,
};
use reth_ethereum_forks::{ForkId, Head};
use reth_network_peers::{NodeRecord, PeerId};
use secp256k1::{PublicKey, SECP256K1, SecretKey};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tracing::{debug, trace};

use super::mainnet::{self, MAINNET_CHAIN_ID, MAINNET_GENESIS};

/// Maximum time we'll spend on TCP connect + ECIES + P2P + Eth handshake
/// before giving up on a peer. Reth's internal eth handshake has its own
/// 10s timeout (HANDSHAKE_TIMEOUT in p2pstream), so anything below that
/// risks killing connections that would have succeeded.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

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
/// The entire connection attempt is bounded by [`CONNECT_TIMEOUT`] — most
/// discovered nodes are unreachable or speak the wrong protocol, so we fail
/// fast and let the caller try the next one.
pub async fn connect(
    node: &NodeRecord,
    secret_key: SecretKey,
    our_head: Head,
) -> Result<PeerConnection> {
    timeout(CONNECT_TIMEOUT, connect_inner(node, secret_key, our_head))
        .await
        .map_err(|_| eyre!("connect timed out after {:?}", CONNECT_TIMEOUT))?
}

async fn connect_inner(
    node: &NodeRecord,
    secret_key: SecretKey,
    our_head: Head,
) -> Result<PeerConnection> {
    let addr: SocketAddr = (node.address, node.tcp_port).into();
    let remote_id = node.id;

    // Our own peer id (uncompressed pubkey minus the 0x04 prefix), used in
    // the P2P Hello so the remote can verify the ECIES key matches.
    let our_pubkey = PublicKey::from_secret_key(SECP256K1, &secret_key);
    let our_peer_id = PeerId::from_slice(&our_pubkey.serialize_uncompressed()[1..]);

    trace!(%addr, %remote_id, "connecting to peer");

    // 1. TCP
    let tcp = TcpStream::connect(addr).await?;

    // 2. ECIES encryption
    let ecies = ECIESStream::connect(tcp, secret_key, remote_id).await?;

    // 3. P2P handshake — we advertise OUR identity here, not the remote's.
    //
    // The default builder advertises ALL_VERSIONS [Eth69, Eth68, Eth67, Eth66]
    // so we get the widest possible peer pool: most mainnet peers haven't
    // upgraded to eth/69 yet (Reth shipped it in late 2024 and adoption is
    // still partial as of early 2026), so restricting to eth/69-only would
    // strand the majority of nodes behind a "no capabilities shared" disconnect.
    let hello = HelloMessageWithProtocols::builder(our_peer_id).build();
    let unauthed_p2p = UnauthedP2PStream::new(ecies);
    let (p2p_stream, _their_hello) = unauthed_p2p.handshake(hello).await?;

    // 4. Eth handshake — read the *negotiated* eth version and build Status
    // in the matching wire format.
    //
    // Reth's eth handshake decoder (handshake.rs:121) uses *our* declared
    // `status.version` to parse the peer's reply, so we must match what the
    // P2P layer actually negotiated. eth/68 and eth/69 use incompatible
    // wire layouts:
    //   eth/68: [version, chain, td, blockhash, genesis, forkid]
    //   eth/69: [version, chain, genesis, forkid, blockhash, earliest, latest]
    // Hardcoding either one drops half the peers — eth/68 peers fail to
    // decode our eth/69 reply ("RLP error: unexpected length") and vice versa.
    let negotiated_version = p2p_stream
        .shared_capabilities()
        .eth_version()
        .map_err(|e| eyre!("eth capability not negotiated: {e}"))?;

    let handshake_head = mainnet::handshake_head(our_head);
    let fork_filter = mainnet::mainnet_fork_filter(handshake_head);
    let fork_id = fork_filter.current();

    // eth/69 dropped total_difficulty and added earliest/latest block fields.
    // For older versions we still need a TD; U256::ZERO is a valid placeholder
    // for a fresh node since reth only checks `bit_len() <= 160`.
    let (total_difficulty, earliest_block, latest_block) =
        if negotiated_version >= EthVersion::Eth69 {
            (None, Some(0), Some(our_head.number))
        } else {
            (Some(handshake_head.total_difficulty), None, None)
        };

    let status = UnifiedStatus {
        version: negotiated_version,
        chain: alloy_chains::Chain::from_id(MAINNET_CHAIN_ID),
        genesis: MAINNET_GENESIS,
        forkid: ForkId {
            hash: fork_id.hash,
            next: fork_id.next,
        },
        blockhash: if our_head.hash.is_zero() {
            MAINNET_GENESIS
        } else {
            our_head.hash
        },
        total_difficulty,
        earliest_block,
        latest_block,
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
