use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use eyre::Result;
use reth_discv4::{DiscoveryUpdate, Discv4, Discv4Config};
use reth_network_peers::{NodeRecord, PeerId, mainnet_nodes};
use secp256k1::{PublicKey, SECP256K1, SecretKey};
use tokio_stream::wrappers::ReceiverStream;

const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);

/// Handle to a running discv4 service plus the stream of newly discovered peers.
///
/// We expose the update stream alongside the handle because polling
/// `Discv4::lookup_random` blocks for ~40s when the routing table is empty
/// (one full request_timeout cycle), whereas the stream emits records as
/// soon as the bootstrap pings complete.
pub struct Discovery {
    pub handle: Discv4,
    pub updates: ReceiverStream<DiscoveryUpdate>,
}

/// Start discv4 peer discovery on the given port.
///
/// Spawns the service in the background and returns a handle plus an
/// update stream that emits `DiscoveryUpdate::Added` events as the routing
/// table grows. Seeded with Ethereum mainnet bootnodes.
pub async fn start_discovery(secret_key: SecretKey, discovery_port: u16) -> Result<Discovery> {
    let public_key = PublicKey::from_secret_key(SECP256K1, &secret_key);
    let peer_id = PeerId::from_slice(&public_key.serialize_uncompressed()[1..]);

    let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), discovery_port);
    let local_enr = NodeRecord::new(local_addr, peer_id);

    // Reth/geth-style cold starts keep discovery busy enough that the routing
    // table turns over quickly in the first minute instead of waiting on long
    // default timeouts between walks. We still keep discovery-only bootnodes
    // out of the dial queue; this only makes the DHT bootstrap more eager.
    let mut config_builder = Discv4Config::builder();
    let config = config_builder
        .add_boot_nodes(mainnet_nodes())
        .lookup_interval(DISCOVERY_LOOKUP_INTERVAL)
        .ping_interval(DISCOVERY_PING_INTERVAL)
        .build();

    let (handle, mut service) = Discv4::bind(local_addr, local_enr, secret_key, config).await?;

    // Subscribe BEFORE spawning so we don't miss the first batch of Added events.
    let updates = service.update_stream();
    service.spawn();

    tracing::info!(%local_addr, "discv4 discovery started");

    Ok(Discovery { handle, updates })
}
