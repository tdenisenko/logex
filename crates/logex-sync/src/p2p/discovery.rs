use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use eyre::Result;
use reth_discv4::{Discv4, Discv4Config};
use reth_network_peers::{NodeRecord, PeerId, mainnet_nodes};
use secp256k1::{PublicKey, SECP256K1, SecretKey};

/// Start discv4 peer discovery on the given port.
///
/// Spawns the discovery service in the background and returns a handle
/// for performing peer lookups. Seeds with Ethereum mainnet bootnodes.
pub async fn start_discovery(secret_key: SecretKey, discovery_port: u16) -> Result<Discv4> {
    let public_key = PublicKey::from_secret_key(SECP256K1, &secret_key);
    let peer_id = PeerId::from_slice(&public_key.serialize_uncompressed()[1..]);

    let local_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), discovery_port);
    let local_enr = NodeRecord::new(local_addr, peer_id);

    let config = Discv4Config {
        bootstrap_nodes: mainnet_nodes().into_iter().collect(),
        ..Default::default()
    };

    let discv4 = Discv4::spawn(local_addr, local_enr, secret_key, config).await?;
    tracing::info!(%local_addr, "discv4 discovery started");

    Ok(discv4)
}
