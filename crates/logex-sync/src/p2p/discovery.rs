use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use alloy_chains::NamedChain;
use eyre::Result;
use reth_discv4::{DiscoveryUpdate, Discv4, Discv4Config};
use reth_dns_discovery::{DnsDiscoveryConfig, DnsDiscoveryService, DnsResolver};
use reth_network_peers::{NodeRecord, PeerId, mainnet_nodes};
use secp256k1::{PublicKey, SECP256K1, SecretKey};
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, warn};

const DISCOVERY_LOOKUP_INTERVAL: Duration = Duration::from_secs(3);
const DISCOVERY_PING_INTERVAL: Duration = Duration::from_secs(5);
const DISCOVERY_CANDIDATE_CHANNEL_CAPACITY: usize = 1024;
const DNS_EAGER_BOOTSTRAP_BUDGET: Duration = Duration::from_secs(2);
const DNS_EAGER_BOOTSTRAP_LIMIT: usize = 64;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum DiscoveryCandidateSource {
    Discv4,
    Dns,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct DiscoveryCandidate {
    pub node: NodeRecord,
    pub source: DiscoveryCandidateSource,
}

/// Handle to a running discv4 service plus the stream of newly discovered peers.
///
/// We expose the update stream alongside the handle because polling
/// `Discv4::lookup_random` blocks for ~40s when the routing table is empty
/// (one full request_timeout cycle), whereas the stream emits records as
/// soon as the bootstrap pings complete. DNS discovery candidates are folded
/// into the same stream so the peer manager can prioritize them without
/// having to juggle multiple discovery protocols.
pub struct Discovery {
    pub handle: Discv4,
    pub candidates: ReceiverStream<DiscoveryCandidate>,
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

    let (candidate_tx, candidate_rx) = mpsc::channel(DISCOVERY_CANDIDATE_CHANNEL_CAPACITY);
    tokio::spawn(forward_discv4_candidates(updates, candidate_tx.clone()));
    let dns_enabled = spawn_dns_bootstrap(candidate_tx).await;

    tracing::info!(%local_addr, dns_enabled, "discv4 discovery started");

    Ok(Discovery {
        handle,
        candidates: ReceiverStream::new(candidate_rx),
    })
}

async fn forward_discv4_candidates(
    mut updates: ReceiverStream<DiscoveryUpdate>,
    candidate_tx: mpsc::Sender<DiscoveryCandidate>,
) {
    while let Some(update) = updates.next().await {
        let mut records = Vec::new();
        collect_discv4_candidates(update, &mut records);

        for node in records {
            if candidate_tx
                .send(DiscoveryCandidate {
                    node,
                    source: DiscoveryCandidateSource::Discv4,
                })
                .await
                .is_err()
            {
                debug!("discv4 candidate channel closed");
                return;
            }
        }
    }

    debug!("discv4 update stream closed");
}

fn collect_discv4_candidates(update: DiscoveryUpdate, records: &mut Vec<NodeRecord>) {
    match update {
        DiscoveryUpdate::Added(record) | DiscoveryUpdate::DiscoveredAtCapacity(record) => {
            records.push(record);
        }
        DiscoveryUpdate::Batch(updates) => {
            for update in updates {
                collect_discv4_candidates(update, records);
            }
        }
        DiscoveryUpdate::EnrForkId(_, _) | DiscoveryUpdate::Removed(_) => {}
    }
}

async fn spawn_dns_bootstrap(candidate_tx: mpsc::Sender<DiscoveryCandidate>) -> bool {
    let Some(dns_network) = NamedChain::Mainnet.public_dns_network_protocol() else {
        warn!("mainnet DNS bootstrap link unavailable");
        return false;
    };

    let dns_link = match dns_network.parse() {
        Ok(link) => link,
        Err(error) => {
            warn!(%dns_network, %error, "failed to parse mainnet DNS bootstrap link");
            return false;
        }
    };

    let resolver = match DnsResolver::from_system_conf() {
        Ok(resolver) => Arc::new(resolver),
        Err(error) => {
            warn!(%error, "failed to initialize DNS resolver for peer discovery");
            return false;
        }
    };

    let config = DnsDiscoveryConfig {
        bootstrap_dns_networks: Some(HashSet::from([dns_link])),
        ..DnsDiscoveryConfig::default()
    };

    let mut service = DnsDiscoveryService::new(resolver, config);
    let mut updates = service.node_record_stream();
    service.spawn();

    let mut eager_candidates = Vec::new();
    let bootstrap_started_at = Instant::now();
    while eager_candidates.len() < DNS_EAGER_BOOTSTRAP_LIMIT {
        let remaining = DNS_EAGER_BOOTSTRAP_BUDGET.saturating_sub(bootstrap_started_at.elapsed());
        if remaining.is_zero() {
            break;
        }

        match timeout(remaining, updates.next()).await {
            Ok(Some(update)) => eager_candidates.push(update.node_record),
            Ok(None) => {
                debug!("DNS discovery update stream closed during eager bootstrap");
                break;
            }
            Err(_) => break,
        }
    }

    tracing::info!(
        %dns_network,
        eager_candidates = eager_candidates.len(),
        "DNS discovery bootstrap started"
    );
    tokio::spawn(async move {
        for node in eager_candidates {
            if candidate_tx
                .send(DiscoveryCandidate {
                    node,
                    source: DiscoveryCandidateSource::Dns,
                })
                .await
                .is_err()
            {
                debug!("DNS candidate channel closed");
                return;
            }
        }

        while let Some(update) = updates.next().await {
            if candidate_tx
                .send(DiscoveryCandidate {
                    node: update.node_record,
                    source: DiscoveryCandidateSource::Dns,
                })
                .await
                .is_err()
            {
                debug!("DNS candidate channel closed");
                return;
            }
        }

        debug!("DNS discovery update stream closed");
    });

    true
}
