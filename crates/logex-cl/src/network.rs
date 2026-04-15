use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::net::{IpAddr, Ipv4Addr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alloy_primitives::hex;
use discv5::enr::{CombinedKey, NodeId};
use discv5::{ConfigBuilder, Discv5, Enr, Event, ListenConfig};
use logex_types::{ConsensusNetworkStatus, SyncStatus};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;
use tokio::task::JoinHandle;

const CONSENSUS_STATE_DIR: &str = "cl";
const DISCOVERY_SECRET_FILE: &str = "discovery-secret";
const KNOWN_PEERS_FILE: &str = "known-peers.json";
const DISCOVERY_QUERY_INTERVAL: Duration = Duration::from_secs(15);
const KNOWN_PEER_PERSIST_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct ConsensusNetworkConfig {
    pub data_dir: PathBuf,
    pub discovery_port: u16,
    pub p2p_port: u16,
    pub max_peers: usize,
}

#[derive(Debug, Error)]
pub enum ConsensusNetworkError {
    #[error("failed to load discovery secret {path}: {source}")]
    ReadSecret { path: PathBuf, source: io::Error },
    #[error("failed to parse discovery secret {path}: {message}")]
    ParseSecret { path: PathBuf, message: String },
    #[error("failed to persist discovery secret {path}: {source}")]
    PersistSecret { path: PathBuf, source: io::Error },
    #[error("failed to load known peers {path}: {source}")]
    ReadKnownPeers { path: PathBuf, source: io::Error },
    #[error("failed to parse known peers {path}: {message}")]
    ParseKnownPeers { path: PathBuf, message: String },
    #[error("failed to persist known peers {path}: {source}")]
    PersistKnownPeers { path: PathBuf, source: io::Error },
    #[error("invalid built-in mainnet bootnode ENR: {0}")]
    InvalidBootnode(String),
    #[error("built-in mainnet bootnodes did not expose an eth2 fork id")]
    MissingBootnodeForkId,
    #[error("failed to construct consensus discovery service: {0}")]
    ConstructDiscovery(String),
    #[error("failed to start consensus discovery service: {0}")]
    StartDiscovery(String),
    #[error("failed to open consensus discovery event stream: {0}")]
    EventStream(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct PersistedPeer {
    enr: String,
}

pub fn spawn_consensus_network(
    config: ConsensusNetworkConfig,
    sync_status: Arc<Mutex<SyncStatus>>,
    shutdown: watch::Receiver<bool>,
) -> Result<JoinHandle<()>, ConsensusNetworkError> {
    let network = ConsensusNetwork::new(config, sync_status)?;
    Ok(tokio::spawn(async move {
        if let Err(error) = network.run(shutdown).await {
            tracing::error!(%error, "consensus discovery task exited with an error");
        }
    }))
}

struct ConsensusNetwork {
    config: ConsensusNetworkConfig,
    sync_status: Arc<Mutex<SyncStatus>>,
    discv5: Discv5,
    bootnode_count: usize,
    known_peers_path: PathBuf,
    last_persisted: Vec<PersistedPeer>,
    observed: BTreeSet<String>,
}

impl ConsensusNetwork {
    fn new(
        config: ConsensusNetworkConfig,
        sync_status: Arc<Mutex<SyncStatus>>,
    ) -> Result<Self, ConsensusNetworkError> {
        let bootnodes = mainnet_bootnodes()?;
        let fork_id = current_eth2_fork_id(&bootnodes)?;
        let secret_path = discovery_secret_path(&config.data_dir);
        let known_peers_path = known_peers_path(&config.data_dir);
        let enr_key = load_or_create_secret_key(&secret_path)?;
        let local_enr = build_local_enr(&enr_key, &fork_id, config.discovery_port, config.p2p_port);
        let listen_config =
            ListenConfig::from_ip(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.discovery_port);
        let discovery_config = ConfigBuilder::new(listen_config)
            .enable_packet_filter()
            .build();
        let discv5 = Discv5::new(local_enr, enr_key, discovery_config)
            .map_err(|error| ConsensusNetworkError::ConstructDiscovery(error.to_string()))?;

        let known_peers = load_known_peers(&known_peers_path)?;

        for enr in &bootnodes {
            if let Err(error) = discv5.add_enr(enr.clone()) {
                tracing::warn!(%error, enr = %enr, "failed to seed consensus bootnode");
            }
        }

        for peer in &known_peers {
            match peer.enr.parse::<Enr>() {
                Ok(enr) => {
                    if let Err(error) = discv5.add_enr(enr.clone()) {
                        tracing::debug!(
                            %error,
                            enr = %peer.enr,
                            "skipping cached consensus peer that could not be inserted"
                        );
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        %error,
                        enr = %peer.enr,
                        "ignoring invalid cached consensus peer ENR"
                    );
                }
            }
        }

        Ok(Self {
            config,
            sync_status,
            discv5,
            bootnode_count: bootnodes.len(),
            known_peers_path,
            last_persisted: known_peers,
            observed: BTreeSet::new(),
        })
    }

    async fn run(
        mut self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), ConsensusNetworkError> {
        self.discv5
            .start()
            .await
            .map_err(|error| ConsensusNetworkError::StartDiscovery(error.to_string()))?;
        let mut event_stream = self
            .discv5
            .event_stream()
            .await
            .map_err(|error| ConsensusNetworkError::EventStream(error.to_string()))?;

        let local_enr = self.discv5.local_enr();
        tracing::info!(
            local_enr = %local_enr.to_base64(),
            node_id = %local_enr.node_id(),
            discovery_port = self.config.discovery_port,
            p2p_port = self.config.p2p_port,
            bootnodes = self.bootnode_count,
            "consensus discovery started"
        );
        self.refresh_status();

        let mut query_interval = tokio::time::interval(DISCOVERY_QUERY_INTERVAL);
        query_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut persist_interval = tokio::time::interval(KNOWN_PEER_PERSIST_INTERVAL);
        persist_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = wait_for_shutdown(&mut shutdown) => {
                    tracing::info!("consensus discovery shutting down");
                    break;
                }
                _ = query_interval.tick() => {
                    self.drive_discovery_queries().await;
                    self.refresh_status();
                }
                _ = persist_interval.tick() => {
                    if let Err(error) = self.persist_known_peers() {
                        tracing::warn!(%error, "failed to persist consensus peer cache");
                    }
                    self.refresh_status();
                }
                event = event_stream.recv() => {
                    match event {
                        Some(event) => {
                            self.handle_event(event);
                            self.refresh_status();
                        }
                        None => {
                            tracing::warn!("consensus discovery event stream ended unexpectedly");
                            break;
                        }
                    }
                }
            }
        }

        if let Err(error) = self.persist_known_peers() {
            tracing::warn!(%error, "failed to persist consensus peer cache during shutdown");
        }
        self.discv5.shutdown();
        self.refresh_status();
        Ok(())
    }

    async fn drive_discovery_queries(&mut self) {
        let target = NodeId::random();
        match self.discv5.find_node(target).await {
            Ok(found) => {
                for enr in found {
                    self.observe_enr(&enr);
                }
            }
            Err(error) => {
                tracing::debug!(%error, "consensus discovery query failed");
            }
        }
    }

    fn handle_event(&mut self, event: Event) {
        match event {
            Event::Discovered(enr) => self.observe_enr(&enr),
            Event::SessionEstablished(enr, socket) => {
                tracing::debug!(%socket, node_id = %enr.node_id(), "consensus discovery session established");
                self.observe_enr(&enr);
            }
            Event::NodeInserted { node_id, replaced } => {
                tracing::debug!(%node_id, replaced = replaced.map(|id| id.to_string()), "consensus discovery routing table updated");
            }
            Event::UnverifiableEnr { enr, socket, node_id } => {
                tracing::debug!(%socket, %node_id, enr = %enr.to_base64(), "consensus discovery received unverifiable ENR");
            }
            Event::SocketUpdated(socket) => {
                tracing::info!(%socket, "consensus discovery updated its observed socket");
            }
            Event::SessionsExpired(expired) => {
                tracing::debug!(count = expired.len(), "consensus discovery sessions expired");
            }
            Event::TalkRequest(_) => {
                tracing::debug!("consensus discovery received an unsupported TALKREQ");
            }
            _ => {}
        }
    }

    fn observe_enr(&mut self, enr: &Enr) {
        self.observed.insert(
            enr.node_id().to_string(),
        );
    }

    fn refresh_status(&self) {
        let table_entries = self.discv5.table_entries_enr();
        let status = ConsensusNetworkStatus {
            local_enr: Some(self.discv5.local_enr().to_base64()),
            local_node_id: Some(self.discv5.local_enr().node_id().to_string()),
            discovery_port: self.config.discovery_port,
            p2p_port: self.config.p2p_port,
            max_peers: self.config.max_peers,
            bootnode_count: self.bootnode_count,
            discovered_peers: self.observed.len(),
            dialable_peers: table_entries
                .iter()
                .filter(|enr| enr.tcp4().is_some() || enr.tcp6().is_some())
                .count(),
            routing_table_peers: table_entries.len(),
            active_sessions: self.discv5.connected_peers(),
        };
        self.sync_status.lock().unwrap().consensus_network = Some(status);
    }

    fn persist_known_peers(&mut self) -> Result<(), ConsensusNetworkError> {
        let mut peers = self
            .discv5
            .table_entries_enr()
            .into_iter()
            .filter(|enr| enr.tcp4().is_some() || enr.tcp6().is_some())
            .map(|enr| PersistedPeer {
                enr: enr.to_base64(),
            })
            .collect::<Vec<_>>();

        if self.config.max_peers > 0 && peers.len() > self.config.max_peers {
            peers.truncate(self.config.max_peers);
        }
        peers.sort_by(|left, right| left.enr.cmp(&right.enr));

        if peers == self.last_persisted {
            return Ok(());
        }

        persist_known_peers(&self.known_peers_path, &peers)?;
        self.last_persisted = peers;
        Ok(())
    }
}

fn build_local_enr(
    enr_key: &CombinedKey,
    fork_id: &[u8],
    discovery_port: u16,
    p2p_port: u16,
) -> Enr {
    let mut builder = Enr::builder();
    builder
        .udp4(discovery_port)
        .tcp4(p2p_port)
        .add_value("eth2", &fork_id);
    builder
        .build(enr_key)
        .expect("local consensus ENR should always be constructible")
}

fn discovery_secret_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join(CONSENSUS_STATE_DIR)
        .join(DISCOVERY_SECRET_FILE)
}

fn known_peers_path(data_dir: &Path) -> PathBuf {
    data_dir.join(CONSENSUS_STATE_DIR).join(KNOWN_PEERS_FILE)
}

fn load_or_create_secret_key(secret_key_path: &Path) -> Result<CombinedKey, ConsensusNetworkError> {
    match secret_key_path.try_exists() {
        Ok(true) => {
            let contents = fs::read_to_string(secret_key_path).map_err(|source| {
                ConsensusNetworkError::ReadSecret {
                    path: secret_key_path.to_path_buf(),
                    source,
                }
            })?;
            let hex_key = contents.trim().trim_start_matches("0x");
            let mut bytes = hex::decode(hex_key).map_err(|error| {
                ConsensusNetworkError::ParseSecret {
                    path: secret_key_path.to_path_buf(),
                    message: error.to_string(),
                }
            })?;
            CombinedKey::secp256k1_from_bytes(&mut bytes).map_err(|error| {
                ConsensusNetworkError::ParseSecret {
                    path: secret_key_path.to_path_buf(),
                    message: error.to_string(),
                }
            })
        }
        Ok(false) => {
            if let Some(dir) = secret_key_path.parent() {
                fs::create_dir_all(dir).map_err(|source| ConsensusNetworkError::PersistSecret {
                    path: secret_key_path.to_path_buf(),
                    source,
                })?;
            }

            let key = CombinedKey::generate_secp256k1();
            fs::write(secret_key_path, hex::encode(key.encode())).map_err(|source| {
                ConsensusNetworkError::PersistSecret {
                    path: secret_key_path.to_path_buf(),
                    source,
                }
            })?;
            Ok(key)
        }
        Err(source) => Err(ConsensusNetworkError::ReadSecret {
            path: secret_key_path.to_path_buf(),
            source,
        }),
    }
}

fn load_known_peers(path: &Path) -> Result<Vec<PersistedPeer>, ConsensusNetworkError> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path).map_err(|source| ConsensusNetworkError::ReadKnownPeers {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_str(&contents).map_err(|error| ConsensusNetworkError::ParseKnownPeers {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

fn persist_known_peers(
    path: &Path,
    peers: &[PersistedPeer],
) -> Result<(), ConsensusNetworkError> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir).map_err(|source| ConsensusNetworkError::PersistKnownPeers {
            path: path.to_path_buf(),
            source,
        })?;
    }

    let json = serde_json::to_vec_pretty(peers).map_err(|error| {
        ConsensusNetworkError::ParseKnownPeers {
            path: path.to_path_buf(),
            message: error.to_string(),
        }
    })?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json).map_err(|source| ConsensusNetworkError::PersistKnownPeers {
        path: tmp.clone(),
        source,
    })?;
    fs::rename(&tmp, path).map_err(|source| ConsensusNetworkError::PersistKnownPeers {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(())
}

fn mainnet_bootnodes() -> Result<Vec<Enr>, ConsensusNetworkError> {
    MAINNET_BOOTNODES
        .iter()
        .map(|enr| {
            enr.parse::<Enr>()
                .map_err(|_| ConsensusNetworkError::InvalidBootnode((*enr).to_string()))
        })
        .collect()
}

#[allow(deprecated)]
fn current_eth2_fork_id(bootnodes: &[Enr]) -> Result<Vec<u8>, ConsensusNetworkError> {
    bootnodes
        .iter()
        .find_map(|enr| enr.get("eth2").map(|bytes| bytes.to_vec()))
        .ok_or(ConsensusNetworkError::MissingBootnodeForkId)
}

async fn wait_for_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

const MAINNET_BOOTNODES: &[&str] = &[
    "enr:-KG4QNTx85fjxABbSq_Rta9wy56nQ1fHK0PewJbGjLm1M4bMGx5-3Qq4ZX2-iFJ0pys_O90sVXNNOxp2E7afBsGsBrgDhGV0aDKQu6TalgMAAAD__________4JpZIJ2NIJpcIQEnfA2iXNlY3AyNTZrMaECGXWQ-rQ2KZKRH1aOW4IlPDBkY4XDphxg9pxKytFCkayDdGNwgiMog3VkcIIjKA",
    "enr:-KG4QF4B5WrlFcRhUU6dZETwY5ZzAXnA0vGC__L1Kdw602nDZwXSTs5RFXFIFUnbQJmhNGVU6OIX7KVrCSTODsz1tK4DhGV0aDKQu6TalgMAAAD__________4JpZIJ2NIJpcIQExNYEiXNlY3AyNTZrMaECQmM9vp7KhaXhI-nqL_R0ovULLCFSFTa9CPPSdb1zPX6DdGNwgiMog3VkcIIjKA",
    "enr:-Ku4QImhMc1z8yCiNJ1TyUxdcfNucje3BGwEHzodEZUan8PherEo4sF7pPHPSIB1NNuSg5fZy7qFsjmUKs2ea1Whi0EBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQOVphkDqal4QzPMksc5wnpuC3gvSC8AfbFOnZY_On34wIN1ZHCCIyg",
    "enr:-Ku4QP2xDnEtUXIjzJ_DhlCRN9SN99RYQPJL92TMlSv7U5C1YnYLjwOQHgZIUXw6c-BvRg2Yc2QsZxxoS_pPRVe0yK8Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpD1pf1CAAAAAP__________gmlkgnY0gmlwhBLf22SJc2VjcDI1NmsxoQMeFF5GrS7UZpAH2Ly84aLK-TyvH-dRo0JM1i8yygH50YN1ZHCCJxA",
    "enr:-Le4QPUXJS2BTORXxyx2Ia-9ae4YqA_JWX3ssj4E_J-3z1A-HmFGrU8BpvpqhNabayXeOZ2Nq_sbeDgtzMJpLLnXFgAChGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISsaa0Zg2lwNpAkAIkHAAAAAPA8kv_-awoTiXNlY3AyNTZrMaEDHAD2JKYevx89W0CcFJFiskdcEzkH_Wdv9iW42qLK79ODdWRwgiMohHVkcDaCI4I",
    "enr:-Le4QLHZDSvkLfqgEo8IWGG96h6mxwe_PsggC20CL3neLBjfXLGAQFOPSltZ7oP6ol54OvaNqO02Rnvb8YmDR274uq8ChGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLosQxg2lwNpAqAX4AAAAAAPA8kv_-ax65iXNlY3AyNTZrMaEDBJj7_dLFACaxBfaI8KZTh_SSJUjhyAyfshimvSqo22WDdWRwgiMohHVkcDaCI4I",
    "enr:-Le4QH6LQrusDbAHPjU_HcKOuMeXfdEB5NJyXgHWFadfHgiySqeDyusQMvfphdYWOzuSZO9Uq2AMRJR5O4ip7OvVma8BhGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLY9ncg2lwNpAkAh8AgQIBAAAAAAAAAAmXiXNlY3AyNTZrMaECDYCZTZEksF-kmgPholqgVt8IXr-8L7Nu7YrZ7HUpgxmDdWRwgiMohHVkcDaCI4I",
    "enr:-Le4QIqLuWybHNONr933Lk0dcMmAB5WgvGKRyDihy1wHDIVlNuuztX62W51voT4I8qD34GcTEOTmag1bcdZ_8aaT4NUBhGV0aDKQtTA_KgEAAAAAIgEAAAAAAIJpZIJ2NIJpcISLY04ng2lwNpAkAh8AgAIBAAAAAAAAAA-fiXNlY3AyNTZrMaEDscnRV6n1m-D9ID5UsURk0jsoKNXt1TIrj8uKOGW6iluDdWRwgiMohHVkcDaCI4I",
    "enr:-Ku4QHqVeJ8PPICcWk1vSn_XcSkjOkNiTg6Fmii5j6vUQgvzMc9L1goFnLKgXqBJspJjIsB91LTOleFmyWWrFVATGngBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAMRHkWJc2VjcDI1NmsxoQKLVXFOhp2uX6jeT0DvvDpPcU8FWMjQdR4wMuORMhpX24N1ZHCCIyg",
    "enr:-Ku4QG-2_Md3sZIAUebGYT6g0SMskIml77l6yR-M_JXc-UdNHCmHQeOiMLbylPejyJsdAPsTHJyjJB2sYGDLe0dn8uYBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhBLY-NyJc2VjcDI1NmsxoQORcM6e19T1T9gi7jxEZjk_sjVLGFscUNqAY9obgZaxbIN1ZHCCIyg",
    "enr:-Ku4QPn5eVhcoF1opaFEvg1b6JNFD2rqVkHQ8HApOKK61OIcIXD127bKWgAtbwI7pnxx6cDyk_nI88TrZKQaGMZj0q0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDayLMaJc2VjcDI1NmsxoQK2sBOLGcUb4AwuYzFuAVCaNHA-dy24UuEKkeFNgCVCsIN1ZHCCIyg",
    "enr:-Ku4QEWzdnVtXc2Q0ZVigfCGggOVB2Vc1ZCPEc6j21NIFLODSJbvNaef1g4PxhPwl_3kax86YPheFUSLXPRs98vvYsoBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhDZBrP2Jc2VjcDI1NmsxoQM6jr8Rb1ktLEsVcKAPa08wCsKUmvoQ8khiOl_SLozf9IN1ZHCCIyg",
    "enr:-LK4QA8FfhaAjlb_BXsXxSfiysR7R52Nhi9JBt4F8SPssu8hdE1BXQQEtVDC3qStCW60LSO7hEsVHv5zm8_6Vnjhcn0Bh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhAN4aBKJc2VjcDI1NmsxoQJerDhsJ-KxZ8sHySMOCmTO6sHM3iCFQ6VMvLTe948MyYN0Y3CCI4yDdWRwgiOM",
    "enr:-LK4QKWrXTpV9T78hNG6s8AM6IO4XH9kFT91uZtFg1GcsJ6dKovDOr1jtAAFPnS2lvNltkOGA9k29BUN7lFh_sjuc9QBh2F0dG5ldHOIAAAAAAAAAACEZXRoMpC1MD8qAAAAAP__________gmlkgnY0gmlwhANAdd-Jc2VjcDI1NmsxoQLQa6ai7y9PMN5hpLe5HmiJSlYzMuzP7ZhwRiwHvqNXdoN0Y3CCI4yDdWRwgiOM",
];

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn discovery_secret_is_stable_after_first_write() {
        let temp = TempDir::new().unwrap();
        let path = discovery_secret_path(temp.path());

        let first = load_or_create_secret_key(&path).unwrap();
        let second = load_or_create_secret_key(&path).unwrap();

        assert_eq!(first.encode(), second.encode());
    }

    #[test]
    fn known_peers_round_trip() {
        let temp = TempDir::new().unwrap();
        let path = known_peers_path(temp.path());
        let peers = vec![
            PersistedPeer {
                enr: MAINNET_BOOTNODES[0].to_string(),
            },
            PersistedPeer {
                enr: MAINNET_BOOTNODES[1].to_string(),
            },
        ];

        persist_known_peers(&path, &peers).unwrap();
        let loaded = load_known_peers(&path).unwrap();

        assert_eq!(loaded, peers);
    }

    #[test]
    fn bundled_bootnodes_parse() {
        let bootnodes = mainnet_bootnodes().unwrap();

        assert!(bootnodes.len() >= 10);
        assert!(bootnodes.iter().all(|enr| enr.udp4().is_some()));
        assert_eq!(current_eth2_fork_id(&bootnodes).unwrap().len(), 16);
    }
}
