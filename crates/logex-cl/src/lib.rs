use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use logex_types::{
    ChainAnchors, ConsensusLightClientStatus, ExecutionAnchor, LightClientBootstrapStatus,
    LightClientFinalityUpdateStatus, LightClientOptimisticUpdateStatus, WeakSubjectivityCheckpoint,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod chain;
mod light_client;
mod network;
mod rpc;

pub(crate) use chain::MAINNET_CONSENSUS_CHAIN_SPEC;
pub use light_client::{
    LightClientDecodeError, decode_bootstrap, decode_finality_update, decode_optimistic_update,
};
pub use network::{ConsensusNetworkConfig, ConsensusNetworkError, spawn_consensus_network};
use rpc::RawRpcResponse;

const CONSENSUS_STATE_DIR: &str = "cl";
const CONSENSUS_STATE_FILE: &str = "consensus_state.json";

#[derive(Debug, Error)]
pub enum ConsensusStateError {
    #[error("missing weak-subjectivity checkpoint for a fresh data directory")]
    MissingCheckpoint,
    #[error("unsupported checkpoint descriptor format for {0}")]
    UnsupportedDescriptor(PathBuf),
    #[error("failed to read checkpoint descriptor {path}: {source}")]
    ReadDescriptor { path: PathBuf, source: io::Error },
    #[error("failed to parse checkpoint descriptor {path}: {message}")]
    ParseDescriptor { path: PathBuf, message: String },
    #[error("failed to load consensus state {path}: {source}")]
    ReadState { path: PathBuf, source: io::Error },
    #[error("failed to parse consensus state {path}: {message}")]
    ParseState { path: PathBuf, message: String },
    #[error("failed to persist consensus state {path}: {source}")]
    PersistState { path: PathBuf, source: io::Error },
    #[error("invalid checkpoint string: {0}")]
    InvalidCheckpoint(String),
    #[error(
        "existing consensus state is rooted at {persisted_root} slot {persisted_slot:?}, but startup requested {requested_root} slot {requested_slot:?}"
    )]
    ConflictingCheckpoint {
        persisted_root: alloy_primitives::B256,
        persisted_slot: Option<u64>,
        requested_root: alloy_primitives::B256,
        requested_slot: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorRecord {
    pub anchor: ExecutionAnchor,
    #[serde(default)]
    pub finalized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusSnapshot {
    pub checkpoint: WeakSubjectivityCheckpoint,
    #[serde(default)]
    pub anchors: ChainAnchors,
    #[serde(default)]
    pub ordered_anchors: Vec<AnchorRecord>,
    #[serde(default)]
    pub light_client: ConsensusLightClientStatus,
    #[serde(default)]
    pub light_client_payloads: PersistedLightClientPayloads,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedLightClientPayloads {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<RawRpcResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finality_update: Option<RawRpcResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimistic_update: Option<RawRpcResponse>,
}

#[derive(Debug)]
pub struct ConsensusStore {
    path: PathBuf,
    inner: Arc<Mutex<ConsensusSnapshot>>,
}

impl ConsensusStore {
    pub fn open(
        data_dir: impl AsRef<Path>,
        checkpoint: Option<&str>,
    ) -> Result<Self, ConsensusStateError> {
        let path = consensus_state_path(data_dir.as_ref());
        if path.exists() {
            let json =
                fs::read_to_string(&path).map_err(|source| ConsensusStateError::ReadState {
                    path: path.clone(),
                    source,
                })?;
            let snapshot: ConsensusSnapshot =
                serde_json::from_str(&json).map_err(|error| ConsensusStateError::ParseState {
                    path: path.clone(),
                    message: error.to_string(),
                })?;
            let store = Self {
                path,
                inner: Arc::new(Mutex::new(snapshot)),
            };
            if let Some(checkpoint) = checkpoint {
                let requested = load_checkpoint_descriptor(checkpoint)?.checkpoint;
                if store.reconcile_checkpoint(requested)? {
                    store.persist()?;
                }
            }
            return Ok(store);
        }

        let checkpoint = checkpoint.ok_or(ConsensusStateError::MissingCheckpoint)?;
        let snapshot = load_checkpoint_descriptor(checkpoint)?;
        let store = Self {
            path,
            inner: Arc::new(Mutex::new(snapshot)),
        };
        store.persist()?;
        Ok(store)
    }

    pub fn checkpoint(&self) -> WeakSubjectivityCheckpoint {
        self.inner.lock().unwrap().checkpoint
    }

    pub fn chain_anchors(&self) -> ChainAnchors {
        self.inner.lock().unwrap().anchors.clone()
    }

    pub fn ordered_anchors(&self) -> Vec<AnchorRecord> {
        self.inner.lock().unwrap().ordered_anchors.clone()
    }

    pub fn light_client_status(&self) -> ConsensusLightClientStatus {
        self.inner.lock().unwrap().light_client.clone()
    }

    pub fn light_client_payloads(&self) -> PersistedLightClientPayloads {
        self.inner.lock().unwrap().light_client_payloads.clone()
    }

    pub fn next_anchor_after(&self, block_number: u64) -> Option<ExecutionAnchor> {
        self.inner
            .lock()
            .unwrap()
            .ordered_anchors
            .iter()
            .find(|record| record.anchor.block_number > block_number)
            .map(|record| record.anchor)
    }

    pub fn anchor_at(&self, block_number: u64) -> Option<ExecutionAnchor> {
        self.inner
            .lock()
            .unwrap()
            .ordered_anchors
            .iter()
            .find(|record| record.anchor.block_number == block_number)
            .map(|record| record.anchor)
    }

    pub fn replace_anchors(&self, anchors: Vec<AnchorRecord>) -> Result<(), ConsensusStateError> {
        let mut snapshot = self.inner.lock().unwrap();
        snapshot.ordered_anchors = normalize_anchor_records(anchors);
        snapshot.anchors = compute_chain_anchors(&snapshot.ordered_anchors);
        drop(snapshot);
        self.persist()
    }

    pub fn append_anchors(&self, anchors: Vec<AnchorRecord>) -> Result<(), ConsensusStateError> {
        let mut snapshot = self.inner.lock().unwrap();
        snapshot.ordered_anchors.extend(anchors);
        let ordered = std::mem::take(&mut snapshot.ordered_anchors);
        snapshot.ordered_anchors = normalize_anchor_records(ordered);
        snapshot.anchors = compute_chain_anchors(&snapshot.ordered_anchors);
        drop(snapshot);
        self.persist()
    }

    pub fn update_bootstrap_status(
        &self,
        status: LightClientBootstrapStatus,
        payload: Option<RawRpcResponse>,
    ) -> Result<(), ConsensusStateError> {
        {
            let mut snapshot = self.inner.lock().unwrap();
            if snapshot.checkpoint.beacon_slot.is_none() {
                snapshot.checkpoint.beacon_slot = Some(status.header.beacon_slot);
            }
            snapshot.light_client.bootstrap = Some(status);
            if let Some(payload) = payload {
                snapshot.light_client_payloads.bootstrap = Some(payload);
            }
        }
        self.persist()
    }

    pub fn update_finality_update_status(
        &self,
        status: LightClientFinalityUpdateStatus,
        payload: Option<RawRpcResponse>,
    ) -> Result<(), ConsensusStateError> {
        let mut snapshot = self.inner.lock().unwrap();
        snapshot.light_client.finality_update = Some(status);
        if let Some(payload) = payload {
            snapshot.light_client_payloads.finality_update = Some(payload);
        }
        self.persist()
    }

    pub fn update_optimistic_update_status(
        &self,
        status: LightClientOptimisticUpdateStatus,
        payload: Option<RawRpcResponse>,
    ) -> Result<(), ConsensusStateError> {
        let mut snapshot = self.inner.lock().unwrap();
        snapshot.light_client.optimistic_update = Some(status);
        if let Some(payload) = payload {
            snapshot.light_client_payloads.optimistic_update = Some(payload);
        }
        self.persist()
    }

    pub fn persist(&self) -> Result<(), ConsensusStateError> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|source| ConsensusStateError::PersistState {
                path: self.path.clone(),
                source,
            })?;
        }

        let tmp = self.path.with_extension("json.tmp");
        let snapshot = self.inner.lock().unwrap().clone();
        let json = serde_json::to_vec_pretty(&snapshot).map_err(|error| {
            ConsensusStateError::ParseState {
                path: self.path.clone(),
                message: error.to_string(),
            }
        })?;
        fs::write(&tmp, json).map_err(|source| ConsensusStateError::PersistState {
            path: tmp.clone(),
            source,
        })?;
        fs::rename(&tmp, &self.path).map_err(|source| ConsensusStateError::PersistState {
            path: self.path.clone(),
            source,
        })?;
        Ok(())
    }

    pub fn state_path(&self) -> &Path {
        &self.path
    }

    fn reconcile_checkpoint(
        &self,
        requested: WeakSubjectivityCheckpoint,
    ) -> Result<bool, ConsensusStateError> {
        let mut snapshot = self.inner.lock().unwrap();
        if snapshot.checkpoint.beacon_root != requested.beacon_root {
            return Err(ConsensusStateError::ConflictingCheckpoint {
                persisted_root: snapshot.checkpoint.beacon_root,
                persisted_slot: snapshot.checkpoint.beacon_slot,
                requested_root: requested.beacon_root,
                requested_slot: requested.beacon_slot,
            });
        }

        match (snapshot.checkpoint.beacon_slot, requested.beacon_slot) {
            (Some(existing_slot), Some(requested_slot)) if existing_slot != requested_slot => {
                Err(ConsensusStateError::ConflictingCheckpoint {
                    persisted_root: snapshot.checkpoint.beacon_root,
                    persisted_slot: snapshot.checkpoint.beacon_slot,
                    requested_root: requested.beacon_root,
                    requested_slot: requested.beacon_slot,
                })
            }
            (None, Some(slot)) => {
                snapshot.checkpoint.beacon_slot = Some(slot);
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct CheckpointDescriptor {
    pub beacon_root: alloy_primitives::B256,
    #[serde(default)]
    pub beacon_slot: Option<u64>,
    #[serde(default)]
    pub anchors: Vec<AnchorRecord>,
}

fn load_checkpoint_descriptor(input: &str) -> Result<ConsensusSnapshot, ConsensusStateError> {
    let path = PathBuf::from(input);
    if path.exists() {
        let contents =
            fs::read_to_string(&path).map_err(|source| ConsensusStateError::ReadDescriptor {
                path: path.clone(),
                source,
            })?;
        let descriptor = match path.extension().and_then(|ext| ext.to_str()) {
            Some("json") => {
                serde_json::from_str::<CheckpointDescriptor>(&contents).map_err(|error| {
                    ConsensusStateError::ParseDescriptor {
                        path: path.clone(),
                        message: error.to_string(),
                    }
                })?
            }
            Some("toml") => toml::from_str::<CheckpointDescriptor>(&contents).map_err(|error| {
                ConsensusStateError::ParseDescriptor {
                    path: path.clone(),
                    message: error.to_string(),
                }
            })?,
            _ => return Err(ConsensusStateError::UnsupportedDescriptor(path)),
        };
        let ordered_anchors = normalize_anchor_records(descriptor.anchors);
        return Ok(ConsensusSnapshot {
            checkpoint: WeakSubjectivityCheckpoint {
                beacon_root: descriptor.beacon_root,
                beacon_slot: descriptor.beacon_slot,
            },
            anchors: compute_chain_anchors(&ordered_anchors),
            ordered_anchors,
            light_client: ConsensusLightClientStatus::default(),
            light_client_payloads: PersistedLightClientPayloads::default(),
        });
    }

    let checkpoint = parse_checkpoint_string(input)?;
    Ok(ConsensusSnapshot {
        checkpoint,
        anchors: ChainAnchors::default(),
        ordered_anchors: Vec::new(),
        light_client: ConsensusLightClientStatus::default(),
        light_client_payloads: PersistedLightClientPayloads::default(),
    })
}

fn parse_checkpoint_string(input: &str) -> Result<WeakSubjectivityCheckpoint, ConsensusStateError> {
    if let Some((slot, root)) = input.split_once('@') {
        let beacon_slot = slot.parse().map_err(|_| {
            ConsensusStateError::InvalidCheckpoint(format!("invalid slot in {input}"))
        })?;
        let beacon_root = root.parse().map_err(|_| {
            ConsensusStateError::InvalidCheckpoint(format!("invalid beacon root in {input}"))
        })?;
        return Ok(WeakSubjectivityCheckpoint {
            beacon_root,
            beacon_slot: Some(beacon_slot),
        });
    }

    let beacon_root = input.parse().map_err(|_| {
        ConsensusStateError::InvalidCheckpoint(format!(
            "expected 0x-prefixed beacon root or <slot>@<root>, got {input}"
        ))
    })?;
    Ok(WeakSubjectivityCheckpoint {
        beacon_root,
        beacon_slot: None,
    })
}

fn normalize_anchor_records(mut anchors: Vec<AnchorRecord>) -> Vec<AnchorRecord> {
    let mut deduped = BTreeMap::new();
    for record in anchors.drain(..) {
        deduped.insert(record.anchor.block_number, record);
    }
    deduped.into_values().collect()
}

fn compute_chain_anchors(anchors: &[AnchorRecord]) -> ChainAnchors {
    let optimistic_head = anchors.last().map(|record| record.anchor);
    let finalized_head = anchors
        .iter()
        .rev()
        .find(|record| record.finalized)
        .map(|record| record.anchor);

    ChainAnchors {
        indexed_head: None,
        finalized_head,
        optimistic_head,
    }
}

fn consensus_state_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join(CONSENSUS_STATE_DIR)
        .join(CONSENSUS_STATE_FILE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use tempfile::TempDir;

    #[test]
    fn parses_inline_checkpoint_root() {
        let checkpoint = parse_checkpoint_string(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
        )
        .unwrap();
        assert_eq!(checkpoint.beacon_slot, None);
        assert_eq!(checkpoint.beacon_root, B256::repeat_byte(0x11));
    }

    #[test]
    fn parses_inline_checkpoint_with_slot() {
        let checkpoint = parse_checkpoint_string(
            "12345@0x2222222222222222222222222222222222222222222222222222222222222222",
        )
        .unwrap();
        assert_eq!(checkpoint.beacon_slot, Some(12_345));
        assert_eq!(checkpoint.beacon_root, B256::repeat_byte(0x22));
    }

    #[test]
    fn loads_descriptor_and_persists_state() {
        let temp = TempDir::new().unwrap();
        let descriptor = temp.path().join("checkpoint.json");
        fs::write(
            &descriptor,
            serde_json::json!({
                "beacon_root": format!("{:#x}", B256::repeat_byte(0x33)),
                "beacon_slot": 777,
                "anchors": [
                    {
                        "anchor": {
                            "beacon_root": format!("{:#x}", B256::repeat_byte(0x44)),
                            "beacon_slot": 800,
                            "block_number": 10,
                            "block_hash": format!("{:#x}", B256::repeat_byte(0x55)),
                            "receipts_root": format!("{:#x}", B256::repeat_byte(0x66))
                        },
                        "finalized": true
                    }
                ]
            })
            .to_string(),
        )
        .unwrap();

        let store = ConsensusStore::open(temp.path(), Some(descriptor.to_str().unwrap())).unwrap();
        assert_eq!(store.checkpoint().beacon_slot, Some(777));
        assert_eq!(
            store
                .chain_anchors()
                .finalized_head
                .map(|anchor| anchor.block_number),
            Some(10)
        );
        assert!(store.state_path().exists());

        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.checkpoint(), store.checkpoint());
        assert_eq!(
            reopened.chain_anchors().finalized_head,
            store.chain_anchors().finalized_head
        );
    }

    #[test]
    fn next_anchor_after_returns_ordered_anchor() {
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        store
            .append_anchors(vec![
                AnchorRecord {
                    anchor: ExecutionAnchor {
                        beacon_root: B256::repeat_byte(0x01),
                        beacon_slot: 1,
                        block_number: 3,
                        block_hash: B256::repeat_byte(0x03),
                        receipts_root: B256::repeat_byte(0x13),
                    },
                    finalized: false,
                },
                AnchorRecord {
                    anchor: ExecutionAnchor {
                        beacon_root: B256::repeat_byte(0x01),
                        beacon_slot: 1,
                        block_number: 2,
                        block_hash: B256::repeat_byte(0x02),
                        receipts_root: B256::repeat_byte(0x12),
                    },
                    finalized: false,
                },
            ])
            .unwrap();

        assert_eq!(
            store.next_anchor_after(1).map(|anchor| anchor.block_number),
            Some(2)
        );
        assert_eq!(
            store.anchor_at(2).map(|anchor| anchor.block_hash),
            Some(B256::repeat_byte(0x02))
        );
        assert_eq!(
            store.next_anchor_after(2).map(|anchor| anchor.block_number),
            Some(3)
        );
        assert_eq!(store.next_anchor_after(3), None);
    }

    #[test]
    fn bootstrap_status_recovers_checkpoint_slot_when_missing() {
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();

        assert_eq!(store.checkpoint().beacon_slot, None);
        store
            .update_bootstrap_status(
                LightClientBootstrapStatus {
                    fork: logex_types::ConsensusDataFork::Deneb,
                    header: logex_types::LightClientHeaderSummary {
                        beacon_slot: 12_345,
                        execution: None,
                    },
                    current_sync_committee_pubkeys: 512,
                    current_sync_committee_branch_depth: 5,
                },
                None,
            )
            .unwrap();

        assert_eq!(store.checkpoint().beacon_slot, Some(12_345));
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.checkpoint().beacon_slot, Some(12_345));
    }

    #[test]
    fn reopening_with_same_root_and_known_slot_enriches_persisted_checkpoint() {
        let temp = TempDir::new().unwrap();
        let root = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        ConsensusStore::open(temp.path(), Some(root)).unwrap();

        let reopened = ConsensusStore::open(temp.path(), Some(&format!("777@{root}"))).unwrap();
        assert_eq!(reopened.checkpoint().beacon_slot, Some(777));

        let persisted = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(persisted.checkpoint().beacon_slot, Some(777));
    }

    #[test]
    fn reopening_with_conflicting_checkpoint_root_fails() {
        let temp = TempDir::new().unwrap();
        ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();

        let error = ConsensusStore::open(
            temp.path(),
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConsensusStateError::ConflictingCheckpoint { .. }
        ));
    }
}
