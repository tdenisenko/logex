use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use alloy_primitives::B256;
use logex_types::{
    ChainAnchors, ConsensusLightClientStatus, ExecutionAnchor, LightClientBootstrapStatus,
    LightClientFinalityUpdateStatus, LightClientOptimisticUpdateStatus, WeakSubjectivityCheckpoint,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod beacon_block;
mod chain;
mod light_client;
mod network;
mod rpc;

pub(crate) use beacon_block::{VerifiedBeaconBlock, decode_verified_beacon_block};
pub use chain::{
    CONSENSUS_HEAD_FRESHNESS_TOLERANCE_SLOTS, MAINNET_CONSENSUS_CHAIN_SPEC,
    optimistic_head_is_fresh_at, optimistic_head_lag_slots,
};
pub(crate) use light_client::{
    AppliedLightClientUpdate, VerifiedLightClientStore, apply_finality_update_payload,
    apply_light_client_update_payload, apply_optimistic_update_payload,
    force_update_light_client_store, verify_bootstrap_payload,
};
pub use light_client::{
    LightClientDecodeError, LightClientVerificationError, decode_bootstrap, decode_finality_update,
    decode_optimistic_update,
};
pub use network::{
    ConsensusDialAddressFamilies, ConsensusNetworkConfig, ConsensusNetworkError,
    spawn_consensus_network,
};
use rpc::RawRpcResponse;

const CONSENSUS_STATE_DIR: &str = "cl";
const CONSENSUS_STATE_FILE: &str = "consensus_state.json";
// The full consensus-spec weak-subjectivity period is state-derived. Until LogEx
// persists enough beacon state to compute it exactly, use the published mainnet
// upper-bound reference window for Electra-style weak subjectivity protection.
pub const CONSERVATIVE_WEAK_SUBJECTIVITY_FRESHNESS_EPOCHS: u64 = 3_532;

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
    #[error(
        "persisted consensus trusted slot {trusted_slot} at epoch {trusted_epoch} is stale at current epoch {current_epoch}; it exceeds the conservative weak-subjectivity freshness window of {max_epochs} epochs. Start with a recent --checkpoint in a fresh data directory."
    )]
    StaleWeakSubjectivityCheckpoint {
        trusted_slot: u64,
        trusted_epoch: u64,
        current_epoch: u64,
        max_epochs: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnchorRecord {
    pub anchor: ExecutionAnchor,
    #[serde(default)]
    pub finalized: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_beacon_root: Option<B256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnchorCoverage {
    pub floor: Option<ExecutionAnchor>,
    pub ceiling: Option<ExecutionAnchor>,
    pub count: usize,
    pub gap_count: usize,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    verified_light_client_store: Option<VerifiedLightClientStore>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PersistedLightClientPayloads {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bootstrap: Option<RawRpcResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finality_update: Option<RawRpcResponse>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub optimistic_update: Option<RawRpcResponse>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub updates_by_period: BTreeMap<u64, RawRpcResponse>,
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
            let snapshot = rehydrate_verified_light_client_state(snapshot);
            ensure_snapshot_within_weak_subjectivity_period(&snapshot)?;
            let store = Self {
                path,
                inner: Arc::new(Mutex::new(snapshot)),
            };
            if let Some(checkpoint) = checkpoint {
                let requested = load_checkpoint_descriptor(checkpoint)?.checkpoint;
                if store.reconcile_checkpoint(requested)? {
                    ensure_snapshot_within_weak_subjectivity_period(&store.inner.lock().unwrap())?;
                    store.persist()?;
                }
            }
            return Ok(store);
        }

        let checkpoint = checkpoint.ok_or(ConsensusStateError::MissingCheckpoint)?;
        let snapshot = load_checkpoint_descriptor(checkpoint)?;
        ensure_snapshot_within_weak_subjectivity_period(&snapshot)?;
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

    pub fn trusted_beacon_slot(&self) -> Option<u64> {
        weak_subjectivity_trusted_slot(&self.inner.lock().unwrap())
    }

    pub fn chain_anchors(&self) -> ChainAnchors {
        self.inner.lock().unwrap().anchors.clone()
    }

    pub fn ordered_anchors(&self) -> Vec<AnchorRecord> {
        self.inner.lock().unwrap().ordered_anchors.clone()
    }

    pub fn anchor_coverage(&self) -> AnchorCoverage {
        let snapshot = self.inner.lock().unwrap();
        AnchorCoverage {
            floor: snapshot.ordered_anchors.first().map(|record| record.anchor),
            ceiling: snapshot.ordered_anchors.last().map(|record| record.anchor),
            count: snapshot.ordered_anchors.len(),
            gap_count: anchor_record_gap_count(&snapshot.ordered_anchors),
        }
    }

    pub fn highest_anchor_block_from(&self, start_block: u64) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .ordered_anchors
            .iter()
            .rev()
            .find(|record| record.anchor.block_number >= start_block)
            .map(|record| record.anchor.block_number)
    }

    pub fn light_client_status(&self) -> ConsensusLightClientStatus {
        self.inner.lock().unwrap().light_client.clone()
    }

    pub fn light_client_payloads(&self) -> PersistedLightClientPayloads {
        self.inner.lock().unwrap().light_client_payloads.clone()
    }

    pub(crate) fn light_client_store(&self) -> Option<VerifiedLightClientStore> {
        self.inner
            .lock()
            .unwrap()
            .verified_light_client_store
            .clone()
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
        recompute_snapshot_anchors(&mut snapshot);
        drop(snapshot);
        self.persist()
    }

    pub fn append_anchors(&self, anchors: Vec<AnchorRecord>) -> Result<(), ConsensusStateError> {
        let mut snapshot = self.inner.lock().unwrap();
        snapshot.ordered_anchors.extend(anchors);
        let ordered = std::mem::take(&mut snapshot.ordered_anchors);
        snapshot.ordered_anchors = normalize_anchor_records(ordered);
        recompute_snapshot_anchors(&mut snapshot);
        drop(snapshot);
        self.persist()
    }

    pub fn replace_anchor_range(
        &self,
        start_block: u64,
        end_block: u64,
        anchors: Vec<AnchorRecord>,
    ) -> Result<(), ConsensusStateError> {
        let mut snapshot = self.inner.lock().unwrap();
        snapshot.ordered_anchors.retain(|record| {
            record.anchor.block_number < start_block || record.anchor.block_number > end_block
        });
        snapshot.ordered_anchors.extend(anchors);
        let ordered = std::mem::take(&mut snapshot.ordered_anchors);
        snapshot.ordered_anchors = normalize_anchor_records(ordered);
        recompute_snapshot_anchors(&mut snapshot);
        drop(snapshot);
        self.persist()
    }

    pub(crate) fn record_verified_bootstrap(
        &self,
        status: LightClientBootstrapStatus,
        payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        {
            let mut snapshot = self.inner.lock().unwrap();
            snapshot.checkpoint.beacon_slot = Some(store.bootstrap_slot());
            snapshot.light_client.bootstrap = Some(status);
            snapshot.light_client.finality_update = None;
            snapshot.light_client.optimistic_update = None;
            snapshot.light_client_payloads.bootstrap = Some(payload);
            snapshot.light_client_payloads.finality_update = None;
            snapshot.light_client_payloads.optimistic_update = None;
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(&mut snapshot);
        }
        self.persist()
    }

    pub(crate) fn record_verified_finality_update(
        &self,
        status: LightClientFinalityUpdateStatus,
        payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        {
            let mut snapshot = self.inner.lock().unwrap();
            snapshot.light_client.finality_update = Some(status);
            snapshot.light_client_payloads.finality_update = Some(payload);
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(&mut snapshot);
        }
        self.persist()
    }

    pub(crate) fn record_verified_optimistic_update(
        &self,
        status: LightClientOptimisticUpdateStatus,
        payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        {
            let mut snapshot = self.inner.lock().unwrap();
            snapshot.light_client.optimistic_update = Some(status);
            snapshot.light_client_payloads.optimistic_update = Some(payload);
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(&mut snapshot);
        }
        self.persist()
    }

    pub(crate) fn record_verified_applied_update(
        &self,
        applied: AppliedLightClientUpdate,
        verified_updates_by_period: Vec<(u64, RawRpcResponse)>,
    ) -> Result<(), ConsensusStateError> {
        {
            let mut snapshot = self.inner.lock().unwrap();
            snapshot.light_client.optimistic_update = Some(applied.optimistic_status);
            if let Some(status) = applied.finality_status {
                snapshot.light_client.finality_update = Some(status);
            }
            for (period, payload) in verified_updates_by_period {
                snapshot
                    .light_client_payloads
                    .updates_by_period
                    .insert(period, payload);
            }
            snapshot.verified_light_client_store = Some(applied.store);
            apply_verified_store(&mut snapshot);
        }
        self.persist()
    }

    pub(crate) fn replace_verified_light_client_store(
        &self,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        {
            let mut snapshot = self.inner.lock().unwrap();
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(&mut snapshot);
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
            anchors: ChainAnchors::default(),
            ordered_anchors,
            light_client: ConsensusLightClientStatus::default(),
            light_client_payloads: PersistedLightClientPayloads::default(),
            verified_light_client_store: None,
        }
        .with_recomputed_anchors());
    }

    let checkpoint = parse_checkpoint_string(input)?;
    Ok(ConsensusSnapshot {
        checkpoint,
        anchors: ChainAnchors::default(),
        ordered_anchors: Vec::new(),
        light_client: ConsensusLightClientStatus::default(),
        light_client_payloads: PersistedLightClientPayloads::default(),
        verified_light_client_store: None,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WeakSubjectivityStaleness {
    trusted_slot: u64,
    trusted_epoch: u64,
    current_epoch: u64,
    max_epochs: u64,
}

fn ensure_snapshot_within_weak_subjectivity_period(
    snapshot: &ConsensusSnapshot,
) -> Result<(), ConsensusStateError> {
    match weak_subjectivity_staleness_for_epoch(
        snapshot,
        MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_epoch(),
        CONSERVATIVE_WEAK_SUBJECTIVITY_FRESHNESS_EPOCHS,
    ) {
        Some(staleness) => Err(ConsensusStateError::StaleWeakSubjectivityCheckpoint {
            trusted_slot: staleness.trusted_slot,
            trusted_epoch: staleness.trusted_epoch,
            current_epoch: staleness.current_epoch,
            max_epochs: staleness.max_epochs,
        }),
        None => Ok(()),
    }
}

fn weak_subjectivity_staleness_for_epoch(
    snapshot: &ConsensusSnapshot,
    current_epoch: u64,
    max_epochs: u64,
) -> Option<WeakSubjectivityStaleness> {
    let trusted_slot = weak_subjectivity_trusted_slot(snapshot)?;
    let trusted_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(trusted_slot);
    (current_epoch > trusted_epoch.saturating_add(max_epochs)).then_some(
        WeakSubjectivityStaleness {
            trusted_slot,
            trusted_epoch,
            current_epoch,
            max_epochs,
        },
    )
}

fn weak_subjectivity_trusted_slot(snapshot: &ConsensusSnapshot) -> Option<u64> {
    snapshot
        .verified_light_client_store
        .as_ref()
        // Bootstrap slot metadata from older stores may have come from a
        // caller-supplied hint. Only the verified header establishes freshness.
        .map(|store| store.finalized_header.beacon.slot)
        .or(snapshot.checkpoint.beacon_slot)
}

fn normalize_anchor_records(mut anchors: Vec<AnchorRecord>) -> Vec<AnchorRecord> {
    let mut deduped = BTreeMap::new();
    for record in anchors.drain(..) {
        deduped.insert(record.anchor.block_number, record);
    }
    deduped.into_values().collect()
}

fn anchor_record_gap_count(anchors: &[AnchorRecord]) -> usize {
    anchors
        .windows(2)
        .filter(|window| {
            let previous = window[0];
            let current = window[1];
            current.anchor.block_number != previous.anchor.block_number.saturating_add(1)
                || current.parent_beacon_root != Some(previous.anchor.beacon_root)
        })
        .count()
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

fn apply_verified_store(snapshot: &mut ConsensusSnapshot) {
    if let Some(store) = &snapshot.verified_light_client_store {
        snapshot.anchors.finalized_head = store.finalized_anchor();
        snapshot.anchors.optimistic_head = store.optimistic_anchor();
    }
}

fn recompute_snapshot_anchors(snapshot: &mut ConsensusSnapshot) {
    let indexed_head = snapshot.anchors.indexed_head;
    snapshot.anchors = compute_chain_anchors(&snapshot.ordered_anchors);
    snapshot.anchors.indexed_head = indexed_head;
    apply_verified_store(snapshot);
}

fn rehydrate_verified_light_client_state(mut snapshot: ConsensusSnapshot) -> ConsensusSnapshot {
    if let Some(store) = snapshot.verified_light_client_store.as_mut() {
        if store.bootstrap_slot == 0 {
            store.bootstrap_slot = snapshot
                .checkpoint
                .beacon_slot
                .unwrap_or(store.finalized_header.beacon.slot);
        }
        recompute_snapshot_anchors(&mut snapshot);
        return snapshot;
    }

    recompute_snapshot_anchors(&mut snapshot);
    snapshot.light_client = ConsensusLightClientStatus::default();
    let mut verified_payloads = PersistedLightClientPayloads::default();
    let mut verified_store = None;

    if let Some(payload) = snapshot.light_client_payloads.bootstrap.clone() {
        match verify_bootstrap_payload(&payload.bytes, snapshot.checkpoint) {
            Ok((status, store)) => {
                snapshot.checkpoint.beacon_slot = Some(store.bootstrap_slot());
                snapshot.light_client.bootstrap = Some(status);
                verified_payloads.bootstrap = Some(payload);
                verified_store = Some(store);
            }
            Err(error) => {
                tracing::warn!(%error, "discarding previously persisted unverified bootstrap payload");
            }
        }
    }

    if let (Some(payload), Some(store)) = (
        snapshot.light_client_payloads.finality_update.clone(),
        verified_store.clone(),
    ) {
        match apply_finality_update_payload(&payload.bytes, &store) {
            Ok((status, next_store, _, _)) => {
                snapshot.light_client.finality_update = Some(status);
                verified_payloads.finality_update = Some(payload);
                verified_store = Some(next_store);
            }
            Err(error) => {
                tracing::warn!(%error, "discarding previously persisted unverified finality update payload");
            }
        }
    }

    if let (Some(payload), Some(store)) = (
        snapshot.light_client_payloads.optimistic_update.clone(),
        verified_store.clone(),
    ) {
        match apply_optimistic_update_payload(&payload.bytes, &store) {
            Ok((status, next_store, _)) => {
                snapshot.light_client.optimistic_update = Some(status);
                verified_payloads.optimistic_update = Some(payload);
                verified_store = Some(next_store);
            }
            Err(error) => {
                tracing::warn!(%error, "discarding previously persisted unverified optimistic update payload");
            }
        }
    }

    snapshot.light_client_payloads = verified_payloads;
    snapshot.verified_light_client_store = verified_store;
    recompute_snapshot_anchors(&mut snapshot);
    snapshot
}

trait ConsensusSnapshotExt {
    fn with_recomputed_anchors(self) -> Self;
}

impl ConsensusSnapshotExt for ConsensusSnapshot {
    fn with_recomputed_anchors(mut self) -> Self {
        recompute_snapshot_anchors(&mut self);
        self
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
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn verified_header(
        slot: u64,
        byte: u8,
        block_number: u64,
    ) -> crate::light_client::VerifiedLightClientHeader {
        crate::light_client::VerifiedLightClientHeader {
            fork: logex_types::ConsensusDataFork::Electra,
            beacon: crate::light_client::BeaconBlockHeaderSsz {
                slot,
                proposer_index: 0,
                parent_root: B256::repeat_byte(byte),
                state_root: B256::repeat_byte(byte.wrapping_add(1)),
                body_root: B256::repeat_byte(byte.wrapping_add(2)),
            },
            execution: Some(crate::light_client::VerifiedExecutionPayloadHeader {
                block_number,
                block_hash: B256::repeat_byte(byte.wrapping_add(3)),
                receipts_root: B256::repeat_byte(byte.wrapping_add(4)),
            }),
        }
    }

    fn recent_test_slot(offset: u64) -> u64 {
        MAINNET_CONSENSUS_CHAIN_SPEC
            .wall_clock_epoch()
            .saturating_sub(8)
            .saturating_mul(32)
            .saturating_add(offset)
    }

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
    fn weak_subjectivity_freshness_allows_recent_trusted_slot() {
        let snapshot = ConsensusSnapshot {
            checkpoint: WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(0x10),
                beacon_slot: Some(3_200),
            },
            anchors: ChainAnchors::default(),
            ordered_anchors: Vec::new(),
            light_client: ConsensusLightClientStatus::default(),
            light_client_payloads: PersistedLightClientPayloads::default(),
            verified_light_client_store: None,
        };

        assert_eq!(
            weak_subjectivity_staleness_for_epoch(&snapshot, 110, 10),
            None
        );
        assert_eq!(
            weak_subjectivity_staleness_for_epoch(&snapshot, 111, 10),
            Some(WeakSubjectivityStaleness {
                trusted_slot: 3_200,
                trusted_epoch: 100,
                current_epoch: 111,
                max_epochs: 10,
            })
        );
    }

    #[test]
    fn weak_subjectivity_freshness_uses_verified_finalized_slot_on_restart() {
        let mut snapshot = ConsensusSnapshot {
            checkpoint: WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(0x10),
                beacon_slot: Some(32),
            },
            anchors: ChainAnchors::default(),
            ordered_anchors: Vec::new(),
            light_client: ConsensusLightClientStatus::default(),
            light_client_payloads: PersistedLightClientPayloads::default(),
            verified_light_client_store: Some(VerifiedLightClientStore {
                checkpoint_root: B256::repeat_byte(0x10),
                bootstrap_slot: 32,
                current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                next_sync_committee: None,
                finalized_header: verified_header(3_200, 0x20, 3_200),
                optimistic_header: verified_header(3_232, 0x21, 3_232),
                best_valid_update: None,
                previous_max_active_participants: 0,
                current_max_active_participants: 0,
            }),
        };

        assert_eq!(weak_subjectivity_trusted_slot(&snapshot), Some(3_200));
        assert_eq!(
            weak_subjectivity_staleness_for_epoch(&snapshot, 110, 10),
            None
        );
        assert_eq!(
            weak_subjectivity_staleness_for_epoch(&snapshot, 111, 10)
                .map(|staleness| staleness.trusted_slot),
            Some(3_200)
        );
        for unverified_slot in [0, 32, 1_000_000, u64::MAX] {
            snapshot.checkpoint.beacon_slot = Some(unverified_slot);
            snapshot
                .verified_light_client_store
                .as_mut()
                .unwrap()
                .bootstrap_slot = unverified_slot;
            assert_eq!(weak_subjectivity_trusted_slot(&snapshot), Some(3_200));
            assert!(weak_subjectivity_staleness_for_epoch(&snapshot, 111, 10).is_some());
        }
    }

    #[test]
    fn root_only_checkpoint_cannot_be_declared_stale_before_bootstrap() {
        let snapshot = ConsensusSnapshot {
            checkpoint: WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(0x10),
                beacon_slot: None,
            },
            anchors: ChainAnchors::default(),
            ordered_anchors: Vec::new(),
            light_client: ConsensusLightClientStatus::default(),
            light_client_payloads: PersistedLightClientPayloads::default(),
            verified_light_client_store: None,
        };

        assert_eq!(
            weak_subjectivity_staleness_for_epoch(&snapshot, u64::MAX, 0),
            None
        );
    }

    #[test]
    fn opening_stale_slotted_checkpoint_requires_new_checkpoint() {
        let temp = TempDir::new().unwrap();
        let error = ConsensusStore::open(
            temp.path(),
            Some("0@0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            ConsensusStateError::StaleWeakSubjectivityCheckpoint { .. }
        ));
    }

    #[test]
    fn loads_descriptor_and_persists_state() {
        let temp = TempDir::new().unwrap();
        let descriptor = temp.path().join("checkpoint.json");
        let checkpoint_slot = recent_test_slot(0);
        let anchor_slot = checkpoint_slot + 23;
        let beacon_root = format!("{:#x}", B256::repeat_byte(0x33));
        let anchor_beacon_root = format!("{:#x}", B256::repeat_byte(0x44));
        let block_hash = format!("{:#x}", B256::repeat_byte(0x55));
        let receipts_root = format!("{:#x}", B256::repeat_byte(0x66));
        let descriptor_json = format!(
            r#"{{
  "beacon_root": "{beacon_root}",
  "beacon_slot": {checkpoint_slot},
  "anchors": [
    {{
      "anchor": {{
        "beacon_root": "{anchor_beacon_root}",
        "beacon_slot": {anchor_slot},
        "block_number": 10,
        "block_hash": "{block_hash}",
        "receipts_root": "{receipts_root}"
      }},
      "finalized": true
    }}
  ]
}}"#,
        );
        fs::write(&descriptor, descriptor_json).unwrap();

        let store = ConsensusStore::open(temp.path(), Some(descriptor.to_str().unwrap())).unwrap();
        assert_eq!(store.checkpoint().beacon_slot, Some(checkpoint_slot));
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
                    parent_beacon_root: None,
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
                    parent_beacon_root: None,
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
    fn replace_anchor_range_merges_backward_and_forward_checkpoint_expansions() {
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let anchor = |block_number: u64, byte: u8, finalized: bool| AnchorRecord {
            anchor: ExecutionAnchor {
                beacon_root: B256::repeat_byte(byte),
                beacon_slot: block_number,
                block_number,
                block_hash: B256::repeat_byte(byte.wrapping_add(1)),
                receipts_root: B256::repeat_byte(byte.wrapping_add(2)),
            },
            finalized,
            parent_beacon_root: None,
        };

        store
            .replace_anchor_range(
                100,
                102,
                vec![
                    anchor(100, 0x10, true),
                    anchor(101, 0x11, false),
                    anchor(102, 0x12, false),
                ],
            )
            .unwrap();
        store
            .replace_anchor_range(
                98,
                100,
                vec![
                    anchor(98, 0x08, true),
                    anchor(99, 0x09, true),
                    anchor(100, 0x10, true),
                ],
            )
            .unwrap();

        let ordered = store.ordered_anchors();
        let blocks = ordered
            .iter()
            .map(|record| record.anchor.block_number)
            .collect::<Vec<_>>();
        assert_eq!(blocks, vec![98, 99, 100, 101, 102]);
        assert_eq!(
            store
                .chain_anchors()
                .optimistic_head
                .map(|anchor| anchor.block_number),
            Some(102)
        );
        assert_eq!(
            store
                .chain_anchors()
                .finalized_head
                .map(|anchor| anchor.block_number),
            Some(100)
        );
    }

    #[test]
    fn anchor_coverage_reports_materialized_continuity_gaps() {
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let anchor = |block_number: u64, byte: u8, parent: Option<B256>| AnchorRecord {
            anchor: ExecutionAnchor {
                beacon_root: B256::repeat_byte(byte),
                beacon_slot: block_number,
                block_number,
                block_hash: B256::repeat_byte(byte.wrapping_add(1)),
                receipts_root: B256::repeat_byte(byte.wrapping_add(2)),
            },
            finalized: false,
            parent_beacon_root: parent,
        };

        store
            .append_anchors(vec![
                anchor(10, 0x10, None),
                anchor(11, 0x11, Some(B256::repeat_byte(0x10))),
                anchor(13, 0x13, Some(B256::repeat_byte(0x11))),
                anchor(14, 0x14, Some(B256::repeat_byte(0xee))),
            ])
            .unwrap();

        let coverage = store.anchor_coverage();
        assert_eq!(coverage.floor.map(|anchor| anchor.block_number), Some(10));
        assert_eq!(coverage.ceiling.map(|anchor| anchor.block_number), Some(14));
        assert_eq!(coverage.count, 4);
        assert_eq!(coverage.gap_count, 2);
    }

    #[test]
    fn bootstrap_status_recovers_checkpoint_slot_when_missing() {
        let temp = TempDir::new().unwrap();
        let bootstrap_slot = recent_test_slot(0);
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();

        assert_eq!(store.checkpoint().beacon_slot, None);
        store
            .record_verified_bootstrap(
                LightClientBootstrapStatus {
                    fork: logex_types::ConsensusDataFork::Deneb,
                    header: logex_types::LightClientHeaderSummary {
                        beacon_slot: bootstrap_slot,
                        execution: None,
                    },
                    current_sync_committee_pubkeys: 512,
                    current_sync_committee_branch_depth: 5,
                },
                RawRpcResponse {
                    context_bytes: None,
                    bytes: vec![1, 2, 3],
                },
                VerifiedLightClientStore {
                    checkpoint_root: B256::repeat_byte(0xaa),
                    bootstrap_slot,
                    current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                    next_sync_committee: None,
                    finalized_header: crate::light_client::VerifiedLightClientHeader {
                        fork: logex_types::ConsensusDataFork::Deneb,
                        beacon: crate::light_client::BeaconBlockHeaderSsz {
                            slot: bootstrap_slot,
                            proposer_index: 0,
                            parent_root: B256::ZERO,
                            state_root: B256::ZERO,
                            body_root: B256::ZERO,
                        },
                        execution: None,
                    },
                    optimistic_header: crate::light_client::VerifiedLightClientHeader {
                        fork: logex_types::ConsensusDataFork::Deneb,
                        beacon: crate::light_client::BeaconBlockHeaderSsz {
                            slot: bootstrap_slot,
                            proposer_index: 0,
                            parent_root: B256::ZERO,
                            state_root: B256::ZERO,
                            body_root: B256::ZERO,
                        },
                        execution: None,
                    },
                    best_valid_update: None,
                    previous_max_active_participants: 0,
                    current_max_active_participants: 0,
                },
            )
            .unwrap();

        assert_eq!(store.checkpoint().beacon_slot, Some(bootstrap_slot));
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.checkpoint().beacon_slot, Some(bootstrap_slot));
    }

    #[test]
    fn reopening_with_same_root_and_known_slot_enriches_persisted_checkpoint() {
        let temp = TempDir::new().unwrap();
        let root = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let known_slot = recent_test_slot(0);
        ConsensusStore::open(temp.path(), Some(root)).unwrap();

        let reopened =
            ConsensusStore::open(temp.path(), Some(&format!("{known_slot}@{root}"))).unwrap();
        assert_eq!(reopened.checkpoint().beacon_slot, Some(known_slot));

        let persisted = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(persisted.checkpoint().beacon_slot, Some(known_slot));
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

    #[test]
    fn verified_finality_update_persists_without_deadlocking() {
        let temp = TempDir::new().unwrap();
        let root = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let checkpoint_slot = recent_test_slot(0);
        let finality_slot = checkpoint_slot + 32;
        let signature_slot = finality_slot + 1;
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{checkpoint_slot}@{root}"))).unwrap();

        store
            .record_verified_bootstrap(
                LightClientBootstrapStatus {
                    fork: logex_types::ConsensusDataFork::Electra,
                    header: logex_types::LightClientHeaderSummary {
                        beacon_slot: checkpoint_slot,
                        execution: Some(logex_types::LightClientExecutionData {
                            block_number: 100,
                            block_hash: B256::repeat_byte(0x11),
                            receipts_root: B256::repeat_byte(0x12),
                        }),
                    },
                    current_sync_committee_pubkeys: 512,
                    current_sync_committee_branch_depth: 6,
                },
                RawRpcResponse {
                    context_bytes: Some([1, 2, 3, 4]),
                    bytes: vec![1, 2, 3],
                },
                VerifiedLightClientStore {
                    checkpoint_root: B256::repeat_byte(0xaa),
                    bootstrap_slot: checkpoint_slot,
                    current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                    next_sync_committee: None,
                    finalized_header: verified_header(checkpoint_slot, 0x10, 100),
                    optimistic_header: verified_header(checkpoint_slot, 0x10, 100),
                    best_valid_update: None,
                    previous_max_active_participants: 0,
                    current_max_active_participants: 0,
                },
            )
            .unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = store.record_verified_finality_update(
                LightClientFinalityUpdateStatus {
                    fork: logex_types::ConsensusDataFork::Electra,
                    attested_header: logex_types::LightClientHeaderSummary {
                        beacon_slot: finality_slot,
                        execution: Some(logex_types::LightClientExecutionData {
                            block_number: 101,
                            block_hash: B256::repeat_byte(0x21),
                            receipts_root: B256::repeat_byte(0x22),
                        }),
                    },
                    finalized_header: logex_types::LightClientHeaderSummary {
                        beacon_slot: finality_slot,
                        execution: Some(logex_types::LightClientExecutionData {
                            block_number: 101,
                            block_hash: B256::repeat_byte(0x21),
                            receipts_root: B256::repeat_byte(0x22),
                        }),
                    },
                    signature_slot,
                    sync_committee_participants: 509,
                    finality_branch_depth: 7,
                },
                RawRpcResponse {
                    context_bytes: Some([5, 6, 7, 8]),
                    bytes: vec![4, 5, 6],
                },
                VerifiedLightClientStore {
                    checkpoint_root: B256::repeat_byte(0xaa),
                    bootstrap_slot: checkpoint_slot,
                    current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                    next_sync_committee: None,
                    finalized_header: verified_header(finality_slot, 0x20, 101),
                    optimistic_header: verified_header(finality_slot, 0x20, 101),
                    best_valid_update: None,
                    previous_max_active_participants: 0,
                    current_max_active_participants: 0,
                },
            );
            done_tx.send(result).unwrap();
        });

        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("verified finality update should not deadlock")
            .unwrap();

        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(
            reopened
                .chain_anchors()
                .finalized_head
                .map(|anchor| anchor.block_number),
            Some(101)
        );
        assert_eq!(
            reopened
                .light_client_status()
                .finality_update
                .map(|status| status.finalized_header.beacon_slot),
            Some(finality_slot)
        );
    }

    #[test]
    fn verified_optimistic_update_persists_without_deadlocking() {
        let temp = TempDir::new().unwrap();
        let root = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let checkpoint_slot = recent_test_slot(0);
        let optimistic_slot = checkpoint_slot + 33;
        let signature_slot = optimistic_slot + 1;
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{checkpoint_slot}@{root}"))).unwrap();

        store
            .record_verified_bootstrap(
                LightClientBootstrapStatus {
                    fork: logex_types::ConsensusDataFork::Electra,
                    header: logex_types::LightClientHeaderSummary {
                        beacon_slot: checkpoint_slot,
                        execution: Some(logex_types::LightClientExecutionData {
                            block_number: 100,
                            block_hash: B256::repeat_byte(0x11),
                            receipts_root: B256::repeat_byte(0x12),
                        }),
                    },
                    current_sync_committee_pubkeys: 512,
                    current_sync_committee_branch_depth: 6,
                },
                RawRpcResponse {
                    context_bytes: Some([1, 2, 3, 4]),
                    bytes: vec![1, 2, 3],
                },
                VerifiedLightClientStore {
                    checkpoint_root: B256::repeat_byte(0xaa),
                    bootstrap_slot: checkpoint_slot,
                    current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                    next_sync_committee: None,
                    finalized_header: verified_header(checkpoint_slot, 0x10, 100),
                    optimistic_header: verified_header(checkpoint_slot, 0x10, 100),
                    best_valid_update: None,
                    previous_max_active_participants: 0,
                    current_max_active_participants: 0,
                },
            )
            .unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            let result = store.record_verified_optimistic_update(
                LightClientOptimisticUpdateStatus {
                    fork: logex_types::ConsensusDataFork::Electra,
                    attested_header: logex_types::LightClientHeaderSummary {
                        beacon_slot: optimistic_slot,
                        execution: Some(logex_types::LightClientExecutionData {
                            block_number: 102,
                            block_hash: B256::repeat_byte(0x31),
                            receipts_root: B256::repeat_byte(0x32),
                        }),
                    },
                    signature_slot,
                    sync_committee_participants: 509,
                },
                RawRpcResponse {
                    context_bytes: Some([9, 10, 11, 12]),
                    bytes: vec![7, 8, 9],
                },
                VerifiedLightClientStore {
                    checkpoint_root: B256::repeat_byte(0xaa),
                    bootstrap_slot: checkpoint_slot,
                    current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                    next_sync_committee: None,
                    finalized_header: verified_header(checkpoint_slot, 0x10, 100),
                    optimistic_header: verified_header(optimistic_slot, 0x30, 102),
                    best_valid_update: None,
                    previous_max_active_participants: 0,
                    current_max_active_participants: 0,
                },
            );
            done_tx.send(result).unwrap();
        });

        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("verified optimistic update should not deadlock")
            .unwrap();

        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(
            reopened
                .chain_anchors()
                .optimistic_head
                .map(|anchor| anchor.block_number),
            Some(102)
        );
        assert_eq!(
            reopened
                .light_client_status()
                .optimistic_update
                .map(|status| status.attested_header.beacon_slot),
            Some(optimistic_slot)
        );
    }

    #[test]
    fn verified_updates_by_range_payloads_persist_by_period() {
        let temp = TempDir::new().unwrap();
        let root = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let checkpoint_slot = recent_test_slot(0);
        let finalized_slot = checkpoint_slot + 32;
        let optimistic_slot = finalized_slot + 1;
        let signature_slot = optimistic_slot + 1;
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{checkpoint_slot}@{root}"))).unwrap();

        store
            .record_verified_bootstrap(
                LightClientBootstrapStatus {
                    fork: logex_types::ConsensusDataFork::Electra,
                    header: logex_types::LightClientHeaderSummary {
                        beacon_slot: checkpoint_slot,
                        execution: Some(logex_types::LightClientExecutionData {
                            block_number: 100,
                            block_hash: B256::repeat_byte(0x11),
                            receipts_root: B256::repeat_byte(0x12),
                        }),
                    },
                    current_sync_committee_pubkeys: 512,
                    current_sync_committee_branch_depth: 6,
                },
                RawRpcResponse {
                    context_bytes: Some([1, 2, 3, 4]),
                    bytes: vec![1, 2, 3],
                },
                VerifiedLightClientStore {
                    checkpoint_root: B256::repeat_byte(0xaa),
                    bootstrap_slot: checkpoint_slot,
                    current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                    next_sync_committee: None,
                    finalized_header: verified_header(checkpoint_slot, 0x10, 100),
                    optimistic_header: verified_header(checkpoint_slot, 0x10, 100),
                    best_valid_update: None,
                    previous_max_active_participants: 0,
                    current_max_active_participants: 0,
                },
            )
            .unwrap();

        store
            .record_verified_applied_update(
                AppliedLightClientUpdate {
                    store: VerifiedLightClientStore {
                        checkpoint_root: B256::repeat_byte(0xaa),
                        bootstrap_slot: checkpoint_slot,
                        current_sync_committee: crate::light_client::SyncCommitteeData::default(),
                        next_sync_committee: Some(
                            crate::light_client::SyncCommitteeData::default(),
                        ),
                        finalized_header: verified_header(finalized_slot, 0x20, 101),
                        optimistic_header: verified_header(optimistic_slot, 0x21, 102),
                        best_valid_update: None,
                        previous_max_active_participants: 0,
                        current_max_active_participants: 0,
                    },
                    optimistic_status: LightClientOptimisticUpdateStatus {
                        fork: logex_types::ConsensusDataFork::Electra,
                        attested_header: logex_types::LightClientHeaderSummary {
                            beacon_slot: optimistic_slot,
                            execution: Some(logex_types::LightClientExecutionData {
                                block_number: 102,
                                block_hash: B256::repeat_byte(0x31),
                                receipts_root: B256::repeat_byte(0x32),
                            }),
                        },
                        signature_slot,
                        sync_committee_participants: 509,
                    },
                    finality_status: Some(LightClientFinalityUpdateStatus {
                        fork: logex_types::ConsensusDataFork::Electra,
                        attested_header: logex_types::LightClientHeaderSummary {
                            beacon_slot: optimistic_slot,
                            execution: Some(logex_types::LightClientExecutionData {
                                block_number: 102,
                                block_hash: B256::repeat_byte(0x31),
                                receipts_root: B256::repeat_byte(0x32),
                            }),
                        },
                        finalized_header: logex_types::LightClientHeaderSummary {
                            beacon_slot: finalized_slot,
                            execution: Some(logex_types::LightClientExecutionData {
                                block_number: 101,
                                block_hash: B256::repeat_byte(0x21),
                                receipts_root: B256::repeat_byte(0x22),
                            }),
                        },
                        signature_slot,
                        sync_committee_participants: 509,
                        finality_branch_depth: 7,
                    }),
                },
                vec![
                    (
                        10,
                        RawRpcResponse {
                            context_bytes: Some([9, 9, 9, 9]),
                            bytes: vec![9, 9, 9],
                        },
                    ),
                    (
                        11,
                        RawRpcResponse {
                            context_bytes: Some([8, 8, 8, 8]),
                            bytes: vec![8, 8, 8],
                        },
                    ),
                ],
            )
            .unwrap();

        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        let payloads = reopened.light_client_payloads();
        assert_eq!(payloads.updates_by_period.len(), 2);
        assert_eq!(
            payloads
                .updates_by_period
                .get(&10)
                .map(|payload| payload.bytes.clone()),
            Some(vec![9, 9, 9])
        );
        assert_eq!(
            payloads
                .updates_by_period
                .get(&11)
                .map(|payload| payload.bytes.clone()),
            Some(vec![8, 8, 8])
        );
    }
}
