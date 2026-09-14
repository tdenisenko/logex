use std::collections::BTreeMap;
use std::fs;
use std::io::{self, BufReader, BufWriter, Write};
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
/// Mainnet reference age limit for opening the consensus store. The Electra
/// reference assumes at least 8,388,608 ETH of active balance; it is not a
/// universal lower bound on the state-derived weak-subjectivity period.
/// Node startup also applies its stricter 256-epoch checkpoint refresh policy.
pub const MAINNET_WEAK_SUBJECTIVITY_MAX_AGE_EPOCHS: u64 = 3_532;

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
    #[error("consensus storage stopped after a previous save failure: {0}")]
    StorageFailed(Arc<str>),
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
        "persisted consensus trusted slot {trusted_slot} at epoch {trusted_epoch} is stale at current epoch {current_epoch}; it exceeds the mainnet reference age limit of {max_epochs} epochs. Start with a recent --checkpoint in a fresh data directory."
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
    // Readers never wait for filesystem I/O. Writers serialize the entire
    // candidate -> durable replacement -> publication transaction.
    writer: Mutex<()>,
    storage_failure: tokio::sync::watch::Sender<Option<Arc<str>>>,
}

impl ConsensusStore {
    pub fn open(
        data_dir: impl AsRef<Path>,
        checkpoint: Option<&str>,
    ) -> Result<Self, ConsensusStateError> {
        let path = consensus_state_path(data_dir.as_ref());
        let (mut snapshot, mut needs_save) = match fs::File::open(&path) {
            Ok(file) => {
                let snapshot: ConsensusSnapshot = serde_json::from_reader(BufReader::new(file))
                    .map_err(|error| ConsensusStateError::ParseState {
                        path: path.clone(),
                        message: error.to_string(),
                    })?;
                let snapshot = restore_snapshot(snapshot).map_err(|message| {
                    ConsensusStateError::ParseState {
                        path: path.clone(),
                        message,
                    }
                })?;
                (snapshot, false)
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                // A dangling state-file link is an existing, unavailable state,
                // not permission to initialize a replacement database.
                if fs::symlink_metadata(&path).is_ok() {
                    return Err(ConsensusStateError::ReadState { path, source });
                }
                let checkpoint = checkpoint.ok_or(ConsensusStateError::MissingCheckpoint)?;
                (load_checkpoint_descriptor(checkpoint)?, true)
            }
            Err(source) => return Err(ConsensusStateError::ReadState { path, source }),
        };
        if !needs_save && let Some(checkpoint) = checkpoint {
            let requested = load_checkpoint_descriptor(checkpoint)?.checkpoint;
            needs_save = reconcile_checkpoint(&mut snapshot, requested)?;
        }
        ensure_snapshot_within_weak_subjectivity_period(&snapshot)?;
        // Only startup may initialize directories. An already opened store must
        // fail if its directory disappears instead of creating a replacement.
        if needs_save {
            let parent = path.parent().expect("consensus state path has a parent");
            create_synced_directory(parent).map_err(|source| {
                ConsensusStateError::PersistState {
                    path: path.clone(),
                    source,
                }
            })?;
        }
        let store = Self {
            path,
            inner: Arc::new(Mutex::new(snapshot)),
            writer: Mutex::new(()),
            storage_failure: tokio::sync::watch::channel(None).0,
        };
        if needs_save {
            store.persist()?;
        }
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
        self.update(|snapshot| {
            snapshot.ordered_anchors = normalize_anchor_records(anchors);
            recompute_snapshot_anchors(snapshot);
        })
    }

    pub fn append_anchors(&self, anchors: Vec<AnchorRecord>) -> Result<(), ConsensusStateError> {
        self.update(|snapshot| {
            snapshot.ordered_anchors.extend(anchors);
            let ordered = std::mem::take(&mut snapshot.ordered_anchors);
            snapshot.ordered_anchors = normalize_anchor_records(ordered);
            recompute_snapshot_anchors(snapshot);
        })
    }

    pub fn replace_anchor_range(
        &self,
        start_block: u64,
        end_block: u64,
        anchors: Vec<AnchorRecord>,
    ) -> Result<(), ConsensusStateError> {
        self.update(|snapshot| {
            snapshot.ordered_anchors.retain(|record| {
                record.anchor.block_number < start_block || record.anchor.block_number > end_block
            });
            snapshot.ordered_anchors.extend(anchors);
            let ordered = std::mem::take(&mut snapshot.ordered_anchors);
            snapshot.ordered_anchors = normalize_anchor_records(ordered);
            recompute_snapshot_anchors(snapshot);
        })
    }

    pub(crate) fn record_verified_bootstrap(
        &self,
        status: LightClientBootstrapStatus,
        payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        self.update(|snapshot| {
            snapshot.checkpoint.beacon_slot = Some(store.bootstrap_slot());
            snapshot.light_client.bootstrap = Some(status);
            snapshot.light_client.finality_update = None;
            snapshot.light_client.optimistic_update = None;
            snapshot.light_client_payloads.bootstrap = Some(payload);
            snapshot.light_client_payloads.finality_update = None;
            snapshot.light_client_payloads.optimistic_update = None;
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(snapshot);
        })
    }

    pub(crate) fn record_verified_finality_update(
        &self,
        status: LightClientFinalityUpdateStatus,
        payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        self.update(|snapshot| {
            snapshot.light_client.finality_update = Some(status);
            snapshot.light_client_payloads.finality_update = Some(payload);
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(snapshot);
        })
    }

    pub(crate) fn record_verified_optimistic_update(
        &self,
        status: LightClientOptimisticUpdateStatus,
        payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        self.update(|snapshot| {
            snapshot.light_client.optimistic_update = Some(status);
            snapshot.light_client_payloads.optimistic_update = Some(payload);
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(snapshot);
        })
    }

    pub(crate) fn record_verified_applied_update(
        &self,
        applied: AppliedLightClientUpdate,
        verified_updates_by_period: Vec<(u64, RawRpcResponse)>,
    ) -> Result<(), ConsensusStateError> {
        self.update(|snapshot| {
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
            apply_verified_store(snapshot);
        })
    }

    pub(crate) fn replace_verified_light_client_store(
        &self,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        self.update(|snapshot| {
            snapshot.verified_light_client_store = Some(store);
            apply_verified_store(snapshot);
        })
    }

    /// Subscribe to the first failed save. The error remains latched even when
    /// there were no subscribers at the time of failure; reopening is required.
    pub fn subscribe_storage_failure(&self) -> tokio::sync::watch::Receiver<Option<Arc<str>>> {
        self.storage_failure.subscribe()
    }

    pub fn persist(&self) -> Result<(), ConsensusStateError> {
        self.update(|_| {})
    }

    fn update(
        &self,
        mutate: impl FnOnce(&mut ConsensusSnapshot),
    ) -> Result<(), ConsensusStateError> {
        self.update_with_writer(mutate, write_snapshot)
    }

    fn update_with_writer(
        &self,
        mutate: impl FnOnce(&mut ConsensusSnapshot),
        write: impl FnOnce(&Path, &ConsensusSnapshot) -> io::Result<()>,
    ) -> Result<(), ConsensusStateError> {
        let _writer = self.writer.lock().unwrap();
        if let Some(message) = self.storage_failure.borrow().clone() {
            return Err(ConsensusStateError::StorageFailed(message));
        }
        let mut candidate = self.inner.lock().unwrap().clone();
        mutate(&mut candidate);
        if let Err(source) = write(&self.path, &candidate) {
            let error = ConsensusStateError::PersistState {
                path: self.path.clone(),
                source,
            };
            // The rename may have succeeded before a directory-sync failure.
            // Retrying from the old in-memory snapshot would be unsafe; only a
            // fresh open may reconcile the durable state after any write error.
            self.storage_failure
                .send_replace(Some(error.to_string().into()));
            return Err(error);
        }
        *self.inner.lock().unwrap() = candidate;
        Ok(())
    }

    pub fn state_path(&self) -> &Path {
        &self.path
    }
}

fn reconcile_checkpoint(
    snapshot: &mut ConsensusSnapshot,
    requested: WeakSubjectivityCheckpoint,
) -> Result<bool, ConsensusStateError> {
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

/// Durably replace one complete snapshot. Temp files are unique and owned, so
/// failed writes neither truncate the published file nor reuse another writer's
/// scratch path. Sync the file before rename and its directory before publishing.
fn write_snapshot(path: &Path, snapshot: &ConsensusSnapshot) -> io::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("consensus state has no parent"))?;
    let mut staged = tempfile::Builder::new()
        .prefix(".consensus-state-")
        .tempfile_in(parent)?;
    {
        let mut writer = BufWriter::new(staged.as_file_mut());
        serde_json::to_writer_pretty(&mut writer, snapshot).map_err(io::Error::other)?;
        writer.flush()?;
    }
    staged.as_file().sync_all()?;
    let _published = staged.persist(path).map_err(|error| error.error)?;
    fs::File::open(parent)?.sync_all()
}

fn create_synced_directory(path: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut ancestor = path;
    while !ancestor.try_exists()? {
        missing.push(ancestor);
        ancestor = ancestor
            .parent()
            .ok_or_else(|| io::Error::other("directory has no parent"))?;
        if ancestor.as_os_str().is_empty() {
            ancestor = Path::new(".");
        }
    }
    fs::create_dir_all(path)?;
    for directory in missing.into_iter().rev() {
        fs::File::open(directory)?.sync_all()?;
        let parent = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
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
        MAINNET_WEAK_SUBJECTIVITY_MAX_AGE_EPOCHS,
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

fn restore_snapshot(mut snapshot: ConsensusSnapshot) -> Result<ConsensusSnapshot, String> {
    if snapshot
        .ordered_anchors
        .windows(2)
        .any(|pair| pair[0].anchor.block_number >= pair[1].anchor.block_number)
    {
        return Err("persisted anchor block numbers must be strictly increasing and unique".into());
    }
    if let Some(store) = &snapshot.verified_light_client_store {
        store.validate_persisted_state(snapshot.checkpoint)?;
    } else if snapshot.light_client != ConsensusLightClientStatus::default()
        || snapshot.light_client_payloads != PersistedLightClientPayloads::default()
    {
        return Err("persisted light-client data is missing its verified store; use a fresh checkpoint in a new data directory".into());
    }
    // These summaries are derived from materialized anchors and the selected
    // verified headers. Never let serialized summaries override those sources.
    recompute_snapshot_anchors(&mut snapshot);
    Ok(snapshot)
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
    fn failed_anchor_save_does_not_publish_candidate() {
        for operation in ["append", "replace", "range"] {
            let temp = TempDir::new().unwrap();
            let store = ConsensusStore::open(
                temp.path(),
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .unwrap();
            let before = store.inner.lock().unwrap().clone();
            let original_bytes = fs::read(store.state_path()).unwrap();
            let saved_path = temp.path().join("saved-consensus-state.json");
            fs::rename(store.state_path(), &saved_path).unwrap();
            // An occupied destination makes the atomic replacement fail on both
            // supported platforms, without changing permissions or user files.
            fs::create_dir(store.state_path()).unwrap();
            let anchors = vec![AnchorRecord {
                anchor: ExecutionAnchor {
                    beacon_root: B256::repeat_byte(1),
                    beacon_slot: 1,
                    block_number: 10,
                    block_hash: B256::repeat_byte(2),
                    receipts_root: B256::repeat_byte(3),
                },
                finalized: true,
                parent_beacon_root: None,
            }];
            let result = match operation {
                "append" => store.append_anchors(anchors),
                "replace" => store.replace_anchors(anchors),
                "range" => store.replace_anchor_range(10, 10, anchors),
                _ => unreachable!(),
            };
            assert!(result.is_err(), "{operation}: save must fail");
            assert_eq!(fs::read(&saved_path).unwrap(), original_bytes);
            assert_eq!(
                *store.inner.lock().unwrap(),
                before,
                "{operation}: failed candidate must remain private"
            );
            fs::remove_dir(store.state_path()).unwrap();
            fs::rename(&saved_path, store.state_path()).unwrap();
            let failure = store.subscribe_storage_failure();
            assert!(
                failure.borrow().is_some(),
                "failure must survive without subscribers"
            );
            assert!(matches!(
                store.persist(),
                Err(ConsensusStateError::StorageFailed(_))
            ));
            assert_eq!(fs::read(store.state_path()).unwrap(), original_bytes);
            assert_eq!(
                fs::read_dir(store.state_path().parent().unwrap())
                    .unwrap()
                    .count(),
                1,
                "failed save must clean up its staging file"
            );
            let reopened = ConsensusStore::open(temp.path(), None).unwrap();
            assert_eq!(*reopened.inner.lock().unwrap(), before);
        }
    }

    fn test_anchor(block_number: u64) -> AnchorRecord {
        AnchorRecord {
            anchor: ExecutionAnchor {
                beacon_root: B256::repeat_byte(1),
                beacon_slot: block_number,
                block_number,
                block_hash: B256::repeat_byte(2),
                receipts_root: B256::repeat_byte(3),
            },
            finalized: false,
            parent_beacon_root: None,
        }
    }

    #[test]
    fn readers_keep_previous_snapshot_until_save_completes() {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap(),
        );
        let (entered_tx, entered_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let writer_store = Arc::clone(&store);
        let writer = std::thread::spawn(move || {
            writer_store.update_with_writer(
                |candidate| {
                    candidate.ordered_anchors.push(test_anchor(10));
                    recompute_snapshot_anchors(candidate);
                },
                |path, candidate| {
                    entered_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    write_snapshot(path, candidate)
                },
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (read_tx, read_rx) = mpsc::channel();
        let reader_store = Arc::clone(&store);
        let reader =
            std::thread::spawn(move || read_tx.send(reader_store.ordered_anchors()).unwrap());
        let observed = read_rx.recv_timeout(Duration::from_secs(2));
        resume_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        reader.join().unwrap();
        assert_eq!(observed.unwrap(), Vec::<AnchorRecord>::new());
        assert_eq!(store.ordered_anchors(), vec![test_anchor(10)]);
        assert_eq!(
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .ordered_anchors(),
            store.ordered_anchors()
        );
    }

    #[test]
    fn concurrent_anchor_appends_reopen_as_the_published_snapshot() {
        let temp = TempDir::new().unwrap();
        let store = Arc::new(
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap(),
        );
        let start = Arc::new(std::sync::Barrier::new(8));
        let writers: Vec<_> = (0..8)
            .map(|block| {
                let store = Arc::clone(&store);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    store.append_anchors(vec![test_anchor(block)])
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap().unwrap();
        }
        assert_eq!(
            store.ordered_anchors(),
            (0..8).map(test_anchor).collect::<Vec<_>>()
        );
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(
            *reopened.inner.lock().unwrap(),
            *store.inner.lock().unwrap()
        );
    }

    #[test]
    fn uncertain_save_requires_reopen_and_never_rolls_back_published_file() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        let result = store.update_with_writer(
            |candidate| {
                candidate.ordered_anchors.push(test_anchor(10));
                recompute_snapshot_anchors(candidate);
            },
            |path, candidate| {
                write_snapshot(path, candidate)?;
                // Model an error reported after replacement. The caller cannot
                // assume whether the new file became durable in this case.
                Err(io::Error::other("injected error after replacement"))
            },
        );
        assert!(result.is_err());
        assert!(store.ordered_anchors().is_empty());
        let disk_after_error = fs::read(store.state_path()).unwrap();
        assert!(matches!(
            store.append_anchors(vec![test_anchor(11)]),
            Err(ConsensusStateError::StorageFailed(_))
        ));
        assert_eq!(fs::read(store.state_path()).unwrap(), disk_after_error);
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.ordered_anchors(), vec![test_anchor(10)]);
        reopened.append_anchors(vec![test_anchor(11)]).unwrap();
        assert_eq!(
            reopened.ordered_anchors(),
            vec![test_anchor(10), test_anchor(11)]
        );
    }

    #[test]
    fn save_does_not_recreate_a_disappeared_directory() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        let directory = store.state_path().parent().unwrap();
        let original = fs::read(store.state_path()).unwrap();
        let moved_directory = temp.path().join("unavailable-consensus-directory");
        fs::rename(directory, &moved_directory).unwrap();
        assert!(store.append_anchors(vec![test_anchor(10)]).is_err());
        assert!(!directory.exists());
        assert!(store.ordered_anchors().is_empty());
        assert!(store.subscribe_storage_failure().borrow().is_some());
        assert_eq!(
            fs::read(moved_directory.join(CONSENSUS_STATE_FILE)).unwrap(),
            original
        );
    }

    #[test]
    fn reopening_rejects_payloads_without_verified_state() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        let mut snapshot = store.inner.lock().unwrap().clone();
        snapshot.light_client_payloads.bootstrap = Some(RawRpcResponse {
            context_bytes: None,
            bytes: vec![1, 2, 3],
        });
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        fs::write(store.state_path(), &bytes).unwrap();
        assert!(matches!(
            ConsensusStore::open(temp.path(), None),
            Err(ConsensusStateError::ParseState { .. })
        ));
        assert_eq!(fs::read(store.state_path()).unwrap(), bytes);
    }

    #[test]
    fn reopening_validates_verified_store_before_exposing_it() {
        let temp = TempDir::new().unwrap();
        let slot = recent_test_slot(0);
        let checkpoint = format!("{slot}@{:#x}", B256::repeat_byte(1));
        let store = ConsensusStore::open(temp.path(), Some(&checkpoint)).unwrap();
        let mut snapshot = store.inner.lock().unwrap().clone();
        snapshot.verified_light_client_store = Some(VerifiedLightClientStore {
            checkpoint_root: snapshot.checkpoint.beacon_root,
            bootstrap_slot: slot,
            current_sync_committee: crate::light_client::test_sync_committee(),
            next_sync_committee: None,
            finalized_header: verified_header(slot, 1, 100),
            optimistic_header: verified_header(slot, 1, 100),
            best_valid_update: None,
            previous_max_active_participants: 0,
            current_max_active_participants: 0,
        });
        fs::write(store.state_path(), serde_json::to_vec(&snapshot).unwrap()).unwrap();
        let valid = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(valid.trusted_beacon_slot(), Some(slot));
        assert_eq!(
            valid.chain_anchors().finalized_head.unwrap().block_number,
            100
        );

        snapshot
            .verified_light_client_store
            .as_mut()
            .unwrap()
            .previous_max_active_participants = 513;
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        fs::write(store.state_path(), &bytes).unwrap();
        let error = ConsensusStore::open(temp.path(), None).unwrap_err();
        assert!(matches!(error, ConsensusStateError::ParseState { .. }));
        assert!(error.to_string().contains("participant count"));
        assert_eq!(fs::read(store.state_path()).unwrap(), bytes);
    }

    #[cfg(unix)]
    #[test]
    fn dangling_state_link_is_not_reinitialized() {
        let temp = TempDir::new().unwrap();
        fs::create_dir(temp.path().join("cl")).unwrap();
        let missing_target = temp.path().join("missing-state.json");
        let path = consensus_state_path(temp.path());
        std::os::unix::fs::symlink(&missing_target, &path).unwrap();
        assert!(matches!(
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1)))),
            Err(ConsensusStateError::ReadState { .. })
        ));
        assert_eq!(fs::read_link(&path).unwrap(), missing_target);
        assert!(!missing_target.exists());
    }

    #[test]
    fn reopening_rejects_unordered_or_duplicate_anchor_records() {
        for blocks in [[11, 10], [10, 10]] {
            let temp = TempDir::new().unwrap();
            let store = ConsensusStore::open(
                temp.path(),
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .unwrap();
            let mut snapshot = store.inner.lock().unwrap().clone();
            snapshot.ordered_anchors = blocks
                .into_iter()
                .map(|block_number| AnchorRecord {
                    anchor: ExecutionAnchor {
                        beacon_root: B256::repeat_byte(1),
                        beacon_slot: block_number,
                        block_number,
                        block_hash: B256::repeat_byte(2),
                        receipts_root: B256::repeat_byte(3),
                    },
                    finalized: true,
                    parent_beacon_root: None,
                })
                .collect();
            fs::write(store.state_path(), serde_json::to_vec(&snapshot).unwrap()).unwrap();
            assert!(
                matches!(
                    ConsensusStore::open(temp.path(), None),
                    Err(ConsensusStateError::ParseState { .. })
                ),
                "invalid order {blocks:?} must not affect anchor lookup"
            );
        }
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
                current_sync_committee: crate::light_client::test_sync_committee(),
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
                    current_sync_committee: crate::light_client::test_sync_committee(),
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
                    current_sync_committee: crate::light_client::test_sync_committee(),
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
                    current_sync_committee: crate::light_client::test_sync_committee(),
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
                    current_sync_committee: crate::light_client::test_sync_committee(),
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
                    current_sync_committee: crate::light_client::test_sync_committee(),
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
                    current_sync_committee: crate::light_client::test_sync_committee(),
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
                        current_sync_committee: crate::light_client::test_sync_committee(),
                        next_sync_committee: Some(crate::light_client::test_sync_committee()),
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
