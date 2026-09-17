use std::collections::{BTreeMap, HashSet};
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
mod beacon_cache;
mod candidate_metadata;
mod chain;
mod discovery_key;
mod history_range_scan;
mod journal;
mod light_client;
mod network;
mod rpc;
mod rpc_memory;
mod snapshot_format;
mod state_delta;
use state_delta::StateDelta;

pub(crate) use beacon_block::{VerifiedBeaconBlock, decode_verified_beacon_block};
pub use chain::{
    CONSENSUS_HEAD_FRESHNESS_TOLERANCE_SLOTS, MAINNET_CONSENSUS_CHAIN_SPEC,
    optimistic_head_is_fresh_at, optimistic_head_lag_slots,
};
pub use discovery_key::load_or_create_discovery_key;
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
    prepare_consensus_network,
};
use rpc::RawRpcResponse;

const CONSENSUS_STATE_DIR: &str = "cl";
const CONSENSUS_STATE_FILE: &str = "CURRENT";
const CONSENSUS_STORE_DIR: &str = "consensus_state";
const LEGACY_BINARY_STATE_FILE: &str = "consensus_state.bin";
const LEGACY_CONSENSUS_STATE_FILE: &str = "consensus_state.json";
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
    #[error("invalid verified consensus payload metadata: {0}")]
    InvalidCachedPayload(String),
    #[error("invalid consensus mutation: {0}")]
    InvalidMutation(String),
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

/// Evidence for a reorg decision captured under a single consensus lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReorgAnchorSnapshot {
    /// The tracked tip matches consensus; no range allocation is needed.
    MatchingTip,
    /// The selected head is at/below the tracked tip or materialized terminal,
    /// and its complete lineage has not replaced the old materialization yet.
    PendingMaterialization,
    NoTrackedHeaders,
    Window {
        anchors: Vec<ExecutionAnchor>,
        first_anchor_block: Option<u64>,
        last_anchor_block: Option<u64>,
        selected_head_is_lower: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusSnapshot {
    pub checkpoint: WeakSubjectivityCheckpoint,
    #[serde(default)]
    pub anchors: ChainAnchors,
    #[serde(default)]
    pub ordered_anchors: Vec<AnchorRecord>,
    // Recomputed whenever materialized anchors change and on restore. Keep this
    // derived cache in the same publication transaction, never in the file.
    #[serde(skip)]
    anchor_gap_count: usize,
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
    // prepare delta -> durable frontier -> publication transaction.
    writer: Mutex<journal::Journal>,
    storage_failure: tokio::sync::watch::Sender<Option<Arc<str>>>,
}

impl ConsensusStore {
    pub fn open(
        data_dir: impl AsRef<Path>,
        checkpoint: Option<&str>,
    ) -> Result<Self, ConsensusStateError> {
        let path = consensus_state_path(data_dir.as_ref());
        let (mut snapshot, mut journal, fresh) = if state_entry_exists(path.parent().unwrap())? {
            let (journal, snapshot) = journal::Journal::open::<StateDelta>(
                &path,
                |snapshot| {
                    restore_snapshot(snapshot)
                        .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))
                },
                |snapshot, delta| delta.apply_replayed(snapshot),
            )
            .map_err(|source| match source.kind() {
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof => {
                    ConsensusStateError::ParseState {
                        path: path.clone(),
                        message: source.to_string(),
                    }
                }
                _ => ConsensusStateError::ReadState {
                    path: path.clone(),
                    source,
                },
            })?;
            let snapshot =
                restore_snapshot(snapshot).map_err(|message| ConsensusStateError::ParseState {
                    path: path.clone(),
                    message,
                })?;
            (snapshot, Some(journal), false)
        } else {
            for name in [LEGACY_BINARY_STATE_FILE, LEGACY_CONSENSUS_STATE_FILE] {
                let legacy = data_dir.as_ref().join(CONSENSUS_STATE_DIR).join(name);
                if state_entry_exists(&legacy)? {
                    return Err(ConsensusStateError::ParseState { path: legacy, message: "older consensus format requires a recent checkpoint in a fresh data directory; existing files were not changed".into() });
                }
            }
            (
                load_checkpoint_descriptor(
                    checkpoint.ok_or(ConsensusStateError::MissingCheckpoint)?,
                )?,
                None,
                true,
            )
        };
        let reconcile = if !fresh && let Some(checkpoint) = checkpoint {
            let requested = load_checkpoint_descriptor(checkpoint)?.checkpoint;
            reconcile_checkpoint(&mut snapshot, requested)?
        } else {
            false
        };
        // A durable journal may advance the trusted slot beyond its checkpoint.
        ensure_snapshot_within_weak_subjectivity_period(&snapshot)?;
        if fresh {
            let directory = path.parent().unwrap();
            let parent = directory.parent().unwrap();
            create_synced_directory(parent).map_err(|source| {
                ConsensusStateError::PersistState {
                    path: path.clone(),
                    source,
                }
            })?;
            let initialized = initialize_journal(&path, &snapshot).map_err(|source| {
                ConsensusStateError::PersistState {
                    path: path.clone(),
                    source,
                }
            })?;
            journal = Some(initialized);
        } else if reconcile {
            journal
                .as_mut()
                .unwrap()
                .checkpoint(&path, &snapshot)
                .map_err(|source| ConsensusStateError::PersistState {
                    path: path.clone(),
                    source,
                })?;
        }
        let store = Self {
            path,
            inner: Arc::new(Mutex::new(snapshot)),
            writer: Mutex::new(journal.expect("opened or initialized consensus journal")),
            storage_failure: tokio::sync::watch::channel(None).0,
        };
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

    /// Copy at most `limit` materialized records strictly after the given block.
    pub fn anchor_records_after(&self, block_number: u64, limit: usize) -> Vec<AnchorRecord> {
        if limit == 0 {
            return Vec::new();
        }
        let snapshot = self.inner.lock().unwrap();
        let anchors = &snapshot.ordered_anchors;
        let start = anchors.partition_point(|record| record.anchor.block_number <= block_number);
        let count = limit.min(anchors.len() - start);
        anchors[start..start + count].to_vec()
    }

    /// Check the tracked tip without allocating, otherwise copy a bounded
    /// inclusive anchor range and coverage under the same lock. Invalid or
    /// oversized windows are refused instead of truncating reorg evidence.
    pub fn reorg_anchor_snapshot(
        &self,
        first_block: u64,
        tracked_tip: Option<(u64, B256)>,
        max_records: usize,
    ) -> Option<ReorgAnchorSnapshot> {
        self.with_reorg_anchor_snapshot(first_block, tracked_tip, max_records, |snapshot| snapshot)
    }

    /// Admit a synchronous publication against the current materialized anchor.
    /// This serializes against in-memory selection replacement, not future
    /// finality or cross-file durability. The callback must not await or call
    /// consensus APIs, peer caches, subscribers, or progress callbacks.
    pub fn with_current_anchor<R>(
        &self,
        anchor: &ExecutionAnchor,
        publish: impl FnOnce() -> R,
    ) -> Option<R> {
        let snapshot = self.inner.lock().unwrap();
        if selected_lineage_pending(&snapshot, Some(anchor.block_number)) {
            return None;
        }
        let records = &snapshot.ordered_anchors;
        let index = records
            .binary_search_by_key(&anchor.block_number, |record| record.anchor.block_number)
            .ok()?;
        if records[index].anchor != *anchor {
            return None;
        }
        Some(publish())
    }

    /// Re-evaluate a bounded reorg decision and publish synchronously while its
    /// decisive snapshot stays current. Acquire execution storage before calling;
    /// the callback must not await or re-enter consensus APIs/other callbacks.
    pub fn with_reorg_anchor_snapshot<R>(
        &self,
        first_block: u64,
        tracked_tip: Option<(u64, B256)>,
        max_records: usize,
        publish: impl FnOnce(ReorgAnchorSnapshot) -> R,
    ) -> Option<R> {
        let snapshot = self.inner.lock().unwrap();
        Some(publish(build_reorg_anchor_snapshot(
            &snapshot,
            first_block,
            tracked_tip,
            max_records,
        )?))
    }

    pub fn anchor_coverage(&self) -> AnchorCoverage {
        let snapshot = self.inner.lock().unwrap();
        AnchorCoverage {
            floor: snapshot.ordered_anchors.first().map(|record| record.anchor),
            ceiling: snapshot.ordered_anchors.last().map(|record| record.anchor),
            count: snapshot.ordered_anchors.len(),
            gap_count: snapshot.anchor_gap_count,
        }
    }

    pub fn highest_anchor_block_from(&self, start_block: u64) -> Option<u64> {
        self.inner
            .lock()
            .unwrap()
            .ordered_anchors
            .last()
            .filter(|record| record.anchor.block_number >= start_block)
            .map(|record| record.anchor.block_number)
    }

    pub fn light_client_status(&self) -> ConsensusLightClientStatus {
        self.inner.lock().unwrap().light_client.clone()
    }

    pub(crate) fn light_client_bootstrap_payload(&self) -> Option<RawRpcResponse> {
        self.inner
            .lock()
            .unwrap()
            .light_client_payloads
            .bootstrap
            .clone()
    }

    pub(crate) fn light_client_finality_update_payload(&self) -> Option<RawRpcResponse> {
        self.inner
            .lock()
            .unwrap()
            .light_client_payloads
            .finality_update
            .clone()
    }

    pub(crate) fn light_client_optimistic_update_payload(&self) -> Option<RawRpcResponse> {
        self.inner
            .lock()
            .unwrap()
            .light_client_payloads
            .optimistic_update
            .clone()
    }

    pub(crate) fn light_client_update_payloads(
        &self,
        start_period: u64,
        count: u64,
    ) -> Vec<RawRpcResponse> {
        let snapshot = self.inner.lock().unwrap();
        cached_light_client_update_payloads_by_range(
            start_period,
            count,
            &snapshot.light_client_payloads.updates_by_period,
        )
    }

    pub(crate) fn light_client_store(&self) -> Option<VerifiedLightClientStore> {
        self.inner
            .lock()
            .unwrap()
            .verified_light_client_store
            .clone()
    }

    /// Inspect and synchronously reconcile cached metadata with one published
    /// snapshot, without cloning retained history or committee state. The
    /// callback must not await, perform I/O, or re-enter consensus store APIs.
    pub(crate) fn with_snapshot<R>(&self, inspect: impl FnOnce(&ConsensusSnapshot) -> R) -> R {
        inspect(&self.inner.lock().unwrap())
    }

    pub fn next_anchor_after(&self, block_number: u64) -> Option<ExecutionAnchor> {
        let snapshot = self.inner.lock().unwrap();
        let anchors = &snapshot.ordered_anchors;
        let index = anchors.partition_point(|record| record.anchor.block_number <= block_number);
        anchors.get(index).map(|record| record.anchor)
    }

    pub fn anchor_at(&self, block_number: u64) -> Option<ExecutionAnchor> {
        let snapshot = self.inner.lock().unwrap();
        let anchors = &snapshot.ordered_anchors;
        anchors
            .binary_search_by_key(&block_number, |record| record.anchor.block_number)
            .ok()
            .map(|index| anchors[index].anchor)
    }

    pub fn replace_anchors(&self, anchors: Vec<AnchorRecord>) -> Result<(), ConsensusStateError> {
        self.update_delta(StateDelta::ReplaceAnchors(normalize_anchor_records(
            anchors,
        )))
    }

    pub fn append_anchors(&self, anchors: Vec<AnchorRecord>) -> Result<(), ConsensusStateError> {
        self.update_delta(StateDelta::UpsertAnchors(normalize_anchor_records(anchors)))
    }

    pub fn replace_anchor_range(
        &self,
        start_block: u64,
        end_block: u64,
        anchors: Vec<AnchorRecord>,
    ) -> Result<(), ConsensusStateError> {
        self.replace_anchor_range_with_replaced_roots(start_block, end_block, anchors)
            .map(|_| ())
    }

    /// Return prior roots removed or changed at their execution height only
    /// after the range has been durably published. These are release candidates,
    /// not proof of lost ownership: the same root may remain at another height
    /// or be reintroduced by a later writer. Consumers must recheck ownership.
    pub(crate) fn replace_anchor_range_with_replaced_roots(
        &self,
        start_block: u64,
        end_block: u64,
        anchors: Vec<AnchorRecord>,
    ) -> Result<Vec<B256>, ConsensusStateError> {
        let anchors = normalize_anchor_records(anchors);
        let mut replaced = Vec::new();
        self.update_if(|current| {
            if start_block <= end_block
                && anchors
                    .iter()
                    .all(|r| (start_block..=end_block).contains(&r.anchor.block_number))
            {
                let start = current
                    .ordered_anchors
                    .partition_point(|r| r.anchor.block_number < start_block);
                let end = current
                    .ordered_anchors
                    .partition_point(|r| r.anchor.block_number <= end_block);
                if current.ordered_anchors[start..end] == anchors {
                    return Ok(None);
                }
            }
            replaced =
                replaced_anchor_roots(&current.ordered_anchors, start_block, end_block, &anchors);
            Ok(Some(StateDelta::ReplaceRange {
                start: start_block,
                end: end_block,
                anchors,
            }))
        })?;
        Ok(replaced)
    }

    pub(crate) fn record_verified_bootstrap(
        &self,
        status: LightClientBootstrapStatus,
        mut payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        light_client::normalize_cached_context(&mut payload, status.header.beacon_slot)
            .map_err(ConsensusStateError::InvalidCachedPayload)?;
        self.update_delta(StateDelta::Bootstrap {
            status,
            payload,
            store: Box::new(store),
        })
    }

    /// Record verified state independently of the best diagnostic summary and
    /// cached singleton response. Range updates can advance the summary alone.
    pub(crate) fn record_verified_finality_update(
        &self,
        status: LightClientFinalityUpdateStatus,
        mut payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<bool, ConsensusStateError> {
        light_client::normalize_cached_context(&mut payload, status.attested_header.beacon_slot)
            .map_err(ConsensusStateError::InvalidCachedPayload)?;
        self.update_if(|current| {
            let cached = current
                .light_client_payloads
                .finality_update
                .as_ref()
                .map(|payload| {
                    light_client::decode_finality_update(&payload.bytes).map_err(|error| {
                        ConsensusStateError::InvalidCachedPayload(format!(
                            "cached finality update: {error}"
                        ))
                    })
                })
                .transpose()?;
            let replace_payload = cached.as_ref().is_none_or(|previous| {
                finality_cache_priority(&status) > finality_cache_priority(previous)
            });
            let replace_summary =
                current
                    .light_client
                    .finality_update
                    .as_ref()
                    .is_none_or(|previous| {
                        finality_cache_priority(&status) > finality_cache_priority(previous)
                    });
            if !replace_payload
                && !replace_summary
                && current.verified_light_client_store.as_ref() == Some(&store)
            {
                return Ok(None);
            }
            Ok(Some(StateDelta::VerifiedPatch {
                store: Box::new(store),
                finality: replace_summary.then(|| Box::new(status)),
                finality_payload: replace_payload.then_some(payload),
                optimistic: None,
                optimistic_payload: None,
                periods: BTreeMap::new(),
            }))
        })
    }

    /// Record verified state independently of the best diagnostic summary and
    /// cached singleton response. Range updates can advance the summary alone.
    pub(crate) fn record_verified_optimistic_update(
        &self,
        status: LightClientOptimisticUpdateStatus,
        mut payload: RawRpcResponse,
        store: VerifiedLightClientStore,
    ) -> Result<bool, ConsensusStateError> {
        light_client::normalize_cached_context(&mut payload, status.attested_header.beacon_slot)
            .map_err(ConsensusStateError::InvalidCachedPayload)?;
        self.update_if(|current| {
            let cached = current
                .light_client_payloads
                .optimistic_update
                .as_ref()
                .map(|payload| {
                    light_client::decode_optimistic_update(&payload.bytes).map_err(|error| {
                        ConsensusStateError::InvalidCachedPayload(format!(
                            "cached optimistic update: {error}"
                        ))
                    })
                })
                .transpose()?;
            let replace_payload = cached.as_ref().is_none_or(|previous| {
                optimistic_cache_priority(&status) > optimistic_cache_priority(previous)
            });
            let replace_summary =
                current
                    .light_client
                    .optimistic_update
                    .as_ref()
                    .is_none_or(|previous| {
                        optimistic_cache_priority(&status) > optimistic_cache_priority(previous)
                    });
            if !replace_payload
                && !replace_summary
                && current.verified_light_client_store.as_ref() == Some(&store)
            {
                return Ok(None);
            }
            Ok(Some(StateDelta::VerifiedPatch {
                store: Box::new(store),
                optimistic: replace_summary.then_some(status),
                optimistic_payload: replace_payload.then_some(payload),
                finality: None,
                finality_payload: None,
                periods: BTreeMap::new(),
            }))
        })
    }

    pub(crate) fn record_verified_applied_update(
        &self,
        applied: AppliedLightClientUpdate,
        verified_updates_by_period: Vec<(u64, RawRpcResponse)>,
    ) -> Result<(), ConsensusStateError> {
        // Match insertion's last-record-wins behavior before comparing payloads.
        let verified_updates_by_period: BTreeMap<_, _> =
            verified_updates_by_period.into_iter().collect();
        self.update_if(move |current| {
            let optimistic_changed =
                current
                    .light_client
                    .optimistic_update
                    .as_ref()
                    .is_none_or(|previous| {
                        optimistic_cache_priority(&applied.optimistic_status)
                            > optimistic_cache_priority(previous)
                    });
            let finality_changed = applied.finality_status.as_ref().is_some_and(|status| {
                current
                    .light_client
                    .finality_update
                    .as_ref()
                    .is_none_or(|previous| {
                        finality_cache_priority(status) > finality_cache_priority(previous)
                    })
            });
            let payloads_changed = verified_updates_by_period.iter().any(|(period, payload)| {
                current.light_client_payloads.updates_by_period.get(period) != Some(payload)
            });
            if !optimistic_changed
                && !finality_changed
                && !payloads_changed
                && current.verified_light_client_store.as_ref() == Some(&applied.store)
            {
                return Ok(None);
            }
            let periods = verified_updates_by_period
                .into_iter()
                .filter(|(period, payload)| {
                    current.light_client_payloads.updates_by_period.get(period) != Some(payload)
                })
                .collect();
            Ok(Some(StateDelta::VerifiedPatch {
                store: Box::new(applied.store),
                optimistic: optimistic_changed.then_some(applied.optimistic_status),
                finality: if finality_changed {
                    applied.finality_status.map(Box::new)
                } else {
                    None
                },
                finality_payload: None,
                optimistic_payload: None,
                periods,
            }))
        })
        .map(|_| ())
    }

    pub(crate) fn replace_verified_light_client_store(
        &self,
        store: VerifiedLightClientStore,
    ) -> Result<(), ConsensusStateError> {
        self.update_delta(StateDelta::verified(store))
    }

    /// The first uncertain write stops further mutations until a fresh open.
    pub fn subscribe_storage_failure(&self) -> tokio::sync::watch::Receiver<Option<Arc<str>>> {
        self.storage_failure.subscribe()
    }

    pub fn persist(&self) -> Result<(), ConsensusStateError> {
        let mut journal = self.writer.lock().unwrap();
        self.ensure_storage_available()?;
        let candidate = self.inner.lock().unwrap().clone();
        journal
            .checkpoint(&self.path, &candidate)
            .map_err(|source| self.latch_failure(source))
    }

    fn ensure_storage_available(&self) -> Result<(), ConsensusStateError> {
        match self.storage_failure.borrow().clone() {
            Some(message) => Err(ConsensusStateError::StorageFailed(message)),
            None => Ok(()),
        }
    }

    fn latch_failure(&self, source: io::Error) -> ConsensusStateError {
        let error = ConsensusStateError::PersistState {
            path: self.path.clone(),
            source,
        };
        self.storage_failure
            .send_replace(Some(error.to_string().into()));
        error
    }

    fn update_delta(&self, delta: StateDelta) -> Result<(), ConsensusStateError> {
        self.update_if(|_| Ok(Some(delta))).map(|_| ())
    }

    fn update_if(
        &self,
        prepare: impl FnOnce(&ConsensusSnapshot) -> Result<Option<StateDelta>, ConsensusStateError>,
    ) -> Result<bool, ConsensusStateError> {
        self.update_if_with_writer(prepare, |journal, path, delta| journal.append(path, delta))
    }

    fn update_if_with_writer(
        &self,
        prepare: impl FnOnce(&ConsensusSnapshot) -> Result<Option<StateDelta>, ConsensusStateError>,
        write: impl FnOnce(&mut journal::Journal, &Path, &StateDelta) -> io::Result<()>,
    ) -> Result<bool, ConsensusStateError> {
        let mut journal = self.writer.lock().unwrap();
        self.ensure_storage_available()?;
        let Some(delta) = prepare(&self.inner.lock().unwrap())? else {
            return Ok(false);
        };
        delta
            .validate()
            .map_err(|error| ConsensusStateError::InvalidMutation(error.to_string()))?;
        if journal.checkpoint_due() {
            // History-sized copies occur only at periodic checkpoints. Readers
            // keep the old state while the candidate is serialized and synced.
            let mut candidate = self.inner.lock().unwrap().clone();
            delta
                .apply(&mut candidate)
                .map_err(|source| self.latch_failure(source))?;
            journal
                .checkpoint(&self.path, &candidate)
                .map_err(|source| self.latch_failure(source))?;
            *self.inner.lock().unwrap() = candidate;
        } else {
            write(&mut journal, &self.path, &delta).map_err(|source| self.latch_failure(source))?;
            // Prepared local deltas satisfy their structural invariants before
            // commit; replay additionally checks encoded anchor ordering.
            delta
                .apply(&mut self.inner.lock().unwrap())
                .map_err(|source| self.latch_failure(source))?;
        }
        Ok(true)
    }

    #[cfg(test)]
    fn update(
        &self,
        mutate: impl FnOnce(&mut ConsensusSnapshot),
    ) -> Result<(), ConsensusStateError> {
        let mut journal = self.writer.lock().unwrap();
        self.ensure_storage_available()?;
        let mut candidate = self.inner.lock().unwrap().clone();
        mutate(&mut candidate);
        journal
            .checkpoint(&self.path, &candidate)
            .map_err(|source| self.latch_failure(source))?;
        *self.inner.lock().unwrap() = candidate;
        Ok(())
    }

    pub fn state_path(&self) -> &Path {
        &self.path
    }
}

fn selected_optimistic_anchor(snapshot: &ConsensusSnapshot) -> Option<ExecutionAnchor> {
    // Only a verified store gives this cached summary selected-head authority.
    snapshot
        .verified_light_client_store
        .as_ref()
        .and(snapshot.anchors.optimistic_head)
}

fn selected_lineage_pending(snapshot: &ConsensusSnapshot, tip_block: Option<u64>) -> bool {
    let Some(selected) = selected_optimistic_anchor(snapshot) else {
        return false;
    };
    let materialized_tip = snapshot.ordered_anchors.last().map(|record| record.anchor);
    let requires_complete_lineage = tip_block.is_some_and(|last| selected.block_number <= last)
        || materialized_tip.is_some_and(|tip| tip.block_number >= selected.block_number);
    // Preserve ahead-of-materialization progress. This does not prove ancestry
    // under an as-yet-unmaterialized higher selected head.
    requires_complete_lineage && materialized_tip != Some(selected)
}

fn build_reorg_anchor_snapshot(
    snapshot: &ConsensusSnapshot,
    first_block: u64,
    tracked_tip: Option<(u64, B256)>,
    max_records: usize,
) -> Option<ReorgAnchorSnapshot> {
    if tracked_tip.is_some_and(|(last, _)| first_block > last) {
        return None;
    }
    let anchors = &snapshot.ordered_anchors;
    let selected = selected_optimistic_anchor(snapshot);
    if selected_lineage_pending(snapshot, tracked_tip.map(|(number, _)| number)) {
        return Some(ReorgAnchorSnapshot::PendingMaterialization);
    }
    let Some((last_block, tip_hash)) = tracked_tip else {
        return Some(ReorgAnchorSnapshot::NoTrackedHeaders);
    };
    if let Ok(index) =
        anchors.binary_search_by_key(&last_block, |record| record.anchor.block_number)
        && anchors[index].anchor.block_hash == tip_hash
    {
        return Some(ReorgAnchorSnapshot::MatchingTip);
    }
    let start = anchors.partition_point(|record| record.anchor.block_number < first_block);
    let end = anchors.partition_point(|record| record.anchor.block_number <= last_block);
    if end - start > max_records {
        return None;
    }
    Some(ReorgAnchorSnapshot::Window {
        anchors: anchors[start..end]
            .iter()
            .map(|record| record.anchor)
            .collect(),
        first_anchor_block: anchors.first().map(|record| record.anchor.block_number),
        last_anchor_block: anchors.last().map(|record| record.anchor.block_number),
        selected_head_is_lower: selected.is_some_and(|head| head.block_number < last_block),
    })
}

fn optimistic_cache_priority(status: &LightClientOptimisticUpdateStatus) -> (u64, usize) {
    (
        status.attested_header.beacon_slot,
        status.sync_committee_participants,
    )
}

fn finality_cache_priority(status: &LightClientFinalityUpdateStatus) -> (u64, bool, u64, usize) {
    (
        status.finalized_header.beacon_slot,
        // 342 is the minimum supermajority in the 512-member mainnet committee.
        status.sync_committee_participants >= 342,
        status.attested_header.beacon_slot,
        status.sync_committee_participants,
    )
}

fn cached_light_client_update_payloads_by_range(
    start_period: u64,
    count: u64,
    payloads: &BTreeMap<u64, RawRpcResponse>,
) -> Vec<RawRpcResponse> {
    if count == 0 {
        return Vec::new();
    }

    let last_period = start_period.saturating_add(count - 1);
    let mut range = payloads.range(start_period..=last_period);
    let Some((&first_period, first_payload)) = range.next() else {
        return Vec::new();
    };

    let mut responses = vec![first_payload.clone()];
    let mut expected_period = first_period.saturating_add(1);
    for (&period, payload) in range {
        if period != expected_period {
            break;
        }
        responses.push(payload.clone());
        expected_period = expected_period.saturating_add(1);
    }

    responses
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

#[cfg(test)]
thread_local! {
    static INITIALIZATION_FAILURE: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn initialization_phase(phase: &'static str) -> io::Result<()> {
    if INITIALIZATION_FAILURE.with(|failure| failure.get() == Some(phase)) {
        INITIALIZATION_FAILURE.with(|failure| failure.set(None));
        return Err(io::Error::other(format!(
            "injected initialization failure at {phase}"
        )));
    }
    Ok(())
}

/// Fully initialize an owned staging directory before publishing its name.
/// As with the existing store, callers own the data directory exclusively; node
/// startup enforces this with its directory lock. Concurrent initializers are
/// not supported by the rename transaction.
fn initialize_journal(path: &Path, snapshot: &ConsensusSnapshot) -> io::Result<journal::Journal> {
    let directory = path.parent().expect("CURRENT has a state directory");
    let parent = directory.parent().expect("state directory has a parent");
    let staged = logex_fs::StagedDirectory::new_in(parent, ".consensus-state-")?;
    let result = (|| {
        let journal =
            journal::Journal::create(&staged.path().join(CONSENSUS_STATE_FILE), snapshot)?;
        fs::File::open(staged.path())?.sync_all()?;
        #[cfg(test)]
        initialization_phase("staged")?;
        match fs::symlink_metadata(directory) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "consensus state directory already exists",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::rename(staged.path(), directory)?;
        #[cfg(test)]
        initialization_phase("renamed")?;
        fs::File::open(parent)?.sync_all()?;
        #[cfg(test)]
        initialization_phase("synced")?;
        Ok(journal)
    })();
    if result.is_err() {
        // Only our unpublished staging directory may be removed. If rename
        // succeeded before a sync error, the published directory stays intact.
        if let Err(error) = fs::remove_dir_all(staged.path())
            && error.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(%error, "retained failed consensus initialization staging directory");
        }
    }
    result
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
            anchor_gap_count: 0,
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
        anchor_gap_count: 0,
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

/// Capture changed prior roots without inspecting unaffected retained history.
/// Both inputs are sorted and unique by execution height. The range fallback
/// also upserts incoming records outside the deletion interval.
fn replaced_anchor_roots(
    previous: &[AnchorRecord],
    start: u64,
    end: u64,
    incoming: &[AnchorRecord],
) -> Vec<B256> {
    let mut roots = Vec::new();
    let mut seen = HashSet::new();
    let mut nominate = |root| {
        if seen.insert(root) {
            roots.push(root);
        }
    };
    if start <= end {
        let first = previous.partition_point(|record| record.anchor.block_number < start);
        let last = previous.partition_point(|record| record.anchor.block_number <= end);
        let mut next = 0;
        for old in &previous[first..last] {
            while next < incoming.len()
                && incoming[next].anchor.block_number < old.anchor.block_number
            {
                next += 1;
            }
            if incoming.get(next).is_none_or(|new| {
                new.anchor.block_number != old.anchor.block_number
                    || new.anchor.beacon_root != old.anchor.beacon_root
            }) {
                nominate(old.anchor.beacon_root);
            }
        }
    }
    for new in incoming {
        if start <= end && (start..=end).contains(&new.anchor.block_number) {
            continue;
        }
        if let Ok(index) = previous.binary_search_by_key(&new.anchor.block_number, |record| {
            record.anchor.block_number
        }) && previous[index].anchor.beacon_root != new.anchor.beacon_root
        {
            nominate(previous[index].anchor.beacon_root);
        }
    }
    roots
}

fn normalize_anchor_records(mut anchors: Vec<AnchorRecord>) -> Vec<AnchorRecord> {
    if anchors
        .windows(2)
        .all(|pair| pair[0].anchor.block_number < pair[1].anchor.block_number)
    {
        return anchors;
    }
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
    snapshot.anchor_gap_count = anchor_record_gap_count(&snapshot.ordered_anchors);
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
        light_client::validate_cached_light_client_payloads(
            &mut snapshot.light_client_payloads,
            snapshot.checkpoint,
        )?;
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

/// Path of the checksummed committed frontier. Its parent directory contains
/// the complete consensus state and must be archived or moved as one group.
pub fn consensus_state_path(data_dir: &Path) -> PathBuf {
    data_dir
        .join(CONSENSUS_STATE_DIR)
        .join(CONSENSUS_STORE_DIR)
        .join(CONSENSUS_STATE_FILE)
}

/// Include older-format entries so callers cannot treat an existing store as a
/// fresh directory. Presence does not imply validity; opening validates contents.
pub fn consensus_state_exists(data_dir: &Path) -> Result<bool, ConsensusStateError> {
    if state_entry_exists(consensus_state_path(data_dir).parent().unwrap())? {
        return Ok(true);
    }
    for name in [LEGACY_BINARY_STATE_FILE, LEGACY_CONSENSUS_STATE_FILE] {
        if state_entry_exists(&data_dir.join(CONSENSUS_STATE_DIR).join(name))? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn state_entry_exists(path: &Path) -> Result<bool, ConsensusStateError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(source) => Err(ConsensusStateError::ReadState {
            path: path.to_owned(),
            source,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn install_snapshot(path: &Path, snapshot: &ConsensusSnapshot) -> Vec<u8> {
        let parent = path.parent().unwrap();
        if parent.exists() {
            fs::remove_dir_all(parent).unwrap();
        }
        fs::create_dir_all(parent).unwrap();
        journal::Journal::create(path, snapshot).unwrap();
        fs::read(path).unwrap()
    }

    fn checkpoint_file(path: &Path) -> PathBuf {
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|entry| {
                entry.is_dir()
                    && entry
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("generation-")
            })
            .unwrap()
            .join("checkpoint.bin")
    }

    #[test]
    fn initialization_failure_preserves_published_group_and_cleans_only_staging() {
        for phase in ["staged", "renamed", "synced"] {
            let temp = TempDir::new().unwrap();
            INITIALIZATION_FAILURE.with(|failure| failure.set(Some(phase)));
            let result =
                ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))));
            assert!(result.is_err(), "{phase}");
            let path = consensus_state_path(temp.path());
            let entries = fs::read_dir(temp.path().join(CONSENSUS_STATE_DIR))
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>();
            assert!(
                !entries
                    .iter()
                    .any(|name| name.to_str().unwrap().starts_with(".consensus-state-"))
            );
            if phase == "staged" {
                assert!(!path.parent().unwrap().exists());
                assert!(!consensus_state_exists(temp.path()).unwrap());
            } else {
                assert!(path.is_file());
                let current = fs::read(&path).unwrap();
                assert_eq!(
                    ConsensusStore::open(temp.path(), None)
                        .unwrap()
                        .checkpoint()
                        .beacon_root,
                    B256::repeat_byte(1)
                );
                assert_eq!(fs::read(path).unwrap(), current);
            }
        }
    }

    #[test]
    fn invalid_prepared_delta_rejects_before_commit_without_latching_writer() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        let before = fs::read(store.state_path()).unwrap();
        assert!(matches!(
            store.update_delta(StateDelta::UpsertAnchors(vec![
                test_anchor(3),
                test_anchor(1)
            ])),
            Err(ConsensusStateError::InvalidMutation(_))
        ));
        assert_eq!(fs::read(store.state_path()).unwrap(), before);
        assert!(store.ordered_anchors().is_empty());
        assert!(store.subscribe_storage_failure().borrow().is_none());
        store.append_anchors(vec![test_anchor(2)]).unwrap();
        assert_eq!(
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .ordered_anchors(),
            vec![test_anchor(2)]
        );
    }

    #[test]
    fn replay_rejects_invalid_intermediate_delta_even_when_later_overwritten() {
        for malformed_store in [false, true] {
            let temp = TempDir::new().unwrap();
            let fixture =
                crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
            let (store, _) = initialized_fixture_store(&temp, &fixture);
            let good = StateDelta::VerifiedPatch {
                store: Box::new(fixture.store.clone()),
                finality: None,
                optimistic: None,
                finality_payload: fixture.payloads.finality_update.clone(),
                optimistic_payload: None,
                periods: BTreeMap::new(),
            };
            let mut bad = good.clone();
            if let StateDelta::VerifiedPatch {
                store,
                finality_payload,
                ..
            } = &mut bad
            {
                if malformed_store {
                    store.previous_max_active_participants = 513;
                } else {
                    finality_payload.as_mut().unwrap().bytes.truncate(3);
                }
            }
            // Encode otherwise valid checksummed frames directly to model local
            // corruption with recomputed integrity, not an ordinary verified API.
            let mut journal = store.writer.lock().unwrap();
            journal.append(store.state_path(), &bad).unwrap();
            journal.append(store.state_path(), &good).unwrap();
            drop(journal);
            let before = fs::read(store.state_path()).unwrap();
            assert!(matches!(
                ConsensusStore::open(temp.path(), None),
                Err(ConsensusStateError::ParseState { .. })
            ));
            assert_eq!(fs::read(store.state_path()).unwrap(), before);
        }
    }

    #[test]
    fn journal_anchor_deltas_match_full_snapshot_reference() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        let mut reference = store.inner.lock().unwrap().clone();
        let mut linked = test_anchor(3);
        linked.finalized = true;
        linked.parent_beacon_root = Some(test_anchor(2).anchor.beacon_root);
        let operations = vec![
            StateDelta::ReplaceAnchors(vec![
                test_anchor(1),
                test_anchor(2),
                linked,
                test_anchor(8),
            ]),
            StateDelta::UpsertAnchors(vec![test_anchor(0), test_anchor(2), test_anchor(9)]),
            StateDelta::ReplaceRange {
                start: 2,
                end: 3,
                anchors: vec![],
            },
            StateDelta::ReplaceRange {
                start: 5,
                end: 1,
                anchors: vec![linked],
            },
            StateDelta::ReplaceRange {
                start: 1,
                end: 2,
                anchors: vec![test_anchor(0), test_anchor(7)],
            },
            StateDelta::UpsertAnchors(vec![test_anchor(u64::MAX)]),
            StateDelta::ReplaceRange {
                start: 0,
                end: u64::MAX,
                anchors: vec![],
            },
            StateDelta::UpsertAnchors(vec![linked]),
        ];
        for delta in operations {
            match &delta {
                StateDelta::ReplaceAnchors(rows) => reference.ordered_anchors = rows.clone(),
                StateDelta::UpsertAnchors(rows) => {
                    reference.ordered_anchors.extend_from_slice(rows)
                }
                StateDelta::ReplaceRange {
                    start,
                    end,
                    anchors,
                } => {
                    reference
                        .ordered_anchors
                        .retain(|r| r.anchor.block_number < *start || r.anchor.block_number > *end);
                    reference.ordered_anchors.extend_from_slice(anchors);
                }
                _ => unreachable!(),
            }
            reference.ordered_anchors = normalize_anchor_records(reference.ordered_anchors);
            recompute_snapshot_anchors(&mut reference);
            store.update_delta(delta).unwrap();
            assert_eq!(*store.inner.lock().unwrap(), reference);
            assert_eq!(
                *ConsensusStore::open(temp.path(), None)
                    .unwrap()
                    .inner
                    .lock()
                    .unwrap(),
                reference
            );
        }
        store.persist().unwrap();
        assert_eq!(
            *ConsensusStore::open(temp.path(), None)
                .unwrap()
                .inner
                .lock()
                .unwrap(),
            reference
        );
    }

    #[test]
    fn journal_periodic_checkpoint_keeps_mutation_and_reader_semantics() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        store
            .append_anchors(vec![test_anchor(1), test_anchor(3)])
            .unwrap();
        store.writer.lock().unwrap().force_checkpoint_due_for_test();
        store.append_anchors(vec![test_anchor(4)]).unwrap();
        assert_eq!(
            store.ordered_anchors(),
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .ordered_anchors()
        );
        // The next ordinary delta must replay against a nonempty checkpoint's
        // reconstructed gap cache, not the skipped serialized default zero.
        store
            .replace_anchor_range(1, 3, vec![test_anchor(2)])
            .unwrap();
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(
            *store.inner.lock().unwrap(),
            *reopened.inner.lock().unwrap()
        );
        store.writer.lock().unwrap().force_checkpoint_due_for_test();
        let before = store.inner.lock().unwrap().clone();
        let saved = temp.path().join("held-current");
        fs::rename(store.state_path(), &saved).unwrap();
        assert!(store.append_anchors(vec![test_anchor(5)]).is_err());
        assert_eq!(*store.inner.lock().unwrap(), before);
        assert!(store.subscribe_storage_failure().borrow().is_some());
        fs::rename(saved, store.state_path()).unwrap();
        assert_eq!(
            *ConsensusStore::open(temp.path(), None)
                .unwrap()
                .inner
                .lock()
                .unwrap(),
            before
        );
    }

    #[test]
    fn journal_single_anchor_record_does_not_serialize_old_history() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        store
            .replace_anchors((100..116).map(test_anchor).collect())
            .unwrap();
        store.persist().unwrap();
        let checkpoint = checkpoint_file(store.state_path());
        let journal = checkpoint.parent().unwrap().join("journal.bin");
        let before = fs::metadata(&journal).unwrap().len();
        store.append_anchors(vec![test_anchor(200)]).unwrap();
        let bytes = fs::read(&journal).unwrap();
        let delta_bytes = &bytes[before as usize..];
        let text = String::from_utf8_lossy(delta_bytes);
        assert!(text.contains("UpsertAnchors"));
        assert!(text.contains("\"block_number\":200"));
        assert!(!text.contains("\"block_number\":100"));
        let mut legacy = io::Cursor::new(Vec::new());
        // This retained original serializer demonstrates the previous operation's
        // full snapshot content on the same finite logical state, without timing.
        snapshot_format::write(&mut legacy, &store.inner.lock().unwrap()).unwrap();
        assert!(String::from_utf8_lossy(legacy.get_ref()).contains("\"block_number\": 100"));
        assert!(delta_bytes.len() < legacy.get_ref().len());
        eprintln!(
            "finite persistence witness: original full snapshot {} bytes; new one-anchor frame {} bytes",
            legacy.get_ref().len(),
            delta_bytes.len()
        );
        assert_eq!(
            store.ordered_anchors(),
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .ordered_anchors()
        );
    }

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

    fn recent_cache_fixture_slot() -> u64 {
        (recent_test_slot(0) / 8192) * 8192 + 16
    }

    #[test]
    fn gossip_cache_recording_supplies_its_attested_context() {
        let fixture =
            crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some(&format!(
                "{}@{:#x}",
                fixture.checkpoint.beacon_slot.unwrap(),
                fixture.checkpoint.beacon_root
            )),
        )
        .unwrap();
        let status = fixture.status.optimistic_update.unwrap();
        let expected = MAINNET_CONSENSUS_CHAIN_SPEC
            .fork_digest_for_epoch(status.attested_header.beacon_slot / 32);
        let payload = fixture.payloads.optimistic_update.unwrap();
        assert_eq!(payload.context_bytes, None);
        let expected_bytes = payload.bytes.clone();
        store
            .record_verified_optimistic_update(status, payload, fixture.store)
            .unwrap();
        let cached = store.light_client_optimistic_update_payload().unwrap();
        assert_eq!(cached.bytes, expected_bytes);
        assert_eq!(cached.context_bytes, Some(expected));
        assert_eq!(
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .light_client_optimistic_update_payload(),
            Some(cached)
        );
    }

    #[test]
    fn incorrect_cached_context_is_rejected_before_persistence() {
        let fixture =
            crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some(&format!(
                "{}@{:#x}",
                fixture.checkpoint.beacon_slot.unwrap(),
                fixture.checkpoint.beacon_root
            )),
        )
        .unwrap();
        let status = fixture.status.optimistic_update.unwrap();
        let mut payload = fixture.payloads.optimistic_update.unwrap();
        let original = fs::read(store.state_path()).unwrap();
        let before = store.inner.lock().unwrap().clone();
        let mut context = MAINNET_CONSENSUS_CHAIN_SPEC
            .fork_digest_for_epoch(status.attested_header.beacon_slot / 32);
        context[0] ^= 1;
        payload.context_bytes = Some(context);
        assert!(matches!(
            store.record_verified_optimistic_update(
                status.clone(),
                payload.clone(),
                fixture.store.clone()
            ),
            Err(ConsensusStateError::InvalidCachedPayload(_))
        ));
        assert_eq!(*store.inner.lock().unwrap(), before);
        assert_eq!(fs::read(store.state_path()).unwrap(), original);
        assert!(store.subscribe_storage_failure().borrow().is_none());
        // Invalid input is different from a failed filesystem save: a corrected
        // valid payload can still be recorded on the same store instance.
        payload.context_bytes = None;
        store
            .record_verified_optimistic_update(status, payload, fixture.store)
            .unwrap();
        assert!(
            store
                .light_client_optimistic_update_payload()
                .unwrap()
                .context_bytes
                .is_some()
        );
    }

    #[test]
    fn reopening_rejects_invalid_cached_payload_before_serving() {
        let fixture =
            crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
        let temp = TempDir::new().unwrap();
        let path = consensus_state_path(temp.path());
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        let snapshot = ConsensusSnapshot {
            checkpoint: fixture.checkpoint,
            anchors: ChainAnchors::default(),
            ordered_anchors: Vec::new(),
            anchor_gap_count: 0,
            light_client: fixture.status,
            light_client_payloads: fixture.payloads,
            verified_light_client_store: Some(fixture.store),
        };
        install_snapshot(&path, &snapshot);
        assert!(ConsensusStore::open(temp.path(), None).is_ok());
        for family in 0..4 {
            let mut malformed = snapshot.clone();
            let payload = match family {
                0 => malformed.light_client_payloads.bootstrap.as_mut().unwrap(),
                1 => malformed
                    .light_client_payloads
                    .finality_update
                    .as_mut()
                    .unwrap(),
                2 => malformed
                    .light_client_payloads
                    .optimistic_update
                    .as_mut()
                    .unwrap(),
                3 => malformed
                    .light_client_payloads
                    .updates_by_period
                    .values_mut()
                    .next()
                    .unwrap(),
                _ => unreachable!(),
            };
            payload.bytes.truncate(3);
            let bytes = install_snapshot(&path, &malformed);
            assert!(
                matches!(
                    ConsensusStore::open(temp.path(), None),
                    Err(ConsensusStateError::ParseState { .. })
                ),
                "family {family} must be rejected"
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
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
                2,
                "failed save must retain its generation without a staging CURRENT"
            );
            let reopened = ConsensusStore::open(temp.path(), None).unwrap();
            assert_eq!(*reopened.inner.lock().unwrap(), before);
        }
    }

    #[test]
    fn snapshot_integrity_rejects_changed_root_bytes_before_reopen() {
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        store.append_anchors(vec![test_anchor(10)]).unwrap();
        store.persist().unwrap();
        let path = checkpoint_file(store.state_path());
        drop(store);
        let mut bytes = fs::read(&path).unwrap();
        let prefix = b"\"receipts_root\": \"0x";
        let records = bytes
            .windows(b"\"ordered_anchors\"".len())
            .position(|part| part == b"\"ordered_anchors\"")
            .unwrap();
        let digit = records
            + bytes[records..]
                .windows(prefix.len())
                .position(|part| part == prefix)
                .unwrap()
            + prefix.len();
        // A same-length hexadecimal edit remains valid JSON and valid metadata.
        bytes[digit] = if bytes[digit] == b'a' { b'b' } else { b'a' };
        fs::write(&path, &bytes).unwrap();
        let error = ConsensusStore::open(temp.path(), None).unwrap_err();
        assert!(matches!(error, ConsensusStateError::ParseState { .. }));
        assert!(error.to_string().contains("checksum"));
        assert_eq!(fs::read(&path).unwrap(), bytes);
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

    #[cfg(unix)]
    #[test]
    fn retained_anchors_unchanged_range_keeps_the_published_file() {
        use std::os::unix::fs::MetadataExt;

        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let anchors = vec![test_anchor(2), test_anchor(4), test_anchor(6)];
        store.replace_anchors(anchors.clone()).unwrap();
        // Keep the original inode alive so a replacement cannot reuse it.
        let published = fs::File::open(store.state_path()).unwrap();
        let original = published.metadata().unwrap();
        store.replace_anchor_range(2, 6, anchors.clone()).unwrap();
        store
            .replace_anchor_range(2, 6, anchors.iter().rev().copied().collect())
            .unwrap();
        store.replace_anchor_range(7, u64::MAX, vec![]).unwrap();
        let unchanged = fs::metadata(store.state_path()).unwrap();
        assert_eq!(
            (unchanged.dev(), unchanged.ino()),
            (original.dev(), original.ino())
        );
        assert_eq!(
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .ordered_anchors(),
            anchors
        );

        let mut changed = test_anchor(4);
        changed.finalized = !changed.finalized;
        store.replace_anchor_range(4, 4, vec![changed]).unwrap();
        let replaced = fs::metadata(store.state_path()).unwrap();
        assert_ne!(
            (replaced.dev(), replaced.ino()),
            (original.dev(), original.ino())
        );
        assert_eq!(
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .ordered_anchors()[1],
            changed
        );

        store
            .storage_failure
            .send_replace(Some("previous save failed".into()));
        assert!(matches!(
            store.replace_anchor_range(4, 4, vec![changed]),
            Err(ConsensusStateError::StorageFailed(_))
        ));
    }

    #[test]
    fn retained_anchors_replacements_match_last_record_wins_reference() {
        let base = vec![
            test_anchor(0),
            test_anchor(3),
            test_anchor(4),
            test_anchor(9),
            test_anchor(u64::MAX),
        ];
        let mut changed = test_anchor(4);
        changed.parent_beacon_root = Some(B256::repeat_byte(7));
        let cases = [
            (3, 9, vec![test_anchor(9), test_anchor(3), changed]),
            (3, 9, vec![]),
            (1, 2, vec![test_anchor(2)]),
            (0, u64::MAX, vec![]),
            (4, 4, vec![test_anchor(4), changed]),
            (3, 4, vec![test_anchor(12), test_anchor(9), changed]),
            (9, 3, vec![changed]),
            (u64::MAX, u64::MAX, vec![test_anchor(u64::MAX)]),
        ];
        for (start, end, incoming) in cases {
            let mut combined = base
                .iter()
                .copied()
                .filter(|r| r.anchor.block_number < start || r.anchor.block_number > end)
                .collect::<Vec<_>>();
            combined.extend(&incoming);
            let mut expected = combined.iter().rev().copied().fold(
                Vec::<AnchorRecord>::new(),
                |mut result, record| {
                    if !result
                        .iter()
                        .any(|old| old.anchor.block_number == record.anchor.block_number)
                    {
                        result.push(record);
                    }
                    result
                },
            );
            expected.sort_by_key(|r| r.anchor.block_number);
            let temp = TempDir::new().unwrap();
            let store = ConsensusStore::open(
                temp.path(),
                Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .unwrap();
            store.replace_anchors(base.clone()).unwrap();
            store.replace_anchor_range(start, end, incoming).unwrap();
            assert_eq!(store.ordered_anchors(), expected);
            assert_eq!(
                ConsensusStore::open(temp.path(), None)
                    .unwrap()
                    .ordered_anchors(),
                expected
            );
        }
    }

    #[test]
    fn retained_anchors_lookups_match_scan_at_gaps_and_integer_bounds() {
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        for anchors in [
            vec![],
            vec![
                test_anchor(0),
                test_anchor(3),
                test_anchor(8),
                test_anchor(u64::MAX),
            ],
        ] {
            store.replace_anchors(anchors.clone()).unwrap();
            for block in [0, 1, 2, 3, 4, 7, 8, 9, u64::MAX - 1, u64::MAX] {
                assert_eq!(
                    store.anchor_at(block),
                    anchors
                        .iter()
                        .find(|r| r.anchor.block_number == block)
                        .map(|r| r.anchor)
                );
                assert_eq!(
                    store.next_anchor_after(block),
                    anchors
                        .iter()
                        .find(|r| r.anchor.block_number > block)
                        .map(|r| r.anchor)
                );
                assert_eq!(
                    store.highest_anchor_block_from(block),
                    anchors
                        .iter()
                        .rev()
                        .find(|r| r.anchor.block_number >= block)
                        .map(|r| r.anchor.block_number)
                );
            }
        }
    }

    #[test]
    fn selected_lower_head_must_not_accept_stale_materialized_tip() {
        let slot = recent_cache_fixture_slot();
        let fixture = light_client::test_cached_light_client_fixture(slot);
        let temp = tempfile::tempdir().unwrap();
        let consensus = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let old = vec![test_anchor(100), test_anchor(101)];
        consensus.replace_anchors(old.clone()).unwrap();
        // A constructed verified-store state isolates snapshot admission. Signed
        // lower-height update acceptance is covered separately in light_client.
        let mut selected = fixture.store;
        selected.optimistic_header.beacon.slot = slot + 100;
        selected.optimistic_header.execution = Some(light_client::VerifiedExecutionPayloadHeader {
            block_number: 100,
            block_hash: B256::repeat_byte(0xef),
            receipts_root: B256::ZERO,
        });
        {
            let mut snapshot = consensus.inner.lock().unwrap();
            snapshot.verified_light_client_store = Some(selected);
            apply_verified_store(&mut snapshot);
        }
        assert!(matches!(
            consensus
                .reorg_anchor_snapshot(100, Some((101, old[1].anchor.block_hash)), 2)
                .unwrap(),
            ReorgAnchorSnapshot::PendingMaterialization
        ));
    }

    #[test]
    fn selected_head_snapshot_waits_for_exact_lineage_without_blocking_ahead_progress() {
        let slot = recent_cache_fixture_slot();
        let fixture = light_client::test_cached_light_client_fixture(slot);
        let temp = tempfile::tempdir().unwrap();
        let consensus = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let select = |number, hash| {
            let mut store = fixture.store.clone();
            store.optimistic_header.beacon.slot = slot + 100;
            store.optimistic_header.execution =
                Some(light_client::VerifiedExecutionPayloadHeader {
                    block_number: number,
                    block_hash: hash,
                    receipts_root: B256::ZERO,
                });
            let head = store.optimistic_anchor().unwrap();
            {
                let mut snapshot = consensus.inner.lock().unwrap();
                snapshot.verified_light_client_store = Some(store);
                apply_verified_store(&mut snapshot);
            }
            head
        };
        let selected = select(100, B256::repeat_byte(0xef));
        let old = vec![test_anchor(99), test_anchor(100), test_anchor(101)];
        consensus.replace_anchors(old.clone()).unwrap();
        for tip in [
            None,
            Some((99, old[0].anchor.block_hash)),
            Some((100, selected.block_hash)),
            Some((101, old[2].anchor.block_hash)),
        ] {
            assert_eq!(
                consensus.reorg_anchor_snapshot(99, tip, 3),
                Some(ReorgAnchorSnapshot::PendingMaterialization)
            );
        }
        // Matching execution number/hash alone cannot prove selected ancestry.
        let record = AnchorRecord {
            anchor: selected,
            finalized: false,
            parent_beacon_root: None,
        };
        let mut wrong_root = record;
        wrong_root.anchor.beacon_root = B256::repeat_byte(0xee);
        consensus.replace_anchors(vec![old[0], wrong_root]).unwrap();
        for tip in [
            None,
            Some((100, selected.block_hash)),
            Some((101, old[2].anchor.block_hash)),
        ] {
            assert_eq!(
                consensus.reorg_anchor_snapshot(99, tip, 3),
                Some(ReorgAnchorSnapshot::PendingMaterialization)
            );
        }
        consensus.replace_anchors(vec![old[0], record]).unwrap();
        assert!(matches!(
            consensus.reorg_anchor_snapshot(99, Some((101, old[2].anchor.block_hash)), 3),
            Some(ReorgAnchorSnapshot::Window {
                selected_head_is_lower: true,
                ..
            })
        ));
        assert_eq!(
            consensus.reorg_anchor_snapshot(99, Some((100, selected.block_hash)), 0),
            Some(ReorgAnchorSnapshot::MatchingTip)
        );
        assert_eq!(
            consensus.reorg_anchor_snapshot(0, None, 0),
            Some(ReorgAnchorSnapshot::NoTrackedHeaders)
        );
        // A same-height conflict also waits until its complete selected terminal.
        consensus.replace_anchors(old[..2].to_vec()).unwrap();
        assert_eq!(
            consensus.reorg_anchor_snapshot(99, Some((100, old[1].anchor.block_hash)), 2),
            Some(ReorgAnchorSnapshot::PendingMaterialization)
        );
        consensus.replace_anchors(vec![old[0], record]).unwrap();
        assert!(matches!(
            consensus.reorg_anchor_snapshot(99, Some((100, old[1].anchor.block_hash)), 2),
            Some(ReorgAnchorSnapshot::Window {
                selected_head_is_lower: false,
                ..
            })
        ));
        // Ordinary newer selected heads may lag in materialization; a partial
        // authenticated gap still uses the existing bounded sparse scan.
        select(105, B256::repeat_byte(0xf5));
        consensus.replace_anchors(old[..2].to_vec()).unwrap();
        assert_eq!(
            consensus.reorg_anchor_snapshot(99, Some((100, old[1].anchor.block_hash)), 0),
            Some(ReorgAnchorSnapshot::MatchingTip)
        );
        assert!(matches!(
            consensus.reorg_anchor_snapshot(99, Some((101, old[2].anchor.block_hash)), 3),
            Some(ReorgAnchorSnapshot::Window {
                selected_head_is_lower: false,
                ..
            })
        ));
        assert_eq!(
            consensus.reorg_anchor_snapshot(0, None, 0),
            Some(ReorgAnchorSnapshot::NoTrackedHeaders)
        );
        consensus.inner.lock().unwrap().verified_light_client_store = None;
        assert_eq!(
            consensus.reorg_anchor_snapshot(99, Some((100, old[1].anchor.block_hash)), 0),
            Some(ReorgAnchorSnapshot::MatchingTip)
        );
    }

    #[test]
    fn reorg_anchor_snapshot_bounds_windows_and_keeps_matching_tip_allocation_free() {
        let temp = tempfile::tempdir().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let mismatch = B256::repeat_byte(0xee);
        assert!(
            store
                .reorg_anchor_snapshot(1, Some((0, mismatch)), 10)
                .is_none()
        );
        let window = |first, last, limit| match store
            .reorg_anchor_snapshot(first, Some((last, mismatch)), limit)
            .unwrap()
        {
            ReorgAnchorSnapshot::Window {
                anchors,
                first_anchor_block,
                last_anchor_block,
                ..
            } => (anchors, first_anchor_block, last_anchor_block),
            _ => panic!("unexpected snapshot result"),
        };
        assert!(window(0, u64::MAX, 0).0.is_empty());
        let anchors = vec![
            test_anchor(0),
            test_anchor(3),
            test_anchor(8),
            test_anchor(u64::MAX),
        ];
        store.replace_anchors(anchors.clone()).unwrap();
        // Zero window capacity still returns the allocation-free variant when
        // the tip matches. No range data is copied or retained in that variant.
        assert_eq!(
            store.reorg_anchor_snapshot(0, Some((u64::MAX, anchors[3].anchor.block_hash)), 0),
            Some(ReorgAnchorSnapshot::MatchingTip)
        );
        assert!(
            store
                .reorg_anchor_snapshot(0, Some((u64::MAX, mismatch)), 3)
                .is_none()
        );
        assert!(
            store
                .reorg_anchor_snapshot(3, Some((8, mismatch)), 1)
                .is_none()
        );
        let (copied, first, last) = window(3, 8, 2);
        assert_eq!(copied, vec![anchors[1].anchor, anchors[2].anchor]);
        assert_eq!(first, Some(0));
        assert_eq!(last, Some(u64::MAX));
        assert_eq!(window(u64::MAX, u64::MAX, 1).0, vec![anchors[3].anchor]);
        assert!(window(4, 7, 0).0.is_empty());
        store.replace_anchors(vec![test_anchor(10)]).unwrap();
        assert_eq!(first, Some(0));
        assert_eq!(last, Some(u64::MAX));
        assert_eq!(copied, vec![anchors[1].anchor, anchors[2].anchor]);
    }

    #[test]
    fn retained_anchors_sorted_normalization_reuses_input_allocation() {
        let anchors = vec![test_anchor(0), test_anchor(2), test_anchor(u64::MAX)];
        let address = anchors.as_ptr();
        let normalized = normalize_anchor_records(anchors);
        assert_eq!(normalized.as_ptr(), address);
        assert_eq!(
            normalized,
            vec![test_anchor(0), test_anchor(2), test_anchor(u64::MAX)]
        );
    }

    fn assert_coverage_matches_records(store: &ConsensusStore) {
        let records = store.ordered_anchors();
        let mut previous: Option<AnchorRecord> = None;
        let mut gaps = 0;
        for record in &records {
            if let Some(parent) = previous
                && (parent.anchor.block_number.checked_add(1) != Some(record.anchor.block_number)
                    || record.parent_beacon_root != Some(parent.anchor.beacon_root))
            {
                gaps += 1;
            }
            previous = Some(*record);
        }
        assert_eq!(
            store.anchor_coverage(),
            AnchorCoverage {
                floor: records.first().map(|record| record.anchor),
                ceiling: records.last().map(|record| record.anchor),
                count: records.len(),
                gap_count: gaps,
            }
        );
        for block in [0, 1, 2, 3, 5, 9, u64::MAX - 1, u64::MAX] {
            for limit in [0, 1, 3, usize::MAX] {
                let expected: Vec<_> = records
                    .iter()
                    .copied()
                    .filter(|record| record.anchor.block_number > block)
                    .take(limit)
                    .collect();
                assert_eq!(store.anchor_records_after(block, limit), expected);
            }
        }
    }

    #[test]
    fn retained_anchor_coverage_and_suffix_match_reference_after_mutations() {
        let temp = TempDir::new().unwrap();
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let mut linked = test_anchor(2);
        linked.parent_beacon_root = Some(test_anchor(1).anchor.beacon_root);
        let sets = [
            Vec::new(),
            vec![test_anchor(1), linked, test_anchor(5)],
            vec![test_anchor(u64::MAX), test_anchor(0), test_anchor(0)],
        ];
        for records in sets {
            store.replace_anchors(records).unwrap();
            assert_coverage_matches_records(&store);
            store.append_anchors(vec![test_anchor(9), linked]).unwrap();
            assert_coverage_matches_records(&store);
            store.replace_anchor_range(1, 5, vec![linked]).unwrap();
            assert_coverage_matches_records(&store);
            store.replace_anchor_range(1, 5, Vec::new()).unwrap();
            assert_coverage_matches_records(&store);
            // Preserve the public reversed/out-of-range merge fallback too.
            store.replace_anchor_range(5, 1, vec![linked]).unwrap();
            assert_coverage_matches_records(&store);
            store
                .replace_anchor_range(1, 3, vec![test_anchor(5)])
                .unwrap();
            assert_coverage_matches_records(&store);
            let reopened = ConsensusStore::open(temp.path(), None).unwrap();
            assert_coverage_matches_records(&reopened);
            assert_eq!(reopened.anchor_coverage(), store.anchor_coverage());
        }
        let snapshot = store.inner.lock().unwrap();
        assert!(
            serde_json::to_value(&*snapshot)
                .unwrap()
                .get("anchor_gap_count")
                .is_none()
        );
    }

    #[test]
    fn replaced_roots_match_independent_range_reference_and_reopen() {
        let record = |height, root| {
            let mut record = test_anchor(height);
            record.anchor.beacon_root = B256::repeat_byte(root);
            record
        };
        let previous = vec![
            record(0, 1),
            record(2, 2),
            record(4, 1),
            record(7, 3),
            record(u64::MAX, 4),
        ];
        let cases = [
            (2, 4, vec![record(2, 5), record(3, 6)]),
            (4, 4, Vec::new()), // Root 1 remains owned at height 0.
            (2, 7, vec![record(3, 2), record(6, 1)]), // Roots move heights.
            (0, u64::MAX, Vec::new()),
            (5, 6, Vec::new()),
            (7, 2, vec![record(7, 8), record(2, 9)]),
            (2, 4, vec![record(7, 8), record(0, 9)]),
            (0, 4, vec![record(4, 5), record(0, 7), record(4, 1)]),
            (u64::MAX, u64::MAX, vec![record(u64::MAX, 9)]),
        ];
        for checkpoint in [false, true] {
            for (start, end, incoming) in &cases {
                let temp = TempDir::new().unwrap();
                let store = ConsensusStore::open(
                    temp.path(),
                    Some(&format!("{:#x}", B256::repeat_byte(1))),
                )
                .unwrap();
                store.replace_anchors(previous.clone()).unwrap();
                if checkpoint {
                    store.writer.lock().unwrap().force_checkpoint_due_for_test();
                }
                // Independent map reference: delete the inclusive interval,
                // then upsert in input order so the last duplicate key wins.
                let mut expected: BTreeMap<_, _> = previous
                    .iter()
                    .map(|record| (record.anchor.block_number, *record))
                    .collect();
                if start <= end {
                    expected.retain(|height, _| height < start || height > end);
                }
                for record in incoming {
                    expected.insert(record.anchor.block_number, *record);
                }
                let expected_roots: HashSet<_> = previous
                    .iter()
                    .filter(|old| {
                        expected
                            .get(&old.anchor.block_number)
                            .is_none_or(|new| new.anchor.beacon_root != old.anchor.beacon_root)
                    })
                    .map(|record| record.anchor.beacon_root)
                    .collect();
                let roots = store
                    .replace_anchor_range_with_replaced_roots(*start, *end, incoming.clone())
                    .unwrap();
                assert_eq!(roots.len(), expected_roots.len());
                assert_eq!(roots.into_iter().collect::<HashSet<_>>(), expected_roots);
                let expected: Vec<_> = expected.into_values().collect();
                assert_eq!(store.ordered_anchors(), expected);
                assert_eq!(
                    ConsensusStore::open(temp.path(), None)
                        .unwrap()
                        .ordered_anchors(),
                    expected
                );
                assert_coverage_matches_records(&store);
            }
        }
    }

    #[test]
    fn replaced_roots_distinguish_noop_from_same_root_metadata_change() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        let mut record = test_anchor(10);
        store.replace_anchors(vec![record]).unwrap();
        let before = fs::read(store.state_path()).unwrap();
        assert!(
            store
                .replace_anchor_range_with_replaced_roots(10, 10, vec![record])
                .unwrap()
                .is_empty()
        );
        assert_eq!(fs::read(store.state_path()).unwrap(), before);
        record.finalized = !record.finalized;
        record.parent_beacon_root = Some(B256::repeat_byte(8));
        assert!(
            store
                .replace_anchor_range_with_replaced_roots(10, 10, vec![record])
                .unwrap()
                .is_empty()
        );
        assert_ne!(fs::read(store.state_path()).unwrap(), before);
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.ordered_anchors(), vec![record]);
        let summary = reopened.with_snapshot(|snapshot| {
            (
                snapshot.ordered_anchors[0],
                snapshot.verified_light_client_store.is_none(),
            )
        });
        assert_eq!(summary, (record, true));
    }

    #[test]
    fn replaced_roots_are_not_returned_after_failed_publication() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        let original = test_anchor(10);
        store.replace_anchors(vec![original]).unwrap();
        let mut replacement = original;
        replacement.anchor.beacon_root = B256::repeat_byte(9);
        let directory = store.state_path().parent().unwrap();
        let moved = temp.path().join("paused-test-store");
        fs::rename(directory, &moved).unwrap();
        assert!(
            store
                .replace_anchor_range_with_replaced_roots(10, 10, vec![replacement])
                .is_err()
        );
        assert_eq!(store.ordered_anchors(), vec![original]);
        assert!(!directory.exists());
        // The failure latch also precedes the exact-no-op shortcut.
        assert!(matches!(
            store.replace_anchor_range_with_replaced_roots(10, 10, vec![original]),
            Err(ConsensusStateError::StorageFailed(_))
        ));
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
            writer_store.update_if_with_writer(
                |_| {
                    Ok(Some(StateDelta::UpsertAnchors(vec![
                        test_anchor(10),
                        test_anchor(12),
                    ])))
                },
                |journal, path, delta| {
                    entered_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    journal.append(path, delta)
                },
            )
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let (read_tx, read_rx) = mpsc::channel();
        let reader_store = Arc::clone(&store);
        let reader = std::thread::spawn(move || {
            read_tx
                .send((
                    reader_store.ordered_anchors(),
                    reader_store.anchor_coverage(),
                ))
                .unwrap()
        });
        let observed = read_rx.recv_timeout(Duration::from_secs(2));
        resume_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        reader.join().unwrap();
        let (records, coverage) = observed.unwrap();
        assert!(records.is_empty());
        assert_eq!(coverage.count, 0);
        assert_eq!(coverage.gap_count, 0);
        assert_eq!(
            store.ordered_anchors(),
            vec![test_anchor(10), test_anchor(12)]
        );
        assert_eq!(store.anchor_coverage().gap_count, 1);
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
        let result = store.update_if_with_writer(
            |_| {
                Ok(Some(StateDelta::UpsertAnchors(vec![
                    test_anchor(10),
                    test_anchor(12),
                ])))
            },
            |journal, path, delta| {
                journal.append(path, delta)?;
                // Model an error reported after replacement. The caller cannot
                // assume whether the new file became durable in this case.
                Err(io::Error::other("injected error after replacement"))
            },
        );
        assert!(result.is_err());
        assert!(store.ordered_anchors().is_empty());
        assert_eq!(store.anchor_coverage().count, 0);
        assert_eq!(store.anchor_coverage().gap_count, 0);
        let disk_after_error = fs::read(store.state_path()).unwrap();
        assert!(matches!(
            store.append_anchors(vec![test_anchor(11)]),
            Err(ConsensusStateError::StorageFailed(_))
        ));
        assert_eq!(fs::read(store.state_path()).unwrap(), disk_after_error);
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(
            reopened.ordered_anchors(),
            vec![test_anchor(10), test_anchor(12)]
        );
        assert_eq!(reopened.anchor_coverage().gap_count, 1);
        reopened.append_anchors(vec![test_anchor(11)]).unwrap();
        assert_eq!(
            reopened.ordered_anchors(),
            vec![test_anchor(10), test_anchor(11), test_anchor(12)]
        );
        assert_coverage_matches_records(&reopened);
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
        let bytes = install_snapshot(store.state_path(), &snapshot);
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
        install_snapshot(store.state_path(), &snapshot);
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
        let bytes = install_snapshot(store.state_path(), &snapshot);
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
        fs::create_dir_all(path.parent().unwrap()).unwrap();
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
            install_snapshot(store.state_path(), &snapshot);
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
    fn cached_light_client_updates_by_range_start_at_earliest_known_period() {
        let payload = |byte: u8| RawRpcResponse {
            context_bytes: Some([byte; 4]),
            bytes: vec![byte],
        };
        let payloads = BTreeMap::from([
            (10u64, payload(0x0a)),
            (12u64, payload(0x0c)),
            (13u64, payload(0x0d)),
        ]);

        let responses = cached_light_client_update_payloads_by_range(11, 4, &payloads);

        assert_eq!(responses, vec![payload(0x0c), payload(0x0d)]);
    }

    #[test]
    fn cached_light_client_updates_by_range_stops_on_first_gap() {
        let payload = |byte: u8| RawRpcResponse {
            context_bytes: Some([byte; 4]),
            bytes: vec![byte],
        };
        let payloads = BTreeMap::from([
            (20u64, payload(0x14)),
            (21u64, payload(0x15)),
            (23u64, payload(0x17)),
        ]);

        let responses = cached_light_client_update_payloads_by_range(20, 8, &payloads);

        assert_eq!(responses, vec![payload(0x14), payload(0x15)]);
    }

    #[test]
    fn cached_light_client_range_selection_matches_independent_scan() {
        for mask in 0..256u16 {
            let payloads: BTreeMap<_, _> = (0..8u64)
                .filter(|period| mask & (1 << period) != 0)
                .map(|period| {
                    (
                        period,
                        RawRpcResponse {
                            context_bytes: None,
                            bytes: vec![period as u8],
                        },
                    )
                })
                .collect();
            for start in 0..10u64 {
                for count in 0..10u64 {
                    let eligible: Vec<_> = payloads
                        .iter()
                        .filter(|(period, _)| **period >= start && **period - start < count)
                        .collect();
                    let expected: Vec<_> = eligible.first().map_or_else(Vec::new, |(first, _)| {
                        eligible
                            .iter()
                            .enumerate()
                            .take_while(|(index, (period, _))| **period - **first == *index as u64)
                            .map(|(_, (_, payload))| (*payload).clone())
                            .collect()
                    });
                    assert_eq!(
                        cached_light_client_update_payloads_by_range(start, count, &payloads),
                        expected,
                        "mask={mask} start={start} count={count}"
                    );
                }
            }
        }
    }

    #[test]
    fn cached_light_client_range_selection_includes_maximum_key() {
        // Arithmetic control only: real committee periods come from slot/8192
        // and never approach this boundary.
        let payload = RawRpcResponse {
            context_bytes: None,
            bytes: vec![1],
        };
        let payloads = BTreeMap::from([(u64::MAX, payload.clone())]);
        assert_eq!(
            cached_light_client_update_payloads_by_range(u64::MAX, 1, &payloads),
            vec![payload.clone()]
        );
        assert_eq!(
            cached_light_client_update_payloads_by_range(u64::MAX - 1, 2, &payloads),
            vec![payload]
        );
        assert!(
            cached_light_client_update_payloads_by_range(u64::MAX - 1, 1, &payloads).is_empty()
        );
        assert!(cached_light_client_update_payloads_by_range(u64::MAX, 0, &payloads).is_empty());
    }

    #[test]
    fn cached_payload_accessors_select_owned_results() {
        let temp = TempDir::new().unwrap();
        let store =
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap();
        assert_eq!(store.light_client_bootstrap_payload(), None);
        assert_eq!(store.light_client_finality_update_payload(), None);
        assert_eq!(store.light_client_optimistic_update_payload(), None);
        assert!(store.light_client_update_payloads(0, 128).is_empty());
        let payload = |byte| RawRpcResponse {
            context_bytes: Some([byte; 4]),
            bytes: vec![byte; 16],
        };
        // Opaque bytes deliberately isolate selection/ownership from the SSZ
        // restore tests; this synthetic in-memory cache is never persisted.
        store.inner.lock().unwrap().light_client_payloads = PersistedLightClientPayloads {
            bootstrap: Some(payload(1)),
            finality_update: Some(payload(2)),
            optimistic_update: Some(payload(3)),
            updates_by_period: (0..256).map(|period| (period, payload(4))).collect(),
        };
        assert_eq!(
            store.light_client_finality_update_payload(),
            Some(payload(2))
        );
        assert_eq!(
            store.light_client_optimistic_update_payload(),
            Some(payload(3))
        );
        let mut bootstrap = store.light_client_bootstrap_payload().unwrap();
        bootstrap.bytes[0] = 0;
        assert_eq!(store.light_client_bootstrap_payload(), Some(payload(1)));
        let mut selected = store.light_client_update_payloads(100, 2);
        assert_eq!(selected, vec![payload(4); 2]);
        selected[0].bytes[0] = 0;
        assert_eq!(
            store.light_client_update_payloads(100, 2),
            vec![payload(4); 2]
        );
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
            anchor_gap_count: 0,
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
            anchor_gap_count: 0,
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
            anchor_gap_count: 0,
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

    fn initialized_fixture_store(
        temp: &TempDir,
        fixture: &crate::light_client::TestLightClientCache,
    ) -> (Arc<ConsensusStore>, VerifiedLightClientStore) {
        let store = Arc::new(
            ConsensusStore::open(
                temp.path(),
                Some(&format!("{:#x}", fixture.checkpoint.beacon_root)),
            )
            .unwrap(),
        );
        assert_eq!(store.checkpoint().beacon_slot, None);
        let bootstrap = fixture.payloads.bootstrap.clone().unwrap();
        let (status, initial) =
            verify_bootstrap_payload(&bootstrap.bytes, fixture.checkpoint).unwrap();
        store
            .record_verified_bootstrap(status, bootstrap, initial.clone())
            .unwrap();
        (store, initial)
    }

    #[test]
    fn bootstrap_status_recovers_checkpoint_slot_when_missing() {
        let temp = TempDir::new().unwrap();
        let fixture =
            crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
        let (store, _) = initialized_fixture_store(&temp, &fixture);
        assert_eq!(store.checkpoint(), fixture.checkpoint);
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.checkpoint(), fixture.checkpoint);
        assert_eq!(
            reopened.light_client_bootstrap_payload(),
            fixture.payloads.bootstrap
        );
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
        let fixture =
            crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
        let (store, initial) = initialized_fixture_store(&temp, &fixture);
        let payload = fixture.payloads.finality_update.unwrap();
        assert_eq!(payload.context_bytes, None);
        let (status, next, _, _) = apply_finality_update_payload(&payload.bytes, &initial).unwrap();
        let expected_status = status.clone();
        let expected_anchor = next.finalized_anchor();
        let expected_bytes = payload.bytes.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            done_tx
                .send(store.record_verified_finality_update(status, payload, next))
                .unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("verified finality update must not deadlock")
            .unwrap();
        writer.join().unwrap();
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.chain_anchors().finalized_head, expected_anchor);
        assert_eq!(
            reopened.light_client_status().finality_update,
            Some(expected_status)
        );
        let payload = reopened.light_client_finality_update_payload().unwrap();
        assert_eq!(payload.bytes, expected_bytes);
        assert!(payload.context_bytes.is_some());
    }

    #[test]
    fn verified_optimistic_update_persists_without_deadlocking() {
        let temp = TempDir::new().unwrap();
        let fixture =
            crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
        let (store, initial) = initialized_fixture_store(&temp, &fixture);
        let payload = fixture.payloads.optimistic_update.unwrap();
        assert_eq!(payload.context_bytes, None);
        let (status, next, _) = apply_optimistic_update_payload(&payload.bytes, &initial).unwrap();
        let expected_status = status.clone();
        let expected_anchor = next.optimistic_anchor();
        let expected_bytes = payload.bytes.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            done_tx
                .send(store.record_verified_optimistic_update(status, payload, next))
                .unwrap();
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("verified optimistic update must not deadlock")
            .unwrap();
        writer.join().unwrap();
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.chain_anchors().optimistic_head, expected_anchor);
        assert_eq!(
            reopened.light_client_status().optimistic_update,
            Some(expected_status)
        );
        let payload = reopened.light_client_optimistic_update_payload().unwrap();
        assert_eq!(payload.bytes, expected_bytes);
        assert!(payload.context_bytes.is_some());
    }

    #[test]
    fn verified_cache_malformed_retained_payload_is_an_actionable_local_error() {
        for finality in [false, true] {
            let temp = TempDir::new().unwrap();
            let fixture =
                crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
            let (store, _) = initialized_fixture_store(&temp, &fixture);
            {
                let mut snapshot = store.inner.lock().unwrap();
                let malformed = Some(RawRpcResponse {
                    context_bytes: None,
                    bytes: vec![0],
                });
                if finality {
                    snapshot.light_client_payloads.finality_update = malformed;
                } else {
                    snapshot.light_client_payloads.optimistic_update = malformed;
                }
            }
            let before = store.inner.lock().unwrap().clone();
            let disk_before = fs::read(store.state_path()).unwrap();
            let result = if finality {
                store.record_verified_finality_update(
                    fixture.status.finality_update.unwrap(),
                    fixture.payloads.finality_update.unwrap(),
                    fixture.store,
                )
            } else {
                store.record_verified_optimistic_update(
                    fixture.status.optimistic_update.unwrap(),
                    fixture.payloads.optimistic_update.unwrap(),
                    fixture.store,
                )
            };
            assert!(
                matches!(result, Err(ConsensusStateError::InvalidCachedPayload(message)) if message.contains("cached") && message.contains("update"))
            );
            assert_eq!(*store.inner.lock().unwrap(), before);
            assert_eq!(fs::read(store.state_path()).unwrap(), disk_before);
            assert!(store.subscribe_storage_failure().borrow().is_none());
        }
    }

    #[test]
    fn verified_cache_preserves_newer_responses_and_skips_unchanged_writes() {
        let temp = TempDir::new().unwrap();
        let slot = recent_cache_fixture_slot();
        let fixture = crate::light_client::test_cached_light_client_fixture(slot);
        let older = crate::light_client::test_cached_light_client_fixture(slot - 4);
        let (store, _) = initialized_fixture_store(&temp, &fixture);
        store
            .record_verified_finality_update(
                fixture.status.finality_update.clone().unwrap(),
                fixture.payloads.finality_update.clone().unwrap(),
                fixture.store.clone(),
            )
            .unwrap();
        store
            .record_verified_optimistic_update(
                fixture.status.optimistic_update.clone().unwrap(),
                fixture.payloads.optimistic_update.clone().unwrap(),
                fixture.store.clone(),
            )
            .unwrap();
        let expected = store.inner.lock().unwrap().clone();
        // Moving only this fixture's snapshot establishes that duplicate/older
        // cache candidates cause no file write, without relying on timestamps.
        let saved = temp.path().join("saved-snapshot.json");
        fs::rename(store.state_path(), &saved).unwrap();
        for candidate in [&fixture, &older] {
            store
                .record_verified_finality_update(
                    candidate.status.finality_update.clone().unwrap(),
                    candidate.payloads.finality_update.clone().unwrap(),
                    fixture.store.clone(),
                )
                .unwrap();
            store
                .record_verified_optimistic_update(
                    candidate.status.optimistic_update.clone().unwrap(),
                    candidate.payloads.optimistic_update.clone().unwrap(),
                    fixture.store.clone(),
                )
                .unwrap();
        }
        assert!(!store.state_path().exists());
        assert_eq!(store.inner.lock().unwrap().clone(), expected);
        fs::rename(&saved, store.state_path()).unwrap();
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.inner.lock().unwrap().clone(), expected);
    }

    #[test]
    fn verified_cache_persists_store_changes_without_replacing_newer_payloads() {
        let temp = TempDir::new().unwrap();
        let slot = recent_cache_fixture_slot();
        let fixture = crate::light_client::test_cached_light_client_fixture(slot);
        let older = crate::light_client::test_cached_light_client_fixture(slot - 4);
        let (store, _) = initialized_fixture_store(&temp, &fixture);
        store
            .record_verified_finality_update(
                fixture.status.finality_update.clone().unwrap(),
                fixture.payloads.finality_update.clone().unwrap(),
                fixture.store.clone(),
            )
            .unwrap();
        store
            .record_verified_optimistic_update(
                fixture.status.optimistic_update.clone().unwrap(),
                fixture.payloads.optimistic_update.clone().unwrap(),
                fixture.store.clone(),
            )
            .unwrap();
        let payloads = store.inner.lock().unwrap().clone().light_client_payloads;
        // The recorder receives an already verified store from its caller.
        // Exercise a participation-only change independently of gossip policy.
        let mut next = fixture.store.clone();
        next.previous_max_active_participants = 1;
        store
            .record_verified_finality_update(
                older.status.finality_update.clone().unwrap(),
                older.payloads.finality_update.clone().unwrap(),
                next.clone(),
            )
            .unwrap();
        assert_eq!(store.light_client_store(), Some(next.clone()));
        next.previous_max_active_participants = 2;
        store
            .record_verified_optimistic_update(
                older.status.optimistic_update.clone().unwrap(),
                older.payloads.optimistic_update.clone().unwrap(),
                next.clone(),
            )
            .unwrap();
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(reopened.light_client_store(), Some(next));
        assert_eq!(
            reopened.inner.lock().unwrap().clone().light_client_payloads,
            payloads
        );
        assert_eq!(reopened.light_client_status(), fixture.status);
    }

    #[test]
    fn verified_cache_priority_keeps_supermajority_and_prefers_newer_headers() {
        let fixture =
            crate::light_client::test_cached_light_client_fixture(recent_cache_fixture_slot());
        let mut stronger = fixture.status.finality_update.unwrap();
        stronger.sync_committee_participants = 342;
        let mut later_weaker = stronger.clone();
        later_weaker.attested_header.beacon_slot += 1;
        later_weaker.sync_committee_participants = 341;
        assert!(finality_cache_priority(&stronger) > finality_cache_priority(&later_weaker));
        later_weaker.sync_committee_participants = 342;
        assert!(finality_cache_priority(&later_weaker) > finality_cache_priority(&stronger));
        stronger.finalized_header.beacon_slot += 1;
        assert!(finality_cache_priority(&stronger) > finality_cache_priority(&later_weaker));
    }

    #[cfg(unix)]
    #[test]
    fn applied_update_noop_preserves_file_and_detects_each_changed_component() {
        use std::os::unix::fs::MetadataExt;

        let temp = TempDir::new().unwrap();
        let slot = recent_cache_fixture_slot();
        let mut fixture = crate::light_client::test_cached_light_client_fixture(slot);
        let (store, initial) = initialized_fixture_store(&temp, &fixture);
        let (period, mut payload) = fixture.payloads.updates_by_period.pop_first().unwrap();
        payload.context_bytes = None;
        let applied = apply_light_client_update_payload(&payload.bytes, &initial).unwrap();
        store
            .record_verified_applied_update(applied.clone(), vec![(period, payload.clone())])
            .unwrap();
        let held = fs::File::open(store.state_path()).unwrap();
        let identity = (
            held.metadata().unwrap().dev(),
            held.metadata().unwrap().ino(),
        );
        let mut contextual = payload.clone();
        crate::light_client::normalize_cached_context(
            &mut contextual,
            applied.optimistic_status.attested_header.beacon_slot,
        )
        .unwrap();
        assert_ne!(payload.context_bytes, contextual.context_bytes);
        // An earlier differing duplicate must not force a write: final key wins.
        store
            .record_verified_applied_update(
                applied.clone(),
                vec![(period, contextual.clone()), (period, payload.clone())],
            )
            .unwrap();
        let metadata = fs::metadata(store.state_path()).unwrap();
        assert_eq!((metadata.dev(), metadata.ino()), identity);

        // A context-only cache change remains meaningful even when store/status match.
        store
            .record_verified_applied_update(applied.clone(), vec![(period, contextual.clone())])
            .unwrap();
        let metadata = fs::metadata(store.state_path()).unwrap();
        assert_ne!((metadata.dev(), metadata.ino()), identity);
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(
            reopened.light_client_update_payloads(period, 1),
            vec![contextual.clone()]
        );
        drop(reopened);

        // Independently reset only diagnostic summaries, keeping real verified bytes/store.
        store
            .update(|snapshot| {
                snapshot.light_client.optimistic_update = None;
                snapshot.light_client.finality_update = None;
            })
            .unwrap();
        store
            .record_verified_applied_update(applied.clone(), vec![(period, contextual.clone())])
            .unwrap();
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(
            reopened.light_client_status().optimistic_update,
            Some(applied.optimistic_status.clone())
        );
        assert_eq!(
            reopened.light_client_status().finality_update,
            applied.finality_status.clone()
        );
        drop(reopened);

        // Restore the genuine bootstrap store alone; equal summaries/payloads must
        // not suppress publishing the full subsequently verified store again.
        store
            .update(|snapshot| {
                snapshot.verified_light_client_store = Some(initial.clone());
                apply_verified_store(snapshot);
            })
            .unwrap();
        assert_ne!(store.light_client_store(), Some(applied.store.clone()));
        store
            .record_verified_applied_update(applied.clone(), vec![(period, contextual.clone())])
            .unwrap();
        assert_eq!(
            ConsensusStore::open(temp.path(), None)
                .unwrap()
                .light_client_store(),
            Some(applied.store.clone())
        );
        store
            .storage_failure
            .send_replace(Some("prior save failure".into()));
        assert!(matches!(
            store.record_verified_applied_update(applied, vec![(period, contextual)]),
            Err(ConsensusStateError::StorageFailed(_))
        ));
    }

    #[test]
    fn verified_updates_by_range_payloads_persist_by_period() {
        let temp = TempDir::new().unwrap();
        let slot = recent_cache_fixture_slot();
        let mut fixture = crate::light_client::test_cached_light_client_fixture(slot);
        let mut historical = crate::light_client::test_cached_light_client_fixture(slot - 8192);
        let (store, initial) = initialized_fixture_store(&temp, &fixture);
        let (period, payload) = fixture.payloads.updates_by_period.pop_first().unwrap();
        let (old_period, old_payload) = historical.payloads.updates_by_period.pop_first().unwrap();
        let applied = apply_light_client_update_payload(&payload.bytes, &initial).unwrap();
        let expected = vec![old_payload.clone(), payload.clone()];
        store
            .record_verified_applied_update(
                applied,
                vec![(old_period, old_payload), (period, payload)],
            )
            .unwrap();
        let reopened = ConsensusStore::open(temp.path(), None).unwrap();
        assert_eq!(period, old_period + 1);
        assert_eq!(
            reopened.light_client_update_payloads(old_period, 2),
            expected
        );
        assert_eq!(
            reopened
                .inner
                .lock()
                .unwrap()
                .light_client_payloads
                .updates_by_period
                .len(),
            2
        );
    }
    #[test]
    fn selection_publication_admits_compatible_anchor_and_holds_snapshot_lock() {
        let directory = tempfile::tempdir().unwrap();
        let store = ConsensusStore::open(
            directory.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let initial = test_anchor(100);
        store.replace_anchors(vec![initial]).unwrap();
        assert_eq!(
            store.with_current_anchor(&initial.anchor, || {
                assert!(store.inner.try_lock().is_err());
                7
            }),
            Some(7)
        );
        let mut finalized = initial;
        finalized.finalized = true;
        finalized.parent_beacon_root = Some(B256::repeat_byte(9));
        store
            .replace_anchors(vec![finalized, test_anchor(101)])
            .unwrap();
        assert_eq!(store.with_current_anchor(&initial.anchor, || 8), Some(8));
        assert_eq!(
            store.with_reorg_anchor_snapshot(
                100,
                Some((100, initial.anchor.block_hash)),
                1,
                |snapshot| {
                    assert!(store.inner.try_lock().is_err());
                    snapshot
                }
            ),
            Some(ReorgAnchorSnapshot::MatchingTip)
        );
        for field in 0..4 {
            let mut replaced = initial;
            match field {
                0 => replaced.anchor.block_hash = B256::repeat_byte(17),
                1 => replaced.anchor.beacon_root = B256::repeat_byte(18),
                2 => replaced.anchor.beacon_slot += 1,
                _ => replaced.anchor.receipts_root = B256::repeat_byte(19),
            }
            store.replace_anchors(vec![replaced]).unwrap();
            assert!(
                store
                    .with_current_anchor(&initial.anchor, || panic!("replaced anchor admitted"))
                    .is_none()
            );
        }
        store.replace_anchors(vec![]).unwrap();
        assert!(
            store
                .with_current_anchor(&initial.anchor, || panic!("missing anchor admitted"))
                .is_none()
        );
        assert!(
            store
                .with_reorg_anchor_snapshot(
                    101,
                    Some((100, initial.anchor.block_hash)),
                    0,
                    |_| panic!("invalid window admitted")
                )
                .is_none()
        );
    }

    #[test]
    fn selection_publication_reuses_pending_lineage_and_ahead_progress_rules() {
        let slot = recent_cache_fixture_slot();
        let fixture = light_client::test_cached_light_client_fixture(slot);
        let directory = tempfile::tempdir().unwrap();
        let store = ConsensusStore::open(
            directory.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        let old = test_anchor(100);
        store.replace_anchors(vec![old]).unwrap();
        let select = |number| {
            let mut verified = fixture.store.clone();
            verified.optimistic_header.beacon.slot = slot + 100;
            verified.optimistic_header.execution =
                Some(light_client::VerifiedExecutionPayloadHeader {
                    block_number: number,
                    block_hash: B256::repeat_byte(0xef),
                    receipts_root: B256::ZERO,
                });
            let anchor = verified.optimistic_anchor().unwrap();
            let mut snapshot = store.inner.lock().unwrap();
            snapshot.verified_light_client_store = Some(verified);
            apply_verified_store(&mut snapshot);
            anchor
        };
        for height in [99, 100] {
            select(height);
            assert!(
                store
                    .with_current_anchor(&old.anchor, || panic!("pending selection admitted"))
                    .is_none()
            );
        }
        select(105);
        // Preserve existing liveness policy, not a claim of ancestry to the
        // unmaterialized higher selected head.
        assert_eq!(store.with_current_anchor(&old.anchor, || true), Some(true));
        let selected = select(100);
        store
            .replace_anchors(vec![AnchorRecord {
                anchor: selected,
                finalized: false,
                parent_beacon_root: None,
            }])
            .unwrap();
        assert_eq!(store.with_current_anchor(&selected, || true), Some(true));
        assert!(
            store
                .with_current_anchor(&old.anchor, || panic!("old branch admitted"))
                .is_none()
        );
    }
}
