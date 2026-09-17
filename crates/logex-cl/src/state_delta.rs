//! Typed durable mutations. Applying a delta never copies unchanged history.
use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum StateDelta {
    ReplaceAnchors(Vec<AnchorRecord>),
    UpsertAnchors(Vec<AnchorRecord>),
    ReplaceRange {
        start: u64,
        end: u64,
        anchors: Vec<AnchorRecord>,
    },
    Bootstrap {
        status: LightClientBootstrapStatus,
        payload: RawRpcResponse,
        store: Box<VerifiedLightClientStore>,
    },
    VerifiedPatch {
        store: Box<VerifiedLightClientStore>,
        finality: Option<Box<LightClientFinalityUpdateStatus>>,
        optimistic: Option<LightClientOptimisticUpdateStatus>,
        finality_payload: Option<RawRpcResponse>,
        optimistic_payload: Option<RawRpcResponse>,
        periods: BTreeMap<u64, RawRpcResponse>,
    },
}

impl StateDelta {
    pub(crate) fn verified(store: VerifiedLightClientStore) -> Self {
        Self::VerifiedPatch {
            store: Box::new(store),
            finality: None,
            optimistic: None,
            finality_payload: None,
            optimistic_payload: None,
            periods: BTreeMap::new(),
        }
    }

    pub(crate) fn validate(&self) -> io::Result<()> {
        match self {
            Self::ReplaceAnchors(anchors)
            | Self::UpsertAnchors(anchors)
            | Self::ReplaceRange { anchors, .. } => validate_anchors(anchors),
            Self::Bootstrap { .. } | Self::VerifiedPatch { .. } => Ok(()),
        }
    }

    /// Validate every committed record, including data later overwritten by a
    /// newer record. Only touched store/cache fragments are examined; retained
    /// anchor and period history is not cloned or rescanned here.
    pub(crate) fn apply_replayed(self, snapshot: &mut ConsensusSnapshot) -> io::Result<()> {
        let mut checkpoint = snapshot.checkpoint;
        let (store, mut payloads) = match &self {
            Self::Bootstrap { store, payload, .. } => {
                checkpoint.beacon_slot = Some(store.bootstrap_slot());
                (
                    Some(store.as_ref()),
                    PersistedLightClientPayloads {
                        bootstrap: Some(payload.clone()),
                        ..Default::default()
                    },
                )
            }
            Self::VerifiedPatch {
                store,
                finality_payload,
                optimistic_payload,
                periods,
                ..
            } => (
                Some(store.as_ref()),
                PersistedLightClientPayloads {
                    finality_update: finality_payload.clone(),
                    optimistic_update: optimistic_payload.clone(),
                    updates_by_period: periods.clone(),
                    ..Default::default()
                },
            ),
            _ => (None, PersistedLightClientPayloads::default()),
        };
        if let Some(store) = store {
            store
                .validate_persisted_state(checkpoint)
                .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
            light_client::validate_cached_light_client_payloads(&mut payloads, checkpoint)
                .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;
        }
        self.apply(snapshot)
    }

    pub(crate) fn apply(self, snapshot: &mut ConsensusSnapshot) -> io::Result<()> {
        self.validate()?;
        match self {
            Self::ReplaceAnchors(anchors) => {
                snapshot.ordered_anchors = anchors;
                recompute_snapshot_anchors(snapshot);
            }
            Self::UpsertAnchors(anchors) => {
                upsert(snapshot, anchors);
            }
            Self::ReplaceRange {
                start,
                end,
                anchors,
            } => {
                let left = snapshot
                    .ordered_anchors
                    .partition_point(|r| r.anchor.block_number < start);
                let right = snapshot
                    .ordered_anchors
                    .partition_point(|r| r.anchor.block_number <= end);
                if start <= end {
                    if anchors
                        .iter()
                        .all(|r| (start..=end).contains(&r.anchor.block_number))
                    {
                        splice(snapshot, left..right, anchors);
                        return Ok(());
                    }
                    splice(snapshot, left..right, Vec::new());
                }
                upsert(snapshot, anchors);
            }
            Self::Bootstrap {
                status,
                payload,
                store,
            } => {
                snapshot.checkpoint.beacon_slot = Some(store.bootstrap_slot());
                snapshot.light_client.bootstrap = Some(status);
                snapshot.light_client.finality_update = None;
                snapshot.light_client.optimistic_update = None;
                snapshot.light_client_payloads.bootstrap = Some(payload);
                snapshot.light_client_payloads.finality_update = None;
                snapshot.light_client_payloads.optimistic_update = None;
                snapshot.verified_light_client_store = Some(*store);
                apply_verified_store(snapshot);
            }
            Self::VerifiedPatch {
                store,
                finality,
                optimistic,
                finality_payload,
                optimistic_payload,
                periods,
            } => {
                if let Some(value) = finality {
                    snapshot.light_client.finality_update = Some(*value);
                }
                if let Some(value) = optimistic {
                    snapshot.light_client.optimistic_update = Some(value);
                }
                if let Some(value) = finality_payload {
                    snapshot.light_client_payloads.finality_update = Some(value);
                }
                if let Some(value) = optimistic_payload {
                    snapshot.light_client_payloads.optimistic_update = Some(value);
                }
                snapshot
                    .light_client_payloads
                    .updates_by_period
                    .extend(periods);
                snapshot.verified_light_client_store = Some(*store);
                apply_verified_store(snapshot);
            }
        }
        Ok(())
    }
}

fn validate_anchors(anchors: &[AnchorRecord]) -> io::Result<()> {
    if anchors
        .windows(2)
        .any(|p| p[0].anchor.block_number >= p[1].anchor.block_number)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "journal anchors must be strictly ordered and unique",
        ));
    }
    Ok(())
}

fn gap(left: &AnchorRecord, right: &AnchorRecord) -> usize {
    usize::from(
        right.anchor.block_number != left.anchor.block_number.saturating_add(1)
            || right.parent_beacon_root != Some(left.anchor.beacon_root),
    )
}

// Count only edges touching the replaced interval, including its two boundaries.
fn affected_gaps(anchors: &[AnchorRecord], range: std::ops::Range<usize>) -> usize {
    let start = range.start.saturating_sub(1);
    let end = range.end.saturating_add(1).min(anchors.len());
    anchors[start..end]
        .windows(2)
        .map(|p| gap(&p[0], &p[1]))
        .sum()
}

fn splice(
    snapshot: &mut ConsensusSnapshot,
    range: std::ops::Range<usize>,
    anchors: Vec<AnchorRecord>,
) {
    let old_finalized = snapshot.anchors.finalized_head;
    let removed_finalized = old_finalized.is_some_and(|head| {
        snapshot.ordered_anchors[range.clone()]
            .binary_search_by_key(&head.block_number, |r| r.anchor.block_number)
            .is_ok()
    });
    let incoming_finalized = anchors.iter().rev().find(|r| r.finalized).map(|r| r.anchor);
    let start = range.start;
    let inserted = anchors.len();
    snapshot.anchor_gap_count -= affected_gaps(&snapshot.ordered_anchors, range.clone());
    snapshot.ordered_anchors.splice(range, anchors);
    snapshot.anchor_gap_count += affected_gaps(&snapshot.ordered_anchors, start..start + inserted);
    if snapshot.verified_light_client_store.is_some() {
        apply_verified_store(snapshot);
        return;
    }
    snapshot.anchors.optimistic_head = snapshot.ordered_anchors.last().map(|r| r.anchor);
    snapshot.anchors.finalized_head = if removed_finalized {
        snapshot
            .ordered_anchors
            .iter()
            .rev()
            .find(|r| r.finalized)
            .map(|r| r.anchor)
    } else {
        old_finalized
            .into_iter()
            .chain(incoming_finalized)
            .max_by_key(|a| a.block_number)
    };
}

fn upsert(snapshot: &mut ConsensusSnapshot, anchors: Vec<AnchorRecord>) {
    let Some(first) = anchors.first() else {
        return;
    };
    let last = anchors.last().unwrap().anchor.block_number;
    let start = snapshot
        .ordered_anchors
        .partition_point(|r| r.anchor.block_number < first.anchor.block_number);
    let end = snapshot
        .ordered_anchors
        .partition_point(|r| r.anchor.block_number <= last);
    if start == end {
        splice(snapshot, start..end, anchors);
        return;
    }
    // Merge only the affected span once, then shift the untouched suffix once.
    // In particular, historical/interleaved batches must not perform one Vec
    // suffix shift for every incoming record. Tail append has no merge buffer.
    let mut existing = snapshot.ordered_anchors[start..end]
        .iter()
        .copied()
        .peekable();
    let mut merged = Vec::new();
    for record in anchors {
        while existing
            .peek()
            .is_some_and(|old| old.anchor.block_number < record.anchor.block_number)
        {
            merged.push(existing.next().unwrap());
        }
        if existing
            .peek()
            .is_some_and(|old| old.anchor.block_number == record.anchor.block_number)
        {
            existing.next();
        }
        merged.push(record);
    }
    merged.extend(existing);
    splice(snapshot, start..end, merged);
}
