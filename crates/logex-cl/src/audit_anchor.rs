//! Finalized admission for explicit maintenance, stricter than live ingestion.
use super::{ConsensusSnapshot, ConsensusStore};
use logex_types::{ExecutionAnchor, WeakSubjectivityCheckpoint};

/// A retained execution anchor admitted against the verified light-client store.
/// It cannot be constructed or deserialized by callers. Recheck it at completion;
/// holding this value does not freeze consensus or prove execution data contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FinalizedAuditAnchor {
    anchor: ExecutionAnchor,
    checkpoint: WeakSubjectivityCheckpoint,
}

impl FinalizedAuditAnchor {
    pub fn execution(&self) -> ExecutionAnchor {
        self.anchor
    }

    pub fn checkpoint(&self) -> WeakSubjectivityCheckpoint {
        self.checkpoint
    }
}

impl ConsensusStore {
    /// Capture the verified finalized head only after its exact execution anchor
    /// is materialized. An AnchorRecord's `finalized` flag alone is insufficient.
    pub fn finalized_audit_anchor(&self) -> Option<FinalizedAuditAnchor> {
        let snapshot = self.inner.lock().unwrap();
        let anchor = snapshot
            .verified_light_client_store
            .as_ref()?
            .finalized_anchor()?;
        finalized_materialization(&snapshot, &anchor)?;
        Some(FinalizedAuditAnchor {
            anchor,
            checkpoint: snapshot.checkpoint,
        })
    }

    /// Re-authenticate a persisted job's anchor under the original trust root.
    /// Every retained beacon parent between it and the current verified finalized
    /// head must link, without skipped execution blocks or changed identities.
    /// No network fetch, query index, or metadata finality flag supplies trust.
    pub fn restore_finalized_audit_anchor(
        &self,
        checkpoint: WeakSubjectivityCheckpoint,
        anchor: ExecutionAnchor,
    ) -> Option<FinalizedAuditAnchor> {
        let snapshot = self.inner.lock().unwrap();
        if snapshot.checkpoint != checkpoint {
            return None;
        }
        finalized_materialization(&snapshot, &anchor)?;
        Some(FinalizedAuditAnchor { anchor, checkpoint })
    }

    /// Re-admit a result while holding the same consensus snapshot lock.
    /// Acquire execution storage ownership first. The callback must not await,
    /// perform large I/O, or re-enter consensus/peer/progress callbacks. Prepare
    /// durable artifacts before admission; this is not cross-file atomicity.
    pub fn with_finalized_audit_anchor<R>(
        &self,
        captured: &FinalizedAuditAnchor,
        publish: impl FnOnce() -> R,
    ) -> Option<R> {
        let snapshot = self.inner.lock().unwrap();
        if captured.checkpoint != snapshot.checkpoint {
            return None;
        }
        finalized_materialization(&snapshot, &captured.anchor)?;
        Some(publish())
    }
}

fn finalized_materialization(snapshot: &ConsensusSnapshot, anchor: &ExecutionAnchor) -> Option<()> {
    let finalized = snapshot
        .verified_light_client_store
        .as_ref()?
        .finalized_anchor()?;
    if anchor.block_number > finalized.block_number || anchor.beacon_slot > finalized.beacon_slot {
        return None;
    }
    let records = &snapshot.ordered_anchors;
    let first = records
        .binary_search_by_key(&anchor.block_number, |r| r.anchor.block_number)
        .ok()?;
    let last = records
        .binary_search_by_key(&finalized.block_number, |r| r.anchor.block_number)
        .ok()?;
    if records[first].anchor != *anchor || records[last].anchor != finalized {
        return None;
    }
    for pair in records[first..=last].windows(2) {
        let parent = &pair[0].anchor;
        let child = &pair[1].anchor;
        if parent.block_number.checked_add(1) != Some(child.block_number)
            || child.beacon_slot <= parent.beacon_slot
            || pair[1].parent_beacon_root != Some(parent.beacon_root)
        {
            return None;
        }
    }
    Some(())
}

#[cfg(test)]
mod tests;
