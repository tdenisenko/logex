use super::super::catalog::CanonicalReorgIntent;
use super::super::catalog::SegmentDescriptor;
use super::{NativeStorage, ReadViewEpoch};
use crate::SegmentReader;
use crate::{SyncHead, durability};
use alloy_consensus::Header;
use alloy_primitives::B256;
use logex_types::ExecutionAnchor;
use logex_types::LogRow;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(test)]
thread_local! {
    pub(super) static AFTER_NOTIFICATION_COMMIT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

/// An admitted durable reorg exclusively borrows storage until completion.
/// Dropping an active handle performs no I/O or rollback: writes remain blocked
/// and read views invalid, until reopening completes the existing catalog intent.
#[must_use = "finish the admitted reorg, or reopen storage before further access"]
pub struct PendingCanonicalReorg<'a> {
    storage: &'a mut NativeStorage,
    active: bool,
}

// Transient identity selection only; payloads are read after committed finish.
// Keep one reader at a time instead of pinning all affected segment artifacts.
pub(super) struct CanonicalReorgSelection {
    pub(super) catalog_index: usize,
    pub(super) descriptor: SegmentDescriptor,
    pub(super) row_ids: Vec<u32>,
}

impl PendingCanonicalReorg<'_> {
    pub fn finish(self) -> io::Result<u64> {
        if self.active {
            self.storage.finish_canonical_reorg()
        } else {
            Ok(0)
        }
    }

    /// Commit retirement completely, then publish exact changed rows in batches.
    /// Storage remains exclusively borrowed through delivery; callbacks must not
    /// reacquire its outer storage lock. A post-commit read
    /// error is terminal to the caller; emitted batches are already committed
    /// removals, but network delivery is not an atomic or persisted transaction.
    /// Selected row IDs and the current segment's reader buffers are transient;
    /// raw readers retain existing whole-column buffers, not a fixed byte budget.
    pub fn finish_with_notifications(self, mut notify: impl FnMut(&[LogRow])) -> io::Result<u64> {
        if !self.active {
            return Ok(0);
        }
        let intent = self
            .storage
            .catalog
            .state
            .canonical_reorg
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "active reorg intent is missing")
            })?;
        let retired_hashes = self
            .storage
            .catalog
            .state
            .recent_headers
            .get(intent.retained_header_count..)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid retired header boundary",
                )
            })?
            .iter()
            .map(Header::hash_slow)
            .collect::<BTreeSet<_>>();
        let mut selections = Vec::new();
        let count = self
            .storage
            .finish_canonical_reorg_selecting(Some(&mut selections))?;
        #[cfg(test)]
        if let Some(hook) = AFTER_NOTIFICATION_COMMIT.with_borrow_mut(Option::take) {
            hook();
        }
        self.storage
            .publish_reorg_rows(selections, &retired_hashes, &mut notify)?;
        Ok(count)
    }
}

impl NativeStorage {
    /// Durably retire a canonical suffix and rewind its progress as one recoverable operation.
    /// After interruption, opening storage completes the intent before exposing any rows.
    pub fn apply_canonical_reorg(
        &mut self,
        reverted_hashes: &[B256],
        retained_headers: &[Header],
        indexed_head: Option<ExecutionAnchor>,
    ) -> io::Result<u64> {
        self.begin_canonical_reorg(reverted_hashes, retained_headers, indexed_head)?
            .finish()
    }

    /// Validate and durably admit the existing catalog intent without scanning
    /// row sources. The caller may release its selection guard before finishing.
    /// Checkpointing and catalog persistence can perform I/O; callers holding a
    /// selection lock should checkpoint beforehand under exclusive storage access.
    pub fn begin_canonical_reorg(
        &mut self,
        reverted_hashes: &[B256],
        retained_headers: &[Header],
        indexed_head: Option<ExecutionAnchor>,
    ) -> io::Result<PendingCanonicalReorg<'_>> {
        self.ensure_writable()?;
        let old_headers = &self.catalog.state.recent_headers;
        if !old_headers.starts_with(retained_headers)
            || old_headers.len().saturating_sub(retained_headers.len()) != reverted_hashes.len()
            || !old_headers[retained_headers.len()..]
                .iter()
                .map(Header::hash_slow)
                .eq(reverted_hashes.iter().copied())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "canonical reorg must retire exactly the persisted header suffix",
            ));
        }
        if reverted_hashes.is_empty() {
            if self.catalog.anchors.indexed_head != indexed_head {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "an empty canonical reorg cannot change the indexed anchor",
                ));
            }
            return Ok(PendingCanonicalReorg {
                storage: self,
                active: false,
            });
        }
        let intent = CanonicalReorgIntent {
            retained_header_count: retained_headers.len(),
            indexed_head,
        };
        intent.validate(&self.catalog.state)?;
        self.checkpoint_durable()?;
        // The old header window remains the bounded, checksummed source of all
        // reverted hashes until the final catalog publication clears this intent.
        self.recovery_required = true;
        self.read_view = ReadViewEpoch(Arc::new(AtomicBool::new(false)));
        self.catalog.state.canonical_reorg = Some(intent);
        self.persist_catalog()?;
        durability::checkpoint("reorg_intent_published", self.paths.root())?;
        Ok(PendingCanonicalReorg {
            storage: self,
            active: true,
        })
    }

    pub(super) fn finish_canonical_reorg(&mut self) -> io::Result<u64> {
        self.finish_canonical_reorg_selecting(None)
    }

    fn finish_canonical_reorg_selecting(
        &mut self,
        selections: Option<&mut Vec<CanonicalReorgSelection>>,
    ) -> io::Result<u64> {
        let Some(intent) = self.catalog.state.canonical_reorg.clone() else {
            return Ok(0);
        };
        intent.validate(&self.catalog.state)?;
        self.recovery_required = true;
        self.read_view = ReadViewEpoch(Arc::new(AtomicBool::new(false)));
        let reverted_hashes = self.catalog.state.recent_headers[intent.retained_header_count..]
            .iter()
            .map(Header::hash_slow)
            .collect::<BTreeSet<_>>();
        let reverted = self.apply_non_canonical_hashes(&reverted_hashes, selections)?;
        durability::checkpoint("reorg_rows_applied", self.paths.root())?;
        self.catalog
            .state
            .recent_headers
            .truncate(intent.retained_header_count);
        self.catalog.state.sync_head =
            self.catalog
                .state
                .recent_headers
                .last()
                .map(|header| SyncHead {
                    block_number: header.number,
                    block_hash: header.hash_slow(),
                    timestamp: header.timestamp,
                });
        self.catalog.anchors.indexed_head = intent.indexed_head;
        self.catalog.state.canonical_reorg = None;
        self.persist_catalog()?;
        durability::checkpoint("reorg_completed", self.paths.root())?;
        self.recovery_required = false;
        self.read_view.0.store(true, Ordering::SeqCst);
        Ok(reverted)
    }

    fn publish_reorg_rows(
        &self,
        selections: Vec<CanonicalReorgSelection>,
        retired_hashes: &BTreeSet<B256>,
        notify: &mut impl FnMut(&[LogRow]),
    ) -> io::Result<()> {
        self.ensure_writable()?;
        for selection in selections {
            let expected = &selection.descriptor;
            let descriptor = self
                .catalog
                .segments
                .get(selection.catalog_index)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "retired source disappeared")
                })?;
            if descriptor.id != expected.id
                || descriptor.generation != expected.generation
                || descriptor.source_namespace != expected.source_namespace
                || descriptor.source_commitment != expected.source_commitment
                || descriptor.row_count != expected.row_count
                || descriptor.column_bundle != expected.column_bundle
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "retired source identity changed",
                ));
            }
            let dir = self.paths.segment_dir(descriptor.id);
            // Existing background compaction can outlive its outer storage lock.
            // Serialize capture with its source owner; no reader fleet is held.
            let _bundle_owner = if descriptor.column_bundle.is_some() {
                super::super::segment::SegmentMaintenanceGuard::acquire(&self.paths, descriptor)?
            } else {
                None
            };
            let _raw_owner = if descriptor.column_bundle.is_none() {
                Some(match descriptor.source_namespace {
                    Some(namespace) => crate::column::SourceWriteGuard::acquire_bound(
                        &dir,
                        namespace.0,
                        descriptor.generation,
                        descriptor.id,
                    )?,
                    None => crate::column::SourceWriteGuard::acquire_legacy(&dir)?,
                })
            } else {
                None
            };
            let reader = SegmentReader::open(&dir)?;
            if reader.generation() != descriptor.generation
                || reader.source_namespace() != descriptor.source_namespace.map(|value| value.0)
                || reader.source_commitment()? != descriptor.source_commitment.map(|value| value.0)
                || reader.bundle_reference() != descriptor.column_bundle.as_ref()
                || reader.read_row_count()? != descriptor.row_count
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "retired reader differs from committed source",
                ));
            }
            let canonical = reader.read_canonical()?;
            if selection
                .row_ids
                .iter()
                .any(|&row| canonical.is_present(u64::from(row)))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "retired row is still canonical",
                ));
            }
            for batch in reader.log_row_batches(&selection.row_ids)? {
                let batch = batch?;
                if batch
                    .iter()
                    .any(|row| !retired_hashes.contains(&row.block_hash))
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "retired row hash differs from selected suffix",
                    ));
                }
                notify(&batch);
            }
        }
        Ok(())
    }
}
