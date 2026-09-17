use super::super::catalog::CanonicalReorgIntent;
use super::{NativeStorage, ReadViewEpoch};
use crate::{SyncHead, durability};
use alloy_consensus::Header;
use alloy_primitives::B256;
use logex_types::ExecutionAnchor;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// An admitted durable reorg exclusively borrows storage until completion.
/// Dropping an active handle performs no I/O or rollback: writes remain blocked
/// and read views invalid, until reopening completes the existing catalog intent.
#[must_use = "finish the admitted reorg, or reopen storage before further access"]
pub struct PendingCanonicalReorg<'a> {
    storage: &'a mut NativeStorage,
    active: bool,
}

impl PendingCanonicalReorg<'_> {
    pub fn finish(self) -> io::Result<u64> {
        if self.active {
            self.storage.finish_canonical_reorg()
        } else {
            Ok(0)
        }
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
        let reverted = self.apply_non_canonical_hashes(&reverted_hashes)?;
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
}
