//! Offline derived-index replacement. Primary files and the catalog never change.
//! A durable interlock precedes staging; originals remain in quarantine after a
//! successful repair. Interrupted builds and installations resume under one owner.
mod journal;

use alloy_primitives::FixedBytes;
use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use super::{
    invalid,
    publication::{
        check_headroom, ensure_directory, exists, ordinary_directory, ordinary_file, rename_owned,
        require_no_pending_repair,
    },
};
use crate::native::{
    InspectionLimits, NativeStorageCatalog, PrimaryDataDisposition, PrimaryDataInspection,
    StorageCatalogPaths, directory_lock::DataDirectoryLock, inspection,
};
use crate::{
    durability,
    index_checkpoint::{
        INDEX_CHECKPOINT_FILE, MAX_ARTIFACTS, MAX_CHECKPOINT_BYTES, validate_artifact_name,
    },
};
use journal::{Entry, Journal, Metadata};

pub(in crate::native) const JOURNAL_FILE: &str = "index-repair.journal";

#[derive(Debug)]
pub struct PendingIndexRepair {
    owner: DataDirectoryLock,
    paths: StorageCatalogPaths,
    journal: Journal,
}

pub(super) fn inspect_owned(
    owner: DataDirectoryLock,
    paths: StorageCatalogPaths,
) -> io::Result<PendingIndexRepair> {
    let journal =
        Journal::load(&paths)?.ok_or_else(|| invalid("index repair journal disappeared"))?;
    verify_catalog(&paths, &journal)?;
    Ok(PendingIndexRepair {
        owner,
        paths,
        journal,
    })
}

impl PendingIndexRepair {
    pub fn operation_id(&self) -> FixedBytes<16> {
        self.journal.metadata.operation
    }
    pub fn quarantine_dir(&self) -> PathBuf {
        operation_root(&self.paths, &self.journal).join("quarantine")
    }
    pub fn required_artifacts(&self) -> &[String] {
        &self.journal.metadata.artifacts
    }
    pub fn segment_ids(&self) -> impl Iterator<Item = u64> + '_ {
        self.journal.metadata.entries.iter().map(|entry| entry.id)
    }

    /// Selected row counts come from the validated immutable journal catalog.
    pub fn segment_row_counts(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.journal.metadata.entries.iter().map(|entry| {
            let descriptor = self
                .journal
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == entry.id)
                .expect("validated index repair selection belongs to its catalog");
            (entry.id, descriptor.row_count)
        })
    }

    /// The existing journal already consumes space; reserve one additional
    /// bounded journal for an atomic rewrite or the completed archive.
    pub fn estimate_metadata_bytes(&self) -> u64 {
        journal::MAX_BYTES as u64
    }

    pub fn resume(self, limits: InspectionLimits) -> io::Result<IndexRepair> {
        let inspection = inspection::inspect_owned(self.owner, self.paths, limits)?;
        let repair = IndexRepair {
            inspection,
            journal: self.journal,
            limits,
        };
        repair.verify_sources()?;
        Ok(repair)
    }
}

/// Owns the existing data-directory lock. No ingestion handle is opened.
#[derive(Debug)]
pub struct IndexRepair {
    inspection: PrimaryDataInspection,
    journal: Journal,
    limits: InspectionLimits,
}

impl PrimaryDataInspection {
    /// Incremental logical file bound for the active and replacement/archive
    /// journals. Staging files and filesystem metadata require separate allowance.
    pub fn estimate_index_repair_metadata_bytes(&self) -> u64 {
        2 * journal::MAX_BYTES as u64
    }

    /// Begin local index repair only for segments whose primary commitment can be
    /// verified again. Public inspection report fields are never trusted as proof.
    /// `required_free_bytes` covers the complete attempt, including metadata.
    pub fn begin_index_repair(
        self,
        ids: &[u64],
        artifacts: &[&str],
        limits: InspectionLimits,
        required_free_bytes: u64,
    ) -> io::Result<IndexRepair> {
        if ids.is_empty()
            || ids.len() > journal::MAX_ENTRIES
            || artifacts.is_empty()
            || artifacts.len() > MAX_ARTIFACTS
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "index repair selection exceeds format bounds",
            ));
        }
        for name in artifacts {
            validate_artifact_name(name)?;
        }
        require_no_pending_repair(self.paths.root())?;
        let catalog = NativeStorageCatalog::load_existing(&self.paths)?;
        if catalog != self.catalog {
            return Err(invalid("catalog differs from the inspected snapshot"));
        }
        let mut operation = [0; 16];
        getrandom::fill(&mut operation).map_err(|error| io::Error::other(error.to_string()))?;
        let mut selected = ids.to_vec();
        selected.sort_unstable();
        let mut names: Vec<_> = artifacts.iter().map(|name| (*name).to_owned()).collect();
        names.sort_unstable();
        let entries = selected
            .into_iter()
            .map(|id| {
                let index_dir = self.paths.segment_dir(id).join("indexes");
                let had_indexes = exists(&index_dir)?;
                if had_indexes {
                    ordinary_directory(&index_dir)?;
                }
                Ok(Entry {
                    id,
                    had_indexes,
                    prepared: None,
                })
            })
            .collect::<io::Result<_>>()?;
        let journal = Journal {
            catalog,
            metadata: Metadata {
                operation: operation.into(),
                artifacts: names,
                entries,
            },
        };
        // Encode checks selection/artifact bounds before any write.
        let encoded = journal.encode()?;
        let repair = IndexRepair {
            inspection: self,
            journal,
            limits,
        };
        repair.verify_sources()?;
        let root = operation_root(&repair.inspection.paths, &repair.journal);
        if exists(&root)? {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "index repair operation directory already exists",
            ));
        }
        check_headroom(repair.inspection.paths.root(), required_free_bytes)?;
        durability::write_bytes(&repair.inspection.paths.root().join(JOURNAL_FILE), &encoded)?;
        repair.ensure_directories()?;
        Ok(repair)
    }
}

impl IndexRepair {
    pub fn operation_id(&self) -> FixedBytes<16> {
        self.journal.metadata.operation
    }
    pub fn quarantine_dir(&self) -> PathBuf {
        operation_root(&self.inspection.paths, &self.journal).join("quarantine")
    }
    pub fn required_artifacts(&self) -> &[String] {
        &self.journal.metadata.artifacts
    }

    /// Build and verify every selected index tree before installing any of them.
    /// Callbacks run synchronously under the root owner; they must build only at
    /// the supplied fresh destination and verify all requested artifacts against
    /// the supplied primary source. Errors retain intent and all originals.
    /// A retry rechecks disk headroom; existing attempts are neither freed nor
    /// counted as future growth. Logical byte estimates are not a reservation.
    pub fn execute(
        &mut self,
        required_free_bytes: u64,
        mut build: impl FnMut(&Path, &Path) -> io::Result<()>,
        mut verify: impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<PathBuf> {
        self.verify_intent()?;
        self.verify_sources()?;
        check_headroom(self.inspection.paths.root(), required_free_bytes)?;
        self.ensure_directories()?;
        for index in 0..self.journal.metadata.entries.len() {
            self.prepare(index, &mut build, &mut verify)?;
        }
        for index in 0..self.journal.metadata.entries.len() {
            self.install(index, &mut verify)?;
        }
        self.verify_sources()?;
        for entry in &self.journal.metadata.entries {
            let source = self.inspection.paths.segment_dir(entry.id);
            self.verify_prepared(entry, &source, &source.join("indexes"), &mut verify)?;
        }
        self.verify_intent()?;
        let bytes = self.journal.encode()?;
        // Completion can be interrupted before the active interlock is removed.
        // A later verified rebuild changes checkpoint identities. Keep each
        // completed state immutable instead of overwriting or rejecting its
        // predecessor merely because the same operation needed another attempt.
        let digest = FixedBytes::from(*blake3::hash(&bytes).as_bytes());
        let archive = operation_root(&self.inspection.paths, &self.journal)
            .join(format!("completed-{digest:x}.journal"));
        if exists(&archive)? {
            ordinary_file(&archive)?;
            let mut retained = Vec::new();
            fs::File::open(&archive)?
                .take(bytes.len() as u64 + 1)
                .read_to_end(&mut retained)?;
            if retained != bytes {
                return Err(invalid("completed index repair evidence differs"));
            }
            durability::sync_file_and_directory(&fs::File::open(&archive)?, &archive)?;
        } else {
            durability::write_bytes(&archive, &bytes)?;
        }
        durability::remove_file(&self.inspection.paths.root().join(JOURNAL_FILE))?;
        Ok(self.quarantine_dir())
    }

    /// Reassess under the same lock after successful completion. An unfinished
    /// journal is a blocker, even if some selected indexes have been installed.
    pub fn into_inspection(self) -> io::Result<PrimaryDataInspection> {
        require_no_pending_repair(self.inspection.paths.root())?;
        self.inspection.reinspect(self.limits)
    }

    fn verify_sources(&self) -> io::Result<()> {
        verify_catalog(&self.inspection.paths, &self.journal)?;
        let prerequisites =
            inspection::recovery_prerequisites(&self.inspection.paths, &self.journal.catalog)?;
        if prerequisites
            .iter()
            .any(|path| *path != self.inspection.paths.root().join(JOURNAL_FILE))
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "complete verified storage recovery before rebuilding indexes",
            ));
        }
        for entry in &self.journal.metadata.entries {
            let descriptor = self
                .journal
                .catalog
                .segments
                .iter()
                .find(|segment| segment.id == entry.id)
                .ok_or_else(|| invalid("index repair source is absent from the catalog"))?;
            match inspection::inspect_segment(
                &self.inspection.paths.segment_dir(entry.id),
                descriptor,
                self.limits,
            ) {
                PrimaryDataDisposition::CommitmentVerified => {}
                disposition => {
                    let kind = match disposition {
                        PrimaryDataDisposition::LimitExceeded { .. } => io::ErrorKind::InvalidInput,
                        PrimaryDataDisposition::Incomplete { kind, .. } => kind,
                        PrimaryDataDisposition::Unbound => io::ErrorKind::Unsupported,
                        PrimaryDataDisposition::RecoveryRequired => io::ErrorKind::WouldBlock,
                        _ => io::ErrorKind::InvalidData,
                    };
                    return Err(io::Error::new(
                        kind,
                        format!(
                            "index repair requires verified primary segment {}: {disposition:?}",
                            entry.id
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    fn verify_intent(&self) -> io::Result<()> {
        verify_catalog(&self.inspection.paths, &self.journal)?;
        if Journal::load(&self.inspection.paths)?.as_ref() != Some(&self.journal) {
            return Err(invalid(
                "index repair journal differs from the retained intent; resume inspection",
            ));
        }
        Ok(())
    }

    fn ensure_directories(&self) -> io::Result<()> {
        ensure_directory(&self.inspection.paths.root().join("repair"))?;
        let root = operation_root(&self.inspection.paths, &self.journal);
        ensure_directory(&root)?;
        // Harden retained destinations before any source-directory barrier on
        // retry. A prior rename may have succeeded before its first fsync failed.
        for name in ["quarantine", "attempts"] {
            ensure_directory(&root.join(name))?;
        }
        for entry in &self.journal.metadata.entries {
            durability::sync_directory(&self.inspection.paths.segment_dir(entry.id))?;
        }
        ensure_directory(&root.join("staging"))?;
        Ok(())
    }

    fn stage_path(&self, id: u64) -> PathBuf {
        operation_root(&self.inspection.paths, &self.journal)
            .join("staging")
            .join(format!("s_{id:016}"))
    }

    fn original_path(&self, id: u64) -> PathBuf {
        self.quarantine_dir().join(format!("s_{id:016}"))
    }

    fn retain_attempt(&self, id: u64, path: &Path) -> io::Result<()> {
        let attempts = operation_root(&self.inspection.paths, &self.journal).join("attempts");
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).map_err(|error| io::Error::other(error.to_string()))?;
        rename_owned(
            path,
            &attempts.join(format!("s_{id:016}_{:x}", FixedBytes::from(nonce))),
        )
    }

    // Recognize only locations implied by the recorded intent. Missing originals
    // and ambiguous duplicate destinations are never guessed away.
    fn locations(&self, entry: &Entry) -> io::Result<(bool, bool, bool)> {
        let source = self.inspection.paths.segment_dir(entry.id);
        let stage = exists(&self.stage_path(entry.id))?;
        let active = exists(&source.join("indexes"))?;
        let original = exists(&self.original_path(entry.id))?;
        if (!entry.had_indexes && original)
            || (entry.had_indexes && !original && !active)
            || (original && entry.prepared.is_none())
            || (!entry.had_indexes && active && entry.prepared.is_none())
            || ((original || !entry.had_indexes) && active && stage)
        {
            return Err(invalid(
                "index repair has conflicting or missing artifact locations",
            ));
        }
        if stage {
            ordinary_directory(&self.stage_path(entry.id))?;
        }
        if active {
            ordinary_directory(&source.join("indexes"))?;
        }
        if original {
            ordinary_directory(&self.original_path(entry.id))?;
        }
        Ok((stage, active, original))
    }

    fn prepare(
        &mut self,
        index: usize,
        build: &mut impl FnMut(&Path, &Path) -> io::Result<()>,
        verify: &mut impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        self.verify_intent()?;
        let entry = self.journal.metadata.entries[index].clone();
        let source = self.inspection.paths.segment_dir(entry.id);
        let stage = self.stage_path(entry.id);
        let (staged, active, original) = self.locations(&entry)?;
        if active && (original || !entry.had_indexes) {
            match self.verify_prepared(&entry, &source, &source.join("indexes"), verify) {
                Ok(()) => return Ok(()),
                Err(error) if rebuildable(&error) => {
                    self.retain_attempt(entry.id, &source.join("indexes"))?
                }
                Err(error) => return Err(error),
            }
        }
        let mut reuse = false;
        if staged {
            verify_index_tree_paths(&stage)?;
            let result = if entry.prepared.is_some() {
                self.verify_prepared(&entry, &source, &stage, verify)
            } else {
                verify(&source, &stage).and_then(|()| checkpoint_digest(&stage).map(|_| ()))
            };
            match result {
                Ok(()) => reuse = true,
                Err(error) if rebuildable(&error) => self.retain_attempt(entry.id, &stage)?,
                Err(error) => return Err(error),
            }
        }
        if !reuse {
            build(&source, &stage)?;
        }
        verify_index_tree_paths(&stage)?;
        verify(&source, &stage)?;
        let digest = checkpoint_digest(&stage)?;
        // Repeat the file barriers for a previously successful build whose final
        // directory fsync failed. The marker itself is part of the prepared proof.
        durability::sync_tree(&stage)?;
        let mut journal = self.journal.clone();
        journal.metadata.entries[index].prepared = Some(digest);
        journal.persist(&self.inspection.paths)?;
        self.journal = journal;
        Ok(())
    }

    fn verify_prepared(
        &self,
        entry: &Entry,
        source: &Path,
        indexes: &Path,
        verify: &mut impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        verify_index_tree_paths(indexes)?;
        if Some(checkpoint_digest(indexes)?) != entry.prepared {
            return Err(invalid(
                "index repair checkpoint differs from its prepared identity",
            ));
        }
        verify(source, indexes)
    }

    fn install(
        &self,
        index: usize,
        verify: &mut impl FnMut(&Path, &Path) -> io::Result<()>,
    ) -> io::Result<()> {
        self.verify_intent()?;
        let entry = &self.journal.metadata.entries[index];
        let source = self.inspection.paths.segment_dir(entry.id);
        let active_path = source.join("indexes");
        let stage_path = self.stage_path(entry.id);
        let (staged, active, original) = self.locations(entry)?;
        if !staged {
            if !active || (entry.had_indexes && !original) {
                return Err(invalid("prepared index repair stage is missing"));
            }
            self.verify_prepared(entry, &source, &active_path, verify)?;
        } else {
            self.verify_prepared(entry, &source, &stage_path, verify)?;
            if entry.had_indexes && !original {
                rename_owned(&active_path, &self.original_path(entry.id))?;
            }
            rename_owned(&stage_path, &active_path)?;
            self.verify_prepared(entry, &source, &active_path, verify)?;
        }
        // A retry may observe the rename after either parent barrier failed.
        for directory in [
            &source,
            &self.quarantine_dir(),
            &stage_path.parent().unwrap().to_owned(),
        ] {
            durability::sync_directory(directory)?;
        }
        Ok(())
    }
}

fn verify_catalog(paths: &StorageCatalogPaths, journal: &Journal) -> io::Result<()> {
    if NativeStorageCatalog::load_existing(paths)? != journal.catalog {
        return Err(invalid("catalog changed during offline index repair"));
    }
    crate::native::storage::verify_recent_headers(&journal.catalog.state)
}

fn operation_root(paths: &StorageCatalogPaths, journal: &Journal) -> PathBuf {
    paths
        .root()
        .join("repair")
        .join(format!("index-{:x}", journal.metadata.operation))
}

/// Index outputs are flat files. In particular, never let a generic durability
/// walk follow a symlink or open a FIFO supplied in a retained attempt. Regular
/// auxiliary files are safe to retain/flush, including AppleDouble companions.
/// The storage owner must keep these paths stable through verification/barriers.
fn verify_index_tree_paths(indexes: &Path) -> io::Result<()> {
    ordinary_directory(indexes)?;
    for entry in fs::read_dir(indexes)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "index repair output requires ordinary flat files: {}",
                    entry.path().display()
                ),
            ));
        }
    }
    Ok(())
}

fn checkpoint_digest(indexes: &Path) -> io::Result<FixedBytes<32>> {
    ordinary_directory(indexes)?;
    let marker = indexes.join(INDEX_CHECKPOINT_FILE);
    ordinary_file(&marker)?;
    let mut bytes = Vec::new();
    fs::File::open(&marker)?
        .take(MAX_CHECKPOINT_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() > MAX_CHECKPOINT_BYTES {
        return Err(invalid("index repair checkpoint exceeds bounds"));
    }
    Ok(FixedBytes::from(*blake3::hash(&bytes).as_bytes()))
}

fn rebuildable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof | io::ErrorKind::NotFound
    )
}

#[cfg(test)]
mod tests;
