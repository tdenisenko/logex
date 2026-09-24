//! Exclusive offline replacement publication. Original trees survive until commit.
use std::{
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
};

use alloy_primitives::FixedBytes;
use logex_types::LogRow;

use super::super::{
    InspectionLimits, NativeStorageCatalog, PrimaryDataDisposition, PrimaryDataInspection,
    SegmentDescriptor, SegmentManifest, StorageCatalogPaths, directory_lock::DataDirectoryLock,
    inspection,
};
use super::{
    RepairOwnershipPlan, RepairPlanLimits, StagedRepairCandidate, VerifiedRepairCandidate, invalid,
    journal::{JOURNAL_FILE, RepairEntry, RepairJournal},
};
use crate::{NullBitmap, SegmentReader, durability};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairCatalogState {
    BeforePublication,
    AfterPublication,
}

/// Read-only recovery classification retaining the original directory lock.
#[derive(Debug)]
pub struct PendingRepair {
    owner: DataDirectoryLock,
    paths: StorageCatalogPaths,
    journal: RepairJournal,
    state: RepairCatalogState,
}

/// Read-only repair classification retaining one exclusive directory owner.
/// Pending publication evidence is checked before primary-data inspection.
#[derive(Debug)]
pub enum RepairInspection {
    Primary(Box<PrimaryDataInspection>),
    Pending(Box<PendingRepair>),
}

/// Inspect existing repair evidence without opening storage or performing recovery.
/// Both outcomes retain the same owner acquired here for subsequent planning.
pub fn inspect_repair(root: &Path, limits: InspectionLimits) -> io::Result<RepairInspection> {
    let owner = DataDirectoryLock::acquire_existing(root)?;
    let paths = StorageCatalogPaths::new(std::path::absolute(root)?);
    match RepairJournal::load(&paths)? {
        Some(journal) => Ok(RepairInspection::Pending(Box::new(pending_owned(
            owner, paths, journal,
        )?))),
        None => Ok(RepairInspection::Primary(Box::new(
            inspection::inspect_owned(owner, paths, limits)?,
        ))),
    }
}

pub(in crate::native) fn require_no_pending_repair(root: &Path) -> io::Result<()> {
    if exists(&root.join(JOURNAL_FILE))? {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "offline repair journal is present; resume repair before opening storage",
        ));
    }
    Ok(())
}

/// Inspect the complete journal/catalog relation without replay, cleanup or writes.
/// Any unreadable evidence or third catalog state is an explicit blocker.
pub fn inspect_pending_repair(root: &Path) -> io::Result<Option<PendingRepair>> {
    let owner = DataDirectoryLock::acquire_existing(root)?;
    let paths = StorageCatalogPaths::new(std::path::absolute(root)?);
    let Some(journal) = RepairJournal::load(&paths)? else {
        return Ok(None);
    };
    pending_owned(owner, paths, journal).map(Some)
}

fn pending_owned(
    owner: DataDirectoryLock,
    paths: StorageCatalogPaths,
    journal: RepairJournal,
) -> io::Result<PendingRepair> {
    let state = catalog_state(&paths, &journal)?;
    Ok(PendingRepair {
        owner,
        paths,
        journal,
        state,
    })
}

impl PendingRepair {
    pub fn state(&self) -> RepairCatalogState {
        self.state
    }
    pub fn operation_id(&self) -> FixedBytes<16> {
        self.journal.operation
    }
    pub fn quarantine_dir(&self) -> PathBuf {
        operation_root(&self.paths, &self.journal).join("quarantine")
    }

    /// Resume reconstruction under the same owner. A prepared journal still
    /// requires a fresh verified reconstruction and current-consensus admission.
    pub fn into_plan(
        self,
        inspection: InspectionLimits,
        limits: RepairPlanLimits,
    ) -> io::Result<RepairOwnershipPlan> {
        if self.state != RepairCatalogState::BeforePublication {
            return Err(invalid(
                "committed repair must finish quarantine, not reconstruct originals",
            ));
        }
        let report = inspection::inspect_owned(self.owner, self.paths, inspection)?;
        let seeds = self.journal.seeds.clone();
        report.into_repair_plan_with_journal(&seeds, limits, Some(self.journal))
    }

    /// Finish an already-published repair. Consensus changes cannot roll back
    /// its committed catalog. The caller verifies the required derived indexes.
    pub fn finish(
        self,
        limits: InspectionLimits,
        verify_indexes: impl FnMut(&Path) -> io::Result<()>,
    ) -> io::Result<PathBuf> {
        if self.state != RepairCatalogState::AfterPublication {
            return Err(invalid(
                "uncommitted repair requires fresh reconstruction and admission",
            ));
        }
        finish_publication(&self.paths, &self.journal, limits, verify_indexes)
    }
}

/// A durable intent and reserved new IDs, borrowing the inspection's owner.
/// No normal storage open or second directory-lock acquisition occurs here.
#[derive(Debug)]
pub struct RepairPublication<'a> {
    plan: &'a RepairOwnershipPlan,
    journal: RepairJournal,
    _owner: PublicationOwner<'a>,
}

#[derive(Debug)]
struct PublicationOwner<'a>(&'a std::sync::atomic::AtomicBool);

impl Drop for PublicationOwner<'_> {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

impl RepairOwnershipPlan {
    /// Record durable intent before creating staging artifacts. `required_free_bytes`
    /// is caller-estimated headroom, not a reservation or a guarantee against a
    /// later full device. All write failures retain the journal and original data.
    pub fn begin_publication(&self, required_free_bytes: u64) -> io::Result<RepairPublication<'_>> {
        self.publication_active
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "repair plan already has an active publication owner",
                )
            })?;
        let owner = PublicationOwner(&self.publication_active);
        if self.segment_ids().is_empty() || self.segment_ids().len() > super::journal::MAX_ENTRIES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "repair selection exceeds journal bounds",
            ));
        }
        let paths = &self.inspection.paths;
        if NativeStorageCatalog::load_existing(paths)? != *self.catalog() {
            return Err(invalid("repair catalog changed before intent creation"));
        }
        check_headroom(paths.root(), required_free_bytes)?;
        let journal = if let Some(expected) = &self.pending {
            let actual =
                RepairJournal::load(paths)?.ok_or_else(|| invalid("repair journal disappeared"))?;
            if actual != *expected
                || catalog_state(paths, &actual)? != RepairCatalogState::BeforePublication
            {
                return Err(invalid("repair resumption evidence changed"));
            }
            actual
        } else {
            require_no_pending_repair(paths.root())?;
            let mut operation = [0; 16];
            getrandom::fill(&mut operation).map_err(|error| io::Error::other(error.to_string()))?;
            let mut journal = RepairJournal {
                operation: operation.into(),
                before: self.catalog().clone(),
                after: None,
                seeds: self.seeds.clone(),
                entries: Vec::new(),
            };
            journal
                .entries
                .try_reserve_exact(self.segment_ids().len())
                .map_err(io::Error::other)?;
            for (index, &id) in self.segment_ids().iter().enumerate() {
                let proof = self.begin_candidate(id)?;
                let replacement_id = self
                    .catalog()
                    .next_segment_id
                    .checked_add(u64::try_from(index).map_err(io::Error::other)?)
                    .ok_or_else(|| invalid("repair replacement IDs exhausted"))?;
                journal.entries.push(RepairEntry {
                    original_id: id,
                    replacement_id,
                    canonical_digest: canonical_digest(&proof.canonical)?,
                    staged_manifest: None,
                });
            }
            if exists(&operation_root(paths, &journal))? {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "repair operation directory already exists",
                ));
            }
            journal.persist(paths)?;
            journal
        };
        ensure_operation_dirs(paths, &journal)?;
        Ok(RepairPublication {
            plan: self,
            journal,
            _owner: owner,
        })
    }
}

impl<'a> RepairPublication<'a> {
    pub fn operation_id(&self) -> FixedBytes<16> {
        self.journal.operation
    }
    pub fn replacement_id(&self, original_id: u64) -> Option<u64> {
        self.journal
            .entries
            .iter()
            .find(|entry| entry.original_id == original_id)
            .map(|entry| entry.replacement_id)
    }
    pub fn plan(&self) -> &'a RepairOwnershipPlan {
        self.plan
    }

    /// Encode fresh files under a reserved ID, or reopen complete preparations
    /// after checking a freshly reconstructed candidate. Partial attempts are
    /// retained under the same operation instead of overwritten or deleted.
    pub fn stage_candidate(
        &mut self,
        candidate: &VerifiedRepairCandidate<'a>,
        rows: &[LogRow],
        limits: InspectionLimits,
    ) -> io::Result<StagedRepairCandidate<'a>> {
        if !std::ptr::eq(candidate.plan, self.plan) {
            return Err(invalid("repair candidate belongs to another owner"));
        }
        let entry = self
            .journal
            .entries
            .iter()
            .find(|entry| entry.original_id == candidate.descriptor().id)
            .ok_or_else(|| invalid("candidate is outside repair intent"))?;
        if canonical_digest(candidate.canonical())? != entry.canonical_digest {
            return Err(invalid(
                "original canonical flags changed after repair intent",
            ));
        }
        // Verify detached rows even when a durable preparation can be reused.
        let mut verifier = candidate.verifier()?;
        verifier.append(rows)?;
        let source = verifier.finish()?;
        let paths = &self.plan.inspection.paths;
        if let Some(after) = &self.journal.after {
            let descriptor = after
                .segments
                .iter()
                .find(|s| s.id == entry.replacement_id)
                .ok_or_else(|| invalid("prepared replacement descriptor is missing"))?
                .clone();
            let stage = stage_paths(paths, &self.journal, entry.replacement_id);
            let installed = exists(&paths.segment_dir(entry.replacement_id))?;
            let staged = exists(&stage.segment_dir(entry.replacement_id))?;
            if installed == staged {
                return Err(invalid(
                    "prepared replacement must have exactly one location",
                ));
            }
            let result = StagedRepairCandidate {
                source,
                paths: if installed { paths.clone() } else { stage },
                descriptor,
                manifest: entry
                    .staged_manifest
                    .clone()
                    .ok_or_else(|| invalid("prepared manifest missing"))?,
                limits,
            };
            result.verify()?;
            return Ok(result);
        }
        if exists(&paths.segment_dir(entry.replacement_id))? {
            return Err(invalid("unprepared replacement ID is already installed"));
        }
        let stage = stage_paths(paths, &self.journal, entry.replacement_id);
        if exists(stage.root())? {
            let retained = operation_root(paths, &self.journal).join("attempts");
            ensure_directory(&retained)?;
            let mut name = [0; 16];
            getrandom::fill(&mut name).map_err(|error| io::Error::other(error.to_string()))?;
            rename_owned(
                stage.root(),
                &retained.join(format!(
                    "s_{}_{:x}",
                    entry.replacement_id,
                    FixedBytes::from(name)
                )),
            )?;
        }
        source.stage_as(stage.root(), rows, limits, entry.replacement_id)
    }

    /// Independently verify every complete replacement and its required indexes,
    /// then order all trees before the prepared journal. Run this expensive phase
    /// before taking the consensus admission guard. Consumes the writable staging
    /// handles; callers must not subsequently modify the prepared paths.
    pub fn prepare(
        mut self,
        stages: Vec<StagedRepairCandidate<'a>>,
        mut verify_indexes: impl FnMut(&Path) -> io::Result<()>,
    ) -> io::Result<PreparedRepairPublication<'a>> {
        if stages.len() != self.journal.entries.len() {
            return Err(invalid("repair preparation is missing selected owners"));
        }
        if catalog_state(&self.plan.inspection.paths, &self.journal)?
            != RepairCatalogState::BeforePublication
        {
            return Err(invalid("repair already published"));
        }
        let mut after = self.journal.before.clone();
        let staging_root =
            operation_root(&self.plan.inspection.paths, &self.journal).join("staging");
        for entry in &mut self.journal.entries {
            let mut matches = stages
                .iter()
                .filter(|stage| stage.source.descriptor().id == entry.original_id);
            let stage = matches
                .next()
                .ok_or_else(|| invalid("selected repair stage missing"))?;
            if matches.next().is_some()
                || !std::ptr::eq(stage.source.plan, self.plan)
                || stage.descriptor.id != entry.replacement_id
                || canonical_digest(stage.source.canonical())? != entry.canonical_digest
            {
                return Err(invalid("repair stage ownership or identity differs"));
            }
            let expected_root = staging_root.join(format!("r_{:016}", entry.replacement_id));
            if stage.paths.root() != expected_root && stage.paths != self.plan.inspection.paths {
                return Err(invalid("repair stage lies outside its journaled operation"));
            }
            stage.verify()?;
            verify_indexes(&stage.segment_dir())?;
            let descriptor = after
                .segments
                .iter_mut()
                .find(|s| s.id == entry.original_id)
                .ok_or_else(|| invalid("repair original descriptor missing"))?;
            *descriptor = stage.descriptor.clone();
            entry.staged_manifest = Some(stage.manifest.clone());
            if after.active_hot_segment == Some(entry.original_id) {
                after.active_hot_segment = Some(entry.replacement_id);
            }
            if after.active_historical_segment == Some(entry.original_id) {
                after.active_historical_segment = Some(entry.replacement_id);
            }
        }
        after.next_segment_id = after
            .next_segment_id
            .checked_add(u64::try_from(stages.len()).map_err(io::Error::other)?)
            .ok_or_else(|| invalid("repair replacement IDs exhausted"))?;
        self.journal.after = Some(after);
        let bytes = self.journal.encode()?;
        let trees: Vec<_> = stages
            .iter()
            .map(StagedRepairCandidate::segment_dir)
            .collect();
        durability::publish_catalog_after_trees(
            trees.iter().map(PathBuf::as_path),
            &self.plan.inspection.paths.root().join(JOURNAL_FILE),
            &bytes,
        )?;
        Ok(PreparedRepairPublication {
            publication: self,
            stages,
        })
    }
}

/// Complete preparations. Commit only inside a fresh whole-transcript admission.
#[derive(Debug)]
pub struct PreparedRepairPublication<'a> {
    publication: RepairPublication<'a>,
    stages: Vec<StagedRepairCandidate<'a>>,
}

impl<'a> PreparedRepairPublication<'a> {
    /// Install fresh IDs and publish one complete catalog. A failure may leave
    /// either recognized catalog state; retain the journal and resume explicitly.
    /// No original tree is moved here, including after the catalog rename.
    pub fn commit(self) -> io::Result<CommittedRepairPublication<'a>> {
        let paths = &self.publication.plan.inspection.paths;
        let journal = &self.publication.journal;
        if catalog_state(paths, journal)? != RepairCatalogState::BeforePublication {
            return Err(invalid("repair catalog changed before publication"));
        }
        for stage in &self.stages {
            let source = stage.segment_dir();
            let destination = paths.segment_dir(stage.descriptor.id);
            if source != destination {
                rename_owned(&source, &destination)?;
            }
        }
        let after = journal
            .after
            .as_ref()
            .ok_or_else(|| invalid("repair is not prepared"))?;
        let trees: Vec<_> = journal
            .entries
            .iter()
            .map(|entry| paths.segment_dir(entry.replacement_id))
            .collect();
        durability::checkpoint("repair_publish_catalog", &paths.catalog_path())?;
        durability::publish_catalog_after_trees(
            trees.iter().map(PathBuf::as_path),
            &paths.catalog_path(),
            &after.encode()?,
        )?;
        Ok(CommittedRepairPublication {
            publication: self.publication,
        })
    }
}

#[derive(Debug)]
pub struct CommittedRepairPublication<'a> {
    publication: RepairPublication<'a>,
}

impl CommittedRepairPublication<'_> {
    /// Verify committed replacements, harden the catalog, and quarantine each
    /// original before removing the active journal. Run outside the consensus lock.
    pub fn finish(
        self,
        limits: InspectionLimits,
        verify_indexes: impl FnMut(&Path) -> io::Result<()>,
    ) -> io::Result<PathBuf> {
        finish_publication(
            &self.publication.plan.inspection.paths,
            &self.publication.journal,
            limits,
            verify_indexes,
        )
    }
}

fn finish_publication(
    paths: &StorageCatalogPaths,
    journal: &RepairJournal,
    limits: InspectionLimits,
    mut verify_indexes: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<PathBuf> {
    if catalog_state(paths, journal)? != RepairCatalogState::AfterPublication {
        return Err(invalid(
            "repair catalog is not the recorded committed state",
        ));
    }
    let after = journal
        .after
        .as_ref()
        .ok_or_else(|| invalid("repair is not prepared"))?;
    for entry in &journal.entries {
        let descriptor = after
            .segments
            .iter()
            .find(|s| s.id == entry.replacement_id)
            .ok_or_else(|| invalid("committed replacement missing"))?;
        let dir = paths.segment_dir(entry.replacement_id);
        let manifest = SegmentManifest::load(&dir.join("segment.json")).map_err(io::Error::from)?;
        if manifest.as_ref() != entry.staged_manifest.as_ref() {
            return Err(invalid(
                "committed replacement manifest differs from repair journal",
            ));
        }
        let disposition = inspection::inspect_segment(&dir, descriptor, limits);
        if !matches!(disposition, PrimaryDataDisposition::CommitmentVerified) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "committed replacement {} failed bounded primary-data verification: {disposition:?}",
                    descriptor.id
                ),
            ));
        }
        let mut reader = SegmentReader::open_for_inspection(&dir)?;
        let canonical = reader.read_canonical_for_inspection(limits.max_retained_artifact_bytes)?;
        if canonical_digest(&canonical)? != entry.canonical_digest {
            return Err(invalid("committed replacement canonical flags changed"));
        }
        verify_indexes(&dir)?;
    }
    // The observed AFTER catalog must be durable before any original disappears.
    durability::sync_directory(paths.root())?;
    ensure_operation_dirs(paths, journal)?;
    let quarantine = operation_root(paths, journal).join("quarantine");
    ensure_directory(&quarantine)?;
    for entry in &journal.entries {
        let original = journal
            .before
            .segments
            .iter()
            .find(|s| s.id == entry.original_id)
            .ok_or_else(|| invalid("journal original descriptor missing"))?;
        let source = paths.segment_dir(entry.original_id);
        let destination = quarantine.join(format!("s_{:016}", entry.original_id));
        match (exists(&source)?, exists(&destination)?) {
            (true, false) => {
                verify_original_manifest(&source, original)?;
                rename_owned(&source, &destination)?;
            }
            (false, true) => verify_original_manifest(&destination, original)?,
            _ => {
                return Err(invalid(
                    "original has conflicting or missing quarantine locations",
                ));
            }
        }
    }
    durability::sync_directory(&quarantine)?;
    durability::sync_directory(&paths.segments_dir())?;
    let archive = operation_root(paths, journal).join("completed.journal");
    let bytes = journal.encode()?;
    if exists(&archive)? {
        ordinary_file(&archive)?;
        let file = fs::File::open(&archive)?;
        let mut retained = Vec::new();
        file.take(bytes.len() as u64 + 1)
            .read_to_end(&mut retained)?;
        if retained != bytes {
            return Err(invalid(
                "completed repair journal conflicts with retained evidence",
            ));
        }
        durability::sync_file_and_directory(&fs::File::open(&archive)?, &archive)?;
    } else {
        durability::write_bytes(&archive, &bytes)?;
    }
    durability::remove_file(&paths.root().join(JOURNAL_FILE))?;
    Ok(quarantine)
}

fn verify_original_manifest(dir: &Path, descriptor: &SegmentDescriptor) -> io::Result<()> {
    ordinary_directory(dir)?;
    ordinary_file(&dir.join("segment.json"))?;
    let manifest = SegmentManifest::load(&dir.join("segment.json"))
        .map_err(io::Error::from)?
        .ok_or_else(|| invalid("original manifest missing during quarantine"))?;
    if manifest
        != super::super::segment::manifest_with_columns(descriptor, manifest.columns.clone())
    {
        return Err(invalid("original identity changed before quarantine"));
    }
    Ok(())
}

fn catalog_state(
    paths: &StorageCatalogPaths,
    journal: &RepairJournal,
) -> io::Result<RepairCatalogState> {
    let catalog = NativeStorageCatalog::load_existing(paths)?;
    super::super::storage::verify_recent_headers(&catalog.state)?;
    if catalog == journal.before {
        return Ok(RepairCatalogState::BeforePublication);
    }
    if journal.after.as_ref() == Some(&catalog) {
        return Ok(RepairCatalogState::AfterPublication);
    }
    Err(invalid(
        "catalog matches neither repair snapshot; retain all artifacts for inspection",
    ))
}

fn canonical_digest(canonical: &NullBitmap) -> io::Result<FixedBytes<32>> {
    let mut hash = blake3::Hasher::new();
    canonical.write_to(&mut hash)?;
    Ok(FixedBytes::from(*hash.finalize().as_bytes()))
}

fn operation_root(paths: &StorageCatalogPaths, journal: &RepairJournal) -> PathBuf {
    paths
        .root()
        .join("repair")
        .join(format!("{:x}", journal.operation))
}
fn stage_paths(
    paths: &StorageCatalogPaths,
    journal: &RepairJournal,
    id: u64,
) -> StorageCatalogPaths {
    StorageCatalogPaths::new(
        operation_root(paths, journal)
            .join("staging")
            .join(format!("r_{id:016}")),
    )
}
fn ensure_operation_dirs(paths: &StorageCatalogPaths, journal: &RepairJournal) -> io::Result<()> {
    ensure_directory(&paths.root().join("repair"))?;
    let operation = operation_root(paths, journal);
    ensure_directory(&operation)?;
    ensure_directory(&operation.join("staging"))
}
fn exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}
fn ordinary_directory(path: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(path)?.file_type().is_dir() {
        return Err(invalid("repair requires an ordinary directory"));
    }
    Ok(())
}
fn ordinary_file(path: &Path) -> io::Result<()> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid("repair requires an ordinary file"));
    }
    Ok(())
}
fn ensure_directory(path: &Path) -> io::Result<()> {
    match fs::create_dir(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => ordinary_directory(path)?,
        Err(error) => return Err(error),
    }
    // A prior attempt may have created the name but failed either barrier.
    // Repeat both on resume before using the directory to retain original data.
    durability::sync_directory(path)?;
    durability::sync_directory(
        path.parent()
            .ok_or_else(|| invalid("repair directory has no parent"))?,
    )
}
fn rename_owned(source: &Path, destination: &Path) -> io::Result<()> {
    ordinary_directory(source)?;
    if exists(destination)? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "repair destination already exists",
        ));
    }
    let from = source
        .parent()
        .ok_or_else(|| invalid("repair source has no parent"))?;
    let to = destination
        .parent()
        .ok_or_else(|| invalid("repair destination has no parent"))?;
    ordinary_directory(from)?;
    ordinary_directory(to)?;
    durability::checkpoint("repair_rename", destination)?;
    fs::rename(source, destination)?;
    durability::sync_directory(to)?;
    durability::sync_directory(from)
}

#[cfg(unix)]
fn check_headroom(path: &Path, required: u64) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let directory = fs::File::open(path)?;
    let mut status = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: a valid borrowed descriptor and writable statvfs allocation are
    // retained for the call; the structure is read only on a successful result.
    if unsafe { libc::fstatvfs(directory.as_raw_fd(), status.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstatvfs initialized every field consumed below.
    let status = unsafe { status.assume_init() };
    let available = u128::from(status.f_bavail) * u128::from(status.f_frsize);
    if status.f_flag & libc::ST_RDONLY != 0 || available < u128::from(required) {
        return Err(io::Error::other(format!(
            "repair needs {required} free bytes on writable storage; {available} available"
        )));
    }
    Ok(())
}
#[cfg(not(unix))]
fn check_headroom(_: &Path, _: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "repair headroom checks require macOS or Linux",
    ))
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "inspection_tests.rs"]
mod inspection_tests;
