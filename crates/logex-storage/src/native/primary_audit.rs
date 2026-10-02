//! Read every physical row of a captured storage prefix, without query pruning.
//!
//! This is the storage input to an independent authenticated history auditor,
//! not an Ethereum-completeness certificate. Callers must compare these rows
//! (including duplicate identities) with complete authenticated block receipts.
//! A source commitment only identifies the captured local content.
use super::super::InspectionLimits;
use super::*;
use crate::commitment::PrefixState;

/// Explicit maintenance allowances; no query limits or implicit defaults apply.
/// Retained artifact and decoded payload limits apply to one opened segment,
/// including a later append visible when that segment is opened. They bound
/// inputs/work, not total allocations or process RSS. One segment's selected
/// fixed columns, row IDs and canonical bitmap coexist with one payload batch.
#[derive(Debug, Clone, Copy)]
pub struct PrimaryAuditLimits {
    pub max_segments: usize,
    pub max_total_rows: u64,
    pub segment: InspectionLimits,
}

/// A frozen list of physical prefixes retaining the existing directory owner.
/// Later appends are excluded; representation-only compaction is permitted.
/// Reorgs and storage close invalidate the view. No metadata or source is written.
///
/// A `.` data root remains relative to the process's pinned working directory,
/// just like NativeStorage. Do not change cwd while using this snapshot.
#[derive(Debug, Clone)]
pub struct PrimaryAuditSnapshot {
    paths: StorageCatalogPaths,
    segments: Vec<SegmentDescriptor>,
    validity: ReadViewToken,
    _owner: Arc<DataDirectoryLock>,
    limits: PrimaryAuditLimits,
    physical_rows: u64,
    source_prefix_fingerprint: B256,
}

/// A row's original physical position and captured canonical membership.
/// No event identity is deduplicated or filtered by stored block bounds.
#[derive(Debug, Clone, Copy)]
pub struct PrimaryAuditedRow<'a> {
    pub segment_id: u64,
    pub row_id: u32,
    pub canonical: bool,
    pub row: &'a LogRow,
}

/// Provisional decoded rows. An error later in the scan invalidates completion;
/// consumers must stage work until the whole scan and final view check succeed.
pub struct PrimaryAuditBatch<'a> {
    segment_id: u64,
    first_row: u32,
    rows: &'a [LogRow],
    canonical: &'a NullBitmap,
}

impl PrimaryAuditBatch<'_> {
    pub fn rows(&self) -> impl ExactSizeIterator<Item = PrimaryAuditedRow<'_>> {
        self.rows.iter().enumerate().map(|(offset, row)| {
            // Every segment and batch was bounded to u32 before decoding.
            let row_id = self.first_row + offset as u32;
            PrimaryAuditedRow {
                segment_id: self.segment_id,
                row_id,
                canonical: self.canonical.is_present(u64::from(row_id)),
                row,
            }
        })
    }
}

/// Complete scan of one catalog-selected physical prefix. Canonical membership
/// is fingerprinted separately because logical commitments also retain fork rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimarySegmentAudit {
    pub segment_id: u64,
    pub physical_rows: u64,
    pub canonical_rows: u64,
    pub source_namespace: Option<[u8; 16]>,
    pub source_commitment: Option<B256>,
    pub canonical_digest: B256,
}

/// Local physical scan evidence only. Empty segments do not prove empty blocks.
/// This says nothing about receipt roots, canonical ancestry or missing blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimaryAuditReport {
    /// Identity of the selected segment namespaces, row counts and commitments;
    /// excludes representation generation and later append-only rows.
    pub source_prefix_fingerprint: B256,
    pub physical_rows: u64,
    pub canonical_rows: u64,
    pub segments: Vec<PrimarySegmentAudit>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn limit(resource: &str, required: u64, allowed: u64) -> io::Result<()> {
    if required > allowed {
        return Err(limit_error(resource, required, allowed));
    }
    Ok(())
}

fn limit_error(resource: &str, required: u64, allowed: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("primary audit {resource} needs {required}, limit is {allowed}"),
    )
}

impl NativeStorage {
    /// Capture metadata only, while holding the caller's ordinary storage guard.
    /// Scan outside that guard so decoding cannot block live publication.
    /// Recovery-required storage is rejected without attempting repairs.
    pub fn primary_audit_snapshot(
        &self,
        limits: PrimaryAuditLimits,
    ) -> io::Result<PrimaryAuditSnapshot> {
        self.ensure_writable()?;
        limit(
            "segments",
            self.catalog.segments.len() as u64,
            limits.max_segments as u64,
        )?;
        let mut physical_rows = 0u64;
        let mut ids = BTreeSet::new();
        let mut fingerprint = blake3::Hasher::new();
        fingerprint.update(b"logex.primary-audit.prefixes.v1\0");
        fingerprint.update(&(self.catalog.segments.len() as u64).to_le_bytes());
        for segment in &self.catalog.segments {
            if !ids.insert(segment.id) {
                return Err(invalid("duplicate physical segment in audit snapshot"));
            }
            limit(
                "segment rows",
                segment.row_count,
                limits.segment.max_segment_rows,
            )?;
            limit("row addressing", segment.row_count, u32::MAX.into())?;
            if segment.row_count != 0
                && (segment.source_namespace.is_none() || segment.source_commitment.is_none())
            {
                return Err(invalid(
                    "primary audit requires bound nonempty source prefixes",
                ));
            }
            if segment.source_namespace.is_some() != segment.source_commitment.is_some() {
                return Err(invalid("incomplete audit prefix identity"));
            }
            physical_rows = physical_rows
                .checked_add(segment.row_count)
                .ok_or_else(|| invalid("primary audit total row count overflow"))?;
            limit("total rows", physical_rows, limits.max_total_rows)?;
            fingerprint.update(&segment.id.to_le_bytes());
            fingerprint.update(&segment.row_count.to_le_bytes());
            fingerprint.update(&[u8::from(segment.source_namespace.is_some())]);
            if let (Some(namespace), Some(commitment)) =
                (segment.source_namespace, segment.source_commitment)
            {
                fingerprint.update(namespace.as_slice());
                fingerprint.update(commitment.as_slice());
            }
        }
        let snapshot = PrimaryAuditSnapshot {
            paths: self.paths.clone(),
            segments: self.catalog.segments.clone(),
            validity: self.read_view_token(),
            _owner: Arc::clone(&self.directory_lock),
            limits,
            physical_rows,
            source_prefix_fingerprint: B256::from(*fingerprint.finalize().as_bytes()),
        };
        snapshot.validate()?;
        Ok(snapshot)
    }
}

impl PrimaryAuditSnapshot {
    /// Recheck before admitting downstream results. A successful earlier scan
    /// does not make a subsequently invalidated view current again.
    pub fn validate(&self) -> io::Result<()> {
        if !self.validity.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "primary audit view changed during reorg or storage close; capture again",
            ));
        }
        Ok(())
    }

    /// Check writable filesystem headroom in the captured, owned data namespace.
    /// This is a point-in-time resource check, not a space reservation. Callers
    /// must still bound their artifacts and handle I/O failures without changing
    /// primary data. As with the snapshot itself, the caller must keep the
    /// captured process cwd pinned when storage uses a relative root.
    pub fn check_available_space(&self, required_free_bytes: u64) -> io::Result<()> {
        self.validate()?;
        let result = crate::native::repair::publication::check_headroom(
            self.paths.root(),
            required_free_bytes,
        );
        self.validate()?;
        result
    }

    fn check(&self, cancelled: &dyn Fn() -> bool) -> io::Result<()> {
        self.validate()?;
        if cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "primary audit cancelled",
            ));
        }
        Ok(())
    }

    /// Visit every catalog-selected physical row once, including fork rows.
    /// Neither bounds, indexes nor verified-coverage markers prune the scan.
    ///
    /// Cancellation is cooperative between finite I/O/decode/callback steps,
    /// not hard preemption of a filesystem operation. Callback output remains
    /// provisional until this returns successfully and `validate` still passes.
    pub fn scan(
        &self,
        cancelled: &dyn Fn() -> bool,
        visit: impl FnMut(PrimaryAuditBatch<'_>) -> io::Result<()>,
    ) -> io::Result<PrimaryAuditReport> {
        let result = self.scan_inner(cancelled, visit);
        // A concurrently invalidated view is unavailable, even when source I/O
        // also failed. Do not misclassify a raced reorg as physical corruption.
        self.validate()?;
        result
    }

    fn scan_inner(
        &self,
        cancelled: &dyn Fn() -> bool,
        mut visit: impl FnMut(PrimaryAuditBatch<'_>) -> io::Result<()>,
    ) -> io::Result<PrimaryAuditReport> {
        self.check(cancelled)?;
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(self.segments.len())
            .map_err(io::Error::other)?;
        let mut canonical_rows = 0u64;
        for descriptor in &self.segments {
            self.check(cancelled)?;
            let audited = self.scan_segment(descriptor, cancelled, &mut visit)?;
            canonical_rows = canonical_rows
                .checked_add(audited.canonical_rows)
                .ok_or_else(|| invalid("primary audit canonical count overflow"))?;
            segments.push(audited);
        }
        self.check(cancelled)?;
        Ok(PrimaryAuditReport {
            source_prefix_fingerprint: self.source_prefix_fingerprint,
            physical_rows: self.physical_rows,
            canonical_rows,
            segments,
        })
    }

    fn scan_segment(
        &self,
        descriptor: &SegmentDescriptor,
        cancelled: &dyn Fn() -> bool,
        visit: &mut impl FnMut(PrimaryAuditBatch<'_>) -> io::Result<()>,
    ) -> io::Result<PrimarySegmentAudit> {
        let mut reader =
            SegmentReader::open_for_inspection(&self.paths.segment_dir(descriptor.id))?;
        let opened_rows = reader.read_row_count()?;
        limit(
            "opened segment rows",
            opened_rows,
            self.limits.segment.max_segment_rows,
        )?;
        if reader
            .captured_manifest_identity()
            .is_none_or(|(id, _)| id != descriptor.id)
            || opened_rows < descriptor.row_count
            || ((descriptor.row_count != 0 || descriptor.source_namespace.is_some())
                && reader.source_namespace() != descriptor.source_namespace.map(|v| v.0))
        {
            return Err(invalid("opened source differs from captured audit prefix"));
        }
        // Fresh empty segments may acquire their first namespace on append.
        // Such a captured prefix has no old rows to bind or visit.
        self.check(cancelled)?;
        reader
            .inspection_preflight(
                self.limits.segment.max_retained_artifact_bytes,
                self.limits.segment.max_decoded_payload_bytes,
            )
            .map_err(|error| match error {
                crate::segment_reader::InspectionPreflightError::Io(error) => error,
                crate::segment_reader::InspectionPreflightError::LimitExceeded {
                    resource,
                    required,
                    limit: allowed,
                } => limit_error(resource, required, allowed),
            })?;
        self.check(cancelled)?;
        let canonical = match reader.read_canonical() {
            Ok(bits) => {
                if bits.len() != opened_rows {
                    return Err(invalid(
                        "audit canonical bitmap differs from opened source row count",
                    ));
                }
                bits
            }
            Err(error)
                if descriptor.row_count == 0
                    && opened_rows == 0
                    && reader.bundle_reference().is_none()
                    && error.kind() == io::ErrorKind::NotFound =>
            {
                NullBitmap::new()
            }
            Err(error) => return Err(error),
        };
        let rows = u32::try_from(descriptor.row_count).map_err(io::Error::other)?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(rows as usize)
            .map_err(io::Error::other)?;
        ids.extend(0..rows);
        let mut prefix = descriptor.source_namespace.map(|v| PrefixState::empty(v.0));
        let mut bounds: Option<RowBounds> = None;
        let mut count = 0u64;
        let mut canonical_rows = 0u64;
        let mut canonical_hash = blake3::Hasher::new();
        canonical_hash.update(b"logex.primary-audit.canonical.v1\0");
        canonical_hash.update(&descriptor.id.to_le_bytes());
        canonical_hash.update(&descriptor.row_count.to_le_bytes());
        self.check(cancelled)?;
        for batch in reader.log_row_batches(&ids)? {
            self.check(cancelled)?;
            let batch = batch?;
            let end = count
                .checked_add(batch.len() as u64)
                .filter(|&end| end <= descriptor.row_count)
                .ok_or_else(|| invalid("primary audit decoded an unexpected row count"))?;
            for (position, row) in (count..end).zip(&batch) {
                if let Some(bounds) = &mut bounds {
                    bounds.include(row);
                } else {
                    bounds = Some(RowBounds::from_row(row));
                }
                let present = canonical.is_present(position);
                canonical_rows += u64::from(present);
                canonical_hash.update(&[u8::from(present)]);
            }
            if let Some(previous) = &prefix {
                prefix = Some(previous.extend(&batch)?);
            }
            self.check(cancelled)?;
            visit(PrimaryAuditBatch {
                segment_id: descriptor.id,
                first_row: count as u32,
                rows: &batch,
                canonical: &canonical,
            })?;
            self.check(cancelled)?;
            count = end;
        }
        if count != descriptor.row_count
            || bounds.map(|v| v.min_block) != descriptor.min_block
            || bounds.map(|v| v.max_block) != descriptor.max_block
            || bounds.map(|v| v.min_timestamp) != descriptor.min_timestamp
            || bounds.map(|v| v.max_timestamp) != descriptor.max_timestamp
        {
            return Err(invalid(
                "audit physical rows differ from captured count or bounds",
            ));
        }
        if prefix.as_ref().map(PrefixState::commitment) != descriptor.source_commitment {
            return Err(invalid(
                "audit physical rows differ from captured source commitment",
            ));
        }
        self.check(cancelled)?;
        Ok(PrimarySegmentAudit {
            segment_id: descriptor.id,
            physical_rows: count,
            canonical_rows,
            source_namespace: descriptor.source_namespace.map(|v| v.0),
            source_commitment: descriptor.source_commitment,
            canonical_digest: B256::from(*canonical_hash.finalize().as_bytes()),
        })
    }
}

#[cfg(test)]
#[path = "primary_audit_tests.rs"]
mod tests;
