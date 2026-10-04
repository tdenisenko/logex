//! Retained source prefixes for bounded online index verification.
//! No network fetch, primary-row mutation or receipt-audit traversal is performed.
use super::super::InspectionLimits;
use super::*;

/// Explicit maintenance allowances; no query limits or implicit defaults apply.
/// Retained artifact and decoded payload limits apply to one opened segment,
/// including a later append visible when that segment is opened. They bound
/// inputs/work, not total allocations or process RSS. Index verification controls
/// its own per-artifact memory and visits every expected source membership.
#[derive(Debug, Clone, Copy)]
pub struct IndexAuditLimits {
    pub max_segments: usize,
    pub max_total_rows: u64,
    pub segment: InspectionLimits,
}

/// A frozen list of physical prefixes retaining the existing directory owner.
/// Later segments are excluded; opened sources may include bounded later appends.
/// Representation-only compaction is permitted.
/// Reorgs and storage close invalidate the view. No metadata or source is written.
///
/// A `.` data root remains relative to the process's pinned working directory,
/// just like NativeStorage. Do not change cwd while using this snapshot.
#[derive(Debug, Clone)]
pub struct IndexAuditSnapshot {
    paths: StorageCatalogPaths,
    segments: Vec<SegmentDescriptor>,
    validity: ReadViewToken,
    _owner: Arc<DataDirectoryLock>,
    limits: IndexAuditLimits,
    physical_rows: u64,
    source_prefix_fingerprint: B256,
}

/// One retained source for derived-index verification. The reader includes the
/// complete prefix opened now, which may include appends after the job captured
/// its minimum row boundary. Every originally selected row remains included.
/// The storage owner/read-view guard remains with the enclosing snapshot.
pub struct IndexAuditSource<'a> {
    pub segment_id: u64,
    pub minimum_rows: u64,
    pub reader: &'a SegmentReader,
    pub index_directory: &'a Path,
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
        format!("index snapshot {resource} needs {required}, limit is {allowed}"),
    )
}

impl NativeStorage {
    /// Capture metadata only, while holding the caller's ordinary storage guard.
    /// Verify indexes outside that guard so I/O cannot block live publication.
    /// Recovery-required storage is rejected without attempting repairs.
    pub fn index_audit_snapshot(&self, limits: IndexAuditLimits) -> io::Result<IndexAuditSnapshot> {
        self.ensure_writable()?;
        limit(
            "segments",
            self.catalog.segments.len() as u64,
            limits.max_segments as u64,
        )?;
        let mut physical_rows = 0u64;
        let mut ids = BTreeSet::new();
        let mut fingerprint = blake3::Hasher::new();
        // Preserve the source identity domain used by existing index reports.
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
                    "index snapshot requires bound nonempty source prefixes",
                ));
            }
            if segment.source_namespace.is_some() != segment.source_commitment.is_some() {
                return Err(invalid("incomplete audit prefix identity"));
            }
            physical_rows = physical_rows
                .checked_add(segment.row_count)
                .ok_or_else(|| invalid("index snapshot total row count overflow"))?;
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
        let snapshot = IndexAuditSnapshot {
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

impl IndexAuditSnapshot {
    pub fn physical_rows(&self) -> u64 {
        self.physical_rows
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn source_prefix_fingerprint(&self) -> B256 {
        self.source_prefix_fingerprint
    }

    /// Visit a finite selection of nonempty sources for an online derived-index
    /// scrub. Source artifacts are retained once per callback; no global storage
    /// selection lock is held while reading. Appends and equivalent compaction
    /// are allowed; reorg/close invalidation prevents successful completion.
    ///
    /// The callback must bind its publication to this exact reader, check all
    /// memberships, and preserve any provisional evidence only after this method
    /// and a final `validate` succeed. Empty selected prefixes have no required
    /// memberships. New segments after capture belong to a later tail check.
    /// This screens source shape and budgets. The callback must verify source
    /// contents and index memberships; this is not Ethereum receipt authentication.
    pub fn visit_index_sources(
        &self,
        cancelled: &dyn Fn() -> bool,
        mut visit: impl FnMut(IndexAuditSource<'_>) -> io::Result<()>,
    ) -> io::Result<()> {
        let result = (|| {
            self.check(cancelled)?;
            let mut opened_rows = 0u64;
            for descriptor in &self.segments {
                self.check(cancelled)?;
                if descriptor.row_count == 0 {
                    continue;
                }
                let directory = self.paths.segment_dir(descriptor.id);
                let mut reader = SegmentReader::open_for_inspection(&directory)?;
                let rows = reader.read_row_count()?;
                opened_rows = opened_rows
                    .checked_add(rows)
                    .ok_or_else(|| invalid("index audit opened row count overflow"))?;
                limit("opened total rows", opened_rows, self.limits.max_total_rows)?;
                limit(
                    "opened segment rows",
                    rows,
                    self.limits.segment.max_segment_rows,
                )?;
                if reader
                    .captured_manifest_identity()
                    .is_none_or(|(id, _)| id != descriptor.id)
                    || rows < descriptor.row_count
                    || reader.source_namespace() != descriptor.source_namespace.map(|v| v.0)
                    || (rows == descriptor.row_count
                        && reader.source_commitment()? != descriptor.source_commitment.map(|v| v.0))
                {
                    return Err(invalid("index audit source differs from captured prefix"));
                }
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
                reader.retain_inspected_append_prefix()?;
                self.check(cancelled)?;
                visit(IndexAuditSource {
                    segment_id: descriptor.id,
                    minimum_rows: descriptor.row_count,
                    reader: &reader,
                    index_directory: &directory.join("indexes"),
                })?;
                self.check(cancelled)?;
            }
            Ok(())
        })();
        self.validate()?;
        result
    }

    /// Recheck before admitting downstream results. An earlier successful visit
    /// does not make a subsequently invalidated view current again.
    pub fn validate(&self) -> io::Result<()> {
        if !self.validity.is_valid() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "index snapshot view changed during reorg or storage close; capture again",
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
                "index snapshot cancelled",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "index_snapshot_tests.rs"]
mod tests;
