use std::io;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, Bytes};
use logex_types::{LogRow, QueryBuffer, QueryMemoryBudget, Source};

use crate::column_artifact::ColumnArtifacts;
use crate::native::{ColumnDescriptor, CompressionCodec, SegmentManifest};
use crate::page::{
    PageIndexEntry, decode_fixed_width_page_accounted, decode_u8_page_accounted,
    decode_u32_page_accounted, decode_u64_page_accounted, decode_var_bytes_page_bounded,
    decode_var_bytes_page_selected_accounted, read_page_index, read_page_index_accounted,
};
use crate::reader::{RawBytesColumn, RawFixedColumn, validate_null_bytes};
use crate::{
    ColumnFileHeader, NullBitmap,
    column::{PrefixRecoveryGuard, read_source_binding, verify_prefix_recovery_pending},
};

#[cfg(test)]
thread_local! {
    static AFTER_ARTIFACT_CAPTURE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static BATCH_PAGE_READS: std::cell::RefCell<Option<Vec<(String, u64)>>> = const { std::cell::RefCell::new(None) };
}

#[derive(Debug, Clone)]
pub struct SegmentReader {
    dir: PathBuf,
    manifest: Option<SegmentManifest>,
    artifacts: ColumnArtifacts,
    source_namespace: Option<[u8; 16]>,
    captured_rows: Option<u64>,
    canonical_metadata: Option<crate::column::RawCanonicalMetadata>,
    memory: Option<QueryMemoryBudget>,
}

/// Immutable canonical bits retaining their accounted encoded backing.
#[derive(Debug)]
pub struct CanonicalBitmap {
    data: QueryBuffer<u8>,
    bits_offset: usize,
    rows: u64,
}

impl CanonicalBitmap {
    pub fn len(&self) -> u64 {
        self.rows
    }
    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }
    pub fn is_present(&self, row: u64) -> bool {
        row < self.rows && self.data[self.bits_offset + (row / 8) as usize] & (1 << (row % 8)) != 0
    }
}

#[derive(Debug)]
pub(crate) enum InspectionPreflightError {
    LimitExceeded {
        resource: &'static str,
        required: u64,
        limit: u64,
    },
    Io(io::Error),
}

impl From<io::Error> for InspectionPreflightError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn inspection_charge(
    total: &mut u64,
    additional: u64,
    limit: u64,
    resource: &'static str,
) -> Result<(), InspectionPreflightError> {
    match total.checked_add(additional) {
        Some(required) if required <= limit => {
            *total = required;
            Ok(())
        }
        required => Err(InspectionPreflightError::LimitExceeded {
            resource,
            required: required.unwrap_or(u64::MAX),
            limit,
        }),
    }
}

#[derive(Debug)]
struct PageSelection {
    entry: PageIndexEntry,
    local_rows: QueryBuffer<usize>,
    output_positions: QueryBuffer<usize>,
}

enum BatchPayload<'a> {
    Raw(RawBytesColumn),
    Paged {
        descriptor: &'a ColumnDescriptor,
        entries: QueryBuffer<PageIndexEntry>,
    },
}

enum BatchLengths<'a> {
    Raw(RawFixedColumn<4>),
    Paged {
        descriptor: &'a ColumnDescriptor,
        entries: QueryBuffer<PageIndexEntry>,
        cached: Option<(PageIndexEntry, QueryBuffer<u32>)>,
    },
}

impl<'a> BatchLengths<'a> {
    fn new(reader: &'a SegmentReader, memory: Option<&QueryMemoryBudget>) -> io::Result<Self> {
        Ok(match reader.compacted_column("data_len") {
            Some(descriptor) => Self::Paged {
                descriptor,
                entries: reader.read_compacted_page_index_accounted(descriptor, None, memory)?,
                cached: None,
            },
            None => Self::Raw(reader.raw_fixed_accounted::<4>("data_len.col", None, memory)?),
        })
    }

    fn row(
        &mut self,
        reader: &SegmentReader,
        row: u64,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<u32> {
        let missing = || io::Error::new(io::ErrorKind::InvalidData, "data length row is missing");
        match self {
            Self::Raw(column) => column
                .values()
                .get(usize::try_from(row).map_err(|_| missing())?)
                .map(|bytes| u32::from_le_bytes(*bytes))
                .ok_or_else(missing),
            Self::Paged {
                descriptor,
                entries,
                cached,
            } => {
                if !cached.as_ref().is_some_and(|(entry, _)| {
                    row >= entry.first_row && row - entry.first_row < u64::from(entry.row_count)
                }) {
                    let index = entries.partition_point(|entry| {
                        entry.first_row + u64::from(entry.row_count) <= row
                    });
                    let entry = *entries.get(index).ok_or_else(missing)?;
                    if row < entry.first_row {
                        return Err(missing());
                    }
                    // No references into the old page survive this lookup.
                    // Release it before admitting the next page's buffers.
                    *cached = None;
                    let values = decode_u32_page_accounted(
                        &reader.read_page_payload_accounted(descriptor, &entry, memory)?,
                        entry.row_count as usize,
                        descriptor.codec,
                        memory,
                    )?;
                    *cached = Some((entry, values));
                }
                let (entry, values) = cached.as_ref().ok_or_else(missing)?;
                values
                    .get((row - entry.first_row) as usize)
                    .copied()
                    .ok_or_else(missing)
            }
        }
    }
}

struct PreparedVarBytes<'a> {
    payload: BatchPayload<'a>,
    lengths: BatchLengths<'a>,
}

impl<'a> PreparedVarBytes<'a> {
    fn new(reader: &'a SegmentReader, memory: Option<&QueryMemoryBudget>) -> io::Result<Self> {
        let payload = match reader.compacted_column("data") {
            Some(descriptor) => BatchPayload::Paged {
                descriptor,
                entries: reader.read_compacted_page_index_accounted(descriptor, None, memory)?,
            },
            None => {
                let data = if memory.is_some() {
                    reader.artifacts.read_accounted("data.col")?
                } else {
                    QueryBuffer::unaccounted(reader.artifacts.read("data.col")?)
                };
                let column = RawBytesColumn::from_accounted(&reader.dir.join("data.col"), data)?;
                if (column.row_count() as u64) < reader.read_row_count()? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "raw data does not cover the captured segment rows",
                    ));
                }
                BatchPayload::Raw(column)
            }
        };
        let lengths = BatchLengths::new(reader, memory)?;
        Ok(Self { payload, lengths })
    }

    fn next_batch(
        &mut self,
        reader: &SegmentReader,
        remaining: &mut &[u32],
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<Bytes>> {
        let count;
        let output = match &self.payload {
            BatchPayload::Raw(column) => {
                count = remaining.len().min(crate::page::MAX_PAGE_ROWS as usize);
                let mut validated_row = |row: u32| {
                    let bytes = column.row(row as usize)?;
                    if bytes.len() != self.lengths.row(reader, u64::from(row), memory)? as usize {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "data bytes differ from their row-length metadata",
                        ));
                    }
                    Ok(bytes)
                };
                if memory.is_some() {
                    for &row in &remaining[..count] {
                        validated_row(row)?;
                    }
                    column.materialize_accounted(Some(&remaining[..count]), None, memory)?
                } else {
                    // Maintenance callers retain independent per-row backing;
                    // only accounted query batches share a selected payload.
                    let mut output =
                        QueryBuffer::try_with_capacity(count, None, "variable result headers")?;
                    for &row in &remaining[..count] {
                        output.try_push(Bytes::copy_from_slice(validated_row(row)?))?;
                    }
                    output
                }
            }
            BatchPayload::Paged {
                descriptor,
                entries,
            } => {
                let row = u64::from(remaining[0]);
                let index = entries
                    .partition_point(|entry| entry.first_row + u64::from(entry.row_count) <= row);
                let entry = entries
                    .get(index)
                    .filter(|entry| entry.first_row <= row)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "selected data page is missing")
                    })?;
                let end = entry.first_row + u64::from(entry.row_count);
                count = remaining.partition_point(|&row| u64::from(row) < end);
                let mut expected = QueryBuffer::try_with_capacity(
                    entry.row_count as usize,
                    memory,
                    "variable batch companion lengths",
                )?;
                for row in entry.first_row..end {
                    expected.try_push(self.lengths.row(reader, row, memory)?)?;
                }
                let encoded = reader.read_page_payload_accounted(descriptor, entry, memory)?;
                if memory.is_some() {
                    let mut local_rows =
                        QueryBuffer::try_with_capacity(count, memory, "variable batch selection")?;
                    for &row in &remaining[..count] {
                        local_rows.try_push((u64::from(row) - entry.first_row) as usize)?;
                    }
                    decode_var_bytes_page_selected_accounted(
                        &encoded,
                        descriptor.codec,
                        &expected,
                        Some(&local_rows),
                        memory,
                    )?
                } else {
                    let page =
                        decode_var_bytes_page_bounded(&encoded, descriptor.codec, &expected)?;
                    let mut output =
                        QueryBuffer::try_with_capacity(count, None, "variable result headers")?;
                    for &row in &remaining[..count] {
                        output
                            .try_push(page[(u64::from(row) - entry.first_row) as usize].clone())?;
                    }
                    output
                }
            }
        };
        *remaining = &remaining[count..];
        Ok(output)
    }
}

struct PreparedLogColumns {
    addresses: Vec<Address>,
    block_numbers: Vec<u64>,
    block_hashes: Vec<B256>,
    timestamps: Vec<u64>,
    tx_hashes: Vec<B256>,
    tx_indices: Vec<u32>,
    log_indices: Vec<u32>,
    topic0s: Vec<Option<B256>>,
    topic1s: Vec<Option<B256>>,
    topic2s: Vec<Option<B256>>,
    topic3s: Vec<Option<B256>>,
    sources: Vec<Source>,
}

impl PreparedLogColumns {
    fn new(reader: &SegmentReader, row_ids: &[u32]) -> io::Result<Self> {
        let addresses = reader.read_address(Some(row_ids))?;
        let block_numbers = reader.read_u64("block_number", Some(row_ids))?;
        let block_hashes = reader.read_b256("block_hash", Some(row_ids))?;
        let timestamps = reader.read_u64("timestamp", Some(row_ids))?;
        let tx_hashes = reader.read_b256("tx_hash", Some(row_ids))?;
        let tx_indices = reader.read_u32("tx_index", Some(row_ids))?;
        let log_indices = reader.read_u32("log_index", Some(row_ids))?;
        let topic0s = reader.read_nullable_b256("topic0", Some(row_ids))?;
        let topic1s = reader.read_nullable_b256("topic1", Some(row_ids))?;
        let topic2s = reader.read_nullable_b256("topic2", Some(row_ids))?;
        let topic3s = reader.read_nullable_b256("topic3", Some(row_ids))?;
        let sources = reader
            .read_u8("source", Some(row_ids))?
            .into_iter()
            .map(|source| {
                Source::from_u8(source).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid source byte: {source}"),
                    )
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        if [
            addresses.len(),
            block_numbers.len(),
            block_hashes.len(),
            timestamps.len(),
            tx_hashes.len(),
            tx_indices.len(),
            log_indices.len(),
            topic0s.len(),
            topic1s.len(),
            topic2s.len(),
            topic3s.len(),
            sources.len(),
        ]
        .into_iter()
        .any(|len| len != row_ids.len())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "selected log columns have inconsistent row counts",
            ));
        }
        Ok(Self {
            addresses,
            block_numbers,
            block_hashes,
            timestamps,
            tx_hashes,
            tx_indices,
            log_indices,
            topic0s,
            topic1s,
            topic2s,
            topic3s,
            sources,
        })
    }
}

impl SegmentReader {
    pub(crate) fn generation(&self) -> u64 {
        self.manifest
            .as_ref()
            .map_or(0, |manifest| manifest.generation)
    }
    pub(crate) fn bundle_reference(&self) -> Option<&crate::bundle::BundleReference> {
        self.artifacts.bundle().map(|bundle| bundle.reference())
    }

    pub(crate) fn captured_manifest_identity(&self) -> Option<(u64, crate::native::SegmentKind)> {
        self.manifest
            .as_ref()
            .map(|manifest| (manifest.segment_id, manifest.kind))
    }
    /// Captured publication's logical-prefix commitment, checked against its
    /// canonical envelope for identified raw storage. Query capture validates
    /// metadata, not every row's content; recovery recomputes the saved prefix.
    /// Legacy data without a commitment remains readable but index-ineligible.
    pub fn source_commitment(&self) -> io::Result<Option<[u8; 32]>> {
        let commitment = match self.manifest.as_ref() {
            Some(manifest) => manifest.source_commitment,
            None if self.source_namespace.is_some() => {
                self.artifacts
                    .raw_canonical_metadata("canonical.bitmap")?
                    .validate_committed(None, self.captured_rows.unwrap_or(0))?
                    .commitment
            }
            None => None,
        };
        Ok(commitment.map(|root| root.0))
    }

    /// Stable storage-owned namespace retained across append and reopen.
    /// Full replacement rotates it; legacy sources can have no namespace.
    pub fn source_namespace(&self) -> Option<[u8; 16]> {
        self.source_namespace
    }

    pub fn open(dir: &Path) -> io::Result<Self> {
        Self::open_inner(dir, None)
    }

    /// Capture an offline source without opening symlinks or special artifacts.
    /// The caller must hold the offline directory lock for the complete read.
    pub(crate) fn open_for_inspection(dir: &Path) -> io::Result<Self> {
        Self::open_for_inspection_projected(dir, None)
    }

    /// Selective offline capture retains the same regular-file checks while
    /// allowing intact ownership metadata to be read beside damaged payloads.
    pub(crate) fn open_for_inspection_projected(
        dir: &Path,
        projection: Option<&[&str]>,
    ) -> io::Result<Self> {
        for relative in ["segment.json", crate::column::SOURCE_MARKER_FILE] {
            match crate::column_artifact::require_regular_artifact(dir, Path::new(relative)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Self::open_manifest_checked(dir, projection, load_manifest(dir)?, true)
    }

    /// Capture only the columns needed by a query, plus canonicality and row-count
    /// metadata. Include predicate, ordering and output columns, including those
    /// needed if an index is unavailable. Other columns may not be readable.
    pub fn open_projected(dir: &Path, columns: &[&str]) -> io::Result<Self> {
        Self::open_inner(dir, Some(columns))
    }

    /// Capture query artifacts with shared accounting retained by read results.
    /// Fixed and variable query reads retain their buffers through returned owners.
    pub fn open_projected_with_memory(
        dir: &Path,
        columns: &[&str],
        memory: QueryMemoryBudget,
    ) -> io::Result<Self> {
        Self::open_manifest_with_memory(
            dir,
            Some(columns),
            load_manifest(dir)?,
            false,
            Some(memory),
        )
    }

    /// Maintenance-only capture from an independently authoritative manifest.
    /// Caller retains directory ownership and validates its catalog binding.
    pub(crate) fn open_for_inspection_manifest(
        dir: &Path,
        manifest: SegmentManifest,
    ) -> io::Result<Self> {
        Self::open_manifest_checked(dir, None, Some(manifest), true)
    }

    pub(crate) fn open_recovering_prefix(owner: &PrefixRecoveryGuard) -> io::Result<Self> {
        let dir = owner.dir();
        verify_prefix_recovery_pending(dir, owner)?;
        let mut manifest = load_manifest(dir)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "verified prefix recovery requires its catalog-selected manifest",
            )
        })?;
        if manifest.source_namespace.map(|namespace| namespace.0) != Some(owner.namespace())
            || manifest.generation != owner.generation()
            || manifest.segment_id != owner.segment_id()
            || manifest.column_bundle.is_some()
            || manifest.row_count < owner.prefix_rows()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "prefix recovery manifest differs from authoritative source metadata",
            ));
        }
        manifest.validate_read_bounds()?;
        manifest.row_count = owner.prefix_rows();
        manifest.source_commitment = owner.commitment();
        let artifacts = ColumnArtifacts::open_projected(dir, Some(&manifest), None)?;
        let canonical_metadata = artifacts
            .raw_canonical_metadata("canonical.bitmap")?
            .validate_recovery(owner)?;
        verify_prefix_recovery_pending(dir, owner)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            manifest: Some(manifest),
            artifacts,
            source_namespace: Some(owner.namespace()),
            captured_rows: Some(owner.prefix_rows()),
            canonical_metadata: Some(canonical_metadata),
            memory: None,
        })
    }

    fn open_inner(dir: &Path, projection: Option<&[&str]>) -> io::Result<Self> {
        Self::open_manifest(dir, projection, load_manifest(dir)?)
    }

    fn open_manifest(
        dir: &Path,
        projection: Option<&[&str]>,
        manifest: Option<SegmentManifest>,
    ) -> io::Result<Self> {
        Self::open_manifest_checked(dir, projection, manifest, false)
    }

    fn open_manifest_checked(
        dir: &Path,
        projection: Option<&[&str]>,
        manifest: Option<SegmentManifest>,
        inspection: bool,
    ) -> io::Result<Self> {
        Self::open_manifest_with_memory(dir, projection, manifest, inspection, None)
    }

    fn open_manifest_with_memory(
        dir: &Path,
        projection: Option<&[&str]>,
        mut manifest: Option<SegmentManifest>,
        inspection: bool,
        memory: Option<QueryMemoryBudget>,
    ) -> io::Result<Self> {
        for _ in 0..3 {
            if let Some(manifest) = &manifest {
                manifest.validate_read_bounds()?;
            }
            let bundled = manifest
                .as_ref()
                .is_some_and(|manifest| manifest.column_bundle.is_some());
            let expected_post = manifest.as_ref().and_then(|manifest| {
                let namespace = manifest.source_namespace?;
                (manifest.column_bundle.is_none() && manifest.row_count != 0).then_some(
                    crate::column::SourceBinding {
                        namespace: namespace.0,
                        generation: manifest.generation,
                        segment_id: manifest.segment_id,
                    },
                )
            });
            let marker_before = if bundled || expected_post.is_some() {
                None
            } else {
                read_source_binding(dir)?
            };
            let source_namespace = match manifest.as_ref() {
                Some(manifest) => manifest.source_namespace.map(|namespace| namespace.0),
                None => marker_before.map(|binding| binding.namespace),
            };
            if !bundled
                && expected_post.is_none()
                && let (Some(manifest), Some(actual)) = (manifest.as_ref(), marker_before)
                && manifest.source_namespace.is_some()
                && (manifest.source_namespace.map(|namespace| namespace.0)
                    != Some(actual.namespace)
                    || manifest.generation != actual.generation
                    || manifest.segment_id != actual.segment_id)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw source namespace differs from the manifest",
                ));
            }
            let captured = if inspection {
                ColumnArtifacts::open_for_inspection(dir, manifest.as_ref(), projection)
            } else {
                ColumnArtifacts::open_projected_with_memory(
                    dir,
                    manifest.as_ref(),
                    projection,
                    memory.as_ref(),
                )
            };
            match captured {
                Ok(artifacts) => {
                    #[cfg(test)]
                    if let Some(hook) = AFTER_ARTIFACT_CAPTURE.with_borrow_mut(Option::take) {
                        hook();
                    }
                    let canonical_metadata = expected_post
                        .map(|expected| {
                            artifacts
                                .raw_canonical_metadata("canonical.bitmap")?
                                .validate_committed(
                                    Some(expected),
                                    manifest.as_ref().map_or(0, |manifest| manifest.row_count),
                                )?
                                .validate_capture_commitment(
                                    manifest
                                        .as_ref()
                                        .and_then(|manifest| manifest.source_commitment),
                                    manifest.as_ref().map_or(0, |manifest| manifest.row_count),
                                )
                        })
                        .transpose();
                    let canonical_metadata = match canonical_metadata {
                        Ok(metadata) => metadata,
                        Err(error) => {
                            let current = load_manifest(dir)?;
                            if current == manifest {
                                return Err(error);
                            }
                            manifest = current;
                            continue;
                        }
                    };
                    let after = if expected_post.is_some() {
                        None
                    } else if !bundled {
                        read_source_binding(dir)?
                    } else {
                        marker_before
                    };
                    if !bundled && after != marker_before {
                        return Err(io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "raw source changed during capture; retry the read",
                        ));
                    }
                    let mut reader = Self {
                        dir: dir.to_path_buf(),
                        manifest,
                        artifacts,
                        source_namespace,
                        captured_rows: None,
                        canonical_metadata,
                        memory: memory.clone(),
                    };
                    if reader.manifest.is_none() {
                        reader.captured_rows = Some(reader.read_row_count()?);
                    }
                    return Ok(reader);
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    // Publication can retire old names during capture. Retry only
                    // when there is evidence of a different manifest, never to
                    // conceal a file missing from the same committed generation.
                    let current = load_manifest(dir)?;
                    if current == manifest {
                        return Err(error);
                    }
                    manifest = current;
                }
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "segment changed repeatedly during capture; retry the read",
        ))
    }

    fn raw_fixed_accounted<const WIDTH: usize>(
        &self,
        path: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<RawFixedColumn<WIDTH>> {
        let visible = self
            .manifest
            .as_ref()
            .map(|m| m.row_count)
            .or(self.captured_rows);
        let requested = row_ids
            .and_then(|ids| ids.iter().max())
            .map(|&id| u64::from(id) + 1);
        if visible
            .zip(requested)
            .is_some_and(|(visible, required)| required > visible)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw selection exceeds captured rows",
            ));
        }
        let prefix = requested
            .or(visible)
            .map(usize::try_from)
            .transpose()
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw prefix exceeds address space",
                )
            })?;
        let data = if memory.is_some() {
            let rows = if row_ids.is_some_and(|ids| ids.is_empty()) {
                0
            } else {
                prefix.unwrap_or(0)
            };
            let length = rows
                .checked_mul(WIDTH)
                .and_then(|n| n.checked_add(ColumnFileHeader::SIZE))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "raw prefix length overflow")
                })?;
            return RawFixedColumn::from_accounted(
                &self.dir.join(path),
                self.artifacts
                    .read_range_accounted(path, 0..length as u64)?,
                Some(rows),
            );
        } else {
            QueryBuffer::unaccounted(self.artifacts.read(path)?)
        };
        RawFixedColumn::from_accounted(&self.dir.join(path), data, prefix)
    }

    pub fn read_address(&self, row_ids: Option<&[u32]>) -> io::Result<Vec<Address>> {
        self.read_address_core(row_ids, None)
            .map(|buffer| buffer.into_parts().0)
    }
    pub fn read_address_with_memory(
        &self,
        row_ids: Option<&[u32]>,
    ) -> io::Result<QueryBuffer<Address>> {
        self.read_address_core(row_ids, self.memory.as_ref())
    }
    fn read_address_core(
        &self,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<Address>> {
        if self.compacted_column("address").is_none() {
            return self
                .raw_fixed_accounted::<20>("address.col", row_ids, memory)?
                .materialize_accounted(row_ids, memory, |_, value| Address::from(*value));
        }
        self.read_fixed_typed::<20, Address>("address", row_ids, memory, |value| {
            Address::from(*value)
        })
    }

    pub fn read_b256(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<B256>> {
        self.read_b256_core(column, row_ids, None)
            .map(|buffer| buffer.into_parts().0)
    }
    pub fn read_b256_with_memory(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<QueryBuffer<B256>> {
        self.read_b256_core(column, row_ids, self.memory.as_ref())
    }
    fn read_b256_core(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<B256>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed_accounted::<32>(raw_column_path(column), row_ids, memory)?
                .materialize_accounted(row_ids, memory, |_, value| B256::from(*value));
        }
        self.read_fixed_typed::<32, B256>(column, row_ids, memory, |value| B256::from(*value))
    }

    pub fn read_u64(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u64>> {
        self.read_u64_core(column, row_ids, None)
            .map(|buffer| buffer.into_parts().0)
    }
    pub fn read_u64_with_memory(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<QueryBuffer<u64>> {
        self.read_u64_core(column, row_ids, self.memory.as_ref())
    }
    fn read_u64_core(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<u64>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed_accounted::<8>(raw_column_path(column), row_ids, memory)?
                .materialize_accounted(row_ids, memory, |_, value| u64::from_le_bytes(*value));
        }
        self.read_u64_values_accounted(column, row_ids, memory)
    }

    pub fn read_u32(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u32>> {
        self.read_u32_core(column, row_ids, None)
            .map(|buffer| buffer.into_parts().0)
    }
    pub fn read_u32_with_memory(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<QueryBuffer<u32>> {
        self.read_u32_core(column, row_ids, self.memory.as_ref())
    }
    fn read_u32_core(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<u32>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed_accounted::<4>(raw_column_path(column), row_ids, memory)?
                .materialize_accounted(row_ids, memory, |_, value| u32::from_le_bytes(*value));
        }
        self.read_u32_values_accounted(column, row_ids, memory)
    }

    pub fn read_u8(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u8>> {
        self.read_u8_core(column, row_ids, None)
            .map(|buffer| buffer.into_parts().0)
    }
    pub fn read_u8_with_memory(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<QueryBuffer<u8>> {
        self.read_u8_core(column, row_ids, self.memory.as_ref())
    }
    fn read_u8_core(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<u8>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed_accounted::<1>(raw_column_path(column), row_ids, memory)?
                .materialize_accounted(row_ids, memory, |_, value| value[0]);
        }
        self.read_u8_values_accounted(column, row_ids, memory)
    }

    pub fn read_nullable_b256(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<Vec<Option<B256>>> {
        self.read_nullable_b256_core(column, row_ids, None)
            .map(|buffer| buffer.into_parts().0)
    }
    pub fn read_nullable_b256_with_memory(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<QueryBuffer<Option<B256>>> {
        self.read_nullable_b256_core(column, row_ids, self.memory.as_ref())
    }
    fn read_nullable_b256_core(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<Option<B256>>> {
        if self.compacted_column(column).is_none() {
            let fixed =
                self.raw_fixed_accounted::<32>(&format!("{column}.col"), row_ids, memory)?;
            let path = format!("{column}.null");
            let required = fixed.values().len() as u64;
            let data = if memory.is_some() {
                self.artifacts
                    .read_range_accounted(&path, 0..8 + required.div_ceil(8))?
            } else {
                QueryBuffer::unaccounted(self.artifacts.read(&path)?)
            };
            let rows = validate_null_bytes(
                &data,
                self.artifacts.len(&path)?,
                required,
                memory.is_some()
                    || self.manifest.is_some()
                    || row_ids.is_some_and(|ids| !ids.is_empty()),
            )?;
            return fixed.materialize_accounted(row_ids, memory, |row, value| {
                bitmap_present(&data, rows, row as u64).then(|| B256::from(*value))
            });
        }
        let values =
            self.read_fixed_typed::<32, B256>(column, row_ids, memory, |value| B256::from(*value))?;
        let descriptor = self.compacted_column(column).unwrap();
        let path = descriptor.null_bitmap_path.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "nullable column is missing null bitmap",
            )
        })?;
        let data = if memory.is_some() {
            self.artifacts.read_accounted(path)?
        } else {
            QueryBuffer::unaccounted(self.artifacts.read(path)?)
        };
        let rows = validate_null_bytes(&data, data.len() as u64, self.read_row_count()?, true)?;
        self.validate_bitmap_rows(rows)?;
        let mut result =
            QueryBuffer::try_with_capacity(values.len(), memory, "nullable fixed output")?;
        for (position, value) in values.iter().enumerate() {
            let row = row_ids.map_or(position as u64, |ids| u64::from(ids[position]));
            result.try_push(bitmap_present(&data, rows, row).then_some(*value))?;
        }
        Ok(result)
    }

    pub fn read_var_bytes(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<Bytes>> {
        self.read_var_bytes_with_lengths(column, row_ids)
            .map(|(values, _)| values)
    }

    /// Read selected variable values with both result headers and shared payload
    /// backing accounted until their respective final owners are dropped.
    pub fn read_var_bytes_with_memory(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<QueryBuffer<Bytes>> {
        let Some(memory) = self.memory.as_ref() else {
            return self
                .read_var_bytes(column, row_ids)
                .map(QueryBuffer::unaccounted);
        };
        if self.compacted_column(column).is_none() {
            let path = raw_column_path(column);
            let source = RawBytesColumn::from_accounted(
                &self.dir.join(path),
                self.artifacts.read_accounted(path)?,
            )?;
            let lengths = self.read_u32_with_memory("data_len", row_ids)?;
            let values = source.materialize_accounted(
                row_ids,
                self.manifest
                    .as_ref()
                    .map(|manifest| manifest.row_count)
                    .or(self.captured_rows),
                Some(memory),
            )?;
            validate_data_lengths(&values, &lengths)?;
            return Ok(values);
        }
        let descriptor = self.compacted_column(column).unwrap();
        let index = self.read_compacted_page_index_accounted(descriptor, row_ids, Some(memory))?;
        match row_ids {
            Some(ids) => {
                self.read_selected_var_bytes_pages_accounted(descriptor, ids, &index, memory)
            }
            None => {
                let lengths = self.read_u32_with_memory("data_len", None)?;
                let mut values = QueryBuffer::try_with_capacity(
                    lengths.len(),
                    Some(memory),
                    "variable result headers",
                )?;
                for entry in index.iter() {
                    let start = usize::try_from(entry.first_row).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "data row offset overflow")
                    })?;
                    let end = start.checked_add(entry.row_count as usize).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                    })?;
                    let expected = lengths.get(start..end).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "data length rows differ")
                    })?;
                    let encoded =
                        self.read_page_payload_accounted(descriptor, entry, Some(memory))?;
                    let page = decode_var_bytes_page_selected_accounted(
                        &encoded,
                        descriptor.codec,
                        expected,
                        None,
                        Some(memory),
                    )?;
                    values.try_extend_from_slice(&page)?;
                }
                if values.len() != lengths.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "decoded data rows differ from data lengths",
                    ));
                }
                Ok(values)
            }
        }
    }

    fn read_selected_var_bytes_pages_accounted(
        &self,
        descriptor: &ColumnDescriptor,
        ids: &[u32],
        index: &[PageIndexEntry],
        memory: &QueryMemoryBudget,
    ) -> io::Result<QueryBuffer<Bytes>> {
        if ids.is_empty() {
            return QueryBuffer::try_with_capacity(0, Some(memory), "variable result headers");
        }
        let selections = build_selections_accounted(ids, index, Some(memory))?;
        let coverage_len = selections.iter().try_fold(0usize, |total, selection| {
            total
                .checked_add(selection.entry.row_count as usize)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                })
        })?;
        let mut coverage =
            QueryBuffer::try_with_capacity(coverage_len, Some(memory), "variable page coverage")?;
        for selection in selections.iter() {
            let end = selection
                .entry
                .first_row
                .checked_add(u64::from(selection.entry.row_count))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                })?;
            for row in selection.entry.first_row..end {
                coverage.try_push(u32::try_from(row).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row exceeds addressing")
                })?)?;
            }
        }
        let lengths = self.read_u32_with_memory("data_len", Some(&coverage))?;
        if lengths.len() != coverage.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "data length rows differ",
            ));
        }
        let mut values =
            QueryBuffer::try_with_capacity(ids.len(), Some(memory), "variable result headers")?;
        values.try_resize(ids.len(), Bytes::new())?;
        let mut cursor = 0usize;
        for selection in selections.iter() {
            let end = cursor
                .checked_add(selection.entry.row_count as usize)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                })?;
            let expected = lengths.get(cursor..end).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "data length rows differ")
            })?;
            let encoded =
                self.read_page_payload_accounted(descriptor, &selection.entry, Some(memory))?;
            let page = decode_var_bytes_page_selected_accounted(
                &encoded,
                descriptor.codec,
                expected,
                Some(&selection.local_rows),
                Some(memory),
            )?;
            if page.len() != selection.output_positions.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "selected data rows differ",
                ));
            }
            for (value, &position) in page.iter().zip(selection.output_positions.iter()) {
                values[position] = value.clone();
            }
            cursor = end;
        }
        Ok(values)
    }

    /// Read strictly increasing, unique selected data rows in bounded batches.
    /// Raw columns are loaded once and copied in groups of at most 16,384 rows;
    /// compacted columns yield one selected physical page per batch. Payload and
    /// companion-length initialization is deferred until the first `next()`, so
    /// callers can check cancellation before any batch's payload I/O. Raw file
    /// buffers and page indexes remain owned until the iterator is dropped.
    /// Preparation validates each complete captured page index; decoded data
    /// pages are checked against every companion length, including unselected rows.
    pub fn var_bytes_batches<'a>(
        &'a self,
        column: &str,
        row_ids: &'a [u32],
    ) -> io::Result<impl Iterator<Item = io::Result<Vec<Bytes>>> + 'a> {
        Ok(self
            .var_bytes_batches_core(column, row_ids, None)?
            .map(|batch| batch.map(|values| values.into_parts().0)))
    }

    /// As `var_bytes_batches`, with the reader's query budget retained by raw
    /// buffers, page indexes and the current companion-length page. Each batch
    /// owns its result headers; payload aliases retain their shared backing even
    /// after the batch, iterator and reader are dropped. Preparation remains lazy.
    /// Readers opened without a budget return unaccounted buffers.
    pub fn var_bytes_batches_with_memory<'a>(
        &'a self,
        column: &str,
        row_ids: &'a [u32],
    ) -> io::Result<impl Iterator<Item = io::Result<QueryBuffer<Bytes>>> + 'a> {
        self.var_bytes_batches_core(column, row_ids, self.memory.as_ref())
    }

    fn var_bytes_batches_core<'a>(
        &'a self,
        column: &str,
        row_ids: &'a [u32],
        memory: Option<&'a QueryMemoryBudget>,
    ) -> io::Result<impl Iterator<Item = io::Result<QueryBuffer<Bytes>>> + 'a> {
        let visible = self.read_row_count()?;
        if column != "data"
            || row_ids.windows(2).any(|pair| pair[0] >= pair[1])
            || row_ids.last().is_some_and(|&row| u64::from(row) >= visible)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data batches require increasing unique rows within the captured segment",
            ));
        }
        let mut remaining = row_ids;
        let mut prepared = None;
        Ok(std::iter::from_fn(move || {
            if remaining.is_empty() {
                return None;
            }
            let result = (|| {
                if prepared.is_none() {
                    prepared = Some(PreparedVarBytes::new(self, memory)?);
                }
                prepared.as_mut().expect("initialized above").next_batch(
                    self,
                    &mut remaining,
                    memory,
                )
            })();
            if result.is_err() {
                remaining = &[];
                prepared = None;
            }
            Some(result)
        }))
    }

    fn read_var_bytes_with_lengths(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<(Vec<Bytes>, Vec<u32>)> {
        if self.compacted_column(column).is_none() {
            let path = raw_column_path(column);
            let values =
                RawBytesColumn::from_bytes(&self.dir.join(path), self.artifacts.read(path)?)?
                    .materialize(
                        row_ids,
                        self.manifest
                            .as_ref()
                            .map(|m| m.row_count)
                            .or(self.captured_rows),
                    )?;
            let lengths = self.read_u32("data_len", row_ids)?;
            validate_data_lengths(&values, &lengths)?;
            return Ok((values, lengths));
        }
        self.read_var_bytes_values(column, row_ids)
    }

    pub fn read_canonical(&self) -> io::Result<NullBitmap> {
        let data = self.artifacts.read(self.canonical_relative_path())?;
        let bitmap = if self.artifacts.bundle().is_some() {
            NullBitmap::read_from(&data).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap")
            })?
        } else if let Some(metadata) = self.canonical_metadata {
            metadata.bitmap(&data)?
        } else {
            crate::column::read_canonical_bitmap(&data, None)?
        };
        self.validate_bitmap_rows(bitmap.len())?;
        Ok(bitmap)
    }

    /// Read canonicality from a budgeted capture, retaining encoded bytes rather
    /// than cloning a second bitmap. Padding remains inaccessible past `len()`.
    pub fn read_canonical_accounted(&self) -> io::Result<CanonicalBitmap> {
        if self.memory.is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "canonical query reads require a reader captured with memory accounting",
            ));
        }
        let data = self
            .artifacts
            .read_accounted(self.canonical_relative_path())?;
        let range = if self.artifacts.bundle().is_some() {
            0..data.len()
        } else {
            let metadata = match self.canonical_metadata {
                Some(metadata) => metadata,
                None => crate::column::RawCanonicalMetadata::parse(&data, data.len() as u64)?
                    .validate_committed(None, 0)?,
            };
            metadata.bitmap_range(&data)?
        };
        let bytes = &data[range.clone()];
        let rows = bytes
            .get(..8)
            .and_then(|x| x.try_into().ok())
            .map(u64::from_le_bytes)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap")
            })?;
        let required = usize::try_from(rows.div_ceil(8))
            .ok()
            .and_then(|n| n.checked_add(8))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical bitmap length overflow",
                )
            })?;
        if bytes.len() < required {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated canonical bitmap",
            ));
        }
        self.validate_bitmap_rows(rows)?;
        Ok(CanonicalBitmap {
            data,
            bits_offset: range.start + 8,
            rows,
        })
    }

    /// Bound the encoded canonical artifact before allocating its body. Raw
    /// lengths are captured on the same pinned files; bundled lengths are part
    /// of their immutable table. The decoded bitmap is additionally row-bounded
    /// by the caller before this method is used.
    pub(crate) fn read_canonical_for_inspection(
        &mut self,
        max_artifact_bytes: u64,
    ) -> io::Result<NullBitmap> {
        self.artifacts.inspection_artifact_lengths()?;
        let length = self.artifacts.len(self.canonical_relative_path())?;
        if length > max_artifact_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "canonical artifact requires {length} bytes; limit is {max_artifact_bytes}"
                ),
            ));
        }
        self.read_canonical()
    }

    pub fn read_canonical_len(&self) -> io::Result<u64> {
        if self.artifacts.bundle().is_some() {
            return self.read_canonical().map(|bitmap| bitmap.len());
        }
        let metadata = match self.canonical_metadata {
            Some(metadata) => metadata,
            None => self
                .artifacts
                .raw_canonical_metadata(self.canonical_relative_path())?
                .validate_committed(None, self.read_row_count()?)?,
        };
        self.validate_bitmap_rows(metadata.rows)?;
        Ok(metadata.rows)
    }

    fn validate_bitmap_rows(&self, rows: u64) -> io::Result<()> {
        // An unbundled bitmap can include a newer append, but every bit in this
        // reader's captured row boundary must exist. Missing bits are corruption,
        // not null/noncanonical values.
        if rows < self.read_row_count()? {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bitmap does not cover the captured segment rows",
            ));
        }
        Ok(())
    }

    pub fn read_row_count(&self) -> io::Result<u64> {
        if let Some(manifest) = &self.manifest {
            Ok(manifest.row_count)
        } else if let Some(captured_rows) = self.captured_rows {
            Ok(captured_rows)
        } else {
            let data = self
                .artifacts
                .read_range_accounted("address.col", 0..ColumnFileHeader::SIZE as u64)?;
            crate::reader::raw_address_row_count(&data, self.artifacts.len("address.col")?)
        }
    }

    pub fn read_log_rows(&self, row_ids: Option<&[u32]>) -> io::Result<Vec<LogRow>> {
        self.materialize_log_rows(row_ids, None)
    }

    /// Materialize an explicit query batch with source columns, output rows and
    /// nested payload backing charged to this reader's captured memory budget.
    /// The reader must be opened with `open_projected_with_memory` and include
    /// all log columns. Selection order and duplicate IDs are preserved.
    /// Cancellation is checked between column reads and every 256 assembled rows;
    /// a single admitted column read/decode is a synchronous boundary.
    pub fn read_log_rows_with_memory(
        &self,
        row_ids: &[u32],
        cancel: Option<&dyn Fn() -> bool>,
    ) -> io::Result<QueryBuffer<LogRow>> {
        let memory = self.memory.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "accounted log rows require a budgeted reader",
            )
        })?;
        check_log_materialization_cancel(cancel)?;
        if row_ids.is_empty() {
            return QueryBuffer::try_with_capacity(0, Some(memory), "native log rows");
        }
        self.materialize_log_rows_core(Some(row_ids), None, Some(memory), cancel)
    }

    /// Preflight only the fixed repair routing columns. No data payload or
    /// companion data-length column is read. The reader must be offline-projected
    /// to these columns under the maintenance directory owner.
    pub(crate) fn repair_routing_preflight(&mut self, limit: u64) -> io::Result<()> {
        const COLUMNS: [(&str, u64); 4] = [
            ("block_number", 8),
            ("block_hash", 32),
            ("log_index", 4),
            ("source", 1),
        ];
        let mut total = 0u64;
        let mut charge = |bytes: u64| -> io::Result<()> {
            total = total
                .checked_add(bytes)
                .filter(|&sum| sum <= limit)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "repair routing artifacts exceed byte budget",
                    )
                })?;
            Ok(())
        };
        if let Some(bundle) = self.artifacts.bundle() {
            charge(u64::from(bundle.reference().chain_bytes))?;
            for (name, _) in COLUMNS {
                let descriptor = self.compacted_column(name).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing bundled routing column")
                })?;
                charge(self.artifacts.len(&descriptor.data_path)?)?;
                let index = descriptor.page_index_path.as_ref().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing routing page index")
                })?;
                charge(self.artifacts.len(index)?)?;
                charge(crate::page::PAGE_INDEX_HEADER_BYTES as u64)?;
            }
        } else {
            // Capture lengths before full raw reads, including the separately
            // captured canonical envelope. Subsequent reads reject length changes.
            for bytes in self.artifacts.inspection_artifact_lengths()? {
                charge(bytes)?;
            }
        }
        let rows = self.read_row_count()?;
        for (name, width) in COLUMNS {
            if let Some(descriptor) = self.compacted_column(name) {
                self.read_compacted_page_index(descriptor, None)?;
                continue;
            }
            let path = raw_column_path(name);
            let bytes = self
                .artifacts
                .read_range(path, 0..ColumnFileHeader::SIZE as u64)?;
            let header = ColumnFileHeader::read_from(&bytes).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid raw routing header")
            })?;
            let expected = rows
                .checked_mul(width)
                .and_then(|bytes| bytes.checked_add(ColumnFileHeader::SIZE as u64))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "raw routing size overflow")
                })?;
            if header.version != crate::column::COLUMN_VERSION
                || header.compression != 0
                || header.row_count != rows
                || self.artifacts.len(path)? != expected
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw routing shape differs from captured rows",
                ));
            }
        }
        Ok(())
    }

    /// Screen a locked offline inspection before row IDs, retained raw buffers,
    /// or data payloads are materialized. These caller-supplied allowances are
    /// not query limits or a total RSS bound. Decoded bytes include page framing.
    pub(crate) fn inspection_preflight(
        &mut self,
        max_retained_artifact_bytes: u64,
        max_decoded_payload_bytes: u64,
    ) -> Result<(), InspectionPreflightError> {
        let mut artifact_bytes = 0;
        for length in self.artifacts.inspection_artifact_lengths()? {
            inspection_charge(
                &mut artifact_bytes,
                length,
                max_retained_artifact_bytes,
                "retained_artifact_bytes",
            )?;
        }
        let rows = self.read_row_count()?;
        self.inspection_validate_raw_shapes(rows)?;
        if rows == 0 {
            if let Some(manifest) = &self.manifest {
                for descriptor in &manifest.columns {
                    let Some(index_path) = &descriptor.page_index_path else {
                        continue;
                    };
                    if !read_page_index(&self.artifacts.read(index_path)?)?.is_empty()
                        || self.artifacts.len(&descriptor.data_path)? != 0
                    {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "empty segment has nonempty paged artifacts",
                        )
                        .into());
                    }
                    if let Some(path) = &descriptor.null_bitmap_path {
                        let bytes = self.artifacts.read(path)?;
                        if bytes != [0; 8] {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "empty segment has invalid null bitmap",
                            )
                            .into());
                        }
                    }
                }
            }
            return Ok(());
        }
        let mut lengths = BatchLengths::new(self, None)?;
        let mut decoded = 0;
        // Every companion length is checked before even looking at a data
        // payload's codec tag. Length pages have fixed, format-bound decoding.
        for row in 0..rows {
            inspection_charge(
                &mut decoded,
                u64::from(lengths.row(self, row, None)?),
                max_decoded_payload_bytes,
                "decoded_payload_bytes",
            )?;
        }
        if let Some(descriptor) = self.compacted_column("data") {
            let entries = self.read_compacted_page_index(descriptor, None)?;
            let stream_len = self.artifacts.len(&descriptor.data_path)?;
            for entry in entries {
                let end = entry
                    .offset
                    .checked_add(u64::from(entry.encoded_len))
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "inspection page range overflow")
                    })?;
                if end > stream_len {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "inspection data page exceeds its artifact",
                    )
                    .into());
                }
                let offset_width = match descriptor.codec {
                    CompressionCodec::None | CompressionCodec::Zstd | CompressionCodec::Lz4 => 8,
                    CompressionCodec::AdaptiveBytes => {
                        // This format byte selects the persisted offset width;
                        // never trust a compression frame's claimed decoded size.
                        let tag = self
                            .artifacts
                            .read_range(&descriptor.data_path, entry.offset..entry.offset + 1)?;
                        match tag.as_slice() {
                            [0] => 8,
                            [1] => 4,
                            _ => {
                                return Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "invalid adaptive bytes tag",
                                )
                                .into());
                            }
                        }
                    }
                    _ => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid inspection data codec",
                        )
                        .into());
                    }
                };
                let framing = (u64::from(entry.row_count) + 1) * offset_width + 4;
                inspection_charge(
                    &mut decoded,
                    framing,
                    max_decoded_payload_bytes,
                    "decoded_payload_bytes",
                )?;
            }
        } else {
            let expected = rows
                .checked_add(1)
                .and_then(|count| count.checked_mul(8))
                .and_then(|offsets| offsets.checked_add(ColumnFileHeader::SIZE as u64))
                .and_then(|metadata| metadata.checked_add(decoded))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "raw data length overflow")
                })?;
            if self.artifacts.len("data.col")? != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw data bytes differ from companion lengths",
                )
                .into());
            }
        }
        Ok(())
    }

    fn inspection_validate_raw_shapes(&self, rows: u64) -> io::Result<()> {
        if self.artifacts.bundle().is_some() {
            return Ok(());
        }
        for (name, width) in [
            ("address", 20),
            ("block_number", 8),
            ("block_hash", 32),
            ("timestamp", 8),
            ("tx_hash", 32),
            ("tx_index", 4),
            ("log_index", 4),
            ("data_len", 4),
            ("source", 1),
            ("topic0", 32),
            ("topic1", 32),
            ("topic2", 32),
            ("topic3", 32),
            ("data", 0),
        ] {
            if self.compacted_column(name).is_some() {
                continue;
            }
            let path = format!("{name}.col");
            let len = match self.artifacts.len(&path) {
                Ok(len) => len,
                Err(error) if rows == 0 && error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let bytes = self
                .artifacts
                .read_range(&path, 0..ColumnFileHeader::SIZE as u64)?;
            let header = ColumnFileHeader::read_from(&bytes).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid inspection raw header")
            })?;
            if header.version != crate::column::COLUMN_VERSION
                || header.compression != 0
                || header.row_count != rows
                || (width != 0 && len != ColumnFileHeader::SIZE as u64 + rows * width)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw artifact differs from captured row count",
                ));
            }
            if name == "data" && rows == 0 {
                let start = ColumnFileHeader::SIZE as u64;
                if len != start + 8 || self.artifacts.read_range(&path, start..start + 8)? != [0; 8]
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid empty raw data offsets",
                    ));
                }
            }
        }
        for topic in 0..4 {
            let name = format!("topic{topic}");
            if self.compacted_column(&name).is_some() {
                continue;
            }
            let path = format!("{name}.null");
            let len = match self.artifacts.len(&path) {
                Ok(len) => len,
                Err(error) if rows == 0 && error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(error),
            };
            let header = self.artifacts.read_range(&path, 0..8)?;
            if len != 8 + rows.div_ceil(8) || header.as_slice() != rows.to_le_bytes() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw null bitmap differs from captured rows",
                ));
            }
        }
        match self
            .artifacts
            .raw_canonical_metadata(self.canonical_relative_path())
        {
            Ok(metadata) => {
                metadata
                    .validate_committed(None, rows)?
                    .validate_exact_len(self.artifacts.len(self.canonical_relative_path())?)?;
            }
            Err(error) if rows == 0 && error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    /// Prepare selected fixed metadata once and materialize one payload page or
    /// raw row group at a time. This bounds logical batches, not bytes or RSS:
    /// selected metadata and the payload iterator's raw buffers/page indexes are
    /// retained for the lifetime of this iterator. Payload I/O remains lazy.
    pub(crate) fn log_row_batches<'a>(
        &'a self,
        row_ids: &'a [u32],
    ) -> io::Result<impl Iterator<Item = io::Result<Vec<LogRow>>> + 'a> {
        let mut payloads = self.var_bytes_batches("data", row_ids)?;
        let columns = if row_ids.is_empty() {
            None
        } else {
            Some(PreparedLogColumns::new(self, row_ids)?)
        };
        let mut position = 0usize;
        let mut finished = false;
        Ok(std::iter::from_fn(move || {
            if finished {
                return None;
            }
            let payload = match payloads.next() {
                Some(payload) => payload,
                None => {
                    finished = true;
                    return (position != row_ids.len()).then(|| {
                        Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "selected log payload rows are missing",
                        ))
                    });
                }
            };
            let result = (|| {
                let data = payload?;
                let end = position
                    .checked_add(data.len())
                    .filter(|&end| !data.is_empty() && end <= row_ids.len())
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "selected log payload rows differ",
                        )
                    })?;
                let columns = columns.as_ref().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "selected log metadata is missing",
                    )
                })?;
                let mut rows = Vec::new();
                rows.try_reserve_exact(data.len())
                    .map_err(io::Error::other)?;
                for (index, data) in (position..end).zip(data) {
                    let data_len = u32::try_from(data.len()).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "selected log data length exceeds u32",
                        )
                    })?;
                    rows.push(LogRow {
                        address: columns.addresses[index],
                        block_number: columns.block_numbers[index],
                        block_hash: columns.block_hashes[index],
                        timestamp: columns.timestamps[index],
                        tx_hash: columns.tx_hashes[index],
                        tx_index: columns.tx_indices[index],
                        log_index: columns.log_indices[index],
                        topic0: columns.topic0s[index],
                        topic1: columns.topic1s[index],
                        topic2: columns.topic2s[index],
                        topic3: columns.topic3s[index],
                        source: columns.sources[index],
                        data,
                        data_len,
                    });
                }
                position = end;
                Ok(rows)
            })();
            if result.is_err() {
                finished = true;
            }
            Some(result)
        }))
    }

    /// Materialize a small maintenance candidate without trusting compressed
    /// frame sizes or allocating its complete data stream up front.
    /// `lengths` is the complete data_len column already read from this reader
    /// while screening the candidate's payload budget.
    pub(crate) fn read_log_rows_bounded(
        &self,
        budget: usize,
        lengths: Vec<u32>,
    ) -> io::Result<Vec<LogRow>> {
        let descriptor = self.compacted_column("data").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "bounded read requires paged data",
            )
        })?;
        let expected_rows = self
            .manifest
            .as_ref()
            .and_then(|manifest| usize::try_from(manifest.row_count).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "bounded row count is invalid")
            })?;
        if lengths.len() != expected_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "data length rows differ from the segment",
            ));
        }
        lengths
            .iter()
            .try_fold(0usize, |sum, &len| sum.checked_add(len as usize))
            .filter(|&sum| sum <= budget)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "data exceeds maintenance budget",
                )
            })?;
        let mut data = Vec::new();
        for entry in self.read_compacted_page_index(descriptor, None)? {
            let start = usize::try_from(entry.first_row).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "data row offset overflow")
            })?;
            let end = start.checked_add(entry.row_count as usize).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
            })?;
            let lengths = lengths.get(start..end).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "data length rows differ")
            })?;
            let page = decode_var_bytes_page_bounded(
                &self.read_page_payload(descriptor, &entry)?,
                descriptor.codec,
                lengths,
            )?;
            data.extend(page);
        }
        self.materialize_log_rows(None, Some((data, lengths)))
    }

    fn materialize_log_rows(
        &self,
        row_ids: Option<&[u32]>,
        data: Option<(Vec<Bytes>, Vec<u32>)>,
    ) -> io::Result<Vec<LogRow>> {
        self.materialize_log_rows_core(row_ids, data, None, None)
            .map(|rows| rows.into_parts().0)
    }

    fn materialize_log_rows_core(
        &self,
        row_ids: Option<&[u32]>,
        data: Option<(Vec<Bytes>, Vec<u32>)>,
        memory: Option<&QueryMemoryBudget>,
        cancel: Option<&dyn Fn() -> bool>,
    ) -> io::Result<QueryBuffer<LogRow>> {
        macro_rules! read_column {
            ($read:expr) => {{
                check_log_materialization_cancel(cancel)?;
                $read?
            }};
        }
        let addresses = read_column!(self.read_address_core(row_ids, memory));
        let block_numbers = read_column!(self.read_u64_core("block_number", row_ids, memory));
        let block_hashes = read_column!(self.read_b256_core("block_hash", row_ids, memory));
        let timestamps = read_column!(self.read_u64_core("timestamp", row_ids, memory));
        let tx_hashes = read_column!(self.read_b256_core("tx_hash", row_ids, memory));
        let tx_indices = read_column!(self.read_u32_core("tx_index", row_ids, memory));
        let log_indices = read_column!(self.read_u32_core("log_index", row_ids, memory));
        let topic0s = read_column!(self.read_nullable_b256_core("topic0", row_ids, memory));
        let topic1s = read_column!(self.read_nullable_b256_core("topic1", row_ids, memory));
        let topic2s = read_column!(self.read_nullable_b256_core("topic2", row_ids, memory));
        let topic3s = read_column!(self.read_nullable_b256_core("topic3", row_ids, memory));
        let (data, data_lens) = match data {
            Some((values, lengths)) => (QueryBuffer::unaccounted(values), Some(lengths)),
            // The accounted variable reader validates selected raw lengths and
            // every decoded page's companion lengths before returning Bytes.
            // Derive LogRow lengths from those validated values, avoiding a
            // second companion read and a redundant output vector.
            None if memory.is_some() => (
                read_column!(self.read_var_bytes_with_memory("data", row_ids)),
                None,
            ),
            None => {
                let (values, lengths) = self.read_var_bytes_with_lengths("data", row_ids)?;
                (QueryBuffer::unaccounted(values), Some(lengths))
            }
        };
        let sources = read_column!(self.read_u8_core("source", row_ids, memory));

        if [
            block_numbers.len(),
            block_hashes.len(),
            timestamps.len(),
            tx_hashes.len(),
            tx_indices.len(),
            log_indices.len(),
            topic0s.len(),
            topic1s.len(),
            topic2s.len(),
            topic3s.len(),
            data.len(),
            sources.len(),
        ]
        .into_iter()
        .any(|len| len != addresses.len())
            || data_lens.as_ref().is_some_and(|lengths| {
                lengths.len() != addresses.len()
                    || data
                        .iter()
                        .zip(lengths)
                        .any(|(bytes, &len)| bytes.len() as u64 != u64::from(len))
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "log columns have inconsistent row or data lengths",
            ));
        }

        check_log_materialization_cancel(cancel)?;
        let mut rows = QueryBuffer::try_with_capacity(addresses.len(), memory, "native log rows")?;
        for index in 0..addresses.len() {
            if index % 256 == 0 {
                check_log_materialization_cancel(cancel)?;
            }
            rows.try_push(LogRow {
                block_number: block_numbers[index],
                block_hash: block_hashes[index],
                timestamp: timestamps[index],
                tx_hash: tx_hashes[index],
                tx_index: tx_indices[index],
                log_index: log_indices[index],
                address: addresses[index],
                topic0: topic0s[index],
                topic1: topic1s[index],
                topic2: topic2s[index],
                topic3: topic3s[index],
                data: data[index].clone(),
                data_len: match &data_lens {
                    Some(lengths) => lengths[index],
                    None => u32::try_from(data[index].len()).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "log data length exceeds u32")
                    })?,
                },
                source: Source::from_u8(sources[index]).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid source byte: {}", sources[index]),
                    )
                })?,
            })?;
        }
        check_log_materialization_cancel(cancel)?;
        Ok(rows)
    }

    fn compacted_column(&self, name: &str) -> Option<&ColumnDescriptor> {
        self.manifest
            .as_ref()?
            .columns
            .iter()
            .find(|column| column.name == name && column.page_index_path.is_some())
    }

    fn canonical_relative_path(&self) -> &str {
        self.manifest
            .as_ref()
            .map_or("canonical.bitmap", |manifest| &manifest.canonical_rows_path)
    }

    fn read_fixed_typed<const WIDTH: usize, T: Copy + Default>(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
        decode: impl Fn(&[u8; WIDTH]) -> T,
    ) -> io::Result<QueryBuffer<T>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let index = self.read_compacted_page_index_accounted(descriptor, row_ids, memory)?;
        let complete = (row_ids.is_none() && memory.is_none())
            .then(|| self.artifacts.read(&descriptor.data_path))
            .transpose()?;
        read_selected_pages_accounted(row_ids, &index, memory, |entry| {
            let payload;
            let bytes = if let Some(data) = complete.as_ref() {
                self.slice_page(data, entry)?
            } else {
                payload = self.read_page_payload_accounted(descriptor, entry, memory)?;
                &payload
            };
            let page = decode_fixed_width_page_accounted(
                bytes,
                entry.row_count as usize,
                WIDTH,
                descriptor.codec,
                memory,
            )?;
            let mut values = QueryBuffer::try_with_capacity(
                entry.row_count as usize,
                memory,
                "fixed decoded values",
            )?;
            for value in page.as_chunks::<WIDTH>().0 {
                values.try_push(decode(value))?;
            }
            Ok(values)
        })
    }

    fn read_u64_values_accounted(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<u64>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let index = self.read_compacted_page_index_accounted(descriptor, row_ids, memory)?;
        let complete = (row_ids.is_none() && memory.is_none())
            .then(|| self.artifacts.read(&descriptor.data_path))
            .transpose()?;
        read_selected_pages_accounted(row_ids, &index, memory, |entry| {
            if let Some(data) = complete.as_ref() {
                decode_u64_page_accounted(
                    self.slice_page(data, entry)?,
                    entry.row_count as usize,
                    descriptor.codec,
                    memory,
                )
            } else {
                decode_u64_page_accounted(
                    &self.read_page_payload_accounted(descriptor, entry, memory)?,
                    entry.row_count as usize,
                    descriptor.codec,
                    memory,
                )
            }
        })
    }

    fn read_u32_values_accounted(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<u32>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let index = self.read_compacted_page_index_accounted(descriptor, row_ids, memory)?;
        let complete = (row_ids.is_none() && memory.is_none())
            .then(|| self.artifacts.read(&descriptor.data_path))
            .transpose()?;
        read_selected_pages_accounted(row_ids, &index, memory, |entry| {
            if let Some(data) = complete.as_ref() {
                decode_u32_page_accounted(
                    self.slice_page(data, entry)?,
                    entry.row_count as usize,
                    descriptor.codec,
                    memory,
                )
            } else {
                decode_u32_page_accounted(
                    &self.read_page_payload_accounted(descriptor, entry, memory)?,
                    entry.row_count as usize,
                    descriptor.codec,
                    memory,
                )
            }
        })
    }

    fn read_u8_values_accounted(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<u8>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let index = self.read_compacted_page_index_accounted(descriptor, row_ids, memory)?;
        let complete = (row_ids.is_none() && memory.is_none())
            .then(|| self.artifacts.read(&descriptor.data_path))
            .transpose()?;
        read_selected_pages_accounted(row_ids, &index, memory, |entry| {
            if let Some(data) = complete.as_ref() {
                decode_u8_page_accounted(
                    self.slice_page(data, entry)?,
                    entry.row_count as usize,
                    descriptor.codec,
                    memory,
                )
            } else {
                decode_u8_page_accounted(
                    &self.read_page_payload_accounted(descriptor, entry, memory)?,
                    entry.row_count as usize,
                    descriptor.codec,
                    memory,
                )
            }
        })
    }

    fn read_var_bytes_values(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<(Vec<Bytes>, Vec<u32>)> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let page_index = self.read_compacted_page_index(descriptor, row_ids)?;

        match row_ids {
            Some(ids) => self.read_selected_var_bytes_pages(descriptor, ids, &page_index),
            None => {
                let lengths = self.read_u32("data_len", None)?;
                let data = self.artifacts.read(&descriptor.data_path)?;
                let mut values = Vec::with_capacity(lengths.len());
                for entry in &page_index {
                    let start = usize::try_from(entry.first_row).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "data row offset overflow")
                    })?;
                    let end = start.checked_add(entry.row_count as usize).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                    })?;
                    let expected = lengths.get(start..end).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "data length rows differ")
                    })?;
                    values.extend(decode_var_bytes_page_bounded(
                        self.slice_page(&data, entry)?,
                        descriptor.codec,
                        expected,
                    )?);
                }
                if values.len() != lengths.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "decoded data rows differ from data lengths",
                    ));
                }
                Ok((values, lengths))
            }
        }
    }

    fn read_selected_var_bytes_pages(
        &self,
        descriptor: &ColumnDescriptor,
        row_ids: &[u32],
        page_index: &[PageIndexEntry],
    ) -> io::Result<(Vec<Bytes>, Vec<u32>)> {
        if row_ids.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        let selections = build_selections(row_ids, page_index)?;
        let coverage_len = selections.iter().try_fold(0usize, |sum, selection| {
            sum.checked_add(selection.entry.row_count as usize)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                })
        })?;
        let mut coverage = Vec::with_capacity(coverage_len);
        for selection in &selections {
            let end = selection
                .entry
                .first_row
                .checked_add(u64::from(selection.entry.row_count))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                })?;
            for row in selection.entry.first_row..end {
                coverage.push(u32::try_from(row).map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row exceeds addressing")
                })?);
            }
        }
        let covered_lengths = self.read_u32("data_len", Some(&coverage))?;
        if covered_lengths.len() != coverage.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "data length rows differ",
            ));
        }

        let mut values = vec![None; row_ids.len()];
        let mut lengths = vec![None; row_ids.len()];
        let mut cursor = 0usize;
        for selection in selections {
            let end = cursor
                .checked_add(selection.entry.row_count as usize)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "data row range overflow")
                })?;
            let expected = &covered_lengths[cursor..end];
            let page = decode_var_bytes_page_bounded(
                &self.read_page_payload(descriptor, &selection.entry)?,
                descriptor.codec,
                expected,
            )?;
            for (&local_row, &output_position) in selection
                .local_rows
                .iter()
                .zip(selection.output_positions.iter())
            {
                values[output_position] = Some(page[local_row].clone());
                lengths[output_position] = Some(expected[local_row]);
            }
            cursor = end;
        }
        Ok((
            materialize_selected(values)?,
            materialize_selected(lengths)?,
        ))
    }

    fn read_compacted_page_index(
        &self,
        descriptor: &ColumnDescriptor,
        row_ids: Option<&[u32]>,
    ) -> io::Result<Vec<PageIndexEntry>> {
        self.read_compacted_page_index_accounted(descriptor, row_ids, None)
            .map(|buffer| buffer.into_parts().0)
    }
    fn read_compacted_page_index_accounted(
        &self,
        descriptor: &ColumnDescriptor,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<PageIndexEntry>> {
        let path = descriptor.page_index_path.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted column is missing a page index",
            )
        })?;
        let data = if memory.is_some() {
            self.artifacts.read_accounted(path)?
        } else {
            QueryBuffer::unaccounted(self.artifacts.read(path)?)
        };
        let mut entries = read_page_index_accounted(&data, memory)?;
        let visible_rows = self.read_row_count()?;
        let required_rows = row_ids
            .and_then(|ids| ids.iter().max())
            .map_or(visible_rows, |&last| u64::from(last) + 1);
        if required_rows > visible_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "row exceeds manifest boundary",
            ));
        }
        let mut rows = 0u64;
        let mut offset = 0u64;
        let mut visible_entries = 0;
        for entry in entries.iter() {
            if rows >= required_rows {
                break;
            }
            if entry.first_row != rows
                || entry.offset != offset
                || entry.row_count == 0
                || entry.row_count > descriptor.page_rows
                || entry.encoded_len == 0
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "page index does not describe a contiguous manifest prefix",
                ));
            }
            rows = rows
                .checked_add(u64::from(entry.row_count))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "page row range overflow")
                })?;
            offset = offset
                .checked_add(u64::from(entry.encoded_len))
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "page byte range overflow")
                })?;
            visible_entries += 1;
        }
        if rows < required_rows || rows > visible_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "page index does not cover the exact manifest row boundary",
            ));
        }
        // An append can replace its index before publishing a new manifest.
        // Existing readers retain their captured row boundary and page prefix.
        entries.truncate(visible_entries);
        Ok(entries)
    }

    fn slice_page<'a>(&self, data: &'a [u8], entry: &PageIndexEntry) -> io::Result<&'a [u8]> {
        let bounds = usize::try_from(entry.offset).ok().and_then(|start| {
            start
                .checked_add(entry.encoded_len as usize)
                .map(|end| start..end)
        });
        bounds.and_then(|range| data.get(range)).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted page is out of bounds",
            )
        })
    }

    fn read_page_payload_accounted(
        &self,
        descriptor: &ColumnDescriptor,
        entry: &PageIndexEntry,
        memory: Option<&QueryMemoryBudget>,
    ) -> io::Result<QueryBuffer<u8>> {
        if memory.is_none() {
            return self
                .read_page_payload(descriptor, entry)
                .map(QueryBuffer::unaccounted);
        }
        #[cfg(test)]
        BATCH_PAGE_READS.with_borrow_mut(|reads| {
            if let Some(reads) = reads {
                reads.push((descriptor.name.clone(), entry.first_row));
            }
        });
        let end = entry
            .offset
            .checked_add(u64::from(entry.encoded_len))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "page range overflow"))?;
        self.artifacts
            .read_range_accounted(&descriptor.data_path, entry.offset..end)
    }

    fn read_page_payload(
        &self,
        descriptor: &ColumnDescriptor,
        entry: &PageIndexEntry,
    ) -> io::Result<Vec<u8>> {
        #[cfg(test)]
        BATCH_PAGE_READS.with_borrow_mut(|reads| {
            if let Some(reads) = reads {
                reads.push((descriptor.name.clone(), entry.first_row));
            }
        });
        let end = entry
            .offset
            .checked_add(u64::from(entry.encoded_len))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "page range overflow"))?;
        self.artifacts
            .read_range(&descriptor.data_path, entry.offset..end)
    }
}

fn raw_column_path(column: &str) -> &'static str {
    match column {
        "block_number" => "block_number.col",
        "block_hash" => "block_hash.col",
        "timestamp" => "timestamp.col",
        "tx_hash" => "tx_hash.col",
        "tx_index" => "tx_index.col",
        "log_index" => "log_index.col",
        "data_len" => "data_len.col",
        "source" => "source.col",
        "data" => "data.col",
        _ => panic!("unsupported raw column: {column}"),
    }
}

fn load_manifest(dir: &Path) -> io::Result<Option<SegmentManifest>> {
    Ok(SegmentManifest::load(&dir.join("segment.json"))?)
}

fn build_selections(
    row_ids: &[u32],
    page_index: &[PageIndexEntry],
) -> io::Result<Vec<PageSelection>> {
    build_selections_accounted(row_ids, page_index, None).map(|buffer| buffer.into_parts().0)
}

fn build_selections_accounted(
    row_ids: &[u32],
    page_index: &[PageIndexEntry],
    memory: Option<&QueryMemoryBudget>,
) -> io::Result<QueryBuffer<PageSelection>> {
    if row_ids.is_empty() {
        return QueryBuffer::try_with_capacity(0, memory, "page selections");
    }

    let mut selections =
        QueryBuffer::<PageSelection>::try_with_capacity(0, memory, "page selections")?;
    let mut page_cursor = 0usize;

    // Scan pages in physical order, then scatter into the caller's order. SQL
    // ORDER BY can select descending or arbitrary rows; a forward-only cursor
    // cannot follow those IDs directly. The usual ascending index scan keeps
    // its allocation-free ordering path, and every selected page decodes once.
    let sorted_positions = if row_ids.is_sorted() {
        None
    } else {
        let mut positions =
            QueryBuffer::try_with_capacity(row_ids.len(), memory, "selection order")?;
        positions.try_extend(0..row_ids.len())?;
        positions.sort_unstable_by_key(|position| row_ids[*position]);
        Some(positions)
    };

    for position in 0..row_ids.len() {
        let output_position = sorted_positions
            .as_ref()
            .map_or(position, |positions| positions[position]);
        let row_id = row_ids[output_position];
        while page_cursor < page_index.len()
            && row_id as u64
                >= page_index[page_cursor].first_row + page_index[page_cursor].row_count as u64
        {
            page_cursor += 1;
        }

        let entry = page_index.get(page_cursor).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "requested row is outside the page index",
            )
        })?;
        if (row_id as u64) < entry.first_row {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "requested row is before the current page range",
            ));
        }

        let local_row = (row_id as u64 - entry.first_row) as usize;
        if let Some(last) = selections.last_mut()
            && last.entry == *entry
        {
            last.local_rows.try_push(local_row)?;
            last.output_positions.try_push(output_position)?;
        } else {
            let mut local_rows = QueryBuffer::try_with_capacity(1, memory, "local selection rows")?;
            let mut output_positions =
                QueryBuffer::try_with_capacity(1, memory, "selection output positions")?;
            local_rows.try_push(local_row)?;
            output_positions.try_push(output_position)?;
            selections.try_push(PageSelection {
                entry: *entry,
                local_rows,
                output_positions,
            })?;
        }
    }

    Ok(selections)
}

#[cfg(test)]
fn read_selected_pages<T: Copy + Default>(
    row_ids: Option<&[u32]>,
    page_index: &[PageIndexEntry],
    mut decode: impl FnMut(&PageIndexEntry) -> io::Result<Vec<T>>,
) -> io::Result<Vec<T>> {
    read_selected_pages_accounted(row_ids, page_index, None, |entry| {
        decode(entry).map(QueryBuffer::unaccounted)
    })
    .map(|buffer| buffer.into_parts().0)
}

fn read_selected_pages_accounted<T: Copy + Default>(
    row_ids: Option<&[u32]>,
    page_index: &[PageIndexEntry],
    memory: Option<&QueryMemoryBudget>,
    mut decode: impl FnMut(&PageIndexEntry) -> io::Result<QueryBuffer<T>>,
) -> io::Result<QueryBuffer<T>> {
    let mut decode_checked = |entry: &PageIndexEntry| {
        let page = decode(entry)?;
        if page.len() != entry.row_count as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded page row count differs from its index",
            ));
        }
        Ok(page)
    };
    let count = match row_ids {
        Some(ids) => ids.len(),
        None => page_index.iter().try_fold(0usize, |total, entry| {
            total.checked_add(entry.row_count as usize).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "page row count overflow")
            })
        })?,
    };
    let mut result = QueryBuffer::try_with_capacity(count, memory, "selected fixed output")?;
    match row_ids {
        Some(ids) => {
            let selections = build_selections_accounted(ids, page_index, memory)?;
            result.try_resize(count, T::default())?;
            for selection in selections.iter() {
                let page = decode_checked(&selection.entry)?;
                for (&local, &output) in selection
                    .local_rows
                    .iter()
                    .zip(selection.output_positions.iter())
                {
                    result[output] = page[local];
                }
            }
        }
        None => {
            for entry in page_index {
                result.try_extend_from_slice(&decode_checked(entry)?)?;
            }
        }
    }
    Ok(result)
}

fn bitmap_present(data: &[u8], rows: u64, row: u64) -> bool {
    row < rows
        && data
            .get(8 + (row / 8) as usize)
            .is_some_and(|byte| byte & (1 << (row % 8)) != 0)
}

fn materialize_selected<T>(values: Vec<Option<T>>) -> io::Result<Vec<T>> {
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value.ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("requested row was not materialized at output position {index}"),
                )
            })
        })
        .collect()
}

fn validate_data_lengths(values: &[Bytes], lengths: &[u32]) -> io::Result<()> {
    if values.len() != lengths.len()
        || values
            .iter()
            .zip(lengths)
            .any(|(value, &length)| value.len() != length as usize)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "data bytes differ from their row-length metadata",
        ));
    }
    Ok(())
}

fn check_log_materialization_cancel(cancel: Option<&dyn Fn() -> bool>) -> io::Result<()> {
    if cancel.is_some_and(|check| check()) {
        Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    use alloy_primitives::{Address, bytes};
    use tempfile::TempDir;

    use crate::ColumnFile;
    use crate::native::{
        SegmentDescriptor, SegmentKind, StorageCatalogPaths, compact_segment,
        persist_initial_raw_manifest_for_test, persist_segment_manifest,
    };

    #[test]
    fn inspection_preflight_raw_allowances_preserve_complete_rows() {
        let tmp = TempDir::new().unwrap();
        let rows = make_rows();
        ColumnFile::write_batch(tmp.path(), &rows).unwrap();
        let mut reader = SegmentReader::open_for_inspection(tmp.path()).unwrap();
        let artifact_bytes: u64 = reader
            .artifacts
            .inspection_artifact_lengths()
            .unwrap()
            .into_iter()
            .sum();
        let decoded: u64 = rows.iter().map(|row| u64::from(row.data_len)).sum();
        assert!(matches!(
            reader.inspection_preflight(artifact_bytes - 1, decoded),
            Err(InspectionPreflightError::LimitExceeded {
                resource: "retained_artifact_bytes",
                ..
            })
        ));
        assert!(matches!(
            reader.inspection_preflight(artifact_bytes, decoded - 1),
            Err(InspectionPreflightError::LimitExceeded {
                resource: "decoded_payload_bytes",
                ..
            })
        ));
        reader
            .inspection_preflight(artifact_bytes, decoded)
            .unwrap();
        assert_eq!(reader.read_log_rows(None).unwrap(), rows);
    }

    #[test]
    fn inspection_preflight_limits_precede_corrupt_payload_decoding() {
        let (_tmp, dir) = compacted_fixture();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        let descriptor = manifest
            .columns
            .iter()
            .find(|column| column.name == "data")
            .unwrap();
        let path = dir.join(&descriptor.data_path);
        let len = fs::metadata(&path).unwrap().len() as usize;
        fs::write(&path, vec![0; len]).unwrap();
        let mut reader = SegmentReader::open_for_inspection(&dir).unwrap();
        BATCH_PAGE_READS.with_borrow_mut(|reads| *reads = Some(Vec::new()));
        let result = reader.inspection_preflight(u64::MAX, 0);
        let reads = BATCH_PAGE_READS.with_borrow_mut(Option::take).unwrap();
        assert!(matches!(
            result,
            Err(InspectionPreflightError::LimitExceeded {
                resource: "decoded_payload_bytes",
                ..
            })
        ));
        assert!(reads.iter().all(|(name, _)| name != "data"));
    }

    #[test]
    fn inspection_artifact_limit_precedes_malformed_raw_metadata() {
        let tmp = TempDir::new().unwrap();
        ColumnFile::write_batch(tmp.path(), &make_rows()).unwrap();
        fs::write(tmp.path().join("data_len.col"), [0; ColumnFileHeader::SIZE]).unwrap();
        let mut reader = SegmentReader::open_for_inspection(tmp.path()).unwrap();
        assert!(matches!(
            reader.inspection_preflight(0, u64::MAX),
            Err(InspectionPreflightError::LimitExceeded {
                resource: "retained_artifact_bytes",
                ..
            })
        ));
        assert!(
            matches!(reader.inspection_preflight(u64::MAX, u64::MAX), Err(InspectionPreflightError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
        );
    }

    #[test]
    fn inspection_rejects_raw_tail_and_post_preflight_growth() {
        let tmp = TempDir::new().unwrap();
        let rows = make_rows();
        ColumnFile::write_batch(tmp.path(), &rows).unwrap();
        let mut reader = SegmentReader::open_for_inspection(tmp.path()).unwrap();
        reader.inspection_preflight(u64::MAX, u64::MAX).unwrap();
        let path = tmp.path().join("data_len.col");
        fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[0])
            .unwrap();
        assert_eq!(
            reader.read_u32("data_len", None).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut reopened = SegmentReader::open_for_inspection(tmp.path()).unwrap();
        assert!(
            matches!(reopened.inspection_preflight(u64::MAX, u64::MAX), Err(InspectionPreflightError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
        );
        // The offline-only bound must not change ordinary captured-prefix reads.
        assert_eq!(
            SegmentReader::open(tmp.path())
                .unwrap()
                .read_u32("data_len", None)
                .unwrap()
                .len(),
            rows.len()
        );
    }

    #[test]
    fn inspection_capture_rejects_nonregular_sources_before_open() {
        for name in [
            "segment.json",
            crate::column::SOURCE_MARKER_FILE,
            "data.col",
        ] {
            let tmp = TempDir::new().unwrap();
            ColumnFile::write_batch(tmp.path(), &make_rows()).unwrap();
            let path = tmp.path().join(name);
            if path.exists() {
                fs::remove_file(&path).unwrap();
            }
            fs::create_dir(&path).unwrap();
            assert_eq!(
                SegmentReader::open_for_inspection(tmp.path())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::Unsupported
            );
        }
    }

    #[test]
    fn inspection_charge_overflow_is_an_explicit_limit_outcome() {
        let mut bytes = u64::MAX;
        assert!(matches!(
            inspection_charge(&mut bytes, 1, u64::MAX, "retained_artifact_bytes"),
            Err(InspectionPreflightError::LimitExceeded {
                required: u64::MAX,
                ..
            })
        ));
    }

    #[test]
    fn inspection_empty_paged_source_cannot_ignore_present_rows() {
        let (_tmp, dir) = compacted_fixture();
        let mut reader = SegmentReader::open_for_inspection(&dir).unwrap();
        reader.manifest.as_mut().unwrap().row_count = 0;
        assert!(
            matches!(reader.inspection_preflight(u64::MAX, u64::MAX), Err(InspectionPreflightError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
        );
    }

    const QUERY_LOG_COLUMNS: &[&str] = &[
        "address",
        "block_number",
        "block_hash",
        "timestamp",
        "tx_hash",
        "tx_index",
        "log_index",
        "topic0",
        "topic1",
        "topic2",
        "topic3",
        "data",
        "data_len",
        "source",
    ];

    #[test]
    fn accounted_log_materialization_preserves_rows_and_payload_aliases() {
        let expected = make_rows();
        let raw = TempDir::new().unwrap();
        ColumnFile::write_batch(raw.path(), &expected).unwrap();
        let (_paged, paged_dir) = compacted_fixture();
        let bundled = TempDir::new().unwrap();
        let mut storage = crate::native::NativeStorage::open(crate::native::NativeStorageConfig {
            data_dir: bundled.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        let bundle_dir = storage.write_historical_batch(&expected).unwrap()[0]
            .path
            .clone();
        drop(storage);
        for dir in [raw.path(), paged_dir.as_path(), bundle_dir.as_path()] {
            let memory = QueryMemoryBudget::new(
                logex_types::QueryMemoryLimit::new(4 * 1024 * 1024).unwrap(),
            );
            let reader =
                SegmentReader::open_projected_with_memory(dir, QUERY_LOG_COLUMNS, memory.clone())
                    .unwrap();
            for ids in [&[][..], &[19, 0, 19, 7][..], &[0, 7, 19][..]] {
                let rows = reader.read_log_rows_with_memory(ids, None).unwrap();
                let reference: Vec<_> = ids
                    .iter()
                    .map(|&id| expected[id as usize].clone())
                    .collect();
                assert_eq!(&*rows, reference.as_slice());
            }
            let rows = reader
                .read_log_rows_with_memory(&[19, 0, 19], None)
                .unwrap();
            let payload = rows[0].data.clone();
            let slice = payload.slice(1..2);
            drop(reader);
            let rows_and_payloads = memory.used();
            assert!(rows_and_payloads >= (rows.capacity() * std::mem::size_of::<LogRow>()) as u128);
            drop(rows);
            drop(payload);
            assert!(memory.used() > 0);
            assert!(memory.used() < rows_and_payloads);
            assert_eq!(slice.as_ref(), &[0xad]);
            drop(slice);
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn accounted_log_materialization_pressure_cancel_and_invalid_ids_release() {
        use std::cell::Cell;
        let raw = TempDir::new().unwrap();
        ColumnFile::write_batch(raw.path(), &make_rows()).unwrap();
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader = SegmentReader::open_projected_with_memory(
            raw.path(),
            QUERY_LOG_COLUMNS,
            memory.clone(),
        )
        .unwrap();
        let baseline = memory.used();
        let competitor = memory
            .reserve(
                memory.limit() - usize::try_from(baseline).unwrap() - 1,
                "fixture competitor",
            )
            .unwrap();
        let error = reader.read_log_rows_with_memory(&[0], None).unwrap_err();
        assert!(matches!(
            error
                .get_ref()
                .and_then(|error| error.downcast_ref::<logex_types::QueryMemoryError>()),
            Some(logex_types::QueryMemoryError::CapacityExceeded { .. })
        ));
        assert_eq!(memory.used(), baseline + competitor.bytes());
        drop(competitor);
        for stop_at in [1, 5, 16, 17] {
            let calls = Cell::new(0);
            let cancel = || {
                calls.set(calls.get() + 1);
                calls.get() >= stop_at
            };
            let error = reader
                .read_log_rows_with_memory(&[19, 0, 19], Some(&cancel))
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
            assert_eq!(memory.used(), baseline);
        }
        assert_eq!(
            reader
                .read_log_rows_with_memory(&[20], None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(memory.used(), baseline);
        assert_eq!(
            reader.read_log_rows_with_memory(&[0], None).unwrap()[0],
            make_rows()[0]
        );
        drop(reader);
        assert_eq!(memory.used(), 0);
        let unbudgeted = SegmentReader::open_projected(raw.path(), QUERY_LOG_COLUMNS).unwrap();
        assert_eq!(
            unbudgeted
                .read_log_rows_with_memory(&[], None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    fn make_rows() -> Vec<LogRow> {
        (0..20)
            .map(|index| LogRow {
                block_number: 10 + index as u64,
                block_hash: B256::repeat_byte(index as u8),
                timestamp: 1_700_000_000 + index as u64 * 12,
                tx_hash: B256::repeat_byte((index + 1) as u8),
                tx_index: (index % 3) as u32,
                log_index: index as u32,
                address: Address::repeat_byte((index % 2) as u8),
                topic0: Some(B256::repeat_byte(0xAA)),
                topic1: if index % 2 == 0 {
                    Some(B256::repeat_byte(0xBB))
                } else {
                    None
                },
                topic2: None,
                topic3: None,
                data: bytes!("deadbeef"),
                data_len: 4,
                source: Source::Receipt,
            })
            .collect()
    }

    fn write_native_raw(dir: &Path, rows: &[LogRow], descriptor: &mut SegmentDescriptor) {
        let namespace = [descriptor.id as u8; 16];
        descriptor.source_namespace = Some(namespace.into());
        descriptor.source_state = Some(crate::PrefixState::from_rows(namespace, rows).unwrap());
        descriptor.source_commitment = descriptor
            .source_state
            .as_ref()
            .map(crate::PrefixState::commitment);
        ColumnFile::write_initial_batch_with_source_identity(
            dir,
            rows,
            None,
            crate::durability::Publication::Ordered,
            crate::column::SourceIdentity {
                namespace,
                generation: descriptor.generation,
                segment_id: descriptor.id,
                kind: descriptor.kind,
            },
            descriptor.source_state.as_ref(),
        )
        .unwrap();
    }

    fn identified_raw_fixture() -> (TempDir, PathBuf, SegmentDescriptor, Vec<LogRow>) {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let mut descriptor = SegmentDescriptor {
            column_bundle: None,
            source_namespace: None,
            source_commitment: None,
            source_state: None,
            id: 11,
            generation: 3,
            kind: SegmentKind::Hot,
            relative_path: PathBuf::from("segments/s_0000000000000011"),
            manifest_relative_path: PathBuf::from("segments/s_0000000000000011/segment.json"),
            min_block: Some(10),
            max_block: Some(29),
            min_timestamp: Some(1_700_000_000),
            max_timestamp: Some(1_700_000_228),
            row_count: 20,
        };
        let dir = paths.segment_dir(descriptor.id);
        let rows = make_rows();
        write_native_raw(&dir, &rows, &mut descriptor);
        persist_initial_raw_manifest_for_test(&paths, &descriptor).unwrap();
        (tmp, dir, descriptor, rows)
    }

    fn after_artifact_capture(action: impl FnOnce() + 'static) {
        AFTER_ARTIFACT_CAPTURE.with_borrow_mut(|hook| *hook = Some(Box::new(action)));
    }

    /// Disposable raw, legacy paged and bundled fixtures exercise the same
    /// selected-read contract and retain output credit beyond reader lifetime.
    #[test]
    fn accounted_fixed_reads_match_all_layouts_and_release_owners() {
        let fixture_rows = make_rows();
        let raw = TempDir::new().unwrap();
        ColumnFile::write_batch(raw.path(), &fixture_rows).unwrap();
        let (_paged, paged_dir) = compacted_fixture();
        let bundled = TempDir::new().unwrap();
        let mut storage = crate::native::NativeStorage::open(crate::native::NativeStorageConfig {
            data_dir: bundled.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        let appended = storage.write_historical_batch(&fixture_rows).unwrap();
        let bundle_dir = appended[0].path.clone();
        drop(storage);
        for dir in [raw.path(), paged_dir.as_path(), bundle_dir.as_path()] {
            let memory = QueryMemoryBudget::new(
                logex_types::QueryMemoryLimit::new(4 * 1024 * 1024).unwrap(),
            );
            let columns = [
                "address",
                "block_number",
                "block_hash",
                "timestamp",
                "tx_hash",
                "tx_index",
                "log_index",
                "source",
                "data_len",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
            ];
            let reader =
                SegmentReader::open_projected_with_memory(dir, &columns, memory.clone()).unwrap();
            let reference = SegmentReader::open_projected(dir, &columns).unwrap();
            for ids in [
                None,
                Some(&[19, 0, 19, 7][..]),
                Some(&[0, 0, 7, 19][..]),
                Some(&[][..]),
            ] {
                let selected: Vec<&LogRow> = match ids {
                    Some(ids) => ids.iter().map(|id| &fixture_rows[*id as usize]).collect(),
                    None => fixture_rows.iter().collect(),
                };
                macro_rules! same {
                    ($accounted:expr, $reference:expr, $expected:expr) => {{
                        let actual = $accounted.unwrap();
                        assert_eq!(&*actual, $reference.unwrap().as_slice());
                        assert_eq!(&*actual, $expected.as_slice());
                    }};
                }
                same!(
                    reader.read_address_with_memory(ids),
                    reference.read_address(ids),
                    selected.iter().map(|row| row.address).collect::<Vec<_>>()
                );
                for name in ["block_number", "timestamp"] {
                    let expected = selected
                        .iter()
                        .map(|row| {
                            if name == "block_number" {
                                row.block_number
                            } else {
                                row.timestamp
                            }
                        })
                        .collect::<Vec<_>>();
                    same!(
                        reader.read_u64_with_memory(name, ids),
                        reference.read_u64(name, ids),
                        expected
                    );
                }
                for name in ["block_hash", "tx_hash"] {
                    let expected = selected
                        .iter()
                        .map(|row| {
                            if name == "block_hash" {
                                row.block_hash
                            } else {
                                row.tx_hash
                            }
                        })
                        .collect::<Vec<_>>();
                    same!(
                        reader.read_b256_with_memory(name, ids),
                        reference.read_b256(name, ids),
                        expected
                    );
                }
                for name in ["tx_index", "log_index", "data_len"] {
                    let expected = selected
                        .iter()
                        .map(|row| match name {
                            "tx_index" => row.tx_index,
                            "log_index" => row.log_index,
                            _ => row.data_len,
                        })
                        .collect::<Vec<_>>();
                    same!(
                        reader.read_u32_with_memory(name, ids),
                        reference.read_u32(name, ids),
                        expected
                    );
                }
                same!(
                    reader.read_u8_with_memory("source", ids),
                    reference.read_u8("source", ids),
                    selected
                        .iter()
                        .map(|row| row.source as u8)
                        .collect::<Vec<_>>()
                );
                for name in ["topic0", "topic1", "topic2", "topic3"] {
                    let expected = selected
                        .iter()
                        .map(|row| match name {
                            "topic0" => row.topic0,
                            "topic1" => row.topic1,
                            "topic2" => row.topic2,
                            _ => row.topic3,
                        })
                        .collect::<Vec<_>>();
                    same!(
                        reader.read_nullable_b256_with_memory(name, ids),
                        reference.read_nullable_b256(name, ids),
                        expected
                    );
                }
            }
            let before = memory.used();
            assert!(
                reader
                    .read_u64_with_memory("block_number", Some(&[20]))
                    .is_err()
            );
            assert_eq!(memory.used(), before);
            let pressure = memory
                .reserve(
                    memory.limit() - usize::try_from(memory.used()).unwrap() - 1,
                    "test pressure",
                )
                .unwrap();
            assert!(
                reader
                    .read_u64_with_memory("block_number", Some(&[0]))
                    .is_err()
            );
            drop(pressure);
            assert_eq!(memory.used(), before);
            let retained = reader
                .read_u64_with_memory("block_number", Some(&[19, 0, 19]))
                .unwrap();
            let retained_bytes = retained.capacity() * std::mem::size_of::<u64>();
            drop(reader);
            assert_eq!(memory.used(), retained_bytes as u128);
            assert_eq!(&*retained, &[29, 10, 29]);
            drop(retained);
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn accounted_raw_prefix_does_not_allocate_appended_tail() {
        let (_tmp, dir, _, rows) = identified_raw_fixture();
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader = SegmentReader::open_projected_with_memory(
            &dir,
            &["block_number", "topic0"],
            memory.clone(),
        )
        .unwrap();
        let appended = vec![rows[0].clone(); 128];
        ColumnFile::append_batch(&dir, &appended, rows.len() as u64).unwrap();
        let needed = ColumnFileHeader::SIZE + rows.len() * 8 * 2;
        let pressure = memory
            .reserve(
                memory.limit() - usize::try_from(memory.used()).unwrap() - needed,
                "test prefix allowance",
            )
            .unwrap();
        let output = reader.read_u64_with_memory("block_number", None).unwrap();
        assert_eq!(
            &*output,
            rows.iter()
                .map(|row| row.block_number)
                .collect::<Vec<_>>()
                .as_slice()
        );
        drop(output);
        drop(pressure);
        let nullable = reader
            .read_nullable_b256_with_memory("topic0", Some(&[19, 0, 19]))
            .unwrap();
        assert_eq!(
            &*nullable,
            &[rows[19].topic0, rows[0].topic0, rows[19].topic0]
        );
        drop(nullable);
        drop(reader);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn accounted_fixed_read_errors_release_temporary_buffers() {
        let tmp = TempDir::new().unwrap();
        ColumnFile::write_batch(tmp.path(), &make_rows()).unwrap();
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader = SegmentReader::open_projected_with_memory(
            tmp.path(),
            &["block_number", "topic0"],
            memory.clone(),
        )
        .unwrap();
        let baseline = memory.used();
        std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path().join("block_number.col"))
            .unwrap()
            .set_len(ColumnFileHeader::SIZE as u64)
            .unwrap();
        assert!(
            reader
                .read_u64_with_memory("block_number", Some(&[19]))
                .is_err()
        );
        assert_eq!(memory.used(), baseline);
        std::fs::OpenOptions::new()
            .write(true)
            .open(tmp.path().join("topic0.null"))
            .unwrap()
            .set_len(8)
            .unwrap();
        assert!(
            reader
                .read_nullable_b256_with_memory("topic0", Some(&[19]))
                .is_err()
        );
        assert_eq!(memory.used(), baseline);
        drop(reader);
        assert_eq!(memory.used(), 0);
    }

    fn rewrite_variable_fixture_pages(dir: &Path, codec: CompressionCodec, values: &[Bytes]) {
        let mut manifest = load_manifest(dir).unwrap().unwrap();
        let lengths: Vec<u32> = values.iter().map(|value| value.len() as u32).collect();
        for (name, ranges) in [
            ("data", vec![0..11, 11..20]),
            ("data_len", vec![0..7, 7..13, 13..20]),
        ] {
            let descriptor = manifest
                .columns
                .iter_mut()
                .find(|column| column.name == name)
                .unwrap();
            if name == "data" {
                descriptor.codec = codec;
            }
            let mut encoded = Vec::new();
            let mut index = Vec::new();
            for range in ranges {
                let page = if name == "data" {
                    crate::page::encode_var_bytes_page(&values[range.clone()], descriptor.codec)
                } else {
                    crate::page::encode_u32_page(&lengths[range.clone()], descriptor.codec)
                }
                .unwrap();
                index.push(PageIndexEntry {
                    first_row: range.start as u64,
                    row_count: range.len() as u32,
                    offset: encoded.len() as u64,
                    encoded_len: page.len() as u32,
                });
                encoded.extend(page);
            }
            fs::write(dir.join(&descriptor.data_path), encoded).unwrap();
            fs::write(
                dir.join(descriptor.page_index_path.as_ref().unwrap()),
                crate::page::write_page_index(&index),
            )
            .unwrap();
        }
        fs::write(
            dir.join("segment.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
    }

    fn assert_accounted_variable_fixture(dir: &Path, expected: &[Bytes]) {
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(4 * 1024 * 1024).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(dir, &["data"], memory.clone()).unwrap();
        for ids in [
            None,
            Some(&[19, 0, 19, 7][..]),
            Some(&[0, 0, 7, 19][..]),
            Some(&[][..]),
        ] {
            let output = reader.read_var_bytes_with_memory("data", ids).unwrap();
            let selected: Vec<Bytes> = match ids {
                Some(ids) => ids
                    .iter()
                    .map(|id| expected[*id as usize].clone())
                    .collect(),
                None => expected.to_vec(),
            };
            assert_eq!(&*output, selected.as_slice());
        }
        let baseline = memory.used();
        assert!(
            reader
                .read_var_bytes_with_memory("data", Some(&[20]))
                .is_err()
        );
        assert_eq!(memory.used(), baseline);
        let pressure = memory
            .reserve(
                memory.limit() - usize::try_from(memory.used()).unwrap() - 1,
                "variable test pressure",
            )
            .unwrap();
        assert!(
            reader
                .read_var_bytes_with_memory("data", Some(&[19]))
                .is_err()
        );
        drop(pressure);
        assert_eq!(memory.used(), baseline);
        let output = reader
            .read_var_bytes_with_memory("data", Some(&[19, 7, 19]))
            .unwrap();
        let clone = output[0].clone();
        let slice = clone.slice(1..);
        drop(clone);
        drop(output);
        drop(reader);
        // Only the selected payload backing survives, not source bytes or result headers.
        assert!(memory.used() >= expected[19].len() as u128);
        assert!(memory.used() <= (expected[19].len() * 2 + expected[7].len()) as u128);
        assert_eq!(slice.as_ref(), &expected[19][1..]);
        drop(slice);
        assert_eq!(memory.used(), 0);

        let reader =
            SegmentReader::open_projected_with_memory(dir, &["data"], memory.clone()).unwrap();
        let baseline = memory.used();
        let ids = [0, 7, 19];
        let mut batches = reader.var_bytes_batches_with_memory("data", &ids).unwrap();
        assert_eq!(
            memory.used(),
            baseline,
            "batch preparation must remain lazy"
        );
        let pressure = memory
            .reserve(
                memory.limit() - usize::try_from(baseline).unwrap() - 1,
                "variable batch test pressure",
            )
            .unwrap();
        let error = batches.next().unwrap().unwrap_err();
        assert!(
            error
                .get_ref()
                .is_some_and(|error| error.is::<logex_types::QueryMemoryError>())
        );
        assert!(batches.next().is_none());
        drop(batches);
        drop(pressure);
        assert_eq!(memory.used(), baseline);

        let batches = reader
            .var_bytes_batches_with_memory("data", &ids)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert!(
            batches
                .iter()
                .flat_map(|batch| batch.iter())
                .eq(ids.iter().map(|&row| &expected[row as usize]))
        );
        let alias = batches.last().unwrap().last().unwrap().slice(1..);
        drop(batches);
        drop(reader);
        assert!(memory.used() >= expected[19].len() as u128);
        assert!(
            memory.used()
                <= ids
                    .iter()
                    .map(|&row| expected[row as usize].len() as u128)
                    .sum()
        );
        assert_eq!(alias.as_ref(), &expected[19][1..]);
        drop(alias);
        assert_eq!(memory.used(), 0);
    }

    fn assert_accounted_canonical_fixture(dir: &Path) {
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader = SegmentReader::open_projected_with_memory(dir, &[], memory.clone()).unwrap();
        let baseline = memory.used();
        let pressure = memory
            .reserve(memory.limit() - baseline as usize - 1, "pressure")
            .unwrap();
        assert!(reader.read_canonical_accounted().is_err());
        drop(pressure);
        assert_eq!(memory.used(), baseline);
        let bitmap = reader.read_canonical_accounted().unwrap();
        assert_eq!(bitmap.len(), 20);
        assert!(!bitmap.is_empty());
        assert!((0..20).all(|row| bitmap.is_present(row)));
        assert!(!bitmap.is_present(20));
        assert!(!bitmap.is_present(u64::MAX));
        drop(reader);
        assert_eq!(memory.used(), bitmap.data.capacity() as u128);
        drop(bitmap);
        assert_eq!(memory.used(), 0);
        assert_eq!(
            SegmentReader::open_projected(dir, &[])
                .unwrap()
                .read_canonical_accounted()
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[test]
    fn accounted_canonical_retains_backing_across_all_layouts() {
        let raw = TempDir::new().unwrap();
        ColumnFile::write_batch(raw.path(), &make_rows()).unwrap();
        assert_accounted_canonical_fixture(raw.path());
        let (_tmp, dir, _, _) = identified_raw_fixture();
        assert_accounted_canonical_fixture(&dir);
        let (_tmp, dir) = compacted_fixture();
        assert_accounted_canonical_fixture(&dir);
        let tmp = TempDir::new().unwrap();
        let mut storage = crate::native::NativeStorage::open(crate::native::NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        let appended = storage.write_historical_batch(&make_rows()).unwrap();
        assert_accounted_canonical_fixture(&appended[0].path);
    }

    #[test]
    fn accounted_canonical_checks_captured_body_and_masks_padding() {
        let raw = TempDir::new().unwrap();
        ColumnFile::write_batch(raw.path(), &make_rows()).unwrap();
        // Current writers produce a source-bound checksummed envelope. Build
        // an actual legacy fixture: no source marker, plain row count and bits.
        fs::remove_file(raw.path().join(crate::column::SOURCE_MARKER_FILE)).unwrap();
        let path = raw.path().join("canonical.bitmap");
        let mut encoded = 20u64.to_le_bytes().to_vec();
        encoded.extend_from_slice(&[0xfd, 0xff, 0xff]);
        // Row 1 is absent; high bits of the final byte are legacy padding.
        fs::write(&path, encoded).unwrap();
        let memory = QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(4096).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(raw.path(), &[], memory.clone()).unwrap();
        let bitmap = reader.read_canonical_accounted().unwrap();
        assert!(!bitmap.is_present(1));
        assert!(bitmap.is_present(19));
        assert!(!bitmap.is_present(20));
        drop(bitmap);
        drop(reader);
        assert_eq!(memory.used(), 0);
        let (_tmp, dir, _, _) = identified_raw_fixture();
        let reader = SegmentReader::open_projected_with_memory(&dir, &[], memory.clone()).unwrap();
        let path = dir.join("canonical.bitmap");
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, bytes).unwrap();
        assert_eq!(
            reader.read_canonical_accounted().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        drop(reader);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn accounted_variable_sources_preserve_selection_and_aliases_all_layouts() {
        let mut rows = make_rows();
        for (index, row) in rows.iter_mut().enumerate() {
            row.data = Bytes::from(vec![index as u8; index % 7]);
            row.data_len = row.data.len() as u32;
        }
        let expected: Vec<_> = rows.iter().map(|row| row.data.clone()).collect();
        let raw = TempDir::new().unwrap();
        ColumnFile::write_batch(raw.path(), &rows).unwrap();
        assert_accounted_variable_fixture(raw.path(), &expected);
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
            CompressionCodec::AdaptiveBytes,
        ] {
            let (_tmp, dir) = compacted_fixture();
            rewrite_variable_fixture_pages(&dir, codec, &expected);
            assert_accounted_variable_fixture(&dir, &expected);
        }
        let tmp = TempDir::new().unwrap();
        let mut storage = crate::native::NativeStorage::open(crate::native::NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 20,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        let appended = storage.write_historical_batch(&rows).unwrap();
        assert_accounted_variable_fixture(&appended[0].path, &expected);
    }

    #[test]
    fn accounted_variable_reads_validate_unselected_offsets_and_page_lengths() {
        let raw = TempDir::new().unwrap();
        ColumnFile::write_batch(raw.path(), &make_rows()).unwrap();
        let path = raw.path().join("data.col");
        let mut data = fs::read(&path).unwrap();
        data[ColumnFileHeader::SIZE + 8..ColumnFileHeader::SIZE + 16]
            .copy_from_slice(&u64::MAX.to_le_bytes());
        fs::write(path, data).unwrap();
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(raw.path(), &["data"], memory.clone())
                .unwrap();
        for ids in [Some(&[19][..]), Some(&[][..])] {
            assert_eq!(
                reader
                    .read_var_bytes_with_memory("data", ids)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(memory.used(), 0);
        }
        let mut batches = reader.var_bytes_batches_with_memory("data", &[19]).unwrap();
        assert_eq!(
            batches.next().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(batches.next().is_none());
        drop(batches);
        assert_eq!(memory.used(), 0);
        for codec in [
            CompressionCodec::None,
            CompressionCodec::Lz4,
            CompressionCodec::Zstd,
            CompressionCodec::AdaptiveBytes,
        ] {
            let (_tmp, dir) = compacted_fixture();
            let mut values = vec![bytes!("deadbeef"); 20];
            rewrite_variable_fixture_pages(&dir, codec, &values);
            // Keep total payload unchanged while mismatching an unselected row's companion length.
            values[0] = bytes!("aabbcc");
            values[1] = bytes!("aabbccddee");
            let manifest = load_manifest(&dir).unwrap().unwrap();
            let descriptor = manifest
                .columns
                .iter()
                .find(|column| column.name == "data")
                .unwrap();
            let encoded = crate::page::encode_var_bytes_page(&values, codec).unwrap();
            fs::write(dir.join(&descriptor.data_path), &encoded).unwrap();
            fs::write(
                dir.join(descriptor.page_index_path.as_ref().unwrap()),
                crate::page::write_page_index(&[PageIndexEntry {
                    first_row: 0,
                    row_count: 20,
                    offset: 0,
                    encoded_len: encoded.len() as u32,
                }]),
            )
            .unwrap();
            let memory =
                QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
            let reader =
                SegmentReader::open_projected_with_memory(&dir, QUERY_LOG_COLUMNS, memory.clone())
                    .unwrap();
            assert_eq!(
                reader
                    .read_log_rows_with_memory(&[19], None)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData,
            );
            assert_eq!(memory.used(), 0);
            assert_eq!(
                reader
                    .read_var_bytes_with_memory("data", Some(&[19]))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(memory.used(), 0);
            let mut batches = reader.var_bytes_batches_with_memory("data", &[19]).unwrap();
            assert_eq!(
                batches.next().unwrap().unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "every companion length must be checked, including unselected rows"
            );
            assert!(batches.next().is_none());
            drop(batches);
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn accounted_variable_capture_survives_replacement_and_append() {
        let raw = TempDir::new().unwrap();
        let rows = make_rows();
        ColumnFile::write_batch(raw.path(), &rows).unwrap();
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(raw.path(), &["data"], memory.clone())
                .unwrap();
        let ids = [0, 19];
        let baseline = memory.used();
        let mut batches = reader.var_bytes_batches_with_memory("data", &ids).unwrap();
        assert_eq!(memory.used(), baseline);
        let mut appended = rows[0].clone();
        appended.data = bytes!("010203");
        appended.data_len = 3;
        ColumnFile::append_batch(raw.path(), &[appended], rows.len() as u64).unwrap();
        let batch = batches.next().unwrap().unwrap();
        assert_eq!(&*batch, &ids.map(|id| rows[id as usize].data.clone()));
        assert!(batches.next().is_none());
        let batch_alias = batch[0].clone();
        drop(batch);
        drop(batches);
        assert!(reader.var_bytes_batches_with_memory("data", &[20]).is_err());
        let result = reader.read_var_bytes_with_memory("data", None).unwrap();
        assert_eq!(
            &*result,
            rows.iter()
                .map(|row| row.data.clone())
                .collect::<Vec<_>>()
                .as_slice()
        );
        let alias = result[0].clone();
        drop(result);
        drop(reader);
        assert!(memory.used() > 0);
        assert_eq!(alias, rows[0].data);
        drop(alias);
        assert_eq!(batch_alias, rows[0].data);
        assert!(memory.used() > 0);
        drop(batch_alias);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn log_row_batches_match_raw_selected_rows_and_reject_invalid_ids() {
        let tmp = TempDir::new().unwrap();
        let mut rows = make_rows();
        for (index, row) in rows.iter_mut().enumerate() {
            row.data = Bytes::from(vec![index as u8; index % 7]);
            row.data_len = row.data.len() as u32;
            row.source = if index % 2 == 0 {
                Source::Trace
            } else {
                Source::Receipt
            };
            row.topic2 = (index % 3 == 0).then_some(B256::repeat_byte(index as u8));
            row.topic3 = (index % 4 == 0).then_some(B256::repeat_byte(index as u8 + 1));
        }
        ColumnFile::write_batch(tmp.path(), &rows).unwrap();
        let reader = SegmentReader::open(tmp.path()).unwrap();
        let ids = [0, 2, 7, 11, 19];
        assert_eq!(
            reader
                .log_row_batches(&ids)
                .unwrap()
                .collect::<io::Result<Vec<_>>>()
                .unwrap()
                .concat(),
            ids.map(|id| rows[id as usize].clone())
        );
        assert!(reader.log_row_batches(&[]).unwrap().next().is_none());
        for ids in [&[1, 0][..], &[0, 0], &[20]] {
            assert_eq!(
                reader.log_row_batches(ids).err().unwrap().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let batches = reader.log_row_batches(&ids).unwrap();
        let mut replacement = rows.clone();
        replacement[0].data = bytes!("aabbcc");
        replacement[0].data_len = 3;
        replacement[0].block_number += 1000;
        ColumnFile::write_batch(tmp.path(), &replacement).unwrap();
        assert_eq!(
            batches.collect::<io::Result<Vec<_>>>().unwrap().concat(),
            ids.map(|id| rows[id as usize].clone())
        );
    }

    #[test]
    fn log_row_batches_match_paged_rows_across_selected_gaps() {
        let (_tmp, dir) = compacted_fixture();
        let rows = make_rows();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        let descriptor = manifest
            .columns
            .iter()
            .find(|column| column.name == "data")
            .unwrap();
        let mut encoded = Vec::new();
        let mut index = Vec::new();
        for range in [0..7, 7..13, 13..20] {
            let values: Vec<_> = rows[range.clone()]
                .iter()
                .map(|row| row.data.clone())
                .collect();
            let page = crate::page::encode_var_bytes_page(&values, descriptor.codec).unwrap();
            index.push(PageIndexEntry {
                first_row: range.start as u64,
                row_count: range.len() as u32,
                offset: encoded.len() as u64,
                encoded_len: page.len() as u32,
            });
            encoded.extend(page);
        }
        fs::write(dir.join(&descriptor.data_path), encoded).unwrap();
        fs::write(
            dir.join(descriptor.page_index_path.as_ref().unwrap()),
            crate::page::write_page_index(&index),
        )
        .unwrap();
        let reader = SegmentReader::open(&dir).unwrap();
        let ids = [0, 6, 7, 12, 13, 19];
        let batches = reader
            .log_row_batches(&ids)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2, 2]);
        assert_eq!(batches.concat(), ids.map(|id| rows[id as usize].clone()));

        let mut batches = reader.log_row_batches(&ids).unwrap();
        assert_eq!(
            batches.next().unwrap().unwrap(),
            [rows[0].clone(), rows[6].clone()]
        );
        fs::OpenOptions::new()
            .write(true)
            .open(dir.join(&descriptor.data_path))
            .unwrap()
            .set_len(index[1].offset)
            .unwrap();
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
        assert!(batches.next().is_none());
    }

    #[test]
    fn log_row_batches_match_bundled_rows() {
        let tmp = TempDir::new().unwrap();
        let rows = make_rows();
        let mut storage = crate::native::NativeStorage::open(crate::native::NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: rows.len() as u64,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        let appended = storage.write_historical_batch(&rows).unwrap();
        assert_eq!(appended.len(), 1);
        let reader = SegmentReader::open(&appended[0].path).unwrap();
        assert!(reader.bundle_reference().is_some());
        let ids = [0, 7, 19];
        assert_eq!(
            reader
                .log_row_batches(&ids)
                .unwrap()
                .collect::<io::Result<Vec<_>>>()
                .unwrap()
                .concat(),
            ids.map(|id| rows[id as usize].clone())
        );
    }

    #[test]
    fn log_row_batches_reject_corrupt_source_metadata_and_payload() {
        for column in ["source.col", "block_hash.col", "data_len.col", "data.col"] {
            let tmp = TempDir::new().unwrap();
            ColumnFile::write_batch(tmp.path(), &make_rows()).unwrap();
            let path = tmp.path().join(column);
            let mut bytes = fs::read(&path).unwrap();
            match column {
                "source.col" => bytes[ColumnFileHeader::SIZE] = 255,
                "data_len.col" => bytes[ColumnFileHeader::SIZE..ColumnFileHeader::SIZE + 4]
                    .copy_from_slice(&3u32.to_le_bytes()),
                _ => bytes.truncate(ColumnFileHeader::SIZE),
            }
            fs::write(path, bytes).unwrap();
            let reader = SegmentReader::open(tmp.path()).unwrap();
            let prepared = reader.log_row_batches(&[0, 19]);
            if column == "source.col" || column == "block_hash.col" {
                assert_eq!(prepared.err().unwrap().kind(), io::ErrorKind::InvalidData);
            } else {
                let mut batches = prepared.unwrap();
                assert_eq!(
                    batches.next().unwrap().unwrap_err().kind(),
                    io::ErrorKind::InvalidData
                );
                assert!(batches.next().is_none());
                assert!(batches.next().is_none());
            }
        }
    }

    #[test]
    fn log_rows_reject_inconsistent_data_lengths() {
        let tmp = TempDir::new().unwrap();
        ColumnFile::write_batch(tmp.path(), &make_rows()).unwrap();
        let path = tmp.path().join("data_len.col");
        let mut bytes = fs::read(&path).unwrap();
        bytes[ColumnFileHeader::SIZE..ColumnFileHeader::SIZE + 4]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(path, bytes).unwrap();
        let reader = SegmentReader::open(tmp.path()).unwrap();
        assert_eq!(
            reader.read_log_rows(None).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(
            reader.read_log_rows(Some(&[0])).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn direct_raw_variable_reads_validate_and_retain_length_metadata() {
        let tmp = TempDir::new().unwrap();
        let rows = make_rows();
        ColumnFile::write_batch(tmp.path(), &rows).unwrap();

        let reader = SegmentReader::open_projected(tmp.path(), &["data"]).unwrap();
        assert_eq!(
            reader.read_var_bytes("data", None).unwrap(),
            vec![bytes!("deadbeef"); 20]
        );
        assert_eq!(
            reader.read_var_bytes("data", Some(&[19, 0, 7, 7])).unwrap(),
            vec![bytes!("deadbeef"); 4]
        );

        let length_path = tmp.path().join("data_len.col");
        let retired_path = tmp.path().join("data_len.col.retired");
        fs::rename(&length_path, &retired_path).unwrap();
        assert_eq!(
            reader.read_var_bytes("data", Some(&[3])).unwrap(),
            vec![bytes!("deadbeef")]
        );

        fs::rename(&retired_path, &length_path).unwrap();
        let mut lengths = fs::read(&length_path).unwrap();
        lengths[ColumnFileHeader::SIZE..ColumnFileHeader::SIZE + 4]
            .copy_from_slice(&3u32.to_le_bytes());
        lengths[ColumnFileHeader::SIZE + 4..ColumnFileHeader::SIZE + 8]
            .copy_from_slice(&5u32.to_le_bytes());
        fs::write(&length_path, lengths).unwrap();

        let corrupt = SegmentReader::open_projected(tmp.path(), &["data"]).unwrap();
        assert!(
            corrupt
                .var_bytes_batches("data", &[0])
                .unwrap()
                .next()
                .unwrap()
                .is_err()
        );
        for row_ids in [None, Some(&[1, 0, 1][..])] {
            assert_eq!(
                corrupt.read_var_bytes("data", row_ids).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
        }
        // A structurally valid shorter data file must not turn a selected
        // prefix into success when the captured segment still contains 20 rows.
        let shorter = TempDir::new().unwrap();
        ColumnFile::write_batch(shorter.path(), &rows[..19]).unwrap();
        fs::write(
            tmp.path().join("data.col"),
            fs::read(shorter.path().join("data.col")).unwrap(),
        )
        .unwrap();
        let mut batches = corrupt.var_bytes_batches("data", &[2]).unwrap();
        assert_eq!(
            batches.next().unwrap().unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(batches.next().is_none());
    }

    #[test]
    fn captured_raw_reader_keeps_replaced_source_snapshot() {
        let tmp = TempDir::new().unwrap();
        let original = make_rows();
        ColumnFile::write_batch(tmp.path(), &original).unwrap();
        let captured = SegmentReader::open(tmp.path()).unwrap();
        let captured_namespace = captured.source_namespace().unwrap();
        let batches = captured.var_bytes_batches("data", &[0, 19]).unwrap();

        let mut replacement = original.clone();
        replacement[0].block_number += 1_000;
        replacement[0].data = bytes!("aabbcc");
        replacement[0].data_len = 3;
        ColumnFile::write_batch(tmp.path(), &replacement).unwrap();

        assert_eq!(captured.read_log_rows(None).unwrap(), original);
        assert_eq!(
            batches.collect::<io::Result<Vec<_>>>().unwrap().concat(),
            [original[0].data.clone(), original[19].data.clone()]
        );
        let current = SegmentReader::open(tmp.path()).unwrap();
        assert_eq!(current.read_log_rows(None).unwrap(), replacement);
        assert_ne!(current.source_namespace().unwrap(), captured_namespace);
    }

    #[test]
    fn identified_capture_accepts_exact_previous_append_prefix() {
        let (_tmp, dir, descriptor, rows) = identified_raw_fixture();
        let append_dir = dir.clone();
        let suffix = rows[..1].to_vec();
        crate::column_artifact::BEFORE_CANONICAL_CAPTURE.with_borrow_mut(|hook| {
            *hook = Some(Box::new(move || {
                ColumnFile::append_batch(&append_dir, &suffix, descriptor.row_count).unwrap();
            }));
        });
        let reader = SegmentReader::open(&dir).unwrap();
        assert_eq!(reader.read_log_rows(None).unwrap(), rows);
        let ids = [rows.len() as u32 - 1, 0, rows.len() as u32 - 1];
        assert_eq!(
            crate::ColumnReader::read_log_rows(&dir, Some(&ids)).unwrap(),
            ids.map(|row| rows[row as usize].clone())
        );
        assert_eq!(
            reader.source_commitment().unwrap(),
            descriptor.source_commitment.map(|root| root.0)
        );
    }

    #[test]
    fn identified_raw_capture_rejects_replacement_before_canonical_capture() {
        let (_tmp, dir, _descriptor, rows) = identified_raw_fixture();
        let replace_dir = dir.clone();
        crate::column_artifact::BEFORE_CANONICAL_CAPTURE.with_borrow_mut(|hook| {
            *hook = Some(Box::new(move || {
                ColumnFile::write_batch(&replace_dir, &rows).unwrap()
            }));
        });
        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn identified_raw_capture_keeps_coherent_snapshot_after_canonical_capture() {
        for action in 0..3 {
            let (_tmp, dir, descriptor, rows) = identified_raw_fixture();
            let change_dir = dir.clone();
            let replacement = rows.clone();
            after_artifact_capture(move || match action {
                0 => ColumnFile::write_batch(&change_dir, &replacement).unwrap(),
                1 => crate::column::mark_source_updating_for_test(
                    &change_dir,
                    crate::column::SourceIdentity {
                        namespace: descriptor.source_namespace.unwrap().0,
                        generation: descriptor.generation,
                        segment_id: descriptor.id,
                        kind: descriptor.kind,
                    },
                )
                .unwrap(),
                _ => crate::column::remove_source_marker_for_test(&change_dir).unwrap(),
            });
            let captured = SegmentReader::open(&dir).unwrap();
            assert_eq!(captured.read_log_rows(None).unwrap(), rows);
            assert_eq!(captured.read_canonical_len().unwrap(), rows.len() as u64);
        }
    }

    #[test]
    fn native_manifest_before_commitment_format_is_rejected_without_writes() {
        let (_tmp, dir, _, _) = identified_raw_fixture();
        let path = dir.join("segment.json");
        let mut manifest = SegmentManifest::load(&path).unwrap().unwrap();
        manifest.format_version = 9;
        manifest.source_commitment = None;
        let old = serde_json::to_vec(&manifest).unwrap();
        fs::write(&path, &old).unwrap();
        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(path).unwrap(), old);
    }

    #[test]
    fn commitment_without_namespace_is_not_a_valid_manifest() {
        let (_tmp, dir, _, _) = identified_raw_fixture();
        let path = dir.join("segment.json");
        let mut manifest = SegmentManifest::load(&path).unwrap().unwrap();
        assert!(manifest.source_commitment.is_some());
        manifest.source_namespace = None;
        fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn legacy_manifest_retains_relative_canonical_path_support() {
        let (_tmp, dir, _, rows) = identified_raw_fixture();
        let path = dir.join("segment.json");
        let mut manifest = SegmentManifest::load(&path).unwrap().unwrap();
        manifest.source_namespace = None;
        manifest.source_commitment = None;
        manifest.canonical_rows_path = "legacy/canonical.bits".into();
        fs::create_dir(dir.join("legacy")).unwrap();
        let mut bitmap = NullBitmap::new();
        for row in 0..rows.len() {
            bitmap.push(row != 0);
        }
        let mut bytes = Vec::new();
        bitmap.write_to(&mut bytes).unwrap();
        fs::write(dir.join(&manifest.canonical_rows_path), bytes).unwrap();
        fs::remove_file(dir.join("canonical.bitmap")).unwrap();
        crate::column::remove_source_marker_for_test(&dir).unwrap();
        fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let reader = SegmentReader::open(&dir).unwrap();
        assert_eq!(reader.source_namespace(), None);
        assert_eq!(reader.read_log_rows(None).unwrap(), rows);
        assert_eq!(reader.read_canonical_len().unwrap(), rows.len() as u64);
        assert!(!reader.read_canonical().unwrap().is_present(0));
    }

    #[test]
    fn identified_raw_capture_rejects_invalid_canonical_and_all_descriptor_aliases() {
        for damage in 0..7 {
            let (_tmp, dir, _, _) = identified_raw_fixture();
            let path = dir.join("canonical.bitmap");
            let mut bytes = fs::read(&path).unwrap();
            match damage {
                0 => {
                    fs::remove_file(&path).unwrap();
                }
                1 => {
                    fs::write(&path, 20u64.to_le_bytes()).unwrap();
                }
                2 => {
                    bytes[9] = 1;
                    let crc = crc32fast::hash(&bytes[..50]);
                    bytes[50..54].copy_from_slice(&crc.to_le_bytes());
                    fs::write(&path, bytes).unwrap();
                }
                3 => {
                    bytes[10] ^= 1;
                    fs::write(&path, bytes).unwrap();
                }
                4 => {
                    bytes.push(0);
                    fs::write(&path, bytes).unwrap();
                }
                5 => {
                    bytes.pop();
                    fs::write(&path, bytes).unwrap();
                }
                _ => {
                    bytes[54] ^= 1;
                    fs::write(&path, bytes).unwrap();
                }
            }
            assert!(
                SegmentReader::open_projected(&dir, &[]).is_err(),
                "damage {damage}"
            );
        }
        for alias in 0..4 {
            let (_tmp, dir, _, _) = identified_raw_fixture();
            let path = dir.join("segment.json");
            let mut manifest = SegmentManifest::load(&path).unwrap().unwrap();
            let column = manifest
                .columns
                .iter_mut()
                .find(|column| column.name == "topic0")
                .unwrap();
            match alias {
                0 => column.data_path = "canonical.bitmap".into(),
                1 => column.page_index_path = Some("canonical.bitmap".into()),
                2 => column.null_bitmap_path = Some("canonical.bitmap".into()),
                _ => manifest.canonical_rows_path = "other.bitmap".into(),
            }
            fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
            assert!(
                SegmentReader::open_projected(&dir, &[]).is_err(),
                "alias {alias}"
            );
        }
    }

    #[test]
    fn verified_prefix_capture_accepts_both_canonical_states_and_preserves_bits() {
        for pending_canonical in [false, true] {
            let (_tmp, dir, descriptor, rows) = identified_raw_fixture();
            let mut canonical = NullBitmap::new();
            for index in 0..rows.len() {
                canonical.push(index != 0);
            }
            ColumnFile::replace_canonical_bitmap(&dir, &canonical).unwrap();
            let namespace = descriptor.source_namespace.unwrap().0;
            crate::column::mark_prefix_rewrite_for_test(
                &dir,
                namespace,
                descriptor.row_count,
                descriptor.generation,
                descriptor.id,
                descriptor.kind,
            )
            .unwrap();
            if pending_canonical {
                let path = dir.join("canonical.bitmap");
                let mut bytes = fs::read(&path).unwrap();
                bytes[9] = 1;
                let crc = crc32fast::hash(&bytes[..132]);
                bytes[132..136].copy_from_slice(&crc.to_le_bytes());
                fs::write(path, bytes).unwrap();
                assert_eq!(
                    SegmentReader::open(&dir).unwrap_err().kind(),
                    io::ErrorKind::WouldBlock
                );
            } else {
                // Complete canonical-last publication is query coherent even
                // while catalog finalization remains blocked by the sidecar.
                assert_eq!(
                    SegmentReader::open(&dir)
                        .unwrap()
                        .read_log_rows(None)
                        .unwrap(),
                    rows
                );
            }
            let owner = crate::column::begin_prefix_recovery(
                &dir,
                namespace,
                descriptor.row_count,
                descriptor.generation,
                descriptor.id,
                descriptor.kind,
                descriptor.source_commitment,
            )
            .unwrap();
            let recovering = SegmentReader::open_recovering_prefix(&owner).unwrap();
            assert_eq!(recovering.read_log_rows(None).unwrap(), rows);
            assert!(!recovering.read_canonical().unwrap().is_present(0));
            ColumnFile::rewrite_verified_prefix(
                &dir,
                &rows,
                &canonical,
                namespace,
                descriptor.generation,
                &owner,
            )
            .unwrap();
            assert!(crate::column::read_source_binding(&dir).is_err());
            assert!(
                !SegmentReader::open_recovering_prefix(&owner)
                    .unwrap()
                    .read_canonical()
                    .unwrap()
                    .is_present(0)
            );
            ColumnFile::finish_verified_prefix(&dir, &owner).unwrap();
            assert!(
                !SegmentReader::open(&dir)
                    .unwrap()
                    .read_canonical()
                    .unwrap()
                    .is_present(0)
            );
        }
    }

    #[test]
    fn identified_raw_capture_keeps_its_bound_prefix_across_append() {
        let (_tmp, dir, _, rows) = identified_raw_fixture();
        let append_dir = dir.clone();
        let mut appended = rows[0].clone();
        appended.block_number += 1_000;
        let existing_rows = rows.len() as u64;
        after_artifact_capture(move || {
            ColumnFile::append_batch(&append_dir, &[appended], existing_rows).unwrap();
        });

        let captured = SegmentReader::open(&dir).unwrap();
        assert_eq!(captured.read_row_count().unwrap(), rows.len() as u64);
        assert_eq!(captured.read_log_rows(None).unwrap(), rows);
        let canonical = captured.read_canonical().unwrap();
        assert!((0..rows.len() as u64).all(|row| canonical.is_present(row)));
    }

    #[test]
    fn identified_raw_capture_keeps_its_bound_prefix_across_exact_rewrite() {
        let (_tmp, dir, descriptor, rows) = identified_raw_fixture();
        let rewrite_dir = dir.clone();
        let rewrite_rows = rows.clone();
        after_artifact_capture(move || {
            let namespace = descriptor.source_namespace.unwrap().0;
            crate::column::mark_prefix_rewrite_for_test(
                &rewrite_dir,
                namespace,
                descriptor.row_count,
                descriptor.generation,
                descriptor.id,
                descriptor.kind,
            )
            .unwrap();
            let owner = crate::column::begin_prefix_recovery(
                &rewrite_dir,
                namespace,
                descriptor.row_count,
                descriptor.generation,
                descriptor.id,
                descriptor.kind,
                descriptor.source_commitment,
            )
            .unwrap();
            let mut canonical = NullBitmap::new();
            for _ in &rewrite_rows {
                canonical.push(true);
            }
            ColumnFile::rewrite_verified_prefix(
                &rewrite_dir,
                &rewrite_rows,
                &canonical,
                namespace,
                descriptor.generation,
                &owner,
            )
            .unwrap();
            ColumnFile::finish_verified_prefix(&rewrite_dir, &owner).unwrap();
        });

        let captured = SegmentReader::open(&dir).unwrap();
        assert_eq!(captured.read_row_count().unwrap(), rows.len() as u64);
        assert_eq!(captured.read_log_rows(None).unwrap(), rows);
        let canonical = captured.read_canonical().unwrap();
        assert!((0..rows.len() as u64).all(|row| canonical.is_present(row)));
    }

    #[test]
    fn identified_zero_row_capture_still_rejects_marker_replacement() {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let mut descriptor = SegmentDescriptor {
            column_bundle: None,
            source_namespace: None,
            source_commitment: None,
            source_state: None,
            id: 12,
            generation: 0,
            kind: SegmentKind::Hot,
            relative_path: PathBuf::from("segments/s_0000000000000012"),
            manifest_relative_path: PathBuf::from("segments/s_0000000000000012/segment.json"),
            min_block: None,
            max_block: None,
            min_timestamp: None,
            max_timestamp: None,
            row_count: 0,
        };
        let dir = paths.segment_dir(descriptor.id);
        write_native_raw(&dir, &[], &mut descriptor);
        persist_segment_manifest(&paths, &descriptor).unwrap();
        let replacement_dir = dir.clone();
        after_artifact_capture(move || {
            ColumnFile::write_batch(&replacement_dir, &[make_rows()[0].clone()]).unwrap();
        });
        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn legacy_capture_still_rejects_a_replacement_between_marker_reads() {
        let tmp = TempDir::new().unwrap();
        let rows = make_rows();
        ColumnFile::write_batch(tmp.path(), &rows).unwrap();
        let dir = tmp.path().to_path_buf();
        after_artifact_capture(move || ColumnFile::write_batch(&dir, &rows).unwrap());
        assert_eq!(
            SegmentReader::open(tmp.path()).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn reads_compacted_segment_rows() {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let mut descriptor = SegmentDescriptor {
            column_bundle: None,
            source_namespace: None,
            source_commitment: None,
            source_state: None,
            id: 1,
            generation: 0,
            kind: SegmentKind::Sealed,
            relative_path: PathBuf::from("segments").join("s_0000000000000001"),
            manifest_relative_path: PathBuf::from("segments")
                .join("s_0000000000000001")
                .join("segment.json"),
            min_block: Some(10),
            max_block: Some(29),
            min_timestamp: Some(1_700_000_000),
            max_timestamp: Some(1_700_000_228),
            row_count: 20,
        };
        let dir = paths.segment_dir(descriptor.id);
        fs::create_dir_all(&dir).unwrap();

        let rows = make_rows();
        write_native_raw(&dir, &rows, &mut descriptor);
        persist_segment_manifest(&paths, &descriptor).unwrap();
        assert_eq!(
            SegmentReader::open(&dir).unwrap().source_namespace(),
            descriptor.source_namespace.map(|namespace| namespace.0)
        );
        let before = load_manifest(&dir).unwrap();
        compact_segment(&paths, &descriptor).unwrap();

        // Deterministically put publication between loading the old manifest
        // and capturing its artifacts. Missing retired files must retry the new
        // manifest, while missing files in the same generation remain errors.
        let reader = SegmentReader::open_manifest(&dir, None, before).unwrap();
        assert_eq!(reader.read_canonical_len().unwrap(), rows.len() as u64);
        let reread = reader.read_log_rows(None).unwrap();
        assert_eq!(reread, rows);

        let selected = reader.read_log_rows(Some(&[1, 7, 12])).unwrap();
        assert_eq!(selected[0], rows[1]);
        assert_eq!(selected[1], rows[7]);
        assert_eq!(selected[2], rows[12]);
        let manifest = load_manifest(&dir).unwrap().unwrap();
        fs::remove_file(dir.join(&manifest.columns[0].data_path)).unwrap();
        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn selected_pages_preserve_arbitrary_order_and_duplicates() {
        let index: Vec<_> = (0..3)
            .map(|page| PageIndexEntry {
                first_row: page * 2,
                row_count: 2,
                offset: 0,
                encoded_len: 0,
            })
            .collect();
        for ids in [
            &[5, 4, 3, 2, 1, 0][..],
            &[4, 0, 4, 3, 1, 5],
            &[0, 1, 1, 5],
            &[],
        ] {
            let mut decoded_pages = Vec::new();
            let actual = read_selected_pages(Some(ids), &index, |entry| {
                decoded_pages.push(entry.first_row);
                Ok(vec![entry.first_row + 100, entry.first_row + 101])
            })
            .unwrap();
            assert_eq!(
                actual,
                ids.iter()
                    .map(|id| u64::from(*id) + 100)
                    .collect::<Vec<_>>()
            );
            let mut expected_pages: Vec<_> = ids.iter().map(|id| u64::from(*id / 2) * 2).collect();
            expected_pages.sort_unstable();
            expected_pages.dedup();
            assert_eq!(
                decoded_pages, expected_pages,
                "each selected page decodes once"
            );
        }
        assert!(build_selections(&[6, 0], &index).is_err());
    }

    #[test]
    fn compacted_log_rows_support_descending_page_crossings() {
        let tmp = TempDir::new().unwrap();
        let mut storage = crate::PartitionManager::open(crate::PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 100_000,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        let prototypes = make_rows();
        let rows: Vec<_> = (0..32_769)
            .map(|index| {
                let mut row = prototypes[index % prototypes.len()].clone();
                row.block_number = index as u64;
                row.timestamp = 1_700_000_000 + index as u64 * 12;
                row
            })
            .collect();
        storage.write_historical_batch(&rows).unwrap();
        storage.finalize_historical_segment().unwrap();
        let partition = &storage.sealed_partitions()[0];
        let reader = SegmentReader::open(&partition.meta.path).unwrap();
        let ids = [32_768, 16_384, 1, 16_383, 0, 32_768];
        let expected: Vec<_> = ids.iter().map(|id| rows[*id as usize].clone()).collect();
        assert_eq!(reader.read_log_rows(Some(&ids)).unwrap(), expected);
    }

    #[test]
    fn corrupted_page_index_is_rejected() {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let mut descriptor = SegmentDescriptor {
            column_bundle: None,
            source_namespace: None,
            source_commitment: None,
            source_state: None,
            id: 2,
            generation: 0,
            kind: SegmentKind::Sealed,
            relative_path: PathBuf::from("segments").join("s_0000000000000002"),
            manifest_relative_path: PathBuf::from("segments")
                .join("s_0000000000000002")
                .join("segment.json"),
            min_block: Some(10),
            max_block: Some(29),
            min_timestamp: Some(1_700_000_000),
            max_timestamp: Some(1_700_000_228),
            row_count: 20,
        };
        let dir = paths.segment_dir(descriptor.id);
        fs::create_dir_all(&dir).unwrap();

        write_native_raw(&dir, &make_rows(), &mut descriptor);
        persist_segment_manifest(&paths, &descriptor).unwrap();
        compact_segment(&paths, &descriptor).unwrap();

        let manifest: SegmentManifest =
            serde_json::from_slice(&fs::read(paths.segment_manifest_path(descriptor.id)).unwrap())
                .unwrap();
        let topic0 = manifest
            .columns
            .iter()
            .find(|column| column.name == "topic0")
            .unwrap();
        let page_index_path = dir.join(topic0.page_index_path.as_ref().unwrap());
        let mut file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(page_index_path)
            .unwrap();
        file.write_all(b"bad").unwrap();
        file.flush().unwrap();

        let reader = SegmentReader::open(&dir).unwrap();
        let err = reader
            .read_nullable_b256("topic0", Some(&[0]))
            .expect_err("page-index corruption should fail reads");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    fn compacted_fixture() -> (TempDir, PathBuf) {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let mut descriptor = SegmentDescriptor {
            column_bundle: None,
            source_namespace: None,
            source_commitment: None,
            source_state: None,
            id: 1,
            generation: 0,
            kind: SegmentKind::Sealed,
            relative_path: PathBuf::from("segments/s_0000000000000001"),
            manifest_relative_path: PathBuf::from("segments/s_0000000000000001/segment.json"),
            min_block: Some(10),
            max_block: Some(29),
            min_timestamp: Some(1_700_000_000),
            max_timestamp: Some(1_700_000_228),
            row_count: 20,
        };
        let dir = paths.segment_dir(descriptor.id);
        fs::create_dir_all(&dir).unwrap();
        write_native_raw(&dir, &make_rows(), &mut descriptor);
        persist_segment_manifest(&paths, &descriptor).unwrap();
        compact_segment(&paths, &descriptor).unwrap();
        (tmp, dir)
    }

    #[test]
    fn unbundled_manifest_rejects_unknown_column_name() {
        let (_tmp, dir) = compacted_fixture();
        let mut manifest = load_manifest(&dir).unwrap().unwrap();
        manifest.columns[0].name = "unknown_column".to_owned();
        fs::write(
            dir.join("segment.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();

        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    fn assert_variable_page_row_count_is_checked(row_ids: Option<&[u32]>) {
        let (_tmp, dir) = compacted_fixture();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        let data = manifest.columns.iter().find(|c| c.name == "data").unwrap();

        // Both files are valid individually: only their row counts disagree.
        // Exercise too few and too many values, plus the valid control.
        for count in [19, 21, 20] {
            let values = vec![bytes!("deadbeef"); count];
            let encoded = crate::page::encode_var_bytes_page(&values, data.codec).unwrap();
            fs::write(dir.join(&data.data_path), &encoded).unwrap();
            fs::write(
                dir.join(data.page_index_path.as_ref().unwrap()),
                crate::page::write_page_index(&[PageIndexEntry {
                    first_row: 0,
                    row_count: 20,
                    offset: 0,
                    encoded_len: encoded.len() as u32,
                }]),
            )
            .unwrap();
            let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
            let result = reader.read_var_bytes("data", row_ids);
            if count == 20 {
                assert_eq!(
                    result.unwrap(),
                    vec![bytes!("deadbeef"); row_ids.map_or(20, <[u32]>::len)]
                );
            } else {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
            }
        }
    }

    #[test]
    fn full_variable_page_read_rejects_wrong_row_count() {
        assert_variable_page_row_count_is_checked(None);
    }

    #[test]
    fn selected_variable_page_read_rejects_wrong_row_count() {
        assert_variable_page_row_count_is_checked(Some(&[19, 0, 19]));
        // An extra row must also fail when selecting an otherwise valid prefix.
        assert_variable_page_row_count_is_checked(Some(&[0]));
    }

    #[test]
    fn direct_variable_reads_reject_inconsistent_per_row_lengths() {
        let (_tmp, dir) = compacted_fixture();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        let data = manifest.columns.iter().find(|c| c.name == "data").unwrap();
        let mut values = vec![bytes!("deadbeef"); 20];
        values[0] = bytes!("aabbcc");
        values[1] = bytes!("aabbccddee");
        let encoded = crate::page::encode_var_bytes_page(&values, data.codec).unwrap();
        fs::write(dir.join(&data.data_path), &encoded).unwrap();
        fs::write(
            dir.join(data.page_index_path.as_ref().unwrap()),
            crate::page::write_page_index(&[PageIndexEntry {
                first_row: 0,
                row_count: 20,
                offset: 0,
                encoded_len: encoded.len() as u32,
            }]),
        )
        .unwrap();

        let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
        assert_eq!(
            reader
                .var_bytes_batches("data", &[19])
                .unwrap()
                .next()
                .unwrap()
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        for row_ids in [None, Some(&[1, 0, 1][..]), Some(&[19][..])] {
            let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
            let Err(error) = reader.read_var_bytes("data", row_ids) else {
                panic!("inconsistent lengths must be rejected");
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn direct_variable_reads_bound_compressed_expansion_to_stored_lengths() {
        let (_tmp, dir) = compacted_fixture();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        let data = manifest.columns.iter().find(|c| c.name == "data").unwrap();
        let mut values = vec![bytes!("deadbeef"); 20];
        values[0] = Bytes::from(vec![0x5a; 64 * 1024]);
        let encoded = crate::page::encode_var_bytes_page(&values, data.codec).unwrap();
        fs::write(dir.join(&data.data_path), &encoded).unwrap();
        fs::write(
            dir.join(data.page_index_path.as_ref().unwrap()),
            crate::page::write_page_index(&[PageIndexEntry {
                first_row: 0,
                row_count: 20,
                offset: 0,
                encoded_len: encoded.len() as u32,
            }]),
        )
        .unwrap();

        let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
        assert!(
            reader
                .var_bytes_batches("data", &[0])
                .unwrap()
                .next()
                .unwrap()
                .is_err()
        );
        let Err(error) = reader.read_var_bytes("data", Some(&[0])) else {
            panic!("inconsistent page size must be rejected");
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn variable_reads_correlate_different_data_and_length_page_layouts() {
        let (_tmp, dir) = compacted_fixture();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        let values: Vec<_> = (0..20)
            .map(|row| Bytes::from(vec![row as u8; row % 7]))
            .collect();
        let lengths: Vec<_> = values.iter().map(|value| value.len() as u32).collect();
        for (name, ranges) in [
            ("data", vec![0..11, 11..20]),
            ("data_len", vec![0..7, 7..13, 13..20]),
        ] {
            let descriptor = manifest
                .columns
                .iter()
                .find(|column| column.name == name)
                .unwrap();
            let mut encoded = Vec::new();
            let mut index = Vec::new();
            for range in ranges {
                let page = if name == "data" {
                    crate::page::encode_var_bytes_page(&values[range.clone()], descriptor.codec)
                } else {
                    crate::page::encode_u32_page(&lengths[range.clone()], descriptor.codec)
                }
                .unwrap();
                index.push(PageIndexEntry {
                    first_row: range.start as u64,
                    row_count: (range.end - range.start) as u32,
                    offset: encoded.len() as u64,
                    encoded_len: page.len() as u32,
                });
                encoded.extend(page);
            }
            fs::write(dir.join(&descriptor.data_path), encoded).unwrap();
            fs::write(
                dir.join(descriptor.page_index_path.as_ref().unwrap()),
                crate::page::write_page_index(&index),
            )
            .unwrap();
        }

        let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
        assert_eq!(reader.read_var_bytes("data", None).unwrap(), values);
        let ids = [19, 0, 8, 11, 8];
        assert_eq!(
            reader.read_var_bytes("data", Some(&ids)).unwrap(),
            ids.map(|row| values[row as usize].clone())
        );
        BATCH_PAGE_READS.with_borrow_mut(|reads| *reads = Some(Vec::new()));
        let sorted = [0, 8, 11, 19];
        let batches = reader
            .var_bytes_batches("data", &sorted)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        let reads = BATCH_PAGE_READS.with_borrow_mut(Option::take).unwrap();
        assert_eq!(batches.iter().map(Vec::len).collect::<Vec<_>>(), [2, 2]);
        assert_eq!(
            batches.concat(),
            sorted.map(|row| values[row as usize].clone())
        );
        let expected_reads = [
            ("data_len".to_owned(), 0),
            ("data_len".to_owned(), 7),
            ("data".to_owned(), 0),
            ("data_len".to_owned(), 13),
            ("data".to_owned(), 11),
        ];
        assert_eq!(reads, expected_reads);

        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(&dir, &["data"], memory.clone()).unwrap();
        BATCH_PAGE_READS.with_borrow_mut(|reads| *reads = Some(Vec::new()));
        let batches = reader
            .var_bytes_batches_with_memory("data", &sorted)
            .unwrap()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        let reads = BATCH_PAGE_READS.with_borrow_mut(Option::take).unwrap();
        assert_eq!(
            batches.iter().map(|batch| batch.len()).collect::<Vec<_>>(),
            [2, 2]
        );
        assert!(
            batches
                .iter()
                .flat_map(|batch| batch.iter())
                .eq(sorted.iter().map(|&row| &values[row as usize]))
        );
        assert_eq!(reads, expected_reads, "accounting must not reread pages");
        drop(batches);
        drop(reader);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn variable_batches_retain_raw_buffers_and_validate_selection() {
        let tmp = TempDir::new().unwrap();
        let rows = vec![make_rows()[0].clone(); crate::page::MAX_PAGE_ROWS as usize + 1];
        ColumnFile::write_batch(tmp.path(), &rows).unwrap();
        let reader = SegmentReader::open_projected(tmp.path(), &["data"]).unwrap();
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(4 * 1024 * 1024).unwrap());
        let accounted =
            SegmentReader::open_projected_with_memory(tmp.path(), &["data"], memory.clone())
                .unwrap();
        assert!(
            reader
                .var_bytes_batches("data", &[])
                .unwrap()
                .next()
                .is_none()
        );
        for ids in [&[1, 0][..], &[0, 0], &[rows.len() as u32]] {
            assert_eq!(
                reader.var_bytes_batches("data", ids).err().unwrap().kind(),
                io::ErrorKind::InvalidInput
            );
            assert_eq!(
                accounted
                    .var_bytes_batches_with_memory("data", ids)
                    .err()
                    .unwrap()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        assert!(reader.var_bytes_batches("source", &[0]).is_err());
        assert!(
            accounted
                .var_bytes_batches_with_memory("source", &[0])
                .is_err()
        );
        assert!(
            accounted
                .var_bytes_batches_with_memory("data", &[])
                .unwrap()
                .next()
                .is_none()
        );
        let ids: Vec<_> = (0..rows.len() as u32).collect();
        let mut batches = reader.var_bytes_batches("data", &ids).unwrap();
        let baseline = memory.used();
        let mut owned_batches = accounted
            .var_bytes_batches_with_memory("data", &ids)
            .unwrap();
        assert_eq!(memory.used(), baseline);
        let first = batches.next().unwrap().unwrap();
        assert_eq!(
            first,
            vec![rows[0].data.clone(); crate::page::MAX_PAGE_ROWS as usize]
        );
        let owned_first = owned_batches.next().unwrap().unwrap();
        assert_eq!(&*owned_first, first.as_slice());
        let alias = owned_first[0].clone();
        drop(owned_first);
        assert!(memory.used() > baseline);
        // Once prepared, later batches must use the validated retained buffers,
        // even if the captured files are subsequently damaged in place.
        fs::write(tmp.path().join("data.col"), []).unwrap();
        fs::write(tmp.path().join("data_len.col"), []).unwrap();
        assert_eq!(batches.next().unwrap().unwrap(), [rows[0].data.clone()]);
        assert!(batches.next().is_none());
        assert!(batches.next().is_none());
        assert_eq!(
            &*owned_batches.next().unwrap().unwrap(),
            &[rows[0].data.clone()]
        );
        assert!(owned_batches.next().is_none());
        drop(owned_batches);
        drop(accounted);
        assert_eq!(alias, rows[0].data);
        assert_eq!(
            memory.used(),
            (crate::page::MAX_PAGE_ROWS as usize * rows[0].data.len()) as u128
        );
        drop(alias);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn variable_batches_initialize_lazily_and_stop_after_corruption() {
        let (_tmp, dir) = compacted_fixture();
        let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
        let mut batches = reader.var_bytes_batches("data", &[0, 19]).unwrap();
        let memory =
            QueryMemoryBudget::new(logex_types::QueryMemoryLimit::new(1024 * 1024).unwrap());
        let accounted =
            SegmentReader::open_projected_with_memory(&dir, &["data"], memory.clone()).unwrap();
        let baseline = memory.used();
        let mut owned_batches = accounted
            .var_bytes_batches_with_memory("data", &[0, 19])
            .unwrap();
        assert_eq!(memory.used(), baseline);
        let descriptor = reader.compacted_column("data").unwrap();
        fs::write(dir.join(&descriptor.data_path), []).unwrap();
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
        assert!(owned_batches.next().unwrap().is_err());
        assert!(owned_batches.next().is_none());
        assert_eq!(memory.used(), baseline);
        drop(owned_batches);
        drop(accounted);
        assert_eq!(memory.used(), 0);
        assert!(
            reader
                .var_bytes_batches("data", &[])
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn variable_batches_support_mixed_raw_and_paged_columns() {
        for raw_name in ["data", "data_len"] {
            let (_tmp, dir) = compacted_fixture();
            let raw = TempDir::new().unwrap();
            let rows = make_rows();
            ColumnFile::write_batch(raw.path(), &rows).unwrap();
            let mut manifest = load_manifest(&dir).unwrap().unwrap();
            let descriptor = manifest
                .columns
                .iter_mut()
                .find(|column| column.name == raw_name)
                .unwrap();
            descriptor.codec = crate::native::CompressionCodec::None;
            descriptor.page_index_path = None;
            descriptor.data_path = format!("{raw_name}.col");
            fs::copy(
                raw.path().join(&descriptor.data_path),
                dir.join(&descriptor.data_path),
            )
            .unwrap();
            fs::write(
                dir.join("segment.json"),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
            let ids = [0, 3, 19];
            let batches = reader
                .var_bytes_batches("data", &ids)
                .unwrap()
                .collect::<io::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(
                batches.concat(),
                ids.map(|row| rows[row as usize].data.clone())
            );
            let expected: Vec<_> = rows.iter().map(|row| row.data.clone()).collect();
            assert_accounted_variable_fixture(&dir, &expected);
        }
    }

    #[test]
    fn projected_data_reader_retains_only_its_required_artifacts_after_rename() {
        let (_tmp, dir) = compacted_fixture();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        assert!(manifest.column_bundle.is_none());
        let data_reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
        let length_reader = SegmentReader::open_projected(&dir, &["data_len"]).unwrap();
        let batches = data_reader.var_bytes_batches("data", &[0, 7, 19]).unwrap();

        for descriptor in manifest
            .columns
            .iter()
            .filter(|column| matches!(column.name.as_str(), "data" | "data_len"))
        {
            for path in
                std::iter::once(&descriptor.data_path).chain(descriptor.page_index_path.iter())
            {
                fs::rename(dir.join(path), dir.join(format!("{path}.retired"))).unwrap();
            }
        }

        assert_eq!(
            data_reader
                .read_var_bytes("data", Some(&[19, 0, 7, 7]))
                .unwrap(),
            vec![bytes!("deadbeef"); 4]
        );
        assert_eq!(
            length_reader
                .read_u32("data_len", Some(&[19, 0, 7, 7]))
                .unwrap(),
            vec![4; 4]
        );
        assert_eq!(
            batches.collect::<io::Result<Vec<_>>>().unwrap().concat(),
            vec![bytes!("deadbeef"); 3]
        );
        assert!(
            length_reader.read_var_bytes("data", Some(&[0])).is_err(),
            "data_len-only projection unexpectedly retained the payload"
        );
        assert_eq!(
            SegmentReader::open_projected(&dir, &["data"])
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn integrity_short_topic_bitmap_is_not_a_null_value() {
        let (_tmp, dir) = compacted_fixture();
        let manifest = load_manifest(&dir).unwrap().unwrap();
        let topic = manifest
            .columns
            .iter()
            .find(|c| c.name == "topic0")
            .unwrap();
        for len in [0, 19, 20, 21] {
            let mut bitmap = NullBitmap::new();
            for row in 0..len {
                bitmap.push(row % 2 == 1);
            }
            let mut bytes = Vec::new();
            bitmap.write_to(&mut bytes).unwrap();
            fs::write(dir.join(topic.null_bitmap_path.as_ref().unwrap()), &bytes).unwrap();
            let reader = SegmentReader::open_projected(&dir, &["topic0"]).unwrap();
            for ids in [None, Some(&[19, 0, 19][..])] {
                let result = reader.read_nullable_b256("topic0", ids);
                if len < 20 {
                    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
                } else {
                    let rows = ids.map_or_else(|| (0..20).collect(), <[u32]>::to_vec);
                    assert_eq!(
                        result.unwrap(),
                        rows.iter()
                            .map(|i| (i % 2 == 1).then(|| B256::repeat_byte(0xaa)))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }

    #[test]
    fn integrity_short_canonical_bitmap_is_not_a_noncanonical_row() {
        let (_tmp, dir) = compacted_fixture();
        for len in [0, 19, 20, 21] {
            let mut bitmap = NullBitmap::new();
            for row in 0..len {
                bitmap.push(row % 2 == 1);
            }
            let mut bytes = Vec::new();
            let manifest = load_manifest(&dir).unwrap().unwrap();
            let prefix_rows = make_rows()
                .into_iter()
                .cycle()
                .take(len as usize)
                .collect::<Vec<_>>();
            let prefix =
                crate::PrefixState::from_rows(manifest.source_namespace.unwrap().0, &prefix_rows)
                    .unwrap();
            crate::column::write_raw_canonical_with_previous(
                &mut bytes,
                &bitmap,
                manifest
                    .source_namespace
                    .map(|namespace| crate::column::SourceBinding {
                        namespace: namespace.0,
                        generation: manifest.generation,
                        segment_id: manifest.segment_id,
                    }),
                Some(prefix.commitment()),
                Some(&prefix),
                (len > 20).then(|| (20, manifest.source_commitment.unwrap())),
            )
            .unwrap();
            fs::write(dir.join("canonical.bitmap"), &bytes).unwrap();
            let captured = SegmentReader::open_projected(&dir, &[]);
            if len < 20 {
                assert_eq!(captured.unwrap_err().kind(), io::ErrorKind::InvalidData);
            } else {
                let reader = captured.unwrap();
                let bitmap = reader.read_canonical().unwrap();
                assert!(bitmap.is_present(19));
                assert!(!bitmap.is_present(0));
                assert_eq!(reader.read_canonical_len().unwrap(), len);
            }
        }
    }

    #[test]
    fn integrity_manifest_reads_are_bounded() {
        let (_tmp, dir) = compacted_fixture();
        let path = dir.join("segment.json");
        let mut bytes = fs::read(&path).unwrap();
        // Valid JSON with excessive trailing whitespace, not a parse error.
        for size in [1024 * 1024, 1024 * 1024 + 1] {
            bytes.resize(size, b' ');
            fs::write(&path, &bytes).unwrap();
            let result = SegmentReader::open(&dir);
            if size == 1024 * 1024 {
                assert_eq!(result.unwrap().read_row_count().unwrap(), 20);
            } else {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
            }
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }

    #[cfg(unix)]
    #[test]
    fn integrity_dangling_manifest_does_not_fall_back_to_raw_data() {
        let tmp = TempDir::new().unwrap();
        ColumnFile::write_batch(tmp.path(), &make_rows()).unwrap();
        let manifest = tmp.path().join("segment.json");
        std::os::unix::fs::symlink("unavailable-manifest", &manifest).unwrap();
        assert_eq!(
            SegmentReader::open(tmp.path()).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            crate::ColumnReader::read_log_rows(tmp.path(), None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(
            fs::read_link(manifest).unwrap(),
            Path::new("unavailable-manifest")
        );
    }

    #[test]
    fn integrity_unbundled_page_rows_cannot_increase_decoder_limits() {
        let (_tmp, dir) = compacted_fixture();
        let mut manifest = load_manifest(&dir).unwrap().unwrap();
        manifest
            .columns
            .iter_mut()
            .find(|c| c.name == "data_len")
            .unwrap()
            .page_rows = u32::MAX;
        fs::write(
            dir.join("segment.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn integrity_manifest_rows_must_fit_segment_addressing() {
        let (_tmp, dir) = compacted_fixture();
        let mut manifest = load_manifest(&dir).unwrap().unwrap();
        manifest.row_count = u64::from(u32::MAX) + 1;
        fs::write(
            dir.join("segment.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        assert_eq!(
            SegmentReader::open(&dir).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn integrity_raw_row_count_validates_header_and_addressing() {
        let tmp = TempDir::new().unwrap();
        for (version, compression, row_count) in [
            (2, 0, 0),
            (1, 1, 0),
            (1, 0, u64::MAX),
            (1, 0, u64::from(u32::MAX)),
            (1, 0, 1),
            (1, 0, 0),
        ] {
            let mut bytes = Vec::new();
            ColumnFileHeader {
                version,
                compression,
                row_count,
            }
            .write_to(&mut bytes)
            .unwrap();
            fs::write(tmp.path().join("address.col"), bytes).unwrap();
            let result = SegmentReader::open(tmp.path()).and_then(|reader| reader.read_row_count());
            if (version, compression, row_count) == (1, 0, 0) {
                assert_eq!(result.unwrap(), 0);
            } else {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
            }
        }
    }

    #[test]
    fn canonical_len_rejects_truncated_bitmap() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("segment");
        fs::create_dir_all(&dir).unwrap();
        ColumnFile::write_batch(&dir, &make_rows()).unwrap();
        fs::write(dir.join("canonical.bitmap"), 20u64.to_le_bytes()).unwrap();

        let reader = SegmentReader::open(&dir).unwrap();
        let err = reader
            .read_canonical_len()
            .expect_err("truncated canonical bitmap should fail");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
