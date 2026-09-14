use std::io;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, Bytes};
use logex_types::{LogRow, Source};

use crate::column_artifact::ColumnArtifacts;
use crate::native::{ColumnDescriptor, SegmentManifest};
use crate::page::{
    PageIndexEntry, decode_fixed_width_page, decode_u8_page, decode_u32_page, decode_u64_page,
    decode_var_bytes_page_bounded, read_page_index,
};
use crate::reader::{RawBytesColumn, RawFixedColumn};
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
}

#[derive(Debug, Clone)]
struct PageSelection {
    entry: PageIndexEntry,
    local_rows: Vec<usize>,
    output_positions: Vec<usize>,
}

enum BatchPayload<'a> {
    Raw(RawBytesColumn),
    Paged {
        descriptor: &'a ColumnDescriptor,
        entries: Vec<PageIndexEntry>,
    },
}

enum BatchLengths<'a> {
    Raw(RawFixedColumn<4>),
    Paged {
        descriptor: &'a ColumnDescriptor,
        entries: Vec<PageIndexEntry>,
        cached: Option<(PageIndexEntry, Vec<u32>)>,
    },
}

impl BatchLengths<'_> {
    fn row(&mut self, reader: &SegmentReader, row: u64) -> io::Result<u32> {
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
                    let values = decode_u32_page(
                        &reader.read_page_payload(descriptor, &entry)?,
                        entry.row_count as usize,
                        descriptor.codec,
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
    fn new(reader: &'a SegmentReader) -> io::Result<Self> {
        let payload = match reader.compacted_column("data") {
            Some(descriptor) => BatchPayload::Paged {
                descriptor,
                entries: reader.read_compacted_page_index(descriptor, None)?,
            },
            None => {
                let column = RawBytesColumn::from_bytes(
                    &reader.dir.join("data.col"),
                    reader.artifacts.read("data.col")?,
                )?;
                if (column.row_count() as u64) < reader.read_row_count()? {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "raw data does not cover the captured segment rows",
                    ));
                }
                BatchPayload::Raw(column)
            }
        };
        let lengths = match reader.compacted_column("data_len") {
            Some(descriptor) => BatchLengths::Paged {
                descriptor,
                entries: reader.read_compacted_page_index(descriptor, None)?,
                cached: None,
            },
            None => BatchLengths::Raw(reader.raw_fixed::<4>("data_len.col", None)?),
        };
        Ok(Self { payload, lengths })
    }

    fn next_batch(
        &mut self,
        reader: &SegmentReader,
        remaining: &mut &[u32],
    ) -> io::Result<Vec<Bytes>> {
        let count;
        let mut output = Vec::new();
        match &self.payload {
            BatchPayload::Raw(column) => {
                count = remaining.len().min(crate::page::MAX_PAGE_ROWS as usize);
                output.try_reserve_exact(count).map_err(io::Error::other)?;
                for &row in &remaining[..count] {
                    let bytes = column.row(row as usize)?;
                    if bytes.len() != self.lengths.row(reader, u64::from(row))? as usize {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "data bytes differ from their row-length metadata",
                        ));
                    }
                    output.push(Bytes::copy_from_slice(bytes));
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
                let expected: Vec<u32> = (entry.first_row..end)
                    .map(|row| self.lengths.row(reader, row))
                    .collect::<io::Result<_>>()?;
                let page = decode_var_bytes_page_bounded(
                    &reader.read_page_payload(descriptor, entry)?,
                    descriptor.codec,
                    &expected,
                )?;
                output.try_reserve_exact(count).map_err(io::Error::other)?;
                for &row in &remaining[..count] {
                    output.push(page[(u64::from(row) - entry.first_row) as usize].clone());
                }
            }
        }
        *remaining = &remaining[count..];
        Ok(output)
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

    /// Capture only the columns needed by a query, plus canonicality and row-count
    /// metadata. Include predicate, ordering and output columns, including those
    /// needed if an index is unavailable. Other columns may not be readable.
    pub fn open_projected(dir: &Path, columns: &[&str]) -> io::Result<Self> {
        Self::open_inner(dir, Some(columns))
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
        })
    }

    fn open_inner(dir: &Path, projection: Option<&[&str]>) -> io::Result<Self> {
        Self::open_manifest(dir, projection, load_manifest(dir)?)
    }

    fn open_manifest(
        dir: &Path,
        projection: Option<&[&str]>,
        mut manifest: Option<SegmentManifest>,
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
            match ColumnArtifacts::open_projected(dir, manifest.as_ref(), projection) {
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

    fn raw_fixed<const WIDTH: usize>(
        &self,
        path: &str,
        row_ids: Option<&[u32]>,
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
        RawFixedColumn::from_bytes(&self.dir.join(path), self.artifacts.read(path)?, prefix)
    }

    pub fn read_address(&self, row_ids: Option<&[u32]>) -> io::Result<Vec<Address>> {
        if self.compacted_column("address").is_none() {
            return self
                .raw_fixed::<20>("address.col", row_ids)?
                .materialize(row_ids, |_, value| Address::from(*value));
        }

        self.read_fixed_width_values("address", 20, row_ids)?
            .into_iter()
            .map(|bytes| Ok(Address::from_slice(&bytes)))
            .collect()
    }

    pub fn read_b256(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<B256>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed::<32>(raw_column_path(column), row_ids)?
                .materialize(row_ids, |_, value| B256::from(*value));
        }

        self.read_fixed_width_values(column, 32, row_ids)?
            .into_iter()
            .map(|bytes| Ok(B256::from_slice(&bytes)))
            .collect()
    }

    pub fn read_nullable_b256(
        &self,
        column: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<Vec<Option<B256>>> {
        if self.compacted_column(column).is_none() {
            let fixed = self.raw_fixed::<32>(&format!("{column}.col"), row_ids)?;
            let path = format!("{column}.null");
            let nulls = fixed.read_nulls_from_bytes(
                &self.dir.join(&path),
                &self.artifacts.read(&path)?,
                self.manifest.is_some() || row_ids.is_some_and(|ids| !ids.is_empty()),
            )?;
            return fixed.materialize(row_ids, |row, value| {
                nulls.is_present(row as u64).then(|| B256::from(*value))
            });
        }

        let values = self.read_fixed_width_values(column, 32, row_ids)?;
        let nulls = self.read_null_bitmap(column)?;

        let mut result = Vec::with_capacity(values.len());
        match row_ids {
            Some(ids) => {
                for (idx, bytes) in ids.iter().zip(values) {
                    if nulls.is_present(*idx as u64) {
                        result.push(Some(B256::from_slice(&bytes)));
                    } else {
                        result.push(None);
                    }
                }
            }
            None => {
                for (idx, bytes) in values.into_iter().enumerate() {
                    if nulls.is_present(idx as u64) {
                        result.push(Some(B256::from_slice(&bytes)));
                    } else {
                        result.push(None);
                    }
                }
            }
        }

        Ok(result)
    }

    pub fn read_u64(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u64>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed::<8>(raw_column_path(column), row_ids)?
                .materialize(row_ids, |_, value| u64::from_le_bytes(*value));
        }
        self.read_u64_values(column, row_ids)
    }

    pub fn read_u32(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u32>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed::<4>(raw_column_path(column), row_ids)?
                .materialize(row_ids, |_, value| u32::from_le_bytes(*value));
        }
        self.read_u32_values(column, row_ids)
    }

    pub fn read_u8(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u8>> {
        if self.compacted_column(column).is_none() {
            return self
                .raw_fixed::<1>(raw_column_path(column), row_ids)?
                .materialize(row_ids, |_, value| value[0]);
        }
        self.read_u8_values(column, row_ids)
    }

    pub fn read_var_bytes(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<Bytes>> {
        self.read_var_bytes_with_lengths(column, row_ids)
            .map(|(values, _)| values)
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
                    prepared = Some(PreparedVarBytes::new(self)?);
                }
                prepared
                    .as_mut()
                    .expect("initialized above")
                    .next_batch(self, &mut remaining)
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
                .read_range("address.col", 0..ColumnFileHeader::SIZE as u64)?;
            let header = ColumnFileHeader::read_from(&data)
                .filter(|header| {
                    header.version == crate::column::COLUMN_VERSION
                        && header.compression == 0
                        && header.row_count <= u64::from(u32::MAX)
                })
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "corrupt captured row-count header",
                    )
                })?;
            let expected = (ColumnFileHeader::SIZE as u64) + header.row_count * 20;
            if self.artifacts.len("address.col")? != expected {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "raw row count does not match its address column",
                ));
            }
            Ok(header.row_count)
        }
    }

    pub fn read_log_rows(&self, row_ids: Option<&[u32]>) -> io::Result<Vec<LogRow>> {
        self.materialize_log_rows(row_ids, None)
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
        let addresses = self.read_address(row_ids)?;
        let block_numbers = self.read_u64("block_number", row_ids)?;
        let block_hashes = self.read_b256("block_hash", row_ids)?;
        let timestamps = self.read_u64("timestamp", row_ids)?;
        let tx_hashes = self.read_b256("tx_hash", row_ids)?;
        let tx_indices = self.read_u32("tx_index", row_ids)?;
        let log_indices = self.read_u32("log_index", row_ids)?;
        let topic0s = self.read_nullable_b256("topic0", row_ids)?;
        let topic1s = self.read_nullable_b256("topic1", row_ids)?;
        let topic2s = self.read_nullable_b256("topic2", row_ids)?;
        let topic3s = self.read_nullable_b256("topic3", row_ids)?;
        let (data, data_lens) = match data {
            Some(data) => data,
            None => self.read_var_bytes_with_lengths("data", row_ids)?,
        };
        let sources = self.read_u8("source", row_ids)?;

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
            data_lens.len(),
            sources.len(),
        ]
        .into_iter()
        .any(|len| len != addresses.len())
            || data
                .iter()
                .zip(&data_lens)
                .any(|(bytes, &len)| bytes.len() as u64 != u64::from(len))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "log columns have inconsistent row or data lengths",
            ));
        }

        let mut rows = Vec::with_capacity(addresses.len());
        for index in 0..addresses.len() {
            rows.push(LogRow {
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
                data_len: data_lens[index],
                source: Source::from_u8(sources[index]).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid source byte: {}", sources[index]),
                    )
                })?,
            });
        }

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

    fn read_fixed_width_values(
        &self,
        column: &str,
        item_size: usize,
        row_ids: Option<&[u32]>,
    ) -> io::Result<Vec<Vec<u8>>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let page_index = self.read_compacted_page_index(descriptor, row_ids)?;

        match row_ids {
            Some(ids) => {
                let mut result = vec![None; ids.len()];
                for selection in build_selections(ids, &page_index)? {
                    let page_data = self.read_page_payload(descriptor, &selection.entry)?;
                    let page = decode_fixed_width_page(
                        &page_data,
                        selection.entry.row_count as usize,
                        item_size,
                        descriptor.codec,
                    )?;
                    for (local_row, output_position) in selection
                        .local_rows
                        .iter()
                        .zip(selection.output_positions.iter())
                    {
                        let start = local_row * item_size;
                        let end = start + item_size;
                        result[*output_position] = Some(page[start..end].to_vec());
                    }
                }
                materialize_selected(result)
            }
            None => {
                let data = self.artifacts.read(&descriptor.data_path)?;
                let mut result = Vec::with_capacity(self.read_row_count()? as usize);
                for entry in page_index {
                    let page = decode_fixed_width_page(
                        self.slice_page(&data, &entry)?,
                        entry.row_count as usize,
                        item_size,
                        descriptor.codec,
                    )?;
                    result.extend(page.chunks_exact(item_size).map(|chunk| chunk.to_vec()));
                }
                Ok(result)
            }
        }
    }

    fn read_u64_values(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u64>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let page_index = self.read_compacted_page_index(descriptor, row_ids)?;

        match row_ids {
            Some(_) => read_selected_pages(row_ids, &page_index, |entry| {
                decode_u64_page(
                    &self.read_page_payload(descriptor, entry)?,
                    entry.row_count as usize,
                    descriptor.codec,
                )
            }),
            None => {
                let data = self.artifacts.read(&descriptor.data_path)?;
                read_selected_pages(None, &page_index, |entry| {
                    decode_u64_page(
                        self.slice_page(&data, entry)?,
                        entry.row_count as usize,
                        descriptor.codec,
                    )
                })
            }
        }
    }

    fn read_u32_values(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u32>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let page_index = self.read_compacted_page_index(descriptor, row_ids)?;

        match row_ids {
            Some(_) => read_selected_pages(row_ids, &page_index, |entry| {
                decode_u32_page(
                    &self.read_page_payload(descriptor, entry)?,
                    entry.row_count as usize,
                    descriptor.codec,
                )
            }),
            None => {
                let data = self.artifacts.read(&descriptor.data_path)?;
                read_selected_pages(None, &page_index, |entry| {
                    decode_u32_page(
                        self.slice_page(&data, entry)?,
                        entry.row_count as usize,
                        descriptor.codec,
                    )
                })
            }
        }
    }

    fn read_u8_values(&self, column: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u8>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let page_index = self.read_compacted_page_index(descriptor, row_ids)?;

        match row_ids {
            Some(_) => read_selected_pages(row_ids, &page_index, |entry| {
                decode_u8_page(
                    &self.read_page_payload(descriptor, entry)?,
                    entry.row_count as usize,
                    descriptor.codec,
                )
            }),
            None => {
                let data = self.artifacts.read(&descriptor.data_path)?;
                read_selected_pages(None, &page_index, |entry| {
                    decode_u8_page(
                        self.slice_page(&data, entry)?,
                        entry.row_count as usize,
                        descriptor.codec,
                    )
                })
            }
        }
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
            for (&local_row, &output_position) in
                selection.local_rows.iter().zip(&selection.output_positions)
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

    fn read_null_bitmap(&self, column: &str) -> io::Result<NullBitmap> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let null_path = descriptor.null_bitmap_path.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("nullable column {column} is missing a null bitmap"),
            )
        })?;
        let data = self.artifacts.read(null_path)?;
        let bitmap = NullBitmap::read_from(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt null bitmap"))?;
        self.validate_bitmap_rows(bitmap.len())?;
        Ok(bitmap)
    }

    fn read_compacted_page_index(
        &self,
        descriptor: &ColumnDescriptor,
        row_ids: Option<&[u32]>,
    ) -> io::Result<Vec<PageIndexEntry>> {
        let path = descriptor.page_index_path.as_ref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "compacted column is missing a page index",
            )
        })?;
        let data = self.artifacts.read(path)?;
        let mut entries = read_page_index(&data)?;
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
        for entry in &entries {
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
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut selections: Vec<PageSelection> = Vec::new();
    let mut page_cursor = 0usize;

    // Scan pages in physical order, then scatter into the caller's order. SQL
    // ORDER BY can select descending or arbitrary rows; a forward-only cursor
    // cannot follow those IDs directly. The usual ascending index scan keeps
    // its allocation-free ordering path, and every selected page decodes once.
    let sorted_positions = if row_ids.is_sorted() {
        None
    } else {
        let mut positions: Vec<_> = (0..row_ids.len()).collect();
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
            last.local_rows.push(local_row);
            last.output_positions.push(output_position);
        } else {
            selections.push(PageSelection {
                entry: *entry,
                local_rows: vec![local_row],
                output_positions: vec![output_position],
            });
        }
    }

    Ok(selections)
}

fn read_selected_pages<T, F>(
    row_ids: Option<&[u32]>,
    page_index: &[PageIndexEntry],
    mut decode_page: F,
) -> io::Result<Vec<T>>
where
    T: Clone,
    F: FnMut(&PageIndexEntry) -> io::Result<Vec<T>>,
{
    let mut decode_checked_page = |entry: &PageIndexEntry| {
        let page = decode_page(entry)?;
        if page.len() != entry.row_count as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded page row count differs from its index",
            ));
        }
        Ok(page)
    };
    match row_ids {
        Some(ids) => {
            let mut result = vec![None; ids.len()];
            for selection in build_selections(ids, page_index)? {
                let page = decode_checked_page(&selection.entry)?;
                for (local_row, output_position) in selection
                    .local_rows
                    .iter()
                    .zip(selection.output_positions.iter())
                {
                    result[*output_position] = Some(page[*local_row].clone());
                }
            }
            materialize_selected(result)
        }
        None => {
            let mut result = Vec::new();
            for entry in page_index {
                result.extend(decode_checked_page(entry)?);
            }
            Ok(result)
        }
    }
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
                let crc = crc32fast::hash(&bytes[..128]);
                bytes[128..132].copy_from_slice(&crc.to_le_bytes());
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
        assert_eq!(
            reads,
            [
                ("data_len".to_owned(), 0),
                ("data_len".to_owned(), 7),
                ("data".to_owned(), 0),
                ("data_len".to_owned(), 13),
                ("data".to_owned(), 11),
            ]
        );
    }

    #[test]
    fn variable_batches_retain_raw_buffers_and_validate_selection() {
        let tmp = TempDir::new().unwrap();
        let rows = vec![make_rows()[0].clone(); crate::page::MAX_PAGE_ROWS as usize + 1];
        ColumnFile::write_batch(tmp.path(), &rows).unwrap();
        let reader = SegmentReader::open_projected(tmp.path(), &["data"]).unwrap();
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
        }
        assert!(reader.var_bytes_batches("source", &[0]).is_err());
        let ids: Vec<_> = (0..rows.len() as u32).collect();
        let mut batches = reader.var_bytes_batches("data", &ids).unwrap();
        let first = batches.next().unwrap().unwrap();
        assert_eq!(
            first,
            vec![rows[0].data.clone(); crate::page::MAX_PAGE_ROWS as usize]
        );
        // Once prepared, later batches must use the validated retained buffers,
        // even if the captured files are subsequently damaged in place.
        fs::write(tmp.path().join("data.col"), []).unwrap();
        fs::write(tmp.path().join("data_len.col"), []).unwrap();
        assert_eq!(batches.next().unwrap().unwrap(), [rows[0].data.clone()]);
        assert!(batches.next().is_none());
        assert!(batches.next().is_none());
    }

    #[test]
    fn variable_batches_initialize_lazily_and_stop_after_corruption() {
        let (_tmp, dir) = compacted_fixture();
        let reader = SegmentReader::open_projected(&dir, &["data"]).unwrap();
        let mut batches = reader.var_bytes_batches("data", &[0, 19]).unwrap();
        let descriptor = reader.compacted_column("data").unwrap();
        fs::write(dir.join(&descriptor.data_path), []).unwrap();
        assert!(batches.next().unwrap().is_err());
        assert!(batches.next().is_none());
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
