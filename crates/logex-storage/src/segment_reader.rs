use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use alloy_primitives::{Address, B256, Bytes};
use logex_types::{LogRow, Source};

use crate::column_artifact::ColumnArtifacts;
use crate::native::{ColumnDescriptor, SegmentManifest};
use crate::page::{
    PageIndexEntry, decode_fixed_width_page, decode_u8_page, decode_u32_page, decode_u64_page,
    decode_var_bytes_page, read_page_index,
};
use crate::reader::{RawBytesColumn, RawFixedColumn};
use crate::{ColumnFileHeader, NullBitmap};

#[derive(Debug, Clone)]
pub struct SegmentReader {
    dir: PathBuf,
    manifest: Option<SegmentManifest>,
    artifacts: ColumnArtifacts,
}

#[derive(Debug, Clone)]
struct PageSelection {
    entry: PageIndexEntry,
    local_rows: Vec<usize>,
    output_positions: Vec<usize>,
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

    pub fn open(dir: &Path) -> io::Result<Self> {
        Self::open_inner(dir, None)
    }

    /// Capture only the columns needed by a query, plus canonicality and row-count
    /// metadata. Include predicate, ordering and output columns, including those
    /// needed if an index is unavailable. Other columns may not be readable.
    pub fn open_projected(dir: &Path, columns: &[&str]) -> io::Result<Self> {
        Self::open_inner(dir, Some(columns))
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
            if manifest.as_ref().is_some_and(|manifest| {
                manifest.format_version != crate::native::STORAGE_FORMAT_VERSION
            }) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported segment format",
                ));
            }
            match ColumnArtifacts::open_projected(dir, manifest.as_ref(), projection) {
                Ok(artifacts) => {
                    return Ok(Self {
                        dir: dir.to_path_buf(),
                        manifest,
                        artifacts,
                    });
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
        let visible = self.manifest.as_ref().map(|m| m.row_count);
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
        if self.compacted_column(column).is_none() {
            let path = raw_column_path(column);
            return RawBytesColumn::from_bytes(&self.dir.join(path), self.artifacts.read(path)?)?
                .materialize(row_ids, self.manifest.as_ref().map(|m| m.row_count));
        }
        self.read_var_bytes_values(column, row_ids)
    }

    pub fn read_canonical(&self) -> io::Result<NullBitmap> {
        let data = self.artifacts.read(self.canonical_relative_path())?;
        NullBitmap::read_from(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap"))
    }

    pub fn read_canonical_len(&self) -> io::Result<u64> {
        if self.artifacts.bundle().is_some() {
            return self.read_canonical().map(|bitmap| bitmap.len());
        }
        let bytes = self
            .artifacts
            .read_range(self.canonical_relative_path(), 0..8)?;
        let len = u64::from_le_bytes(bytes.try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid captured bitmap header")
        })?);
        let expected_len = 8 + len.div_ceil(8);
        let actual_len = self.artifacts.len(self.canonical_relative_path())?;
        if actual_len < expected_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "canonical bitmap is truncated",
            ));
        }
        Ok(len)
    }

    pub fn read_row_count(&self) -> io::Result<u64> {
        if let Some(manifest) = &self.manifest {
            Ok(manifest.row_count)
        } else {
            let data = self.artifacts.read("address.col")?;
            ColumnFileHeader::read_from(&data)
                .map(|header| header.row_count)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "corrupt captured row-count header",
                    )
                })
        }
    }

    pub fn read_log_rows(&self, row_ids: Option<&[u32]>) -> io::Result<Vec<LogRow>> {
        self.materialize_log_rows(row_ids, None)
    }

    /// Materialize a small maintenance candidate without trusting compressed
    /// frame sizes or allocating its complete data stream up front.
    pub(crate) fn read_log_rows_bounded(&self, budget: usize) -> io::Result<Vec<LogRow>> {
        let descriptor = self.compacted_column("data").ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "bounded read requires paged data",
            )
        })?;
        let lengths = self.read_u32("data_len", None)?;
        let mut data = Vec::new();
        let mut remaining = budget;
        for entry in self.read_compacted_page_index(descriptor, None)? {
            let start = usize::try_from(entry.first_row).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "data row offset overflow")
            })?;
            let lengths = lengths
                .get(start..start + entry.row_count as usize)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "data length rows differ")
                })?;
            let bytes = lengths
                .iter()
                .try_fold(0usize, |sum, &len| sum.checked_add(len as usize))
                .filter(|&sum| sum <= remaining)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "data page exceeds maintenance budget",
                    )
                })?;
            let page = crate::page::decode_var_bytes_page_bounded(
                &self.read_page_payload(descriptor, &entry)?,
                descriptor.codec,
                entry.row_count as usize,
                bytes,
            )?;
            for value in &page {
                remaining = remaining.checked_sub(value.len()).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "maintenance payload budget exceeded",
                    )
                })?;
            }
            data.extend(page);
        }
        self.materialize_log_rows(None, Some(data))
    }

    fn materialize_log_rows(
        &self,
        row_ids: Option<&[u32]>,
        data: Option<Vec<Bytes>>,
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
        let data = match data {
            Some(data) => data,
            None => self.read_var_bytes("data", row_ids)?,
        };
        let data_lens = self.read_u32("data_len", row_ids)?;
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
    ) -> io::Result<Vec<Bytes>> {
        let descriptor = self.compacted_column(column).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "column is not compacted")
        })?;
        let page_index = self.read_compacted_page_index(descriptor, row_ids)?;

        match row_ids {
            Some(_) => read_selected_pages(row_ids, &page_index, |entry| {
                decode_var_bytes_page(
                    &self.read_page_payload(descriptor, entry)?,
                    descriptor.codec,
                )
            }),
            None => {
                let data = self.artifacts.read(&descriptor.data_path)?;
                read_selected_pages(None, &page_index, |entry| {
                    decode_var_bytes_page(self.slice_page(&data, entry)?, descriptor.codec)
                })
            }
        }
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
        NullBitmap::read_from(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt null bitmap"))
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
    let path = dir.join("segment.json");
    if !path.exists() {
        return Ok(None);
    }
    let json = fs::read(&path)?;
    let manifest = serde_json::from_slice(&json).map_err(io::Error::other)?;
    Ok(Some(manifest))
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
    match row_ids {
        Some(ids) => {
            let mut result = vec![None; ids.len()];
            for selection in build_selections(ids, page_index)? {
                let page = decode_page(&selection.entry)?;
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
                result.extend(decode_page(entry)?);
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use alloy_primitives::{Address, bytes};
    use tempfile::TempDir;

    use crate::ColumnFile;
    use crate::native::{
        SegmentDescriptor, SegmentKind, StorageCatalogPaths, compact_segment,
        persist_segment_manifest,
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
    fn reads_compacted_segment_rows() {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let descriptor = SegmentDescriptor {
            column_bundle: None,
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
        ColumnFile::write_batch(&dir, &rows).unwrap();
        persist_segment_manifest(&paths, &descriptor).unwrap();
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
        let descriptor = SegmentDescriptor {
            column_bundle: None,
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

        ColumnFile::write_batch(&dir, &make_rows()).unwrap();
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
