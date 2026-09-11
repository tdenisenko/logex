use crate::bundle::{BundleReader, BundleReference, BundleWriter};
use crate::column_artifact::{BUNDLE_PATH, ColumnArtifacts, stream_id};
use crate::durability::{self, Publication};
use std::fs;
use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::thread;

use alloy_primitives::{Address, B256};
use logex_types::LogRow;

use crate::column::{ColumnFile, ColumnFileHeader, NullBitmap};
use crate::page::{
    PageIndexEntry, encode_fixed_width_page, encode_u8_page, encode_u32_page, encode_u64_page,
    encode_var_bytes_page, read_page_index, write_page_index,
};
use crate::reader::{ColumnReader, RawBytesColumn, RawFixedColumn};
use crate::segment_reader::SegmentReader;

use super::catalog::{
    ColumnDescriptor, CompressionCodec, IndexDescriptor, IndexKind, SegmentDescriptor, SegmentKind,
    SegmentManifest, StorageCatalogPaths,
};

const DEFAULT_PAGE_ROWS: u32 = 16_384;
const RECOMPACTED_COLUMNS_DIR: &str = "columns_profile_v2";
const RAW_FIXED_COLUMNS: &[(&str, u64)] = &[
    ("address.col", 20),
    ("block_number.col", 8),
    ("block_hash.col", 32),
    ("timestamp.col", 8),
    ("tx_hash.col", 32),
    ("tx_index.col", 4),
    ("log_index.col", 4),
    ("data_len.col", 4),
    ("source.col", 1),
    ("topic0.col", 32),
    ("topic1.col", 32),
    ("topic2.col", 32),
    ("topic3.col", 32),
];
const RAW_NULL_BITMAP_COLUMNS: &[&str] =
    &["topic0.null", "topic1.null", "topic2.null", "topic3.null"];
const RAW_BITMAP_COLUMNS: &[&str] = &[
    "topic0.null",
    "topic1.null",
    "topic2.null",
    "topic3.null",
    "canonical.bitmap",
];

/// Existing page payloads and index entries form an immutable append prefix.
/// Per-column indexes and bitmaps are replaced before manifest publication;
/// bundled indexes append inside immutable tables.
struct PageOutput<'a> {
    dir: &'a Path,
    existing_rows: u64,
    previous: std::collections::BTreeMap<String, ExistingPages>,
    canonical: Option<NullBitmap>,
    replacements: Option<durability::ReplacementBatch>,
    bundle: Option<(BundleWriter, u64)>,
}

pub(crate) struct EncodedColumns {
    columns: Vec<ColumnDescriptor>,
    bundle: Option<BundleReference>,
}

impl EncodedColumns {
    pub(crate) fn apply_to(self, descriptor: &mut SegmentDescriptor) -> Vec<ColumnDescriptor> {
        descriptor.column_bundle = self.bundle;
        self.columns
    }
}

struct ExistingPages {
    column: ColumnDescriptor,
    entries: Vec<PageIndexEntry>,
    encoded_bytes: u64,
    nulls: Option<NullBitmap>,
}

impl<'a> PageOutput<'a> {
    fn new(dir: &'a Path) -> Self {
        Self {
            dir,
            existing_rows: 0,
            previous: Default::default(),
            canonical: Some(NullBitmap::new()),
            replacements: None,
            bundle: None,
        }
    }

    fn append(
        dir: &'a Path,
        manifest: &SegmentManifest,
        row_count: usize,
        publication: Publication,
        inspected: Option<BundleReader>,
    ) -> std::io::Result<Self> {
        manifest
            .row_count
            .checked_add(row_count as u64)
            .filter(|&rows| rows <= u64::from(u32::MAX))
            .ok_or_else(|| std::io::Error::other("row count exceeds segment addressing"))?;
        let artifacts = ColumnArtifacts::open_inspected(dir, Some(manifest), inspected)?;
        let (mut output, tail) = Self::inspect(dir, manifest, &artifacts)?;
        let bundle = artifacts.bundle().cloned();
        drop(artifacts);
        if tail {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compacted segment has an unpublished tail; reopen before appending",
            ));
        }
        if bundle.is_none() {
            output.replacements = Some(durability::ReplacementBatch::new(publication));
        }
        if let Some(reader) = bundle {
            output.bundle = Some((
                BundleWriter::append_inspected(&dir.join(BUNDLE_PATH), reader)?,
                manifest.row_count + row_count as u64,
            ));
        }
        Ok(output)
    }

    /// Inspect the complete committed prefix without interpreting appended
    /// pages as committed data. A malformed or missing prefix is never repairable
    /// merely by truncation; preserve it for verified recovery instead.
    fn inspect(
        dir: &'a Path,
        manifest: &SegmentManifest,
        artifacts: &ColumnArtifacts,
    ) -> std::io::Result<(Self, bool)> {
        let invalid = |reason: &str| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "cannot append compacted segment {}: {reason}",
                    manifest.segment_id
                ),
            )
        };
        if manifest.format_version != super::catalog::STORAGE_FORMAT_VERSION
            || (manifest.kind != SegmentKind::Sealed && manifest.column_bundle.is_none())
            || manifest.canonical_rows_path != "canonical.bitmap"
            || !columns_match_current_profile(&manifest.columns)
        {
            return Err(invalid("columns do not use the current compaction profile"));
        }
        let mut previous = std::collections::BTreeMap::new();
        let mut tail = manifest
            .column_bundle
            .as_ref()
            .map(|reference| {
                Ok::<_, std::io::Error>(
                    fs::metadata(dir.join(BUNDLE_PATH))?.len() != reference.end()?,
                )
            })
            .transpose()?
            .unwrap_or(false);
        for column in &manifest.columns {
            // Persisted paths must select only known column artifacts inside
            // this segment. Do not turn malformed metadata into arbitrary writes.
            let suffix = format!("/{}.pages", column.name);
            let base = column
                .data_path
                .strip_suffix(&suffix)
                .filter(|base| {
                    matches!(*base, "columns" | "columns_profile_v2")
                        || (*base == "columns_block_number_v2" && column.name == "block_number")
                })
                .ok_or_else(|| invalid("invalid column data path"))?;
            let index = format!("{base}/{}.pages.idx", column.name);
            let is_topic = matches!(
                column.name.as_str(),
                "topic0" | "topic1" | "topic2" | "topic3"
            );
            let null_path = is_topic.then(|| format!("{base}/{}.null", column.name));
            if column.page_rows != DEFAULT_PAGE_ROWS
                || column.page_index_path.as_deref() != Some(index.as_str())
                || column.null_bitmap_path != null_path
            {
                return Err(invalid("invalid column index/bitmap descriptor"));
            }
            let mut entries = read_page_index(&artifacts.read(&index)?)?;
            let mut rows = 0u64;
            let mut offset = 0u64;
            let mut prefix_entries = 0;
            for entry in &entries {
                if rows == manifest.row_count {
                    break;
                }
                if entry.first_row != rows
                    || entry.offset != offset
                    || entry.row_count == 0
                    || entry.row_count > DEFAULT_PAGE_ROWS
                    || entry.encoded_len == 0
                {
                    return Err(invalid("page index does not describe a contiguous prefix"));
                }
                rows = rows
                    .checked_add(u64::from(entry.row_count))
                    .ok_or_else(|| invalid("page row overflow"))?;
                offset = offset
                    .checked_add(u64::from(entry.encoded_len))
                    .ok_or_else(|| invalid("page offset overflow"))?;
                prefix_entries += 1;
            }
            let file_len = artifacts.len(&column.data_path)?;
            if rows != manifest.row_count || file_len < offset {
                return Err(invalid("page prefix length does not match the manifest"));
            }
            let extra_entries = prefix_entries != entries.len();
            tail |= extra_entries || file_len != offset;
            entries.truncate(prefix_entries);
            let nulls = null_path
                .map(|path| parse_bitmap_prefix(&artifacts.read(&path)?, manifest.row_count))
                .transpose()?;
            if manifest.column_bundle.is_some()
                && (extra_entries
                    || file_len != offset
                    || nulls.as_ref().is_some_and(|bitmap| bitmap.len() != rows))
            {
                return Err(invalid(
                    "bundle streams do not match their immutable row boundary",
                ));
            }
            tail |= nulls.as_ref().is_some_and(|bitmap| bitmap.len() != rows);
            previous.insert(
                column.name.clone(),
                ExistingPages {
                    column: column.clone(),
                    entries,
                    encoded_bytes: offset,
                    nulls,
                },
            );
        }
        let canonical =
            parse_bitmap_prefix(&artifacts.read("canonical.bitmap")?, manifest.row_count)?;
        tail |= canonical.len() != manifest.row_count;
        Ok((
            Self {
                dir,
                existing_rows: manifest.row_count,
                previous,
                canonical: Some(canonical),
                replacements: None,
                bundle: None,
            },
            tail,
        ))
    }

    fn metadata(
        &self,
        path: &Path,
        write: impl FnOnce(&mut dyn Write) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        if let Some((bundle, rows)) = &self.bundle {
            let relative = path
                .strip_prefix(self.dir)
                .ok()
                .and_then(Path::to_str)
                .ok_or_else(|| std::io::Error::other("invalid column artifact path"))?;
            let mut bytes = Vec::new();
            write(&mut bytes)?;
            let encoded = crate::column_artifact::encode_bitmap(&bytes, *rows)?;
            return bundle.replace_metadata(stream_id(relative)?, &encoded);
        }
        if let Some(replacements) = &self.replacements {
            replacements.write(path, |writer| write(writer))
        } else {
            let mut writer = BufWriter::new(File::create(path)?);
            write(&mut writer)?;
            writer.flush()
        }
    }

    fn previous_nulls(&self, name: &str) -> std::io::Result<NullBitmap> {
        self.previous
            .get(name)
            .map(|previous| {
                previous.nulls.clone().ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "nullable column is missing its bitmap",
                    )
                })
            })
            .unwrap_or_else(|| Ok(NullBitmap::new()))
    }

    fn append_canonical(&self, count: usize) -> std::io::Result<()> {
        let mut bitmap = self
            .canonical
            .clone()
            .ok_or_else(|| std::io::Error::other("missing canonical append prefix"))?;
        for _ in 0..count {
            bitmap.push(true);
        }
        self.metadata(&self.dir.join("canonical.bitmap"), |writer| {
            bitmap.write_to(writer)
        })
    }

    fn finish(self) -> std::io::Result<Option<BundleReference>> {
        let reference = self
            .bundle
            .map(|(bundle, rows)| bundle.finish(rows))
            .transpose()?;
        self.replacements
            .map(durability::ReplacementBatch::publish)
            .unwrap_or(Ok(()))?;
        Ok(reference)
    }
}

fn parse_bitmap_prefix(bytes: &[u8], rows: u64) -> std::io::Result<NullBitmap> {
    let bitmap = NullBitmap::read_from(bytes)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid bitmap"))?;
    if bitmap.len() < rows || bytes.len() as u64 != 8 + bitmap.len().div_ceil(8) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bitmap does not match the committed row count",
        ));
    }
    Ok(bitmap)
}

pub(crate) fn compacted_segment_has_uncommitted_tail(
    dir: &Path,
    manifest: &SegmentManifest,
) -> std::io::Result<bool> {
    let artifacts = ColumnArtifacts::open(dir, Some(manifest))?;
    PageOutput::inspect(dir, manifest, &artifacts).map(|(_, tail)| tail)
}

/// Restore only the append suffix. The catalog identifies the complete immutable
/// table, so recovery never has to decompress and rewrite committed column data.
pub(crate) fn restore_bundled_checkpoint(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> std::io::Result<()> {
    let dir = paths.segment_dir(descriptor.id);
    let reference = descriptor
        .column_bundle
        .as_ref()
        .ok_or_else(|| std::io::Error::other("missing catalog bundle reference"))?;
    let columns = current_column_profile()
        .iter()
        .map(|(name, codec)| ColumnDescriptor {
            name: (*name).to_owned(),
            codec: *codec,
            page_rows: DEFAULT_PAGE_ROWS,
            data_path: format!("columns/{name}.pages"),
            page_index_path: Some(format!("columns/{name}.pages.idx")),
            null_bitmap_path: matches!(*name, "topic0" | "topic1" | "topic2" | "topic3")
                .then(|| format!("columns/{name}.null")),
        })
        .collect();
    let prefix = SegmentManifest {
        column_bundle: Some(reference.clone()),
        format_version: super::catalog::STORAGE_FORMAT_VERSION,
        segment_id: descriptor.id,
        generation: descriptor.generation,
        kind: descriptor.kind,
        min_block: descriptor.min_block,
        max_block: descriptor.max_block,
        min_timestamp: descriptor.min_timestamp,
        max_timestamp: descriptor.max_timestamp,
        row_count: descriptor.row_count,
        canonical_rows_path: "canonical.bitmap".to_owned(),
        columns,
        indexes: collect_indexes(&dir)?,
    };
    let artifacts = ColumnArtifacts::open(&dir, Some(&prefix))?;
    artifacts.verify_bundle()?;
    let (inspected, tail) = PageOutput::inspect(&dir, &prefix, &artifacts)?;
    // A bundle's manifest is derived metadata. The catalog pins the complete
    // schema, row boundary and immutable canonical bitmap; an unpublished or
    // damaged manifest is never used to decide which data to retain.
    let previous: Option<SegmentManifest> =
        match fs::read(paths.segment_manifest_path(descriptor.id)) {
            Ok(bytes) => serde_json::from_slice(&bytes).ok(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
    if !tail && previous.as_ref() == Some(&prefix) {
        return Ok(());
    }
    let canonical = inspected
        .canonical
        .ok_or_else(|| std::io::Error::other("missing canonical prefix"))?;
    if canonical.len() != descriptor.row_count {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "immutable canonical bitmap does not match the catalog",
        ));
    }
    persist_segment_manifest_with_columns(paths, descriptor, prefix.columns)?;
    let path = dir.join(BUNDLE_PATH);
    let file = fs::OpenOptions::new().write(true).open(&path)?;
    if file.metadata()?.len() != reference.end()? {
        durability::checkpoint("bundle_trim_uncommitted_tail", &path)?;
        file.set_len(reference.end()?)?;
        durability::sync_file_and_directory(&file, &path)?;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn append_rows(
    segment_dir: &Path,
    existing_rows: u64,
    rows: &[LogRow],
) -> std::io::Result<()> {
    append_ingest_rows(segment_dir, existing_rows, rows, Publication::Ordered)
}

pub(crate) fn append_ingest_rows(
    segment_dir: &Path,
    existing_rows: u64,
    rows: &[LogRow],
    publication: Publication,
) -> std::io::Result<()> {
    if existing_rows == 0 {
        ColumnFile::write_batch_with_publication(segment_dir, rows, None, publication)
    } else {
        ColumnFile::append_batch_with_publication(segment_dir, rows, existing_rows, publication)
    }
}

pub(crate) fn write_compacted_rows(
    segment_dir: &Path,
    rows: &[LogRow],
) -> std::io::Result<Vec<ColumnDescriptor>> {
    fs::create_dir_all(segment_dir)?;
    fs::create_dir_all(segment_dir.join("columns"))?;
    let output = PageOutput::new(segment_dir);
    // This is a new representation with no committed rows. Publish its bitmap
    // together with its columns; the containing manifest/catalog supplies the
    // ordering and durable flush, just as it does for the new page payloads.
    output.append_canonical(rows.len())?;
    let columns = write_compacted_values(&output, rows)?;
    output.finish()?;
    Ok(columns)
}

/// Start a segment with no committed rows. Empty raw files left by a previous
/// rollback are disposable; nonempty raw prefixes must use their append path.
pub(crate) fn write_bundled_rows(
    segment_dir: &Path,
    rows: &[LogRow],
) -> std::io::Result<EncodedColumns> {
    fs::create_dir_all(segment_dir.join("columns"))?;
    let mut output = PageOutput::new(segment_dir);
    output.bundle = Some((
        BundleWriter::create(&segment_dir.join(BUNDLE_PATH))?,
        rows.len() as u64,
    ));
    output.append_canonical(rows.len())?;
    let columns = write_compacted_values(&output, rows)?;
    let encoded = EncodedColumns {
        columns,
        bundle: output.finish()?,
    };
    for name in RAW_FIXED_COLUMNS
        .iter()
        .map(|(name, _)| *name)
        .chain(std::iter::once("data.col"))
        .chain(RAW_BITMAP_COLUMNS.iter().copied())
    {
        let path = segment_dir.join(name);
        if path.try_exists()? {
            durability::checkpoint("bundle_remove_empty_raw_artifact", &path)?;
            fs::remove_file(path)?;
        }
    }
    Ok(encoded)
}

/// Reserve worst-case compressed extents before writing any part of a batch.
/// All fixed columns fit in one extent per page under the current profile.
/// Variable bytes use zstd's bound over the larger u64-offset representation.
/// Carry the validated snapshot into append inspection instead of rereading it.
pub(crate) fn bundled_row_capacity(
    dir: &Path,
    reference: Option<&BundleReference>,
    rows: &[LogRow],
) -> std::io::Result<(usize, Option<BundleReader>)> {
    use crate::bundle::{DATA_STREAMS, MAX_EXTENT_BYTES, MAX_EXTENTS};
    let reader = reference
        .map(|reference| BundleReader::open(&dir.join(BUNDLE_PATH), reference))
        .transpose()?;
    let capacity = reader
        .as_ref()
        .map(BundleReader::remaining_data_extents)
        .transpose()?
        .unwrap_or([MAX_EXTENTS; DATA_STREAMS as usize]);
    let mut pages = capacity[..13]
        .iter()
        .chain(&capacity[14..])
        .copied()
        .min()
        .unwrap_or(0);
    let mut data_extents = capacity[13];
    let mut accepted = 0;
    while accepted < rows.len() && pages > 0 && data_extents > 0 {
        let page = &rows[accepted..rows.len().min(accepted + DEFAULT_PAGE_ROWS as usize)];
        let raw_bytes = page
            .iter()
            .try_fold(16usize + page.len() * 8, |bytes, row| {
                bytes
                    .checked_add(row.data.len())
                    .ok_or_else(|| std::io::Error::other("variable page length overflow"))
            })?;
        let full_bound = zstd::zstd_safe::compress_bound(raw_bytes)
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("compressed page bound overflow"))?;
        let fits = |bound| bound <= data_extents * MAX_EXTENT_BYTES && bound <= u32::MAX as usize;
        let (count, encoded_bound) = if fits(full_bound) {
            (page.len(), full_bound)
        } else {
            // Only the final capacity-constrained page needs a row-wise check.
            let mut count = 0;
            let mut raw_bytes = 16usize;
            let mut encoded_bound = 0;
            for row in page {
                let next = raw_bytes
                    .checked_add(row.data.len())
                    .and_then(|n| n.checked_add(8))
                    .ok_or_else(|| std::io::Error::other("variable page length overflow"))?;
                let bound = zstd::zstd_safe::compress_bound(next)
                    .checked_add(1)
                    .ok_or_else(|| std::io::Error::other("compressed page bound overflow"))?;
                if !fits(bound) {
                    break;
                }
                raw_bytes = next;
                encoded_bound = bound;
                count += 1;
            }
            (count, encoded_bound)
        };
        if count == 0 {
            break;
        }
        accepted += count;
        pages -= 1;
        data_extents -= encoded_bound.div_ceil(MAX_EXTENT_BYTES);
        if count < DEFAULT_PAGE_ROWS as usize {
            break;
        }
    }
    Ok((accepted, reader))
}

pub(crate) fn append_compacted_rows(
    segment_dir: &Path,
    existing_rows: u64,
    rows: &[LogRow],
    publication: Publication,
    inspected: Option<BundleReader>,
) -> std::io::Result<EncodedColumns> {
    let manifest: SegmentManifest =
        serde_json::from_slice(&fs::read(segment_dir.join("segment.json"))?)
            .map_err(std::io::Error::other)?;
    if manifest.row_count != existing_rows {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "segment append manifest changed",
        ));
    }
    let output = PageOutput::append(segment_dir, &manifest, rows.len(), publication, inspected)?;
    output.append_canonical(rows.len())?;
    let columns = write_compacted_values(&output, rows)?;
    Ok(EncodedColumns {
        columns,
        bundle: output.finish()?,
    })
}

/// Append a new canonical view without changing the catalog-pinned bitmap.
/// The caller must publish the returned reference and flush this tree before
/// acknowledging the catalog commit; an interrupted update remains a tail.
pub(crate) fn append_bundled_canonical(
    dir: &Path,
    reference: &BundleReference,
    canonical: &NullBitmap,
) -> std::io::Result<BundleReference> {
    let mut bytes = Vec::new();
    canonical.write_to(&mut bytes)?;
    let encoded = crate::column_artifact::encode_bitmap(&bytes, reference.row_count)?;
    let reader = BundleReader::open(&dir.join(BUNDLE_PATH), reference)?;
    let writer = BundleWriter::append_inspected(&dir.join(BUNDLE_PATH), reader)?;
    writer.replace_metadata(crate::column_artifact::CANONICAL_STREAM, &encoded)?;
    writer.finish(reference.row_count)
}

fn write_compacted_values(
    output: &PageOutput<'_>,
    rows: &[LogRow],
) -> std::io::Result<Vec<ColumnDescriptor>> {
    thread::scope(|scope| {
        let address =
            scope.spawn(|| compact_address_values(output, rows.iter().map(|row| row.address)));
        let block_number = scope.spawn(|| {
            compact_u64_values(
                output,
                "block_number",
                CompressionCodec::DeltaZigZag,
                rows.iter().map(|row| row.block_number),
            )
        });
        let block_hash = scope.spawn(|| {
            compact_b256_values(
                output,
                "block_hash",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.block_hash),
            )
        });
        let timestamp = scope.spawn(|| {
            compact_u64_values(
                output,
                "timestamp",
                CompressionCodec::DeltaOfDelta,
                rows.iter().map(|row| row.timestamp),
            )
        });
        let tx_hash = scope.spawn(|| {
            compact_b256_values(
                output,
                "tx_hash",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.tx_hash),
            )
        });
        let tx_index = scope.spawn(|| {
            compact_u32_values(
                output,
                "tx_index",
                CompressionCodec::Zstd,
                rows.iter().map(|row| row.tx_index),
            )
        });
        let log_index = scope.spawn(|| {
            compact_u32_values(
                output,
                "log_index",
                CompressionCodec::Zstd,
                rows.iter().map(|row| row.log_index),
            )
        });
        let data_len = scope.spawn(|| {
            compact_u32_values(
                output,
                "data_len",
                CompressionCodec::Zstd,
                rows.iter().map(|row| row.data_len),
            )
        });
        let source = scope.spawn(|| {
            compact_u8_values(
                output,
                "source",
                CompressionCodec::Dictionary,
                rows.iter().map(|row| row.source as u8),
            )
        });
        let topic0 = scope.spawn(|| {
            compact_nullable_b256_values(
                output,
                "topic0",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic0),
            )
        });
        let topic1 = scope.spawn(|| {
            compact_nullable_b256_values(
                output,
                "topic1",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic1),
            )
        });
        let topic2 = scope.spawn(|| {
            compact_nullable_b256_values(
                output,
                "topic2",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic2),
            )
        });
        let topic3 = scope.spawn(|| {
            compact_nullable_b256_values(
                output,
                "topic3",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic3),
            )
        });
        let data = scope.spawn(|| compact_data_values(output, rows));

        Ok(vec![
            join_column_worker(address)?,
            join_column_worker(block_number)?,
            join_column_worker(block_hash)?,
            join_column_worker(timestamp)?,
            join_column_worker(tx_hash)?,
            join_column_worker(tx_index)?,
            join_column_worker(log_index)?,
            join_column_worker(data_len)?,
            join_column_worker(source)?,
            join_column_worker(topic0)?,
            join_column_worker(topic1)?,
            join_column_worker(topic2)?,
            join_column_worker(topic3)?,
            join_column_worker(data)?,
        ])
    })
}

fn join_column_worker(
    handle: thread::ScopedJoinHandle<'_, std::io::Result<ColumnDescriptor>>,
) -> std::io::Result<ColumnDescriptor> {
    handle
        .join()
        .map_err(|_| std::io::Error::other("compacted column worker panicked"))?
}

pub(crate) fn apply_rows_to_descriptor(descriptor: &mut SegmentDescriptor, rows: &[LogRow]) {
    if rows.is_empty() {
        return;
    }

    let min_block = rows.iter().map(|row| row.block_number).min().unwrap_or(0);
    let max_block = rows.iter().map(|row| row.block_number).max().unwrap_or(0);
    let min_timestamp = rows.iter().map(|row| row.timestamp).min().unwrap_or(0);
    let max_timestamp = rows.iter().map(|row| row.timestamp).max().unwrap_or(0);

    descriptor.min_block = Some(
        descriptor
            .min_block
            .map(|current| current.min(min_block))
            .unwrap_or(min_block),
    );
    descriptor.max_block = Some(
        descriptor
            .max_block
            .map(|current| current.max(max_block))
            .unwrap_or(max_block),
    );
    descriptor.min_timestamp = Some(
        descriptor
            .min_timestamp
            .map(|current| current.min(min_timestamp))
            .unwrap_or(min_timestamp),
    );
    descriptor.max_timestamp = Some(
        descriptor
            .max_timestamp
            .map(|current| current.max(max_timestamp))
            .unwrap_or(max_timestamp),
    );
    descriptor.row_count += rows.len() as u64;
}

pub(crate) fn apply_ordered_rows_to_descriptor(
    descriptor: &mut SegmentDescriptor,
    rows: &[LogRow],
) {
    let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
        return;
    };

    let min_block = first.block_number.min(last.block_number);
    let max_block = first.block_number.max(last.block_number);
    let min_timestamp = first.timestamp.min(last.timestamp);
    let max_timestamp = first.timestamp.max(last.timestamp);

    descriptor.min_block = Some(
        descriptor
            .min_block
            .map(|current| current.min(min_block))
            .unwrap_or(min_block),
    );
    descriptor.max_block = Some(
        descriptor
            .max_block
            .map(|current| current.max(max_block))
            .unwrap_or(max_block),
    );
    descriptor.min_timestamp = Some(
        descriptor
            .min_timestamp
            .map(|current| current.min(min_timestamp))
            .unwrap_or(min_timestamp),
    );
    descriptor.max_timestamp = Some(
        descriptor
            .max_timestamp
            .map(|current| current.max(max_timestamp))
            .unwrap_or(max_timestamp),
    );
    descriptor.row_count += rows.len() as u64;
}

pub(crate) fn persist_segment_manifest(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> std::io::Result<()> {
    let columns = existing_columns(paths, descriptor.id)?.unwrap_or_else(default_columns);
    persist_segment_manifest_with_columns(paths, descriptor, columns)
}

pub(crate) fn persist_segment_manifest_with_columns(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    columns: Vec<ColumnDescriptor>,
) -> std::io::Result<()> {
    persist_manifest(paths, descriptor, columns, Publication::Durable)
}

pub(crate) fn persist_ingest_manifest(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    publication: Publication,
) -> std::io::Result<()> {
    let columns = existing_columns(paths, descriptor.id)?.unwrap_or_else(default_columns);
    persist_manifest(paths, descriptor, columns, publication)
}

pub(crate) fn persist_ingest_manifest_with_columns(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    columns: Vec<ColumnDescriptor>,
    publication: Publication,
) -> std::io::Result<()> {
    persist_manifest(paths, descriptor, columns, publication)
}

fn persist_manifest(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    columns: Vec<ColumnDescriptor>,
    publication: Publication,
) -> std::io::Result<()> {
    let segment_dir = paths.segment_dir(descriptor.id);
    fs::create_dir_all(&segment_dir)?;

    let manifest = SegmentManifest {
        column_bundle: descriptor.column_bundle.clone(),
        format_version: super::catalog::STORAGE_FORMAT_VERSION,
        segment_id: descriptor.id,
        generation: descriptor.generation,
        kind: descriptor.kind,
        min_block: descriptor.min_block,
        max_block: descriptor.max_block,
        min_timestamp: descriptor.min_timestamp,
        max_timestamp: descriptor.max_timestamp,
        row_count: descriptor.row_count,
        canonical_rows_path: "canonical.bitmap".to_owned(),
        columns,
        indexes: collect_indexes(&segment_dir)?,
    };

    let path = paths.segment_manifest_path(descriptor.id);
    let json = serde_json::to_vec(&manifest).map_err(std::io::Error::other)?;
    match publication {
        Publication::Deferred => durability::write_bytes_deferred(&path, &json),
        Publication::Ordered => {
            durability::publish_tree_ordered(&segment_dir, &path, &json, &paths.catalog_path())
        }
        Publication::Durable => durability::publish_tree(&segment_dir, &path, &json),
    }
}

pub(crate) fn compact_segment(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> std::io::Result<()> {
    compact_ingest_segment(paths, descriptor, Publication::Durable)
}

pub(crate) fn compact_ingest_segment(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    publication: Publication,
) -> std::io::Result<()> {
    if descriptor.kind != SegmentKind::Sealed || descriptor.row_count == 0 {
        return persist_ingest_manifest(paths, descriptor, publication);
    }

    if segment_uses_current_compaction_profile(paths, descriptor.id)? {
        return persist_ingest_manifest(paths, descriptor, publication);
    }

    if segment_is_compacted(paths, descriptor.id)? {
        return recompact_segment(paths, descriptor);
    }

    let segment_dir = paths.segment_dir(descriptor.id);
    fs::create_dir_all(segment_dir.join("columns"))?;
    verify_raw_segment_files_complete(descriptor, &segment_dir)?;

    let output = PageOutput::new(&segment_dir);
    let columns = thread::scope(|scope| {
        let address = scope.spawn(|| compact_address_column(&output));
        let block_number = scope
            .spawn(|| compact_u64_column(&output, "block_number", CompressionCodec::DeltaZigZag));
        let block_hash = scope
            .spawn(|| compact_b256_column(&output, "block_hash", CompressionCodec::AdaptiveFixed));
        let timestamp = scope
            .spawn(|| compact_u64_column(&output, "timestamp", CompressionCodec::DeltaOfDelta));
        let tx_hash = scope
            .spawn(|| compact_b256_column(&output, "tx_hash", CompressionCodec::AdaptiveFixed));
        let tx_index =
            scope.spawn(|| compact_u32_column(&output, "tx_index", CompressionCodec::Zstd));
        let log_index =
            scope.spawn(|| compact_u32_column(&output, "log_index", CompressionCodec::Zstd));
        let data_len =
            scope.spawn(|| compact_u32_column(&output, "data_len", CompressionCodec::Zstd));
        let source =
            scope.spawn(|| compact_u8_column(&output, "source", CompressionCodec::Dictionary));
        let topic0 = scope.spawn(|| {
            compact_nullable_b256_column(&output, "topic0", CompressionCodec::AdaptiveFixed)
        });
        let topic1 = scope.spawn(|| {
            compact_nullable_b256_column(&output, "topic1", CompressionCodec::AdaptiveFixed)
        });
        let topic2 = scope.spawn(|| {
            compact_nullable_b256_column(&output, "topic2", CompressionCodec::AdaptiveFixed)
        });
        let topic3 = scope.spawn(|| {
            compact_nullable_b256_column(&output, "topic3", CompressionCodec::AdaptiveFixed)
        });
        let data = scope.spawn(|| compact_data_column(&output, CompressionCodec::AdaptiveBytes));
        Ok::<_, std::io::Error>(vec![
            join_column_worker(address)?,
            join_column_worker(block_number)?,
            join_column_worker(block_hash)?,
            join_column_worker(timestamp)?,
            join_column_worker(tx_hash)?,
            join_column_worker(tx_index)?,
            join_column_worker(log_index)?,
            join_column_worker(data_len)?,
            join_column_worker(source)?,
            join_column_worker(topic0)?,
            join_column_worker(topic1)?,
            join_column_worker(topic2)?,
            join_column_worker(topic3)?,
            join_column_worker(data)?,
        ])
    })?;

    output.finish()?;

    persist_ingest_manifest_with_columns(paths, descriptor, columns, publication)?;
    remove_raw_hot_files(&segment_dir)?;

    tracing::info!(
        segment_id = descriptor.id,
        row_count = descriptor.row_count,
        "compacted sealed storage segment"
    );

    Ok(())
}

fn recompact_segment(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> std::io::Result<()> {
    if recompact_block_number_profile(paths, descriptor)? {
        return Ok(());
    }

    let segment_dir = paths.segment_dir(descriptor.id);
    let tmp_dir = segment_dir.join(".recompact_tmp");
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)?;
    }

    let rows = SegmentReader::open(&segment_dir)?.read_log_rows(None)?;
    let mut columns = write_compacted_rows(&tmp_dir, &rows)?;
    let target_columns = segment_dir.join(RECOMPACTED_COLUMNS_DIR);
    if target_columns.exists() {
        fs::remove_dir_all(&target_columns)?;
    }
    fs::rename(tmp_dir.join("columns"), &target_columns)?;
    rewrite_column_dir(&mut columns, RECOMPACTED_COLUMNS_DIR);

    persist_segment_manifest_with_columns(paths, descriptor, columns)?;

    remove_superseded_column_dirs(&segment_dir, RECOMPACTED_COLUMNS_DIR)?;
    if tmp_dir.exists() {
        fs::remove_dir_all(tmp_dir)?;
    }

    tracing::info!(
        segment_id = descriptor.id,
        row_count = descriptor.row_count,
        "recompacted sealed storage segment"
    );

    Ok(())
}

fn recompact_block_number_profile(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> std::io::Result<bool> {
    let Some(mut columns) = existing_columns(paths, descriptor.id)? else {
        return Ok(false);
    };
    let Some(block_column_index) = block_number_only_profile_mismatch(&columns) else {
        return Ok(false);
    };

    let segment_dir = paths.segment_dir(descriptor.id);
    let tmp_dir = segment_dir.join(".block_number_recompact_tmp");
    if tmp_dir.exists() {
        fs::remove_dir_all(&tmp_dir)?;
    }
    fs::create_dir_all(tmp_dir.join("columns"))?;

    let values = SegmentReader::open(&segment_dir)?.read_u64("block_number", None)?;
    let output = PageOutput::new(&tmp_dir);
    let mut block_number_column = compact_u64_values(
        &output,
        "block_number",
        CompressionCodec::DeltaZigZag,
        values,
    )?;
    output.finish()?;
    let target_dir_name = "columns_block_number_v2";
    let target_dir = segment_dir.join(target_dir_name);
    if target_dir.exists() {
        fs::remove_dir_all(&target_dir)?;
    }
    fs::rename(tmp_dir.join("columns"), &target_dir)?;
    rewrite_column_path_descriptor(&mut block_number_column, target_dir_name);

    let old_column = std::mem::replace(&mut columns[block_column_index], block_number_column);
    persist_segment_manifest_with_columns(paths, descriptor, columns)?;
    remove_column_files(&segment_dir, &old_column)?;
    if tmp_dir.exists() {
        fs::remove_dir_all(tmp_dir)?;
    }

    tracing::info!(
        segment_id = descriptor.id,
        row_count = descriptor.row_count,
        "recompressed block_number column with signed deltas"
    );

    Ok(true)
}

fn block_number_only_profile_mismatch(columns: &[ColumnDescriptor]) -> Option<usize> {
    if columns.len() != current_column_profile().len() {
        return None;
    }

    let mut block_column_index = None;
    for (name, codec) in current_column_profile() {
        let (index, column) = columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == *name && column.page_index_path.is_some())?;
        if *name == "block_number" {
            if column.codec != CompressionCodec::Delta {
                return None;
            }
            block_column_index = Some(index);
        } else if column.codec != *codec {
            return None;
        }
    }

    block_column_index
}

pub(crate) fn segment_is_compacted(
    paths: &StorageCatalogPaths,
    segment_id: u64,
) -> std::io::Result<bool> {
    Ok(existing_columns(paths, segment_id)?
        .map(|columns| {
            columns
                .iter()
                .all(|column| column.page_index_path.is_some())
        })
        .unwrap_or(false))
}

pub(crate) fn segment_uses_current_compaction_profile(
    paths: &StorageCatalogPaths,
    segment_id: u64,
) -> std::io::Result<bool> {
    Ok(existing_columns(paths, segment_id)?
        .map(|columns| columns_match_current_profile(&columns))
        .unwrap_or(false))
}

fn columns_match_current_profile(columns: &[ColumnDescriptor]) -> bool {
    columns.len() == current_column_profile().len()
        && current_column_profile().iter().all(|(name, codec)| {
            columns.iter().any(|column| {
                column.name == *name && column.codec == *codec && column.page_index_path.is_some()
            })
        })
}

pub(crate) fn current_column_profile() -> &'static [(&'static str, CompressionCodec)] {
    &[
        ("address", CompressionCodec::AdaptiveFixed),
        ("block_number", CompressionCodec::DeltaZigZag),
        ("block_hash", CompressionCodec::AdaptiveFixed),
        ("timestamp", CompressionCodec::DeltaOfDelta),
        ("tx_hash", CompressionCodec::AdaptiveFixed),
        ("tx_index", CompressionCodec::Zstd),
        ("log_index", CompressionCodec::Zstd),
        ("data_len", CompressionCodec::Zstd),
        ("source", CompressionCodec::Dictionary),
        ("topic0", CompressionCodec::AdaptiveFixed),
        ("topic1", CompressionCodec::AdaptiveFixed),
        ("topic2", CompressionCodec::AdaptiveFixed),
        ("topic3", CompressionCodec::AdaptiveFixed),
        ("data", CompressionCodec::AdaptiveBytes),
    ]
}

fn existing_columns(
    paths: &StorageCatalogPaths,
    segment_id: u64,
) -> std::io::Result<Option<Vec<ColumnDescriptor>>> {
    let path = paths.segment_manifest_path(segment_id);
    if !path.exists() {
        return Ok(None);
    }

    let json = fs::read(&path)?;
    let manifest: SegmentManifest = serde_json::from_slice(&json).map_err(std::io::Error::other)?;
    Ok(Some(manifest.columns))
}

pub(super) fn default_columns() -> Vec<ColumnDescriptor> {
    vec![
        fixed_column("address", "address.col"),
        fixed_column("block_number", "block_number.col"),
        fixed_column("block_hash", "block_hash.col"),
        fixed_column("timestamp", "timestamp.col"),
        fixed_column("tx_hash", "tx_hash.col"),
        fixed_column("tx_index", "tx_index.col"),
        fixed_column("log_index", "log_index.col"),
        fixed_column("data_len", "data_len.col"),
        fixed_column("source", "source.col"),
        nullable_column("topic0"),
        nullable_column("topic1"),
        nullable_column("topic2"),
        nullable_column("topic3"),
        ColumnDescriptor {
            name: "data".to_owned(),
            codec: CompressionCodec::None,
            page_rows: DEFAULT_PAGE_ROWS,
            data_path: "data.col".to_owned(),
            null_bitmap_path: None,
            page_index_path: None,
        },
    ]
}

fn fixed_column(name: &str, data_path: &str) -> ColumnDescriptor {
    ColumnDescriptor {
        name: name.to_owned(),
        codec: CompressionCodec::None,
        page_rows: DEFAULT_PAGE_ROWS,
        data_path: data_path.to_owned(),
        null_bitmap_path: None,
        page_index_path: None,
    }
}

fn nullable_column(base_name: &str) -> ColumnDescriptor {
    ColumnDescriptor {
        name: base_name.to_owned(),
        codec: CompressionCodec::None,
        page_rows: DEFAULT_PAGE_ROWS,
        data_path: format!("{base_name}.col"),
        null_bitmap_path: Some(format!("{base_name}.null")),
        page_index_path: None,
    }
}

fn compact_address_column(output: &PageOutput<'_>) -> std::io::Result<ColumnDescriptor> {
    compact_fixed_column::<20>(output, "address", CompressionCodec::AdaptiveFixed)
}

fn compact_fixed_column<const WIDTH: usize>(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let column = RawFixedColumn::<WIDTH>::open(&output.dir.join(format!("{name}.col")))?;
    let values = column.values();
    write_encoded_pages(output, name, codec, values.len(), |range| {
        encode_fixed_width_page(values[range].as_flattened(), WIDTH, codec)
    })
}

fn compact_address_values(
    output: &PageOutput<'_>,
    values: impl IntoIterator<Item = Address>,
) -> std::io::Result<ColumnDescriptor> {
    let values = values.into_iter();
    let capacity = values
        .size_hint()
        .0
        .checked_mul(20)
        .ok_or_else(|| std::io::Error::other("fixed column capacity overflow"))?;
    let mut raw = Vec::new();
    raw.try_reserve_exact(capacity)
        .map_err(std::io::Error::other)?;
    for value in values {
        raw.extend_from_slice(value.as_slice());
    }
    write_encoded_pages(
        output,
        "address",
        CompressionCodec::AdaptiveFixed,
        raw.len() / 20,
        |range| {
            encode_fixed_width_page(
                &raw[range.start * 20..range.end * 20],
                20,
                CompressionCodec::AdaptiveFixed,
            )
        },
    )
}

fn compact_b256_values(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = B256>,
) -> std::io::Result<ColumnDescriptor> {
    let values = values.into_iter();
    let capacity = values
        .size_hint()
        .0
        .checked_mul(32)
        .ok_or_else(|| std::io::Error::other("fixed column capacity overflow"))?;
    let mut raw = Vec::new();
    raw.try_reserve_exact(capacity)
        .map_err(std::io::Error::other)?;
    for value in values {
        raw.extend_from_slice(value.as_slice());
    }
    write_encoded_pages(output, name, codec, raw.len() / 32, |range| {
        encode_fixed_width_page(&raw[range.start * 32..range.end * 32], 32, codec)
    })
}

fn compact_b256_column(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    compact_fixed_column::<32>(output, name, codec)
}

fn compact_nullable_b256_column(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let mut column = RawFixedColumn::<32>::open(&output.dir.join(format!("{name}.col")))?;
    let nulls = column.read_nulls(&output.dir.join(format!("{name}.null")))?;
    // Match the typed encoder's canonical zero slots for absent values, even
    // if an input file contains nonzero bytes under an unset presence bit.
    for (row, value) in column.values_mut().iter_mut().enumerate() {
        if !nulls.is_present(row as u64) {
            value.fill(0);
        }
    }
    let values = column.values();
    let descriptor = write_encoded_pages(output, name, codec, values.len(), |range| {
        encode_fixed_width_page(values[range].as_flattened(), 32, codec)
    })?;
    write_compacted_nulls(output, descriptor, &nulls)
}

fn compact_nullable_b256_values(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = Option<B256>>,
) -> std::io::Result<ColumnDescriptor> {
    let values = values.into_iter();
    let capacity = values
        .size_hint()
        .0
        .checked_mul(32)
        .ok_or_else(|| std::io::Error::other("fixed column capacity overflow"))?;
    let mut raw = Vec::new();
    raw.try_reserve_exact(capacity)
        .map_err(std::io::Error::other)?;
    let mut nulls = output.previous_nulls(name)?;
    for value in values {
        match value {
            Some(value) => {
                nulls.push(true);
                raw.extend_from_slice(value.as_slice());
            }
            None => {
                nulls.push(false);
                raw.extend_from_slice(&[0u8; 32]);
            }
        }
    }

    let descriptor = write_encoded_pages(output, name, codec, raw.len() / 32, |range| {
        encode_fixed_width_page(&raw[range.start * 32..range.end * 32], 32, codec)
    })?;
    write_compacted_nulls(output, descriptor, &nulls)
}

fn write_compacted_nulls(
    output: &PageOutput<'_>,
    mut descriptor: ColumnDescriptor,
    nulls: &NullBitmap,
) -> std::io::Result<ColumnDescriptor> {
    let null_rel = output
        .previous
        .get(&descriptor.name)
        .and_then(|previous| previous.column.null_bitmap_path.clone())
        .unwrap_or_else(|| format!("columns/{}.null", descriptor.name));
    output.metadata(&output.dir.join(&null_rel), |writer| nulls.write_to(writer))?;
    descriptor.null_bitmap_path = Some(null_rel);
    Ok(descriptor)
}

fn compact_u64_column(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u64(output.dir, &format!("{name}.col"), None)?;
    compact_u64_values(output, name, codec, values)
}

fn compact_u64_values(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = u64>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    write_typed_pages(output, name, codec, &values, |slice| {
        encode_u64_page(slice, codec)
    })
}

fn compact_u32_column(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u32(output.dir, &format!("{name}.col"), None)?;
    compact_u32_values(output, name, codec, values)
}

fn compact_u32_values(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = u32>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    write_typed_pages(output, name, codec, &values, |slice| {
        encode_u32_page(slice, codec)
    })
}

fn compact_u8_column(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u8(output.dir, &format!("{name}.col"), None)?;
    compact_u8_values(output, name, codec, values)
}

fn compact_u8_values(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = u8>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    write_typed_pages(output, name, codec, &values, |slice| {
        encode_u8_page(slice, codec)
    })
}

fn compact_data_column(
    output: &PageOutput<'_>,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let column = RawBytesColumn::open(&output.dir.join("data.col"))?;
    let mut values = Vec::with_capacity(DEFAULT_PAGE_ROWS as usize);
    write_encoded_pages(output, "data", codec, column.row_count(), |range| {
        values.clear();
        for row in range {
            values.push(column.row(row)?);
        }
        encode_var_bytes_page(&values, codec)
    })
}

fn compact_data_values(
    output: &PageOutput<'_>,
    rows: &[LogRow],
) -> std::io::Result<ColumnDescriptor> {
    // Borrow payloads for one page instead of cloning the entire column before
    // compression. This bounds temporary references and avoids Bytes refcounts.
    let mut values = Vec::with_capacity(DEFAULT_PAGE_ROWS as usize);
    write_typed_pages(
        output,
        "data",
        CompressionCodec::AdaptiveBytes,
        rows,
        |slice| {
            values.clear();
            values.extend(slice.iter().map(|row| row.data.as_ref()));
            encode_var_bytes_page(&values, CompressionCodec::AdaptiveBytes)
        },
    )
}

fn rewrite_column_dir(columns: &mut [ColumnDescriptor], dir_name: &str) {
    for column in columns {
        rewrite_column_path_descriptor(column, dir_name);
    }
}

fn rewrite_column_path_descriptor(column: &mut ColumnDescriptor, dir_name: &str) {
    column.data_path = rewrite_column_path(&column.data_path, dir_name);
    column.null_bitmap_path = column
        .null_bitmap_path
        .as_ref()
        .map(|path| rewrite_column_path(path, dir_name));
    column.page_index_path = column
        .page_index_path
        .as_ref()
        .map(|path| rewrite_column_path(path, dir_name));
}

fn rewrite_column_path(path: &str, dir_name: &str) -> String {
    path.strip_prefix("columns/")
        .map(|suffix| format!("{dir_name}/{suffix}"))
        .unwrap_or_else(|| path.to_owned())
}

fn write_encoded_pages<F>(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
    row_count: usize,
    mut encode_page: F,
) -> std::io::Result<ColumnDescriptor>
where
    F: FnMut(Range<usize>) -> std::io::Result<Vec<u8>>,
{
    let (data_path, index_path, page_rows) =
        write_pages(output, name, row_count, &mut encode_page)?;
    Ok(ColumnDescriptor {
        name: name.to_owned(),
        codec,
        page_rows,
        data_path,
        null_bitmap_path: None,
        page_index_path: Some(index_path),
    })
}

fn write_typed_pages<'a, T, F>(
    output: &PageOutput<'_>,
    name: &str,
    codec: CompressionCodec,
    values: &'a [T],
    mut encode_page: F,
) -> std::io::Result<ColumnDescriptor>
where
    F: FnMut(&'a [T]) -> std::io::Result<Vec<u8>>,
{
    let (data_path, index_path, page_rows) = write_pages(output, name, values.len(), |range| {
        encode_page(&values[range])
    })?;
    Ok(ColumnDescriptor {
        name: name.to_owned(),
        codec,
        page_rows,
        data_path,
        null_bitmap_path: None,
        page_index_path: Some(index_path),
    })
}

fn write_pages<F>(
    output: &PageOutput<'_>,
    name: &str,
    row_count: usize,
    mut encode_page: F,
) -> std::io::Result<(String, String, u32)>
where
    F: FnMut(Range<usize>) -> std::io::Result<Vec<u8>>,
{
    let page_rows = DEFAULT_PAGE_ROWS;
    let previous = output.previous.get(name);
    let data_rel = previous
        .map(|p| p.column.data_path.clone())
        .unwrap_or_else(|| format!("columns/{name}.pages"));
    let index_rel = previous
        .and_then(|p| p.column.page_index_path.clone())
        .unwrap_or_else(|| format!("columns/{name}.pages.idx"));
    let data_path = output.dir.join(&data_rel);
    let index_path = output.dir.join(&index_rel);
    let mut writer = if output.bundle.is_some() {
        None
    } else {
        let file = if previous.is_some() {
            fs::OpenOptions::new().append(true).open(&data_path)?
        } else {
            File::create(&data_path)?
        };
        Some(BufWriter::new(file))
    };
    let mut entries = if output.bundle.is_some() {
        Vec::new()
    } else {
        previous.map(|p| p.entries.clone()).unwrap_or_default()
    };
    let mut offset = previous.map_or(0, |p| p.encoded_bytes);

    let mut start = 0usize;
    while start < row_count {
        let end = start.saturating_add(page_rows as usize).min(row_count);
        let encoded = encode_page(start..end)?;
        let encoded_len = u32::try_from(encoded.len()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "encoded page exceeds page-index length",
            )
        })?;
        let first_row = output
            .existing_rows
            .checked_add(start as u64)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "page row offset overflow")
            })?;
        let next_offset = offset.checked_add(u64::from(encoded_len)).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "page byte offset overflow",
            )
        })?;
        if let Some((bundle, _)) = &output.bundle {
            bundle.append_data(stream_id(&data_rel)?, &encoded)?;
        } else if let Some(writer) = &mut writer {
            writer.write_all(&encoded)?;
        }
        entries.push(PageIndexEntry {
            first_row,
            row_count: (end - start) as u32,
            offset,
            encoded_len,
        });
        offset = next_offset;
        start = end;
    }
    if let Some(writer) = &mut writer {
        writer.flush()?;
    }
    let index = write_page_index(&entries);
    if let Some((bundle, _)) = &output.bundle {
        bundle.append_data(
            stream_id(&index_rel)?,
            &index[crate::page::PAGE_INDEX_HEADER_BYTES..],
        )?;
    } else {
        output.metadata(&index_path, |writer| writer.write_all(&index))?;
    }
    Ok((data_rel, index_rel, page_rows))
}

fn remove_raw_hot_files(segment_dir: &Path) -> std::io::Result<()> {
    for name in RAW_FIXED_COLUMNS
        .iter()
        .map(|(name, _width)| *name)
        .chain(std::iter::once("data.col"))
        .chain(RAW_NULL_BITMAP_COLUMNS.iter().copied())
    {
        let path = segment_dir.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

pub(crate) fn verify_raw_segment_files_complete(
    descriptor: &SegmentDescriptor,
    segment_dir: &Path,
) -> std::io::Result<()> {
    for (name, width) in RAW_FIXED_COLUMNS {
        verify_fixed_raw_column_file(descriptor, segment_dir, name, *width)?;
    }
    for name in RAW_BITMAP_COLUMNS {
        verify_raw_bitmap_file(descriptor, segment_dir, name)?;
    }
    verify_raw_data_column_file(descriptor, segment_dir)?;
    Ok(())
}

fn verify_fixed_raw_column_file(
    descriptor: &SegmentDescriptor,
    segment_dir: &Path,
    name: &str,
    width: u64,
) -> std::io::Result<()> {
    let path = segment_dir.join(name);
    let header = read_raw_column_header(descriptor, &path, name)?;
    if header.row_count != descriptor.row_count {
        return Err(raw_segment_error(
            descriptor,
            name,
            format!(
                "row-count mismatch: manifest={} header={}",
                descriptor.row_count, header.row_count
            ),
        ));
    }

    let expected_len =
        (ColumnFileHeader::SIZE as u64)
            .checked_add(header.row_count.checked_mul(width).ok_or_else(|| {
                raw_segment_error(descriptor, name, "expected byte length overflow")
            })?)
            .ok_or_else(|| raw_segment_error(descriptor, name, "expected byte length overflow"))?;
    let actual_len = raw_file_len(descriptor, &path, name)?;
    if actual_len != expected_len {
        return Err(raw_segment_error(
            descriptor,
            name,
            format!("length mismatch: expected {expected_len} bytes, got {actual_len} bytes"),
        ));
    }

    Ok(())
}

fn verify_raw_bitmap_file(
    descriptor: &SegmentDescriptor,
    segment_dir: &Path,
    name: &str,
) -> std::io::Result<()> {
    let path = segment_dir.join(name);
    let mut file = open_raw_file(descriptor, &path, name)?;
    let mut len_bytes = [0u8; 8];
    file.read_exact(&mut len_bytes).map_err(|error| {
        raw_segment_error(
            descriptor,
            name,
            format!("failed to read bitmap length: {error}"),
        )
    })?;
    let bitmap_rows = u64::from_le_bytes(len_bytes);
    if bitmap_rows != descriptor.row_count {
        return Err(raw_segment_error(
            descriptor,
            name,
            format!(
                "row-count mismatch: manifest={} bitmap={bitmap_rows}",
                descriptor.row_count
            ),
        ));
    }

    let expected_len = 8u64
        .checked_add(bitmap_rows.div_ceil(8))
        .ok_or_else(|| raw_segment_error(descriptor, name, "expected byte length overflow"))?;
    let actual_len = file
        .metadata()
        .map_err(|error| {
            raw_segment_error(
                descriptor,
                name,
                format!("failed to read file metadata: {error}"),
            )
        })?
        .len();
    if actual_len != expected_len {
        return Err(raw_segment_error(
            descriptor,
            name,
            format!("length mismatch: expected {expected_len} bytes, got {actual_len} bytes"),
        ));
    }

    Ok(())
}

fn verify_raw_data_column_file(
    descriptor: &SegmentDescriptor,
    segment_dir: &Path,
) -> std::io::Result<()> {
    let name = "data.col";
    let path = segment_dir.join(name);
    let mut file = open_raw_file(descriptor, &path, name)?;
    let header = read_raw_column_header_from_file(descriptor, &mut file, name)?;
    if header.row_count != descriptor.row_count {
        return Err(raw_segment_error(
            descriptor,
            name,
            format!(
                "row-count mismatch: manifest={} header={}",
                descriptor.row_count, header.row_count
            ),
        ));
    }

    let offsets_size = header
        .row_count
        .checked_add(1)
        .and_then(|count| count.checked_mul(8))
        .ok_or_else(|| raw_segment_error(descriptor, name, "offset table size overflow"))?;
    let blob_start = (ColumnFileHeader::SIZE as u64)
        .checked_add(offsets_size)
        .ok_or_else(|| raw_segment_error(descriptor, name, "blob offset overflow"))?;
    let actual_len = file
        .metadata()
        .map_err(|error| {
            raw_segment_error(
                descriptor,
                name,
                format!("failed to read file metadata: {error}"),
            )
        })?
        .len();
    if actual_len < blob_start {
        return Err(raw_segment_error(
            descriptor,
            name,
            format!(
                "offset table is truncated: expected at least {blob_start} bytes, got {actual_len} bytes"
            ),
        ));
    }

    let last_offset_position =
        (ColumnFileHeader::SIZE as u64)
            .checked_add(header.row_count.checked_mul(8).ok_or_else(|| {
                raw_segment_error(descriptor, name, "last offset position overflow")
            })?)
            .ok_or_else(|| raw_segment_error(descriptor, name, "last offset position overflow"))?;
    file.seek(SeekFrom::Start(last_offset_position))
        .map_err(|error| {
            raw_segment_error(
                descriptor,
                name,
                format!("failed to seek final data offset: {error}"),
            )
        })?;
    let mut last_offset_bytes = [0u8; 8];
    file.read_exact(&mut last_offset_bytes).map_err(|error| {
        raw_segment_error(
            descriptor,
            name,
            format!("failed to read final data offset: {error}"),
        )
    })?;
    let last_offset = u64::from_le_bytes(last_offset_bytes);
    let blob_len = actual_len - blob_start;
    if last_offset != blob_len {
        return Err(raw_segment_error(
            descriptor,
            name,
            format!(
                "blob length mismatch: final offset={last_offset} bytes, blob={blob_len} bytes"
            ),
        ));
    }

    Ok(())
}

fn read_raw_column_header(
    descriptor: &SegmentDescriptor,
    path: &Path,
    name: &str,
) -> std::io::Result<ColumnFileHeader> {
    let mut file = open_raw_file(descriptor, path, name)?;
    read_raw_column_header_from_file(descriptor, &mut file, name)
}

fn open_raw_file(descriptor: &SegmentDescriptor, path: &Path, name: &str) -> std::io::Result<File> {
    File::open(path).map_err(|error| {
        raw_segment_error(
            descriptor,
            name,
            format!("failed to open {}: {error}", path.display()),
        )
    })
}

fn raw_file_len(descriptor: &SegmentDescriptor, path: &Path, name: &str) -> std::io::Result<u64> {
    fs::metadata(path)
        .map_err(|error| {
            raw_segment_error(
                descriptor,
                name,
                format!("failed to read metadata for {}: {error}", path.display()),
            )
        })
        .map(|metadata| metadata.len())
}

fn read_raw_column_header_from_file(
    descriptor: &SegmentDescriptor,
    file: &mut File,
    name: &str,
) -> std::io::Result<ColumnFileHeader> {
    let mut header_buf = [0u8; ColumnFileHeader::SIZE];
    file.read_exact(&mut header_buf).map_err(|error| {
        raw_segment_error(
            descriptor,
            name,
            format!("failed to read column header: {error}"),
        )
    })?;
    ColumnFileHeader::read_from(&header_buf)
        .ok_or_else(|| raw_segment_error(descriptor, name, "corrupt column header"))
}

fn raw_segment_error(
    descriptor: &SegmentDescriptor,
    name: &str,
    detail: impl std::fmt::Display,
) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!(
            "segment {} raw column {name} is incomplete: {detail}",
            descriptor.id
        ),
    )
}

fn remove_superseded_column_dirs(segment_dir: &Path, active_dir: &str) -> std::io::Result<()> {
    for entry in fs::read_dir(segment_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }

        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with("columns") && name != active_dir {
            let _ = fs::remove_dir_all(entry.path());
        }
    }
    Ok(())
}

fn remove_column_files(segment_dir: &Path, column: &ColumnDescriptor) -> std::io::Result<()> {
    remove_file_if_exists(segment_dir.join(&column.data_path))?;
    if let Some(path) = &column.null_bitmap_path {
        remove_file_if_exists(segment_dir.join(path))?;
    }
    if let Some(path) = &column.page_index_path {
        remove_file_if_exists(segment_dir.join(path))?;
    }
    Ok(())
}

fn remove_file_if_exists(path: PathBuf) -> std::io::Result<()> {
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn collect_indexes(segment_dir: &Path) -> std::io::Result<Vec<IndexDescriptor>> {
    let index_dir = segment_dir.join("indexes");
    if !index_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries: Vec<PathBuf> = fs::read_dir(&index_dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_file())
        .collect();
    entries.sort();

    Ok(entries
        .into_iter()
        .filter_map(|path| {
            let file_name = path.file_name()?.to_str()?.to_owned();
            let kind = match file_name.as_str() {
                "address.bptree" => IndexKind::Address,
                "topic0.bptree" => IndexKind::Topic0,
                "block_number.bptree" => IndexKind::BlockNumber,
                "block_hash.bptree" => IndexKind::BlockHash,
                "timestamp.bptree" => IndexKind::Timestamp,
                "address_topic0.bptree" => IndexKind::AddressTopic0,
                "address_topic0_topic1.bptree" => IndexKind::AddressTopic0Topic1,
                "address_topic0_topic2.bptree" => IndexKind::AddressTopic0Topic2,
                _ => IndexKind::Custom,
            };
            Some(IndexDescriptor {
                kind,
                name: file_name.clone(),
                data_path: format!("indexes/{file_name}"),
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::bytes;
    use logex_types::Source;
    use tempfile::TempDir;

    fn descending_rows() -> Vec<LogRow> {
        (0..8192)
            .map(|index| {
                let block_number = 2_000u64 - (index / 8) as u64;
                LogRow {
                    block_number,
                    block_hash: B256::repeat_byte((block_number % 251) as u8),
                    timestamp: 1_700_000_000 - (index / 8) as u64 * 12,
                    tx_hash: B256::repeat_byte((index % 251) as u8),
                    tx_index: (index % 4) as u32,
                    log_index: index as u32,
                    address: Address::repeat_byte((index % 17) as u8),
                    topic0: Some(B256::repeat_byte(0xdd)),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                    data: bytes!("cafe"),
                    data_len: 2,
                    source: Source::Receipt,
                }
            })
            .collect()
    }

    #[test]
    fn appended_pages_wait_for_manifest_publication() {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_owned());
        paths.ensure_base_dirs().unwrap();
        let mut descriptor = SegmentDescriptor {
            column_bundle: None,
            id: 7,
            generation: 0,
            kind: SegmentKind::Sealed,
            relative_path: PathBuf::from("segments/s_0000000000000007"),
            manifest_relative_path: PathBuf::from("segments/s_0000000000000007/segment.json"),
            min_block: None,
            max_block: None,
            min_timestamp: None,
            max_timestamp: None,
            row_count: 0,
        };
        let dir = paths.segment_dir(descriptor.id);
        let rows = &descending_rows()[..5];
        let columns = write_compacted_rows(&dir, &rows[..2]).unwrap();
        apply_rows_to_descriptor(&mut descriptor, &rows[..2]);
        persist_segment_manifest_with_columns(&paths, &descriptor, columns.clone()).unwrap();
        let mut bits = NullBitmap::new();
        bits.push(false);
        bits.push(true);
        ColumnFile::replace_canonical_bitmap(&dir, &bits).unwrap();
        let snapshot = SegmentReader::open(&dir).unwrap();
        let payloads: Vec<_> = columns
            .iter()
            .map(|column| fs::read(dir.join(&column.data_path)).unwrap())
            .collect();
        let indexes: Vec<_> = columns
            .iter()
            .map(|column| {
                read_page_index(
                    &fs::read(dir.join(column.page_index_path.as_ref().unwrap())).unwrap(),
                )
                .unwrap()
            })
            .collect();
        let appended =
            append_compacted_rows(&dir, 2, &rows[2..], Publication::Ordered, None).unwrap();
        for ((column, payload), index) in columns.iter().zip(payloads).zip(indexes) {
            assert!(
                fs::read(dir.join(&column.data_path))
                    .unwrap()
                    .starts_with(&payload)
            );
            let after = read_page_index(
                &fs::read(dir.join(column.page_index_path.as_ref().unwrap())).unwrap(),
            )
            .unwrap();
            assert!(after.starts_with(&index));
        }
        assert_eq!(snapshot.read_log_rows(None).unwrap(), rows[..2]);
        assert!(snapshot.read_log_rows(Some(&[2])).is_err());
        apply_rows_to_descriptor(&mut descriptor, &rows[2..]);
        let appended = appended.apply_to(&mut descriptor);
        persist_segment_manifest_with_columns(&paths, &descriptor, appended).unwrap();
        assert_eq!(snapshot.read_log_rows(None).unwrap(), rows[..2]);
        let current = SegmentReader::open(&dir).unwrap();
        assert_eq!(current.read_log_rows(None).unwrap(), rows);
        let bits = current.read_canonical().unwrap();
        assert!(!bits.is_present(0));
        for row in 1..5 {
            assert!(bits.is_present(row));
        }
    }

    #[test]
    fn compacted_append_rejects_bad_metadata_without_mutating_files() {
        for bundled in [false, true] {
            let tmp = TempDir::new().unwrap();
            let paths = StorageCatalogPaths::new(tmp.path().to_owned());
            paths.ensure_base_dirs().unwrap();
            let config = super::super::catalog::NativeStorageConfig {
                data_dir: tmp.path().to_owned(),
                ..Default::default()
            };
            let (mut catalog, _) =
                super::super::catalog::NativeStorageCatalog::open_or_create(&config).unwrap();
            let mut descriptor = catalog.allocate_segment(SegmentKind::Sealed).unwrap();
            let dir = paths.segment_dir(descriptor.id);
            let rows = &descending_rows()[..5];
            apply_rows_to_descriptor(&mut descriptor, &rows[..2]);
            let columns = if bundled {
                write_bundled_rows(&dir, &rows[..2])
                    .unwrap()
                    .apply_to(&mut descriptor)
            } else {
                write_compacted_rows(&dir, &rows[..2]).unwrap()
            };
            persist_segment_manifest_with_columns(&paths, &descriptor, columns).unwrap();
            let original: SegmentManifest =
                serde_json::from_slice(&fs::read(dir.join("segment.json")).unwrap()).unwrap();
            let paths: Vec<String> = if bundled {
                vec![BUNDLE_PATH.to_owned()]
            } else {
                original
                    .columns
                    .iter()
                    .flat_map(|column| {
                        [
                            Some(column.data_path.clone()),
                            column.page_index_path.clone(),
                            column.null_bitmap_path.clone(),
                        ]
                        .into_iter()
                        .flatten()
                    })
                    .chain(["canonical.bitmap".to_owned()])
                    .collect()
            };
            let files: Vec<_> = paths
                .iter()
                .map(|p| fs::read(dir.join(p)).unwrap())
                .collect();
            for case in 0..12 {
                let mut manifest = original.clone();
                match case {
                    0 => manifest.format_version += 1,
                    1 if bundled => manifest.column_bundle.as_mut().unwrap().row_count += 1,
                    1 => manifest.kind = SegmentKind::Hot,
                    2 => manifest.columns[0].data_path = "../address.pages".into(),
                    3 => manifest.columns[0].page_index_path = Some("../index".into()),
                    4 => manifest.canonical_rows_path = "../canonical.bitmap".into(),
                    5 => manifest.columns[0].page_rows = u32::MAX,
                    6 => manifest.columns[0].codec = CompressionCodec::None,
                    7 => manifest.columns[0] = manifest.columns[1].clone(),
                    8 => {
                        manifest.columns.last_mut().unwrap().null_bitmap_path =
                            Some("../null".into())
                    }
                    9 => {
                        let path = dir.join(if bundled {
                            BUNDLE_PATH
                        } else {
                            &original.columns[0].data_path
                        });
                        fs::OpenOptions::new()
                            .write(true)
                            .open(path)
                            .unwrap()
                            .set_len(0)
                            .unwrap();
                    }
                    10 => {
                        let index = original.columns[0].page_index_path.as_ref().unwrap();
                        let artifacts = ColumnArtifacts::open(&dir, Some(&original)).unwrap();
                        let mut entries = read_page_index(&artifacts.read(index).unwrap()).unwrap();
                        entries[0].row_count += 1;
                        if bundled {
                            let writer = BundleWriter::append(
                                &dir.join(BUNDLE_PATH),
                                original.column_bundle.as_ref().unwrap(),
                            )
                            .unwrap();
                            writer
                                .append_data(
                                    stream_id(index).unwrap(),
                                    &write_page_index(&entries)
                                        [crate::page::PAGE_INDEX_HEADER_BYTES..],
                                )
                                .unwrap();
                            manifest.column_bundle = Some(writer.finish(2).unwrap());
                        } else {
                            fs::write(dir.join(index), write_page_index(&entries)).unwrap();
                        }
                    }
                    11 if bundled => {
                        let writer = BundleWriter::append(
                            &dir.join(BUNDLE_PATH),
                            original.column_bundle.as_ref().unwrap(),
                        )
                        .unwrap();
                        writer
                            .replace_metadata(crate::column_artifact::CANONICAL_STREAM, &[0; 8])
                            .unwrap();
                        manifest.column_bundle = Some(writer.finish(2).unwrap());
                    }
                    11 => fs::write(dir.join("canonical.bitmap"), [0u8; 8]).unwrap(),
                    _ => unreachable!(),
                }
                let json = serde_json::to_vec(&manifest).unwrap();
                fs::write(dir.join("segment.json"), &json).unwrap();
                let before: Vec<_> = paths
                    .iter()
                    .map(|p| fs::read(dir.join(p)).unwrap())
                    .collect();
                assert!(
                    append_compacted_rows(&dir, 2, &rows[2..], Publication::Deferred, None)
                        .is_err(),
                    "case {case}"
                );
                assert_eq!(fs::read(dir.join("segment.json")).unwrap(), json);
                for ((path, before), original) in paths.iter().zip(before).zip(&files) {
                    assert_eq!(
                        fs::read(dir.join(path)).unwrap(),
                        before,
                        "case {case}: {path}"
                    );
                    fs::write(dir.join(path), original).unwrap();
                }
            }
        }
    }

    #[test]
    fn inspected_append_rejects_changed_manifest_reference_without_writes() {
        let tmp = TempDir::new().unwrap();
        let config = super::super::catalog::NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            ..Default::default()
        };
        let (mut catalog, paths) =
            super::super::catalog::NativeStorageCatalog::open_or_create(&config).unwrap();
        let mut descriptor = catalog.allocate_segment(SegmentKind::Sealed).unwrap();
        let dir = paths.segment_dir(descriptor.id);
        let rows = &descending_rows()[..5];
        apply_rows_to_descriptor(&mut descriptor, &rows[..2]);
        let columns = write_bundled_rows(&dir, &rows[..2])
            .unwrap()
            .apply_to(&mut descriptor);
        persist_segment_manifest_with_columns(&paths, &descriptor, columns.clone()).unwrap();
        let inspected = BundleReader::open(
            &dir.join(BUNDLE_PATH),
            descriptor.column_bundle.as_ref().unwrap(),
        )
        .unwrap();
        let writer = BundleWriter::append(
            &dir.join(BUNDLE_PATH),
            descriptor.column_bundle.as_ref().unwrap(),
        )
        .unwrap();
        descriptor.column_bundle = Some(writer.finish(2).unwrap());
        persist_segment_manifest_with_columns(&paths, &descriptor, columns).unwrap();
        let paths = [BUNDLE_PATH, "segment.json"];
        let before: Vec<_> = paths
            .iter()
            .map(|path| fs::read(dir.join(path)).unwrap())
            .collect();
        assert!(
            append_compacted_rows(
                &dir,
                2,
                &rows[2..],
                Publication::Deferred,
                Some(inspected.clone())
            )
            .is_err()
        );
        let mut manifest: SegmentManifest = serde_json::from_slice(&before[1]).unwrap();
        manifest.column_bundle = None;
        assert!(ColumnArtifacts::open_inspected(&dir, Some(&manifest), Some(inspected)).is_err());
        for (path, bytes) in paths.iter().zip(before) {
            assert_eq!(fs::read(dir.join(path)).unwrap(), bytes);
        }
    }

    #[test]
    fn borrowed_fixed_compaction_matches_typed_pages_across_page_boundary() {
        let tmp = TempDir::new().unwrap();
        let raw_dir = tmp.path().join("raw");
        let typed_dir = tmp.path().join("typed");
        fs::create_dir_all(raw_dir.join("columns")).unwrap();
        fs::create_dir_all(typed_dir.join("columns")).unwrap();
        let mut rows: Vec<_> = descending_rows()
            .into_iter()
            .cycle()
            .take(DEFAULT_PAGE_ROWS as usize + 3)
            .collect();
        for (index, row) in rows.iter_mut().enumerate() {
            row.topic0 = (index % 3 == 0).then_some(row.tx_hash);
        }
        ColumnFile::write_batch_with_publication(&raw_dir, &rows, None, Publication::Deferred)
            .unwrap();
        // Absent slots may contain arbitrary bytes; the presence bit controls
        // their meaning. Both encoders must produce the same zeroed payload.
        let path = raw_dir.join("topic0.col");
        let mut bytes = fs::read(&path).unwrap();
        for (row, value) in bytes[ColumnFileHeader::SIZE..]
            .as_chunks_mut::<32>()
            .0
            .iter_mut()
            .enumerate()
        {
            if rows[row].topic0.is_none() {
                value.fill(0xa5);
            }
        }
        fs::write(path, bytes).unwrap();
        for (actual, expected) in [
            (
                compact_address_column(&PageOutput::new(&raw_dir)).unwrap(),
                compact_address_values(
                    &PageOutput::new(&typed_dir),
                    rows.iter().map(|r| r.address),
                )
                .unwrap(),
            ),
            (
                compact_b256_column(
                    &PageOutput::new(&raw_dir),
                    "block_hash",
                    CompressionCodec::AdaptiveFixed,
                )
                .unwrap(),
                compact_b256_values(
                    &PageOutput::new(&typed_dir),
                    "block_hash",
                    CompressionCodec::AdaptiveFixed,
                    rows.iter().map(|r| r.block_hash),
                )
                .unwrap(),
            ),
            (
                compact_nullable_b256_column(
                    &PageOutput::new(&raw_dir),
                    "topic0",
                    CompressionCodec::AdaptiveFixed,
                )
                .unwrap(),
                compact_nullable_b256_values(
                    &PageOutput::new(&typed_dir),
                    "topic0",
                    CompressionCodec::AdaptiveFixed,
                    rows.iter().map(|r| r.topic0),
                )
                .unwrap(),
            ),
        ] {
            assert_eq!(
                serde_json::to_value(&actual).unwrap(),
                serde_json::to_value(&expected).unwrap()
            );
            for relative in [
                Some(actual.data_path),
                actual.page_index_path,
                actual.null_bitmap_path,
            ]
            .into_iter()
            .flatten()
            {
                assert_eq!(
                    fs::read(raw_dir.join(&relative)).unwrap(),
                    fs::read(typed_dir.join(&relative)).unwrap(),
                    "{relative}"
                );
            }
        }
    }

    #[test]
    fn recompact_segment_rewrites_only_legacy_block_number_profile() {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let descriptor = SegmentDescriptor {
            column_bundle: None,
            id: 7,
            generation: 0,
            kind: SegmentKind::Sealed,
            relative_path: PathBuf::from("segments").join("s_0000000000000007"),
            manifest_relative_path: PathBuf::from("segments")
                .join("s_0000000000000007")
                .join("segment.json"),
            min_block: Some(0),
            max_block: Some(2_000),
            min_timestamp: Some(1_699_996_940),
            max_timestamp: Some(1_700_000_000),
            row_count: 8192,
        };
        let segment_dir = paths.segment_dir(descriptor.id);
        let rows = descending_rows();
        let mut columns = write_compacted_rows(&segment_dir, &rows).unwrap();
        let legacy_block_number = compact_u64_values(
            &PageOutput::new(&segment_dir),
            "block_number",
            CompressionCodec::Delta,
            rows.iter().map(|row| row.block_number),
        )
        .unwrap();
        let block_number_index = columns
            .iter()
            .position(|column| column.name == "block_number")
            .unwrap();
        columns[block_number_index] = legacy_block_number.clone();
        persist_segment_manifest_with_columns(&paths, &descriptor, columns).unwrap();

        let old_size = fs::metadata(segment_dir.join(&legacy_block_number.data_path))
            .unwrap()
            .len();
        compact_segment(&paths, &descriptor).unwrap();

        let columns = existing_columns(&paths, descriptor.id).unwrap().unwrap();
        let block_number = columns
            .iter()
            .find(|column| column.name == "block_number")
            .unwrap();
        assert_eq!(block_number.codec, CompressionCodec::DeltaZigZag);
        let new_size = fs::metadata(segment_dir.join(&block_number.data_path))
            .unwrap()
            .len();
        assert!(
            new_size.saturating_mul(10) < old_size,
            "old_size={old_size} new_size={new_size}"
        );

        let reread = SegmentReader::open(&segment_dir)
            .unwrap()
            .read_u64("block_number", None)
            .unwrap();
        assert_eq!(
            reread,
            rows.iter().map(|row| row.block_number).collect::<Vec<_>>()
        );
    }
}
