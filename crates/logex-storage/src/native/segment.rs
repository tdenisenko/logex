use crate::durability;
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
    encode_var_bytes_page, write_page_index,
};
use crate::reader::ColumnReader;
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

pub(crate) fn append_rows(
    segment_dir: &Path,
    existing_rows: u64,
    rows: &[LogRow],
) -> std::io::Result<()> {
    if existing_rows == 0 {
        ColumnFile::write_batch(segment_dir, rows)
    } else {
        ColumnFile::append_batch(segment_dir, rows, existing_rows)
    }
}

pub(crate) fn write_compacted_rows(
    segment_dir: &Path,
    rows: &[LogRow],
) -> std::io::Result<Vec<ColumnDescriptor>> {
    fs::create_dir_all(segment_dir)?;
    fs::create_dir_all(segment_dir.join("columns"))?;
    ColumnFile::write_canonical_bitmap(segment_dir, rows.len() as u64)?;

    thread::scope(|scope| {
        let address =
            scope.spawn(|| compact_address_values(segment_dir, rows.iter().map(|row| row.address)));
        let block_number = scope.spawn(|| {
            compact_u64_values(
                segment_dir,
                "block_number",
                CompressionCodec::DeltaZigZag,
                rows.iter().map(|row| row.block_number),
            )
        });
        let block_hash = scope.spawn(|| {
            compact_b256_values(
                segment_dir,
                "block_hash",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.block_hash),
            )
        });
        let timestamp = scope.spawn(|| {
            compact_u64_values(
                segment_dir,
                "timestamp",
                CompressionCodec::DeltaOfDelta,
                rows.iter().map(|row| row.timestamp),
            )
        });
        let tx_hash = scope.spawn(|| {
            compact_b256_values(
                segment_dir,
                "tx_hash",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.tx_hash),
            )
        });
        let tx_index = scope.spawn(|| {
            compact_u32_values(
                segment_dir,
                "tx_index",
                CompressionCodec::Zstd,
                rows.iter().map(|row| row.tx_index),
            )
        });
        let log_index = scope.spawn(|| {
            compact_u32_values(
                segment_dir,
                "log_index",
                CompressionCodec::Zstd,
                rows.iter().map(|row| row.log_index),
            )
        });
        let data_len = scope.spawn(|| {
            compact_u32_values(
                segment_dir,
                "data_len",
                CompressionCodec::Zstd,
                rows.iter().map(|row| row.data_len),
            )
        });
        let source = scope.spawn(|| {
            compact_u8_values(
                segment_dir,
                "source",
                CompressionCodec::Dictionary,
                rows.iter().map(|row| row.source as u8),
            )
        });
        let topic0 = scope.spawn(|| {
            compact_nullable_b256_values(
                segment_dir,
                "topic0",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic0),
            )
        });
        let topic1 = scope.spawn(|| {
            compact_nullable_b256_values(
                segment_dir,
                "topic1",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic1),
            )
        });
        let topic2 = scope.spawn(|| {
            compact_nullable_b256_values(
                segment_dir,
                "topic2",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic2),
            )
        });
        let topic3 = scope.spawn(|| {
            compact_nullable_b256_values(
                segment_dir,
                "topic3",
                CompressionCodec::AdaptiveFixed,
                rows.iter().map(|row| row.topic3),
            )
        });
        let data = scope.spawn(|| compact_data_values(segment_dir, rows));

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
    persist_manifest(paths, descriptor, columns, false)
}

pub(crate) fn persist_ingest_manifest(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    ordered: bool,
) -> std::io::Result<()> {
    let columns = existing_columns(paths, descriptor.id)?.unwrap_or_else(default_columns);
    persist_manifest(paths, descriptor, columns, ordered)
}

pub(crate) fn persist_ingest_manifest_with_columns(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    columns: Vec<ColumnDescriptor>,
    ordered: bool,
) -> std::io::Result<()> {
    persist_manifest(paths, descriptor, columns, ordered)
}

fn persist_manifest(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    columns: Vec<ColumnDescriptor>,
    ordered: bool,
) -> std::io::Result<()> {
    let segment_dir = paths.segment_dir(descriptor.id);
    fs::create_dir_all(&segment_dir)?;

    let manifest = SegmentManifest {
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
    if ordered {
        durability::publish_tree_ordered(&segment_dir, &path, &json, &paths.catalog_path())
    } else {
        durability::publish_tree(&segment_dir, &path, &json)
    }
}

pub(crate) fn compact_segment(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> std::io::Result<()> {
    compact_ingest_segment(paths, descriptor, false)
}

pub(crate) fn compact_ingest_segment(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
    ordered: bool,
) -> std::io::Result<()> {
    if descriptor.kind != SegmentKind::Sealed || descriptor.row_count == 0 {
        return persist_ingest_manifest(paths, descriptor, ordered);
    }

    if segment_uses_current_compaction_profile(paths, descriptor.id)? {
        return persist_ingest_manifest(paths, descriptor, ordered);
    }

    if segment_is_compacted(paths, descriptor.id)? {
        return recompact_segment(paths, descriptor);
    }

    let segment_dir = paths.segment_dir(descriptor.id);
    fs::create_dir_all(segment_dir.join("columns"))?;
    verify_raw_segment_files_complete(descriptor, &segment_dir)?;

    let columns = thread::scope(|scope| {
        let address = scope.spawn(|| compact_address_column(&segment_dir));
        let block_number = scope.spawn(|| {
            compact_u64_column(&segment_dir, "block_number", CompressionCodec::DeltaZigZag)
        });
        let block_hash = scope.spawn(|| {
            compact_b256_column(&segment_dir, "block_hash", CompressionCodec::AdaptiveFixed)
        });
        let timestamp = scope.spawn(|| {
            compact_u64_column(&segment_dir, "timestamp", CompressionCodec::DeltaOfDelta)
        });
        let tx_hash = scope.spawn(|| {
            compact_b256_column(&segment_dir, "tx_hash", CompressionCodec::AdaptiveFixed)
        });
        let tx_index =
            scope.spawn(|| compact_u32_column(&segment_dir, "tx_index", CompressionCodec::Zstd));
        let log_index =
            scope.spawn(|| compact_u32_column(&segment_dir, "log_index", CompressionCodec::Zstd));
        let data_len =
            scope.spawn(|| compact_u32_column(&segment_dir, "data_len", CompressionCodec::Zstd));
        let source =
            scope.spawn(|| compact_u8_column(&segment_dir, "source", CompressionCodec::Dictionary));
        let topic0 = scope.spawn(|| {
            compact_nullable_b256_column(&segment_dir, "topic0", CompressionCodec::AdaptiveFixed)
        });
        let topic1 = scope.spawn(|| {
            compact_nullable_b256_column(&segment_dir, "topic1", CompressionCodec::AdaptiveFixed)
        });
        let topic2 = scope.spawn(|| {
            compact_nullable_b256_column(&segment_dir, "topic2", CompressionCodec::AdaptiveFixed)
        });
        let topic3 = scope.spawn(|| {
            compact_nullable_b256_column(&segment_dir, "topic3", CompressionCodec::AdaptiveFixed)
        });
        let data =
            scope.spawn(|| compact_data_column(&segment_dir, CompressionCodec::AdaptiveBytes));
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

    persist_ingest_manifest_with_columns(paths, descriptor, columns, ordered)?;
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
    let mut block_number_column = compact_u64_values(
        &tmp_dir,
        "block_number",
        CompressionCodec::DeltaZigZag,
        values,
    )?;
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

fn current_column_profile() -> &'static [(&'static str, CompressionCodec)] {
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

fn default_columns() -> Vec<ColumnDescriptor> {
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

fn compact_address_column(segment_dir: &Path) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_address(segment_dir, None)?;
    compact_address_values(segment_dir, values)
}

fn compact_address_values(
    segment_dir: &Path,
    values: impl IntoIterator<Item = Address>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    let mut raw = Vec::with_capacity(values.len() * 20);
    for value in values {
        raw.extend_from_slice(value.as_slice());
    }
    write_fixed_width_pages(
        segment_dir,
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
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = B256>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    let mut raw = Vec::with_capacity(values.len() * 32);
    for value in values {
        raw.extend_from_slice(value.as_slice());
    }
    write_fixed_width_pages(segment_dir, name, codec, raw.len() / 32, |range| {
        encode_fixed_width_page(&raw[range.start * 32..range.end * 32], 32, codec)
    })
}

fn compact_b256_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_b256(segment_dir, &format!("{name}.col"), None)?;
    compact_b256_values(segment_dir, name, codec, values)
}

fn compact_nullable_b256_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_nullable_b256(segment_dir, name, None)?;
    compact_nullable_b256_values(segment_dir, name, codec, values)
}

fn compact_nullable_b256_values(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = Option<B256>>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    let mut raw = Vec::with_capacity(values.len() * 32);
    let mut nulls = NullBitmap::new();
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

    let mut descriptor =
        write_fixed_width_pages(segment_dir, name, codec, raw.len() / 32, |range| {
            encode_fixed_width_page(&raw[range.start * 32..range.end * 32], 32, codec)
        })?;

    let null_rel = format!("columns/{name}.null");
    let null_file = File::create(segment_dir.join(&null_rel))?;
    let mut null_writer = BufWriter::new(null_file);
    nulls.write_to(&mut null_writer)?;
    null_writer.flush()?;
    descriptor.null_bitmap_path = Some(null_rel);
    Ok(descriptor)
}

fn compact_u64_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u64(segment_dir, &format!("{name}.col"), None)?;
    compact_u64_values(segment_dir, name, codec, values)
}

fn compact_u64_values(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = u64>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    write_typed_pages(segment_dir, name, codec, &values, |slice| {
        encode_u64_page(slice, codec)
    })
}

fn compact_u32_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u32(segment_dir, &format!("{name}.col"), None)?;
    compact_u32_values(segment_dir, name, codec, values)
}

fn compact_u32_values(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = u32>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    write_typed_pages(segment_dir, name, codec, &values, |slice| {
        encode_u32_page(slice, codec)
    })
}

fn compact_u8_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u8(segment_dir, &format!("{name}.col"), None)?;
    compact_u8_values(segment_dir, name, codec, values)
}

fn compact_u8_values(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
    values: impl IntoIterator<Item = u8>,
) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = values.into_iter().collect();
    write_typed_pages(segment_dir, name, codec, &values, |slice| {
        encode_u8_page(slice, codec)
    })
}

fn compact_data_column(
    segment_dir: &Path,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_var_bytes(segment_dir, "data.col", None)?;
    write_typed_pages(segment_dir, "data", codec, &values, |slice| {
        encode_var_bytes_page(slice, codec)
    })
}

fn compact_data_values(segment_dir: &Path, rows: &[LogRow]) -> std::io::Result<ColumnDescriptor> {
    let values: Vec<_> = rows.iter().map(|row| row.data.clone()).collect();
    write_typed_pages(
        segment_dir,
        "data",
        CompressionCodec::AdaptiveBytes,
        &values,
        |slice| encode_var_bytes_page(slice, CompressionCodec::AdaptiveBytes),
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

fn write_fixed_width_pages<F>(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
    row_count: usize,
    mut encode_page: F,
) -> std::io::Result<ColumnDescriptor>
where
    F: FnMut(Range<usize>) -> std::io::Result<Vec<u8>>,
{
    let (data_path, index_path, page_rows) =
        write_pages(segment_dir, name, row_count, &mut encode_page)?;
    Ok(ColumnDescriptor {
        name: name.to_owned(),
        codec,
        page_rows,
        data_path,
        null_bitmap_path: None,
        page_index_path: Some(index_path),
    })
}

fn write_typed_pages<T, F>(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
    values: &[T],
    mut encode_page: F,
) -> std::io::Result<ColumnDescriptor>
where
    T: Clone,
    F: FnMut(&[T]) -> std::io::Result<Vec<u8>>,
{
    let (data_path, index_path, page_rows) =
        write_pages(segment_dir, name, values.len(), |range| {
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
    segment_dir: &Path,
    name: &str,
    row_count: usize,
    mut encode_page: F,
) -> std::io::Result<(String, String, u32)>
where
    F: FnMut(Range<usize>) -> std::io::Result<Vec<u8>>,
{
    let page_rows = DEFAULT_PAGE_ROWS;
    let data_rel = format!("columns/{name}.pages");
    let index_rel = format!("columns/{name}.pages.idx");
    let data_path = segment_dir.join(&data_rel);
    let index_path = segment_dir.join(&index_rel);
    let mut writer = BufWriter::new(File::create(&data_path)?);
    let mut entries = Vec::new();
    let mut offset = 0u64;

    let mut start = 0usize;
    while start < row_count {
        let end = (start + page_rows as usize).min(row_count);
        let encoded = encode_page(start..end)?;
        writer.write_all(&encoded)?;
        entries.push(PageIndexEntry {
            first_row: start as u64,
            row_count: (end - start) as u32,
            offset,
            encoded_len: encoded.len() as u32,
        });
        offset += encoded.len() as u64;
        start = end;
    }
    writer.flush()?;

    fs::write(index_path, write_page_index(&entries))?;
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
    fn recompact_segment_rewrites_only_legacy_block_number_profile() {
        let tmp = TempDir::new().unwrap();
        let paths = StorageCatalogPaths::new(tmp.path().to_path_buf());
        paths.ensure_base_dirs().unwrap();
        let descriptor = SegmentDescriptor {
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
            &segment_dir,
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
