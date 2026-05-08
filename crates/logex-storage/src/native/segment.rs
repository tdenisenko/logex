use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::thread;

use alloy_primitives::{Address, B256};
use logex_types::LogRow;

use crate::column::{ColumnFile, NullBitmap};
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
                CompressionCodec::Delta,
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
    let segment_dir = paths.segment_dir(descriptor.id);
    fs::create_dir_all(&segment_dir)?;

    let manifest = SegmentManifest {
        format_version: super::catalog::STORAGE_FORMAT_VERSION,
        segment_id: descriptor.id,
        generation: descriptor.generation,
        kind: descriptor.kind,
        min_block: descriptor.min_block,
        max_block: descriptor.max_block,
        row_count: descriptor.row_count,
        canonical_rows_path: "canonical.bitmap".to_owned(),
        columns,
        indexes: collect_indexes(&segment_dir)?,
    };

    let path = paths.segment_manifest_path(descriptor.id);
    let tmp = path.with_extension("json.tmp");
    let json = serde_json::to_vec_pretty(&manifest).map_err(std::io::Error::other)?;
    fs::write(&tmp, json)?;
    fs::rename(tmp, path)?;
    Ok(())
}

pub(crate) fn compact_segment(
    paths: &StorageCatalogPaths,
    descriptor: &SegmentDescriptor,
) -> std::io::Result<()> {
    if descriptor.kind != SegmentKind::Sealed || descriptor.row_count == 0 {
        return persist_segment_manifest(paths, descriptor);
    }

    if segment_uses_current_compaction_profile(paths, descriptor.id)? {
        return persist_segment_manifest(paths, descriptor);
    }

    if segment_is_compacted(paths, descriptor.id)? {
        return recompact_segment(paths, descriptor);
    }

    let segment_dir = paths.segment_dir(descriptor.id);
    fs::create_dir_all(segment_dir.join("columns"))?;

    let columns = vec![
        compact_address_column(&segment_dir)?,
        compact_u64_column(&segment_dir, "block_number", CompressionCodec::Delta)?,
        compact_b256_column(&segment_dir, "block_hash", CompressionCodec::AdaptiveFixed)?,
        compact_u64_column(&segment_dir, "timestamp", CompressionCodec::DeltaOfDelta)?,
        compact_b256_column(&segment_dir, "tx_hash", CompressionCodec::AdaptiveFixed)?,
        compact_u32_column(&segment_dir, "tx_index", CompressionCodec::Zstd)?,
        compact_u32_column(&segment_dir, "log_index", CompressionCodec::Zstd)?,
        compact_u32_column(&segment_dir, "data_len", CompressionCodec::Zstd)?,
        compact_u8_column(&segment_dir, "source", CompressionCodec::Dictionary)?,
        compact_nullable_b256_column(&segment_dir, "topic0", CompressionCodec::AdaptiveFixed)?,
        compact_nullable_b256_column(&segment_dir, "topic1", CompressionCodec::AdaptiveFixed)?,
        compact_nullable_b256_column(&segment_dir, "topic2", CompressionCodec::AdaptiveFixed)?,
        compact_nullable_b256_column(&segment_dir, "topic3", CompressionCodec::AdaptiveFixed)?,
        compact_data_column(&segment_dir, CompressionCodec::AdaptiveBytes)?,
    ];

    persist_segment_manifest_with_columns(paths, descriptor, columns)?;
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
        ("block_number", CompressionCodec::Delta),
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
    for name in [
        "address.col",
        "block_number.col",
        "block_hash.col",
        "timestamp.col",
        "tx_hash.col",
        "tx_index.col",
        "log_index.col",
        "data.col",
        "data_len.col",
        "source.col",
        "topic0.col",
        "topic1.col",
        "topic2.col",
        "topic3.col",
        "topic0.null",
        "topic1.null",
        "topic2.null",
        "topic3.null",
    ] {
        let path = segment_dir.join(name);
        if path.exists() {
            fs::remove_file(path)?;
        }
    }
    Ok(())
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
                "address_topic0.bptree" => IndexKind::AddressTopic0,
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
