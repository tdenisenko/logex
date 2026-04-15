use std::fs;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use logex_types::LogRow;

use crate::column::ColumnFile;
use crate::page::{
    PageIndexEntry, encode_fixed_width_page, encode_u32_page, encode_u64_page, encode_u8_page,
    encode_var_bytes_page, write_page_index,
};
use crate::reader::ColumnReader;

use super::catalog::{
    ColumnDescriptor, CompressionCodec, IndexDescriptor, IndexKind, SegmentDescriptor,
    SegmentKind, SegmentManifest, StorageCatalogPaths,
};

const DEFAULT_PAGE_ROWS: u32 = 16_384;

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

    if segment_is_compacted(paths, descriptor.id)? {
        return persist_segment_manifest(paths, descriptor);
    }

    let segment_dir = paths.segment_dir(descriptor.id);
    fs::create_dir_all(segment_dir.join("columns"))?;

    let mut columns = Vec::with_capacity(14);
    columns.push(compact_address_column(&segment_dir)?);
    columns.push(compact_u64_column(
        &segment_dir,
        "block_number",
        CompressionCodec::Delta,
    )?);
    columns.push(compact_b256_column(
        &segment_dir,
        "block_hash",
        CompressionCodec::None,
    )?);
    columns.push(compact_u64_column(
        &segment_dir,
        "timestamp",
        CompressionCodec::DeltaOfDelta,
    )?);
    columns.push(compact_b256_column(
        &segment_dir,
        "tx_hash",
        CompressionCodec::None,
    )?);
    columns.push(compact_u32_column(
        &segment_dir,
        "tx_index",
        CompressionCodec::Zstd,
    )?);
    columns.push(compact_u32_column(
        &segment_dir,
        "log_index",
        CompressionCodec::Zstd,
    )?);
    columns.push(compact_u32_column(
        &segment_dir,
        "data_len",
        CompressionCodec::Zstd,
    )?);
    columns.push(compact_u8_column(
        &segment_dir,
        "source",
        CompressionCodec::Dictionary,
    )?);
    columns.push(compact_nullable_b256_column(
        &segment_dir,
        "topic0",
        CompressionCodec::Dictionary,
    )?);
    columns.push(compact_nullable_b256_column(
        &segment_dir,
        "topic1",
        CompressionCodec::Zstd,
    )?);
    columns.push(compact_nullable_b256_column(
        &segment_dir,
        "topic2",
        CompressionCodec::Zstd,
    )?);
    columns.push(compact_nullable_b256_column(
        &segment_dir,
        "topic3",
        CompressionCodec::Zstd,
    )?);
    columns.push(compact_data_column(&segment_dir, CompressionCodec::Lz4)?);

    persist_segment_manifest_with_columns(paths, descriptor, columns)?;
    remove_raw_hot_files(&segment_dir)?;

    tracing::info!(
        segment_id = descriptor.id,
        row_count = descriptor.row_count,
        "compacted sealed storage segment"
    );

    Ok(())
}

fn segment_is_compacted(paths: &StorageCatalogPaths, segment_id: u64) -> std::io::Result<bool> {
    Ok(existing_columns(paths, segment_id)?
        .map(|columns| columns.iter().all(|column| column.page_index_path.is_some()))
        .unwrap_or(false))
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
    let mut raw = Vec::with_capacity(values.len() * 20);
    for value in values {
        raw.extend_from_slice(value.as_slice());
    }
    write_fixed_width_pages(
        segment_dir,
        "address",
        CompressionCodec::Dictionary,
        raw.len() / 20,
        |range| encode_fixed_width_page(&raw[range.start * 20..range.end * 20], 20, CompressionCodec::Dictionary),
    )
}

fn compact_b256_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_b256(segment_dir, &format!("{name}.col"), None)?;
    let mut raw = Vec::with_capacity(values.len() * 32);
    for value in values {
        raw.extend_from_slice(value.as_slice());
    }
    write_fixed_width_pages(segment_dir, name, codec, raw.len() / 32, |range| {
        encode_fixed_width_page(&raw[range.start * 32..range.end * 32], 32, codec)
    })
}

fn compact_nullable_b256_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_nullable_b256(segment_dir, name, None)?;
    let mut raw = Vec::with_capacity(values.len() * 32);
    for value in values {
        match value {
            Some(value) => raw.extend_from_slice(value.as_slice()),
            None => raw.extend_from_slice(&[0u8; 32]),
        }
    }

    let mut descriptor =
        write_fixed_width_pages(segment_dir, name, codec, raw.len() / 32, |range| {
            encode_fixed_width_page(&raw[range.start * 32..range.end * 32], 32, codec)
        })?;

    let null_rel = format!("columns/{name}.null");
    fs::copy(segment_dir.join(format!("{name}.null")), segment_dir.join(&null_rel))?;
    descriptor.null_bitmap_path = Some(null_rel);
    Ok(descriptor)
}

fn compact_u64_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u64(segment_dir, &format!("{name}.col"), None)?;
    write_typed_pages(segment_dir, name, codec, &values, |slice| encode_u64_page(slice, codec))
}

fn compact_u32_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u32(segment_dir, &format!("{name}.col"), None)?;
    write_typed_pages(segment_dir, name, codec, &values, |slice| encode_u32_page(slice, codec))
}

fn compact_u8_column(
    segment_dir: &Path,
    name: &str,
    codec: CompressionCodec,
) -> std::io::Result<ColumnDescriptor> {
    let values = ColumnReader::read_u8(segment_dir, &format!("{name}.col"), None)?;
    write_typed_pages(segment_dir, name, codec, &values, |slice| encode_u8_page(slice, codec))
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
        write_pages(segment_dir, name, row_count, |range| encode_page(range))?;
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
    let (data_path, index_path, page_rows) = write_pages(segment_dir, name, values.len(), |range| {
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
