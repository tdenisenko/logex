//! Resolve the fixed logical column schema through one captured bundle table.
use std::fs;
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

use crate::bundle::{BundleReader, MAX_EXTENTS, MAX_ROWS};
use crate::native::{STORAGE_FORMAT_VERSION, SegmentKind, SegmentManifest};
use crate::page::{PAGE_INDEX_ENTRY_BYTES, frame_page_index};

pub(crate) const BUNDLE_PATH: &str = "columns/segment.bundle";
pub(crate) const COLUMN_NAMES: [&str; 14] = [
    "address",
    "block_number",
    "block_hash",
    "timestamp",
    "tx_hash",
    "tx_index",
    "log_index",
    "data_len",
    "source",
    "topic0",
    "topic1",
    "topic2",
    "topic3",
    "data",
];

pub(crate) fn stream_id(path: &str) -> io::Result<u8> {
    let path = path
        .strip_prefix("columns/")
        .ok_or_else(|| invalid("invalid bundle directory"))?;
    let (name, offset) = if let Some(name) = path.strip_suffix(".pages.idx") {
        (name, 14)
    } else if let Some(name) = path.strip_suffix(".pages") {
        (name, 0)
    } else if let Some(name) = path.strip_suffix(".null") {
        (name, 19)
    } else {
        return Err(invalid("unknown bundled column artifact"));
    };
    let id = COLUMN_NAMES
        .iter()
        .position(|candidate| *candidate == name)
        .ok_or_else(|| invalid("unknown bundled column"))?;
    if offset == 19 && !(9..13).contains(&id) {
        return Err(invalid("nonnullable bundled column"));
    }
    Ok(id as u8 + offset)
}

#[derive(Debug, Clone)]
pub(crate) struct ColumnArtifacts {
    dir: PathBuf,
    bundle: Option<BundleReader>,
}

impl ColumnArtifacts {
    pub(crate) fn open(dir: &Path, manifest: Option<&SegmentManifest>) -> io::Result<Self> {
        Self::open_inspected(dir, manifest, None)
    }

    pub(crate) fn open_inspected(
        dir: &Path,
        manifest: Option<&SegmentManifest>,
        inspected: Option<BundleReader>,
    ) -> io::Result<Self> {
        if inspected.is_some()
            && manifest
                .and_then(|manifest| manifest.column_bundle.as_ref())
                .is_none()
        {
            return Err(invalid("unexpected bundle snapshot for unbundled segment"));
        }
        let bundle = manifest
            .and_then(|manifest| {
                manifest
                    .column_bundle
                    .as_ref()
                    .map(|reference| (manifest, reference))
            })
            .map(|(manifest, reference)| {
                if manifest.format_version != STORAGE_FORMAT_VERSION
                    || manifest.kind != SegmentKind::Sealed
                    || manifest.row_count != reference.row_count
                    || manifest.canonical_rows_path != "canonical.bitmap"
                    || manifest.columns.len() != COLUMN_NAMES.len()
                {
                    return Err(invalid("invalid bundled segment identity or schema"));
                }
                for (name, codec) in crate::native::current_column_profile() {
                    let column = manifest
                        .columns
                        .iter()
                        .find(|column| column.name == *name)
                        .ok_or_else(|| invalid("missing bundled column"))?;
                    let topic = matches!(*name, "topic0" | "topic1" | "topic2" | "topic3");
                    if column.codec != *codec
                        || column.page_rows != 16_384
                        || column.data_path != format!("columns/{name}.pages")
                        || column.page_index_path != Some(format!("columns/{name}.pages.idx"))
                        || column.null_bitmap_path != topic.then(|| format!("columns/{name}.null"))
                    {
                        return Err(invalid("invalid bundled column descriptor"));
                    }
                }
                let reader = if let Some(reader) = inspected {
                    if reader.reference() != reference {
                        return Err(invalid("bundle reference changed after inspection"));
                    }
                    reader
                } else {
                    BundleReader::open(&dir.join(BUNDLE_PATH), reference)?
                };
                if !reader.has_complete_schema() {
                    return Err(invalid("incomplete bundled column streams"));
                }
                Ok(reader)
            })
            .transpose()?;
        Ok(Self {
            dir: dir.to_owned(),
            bundle,
        })
    }

    pub(crate) fn bundle(&self) -> Option<&BundleReader> {
        self.bundle.as_ref()
    }

    pub(crate) fn read(&self, path: &str) -> io::Result<Vec<u8>> {
        match &self.bundle {
            Some(bundle) => {
                let id = stream_id(path)?;
                let len = bundle.stream_len(id)?;
                match id {
                    14..28 => {
                        if len > (MAX_EXTENTS * PAGE_INDEX_ENTRY_BYTES) as u64
                            || !len.is_multiple_of(PAGE_INDEX_ENTRY_BYTES as u64)
                        {
                            return Err(invalid("bundled page index exceeds its bound"));
                        }
                        frame_page_index(&bundle.read_stream(id)?)
                    }
                    28..32 => {
                        let expected = bitmap_bytes(bundle.row_count())?;
                        if len > expected as u64 + 1 {
                            return Err(invalid("bundled bitmap exceeds its bound"));
                        }
                        decode_bitmap(&bundle.read_stream(id)?, bundle.row_count())
                    }
                    _ => bundle.read_stream(id),
                }
            }
            None => fs::read(self.dir.join(path)),
        }
    }

    pub(crate) fn len(&self, path: &str) -> io::Result<u64> {
        match &self.bundle {
            Some(bundle) => bundle.stream_len(stream_id(path)?),
            None => Ok(fs::metadata(self.dir.join(path))?.len()),
        }
    }

    pub(crate) fn read_bundle_range(
        &self,
        path: &str,
        range: Range<u64>,
    ) -> Option<io::Result<Vec<u8>>> {
        self.bundle
            .as_ref()
            .map(|bundle| stream_id(path).and_then(|id| bundle.read_range(id, range)))
    }

    pub(crate) fn verify_bundle(&self) -> io::Result<()> {
        self.bundle
            .as_ref()
            .map_or(Ok(()), BundleReader::verify_all)
    }
}

fn bitmap_bytes(rows: u64) -> io::Result<usize> {
    if rows > MAX_ROWS {
        return Err(invalid("bundled bitmap row count exceeds its bound"));
    }
    usize::try_from(8 + rows.div_ceil(8))
        .map_err(|_| invalid("bundled bitmap exceeds address space"))
}

pub(crate) fn encode_bitmap(bytes: &[u8], rows: u64) -> io::Result<Vec<u8>> {
    if bytes.len() != bitmap_bytes(rows)? || bytes[..8] != rows.to_le_bytes() {
        return Err(invalid("bundled bitmap row count mismatch"));
    }
    // The row count in the captured reference bounds decompression; no size
    // from the compressed payload is ever trusted for allocation.
    let compressed = lz4_flex::block::compress(bytes);
    let (tag, payload) = if compressed.len() < bytes.len() {
        (1, compressed.as_slice())
    } else {
        (0, bytes)
    };
    let mut encoded = Vec::with_capacity(1 + payload.len());
    encoded.push(tag);
    encoded.extend_from_slice(payload);
    Ok(encoded)
}

fn decode_bitmap(encoded: &[u8], rows: u64) -> io::Result<Vec<u8>> {
    let expected = bitmap_bytes(rows)?;
    if encoded.len() > expected + 1 {
        return Err(invalid("bundled bitmap exceeds its bound"));
    }
    let bytes = match encoded.split_first() {
        Some((0, bytes)) if bytes.len() == expected => bytes.to_vec(),
        Some((1, bytes)) => {
            let mut output = vec![0; expected];
            let len = lz4_flex::block::decompress_into(bytes, &mut output)
                .map_err(|_| invalid("invalid compressed bundled bitmap"))?;
            if len != expected {
                return Err(invalid("bundled bitmap length mismatch"));
            }
            output
        }
        _ => return Err(invalid("invalid bundled bitmap encoding")),
    };
    if bytes[..8] != rows.to_le_bytes() {
        return Err(invalid("bundled bitmap row count mismatch"));
    }
    Ok(bytes)
}

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::NullBitmap;

    #[test]
    fn bundled_bitmaps_roundtrip_sparse_dense_and_random_nulls() {
        let mut seed = 919u64;
        for rows in [0, 1, 7, 8, 9, 16_385, 100_000] {
            for mode in 0..3 {
                let mut bitmap = NullBitmap::new();
                for _ in 0..rows {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    bitmap.push(mode == 1 || (mode == 2 && seed >> 63 != 0));
                }
                let mut bytes = Vec::new();
                bitmap.write_to(&mut bytes).unwrap();
                let encoded = encode_bitmap(&bytes, rows).unwrap();
                assert!(encoded.len() <= bytes.len() + 1);
                assert_eq!(decode_bitmap(&encoded, rows).unwrap(), bytes);
                if rows >= 16_385 && mode != 2 {
                    assert!(encoded.len() < bytes.len() / 10);
                }
            }
        }
    }

    #[test]
    fn bundled_bitmaps_reject_malformed_encoding_and_unbounded_sizes() {
        let rows = 8192u64;
        let mut raw = vec![0; bitmap_bytes(rows).unwrap()];
        raw[..8].copy_from_slice(&rows.to_le_bytes());
        let compressed = encode_bitmap(&raw, rows).unwrap();
        assert_eq!(compressed[0], 1);
        for len in 0..compressed.len() {
            assert!(
                decode_bitmap(&compressed[..len], rows).is_err(),
                "length {len}"
            );
        }
        assert!(decode_bitmap(&compressed, rows + 1).is_err());
        assert!(decode_bitmap(&compressed, u64::MAX).is_err());
        assert!(decode_bitmap(&[0; 1034], rows).is_err());
        assert!(decode_bitmap(&[2], rows).is_err());
        // A syntactically valid compressed block must not overflow the output
        // allocation derived from the captured row count.
        let mut oversized = vec![1];
        oversized.extend(lz4_flex::block::compress(&vec![0; raw.len() * 2]));
        assert!(decode_bitmap(&oversized, rows).is_err());
        let mut undersized = vec![1];
        undersized.extend(lz4_flex::block::compress(&raw[..raw.len() - 1]));
        assert!(decode_bitmap(&undersized, rows).is_err());
        assert!(encode_bitmap(&raw, rows + 1).is_err());
        assert!(encode_bitmap(&[], 0).is_err());
        let mut plain = vec![0];
        plain.extend_from_slice(&raw);
        assert_eq!(decode_bitmap(&plain, rows).unwrap(), raw);
        plain[1] ^= 1;
        assert!(decode_bitmap(&plain, rows).is_err());
    }
}
