use std::io;

use alloy_primitives::Bytes;

use crate::compression::{
    delta_decode, delta_encode, delta_of_delta_decode, delta_of_delta_encode, dict_decode,
    dict_encode_raw, lz4_compress, lz4_decompress, zstd_compress, zstd_compress_level,
    zstd_decompress,
};
use crate::native::CompressionCodec;

const PAGE_INDEX_MAGIC: &[u8; 4] = b"LXPI";
const PAGE_INDEX_VERSION: u32 = 1;
const ADAPTIVE_FIXED_NONE: u8 = 0;
const ADAPTIVE_FIXED_DICTIONARY: u8 = 1;
const ADAPTIVE_FIXED_ZSTD: u8 = 2;
const ADAPTIVE_FIXED_ZSTD_B256_LOW20: u8 = 3;
const ADAPTIVE_BYTES_ZSTD_U64_OFFSETS: u8 = 0;
const ADAPTIVE_BYTES_ZSTD_U32_OFFSETS: u8 = 1;
const ZSTD_STORAGE_LEVEL: i32 = 1;
const DICTIONARY_FAST_PATH_NUMERATOR: usize = 3;
const DICTIONARY_FAST_PATH_DENOMINATOR: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageIndexEntry {
    pub first_row: u64,
    pub row_count: u32,
    pub offset: u64,
    pub encoded_len: u32,
}

pub fn write_page_index(entries: &[PageIndexEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + entries.len() * 24);
    out.extend_from_slice(PAGE_INDEX_MAGIC);
    out.extend_from_slice(&PAGE_INDEX_VERSION.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.first_row.to_le_bytes());
        out.extend_from_slice(&entry.row_count.to_le_bytes());
        out.extend_from_slice(&entry.offset.to_le_bytes());
        out.extend_from_slice(&entry.encoded_len.to_le_bytes());
    }
    out
}

pub fn read_page_index(data: &[u8]) -> io::Result<Vec<PageIndexEntry>> {
    if data.len() < 12 || &data[..4] != PAGE_INDEX_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid page index header",
        ));
    }

    let version = u32::from_le_bytes(
        data[4..8]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated page index"))?,
    );
    if version != PAGE_INDEX_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported page index version: {version}"),
        ));
    }

    let count = u32::from_le_bytes(
        data[8..12]
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "truncated page index"))?,
    ) as usize;

    let expected = 12 + count * 24;
    if data.len() != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "page index length mismatch",
        ));
    }

    let mut entries = Vec::with_capacity(count);
    let mut cursor = 12;
    for _ in 0..count {
        let first_row = u64::from_le_bytes(data[cursor..cursor + 8].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated page index entry")
        })?);
        cursor += 8;

        let row_count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated page index entry")
        })?);
        cursor += 4;

        let offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "truncated page index entry")
        })?);
        cursor += 8;

        let encoded_len =
            u32::from_le_bytes(data[cursor..cursor + 4].try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated page index entry")
            })?);
        cursor += 4;

        entries.push(PageIndexEntry {
            first_row,
            row_count,
            offset,
            encoded_len,
        });
    }

    Ok(entries)
}

pub fn encode_fixed_width_page(
    raw_values: &[u8],
    item_size: usize,
    codec: CompressionCodec,
) -> io::Result<Vec<u8>> {
    if item_size == 0 || !raw_values.len().is_multiple_of(item_size) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fixed-width page has invalid item size",
        ));
    }

    match codec {
        CompressionCodec::None => Ok(raw_values.to_vec()),
        CompressionCodec::Dictionary => Ok(dict_encode_raw(raw_values, item_size)),
        CompressionCodec::Zstd => zstd_compress(raw_values),
        CompressionCodec::Lz4 => Ok(lz4_compress(raw_values)),
        CompressionCodec::AdaptiveFixed => encode_adaptive_fixed_width_page(raw_values, item_size),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported fixed-width codec: {other:?}"),
        )),
    }
}

pub fn decode_fixed_width_page(
    encoded: &[u8],
    row_count: usize,
    item_size: usize,
    codec: CompressionCodec,
) -> io::Result<Vec<u8>> {
    let raw = match codec {
        CompressionCodec::None => encoded.to_vec(),
        CompressionCodec::Dictionary => dict_decode(encoded, row_count, item_size)?,
        CompressionCodec::Zstd => zstd_decompress(encoded)?,
        CompressionCodec::Lz4 => lz4_decompress(encoded)?,
        CompressionCodec::AdaptiveFixed => {
            decode_adaptive_fixed_width_page(encoded, row_count, item_size)?
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported fixed-width codec: {other:?}"),
            ));
        }
    };

    if raw.len() != row_count * item_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded fixed-width page has unexpected length",
        ));
    }

    Ok(raw)
}

fn encode_adaptive_fixed_width_page(raw_values: &[u8], item_size: usize) -> io::Result<Vec<u8>> {
    let dictionary = dict_encode_raw(raw_values, item_size);
    if dictionary
        .len()
        .saturating_mul(DICTIONARY_FAST_PATH_DENOMINATOR)
        <= raw_values
            .len()
            .saturating_mul(DICTIONARY_FAST_PATH_NUMERATOR)
    {
        let mut out = Vec::with_capacity(dictionary.len() + 1);
        out.push(ADAPTIVE_FIXED_DICTIONARY);
        out.extend_from_slice(&dictionary);
        return Ok(out);
    }

    let zstd = zstd_compress_level(raw_values, ZSTD_STORAGE_LEVEL)?;

    let mut candidates = Vec::with_capacity(4);
    candidates.push((ADAPTIVE_FIXED_NONE, raw_values));
    candidates.push((ADAPTIVE_FIXED_DICTIONARY, dictionary.as_slice()));
    candidates.push((ADAPTIVE_FIXED_ZSTD, zstd.as_slice()));

    let low20;
    if item_size == 32
        && raw_values
            .chunks_exact(32)
            .all(|chunk| chunk[..12] == [0; 12])
    {
        let mut tails = Vec::with_capacity(raw_values.len() / 32 * 20);
        for value in raw_values.chunks_exact(32) {
            tails.extend_from_slice(&value[12..]);
        }
        low20 = zstd_compress_level(&tails, ZSTD_STORAGE_LEVEL)?;
        candidates.push((ADAPTIVE_FIXED_ZSTD_B256_LOW20, low20.as_slice()));
    }

    candidates.sort_by_key(|(_, data)| data.len());

    let (tag, payload) = candidates[0];
    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(tag);
    out.extend_from_slice(payload);
    Ok(out)
}

fn decode_adaptive_fixed_width_page(
    encoded: &[u8],
    row_count: usize,
    item_size: usize,
) -> io::Result<Vec<u8>> {
    let (tag, payload) = encoded
        .split_first()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "adaptive page is empty"))?;

    match *tag {
        ADAPTIVE_FIXED_NONE => Ok(payload.to_vec()),
        ADAPTIVE_FIXED_DICTIONARY => dict_decode(payload, row_count, item_size),
        ADAPTIVE_FIXED_ZSTD => zstd_decompress(payload),
        ADAPTIVE_FIXED_ZSTD_B256_LOW20 => decode_b256_low20_page(payload, row_count, item_size),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported adaptive fixed-width tag: {other}"),
        )),
    }
}

fn decode_b256_low20_page(
    payload: &[u8],
    row_count: usize,
    item_size: usize,
) -> io::Result<Vec<u8>> {
    if item_size != 32 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "low20 adaptive page can only decode 32-byte values",
        ));
    }

    let tails = zstd_decompress(payload)?;
    if tails.len() != row_count * 20 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "low20 adaptive page has unexpected length",
        ));
    }

    let mut out = Vec::with_capacity(row_count * 32);
    for tail in tails.chunks_exact(20) {
        out.extend_from_slice(&[0u8; 12]);
        out.extend_from_slice(tail);
    }
    Ok(out)
}

pub fn encode_u64_page(values: &[u64], codec: CompressionCodec) -> io::Result<Vec<u8>> {
    let mut raw = Vec::with_capacity(values.len() * 8);
    for value in values {
        raw.extend_from_slice(&value.to_le_bytes());
    }

    match codec {
        CompressionCodec::None => Ok(raw),
        CompressionCodec::Delta => Ok(delta_encode(values)),
        CompressionCodec::DeltaOfDelta => Ok(delta_of_delta_encode(values)),
        CompressionCodec::Zstd => zstd_compress(&raw),
        CompressionCodec::Lz4 => Ok(lz4_compress(&raw)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported u64 codec: {other:?}"),
        )),
    }
}

pub fn decode_u64_page(
    encoded: &[u8],
    row_count: usize,
    codec: CompressionCodec,
) -> io::Result<Vec<u64>> {
    match codec {
        CompressionCodec::None => decode_plain_u64_page(encoded, row_count),
        CompressionCodec::Delta => delta_decode(encoded, row_count),
        CompressionCodec::DeltaOfDelta => delta_of_delta_decode(encoded, row_count),
        CompressionCodec::Zstd => decode_plain_u64_page(&zstd_decompress(encoded)?, row_count),
        CompressionCodec::Lz4 => decode_plain_u64_page(&lz4_decompress(encoded)?, row_count),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported u64 codec: {other:?}"),
        )),
    }
}

pub fn encode_u32_page(values: &[u32], codec: CompressionCodec) -> io::Result<Vec<u8>> {
    let mut raw = Vec::with_capacity(values.len() * 4);
    for value in values {
        raw.extend_from_slice(&value.to_le_bytes());
    }

    match codec {
        CompressionCodec::None => Ok(raw),
        CompressionCodec::Zstd => zstd_compress(&raw),
        CompressionCodec::Lz4 => Ok(lz4_compress(&raw)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported u32 codec: {other:?}"),
        )),
    }
}

pub fn decode_u32_page(
    encoded: &[u8],
    row_count: usize,
    codec: CompressionCodec,
) -> io::Result<Vec<u32>> {
    let raw = match codec {
        CompressionCodec::None => encoded.to_vec(),
        CompressionCodec::Zstd => zstd_decompress(encoded)?,
        CompressionCodec::Lz4 => lz4_decompress(encoded)?,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported u32 codec: {other:?}"),
            ));
        }
    };
    decode_plain_u32_page(&raw, row_count)
}

pub fn encode_u8_page(values: &[u8], codec: CompressionCodec) -> io::Result<Vec<u8>> {
    match codec {
        CompressionCodec::None => Ok(values.to_vec()),
        CompressionCodec::Dictionary => Ok(dict_encode_raw(values, 1)),
        CompressionCodec::Zstd => zstd_compress(values),
        CompressionCodec::Lz4 => Ok(lz4_compress(values)),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported u8 codec: {other:?}"),
        )),
    }
}

pub fn decode_u8_page(
    encoded: &[u8],
    row_count: usize,
    codec: CompressionCodec,
) -> io::Result<Vec<u8>> {
    let raw = match codec {
        CompressionCodec::None => encoded.to_vec(),
        CompressionCodec::Dictionary => dict_decode(encoded, row_count, 1)?,
        CompressionCodec::Zstd => zstd_decompress(encoded)?,
        CompressionCodec::Lz4 => lz4_decompress(encoded)?,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported u8 codec: {other:?}"),
            ));
        }
    };

    if raw.len() != row_count {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded u8 page has unexpected length",
        ));
    }

    Ok(raw)
}

pub fn encode_var_bytes_page(values: &[Bytes], codec: CompressionCodec) -> io::Result<Vec<u8>> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(values.len() as u32).to_le_bytes());

    let mut offset = 0u64;
    raw.extend_from_slice(&offset.to_le_bytes());
    for value in values {
        offset += value.len() as u64;
        raw.extend_from_slice(&offset.to_le_bytes());
    }
    for value in values {
        raw.extend_from_slice(value);
    }

    match codec {
        CompressionCodec::None => Ok(raw),
        CompressionCodec::Zstd => zstd_compress(&raw),
        CompressionCodec::Lz4 => Ok(lz4_compress(&raw)),
        CompressionCodec::AdaptiveBytes => encode_adaptive_var_bytes_page(values),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported bytes codec: {other:?}"),
        )),
    }
}

pub fn decode_var_bytes_page(encoded: &[u8], codec: CompressionCodec) -> io::Result<Vec<Bytes>> {
    let raw = match codec {
        CompressionCodec::None => encoded.to_vec(),
        CompressionCodec::Zstd => zstd_decompress(encoded)?,
        CompressionCodec::Lz4 => lz4_decompress(encoded)?,
        CompressionCodec::AdaptiveBytes => return decode_adaptive_var_bytes_page(encoded),
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("unsupported bytes codec: {other:?}"),
            ));
        }
    };

    if raw.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page is truncated",
        ));
    }

    let row_count = u32::from_le_bytes(raw[..4].try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page row-count is truncated",
        )
    })?) as usize;
    let offsets_start = 4;
    let offsets_len = (row_count + 1) * 8;
    let blob_start = offsets_start + offsets_len;
    if raw.len() < blob_start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page offsets are truncated",
        ));
    }

    let mut offsets = Vec::with_capacity(row_count + 1);
    for index in 0..=row_count {
        let start = offsets_start + index * 8;
        let end = start + 8;
        offsets.push(u64::from_le_bytes(raw[start..end].try_into().map_err(
            |_| io::Error::new(io::ErrorKind::InvalidData, "bytes page offset is truncated"),
        )?));
    }

    let blob = &raw[blob_start..];
    let mut values = Vec::with_capacity(row_count);
    for index in 0..row_count {
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        if end < start || end > blob.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bytes page offset is out of bounds",
            ));
        }
        values.push(Bytes::copy_from_slice(&blob[start..end]));
    }
    Ok(values)
}

fn encode_adaptive_var_bytes_page(values: &[Bytes]) -> io::Result<Vec<u8>> {
    let plain_u64 = encode_var_bytes_raw_u64(values);
    let compressed_u64 = zstd_compress_level(&plain_u64, ZSTD_STORAGE_LEVEL)?;

    let total_len = values
        .iter()
        .try_fold(0u64, |acc, value| acc.checked_add(value.len() as u64))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bytes page is too large"))?;

    if total_len > u32::MAX as u64 {
        let mut out = Vec::with_capacity(compressed_u64.len() + 1);
        out.push(ADAPTIVE_BYTES_ZSTD_U64_OFFSETS);
        out.extend_from_slice(&compressed_u64);
        return Ok(out);
    }

    let plain_u32 = encode_var_bytes_raw_u32(values);
    let compressed_u32 = zstd_compress_level(&plain_u32, ZSTD_STORAGE_LEVEL)?;
    let (tag, payload) = if compressed_u32.len() < compressed_u64.len() {
        (ADAPTIVE_BYTES_ZSTD_U32_OFFSETS, compressed_u32.as_slice())
    } else {
        (ADAPTIVE_BYTES_ZSTD_U64_OFFSETS, compressed_u64.as_slice())
    };

    let mut out = Vec::with_capacity(payload.len() + 1);
    out.push(tag);
    out.extend_from_slice(payload);
    Ok(out)
}

fn decode_adaptive_var_bytes_page(encoded: &[u8]) -> io::Result<Vec<Bytes>> {
    let (tag, payload) = encoded.split_first().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "adaptive bytes page is empty")
    })?;

    match *tag {
        ADAPTIVE_BYTES_ZSTD_U64_OFFSETS => decode_var_bytes_raw_u64(&zstd_decompress(payload)?),
        ADAPTIVE_BYTES_ZSTD_U32_OFFSETS => decode_var_bytes_raw_u32(&zstd_decompress(payload)?),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported adaptive bytes tag: {other}"),
        )),
    }
}

fn encode_var_bytes_raw_u64(values: &[Bytes]) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(values.len() as u32).to_le_bytes());

    let mut offset = 0u64;
    raw.extend_from_slice(&offset.to_le_bytes());
    for value in values {
        offset += value.len() as u64;
        raw.extend_from_slice(&offset.to_le_bytes());
    }
    for value in values {
        raw.extend_from_slice(value);
    }
    raw
}

fn encode_var_bytes_raw_u32(values: &[Bytes]) -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(&(values.len() as u32).to_le_bytes());

    let mut offset = 0u32;
    raw.extend_from_slice(&offset.to_le_bytes());
    for value in values {
        offset += value.len() as u32;
        raw.extend_from_slice(&offset.to_le_bytes());
    }
    for value in values {
        raw.extend_from_slice(value);
    }
    raw
}

fn decode_var_bytes_raw_u64(raw: &[u8]) -> io::Result<Vec<Bytes>> {
    if raw.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page is truncated",
        ));
    }

    let row_count = u32::from_le_bytes(raw[..4].try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page row-count is truncated",
        )
    })?) as usize;
    let offsets_start = 4;
    let offsets_len = (row_count + 1) * 8;
    let blob_start = offsets_start + offsets_len;
    if raw.len() < blob_start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page offsets are truncated",
        ));
    }

    let mut offsets = Vec::with_capacity(row_count + 1);
    for index in 0..=row_count {
        let start = offsets_start + index * 8;
        let end = start + 8;
        offsets.push(u64::from_le_bytes(raw[start..end].try_into().map_err(
            |_| io::Error::new(io::ErrorKind::InvalidData, "bytes page offset is truncated"),
        )?));
    }

    materialize_var_bytes(&raw[blob_start..], &offsets)
}

fn decode_var_bytes_raw_u32(raw: &[u8]) -> io::Result<Vec<Bytes>> {
    if raw.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page is truncated",
        ));
    }

    let row_count = u32::from_le_bytes(raw[..4].try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page row-count is truncated",
        )
    })?) as usize;
    let offsets_start = 4;
    let offsets_len = (row_count + 1) * 4;
    let blob_start = offsets_start + offsets_len;
    if raw.len() < blob_start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bytes page offsets are truncated",
        ));
    }

    let mut offsets = Vec::with_capacity(row_count + 1);
    for index in 0..=row_count {
        let start = offsets_start + index * 4;
        let end = start + 4;
        offsets.push(u32::from_le_bytes(raw[start..end].try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "bytes page offset is truncated")
        })?) as u64);
    }

    materialize_var_bytes(&raw[blob_start..], &offsets)
}

fn materialize_var_bytes(blob: &[u8], offsets: &[u64]) -> io::Result<Vec<Bytes>> {
    let row_count = offsets.len().saturating_sub(1);
    let mut values = Vec::with_capacity(row_count);
    for index in 0..row_count {
        let start = offsets[index] as usize;
        let end = offsets[index + 1] as usize;
        if end < start || end > blob.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bytes page offset is out of bounds",
            ));
        }
        values.push(Bytes::copy_from_slice(&blob[start..end]));
    }
    Ok(values)
}

fn decode_plain_u64_page(raw: &[u8], row_count: usize) -> io::Result<Vec<u64>> {
    if raw.len() != row_count * 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded u64 page has unexpected length",
        ));
    }

    raw.chunks_exact(8)
        .map(|chunk| {
            chunk.try_into().map(u64::from_le_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "u64 page entry is truncated")
            })
        })
        .collect()
}

fn decode_plain_u32_page(raw: &[u8], row_count: usize) -> io::Result<Vec<u32>> {
    if raw.len() != row_count * 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "decoded u32 page has unexpected length",
        ));
    }

    raw.chunks_exact(4)
        .map(|chunk| {
            chunk.try_into().map(u32::from_le_bytes).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "u32 page entry is truncated")
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_index_roundtrip() {
        let entries = vec![
            PageIndexEntry {
                first_row: 0,
                row_count: 1024,
                offset: 0,
                encoded_len: 4096,
            },
            PageIndexEntry {
                first_row: 1024,
                row_count: 512,
                offset: 4096,
                encoded_len: 2048,
            },
        ];

        let encoded = write_page_index(&entries);
        let decoded = read_page_index(&encoded).unwrap();
        assert_eq!(decoded, entries);
    }

    #[test]
    fn variable_bytes_page_roundtrip() {
        let values = vec![
            Bytes::from_static(b""),
            Bytes::from_static(b"hello"),
            Bytes::from_static(b"world"),
        ];

        let encoded = encode_var_bytes_page(&values, CompressionCodec::Lz4).unwrap();
        let decoded = decode_var_bytes_page(&encoded, CompressionCodec::Lz4).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn adaptive_fixed_width_roundtrips_zero_prefixed_b256_values() {
        let mut raw = Vec::new();
        for value in 0..128u8 {
            raw.extend_from_slice(&[0u8; 12]);
            raw.extend_from_slice(&[value; 20]);
        }

        let encoded = encode_fixed_width_page(&raw, 32, CompressionCodec::AdaptiveFixed).unwrap();
        let decoded =
            decode_fixed_width_page(&encoded, 128, 32, CompressionCodec::AdaptiveFixed).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn adaptive_fixed_width_uses_dictionary_fast_path_for_repeated_values() {
        let mut raw = Vec::new();
        for _ in 0..128 {
            raw.extend_from_slice(&[0xAB; 32]);
        }

        let encoded = encode_fixed_width_page(&raw, 32, CompressionCodec::AdaptiveFixed).unwrap();
        assert_eq!(encoded.first().copied(), Some(ADAPTIVE_FIXED_DICTIONARY));
        let decoded =
            decode_fixed_width_page(&encoded, 128, 32, CompressionCodec::AdaptiveFixed).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn adaptive_bytes_page_roundtrip() {
        let values = vec![
            Bytes::from_static(b""),
            Bytes::copy_from_slice(&[0u8; 32]),
            Bytes::copy_from_slice(&[1u8; 64]),
        ];

        let encoded = encode_var_bytes_page(&values, CompressionCodec::AdaptiveBytes).unwrap();
        let decoded = decode_var_bytes_page(&encoded, CompressionCodec::AdaptiveBytes).unwrap();
        assert_eq!(decoded, values);
    }
}
