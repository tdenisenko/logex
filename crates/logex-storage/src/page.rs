use std::io;

use alloy_primitives::Bytes;

use crate::compression::{
    delta_decode, delta_encode, delta_of_delta_decode, delta_of_delta_encode, dict_decode,
    dict_encode, lz4_compress, lz4_decompress, zstd_compress, zstd_decompress,
};
use crate::native::CompressionCodec;

const PAGE_INDEX_MAGIC: &[u8; 4] = b"LXPI";
const PAGE_INDEX_VERSION: u32 = 1;

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
        CompressionCodec::Dictionary => {
            let values: Vec<&[u8]> = raw_values.chunks_exact(item_size).collect();
            Ok(dict_encode(&values, item_size))
        }
        CompressionCodec::Zstd => zstd_compress(raw_values),
        CompressionCodec::Lz4 => Ok(lz4_compress(raw_values)),
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
        CompressionCodec::Dictionary => {
            let borrowed: Vec<&[u8]> = values.iter().map(std::slice::from_ref).collect();
            Ok(dict_encode(&borrowed, 1))
        }
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
}
