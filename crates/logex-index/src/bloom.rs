//! Shared protected framing and bounded bit geometry for segment skip filters.
use crate::index_file::IndexFile;
use std::io::{self, Read, Seek, SeekFrom};

pub(crate) const HEADER_LEN: u64 = 8 + 8 + 4;
pub(crate) const MIN_FILTER_BYTES: usize = 256 * 1024;
pub(crate) const MAX_FILTER_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const MAX_FILTER_BITS: u64 = (MAX_FILTER_BYTES as u64) * 8;
pub(crate) const HASH_ROUNDS: u64 = 4;

pub(crate) fn encoded_logical_size_for_rows(rows: u64) -> io::Result<u64> {
    let rows = usize::try_from(rows).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "bloom row count exceeds address space",
        )
    })?;
    HEADER_LEN
        .checked_add(filter_bytes(rows) as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bloom size bound overflow"))
}

// Size by source rows, retaining the existing 256 KiB floor and 2 MiB ceiling.
// More keys may increase false positives but never permit false negatives.
// Clamp before multiplication and power-of-two rounding.
pub(crate) fn filter_bytes(row_count: usize) -> usize {
    (row_count.min(MAX_FILTER_BYTES / 32) * 32)
        .next_power_of_two()
        .max(MIN_FILTER_BYTES)
}

pub(crate) fn open_bloom(
    mut reader: IndexFile,
    expected_magic: &[u8; 8],
) -> io::Result<(IndexFile, u64)> {
    if !reader.is_protected() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy bloom has no integrity checks; rebuild derived indexes",
        ));
    }
    let mut header = [0; HEADER_LEN as usize];
    reader.read_exact(&mut header)?;
    let bit_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
    if &header[..8] != expected_magic
        || !bit_len.is_power_of_two()
        || !(MIN_FILTER_BYTES as u64 * 8..=MAX_FILTER_BITS).contains(&bit_len)
        || header[16..20] != (HASH_ROUNDS as u32).to_le_bytes()
        || reader.logical_len() != HEADER_LEN + bit_len / 8
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid bloom file geometry",
        ));
    }
    Ok((reader, bit_len - 1))
}

/// Probe protected pages with the same bit addressing used by the builder.
pub(crate) fn read_key(reader: &mut IndexFile, mask: u64, hashes: (u64, u64)) -> io::Result<bool> {
    for bit in key_bits(hashes, mask) {
        reader.seek(SeekFrom::Start(HEADER_LEN + bit / 8))?;
        let mut byte = [0u8; 1];
        reader.read_exact(&mut byte)?;
        if byte[0] & (1 << (bit % 8)) == 0 {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn key_bits((h1, h2): (u64, u64), mask: u64) -> impl Iterator<Item = u64> {
    (0..HASH_ROUNDS).map(move |round| h1.wrapping_add(round.wrapping_mul(h2)) & mask)
}
