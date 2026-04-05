use std::io;

/// Compression codec identifier stored in column file headers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Codec {
    /// No compression.
    None = 0,
    /// Dictionary encoding: dictionary of unique values + bitpacked indices.
    /// Good for low-to-medium cardinality columns (address, topic0).
    Dictionary = 1,
    /// Delta encoding + bitpacking. For monotonically increasing u64 columns (block_number).
    Delta = 2,
    /// Zstd compression. General-purpose, good ratio. For high-cardinality 32-byte columns (topic1-3).
    Zstd = 3,
    /// LZ4 compression. Fast decompression. For variable-length data column.
    Lz4 = 4,
    /// Delta-of-delta encoding. For near-constant-interval u64 columns (timestamp post-merge).
    DeltaOfDelta = 5,
}

impl Codec {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::None),
            1 => Some(Self::Dictionary),
            2 => Some(Self::Delta),
            3 => Some(Self::Zstd),
            4 => Some(Self::Lz4),
            5 => Some(Self::DeltaOfDelta),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Dictionary codec: for fixed-size values (address 20B, topic0 32B)
// Format: [dict_size: u32] [dict entries...] [index_bits: u8] [packed indices...]
// ---------------------------------------------------------------------------

pub fn dict_encode(values: &[&[u8]], item_size: usize) -> Vec<u8> {
    // Build dictionary
    let mut dict: Vec<Vec<u8>> = Vec::new();
    let mut index_map: std::collections::HashMap<Vec<u8>, u32> = std::collections::HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());

    for val in values {
        let key = val.to_vec();
        let idx = if let Some(&existing) = index_map.get(&key) {
            existing
        } else {
            let idx = dict.len() as u32;
            index_map.insert(key.clone(), idx);
            dict.push(key);
            idx
        };
        indices.push(idx);
    }

    let dict_size = dict.len() as u32;
    let bits_needed = if dict_size <= 1 {
        1
    } else {
        32 - (dict_size - 1).leading_zeros() as u8
    };

    let mut out = Vec::new();
    // Dict size
    out.extend_from_slice(&dict_size.to_le_bytes());
    // Item size
    out.extend_from_slice(&(item_size as u32).to_le_bytes());
    // Dict entries
    for entry in &dict {
        out.extend_from_slice(entry);
    }
    // Bits per index
    out.push(bits_needed);
    // Bitpacked indices
    bitpack_u32(&indices, bits_needed, &mut out);

    out
}

pub fn dict_decode(data: &[u8], row_count: usize, item_size: usize) -> io::Result<Vec<u8>> {
    if data.len() < 8 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "dict too short"));
    }

    let dict_size = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    let stored_item_size = u32::from_le_bytes(data[4..8].try_into().unwrap()) as usize;
    if stored_item_size != item_size {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dict item size mismatch",
        ));
    }

    let dict_start = 8;
    let dict_end = dict_start + dict_size * item_size;
    if data.len() < dict_end + 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dict data truncated",
        ));
    }

    let dict_bytes = &data[dict_start..dict_end];
    let bits_needed = data[dict_end];
    let packed_start = dict_end + 1;

    let indices = bitunpack_u32(&data[packed_start..], row_count, bits_needed)?;

    let mut result = Vec::with_capacity(row_count * item_size);
    for idx in indices {
        let offset = idx as usize * item_size;
        result.extend_from_slice(&dict_bytes[offset..offset + item_size]);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Delta codec: for monotonically increasing u64 values (block_number)
// Format: [base: u64] [max_delta_bits: u8] [packed deltas...]
// ---------------------------------------------------------------------------

pub fn delta_encode(values: &[u64]) -> Vec<u8> {
    if values.is_empty() {
        return Vec::new();
    }

    let base = values[0];
    let mut deltas: Vec<u64> = Vec::with_capacity(values.len().saturating_sub(1));
    let mut prev = base;
    for &v in &values[1..] {
        deltas.push(v.wrapping_sub(prev));
        prev = v;
    }

    let max_delta = deltas.iter().copied().max().unwrap_or(0);
    let bits = if max_delta == 0 {
        1
    } else {
        64 - max_delta.leading_zeros() as u8
    };

    let mut out = Vec::new();
    out.extend_from_slice(&base.to_le_bytes());
    out.push(bits);
    bitpack_u64(&deltas, bits, &mut out);

    out
}

pub fn delta_decode(data: &[u8], row_count: usize) -> io::Result<Vec<u64>> {
    if row_count == 0 {
        return Ok(Vec::new());
    }
    if data.len() < 9 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "delta data too short",
        ));
    }

    let base = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let bits = data[8];

    let deltas = bitunpack_u64(&data[9..], row_count - 1, bits)?;

    let mut result = Vec::with_capacity(row_count);
    result.push(base);
    let mut prev = base;
    for d in deltas {
        prev = prev.wrapping_add(d);
        result.push(prev);
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Delta-of-delta codec: for near-constant-interval u64 (timestamp)
// Format: [base: u64] [first_delta: i64] [max_dd_bits: u8] [packed dd as zigzag...]
// ---------------------------------------------------------------------------

pub fn delta_of_delta_encode(values: &[u64]) -> Vec<u8> {
    if values.len() <= 1 {
        let mut out = Vec::new();
        if let Some(&v) = values.first() {
            out.extend_from_slice(&v.to_le_bytes());
        }
        return out;
    }

    let base = values[0];
    let first_delta = values[1] as i64 - values[0] as i64;

    let mut dds: Vec<i64> = Vec::with_capacity(values.len().saturating_sub(2));
    let mut prev_delta = first_delta;
    for i in 2..values.len() {
        let delta = values[i] as i64 - values[i - 1] as i64;
        dds.push(delta - prev_delta);
        prev_delta = delta;
    }

    // Zigzag encode the delta-of-deltas so they're unsigned
    let zigzag: Vec<u64> = dds.iter().map(|&v| zigzag_encode(v)).collect();
    let max_zz = zigzag.iter().copied().max().unwrap_or(0);
    let bits = if max_zz == 0 {
        1
    } else {
        64 - max_zz.leading_zeros() as u8
    };

    let mut out = Vec::new();
    out.extend_from_slice(&base.to_le_bytes());
    out.extend_from_slice(&first_delta.to_le_bytes());
    out.push(bits);
    bitpack_u64(&zigzag, bits, &mut out);

    out
}

pub fn delta_of_delta_decode(data: &[u8], row_count: usize) -> io::Result<Vec<u64>> {
    if row_count == 0 {
        return Ok(Vec::new());
    }
    if data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dod data too short",
        ));
    }

    let base = u64::from_le_bytes(data[0..8].try_into().unwrap());
    if row_count == 1 {
        return Ok(vec![base]);
    }

    if data.len() < 17 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "dod data too short for first_delta",
        ));
    }

    let first_delta = i64::from_le_bytes(data[8..16].try_into().unwrap());
    let bits = data[16];

    let zigzag = bitunpack_u64(&data[17..], row_count - 2, bits)?;
    let dds: Vec<i64> = zigzag.iter().map(|&v| zigzag_decode(v)).collect();

    let mut result = Vec::with_capacity(row_count);
    result.push(base);
    result.push((base as i64 + first_delta) as u64);

    let mut prev_delta = first_delta;
    for &dd in &dds {
        let delta = prev_delta + dd;
        let prev_val = *result.last().unwrap();
        result.push((prev_val as i64 + delta) as u64);
        prev_delta = delta;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Zstd wrapper
// ---------------------------------------------------------------------------

pub fn zstd_compress(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::encode_all(data, 3)
}

pub fn zstd_decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    zstd::decode_all(data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// ---------------------------------------------------------------------------
// LZ4 wrapper
// ---------------------------------------------------------------------------

pub fn lz4_compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

pub fn lz4_decompress(data: &[u8]) -> io::Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// ---------------------------------------------------------------------------
// Bitpacking helpers
// ---------------------------------------------------------------------------

fn bitpack_u32(values: &[u32], bits: u8, out: &mut Vec<u8>) {
    if values.is_empty() || bits == 0 {
        return;
    }
    let bits = bits as u32;
    let mut buffer: u64 = 0;
    let mut buf_bits: u32 = 0;

    for &val in values {
        buffer |= (val as u64 & ((1u64 << bits) - 1)) << buf_bits;
        buf_bits += bits;
        while buf_bits >= 8 {
            out.push(buffer as u8);
            buffer >>= 8;
            buf_bits -= 8;
        }
    }
    if buf_bits > 0 {
        out.push(buffer as u8);
    }
}

fn bitunpack_u32(data: &[u8], count: usize, bits: u8) -> io::Result<Vec<u32>> {
    if count == 0 || bits == 0 {
        return Ok(vec![0; count]);
    }
    let bits = bits as u32;
    let mask = (1u64 << bits) - 1;
    let mut result = Vec::with_capacity(count);
    let mut bit_offset: usize = 0;

    for _ in 0..count {
        let byte_pos = bit_offset / 8;
        let bit_pos = (bit_offset % 8) as u32;

        // Read up to 8 bytes starting at byte_pos
        let mut buf = [0u8; 8];
        let available = data.len().saturating_sub(byte_pos).min(8);
        buf[..available].copy_from_slice(&data[byte_pos..byte_pos + available]);
        let word = u64::from_le_bytes(buf);

        result.push(((word >> bit_pos) & mask) as u32);
        bit_offset += bits as usize;
    }

    Ok(result)
}

fn bitpack_u64(values: &[u64], bits: u8, out: &mut Vec<u8>) {
    if values.is_empty() || bits == 0 {
        return;
    }
    let bits = bits as u32;
    let mut buffer: u128 = 0;
    let mut buf_bits: u32 = 0;

    for &val in values {
        let mask = if bits >= 64 {
            u64::MAX
        } else {
            (1u64 << bits) - 1
        };
        buffer |= ((val & mask) as u128) << buf_bits;
        buf_bits += bits;
        while buf_bits >= 8 {
            out.push(buffer as u8);
            buffer >>= 8;
            buf_bits -= 8;
        }
    }
    if buf_bits > 0 {
        out.push(buffer as u8);
    }
}

fn bitunpack_u64(data: &[u8], count: usize, bits: u8) -> io::Result<Vec<u64>> {
    if count == 0 || bits == 0 {
        return Ok(vec![0; count]);
    }
    let bits = bits as u32;
    let mask: u128 = if bits >= 64 {
        u64::MAX as u128
    } else {
        (1u128 << bits) - 1
    };
    let mut result = Vec::with_capacity(count);
    let mut bit_offset: usize = 0;

    for _ in 0..count {
        let byte_pos = bit_offset / 8;
        let bit_pos = (bit_offset % 8) as u32;

        let mut buf = [0u8; 16];
        let available = data.len().saturating_sub(byte_pos).min(16);
        buf[..available].copy_from_slice(&data[byte_pos..byte_pos + available]);
        let word = u128::from_le_bytes(buf);

        result.push(((word >> bit_pos) & mask) as u64);
        bit_offset += bits as usize;
    }

    Ok(result)
}

fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dict_roundtrip_20b() {
        // Simulate address data (20 bytes each), with repetition
        let addr1 = [1u8; 20];
        let addr2 = [2u8; 20];
        let addr3 = [3u8; 20];
        let values: Vec<&[u8]> = vec![&addr1, &addr2, &addr1, &addr3, &addr2, &addr1];

        let encoded = dict_encode(&values, 20);
        let decoded = dict_decode(&encoded, 6, 20).unwrap();

        assert_eq!(decoded.len(), 6 * 20);
        assert_eq!(&decoded[0..20], &addr1);
        assert_eq!(&decoded[20..40], &addr2);
        assert_eq!(&decoded[40..60], &addr1);
        assert_eq!(&decoded[60..80], &addr3);
    }

    #[test]
    fn test_dict_roundtrip_32b() {
        // Simulate topic0 data (32 bytes), low cardinality
        let t1 = [0xAAu8; 32];
        let t2 = [0xBBu8; 32];
        let values: Vec<&[u8]> = vec![&t1, &t1, &t2, &t1, &t2];

        let encoded = dict_encode(&values, 32);
        let decoded = dict_decode(&encoded, 5, 32).unwrap();

        assert_eq!(&decoded[0..32], &t1);
        assert_eq!(&decoded[32..64], &t1);
        assert_eq!(&decoded[64..96], &t2);
    }

    #[test]
    fn test_delta_roundtrip() {
        let values: Vec<u64> = vec![1000, 1001, 1003, 1006, 1010, 1015];
        let encoded = delta_encode(&values);
        let decoded = delta_decode(&encoded, values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_delta_constant() {
        // All same value
        let values: Vec<u64> = vec![42; 100];
        let encoded = delta_encode(&values);
        let decoded = delta_decode(&encoded, 100).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_delta_of_delta_constant_interval() {
        // Post-merge: 12s cadence
        let values: Vec<u64> = (0..100).map(|i| 1_700_000_000 + i * 12).collect();
        let encoded = delta_of_delta_encode(&values);
        // Should be very compact since delta-of-delta is 0
        let decoded = delta_of_delta_decode(&encoded, 100).unwrap();
        assert_eq!(decoded, values);

        // Very compact: base(8) + first_delta(8) + bits(1) + packed(ceil(98*1/8))
        assert!(encoded.len() < 32, "encoded size: {}", encoded.len());
    }

    #[test]
    fn test_delta_of_delta_varying_interval() {
        let values: Vec<u64> = vec![100, 112, 124, 137, 149, 160, 172];
        let encoded = delta_of_delta_encode(&values);
        let decoded = delta_of_delta_decode(&encoded, values.len()).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"hello world hello world hello world";
        let compressed = zstd_compress(data).unwrap();
        let decompressed = zstd_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"the quick brown fox jumps over the lazy dog again and again";
        let compressed = lz4_compress(data);
        let decompressed = lz4_decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_bitpack_u32_roundtrip() {
        let values: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7];
        let mut packed = Vec::new();
        bitpack_u32(&values, 3, &mut packed);
        let unpacked = bitunpack_u32(&packed, 8, 3).unwrap();
        assert_eq!(unpacked, values);
    }

    #[test]
    fn test_bitpack_u64_roundtrip() {
        let values: Vec<u64> = vec![0, 5, 10, 15, 20, 100, 255];
        let mut packed = Vec::new();
        bitpack_u64(&values, 8, &mut packed);
        let unpacked = bitunpack_u64(&packed, 7, 8).unwrap();
        assert_eq!(unpacked, values);
    }

    #[test]
    fn test_zigzag() {
        assert_eq!(zigzag_encode(0), 0);
        assert_eq!(zigzag_encode(-1), 1);
        assert_eq!(zigzag_encode(1), 2);
        assert_eq!(zigzag_encode(-2), 3);

        for v in [-1000i64, -1, 0, 1, 1000, i64::MIN, i64::MAX] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    #[test]
    fn test_dict_single_value() {
        let val = [0x42u8; 20];
        let values: Vec<&[u8]> = vec![&val; 1000];
        let encoded = dict_encode(&values, 20);
        let decoded = dict_decode(&encoded, 1000, 20).unwrap();
        // Should be very compact: 1 dict entry + 1 bit per index
        assert!(encoded.len() < 200, "encoded size: {}", encoded.len());
        assert_eq!(decoded.len(), 1000 * 20);
        assert_eq!(&decoded[0..20], &val);
        assert_eq!(&decoded[19980..20000], &val);
    }
}
