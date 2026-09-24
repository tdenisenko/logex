//! Query-only portable decoding with an allocation-free validated preparation.
use super::container::{Container, ARRAY_LIMIT};
use super::store::{ArrayStore, BitmapStore, Store, BITMAP_LENGTH};
use super::RoaringBitmap;
use std::io;
use std::mem::size_of;

const DENSE_BYTES: usize = BITMAP_LENGTH * size_of::<u64>();

/// A validated, immutable portable bitmap and its requested heap allocation.
///
/// Preparation borrows the input and allocates no heap storage on success.
/// Keep the input's own memory charge separately from the decoded allocation.
/// This query-specific API does not change the ordinary deserializer.
#[derive(Debug)]
pub struct PreparedBitmap<'a> {
    descriptions: &'a [u8],
    runs: Option<&'a [u8]>,
    payload: &'a [u8],
    allocation_bytes: usize,
}

impl RoaringBitmap {
    /// Validate a complete portable bitmap without allocating output or scratch.
    ///
    /// Validates container keys, offsets, exact extent, sorted array values,
    /// dense cardinalities and ordered disjoint runs with exact cardinality.
    /// The returned preparation retains a borrow of these validated bytes.
    pub fn prepare_deserialize(bytes: &[u8]) -> io::Result<PreparedBitmap<'_>> {
        let mut cursor = Cursor(bytes);
        let cookie = cursor.u32()?;
        let (count, offsets, runs) = if cookie == 12346 {
            let count = cursor.u32()? as usize;
            if count > 65536 {
                return Err(invalid("too many Roaring containers"));
            }
            (count, true, None)
        } else if cookie as u16 == 12347 {
            let count = ((cookie >> 16) + 1) as usize;
            (count, count >= 4, Some(cursor.take((count + 7) / 8)?))
        } else {
            return Err(invalid("invalid Roaring bitmap cookie"));
        };
        let descriptions = cursor.take(multiply(count, 4)?)?;
        let offsets = if offsets {
            Some(cursor.take(multiply(count, 4)?)?)
        } else {
            None
        };
        let payload = cursor.0;
        let mut allocation_bytes = multiply(count, size_of::<Container>())?;
        let mut previous_key = None;
        for (index, description) in descriptions.chunks_exact(4).enumerate() {
            let key = le_u16(&description[..2]);
            if previous_key.map_or(false, |previous| previous >= key) {
                return Err(invalid(
                    "Roaring container keys are not strictly increasing",
                ));
            }
            previous_key = Some(key);
            if let Some(offsets) = offsets {
                let offset = le_u32(&offsets[index * 4..index * 4 + 4]);
                if offset as u64 != (bytes.len() - cursor.0.len()) as u64 {
                    return Err(invalid("noncontiguous Roaring container offset"));
                }
            }
            let cardinality = usize::from(le_u16(&description[2..])) + 1;
            let is_run = runs.map_or(false, |runs| runs[index / 8] & (1 << (index % 8)) != 0);
            if is_run {
                let run_count = usize::from(cursor.u16()?);
                let intervals = cursor.take(multiply(run_count, 4)?)?;
                let mut previous_end = None;
                let mut actual = 0usize;
                for run in intervals.chunks_exact(4) {
                    let start = le_u16(&run[..2]);
                    let length = le_u16(&run[2..]);
                    let end = start
                        .checked_add(length)
                        .ok_or_else(|| invalid("Roaring run exceeds its container"))?;
                    if previous_end.map_or(false, |previous| start <= previous) {
                        return Err(invalid("Roaring runs overlap or are not ordered"));
                    }
                    previous_end = Some(end);
                    actual = add(actual, usize::from(length) + 1)?;
                }
                if actual != cardinality {
                    return Err(invalid("Roaring run cardinality mismatch"));
                }
            } else if cardinality <= ARRAY_LIMIT as usize {
                let values = cursor.take(multiply(cardinality, size_of::<u16>())?)?;
                let mut previous = None;
                for value in values.chunks_exact(2) {
                    let value = le_u16(value);
                    if previous.map_or(false, |previous| previous >= value) {
                        return Err(invalid("Roaring array values are not strictly increasing"));
                    }
                    previous = Some(value);
                }
            } else {
                let dense = cursor.take(DENSE_BYTES)?;
                let actual: u32 = dense
                    .chunks_exact(8)
                    .map(|word| u64::from_le_bytes(word.try_into().unwrap()).count_ones())
                    .sum();
                if actual as usize != cardinality {
                    return Err(invalid("Roaring dense cardinality mismatch"));
                }
            }
            allocation_bytes = add(
                allocation_bytes,
                if cardinality <= ARRAY_LIMIT as usize {
                    multiply(cardinality, size_of::<u16>())?
                } else {
                    DENSE_BYTES
                },
            )?;
        }
        if !cursor.0.is_empty() {
            return Err(invalid("trailing Roaring bitmap payload bytes"));
        }
        Ok(PreparedBitmap {
            descriptions,
            runs,
            payload,
            allocation_bytes,
        })
    }

    /// Exact bytes represented by currently retained heap allocation capacities.
    ///
    /// Includes the private container Vec, each array Vec, and dense boxes.
    /// Excludes allocator headers, stack storage and the bitmap struct itself.
    /// Unlike serialization/statistics sizes, this includes unused Vec capacity.
    pub fn heap_size_bytes(&self) -> io::Result<usize> {
        let mut bytes = multiply(self.containers.capacity(), size_of::<Container>())?;
        for container in &self.containers {
            bytes = add(
                bytes,
                match &container.store {
                    Store::Array(array) => multiply(array.capacity(), size_of::<u16>())?,
                    Store::Bitmap(_) => DENSE_BYTES,
                },
            )?;
        }
        Ok(bytes)
    }
}

impl PreparedBitmap<'_> {
    /// Bytes requested for the output's container vector and normalized stores.
    ///
    /// There are no heap scratch allocations during decoding. The allocator can
    /// return larger Vec capacities; measure the returned bitmap with
    /// `RoaringBitmap::heap_size_bytes` while it remains owned and charged.
    pub fn allocation_bytes(&self) -> usize {
        self.allocation_bytes
    }

    /// Decode previously validated bytes into normalized containers.
    ///
    /// Reserve `allocation_bytes()` before calling. This API reports allocator
    /// reservation failures but is not itself a memory limiter. Dense serialized
    /// payloads use the existing direct full-buffer constructor, without another
    /// zero-fill or intermediate copy. Release-mode decoding reuses validated
    /// cardinalities; upstream debug assertions may recheck contents.
    pub fn deserialize(self) -> io::Result<RoaringBitmap> {
        let count = self.descriptions.len() / 4;
        let mut containers = Vec::new();
        containers
            .try_reserve_exact(count)
            .map_err(allocation_error)?;
        let mut cursor = Cursor(self.payload);
        for (index, description) in self.descriptions.chunks_exact(4).enumerate() {
            let key = le_u16(&description[..2]);
            let cardinality = usize::from(le_u16(&description[2..])) + 1;
            let is_run = self
                .runs
                .map_or(false, |runs| runs[index / 8] & (1 << (index % 8)) != 0);
            let store = if is_run {
                let count = usize::from(cursor.u16()?);
                let runs = cursor.take(multiply(count, 4)?)?;
                if cardinality <= ARRAY_LIMIT as usize {
                    let mut values = Vec::new();
                    values
                        .try_reserve_exact(cardinality)
                        .map_err(allocation_error)?;
                    for run in runs.chunks_exact(4) {
                        let start = le_u16(&run[..2]);
                        let end = start + le_u16(&run[2..]); // validated before preparation
                        values.extend(start..=end);
                    }
                    Store::Array(ArrayStore::from_vec_unchecked(values))
                } else {
                    let mut bitmap = BitmapStore::new();
                    for run in runs.chunks_exact(4) {
                        let start = le_u16(&run[..2]);
                        let end = start + le_u16(&run[2..]);
                        bitmap.insert_range(start..=end);
                    }
                    Store::Bitmap(bitmap)
                }
            } else if cardinality <= ARRAY_LIMIT as usize {
                let bytes = cursor.take(multiply(cardinality, size_of::<u16>())?)?;
                let mut values = Vec::new();
                values
                    .try_reserve_exact(cardinality)
                    .map_err(allocation_error)?;
                values.extend(bytes.chunks_exact(2).map(le_u16));
                Store::Array(ArrayStore::from_vec_unchecked(values))
            } else {
                let bytes = cursor.take(DENSE_BYTES)?;
                Store::Bitmap(BitmapStore::from_lsb0_bytes_unchecked(
                    bytes,
                    0,
                    cardinality as u64,
                ))
            };
            containers.push(Container { key, store });
        }
        Ok(RoaringBitmap { containers })
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn allocation_error(error: std::collections::TryReserveError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, error)
}
fn add(a: usize, b: usize) -> io::Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| invalid("Roaring allocation size overflow"))
}
fn multiply(a: usize, b: usize) -> io::Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| invalid("Roaring allocation size overflow"))
}
fn le_u16(bytes: &[u8]) -> u16 {
    u16::from_le_bytes([bytes[0], bytes[1]])
}
fn le_u32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
}
struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> io::Result<&'a [u8]> {
        if len > self.0.len() {
            return Err(invalid("truncated Roaring bitmap"));
        }
        let (head, tail) = self.0.split_at(len);
        self.0 = tail;
        Ok(head)
    }
    fn u16(&mut self) -> io::Result<u16> {
        self.take(2).map(le_u16)
    }
    fn u32(&mut self) -> io::Result<u32> {
        self.take(4).map(le_u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Build portable containers independently of the decoder. Cardinality is
    // explicit so malformed content/header disagreements can also be tested.
    fn portable(parts: &[(u16, u32, bool, Vec<u8>)]) -> Vec<u8> {
        let has_runs = parts.iter().any(|part| part.2);
        let mut bytes = Vec::new();
        if has_runs {
            bytes.extend_from_slice(&(12347u32 | (((parts.len() - 1) as u32) << 16)).to_le_bytes());
            let mut mask = vec![0u8; (parts.len() + 7) / 8];
            for (i, part) in parts.iter().enumerate() {
                if part.2 {
                    mask[i / 8] |= 1 << (i % 8);
                }
            }
            bytes.extend(mask);
        } else {
            bytes.extend_from_slice(&12346u32.to_le_bytes());
            bytes.extend_from_slice(&(parts.len() as u32).to_le_bytes());
        }
        for (key, cardinality, _, _) in parts {
            bytes.extend_from_slice(&key.to_le_bytes());
            bytes.extend_from_slice(&((*cardinality - 1) as u16).to_le_bytes());
        }
        if !has_runs || parts.len() >= 4 {
            let mut offset = bytes.len() + parts.len() * 4;
            for part in parts {
                bytes.extend_from_slice(&(offset as u32).to_le_bytes());
                offset += part.3.len();
            }
        }
        for part in parts {
            bytes.extend_from_slice(&part.3);
        }
        bytes
    }

    fn run_payload(runs: &[(u16, u16)]) -> Vec<u8> {
        let mut bytes = (runs.len() as u16).to_le_bytes().to_vec();
        for (start, length_minus_one) in runs {
            bytes.extend_from_slice(&start.to_le_bytes());
            bytes.extend_from_slice(&length_minus_one.to_le_bytes());
        }
        bytes
    }

    #[test]
    fn logex_prepared_roundtrip_empty_sparse_dense_and_boundaries() {
        for values in [
            Vec::new(),
            vec![0, 65535, 65536, u32::MAX],
            (0..4096).collect(),
            (0..4097).collect(),
            (0xffff0000..=u32::MAX).collect(),
        ] {
            let original: RoaringBitmap = values.into_iter().collect();
            let mut bytes = Vec::new();
            original.serialize_into(&mut bytes).unwrap();
            let prepared = RoaringBitmap::prepare_deserialize(&bytes).unwrap();
            let requested = prepared.allocation_bytes();
            let result = prepared.deserialize().unwrap();
            assert_eq!(result, original);
            assert_eq!(requested, result.heap_size_bytes().unwrap());
        }
    }

    #[test]
    fn logex_runs_are_normalized_without_oversized_array_or_scratch() {
        for cardinality in [1, 4096, 4097, 5000] {
            let runs: Vec<_> = (0..cardinality).map(|i| ((i * 2) as u16, 0)).collect();
            let bytes = portable(&[(65535, cardinality, true, run_payload(&runs))]);
            let prepared = RoaringBitmap::prepare_deserialize(&bytes).unwrap();
            let expected_bytes = size_of::<Container>()
                + if cardinality <= 4096 {
                    cardinality as usize * 2
                } else {
                    DENSE_BYTES
                };
            assert_eq!(prepared.allocation_bytes(), expected_bytes);
            let result = prepared.deserialize().unwrap();
            assert_eq!(result.heap_size_bytes().unwrap(), expected_bytes);
            assert_eq!(result.len(), cardinality as u64);
            assert_eq!(
                result.iter().collect::<Vec<_>>(),
                (0..cardinality)
                    .map(|i| 0xffff0000 + i * 2)
                    .collect::<Vec<_>>()
            );
            assert_eq!(result, RoaringBitmap::deserialize_from(&bytes[..]).unwrap());
            assert!(
                matches!(&result.containers[0].store, Store::Array(_)) == (cardinality <= 4096)
            );
        }
        let bytes = portable(&[(65535, 65536, true, run_payload(&[(0, 65535)]))]);
        let bitmap = RoaringBitmap::prepare_deserialize(&bytes)
            .unwrap()
            .deserialize()
            .unwrap();
        assert_eq!(bitmap.len(), 65536);
        assert_eq!(bitmap.max(), Some(u32::MAX));
    }

    #[test]
    fn logex_mixed_runs_arrays_and_unaligned_dense_payloads() {
        let parts = vec![
            (0, 3, true, run_payload(&[(4, 2)])),
            (1, 2, false, vec![0, 0, 255, 255]),
            (17, 65536, false, vec![255; DENSE_BYTES]),
            (65535, 5000, true, run_payload(&[(0, 4999)])),
        ];
        let bytes = portable(&parts);
        let prepared = RoaringBitmap::prepare_deserialize(&bytes).unwrap();
        let requested = prepared.allocation_bytes();
        let bitmap = prepared.deserialize().unwrap();
        assert_eq!(bitmap.heap_size_bytes().unwrap(), requested);
        assert_eq!(bitmap, RoaringBitmap::deserialize_from(&bytes[..]).unwrap());
        // Every proper prefix and a trailing byte must be rejected preallocation.
        for end in 0..bytes.len() {
            assert!(RoaringBitmap::prepare_deserialize(&bytes[..end]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(RoaringBitmap::prepare_deserialize(&trailing).is_err());
    }

    #[test]
    fn logex_preflight_rejects_corrupt_geometry_and_contents() {
        let rejects = |bytes: Vec<u8>| {
            assert_eq!(
                RoaringBitmap::prepare_deserialize(&bytes)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        };
        rejects(vec![0; 8]);
        let mut count = 12346u32.to_le_bytes().to_vec();
        count.extend_from_slice(&65537u32.to_le_bytes());
        rejects(count);
        rejects(portable(&[(1, 2, false, vec![1, 0, 1, 0])]));
        rejects(portable(&[(1, 2, false, vec![2, 0, 1, 0])]));
        rejects(portable(&[
            (1, 1, false, vec![0, 0]),
            (1, 1, false, vec![0, 0]),
        ]));
        rejects(portable(&[
            (2, 1, false, vec![0, 0]),
            (1, 1, false, vec![0, 0]),
        ]));
        rejects(portable(&[(1, 4097, false, vec![0; DENSE_BYTES])]));
        rejects(portable(&[(1, 4, true, run_payload(&[(0, 2)]))]));
        rejects(portable(&[(1, 4, true, run_payload(&[(0, 1), (1, 1)]))]));
        rejects(portable(&[(1, 4, true, run_payload(&[(9, 1), (0, 1)]))]));
        rejects(portable(&[(1, 2, true, run_payload(&[(65535, 1)]))]));
        rejects(portable(&[(1, 1, true, run_payload(&[]))]));
        let mut offset = portable(&[(1, 1, false, vec![0, 0])]);
        offset[12..16].copy_from_slice(&15u32.to_le_bytes());
        rejects(offset);
        assert!(add(usize::MAX, 1).is_err());
        assert!(multiply(usize::MAX, 2).is_err());
    }

    #[test]
    fn logex_heap_measurement_counts_spare_capacity_and_dense_bytes() {
        let mut values = Vec::with_capacity(9000);
        values.extend([1, 9]);
        let array_capacity = values.capacity();
        let mut containers = Vec::with_capacity(5);
        containers.push(Container {
            key: 0,
            store: Store::Array(ArrayStore::from_vec_unchecked(values)),
        });
        containers.push(Container {
            key: 1,
            store: Store::Bitmap(BitmapStore::full()),
        });
        let expected =
            containers.capacity() * size_of::<Container>() + array_capacity * 2 + DENSE_BYTES;
        let bitmap = RoaringBitmap { containers };
        assert_eq!(bitmap.heap_size_bytes().unwrap(), expected);
    }
}
