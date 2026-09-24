//! Query-owned decoded bitmaps, with charges that follow their heap allocations.
use std::io;

use logex_types::{QueryMemoryBudget, QueryMemoryError, QueryMemoryReservation};
use roaring::RoaringBitmap;

/// A decoded query result whose container and payload capacities remain charged.
///
/// Immutable iteration does not allocate. This type deliberately has no `Clone`
/// or mutable bitmap escape: a new allocation needs a separate reservation.
#[derive(Debug)]
pub struct QueryBitmap {
    bitmap: RoaringBitmap,
    // Field order releases the backing allocations before their charge.
    reservation: QueryMemoryReservation,
}

impl QueryBitmap {
    pub(crate) fn decode(data: &[u8], memory: &QueryMemoryBudget) -> io::Result<Self> {
        const STAGE: &str = "decoded index bitmap";
        // Preparation validates borrowed bytes without allocating. The pinned
        // decoder preallocates the container vector and normalized stores, so
        // this allowance includes every requested output allocation.
        let prepared = RoaringBitmap::prepare_deserialize(data)?;
        let reservation = memory
            .reserve(prepared.allocation_bytes(), STAGE)
            .map_err(io::Error::other)?;
        let bitmap = prepared.deserialize()?;
        let mut owned = Self {
            bitmap,
            reservation,
        };
        let actual = owned.bitmap.heap_size_bytes()?;
        let reserved = usize::try_from(owned.reservation.bytes()).map_err(io::Error::other)?;
        if actual > reserved {
            // Allocator rounding is existing memory, even if it crosses the
            // cooperative limit. Record it until the rejected owner is dropped.
            let additional = actual - reserved;
            let used = memory.used();
            owned.reservation.record_existing(additional);
            if memory.used() > memory.limit() as u128 {
                return Err(io::Error::other(QueryMemoryError::CapacityExceeded {
                    requested: additional,
                    used,
                    limit: memory.limit(),
                    stage: STAGE,
                }));
            }
        } else {
            owned
                .reservation
                .shrink(reserved - actual)
                .map_err(io::Error::other)?;
        }
        Ok(owned)
    }

    pub fn len(&self) -> u64 {
        self.bitmap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    pub fn contains(&self, row: u32) -> bool {
        self.bitmap.contains(row)
    }

    pub fn iter(&self) -> roaring::bitmap::Iter<'_> {
        self.bitmap.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_types::QueryMemoryLimit;
    use std::sync::Arc;

    #[test]
    fn decoded_capacity_is_retained_by_shared_result_and_released_on_denial() {
        for rows in [vec![], vec![1, 9, u32::MAX], (0..5000).collect()] {
            let expected: RoaringBitmap = rows.iter().copied().collect();
            let mut encoded = Vec::new();
            expected.serialize_into(&mut encoded).unwrap();
            let allowance = RoaringBitmap::prepare_deserialize(&encoded)
                .unwrap()
                .allocation_bytes();
            let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(allowance.max(1)).unwrap());
            let result = QueryBitmap::decode(&encoded, &memory).unwrap();
            assert_eq!(result.iter().collect::<Vec<_>>(), rows);
            assert_eq!(result.len(), rows.len() as u64);
            assert_eq!(result.is_empty(), rows.is_empty());
            assert_eq!(
                memory.used(),
                result.bitmap.heap_size_bytes().unwrap() as u128
            );
            let result = Arc::new(result);
            let alias = result.clone();
            drop(result);
            assert_eq!(memory.used(), alias.reservation.bytes());
            assert!(rows.iter().all(|row| alias.contains(*row)));
            drop(alias);
            assert_eq!(memory.used(), 0);
            if allowance > 1 {
                let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(allowance - 1).unwrap());
                let error = QueryBitmap::decode(&encoded, &memory).unwrap_err();
                assert!(error.get_ref().unwrap().is::<QueryMemoryError>());
                assert_eq!(memory.used(), 0);
            }
        }
    }
}
