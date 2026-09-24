//! Query-owned bitmaps, with charges that follow their heap allocations.
use std::io;

use logex_types::{QueryBuffer, QueryMemoryBudget, QueryMemoryError, QueryMemoryReservation};
use roaring::{RoaringBitmap, bitmap::QuerySetOp};

/// A query bitmap whose container and payload capacities remain charged.
///
/// Immutable iteration does not allocate. This type deliberately has no `Clone`
/// or mutable bitmap escape: an allocation needs a separate admission.
#[derive(Debug)]
pub struct QueryBitmap {
    bitmap: RoaringBitmap,
    // Field order releases the backing allocations before their charge.
    reservation: QueryMemoryReservation,
}

/// Owned operators move allocations between operands, including whole-map swaps.
/// Both data fields must die before either charge on every error/unwind path.
struct BitmapOperation {
    left: RoaringBitmap,
    right: RoaringBitmap,
    reservation: QueryMemoryReservation,
    right_reservation: QueryMemoryReservation,
}

impl QueryBitmap {
    pub fn empty(memory: &QueryMemoryBudget) -> io::Result<Self> {
        Ok(Self {
            bitmap: RoaringBitmap::new(),
            reservation: memory
                .reserve(0, "query bitmap")
                .map_err(io::Error::other)?,
        })
    }

    pub(crate) fn decode(data: &[u8], memory: &QueryMemoryBudget) -> io::Result<Self> {
        // Preparation validates borrowed bytes without allocating. The pinned
        // decoder preallocates the container vector and normalized stores.
        let prepared = RoaringBitmap::prepare_deserialize(data)?;
        let reservation = memory
            .reserve(prepared.allocation_bytes(), "decoded index bitmap")
            .map_err(io::Error::other)?;
        let bitmap = prepared.deserialize()?;
        let mut owned = Self {
            bitmap,
            reservation,
        };
        owned.reconcile()?;
        Ok(owned)
    }

    pub fn try_clone(&self, memory: &QueryMemoryBudget) -> io::Result<Self> {
        let prepared = self.bitmap.prepare_clone()?;
        let mut reservation = memory
            .reserve(prepared.allocation_bytes(), "bitmap clone")
            .map_err(io::Error::other)?;
        let bitmap = prepared.materialize(|requested, actual| {
            observe_allocation(&mut reservation, requested, actual)
        })?;
        let mut owned = Self {
            bitmap,
            reservation,
        };
        owned.reconcile()?;
        Ok(owned)
    }

    /// Consume both owners, preserving upstream container and payload moves.
    pub fn union(self, other: Self) -> io::Result<Self> {
        self.assign_owned(other, QuerySetOp::Union)
    }

    /// Consume both owners, preserving the smaller-map/smaller-array reuse.
    pub fn intersection(self, other: Self) -> io::Result<Self> {
        self.assign_owned(other, QuerySetOp::Intersection)
    }

    fn assign_owned(self, other: Self, operation: QuerySetOp) -> io::Result<Self> {
        let mut owned = BitmapOperation {
            left: self.bitmap,
            right: other.bitmap,
            reservation: self.reservation,
            right_reservation: other.reservation,
        };
        // Transfer existing credit; never release it and compete to reacquire
        // memory which is still alive under the combined operation owner.
        owned
            .reservation
            .absorb(&mut owned.right_reservation)
            .map_err(io::Error::other)?;
        let prepared = owned
            .left
            .prepare_owned_assign(&mut owned.right, operation)?;
        owned
            .reservation
            .try_grow(prepared.additional_allocation_bytes())
            .map_err(io::Error::other)?;
        prepared.apply(|requested, actual| {
            observe_allocation(&mut owned.reservation, requested, actual)
        })?;
        // Empty rows do not imply zero Vec capacity. Dispose of the RHS backing
        // before releasing any combined credit or transferring the result.
        drop(std::mem::take(&mut owned.right));
        let mut result = Self {
            bitmap: owned.left,
            reservation: owned.reservation,
        };
        result.reconcile()?;
        Ok(result)
    }

    /// Union with a retained source, preserving reference-operator semantics.
    pub fn union_ref(mut self, other: &Self) -> io::Result<Self> {
        let prepared = self
            .bitmap
            .prepare_assign(&other.bitmap, QuerySetOp::Union)?;
        self.reservation
            .try_grow(prepared.additional_allocation_bytes())
            .map_err(io::Error::other)?;
        prepared.apply(|requested, actual| {
            observe_allocation(&mut self.reservation, requested, actual)
        })?;
        self.reconcile()?;
        Ok(self)
    }

    /// Preserve the index range-union policy: reference MultiOps for a narrow
    /// container span, and ordered reference pairwise unions for wider spans.
    /// All input owners remain alive until the separately charged result exists.
    pub fn union_all(bitmaps: &[Self], memory: &QueryMemoryBudget) -> io::Result<Self> {
        match bitmaps {
            [] => return Self::empty(memory),
            [bitmap] => return bitmap.try_clone(memory),
            _ => {}
        }
        let mut first = u32::MAX;
        let mut last = 0;
        for bitmap in bitmaps {
            if let (Some(low), Some(high)) = (bitmap.min(), bitmap.max()) {
                first = first.min(low >> 16);
                last = last.max(high >> 16);
                if last - first >= 8 {
                    let mut result = Self::empty(memory)?;
                    for bitmap in bitmaps {
                        result = result.union_ref(bitmap)?;
                    }
                    return Ok(result);
                }
            }
        }
        let mut sources =
            QueryBuffer::try_with_capacity(bitmaps.len(), Some(memory), "bitmap union references")?;
        for bitmap in bitmaps {
            sources.try_push(&bitmap.bitmap)?;
        }
        let prepared = RoaringBitmap::prepare_union_refs(&sources)?;
        let mut reservation = memory
            .reserve(prepared.allocation_bytes(), "bitmap range union")
            .map_err(io::Error::other)?;
        let bitmap = prepared.materialize(|requested, actual| {
            observe_allocation(&mut reservation, requested, actual)
        })?;
        let mut result = Self {
            bitmap,
            reservation,
        };
        result.reconcile()?;
        Ok(result)
    }

    /// Convert once to sorted row IDs, retaining both input and output charges
    /// during construction. The returned buffer preserves capacity across LIMIT
    /// truncation, in-place refinement and shared plan/stream ownership.
    pub fn into_row_ids(self, memory: &QueryMemoryBudget) -> io::Result<QueryBuffer<u32>> {
        const STAGE: &str = "query candidate row IDs";
        let count = usize::try_from(self.len())
            .map_err(|_| io::Error::other(QueryMemoryError::SizeOverflow { stage: STAGE }))?;
        let mut rows = QueryBuffer::try_with_capacity(count, Some(memory), STAGE)?;
        rows.try_extend(self.bitmap.iter())?;
        Ok(rows)
    }

    fn reconcile(&mut self) -> io::Result<()> {
        let actual = self.bitmap.heap_size_bytes()?;
        let reserved = self.reservation.bytes();
        if actual as u128 > reserved {
            account_existing(
                &mut self.reservation,
                actual - usize::try_from(reserved).map_err(io::Error::other)?,
            )?;
        } else if reserved > actual as u128 {
            self.reservation
                .shrink(usize::try_from(reserved - actual as u128).map_err(io::Error::other)?)
                .map_err(io::Error::other)?;
        }
        Ok(())
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
    pub fn min(&self) -> Option<u32> {
        self.bitmap.min()
    }
    pub fn max(&self) -> Option<u32> {
        self.bitmap.max()
    }
    pub fn iter(&self) -> roaring::bitmap::Iter<'_> {
        self.bitmap.iter()
    }
}

fn observe_allocation(
    reservation: &mut QueryMemoryReservation,
    requested: usize,
    actual: usize,
) -> io::Result<()> {
    // The preparation includes every requested allocation. Keep cumulative
    // allocator excess until final reconciliation, including temporary vectors
    // which may disappear during normalization. Usually this is a no-op.
    account_existing(reservation, actual.saturating_sub(requested))
}

fn account_existing(reservation: &mut QueryMemoryReservation, additional: usize) -> io::Result<()> {
    if let Err(error) = reservation.try_grow(additional) {
        // Observation follows allocation. Credit must remain consumed until the
        // rejected allocation and its enclosing owner have actually been dropped.
        reservation.record_existing(additional);
        return Err(io::Error::other(error));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_types::QueryMemoryLimit;
    use std::collections::BTreeSet;
    use std::sync::Arc;

    fn owned(rows: &[u32], memory: &QueryMemoryBudget) -> QueryBitmap {
        let bitmap: RoaringBitmap = rows.iter().copied().collect();
        let mut encoded = Vec::new();
        bitmap.serialize_into(&mut encoded).unwrap();
        QueryBitmap::decode(&encoded, memory).unwrap()
    }

    fn memory() -> QueryMemoryBudget {
        QueryMemoryBudget::new(QueryMemoryLimit::new(4 * 1024 * 1024).unwrap())
    }

    #[test]
    fn owned_and_borrowed_operations_preserve_sets_and_actual_charges() {
        let cases = [
            (vec![], vec![1]),
            (vec![1, 3, 65_539], vec![2, 3, u32::MAX]),
            ((0..5000).collect(), vec![3, 6000]),
            (vec![3, 6000], (0..5000).collect()),
            ((0..5000).collect(), (4500..10_000).collect()),
        ];
        for (left_rows, right_rows) in cases {
            let left_set: BTreeSet<_> = left_rows.iter().copied().collect();
            let right_set: BTreeSet<_> = right_rows.iter().copied().collect();
            for intersection in [false, true] {
                let memory = memory();
                let left = owned(&left_rows, &memory);
                let right = owned(&right_rows, &memory);
                let expected: Vec<_> = if intersection {
                    left_set.intersection(&right_set).copied().collect()
                } else {
                    left_set.union(&right_set).copied().collect()
                };
                let result = if intersection {
                    left.intersection(right).unwrap()
                } else {
                    left.union(right).unwrap()
                };
                assert_eq!(result.iter().collect::<Vec<_>>(), expected);
                assert_eq!(
                    memory.used(),
                    result.bitmap.heap_size_bytes().unwrap() as u128
                );
                drop(result);
                assert_eq!(memory.used(), 0);
            }
            let memory = memory();
            let right = owned(&right_rows, &memory);
            let retained = memory.used();
            let result = owned(&left_rows, &memory).union_ref(&right).unwrap();
            assert_eq!(
                result.iter().collect::<BTreeSet<_>>(),
                &left_set | &right_set
            );
            assert_eq!(memory.used(), retained + result.reservation.bytes());
            drop(result);
            assert_eq!(memory.used(), retained);
            assert_eq!(right.iter().collect::<BTreeSet<_>>(), right_set);
            drop(right);
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn owned_operations_admit_only_additional_work_and_release_denied_inputs() {
        for operation in [QuerySetOp::Union, QuerySetOp::Intersection] {
            for deny in [false, true] {
                let memory = memory();
                let mut left = owned(&(0..5000).collect::<Vec<_>>(), &memory);
                let mut right = owned(&[4500, 4999, 6000, 1 << 16], &memory);
                let extra = left
                    .bitmap
                    .prepare_owned_assign(&mut right.bitmap, operation)
                    .unwrap()
                    .additional_allocation_bytes();
                if deny && extra == 0 {
                    continue;
                }
                let available = extra.saturating_sub(usize::from(deny));
                let held = memory
                    .reserve(
                        memory.limit() - memory.used() as usize - available,
                        "competing query",
                    )
                    .unwrap();
                let result = left.assign_owned(right, operation);
                if deny {
                    let error = result.unwrap_err();
                    assert!(error.get_ref().is_some_and(|e| e.is::<QueryMemoryError>()));
                } else {
                    let result = result.unwrap();
                    assert_eq!(memory.used(), held.bytes() + result.reservation.bytes());
                    drop(result);
                }
                assert_eq!(memory.used(), held.bytes());
                drop(held);
                assert_eq!(memory.used(), 0);
            }
        }
        let left_memory = memory();
        let right_memory = memory();
        let error = owned(&[1], &left_memory)
            .union(owned(&[2], &right_memory))
            .unwrap_err();
        assert_eq!(
            error.get_ref().unwrap().downcast_ref::<QueryMemoryError>(),
            Some(&QueryMemoryError::DifferentBudget)
        );
        assert_eq!((left_memory.used(), right_memory.used()), (0, 0));
    }

    #[test]
    fn range_unions_keep_sources_owned_through_both_routes_and_row_id_conversion() {
        for span in [7, 8] {
            let memory = memory();
            let bitmaps = [
                owned(&[1, 9], &memory),
                owned(&[9, (span << 16) | 4], &memory),
            ];
            let inputs = memory.used();
            let result = QueryBitmap::union_all(&bitmaps, &memory).unwrap();
            assert_eq!(result.iter().collect::<Vec<_>>(), [1, 9, (span << 16) | 4]);
            assert_eq!(memory.used(), inputs + result.reservation.bytes());
            let mut rows = result.into_row_ids(&memory).unwrap();
            assert_eq!(memory.used(), inputs + (rows.capacity() * 4) as u128);
            let capacity = rows.capacity();
            rows.retain(|row| *row >= 9);
            rows.truncate(1);
            assert_eq!(&*rows, &[9]);
            assert_eq!(rows.capacity(), capacity);
            let rows = Arc::new(rows);
            let alias = rows.clone();
            drop(rows);
            drop(bitmaps);
            assert_eq!(memory.used(), (capacity * 4) as u128);
            assert_eq!(&**alias, &[9]);
            drop(alias);
            assert_eq!(memory.used(), 0);
        }
        let memory = memory();
        let source = owned(&[1, 5, 8], &memory);
        let held = memory
            .reserve(memory.limit() - memory.used() as usize, "competing query")
            .unwrap();
        assert!(source.into_row_ids(&memory).is_err());
        assert_eq!(memory.used(), held.bytes());
        drop(held);
        assert_eq!(memory.used(), 0);
    }

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
