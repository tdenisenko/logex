//! Owned query buffers whose accounting follows the backing allocation.
use std::io;
use std::ops::{Deref, DerefMut};

use crate::{QueryMemoryBudget, QueryMemoryError, QueryMemoryReservation};

/// Account the vector's backing capacity for as long as it remains owned.
/// Allocations inside individual elements require their own accounting owners.
/// Slice access cannot grow the backing vector without reserving more capacity.
#[derive(Debug)]
pub struct QueryBuffer<T> {
    values: Vec<T>,
    reservation: Option<QueryMemoryReservation>,
}

impl<T> QueryBuffer<T> {
    pub fn try_with_capacity(
        capacity: usize,
        memory: Option<&QueryMemoryBudget>,
        stage: &'static str,
    ) -> io::Result<Self> {
        let bytes =
            QueryMemoryBudget::array_bytes::<T>(capacity, stage).map_err(io::Error::other)?;
        let reservation = memory
            .map(|budget| budget.reserve(bytes, stage))
            .transpose()
            .map_err(io::Error::other)?;
        // Declare the allocation after its reservation so an allocation failure
        // unwinds the backing storage before releasing its charge.
        let mut values = Vec::new();
        values
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        Self::from_reserved(values, reservation)
    }

    /// Preserve an existing path that does not participate in query accounting.
    pub fn unaccounted(values: Vec<T>) -> Self {
        Self {
            values,
            reservation: None,
        }
    }

    /// Transfer a vector alongside its prior allocation reservation. Reconcile
    /// its actual capacity while both remain owned, releasing both on rejection.
    /// This does not substitute for reserving before the original allocation.
    pub fn from_reserved(
        values: Vec<T>,
        reservation: Option<QueryMemoryReservation>,
    ) -> io::Result<Self> {
        let mut owned = Self {
            values,
            reservation,
        };
        if let Some(reservation) = &mut owned.reservation {
            let actual =
                QueryMemoryBudget::array_bytes::<T>(owned.values.capacity(), reservation.stage())
                    .map_err(io::Error::other)?;
            let reserved = reservation.bytes();
            if actual as u128 > reserved {
                let additional = actual - usize::try_from(reserved).map_err(io::Error::other)?;
                let used = reservation.budget().used();
                reservation.record_existing(additional);
                if reservation.budget().used() > reservation.budget().limit() as u128 {
                    return Err(io::Error::other(QueryMemoryError::CapacityExceeded {
                        requested: additional,
                        used,
                        limit: reservation.budget().limit(),
                        stage: reservation.stage(),
                    }));
                }
            } else if reserved > actual as u128 {
                let extra = usize::try_from(reserved - actual as u128).map_err(io::Error::other)?;
                reservation.shrink(extra).map_err(io::Error::other)?;
            }
        }
        Ok(owned)
    }

    /// Transfer both owners, for example when handing storage into Arrow.
    /// The reservation must remain live until the backing allocation is released
    /// or transferred to another owner that carries the same reservation.
    pub fn into_parts(self) -> (Vec<T>, Option<QueryMemoryReservation>) {
        (self.values, self.reservation)
    }
    pub fn capacity(&self) -> usize {
        self.values.capacity()
    }
    pub fn clear(&mut self) {
        self.values.clear();
    }
    pub fn truncate(&mut self, len: usize) {
        self.values.truncate(len);
    }
    /// Remove a value while retaining the backing capacity and its charge.
    pub fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }

    #[inline]
    pub fn try_reserve(&mut self, additional: usize) -> io::Result<()> {
        let stage = self
            .reservation
            .as_ref()
            .map_or("query buffer", QueryMemoryReservation::stage);
        let required = self
            .values
            .len()
            .checked_add(additional)
            .ok_or_else(|| io::Error::other(QueryMemoryError::SizeOverflow { stage }))?;
        if required <= self.values.capacity() {
            return Ok(());
        }
        if self.reservation.is_none() {
            return self
                .values
                .try_reserve(additional)
                .map_err(io::Error::other);
        }
        let target = self
            .values
            .capacity()
            .checked_mul(2)
            .unwrap_or(required)
            .max(required);
        let memory = self
            .reservation
            .as_ref()
            .map(|reservation| reservation.budget().clone());
        let mut replacement = Self::try_with_capacity(target, memory.as_ref(), stage)?;
        replacement.values.append(&mut self.values);
        // Both backing allocations are charged through the move. Dropping the
        // replaced object frees its old capacity before releasing its charge.
        let old = std::mem::replace(self, replacement);
        drop(old);
        Ok(())
    }

    #[inline]
    pub fn try_push(&mut self, value: T) -> io::Result<()> {
        self.try_reserve(1)?;
        self.values.push(value);
        Ok(())
    }

    pub fn try_extend(&mut self, values: impl IntoIterator<Item = T>) -> io::Result<()> {
        let values = values.into_iter();
        self.try_reserve(values.size_hint().0)?;
        for value in values {
            self.try_push(value)?;
        }
        Ok(())
    }
}

impl QueryBuffer<u8> {
    /// Share immutable bytes without detaching their allocation reservation.
    /// Clones and slices retain the whole backing capacity until the last alias
    /// is released. The small reference-counted owner is control overhead.
    pub fn into_bytes(self) -> alloy_primitives::Bytes {
        if self.values.is_empty() {
            return alloy_primitives::Bytes::new();
        }
        if self.reservation.is_none() {
            return self.values.into();
        }
        bytes::Bytes::from_owner(self).into()
    }
}

impl<T: Clone> QueryBuffer<T> {
    pub fn try_extend_from_slice(&mut self, values: &[T]) -> io::Result<()> {
        self.try_reserve(values.len())?;
        self.values.extend_from_slice(values);
        Ok(())
    }
    pub fn try_resize(&mut self, len: usize, value: T) -> io::Result<()> {
        self.try_reserve(len.saturating_sub(self.values.len()))?;
        self.values.resize(len, value);
        Ok(())
    }
}

impl<T> Deref for QueryBuffer<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        &self.values
    }
}
impl<T> DerefMut for QueryBuffer<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        &mut self.values
    }
}
impl<T> AsRef<[T]> for QueryBuffer<T> {
    fn as_ref(&self) -> &[T] {
        &self.values
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryMemoryLimit;

    #[test]
    fn growth_accounts_old_and_new_and_failed_growth_preserves_values() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16).unwrap());
        let mut values = QueryBuffer::<u64>::try_with_capacity(1, Some(&memory), "test").unwrap();
        values.try_push(7).unwrap();
        assert!(values.try_push(9).is_err());
        assert_eq!(&*values, &[7]);
        assert_eq!(memory.used(), 8);
        values.clear();
        assert_eq!(memory.used(), 8);
        drop(values);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn transferred_allocations_reconcile_and_release_on_rejection() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(32).unwrap());
        let reservation = memory.reserve(32, "test").unwrap();
        let values =
            QueryBuffer::from_reserved(Vec::<u64>::with_capacity(1), Some(reservation)).unwrap();
        assert_eq!(memory.used(), (values.capacity() * 8) as u128);
        drop(values);
        assert_eq!(memory.used(), 0);
        let reservation = memory.reserve(1, "test").unwrap();
        let values =
            QueryBuffer::from_reserved(Vec::<u64>::with_capacity(2), Some(reservation)).unwrap();
        assert_eq!(memory.used(), (values.capacity() * 8) as u128);
        drop(values);
        let reservation = memory.reserve(1, "test").unwrap();
        let error = QueryBuffer::from_reserved(Vec::<u64>::with_capacity(8), Some(reservation))
            .unwrap_err();
        assert!(error.get_ref().unwrap().is::<QueryMemoryError>());
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn growth_transfer_and_zero_sized_values() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(32).unwrap());
        let mut values = QueryBuffer::<u64>::try_with_capacity(1, Some(&memory), "test").unwrap();
        values.try_extend([1, 2]).unwrap();
        assert_eq!(memory.used(), 16);
        let (data, reservation) = values.into_parts();
        assert_eq!(data, [1, 2]);
        drop(data);
        drop(reservation);
        assert_eq!(memory.used(), 0);
        let mut empty = QueryBuffer::try_with_capacity(4, Some(&memory), "zst").unwrap();
        empty.try_extend([(), (), ()]).unwrap();
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn immutable_bytes_keep_capacity_until_the_last_slice_drops() {
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(64).unwrap());
        let mut buffer = QueryBuffer::try_with_capacity(32, Some(&memory), "payload").unwrap();
        buffer.try_extend_from_slice(b"payload").unwrap();
        let pointer = buffer.as_ptr();
        let bytes = buffer.into_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        let clone = bytes.clone();
        let slice = bytes.slice(1..3);
        drop(bytes);
        drop(clone);
        assert_eq!(memory.used(), 32);
        assert_eq!(&slice[..], b"ay");
        drop(slice);
        assert_eq!(memory.used(), 0);

        let empty = QueryBuffer::<u8>::try_with_capacity(32, Some(&memory), "empty")
            .unwrap()
            .into_bytes();
        assert!(empty.is_empty());
        assert_eq!(memory.used(), 0);

        let plain = QueryBuffer::unaccounted(b"legacy".to_vec());
        let pointer = plain.as_ptr();
        let bytes = plain.into_bytes();
        assert_eq!(bytes.as_ptr(), pointer);
        assert_eq!(&bytes[..], b"legacy");
    }
}
