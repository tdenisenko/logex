//! Cooperative accounting for query-owned allocations, not a process RSS limit.
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct QueryMemoryLimit(usize);

impl QueryMemoryLimit {
    pub const DEFAULT_BYTES: usize = 1 << 30;

    pub fn new(bytes: usize) -> Result<Self, String> {
        if bytes == 0 || bytes > isize::MAX as usize {
            return Err(format!(
                "query memory bytes must be between 1 and {}",
                isize::MAX
            ));
        }
        Ok(Self(bytes))
    }

    pub fn get(self) -> usize {
        self.0
    }
}

impl Default for QueryMemoryLimit {
    fn default() -> Self {
        Self(Self::DEFAULT_BYTES)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryMemoryError {
    CapacityExceeded {
        requested: usize,
        used: u128,
        limit: usize,
        stage: &'static str,
    },
    InvalidRelease {
        requested: usize,
        reserved: u128,
    },
    SizeOverflow {
        stage: &'static str,
    },
}

impl std::fmt::Display for QueryMemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CapacityExceeded {
                requested,
                used,
                limit,
                stage,
            } => write!(
                f,
                "query memory capacity exceeded at {stage}: requested {requested} bytes, {used} accounted, limit {limit}"
            ),
            Self::InvalidRelease {
                requested,
                reserved,
            } => write!(
                f,
                "cannot release {requested} query memory bytes from reservation of {reserved}"
            ),
            Self::SizeOverflow { stage } => write!(f, "query memory size overflow at {stage}"),
        }
    }
}
impl std::error::Error for QueryMemoryError {}

#[derive(Debug)]
struct State {
    limit: QueryMemoryLimit,
    used: Mutex<u128>,
}

/// Clones share a single ledger. Construct once for the serving application.
#[derive(Clone, Debug)]
pub struct QueryMemoryBudget(Arc<State>);

impl QueryMemoryBudget {
    pub fn new(limit: QueryMemoryLimit) -> Self {
        Self(Arc::new(State {
            limit,
            used: Mutex::new(0),
        }))
    }
    fn ledger(&self) -> MutexGuard<'_, u128> {
        self.0
            .used
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
    pub fn limit(&self) -> usize {
        self.0.limit.get()
    }
    pub fn used(&self) -> u128 {
        *self.ledger()
    }
    pub fn reserve(
        &self,
        bytes: usize,
        stage: &'static str,
    ) -> Result<QueryMemoryReservation, QueryMemoryError> {
        let mut reservation = QueryMemoryReservation {
            budget: self.clone(),
            bytes: 0,
            stage,
        };
        reservation.try_grow(bytes)?;
        Ok(reservation)
    }
    pub fn array_bytes<T>(len: usize, stage: &'static str) -> Result<usize, QueryMemoryError> {
        len.checked_mul(std::mem::size_of::<T>())
            .ok_or(QueryMemoryError::SizeOverflow { stage })
    }
}

/// Unique mutable ownership of a charge. Move/split it with the allocation; use
/// `Arc<QueryMemoryReservation>` for immutable aliases of the SAME allocation.
/// Keep this field after the allocation so the allocation is dropped first.
#[derive(Debug)]
#[must_use = "retain the reservation for as long as its allocation remains owned"]
pub struct QueryMemoryReservation {
    budget: QueryMemoryBudget,
    bytes: u128,
    stage: &'static str,
}

impl QueryMemoryReservation {
    pub(crate) fn budget(&self) -> &QueryMemoryBudget {
        &self.budget
    }
    pub(crate) fn stage(&self) -> &'static str {
        self.stage
    }
    pub fn bytes(&self) -> u128 {
        self.bytes
    }
    pub fn try_grow(&mut self, additional: usize) -> Result<(), QueryMemoryError> {
        if additional == 0 {
            return Ok(());
        }
        let mut used = self.budget.ledger();
        let next = used
            .checked_add(additional as u128)
            .filter(|next| *next <= self.budget.limit() as u128);
        let Some(next) = next else {
            return Err(QueryMemoryError::CapacityExceeded {
                requested: additional,
                used: *used,
                limit: self.budget.limit(),
                stage: self.stage,
            });
        };
        *used = next;
        self.bytes += additional as u128;
        Ok(())
    }
    /// Record memory already allocated by an infallible third-party API.
    /// This may exceed the configured limit; subsequent fallible growth fails.
    /// A wide ledger avoids usize overflow when independent live consumers
    /// account more than an address-space-sized sum. It does not authorize
    /// allocations or impose a hard allocator/RSS ceiling.
    pub fn record_existing(&mut self, additional: usize) {
        *self.budget.ledger() += additional as u128;
        self.bytes += additional as u128;
    }
    pub fn shrink(&mut self, bytes: usize) -> Result<(), QueryMemoryError> {
        if bytes as u128 > self.bytes {
            return Err(QueryMemoryError::InvalidRelease {
                requested: bytes,
                reserved: self.bytes,
            });
        }
        self.bytes -= bytes as u128;
        *self.budget.ledger() -= bytes as u128;
        Ok(())
    }
    pub fn split(&mut self, bytes: usize) -> Result<Self, QueryMemoryError> {
        if bytes as u128 > self.bytes {
            return Err(QueryMemoryError::InvalidRelease {
                requested: bytes,
                reserved: self.bytes,
            });
        }
        self.bytes -= bytes as u128;
        Ok(Self {
            budget: self.budget.clone(),
            bytes: bytes as u128,
            stage: self.stage,
        })
    }
}
impl Drop for QueryMemoryReservation {
    fn drop(&mut self) {
        *self.budget.ledger() -= self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn limits_and_checked_sizes() {
        assert!(QueryMemoryLimit::new(0).is_err());
        assert!(QueryMemoryLimit::new(isize::MAX as usize + 1).is_err());
        assert!(QueryMemoryLimit::new(isize::MAX as usize).is_ok());
        assert!(QueryMemoryBudget::array_bytes::<u64>(usize::MAX, "rows").is_err());
    }
    #[test]
    fn growth_split_aliases_and_overcommit_release_exactly() {
        let budget = QueryMemoryBudget::new(QueryMemoryLimit::new(10).unwrap());
        let mut a = budget.reserve(10, "test").unwrap();
        assert!(a.try_grow(1).is_err());
        assert_eq!(budget.used(), 10);
        assert!(a.shrink(11).is_err());
        assert!(a.split(11).is_err());
        assert_eq!(a.bytes(), 10);
        let b = Arc::new(a.split(4).unwrap());
        let alias = b.clone();
        drop(b);
        drop(a);
        assert_eq!(budget.used(), 4);
        drop(alias);
        assert_eq!(budget.used(), 0);
        let mut a = budget.reserve(0, "existing").unwrap();
        a.record_existing(usize::MAX);
        a.record_existing(usize::MAX);
        assert_eq!(budget.used(), 2 * usize::MAX as u128);
        assert!(budget.reserve(1, "new").is_err());
        assert!(budget.reserve(0, "empty").is_ok());
        a.shrink(usize::MAX).unwrap();
        drop(a);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn competing_reservations_and_unwind_return_credit() {
        let budget = QueryMemoryBudget::new(QueryMemoryLimit::new(1).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(3));
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    let budget = budget.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        let held = budget.reserve(1, "race");
                        barrier.wait();
                        held.is_ok()
                    })
                })
                .collect();
            barrier.wait();
            barrier.wait();
            assert_eq!(
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .filter(|ok| *ok)
                    .count(),
                1
            );
        });
        assert_eq!(budget.used(), 0);
        let _ = std::panic::catch_unwind(|| {
            let _held = budget.reserve(1, "unwind").unwrap();
            panic!("fixture");
        });
        assert_eq!(budget.used(), 0);
    }
}
