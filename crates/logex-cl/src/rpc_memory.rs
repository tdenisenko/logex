//! Nonwaiting reservations for transient RPC response buffers.
use std::collections::TryReserveError;
use std::io;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

// Shared transient byte limits: 256 MiB for Beacon responses and a separate
// 8 MiB for control/light-client responses; each response retains at most 64 MiB
// decoded. These account for payload buffers, not all process allocations.
const BEACON_BYTES: usize = 256 * 1024 * 1024;
const CONTROL_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const RESPONSE_DECODED_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RpcMemoryError {
    #[error("RPC response memory exhausted: requested {requested}, used {used}, limit {limit}")]
    Capacity {
        requested: usize,
        used: usize,
        limit: usize,
    },
    #[error("RPC response exceeds decoded-byte limit {limit}")]
    ResponseLimit { limit: usize },
    #[error("RPC response allocation failed: {message}")]
    Allocation { message: String },
}

pub(crate) fn from_io(error: &io::Error) -> Option<&RpcMemoryError> {
    error.get_ref()?.downcast_ref()
}

pub(crate) fn response_limit(limit: usize) -> io::Error {
    io::Error::other(RpcMemoryError::ResponseLimit { limit })
}

pub(crate) fn allocation_error(error: TryReserveError) -> io::Error {
    io::Error::other(RpcMemoryError::Allocation {
        message: error.to_string(),
    })
}

#[derive(Clone, Debug)]
pub(crate) struct RpcResponseBudgets {
    beacon: RpcMemoryPool,
    control: RpcMemoryPool,
    max_decoded: usize,
}
impl Default for RpcResponseBudgets {
    fn default() -> Self {
        Self::with_limits(BEACON_BYTES, CONTROL_BYTES, RESPONSE_DECODED_BYTES)
    }
}
impl RpcResponseBudgets {
    pub(crate) fn with_limits(
        beacon_bytes: usize,
        control_bytes: usize,
        max_decoded: usize,
    ) -> Self {
        Self {
            beacon: RpcMemoryPool::new(beacon_bytes),
            control: RpcMemoryPool::new(control_bytes),
            max_decoded,
        }
    }
    pub(crate) fn pool(&self, beacon: bool) -> RpcMemoryPool {
        if beacon {
            self.beacon.clone()
        } else {
            self.control.clone()
        }
    }
    pub(crate) fn max_decoded(&self) -> usize {
        self.max_decoded
    }
}

#[derive(Debug)]
struct PoolInner {
    used: AtomicUsize,
    max: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct RpcMemoryPool(Arc<PoolInner>);
impl RpcMemoryPool {
    fn new(max: usize) -> Self {
        Self(Arc::new(PoolInner {
            used: AtomicUsize::new(0),
            max,
        }))
    }
    pub(crate) fn try_reserve(&self, bytes: usize) -> io::Result<RpcReservation> {
        // This atomic only tracks ownership counts; it does not publish buffer
        // contents. Relaxed ordering still serializes admission and release.
        let mut used = self.0.used.load(Ordering::Relaxed);
        loop {
            // Subtract first: arbitrary requested sizes must not overflow.
            if bytes > self.0.max - used {
                return Err(io::Error::other(RpcMemoryError::Capacity {
                    requested: bytes,
                    used,
                    limit: self.0.max,
                }));
            }
            match self.0.used.compare_exchange_weak(
                used,
                used + bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(RpcReservation {
                        pool: self.clone(),
                        bytes,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }
    #[cfg(test)]
    pub(crate) fn used(&self) -> usize {
        self.0.used.load(Ordering::Relaxed)
    }
}

/// Unique accounting owner. Drop only after the corresponding buffers are freed.
#[derive(Debug)]
pub(crate) struct RpcReservation {
    pool: RpcMemoryPool,
    bytes: usize,
}
impl RpcReservation {
    pub(crate) fn shrink_to(&mut self, retained_bytes: usize) {
        assert!(
            retained_bytes <= self.bytes,
            "RPC reservation cannot grow by shrinking"
        );
        let released = self.bytes - retained_bytes;
        self.bytes = retained_bytes;
        self.pool.0.used.fetch_sub(released, Ordering::Relaxed);
    }
}
impl Drop for RpcReservation {
    fn drop(&mut self) {
        self.pool.0.used.fetch_sub(self.bytes, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn shared_budgets_keep_pools_separate_and_release_surplus() {
        let budgets = RpcResponseBudgets::with_limits(8, 3, 6);
        let cloned = budgets.clone();
        assert_eq!(cloned.max_decoded(), 6);
        let beacon = budgets.pool(true);
        let mut held = beacon.try_reserve(8).unwrap();
        assert!(cloned.pool(true).try_reserve(1).is_err());
        let control = cloned.pool(false).try_reserve(3).unwrap();
        held.shrink_to(5);
        assert_eq!(beacon.used(), 5);
        let rest = cloned.pool(true).try_reserve(3).unwrap();
        drop(held);
        assert_eq!(beacon.used(), 3);
        drop(rest);
        drop(control);
        assert_eq!(beacon.used(), 0);
        assert_eq!(budgets.pool(false).used(), 0);
    }

    #[test]
    fn rejected_reservations_and_cancelled_owner_do_not_leak() {
        let pool = RpcMemoryPool::new(4);
        let held = pool.try_reserve(3).unwrap();
        let error = pool.try_reserve(usize::MAX).unwrap_err();
        assert!(matches!(
            from_io(&error),
            Some(RpcMemoryError::Capacity {
                requested: usize::MAX,
                used: 3,
                limit: 4,
            })
        ));
        let mut cancelled = Box::pin(async move {
            let _held = held;
            std::future::pending::<()>().await;
        });
        let waker = futures::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        assert!(std::future::Future::poll(cancelled.as_mut(), &mut context).is_pending());
        drop(cancelled);
        assert_eq!(pool.used(), 0);
        let zero = pool.try_reserve(0).unwrap();
        drop(zero);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn concurrent_admission_holds_exactly_one_full_reservation() {
        let pool = RpcMemoryPool::new(7);
        let barrier = Arc::new(Barrier::new(8));
        let winners = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| {
                    let pool = pool.clone();
                    let barrier = barrier.clone();
                    scope.spawn(move || {
                        barrier.wait();
                        let held = pool.try_reserve(7);
                        barrier.wait(); // No winner releases before every attempt finishes.
                        held.is_ok()
                    })
                })
                .collect();
            threads
                .into_iter()
                .map(|thread| thread.join().unwrap())
                .filter(|won| *won)
                .count()
        });
        assert_eq!(winners, 1);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn growth_attempt_panics_without_losing_original_reservation() {
        let pool = RpcMemoryPool::new(2);
        let mut held = pool.try_reserve(2).unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                held.shrink_to(3);
            }))
            .is_err()
        );
        assert_eq!(pool.used(), 2);
        drop(held);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn local_errors_remain_typed_without_allocating_large_buffers() {
        assert!(matches!(
            from_io(&response_limit(9)),
            Some(RpcMemoryError::ResponseLimit { limit: 9 })
        ));
        let error = Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err();
        assert!(matches!(
            from_io(&allocation_error(error)),
            Some(RpcMemoryError::Allocation { .. })
        ));
        assert!(from_io(&io::Error::from(io::ErrorKind::UnexpectedEof)).is_none());
    }
}
