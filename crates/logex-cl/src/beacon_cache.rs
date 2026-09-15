//! Optional serving bodies; verified ancestry is retained independently.
use crate::rpc::RawRpcResponse;
use alloy_primitives::B256;
use std::collections::{HashMap, VecDeque};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

const MAX_RESIDENT_BODY_BYTES: usize = 128 * 1024 * 1024;
const MAX_RESIDENT_BODIES: usize = 4096;

#[derive(Default)]
struct Resident {
    bytes: AtomicUsize,
    entries: AtomicUsize,
}
struct Body {
    raw: RawRpcResponse,
    resident: Arc<Resident>,
}
impl Drop for Body {
    fn drop(&mut self) {
        let bytes = std::mem::take(&mut self.raw.bytes);
        let capacity = bytes.capacity();
        drop(bytes);
        self.resident.bytes.fetch_sub(capacity, Ordering::Relaxed);
        self.resident.entries.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Shared immutable serving body. Its allocation remains charged until the last owner drops.
#[derive(Clone)]
pub struct CachedBeaconPayload(Arc<Body>);
impl CachedBeaconPayload {
    pub fn as_raw(&self) -> &RawRpcResponse {
        &self.0.raw
    }
}
impl AsRef<RawRpcResponse> for CachedBeaconPayload {
    fn as_ref(&self) -> &RawRpcResponse {
        self.as_raw()
    }
}
impl std::fmt::Debug for CachedBeaconPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedBeaconPayload")
            .field("bytes", &self.0.raw.bytes.len())
            .field("context", &self.0.raw.context_bytes)
            .finish()
    }
}
impl PartialEq for CachedBeaconPayload {
    fn eq(&self, other: &Self) -> bool {
        self.as_raw() == other.as_raw()
    }
}
impl Eq for CachedBeaconPayload {}

pub(crate) struct BeaconPayloadCache {
    payloads: HashMap<B256, CachedBeaconPayload>,
    order: VecDeque<B256>,
    resident: Arc<Resident>,
    max_bytes: usize,
    max_entries: usize,
}
impl Default for BeaconPayloadCache {
    fn default() -> Self {
        Self::with_limits(MAX_RESIDENT_BODY_BYTES, MAX_RESIDENT_BODIES)
    }
}
impl BeaconPayloadCache {
    pub(crate) fn with_limits(max_bytes: usize, max_entries: usize) -> Self {
        Self {
            payloads: HashMap::new(),
            order: VecDeque::new(),
            resident: Arc::default(),
            max_bytes,
            max_entries,
        }
    }
    pub(crate) fn get(&self, root: &B256) -> Option<&CachedBeaconPayload> {
        self.payloads.get(root)
    }
    pub(crate) fn insert(&mut self, root: B256, raw: RawRpcResponse) {
        if self.payloads.contains_key(&root)
            || raw.bytes.capacity() > self.max_bytes
            || self.max_entries == 0
        {
            return;
        }
        let fits = |resident: &Resident| {
            resident.bytes.load(Ordering::Relaxed) <= self.max_bytes - raw.bytes.capacity()
                && resident.entries.load(Ordering::Relaxed) < self.max_entries
        };
        while !fits(&self.resident) {
            let Some(oldest) = self.order.pop_front() else {
                return;
            };
            self.payloads.remove(&oldest);
        }
        // Admission is single-owner; other threads can only release reservations.
        self.resident
            .bytes
            .fetch_add(raw.bytes.capacity(), Ordering::Relaxed);
        self.resident.entries.fetch_add(1, Ordering::Relaxed);
        self.payloads.insert(
            root,
            CachedBeaconPayload(Arc::new(Body {
                raw,
                resident: self.resident.clone(),
            })),
        );
        self.order.push_back(root);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn raw(capacity: usize) -> RawRpcResponse {
        RawRpcResponse {
            context_bytes: None,
            bytes: Vec::with_capacity(capacity),
        }
    }
    #[test]
    fn resident_charge_tracks_capacity_and_last_reference() {
        let mut cache = BeaconPayloadCache::with_limits(8, 2);
        let first = raw(8);
        let capacity = first.bytes.capacity();
        cache.insert(B256::ZERO, first);
        let shared = cache.get(&B256::ZERO).unwrap().clone();
        assert_eq!(cache.resident.bytes.load(Ordering::Relaxed), capacity);
        cache.insert(B256::repeat_byte(1), raw(1));
        assert!(cache.payloads.is_empty());
        assert_eq!(cache.resident.bytes.load(Ordering::Relaxed), capacity);
        drop(shared);
        assert_eq!(cache.resident.bytes.load(Ordering::Relaxed), 0);
        assert_eq!(cache.resident.entries.load(Ordering::Relaxed), 0);
        cache.insert(B256::repeat_byte(1), raw(1));
        assert!(cache.get(&B256::repeat_byte(1)).is_some());
    }
    #[test]
    fn queued_body_outlives_cache_and_releases_only_after_last_clone() {
        let mut cache = BeaconPayloadCache::with_limits(4, 1);
        cache.insert(B256::ZERO, raw(4));
        let held = cache.get(&B256::ZERO).unwrap().clone();
        let second = held.clone();
        let resident = cache.resident.clone();
        drop(cache);
        drop(held);
        assert_eq!(resident.bytes.load(Ordering::Relaxed), 4);
        assert_eq!(resident.entries.load(Ordering::Relaxed), 1);
        drop(second);
        assert_eq!(resident.bytes.load(Ordering::Relaxed), 0);
        assert_eq!(resident.entries.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn fifo_duplicates_and_oversize_do_not_grow_ownership() {
        let mut cache = BeaconPayloadCache::with_limits(2, 2);
        for byte in [1, 2, 1] {
            cache.insert(B256::repeat_byte(byte), raw(1));
        }
        assert_eq!(cache.order.len(), 2);
        assert_eq!(cache.resident.bytes.load(Ordering::Relaxed), 2);
        cache.insert(B256::repeat_byte(4), raw(3));
        assert_eq!(cache.order.len(), 2);
        cache.insert(B256::repeat_byte(3), raw(1));
        assert!(cache.get(&B256::repeat_byte(1)).is_none());
        assert!(cache.get(&B256::repeat_byte(2)).is_some());
    }
    #[test]
    fn zero_capacity_references_still_consume_entry_quota() {
        let mut cache = BeaconPayloadCache::with_limits(0, 1);
        cache.insert(B256::ZERO, raw(0));
        let held = cache.get(&B256::ZERO).unwrap().clone();
        cache.insert(B256::repeat_byte(1), raw(0));
        assert!(cache.payloads.is_empty());
        assert_eq!(cache.resident.entries.load(Ordering::Relaxed), 1);
        drop(held);
        cache.insert(B256::repeat_byte(1), raw(0));
        assert_eq!(cache.payloads.len(), 1);
        assert_eq!(cache.order.len(), 1);
    }
}
