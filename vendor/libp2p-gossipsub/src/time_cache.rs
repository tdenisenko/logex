// Copyright 2020 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Time-based caches with fixed insertion deadlines for gossipsub state.

use std::{
    collections::{
        hash_map::{
            self,
            Entry::{Occupied, Vacant},
        },
        VecDeque,
    },
    time::Duration,
};

use fnv::FnvHashMap;
use web_time::Instant;

struct ExpiringElement<Element> {
    /// The element that expires
    element: Element,
    /// The expire time.
    expires: Instant,
}

pub(crate) struct TimeCache<Key, Value> {
    /// Mapping a key to its value and fixed insertion deadline.
    map: FnvHashMap<Key, ExpiringElement<Value>>,
    /// An ordered list of keys by expires time.
    list: VecDeque<ExpiringElement<Key>>,
    /// The time elements remain in the cache.
    ttl: Duration,
}

pub(crate) struct OccupiedEntry<'a, K, V> {
    entry: hash_map::OccupiedEntry<'a, K, ExpiringElement<V>>,
}

impl<'a, K, V> OccupiedEntry<'a, K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub(crate) fn into_mut(self) -> &'a mut V {
        &mut self.entry.into_mut().element
    }
}

pub(crate) struct VacantEntry<'a, K, V> {
    expiration: Instant,
    entry: hash_map::VacantEntry<'a, K, ExpiringElement<V>>,
    list: &'a mut VecDeque<ExpiringElement<K>>,
}

impl<'a, K, V> VacantEntry<'a, K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub(crate) fn insert(self, value: V) -> &'a mut V {
        self.list.push_back(ExpiringElement {
            element: self.entry.key().clone(),
            expires: self.expiration,
        });
        &mut self
            .entry
            .insert(ExpiringElement {
                element: value,
                expires: self.expiration,
            })
            .element
    }
}

pub(crate) enum Entry<'a, K: 'a, V: 'a> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: 'a, V: 'a> Entry<'a, K, V>
where
    K: Eq + std::hash::Hash + Clone,
{
    pub(crate) fn or_default(self) -> &'a mut V
    where
        V: Default,
    {
        match self {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(V::default()),
        }
    }
}

impl<Key, Value> TimeCache<Key, Value>
where
    Key: Eq + std::hash::Hash + Clone,
{
    pub(crate) fn new(ttl: Duration) -> Self {
        TimeCache {
            map: FnvHashMap::default(),
            list: VecDeque::new(),
            ttl,
        }
    }

    fn remove_expired_keys(&mut self, now: Instant) {
        self.remove_expired_keys_with(now, |_| {});
    }

    fn remove_expired_keys_with(&mut self, now: Instant, mut removed: impl FnMut(Value)) {
        while let Some(element) = self.list.pop_front() {
            if element.expires > now {
                self.list.push_front(element);
                break;
            }
            if let Occupied(entry) = self.map.entry(element.element.clone()) {
                if entry.get().expires <= now {
                    removed(entry.remove().element);
                }
            }
        }
    }

    pub(crate) fn entry(&mut self, key: Key) -> Entry<'_, Key, Value> {
        self.entry_at(key, Instant::now())
    }

    fn entry_at(&mut self, key: Key, now: Instant) -> Entry<'_, Key, Value> {
        self.remove_expired_keys(now);
        match self.map.entry(key) {
            Occupied(entry) => Entry::Occupied(OccupiedEntry { entry }),
            Vacant(entry) => {
                let expiration = now.checked_add(self.ttl).unwrap_or_else(|| {
                    tracing::error!("invalid time cache ttl");
                    now
                });
                Entry::Vacant(VacantEntry {
                    expiration,
                    entry,
                    list: &mut self.list,
                })
            }
        }
    }

    /// Empties the entire cache.
    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.map.clear();
        self.list.clear();
    }

    pub(crate) fn contains_key(&self, key: &Key) -> bool {
        self.contains_key_at(key, Instant::now())
    }

    fn contains_key_at(&self, key: &Key, now: Instant) -> bool {
        self.map.get(key).is_some_and(|entry| entry.expires > now)
    }
}

/// Admission never evicts a live entry or extends its original deadline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CacheAdmission {
    New,
    Duplicate,
    Full,
}

pub(crate) struct DuplicateCache<Key> {
    cache: TimeCache<Key, usize>,
    max_entries: usize,
    max_bytes: usize,
    retained_bytes: usize,
    key_bytes: fn(&Key) -> usize,
}

impl<Key> DuplicateCache<Key>
where
    Key: Eq + std::hash::Hash + Clone,
{
    #[cfg(test)]
    pub(crate) fn new(ttl: Duration) -> Self {
        Self::with_limits(ttl, usize::MAX, usize::MAX, |_| 0)
    }

    /// `key_bytes` measures all retained key storage, including the ordered-list clone.
    /// It must account for the actual moved key and its clone. Admission for a
    /// borrowed key may overestimate a subsequent cloned insertion.
    pub(crate) fn with_limits(
        ttl: Duration,
        max_entries: usize,
        max_bytes: usize,
        key_bytes: fn(&Key) -> usize,
    ) -> Self {
        Self {
            cache: TimeCache::new(ttl),
            max_entries,
            max_bytes,
            retained_bytes: 0,
            key_bytes,
        }
    }

    /// Checks admission after pruning expired entries, without reserving space.
    pub(crate) fn admission(&mut self, key: &Key) -> CacheAdmission {
        self.admission_at(key, Instant::now()).0
    }

    fn admission_at(&mut self, key: &Key, now: Instant) -> (CacheAdmission, usize) {
        self.prune_expired(now);
        if self.cache.map.contains_key(key) {
            return (CacheAdmission::Duplicate, 0);
        }
        if self.cache.map.len() >= self.max_entries {
            return (CacheAdmission::Full, 0);
        }
        let bytes = (self.key_bytes)(key);
        if bytes > self.max_bytes - self.retained_bytes {
            return (CacheAdmission::Full, 0);
        }
        (CacheAdmission::New, bytes)
    }

    pub(crate) fn try_insert(&mut self, key: Key) -> CacheAdmission {
        self.try_insert_at(key, Instant::now())
    }

    fn try_insert_at(&mut self, key: Key, now: Instant) -> CacheAdmission {
        let (admission, bytes) = self.admission_at(&key, now);
        if admission == CacheAdmission::New {
            if let Entry::Vacant(entry) = self.cache.entry_at(key, now) {
                entry.insert(bytes);
                // Admission checked the remaining capacity without overflowing.
                self.retained_bytes += bytes;
            }
        }
        admission
    }

    #[cfg(test)]
    pub(crate) fn insert(&mut self, key: Key) -> bool {
        self.try_insert(key) == CacheAdmission::New
    }

    /// Release expired entries even when no new messages are inserted.
    pub(crate) fn prune_expired(&mut self, now: Instant) {
        let retained_bytes = &mut self.retained_bytes;
        self.cache.remove_expired_keys_with(now, |bytes| {
            *retained_bytes -= bytes;
        });
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.cache.clear();
        self.retained_bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn insert_at(&mut self, key: Key, now: Instant) -> bool {
        self.try_insert_at(key, now) == CacheAdmission::New
    }

    #[cfg(test)]
    pub(crate) fn retained_len(&self) -> usize {
        assert_eq!(self.cache.map.len(), self.cache.list.len());
        self.cache.map.len()
    }

    pub(crate) fn contains(&self, key: &Key) -> bool {
        self.cache.contains_key(key)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn cache_added_entries_exist() {
        let mut cache = DuplicateCache::new(Duration::from_secs(10));

        cache.insert("t");
        cache.insert("e");

        // Should report that 't' and 't' already exists
        assert!(!cache.insert("t"));
        assert!(!cache.insert("e"));
    }

    #[test]
    fn cache_entries_expire() {
        let mut cache = DuplicateCache::new(Duration::from_millis(100));

        cache.insert("t");
        assert!(!cache.insert("t"));
        cache.insert("e");
        // assert!(!cache.insert("t"));
        assert!(!cache.insert("e"));
        // sleep until cache expiry
        std::thread::sleep(Duration::from_millis(101));
        // add another element to clear previous cache
        cache.insert("s");

        // should be removed from the cache
        assert!(cache.insert("t"));
    }
}

#[cfg(test)]
mod logex_expiry_tests {
    use super::*;

    #[test]
    fn logex_expiry_lookup_preserves_first_insertion_deadline() {
        let ttl = Duration::from_secs(10);
        let start = Instant::now();
        let mut cache = DuplicateCache::new(ttl);
        assert!(cache.insert_at([1_u8; 20], start));
        assert!(!cache.insert_at([1_u8; 20], start + Duration::from_secs(5)));
        assert_eq!(cache.retained_len(), 1);
        assert!(cache
            .cache
            .contains_key_at(&[1; 20], start + ttl - Duration::from_nanos(1)));
        assert!(!cache.cache.contains_key_at(&[1; 20], start + ttl));
        assert!(cache.insert_at([1; 20], start + ttl));
        assert_eq!(cache.retained_len(), 1);
    }
}

#[cfg(test)]
mod logex_capacity_tests {
    use super::*;

    #[test]
    fn logex_entry_bound_retains_full_cache_and_duplicate_deadlines() {
        let now = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut cache = DuplicateCache::with_limits(ttl, 1, 8, |_| 8);
        assert_eq!(cache.try_insert_at(1, now), CacheAdmission::New);
        assert_eq!(cache.try_insert_at(2, now), CacheAdmission::Full);
        assert_eq!(
            cache.try_insert_at(1, now + ttl / 2),
            CacheAdmission::Duplicate
        );
        assert_eq!(cache.retained_len(), 1);
        assert_eq!(cache.retained_bytes, 8);
        assert!(cache
            .cache
            .contains_key_at(&1, now + ttl - Duration::from_nanos(1)));
        assert_eq!(cache.try_insert_at(2, now + ttl), CacheAdmission::New);
        assert_eq!(cache.retained_len(), 1);
        assert_eq!(cache.retained_bytes, 8);
    }

    #[test]
    fn logex_byte_bound_expiry_reuse_and_idle_prune() {
        let now = Instant::now();
        let ttl = Duration::from_secs(10);
        let mut cache = DuplicateCache::with_limits(ttl, 10, 6, |key: &String| key.len() * 2);
        assert_eq!(cache.try_insert_at("a".into(), now), CacheAdmission::New);
        assert_eq!(
            cache.try_insert_at("bc".into(), now + ttl / 2),
            CacheAdmission::New
        );
        assert_eq!(cache.retained_bytes, 6);
        assert_eq!(
            cache.try_insert_at("d".into(), now + ttl / 2),
            CacheAdmission::Full
        );
        cache.prune_expired(now + ttl);
        assert_eq!(cache.retained_bytes, 4);
        assert_eq!(
            cache.try_insert_at("d".into(), now + ttl),
            CacheAdmission::New
        );
        assert_eq!(cache.retained_bytes, 6);
        cache.prune_expired(now + ttl * 2);
        assert_eq!(cache.retained_len(), 0);
        assert_eq!(cache.retained_bytes, 0);
        cache.prune_expired(now + ttl * 3);
        assert_eq!(cache.retained_bytes, 0);
    }

    #[test]
    fn logex_byte_accounting_cannot_overflow_and_clear_releases_capacity() {
        let now = Instant::now();
        let mut cache =
            DuplicateCache::with_limits(Duration::from_secs(10), 3, usize::MAX, |key: &usize| *key);
        assert_eq!(cache.try_insert_at(usize::MAX, now), CacheAdmission::New);
        assert_eq!(cache.try_insert_at(1, now), CacheAdmission::Full);
        assert_eq!(
            cache.try_insert_at(usize::MAX, now),
            CacheAdmission::Duplicate
        );
        assert_eq!(cache.retained_bytes, usize::MAX);
        cache.clear();
        assert_eq!(cache.retained_len(), 0);
        assert_eq!(cache.retained_bytes, 0);
        assert_eq!(cache.try_insert_at(1, now), CacheAdmission::New);
    }

    #[test]
    fn logex_admission_does_not_reserve_capacity_or_admit_oversized_keys() {
        let now = Instant::now();
        let mut cache =
            DuplicateCache::with_limits(Duration::from_secs(10), 1, 2, |key: &usize| *key);
        assert_eq!(cache.admission(&2), CacheAdmission::New);
        assert_eq!(cache.retained_len(), 0);
        assert_eq!(cache.retained_bytes, 0);
        assert_eq!(cache.try_insert_at(3, now), CacheAdmission::Full);
        assert_eq!(cache.try_insert_at(2, now), CacheAdmission::New);
        assert_eq!(cache.admission(&2), CacheAdmission::Duplicate);
        assert_eq!(cache.admission(&1), CacheAdmission::Full);
        let mut disabled =
            DuplicateCache::with_limits(Duration::from_secs(10), 0, usize::MAX, |_| 0);
        assert_eq!(disabled.try_insert_at(0, now), CacheAdmission::Full);
    }
}
