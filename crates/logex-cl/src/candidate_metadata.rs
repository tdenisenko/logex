//! FIFO bookkeeping for optional Beacon metadata, including released forks.
//!
//! The caller owns block/child maps and must remove every root returned by
//! `trim`. Complete an admission or owner-release batch, promote required roots
//! with `protect`, then trim before accepting another batch. Membership can
//! temporarily exceed capacity until trim; this tracker does not bound batch size.

use std::collections::{HashSet, VecDeque};

use alloy_primitives::B256;

pub(crate) struct CandidateMetadata {
    capacity: usize,
    candidates: HashSet<B256>,
    queued: HashSet<B256>,
    order: VecDeque<B256>,
}

impl CandidateMetadata {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            candidates: HashSet::new(),
            queued: HashSet::new(),
            order: VecDeque::new(),
        }
    }

    pub(crate) fn contains(&self, root: &B256) -> bool {
        self.candidates.contains(root)
    }

    /// Admit a new candidate or demote metadata whose required owners have ended.
    pub(crate) fn insert(&mut self, root: B256) {
        if !self.candidates.insert(root) {
            return;
        }
        // Reinsertion after protection gets a new FIFO position. Without this
        // step, an old tombstone could evict the new candidate prematurely.
        if !self.queued.insert(root) {
            self.order.retain(|queued| *queued != root);
        }
        self.order.push_back(root);
    }

    pub(crate) fn protect(&mut self, root: &B256) {
        self.candidates.remove(root);
    }

    pub(crate) fn trim(&mut self) -> Vec<B256> {
        let mut evicted = Vec::new();
        while self.candidates.len() > self.capacity {
            let root = self.order.pop_front().expect("candidate has a FIFO entry");
            self.queued.remove(&root);
            if self.candidates.remove(&root) {
                evicted.push(root);
            }
        }
        while self
            .order
            .front()
            .is_some_and(|root| !self.candidates.contains(root))
        {
            let root = self.order.pop_front().expect("front tombstone exists");
            self.queued.remove(&root);
        }
        // Amortize tombstone removal while bounding bookkeeping after each
        // batch. Saturation makes arbitrary caller capacities overflow-safe.
        if self.order.len() > self.capacity.saturating_mul(2) {
            self.order.retain(|root| self.candidates.contains(root));
            self.queued.retain(|root| self.candidates.contains(root));
        }
        evicted
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.candidates.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(value: u64) -> B256 {
        B256::from_slice(&{
            let mut bytes = [0; 32];
            bytes[24..].copy_from_slice(&value.to_be_bytes());
            bytes
        })
    }

    fn assert_bounds(tracker: &CandidateMetadata) {
        assert!(tracker.len() <= tracker.capacity);
        assert!(tracker.order.len() <= tracker.capacity.saturating_mul(2));
        assert_eq!(tracker.order.len(), tracker.queued.len());
        assert!(tracker.candidates.is_subset(&tracker.queued));
        assert_eq!(
            tracker.order.iter().copied().collect::<HashSet<_>>(),
            tracker.queued
        );
    }

    #[test]
    fn duplicates_and_promotions_preserve_fifo() {
        let mut tracker = CandidateMetadata::new(2);
        for value in [1, 2, 1, 3] {
            tracker.insert(root(value));
        }
        assert_eq!(tracker.len(), 3);
        assert_eq!(tracker.order.len(), 3);
        tracker.protect(&root(1));
        assert!(tracker.trim().is_empty());
        tracker.insert(root(4));
        assert_eq!(tracker.trim(), vec![root(2)]);
        assert!(!tracker.contains(&root(1)));
        assert!(!tracker.contains(&root(2)));
        assert!(tracker.contains(&root(3)));
        assert!(tracker.contains(&root(4)));
        assert_bounds(&tracker);
    }

    #[test]
    fn reinsertion_after_protection_gets_new_position() {
        let mut tracker = CandidateMetadata::new(2);
        tracker.insert(root(1));
        tracker.insert(root(2));
        tracker.protect(&root(1));
        tracker.insert(root(1));
        tracker.insert(root(3));
        assert_eq!(tracker.trim(), vec![root(2)]);
        assert_bounds(&tracker);
    }

    #[test]
    fn repeated_batches_bound_tombstones_and_membership() {
        let mut tracker = CandidateMetadata::new(2);
        for batch in 0..32 {
            for value in batch * 4..batch * 4 + 4 {
                tracker.insert(root(value));
                tracker.insert(root(value));
            }
            tracker.protect(&root(batch * 4));
            tracker.protect(&root(batch * 4 + 1));
            let evicted = tracker.trim();
            assert!(evicted.iter().all(|root| !tracker.contains(root)));
            assert!(tracker.contains(&root(batch * 4 + 2)));
            assert!(tracker.contains(&root(batch * 4 + 3)));
            assert_bounds(&tracker);
        }
        for value in 128..160 {
            tracker.insert(root(value));
            tracker.protect(&root(value));
            tracker.trim();
            assert_bounds(&tracker);
        }
    }

    #[test]
    fn zero_and_maximum_capacities_are_safe() {
        let mut zero = CandidateMetadata::new(0);
        zero.insert(root(1));
        zero.insert(root(2));
        zero.protect(&root(1));
        assert_eq!(zero.trim(), vec![root(2)]);
        assert_bounds(&zero);
        let mut unlimited = CandidateMetadata::new(usize::MAX);
        unlimited.insert(root(1));
        assert!(unlimited.trim().is_empty());
        assert_bounds(&unlimited);
    }
}
