use alloy_consensus::{BlockHeader, Header};
use alloy_primitives::B256;
use std::collections::VecDeque;

/// Tracks the recent chain head for reorg detection.
///
/// Maintains a sliding window of recent canonical headers. When a new header
/// arrives whose parent hash doesn't match the current tip, the tracker walks
/// backward to find the fork point and reports which blocks were reverted.
pub struct HeadTracker {
    /// Recent canonical headers and their hashes.
    recent: VecDeque<TrackedHeader>,
    /// Maximum number of recent blocks to keep.
    max_depth: usize,
}

#[derive(Clone)]
struct TrackedHeader {
    header: Header,
    hash: B256,
}

/// Information about a detected chain reorganization.
pub struct ReorgInfo {
    /// The block number where the fork diverged.
    pub fork_block: u64,
    /// The hash of the fork point block.
    pub fork_hash: B256,
    /// Hashes of blocks that were reverted (need to be marked non-canonical).
    pub reverted_hashes: Vec<B256>,
}

impl HeadTracker {
    pub fn new(max_depth: usize) -> Self {
        Self {
            recent: VecDeque::new(),
            max_depth,
        }
    }

    /// Restore the tracker from a previously persisted canonical header window.
    pub fn restore<I>(&mut self, headers: I)
    where
        I: IntoIterator<Item = Header>,
    {
        self.recent.clear();
        for header in headers {
            self.push(header);
        }
    }

    /// Snapshot the current recent canonical header window for persistence.
    pub fn snapshot(&self) -> Vec<Header> {
        self.recent
            .iter()
            .map(|tracked| tracked.header.clone())
            .collect()
    }

    /// Record a new block. Returns `None` if it extends the chain normally,
    /// or `Some(ReorgInfo)` if a reorg is detected.
    ///
    /// After calling this, the tracker's state reflects the new chain
    /// (reverted blocks are removed, new block is added).
    pub fn track(&mut self, header: Header) -> Option<ReorgInfo> {
        let tracked = TrackedHeader::new(header);
        if let Some(tip) = self.recent.back() {
            if tracked.header.parent_hash() == tip.hash {
                self.push_tracked(tracked);
                return None;
            }

            if let Some(fork_idx) = self
                .recent
                .iter()
                .rposition(|candidate| candidate.hash == tracked.header.parent_hash())
            {
                let fork = self.recent[fork_idx].clone();
                let reverted_hashes = self
                    .recent
                    .iter()
                    .skip(fork_idx + 1)
                    .map(|candidate| candidate.hash)
                    .collect();

                self.recent.truncate(fork_idx + 1);
                self.push_tracked(tracked);

                return Some(ReorgInfo {
                    fork_block: fork.header.number(),
                    fork_hash: fork.hash,
                    reverted_hashes,
                });
            }

            let reverted_hashes = self.recent.iter().map(|candidate| candidate.hash).collect();
            self.recent.clear();
            self.push_tracked(tracked);

            return Some(ReorgInfo {
                fork_block: 0,
                fork_hash: B256::ZERO,
                reverted_hashes,
            });
        }

        self.push_tracked(tracked);
        None
    }

    /// The current chain tip, if any.
    pub fn tip(&self) -> Option<(u64, B256)> {
        self.recent
            .back()
            .map(|tracked| (tracked.header.number(), tracked.hash))
    }

    /// The current canonical tip header, if any.
    pub fn tip_header(&self) -> Option<&Header> {
        self.recent.back().map(|tracked| &tracked.header)
    }

    /// Number of blocks currently tracked.
    pub fn len(&self) -> usize {
        self.recent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recent.is_empty()
    }

    fn push(&mut self, header: Header) {
        self.push_tracked(TrackedHeader::new(header));
    }

    fn push_tracked(&mut self, tracked: TrackedHeader) {
        self.recent.push_back(tracked);
        if self.recent.len() > self.max_depth {
            self.recent.pop_front();
        }
    }
}

impl TrackedHeader {
    fn new(header: Header) -> Self {
        let hash = header.hash_slow();
        Self { header, hash }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(number: u64, parent_hash: B256, marker: u8) -> Header {
        let mut header = Header {
            number,
            parent_hash,
            gas_limit: 30_000_000,
            timestamp: 1_700_000_000 + number,
            ..Default::default()
        };
        header.extra_data = vec![marker].into();
        header
    }

    #[test]
    fn normal_extension() {
        let mut tracker = HeadTracker::new(64);
        let first = header(100, B256::ZERO, 1);
        let second = header(101, first.hash_slow(), 2);
        let third = header(102, second.hash_slow(), 3);

        assert!(tracker.track(first.clone()).is_none());
        assert_eq!(tracker.tip(), Some((100, first.hash_slow())));

        assert!(tracker.track(second.clone()).is_none());
        assert_eq!(tracker.tip(), Some((101, second.hash_slow())));

        assert!(tracker.track(third.clone()).is_none());
        assert_eq!(tracker.tip(), Some((102, third.hash_slow())));
        assert_eq!(tracker.len(), 3);
    }

    #[test]
    fn single_block_reorg() {
        let mut tracker = HeadTracker::new(64);
        let first = header(100, B256::ZERO, 1);
        let second = header(101, first.hash_slow(), 2);
        let third = header(102, second.hash_slow(), 3);
        let competing = header(102, second.hash_slow(), 0xF3);

        tracker.track(first);
        tracker.track(second.clone());
        tracker.track(third.clone());

        let reorg = tracker.track(competing.clone()).unwrap();
        assert_eq!(reorg.fork_block, 101);
        assert_eq!(reorg.fork_hash, second.hash_slow());
        assert_eq!(reorg.reverted_hashes, vec![third.hash_slow()]);
        assert_eq!(tracker.tip(), Some((102, competing.hash_slow())));
    }

    #[test]
    fn multi_block_reorg() {
        let mut tracker = HeadTracker::new(64);
        let first = header(100, B256::ZERO, 1);
        let second = header(101, first.hash_slow(), 2);
        let third = header(102, second.hash_slow(), 3);
        let fourth = header(103, third.hash_slow(), 4);
        let competing = header(102, second.hash_slow(), 0xA2);

        tracker.track(first);
        tracker.track(second.clone());
        tracker.track(third.clone());
        tracker.track(fourth.clone());

        let reorg = tracker.track(competing.clone()).unwrap();
        assert_eq!(reorg.fork_block, 101);
        assert_eq!(
            reorg.reverted_hashes,
            vec![third.hash_slow(), fourth.hash_slow()]
        );
        assert_eq!(tracker.tip(), Some((102, competing.hash_slow())));
        assert_eq!(tracker.len(), 3);
    }

    #[test]
    fn deep_reorg_beyond_window() {
        let mut tracker = HeadTracker::new(3);
        let first = header(100, B256::ZERO, 1);
        let second = header(101, first.hash_slow(), 2);
        let third = header(102, second.hash_slow(), 3);
        let fourth = header(103, third.hash_slow(), 4);
        let competing = header(102, first.hash_slow(), 0xB2);

        tracker.track(first.clone());
        tracker.track(second.clone());
        tracker.track(third.clone());
        tracker.track(fourth.clone());
        assert_eq!(tracker.len(), 3);

        let reorg = tracker.track(competing).unwrap();
        assert_eq!(reorg.fork_block, 0);
        assert_eq!(
            reorg.reverted_hashes,
            vec![second.hash_slow(), third.hash_slow(), fourth.hash_slow()]
        );
    }

    #[test]
    fn sliding_window_evicts_old_blocks() {
        let mut tracker = HeadTracker::new(3);
        let first = header(100, B256::ZERO, 1);
        let second = header(101, first.hash_slow(), 2);
        let third = header(102, second.hash_slow(), 3);
        let fourth = header(103, third.hash_slow(), 4);

        tracker.track(first);
        tracker.track(second);
        tracker.track(third);
        tracker.track(fourth.clone());

        assert_eq!(tracker.len(), 3);
        assert_eq!(tracker.tip(), Some((103, fourth.hash_slow())));
    }

    #[test]
    fn restore_rehydrates_tip_and_snapshot() {
        let mut tracker = HeadTracker::new(4);
        let first = header(100, B256::ZERO, 1);
        let second = header(101, first.hash_slow(), 2);
        let third = header(102, second.hash_slow(), 3);

        tracker.restore(vec![first.clone(), second.clone(), third.clone()]);

        assert_eq!(tracker.tip(), Some((102, third.hash_slow())));
        assert_eq!(tracker.tip_header(), Some(&third));
        assert_eq!(tracker.snapshot(), vec![first, second, third]);
    }
}
