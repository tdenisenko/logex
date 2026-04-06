use alloy_primitives::B256;
use std::collections::VecDeque;

/// Tracks the recent chain head for reorg detection.
///
/// Maintains a sliding window of recent block hashes. When a new block
/// arrives whose parent_hash doesn't match the current tip, the tracker
/// walks backward to find the fork point and reports which blocks were
/// reverted.
pub struct HeadTracker {
    /// Recent blocks: (block_number, block_hash, parent_hash).
    recent: VecDeque<(u64, B256, B256)>,
    /// Maximum number of recent blocks to keep.
    max_depth: usize,
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

    /// Record a new block. Returns `None` if it extends the chain normally,
    /// or `Some(ReorgInfo)` if a reorg is detected.
    ///
    /// After calling this, the tracker's state reflects the new chain
    /// (reverted blocks are removed, new block is added).
    pub fn track(&mut self, number: u64, hash: B256, parent_hash: B256) -> Option<ReorgInfo> {
        if let Some(tip) = self.recent.back() {
            if parent_hash == tip.1 {
                // Normal extension — parent matches our tip
                self.push(number, hash, parent_hash);
                return None;
            }

            // Parent doesn't match tip — potential reorg.
            // Walk backward to find the fork point.
            if let Some(fork_idx) = self.recent.iter().rposition(|(_, h, _)| *h == parent_hash) {
                // Fork point found in our window.
                let fork = self.recent[fork_idx];

                // Collect reverted block hashes (everything after fork point).
                let reverted_hashes: Vec<B256> = self
                    .recent
                    .iter()
                    .skip(fork_idx + 1)
                    .map(|(_, h, _)| *h)
                    .collect();

                // Trim the chain back to the fork point.
                self.recent.truncate(fork_idx + 1);

                // Add the new block on top.
                self.push(number, hash, parent_hash);

                return Some(ReorgInfo {
                    fork_block: fork.0,
                    fork_hash: fork.1,
                    reverted_hashes,
                });
            }

            // Parent not found in our window — deep reorg beyond tracking depth.
            // Report all tracked blocks as reverted. The caller must handle
            // this as a full resync from the new block's ancestry.
            let reverted_hashes: Vec<B256> = self.recent.iter().map(|(_, h, _)| *h).collect();
            self.recent.clear();
            self.push(number, hash, parent_hash);

            return Some(ReorgInfo {
                fork_block: 0,
                fork_hash: B256::ZERO,
                reverted_hashes,
            });
        }

        // First block ever tracked.
        self.push(number, hash, parent_hash);
        None
    }

    /// The current chain tip, if any.
    pub fn tip(&self) -> Option<(u64, B256)> {
        self.recent.back().map(|(n, h, _)| (*n, *h))
    }

    /// Number of blocks currently tracked.
    pub fn len(&self) -> usize {
        self.recent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recent.is_empty()
    }

    fn push(&mut self, number: u64, hash: B256, parent_hash: B256) {
        self.recent.push_back((number, hash, parent_hash));
        if self.recent.len() > self.max_depth {
            self.recent.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u8) -> B256 {
        B256::repeat_byte(n)
    }

    #[test]
    fn normal_extension() {
        let mut tracker = HeadTracker::new(64);

        // First block — no reorg.
        assert!(tracker.track(100, hash(1), hash(0)).is_none());
        assert_eq!(tracker.tip(), Some((100, hash(1))));

        // Normal extension.
        assert!(tracker.track(101, hash(2), hash(1)).is_none());
        assert_eq!(tracker.tip(), Some((101, hash(2))));

        assert!(tracker.track(102, hash(3), hash(2)).is_none());
        assert_eq!(tracker.tip(), Some((102, hash(3))));
        assert_eq!(tracker.len(), 3);
    }

    #[test]
    fn single_block_reorg() {
        let mut tracker = HeadTracker::new(64);

        tracker.track(100, hash(1), hash(0));
        tracker.track(101, hash(2), hash(1));
        tracker.track(102, hash(3), hash(2));

        // Block 102' with parent=hash(2) — reverts block 102.
        let reorg = tracker.track(102, hash(0xF3), hash(2)).unwrap();
        assert_eq!(reorg.fork_block, 101);
        assert_eq!(reorg.fork_hash, hash(2));
        assert_eq!(reorg.reverted_hashes, vec![hash(3)]);
        assert_eq!(tracker.tip(), Some((102, hash(0xF3))));
    }

    #[test]
    fn multi_block_reorg() {
        let mut tracker = HeadTracker::new(64);

        tracker.track(100, hash(1), hash(0));
        tracker.track(101, hash(2), hash(1));
        tracker.track(102, hash(3), hash(2));
        tracker.track(103, hash(4), hash(3));

        // Reorg back to block 101: new block 102' with parent=hash(2).
        let reorg = tracker.track(102, hash(0xA2), hash(2)).unwrap();
        assert_eq!(reorg.fork_block, 101);
        assert_eq!(reorg.reverted_hashes, vec![hash(3), hash(4)]);
        assert_eq!(tracker.tip(), Some((102, hash(0xA2))));
        // Tracker now has: 100, 101, 102'
        assert_eq!(tracker.len(), 3);
    }

    #[test]
    fn deep_reorg_beyond_window() {
        let mut tracker = HeadTracker::new(3);

        tracker.track(100, hash(1), hash(0));
        tracker.track(101, hash(2), hash(1));
        tracker.track(102, hash(3), hash(2));
        // Window is full (3 blocks). Adding 103 evicts 100.
        tracker.track(103, hash(4), hash(3));
        assert_eq!(tracker.len(), 3); // 101, 102, 103

        // Now a reorg with parent=hash(1) — which was evicted.
        let reorg = tracker.track(102, hash(0xB2), hash(1)).unwrap();
        assert_eq!(reorg.fork_block, 0); // deep reorg sentinel
        assert_eq!(reorg.reverted_hashes, vec![hash(2), hash(3), hash(4)]);
    }

    #[test]
    fn sliding_window_evicts_old_blocks() {
        let mut tracker = HeadTracker::new(3);

        tracker.track(100, hash(1), hash(0));
        tracker.track(101, hash(2), hash(1));
        tracker.track(102, hash(3), hash(2));
        tracker.track(103, hash(4), hash(3));

        assert_eq!(tracker.len(), 3);
        assert_eq!(tracker.tip(), Some((103, hash(4))));
    }
}
