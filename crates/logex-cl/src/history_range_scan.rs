//! Bounded backward range search for an authenticated child's missing parent.
//!
//! A pass searches slots, not proofs of absence. A peer may omit the sought
//! block, so exhaustion requires caller retry policy and never means complete.

use alloy_primitives::B256;

use crate::rpc::BeaconBlocksByRangeRequest;

pub(crate) const MAX_SCAN_BATCH: u64 = 128;

#[derive(Debug, Clone)]
pub(crate) struct HistoryRangeScan {
    missing_root: B256,
    checkpoint_slot: u64,
    batch_limit: u64,
    // Original window bounds remain fixed while partial responses consume its
    // lower prefix. Finishing the suffix then moves below window_start.
    window_start: u64,
    window_end: u64,
    next_start: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanProgress {
    Continue,
    Exhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScanAdvanceError {
    UnexpectedRequest,
    LastSlotOutsideRequest,
}

impl HistoryRangeScan {
    /// Search the inclusive checkpoint floor up to the exclusive child slot.
    pub(crate) fn new(
        missing_root: B256,
        child_slot: u64,
        checkpoint_slot: u64,
        batch_limit: u64,
    ) -> Option<Self> {
        if child_slot <= checkpoint_slot || !(1..=MAX_SCAN_BATCH).contains(&batch_limit) {
            return None;
        }
        let window_start = child_slot.saturating_sub(batch_limit).max(checkpoint_slot);
        Some(Self {
            missing_root,
            checkpoint_slot,
            batch_limit,
            window_start,
            window_end: child_slot,
            next_start: Some(window_start),
        })
    }

    pub(crate) fn missing_root(&self) -> B256 {
        self.missing_root
    }

    pub(crate) fn request(&self, batch_limit: u64) -> Option<BeaconBlocksByRangeRequest> {
        if !(1..=MAX_SCAN_BATCH).contains(&batch_limit) {
            return None;
        }
        let start_slot = self.next_start?;
        Some(BeaconBlocksByRangeRequest {
            start_slot,
            count: (self.window_end - start_slot).min(batch_limit),
            step: 1,
        })
    }

    /// Advance only after the caller validates and correlates the complete
    /// response with its exact issued request. The issued request may be any
    /// bounded contiguous prefix at the cursor, allowing dynamic batch limits.
    /// `last_slot` is its greatest returned slot, or None if empty.
    pub(crate) fn advance(
        &mut self,
        request: BeaconBlocksByRangeRequest,
        last_slot: Option<u64>,
    ) -> Result<ScanProgress, ScanAdvanceError> {
        if self.next_start != Some(request.start_slot)
            || request.step != 1
            || !(1..=MAX_SCAN_BATCH).contains(&request.count)
            || request.count > self.window_end.saturating_sub(request.start_slot)
        {
            return Err(ScanAdvanceError::UnexpectedRequest);
        }
        // The count is bounded by the remaining exclusive window, so the
        // addition cannot overflow even with a child slot at u64::MAX.
        let request_end = request.start_slot + request.count;
        let next_start = if let Some(last_slot) = last_slot {
            if last_slot < request.start_slot || last_slot >= request_end {
                return Err(ScanAdvanceError::LastSlotOutsideRequest);
            }
            last_slot + 1
        } else {
            // Empty responses consume only the issued prefix, not a suffix
            // that was excluded by a temporarily reduced request limit.
            request_end
        };
        if next_start < self.window_end {
            self.next_start = Some(next_start);
            return Ok(ScanProgress::Continue);
        }
        if self.window_start == self.checkpoint_slot {
            self.next_start = None;
            return Ok(ScanProgress::Exhausted);
        }
        self.window_end = self.window_start;
        self.window_start = self
            .window_end
            .saturating_sub(self.batch_limit)
            .max(self.checkpoint_slot);
        self.next_start = Some(self.window_start);
        Ok(ScanProgress::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(start_slot: u64, count: u64) -> BeaconBlocksByRangeRequest {
        BeaconBlocksByRangeRequest {
            start_slot,
            count,
            step: 1,
        }
    }

    #[test]
    fn full_windows_move_backward_and_exhaust_at_floor() {
        let root = B256::repeat_byte(7);
        let mut scan = HistoryRangeScan::new(root, 10, 2, 3).unwrap();
        assert_eq!(scan.missing_root(), root);
        for (expected, result) in [
            (request(7, 3), ScanProgress::Continue),
            (request(4, 3), ScanProgress::Continue),
            (request(2, 2), ScanProgress::Exhausted),
        ] {
            assert_eq!(scan.request(MAX_SCAN_BATCH), Some(expected));
            assert_eq!(
                scan.advance(expected, Some(expected.start_slot + expected.count - 1)),
                Ok(result)
            );
        }
        assert_eq!(scan.request(MAX_SCAN_BATCH), None);
        assert_eq!(
            scan.advance(request(2, 2), None),
            Err(ScanAdvanceError::UnexpectedRequest)
        );
    }

    #[test]
    fn partial_prefix_preserves_upper_suffix_and_original_lower_boundary() {
        let mut scan = HistoryRangeScan::new(B256::ZERO, 10, 0, 4).unwrap();
        assert_eq!(scan.request(MAX_SCAN_BATCH), Some(request(6, 4)));
        assert_eq!(
            scan.advance(request(6, 4), Some(6)),
            Ok(ScanProgress::Continue)
        );
        assert_eq!(scan.request(MAX_SCAN_BATCH), Some(request(7, 3)));
        // Slot 7 may be skipped: a valid response ending at 8 still leaves 9.
        assert_eq!(
            scan.advance(request(7, 3), Some(8)),
            Ok(ScanProgress::Continue)
        );
        assert_eq!(scan.request(MAX_SCAN_BATCH), Some(request(9, 1)));
        assert_eq!(
            scan.advance(request(9, 1), None),
            Ok(ScanProgress::Continue)
        );
        assert_eq!(scan.request(MAX_SCAN_BATCH), Some(request(2, 4)));
    }

    #[test]
    fn empty_windows_cover_bounded_slot_intervals_without_claiming_absence() {
        let mut scan = HistoryRangeScan::new(B256::ZERO, 8, 1, 3).unwrap();
        for expected in [request(5, 3), request(2, 3), request(1, 1)] {
            assert_eq!(scan.request(MAX_SCAN_BATCH), Some(expected));
            let progress = scan.advance(expected, None).unwrap();
            assert_eq!(
                progress == ScanProgress::Exhausted,
                expected.start_slot == 1
            );
        }
        assert!(scan.request(MAX_SCAN_BATCH).is_none());
        // Another peer/pass can retry the same slots; exhaustion was no proof.
        assert!(HistoryRangeScan::new(B256::ZERO, 8, 1, 3).is_some());
    }

    #[test]
    fn invalid_or_stale_response_does_not_advance() {
        let mut scan = HistoryRangeScan::new(B256::ZERO, 9, 0, 3).unwrap();
        let expected = request(6, 3);
        for stale in [
            request(5, 3),
            request(6, 0),
            BeaconBlocksByRangeRequest {
                step: 2,
                ..expected
            },
        ] {
            assert_eq!(
                scan.advance(stale, None),
                Err(ScanAdvanceError::UnexpectedRequest)
            );
            assert_eq!(scan.request(MAX_SCAN_BATCH), Some(expected));
        }
        for outside in [5, 9, u64::MAX] {
            assert_eq!(
                scan.advance(expected, Some(outside)),
                Err(ScanAdvanceError::LastSlotOutsideRequest)
            );
            assert_eq!(scan.request(MAX_SCAN_BATCH), Some(expected));
        }
        scan.advance(expected, Some(6)).unwrap();
        assert_eq!(
            scan.advance(expected, Some(7)),
            Err(ScanAdvanceError::UnexpectedRequest)
        );
        assert_eq!(scan.request(MAX_SCAN_BATCH), Some(request(7, 2)));
    }

    #[test]
    fn changing_limits_preserve_original_window_and_empty_prefix_suffix() {
        let mut scan = HistoryRangeScan::new(B256::ZERO, 12, 0, 8).unwrap();
        assert_eq!(scan.request(2), Some(request(4, 2)));
        assert_eq!(
            scan.advance(request(4, 2), None),
            Ok(ScanProgress::Continue)
        );
        assert_eq!(scan.request(1), Some(request(6, 1)));
        scan.advance(request(6, 1), Some(6)).unwrap();
        assert_eq!(scan.request(8), Some(request(7, 5)));
        // A partial response consumes only through its greatest returned slot.
        scan.advance(request(7, 5), Some(8)).unwrap();
        assert_eq!(scan.request(1), Some(request(9, 1)));
        scan.advance(request(9, 1), None).unwrap();
        assert_eq!(scan.request(128), Some(request(10, 2)));
        scan.advance(request(10, 2), None).unwrap();
        assert_eq!(scan.request(128), Some(request(0, 4)));
        assert_eq!(
            scan.advance(request(0, 4), None),
            Ok(ScanProgress::Exhausted)
        );
    }

    #[test]
    fn issued_prefix_bounds_validate_last_slot_and_leave_errors_unchanged() {
        let mut scan = HistoryRangeScan::new(B256::ZERO, 10, 0, 4).unwrap();
        assert_eq!(scan.request(0), None);
        assert_eq!(scan.request(129), None);
        for invalid in [request(6, 5), request(6, 129), request(u64::MAX, 1)] {
            assert_eq!(
                scan.advance(invalid, None),
                Err(ScanAdvanceError::UnexpectedRequest)
            );
            assert_eq!(scan.request(4), Some(request(6, 4)));
        }
        // Slot 8 is in the original window, but outside this issued prefix.
        assert_eq!(
            scan.advance(request(6, 2), Some(8)),
            Err(ScanAdvanceError::LastSlotOutsideRequest)
        );
        assert_eq!(scan.request(4), Some(request(6, 4)));
        scan.advance(request(6, 2), Some(7)).unwrap();
        assert_eq!(scan.request(4), Some(request(8, 2)));
    }

    #[test]
    fn arithmetic_boundaries_and_batch_limits() {
        for (child, floor, batch) in [(0, 0, 1), (1, 1, 1), (1, 2, 1), (1, 0, 0), (1, 0, 129)] {
            assert!(HistoryRangeScan::new(B256::ZERO, child, floor, batch).is_none());
        }
        let mut zero = HistoryRangeScan::new(B256::ZERO, 1, 0, 128).unwrap();
        assert_eq!(zero.request(MAX_SCAN_BATCH), Some(request(0, 1)));
        assert_eq!(
            zero.advance(request(0, 1), Some(0)),
            Ok(ScanProgress::Exhausted)
        );
        let mut max = HistoryRangeScan::new(B256::ZERO, u64::MAX, u64::MAX - 2, 128).unwrap();
        let first = request(u64::MAX - 2, 2);
        assert_eq!(max.request(MAX_SCAN_BATCH), Some(first));
        max.advance(first, Some(u64::MAX - 2)).unwrap();
        let last = request(u64::MAX - 1, 1);
        assert_eq!(max.request(MAX_SCAN_BATCH), Some(last));
        assert_eq!(
            max.advance(last, Some(u64::MAX - 1)),
            Ok(ScanProgress::Exhausted)
        );
    }
}
