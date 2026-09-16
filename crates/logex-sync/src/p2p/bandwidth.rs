//! Bounded recent-rate telemetry; cumulative payload counts retain u64 saturation.
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const RATE_WINDOW: Duration = Duration::from_secs(15);
const BUCKET_WIDTH: Duration = Duration::from_millis(250);
// Starts are at least 250 ms apart, with at most one partially expired bucket.
const MAX_BUCKETS: usize = 61;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PayloadBandwidthSnapshot {
    pub(super) bytes_per_sec: u64,
    pub(super) total_payload_bytes: u64,
}

#[derive(Debug)]
struct Bucket {
    first_at: Instant,
    last_at: Instant,
    payload_bytes: u128,
}

#[derive(Debug, Default)]
pub(super) struct PayloadBandwidthWindow {
    buckets: VecDeque<Bucket>,
    total_payload_bytes: u64,
}

impl PayloadBandwidthWindow {
    pub(super) fn record(&mut self, payload_bytes: u64, now: Instant) {
        if payload_bytes == 0 {
            return;
        }
        // Production callers sample a monotonic clock while holding the owner
        // or metrics lock. Clamp an earlier supplied timestamp defensively.
        let now = self
            .buckets
            .back()
            .map_or(now, |last| now.max(last.last_at));
        self.total_payload_bytes = self.total_payload_bytes.saturating_add(payload_bytes);
        while self
            .buckets
            .front()
            .is_some_and(|first| now.saturating_duration_since(first.last_at) > RATE_WINDOW)
        {
            self.buckets.pop_front();
        }
        if let Some(last) = self.buckets.back_mut()
            && now.saturating_duration_since(last.first_at) < BUCKET_WIDTH
        {
            last.last_at = now;
            last.payload_bytes = last.payload_bytes.saturating_add(u128::from(payload_bytes));
        } else {
            self.buckets.push_back(Bucket {
                first_at: now,
                last_at: now,
                payload_bytes: u128::from(payload_bytes),
            });
        }
        debug_assert!(self.buckets.len() <= MAX_BUCKETS);
    }

    /// Recent rate uses whole buckets: bytes can remain in the 15-second window
    /// for less than 250 ms extra. Lifetime totals have no bucketing approximation.
    pub(super) fn snapshot(&self, now: Instant) -> PayloadBandwidthSnapshot {
        let mut bytes = 0u128;
        let mut first_at = None;
        for bucket in &self.buckets {
            if now.saturating_duration_since(bucket.last_at) > RATE_WINDOW {
                continue;
            }
            first_at.get_or_insert(bucket.first_at);
            bytes = bytes.saturating_add(bucket.payload_bytes);
        }
        let bytes_per_sec = first_at.map_or(0, |first_at| {
            let elapsed = now
                .saturating_duration_since(first_at)
                .clamp(Duration::from_secs(1), RATE_WINDOW);
            (bytes as f64 / elapsed.as_secs_f64()).round() as u64
        });
        PayloadBandwidthSnapshot {
            bytes_per_sec,
            total_payload_bytes: self.total_payload_bytes,
        }
    }

    #[cfg(test)]
    pub(super) fn sample_count(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dense_accounting_is_bounded_and_keeps_cumulative_totals() {
        let mut window = PayloadBandwidthWindow::default();
        let start = Instant::now();
        for millis in 0..60_000 {
            let now = start + Duration::from_millis(millis);
            window.record(7, now);
            assert!(window.sample_count() <= 61);
            assert_eq!(window.snapshot(now).total_payload_bytes, (millis + 1) * 7);
        }
        assert_eq!(window.sample_count(), 61);
    }

    #[test]
    fn expiry_keeps_partial_boundary_bucket_for_less_than_one_bucket_width() {
        let mut window = PayloadBandwidthWindow::default();
        let start = Instant::now();
        window.record(3_000, start);
        window.record(6_000, start + Duration::from_millis(249));
        window.record(9_000, start + Duration::from_millis(250));
        assert_eq!(window.sample_count(), 2);
        assert_eq!(
            window
                .snapshot(start + Duration::from_secs(15))
                .bytes_per_sec,
            1_200
        );
        assert_eq!(
            window
                .snapshot(start + Duration::from_millis(15_249))
                .bytes_per_sec,
            1_200
        );
        assert_eq!(
            window
                .snapshot(start + Duration::from_millis(15_250))
                .bytes_per_sec,
            600
        );
        let expired = window.snapshot(start + Duration::from_millis(15_251));
        assert_eq!(expired.bytes_per_sec, 0);
        assert_eq!(expired.total_payload_bytes, 18_000);
    }

    #[test]
    fn idle_restart_and_zero_payloads_preserve_totals() {
        let mut window = PayloadBandwidthWindow::default();
        let start = Instant::now();
        window.record(0, start);
        assert_eq!(window.snapshot(start), PayloadBandwidthSnapshot::default());
        assert_eq!(window.sample_count(), 0);
        window.record(500, start);
        let later = start + Duration::from_secs(86_400);
        assert_eq!(window.snapshot(later).bytes_per_sec, 0);
        window.record(200, later);
        assert_eq!(window.sample_count(), 1);
        assert_eq!(window.snapshot(later).bytes_per_sec, 200);
        assert_eq!(window.snapshot(later).total_payload_bytes, 700);
    }

    #[test]
    fn large_counts_do_not_understate_recent_rate_after_earlier_bucket_expires() {
        let mut window = PayloadBandwidthWindow::default();
        let start = Instant::now();
        window.record(u64::MAX, start);
        window.record(u64::MAX, start + Duration::from_secs(1));
        let snapshot = window.snapshot(start + Duration::from_millis(15_001));
        let expected = (u64::MAX as f64 / 14.001).round() as u64;
        assert_eq!(snapshot.bytes_per_sec, expected);
        assert_eq!(snapshot.total_payload_bytes, u64::MAX);
        let mut window = PayloadBandwidthWindow::default();
        window.record(u64::MAX, start);
        window.record(u64::MAX, start);
        assert_eq!(window.snapshot(start).bytes_per_sec, u64::MAX);
    }

    #[test]
    fn earlier_supplied_timestamp_does_not_break_bucket_order() {
        let mut window = PayloadBandwidthWindow::default();
        let start = Instant::now();
        window.record(100, start + Duration::from_secs(1));
        window.record(200, start);
        assert_eq!(window.sample_count(), 1);
        assert_eq!(
            window
                .snapshot(start + Duration::from_secs(2))
                .bytes_per_sec,
            300
        );
    }
}
