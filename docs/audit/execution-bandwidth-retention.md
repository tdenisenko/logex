# Bounded execution bandwidth telemetry

## Finding

**B4-42, medium — recent-rate telemetry retains one allocation record per payload
event.** The upload and download counters both retained every nonzero accounting
event for fifteen seconds. Time expiry did not impose an entry bound. Download
status traversed all retained events; upload pruned them while holding its metrics
mutex. High event rates therefore increased optional telemetry memory and work.
This is a resource-bound finding, not an observed production memory exhaustion.

Two controls against unchanged production at `bedfed0d` each recorded 4,096
one-byte events at one timestamp. Both retained 4,096 entries and failed the
proposed 61-sample bound. They do not send traffic or allocate network payloads.

## Correction and precision

Share one crate-private counter implementation between upload and download.
Combine events into buckets less than 250 ms wide and expire a bucket when its
last event is more than fifteen seconds old. Bucket starts are at least 250 ms
apart, so at most 61 buckets remain. Record insertion/pruning is amortized constant
work and each status snapshot visits at most 61 buckets. Backing capacity can
exceed the live entry count due to VecDeque allocation rounding, but no longer
depends on the number of events per time window. Existing upload locking remains.

Lifetime totals keep the existing u64 saturation semantics; bucketing does not
approximate them. Recent displayed rates now have a bounded time approximation:
older bytes can remain included for less than 250 ms beyond the fifteen-second
boundary. This is a time-precision bound, not a percentage-error guarantee for a
burst at the boundary. The existing one-second minimum rate denominator and
fifteen-second maximum denominator remain. Public field names and types, payload
measurement basis, ingestion, queries and serving behavior are unchanged.

Use wider bucket sums so saturation of a running u64 sum followed by subtraction
cannot understate the remaining window. Snapshot sums saturate safely, and final
rates still use the existing rounded, saturating float-to-u64 conversion. Earlier
supplied timestamps are clamped to the latest recorded timestamp to preserve
bucket ordering; production callers sample their monotonic clock under ownership
or the metrics lock. Zero payloads do not add samples.

## Review and validation

Seven new controls cover each original counter path, dense accounting with 60,000
explicit timestamps, bucket and expiry boundaries, cumulative counts, idle restart,
zero payloads, large counts and earlier supplied timestamps. Existing rate and
cache/peer controls remain. The first candidate passed 355 P2P tests but exposed
one obsolete Duration import; remove it and rerun after clarifying the saturation
fixture. Final validation passes all 355 P2P tests (one ignored component workload). No sleeping, live peers,
external storage or Mac mini is needed for the new pure counter tests.

Source review traced every upload/download accounting caller and both status
snapshots. Consolidation removes two duplicate event-queue implementations,
two duplicate window constants and separate snapshot structures. No independent
review is claimed. This is bounded telemetry accounting; no measured ingestion
speedup, total network-memory limit or whole-batch completion is claimed.

Full workspace gates and PR/CI/merge are pending.

The separate retained-state inventory still includes negative peer hints,
request-accounting queues and outgoing/transient response ownership. Their limits
must be assessed at the actual producers and consumers rather than inferred from
the bounded optional serving cache.

[Validation record](baselines/2026-09-16-execution-bandwidth-retention.json).
