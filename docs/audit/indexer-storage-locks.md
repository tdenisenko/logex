# Background index inspection and storage locks

## Finding

**B10-20 — moderate responsiveness risk: index freshness inspection performed
filesystem work on an async worker and retained the ingestion lock for sealed
segments.** `sealed_query_index_targets` opened source, checkpoint and index
files while its caller held the storage read guard. A delayed filesystem read
therefore delayed the storage writer as well as an async runtime worker. The hot
freshness check also ran directly in the async loop, although it had released its
storage guard. Actual index construction already used blocking workers.

A regression on `17b044f9` adds a test-only observation at the entry to the actual
freshness helper; it does not replace or move the probe. With two owned sealed
segments, the unchanged selector invokes that observation while holding its read
guard. `try_write` fails, demonstrating the retained ingestion lock. The original
control fails once. This is a lock-ownership observation, not a filesystem stall
or a throughput benchmark; no physical storage is altered.

## Change and invariants

Sealed-index candidate capture now copies at most 64 eligible paths under each
short read guard. The worker releases that guard before any freshness inspection
or index construction. The entire scan/build loop runs in one blocking task,
retaining shared storage ownership until it finishes. It builds at most the
existing eight missing ERC20 event index sets per idle pass. Empty and active
historical staging segments remain excluded; active historical sync still defers
query indexing entirely.

Each pass uses the initial sealed-list length as its traversal bound, with bounded
positional batches. It does not assume that catalog IDs are sorted, clone every
partition at once, or repeatedly search from the beginning. Concurrent rotation
or list changes may defer work to the next pass; this is advisory maintenance,
not coverage or a query snapshot. Current source eligibility and publication
identity are checked by the existing builder for every build. This preserves
source-change rejection and the safe query fallback for missing/stale indexes.

Hot freshness inspection now runs in the same blocking closure as its optional
build. Its empty-source, changed-row, rotation, missing-index, unsupported-source
and retry/error policies remain. Only successful actual builds update the cached
hot state. No additional ingestion writes, index formats, SQL evaluation or
publication guarantees change.

Removed the completed-max-ID marker: it was updated only after the full scan and
never skipped a filesystem check. Removed the unreachable active-sync index limit
branch; the earlier active-sync guard already skips the entire indexing section.

## Validation and scope

All 19 background controls and all 207 node tests passed before the final removal
of the unreachable limit branch. New controls check writer access at both the
freshness and real build boundaries, exact build limits and resulting published
indexes, 64-candidate metadata batches across a larger list, a real append/rotation
inside a freshness callback, finite initial-pass work and later discovery of new
segments, disabled-pass behavior under an existing write guard, and the retained
hot rebuild policy with real index artifacts. Existing staging exclusion,
checkpoint failure and worker-cleanup tests remain.

Implementer review traced the actual source/checkpoint/artifact opens, read-guard
lifetimes, storage ownership across worker completion, publication checks and
unchanged indexing policy. No independent review is claimed. All eight local gates pass on `c86d5e8a`: vendor verification, workspace/patched-vendor formatting, check, strict Clippy, 1,832 workspace tests (24 ignored), documentation tests and release build. Exact-head Linux/macOS CI and merge remain pending.

The lock control establishes that index inspection no longer excludes an ingestion
writer. Moving hot inspection off the async worker is verified by call-site review.
There is no measured throughput or latency percentage claim and no additional
benchmark campaign. Metadata copies are bounded by the 64-path batch rather than
partition count. An idle scan may still inspect every initially visible sealed
partition, as before; per-pass index work remains capped at eight builds.

Compaction/backlog inspection still performs filesystem work while holding a
storage read guard on its blocking worker. Safely separating compaction eligibility
and publication requires a distinct follow-up and remains on the roadmap. Existing
whole-node shutdown deadlines cover started blocking work; this milestone does not
add per-file cancellation or establish live-sync readiness.

Machine-readable evidence: [baseline](baselines/2026-09-17-indexer-storage-locks.json).
