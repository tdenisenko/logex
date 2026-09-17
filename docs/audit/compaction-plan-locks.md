# Compaction inspection and storage locks

## Findings

**B10-21 — moderate responsiveness risk: background compaction planning and
backlog scans retained the ingestion read lock during filesystem inspection.**
Source review on `d6e71722` traced the background worker's `blocking_read` guard
through raw/current-profile checks to manifest and column-header reads. Actual
compaction already ran outside that guard, but a slow planning read still
prevented the ingestion writer from acquiring storage. This finding is based on
call-site and guard-lifetime review; no original filesystem-stall benchmark is
claimed.

**B10-22 — low severity: backlog counts included unfinished ingestion that plans
excluded.** An original-source regression in
`published_ingestion_blocks_compaction_until_hardened_or_reopened` observes one
raw backlog segment while the actual raw plan is empty. Counts omitted pending
and published ingestion/checkpoint exclusions. The original test fails once with
exit 101. This is an inaccurate maintenance/status count and scheduling input;
it does not demonstrate incorrect compaction or data loss.

## Implementation and invariants

Storage now exposes metadata-only candidate capture over a catalog position
range. Background planning and counting capture at most 64 positions per read
guard, then release the guard before filesystem work. The initial catalog length
bounds each scan. The oldest/newest traversal order is preserved without assuming
sorted segment IDs; new positions wait for the next pass. No whole-catalog clone
or persistent metadata is added. Scheduling remains advisory across batches.

One shared eligibility predicate excludes active historical staging, pending and
published ingestion, and the pending checkpoint's affected segments. It also
preserves sealed/nonempty eligibility and the existing finalized-anchor/head
safety margin. Both the existing plan APIs and all backlog modes use it. The
three duplicate mode-specific eligibility helpers were removed. Existing public
plan APIs remain for maintenance callers and tests.

Each captured task retains the exclusive data-directory owner. The actual
compactor still acquires the segment maintenance lock and verifies source
binding, generation, bundle reference, namespace and commitment before writing.
The raw canonical writer uses the same segment-directory lock. Manifest mode
inspection is only a hint and does not replace these publication checks.

The background worker retains its active-sync, historical deferral, memory
pressure, refresh interval, raw/rewrite ordering, error and per-pass limit
policies. Hot and sealed index behavior is unchanged. There are no new ingestion
writes, format changes, SQL changes or network activity.

## Validation and limits

All seven focused storage compaction tests and all 22 background tests pass.
New/extended controls cover a writer held during real manifest inspection and
compaction; oldest/newest selection and exact limits; raw/current/rewrite mode
behavior; disabled work while a writer is held; traversal across 64-position
batches with actual concurrent append and later discovery; unfinished ingestion
and checkpoint exclusions; capture with an owned segment temporarily unavailable;
unsorted catalog positions and range bounds; and directory ownership retained
until both plans and captured candidates drop. The original backlog regression
passes after durable checkpointing and reopening are exercised.

Implementer review traced the mode checks, lock lifetimes, bounded allocations,
source verification and background call sites. No independent review is claimed.
All eight local gates pass on `948bece4`: vendor verification, workspace/patched-vendor formatting, check, strict Clippy, 1,836 workspace tests (24 ignored), documentation tests and release build. Exact-head Linux/macOS CI and merge remain pending.

This removes filesystem I/O from the affected ingestion read guards. It makes no
numerical throughput or latency claim and starts no benchmark campaign. Scans
still inspect the initially visible catalog in bounded batches; filesystem work
can still be slow on its blocking worker. Existing supervision/shutdown bounds
remain responsible for that lifecycle. Live sync and the broader audit remain
incomplete.

Machine-readable evidence: [baseline](baselines/2026-09-17-compaction-plan-locks.json).
