# Bounded WAL checkpoint investigation — 2026-09-11

**Unaccepted performance: PR #130 remains a draft.** The user authorized
engineering judgment for bounded checkpoints and requires at most 10% ingestion
regression. The current code does not meet that requirement. These are local
storage timings, not an end-to-end P2P sync measurement.

## Method and exploratory results

Same baseline executable, pinned toolchain, Mac14,15 hardware and internal APFS
SSD as the [initial comparison](2026-09-10-commit-replay.md). Each experiment uses
one baseline/candidate pair per profile, three iterations per process, 200,000
rows, 8,192-row batches, 50,000-row segments and four query workers. No concurrent
build or test process ran during timing. The original fixture digests and exact
result oracles remain unchanged. Binary hashes, individual timings, throughput,
logical disk use and process RSS are retained in each JSONL file. The lockfile
change only declares the already-resolved Apple libc dependency; package versions
and the pinned compiler remain unchanged.

The candidate harness explicitly calls `checkpoint()` before stopping the live
ingestion timer. Historical ingestion includes finalization and checkpoint.
The original baseline retired its WAL after every live batch, so it needs no
new finalization call. This harness difference deliberately charges the candidate
for deferred work; it does not move that work into indexing or reopen. All exact
oracles passed, including checks after indexing, compaction and reopen.

| Candidate | Profile | Metric | Baseline median ms | Candidate median ms | Regression |
| --- | --- | --- | ---: | ---: | ---: |
| v1 | dense | live_storage_ingest | 422.602 | 693.968 | +64.21% |
| v1 | dense | historical_storage_ingest | 236.849 | 721.906 | +204.80% |
| v1 | sparse | live_storage_ingest | 400.028 | 764.462 | +91.10% |
| v1 | sparse | historical_storage_ingest | 270.771 | 936.887 | +246.01% |
| v2 | dense | live_storage_ingest | 396.332 | 517.144 | +30.48% |
| v2 | dense | historical_storage_ingest | 236.307 | 606.205 | +156.53% |
| v2 | sparse | live_storage_ingest | 372.916 | 537.048 | +44.01% |
| v2 | sparse | historical_storage_ingest | 233.826 | 635.110 | +171.62% |
| v3 | dense | live_storage_ingest | 378.958 | 519.235 | +37.02% |
| v3 | dense | historical_storage_ingest | 227.728 | 572.189 | +151.26% |
| v3 | sparse | live_storage_ingest | 412.823 | 520.389 | +26.06% |
| v3 | sparse | historical_storage_ingest | 250.736 | 576.562 | +129.95% |
| v4 | dense | live_storage_ingest | 379.169 | 478.949 | +26.32% |
| v4 | dense | historical_storage_ingest | 211.784 | 556.969 | +162.99% |
| v4 | sparse | live_storage_ingest | 380.246 | 473.722 | +24.58% |
| v4 | sparse | historical_storage_ingest | 237.226 | 571.927 | +141.09% |

These exploratory pairs are not statistical acceptance evidence. Versions:

- [v1](2026-09-11-checkpoint-v1.jsonl): bounded multi-batch WAL epochs, grouped
  column replacements, ordered ingestion manifests and a final checkpoint.
- [v2](2026-09-11-checkpoint-v2.jsonl): remove redundant same-device ordering
  barriers and consolidate catalog/WAL/journal retirement into one full flush.
- [v3](2026-09-11-checkpoint-v3.jsonl): test parallel raw-column compaction using
  the existing scoped worker approach.
- [v4](2026-09-11-checkpoint-v4.jsonl): additionally test four worker groups for
  appending independent columns. Retention depends on repeated comparisons.

## Exact committed candidate comparison

[v5 raw results](2026-09-11-checkpoint-v5.jsonl) compare `ff728ea3` with
`09a63f55` after the final correctness fixes. This is the same internal-APFS
environment and fixture configuration above: five alternating baseline/candidate
pairs per profile, three iterations per process, 20 processes and 60 workload
iterations. No local compilation or other test workload ran during measurement.
Every exact lifecycle/query oracle passed. The new optional cross-filesystem
test added afterward changes no production code or benchmark behavior.

| Profile | Metric | Baseline median ms | Candidate median ms | Regression |
| --- | --- | ---: | ---: | ---: |
| dense | live_storage_ingest | 410.721 | 484.955 | +18.07% |
| sparse | live_storage_ingest | 409.886 | 480.874 | +17.32% |
| dense | historical_storage_ingest | 232.346 | 569.920 | +145.29% |
| sparse | historical_storage_ingest | 240.724 | 575.825 | +139.21% |

All five pair-level live median regressions exceed 10%: dense ranges from
10.70–20.22%, sparse from 14.14–22.63%. Historical pair-level regressions range
from 126.01–164.15% dense and 124.63–148.17% sparse. The requirement remains
unmet; neither the lower live regression in this run nor green CI authorizes
merging. Concurrent native query pooled medians changed by -5.53% dense and
-1.06% sparse; these do not isolate the cause of earlier query variance.

The binary SHA-256 values are recorded in the raw file. Candidate:
`d1f2dd4020533f4b6c6db88f9d798def1ac84bb67f6d80f37e99d0f357215e69`.
Individual timings, throughput, logical storage and whole-process RSS are
retained. With only 15 observations per metric/profile/revision, nearest-rank
p95 is the maximum and must not be described as production tail latency.

The first attempt completed the baseline oracle successfully, but the sandbox
denied `/usr/bin/time` its `kern.clockrate` read and that wrapper returned an
error. The comparison was restarted with working system timing; the interrupted
attempt was not combined with these measurements.

## Larger caller-batch diagnostic

[Raw results](2026-09-11-checkpoint-large-batch.jsonl) additionally use 1,000,000
rows, 500,000-row calls and the default 1,000,000-row segment target, with the
same baseline/v5 executables. Three alternating pairs per profile, one iteration
per process, passed every exact oracle. These sizes exercise the historical
writer's direct-compaction path and match its normal-memory row batching limit.
They do not model the engine's additional 2,048-block cap, metadata publication,
or live per-block calls. No local build or test ran during these measurements.

| Profile | Metric | Baseline median ms | Candidate median ms | Change |
| --- | --- | ---: | ---: | ---: |
| dense | live_storage_ingest | 730.832 | 770.647 | +5.45% |
| sparse | live_storage_ingest | 744.898 | 736.842 | -1.08% |
| dense | historical_storage_ingest | 366.541 | 621.136 | +69.46% |
| sparse | historical_storage_ingest | 348.339 | 727.714 | +108.91% |

Larger calls reduce some overhead but do not resolve historical ingestion.
Their live figures are diagnostics of a large storage call, not evidence of
acceptable live sync performance. The original small-call regressions remain
recorded and unacceptable. A separate storage-publication fixture now follows
the actual live/historical metadata call sequences, including the 8,192-header
window and empty blocks; it must be evaluated before drawing pipeline conclusions.

## Repeated worker comparison

[Raw results](2026-09-11-checkpoint-workers.jsonl) cover five alternating process
orders per profile, three iterations each: 30 processes and 90 measured workload
iterations. This compares checkpoint candidates with one another, not with the
original baseline. All exact oracles passed.

| Profile | Metric | v2 median ms | v3 median ms | v4 median ms | v4 versus v2 |
| --- | --- | ---: | ---: | ---: | ---: |
| dense | live_storage_ingest | 516.071 | 518.834 | 472.993 | -8.35% |
| dense | historical_storage_ingest | 591.926 | 578.823 | 537.125 | -9.26% |
| dense | compaction | 109.961 | 82.235 | 81.483 | -25.90% |
| sparse | live_storage_ingest | 521.836 | 518.926 | 468.008 | -10.32% |
| sparse | historical_storage_ingest | 635.841 | 589.183 | 579.085 | -8.93% |
| sparse | compaction | 119.944 | 83.965 | 84.086 | -29.90% |

Retain parallel raw compaction: its isolated compaction median improves by
25–30%, with historical ingestion also improving. Retain four-worker append
groups: live ingestion improves by about 9–10% relative to v3, dense historical
by about 7%; sparse historical changes by less than 2%. These are improvements
within an unaccepted prototype, not evidence that the user’s 10% limit relative
to the original baseline has been met. All individual/tail timings and RSS are
available in the raw records. No correctness check or synchronization guarantee
was removed to obtain these gains.

## Attribution

A separate, instrumented one-iteration dense v3 run measured these inclusive
phase times. Instrumentation was removed after building its copied executable;
it is not part of production code or the acceptance timings. Manifest time is
also included inside compaction, so the columns below must not be summed.
Untimed startup also contributes a small initial manifest publication.

| Phase | Live ms | Historical ms |
| --- | ---: | ---: |
| Whole measured ingestion | 527.88 | 550.11 |
| WAL append/synchronization, 25 calls | 148.39 | 156.02 |
| Raw append, 28 calls | 254.56 | 216.04 |
| Manifest publication, 37 calls | 69.23 | 69.22 |
| Checkpoints, 2 calls | 25.37 | 25.61 |
| Historical compaction, 4 calls | — | 66.04 |

The original historical path had no WAL, while the prototype journals historical
writes to recover multi-batch staging/compaction failures. Both the extra WAL I/O
and ordered column publication therefore need attention. The prototype amortizes
full column flushes, but still orders files before each manifest; it is not a
buffered column checkpoint that can discard and rebuild an entire unflushed epoch.

## Correctness and outstanding scope

The [recovery protocol](../storage-commit-replay.md) documents the active/complete
journal states, bounded WAL, route changes, maintenance barriers and restart.
Cross-device ordering was corrected during this work: ordering barriers on two
devices do not order them relative to one another. External-device data now
completes a full sync before a manifest or later catalog can publish its reference.
A forced-device unit test covers that branch and error propagation. A final
review regression also reproduced missing file synchronization beneath directory
symlinks; traversal now follows those directories and rejects cycles/excessive
nesting before publishing. Actual
isolated cross-device mount and power-loss validation remain pending.

The expanded historical failure matrix caught an empty-WAL recovery check that
mistook a compacted segment's retained canonical bitmap for raw columns. Recovery
now uses the manifest's representation before requiring those raw files. A second
regression covers partially deleted obsolete raw files after compaction. A
deterministic duplicate-descriptor test also reproduced the intermittent lock
release symptom; final managed ownership now explicitly unlocks, while detached
compaction plans retain that ownership.

Remaining work includes performance within the 10% limit, production batch shapes and metadata/coverage publication costs,
current Linux/macOS CI, removable-volume measurements, and the
later integrated staging soak. No production disk or service was modified. The
local host currently has only its internal volume mounted; these APFS results do
not establish performance on the deployment's external filesystem.

A final state-transition regression also reproduced silent journal removal when
both a completed checkpoint's committed rows and its retired WAL were missing.
That state now returns an error and preserves the journal. WAL file aliases sync
the actual created target directory, and journal ordering covers a different
WAL device. These final correctness changes require current validation and do
not make the earlier performance samples acceptance evidence.

## Validation checkpoint

All six current local gates pass: formatting, all-target workspace check, strict
all-target Clippy, 823 workspace tests (four ignored benchmarks), doc tests and
release node build. Storage contributes 117 passing tests. The first workspace
check found one immutable reorg guard after the mutable API change; that caller
was corrected before the complete successful run. The pre-fix raw cleanup and
duplicate-descriptor regressions both failed as expected, then passed after fixes.

The initial sparse-image attempt returned `Operation not permitted`. A later
blank `UDIF` image succeeded without changing system protections. The
[isolated ExFAT report](2026-09-11-exfat.md) records 117 passing storage tests
on `mac-mini`, syscall compatibility and a new actual cross-filesystem recovery
test. Existing external-volume contents remain untouched. Image-backed results
do not establish physical USB-drive performance or power-loss durability.

These green correctness gates do not satisfy the performance requirement.


Current harness SHA-256: `c34cc83ef7e87251209856c835e745564ef02274ff09d07a720b1e45fde1a8e7`.
Current lockfile SHA-256: `58e0b2b08b195217bea2a440dfd63e6ab01925df78a5251a6d07986fe830b795`.
