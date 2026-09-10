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

An isolated ExFAT-image attempt used `hdiutil create -size 512m -fs ExFAT` in a
new `/tmp` directory. The host returned `Operation not permitted`, created no
image and mounted nothing. Actual ExFAT/cross-device validation is therefore
blocked on an environment that permits an isolated test mount. This was an OS
operation failure, not evidence that the filesystem implementation passed or failed.
No production mount was accessed and no system protection was changed.

These green correctness gates do not satisfy the performance requirement.


Current harness SHA-256: `c34cc83ef7e87251209856c835e745564ef02274ff09d07a720b1e45fde1a8e7`.
Current lockfile SHA-256: `58e0b2b08b195217bea2a440dfd63e6ab01925df78a5251a6d07986fe830b795`.
