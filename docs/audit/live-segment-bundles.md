# Live segment bundles

Unmerged successor to `c63fb9ed` in draft PR #130. Fresh live sync segments now
use the immutable bundle representation. The grouped-table follow-up summarizes bounded
groups of updates; see [its measurements](bundle-table-groups.md). The current
[table-record candidate](bundle-table-codecs.md) uses catalog v9 (`LXCAT009`),
segment manifest v7 and bundle v5, with full validation still in progress.
The live-bundle measurements below describe the preceding v7/v5/v3 milestone. Prior catalogs are rejected without rewriting or deleting their data.
The user authorized fresh incompatible directories, but no production data reset
or deployment has occurred.

## Why this change

The ordered-checkpoint candidate still regresses Intel ExFAT per-block live
publication by 26.31%. Profiling identifies raw column updates and filesystem
publication as substantial costs. Skipping AppleDouble companion flushes did not
produce a meaningful improvement and was rejected. Reuse of the historical
bundle writer avoids introducing another payload format or recovery journal.

Two isolated feasibility comparisons used three rotating process triplets, three
samples per process, 128 live blocks, 128 rows per nonempty block, 8,192 cached
headers, and publication after each block. Identical row, progress and reopen
oracles passed. Times include final checkpoint costs; caches were not evicted.

| Disposable ExFAT host | Original `09a63f55` | Ordered `c63fb9ed` | Bundle prototype | Prototype vs original |
| --- | ---: | ---: | ---: | ---: |
| Apple Silicon | 7,572.695 ms | 6,291.068 ms | 4,112.006 ms | -45.70% |
| Intel Mac mini | 13,114.180 ms | 16,672.930 ms | 8,196.888 ms | -37.50% |

Prototype vs current is -34.64% on Apple Silicon and -50.84% on Intel. Intel
current independently repeats its failure at +27.14%. Both disposable images
were detached after timing. These are storage-call measurements, not network
throughput, full-node acceptance or power-loss proof. Raw
[Apple Silicon](baselines/2026-09-11-live-bundle-feasibility.jsonl) and
[Intel](baselines/2026-09-11-live-bundle-intel-feasibility.jsonl) records capture
source patches, binary identities, pinned toolchain, hardware and runner details.
The integrated implementation adds empty-raw-artifact cleanup and regression
coverage; it requires its own measurements before these results can be accepted.

## Invariants

- Fresh combined live sync writes use bundled columns and immutable canonical
  bits. Existing nonempty raw hot segments retain their raw append path. Generic
  row-only writes still start raw segments and retain their WAL contract; they
  can append to an existing live bundle after the sync checkpoint is hardened.
- Bundle append reserves capacity before writing and rotates when its bounded
  extent capacity is exhausted. It reuses the validated reader for append.
- The catalog pins the complete immutable table. Startup reconstructs derived
  hot manifests from that reference and trims only unpublished suffixes. Missing
  or corrupt committed bytes remain an explicit error and are preserved.
- An empty catalog prefix can recover even when a first bundle reached disk
  before its manifest. Retry removes known empty raw artifacts produced by that
  rollback; it does not convert or delete a nonempty committed raw prefix.
- [Bounded ordered publication](published-ingestion-checkpoints.md) is unchanged:
  the shared 32 MiB / 64-call / five-second window and hard boundaries still apply.
  This optimization removes file work, not integrity or ordering checks.

## Validation and remaining work

The new live bundle/snapshot test fails against the preceding implementation
because live segments are raw; its [before log](baselines/2026-09-11-live-bundle-before.log)
is retained. Integrated focused tests pass for first-bundle publication at every
injected main-thread I/O phase, cleanup after rollback to empty, WAL appends with
and without rotation, exact repeated reopen, and preservation of canonical bits.
Both routes pass torn append/manifest recovery and reorg publication/snapshot
checks. Capacity exhaustion is exercised with both hot and historical bundles.

Existing raw compaction protection now seeds a raw WAL prefix so it continues to
test a real compaction candidate. Malformed metadata still rejects hot unbundled
compacted manifests; hot bundle manifests are intentionally valid. Corruption
fixtures damage the actual bundle instead of writing unrelated raw filenames.

All six workspace gates passed before adding the query/index-growth test:
888 tests passed, nine ignored. The first sandbox attempt could not bind loopback
listeners; the authorized rerun passed. [Storage validation record](baselines/2026-09-11-live-bundle-storage-validation.jsonl).

The subsequent query integration test **fails** after growing an indexed hot
segment and rotating it: native filtering returns 10,082 matches instead of
11,322. The persisted index still describes 8,192 rows in the segment that has
since grown to 10,000. Existing lookups do not verify index coverage against the
reader. [Failing regression](baselines/2026-09-11-live-bundle-query-before.log).
This failure was not covered by the preceding green gates. The subsequent
[index-checkpoint fix](index-checkpoints.md) now passes both raw and bundled query
regressions, including SQL counts/order, reorg and restart. It binds index
publication to the source state without per-ingestion metadata writes.

The integrated storage comparison uses five alternating process pairs, three
samples per process and the original v3 oracles. All ten measured medians meet
the 10% ceiling: historical profiles range from -82.73% to +4.58% (large history);
grouped live is -95.35%, per-block live -68.89%, one-row live -67.37%, and rich
header live -70.05%. The benchmark binary predates the index-checkpoint follow-up.
[Integrated storage record](baselines/2026-09-11-live-bundle-integrated-broad.jsonl).
The combined index-checkpoint implementation passes all six local workspace
gates (895 tests/nine ignored). The platform checks below pass; query/startup/index costs and sparse bundle
growth still prevent complete acceptance.
PR #130 stays draft and unmerged. Broader query snapshot lifetime and index
publication review are still required; the new reader tests do not complete those
audit batches.


## Integrated platform and lifecycle checks

Saved `196664e4` passes all six Linux/macOS CI jobs. Exact integrated `6ce06c13`
binaries pass the full storage suite (184 passed/four ignored), all 128
cross-mount recovery cases, and the query harness (five passed/one ignored) on
both Apple architectures' disposable ExFAT images. Both images detached after
validation; subsequent timing uses a separate attachment. The later clock-only
test correction does not change production behavior.
[Platform record](baselines/2026-09-11-live-bundle-platform-validation.jsonl).

The [index follow-up](index-checkpoints.md) records the first combined lifecycle
comparison, including remaining index-build, generic row-only API and warm-start
costs. These are not hidden by the combined sync improvements. Exact integrated
Intel ExFAT timings now pass the ingestion ceiling, with 15 samples per revision:

| Intel disposable ExFAT workload | Original median | Integrated median | Change |
| --- | ---: | ---: | ---: |
| Tiny historical calls | 624.348 ms | 128.003 ms | -79.50% |
| Short history | 256.049 ms | 63.902 ms | -75.04% |
| Per-block live | 13,122.895 ms | 8,122.487 ms | -38.10% |
| One-row live | 12,147.744 ms | 8,088.388 ms | -33.42% |

Exact oracles pass, no build/test workload overlapped timing on that host, and
the disposable image detached. [Raw integrated Intel comparison](baselines/2026-09-11-live-bundle-integrated-intel-exfat.jsonl).

## Mixed payloads and sparse growth

The extended benchmark retains v3 transfer fixtures and adds v4 mixed payloads
with independently hashed 0–1024-byte data. Source adapters, immutable binary
hashes, environment, runner and raw records are retained in the
[lifecycle comparison](baselines/2026-09-11-live-bundle-lifecycle.jsonl).
Original/candidate production revisions are `09a63f55`/`196664e4`; only the harness
changes for these measurements. A macOS dev-dependency on already locked `libc`
adds IO accounting to the original fixture without changing production versions.

| ARM APFS workload | Samples each | Ingestion change | File bytes, original → current | Full-row validation, original → current |
| --- | ---: | ---: | ---: | ---: |
| Mixed history, 2,048 blocks, 128 logs/nonempty block, 64-block calls | 15 | -87.18% | 46,876,970 → 46,423,812 | 134.047 → 137.119 ms |
| Mixed live, 128 blocks, publication each block | 15 | -70.81% | 16,415,724 → 7,373,192 | 4.441 → 9.735 ms |
| Sparse history, 1,024 one-block calls | 15 | -63.45% | 136,843 → 9,352,715 | 0.947 → 14.611 ms |
| Sparse live, 1,024 blocks, publication each block | 3 | -67.70% | 10,344,096 → 13,504,216 | 0.592 → 14.913 ms |

Every sixteenth block is empty, so each sparse dataset contains 960 logs. All
four profiles retain one nonempty segment. The sparse live run has only three
samples and is a growth diagnostic, not a tail-latency acceptance result. Full-row
validation includes reading, sorting and exact comparison; it is not SQL latency.

OS-attributed write medians decrease 94.24% / 64.25% / 88.18% / 59.24% in table
order. These counters do not measure physical NAND write amplification. Warm
reopen changes from 0.782 → 14.451 ms for mixed history, 19.990 → 18.882 ms for
mixed live, 0.455 → 14.651 ms for sparse history, and 18.101 → 26.945 ms for sparse
live. Caches are not evicted. No local builds/tests overlapped timing.

**Remaining finding:** tiny appends retain excessive table/page metadata and
require many small extent reads. Sparse history uses 68 times the original file
bytes and roughly 15 times full-row validation time despite faster ingestion.
This is substantially better than the rejected 295 MB format, but is not accepted
as the final layout. Reduce metadata and read overhead without relaxing bounds,
checksums, catalog authority or the user's ingestion ceiling. Index publication
and generic API costs from the other comparison also remain visible.

The extended harness passes formatting, workspace check, strict Clippy, its
ordinary fixture, and both release fixtures' exact oracles.
[Validation](baselines/2026-09-11-publication-lifecycle-validation.json).


The [read-cost follow-up](bundle-read-costs.md) now measures a bounded nearby-extent
read window. All six local gates pass (896 tests/nine ignored); sparse read/reopen
medians improve about 40% against this integrated candidate while mixed workloads
remain within 2%. The focused 45-sample repeat confirms the read/tail gain; final platform checks remain. No table
compression or logical-offset lookup change is retained, and metadata growth is
still unresolved.
