# Live segment bundles

Unmerged successor to `c63fb9ed` in draft PR #130. Fresh live sync segments now
use the existing immutable bundle representation. Catalog v7 (`LXCAT007`) and
segment manifest v5 allow both hot and sealed bundle descriptors; bundle v3 is
unchanged. Prior catalogs are rejected without rewriting or deleting their data.
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
This new failure is not covered by the preceding green gates and must be fixed
before merge. Index freshness must be checked without adding per-ingestion metadata
writes; partial publication and concurrent rebuilds also need explicit handling.

Integrated release ingestion comparisons are running. Current-tree gates,
platform validation, query/startup cost, disk growth and write amplification
remain acceptance work.
PR #130 stays draft and unmerged. Broader query snapshot lifetime and index
publication review are still required; the new reader tests do not complete those
audit batches.
