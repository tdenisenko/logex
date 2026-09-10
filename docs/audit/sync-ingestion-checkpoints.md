# Combined sync ingestion checkpoints

This is an **unmerged prototype** continuing PR #130. It addresses B2-10 in the
sync callers and tests the user's proposed bounded rewind/re-fetch approach.
Performance acceptance, platform validation and the wider audit remain open.
The user permits incompatible changes if they materially help performance and
is willing to perform a fresh sync. The current candidate changes the catalog
format to version 2; segment/column encodings remain version 1. No existing
production dataset has been reset or modified.

## Behavior and invariants

Live blocks, forward-gap chunks and historical chunks now associate their rows
and progress in one storage operation. Each operation takes complete validated
blocks, including blocks with no logs. Subscribers are notified after that
operation succeeds. Forward-gap publication uses its final header window once;
the superseded list of per-block 8,192-header snapshots is removed.

Successful sync operations become queryable in the running process. They are
not individually promised durable across restart: a restart may discard the
unfinished epoch and resume the existing verified fetch pipeline from the last
durable head/floor. `checkpoint()` makes the current rows and progress durable.
This is an intentional acknowledgment change for the combined sync APIs,
authorized by the user's preference for bounded re-ingestion over duplicated
recovery row data. Generic `write_batch`/`write_historical_batch` retain their
WAL-backed durable behavior; their separately published progress is not a
transaction, and sync no longer uses that sequence.

An epoch contains at most 32 MiB of equivalent encoded row payload or 64 caller
batches. Validation computes the size without serializing a second row copy.
A valid oversized caller batch checkpoints before returning. Five seconds makes
an epoch eligible for checkpoint at the next write or existing background tick;
it is not a hard wall-clock deadline. Route changes, generic durable writes,
standalone metadata updates and relevant maintenance boundaries checkpoint first.
Detached compaction plans exclude the epoch's affected segments.

## Catalog version 2 recovery protocol

The checksummed `catalog.json` is the single durable authority for segment row
counts, allocation IDs, canonical head/header window, chain anchors and historical
progress. Its versioned envelope contains the catalog JSON and its CRC32, with a
64 MiB read/encoding limit. The checksum detects accidental corruption, not
malicious tampering. Segment IDs, relative paths and active ownership are checked.
Unknown fields and unsupported versions fail explicitly.

1. Keep the durable catalog unchanged during an ingestion epoch. Update rows and
   progress in memory. Files containing a previously committed prefix still order
   complete replacements before rename; older rows cannot be sacrificed.
2. Newly allocated segments, and an initial active segment with zero committed
   rows, can defer artifact/manifest synchronization. They contain no data the
   catalog promises. Their filenames are published atomically for current readers.
3. At checkpoint, submit all deferred segment trees and parent directory entries,
   order them before the catalog, and fully synchronize any other devices first.
   Publish one atomic checksummed catalog and complete its device synchronization.
   The catalog can then reference only complete, already-ordered segment contents.
4. On startup, validate catalog and WAL evidence before modifying rows. Restore
   active segment prefixes and canonical bits to the catalog's counts; discard
   only canonical segment IDs at or above its allocation boundary. Never infer
   committed rows/progress by adopting newer manifests. Missing committed
   artifacts fail explicitly. Damage confined to wholly uncommitted segments can
   be discarded, including malformed initial-hot manifests and partial columns.
5. Publish each restored raw prefix durably before removing obsolete compressed
   pages. The catalog remains unchanged, so an interrupted rollback repeats
   safely. The existing verified sync path re-fetches from its restored head/floor.

Recovery runs under exclusive data-directory ownership before queries. Complete
caller batches define checkpoints, including empty blocks and blocks split across
segments. Chain finality is not a substitute for local persistence ordering.
Generic WAL-backed writes retain their recovery journal and use the same catalog;
WAL validation precedes rollback so damaged recovery evidence remains inspectable.

The separate sync `storage_state.json`, `wal/ingestion.json`, publishing decision,
and pair of staged metadata files from `aefbc0db` are superseded. This removes
several full flushes per checkpoint and avoids duplicating the header window in
JSON serialization. It intentionally drops automatic manifest adoption on startup.

## Fresh-sync compatibility decision

Version 1 directories are rejected with an actionable diagnostic; they are not
migrated, reset or rewritten. A missing catalog alongside existing artifacts, or
a dangling catalog alias, also fails instead of initializing an empty database.
Older binaries cannot read the new envelope as an old catalog. A deployment must
use a **new empty data directory** and verified fresh sync. Rolling back the binary
requires its original directory or another fresh sync, not reuse of version 2.
Retain original directories until their owner explicitly chooses otherwise.
The user's protected external-volume contents are outside this test scope.

This breaking change is a performance candidate, not an accepted release. It will
be retained only with measured evidence meeting the user's 10% limit. Starting
fresh alone does not improve performance; removing redundant durability work is
the intended benefit.

## Validation and remaining work

The preceding `aefbc0db` prototype passed all six local gates (833 tests, six
ignored; [results](baselines/2026-09-11-sync-checkpoint-gates.jsonl)). Those results
do **not** validate the new catalog/deferred-publication protocol. Its storage
suite passes 129 tests with two explicitly ignored cases, including the additional
recovery-evidence and malformed-uncommitted-artifact checks. Strict workspace
Clippy also passes. All six [local gates](baselines/2026-09-11-catalog-v2-gates.jsonl)
pass for `adff367c`: 836 tests passed, six intentionally ignored, and the release
build succeeds. Performance and platform validation remain pending.

The current regressions exercise live/historical retry, empty progress, rotation,
compaction, prior non-canonical rows, each main-thread write/commit failure point,
interrupted rollback, origin damage, route/API/configuration changes and actual
child-process exit. Catalog tests cover every truncation/single-byte mutation,
valid-checksum invalid identities, old versions, oversized input and missing
aliases. Worker failures and physical power-loss interleavings are not exhaustively
covered by the main-thread injection matrix.

Pending: new release comparisons including large historical calls and per-block
live checkpoints; Linux/macOS CI; isolated ExFAT/distinct-device
recovery; concurrent-query failure behavior; recovery memory/startup cost; and the
broader storage audit. Previous ExFAT results apply to `ff728ea3` only.

## First release comparison

[Raw results](baselines/2026-09-11-sync-checkpoint-publication.jsonl) compare the
original `09a63f55` baseline with prototype `aefbc0db` on internal APFS, using the
same pinned compiler, Mac14,15, 16 GiB RAM and warm-cache conditions as the prior
publication fixture. Three alternating process pairs, three iterations per
process, 128 measured blocks, 128 rows per nonempty block and the 8,192-header
window. No build or test ran during timing. Both revisions pass the exact row,
head, anchor, floor and reopen oracles; final checkpoint/finalization is included.

| Workload | Baseline median ms | Prototype median ms | Change |
| --- | ---: | ---: | ---: |
| Live storage publication | 2,799.517 | 789.061 | -71.81% |
| Short historical storage publication | 14.960 | 51.719 | +245.72% |

This establishes a live improvement for this fixture, not end-to-end P2P sync or
paced live performance. The short historical fixture fits in one sparse staging
chunk; it still fails the 10% ceiling badly. Removing its WAL copy did not resolve
that cost. The next investigation separates raw-column, compaction and checkpoint
costs and measures larger production-shaped history chunks before selecting a
format change or a further durability optimization.

A [temporary release attribution run](baselines/2026-09-11-sync-checkpoint-profile.json)
separated the short historical append (24.17 ms) and finalization/checkpoint
(32.64 ms). Full directory syncs account for approximately 21.86 ms across those
phases, while raw-file flush helpers account for another 13.45 ms during append.
Helper totals include worker overlap; this is instrumentation evidence, not an
acceptance benchmark. A 491,520-row historical call took 156.96 ms in the same
instrumented diagnostic, including its automatic oversized-batch checkpoint;
its corresponding baseline comparison is still needed. All temporary profiling
code was removed after capture.

These measured costs motivated catalog version 2 and deferred publication as
described above. The historical comparison records the rejected earlier design;
it must not be presented as performance of the new candidate.

## Catalog v2 release comparison

[Raw samples](baselines/2026-09-11-catalog-v2-comparison.jsonl) compare original
`09a63f55` with `adff367c` on internal APFS (Mac14,15, 16 GiB, pinned nightly
2026-08-24). Three alternating pairs per profile, three iterations per process;
all exact row/head/anchor/floor/reopen oracles pass. Final checkpoint/finalization
is included; no build or test ran during timing. Fresh directories, OS caches
not evicted, 8,192 warm headers and a million-row segment target. Fixture v2 adds
route selection and per-block checkpoint controls, applied consistently to the
baseline harness; baseline calls already publish separately. The per-block case
forces the candidate's durability boundary without sleeping or network traffic.
Binary hashes and fixture digests are in the raw records. `/usr/bin/time -l`
required access to system counters; the initial sandboxed process passed its
oracle but failed resource collection and was rerun, not combined with results.

| Workload | Baseline median ms | Catalog v2 median ms | Change |
| --- | ---: | ---: | ---: |
| Live, 128 blocks / 15,360 rows | 2,854.581 | 515.835 | -81.93% |
| Historical, same short input | 14.781 | 22.782 | +54.13% |
| Historical, 2,048 blocks / 491,520 rows | 67.838 | 85.921 | +26.66% |
| Live, checkpoint after every block | 2,759.964 | 3,322.951 | +20.40% |

**This candidate still fails performance acceptance.** Grouped live improvement
cannot be generalized to sparse live traffic or historical sync. Full local
gates pass, but this PR must remain unmerged.

[Temporary phase profiling](baselines/2026-09-11-catalog-v2-profile.json) shows
checkpoint ordering/full synchronization at roughly 8-10 ms and short raw writes
at roughly 8-9 ms after warmup. Individual file submission is below 0.2 ms in
these samples, so parallelizing file fsync is not supported by this profile.
Potential next reductions are redundant ordering before the catalog's own barrier,
first writes to unpublished files, and per-call publication that can safely defer
to the same catalog checkpoint. Committed prefixes and current readers still
require protection; any optimization needs its own recovery and timing evidence.

The next isolated change prepares the catalog temporary file before the barrier
for deferred trees. The same barrier orders both payloads before publishing the
catalog's name; other devices are fully persisted first, and the final catalog
parent sync still supplies durability. Nine focused ingestion/recovery tests
pass, including both interruption matrices. New timing evidence is pending.
