# Combined sync ingestion checkpoints

This is an **unmerged prototype** continuing PR #130. It addresses B2-10 in the
sync callers and tests the user's proposed bounded rewind/re-fetch approach.
Performance acceptance, platform validation and the wider audit remain open.
The user permits incompatible changes if they materially help performance and
is willing to perform a fresh sync. The current candidate changes the catalog
format to version 3; segment/column encodings remain version 1. No existing
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

## Catalog version 3 recovery protocol

The checksummed catalog is the single durable authority for segment row counts,
allocation IDs, canonical head/header window, chain anchors and historical progress.
The legacy filename `catalog.json` is deliberately retained: older binaries must
fail to parse the new bytes at their known path rather than create another catalog.
Its contents are now a binary frame: eight-byte `LXCAT003` magic, two little-endian
32-bit lengths (metadata and header list), a CRC32, JSON metadata and a canonical
RLP list of the complete recent headers. The checksum covers the first 16 prefix
bytes and both payloads. It detects accidental corruption, not malicious tampering.

Reads/encodings are bounded to 64 MiB, the header list to 8,192 entries, and each
header frame to 16 KiB before decoding its body. Truncation, trailing bytes, bad
checksums, unknown metadata fields and unsupported versions fail explicitly.
Segment IDs, relative paths and active ownership are checked. Cached headers must
have representable RLP optional-field sequences and at most 32 bytes of extra data;
callers reject these encoding errors before publishing rows or changing progress.
Chain authentication and fork/ancestry validation remain separate invariants.
The disk-size cache reads only small metadata as an untrusted hint; it never uses
that shortcut for recovery, coverage or query state.

1. Keep the durable catalog unchanged during an ingestion epoch. Update rows and
   progress in memory. Files containing a previously committed prefix still order
   complete replacements before rename; older rows cannot be sacrificed.
2. Newly allocated segments, and an initial active segment with zero committed
   rows, can defer artifact/manifest synchronization. They contain no data the
   catalog promises. Exclusive first creation avoids an unnecessary temporary
   rename; replacements of existing files stay atomic for current readers.
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

Version 1 and the unmerged version 2 directories are rejected with an actionable
diagnostic; they are not migrated, reset or rewritten. A missing catalog alongside existing artifacts, or
a dangling catalog alias, also fails instead of initializing an empty database.
Older binaries fail to parse the binary frame at the original catalog path.
A deployment must use a **new empty data directory** and verified fresh sync. Rolling back the binary
requires its original directory or another fresh sync, not reuse of version 3.
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
suite at that stage passed 129 tests with two explicitly ignored cases, including
recovery-evidence and malformed-uncommitted-artifact checks. All six [local gates](baselines/2026-09-11-catalog-v2-gates.jsonl)
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

[The barrier-only comparison](baselines/2026-09-11-catalog-barrier-comparison.jsonl)
at `b2b8b7b9` repeats the same 18 processes/54 iterations with exact oracles passing.
Short historical publication falls to 21.042 ms versus a paired 14.928 ms baseline
(+40.96%); large history is 80.922 versus 66.414 ms (+21.85%). Grouped live is
495.086 versus 2,870.973 ms (-82.76%); per-block live remains 3,340.908 versus
2,773.894 ms (+20.44%). The isolated change reduces historical cost but does not
meet acceptance. Per-block live mostly writes existing prefixes, for which there
was no deferred-tree barrier to remove.

All six Linux/macOS CI jobs for catalog candidate `adff367c` pass in run
`34533144525`. That CI does not cover later performance changes. The next
experiment removes temporary creation/rename only when exclusive creation proves
a wholly uncommitted destination does not exist; existing files retain atomic
replacement. Its full storage suite passes 130 tests with two ignored, and
strict storage Clippy passes. Its timing is pending.

[Exclusive-creation measurements](baselines/2026-09-11-direct-create-comparison.jsonl)
at `57c1e67e` use three alternating short-fixture pairs with three iterations.
Short history is 20.299 versus 14.659 ms (+38.48%); grouped live is 491.057 versus
2,914.074 ms (-83.15%). The small APFS change needs further confirmation against
noise and on ExFAT before treating it as a retained optimization. Per-block live
and large history were not remeasured for this isolated first-creation change.

[Per-block live profiling](baselines/2026-09-11-live-checkpoint-profile.json)
shows approximately 8 ms/block encoding the entire 8,192-header window and another
7.5-8 ms/block in final synchronization. This identifies repeated large metadata
serialization/publication as a larger target than first-file renames. The next
format experiment will consider a compact encoding for cached headers while
retaining one authoritative catalog and the full recent-header window; it must
be measured before acceptance. No profiling hooks remain in production code.

## Compact cached-header experiment

[Encoding diagnostics](baselines/2026-09-11-cached-header-encoding.json) identify
an existing codec that avoids repeatedly serializing multi-megabyte JSON header
windows. With 8,192 populated synthetic headers, median RLP encoding is 1.443 ms
and 5,365,633 bytes versus JSON 8.254 ms and 13,465,461 bytes. The minimal fixture
is 1.017 ms / 4,153,348 bytes versus 6.067 ms / 10,067,969 bytes. All 18 samples
pass exact RLP round-trip equality. Fixed codec ordering makes this a diagnostic,
not a performance acceptance result. LZ4 added about 13 ms for populated headers,
so compressing the JSON is not justified by these samples.

Version 3 uses Alloy's already locked RLP implementation, retaining all headers,
checksums and checkpoint ordering. No package versions or column encodings change.
Removed the obsolete JSON envelope/raw-value feature and the server's duplicate
catalog parser. New tests cover optional fields, oversized/trailing RLP, invalid
encoding inputs before publication, and segment-ID exhaustion. The last two
regressions failed before their fixes. Full version 3 gates and actual ingestion
comparisons are in progress; earlier version 2 results do not validate version 3.

## Catalog v3 release comparison

All six [local gates](baselines/2026-09-11-catalog-v3-gates.jsonl) pass for committed
`3f457987`: 841 tests passed, seven explicitly ignored cases, strict workspace
Clippy, doc tests and release linking. [Paired release samples](baselines/2026-09-11-catalog-v3-comparison.jsonl)
use four profiles, three alternating process pairs each, three fresh iterations
per process. Final checkpoint/finalization remains timed; every exact row and
progress/reopen oracle passes. The fixture digest and tip hash match across each
pair. Builds/tests were stopped during timing. Hardware, cache conditions,
compiler, binary and fixture hashes are captured in the raw output. The v3
[baseline adapter](baselines/2026-09-11-publication-v3-baseline.patch) preserves
original production code while giving it identical fixture inputs and oracles.

| Workload | Baseline median ms | Catalog v3 median ms | Change |
| --- | ---: | ---: | ---: |
| Grouped live, minimal headers | 2,777.323 | 469.196 | -83.11% |
| Historical, 15,360 rows | 14.780 | 19.751 | +33.63% |
| Historical, 491,520 rows | 74.197 | 81.285 | +9.55% |
| Per-block checkpoint live, minimal headers | 2,801.352 | 1,985.857 | -29.11% |
| Per-block checkpoint live, populated headers | 3,509.579 | 2,115.962 | -39.71% |

The compact header format removes the measured live regression, including the
per-block boundary and populated fields. It does **not** finish performance
acceptance: short history remains above 10%; large history narrowly fits in this
run and needs confirmation because its earlier baseline median was lower.
These are storage-call timings, not end-to-end sync throughput. Process peak RSS
medians were ~53-55 MiB candidate versus ~662-867 MiB baseline for per-block live;
large-history RSS was ~1,270 MiB on both, including fixture/oracle allocations.
All six Linux/macOS CI jobs pass for `3f457987` in run `34538163643`. Historical
raw writes/compaction, current cross-device and ExFAT recovery checks, and remaining
storage review still gate merge.

[Historical phase attribution](baselines/2026-09-11-catalog-v3-history-profile.json)
shows short raw writes around 8 ms, compaction 5.5 ms and checkpoint 7-8.5 ms.
In warm large calls, preparing/encoding the payload column takes 60-65 ms, while
its page writer takes 31-38 ms. These overlapping worker durations include
scheduling; they are not exclusive CPU totals. Code inspection confirms an entire
column of cloned `Bytes` before paging and an unused raw buffer built even for
adaptive encoding. The next experiment borrows payloads per page and removes
that unused adaptive-path serialization. Temporary profiling scopes were removed.

[Payload-borrowing comparison](baselines/2026-09-11-borrowed-data-comparison.jsonl)
at `45372779` versus `3f457987` isolates page-sized borrowed payload references and
removal of unused adaptive-path raw serialization. Three alternating pairs with
three iterations each pass exact row/progress/reopen oracles. Large-history median
falls 80.056 → 71.005 ms (-11.31%); short history is 20.021 → 20.737 ms (+3.58%).
The latter path does not use the full-column borrowing change; repeat it with the
next short-write investigation to distinguish noise from an encoder regression.
All 134 storage tests and strict storage Clippy pass. The original-baseline 10%
ceiling still applies, and neither this isolated result nor earlier CI permits merge.
