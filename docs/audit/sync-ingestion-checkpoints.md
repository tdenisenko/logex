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

[Four-worker initial-write comparison](baselines/2026-09-11-raw-four-comparison.jsonl)
at `2355d6f4` versus `45372779` reduces short-history median 19.897 → 17.237 ms
(-13.37%), with three alternating pairs and exact oracles passing. It uses the
same four column groups as appends; publication ordering and worker error
propagation stay intact. All 134 storage tests and strict storage Clippy pass.
This is still above the original short-history baseline's 10% allowance. The
remaining raw-to-compressed payload path allocates one `Bytes` per row; a bounded
borrowed reader can remove that overhead while validating its offsets before
allocation. That reader's corrupt-length behavior needs a regression first.

The borrowed raw-payload experiment also fixes B2-12: `read_var_bytes` previously
computed/allocated offsets from the untrusted row count before checking the table
fits in the file. The pre-fix 44-byte/`u64::MAX` fixture panics; it now returns
`InvalidData`. A shared owned file buffer validates the raw version, compression,
count arithmetic, offset table and full monotonic/sentinel layout before either
public query materialization or page-level compaction borrowing. Empty and repeated
selected rows retain their order. Query results still own individual payloads, so
a small retained result does not pin an entire raw column. The complete raw file
is still read into memory for compaction; this is not a streaming-file or globally
bounded-recovery claim. Superseded offset vectors and the unused encoder wrapper
are removed. Focused tests pass; full storage validation/timing is in progress.

[Validated raw-payload borrowing](baselines/2026-09-11-raw-borrow-comparison.jsonl)
at `2872595b` versus `2355d6f4` gives 18.079 → 17.205 ms short-history median
(-4.83%) across three alternating pairs; all exact oracles pass. The small timing
change needs confirmation against noise; the reproduced malformed-layout fix
is independently required. Full storage tests pass (136, two ignored), as do the
final layout tests and strict storage Clippy. The next isolated experiment avoids
spawning fourteen compaction workers when each column occupies only one page;
multipage raw segments retain parallel compaction. Nine focused compaction tests
and strict Clippy pass; timing is pending.

**Rejected experiment:** [single-page serial compaction](baselines/2026-09-11-single-page-rejected.jsonl)
at `2635b41c` increases the short median 16.985 → 20.208 ms (+18.97%) versus
`2872595b`. All oracles pass, but the performance result rejects the scheduling
assumption even at one page per column. Restored the previous parallel compaction
implementation; no serial-size threshold remains. Recompare the combined retained
changes against original `09a63f55` before further tuning or acceptance.

## Combined confirmation at 224a9d30

All six [local gates](baselines/2026-09-11-catalog-v3-final-gates.jsonl) pass with
843 tests and seven ignored cases. All six Linux/macOS CI jobs pass in run
`34540157764`. [Expanded original-baseline comparison](baselines/2026-09-11-catalog-v3-final-comparison.jsonl)
uses five alternating pairs for grouped live/short history and large history,
three pairs for each per-block live profile, and three iterations per process.
All exact oracles and fixture identifiers agree. No builds/tests ran during timing;
final checkpoint/finalization is included. Raw output records median and observed
nearest-rank p95 (9/15 samples are not a production latency distribution).

| Workload | Original baseline median ms | Candidate median ms | Change |
| --- | ---: | ---: | ---: |
| Grouped live | 2,889.054 | 518.361 | -82.06% |
| Short historical | 15.202 | 18.388 | +20.96% |
| Large historical | 66.830 | 71.237 | +6.59% |
| Per-block live, minimal headers | 2,850.929 | 2,067.854 | -27.47% |
| Per-block live, populated headers | 3,532.592 | 2,232.949 | -36.79% |

Short history still fails the 10% ceiling, so the candidate remains unmerged.
The large profile fits the limit in this confirmation, and live remains faster;
these are storage-call results, not measured P2P throughput. Next isolate whether
initial raw-file creation benefits from two workers instead of four; the existing
append scheduling and compaction remain unchanged in that experiment.

**Rejected experiment:** [two initial workers](baselines/2026-09-11-raw-two-rejected.jsonl)
at `6a659209` gives 17.216 → 17.776 ms (+3.25%) versus `224a9d30` in the isolated
short-history comparison. It does not demonstrate improvement; restored four
workers. Thirteen focused column/recovery tests and strict Clippy had passed.
The remaining prototype therefore matches the previously validated production
code while further performance work continues.

[All-null file extension](baselines/2026-09-11-null-extension-comparison.jsonl)
at `080c9eec` gives 19.058 → 17.134 ms (-10.10%) versus the validated `224a9d30`
in three alternating isolated short-history pairs. It extends new replacement
files to their required zero-filled logical length instead of writing each null
slot, with unchanged headers, bitmap bytes and publication ordering. Fourteen
focused column/recovery tests and strict Clippy pass, including exact file-byte
checks for new files, replacements, empty/partial-byte counts and repopulation.
This is promising but needs combined original-baseline confirmation and ExFAT
validation; it does not establish acceptance for all sync workloads.

The [mixed original-baseline confirmation](baselines/2026-09-11-null-original-comparison.jsonl)
at `080c9eec` (docs-only HEAD `5cb4518d`) still fails: five alternating pairs,
three iterations per route give short history 14.800 → 18.152 ms (+22.65%),
and grouped live 2,881.240 → 490.167 ms (-82.99%). All exact oracles pass.
The isolated all-null gain did not establish a convincing improvement over the
previous mixed confirmation; it remains provisional, and PR #130 stays unmerged.
The original historical path omitted fsync, unlike its live WAL path. Its faster
short finalization therefore includes no equivalent durability guarantee. This
explains a fixed cost but does not waive the user's 10% performance ceiling.
A bounded larger replacement-write buffer is the next isolated experiment: raw
fixed-width writes currently pass through an 8 KiB buffer, generating repeated
small writes before compression. No checkpoint ordering is relaxed.

[Larger replacement write buffers](baselines/2026-09-11-replacement-buffer-comparison.jsonl)
at `260f5c64` versus `080c9eec` reduce isolated short-history median
18.009 → 16.182 ms (-10.14%), with three alternating pairs and all exact
oracles passing. The 64 KiB buffers retain at most 2 MiB for the bounded
replacement set. Fourteen focused column/recovery tests and strict storage
Clippy pass. Combined original-baseline confirmation is still required.

Current combined-sync cross-device recovery now has an explicit ignored test
using `LOGEX_TEST_VOLUME_A/B`: both filesystem directions, live/history,
empty/three/twelve incoming rows, checkpoint versus restart rewind, repeated
reopen and exact retry. All 24 combinations pass on the disposable local
APFS/ExFAT image. The eight existing generic-WAL cross-device cases also pass
after initializing a catalog before installing the dangling WAL alias: catalog
v3 correctly rejects such an artifact in an otherwise uninitialized directory.
These are process-reopen/order checks, not physical power-cut certification.
The complete storage suite on ExFAT is running before performance confirmation.


At `83b5720e` (production identical to `260f5c64`), all six local workspace
gates pass: 844 tests and eight ignored cases. The complete local ExFAT run
reports 133 passed, four failed and three ignored: four cleanup tests assumed
that only application files could exist. A focused diagnostic found `column`
and `._column`, whose companion has the observed AppleDouble v2 header. The
assertions now allow valid companions of expected files while still rejecting
leftover temporary artifacts and their companions. No production cleanup or
integrity rule was relaxed. All 32 focused ExFAT checks now pass (10 durability, 21 reader and one
compaction oracle), including the new fixed-column/prefix behavior.
The local long run used a Cargo output binary while later reader work rebuilt
that path, so its process-spawn cases are preliminary; future full runs use an
immutable copied executable. The Intel run uses such a copy and is still running.

Fixed-width raw reader regressions reproduce B2-13 (count-allocation panics,
invalid format/layout acceptance and out-of-bounds null results). The new byte
view also removes typed-to-raw copies during compaction. Twenty-one reader cases,
a page-boundary byte-equivalence oracle and strict storage Clippy pass on APFS.
Original-baseline timing for the buffer change and isolated timing for this
next representation change remain pending; none authorizes merge.


The [original-baseline buffer confirmation](baselines/2026-09-11-buffer-original-comparison.jsonl)
still fails short history at `83b5720e`: 14.737 → 18.044 ms (+22.44%),
with grouped live 2,959.867 → 544.773 ms (-81.59%). Five alternating pairs
with three iterations pass all exact oracles. The buffer's isolated gain did
not reproduce convincingly in this mixed workload; it remains provisional.
[Fixed-view compaction](baselines/2026-09-11-fixed-view-comparison.jsonl) at
`89718b71` versus `83b5720e` gives 18.228 → 17.950 ms (-1.53%) in three
isolated pairs. This is within likely noise and is not a claimed speedup;
the reproduced reader correctness fixes independently justify the validation.
The original-baseline cap is still unmet. Further investigation must address
raw historical staging followed by a second compressed write, without creating
extra small segments or hiding finalization work in an unmeasured background job.

[Intel validation](baselines/2026-09-11-intel-buffer-validation.jsonl) at
`83b5720e` passes all 137 storage tests (three ignored) and both cross-mount
tests on the disposable ExFAT image; its copied executable also makes child
process tests use that exact build. All six Linux/macOS CI jobs pass at that
commit in run `34543665334`. Both disposable images are detached. Current
fixed-view full gates/platform checks are still required before acceptance.


All six [local gates](baselines/2026-09-11-fixed-view-gates.jsonl) pass at
`89718b71`: 849 tests and eight ignored cases. The fixed-reader change has
passed focused ExFAT tests; full current platform validation and the historical
performance ceiling remain open. A diagnostic direct-compression probe will
estimate the removable raw-staging cost. A lower dense threshold by itself is
not a production fix: it creates extra small segments. Any retained design must
continue coalescing historical chunks, preserve existing committed pages through
interruption, bound index/page growth, and include final publication costs.


The [direct-compression diagnostic](baselines/2026-09-11-direct-staging-probe.jsonl)
reduces isolated short-history median 17.137 → 12.190 ms (-28.87%) across three
alternating pairs, with exact row/progress/reopen oracles passing. It temporarily
caps the dense threshold at 8,192 rows; the captured source diff records that
one-line probe. The [coalescing regression](baselines/2026-09-11-direct-staging-coalescing-failure.log)
fails as expected: sixteen medium chunks produce sixteen segments instead of
one. **The threshold change was restored and is not a candidate for merge.**

This supports investigating compressed page appends inside the existing active
historical segment. Preserve row/segment/block-span limits and the catalog
checkpoint boundary; keep committed page payloads/index entries unchanged,
append new pages, atomically replace page indexes/bitmaps, and publish the
manifest only after required ordering. Readers must respect their manifest's
row boundary while later pages are appended. Test interruption at append/index/
manifest/catalog phases, repeated rewind/retry, exact page/row results and sparse
multi-batch workloads. Existing readers support explicit per-page row counts;
no page-boundary assumption or reduction in integrity checks may be introduced.
Full format/checksum and snapshot-lifetime review remain required audit work.


## Compressed historical staging candidate

Historical staging now uses the existing compressed encoders directly, appending
pages to the active historical segment. The dense threshold, target row count
and block-span limit are unchanged. Previously committed payload bytes and page
index entries are immutable; indexes, nullable bitmaps and canonical bits are
published through the existing replacement batch before the manifest. The catalog
remains the durable row/progress boundary. Startup validates that complete prefix,
rewinds unpublished tails, and preserves healthy compressed segments. WAL replay
uses the catalog's active segment, replacing the old inference that compression
implies finalization. Background query indexing already excludes this segment.

The [publication regression](baselines/2026-09-11-page-append-publication-before.log)
initially exposed five rows through a reader holding a two-row manifest. Readers
now validate and clip page indexes to their captured row boundary; selected reads
also enforce that boundary. Page payload reads check actual file bounds before
allocation and checked arithmetic prevents offset wrap. Tests preserve prior
noncanonical bits and exact payload/index prefixes, keep sixteen medium batches
in one segment, reject twelve malformed metadata/prefix cases without mutation,
and simulate seven append/publication crash states with repeated reopen and retry.
Main-thread failure matrices now include both small appends and rotation. Raw
layout assumptions in old fixtures were updated without dropping row/recovery
assertions. Full current workspace/platform validation is still required.

The [original comparison](baselines/2026-09-11-page-append-original-comparison.jsonl)
uses fixture v3, five alternating pairs with three iterations for short/large
workloads, and three pairs for sustained history. Finalization and checkpoint
costs are included, and all exact row/progress/reopen oracles pass:

| Workload | Original median | Candidate median | Change |
| --- | ---: | ---: | ---: |
| Grouped live, 128 blocks × 128 rows | 2,849.237 ms | 548.022 ms | -80.77% |
| Short history, one 128-block chunk | 14.237 ms | 12.989 ms | -8.77% |
| Sustained history, sixteen 128-block chunks | 273.934 ms | 123.813 ms | -54.80% |
| Large history, 512 blocks × 1,024 rows | 60.787 ms | 67.158 ms | +10.48% |

The [sparse comparison](baselines/2026-09-11-page-append-sparse-comparison.jsonl)
uses three alternating pairs with three iterations and one row per nonempty block:

| Workload | Original median | Candidate median | Change |
| --- | ---: | ---: | ---: |
| 128 blocks, one chunk | 6.247 ms | 11.097 ms | +77.63% |
| 2,048 blocks, sixteen 128-block chunks | 46.703 ms | 69.125 ms | +48.01% |
| 8,192 blocks, four 2,048-block chunks | 19.388 ms | 26.909 ms | +38.79% |

Every sixteenth block is empty. These are local APFS storage-call measurements,
not network sync throughput. Binary hashes, captured source diff, hardware,
toolchain, cache conditions, samples, p95 and process peak RSS are in the artifacts.
The original historical path omitted fsync; that does not waive the user's 10%
limit. **The candidate still fails acceptance and PR #130 must not merge.**
Profile the fixed small-file publication cost before selecting another change;
large encoding allocations and page/index growth also need measurement. Broader
format checksums, query snapshot lifetime and repair remain unfinished audit work.


### Fixed-cost follow-up

[Instrumented phase timings](baselines/2026-09-11-page-append-phase-profile.jsonl)
show the final device/directory synchronization dominates tiny chunks (roughly
5–6 ms in the diagnostic runs); the 39 individual flush calls total only about
0.1 ms. Nested and parallel phase durations overlap, so they are not additive or
acceptance timings. Merely parallelizing file flushes is not supported by this
profile. Creating the canonical bitmap separately also ordered it once before
the containing publication ordered it again.

The new bitmap is now written with the other new page artifacts and remains
covered by their manifest/catalog publication. Existing committed canonical
prefix replacements retain their ordering. The standalone bitmap helper is now
test-only. [Isolated timing](baselines/2026-09-11-new-bitmap-comparison.jsonl)
versus `e60718fc` is short history -15.12%, few logs -2.34%, tiny chunks +0.87%,
and large history +0.41%. Only the short-history result supports a speedup; the
others are within likely noise. Four segment regressions, crash-phase recovery
and strict storage Clippy pass. This remains provisional until combined checks.

Two further diagnostics were restored, not retained:

- [Omitting the empty initial manifest](baselines/2026-09-11-first-history-manifest-comparison.jsonl)
  gave few logs +13.78%, tiny chunks +0.01%, short history -4.55% and sparse history
  -3.69%. Results do not establish a retained improvement; the original allocation
  and publication sequence is restored.
- [Four compressed-column workers below 4,096 rows](baselines/2026-09-11-small-page-workers-comparison.jsonl)
  gave few logs -6.68%, tiny chunks -3.29%, short history +3.34% and sparse history
  +0.63%. It does not resolve sparse overhead and most results are within noise.
  The established fourteen column workers are restored to isolate further work.

The sparse acceptance gap remains. A compact shared page artifact is a candidate
for the user's authorized fresh-format approach: reduce file/index/bitmap creation
and publication overhead while keeping immutable committed prefixes and bounded
checkpoint rewind. This requires a concrete format and failure tests before any
claim that it solves performance. No protected dataset or deployment is affected.


All six [local workspace gates](baselines/2026-09-11-page-append-gates.jsonl)
pass for page appends plus the new-bitmap publication simplification: 853 tests,
eight ignored, doc tests, strict Clippy, formatting, all-target check and release
linking. The rejected empty-manifest/worker changes are absent. The performance
ceiling and current platform checks remain open; green gates do not permit merge.
A separate equal-bytes artifact-layout probe will test the shared-file hypothesis
before implementing any new container format.


The [equal-bytes artifact probe](baselines/2026-09-11-artifact-layout-probe.jsonl)
compares 32 encoded column/index/null files with one shared artifact, retaining
canonical/manifest files and identical durable publication. Fifteen alternating
pairs per size pass byte-for-byte readback and checksum checks. Creation plus
publication median changes 12.212 → 8.142 ms for 120 rows (-33.33%),
10.941 → 8.495 ms for 15,360 rows (-22.36%), and 21.380 → 20.486 ms for
491,520 rows (-4.18%). Compression is outside this diagnostic timer, so these
are not ingestion speedups. The first two results support a shared-artifact
prototype; the large change is within likely noise. Actual append/recovery/query
and original-baseline acceptance remain required.
