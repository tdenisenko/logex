# Journaled commit recovery and publication durability

This document retains the investigation chronology and revision-specific results.
Use [the PR #130 acceptance record](pr130-acceptance.md) for the final source,
validation status, compatibility decision and performance tradeoffs.

This batch 2 milestone follows PR #129. It fixes replay of a WAL batch after some
or all of that batch has already reached committed segments. It also establishes
file and directory synchronization before clearing that batch's recovery data.
The generic WAL encoding is preserved. The user approved a separate recovery
journal and subsequently authorized a fresh sync with breaking storage changes.
The current catalog/segment/bundle/index versions are 11/9/5/2; the protocol history
below is explicitly revision-specific. Compaction replacement and generation
coalescing are covered in their linked findings, with final acceptance still open.

PR #130 remains unmerged: the initial ingestion slowdown was rejected. The user
requires no more than 10% degradation and authorized bounded WAL checkpoints.
The [grouped-flush investigation](baselines/2026-09-11-grouped-flush.md) records
the tested intermediate changes; the [checkpoint investigation](baselines/2026-09-11-checkpoints.md)
records that rejected intermediate prototype. The current
[combined-checkpoint design](sync-ingestion-checkpoints.md) combines sync rows/progress,
uses bounded restart re-fetch, and requires a new data directory. The protocol
and old-format downgrade notes below describe the preceding WAL-checkpoint
implementation; they do not authorize opening catalog v2 with an older binary.

## Findings

| ID | Severity | Evidence and disposition |
| --- | --- | --- |
| B2-05 | P1, duplicate query rows after restart | With three initial rows and a 25-row WAL batch, simulate exit after committing 1, 7, 12 or 25 rows across 10-row segments. Baseline reopen returns 29, 35, 40 or 53 rows instead of 28; a second reopen retains the duplicates. Record the transaction's starting segment position before appending the WAL, compare the committed prefix exactly, and append only the remainder. A characterization test constructs identical legacy file bytes for an unapplied intentionally repeated row and an already-applied first row: equality cannot establish the transaction boundary. |
| B2-06 | P1, missing publication durability | Column, catalog and manifest paths flushed userspace buffers or renamed temporary files without synchronizing referenced data and directory entries before discarding WAL contents. Recovery also rewrote committed column prefixes directly. Synchronize data before manifest/catalog publication, synchronize replacement names and WAL truncation, and atomically replace recovered columns while preserving the original prefix. Failure tests cover the coordinator's publication checkpoints and the shared replacement primitive. |
| B2-07 | P1, orphaned rows made canonical | Startup recreated missing/short canonical bitmaps with all bits true. A test retaining a false committed bit but shortening the bitmap succeeded and made that row canonical on the baseline. Missing committed canonical bits now stop startup; an explained uncommitted append tail is rebuilt with the original committed bits. Interrupted recovery must preserve those bits too. |
| B2-08 | P1, conflicting data-directory owners | Multiple storage instances could open the same directory and independently append/replay/truncate its WAL. Acquire a nonblocking exclusive lock on the directory inode before catalog or WAL access, retain it in background compaction plans, and reject competing owners with an actionable path. This implements the exclusivity prerequisite from batch 10; volume supervision remains pending. |
| B2-09 | P2, valid compacted storage refused on reopen | A compacted manifest can coexist with a subset of obsolete raw files after interrupted cleanup. Recovery mistook its retained canonical bitmap for raw storage, and integrity checking required missing raw columns whenever address.col survived. Regressions cover an empty WAL with a complete journal and repeated ordinary reopen. Validate the representation referenced by the manifest; raw manifests still require every raw column. |
| B2-10 | P1, sync repeats rows after a progress-publication interruption | **Regression passes in the unmerged [combined sync checkpoint prototype](sync-ingestion-checkpoints.md); performance/platform acceptance remains pending.** On the prior revisions, restart after the row write but before the engine's head/floor update; resuming from the old marker gives six rows instead of three, on both `09a63f55` and `ff728ea3`, in both ingestion routes. [Caller-level findings and measurements](ingestion-publication.md) require a joint data/progress recovery boundary. Exact WAL replay alone cannot repair this gap, and generic equality-based deduplication would be incorrect. |
| B2-11 | P2, segment-ID exhaustion panics or wraps | A catalog with `next_segment_id = u64::MAX` reaches unchecked allocation. The debug regression panics on overflow; release arithmetic can wrap the allocation boundary. Allocation and registration now return a checked error without changing catalog state. The focused regression failed before the fix and passes after it. No data-loss simulation is claimed for this boundary. |
| B2-12 | P2, raw variable-column lengths panic before validation | A 44-byte column declaring `u64::MAX` rows panics while computing its offset table, before rejecting the file. A regression failed before the fix. Raw payload layout now checks version/compression, count arithmetic, table size, monotonic offsets, zero origin and exact final sentinel before allocating row results, including selected/empty selections. Compaction borrows one page from the validated file buffer; query payloads retain independent ownership. The malformed-layout and selection-order regressions pass. |

| B2-13 | P2, fixed raw-reader allocation and nullable bounds | Tiny headers reproduce address/hash/topic capacity panics, and an out-of-range nullable read returned `None`. A shared checked view validates full layouts or requested append prefixes before materialization. See the detailed finding and before-fix evidence below. The unmerged fix preserves selected-prefix append behavior and enables borrowed compaction; no measured speedup is claimed yet. |

## Commit and recovery protocol

`wal/recovery.json` has a checksum over its canonical serialized payload and a
16 KiB read limit. Version 1 single-batch journals remain readable. Version 2
adds a checkpoint state (live/historical route and active/complete phase) while
preserving the WAL frames and all segment encodings. Both versions record the
starting descriptor and the segment-allocation boundary. An active checkpoint
uses WAL frame checksums; a complete checkpoint also records the cumulative row
count and checksum of the equivalent concatenated binary row payload.

1. Validate/encode the entire caller batch before changing files. Finish the
   previous checkpoint on a route change, a 32 MiB accumulated-WAL boundary,
   count overflow, or age of at least five seconds.
2. For a new epoch, order its active journal before any WAL/column mutation.
   The WAL's full sync supplies persistence before segment writes proceed.
3. Append and synchronize every caller batch's WAL frame and its parent. Both
   live and historical successful writes have durable recovery data.
4. Apply rows and order column files before each manifest. Defer ingestion-only
   catalog rewrites; manifests reconstruct those descriptors on recovery.
   External-device dependencies receive a full sync before their references
   can publish on another device. Public anchor/state updates remain durable.
5. At checkpoint, record the complete count/checksum, order catalog publication
   before WAL truncation, then order truncation before removing the journal.
   Journal removal completes the full device flush before checkpoint success.

The 32 MiB threshold bounds accumulation across calls. A single already-valid
larger caller batch is allowed by the existing API, but is checkpointed before
returning success and cannot accumulate other batches beside it. Route switches,
reorg mutation, index/manifest publication and synchronous compaction also finish
a pending checkpoint. Detached compaction plans exclude the current epoch's
segments. The background indexer checks age on its ten-second ticker; five
seconds is eligibility, not a hard completion deadline. It performs the I/O on a
blocking worker. Closing storage with pending WAL remains a supported recovery
path; no destructor performs fallible I/O.

`checkpoint()` and `checkpoint_if_due()` are available on the storage facade.
`mark_non_canonical` now requires a mutable receiver so it can retire the prior
checkpoint before changing canonical bits. Runtime callers already hold mutable
storage guards; this is a Rust source API change, not a wire or file-format change.

Any error after preparation starts leaves the storage instance closed to further
mutations until it is dropped and reopened. This includes canonical-state updates,
historical writes and creating compaction plans. Existing read interfaces remain;
whole-node storage-failure health and bounded shutdown belong to batch 10.

Recovery first validates the complete WAL using PR #129's conservative tail
policy. Journal positions identify already-published rows in the original starting
segment and segments allocated during the transaction. Preexisting historical
segments between those IDs are excluded. Read selections are chunked; applied
rows must match the WAL prefix exactly and have present, true canonical bits.
Partial raw-column tails beyond the manifest are rebuilt before appending the
remaining rows. Historical replay recovers the newest affected raw staging segment; a compacted
segment is not a raw append target. A zero-row segment can contain any subset of files after its
first interrupted write; it is rebuilt without requiring a complete prior file
set. Missing files for a nonempty committed prefix remain an error. Existing committed canonical flags are copied into every
replacement, including when rebuilding itself is interrupted.

With an empty WAL, a prepared journal can be retired only when there are no
applied rows or the entire applied batch matches its recorded count/fingerprint.
A completed version 2 checkpoint with no applied rows is an error, even when
the WAL is empty: it cannot be confused with preparation before the first append.
A regression reproduced silent journal removal in that damaged state and now
requires preserving the evidence on repeated reopen. Affected physical artifacts
must also agree with committed metadata. Partial
commits, unexplained physical rows, malformed metadata and mismatched payloads
stop with an error and retain the journal/WAL. A missing or damaged committed
canonical bitmap cannot be reconstructed by guessing all rows were canonical.

Legacy WALs without a journal remain readable. Nonoverlapping pending rows are
adopted into a journal before recovery starts. Overlap with a committed
`(block_hash, log_index, source)` identity is ambiguous, even when bytes match:
startup stops and requests explicit verified recovery. Intentionally repeated
batches written by this version remain supported and are not deduplicated.

## Filesystem and compatibility boundaries

Replacement files are created exclusively under unique temporary names in the
same directory. Contents are ordered before rename; column replacements are
ordered as a group before manifest publication. Successful batches remain
recoverable from the durable WAL until checkpoint supplies final persistence. Standalone metadata
replacements also synchronize their containing directory before returning.
Handled errors attempt to remove their temporary file without hiding the primary
failure. Abrupt process exit may leave unreferenced `.name.pid.sequence.tmp`
artifacts. Startup does not infer committed data from those names or delete
unrelated files. A stale conventional `.name.tmp` symlink is not followed.

The pinned Rust library uses `F_FULLFSYNC` for `File::sync_all` on Apple and
`fsync` on Linux. The implementation groups explicit Apple `fsync`
calls and ordering barriers before a full sync per touched device. Traversal follows directory symlinks, checks directory identities for cycles,
and bounds nesting to 64 levels before any manifest can publish. This prevents
synchronizing only a symlinked columns directory while leaving its files dirty.
Unsupported
ordering barriers fall back to full sync; other I/O errors propagate. The two
FFI calls, ownership assumptions and platform tests are documented in the
[grouped-flush investigation](baselines/2026-09-11-grouped-flush.md). WAL aliases also require synchronizing the actual newly created target
directory and persisting a journal on another device before WAL bytes.
Directory entries need their own synchronization; see the
[Linux fsync contract](https://man7.org/linux/man-pages/man2/fsync.2.html).
The exact pinned implementation was inspected in the installed standard-library
source. These barriers depend on the filesystem/device honoring its flush
contract; deterministic injected errors do not simulate arbitrary torn sectors,
controller failure or physical power removal.

The directory lock uses the pinned standard library, requires no new dependency
or lock file, and survives the originating storage handle while a compaction plan
still exists. Alternate path spellings resolving to the same directory conflict.
The last managed owner explicitly unlocks before closing its descriptor. A
duplicated descriptor can otherwise retain the same lock after the storage
owner drops; this is also relevant to transient process creation in other
threads. A deterministic duplicate-descriptor regression failed before the fix.
Compaction plans continue to retain the shared owner until they are dropped.
The original intermittent test timing is consistent with descriptor inheritance,
but that timing itself was not captured. Apple documents shared dup/fork lock
references in [flock(2)](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/flock.2).
Older binaries and arbitrary low-level filesystem writers do not participate.
Offline commands that open storage must stop the other owner first; use the
running server's API for concurrent log queries.

For the preceding **catalog v1** prototype only, stop cleanly before downgrading: require `pending.wal` empty and
`recovery.json` absent. An older binary ignores the new journal and can duplicate
or discard a pending transaction. Preserve the whole closed data directory and
use this version to finish recovery before downgrading. On an ambiguity/corruption
diagnostic, retain all files and recover through trusted data or a known-good
backup; deleting the journal or truncating the WAL to bypass the error is unsafe.
No new repair command or production migration is delivered here.

## Validation and measurement

Tests cover committed-prefix replay across multiple rotations and repeated
reopen, changed target sizes, intentionally identical batches, legacy ambiguity,
preexisting historical segments, bad journal checksums/versions/positions/size,
WAL count/payload mismatch, an incorrect committed prefix, missing WAL with
uncommitted artifacts, canonical-bit damage, interrupted prefix rebuild and missing first-batch columns. A subprocess test checks
interprocess exclusion, then exits after a partial rotating commit without
running Rust destructors; the parent reopens twice and checks the exact rows.

The transaction failure matrix injects an error before every recorded main-thread
I/O checkpoint in a rotating write, rejects subsequent mutations, and verifies
exact rows on two reopens. Column writers run on scoped worker threads; their
checkpoints are not included in that thread-local matrix. Separate replacement
primitive tests cover write errors, pre-sync, pre-rename and post-rename directory
sync failure, proving each replacement leaves a complete old or new file.
This is not a claim to exercise every OS write or power-loss interleaving.

A bounded eight-worker flush experiment was measured after profiling and rejected:
it did not demonstrate a useful improvement. Subsequent grouping reduces device
flushes. Thirty process runs compare later column-worker changes: parallel raw
compaction improves its median by 25–30%, and four groups of raw append workers
improve live ingestion about 9–10% within the checkpoint prototype. The current
[measurements](baselines/2026-09-11-checkpoints.md) still exceed the user’s overall
10% ingestion-regression ceiling; neither timing nor correctness gates authorize
merging the unfinished performance work.

All six pre-checkpoint local workspace gates passed at `e811325f`: 809 tests,
four explicitly ignored benchmarks, strict Clippy, doc tests and release linking.
The checkpoint implementation and publication fixture pass all six local gates:
824 tests, including 117 storage unit tests, with six ignored cases. The optional
cross-filesystem test passed explicitly on isolated APFS/ExFAT mounts.
[Current gate results](baselines/2026-09-11-publication-gates.jsonl).
All six Linux/macOS CI jobs also passed at `ff728ea3` in run `34520575847`.

The [isolated ExFAT validation](baselines/2026-09-11-exfat.md) passed all 117
storage tests on `mac-mini`; only its explicit WAL benchmark was ignored.
Creating a sparse image failed, but a blank writable image succeeded without
changing system protections or external-volume contents. Five alternating image
benchmark pairs show +38–39% live and +49–55% historical storage-ingestion time;
all exact oracles pass, but the performance requirement remains unmet. The
[release comparison](baselines/2026-09-10-commit-replay.md) records write, index,
compaction, warm reopen and query costs using unchanged dense/sparse fixtures.

## Cleanup and remaining work

Removed the equality-based already-applied replay helper, permissive all-true
canonical repair and duplicated temporary-file replacement logic. Full-column
serialization writes checked offsets directly instead of allocating an intermediate
offset vector; the stale four-byte offset comment is corrected to eight bytes.
Active hot and historical segment lookups no longer republish unchanged manifests.
Four old restart tests now drop the first owner before reopening.

Still pending: atomic compaction directory replacement (including profile
rewrites), historical transaction/coverage coupling, full parser/catalog
validation and allocation limits, query snapshots and file lifetime, derived
index corruption recovery, task supervision, external-volume identity/loss
handling, verified offline repair and the integrated staging soak. The separate
metadata file adds a compatibility condition for downgrade and durable writes
have measurable cost. Only read-only metadata of the existing external volume
was inspected; production data contents and services were not changed.


### B2-13 — fixed raw readers trusted unbounded counts and nullable row bounds

**Confirmed, unmerged fix.** A 17-byte raw header with `u64::MAX` rows causes
capacity-overflow panics in address, hash and nullable-topic materialization.
A nullable request outside the declared rows can return `None` instead of an
error. Fixed readers also accepted unsupported versions/compression, and whole
reads could ignore trailing bytes or missing bytes under null slots.
The [count/layout reproducer](baselines/2026-09-11-fixed-layout-before.log) and
[nullable bound reproducer](baselines/2026-09-11-null-bound-before.log) fail before
the fix. This is a raw-reader/storage corruption finding, not a demonstrated
network validation bypass; native startup separately checks many file lengths.

A shared fixed-width byte view checks format and length arithmetic before
allocating row results. Whole reads and compaction require the exact declared
body and matching bitmap. Nonempty selected reads retain the established
append-prefix behavior: requested rows must fit the declared and physical
column and bitmap, while an unrelated growing tail need not yet be complete.
This matters because fixed columns append in place. Empty selections still
validate the whole layout. Repeated selection order, all six reader variants,
five unfinished-append states and null bounds are covered explicitly.

Raw address/hash/topic compaction borrows these validated bytes instead of
materializing typed values and then serializing them back into another buffer.
Absent slots are zeroed in memory to match the existing typed encoder. A
multi-page byte oracle compares both encoders' data, page indexes, descriptors
and null bitmaps. Query results still own their values; the complete raw file
is read into memory. This does not resolve cross-column snapshot/file-lifetime
races; those remain part of the query audit.


## Already-empty WAL startup

Phase profiling at a0ed88c1 found generic populated reopen spending roughly
4–9 ms truncating and fully synchronizing an already-empty WAL, in addition to
4–6 ms of required catalog hardening. With no journal and a physically empty WAL,
startup now skips that mutation. A partial frame can decode to zero rows while
still occupying bytes; that case continues to durably truncate before appending.
The catalog is still hardened, recovery evidence is still validated, and active
or completed journals keep their previous paths.

The new regression fails before the fix at its unwanted-mutation assertion and
passes afterward. It covers repeated empty reopen, a recoverable partial header,
exact original rows and subsequent append/checkpoint/reopen. All 207 storage
unit tests pass; five platform/benchmark tests are intentionally ignored here.
[Failure, exact patch and validation](baselines/2026-09-11-empty-wal-validation.json).
[Diagnostic phase evidence](baselines/2026-09-11-storage-phase-attribution.json).
This removes redundant work without claiming that required startup integrity or
full synchronization can be omitted.


An isolated Apple synchronization experiment removed the initial ordinary fsync
of the exact file retained for the final full sync, keeping directory fsyncs and
every device's final synchronization. Apple's [F_FULLFSYNC contract](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2)
supports the file-plus-device flush, but the measured change was inconsistent:
dense generic live/history medians -3.62%/-2.03%, sparse +0.41%/+2.91%, with larger
tail variation. Its 206 release storage tests passed. The experiment is rejected;
production retains the preceding synchronization implementation and guarantees.
[Source, tests and paired measurements](baselines/2026-09-11-single-file-sync-rejected.jsonl).
