# Journaled commit recovery and publication durability

This batch 2 milestone follows PR #129. It fixes replay of a WAL batch after some
or all of that batch has already reached committed segments. It also establishes
file and directory synchronization before clearing that batch's recovery data.
It preserves the WAL and segment encodings; the user approved a separate journal
instead of a versioned WAL. Other storage formats and compaction replacement
protocols still require review.

## Findings

| ID | Severity | Evidence and disposition |
| --- | --- | --- |
| B2-05 | P1, duplicate query rows after restart | With three initial rows and a 25-row WAL batch, simulate exit after committing 1, 7, 12 or 25 rows across 10-row segments. Baseline reopen returns 29, 35, 40 or 53 rows instead of 28; a second reopen retains the duplicates. Record the transaction's starting segment position before appending the WAL, compare the committed prefix exactly, and append only the remainder. A characterization test constructs identical legacy file bytes for an unapplied intentionally repeated row and an already-applied first row: equality cannot establish the transaction boundary. |
| B2-06 | P1, missing publication durability | Column, catalog and manifest paths flushed userspace buffers or renamed temporary files without synchronizing referenced data and directory entries before discarding WAL contents. Recovery also rewrote committed column prefixes directly. Synchronize data before manifest/catalog publication, synchronize replacement names and WAL truncation, and atomically replace recovered columns while preserving the original prefix. Failure tests cover the coordinator's publication checkpoints and the shared replacement primitive. |
| B2-07 | P1, orphaned rows made canonical | Startup recreated missing/short canonical bitmaps with all bits true. A test retaining a false committed bit but shortening the bitmap succeeded and made that row canonical on the baseline. Missing committed canonical bits now stop startup; an explained uncommitted append tail is rebuilt with the original committed bits. Interrupted recovery must preserve those bits too. |
| B2-08 | P1, conflicting data-directory owners | Multiple storage instances could open the same directory and independently append/replay/truncate its WAL. Acquire a nonblocking exclusive lock on the directory inode before catalog or WAL access, retain it in background compaction plans, and reject competing owners with an actionable path. This implements the exclusivity prerequisite from batch 10; volume supervision remains pending. |

## Commit and recovery protocol

A new `wal/recovery.json` contains version 1 metadata, protected by a CRC32 over
its canonical serialized payload. It records the starting hot descriptor,
`next_segment_id`, batch row count and CRC32 of the unchanged binary WAL row
encoding. Reads are limited to 16 KiB, reject unknown fields and validate version,
kind, position and nonempty count. Checksums detect accidental damage; they are
not chain authentication or protection against malicious filesystem edits.

1. Validate/encode the batch before any transaction writes. Require an empty WAL.
2. Durably publish the journal before appending any WAL bytes or segment rows.
3. Append and synchronize the WAL and its parent directory.
4. Apply rows, synchronizing segment artifacts before each manifest and the
   catalog. Manifests remain the existing source for recovering a catalog whose
   publication was interrupted. Newly allocated segment directory entries are
   synchronized before the commit returns.
5. Synchronize WAL truncation, then remove the journal and synchronize its parent.

Any error after preparation starts leaves the storage instance closed to further
mutations until it is dropped and reopened. This includes canonical-state updates,
historical writes and creating compaction plans. Existing read interfaces remain;
whole-node storage-failure health and bounded shutdown belong to batch 10.

Recovery first validates the complete WAL using PR #129's conservative tail
policy. Journal positions identify already-published rows in the original hot
segment and segments allocated during the transaction. Preexisting historical
segments between those IDs are excluded. Read selections are chunked; applied
rows must match the WAL prefix exactly and have present, true canonical bits.
Partial raw-column tails beyond the manifest are rebuilt before appending the
remaining rows. A zero-row segment can contain any subset of files after its
first interrupted write; it is rebuilt without requiring a complete prior file
set. Missing files for a nonempty committed prefix remain an error. Existing committed canonical flags are copied into every
replacement, including when rebuilding itself is interrupted.

With an empty WAL, a prepared journal can be retired only when there are no
applied rows or the entire applied batch matches its recorded count/fingerprint.
Affected physical artifacts must also agree with committed metadata. Partial
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
same directory, flushed, synchronized, renamed and followed by a directory sync.
Handled errors attempt to remove their temporary file without hiding the primary
failure. Abrupt process exit may leave unreferenced `.name.pid.sequence.tmp`
artifacts. Startup does not infer committed data from those names or delete
unrelated files. A stale conventional `.name.tmp` symlink is not followed.

The pinned Rust library uses `F_FULLFSYNC` for `File::sync_all` on Apple and
`fsync` on Linux. Directory entries need their own synchronization; see the
[Linux fsync contract](https://man7.org/linux/man-pages/man2/fsync.2.html).
The exact pinned implementation was inspected in the installed standard-library
source. These barriers depend on the filesystem/device honoring its flush
contract; deterministic injected errors do not simulate arbitrary torn sectors,
controller failure or physical power removal.

The directory lock uses the pinned standard library, requires no new dependency
or lock file, and survives the originating storage handle while a compaction plan
still exists. Alternate path spellings resolving to the same directory conflict.
Older binaries and arbitrary low-level filesystem writers do not participate.
Offline commands that open storage must stop the other owner first; use the
running server's API for concurrent log queries.

Stop the node cleanly before downgrading: require `pending.wal` empty and
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
it did not demonstrate a useful improvement. The final path retains sequential
segment synchronization and the existing scoped column writers.

All six final local workspace gates pass: 807 tests, four explicitly ignored
benchmarks, strict Clippy, doc tests and release linking. Linux/macOS CI is
required before merge. The
[release comparison](baselines/2026-09-10-commit-replay.md) records write, index,
compaction, warm reopen and query costs using unchanged dense/sparse fixtures.

## Cleanup and remaining work

Removed the equality-based already-applied replay helper, permissive all-true
canonical repair and duplicated temporary-file replacement logic. Full-column
serialization writes checked offsets directly instead of allocating an unused
offset vector; the stale four-byte offset comment is corrected to eight bytes.
The active hot-segment lookup no longer republishes an unchanged manifest.
Four old restart tests now drop the first owner before reopening.

Still pending: atomic compaction directory replacement (including profile
rewrites), historical transaction/coverage coupling, full parser/catalog
validation and allocation limits, query snapshots and file lifetime, derived
index corruption recovery, task supervision, external-volume identity/loss
handling, verified offline repair and the integrated staging soak. The separate
metadata file adds a compatibility condition for downgrade and durable writes
have measurable cost. No production data or service was accessed.
