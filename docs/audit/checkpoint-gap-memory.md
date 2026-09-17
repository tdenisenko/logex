# Checkpoint-gap header memory

This batch-5 follow-up is on `audit/checkpoint-gap-memory`, based on PR #221
merge `e54e9303`. Source `b327cf90` is committed and independently reviewed.
All ten local gates pass; publication and CI remain before merge.
It addresses the B5-11 retention finding recorded in the
[sync-state review](sync-state-review.md).

## Finding and required behavior

The original stale-restart path downloads every header from the persisted tip to
the next consensus anchor into a vector. After validating the terminal header it
allocates a second, gap-sized hash vector; both remain alive during body/receipt
ingestion. Bounded network pages and a bounded payload pipeline therefore do not
bound this retained header working set. This is established by the original code,
not by an RSS measurement or an observed out-of-memory failure.

Every downloaded header must still pass standalone and parent validation, and the
terminal number, hash and receipt root must match the trusted consensus anchor
before any payload ingestion is possible. Partial committed progress, empty-block
progress, request attribution and ordinary retry behavior must remain consistent.
An arbitrary maximum-gap rejection or publication before terminal authentication
would not preserve those requirements.

## Implementation approach

Use an exclusively created temporary file in the existing storage directory.
Clone the open handle and remove its directory entry before writing headers, so
the file's lifetime belongs to the remaining handle. A process failure cannot
leave a large filled scratch artifact. A failure between creation and unlink can
at most leave an empty file. No directory is created and no system-temp fallback
or path canonicalization is used. Supervised deployments already anchor their
working directory to an opened data directory and pass `.` to storage; preserving
that namespace also preserves the mount-loss protection.

The writer checks each downloaded header page before encoding it. It keeps only
the last header, record count, maximum validated encoding length, fixed I/O buffer
and a fresh authentication key. The key remains in memory. Each record binds its
position, byte length and exact RLP bytes through keyed BLAKE3. The package is
already locked in the repository; sync adds a direct dependency on the same
version requirement without updating resolved versions.

Only sealing against the exact consensus anchor exposes a reader. Each replayed
record is authenticated before decoding or use. Lengths are checked against the
maximum collected from the original validated headers before allocation. A
plain stored checksum would not bind replay to the original validated bytes;
checking a whole-file digest only after ingestion would also be too late. The
per-record authentication avoids retaining an offset/digest table or performing
another full-file pass. It assumes the existing trusted process-memory boundary.

Header downloads and replay batches have an explicit 1,024-header ceiling.
The existing 128-block parallel chunks and maximum eight outstanding/completed
payload chunks remain. Smaller configured sequential batches are respected;
larger settings are split into bounded reads. A declined parallel request restores
its bounded cursor before sequential fallback, so no already-read rows are lost.
Hashes for body requests come directly from the authenticated canonical RLP bytes
on the blocking worker, avoiding another encode/hash loop on the async worker.

File operations use owned blocking tasks. Queued work is aborted when its future
is dropped; an already-started operation retains its temporary-file ownership
until it ends. Successful append operations flush their buffer on the blocking
worker, preventing buffered I/O during later async-side drop. This is an ordinary
buffer flush, not an fsync or a durable database publication. Normal retry and
restart download the remaining gap again; the file is not recovery authority.

## Review and validation boundaries

The initial candidate's terminal mismatch incorrectly became a fatal error;
review required preserving the original retry result while propagating actual
local storage failures. Review also required abort-on-drop blocking ownership,
buffer flushing off the async executor, cancelable fallback rewind, fallible
reservations and an explicit replay ceiling. These are development corrections,
not bugs shipped in the preceding merged code.

Eleven new controls pass within the 166-test engine suite. Eight spool controls
cover bounded round trips, canonical hash equivalence, frame changes before and
after seal, length bounds, truncation/extra data, positional binding, terminal
trust, cursor fallback, I/O failures and cancellation. Cross-file binding relies
on a fresh key and standard keyed-MAC design review; no direct cross-file copy
control is claimed. A queued real append is canceled before it can write; a
separate owner-lifetime control explicitly waits for a running worker to drop its
scratch. It does not block a real filesystem syscall.

Three engine controls compare 131 nonempty blocks under 128-header chunks and
rewind followed by 13-header groups; rows, recent headers and chain anchors match.
The indexed anchor appears only on the terminal chunk. An actual declined parallel
plan with no serving peers preserves the starting tip and empty log set; invalid
tail receipts preserve 128 earlier valid rows and withhold the terminal anchor.
Successful payload equivalence calls the shared production ingestion routine;
it does not emulate a successful external parallel exchange.

Focused all-target Clippy, workspace formatting and diff checks pass. An initial
local type-inference error was corrected. Two intermediate engine controls failed
at sandboxed localhost-listener setup; the authorized rerun passed. These are
recorded separately from behavior checks. All ten final-source local gates pass:
1,951 workspace tests, zero failures, 24 existing ignores across 35 targets,
documentation tests and the release build. Tests for the new interfaces are
conformance tests; no original failing run is claimed for them.

The original terminal cursor also uses saturating increment at `u64::MAX`.
Remaining-count traversal completes before incrementing that endpoint. The old
loop re-enters at the terminal value, but later network errors or validation may
exit it; an infinite run is not established. The spool endpoint control passes,
and the outer loop is source-reviewed. No real-chain occurrence is claimed.

The resource claim concerns application-owned header records and metadata, not
total RSS, operating-system page cache, existing payload memory or measured
throughput. Scratch disk use grows with the actual gap; I/O errors remain explicit.
No broad benchmark, remote-host operation, physical-volume test, production-data
change, live sync or deployment is part of this task. Other batch-5 integration
items and automatic offline repair remain open.

## Evidence and publication

The [machine-readable record](baselines/2026-09-17-checkpoint-gap-memory.json)
links hashed archives of the preserved original source, exact focused commands and
intermediate outcomes, independent review, source inventories and complete gate
logs. All recorded source and log hashes were independently verified. Existing
vendored dependency and debug-linker warnings remain recorded; all final gates
pass. Publication and CI results will be recorded before closure.
