# Native API worker and snapshot ownership

Review base: PR #217 merge `40d247e6`, on `audit/query-resource-ownership`.
This milestone covers native JSON-RPC and gRPC log scans. Shared query memory,
request admission and retained response budgets remain a separate policy and
implementation item. Source `f2293fd4` passes all ten local gates (1,902 workspace tests / 24
existing ignores), including documentation tests and release linking.
Exact-head CI and merge remain pending.

## B8-05: native API scans block runtime workers and ingestion (P1)

`eth_getLogs`, gRPC `GetLogs` and gRPC `StreamLogs` acquire the live storage read
guard, call the synchronous native scanner, and convert the result while still
inside the async handler. Filesystem work therefore occupies a runtime worker
and retains the lock needed by ingestion/reorg writes. The scanner already
captures a bounded optimistic snapshot internally, but its API borrows the live
manager for the entire call. SQL snapshot execution had already removed this
external lock lifetime in PR #136; that correction did not cover native APIs.

Six bounded public-handler assertions run against the exact original production
files. A current-thread runtime's sole blocking worker is occupied by a channel
gate. The original native handlers still complete during their first poll; all
six assertions fail at the expected missing-yield check. This proves the
synchronous handler path. Retained source establishes the read-guard lifetime;
the test does not claim to observe a lock midway through that synchronous poll.
No timing threshold, large input, altered filesystem read or network endpoint is
needed. Two stored rows per fixture suffice.

## Correction and ownership

The native query crate exposes execution against a captured storage snapshot.
Existing live-manager wrappers delegate to it. Captured row boundaries exclude
later appends; compaction preserves the view, while reorg/close invalidates it.
Validation runs before execution and after success or failure, including an
empty requested page. Invalid views report a retryable snapshot error before a
simultaneous cancellation error.

One server helper acquires execution capacity, captures the snapshot and head
under the read lock, then releases the lock before dispatching a blocking worker.
Scanning and native row-to-protocol conversion run on that worker. The helper
retains the snapshot and validates it again after conversion, including error
exits, before interpreting cancellation. A reorg during conversion cannot yield
a successful stale response. gRPC maps snapshot invalidation to Aborted and
request cancellation to Cancelled; storage failure remains Unavailable.

A private execution semaphore is initialized lazily from the serving runtime's
async-worker count. This keeps native scan dispatch from expanding to the much
larger blocking pool's capacity. It also handles application state constructed
outside the runtime. The owned permit moves into the worker closure and remains
there until completion or disposal. Dropping a request cancels its permanent
token and aborts a queued worker; already-started filesystem work keeps its
permit until it exits. Operating-system I/O is still cooperatively cancellable
between operations, not forcibly interruptible.

Storage failure wakes both execution-capacity and storage-lock waiters. Worker
join failures become explicit execution errors. A final cancellation check
rejects conversion completed after cancellation. Buffered gRPC streams retain
their existing request guard and per-entry storage-failure check. Shared row
conversion is reused for unary and streaming gRPC; the redundant intermediate
vector of `Result<LogEntry, Status>` is removed.

This gate bounds submitted native jobs, not queued request count, complete
response bytes, DataFusion state or process memory. Those remain in the broader
resource-budget work. No new public result truncation is introduced.

## Reviewed alternatives and retained behavior

The native SQL loops calculate a CPU-related count capped at eight, then process
a window four times that size with one scoped thread per partition. Inspection
establishes a bounded fan-out of up to 32, not a documented eight-worker contract.
The initial variable-name inference was withdrawn. Neither those loops nor
their parallelism were changed, and no performance benefit is claimed from
reducing concurrency. A shared query policy must consider their actual work.

No ingestion write, storage format, dependency version or public filter/page
contract changes. Snapshot capture replaces the scanner's existing capture;
it does not add a second metadata clone. Protocol conversion and native scan
algorithms retain their existing work. The new scheduling gate adds one permit
per operation and a blocking-task dispatch. No benchmark campaign or quantified
throughput claim is part of this correction.

## Validation scope

The four new query API controls are conformance tests because the original
snapshot entry point did not exist. They cover raw/bundled append exclusion,
ordering/pagination, repeated concurrent calls, compaction, cancellation/reorg
at actual check boundaries, invalidation on error and closed/zero-limit views.
The query-focused run passes 115 tests with two existing ignores, plus scoped
strict Clippy and formatting. The subsequent visibility-only change exposes the
same snapshot validity check for the server's post-conversion validation.

Public native API controls verify yielding, writer access while queued, captured
rows across append, reorg rejection and successful fresh queries for all three
protocol methods. Server ownership controls cover abandoned started/queued work,
retained capacity, cancellation after conversion, reorg priority and ordinary
operation errors. All 111 server/protocol controls and strict Clippy pass on final source.
The ten final-source workspace gates pass; platform CI and merge follow. All data fixtures use temporary directories.


## Retained evidence

[Validation record](baselines/2026-09-17-native-query-workers.json),
[raw gate logs](baselines/2026-09-17-native-query-workers-validation.json.gz) and
[investigation bundle](baselines/2026-09-17-native-query-workers-evidence.json.gz)
retain commands, outcomes and hashes. The investigation contains 56 files,
including exact original assertions/sources and final reviewed identities.
The first focused server command named a nonexistent test target; it was
corrected before tests ran. A subsequent strict Clippy finding on the new Tonic
helper was resolved using the existing boxed-error convention; final tests and
Clippy pass without a lint suppression.
