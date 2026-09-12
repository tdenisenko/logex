# Query snapshot consistency

Base: `6ffc1201` (merged PR #133). This pass covers rows and canonicality across
query capture, ingestion, reorg, compaction and storage close/reopen. It does not
establish general SQL correctness or complete query-memory accounting.

## Confirmed finding

**B7-03 — P1, query views change after capture.** `StorageSnapshot` retained only
partition metadata, while candidate selection reopened each segment and used
its current row count and canonical bitmap. Capturing two rows and then appending
one returned three rows/count instead of two. Marking the captured block
noncanonical changed an existing view to zero rows/count. Native SQL selection,
native COUNT and DataFusion reproduced both failures on raw and bundled storage.
The four [before-fix failures](baselines/2026-09-12-query-snapshots-before.log)
include a passing completed-compaction control.

## Contract and implementation

A query captures segment IDs, ranges and visible row boundaries under the
caller's existing storage read guard. Appending rows, adding segments, and
representation-only compaction preserve the existing logical row prefix. Every
native/DataFusion selection and aggregate path now uses the captured boundary.
A current index may contain later rows; exclude those while still rejecting
missing captured rows and invalid physical row IDs. Existing checksum, canonical
bitmap and index/source identity checks remain.

A small shared validity token is invalidated before the first canonical write
in a reorg. Old views stay invalid permanently. A new view becomes valid only
after all canonical writes/publication succeed; failed mutation requires storage
recovery before a valid new view can be obtained. A no-op reorg does not invalidate
views. Closing storage invalidates its views before releasing files and directory
exclusivity, preventing reuse across reopen.

SQL checks validity before and after the entire execution, including error and
native aggregate paths. Existing cancellation checkpoints also stop work when
the view changes. Any result spanning a reorg is rejected as `SnapshotChanged`;
REST returns HTTP 409 and gRPC maps this to Aborted. Clients can retry against a
fresh view. There are no automatic retry loops or partial successful results.
Results validated before a later reorg remain legitimate results for their
captured state; this is not a promise that canonicality cannot subsequently change.

No query-wide ingestion lock, per-batch disk write, file format change, or table
of open readers is introduced. A snapshot holds metadata plus one shared atomic
token. Readers continue opening only the columns/segments being accessed.
Compaction/repacking retain logical row order and preserve current reader file
handles; a filesystem error while opening a changing representation remains an
explicit query error, never a successful omission. This does not guarantee that
every racing filesystem open succeeds without retry.

## Validation

Nine integration tests cover raw/bundled append and reorg, all native aggregate
paths and DataFusion, fresh indexes containing later rows, historical insertion,
no-new-segment visibility, completed compaction, sparse live/historical bundle
repacking and close/reopen. The repack test verifies an actual generation change.
A deterministic reorg sweep discovers and exercises every cancellation checkpoint
in six query shapes, including the final DataFusion checkpoint after collection.

Two query unit regressions check invalid index IDs/missing captured rows, and
consume three lazy DataFusion batches with append/compaction between the first
and later batches. A storage regression verifies no-op/append validity, failure
before the first canonical write, successful retry and close invalidation for
both raw and bundled storage. These tests use isolated temporary directories.

All six workspace gates pass, including 955 tests (ten intentional ignored
tests), documentation tests and the release node build. The release performance
comparison remains before this task is merged. The remaining audit, actual live sync and staging soak stay
separate completion gates.

## Further findings

The initial DataFusion reproducer separately exposed Decimal128 arithmetic being
serialized as a datatype label instead of its value. The snapshot fixture now
forces DataFusion through ORDER BY rather than arithmetic projection, isolating
the failures above. Decimal and other result-type handling remain explicitly
tracked for the next SQL pass.
