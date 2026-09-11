# Index freshness during ingestion

Unmerged follow-up within storage PR #130. The live-bundle query regression found
that an index built at 8,192 rows was still used after that segment grew to 10,000
rows and rotated. Native filtering returned 10,082 matches across the dataset
instead of 11,322. The missing rows were present in storage.
[Failing regression](baselines/2026-09-11-live-bundle-query-before.log).

## Publication contract

Managed `IndexBuilder` operations now publish an `indexes/index-checkpoint` file
after their output is complete. The bounded, checksummed record identifies the
source row count and, for bundles, the exact immutable table/canonical reference.
Raw prefixes rely on their append-only row boundary; storage rollback deletes
their derived indexes before replacing rows. A future repair that replaces rows
must rebuild indexes as part of its offline publication protocol.

The index directory inode supplies the lock, without a separate lock file.
Builders take exclusive access and durably withdraw the former checkpoint before
overwriting any index. They clear known obsolete index files when the source
changed or a previous build was incomplete, preserving custom artifacts. After
building, they verify that the source is unchanged and publish the checkpoint
with the storage layer's existing ordered tree/full-flush primitive. A changed
source or busy reader returns `WouldBlock` for retry instead of reporting a
published current index.

Queries take shared access only while consulting a matching published index set.
Missing or stale checkpoints and an active builder trigger column filtering.
Malformed checkpoint metadata returns an explicit error and can be rebuilt;
reads are capped at 1,025 bytes before decoding. The query fallback still applies
the predicates needed by native SQL aggregates; it does not count all rows merely
because indexes are unavailable. The SQL data-sum path shares its segment reader
across bloom checks, row selection and materialization.

No ingestion call writes an index checkpoint. Index files keep their existing
encodings; unmarked legacy indexes are treated as needing a rebuild. Background
and CLI missing-index checks include freshness. Background work also retries
after canonical changes with an unchanged row count or maximum segment id.

## Validation

- Both raw and bundled live query fixtures pass native filters, SQL counts and
  ordered SQL before/after index construction, subsequent append, rotation,
  canonical removal and two restarts.
- Storage tests cover reader/builder exclusion, stale rows, a canonical reference
  change with an unchanged row count, source advancement during a build, bounded
  malformed/truncated metadata, and every injected main-thread rebuild phase.
  No incomplete mock index is exposed with a current checkpoint.
- All three build profiles now fill their declared required files, including
  missing composite indexes and the LogQuery event bloom. The profile regression
  exercises stale data, incomplete files, cross-profile rebuilding and actual
  B-tree contents. All 27 index tests pass.

All six combined local workspace gates pass: 895 tests passed, nine ignored.
Final wrapper/background cleanup also passes formatting, workspace check, strict
Clippy, the query harness and background tests. [Validation record](baselines/2026-09-11-index-checkpoint-validation.jsonl).
All six Linux/macOS CI jobs pass at `196664e4`. Both Apple architectures pass
184 storage tests/four ignored, 128 cross-mount recovery cases, and five query
tests/one ignored on disposable ExFAT images at integrated `6ce06c13`.
[Platform record](baselines/2026-09-11-live-bundle-platform-validation.jsonl).

The first integrated APFS lifecycle comparison uses five alternating process
pairs × three fresh datasets per dense/sparse profile (200,000 rows). Native,
count and ordered query medians increase 1.0–3.7%; concurrent native queries are
+2.1% dense / +6.6% sparse. Index construction is +26.2% dense / +13.1% sparse.
Warm reopen is 12.7–13.1 ms versus 1.2–1.3 ms. Generic row-only live writes are
+19.2–19.9%; generic historical writes are +30.4–36.5%. Those generic APIs are no
longer production sync callers; combined sync has separate measurements. The
results remain visible and are not waived as successful performance acceptance.
[Raw comparison](baselines/2026-09-11-live-bundle-query-performance.jsonl).
Index flushing, startup validation and concurrent cost need further attribution. This is not completion of batch 6: malformed
index payloads, standalone low-level writer APIs, complete query snapshot/file
lifetimes, derived artifact enumeration and broader randomized index equivalence
remain audit work. The PR remains draft and unmerged.
