# Page row-count integrity

Base: `9a3318b7` (merged PR #130). Scope: the shared compacted-page selection
boundary used by projected column reads and full-row materialization.

## Confirmed finding

**B2-15 (P2): independently valid variable pages and indexes can disagree about
row count.** The variable decoder trusted its embedded count. Selected reads
indexed the decoded vector using the page index without checking its length;
full reads concatenated all decoded values without requiring the indexed count.
A damaged unbundled compacted segment could therefore panic a query worker or
return a column with the wrong number of values. Full-row assembly has separate
column-length checks, but standalone projected column reads did not.

The new tests compact a real 20-row segment, then replace its data page with a
valid encoded 19- or 21-row payload and a valid 20-row index. Before the fix:

- `selected_variable_page_read_rejects_wrong_row_count` panicked with index 19
  into a 19-element vector.
- `full_variable_page_read_rejects_wrong_row_count` returned 19 values successfully.

A bundle checksum normally detects accidental payload changes earlier. That
check does not establish consistency between independently valid page metadata;
the decoded/index invariant belongs at the common reader boundary.

## Resolution and invariants

Every page must decode to exactly its indexed row count before selection or
concatenation. A mismatch returns `InvalidData`; partial results are not returned.
The regression covers full reads, descending/duplicate selection, selection of
an otherwise valid prefix, both mismatch directions, and an unchanged valid page.
It uses a reader projected to `data`, so unrelated column checks cannot mask it.

No writer, encoding, durability or ingestion algorithm changes. The check is one
length comparison per decoded page. The shared location also protects future
column decoders. Existing arbitrary-order/page-crossing tests remain in place.

## Validation and limits

The two new tests [failed before the fix](baselines/2026-09-12-page-row-count-before.log).
All eight segment-reader tests pass after it. All six local gates pass: 923
workspace tests, 10 deliberately ignored benchmarks/subprocess entries, doc
tests, formatting, checking, warning-free workspace Clippy and release node build.
The initial sandboxed suite could not bind loopback mock servers; rerunning with
socket access passed. The [release comparison](baselines/2026-09-12-page-row-count.md) passes: no
repeatable median regression above 5% was established. Linux/macOS CI remains
pending in the implementation PR.

Reviewed the touched helper and all four callers for obsolete paths: none need
removal. Typed integer decoders retain their existing length validation; the
shared row check protects the variable decoder and future callers.

This fix does not impose a decompression or total-query memory budget. Variable
pages still have an unbounded decode path; bounded maintenance decoding exists
but blindly applying its limit to queries could preallocate excessive memory or
reject valid data. Query allocation/resource accounting remains an explicit
follow-up in the offline audit, along with manifest/raw-column bounds. No claim
of complete storage or query audit is made by this fix.
