# Log extraction boundaries

Batch 1 follow-up to receipt validation and beacon SSZ validation. The row schema,
receipt provenance model, and successful public conversion behavior remain unchanged.
This is boundary hardening, not evidence of a forged authenticated Ethereum block.

## Findings and changes

### B1-15 — lossy primitive conversion (P2, fixed)

`LogRow::from_primitives_log` previously selected the first four topics from an
unchecked Alloy `LogData`, silently dropping any others. A five-topic reproducer
failed before this change. The data length also used `usize as u32`, which would
truncate a payload larger than the persisted `u32` field can represent.

Add `LogRowConversionError` and checked `try_from_primitives_log` /
`try_from_alloy_log` constructors. Reject excessive topics and use a checked data
length conversion before constructing a row. Existing constructor signatures stay
available and delegate to the checked conversion, with documented panics for
unrepresentable input. Sync uses the fallible constructor and propagates errors.
Valid callers require no migration; callers accepting unchecked input should use
the new methods. No topic, payload, or result is silently truncated.

These methods still require caller-authenticated receipts and supplied metadata.
They do not authenticate RPC metadata, and they still assign `Source::Receipt`.
Public struct fields and serde construction remain available; persisted-row
validation belongs to the storage review. The receipt wire codec already rejects
excess topics. No reachable mainnet four-gigabyte log was demonstrated.

### B1-16 — extraction completeness and numeric bounds (P3, fixed)

The internal body/receipt extractor used `zip` without checking lengths; a receipt
for an empty body was silently discarded. Production callers already checked
transaction counts and receipt roots, so this is a missing local invariant, not a
newly demonstrated network validation bypass.

Require matching counts, checked row-count addition, representable transaction
and block log indices, and fallible row allocation. Check the last index rather
than requiring the count itself to fit: `u32::MAX` remains a valid index. Global
block indices derive from the number of rows appended for this block, avoiding
an overflowing final `u32` increment. Historical batches can contain more rows
than one block's index domain, subject to `usize`/allocation bounds.

Both extraction paths now share the same row-building loop. On an error in a
later log, truncate only this block's appended rows; prior batch rows remain
unchanged. Capacity may grow. Historical allocation/extraction failures use the
local error channel rather than incorrectly attributing them to a peer. Existing
body/receipt commitment failures retain peer attribution. A failed historical
chunk is never returned for writing. Checkpoint-gap extraction happens before
that block's head/cache updates, and ordinary ingestion extracts before storage
writes, coverage metadata, or notifications.

This does not prove cancellation, head-tracker rollback, crash consistency, or
multi-block publication atomicity; those remain in batches 2 and 5.

## Validation

- Before-fix failures: fifth-topic loss and ignored unmatched receipt.
- Checked conversion covers 0–5 topics, absent versus zero topics, exact payload
  length, maximum metadata and indices, and both public conversion paths.
- Arithmetic tests cover zero, `u32::MAX`, the last representable index,
  over-limit counts, and `usize` addition overflow without giant allocations.
  No actual four-gigabyte payload or billions-of-rows fixture is allocated.
- Both extraction paths preserve empty transactions and transaction/global log
  ordering. A malformed later log leaves an existing batch unchanged; both
  directions of transaction/receipt mismatch fail.
- A synthetic header/body/receipt with self-consistent commitments but a malformed
  log exercises local extraction failure after commitment validation. It is not
  a valid EVM block or a consensus proof; it tests error routing without network
  fixtures or large allocations.
- All six local workspace gates passed: formatting, locked all-target check,
  strict Clippy, 769 tests (three explicit ignored benchmarks), doc tests, and
  release node build. Cross-platform CI is required before merge.

## Release comparison

Run `cargo test -p logex-sync --release --locked extraction_release_baseline --
--ignored --nocapture`. The fixture has 4,096 transactions with 0–4 logs each,
256 payload bytes per log, and 0–3 topics. Expected rows are constructed
independently and checked before timing. The owned path allocates and drops each
result; the append path clears and reuses a preallocated buffer. Each process
warms each path for 100 iterations and then records five 100-iteration samples.
These are warm in-memory conversion measurements, not network, cryptographic
validation, disk ingestion, or full-node throughput.

The initial checked implementation regressed both paths by roughly 47% in five
alternating process pairs. A short macOS `sample` profile captured the conversion
function, row-copy routines, and successful `eyre::WrapErr` calls in the hot loop.
Inlining the checked constructor reduced that cost but left roughly 22% overhead.
The next revision handles errors explicitly in the loop, so error context is
constructed only on failure and successful rows do not pass through a generic
error-context adapter. All numeric/shape checks and rollback behavior are retained.
Final measurements retain +7.59%/+8.92% median costs after isolating diagnostic
formatting in a cold function. See the [baseline report](baselines/2026-09-09-extraction.md)
for raw samples, profiling, environment and the explicit correctness tradeoff.
