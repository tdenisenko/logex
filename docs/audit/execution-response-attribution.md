# Execution response attribution

This milestone removes receipt-peer penalties based only on transaction counts
from another peer's unvalidated body response. Transport assembly retains both
source IDs. Existing body/header commitment checks establish body validity before
receipt count, root, gas and bloom checks and before ingestion.

## Finding B4-12

**Severity: medium, availability and peer attribution.** A decoded body is not yet
proof of its transaction count. The paired request planner, cached-arrival paths,
salvage and separate receipt APIs used this count as an expectation. More receipts
than expected caused a protocol penalty; fewer caused receipt-role quarantine or
disabling. An incorrect body from peer A could therefore penalize an honest receipt
response from peer B, reduce useful peers and delay ingestion. This finding does
not establish acceptance of invalid logs: consumer validation already checks the
body first and rejects invalid content before publication.

The original count validator and two classification functions are retained in the
local reproducer. With one receipt and a body-derived expectation of zero or two,
the original policies respectively blame or disable the receipt peer without
independent evidence. The control uses real receipt types and the exact original
helpers; it is not a full baseline node or network integration test.

## Correction and invariants

- Receipt APIs no longer accept a per-block count hint. Live, historical and four
  anchored call sites retain peer preferences, timeouts and attempt limits.
- Completed body and receipt responses pair only with exact requested outer block
  cardinality. An explicit internal assertion records that every body caller has
  already completed its exact outer request; network partial responses cannot
  reach this assertion. Each inner receipt list remains intact, including a complete empty
  list, and separate source IDs remain available to content validation.
- Outer overflow, empty response handling and partial-prefix retries remain.
  ETH70 continues to use its incomplete-last-block flag and receipt cursor;
  append shape, overflow and no-progress checks remain independent of body counts.
- Consumers retain body commitment validation before cross-source count and
  receipt-root validation. A bad body is attributed to its supplier; a correct
  body with inconsistent receipts is attributed to the receipt supplier.
- Request ownership and session accounting from PR #176 remain in place. This
  milestone adds no disk write, format, dependency, validation hash pass or benchmark.

Removing count vectors and unsupported early comparisons reduces planner work.
No numerical throughput claim is made. Receipt progression budgets and send-queue
deadlines are separate review items; exact outer completion does not itself bound
an indefinitely progressing ETH70 response's total memory.

## Validation and cleanup

Three added tests and all 344 sync library tests pass (one ignored). Both exact
original-helper controls fail as expected, and candidate source is restored
byte-for-byte. Final independent review passes. Source `4c60b745` passes all seven local gates: vendor verification, formatting, workspace check, strict Clippy, 1,448 workspace tests (23 ignored), documentation tests and release node linking. All six CI jobs passed on head `2bb89e91` (run `35010074401`); PR #177 merged as `7bc714c8` after exact head/base verification.

The regression scope includes both response arrival orders, mismatching inner
counts, empty inner receipt sets, incomplete outer shape, and existing ETH70
continuation controls. The consumer fixture uses one structural transaction and
its header commitments without submitting or executing a transaction. Both
parallel validation and streaming validation/extraction must identify the correct
source and accept the complete control fixture.

Obsolete count-hint APIs, vectors, mismatch metadata and unsupported early-blame
branches are removed; authenticated consumer count checks remain necessary.

Evidence is indexed in [the validation record](baselines/2026-09-16-execution-response-attribution.json).
No remote work was needed. Mac-mini testing and owned temporary-file cleanup are
complete; its physical external volume and unrelated files remain untouched.
