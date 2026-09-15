# Per-block execution receipt sources

This milestone preserves the supplier of every block's receipts from standalone
fetching through validation, failure handling and successful-serving credit.

## Finding B4-15

**Severity: medium, attribution and availability.** The parallel receipt collector
already records each successful chunk's supplier. A standalone API adapter then
took the first supplier, discarded every per-block source and returned that one
peer with the whole batch. A later block from another supplier could therefore
cause the first peer to be penalized, disconnected and forgotten. Valid data from
later suppliers was also credited to the first peer.

Content validation still rejects receipt disagreements; this finding does not
establish acceptance of invalid block data. Mixed sources require a large enough
batch, multiple dynamic chunks and different successful suppliers. Following
PR #179, explicitly limited APIs use the sequential path, so those two callers
need the uniform type migration but are not currently exposed to mixed bulk
sources. Four unbounded consumer paths remain exposed.

The actual original public preferred-receipt API reproduces the defect using 64
hashes, two 32-block ranges and ordinary decoded receipt markers. The later range
is answered and processed before the earlier range. All payload-order assertions
pass, but the supplier assertion fails on the second range. The fixture uses
local channels, paused time and the same dormant localhost manager described in
[the explicit-limit milestone](execution-request-limits.md).

## Correction and cost

The three standalone receipt APIs return the existing `SourcedReceiptSet` for
each block. Their inner implementation directly retains the parallel collector's
sourced vector. Sequential and ETH70 success paths tag their receipt sets with
the selected peer. Empty requests return an empty vector.

All six consumers preserve those tuples. Three indexed validation loops use each
block's receipt supplier for failures and serving credit; three paired assembly
sites directly zip sourced bodies with sourced receipts. Outer length checks,
body-before-receipt validation, receipt contents and block order remain intact.
Batch-shape diagnostics no longer invent a single supplier for a mixed batch.

Remove the obsolete source-stripping adapter and its return alias. The parallel
path avoids that additional conversion/allocation. Sequential prefixes receive a
peer ID as they are collected, avoiding a second full-batch conversion and any
receipt-payload cloning. Scheduler limits, concurrency,
timeouts, retries, salvage, ETH70 continuation and content-validation work stay
unchanged. No storage format, write, dependency version or benchmark is added.

## Validation and remaining scope

All 364 sync tests pass (one ignored), including five source controls for
out-of-order replies, replacement-peer retries, partial responses, empty requests
and ETH70 continuation. The existing explicit-limit controls remain green.
The new consumer control
checks successful source retention and serving notes for two suppliers, then
confirms that a second-block receipt/header disagreement is attributed to that
block's supplier in both parallel validation and streaming extraction.

See [the validation record](baselines/2026-09-16-execution-receipt-sources.json).
Final independent review and all seven local gates pass on source `35750c2d`:
vendor verification, formatting, workspace check, strict Clippy, 1,468 workspace
tests (23 ignored), documentation tests and release node linking. PR and CI remain
pending. Aggregate
salvage/ETH70 budgets and the telemetry encoding-cost lead remain separate review
items. The broader offline audit, external-volume supervision, verified repair
and later live/staging acceptance remain incomplete. Mac-mini testing and owned
artifact cleanup remain complete; this milestone requires no remote work.
