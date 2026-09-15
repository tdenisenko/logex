# Consensus history work and retention consumers

This milestone removes repeated work from status, forward execution ingestion,
history scheduling and duplicate verified range updates. It preserves retained
history and successful query/ingestion results. The findings are performance
opportunities, not evidence of previously wrong anchor selection or ancestry
results. No benchmark campaign or production-data operation was performed.

## Findings

| ID | Before | After and evidence |
| --- | --- | --- |
| B3-55 | Every coverage call scanned all adjacent anchor records under the shared state mutex to count gaps. Status, sync-head and restart helpers call it repeatedly. | Cache the derived count in the same in-memory snapshot, recompute on anchor mutation/restore, and publish it with the durable candidate. Coverage reads are constant-time. The cache is skipped by serialization. |
| B3-56 | Every bounded forward batch cloned the entire anchor vector and scanned its prefix. Reorg fallback cloned it solely to check for an anchor at/before the tip. | Use a binary partition point and copy at most the requested number of records; keep the existing contiguous/bootstrap selector. Reorg fallback reads the coverage floor. Small actual-store controls compare against the previous full-history algorithm. |
| B3-57 | History-target refresh built and reversed a complete canonical chain only to test whether one existed. | Share the original checked reverse traversal between real collectors and a no-op visitor. Completeness allocates no chain vector. An independent reference over 500 small graphs and explicit metadata/checkpoint controls checks equivalence. |
| B3-58 | Identical applied range updates cloned and durably rewrote the entire snapshot. A duplicate response can still be relevant when its attested slot is above the finalized slot. | Compare the full verified store, selected summary improvements and final per-period raw payloads under the existing writer. Skip only an exact semantic no-op. An isolated original-method control fails the unchanged-file assertion; changed component controls still publish and reopen. |

## Derived coverage and bounded reads

`anchor_gap_count` is private, skipped by serde and never trusted from disk.
Every production materialized-anchor mutation already recomputes anchor summaries;
that path now also recomputes gaps in the private candidate. Restore validates
ordering and recomputes it. Light-client-only changes preserve the unchanged count.
Readers obtain floor, ceiling, count and gaps from one snapshot lock. The existing
write failure latch and durable-publication boundary apply to the cache as well.
Readers see the previous coverage while a write is blocked or fails, and reopen
recomputes coverage from whichever snapshot became durable.

`anchor_records_after` performs a partition point on the strictly increasing block
numbers, bounds the copied suffix by its actual length and the requested limit,
and handles zero and maximum values without incrementing the query block number.
For N retained records and K returned candidates, selection changes from O(N)
copy/scan to O(log N + K). The existing contiguous selector still stops at gaps and
allows a starting jump only under its previous no-execution-head condition. Its
output allocation and the bounded temporary suffix remain separate small vectors.
No global history copy is needed merely to classify an all-ahead reorg case.

Gap recomputation remains O(N) when materialized anchors actually change; those
changes already clone/serialize the full snapshot. This moves work away from
repeated reads rather than eliminating all history-dependent mutation cost.

## Ancestry completeness and applied updates

The shared visitor preserves the original checkpoint root/slot checks, map-key/root
agreement, missing-parent rejection and strictly decreasing parent slots. The last
condition also prevents cycles. Real callers still collect and reverse the chain
into checkpoint-to-head order. The visitor can see a prefix before a failure, but
the collector keeps it private and discards it on error; completeness uses an empty
visitor. Traversal remains O(lineage length). This helper did not validate execution
parent hashes before the change and does not newly claim that validation.

Applied-update preparation compares complete `VerifiedLightClientStore` equality,
including committees, headers, best update and participation counters. Summary
priority rules are unchanged. Raw payload equality includes both context and bytes.
A bounded incoming BTreeMap preserves the old last-duplicate-key-wins behavior;
an earlier duplicate alone cannot force a write when the final value is unchanged.
That small per-response normalization is additional work, bounded by the existing
verified response count. Actual changed payloads, store or chosen summaries still
clone, save and publish normally. The prior storage failure is checked before the
no-op decision, so an unchanged response cannot hide an uncertain save.

The implementation removes full-history copies, repeated scans and file
replacement directly; no wall-clock gain or throughput percentage is inferred.
Existing real-write synchronization and trust checks remain unchanged.

## Retention disposition and remaining work

The current architecture uses ordered anchors to reconstruct verified Beacon
metadata on restart, follow checkpoint-to-head lineage, select forward execution
batches, classify reorgs and calculate restart freshness. There is no demonstrated
safe arbitrary cutoff. The period-payload map also has real serving and restore
validation consumers. It retains one payload per period, independently of the
128-response request bound. That request bound is not a lifetime retention cap.

Optional raw bodies are separately bounded to 128 MiB of retained capacity and
4,096 resident entries. These limits do not bound verified metadata/child maps,
ordered anchor history or accumulated period payloads. Metadata may include bodies
not yet connected to trusted lineage; those candidate records require a separate
admission/eviction review that preserves pending work and canonical ancestry.

This milestone deliberately does not delete anchors or payloads. A continuously
running process can outlive the startup checkpoint-freshness window. Actual changed
snapshots still clone/rewrite accumulated history, and materialization still builds
real anchor records when needed. The remaining roadmap names metadata admission
and a justified lifetime/cost policy explicitly; they are not closed by a passing
no-op or bounded-selection test.

## Validation and cleanup

The focused suites pass 313 consensus and 323 sync tests (one ignored each),
and strict CL/sync Clippy. The initial new
applied-update fixture already contained context, contrary to the draft test's
assumption. The final control explicitly begins with the valid contextless variant;
it exercises context-only persistence, last-key-wins no-op behavior, summary-only
and verified-store-only changes, reopen and the failure latch. The original applied
method was substituted into that candidate harness and failed on file replacement;
the candidate was restored byte-for-byte. This is an isolated method control, not
a full baseline checkout. No timing control is substituted for correctness.

Coverage/suffix controls compare against independent scans across empty, linked,
gapped, overlapping, reversed-range and maximum-number cases. Existing paused-save
and after-replacement-error tests now check coverage alongside records. The new sync
maximum-number fixture initially overflowed its ordinary timestamp helper; the
corrected fixture sets its block number independently. This was a test setup issue,
not a source failure. Independent reviews cover cache publication, bounded consumer
equivalence, ancestry traversal and exact update comparisons.

Removed both production full-history copies from `anchored.rs`, the collecting
method wrapper used solely for completeness, repeated gap scans in readers and
unconditional duplicate-update persistence. Full history reads remain required at
network startup; real canonical collectors remain required for serving and
materialization. No dependency, unsafe block or new persisted field was added.

Source `7ef16e28` passes all seven local gates: vendor integrity, formatting,
workspace check, strict Clippy, 1,397 workspace tests (23 ignored), documentation
tests and the release node build. All six CI jobs passed on head `c0059535`
before [PR #171](https://github.com/tdenisenko/logex/pull/171) merged as `ccb19d3b`
after exact-head/base verification.
Final focused, workspace and CI results are recorded in the
[validation record](baselines/2026-09-15-consensus-history-cost.json).
This milestone does not establish offline completion or release readiness.
