# Execution cache payload admission

## Finding B4-21

**Severity: medium, availability/resource predictability.** The optional execution
serving cache limits retention to 4,096 block/receipt entries and 8,192 headers,
but transaction inputs and receipt logs vary in size. Count limits do not supply
an operational payload budget. Four sync call sites clone bodies before optional
cache admission; even a historical block immediately evicted by the count policy
pays that copy cost. These are validated blocks, not an unauthenticated cache
injection claim. No measured allocation exhaustion or throughput regression is
claimed.

## Selected policy

Retain the count limits and add a 128 MiB **logical normalized payload** budget
for cached blocks plus receipts. Charge the encoded header and body lengths plus
the sum of each receipt's encoded length with a fixed zero bloom. A bloom's value
does not change its encoded length; no bloom hashing, serialization or compression
is needed. This is the same normalized representation used for payload telemetry,
not the original ETH69/70 wire representation. It includes a bloom in each receipt
charge even though the cache stores bloom-free receipts, and excludes an outer
receipt-list envelope.

This is not a resident-memory or allocation cap. Spare capacity, allocator/map
metadata, hidden shared Bytes backing allocations, the separately count-bounded
header inventory, caller batches and provider-owned outgoing response copies are
outside the metric. Generic InMemorySize is intentionally avoided: the pinned
signed-transaction estimator can initialize a transaction hash, and the generic
estimators are explicitly heuristic. A strict process-memory guarantee would
require broader lifetime and backing-allocation changes; that is not necessary
for a useful optional-cache retention policy.

Admission prefers recent block numbers, matching current count eviction. A
candidate is admitted only if it and newer retained entries can fit both limits;
older entries can be evicted to make room. Reject an oversized or too-old optional
payload without failing ingestion or evicting useful data for a candidate that
cannot remain. Publish its verified header and remove conflicting canonical body
entries even when its payload is skipped. Keep independently retained headers,
source verification, persistence and normal query behavior unchanged.

Borrow header/body at the cache API so admission can precede optional clones.
Keep payload-length work and copies outside the cache write lock. A read preflight
avoids known rejected copies, followed by a final write-lock recheck and exact
cached-charge updates on replacement, eviction and explicit removal. Concurrent
state changes can conservatively miss an admission or cause a preflight-accepted
copy to be discarded. The final retained metric must stay within the limit; no
transient allocation or outgoing-response bound is inferred.

The existing status wrapper continues synchronizing its history range after
insertion. The advertised cache history range is the contiguous suffix ending at the
highest retained body; other retained entries may have gaps. The existing
empty-cache status fallback to the local head is a separate policy review item;
this milestone does not claim that fallback is cache-derived.

## Validation status

Six controls against unchanged production at `bc585616` yielded one pass and
five policy failures. Their original fixture constructor intentionally ignores the
small requested budget because the original provider has no such setting. This
establishes the missing policy with small examples; it does not claim an existing
runtime option was ignored or a production out-of-memory event was reproduced.
An initial test-authoring bracket typo was corrected before the original run;
the compiler error and subsequent original failures are retained separately.

All 32 cache tests now pass, including eight new controls: exact/over budget,
independent body and receipt growth, multiple oldest evictions, descending arrival,
oversized canonical replacement, duplicate/removal/reorg accounting, a 36-operation
reference model and four concurrent writers inserting 32 small blocks. Independent
actual RLP encoding checks every retained charge, total and map identity after
mutations. Existing provider, genesis, count limits and range tests pass unchanged
apart from adapting calls to references. Tests use structural typed fixtures rather
than authenticated chain data and perform no peer I/O or production writes.

All four engine callers now pass borrowed header/body values. Rejected optional
payloads therefore avoid body and receipt-vector copies; the full-count/older
case also skips payload traversal. Admitted entries add one allocation-free length
walk and short read preflights. Under pressure, preflight and final admission can
each inspect at most 4,096 older entries. Payload cloning stays outside the write
guard; actual evictions and map updates stay inside it. No benchmark or quantified
speedup is claimed. The 128 MiB policy is a deliberate fixed operational default,
not a measured optimal cache size.

Independent implementation review found no actionable defect. Seven local gates,
CI and merge remain pending. Evidence is recorded in
[the validation record](baselines/2026-09-16-execution-cache-payload-budget.json).
The broader offline audit remains open. Mac-mini tests and obsolete-artifact
cleanup are complete; no new remote work was needed.
