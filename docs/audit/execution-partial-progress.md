# Execution request progress and failure accounting

## Findings

- **B4-24 — medium, useful-peer scheduling:** sequential body and ETH68/69 receipt
  callers recorded a positive partial reply as success, then immediately paused
  the role for eight seconds, incremented its failure counter and demoted it.
  The same loop requested the tail immediately; the penalty instead affected
  later scheduling and could survive local cancellation or deadline expiry.
- **B4-25 — medium, request adaptation:** a slow partial reply reduced the request
  limit once for elapsed time and again for response shape. The existing decrease
  is ceiling(two-thirds), so 32 became 22 then 15. A fast partial returning at least
  the current limit could instead increase and decrease it in the same response.
- **B4-26 — medium, failure attribution:** after an original four-block request
  returned two blocks, a two-block tail reply containing three or four blocks was
  classified against the original four. It became partial or complete for global
  failure handling, although the collector had rejected it. Role disabling and
  final content validation already prevented publication; this is not a demonstrated
  invalid-data acceptance or query-result bug.
- **B4-27 — medium, peer lifecycle:** successful ETH68/69/70 receipt retries returned
  before removing earlier peers already marked for removal. A peer at timeout
  counter seven remained registered after reaching eight if another supplier won.

Small, typed channel controls reproduce these effects on unchanged production at
`9317c42a`. Three positive controls passed and nineteen regression assertions failed.
The fixture binds a dormant loopback listener for Reth handle construction, with
no discovery or connection tasks. Inputs contain at most a handful of empty block
or receipt containers; no live service, production data or large allocation is used.

The protocol permits smaller replies, including its recommended body response-size
limit ([devp2p specification](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#blockbodies-0x06)).
Useful transport progress remains provisional: existing header, body and receipt-root
validation still determines whether fetched data can be ingested.

## Changes and invariants

Common progress bookkeeping resets timeout state, clears the role pause, clears
receipt quarantine and updates serving status/rate. Complete replies retain the
existing latency/count adaptation. Positive sequential partial replies use the
same bookkeeping followed by one existing two-thirds limit reduction; they add
no failure penalty or pause. True empty replies, transport timeouts and malformed
responses retain their existing failure handling. Parallel and ETH70 collector
progress semantics are otherwise unchanged.

The ambiguous private `Incomplete` failure is replaced by explicit `EmptyResponse`
and `ResponseOverflow { requested, returned }` outcomes. Every body/legacy receipt
collector and both early receipt guards retain the current wire request size.
Original chunk size remains available for whole-chunk telemetry but cannot turn
an overflowing tail into useful progress. Both outcomes continue to disable the
failed role. Overflow triggers existing protocol-fault removal.

Both successful receipt return paths now remove the accumulated dead-peer set,
matching body/header behavior while retaining the winning supplier for every
returned block. Timeout increments saturate at the existing removal threshold,
matching soft-failure counters. The synthetic arithmetic boundary test is defensive;
no practical path to billions of timeouts or overflow vulnerability is claimed.

The old partial-failure helper and its obsolete partial/complete error-classification
branches are removed. No network wire, storage format, validation authority,
request deadline, retry budget or concurrency setting changes. Per-response work
is slightly smaller, with no new payload copies or allocations. No benchmark or
throughput gain is claimed.

## Validation

The twenty-two original controls cover three useful-prefix APIs, slow/fast/complete
adaptation, genuine timeout/empty policy, twelve manager/plan tail cases, and three
successful-retry cleanup versions. Four final controls additionally cover restoration
of old failure/quarantine state, one exact rate update, defensive counter saturation,
and a fast partial that must not increase its limit before decreasing it.

An initial test compilation mismatch and incorrect halving oracle were corrected
before interpreting original results; their logs are retained. The first final
focused run passed 245 tests and failed a synthetic test that expected integer-max
saturation instead of the chosen existing threshold. That test expectation was
corrected; production behavior was unchanged. These harness issues are not recorded
as product defects. Final review and gate outcomes are recorded below.

All 247 peer-manager tests (26 new controls) pass. Independent final source/test
review found no remaining actionable issue. All seven local gates pass on `764dbe5e`, including
1,558 workspace tests (23 ignored), doc tests and release linking.
All six CI jobs passed on `691a4649`;
[PR #187](https://github.com/tdenisenko/logex/pull/187) merged as `02a812e4`.

[Validation record](baselines/2026-09-16-execution-partial-progress.json).
No Mac mini work or additional cleanup was needed in this milestone.

## Reverse header pages found during live acceptance (2026-09-26)

**B4-50 — medium, useful-prefix retention and peer attribution:** the parallel
header collector accepted a positive short response and then appended the next
preplanned page. Its numeric start assumed a full preceding response. For example,
requests for `[105, 104]` and `[103, 102]` could produce `[105, 103, 102]` when the
first supplier returned only header 105. Cross-page validation rejected the gap,
discarded useful work and incorrectly blamed the second supplier.

The [header request protocol](https://github.com/ethereum/devp2p/blob/master/caps/eth.md#getblockheaders-0x03)
specifies a maximum reply count, so a positive short response must remain usable.
The final collector keeps the contiguous prefix through the first short page and
lets the next fetch continue at the missing header. Missing, empty and oversized
pages also end the prefix; later results cannot restart it. All already completed
requests still receive the existing session-aware success/failure accounting and
successful-payload byte accounting, even when their headers cannot join the prefix.
The empty-response return policy is preserved.

Header ancestry and execution validation remain the ingestion gate. Request sizes,
parallelism, retries and deadlines are unchanged. The correction adds one prefix
state flag without payload copies, a cache or persistent writes. It avoids a
demonstrated discarded-prefix path; no measured throughput gain is claimed.

The acceptance log had eight 512-block boundary gaps between 08:11 and 08:17 UTC,
with supplier disconnections and subsequent historical progress. Per-page reply
lengths were not logged, so that exact live cause is inferred from the pattern and
source. The local finite-response controls reproduce the assembly defect directly:
short first/middle pages failed on unchanged source; complete and short final pages
passed. With the correction, six controls cover prefix continuity, continuation,
out-of-order completion, peer retention, received-payload accounting, completed
timeouts and missing/empty-page behavior. These use the existing dormant loopback
fixture and at most seven linked headers. Two fixture assumptions (channel admission
and empty-response telemetry) were corrected before interpreting their results.

The original client and supervisor exited cleanly at 2026-09-26 08:26:12 UTC after
monitoring was paused. Only the owned acceptance dataset was deleted; logs and
useful build artifacts were retained. Full sync and the 48-hour window require a
new uninterrupted run after the correction is validated, merged and built natively.
All six required local gates pass: 2,528 workspace tests, 24 existing
ignores across 43 targets, doc tests and release build. Merge is gated on all
required CI checks. Native compilation and fresh acceptance follow merge.
[Validation record](baselines/2026-09-26-reverse-header-prefix.json).
