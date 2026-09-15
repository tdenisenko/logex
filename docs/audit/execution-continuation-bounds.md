# Standalone execution continuation lifetime

## Findings and scope

**B4-22 — medium, availability:** standalone body and receipt requests apply a
fresh timeout to each queue admission/response exchange. Positive partial replies
can extend a selected peer's work well beyond a reasonable role window. Body and
ETH68/69 progress is bounded by the finite requested hash count. ETH70 also permits
partial receipts within one block; repeated nonempty incomplete fragments advance
the cursor without completing even one requested block. The standalone collectors
have no aggregate deadline, and their engine callers have shutdown cancellation
rather than an elapsed bound. Both sequential fallback and standalone parallel
chunks reach these helpers.

The paired request plan is different: its main phase already has a 45-second
window and PR #183 enforced its 12-second salvage window. Do not add redundant
standalone timers inside those already-supervised plan methods.

**B4-23 — open, resource retention:** time limits do not bound fast accumulated
receipt fragments. The 10 MiB upstream per-message bound and bounded bloom lookup
caches also do not bound the merged receipt list. This is a separate required
correction. No memory-safety or complete resource-budget closure is claimed by
B4-22.

## Selected elapsed policy

Use one 45-second local continuation timer per selected peer/role. If an explicit
per-exchange timeout is longer, allow that duration as the continuation window so
one explicitly permitted exchange is not shortened. Reuse the timer across
partial exchanges, queue waits and ETH70 fragments; give local expiry priority
when it becomes ready with a per-wire timeout. Keep normal per-wire limits and
peer-selection limits.

Return a distinct `ContinuationDeadline` outcome, leaving peer timeout counts, request
pauses, reputation and receipt quarantine unchanged by expiry. Existing positive
partial-response scoring remains unchanged; controls compare the same partial
sequence with and without expiry rather than claiming that sequence has no score effects. The unsuccessful attempt
still uses existing finite retry selection. Preserve the existing data behavior:
sequential body collection can retain full accepted block prefixes across peers;
sequential receipts restart their collection on another peer; completed parallel
chunks stay with their sources and statistics. Incomplete fragment lists are not
published. No successful result is silently truncated.

This is a per-peer/role limit, not a single 45-second bound on an entire public call:
parallel waves, retries, candidate peers and discovery refill can add time. It is
cooperative cancellation, cannot preempt synchronous work in one poll, and cannot
retract a transport request already admitted to Reth. The private receiver is
closed when local waiting ends.

## Resource follow-up

[The receipt-pagination specification](https://eips.ethereum.org/EIPS/eip-7975)
requires checks informed by block/transaction context before retaining arbitrary
partial lists. Previously removed body-count hints were unverified and must not
be restored as authority. Current gas vectors only guide scheduling and are not
retained in the plan; trusted request metadata must be explicitly aligned and
carried if used for rejection.

A potential conservative bound charges transaction base gas plus retained LOG
costs and compares it with twice authenticated net gas used. Historical refunds
are relevant: [EIP-3529](https://eips.ethereum.org/EIPS/eip-3529) reduced the old
one-half cap to one-fifth. This is not an encoded-receipt gas/8 limit; fixed receipt
fields and internal bloom reconstruction require separate reasoning. Source review supports the conservative inequality through the pinned mainnet
schedule, including historical refunds. Enforcement, provenance-carrying request
metadata and cumulative fragment accounting remain B4-23 work.
A global pool for simultaneous requests and outgoing copies remains distinct.

## Implementation and cost

The sequential body and legacy receipt loops own one pinned timer per selected
peer. Standalone parallel helpers own one timer per chunk/role attempt. The shared
manager ETH70 helper owns the fragment timer, so its sequential and parallel
callers do not create duplicate timers. The paired plan retains its own existing
main/salvage supervision. The generic wrapper is unboxed and selects local expiry
first, including a tie with a wire timeout. Genuine earlier wire errors retain
their existing classification and retry behavior.

Neutral expiry has the lowest request-failure severity during coalescing, cannot
mask a genuine earlier timeout, and neither disables nor quarantines the peer.
Finite retries may select it again. The timer adds no payload copy, storage write,
spawned task or per-row work. This is a source-level cost review, not a measured
speedup or a claim about overall ingestion throughput. No benchmark was run.

## Validation status

The eight original-code paused-clock controls compiled: one normal/long-timeout
control passed and seven selected-deadline controls failed. The original production
file matched the base byte-for-byte except for its test module declaration. These
prove absence of the selected policy, not an infinite body/hash-count loop.
The existing manager fixture needs a dormant localhost listener solely to
construct the pinned Reth public handle; it runs no network/discovery services and
makes no peer connections.

All 13 final controls passed. They cover public body and ETH68/69/70 expiry;
private standalone helpers; explicit 60-second wire timeout with success at 50;
completion across partials within the window; preserved body prefixes versus
restarted receipt prefixes; a real earlier wire timeout; neutral local accounting
and failure coalescing; exact timer ties; and parallel completed-chunk/source/stat
retention while another range expires and succeeds on retry. Independent source
review found no actionable defect. Searches retained the still-used per-wire,
paired-plan and merge helpers; both explicit-limit API comments were updated.

The first gates passed vendor verification, formatting and workspace checking;
strict Clippy identified a test-only cloned one-item slice. It now borrows via
`std::slice::from_ref`; the failed log is retained. Full gates are rerun on the
corrected commit `8a34ef6b`: all seven gates pass, including 1,518
workspace tests (23 ignored), doc tests and release linking. All six CI jobs then passed on `11098ade`;
[PR #185](https://github.com/tdenisenko/logex/pull/185) merged as `1dac15e2`. Evidence hashes and commands are
recorded in [the validation record](baselines/2026-09-16-execution-continuation-bounds.json).
No mac-mini work is planned.
