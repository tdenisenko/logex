# Live subscription ownership and retained history

Base: PR #227 merge `50c1194ff5f9c56581587a84eb46a408cd62b1e7`.
Branch: `audit/api-subscription-lifecycle`.
This is the first batch-8 subscription milestone. Implementation and eleven local gates are complete; PR #228 merged as `910a74ae` after all six CI jobs.

## Findings and corrections

**B8-06 — named ephemeral sessions outlive their last socket (P2).** A supplied
subscription ID sends an otherwise default ephemeral WebSocket request through
the retained-session manager. Its last disconnect decrements the active count
but neither removes nor expires the entry, so it keeps collecting events.
The manager now removes ephemeral entries after their last owning attachment disconnects. An
explicit Service session retains its process lifetime; Dashboard keeps its
existing reconnect grace period. HTTP subscription creation explicitly selects
Service and is unaffected.

**B8-07 — an old socket detaches a replacement session (P2).** A detach guard
identifies its session only by the public string ID. Deletion and recreation
can reuse that ID while the old socket still owns a guard. Dropping the old guard
then reduces the replacement's active count and may schedule expiration while
its replacement socket is connected. Guards now carry the original session
instance identity. An ordinary upsert of an existing entry still shares its identity,
configuration, channel and history across overlapping connections.

**B8-08 — retained history drops the wrong end of a batch (P2).** Reverse
iteration followed by front insertion keeps each incoming batch internally
oldest first. Consecutive batches then produce inconsistent overall ordering.
At the 10,000-entry history cap, a sufficiently large batch can discard its newest
entries. The manager now retains notifications newest first in publication order and evicts oldest
entries regardless of batch boundaries. Live notification arrays keep their
existing chronological order and eviction counts remain accurate.

## Scope and cost

The correction is limited to retained subscription ownership and history. It
does not add ingestion/storage barriers, change filtering, impose new session
budgets or modify query execution. A private Arc identity identifies the entry without keeping a deleted
broadcast sender alive. Drop cleanup remains effective when storage failure
or request cancellation ends the socket future.

Existing shared-ID upserts intentionally update the shared filter and scope;
this pass preserves that behavior, including changes between scopes. History
can therefore include earlier filter configurations. Service subscriptions remain
explicitly retained until deletion or process exit. No aggregate memory ceiling,
throughput gain or complete subscription correctness claim is made.

## Regression evidence

Four finite in-memory manager/guard controls fail on the exact original source,
while its fifteen existing WebSocket tests pass. They cover final ephemeral
detach, delete/recreate with an old guard, batch-invariant history and the actual
10,000-entry history cap. The final source passes all 108 server library tests
and strict server Clippy; independent review found no actionable scoped defect.

Positive controls cover overlapping connections, dashboard grace expiry, explicit
service retention, ordinary upserts and current filter/scope behavior, chronological
live wire batches, dropped-work cleanup and deletion closing the old broadcast
channel despite a surviving identity guard. The receiver-closure check uses an
empty channel; an already-buffered batch may still drain before closure is seen.
No live sockets, network traffic or existing user data were used. Workspace gates
and platform CI are recorded below before closure.

## Remaining batch-8 work

Read-only reviews also identified JSON-RPC envelope/error handling, Ethereum
filter normalization and block-tag semantics, silent broadcast gaps and missing
reorg retractions as subsequent milestones. Each requires focused reproductions
and its own validated fix. A source review also confirms that retained sockets
keep their initial acknowledgement snapshot after sending it; releasing that
unused copy belongs with the subsequent socket-lifetime cleanup. No memory
measurement or global-bound claim is inferred. The dashboard mirrors the old within-batch ordering
and remains in batch 9. Shared query memory/admission policy remains a separate
pending decision. Authentication routing and current query-cancellation ownership
had no newly demonstrated defect in this review; broader integrated acceptance
remains in batch 12.

## Final local validation

All eleven final-source local gates pass: 1,973 workspace tests, zero failures, 24 existing ignores across 35 targets, documentation checks and release node build.

Source is `dbd5b48a9767bbd35ca26f7c68f340312323188e`. The [validation record](baselines/2026-09-17-api-subscription-lifecycle.json)
retains full gate logs, source hashes, original failures and review evidence.
All 108 server library controls pass; independent source review passes.
Vendor sources are unchanged from PR #226; current CI still runs its separate
expiry regressions on Linux/macOS. Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `ec0ac417` and ten Linux volume/template controls passed with verified cleanup. [PR #228](https://github.com/tdenisenko/logex/pull/228) merged as `910a74ae`. The merge tree is identical to the tested head. B8-06–08 are closed. JSON-RPC/filter semantics, slow-consumer gaps, reorg notifications, dashboard ordering and integrated acceptance remain open.
