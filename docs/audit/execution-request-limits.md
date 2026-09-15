# Explicit execution request limits

This milestone makes the body and receipt APIs honor an explicitly requested
timeout and peer-selection limit before dispatching any requests.

## Finding B4-14

**Severity: medium, request liveness.** Both APIs previously tried the optional
parallel bulk scheduler before entering their limited sequential loop. That
scheduler uses its own default deadlines, chunk retries and salvage candidates;
it does not accept the caller's limits. With at least 64 hashes and multiple
dynamic chunks, a two-second request could still be waiting after two seconds,
and a two-peer limit could contact three peers before returning.

Separate body and receipt controls reproduce both failures against the actual
original public APIs. The tests inject typed local responses into session channels
and use a paused clock. Reth's public handle requires a TCP listener: the fixture
binds localhost on an assigned port, retains an unpolled network manager, and
disables discovery, NAT and background connection work. It uses no remote peers.

Normal anchored forward batches are capped at 32 blocks, or four while historical
work is active, below the bulk threshold. A larger checkpoint-gap fallback can
reach this defect: a 128-block paired probe can form one range and decline parallel
planning, while the full remaining gap forms several standalone bulk ranges.
This is a static reachable-path analysis, not a measured occurrence frequency.

## Correction and cost

Calls with either explicit limit use the existing limited peer-selection loop.
Default calls retain the same parallel scheduler, retries, salvage and payload
processing. This avoids redefining peer attempts as chunk attempts or adding a
second budget/accounting system to the bulk scheduler.

The explicit timeout covers one exchange, including local queue admission and
response waiting. The peer-selection limit has a minimum of one. Positive partial
responses can continue on the selected peer, and ETH70 can request continuation
pages; neither parameter is a whole-batch duration or wire-request bound.

Large explicitly limited fallback batches now select peers sequentially, as their
existing bounded path does for smaller requests. This can reduce concurrency for
that fallback, in exchange for actually enforcing its caller's liveness policy.
Ordinary forward batches and default bulk APIs keep their previous scheduling.
No storage writes, validation passes, dependency changes or per-row work are added.
No benchmark campaign was run under the user's updated performance policy.

## Validation and remaining scope

Eight tests independently exercise both roles: explicit two-second deadlines,
two-peer exhaustion, same-peer positive partial continuation, and default bulk
concurrent admission. Before the fix, both deadline and peer-cap controls fail;
both chosen sequential-continuation controls also fail because bulk dispatch
starts multiple chunks. Both default concurrency controls already pass. All
eight controls and all 358 sync tests pass after the fix (one ignored).

Final independent review and all seven local gates pass on source `9f921a9a`:
vendor verification, formatting, workspace check, strict Clippy, 1,462 workspace
tests (23 ignored), documentation tests and release node linking. All six CI jobs passed on head `1861d48a` (run `35015718354`); PR #179
merged as `231572a7` after fresh exact-head/base verification. See the
[validation record](baselines/2026-09-16-execution-request-limits.json).

Aggregate salvage/ETH70 continuation budgets remain separate review items. A
neighboring receipt API also collapses per-block supplier identity to one peer;
its consumer attribution needs a separate correction/review. This milestone does
not change that result shape. The broader offline audit, volume supervision,
verified offline repair and later live/staging acceptance remain incomplete.
Mac-mini testing and owned-artifact cleanup are complete; no remote work was
needed here.
