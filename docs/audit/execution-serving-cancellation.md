# Execution serving cancellation

## Finding B4-46

**Low, avoidable work after session closure.** The pinned request handler dequeued
header, body and ETH68/69/70 receipt requests without checking whether the response
receiver was still open. If a session closed while a request waited in the handler
queue, the handler still read and cloned cache entries, measured response lengths
and, for ETH68 receipts, computed blooms before dropping the undeliverable result.
There is no invalid-data acceptance or measured production outage claim.

Five local controls against unchanged production at `a03e2d51` reproduce a cache
read for each already-cancelled response. They use the provider's cumulative
normalized-payload counter as a witness that the actual provider path ran; this
counter is not a wire-delivery measurement. Small structural typed fixtures are
sufficient, with no large workloads, sockets or service tasks.

## Change and invariants

Each of the five existing typed handlers checks `response.is_closed()` after
incrementing its received-request counter and before accessing the provider. A
closed receiver ends that request immediately. Open receivers use the existing
handler unchanged: same data, lookup/response limits, ordering, receipt bloom
rules and ETH70 pagination. The ignored node-data variant performs no provider
work and needs no additional guard.

The check handles both dropping and explicitly closing a oneshot receiver while
the request is queued. Later live requests remain usable. It is a best-effort
entry check: closure racing after the check can still leave the current synchronous
request running. This does not cancel already queued wire requests, interrupt a
provider operation or promise immediate termination of all synchronous work.

The change adds one cheap channel-state read per served request and avoids the
entire provider/response-building path for an already-closed receiver. It adds no
per-block ingestion work, disk I/O, queue, timer or public configuration. No
wall-clock benchmark, ingestion speedup or memory-exhaustion result is claimed.
The deterministic control verifies eliminated work, not its percentage of node
runtime. Broader benchmarking remains discontinued at the user's request.

## Resource review boundaries

The cache's 128 MiB normalized-payload limit bounds retained optional cache data,
not all outgoing copies. Provider trait calls return owned vectors/blocks; those
copies can outlive cache eviction. The handler input channel has 256 slots in the
pinned builder. Active sessions apply backpressure to outstanding remote requests
and queued responses; these are separate counts and cannot be treated as one
five-response or 10 MiB process budget. The pinned P2P stream has a two-message
outgoing capacity and a 16 MiB payload limit, but ETH response encoding occurs
before the lower stream's size check. Existing 2 MiB handler targets remain soft
so complete blocks/receipts can exceed them.

These source observations establish existing backpressure, not a verified strict
RSS bound or a reason to change successful response semantics. Full transient
allocation/engine-admission disposition remains open. This fix only removes work
for receivers already known to be closed, within the existing bounded queues.

## Validation and cleanup

All five original controls fail at the cancelled-request lookup assertion. Each
final test exercises a dropped receiver, an open receiver, an explicitly closed
receiver and another open receiver, checking exact live response contents. All
**372 focused P2P tests pass**, with one ignored component workload. Existing
large-receipt pagination, peer lifetime, retry, cache and range controls also pass.

Only `reth-network/src/eth_requests.rs` changes production behavior. The same Reth
v1.11.3 commit, manifest, dependency features and Cargo lock remain. Updated the
recorded patch and its source/diff checksums; vendor verification checks all 156
upstream files across three packages. Applying the full patch to the exact pinned
upstream files reproduces all six reviewed modified/added files byte for byte.
Blank context lines were normalized in the patch artifact to avoid trailing
whitespace; the application check verifies this formatting remains valid.

Implementer review traced all request variants, provider reads, response ownership
and channel closure behavior. No independent review or live-peer test is claimed.
The existing handler fixture is reused; its retained provider reference is test
only. No provider helper became obsolete, and no unrelated production code was
removed. Mac mini and external-volume contents were not accessed.

Full workspace gates and PR/CI/merge are pending.

[Validation record](baselines/2026-09-16-execution-serving-cancellation.json).
