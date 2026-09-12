# gRPC SQL query lock lifetime

Base: `a52d79e9` (merged PR #135). This pass covers the asynchronous gRPC SQL
handler's storage lock. Other RPC/native-filter blocking work, query admission,
cancellation ownership and protocol/security behavior remain in the wider audit.

## Confirmed finding

**B8-01 — P1, SQL clients stall ingestion through a retained read lock.** The gRPC
SQL handler acquired the shared storage read guard and retained it while awaiting
SQL execution and building the response. A slow query therefore prevented the
writer from acquiring storage for ingestion or a reorg. REST already captured a
bounded optimistic snapshot and released its guard before execution.

The [before-fix regression](baselines/2026-09-12-grpc-query-locks-before.log)
constructs two physical input partitions and polls a real DataFusion aggregate
once on a current-thread runtime. Its input tasks cannot finish before that poll
yields. `try_write()` then fails with the old handler, proving lock retention
without elapsed-time thresholds or sleeps.

## Contract and correction

Validate the requested page, capture row boundaries, validity token and head under
the existing read guard, then release it before invoking the existing snapshot
SQL entry point. This performs the same snapshot capture as the old query helper,
without retaining the guard for the rest of the request. No new writer lock,
extra snapshot copy, storage format, ingestion write or request cap is introduced.

Later appends are excluded from the captured result, including new partitions
and the `latest` bound. A reorg invalidates an old query with the existing gRPC
Aborted status; a fresh query sees the new canonical rows. Dropping one query does
not invalidate another query or retain writer access. These are the previously
established query snapshot semantics, now used by the gRPC SQL handler.

Remove the fallback that manufactured a successful JSON error row if serialization
failed. Keep the existing serde serializer and preallocated response vector; any
serialization error propagates as an internal RPC error instead of partial success.

## Validation

Three controlled regressions cover ingestion while a query is pending, reorg
invalidation/fresh-query recovery, and dropping one of two pending clients. The
append test commits a third block while the query is suspended and verifies that
its result still contains the original two rows/head. Existing gRPC tests pass.
A direct gRPC-handler release fixture uses 20,000 coherent rows in indexed,
compacted 8,192-row segments. It rotates native COUNT, a 1,000-row full projection
and a DataFusion aggregate, checking response counts and every JSON field against
an independent oracle outside timing. Each process warms the paths, then measures
50 requests per shape. The identical fixture is used in both revisions.

```bash
cargo test -p logex-server --test query_latency --release --locked \
  benchmark_grpc_query_latency -- --ignored --nocapture --test-threads=1
```

Five alternating process pairs provide 250 samples per shape/revision. Record
source/executable hashes, toolchain, hardware, filesystem, cache conditions,
median/p95 and peak process memory. This measures handler execution and response
serialization, excluding network transport and live peers. The controlled tests
prove writer availability while a query is still pending; a latency comparison
alone cannot establish that guarantee. All six local gates pass: 981 tests (12 intentionally ignored), documentation
tests and the release node build. Release-mode lock regressions and repeated
handler comparisons remain before PR/CI/merge.

This milestone does not complete the remaining offline audit or establish
live-sync/release readiness. All fixtures use temporary directories; production
files, external volumes and services are untouched.
