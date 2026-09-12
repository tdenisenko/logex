# REST query cancellation ownership

Base: `73443007` (merged PR #136). This pass covers the shared REST SQL query
admission/cancellation controller. Successful query semantics, request limits,
storage formats and ingestion remain unchanged. Other protocols' admission and
runtime blocking work remain in the wider audit.

## Confirmed finding

**B8-02 — P2, acknowledged cancellation can be lost across request transitions.**
`start` published an active ID and then cleared a separate global cancellation
flag. A cancellation between those operations returned true but was erased.
`finish` similarly published an idle slot before clearing the flag; a new query
could start and be canceled before the previous guard erased its cancellation.
A long-running query could therefore continue after the operator requested stop.

Two-thread tests invoke the actual controller methods, with barriers delimiting
20,000 start/cancel or finish/start/cancel rounds. They do not patch atomics,
insert production scheduling hooks, sleep or use elapsed-time assertions. After
cancellation returns true and before that query is dropped, its cancellation
must remain observable. The [first baseline run](baselines/2026-09-12-query-cancellation-before.log)
lost one start-time cancellation. The [expanded baseline run](baselines/2026-09-12-query-cancellation-before-expanded.log)
lost 36 cancellations during the previous query's completion; the start race
did not trigger in that run. Race exposure depends on scheduling; the invariant
and the observed failures do not depend on any timing threshold.

## Correction and invariants

Each admitted request owns a new `Arc<AtomicBool>`. A short standard mutex
serializes admission, cancellation and guard drop. At most one non-cloneable
guard owns the slot. Cancel marks that request's token; drop permanently marks
it and releases admission. The next query receives a different token, so retained
callbacks/tasks cannot revive or affect a later query. Cancel remains idempotent
and does not free admission until the request finishes. Idle cancel returns false.

The query's cancellation callback reads one atomic without a mutex, replacing
reads of the global active ID and flag. No mutex is held during SQL, I/O or await.
Critical sections only handle owned pointers/atomics, with no user callbacks;
poison recovery preserves a valid optional slot. One token allocation is added
per admitted request. Busy requests do not allocate a token. The numeric ID
counter and its eventual overflow/sentinel ambiguity are removed entirely.

Cancellation remains cooperative: this fix makes the request persist; it does
not promise that blocked operating-system I/O stops instantly. Query cancellation
frequency, result materialization and bounded shutdown retain their separate
resource/liveness audit. Single-active REST SQL admission and its conflict
response are unchanged; no new cross-protocol restriction is introduced.

## Validation and performance

Six added tests cover both transition races, permanent independent tokens,
concurrent single-owner admission, pending REST cancellation/fresh-query recovery
and dropping a pending REST request. The handler tests poll a two-partition
DataFusion aggregate on a current-thread runtime to suspend real execution
without sleeps, verify busy/canceled responses, then verify a fresh exact result.
All 89 server unit tests pass.

The existing direct-handler release fixture now also measures REST, sharing the
same coherent 20,000-row dataset and independent complete response oracle with
the gRPC fixture. Three rotating shapes cover native count, 1,000 ordered full
rows and a DataFusion aggregate. REST timing includes admission, SQL, response
serialization and guard drop; response parsing/assertions are outside timing.
Five alternating process pairs provide 250 samples per shape/revision. Build
identical fixtures from the baseline and candidate, require fresh compiler
artifacts/distinct executable hashes and retain all raw metadata and samples.

```bash
cargo test -p logex-server --test query_latency --release --locked \
  benchmark_rest_query_latency -- --ignored --nocapture --test-threads=1
```

All six local workspace gates pass: formatting, check, Clippy, 987 tests
(13 ignored), documentation tests and the release node build. Release-mode
regressions also pass (89 server tests). [Performance acceptance](baselines/2026-09-12-query-cancellation.md)
retains all 250 samples per shape/revision: median latency −1.15% to +0.02%,
p95 −4.89% to −1.47%; peak RSS median +0.15%, p95 −1.35%. The
per-request token allocation is included; no material regression was measured.
Implementation `d08c69e8` awaits PR/CI/merge. The wider offline audit and subsequent live-sync/staging acceptance are
not complete. No production data, external volumes or services are accessed.
