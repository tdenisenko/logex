# Code audit

This ledger tracks the audit requested in September 2026. A passing test suite
is a baseline, not evidence that the whole system has been audited. Production
data and running deployments are not modified by the benchmark suite.

## Batch ledger

| Batch | Review area | Status / acceptance |
| --- | --- | --- |
| 0 | Baseline, dependency inventory, CI, benchmarks | In progress; capture release results and merge infrastructure. |
| 1 | Shared types, checkpoints, CL proofs, EL validation, extraction | Pending; invalid input cannot advance trusted state or publish logs. |
| 2 | WAL, catalogs, segments, codecs, readers, durability | Pending; recover interrupted commits without hiding lost data. |
| 3 | CL RPC, gossip, discovery, scheduling, supervision | Pending; bounded requests and recovery across stale heads and committee transitions. |
| 4 | EL discovery, peer management, requests, serving cache | Pending; bounded accounting, correct response attribution and cancellation. |
| 5 | Live/historical sync, reorgs, ingestion, coverage | Pending; publish only verified contiguous data across crashes and reorgs. |
| 6 | Indexes, bloom filters, index publication | Pending; exact equivalence to an independent scan oracle, including damaged indexes. |
| 7 | Native and SQL queries, snapshots, pushdown, cancellation | Pending; exact filtering/order/pagination and safe concurrent file lifetimes. |
| 8 | HTTP, JSON-RPC, gRPC, WebSocket, authentication | Pending; consistent results/errors, bounded clients and correct cancellation ownership. |
| 9 | Dashboard, metrics, health, status | Pending; safe rendering, responsive controls, accurate unavailable/stale states. |
| 10 | CLI/runtime and volume supervision | Pending; preflight volume identity and writability, runtime loss detection, launchd/systemd templates. |
| 11 | Offline automatic segment repair | Pending; dry-run, quarantine, verified refetch, resumable publication, exclusive access. |
| 12 | Integrated regression and performance | Pending; repeated baselines, fault injection, platform validation and a 24-hour staging soak. |

Finish and merge each coherent task before starting the next. Record severity,
reproducer, validation, and measured tradeoffs in its PR. Critical findings can
change the order. The intentionally ignored root `ROADMAP.md` remains local.

## Architecture and contracts

The production dependency direction is:

```text
types <- storage <- index <- query <- server <- sync <- node
types <- cl <--------------------------------- sync <- node
```

Each crate also uses lower-level crates directly where required. In particular,
sync publishes subscriptions through server; changing that coupling is not a
baseline prerequisite. `PartitionManager` is the current public storage facade.

| Boundary | Sources of truth / compatibility obligations |
| --- | --- |
| Trust | Checkpoint/quorum handling in node; BLS/SSZ and fork handling in cl; header/body/receipt verification in sync. Verify the entire chain before publication. |
| Persistence | CL store and peer caches; storage catalog/manifests, WAL, column/page formats, indexes, canonical bitmaps, sync head and anchors. Readers must preserve existing directories. |
| Query | Shared log row schema, native filter, legacy LogSQL rewrites, DataFusion table provider, snapshot metadata and cancellation. |
| Wire | REST/JSON-RPC/WS serializers, `logex.proto`, shared status types, dashboard consumers. Preserve successful-query semantics and explicit pagination. |
| Runtime | Clap/TOML precedence, node lifecycle and task supervision, background indexing/compaction, filesystem identity and platform probes. |

## Baseline findings

- **B0-01 — benchmark validity (fixed by this batch):** the old synthetic fixture
  assigned different hashes to rows in one block and used global row offsets as
  block log indexes. Its assertions checked only nonempty results. Replace it
  with coherent deterministic fixtures, exact expected rows/counts/order, and
  storage/index/concurrency measurements. These remain synthetic storage rows,
  not cryptographically verified Ethereum blocks.
- **B0-02 — CI coverage (fixed by this batch):** CI omitted all-target Clippy,
  explicit doc tests, release linking, and macOS execution. Add those checks;
  keep the pinned toolchain and existing Linux job names.
- **B0-03 — dependency advisories (open):** the original lockfile has RustSec
  findings. See [dependency review](dependencies.md). Review reachability and
  dependency-compatible remediation before claiming production readiness.
- **B0-04 — compiler compatibility (open):** the pinned compiler reports future
  incompatibilities in discv5 0.10.4, proc-macro-error2 2.0.1, quinn 0.11.9, and
  quinn-udp 0.5.14. These are dependency warnings, not failed workspace Clippy.
  Keep the current pin until replacements pass protocol and platform tests.

## Decisions

- Correctness before speed. No silent row caps, reduced verification, wholesale
  rewrites, or new benchmark dependencies.
- Repair takes queries and normal ingestion offline; health/status stay
  available. Reuse authenticated EL fetching, keep quarantined originals, use a
  resumable journal, and stop when trusted anchors or replacement ranges cannot
  be established. Expose `repair --dry-run`, `repair`, and opt-in
  `sync --repair-corrupt-segments`.
- External-volume supervision targets macOS and Linux. Check stable expected
  identity before any directory creation and on restart. Service logs must not
  depend on the removable volume. Production installation is a separate step.
- Public formats stay compatible by default. Any unavoidable migration requires
  an explicit design decision and recovery/rollback instructions.

See [benchmark instructions](benchmarks.md) for commands and measurement limits.
