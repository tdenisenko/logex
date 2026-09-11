# Code audit

This ledger tracks the audit requested in September 2026. A passing test suite
is a baseline, not evidence that the whole system has been audited. Production
data and running deployments are not modified by the benchmark suite.

## Batch ledger

| Batch | Review area | Status / acceptance |
| --- | --- | --- |
| 0 | Baseline, dependency inventory, CI, benchmarks | Complete: PR #120 merged as `afe5939c`; all six CI jobs passed. Includes compacted-page ordering fix B0-05. |
| 0a | Compatible dependency security remediation | Complete: PR #121 merged as `bf4b97ab`; all six CI jobs passed. Seven vulnerability matches removed; remaining advisories and measured tradeoffs documented. |
| 1 | Shared types, checkpoints, CL proofs, EL validation, extraction | In progress: [header/checkpoint fixes](consensus-header-trust.md) merged in PR #122; [HTTP checkpoint resolution](checkpoint-resolution.md) in PR #123; [cached-state guards](consensus-cached-state.md) in PR #124; [receipt decoding/extraction](execution-receipt-validation.md) in PR #125. [Beacon SSZ/context fixes](beacon-ssz-validation.md) merged in PR #126 as `a83023cf`. [Extraction boundary checks](extraction-boundaries.md) merged in PR #127 as `f5fdd83c`, with all six CI jobs passing. [Execution header boundaries](execution-header-boundaries.md) merged in PR #128 as `11c1bec7` after all six CI jobs passed. Other trust paths remain pending. |
| 2 | WAL, catalogs, segments, codecs, readers, durability | In progress: [WAL recovery integrity](wal-recovery.md) merged in PR #129 as `09a63f55`. [Journaled commit recovery](storage-commit-replay.md) remains in draft [PR #130](https://github.com/tdenisenko/logex/pull/130): checkpoint correctness/CI and [isolated ExFAT tests](baselines/2026-09-11-exfat.md) pass. Initial implementations exceeded the user’s 10% limit; later combined sync storage comparisons meet it, with remaining space/read/index/startup costs still blocking acceptance. The [selected-read investigation](bundle-selected-reads.md) adds missing caller coverage and a measured lazy lookup; full lifecycle acceptance remains pending. The [compaction lifetime review](compaction-readers.md) confirms reader retirement failures, incorrect decoding after interrupted profile rewrites and overbroad cleanup; the captured/projected reader, immutable rewrite and sealed maintenance ownership fixes pass local gates, Linux/macOS CI, ARM/Intel ExFAT checks and paired performance confirmation. [Caller-level publication work](ingestion-publication.md) reproduced a row/progress restart gap on both the original baseline and candidate. Its regression now passes in the unmerged [combined sync checkpoint prototype](sync-ingestion-checkpoints.md), now testing catalog v9 / segment v7 / [bounded LZ4 bundle v5 records](bundle-table-codecs.md) over [grouped tables](bundle-table-groups.md) with [bundled live and historical columns](live-segment-bundles.md), compact RLP cached headers and deferred publication for wholly uncommitted segments. Fresh-sync deployment is conditional on performance/platform acceptance. Remaining formats and persisted state are pending. |
| 3 | CL RPC, gossip, discovery, scheduling, supervision | Pending; bounded requests and recovery across stale heads and committee transitions. |
| 4 | EL discovery, peer management, requests, serving cache | Pending; bounded accounting, correct response attribution and cancellation. |
| 5 | Live/historical sync, reorgs, ingestion, coverage | Pending; publish only verified contiguous data across crashes and reorgs. |
| 6 | Indexes, bloom filters, index publication | Pending; exact equivalence to an independent scan oracle, including damaged indexes. |
| 7 | Native and SQL queries, snapshots, pushdown, cancellation | Pending; exact filtering/order/pagination and safe concurrent file lifetimes. |
| 8 | HTTP, JSON-RPC, gRPC, WebSocket, authentication | Pending; consistent results/errors, bounded clients and correct cancellation ownership. |
| 9 | Dashboard, metrics, health, status | Pending; safe rendering, responsive controls, accurate unavailable/stale states. |
| 10 | CLI/runtime and volume supervision | Directory exclusivity prerequisite implemented with journal recovery; remaining work pending: preflight volume identity and writability, runtime loss detection, launchd/systemd templates. |
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
  [Compatible remediation](dependency-remediation.md) records subsequent fixes
  and remaining constraints; the initial report is retained as historical evidence.
- **B0-04 — compiler compatibility (open):** the pinned compiler reports future
  incompatibilities in discv5 0.10.4, proc-macro-error2 2.0.1, quinn 0.11.9, and
  quinn-udp 0.5.14. These are dependency warnings, not failed workspace Clippy.
  Keep the current pin until replacements pass protocol and platform tests.
- **B0-05 — compacted page selection (P1, fixed by this batch):** the full-size
  benchmark reproduced `requested row is before the current page range` on
  descending SQL across compressed pages. The page selector advanced only
  forward through an unsorted request. Sort output positions by physical row
  only for unordered requests, decode each selected page once, and scatter back
  into the original order. Preserve the existing ascending path without an
  extra ordering allocation. A focused regression failed before the fix;
  descending/shuffled/duplicate selections, full log columns, and a SQL query
  crossing the 16,384-row boundary are now covered in ordinary CI. This is a
  correctness fix required to finish the baseline, not a claimed speedup.

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

See [benchmark instructions](benchmarks.md) for commands and measurement limits,
and the [initial results](baselines/2026-09-07.md) for raw samples and validation.
