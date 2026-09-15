# Code audit

This ledger tracks the audit requested in September 2026. A passing test suite
is a baseline, not evidence that the whole system has been audited. Production
data and running deployments are not modified by the benchmark suite.

## Batch ledger

| Batch | Review area | Status / acceptance |
| --- | --- | --- |
| 0 | Baseline, dependency inventory, CI, benchmarks | Complete: PR #120 merged as `afe5939c`; all six CI jobs passed. Includes compacted-page ordering fix B0-05. |
| 0a | Compatible dependency security remediation | Complete: PR #121 merged as `bf4b97ab`; all six CI jobs passed. Seven vulnerability matches removed; remaining advisories and measured tradeoffs documented. |
| 1 | Shared types, checkpoints, CL proofs, EL validation, extraction | Offline verification review complete: [header/checkpoint fixes](consensus-header-trust.md) merged in PR #122; [HTTP checkpoint resolution](checkpoint-resolution.md) in PR #123; [cached-state guards](consensus-cached-state.md) in PR #124; [receipt decoding/extraction](execution-receipt-validation.md) in PR #125. [Beacon SSZ/context fixes](beacon-ssz-validation.md) merged in PR #126 as `a83023cf`. [Extraction boundary checks](extraction-boundaries.md) merged in PR #127 as `f5fdd83c`, with all six CI jobs passing. [Execution header boundaries](execution-header-boundaries.md) merged in PR #128 as `11c1bec7` after all six CI jobs passed. [Remaining trust-path boundaries](trust-path-boundaries.md) merged in PR #153 (`037e54ad`), with all seven local gates and six CI checks passing, covering historical job shape, repeated beacon hashing and independent domain/commitment controls. [Fork conformance](consensus-fork-conformance.md) resolves historical digest, genesis finality and absent-committee proof leads, verifies the current mainnet schedule and adds 13 official SSZ fixtures; merged in PR #156 (`e79e2d23`) after all seven local gates (1,257 tests) and all six CI jobs passed. Stored-state integrity and network ownership remain in their respective batches. |
| 2 | WAL, catalogs, segments, codecs, readers, durability | In progress: [WAL integrity](wal-recovery.md) merged in PR #129 as `09a63f55`. [PR #130 acceptance](pr130-acceptance.md) records the journaled replay and joint block/progress publication milestone, immutable bundles, bounded coalescing, reader lifetime, parser fixes and independent index publication. Final source `4408e070` passes all six local/CI gates, startup and production-sync performance acceptance, and exact-source ARM/Intel ExFAT with 136 cross-mount cases each. The linked PR records final-tip CI and merge. [Variable page and dictionary decoding](storage-decode-bounds.md) merged in PR #147 as `ae4c01c9`, with all six exact-head CI jobs passing and retained tail-latency investigation. [Storage allocation and aggregate batches](storage-resource-bounds.md) merged in PR #151 (`29376426`) after all nine local gates and all six CI jobs passed. It corrects invalid append layouts, decoder allocation and selected payload scans. [Legacy column readers](legacy-column-readers.md) merged in PR #152 (`733a248a`) after all eight local gates and all six CI jobs passed, removing duplicate row assembly and validating raw row-count reads. Remaining storage/parser boundaries require further review; this milestone does not approve production deployment. |
| 3 | CL RPC, gossip, discovery, scheduling, supervision | In progress: [consensus snapshot durability](consensus-store-durability.md) addresses failed publication, restored structure and fatal-save supervision; merged in PR #154 (`d0a31b2a`) after all seven local gates (1,227 tests) and all six CI jobs passed. [Cached payload validation and serving](consensus-payload-cache.md) merged in PR #155 (`d67f9360`) after all seven local gates (1,249 tests) and six CI jobs passed, covering gossip context framing, restore checks and selected cache reads. [Request framing and ownership](consensus-request-lifecycle.md) fixes surplus frame output, type bounds, response correlation and canceled-request handling; merged in PR #157 (`aa624447`) after all seven local gates (1,280 tests, 23 ignored) and all six CI jobs passed. [Peer inventory retention](consensus-peer-retention.md) corrects uncounted inbound records, configured-family eligibility and repeated cache-selection parsing; merged in PR #158 (`c7ef4411`) after all seven local gates (1,286 tests, 23 ignored) and all six CI jobs passed. [Peer record freshness](consensus-peer-freshness.md) merged in PR #159 (`0d5104e6`) after all seven local gates (1,294 tests, 23 ignored) and all six CI jobs passed, correcting sequence ordering, withdrawal and cache/fork integration. [Gossip IDs, bounds and parameters](consensus-gossip-conformance.md) merged in PR #160 (`10d05727`) after all seven local gates (1,299 tests, 23 ignored) and all six CI jobs passed. [Gossip admission and forwarding](consensus-gossip-admission.md) merged in PR #161 (`b72c8a01`) after all seven local gates (1,318 tests, 23 ignored) and all six CI jobs passed, covering topic context, propagation timing, forwarding history, local committee availability and conditional cache persistence. [RPC participation processing](consensus-rpc-participation.md) merged in PR #162 (`6e1bb320`) after independent review, all seven local gates (1,327 workspace tests, 23 ignored) and all six CI jobs passed. It corrects stronger same-slot participation and range-summary/cache ranking. Aggregate response memory and remaining recovery review remain open. |
| 4 | EL discovery, peer management, requests, serving cache | Pending; bounded accounting, correct response attribution and cancellation. |
| 5 | Live/historical sync, reorgs, ingestion, coverage | Pending; publish only verified contiguous data across crashes and reorgs. |
| 6 | Indexes, bloom filters, index publication | In progress: [numeric range boundary fixes](sql-predicates.md) cover reversed ranges and inclusive maximum keys. [Derived-file integrity](index-file-integrity.md) now covers B-tree/bitmap structure, bloom page checks, captured source-row counts and legacy publication rebuilds. Merged in PR #148 as `e317a529`, with all nine local gates and all six exact-head CI jobs passing. Retained original-baseline comparisons and fixed confirmations record gains and remaining tail limits. [Publication binding](index-publication-binding.md) closes individual artifact substitution in merged PR #149 (`9c0a58fc`), with all nine local gates and all six exact-head CI jobs passing. Complete measurement evidence and unresolved mixed query-tail observations are retained. [Source publication identity](source-publication-identity.md) closes confirmed whole-set copying, divergent copies and recovery origins in PR #150 (`8595e040`). Retained source `e76f4dda` uses bounded streaming commitments and overlaps historical hashing with existing workers; all nine local gates pass. The audit owner accepted the documented performance tradeoff and ended further benchmarking. All 30 final Intel/APFS/ExFAT controls and all six final-head CI jobs pass. The rest of this area remains pending. |
| 7 | Native and SQL queries, snapshots, pushdown, cancellation | In progress: [ordering/pagination fixes](query-pagination.md) have before-fix regressions and reference/DataFusion equivalence tests. [Snapshot consistency](query-snapshots.md) fixes captured row boundaries and explicit reorg invalidation. [SQL result values and projections](sql-result-values.md) merged in PR #135 as `a52d79e9`, with all six CI jobs passing and explicit performance acceptance. [Native filter/pushdown predicates](sql-predicates.md) have independent-oracle reproductions and corrections. [Custom aggregate evaluation](sql-aggregate-semantics.md) merged in PR #141 as `f45f2640`, with all six CI jobs passing and retained reference/performance evidence. [Identifier and alias binding](sql-identifier-binding.md) merged in PR #142 as `99beaa83`, with all six CI jobs passing and retained metadata performance investigation. [Metadata predicate semantics](sql-metadata-semantics.md) merged in PR #143 as `2d443187`, with all six CI jobs passing and retained reference/performance evidence. [Named-subquery scope](sql-query-scope.md) merged in PR #144 as `c78e519c`, with all six CI jobs passing and all positive measured latency changes below 1%. [Parsed syntax eligibility](sql-syntax-eligibility.md) merged in PR #145 as `c24d56b6`, with all six CI jobs passing and its complete tail-latency investigation retained. [Grouping and membership semantics](sql-grouping-aggregates.md) merged in PR #146 as `afc7c7dc`, with all six CI jobs passing and explicit performance measurement limits retained. [Storage allocation and aggregate batches](storage-resource-bounds.md) fixes data-only aggregate cancellation and repeated reads in merged PR #151 (`29376426`), with all local and Linux/macOS CI gates passing. Broader resource review remains pending. |
| 8 | HTTP, JSON-RPC, gRPC, WebSocket, authentication | In progress: [gRPC SQL lock lifetime](grpc-query-locks.md) merged in PR #136 after all six CI jobs passed. [REST cancellation ownership](query-cancellation.md) merged in PR #137 after all six CI jobs passed. [Read-only SQL execution](sql-read-only.md) merged in PR #139 after all six CI jobs passed. [SQL expression growth](sql-expression-limits.md) merged in PR #140 after all six CI jobs passed. Remaining protocol, resource and security review is pending. |
| 9 | Dashboard, metrics, health, status | Pending; safe rendering, responsive controls, accurate unavailable/stale states. |
| 10 | CLI/runtime and volume supervision | Directory exclusivity prerequisite implemented with journal recovery; remaining work pending: preflight volume identity and writability, runtime loss detection, launchd/systemd templates. |
| 11 | Offline automatic segment repair | Pending; dry-run, quarantine, verified refetch, resumable publication, exclusive access. |
| 12 | Integrated regression and performance | Pending; deterministic mixed workloads, failure injection and platform validation. Further performance experiments require a concrete implementation opportunity. Live sync and the 24-hour staging soak follow offline completion. |

## Offline completion boundary

The remaining work is partitioned into the review areas above and outcome-based
items in the local roadmap. Continue each area through a recorded disposition:
verified without changes, fixed with regression evidence, or explicitly blocked
with the remaining condition. A merged fix does not close the rest of its area.
The [page row-count fix](page-row-count.md) merged in PR #131 as `003df476`
after all six CI gates passed. [Segment-reader integrity](segment-read-integrity.md)
merged in PR #132 as `7ebc3795`, also with all six CI jobs passing. [Query pagination](query-pagination.md) merged in PR #133 as `6ffc1201` after all
six CI jobs passed. [Snapshot consistency](query-snapshots.md) merged in PR #134 as `4be5250c` after
all six CI jobs passed. [SQL result values](sql-result-values.md) merged in PR #135 as `a52d79e9` after
all six CI jobs passed. [gRPC SQL lock lifetime](grpc-query-locks.md) merged in PR #136 as `73443007` after
all six CI jobs passed. [REST cancellation ownership](query-cancellation.md) merged in PR #137 as `f19f47c4` after
all six CI jobs passed. PR #138 merged the [native filters and predicate pushdown](sql-predicates.md)
corrections as `13bd33cc`, with all six CI jobs passing. PR #139 merged the
[read-only SQL execution boundaries](sql-read-only.md) as `452d0874` after all
six CI jobs passed. PR #140 merged [SQL expression growth limits](sql-expression-limits.md)
as `84a7889d` after all six CI jobs passed. PR #141 merged [custom aggregate predicates, nulls and integer semantics](sql-aggregate-semantics.md) as `f45f2640` after all six CI jobs passed. PR #142 merged [identifier and alias binding](sql-identifier-binding.md) as `99beaa83` after all six CI jobs passed. PR #143 merged [metadata predicate semantics](sql-metadata-semantics.md) as `2d443187` after all six CI jobs passed. PR #144 merged [named-subquery scope](sql-query-scope.md) as `c78e519c` after all six CI jobs passed. PR #145 merged [parsed syntax eligibility](sql-syntax-eligibility.md) as `c24d56b6` after all six CI jobs passed. PR #146 merged [grouping and membership semantics](sql-grouping-aggregates.md) as `afc7c7dc` after all six CI jobs passed. PR #147 merged [storage page decoding and metadata bounds](storage-decode-bounds.md) as `ae4c01c9` after all six CI jobs passed. PR #148 merged [derived-index file integrity](index-file-integrity.md) as `e317a529` after all six CI jobs passed. PR #149 merged index artifact binding as `9c0a58fc`; PR #150 merged logical source-prefix binding as `8595e040`, each after all six CI jobs passed. PR #151 merged storage allocation and aggregate batch corrections as `29376426` after all six CI jobs passed. PR #152 merged legacy column-reader corrections as `733a248a` after all six CI jobs passed. PR #153 merged trust-path boundaries and independent controls as `037e54ad` after all six CI jobs passed. PR #154 merged consensus-store durability and structural reopen checks as `d0a31b2a` after all six CI jobs passed. PR #155 merged cached-response validation and serving as `d67f9360`. PR #156 merged fork conformance as `e79e2d23`. PR #157 merged consensus request lifecycle and framing as `aa624447`. PR #158 merged peer retention as `c7ef4411`; PR #159 merged peer freshness as `0d5104e6`; PR #160 merged gossip conformance as `10d05727`; PR #161 merged gossip admission as `b72c8a01`. PR #162 merged [RPC participation processing](consensus-rpc-participation.md) as `6e1bb320`. The [artifact cleanup record](artifact-cleanup.md) documents removal of obsolete local and Mac mini test/build artifacts while retaining audit evidence.

Complete deterministic offline validation and merge implementation PRs before
starting an actual live sync. Live-sync acceptance and the minimum 24-hour
staging soak are subsequent release gates. Synthetic fixtures do not substitute
for those gates; this ledger must not imply release readiness before they pass.

Finish and merge each coherent task before starting the next. Record severity,
reproducer, validation, and measured tradeoffs in its PR. Critical findings can
change the order. The intentionally ignored root `ROADMAP.md` remains local.

The audit owner has ended the extended source-identity benchmark campaign and
selected its best tested implementation, accepting modest performance costs for
necessary correctness and integrity fixes. Further performance experiments need
a concrete implementation opportunity with a potentially substantial benefit.
Earlier inconclusive and adverse measurements remain preserved with their original
dispositions; this decision does not turn them into passing statistical results.
Correctness tests, platform checks, CI and the later live/staging gates still apply.

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
- The audit owner waived backward compatibility and migration requirements.
  Formats and legacy helpers may change when correctness or maintainability
  benefits justify it. Existing user data remains protected; tests use isolated
  fixtures, and any fresh production sync is a later deployment step.

See [benchmark instructions](benchmarks.md) for commands and measurement limits,
and the [initial results](baselines/2026-09-07.md) for raw samples and validation.
