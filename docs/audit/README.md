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
| 3 | CL RPC, gossip, discovery, scheduling, supervision | In progress: [consensus snapshot durability](consensus-store-durability.md) addresses failed publication, restored structure and fatal-save supervision; merged in PR #154 (`d0a31b2a`) after all seven local gates (1,227 tests) and all six CI jobs passed. [Cached payload validation and serving](consensus-payload-cache.md) merged in PR #155 (`d67f9360`) after all seven local gates (1,249 tests) and six CI jobs passed, covering gossip context framing, restore checks and selected cache reads. [Request framing and ownership](consensus-request-lifecycle.md) fixes surplus frame output, type bounds, response correlation and canceled-request handling; merged in PR #157 (`aa624447`) after all seven local gates (1,280 tests, 23 ignored) and all six CI jobs passed. [Peer inventory retention](consensus-peer-retention.md) corrects uncounted inbound records, configured-family eligibility and repeated cache-selection parsing; merged in PR #158 (`c7ef4411`) after all seven local gates (1,286 tests, 23 ignored) and all six CI jobs passed. [Peer record freshness](consensus-peer-freshness.md) merged in PR #159 (`0d5104e6`) after all seven local gates (1,294 tests, 23 ignored) and all six CI jobs passed, correcting sequence ordering, withdrawal and cache/fork integration. [Gossip IDs, bounds and parameters](consensus-gossip-conformance.md) merged in PR #160 (`10d05727`) after all seven local gates (1,299 tests, 23 ignored) and all six CI jobs passed. [Gossip admission and forwarding](consensus-gossip-admission.md) merged in PR #161 (`b72c8a01`) after all seven local gates (1,318 tests, 23 ignored) and all six CI jobs passed, covering topic context, propagation timing, forwarding history, local committee availability and conditional cache persistence. [RPC participation processing](consensus-rpc-participation.md) merged in PR #162 (`6e1bb320`) after independent review, all seven local gates (1,327 workspace tests, 23 ignored) and all six CI jobs passed. It corrects stronger same-slot participation and range-summary/cache ranking. [Peer score arithmetic](consensus-peer-scoring.md) fixes address-score overflow and saturated lifecycle success increments; four before/after controls, independent review, all seven local gates (1,331 workspace tests, 23 ignored) and all six CI jobs passed before PR #164 merged as `3ba2bb0e`. [Peer-cache recovery](consensus-peer-cache-recovery.md) merged in PR #165 as `82c5dfce` after all seven local gates (1,340 tests, 23 ignored) and all six CI jobs passed. [Beacon body memory and serving](consensus-beacon-body-memory.md) implements a bounded shared body cache, range-hole correction, chunked output and exact decoded limits; independent review and all seven local gates pass (1,350 tests, 23 ignored), and all six CI jobs passed before PR #166 merged as `a4cb1085`. [Incoming response memory](consensus-incoming-memory.md) is implemented on `bb69d1b0`: shared payload reservations, incremental decoding, early actual-request counts and bounded adaptive historical retries. Three original-reader controls reproduce missing limits; 288 consensus tests and independent review pass. All seven local gates pass (1,366 workspace tests, 23 ignored); PR #167 merged as `16b69ec9` after all six CI jobs passed. [Serving memory and temporary availability](consensus-serving-memory.md) is implemented on `c10d4bef`, covering outgoing ownership, bounded diagnostics, finite remote rate-limit rotation and singleton output bounds. Six isolated original-path reproductions, 300 consensus tests and independent review pass; all seven local gates pass (1,378 workspace tests, 23 ignored). PR #168 merged as `a9126a61` after all six revised-head CI jobs passed; its initial passing source was revised after late scheduling review. [Typed decoding and retained-anchor work](consensus-decoding-retention.md) is implemented on `331c97fa`: streaming transaction roots, no-op range publication and sorted-anchor lookup/normalization. The finite typed-decoder overhead is documented; five controls, 305 consensus tests and independent reviews pass, all seven local gates pass (1,383 workspace tests, 23 ignored), followed by all six CI jobs. PR #169 merged as `2bf55c4a`. [Snapshot integrity](consensus-snapshot-integrity.md) is implemented in `6d7371a2`: framed checks before restore, explicit startup/info errors and preserved stale archives; all seven local gates pass (1,393 workspace tests, 23 ignored); all six CI jobs passed before PR #170 merged as `9a57c22c`. [History work](consensus-history-cost.md) removes repeated coverage scans, full bounded-batch copies, completeness-vector allocation and identical range-update saves in source `7ef16e28`. Focused tests/reviews and all seven local gates pass (1,397 workspace tests, 23 ignored); all six CI jobs passed before PR #171 merged as `ccb19d3b`. [Candidate metadata and backward recovery](consensus-metadata-retention.md) bounds unconnected entries while preserving authenticated ancestry and range-only recovery. Source `6772c7fa` passes independent review and all seven local gates (1,417 workspace tests, 23 ignored); all six CI jobs passed before PR #172 merged as `5df5b2f5`. [Discovery identity persistence](discovery-identity-persistence.md) consolidates bounded CL/EL startup reads and durable cooperative publication; source `b966f252` passes independent review and all seven local gates (1,427 workspace tests, 23 ignored); all six CI jobs passed before PR #173 merged as `558bef69`. Trusted-history lifetime remains open. [Supervisor ownership](consensus-supervisor-ownership.md) fixes detached children, outer monitoring and shutdown results; merged in PR #201 (`a3c57ac2`) after eight local gates (1,710 tests, 24 ignored) and six CI jobs. |
| 4 | EL discovery, peer management, requests, serving cache | In progress: [execution peer persistence](execution-peer-persistence.md) bounds startup cache reads/records, preserves damaged hints and isolates staging writes. Source `3dfd553a` passes independent review and all seven local gates (1,435 tests, 23 ignored); all six CI jobs passed before PR #174 merged as `556b988c`. The [live retry-hint milestone](execution-peer-retention.md) now bounds learned hints and prioritizes pending admission; independent review and all seven local gates pass on `999cac68` (1,440 tests, 23 ignored). All six CI jobs passed before PR #175 merged as `ae9da21e`. The [request-ownership milestone](execution-request-ownership.md) corrects overlapping reservation release and late plan/session effects. Source `36db2d3d` passes independent review and all seven local gates (1,445 tests, 23 ignored); all six CI jobs passed before PR #176 merged as `99128441`. The [response-attribution milestone](execution-response-attribution.md) removes unverified per-block receipt-count hints; all 344 sync tests, two original-helper controls and final review pass. All seven local gates pass on `4c60b745` (1,448 tests, 23 ignored); All six CI jobs passed before PR #177 merged as `7bc714c8`. The [request-deadline milestone](execution-request-deadlines.md) covers queue admission and response waiting; 350 sync tests, two actual original-helper controls and final review pass. All seven local gates pass on `a5180319` (1,454 tests, 23 ignored); All six CI jobs passed before PR #178 merged as `19f8c139`. [Explicit request limits](execution-request-limits.md) are corrected with eight public API controls and 358 passing sync tests; final review and all seven local gates pass on `9f921a9a` (1,462 workspace tests, 23 ignored). All six CI jobs passed before PR #179 merged as `231572a7`. [Per-block receipt sources](execution-receipt-sources.md) are preserved through standalone APIs and six consumers after an actual original-API reproduction. Independent review and all seven local gates pass on `35750c2d` (1,468 workspace tests, 23 ignored); All six CI jobs passed before PR #180 merged as `87692192`. [Cache canonicality and provider ranges](execution-cache-canonicality.md) are corrected after eight original-code regression failures; source `910f4a72` passes independent review and all seven local gates (1,483 tests, 23 ignored). All six CI jobs passed before PR #181 merged as `a56470af`. [Network payload accounting](network-payload-accounting.md) removes telemetry-only re-encoding/recompression and misleading wire units. Source `72c5a022` passes independent review and all seven local gates (1,489 tests, 23 ignored); All six CI jobs passed before PR #182 merged as `76ecd184`. [Prefix salvage deadlines](execution-salvage-deadlines.md) enforce the existing 12-second budget after six original regression failures; eight focused corrected controls and independent review pass. All seven local gates pass on `2894e2b5` (1,497 workspace tests, 23 ignored); All six CI jobs passed before PR #183 merged as `bc585616`. [Execution-cache payload admission](execution-cache-payload-budget.md) adds a documented 128 MiB logical budget and avoids copies for rejected optional payloads. Eight new controls and all 32 cache tests pass; independent final review and all seven local gates pass on `e9e83ef8` (1,505 workspace tests, 23 ignored). All six CI jobs passed before PR #184 merged as `d73fd32a`. General standalone continuation policy and outgoing/transient memory bounds remain open. |
| 5 | Live/historical sync, reorgs, ingestion, coverage | In progress; publish only verified contiguous data across crashes and reorgs. |
| 6 | Indexes, bloom filters, index publication | In progress: [numeric range boundary fixes](sql-predicates.md) cover reversed ranges and inclusive maximum keys. [Derived-file integrity](index-file-integrity.md) now covers B-tree/bitmap structure, bloom page checks, captured source-row counts and legacy publication rebuilds. Merged in PR #148 as `e317a529`, with all nine local gates and all six exact-head CI jobs passing. Retained original-baseline comparisons and fixed confirmations record gains and remaining tail limits. [Publication binding](index-publication-binding.md) closes individual artifact substitution in merged PR #149 (`9c0a58fc`), with all nine local gates and all six exact-head CI jobs passing. Complete measurement evidence and unresolved mixed query-tail observations are retained. [Source publication identity](source-publication-identity.md) closes confirmed whole-set copying, divergent copies and recovery origins in PR #150 (`8595e040`). Retained source `e76f4dda` uses bounded streaming commitments and overlaps historical hashing with existing workers; all nine local gates pass. The audit owner accepted the documented performance tradeoff and ended further benchmarking. All 30 final Intel/APFS/ExFAT controls and all six final-head CI jobs pass. The rest of this area remains pending. |
| 7 | Native and SQL queries, snapshots, pushdown, cancellation | In progress: [ordering/pagination fixes](query-pagination.md) have before-fix regressions and reference/DataFusion equivalence tests. [Snapshot consistency](query-snapshots.md) fixes captured row boundaries and explicit reorg invalidation. [SQL result values and projections](sql-result-values.md) merged in PR #135 as `a52d79e9`, with all six CI jobs passing and explicit performance acceptance. [Native filter/pushdown predicates](sql-predicates.md) have independent-oracle reproductions and corrections. [Custom aggregate evaluation](sql-aggregate-semantics.md) merged in PR #141 as `f45f2640`, with all six CI jobs passing and retained reference/performance evidence. [Identifier and alias binding](sql-identifier-binding.md) merged in PR #142 as `99beaa83`, with all six CI jobs passing and retained metadata performance investigation. [Metadata predicate semantics](sql-metadata-semantics.md) merged in PR #143 as `2d443187`, with all six CI jobs passing and retained reference/performance evidence. [Named-subquery scope](sql-query-scope.md) merged in PR #144 as `c78e519c`, with all six CI jobs passing and all positive measured latency changes below 1%. [Parsed syntax eligibility](sql-syntax-eligibility.md) merged in PR #145 as `c24d56b6`, with all six CI jobs passing and its complete tail-latency investigation retained. [Grouping and membership semantics](sql-grouping-aggregates.md) merged in PR #146 as `afc7c7dc`, with all six CI jobs passing and explicit performance measurement limits retained. [Storage allocation and aggregate batches](storage-resource-bounds.md) fixes data-only aggregate cancellation and repeated reads in merged PR #151 (`29376426`), with all local and Linux/macOS CI gates passing. Broader resource review remains pending. |
| 8 | HTTP, JSON-RPC, gRPC, WebSocket, authentication | In progress: [gRPC SQL lock lifetime](grpc-query-locks.md) merged in PR #136 after all six CI jobs passed. [REST cancellation ownership](query-cancellation.md) merged in PR #137 after all six CI jobs passed. [Read-only SQL execution](sql-read-only.md) merged in PR #139 after all six CI jobs passed. [SQL expression growth](sql-expression-limits.md) merged in PR #140 after all six CI jobs passed. Remaining protocol, resource and security review is pending. |
| 9 | Dashboard, metrics, health, status | Pending; safe rendering, responsive controls, accurate unavailable/stale states. |
| 10 | CLI/runtime and volume supervision | [Configuration precedence](config-precedence.md) fixes explicit option overrides and config diagnostics; 172 node tests pass, workspace gates/CI/merge pending. [Storage health probes](storage-health-probes.md) retain failed/timed-out checks and keep the supervisor responsive; merged in PR #202 (`ba4957f2`) after eight local gates (1,722 tests, 24 ignored) and six CI jobs. [Cleanup deadlines and results](runtime-cleanup-deadlines.md) cover ordinary shutdown through runtime destruction and preserve cleanup failures; merged in PR #200 (`0e94d900`) after eight local gates (1,700 tests, 24 ignored) and six CI jobs. [Node worker supervision](node-worker-supervision.md) fixes lost HTTP/gRPC and checkpoint errors and shutdown-unwind reporting; merged in PR #199 (`4915ad57`) after eight local gates (1,687 tests, 24 ignored) and six CI jobs. Directory exclusivity prerequisite implemented with journal recovery; remaining work pending: preflight volume identity and writability, runtime loss detection, launchd/systemd templates. |
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

The [standalone execution continuation review](execution-continuation-bounds.md)
records merged PR #185 (`1dac15e2`) for B4-22 elapsed limits
and the separate B4-23 resource correction below.

The [receipt resource review](execution-receipt-resource-bounds.md) records B4-23
mandatory header contexts and cumulative logical resource checks, merged in
PR #186 (`9317c42a`) after all local and CI gates.

The [execution progress review](execution-partial-progress.md) records B4-24–27:
useful partial-response accounting, exact tail failure context and receipt retry
cleanup, merged in PR #187 (`02a812e4`) after all local and CI gates.

The [execution serving review](execution-serving-contracts.md) records B4-28–32:
truthful empty-cache availability, coherent periodic range announcements and
progressing ETH70 receipt pagination. Merged in PR #188 (`e07fb451`) after all local and CI gates.

The [cache publication review](execution-cache-publication.md) records B4-33:
lazy availability lookup and suppression of unchanged submissions, preserving forced
head restoration. Merged in PR #189 (`e17bd1bf`) after the component comparison, local gates and CI.

The [execution worker lifecycle review](execution-task-lifecycle.md) records B4-34–36:
completed-stream retirement, owned worker cancellation and fatal task supervision.
Merged in PR #190 (`eb326cfb`) after 329 focused tests, eight local gates and six CI jobs.

The [execution endpoint review](execution-endpoint-selection.md) records B4-37–38:
usable TCP family selection and independent UDP discovery availability.
Merged in PR #191 (`e5eb60ac`) after 290 focused tests, eight local gates and six CI jobs.

The [session endpoint reporting review](execution-session-endpoints.md) records B4-39:
connection metrics and logs use the actual socket while retaining advertised retry hints.
Merged in PR #192 (`2e547fc9`) after 294 focused tests, eight local gates and six CI jobs.

The [peer rehabilitation review](execution-peer-rehabilitation.md) records B4-40–41:
retain longer active backoffs and admit restart hints after quarantine expiry.
Merged in PR #193 (`bedfed0d`) after 301 focused tests, eight local gates and six CI jobs.

The [execution bandwidth retention review](execution-bandwidth-retention.md) records B4-42:
bounded shared telemetry buckets with explicit recent-rate precision.
Merged in PR #194 (`840338b3`) after 355 focused P2P tests, eight local gates and six CI jobs.

The [execution retry history review](execution-backoff-retention.md) records B4-43–45:
bound disconnected history, preserve active deadlines and separate cooldowns from dial capacity.
Merged in PR #195 (`a03e2d51`) after 367 focused P2P tests, eight local gates and six CI jobs.

The [serving cancellation review](execution-serving-cancellation.md) records B4-46:
skip provider reads and response construction for receivers already closed in the queue.
Merged in PR #196 (`cc1bb889`) after 372 focused P2P tests, eight local gates and six CI jobs.

The [historical work cancellation review](historical-work-cancellation.md) records
B5-04–06: owned jobs, retained write results on stop and consistent shutdown
channels; merged in PR #204 (`8d7426aa`) after eight local gates (1,747 tests, 24 ignored) and six CI jobs.

The [historical memory probe review](historical-memory-probes.md) records B5-02/B5-03:
bounded host-reference ownership and nonoverlapping macOS page accounting;
merged in PR #203 (`fd57def9`) after eight local gates (1,731 tests, 24 ignored) and six CI jobs.

The [historical fetch supervision review](historical-fetch-supervision.md) records B5-01:
report workers that finish without a result while preserving queued successes and cancellation.
Merged in PR #197 (`f6f05acc`) after 552 focused sync tests, eight local gates and six CI jobs.

The [runtime failure-exit review](runtime-failure-exit.md) records B10-01/B10-02:
preserve failure exit status, bound cleanup after engine/disk failures and tolerate interrupted telemetry updates.
Merged in PR #198 (`a47ecbd8`) after 124 focused node tests, eight local gates and six CI jobs.

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
