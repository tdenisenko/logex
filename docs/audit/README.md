# Code audit

This ledger records the offline audit begun at PR #120 in September 2026. All
review areas have the offline dispositions below. Production data and running
deployments were not used as disposable audit fixtures. The closing PR records
its exact final source, merge gates and remaining limits; this ledger does not
declare a stable release. A minimum **48-hour live-sync acceptance test** follows
offline completion and must pass before a release-readiness claim or stable tag.

## Current offline dispositions

The later PRs supersede the open-work statements in historical milestones below.
An offline disposition means the relevant code, failure paths and practical
controls were reviewed, with confirmed defects corrected. It does not prove the
absence of all bugs, uniform throughput or physical-device failure behavior.

| Batch | Final offline disposition | Evidence |
| --- | --- | --- |
| 0 / 0a | Baseline, dependency inventory, toolchain and CI reviewed; compatible dependency fixes applied. Remaining advisories have explicit feature/call-path dispositions. | PRs #120–121; [fresh dependency recheck](dependency-remediation.md), including Rustls 0.23.45. |
| 1 | Checkpoint, consensus, execution and extraction trust boundaries reviewed and corrected; official fork fixtures retained. | PRs #122–128, #153 and #156; [fork conformance](consensus-fork-conformance.md). |
| 2 | Recovery, durability, persisted formats, bounded decoding and reader lifetimes reviewed and corrected. | PR #130 and [storage boundary disposition](storage-boundary-disposition.md), PR #225; [recorded storage acceptance](pr130-acceptance.md). |
| 3 | Consensus framing, scheduling, persistence and ownership reviewed; history journal, gossip retention and capacity policies implemented. | [History journal](consensus-history-journal.md); PRs #234–237, #242–246 and [#254](https://github.com/tdenisenko/logex/pull/254). |
| 4 | Execution requests, partial progress, retry ownership, serving, cache and peer lifecycles reviewed and corrected. | [Final execution response disposition](execution-response-lifecycle.md), PR #227. |
| 5 | Live/historical ingestion, selection, reorg and coverage publication reviewed and corrected. | [Sync selection integration](sync-selection-integration.md), PR #223; reorg delivery in PR #233. |
| 6 | Index structure, binding, lifecycle, bloom behavior and fallback reviewed and corrected. | [Index lifecycle](index-lifecycle-integration.md), PR #224; canonical bitmap integrity in [#241](https://github.com/tdenisenko/logex/pull/241). |
| 7 | Query semantics, pagination, snapshots, aggregates, cancellation and shared memory ownership reviewed and corrected. | Earlier query reports below; PRs #259–276, ending with [composed memory ownership](https://github.com/tdenisenko/logex/pull/276). |
| 8 | Cross-protocol filters, request correlation, response ownership, authentication, browser origins and subscriptions reviewed and corrected. | PRs #228–233, #259–268, #276 and [HTTP access](https://github.com/tdenisenko/logex/pull/277). |
| 9 | Dashboard, status, health and reconnect behavior reviewed and corrected; maintenance integration completed. | [Dashboard review](dashboard-observability.md), PR #238; repair startup in PR #257. |
| 10 | Runtime/configuration, worker failures, shutdown and expected-volume supervision reviewed and corrected. | [Runtime disposition](runtime-config-review.md), PR #219; [volume supervision](expected-volume-supervision.md), PRs #207–208; repair handoff in PR #257. |
| 11 | Inspection, classification, verified refetch, staging, journaled publication, quarantine and CLI/automatic repair implemented and reviewed. | PRs #239–240, #247–250, #252–253 and #255–258; [independent repair equivalence](https://github.com/tdenisenko/logex/pull/258). |
| 12 | Integrated fixture coverage, dependency follow-up and platform/deployment evidence reviewed. No additional implementation defect or necessary omnibus fixture was identified beyond the final TLS patch. | Integrated controls and accepted limits below; exact final validation in the closing PR. Live acceptance remains separate. |

## Integrated offline evidence and limits

The normal workspace suite already composes the important subsystem boundaries.
These controls use independent expected rows or explicit state/publication
invariants; they do not merely compare two wrappers around the same operation.

| Composition | Practical evidence |
| --- | --- |
| Live/historical writes, concurrent queries, indexing, compaction and reopen | [Audit harness](../../crates/logex-query/tests/audit_harness.rs): ordinary sparse/dense fixtures with exact result oracles, real query threads, page boundaries, append/rotation and historical restart. |
| Query capture versus append, reorg, index publication and compaction | [Snapshot controls](../../crates/logex-query/tests/snapshots.rs) and [pagination controls](../../crates/logex-query/tests/pagination.rs) check exact results or explicit invalidation at observed publication points. |
| Peer outcomes versus selected-chain and storage publication | [Selection integration](sync-selection-integration.md), historical fetch supervision and anchored cancellation controls cover obsolete work, partial outcomes, queued cancellation and started-write ownership. |
| Repair, unavailable history, retry and query equivalence | [Repair equivalence](../../crates/logex-sync/src/repair/execution/equivalence_tests.rs) and [retry controls](../../crates/logex-sync/src/repair/execution/retry_tests.rs) cover hot/sealed owners, overlaps, empty blocks, preserved originals, native-query oracles and repeated reopen. Test-owned trust anchors do not substitute for consensus-proof validation. |
| Interrupted publication and subsequent operation | [Repair publication controls](../../crates/logex-storage/src/native/repair/publication/tests.rs) interrupt observed durability boundaries, resume the journal, preserve quarantine and check appends after repair. |
| Storage failure, shutdown and repair-to-sync handoff | Node runtime/CLI controls and [expected-volume fixtures](expected-volume-supervision.md) exercise failed workers/probes, signal ownership, deadlines, missing/wrong volumes, remount and cleanup. They start no production sync. |

The same shared query budget covers admitted source/candidate/group/result and
response ownership. The [documented exclusions](../../README.md#storage-and-query-engine)
still apply to parser/manifest/control metadata, selected engine temporaries,
allocator effects and cooperative synchronous operations. It is not a process-RSS
ceiling. Subscription notifications are transient; reconnect/restart is not a
durable replay contract. Successful queries are never silently truncated to fit
capacity.

Retain the historical performance evidence without treating host variation as a
uniform acceptance result. In particular, grouping measured sparse historical
ingestion +13.70% combined (+6.12% confirmation), isolated dense reopen +15.84%
and small-list membership +5.01%. Decoding measured dense compaction p95 +6.02%
and concurrent-query p95 +5.79%, with fixed confirmations -0.24%/-1.04%.
Artifact binding measured sparse concurrent-query p95 +11.47% and ordered-query
p95 +5.93%; the concurrent confirmation was +7.55% and steady diagnostic -4.95%.
Conversion's mixed sparse concurrent observation was +6.04% versus +0.87% isolated
median; metadata pooled p95 was +6.30%; syntax count p95 was initially +5.39%,
then -1.11% confirmation and +2.39% combined. Earlier larger spikes and all samples
remain in their linked reports. Identical-executable controls show material host
variation but do not prove every tail harmless. The owner accepted the best
tested implementation and ended further timing campaigns absent a concrete
opportunity for substantial code improvement. No new end-to-end performance
comparison or live-throughput guarantee is claimed by this final review.

Current local and hosted macOS evidence is ARM64 macOS 26.6.2. The hosted
`macos-latest` job in [PR #277's CI](https://github.com/tdenisenko/logex/actions/runs/36068871294)
used image `macos-26-arm64`, version `20260907.0351.1`, and restored a dependency
cache. Linux CI runs workspace/vendor checks, release linking and ten disposable
ext4 volume/template controls. Earlier ARM/Intel ExFAT component evidence remains
scoped to the sources and fixtures in its reports. Historical native-C deployment
target warnings are not reproduced in the latest local release log, but cached
local/CI builds cannot certify clean native dependencies or an older macOS minimum.
Build and test on the intended deployment OS before distributing binaries for it.
Service templates are validated templates; production installation and the
48-hour live-sync acceptance test remain subsequent work.

## Live/backfill scheduling follow-up

The first native Intel/macOS acceptance attempt used audited merge `2e584917`.
It ran from 2026-09-25 10:47:01 UTC until a clean, requested stop at 11:33:54 UTC.
During initial catch-up, the execution head lag fell from 86 to 35 blocks,
increased to 60, and later recovered to five. Consensus stayed fresh; sampled
queries and storage health passed. These observations do not establish a
permanent stall, a data-integrity failure or an overall ingestion regression.

Review identified foreground historical network waits before eligible live
work, a four-block live cap during backfill, and repeated historical drains
between live turns. These scheduling choices predate the September audit. The
follow-up gives eligible live work its turn first, uses the existing 32-block
catch-up bound, and yields after one completed, bounded coalesced historical
write when more live work is pending. Pending fetch/prepare workers remain
owned and reusable; already-started writes still finish under existing shutdown
supervision. Live request deadlines, retry limits, validation and publication
checks are preserved. Unavailable live gaps still give history a turn.

Single-page and small-peer-pool historical header requests now use the existing
background request plan. They retain sequential peer selection, so this move
does not introduce duplicate concurrent header requests. Existing larger-page
parallelism remains unchanged. Empty committed header prefixes enter the ordered
prepare/write stream without fetching empty bodies or receipts. Refill retains
pending prepares and active writes at the terminal fetch boundary. A mixed page
may refetch its remaining headers after an empty prefix; its payload and floor
are never skipped or published out of order.

Finite local-channel controls reproduce the original scheduling failures and
exercise delayed history, new live anchors, empty blocks through genesis,
unavailable live gaps, retained workers, coalesced writes and peer fallback.
The 64-block catch-up fixture uses two live header exchanges while historical
replies are withheld, then finishes history after those replies are released.
This is evidence of request scheduling and operation counts, not a release
throughput benchmark. Existing cancellation, reorg, validation and storage
controls remain part of the workspace gates. The implementation PR records
the exact-source validation results.

The owner requires **one uninterrupted acceptance run of one final binary**.
When code changes are needed, pause monitoring, stop the owned client promptly
to avoid billed historical bandwidth, and remove only its owned test dataset
after verified exit. Finish validation and compilation before a fresh sync.
Resume the 30-minute monitor after initial analysis of the new run. The first
attempt's logs and provenance are retained, its approximately 13.1 GiB dataset
was removed, and none of its elapsed time or data counts toward acceptance.
An intentional restart is not part of the new acceptance window. Report full
sync separately, then continue through at least 48 uninterrupted issue-free
hours. A stable tag still waits for the owner's subsequent acceptance.

## Historical milestones

These reports retain their original source identities, failures, measurements
and then-open follow-ups. The current dispositions above identify their closure.

The earlier [consensus cache lifecycle review](consensus-cache-lifecycle.md) fixes
expired duplicate-ID reads and idle heartbeat retention (B3-68). Three original
controls fail before the correction and pass afterward; focused integration and
independent review pass. Source `1310497a` passes all twelve local gates
(1,962 workspace tests / 24 ignores); PR #226 merged as `595e9974` after all six CI jobs and ten Linux controls. Trusted-history
persistence and aggregate network admission remain separate open work.

The [execution response lifecycle review](execution-response-lifecycle.md) records
inbound/outgoing ownership dispositions and fixes unrepresentable peer-count
configuration before startup (B4-49). Source `eecd57d7` passes six focused controls,
strict sync Clippy, independent review and all eleven local gates (1,968 tests / 24 ignores); PR #227 merged as `50c1194f` after all six CI jobs and ten Linux controls.

The [live subscription ownership milestone](api-subscription-lifecycle.md) merged in
PR #228 as `910a74ae`: four original-code controls
reproduce named ephemeral lifetime, replaced-session detach and retained history
ordering/eviction (B8-06–08). Final source `dbd5b48a` passes 108 server tests,
independent review and eleven local gates (1,973 tests / 24 ignores). Other API protocol/filter/notification findings are
recorded for sequential fixes. All eleven local gates, six CI jobs and ten Linux
controls passed; this is not completion of batch 8.

The [single-request JSON-RPC admission review](jsonrpc-admission.md) fixes
request envelopes/notifications, argument errors before storage and exact numeric
ID correlation (B8-09–11). PR #229 merged as `61f2506b` after all six CI jobs and ten Linux controls.
Batch execution and shared resource policy remain separate.

The [Ethereum filter consistency review](ethereum-filter-semantics.md) addresses
literal decoding, required wildcard topic positions and cross-protocol bounds
(B8-12–14). Six expanded original-code groups reproduce the failures. Source `76667e69` passes an independent oracle across hot/sealed and indexed/
unindexed storage, focused checks and all eleven local gates (2,000 workspace
tests / 24 existing ignores). PR #230 merged as `a4eafd46` after all six CI jobs and ten Linux controls.

The [offline repair inspection milestone](offline-repair-inspection.md) adds
exclusive nonmutating primary-data inspection beside normal startup recovery.
Source `9d454ff9` passes independent review and all twelve local gates
(2,103 Rust tests / 24 existing ignores). PR #239 merged as `b141f1a0`
after all six CI jobs and ten Linux controls. Authenticated fetching, complete-block replacement, quarantine,
CLI/status integration and automatic repair remain open in batch 11.

The [finite repair fetching milestone](verified-repair-fetch.md) adds an explicit
caller-anchored descending block cursor with separate complete-range evidence,
bounded work and terminal cancellation. Source `cbae4942` passes eleven focused
cursor controls, three transport controls, independent review and all twelve
local gates (2,117 Rust tests / 24 existing ignores). Exact-head CI and merge
remain. Replacement planning, staging/quarantine/publication and CLI/status/runtime
integration remain open in batch 11.

## Batch ledger

The [WebSocket delivery review](websocket-delivery.md) makes detected broadcast
gaps terminal and releases copied acknowledgement history before socket awaits
(B8-15–16). Original raw/retained loopback controls reproduce silent continuation;
source `71ef3cc0` passes 149 focused server tests, five browser callback controls,
independent review and all eleven local gates (2,007 tests / 24 existing ignores).
PR #231 merged as `334ec9ef` after all six CI jobs and ten Linux controls.

The [ERC20 subscription review](erc20-subscription-inputs.md) addresses literal
field types, missing amount digits and exact Transfer topic shape (B8-17–19).
Three finite original-source groups reproduce the defects. Source `cdb17543`
passes 158 focused server tests, independent review and all eleven local gates
(2,016 tests / 24 existing ignores). Stored chain logs and ingestion are unchanged.
PR #232 merged as `c85df998` after all six CI jobs and ten Linux controls.

The [canonical reorg subscription review](reorg-subscription-delivery.md) closes
missing removal publication and retained-history reconciliation (B8-20–21).
The approved implementation delivers removals after committed retirement and
before replacement additions, with owned blocking work and source lifetime
checks. Source `eaee66f8` passes focused and independent review plus all eleven
local gates (2,031 tests / 24 existing ignores).
PR #233 merged as `22fcd71d` after all six CI jobs and ten Linux controls.

The [consensus history journal review](consensus-history-journal.md) addresses
full-history copying and rewriting on ordinary trusted-state changes (B3-69).
Source `1b751e5a` uses typed deltas, a checksummed committed frontier and periodic
checkpoints while retaining required anchors and serving payloads. Focused
consensus and node checks pass, alongside all eleven local gates (2,055 tests /
24 existing ignores). PR #234 merged as `115ac554` after all six CI jobs and ten Linux controls.
This does not close aggregate networking admission or batch 3.

The [consensus network admission review](consensus-network-admission.md) fixes
silently reduced large configured peer limits (B3-70). Checked startup arithmetic
preserves ordinary and zero semantics and rejects unrepresentable values before
consensus identity/transport setup. Source `2cc8bba7` passes independent review
and all eleven local gates (2,058 tests / 24 existing ignores). PR #235
merged as `66512764` after all six CI jobs and ten Linux controls. Protected metadata and gossip cache/topic/control ownership are
recorded as separate open findings; this change adds no ingestion work.

The [gossip topic ownership review](gossip-topic-ownership.md) closes unsupported
topic retention and late type-size admission (B3-71–72). Static CL eligibility is
enforced before message caches, GRAFT peer topics and PRUNE backoffs; supported
fork lifecycle and message IDs are unchanged. Source `9778b322` passes independent
review, 162 standalone vendor tests and all eleven local gates (2,060 workspace
tests / 24 existing ignores). PR #236 merged as `59f5722c` after all six CI jobs and ten Linux controls. Eligible-cache capacity, priority controls and
protected metadata are separate open ownership work.

| Batch | Review area | Status / acceptance |
| --- | --- | --- |
| 0 | Baseline, dependency inventory, CI, benchmarks | Complete: PR #120 merged as `afe5939c`; all six CI jobs passed. Includes compacted-page ordering fix B0-05. |
| 0a | Compatible dependency security remediation | Complete: PR #121 merged as `bf4b97ab`; all six CI jobs passed. Seven vulnerability matches removed; remaining advisories and measured tradeoffs documented. |
| 1 | Shared types, checkpoints, CL proofs, EL validation, extraction | Offline verification review complete: [header/checkpoint fixes](consensus-header-trust.md) merged in PR #122; [HTTP checkpoint resolution](checkpoint-resolution.md) in PR #123; [cached-state guards](consensus-cached-state.md) in PR #124; [receipt decoding/extraction](execution-receipt-validation.md) in PR #125. [Beacon SSZ/context fixes](beacon-ssz-validation.md) merged in PR #126 as `a83023cf`. [Extraction boundary checks](extraction-boundaries.md) merged in PR #127 as `f5fdd83c`, with all six CI jobs passing. [Execution header boundaries](execution-header-boundaries.md) merged in PR #128 as `11c1bec7` after all six CI jobs passed. [Remaining trust-path boundaries](trust-path-boundaries.md) merged in PR #153 (`037e54ad`), with all seven local gates and six CI checks passing, covering historical job shape, repeated beacon hashing and independent domain/commitment controls. [Fork conformance](consensus-fork-conformance.md) resolves historical digest, genesis finality and absent-committee proof leads, verifies the current mainnet schedule and adds 13 official SSZ fixtures; merged in PR #156 (`e79e2d23`) after all seven local gates (1,257 tests) and all six CI jobs passed. Stored-state integrity and network ownership remain in their respective batches. |
| 2 | WAL, catalogs, segments, codecs, readers, durability | Offline structural review complete through [storage boundary disposition](storage-boundary-disposition.md), PR #225 (`6fc297c9`). Earlier milestones: [WAL integrity](wal-recovery.md) merged in PR #129 as `09a63f55`. [PR #130 acceptance](pr130-acceptance.md) records the journaled replay and joint block/progress publication milestone, immutable bundles, bounded coalescing, reader lifetime, parser fixes and independent index publication. Final source `4408e070` passes all six local/CI gates, startup and production-sync performance acceptance, and exact-source ARM/Intel ExFAT with 136 cross-mount cases each. The linked PR records final-tip CI and merge. [Variable page and dictionary decoding](storage-decode-bounds.md) merged in PR #147 as `ae4c01c9`, with all six exact-head CI jobs passing and retained tail-latency investigation. [Storage allocation and aggregate batches](storage-resource-bounds.md) merged in PR #151 (`29376426`) after all nine local gates and all six CI jobs passed. It corrects invalid append layouts, decoder allocation and selected payload scans. [Legacy column readers](legacy-column-readers.md) merged in PR #152 (`733a248a`) after all eight local gates and all six CI jobs passed, removing duplicate row assembly and validating raw row-count reads. [Historical range bounds](historical-range-bounds.md) correct accepted unordered input metadata and resulting native filter/pagination errors; source `75a73e4d` passes ten local gates (1,940 tests / 24 ignores). All six CI jobs and ten Linux controls passed before PR #221 merged as `e54e9303`. [Remaining storage boundary disposition](storage-boundary-disposition.md) fixes plain scalar prevalidation copies (B2-28) and removes an unused result enum. Source `a7eae4b9` passes independent review and all ten local gates (1,962 tests / 24 ignores); all six CI jobs and ten Linux controls passed before PR #225 merged as `6fc297c9`. Shared query resource policy and automatic repair remain separate work; this does not approve production deployment. |
| 3 | CL RPC, gossip, discovery, scheduling, supervision | In progress: [consensus snapshot durability](consensus-store-durability.md) addresses failed publication, restored structure and fatal-save supervision; merged in PR #154 (`d0a31b2a`) after all seven local gates (1,227 tests) and all six CI jobs passed. [Cached payload validation and serving](consensus-payload-cache.md) merged in PR #155 (`d67f9360`) after all seven local gates (1,249 tests) and six CI jobs passed, covering gossip context framing, restore checks and selected cache reads. [Request framing and ownership](consensus-request-lifecycle.md) fixes surplus frame output, type bounds, response correlation and canceled-request handling; merged in PR #157 (`aa624447`) after all seven local gates (1,280 tests, 23 ignored) and all six CI jobs passed. [Peer inventory retention](consensus-peer-retention.md) corrects uncounted inbound records, configured-family eligibility and repeated cache-selection parsing; merged in PR #158 (`c7ef4411`) after all seven local gates (1,286 tests, 23 ignored) and all six CI jobs passed. [Peer record freshness](consensus-peer-freshness.md) merged in PR #159 (`0d5104e6`) after all seven local gates (1,294 tests, 23 ignored) and all six CI jobs passed, correcting sequence ordering, withdrawal and cache/fork integration. [Gossip IDs, bounds and parameters](consensus-gossip-conformance.md) merged in PR #160 (`10d05727`) after all seven local gates (1,299 tests, 23 ignored) and all six CI jobs passed. [Gossip admission and forwarding](consensus-gossip-admission.md) merged in PR #161 (`b72c8a01`) after all seven local gates (1,318 tests, 23 ignored) and all six CI jobs passed, covering topic context, propagation timing, forwarding history, local committee availability and conditional cache persistence. [RPC participation processing](consensus-rpc-participation.md) merged in PR #162 (`6e1bb320`) after independent review, all seven local gates (1,327 workspace tests, 23 ignored) and all six CI jobs passed. It corrects stronger same-slot participation and range-summary/cache ranking. [Peer score arithmetic](consensus-peer-scoring.md) fixes address-score overflow and saturated lifecycle success increments; four before/after controls, independent review, all seven local gates (1,331 workspace tests, 23 ignored) and all six CI jobs passed before PR #164 merged as `3ba2bb0e`. [Peer-cache recovery](consensus-peer-cache-recovery.md) merged in PR #165 as `82c5dfce` after all seven local gates (1,340 tests, 23 ignored) and all six CI jobs passed. [Beacon body memory and serving](consensus-beacon-body-memory.md) implements a bounded shared body cache, range-hole correction, chunked output and exact decoded limits; independent review and all seven local gates pass (1,350 tests, 23 ignored), and all six CI jobs passed before PR #166 merged as `a4cb1085`. [Incoming response memory](consensus-incoming-memory.md) is implemented on `bb69d1b0`: shared payload reservations, incremental decoding, early actual-request counts and bounded adaptive historical retries. Three original-reader controls reproduce missing limits; 288 consensus tests and independent review pass. All seven local gates pass (1,366 workspace tests, 23 ignored); PR #167 merged as `16b69ec9` after all six CI jobs passed. [Serving memory and temporary availability](consensus-serving-memory.md) is implemented on `c10d4bef`, covering outgoing ownership, bounded diagnostics, finite remote rate-limit rotation and singleton output bounds. Six isolated original-path reproductions, 300 consensus tests and independent review pass; all seven local gates pass (1,378 workspace tests, 23 ignored). PR #168 merged as `a9126a61` after all six revised-head CI jobs passed; its initial passing source was revised after late scheduling review. [Typed decoding and retained-anchor work](consensus-decoding-retention.md) is implemented on `331c97fa`: streaming transaction roots, no-op range publication and sorted-anchor lookup/normalization. The finite typed-decoder overhead is documented; five controls, 305 consensus tests and independent reviews pass, all seven local gates pass (1,383 workspace tests, 23 ignored), followed by all six CI jobs. PR #169 merged as `2bf55c4a`. [Snapshot integrity](consensus-snapshot-integrity.md) is implemented in `6d7371a2`: framed checks before restore, explicit startup/info errors and preserved stale archives; all seven local gates pass (1,393 workspace tests, 23 ignored); all six CI jobs passed before PR #170 merged as `9a57c22c`. [History work](consensus-history-cost.md) removes repeated coverage scans, full bounded-batch copies, completeness-vector allocation and identical range-update saves in source `7ef16e28`. Focused tests/reviews and all seven local gates pass (1,397 workspace tests, 23 ignored); all six CI jobs passed before PR #171 merged as `ccb19d3b`. [Candidate metadata and backward recovery](consensus-metadata-retention.md) bounds unconnected entries while preserving authenticated ancestry and range-only recovery. Source `6772c7fa` passes independent review and all seven local gates (1,417 workspace tests, 23 ignored); all six CI jobs passed before PR #172 merged as `5df5b2f5`. [Discovery identity persistence](discovery-identity-persistence.md) consolidates bounded CL/EL startup reads and durable cooperative publication; source `b966f252` passes independent review and all seven local gates (1,427 workspace tests, 23 ignored); all six CI jobs passed before PR #173 merged as `558bef69`. Trusted-history lifetime remains open. [Supervisor ownership](consensus-supervisor-ownership.md) fixes detached children, outer monitoring and shutdown results; merged in PR #201 (`a3c57ac2`) after eight local gates (1,710 tests, 24 ignored) and six CI jobs. [Shared IPv6 ports](enr-ipv6-endpoints.md) corrects signed endpoint interpretation in LogEx and pinned discovery dependencies (B3-67/B4-47/B4-48); all nine final-source local gates pass (1,844 tests / 24 ignored); all six CI jobs passed before PR #211 merged as `65844f06`. |
| 4 | EL discovery, peer management, requests, serving cache | Offline review complete: [execution peer persistence](execution-peer-persistence.md) bounds startup cache reads/records, preserves damaged hints and isolates staging writes. Source `3dfd553a` passes independent review and all seven local gates (1,435 tests, 23 ignored); all six CI jobs passed before PR #174 merged as `556b988c`. The [live retry-hint milestone](execution-peer-retention.md) now bounds learned hints and prioritizes pending admission; independent review and all seven local gates pass on `999cac68` (1,440 tests, 23 ignored). All six CI jobs passed before PR #175 merged as `ae9da21e`. The [request-ownership milestone](execution-request-ownership.md) corrects overlapping reservation release and late plan/session effects. Source `36db2d3d` passes independent review and all seven local gates (1,445 tests, 23 ignored); all six CI jobs passed before PR #176 merged as `99128441`. The [response-attribution milestone](execution-response-attribution.md) removes unverified per-block receipt-count hints; all 344 sync tests, two original-helper controls and final review pass. All seven local gates pass on `4c60b745` (1,448 tests, 23 ignored); All six CI jobs passed before PR #177 merged as `7bc714c8`. The [request-deadline milestone](execution-request-deadlines.md) covers queue admission and response waiting; 350 sync tests, two actual original-helper controls and final review pass. All seven local gates pass on `a5180319` (1,454 tests, 23 ignored); All six CI jobs passed before PR #178 merged as `19f8c139`. [Explicit request limits](execution-request-limits.md) are corrected with eight public API controls and 358 passing sync tests; final review and all seven local gates pass on `9f921a9a` (1,462 workspace tests, 23 ignored). All six CI jobs passed before PR #179 merged as `231572a7`. [Per-block receipt sources](execution-receipt-sources.md) are preserved through standalone APIs and six consumers after an actual original-API reproduction. Independent review and all seven local gates pass on `35750c2d` (1,468 workspace tests, 23 ignored); All six CI jobs passed before PR #180 merged as `87692192`. [Cache canonicality and provider ranges](execution-cache-canonicality.md) are corrected after eight original-code regression failures; source `910f4a72` passes independent review and all seven local gates (1,483 tests, 23 ignored). All six CI jobs passed before PR #181 merged as `a56470af`. [Network payload accounting](network-payload-accounting.md) removes telemetry-only re-encoding/recompression and misleading wire units. Source `72c5a022` passes independent review and all seven local gates (1,489 tests, 23 ignored); All six CI jobs passed before PR #182 merged as `76ecd184`. [Prefix salvage deadlines](execution-salvage-deadlines.md) enforce the existing 12-second budget after six original regression failures; eight focused corrected controls and independent review pass. All seven local gates pass on `2894e2b5` (1,497 workspace tests, 23 ignored); All six CI jobs passed before PR #183 merged as `bc585616`. [Execution-cache payload admission](execution-cache-payload-budget.md) adds a documented 128 MiB logical budget and avoids copies for rejected optional payloads. Eight new controls and all 32 cache tests pass; independent final review and all seven local gates pass on `e9e83ef8` (1,505 workspace tests, 23 ignored). All six CI jobs passed before PR #184 merged as `d73fd32a`. The [final response ownership review](execution-response-lifecycle.md) completes standalone continuation, outgoing/transient ownership, retained state and implementation-cost disposition; B4-49 numeric startup admission is corrected in PR #227. This supersedes earlier open lifetime notes. Separate limits do not establish a global RSS guarantee; integrated mixed workloads remain in batch 12. [Shared IPv6 ports](enr-ipv6-endpoints.md) corrects signed endpoint interpretation in LogEx and pinned discovery dependencies (B3-67/B4-47/B4-48); all nine final-source local gates pass (1,844 tests / 24 ignored); all six CI jobs passed before PR #211 merged as `65844f06`. |
| 5 | Live/historical sync, reorgs, ingestion, coverage | Offline code review complete through [selection integration](sync-selection-integration.md), PR #223 (`1a34273c`), after all local and CI gates. Integrated workloads, live sync and staging acceptance remain separate. |
| 6 | Indexes, bloom filters, index publication | Offline review complete through [lifecycle integration](index-lifecycle-integration.md), PR #224 (`2b86b8a6`). Earlier milestones: [numeric range boundary fixes](sql-predicates.md) cover reversed ranges and inclusive maximum keys. [Derived-file integrity](index-file-integrity.md) now covers B-tree/bitmap structure, bloom page checks, captured source-row counts and legacy publication rebuilds. Merged in PR #148 as `e317a529`, with all nine local gates and all six exact-head CI jobs passing. Retained original-baseline comparisons and fixed confirmations record gains and remaining tail limits. [Publication binding](index-publication-binding.md) closes individual artifact substitution in merged PR #149 (`9c0a58fc`), with all nine local gates and all six exact-head CI jobs passing. Complete measurement evidence and unresolved mixed query-tail observations are retained. [Source publication identity](source-publication-identity.md) closes confirmed whole-set copying, divergent copies and recovery origins in PR #150 (`8595e040`). Retained source `e76f4dda` uses bounded streaming commitments and overlaps historical hashing with existing workers; all nine local gates pass. The audit owner accepted the documented performance tradeoff and ended further benchmarking. All 30 final Intel/APFS/ExFAT controls and all six final-head CI jobs pass. Integrated acceptance and shared resource/repair work remain separate. |
| 7 | Native and SQL queries, snapshots, pushdown, cancellation | In progress: [ordering/pagination fixes](query-pagination.md) have before-fix regressions and reference/DataFusion equivalence tests. [Snapshot consistency](query-snapshots.md) fixes captured row boundaries and explicit reorg invalidation. [SQL result values and projections](sql-result-values.md) merged in PR #135 as `a52d79e9`, with all six CI jobs passing and explicit performance acceptance. [Native filter/pushdown predicates](sql-predicates.md) have independent-oracle reproductions and corrections. [Custom aggregate evaluation](sql-aggregate-semantics.md) merged in PR #141 as `f45f2640`, with all six CI jobs passing and retained reference/performance evidence. [Identifier and alias binding](sql-identifier-binding.md) merged in PR #142 as `99beaa83`, with all six CI jobs passing and retained metadata performance investigation. [Metadata predicate semantics](sql-metadata-semantics.md) merged in PR #143 as `2d443187`, with all six CI jobs passing and retained reference/performance evidence. [Named-subquery scope](sql-query-scope.md) merged in PR #144 as `c78e519c`, with all six CI jobs passing and all positive measured latency changes below 1%. [Parsed syntax eligibility](sql-syntax-eligibility.md) merged in PR #145 as `c24d56b6`, with all six CI jobs passing and its complete tail-latency investigation retained. [Grouping and membership semantics](sql-grouping-aggregates.md) merged in PR #146 as `afc7c7dc`, with all six CI jobs passing and explicit performance measurement limits retained. [Storage allocation and aggregate batches](storage-resource-bounds.md) fixes data-only aggregate cancellation and repeated reads in merged PR #151 (`29376426`), with all local and Linux/macOS CI gates passing. [Scalar expression review](sql-scalar-expressions.md) reproduces remaining NULL, domain and evaluation defects (B7-26–29); all nine local gates pass on `6d766f98` (1,855 tests / 24 ignored); all six CI jobs passed before PR #213 merged as `8c881b94`. Broader resource review remains pending. |
| 8 | HTTP, JSON-RPC, gRPC, WebSocket, authentication | In progress: [gRPC SQL lock lifetime](grpc-query-locks.md) merged in PR #136 after all six CI jobs passed. [REST cancellation ownership](query-cancellation.md) merged in PR #137 after all six CI jobs passed. [Read-only SQL execution](sql-read-only.md) merged in PR #139 after all six CI jobs passed. [SQL expression growth](sql-expression-limits.md) merged in PR #140 after all six CI jobs passed. Remaining protocol, resource and security review is pending. |
| 9 | Dashboard, metrics, health, status | Offline review complete: [dashboard/status fixes](dashboard-observability.md), PR #238 (`77d03349`), close B9-01–13 after all twelve local gates, six CI jobs and ten Linux controls. 2,074 Rust tests / 24 existing ignores and 36 browser controls pass; native browser layout/keyboard checks pass. Repair-state integration remains in batch 11. |
| 10 | CLI/runtime and volume supervision | [Sync-mode persistence](sync-mode-persistence.md) makes startup policy publication durable and rejects unavailable storage/unexpected markers; merged in PR #206 (`0b611c2d`) after eight local gates (1,781 tests, 24 ignored) and six CI jobs. [Configuration precedence](config-precedence.md) fixes explicit option overrides and config diagnostics; merged in PR #205 (`c63d9c27`) after eight local gates (1,765 tests, 24 ignored) and six CI jobs. [Storage health probes](storage-health-probes.md) retain failed/timed-out checks and keep the supervisor responsive; merged in PR #202 (`ba4957f2`) after eight local gates (1,722 tests, 24 ignored) and six CI jobs. [Cleanup deadlines and results](runtime-cleanup-deadlines.md) cover ordinary shutdown through runtime destruction and preserve cleanup failures; merged in PR #200 (`0e94d900`) after eight local gates (1,700 tests, 24 ignored) and six CI jobs. [Node worker supervision](node-worker-supervision.md) fixes lost HTTP/gRPC and checkpoint errors and shutdown-unwind reporting; merged in PR #199 (`4915ad57`) after eight local gates (1,687 tests, 24 ignored) and six CI jobs. Directory exclusivity prerequisite implemented with journal recovery; [Expected-volume supervision](expected-volume-supervision.md) implements identity preflight, anchored paths, failure admission and service templates; merged in PR #207 (`43b4f182`) after eight local gates (1,829 tests / 24 ignored), six CI jobs and real ExFAT/Linux lifecycle controls. [Independent ordinary-storage monitoring](independent-storage-health.md) corrects the remaining same-poll health delay; original regression, all 203 node tests and eight local gates (1,828 workspace tests / 24 ignored) pass; all six CI jobs passed before PR #208 merged as `17b044f9`. [Background index inspection](indexer-storage-locks.md) releases the ingestion guard before freshness checks and moves all index I/O to blocking workers; original regression and all eight final-source local gates pass (1,832 tests / 24 ignored); all six CI jobs passed before PR #209 merged as `d6e71722`. Remaining runtime review continues separately. [Compaction inspection](compaction-plan-locks.md) moves manifest/backlog I/O outside bounded storage guards and corrects unfinished-ingestion counts (B10-21/B10-22); original regression and all eight final-source local gates pass (1,836 tests / 24 ignored); all six CI jobs passed before PR #210 merged as `6d0c272e`. [Blocking maintenance workers](maintenance-worker-supervision.md) propagates compaction/index join failures into node supervision (B10-23); all nine final-source local gates pass (1,848 tests / 24 ignored); all six CI jobs passed before PR #212 merged as `4b0b1437`. |
| 11 | Offline automatic segment repair | Pending; dry-run, quarantine, verified refetch, resumable publication, exclusive access. |
| 12 | Integrated regression and performance | Historical scope; completed offline disposition and retained limits are recorded above. Further performance experiments require a concrete implementation opportunity. The minimum 48-hour live-sync acceptance test follows offline completion. |

The [query scan execution correction](query-scan-execution.md) addresses B7-30,
missing recursive-query iterations. All nine local gates pass on `86c7e6f8`
(1,861 tests / 24 ignored); six CI jobs passed before PR #214
merged as `657c99f4`.

The [aggregate result-contract review](query-aggregate-contracts.md) checks the
mixed exact/numeric output contract and fixes typed aggregate name collisions
(B7-31). All nine local gates pass on `c95ed325` (1,870 tests / 24 ignored), plus
500 isolated planner tests. Six CI jobs passed before PR #215 merged as `8fd2e55f`. Its ordinary-integer
SUM overflow finding (B7-32) is covered by the following scoped correction.

The [ordinary sum review](query-sum-overflow.md) addresses the confirmed fixed-width
and decimal-precision failures plus distinct-window null/state defects (B7-32–35).
Source `52ce3bfd` passes 33 focused tests, 99 isolated package tests and all ten
workspace gates (1,875 tests / 24 ignored). Six CI jobs passed before PR #216 merged as `929a3639`. Its checked-subtotal contract
preserves existing result types and explicitly reports overflow, including
transient window states. Decimal-average helper behavior remains separate.

The [average arithmetic and window review](query-decimal-average.md) has bounded public
reproductions for wrapped decimal means, incorrect empty average windows and negative
scale factors (B7-36–38), a direct grouped-memory undercount (B7-39), and
duration subtotal overflow (B7-40).
Source `51fed125` passes 44 focused application tests, 108 isolated package tests
and all ten local gates (1,886 workspace tests / 24 ignored). Six CI jobs passed before PR #217 merged as `40d247e6`.

The [native API worker review](native-query-workers.md) addresses synchronous
filesystem scans and retained ingestion read locks in JSON-RPC/gRPC log methods
(B8-05). Source `f2293fd4` passes 111 server/protocol controls, alongside 115 query
controls with two existing ignores. All ten local gates pass (1,902 workspace
tests / 24 ignored). Six CI jobs passed before PR #218 merged as `e76fd8cc`.
This is a native execution/lifetime correction; shared query memory and request
admission remain open.

The [remaining runtime review](runtime-config-review.md) corrects `info` ownership
and signal-listener failure handling (B10-24/B10-25). Source `aaa8786d` passes
233 node/CLI/example controls and all ten workspace gates. Six CI jobs and ten Linux controls passed before PR #219 merged as `43bb6bc0`; the remaining batch-10 offline code review is closed.

The [sync-state review](sync-state-review.md) corrects consensus admission,
partial progress, whole-reorg recovery, sparse/selected-head decisions and bundled
maintenance ownership (B5-07–10/B5-12–14). Source `9869104d` passes all ten local
gates (1,930 tests / 24 ignores), with six effective original regression
patches, repeated recovery checks and selected-lineage controls. The head check
reuses the existing verified cache to avoid repeated beacon-root hashing.
All six CI jobs and ten Linux controls passed before PR #220 merged as `4635cbda`. Remaining batch-5 work stays open.

## Offline completion boundary

Each review area now has the current disposition above: reviewed without changes
where sufficient, or corrected with practical regression evidence. Historical
follow-ups in the milestone narrative below are closed by those later PRs.
The intentionally local roadmap records final validation and publication status.
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
starting an actual live sync. Live-sync acceptance and the minimum 48-hour
live-sync test are subsequent release gates. Synthetic fixtures do not substitute
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
fs <------------------------------------- cl, sync, node
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
The same report records B4-50: retaining contiguous reverse-header prefixes after
a short page while accounting for completed suffix responses, found during live
acceptance and covered by finite offline controls.

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

## Baseline findings and final disposition

- **B0-01 — benchmark validity (fixed by this batch):** the old synthetic fixture
  assigned different hashes to rows in one block and used global row offsets as
  block log indexes. Its assertions checked only nonempty results. Replace it
  with coherent deterministic fixtures, exact expected rows/counts/order, and
  storage/index/concurrency measurements. These remain synthetic storage rows,
  not cryptographically verified Ethereum blocks.
- **B0-02 — CI coverage (fixed by this batch):** CI omitted all-target Clippy,
  explicit doc tests, release linking, and macOS execution. Those checks are now
  included, with the pinned toolchain and existing Linux job names retained.
- **B0-03 — dependency advisories (scoped disposition complete):** the original lockfile has RustSec
  findings. See [initial dependency review](dependencies.md). The final
  reachability review and compatible remediation are recorded separately; this
  does not imply all lockfile advisory matches are removed.
  [Compatible remediation](dependency-remediation.md) records subsequent fixes
  and remaining constraints; the initial report is retained as historical evidence.
- **B0-04 — compiler compatibility (accepted pinned-toolchain limit):** the pinned compiler reports future
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

- Historical finding ID `B7-26` occurs in two reports: PR #151's storage-resource
  report names aggregate payload batching, while PR #213's scalar report names
  math NULL simplification. Cite the report/PR alongside that ID; retained
  original evidence is not renumbered. New findings continue after B7-30.

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

The [checkpoint-gap memory review](checkpoint-gap-memory.md) addresses B5-11's
whole-gap header/hash retention with bounded authenticated temporary storage.
Source `b327cf90` passes ten local gates (1,951 tests / 24 existing ignores).
All six CI jobs and ten Linux controls passed before PR #222 merged as `bdee0df8`. Other sync integration items remain open.

The [sync selection integration review](sync-selection-integration.md) corrects
obsolete forward and rewind publication after storage waits (B5-15/B5-16).
Source `b61e6363` passes two final source reviews and ten local gates
(1,960 tests / 24 ignores). All six CI jobs and ten Linux controls passed before PR #223 merged as `1a34273c`. The accompanying
historical disposition covers ancestry, ordered outcomes and empty/genesis
progress; broader offline audit and integrated acceptance stay open.

The [index lifecycle integration review](index-lifecycle-integration.md) fixes
the maximum-prefix boundary in two public composite helpers (B6-05). Source
`8c47fa8e` has two original failing regressions and passes their fixed controls, 82 index tests and independent
review. All ten local gates pass (1,961 tests / 24 ignores); all six CI jobs and ten Linux controls passed before PR #224 merged as `2b86b8a6`. Reader, builder and
storage-publication dispositions found no additional scoped defect requiring
a change; integrated workloads and broader memory/repair work remain separate.

The [consensus metadata lifetime correction](consensus-metadata-lifetime.md)
addresses B3-73, permanently protected metadata from obsolete authenticated forks.
Source `ab618bf5` passes nine metadata controls, 405 consensus tests / one existing
ignore and strict Clippy. Independent ownership/store reviews and all eleven local gates pass
(2,072 workspace tests / 24 existing ignores). PR #237 merged as `93c160c3` after all six CI jobs and ten Linux controls.
Required trusted history remains retained.
