# Ethereum filter decoding and predicate consistency

Base: PR #229 merge `61f2506b32031f2e9c9bb895077513b3e7f4b1ad`.
Branch: `audit/ethereum-filter-semantics`.
Exact-base controls reproduce array-filter acceptance, changed unknown-extension
meaning, a missing-topic wildcard match and a named-tag alias. Additional
protocol/field tables now run every case and reproduce four HTTP/gRPC groups
and two raw-WebSocket groups. Implementation and eleven local gates are complete; PR #230 merged as `a4eafd46` after all six CI jobs.

## Scope and invariants

- B8-12 (P2): preserve literal nested filter input and require an object
  filter. Strictly parse address and every topic-OR member; do not silently drop
  wrong-type members or reinterpret a library-private object as a scalar. Keep
  ordinary unknown-field policy distinct from invalid field value types.
- B8-13 (P2): align address/null/empty/topic wildcard meaning across native,
  JSON-RPC, gRPC and raw WebSocket paths. Preserve the number of requested topic
  positions, including trailing wildcards. Native `AnyOf([])` remains match-none;
  default native/SQL arity remains zero.
- B8-14 (P2): validate filter conflicts and bounds before query/subscription
  admission, apply inclusive numeric/earliest bounds in raw WebSocket matching,
  and explicitly reject named bounds whose semantics are unsupported. HTTP
  `latest` still resolves against its captured storage head. Do not invent
  safe/finalized/pending state from unrelated status samples.

The Ethereum [filter schema](https://raw.githubusercontent.com/ethereum/execution-apis/main/src/schemas/filter.yaml)
and Geth [filter matching](https://raw.githubusercontent.com/ethereum/go-ethereum/master/eth/filters/filter.go)
were reviewed on 2026-09-17 for wire normalization and position-presence semantics.
LogEx's omitted block bounds and page-window behavior remain unchanged.

Topic presence must be enforced before pagination in both row and column/index
paths. The storage row type and independently nullable columns do not establish
that every stored row has contiguous topics. Check every required leading
position, reusing existing constrained-column reads and adding only needed
wildcard presence reads. An out-of-range public arity must not index past four
columns. No persisted schema or index format change is needed.

The existing typed `blockHash` decoder also accepts a byte array. This inherited
extension is retained; it is separate from the accidental object reinterpretation
corrected for nested parameters. The address/hash string helpers decode into
fixed arrays, avoiding their prior temporary decoded-byte allocations and checking
length before decoding. No request-budget or throughput improvement is claimed.

## Validation plan

Use a few rows with zero through four topics, present all-zero topics, distinct
addresses/hashes and inclusive block endpoints. An independent test predicate
provides expected row identities; compare actual router, gRPC, native indexed/
unindexed and raw-WebSocket helper results. Include wrong-type members, array
filter roots, null/empty OR forms, missing positions, page boundaries and native
empty-disjunction controls. Capture original failures before source changes.
Keep existing ID, notification cancellation and snapshot/reorg checks.

The focused filter fixtures use in-process routers, service calls and predicates.
Workspace gates also run the existing local loopback fixtures. No live chain,
stress workload, benchmark, production dataset or remote host is involved.
Retained subscription history, lag/reorg notifications, shared query
budgets, richer authenticated named-tag support, verified repair and integrated
acceptance remain separate work. Current gRPC already rejects blockHash plus
range; retain it as a cross-protocol control rather than a new finding.

## Final local validation

All eleven final-source local gates pass: 2,000 workspace tests, zero failures, 24 existing ignores across 37 targets, documentation checks and the release node build.

Source is `76667e69a6f95725b46ae2332f925ee4cdddfa88`. The
[validation record](baselines/2026-09-17-ethereum-filter-semantics.json) retains
source hashes, all original and final controls, independent review and full gate
logs. All 142 focused server tests pass with two existing benchmark ignores;
15 native and two storage controls, strict selected-crate Clippy and formatting
also pass. Existing query snapshot, cancellation and ingestion recovery checks
remain in the full suite. Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `e3ced8d9` and ten Linux volume/template controls passed with verified cleanup. [PR #230](https://github.com/tdenisenko/logex/pull/230) merged as `a4eafd46`. The merge tree is identical to the tested head. B8-12–14 are closed within literal decoding and predicate consistency scope. Subscription lag/reorg notifications, shared query budgets, dashboard, verified repair and integrated acceptance remain open.
