# Gossip topic ownership and early type bounds

Base: PR #235 merge `66512764923f3598b5eb5c107b93bfc5a2474eb8`.
Branch: `audit/gossip-topic-ownership`.
Source: `9778b322a703c61c0b2d0c89ccb82df6c9014db4`.
All eleven local gates passed on the frozen source. PR #236 merged as `59f5722c` after all six CI jobs.

## B3-71: unsupported topics acquired retained state

The application recognized only canonical supported light-client topics, but its
gossipsub behavior used the default unrestricted subscription filter. The vendor
receive path transformed and cached data before checking local subscription, so
application validation did not see or release messages on unrelated topics.
Remote subscriptions and GRAFT could retain arbitrary topic names in peer state.
PRUNE could retain backoff entries even when a topic was unknown to the local
mesh. Existing per-frame limits and expiry did not restrict the topic namespace.

This is a retained-resource correctness finding. It does not demonstrate invalid
consensus data entering trusted state or establish a full-node stall. The finite
reproducer inspects the actual in-memory handlers and their owned state.

CL now builds a static whitelist from the same supported-digest enumeration used
by its topic parser. The current repository schedule contributes six digest
states and twelve canonical finality/optimistic topic strings, including both
blob-schedule transitions. All supported past/future topics remain eligible;
active, pre-subscribed and retiring topic decisions still belong to consensus
validation. The whitelist does not authenticate a payload.

The vendored handlers apply topic eligibility before retained work:

- Receive checks before metrics, transform cloning, IDs, caches and events.
- GRAFT filters each topic once before peer-topic insertion, mesh work or
  explicit-peer responses.
- PRUNE checks before backoff tracking and peer exchange.
- Codec-invalid publications check before invalid-topic metrics, scoring or
  logging, covering the route that bypasses ordinary receive.

Existing SUBSCRIBE/UNSUBSCRIBE filtering remains. IHAVE already requires a local
mesh topic; IWANT and IDONTWANT carry no topic strings. Ineligible topic items
are neutral and do not produce invalid-message penalties. Allowed items retain
normal handling. Locally configured score/publish APIs are separate inputs.

The vendor `can_subscribe` contract now explicitly includes these traffic-driven
eligibility checks. Custom mutable callbacks observe additional calls; this hook
is an eligibility predicate rather than a consumable quota. Incoming subscription
batch/cardinality overrides remain separate. CL uses a static whitelist, while
the default AllowAll behavior has an unchanged compatibility control.

## B3-72: type limits ran after message ID/cache work

The initial transform enforced a global 10 MiB bound. Application decoding later
enforced the much smaller existing light-client limits: 1,032 optimistic and
2,120 finality bytes. Known-topic oversized messages could therefore incur ID and
cache work before the application rejected them.

Inbound and outbound transforms now use the existing type-specific compressed
and declared decoded-length limits. Wire bytes are preserved, and the ID
algorithm is unchanged. Within those bounds, malformed Snappy still receives the
required INVALID-domain ID. Direct ID conformance controls remain independent of
admission, including the valid-Snappy example above an optimistic type limit.
Cryptographic verification, timing, committees, persistence and forwarding rules
are unchanged.

This correction adds a short static-set lookup and borrowed topic/header checks
to the normal path. It does not add persistence barriers or a new per-ingestion
operation. No timing benchmark or measured throughput/RSS claim is made.

## Regression evidence and remaining limits

Exact original application production plus two additive controls compiled and
failed both new assertions, with eleven existing controls passing. Original
vendor production plus eight additive handler controls compiled and produced
five failures and three compatibility passes. These use tiny buffers, topic
sets and one in-memory peer; no network connection or load generation is used.

The corrected source passes 25 focused gossip controls, all 393 consensus tests
(one existing ignore), strict consensus Clippy and all 162 standalone vendor
library tests. Independent application and combined source reviews found no
blocking defects. Controls cover allowed Accept/Reject delivery, mixed and
explicit-peer GRAFT, PRUNE/backoff, subscriptions, codec-invalid score neutrality,
AllowAll behavior, size boundaries and supported fork transitions.

The vendor archive checksum and all 34 original inventory entries were verified.
The retained patch was applied to verified upstream copies and reconstructed all
six modified package files exactly. The repository verifier covers 483 files
across ten vendored packages. No dependency version or lockfile changed in this
milestone. CI retains expiry controls, adds the topic controls through `logex_`,
and formats the subscription-filter documentation source.

Initial RPC decoding remains bounded by the existing global frame limit, and
the generic transform's raw clone still precedes type validation. Eligible-topic
cache entries/bytes, event/forwarding copies and priority-control queues have
separate ownership; this change does not establish a total memory cap. Cache
overload policy and protected obsolete-fork metadata remain open. Shared query
resources, dashboard review, automatic verified repair and integrated acceptance
also remain unfinished. No live sync, remote-host work or production data was
used, and this milestone does not establish release readiness.

## Full validation record

All eleven local gates passed: vendor integrity, workspace/patched Reth/ENR/
gossip/aggregate formatting, locked workspace check, strict Clippy, all-target
tests, documentation checks and release node build. The workspace run passed
2,060 tests across 37 targets with no failures and 24 existing ignores.
Documentation checks covered eight targets with no examples. Existing dependency
deprecation and future-compatibility warnings remain separate recorded work.

The [machine-readable record](baselines/2026-09-18-gossip-topic-ownership.json)
binds original and final source, regression logs, independent reviews, exact
vendor reconstruction and the gate logs. The evidence archive contains 33 files
(1,876,422 uncompressed bytes; 317,605 compressed), SHA-256
`5f48ae8af74f0f8c02bcc69a62ac268862a0ad70260926c29fd522b84befe2ba`.
The validation archive contains 14 files (245,423 uncompressed bytes; 47,067
compressed), SHA-256
`6cf075db63063a778e55235eb617d31181b724e464ecfddd9b8b562b47470bab`.
Original-source failures are expected regression evidence; corrected-source
validation has no failures.

All six CI jobs passed on `b976cd76` and ten Linux volume/template controls passed with verified cleanup. [PR #236](https://github.com/tdenisenko/logex/pull/236) merged as `59f5722c`. The merge tree is identical to the tested head. B3-71 and B3-72 are closed in their documented scope. Protected metadata, gossip admission, shared query resources, dashboard review, verified repair and integrated acceptance remain open.
