# Consensus network admission review

Base: PR #234 merge `115ac55431be4b7df60f45ce1a6c8f4582ec33a5`.
Branch: `audit/consensus-network-admission`. This discrete milestone fixes
peer-count arithmetic (B3-70) and records the remaining resource owners.
Source `2cc8bba7acc3d8948305751b16a61ef6fd390612` passes focused consensus
checks, independent review and all eleven local gates. Exact-head CI and merge
remain.

## B3-70: silently reduced configured connection limits

The original CL connection limiter narrows a configured `usize` target to `u32`,
saturates multiplication by 9, 11 or 13, then divides by 10 with upward rounding.
Saturation before division silently changes otherwise representable limits.
For a target of 500,000,000, all three limits become 429,496,730 instead of
450,000,000 incoming, 550,000,000 outgoing and 650,000,000 total.

This is a low-severity local configuration defect. The default target of 32 is
unaffected. The reproducer constructs the actual pinned dependency configuration,
which owns six optional integers and empty bookkeeping containers; it opens no
connection and allocates no buffers proportional to the supplied count. Exact
original production source plus the additive control fails after compilation.

Scaling now uses checked `u64` arithmetic, then checked conversion into the
dependency's actual `u32` fields. The largest representable target is
3,303,820,996: its total limit is `u32::MAX`; the next target requires
4,294,967,297 and rejects. This is a representation boundary, not an operational
recommendation. Zero leaves aggregate established limits unset; pending and
per-peer limits retain their existing values. Ordinary ceil ratios are unchanged.

Configuration validation is the first `ConsensusNetwork` constructor operation.
It precedes discovery-key files, built-in peer parsing and transport construction.
The validated behavior is passed into swarm construction, avoiding a second
conversion. Overall node startup can initialize shared state before reaching
this constructor; the change does not claim whole-node startup is immutable.
CLI help now describes the peer target and zero behavior rather than incorrectly
calling it a retained-discovery inventory limit.

Three controls cover zero/small/default/large representable values, the numeric
boundary, and immediate invalid construction without a Tokio runtime or network
directory creation. The complete focused CL suite passes 391 tests with one
existing ignore, alongside strict all-target Clippy, formatting and independent
review. The superseded saturating conversion was removed. No dependency,
persisted format, trust rule, operational quota or per-message work changed.

## Validation and evidence

All eleven gates passed on the frozen source: vendor integrity; workspace,
patched Reth, ENR, gossip and aggregate formatting; locked workspace check;
strict Clippy; all-target tests; documentation checks; and release node build.
The workspace run passed 2,058 tests across 37 targets, with no failures and 24
existing ignores. Documentation checks covered eight targets with no examples.
Existing dependency deprecation and future-compatibility warnings remain separate
dependency work. No throughput measurement was needed for startup-only arithmetic.

The [machine-readable record](baselines/2026-09-18-consensus-peer-limits.json)
binds the base and final source hashes, before/fixed controls, reviews and gate
logs. Its evidence archive contains 15 files (644,414 uncompressed bytes;
119,009 compressed), SHA-256
`e9348a9c625d0af13bb3256426d9b5e029801d2c184c5575da4948afa68b1686`.
The validation archive contains 14 files (243,925 uncompressed bytes; 46,576
compressed), SHA-256
`944350d173358a7c1c95b943c2d46b91f5670a007621dd802243dc20e59a367e`.
The original-control failure is expected evidence; it is not a candidate failure.

## Remaining resource owners

The following are source-review findings, with implementation and recovery
controls still required. This milestone does not establish a global memory cap.

- Protected authenticated Beacon metadata can outlive its selected fork for the
  entire process. Required ancestry includes persisted anchor roots, the fixed
  checkpoint, both active and latest targets, and pending request/recovery roots.
  Reclamation must establish that closure before demoting obsolete records into
  the existing bounded candidate lifecycle. Required anchors and period serving
  history remain retained under the approved journal policy.
- Gossip admits transformed messages into duplicate and payload/history caches
  before checking local topic subscription. CL validation cannot prevent this
  earlier ownership. Correct expiry (PR #226) does not cap the number of distinct
  entries within the 768-second seen interval or six heartbeat history buckets.
  Reject/Ignore releases payloads but leaves history and seen bookkeeping.
- Remote SUBSCRIBE and GRAFT paths can retain arbitrary topic strings for a
  connection's lifetime. The latter inserts topic membership before its unknown
  topic check, bypassing the subscription filter. A finite supported-topic
  policy must cover both paths and preserve fork pre-subscription/retirement.
- The handler queue length bounds ordinary forwarding queues, while priority
  subscription/GRAFT/PRUNE controls use an unbounded queue. Its ownership and
  overload behavior need a separate correction; silently losing required
  control state is not an acceptable substitute for bounded storage.

Cache overload policy is awaiting the user's choice: preserve admitted entries
until normal expiry and drop excess new gossip, or evict older entries to admit
new data. The former preserves validation ownership and duplicate lifetime but
can reduce gossip availability until capacity returns. The existing verified RPC
head-recovery scheduler is independent, subject to usable peers and event-loop
progress. No unconditional head-liveness guarantee under overload is inferred.

No live traffic, stress allocation, remote-host work or benchmarking was used in
these reviews. Shared query resources, dashboard review, automatic verified
segment repair and integrated acceptance remain separate audit work.
