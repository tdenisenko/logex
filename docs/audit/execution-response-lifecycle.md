# Execution response ownership and peer-count admission

Base: PR #226 merge `595e99745631c515d1c13f8909426af539947095`.
Branch: `audit/execution-response-lifecycle`.
This pass reviews remaining inbound/outgoing response ownership and fixes a
numeric startup boundary. All eleven final-source gates and six CI jobs pass; PR #227 merged as `50c1194f`.

## B4-49: unchecked configured peer counts

The CLI and public `PeerManagerConfig` accept a `usize` peer count. The original
constructor divides this into outbound and inbound counts, narrows inbound to a
`u32` session field, and calls the pinned dependency's event-buffer scaler. That
scaler doubles the total before applying its small cap.

On 64-bit targets, 6,442,450,943 total peers first produces an inbound count of
4,294,967,296, which narrows to zero. Values above `usize::MAX / 2` cannot be
doubled by the scaler: debug arithmetic panics, while release arithmetic can wrap.
These are low-severity local configuration/startup defects. No remote input,
normal-load failure, large allocation or network experiment is claimed.

The constructor now builds validated peer/session configuration as its first
operation. Checked multiplication verifies the pinned scaler's arithmetic;
checked conversion produces the actual `u32` inbound value consumed downstream.
The existing builder block moves into this preflight. NAT resolution, bootnode
parsing and network setup follow only after it succeeds.

This ordering applies to the execution constructor. Node startup reaches it after
some shared state and services have initialized; the existing runtime error branch
logs the constructor diagnostic and exits with status 1.

Zero, normal counts, outbound/inbound splitting, dial limits, request timeouts,
backoff and event-buffer scaling remain unchanged. The existing outbound pending
conversion is safe under its explicit 32–96 dial clamp. The accepted upper numeric
boundary is 6,442,450,942 on 64-bit and `usize::MAX / 2` on 32-bit. Those are
representation limits, not recommendations to run those counts. No operational
peer cap or new per-request/ingestion work is introduced.

## Evidence and validation scope

Two additive tests against exact original production source call the real peer
split helper and require its results to fit downstream arithmetic/fields. Both
fail. These are original arithmetic witnesses, not original constructor startup
tests: they do not run the narrowing/scaler, allocate large channels or start a
node. Original source and the test-only patch are retained.

Six final controls exercise actual configuration objects: zero/small/default
splits, pending limits, pinned buffer scaling, largest accepted and first rejected
values, each invalid arithmetic category, and immediate error from the public
constructor without a Tokio runtime. They all pass, as does strict sync Clippy.
The 32-bit conditional source is reviewed, but no 32-bit target was compiled.
Final workspace and platform validation are recorded below.

## Inbound ownership disposition

The request deadline spans bounded session-channel admission and the response
wait. Canceling before channel admission removes that send; canceling afterward
drops the receiver. The session owns an admitted request by ID until response,
timeout/tombstone handling or disconnect. It can still decode and discard a
response whose local receiver has closed. That is the documented transport
lifetime, not a demonstrated cross-request delivery or retained-state leak.

The pinned P2P layer bounds declared decompression at 16 MiB before allocation;
the ETH layer rejects messages above 10 MiB after decompression and before typed
RLP decode. These protect different phases. Checked header-derived receipt
weights apply after decoding and before retained ETH70 fragments or reconstructed
blooms. Existing body-first commitment validation and per-role source attribution
remain necessary and unchanged.

The 32-entry session command queue, per-operation scheduler limits, typed response
buffers and engine pipeline windows are separate owners. A queue capacity does
not equal an inflight or aggregate byte budget. Existing pressure reactions are
scheduling controls; no exact total process memory reservation is inferred.

## Outgoing ownership disposition

Provider calls return owned body/receipt clones. Cache read guards do not escape,
and earlier results intentionally remain usable after cache eviction. Responses
move through oneshots, session queues and the encoder; failed sends and session
destruction release their ownership. Existing canceled-receiver entry checks skip
provider work before serving begins; a later close may still leave synchronous
work running.

The serial request handler, per-session pending/outgoing counts and lower stream
queue each apply their own backpressure. The 2 MiB handler target is soft so whole
items and required progress can exceed it. Valid empty items and ETH70 cursor/
completion semantics remain intact. Encoding precedes the lower stream's hard
size check, so that check is not a bound on earlier allocations. Normalized cache/
provider telemetry is not a wire-delivery or total resident-memory measure.

No additional response-validation, attribution or serving-lifetime defect was
established in these source reviews. A cursor-aware provider could avoid cloning
receipt prefixes; a pre-wire receiver check could skip some canceled work. Neither
has a demonstrated substantial normal-ingestion benefit here, and neither is
needed to correct this numeric admission defect. Existing APIs and semantics are
retained rather than redesigned for those optional opportunities.

## Retained state, cleanup and remaining work

Peer/order teardown, bounded learned hints/backoff history and registered request
owner retirement retain their existing tested lifetimes. Configured peer IDs are
finite local configuration, and active owner records are removed on retirement.
No additional obsolete helper or duplicated lifetime owner was found.

Removed the superseded late constructor builder block and unchecked inbound
narrowing. The original peer split helper remains used. No dependency, vendor
patch, wire/storage encoding or successful-response behavior changes.

The existing limits do not promise an aggregate network RSS ceiling. Integrated
offline workloads still need to cover concurrent ingestion, queries, serving and
restarts. The pending shared-query budget choice does not by itself select a new
global network admission policy. Automatic verified repair and the broader
offline audit remain open; live sync and staging follow offline completion.

## Batch disposition

With the numeric correction validated and merged, this pass completes the
batch-4 offline protocol, ownership, retention and implementation-cost review. It
supersedes earlier open transient/provider-lifetime notes in the batch ledger.
No additional persistent remote-derived owner requiring a fix was established.
This does not establish a global RSS guarantee or complete batch-12 integrated
robustness and performance acceptance.

## Final local gates

All eleven final-source local gates pass: 1,968 workspace tests, zero failures, 24 existing ignores across 35 targets, documentation tests and release node build.

Source is `eecd57d771dfd0ab673cfdf9cc441d16547822fd`. The [validation record](baselines/2026-09-17-execution-response-lifecycle.json)
retains full gate logs, source/review hashes and original arithmetic evidence.
Existing dependency/linker/future-compatibility warnings remain recorded. Vendor
sources are unchanged from PR #226; its separate local dependency suite is not
repeated for this unrelated source change. Linux/macOS CI still runs those expiry
regressions in addition to workspace checks. Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `25f88b99` and ten Linux volume/template controls passed with verified cleanup. [PR #227](https://github.com/tdenisenko/logex/pull/227) merged as `50c1194f`. The merge tree is identical to the tested head. B4-49 and the batch-4 offline protocol, ownership, retention and implementation-cost review are complete. This supersedes older open transient/provider-lifetime notes; no global RSS guarantee is claimed. Integrated mixed workloads remain in batch 12.
