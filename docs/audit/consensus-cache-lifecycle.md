# Consensus duplicate-cache expiry and history review

Base: PR #225 merge `6fc297c9b818e5ff731af353fa96172ead4bffd3`.
Branch: `audit/consensus-resource-lifecycle`.
This milestone fixes duplicate-ID expiry in the pinned gossip dependency. It
does not close aggregate network admission, protected-history lifetime or the
broader offline audit. All twelve final-source gates and six CI jobs pass; PR #226 merged as `595e9974`.

## Confirmed finding

**B3-68 — moderate, expired duplicate membership and idle retention.** In pinned
`libp2p-gossipsub` 0.49.4, insertion removes expired records, but membership reads
only inspect the map. After insertion stops, an expired ID remains present and
later IHAVE announcements for that ID do not elicit IWANT. Heartbeats also leave
expired duplicate and locally published ID entries owned by the cache. Three
small original-source controls reproduce these behaviors.

The demonstrated effect is stale announcement suppression and expired entry
retention. A directly received full message already prunes on insertion. LogEx
has no current production caller of local gossip publication. No test demonstrates
a chain stall, missed useful late update, memory exhaustion or incorrect query
result; RPC recovery and light-client timing rules remain separate.

## Correction and cost

Membership now requires the stored deadline to be strictly later than the sampled
monotonic instant. Each heartbeat prunes both ID caches, using the existing expiry
queue. Its metrics timer includes this work. First insertion still establishes
the deadline, duplicate insertion does not extend it, and insertion's boolean
result keeps its original meaning. Peer-score delivery records continue to use
the unchanged entry semantics.

LogEx retains its existing two-epoch seen lifetime and 700 ms heartbeat, configured
from the pinned [consensus networking specification](https://github.com/ethereum/consensus-specs/blob/v1.6.0/specs/phase0/p2p-interface.md).
Expiry-aware reads work between heartbeats; physical removal needs an insertion
or a polled heartbeat. Live entries are not evicted early. Removed entries do not
imply shrinking collection capacity or reduced process RSS.

The change adds a clock/deadline check to membership reads and runs the existing
expiry removal during heartbeats as well as insertion. Removal visits expired
entries until the first live deadline. It adds no storage writes, persisted fields,
ingestion barriers or full-history scans. There is no benchmark or throughput
percentage claim.

## Dependency provenance and regression evidence

The complete published package is vendored at its existing version. Its archive
SHA-256 matches the original application lockfile:
`a538e571cd38f504f761c61b8f79127489ea7a7d6f05c41ca15d31ffb5726326`.
The upstream inventory, reviewed patch, embedded license notices and provenance
are retained under `vendor/libp2p-gossipsub`. The offline vendor checker verifies
every upstream file and exactly five modified files. The application's lockfile
only changes this package from registry to path; dependency versions do not change.

The excluded dependency cannot run its library tests as a workspace member. Its
published standalone manifest also omits the upstream `quickcheck` test edge.
The local manifest restores that dev dependency at 1.0.3; the standalone lock adds
only quickcheck 1.0.3 and env_logger 0.8.4 and changes lock format 3 to 4. Existing
locked versions remain unchanged. The upstream smoke target remains untouched
and is outside the `--lib` validation scope.

The three new tests use explicit monotonic instants and 20-byte IDs, without
sleeping or sockets. They exercise exact expiry, nonrefreshing live duplicates,
real in-memory IHAVE handling and real heartbeat cleanup of both ID caches.
Their insertions preserve the chronological expiry-queue invariant. Original
production source plus the additive test harness fails all three after successful
compilation; the corrected source passes all three. Earlier nonmember and missing
dependency attempts are setup errors, not behavioral regression evidence.

Linux and macOS CI explicitly run these supplementary tests against the package's
standalone locked dependency graph and check patched Rust formatting. Workspace
tests and strict Clippy separately cover the application's dependency graph.
An additional standalone strict-Clippy attempt reports existing upstream lints
outside the changed hunks; no lint allowances or unrelated source rewrites were
added. Those supplementary failures are retained alongside the successful
application gates rather than described as a clean standalone lint run.

## History and resource-policy disposition

The source review confirms that canonical ordered anchors are needed to rebuild
verified Beacon metadata on restart, establish checkpoint-to-head ancestry, select
execution ranges, admit reorgs and calculate coverage. The active target can lag
the newest target; pending authenticated requests, disconnected trusted prefixes
and force-selectable verified updates need appropriate ancestry too. Arbitrary
history truncation is not justified.

Authenticated obsolete forks can remain permanently protected by the current
metadata cache, and the optional RPC-serving period-payload map grows by retained
periods. Candidate metadata and raw-body caps do not cover those owners. A safe
lifetime policy must distinguish required ancestry from optional serving or fork
history and preserve recovery when targets change. That work remains open.

Actual changed consensus saves clone and rewrite all retained anchors and period
payloads, even for a small head/status change. Materialization also builds the
real checkpoint-to-target anchor sequence. The prior no-op and bounded-read
optimizations do not remove that cost. An incremental checksummed journal with
periodic checkpoints is proposed while retaining all trusted anchors; the
material persistence-design choice and its recovery implementation remain open.

Duplicate caches still have no aggregate entry/byte admission limit. Application
Ignore/Reject and frame bounds cannot provide one after dependency admission.
Changing overload behavior requires a separate policy that preserves the lifetime
of already admitted IDs and distinguishes local resource pressure from duplicates
or peer faults. The expiry fix is not a claim of a total network memory bound.

## Cleanup and acceptance boundary

The stale membership implementation is replaced, and idle expiry reuses the
existing queue-removal logic. No upstream smoke tests, public APIs, legitimate
serving paths or history consumers are removed. Original package files and source
notices remain reviewable. Shared query admission, automatic verified offline
repair and integrated acceptance remain separate roadmap work. No live sync,
remote-host testing, production-data change or deployment is part of this pass.

## Final local validation

All twelve final-source local gates pass: 1,962 workspace tests, zero failures, 24 existing ignores across 35 targets, plus three standalone expiry regressions, documentation tests and the release node build.

Source is `1310497ad62d50afb3802064248f894151471247`. The [validation record](baselines/2026-09-17-consensus-cache-lifecycle.json)
contains exact source/command/log hashes and lossless evidence archives, including
original-source controls, final reviews and known supplementary lint failures.
The full 154-test standalone library suite and 366 consensus tests passed before
the telemetry-only timer reorder; final expiry and workspace gates cover the final
source. Existing linker/future-compatibility warnings remain in the complete logs.
Exact-head CI and merge passed; verified closure follows.

All six CI jobs passed on `c5f3fd0b`, including the standalone expiry regressions on Linux/macOS and ten Linux volume/template cases with verified cleanup. [PR #226](https://github.com/tdenisenko/logex/pull/226) merged as `595e9974`. The merge tree is identical to the tested head. B3-68 is closed; aggregate gossip admission, history lifetime/persistence, shared query resource policy, automatic verified repair and integrated acceptance remain open.

After local gates and exact-head CI, the disposable standalone dependency target
was removed: 3,672 build files containing 704,618,546 logical file bytes. The
source snapshots, logs and evidence remain. No other build tree or remote host
was changed by this cleanup.
