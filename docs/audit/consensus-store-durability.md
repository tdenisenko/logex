# Consensus snapshot durability and restore boundaries

Base: `037e54ad`, after merged PR #153. This batch closes demonstrated snapshot
publication, restore-shape and save-failure handling defects. It does not complete
CL protocol conformance, network resource limits, volume supervision or repair.

## Findings and resulting behavior

| ID | Severity | Evidence and correction |
| --- | --- | --- |
| B3-01 | P1, failed-write publication | Anchor mutators changed the shared snapshot before saving. With an occupied destination, saving returned an error while readers already saw the new finalized/optimistic anchor. All snapshot mutations now build a private candidate, serialize writers, durably replace the file and only then publish to readers. |
| B3-02 | P1, restart durability | The old save used a shared `.json.tmp`, ordinary write and rename, with neither file nor directory synchronization. Concurrent public writers could also race the shared temporary file. Saves use unique owned temporary files, buffered serialization, file sync before atomic rename and directory sync before publication. New directory entries are synced at initialization. Existing stores never recreate a disappeared directory. |
| B3-03 | P2, invalid restored state | Reopening accepted out-of-order/duplicate anchors and impossible participant counts, committee lengths and checkpoint/slot relationships. Restore rejects these structures before exposing them. Derived consensus heads are recomputed from the retained sources. No new peer-signature bypass is demonstrated. |
| B3-04 | P1, write-failure supervision | Several gossip/RPC/force-update callers logged save failures and continued scheduling/publication. A first save failure now permanently latches an error, rejects further writes and stops consensus request processing. Node supervision signals shutdown and reports a nonzero exit. |

The primary before-fix publication regression failed on the first append case;
replace and range cases were reached after the fix and pass. The ordering
regression failed on descending anchors before the guard; the duplicate case was
then also checked after the fix. Structural light-client tests independently
restore each malformed fixture through serde before requiring validator rejection.
The latter demonstrate serde acceptance, not a separate pre-patch validator run.

## Save and shutdown contract

The writer mutex covers snapshot cloning, candidate changes, serialization,
durability and publication. Readers hold only the memory mutex and can continue
reading the last published snapshot while a save is blocked. There is one snapshot
clone per save, as previously; the serialized whole-file byte buffer is replaced
with a buffered writer. Restore also avoids a separate whole-file UTF-8 string.
The normal consensus network already serializes its updates; concurrent-writer
coverage applies to the public shared-store API, not an asserted two-writer bug
in the normal event loop. Data-directory process exclusivity remains owned by
the existing node/storage lifecycle; two separately opened ConsensusStore objects
are not a supported way to coordinate multiple writers.

A write error can occur after rename. The old memory snapshot stays visible, and
all writes on that instance fail until the process reopens the state. Reopen may
therefore find either the previous complete snapshot or the new complete snapshot;
it must never overwrite the file by retrying an old memory image after uncertainty.
The first error survives even if no watcher was subscribed when it happened.
Network responses return before seeding candidate headers after a failed save,
and normal network-error restart behavior is retained.

Runtime subscribes before starting consensus networking. Its failure branch
signals global shutdown and bounds the engine wait. A separate thread starts
before the network, observes the failure independently of application runtime
workers, and enforces a 180-second cleanup grace before exit 1. Guard destruction
disarms both its initial notification wait and its deadline. Tests inject an
expiry callback rather than terminating a test process. All eight focused runtime
controls pass; complete merge gates remain pending.

File synchronization uses the pinned standard library, consistent with existing
storage durability code on macOS and Linux. These tests establish ordering and
error handling in software; they do not prove physical-device power-loss behavior.
No new external-volume testing or Mac mini work was performed.

The durability barriers are new work per consensus snapshot save and can increase
save latency. They do not add per-row writes to the execution ingestion columns.
No throughput percentage is claimed: the owner ended timing campaigns and accepts
necessary integrity costs. Further measurement requires a concrete implementation
opportunity; historical benchmarks and their limitations remain unchanged.

## Restore contract and compatibility

Preserved valid states include descriptor-only bootstrap state, gaps in ordered
anchors, progressing finalized/optimistic state, independent roots at the same
slot, force updates and committee rotation. Stored header fork metadata is not
required to equal a fork inferred solely from its slot. Cached best updates are
not rejected merely for being older than a later selected head.

The obsolete payload-only reconstruction path and bootstrap-slot inference are
removed under the owner's explicit waiver of backward compatibility. A snapshot
containing light-client payload/status data without its verified store now returns
an actionable error. It is neither silently cleared nor migrated. Normal saved
bootstrap/finality/optimistic/period payloads remain available for RPC serving.
Existing persistence fixtures now use 512 valid deterministic committee keys and
their aggregate; synthetic status/payload fixtures remain storage tests rather
than authenticated protocol fixtures.

These checks establish structure, not authenticity of arbitrary edits to local
files. The JSON format has no new integrity envelope in this batch. Cached SSZ
payload decoding, a corruption-detection envelope and resource/retention policy
remain explicit follow-ups. Ordered anchors and period payloads have no enforced
fixed historical size bound; inventing a file-size cap would reject legitimate
output. Singleton and range RPC reads currently clone the full payload map, a
concrete cost-reduction lead for the next network batch.

## Validation and cleanup

Focused tests cover failed saves for all anchor mutations, permanent failure
notification without existing subscribers, staging-file cleanup, disappearing
directories, dangling state-file links, readers during a blocked save, concurrent
appends and exact reopen equivalence. An injected error after completed replacement
checks old memory/new file behavior and rejection of rollback writes. No actual
power loss is simulated by that injected error.

Restore controls cover bad anchor order/duplicates, missing verified state,
participant bounds integrated through open, and valid checkpoint enrichment.
Light-client tests cover field mutations plus real fixture-based force/rotation
transitions. Network/node watcher controls use in-process channels without sockets.

Removed premature shared mutations, fixed-name snapshot staging, whole-file JSON
buffers, and legacy payload reconstruction that could silently discard failed
restorations. No blanket lint changes, new unsafe code, retention cap or benchmark
campaign was introduced. The pinned existing tempfile dependency is promoted from
dev-only to production use; the lockfile is unchanged.

Source `8832a8d528f06cdff97ed398f24253c6c7f08457` passed all seven local gates:
vendored-source verification, formatting, locked workspace/all-target check,
strict Clippy, all-target tests (1,227 passed, 23 intentionally ignored), doc tests
and the release node build. Existing dependency future-incompatibility and Apple
debug-linker warnings remain. Evidence is in
[the gate record](baselines/2026-09-15-consensus-store-durability-gates.json).
All six final-head CI checks passed in run `34911640046`, including Linux and
macOS tests, on `8571c7ed`. [PR #154](https://github.com/tdenisenko/logex/pull/154)
merged as `d0a31b2aefa3b9b3032eaaad67d32bd097da5cbb` at 2026-09-15 00:12:20 UTC.
[CI and merge evidence](baselines/2026-09-15-consensus-store-durability-ci.json).
