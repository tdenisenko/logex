# Runtime configuration and ownership review

## Findings

**B10-24 — low: `info` read consensus before acquiring data-directory ownership.**
At `e76fd8cc`, `run_info` opens consensus first and storage second. Another node
can finish a consensus/storage update and release its lock between those reads,
allowing the command to combine observations from different ownership periods.
It also decodes consensus unnecessarily before discovering an active owner.
`ConsensusStore::open(path, None)` does not publish an existing snapshot; this
finding does not claim that `info` overwrote a running node's consensus state.

The actual CLI regression holds a `PartitionManager` in the parent and starts an
`info` child against the same disposable directory, containing an unavailable
legacy consensus fixture. Original production returns the consensus parse error
instead of the directory-in-use error. This proves the read ordering; it is not
a scheduled reproduction of mixed consensus/storage values. Two original controls
for ordinary inspection and preservation of an unavailable snapshot pass.

**B10-25 — moderate failure-path gap: signal-listener errors escaped supervision.**
The Unix listener used `expect` for registration, so an I/O error unwound past
the supervisor instead of requesting engine shutdown and arming its whole-cleanup
deadline. A closed stream was reported as a normal signal. The non-Unix branch
also discarded the `ctrl_c` result. These are distinct from ordinary received
signals, whose existing successful shutdown behavior remains valid.

The unchanged Unix listener fails a control using an owned, already-closed Tokio
runtime handle. Pinned Tokio 1.51.0 returns `signal driver gone` before installing
an OS handler, and the original `expect` unwinds. This is a controlled registration
I/O failure, not evidence that the normal node runtime loses its driver or that an
OS registration failure occurred in a deployment. No OS signal is sent.

## Corrections

`info` now opens storage first and keeps its owner through consensus inspection
and output. Its private consensus helper requires a borrowed `PartitionManager`
instead of an independent path, making the ownership requirement explicit.
As with other maintenance commands, normal storage open may perform verified
recovery before later inspection reports an error. Storage-open errors now take
priority over consensus errors. Successful fields and consensus validation are
unchanged, including explicit errors for unavailable or stale snapshots.

The signal future now returns an I/O result with registration context. Closed
streams return `BrokenPipe`; the non-Unix branch preserves its error. The
supervisor treats an error as terminal, arms the existing whole-shutdown guard
before reporting, notifies the engine, waits through the existing grace period,
preserves the first failure and leaves final exit unsuccessful. Normal signal
events still produce successful cooperative shutdown. There is no retry of a
failed listener and no new watchdog, timer or shutdown timeout.

## Remaining runtime review disposition

| Area | Offline disposition |
| --- | --- |
| CLI and TOML resolution | Explicit value-source precedence, strict supported keys, value-safe diagnostics and typed numeric/address parsing remain as covered by [configuration precedence](config-precedence.md). No new framework or broad validation policy was needed. Invalid zero segment targets are rejected by catalog encoding without publishing a replacement. Existing maintenance-job normalization and range selection are retained. |
| Startup and exclusivity | Sync holds `PartitionManager` during consensus mutation; index/compact commands retain it through scoped work. `info` ordering is corrected here. Checkpoint resolution and initial existence probes are advisory reads before ownership. The previously recorded missing-consensus/startup-state transition review remains in batch 5. |
| Worker exits and maintenance | Service and consensus/execution supervisors retain first failures. Checkpoint failures are fatal; optional derived-index/compaction operation errors retain their retry policy. Blocking worker join failures reach supervision. See [node workers](node-worker-supervision.md), [maintenance workers](maintenance-worker-supervision.md) and [consensus supervision](consensus-supervisor-ownership.md). |
| Shutdown | Existing 120-second engine grace, 60-second inner cleanup waits and absolute 180-second whole-shutdown deadline remain. Ownership spans runtime and monitor destruction. Signal listener errors now enter that lifecycle. See [cleanup deadlines](runtime-cleanup-deadlines.md). |
| Storage health and external volumes | Existing independent probes, terminal deadlines, descriptor-relative paths and platform identity checks are retained. See [ordinary monitoring](independent-storage-health.md) and [expected volumes](expected-volume-supervision.md), including their platform evidence and support limits. |
| Unsafe initialization and FFI | Rechecked the node call sites: `rlimit`, `statvfs`, Unix descriptor operations and macOS volume attributes document pointer/lifetime/initialization contracts. Result structures are assumed initialized only after successful system calls; newly owned descriptors transfer once. Existing platform fixtures exercise decoding and lifecycle. No unsafe block changes. |
| Best-effort state | Final peer hints remain a derived cache: persistence failure is reported without converting a clean primary-storage shutdown into failure. Trusted storage and consensus failures use their separate terminal paths. |

This closes the remaining batch-10 offline code review.
It does not close shared query budgets, sync-state review, verified offline repair
or integrated acceptance. Ordinary storage startup before `PartitionManager::open`
finishes remains outside its health-monitor lifetime; expected-volume preflight
has its separate deadline. Signal observation still depends on the runtime being
able to poll. No arbitrary syscall cancellation or real-time deadline is promised.

## Validation and cleanup

Original production fails both named controls. The final node run passes 217 unit
tests, three actual CLI tests and 13 volume-example tests. Added controls also
verify first-failure text, notification, stopped status, deadline ordering and
engine grace expiry. The CLI cases compare all fixture file names and bytes
before/after inspection, exercise both absent and present consensus, and verify
that failed inspection releases ownership. Fixtures are isolated and start no
sync engine or peer network. Existing unit controls use owned ephemeral listeners.

Removed the path-only info helper contract, both signal-registration `expect`
calls and ignored signal results. Existing success tests now explicitly supply
`Ok` events; they retain their assertions. Source review traced all call sites
and cleanup ownership. No independent reviewer is claimed.

No ingestion writes, per-block computation, query algorithm, dependency, format
or configuration default changes. No benchmark is warranted for these inspection
and terminal-error changes. All ten local gates pass on `aaa8786d`, including 1,908 workspace tests / 24 existing ignores. Publication is recorded in
the [validation ledger](baselines/2026-09-17-runtime-config-review.json). No live
sync, deployment, mac-mini work or production-data access occurred.

All six CI jobs passed on `3336965a`, including ten Linux volume/template cases with verified cleanup. [PR #219](https://github.com/tdenisenko/logex/pull/219) merged as `43bb6bc0`. The merge tree is identical to the tested head.
