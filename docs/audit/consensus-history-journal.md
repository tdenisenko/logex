# Incremental consensus history persistence

Base: PR #233 merge `22fcd71d73dfec375bc3837fdf91ae126d5a4f06`.
Branch: `audit/consensus-history-journal`. The user approved an incremental
checksummed journal with periodic checkpoints, preserving required trusted
anchors. Implementation and all eleven local gates are complete; PR #234 merged as `115ac554` after all six CI jobs.
Finding B3-69 records the demonstrated full-history mutation cost and the scoped correction.

## Scope and existing cost

At the base commit, every changed ConsensusStore mutation clones the full published
snapshot and rewrites it through the checksummed snapshot envelope. This
includes all ordered anchors and per-period serving payloads even when only one
verified header or one period entry changes. Existing no-op suppression and
bounded readers are already correct and must remain. The finding is an
implementation cost, not a newly established incorrect consensus decision.

Typed changes are prepared under the existing serialized writer, persisted
outside the reader mutex, and applied to shared memory only after success. The
permanent uncertain-save failure latch remains. Periodic checkpoint creation may
clone the complete state; ordinary updates do not. Full anchor replacement
naturally handles all replacement records. Localized updates account for changed
gap edges; upserts merge the affected span once and shift the untouched vector
suffix once. Tail append needs no merge buffer. Overlapping historical updates
may still move a large suffix; this is not a constant-time guarantee.

## Selected persistence layout

One state directory, `cl/consensus_state/`, lets startup preservation and
stale-checkpoint archival move the complete state atomically. It contains a checksummed CURRENT commit
frontier, an immutable checkpoint and an append journal generation. CURRENT
binds the checkpoint identity, journal generation, committed byte offset,
sequence and hash-chain head. Validate every committed record and reject
missing/truncated/incorrect committed data; never fall back to an older valid
prefix or checkpoint. Ignore only bytes beyond the published frontier. Read-only
reopen should not truncate or create files; the next writer may overwrite or
truncate only that uncommitted suffix.

A normal transaction syncs journal bytes before staging, syncing and atomically
publishing CURRENT, then syncing its parent directory. This introduces an
additional small metadata publication compared with a single append; it must be
reported explicitly rather than weakening acknowledgement or claiming a timing
gain. Full checkpoints and fresh journals publish as a new generation before
CURRENT points to them. Reclaim covered generations only after that publication
is durable. Interrupted staging never authorizes fresh initialization over an
existing state entry. Ordinary writes never recreate disappeared directories.

Checkpoint rotation is due when journal bytes reach the greater of the previous
checkpoint's encoded size and 1 MiB. The next changed update is included directly
in the new checkpoint, avoiding a redundant append first. Explicit `persist()`
also checkpoints. This cadence amortizes history-sized work across journal bytes;
it is not a fixed operation count or a global retained-history limit. A single
large update can exceed the threshold. Checkpoint cloning and serialization
still cost memory and time proportional to retained history.

## Retention and trust boundaries

Preserve the current full logical snapshot. Ordered anchors reconstruct
checkpoint-to-head lineage on restart, and period payloads have historical
serving consumers. A request's 128-item bound is not a history-retention limit.
Only journal records covered by a durable persistence checkpoint are reclaimable;
a persistence checkpoint does not replace the weak-subjectivity trust checkpoint.
Authenticated obsolete-fork metadata lifetime remains separate because not all
of that in-memory map is represented in the persisted ordered anchors.

Checksums detect damage and inconsistent artifacts; a complete coherent local
filesystem rollback is outside that guarantee. No compatibility migration is
required, but previous state must be diagnosed and preserved rather than silently
reset. No live sync, production data, Mac mini work or benchmark campaign is part
of this milestone.

## Validation and evidence

Source `1b751e5a686fdc544ae58a45fa4ed7a3355fcafe` passes all eleven local gates:
vendor integrity, workspace and patched-vendor formatting, workspace check,
strict all-target Clippy, all-target tests, documentation checks and release
node build. Workspace tests pass 2,055 tests with zero failures and 24 existing
ignores across 37 targets. Eight documentation targets compile with zero doctest
examples. Focused consensus checks pass 388 tests with one existing ignore;
focused node checks pass 237 tests without ignores. All Cargo commands use the
locked offline dependency set with two build jobs. Existing dependency future-
compatibility warnings are retained in the logs.

Independent format, domain and node reviews found and closed candidate gaps
before publication: checkpoint-derived state is reconstructed before replay,
intermediate replay data is validated before later overwrites, prepared anchor
mutations reject before any commit, and historical anchor batches merge their
affected span once. Finite controls cover these, reference/reopen equivalence,
readers during saves, concurrent writers, failure latching, committed metadata
disagreement, incomplete tails, checkpoint rotation, cleanup and interrupted
whole-directory initialization. These are deterministic file-operation controls,
not physical power-cut or live-chain tests.

A same-state content witness uses the original snapshot encoding: 17 retained
anchors produce a 7,514-byte full snapshot, while the actual one-anchor delta
frame is 473 bytes. The new commit additionally publishes 152 bytes of CURRENT
and filesystem metadata. This demonstrates avoided serialization of unchanged
anchors; it is not an ingestion throughput or latency benchmark, and does not
measure physical write amplification. No CPU or memory contention benchmark ran.

The [baseline record](baselines/2026-09-18-consensus-history-journal.json) binds
the exact source inventory, all gate logs, test totals and publication status.
Its compressed evidence archive contains 27 files (SHA-256
`5a5bae18d146bf9f79bcfd35270d0fa113d9d35b80cecf48086149271c0519e9`);
its 14-file validation archive has SHA-256
`cde69f07220a78e32bb0b02fe84381820be6106910e47c11129bd7b5ae0e2e9d`.
Archive member contents, lengths and hashes were round-trip verified. Actual
original source copies and the finite encoding witness are retained; no claim is
made that the entire baseline workspace was rerun for this cost improvement.

Obsolete production snapshot mutation/writer code and its unused read wrapper
were removed. The original encoding write convenience remains test-only. Node
startup and `info` controls preserve older formats and incomplete native groups;
archival retains every file byte and the relocated group replays successfully.
No production/remote files, dependencies, unsafe code or SQL behavior changed.
Exact-head CI and merge remain.

All six CI jobs passed on `a13b29f6` and ten Linux volume/template controls passed with verified cleanup. [PR #234](https://github.com/tdenisenko/logex/pull/234) merged as `115ac554`. The merge tree is identical to the tested head. B3-69 is closed for incremental consensus persistence with periodic checkpoints. Required trust/serving history is retained. Aggregate networking admission, shared query resources, dashboard review, verified repair and integrated acceptance remain open.
