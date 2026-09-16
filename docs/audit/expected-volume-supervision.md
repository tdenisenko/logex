# Expected-volume supervision

## Findings and scope

**B10-16 — high operational risk: the node had no expected-volume identity or
anchored data namespace.** A configured pathname and free-space probe cannot
establish which volume is mounted there, or prevent a later pathname resolution
from reaching an underlying directory after detach. This is the agreed missing
operational capability, established by source inspection. It is not a claim that
an actual user's directory was overwritten. Disposable mount controls demonstrate
the new protection, including a pre-existing underlay.

**B10-17 — moderate: fatal runtime failure did not close query admission.**
The supervisor's failure callback was empty; health and queries could continue
waiting on the storage lock during cleanup. Source review established this gap.
New controls retain a storage write lock while checking that failure is reported
without waiting for it. This includes failures in non-storage workers: the reason
identifies the terminal failure, while storage-backed work becomes unavailable.

**B10-18 — moderate: the consensus peer-cache writer recreated missing parents.**
An owned missing-parent control fails against the unchanged production file at
`0b611c2d`: the writer succeeds and creates the unavailable storage parent.
The same operation now fails without recreating it. Four older unit fixtures now
explicitly initialize their parents, as the production consensus-store owner
already does. Derived hints retain atomic publication without new fsync barriers.

## Implementation

Two optional global CLI/config values identify an absolute mount and stable
filesystem UUID. Both are required when either is configured, with normal explicit
CLI precedence. Before opening storage, preflight checks identity, mount location,
10 GiB of available space and actual write access. New directory components are
created relative to verified open descriptors, with no-follow opens and same-device
checks. Existing data entries must be regular files or directories on that device;
symlinks and cross-filesystem descendants are refused. The trusted data-directory
model still excludes concurrent edits or mount changes inside the tree by other
programs; this is not a general isolation boundary against its owner.

The standalone executable pins its working directory to the opened data directory
before starting database/runtime workers. All database roots then use `.` and the
working directory stays pinned through process exit. Absolute configured paths
are subsequently used only for observation. This prevents writes from resolving
through a replacement mount path between periodic checks. Existing relative
checkpoint descriptor paths are resolved before changing directory, within the
preflight deadline. Config loading already precedes preflight.

macOS uses `fgetattrlist` UUID attributes and `fstatfs` on the opened mount. It
validates returned lengths, attribute masks and nonzero UUIDs. Linux obtains the
opened descriptor's mount ID from fdinfo, matches its exact mountinfo record and
`st_dev`, and resolves the configured UUID to the mount source's block device via
`/dev/disk/by-uuid`. The parser preserves path bytes, decodes kernel escapes and
rejects incomplete/ambiguous records. Unsupported or unresolvable sources fail
explicitly. Overlay/network filesystems and ambiguous multi-device layouts are not
promised support. Filesystem UUIDs must be unique among attached volumes.

Pinned tempfile 3.27.0 converts relative staging parents to absolute paths, which
would undo this protection. A small `logex-fs` crate now owns exclusive private
staging files/directories without changing the caller's path namespace. Existing
storage replacement code already preserves relative paths. CL snapshots and
identity, CL/EL peer hints and quarantine, startup policy and consensus archives
use the shared staging helper. Publication keeps each caller's existing durability
barriers. Archive drop removes only an empty scratch directory, preserving any
original artifact already moved into it. Production tempfile dependencies become
dev-only in CL, sync and node; the lockfile adds no external package/version.

Preflight has an independent 180-second deadline. An owned thread monitors volume
availability through startup, runtime destruction and offline commands, with an
independent 10-second deadline for each probe. It retains its 10-second interval,
checking identity, configured directory identity, free space, read-only state and
a tiny write/delete operation. A missing or replacement mount never becomes a new
storage root. Permission or write failure is terminal. The existing independent
180-second whole-node cleanup deadline remains in force. Volume failure also
arms an independent terminal deadline before invoking the notification callback.
The callback wakes the supervisor and closes query admission directly, without
requiring the engine to yield. A blocked OS call cannot be cooperatively canceled;
this deadline bounds terminal volume failure even if the engine poll is stuck.
Main retains monitor ownership through runtime and monitor teardown, both inside
the ordinary-shutdown deadline. The callback holds only a weak application-state
reference, so it does not extend storage lifetime through normal shutdown.

Fatal runtime failure latches the first reason, closes query admission and cancels
owned work. REST health/status return 503 without taking the storage lock or
refreshing filesystem metrics. JSON-RPC storage methods return an explicit error;
metadata-only methods remain available. gRPC returns unavailable and buffered
streams check failure before yielding entries. SQL execution races the failure
notification and retains cooperative cancellation; native scans check between
filesystem operations. WebSocket work closes and its session guard detaches on
future cancellation. Successful query semantics and pagination are unchanged.

## Validation and evidence

The [ledger](baselines/2026-09-17-expected-volume-supervision.json) records original
and candidate evidence and exact source inventories. Focused local controls cover
private staging, publication failure/cleanup, retained archives, an owned parent
rename and replacement, configuration precedence, UUID/path decoding, missing and
wrong mounts, low space, aliases, permission loss, and checkpoint path preservation.
The directory/mount-identity unit controls inject identity observations in child
processes; they are not represented as real mount validation. New monitor children
cover ordinary stop, probe failure, a stalled probe and unwind without terminating
the test harness. Query controls cover admission, pending locks/execution,
cancellation ownership, failure reason retention and buffered stream errors.

Nine real ExFAT lifecycle cases passed on an isolated Intel mac-mini using new
512 MiB and 16 GiB disk images. They covered missing/wrong identity, low space,
normal preflight/write, forced detach, an existing unmounted path, a different
volume at the same mount, preservation of both underlay and replacement contents,
and correct-volume remount/restart. The fixture builds the actual volume/staging
components and starts no network client or full node. The OS rejected the initial
SPARSE-image recipe; UDIF image creation succeeded. Initial controller assumptions
about partition layout and attach metadata were corrected, with failed runs kept
in evidence; final detach uses verified image/device associations.

After those runs, review extended the common preflight wrapper to cover checkpoint
resolution and added independent lifetime-wide monitoring, plus tests and a safety comment. The
macOS identity implementation, staging implementation and production Unix I/O
sequence are unchanged from the tested source. This distinction is retained in
the inventory instead of claiming the entire final node ran on those images.
Both images were detached, evidence copied and hash-verified, and the exact owned
remote directory removed: 933 files / 17,739,526,958 bytes. No physical external
volume, unrelated files or other processes were changed. Mac-mini work is done.

Linux CI additionally builds the same fixture driver and runs disposable ext4 loop
images through equivalent lifecycle cases, including lazy detach. It verifies
actual loop attachments before cleanup; only new owned regular image files are
formatted. CI also validates the systemd template without installation. Initial local gates
passed on `099ca405` (1,827 tests / 24 ignored). Initial CI on `e6c0b08a` passed
five jobs and all ten Linux mount/template controls, but failed fixture cleanup:
the cleanup required literal absolute UUID symlink targets, while device-manager
links may use relative targets. Cleanup now verifies the resolved owned device,
always attempts owned-image detach, and retains diagnostic tracebacks. The initial
report explicitly records incomplete cleanup; it is not treated as passing CI.

Final review also reproduced a same-poll supervision gap on `e6c0b08a`: a bounded
synchronous engine operation prevents its sibling async health future from
running. Actual ingestion, historical metadata and reorg writes contain synchronous
I/O in that engine. Expected-volume mode now retains independent monitoring for
the whole process lifetime, instead of handing it to that async future. Owned
controls demonstrate notification while a current-thread runtime is blocked,
first-reason retention, a blocked callback's terminal deadline and nonzero exit
even if teardown begins first. A cleanup regression covers remaining owner teardown
inside the existing watchdog. The ordinary unconfigured health path still uses
its existing async timer; that broader runtime finding remains on the roadmap.
All eight local gates pass on corrected source `de1a42f2`: vendor verification,
workspace/patched-vendor formatting, check, strict Clippy, 1,829 workspace tests
(24 ignored), documentation tests and release build. New exact-head CI and merge
remain pending.

The launchd plist passed syntax validation and its fields were checked against
the installed platform manual. Both templates restart with backoff and logs outside
the removable volume, so every new process repeats preflight. The launchd template
uses the standard process class and private creation mask. Neither service was
installed, and no live sync was started.

No benchmark or throughput claim is made. This adds startup metadata traversal,
periodic tiny probes, and query-admission/cancellation bookkeeping. It adds no
per-block writes, new ingestion flushes, storage encoding changes or extra
periodic full-device barriers. Broader query memory, remaining runtime review,
verified offline segment repair and integrated/live acceptance remain separate.

## Cleanup and references

Removed superseded absolute-converting staging paths, the CL writer's parent
creation, duplicate manual WebSocket detach calls and obsolete direct storage-lock
admission paths. Retained existing format/durability logic and caller ownership.
All newly added FFI calls have local safety contracts. No independent review is
claimed; implementation and review were performed within this task.

- [Linux kernel proc documentation](https://docs.kernel.org/filesystems/proc.html):
  exact fdinfo mount IDs and mountinfo record meanings.
- [Apple launchd source manual](https://github.com/apple-oss-distributions/launchd/blob/main/man/launchd.plist.5)
  and the installed macOS `launchd.plist(5)`, `getattrlist(2)`, `chdir(2)` manuals.
- [systemd service documentation source](https://github.com/systemd/systemd/blob/main/man/systemd.service.xml):
  restart and shutdown options; executable preflight supplies the identity check.
- [Operator templates and recovery steps](../../deploy/README.md).
