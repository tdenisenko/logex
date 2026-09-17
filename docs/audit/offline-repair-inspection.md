# Offline repair: primary-data inspection

This milestone adds the read-only storage inspection prerequisite for batch 11.
It does not implement `logex repair`, automatic repair, authenticated replacement
fetching, quarantine, index rebuilds or repaired-catalog publication. Those remain
explicit roadmap work.

## Why a separate inspection path is necessary

Ordinary `NativeStorage::open` initializes missing directories, may update
configuration, validates/replays recovery state, restores committed prefixes,
creates an active segment and completes interrupted reorgs. Those operations are
necessary during normal startup but cannot implement an immutable repair dry-run.
The existing startup integrity check validates metadata and row boundaries; it
intentionally does not scrub every stored payload on every restart.

The new inspection path shares the existing directory-inode ownership and
bounded catalog decoder. It requires existing storage and retains the exclusive
owner until its returned inspection is dropped. It performs no recovery, creates
no lock file and never publishes or removes artifacts. Immutability covers file
contents, directory entries and modification times; filesystem access-time updates
from reads are outside that guarantee. Normal storage and
background compaction ownership still conflict with inspection.

## What the report establishes

Inspection is limited to catalog-selected primary data. It distinguishes the
active hot segment, the appendable historical segment and completed sealed
segments. A sealed representation alone does not imply consensus finality or
that an active historical segment has stopped appending.

For each inspected source, it checks captured identity and addressing, canonical
bitmap structure, decoded logical row counts and extrema, and recomputes the
existing grouping-independent logical commitment in row order. An explicit
commitment match proves local stored content identity. It does not prove that
canonical flags describe the current chain, that every log in a block was
originally stored, or that a numeric block interval has complete coverage.
Sources without a published commitment cannot receive that same verdict.

Caller-selected per-segment row, retained-artifact and decoded-payload limits are
checked before large materialization. A limit or I/O failure produces an
incomplete inspection, never a corruption verdict or replacement authorization.
These are input/work bounds, not a process-wide RSS promise. Segments are scanned
serially; selected fixed metadata, captured raw buffers/page indexes and one
payload batch can coexist. The ordinary query and ingestion paths receive no new
admission limits.

Pending WAL, recovery metadata or a canonical reorg require the existing recovery
protocol to be resolved before a source verdict. Inspection preserves that
evidence and does not treat a journal checksum alone as proof of recoverability.
Derived indexes are explicitly not inspected by this primary-data milestone.
Unsupported artifact layouts remain actionable incomplete results.

## Repair prerequisites still open

Segment block bounds are extrema of rows, not complete-block ownership. Writers
can split a single block across segments. A future replacement planner must
account for inclusive/transitive overlap and preserve healthy fragments,
noncanonical rows and source semantics. It must not infer empty blocks or fill
unfetched historical/live gaps from absent rows.

The existing CL/EL validators can authenticate a finite range by starting at an
intact selected execution anchor at or above its end and verifying parent hashes
backwards. Existing normal sync methods additionally mutate progress and publish
live notifications, so repair needs a finite collector with separate output and
complete-block accounting, including zero-log blocks. Missing trust or peer
history is a blocker, not permission to reset data.

The node also needs a supervised maintenance phase before ordinary storage open,
read-only expected-volume preflight for dry-run, and repair health/status without
an opened normal query store. Existing expected-volume preparation creates
missing descendants and performs write probes, so it cannot be reused unchanged
for dry-run. No volume or service lifecycle change is part of this milestone.

## Validation and publication

Source `9d454ff9da3d957f1c1d012d6ad6ac300557ef11` on
`audit/offline-segment-repair`, based on PR #238 merge `77d03349`, passes 24
focused controls and independent review. All twelve local gates pass; exact-head
Linux/macOS CI and merge remain. No live
sync, production data, remote host or new benchmarking campaign is used.

## Frozen-source validation

All twelve local gates pass on `9d454ff9da3d957f1c1d012d6ad6ac300557ef11`: dashboard controls, vendor integrity,
workspace and patched-package formatting, workspace check, strict Clippy,
all-target tests, documentation tests and the release node build. There are
2,103 passing Rust tests, zero failures and 24 existing ignores across
37 targets. Documentation checks pass across 8 targets (0 examples).
The existing 36 dashboard controls also pass. Focused inspection passes 24/24;
the complete storage suite passes 377 tests with eight existing ignores.

The [machine-readable record](baselines/2026-09-18-offline-repair-inspection.json)
retains exact commands, source hashes and outcomes. Compressed
[review and draft-control evidence](baselines/2026-09-18-offline-repair-inspection-evidence.json.gz)
and [validation logs](baselines/2026-09-18-offline-repair-inspection-validation.json.gz)
contain individual file sizes and SHA-256 digests. Draft controls are distinguished
from original-repository behavior: two draft code gaps were corrected, and a
fixture that accidentally created an identified source was fixed without relaxing
production identity checks.

Exact-head CI, including both platforms and ten disposable Linux volume/template
controls with cleanup, remains required before merge. Automatic repair remains
unimplemented; this milestone does not establish release readiness.
