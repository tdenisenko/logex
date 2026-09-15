# Execution peer-cache persistence

The execution peer cache stores optional restart hints. It is separate from the
discovery identity and authenticated chain state. This milestone bounds cache
loading, preserves damaged originals and gives concurrent saves independent
staging files, while retaining best-effort power-loss semantics for these hints.

## Findings and behavior

- **B4-01: bounded writers did not imply bounded readers.** The manager exported
  at most 512 peers, but the loader read an entire file and decoded any number of
  records. Loading now consumes at most 1 MiB plus one detection byte and decodes
  at most 512 records. The manager and storage helper share that record limit.
- **B4-02: storage errors were confused with an absent cache.** `Path::exists`
  hid some filesystem errors, and runtime then discarded all other loader errors.
  An ordinary regular file used as a parent component reproduces the former case.
  A genuinely absent cache is still valid; real read or preservation failures now
  stop startup with a path-specific diagnostic.
- **B4-03: damaged hints were not preserved before replacement.** Invalid JSON,
  excess bytes or excess records are now moved into a unique adjacent quarantine
  directory before returning an empty cache. A failed preservation operation
  returns an error. The original remains available for inspection; later normal
  discovery can write new hints without overwriting that evidence.
- **B4-04: every save shared `known-peers.json.tmp`.** Writers could interfere
  through that one staging path. Each save now owns a unique temporary file,
  writes one complete bounded snapshot and atomically replaces the cache. The
  last successful replacement wins; these replaceable hints are not an append
  log or a concurrent update-merging API. Saves require the initialized storage
  parent and never recreate a missing directory.
- **B4-05: special or dangling peer-cache entries could stall or disappear from
  diagnostics.** Both CL and EL opened the cache before checking its type. A
  special entry could block startup; a dangling link looked like a missing file.
  Both now reject nonregular entries before opening and verify the opened handle
  is regular. Ordinary link and dangling-link fixtures preserve those entries
  and their target. This is not protection against concurrent hostile path swaps.

The node opens its storage owner before peer loading or saving. The new startup
error behavior applies only to actual I/O/preservation failures: recoverable
malformed hints are handled by the loader after preservation. Periodic save
failures still log and retain the previous in-memory saved marker so a later
changed-cache attempt can retry. Shutdown retains its existing best-effort save.

## Implementation and cost

Use the existing serde sequence visitor pattern to stop record materialization at
512 and verify complete JSON consumption. Node records contain fixed-size peer
IDs, IP addresses and ports; there is no arbitrary persisted ENR string in this
format. The byte limit also bounds input retained for decoding. This does not
establish a process-RSS or total networking memory budget.

Promote the already locked tempfile package to a normal dependency and add the
existing workspace serde dependency directly. No new package version, toolchain,
unsafe code or handwritten temporary-file/JSON machinery is introduced. The
existing unchanged-cache comparison remains in front of serialization and I/O.
New staging preserves atomic visibility without adding periodic fsync barriers.
No benchmark or ingestion-throughput percentage is claimed.

Quarantine preserves files during normal operation, with the same best-effort
power-loss contract as the derived cache. It is not the verified segment repair
coordinator and does not establish expected-volume identity. Normal node startup
owns the data directory; concurrent unsupported maintenance/path replacement is
outside the cache-loader protocol. Multiple replacement writers may safely use
independent staging files.

## Validation

Small deterministic fixtures exercise absent and invalid paths, preservation,
record/byte boundaries, concurrent complete snapshots, failed publication, saved
marker retry behavior and initialized-parent requirements. The original EL reader and writer methods fail the new parent-error and
preexisting-staging regressions respectively; candidate source was restored
byte-for-byte after each control. Independent review found the additional entry
check above, which was corrected in both loaders. The original CL loader also fails the new ordinary-link regression; its source
was restored exactly afterward. Final independent review passes, as do 11 execution
persistence tests and all 343 consensus tests (one existing ignored test). Source `3dfd553a` passes all seven local gates: vendor verification,
formatting, workspace check, strict Clippy, 1,435 workspace tests
(23 ignored), documentation tests and release node linking. All six CI jobs passed in run `34999012759` on head `77bf70b6`.
[PR #174](https://github.com/tdenisenko/logex/pull/174) merged as `556b988c`
after exact head/base verification.

[Validation record](baselines/2026-09-15-execution-peer-persistence.json).

## Remaining scope

The live `known_peers` inventory can grow independently of the persisted and
productive limits, and reseeding clones it. Its lifetime and scheduling policy
remain a separate explicit EL review item; this file-reader bound does not close
that lead. Request ownership, peer rehabilitation, volume supervision, verified
offline repair and integrated acceptance also remain open. No remote operation,
live sync or benchmark is part of this milestone.
