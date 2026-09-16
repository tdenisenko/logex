# Startup sync-mode persistence

## Findings

**B10-13 — moderate: startup policy publication lacked durability barriers.**
`sync-mode.json` records a forward-only directory's policy. The original writer
used truncating `fs::write`; its remover unlinked without synchronizing the
directory. An interrupted write could leave incomplete JSON, and an unpersisted
write/removal could leave restart policy inconsistent with later storage work.
This durability gap is established by the original call sequence; no physical
power-loss reproduction or demonstrated log-data loss is claimed.

**B10-14 — moderate: unavailable storage could look like an absent marker.**
Reads returned `None` and removal returned success for a missing data directory.
Writing called `create_dir_all`, recreating that missing initialized directory.
Three controls fail against unchanged production at `c63d9c27`. They use an absent
directory beneath an owned temporary root, not an actual detached volume.

**B10-15 — moderate: marker input was unbounded and followed file aliases.**
The original loader reads the whole file before parsing. A 4,097-byte fixture
demonstrates the missing new bound without attempting memory exhaustion. Another
original control shows the writer following an ordinary marker symlink and
replacing its target's contents. These are local persisted-file checks, not claims
about remotely supplied input or protection against concurrent hostile mutation.

## Implementation and invariants

The private runtime persistence module keeps the existing one-boolean JSON shape
and filename. It requires an existing directory and a regular final marker entry,
rejects unknown JSON fields and reads at most 4,097 bytes before enforcing the
4,096-byte limit. Metadata supplies an early size check; a limited read also
covers growth after that check. Invalid and unexpected artifacts remain untouched.
Directory aliases are not generally prohibited; final marker aliases are rejected.

The writer creates an owned temporary file in the initialized parent, writes the
complete serialization, synchronizes the file, atomically replaces the marker and
synchronizes its parent. It never creates storage directories. Removal also
synchronizes the parent, including a retried already-absent marker. Reopening a
valid marker synchronizes its file and name before accepting it; reopening an
absent marker synchronizes its parent. This hardens an observed result after an
earlier uncertain synchronization error rather than assuming a visible result
was durably committed. Errors propagate to startup; a post-publication error
does not roll back or delete a complete new marker.

The normal node already holds the storage directory's exclusive lock during
mode resolution. Its policy state machine is unchanged: only fresh storage can
initially disable history, a marked directory can resume with the flag, and
omitting the flag converts it to normal backfill. Existing tests exercise these
paths with actual storage. Ordinary returned failures clean the operation's own
temporary file; unrelated/stale staging files are never promoted or removed.

Opened parent descriptors provide the synchronization target, not comprehensive
path anchoring. This change does not establish mounted-volume identity or prevent
all concurrent path-replacement races. Expected-volume protection remains a
separate prerequisite before making that promise.

## Validation, cost and cleanup

Original runtime production was byte-identical except for adding a test-module
declaration. Five controls fail and two ordinary-behavior controls pass. All seven
pass after the fix. The final sixteen controls also cover valid data at the size
limit; malformed and unexpected entries; alias preservation; retained staging
artifacts; five write interruption points for both first publication and
replacement; three removal interruption points; retry; and an actual rename
failure caused by an owned occupied destination. All 188 node tests pass.

The finite error callbacks run the actual I/O sequence and return ordinary test
errors. Production supplies a no-op callback; there is no global fault state or
environment switch. These tests check complete visible state, retained errors,
cleanup and retry. They do not simulate kernel/power-loss durability or a real
volume detach. No user data, remote Mac, public listener or live sync is involved.

Removed the old unbounded reader, truncating writer, missing-parent creation and
unsynchronized remover from `runtime.rs`, along with their obsolete imports and
state definition. The dedicated module owns the persistence contract; the policy
decision remains in runtime. Implementer review traced startup ownership,
publication ordering, error propagation, retries and the unchanged mode decision.
No independent review is claimed.

All eight local gates pass on `37b8b7f3`: vendor integrity, workspace and
patched-vendor formatting, check, strict Clippy, 1,781 workspace tests (24 ignored),
documentation tests and release build. All six CI jobs passed on `20dbfa4a`;
[PR #206](https://github.com/tdenisenko/logex/pull/206) merged as `0b611c2d`. Evidence is recorded in the
[validation ledger](baselines/2026-09-17-sync-mode-persistence.json). This introduces
startup-only synchronization, with no per-block writes, changed ingestion batches,
query work or dependencies. No benchmark or throughput claim is made. Broader
offline audit, volume supervision and verified repair remain open.

## References

- [Pinned tempfile 3.27.0 publication contract](https://docs.rs/tempfile/3.27.0/tempfile/struct.NamedTempFile.html#method.persist): atomic replacement requires separate file and directory synchronization.
- Existing consensus snapshot and discovery identity persistence use the same
  file-sync, replace, parent-sync ordering; their code was inspected.
