# Consensus snapshot integrity

The trusted consensus snapshot now carries an explicit format, exact payload length
and SHA-256 checksum. Reopen checks the envelope before restoring or publishing
trusted state. This closes accidental corruption that preserves valid JSON and
the existing structural invariants. It does not complete the remaining retention,
network or offline audit work.

## Findings and scope

| ID | Severity / classification | Evidence and resulting behavior |
| --- | --- | --- |
| B3-52 | P1, trusted-state integrity | Changing one hexadecimal digit in an ordered anchor's receipt root retained valid JSON, length and metadata shape. The original reader accepted that altered trusted record. The framed reader rejects it before restoration and leaves the original bytes unchanged. |
| B3-53 | P2, error reporting | `info` discarded consensus reopen errors through `.ok()`, making unreadable or invalid state indistinguishable from absent state. Shared fallible presence checks and explicit open errors now stop the command with an error. |
| B3-54 | P2, archive preservation | The stale-checkpoint archive used an existence check followed by rename to a timestamp candidate, with no directory durability barriers. A uniquely owned archive directory removes the competing destination-name window. Ordered directory syncs preserve the archive publication sequence. This finding is supported by source review; physical power-loss behavior is not claimed as experimentally reproduced. |

The new-format startup and archive path changes are required integration, not
evidence that the original runtime missed the original filename. Metadata errors
and dangling entries must also remain distinct from a fresh directory. The shared
presence helper checks native and legacy entries using `symlink_metadata`; only
NotFound means absent. Actual opening still determines validity.

## Format and publication

The native path is `cl/consensus_state.bin`. Its fixed 48-byte header contains:

| Bytes | Meaning |
| --- | --- |
| 0–7 | Exact magic/version `LXCLSN01` |
| 8–15 | Little-endian u64 JSON payload byte length |
| 16–47 | SHA-256 of every payload byte, including whitespace |
| 48 onward | The existing pretty-printed `ConsensusSnapshot` JSON schema |

The writer streams one serialization through a buffered counting/hash adapter
into a new empty staging file, then seeks back to fill the header. The adapter
counts and hashes only successfully written bytes, including partial writes.
Buffering outside the adapter batches serde's small writes. The established
file-sync, atomic replacement and parent-directory-sync sequence remains intact;
there is no additional normal-save durability barrier, full-file buffer, second
serialization or separate payload hashing pass. Actual changed snapshots still
clone and rewrite retained history; this milestone does not solve that cost.

The reader obtains metadata from the same opened file, checks magic and exact
length before decoding, and streams bounded reads through the hash adapter. It
requires complete payload consumption, an equal digest and underlying EOF before
restoring or reconciling the snapshot. A final read detects appended bytes after
the initial length check. Real I/O errors remain read errors; malformed data and
unexpected EOF become parse errors. Existing structural validation, cached-payload
checks, serialized writers, publication ordering and permanent save-failure latch
remain in force.

The checksum detects accidental damage. It is not local-file authentication or
rollback prevention. JSON is parsed before digest comparison but remains private;
this does not add a total memory bound or cap legitimately retained history.
Arbitrary concurrent external coherent file rewrites are outside this guarantee.
Hashing adds CPU work proportional to snapshot bytes; no timing percentage or
ingestion speed guarantee is inferred. No benchmark campaign was reopened.

## Startup and preserved artifacts

The user waived backward compatibility and migrations. A legacy-only snapshot
therefore produces a diagnostic requiring a recent checkpoint in a fresh data
directory, even when an explicit checkpoint is supplied. No legacy artifact is
converted, removed or replaced. If both files exist, the native file is
authoritative and the legacy file remains untouched; invalid native data never
falls back to legacy data.

Normal sync and `info` use the shared presence/open path. Only the existing exact
stale-checkpoint conditions trigger automatic checkpoint refresh. Integrity,
legacy and filesystem errors remain failures rather than reset requests.

For stale refresh, create a uniquely owned sibling archive directory and sync its
parent before moving the native snapshot into it. Keep the directory immediately
after a successful rename, then sync the archive directory and parent. Pre-move
errors leave the original name intact; later reported sync errors retain the
moved original. These barriers affect the rare archive operation only. The
already-pinned `tempfile` dependency moves from node tests to production use;
there is no new package/version or lockfile update.

## Validation and cleanup

The isolated original-reader regression fails because it accepts the changed
ordered record. It is the original CL reader/writer plus the new bounded test,
not a complete baseline checkout. An earlier draft changed a derived summary,
which restoration recomputes; that passing draft is retained separately and is
not evidence for B3-52.

Small offline controls cover exact framing/digest/whitespace, short headers,
invalid length/version/checksum, truncation, appended bytes, changing length after
metadata, partial reads/writes and staging write/seek errors. Existing malformed
semantic fixtures are now correctly sealed so they continue to exercise structural
and cached-payload validation after envelope checks. Node controls cover native
reopen, legacy preservation with and without an explicit checkpoint, real metadata
errors, dangling native/legacy entries, `info` errors and archive byte preservation
followed by fresh-checkpoint reopen. No production data or remote host was used.

The focused suites pass 310 consensus tests (one ignored) and 107 node tests.
The first sandboxed node run could not bind 11 existing local fixture servers;
the authorized rerun passed all 107. The final expanded dangling-entry control
and strict node Clippy also pass. Independent final CL and node reviews are clear.
Removed duplicate hard-coded JSON paths, discarded open errors and the timestamp
archive retry loop. Existing JSON internals and deliberate legacy-detection tests
remain necessary.

Source `6d7371a2` passes all seven local gates: vendored dependency integrity,
formatting, workspace check, strict Clippy, 1,393 workspace tests (23 ignored),
documentation tests and the release node build. All six Linux/macOS CI jobs passed in run `34982691191` on head `5508b7c0`.
PR #170 merged as `9a57c22c` after exact head/base verification.
Full workspace and CI acceptance are recorded in the
[validation record](baselines/2026-09-15-consensus-snapshot-integrity.json).
