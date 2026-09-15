# Discovery identity persistence

Both consensus and execution discovery now use one bounded identity loader and
cooperating creation protocol. This changes startup and network restart only;
it adds no work to block ingestion, queries or periodic peer-cache saves.

## Findings

- **B3-60: creation did not preserve one durable identity.** Both old loaders
  checked for absence and then wrote directly to the final path. Concurrent
  creators could overwrite one another, and readers could observe an incomplete
  write. Neither file nor directory was synced before reporting success. This
  finding follows the old write ordering; no physical power-loss experiment is
  claimed.
- **B3-61: fixed-size identities used unbounded input reads.** Both loaders read
  the entire file before decoding a 32-byte key. The replacement consumes at most
  129 bytes, accepting at most 128 encoded bytes including surrounding whitespace
  and one optional `0x` prefix. Existing invalid contents are preserved.
- **B3-62: a networking restart could recreate a missing storage parent.** The
  loaders called `create_dir_all` themselves. The directory owners already
  initialize storage before discovery starts. Missing parents now produce an
  actionable I/O error. A bounded original-loader control reproduces the former
  behavior with an absent temporary directory.
- **B3-63: new identity permissions depended on the ambient creation mask.**
  New key staging files and the sidecar request private Unix permissions. Existing
  permissions are preserved. Filesystems such as ExFAT do not enforce individual
  POSIX permission bits; this is not a claim of per-file privacy on such volumes.

These are discovery identities, separate from trusted chain snapshots and from
regenerable peer hints. Invalid existing identities are never silently replaced.
This milestone does not complete expected-volume supervision or repair.

## Publication and scope

The shared helper lives in `logex-cl`, an existing dependency of `logex-sync`.
Each caller retains its own native key conversion. Validation uses a copy because
its existing ENR parser clears its successful input. No dependency, toolchain or
key encoding change is needed.

On an existing regular file, the helper performs bounded text and scalar
validation and syncs the parent directory before returning. It preserves existing
bytes and modes; it does not retroactively file-sync an externally imported key.
Ordinary symlink and nonregular final entries are rejected. Trusted initialized
parent paths are required; this is not a defense against concurrent hostile path
replacement or an entire data-directory lease.

For creation, a stable empty `<filename>.lock` sidecar is opened without
truncation and locked nonblockingly. It is never unlinked, so current cooperating
writers use the same inode. Contention returns an explicit retryable `WouldBlock`
error before generation. After acquiring the lock, the helper rechecks the final
entry, writes the complete key to unique same-directory staging, syncs that file,
rechecks the destination, atomically renames and syncs the parent before success.
A concurrent reader can see only a complete new publication and also syncs its
parent before returning. A post-rename sync error preserves the published key and
returns failure; a later startup can validate the same identity.

Every current LogEx creator follows this protocol. External writers and older
clients ignoring the lock are outside its serialization guarantee, consistent
with the approved absence of backward compatibility. Crashes may leave an
unpublished uniquely named temporary file; it is not loaded as an identity. The
stable lock is intentional metadata and must not be removed while writers run.

## Platform validation and cost

The native exclusive-rename approach was rejected after an isolated macOS ExFAT
probe returned `ENOTSUP`. A second probe on the same disposable image confirmed
nonblocking lock contention, lock handoff, stable inode identity, and ordinary
rename with file and directory sync. This validates the selected primitives on
macOS 15.7.9; it does not run the full Rust helper on ExFAT or simulate physical
power loss. Full Rust tests run locally and in Linux/macOS CI.

The probes used fixed ordinary fixture bytes and a dedicated temporary directory
on mac-mini. Its 512 MiB image was detached and the entire exact-inventory folder
was removed afterward (536,902,963 file bytes). No physical external-volume data,
existing identities or unrelated process was used. Earlier obsolete audit outputs
remain cleaned up; unique reports and useful build outputs are retained.

An initial 128 MiB image creation failed with a generic operating-system error;
the documented 512 MiB recipe succeeded. The generic error is not attributed to a
security policy. An initial filename test also hit macOS rejection of invalid
UTF-8 filenames; lossless path construction is now tested on Unix, with actual
invalid-byte filename I/O covered on Linux.

The startup protocol adds one nonblocking lock for creation and the required
initial durability barriers. Existing identity reads have a fixed small buffer
and one parent sync. It adds no ingestion locks or writes. No benchmark or
throughput percentage is claimed.

## Validation

Small tests cover creation/reopen, four simultaneous creators, held-lock retry,
invalid key and lock preservation, read limits, missing parents, file types,
private creation and existing modes, filename bytes, and native CL/EL conversion.
The original EL loader is substituted into the new missing-parent test, and the
candidate source is restored byte-for-byte afterward. This is an original-method
control, not a complete baseline checkout. Independent review covers both callers,
parent initialization and the publication protocol.

The focused consensus suite passes 342 tests with one existing ignored test.
Source `b966f252` passes all seven local gates: vendor verification,
formatting, workspace check, strict Clippy, 1,427 workspace tests
(23 ignored), documentation tests and the release node build. All six CI jobs passed in run `34996540273` on head `b08f040e`.
[PR #173](https://github.com/tdenisenko/logex/pull/173) merged as `558bef69`
after exact head/base verification. See the accompanying
[validation record](baselines/2026-09-15-discovery-identity-persistence.json).

## Remaining scope

Broader CL history lifetime, EL peer-hint persistence and request management,
volume supervision, offline verified repair and integrated acceptance remain in
the audit ledger. Passing this milestone does not establish release readiness.
