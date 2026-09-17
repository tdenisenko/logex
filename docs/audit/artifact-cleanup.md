# Audit artifact cleanup — 15 September 2026

The audit owner requested removal of obsolete temporary files and LogEx build
outputs on the development machine and Mac mini. This pass changes documentation
only. It does not change ingestion, storage, queries or runtime behavior.

## Scope and preservation

Cleanup used an explicit inventory of LogEx-owned paths. Before removal, checks
covered ownership, symlinks, open files, Cargo locks and mounted test images as
applicable. Main workspace outputs were cleaned with Cargo. Temporary build trees,
compiled benchmark/test captures and detached disposable images were removed only
after confirming they were no longer in use. Binary identities were recorded first.

Sources, scripts, unique reports, measurements, patches and current audit evidence
remain available. The current release executable was copied and its SHA256 checked
before Cargo cleanup. It remains in the protected cleanup evidence directory.
Twenty-eight local tar archives were compressed losslessly: each replacement's
complete decompressed SHA256 matched the original before the original was removed.
Their contents were not filtered or changed.

Unrelated temporary files, other projects, application processes, production data
and the physical external volume were outside the cleanup. No process was stopped,
service restarted or live ingestion workload started. The cleanup is not a general
permission to remove everything under `/tmp`.

## Local results

- Cleaned obsolete workspace debug and release outputs with Cargo, retaining
  useful dependency caches and the current release executable.
- Removed 12 old temporary target trees and 496 compiled standalone captures.
- Removed one detached, completed test image; retained its test results.
- Replaced 28 tar archives with verified gzip copies.
- Observed approximately **148.3 GiB** more free space across the four cleanup steps.

The reported gain uses before/after filesystem free space. It is approximate:
APFS sharing and concurrent machine activity mean summed logical file sizes and
Cargo's reported removal size are not the amount of physical space recovered.

Final local verification confirmed all 509 selected deleted paths were absent,
all 28 compressed replacements existed with the recorded sizes, the preserved
release executable matched its recorded SHA256, and PR #162's gate evidence remained.
Full archive-content checks ran before original-file removal; the final verification
did not repeat those already successful checks.

## Mac mini results

- Pruned 12 obsolete build roots, retaining 19 small reports at their original
  paths (604 KiB of allocated space).
- Removed 55 captured executables and five detached disposable test images.
- Removed 7,863 registry source copies, 7,863 crate archives and 10 Git checkouts
  from ten obsolete temporary Cargo homes. Package content matched archives whose
  SHA256 matched retained lockfiles; Git checkouts were clean at locked revisions.
- Retained 150 package entries without sufficient lock/archive proof, plus bare
  Git databases, index/configuration metadata and all unlisted contents.
- Observed approximately **15.0 GiB** more free space across the two removal steps.

Only the explicitly inventoried LogEx audit roots under `/private/tmp` were in
scope. Final checks found no matching open files, mounted descendants, attached
images or temporary audit jobs. All remote cleanup commands exited. No physical
external volume or production directory was modified.

## Evidence and validation

The local receipt directory is
`/private/tmp/logex-artifact-cleanup-20260915.d4guhroa`.
It retains the inventories, commands, preflight checks, deletion identities,
compression receipts, free-space observations and current executable. Compact
results and receipt hashes are recorded in the [tracked baseline](baselines/2026-09-15-artifact-cleanup.json).
Historical references to a deleted compiled capture identify its original run;
source, commands and measurements remain retained. Archives listed in the cleanup
receipt now use a `.gz` suffix and recover their original bytes by decompression.

PR #162's implementation is unchanged by this documentation pass. Its seven local
gates passed on source `2556fad6` (1,327 workspace tests passed, 23 ignored), and all
six Linux/macOS CI jobs passed at `c68a8829` before merge `6e1bb320`.
The cleanup pass checks formatting, vendored-source integrity, JSON records,
document links and unchanged non-documentation content. It does not rebuild the
same locally validated code merely to recreate deleted outputs. [PR #163](https://github.com/tdenisenko/logex/pull/163) passed all six configured
Linux/macOS CI jobs at head `04b09053` in run `34958538490` before merge
`0d5bec37`; see the [CI and merge record](baselines/2026-09-15-artifact-cleanup-ci.json).

This pass neither completes the offline audit nor establishes live-sync readiness.
The remaining batch dispositions, volume supervision, offline repair and subsequent
live/staging acceptance remain tracked in the audit ledger and local roadmap.

## September 17 incremental compiler cache cleanup

During the sync-state review, local free space fell to approximately 11.4 GiB.
After all active Rust checks released their build ownership, the cleanup removed
764 older directories under this repository's `target/debug/incremental` only.
Four recently modified cache directories per crate were retained, and five
candidates whose contents had changed since inventory were skipped. Sources,
dependency outputs, release binaries, current audit evidence and unrelated files
were retained. No remote host was accessed.

Filesystem available space increased by approximately 41.7 GiB, to 53.1 GiB.
This is a before/after free-space observation, not a sum of file references; APFS
sharing and concurrent machine activity affect the exact physical gain. The
per-directory receipt is retained with the
[sync-state review evidence](sync-state-review.md).

## September 17 obsolete test compilation units

After the historical bounds source passed all ten local gates, Cargo was idle.
The cleanup removed 451 older generated test compilation units under this
repository's `target/debug/build/logex-*` directories, including each selected
unit's own fingerprint and outputs. Normal library/build-script units and
dependency caches were excluded. The inventory retained all 38 units referenced
by current audit logs, at least two newest cached and runnable variants per test
target, and 108 units in total.

Every selected path had a known generated test fingerprint and output naming
pattern. The cleanup checked all path/file metadata again before removal, rejected
links/unknown structures, checked compiler idleness, and verified retained paths
and content metadata afterward. All 451 candidates were removed; none changed
and required skipping. Sources, release binaries, current test binaries, evidence
and unrelated files remain. No remote host was accessed.

Available filesystem space increased by approximately **64.9 GiB**, from 44.1 GiB
to 109.0 GiB. This is an observed before/after change, not a sum of hardlinked
file sizes. Reusing an old removed test configuration may require recompilation;
its complete freshness state was removed with its generated outputs. The
[tracked receipt](baselines/2026-09-17-test-build-cleanup.json) records exact removed
paths, retained counts, policy and full local inventory/script hashes.
