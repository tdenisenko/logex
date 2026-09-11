# Sparse bundle page coalescing

This document retains the diagnostic history and the subsequently integrated
coordinator. Current source is a0ed88c1 (catalog 11 / segment manifest 9 / bundle 5 /
index checkpoint 2); final performance/platform acceptance remains in progress.

The initial follow-up to LZ4 candidate `6156ef33` measured whether page coalescing
could help. LZ4 reduces
metadata, but a sparse segment still contains hundreds of tiny compressed pages.
This isolated experiment measures whether rebuilding the same rows into full
pages can address the remaining space/read gap. It is **not** a replacement
publication, reclamation or automatic maintenance implementation.

## Fixture and scope

An isolated release library test constructs one historical segment through
one-row calls, with every sixteenth block empty, then takes a durable checkpoint
and makes one row noncanonical. Four profiles contain 60, 240, 960 and 1,920 logs.
Their synthetic hashes/topics/four-byte payload differ from the main transfer
benchmark; compare only within this diagnostic. The original source stays intact.

For each profile, five replacement builds materialize all original rows and
canonical bits, write a new bundle in a separate temporary directory, preserve the
canonical flags and durably publish its manifest. Each read opens the manifest
and materializes all rows/flags. Old/new read order alternates; independent expected
rows and every canonical bit are checked outside the recorded read interval.
Existing captured readers also retain exact source rows and unchanged file size.

These are internal ARM APFS warm-cache measurements without concurrent local
build/test work during execution. Bundle sizes exclude catalog and other files.
The release library test retains the disabled cfg(test) durability hook's thread-
local check; fault injection/event recording is not enabled. Source, executable
hash, toolchain, machine, runner and all individual samples are recorded.
[Complete source and measurements](baselines/2026-09-11-bundle-repack-feasibility.jsonl).

| Rows | Source bundle bytes | Repacked bundle median bytes | Source read median | Repacked read median | Replacement build median |
| --- | ---: | ---: | ---: | ---: | ---: |
| 60 | 57,408 | 3,147 | 0.890 ms | 0.146 ms | 7.149 ms |
| 240 | 260,344 | 9,601 | 2.173 ms | 0.167 ms | 8.014 ms |
| 960 | 1,140,569 | 35,391 | 8.454 ms | 0.427 ms | 15.265 ms |
| 1,920 | 2,353,746 | 69,980 | 16.663 ms | 0.752 ms | 23.198 ms |

The 960-row case uses about 97% fewer bundle bytes and reads about 95% faster.
This supports pursuing page coalescing, but the measured build cost excludes
catalog publication, source conflict checks, index refresh, retirement of old
files and interference with ongoing ingestion. Five builds of one source per
profile do not establish tail latency, production sync cost or platform acceptance.

## Required publication work

The fixed columns/segment.bundle pathname cannot be overwritten or retired while
an older authoritative catalog may reference it. A safe implementation must keep
old and replacement artifacts separately addressable, publish the exact replacement
through the authoritative catalog, and retire only artifacts no longer needed by
recovery or captured readers. The existing segment generation field is currently
initialized to zero; it can support this ownership review without assuming that
its present path semantics already implement generation-aware storage.

Before integration, cover interruption before/after every publication and cleanup
phase, appends/reorgs concurrent with preparation, unchanged coverage including
empty blocks, precise cleanup that preserves unrelated files, exact old/new query
views, index source identity and bounded memory/disk work. Measure finalization and
ingestion cost on the real combined-sync fixture; do not add a full rewrite after
every batch based only on this read-time gain. No production coordinator or file
path change was introduced by this diagnostic, and no existing data was replaced.

## Integrated coordinator (validation in progress)

The current branch adds bounded automatic coalescing at a combined-ingestion
checkpoint. It considers only touched bundled segments with at least 128 table
publications and 128 address pages, at most 16,384 rows, at most 16 MiB of encoded
column streams, and at most 8 MiB of variable payload. It processes candidates
serially under the storage owner; sealed segment maintenance also holds its
existing inode lock. A busy maintenance owner postpones coalescing. Dense and
larger segments continue through the existing append/rotation path. This policy
is deliberately bounded; it is not a general streaming compactor.

Catalog v10 (`LXCAT010`) and manifest v8 give the existing generation field
physical path semantics. Generation zero uses `columns/segment.bundle`; later
generations use `bundle_{generation:016x}/segment.bundle`. Bundle v5 payload/table
encoding is unchanged. Creation is exclusive. Existing reserved names are skipped
with a 64-attempt bound so an orphan directory containing unrelated files does
not prevent future maintenance. Index checkpoint v2 (`LXICP002`) also binds the
generation. Existing data directories require the explicitly authorized fresh
sync decision; no migration or production data change is performed.

The coordinator captures the current rows and canonical bitmap, writes the new
file and derived manifest, then forces the existing strong data-before-catalog
publication. Only a successful durable catalog publication permits retirement of
the previous file. The descriptor keeps its segment ID, row order, ranges and
coverage, including empty blocks. Existing readers keep their file descriptors;
new readers capture the new manifest. No reader waits for the maintenance lock.
A stale compaction/manifest-refresh plan is rejected before writing its old
reference back. Repacking invalidates derived index identity; ordinary scan
fallback remains correct until the existing index builder publishes a new set.

Recovery first hardens the observed catalog and verifies/restores its referenced
bundle. It then removes only the known unreferenced bundle files and empty
parents, preserving unrelated files. Cleanup is repeatable if deletions are lost
on power failure. A failed write/publication/retirement poisons the current writer
and requires reopening; the coordinator never assumes a failed catalog rename
means the old catalog remains authoritative. There is no extra durability promise
for deletion itself, and no in-place overwrite of a predecessor bundle.

The regression suite covers hot/historical replacement, retained readers,
canonical changes, empty-block progress, later appends, stale maintenance and
index builders, busy ownership, orphan/name collisions and precise cleanup. A
fault sweep exercises every owner-thread checkpoint, both observed and preceding
catalog outcomes, and repeated reopen. A distinct-mount fixture covers both
routes and publication outcomes with catalog/columns on opposite filesystems.
These are controlled storage fixtures, not physical-device power-loss proof.

### Decoder findings encountered during implementation

- **B2-14 (P2): variable pages accepted unreferenced prefix/suffix bytes.** The
  failing `bytes_pages_reject_unreferenced_payload` regression reproduces this.
  Both offset encodings now require zero origin and an exact final sentinel;
  duplicate u64 materialization was removed. Row materialization also rejects
  inconsistent per-column counts and data_len values instead of indexing blindly
  or returning internally inconsistent rows.
- **B2-15 (P1): truncated packed values could become different valid values.**
  `packed_decoders_reject_missing_bits_and_invalid_widths` failed because an empty
  one-bit input decoded as zero. The decoder now checks bit width, checked total
  length and zero padding before allocating/reading. Dictionary size arithmetic
  is checked. All integer codec prefixes and extra suffixes are exercised against
  independent full-domain values.
- **B2-16 (P2): extreme timestamp differences panicked in debug builds.** The
  failing `timestamp_codec_round_trips_extreme_differences` fixture crosses the
  complete u64 domain. Explicit modular arithmetic matches the signed-delta
  representation and the prior release behavior, preserving valid encodings
  while giving debug/release the same round trip.

Fixed-width compressed pages now bound decompression by their checked expected
size. Maintenance variable pages use a per-page budget derived from validated
length columns within the aggregate 8 MiB budget. The streaming prototype used
bounded output plus a bounded Zstd window; the retained implementation uses direct bounded decompression; the later
materialization-order comparison below records the measured read-cost correction. General query variable-page resource
limits remain a separate parser/query audit item; this coordinator does not call
that unbounded path. No new unsafe block or dependency is introduced.

### First integrated release comparison

Fifteen samples per revision/profile compare the first integrated streaming-
decoder prototype with exact `6156ef33`. Finalization/checkpoints, exact row and
progress/reopen oracles are included. Internal ARM APFS, alternating process order,
no concurrent local builds/tests, no cache eviction. All samples are retained.
This prototype predates the additional codec regressions and bounded direct-
decoder change; it is evidence for the coordinator, not final-source acceptance.

| Profile | Ingestion median change | Full-row validation | Warm reopen |
| --- | ---: | ---: | ---: |
| Large history | +1.18% | +0.25% | -0.86% |
| Mixed history | +1.69% | +6.91% | -1.76% |
| Mixed live | +0.41% | -0.20% | +0.75% |
| Sparse history | -21.05% | -85.65% | -34.92% |

Sparse ingestion is 803.072→634.065 ms; validation 7.038→1.010 ms; reopen
7.014→4.565 ms; logical bytes 1,261,501→195,339 (-84.52%). Process-attributed
writes rise 2.00%; this is not physical NAND write amplification. Mixed-history
read cost requires investigation. Mixed-live reopen p95 rises 20.134→42.829 ms
while its median changes less than 1%; this tail is retained for confirmation.
[All measurements](baselines/2026-09-11-bundle-repack-stream.jsonl) and
[exact prototype patch/build identity](baselines/2026-09-11-bundle-repack-stream-build.jsonl).

All six local gates now pass on the frozen archive cb40500627ef2700af4e8f19a8d9bb36dfad94100b9f61e46da064c0b699d4e1:
917 workspace tests/10 ignored, including 206 storage tests/five ignored.
[Failure reproductions, source identities and validation](baselines/2026-09-11-bundle-repack-validation.json).
Release confirmation, both ExFAT architectures and PR merge remain outstanding. No live deployment or protected-volume access occurred.


### Exact b457ca60 and materialization-order follow-up

All six CI jobs pass at b457ca60 on Linux/macOS. The exact release comparison
against 6156 retains the sparse improvement: ingestion -21.34%, full-row
validation -87.22%, reopen -30.69%, with four files and unchanged row/progress
oracles. Large/mixed-history/live ingestion medians are -1.36%/+0.64%/-0.52%.
However, large/mixed-history validation medians rise 6.50%/10.62%; that result is
not accepted as a completed read-performance improvement.
[Exact comparison](baselines/2026-09-11-bundle-repack-exact.jsonl).

The bounded-read refactor had also moved ordinary variable-data materialization
before all fixed columns. Restoring its prior position after topic columns keeps
all checks and gives large-history validation -5.87% (228.057→214.675 ms) against
b457ca60; mixed-history validation -1.94% (141.037→138.297 ms). Ingestion changes
-0.07%/-1.90%; reopen +0.22%/+1.05%. Large-read p95 rises 4.69% while its median
improves; all samples remain. Baseline timing also varies across runs, so this
does not attribute all earlier mixed-history variation to one cause. The direct
bounded decoder alone did not remove the earlier regression. The retained order
change restores prior behavior and the measured large-read cost.
[Paired order comparison](baselines/2026-09-11-bundle-repack-read-order.jsonl) and
[exact build/patch](baselines/2026-09-11-bundle-repack-read-order-build.jsonl).

All six local gates pass for this follow-up (917 tests/10 ignored), including
the six SegmentReader regressions. [Exact validation and builds](baselines/2026-09-11-bundle-repack-read-order-validation.json).
Original-baseline/platform confirmation remains pending. Historical benchmark
coverage is also being extended to include a retained live-head cache during backfill;
previous historical profiles seeded only a floor/anchor, not that cached state.

### Original baseline with cached historical head

The extended fixture now checks historical backfill with and without an existing
canonical head and retained header window. Both release binaries use identical
fixture inputs/oracles; the original 09a63f55 source has only the recorded
benchmark API adapter and an existing-version libc dev-dependency edge. Five
alternating process pairs, three fresh datasets per process give 15 samples per
revision/profile on internal ARM APFS, without concurrent local builds or tests.
The final checkpoint/finalization is included; all rows, empty-block progress,
head hashes, anchors and retained headers are checked after reopen.

| Profile | Ingestion median, original → e2635a2f | Change | Full-row validation change | Warm reopen, original → current |
| --- | ---: | ---: | ---: | ---: |
| Cached large history | 72.322 → 69.982 ms | -3.24% | +2.34% | 19.015 → 22.908 ms |
| Cached mixed history | 917.528 → 87.923 ms | -90.42% | +0.09% | 18.829 → 26.977 ms |
| Cached sparse history | 11,316.077 → 770.691 ms | -93.19% | -2.48% | 19.034 → 17.408 ms |
| Cached rich mixed history | 1,037.014 → 91.883 ms | -91.14% | -0.29% | 24.118 → 29.922 ms |
| Uncached large history | 61.988 → 66.724 ms | +7.64% | +1.17% | 0.936 → 10.267 ms |
| Mixed live | 3,286.072 → 936.189 ms | -71.51% | +139.94% | 20.219 → 19.272 ms |
| Uncached sparse history | 2,446.148 → 704.172 ms | -71.21% | -8.58% | 0.462 → 4.790 ms |

All ingestion medians meet the user's 10% ceiling; this is not acceptance of all
lifecycle costs or end-to-end sync performance. The uncached large-history +7.64%
requires attribution under the audit's 5% investigation rule. Small live full-row
validation is 4.331→10.393 ms and remains an open read-cost finding; it is not a
SQL-query measurement. The original small-live fixture retains raw columns while
the current source writes pages. Startup performs stronger recovery/integrity
work, but redundant work must still be removed before accepting a tradeoff.

[Every sample and environment](baselines/2026-09-11-publication-cached-head.jsonl),
[build/source identities](baselines/2026-09-11-publication-cached-head-build.jsonl)
and [baseline-only adapter](baselines/2026-09-11-publication-cached-head-baseline.patch).
The original lockfile change was verified to contain only the libc dependency
edge, with no version change. This supersedes the earlier claim that every
historical profile included a retained canonical-header cache.

Intel ExFAT validation is complete for b457ca60 (206 storage tests, five query
tests and 136 cross-mount recovery cases). The exact e2635a2f follow-up passes six
reader tests, three repacking tests, five query tests and eight cross-mount
repacking cases. Both runs detach the exact disposable image. An initial
follow-up harness mistyped an expected test name; that attempt was rejected,
all 88 source hashes were reverified and the corrected harness rebuilt before
running tests. [Builds, logs, CI and platform scope](baselines/2026-09-11-bundle-repack-intel-validation.json).
No production volume contents were used. ARM final-source validation remains.


### Growth beyond the repack bound

At a0ed88c1, five alternating process pairs per profile retain exact full-row,
head/floor and restart oracles. Sparse history uses 18,432 blocks with one row in
each nonempty block (17,280 rows), crossing the 16,384-row repack bound within one
segment. Dense history uses 2,304 blocks × 1,024 rows in nonempty blocks (2,211,840
rows), crossing two million rows and ending in three segments. Empty blocks are
included in progress. Finalization is timed, and caches are not evicted.

| Profile | Ingestion, original → a0ed88c1 | Change | Full-row oracle | Warm reopen |
| --- | ---: | ---: | ---: | ---: |
| Sparse boundary | 44,943.359 → 12,192.104 ms | -72.87% | 8.858 → 11.931 ms | 0.897 → 8.122 ms |
| Dense rotation | 4,558.812 → 821.790 ms | -81.97% | 2,206.623 → 2,507.718 ms | 1.469 → 93.540 ms |

Process-attributed writes fall 97.07% sparse and 90.12% dense. Sparse retained
bytes are 3,450,298 versus 2,310,887 (+49.31%); coalescing stops at its explicit
row bound and subsequent appends retain small pages. Dense retained bytes fall
0.53%. Read/startup costs remain recorded for attribution, with no claim that
this proves end-to-end sync or uniformly faster reads. The larger fixture also
retains every observed tail and process RSS; no outlier is discarded.
[Growth measurements and runner](baselines/2026-09-11-independent-index-growth.jsonl).
