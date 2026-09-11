# Sparse bundle page coalescing feasibility

Follow-up to the LZ4 table candidate `6156ef33` within draft PR #130. LZ4 reduces
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
