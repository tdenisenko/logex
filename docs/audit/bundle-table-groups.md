# Grouped bundle tables

Unmerged follow-up to `9215da2c` in draft PR #130. This addresses repeated full
metadata snapshots, which account for most of the sparse bundle growth described
in [the read-cost investigation](bundle-read-costs.md). It does not finish the
storage audit or waive remaining read, index, startup or space costs.

## Format and recovery

Catalog v8 (`LXCAT008`), segment manifest v6 and bundle v4 (`LXBND004`,
`LXBT0004`) intentionally require a fresh directory. The user authorized breaking
formats to meet performance requirements. Existing directories are rejected;
no production data has been deleted, rewritten or migrated.

Each bundle reference and table now carries a nonzero local publication sequence.
Tables form its base-eight decomposition. Ordinary appends write a delta; every
eighth append summarizes its group, every 64th summarizes 64 updates, and so on.
For example, sequence 16 contains a summary of updates 9–16 whose parent is the
summary at sequence 8. It does not copy the metadata from updates 1–8 again.
Old tables and payloads remain immutable and readable through their references.

The sequence determines the group span and exact ancestor depth. A parent must
have the expected sequence, precede its child physically, have no more rows,
and account for the child's exact depth and cumulative table bytes. Every table
and selected payload still passes its checksum. The existing 4 MiB cumulative
decoded-table budget and per-stream extent/index bounds remain; the maximum
possible reference depth is 148 for a u64 sequence. Budget exhaustion or sequence
overflow writes a complete snapshot and resets the local sequence to one.

During normal table validation, the reader captures the next group's boundary
if needed: append-only stream lengths/counts and the five replaceable metadata
stream descriptors. The writer reuses those validated values. It neither copies
the old data/index prefix into that capture nor reopens an older snapshot by
pathname. Changed metadata, including replacement with an empty stream, remains
part of the new summary. Stream schema additions are preserved even when empty.

The catalog remains the authority for visible rows, canonicality and coverage.
The 32 MiB / 64-call / five-second ordered recovery window and explicit durable
checkpoint are unchanged. Interruption can discard only an unpublished suffix;
grouping does not overwrite committed payloads or permit corrupt committed data.

## Measurements and rejected alternatives

All comparisons use alternating release processes, exact row/progress/reopen
oracles, fresh internal APFS directories on Apple Silicon, and no concurrent
local build/test workload. Caches are not evicted. Mixed profiles and sparse
history have five pairs × three samples; sparse live has three pairs × one sample.
Full-row validation includes reading, sorting and oracle comparison, not SQL
latency. OS process-write counters do not measure physical NAND amplification.

The first base-32 prototype reduces sparse historical files from 9,352,715 to
2,094,587 bytes, but ingestion rises 6.57% against `9215da2c`. This is investigated
and superseded, not hidden or accepted as a performance tradeoff.
[Complete base-32 comparison](baselines/2026-09-11-bundle-groups32.jsonl).

Smaller base-eight groups reduce ancestor traversal. Median changes against
`9215da2c`, before reusing captured boundaries:

| Workload | Ingestion | Full-row validation | Warm reopen | Logical file bytes |
| --- | ---: | ---: | ---: | ---: |
| Mixed history | -0.61% | +0.78% | -0.73% | +0.04% |
| Mixed live | -0.76% | -1.88% | +0.25% | -0.21% |
| Sparse history | +0.47% | +4.84% | -0.73% | -70.43% |
| Sparse live | -0.53% | +1.76% | -2.30% | -48.78% |

Sparse history uses 2,765,385 bytes and sparse live 6,916,888 bytes. These are
fixture totals, not a universal compression ratio. Median throughput remains
within noise in this comparison. Some mixed-read tails vary substantially; the
raw samples remain available and this is not tail-latency acceptance.
[Complete base-eight comparison](baselines/2026-09-11-bundle-groups8.jsonl).

Reusing the validated boundary instead of reopening its prefix changes sparse
historical ingestion by -2.56% against the base-eight prototype, mixed history
-0.33%, mixed live +0.89%, and sparse live +0.50%. Logical file bytes are unchanged.
The reuse also preserves the inspected file when its pathname changes. This is
the integrated implementation. These small CPU differences are not a claimed
end-to-end sync gain. [Boundary-reuse comparison](baselines/2026-09-11-bundle-group-boundary-reuse.jsonl).

No table-compression codec or new dependency is retained. The superseded
base-32 traversal and pathname-reopening prototype are absent from production.

## Regression evidence and remaining acceptance

The strengthened two-column, 1,024-append fixture must stay below 512 KiB. It
fails with the original `9215da2c` writer. Existing snapshot coverage now checks
every reference through 1,057 appends, including multiple grouping levels.
Additional tests cover 24 failures around eight group boundaries, suffix rollback,
retry, inline indexes, replacement with empty metadata, and preserved old views.

A valid-checksum forged table with the wrong group parent is rejected; disabling
the sequence relation makes the regression fail. A pathname-replacement test
fails in the superseded prefix-reopening prototype and passes with the captured
boundary. The latter two are guards for the new grouping implementation, not
claims of defects in the old ungrouped production writer. Existing malformed
field mutations now target the new byte offsets rather than accidentally testing
the wrong fields. [Before proofs and restoration record](baselines/2026-09-11-bundle-group-guard-proofs.json).

All 18 focused bundle tests and all six workspace gates pass: 899 tests passed,
nine ignored. [Validation record](baselines/2026-09-11-bundle-groups-validation.json). Exact integrated performance confirmation and Linux/macOS CI
plus both ExFAT recovery/query suites remain required for this format.

Grouping removes the repeated full-prefix growth pattern; it does not coalesce
the many tiny payload pages or reclaim all superseded records. Sparse historical
storage and read time remain above the original `09a63f55` fixture. Safe
reclamation, read/index/startup costs and concurrent query lifetime still need
disposition. PR #130 remains draft and unmerged.
