# Checked extraction release comparison — 2026-09-09

Baseline: merged commit `a83023cfc3d9714c24938d6fe677f333d04c31e5`.
Candidate: extraction-boundary change in the PR containing this report. Binary
SHA-256 and measured production-source SHA-256 identify each revision in the raw
JSONL configuration. The benchmark is described in
[the extraction audit](../extraction-boundaries.md).

## Environment and procedure

Mac14,15, 8 logical cores, 16 GiB RAM; macOS 26.6.2 (25G83), ARM64. Internal APFS
SSD with approximately 150 GiB free. Pinned nightly-2026-08-24, rustc
1.100.0-nightly (`fb6531d550e0075b9eb9a51464f404805eec87d9`), LLVM 23.1.0;
default Cargo release profile. The desktop was not isolated or CPU pinned.

Inject the same ignored benchmark into an isolated baseline checkout, adapting
only the new Result unwrapping to the old signatures. Build release test
executables, copy each executable out of Cargo's target directory, and verify
distinct binary hashes and test inventories. When sharing the dependency target
directory between checkouts, clear `logex-types` and `logex-sync` release artifacts
before each build; verify `fresh: false` for the benchmark artifact. The temporary
baseline worktree was removed after confirming its only edit was the benchmark.

Each comparison uses five alternating process pairs, with baseline first in pairs
0, 2 and 4. Every process verifies all 8,190 expected rows for each path, warms
both paths, then measures five samples of 100 iterations per path. This gives 25
samples per version/path and 10,000 timed block conversions per comparison.
`/usr/bin/time -l` records peak RSS per process. Builds and sampling profiling are
excluded from measured runs. File I/O and cache flushing are outside this warm
in-memory workload; disk use/write amplification are not applicable to conversion.

## Results and retained tradeoff

| Path | Baseline median | Final median | Change | Baseline p95 batch average | Final p95 batch average |
| --- | ---: | ---: | ---: | ---: | ---: |
| Fresh result allocation | 0.136750 ms | 0.147127 ms | +7.59% | 0.139781 ms | 0.148182 ms |
| Reused append buffer | 0.139827 ms | 0.152304 ms | +8.92% | 0.141692 ms | 0.158508 ms |

These p95 values are percentiles of **100-iteration batch averages**, not individual
request latency. Median process peak RSS was 22,265,856 bytes baseline versus
22,200,320 final; maxima were 22,282,240 versus 22,216,704 bytes. This is comparable
memory use, not an established memory optimization.

The regression exceeds 5% and is explicitly retained for checked shape/index/count
handling, complete-block rollback and actionable error propagation. Its absolute
median cost is about 10–12 microseconds per 8,190-log block. Per-pair median changes
were +8.46/+8.67/+7.33/+4.45/+6.42% for fresh allocation and
+10.56/+8.37/+7.69/+9.46/+8.22% for reused buffers. This is a repeatable cost,
not dismissed as noise. It is not a full-node ingestion regression measurement;
network, receipt authentication, WAL, compression, indexing and queries are absent.
Revisit end-to-end effects in the storage/sync and integrated performance batches.

## Investigation

The first checked revision cost roughly +47% in both paths. A macOS `sample`
profile of that executable captured 38 benchmark-thread stacks: 12 had the checked
constructor at the top and 8 had platform memory movement at the top, with further
stacks in successful `WrapErr` adapters. This short profile is a hotspot clue,
not a statistically precise CPU breakdown.

1. Mark the checked constructor inline so callers can optimize row construction:
   overhead fell to approximately +22% against contemporaneous baseline runs.
2. Match conversion errors explicitly and map numeric errors only on failure:
   overhead fell to roughly +11–12%. A second short profile no longer showed
   constructor or successful error-context adapter frames, but still showed copies.
3. Move diagnostic formatting into a small cold function, retaining all checks:
   final results are above. No unchecked indexing, unsafe writes, skipped topics,
   truncated lengths, or reduced validation were introduced.

This retains useful, measured reductions in the initial implementation cost,
without claiming an optimization relative to the original unchecked baseline.

Raw measurements (each includes the relevant binary/source hashes):

- [Initial checked revision](2026-09-09-extraction-initial.jsonl)
- [Inline constructor revision](2026-09-09-extraction-inline.jsonl)
- [Error-only context revision](2026-09-09-extraction-errors.jsonl)
- [Final revision](2026-09-09-extraction.jsonl)
