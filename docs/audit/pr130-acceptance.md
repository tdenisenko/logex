# PR #130 acceptance record

This is a storage recovery and performance milestone, not completion of the
whole audit or approval to deploy/live-sync production data. Implementation and
acceptance are complete at production source
`4408e0704af38afd6e95cd7e5aa7144da14edc80`; subsequent documentation-only changes
carry the final evidence. [PR #130](https://github.com/tdenisenko/logex/pull/130)
records the final-tip CI and merge state.

## Implemented scope and compatibility

Recovery distinguishes intentional identical WAL writes from replay, publishes
complete validated blocks and coverage together, preserves canonicality and
committed evidence, and holds exclusive directory ownership. Immutable bundle
and rewrite generations preserve reader lifetimes. Managed indexes bind exact
source identity and publish independently of segment metadata. Malformed raw,
page, table and integer inputs have specific regression fixes; this does not
claim an exhaustive review of every parser or query path.

The final formats are catalog 11 (`LXCAT011`), segment manifest 9, bundle 5 and
index checkpoint 2. A fresh directory is required. Older formats fail without
migration/reset; retain the original directory for rollback. The user authorized
this compatibility decision, but no existing production data is discarded and no
service is installed or restarted by this PR.

Combined sync uses a shared bounded recovery window (32 MiB / 64 calls / five
seconds). Apple ordered publication may lose recent complete checkpoints on
power loss and require verified re-ingestion; explicit durable checkpoints and
route/WAL/reorg/maintenance/window boundaries retain strong persistence. Age is
checked at operation/idle boundaries and is not a deadline while I/O is blocked.
Cross-device dependencies are fully synchronized. Generic row-only WAL APIs keep
their strong durability contract. These assumptions require devices to honor
flush requests. See [the durability contract](published-ingestion-checkpoints.md).

Clean startup checks metadata and row boundaries; payload CRCs are checked on
read. Full payload verification remains mandatory before recovery mutation or
retirement of predecessor/orphan bundles. The node does not label this limited
startup check a full scrub. See [startup checks and regression evidence](startup-checks.md).

## Sync performance decision

Original source 09a63f55 versus 13588219, identical publication fixtures and API-
only baseline adapter, release Rust builds on internal Apple Silicon APFS. Five
alternating process pairs, three fresh datasets each (15 samples); uncached large
history and the cached-large tail confirmation each have 45 samples. Final
checkpoints, exact whole-row/progress/head/anchor/reopen oracles are included.
Cached-history cases seed and verify 8,192 retained canonical headers. Caches are
not evicted; no local build/test runs overlap timings. Other user applications
are not stopped. These are storage-call workloads, not network throughput or
measurements of the protected external SSD.

| Workload | Original → candidate median, ms | Change |
|---|---:|---:|
| Cached large history | 67.427 → 64.268 | -4.69% |
| Cached mixed history | 824.879 → 85.233 | -89.67% |
| Cached sparse history | 11085.402 → 722.072 | -93.49% |
| Rich-header cached mixed history | 945.956 → 88.389 | -90.66% |
| Uncached large history, 45 samples | 63.400 → 66.001 | +4.10% |
| Mixed live | 2977.217 → 846.824 | -71.56% |
| Uncached sparse history | 2276.271 → 648.973 | -71.49% |
| Mixed live, durable checkpoint every block | 2995.607 → 1471.994 | -50.86% |

Every production-sync fixture median meets the user's 10% ingestion ceiling.
The initially higher cached-large p95 (75.012 → 93.150 ms) does not repeat in the
45-sample confirmation: median 73.839 → 70.005 ms (-5.19%), p95 91.105 → 76.152 ms
(-16.41%). The longer uncached-large run reduces the earlier +7.64% observation
to +4.10%, with p95 70.259 → 71.985 ms. All samples remain recorded, including
unfavorable observations; this is not a guarantee for arbitrary hardware/data.

[Publication samples](baselines/2026-09-11-final-publication.jsonl) and
[tail confirmation](baselines/2026-09-11-final-cached-tail-confirmation.jsonl)
include source/build hashes, parameters and oracles. The strongest live profile
uses the candidate's explicit durable-per-block API; the original adapter has no
such API and uses its original per-write durability behavior, as recorded.

Growth at a0ed88c1 covers 17,280 sparse rows beyond the 16,384-row repack cap and
2,211,840 dense rows across three segments. Ingestion improves 72.87% and 81.97%,
respectively. Exact rows and progress pass. The latter's excessive full-payload
startup scan is addressed by the final startup-only change and measured separately.
[Growth samples](baselines/2026-09-11-independent-index-growth.jsonl).

## Final startup-only comparison

Production source 4408e070 versus the preceding 13588219, with the same release
fixture and publication settings. Five alternating pairs; one fresh dataset per
pair/revision for growth and three for mixed workloads (5 or 15 samples). All
whole-row, canonicality, progress, head/anchor and reopen oracles pass. This change
only moves clean-startup validation and updates its success log; ingestion codecs,
persistence boundaries and query algorithms are unchanged.

| Workload | Warm reopen before → after, ms | Change | Ingestion median change |
|---|---:|---:|---:|
| 2,211,840 rows / three segments | 87.042 → 7.528 | -91.35% | -6.91% |
| 17,280 sparse rows, past repack cap | 7.666 → 5.694 | -25.72% | +0.07% |
| Cached mixed history | 26.077 → 17.106 | -34.40% | +0.18% |
| Mixed live | 18.521 → 16.527 | -10.77% | +0.09% |

Full-row validation changes -0.68%, -0.06%, +1.84% and -2.57%, respectively.
Allocated-byte and file/segment-count medians are identical; median process-
attributed writes are identical or differ by 4 KiB. Subkilobyte logical-size
differences are retained in the raw samples without attributing their cause. The
startup reduction exceeds the observed noise; unrelated ingestion/read differences
are not attributed to a new ingestion optimization. These are warm-cache startup
measurements; removing the unconditional payload scan also removes its mandatory
full-file I/O on a cold start, without claiming measured cold-start latency.

[Complete paired results](baselines/2026-09-12-startup-performance.jsonl) include
both release build records and executable hashes, hardware/toolchain/cache controls
and every sample. Local builds/tests do not overlap these timings. Final source
matches all 88 tracked Rust/Cargo/toolchain hashes in the archived build inputs.

## Retained costs and limits

These tradeoffs are explicit; the sync ceiling is not silently applied only to
a favorable generic benchmark or waived for production callers.

- **Standalone row-only APIs:** 45-sample dense/sparse medians show live +10.72% /
  +10.27%, history +20.61% / +16.47% versus the original implementation. These
  APIs retain the extra strong journal/WAL durability required to fix ambiguous
  replay. Phase profiling identifies synchronization as a material cost. Actual
  sync callers in `engine/ingest.rs` and `engine/anchored.rs` use joint block/
  progress ingestion, measured above; the node crate's generic write calls are
  inside test modules. The generic
  API regressions are accepted for this stronger contract; they are not presented
  as satisfying the production-sync performance ceiling. A redundant empty-WAL
  truncation and obsolete index-to-segment publication have been removed.
- **Index construction:** +10.57% dense / +5.62% sparse (about 24–25 ms per 200k
  rows), with exclusive source validation and strong checkpoint publication.
  Removing redundant manifest publication improves the preceding candidate by
  7.11% / 6.68%. Remaining strong publication is retained for consistent index
  visibility. Indexes are derived; broader index payload auditing remains batch 6.
- **Read layout:** small live full-row materialization/sort/oracle costs 3.850 →
  9.473 ms; durable-per-block live costs 3.847 → 11.376 ms. The bounded fragmented
  compressed layout trades this full-materialization cost for much lower sync
  writes and approximately 56% smaller logical storage in this fixture. The
  repack cap prevents unbounded checkpoint memory/latency. Larger mixed/large
  publication full-row oracles range +0.34% to +1.12%; the dense growth oracle is
  +13.65% at a0ed88c1. These are not equivalent to filtered SQL queries.
- **Query fixture scope:** native filters, SQL counts, ordering and four concurrent
  native clients on the existing 200k-row dense/sparse fixture show changes from
  -6.21% to +1.77%, with exact equivalence. Compaction is -3.14% / -6.52%. This
  generic-storage fixture does not prove all new live bundle query workloads;
  mixed ingestion/query/reorg behavior remains later audit work.
- **Startup:** empty-WAL cleanup falls from approximately 16–17 ms to 6 ms on the
  generic fixture. Required catalog/root hardening remains. A clean reopen no
  longer needs a full payload scan; pending recovery and retirement still do.
- **Process peak memory:** median peak RSS across complete benchmark processes
  is 662 → 68 MiB for mixed live, 1456 → 785 MiB for cached mixed history and
  4222 → 4013 MiB for dense growth. Costs remain for uncached sparse history
  (14 → 21 MiB), sparse growth (69 → 189 MiB), and the full generic/query fixture
  (889 → 979 MiB dense; 622 → 951 MiB sparse). These high-water marks include
  fixture generation, expected rows, repeated runs, result buffers and allocator
  retention; they do not isolate node steady-state memory or establish the cause
  of a difference. Repack/table/read-window bounds remain enforced. The mixed
  workload/staging audit must measure steady-state memory before live deployment;
  this PR does not claim that every memory workload improved.
- **Space and writes:** sparse history can use 43–49% more logical bytes for
  immutable metadata/canonical evidence while process-attributed writes fall
  92–97%. Dense growth uses 0.53% fewer logical bytes and 90.12% fewer process
  writes. These OS counters are not physical NAND write amplification.

[Generic/query samples](baselines/2026-09-11-final-query-performance.jsonl),
[index comparison](baselines/2026-09-11-independent-index-comparison.jsonl),
[phase attribution](baselines/2026-09-11-storage-phase-attribution.json).
Whole-file caching, wider extent layouts, Zstd tables, speculative worker/reader
changes and the isolated single-file fsync experiment were rejected when costs
or noise did not justify them. Payload checks remain on reads and before destructive recovery. The deliberate
clean-startup check scope is documented above.

## Validation and disposition

At 13588219 all six local gates pass (919 workspace tests, 10 intentionally
ignored); all six Linux/macOS CI jobs pass. Both ARM and Intel disposable ExFAT
runs pass 207 storage tests (five ignored), five query tests (one ignored), 27
index tests, the CLI index command regression and publication smoke. Each also
passes 136 controlled cross-mount recovery cases: WAL 8, sync 24, published 96,
repack 8. Both images were verified detached. These are system-disk-backed test
images; no contents of the protected external volume were accessed.

The final startup change adds two tests and extends corruption fixtures. All six
local gates pass (921 tests, 10 intentionally ignored), as do all six Linux/macOS
CI jobs at 4408e070. Both ARM and Intel ExFAT pass 209 storage tests (five ignored),
five query tests (one ignored), 27 index tests, CLI/publication smoke and all 136
cross-mount recovery cases per platform. Both images are verified detached. The
new clean-read and predecessor-preservation regressions are confirmed in both
platform logs. Startup performance results are recorded above.

- [Final local gates and logs](baselines/2026-09-11-startup-local-validation.json)
- [Archived inputs, 88 source hashes, ARM binaries and source CI](baselines/2026-09-12-startup-source-build.json)
- [Final ARM ExFAT/cross-mount logs](baselines/2026-09-12-startup-arm-exfat.json)
- [Final Intel build/ExFAT/cross-mount logs](baselines/2026-09-12-startup-intel-exfat.json)
- [Failing-before and passing-after startup regressions](baselines/2026-09-11-startup-regressions.json)

No measured-host build/test ran during performance comparisons. Subsequent edits
change documentation only; the complete set of tracked Rust/Cargo/toolchain files
still matches the tested archive. Before-fix failures, the corrected fixture
helper, unfavorable samples and rejected experiments remain explicit evidence.

Remaining audit work includes trust/fork conformance, networking/liveness, sync
reorg/cancellation, broad index/query/protocol/dashboard review, external-volume
supervision, offline authenticated repair and the integrated 24-hour staging
soak. This PR neither completes those tasks nor certifies readiness for live sync.
