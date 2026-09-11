# Bounded bundle table records

Unmerged follow-up to `a0c3aa64` in draft PR #130. Small sync calls leave repeated
stream descriptors and inline page indexes in immutable grouped tables. The
current experiment reduces their stored bytes without changing payload pages,
group structure, checkpoint boundaries or allocation limits. Sparse fragmentation
and safe reclamation still need work; this is not complete lifecycle acceptance.

## Format and validation

The integrated candidate uses catalog v9 (`LXCAT009`), segment manifest v7 and
bundle v5 (`LXBND005`, decoded table magic `LXBT0005`). The user permits useful
breaking formats and fresh sync. Older catalogs are rejected unchanged; no
production data was reset or migrated. Isolated codec variants below used older
catalog/segment constants for measurement and are not interchangeable deployments.

Each physical table record contains a one-byte codec (zero for raw, one for LZ4),
a little-endian u32 decoded length and its payload. The existing reference CRC32
covers this entire record. LZ4 is used only when smaller; raw fallback adds five
bytes. The existing lz4_flex dependency supplies the block codec; no dependency
or unsafe block is added. Payload extent checksums remain unchanged.

Reference table_len now counts stored record bytes. chain_bytes still counts
**decoded** table bytes. The reader verifies the physical checksum, rejects an
unknown codec or decoded length outside 32 bytes–4 MiB or beyond the remaining
chain budget, then allocates a bounded destination. Raw length must match exactly;
LZ4 must produce exactly the declared bytes. Existing table identity, parent span,
row ordering, physical overlap, schema, index/extent limits and exact-consumption
checks then run on the decoded table. Parent budget arithmetic adds decoded lengths,
so compression cannot admit a larger chain or conceal an oversized allocation.

Two added tests cover incompressible/raw and compressible/LZ4 round trips through
the maximum decoded size, insufficient budgets, malformed headers, every prefix
of a small compressed record, invalid output lengths and valid-checksum malicious
records. Existing malformed-table tests mutate decoded fields and re-encode/checksum
the record so they continue reaching the intended table checks. Grouped snapshots,
interrupted append/rollback, retained readers and index-bound fixtures still pass.
The isolated 21 bundle tests and strict storage Clippy pass; the integrated storage
suite passes 197 tests/four ignored. All six required workspace gates pass (908 tests/nine ignored, documentation tests
and release node build). Platform and exact performance confirmation at `6156ef33` are recorded below.
[Source identities and focused validation](baselines/2026-09-11-bundle-table-codec-validation.json).

## Codec comparison

Both experiments use exact `a0c3aa64` as baseline, five alternating release process
pairs with three fresh datasets each (**15 samples per revision/profile**), internal
Apple Silicon APFS and no concurrent local builds/tests. Caches are not evicted.
Final checkpoints and independent row/progress/reopen oracles are included.
Source files, binary hashes, hardware, toolchain, parameters and every sample are
retained. Full-row validation includes sorting/oracle comparison, not SQL latency.
OS-attributed writes do not measure physical NAND write amplification.

| LZ4 profile | Ingestion median | Full-row validation | Warm reopen | Logical bytes |
| --- | ---: | ---: | ---: | ---: |
| Large history, 491,520 rows | +0.11% | -1.80% | -0.17% | -0.05% |
| Mixed history | +1.67% | +0.15% | -1.26% | -0.06% |
| Mixed live | +1.76% | -2.77% | +1.74% | -1.76% |
| Sparse history, 960 rows | +2.39% | -19.83% | -15.51% | -54.38% |

Sparse logical bytes fall from 2,765,385 to a median 1,261,497. Full reads fall
8.862→7.104 ms, reopen 8.657→7.314 ms and ingestion rises 789.042→807.939 ms.
Payload worker order can change physical offsets and hence compressed-table size;
all fixture digests and exact row/progress/reopen checks agree. RSS medians remain
within +0.2%, including fixture buffers. The mixed-history ingestion p95 rises
13.65% (106.158→120.652 ms), prompting a larger exact integrated confirmation.
This tail is retained, not excluded or assumed to be noise.
[LZ4 source and complete samples](baselines/2026-09-11-bundle-table-lz4.jsonl).

Zstd level 1 reduces sparse logical bytes 64.45%, but raises sparse ingestion
**16.96%**, exceeding the user's 10% ceiling; it is rejected. Sparse full reads
improve 20.35%, little more than LZ4's 19.83%, while reopen improves 20.10%.
Large/mixed-history/live ingestion medians change +0.11%/+2.73%/+2.63%. Several
mixed-workload tails are much worse, including mixed-history ingestion +221.98%
and mixed-live reopen +2116.77%; these observations are retained without causal
attribution. No Zstd table record or codec-selection branch enters production.
[Rejected Zstd source and all samples](baselines/2026-09-11-bundle-table-zstd.jsonl).

LZ4 is the candidate for full confirmation. The older original-baseline sparse
space/read gap is still substantial even with smaller tables. Neither these
microbenchmarks nor fault injection establish network throughput, USB-device
power-loss behavior, complete snapshot isolation or staging readiness.

## Exact integrated confirmation at 6156ef33

All six required local gates pass (908 tests/nine ignored, documentation tests and
release node build), followed by all six Linux/macOS CI jobs in run 34603732010.
Both ARM/Intel disposable ExFAT runs pass 197 storage tests/four ignored, five
query tests/one ignored and all 128 cross-mount recovery cases (8 WAL, 24 combined
sync, 96 preceding-checkpoint cases). All 88 archived Rust/manifest/toolchain files
match the tested source. Both images were verified by identity and detached.
[CI](baselines/2026-09-11-bundle-table-codec-ci.json),
[platform sources, binary hashes, complete logs and detach records](baselines/2026-09-11-bundle-table-codec-platform-validation.jsonl).
These tests do not prove physical power-loss behavior or USB-device throughput.

The exact candidate comparison increases each profile to **45 samples per
revision**, with 15 alternating process pairs and the same three fresh datasets
per process. Local builds and tests finish before timing starts. All exact oracles
pass; every sample, including earlier outliers, is retained.

| Profile | Ingestion median | Ingestion p95 | Full-read median | Warm-reopen median |
| --- | ---: | ---: | ---: | ---: |
| Large history | +1.29% | +3.24% | -0.19% | -0.60% |
| Mixed history | -0.26% | +9.35% | -2.28% | -1.30% |
| Mixed live | +0.58% | +4.64% | -1.37% | +0.39% |
| Sparse history | +2.36% | -16.75% | -21.30% | -20.40% |

Sparse files use a median 1,261,525 bytes (-54.38%); median OS-attributed writes
fall 8.62%. Sparse RSS changes +0.67% (full fixture included), while other profile
RSS medians decrease. The mixed-history p95 remains +9.35% (81.522→89.143 ms)
after its initial +13.65% observation: within the user's 10% ceiling, but a repeated
tail cost requiring further attribution before overall acceptance. Large-history
full-read p95 +5.30% and mixed-live full-read p95 +7.73% are also recorded; their
medians improve. These tails are not characterized as stable gains or dismissed
as noise. [Exact source and complete confirmation](baselines/2026-09-11-bundle-table-codec-confirmation.jsonl).

A separate exact comparison against original `09a63f55` uses 15 samples per
revision/profile. The original has only the tracked benchmark lifecycle adapter;
its production code is unchanged. It uses separate row/progress calls, while the
candidate uses combined sync and the approved bounded publication/re-ingestion
contract. Both include final checkpoints and pass the same exact fixture oracles.

| Profile | Original ingestion median | Candidate ingestion median | Change |
| --- | ---: | ---: | ---: |
| Large history | 68.237 ms | 67.808 ms | -0.63% |
| Mixed live | 3,052.998 ms | 851.266 ms | -72.12% |
| Sparse history | 2,503.216 ms | 856.563 ms | -65.78% |

These recorded ingestion medians meet the user's ceiling. Large-history median
is effectively unchanged; its earlier +6.95% result belongs to the identified
older source/run, and no stable large-history speedup is claimed. Original p95
is 303.666 ms versus candidate 93.094 ms, reflecting substantial observed spread;
all samples remain in the evidence. This is storage-call throughput, not P2P sync.
[Exact original-baseline comparison](baselines/2026-09-11-bundle-table-codec-original.jsonl).

The broader lifecycle gap remains: sparse full reads are 7.212 versus 0.968 ms,
warm reopen 7.515 versus 0.488 ms, and logical files 1,261,603 versus 136,843 bytes.
Sparse process RSS is 22.97 versus 14.88 MB. Mixed-live full reads are 10.321
versus 4.802 ms, while large-history full reads improve (218.500 versus 234.984 ms).
Large-history warm reopen is 9.899 versus 0.875 ms. Smaller tables help but do not
remove many small payload pages or provide safe artifact reclamation. Those costs,
generic WAL/index costs and the repeated mixed-history tail remain audit work.
PR #130 stays draft and unmerged.
