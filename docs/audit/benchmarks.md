# Reproducible audit benchmarks

For beacon SSZ decode/root cost, see the [focused release comparison](baselines/2026-09-09-beacon.md)
and its explicit ignored benchmark, including build-cache isolation precautions.

The ignored integration benchmark in `crates/logex-query/tests/audit_harness.rs`
uses only existing dependencies. A small, non-ignored version runs in ordinary
CI and verifies the same paths for both fixture profiles.

## Run

Build first so compiler memory/time does not contaminate measurements:

```sh
cargo test -p logex-query --test audit_harness --release --locked --no-run
LOGEX_BENCH_PROFILE=dense cargo test -p logex-query --test audit_harness --release --locked -- --ignored --nocapture --test-threads=1
LOGEX_BENCH_PROFILE=sparse cargo test -p logex-query --test audit_harness --release --locked -- --ignored --nocapture --test-threads=1
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `LOGEX_BENCH_ROWS` | 200000 | Generated rows per fresh dataset |
| `LOGEX_BENCH_REPEATS` | 5 | Independent fresh-directory iterations |
| `LOGEX_BENCH_PROFILE` | dense | `dense`: 128 logs/block; `sparse`: one log every three blocks |
| `LOGEX_BENCH_SEGMENT_ROWS` | 50000 | Segment rotation target |
| `LOGEX_BENCH_BATCH_ROWS` | 8192 | Write batch size, including reverse historical batches |
| `LOGEX_BENCH_WORKERS` | 4 | Simultaneous native query threads |
| `TMPDIR` | OS default | Filesystem for disposable fixture directories |

Numeric inputs must be positive. Choose a dataset larger than a segment to
exercise compaction; check `compacted_segments` rather than assuming it ran.
Temporary directories clean themselves up after each iteration. No existing
data directory is opened.

## What is measured

- Live storage ingestion, including its WAL, segment/catalog publication and
  final checkpoint. Sync-engine canonical-header, anchor and coverage-state
  publication are not part of this fixture.
- Index construction and manifest publication, separate from compaction.
- Eligible segment compaction and startup/reopen validation.
- Native filters, SQL count and ordered/limited queries, warmed once per path.
- A group of concurrent native queries on OS threads with a shared start
  barrier. Timing includes barrier release and joins, excludes thread creation
  and expected-result assertions.
- Reverse historical storage ingestion, including finalization of sparse
  staging, followed by reopen and exact-result validation.
- Logical file bytes before indexes, after indexes, and after compaction.

Fixture construction and independent expected-result computation are untimed.
All query results are compared exactly, including native log payloads and SQL
order/count. The fixture version and digest identify input changes. Hashes and
timestamps are consistent within each block; log indexes restart per block.
Skewed addresses, optional topics, and mixed payload sizes exercise compression
and selection. The sparse profile has absent block numbers, but does not claim
to test validation or coverage publication of empty Ethereum blocks.

Output contains JSON records: `config`, `sample`, `storage`, or `summary`.
Libtest may prefix the first config record with the test name; strip text
before its opening `{` when extracting JSON rather than dropping that record. Summaries contain median and nearest-rank p95; with five samples p95
is the maximum and is not a production latency estimate. Warmups do not emit
sample records. `work_rows` is the input dataset size for storage lifecycle
operations and matched/output rows for queries; it is not bytes read or a
DataFusion scan counter. Aggregate concurrent throughput uses total returned
rows across the group divided by group elapsed time.

## Comparison protocol

Record base/candidate commits, harness revision, dirty diff, lockfile digest,
`rustc -Vv`, `cargo -V`, OS/CPU/RAM, filesystem, free space, parameters and raw
JSON output. Use the **same harness and fixture digest** for both revisions.
If an older commit lacks the harness, apply only the benchmark files to an
isolated checkout and record that fact.

Use identical release profiles and quiet hardware. Fresh directories are not
cold caches: this harness does not evict the OS page cache. Do not call warm
reopen timings cold-start measurements. Alternate base/candidate runs; increase
repetitions when variance masks the difference. Investigate repeatable changes
over 5%, and retain optimizations only when benefits exceed observed noise.

On macOS, use `/usr/bin/time -l` around the already-built test executable for
process peak RSS; on Linux use `/usr/bin/time -v`. Test executable paths are
reported by the `--no-run` build. Memory includes fixture/oracle/result buffers;
it is not per-query peak memory. Use platform profilers for allocation/CPU/I/O
attribution before changing production code. Logical file size does not measure
physical write amplification; collect device/process I/O separately when
auditing write paths.

This baseline does not measure P2P throughput, cryptographic validation,
concurrent ingestion with queries, HTTP/WS backpressure, or the production API's
query admission policy. Those require the later subsystem and integrated
batches. No speedup is claimed by adding this harness.

Checked extraction fixtures and release comparisons are documented in the
[extraction audit](extraction-boundaries.md) and its
[baseline report](baselines/2026-09-09-extraction.md).

## Sync storage publication

The separate `logex-storage` integration fixture includes the live canonical
header/anchor writes and historical floor updates omitted by the row-only
benchmark. See its [findings and baseline](ingestion-publication.md).

```sh
cargo test -p logex-storage --test ingestion_publication --release --locked --no-run
cargo test -p logex-storage --test ingestion_publication --release --locked -- \
  --ignored --nocapture --test-threads=1
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `LOGEX_PUBLICATION_BLOCKS` | 128 | Measured complete blocks |
| `LOGEX_PUBLICATION_ROWS_PER_BLOCK` | 128 | Rows in each nonempty block |
| `LOGEX_PUBLICATION_WARM_HEADERS` | 8192 | Untimed prior header-window setup |
| `LOGEX_PUBLICATION_HISTORY_BLOCKS` | 2048 | Maximum complete blocks per historical call |
| `LOGEX_PUBLICATION_SEGMENT_ROWS` | 1000000 | Segment row target |
| `LOGEX_PUBLICATION_REPEATS` | 3 | Fresh-directory repetitions |

All values are positive. Every sixteenth block is empty. This fixture holds the
production 8,192-header window but does not warm an existing million-row hot
segment; that additional write-amplification scenario remains to be measured.
It prints individual timings and exact fixture identifiers, with no in-process
summary or claim of end-to-end P2P throughput.
