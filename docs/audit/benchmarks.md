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
The current fixture follows the [combined sync APIs](sync-ingestion-checkpoints.md),
with final checkpoint cost included. Comparisons with older separate-call
revisions must record that API/durability-strategy difference; inputs and final
row/progress oracles remain equivalent.

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
| `LOGEX_PUBLICATION_HISTORY_CACHED_HEAD` | 0 | `1` seeds the retained canonical head/header window before historical timing (fixture v5) and verifies it remains unchanged after backfill/reopen; useful for backfill while live state exists |
| `LOGEX_PUBLICATION_SEGMENT_ROWS` | 1000000 | Segment row target |
| `LOGEX_PUBLICATION_REPEATS` | 3 | Fresh-directory repetitions |
| `LOGEX_PUBLICATION_HEADER_FIELDS` | minimal | `rich` populates hash, bloom and fork fields with deterministic synthetic data; fixture v3 |
| `LOGEX_PUBLICATION_PAYLOAD` | transfer | `transfer` preserves fixture v3's 32-byte data; `mixed` selects fixture v4, varying 0–1024 bytes with independently hashed words |
| `LOGEX_PUBLICATION_READ_MODE` | full | `full` reads whole columns; `selected` passes all row IDs through the page-selection path used by query callers. Both validate identical rows before and after reopen; ingestion inputs and timing are unchanged. |
| `LOGEX_PUBLICATION_ROUTE` | both | `live`, `historical` or `both` |
| `LOGEX_PUBLICATION_CHECKPOINT_EACH_BLOCK` | 0 | `1` calls `checkpoint()` after every live block; useful for publication boundary cost without a wall-clock sleep. Since the ordered-publication successor, this is not a promise of per-block power-loss durability; report the candidate contract explicitly |
| `LOGEX_PUBLICATION_DURABLE_CHECKPOINT` | 0 | `1` uses `checkpoint_durable()` for the final checkpoint and any per-block checkpoint. Report this separately from bounded ordered publication; both include exact clean-reopen oracles. |

Numeric sizes and repetition counts must be positive. Every sixteenth block is empty.
Live profiles hold the production 8,192-header window. Historical profiles seed
only their floor/anchor unless `LOGEX_PUBLICATION_HISTORY_CACHED_HEAD=1`; earlier
historical measurements therefore exclude publication of a retained live cache.
Neither mode warms an existing million-row hot
segment; that additional write-amplification scenario remains to be measured.
It prints individual timings and exact fixture identifiers, with no in-process
summary or claim of end-to-end P2P throughput.

The `lifecycle` records supplement ingestion timing with logical/allocated file
bytes, file and segment counts, warm reopen time, full-row validation time, and
OS-attributed process writes. Collection is outside the ingestion timer. Full-row
validation includes materialization, sorting and comparison with the independent
oracle; it is not query latency. The existing `full_row_validation_ms` field
means validation of all rows in either `read_mode`; selected mode also includes
building the row-ID vector. It does not include SQL planning, predicate matching
or index lookup. Report the mode and compare identical modes for optimization
acceptance. A same-binary full-versus-selected comparison diagnoses caller cost
and is not an old-versus-new performance result. See the
[selection investigation](bundle-selected-reads.md).
Unix allocated bytes use `stat` blocks and exclude
directory/filesystem metadata. The process write counters are
[Apple's `proc_pid_rusage` v2](https://github.com/apple-oss-distributions/xnu/blob/main/libsyscall/wrappers/libproc/libproc.h)
([matching structure](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/sys/resource.h))
and [Linux `/proc/self/io` `write_bytes`](https://www.kernel.org/doc/html/latest/filesystems/proc.html#proc-pid-io-display-the-io-accounting-fields).
They do not measure device/NAND write amplification: delayed writeback can be
charged after sampling, and Linux counts page dirtying before writeout or later
truncation. Compare on the same OS/filesystem with identical checkpoint policy.
Other platforms report an unavailable counter rather than zero.

The small CI fixture includes mixed payloads, rotation, empty blocks and exact
reopen checks, both with and without the cached historical head. Cached history
also checks the exact retained headers, head hash and indexed anchor. Transfer
fixture v3 inputs remain unchanged for earlier comparisons.

For the separate CPU-only cached-header codec diagnostic, use:

```sh
cargo test -p logex-storage --test ingestion_publication --release --locked \
  benchmark_cached_header_encoding -- --ignored --nocapture --test-threads=1
```

This compares JSON, RLP and JSON followed by LZ4 on 8,192 minimal/populated headers,
with exact RLP round trips. Its fixed codec order and absence of persistence mean
it cannot establish ingestion performance acceptance. Use paired publication
runs with both header profiles to validate the actual storage change.

To reproduce the original production baseline with fixture v3, copy
`crates/logex-storage/tests/ingestion_publication.rs` from `3f457987` into an
isolated checkout of `09a63f55`, then apply the
[baseline-only adapter](baselines/2026-09-11-publication-v3-baseline.patch).
It restores the old separate-call sequence, removes the new codec-only diagnostic,
and leaves baseline production code/dependencies unchanged. Build both revisions
with the same pinned release profile and match fixture digests and tip hashes.


For the later lifecycle/mixed-payload harness, the
[baseline-only adapter](baselines/2026-09-11-publication-lifecycle-baseline.patch)
is against the harness at the corresponding audit commit. Copy that harness into
an isolated `09a63f55` checkout, apply this adapter, and add `libc = "0.2"` under
`[target.'cfg(target_os = "macos")'.dev-dependencies]`. The existing lockfile gains
only `libc` in `logex-storage`'s dependency list; no dependency version changes.
The adapter preserves every fixture/oracle and restores the original separate
calls, rejects the unsupported strong-checkpoint option, and omits the unrelated
new header-codec diagnostic. Record both source and binary hashes. The original
production APIs publish on each call; current final checkpoint cost remains timed.

## DataFusion value conversion

The general SQL result benchmark bypasses native SELECT using arithmetic in
ORDER BY while projecting baseline-compatible integer, string and nullable
columns. A general aggregate adds a small-output control. Results are checked
against an independent fixture oracle after timing. Narrow and ten-column wide
queries return at most 1,000 rows; aggregates scan the same indexed matches.

```bash
LOGEX_BENCH_PROFILE=dense LOGEX_BENCH_ROWS=200000 LOGEX_BENCH_REPEATS=100 \
  cargo test -p logex-query --test audit_harness --release --locked \
  benchmark_datafusion_result_values -- --ignored --nocapture --test-threads=1
```

Repeat with `LOGEX_BENCH_PROFILE=sparse`. Each process builds, indexes, compacts
and reopens a fresh fixture, warms every query once, then rotates their order.
For a comparison, install the identical fixture in both isolated source trees,
build equivalent release binaries sequentially, verify source/binary hashes and
fresh Cargo artifacts, then alternate at least five process pairs. Preserve all
samples and record median/p95 and process RSS; caches are not forcibly evicted.
Do not compare speed against a baseline that returns incorrect values.

## Exact aggregate evaluation

The aggregate fixture measures plain, conditional, residual-filter, nested-CASE
and grouped totals on indexed synthetic rows with 32-byte amounts. An independent
in-memory numeric table supplies each expected value outside the timed interval.
The fixture uses a temporary directory, warms each shape once, then rotates
50 measurements per shape. Set `LOGEX_AGGREGATE_BENCH_ROWS` to 100 or 20000;
the default is 20000 and the fixture is bounded to at most 20000 rows.

```bash
LOGEX_AGGREGATE_BENCH_ROWS=20000 \
  cargo test -p logex-query --test sql_aggregates --release --locked \
  aggregate_latency -- --exact --ignored --nocapture --test-threads=1
```

Use identical fixture source in both isolated revisions and alternate at least
five process pairs per size. Copy each executable before building the next
revision, verify fresh Cargo artifacts and distinct binary hashes, and retain
all latency and process-memory samples. The [aggregate acceptance record](sql-aggregate-semantics.md)
includes the complete runner, environment, all initial/prototype measurements
and final results. Compacted/reopened correctness is tested separately; this
timing fixture does not establish live-peer or concurrent-ingestion throughput.

The ignored library test `aggregate_expression_preparation_latency` is a bounded
1000-iteration diagnostic for context construction, query-state snapshots and
expression preparation. Its averages identify fixed setup cost; they do not
replace repeated end-to-end query latency and memory acceptance.


## Identifier binding acceptance

The [identifier-binding record](sql-identifier-binding.md) compares merged
`f45f2640` with implementation `b81a9bb2`. Its linked final measurement record
contains the complete Python runner, exact hardware/toolchain/filesystem,
binary and fixture hashes, every latency/RSS sample and summaries. Use a new
empty output destination when rerunning that stored runner; it refuses to
replace an existing result directory. It creates local archives and uses the
existing target directory, refreshing source mtimes and verifying fresh artifacts
before copying each executable. Keep other builds and tests stopped during timing.

The same final identifier fixture runs in both archives before timing. All six
regressions fail on the baseline and pass on the candidate. The unchanged REST
fixture then creates/indexes/compacts/reopens 20,000 rows per process, warms each
shape and rotates 50 calls across five shapes. Five alternating process pairs
provide 250 samples per shape and revision and five peak-RSS observations per
revision. Every successful response matches exact expected values. This measures
handler/query/serialization work, excluding network transport and live peers;
it does not establish production ingestion throughput. All six workspace gates,
the release query suite and protocol-consistency checks pass separately.

The separate metadata coverage uses `metadata_latency` in the same identifier
fixture: three exact-result shapes, 1,000 rotating samples each and five
alternating process pairs. The metadata investigation record links the complete
gzip-compressed JSONL streams and their original hashes. Decompress with a
standard gzip reader before reading their embedded fixture/runner or samples;
the bytes match the original logs exactly. Its confirmation runner verifies and
reuses the same two binaries, begins candidate-first and pools every original
sample with five additional pairs. Initial, confirmation and pooled results
remain separate, including the initial candidate tail outlier and retained
+6.30% pooled table-metadata p95. The observation was investigated and remains
below the 10% ceiling; it is not silently removed or described as a speedup.


The [metadata predicate pass](sql-metadata-semantics.md) reuses the same ignored
metadata and REST benchmarks after independent before/after predicate validation.
Its release record embeds the final reference/benchmark fixtures and complete
archive/build runner. Five alternating pairs retain 5,000 samples per metadata
shape/revision and 250 per REST shape/revision, plus all process counters. Exact
source/fixture/binary identities, full correctness logs and losslessly compressed
original timing outputs are linked there. This is an offline query comparison;
live-peer ingestion and integrated staging remain separate gates.


The [named-subquery scope pass](sql-query-scope.md) reuses the unchanged REST
fixture after final before/after scope and identifier checks. Five alternating
pairs provide 250 latency samples per shape/revision and five process-memory
observations per revision. Its release record embeds the runner and fixtures;
the linked record and compressed logs retain every build, correctness outcome
and timing observation.
Newly accepted queries are validated against the independent reference, not
benchmarked against the preceding rejection. The unchanged metadata path's
prior measurements remain retained separately.

The [parsed-syntax eligibility pass](sql-syntax-eligibility.md) changes the first
validation walk, which also precedes metadata execution. Its acceptance therefore
repeats both unchanged metadata and REST fixtures. The exact final syntax fixture
runs against both archived revisions before timing; supported controls must pass
on both, while the recorded baseline rejection/result defects must fail only on
the preceding revision. Preserve fresh-artifact checks, separately copied
binaries, all five alternating pairs and every original log. Query measurements
do not establish whole-node ingestion throughput.

Its native-count tail investigation reuses the same binaries for five additional
REST pairs, starting candidate-first. Initial, confirmation and combined records
remain separate and complete. The initial +5.39% p95 is retained; confirmation
changes it by -1.11%, and pooling all observations gives +2.39%. The linked
confirmation record embeds the exact runner, environment checks, binary hashes
and every output hash. This is the complete investigation, with no exclusions.
