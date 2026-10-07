# Reproducible benchmarks

The repository retains executable correctness fixtures and opt-in release
benchmarks. Historical audit reports, machine-specific logs and raw measurement
archives are kept outside Git; they are not inputs to the build or test suite.

## Complete mainnet query references

`tools/live_query_complete_reference.py` captures every selected local input in
the catalog's original inclusive query range and evaluates the original SQL with
SQLite or exact Python integer arithmetic. It does not shorten queries to their
separate bounded verification ranges. Multiple cases with the same emitter set
and range share captured inputs. A conservative predicate proof can restrict
topics only when every direct `logs` read requires them; unsupported predicates
retain all topics. SQLite remains the independent expression evaluator.

Inspect the plan before executing against an owned deployment:

```sh
python3 tools/live_query_complete_reference.py benchmarks/ethereum-mainnet.json \
  --output /private/path/mainnet-complete-reference \
  --url http://owned-logex-host:18683 \
  --credentials-config /private/path/runtime-config.toml \
  --ssh-host owned-logex-host --remote-root /path/to/owned/deployment \
  --expected-identity /private/path/initial-identity.json \
  --case usdc_daily_transfers --plan-only
```

Remove `--plan-only` for the explicitly selected run. The existing identity
checker must match process start, parent, commands, binary, data inode and volume;
this helper never initializes identity or changes a node. Healthy finalized
coverage is required before and after bounded work. No cache flush, shared-query
cancellation, offline live-store verifier or external Ethereum download occurs.

RPC responses at the row cap cause an exact non-overlapping range split. A single
block at the cap cannot pass. Every complete leaf is compared with a raw SQL
projection; duplicate canonical identities within or across leaves fail. Inputs
are loaded one leaf at a time into a disk-backed SQLite database with a bounded
page cache. Original SQL, complete range, process identity, reference mode, input
pieces and hashes, health samples and final result are retained outside Git.

The default allowances are 10,000 rows per response, 5 million selected rows and
16 GiB captured evidence per emitter/range group, a 64 MiB response bound, 10 GiB
minimum local free space, and a one-day invocation deadline checked between work
units. These are input/work limits, not a promised duration or peak-RSS bound.
The temporary SQLite database is removed after evaluation; immutable captures stay.

The request intent is saved before submission. An interrupted or failed request
must be investigated before another attempt. Only complete saved pieces from the
same plan, helper versions and deployment can be reused; failed or completed runs
are never silently retried. A transport timeout does not prove server work stopped.

Matching results establish independent evaluation over the complete selected
**local** inputs. RPC and SQL share the same underlying store; this is not an
independent completeness proof for Ethereum history. Frozen historical references
must retain their original capture source rather than being relabeled after upgrades.

## Storage, indexes and queries

The `audit_harness` integration target creates disposable deterministic datasets.
A small, non-ignored control runs in ordinary CI. Build first so compilation does
not contaminate measurements:

```sh
cargo test -p logex-query --test audit_harness --release --locked --no-run
LOGEX_BENCH_PROFILE=dense cargo test -p logex-query --test audit_harness --release --locked benchmark_storage_indexes_and_queries -- --exact --ignored --nocapture --test-threads=1
LOGEX_BENCH_PROFILE=sparse cargo test -p logex-query --test audit_harness --release --locked benchmark_storage_indexes_and_queries -- --exact --ignored --nocapture --test-threads=1
```

| Variable | Default | Meaning |
| --- | --- | --- |
| `LOGEX_BENCH_ROWS` | 200000 | Generated rows per fresh dataset |
| `LOGEX_BENCH_REPEATS` | 5 | Fresh-directory repetitions |
| `LOGEX_BENCH_PROFILE` | dense | `dense`: 128 logs/block; `sparse`: one log every three blocks |
| `LOGEX_BENCH_SEGMENT_ROWS` | 50000 | Segment rotation target |
| `LOGEX_BENCH_BATCH_ROWS` | 8192 | Write batch size, including reverse history |
| `LOGEX_BENCH_WORKERS` | 4 | Simultaneous native query threads |
| `TMPDIR` | OS default | Filesystem for disposable fixture directories |

Numeric inputs must be positive. Use more rows than a segment and check
`compacted_segments` to establish that compaction ran. The fixture measures live
and reverse historical storage ingestion, index publication, compaction, warm
reopen, native/SQL queries and concurrent query threads. Independent expected
rows are checked outside timing. It does not open an existing node dataset.

JSON output contains `config`, `sample`, `storage` and `summary` records. Libtest
may prefix the first record with its test name: extract from the opening `{`
rather than dropping it. Median and nearest-rank p95 describe these samples;
with five repetitions, p95 is the maximum. Logical file bytes are not allocated
disk space or device write amplification.

## Focused release fixtures

Build the selected target with `--no-run` first, then run one fixture at a time.
All examples use synthetic data or disposable directories.

```sh
# Canonical header/anchor publication, historical floor and clean reopen.
cargo test -p logex-storage --test ingestion_publication --release --locked -- --ignored --nocapture --test-threads=1

# Cached-header encoding diagnostic, without storage I/O.
cargo test -p logex-storage --test ingestion_publication --release --locked benchmark_cached_header_encoding -- --exact --ignored --nocapture --test-threads=1

# Selected/full page decoding with exact expected values.
cargo test -p logex-storage --test page_decode_performance --release --locked benchmark_projected_page_decoding -- --exact --ignored --nocapture --test-threads=1

# DataFusion result conversion and exact aggregate evaluation.
LOGEX_BENCH_PROFILE=dense cargo test -p logex-query --test audit_harness --release --locked benchmark_datafusion_result_values -- --exact --ignored --nocapture --test-threads=1
LOGEX_AGGREGATE_BENCH_ROWS=20000 cargo test -p logex-query --test sql_aggregates --release --locked aggregate_latency -- --exact --ignored --nocapture --test-threads=1

# Direct REST/gRPC handlers; these exclude network transport and live peers.
cargo test -p logex-server --lib tests::query_latency --release --locked -- --ignored --nocapture --test-threads=1

# Receipt extraction and WAL encoding/replay.
cargo test -p logex-sync --lib --release --locked extraction_release_baseline -- --ignored --nocapture --test-threads=1
cargo test -p logex-storage --lib --release --locked wal_release_baseline -- --ignored --nocapture --test-threads=1
```

The publication fixture exposes `LOGEX_PUBLICATION_*` controls in
[`ingestion_publication.rs`](../crates/logex-storage/tests/ingestion_publication.rs).
Record the selected live/historical route, retained-header setup, payload,
read mode and checkpoint policy. `checkpoint_durable()` and ordinary ordered
publication have different durability contracts and must not be pooled as the
same measurement. Lifecycle allocated-file counters exclude directory and
filesystem metadata; OS process-write counters are not device/NAND writes.

Page decoding accepts `LOGEX_PAGE_DECODE_REPEATS` from 1 to 10000 (default 50).
Aggregate latency accepts `LOGEX_AGGREGATE_BENCH_ROWS` of 100 or 20000 (default
20000). WAL codec iterations use positive `LOGEX_WAL_CODEC_ITERATIONS` (default
10). Other fixture defaults and exact result oracles live alongside the tests.

## Comparison protocol

Record source commits, harness/fixture and binary hashes, dirty changes, lockfile,
resolved features, compiler, OS/CPU/RAM, filesystem, free space and parameters.
Use the same fixture and release profile for both revisions. For node-equivalent
dependency features, build with
`cargo test --workspace --test TARGET --release --locked --no-run --message-format=json`
and run the reported executable directly. Package-only builds can have a
different feature union, including Zstd options enabled by Reth.

Build revisions separately, retain each executable, and alternate runs on quiet
hardware. Keep all samples and failed correctness controls. Investigate
repeatable regressions; increase repetitions when variance masks the effect.
Fresh directories do not clear the OS page cache, and warm reopen is not a
cold-start measurement. A fixture speedup does not establish live P2P throughput.

Use `/usr/bin/time -l` on macOS or `/usr/bin/time -v` on Linux around the compiled
test executable for process RSS. This includes fixture and oracle buffers; it is
not per-query memory. Use platform profilers for allocation and I/O attribution.
Keep raw outputs and comparison archives outside the repository.
