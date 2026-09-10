# Grouped flush investigation — 2026-09-11

The initial PR #130 ingestion regressions were rejected. The user subsequently
set a maximum **10% regression** for live and historical ingestion and authorized
using engineering judgment to extend the recovery journal to bounded WAL
checkpoints. This investigation is incomplete; neither candidate meets that bar.
Do not merge on the strength of the earlier correctness CI results.

## Preliminary release measurements

Same host, toolchain, original `09a63f55` baseline executable, unchanged harness
and fixture settings as the [initial comparison](2026-09-10-commit-replay.md).
These are exploratory runs: one baseline/candidate pair per profile per version,
three measured iterations per process, no simultaneous compilation or tests.
All exact-result oracles passed. Each candidate binary was copied after Cargo
reported `fresh: false`; binary hashes and raw results are retained below.
These samples guide the next experiment, not a final statistical acceptance.

| Candidate | Profile | Operation | Baseline median ms | Candidate median ms | Ratio |
| --- | --- | --- | ---: | ---: | ---: |
| v1 | dense | live_storage_ingest | 421.891 | 1888.850 | 4.48× |
| v1 | dense | historical_storage_ingest | 232.742 | 1325.611 | 5.70× |
| v1 | sparse | live_storage_ingest | 376.785 | 1870.058 | 4.96× |
| v1 | sparse | historical_storage_ingest | 229.146 | 1653.067 | 7.21× |
| v2 | dense | live_storage_ingest | 473.071 | 1584.832 | 3.35× |
| v2 | dense | historical_storage_ingest | 279.562 | 1213.078 | 4.34× |
| v2 | sparse | live_storage_ingest | 387.269 | 1586.955 | 4.10× |
| v2 | sparse | historical_storage_ingest | 230.666 | 1440.024 | 6.24× |

- [v1 raw results](2026-09-11-grouped-flush-v1.jsonl): group file fsyncs by device,
  use ordering barriers before column rename, retain a full sync before manifest
  publication, and avoid republishing an unchanged historical manifest.
- [v2 raw results](2026-09-11-grouped-flush-v2.jsonl): additionally group manifest
  and directory publication into one full flush, group WAL file/parent sync, and
  order the journal before the WAL's full sync. The final helper tests/error
  diagnostic cleanup do not change the measured release behavior.

An isolated 21-call microbenchmark on a disposable 4 KiB-write file observed
median syscall times of 0.036 ms (`fsync`), 0.535 ms (`F_BARRIERFSYNC`), and
4.063 ms (`F_FULLFSYNC`). This is attribution evidence, not node throughput.
The full cache drain remains expensive even after removing redundant calls.

## Durability basis and limits

Apple's [fcntl manual](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/fcntl.2)
explicitly specifies that an ordering barrier orders prior fsync'd data on the
same device, while a full sync persists all such prior data. Grouping retains
one descriptor per device, including nested mounts and file symlink targets.
Unsupported ordering barriers fall back to full sync; real I/O errors propagate.
Linux still uses normal `fsync`, including directory entries, as described by
[fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html).

The two Apple FFI calls borrow valid File descriptors, pass no pointers and do
not transfer ownership; EINTR is retried. Tests cover unsupported barriers,
propagation of actual errors and failed fallbacks, ordering before manifest
publication, final persistence after directory updates and interruption at each
coordinator checkpoint. This does not simulate every hardware power-loss case.

A first v2 storage run had one lock-release test failure whose old assertion
hid the I/O error. The assertion now exposes the error; the focused test and a
subsequent complete storage run (103 tests) passed. The intermittent cause is
not established and must remain visible during full workspace/CI validation.

## Next step

Implement bounded WAL checkpoints, retain durable successful writes, include
both live and historical behavior and metadata/coverage ordering, preserve exact
query results, and compare repeated release runs against the original baseline.
Checkpoint cost must be measured, not hidden by shifting it to indexing, close
or reopen. No accepted performance tradeoff is recorded for the current code.
