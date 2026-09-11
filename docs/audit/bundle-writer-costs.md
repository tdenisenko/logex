# Bundle writer cost attribution

Exact grouped-format ingestion meets the original-baseline 10% ceiling in the
recorded profiles, but large historical ingestion is 6.95% slower in the latest
comparison. That exceeds the audit's 5% investigation threshold. The isolated
diagnostic below measures the writer's work before selecting an optimization.

Source is `1e27d838` plus temporary phase counters, absent from production.
Ten release samples per profile run on internal Apple Silicon APFS, with fresh
directories, no cache eviction or concurrent local builds/tests, and unchanged
row/progress/reopen oracles. Instrumentation adds clocks and atomic counters;
these timings attribute work and are not an acceptance comparison. Lock-wait
durations sum across compression workers and overlap other phases. Do not add
them together as elapsed wall time. Table/catalog checksums are outside this
particular instrumented payload scope.

| Profile | Ingestion median | Payload CRC time | Payload bytes | Artifact write time | Summed lock wait |
| --- | ---: | ---: | ---: | ---: | ---: |
| Large history, 491,520 rows | 68.984 ms | 2.834 ms | 21,207,018 | 6.320 ms | 76.487 ms |
| Mixed history, 245,760 rows | 66.905 ms | 5.928 ms | 46,372,949 | 7.860 ms | 40.148 ms |

Checksums currently execute under the shared artifact writer lock, along with
file writes. This serializes independent checksum work from the existing
compression workers. It does not prove that checksum cost alone explains the
entire original-baseline difference. The data supports testing checksum
calculation before acquiring that lock, while preserving serialized placement,
capacity/offset checks, writer poisoning and the complete publication protocol.
[Instrumented source, parameters and samples](baselines/2026-09-11-bundle-write-cost-profile.jsonl).

The scheduling experiment remains isolated and is not retained. Uninstrumented
paired release runs give ingestion changes of -1.32% large history, -0.98% mixed
history, -2.55% mixed live and +0.98% sparse history (15 samples each), and +0.19%
sparse live (three samples). Large-history process-pair medians vary from +4.08%
to -11.50%; mixed-history pairs range from +2.68% to -2.50%. The overall small
gains are not convincing relative to that variation. Mixed-live read median
improves 6.58%, but p95 worsens 17.60%; three sparse-live samples do not establish
its apparently larger read improvement. No sample is discarded.

Focused bundle tests, strict Clippy and full/selected exact fixtures pass in the
prototype. It changes when existing workers acquire the lock, which can change
physical column order and a few bytes of position metadata; it does not remove
checksums or alter a format/durability rule. The extra early allocation/checksum
work on a writer that may already be full or failed is another reason to require
a clear benefit before adopting it. Production keeps its original scheduling.
[Complete comparison](baselines/2026-09-11-bundle-checksum-scheduling.jsonl).

Payload CRC work is now a measured contributor, but this experiment does not
fully attribute the original-baseline 6.95% difference. Safe compaction and the
larger sparse fragment/read/space costs remain higher-impact unresolved work.
