# Execution cache publication cost

## Finding and scope

B4-33 is an implementation cost opportunity. The existing availability getter
walks the retained contiguous suffix through repeated B-tree lookups, up to 4,096
entries, on every call. Canonical block attempts and removals call it even when
body retention does not change. Live ingestion also calls it again after updating
the consensus head. Every ordinary publication enqueues a Reth status command,
even for the same complete range.

The cache provider is shared with the serving handler, but the actual engine owns
its peer manager directly and invokes mutation methods sequentially. The handler
reads the cache. The production call graph does not establish concurrent cache
writers; mutable manager methods make the publication sequence explicit.

## Correction and invariants

The derived optional range is cached lazily inside the existing cache state. It is computed
with reverse ordered iteration under a read guard and invalidated under the
same write guard whenever body-number mappings change. Empty results are cached
too. This avoids repeated unchanged scans; the first query after a mapping change
still computes the suffix. It is neither a new persisted format nor a new worker.

Suppression compares the entire last submitted tuple, including its latest hash.
A consensus head update remains special: pinned Reth overwrites handshake status
latest/hash without updating the shared serving range. The existing subsequent
range command must therefore still run even when its tuple is unchanged. This
preserves existing FIFO submission order; the two commands are not an atomic
operation in Reth and may straddle a polling budget.

Invalidation must include canonical header replacement, count and byte eviction,
removing the last body, and replacement rejection after the old canonical body was
removed. An unsuccessful candidate is not necessarily a no-op. Same-hash payload
replacement can also evict other bodies. Missing/stale removals and header-only
changes must not invalidate unchanged body mappings.

## Evidence and validation

A bounded release fixture fills 4,096 synthetic empty blocks and measures five
samples of 10,000 repeated availability queries with setup outside the timer. This
isolates the concrete repeated lookup cost; it is not an ingestion or whole-node
benchmark. Original and candidate use the same fixture and pinned toolchain.

The original median was **118.816 microseconds per query** (sample range
115.665–120.826 microseconds). The candidate median was **14.796 nanoseconds per
query** (14.767–14.875 nanoseconds). Reusing one already computed tuple removes the
repeated traversal in this specific warm unchanged-cache case. This is not a claim
about ingestion throughput, first-query-after-mutation latency, global memory or
RSS. Duplicate retained-body replacement still invalidates conservatively because
the existing remove/reinsert path can evict other entries. The first query after
any actual mapping change still traverses the suffix, now linearly.

Both runs used nightly-2026-08-24 release builds on an Apple M2 with 16 GiB memory,
macOS 26.6.2, the same fixture and warm in-memory data. Baseline production is
`e07fb451`, with only the measurement fixture added; its source hash and exact
samples are retained in the validation record. The first release test build had to
compile its dependency feature graph; that build time is outside the measurement.
The original fixture needed a formatting-only adjustment. The first strict Clippy
run then flagged its intentional constant release-mode assertion. A local explained
lint expectation keeps that runtime guard without failing debug compilation;
measurement behavior is unchanged. Rust ignores that attribute on the assertion
macro itself, so it is attached to the benchmark function. Both initial gate
attempts are retained separately.

Reproduce the component workload explicitly:

```bash
cargo test -p logex-sync --release --locked \
  availability_lookup_repeated_full_cache_microbenchmark -- --ignored --nocapture
```

All 316 focused execution-network tests pass, with the one new benchmark ignored
by normal checks. Seven new correctness controls include an independent ascending
full-map suffix oracle, mixed mutations, count/byte eviction, rejected conflicting
replacement, no-op preservation, extreme heights and bounded concurrent clone
reads. Publication tests compare complete tuples and manually poll a dormant local
Reth fixture to prove `set_head` restores unchanged cached availability. They make
no remote connections or discovery requests.

Independent review found no correctness defect. The tracker was simplified from
an optional value to a mandatory startup tuple because no `None` state was used.
The final focused tests passed again after this cleanup. Source review confirmed
all production body-map mutation sites are covered by invalidation. The obsolete
repeated predecessor lookup loop and unconditional ordinary submissions are removed;
necessary forced head submissions remain. Full workspace gates and PR/CI/merge
are pending.
Mac mini cleanup remains complete; this task uses no remote host or external volume.

[Validation record](baselines/2026-09-16-execution-cache-publication.json).
