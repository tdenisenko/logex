# WAL recovery integrity

Batch 2 begins with WAL framing, row decoding and startup replay. This milestone
preserves the binary v1 and legacy JSON formats and the public WAL interface.
It does not establish power-loss durability for the full storage commit path.

## Findings

| ID | Severity | Evidence and disposition |
| --- | --- | --- |
| B2-01 | P1, recovery data loss | A complete entry with a bad checksum or invalid payload made `read_all` return a successful prefix. Startup replayed that prefix and truncated the entire WAL, including later complete entries. Recovery now fails with the WAL path and entry offset; no prefix is returned or replayed. A three-entry corruption reproducer failed before the fix, including through `NativeStorage::open`. Repeated failed reopen preserves all WAL bytes and does not advance the catalog row count. |
| B2-02 | P2, malformed-length resource exhaustion | Outer payload lengths, binary row counts and per-row data lengths caused allocations before checking the available bytes. The decoder now bounds each claim by the containing file/payload before allocation and uses fallible reservations. Tiny inputs claiming `u32::MAX` bytes or `usize::MAX` rows return errors. Append checks row count, total encoded size and row metadata before opening the WAL. |
| B2-03 | P2, persisted-row correctness | Legacy JSON ignored the frame row count; binary decoding accepted reserved topic-mask bits and inconsistent data-length metadata. These could silently alter the recovered row set or query-visible data length. Both representations now validate their row shape/count. Existing optional-topic patterns, including gaps, and both source tags remain supported. The JSON count, topic mask and invalid-append regressions failed before the fix. |
| B2-04 | P2, interrupted-startup recovery | An empty successful replay left an incomplete tail in place, so a later append could follow that tail and become unreadable. Startup now clears an accepted tail even if it contains no recoverable rows. Reopen → append → reopen → reopen verifies exact recovered rows and no duplicates for partial-header and empty-payload/missing-checksum cases. |

## Recovery policy

The outer eight-byte header contains row count and payload length but has no
checksum. Only the payload is checksummed. In this format an oversized length
cannot reliably distinguish an interrupted payload write from corrupted framing
that conceals later complete entries. Recovery therefore uses these rules:

| Final bytes or damage | Result |
| --- | --- |
| Missing or empty WAL | Empty recovery; no file is created by reading/truncating a missing WAL. |
| Complete entries with valid count, payload and CRC | Return all rows, preserving entry and row order. |
| Fewer than eight bytes of a final header | Discard that incomplete entry after reading its bytes successfully. |
| Complete, valid payload with zero to three matching CRC bytes | Discard that incomplete entry. Do not replay it without the complete checksum. |
| Complete header whose payload extends past EOF | Error; preserve the WAL for inspection. |
| Mismatched CRC, invalid row shape/count/version or trailing payload bytes | Error; preserve the WAL, including any valid prefix and later entries. |
| Read/metadata/permission error, unexpected EOF during a read or detected growth | Error; do not reinterpret it as an ordinary incomplete tail. |

This intentionally makes some interrupted writes require operator recovery where
previous startup silently discarded them. It prevents an ambiguous length from
being treated as proof that all remaining bytes are disposable. Preserve the
reported WAL and data directory when startup fails; inspect the reported offset
and recover from verified data or a known-good backup. Do not truncate the WAL
merely to bypass the diagnostic. Automatic verified repair is still a later batch.

A read is immutable. Startup still performs its existing catalog/manifest setup
before replay, so this is not a guarantee that a failed startup changes no other
files. No new format, repair command or production deployment is introduced.

## Tests and validation

- Five initial regressions failed on the merged baseline `11c1bec7` and pass with
  this change: complete corruption, native startup corruption, JSON row count,
  reserved topic mask and invalid append.
- Deterministic tests cut the final frame at every byte, flip every bit of a
  complete middle frame, truncate the binary payload at every byte, and inject a
  non-EOF read error at every position. No corruption returns a successful prefix.
- Additional cases cover maximum length/count claims, unknown version/source,
  trailing bytes, inconsistent lengths in both encodings, partial CRC mismatch,
  all 16 optional-topic masks, empty data, receipt/trace source and empty batches.
- Existing partial-hot-commit repair and idempotent replay tests remain enabled.
- Full local gate and Linux/macOS CI results are recorded in the PR.

## Performance comparison

The ignored `wal::tests::wal_release_baseline` uses 4,096 deterministic rows,
0–384 data bytes per row, varied optional topics and exact recovered-row checks.
It measures encoding, decoding, synced append and warm WAL read separately.
Codec samples average ten operations; disk operations use one operation per
sample. Fifteen samples are recorded per process after untimed correctness
checks. Append reuses a truncated temporary WAL; directory creation and truncation
are outside the append measurement. This does not measure durable catalog/segment
commit, cold startup or write amplification.

```sh
cargo test -p logex-storage --lib --release --locked --no-run
cargo test -p logex-storage --lib --release --locked wal::tests::wal_release_baseline -- --ignored --exact --nocapture
```

Run already-built binaries for comparisons; alternate equivalent
release builds and verify their fixture CRC, executable hashes and row equality.
The [release comparison](baselines/2026-09-09-wal.md) records the retained
6.27% decode correctness cost and the isolated single-copy improvement.

## Cleanup and remaining storage work

Removed the warning-and-success recovery paths, unchecked frame-width casts,
blind length allocations and the test that blessed arbitrary garbage as a safe
tail. File opening/truncation now distinguishes `NotFound` from other I/O errors
instead of using `Path::exists` to hide them. No production `unwrap` or new
unsafe code was added.

The next durability review must resolve segment/manifest/catalog synchronization
and ordering before WAL deletion. Inspection found buffered column writes and
catalog temporary-write/rename paths without the corresponding durability
barriers. This is a separate high-priority finding requiring interruption tests;
this PR does not claim to fix it. WAL truncation synchronization alone would be
unsafe before replacement data is durable and is deliberately not added here.

Partial append failure/cancellation, process exclusivity, whole-WAL memory use,
file replacement races, corruption classification outside the WAL and offline
repair remain open. File-size checks bound allocation claims by persisted input;
they are not a process memory quota. The growth check is defensive, not a lock
or protection against same-length concurrent file edits.
