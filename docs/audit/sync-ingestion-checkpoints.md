# Combined sync ingestion checkpoints

This is an **unmerged prototype** continuing PR #130. It addresses B2-10 in the
sync callers and tests the user's proposed bounded rewind/re-fetch approach.
Performance acceptance, platform validation and the wider audit remain open.
The user permits incompatible changes if they materially help performance and
is willing to perform a fresh sync; this prototype has not required a column
format change. No existing production dataset has been reset or modified.

## Behavior and invariants

Live blocks, forward-gap chunks and historical chunks now associate their rows
and progress in one storage operation. Each operation takes complete validated
blocks, including blocks with no logs. Subscribers are notified after that
operation succeeds. Forward-gap publication uses its final header window once;
the superseded list of per-block 8,192-header snapshots is removed.

Successful sync operations become queryable in the running process. They are
not individually promised durable across restart: a restart may discard the
unfinished epoch and resume the existing verified fetch pipeline from the last
durable head/floor. `checkpoint()` makes the current rows and progress durable.
This is an intentional acknowledgment change for the combined sync APIs,
authorized by the user's preference for bounded re-ingestion over duplicated
recovery row data. Generic `write_batch`/`write_historical_batch` retain their
WAL-backed durable behavior; their separately published progress is not a
transaction, and sync no longer uses that sequence.

An epoch contains at most 32 MiB of equivalent encoded row payload or 64 caller
batches. Validation computes the size without serializing a second row copy.
A valid oversized caller batch checkpoints before returning. Five seconds makes
an epoch eligible for checkpoint at the next write or existing background tick;
it is not a hard wall-clock deadline. Route changes, generic durable writes,
standalone metadata updates and relevant maintenance boundaries checkpoint first.
Detached compaction plans exclude the epoch's affected segments.

## Recovery protocol

`wal/ingestion.json` is a checksummed, versioned journal limited to 16 KiB. It
records the route, original active segment descriptor, segment-allocation boundary
and fingerprints of the durable catalog/state. Referenced metadata files have a
64 MiB limit and are checked using bounded streaming reads. The existing row WAL
and its version 1/2 recovery journal cannot coexist with an ingestion journal.

1. Persist an active journal before modifying segment artifacts. Leave the old
   catalog and state files unchanged while publishing in-memory rows and progress.
   Column replacements still order complete contents before rename, preserving
   older committed prefixes; removing those ordering operations would be unsafe.
2. At checkpoint, stage the new catalog/state under reserved temporary names.
   Fully persist their contents and the preceding segment publications before
   recording a durable publishing decision. External segment devices are already
   fully synchronized before their manifests can refer across devices.
3. Rename both staged metadata files to their normal names, persist both names,
   then remove the journal durably. The journal contains their lengths/checksums.
4. Startup with an active journal verifies the old metadata before opening it.
   Restore the original segment prefix and canonical bits; discard only newly
   allocated segment directories. Restore a raw manifest and discard derived
   indexes for that prefix. Keep the journal until rollback is fully persisted.
   Interruption during rollback repeats the same operation safely.
5. Startup with a publishing decision validates both new metadata files before
   replacing either destination. A rename already completed is accepted only
   when the destination matches the recorded fingerprint. Finish publication;
   do not rewind a decided commit. Missing/corrupt recovery inputs fail explicitly
   and leave recovery artifacts in place.

Recovery runs under exclusive data-directory ownership before ordinary manifest
adoption or queries. A configuration change must not overwrite fingerprinted
metadata before recovery completes. The checkpoint boundary is a complete caller
batch, so rows from a block spanning multiple segments roll back together with
its progress; chain finality alone is not used as a local persistence boundary.

## Validation and remaining work

All six local workspace gates passed with 833 tests, zero failures and six
explicitly ignored cases ([results](baselines/2026-09-11-sync-checkpoint-gates.jsonl)).
The first sandboxed run could not bind the existing loopback HTTP test fixtures;
the complete rerun with local sockets allowed passed. Subsequent cleanup changes
only clarify API comments, a constant name and a recovery error message.

New regressions cover live/historical retries, zero-log progress, rotation and
historical compaction, prior non-canonical rows, every main-thread write/commit
failure checkpoint, interrupted rollback, origin metadata damage, staged metadata
damage, journal bounds/checksums, route changes, generic API transitions,
configuration changes and actual child-process exit during the first batch.
Worker-syscall failures and physical power-loss interleavings are not exhaustively
covered by the main-thread injection matrix. The previous ExFAT test results
apply to `ff728ea3`, not this new protocol.

Remaining validation includes isolated ExFAT and distinct-device recovery for the
new journal, representative large/sparse historical batches, paced live writes,
concurrent query behavior during failed writes, recovery memory/startup costs and
current Linux/macOS CI. The existing caller-shaped release fixture now invokes
the combined APIs, with unchanged input construction, final checkpoint timing and
exact post-reopen oracles. Original baseline executables still use the original
separate calls; comparisons must identify this changed durability strategy.

Column replacement ordering and variable-data rewrites are still candidates for
profiling if ingestion exceeds the 10% ceiling. The permission to start fresh
allows a format change where evidence justifies it; resetting a database by itself
does not remove write or synchronization overhead.
