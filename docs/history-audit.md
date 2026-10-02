# One-time historical audit

Normal sync already validates contiguous authenticated execution headers, complete
transaction and receipt roots, exact persisted events, and verified coverage.
The optional audit independently re-fetches a frozen finalized range and compares
it against **every canonical physical event occurrence**. It detects missing and
extra events, altered fields, and duplicate identities across segments. Zero-event
blocks are checked too. It does not execute the EVM or recover arbitrary state.

This operation costs network bandwidth, CPU and disk I/O. It is disabled by
default, has no TOML configuration switch, and never starts automatically on a
normal restart. It does not modify primary data or repair a mismatch. A mismatch
stops the audit and preserves its evidence; normal sync and query service remain
running. Investigate the mismatch before deciding whether repair is appropriate.

## Start with a bounded pilot

Supply a JSON plan explicitly when starting the node:

```sh
logex --data-dir /path/to/existing/data sync --history-audit-plan /path/to/pilot.json
```

Keep the deployment's normal listener, authentication, consensus and volume
options. Resolve the plan path before volume supervision pins the process working
directory. A pilot waits for verified genesis-to-live coverage, fresh consensus,
connected peers and near-current live progress. It then freezes a finalized
consensus anchor and validates complete payloads for the most recent `new_blocks`.
It does **not** scan the complete primary store or claim complete event integrity.

Example finite pilot plan (replace `request_id` with a fresh nonzero 32-byte hex
identifier, e.g. the output of `openssl rand -hex 32` prefixed with `0x`):

```json
{
  "version": 1,
  "request_id": "0x1111111111111111111111111111111111111111111111111111111111111111",
  "scope": {"kind": "pilot"},
  "new_blocks": 64,
  "deadline_secs": 300,
  "network_batch_blocks": 4,
  "minimum_free_bytes": 10737418240,
  "source": {
    "segments": 20000,
    "total_rows": 10000000000,
    "segment_rows": 4000000,
    "retained_segment_bytes": 536870912,
    "decoded_segment_bytes": 1073741824
  },
  "manifest": {
    "sort_records": 65536,
    "merge_fan_in": 8,
    "scratch_bytes": 8589934592,
    "runs": 40000000
  },
  "journal": {
    "max_bytes": 34359738368,
    "max_chunks": 100000,
    "checkpoint_blocks": 64
  },
  "fetch": {
    "header_page": 64,
    "headers": 30000000,
    "transactions_per_block": 100000,
    "encoded_body_bytes": 67108864,
    "events_per_block": 1000000,
    "event_data_bytes": 67108864,
    "request_timeout_secs": 30,
    "attempts": 2,
    "max_transient_retries": 0,
    "retry_delay_secs": 5
  }
}
```

These are example work allowances, **not measured hardware recommendations**.
All fields are required; unknown fields fail. Adjust source limits to the actual
store. Header pages are at most 1,024 headers, payload batches at most 32 blocks,
attempts at most four, request timeout at most 60 seconds, and each invocation at
most seven days. The overall deadline includes waiting and local scans. A work
limit stops the audit instead of silently dropping rows or treating partial work
as complete. Limits are not a process RSS cap or an exact wire-byte spending cap.

Network requests use the existing node's peer connections. New requests are
admitted only when live sync is fresh and has no ready historical/forward work.
There is at most one owned network plan in flight; finite prefetched payloads are
still validated block by block. An already dispatched request or local source
scan can compete for resources, so idle admission does not promise zero impact.
Measure live progress, memory, CPU, I/O, disk space and duration during the pilot.
Recent payload availability does not establish archival availability for every era.

## Compare a frozen range

After measuring cost, explicitly use a new request ID and
`"scope": {"kind": "audit", "from_block": 0}` for genesis through the finalized
anchor captured when work starts. `new_blocks` limits **new comparisons in this
invocation**, not the frozen range. Set it and the deadline deliberately; the
header allowance must cover the whole frozen range, including on resume.

The audit first performs a complete physical source scan outside the storage
selection lock. This includes all segments, regardless of the chosen comparison
range; query indexes, declared block bounds and deduplication cannot hide rows.
Bounded external sorting groups exact field digests by block and logical event
sequence. Scratch limits count merge input and output together. The source is
held through comparison and rechecked with the finalized consensus anchor before
publishing a successful result. Appends and equivalent compaction are allowed;
canonical changes or closing storage invalidate the captured view.

For a full comparison, `fetch.max_transient_retries` can allow up to 32
recoveries from exhausted unavailable-peer requests within this invocation.
Each recovery checkpoints complete comparisons and retries only the next
unfinished block against the same manifest and anchor; it does not restart the
client or rescan storage. `retry_delay_secs` is 1–60 seconds, bounded by the overall
deadline. Counts and individual failures are reported. Invalid proofs, mismatched
events, local failures and cancellation are not retried. A pilot must set this
allowance to zero, so its payload trial is not silently repeated.

The journal records complete authenticated headers and compared event counts in
ordered, checksummed, immutable chunks. Its checksums are private local evidence,
not portable Ethereum receipt proofs. The job remains bound to its original
weak-subjectivity checkpoint, finalized anchor, source namespaces, exact selected
event digest and journal allowances.

## Resume and cancel

Repeating a start with an existing request ID is refused. An ordinary startup
without the audit flags performs no audit work, even when partial jobs exist.
Explicitly resume during a later startup with the same plan and request ID:

```sh
logex --data-dir /path/to/existing/data sync --history-audit-plan /path/to/audit.json --history-audit-resume
```

Resume requires a **new complete local physical scan**, identical frozen selection,
valid retained consensus ancestry, and a complete valid journal prefix. Normal
live tail appends do not require re-downloading the completed prefix. An unfinished
in-memory suffix can be fetched again. Unknown, damaged, reordered or truncated
journal chunks are rejected. Journal bounds and frozen scope cannot change on
resume. Invocation duration and new-block allowance can change. Pilots do not
resume; each new pilot needs its own request ID.

Cancel only this audit while keeping the running client active:

```sh
logex --data-dir /path/to/existing/data cancel-history-audit 0xYOUR_REQUEST_ID
```

Use the same volume guard options as the running node. The command records a
local request marker; it does not claim immediate cancellation. The worker checks
it at bounded scan/fetch boundaries, normally within five seconds outside an
active request. An active network wait may last up to its configured timeout.
Normal node shutdown also cancels and joins owned audit work. Explicit resume
revokes only the matching validated cancellation marker.

## Evidence and interpretation

Artifacts are kept inside the owned data directory under
`history-audits/request-<id>/`. `request.json` freezes identity;
`initial-plan.json` preserves the initial budgets; `progress.json` is replaceable
status; each `invocation-<time>.json` preserves its final outcome. Session chunks
are in a single owned `audit-*` subdirectory. Canceled or incomplete staging
artifacts are never promoted to successful evidence. Normal storage disk metrics
include these files. Preserve evidence outside source control.

Reports distinguish restored, compared and successfully checkpointed block counts.
Returned-prefix RLP byte counters exclude transport framing, requests, discarded
retries and residual responses; receipt blooms may be reconstructed by protocol
decoders. Node download counters also include concurrent live sync. **Neither is
billed wire traffic.** Measure the deployment's network counters separately before
estimating a full pass. A recent pilot alone is not representative of all eras.

`pilot_complete` means that finite payload validation completed.
`frozen_range_compared` means the declared frozen range passed exact primary-event
comparison and final source/anchor admission. Neither phase claims complete
release acceptance: exhaustive derived-index verification, restart/resume on the
actual deployment, finalized-tail reconciliation and query benchmarks remain
separate. Cryptographic, consensus and shared-library trust assumptions still
apply. No elapsed-time gate can substitute for these checks.
