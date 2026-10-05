# Online index verification

Normal index publication verifies every derived membership before making that
publication available to queries. An explicit index audit repeats verification
against retained local source snapshots, without rebuilding indexes, changing
primary events, or downloading Ethereum data. Local index agreement does not
independently prove that the primary store contains every authenticated Ethereum
event; contiguous header, transaction-root, receipt-root and exact persisted-event
validation remain mandatory during ingestion.

Start this finite operation explicitly alongside the normal sync service:

```sh
logex --data-dir /path/to/existing/data sync --index-audit-plan /path/to/index-audit.json
```

Preserve the deployment's normal listener, authentication, consensus and volume
options. The plan is parsed before supervised storage pins the working directory.
There is no TOML switch, automatic restart job, repair, or network fetch. Each
request ID starts once; reusing a previously started ID is refused.

Example work allowances, **not measured hardware recommendations** (replace the
request ID with a fresh nonzero 32-byte hex value):

```json
{
  "version": 1,
  "request_id": "0x2222222222222222222222222222222222222222222222222222222222222222",
  "deadline_secs": 86400,
  "minimum_free_bytes": 10737418240,
  "max_segments": 20000,
  "max_total_rows": 10000000000,
  "max_segment_rows": 4000000,
  "max_retained_source_bytes": 536870912,
  "max_decoded_payload_bytes": 1073741824,
  "max_index_logical_bytes_per_segment": 536870912,
  "max_manifest_bytes": 67108864
}
```

All fields are required and unknown fields fail. Duration is at most seven days.
The source and index limits apply per opened segment, except the explicit total
row/segment limits. Appends after selection count against the opened-row limit.
These bounds constrain input work and logical lengths; they are not process RSS,
physical I/O or filesystem allocation guarantees. Work can compete with live
sync, queries and background indexing. Measure resources on the target machine.

The worker waits for fresh near-current sync and verified genesis coverage, then
captures a finite catalog selection while retaining storage ownership. It releases
the global selection lock before I/O and verification. Each source reader keeps
its artifacts; a shared publication lock binds all checked files to that exact
source namespace, prefix commitment and row count. Every published artifact is
checked, including optional B-trees beyond the required general-event filter and
emitter/event row index. Finish rebuilding the required profile before starting
verification. If a publication is withdrawn during a rebuild, locked, stale,
or lacks required bindings, the worker waits within the original job deadline.
It reopens only that source, rechecks its captured identity and budgets, then
verifies a matching publication. Previously completed segments are not repeated.
Waiting neither rebuilds indexes nor admits unverified rows. A missing published
artifact, corrupt checkpoint or index, unknown published file, changed source
view, exhausted limit or failed write stops verification immediately.
Unpublished loose files are not admitted query indexes and are not counted.

Every source membership is checked, including retained noncanonical rows that
queries later mask. Bloom false positives are allowed, but any missing required
bit fails. B-trees must contain exactly the expected row-to-key memberships, with
no missing, repeated or out-of-range row IDs. Every protected index page is read.
Verification never reopens mutable source paths between individual indexes.

Later appends may be included in a segment's opened prefix. Bounded raw reads
retain their preflight lengths and reject shrinkage; an append cannot expand the
read/allocation allowance. Offline inspection retains its stricter no-growth
rule. Equivalent compaction can retire paths while captured files remain usable.
A reorg or storage close invalidates the audit view. Empty captured prefixes
have no index memberships. New segments created after selection belong to a
subsequent tail check, so completion is not a statement about every future row.

Artifacts are stored under `index-audits/request-<id>/`:

- `plan.json` preserves the explicit request and bounds.
- `progress.json` is provisional status, including verified rows and logical bytes.
  `waiting_for_index_publication`, `waiting_segment` and `publication_retries`
  identify publication waits; they are not successful verification evidence.
- `segments.jsonl` records each verified source and all of its published file IDs.
- `result.json` records success or failure. Only `complete: true` and phase
  `captured_publications_verified` admit the complete selection. Its domain-separated
  BLAKE3 digest commits to the complete newline-delimited segment manifest.

The manifest is synced before success and the source view is re-admitted. Partial
progress or a surviving manifest does not constitute a completed audit. If a
publication remains unavailable until the deadline, the job fails with its partial
progress preserved. Investigate failures before starting another explicitly
identified pass. The verifier does not rebuild or skip an unavailable index.
Historical receipt proof, advancing-tail reconciliation and real-query correctness/performance remain separate gates.

To cancel only this job while keeping the node running:

```sh
logex --data-dir /path/to/existing/data cancel-index-audit 0xYOUR_REQUEST_ID
```

Use the running deployment's volume guard options. Cancellation requires an
existing matching plan and is cooperative at bounded read/decode/membership
boundaries; the command does not claim that the worker has already stopped.
Normal node shutdown cancels and joins its owned worker. Neither cancellation
nor an ordinary restart starts a replacement verification job.
