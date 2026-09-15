# Network payload accounting cost and meaning

This milestone removes duplicate execution-response encoding and compression
performed only for traffic counters. It makes their application-level estimate
explicit in status documentation and dashboard labels.

## Finding B4-19

**Severity: low, avoidable implementation cost and observability.** Every counted
successful EL header/body/receipt response was sized, encoded into a new RLP
buffer, and Snappy-compressed into another buffer solely for telemetry. A factor
of 1.45 then estimated wire overhead; status added another 2.75% of downloaded
bytes to upload as estimated TCP acknowledgements. These were neither observed
wire counters nor one consistent representation: receipt blooms may already have
been reconstructed after ETH69/70 decoding, while serving bodies used in-memory
size and requests used fixed-field estimates.

The pinned Reth integration exposes no practical retained transport-byte accessor
for these counters. Adding a custom transport integration would be a substantially
larger change. Retain cheap, explicitly approximate application accounting instead.

An instrumented valid Encodable fixture confirms that the original header metric
calls encoding again. An actual status control shows 96 recorded upload bytes
becoming 124 after 1,000 download bytes, despite no additional upload record. The
original serving-body metric reports memory size instead of encoded field length.
The original controls report two existing passes and six failures under the new
payload-accounting contract. The normalized-length controls document that unit
change; they do not establish six independent data-integrity bugs.

## Correction and retained limitations

Successful EL downloads use the existing normalized RLP encoded length of the
returned list. The pinned concrete header, transaction, body, receipt and vector
implementations measure lengths without building an encoded buffer. Remove both
telemetry compression helpers, the wire/ACK factors and the sync crate's direct
Snappy dependency. Snappy remains in the workspace for real protocol/storage uses;
no dependency version changes.

Serving bodies and transactions use their encoded field lengths instead of
in-memory size. Receipt sizing uses a zero bloom only to measure length: blooms
always encode as the same 256-byte string. The original values, bloom construction
for actual validation/encoding, response encoders and network payloads remain
unchanged. Typed receipt sizing reuses its measured inner length for the one-byte
type envelope, eliminating two additional walks over the same fields.

Counters still cover selected successful responses, request attempts and provider
lookups; serving lookup does not confirm transmission. Request field estimates
and sums of individually served records omit some outer list/request-ID envelopes.
CL still counts compressed gossip data and decoded RPC SSZ/context bytes, with
outgoing accounting at queue submission. Compression, framing, encryption,
retransmissions and acknowledgement traffic are not represented. The dashboard
therefore labels combined values **P2P payload rate (est.)**, explains their mixed
basis and notes that upload does not confirm delivery. Public field names, numeric
types, units, aggregation and rate windows remain unchanged.

Displayed numbers can change materially on the same workload because the basis
changes, including removal of compression and synthetic acknowledgements. They
must not be compared with prior values as a speedup or with OS network counters
as physical traffic. Length measurement still traverses nested fields; this is
not a constant-time or total-memory-bound claim. A future generic Encodable type
could use its default encode-based length, so retain the pinned concrete-type
assumption when extending these paths.

No measured speedup is claimed. The removal of full telemetry encoding/compression
buffers, discarded bloom work and repeated length traversal is established from
implementation and controls. There is no ingestion validation, scheduler, retry,
request ownership, persisted format or storage-write change. No benchmark or
mac-mini work is performed under the current audit policy.

## Validation

Six added controls cover encoding call counts, empty and populated normalized
header/body/receipt batches, independent upload totals and served body/transaction/
receipt lengths. They include historical post-state and typed receipts. The
existing Alloy-reference receipt matrix directly checks reported encoded lengths
for all five transaction types, both statuses, historical legacy post-state, topic
counts zero through four and data sizes around RLP boundaries.

The sync suite passes 385 tests (one ignored). A later assertion-only strengthening
of the receipt matrix is checked separately before workspace gates. Independent
review verifies concrete length implementations, the fixed-size bloom assumption,
typed envelope arithmetic, unchanged request behavior and the exact direct
lockfile edge removal. Dashboard checks verify syntax, unique IDs, description
references and unchanged JavaScript calculations; no browser-rendering claim.

See [the validation record](baselines/2026-09-16-network-payload-accounting.json).
All seven local gates pass on source `72c5a022`: vendor verification, formatting,
workspace check, strict Clippy, 1,489 workspace tests (23 ignored), documentation
tests and release node linking. All six CI jobs passed on head `e32fda5e` (run
`35024210393`); PR #182 merged as `76ecd184` after fresh exact-head/base checks.
Cache payload admission, aggregate continuation budgets, the broader offline
audit, volume supervision and verified repair remain open. Live sync and staging
acceptance follow offline completion. Mac-mini audit testing and obsolete artifact
cleanup remain complete.
