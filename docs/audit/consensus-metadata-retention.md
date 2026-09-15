# Consensus candidate metadata and history recovery

This batch bounds optional Beacon metadata without discarding authenticated
ancestry. It follows the completed history-cost work in PR #171. It does not
establish a bound on all retained consensus history, complete the offline audit,
or establish release readiness.

## Finding and invariants

**B3-59 — unconnected range metadata had no retention limit.** Every successfully
decoded, request-shaped Beacon range body added a record to the root map and its
parent's child index. The separate raw-body cache had byte and entry limits, but
those limits did not cover these metadata structures. Repeated unrelated range
results could therefore accumulate metadata even when they supplied no path to
an authenticated light-client header. The reproduced effect is retained entry
growth; no timing percentage, memory exhaustion experiment, or incorrect query
result is claimed.

A decoded body has a computed root. That alone does not authenticate its execution
anchor. Existing publication still requires the exact checkpoint-to-target
ancestry, matching roots and strictly decreasing parent slots. This patch does not
change consensus signatures, execution validation, snapshot encoding, durable
write ordering, or successful-query semantics.

## Admission and eviction

The network retains at most 8,192 unconnected candidate records after a response
batch. A private FIFO tracks membership, queued entries and promotion tombstones.
Its queue is compacted to at most twice the configured capacity after trimming;
normal insertion and promotion use constant-time set operations. Reinsertions
remove an old queued occurrence before assigning a new FIFO position. Tests use
capacities zero and two; production uses the fixed limit above.

Existing restored anchors and verified light-client seed headers are protected.
The production by-root request builder selects exact missing roots from those
trusted targets and their committed parents. Its matched responses can therefore
promote metadata. A range entry is promoted when a retained protected child
commits to its exact root and has a greater slot. Promotion walks backward through
cached parents and stops at missing or already-protected ancestry. Attaching an
arbitrary forward child to a checkpoint does not confer this protection.

The complete bounded response is admitted and its authenticated ancestry promoted
before trimming. This preserves a connected response batch larger than the
candidate quota. Eviction removes both the root record and that record's edge in
its parent's child bucket. Edges from other retained children to the now-missing
root remain: they are needed to authenticate a later refetch. Duplicate insertion
and defensive metadata replacement maintain the same exact child index.

The body cache remains independently bounded and may retain a body after its
optional metadata is evicted. Local metadata eviction does not count as a peer
validation failure. Authenticated does not imply currently canonical: protected
old target and side-fork metadata remain outside this candidate limit.

## Recovery after pressure

A FIFO limit alone would be incomplete. With only range-serving peers, evicting a
checkpoint-connected forward prefix could repeatedly restart forward work before
it ever reached the trusted head. When candidate eviction occurs, a process-local
pressure flag enables a backward fallback for an incomplete active target.
Ordinary forward scheduling and its concurrency remain available when no such
backward gap exists. The flag remains set for the process lifetime.

The fallback starts from an authenticated optimistic or finalized header and
follows its committed parents to the first missing root. It searches bounded slot
windows below that child, down to the checkpoint floor. Range results confer
protection only through exact root linkage. Only one backward request is in flight
at a time; existing global history request limits still apply. By-root recovery
continues through its existing scheduler.

Each pending range owns its exact issued request and an optional shared scan
identity. The identity binds the active target, authenticated child and selected
peer. Pointer identity distinguishes a new pass even if all numerical fields
match an older pass. Timeout, cancellation, local resource failure, disconnect and
remote rate-limit handling release the same pending record. Unsuccessful attempts
do not advance the cursor. Pending backward work is excluded from forward-range
progress and its status count.

A valid partial response ending at slot S leaves the upper suffix starting at S+1.
An empty response consumes only its actual requested interval, including when the
local memory controller reduced the request count. Exhausting the original window
moves to the previous window. Changing peers resets the pass to the authenticated
child boundary; a usable current peer is preferred while its pass is active.

Empty windows do not prove absence, and exhausting a pass cannot publish missing
history as complete. A complete unsuccessful pass records the existing peer
availability failure and remains retryable. Intermediate empty/unrelated windows
do not reset those failures as useful successes. Search memory and each request
are bounded; total search time across repeated unavailable peers is not given a
new global limit by this change.

## Validation and implementation cost

Small deterministic tests cover FIFO limits and tombstones, exact child-index
reconstruction, protected ancestry, duplicate promotion, eviction without anchor
publication, a range-only chain longer than its candidate capacity, partial/empty
suffixes, integer boundaries, adaptive request counts, peer/target/pass changes,
timeout/resource failure cleanup, late replies, and unavailable-history passes.
One response-handler control decodes an existing SSZ body and verifies that a
modeled authenticated child's exact parent survives even at candidate capacity
zero. Synthetic metadata fixtures model the state after decoding and authenticated
header verification; they do not substitute for cryptographic protocol fixtures.

The original insertion method is substituted into the candidate test harness for
a bounded-retention regression. This is an isolated original-path comparison, not
a full original checkout. Candidate source is restored byte-for-byte afterward.
Independent source review covers request ownership, root provenance, promotion,
index consistency and recovery liveness.

No benchmark campaign, remote operation, dependency update, new persisted field,
or new unsafe block is introduced. Admission adds bounded in-memory bookkeeping;
it adds no storage writes or durability barriers. The fallback trades range
parallelism for bounded optional metadata after pressure. No throughput improvement
or regression percentage is inferred from these implementation observations.

The focused consensus suite passes 333 tests with one existing test ignored.
Workspace gates, final source reconciliation, CI and merge remain pending.
Evidence is recorded in the accompanying validation record when those checks finish.

## Remaining scope

Protected canonical anchors, authenticated obsolete forks and actual changed
snapshot rewrites need a separate justified lifetime/cost disposition. Arbitrary
pruning would break checkpoint ancestry, restart, execution selection or reorg
handling. This candidate cap is not a total metadata, snapshot, process-RSS or
query-memory budget. External-volume supervision and automatic offline repair
remain separate unfinished audit work.
