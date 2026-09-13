# Source publication identity

This batch follows merged PR #149 and is in progress. It addresses source identity
behind derived indexes; it does not claim the rest of the offline audit is done.

## Confirmed failures

Two bounded regressions on `ebd9e550` use disposable two-row datasets. Copying
an entire checkpoint/index directory from another equal-row source, and replacing
all source columns with different rows of the same count, both return no indexed
rows while an independent full scan finds row 0. The complete [before-fix evidence](baselines/2026-09-13-source-identity-reproduction-1.json)
retains the patch, commands, toolchain, logs and hashes, including both expected
failures. PR #149's individual file binding remains necessary but cannot establish
source identity when its checkpoint is copied with the files.

## Implementation direction and invariants

Storage will own a random namespace for each logical segment incarnation. Native
catalog descriptors and manifests carry the namespace; readers retain the
captured namespace and indexes bind to it alongside existing row, generation and
bundle metadata. Ordinary appends preserve the namespace and continue to publish
their existing row boundary. Representation-only compaction preserves logical
identity. This must not add a random draw or a durability barrier to each native
sync batch, or extra marker reads to bundled queries.

The committed raw marker's prefix length is not an evolving ingestion checkpoint.
Ordinary append validates the stable namespace, generation and segment ID under
source ownership, then uses the existing column/manifest/catalog publication.
Only a pending exact-prefix repair binds its authoritative recovery row boundary.

Legacy/raw replacement publishes an explicit updating state before replacing
same-name column files and a committed identity after completing publication.
Readers validate the committed state before and after capturing handles. A marker
updated only before or only after replacement cannot exclude a mixed capture.
Native manifests and raw markers must agree; a standalone raw replacement cannot
silently keep using the former native identity. Captured raw row boundaries must
remain fixed through prefix appends or yield an explicit error.

Missing legacy identity is a compatibility condition, not evidence that old
indexes are correct. Such data remains scan-readable but index-ineligible until
a complete storage-owned rewrite establishes identity; the diagnostic must
distinguish this from a transient rebuild. No automatic row-count-based identity
migration is planned. Existing native metadata without identity must likewise
remain explicit unless its established exclusive recovery/publication boundary
can safely supply identity without a new migration subsystem. Interrupted replacement must be resolved only by
a complete rewrite or existing verified recovery evidence. Never infer completion
from equal row counts, silently clear an updating state, or schedule an endless
background rebuild loop. Existing maintenance ownership and recovery boundaries
must remain consistent; query capture must not recursively acquire writer locks.

## Prefix-repair recovery boundary under implementation

Native recovery can rewrite a catalog-authoritative prefix without changing its
logical contents. Every old and replacement column must encode exactly the same
first N rows and canonical bits. A partially published set can therefore recover
that prefix only when an explicit prefix-repair transaction recorded the exact
source namespace, descriptor and N before replacement, under exclusive source
ownership. This is stronger than recognizing an arbitrary updating marker or
observing equal row counts. The private recovery capture must also cover WAL
verification that currently occurs before startup repair; ordinary readers never
receive a bypass. A generic full replacement cannot enter this recovery path.

Canonicality updates remain separate from index-key source identity: indexes
cover all source rows and canonical filtering happens during query execution.
Changing canonical bits is not a prefix-preserving repair. The existing canonical
publication path must serialize with prefix repair so it cannot change captured
bits during the transaction. Compaction and other source writers must obey the
same pending-state/ownership constraints. These are implementation requirements,
not claims that the new recovery protocol has already passed validation.

Review of the draft also requires native raw append to compare the expected
namespace, generation and segment ID while holding source ownership, before any
column mutation. Merely finding a committed marker is insufficient if a
standalone replacement completed after the storage handle opened. Raw canonical
updates must likewise own the source throughout capture, bitmap calculation and
publication. Acquiring a lock only when writing the bitmap leaves an intervening
replacement or append possible. Ordinary committed identity excludes segment
kind because sealing legitimately changes Hot to Sealed without changing the
logical source; an interrupted prefix-rewrite transaction still binds its exact
authoritative descriptor, including kind.

## Focused validation and remaining acceptance

Candidate `d117f8eb745c614cdc62069376350c117a3a8dbb` passes formatting, strict
workspace/all-targets Clippy, 245 storage tests, 80 index tests, 13 native query
tests and 11 background-worker tests. Four storage tests requiring isolated
distinct mounts and one explicit WAL benchmark remain ignored in that debug run. The [focused evidence](baselines/2026-09-13-source-identity-focused-validation-1.json)
verifies that the final tested source patch exactly matches this committed
candidate. It retains the fifth unsuccessful attempt and the passing sixth run;
the [earlier unsuccessful attempts](baselines/2026-09-13-source-identity-focused-failures-1.json)
retain all first four patches, complete logs and command results. Compilation,
fixture metadata and test expectation corrections are preserved, rather than
presented as a clean first run.

Implementation is not yet accepted. Required coverage includes copied complete
sets, equal-row replacement, separate native datasets, captured-reader lifetime,
prefix append, compaction, missing identity and interrupted publication/recovery.
Run focused tests, complete workspace/release gates and equivalent repeated
release measurements before committing an accepted implementation. Preserve every
observation and the existing unresolved query-tail measurements. Exact-head Linux
and macOS CI and a merged PR close this milestone; live sync remains deferred.

## Initial performance result and scoped optimization

The [initial release comparison](baselines/2026-09-13-source-identity-initial-release.json)
compares `d117f8eb` with merged PR #149 (`9c0a58fc`). Ten predefined balanced
pairs across six workloads retain 20,000 timings, 100 explicit warmups and 120
process RSS observations. Sources, unchanged fixtures, workspace-selected features,
fresh release artifacts and every emitted record are retained. Actual live and
historical publication medians changed -0.44% and -1.68%, respectively. However,
raw live ingestion regressed 20.13% for dense rows and 18.19% for sparse rows.
The block-hash point and absent-topic query medians regressed 10.64% and 14.55%.
This candidate is rejected on performance; the successful bundled publication
results do not excuse regressions in other supported paths. Dense historical
ingestion p95 also increased 122.77% despite its median changing only +0.08%;
that observation remains pending controlled follow-up.

The [profile evidence](baselines/2026-09-13-source-identity-profiles-1.json)
retains both attempts. An initial query invocation exceeded the fixture's bounded
repeat limit and failed before measurement. The corrected query invocation
completed but yielded no usable sampled call stacks. The raw mixed-workload
profile contains usable stacks in both newly added initialization marker
barriers. Sample counts support investigating those barriers, but do not estimate
removable elapsed time. Instrumented timings remain separate from acceptance
measurements.

The next draft limits native initialization to a catalog-authoritative zero
prefix. It retains a visibly pending marker before any column replacement, while
deferring both initial marker writes to the caller's existing manifest/catalog
tree publication. A crash before that publication can restore the authoritative
empty prefix and replay verified WAL. The initializer rejects nonzero or incomplete
markers before mutation; it cannot bypass recovery. Standalone replacement and
nonzero prefix repair retain their existing ordering. Query capture is unchanged
in this draft so the initialization bottleneck can be measured independently.
The initializer is committed as `8f7e1dcb304551e9dfb78ea1e3c0636748231a4d`.
[Focused attempt 7](baselines/2026-09-13-source-identity-initialization-focused-1.json)
passes strict Clippy, 246 storage tests, 80 index tests, 13 native query tests and
11 background tests, with an exact tested-patch/commit comparison. Its separate
release comparison is retained in the
[initialization evidence](baselines/2026-09-13-source-identity-initialization-release.json).
Against `d117f8eb`, raw live ingestion medians improve 15.95% dense and 15.24%
sparse; p95 improves 10.62% and 19.61%. Actual live/historical publication medians
change -0.30%/-0.34%. All 4,000 timings and 60 RSS observations remain retained.
Dense historical p95 +123.79%, dense index-build p95 +6.50%, and publication RSS
median +6.68% require investigation. A predefined confirmation and identical-binary
control schedule uses unchanged artifacts and workload parameters.
The [completed investigation](baselines/2026-09-13-source-identity-initialization-tail-1.json)
retains 8,000 further timings and 120 RSS observations, including both identical-base
and identical-candidate dense controls. Controls are never pooled with source
comparisons. The 20-pair dense source confirmation changes live-ingestion median
-15.04%, historical p95 +1.16%, and index-build p95 -0.69%. Pooling all original
and confirmation source samples gives dense live median/p95 -14.96%/-11.59%,
historical +0.10%/+4.72%, and index build -1.42%/-0.09%. Identical-candidate
historical and index-build p95 vary +9.38% and +7.96%; the initial large historical
tail difference is not reproduced by the fixed source confirmation.

Publication source confirmation changes RSS median +0.82%; its identical-candidate
control changes RSS median/p95 +2.73%/+12.22%. All original and confirmation
publication source samples give live median/p95 -2.04%/-3.49%, historical
-0.37%/+0.97%, and RSS +4.73%/+1.40%. The confirmation alone has historical p95
+15.59%, following -4.54% initially; it remains visible in the evidence. No uniform
tail pass is claimed. The repeatable raw ingestion benefit supports retaining the
initialization optimization. These are comparisons against the preceding
implementation candidate, not final acceptance against merged PR #149.

## Query capture optimization under implementation

An identified nonempty native manifest already supplies the expected namespace,
generation, segment ID and visible row boundary. Validate the committed marker
against that expected binding after all selected file handles are pinned. A full
replacement rotates the namespace, while append and exact-prefix recovery preserve
the captured logical prefix. This permits one marker read for that path. Every
manifest retry must derive its expectation from the same manifest used to capture
files. Missing, pending or foreign identity still produces an explicit error.
Bundled readers continue to use their captured manifest without a raw marker.

Keep existing before/after checks for zero-row, unidentified and manifestless
readers. This restriction catches marker transitions during capture; it does not
claim that a stable new canonical bitmap cannot coexist with an older zero-row
manifest. Canonical bitmap readers already allow newer append bits. Current query
callers enforce the captured row boundary and return no candidates for zero rows
before reading canonical bits. No wrong query result was demonstrated in that
stable zero-row case. Broader snapshot/caller review remains in batch 7.
