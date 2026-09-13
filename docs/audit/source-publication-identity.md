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
Manifestless, unidentified and zero-row readers validate the committed state
before and after capturing handles. Identified nonempty native readers compare
the committed marker after capture with the identity from that captured manifest,
as detailed below. Publishing a marker only before or only after replacement
cannot exclude a mixed capture.
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

## Query capture optimization

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

The implementation is committed as `7bfcf4595fc4bc5df63c5d010fd8640af3585186`.
[Focused validation](baselines/2026-09-13-source-identity-capture-focused-1.json)
passes formatting, strict workspace Clippy, 251 storage tests, 80 index tests,
13 native query tests and 11 background tests. Five storage cases remain ignored
as described above. Deterministic hooks cover pending/missing/foreign markers
after file capture, prefix append and exact-prefix rewrite, and the retained
legacy/zero-row transition checks. Existing raw-to-paged unbundled compaction
coverage exercises manifest retry and retained file handles. Expected identity is
recomputed inside every retry iteration; changed-identity retry was reviewed in
source, rather than claimed as a new deterministic test.

The archive also retains failed attempts 8 and 9: three new cases initially failed
while their generation-3 fixture called a maintenance refresh before publishing
its first manifest; a later unreachable-branch cleanup required collapsing the
remaining condition for Clippy. The corrected fixture explicitly publishes its
initial raw schema. No production validation was weakened. Attempt 10 passes,
and its complete tested-source patch exactly matches the committed candidate.
The [isolated five-path comparison](baselines/2026-09-13-source-identity-capture-release.json)
against `8f7e1dcb` retains 10,000 timings, 100 explicit warmups and 20 RSS
observations. Block-hash point, block-number range, timestamp range, present-topic
and absent-topic medians change -6.87%, -4.23%, -3.19%, -4.41% and -8.18%.
Their p95 changes are -7.42%, -1.65%, -2.76%, -8.04% and -8.11%; RSS median
changes +0.36%. This supports retaining the scoped query optimization. The final
six-workload comparison against merged PR #149 is retained below; complete
workspace/release gates, exact-head CI and merge remain pending.

## Direct comparison with merged PR #149

The [direct release comparison](baselines/2026-09-13-source-identity-final-release.json)
retains all 20,000 timings, 100 explicit warmups and 120 RSS observations for
`9c0a58fc` versus `7bfcf459`. Raw live ingestion median/p95 changes are
+0.60%/-1.16% dense and +3.19%/+1.07% sparse, resolving the initial 18–20%
median regression. Actual live publication changes -0.28%/+5.62%; historical
publication +0.32%/+5.13%. These publication tails remain under investigation.

The short block-hash point and absent-topic median changes are +6.49% and +9.69%,
with p95 +10.18% and +11.22%. Other short-query p95 changes are +7.12–8.42%.
Dense engine p95 changes reach +11.95% while its medians are slightly lower;
dense concurrent p95 is +7.48%, and sparse count/reopen/index-build p95 changes
are +12.18%/+8.48%/+5.04%. None of these observations is discarded or treated as
an automatic pass. The complete table and absolute timings are in the evidence.

The [completed fixed investigation](baselines/2026-09-13-source-identity-final-tail-1.json)
repeats each affected complete workload with 20 balanced source pairs and 10
identical-candidate pairs: short paths, dense engine, dense mixed, sparse mixed
and actual publication. It retains 51,000 timings, 300 explicit warmups and 300
RSS observations using the exact saved artifacts. Controls remain separate from
source comparisons, and all initial samples are included in pooled source
statistics. Pooled raw live ingestion median/p95 changes are +1.58%/+3.29% dense
and +2.48%/+2.87% sparse; actual live publication +0.24%/-4.13%, historical
-1.73%/-3.97%. The initial large engine and publication tails do not repeat in
this fixed schedule. Dense mixed native-query p95 +9.70%, index-build p95 +5.24%,
sparse reopen p95 +5.22% and count p95 +5.84% remain explicit observations.

The empty-result topic query still exceeds the user's limit: median +9.69%
initially, +10.48% in confirmation and +10.98% with all source samples pooled.
Its pooled p95 is +8.50%; other short-query pooled median changes are +1.67–6.85%.
This candidate is not accepted. Identical-candidate short-query median differences
range -0.59% to -1.50%; their much larger negative tail differences illustrate
tail variability, but do not explain away the repeated source median regression.

A [third bounded sampling attempt](baselines/2026-09-13-source-identity-profiles-2.json)
completed the unchanged query workload but could not attach the sampler. It
provides no hotspot attribution; all instrumented observations are retained and
excluded from acceptance. Source review finds one marker read on the absent-bloom
path, with no redundant checkpoint marker read. A small experiment will replace
the trailing EOF read with a length check on the same opened handle, then read
the fixed payload. Immutable atomic replacement makes that handle's length stable
under the writer protocol. This retains the number of system calls and is not
assumed faster; it must demonstrate a benefit before retention. Full gates, CI
and merge remain pending.

The same-handle marker reader experiment is committed as
`b6505ee3e28d506a667aa7674a8775919b8abc12`.
[Focused attempt 11](baselines/2026-09-13-source-identity-marker-read-focused-1.json)
passes formatting, strict Clippy, 252 storage tests, 80 index tests, 13 native
query tests and 11 background tests. The fixed-size fixture checks valid,
one-byte-short and one-byte-long files; existing checksum/state checks remain.
The archive verifies exact equality between the tested and committed source
patch. Four unused test descriptor path labels now match their decimal segment
IDs. The README also explains the old-data compatibility condition rather than
suggesting an index rebuild can establish missing source identity.

Its predefined ten-pair comparison uses all five unchanged short-query paths
against `7bfcf459`. The first launch stopped before builds or timings because
the sandbox blocked the read-only hardware inventory; its log is retained.
The subsequent launch uses the same plan with the necessary execution permission.
The [completed isolated comparison](baselines/2026-09-13-source-identity-marker-read-release.json)
retains all 10,000 timings, 100 warmups and 20 RSS observations. Block-hash,
block-number, timestamp, present-topic and absent-topic median changes are
-3.52%, -2.91%, -3.13%, -4.22% and -3.05%, respectively; p95 changes are
-4.22%, -4.90%, -10.45%, -11.71% and -21.80%. RSS median changes -0.67%.
These measurements support provisional retention. A direct six-workload
comparison against merged PR #149 used the frozen candidate; full
acceptance is not inferred by multiplying earlier percentage improvements.
The [direct marker-reader baseline comparison](baselines/2026-09-13-source-identity-marker-read-baseline-release.json)
retains all 20,000 timings, 100 warmups and 120 RSS observations. Raw live
ingestion median/p95 changes are +1.47%/-1.88% dense and +2.16%/+8.79% sparse;
historical ingestion medians +0.17%/-1.14%. Actual live publication changes
+0.38%/+4.34%, historical +0.83%/-6.58%. Engine-query medians range -0.41% to
+0.07%. Sparse reopen p95 +6.86%, sparse count p95 +6.33% and sparse engine
aggregate p95 +5.15% remain explicit observations.

The short-query result still prevents acceptance: absent-topic median +10.56%,
p95 +25.21%; block-hash median +7.48%, p95 +27.32%. Other short-query medians
are +3.49–4.76% and p95 +9.28–22.76%. The isolated marker-read improvement does
not establish compliance with the direct baseline. Source review identified a
small heap-backed set in unbundled schema validation over a fixed 14-name domain.
The next experiment uses a stack array for exactly the same unknown-name,
duplicate-name and non-topic-null-bitmap checks, preserving existing permissive
unbundled completeness and topic-nullability rules. It will be validated and
measured independently before any retention decision.

The fixed-domain schema experiment is committed as
`7e411084385e80d6700978288d8e46dd89a63cb4`.
[Focused attempt 12](baselines/2026-09-13-source-identity-schema-focused-1.json)
passes formatting, strict workspace Clippy, 253 storage tests, 80 index tests,
13 native query tests and 11 background tests. The new finite fixture checks an
unknown descriptor name in an unbundled manifest; existing fixtures cover
writer rejection of duplicates and a null bitmap on a non-topic column. Those
writer cases exercise PageOutput/profile inspection, not the changed reader's
schema gate; the reader's duplicate/nullability equivalence was reviewed in
source. The stack array does not
require all names to be present or require topic bitmap declarations. Projection,
file capture and path validation remain unchanged. The tested-source/commit
comparison passes; the ten-pair isolated comparison against `b6505ee3` follows.

The [isolated schema comparison](baselines/2026-09-13-source-identity-schema-release.json)
retains 10,000 timings, 100 warmups and 20 RSS observations. Its small median
improvements did not survive the
[fixed confirmation and control schedule](baselines/2026-09-13-source-identity-schema-tail-1.json),
which retains another 30,000 timings, 300 warmups and 60 RSS observations.
Pooled source median changes are +0.06% block hash, -0.05% block number,
-0.07% timestamp, +0.45% present topic and -0.25% absent topic. Identical-candidate
median differences range -0.37% to -2.39%. Thus no improvement beyond observed
variability is established. The production stack-array change is rejected and
the original validation is restored; the unknown-column regression is retained.
All timing and memory observations, including tail changes, remain archived.

A [fourth short profiling attempt](baselines/2026-09-13-source-identity-profiles-3.json)
successfully samples the exact schema candidate with the required execution
permission. It contains usable artifact-capture and marker-reader call stacks
across all five query paths. The 1,000 instrumented timings and five warmups remain
separate; sampled counts do not quantify removable latency for an individual path.
The remaining investigation concerns validating source identity through already
captured canonical metadata, with explicit publication ordering and retained
recovery evidence. That experiment is being implemented; it is not accepted yet.

The schema optimization was reverted in
`b8754cc61de5494930ea11cad2f86d2f5d5e8dea`. The
[revert checks](baselines/2026-09-13-source-identity-schema-revert-focused-1.json)
pass formatting, strict workspace Clippy, the direct unknown-column reader
regression and the existing compacted-writer metadata regression. Production
source again matches `b6505ee3`; only the additional reader test remains.

### Canonical metadata experiment

The next experiment carries the source binding in the unbundled canonical
artifact already opened by queries. It retains the separate publication marker
as writer/recovery evidence. Generic null bitmaps and bundled streams retain
their encodings. This changes the new, still-unmerged identified raw format;
unidentified older sources remain scan-readable and index-ineligible.

Readers must capture selected noncanonical artifacts first and canonical metadata
last, then validate its committed identity against their captured manifest.
Canonical path aliases must be rejected: deduplicating file paths must never
allow a column descriptor to capture the canonical artifact early. Full source
replacement must order a pending canonical record before changing columns and
publish committed canonical metadata after every column replacement. Ordinary
append reuses its existing canonical replacement, with no additional marker
write, randomness or durability barrier. Exact-prefix recovery retains its
capability checks and verified prefix bits throughout interruptions.

A committed canonical snapshot can represent complete, coherent files even when
the separate recovery marker still requires finalization. Queries validate that
snapshot; startup, maintenance and writers continue to honor the recovery marker.
This distinction does not permit queries during node recovery or waive catalog
and WAL verification. All interleavings, recovery phases, compatibility behavior
and performance still require validation before this design can be retained.
