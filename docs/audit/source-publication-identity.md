# Source publication identity

This batch follows merged PR #149 and is in progress. It addresses source identity
behind derived indexes; it does not claim the rest of the offline audit is done.
The current source checkpoint is `c229ace0`, with production behavior restored to
`179e0ff7` and all nine local workspace/release gates passing. The latest
[direct mac-mini comparison](#completed-direct-mac-mini-comparison) completed
576 fixture processes and 96,000 measured timings with every correctness check
passing. Independent arithmetic review agrees with the result: 61 of 62 primary
endpoints are numerically below the 10% limit; sparse COUNT p95 remains uncertain.
The full packet remains INCONCLUSIVE. Read-only query-path review found no
COUNT-specific defect or unnecessary work. Mac-mini was released at
2026-09-13 19:39:38 UTC, with no audit processes remaining. A separately frozen
sparse confirmation is now in preparation on that host. Performance clearance,
exact-head Linux/macOS CI, PR and merge remain pending.
Earlier candidates and rejected experiments are retained below as audit history,
not separate accepted implementations.

## Confirmed failures

Two bounded regressions on `ebd9e550` use disposable two-row datasets. Copying
an entire checkpoint/index directory from another equal-row source, and replacing
all source columns with different rows of the same count, both return no indexed
rows while an independent full scan finds row 0. The complete [before-fix evidence](baselines/2026-09-13-source-identity-reproduction-1.json)
retains the patch, commands, toolchain, logs and hashes, including both expected
failures. PR #149's individual file binding remains necessary but cannot establish
source identity when its checkpoint is copied with the files.

## Implementation direction and invariants

Storage owns a random namespace for each logical segment incarnation. Native
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
before and after capturing handles. Identified nonempty native readers capture
selected columns first and canonical metadata last, then compare the canonical
identity with the captured manifest. The canonical envelope and ordered publication
protocol are detailed below; earlier marker-reader experiments remain recorded
as history. Startup and writers additionally validate the sidecar under source
ownership. A standalone raw replacement cannot
silently keep using the former native identity. Captured raw row boundaries must
remain fixed through prefix appends or yield an explicit error.

Missing legacy identity is a compatibility condition, not evidence that old
indexes are correct. Such data remains scan-readable but index-ineligible;
the diagnostic distinguishes this from a transient rebuild. Existing native
unidentified segments currently require a fresh sync into a new directory for
indexing. No in-place identity migration command or automatic row-count-based
identity migration is provided. Native recovery and representation compaction
preserve absent identity. A standalone full raw rewrite establishes its own
identity but does not migrate the native catalog/manifest. Interrupted replacement must be resolved only by
a complete rewrite or existing verified recovery evidence. Never infer completion
from equal row counts, silently clear an updating state, or schedule an endless
background rebuild loop. Existing maintenance ownership and recovery boundaries
must remain consistent; query capture must not recursively acquire writer locks.

Downgrade rejection is not guaranteed. Native catalog/storage version numbers
remain unchanged, and older binaries may ignore added identity fields in
otherwise compatible bundled data; raw canonical envelopes also differ from
their plain-bitmap reader. Do not use an older binary on a directory written by
this version. Operational rollback uses a preserved pre-upgrade directory or
backup. This is one-way upgrade guidance, not a tested downgrade migration.

## Prefix-repair recovery boundary

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
same pending-state/ownership constraints. The focused recovery checks are recorded below; complete acceptance remains
pending.

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

Caller review exposed two necessary integration changes. Startup integrity and
maintenance validation previously obtained sidecar checks indirectly through
`SegmentReader`. Once identified query snapshots use the canonical envelope,
those callers need explicit sidecar authority checks. Otherwise a coherent
query snapshot could incorrectly authorize startup or further mutation after
missing, foreign or incomplete recovery metadata.

Paged unbundled append also lacked segment-source ownership: its preceding
compaction releases the segment lock before append begins. The native data
directory lock uses a different inode from standalone column replacement's
segment lock. Append must acquire that same source owner through inspection and
publication. Bundled append retains its existing path. These changes belong to
the source-identity contract and require focused regressions; they are not
evidence that the implementation has already passed validation.

The canonical experiment is committed as
`0841db0d11380ed2d461a7639f684cc7e819dd37`.
[Focused attempt 15](baselines/2026-09-13-source-identity-canonical-focused-1.json)
passes formatting, strict workspace Clippy, 262 storage tests, 80 index tests,
13 native query tests and 11 background tests. Five existing storage checks
remain ignored. The final source patch is identical after every passing check
and exactly matches the committed source diff. Attempt 14 stopped on two newly
test-only helpers; its initial patch and complete logs remain retained, without
claiming a post-format failure snapshot that the older runner did not collect.

The raw envelope has a 54-byte checked header followed by the unchanged bitmap
length and bits. Header validation covers version, state, binding and row count;
the count must match the bitmap length and supported addressing, and the physical
framed length is exact. This header checksum does not authenticate bitmap bits.
Legacy query decoding and valid relative canonical paths remain supported;
legacy writer/integrity paths retain their stricter exact-length checks.

Regression coverage includes replacement before and after canonical capture,
all canonical descriptor alias fields, append rejection before column mutation,
and interrupted verified-prefix publication at five stages, including the final
directory-order barrier. Reopen must recover the exact three committed rows and
preserve a noncanonical bit while discarding the two-row unpublished suffix.
Both pending and completed canonical metadata with an unfinished sidecar are
covered. Missing sidecar evidence cannot be reconstructed from a coherent query
snapshot, even when an appended tail would otherwise initiate recovery. Bundled
recovery remains separate and does not require a raw sidecar.

The [predefined isolated ten-pair comparison](baselines/2026-09-13-source-identity-canonical-release.json)
against `b6505ee3` retains 10,000 timings, 100 warmups and 20 RSS observations
from unchanged release fixtures. Median/p95 changes are -3.75%/-6.57% for block
hash, +2.93%/+0.42% for block number, +2.93%/-3.75% for timestamp,
-1.83%/-8.67% for present topic and -5.75%/-8.60% for absent topic. RSS changes
+0.08% median/+2.69% p95. This supports continuing the experiment but does not
establish a direct baseline pass or improvement on every path.

Further review found an authority/capture race in `0841db0d` startup integrity:
the explicit sidecar check preceded query capture without owning the segment.
A standalone writer could enter its pending state between those operations,
while the new query reader correctly accepted the still-coherent old canonical
snapshot. Integrity validation must retain the bound/legacy source owner across
all its file reads. Its callers do not retain a segment guard; maintenance already
does and must not recursively acquire it. This correction and its regression are
required before direct six-workload baseline acceptance and complete gates.
No live or external-volume operations have been performed.

The ownership race is fixed in
`d46a6cdf35ce3cca675be9c0b9076b0f740e7cc6`. The
[reproduction and focused validation](baselines/2026-09-13-source-identity-integrity-focused-1.json)
retain the test-only before-fix patch: one compiled test failed because a writer
acquired the source during canonical capture and integrity returned `Ok(())`.
The corrected function owns the nonzero unbundled source through all validation
reads. The regression checks both orderings: a writer is excluded during
verification, and verification cannot begin while the writer owns the source.
Bundled/zero-row paths and maintenance ownership are unchanged.

Focused attempt 16 passes formatting, strict workspace Clippy, 263 storage tests,
80 index tests, 13 native query tests and 11 background tests. Five existing storage
checks remain ignored. The archive verifies the complete before-fix failure and
equality of tested and committed source. The direct six-workload release
comparison against merged PR #149 has completed; final performance acceptance,
full gates, CI and merge remain pending.

### Direct canonical baseline and fixed investigation

The [direct comparison](baselines/2026-09-13-source-identity-canonical-baseline-release.json)
of `d46a6cdf` with merged PR #149 retains all 20,000 timings, 100 explicit
warmups and 120 process RSS observations. Actual live publication median/p95
changes are +0.03%/-9.03%; historical publication -0.07%/-6.02%. Raw live
ingestion medians are +3.69% dense/+2.02% sparse. The five short-query medians
range +1.13% to +5.52%, with absent topic +2.91%.

Block-number/timestamp medians +5.06%/+5.52%, sparse reopen p95 +14.94%,
sparse ordered-query p95 +5.57%, and publication RSS median +5.43% trigger
investigation. A fixed schedule adds 20 balanced source pairs and 10 identical-
candidate pairs for each of the three complete affected suites, retaining
36,600 timings, 300 warmups and 180 RSS observations. It reuses the exact saved
binaries and fixture parameters. All initial and confirmation source samples
will be reported separately and pooled; controls remain separate. No results
are removed, and the schedule will not change in response to intermediate values.

Read-only caller review during those measurements found a legacy recovery
ownership gap: the unidentified branches of `restore_committed_prefix` and
`rebuild_partial_raw_segment` capture rows/bits before the rewrite helper obtains
its source lock, and release that helper's lock before publishing the manifest.
A concurrent standalone replacement can enter those gaps. This requires a bounded
regression and correction after the frozen comparison, preserving legacy index
ineligibility and holding one owner through capture, rewrite and publication.

The same unidentified rewrite emits a plain canonical bitmap even when a committed
incidental sidecar exists. Later append expects the corresponding canonical
binding and rejects that mismatch. The correction must preserve the borrowed
owner's canonical binding while leaving catalog/manifest identity absent.
Recover-then-append tests must exercise both incidental-sidecar and truly unbound
legacy sources; ordinary reopen alone does not directly cover both recovery
helpers.

The [completed fixed investigation](baselines/2026-09-13-source-identity-canonical-tail-1.json)
retains all 36,600 additional timings, 300 warmups and 180 RSS observations.
Pooled short-query median/p95 changes are +1.91%/+1.87% block hash,
+4.69%/+4.31% block number, +5.05%/+3.97% timestamp, +0.75%/+0.53%
present topic and +2.16%/+0.50% absent topic. Confirmation-only medians range
+0.72% to +4.76%; identical-candidate median differences range -0.16% to -0.29%.
The remaining roughly 5% range-query cost is explicit.

Pooled actual publication changes are +0.10%/-2.67% live and -0.27%/-12.29%
historical, with RSS -0.59%/-1.22%. The initial RSS increase does not repeat.
Pooled sparse live ingestion is +2.49% median/+11.13% p95, and reopen
+3.35%/+12.01%; these tails prevent performance acceptance. Confirmation-only
tails are +9.89%/+8.02%, while identical-candidate tails change +0.21%/+1.33%.
Neither the small medians nor the publication results excuse those observations.
Sparse historical ingestion is +1.20%/+3.23%; all sparse query medians are within
1%, with pooled p95 no higher than +2.10%. Every initial/source/control result
remains visible.

The [fifth profiling attempt](baselines/2026-09-13-source-identity-profiles-4.json)
uses the exact saved baseline and candidate binaries sequentially with ten-second
sampling of the complete sparse workload. Both workloads and samplers complete.
All 1,440 instrumented timings are retained separately from acceptance. Reopen
stacks include the existing root-directory device synchronization in both builds;
source-marker and integrity reads also appear in the candidate. Multi-phase
sampled counts do not prove which cost causes the tail difference or quantify
removable latency. Required durability operations remain in place.

The legacy regression patch compiles and runs ten tiny tests on the unchanged
`d46a6cdf` production behavior: eight expected failures and two passing controls.
Both private recovery branches allow replacement after capture and before manifest
publication, read before obtaining a writer-first lock, and fail subsequent append
when an incidental sidecar is retained. The two no-sidecar append controls pass.
The complete patch, commands, toolchain, logs and hashes are retained for the
focused correction's evidence packet.

### Legacy recovery correction

Commit `66d511ceac9bb086f6391acc67c7d21cfdb2709e` holds one legacy source owner
from before capture through rewriting, manifest/catalog publication and cleanup.
The private rewrite helper borrows that owner, checks its directory and preserves
any incidental canonical binding. Catalog/manifest identity remains absent, so
legacy data remains index-ineligible. The
[complete reproduction and focused checks](baselines/2026-09-13-source-identity-legacy-focused-1.json)
retain all eight before-fix failures, two controls and the exact corrected source.
Focused attempt 17 passes formatting, strict workspace Clippy, 273 storage tests,
80 index tests, 13 native query tests and 11 background tests. Five existing
storage checks remain ignored. The writer-first cases also verify unchanged
artifact and catalog bytes. Both private recovery branches are called directly.

Source review found repeated directory preparation introduced by the initial
identity implementation: acquisition prepares a directory, and both private owned
write helpers prepare it again. Existing-source append and integrity acquisition
also prepare the directory before opening it. A bounded separate experiment will
remove verified duplicate preparation and distinguish existing-source ownership
from allowed initialization, while preserving same-handle directory checks,
zero-prefix recovery, locks and all publication barriers. Its performance benefit
is not yet measured. The earlier pooled sparse tails remain unresolved.

Entry-point review also identified two older public-append gaps. Its absence
observation precedes ownership, so a concurrent completed initialization can be
overwritten by a stale initialization decision. An absent directory also enters
full initialization even if the caller supplied a nonzero expected prefix.
The next bounded regressions will require preserving a concurrent publication
and rejecting a missing nonzero prefix without creating files. These correctness
requirements are independent of whether removing redundant preparation improves
measured performance.

### Existing-source acquisition and absent append

Commit `179e0ff7c7b2816c96d1a62195fa75b2cf8d3b09` opens an existing source
directly, checks directory type on the same handle and acquires the same inode
lock. Initialization prepares its directory once; private owned-write helpers
no longer repeat that preparation. Missing nonzero sources are not created by
mutation, verification or recovery ownership. Explicit zero-prefix recovery keeps
its existing initialization capability; ordinary startup still requires every
catalog segment directory to exist. No dependencies or durability barriers change.

Public append rejects a missing nonzero prefix before creation. A zero-prefix
NotFound fallback rechecks marker and directory contents while holding ownership;
a completed, interrupted or partial source that appeared meanwhile yields a
conflict without replacement. The
[reproduction and focused evidence](baselines/2026-09-13-source-identity-directory-focused-1.json)
retains both before-fix failures and the complete directory-preparation draft in
which they ran. This was not a test-only diff against `66d511ce`. Focused attempt
18 passes formatting, strict Clippy, 279 storage tests, 80 index tests, 13 native
query tests and 11 background tests; five existing storage checks remain ignored.
Additional cases cover directory type, missing paths, lock exclusion, partial/
interrupted initialization preservation, and four zero-row legacy startup/direct-
recovery scenarios followed by first append and reopen without identity promotion.
Independent review found no additional correctness defect in this draft.

A fixed ten-pair [release comparison](baselines/2026-09-13-source-identity-directory-release.json)
with `66d511ce` completed on both complete mixed workloads and actual publication,
using unchanged parameters and fixtures. It retains all 4,000 timings and 60 RSS
observations. The required append
correctness fixes are independent of any measured preparation benefit; source
inspection alone does not justify claiming a performance improvement. The earlier
pooled sparse tail differences, direct final-baseline acceptance, all workspace/
release gates, CI and merge remain pending.

Independent retention review distinguishes correctness/cleanup from optimization.
The absent-append checks prevent reproduced overwrites and missing-prefix loss.
Separating existing acquisition from explicit creation prevents missing nonzero
sources from being manufactured by an ownership check. The two removed private
preparations are obsolete under their verified prepared-directory ownership
precondition. Retain this coherent subset for those reasons; do not claim a
speedup from syscall counts or sub-noise median changes. Direct-baseline
performance acceptance remains mandatory.

Before taking any final direct samples, the next schedule is predefined against
merged PR #149: ten initial balanced source pairs for all six unchanged workloads,
then twenty more source pairs and ten identical-candidate pairs for every suite,
regardless of initial outcomes. This yields 30 source pairs per suite and retains
80,000 total timings, 400 explicit warmups and 480 RSS observations across both
phases. Controls remain separate from the pooled source results. That comparison
started only after completing and packaging the directory confirmation below.

The [completed directory investigation](baselines/2026-09-13-source-identity-directory-tail-1.json)
retains all 12,000 additional timings and 180 RSS observations from twenty source
pairs and ten identical-candidate pairs for each complete affected suite. Pooled
source live-ingestion median/p95 changes are -0.54%/-0.07% dense and -0.10%/+0.13%
sparse; historical ingestion -0.10%/+1.14% dense and +0.02%/+0.01% sparse.
Actual publication changes are -0.84%/-0.78% live and +0.24%/-1.86% historical.
These small median changes do not establish a preparation speedup.

Dense concurrent-query p95 remains +13.65% pooled and +14.07% in confirmation,
versus +3.73% in the identical-candidate control. It remains unresolved. Sparse
concurrent-query p95 is +2.26% pooled, -7.40% in confirmation and +14.90% in the
identical-candidate control. Sparse count p95 is +5.75% pooled, +3.32% in
confirmation and -10.23% in the control. Historical publication p95 is +9.40% in
confirmation but -1.86% pooled; its control changes -0.39%. Publication RSS
median/p95 changes -3.07%/-3.34% pooled, while its identical-candidate median
changes +9.10%. Every observation remains retained; these differences are not
blanket-dismissed as noise or treated as a direct-master acceptance result.

The final direct comparison uses merged `9c0a58fc` and corrected `179e0ff7`.
Source and HEAD remain frozen through both predefined phases. Complete local
gates, platform CI and merge follow only after assessing those results.

Its [initial ten-pair phase](baselines/2026-09-13-source-identity-complete-baseline-release.json)
is complete and retains all 20,000 timings, 100 warmups and 120 RSS observations.
Live ingestion median/p95 changes are +2.05%/-6.77% dense and +1.93%/-1.20%
sparse; historical ingestion +0.71%/-3.27% dense and -0.11%/-1.57% sparse.
Reopen changes +3.42%/-0.67% dense and +1.83%/+1.80% sparse. Actual publication
changes +0.11%/-3.16% live and -1.68%/-7.73% historical. Publication RSS median
is -6.57%; all other initial RSS medians are within 0.5%.

Short-query medians range -0.77% to +3.47%, while p95 changes range +5.96% to
+11.35%. Dense engine query medians range +0.74% to +1.18%, with p95 +15.83%
to +18.29%. Sparse engine medians are within 0.12%, with p95 -4.56% to -8.68%.
Mixed query medians are within 0.58% and their p95 increases do not exceed 2.16%.
The larger short-query/dense-engine tails require investigation. The already-
declared follow-up is running for every suite, using the same saved binaries;
the initial results do not alter its scope or establish final acceptance.

## Final direct performance disposition

The [complete fixed follow-up](baselines/2026-09-13-source-identity-complete-tail-1.json)
is retained with its initial phase: 80,000 timings, 400 explicit warmups and 480
RSS observations. Every suite has thirty source pairs and ten separate
identical-candidate pairs. The verifier checks the predeclared schedule, input
hashes, saved binaries, fixture parameters, all raw logs/observations, per-process
summaries, pooled source statistics and archive roundtrip. Source and HEAD stayed
at `179e0ff7` through both phases. No source observations are discarded, and
controls are never pooled with source comparisons.

Selected pooled source changes against merged `9c0a58fc`:

| Workload | Median latency | p95 latency |
| --- | ---: | ---: |
| Actual live storage publication | +0.58% | +3.60% |
| Actual historical storage publication | -0.12% | +6.91% |
| Dense raw live ingestion | +2.97% | +5.71% |
| Sparse raw live ingestion | +1.94% | -0.05% |
| Dense raw historical ingestion | +0.08% | -0.47% |
| Sparse raw historical ingestion | -0.22% | -1.27% |
| Dense reopen | +4.66% | +3.13% |
| Sparse reopen | +2.21% | +6.40% |
| Dense compaction | +0.97% | +6.03% |
| Sparse compaction | +0.31% | -0.90% |
| Short block-hash lookup | +1.70% | +8.69% |
| Short block-number range | +4.80% | +10.16% |
| Short timestamp range | +4.84% | +11.54% |
| Short present-topic lookup | +0.92% | +7.71% |
| Short absent-topic lookup | +2.07% | +11.04% |
| Dense concurrent native queries | -0.09% | -0.76% |
| Sparse concurrent native queries | +0.73% | +11.55% |

Dense engine query medians range +0.09% to +0.38%, with p95 +1.65% to +3.17%.
Sparse engine medians range +0.002% to +0.26%, with p95 -1.70% to +0.70%.
Remaining mixed-query medians are within 0.94%; sparse native-filter/count p95
changes are +6.54%/+6.50%. Pooled RSS medians are within 0.90% except publication
at +2.34%; all pooled RSS p95 changes are within 1.46%. Complete values, including
every phase and control, are in the linked report.

These measurements do not show a large ingestion slowdown. They do not establish
complete performance acceptance: the pooled short-query tail exceedances remain
unresolved. The initial and confirmation latency regimes differ, and a pooled
quantile is not an average of phase quantiles. Short-query controls taken later
have tight tails but cannot retrospectively describe the initial regime. Analyze
retained phase/process/order distributions and paired process-level uncertainty
before deciding whether separate attribution instrumentation is needed. Do not
replace the pooled results with favorable confirmation values or subtract control
percentages. No further acceptance runs are scheduled.

Sparse concurrent-query confirmation p95 is +17.92%, while its identical-candidate
control is +17.09%. This demonstrates substantial variation without changing
source, but does not cancel the pooled +11.55% observation or prove its entire
difference is unrelated to source. Dense compaction p95 is +13.78% in confirmation,
-3.37% initially and +0.01% in the control, with the pooled +6.03% retained.
Historical publication p95 is +18.74% in confirmation, -7.73% initially and
-3.56% in the identical-candidate control, versus +6.91% pooled. This phase
excursion is included in the retained-data investigation because ingestion is a
priority; its pooled median -0.12% does not erase the tail observations. Live
publication confirmation p95 is +8.77%, versus +3.60% pooled and +0.13% control.

Read-only compaction review identifies necessary new raw sidecar authority and
manifest/canonical capture validation under the existing maintenance owner.
Routing and raw-completeness checks subsequently reopen some of that metadata;
safe reuse would need to retain exact row counts and legacy physical lengths,
which have stronger requirements than ordinary captured-prefix validation.
Existing tree publication also flushes the new sidecar. Codecs, worker/write
logic and maintenance locking are unchanged; no compaction random draw or raw
canonical rewrite was added. These are attribution leads, not measured removable
latency or permission to omit integrity/durability checks.

The [retained-data analysis](baselines/2026-09-13-source-identity-distribution-1.json)
reuses 44,800 selected observations and 400 warmups from the archived schedule;
it creates no new workload measurements. Configuration and seeds were declared
before each analysis. A paired process bootstrap uses 4,000 draws per domain and
workload family, preserving initial/confirmation strata and keeping controls
separate. Independent verification checks the completed-file inventory, input
hashes, original result-line references, process summaries, every selected pair
in the random sequence, all 80,000 resampling draws and all 44 published point
comparisons. All outputs and draws remain archived.

For the five short-query paths, 127–129 baseline and 119–123 candidate observations
in each 151-observation pooled upper tail come from the initial phase, although
that phase supplies only a third of the source timings. Initial pairs 4 and 5 are
the largest contributors. They remain included. Descriptive p95-change intervals
for block-number, timestamp and absent-topic queries are respectively
[+2.26%, +12.91%], [+1.69%, +13.79%] and [-1.34%, +12.02%]. Their median changes
remain positive. Sparse concurrent-query p95 has [+3.67%, +21.90%], and historical
publication p95 [-4.39%, +25.71%]. These unadjusted conditional intervals neither
establish a performance pass nor prove a greater-than-10% causal source cost.
They do not replace the fixed comparison or provide simultaneous guarantees.

An analysis-manifest provenance limitation is explicit in the packet: an
intermediate manifest hashed its redirected log before the final status line,
and an early finalizer refreshed that metadata. Refreshed manifests and labeled
reconstructions are retained; reconstructed metadata is not presented as a
preserved original. The final stable inventory seals the completed files.
Benchmark inputs, scripts, configurations, timing records and resampling draws
remain unchanged, and the independent verification above uses the committed
benchmark evidence rather than trusting the intermediate manifests.

Performance acceptance remains unresolved. The completed
[native stage diagnostic](baselines/2026-09-13-source-identity-attribution-1.json)
uses identical instrumentation in disposable `9c0a58fc`/`179e0ff7` source copies.
The predefined 12 rounds interleave source pairs with identical-baseline and
identical-candidate controls, balancing process and pair order. All 48 processes,
24,000 measured queries, 240 warmups and 20 separate smoke queries passed the
existing independent row oracle. Stage sums reconcile exactly for every query;
all expected index checkpoints were available. Source archives, original and
formatted patches, fresh workspace-selected release builds, raw logs, counters,
analysis and scripts are retained. Independent verification matches all 48,570
fixture records to original log lines and recomputes group statistics and actual
tail-query stage contributions. No instrumentation enters production.

Absent-topic queries always exit through three bloom exclusions, with no candidate
lookup, canonical bitmap read or row materialization. Their internal mean grows
15.20 microseconds: projected capture contributes 9.71, checkpoint validation
4.56 and bloom checks 0.84 microseconds. Full capture contributes 7.60–10.17
microseconds of mean difference for the two range cases and 25.41 for present-topic
queries. Candidate round 03 supplies 38–53 of the 61 observations at or above each
case's pooled p95. All remain included; the separate identical-candidate
present-topic control itself has a +13.65% p95 difference. Actual total-query tail
vectors are retained; stage quantiles are never summed.

Instrumentation perturbs timing and these results do not establish acceptance or
source causality. They identify file capture and checkpoint validation as the
next areas to examine. The schema-name BTreeSet and selected-path Vec already
exist in the baseline, so they are not newly introduced allocation costs. The
current narrow experiment removes a seek from the new canonical metadata read
on Unix by reading exactly at offset zero. It retains the same pinned file,
metadata length, bounded prefix, mutex and all parsing/identity checks; other
platforms retain the existing seek/read fallback. A framed/legacy regression
checks metadata after complete reads have advanced the captured handle to EOF.
Focused19 passes formatting, strict workspace Clippy, 280 storage tests
(five existing ignored), 80 index tests, 13 native-query tests and 11 background
tests. Independent review found no validation or pinned-handle defect. An
equivalent release comparison remains pending; no benefit or retention decision
is claimed yet. Cursor non-mutation is not a new public contract: the regression
checks metadata correctness after a previous read, and performance measurements
will determine whether the syscall change is worth retaining.


The [fixed positional-read experiment](baselines/2026-09-13-source-identity-positional-release-1.json)
compares `179e0ff7` with `8d61fa8e` using the unchanged native fixture. All 24
source pairs and 12 interleaved identical-candidate pairs completed: 36,000
measured queries, 360 warmups and 72 RSS observations. Equivalent fresh
workspace-selected release builds use the exact declared Zstd feature union.
Independent verification checks every original observation and all 180 paired
metric effects. Source median changes range -0.37% to -1.05%; corresponding
controls range -0.14% to -0.91%. Benefits exceed the absolute same-round control
in only 5–9 of 24 source pairs per metric (3–7 for p95). Large source and control
tail excursions remain retained. The experiment does not show a consistent
benefit beyond variation and is not retained. This is not a new comparison with
merged master and does not alter the earlier acceptance disposition.

The Unix positional-read optimization is reverted; the framed/legacy regression
remains. Formatting and that regression pass after reversion. Production
behavior returns to `179e0ff7`; complete workspace/release gates are next. The
[focused pre-revert evidence](baselines/2026-09-13-source-identity-positional-focused-1.json)
also remains available. Independent review recommends stopping speculative
micro-optimizations: required identity checks remain intact, and further timing
work needs a materially quieter environment with a fixed precision criterion and
pass/fail/inconclusive disposition declared before measurements.

The correctness milestone is ready for complete validation, but performance
clearance remains unresolved. In particular, the retained historical publication
tail uncertainty deserves attention despite its stable median. Read-only
suitability checks on the previously offered mac-mini observed approximately
24% CPU use in the second sample, 15 GB physical memory used and 670 MB unused;
no workload, source transfer, data change or service change was performed during
that initial check. An idle host or quiet test window has been requested while local correctness
validation proceeds. No performance waiver or release-readiness claim is made.


All [nine local workspace/release gates](baselines/2026-09-13-source-identity-validation-final.json)
pass on committed checkpoint `c229ace0`: vendor verification, formatting,
workspace check, strict Clippy, 1,151 workspace tests (23 existing ignored),
documentation checks, release node build, 146 release query tests (nine existing
ignored) and two release API consistency tests. The packet verifies the exact
Git archive against every recorded source hash and retains complete logs plus
the successful optimization-revert regression check. Linux/macOS CI and merge
remain separate pending work.

The mac-mini's internal temporary filesystem is confirmed APFS on a fixed SSD,
with approximately 104 GiB available at preflight. A new private directory,
`/private/tmp/logex-source-identity-controls.0ivODK`, contains the exact `c229ace0`
source archive for isolated build preparation. No test workload or service has
started there, and external-volume contents remain untouched. CPU use and low
unused memory alone do not establish timing stability or memory pressure;
inactive/file-backed memory is substantial and no swapouts were observed. A
single finite same-binary control packet is being designed to assess precision
before considering any new source comparison. The pending quiet-window question
does not authorize assuming that the host is quiet. All earlier measurements and
the unresolved performance disposition remain unchanged.

Two subsequent isolated build preparations stopped before compilation. The
first private environment could invoke the installed pinned compiler through
rustup directly, but Cargo could not find `rustc` through its process environment.
Explicit pinned `RUSTC`, `RUSTDOC` and toolchain-bin discovery corrected that
setup error. The second preparation reached dependency resolution and found a
crate absent from the copied cache. Both [complete failure records and logs](baselines/2026-09-13-source-identity-remote-preparation.json)
are retained, with every command-log, script and source-inventory hash checked.
Neither attempt executed a benchmark or produced a candidate test binary.

The third preparation uses a fresh private internal-APFS directory,
`/private/tmp/logex-source-identity-controls-3.PeHWi0`. It fetches the existing
lockfile's dependencies into its private Cargo cache, then performs metadata
inspection and compilation offline. Source bytes, lockfile and fixture parameters
remain unchanged. The control-only schedule is fixed at 72 processes with
14,640 measured timings, 120 explicit warmups and 72 RSS records. It compares
identical saved binaries and can qualify measurement precision or return
inconclusive; it cannot establish source-performance acceptance by itself.
No remote timing result exists yet, and the quiet-window clarification remains
pending. No production/external-volume or service changes were made.

The [fixed control design and reviewed scripts](baselines/2026-09-13-source-identity-control-design.json)
are retained before timing. Twelve rounds rotate the three suites and balance
AB/BA order, using the same saved candidate binary for both labels. All eight
required metrics must have both median and p95 label-effect intervals inside
±5%, and order-drift intervals inside ±5 percentage points. Whole paired
processes are resampled with the declared fixed seed and 4,000 draws per suite;
all observations and draws are retained. This precision budget does not change
the user's source-regression limit. Any failed check or insufficient precision
returns inconclusive, with no replacement processes or repeated packet.

Local preparation checks cover normal completion, a nonzero exit, timeout and
parent-lifetime-pipe closure using benign owned Python subprocesses. The runner
keeps its process-group leader reserved until cleanup finishes. Eight synthetic
numeric/error-handling checks pass for the analyzer, including errors after a
provisional eligibility result and changes to external input records. Python 3.9
syntax is checked; these preparation checks ran locally on Python 3.13. They are
not benchmark observations or a substitute for the remote packet's own checks.

The remote runner/platform preflight subsequently passed on Python 3.9.6 using
only `/usr/bin/true`; no LogEx fixture was involved. The third preparation's
dependency download was progressing slowly, so only that owned Cargo fetch was
interrupted. Its complete record remains a stopped preparation, not a successful
build. All 18 remaining archives were available locally and verified against
Cargo.lock; 17 were supplied to the stopped private cache and one had already
finished downloading. A fresh fourth build in
`/private/tmp/logex-source-identity-controls-4.uHyHMD` copied the completed cache,
passed offline dependency resolution and entered compilation. The
[interrupted attempt, cache verification, platform preflight and exact script updates](baselines/2026-09-13-source-identity-remote-cache.json)
are retained. Only the control design's private root and corresponding script
hash changed; its fixture, schedule, analysis and stopping rules are identical.
No source-performance result or new performance allowance is inferred.

The [completed remote build](baselines/2026-09-13-source-identity-remote-build-final.json)
passes all 16 recorded commands and produces both release harnesses. Independent
verification matches every one of the 796 source files and modes to the original
`c229ace0` Git archive, checks all command-log hashes, and confirms fresh harness
artifacts with the expected Zstd version/features. That preparation preceded the
fixed control packet below; successful compilation alone was not a
correctness-oracle or performance acceptance result.

The [completed fixed remote controls](baselines/2026-09-13-source-identity-remote-controls-1.json)
return **INCONCLUSIVE**. All 72 sequential fixture processes passed their
correctness checks: 14,640 measured timings, 120 explicit warmups and 72 RSS
records. Both labels used the same saved candidate binaries. No source comparison,
replacement process, timing retry, trimming or control subtraction occurred.
Original logs, all 16 metric summaries, 12,000 seeded resampling-index records
and 64,000 numeric draw records are retained losslessly, including binary Python
cache artifacts. Independent verification reconstructs the observations from raw
logs and agrees with all 256,000 draw values, interval endpoints and 32 required
precision/order bounds. Input hashes remained unchanged. The evidence packaging
also retains its initial directory-entry handling failure, which occurred before
writing an archive; correcting the packager did not rerun analysis or measurements.

All eight required metrics fail at least one predeclared bound. For example,
block-hash p95 label-effect interval is [-1.83%, +4.85%], but its order-drift
interval is [+1.11, +15.65] percentage points. Historical-publication p95 effect
is [-3.11%, +5.29%] and order drift is [-17.76, -1.08] percentage points. These
conditional descriptive intervals characterize identical-binary variation; they
do not measure a change caused by source code or override the earlier source
comparisons. Small pooled point effects alone do not meet the fixed precision
criterion.

The 36 host samples report macOS CPU speed limits ranging from 78 to 100,
median 88.5, with 33 below 100. These are reported limits, not measured frequency
ratios or proof of the cause of timing variation. The one-minute load ranges from
3.02 to 5.10 on six CPUs and includes this workload; it cannot all be attributed
to unrelated activity. The absence of a recorded thermal warning does not imply
absence of CPU speed limiting.

The tested conditions do not qualify a new source comparison. Do not repeat this
packet unchanged or relax the performance budget. Further performance clearance
requires a materially more stable environment or a quiet/cool test window, then
a design fixed before measurements with contemporary controls. The pending
environment clarification remains unresolved. All earlier source-tail findings
and the user's 10% limit remain in force. Local correctness gates pass, while
performance clearance, CI and PR/merge remain incomplete. No production data,
external-volume contents, services or global power settings were changed.

The user has now supplied the requested quiet window by stopping most running
mac-mini processes and authorizing testing inside a specifically named temporary
folder. The [new frozen quiet-environment design](baselines/2026-09-13-source-identity-quiet-design-1.json)
uses `/private/tmp/logex-audit-source-identity-quiet-20260913.YSSYXA` for every new
test output and reads the prior exact-source build artifacts without changing
them. Preflight reverified all 796 source files and both saved binary hashes.
The unchanged workloads, 12 pairs per suite, 72 total processes, 4,000 bootstrap
draws and precision/order bounds remain in force. A fixed ten-minute cooling
period precedes the packet; both endpoint probes must report CPU speed and
scheduler limits of 100 with six available CPUs. Failure of admission does not
trigger an automatic extra cooling/retry cycle. Host samples during the packet
are retained without mid-run exclusions. This new environment is evaluated
separately from the first inconclusive packet; neither packet can establish
source-performance acceptance on its own. No new benchmark result is claimed
by this preparation record.

## Completed quiet-window controls

The [quiet-window result](baselines/2026-09-13-source-identity-quiet-controls-1.json)
retains all 72 processes, 14,640 measured timings, 120 warmups and 72 RSS records.
The ten-minute admission passed, and all 36 subsequent host probes reported CPU
speed/scheduler limits of 100 and six available CPUs. Every fixture returned
correct results. Independent verification agrees with all 256,000 resampled
values and all 32 required bounds. No observation was replaced or excluded.

The overall disposition remains **INCONCLUSIVE**. Live publication satisfies all
four required bounds. Historical publication, sparse concurrent queries and all
five short native query cases each fail at least one precision/order bound. The
same-binary live median/p95 difference is -0.003%/-0.272%; historical publication
is +0.094%/+0.833%. These compare identical executable bytes under two labels,
so they are measurement controls, not estimates of a source-change effect. Small
point estimates do not override wide intervals: historical publication's p95
effect upper bound is 5.017%, and sparse concurrency's p95 order-drift upper
bound is 5.503 percentage points, both outside the declared five-unit bounds.
The earlier source-tail findings and 10% source-regression limit remain open.

The requested quiet environment was provided. These observations do not establish
that unrelated user processes caused the remaining variation, nor do reported
CPU limits establish actual clock frequency. Inspection of all 24 retained native
processes instead identifies within-process settling as a measurement-method
lead: across the first process of each pair, the median last-20 versus first-20
change ranges from -2.81% to -5.03% by case; across the second processes it ranges
from -0.16% to +0.67%. This exploratory window analysis retains the full original
observations and is not an acceptance calculation. It does not identify the
underlying CPU, allocator or filesystem mechanism. A bounded local diagnostic
will compare one versus 100 explicit warmup passes using identical production
code, unchanged measured queries and the same row oracle. It cannot establish
source acceptance or replace burst/startup measurements.

Mac-mini was released at 2026-09-13 16:38 UTC after checking that no test process
remained. New writes stayed inside
`/private/tmp/logex-audit-source-identity-quiet-20260913.YSSYXA` (12,064 KiB
retained). Prior build artifacts were read-only; no baseline build or source
comparison ran remotely. External-volume contents, other files, services and
global power settings were unchanged. The raw archive contains 338 files,
including the complete collection/verification record and unexecuted local
source-comparison preparations. An initially mistaken collection assertion about
an escaped display of a newline is also retained and explicitly retracted:
strict JSON validation of the original bytes passes; no remote artifact needed
repair and no measurement or analysis was repeated.

## Warmup diagnostic and prospective direct comparison

The [local native warmup diagnostic](baselines/2026-09-14-source-identity-native-warmup-1.json)
compares one versus 100 explicit passes in a disposable build of `c229ace0`.
Production code, the measured loop and full row oracle are unchanged. All 24
processes pass, retaining 12,000 measured queries, 6,060 warmups and 24 RSS records.
An earlier attempt completed its first fixture but its macOS timing wrapper
could not read `kern.clockrate` in the sandbox. Those 500 measured queries and
five warmups remain separately retained. A read-only counter probe passed outside
the sandbox before the full diagnostic ran there; no partial observation was
used as a replacement or pooled into the completed packet.

Longer warmup changes the four nonempty query cases' p95 point estimates by
-6.10% to -9.89%; the absent-topic case changes -0.89%. This does not establish
a reliable fix for the variation: every interval for the change in absolute
within-process settling includes zero, and process-position intervals remain
wide. This local ARM result also cannot identify the mechanism on the Intel
mac-mini. The native fixture used for source acceptance retains its original
single warmup and every measured iteration. Independent verification reparsed
all raw samples, recomputed all 40 point estimates and 4,000 seeded index records,
and checked every interval against the retained replicate values. It did not
independently recompute every replicate's arithmetic.

Method review identified an unnecessarily restrictive admission strategy. Failing
to contain every identical-binary control interval within +/-5% does not establish
that a direct source comparison cannot distinguish a 10% regression. Both earlier
packets remain INCONCLUSIVE under their original rules. The prospective strategy
uses a single direct baseline/candidate comparison with contemporary controls,
without another control-only admission packet. This changes the measurement
strategy; the source-regression limit remains 10%.

The [new fixed design](baselines/2026-09-14-source-identity-direct-design-1.json)
retains all six original suites, 24 source pairs and 12
control pairs for each binary per suite: 576 processes, 96,000 measured timings,
480 warmups and 576 RSS records. A fixed seed randomizes the balanced round order
before measurement. All 31 timing metrics retain primary median and p95
endpoints; source upper bounds must remain below 10%, with control, order and
chronology review addressing any unresolved confounding. Increases above 5%
require investigation. Control percentages are never subtracted, early samples
are never removed, and a favorable subgroup cannot replace a primary result.
Any unresolved endpoint prevents a performance pass.

Mac-mini use resumed for the separately verified baseline build after the earlier
control window was released. New source, private dependency cache and build
outputs stay under the existing named test root's `baseline-build-9c0a58fc`
subdirectory. The baseline retains its own lockfile and unchanged fixtures;
candidate build artifacts remain read-only. No direct source measurements are
claimed by this preparation, and no live sync or external-volume access is part
of it.


## Verified direct-comparison inputs

The [baseline build and execution preparation](baselines/2026-09-14-source-identity-direct-build-1.json)
retain all 16 successful build/preparation commands, both fresh release harnesses,
the baseline's own lockfile, and the exact frozen input paths and hashes.
Independent local review checks all 720 baseline source files against the original
Git archive, all 32 command logs, and the workspace-selected compiler records.
A separate remote check rehashes all 1,590 baseline/candidate input files,
including the compiled binaries. The original six fixtures are byte-identical.

The fixed 600-second cooldown was operational preparation, with before/after host
conditions retained and no environmental admission thresholds. Its final log
is retained with the execution packet below. No benchmark had started at the
build-evidence checkpoint. The 70-file build/preparation archive has been decoded and compared
byte-for-byte to the retained originals; it establishes provenance, not source
performance clearance.

## Completed direct mac-mini comparison

The [complete direct result](baselines/2026-09-14-source-identity-direct-result-1.json)
retains the execution, raw measurements, analysis, independent verification,
host observations and final release check. The frozen schedule completed once
from 2026-09-13 18:07:07 to 19:38:25 UTC: 576 processes, 96,000 measured timings,
480 explicit warmups and 576 RSS observations. All fixture oracles, configuration
checks and before/after input hashes pass. Collection independently verified all
2,925 original evidence files. No measurement was discarded or replaced.

The original conditional 95% intervals put 61 of 62 primary endpoints below
10%. The remaining endpoint is `integrated_sparse.sql_count` p95: +4.06%, with
interval [-5.38%, +12.65%]. Its median changes +0.25%, with interval
[-1.29%, +1.97%]. This is an uncertain tail estimate, not demonstrated
above-limit overhead. The full packet remains **INCONCLUSIVE** under its frozen
rules. Earlier packets retain their original dispositions.

Actual publication results are:

| Workload | Median change | p95 change | p95 interval |
| --- | ---: | ---: | ---: |
| Live publication | +0.08% | -0.61% | [-2.44%, +2.47%] |
| Historical publication | +0.01% | +2.76% | [-4.66%, +6.03%] |
| Dense raw live ingestion | +0.81% | -0.50% | [-1.29%, +1.19%] |
| Sparse raw live ingestion | +1.24% | +1.90% | [+0.40%, +3.07%] |
| Dense raw historical ingestion | +0.32% | +0.93% | [+0.06%, +2.07%] |
| Sparse raw historical ingestion | -0.10% | +0.91% | [-0.93%, +1.85%] |

The exact intervals and all 31 metric reviews are retained in the
[engineering disposition](baselines/2026-09-14-source-identity-direct-engineering-1.json).
The short absent-topic p95 increase of +5.90% was investigated: its upper bound
is +7.94%, with the prior stage attribution and unsuccessful positional-read
experiment retained. Required source identity validation may cost several percent
on small queries. No supported optimization was removed, and controls are never
subtracted from a source result.

Sparse COUNT's order-specific p95 estimates are +13.39% and -2.48%. Identical
baseline and candidate controls also show substantial order variation. The old
schedule couples source order to control order, so those factors cannot be
separated retrospectively. Inspecting all tail contributors shows observations
from multiple processes; removing an isolated observation would neither be
justified nor resolve this limitation. The read-only COUNT review confirms that
its parser, native aggregate implementation and fixture are unchanged. The
candidate adds bounded identity validation shared by the native filter, COUNT
and ordered-query paths. No COUNT-specific bug or redundant operation was found.

Independent verification checked all 744 point estimates, 744 interval pairs,
62 endpoint dispositions, 4,000 seeded index selections and 124,000 vector
identities. It also independently recomputed 496 vectors (11,904 values) at
16 fixed draw indices; it did not independently recompute every vector's
arithmetic. All checks agree. The analysis remains conditional on these
measurements; it does not establish simultaneous coverage for all endpoints,
a sequential error guarantee over prior experiments or future-host performance.

All 864 host probes and 576 wrapper logs were independently reviewed. Reported
CPU speed limits range 78–100, with 238 of 288 readings below 100. Scheduler
limit and available CPUs remain 100 and six; swap/pageout counters do not
increase. These readings do not identify the cause of timing variation.

Mac-mini was released at 2026-09-13 19:39:38 UTC after collection and a read-only
process check found no matching audit processes. New files remain exclusively
under `/private/tmp/logex-audit-source-identity-quiet-20260913.YSSYXA`, totaling
2,483,512 KiB. Existing external-volume contents were not accessed. Any later
confirmation must have a separately reviewed, prospectively fixed design that
addresses the order coupling; this packet cannot be repeated until it passes.

## Fixed sparse confirmation

The [prospective confirmation design](baselines/2026-09-14-source-identity-sparse-confirmation-design-1.json)
is frozen before measurement. It retains the complete unchanged sparse fixture,
all nine metrics and 18 median/p95 endpoints. Eighty source pairs and 40 pairs
for each identical-binary control produce 320 processes and 28,800 timings.
Both release binaries and all build/source inputs are reused unchanged.

The new schedule independently balances source order, control order, control
binary and pair placement. Twenty balanced four-round blocks span five temporal
cycles. Resampling retains whole blocks, with 10,000 fixed shared draws. Every
pooled endpoint must stay below 10% using a nominal one-sided 99% upper bound;
predeclared source/control order and cycle-sensitivity patterns can still stop
clearance. These are conditional bounds, not an unconditional error guarantee.
No old observations are pooled away, and the prior packet remains INCONCLUSIVE.

Runner and arithmetic checks use small synthetic inputs only. Root review
corrected an unintended hash-buffer substitution and restored the proposal's
byte comparison before freezing; the earlier tooling preparation is retained.
The existing process supervision is unchanged. No production or fixture code
was changed. The full design, independent method review, scripts, exact hashes
and successful preparation checks are archived.

This is one finite confirmation, with no interim acceptance, replacement runs,
post-result exceptions or further unchanged confirmation if it remains uncertain.
Mac-mini use resumed only in a new subdirectory of the existing named test root.
Its expected duration is approximately 66 minutes plus preparation and collection;
the host will be released after collecting the complete result.

The [execution preparation](baselines/2026-09-14-source-identity-sparse-preparation-1.json)
records three successful transfer/verification steps: all 38 transferred files,
nine Python 3.9 syntax checks and all 1,590 unchanged build/source inputs pass.
The fixed cooldown is running at this evidence checkpoint; no benchmark has
started. A separate independent verifier is frozen before data. It will check
every reported effect/interval, all draw identities and temporal sensitivities,
plus raw arithmetic at a declared fixed subset of draw indices. The full
execution evidence will retain the cooldown's final log and outcome.
