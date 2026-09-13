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

## Validation still required

Implementation is not yet accepted. Required coverage includes copied complete
sets, equal-row replacement, separate native datasets, captured-reader lifetime,
prefix append, compaction, missing identity and interrupted publication/recovery.
Run focused tests, complete workspace/release gates and equivalent repeated
release measurements before committing an accepted implementation. Preserve every
observation and the existing unresolved query-tail measurements. Exact-head Linux
and macOS CI and a merged PR close this milestone; live sync remains deferred.
