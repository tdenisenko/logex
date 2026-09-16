# Historical ingestion memory probes

## Findings

**B5-02 — moderate: macOS host send-right references accumulated per probe.**
Both historical scheduler and write-chunk helpers called `mach_host_self` without
releasing its acquired reference. At base `ba4957f2`, two separate regressions
append tests to otherwise unchanged production files and invoke each real helper
twice in its own test process. Both fail: the owned host port's reference count
increases from one to three. The controls release their measured extra references
before asserting. This establishes send-right reference accumulation, not linear
RSS growth or creation of a distinct port for every call.

**B5-03 — moderate: available-memory estimates double counted overlapping pages.**
Both helpers added free, inactive, speculative and purgeable counts. Apple's SDK
states that free already includes speculative pages. XNU also tracks purgeability
separately from page queue membership, so purgeable pages can overlap inactive
pages. The arithmetic bodies in the two original files are byte-identical. A
standalone test of that extracted expression fails for a controlled snapshot with
50 free, 20 inactive, 10 speculative (already free) and 15 purgeable (within the
inactive set): the expression reports 95 pages rather than 70. This is a pure
accounting reproducer, not an observed live workload or process allocation limit.

## Implementation and implications

A private engine memory module now serves both historical callers. On macOS it
retains one task-wide host send-right reference in a `OnceLock`, reusing that right
for fresh read-only statistics queries. Concurrent initialization candidates own
their references and release the unused candidates. Failed acquisition leaves the
cache empty for retry. Only the right is cached; available-memory readings remain
fresh. The kernel reclaims the one process-lifetime reference on process exit.

An owning wrapper releases temporary references on normal return and unwind,
reporting an unexpected release error. The narrow `mach_port_deallocate` binding
uses the installed SDK signature because pinned libc 0.2.184 omits it. No new
dependency, unsafe trait implementation, borrowed-task-port release or process-wide
configuration change is introduced. FFI comments document initialization, output
sizes, alignment, ownership and read-only use.

The macOS estimate is free plus inactive pages. It excludes overlapping extra
categories, validates the returned statistics count against the SDK revision-zero
prefix and the allocated libc buffer, rejects nonpositive page sizes, and checks
byte multiplication. Older complete revision-zero results remain usable. This is
an advisory heuristic: inactive pages are not all immediately reclaimable and
active purgeable pages are omitted. It is not a hard memory budget, a guarantee
of allocatable bytes, or a Linux `MemAvailable` equivalent.

Linux retains its existing `/proc/meminfo` field parsing and checked KiB conversion.
Total-memory values retain the existing one-time cache; available values do not.
Existing unknown-memory fallback behavior and all batch/pipeline thresholds remain.
Corrected macOS readings can select smaller historical batches when the old
reading was inflated; throughput effects are unmeasured. The steady probe path
avoids repeated host-right acquisition. No broad benchmark, throughput percentage,
new ingestion write, storage format, live sync or mac-mini validation is claimed.

## Validation and cleanup

Nine new controls cover overlapping categories at 4 KiB/16 KiB page sizes,
revision-zero/current count acceptance, invalid counts/page sizes, zero and large
page counts, overflow, Linux field parsing, real platform queries, cached totals,
repeated host-right use, normal/unwind release and concurrent initialization
candidate cleanup. Ownership controls run only in isolated copies of their own
test binary; the parent requires an explicit completion marker so an incorrect
test filter cannot silently pass. The concurrent candidate control exercises the
same `OnceLock` ownership handoff and verifies one retained reference followed by
release on cache destruction; it does not claim to force the production fast-path
race. No other process is queried and no resource limit is stressed.

All 570 sync tests pass (two existing workloads ignored). After simplifying the
concurrent ownership control to acquire its two candidates before spawning, all
nine final memory controls pass. All eight local gates pass on `546d6d89`:
vendor integrity, workspace/patched-vendor formatting, check, strict Clippy,
1,731 workspace tests (24 ignored), documentation tests and release build.
PR/CI/merge remain pending.

Implementer review traced every historical memory caller, cache initialization,
failed queries, wrapper lifetime, ABI prefix and arithmetic. No independent review
is claimed. Removed both duplicated Darwin probes, the duplicate Linux parser,
obsolete platform dispatch wrappers, the old KiB constant and the anchored-module
`OnceLock` import. Existing policy tests remain. The small pure parser retains the
original trusted-kernel input semantics rather than adding unrelated parsing rules.

## Primary references

- Apple's [host statistics implementation](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/kern/host.c)
  and [SDK statistics definitions](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/mach/vm_statistics.h)
  establish the free/speculative relationship.
- Apple's [page queue implementation](https://github.com/apple-oss-distributions/xnu/blob/main/osfmk/vm/vm_resident.c)
  tracks volatile-page purgeability independently of active/inactive placement.
- Apple's [IOKit host queries](https://github.com/apple-oss-distributions/IOKitUser/blob/main/IOKitLib.c)
  pair acquired host rights with `mach_port_deallocate`.
- Installed Xcode SDK `mach/host_info.h`, `mach/mach_port.h`, `mach/port.h` and pinned
  libc 0.2.184 were checked for revision counts, C signatures and integer aliases.

This closes only the scoped memory-probe findings after validation and merge.
Historical state, allocation ownership, reorg handling, coverage, expected-volume
protection and offline repair retain their separate audit work.
