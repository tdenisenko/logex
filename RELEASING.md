# Releasing LogEx

Releases are built from a reviewed commit on `master`. The release workflow
creates an annotated tag and a **draft** release only. Publication is a separate,
explicit step after every native package and the exact source commit pass their
checks. Publishing does not install or restart a running node.

## Prepare the source

1. Update the workspace version and lockfile together when changing versions.
   Update the versioned links in `INSTALL.md` and write
   `docs/releases/vVERSION.md`, including compatibility limits and upgrade notes.
2. Keep `tools/release/targets.json`, install instructions and release notes in
   agreement. The supported native targets are Linux GNU and macOS on x86_64
   and ARM64. Windows uses the Linux archive inside WSL2.
3. Review any dependency-license changes. `cargo-about` is pinned by version and
   archive SHA256. Its configuration includes transitive and build dependencies
   for every target and fails on terms outside the reviewed set. Preserve the
   original dependency notices; never substitute LogEx's copyright for them.
4. Run the release helper tests and a workflow validator, then submit and merge
   the release-preparation PR. The release-related PR workflow builds and tests
   the four native packages but does not attest, tag or publish them. Changes
   to runtime source, vendored dependencies, the toolchain or packaged inputs
   also trigger this matrix, including follow-up fixes within an existing PR.
5. Require successful CI on the final merged commit. A green PR run does not
   replace CI on the actual commit used for a release.

```bash
python3 -m unittest discover -s tools/release -p 'test_*.py' -v
bash -n tools/setup_ci_protoc.sh
git diff --check
```

Every release build uses the pinned Rust toolchain, locked dependencies, a native
runner, release-profile workspace tests and an extracted-binary CLI smoke check.
The helper checks the executable architecture and runtime libraries. Linux uses
a glibc 2.35 baseline; macOS uses a conservative deployment target of 15.0.
macOS archives use an ad-hoc integrity signature. They are not signed with an
Apple Developer ID or notarized, and the public installation guide states this.

## Build a draft

From the default branch, dispatch the workflow with the Cargo version, without
its `v` prefix:

```bash
gh workflow run release.yml --repo tdenisenko/logex --ref master -f version=0.1.0
```

Record the resulting workflow run ID and source SHA. Inspect all job results.
The workflow validates the exact source commit, builds and tests all targets,
produces complete dependency notices, verifies the archive contents, attests each
archive, and assembles `SHA256SUMS` and `release-manifest.json`. The final job
rechecks ordinary CI before creating an annotated tag and uploading the draft.

If a build or draft step fails, retain its logs and artifacts and investigate.
Do not overwrite an existing tag, replace assets or blindly rerun draft creation.
A partial draft may already exist; inspect its tag, source SHA, notes and complete
asset inventory before deciding how to recover it. A published immutable release
requires a new version for corrections.

## Verify and publish

Download the successful run's `release-bundle` artifact to a new empty directory.
Use the run ID observed above; do not select artifacts from an unrelated run.

```bash
gh run download RUN_ID --repo tdenisenko/logex --name release-bundle --dir /path/to/new-release-bundle
```

Review the release notes and all four archives, portable Sigstore bundles,
checksums and manifest. Each archive contains only the executable, documentation,
license notices, lockfile and build metadata. The binaries have already been
smoke-tested on their native runners. Keep the reviewed artifact directory
unchanged; publication checks its full contents and GitHub's upload digests.

Enable immutable releases in the repository's release settings before the first
publication. The GitHub REST equivalent is:

```bash
gh api --method PUT repos/tdenisenko/logex/immutable-releases
```

Fetch the exact release commit locally and use a checkout whose tracked contents
match it. Publish with that full commit SHA and the verified artifact directory:

```bash
python3 tools/release/publish.py publish --version 0.1.0 \
  --commit FULL_SOURCE_COMMIT --directory /path/to/new-release-bundle
```

The helper fails unless the latest ordinary CI for that commit and the complete
release workflow succeeded. It verifies every archive hash, target, embedded
source identity, run ID, provenance signature, annotated tag, reviewed notes and
remote asset digest. It also requires immutable releases to be enabled. After
publication it checks immutability, verifies the published assets again and runs
`gh release verify`.

Finally, download the public assets into another empty directory, compare their
hashes against the reviewed files and verify the public release URL and download
links. Record the released commit, tag, workflow and verification results in the
local roadmap. Keep any existing service deployment separate from publication.

See GitHub's documentation for [artifact attestations](https://docs.github.com/en/actions/how-tos/secure-your-work/use-artifact-attestations/use-artifact-attestations)
and [immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases).
