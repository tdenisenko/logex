# Download and run LogEx

LogEx is a command-line application with a built-in browser dashboard. The
release archives contain the executable and documentation; Rust, Cargo and
Protobuf are not needed to run them.

## Choose a download

Download from the [official releases page](https://github.com/tdenisenko/logex/releases).
For the first release, choose one of these `v0.1.0` archives:

| System | Architecture | Archive |
| --- | --- | --- |
| Linux, glibc 2.35+ (Ubuntu 22.04+) | Intel/AMD 64-bit | [x86_64 Linux](https://github.com/tdenisenko/logex/releases/download/v0.1.0/logex-v0.1.0-x86_64-unknown-linux-gnu.tar.gz) |
| Linux, glibc 2.35+ (Ubuntu 22.04+) | ARM64 | [ARM64 Linux](https://github.com/tdenisenko/logex/releases/download/v0.1.0/logex-v0.1.0-aarch64-unknown-linux-gnu.tar.gz) |
| macOS 15+ | Intel | [Intel Mac](https://github.com/tdenisenko/logex/releases/download/v0.1.0/logex-v0.1.0-x86_64-apple-darwin.tar.gz) |
| macOS 15+ | Apple Silicon | [Apple Silicon Mac](https://github.com/tdenisenko/logex/releases/download/v0.1.0/logex-v0.1.0-aarch64-apple-darwin.tar.gz) |
| Windows with WSL2 | Match the Linux environment | Use the corresponding Linux archive inside WSL2. |

Run `uname -sm` in your terminal to check the system and architecture. On Linux,
`x86_64` means Intel/AMD and `aarch64` means ARM64. On macOS, `arm64` means Apple
Silicon. These are GNU/Linux builds; Alpine/musl, 32-bit systems and native
Windows executables are not included in this release.

## Verify and extract

Download your archive and `SHA256SUMS` from the same release. For example, on
Intel/AMD Linux:

```bash
archive=logex-v0.1.0-x86_64-unknown-linux-gnu.tar.gz
release_url=https://github.com/tdenisenko/logex/releases/download/v0.1.0
curl --fail --location --remote-name "$release_url/$archive"
curl --fail --location --remote-name "$release_url/SHA256SUMS"
awk -v file="$archive" '$2 == file' SHA256SUMS | sha256sum --check
```

On macOS, set `archive` to the appropriate Mac filename and use this checksum
command instead:

```bash
awk -v file="$archive" '$2 == file' SHA256SUMS | shasum -a 256 --check
```

Require an `OK` result for your archive. Checksums detect corrupted downloads;
the release also includes signed GitHub build provenance. With a recent
[GitHub CLI](https://cli.github.com/), verify the archive's build identity:

```bash
gh attestation verify "$archive" --repo tdenisenko/logex \
  --signer-workflow tdenisenko/logex/.github/workflows/release.yml \
  --source-ref refs/heads/master --deny-self-hosted-runners
gh release verify v0.1.0 --repo tdenisenko/logex
```

Each archive also has a `.sigstore.json` bundle for `gh attestation verify
--bundle PATH`. The release's `release-manifest.json` records the exact source
commit, targets, sizes and executable/archive hashes. For stricter verification,
pass that expected commit as `--source-digest` and `--signer-digest`.

After verification:

```bash
tar -xzf "$archive"
cd "${archive%.tar.gz}"
./logex --version
./logex --help
```

macOS binaries carry an ad-hoc integrity signature, **not** an Apple Developer ID
signature or Apple notarization. macOS may require per-application approval after
the first launch attempt. After verifying the download, use **System Settings →
Privacy & Security → Open Anyway** as described in
[Apple's instructions](https://support.apple.com/102445).

## Run

From the extracted directory:

```bash
./logex sync
```

Open [http://127.0.0.1:8577/](http://127.0.0.1:8577/) for the dashboard. The
default HTTP and gRPC listeners are local-only. Stop the process with **Ctrl+C**
and let it finish shutting down before replacing the binary or moving its data.

A fresh mainnet directory needs a recent weak-subjectivity checkpoint. The
default command resolves one through a two-of-three trusted checkpoint quorum;
you can provide your own with `--checkpoint` or `--checkpoint-sync-url`. See
the [checkpoint and trust model](https://github.com/tdenisenko/logex#what-logex-verifies).
The process needs network access to Ethereum peers and storage for the history
it downloads. Historical catch-up takes time; SQL only admits verified ranges.

Default data locations:

| System | Directory |
| --- | --- |
| Linux / WSL2 | `$XDG_DATA_HOME/logex`, or `~/.local/share/logex` |
| macOS | `~/Library/Application Support/LogEx` |

To select a data directory on an SSD:

```bash
./logex --data-dir /path/to/logex-data sync
```

Full mainnet history occupies hundreds of gigabytes and continues growing. Keep
room for synchronization, indexes and subsequent blocks. The node's 10 GiB
free-space stop threshold is a safety floor, not an estimate of required capacity.
Use a local filesystem; the release tests do not certify network filesystems.

The [SQL guide](SQL.md) covers querying, event/address literals, pagination and
coverage. `./logex sync --help` lists network and query settings. Public HTTP
requires a dashboard password; review the
[server setup instructions](https://github.com/tdenisenko/logex#run) before
exposing the service.

## Windows through WSL2

Use [Microsoft's WSL installation instructions](https://learn.microsoft.com/windows/wsl/install).
On a supported Windows system, run this in an administrator PowerShell and
complete the requested restart and Ubuntu setup:

```powershell
wsl --install
wsl --list --verbose
```

Confirm the distribution uses **version 2**. Inside Ubuntu, follow the Linux
download instructions above, choosing the architecture reported by `uname -m`.
Use Ubuntu 22.04 or newer. Keep LogEx data in the distribution's Linux filesystem
(for example `~/.local/share/logex`), rather than a `/mnt/c/...` Windows directory.
The release provides Linux binaries and WSL2 instructions; it does not claim
native Windows storage support or a separate WSL2 mainnet acceptance run.

## Optional installation on PATH

You can keep running `./logex` from the extracted directory. To install the
executable for your user account:

```bash
mkdir -p "$HOME/.local/bin"
install -m 0755 ./logex "$HOME/.local/bin/logex"
```

Add `$HOME/.local/bin` to your shell's `PATH` if needed. Installing the binary
does not create a system service or start synchronization. For an upgrade,
stop the existing process, read the new release notes and preserve a backup of
the stopped dataset before replacing executables. Version `0.x` does not promise
stable data formats or API compatibility across future releases.

## What is in an archive

- `logex`: the native executable, tested again after extraction.
- `README.md`: these installation instructions.
- `SQL.md`: the SQL reference, with links to the tagged source.
- `LICENSE-MIT` and `LICENSE-APACHE`: LogEx's declared license options.
- `THIRD-PARTY-LICENSES.html`: dependency notices, including transitive and build
  dependencies across the four release targets.
- `Cargo.lock`: the locked dependency versions used for the build.
- `build-info.json`: source commit, target, compiler, runtime-library information
  and hashes of all payload files.

The release page provides separate archives, portable provenance bundles,
`SHA256SUMS` and the combined manifest. Published release assets and tags are
immutable; corrections are delivered under a new version.
