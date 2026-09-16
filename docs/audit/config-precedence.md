# Configuration precedence and diagnostics

## Findings

**B10-10 — moderate: file settings could replace explicit CLI choices.** At
`8d7426aa`, `main` prefers the config file for the data directory, checkpoint,
checkpoint URL, partition target, API listener hosts, P2P bind, execution discv5
port and public-gRPC permission. NAT and log level instead use their default
strings as omission sentinels, so explicitly choosing `any` or `info` cannot
override a file setting. Password and bootnode handling already prefer the CLI.
This makes operator overrides inconsistent, including directory selection for
maintenance commands. The reproductions resolve options without opening storage
or listeners; they do not demonstrate data loss or a bypass of listener checks.

**B10-11 — moderate: config parse diagnostics repeat file contents.** The original
loader formats the TOML error with `Display`, which can reproduce the offending
line. Config files can contain an HTTP dashboard password. Two unchanged-loader
controls show syntax and type errors repeating a harmless fixture value. Using
only the library's `message()` is insufficient because deserialization messages
can contain the rejected value too.

**B10-12 — low: unknown config keys are silently ignored.** A misspelled
`data_dir` passes the original loader and leaves directory selection at its
default. The loader now rejects unsupported keys and tables instead of accepting
an apparently successful configuration.

## Resolution

Configuration merges once, before storage setup: explicit CLI options, then file
values, then existing defaults. The parser retains `ArgMatches` through this
merge so default-valued options can be distinguished from omitted options.
`value_source` is used for defaulted fields; optional fields preserve an explicitly
supplied value. The existing CLI and command types remain the resolved values,
with no separate configuration framework or dependency changes. Parser matches
are dropped after resolution instead of retaining duplicate argument values for
the lifetime of sync.

Global flags work before or after every subcommand. CLI bootnodes replace the
configured extra list, CLI password takes precedence, and either a disabled file
setting or `--disable-dashboard` disables the dashboard. No new positive dashboard
flag or negative public-gRPC flag is added. Existing password/listener validation,
valid `RUST_LOG` precedence, plain-info normalization, checkpoint URL fallback and
relative-path semantics remain in place. CLI-only ports and peer settings are
unchanged. File keys not listed in the schema now cause a startup error; users
must correct or remove previously ignored keys.

Parse errors retain the escaped path, a one-based line/Unicode-character column
when the parser provides a span, and guidance to check syntax, keys and types.
They deliberately omit the parser's source excerpt and value-bearing message.
This trades some parser-specific detail for preventing config contents from
appearing in startup logs. Read errors retain their I/O cause and path. This is
scoped to config loading, not a claim that all runtime diagnostics redact values.

## Validation and cleanup

The precedence reproductions use a behavior-preserving extraction of the original
inline merge into `Cli::apply_config`; the original source, extraction patch and
test module are retained in the evidence directory. This is not an unchanged-main
execution claim. The three loader controls call the unchanged original loader.
The original run has ten failures and two passing controls. All twelve pass after
the fix. Six additional controls cover untouched CLI/default behavior, both global
flag positions, inherited booleans, diagnostic location with Unicode, missing
files, unknown tables, duplicate keys and invalid types/ranges. All 172 node tests
pass, including the existing listener policy controls. Fixtures use owned temporary
files; no user configuration, database, public listener or remote machine is used.

Removed the duplicated merge in `main`, the NAT default-string branch, and the
old log-level sentinel helper. Retained the standalone info-filter normalization
and its tests. Implementer review checked every supported file key, both global
flag positions, typed CLI values, ownership of parser matches and startup ordering.
No independent review is claimed.

The initial source `4b593957` passed all eight local gates (1,765 tests, 24 ignored).
Final review then noticed that location counters inferred `i32`; they now use
`usize`, matching string offsets. A giant-file runtime reproduction was not run.
Initial CI was canceled before merge, and revised-source gates pass on `c70c2118`: vendor integrity, workspace/patched-vendor
formatting, check, strict Clippy, 1,765 workspace tests (24 ignored), documentation
tests and release build. CI and merge are pending. Four additional checks of the actual release executable pass:
explicit directories before/after `info` select only the requested owned temporary
directory, and unknown-key/syntax errors exit 1 before either candidate data
directory is created. The syntax error does not print the fixture value. All four
temporary fixtures were removed afterward. The machine-readable evidence is
[recorded here](baselines/2026-09-17-config-precedence.json). No benchmark was run:
this is startup-only work, with no changes to ingestion, storage writes or query
execution. Volume identity/preflight, offline repair, broader runtime validation
and the remainder of the offline audit remain open.

## References

- [Clap 4.6.0 argument value sources](https://docs.rs/clap/4.6.0/clap/struct.ArgMatches.html#method.value_source).
- Installed pinned TOML 0.8.23 `src/de.rs` and toml_edit 0.22.27 `src/error.rs`
  were inspected for error message, source rendering and span behavior. Web
  retrieval of the versioned TOML error page was unavailable.
