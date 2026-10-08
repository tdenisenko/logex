#!/usr/bin/env python3
"""Build and verify bounded, source-bound release archives; never start a node."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import shutil
import struct
import subprocess
import tarfile
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY = "tdenisenko/logex"
WORKFLOW = f"{REPOSITORY}/.github/workflows/release.yml"
TARGETS = json.loads((Path(__file__).with_name("targets.json")).read_text())
TARGET_BY_NAME = {target["target"]: target for target in TARGETS}
PAYLOAD_NAMES = {
    "logex", "README.md", "SQL.md", "LICENSE-MIT", "LICENSE-APACHE",
    "THIRD-PARTY-LICENSES.html", "Cargo.lock",
}
MAX_ARCHIVE_BYTES = 512 * 1024 * 1024


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def run(*args: str, cwd: Path = ROOT) -> str:
    return subprocess.check_output(args, cwd=cwd, text=True, timeout=180).strip()


def digest(stream) -> str:
    result = hashlib.sha256()
    for chunk in iter(lambda: stream.read(1024 * 1024), b""):
        result.update(chunk)
    return result.hexdigest()


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return digest(stream)


def version(value: str) -> str:
    require(bool(re.fullmatch(r"(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)", value)),
            "release version must be a stable three-part SemVer such as 0.1.0")
    return value


def commit(value: str) -> str:
    require(bool(re.fullmatch(r"[0-9a-f]{40}", value)), "expected a full Git commit SHA")
    return value


def source_version(root: Path = ROOT) -> str:
    with (root / "Cargo.toml").open("rb") as stream:
        return version(tomllib.load(stream)["workspace"]["package"]["version"])


def source_identity(requested_version: str | None) -> tuple[str, str]:
    actual_version = source_version()
    require(not requested_version or requested_version == actual_version,
            "requested version differs from Cargo.toml")
    actual_commit = commit(run("git", "rev-parse", "HEAD"))
    if os.environ.get("GITHUB_SHA"):
        require(os.environ["GITHUB_SHA"] == actual_commit, "checkout differs from workflow SHA")
    run("git", "diff", "--exit-code", "HEAD", "--")
    return actual_version, actual_commit


def archive_name(release_version: str, target: str) -> str:
    version(release_version)
    require(target in TARGET_BY_NAME, "unsupported release target")
    return f"logex-v{release_version}-{target}.tar.gz"


def verify_binary_header(header: bytes, target: str) -> None:
    require(target in TARGET_BY_NAME, "unsupported binary target")
    require(len(header) >= 32, "truncated executable")
    if target.endswith("linux-gnu"):
        require(header[:6] == b"\x7fELF\x02\x01", "expected a 64-bit little-endian ELF executable")
        machine = struct.unpack_from("<H", header, 18)[0]
        expected = 62 if target.startswith("x86_64") else 183
    else:
        require(header[:4] == b"\xcf\xfa\xed\xfe", "expected a 64-bit little-endian Mach-O executable")
        machine = struct.unpack_from("<I", header, 4)[0]
        expected = 0x01000007 if target.startswith("x86_64") else 0x0100000C
    require(machine == expected, "executable architecture differs from archive target")


def verify_runtime_libraries(binary: Path, target: str) -> str:
    if target.endswith("linux-gnu"):
        output = run("readelf", "--dynamic", "--version-info", str(binary))
        needed = set(re.findall(r"\(NEEDED\).*\[([^]]+)\]", output))
        allowed = {"libc.so.6", "libm.so.6", "libgcc_s.so.1", "libpthread.so.0",
                   "libdl.so.2", "librt.so.1", "ld-linux-x86-64.so.2", "ld-linux-aarch64.so.1"}
        require(needed <= allowed, f"unexpected shared libraries: {sorted(needed - allowed)}")
        glibc = {tuple(map(int, value.split(".")))
                 for value in re.findall(r"GLIBC_(\d+\.\d+(?:\.\d+)?)", output)}
        require(bool(glibc) and max(glibc) <= (2, 35), "binary requires glibc newer than 2.35")
        return output
    output = run("otool", "-L", str(binary))
    libraries = [line.strip().split(" (", 1)[0] for line in output.splitlines()[1:]]
    require(all(name.startswith(("/usr/lib/", "/System/Library/")) for name in libraries),
            "macOS binary references a non-system library")
    load_commands = run("otool", "-l", str(binary))
    minimums = re.findall(r"\bminos (\d+\.\d+(?:\.\d+)?)", load_commands)
    require(bool(minimums) and all(tuple(map(int, value.split("."))) <= (15, 0, 0)
                                 for value in minimums), "unexpected macOS deployment target")
    run("codesign", "--verify", "--strict", str(binary))
    return output + "\n" + load_commands


def document_for_archive(path: Path, release_version: str) -> bytes:
    # Source links stay useful after extracting an archive without the repository.
    text = path.read_text()
    def replace(match):
        destination = match.group(1)
        if destination.startswith(("#", "http:", "https:", "mailto:")):
            return match.group(0)
        return "](" + f"https://github.com/{REPOSITORY}/blob/v{release_version}/" + destination + ")"
    return re.sub(r"\]\(([^)\s]+)\)", replace, text).encode()


def create_archive(destination: Path, prefix: str, payloads: dict[str, Path | bytes],
                   metadata: dict, timestamp: int) -> None:
    require(set(payloads) == PAYLOAD_NAMES, "release payload allowlist differs")
    require(set(metadata["files"]) == PAYLOAD_NAMES, "manifest file set differs")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("xb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as zipped:
            with tarfile.open(fileobj=zipped, mode="w", format=tarfile.PAX_FORMAT) as archive:
                contents = dict(payloads)
                contents["build-info.json"] = (json.dumps(metadata, indent=2, sort_keys=True) + "\n").encode()
                for name, value in sorted(contents.items()):
                    info = tarfile.TarInfo(f"{prefix}/{name}")
                    info.mode = 0o755 if name == "logex" else 0o644
                    info.mtime = timestamp
                    if isinstance(value, Path):
                        require(value.is_file() and not value.is_symlink(), "payload must be a regular file")
                        info.size = value.stat().st_size
                        with value.open("rb") as stream:
                            archive.addfile(info, stream)
                    else:
                        info.size = len(value)
                        archive.addfile(info, io.BytesIO(value))


def validate_archive(path: Path, release_version: str, source_commit: str, target: str) -> dict:
    require(path.name == archive_name(release_version, target), "unexpected release archive name")
    require(path.stat().st_size <= MAX_ARCHIVE_BYTES, "archive exceeds collection budget")
    prefix = path.name.removesuffix(".tar.gz")
    expected = {f"{prefix}/{name}" for name in PAYLOAD_NAMES | {"build-info.json"}}
    with tarfile.open(path, "r:gz") as archive:
        members = []
        unpacked_bytes = 0
        for member in archive:
            require(len(members) < len(expected), "archive has unexpected entries")
            require(member.isfile() and not member.issparse() and member.size >= 0,
                    "archive contains a non-regular entry")
            require(not member.pax_headers, "archive contains unexpected extended metadata")
            unpacked_bytes += member.size
            require(unpacked_bytes <= MAX_ARCHIVE_BYTES, "unpacked archive exceeds budget")
            members.append(member)
        require(len(members) == len(expected) and {m.name for m in members} == expected,
                "archive has missing, duplicate, or unexpected entries")
        info = archive.getmember(f"{prefix}/build-info.json")
        require(info.mode == 0o644 and info.size < 128 * 1024, "unexpected build metadata mode or size")
        metadata = json.load(archive.extractfile(info))
        require(metadata["format_version"] == 1 and metadata["version"] == release_version
                and metadata["source_commit"] == commit(source_commit)
                and metadata["target"] == target, "archive source/version/target differs")
        require(set(metadata["files"]) == PAYLOAD_NAMES, "manifest has an unexpected file set")
        for name in sorted(PAYLOAD_NAMES):
            member = archive.getmember(f"{prefix}/{name}")
            expected_mode = 0o755 if name == "logex" else 0o644
            require(member.mode == expected_mode, f"unexpected mode for {name}")
            recorded = metadata["files"][name]
            require(member.size == recorded["bytes"], f"size mismatch for {name}")
            with archive.extractfile(member) as stream:
                require(digest(stream) == recorded["sha256"], f"checksum mismatch for {name}")
        with archive.extractfile(f"{prefix}/logex") as stream:
            verify_binary_header(stream.read(32), target)
    return metadata


def smoke_archive(path: Path, release_version: str, source_commit: str, target: str) -> None:
    validate_archive(path, release_version, source_commit, target)
    with tempfile.TemporaryDirectory(prefix="logex-release-smoke-") as directory:
        root = Path(directory)
        executable = root / "logex"
        with tarfile.open(path, "r:gz") as archive:
            with archive.extractfile(path.name.removesuffix(".tar.gz") + "/logex") as source:
                with executable.open("xb") as destination:
                    shutil.copyfileobj(source, destination)
        executable.chmod(0o755)
        result = subprocess.run([str(executable), "--version"], capture_output=True, text=True,
                                timeout=30, check=True, cwd=root)
        require(result.stdout.strip() == f"logex {release_version}", "packaged CLI version differs")
        for arguments in [("--help",), ("sync", "--help"), ("repair", "--help")]:
            subprocess.run([str(executable), *arguments], stdout=subprocess.DEVNULL,
                           timeout=30, check=True, cwd=root)
        unused = root / "must-not-be-created"
        result = subprocess.run([str(executable), "--data-dir", str(unused), "sync",
                                 "--query-max-concurrent", "0"], capture_output=True, text=True,
                                timeout=30, cwd=root)
        require(result.returncode == 1
                and "invalid --query-max-concurrent / query_max_concurrent:" in result.stderr
                and not unused.exists(), "invalid admission did not fail before storage initialization")


def package(args) -> None:
    release_version, source_commit = source_identity(args.version)
    target = TARGET_BY_NAME[args.target]
    require(platform.system() == target["system"] and platform.machine() == target["machine"],
            "release must be built and tested on its native target")
    binary = args.binary.resolve()
    with binary.open("rb") as stream:
        verify_binary_header(stream.read(32), args.target)
    libraries = verify_runtime_libraries(binary, args.target)
    payloads = {
        "logex": binary,
        "README.md": document_for_archive(ROOT / "INSTALL.md", release_version),
        "SQL.md": document_for_archive(ROOT / "SQL.md", release_version),
        "LICENSE-MIT": ROOT / "LICENSE-MIT",
        "LICENSE-APACHE": ROOT / "LICENSE-APACHE",
        "Cargo.lock": ROOT / "Cargo.lock",
        "THIRD-PARTY-LICENSES.html": args.licenses.resolve(),
    }
    files = {name: {"bytes": value.stat().st_size if isinstance(value, Path) else len(value),
                    "sha256": sha256(value) if isinstance(value, Path) else hashlib.sha256(value).hexdigest()}
             for name, value in payloads.items()}
    metadata = {
        "format_version": 1, "version": release_version, "source_commit": source_commit,
        "repository": REPOSITORY, "target": args.target,
        "minimum_os": target["minimum_os"], "rustc": run("rustc", "--version", "--verbose"),
        "cargo": run("cargo", "--version"), "runtime_libraries": libraries,
        "github_run_id": os.environ.get("GITHUB_RUN_ID"),
        "apple_notarized": False, "files": files,
    }
    path = args.output / archive_name(release_version, args.target)
    create_archive(path, path.name.removesuffix(".tar.gz"), payloads, metadata,
                   int(run("git", "show", "-s", "--format=%ct", "HEAD")))
    smoke_archive(path, release_version, source_commit, args.target)
    print(json.dumps({"archive": str(path), "sha256": sha256(path), "bytes": path.stat().st_size}))


def verify_attestation(archive: Path, source_commit: str) -> None:
    bundle = Path(str(archive) + ".sigstore.json")
    require(bundle.is_file() and not bundle.is_symlink(), "missing attestation bundle")
    subprocess.run(["gh", "attestation", "verify", str(archive), "--repo", REPOSITORY,
                    "--bundle", str(bundle), "--signer-workflow", WORKFLOW,
                    "--source-ref", "refs/heads/master", "--source-digest", source_commit,
                    "--signer-digest", source_commit, "--deny-self-hosted-runners"], check=True)


def assemble(args) -> None:
    release_version, source_commit = source_identity(args.version)
    expected = {archive_name(release_version, target) for target in TARGET_BY_NAME}
    if args.attestations:
        expected |= {name + ".sigstore.json" for name in expected.copy()}
    require({p.name for p in args.directory.iterdir()} == expected, "release artifact set differs")
    records = []
    for target in TARGET_BY_NAME:
        archive = args.directory / archive_name(release_version, target)
        require(archive.is_file() and not archive.is_symlink(), "invalid artifact path")
        metadata = validate_archive(archive, release_version, source_commit, target)
        require(metadata["github_run_id"] == os.environ.get("GITHUB_RUN_ID"), "mixed workflow runs")
        if args.attestations:
            verify_attestation(archive, source_commit)
        records.append({"archive": archive.name, "sha256": sha256(archive),
                        "bytes": archive.stat().st_size, "target": target,
                        "binary_sha256": metadata["files"]["logex"]["sha256"],
                        "minimum_os": metadata["minimum_os"]})
    manifest = {"format_version": 1, "version": release_version, "source_commit": source_commit,
                "repository": REPOSITORY, "github_run_id": os.environ.get("GITHUB_RUN_ID"),
                "artifacts": records}
    with (args.directory / "release-manifest.json").open("x") as output:
        output.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    files = sorted(args.directory.iterdir())
    with (args.directory / "SHA256SUMS").open("x") as output:
        output.writelines(f"{sha256(path)}  {path.name}\n" for path in files)
    print(json.dumps(manifest, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    metadata = commands.add_parser("metadata")
    metadata.add_argument("--version")
    metadata.add_argument("--github-output", type=Path)
    build = commands.add_parser("package")
    build.add_argument("--version")
    build.add_argument("--target", required=True, choices=TARGET_BY_NAME)
    build.add_argument("--binary", type=Path, required=True)
    build.add_argument("--licenses", type=Path, required=True)
    build.add_argument("--output", type=Path, required=True)
    collect = commands.add_parser("assemble")
    collect.add_argument("--version")
    collect.add_argument("--directory", type=Path, required=True)
    collect.add_argument("--attestations", action="store_true")
    args = parser.parse_args()
    if args.command == "metadata":
        release_version, source_commit = source_identity(args.version)
        require(len(TARGET_BY_NAME) == len(TARGETS) == 4, "release matrix must contain four unique targets")
        outputs = {"version": release_version, "commit": source_commit,
                   "matrix": json.dumps({"include": TARGETS}, separators=(",", ":"))}
        if args.github_output:
            with args.github_output.open("a") as stream:
                stream.writelines(f"{key}={value}\n" for key, value in outputs.items())
        print(json.dumps(outputs, indent=2))
    elif args.command == "package":
        package(args)
    else:
        assemble(args)


if __name__ == "__main__":
    main()
