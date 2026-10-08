#!/usr/bin/env python3
"""Create a fully verified draft, then explicitly publish it without replacing assets."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

from package import (REPOSITORY, ROOT, TARGET_BY_NAME, archive_name, commit, require,
                     run, sha256, source_version, validate_archive, verify_attestation, version)

CI_JOB_NAMES = {"Check", "Format", "Clippy", "Test", "macOS Test", "Release Build"}


def api(endpoint: str, payload: dict | None = None):
    args = ["gh", "api", f"repos/{REPOSITORY}/{endpoint}"]
    if payload is not None:
        args += ["--method", "POST", "--input", "-"]
    result = subprocess.run(args, input=json.dumps(payload) if payload is not None else None,
                            capture_output=True, text=True, timeout=180, check=True)
    return json.loads(result.stdout) if result.stdout.strip() else None


def check_ci_runs(runs: list[dict], expected_commit: str) -> dict:
    matching = [item for item in runs if item["head_sha"] == expected_commit
                and item["head_branch"] == "master" and item["event"] == "push"]
    require(bool(matching), "no default-branch CI run for this exact commit")
    latest = max(matching, key=lambda item: item["id"])
    require(latest["status"] == "completed" and latest["conclusion"] == "success",
            "latest exact-commit CI run has not succeeded")
    return latest


def require_ci(expected_commit: str) -> None:
    listing = api(f"actions/workflows/ci.yml/runs?head_sha={expected_commit}&event=push&per_page=100")
    latest = check_ci_runs(listing["workflow_runs"], expected_commit)
    jobs = api(f"actions/runs/{latest['id']}/jobs?filter=latest&per_page=100")
    require(jobs["total_count"] == len(jobs["jobs"]), "CI job listing was truncated")
    require(CI_JOB_NAMES <= {item["name"] for item in jobs["jobs"]}, "required CI jobs are missing")
    require(all(item["status"] == "completed" and item["conclusion"] == "success"
                for item in jobs["jobs"]), "a CI job did not succeed")


def verify_checksums(directory: Path, expected_names: set[str]) -> None:
    require({p.name for p in directory.iterdir()} == expected_names | {"SHA256SUMS"},
            "release directory contains unexpected or missing files")
    require(all(p.is_file() and not p.is_symlink() for p in directory.iterdir()),
            "release files must be regular files")
    lines = (directory / "SHA256SUMS").read_text().splitlines()
    entries = {}
    for line in lines:
        match = re.fullmatch(r"([0-9a-f]{64})  ([A-Za-z0-9_.-]+)", line)
        require(match is not None, "invalid checksum line")
        value, name = match.groups()
        require(name not in entries, "duplicate checksum entry")
        entries[name] = value
    require(set(entries) == expected_names, "checksum file set differs")
    for name, expected in entries.items():
        require(sha256(directory / name) == expected, f"release checksum differs: {name}")


def verify_directory(directory: Path, release_version: str, expected_commit: str) -> dict:
    version(release_version)
    commit(expected_commit)
    require(source_version() == release_version, "release differs from the checkout version")
    # A squash merge can change the commit while preserving the reviewed tree.
    # Require the actual checkout files to match the explicit release source.
    run("git", "diff", "--exit-code", expected_commit, "--")
    names = {archive_name(release_version, target) for target in TARGET_BY_NAME}
    names |= {name + ".sigstore.json" for name in names.copy()}
    names.add("release-manifest.json")
    verify_checksums(directory, names)
    manifest = json.loads((directory / "release-manifest.json").read_text())
    require(manifest["format_version"] == 1 and manifest["version"] == release_version
            and manifest["source_commit"] == expected_commit
            and manifest["repository"] == REPOSITORY, "release manifest identity differs")
    require(len(manifest["artifacts"]) == len(TARGET_BY_NAME)
            and {a["target"] for a in manifest["artifacts"]} == set(TARGET_BY_NAME),
            "release manifest has missing or duplicate targets")
    for record in manifest["artifacts"]:
        name = archive_name(release_version, record["target"])
        archive = directory / name
        require(record["archive"] == name and record["sha256"] == sha256(archive)
                and record["bytes"] == archive.stat().st_size, "release manifest archive differs")
        metadata = validate_archive(archive, release_version, expected_commit, record["target"])
        require(metadata["github_run_id"] == manifest["github_run_id"], "mixed package runs")
        require(record["binary_sha256"] == metadata["files"]["logex"]["sha256"]
                and record["minimum_os"] == metadata["minimum_os"], "binary metadata differs")
        verify_attestation(archive, expected_commit)
    require(bool(re.fullmatch(r"\d+", str(manifest["github_run_id"]))), "missing workflow run ID")
    return manifest


def require_tag(tag: str, expected_commit: str) -> None:
    reference = api(f"git/ref/tags/{tag}")
    require(reference["object"]["type"] == "tag", "release tag must be annotated")
    annotation = api(f"git/tags/{reference['object']['sha']}")
    require(annotation["tag"] == tag and annotation["object"]["type"] == "commit"
            and annotation["object"]["sha"] == expected_commit, "release tag points elsewhere")


def releases() -> list[dict]:
    pages = json.loads(run("gh", "api", "--paginate", "--slurp",
                           f"repos/{REPOSITORY}/releases?per_page=100"))
    return [item for page in pages for item in page]


def release_for_tag(tag: str) -> dict:
    # The published-release-by-tag endpoint must not be used to discover drafts.
    matches = [item for item in releases() if item["tag_name"] == tag]
    require(len(matches) == 1, "expected exactly one visible release for the tag")
    return api(f"releases/{matches[0]['id']}")


def verify_notes(release: dict, release_version: str) -> None:
    expected = (ROOT / "docs" / "releases" / f"v{release_version}.md").read_text().rstrip()
    require(release["name"] == f"LogEx v{release_version}" and release["body"].rstrip() == expected,
            "release title or notes differ from reviewed source")


def verify_remote_assets(release: dict, directory: Path) -> None:
    local = {path.name: path for path in directory.iterdir()}
    assets = release["assets"]
    require(len(assets) == len(local) and {a["name"] for a in assets} == set(local),
            "remote release asset set differs")
    for asset in assets:
        path = local[asset["name"]]
        require(asset["state"] == "uploaded" and asset["size"] == path.stat().st_size
                and asset.get("digest") == "sha256:" + sha256(path),
                f"remote release asset differs: {asset['name']}")


def draft(args) -> None:
    require(os.environ.get("GITHUB_EVENT_NAME") == "workflow_dispatch"
            and os.environ.get("GITHUB_REF") == "refs/heads/master"
            and os.environ.get("GITHUB_REPOSITORY") == REPOSITORY,
            "draft creation requires the default-branch release workflow")
    expected_commit = commit(os.environ["GITHUB_SHA"])
    manifest = verify_directory(args.directory, args.version, expected_commit)
    require(manifest["github_run_id"] == os.environ.get("GITHUB_RUN_ID"), "draft belongs to another run")
    require_ci(expected_commit)
    notes = ROOT / "docs" / "releases" / f"v{args.version}.md"
    require(notes.is_file(), "reviewed release notes are missing")
    tag = f"v{args.version}"
    refs = api(f"git/matching-refs/tags/{tag}")
    require(not any(item["ref"] == f"refs/tags/{tag}" for item in refs),
            "tag already exists; investigate rather than overwrite or silently resume")
    require(not any(item["tag_name"] == tag for item in releases()), "release already exists")
    annotation = api("git/tags", {"tag": tag, "object": expected_commit, "type": "commit",
                                 "message": f"LogEx {tag}\n\nSource: {expected_commit}\nRelease run: {manifest['github_run_id']}\n"})
    api("git/refs", {"ref": f"refs/tags/{tag}", "sha": annotation["sha"]})
    subprocess.run(["gh", "release", "create", tag, "--repo", REPOSITORY, "--verify-tag",
                    "--draft", "--title", f"LogEx {tag}", "--notes-file", str(notes),
                    *[str(path) for path in sorted(args.directory.iterdir())]], check=True)
    require_tag(tag, expected_commit)
    release = release_for_tag(tag)
    require(release["draft"], "release was unexpectedly published")
    verify_notes(release, args.version)
    verify_remote_assets(release, args.directory)
    print(json.dumps({"draft_url": release["html_url"], "source_commit": expected_commit}))


def publish(args) -> None:
    expected_commit = commit(args.commit)
    manifest = verify_directory(args.directory, args.version, expected_commit)
    require_ci(expected_commit)
    build = api(f"actions/runs/{manifest['github_run_id']}")
    require(build["head_sha"] == expected_commit and build["head_branch"] == "master"
            and build["event"] == "workflow_dispatch"
            and build["path"] == ".github/workflows/release.yml"
            and build["status"] == "completed" and build["conclusion"] == "success",
            "the source-bound release workflow has not completed successfully")
    immutable = api("immutable-releases")
    require(immutable["enabled"], "enable immutable releases before publishing")
    tag = f"v{args.version}"
    require_tag(tag, expected_commit)
    release = release_for_tag(tag)
    require(release["draft"] and not release["prerelease"], "expected an unpublished regular release")
    verify_notes(release, args.version)
    verify_remote_assets(release, args.directory)
    subprocess.run(["gh", "release", "edit", tag, "--repo", REPOSITORY, "--draft=false", "--latest"], check=True)
    published = api(f"releases/tags/{tag}")
    require(not published["draft"] and published["immutable"], "release did not become immutable")
    verify_remote_assets(published, args.directory)
    subprocess.run(["gh", "release", "verify", tag, "--repo", REPOSITORY], check=True)
    print(json.dumps({"url": published["html_url"], "source_commit": expected_commit,
                      "immutable": published["immutable"], "assets": len(published["assets"])}))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("draft", "publish"):
        command = commands.add_parser(name)
        command.add_argument("--version", required=True, type=version)
        command.add_argument("--directory", required=True, type=Path)
        if name == "publish":
            command.add_argument("--commit", required=True, type=commit)
    args = parser.parse_args()
    (draft if args.command == "draft" else publish)(args)


if __name__ == "__main__":
    main()
