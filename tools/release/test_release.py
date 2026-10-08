"""Release boundary regressions: provenance, hostile archives, and publication inputs."""

import copy
import hashlib
import io
import json
from pathlib import Path
import struct
import tarfile
import tempfile
import unittest
from unittest import mock

import package
import publish


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.version = "0.1.0"
        self.commit = "a" * 40

    @staticmethod
    def header(target):
        header = bytearray(64)
        if target.endswith("linux-gnu"):
            header[:6] = b"\x7fELF\x02\x01"
            struct.pack_into("<H", header, 18, 62 if target.startswith("x86_64") else 183)
        else:
            header[:4] = b"\xcf\xfa\xed\xfe"
            struct.pack_into("<I", header, 4, 0x01000007 if target.startswith("x86_64") else 0x0100000C)
        return bytes(header)

    def fixture(self, target="x86_64-unknown-linux-gnu", directory=None):
        payloads = {name: (name + "\n").encode() for name in package.PAYLOAD_NAMES}
        payloads["logex"] = self.header(target)
        metadata = {
            "format_version": 1, "version": self.version, "source_commit": self.commit,
            "target": target, "files": {
                name: {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
                for name, data in payloads.items()
            },
        }
        destination = (directory or self.root) / package.archive_name(self.version, target)
        package.create_archive(destination, destination.name.removesuffix(".tar.gz"), payloads, metadata, 1)
        return destination, metadata

    def rewrite(self, archive_path, transform):
        with tarfile.open(archive_path) as archive:
            entries = [(copy.copy(member), archive.extractfile(member).read()) for member in archive]
        entries = transform(entries)
        with tarfile.open(archive_path, "w:gz") as archive:
            for member, contents in entries:
                member.size = len(contents)
                archive.addfile(member, io.BytesIO(contents))

    def validate(self, path, target="x86_64-unknown-linux-gnu", commit=None):
        return package.validate_archive(path, self.version, commit or self.commit, target)

    def test_every_architecture_roundtrips_with_exact_file_hashes(self):
        for target in package.TARGET_BY_NAME:
            with self.subTest(target=target):
                path, metadata = self.fixture(target)
                self.assertEqual(self.validate(path, target), metadata)

    def test_same_inputs_produce_identical_archives(self):
        first, _ = self.fixture(directory=self.root / "first")
        second, _ = self.fixture(directory=self.root / "second")
        self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_existing_archive_is_never_overwritten(self):
        path, _ = self.fixture()
        original = path.read_bytes()
        with self.assertRaises(FileExistsError):
            self.fixture()
        self.assertEqual(path.read_bytes(), original)

    def test_wrong_commit_or_target_is_rejected(self):
        path, _ = self.fixture()
        with self.assertRaisesRegex(ValueError, "identity|source/version/target"):
            self.validate(path, commit="b" * 40)
        wrong_name = self.root / package.archive_name(self.version, "aarch64-unknown-linux-gnu")
        path.rename(wrong_name)
        old_prefix = path.name.removesuffix(".tar.gz")
        new_prefix = wrong_name.name.removesuffix(".tar.gz")
        def rename_entries(entries):
            for member, _ in entries:
                member.name = member.name.replace(old_prefix, new_prefix, 1)
            return entries
        self.rewrite(wrong_name, rename_entries)
        with self.assertRaisesRegex(ValueError, "source/version/target"):
            self.validate(wrong_name, "aarch64-unknown-linux-gnu")

    def test_architecture_checks_use_binary_headers(self):
        for target in package.TARGET_BY_NAME:
            for actual in package.TARGET_BY_NAME:
                if target != actual:
                    with self.subTest(target=target, actual=actual), self.assertRaises(ValueError):
                        package.verify_binary_header(self.header(actual), target)
        with self.assertRaisesRegex(ValueError, "truncated"):
            package.verify_binary_header(b"", "x86_64-unknown-linux-gnu")

    def test_archive_corruption_fails_even_with_valid_metadata(self):
        path, _ = self.fixture()
        def corrupt(entries):
            return [(member, b"x" * len(data) if member.name.endswith("/SQL.md") else data)
                    for member, data in entries]
        self.rewrite(path, corrupt)
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.validate(path)

    def test_duplicate_traversal_and_extra_entries_are_rejected(self):
        for name in ("duplicate", "../outside", "credentials.env"):
            with self.subTest(name=name):
                path, _ = self.fixture(directory=self.root / name.replace("/", "_"))
                def extra(entries):
                    member, content = entries[0]
                    member = copy.copy(member)
                    if name != "duplicate":
                        member.name = name
                    return entries + [(member, content)]
                self.rewrite(path, extra)
                with self.assertRaisesRegex(ValueError, "entries"):
                    self.validate(path)

    def test_symlinks_and_privileged_modes_are_rejected(self):
        for kind in ("symlink", "setuid"):
            with self.subTest(kind=kind):
                path, _ = self.fixture(directory=self.root / kind)
                def alter(entries):
                    for member, _ in entries:
                        if member.name.endswith("/logex"):
                            if kind == "symlink":
                                member.type = tarfile.SYMTYPE
                                member.linkname = "/outside"
                            else:
                                member.mode = 0o4755
                    return entries
                self.rewrite(path, alter)
                with self.assertRaisesRegex(ValueError, "non-regular|mode"):
                    self.validate(path)

    def test_versions_and_target_names_cannot_inject_paths_or_commands(self):
        for value in ("../0.1.0", "0.1.0\nother=1", "0.1.0;id", "v0.1.0", "01.0.0", "0.1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                package.version(value)
        with self.assertRaises(ValueError):
            package.archive_name(self.version, "../../outside")

    def test_extended_archive_metadata_and_privileged_manifest_are_rejected(self):
        for kind in ("pax", "mode"):
            with self.subTest(kind=kind):
                path, _ = self.fixture(directory=self.root / kind)
                def alter(entries):
                    for member, _ in entries:
                        if member.name.endswith("/build-info.json"):
                            if kind == "pax":
                                member.pax_headers = {"comment": "unexpected extension"}
                            else:
                                member.mode = 0o4755
                    return entries
                self.rewrite(path, alter)
                with self.assertRaisesRegex(ValueError, "metadata"):
                    self.validate(path)

    def test_compressed_archive_cannot_exceed_unpacked_budget(self):
        path, _ = self.fixture()
        self.rewrite(path, lambda entries: [
            (member, b"x" * 4096 if member.name.endswith("/SQL.md") else data)
            for member, data in entries
        ])
        self.assertLess(path.stat().st_size, 2048)
        with mock.patch.object(package, "MAX_ARCHIVE_BYTES", 2048):
            with self.assertRaisesRegex(ValueError, "unpacked archive exceeds"):
                self.validate(path)

    def test_checksum_manifest_requires_every_file_once(self):
        (self.root / "asset").write_bytes(b"payload")
        checksum = package.sha256(self.root / "asset")
        manifest = self.root / "SHA256SUMS"
        manifest.write_text(f"{checksum}  asset\n")
        publish.verify_checksums(self.root, {"asset"})
        for bad in ("", f"{checksum}  ../asset\n", f"{checksum}  asset\n{checksum}  asset\n", f"{'0' * 64}  asset\n"):
            with self.subTest(bad=bad):
                manifest.write_text(bad)
                with self.assertRaises(ValueError):
                    publish.verify_checksums(self.root, {"asset"})

    def test_ci_requires_latest_exact_default_branch_run_to_succeed(self):
        successful = {"id": 1, "head_sha": self.commit, "head_branch": "master",
                      "event": "push", "status": "completed", "conclusion": "success"}
        self.assertEqual(publish.check_ci_runs([successful], self.commit), successful)
        for field, value in (("head_sha", "b" * 40), ("head_branch", "feature/test"),
                             ("event", "pull_request"), ("status", "in_progress"),
                             ("conclusion", "failure"), ("conclusion", "cancelled")):
            with self.subTest(field=field, value=value):
                changed = dict(successful, **{field: value})
                with self.assertRaises(ValueError):
                    publish.check_ci_runs([changed], self.commit)
        with self.assertRaisesRegex(ValueError, "not succeeded"):
            publish.check_ci_runs([successful, dict(successful, id=2, conclusion="failure")], self.commit)

    def test_remote_assets_must_match_upload_digest_and_size(self):
        path = self.root / "asset"
        path.write_bytes(b"payload")
        asset = {"name": "asset", "state": "uploaded", "size": 7,
                 "digest": "sha256:" + package.sha256(path)}
        publish.verify_remote_assets({"assets": [asset]}, self.root)
        for field, value in (("name", "other"), ("state", "new"), ("size", 8), ("digest", None)):
            with self.subTest(field=field), self.assertRaises(ValueError):
                publish.verify_remote_assets({"assets": [dict(asset, **{field: value})]}, self.root)


if __name__ == "__main__":
    unittest.main()
