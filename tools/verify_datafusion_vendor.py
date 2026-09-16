#!/usr/bin/env python3
"""Verify complete vendored crates and reviewed local patches."""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from pathlib import Path, PurePosixPath
import string
import sys


ROOT = Path(__file__).resolve().parents[1]
ADDITIONS = frozenset(
    {
        PurePosixPath("LOGEX-PATCH.diff"),
        PurePosixPath("LOGEX-PATCH.md"),
        PurePosixPath("LOGEX-UPSTREAM-SHA256"),
    }
)


@dataclass(frozen=True)
class Package:
    name: str
    inventory_checksum: str
    modified: dict[PurePosixPath, str]
    patch_checksum: str
    added: dict[PurePosixPath, str] = field(default_factory=dict)

    @property
    def directory(self) -> Path:
        return ROOT / "vendor" / self.name


PACKAGES = (
    Package(
        name="datafusion-sql",
        inventory_checksum="0cde1d552a958a7a7dca76c473334db21d0317313821fb8ba49bc8156ad34ec8",
        modified={
            PurePosixPath("src/select.rs"): "366fb326d853dd359622ba8ada0485515e2c5643f2a05cf02752eb5b1d14ab5a",
        },
        patch_checksum="a7b520cf492ed95209a8335d88c60eec308f01cdd3e29c0734a597ca98f5bb21",
    ),
    Package(
        name="datafusion-optimizer",
        inventory_checksum="7b3b5eb3eba4fcb00d4173f7603cf4661a485930ac909071b822a8c3e718d1c2",
        modified={
            PurePosixPath("src/simplify_expressions/expr_simplifier.rs"): "5d28080f7acf944b63bfbfc6bb48ffddd7764df675dac3a4bdf7028998a9edb2",
            PurePosixPath("src/eliminate_filter.rs"): "6a50ecd0bd22c65fcc15ffa73dc722d7946c89f3ee858ae7361b027c0ad8890d",
        },
        patch_checksum="2e4fd87500b2d1f8e6984f3ffc50bf0dca478799e3b5a88b420e1f80c6311729",
    ),
    Package(
        name="reth-network",
        inventory_checksum="12c15956c224e85aca12c00b90c33b13254df65913695f4495a53169dbb43be0",
        modified={
            PurePosixPath("Cargo.toml"): "41475c2237ce54f86ee60f30c0ef672a741bc1079e394b32715d95e9b960a302",
            PurePosixPath("src/eth_requests.rs"): "e1526e03d76643b115677eafc9a0b67a3529b88b24a7c7061ed4c5e7083e0b4d",
            PurePosixPath("src/session/active.rs"): "a4c0d8b50fbe7cb3895723d6f935abd1c13f44fcd8471463f7aad1c4dcf93534",
            PurePosixPath("src/session/mod.rs"): "fc8dc897438d2d4ce8f6bfd90455250110af1c036483fe23bb22e8758dac688b",
            PurePosixPath("src/session/types.rs"): "584a6bff5d489f973dfecd96d5f5334943f4403a348eba30a863b86053d3f8d1",
        },
        patch_checksum="6c14b3f9c7db7dbc67da9ac229f1936175376eec8e49bf0b644b2377d3174ac7",
        added={
            PurePosixPath("src/session/range_update.rs"): "5518a3cc920616fe298bd4fb2ce859ef57364bbabfd45823515cd665730ba527",
        },
    ),
)


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_checksum(checksum: str, context: str) -> None:
    if (
        len(checksum) != 64
        or checksum != checksum.lower()
        or any(character not in string.hexdigits for character in checksum)
    ):
        raise ValueError(f"invalid SHA-256 for {context}: {checksum!r}")


def read_inventory(package: Package) -> dict[PurePosixPath, str]:
    inventory_path = package.directory / "LOGEX-UPSTREAM-SHA256"
    found_inventory = digest(inventory_path)
    if found_inventory != package.inventory_checksum:
        raise ValueError(
            f"{package.name} inventory differs "
            f"({found_inventory} != {package.inventory_checksum})"
        )
    inventory = inventory_path.read_bytes().decode("utf-8")
    expected: dict[PurePosixPath, str] = {}
    for line_number, line in enumerate(inventory.splitlines(), 1):
        checksum, separator, raw_path = line.partition("  ./")
        if not separator:
            raise ValueError(
                f"{package.name} inventory line {line_number} is malformed: {line!r}"
            )
        validate_checksum(checksum, f"{package.name} inventory line {line_number}")
        if not raw_path:
            raise ValueError(
                f"{package.name} inventory line {line_number} has an empty path"
            )
        path = PurePosixPath(raw_path)
        if (
            path.is_absolute()
            or "\\" in raw_path
            or any(part in {"", ".", ".."} for part in path.parts)
            or raw_path != path.as_posix()
        ):
            raise ValueError(
                f"{package.name} inventory path is not normalized and relative: {raw_path!r}"
            )
        if path in expected:
            raise ValueError(f"{package.name} inventory repeats {path}")
        expected[path] = checksum

    if not expected:
        raise ValueError(f"{package.name} inventory is empty")
    for modified in package.modified:
        if modified not in expected:
            raise ValueError(
                f"{package.name} inventory does not contain modified file {modified}"
            )
    return expected


def actual_files(package: Package) -> set[PurePosixPath]:
    actual = set()
    for path in package.directory.rglob("*"):
        relative = PurePosixPath(path.relative_to(package.directory).as_posix())
        if path.is_symlink():
            raise ValueError(f"{package.name} contains a symlink: {relative}")
        if path.is_file():
            actual.add(relative)
        elif not path.is_dir():
            raise ValueError(f"{package.name} contains a special file: {relative}")
    return actual


def verify_package(package: Package) -> int:
    if package.directory.is_symlink():
        raise ValueError(f"{package.name} package directory is a symlink")
    if not package.directory.is_dir():
        raise ValueError(f"{package.name} package directory is missing")
    validate_checksum(
        package.inventory_checksum, f"{package.name} reviewed inventory"
    )
    for path, checksum in package.modified.items():
        validate_checksum(checksum, f"{package.name} reviewed file {path}")
    validate_checksum(package.patch_checksum, f"{package.name} patch diff")

    expected = read_inventory(package)
    for path, checksum in package.added.items():
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in path.as_posix()
            or path == PurePosixPath(".")
            or path in expected
            or path in ADDITIONS
        ):
            raise ValueError(f"{package.name} invalid added file path: {path}")
        validate_checksum(checksum, f"{package.name} reviewed added file {path}")
    permitted = set(expected) | set(ADDITIONS) | set(package.added)
    actual = actual_files(package)
    if actual != permitted:
        missing = sorted(str(path) for path in permitted - actual)
        extra = sorted(str(path) for path in actual - permitted)
        raise ValueError(
            f"{package.name} file set differs; missing={missing}, extra={extra}"
        )

    for path, original_checksum in expected.items():
        if path in package.modified:
            wanted = package.modified[path]
            description = "reviewed local version"
        else:
            wanted = original_checksum
            description = "published upstream version"
        found = digest(package.directory / Path(path))
        if found != wanted:
            raise ValueError(
                f"{package.name}/{path} differs from its {description} "
                f"({found} != {wanted})"
            )

    for path, wanted in package.added.items():
        found = digest(package.directory / Path(path))
        if found != wanted:
            raise ValueError(
                f"{package.name}/{path} differs from its reviewed added version "
                f"({found} != {wanted})"
            )

    patch_path = package.directory / "LOGEX-PATCH.diff"
    found_patch = digest(patch_path)
    if found_patch != package.patch_checksum:
        raise ValueError(
            f"{package.name} patch diff differs ({found_patch} != {package.patch_checksum})"
        )
    print(
        f"verified {package.name}: {len(expected)} published files, "
        f"{len(package.modified)} reviewed modifications, {len(package.added)} reviewed additions"
    )
    return len(expected)


def main() -> int:
    total = sum(verify_package(package) for package in PACKAGES)
    print(f"verified {total} upstream files across {len(PACKAGES)} vendored crates")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, UnicodeError, ValueError) as error:
        print(f"vendor verification failed: {error}", file=sys.stderr)
        sys.exit(1)
