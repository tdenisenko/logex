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
    provenance_note: PurePosixPath = PurePosixPath("LOGEX-PATCH.md")

    @property
    def directory(self) -> Path:
        return ROOT / "vendor" / self.name


PACKAGES = (
    Package(
        name="libp2p-connection-limits",
        inventory_checksum="54fcaffc6672e5d0c25eb32c003ea55580a132df050cf3f9873254a31611d359",
        modified={
            PurePosixPath("Cargo.lock"): "a848809e1e66f58f3d073ac78d0b1a52abc42e381489de114498adb73b0202f0",
            PurePosixPath("Cargo.toml"): "3592d7289da62ffced42d25be9940d229a18a79ea860dbfd0d042460bbe1103d",
            PurePosixPath("src/lib.rs"): "71e9d25db2cf796c01072e59738f2d48d7eeeee4c966ac653b47c5b48d7d48ff",
        },
        patch_checksum="f40b0048f48bbc5d82735dbd36bf49665a235139dddbc72cb91496fa1311f4dd",
        provenance_note=PurePosixPath("LOGEX-PATCH.txt"),
    ),
    Package(
        name="libp2p-gossipsub",
        inventory_checksum="795183902927a7de54e1f7927d0038f0758420d365f92f357908b3d62fe47289",
        modified={
            PurePosixPath("Cargo.lock"): "9c251863c193ed1c83b1e63ccc6765b553fc3cf4687464377e1b4bbc8d3cf1eb",
            PurePosixPath("Cargo.toml"): "0e8db4bae3efe861d432bb895baefb868a7fba6f0d4f7dc5ee65bde05bbe4d61",
            PurePosixPath("src/backoff.rs"): "692cb4879668eedb71366b38abe2f1b23fc2d6cd8727c730f2ac70d7a8e7acb3",
            PurePosixPath("src/behaviour.rs"): "74a01ad983886735f4a0c736d6bed0ff511951e3564cc63d751d38ff14a2288c",
            PurePosixPath("src/behaviour/tests.rs"): "29c6b35b25d82521f8ce5ceae9d2187eb0d946fe4c8587e8cab366ce7b6df181",
            PurePosixPath("src/config.rs"): "ccc70acb769b367912033d54ffcaa70c07d43c9409a1ccb56a118b842130370d",
            PurePosixPath("src/error.rs"): "9e2231b23f4c85cba02d82abd98d9b842cb83f1bd753524720e5fb37af452772",
            PurePosixPath("src/gossip_promises.rs"): "19b3309b949ea2c2da200fffa09aaa35327a1350e90f6fa1de0aff2e9e628ed2",
            PurePosixPath("src/handler.rs"): "01da20f3633c1f32c1af2e3e2f29bf07cf83c16bfe40674198a01d9a42f6442b",
            PurePosixPath("src/lib.rs"): "5b70d90a6272aabd0be9a61f6baef3d06892e4fcd03cb75b53e79a4c5e8a2d9e",
            PurePosixPath("src/mcache.rs"): "0009f428b925efb6053ef6854a3b49fc61e92e9c442b2c12de094278594497cb",
            PurePosixPath("src/peer_score.rs"): "bc7fee1e17a343ee112e103dc112763f717cfb7c9def011945ee0fe28ad7ca63",
            PurePosixPath("src/rpc.rs"): "734b431caa3589cc5027683105e5cf5eea789fae86a73ebe6c1d6ec2d47b743d",
            PurePosixPath("src/subscription_filter.rs"): "3e0a08d79be552532b60a9fef0141dbb361a68f4525e14aedd227d27d06fc6e5",
            PurePosixPath("src/time_cache.rs"): "3f0226bcb0a3f709de9704372179a22c125f8ca6e6a1ea6937310980020baed5",
            PurePosixPath("src/topic.rs"): "cbfc7410a2f01d8f0b5477c40084397b8a9f7a79300861a2e44a63fe6d5c21da",
            PurePosixPath("src/types.rs"): "70f62528b88a70c801c8b6cf291bab0fcde4e08350d00a8fab3837e85a216ad8",
        },
        patch_checksum="0a9e4ee596f7122b934639eab4d414b401a5755da022c939435fbeeb42f11675",
        added={
            PurePosixPath("src/event_queue.rs"): "b0dbaf2c61970e7b78a65e9d53298b2e90ad76e2081e7f7453ea5e0523dddbb7",
        },
    ),
    Package(
        name="datafusion-functions-aggregate",
        inventory_checksum="ffa15a2b3a48646a9276999ab6afc77b3a20b0917b788137262d386be1b6ce69",
        modified={
            PurePosixPath("src/sum.rs"): "7e32b0693d0cc0f3f87ea06f8d1eec24c1a7867fc0075e739d61336dabae4f80",
            PurePosixPath("src/average.rs"): "1c7593be41592cc411a14a9c7ac0a3edc0b056b4220bd67fdb03218f1456279f",
        },
        patch_checksum="ec4f88b7df5c8ca9292c3e7d3074cc0006be03b9bf73352354752c730ef38324",
    ),
    Package(
        name="datafusion-functions-aggregate-common",
        inventory_checksum="44ecc4490539baefaafe85c44058a761aa1afb343bb38aad4c4fe24197fa142a",
        modified={
            PurePosixPath("src/aggregate/sum_distinct/numeric.rs"): "200e613a0e19b80d5dfa3404ce5c5a405dc905cd8b66608ae4f0de244b5cf124",
            PurePosixPath("src/aggregate/avg_distinct/decimal.rs"): "18737d6fdcedf1e2fce4f86497425e73d96c7d3fcbfe34c2adb7736fe8002742",
            PurePosixPath("src/utils.rs"): "8d2d1a38a2a0cbeff691d4f3196573500f77780131741d3e263d7a9ecbf532ff",
        },
        patch_checksum="a2fd4f7c06aef7502a244fa254246b26a08c5319cdc2204c9525a7165283ac5a",
    ),
    Package(
        name="datafusion-functions-nested",
        inventory_checksum="ce4d4d617631f9af7c84f64f4d320d8c0a32ddbc5034aa0b62fc904f4b9617cb",
        modified={
            PurePosixPath("src/array_has.rs"): "04a62dede5e6ef776ff06563afa48a8e2bfa9df7407fa5940a5c4e0aa487b059",
        },
        patch_checksum="457a9bb118e03dfa812f34af5d1feca77cf6031e49f0488d93a4a05ebfe7d1ce",
    ),
    Package(
        name="datafusion-functions",
        inventory_checksum="1f987c6d9a89c270e6c17922200469c51627998c9f0768368442fb0431d1d7de",
        modified={
            PurePosixPath("src/math/log.rs"): "b3d76a25320c6dc314aedf61384da5d16b6b353b9bfa582263eeac81b329216b",
            PurePosixPath("src/math/power.rs"): "4fc54d4122bc364dd18bfa69e3e004f2b5835201dba1893c59adc4cdb8f56180",
        },
        patch_checksum="ade213cbee5eb6d236b0f66e702d6b09040d11c253e9c96fc21b28500fd18241",
    ),


    Package(
        name="enr",
        inventory_checksum="ae2056c9fe60b5e2f07e2d6a9ed545a45291bdf2e99d80510823591d63863693",
        modified={
            PurePosixPath("src/lib.rs"): "b34435ea6fcf4491ad2ed4a290a1e20295f2a1d8eb3c78d84b4634662050c183",
        },
        patch_checksum="008e041224280f02ea7f6139837db8f1ccbb56c9cc5f03d77426b171e16ccdd8",
    ),
    Package(
        name="reth-discv5",
        inventory_checksum="55974224f027af2dcbc5101f3cb1d1ccc5898f689528e9dc6170bbd26f33c16f",
        modified={
            PurePosixPath("Cargo.toml"): "833e730f0e582ad92f60397c61b0f2ec6538ab160b7f7ece4d1678b0950a956a",
            PurePosixPath("src/lib.rs"): "a7269521c1bc76b8e0dee1ecd5371cd723eb60aa7106762578c0bb504fcd274b",
        },
        patch_checksum="3b9fe645e1c8b1d5df37c131144b16929e1f34101bfa47f019265c29f876e120",
    ),
    Package(
        name="datafusion-sql",
        inventory_checksum="0cde1d552a958a7a7dca76c473334db21d0317313821fb8ba49bc8156ad34ec8",
        modified={
            PurePosixPath("src/select.rs"): "cfdb952619677d91827713cc67f233493f89a2dfc37c1fe8eabf50ed45140e77",
        },
        patch_checksum="4f8e501352ed8791fc0ba15f669768dedceb9ec763e9a4dd3efb2a4457ab3e42",
    ),
    Package(
        name="datafusion-optimizer",
        inventory_checksum="7b3b5eb3eba4fcb00d4173f7603cf4661a485930ac909071b822a8c3e718d1c2",
        modified={
            PurePosixPath("src/eliminate_filter.rs"): "6a50ecd0bd22c65fcc15ffa73dc722d7946c89f3ee858ae7361b027c0ad8890d",
            PurePosixPath("src/simplify_expressions/expr_simplifier.rs"): "4ae50f11e2ab1bd0b7ddb5d247765c8c1658d4fbc42c785bfcdb4955e820be2f",
            PurePosixPath("src/simplify_expressions/utils.rs"): "f2e01ae33f7324072dc31c96dfa085ea30aa9ad7744f2c76bcef93a262a793f3",
        },
        patch_checksum="f9de5b1ce224b5ee17f629cb2278a5bcb71556675904774a7ebcafcc2a3c6340",
    ),
    Package(
        name="reth-network",
        inventory_checksum="12c15956c224e85aca12c00b90c33b13254df65913695f4495a53169dbb43be0",
        modified={
            PurePosixPath("Cargo.toml"): "41475c2237ce54f86ee60f30c0ef672a741bc1079e394b32715d95e9b960a302",
            PurePosixPath("src/eth_requests.rs"): "c6af5374770f9429be3b81c663c2dbceb2dcee89fd1d8a424dc37c7996f91c79",
            PurePosixPath("src/session/active.rs"): "a4c0d8b50fbe7cb3895723d6f935abd1c13f44fcd8471463f7aad1c4dcf93534",
            PurePosixPath("src/session/mod.rs"): "fc8dc897438d2d4ce8f6bfd90455250110af1c036483fe23bb22e8758dac688b",
            PurePosixPath("src/session/types.rs"): "584a6bff5d489f973dfecd96d5f5334943f4403a348eba30a863b86053d3f8d1",
        },
        patch_checksum="ff61b88877851c5cb10382559c239f35dd607e280730378fac526d3842301d97",
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
    if package.provenance_note not in {
        PurePosixPath("LOGEX-PATCH.md"),
        PurePosixPath("LOGEX-PATCH.txt"),
    }:
        raise ValueError(f"{package.name} has an unsupported provenance note path")

    expected = read_inventory(package)
    for path, checksum in package.added.items():
        if (
            path.is_absolute()
            or ".." in path.parts
            or "\\" in path.as_posix()
            or path == PurePosixPath(".")
            or path in expected
            or path in ADDITIONS
            or path == package.provenance_note
        ):
            raise ValueError(f"{package.name} invalid added file path: {path}")
        validate_checksum(checksum, f"{package.name} reviewed added file {path}")
    metadata = (set(ADDITIONS) - {PurePosixPath("LOGEX-PATCH.md")}) | {
        package.provenance_note
    }
    permitted = set(expected) | metadata | set(package.added)
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
