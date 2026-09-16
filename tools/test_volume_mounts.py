#!/usr/bin/env python3
"""Exercise volume protection using newly created Linux loopback fixtures only.

Requires a disposable Linux CI/staging host with udev and noninteractive sudo for
loopback attachment and mount operations. Filesystem formatting targets owned
image files, never a device. No service or existing data directory is opened.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import traceback
import uuid


def run(args, *, check=True, timeout=45):
    result = subprocess.run(args, capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"{args!r}: {result.stderr.decode(errors='replace')}")
    return result


def privileged(*args):
    return run(([] if os.geteuid() == 0 else ["sudo", "-n"]) + list(args))


def loop_devices(path):
    listing = privileged("losetup", "--list", "--json", "--output", "NAME,BACK-FILE")
    return [entry["name"] for entry in json.loads(listing.stdout)["loopdevices"]
            if entry.get("back-file") == str(path)]


class Image:
    def __init__(self, root, name, size):
        self.path = root / f"{name}.img"
        self.uuid = str(uuid.uuid4())
        self.device = None
        self.mount = None
        self.link = Path("/dev/disk/by-uuid") / self.uuid
        with self.path.open("xb") as image:
            image.truncate(size)
        # The exact owned regular file is the formatting target.
        assert self.path.parent == root and self.path.is_file()
        run(["mkfs.ext4", "-q", "-F", "-U", self.uuid, str(self.path)])

    def connect(self):
        self.device = privileged("losetup", "--find", "--show", str(self.path)).stdout.decode().strip()
        self.verify_device()
        # The device manager owns these links. Wait for its loop-device event;
        # never race it by creating, replacing or removing a UUID link ourselves.
        privileged("udevadm", "settle", "--timeout=15")
        actual = privileged("blkid", "-s", "UUID", "-o", "value", self.device).stdout.decode().strip()
        assert actual == self.uuid
        assert os.stat(self.link).st_rdev == os.stat(self.device).st_rdev

    def verify_device(self):
        assert self.device and self.device.startswith("/dev/loop")
        assert stat.S_ISBLK(os.stat(self.device).st_mode)
        assert loop_devices(self.path) == [self.device]

    def attach(self, mount):
        self.verify_device()
        mount.mkdir(exist_ok=True)
        assert not os.path.ismount(mount)
        self.mount = mount
        privileged("mount", self.device, str(mount))
        assert os.stat(mount).st_dev == os.stat(self.device).st_rdev

    def detach(self, *, lazy=False):
        self.verify_device()
        assert self.mount and os.path.ismount(self.mount)
        assert os.stat(self.mount).st_dev == os.stat(self.device).st_rdev
        privileged("umount", *(["-l"] if lazy else []), str(self.mount))
        assert not os.path.ismount(self.mount)
        self.mount = None

    def close(self):
        if self.mount:
            if os.path.ismount(self.mount):
                self.detach()
            else:
                self.mount = None
        # Reconcile actual kernel state, including a timed-out attach command.
        devices = loop_devices(self.path)
        if len(devices) > 1:
            raise RuntimeError("owned image unexpectedly has multiple loop attachments")
        self.device = devices[0] if devices else None
        if self.device:
            self.verify_device()
            privileged("losetup", "--detach", self.device)
            # Detach can request deferred autoclear. Never delete the backing
            # file until the kernel actually releases our exact image.
            deadline = time.monotonic() + 10
            while loop_devices(self.path):
                if time.monotonic() >= deadline:
                    raise RuntimeError("owned image remains attached after detach")
                time.sleep(0.1)
            self.device = None
            privileged("udevadm", "settle", "--timeout=15")
            if os.path.lexists(self.link):
                raise RuntimeError(f"device manager retained fixture UUID link {self.link}")


def response(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        if not selector.select(25):
            raise TimeoutError("fixture response exceeded 25 seconds")
    value = process.stdout.readline().decode().strip()
    if not value:
        process.wait(timeout=5)
        raise RuntimeError("fixture exited: " + process.stderr.read().decode(errors="replace"))
    return value


def send(process, command):
    process.stdin.write((command + "\n").encode())
    process.stdin.flush()
    return response(process)


def snapshot(path):
    return {str(p.relative_to(path)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in path.rglob("*") if p.is_file()}


def exercise(binary, root, results, images, children):
    mount = root / "mount"
    data = mount / "node/data"

    def rejected(volume_id, destination, label):
        result = run([str(binary), str(mount), volume_id, str(destination)], check=False, timeout=30)
        assert result.returncode != 0, label
        results.append({"case": label, "passed": True,
                        "diagnostic": result.stderr.decode(errors="replace")})

    rejected("abcd-1234", data, "missing mount")
    assert not mount.exists()
    data.mkdir(parents=True)
    (data / "probe-state").write_bytes(b"underlay original fixture")
    before = snapshot(mount)
    large = Image(root, "volume", 16 * 1024**3)
    images.append(large)
    small = Image(root, "small-volume", 512 * 1024**2)
    images.append(small)
    large.connect()
    small.connect()
    small.attach(mount)
    rejected(small.uuid, data, "low space")
    assert not (mount / "node").exists()
    small.detach()
    large.attach(mount)
    rejected("abcd-1234", mount / "wrong-node/data", "wrong UUID")
    assert not (mount / "wrong-node").exists()
    # An ordinary service account owns its existing parent, not the volume root.
    privileged("mkdir", str(mount / "node"))
    privileged("chown", f"{os.getuid()}:{os.getgid()}", str(mount / "node"))

    def start():
        process = subprocess.Popen([str(binary), str(mount), large.uuid, str(data)],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        children.append(process)
        assert response(process) == "ready"
        return process

    def finish(process):
        process.stdin.write(b"exit\n")
        process.stdin.flush()
        process.stdin.close()
        assert process.wait(timeout=10) == 0
        process.stdout.close()
        process.stderr.close()
        children.remove(process)

    process = start()
    assert send(process, "check") == "ok"
    assert send(process, "write") == "ok"
    assert (data / "probe-state").read_bytes() == b"owned volume fixture"
    results.append({"case": "real filesystem preflight and anchored write", "passed": True})
    large.detach(lazy=True)
    assert send(process, "check").startswith("error:")
    inflight = send(process, "write")
    assert snapshot(mount) == before
    results.append({"case": "lazy detach retains underlay contents", "passed": True,
                    "inflight_write": inflight})
    rejected(large.uuid, mount / "new-node/data", "unmounted path cannot initialize storage")
    assert not (mount / "new-node").exists()
    small.attach(mount)
    assert send(process, "check").startswith("error:")
    send(process, "write")
    assert not (mount / "node").exists()
    rejected(large.uuid, mount / "wrong-node/data", "different volume at same path")
    assert not (mount / "wrong-node").exists()
    results.append({"case": "replacement volume receives no old-owner writes", "passed": True})
    small.detach()
    assert snapshot(mount) == before
    finish(process)
    large.attach(mount)
    process = start()
    assert send(process, "check") == "ok"
    assert send(process, "write") == "ok"
    finish(process)
    results.append({"case": "correct volume remount and restart", "passed": True})
    large.detach()
    assert snapshot(mount) == before


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if not sys.platform.startswith("linux"):
        parser.error("this runner requires Linux; macOS fixtures use hdiutil")
    binary = args.binary.resolve(strict=True)
    root = Path(tempfile.mkdtemp(prefix="logex-volume-mounts-", dir="/tmp")).resolve()
    results, images, children, errors = [], [], [], []
    try:
        # Validate the unit without installing or starting a service. Substitute
        # only the executable so verification can check a real build artifact.
        unit = Path(__file__).resolve().parent.parent / "deploy/logex.service"
        rendered = root / "logex.service"
        rendered.write_text(unit.read_text().replace("/usr/local/bin/logex", str(binary)))
        verified = run(["systemd-analyze", "verify", "--man=no", "--generators=no",
                        "--recursive-errors=no", str(rendered)])
        results.append({"case": "systemd template validation", "passed": True,
                        "diagnostic": verified.stderr.decode(errors="replace")})
        exercise(binary, root, results, images, children)
    except BaseException as error:
        errors.append(repr(error))
    finally:
        for process in children:
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
            except BaseException as error:
                errors.append(f"owned child cleanup: {error!r}")
        for image in reversed(images):
            try:
                image.close()
            except BaseException as error:
                errors.append(f"cleanup {image.path.name}: {error!r}\n{traceback.format_exc()}")
        try:
            # Inspect actual attachments as well as our command bookkeeping.
            detached = (not os.path.ismount(root / "mount")
                        and all(not loop_devices(image.path) for image in images))
        except BaseException as error:
            errors.append(f"cannot verify detached fixtures: {error!r}")
            detached = False
        if detached:
            shutil.rmtree(root)
        record = {"results": results, "errors": errors, "fixtures_detached": detached,
                  "fixture_root_removed": not root.exists(),
                  "fixture_root": str(root),
                  "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest()}
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(record, indent=2) + "\n")
        print(json.dumps(record))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
