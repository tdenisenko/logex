# Services using an external volume

These are templates, not an installed service. Set the real executable, config,
mount and log locations before installation. Production installation and live
sync are separate deployment steps.

When the dashboard is served through an HTTPS reverse proxy, configure
`http_allowed_origins` with the external browser origins, including scheme and
any non-default port. A nonempty list replaces direct HTTP origin matching;
forwarding headers are not trusted automatically. The same policy applies during
repair and normal sync. Keep the existing HTTP authentication and network
protection in place. Native clients without an `Origin` header remain supported.

Set both `expected_volume_mount` and `expected_volume_uuid` in the config. Their
CLI equivalents are `--expected-volume-mount` and `--expected-volume-uuid`;
explicit CLI values override the file. Both paths must be absolute, without
`..`, and the data directory must be a dedicated descendant of the mount.
Neither option is enabled by default. With them enabled, the executable verifies
the mounted filesystem UUID, available space and actual write access before
opening the database, on every service restart. It never substitutes a volume
label or a transient device number for the UUID.

On macOS, inspect the volume's filesystem UUID with `diskutil info` (or its plist
output). On Linux, use the filesystem UUID reported by `lsblk --fs` or `blkid`;
the configured UUID must resolve through `/dev/disk/by-uuid` to the mount's block
device. Linux sources without that identity, such as overlay/network filesystems,
are rejected. Do not give two different volumes the same filesystem UUID.

Initialize ownership so the service account can write the existing data
directory, or its existing parent when creation is needed. It need not own the
mount root when all descendant directories already exist. At least 10 GiB of
space must be available. Expected-volume mode rejects symlinks, nonregular
entries and nested filesystems in the data tree; use one dedicated filesystem
layout. Startup scans entry metadata to check that layout, without reading log
payloads. Other programs must not modify this directory while LogEx owns it.

After preflight, the process pins its working directory to the opened data
directory and uses relative database paths. Internal metadata staging preserves
that namespace too. This keeps a disappearing mount from redirecting database
writes to the system disk between health checks. The working directory remains
pinned through shutdown. Existing relative checkpoint descriptor paths are
resolved before entering the data directory.

Runtime checks run every 10 seconds, with one filesystem probe and a 10-second
deadline. They check the opened filesystem identity, configured paths, available
space and a tiny owned write/delete probe. They do not add periodic full-device
flushes or per-block barriers. A terminal failure closes query admission,
cancels outstanding query work and reports unavailable storage with its reason.
The existing whole-node shutdown deadline is 180 seconds. Individual blocked OS
filesystem calls cannot be canceled; the process deadline bounds their lifetime.
The independent monitor remains active through startup, runtime destruction and
offline maintenance. Once the node can serve requests, it notifies the supervisor
and closes query admission directly, so synchronous engine I/O cannot delay
detection. A terminal 180-second deadline is armed before notification or logging.
Before the supervisor is available, failure ends the command. Preflight itself
has a 180-second limit.

For systemd, customize `logex.service` and place the config at its `--config`
path. Run as the dedicated `logex` account. Keep journald storage on the system
disk, outside the removable volume. The unit retries after 30 seconds and allows
195 seconds for shutdown before service-manager enforcement.

For launchd, customize `org.logex.node.plist` for the account running the service.
Create the configured log directory with that account's ownership. Keep both log
files and the config on the system disk, outside the removable volume. The
template keeps the process running, throttles restarts to 30 seconds and allows
195 seconds for shutdown. It uses the standard process class, rather than the
more restrictive background class, and a private file-creation mask. Each new
process repeats the same executable preflight.

After a storage failure, restore the correct volume and permissions, ensure free
space, and restart. LogEx reopens its existing recovery state; it does not create
a substitute database elsewhere. A wrong or missing volume keeps failing
preflight until corrected. Offline inspection is available with `logex repair
--dry-run`; it verifies the expected existing volume without a write probe and
does not create a data directory. `logex repair` performs exclusive repair and
retains quarantine artifacts. Both commands use the configured data directory.

Automatic repair before sync is optional: set `repair_corrupt_segments = true`
in the service config. Each restart then uses the same verified repair coordinator
before starting ingestion and queries. Maintenance health/status report HTTP 503;
authentication remains in effect for detailed status and the dashboard. Repair
requires determinable ranges and retained trustworthy anchors when re-fetching
data; it never resets the database or replaces consensus trust to get past a
blocker. Omit checkpoint settings from a manual repair config. See the root
README and `logex repair --help` for exit codes and work allowances.
