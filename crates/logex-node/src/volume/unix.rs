use super::*;
use std::ffi::CString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

#[cfg(target_os = "linux")]
use super::linux as platform;
#[cfg(target_os = "macos")]
use super::macos as platform;

pub(super) fn prepare(
    mount: &Path,
    uuid: &str,
    data: &Path,
    minimum: u64,
) -> io::Result<ExpectedVolume> {
    prepare_with(mount, uuid, data, minimum, platform::verify)
}

fn prepare_with(
    mount: &Path,
    uuid: &str,
    data: &Path,
    minimum: u64,
    verify: fn(&File, &Path, &str) -> io::Result<()>,
) -> io::Result<ExpectedVolume> {
    let mount_path = absolute_normalized(mount)?;
    let data_path = absolute_normalized(data)?;
    let uuid = normalize_uuid(uuid)?;
    // Resolve only the mount's existing ancestors. Aliased data descendants are
    // refused by openat below. A mount must identify the actual mount location.
    let mount_path = fs::canonicalize(mount_path)?;
    let relative = data_path
        .strip_prefix(&mount_path)
        .map_err(|_| invalid("data directory must be beneath the expected mount"))?;
    if relative.as_os_str().is_empty() {
        return Err(invalid(
            "use a dedicated data directory beneath the expected mount",
        ));
    }
    let mount = open_directory(&mount_path)?;
    verify(&mount, &mount_path, &uuid)?;
    check_space(&mount, minimum)?;
    let device = mount.metadata()?.dev();
    let mut directory = mount.try_clone()?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(invalid(
                "data path must contain only normal descendants of the mount",
            ));
        };
        let name = CString::new(name.as_bytes()).map_err(|_| invalid("data path contains NUL"))?;
        let next = match open_child(&directory, &name) {
            Ok(next) => next,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // Test actual write capability on the verified ancestor before
                // creating any data-directory component. All later paths stay
                // relative to the pinned directory, never the mount pathname.
                enter(&directory)?;
                write_probe()?;
                verify(&mount, &mount_path, &uuid)?;
                // SAFETY: parent fd is live, name is a single NUL-terminated
                // component. mkdirat cannot follow a replacement parent path.
                if unsafe { libc::mkdirat(directory.as_raw_fd(), name.as_ptr(), 0o700) } != 0 {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::AlreadyExists {
                        return Err(error);
                    }
                }
                let next = open_child(&directory, &name)?;
                next.sync_all()?;
                directory.sync_all()?;
                next
            }
            Err(error) => return Err(error),
        };
        if next.metadata()?.dev() != device {
            return Err(invalid("data directory crosses a different filesystem"));
        }
        directory = next;
    }
    enter(&directory)?;
    // Check all existing entries before any database reader can follow aliases.
    // LogEx never creates symlinks or nested mounts in its data directory.
    verify_tree(device)?;
    write_probe()?;
    let guard = ExpectedVolume {
        mount,
        data: directory,
        mount_path,
        data_path,
        uuid,
    };
    check_with(&guard, minimum, verify)?;
    Ok(guard)
}

pub(super) fn check(volume: &ExpectedVolume, minimum: u64) -> io::Result<()> {
    check_with(volume, minimum, platform::verify)
}

fn check_with(
    volume: &ExpectedVolume,
    minimum: u64,
    verify: fn(&File, &Path, &str) -> io::Result<()>,
) -> io::Result<()> {
    verify(&volume.mount, &volume.mount_path, &volume.uuid)?;
    // The pathname must still name the same mounted directory. A different
    // filesystem or replacement directory is failure even if it is writable.
    let current_mount = open_directory(&volume.mount_path)?;
    same_entry(&volume.mount, &current_mount)?;
    let current_data = open_directory(&volume.data_path)?;
    same_entry(&volume.data, &current_data)?;
    let cwd = open_directory(Path::new("."))?;
    same_entry(&volume.data, &cwd)?;
    check_space(&volume.data, minimum)?;
    write_probe()
}

fn same_entry(expected: &File, actual: &File) -> io::Result<()> {
    let expected = expected.metadata()?;
    let actual = actual.metadata()?;
    if (expected.dev(), expected.ino()) != (actual.dev(), actual.ino()) {
        return Err(io::Error::other(
            "expected storage directory is no longer mounted at its configured path",
        ));
    }
    Ok(())
}

fn open_directory(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
}

fn open_child(parent: &File, name: &CString) -> io::Result<File> {
    // SAFETY: fd remains open, name is a NUL-terminated component. A successful
    // openat returns a new owned descriptor, transferred exactly once into File.
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fd is the fresh owned descriptor returned above.
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn enter(directory: &File) -> io::Result<()> {
    // SAFETY: directory owns a live directory fd. Main calls this before any
    // database workers exist and retains this working directory until exit.
    if unsafe { libc::fchdir(directory.as_raw_fd()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn verify_tree(device: u64) -> io::Result<()> {
    let mut pending = vec![PathBuf::from(".")];
    while let Some(path) = pending.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_symlink() || metadata.dev() != device {
                return Err(invalid(format!(
                    "expected-volume data contains an alias or another filesystem at {:?}",
                    entry.path()
                )));
            }
            if metadata.is_dir() {
                pending.push(entry.path());
            } else if !metadata.is_file() {
                return Err(invalid(format!(
                    "expected-volume data contains a nonregular entry at {:?}",
                    entry.path()
                )));
            }
        }
    }
    Ok(())
}

fn write_probe() -> io::Result<()> {
    let mut probe = logex_fs::StagedFile::new_in(Path::new("."), ".volume-probe-")?;
    probe.as_file_mut().write_all(b"\0")?;
    // Runtime probes test current write access without flushing other ingestion
    // writes. Actual database durability barriers remain with their owners.
    probe.discard()
}

fn check_space(directory: &File, minimum: u64) -> io::Result<()> {
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: directory fd is live and stat is aligned writable output storage.
    if unsafe { libc::fstatvfs(directory.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: successful fstatvfs initialized the structure.
    let stat = unsafe { stat.assume_init() };
    if stat.f_flag & libc::ST_RDONLY != 0 {
        return Err(io::Error::new(
            io::ErrorKind::ReadOnlyFilesystem,
            "expected volume is read-only",
        ));
    }
    let available = (stat.f_bavail as u128).saturating_mul(stat.f_frsize as u128);
    if available < u128::from(minimum) {
        return Err(io::Error::other(format!(
            "expected volume has {available} free bytes; at least {minimum} are required"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_identity(directory: &File, path: &Path, uuid: &str) -> io::Result<()> {
        if uuid != "abcd-1234" {
            return Err(invalid("fixture UUID differs"));
        }
        same_entry(directory, &open_directory(path)?)
    }

    #[test]
    fn preflight_cases_run_with_isolated_process_working_directories() {
        for case in [
            "create",
            "wrong_uuid",
            "missing_mount",
            "low_space",
            "alias",
            "tree_alias",
            "occupied",
            "replacement",
            "permission",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().canonicalize().unwrap();
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "volume::unix::tests::preflight_child",
                    "--nocapture",
                ])
                .env("LOGEX_VOLUME_TEST_ROOT", &root)
                .env("LOGEX_VOLUME_TEST_CASE", case)
                .current_dir(&root)
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "case {case}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    #[test]
    fn preflight_child() {
        let Some(root) = std::env::var_os("LOGEX_VOLUME_TEST_ROOT") else {
            return;
        };
        let root = PathBuf::from(root);
        let case = std::env::var("LOGEX_VOLUME_TEST_CASE").unwrap();
        let mount = root.join("volume");
        let data = mount.join("node/data");
        if case != "missing_mount" {
            fs::create_dir(&mount).unwrap();
        }
        let mut uuid = "abcd-1234";
        let mut minimum = 0;
        match case.as_str() {
            "wrong_uuid" => uuid = "1234-abcd",
            "low_space" => minimum = u64::MAX,
            "alias" => {
                fs::create_dir(root.join("other")).unwrap();
                std::os::unix::fs::symlink(root.join("other"), mount.join("node")).unwrap();
            }
            "tree_alias" => {
                fs::create_dir_all(&data).unwrap();
                fs::write(root.join("other"), b"retained").unwrap();
                std::os::unix::fs::symlink(root.join("other"), data.join("alias")).unwrap();
            }
            "occupied" => {
                fs::write(mount.join("node"), b"retained").unwrap();
            }
            _ => {}
        }
        let result = prepare_with(&mount, uuid, &data, minimum, fixture_identity);
        match case.as_str() {
            "create" | "replacement" | "permission" => {
                let guard = result.unwrap();
                check_with(&guard, minimum, fixture_identity).unwrap();
                assert_eq!(fs::read_dir(".").unwrap().count(), 0);
                if case == "replacement" {
                    fs::rename(&mount, root.join("retained-volume")).unwrap();
                    fs::create_dir_all(&data).unwrap();
                    fs::write(data.join("unrelated"), b"retained").unwrap();
                    assert!(check_with(&guard, minimum, fixture_identity).is_err());
                    let mut file = logex_fs::StagedFile::new_in(Path::new("."), ".owned-").unwrap();
                    file.as_file_mut().write_all(b"original namespace").unwrap();
                    file.persist(Path::new("owned")).unwrap();
                    assert_eq!(fs::read_dir(&data).unwrap().count(), 1);
                    assert_eq!(fs::read(data.join("unrelated")).unwrap(), b"retained");
                    assert_eq!(
                        fs::read(root.join("retained-volume/node/data/owned")).unwrap(),
                        b"original namespace"
                    );
                } else if case == "permission" {
                    use std::os::unix::fs::PermissionsExt;
                    // Root bypasses POSIX DAC; platform integration runs as an
                    // ordinary account for the permission-loss control.
                    // SAFETY: geteuid takes no pointers or preconditions.
                    if unsafe { libc::geteuid() } != 0 {
                        fs::set_permissions(".", fs::Permissions::from_mode(0o500)).unwrap();
                        let result = check_with(&guard, minimum, fixture_identity);
                        fs::set_permissions(".", fs::Permissions::from_mode(0o700)).unwrap();
                        assert!(result.is_err());
                    }
                }
            }
            _ => {
                assert!(result.is_err());
                if ["wrong_uuid", "missing_mount", "low_space", "occupied"].contains(&case.as_str())
                {
                    assert!(!data.exists());
                }
                if case == "tree_alias" {
                    assert_eq!(fs::read(root.join("other")).unwrap(), b"retained");
                }
                if case == "alias" {
                    assert_eq!(fs::read_dir(root.join("other")).unwrap().count(), 0);
                }
            }
        }
    }
}
