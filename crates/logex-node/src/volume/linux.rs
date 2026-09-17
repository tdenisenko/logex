use super::*;
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;

#[derive(Debug, PartialEq, Eq)]
struct Mount {
    id: u64,
    major: u32,
    minor: u32,
    path: PathBuf,
    source: PathBuf,
}

#[cfg(target_os = "linux")]
pub(super) fn verify(directory: &File, mount: &Path, expected_uuid: &str) -> io::Result<()> {
    use std::fs;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{FileTypeExt, MetadataExt};
    // The kernel's descriptor mount ID selects the actual mount, including
    // stacked mounts and bind mounts. Longest pathname-prefix matching cannot.
    let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{}", directory.as_raw_fd()))?;
    let id = descriptor_mount_id(&fdinfo)?;
    let mounts = fs::read("/proc/self/mountinfo")?;
    let current = find_mount(&mounts, id)?;
    let device = directory.metadata()?.dev();
    if current.path != mount
        || current.major != libc::major(device)
        || current.minor != libc::minor(device)
    {
        return Err(invalid(
            "expected path is not the opened filesystem's current mount location",
        ));
    }
    // Match the filesystem source to the configured stable UUID. Device numbers
    // alone are not persistent identities and Btrfs can use anonymous st_dev.
    let source = fs::metadata(&current.source)
        .map_err(|error| io::Error::other(format!("cannot identify mount source {:?}: {error}; expected-volume mode requires a UUID-addressable block filesystem", current.source)))?;
    if !source.file_type().is_block_device() {
        return Err(invalid(
            "expected-volume mode requires a UUID-addressable block filesystem",
        ));
    }
    let mut matched = None;
    for entry in fs::read_dir("/dev/disk/by-uuid")? {
        let entry = entry?;
        if entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.eq_ignore_ascii_case(expected_uuid))
        {
            if matched.is_some() {
                return Err(invalid("volume UUID lookup is ambiguous"));
            }
            matched = Some(fs::metadata(entry.path())?);
        }
    }
    let matched = matched.ok_or_else(|| {
        invalid(format!(
            "expected volume UUID {expected_uuid} is not available in /dev/disk/by-uuid"
        ))
    })?;
    if !matched.file_type().is_block_device() || matched.rdev() != source.rdev() {
        return Err(invalid(format!(
            "mounted filesystem at {mount:?} does not match expected UUID {expected_uuid}"
        )));
    }
    Ok(())
}

fn descriptor_mount_id(contents: &str) -> io::Result<u64> {
    let mut ids = contents
        .lines()
        .filter_map(|line| line.strip_prefix("mnt_id:"));
    let id = ids
        .next()
        .ok_or_else(|| invalid("opened directory has no mount ID"))?
        .trim()
        .parse()
        .map_err(|_| invalid("invalid descriptor mount ID"))?;
    if ids.next().is_some() {
        return Err(invalid("duplicate descriptor mount ID"));
    }
    Ok(id)
}

fn find_mount(contents: &[u8], id: u64) -> io::Result<Mount> {
    let mut found = None;
    for line in contents
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let fields: Vec<_> = line.split(|byte| *byte == b' ').collect();
        let number = |bytes: &[u8]| -> io::Result<u64> {
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| text.parse().ok())
                .ok_or_else(|| invalid("invalid mountinfo number"))
        };
        let Some(first) = fields.first() else {
            continue;
        };
        if number(first)? != id {
            continue;
        }
        let separator = fields
            .iter()
            .position(|field| *field == b"-")
            .ok_or_else(|| invalid("mountinfo has no field separator"))?;
        if separator < 6 || fields.len() != separator + 4 {
            return Err(invalid("mountinfo has incomplete fields"));
        }
        let colon = fields[2]
            .iter()
            .position(|byte| *byte == b':')
            .ok_or_else(|| invalid("mountinfo has no device numbers"))?;
        let (major, minor) = (&fields[2][..colon], &fields[2][colon + 1..]);
        let mount = Mount {
            id,
            major: u32::try_from(number(major)?)
                .map_err(|_| invalid("mount device number exceeds u32"))?,
            minor: u32::try_from(number(minor)?)
                .map_err(|_| invalid("mount device number exceeds u32"))?,
            path: decode_path(fields[4])?,
            source: decode_path(fields[separator + 2])?,
        };
        if found.replace(mount).is_some() {
            return Err(invalid("duplicate mountinfo ID"));
        }
    }
    found.ok_or_else(|| {
        io::Error::other("opened directory's mount is no longer present in mountinfo")
    })
}

fn decode_path(bytes: &[u8]) -> io::Result<PathBuf> {
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\\' {
            let escape = bytes
                .get(index + 1..index + 4)
                .ok_or_else(|| invalid("incomplete mountinfo path escape"))?;
            let value = match escape {
                b"040" => b' ',
                b"011" => b'\t',
                b"012" => b'\n',
                b"134" => b'\\',
                _ => return Err(invalid("unsupported mountinfo path escape")),
            };
            decoded.push(value);
            index += 4;
        } else {
            if bytes[index] == 0 {
                return Err(invalid("mountinfo path contains NUL"));
            }
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Ok(PathBuf::from(OsString::from_vec(decoded)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_id_selects_actual_stacked_mount() {
        let contents = b"11 1 8:1 / /mnt/data rw - ext4 /dev/first rw\n12 1 8:2 / /mnt/data rw shared:4 - ext4 /dev/second rw\n";
        let mount = find_mount(contents, 12).unwrap();
        assert_eq!(mount.source, Path::new("/dev/second"));
        assert_eq!((mount.major, mount.minor), (8, 2));
        assert!(find_mount(contents, 13).is_err());
        assert_eq!(descriptor_mount_id("pos:\t0\nmnt_id:\t12\n").unwrap(), 12);
        for text in ["", "mnt_id: invalid", "mnt_id: 1\nmnt_id: 2"] {
            assert!(descriptor_mount_id(text).is_err());
        }
    }

    #[test]
    fn mount_paths_preserve_bytes_and_decode_kernel_escapes() {
        use std::os::unix::ffi::OsStrExt;
        assert_eq!(
            decode_path(b"/mnt/a\\040b\\134c\\011d\\012e\xff")
                .unwrap()
                .as_os_str()
                .as_bytes(),
            b"/mnt/a b\\c\td\ne\xff"
        );
        for bytes in [b"/a\\".as_slice(), b"/a\\0", b"/a\\000", b"/a\0"] {
            assert!(decode_path(bytes).is_err());
        }
    }

    #[test]
    fn incomplete_or_ambiguous_mount_records_fail() {
        for contents in [
            "1 0 8:1 / /mnt rw",
            "1 0 8:1 / /mnt rw - ext4 /dev/a",
            "1 0 4294967296:1 / /mnt rw - ext4 /dev/a rw",
            "1 0 8:1 / /mnt rw - ext4 /dev/a rw\n1 0 8:2 / /mnt rw - ext4 /dev/b rw",
        ] {
            assert!(find_mount(contents.as_bytes(), 1).is_err());
        }
    }
}
