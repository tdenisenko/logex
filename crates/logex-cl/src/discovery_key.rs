//! Shared discovery identity persistence. Parent directories must already exist.
//! Final-entry checks reject ordinary symlinks and nonregular files; this is not
//! a defense against adversarial concurrent path or parent-directory mutation.

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use alloy_primitives::hex;
use discv5::enr::CombinedKey;
use tempfile::NamedTempFile;

const MAX_ENCODED_KEY_BYTES: usize = 128;

/// Load a valid secp256k1 identity or durably publish a new one without replacing
/// an existing entry among cooperating LogEx writers. Invalid existing
/// contents are preserved and reported. A stable sidecar lock is never removed;
/// external writers that ignore it are outside this trusted-directory protocol.
///
/// New temporary files use tempfile's private creation permissions where the
/// filesystem enforces POSIX modes. Existing permissions are never changed.
/// The caller must initialize the parent directory first. Concurrent creation
/// returns `WouldBlock` so startup can retry without waiting indefinitely.
pub fn load_or_create_discovery_key(path: &Path) -> io::Result<[u8; 32]> {
    load_or_create_with(path, || {
        CombinedKey::generate_secp256k1()
            .encode()
            .try_into()
            .expect("secp256k1 secret keys contain 32 bytes")
    })
}

fn load_or_create_with(path: &Path, generate: impl FnOnce() -> [u8; 32]) -> io::Result<[u8; 32]> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    match fs::symlink_metadata(path) {
        Ok(_) => return read_and_sync(path, parent),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    // The stable sidecar serializes all current LogEx writers. It must never
    // be unlinked: a replacement inode would permit independent lock holders.
    let lock = acquire_creation_lock(path)?;
    match fs::symlink_metadata(path) {
        Ok(_) => return read_and_sync(path, parent),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    // No directory creation: a missing initialized storage directory is an error.
    let bytes = generate();
    validate_key(bytes)?;
    let mut temporary = NamedTempFile::new_in(parent)?;
    temporary.write_all(hex::encode(bytes).as_bytes())?;
    temporary.as_file().sync_all()?;
    // Preserve an observable external entry, though uncooperative concurrent
    // writers remain outside the protocol's serialization guarantee.
    match fs::symlink_metadata(path) {
        Ok(_) => return read_and_sync(path, parent),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    temporary.persist(path).map_err(|error| error.error)?;
    File::open(parent)?.sync_all()?;
    drop(lock);
    Ok(bytes)
}

fn lock_path(path: &Path) -> io::Result<PathBuf> {
    let mut name = path
        .file_name()
        .ok_or_else(|| invalid("discovery key path needs a filename"))?
        .to_os_string();
    name.push(".lock");
    Ok(path.with_file_name(name))
}

fn acquire_creation_lock(path: &Path) -> io::Result<File> {
    let path = lock_path(path)?;
    match fs::symlink_metadata(&path) {
        Ok(metadata) => validate_lock_entry(&metadata)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&path)?;
    validate_lock_entry(&file.metadata()?)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(TryLockError::WouldBlock) => Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "discovery identity creation is already in progress; retry startup after the other writer finishes",
        )),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

fn validate_lock_entry(metadata: &fs::Metadata) -> io::Result<()> {
    if !metadata.file_type().is_file() || metadata.len() != 0 {
        return Err(invalid(
            "discovery key lock must be an empty regular file, not a symlink or other entry",
        ));
    }
    Ok(())
}

fn read_and_sync(path: &Path, parent: &Path) -> io::Result<[u8; 32]> {
    if !fs::symlink_metadata(path)?.file_type().is_file() {
        return Err(invalid(
            "discovery key must be a regular file, not a symlink or other entry",
        ));
    }
    let file = File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(invalid("opened discovery key is not a regular file"));
    }
    let mut encoded = [0; MAX_ENCODED_KEY_BYTES + 1];
    let mut reader = file.take(encoded.len() as u64);
    let mut length = 0;
    while length < encoded.len() {
        match reader.read(&mut encoded[length..]) {
            Ok(0) => break,
            Ok(count) => length += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
    if length > MAX_ENCODED_KEY_BYTES {
        return Err(invalid("discovery key exceeds 128 encoded bytes"));
    }
    let encoded = std::str::from_utf8(&encoded[..length])
        .map_err(|_| invalid("discovery key must contain hexadecimal text"))?
        .trim();
    let encoded = encoded.strip_prefix("0x").unwrap_or(encoded);
    if encoded.len() != 64 {
        return Err(invalid(
            "discovery key must contain exactly 32 hexadecimal bytes",
        ));
    }
    let mut bytes = [0; 32];
    hex::decode_to_slice(encoded, &mut bytes)
        .map_err(|_| invalid("discovery key contains invalid hexadecimal text"))?;
    validate_key(bytes)?;
    // A reader may observe another creator's publication before that creator
    // syncs the directory. Sync on every successful read, including a concurrent publication.
    File::open(parent)?.sync_all()?;
    Ok(bytes)
}

fn validate_key(bytes: [u8; 32]) -> io::Result<()> {
    // CombinedKey's successful parser zeroizes its input. Validate a copy so
    // the actual identity returned to either discovery stack stays unchanged.
    let mut validation_copy = bytes;
    CombinedKey::secp256k1_from_bytes(&mut validation_copy)
        .map(|_| ())
        .map_err(|_| invalid("discovery key is not a valid secp256k1 secret"))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};
    use tempfile::TempDir;

    #[test]
    fn create_and_reopen_preserve_identity_and_leave_only_key_and_lock() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("key");
        let expected = [7; 32];
        assert_eq!(load_or_create_with(&path, || expected).unwrap(), expected);
        assert_eq!(
            load_or_create_with(&path, || panic!("must reopen")).unwrap(),
            expected
        );
        assert_eq!(fs::read(&path).unwrap(), hex::encode(expected).as_bytes());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
        let generated = directory.path().join("generated");
        let key = load_or_create_discovery_key(&generated).unwrap();
        validate_key(key).unwrap();
        assert_eq!(load_or_create_discovery_key(&generated).unwrap(), key);
    }

    #[test]
    fn four_simultaneous_creators_return_or_retry_the_same_winner() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("key");
        let barrier = Arc::new(Barrier::new(4));
        let handles: Vec<_> = (1..=4u8)
            .map(|fixture_byte| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    load_or_create_with(&path, || [fixture_byte; 32])
                })
            })
            .collect();
        let outcomes: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert!(outcomes.iter().any(Result::is_ok));
        let winner = load_or_create_discovery_key(&path).unwrap();
        for outcome in outcomes {
            match outcome {
                Ok(key) => assert_eq!(key, winner),
                Err(error) => {
                    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
                    assert_eq!(load_or_create_discovery_key(&path).unwrap(), winner);
                }
            }
        }
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 2);
    }

    #[test]
    fn held_lock_returns_busy_without_generation_or_publication() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("key");
        let held = acquire_creation_lock(&path).unwrap();
        let error = load_or_create_with(&path, || panic!("busy must not generate")).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert!(!path.exists());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        drop(held);
        assert_eq!(load_or_create_with(&path, || [9; 32]).unwrap(), [9; 32]);
    }

    #[test]
    fn invalid_lock_entries_are_preserved() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("key");
        let lock = lock_path(&path).unwrap();
        fs::write(&lock, b"ordinary nonempty fixture").unwrap();
        assert_eq!(
            load_or_create_discovery_key(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(&lock).unwrap(), b"ordinary nonempty fixture");
        assert!(!path.exists());
        fs::remove_file(&lock).unwrap();
        fs::create_dir(&lock).unwrap();
        assert_eq!(
            load_or_create_discovery_key(&path).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(lock.is_dir());
    }

    #[test]
    fn invalid_existing_files_remain_unchanged() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("key");
        for contents in [
            vec![],
            b"not hex".to_vec(),
            vec![b'0'; 64],
            vec![b'f'; 64],
            vec![b'1'; 129],
            format!("0x0x{}", "11".repeat(32)).into_bytes(),
        ] {
            fs::write(&path, &contents).unwrap();
            assert_eq!(
                load_or_create_with(&path, || panic!("must preserve existing"))
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
            assert_eq!(fs::read(&path).unwrap(), contents);
        }
        let expected = [17; 32];
        fs::write(&path, format!(" \n0x{}\t", hex::encode(expected))).unwrap();
        assert_eq!(load_or_create_discovery_key(&path).unwrap(), expected);
    }

    #[test]
    fn missing_parent_is_not_created_and_directory_entry_is_rejected() {
        let directory = TempDir::new().unwrap();
        let absent = directory.path().join("absent");
        assert_eq!(
            load_or_create_discovery_key(&absent.join("key"))
                .unwrap_err()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert!(!absent.exists());
        assert_eq!(
            load_or_create_discovery_key(directory.path())
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[cfg(unix)]
    #[test]
    fn sidecar_preserves_non_utf8_filename_bytes() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let directory = TempDir::new().unwrap();
        let path = directory.path().join(OsString::from_vec(vec![b'k', 0xff]));
        let expected_lock = directory.path().join(OsString::from_vec(vec![
            b'k', 0xff, b'.', b'l', b'o', b'c', b'k',
        ]));
        assert_eq!(lock_path(&path).unwrap(), expected_lock);
        // Filename encoding rules depend on the filesystem. Linux's test
        // filesystem accepts arbitrary non-NUL bytes; macOS may reject them.
        #[cfg(target_os = "linux")]
        {
            assert_eq!(load_or_create_with(&path, || [5; 32]).unwrap(), [5; 32]);
            assert!(expected_lock.is_file());
            assert_eq!(load_or_create_discovery_key(&path).unwrap(), [5; 32]);
        }
    }

    #[cfg(unix)]
    #[test]
    fn unix_private_creation_preserves_existing_modes_and_rejects_symlinks() {
        use std::os::unix::fs::{PermissionsExt, symlink};
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("key");
        load_or_create_with(&path, || [3; 32]).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        fs::set_permissions(&path, fs::Permissions::from_mode(0o640)).unwrap();
        load_or_create_discovery_key(&path).unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o640
        );
        let lock = lock_path(&path).unwrap();
        assert_eq!(
            fs::metadata(&lock).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let other = directory.path().join("other");
        symlink(&lock, lock_path(&other).unwrap()).unwrap();
        assert_eq!(
            load_or_create_discovery_key(&other).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert!(!other.exists());
        for (name, target) in [
            ("link", path.clone()),
            ("dangling", directory.path().join("missing")),
        ] {
            let link = directory.path().join(name);
            symlink(target, &link).unwrap();
            assert_eq!(
                load_or_create_discovery_key(&link).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert!(fs::symlink_metadata(link).unwrap().file_type().is_symlink());
        }
    }
}
