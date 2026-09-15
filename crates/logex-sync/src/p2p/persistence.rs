use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use reth_network_peers::NodeRecord;
use secp256k1::SecretKey;

const DISCOVERY_SECRET_FILE: &str = "discovery-secret";
pub(super) const MAX_PERSISTED_PEERS: usize = 512;
const MAX_KNOWN_PEERS_BYTES: usize = 1024 * 1024;

const KNOWN_PEERS_FILE: &str = "known-peers.json";

pub fn discovery_secret_path(data_dir: &Path) -> PathBuf {
    data_dir.join(DISCOVERY_SECRET_FILE)
}

pub fn known_peers_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KNOWN_PEERS_FILE)
}

/// Load the stable identity after the data-directory owner initializes its parent.
pub fn load_or_create_secret_key(secret_key_path: &Path) -> io::Result<SecretKey> {
    let bytes = logex_cl::load_or_create_discovery_key(secret_key_path)?;
    SecretKey::from_slice(&bytes).map_err(io::Error::other)
}

pub fn load_known_peers(path: &Path) -> io::Result<Vec<NodeRecord>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "known-peer cache must be a regular file",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error),
    }
    // Reject existing special entries before opening; the initialized directory
    // is trusted against concurrent path replacement by unrelated writers.
    let file = fs::File::open(path)?;
    if !file.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "opened known-peer cache is not a regular file",
        ));
    }
    let mut contents = Vec::new();
    file.take((MAX_KNOWN_PEERS_BYTES + 1) as u64)
        .read_to_end(&mut contents)?;
    let decoded = if contents.len() > MAX_KNOWN_PEERS_BYTES {
        Err("known-peer cache exceeds byte limit".to_owned())
    } else {
        let mut decoder = serde_json::Deserializer::from_slice(&contents);
        serde::de::Deserializer::deserialize_seq(&mut decoder, KnownPeersVisitor)
            .and_then(|peers| decoder.end().map(|()| peers))
            .map_err(|error| error.to_string())
    };
    match decoded {
        Ok(peers) => Ok(peers),
        Err(reason) => {
            let retained = quarantine_known_peers(path)?;
            tracing::warn!(path = %path.display(), quarantine = %retained.display(), %reason,
                "preserved damaged execution known-peer cache; continuing without cached peers");
            Ok(Vec::new())
        }
    }
}

struct KnownPeersVisitor;

impl<'de> serde::de::Visitor<'de> for KnownPeersVisitor {
    type Value = Vec<NodeRecord>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "at most {MAX_PERSISTED_PEERS} known-peer records"
        )
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut peers = Vec::new();
        while peers.len() < MAX_PERSISTED_PEERS {
            match seq.next_element::<NodeRecord>()? {
                Some(peer) => peers.push(peer),
                None => return Ok(peers),
            }
        }
        if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
            return Err(serde::de::Error::custom(
                "known-peer cache exceeds record limit",
            ));
        }
        Ok(peers)
    }
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn quarantine_known_peers(path: &Path) -> io::Result<PathBuf> {
    let directory = tempfile::Builder::new()
        .prefix(".known-peers-quarantine-")
        .tempdir_in(parent_directory(path))?;
    let retained = directory.path().join(KNOWN_PEERS_FILE);
    fs::rename(path, &retained)?;
    // Once moved, the damaged hints must outlive temporary-directory cleanup.
    let _ = directory.keep();
    Ok(retained)
}

pub fn persist_known_peers(path: &Path, peers: &[NodeRecord]) -> io::Result<()> {
    if peers.len() > MAX_PERSISTED_PEERS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "known-peer cache exceeds record limit",
        ));
    }
    let json = serde_json::to_vec_pretty(peers).map_err(io::Error::other)?;
    if json.len() > MAX_KNOWN_PEERS_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "known-peer cache exceeds byte limit",
        ));
    }
    let mut temporary = tempfile::Builder::new()
        .prefix(".known-peers-")
        .tempfile_in(parent_directory(path))?;
    temporary.write_all(&json)?;
    // Derived hints need atomic visibility, not periodic power-loss barriers.
    // The storage owner initializes the parent; never recreate missing storage.
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(())
}

pub fn persist_known_peers_if_changed(
    path: &Path,
    peers: &[NodeRecord],
    last_persisted: &mut Vec<NodeRecord>,
) -> io::Result<bool> {
    if last_persisted.as_slice() == peers {
        return Ok(false);
    }

    persist_known_peers(path, peers)?;
    *last_persisted = peers.to_vec();
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_network_peers::{NodeRecord, PeerId};
    use std::net::{IpAddr, Ipv4Addr};
    use tempfile::TempDir;

    fn test_node(port: u16, id_byte: u8) -> NodeRecord {
        NodeRecord::new_with_ports(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
            None,
            PeerId::from_slice(&[id_byte; 64]),
        )
    }

    #[test]
    fn secret_key_is_stable_after_first_write() {
        let tmp = TempDir::new().unwrap();
        let path = discovery_secret_path(tmp.path());

        let first = load_or_create_secret_key(&path).unwrap();
        let second = load_or_create_secret_key(&path).unwrap();

        assert_eq!(first.secret_bytes(), second.secret_bytes());
        assert_eq!(
            first.secret_bytes(),
            logex_cl::load_or_create_discovery_key(&path).unwrap()
        );
    }

    #[test]
    fn discovery_secret_does_not_recreate_missing_storage_parent() {
        let tmp = TempDir::new().unwrap();
        let absent = tmp.path().join("missing-storage");
        assert!(load_or_create_secret_key(&discovery_secret_path(&absent)).is_err());
        assert!(!absent.exists());
    }

    fn quarantine_contents(parent: &Path) -> Vec<Vec<u8>> {
        fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(".known-peers-quarantine-")
            })
            .map(|path| fs::read(path.join(KNOWN_PEERS_FILE)).unwrap())
            .collect()
    }

    #[test]
    fn damaged_hints_are_preserved_before_empty_recovery() {
        let tmp = TempDir::new().unwrap();
        let path = known_peers_path(tmp.path());
        let mut expected = Vec::new();
        for contents in [
            b"".to_vec(),
            b"[".to_vec(),
            b"[\"invalid ordinary node\"]".to_vec(),
            b"[] trailing".to_vec(),
        ] {
            fs::write(&path, &contents).unwrap();
            assert!(load_known_peers(&path).unwrap().is_empty());
            assert!(!path.exists());
            expected.push(contents);
        }
        let mut actual = quarantine_contents(tmp.path());
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected);
        assert!(load_known_peers(&path).unwrap().is_empty());
        assert_eq!(quarantine_contents(tmp.path()).len(), expected.len());
        persist_known_peers(&path, &[test_node(30303, 1)]).unwrap();
        assert_eq!(load_known_peers(&path).unwrap(), vec![test_node(30303, 1)]);
    }

    #[test]
    fn record_and_byte_limits_accept_boundaries_and_preserve_excess() {
        let tmp = TempDir::new().unwrap();
        let path = known_peers_path(tmp.path());
        let peers = vec![test_node(30303, 1); MAX_PERSISTED_PEERS];
        persist_known_peers(&path, &peers).unwrap();
        assert_eq!(load_known_peers(&path).unwrap(), peers);
        let original = fs::read(&path).unwrap();
        let excess = vec![test_node(30303, 1); MAX_PERSISTED_PEERS + 1];
        assert_eq!(
            persist_known_peers(&path, &excess).unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        assert_eq!(fs::read(&path).unwrap(), original);
        let encoded = serde_json::to_vec(&excess).unwrap();
        fs::write(&path, &encoded).unwrap();
        assert!(load_known_peers(&path).unwrap().is_empty());
        assert_eq!(quarantine_contents(tmp.path()), vec![encoded]);
        // Ordinary empty JSON with whitespace exercises the exact byte budget.
        let mut padded = b"[]".to_vec();
        padded.resize(MAX_KNOWN_PEERS_BYTES, b' ');
        fs::write(&path, &padded).unwrap();
        assert!(load_known_peers(&path).unwrap().is_empty());
        assert!(path.exists());
        padded.push(b' ');
        fs::write(&path, &padded).unwrap();
        assert!(load_known_peers(&path).unwrap().is_empty());
        assert!(quarantine_contents(tmp.path()).contains(&padded));
    }

    #[test]
    fn filesystem_errors_are_not_empty_recovery_and_missing_parent_is_not_created() {
        let tmp = TempDir::new().unwrap();
        let absent = tmp.path().join("missing");
        assert!(persist_known_peers(&absent.join(KNOWN_PEERS_FILE), &[]).is_err());
        assert!(!absent.exists());
        let parent_file = tmp.path().join("file");
        fs::write(&parent_file, b"ordinary fixture").unwrap();
        assert!(load_known_peers(&parent_file.join(KNOWN_PEERS_FILE)).is_err());
        assert!(load_known_peers(tmp.path()).is_err());
        assert!(quarantine_known_peers(&tmp.path().join("absent.json")).is_err());
        assert!(quarantine_contents(tmp.path()).is_empty());
        assert_eq!(fs::read(&parent_file).unwrap(), b"ordinary fixture");
    }

    #[test]
    fn failed_publication_preserves_marker_and_removes_temporary_file() {
        let tmp = TempDir::new().unwrap();
        let path = known_peers_path(tmp.path());
        fs::create_dir(&path).unwrap();
        fs::write(path.join("keep"), b"ordinary fixture").unwrap();
        let mut marker = vec![test_node(1, 1)];
        let before = marker.clone();
        assert!(persist_known_peers_if_changed(&path, &[test_node(2, 2)], &mut marker).is_err());
        assert_eq!(marker, before);
        assert_eq!(fs::read(path.join("keep")).unwrap(), b"ordinary fixture");
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn existing_links_are_preserved_and_rejected_without_empty_recovery() {
        use std::os::unix::fs::symlink;
        let tmp = TempDir::new().unwrap();
        let target = tmp.path().join("target");
        fs::write(&target, b"[]").unwrap();
        for (name, destination) in [
            ("link", target.clone()),
            ("dangling", tmp.path().join("absent")),
        ] {
            let path = tmp.path().join(name);
            symlink(destination, &path).unwrap();
            assert_eq!(
                load_known_peers(&path).unwrap_err().kind(),
                io::ErrorKind::InvalidData
            );
            assert!(fs::symlink_metadata(path).unwrap().file_type().is_symlink());
        }
        assert_eq!(fs::read(target).unwrap(), b"[]");
        assert!(quarantine_contents(tmp.path()).is_empty());
    }

    #[test]
    fn unique_staging_preserves_preexisting_legacy_temporary_name() {
        let tmp = TempDir::new().unwrap();
        let path = known_peers_path(tmp.path());
        let legacy = path.with_extension("json.tmp");
        fs::write(&legacy, b"ordinary preexisting temporary fixture").unwrap();
        persist_known_peers(&path, &[test_node(30303, 1)]).unwrap();
        assert_eq!(
            fs::read(&legacy).unwrap(),
            b"ordinary preexisting temporary fixture"
        );
        assert_eq!(load_known_peers(&path).unwrap(), vec![test_node(30303, 1)]);
    }

    #[test]
    fn concurrent_saves_publish_one_complete_cache_without_temporary_collisions() {
        use std::sync::{Arc, Barrier};
        let tmp = TempDir::new().unwrap();
        let path = known_peers_path(tmp.path());
        let barrier = Arc::new(Barrier::new(4));
        let handles: Vec<_> = (1..=4)
            .map(|index| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let peers = vec![test_node(30303, index); 3];
                    barrier.wait();
                    persist_known_peers(&path, &peers).unwrap();
                    peers
                })
            })
            .collect();
        let candidates: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();
        assert!(candidates.contains(&load_known_peers(&path).unwrap()));
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 1);
    }

    #[test]
    fn known_peers_round_trip() {
        let tmp = TempDir::new().unwrap();
        let path = known_peers_path(tmp.path());
        let peers = vec![test_node(30303, 1), test_node(30304, 2)];

        persist_known_peers(&path, &peers).unwrap();
        let loaded = load_known_peers(&path).unwrap();

        assert_eq!(loaded, peers);
    }

    #[test]
    fn known_peers_persist_only_when_changed() {
        let tmp = TempDir::new().unwrap();
        let path = known_peers_path(tmp.path());
        let peers = vec![test_node(30303, 1), test_node(30304, 2)];
        let mut last_persisted = Vec::new();

        assert!(persist_known_peers_if_changed(&path, &peers, &mut last_persisted).unwrap());
        assert_eq!(last_persisted, peers);
        assert!(!persist_known_peers_if_changed(&path, &peers, &mut last_persisted).unwrap());

        let loaded = load_known_peers(&path).unwrap();
        assert_eq!(loaded, peers);
    }
}
