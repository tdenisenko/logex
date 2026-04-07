use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use alloy_primitives::hex;
use reth_network_peers::NodeRecord;
use secp256k1::SecretKey;

const DISCOVERY_SECRET_FILE: &str = "discovery-secret";
const KNOWN_PEERS_FILE: &str = "known-peers.json";

pub fn discovery_secret_path(data_dir: &Path) -> PathBuf {
    data_dir.join(DISCOVERY_SECRET_FILE)
}

pub fn known_peers_path(data_dir: &Path) -> PathBuf {
    data_dir.join(KNOWN_PEERS_FILE)
}

pub fn load_or_create_secret_key(secret_key_path: &Path) -> io::Result<SecretKey> {
    match secret_key_path.try_exists() {
        Ok(true) => {
            let contents = fs::read_to_string(secret_key_path)?;
            let hex_key = contents.trim().trim_start_matches("0x");
            let bytes = hex::decode(hex_key).map_err(io::Error::other)?;
            SecretKey::from_slice(&bytes).map_err(io::Error::other)
        }
        Ok(false) => {
            if let Some(dir) = secret_key_path.parent() {
                fs::create_dir_all(dir)?;
            }

            let secret = SecretKey::new(&mut rand::thread_rng());
            fs::write(secret_key_path, hex::encode(secret.secret_bytes()))?;
            Ok(secret)
        }
        Err(err) => Err(err),
    }
}

pub fn load_known_peers(path: &Path) -> io::Result<Vec<NodeRecord>> {
    if !path.exists() {
        return Ok(Vec::new());
    }

    let contents = fs::read_to_string(path)?;
    serde_json::from_str(&contents).map_err(io::Error::other)
}

pub fn persist_known_peers(path: &Path, peers: &[NodeRecord]) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }

    let json = serde_json::to_vec_pretty(peers).map_err(io::Error::other)?;
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json)?;
    fs::rename(tmp, path)?;
    Ok(())
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
}
