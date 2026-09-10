//! Recovery metadata for sync work that may be re-fetched after restart.
//!
//! Active epochs leave catalog/state at the last durable checkpoint. Column
//! replacement must still preserve their committed prefixes. Publishing epochs
//! have durable columns and staged metadata; recovery finishes both renames.
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use serde::{Deserialize, Serialize};

use super::catalog::{SegmentDescriptor, StorageCatalogPaths};
use super::recovery::IngestRoute;
use crate::durability;

const VERSION: u32 = 1;
const MAX_JOURNAL_BYTES: u64 = 16 * 1024;
const MAX_METADATA_BYTES: u64 = 64 * 1024 * 1024;
const CATALOG_STAGE: &str = ".ingestion-catalog.next";
const STATE_STAGE: &str = ".ingestion-state.next";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IngestionJournal {
    version: u32,
    pub(super) route: IngestRoute,
    pub(super) start: Option<SegmentDescriptor>,
    pub(super) next_segment_id: u64,
    origin: Origin,
    publication: Option<Publication>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Origin {
    catalog: Fingerprint,
    state: Option<Fingerprint>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    catalog: Fingerprint,
    state: Fingerprint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Fingerprint {
    bytes: u64,
    checksum: u32,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedJournal {
    journal: IngestionJournal,
    checksum: u32,
}

impl IngestionJournal {
    pub(super) fn new(
        paths: &StorageCatalogPaths,
        route: IngestRoute,
        start: Option<SegmentDescriptor>,
        next_segment_id: u64,
    ) -> io::Result<Self> {
        let catalog = Fingerprint::read(&paths.catalog_path())?;
        let state = match Fingerprint::read(&paths.root().join("storage_state.json")) {
            Ok(state) => Some(state),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            version: VERSION,
            route,
            start,
            next_segment_id,
            origin: Origin { catalog, state },
            publication: None,
        })
    }

    pub(super) fn verify_origin(&self, paths: &StorageCatalogPaths) -> io::Result<()> {
        let catalog_matches = self.origin.catalog.matches(&paths.catalog_path())?;
        let state_matches = match (
            &self.origin.state,
            Fingerprint::read(&paths.root().join("storage_state.json")),
        ) {
            (Some(expected), Ok(actual)) => *expected == actual,
            (None, Err(error)) if error.kind() == io::ErrorKind::NotFound => true,
            (_, Err(error)) => return Err(error),
            _ => false,
        };
        if !catalog_matches || !state_matches {
            return Err(invalid(
                "ingestion recovery origin metadata changed; preserve recovery artifacts",
            ));
        }
        Ok(())
    }

    fn validate(&self) -> io::Result<()> {
        if self.version != VERSION
            || !self.origin.catalog.valid()
            || self
                .origin
                .state
                .as_ref()
                .is_some_and(|state| !state.valid())
            || self
                .start
                .as_ref()
                .is_some_and(|start| start.id >= self.next_segment_id)
            || self
                .publication
                .as_ref()
                .is_some_and(|p| !p.catalog.valid() || !p.state.valid())
        {
            return Err(invalid("invalid ingestion recovery journal"));
        }
        Ok(())
    }

    pub(super) fn includes(&self, id: u64) -> bool {
        self.start.as_ref().is_some_and(|start| start.id == id) || id >= self.next_segment_id
    }

    pub(super) fn path(paths: &StorageCatalogPaths) -> std::path::PathBuf {
        paths.root().join("wal/ingestion.json")
    }

    pub(super) fn persist(&self, paths: &StorageCatalogPaths) -> io::Result<()> {
        self.validate()?;
        let payload = serde_json::to_vec(self).map_err(io::Error::other)?;
        let bytes = serde_json::to_vec(&CheckedJournal {
            journal: self.clone(),
            checksum: crc32fast::hash(&payload),
        })
        .map_err(io::Error::other)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(invalid("ingestion recovery journal exceeds size limit"));
        }
        durability::write_bytes(&Self::path(paths), &bytes)
    }

    pub(super) fn load(paths: &StorageCatalogPaths) -> io::Result<Option<Self>> {
        let file = match File::open(Self::path(paths)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        file.take(MAX_JOURNAL_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(invalid("ingestion recovery journal exceeds size limit"));
        }
        let checked: CheckedJournal = serde_json::from_slice(&bytes).map_err(invalid)?;
        let payload = serde_json::to_vec(&checked.journal).map_err(io::Error::other)?;
        if crc32fast::hash(&payload) != checked.checksum {
            return Err(invalid("ingestion recovery journal checksum mismatch"));
        }
        checked.journal.validate()?;
        Ok(Some(checked.journal))
    }

    pub(super) fn publish(
        mut self,
        paths: &StorageCatalogPaths,
        catalog: &[u8],
        state: &[u8],
    ) -> io::Result<()> {
        durability::write_bytes_ordered(&paths.root().join(CATALOG_STAGE), catalog)?;
        durability::write_bytes_ordered(&paths.root().join(STATE_STAGE), state)?;
        // Ingestion ordered all segment artifacts before their manifests and
        // fully synchronized devices external to this catalog. Persist the
        // same-device columns and the staged metadata before deciding to commit.
        durability::sync_directory(paths.root())?;
        self.publication = Some(Publication {
            catalog: Fingerprint::of(catalog),
            state: Fingerprint::of(state),
        });
        self.persist(paths)?;
        durability::checkpoint("ingestion_commit_decided", paths.root())?;
        self.finish_publication(paths)?;
        Ok(())
    }

    /// Return false for an active epoch that still needs row rollback.
    pub(super) fn finish_publication(&self, paths: &StorageCatalogPaths) -> io::Result<bool> {
        let Some(publication) = &self.publication else {
            return Ok(false);
        };
        // Check BOTH inputs before replacing either destination. A partially
        // completed rename is accepted only when its destination matches exactly.
        let catalog_staged =
            locate_metadata(paths, CATALOG_STAGE, "catalog.json", &publication.catalog)?;
        let state_staged =
            locate_metadata(paths, STATE_STAGE, "storage_state.json", &publication.state)?;
        for (staged, source, destination) in [
            (catalog_staged, CATALOG_STAGE, "catalog.json"),
            (state_staged, STATE_STAGE, "storage_state.json"),
        ] {
            if staged {
                durability::checkpoint(
                    "ingestion_publish_metadata",
                    &paths.root().join(destination),
                )?;
                fs::rename(paths.root().join(source), paths.root().join(destination))?;
            }
        }
        // Persist both names before deleting the decision, including when wal/
        // is an alias on a different device.
        durability::sync_directory(paths.root())?;
        Self::remove(paths)?;
        Ok(true)
    }

    pub(super) fn remove(paths: &StorageCatalogPaths) -> io::Result<()> {
        for stage in [CATALOG_STAGE, STATE_STAGE] {
            match fs::remove_file(paths.root().join(stage)) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        durability::sync_directory(paths.root())?;
        durability::remove_file(&Self::path(paths))
    }
}

impl Fingerprint {
    fn of(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.len() as u64,
            checksum: crc32fast::hash(bytes),
        }
    }

    fn valid(&self) -> bool {
        self.bytes > 0 && self.bytes <= MAX_METADATA_BYTES
    }

    fn read(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let bytes = file.metadata()?.len();
        if bytes == 0 || bytes > MAX_METADATA_BYTES {
            return Err(invalid(format!(
                "invalid ingestion metadata size: {}",
                path.display()
            )));
        }
        let mut reader = file.take(bytes + 1);
        let mut checksum = crc32fast::Hasher::new();
        let mut buffer = [0; 64 * 1024];
        let mut total = 0;
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            total += read as u64;
            checksum.update(&buffer[..read]);
        }
        if total != bytes {
            return Err(invalid("ingestion metadata changed while reading"));
        }
        Ok(Self {
            bytes,
            checksum: checksum.finalize(),
        })
    }

    fn matches(&self, path: &Path) -> io::Result<bool> {
        Ok(*self == Self::read(path)?)
    }
}

fn locate_metadata(
    paths: &StorageCatalogPaths,
    stage: &str,
    destination: &str,
    expected: &Fingerprint,
) -> io::Result<bool> {
    match expected.matches(&paths.root().join(stage)) {
        Ok(true) => return Ok(true),
        Ok(false) => {
            return Err(invalid(format!(
                "corrupt staged ingestion metadata: {stage}"
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    if expected.matches(&paths.root().join(destination))? {
        return Ok(false);
    }
    Err(invalid(format!(
        "missing committed ingestion metadata: {destination}; preserve recovery artifacts"
    )))
}

fn invalid(message: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn origin() -> (tempfile::TempDir, StorageCatalogPaths, IngestionJournal) {
        let dir = tempfile::tempdir().unwrap();
        let paths = StorageCatalogPaths::new(dir.path().to_owned());
        paths.ensure_base_dirs().unwrap();
        fs::create_dir(paths.root().join("wal")).unwrap();
        fs::write(paths.catalog_path(), b"old catalog").unwrap();
        fs::write(paths.root().join("storage_state.json"), b"old state").unwrap();
        let journal = IngestionJournal::new(&paths, IngestRoute::Historical, None, 1).unwrap();
        journal.persist(&paths).unwrap();
        (dir, paths, journal)
    }

    #[test]
    fn committed_metadata_damage_is_detected_before_either_rename() {
        for damaged in [CATALOG_STAGE, STATE_STAGE] {
            let (_dir, paths, mut journal) = origin();
            fs::write(paths.root().join(CATALOG_STAGE), b"new catalog").unwrap();
            fs::write(paths.root().join(STATE_STAGE), b"new state").unwrap();
            journal.publication = Some(Publication {
                catalog: Fingerprint::of(b"new catalog"),
                state: Fingerprint::of(b"new state"),
            });
            journal.persist(&paths).unwrap();
            fs::write(paths.root().join(damaged), b"corrupt").unwrap();
            assert!(journal.finish_publication(&paths).is_err());
            assert_eq!(fs::read(paths.catalog_path()).unwrap(), b"old catalog");
            assert_eq!(
                fs::read(paths.root().join("storage_state.json")).unwrap(),
                b"old state"
            );
            assert!(IngestionJournal::path(&paths).exists());
        }
    }

    #[test]
    fn journal_rejects_unknown_fields_versions_checksums_and_oversize() {
        for damage in 0..4 {
            let (_dir, paths, _) = origin();
            let path = IngestionJournal::path(&paths);
            let mut value: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            match damage {
                0 => value["journal"]["version"] = 2.into(),
                1 => value["journal"]["unexpected"] = true.into(),
                2 => value["checksum"] = 0.into(),
                _ => {}
            }
            let bytes = if damage == 3 {
                vec![b' '; MAX_JOURNAL_BYTES as usize + 1]
            } else {
                serde_json::to_vec(&value).unwrap()
            };
            fs::write(&path, &bytes).unwrap();
            assert!(IngestionJournal::load(&paths).is_err());
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
    }
}
