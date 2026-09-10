use std::fs;
use std::io::{self, Read};

use crate::SyncHead;
use alloy_consensus::Header;
use std::path::{Path, PathBuf};

use crate::durability;
use logex_types::ChainAnchors;
use serde::{Deserialize, Serialize};

pub const STORAGE_FORMAT_VERSION: u32 = 1;
pub const CATALOG_FORMAT_VERSION: u32 = 2;
const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
const CATALOG_FILE: &str = "catalog.json";
const SEGMENTS_DIR: &str = "segments";

#[derive(Debug, Clone)]
pub struct NativeStorageConfig {
    pub data_dir: PathBuf,
    pub hot_target_rows: u64,
    pub compaction_safety_margin_blocks: u64,
}

impl Default for NativeStorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            hot_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageCatalogPaths {
    root: PathBuf,
}

impl StorageCatalogPaths {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.root.join(CATALOG_FILE)
    }

    pub fn segments_dir(&self) -> PathBuf {
        self.root.join(SEGMENTS_DIR)
    }

    pub fn segment_dir(&self, segment_id: u64) -> PathBuf {
        self.segments_dir().join(format!("s_{segment_id:016}"))
    }

    pub fn segment_manifest_path(&self, segment_id: u64) -> PathBuf {
        self.segment_dir(segment_id).join("segment.json")
    }

    pub fn ensure_base_dirs(&self) -> std::io::Result<()> {
        durability::create_dir_all(&self.root)?;
        durability::create_dir_all(&self.segments_dir())?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SegmentKind {
    Hot,
    Sealed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionCodec {
    None,
    Delta,
    DeltaZigZag,
    DeltaOfDelta,
    Dictionary,
    Zstd,
    Lz4,
    AdaptiveFixed,
    AdaptiveBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexKind {
    BlockNumber,
    BlockHash,
    Address,
    Topic0,
    Timestamp,
    AddressTopic0,
    AddressTopic0Topic1,
    AddressTopic0Topic2,
    Custom,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentDescriptor {
    pub id: u64,
    pub generation: u64,
    pub kind: SegmentKind,
    pub relative_path: PathBuf,
    pub manifest_relative_path: PathBuf,
    #[serde(default)]
    pub min_block: Option<u64>,
    #[serde(default)]
    pub max_block: Option<u64>,
    #[serde(default)]
    pub min_timestamp: Option<u64>,
    #[serde(default)]
    pub max_timestamp: Option<u64>,
    pub row_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentManifest {
    pub format_version: u32,
    pub segment_id: u64,
    pub generation: u64,
    pub kind: SegmentKind,
    #[serde(default)]
    pub min_block: Option<u64>,
    #[serde(default)]
    pub max_block: Option<u64>,
    #[serde(default)]
    pub min_timestamp: Option<u64>,
    #[serde(default)]
    pub max_timestamp: Option<u64>,
    pub row_count: u64,
    pub canonical_rows_path: String,
    pub columns: Vec<ColumnDescriptor>,
    pub indexes: Vec<IndexDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub name: String,
    pub codec: CompressionCodec,
    pub page_rows: u32,
    pub data_path: String,
    #[serde(default)]
    pub null_bitmap_path: Option<String>,
    #[serde(default)]
    pub page_index_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDescriptor {
    pub kind: IndexKind,
    pub name: String,
    pub data_path: String,
}

/// Canonical progress committed atomically with its segment positions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageState {
    pub sync_head: Option<SyncHead>,
    pub recent_headers: Vec<Header>,
    pub historical_floor_header: Option<Header>,
    pub historical_anchor_header: Option<Header>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedCatalog<'a> {
    format_version: u32,
    checksum: u32,
    #[serde(borrow)]
    catalog: &'a serde_json::value::RawValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeStorageCatalog {
    pub format_version: u32,
    pub state: StorageState,
    pub hot_target_rows: u64,
    pub next_segment_id: u64,
    #[serde(default)]
    pub active_hot_segment: Option<u64>,
    #[serde(default)]
    pub active_historical_segment: Option<u64>,
    #[serde(default)]
    pub anchors: ChainAnchors,
    pub segments: Vec<SegmentDescriptor>,
}

impl NativeStorageCatalog {
    pub fn open_or_create(
        config: &NativeStorageConfig,
    ) -> std::io::Result<(Self, StorageCatalogPaths)> {
        let paths = StorageCatalogPaths::new(config.data_dir.clone());
        paths.ensure_base_dirs()?;

        let catalog_path = paths.catalog_path();
        let existing = match fs::File::open(&catalog_path) {
            Ok(file) => Some(file),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                // A dangling alias is unavailable storage, not a new database.
                if artifact_exists(&catalog_path)? {
                    return Err(error);
                }
                None
            }
            Err(error) => return Err(error),
        };
        if let Some(file) = existing {
            let mut bytes = Vec::new();
            file.take(MAX_CATALOG_BYTES + 1).read_to_end(&mut bytes)?;
            let mut catalog = Self::decode(&bytes)?;
            if catalog.hot_target_rows != config.hot_target_rows {
                catalog.hot_target_rows = config.hot_target_rows;
                catalog.persist(&paths)?;
            }
            return Ok((catalog, paths));
        }
        if fs::read_dir(paths.segments_dir())?.next().is_some()
            || artifact_exists(&paths.root().join("storage_state.json"))?
            || artifact_exists(&paths.root().join("wal/recovery.json"))?
            || artifact_exists(&paths.root().join("wal/ingestion.json"))?
            || artifact_exists(&paths.root().join("wal/pending.wal"))?
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "missing catalog in an existing data directory; preserve it for verified repair, or start sync in a new directory",
            ));
        }

        let catalog = Self {
            format_version: CATALOG_FORMAT_VERSION,
            state: StorageState::default(),
            hot_target_rows: config.hot_target_rows,
            next_segment_id: 0,
            active_hot_segment: None,
            active_historical_segment: None,
            anchors: ChainAnchors::default(),
            segments: Vec::new(),
        };
        catalog.persist(&paths)?;
        Ok((catalog, paths))
    }

    pub fn persist(&self, paths: &StorageCatalogPaths) -> io::Result<()> {
        paths.ensure_base_dirs()?;
        durability::write_bytes(&paths.catalog_path(), &self.encode()?)
    }

    pub(super) fn encode(&self) -> io::Result<Vec<u8>> {
        self.validate()?;
        let payload = serde_json::to_vec(self).map_err(io::Error::other)?;
        if payload.len() as u64 > MAX_CATALOG_BYTES - 128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "catalog exceeds the 64 MiB format limit",
            ));
        }
        // Serialize the large header window once. The payload already is valid
        // JSON; the fixed wrapper contains only a version and integer checksum.
        let mut bytes = format!(
            "{{\"format_version\":{CATALOG_FORMAT_VERSION},\"checksum\":{},\"catalog\":",
            crc32fast::hash(&payload)
        )
        .into_bytes();
        bytes
            .try_reserve(payload.len() + 1)
            .map_err(io::Error::other)?;
        bytes.extend_from_slice(&payload);
        bytes.push(b'}');
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() as u64 > MAX_CATALOG_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "catalog exceeds the 64 MiB format limit",
            ));
        }
        let checked: CheckedCatalog<'_> = serde_json::from_slice(bytes).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, format!("invalid or unsupported catalog: {error}; catalog format 2 requires a new data directory")))?;
        if checked.format_version != CATALOG_FORMAT_VERSION
            || crc32fast::hash(checked.catalog.get().as_bytes()) != checked.checksum
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "catalog version or checksum mismatch; preserve the directory for verified repair",
            ));
        }
        let catalog: Self = serde_json::from_str(checked.catalog.get())
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        catalog.validate()?;
        Ok(catalog)
    }

    fn validate(&self) -> io::Result<()> {
        let mut ids = std::collections::BTreeSet::new();
        if self.format_version != CATALOG_FORMAT_VERSION || self.hot_target_rows == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid catalog version or segment target",
            ));
        }
        for segment in &self.segments {
            let relative = PathBuf::from(SEGMENTS_DIR).join(format!("s_{:016}", segment.id));
            if !ids.insert(segment.id)
                || segment.id >= self.next_segment_id
                || segment.relative_path != relative
                || segment.manifest_relative_path != relative.join("segment.json")
                || segment.row_count > u64::from(u32::MAX)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid catalog segment identity, path or row count",
                ));
            }
        }
        for (id, kind) in [
            (self.active_hot_segment, SegmentKind::Hot),
            (self.active_historical_segment, SegmentKind::Sealed),
        ] {
            if id.is_some_and(|id| {
                !self
                    .segments
                    .iter()
                    .any(|segment| segment.id == id && segment.kind == kind)
            }) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "invalid active catalog segment",
                ));
            }
        }
        if self.segments.iter().any(|segment| {
            segment.kind == SegmentKind::Hot && Some(segment.id) != self.active_hot_segment
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unowned hot catalog segment",
            ));
        }
        Ok(())
    }

    pub fn register_segment(&mut self, kind: SegmentKind) -> SegmentDescriptor {
        let descriptor = self.allocate_segment(kind);
        if kind == SegmentKind::Hot {
            self.active_hot_segment = Some(descriptor.id);
        }
        self.segments.push(descriptor.clone());
        descriptor
    }

    pub fn allocate_segment(&mut self, kind: SegmentKind) -> SegmentDescriptor {
        let id = self.next_segment_id;
        self.next_segment_id += 1;

        let relative_path = PathBuf::from(SEGMENTS_DIR).join(format!("s_{id:016}"));
        let manifest_relative_path = relative_path.join("segment.json");
        SegmentDescriptor {
            id,
            generation: 0,
            kind,
            relative_path,
            manifest_relative_path,
            min_block: None,
            max_block: None,
            min_timestamp: None,
            max_timestamp: None,
            row_count: 0,
        }
    }

    pub fn active_hot_segment(&self) -> Option<&SegmentDescriptor> {
        let hot_id = self.active_hot_segment?;
        self.segments.iter().find(|segment| segment.id == hot_id)
    }
}

fn artifact_exists(path: &Path) -> io::Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn catalog_bootstraps_and_reloads() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 1234,
            compaction_safety_margin_blocks: 2_048,
        };

        let (mut catalog, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
        assert_eq!(catalog.format_version, CATALOG_FORMAT_VERSION);
        assert_eq!(catalog.hot_target_rows, 1234);
        assert!(paths.catalog_path().exists());

        let hot = catalog.register_segment(SegmentKind::Hot);
        assert_eq!(catalog.active_hot_segment, Some(hot.id));
        catalog.persist(&paths).unwrap();

        let (reloaded, _) = NativeStorageCatalog::open_or_create(&config).unwrap();
        assert_eq!(reloaded.active_hot_segment, Some(hot.id));
        assert_eq!(reloaded.segments.len(), 1);
    }

    #[test]
    fn catalog_updates_hot_target_rows_from_config_on_reload() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_path_buf(),
            hot_target_rows: 100,
            compaction_safety_margin_blocks: 2_048,
        };
        let _ = NativeStorageCatalog::open_or_create(&config).unwrap();

        let updated_config = NativeStorageConfig {
            hot_target_rows: 1_000,
            ..config
        };
        let (reloaded, _) = NativeStorageCatalog::open_or_create(&updated_config).unwrap();

        assert_eq!(reloaded.hot_target_rows, 1_000);
    }

    #[test]
    fn storage_paths_are_stable() {
        let root = PathBuf::from("/tmp/logex-native");
        let paths = StorageCatalogPaths::new(root.clone());

        assert_eq!(paths.root(), root.as_path());
        assert_eq!(paths.catalog_path(), root.join(CATALOG_FILE));
        assert_eq!(paths.segments_dir(), root.join(SEGMENTS_DIR));
        assert_eq!(
            paths.segment_manifest_path(7),
            root.join(SEGMENTS_DIR)
                .join("s_0000000000000007/segment.json")
        );
    }

    #[test]
    fn checked_catalog_rejects_truncation_and_single_byte_damage() {
        let tmp = TempDir::new().unwrap();
        let (catalog, _) = NativeStorageCatalog::open_or_create(&NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        let bytes = catalog.encode().unwrap();
        assert_eq!(NativeStorageCatalog::decode(&bytes).unwrap(), catalog);
        for end in 0..bytes.len() {
            assert!(
                NativeStorageCatalog::decode(&bytes[..end]).is_err(),
                "truncated at {end}"
            );
        }
        for index in 0..bytes.len() {
            let mut damaged = bytes.clone();
            damaged[index] ^= 1;
            assert!(
                NativeStorageCatalog::decode(&damaged).is_err(),
                "damaged at {index}"
            );
        }
    }

    #[test]
    fn catalog_rejects_old_versions_and_invalid_identity_without_rewriting() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            ..Default::default()
        };
        let (catalog, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
        let mut legacy = serde_json::to_value(&catalog).unwrap();
        legacy["format_version"] = 1.into();
        legacy.as_object_mut().unwrap().remove("state");
        let bytes = serde_json::to_vec(&legacy).unwrap();
        fs::write(paths.catalog_path(), &bytes).unwrap();
        assert!(NativeStorageCatalog::open_or_create(&config).is_err());
        assert_eq!(fs::read(paths.catalog_path()).unwrap(), bytes);

        for damage in 0..5 {
            let mut damaged = catalog.clone();
            let hot = damaged.register_segment(SegmentKind::Hot);
            match damage {
                0 => damaged.segments.push(hot),
                1 => damaged.active_hot_segment = None,
                2 => damaged.segments[0].relative_path = PathBuf::from("../outside"),
                3 => damaged.next_segment_id = 0,
                4 => damaged.active_historical_segment = damaged.active_hot_segment,
                _ => unreachable!(),
            }
            assert!(damaged.encode().is_err(), "damage {damage}");
            // Even a correct checksum cannot make inconsistent metadata valid.
            let payload = serde_json::to_vec(&damaged).unwrap();
            let mut wrapper = format!(
                "{{\"format_version\":2,\"checksum\":{},\"catalog\":",
                crc32fast::hash(&payload)
            )
            .into_bytes();
            wrapper.extend_from_slice(&payload);
            wrapper.push(b'}');
            assert!(NativeStorageCatalog::decode(&wrapper).is_err());
        }
    }

    #[test]
    fn catalog_read_is_bounded_and_preserves_oversized_input() {
        let tmp = TempDir::new().unwrap();
        let config = NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            ..Default::default()
        };
        let (_, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
        let file = fs::OpenOptions::new()
            .write(true)
            .open(paths.catalog_path())
            .unwrap();
        file.set_len(MAX_CATALOG_BYTES + 1).unwrap();
        let error = NativeStorageCatalog::open_or_create(&config).err().unwrap();
        assert!(error.to_string().contains("64 MiB"));
        assert_eq!(file.metadata().unwrap().len(), MAX_CATALOG_BYTES + 1);
    }

    #[cfg(unix)]
    #[test]
    fn missing_catalog_alias_is_never_replaced_by_a_fresh_database() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("catalog.json");
        let target = tmp.path().join("unavailable/catalog.json");
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(
            NativeStorageCatalog::open_or_create(&NativeStorageConfig {
                data_dir: tmp.path().to_owned(),
                ..Default::default()
            })
            .is_err()
        );
        assert_eq!(fs::read_link(path).unwrap(), target);
        assert!(!target.exists());
    }
}
