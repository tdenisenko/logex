use std::fs;
use std::path::{Path, PathBuf};

use logex_types::ChainAnchors;
use serde::{Deserialize, Serialize};

pub const STORAGE_FORMAT_VERSION: u32 = 1;
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
        fs::create_dir_all(&self.root)?;
        fs::create_dir_all(self.segments_dir())?;
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeStorageCatalog {
    pub format_version: u32,
    pub hot_target_rows: u64,
    pub next_segment_id: u64,
    #[serde(default)]
    pub active_hot_segment: Option<u64>,
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
        if catalog_path.exists() {
            let json = fs::read_to_string(&catalog_path)?;
            let mut catalog: Self = serde_json::from_str(&json).map_err(std::io::Error::other)?;
            if catalog.hot_target_rows != config.hot_target_rows {
                catalog.hot_target_rows = config.hot_target_rows;
                catalog.persist(&paths)?;
            }
            return Ok((catalog, paths));
        }

        let catalog = Self {
            format_version: STORAGE_FORMAT_VERSION,
            hot_target_rows: config.hot_target_rows,
            next_segment_id: 0,
            active_hot_segment: None,
            anchors: ChainAnchors::default(),
            segments: Vec::new(),
        };
        catalog.persist(&paths)?;
        Ok((catalog, paths))
    }

    pub fn persist(&self, paths: &StorageCatalogPaths) -> std::io::Result<()> {
        paths.ensure_base_dirs()?;
        let path = paths.catalog_path();
        let tmp = path.with_extension("json.tmp");
        let json = serde_json::to_vec(self).map_err(std::io::Error::other)?;
        fs::write(&tmp, json)?;
        fs::rename(tmp, path)?;
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
        assert_eq!(catalog.format_version, STORAGE_FORMAT_VERSION);
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
}
