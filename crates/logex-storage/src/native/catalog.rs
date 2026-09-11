use std::fs;
use std::io::{self, Read};

use crate::SyncHead;
use alloy_consensus::Header;
use alloy_rlp::Decodable;
use std::path::{Path, PathBuf};

use crate::durability;
use logex_types::ChainAnchors;
use serde::{Deserialize, Serialize};

pub const STORAGE_FORMAT_VERSION: u32 = 8;
pub const CATALOG_FORMAT_VERSION: u32 = 10;
const CATALOG_MAGIC: &[u8; 8] = b"LXCAT010";
const CATALOG_PREFIX_BYTES: usize = 20;
const MAX_CACHED_HEADERS: usize = 8192;
const MAX_CACHED_HEADER_BYTES: usize = 16 * 1024;
const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
// Retain the legacy name so older binaries encounter invalid JSON and fail
// instead of treating the directory as empty and allocating replacement segments.
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_bundle: Option<crate::BundleReference>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column_bundle: Option<crate::BundleReference>,
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
    // Encoded as a bounded canonical RLP list after the catalog metadata.
    #[serde(skip)]
    pub recent_headers: Vec<Header>,
    pub historical_floor_header: Option<Header>,
    pub historical_anchor_header: Option<Header>,
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
            || artifact_exists(&paths.root().join("catalog.bin"))?
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
        let metadata = serde_json::to_vec(self).map_err(io::Error::other)?;
        encode_frame(&metadata, &self.state.recent_headers)
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() as u64 > MAX_CATALOG_BYTES {
            return Err(invalid_catalog("catalog exceeds the 64 MiB format limit"));
        }
        let (metadata_len, headers_len) = frame_lengths(bytes)?;
        let expected = CATALOG_PREFIX_BYTES + metadata_len + headers_len;
        if bytes.len() != expected
            || frame_checksum(bytes) != u32::from_le_bytes(bytes[16..20].try_into().unwrap())
        {
            return Err(invalid_catalog(
                "catalog length or checksum mismatch; preserve the directory for verified repair",
            ));
        }
        let split = CATALOG_PREFIX_BYTES + metadata_len;
        let mut catalog: Self = serde_json::from_slice(&bytes[CATALOG_PREFIX_BYTES..split])
            .map_err(|error| invalid_catalog(format!("invalid catalog metadata: {error}")))?;
        let mut encoded = &bytes[split..];
        let mut headers = alloy_rlp::Header::decode_bytes(&mut encoded, true)
            .map_err(|error| invalid_catalog(format!("invalid cached header list: {error}")))?;
        if !encoded.is_empty() {
            return Err(invalid_catalog("trailing data after cached header list"));
        }
        while !headers.is_empty() {
            if catalog.state.recent_headers.len() >= MAX_CACHED_HEADERS {
                return Err(invalid_catalog("cached header count exceeds 8192"));
            }
            // Decode each header in its own bounded slice. A malformed element
            // must not consume fields from the next header or allocate its length.
            let mut payload = headers;
            let frame = alloy_rlp::Header::decode(&mut payload).map_err(|error| {
                invalid_catalog(format!("invalid cached header frame: {error}"))
            })?;
            let length = headers.len() - payload.len() + frame.payload_length;
            if !frame.list || length > MAX_CACHED_HEADER_BYTES {
                return Err(invalid_catalog("invalid or oversized cached header frame"));
            }
            let (mut one, rest) = headers.split_at(length);
            let header = Header::decode(&mut one)
                .map_err(|error| invalid_catalog(format!("invalid cached header: {error}")))?;
            if !one.is_empty() {
                return Err(invalid_catalog("trailing data in cached header"));
            }
            catalog
                .state
                .recent_headers
                .try_reserve(1)
                .map_err(io::Error::other)?;
            catalog.state.recent_headers.push(header);
            headers = rest;
        }
        catalog.validate()?;
        Ok(catalog)
    }

    /// Read only the small metadata section for a disk-size cache hint. This
    /// deliberately skips the header window and its checksum, so it must never
    /// be used as trusted coverage, query or recovery state.
    pub fn active_hot_segment_hint(data_dir: &Path) -> io::Result<Option<u64>> {
        #[derive(Deserialize)]
        struct Hint {
            format_version: u32,
            active_hot_segment: Option<u64>,
        }
        let mut file = match fs::File::open(data_dir.join(CATALOG_FILE)) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut prefix = [0u8; CATALOG_PREFIX_BYTES];
        file.read_exact(&mut prefix)?;
        let (metadata_len, headers_len) = frame_lengths(&prefix)?;
        if file.metadata()?.len() != (CATALOG_PREFIX_BYTES + metadata_len + headers_len) as u64 {
            return Err(invalid_catalog("catalog length mismatch"));
        }
        let mut metadata = Vec::new();
        file.take(metadata_len as u64).read_to_end(&mut metadata)?;
        if metadata.len() != metadata_len {
            return Err(invalid_catalog("truncated catalog metadata"));
        }
        let hint: Hint = serde_json::from_slice(&metadata).map_err(io::Error::other)?;
        if hint.format_version != CATALOG_FORMAT_VERSION {
            return Err(invalid_catalog("unsupported catalog metadata version"));
        }
        Ok(hint.active_hot_segment)
    }

    fn validate(&self) -> io::Result<()> {
        let mut ids = std::collections::BTreeSet::new();
        if self.format_version != CATALOG_FORMAT_VERSION || self.hot_target_rows == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid catalog version or segment target",
            ));
        }
        validate_cached_headers(&self.state.recent_headers)?;
        for header in [
            self.state.historical_floor_header.as_ref(),
            self.state.historical_anchor_header.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_cached_headers(std::slice::from_ref(header))?;
        }
        for segment in &self.segments {
            if let Some(reference) = &segment.column_bundle {
                reference.end()?;
                if reference.row_count != segment.row_count {
                    return Err(invalid_catalog("invalid catalog bundle checkpoint"));
                }
            }
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

    pub fn register_segment(&mut self, kind: SegmentKind) -> io::Result<SegmentDescriptor> {
        let descriptor = self.allocate_segment(kind)?;
        if kind == SegmentKind::Hot {
            self.active_hot_segment = Some(descriptor.id);
        }
        self.segments.push(descriptor.clone());
        Ok(descriptor)
    }

    pub fn allocate_segment(&mut self, kind: SegmentKind) -> io::Result<SegmentDescriptor> {
        let id = self.next_segment_id;
        self.next_segment_id = id
            .checked_add(1)
            .ok_or_else(|| invalid_catalog("catalog segment identifiers are exhausted"))?;

        let relative_path = PathBuf::from(SEGMENTS_DIR).join(format!("s_{id:016}"));
        let manifest_relative_path = relative_path.join("segment.json");
        Ok(SegmentDescriptor {
            column_bundle: None,
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
        })
    }

    pub fn active_hot_segment(&self) -> Option<&SegmentDescriptor> {
        let hot_id = self.active_hot_segment?;
        self.segments.iter().find(|segment| segment.id == hot_id)
    }
}

/// Check storage encoding bounds and optional-field representability. Chain
/// authentication, fork validity and ancestry remain the verified caller's job.
pub(super) fn validate_cached_headers(headers: &[Header]) -> io::Result<()> {
    if headers.len() > MAX_CACHED_HEADERS {
        return Err(invalid_catalog("cached header count exceeds 8192"));
    }
    for header in headers {
        let fields = [
            header.base_fee_per_gas.is_some(),
            header.withdrawals_root.is_some(),
            header.blob_gas_used.is_some(),
            header.excess_blob_gas.is_some(),
            header.parent_beacon_block_root.is_some(),
            header.requests_hash.is_some(),
        ];
        if header.extra_data.len() > 32 || fields.windows(2).any(|pair| !pair[0] && pair[1]) {
            return Err(invalid_catalog(
                "cached header has oversized extra data or a gap in its RLP field sequence",
            ));
        }
    }
    Ok(())
}

fn invalid_catalog(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn frame_lengths(prefix: &[u8]) -> io::Result<(usize, usize)> {
    if prefix.len() < CATALOG_PREFIX_BYTES || &prefix[..8] != CATALOG_MAGIC {
        return Err(invalid_catalog(format!(
            "invalid or unsupported catalog; format {CATALOG_FORMAT_VERSION} requires a new data directory"
        )));
    }
    // The fixed prefix was checked before these exact-width conversions.
    let metadata = u32::from_le_bytes(prefix[8..12].try_into().unwrap()) as usize;
    let headers = u32::from_le_bytes(prefix[12..16].try_into().unwrap()) as usize;
    let length = (CATALOG_PREFIX_BYTES as u64) + metadata as u64 + headers as u64;
    if length > MAX_CATALOG_BYTES {
        return Err(invalid_catalog("catalog exceeds the 64 MiB format limit"));
    }
    Ok((metadata, headers))
}

fn frame_checksum(bytes: &[u8]) -> u32 {
    let mut hash = crc32fast::Hasher::new();
    hash.update(&bytes[..16]);
    hash.update(&bytes[CATALOG_PREFIX_BYTES..]);
    hash.finalize()
}

fn encode_frame(metadata: &[u8], headers: &[Header]) -> io::Result<Vec<u8>> {
    let headers_len = alloy_rlp::list_length(headers);
    let length = CATALOG_PREFIX_BYTES
        .checked_add(metadata.len())
        .and_then(|length| length.checked_add(headers_len))
        .filter(|length| *length as u64 <= MAX_CATALOG_BYTES)
        .ok_or_else(|| invalid_catalog("catalog exceeds the 64 MiB format limit"))?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(io::Error::other)?;
    bytes.extend_from_slice(CATALOG_MAGIC);
    bytes.extend_from_slice(
        &u32::try_from(metadata.len())
            .map_err(io::Error::other)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &u32::try_from(headers_len)
            .map_err(io::Error::other)?
            .to_le_bytes(),
    );
    bytes.extend_from_slice(&[0; 4]);
    bytes.extend_from_slice(metadata);
    alloy_rlp::encode_list(headers, &mut bytes);
    let checksum = frame_checksum(&bytes);
    bytes[16..20].copy_from_slice(&checksum.to_le_bytes());
    Ok(bytes)
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
        assert_eq!(paths.catalog_path(), tmp.path().join("catalog.json"));
        // Older readers must fail at their existing path, never create a new
        // catalog alongside this one. The suffix is retained for that guard.
        assert!(
            serde_json::from_slice::<serde_json::Value>(&fs::read(paths.catalog_path()).unwrap())
                .is_err()
        );

        let hot = catalog.register_segment(SegmentKind::Hot).unwrap();
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

        // Reject each earlier framed version with either its old magic or
        // today's magic and an old metadata version, even with a valid CRC.
        for version in 2..CATALOG_FORMAT_VERSION {
            for old_magic in [false, true] {
                let mut previous = catalog.clone();
                previous.format_version = version;
                let mut bytes = encode_frame(&serde_json::to_vec(&previous).unwrap(), &[]).unwrap();
                if old_magic {
                    bytes[..8].copy_from_slice(format!("LXCAT{version:03}").as_bytes());
                }
                let checksum = frame_checksum(&bytes);
                bytes[16..20].copy_from_slice(&checksum.to_le_bytes());
                fs::write(paths.catalog_path(), &bytes).unwrap();
                assert!(NativeStorageCatalog::open_or_create(&config).is_err());
                assert_eq!(fs::read(paths.catalog_path()).unwrap(), bytes);
            }
        }

        for damage in 0..5 {
            let mut damaged = catalog.clone();
            let hot = damaged.register_segment(SegmentKind::Hot).unwrap();
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
            let wrapper = encode_frame(&payload, &[]).unwrap();
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

    #[test]
    fn cached_header_rlp_preserves_supported_optional_field_sequences() {
        let dir = TempDir::new().unwrap();
        let (mut catalog, _) = NativeStorageCatalog::open_or_create(&NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        let mut header = Header::default();
        for version in 0..7 {
            match version {
                0 => {}
                1 => header.base_fee_per_gas = Some(u64::MAX),
                2 => header.withdrawals_root = Some(alloy_primitives::B256::repeat_byte(2)),
                3 => header.blob_gas_used = Some(u64::MAX),
                4 => header.excess_blob_gas = Some(u64::MAX),
                5 => header.parent_beacon_block_root = Some(alloy_primitives::B256::repeat_byte(5)),
                6 => header.requests_hash = Some(alloy_primitives::B256::repeat_byte(6)),
                _ => unreachable!(),
            }
            catalog.state.recent_headers.push(header.clone());
            assert_eq!(
                NativeStorageCatalog::decode(&catalog.encode().unwrap()).unwrap(),
                catalog
            );
        }
        let mut invalid = Header {
            withdrawals_root: Some(alloy_primitives::B256::ZERO),
            ..Default::default()
        };
        catalog.state.recent_headers = vec![invalid.clone()];
        assert!(catalog.encode().is_err());
        invalid.base_fee_per_gas = Some(1);
        invalid.extra_data = vec![0; 33].into();
        catalog.state.recent_headers = vec![invalid];
        assert!(catalog.encode().is_err());
    }

    #[test]
    fn cached_header_frames_bound_counts_lengths_and_trailing_data() {
        let dir = TempDir::new().unwrap();
        let (catalog, _) = NativeStorageCatalog::open_or_create(&NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        let metadata = serde_json::to_vec(&catalog).unwrap();
        let too_many = vec![Header::default(); MAX_CACHED_HEADERS + 1];
        let error = NativeStorageCatalog::decode(&encode_frame(&metadata, &too_many).unwrap())
            .err()
            .unwrap();
        assert!(error.to_string().contains("count exceeds"));
        let huge = Header {
            extra_data: vec![0; MAX_CACHED_HEADER_BYTES].into(),
            ..Default::default()
        };
        let error = NativeStorageCatalog::decode(&encode_frame(&metadata, &[huge]).unwrap())
            .err()
            .unwrap();
        assert!(error.to_string().contains("oversized cached header frame"));

        let mut bytes = catalog.encode().unwrap();
        bytes.push(0xc0);
        bytes[12..16].copy_from_slice(&2u32.to_le_bytes());
        let checksum = frame_checksum(&bytes);
        bytes[16..20].copy_from_slice(&checksum.to_le_bytes());
        assert!(
            NativeStorageCatalog::decode(&bytes)
                .unwrap_err()
                .to_string()
                .contains("trailing data")
        );

        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(
            NativeStorageCatalog::decode(&bytes)
                .unwrap_err()
                .to_string()
                .contains("64 MiB")
        );
    }

    #[test]
    fn segment_allocation_overflow_returns_an_error() {
        let dir = TempDir::new().unwrap();
        let (mut catalog, _) = NativeStorageCatalog::open_or_create(&NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        catalog.next_segment_id = u64::MAX;
        let before = catalog.clone();
        let error = catalog.allocate_segment(SegmentKind::Sealed).unwrap_err();
        assert!(error.to_string().contains("identifiers are exhausted"));
        assert_eq!(catalog, before);
        assert!(catalog.register_segment(SegmentKind::Hot).is_err());
        assert_eq!(catalog, before);
    }
}
