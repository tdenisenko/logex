use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;

const STORAGE_METRICS_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Default)]
pub struct StorageMetrics {
    /// Logical file lengths, including indexes and metadata (legacy API field).
    pub storage_used_bytes: Option<u64>,
    pub storage_allocated_bytes: Option<u64>,
    pub storage_file_allocated_bytes: Option<u64>,
    pub storage_directory_allocated_bytes: Option<u64>,
    pub disk_free_bytes: Option<u64>,
    pub storage_write_free_bytes: Option<u64>,
    pub storage_write_path: Option<String>,
    pub storage_free_total_bytes: Option<u64>,
    pub storage_free_volumes: Vec<StorageVolumeMetrics>,
    pub storage_limiting_path: Option<String>,
    pub cpu_utilization_pct: Option<f64>,
    pub cpu_utilization_raw_pct: Option<f64>,
    pub cpu_logical_cores: Option<usize>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct StorageVolumeMetrics {
    pub path: String,
    pub free_bytes: u64,
}

#[derive(Debug, Default)]
pub struct CachedStorageMetrics {
    refreshed_at: Option<Instant>,
    process_cpu_time: Option<Duration>,
    refresh_in_progress: bool,
    size_cache: StorageSizeCache,
    metrics: StorageMetrics,
}

impl CachedStorageMetrics {
    fn is_fresh(&self) -> bool {
        self.refreshed_at
            .is_some_and(|refreshed_at| refreshed_at.elapsed() < STORAGE_METRICS_TTL)
    }

    fn metrics(&self) -> StorageMetrics {
        self.metrics.clone()
    }

    fn update(&mut self, metrics: StorageMetrics) {
        self.metrics = metrics;
        self.refreshed_at = Some(Instant::now());
    }
}

pub async fn load_or_refresh(
    cache: Arc<tokio::sync::Mutex<CachedStorageMetrics>>,
    data_dir: PathBuf,
) -> StorageMetrics {
    let (cached_metrics, size_cache) = {
        let mut guard = cache.lock().await;
        if guard.is_fresh() || guard.refresh_in_progress {
            return guard.metrics();
        }

        guard.refresh_in_progress = true;
        (guard.metrics(), guard.size_cache.clone())
    };

    tokio::spawn(async move {
        let (metrics, size_cache) =
            tokio::task::spawn_blocking(move || collect_storage_metrics(&data_dir, size_cache))
                .await
                .unwrap_or_default();

        let mut guard = cache.lock().await;
        update_cached_metrics(&mut guard, metrics, size_cache);
    });

    cached_metrics
}

#[cfg(test)]
pub async fn refresh_for_test(
    cache: Arc<tokio::sync::Mutex<CachedStorageMetrics>>,
    data_dir: PathBuf,
) -> StorageMetrics {
    let size_cache = cache.lock().await.size_cache.clone();
    let (metrics, size_cache) =
        tokio::task::spawn_blocking(move || collect_storage_metrics(&data_dir, size_cache))
            .await
            .unwrap_or_default();

    let mut guard = cache.lock().await;
    update_cached_metrics(&mut guard, metrics, size_cache)
}

fn update_cached_metrics(
    guard: &mut CachedStorageMetrics,
    mut metrics: StorageMetrics,
    size_cache: StorageSizeCache,
) -> StorageMetrics {
    let now = Instant::now();
    let process_cpu_time = process_cpu_time();
    let cpu_raw_pct = guard
        .refreshed_at
        .zip(guard.process_cpu_time)
        .zip(process_cpu_time)
        .and_then(|((refreshed_at, previous_cpu_time), current_cpu_time)| {
            let elapsed = now.saturating_duration_since(refreshed_at);
            let cpu_delta = current_cpu_time.checked_sub(previous_cpu_time)?;
            (elapsed.as_secs_f64() > 0.0)
                .then_some(cpu_delta.as_secs_f64() / elapsed.as_secs_f64() * 100.0)
        });
    metrics.cpu_utilization_raw_pct = cpu_raw_pct;
    metrics.cpu_logical_cores = logical_cpu_count();
    metrics.cpu_utilization_pct =
        normalized_cpu_utilization_pct(cpu_raw_pct, metrics.cpu_logical_cores);
    guard.process_cpu_time = process_cpu_time;
    guard.refresh_in_progress = false;
    guard.size_cache = size_cache;
    guard.update(metrics.clone());
    metrics
}

#[cfg(test)]
pub async fn refresh_in_progress_for_test(
    cache: Arc<tokio::sync::Mutex<CachedStorageMetrics>>,
) -> bool {
    cache.lock().await.refresh_in_progress
}

#[cfg(test)]
pub async fn expire_for_test(cache: Arc<tokio::sync::Mutex<CachedStorageMetrics>>) {
    let mut guard = cache.lock().await;
    if let Some(refreshed_at) = guard.refreshed_at {
        guard.refreshed_at = Some(refreshed_at - STORAGE_METRICS_TTL - Duration::from_secs(1));
    }
}

fn collect_storage_metrics(
    data_dir: &Path,
    mut size_cache: StorageSizeCache,
) -> (StorageMetrics, StorageSizeCache) {
    let usage = dir_usage(data_dir, &mut size_cache).ok();
    let storage_write_path = data_dir
        .canonicalize()
        .unwrap_or_else(|_| data_dir.to_path_buf());
    let storage_write_free_bytes = free_space_bytes(&storage_write_path).ok();
    let storage_free_volumes = storage_free_volumes(data_dir);
    let disk_free_bytes = storage_write_free_bytes;
    let storage_limiting_path = storage_free_volumes
        .iter()
        .min_by_key(|volume| volume.free_bytes)
        .map(|volume| volume.path.clone());
    let storage_free_total_bytes = (!storage_free_volumes.is_empty()).then_some(
        storage_free_volumes
            .iter()
            .map(|volume| volume.free_bytes)
            .fold(0_u64, u64::saturating_add),
    );
    (
        StorageMetrics {
            storage_used_bytes: usage.map(|usage| usage.logical_bytes),
            storage_allocated_bytes: usage.and_then(StorageUsage::allocated_bytes),
            storage_file_allocated_bytes: usage.and_then(|usage| usage.file_allocated_bytes),
            storage_directory_allocated_bytes: usage
                .and_then(|usage| usage.directory_allocated_bytes),
            disk_free_bytes,
            storage_write_free_bytes,
            storage_write_path: Some(storage_write_path.display().to_string()),
            storage_free_total_bytes,
            storage_free_volumes,
            storage_limiting_path,
            cpu_utilization_pct: None,
            cpu_utilization_raw_pct: None,
            cpu_logical_cores: logical_cpu_count(),
        },
        size_cache,
    )
}

fn normalized_cpu_utilization_pct(
    raw_pct: Option<f64>,
    logical_cores: Option<usize>,
) -> Option<f64> {
    let raw_pct = raw_pct?;
    let logical_cores = logical_cores?.max(1) as f64;
    Some(raw_pct / logical_cores)
}

fn logical_cpu_count() -> Option<usize> {
    std::thread::available_parallelism().ok().map(usize::from)
}

#[cfg(unix)]
fn process_cpu_time() -> Option<Duration> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return None;
    }

    let usage = unsafe { usage.assume_init() };
    Some(timeval_duration(usage.ru_utime)? + timeval_duration(usage.ru_stime)?)
}

#[cfg(unix)]
fn timeval_duration(value: libc::timeval) -> Option<Duration> {
    let secs = u64::try_from(value.tv_sec).ok()?;
    let micros = u32::try_from(value.tv_usec).ok()?;
    Some(Duration::new(secs, micros.saturating_mul(1_000)))
}

#[cfg(not(unix))]
fn process_cpu_time() -> Option<Duration> {
    None
}

#[derive(Clone, Debug, Default)]
struct StorageSizeCache {
    segment_dirs: HashMap<SizeCacheKey, CachedDirSize>,
}

#[derive(Clone, Debug)]
struct CachedDirSize {
    signature: SegmentDirSignature,
    // Keep identities, not just a subtotal: aliases can cross cached/uncached
    // subtrees, and directory iteration order is unspecified.
    entries: Arc<[UsageEntry]>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SegmentDirSignature {
    directory: PathMetadataSignature,
    manifest: PathMetadataSignature,
    indexes_directory: Option<PathMetadataSignature>,
    index_files: Vec<(std::ffi::OsString, PathMetadataSignature)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct PathMetadataSignature {
    modified: Option<SystemTime>,
    len: u64,
    allocated_bytes: Option<u64>,
    id: Option<MetadataId>,
}

impl From<&fs::Metadata> for PathMetadataSignature {
    fn from(metadata: &fs::Metadata) -> Self {
        Self {
            modified: metadata.modified().ok(),
            len: metadata.len(),
            allocated_bytes: allocated_bytes(metadata),
            id: metadata_id(metadata),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SizeCacheKey {
    Metadata(MetadataId),
    Path(PathBuf),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct StorageUsage {
    logical_bytes: u64,
    file_allocated_bytes: Option<u64>,
    directory_allocated_bytes: Option<u64>,
}

impl Default for StorageUsage {
    fn default() -> Self {
        Self {
            logical_bytes: 0,
            file_allocated_bytes: cfg!(unix).then_some(0),
            directory_allocated_bytes: cfg!(unix).then_some(0),
        }
    }
}

impl StorageUsage {
    fn allocated_bytes(self) -> Option<u64> {
        self.file_allocated_bytes?
            .checked_add(self.directory_allocated_bytes?)
    }

    fn add(&mut self, other: Self) -> io::Result<()> {
        self.logical_bytes = self
            .logical_bytes
            .checked_add(other.logical_bytes)
            .ok_or_else(|| io::Error::other("logical storage usage overflow"))?;
        self.file_allocated_bytes = self
            .file_allocated_bytes
            .zip(other.file_allocated_bytes)
            .and_then(|(a, b)| a.checked_add(b));
        self.directory_allocated_bytes = self
            .directory_allocated_bytes
            .zip(other.directory_allocated_bytes)
            .and_then(|(a, b)| a.checked_add(b));
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct UsageEntry {
    key: SizeCacheKey,
    usage: StorageUsage,
}

impl UsageEntry {
    fn new(path: &Path, metadata: &fs::Metadata) -> Self {
        let mut usage = StorageUsage::default();
        if metadata.is_file() {
            usage.logical_bytes = metadata.len();
            usage.file_allocated_bytes = allocated_bytes(metadata);
        } else if metadata.is_dir() {
            usage.directory_allocated_bytes = allocated_bytes(metadata);
        }
        Self {
            key: size_cache_key(path, metadata),
            usage,
        }
    }

    fn account(
        &self,
        visited: &mut HashSet<SizeCacheKey>,
        total: &mut StorageUsage,
    ) -> io::Result<()> {
        if visited.insert(self.key.clone()) {
            total.add(self.usage)?;
        }
        Ok(())
    }
}

#[cfg(unix)]
fn allocated_bytes(metadata: &fs::Metadata) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    // st_blocks uses 512-byte units even when the filesystem allocation unit
    // is larger. File lengths and directory lengths are not allocated sizes.
    metadata.blocks().checked_mul(512)
}

#[cfg(not(unix))]
fn allocated_bytes(_metadata: &fs::Metadata) -> Option<u64> {
    None
}

#[cfg(test)]
fn dir_size_bytes(root: &Path, cache: &mut StorageSizeCache) -> io::Result<u64> {
    Ok(dir_usage(root, cache)?.logical_bytes)
}

fn dir_usage(root: &Path, cache: &mut StorageSizeCache) -> io::Result<StorageUsage> {
    // Missing/unreadable storage is unknown, never a measured zero.
    let root_metadata = fs::metadata(root)?;
    if !root_metadata.is_dir() {
        return Err(io::Error::other("storage root is not a directory"));
    }
    let segments_dir = root.join("segments");
    let (active_hot_segment, cache_segments) = match active_hot_segment_path(root) {
        Ok(active_hot_segment) => (active_hot_segment, true),
        Err(_) => (None, false),
    };
    let mut walk = DirSizeWalk {
        segments_dir: &segments_dir,
        active_hot_segment: active_hot_segment.as_deref(),
        cache_segments,
        seen_cached_segments: HashSet::new(),
        visited: HashSet::new(),
        total: StorageUsage::default(),
    };
    walk.visit(root, cache)?;
    cache
        .segment_dirs
        .retain(|key, _| walk.seen_cached_segments.contains(key));
    Ok(walk.total)
}

struct DirSizeWalk<'a> {
    segments_dir: &'a Path,
    active_hot_segment: Option<&'a Path>,
    cache_segments: bool,
    seen_cached_segments: HashSet<SizeCacheKey>,
    visited: HashSet<SizeCacheKey>,
    total: StorageUsage,
}

impl DirSizeWalk<'_> {
    fn visit(&mut self, path: &Path, cache: &mut StorageSizeCache) -> io::Result<()> {
        let metadata = fs::metadata(path)?;
        let entry = UsageEntry::new(path, &metadata);
        if self.visited.contains(&entry.key) {
            return Ok(());
        }
        let cacheable_segment = if self.cache_segments && metadata.is_dir() {
            cacheable_segment_dir(path, self.segments_dir, self.active_hot_segment, &metadata)?
        } else {
            None
        };
        if let Some((key, signature)) = cacheable_segment {
            self.seen_cached_segments.insert(key.clone());
            if let Some(cached) = cache.segment_dirs.get(&key)
                && cached.signature == signature
            {
                for entry in cached.entries.iter() {
                    entry.account(&mut self.visited, &mut self.total)?;
                }
                return Ok(());
            }
            // Inventory independently of the outer visited set; otherwise a
            // first walk can cache a partial subtotal when aliases come first.
            let mut entries = Vec::new();
            let mut local_visited = HashSet::new();
            let cache_safe =
                collect_usage_entries(path, &metadata, &mut entries, &mut local_visited)?;
            for entry in &entries {
                entry.account(&mut self.visited, &mut self.total)?;
            }
            // A nested symlink may point at mutable data outside the sealed
            // segment. A manifest signature cannot validate that target.
            if cache_safe
                && segment_dir_signature(path, &fs::metadata(path)?)? == Some(signature.clone())
            {
                cache.segment_dirs.insert(
                    key,
                    CachedDirSize {
                        signature,
                        entries: entries.into(),
                    },
                );
            } else {
                cache.segment_dirs.remove(&key);
            }
            return Ok(());
        }
        entry.account(&mut self.visited, &mut self.total)?;
        if metadata.is_dir() {
            for entry in fs::read_dir(path)? {
                self.visit(&entry?.path(), cache)?;
            }
        }
        Ok(())
    }
}

fn collect_usage_entries(
    path: &Path,
    metadata: &fs::Metadata,
    entries: &mut Vec<UsageEntry>,
    visited: &mut HashSet<SizeCacheKey>,
) -> io::Result<bool> {
    let entry = UsageEntry::new(path, metadata);
    if !visited.insert(entry.key.clone()) {
        return Ok(true);
    }
    entries.push(entry);
    let mut cache_safe = true;
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            cache_safe &= !entry.file_type()?.is_symlink();
            let path = entry.path();
            cache_safe &= collect_usage_entries(&path, &fs::metadata(&path)?, entries, visited)?;
        }
    }
    Ok(cache_safe)
}

fn active_hot_segment_path(root: &Path) -> io::Result<Option<PathBuf>> {
    Ok(
        logex_storage::native::NativeStorageCatalog::active_hot_segment_hint(root)?
            .map(|id| root.join("segments").join(format!("s_{id:016}"))),
    )
}

fn cacheable_segment_dir(
    path: &Path,
    segments_dir: &Path,
    active_hot_segment: Option<&Path>,
    metadata: &fs::Metadata,
) -> io::Result<Option<(SizeCacheKey, SegmentDirSignature)>> {
    if path.parent() != Some(segments_dir)
        || path.file_name().and_then(parse_segment_dir_name).is_none()
        || active_hot_segment == Some(path)
    {
        return Ok(None);
    }

    let Some(signature) = segment_dir_signature(path, metadata)? else {
        return Ok(None);
    };

    Ok(Some((size_cache_key(path, metadata), signature)))
}

fn parse_segment_dir_name(name: &std::ffi::OsStr) -> Option<u64> {
    let name = name.to_str()?;
    let id = name.strip_prefix("s_")?;
    (id.len() == 16).then(|| id.parse::<u64>().ok())?
}

fn segment_dir_signature(
    path: &Path,
    metadata: &fs::Metadata,
) -> io::Result<Option<SegmentDirSignature>> {
    let manifest_metadata = match fs::metadata(path.join("segment.json")) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    let indexes_dir = path.join("indexes");
    let indexes_metadata = match fs::metadata(&indexes_dir) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error),
    };
    let mut index_files = Vec::new();
    if indexes_metadata.is_some() {
        for entry in fs::read_dir(&indexes_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                // Nonstandard nested or linked indexes have no cheap stable
                // signature; measure them without caching the segment.
                return Ok(None);
            }
            index_files.push((
                entry.file_name(),
                PathMetadataSignature::from(&entry.metadata()?),
            ));
        }
        index_files.sort_by(|left, right| left.0.cmp(&right.0));
    }
    Ok(Some(SegmentDirSignature {
        directory: metadata.into(),
        manifest: (&manifest_metadata).into(),
        indexes_directory: indexes_metadata.as_ref().map(PathMetadataSignature::from),
        index_files,
    }))
}

fn size_cache_key(path: &Path, metadata: &fs::Metadata) -> SizeCacheKey {
    metadata_id(metadata)
        .map(SizeCacheKey::Metadata)
        .unwrap_or_else(|| {
            SizeCacheKey::Path(path.canonicalize().unwrap_or_else(|_| path.to_path_buf()))
        })
}

#[cfg(unix)]
type MetadataId = (u64, u64);

#[cfg(unix)]
fn metadata_id(metadata: &fs::Metadata) -> Option<MetadataId> {
    use std::os::unix::fs::MetadataExt;

    Some((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
type MetadataId = ();

#[cfg(not(unix))]
fn metadata_id(_metadata: &fs::Metadata) -> Option<MetadataId> {
    None
}

fn storage_free_space_probe_paths(data_dir: &Path) -> Vec<PathBuf> {
    let mut probes = BTreeSet::new();
    insert_storage_free_space_probe(&mut probes, data_dir.to_path_buf());
    let segments_dir = data_dir.join("segments");
    if segments_dir.exists() {
        insert_storage_free_space_probe(&mut probes, segments_dir.clone());
    }

    if let Ok(entries) = fs::read_dir(&segments_dir) {
        for entry in entries.flatten() {
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if !file_type.is_symlink() {
                continue;
            }

            let Ok(target) = fs::read_link(entry.path()) else {
                continue;
            };
            let target = if target.is_absolute() {
                target
            } else {
                segments_dir.join(target)
            };
            let probe = target.parent().map(Path::to_path_buf).unwrap_or(target);
            insert_storage_free_space_probe(&mut probes, probe);
        }
    }

    probes.into_iter().collect()
}

fn insert_storage_free_space_probe(probes: &mut BTreeSet<PathBuf>, path: PathBuf) {
    probes.insert(path.canonicalize().unwrap_or(path));
}

fn storage_free_volumes(data_dir: &Path) -> Vec<StorageVolumeMetrics> {
    let mut volumes = BTreeMap::<StorageVolumeKey, StorageVolumeMetrics>::new();
    for path in storage_free_space_probe_paths(data_dir) {
        let Ok(free_bytes) = free_space_bytes(&path) else {
            continue;
        };
        let key = storage_volume_key(&path);
        volumes
            .entry(key)
            .and_modify(|volume| {
                let candidate_path = path.display().to_string();
                if candidate_path.len() < volume.path.len() {
                    volume.path = candidate_path;
                }
                volume.free_bytes = volume.free_bytes.min(free_bytes);
            })
            .or_insert_with(|| StorageVolumeMetrics {
                path: path.display().to_string(),
                free_bytes,
            });
    }
    volumes.into_values().collect()
}

#[cfg(unix)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum StorageVolumeKey {
    Device(u64),
    Path(PathBuf),
}

#[cfg(unix)]
fn storage_volume_key(path: &Path) -> StorageVolumeKey {
    use std::os::unix::fs::MetadataExt;

    fs::metadata(path)
        .map(|metadata| StorageVolumeKey::Device(metadata.dev()))
        .unwrap_or_else(|_| StorageVolumeKey::Path(path.to_path_buf()))
}

#[cfg(not(unix))]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum StorageVolumeKey {
    Path(PathBuf),
}

#[cfg(not(unix))]
fn storage_volume_key(path: &Path) -> StorageVolumeKey {
    StorageVolumeKey::Path(path.to_path_buf())
}

#[cfg(unix)]
fn free_space_bytes(path: &Path) -> io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL byte"))?;

    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(io::Error::last_os_error());
    }

    let stat = unsafe { stat.assume_init() };
    let free = (stat.f_bavail as u128).saturating_mul(stat.f_frsize as u128);
    Ok(free.min(u64::MAX as u128) as u64)
}

#[cfg(not(unix))]
fn free_space_bytes(_path: &Path) -> io::Result<u64> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "free-space reporting is not implemented on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    #[test]
    fn dir_size_counts_nested_files() {
        let tmp = TempDir::new().expect("tempdir");
        let nested = tmp.path().join("nested");
        fs::create_dir_all(&nested).expect("nested dir");

        let mut top = fs::File::create(tmp.path().join("top.bin")).expect("top file");
        let mut inner = fs::File::create(nested.join("inner.bin")).expect("inner file");
        top.write_all(&[0_u8; 7]).expect("write top");
        inner.write_all(&[0_u8; 11]).expect("write inner");

        let mut cache = StorageSizeCache::default();
        assert_eq!(dir_size_bytes(tmp.path(), &mut cache).expect("size"), 18);
    }

    #[cfg(unix)]
    #[test]
    fn dir_size_follows_symlinked_directories_once() {
        use std::os::unix::fs::symlink;

        let tmp = TempDir::new().expect("tempdir");
        let external = TempDir::new().expect("external tempdir");
        let segments = tmp.path().join("segments");
        fs::create_dir_all(&segments).expect("segments dir");

        fs::write(tmp.path().join("root.bin"), [0_u8; 7]).expect("root file");
        fs::write(external.path().join("moved.bin"), [0_u8; 11]).expect("moved file");
        let first = segments.join("s_0000000000000001");
        let second = segments.join("s_0000000000000002");
        symlink(external.path(), first).expect("first symlink");
        symlink(external.path(), second).expect("second symlink");

        let mut cache = StorageSizeCache::default();
        assert_eq!(dir_size_bytes(tmp.path(), &mut cache).expect("size"), 18);
    }

    #[cfg(unix)]
    #[test]
    fn free_space_probe_paths_include_segment_symlink_targets() {
        use std::os::unix::fs::symlink;

        let tmp = TempDir::new().expect("tempdir");
        let external = TempDir::new().expect("external tempdir");
        let segments = tmp.path().join("segments");
        let target_segment = external.path().join("segments").join("s_0000000000000001");
        fs::create_dir_all(&segments).expect("segments dir");
        fs::create_dir_all(&target_segment).expect("target segment");
        symlink(&target_segment, segments.join("s_0000000000000001")).expect("segment symlink");

        let probes = storage_free_space_probe_paths(tmp.path());

        assert!(probes.contains(&tmp.path().canonicalize().unwrap()));
        assert!(probes.contains(&segments.canonicalize().unwrap()));
        assert!(probes.contains(&target_segment.parent().unwrap().canonicalize().unwrap()));
    }

    #[test]
    fn collect_metrics_reports_storage_usage() {
        let tmp = TempDir::new().expect("tempdir");
        fs::write(tmp.path().join("rows.bin"), [1_u8, 2, 3, 4]).expect("rows file");

        let (metrics, _) = collect_storage_metrics(tmp.path(), StorageSizeCache::default());
        assert_eq!(metrics.storage_used_bytes, Some(4));
        #[cfg(unix)]
        {
            const FREE_SPACE_TEST_TOLERANCE_BYTES: u64 = 16 * 1024 * 1024;

            let disk_free = metrics.disk_free_bytes.unwrap_or(0);
            let total_free = metrics.storage_free_total_bytes.unwrap_or(0);
            let volume_free = metrics.storage_free_volumes[0].free_bytes;

            assert!(metrics.disk_free_bytes.unwrap_or(0) > 0);
            assert!(metrics.storage_write_free_bytes.unwrap_or(0) > 0);
            assert!(metrics.storage_write_path.as_deref().is_some_and(|path| {
                path == tmp.path().canonicalize().unwrap().display().to_string()
            }));
            assert_eq!(metrics.storage_free_volumes.len(), 1);
            assert!(total_free.abs_diff(disk_free) <= FREE_SPACE_TEST_TOLERANCE_BYTES);
            assert_eq!(metrics.storage_limiting_path, metrics.storage_write_path);
            assert!(volume_free.abs_diff(disk_free) <= FREE_SPACE_TEST_TOLERANCE_BYTES);
        }
    }

    #[test]
    fn cpu_utilization_is_normalized_by_logical_core_capacity() {
        assert_eq!(
            normalized_cpu_utilization_pct(Some(800.0), Some(8)),
            Some(100.0)
        );
        assert_eq!(
            normalized_cpu_utilization_pct(Some(400.0), Some(8)),
            Some(50.0)
        );
        assert_eq!(
            normalized_cpu_utilization_pct(Some(880.0), Some(8)),
            Some(110.0)
        );
        assert_eq!(normalized_cpu_utilization_pct(None, Some(8)), None);
        assert_eq!(normalized_cpu_utilization_pct(Some(100.0), None), None);
    }

    #[test]
    fn segment_size_cache_refreshes_active_hot_segment() {
        let tmp = TempDir::new().expect("tempdir");
        write_active_hot_catalog(tmp.path(), 1);
        let segment = create_segment(tmp.path(), 1, 4);
        let mut cache = StorageSizeCache::default();

        let first = dir_size_bytes(tmp.path(), &mut cache).expect("first size");
        fs::OpenOptions::new()
            .append(true)
            .open(segment.join("rows.bin"))
            .expect("rows file")
            .write_all(&[0_u8; 5])
            .expect("append");

        let second = dir_size_bytes(tmp.path(), &mut cache).expect("second size");
        assert_eq!(second, first + 5);
    }

    #[test]
    fn segment_size_cache_refreshes_when_manifest_changes() {
        let tmp = TempDir::new().expect("tempdir");
        write_active_hot_catalog(tmp.path(), 9);
        let segment = create_segment(tmp.path(), 1, 4);
        let mut cache = StorageSizeCache::default();

        let first = dir_size_bytes(tmp.path(), &mut cache).expect("first size");
        fs::write(segment.join("compacted.bin"), [0_u8; 7]).expect("compacted file");
        fs::write(segment.join("segment.json"), br#"{"generation":1}"#).expect("manifest");

        let second = dir_size_bytes(tmp.path(), &mut cache).expect("second size");
        assert_eq!(second, first + 7);
    }

    #[test]
    fn segment_size_cache_refreshes_when_index_files_change() {
        let tmp = TempDir::new().expect("tempdir");
        write_active_hot_catalog(tmp.path(), 9);
        let segment = create_segment(tmp.path(), 1, 4);
        let index_dir = segment.join("indexes");
        fs::create_dir_all(&index_dir).expect("indexes dir");
        let mut cache = StorageSizeCache::default();

        let first = dir_size_bytes(tmp.path(), &mut cache).expect("first size");
        fs::write(index_dir.join("address.bptree"), [0_u8; 7]).expect("index file");

        let second = dir_size_bytes(tmp.path(), &mut cache).expect("second size");
        assert_eq!(second, first + 7);
    }

    #[tokio::test]
    async fn load_or_refresh_returns_stale_metrics_while_refresh_runs() {
        let tmp = TempDir::new().expect("tempdir");
        let mut file = fs::File::create(tmp.path().join("data.bin")).expect("file");
        file.write_all(&[0_u8; 9]).expect("write");
        drop(file);

        let cache = Arc::new(tokio::sync::Mutex::new(CachedStorageMetrics::default()));
        let empty = load_or_refresh(Arc::clone(&cache), tmp.path().to_path_buf()).await;
        assert_eq!(empty.storage_used_bytes, None);
        assert!(refresh_in_progress_for_test(Arc::clone(&cache)).await);

        for _ in 0..50 {
            if !refresh_in_progress_for_test(Arc::clone(&cache)).await {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let refreshed = cache.lock().await.metrics();
        assert_eq!(refreshed.storage_used_bytes, Some(9));

        expire_for_test(Arc::clone(&cache)).await;
        let stale = load_or_refresh(Arc::clone(&cache), tmp.path().to_path_buf()).await;
        assert_eq!(stale.storage_used_bytes, Some(9));
        assert!(refresh_in_progress_for_test(cache).await);
    }

    #[cfg(unix)]
    #[test]
    fn storage_usage_counts_sparse_allocation_and_directories() {
        use std::os::unix::fs::MetadataExt;
        let tmp = TempDir::new().unwrap();
        let nested = tmp.path().join("nested");
        fs::create_dir(&nested).unwrap();
        let sparse = nested.join("sparse.bin");
        let file = fs::File::create(&sparse).unwrap();
        file.set_len(8 * 1024 * 1024).unwrap();
        file.sync_all().unwrap();
        let file_blocks = fs::metadata(&sparse).unwrap().blocks() * 512;
        let directory_blocks = [tmp.path(), nested.as_path()]
            .into_iter()
            .map(|p| fs::metadata(p).unwrap().blocks() * 512)
            .sum::<u64>();
        let (metrics, _) = collect_storage_metrics(tmp.path(), StorageSizeCache::default());
        assert_eq!(metrics.storage_used_bytes, Some(8 * 1024 * 1024));
        assert_eq!(metrics.storage_file_allocated_bytes, Some(file_blocks));
        assert_eq!(
            metrics.storage_directory_allocated_bytes,
            Some(directory_blocks)
        );
        assert_eq!(
            metrics.storage_allocated_bytes,
            Some(file_blocks + directory_blocks)
        );
        assert!(file_blocks < metrics.storage_used_bytes.unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn cached_segments_deduplicate_hard_links_across_warm_cold_and_uncached_paths() {
        use std::os::unix::fs::MetadataExt;
        let tmp = TempDir::new().unwrap();
        let first = create_segment(tmp.path(), 1, 4);
        let second = create_segment(tmp.path(), 2, 0);
        fs::remove_file(second.join("rows.bin")).unwrap();
        fs::hard_link(first.join("rows.bin"), second.join("rows.bin")).unwrap();
        let mut cache = StorageSizeCache::default();
        let cold = dir_usage(tmp.path(), &mut cache).unwrap();
        assert_eq!(cache.segment_dirs.len(), 2);
        let expected_logical = 4 + 2 * fs::metadata(first.join("segment.json")).unwrap().len();
        assert_eq!(cold.logical_bytes, expected_logical);
        let entries = cache
            .segment_dirs
            .iter()
            .map(|(key, value)| (key.clone(), Arc::clone(&value.entries)))
            .collect::<HashMap<_, _>>();
        assert_eq!(dir_usage(tmp.path(), &mut cache).unwrap(), cold);
        // Add an alias outside cached subtrees without changing their signatures.
        fs::hard_link(first.join("rows.bin"), tmp.path().join("alias.bin")).unwrap();
        let warm = dir_usage(tmp.path(), &mut cache).unwrap();
        assert_eq!(warm.logical_bytes, expected_logical);
        assert_eq!(warm.file_allocated_bytes, cold.file_allocated_bytes);
        for (key, before) in &entries {
            assert!(Arc::ptr_eq(before, &cache.segment_dirs[key].entries));
        }
        let expected_directories = [
            tmp.path().to_owned(),
            tmp.path().join("segments"),
            first.clone(),
            second.clone(),
        ]
        .into_iter()
        .map(|p| fs::metadata(p).unwrap().blocks() * 512)
        .sum::<u64>();
        assert_eq!(warm.directory_allocated_bytes, Some(expected_directories));
        // Invalidate just one subtree while the other keeps its inventory.
        fs::write(second.join("extra.bin"), [1; 7]).unwrap();
        let partial = dir_usage(tmp.path(), &mut cache).unwrap();
        assert_eq!(partial.logical_bytes, expected_logical + 7);
        let mut uncached = StorageSizeCache::default();
        assert_eq!(partial, dir_usage(tmp.path(), &mut uncached).unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn index_allocation_changes_invalidate_cache_without_length_or_mtime_changes() {
        let tmp = TempDir::new().unwrap();
        let segment = create_segment(tmp.path(), 1, 4);
        let indexes = segment.join("indexes");
        fs::create_dir(&indexes).unwrap();
        let path = indexes.join("address.bptree");
        let mut file = fs::File::create(&path).unwrap();
        file.set_len(8 * 1024 * 1024).unwrap();
        file.sync_all().unwrap();
        let original = file.metadata().unwrap();
        let mut cache = StorageSizeCache::default();
        let before = dir_usage(tmp.path(), &mut cache).unwrap();
        file.write_all(&[1; 64 * 1024]).unwrap();
        file.sync_all().unwrap();
        file.set_times(fs::FileTimes::new().set_modified(original.modified().unwrap()))
            .unwrap();
        let updated = file.metadata().unwrap();
        assert_eq!(original.len(), updated.len());
        assert_eq!(original.modified().unwrap(), updated.modified().unwrap());
        assert_ne!(allocated_bytes(&original), allocated_bytes(&updated));
        let after = dir_usage(tmp.path(), &mut cache).unwrap();
        assert_eq!(after.logical_bytes, before.logical_bytes);
        assert_eq!(
            after.file_allocated_bytes.unwrap() - before.file_allocated_bytes.unwrap(),
            allocated_bytes(&updated).unwrap() - allocated_bytes(&original).unwrap()
        );
        assert_eq!(
            after,
            dir_usage(tmp.path(), &mut StorageSizeCache::default()).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn nested_symlink_targets_are_not_hidden_by_a_segment_cache() {
        use std::os::unix::fs::symlink;
        let tmp = TempDir::new().unwrap();
        let external = TempDir::new().unwrap();
        let segment = create_segment(tmp.path(), 1, 4);
        fs::write(external.path().join("mutable.bin"), [1; 7]).unwrap();
        symlink(external.path(), segment.join("external")).unwrap();
        symlink(tmp.path(), external.path().join("cycle")).unwrap();
        let mut cache = StorageSizeCache::default();
        let before = dir_usage(tmp.path(), &mut cache).unwrap();
        assert!(cache.segment_dirs.is_empty());
        fs::write(external.path().join("mutable.bin"), [1; 17]).unwrap();
        let after = dir_usage(tmp.path(), &mut cache).unwrap();
        assert_eq!(after.logical_bytes, before.logical_bytes + 10);
        assert!(cache.segment_dirs.is_empty());
    }

    #[test]
    fn missing_storage_is_unknown_not_zero() {
        let tmp = TempDir::new().unwrap();
        let (metrics, _) =
            collect_storage_metrics(&tmp.path().join("missing"), StorageSizeCache::default());
        assert_eq!(metrics.storage_used_bytes, None);
        assert_eq!(metrics.storage_allocated_bytes, None);
        assert_eq!(metrics.storage_file_allocated_bytes, None);
        assert_eq!(metrics.storage_directory_allocated_bytes, None);
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_symlink_target_does_not_return_a_partial_total() {
        use std::os::unix::fs::symlink;
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("file.bin"), [1; 7]).unwrap();
        symlink(tmp.path().join("missing"), tmp.path().join("dangling")).unwrap();
        let (metrics, _) = collect_storage_metrics(tmp.path(), StorageSizeCache::default());
        assert_eq!(metrics.storage_used_bytes, None);
        assert_eq!(metrics.storage_allocated_bytes, None);
        assert!(metrics.storage_write_free_bytes.is_some());
    }

    #[test]
    fn allocation_overflow_is_unknown_and_logical_overflow_is_an_error() {
        let mut usage = StorageUsage {
            logical_bytes: 1,
            file_allocated_bytes: Some(u64::MAX),
            directory_allocated_bytes: Some(1),
        };
        assert_eq!(usage.allocated_bytes(), None);
        usage
            .add(StorageUsage {
                logical_bytes: 1,
                file_allocated_bytes: Some(1),
                directory_allocated_bytes: Some(0),
            })
            .unwrap();
        assert_eq!(usage.file_allocated_bytes, None);
        assert_eq!(usage.logical_bytes, 2);
        assert!(
            usage
                .add(StorageUsage {
                    logical_bytes: u64::MAX,
                    ..StorageUsage::default()
                })
                .is_err()
        );
    }

    fn write_active_hot_catalog(root: &Path, active_hot_segment: u64) {
        use logex_storage::native::{
            CATALOG_FORMAT_VERSION, NativeStorageCatalog, SegmentKind, StorageCatalogPaths,
        };
        let mut catalog = NativeStorageCatalog {
            format_version: CATALOG_FORMAT_VERSION,
            state: Default::default(),
            hot_target_rows: 1_000_000,
            next_segment_id: active_hot_segment,
            active_hot_segment: None,
            active_historical_segment: None,
            anchors: Default::default(),
            segments: Vec::new(),
        };
        catalog.register_segment(SegmentKind::Hot).unwrap();
        catalog
            .persist(&StorageCatalogPaths::new(root.to_owned()))
            .expect("catalog");
    }

    fn create_segment(root: &Path, segment_id: u64, rows_len: usize) -> PathBuf {
        let segment = root.join("segments").join(format!("s_{segment_id:016}"));
        fs::create_dir_all(&segment).expect("segment dir");
        fs::write(segment.join("rows.bin"), vec![0_u8; rows_len]).expect("rows");
        fs::write(segment.join("segment.json"), br#"{"generation":0}"#).expect("manifest");
        segment
    }
}
