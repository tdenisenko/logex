use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};

const STORAGE_METRICS_TTL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, Default)]
pub struct StorageMetrics {
    pub storage_used_bytes: Option<u64>,
    pub disk_free_bytes: Option<u64>,
    pub storage_write_free_bytes: Option<u64>,
    pub storage_write_path: Option<String>,
    pub storage_free_total_bytes: Option<u64>,
    pub storage_free_volumes: Vec<StorageVolumeMetrics>,
    pub storage_limiting_path: Option<String>,
    pub cpu_utilization_pct: Option<f64>,
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
    metrics.cpu_utilization_pct = guard
        .refreshed_at
        .zip(guard.process_cpu_time)
        .zip(process_cpu_time)
        .and_then(|((refreshed_at, previous_cpu_time), current_cpu_time)| {
            let elapsed = now.saturating_duration_since(refreshed_at);
            let cpu_delta = current_cpu_time.checked_sub(previous_cpu_time)?;
            (elapsed.as_secs_f64() > 0.0)
                .then_some(cpu_delta.as_secs_f64() / elapsed.as_secs_f64() * 100.0)
        });
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
    let storage_used_bytes = dir_size_bytes(data_dir, &mut size_cache).ok();
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
            storage_used_bytes,
            disk_free_bytes,
            storage_write_free_bytes,
            storage_write_path: Some(storage_write_path.display().to_string()),
            storage_free_total_bytes,
            storage_free_volumes,
            storage_limiting_path,
            cpu_utilization_pct: None,
        },
        size_cache,
    )
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
    bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SegmentDirSignature {
    dir_modified: Option<SystemTime>,
    dir_len: u64,
    manifest_modified: Option<SystemTime>,
    manifest_len: u64,
    manifest_id: Option<MetadataId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum SizeCacheKey {
    Metadata(MetadataId),
    Path(PathBuf),
}

#[derive(Debug, Deserialize)]
struct CatalogSizeSnapshot {
    active_hot_segment: Option<u64>,
}

fn dir_size_bytes(root: &Path, cache: &mut StorageSizeCache) -> io::Result<u64> {
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
        visited_dirs: HashSet::new(),
        visited_files: HashSet::new(),
    };

    let total = walk.size(root, cache)?;
    cache
        .segment_dirs
        .retain(|key, _| walk.seen_cached_segments.contains(key));
    Ok(total)
}

struct DirSizeWalk<'a> {
    segments_dir: &'a Path,
    active_hot_segment: Option<&'a Path>,
    cache_segments: bool,
    seen_cached_segments: HashSet<SizeCacheKey>,
    visited_dirs: HashSet<MetadataId>,
    visited_files: HashSet<MetadataId>,
}

impl DirSizeWalk<'_> {
    fn size(&mut self, path: &Path, cache: &mut StorageSizeCache) -> io::Result<u64> {
        let metadata = match fs::metadata(path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error),
        };

        if metadata.is_file() {
            return Ok(file_size_bytes(&mut self.visited_files, &metadata));
        }

        if !metadata.is_dir() {
            return Ok(0);
        }

        if let Some(id) = metadata_id(&metadata)
            && !self.visited_dirs.insert(id)
        {
            return Ok(0);
        }

        let cacheable_segment = self
            .cache_segments
            .then(|| {
                cacheable_segment_dir(path, self.segments_dir, self.active_hot_segment, &metadata)
            })
            .transpose()?
            .flatten();
        if let Some((key, signature)) = cacheable_segment.as_ref() {
            self.seen_cached_segments.insert(key.clone());
            if let Some(cached) = cache.segment_dirs.get(key)
                && cached.signature == *signature
            {
                return Ok(cached.bytes);
            }
        }

        let mut total = 0_u64;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            total = total.saturating_add(self.size(&entry.path(), cache)?);
        }

        if let Some((key, signature)) = cacheable_segment {
            cache.segment_dirs.insert(
                key,
                CachedDirSize {
                    signature,
                    bytes: total,
                },
            );
        }

        Ok(total)
    }
}

fn active_hot_segment_path(root: &Path) -> io::Result<Option<PathBuf>> {
    let catalog_path = root.join("catalog.json");
    let json = match fs::read(&catalog_path) {
        Ok(json) => json,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let catalog: CatalogSizeSnapshot = serde_json::from_slice(&json).map_err(io::Error::other)?;
    Ok(catalog
        .active_hot_segment
        .map(|id| root.join("segments").join(format!("s_{id:016}"))))
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

    Ok(Some(SegmentDirSignature {
        dir_modified: metadata.modified().ok(),
        dir_len: metadata.len(),
        manifest_modified: manifest_metadata.modified().ok(),
        manifest_len: manifest_metadata.len(),
        manifest_id: metadata_id(&manifest_metadata),
    }))
}

fn size_cache_key(path: &Path, metadata: &fs::Metadata) -> SizeCacheKey {
    metadata_id(metadata)
        .map(SizeCacheKey::Metadata)
        .unwrap_or_else(|| SizeCacheKey::Path(path.to_path_buf()))
}

fn file_size_bytes(visited_files: &mut HashSet<MetadataId>, metadata: &fs::Metadata) -> u64 {
    if let Some(id) = metadata_id(metadata)
        && !visited_files.insert(id)
    {
        return 0;
    }

    metadata.len()
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
            assert!(metrics.disk_free_bytes.unwrap_or(0) > 0);
            assert!(metrics.storage_write_free_bytes.unwrap_or(0) > 0);
            assert!(metrics.storage_write_path.as_deref().is_some_and(|path| {
                path == tmp.path().canonicalize().unwrap().display().to_string()
            }));
            assert_eq!(metrics.storage_free_volumes.len(), 1);
            assert_eq!(metrics.storage_free_total_bytes, metrics.disk_free_bytes);
            assert_eq!(metrics.storage_limiting_path, metrics.storage_write_path);
            assert_eq!(
                metrics.storage_free_volumes[0].free_bytes,
                metrics.disk_free_bytes.unwrap()
            );
        }
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

    fn write_active_hot_catalog(root: &Path, active_hot_segment: u64) {
        fs::write(
            root.join("catalog.json"),
            format!(r#"{{"active_hot_segment":{active_hot_segment}}}"#),
        )
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
