use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

const STORAGE_METRICS_TTL: Duration = Duration::from_secs(15);

#[derive(Clone, Debug, Default)]
pub struct StorageMetrics {
    pub storage_used_bytes: Option<u64>,
    pub disk_free_bytes: Option<u64>,
    pub cpu_utilization_pct: Option<f64>,
}

#[derive(Debug, Default)]
pub struct CachedStorageMetrics {
    refreshed_at: Option<Instant>,
    process_cpu_time: Option<Duration>,
    refresh_in_progress: bool,
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
    let cached = {
        let mut guard = cache.lock().await;
        if guard.is_fresh() || guard.refresh_in_progress {
            return guard.metrics();
        }

        guard.refresh_in_progress = true;
        guard.metrics()
    };

    tokio::spawn(async move {
        let metrics = tokio::task::spawn_blocking(move || collect_storage_metrics(&data_dir))
            .await
            .unwrap_or_default();

        let mut guard = cache.lock().await;
        update_cached_metrics(&mut guard, metrics);
    });

    cached
}

#[cfg(test)]
pub async fn refresh_for_test(
    cache: Arc<tokio::sync::Mutex<CachedStorageMetrics>>,
    data_dir: PathBuf,
) -> StorageMetrics {
    let metrics = tokio::task::spawn_blocking(move || collect_storage_metrics(&data_dir))
        .await
        .unwrap_or_default();

    let mut guard = cache.lock().await;
    update_cached_metrics(&mut guard, metrics)
}

fn update_cached_metrics(
    guard: &mut CachedStorageMetrics,
    mut metrics: StorageMetrics,
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

fn collect_storage_metrics(data_dir: &Path) -> StorageMetrics {
    StorageMetrics {
        storage_used_bytes: dir_size_bytes(data_dir).ok(),
        disk_free_bytes: free_space_bytes(data_dir).ok(),
        cpu_utilization_pct: None,
    }
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

fn dir_size_bytes(root: &Path) -> io::Result<u64> {
    let mut total = 0_u64;
    let mut visited_dirs = HashSet::new();
    let mut visited_files = HashSet::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        let metadata = match fs::metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error),
        };

        if metadata.is_file() {
            add_file_size(&mut total, &mut visited_files, &metadata);
            continue;
        }

        if !metadata.is_dir() {
            continue;
        }

        if let Some(id) = metadata_id(&metadata)
            && !visited_dirs.insert(id)
        {
            continue;
        }

        for entry in fs::read_dir(path)? {
            let entry = entry?;
            stack.push(entry.path());
        }
    }

    Ok(total)
}

fn add_file_size(
    total: &mut u64,
    visited_files: &mut HashSet<MetadataId>,
    metadata: &fs::Metadata,
) {
    if let Some(id) = metadata_id(metadata)
        && !visited_files.insert(id)
    {
        return;
    }

    *total = total.saturating_add(metadata.len());
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

        assert_eq!(dir_size_bytes(tmp.path()).expect("size"), 18);
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
        symlink(external.path(), segments.join("s_0001")).expect("first symlink");
        symlink(external.path(), segments.join("s_0001_alias")).expect("second symlink");

        assert_eq!(dir_size_bytes(tmp.path()).expect("size"), 18);
    }

    #[test]
    fn collect_metrics_reports_storage_usage() {
        let tmp = TempDir::new().expect("tempdir");
        fs::write(tmp.path().join("rows.bin"), [1_u8, 2, 3, 4]).expect("rows file");

        let metrics = collect_storage_metrics(tmp.path());
        assert_eq!(metrics.storage_used_bytes, Some(4));
        #[cfg(unix)]
        assert!(metrics.disk_free_bytes.unwrap_or(0) > 0);
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
}
