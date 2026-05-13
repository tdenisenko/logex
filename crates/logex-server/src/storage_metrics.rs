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
    {
        let guard = cache.lock().await;
        if guard.is_fresh() {
            return guard.metrics();
        }
    }

    let mut metrics = tokio::task::spawn_blocking(move || collect_storage_metrics(&data_dir))
        .await
        .unwrap_or_default();

    let mut guard = cache.lock().await;
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
    guard.update(metrics.clone());
    metrics
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
    if !root.exists() {
        return Ok(0);
    }

    let mut total = 0_u64;
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(entry.path());
            } else if file_type.is_file() {
                total = total.saturating_add(entry.metadata()?.len());
            }
        }
    }

    Ok(total)
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

    #[test]
    fn collect_metrics_reports_storage_usage() {
        let tmp = TempDir::new().expect("tempdir");
        fs::write(tmp.path().join("rows.bin"), [1_u8, 2, 3, 4]).expect("rows file");

        let metrics = collect_storage_metrics(tmp.path());
        assert_eq!(metrics.storage_used_bytes, Some(4));
        #[cfg(unix)]
        assert!(metrics.disk_free_bytes.unwrap_or(0) > 0);
    }
}
