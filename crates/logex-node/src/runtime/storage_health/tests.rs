use super::*;
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[tokio::test]
async fn independent_failure_is_retained_without_another_filesystem_probe() {
    for closed in [false, true] {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        if !closed {
            sender.send_replace(Some("owned volume unavailable".to_owned()));
        }
        drop(sender);
        let failure = wait_for_failure(PathBuf::from("unused-volume-path"), receiver).await;
        let text = failure.to_string();
        assert!(text.contains(if closed {
            "notification channel closed"
        } else {
            "owned volume unavailable"
        }));
    }
}

#[test]
fn disk_space_guard_trips_below_threshold() {
    assert!(disk_space_is_low(9, 10));
    assert!(!disk_space_is_low(10, 10));
}

#[cfg(unix)]
#[test]
fn disk_space_probe_paths_track_writable_roots_not_sealed_targets() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().join("data");
    let segments = data.join("segments");
    let external = temp.path().join("extra/segments");
    let sealed = external.join("s_1");
    std::fs::create_dir_all(&segments).unwrap();
    std::fs::create_dir_all(&sealed).unwrap();
    std::os::unix::fs::symlink(&sealed, segments.join("s_1")).unwrap();
    let probes = disk_space_probe_paths(&data);
    assert!(probes.contains(&data.canonicalize().unwrap()));
    assert!(probes.contains(&segments.canonicalize().unwrap()));
    assert!(!probes.contains(&external.canonicalize().unwrap()));
}

#[cfg(unix)]
#[test]
fn missing_storage_path_preserves_the_probe_error() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("unavailable");
    let failure = check_paths(&path, 0, free_space_bytes).unwrap_err();
    assert!(
        matches!(failure, StorageHealthFailure::Probe { path: failed, source }
        if failed == path && source.kind() == io::ErrorKind::NotFound)
    );
}

#[test]
fn probe_keeps_the_exact_failed_path_and_io_error() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().canonicalize().unwrap();
    std::fs::create_dir(data.join("segments")).unwrap();
    let result = check_paths(&data, 10, |path| {
        if path.ends_with("segments") {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "local probe control",
            ))
        } else {
            Ok(10)
        }
    });
    assert!(
        matches!(result, Err(StorageHealthFailure::Probe { path, source })
        if path == data.join("segments") && source.kind() == io::ErrorKind::PermissionDenied)
    );
}

#[test]
fn low_space_on_the_segments_root_preserves_the_measurement() {
    let temp = tempfile::tempdir().unwrap();
    let data = temp.path().canonicalize().unwrap();
    std::fs::create_dir(data.join("segments")).unwrap();
    let result = check_paths(&data, 10, |path| {
        Ok(if path.ends_with("segments") { 9 } else { 10 })
    });
    assert!(
        matches!(result, Err(StorageHealthFailure::LowSpace { path, free_bytes: 9, min_free_bytes: 10 })
        if path == data.join("segments"))
    );
    assert!(check_paths(&data, 10, |_| Ok(10)).is_ok());
}

#[cfg(unix)]
#[test]
fn initialized_storage_has_both_roots_for_the_first_probe() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("fresh");
    assert!(!path.exists());
    let _storage = logex_storage::PartitionManager::open(logex_storage::PartitionManagerConfig {
        data_dir: path.clone(),
        ..Default::default()
    })
    .unwrap();
    check_paths(&path, 0, free_space_bytes).unwrap();
}

#[cfg(unix)]
#[test]
fn invalid_path_is_rejected_before_the_filesystem_call() {
    use std::os::unix::ffi::OsStrExt;
    let path = Path::new(std::ffi::OsStr::from_bytes(b"owned\0probe"));
    assert_eq!(
        free_space_bytes(path).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn ordinary_monitor_probes_immediately_without_async_polling() {
    let (checked, observed) = mpsc::channel();
    let monitor = StorageMonitor::start_storage(move || {
        checked.send(()).unwrap();
        Ok(())
    })
    .unwrap();
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(monitor);
    assert_eq!(observed.try_recv(), Err(mpsc::TryRecvError::Disconnected));
}

#[test]
fn initialized_storage_keeps_the_existing_volume_monitor() {
    let (checked, observed) = mpsc::channel();
    let mut owner = Some(
        StorageMonitor::start_storage(move || {
            checked.send(()).unwrap();
            Ok(())
        })
        .unwrap(),
    );
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    let root = tempfile::tempdir().unwrap();
    // A substituted ordinary probe would fail on this missing path. Reusing
    // the existing handle also preserves its single callback registration.
    let first = owner.as_ref().unwrap().handle();
    first.set_failure_handler(|_| {}).unwrap();
    let retained = monitor_initialized_storage(&mut owner, &root.path().join("unused")).unwrap();
    assert!(retained.set_failure_handler(|_| {}).is_err());
    drop(owner);
}

#[cfg(unix)]
#[test]
fn ordinary_monitor_failure_cases_run_in_owned_children() {
    for case in ["missing_start", "runtime_blocked"] {
        let directory = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "runtime::storage_health::tests::ordinary_monitor_child",
                "--nocapture",
            ])
            .env("LOGEX_ORDINARY_MONITOR_CASE", case)
            .current_dir(directory.path())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while child.try_wait().unwrap().is_none() {
            if Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!("owned {case} child did not complete: {output:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.code(), Some(1), "{case}: {output:?}");
        assert!(!directory.path().join("unavailable").exists());
        if case == "runtime_blocked" {
            assert_eq!(
                std::fs::read(directory.path().join("verified")).unwrap(),
                b"ordinary failure observed without async progress"
            );
        } else {
            assert!(String::from_utf8_lossy(&output.stderr).contains("unavailable"));
        }
    }
}

#[cfg(unix)]
#[test]
fn ordinary_monitor_child() {
    let Ok(case) = std::env::var("LOGEX_ORDINARY_MONITOR_CASE") else {
        return;
    };
    if case == "missing_start" {
        let mut owner = None;
        // The real production constructor must report this path, not create it
        // or silently use an existing ancestor with healthy free space.
        monitor_initialized_storage(&mut owner, Path::new("unavailable")).unwrap();
        std::thread::sleep(Duration::from_secs(5));
        panic!("missing storage did not fail startup");
    }
    assert_eq!(case, "runtime_blocked");
    let (begin, allowed) = mpsc::channel();
    let monitor = StorageMonitor::start_storage(move || {
        allowed.recv_timeout(Duration::from_secs(5)).unwrap();
        check_paths(Path::new("unavailable"), 0, free_space_bytes).map_err(io::Error::other)
    })
    .unwrap();
    let (failure, receiver) = tokio::sync::watch::channel(None);
    let (reported, observed) = mpsc::channel();
    monitor
        .handle()
        .set_failure_handler(move |reason| {
            failure.send_replace(Some(reason.to_owned()));
            reported.send(reason.to_owned()).unwrap();
        })
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let engine = async {
            begin.send(()).unwrap();
            // Model a bounded synchronous engine call. Its sibling health
            // future never gets polled, yet the monitor must report failure.
            let reason = observed.recv_timeout(Duration::from_secs(5)).unwrap();
            assert!(reason.contains("unavailable"));
        };
        tokio::select! { biased;
            _ = engine => {},
            _ = wait_for_failure(PathBuf::from("unavailable"), receiver) =>
                panic!("engine control unexpectedly yielded"),
        }
    });
    drop(runtime);
    std::fs::write(
        "verified",
        b"ordinary failure observed without async progress",
    )
    .unwrap();
    // Teardown must retain the failure even if the engine exits before the
    // async supervisor observes the notification.
    drop(monitor);
    panic!("ordinary storage failure became successful shutdown");
}
