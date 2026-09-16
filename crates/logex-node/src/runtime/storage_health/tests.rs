use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};

#[tokio::test]
async fn independent_volume_failure_is_retained_without_another_filesystem_probe() {
    for closed in [false, true] {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        if !closed {
            sender.send_replace(Some("owned volume unavailable".to_owned()));
        }
        drop(sender);
        let failure = wait_for_failure(PathBuf::from("unused-volume-path"), Some(receiver)).await;
        let text = failure.to_string();
        assert!(text.contains(if closed {
            "notification channel closed"
        } else {
            "owned volume unavailable"
        }));
    }
}

#[tokio::test]
async fn queued_completion_after_deadline_is_not_healthy() {
    let deadline = tokio::time::Instant::now() - Duration::from_secs(1);
    let mut work = JoinSet::new();
    let task = work.spawn(async { (tokio::time::Instant::now(), Ok(())) });
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    let result = await_probe(
        PathBuf::from("owned-probe-control"),
        Duration::ZERO,
        deadline,
        work,
    )
    .await;
    assert!(
        matches!(result, Err(StorageHealthFailure::Probe { source, .. })
        if source.kind() == io::ErrorKind::TimedOut)
    );
}

#[tokio::test]
async fn queued_timely_completion_survives_delayed_polling() {
    let (finished, observed) = tokio::sync::oneshot::channel();
    let mut work = JoinSet::new();
    let task = work.spawn(async move {
        let completed = tokio::time::Instant::now();
        finished.send(completed).unwrap();
        (completed, Ok(()))
    });
    let deadline = observed.await.unwrap();
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    await_probe(
        PathBuf::from("owned-probe-control"),
        Duration::ZERO,
        deadline,
        work,
    )
    .await
    .unwrap();
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
#[tokio::test]
async fn missing_storage_path_stops_the_health_guard() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("unavailable");
    assert_eq!(
        free_space_bytes(&path).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    let failure =
        tokio::time::timeout(Duration::from_secs(5), wait_for_failure(path.clone(), None))
            .await
            .expect("storage probe error was ignored");
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
#[tokio::test]
async fn healthy_existing_roots_complete_the_probe() {
    let temp = tempfile::tempdir().unwrap();
    std::fs::create_dir(temp.path().join("segments")).unwrap();
    run_probe(temp.path().to_path_buf(), Duration::from_secs(5), |path| {
        check_paths(path, 0, free_space_bytes)
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn worker_unwind_is_an_actionable_probe_failure() {
    let path = PathBuf::from("owned-probe-control");
    let result = run_probe(path.clone(), Duration::from_secs(5), |_| {
        panic!("isolated filesystem probe completion control");
    })
    .await;
    let Err(StorageHealthFailure::Probe {
        path: failed,
        source,
    }) = result
    else {
        panic!("probe failure was lost");
    };
    assert_eq!(failed, path);
    assert!(
        source
            .get_ref()
            .unwrap()
            .downcast_ref::<tokio::task::JoinError>()
            .unwrap()
            .is_panic()
    );
}

#[tokio::test]
async fn started_probe_timeout_does_not_wait_for_blocking_work_again() {
    let (release, blocked) = mpsc::channel::<()>();
    let (started, observed) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(run_probe(
        PathBuf::from("owned-probe-control"),
        Duration::from_secs(1),
        move |_| {
            let _ = started.send(());
            let _ = blocked.recv();
            Ok(())
        },
    ));
    let began = tokio::time::timeout(Duration::from_secs(5), observed).await;
    let result = tokio::time::timeout(Duration::from_secs(5), task).await;
    // Release this test's worker before checking any observation.
    drop(release);
    began.unwrap().unwrap();
    assert!(
        matches!(result.unwrap().unwrap(), Err(StorageHealthFailure::Probe { source, .. })
        if source.kind() == io::ErrorKind::TimedOut)
    );
}

#[test]
fn expired_queued_probe_is_canceled_before_it_can_start() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    let (release, blocked) = mpsc::channel::<()>();
    let (started, observed) = mpsc::channel();
    runtime.spawn_blocking(move || {
        let _ = started.send(());
        let _ = blocked.recv();
    });
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
    let ran = Arc::new(AtomicBool::new(false));
    let worker_ran = Arc::clone(&ran);
    let result = runtime.block_on(run_probe(
        PathBuf::from("queued-probe-control"),
        Duration::ZERO,
        move |_| {
            worker_ran.store(true, Ordering::SeqCst);
            Ok(())
        },
    ));
    drop(release);
    drop(runtime);
    assert!(
        matches!(result, Err(StorageHealthFailure::Probe { source, .. })
        if source.kind() == io::ErrorKind::TimedOut)
    );
    assert!(!ran.load(Ordering::SeqCst));
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
