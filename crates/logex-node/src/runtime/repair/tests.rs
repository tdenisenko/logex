use super::*;
use alloy_primitives::{Address, B256, bytes};
use clap::Parser;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_sync::repair::RepairAssessmentReport;
use std::{
    collections::BTreeMap,
    fs,
    sync::atomic::{AtomicBool, Ordering},
    time::SystemTime,
};

fn args() -> RepairLimitsArgs {
    let crate::cli::Command::Repair { repair_limits, .. } =
        crate::cli::Cli::parse_from(["logex", "repair"]).command
    else {
        unreachable!()
    };
    repair_limits
}

fn storage(root: &Path) -> PartitionManager {
    PartitionManager::open(PartitionManagerConfig {
        data_dir: root.to_owned(),
        ..Default::default()
    })
    .unwrap()
}

type Snapshot = BTreeMap<PathBuf, (SystemTime, Option<Vec<u8>>)>;
fn snapshot(root: &Path) -> Snapshot {
    fn visit(root: &Path, path: &Path, result: &mut Snapshot) {
        let metadata = fs::symlink_metadata(path).unwrap();
        result.insert(
            path.strip_prefix(root).unwrap().to_owned(),
            (
                metadata.modified().unwrap(),
                metadata.is_file().then(|| fs::read(path).unwrap()),
            ),
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), result);
            }
        }
    }
    let mut result = Snapshot::new();
    visit(root, root, &mut result);
    result
}

#[test]
fn dry_run_assessment_preserves_empty_valid_and_pending_storage() {
    for case in 0..3 {
        let temp = tempfile::tempdir().unwrap();
        let mut owner = storage(temp.path());
        if case == 1 {
            owner
                .write_batch(&[logex_types::LogRow {
                    block_number: 100,
                    block_hash: B256::repeat_byte(1),
                    timestamp: 1200,
                    tx_hash: B256::repeat_byte(2),
                    tx_index: 0,
                    log_index: 0,
                    address: Address::repeat_byte(3),
                    topic0: None,
                    topic1: None,
                    topic2: None,
                    topic3: None,
                    data: bytes!("abcd"),
                    data_len: 2,
                    source: logex_types::Source::Receipt,
                }])
                .unwrap();
            owner.checkpoint_durable().unwrap();
        }
        drop(owner);
        if case == 2 {
            fs::write(temp.path().join("wal/pending.wal"), [1, 2, 3]).unwrap();
        }
        let before = snapshot(temp.path());
        let assessment = inspect(temp.path(), &args()).unwrap();
        assert_eq!(snapshot(temp.path()), before);
        if case == 2 {
            let RepairAssessmentReport::RecoveryRequired { artifacts } = assessment.report() else {
                panic!("pending WAL must precede primary assessment")
            };
            assert!(artifacts.iter().any(|path| path.ends_with("pending.wal")));
            assert_eq!(report::report_exit_code(assessment.report()), 2);
        } else {
            assert!(matches!(
                assessment.report(),
                RepairAssessmentReport::Inspected { .. }
            ));
        }
        // The report retains exclusive ownership until explicitly dropped.
        assert!(inspect(temp.path(), &args()).is_err());
        drop(assessment);
        assert_eq!(snapshot(temp.path()), before);
        assert!(inspect(temp.path(), &args()).is_ok());
    }
}

#[test]
fn dry_run_missing_and_locked_storage_fail_without_writes() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("missing");
    let before = snapshot(temp.path());
    assert!(inspect(&missing, &args()).is_err());
    assert_eq!(snapshot(temp.path()), before);
    let owner = storage(temp.path());
    let before = snapshot(temp.path());
    assert!(inspect(temp.path(), &args()).is_err());
    assert_eq!(snapshot(temp.path()), before);
    drop(owner);
}

#[tokio::test]
async fn execution_unwind_allows_cleanup_while_retaining_directory_ownership() {
    let temp = tempfile::tempdir().unwrap();
    drop(storage(temp.path()));
    let assessment = inspect(temp.path(), &args()).unwrap();
    let directory = assessment.retain_directory();
    let result: Result<()> = catch_repair_unwind(async move {
        drop(assessment);
        panic!("isolated maintenance execution unwind");
    })
    .await;
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("execution panicked")
    );
    // Cleanup is reached with exclusive ownership even after the consumed
    // assessment was dropped while executing the failed operation.
    assert_eq!(
        inspect(temp.path(), &args())
            .unwrap_err()
            .downcast_ref::<io::Error>()
            .unwrap()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    tokio::task::yield_now().await;
    drop(directory);
    assert!(inspect(temp.path(), &args()).is_ok());
}

#[test]
fn startup_assessment_distinguishes_fresh_roots_from_unidentified_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("missing");
    let before = snapshot(temp.path());
    assert!(!needs_startup_assessment(&missing).unwrap());
    assert!(!needs_startup_assessment(temp.path()).unwrap());
    assert_eq!(snapshot(temp.path()), before);
    fs::write(temp.path().join("orphaned-primary"), b"retain original").unwrap();
    let before = snapshot(temp.path());
    assert_eq!(
        needs_startup_assessment(temp.path()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(snapshot(temp.path()), before);
    assert_eq!(
        needs_startup_assessment(&temp.path().join("orphaned-primary"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let initialized = tempfile::tempdir().unwrap();
    drop(storage(initialized.path()));
    let before = snapshot(initialized.path());
    assert!(needs_startup_assessment(initialized.path()).unwrap());
    assert_eq!(snapshot(initialized.path()), before);
}

#[cfg(unix)]
#[test]
fn startup_assessment_rejects_directory_alias_without_following_it() {
    let temp = tempfile::tempdir().unwrap();
    let target = temp.path().join("target");
    fs::create_dir(&target).unwrap();
    let alias = temp.path().join("alias");
    std::os::unix::fs::symlink(&target, &alias).unwrap();
    let before = snapshot(temp.path());
    assert_eq!(
        needs_startup_assessment(&alias).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(snapshot(temp.path()), before);
}

#[test]
fn execution_limits_map_allowances_and_reject_zero_or_timeout_overflow() {
    let mut options = args();
    options.repair_timeout_secs = 21;
    options.repair_request_timeout_secs = 7;
    options.repair_max_attempts = 2;
    options.repair_max_segments = 5;
    options.repair_max_segment_rows = 101;
    options.repair_max_segment_bytes = 202;
    options.repair_max_total_rows = 303;
    options.repair_max_total_data_bytes = 404;
    options.repair_max_blocks = 505;
    options.repair_max_headers = 606;
    let before = Instant::now();
    let value = limits::execution_limits(&options).unwrap();
    let after = Instant::now();
    assert!(value.fetch.deadline >= before + Duration::from_secs(21));
    assert!(value.fetch.deadline <= after + Duration::from_secs(21));
    assert_eq!(value.fetch.request_timeout, Duration::from_secs(7));
    assert_eq!(value.fetch.max_attempts, 2);
    assert_eq!(value.fetch.max_headers, 606);
    assert_eq!(value.assessment.primary.max_segment_rows, 101);
    assert_eq!(value.assessment.primary.max_retained_artifact_bytes, 202);
    assert_eq!(value.assessment.primary.max_decoded_payload_bytes, 202);
    assert_eq!(value.assessment.max_index_logical_bytes_per_segment, 202);
    assert_eq!((value.wal.max_rows, value.wal.max_bytes), (101, 202));
    assert_eq!((value.plan.max_segments, value.plan.max_blocks), (5, 505));
    assert_eq!(value.plan.max_segment_rows, 101);
    assert_eq!(value.plan.max_canonical_artifact_bytes, 202);
    assert_eq!(value.plan.max_candidate_data_bytes, 202);
    assert_eq!(
        (
            value.reconstruction.max_total_rows,
            value.reconstruction.max_total_data_bytes
        ),
        (303, 404)
    );
    assert_eq!(value.reconstruction.read.max_routing_artifact_bytes, 202);
    assert_eq!(value.reconstruction.read.max_carry_artifact_bytes, 202);
    assert_eq!(
        value.reconstruction.read.max_carry_decoded_payload_bytes,
        202
    );
    assert_eq!(value.reconstruction.read.max_carry_data_bytes, 202);
    for field in 0..10 {
        let mut invalid = options.clone();
        match field {
            0 => invalid.repair_timeout_secs = 0,
            1 => invalid.repair_request_timeout_secs = 0,
            2 => invalid.repair_max_attempts = 0,
            3 => invalid.repair_max_segments = 0,
            4 => invalid.repair_max_segment_rows = 0,
            5 => invalid.repair_max_segment_bytes = 0,
            6 => invalid.repair_max_total_rows = 0,
            7 => invalid.repair_max_total_data_bytes = 0,
            8 => invalid.repair_max_blocks = 0,
            9 => invalid.repair_max_headers = 0,
            _ => unreachable!(),
        }
        assert!(
            limits::execution_limits(&invalid).is_err(),
            "zero field {field}"
        );
    }
    options.repair_timeout_secs = u64::MAX;
    assert!(limits::execution_limits(&options).is_err());
}

#[tokio::test]
async fn supervision_cancellation_joins_owned_worker_before_returning() {
    let temp = tempfile::tempdir().unwrap();
    let owner = storage(temp.path());
    let cancellation = CancellationToken::new();
    let work_cancel = cancellation.clone();
    let finished = Arc::new(AtomicBool::new(false));
    let work_finished = Arc::clone(&finished);
    let (release, released) = tokio::sync::oneshot::channel();
    let runtime = tokio::runtime::Handle::current();
    let mut worker = tokio::task::spawn_blocking(move || -> Result<()> {
        runtime.block_on(work_cancel.cancelled());
        released.blocking_recv().unwrap();
        drop(owner);
        work_finished.store(true, Ordering::SeqCst);
        Ok(())
    });
    let (stopping, stop_rx) = watch::channel(None);
    let state = MaintenanceState::new(RepairPhase::Inspecting);
    let mut failure = None;
    let mut server_failure = None;
    let supervision = supervise_worker(
        &mut worker,
        WorkerSupervision {
            cancellation: &cancellation,
            stopping: &stopping,
            state: &state,
            deadline: Instant::now() + Duration::from_secs(30),
            failure: &mut failure,
            server_failure: &mut server_failure,
        },
        std::future::ready(Ok("test signal")),
    );
    let release_owned_worker = async {
        cancellation.cancelled().await;
        assert!(stop_rx.borrow().is_some());
        assert!(!finished.load(Ordering::SeqCst));
        let blocked = inspect(temp.path(), &args()).is_err();
        release.send(()).unwrap();
        assert!(
            blocked,
            "worker must retain its directory owner while cancelling"
        );
    };
    let (result, ()) = tokio::join!(supervision, release_owned_worker);
    assert!(result.is_err());
    assert!(finished.load(Ordering::SeqCst));
    assert!(worker.is_finished());
    assert!(inspect(temp.path(), &args()).is_ok());
}

#[tokio::test]
async fn supervision_failures_and_deadline_beat_completed_success() {
    for case in 0..3 {
        let cancellation = CancellationToken::new();
        let (stopping, stop_rx) = watch::channel(None);
        let state = MaintenanceState::new(RepairPhase::Inspecting);
        let (sender, receiver) = watch::channel(Some(Arc::<str>::from("latched fixture failure")));
        let mut failure = (case == 0).then_some(receiver.clone());
        let mut server_failure = (case == 1).then_some(receiver);
        let deadline = if case == 2 {
            Instant::now() - Duration::from_secs(1)
        } else {
            Instant::now() + Duration::from_secs(30)
        };
        let mut worker = tokio::spawn(async { Ok::<_, eyre::Report>(42) });
        while !worker.is_finished() {
            tokio::task::yield_now().await;
        }
        let result = supervise_worker(
            &mut worker,
            WorkerSupervision {
                cancellation: &cancellation,
                stopping: &stopping,
                state: &state,
                deadline,
                failure: &mut failure,
                server_failure: &mut server_failure,
            },
            std::future::pending::<io::Result<&'static str>>(),
        )
        .await;
        assert!(result.is_err(), "case {case}");
        assert!(cancellation.is_cancelled());
        assert!(stop_rx.borrow().is_some());
        assert!(worker.is_finished());
        drop(sender);
    }
}

#[tokio::test]
async fn supervision_arms_stopping_and_propagates_worker_results() {
    for case in 0..3 {
        let cancellation = CancellationToken::new();
        let (stopping, stop_rx) = watch::channel(None);
        let state = MaintenanceState::new(RepairPhase::Inspecting);
        let mut failure = None;
        let mut server_failure = None;
        let mut worker = tokio::spawn(async move {
            match case {
                0 => Ok(42),
                1 => Err(eyre::eyre!("fixture worker error")),
                _ => panic!("fixture worker panic"),
            }
        });
        let result = supervise_worker(
            &mut worker,
            WorkerSupervision {
                cancellation: &cancellation,
                stopping: &stopping,
                state: &state,
                deadline: Instant::now() + Duration::from_secs(30),
                failure: &mut failure,
                server_failure: &mut server_failure,
            },
            std::future::pending::<io::Result<&'static str>>(),
        )
        .await;
        assert!(stop_rx.borrow().is_some());
        assert!(worker.is_finished());
        match case {
            0 => assert_eq!(result.unwrap(), 42),
            1 => assert!(format!("{:#}", result.unwrap_err()).contains("fixture worker error")),
            _ => {
                assert!(format!("{:#}", result.unwrap_err()).contains("maintenance worker failed"))
            }
        }
    }
}

#[tokio::test]
async fn supervision_signal_registration_error_cancels_and_joins_worker() {
    let cancellation = CancellationToken::new();
    let work_cancel = cancellation.clone();
    let mut worker = tokio::spawn(async move {
        work_cancel.cancelled().await;
        Ok::<_, eyre::Report>(())
    });
    let (stopping, stop_rx) = watch::channel(None);
    let state = MaintenanceState::new(RepairPhase::Inspecting);
    let mut failure = None;
    let mut server_failure = None;
    let result = supervise_worker(
        &mut worker,
        WorkerSupervision {
            cancellation: &cancellation,
            stopping: &stopping,
            state: &state,
            deadline: Instant::now() + Duration::from_secs(30),
            failure: &mut failure,
            server_failure: &mut server_failure,
        },
        std::future::ready(Err(io::Error::other("fixture signal registration failed"))),
    )
    .await;
    assert!(format!("{:#}", result.unwrap_err()).contains("fixture signal registration failed"));
    assert!(cancellation.is_cancelled());
    assert!(worker.is_finished());
    assert!(stop_rx.borrow().is_some());
}

#[tokio::test]
async fn shared_shutdown_future_survives_repair_and_delivers_handoff_stop_to_sync() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let (send_signal, receive_signal) = tokio::sync::oneshot::channel::<()>();
        let signal = async {
            receive_signal.await.map_err(io::Error::other)?;
            Ok::<_, io::Error>("handoff stop")
        };
        tokio::pin!(signal);
        let cancellation = CancellationToken::new();
        let (stopping, _) = watch::channel(None);
        let state = MaintenanceState::new(RepairPhase::Inspecting);
        let mut failure = None;
        let mut server_failure = None;
        let mut worker = tokio::spawn(async { Ok::<_, eyre::Report>(42) });
        let result = supervise_worker(
            &mut worker,
            WorkerSupervision {
                cancellation: &cancellation,
                stopping: &stopping,
                state: &state,
                deadline: Instant::now() + Duration::from_secs(30),
                failure: &mut failure,
                server_failure: &mut server_failure,
            },
            signal.as_mut(),
        )
        .await;
        assert_eq!(result.unwrap(), 42);
        assert!(!cancellation.is_cancelled());
        assert!(
            signal.as_mut().now_or_never().is_none(),
            "repair must leave the shared signal future pending"
        );

        // No supervisor polls the signal while startup hands over ownership.
        // Delivery must remain buffered in the SAME receiver, not be discarded
        // with a stage-local receiver and replaced by a fresh registration.
        send_signal.send(()).unwrap();
        tokio::task::yield_now().await;
        let workers = TaskMonitor::default();
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let sync_status = std::sync::Mutex::new(logex_types::SyncStatus::default());
        let mut consensus_failure = None;
        let mut network_failure = None;
        let armed = AtomicBool::new(false);
        let mut errors = Vec::new();
        let exit = super::super::supervision::SyncSupervisor {
            on_shutdown: || armed.store(true, Ordering::SeqCst),
            node_workers: &workers,
            shutdown_tx: &shutdown,
            sync_status: &sync_status,
            consensus_storage_failure: &mut consensus_failure,
            execution_network_failure: &mut network_failure,
            shutdown_timeout: Duration::from_secs(1),
        }
        .run(
            async move {
                while !*shutdown_rx.borrow_and_update() {
                    shutdown_rx.changed().await.map_err(io::Error::other)?;
                }
                Ok::<(), io::Error>(())
            },
            signal.as_mut(),
            std::future::pending(),
            |error| errors.push(error.to_owned()),
        )
        .await;
        assert_eq!(exit, std::process::ExitCode::SUCCESS);
        assert!(armed.load(Ordering::SeqCst));
        assert!(*shutdown.borrow());
        assert!(errors.is_empty(), "{errors:?}");
    })
    .await
    .expect("finite maintenance-to-sync shutdown handoff");
}
