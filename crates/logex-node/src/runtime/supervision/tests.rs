use super::*;
use std::future::{pending, ready};
use std::path::PathBuf;

struct Fixture {
    workers: logex_sync::tasks::TaskMonitor,
    shutdown: watch::Sender<bool>,
    status: Arc<Mutex<SyncStatus>>,
    consensus: Option<watch::Receiver<Option<Arc<str>>>>,
    execution: Option<watch::Receiver<Option<Arc<str>>>>,
}

impl Fixture {
    fn new() -> Self {
        Self {
            workers: Default::default(),
            shutdown: watch::channel(false).0,
            status: Arc::new(Mutex::new(SyncStatus {
                syncing: true,
                consensus_head_fresh: Some(true),
                eta_seconds: Some(10.0),
                historical_eta_seconds: Some(20.0),
                ..Default::default()
            })),
            consensus: None,
            execution: None,
        }
    }

    fn supervisor(&mut self) -> SyncSupervisor<'_> {
        SyncSupervisor {
            node_workers: &self.workers,
            shutdown_tx: &self.shutdown,
            sync_status: &self.status,
            consensus_storage_failure: &mut self.consensus,
            execution_network_failure: &mut self.execution,
            shutdown_timeout: Duration::from_secs(2),
        }
    }

    fn assert_stopped(&self) {
        let status = self.status.lock().unwrap();
        assert!(!status.syncing);
        assert!(status.eta_seconds.is_none());
        assert!(status.historical_eta_seconds.is_none());
        assert_eq!(status.node_state, logex_types::NodeState::Disconnected);
    }
}

async fn engine_after_shutdown(
    mut shutdown: watch::Receiver<bool>,
    result: Result<(), &'static str>,
) -> Result<(), &'static str> {
    while !*shutdown.borrow_and_update() {
        shutdown.changed().await.unwrap();
    }
    result
}

fn low_disk() -> LowDiskSpace {
    LowDiskSpace {
        path: PathBuf::from("isolated-test-storage"),
        free_bytes: 5,
        min_free_bytes: 10,
    }
}

#[tokio::test]
async fn engine_error_is_a_failure() {
    let mut fixture = Fixture::new();
    let _shutdown = fixture.shutdown.subscribe();
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(
            ready(Err::<(), _>("local storage write failed")),
            pending(),
            pending(),
            |message| failures.push(message.to_owned()),
        )
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("local storage write failed"));
    fixture.assert_stopped();
}

#[tokio::test]
async fn low_disk_is_a_failure_after_cooperative_shutdown() {
    let mut fixture = Fixture::new();
    let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Ok(()));
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(engine, pending(), ready(low_disk()), |message| {
            failures.push(message.to_owned())
        })
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(failures.len(), 1);
    fixture.assert_stopped();
}

#[tokio::test]
async fn engine_error_during_signal_shutdown_is_a_failure() {
    let mut fixture = Fixture::new();
    let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Err("final write failed"));
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(engine, ready("test signal"), pending(), |message| {
            failures.push(message.to_owned())
        })
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(failures.len(), 1);
    fixture.assert_stopped();
}

#[tokio::test]
async fn engine_shutdown_timeout_is_a_failure() {
    let mut fixture = Fixture::new();
    let _shutdown = fixture.shutdown.subscribe();
    let mut failures = vec![];
    let mut supervisor = fixture.supervisor();
    supervisor.shutdown_timeout = Duration::ZERO;
    let exit = supervisor
        .run(
            pending::<Result<(), &str>>(),
            ready("test signal"),
            pending(),
            |message| failures.push(message.to_owned()),
        )
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(failures.len(), 1);
    fixture.assert_stopped();
}

#[tokio::test]
async fn normal_signal_shutdown_is_successful() {
    let mut fixture = Fixture::new();
    let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Ok(()));
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(engine, ready("test signal"), pending(), |message| {
            failures.push(message.to_owned())
        })
        .await;
    assert_eq!(exit, ExitCode::SUCCESS);
    assert!(failures.is_empty());
    assert!(*fixture.shutdown.borrow());
}

#[tokio::test]
async fn normal_engine_completion_is_successful() {
    let mut fixture = Fixture::new();
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(ready(Ok::<(), &str>(())), pending(), pending(), |message| {
            failures.push(message.to_owned())
        })
        .await;
    assert_eq!(exit, ExitCode::SUCCESS);
    assert!(failures.is_empty());
}

#[tokio::test]
async fn first_failure_arms_cleanup_before_status_and_engine_shutdown() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let mut fixture = Fixture::new();
    let status = Arc::clone(&fixture.status);
    let armed = Arc::new(AtomicBool::new(false));
    let engine_armed = Arc::clone(&armed);
    let mut shutdown = fixture.shutdown.subscribe();
    let callback_shutdown = shutdown.clone();
    let engine = async move {
        shutdown.changed().await.unwrap();
        assert!(*shutdown.borrow());
        assert!(engine_armed.load(Ordering::SeqCst));
        Err::<(), _>("second error during cleanup")
    };
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(engine, pending(), ready(low_disk()), |message| {
            assert!(status.lock().unwrap().syncing);
            assert!(!*callback_shutdown.borrow());
            armed.store(true, Ordering::SeqCst);
            failures.push(message.to_owned());
        })
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(
        failures.len(),
        1,
        "cleanup deadline must not restart for a second error"
    );
    assert!(failures[0].contains("disk space"));
    fixture.assert_stopped();
}

#[tokio::test]
async fn latched_runtime_failure_preserves_failure_exit_and_consensus_status() {
    for consensus in [false, true] {
        let mut fixture = Fixture::new();
        let (_sender, receiver) = watch::channel(Some(Arc::<str>::from("worker stopped")));
        if consensus {
            fixture.consensus = Some(receiver);
        } else {
            fixture.execution = Some(receiver);
        }
        let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Ok(()));
        let mut failures = vec![];
        let exit = fixture
            .supervisor()
            .run(engine, pending(), pending(), |message| {
                failures.push(message.to_owned())
            })
            .await;
        assert_eq!(exit, ExitCode::FAILURE);
        assert_eq!(failures.len(), 1);
        fixture.assert_stopped();
        assert_eq!(
            fixture.status.lock().unwrap().consensus_head_fresh,
            Some(!consensus)
        );
    }
}

#[tokio::test]
async fn runtime_failure_wins_over_simultaneously_ready_normal_exit() {
    let mut fixture = Fixture::new();
    let (_sender, receiver) = watch::channel(Some(Arc::<str>::from("storage stopped")));
    fixture.consensus = Some(receiver);
    let _shutdown = fixture.shutdown.subscribe();
    let exit = fixture
        .supervisor()
        .run(
            ready(Ok::<(), &str>(())),
            ready("test signal"),
            pending(),
            |_| {},
        )
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(
        fixture.status.lock().unwrap().consensus_head_fresh,
        Some(false)
    );
}

#[tokio::test]
async fn new_execution_failure_stops_a_waiting_engine() {
    let mut fixture = Fixture::new();
    let (sender, receiver) = watch::channel(None);
    fixture.execution = Some(receiver);
    let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Ok(()));
    let mut failures = vec![];
    let mut run = pin!(
        fixture
            .supervisor()
            .run(engine, pending(), pending(), |message| failures
                .push(message.to_owned()))
    );
    assert!(futures_util::poll!(&mut run).is_pending());
    sender
        .send(Some(Arc::from("local network worker stopped")))
        .unwrap();
    assert_eq!(run.await, ExitCode::FAILURE);
}

#[tokio::test]
async fn low_disk_timeout_keeps_the_original_cleanup_deadline() {
    let mut fixture = Fixture::new();
    let _shutdown = fixture.shutdown.subscribe();
    let mut failures = vec![];
    let mut supervisor = fixture.supervisor();
    supervisor.shutdown_timeout = Duration::ZERO;
    let exit = supervisor
        .run(
            pending::<Result<(), &str>>(),
            pending(),
            ready(low_disk()),
            |message| failures.push(message.to_owned()),
        )
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("disk space"));
}

#[test]
fn failure_watchdog_observes_an_already_set_latch_after_sender_drop() {
    let (_, receiver) = watch::channel(Some(Arc::<str>::from("sync engine stopped")));
    let (expired, observed) = std::sync::mpsc::channel();
    let _watchdog =
        super::super::start_runtime_failure_watchdog(receiver, Duration::ZERO, move || {
            expired.send(()).unwrap();
        })
        .unwrap();
    // The real watchdog reports through a local channel instead of exiting.
    observed.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test]
fn interrupted_status_update_does_not_interrupt_failure_cleanup() {
    let status = Mutex::new(SyncStatus {
        syncing: true,
        ..Default::default()
    });
    // Poison only this local status fixture to represent an interrupted update.
    let interrupted = std::panic::catch_unwind(|| {
        let _guard = status.lock().unwrap();
        panic!("simulated interrupted status update");
    });
    assert!(interrupted.is_err());
    assert!(status.is_poisoned());
    mark_sync_stopped_for_runtime_failure(&status, false);
    let status = status.lock().unwrap_err().into_inner();
    assert!(!status.syncing);
    assert_eq!(status.node_state, logex_types::NodeState::Disconnected);
}

#[tokio::test]
async fn engine_future_panic_enters_failure_cleanup() {
    async fn interrupted_engine() -> Result<(), &'static str> {
        panic!("simulated local engine interruption");
    }
    let mut fixture = Fixture::new();
    let shutdown = fixture.shutdown.subscribe();
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(interrupted_engine(), pending(), pending(), |message| {
            failures.push(message.to_owned())
        })
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(failures.len(), 1);
    assert!(failures[0].contains("panicked"));
    assert!(*shutdown.borrow());
    fixture.assert_stopped();
}

#[tokio::test]
async fn interrupted_engine_status_during_low_disk_shutdown_remains_a_failure() {
    let mut fixture = Fixture::new();
    let status = Arc::clone(&fixture.status);
    assert!(
        std::panic::catch_unwind(|| {
            let _guard = status.lock().unwrap();
            panic!("simulated interrupted telemetry update");
        })
        .is_err()
    );
    let mut shutdown = fixture.shutdown.subscribe();
    let engine = async move {
        shutdown.changed().await.unwrap();
        // A remaining engine status reader can still observe the poison.
        let _guard = status.lock().unwrap();
        Ok::<(), &'static str>(())
    };
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(engine, pending(), ready(low_disk()), |message| {
            failures.push(message.to_owned())
        })
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(failures.len(), 1);
    let status = fixture.status.lock().unwrap_err().into_inner();
    assert!(!status.syncing);
}

#[tokio::test]
async fn final_engine_progress_cannot_restore_healthy_failure_status() {
    let mut fixture = Fixture::new();
    let status = Arc::clone(&fixture.status);
    let mut shutdown = fixture.shutdown.subscribe();
    let engine = async move {
        shutdown.changed().await.unwrap();
        let mut status = status.lock().unwrap();
        status.syncing = true;
        status.eta_seconds = Some(5.0);
        status.node_state = logex_types::NodeState::default();
        Ok::<(), &'static str>(())
    };
    let exit = fixture
        .supervisor()
        .run(engine, pending(), ready(low_disk()), |_| {})
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    fixture.assert_stopped();
}

fn service_state(path: &std::path::Path) -> Arc<logex_server::AppState> {
    Arc::new(logex_server::AppState::new(
        logex_storage::PartitionManager::open(logex_storage::PartitionManagerConfig {
            data_dir: path.to_owned(),
            ..Default::default()
        })
        .unwrap(),
        None,
        SyncStatus::default(),
    ))
}

#[tokio::test]
async fn http_bind_failure_stops_sync() {
    listener_failure_stops_sync(false).await;
}

#[tokio::test]
async fn grpc_bind_failure_stops_sync() {
    listener_failure_stops_sync(true).await;
}

async fn listener_failure_stops_sync(grpc: bool) {
    let tmp = tempfile::tempdir().unwrap();
    let state = service_state(tmp.path());
    // Own the ephemeral loopback port for the entire attempt, avoiding a
    // release/rebind race. No requests or external connections are made.
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = occupied.local_addr().unwrap();
    let mut fixture = Fixture::new();
    let task = if grpc {
        super::super::services::spawn_grpc(
            &fixture.workers,
            state,
            addr,
            fixture.shutdown.subscribe(),
        )
    } else {
        super::super::services::spawn_http(
            &fixture.workers,
            state,
            addr,
            fixture.shutdown.subscribe(),
            Default::default(),
        )
    };
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    let failures = fixture.workers.subscribe();
    let failure = failures.borrow().clone().unwrap();
    assert!(failure.starts_with(if grpc {
        "gRPC server failed:"
    } else {
        "HTTP server failed:"
    }));
    let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Ok(()));
    let outcome = tokio::time::timeout(
        Duration::from_secs(1),
        fixture
            .supervisor()
            .run(engine, pending(), pending(), |_| {}),
    )
    .await;
    assert_eq!(
        outcome.expect("listener failure must stop sync"),
        ExitCode::FAILURE
    );
}

#[tokio::test]
async fn requested_shutdown_stops_services_without_false_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let state = service_state(tmp.path());
    let mut fixture = Fixture::new();
    let http = super::super::services::spawn_http(
        &fixture.workers,
        Arc::clone(&state),
        "127.0.0.1:0".parse().unwrap(),
        fixture.shutdown.subscribe(),
        Default::default(),
    );
    let grpc = super::super::services::spawn_grpc(
        &fixture.workers,
        Arc::clone(&state),
        "127.0.0.1:0".parse().unwrap(),
        fixture.shutdown.subscribe(),
    );
    let indexer = fixture.workers.spawn_result(
        "background indexer",
        crate::background::run_background_indexer(state, fixture.shutdown.subscribe()),
    );
    let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Ok(()));
    let exit = fixture
        .supervisor()
        .run(engine, ready("test signal"), pending(), |_| {})
        .await;
    assert_eq!(exit, ExitCode::SUCCESS);
    for task in [http, grpc, indexer] {
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
    }
    assert!(fixture.workers.subscribe().borrow().is_none());
}

#[tokio::test]
async fn worker_failure_before_supervisor_poll_arms_independent_watchdog() {
    let fixture = Fixture::new();
    let (expired, observed) = std::sync::mpsc::channel();
    let _watchdog = super::super::start_runtime_failure_watchdog(
        fixture.workers.subscribe(),
        Duration::ZERO,
        move || expired.send(()).unwrap(),
    )
    .unwrap();
    fixture
        .workers
        .spawn_result("startup service", async {
            Err::<(), _>("isolated startup failure")
        })
        .await
        .unwrap();
    // The main runtime is deliberately not polling the supervisor here.
    observed.recv_timeout(Duration::from_secs(2)).unwrap();
}

#[tokio::test]
async fn unexpected_worker_return_stops_engine_and_marks_failure() {
    let mut fixture = Fixture::new();
    fixture.workers.spawn("indexer", async {}).await.unwrap();
    let engine = engine_after_shutdown(fixture.shutdown.subscribe(), Ok(()));
    let mut failures = vec![];
    let exit = fixture
        .supervisor()
        .run(engine, pending(), pending(), |message| {
            failures.push(message.to_owned());
        })
        .await;
    assert_eq!(exit, ExitCode::FAILURE);
    assert_eq!(
        failures,
        ["node worker failed: indexer exited unexpectedly"]
    );
    fixture.assert_stopped();
}

#[tokio::test]
async fn explicit_worker_failure_during_engine_cleanup_remains_latched() {
    let mut fixture = Fixture::new();
    let (fail, waiting) = tokio::sync::oneshot::channel();
    let worker = fixture.workers.spawn_result("service", async move {
        waiting.await.unwrap();
        Err::<(), _>("service cleanup failed")
    });
    let mut shutdown = fixture.shutdown.subscribe();
    let engine = async move {
        shutdown.changed().await.unwrap();
        fail.send(()).unwrap();
        worker.await.unwrap();
        Ok::<(), String>(())
    };
    let _ = fixture
        .supervisor()
        .run(engine, ready("test signal"), pending(), |_| {})
        .await;
    // run_sync checks this permanent latch again after the shared cleanup;
    // the independently armed worker watchdog stays alive until process exit.
    assert_eq!(
        fixture.workers.subscribe().borrow().as_deref(),
        Some("service failed: service cleanup failed")
    );
}
