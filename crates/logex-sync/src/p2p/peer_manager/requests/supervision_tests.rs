//! Local lifecycle controls; no connection, discovery or remote service is started.
use super::limit_tests::Fixture;
use super::*;

async fn assert_closed_stream_is_retired(network_stream: bool) {
    for dns_enabled in [false, true] {
        let mut fixture = Fixture::new().await;
        if dns_enabled {
            fixture.manager.dns_discovery_events = Some(Box::pin(futures_util::stream::pending()));
        }
        if network_stream {
            fixture.manager.network_events = Box::pin(futures_util::stream::empty());
        } else {
            fixture.manager.discovery_events = Box::pin(futures_util::stream::empty());
        }
        assert!(
            !fixture
                .manager
                .wait_for_activity(Duration::from_secs(1))
                .await
        );
        let waiting = fixture.manager.wait_for_activity(Duration::from_secs(1));
        tokio::pin!(waiting);
        assert!(
            futures_util::poll!(&mut waiting).is_pending(),
            "an exhausted stream must not repeatedly bypass the peer-wait timer"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn supervision_closed_network_stream_is_retired() {
    assert_closed_stream_is_retired(true).await;
}

#[tokio::test(start_paused = true)]
async fn supervision_closed_discovery_stream_is_retired() {
    assert_closed_stream_is_retired(false).await;
}

struct ExitMarker(Option<tokio::sync::oneshot::Sender<()>>);

impl Drop for ExitMarker {
    fn drop(&mut self) {
        if let Some(exited) = self.0.take() {
            let _ = exited.send(());
        }
    }
}

#[tokio::test]
async fn supervision_dropping_manager_stops_owned_background_tasks() {
    let mut fixture = Fixture::new().await;
    let failure = fixture.manager.task_failure_receiver();
    let mut completions = Vec::new();
    let mut aborts = Vec::new();
    for slot in [
        &mut fixture.manager.network_task,
        &mut fixture.manager.eth_request_task,
        &mut fixture.manager.dns_discovery_task,
    ] {
        let (exited, completion) = tokio::sync::oneshot::channel();
        let marker = ExitMarker(Some(exited));
        let (started, ready) = tokio::sync::oneshot::channel();
        let task = fixture
            .manager
            .task_monitor
            .spawn("test execution worker", async move {
                let _marker = marker;
                let _ = started.send(());
                std::future::pending::<()>().await;
            });
        aborts.push(task.abort_handle());
        *slot = Some(task);
        ready.await.unwrap();
        completions.push(completion);
    }
    drop(fixture);
    // Cancellation is scheduled asynchronously; observe completion instead of
    // assuming that a single scheduler yield polls every worker.
    let all_stopped = matches!(
        tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::try_join_all(completions),
        )
        .await,
        Ok(Ok(_))
    );
    // Clean up even when the original implementation leaves tasks detached.
    for abort in aborts {
        abort.abort();
    }
    tokio::task::yield_now().await;
    assert!(failure.borrow().is_none());
    assert!(
        all_stopped,
        "dropping the owner must cancel its background tasks"
    );
}

#[tokio::test(start_paused = true)]
async fn supervision_immediate_drain_retires_all_closed_streams() {
    let mut fixture = Fixture::new().await;
    fixture.manager.network_events = Box::pin(futures_util::stream::empty());
    fixture.manager.discovery_events = Box::pin(futures_util::stream::empty());
    fixture.manager.dns_discovery_events = Some(Box::pin(futures_util::stream::empty()));
    fixture.manager.drain_events_now();
    assert!(fixture.manager.dns_discovery_events.is_none());
    let start = tokio::time::Instant::now();
    assert!(
        !fixture
            .manager
            .wait_for_activity(Duration::from_secs(1))
            .await
    );
    assert_eq!(start.elapsed(), Duration::from_secs(1));
}

#[tokio::test(start_paused = true)]
async fn supervision_shutdown_joins_workers_and_reports_missing_acknowledgement() {
    let mut fixture = Fixture::new().await;
    let failure = fixture.manager.task_failure_receiver();
    for slot in [
        &mut fixture.manager.network_task,
        &mut fixture.manager.eth_request_task,
        &mut fixture.manager.dns_discovery_task,
    ] {
        *slot = Some(
            fixture
                .manager
                .task_monitor
                .spawn("test execution worker", std::future::pending()),
        );
    }
    // The dormant local network cannot acknowledge shutdown; the existing
    // acknowledgement and drain deadlines must still let cleanup complete.
    assert!(fixture.manager.shutdown().await.is_err());
    assert!(fixture.manager.network_task.is_none());
    assert!(fixture.manager.eth_request_task.is_none());
    assert!(fixture.manager.dns_discovery_task.is_none());
    assert!(failure.borrow().is_none());
}

#[tokio::test(start_paused = true)]
async fn supervision_failure_is_visible_without_a_network_event() {
    let mut fixture = Fixture::new().await;
    let mut failure = fixture.manager.task_failure_receiver();
    fixture.manager.eth_request_task = Some(
        fixture
            .manager
            .task_monitor
            .spawn("execution request handler task", async {}),
    );
    failure.changed().await.unwrap();
    assert_eq!(
        failure.borrow().as_deref(),
        Some("execution request handler task exited unexpectedly")
    );
    // The event sender is retained by NetworkHandle; a dead worker need not
    // close an event stream or produce a network event to wake the runtime.
    assert!(
        fixture
            .manager
            .network_events
            .next()
            .now_or_never()
            .is_none()
    );
    assert!(fixture.manager.shutdown().await.is_err());
    assert!(failure.borrow().is_some());
}

#[tokio::test]
async fn cleanup_abort_preserves_expected_cancellation_and_unwind() {
    let task = tokio::spawn(std::future::pending::<()>());
    super::super::lifecycle::abort_and_wait(task, "owned pending worker")
        .await
        .unwrap();
    let task = tokio::spawn(async {
        panic!("isolated cleanup worker control");
    });
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    let error = super::super::lifecycle::abort_and_wait(task, "failed worker")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("failed worker"));
    assert!(error.to_string().contains("panicked"));
}

#[tokio::test]
async fn cleanup_abort_reports_started_blocking_work_timeout() {
    let (release, blocked) = std::sync::mpsc::channel::<()>();
    let (started, observed) = tokio::sync::oneshot::channel();
    let task = tokio::task::spawn_blocking(move || {
        started.send(()).unwrap();
        let _ = blocked.recv();
    });
    observed.await.unwrap();
    let outcome = super::super::lifecycle::abort_and_wait(task, "owned blocking worker").await;
    drop(release);
    assert!(outcome.unwrap_err().to_string().contains("abort exceeded"));
}
