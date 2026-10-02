//! Isolated lifecycle and artifact controls, not mainnet benchmark evidence.
use super::*;
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_sync::history_audit::{AuditJournalLimits, AuditNetworkService};

fn plan() -> plan::Plan {
    serde_json::from_value(serde_json::json!({
        "version":1, "request_id":B256::repeat_byte(7), "scope":{"kind":"audit","from_block":0},
        "new_blocks":10, "deadline_secs":60, "network_batch_blocks":2, "minimum_free_bytes":1,
        "source":{"segments":10,"total_rows":100,"segment_rows":100,"retained_segment_bytes":1048576,"decoded_segment_bytes":1048576},
        "manifest":{"sort_records":8,"merge_fan_in":2,"scratch_bytes":1048576,"runs":100},
        "journal":{"max_bytes":1048576,"max_chunks":100,"checkpoint_blocks":2},
        "fetch":{"header_page":4,"headers":100,"transactions_per_block":100,"encoded_body_bytes":1048576,"events_per_block":100,"event_data_bytes":1048576,"request_timeout_secs":1,"attempts":1,"max_transient_retries":2,"retry_delay_secs":1}
    })).unwrap()
}
fn descriptor(p: &plan::Plan) -> Descriptor {
    Descriptor {
        version: 1,
        request_id: p.request_id,
        scope: p.scope.clone(),
        range: AuditRange {
            from: 0,
            through: 9,
        },
        anchor: ExecutionAnchor {
            block_number: 9,
            block_hash: B256::repeat_byte(1),
            receipts_root: B256::repeat_byte(2),
            beacon_slot: 100,
            beacon_root: B256::repeat_byte(3),
        },
        checkpoint: WeakSubjectivityCheckpoint {
            beacon_root: B256::repeat_byte(4),
            beacon_slot: Some(10),
        },
        journal: p.journal,
    }
}
fn state(root: &Path) -> Arc<AppState> {
    Arc::new(AppState::new(
        PartitionManager::open(PartitionManagerConfig {
            data_dir: root.to_owned(),
            ..Default::default()
        })
        .unwrap(),
        None,
        Default::default(),
    ))
}

#[test]
fn explicit_plan_rejects_unknown_fields_oversize_and_invalid_limits() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("plan.json");
    let value = serde_json::to_value(plan()).unwrap();
    fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(Invocation::read(&path, false).unwrap().plan, plan());
    for (pointer, bad) in [
        ("/version", serde_json::json!(2)),
        ("/deadline_secs", serde_json::json!(0)),
        ("/deadline_secs", serde_json::json!(604801)),
        ("/network_batch_blocks", serde_json::json!(33)),
        ("/minimum_free_bytes", serde_json::json!(0)),
        ("/fetch/max_transient_retries", serde_json::json!(33)),
        ("/fetch/retry_delay_secs", serde_json::json!(0)),
        ("/manifest/merge_fan_in", serde_json::json!(1)),
        ("/journal/checkpoint_blocks", serde_json::json!(1025)),
        ("/fetch/request_timeout_secs", serde_json::json!(61)),
        ("/request_id", serde_json::json!(B256::ZERO)),
    ] {
        let mut changed = value.clone();
        *changed.pointer_mut(pointer).unwrap() = bad;
        fs::write(&path, serde_json::to_vec(&changed).unwrap()).unwrap();
        assert!(Invocation::read(&path, false).is_err(), "{pointer}");
    }
    let mut extra = value;
    extra["fetch"]["automatic_retry_on_restart"] = true.into();
    fs::write(&path, serde_json::to_vec(&extra).unwrap()).unwrap();
    assert!(Invocation::read(&path, false).is_err());
    for bytes in [vec![], vec![b' '; 16 * 1024 + 1]] {
        fs::write(&path, bytes).unwrap();
        assert!(Invocation::read(&path, false).is_err());
    }
    assert!(Invocation::read(temp.path(), false).is_err());
}

#[test]
fn documented_pilot_is_a_valid_explicit_plan() {
    let guide = include_str!("../../../../../docs/history-audit.md");
    let json = guide
        .split("```json\n")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("pilot.json");
    fs::write(&path, json).unwrap();
    let invocation = Invocation::read(&path, false).unwrap();
    assert!(matches!(invocation.plan.scope, Scope::Pilot));
    assert!(Invocation::read(&path, true).is_err());
}

#[test]
fn resume_binds_request_scope_anchor_range_and_journal_before_work() {
    let p = plan();
    let saved = descriptor(&p);
    validate_resume(&saved, &p).unwrap();
    for field in 0..7 {
        let mut changed = saved.clone();
        match field {
            0 => changed.request_id = B256::repeat_byte(8),
            1 => changed.version = 2,
            2 => changed.range.from = 1,
            3 => changed.range.through = 8,
            4 => changed.scope = Scope::Audit { from_block: 1 },
            5 => {
                changed.journal = AuditJournalLimits {
                    max_bytes: 1,
                    ..p.journal
                }
            }
            _ => changed.range.from = 10,
        }
        assert!(validate_resume(&changed, &p).is_err());
    }
    let mut p = plan();
    p.fetch.headers = 9;
    assert!(validate_resume(&saved, &p).is_err());
    let mut p = plan();
    p.scope = Scope::Pilot;
    let mut pilot = saved;
    pilot.scope = Scope::Pilot;
    assert!(validate_resume(&pilot, &p).is_err());
}

#[test]
fn cancellation_is_owned_idempotent_and_refuses_foreign_or_missing_namespaces() {
    let temp = tempfile::tempdir().unwrap();
    let id = plan().request_id;
    assert!(cancel_request(&temp.path().join("absent"), id).is_err());
    assert!(cancel_request(temp.path(), B256::ZERO).is_err());
    let unrelated = temp.path().join("primary");
    fs::write(&unrelated, b"unchanged").unwrap();
    cancel_request(temp.path(), id).unwrap();
    cancel_request(temp.path(), id).unwrap();
    let path = cancel_path(temp.path(), id);
    assert!(cancellation_recorded(&path, id).unwrap());
    assert!(!cancellation_recorded(&cancel_path(temp.path(), B256::repeat_byte(8)), id).unwrap());
    fs::write(&path, B256::repeat_byte(8)).unwrap();
    assert!(cancel_request(temp.path(), id).is_err());
    assert_eq!(fs::read(&path).unwrap(), B256::repeat_byte(8).as_slice());
    fs::write(&path, b"truncated").unwrap();
    assert!(cancel_request(temp.path(), id).is_err());
    assert_eq!(fs::read(&unrelated).unwrap(), b"unchanged");
}

#[cfg(unix)]
#[test]
fn metadata_and_cancel_operations_refuse_symlinks() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let parent = temp.path().join("history-audits");
    symlink(outside.path(), &parent).unwrap();
    assert!(cancel_request(temp.path(), plan().request_id).is_err());
    assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
    fs::remove_file(&parent).unwrap();
    ensure_directory(&parent).unwrap();
    assert_eq!(
        fs::metadata(&parent).unwrap().permissions().mode() & 0o077,
        0
    );
    let target = outside.path().join("report.json");
    fs::write(&target, b"original").unwrap();
    symlink(&target, parent.join("report.json")).unwrap();
    assert!(save_json(&parent, "report.json", &plan(), true).is_err());
    assert!(read_json::<plan::Plan>(&parent.join("report.json")).is_err());
    assert_eq!(fs::read(&target).unwrap(), b"original");
}

#[test]
fn artifact_publication_is_atomic_and_existing_start_is_preserved() {
    let temp = tempfile::tempdir().unwrap();
    let p = plan();
    save_json(temp.path(), "request.json", &p, false).unwrap();
    let before = fs::read(temp.path().join("request.json")).unwrap();
    assert!(save_json(temp.path(), "request.json", &p, false).is_err());
    assert_eq!(fs::read(temp.path().join("request.json")).unwrap(), before);
    save_json(
        temp.path(),
        "progress.json",
        &serde_json::json!({"n":1}),
        true,
    )
    .unwrap();
    save_json(
        temp.path(),
        "progress.json",
        &serde_json::json!({"n":2}),
        true,
    )
    .unwrap();
    assert_eq!(
        read_json::<serde_json::Value>(&temp.path().join("progress.json")).unwrap()["n"],
        2
    );
    assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
    let bogus = temp.path().join("audit-bogus");
    fs::create_dir(&bogus).unwrap();
    assert!(session_directory(temp.path()).is_err());
}

#[test]
fn control_observes_cancel_disk_pressure_deadline_and_closed_storage() {
    let temp = tempfile::tempdir().unwrap();
    let owner = state(temp.path());
    let p = plan();
    let source = owner
        .storage
        .blocking_read()
        .primary_audit_snapshot(p.source_limits())
        .unwrap();
    let mut control = Control {
        cancellation: CancellationToken::new(),
        deadline: Instant::now() + Duration::from_secs(10),
        source,
        minimum_free_bytes: 1,
        cancel_path: cancel_path(temp.path(), p.request_id),
        request_id: p.request_id,
        last_space: Cell::new(Instant::now() - Duration::from_secs(6)),
        calls: Cell::new(0),
        resource_error: RefCell::new(None),
    };
    control.check().unwrap();
    control.minimum_free_bytes = u64::MAX;
    control
        .last_space
        .set(Instant::now() - Duration::from_secs(6));
    assert!(control.check().is_err());
    assert!(control.cancelled());
    control.resource_error.replace(None);
    control.minimum_free_bytes = 1;
    cancel_request(temp.path(), p.request_id).unwrap();
    control
        .last_space
        .set(Instant::now() - Duration::from_secs(6));
    assert_eq!(
        control.check().unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    fs::remove_file(&control.cancel_path).unwrap();
    control.cancellation = CancellationToken::new();
    control.deadline = Instant::now();
    assert_eq!(control.check().unwrap_err().kind(), io::ErrorKind::TimedOut);
    drop(owner);
    assert_eq!(
        control.check().unwrap_err().kind(),
        io::ErrorKind::WouldBlock,
        "source invalidation has precedence"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_or_local_cancel_joins_waiting_audit_without_stopping_storage() {
    for local_cancel in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let owner = state(temp.path());
        let consensus = Arc::new(
            ConsensusStore::open(temp.path(), Some(&format!("{:#x}", B256::repeat_byte(1))))
                .unwrap(),
        );
        let p = plan();
        let invocation = Invocation {
            plan: p.clone(),
            resume: false,
        };
        let (client, _service) = AuditNetworkService::channel(1).unwrap();
        let (shutdown, rx) = watch::channel(false);
        let task = spawn(
            invocation,
            temp.path().to_owned(),
            Arc::clone(&owner),
            consensus,
            client,
            rx,
        );
        tokio::task::yield_now().await;
        if local_cancel {
            cancel_request(temp.path(), p.request_id).unwrap();
        } else {
            shutdown.send(true).unwrap();
        }
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*shutdown.borrow(), !local_cancel);
        assert_eq!(owner.storage.read().await.total_rows(), 0);
        assert!(
            !temp
                .path()
                .join("history-audits")
                .join(format!("request-{:x}", p.request_id))
                .exists()
        );
    }
}

#[test]
fn local_cancellation_interrupts_a_long_retry_delay() {
    let temp = tempfile::tempdir().unwrap();
    let owner = state(temp.path());
    let p = plan();
    let source = owner
        .storage
        .blocking_read()
        .primary_audit_snapshot(p.source_limits())
        .unwrap();
    let control = Control {
        cancellation: CancellationToken::new(),
        deadline: Instant::now() + Duration::from_secs(30),
        source,
        minimum_free_bytes: 1,
        cancel_path: cancel_path(temp.path(), p.request_id),
        request_id: p.request_id,
        last_space: Cell::new(Instant::now()),
        calls: Cell::new(0),
        resource_error: RefCell::new(None),
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let root = temp.path().to_owned();
    let signal = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(30));
        cancel_request(&root, p.request_id).unwrap();
    });
    let started = Instant::now();
    let result = control.retry_pause(runtime.handle(), Duration::from_secs(20));
    signal.join().unwrap();
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
    assert!(
        started.elapsed() < Duration::from_secs(15),
        "local cancellation waited for the whole retry delay"
    );
    assert_eq!(owner.storage.blocking_read().total_rows(), 0);
}
