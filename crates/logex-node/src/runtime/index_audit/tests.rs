//! Isolated lifecycle/corruption controls, not real-mainnet benchmark evidence.
use super::*;
use alloy_primitives::{Address, Bytes};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};

fn plan() -> Plan {
    Plan {
        version: 1,
        request_id: B256::repeat_byte(4),
        deadline_secs: 60,
        minimum_free_bytes: 1,
        max_segments: 10,
        max_total_rows: 100,
        max_segment_rows: 100,
        max_retained_source_bytes: 1 << 20,
        max_decoded_payload_bytes: 1 << 20,
        max_index_logical_bytes_per_segment: 1 << 20,
        max_manifest_bytes: 1 << 20,
    }
}

fn state(root: &Path, with_rows: bool) -> Arc<AppState> {
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: root.to_owned(),
        partition_target_rows: 2,
        ..Default::default()
    })
    .unwrap();
    if with_rows {
        let rows: Vec<_> = (0..3)
            .map(|i| LogRow {
                block_number: i,
                block_hash: B256::repeat_byte(i as u8),
                timestamp: i,
                tx_hash: B256::ZERO,
                tx_index: 0,
                log_index: 0,
                address: Address::ZERO,
                topic0: Some(B256::repeat_byte(1)),
                topic1: Some(B256::repeat_byte(2)),
                topic2: None,
                topic3: None,
                data: Bytes::new(),
                data_len: 0,
                source: Source::Receipt,
            })
            .collect();
        storage.write_batch(&rows).unwrap();
        storage.checkpoint_durable().unwrap();
        for part in storage
            .sealed_partitions()
            .iter()
            .chain(std::iter::once(storage.hot_partition()))
        {
            if part.meta.row_count > 0 {
                IndexBuilder::build_all_indexes(&part.meta.path).unwrap();
            }
        }
    }
    Arc::new(AppState::new(storage, None, Default::default()))
}

fn control(directory: &Path, p: &Plan) -> Control {
    Control {
        cancellation: CancellationToken::new(),
        deadline: Instant::now() + Duration::from_secs(60),
        last_filesystem_check: Cell::new(Instant::now() - Duration::from_secs(2)),
        calls: Cell::new(0),
        error: RefCell::new(None),
        directory: directory.to_owned(),
        request_id: p.request_id,
        minimum_free_bytes: 1,
    }
}

fn job(root: &Path, p: &Plan) -> PathBuf {
    let parent = root.join("index-audits");
    ensure_directory(&parent).unwrap();
    let directory = request_directory(root, p.request_id);
    create_private_directory(&directory).unwrap();
    save_json(&directory, "plan.json", p, false).unwrap();
    directory
}

#[test]
fn plans_reject_unknown_fields_oversized_files_and_invalid_budgets() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("plan.json");
    let p = plan();
    let original = serde_json::to_value(&p).unwrap();
    fs::write(&path, serde_json::to_vec(&original).unwrap()).unwrap();
    Plan::read(&path).unwrap();
    for (field, bad) in [
        ("version", 2.into()),
        ("request_id", serde_json::json!(B256::ZERO)),
        ("deadline_secs", 604801.into()),
        ("max_segments", 0.into()),
        ("max_segment_rows", serde_json::json!(u64::MAX)),
        ("max_manifest_bytes", 0.into()),
    ] {
        let mut v = original.clone();
        v[field] = bad;
        fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
        assert!(Plan::read(&path).is_err(), "{field}");
    }
    let mut v = original;
    v["automatically_repeat"] = true.into();
    fs::write(&path, serde_json::to_vec(&v).unwrap()).unwrap();
    assert!(Plan::read(&path).is_err());
    fs::write(&path, vec![b' '; 16 * 1024 + 1]).unwrap();
    assert!(Plan::read(&path).is_err());
}

#[test]
fn documented_plan_is_valid_and_not_an_automatic_start() {
    let doc = include_str!("../../../../../docs/index-audit.md");
    let json = doc
        .split("```json\n")
        .nth(1)
        .unwrap()
        .split("```")
        .next()
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("plan.json");
    fs::write(&file, json).unwrap();
    Plan::read(&file).unwrap();
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}

#[test]
fn complete_snapshot_checks_every_publication_and_seals_exact_manifest() {
    let temp = tempfile::tempdir().unwrap();
    let owner = state(temp.path(), true);
    let p = plan();
    let dir = job(temp.path(), &p);
    let source = owner
        .storage
        .blocking_read()
        .index_audit_snapshot(p.limits())
        .unwrap();
    let ctl = control(&dir, &p);
    let mut report = Report::new(&p, now_ms().unwrap());
    verify_snapshot(&p, &dir, &owner, &ctl, Instant::now(), &mut report, source).unwrap();
    assert!(report.complete);
    assert_eq!(report.minimum_rows_covered, 3);
    assert_eq!(report.verified_nonempty_segments, 2);
    let bytes = fs::read(dir.join("segments.jsonl")).unwrap();
    assert_eq!(report.manifest_bytes, bytes.len() as u64);
    let mut hash = blake3::Hasher::new();
    hash.update(b"logex.index-audit.segments.v1\0");
    hash.update(&bytes);
    assert_eq!(
        report.manifest_blake3,
        Some(B256::from(*hash.finalize().as_bytes()))
    );
    let rows: Vec<serde_json::Value> = bytes
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|r| r["artifacts"].as_array().unwrap().len() == 11)
    );
    assert_eq!(
        owner.storage.blocking_read().hot_partition().meta.row_count,
        1
    );
}

#[test]
fn missing_indexes_and_manifest_exhaustion_never_complete_or_rebuild() {
    for missing in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let owner = state(temp.path(), true);
        let mut p = plan();
        if !missing {
            p.max_manifest_bytes = 1;
        }
        let dir = job(temp.path(), &p);
        let source = owner
            .storage
            .blocking_read()
            .index_audit_snapshot(p.limits())
            .unwrap();
        let index = owner.storage.blocking_read().sealed_partitions()[0]
            .meta
            .path
            .join("indexes");
        if missing {
            fs::remove_file(index.join(logex_index::EVENT_BLOOM_FILE)).unwrap();
        }
        let ctl = control(&dir, &p);
        let mut report = Report::new(&p, now_ms().unwrap());
        assert!(
            verify_snapshot(&p, &dir, &owner, &ctl, Instant::now(), &mut report, source).is_err()
        );
        assert!(!report.complete);
        assert_eq!(report.manifest_blake3, None);
        if missing {
            assert!(!index.join(logex_index::EVENT_BLOOM_FILE).exists());
        }
    }
}

#[test]
fn control_and_cancellation_bind_existing_job_and_preserve_other_data() {
    let temp = tempfile::tempdir().unwrap();
    let p = plan();
    assert!(cancel_request(temp.path(), p.request_id).is_err());
    let dir = job(temp.path(), &p);
    let sentinel = temp.path().join("unrelated");
    fs::write(&sentinel, b"unchanged").unwrap();
    cancel_request(temp.path(), p.request_id).unwrap();
    cancel_request(temp.path(), p.request_id).unwrap();
    assert_eq!(
        control(&dir, &p).check(None).unwrap_err().kind(),
        io::ErrorKind::Interrupted
    );
    let mut wrong = p.clone();
    wrong.request_id = B256::repeat_byte(5);
    assert!(cancel_request(temp.path(), wrong.request_id).is_err());
    fs::write(dir.join("cancel.json"), b"invalid").unwrap();
    assert!(cancel_request(temp.path(), p.request_id).is_err());
    assert_eq!(fs::read(sentinel).unwrap(), b"unchanged");
    let mut ctl = control(&dir, &p);
    ctl.deadline = Instant::now();
    assert_eq!(ctl.check(None).unwrap_err().kind(), io::ErrorKind::TimedOut);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_joins_waiting_worker_without_stopping_node_state_or_repeating_job() {
    let temp = tempfile::tempdir().unwrap();
    let owner = state(temp.path(), false);
    let p = plan();
    let (tx, rx) = watch::channel(false);
    let task = spawn(p.clone(), temp.path().to_owned(), Arc::clone(&owner), rx);
    let dir = request_directory(temp.path(), p.request_id);
    tokio::time::timeout(Duration::from_secs(3), async {
        while !dir.join("progress.json").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tx.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    let before = fs::read(dir.join("result.json")).unwrap();
    let report: serde_json::Value = serde_json::from_slice(&before).unwrap();
    assert_eq!(report["complete"], false);
    assert_eq!(report["phase"], "failed");
    let root = temp.path().to_owned();
    let keep = Arc::clone(&owner);
    assert!(
        tokio::task::spawn_blocking(move || run(p, root, keep, CancellationToken::new()))
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(fs::read(dir.join("result.json")).unwrap(), before);
    assert_eq!(owner.storage.read().await.hot_partition().meta.row_count, 0);
}

#[cfg(unix)]
#[test]
fn cancellation_and_plan_reads_reject_symlink_targets() {
    use std::os::unix::fs::symlink;
    let temp = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let p = plan();
    symlink(outside.path(), temp.path().join("index-audits")).unwrap();
    assert!(cancel_request(temp.path(), p.request_id).is_err());
    assert_eq!(fs::read_dir(outside.path()).unwrap().count(), 0);
    let regular = outside.path().join("plan.json");
    fs::write(&regular, serde_json::to_vec(&p).unwrap()).unwrap();
    let link = temp.path().join("link.json");
    symlink(&regular, &link).unwrap();
    assert!(Plan::read(&link).is_err());
}

#[test]
fn withdrawn_publication_waits_then_verifies_without_repeating_prior_segments() {
    let temp = tempfile::tempdir().unwrap();
    let owner = state(temp.path(), true);
    let p = plan();
    let dir = job(temp.path(), &p);
    let source = owner
        .storage
        .blocking_read()
        .index_audit_snapshot(p.limits())
        .unwrap();
    let hot = owner
        .storage
        .blocking_read()
        .hot_partition()
        .meta
        .path
        .clone();
    // Ordinary builder lifecycle: beginning a rebuild withdraws its marker.
    // The verifier must wait without treating this unpublished set as corruption.
    drop(logex_storage::IndexBuildCheckpoint::begin(&hot).unwrap());
    let report = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            let ctl = control(&dir, &p);
            let mut report = Report::new(&p, now_ms().unwrap());
            let result =
                verify_snapshot(&p, &dir, &owner, &ctl, Instant::now(), &mut report, source);
            (result, report)
        });
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Ok(bytes) = fs::read(dir.join("progress.json")) {
                let progress: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                if progress["phase"] == "waiting_for_index_publication" {
                    assert_eq!(progress["verified_nonempty_segments"], 1);
                    // This independent maintenance action is not performed by the audit.
                    IndexBuilder::build_all_indexes(&hot).unwrap();
                    break;
                }
            }
            if worker.is_finished() || Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let (result, report) = worker.join().unwrap();
        result.unwrap();
        report
    });
    assert!(report.complete);
    assert_eq!(report.verified_nonempty_segments, 2);
    assert_eq!(report.minimum_rows_covered, 3);
    assert!(report.publication_retries >= 1);
    assert_eq!(report.waiting_segment, None);
    let bytes = fs::read(dir.join("segments.jsonl")).unwrap();
    let records: Vec<serde_json::Value> = bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_ne!(records[0]["segment_id"], records[1]["segment_id"]);
}

#[test]
fn publication_wait_expires_without_rebuilding_or_admitting_partial_success() {
    let temp = tempfile::tempdir().unwrap();
    let owner = state(temp.path(), true);
    let p = plan();
    let dir = job(temp.path(), &p);
    let source = owner
        .storage
        .blocking_read()
        .index_audit_snapshot(p.limits())
        .unwrap();
    let hot = owner
        .storage
        .blocking_read()
        .hot_partition()
        .meta
        .path
        .clone();
    drop(logex_storage::IndexBuildCheckpoint::begin(&hot).unwrap());
    let index = hot.join("indexes").join(logex_index::EVENT_BLOOM_FILE);
    let before = fs::read(&index).unwrap();
    let mut ctl = control(&dir, &p);
    ctl.deadline = Instant::now() + Duration::from_secs(2);
    let mut report = Report::new(&p, now_ms().unwrap());
    let error =
        verify_snapshot(&p, &dir, &owner, &ctl, Instant::now(), &mut report, source).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    assert!(!report.complete);
    assert_eq!(report.manifest_blake3, None);
    assert_eq!(report.verified_nonempty_segments, 1);
    assert!(report.publication_retries >= 1);
    assert!(report.waiting_segment.is_some());
    assert!(!hot.join("indexes/index-checkpoint").exists());
    assert_eq!(fs::read(index).unwrap(), before);
}

#[test]
fn publication_wait_stops_on_cancellation_or_source_invalidation() {
    for reorg in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let owner = state(temp.path(), true);
        let p = plan();
        let dir = job(temp.path(), &p);
        let source = owner
            .storage
            .blocking_read()
            .index_audit_snapshot(p.limits())
            .unwrap();
        let hot = owner
            .storage
            .blocking_read()
            .hot_partition()
            .meta
            .path
            .clone();
        drop(logex_storage::IndexBuildCheckpoint::begin(&hot).unwrap());
        let cancellation = CancellationToken::new();
        let (result, report) = std::thread::scope(|scope| {
            let worker = scope.spawn(|| {
                let mut ctl = control(&dir, &p);
                ctl.cancellation = cancellation.clone();
                let mut report = Report::new(&p, now_ms().unwrap());
                let result =
                    verify_snapshot(&p, &dir, &owner, &ctl, Instant::now(), &mut report, source);
                (result, report)
            });
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut waiting = false;
            while !worker.is_finished() && Instant::now() < deadline {
                if let Ok(bytes) = fs::read(dir.join("progress.json")) {
                    let progress: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    if progress["phase"] == "waiting_for_index_publication" {
                        waiting = true;
                        break;
                    }
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            if waiting && reorg {
                owner
                    .storage
                    .blocking_write()
                    .mark_non_canonical(B256::repeat_byte(2))
                    .unwrap();
            } else {
                cancellation.cancel();
            }
            let result = worker.join().unwrap();
            assert!(waiting, "worker never reached publication wait");
            result
        });
        assert_eq!(
            result.unwrap_err().kind(),
            if reorg {
                io::ErrorKind::WouldBlock
            } else {
                io::ErrorKind::Interrupted
            }
        );
        assert!(!report.complete);
        assert_eq!(report.manifest_blake3, None);
        assert_eq!(report.verified_nonempty_segments, 1);
        assert!(!hot.join("indexes/index-checkpoint").exists());
    }
}
