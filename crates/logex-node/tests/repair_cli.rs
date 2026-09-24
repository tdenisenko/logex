//! Offline command controls. Every child owns only disposable test storage.
use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    time::{Duration, Instant, SystemTime},
};

use alloy_primitives::{Address, B256, bytes};
use logex_index::{IndexBuildProfile, IndexBuilder};
use logex_storage::{PartitionManager, PartitionManagerConfig, SegmentReader};
use logex_types::{LogRow, Source};
use serde_json::Value;

fn run(root: &Path, arguments: &[&str]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_logex"))
        .arg("--data-dir")
        .arg(root)
        .args(arguments)
        .env("RUST_LOG", "info,logex_index=debug")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    // Drain both pipes concurrently so tracing cannot block child termination.
    let out = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stdout.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let err = std::thread::spawn(move || {
        let mut bytes = Vec::new();
        stderr.read_to_end(&mut bytes).unwrap();
        bytes
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    let (status, timed_out) = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break (status, false);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            break (child.wait().unwrap(), true);
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let output = Output {
        status,
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
    };
    assert!(
        !timed_out,
        "CLI exceeded finite test budget: {}",
        text(&output)
    );
    output
}

fn text(output: &Output) -> String {
    format!(
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn json(output: &Output, code: i32) -> Value {
    assert_eq!(output.status.code(), Some(code), "{}", text(output));
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout must contain only JSON, including with tracing enabled: {error}: {}",
            text(output)
        )
    })
}

type Snapshot = BTreeMap<PathBuf, (SystemTime, Option<Vec<u8>>)>;
fn snapshot(root: &Path) -> Snapshot {
    fn visit(root: &Path, path: &Path, output: &mut Snapshot) {
        let metadata = fs::symlink_metadata(path).unwrap();
        output.insert(
            path.strip_prefix(root).unwrap().to_owned(),
            (
                metadata.modified().unwrap(),
                metadata.is_file().then(|| fs::read(path).unwrap()),
            ),
        );
        if metadata.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                visit(root, &entry.unwrap().path(), output);
            }
        }
    }
    let mut output = Snapshot::new();
    visit(root, root, &mut output);
    output
}

fn config(root: &Path) -> PartitionManagerConfig {
    PartitionManagerConfig {
        data_dir: root.to_owned(),
        ..Default::default()
    }
}

fn fixture(root: &Path, indexes: bool) -> (PathBuf, Vec<LogRow>) {
    let mut storage = PartitionManager::open(config(root)).unwrap();
    let rows: Vec<_> = (0..2)
        .map(|index| LogRow {
            block_number: 100 + index,
            block_hash: B256::repeat_byte(index as u8 + 1),
            timestamp: 1200 + index,
            tx_hash: B256::repeat_byte(index as u8 + 3),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(5),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: bytes!("abcd"),
            data_len: 2,
            source: Source::Receipt,
        })
        .collect();
    storage.write_batch(&rows).unwrap();
    storage.checkpoint_durable().unwrap();
    let segment = storage.hot_partition().meta.path.clone();
    drop(storage);
    if indexes {
        IndexBuilder::build_all_indexes(&segment).unwrap();
    }
    (segment, rows)
}

fn assert_no_network_or_consensus_artifacts(root: &Path) {
    for name in ["cl", "discovery-secret", "known-peers.json"] {
        assert!(
            !root.join(name).exists(),
            "unexpected lazy-source artifact {name}"
        );
    }
}

#[test]
fn dry_run_json_exit_codes_preserve_verified_blocked_and_pending_evidence() {
    for case in 0..4 {
        let temp = tempfile::tempdir().unwrap();
        fixture(temp.path(), case == 0 || case == 1);
        if case == 3 {
            fs::write(temp.path().join("wal/pending.wal"), [1, 2, 3]).unwrap();
        }
        let before = snapshot(temp.path());
        let arguments = if case == 1 {
            vec!["repair", "--dry-run", "--repair-max-segment-rows", "1"]
        } else {
            vec!["repair", "--dry-run"]
        };
        let output = run(temp.path(), &arguments);
        let value = json(&output, [0, 1, 2, 2][case]);
        assert_eq!(snapshot(temp.path()), before, "case {case}");
        assert_eq!(value["blocked"], case == 1);
        assert_eq!(value["requires_repair"], case != 0);
        if case == 3 {
            assert_eq!(value["kind"], "recovery_required");
            assert!(
                value["recovery_prerequisites"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|path| path.as_str().unwrap().ends_with("pending.wal"))
            );
        } else {
            assert_eq!(value["kind"], "inspected");
        }
        assert_no_network_or_consensus_artifacts(temp.path());
    }
}

#[test]
fn dry_run_missing_and_locked_roots_fail_without_creating_or_changing_files() {
    let temp = tempfile::tempdir().unwrap();
    let missing = temp.path().join("missing");
    let before = snapshot(temp.path());
    let output = run(&missing, &["repair", "--dry-run"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    assert!(output.stdout.is_empty(), "{}", text(&output));
    assert_eq!(snapshot(temp.path()), before);
    let owner = PartitionManager::open(config(temp.path())).unwrap();
    let before = snapshot(temp.path());
    let output = run(temp.path(), &["repair", "--dry-run"]);
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("already in use"),
        "{}",
        text(&output)
    );
    assert!(output.stdout.is_empty());
    assert_eq!(snapshot(temp.path()), before);
    drop(owner);
}

#[test]
fn local_index_repair_reports_quarantine_without_initializing_peer_or_consensus_state() {
    let temp = tempfile::tempdir().unwrap();
    let (segment, rows) = fixture(temp.path(), true);
    // Retain a corrupt derived artifact so successful repair has real originals
    // to quarantine. Primary data and its published commitment remain intact.
    let index = segment.join("indexes/address.bptree");
    assert!(index.is_file());
    fs::write(&index, b"retained corrupt index fixture").unwrap();
    let catalog =
        logex_storage::native::StorageCatalogPaths::new(temp.path().to_owned()).catalog_path();
    let catalog_before = fs::read(&catalog).unwrap();
    let output = run(
        temp.path(),
        &[
            "repair",
            "--http-port",
            "0",
            "--nat",
            "invalid-offline-fixture",
            "--repair-timeout-secs",
            "30",
        ],
    );
    let value = json(&output, 0);
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("built address index"),
        "tracing must be on stderr: {}",
        text(&output)
    );
    assert_eq!(value["requires_repair"], false);
    let quarantines = value["quarantine_dirs"].as_array().unwrap();
    assert!(!quarantines.is_empty(), "{value}");
    assert!(quarantines.iter().any(|directory| {
        snapshot(Path::new(directory.as_str().unwrap()))
            .values()
            .any(|(_, bytes)| {
                bytes.as_deref() == Some(b"retained corrupt index fixture".as_slice())
            })
    }));
    assert_eq!(fs::read(&catalog).unwrap(), catalog_before);
    assert_eq!(
        SegmentReader::open(&segment)
            .unwrap()
            .read_log_rows(None)
            .unwrap(),
        rows
    );
    IndexBuilder::verify_indexes(&segment, IndexBuildProfile::All).unwrap();
    assert_no_network_or_consensus_artifacts(temp.path());
    let before = snapshot(temp.path());
    let report = json(&run(temp.path(), &["repair", "--dry-run"]), 0);
    assert_eq!(report["requires_repair"], false);
    assert_eq!(snapshot(temp.path()), before);
    let storage = PartitionManager::open(config(temp.path())).unwrap();
    assert_eq!(storage.total_rows(), 2);
    assert_eq!(
        SegmentReader::open(&segment)
            .unwrap()
            .read_log_rows(None)
            .unwrap(),
        rows
    );
}

#[test]
fn repair_rejects_new_trust_and_public_unauthenticated_http_before_data_creation() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("absent-data");
    let checkpoint = format!("0x{}", "11".repeat(32));
    for arguments in [
        vec!["--checkpoint", checkpoint.as_str(), "repair", "--dry-run"],
        vec![
            "--checkpoint-sync-url",
            "http://127.0.0.1:1",
            "repair",
            "--dry-run",
        ],
        vec!["repair", "--http-host", "0.0.0.0", "--http-port", "0"],
    ] {
        let before = snapshot(temp.path());
        let output = run(&root, &arguments);
        assert_eq!(output.status.code(), Some(1), "{}", text(&output));
        assert_eq!(snapshot(temp.path()), before);
        assert!(!root.exists());
        assert!(output.stdout.is_empty());
    }
    let config = temp.path().join("repair.toml");
    fs::write(&config, format!("checkpoint = '{checkpoint}'\n")).unwrap();
    let before = snapshot(temp.path());
    let output = run(
        &root,
        &["--config", config.to_str().unwrap(), "repair", "--dry-run"],
    );
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    assert_eq!(snapshot(temp.path()), before);
    assert!(!root.exists());
}

#[test]
fn opt_in_sync_refuses_unidentified_artifacts_before_normal_initialization() {
    let temp = tempfile::tempdir().unwrap();
    fs::create_dir(temp.path().join("segments")).unwrap();
    fs::write(
        temp.path().join("segments/original"),
        b"unidentified original rows",
    )
    .unwrap();
    let before = snapshot(temp.path());
    let output = run(
        temp.path(),
        &[
            "sync",
            "--repair-corrupt-segments",
            "--http-port",
            "0",
            "--nat",
            "invalid-offline-fixture",
        ],
    );
    assert_eq!(output.status.code(), Some(1), "{}", text(&output));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no catalog"),
        "{}",
        text(&output)
    );
    assert_eq!(snapshot(temp.path()), before);
    assert_no_network_or_consensus_artifacts(temp.path());
}
