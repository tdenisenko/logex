//! Offline CLI controls: each child inspects only its test's temporary directory.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use logex_cl::ConsensusStore;
use logex_storage::{PartitionManager, PartitionManagerConfig};

fn config(path: &Path) -> PartitionManagerConfig {
    PartitionManagerConfig {
        data_dir: path.to_owned(),
        ..Default::default()
    }
}

fn info(path: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_logex"))
        .arg("--data-dir")
        .arg(path)
        .arg("info")
        .env("RUST_LOG", "error")
        .output()
        .unwrap()
}

fn output_text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn artifacts(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, result: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(root, &entry.path(), result);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                result.insert(
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(entry.path()).unwrap(),
                );
            }
        }
    }
    let mut result = BTreeMap::new();
    visit(root, root, &mut result);
    result
}

fn unavailable_consensus(path: &Path) {
    std::fs::create_dir(path.join("cl")).unwrap();
    std::fs::write(path.join("cl/consensus_state.json"), b"old fixture state").unwrap();
}

#[test]
fn info_checks_exclusive_ownership_before_reading_consensus() {
    let temp = tempfile::tempdir().unwrap();
    let owner = PartitionManager::open(config(temp.path())).unwrap();
    unavailable_consensus(temp.path());
    let before = artifacts(temp.path());

    let output = info(temp.path());
    assert_eq!(artifacts(temp.path()), before);
    assert_eq!(output.status.code(), Some(1));
    let text = output_text(&output);
    assert!(text.contains("already in use"), "{text}");
    assert!(!text.contains("failed to open consensus state"), "{text}");
    drop(owner);
}

#[test]
fn info_preserves_unavailable_consensus_and_reports_failure() {
    let temp = tempfile::tempdir().unwrap();
    drop(PartitionManager::open(config(temp.path())).unwrap());
    unavailable_consensus(temp.path());
    let before = artifacts(temp.path());

    let output = info(temp.path());
    assert_eq!(artifacts(temp.path()), before);
    assert_eq!(output.status.code(), Some(1));
    assert!(output_text(&output).contains("failed to open consensus state"));
    // A failed inspection releases the owner for the next maintenance command.
    assert!(PartitionManager::open(config(temp.path())).is_ok());
}

#[test]
fn info_reports_storage_with_and_without_consensus() {
    let temp = tempfile::tempdir().unwrap();
    drop(PartitionManager::open(config(temp.path())).unwrap());
    for with_consensus in [false, true] {
        let checkpoint = format!("0x{}", "11".repeat(32));
        if with_consensus {
            drop(ConsensusStore::open(temp.path(), Some(&checkpoint)).unwrap());
        }
        let before = artifacts(temp.path());
        let output = info(temp.path());
        assert_eq!(artifacts(temp.path()), before);
        assert!(output.status.success(), "{}", output_text(&output));
        let text = output_text(&output);
        assert!(text.contains("LogEx Storage Info"), "{text}");
        assert!(text.contains("Total rows:         0"), "{text}");
        assert_eq!(text.contains("Checkpoint root:"), with_consensus);
        if with_consensus {
            assert!(text.contains(&checkpoint), "{text}");
        }
    }
}
