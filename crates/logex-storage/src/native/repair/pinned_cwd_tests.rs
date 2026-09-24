//! Cwd changes are isolated in child processes; no mount or live path is touched.
use super::super::tests::{data_id, fixture, inspect, inspection_limits, limits, tree};
use super::*;
use crate::{IndexBuildCheckpoint, IndexReadCheckpoint};

const CHILD_CASE: &str = "LOGEX_PINNED_REPAIR_CWD_CASE";
const CHILD_TEST: &str = "native::repair::publication::pinned_cwd_tests::pinned_cwd_child";
const INDEX: &str = "control.index";

fn build_index(source: &Path, indexes: &Path) -> io::Result<()> {
    let mut checkpoint = IndexBuildCheckpoint::begin_fresh_at(source, indexes)?;
    fs::write(indexes.join(INDEX), b"derived control")?;
    checkpoint.register_artifact(INDEX, [7; 16])?;
    checkpoint.publish()
}

fn verify_index(source: &Path, indexes: &Path) -> io::Result<()> {
    let reader = SegmentReader::open_for_inspection(source)?;
    let checkpoint = IndexReadCheckpoint::open_at(indexes, &reader)?
        .ok_or_else(|| invalid("missing source-bound index checkpoint"))?;
    if checkpoint.artifact_id(INDEX) != Some([7; 16])
        || fs::read(indexes.join(INDEX))? != b"derived control"
    {
        return Err(invalid("index control differs"));
    }
    Ok(())
}

fn prepared<'a>(plan: &'a RepairOwnershipPlan, rows: &[LogRow]) -> PreparedRepairPublication<'a> {
    let mut verifier = plan.begin_candidate(plan.segment_ids()[0]).unwrap();
    verifier.append(rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let mut publication = plan.begin_publication(0).unwrap();
    let stage = publication
        .stage_candidate(&candidate, rows, inspection_limits())
        .unwrap();
    publication.prepare(vec![stage], |_| Ok(())).unwrap()
}

fn replace_ancestor(parent: &Path) -> PathBuf {
    fs::rename(parent.join("volume"), parent.join("retained-volume")).unwrap();
    let substitute = parent.join("volume/data");
    fs::create_dir_all(&substitute).unwrap();
    fs::write(
        substitute.join("unrelated"),
        b"do not touch replacement directory",
    )
    .unwrap();
    substitute
}

#[test]
fn pinned_cwd_repair_survives_ancestor_replacement() {
    for case in [
        "index",
        "pending-index",
        "wal",
        "before-publication",
        "after-publication",
    ] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env(CHILD_CASE, case)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{case}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "child control did not run"
        );
    }
}

#[test]
fn pinned_cwd_child() {
    let Ok(case) = std::env::var(CHILD_CASE) else {
        return;
    };
    let parent = tempfile::tempdir().unwrap();
    let volume = parent.path().join("volume");
    let data = volume.join("data");
    fs::create_dir(&volume).unwrap();
    let (fixture, rows) = fixture(false, 100, true);
    fs::rename(fixture.path(), &data).unwrap();
    std::env::set_current_dir(&data).unwrap();
    let root = Path::new(".");

    if case == "wal" {
        fs::write(root.join("wal/pending.wal"), [0; 3]).unwrap();
        let inspection = inspect(root);
        let catalog = inspection.catalog.clone();
        let substitute = replace_ancestor(parent.path());
        let unchanged = tree(&substitute);
        let inspection = inspection
            .recover(crate::native::NativeRecoveryLimits {
                primary: inspection_limits(),
                wal: crate::WalReadLimits {
                    max_bytes: 1024,
                    max_rows: 100,
                },
            })
            .unwrap();
        assert!(inspection.recovery_prerequisites.is_empty());
        assert_eq!(inspection.catalog, catalog);
        assert_eq!(fs::metadata(root.join("wal/pending.wal")).unwrap().len(), 0);
        let reader =
            SegmentReader::open_for_inspection(&inspection.paths.segment_dir(data_id(&inspection)))
                .unwrap();
        let ids: Vec<_> = (0..rows.len() as u32).collect();
        assert_eq!(reader.read_log_rows(Some(&ids)).unwrap(), rows);
        assert_eq!(tree(&substitute), unchanged);
        drop(inspection);
    } else if matches!(case.as_str(), "index" | "pending-index") {
        let RepairInspection::Primary(primary) = inspect_repair(root, inspection_limits()).unwrap()
        else {
            panic!("expected primary owner")
        };
        let id = data_id(&primary);
        let catalog = primary.catalog.clone();
        let original = tree(&primary.paths.segment_dir(id));
        let repair = if case == "pending-index" {
            drop(
                primary
                    .begin_index_repair(&[id], &[INDEX], inspection_limits(), 0)
                    .unwrap(),
            );
            let RepairInspection::PendingIndexes(pending) =
                inspect_repair(root, inspection_limits()).unwrap()
            else {
                panic!("expected pending index owner")
            };
            let substitute = replace_ancestor(parent.path());
            let unchanged = tree(&substitute);
            let mut repair = pending.resume(inspection_limits()).unwrap();
            repair.execute(0, build_index, verify_index).unwrap();
            assert_eq!(tree(&substitute), unchanged);
            repair
        } else {
            let substitute = replace_ancestor(parent.path());
            let unchanged = tree(&substitute);
            let mut repair = primary
                .begin_index_repair(&[id], &[INDEX], inspection_limits(), 0)
                .unwrap();
            repair.execute(0, build_index, verify_index).unwrap();
            assert_eq!(tree(&substitute), unchanged);
            repair
        };
        // `into_inspection` keeps the same directory owner after publication.
        let inspection = repair.into_inspection().unwrap();
        assert_eq!(inspection.catalog, catalog);
        let source = inspection.paths.segment_dir(id);
        verify_index(&source, &source.join("indexes")).unwrap();
        for (path, (_, bytes)) in original {
            if let Some(bytes) = bytes {
                assert_eq!(fs::read(source.join(path)).unwrap(), bytes);
            }
        }
        drop(inspection);
    } else {
        let inspection = inspect(root);
        let id = data_id(&inspection);
        let original_tree = tree(&inspection.paths.segment_dir(id));
        let plan = inspection.into_repair_plan(&[id], limits()).unwrap();
        match case.as_str() {
            "before-publication" => drop(plan.begin_publication(0).unwrap()),
            "after-publication" => drop(prepared(&plan, &rows).commit().unwrap()),
            _ => panic!("unknown child case"),
        }
        drop(plan);
        let pending = inspect_pending_repair(root).unwrap().unwrap();
        let substitute = replace_ancestor(parent.path());
        let unchanged = tree(&substitute);
        let quarantine = match pending.state() {
            RepairCatalogState::BeforePublication => {
                let plan = pending.into_plan(inspection_limits(), limits()).unwrap();
                prepared(&plan, &rows)
                    .commit()
                    .unwrap()
                    .finish(inspection_limits(), |_| Ok(()))
                    .unwrap()
            }
            RepairCatalogState::AfterPublication => {
                pending.finish(inspection_limits(), |_| Ok(())).unwrap()
            }
        };
        assert_eq!(tree(&quarantine.join(format!("s_{id:016}"))), original_tree);
        assert_eq!(tree(&substitute), unchanged);
        assert!(!root.join(JOURNAL_FILE).exists());
        let inspection = inspect(root);
        let descriptor = inspection
            .catalog
            .segments
            .iter()
            .find(|s| s.row_count > 0)
            .unwrap();
        let reader =
            SegmentReader::open_for_inspection(&inspection.paths.segment_dir(descriptor.id))
                .unwrap();
        let ids: Vec<_> = (0..rows.len() as u32).collect();
        assert_eq!(reader.read_log_rows(Some(&ids)).unwrap(), rows);
        drop(inspection);
    }
    // Leave the pinned directory before the isolated fixture owner cleans up.
    std::env::set_current_dir(parent.path()).unwrap();
}
