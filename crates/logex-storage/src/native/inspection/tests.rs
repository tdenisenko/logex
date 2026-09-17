use super::*;
use crate::native::{NativeStorage, NativeStorageConfig};
use alloy_primitives::{Address, B256, Bytes};
use logex_types::{LogRow, Source};
use std::{collections::BTreeMap, fs, time::SystemTime};

fn limits() -> InspectionLimits {
    InspectionLimits {
        max_segment_rows: 100,
        max_retained_artifact_bytes: 16 * 1024 * 1024,
        max_decoded_payload_bytes: 1024 * 1024,
    }
}

fn fixture(bundled: bool) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let rows: Vec<_> = (0..20)
        .map(|i| LogRow {
            block_number: i + 10,
            block_hash: B256::repeat_byte(i as u8),
            timestamp: 100 + i,
            tx_hash: B256::repeat_byte(42),
            tx_index: 0,
            log_index: i as u32,
            address: Address::repeat_byte(7),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::from(vec![i as u8; 4]),
            data_len: 4,
            source: Source::Receipt,
        })
        .collect();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: if bundled { 20 } else { 100 },
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    if bundled {
        storage.write_historical_batch(&rows).unwrap();
    } else {
        storage.write_batch(&rows).unwrap();
    }
    storage.checkpoint_durable().unwrap();
    drop(storage);
    tmp
}

type Tree = BTreeMap<PathBuf, (SystemTime, Option<Vec<u8>>)>;
fn tree(root: &Path) -> Tree {
    fn walk(root: &Path, path: &Path, result: &mut Tree) {
        let metadata = fs::metadata(path).unwrap();
        result.insert(
            path.strip_prefix(root).unwrap().to_owned(),
            (
                metadata.modified().unwrap(),
                if metadata.is_file() {
                    Some(fs::read(path).unwrap())
                } else {
                    None
                },
            ),
        );
        if metadata.is_dir() {
            for child in fs::read_dir(path).unwrap() {
                walk(root, &child.unwrap().path(), result);
            }
        }
    }
    let mut result = Tree::new();
    walk(root, root, &mut result);
    result
}
fn data_segment(report: &PrimaryDataInspection) -> &SegmentInspection {
    let id = report
        .catalog
        .segments
        .iter()
        .find(|segment| segment.row_count != 0)
        .unwrap()
        .id;
    report
        .segments
        .iter()
        .find(|segment| segment.id == id)
        .unwrap()
}

#[test]
fn inspection_missing_path_does_not_create_it() {
    let tmp = tempfile::tempdir().unwrap();
    let missing = tmp.path().join("absent");
    assert_eq!(
        inspect_primary_data(&missing, limits()).unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    assert!(!missing.exists());
}

#[test]
fn inspection_verifies_raw_and_bundled_sources_without_mutation() {
    for bundled in [false, true] {
        let tmp = fixture(bundled);
        let before = tree(tmp.path());
        let report = inspect_primary_data(tmp.path(), limits()).unwrap();
        assert!(report.recovery_prerequisites.is_empty());
        assert!(!report.derived_indexes_inspected);
        assert!(
            report.segments.iter().all(|segment| matches!(
                segment.disposition,
                PrimaryDataDisposition::CommitmentVerified
            )),
            "{:#?}",
            report.segments
        );
        let descriptor = report
            .catalog
            .segments
            .iter()
            .find(|segment| segment.row_count != 0)
            .unwrap();
        assert_eq!(descriptor.column_bundle.is_some(), bundled);
        assert!(matches!(
            data_segment(&report).disposition,
            PrimaryDataDisposition::CommitmentVerified
        ));
        assert_eq!(tree(tmp.path()), before);
        drop(report);
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn inspection_report_keeps_exclusive_ownership_until_drop() {
    let tmp = fixture(false);
    let report = inspect_primary_data(tmp.path(), limits()).unwrap();
    assert_eq!(
        inspect_primary_data(&tmp.path().join("."), limits())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    drop(report);
    assert!(inspect_primary_data(tmp.path(), limits()).is_ok());
}

#[test]
fn inspection_limits_are_incomplete_without_mutation_or_corruption_claim() {
    for restricted in [
        InspectionLimits {
            max_segment_rows: 1,
            ..limits()
        },
        InspectionLimits {
            max_retained_artifact_bytes: 1,
            ..limits()
        },
        InspectionLimits {
            max_decoded_payload_bytes: 1,
            ..limits()
        },
    ] {
        let tmp = fixture(false);
        let before = tree(tmp.path());
        let report = inspect_primary_data(tmp.path(), restricted).unwrap();
        assert!(matches!(
            data_segment(&report).disposition,
            PrimaryDataDisposition::LimitExceeded { .. }
        ));
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn inspection_pending_recovery_preserves_evidence_and_skips_verdicts() {
    for artifact in ["wal/recovery.json", "wal/ingestion.json", "wal/pending.wal"] {
        let tmp = fixture(false);
        fs::write(tmp.path().join(artifact), b"unverified recovery evidence").unwrap();
        let before = tree(tmp.path());
        let report = inspect_primary_data(tmp.path(), limits()).unwrap();
        assert!(
            report
                .recovery_prerequisites
                .contains(&tmp.path().join(artifact))
        );
        assert!(report.segments.iter().all(|segment| matches!(
            segment.disposition,
            PrimaryDataDisposition::RecoveryRequired
        )));
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn inspection_interior_logical_damage_is_not_hidden_by_boundary_rows() {
    for column in ["block_hash.col", "source.col", "data.col"] {
        let tmp = fixture(false);
        let paths = StorageCatalogPaths::new(tmp.path().to_owned());
        let catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
        let descriptor = catalog
            .segments
            .iter()
            .find(|segment| segment.row_count != 0)
            .unwrap();
        let path = paths.segment_dir(descriptor.id).join(column);
        let mut bytes = fs::read(&path).unwrap();
        let offset = if column == "data.col" {
            bytes.len() - 40
        } else {
            crate::ColumnFileHeader::SIZE
                + if column == "block_hash.col" {
                    10 * 32
                } else {
                    10
                }
        };
        bytes[offset] = 255;
        fs::write(path, bytes).unwrap();
        let before = tree(tmp.path());
        let report = inspect_primary_data(tmp.path(), limits()).unwrap();
        if column != "source.col" {
            assert!(matches!(
                data_segment(&report).disposition,
                PrimaryDataDisposition::LogicalCommitmentMismatch
            ));
        } else {
            assert!(matches!(
                data_segment(&report).disposition,
                PrimaryDataDisposition::Incomplete { .. }
            ));
        }
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn inspection_missing_committed_artifact_is_incomplete_and_preserved() {
    let tmp = fixture(false);
    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    let catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
    let descriptor = catalog
        .segments
        .iter()
        .find(|segment| segment.row_count != 0)
        .unwrap();
    fs::remove_file(paths.segment_dir(descriptor.id).join("data.col")).unwrap();
    let before = tree(tmp.path());
    let report = inspect_primary_data(tmp.path(), limits()).unwrap();
    assert!(matches!(
        data_segment(&report).disposition,
        PrimaryDataDisposition::Incomplete { .. }
    ));
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn inspection_active_historical_is_not_finalized_sealed() {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: 100,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let row = LogRow {
        block_number: 10,
        block_hash: B256::ZERO,
        timestamp: 100,
        tx_hash: B256::ZERO,
        tx_index: 0,
        log_index: 0,
        address: Address::ZERO,
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: Bytes::new(),
        data_len: 0,
        source: Source::Receipt,
    };
    storage.write_historical_batch(&[row]).unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let report = inspect_primary_data(tmp.path(), limits()).unwrap();
    assert_eq!(
        data_segment(&report).role,
        InspectedSegmentRole::ActiveHistorical
    );
    assert!(matches!(
        data_segment(&report).disposition,
        PrimaryDataDisposition::CommitmentVerified
    ));
}

#[test]
fn inspection_unbound_source_cannot_be_commitment_verified() {
    let source = fixture(false);
    let paths = StorageCatalogPaths::new(source.path().to_owned());
    let catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
    let mut descriptor = catalog
        .segments
        .iter()
        .find(|segment| segment.row_count != 0)
        .unwrap()
        .clone();
    let reader = SegmentReader::open(&paths.segment_dir(descriptor.id)).unwrap();
    let rows = reader.read_log_rows(None).unwrap();
    let legacy = tempfile::tempdir().unwrap();
    crate::ColumnFile::write_batch(legacy.path(), &rows).unwrap();
    // Match the existing explicit legacy fixture: standalone writers normally
    // create an identity marker, so remove it only while constructing this test.
    fs::remove_file(legacy.path().join(".source-publication")).unwrap();
    descriptor.source_namespace = None;
    descriptor.source_commitment = None;
    descriptor.source_state = None;
    descriptor.generation = 0;
    let before = tree(legacy.path());
    let disposition = inspect_segment(legacy.path(), &descriptor, limits());
    assert!(
        matches!(disposition, PrimaryDataDisposition::Unbound),
        "{disposition:?}"
    );
    assert_eq!(tree(legacy.path()), before);
}

#[cfg(unix)]
#[test]
fn inspection_refuses_segment_alias_without_following_or_modifying_it() {
    let tmp = fixture(false);
    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    let catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
    let descriptor = catalog
        .segments
        .iter()
        .find(|segment| segment.row_count != 0)
        .unwrap();
    let path = paths.segment_dir(descriptor.id);
    let preserved = tmp.path().join("preserved-source");
    fs::rename(&path, &preserved).unwrap();
    std::os::unix::fs::symlink(&preserved, &path).unwrap();
    let before = tree(&preserved);
    let report = inspect_primary_data(tmp.path(), limits()).unwrap();
    assert!(matches!(
        data_segment(&report).disposition,
        PrimaryDataDisposition::Incomplete {
            kind: io::ErrorKind::Unsupported,
            ..
        }
    ));
    assert_eq!(tree(&preserved), before);
    assert!(fs::symlink_metadata(path).unwrap().file_type().is_symlink());
}

#[cfg(unix)]
#[test]
fn inspection_rejects_aliased_parent_directories() {
    for directory in ["segments", "wal"] {
        let tmp = fixture(false);
        let path = tmp.path().join(directory);
        let preserved = tmp.path().join("preserved-directory");
        fs::rename(&path, &preserved).unwrap();
        std::os::unix::fs::symlink(&preserved, &path).unwrap();
        let before = tree(&preserved);
        assert_eq!(
            inspect_primary_data(tmp.path(), limits())
                .unwrap_err()
                .kind(),
            io::ErrorKind::Unsupported
        );
        assert_eq!(tree(&preserved), before);
    }
}

#[cfg(unix)]
#[test]
fn inspection_empty_wal_alias_and_directory_are_recovery_prerequisites() {
    for alias in [false, true] {
        let tmp = fixture(false);
        let path = tmp.path().join("wal/pending.wal");
        if path.exists() {
            fs::remove_file(&path).unwrap();
        }
        if alias {
            let target = tmp.path().join("empty-preserved");
            fs::write(&target, []).unwrap();
            std::os::unix::fs::symlink(target, &path).unwrap();
        } else {
            fs::create_dir(&path).unwrap();
        }
        let report = inspect_primary_data(tmp.path(), limits()).unwrap();
        assert!(report.recovery_prerequisites.contains(&path));
        assert!(report.segments.iter().all(|segment| matches!(
            segment.disposition,
            PrimaryDataDisposition::RecoveryRequired
        )));
        assert!(fs::symlink_metadata(path).is_ok());
    }
}

#[test]
fn inspection_rejects_broken_catalog_header_links_without_mutation() {
    let tmp = fixture(false);
    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    let mut catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
    catalog.state.recent_headers = vec![
        alloy_consensus::Header {
            number: 10,
            ..Default::default()
        },
        alloy_consensus::Header {
            number: 11,
            parent_hash: B256::repeat_byte(123),
            ..Default::default()
        },
    ];
    // The encoded catalog is structurally valid; ordinary startup applies the
    // additional parent-link invariant after decoding it.
    catalog.persist(&paths).unwrap();
    let before = tree(tmp.path());
    let error = inspect_primary_data(tmp.path(), limits()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("broken parent link"));
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn inspection_verifies_fresh_empty_store_without_inventing_artifacts() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    drop(storage);
    let before = tree(tmp.path());
    let report = inspect_primary_data(tmp.path(), limits()).unwrap();
    assert_eq!(report.segments.len(), 1);
    assert!(
        report
            .catalog
            .segments
            .iter()
            .all(|segment| segment.row_count == 0)
    );
    assert!(
        report.segments.iter().all(|segment| matches!(
            segment.disposition,
            PrimaryDataDisposition::CommitmentVerified
        )),
        "{:#?}",
        report.segments
    );
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn inspection_rejects_bundled_manifest_identity_substitution() {
    for field in ["segment_id", "kind"] {
        let tmp = fixture(true);
        let paths = StorageCatalogPaths::new(tmp.path().to_owned());
        let catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
        let descriptor = catalog
            .segments
            .iter()
            .find(|segment| segment.row_count != 0)
            .unwrap();
        assert!(descriptor.column_bundle.is_some());
        let manifest = paths.segment_dir(descriptor.id).join("segment.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
        assert!(value.get(field).is_some());
        value[field] = if field == "kind" {
            serde_json::json!("hot")
        } else {
            serde_json::json!(999)
        };
        fs::write(manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        let before = tree(tmp.path());
        let report = inspect_primary_data(tmp.path(), limits()).unwrap();
        let disposition = &data_segment(&report).disposition;
        assert!(
            matches!(disposition, PrimaryDataDisposition::Incomplete { .. }),
            "{field}: {disposition:?}"
        );
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn inspection_empty_source_does_not_ignore_a_present_malformed_bitmap() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    drop(storage);
    let paths = StorageCatalogPaths::new(tmp.path().to_owned());
    let catalog = NativeStorageCatalog::load_existing(&paths).unwrap();
    assert_eq!(catalog.segments[0].row_count, 0);
    fs::write(
        paths
            .segment_dir(catalog.segments[0].id)
            .join("canonical.bitmap"),
        b"invalid existing bitmap",
    )
    .unwrap();
    let before = tree(tmp.path());
    let report = inspect_primary_data(tmp.path(), limits()).unwrap();
    assert!(
        matches!(
            report.segments[0].disposition,
            PrimaryDataDisposition::Incomplete { .. }
        ),
        "{:#?}",
        report.segments
    );
    assert_eq!(tree(tmp.path()), before);
}
