use super::*;
use crate::native::{NativeStorageConfig, SegmentKind};

fn fixture() -> (tempfile::TempDir, StorageCatalogPaths, RepairJournal) {
    let temp = tempfile::tempdir().unwrap();
    let (mut before, paths) = NativeStorageCatalog::open_or_create(&NativeStorageConfig {
        data_dir: temp.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    let original = before.register_segment(SegmentKind::Hot).unwrap();
    before.state.recent_headers = vec![alloy_consensus::Header {
        number: 123,
        ..Default::default()
    }];
    let journal = RepairJournal {
        operation: FixedBytes::repeat_byte(1),
        entries: vec![RepairEntry {
            original_id: original.id,
            replacement_id: before.next_segment_id,
            canonical_digest: FixedBytes::repeat_byte(2),
            staged_manifest: None,
        }],
        seeds: vec![original.id],
        before,
        after: None,
    };
    (temp, paths, journal)
}

#[test]
fn journal_roundtrip_preserves_full_header_window_and_durable_file() {
    let (_temp, paths, journal) = fixture();
    assert!(RepairJournal::load(&paths).unwrap().is_none());
    let encoded = journal.encode().unwrap();
    let decoded = RepairJournal::decode(&encoded).unwrap();
    assert_eq!(decoded.before, journal.before);
    assert_eq!(decoded.before.state.recent_headers.len(), 1);
    assert_eq!(decoded.encode().unwrap(), encoded);
    journal.persist(&paths).unwrap();
    assert_eq!(
        RepairJournal::load(&paths)
            .unwrap()
            .unwrap()
            .encode()
            .unwrap(),
        encoded
    );
}

#[test]
fn journal_rejects_corruption_truncation_trailing_and_component_overflow() {
    let (_temp, _paths, journal) = fixture();
    let bytes = journal.encode().unwrap();
    for index in [0, 8, 16, 24, 32, HEADER, bytes.len() - 1] {
        let mut damaged = bytes.clone();
        damaged[index] ^= 1;
        assert!(RepairJournal::decode(&damaged).is_err());
    }
    assert!(RepairJournal::decode(&bytes[..bytes.len() - 1]).is_err());
    let mut trailing = bytes.clone();
    trailing.push(0);
    assert!(RepairJournal::decode(&trailing).is_err());
    let mut oversized = bytes;
    oversized[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
    let crc = checksum(&oversized);
    oversized[32..36].copy_from_slice(&crc.to_le_bytes());
    assert!(RepairJournal::decode(&oversized).is_err());
}

#[test]
fn journal_selection_and_allocation_are_exact() {
    let (_temp, _paths, journal) = fixture();
    for case in 0..6 {
        let mut bad = journal.clone();
        match case {
            0 => bad.operation = FixedBytes::ZERO,
            1 => bad.seeds.push(bad.seeds[0]),
            2 => bad.entries[0].original_id += 1,
            3 => bad.entries[0].replacement_id += 1,
            4 => bad.entries.clear(),
            _ => {
                bad.before.next_segment_id = u64::MAX;
                bad.entries[0].replacement_id = u64::MAX;
            }
        }
        assert!(bad.validate().is_err());
    }
}

#[test]
fn prepared_journal_derives_only_replacement_identity_and_active_pointer() {
    let (temp, _paths, mut journal) = fixture();
    let original = journal.before.segments[0].clone();
    let dir = temp.path().join("encoded");
    let columns = segment::write_repair_bundle(&dir, &[], &crate::NullBitmap::new()).unwrap();
    let mut replacement = original.clone();
    replacement.id = journal.entries[0].replacement_id;
    replacement.generation = 0;
    replacement.relative_path = PathBuf::from("segments").join(format!("s_{:016}", replacement.id));
    replacement.manifest_relative_path = replacement.relative_path.join("segment.json");
    let columns = columns.apply_to(&mut replacement);
    journal.entries[0].staged_manifest =
        Some(segment::manifest_with_columns(&replacement, columns));
    let mut after = journal.before.clone();
    after.next_segment_id += 1;
    after.active_hot_segment = Some(replacement.id);
    after.segments[0] = replacement;
    journal.after = Some(after);
    let encoded = journal.encode().unwrap();
    assert_eq!(
        RepairJournal::decode(&encoded).unwrap().after,
        journal.after
    );
    for case in 0..5 {
        let mut bad = journal.clone();
        match case {
            0 => bad.after.as_mut().unwrap().hot_target_rows += 1,
            1 => bad.after.as_mut().unwrap().state.recent_headers.clear(),
            2 => bad.after.as_mut().unwrap().active_hot_segment = Some(original.id),
            3 => {
                bad.entries[0].staged_manifest.as_mut().unwrap().columns[0].data_path =
                    "../other".into()
            }
            _ => bad.after = None,
        }
        assert!(bad.validate().is_err());
    }
}

#[test]
fn journal_loader_refuses_nonregular_and_dangling_entries() {
    let (_temp, paths, _) = fixture();
    let path = paths.root().join(JOURNAL_FILE);
    fs::create_dir(&path).unwrap();
    assert!(RepairJournal::load(&paths).is_err());
    fs::remove_dir(&path).unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(paths.root().join("missing"), &path).unwrap();
        assert!(RepairJournal::load(&paths).is_err());
        assert!(fs::symlink_metadata(path).unwrap().file_type().is_symlink());
    }
}
