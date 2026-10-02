//! Isolated crash/corruption controls using the existing receipt-root-valid
//! scripted repair fixtures. They do not authenticate a mainnet finality anchor.
use super::*;
use crate::{
    history_audit::AuditManifestLimits,
    repair::{
        RepairFetchStep,
        tests::{self as fixture, Scripted},
    },
};
use logex_storage::native::{
    InspectionLimits, NativeStorage, NativeStorageConfig, PrimaryAuditLimits,
};
use logex_types::LogRow;

fn checkpoint() -> WeakSubjectivityCheckpoint {
    WeakSubjectivityCheckpoint {
        beacon_root: B256::repeat_byte(1),
        beacon_slot: Some(10),
    }
}
fn limits() -> AuditJournalLimits {
    AuditJournalLimits {
        max_bytes: 1024 * 1024,
        max_chunks: 16,
        checkpoint_blocks: 2,
    }
}
fn manifest(storage: &NativeStorage, scratch: &Path, range: AuditRange) -> AuditManifest {
    AuditManifest::build(
        storage
            .primary_audit_snapshot(PrimaryAuditLimits {
                max_segments: 100,
                max_total_rows: 1000,
                segment: InspectionLimits {
                    max_segment_rows: 1000,
                    max_retained_artifact_bytes: 1024 * 1024,
                    max_decoded_payload_bytes: 1024 * 1024,
                },
            })
            .unwrap(),
        range,
        scratch,
        AuditManifestLimits {
            sort_records: 1,
            merge_fan_in: 2,
            max_scratch_bytes: 1024 * 1024,
            max_runs: 1000,
        },
        &|| false,
    )
    .unwrap()
}
async fn data() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    NativeStorage,
    AuditManifest,
    Scripted,
    Vec<VerifiedRepairBlock>,
    ExecutionAnchor,
) {
    let (mut source, headers) = fixture::fixture_with_logs(&[1, 3]);
    let anchor = fixture::anchor(&headers[3]);
    let range = AuditRange {
        from: headers[0].number,
        through: anchor.block_number,
    };
    let mut fetch = RepairFetcher::new(
        RepairRange {
            start: range.from,
            end: range.through,
        },
        anchor,
        fixture::limits(),
        CancellationToken::new(),
    )
    .unwrap();
    let mut blocks = Vec::new();
    let mut rows = Vec::new();
    while let RepairFetchStep::Block(block) = fetch.next_block(&mut source).await.unwrap() {
        rows.extend_from_slice(block.rows());
        blocks.push(*block);
    }
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: dir.path().to_owned(),
        hot_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(&rows).unwrap();
    storage.checkpoint_durable().unwrap();
    let manifest = manifest(&storage, scratch.path(), range);
    (dir, scratch, storage, manifest, source, blocks, anchor)
}
fn new<'a>(parent: &Path, m: &'a AuditManifest, a: ExecutionAnchor) -> AuditSession<'a> {
    AuditSession::create_inner(parent, m, a, checkpoint(), limits()).unwrap()
}
fn resume<'a>(p: &Path, m: &'a AuditManifest, a: ExecutionAnchor) -> io::Result<AuditSession<'a>> {
    AuditSession::resume_inner(p, m, a, checkpoint(), &|| false)
}

#[tokio::test]
async fn durable_resume_refetches_only_uncheckpointed_suffix_and_preserves_original_anchor() {
    let (_dir, scratch, _storage, m, mut source, blocks, anchor) = data().await;
    let mut s = new(scratch.path(), &m, anchor);
    let job = s.directory().to_owned();
    s.compare(&blocks[0]).unwrap();
    assert_eq!(s.checkpointed_blocks(), 0);
    assert!(s.continuation().is_err());
    s.compare(&blocks[1]).unwrap();
    assert_eq!(s.compared_blocks(), 2);
    s.compare(&blocks[2]).unwrap();
    assert_eq!(s.compared_blocks(), 3);
    assert_eq!(s.checkpointed_blocks(), 2);
    drop(s); // Third block is intentionally not checkpointed.
    let mut s = resume(&job, &m, anchor).unwrap();
    assert_eq!(s.compared_blocks(), 2);
    assert_eq!(s.next_block_number(), Some(anchor.block_number - 2));
    source.header_calls.clear();
    source.body_calls.clear();
    source.receipt_calls.clear();
    let mut fetch = s
        .fetcher(fixture::limits(), CancellationToken::new())
        .unwrap();
    loop {
        match fetch.next_block(&mut source).await.unwrap() {
            RepairFetchStep::Block(block) => s.compare(&block).unwrap(),
            RepairFetchStep::Complete(completed) => {
                assert_eq!(completed.anchor(), anchor);
                break;
            }
        }
    }
    assert_eq!(
        source.body_calls,
        vec![anchor.block_number - 2, anchor.block_number - 3]
    );
    assert_eq!(source.receipt_calls, source.body_calls);
    assert!(
        !source
            .header_calls
            .iter()
            .any(|(hash, _)| *hash == anchor.block_hash)
    );
    let report = s.finish().unwrap();
    assert_eq!(
        (report.blocks, report.events, report.empty_blocks),
        (4, 4, 2)
    );
    let complete = resume(&job, &m, anchor).unwrap();
    assert!(complete.next_block_number().is_none());
    let mut fetch = complete
        .fetcher(fixture::limits(), CancellationToken::new())
        .unwrap();
    let calls = source.body_calls.len();
    assert!(matches!(
        fetch.next_block(&mut source).await.unwrap(),
        RepairFetchStep::Complete(_)
    ));
    assert_eq!(source.body_calls.len(), calls);
    assert_eq!(complete.finish().unwrap(), report);
}

#[tokio::test]
async fn fresh_physical_rescan_accepts_only_identical_frozen_selection_when_live_tail_appends() {
    let (_dir, scratch, mut storage, m, _source, blocks, anchor) = data().await;
    let mut s = new(scratch.path(), &m, anchor);
    let job = s.directory().to_owned();
    s.compare(&blocks[0]).unwrap();
    s.compare(&blocks[1]).unwrap();
    drop(s);
    let mut tail = blocks[0].rows()[0].clone();
    tail.block_number = anchor.block_number + 1;
    tail.log_index = 0;
    tail.block_hash = B256::repeat_byte(17);
    storage.write_batch(&[tail]).unwrap();
    storage.checkpoint_durable().unwrap();
    let next = manifest(&storage, scratch.path(), m.summary().range);
    assert_ne!(
        m.summary().source_prefix_fingerprint,
        next.summary().source_prefix_fingerprint
    );
    assert!(same_selection(m.summary(), next.summary()));
    assert_eq!(resume(&job, &next, anchor).unwrap().compared_blocks(), 2);
    let other = tempfile::tempdir().unwrap();
    let mut replacement = NativeStorage::open(NativeStorageConfig {
        data_dir: other.path().to_owned(),
        hot_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let rows: Vec<LogRow> = blocks.iter().flat_map(|b| b.rows().to_vec()).collect();
    replacement.write_batch(&rows).unwrap();
    replacement.checkpoint_durable().unwrap();
    let cloned = manifest(&replacement, scratch.path(), m.summary().range);
    assert_eq!(cloned.summary().records_digest, m.summary().records_digest);
    assert_ne!(
        cloned.summary().selected_namespace_fingerprint,
        m.summary().selected_namespace_fingerprint
    );
    assert!(
        resume(&job, &cloned, anchor).is_err(),
        "identical row bytes in another source namespace are not this job"
    );
}

#[tokio::test]
async fn corrupt_truncated_reordered_replayed_or_missing_chunks_never_advance_resume() {
    let (_dir, scratch, _storage, m, _source, blocks, anchor) = data().await;
    for mode in 0..6 {
        let mut s = new(scratch.path(), &m, anchor);
        let job = s.directory().to_owned();
        for b in &blocks {
            s.compare(b).unwrap();
        }
        drop(s);
        let first = job.join(chunk_name(0));
        let second = job.join(chunk_name(1));
        let mut bytes = fs::read(&first).unwrap();
        match mode {
            0 => {
                bytes[100] ^= 1;
                fs::write(&first, bytes).unwrap();
            }
            1 => {
                bytes.truncate(bytes.len() - 1);
                fs::write(&first, bytes).unwrap();
            }
            2 => {
                let other = fs::read(&second).unwrap();
                fs::write(&first, other).unwrap();
                fs::write(&second, bytes).unwrap();
            }
            3 => {
                fs::write(&second, bytes).unwrap();
            }
            4 => {
                fs::remove_file(first).unwrap();
            }
            _ => {
                bytes.extend(b"extra");
                fs::write(&first, bytes).unwrap();
            }
        }
        assert!(resume(&job, &m, anchor).is_err(), "corruption mode {mode}");
    }
}

#[tokio::test]
async fn frame_checksum_cannot_hide_a_broken_header_link_or_event_count() {
    let (_dir, scratch, _storage, m, _source, blocks, anchor) = data().await;
    for field in 0..3 {
        let mut s = new(scratch.path(), &m, anchor);
        let job = s.directory().to_owned();
        let previous = s.previous;
        s.compare(&blocks[0]).unwrap();
        s.compare(&blocks[1]).unwrap();
        drop(s);
        let mut entries: Vec<_> = blocks[..2]
            .iter()
            .map(|b| (b.header().clone(), b.rows().len() as u64))
            .collect();
        match field {
            0 => entries[0].0.number -= 1,
            1 => entries[1].0.parent_hash = B256::repeat_byte(8),
            _ => entries[0].1 += 1,
        };
        fs::write(
            job.join(chunk_name(0)),
            encode_chunk(0, previous, &entries).unwrap(),
        )
        .unwrap();
        assert!(resume(&job, &m, anchor).is_err());
    }
}

#[tokio::test]
async fn ownership_limits_cancellation_wrong_trust_and_unpublished_scratch_fail_safely() {
    let (_dir, scratch, _storage, m, _source, blocks, anchor) = data().await;
    let s = new(scratch.path(), &m, anchor);
    let job = s.directory().to_owned();
    assert!(matches!(resume(&job,&m,anchor),Err(e) if e.kind()==io::ErrorKind::WouldBlock));
    drop(s);
    let mut wrong = anchor;
    wrong.receipts_root = B256::repeat_byte(77);
    assert!(resume(&job, &m, wrong).is_err());
    let mut root = checkpoint();
    root.beacon_root = B256::repeat_byte(76);
    assert!(AuditSession::resume_inner(&job, &m, anchor, root, &|| false).is_err());
    assert!(
        matches!(AuditSession::resume_inner(&job,&m,anchor,checkpoint(),&||true),Err(e) if e.kind()==io::ErrorKind::Interrupted)
    );
    let scratch_file = job.join(".audit-chunk-00000000000000000000000000000000");
    fs::write(&scratch_file, b"partial").unwrap();
    assert_eq!(resume(&job, &m, anchor).unwrap().compared_blocks(), 0);
    assert_eq!(fs::read(scratch_file).unwrap(), b"partial");
    fs::write(job.join("unknown"), b"keep me").unwrap();
    assert!(resume(&job, &m, anchor).is_err());
    assert_eq!(fs::read(job.join("unknown")).unwrap(), b"keep me");
    let mut small = limits();
    small.max_chunks = 1;
    small.checkpoint_blocks = 1;
    let mut s =
        AuditSession::create_inner(scratch.path(), &m, anchor, checkpoint(), small).unwrap();
    s.compare(&blocks[0]).unwrap();
    assert!(s.compare(&blocks[1]).is_err());
    assert!(s.checkpoint().is_err());
    assert!(s.finish().is_err());
    let mut bytes = limits();
    bytes.max_bytes = 1;
    assert!(AuditSession::create_inner(scratch.path(), &m, anchor, checkpoint(), bytes).is_err());
}

#[tokio::test]
async fn invalidated_source_or_incomplete_prefix_cannot_publish_a_finished_job() {
    let (_dir, scratch, storage, m, _source, blocks, anchor) = data().await;
    let mut s = new(scratch.path(), &m, anchor);
    let job = s.directory().to_owned();
    s.compare(&blocks[0]).unwrap();
    s.checkpoint().unwrap();
    assert!(s.finish().is_err());
    let s = resume(&job, &m, anchor).unwrap();
    drop(storage);
    assert!(s.finish().is_err());
    assert!(resume(&job, &m, anchor).is_err());
}

#[tokio::test]
async fn reopened_native_store_resumes_same_job_without_replaying_completed_payloads() {
    let (dir, scratch, storage, m, mut source, blocks, anchor) = data().await;
    let range = m.summary().range;
    let mut s = new(scratch.path(), &m, anchor);
    let job = s.directory().to_owned();
    s.compare(&blocks[0]).unwrap();
    s.compare(&blocks[1]).unwrap();
    let id = s.identity().clone();
    drop(s);
    drop(m);
    drop(storage);
    let reopened = NativeStorage::open(NativeStorageConfig {
        data_dir: dir.path().to_owned(),
        hot_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let rescanned = manifest(&reopened, scratch.path(), range);
    let mut resumed = resume(&job, &rescanned, anchor).unwrap();
    assert_eq!(resumed.identity(), &id);
    source.body_calls.clear();
    source.receipt_calls.clear();
    let mut fetch = resumed
        .fetcher(fixture::limits(), CancellationToken::new())
        .unwrap();
    while let RepairFetchStep::Block(block) = fetch.next_block(&mut source).await.unwrap() {
        resumed.compare(&block).unwrap();
    }
    assert_eq!(
        source.body_calls,
        vec![anchor.block_number - 2, anchor.block_number - 3]
    );
    assert_eq!(source.body_calls, source.receipt_calls);
    assert_eq!(resumed.finish().unwrap().events, 4);
}

#[tokio::test]
#[cfg(unix)]
async fn journal_symlinks_are_rejected_without_reading_or_replacing_their_targets() {
    let (_dir, scratch, _storage, m, _source, blocks, anchor) = data().await;
    let mut s = new(scratch.path(), &m, anchor);
    let job = s.directory().to_owned();
    s.compare(&blocks[0]).unwrap();
    s.compare(&blocks[1]).unwrap();
    drop(s);
    let first = job.join(chunk_name(0));
    let outside = scratch.path().join("preserved-target");
    let raw = fs::read(&first).unwrap();
    fs::write(&outside, &raw).unwrap();
    fs::remove_file(&first).unwrap();
    std::os::unix::fs::symlink(&outside, &first).unwrap();
    assert!(resume(&job, &m, anchor).is_err());
    assert_eq!(fs::read(outside).unwrap(), raw);
}

#[tokio::test]
async fn unavailable_fetch_retries_only_unfinished_blocks_without_source_rescan() {
    let (_dir, scratch, _storage, manifest, mut source, _blocks, anchor) = data().await;
    let mut session = new(scratch.path(), &manifest, anchor);
    let mut fetch = session
        .fetcher(fixture::limits(), CancellationToken::new())
        .unwrap();
    let RepairFetchStep::Block(first) = fetch.next_block(&mut source).await.unwrap() else {
        panic!("missing block")
    };
    session.compare(&first).unwrap();
    assert_eq!(session.checkpointed_blocks(), 0);
    source.body_error = true;
    let error = fetch.next_block(&mut source).await.unwrap_err();
    source.body_error = false;
    let calls = source.body_calls.len();
    let mut retry = session
        .retry_unavailable(error, fixture::limits(), CancellationToken::new())
        .unwrap();
    assert_eq!(session.checkpointed_blocks(), 1);
    assert_eq!(
        source.body_calls.len(),
        calls,
        "retry creation cannot fetch or rescan"
    );
    while let RepairFetchStep::Block(block) = retry.next_block(&mut source).await.unwrap() {
        session.compare(&block).unwrap();
    }
    assert_eq!(
        source.body_calls[calls..],
        [
            anchor.block_number - 1,
            anchor.block_number - 2,
            anchor.block_number - 3
        ]
    );
    assert_eq!(session.finish().unwrap().blocks, 4);
}

#[tokio::test]
async fn invalid_payload_fetch_is_never_retried_as_unavailability() {
    let (_dir, scratch, _storage, manifest, mut source, _blocks, anchor) = data().await;
    let mut session = new(scratch.path(), &manifest, anchor);
    let mut fetch = session
        .fetcher(fixture::limits(), CancellationToken::new())
        .unwrap();
    let initial_calls = source.body_calls.len();
    source.wrong_body = true;
    let error = fetch.next_block(&mut source).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<RepairFetchError>().unwrap().kind,
        RepairFetchErrorKind::InvalidData
    );
    assert!(
        session
            .retry_unavailable(error, fixture::limits(), CancellationToken::new())
            .is_err()
    );
    assert_eq!(session.checkpointed_blocks(), 0);
    assert_eq!(source.body_calls.len(), initial_calls + 1);
}
