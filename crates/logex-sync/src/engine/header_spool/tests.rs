use super::*;
use alloy_consensus::{EMPTY_OMMER_ROOT_HASH, EMPTY_ROOT_HASH};
use alloy_primitives::B256;
use tempfile::TempDir;

pub(crate) fn parent(number: u64) -> Header {
    Header {
        number,
        gas_limit: 5000,
        timestamp: 1000,
        base_fee_per_gas: (number >= 12_965_000).then_some(1),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        transactions_root: EMPTY_ROOT_HASH,
        receipts_root: EMPTY_ROOT_HASH,
        ..Default::default()
    }
}

pub(crate) fn child(parent: &Header) -> Header {
    Header {
        number: parent.number + 1,
        parent_hash: parent.hash_slow(),
        timestamp: parent.timestamp + 1,
        ..parent.clone()
    }
}

pub(crate) fn anchor(header: &Header) -> ExecutionAnchor {
    ExecutionAnchor {
        beacon_root: B256::repeat_byte(1),
        beacon_slot: 1,
        block_number: header.number,
        block_hash: header.hash_slow(),
        receipts_root: header.receipts_root,
    }
}

async fn writer(rows: usize) -> (TempDir, HeaderSpoolWriter, Vec<Header>) {
    let directory = TempDir::new().unwrap();
    let mut current = parent(100);
    let mut writer = HeaderSpoolWriter::new(directory.path().to_owned(), current.clone())
        .await
        .unwrap();
    let mut oracle = Vec::new();
    for _ in 0..rows {
        current = child(&current);
        oracle.push(current.clone());
    }
    for page in oracle.chunks(17) {
        writer = writer.append(page.to_vec()).await.unwrap();
    }
    (directory, writer, oracle)
}

#[tokio::test]
async fn checkpoint_spool_bounded_pages_roundtrip_and_fallback_rewind() {
    let (directory, writer, oracle) = writer(513).await;
    assert_eq!(
        std::fs::read_dir(directory.path()).unwrap().count(),
        0,
        "scratch is anonymous before writing"
    );
    let mut reader = writer.seal(anchor(oracle.last().unwrap())).await.unwrap();
    let (next, _, first) = reader.read_chunk(128).await.unwrap();
    assert_eq!(first.headers, oracle[..128]);
    let (next, position, candidate) = next.read_chunk(128).await.unwrap();
    assert_eq!(candidate.headers, oracle[128..256]);
    assert_eq!(
        candidate.hashes,
        oracle[128..256]
            .iter()
            .map(Header::hash_slow)
            .collect::<Vec<_>>()
    );
    reader = next.rewind(position).await.unwrap();
    let mut offset = 128;
    // The configured sequential fallback may differ from parallel chunk size.
    for limit in [13, 128, 7, 1024] {
        let (next, _, rows) = reader.read_chunk(limit).await.unwrap();
        assert!(rows.headers.len() <= limit);
        assert_eq!(rows.headers, oracle[offset..offset + rows.headers.len()]);
        offset += rows.headers.len();
        reader = next;
    }
    assert_eq!(offset, oracle.len());
    assert_eq!(reader.remaining(), 0);
    drop(reader);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn checkpoint_spool_rejects_untrusted_chain_and_terminal_before_reader() {
    let directory = TempDir::new().unwrap();
    for broken_parent in [false, true] {
        let initial = parent(100);
        let mut bad = child(&initial);
        if broken_parent {
            bad.parent_hash = B256::ZERO;
        } else {
            bad.number += 1;
        }
        let writer = HeaderSpoolWriter::new(directory.path().to_owned(), initial)
            .await
            .unwrap();
        assert!(matches!(
            writer.append(vec![bad]).await,
            Err(AppendError::InvalidHeaders(_))
        ));
    }
    for mismatch in 0..3 {
        let (_directory, writer, oracle) = writer(2).await;
        let mut proof = anchor(oracle.last().unwrap());
        match mismatch {
            0 => proof.block_number += 1,
            1 => proof.block_hash = B256::ZERO,
            _ => proof.receipts_root = B256::ZERO,
        }
        assert!(writer.seal(proof).await.is_err());
    }
    let writer = HeaderSpoolWriter::new(directory.path().to_owned(), parent(100))
        .await
        .unwrap();
    assert!(writer.seal(anchor(&parent(100))).await.is_err());
}

#[tokio::test]
async fn checkpoint_spool_terminal_maximum_does_not_require_successor() {
    let directory = TempDir::new().unwrap();
    let initial = parent(u64::MAX - 2);
    let one = child(&initial);
    let terminal = child(&one);
    let writer = HeaderSpoolWriter::new(directory.path().to_owned(), initial)
        .await
        .unwrap();
    let writer = writer
        .append(vec![one.clone(), terminal.clone()])
        .await
        .unwrap();
    let reader = writer.seal(anchor(&terminal)).await.unwrap();
    let (reader, _, rows) = reader.read_chunk(128).await.unwrap();
    assert_eq!(rows.headers, vec![one, terminal]);
    assert_eq!(reader.remaining(), 0);
}

#[tokio::test]
async fn checkpoint_spool_corruption_truncation_and_frame_reordering_fail_closed() {
    // Corrupt before sealing and after sealing. The key stays only in memory;
    // replay verifies the downloaded bytes rather than trusting scratch hashes.
    for before_seal in [false, true] {
        for mutation in 0..6 {
            let (_directory, mut writer, oracle) = writer(3).await;
            writer.file.flush().unwrap();
            let mut adversary = writer.file.get_ref().try_clone().unwrap();
            let mutate = |file: &mut File| {
                file.seek(SeekFrom::Start(0)).unwrap();
                let mut first_length = [0; 8];
                file.read_exact(&mut first_length).unwrap();
                let length = u64::from_le_bytes(first_length);
                match mutation {
                    0 => {
                        file.seek(SeekFrom::Start(0)).unwrap();
                        file.write_all(&u64::MAX.to_le_bytes()).unwrap();
                    }
                    1 => {
                        file.seek(SeekFrom::Start(9)).unwrap();
                        file.write_all(&[0xFF]).unwrap();
                    }
                    2 => {
                        file.seek(SeekFrom::Start(8 + length)).unwrap();
                        file.write_all(&[0xFF; 32]).unwrap();
                    }
                    3 => {
                        file.set_len(8 + length + 10).unwrap();
                    }
                    4 => {
                        file.seek(SeekFrom::End(0)).unwrap();
                        file.write_all(&[1]).unwrap();
                    }
                    _ => {
                        // Copy an otherwise valid first frame over frame two.
                        file.seek(SeekFrom::Start(0)).unwrap();
                        let mut frame = vec![0; 8 + length as usize + 32];
                        file.read_exact(&mut frame).unwrap();
                        file.write_all(&frame).unwrap();
                    }
                }
            };
            if before_seal {
                mutate(&mut adversary);
            }
            let mut reader = writer.seal(anchor(oracle.last().unwrap())).await.unwrap();
            if !before_seal {
                mutate(&mut adversary);
            }
            // Cloned descriptors share an offset. Restore it without reading any
            // bytes into BufReader before testing the modified anonymous inode.
            reader.file.seek(SeekFrom::Start(0)).unwrap();
            assert!(
                reader.read_chunk(128).await.is_err(),
                "mutation={mutation} before_seal={before_seal}"
            );
        }
    }
}

#[tokio::test]
async fn checkpoint_spool_creation_and_owned_io_failures_cleanup() {
    let directory = TempDir::new().unwrap();
    assert!(
        HeaderSpoolWriter::new(directory.path().join("absent"), parent(100))
            .await
            .is_err()
    );
    assert!(!directory.path().join("absent").exists());
    let marker = directory.path().join("existing");
    std::fs::write(&marker, b"keep").unwrap();
    assert!(
        HeaderSpoolWriter::new(marker.clone(), parent(100))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&marker).unwrap(), b"keep");
    let (_other, mut writer, oracle) = writer(1).await;
    // A read-only descriptor deterministically fails buffered flush, without
    // filling a disk or interacting with any real data.
    writer.file = BufWriter::new(File::open(&marker).unwrap());
    writer.file.write_all(b"pending").unwrap();
    assert!(writer.seal(anchor(&oracle[0])).await.is_err());
    assert_eq!(std::fs::read(&marker).unwrap(), b"keep");
}

#[tokio::test]
async fn checkpoint_spool_cancelled_blocking_owner_cleans_after_completion() {
    let (directory, writer, _) = writer(17).await;
    assert!(writer.file.buffer().is_empty());
    let (started, observed) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let (completed, completion) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        tokio::task::spawn_blocking(move || {
            let owner = writer;
            started.send(()).unwrap();
            wait.recv().unwrap();
            drop(owner);
            completed.send(()).unwrap();
        })
        .await
        .unwrap();
    });
    observed.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    release.send(()).unwrap();
    completion.await.unwrap();
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn checkpoint_spool_cancelled_queued_append_never_writes() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let directory = TempDir::new().unwrap();
        let initial = parent(100);
        let writer = HeaderSpoolWriter::new(directory.path().to_owned(), initial.clone())
            .await
            .unwrap();
        let observer = writer.file.get_ref().try_clone().unwrap();
        let (started, observed) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let occupied = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait.recv().unwrap();
        });
        observed.await.unwrap();
        let mut append = Box::pin(writer.append(vec![child(&initial)]));
        assert!(futures::poll!(&mut append).is_pending());
        drop(append); // AbortOnDropHandle aborts this actual queued spool write.
        release.send(()).unwrap();
        occupied.await.unwrap();
        tokio::task::spawn_blocking(|| ()).await.unwrap();
        assert_eq!(observer.metadata().unwrap().len(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    });
}

#[tokio::test]
async fn checkpoint_spool_oversized_replay_limit_stays_bounded() {
    let (_directory, writer, oracle) = writer(1031).await;
    let reader = writer.seal(anchor(oracle.last().unwrap())).await.unwrap();
    let (reader, _, chunk) = reader.read_chunk(usize::MAX).await.unwrap();
    assert_eq!(chunk.headers.len(), MAX_SPOOL_CHUNK_HEADERS);
    assert_eq!(reader.remaining(), 7);
}
