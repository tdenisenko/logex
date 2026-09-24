//! A failed later response must not publish an earlier, valid repair prefix.
//! All sources are scripted and all storage lives in disposable directories.
use super::*;
use crate::{
    p2p::peer_manager::{SourcedBlockBody, SourcedReceiptSet},
    repair::{
        RepairFetchError, RepairFetchErrorKind, execution::equivalence_tests::expected_rows,
        tests::fixture_with_logs,
    },
};
use alloy_consensus::Header;
use alloy_primitives::B256;
use logex_query::execute_log_filter;
use logex_storage::{PartitionManager, PartitionManagerConfig, native::NativeLogFilter};
use reth_network_peers::PeerId;
use std::time::SystemTime;

#[derive(Clone, Copy, Debug)]
enum LaterResponse {
    IncompleteReceipts,
    UnavailableHistory,
    Cancelled,
}

struct InterruptedSource {
    inner: Scripted,
    stop_at: u64,
    response: LaterResponse,
    cancellation: CancellationToken,
}

impl RepairSource for InterruptedSource {
    async fn headers(
        &mut self,
        start: B256,
        count: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<(PeerId, Vec<Header>)> {
        self.inner.headers(start, count, timeout, attempts).await
    }

    async fn body(
        &mut self,
        hash: B256,
        number: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<Vec<SourcedBlockBody>> {
        self.inner.body(hash, number, timeout, attempts).await
    }

    async fn receipts(
        &mut self,
        header: &Header,
        preferred: PeerId,
        timeout: Duration,
        attempts: usize,
    ) -> Result<Vec<SourcedReceiptSet>> {
        let mut sets = self
            .inner
            .receipts(header, preferred, timeout, attempts)
            .await?;
        if header.number == self.stop_at {
            match self.response {
                LaterResponse::IncompleteReceipts => {
                    assert!(sets[0].1.pop().is_some());
                }
                LaterResponse::UnavailableHistory => {
                    return Err(eyre::eyre!("scripted later receipt history is unavailable"));
                }
                LaterResponse::Cancelled => {
                    self.cancellation.cancel();
                    std::future::pending::<()>().await;
                }
            }
        }
        Ok(sets)
    }
}

fn immutable_snapshot(root: &Path) -> (Tree, BTreeMap<PathBuf, SystemTime>) {
    let contents = tree(root);
    let modified = contents
        .keys()
        .map(|path| {
            (
                path.clone(),
                fs::symlink_metadata(root.join(path))
                    .unwrap()
                    .modified()
                    .unwrap(),
            )
        })
        .collect();
    (contents, modified)
}

#[tokio::test]
async fn later_fetch_failure_preserves_originals_and_clean_retry_restores_exact_queries() {
    for response in [
        LaterResponse::IncompleteReceipts,
        LaterResponse::UnavailableHistory,
        LaterResponse::Cancelled,
    ] {
        // Reverse delivery validates block 2 before reaching the changed response
        // at block 1. Block 0 is still required to complete the selected range.
        let (scripted, headers) = fixture_with_logs(&[0, 2]);
        let expected = expected_rows(&headers, &[0, 2]);
        let tmp = write(&expected);
        let catalog = primary(assess(tmp.path(), IndexBuildProfile::All)).catalog;
        let id = catalog.active_hot_segment.unwrap();
        let original = StorageCatalogPaths::new(tmp.path().to_owned()).segment_dir(id);
        fs::remove_file(original.join("data.col")).unwrap();
        let damaged = tree(&original);
        let before = immutable_snapshot(tmp.path());
        let cancellation = CancellationToken::new();
        let mut source = InterruptedSource {
            inner: scripted,
            stop_at: headers[1].number,
            response,
            cancellation: cancellation.clone(),
        };
        let (_consensus_dir, store) = consensus(&[anchor(&headers[3])]);
        let error = execute_repair(
            assess(tmp.path(), IndexBuildProfile::All),
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &store,
            cancellation,
        )
        .await
        .unwrap_err();
        let expected_kind = match response {
            LaterResponse::IncompleteReceipts => RepairFetchErrorKind::InvalidData,
            LaterResponse::UnavailableHistory => RepairFetchErrorKind::Unavailable,
            LaterResponse::Cancelled => RepairFetchErrorKind::Cancelled,
        };
        assert_eq!(
            error.downcast_ref::<RepairFetchError>().unwrap().kind,
            expected_kind,
            "{response:?}: {error:#}"
        );
        assert_eq!(
            source.inner.receipt_calls,
            vec![headers[2].number, headers[1].number]
        );
        assert_eq!(source.inner.body_calls, source.inner.receipt_calls);
        assert_eq!(immutable_snapshot(tmp.path()), before, "{response:?}");
        assert!(!tmp.path().join("repair.journal").exists());
        assert!(!tmp.path().join("index-repair.journal").exists());

        // A new attempt must reconstruct the entire range, including the valid
        // prefix from the failed attempt. No in-memory partial result is reused.
        let (mut source, _) = fixture_with_logs(&[0, 2]);
        let outcome = execute_repair(
            assess(tmp.path(), IndexBuildProfile::All),
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &store,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        verified(&outcome.assessment, IndexBuildProfile::All);
        assert_eq!(
            source.receipt_calls,
            vec![headers[2].number, headers[1].number, headers[0].number]
        );
        assert_eq!(outcome.quarantine_dirs.len(), 1);
        assert_eq!(
            tree(&outcome.quarantine_dirs[0].join(format!("s_{id:016}"))),
            damaged
        );
        let repaired = primary(outcome.assessment).catalog;
        assert_eq!(repaired.state, catalog.state);
        assert_eq!(repaired.anchors, catalog.anchors);
        for _ in 0..2 {
            let storage = PartitionManager::open(PartitionManagerConfig {
                data_dir: tmp.path().to_owned(),
                partition_target_rows: 100,
                compaction_safety_margin_blocks: 0,
            })
            .unwrap();
            assert_eq!(
                execute_log_filter(&storage, &NativeLogFilter::new()).unwrap(),
                expected,
                "{response:?}"
            );
        }
    }
}
