//! Admission controls use isolated scripted store state, not mainnet evidence.
use super::*;
use crate::{AnchorRecord, light_client};
use alloy_primitives::B256;

fn fixture() -> (tempfile::TempDir, ConsensusStore, ExecutionAnchor) {
    let dir = tempfile::tempdir().unwrap();
    let slot = (crate::MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_epoch() / 256).saturating_sub(1)
        * 8192
        + 16;
    let fixture = light_client::test_cached_light_client_fixture(slot);
    let store = ConsensusStore::open(
        dir.path(),
        Some(&format!("{:#x}", fixture.checkpoint.beacon_root)),
    )
    .unwrap();
    let payload = fixture.payloads.bootstrap.unwrap();
    let (status, initial) =
        crate::verify_bootstrap_payload(&payload.bytes, fixture.checkpoint).unwrap();
    let anchor = initial.finalized_anchor().unwrap();
    store
        .record_verified_bootstrap(status, payload, initial)
        .unwrap();
    (dir, store, anchor)
}
fn record(anchor: ExecutionAnchor, parent: Option<B256>) -> AnchorRecord {
    AnchorRecord {
        anchor,
        finalized: false,
        parent_beacon_root: parent,
    }
}

#[test]
fn finalized_audit_requires_verified_store_and_exact_materialized_anchor() {
    let (dir, store, anchor) = fixture();
    assert!(store.finalized_audit_anchor().is_none());
    store.replace_anchors(vec![record(anchor, None)]).unwrap();
    let captured = store.finalized_audit_anchor().unwrap();
    assert_eq!(captured.execution(), anchor);
    assert_eq!(captured.checkpoint(), store.checkpoint());
    assert_eq!(
        store.with_finalized_audit_anchor(&captured, || {
            assert!(store.inner.try_lock().is_err());
            7
        }),
        Some(7)
    );
    let restored = ConsensusStore::open(dir.path(), None).unwrap();
    assert_eq!(
        restored.restore_finalized_audit_anchor(captured.checkpoint(), anchor),
        Some(captured)
    );
    for field in 0..5 {
        let mut changed = anchor;
        match field {
            0 => changed.block_number += 1,
            1 => changed.block_hash = B256::repeat_byte(91),
            2 => changed.receipts_root = B256::repeat_byte(92),
            3 => changed.beacon_root = B256::repeat_byte(93),
            _ => changed.beacon_slot += 1,
        }
        assert!(
            store
                .restore_finalized_audit_anchor(captured.checkpoint(), changed)
                .is_none()
        );
        store.replace_anchors(vec![record(changed, None)]).unwrap();
        assert!(
            store
                .with_finalized_audit_anchor(&captured, || panic!("changed anchor admitted"))
                .is_none()
        );
    }
    store.replace_anchors(vec![]).unwrap();
    assert!(
        store
            .with_finalized_audit_anchor(&captured, || ())
            .is_none()
    );
    let mut fake = record(anchor, None);
    fake.finalized = true;
    store.replace_anchors(vec![fake]).unwrap();
    store.inner.lock().unwrap().verified_light_client_store = None;
    assert!(store.finalized_audit_anchor().is_none());
    assert!(
        store
            .restore_finalized_audit_anchor(captured.checkpoint(), anchor)
            .is_none()
    );
    assert!(
        store
            .with_finalized_audit_anchor(&captured, || ())
            .is_none()
    );
}

#[test]
fn finalized_audit_requires_same_trust_root_and_complete_link_to_new_finality() {
    let (_dir, store, tip) = fixture();
    let mut old = tip;
    old.block_number -= 2;
    old.beacon_slot -= 4;
    old.beacon_root = B256::repeat_byte(51);
    old.block_hash = B256::repeat_byte(52);
    let mut middle = old;
    middle.block_number += 1;
    middle.beacon_slot += 2;
    middle.beacon_root = B256::repeat_byte(53);
    middle.block_hash = B256::repeat_byte(54);
    let records = vec![
        record(old, None),
        record(middle, Some(old.beacon_root)),
        record(tip, Some(middle.beacon_root)),
    ];
    store.replace_anchors(records.clone()).unwrap();
    let captured = store
        .restore_finalized_audit_anchor(store.checkpoint(), old)
        .unwrap();
    assert_eq!(store.with_finalized_audit_anchor(&captured, || 8), Some(8));
    let mut checkpoint = captured.checkpoint();
    checkpoint.beacon_root = B256::repeat_byte(91);
    assert!(
        store
            .restore_finalized_audit_anchor(checkpoint, old)
            .is_none()
    );
    for change in 0..4 {
        let mut changed = records.clone();
        match change {
            0 => {
                changed.remove(1);
            }
            1 => changed[1].parent_beacon_root = None,
            2 => changed[2].parent_beacon_root = Some(B256::repeat_byte(99)),
            _ => changed[1].anchor.beacon_slot = old.beacon_slot,
        }
        store.replace_anchors(changed).unwrap();
        assert!(
            store
                .restore_finalized_audit_anchor(captured.checkpoint(), old)
                .is_none()
        );
        assert!(
            store
                .with_finalized_audit_anchor(&captured, || panic!("disconnected finality admitted"))
                .is_none()
        );
    }
    store.replace_anchors(records).unwrap();
    assert_eq!(store.with_finalized_audit_anchor(&captured, || 9), Some(9));
    store.inner.lock().unwrap().checkpoint = checkpoint;
    assert!(
        store
            .with_finalized_audit_anchor(&captured, || ())
            .is_none()
    );
}
