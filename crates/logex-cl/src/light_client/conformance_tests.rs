//! Unchanged official random SSZ vectors: serialization, not authentication.
use super::*;
use serde_json::Value;
use ssz::Encode;
use std::path::PathBuf;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/consensus-spec-tests")
}

fn check_header(beacon: &BeaconBlockHeaderSsz, execution: Value, expected: &Value) {
    assert_eq!(serde_json::to_value(beacon).unwrap(), expected["beacon"]);
    for (field, value) in expected["execution"].as_object().unwrap() {
        if field == "extra_data" {
            let bytes: Vec<u8> = serde_json::from_value(execution[field].clone()).unwrap();
            assert_eq!(
                format!("0x{}", alloy_primitives::hex::encode(bytes)),
                value.as_str().unwrap()
            );
        } else {
            assert_eq!(&execution[field], value, "execution field {field}");
        }
    }
    assert_eq!(
        beacon_block_header_root(beacon).to_string(),
        expected["beacon_root"].as_str().unwrap()
    );
}

fn check_aggregate(aggregate: &SyncAggregateRaw, signature_slot: u64, expected: &Value) {
    assert_eq!(signature_slot, expected["signature_slot"].as_u64().unwrap());
    assert_eq!(
        aggregate.sync_committee_bits.to_string(),
        expected["sync_committee_bits"].as_str().unwrap()
    );
    assert_eq!(
        aggregate.sync_committee_signature.to_string(),
        expected["sync_committee_signature"].as_str().unwrap()
    );
}

#[test]
fn official_multifork_ssz_vectors_select_layout_and_preserve_fields_and_bytes() {
    let root = fixture_root();
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(root.join("provenance.json")).unwrap()).unwrap();
    let actual_cases: std::collections::BTreeSet<_> = manifest["cases"]
        .as_array()
        .unwrap()
        .iter()
        .map(|case| {
            (
                case["fork"].as_str().unwrap(),
                case["family"].as_str().unwrap(),
            )
        })
        .collect();
    let expected_cases: std::collections::BTreeSet<_> = ["capella", "deneb", "electra"]
        .into_iter()
        .flat_map(|fork| {
            [
                "LightClientBootstrap",
                "LightClientFinalityUpdate",
                "LightClientOptimisticUpdate",
                "LightClientUpdate",
            ]
            .map(|family| (fork, family))
        })
        .chain(std::iter::once(("fulu", "LightClientUpdate")))
        .collect();
    assert_eq!(manifest["cases"].as_array().unwrap().len(), 13);
    assert_eq!(actual_cases, expected_cases);
    for case in manifest["cases"].as_array().unwrap() {
        let fork = case["fork"].as_str().unwrap();
        let family = case["family"].as_str().unwrap();
        let directory = root.join(fork).join(family);
        let compressed = std::fs::read(directory.join("serialized.ssz_snappy")).unwrap();
        let bytes = snap::raw::Decoder::new()
            .decompress_vec(&compressed)
            .unwrap();
        let expected: Value =
            serde_json::from_slice(&std::fs::read(directory.join("expected.json")).unwrap())
                .unwrap();
        let layout = if fork == "fulu" { "electra" } else { fork };
        macro_rules! header {
            ($value:expr, $name:literal) => {
                check_header(
                    &$value.beacon,
                    serde_json::to_value(&$value.execution).unwrap(),
                    &expected["headers"][$name],
                );
            };
        }
        macro_rules! bootstrap {
            ($value:expr, $layout:literal) => {{
                assert_eq!(layout, $layout, "{fork}/{family}");
                header!($value.header, "header");
                $value.as_ssz_bytes()
            }};
        }
        macro_rules! finality {
            ($value:expr, $layout:literal) => {{
                assert_eq!(layout, $layout, "{fork}/{family}");
                header!($value.attested_header, "attested_header");
                header!($value.finalized_header, "finalized_header");
                check_aggregate(&$value.sync_aggregate, $value.signature_slot, &expected);
                $value.as_ssz_bytes()
            }};
        }
        let encoded = match family {
            "LightClientBootstrap" => match decode_bootstrap_payload(&bytes).unwrap() {
                DecodedBootstrap::Capella(p) => bootstrap!(p, "capella"),
                DecodedBootstrap::Deneb(p) => bootstrap!(p, "deneb"),
                DecodedBootstrap::Electra(p) => bootstrap!(p, "electra"),
            },
            "LightClientFinalityUpdate" => match decode_finality_update_payload(&bytes).unwrap() {
                DecodedFinalityUpdate::Capella(p) => finality!(p, "capella"),
                DecodedFinalityUpdate::Deneb(p) => finality!(p, "deneb"),
                DecodedFinalityUpdate::Electra(p) => finality!(p, "electra"),
            },
            "LightClientOptimisticUpdate" => {
                let expected_layout = if fork == "capella" {
                    "capella"
                } else {
                    "deneb"
                };
                match decode_optimistic_update_payload(&bytes).unwrap() {
                    DecodedOptimisticUpdate::Capella(p) => {
                        assert_eq!(expected_layout, "capella");
                        header!(p.attested_header, "attested_header");
                        check_aggregate(&p.sync_aggregate, p.signature_slot, &expected);
                        p.as_ssz_bytes()
                    }
                    DecodedOptimisticUpdate::Deneb(p) => {
                        assert_eq!(expected_layout, "deneb");
                        header!(p.attested_header, "attested_header");
                        check_aggregate(&p.sync_aggregate, p.signature_slot, &expected);
                        p.as_ssz_bytes()
                    }
                }
            }
            "LightClientUpdate" => match decode_update_payload(&bytes).unwrap() {
                DecodedUpdate::Capella(p) => finality!(p, "capella"),
                DecodedUpdate::Deneb(p) => finality!(p, "deneb"),
                DecodedUpdate::Electra(p) => finality!(p, "electra"),
            },
            _ => panic!("unexpected fixture family {family}"),
        };
        assert_eq!(encoded, bytes, "{fork}/{family} SSZ round trip");
    }
}

#[test]
fn official_fixture_files_match_pinned_provenance() {
    let root = fixture_root();
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(root.join("provenance.json")).unwrap()).unwrap();
    for case in manifest["cases"].as_array().unwrap() {
        let directory = root
            .join(case["fork"].as_str().unwrap())
            .join(case["family"].as_str().unwrap());
        for (name, expected) in case["files"].as_object().unwrap() {
            let bytes = std::fs::read(directory.join(name)).unwrap();
            assert_eq!(bytes.len() as u64, expected["bytes"].as_u64().unwrap());
            assert_eq!(
                alloy_primitives::hex::encode(Sha256::digest(&bytes)),
                expected["sha256"].as_str().unwrap()
            );
        }
    }
}
