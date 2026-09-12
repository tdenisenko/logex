//! Focused retained-format decode benchmark. Run with `--release -- --ignored --nocapture`.

use std::time::Instant;

use alloy_primitives::{Address, B256, Bytes, keccak256};
use logex_storage::SegmentReader;
use logex_storage::native::{NativeStorage, NativeStorageConfig};
use logex_types::{LogRow, Source};
use serde_json::json;
use tempfile::TempDir;

const ROWS: usize = 49_157;
const DEFAULT_REPEATS: usize = 50;
const MAX_REPEATS: usize = 10_000;
const ONE_ROW: [u32; 1] = [32_770];
const SHUFFLED_DUPLICATES: [u32; 6] = [49_156, 0, 16_384, 32_768, 16_384, 5];

fn payload(index: usize) -> Bytes {
    let len = 1 + index.wrapping_mul(37) % 127;
    Bytes::from(
        (0..len)
            .map(|byte| {
                (index as u8)
                    .wrapping_mul(31)
                    .wrapping_add((byte as u8).wrapping_mul(17))
                    .wrapping_add((byte >> 3) as u8)
            })
            .collect::<Vec<_>>(),
    )
}

fn fixture() -> Vec<LogRow> {
    (0..ROWS)
        .map(|index| {
            let data = payload(index);
            let source = if index.is_multiple_of(3) {
                Source::Trace
            } else {
                Source::Receipt
            };
            let block_offset = index as u64 / 4;
            let block_number = 15_000_000 + block_offset;
            let block_hash = keccak256(block_number.to_le_bytes());
            let mut tx_key = block_hash.to_vec();
            tx_key.extend_from_slice(&((index % 4) as u32).to_le_bytes());
            LogRow {
                block_number,
                block_hash,
                timestamp: 1_700_000_000 + block_offset * 12,
                tx_hash: keccak256(tx_key),
                tx_index: (index % 4) as u32,
                log_index: (index % 4) as u32,
                address: Address::repeat_byte(index.wrapping_mul(13) as u8),
                topic0: Some(B256::repeat_byte(0xaa)),
                topic1: None,
                topic2: None,
                topic3: None,
                data_len: data.len() as u32,
                data,
                source,
            }
        })
        .collect()
}

fn timed<T, F>(name: &str, repeats: usize, expected: &[T], mut read: F)
where
    T: PartialEq,
    F: FnMut() -> std::io::Result<Vec<T>>,
{
    assert!(read().unwrap() == expected, "{name} warm read differs");
    for iteration in 0..repeats {
        let start = Instant::now();
        let actual = read().unwrap();
        let elapsed = start.elapsed();
        assert!(actual == expected, "{name} measured read differs");
        println!(
            "{}",
            json!({
                "kind": "sample", "metric": name, "iteration": iteration,
                "elapsed_ms": elapsed.as_secs_f64() * 1000.0,
                "work_rows": actual.len(),
            })
        );
    }
}

#[test]
#[ignore = "focused retained-format decode performance baseline"]
fn benchmark_projected_page_decoding() {
    let repeats = std::env::var("LOGEX_PAGE_DECODE_REPEATS")
        .map(|value| {
            value
                .parse::<usize>()
                .ok()
                .filter(|value| (1..=MAX_REPEATS).contains(value))
                .expect("LOGEX_PAGE_DECODE_REPEATS must be between 1 and 10000")
        })
        .unwrap_or(DEFAULT_REPEATS);
    let rows = fixture();
    let tmp = TempDir::new().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_path_buf(),
        hot_target_rows: ROWS as u64,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    let appended = storage.write_historical_batch(&rows).unwrap();
    assert_eq!(appended.len(), 1);
    let data_reader = SegmentReader::open_projected(&appended[0].path, &["data"]).unwrap();
    let source_reader = SegmentReader::open_projected(&appended[0].path, &["source"]).unwrap();

    let expected_one = ONE_ROW
        .iter()
        .map(|&row| payload(row as usize))
        .collect::<Vec<_>>();
    let expected_shuffled = SHUFFLED_DUPLICATES
        .iter()
        .map(|&row| payload(row as usize))
        .collect::<Vec<_>>();
    let expected_full = (0..ROWS).map(payload).collect::<Vec<_>>();
    let expected_sources = (0..ROWS)
        .map(|index| u8::from(index.is_multiple_of(3)))
        .collect::<Vec<_>>();

    println!(
        "{}",
        json!({
            "kind": "config", "fixture_version": 1,
            "rows": ROWS,
            "page_rows": 16_384,
            "payload_length": "1 + (row * 37 % 127)",
            "fixture_digest": keccak256(serde_json::to_vec(&rows).unwrap()).to_string(),
            "one_row": ONE_ROW,
            "shuffled_duplicates": SHUFFLED_DUPLICATES,
            "repeats": repeats,
            "cache": "captured readers; each path warmed once; OS cache not evicted",
        })
    );
    timed("data_one_row", repeats, &expected_one, || {
        data_reader.read_var_bytes("data", Some(&ONE_ROW))
    });
    timed(
        "data_shuffled_duplicates",
        repeats,
        &expected_shuffled,
        || data_reader.read_var_bytes("data", Some(&SHUFFLED_DUPLICATES)),
    );
    timed("data_full", repeats, &expected_full, || {
        data_reader.read_var_bytes("data", None)
    });
    timed("source_dictionary_full", repeats, &expected_sources, || {
        source_reader.read_u8("source", None)
    });
}
