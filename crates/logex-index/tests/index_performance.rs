//! Opt-in release fixture for index I/O. Every measured lookup is checked
//! against the generated rows after timing ends. This writes only a disposable
//! temporary tree; assertions and returned bitmap destruction are not timed.
use std::hint::black_box;
use std::time::Instant;

use alloy_primitives::{Address, B256, Bytes};
use logex_index::{
    BTreeIndex, BTreeIndexReader, ERC20_EVENTS_BLOOM_FILE, Erc20EventBloom, Erc20EventBloomReader,
    transfer_topic0,
};
use logex_storage::ColumnFile;
use logex_types::{LogRow, Source};

fn word(value: u64) -> B256 {
    B256::left_padding_from(&value.to_be_bytes())
}

fn parameter(name: &str, default: usize, max: usize) -> usize {
    let value = std::env::var(name)
        .map(|value| value.parse::<usize>().expect("integer benchmark parameter"))
        .unwrap_or(default);
    assert!(value > 0 && value <= max, "{name} out of benchmark bounds");
    value
}

fn sample<T>(name: &str, iteration: usize, run: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let result = black_box(run());
    let elapsed_ns = start.elapsed().as_nanos();
    println!("index_sample metric={name} iteration={iteration} elapsed_ns={elapsed_ns}");
    result
}

#[test]
#[ignore = "release performance fixture; configure and retain repeated equivalent runs"]
fn index_io_performance() {
    let keys = parameter("LOGEX_INDEX_KEYS", 8192, 131072);
    let rows = parameter("LOGEX_INDEX_ROWS", 65536, 1048576);
    let repeats = parameter("LOGEX_INDEX_REPEATS", 50, 1000);
    let writes = parameter("LOGEX_INDEX_WRITES", 10, 100);
    assert!(rows >= keys && rows.is_multiple_of(keys));
    println!("index_config keys={keys} rows={rows} repeats={repeats} writes={writes}");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("numeric.bptree");
    let mut index = BTreeIndex::new(8);
    for row in 0..rows {
        index.insert(&((row % keys * 2) as u64).to_be_bytes(), row as u32);
    }
    index.write_to_file(&path).unwrap();
    // Warm the relevant file path once. No cache eviction is attempted.
    black_box(BTreeIndexReader::open(&path).unwrap());
    let expected_per_key = (rows / keys) as u64;
    for iteration in 0..repeats {
        let key = ((iteration * 7919 % keys) * 2) as u64;
        let key_bytes = key.to_be_bytes();
        let absent_key_bytes = (key + 1).to_be_bytes();
        let found = sample("point_present", iteration, || {
            BTreeIndexReader::get_from_file(&path, &key_bytes)
        })
        .unwrap()
        .unwrap();
        assert_eq!(found.len(), expected_per_key);
        assert!(
            found
                .iter()
                .all(|row| (row as usize) < rows && (row as usize % keys * 2) as u64 == key)
        );
        black_box(found);
        let absent = sample("point_absent", iteration, || {
            BTreeIndexReader::get_from_file(&path, &absent_key_bytes)
        })
        .unwrap();
        assert!(absent.is_none());
        let upper = (keys / 16).max(1) as u64 * 2;
        let lower_bytes = 0u64.to_be_bytes();
        let upper_bytes = upper.to_be_bytes();
        let found = sample("open_range", iteration, || {
            let reader = BTreeIndexReader::open(&path)?;
            // Keep full-reader destruction timed, but return the result bitmap
            // so its oracle scan and destruction are outside the measurement.
            Ok::<_, std::io::Error>(reader.range(&lower_bytes, &upper_bytes))
        })
        .unwrap();
        assert_eq!(found.len(), expected_per_key * (upper / 2));
        assert!(
            found
                .iter()
                .all(|row| (row as usize) < rows && (row as usize % keys * 2) < upper as usize)
        );
        black_box(found);
    }
    for iteration in 0..writes {
        sample("write_btree", iteration, || index.write_to_file(&path)).unwrap();
    }
    println!(
        "index_disk btree_bytes={}",
        std::fs::metadata(&path).unwrap().len()
    );

    let partition = dir.path().join("partition");
    let indexes = partition.join("indexes");
    let event = transfer_topic0();
    let logs: Vec<_> = (0..rows)
        .map(|row| LogRow {
            block_number: 100 + row as u64 / 64,
            block_hash: B256::repeat_byte(1),
            timestamp: 1700000000 + row as u64 / 64,
            tx_hash: B256::repeat_byte(2),
            tx_index: (row % 64) as u32,
            log_index: (row % 64) as u32,
            address: Address::from_word(word((row % 257 + 1) as u64)),
            topic0: Some(event),
            topic1: Some(word((row + 1) as u64)),
            topic2: Some(word((row + rows + 1) as u64)),
            topic3: None,
            data: Bytes::new(),
            data_len: 0,
            source: Source::Receipt,
        })
        .collect();
    ColumnFile::write_batch(&partition, &logs).unwrap();
    std::fs::create_dir_all(&indexes).unwrap();
    Erc20EventBloom::build(&partition, &indexes).unwrap();
    let bloom_path = indexes.join(ERC20_EVENTS_BLOOM_FILE);
    let mut bloom = Erc20EventBloomReader::open(&bloom_path).unwrap();
    for iteration in 0..repeats {
        let row = &logs[iteration * 7919 % rows];
        let topic = row.topic1.unwrap();
        let present = sample("bloom_present_open", iteration, || {
            Erc20EventBloom::may_contain_from_file(&bloom_path, &event, &row.address, 1, &topic)
        })
        .unwrap();
        assert!(present);
        let present = sample("bloom_present_reuse", iteration, || {
            bloom.may_contain(&event, &row.address, 1, &topic)
        })
        .unwrap();
        assert!(present);
        let absent = word((rows + iteration + 1) as u64);
        let may_contain = sample("bloom_absent_open", iteration, || {
            Erc20EventBloom::may_contain_from_file(&bloom_path, &event, &row.address, 1, &absent)
        })
        .unwrap();
        // A bloom false positive is valid; only errors or false negatives
        // on the known-present path violate its contract.
        black_box(may_contain);
    }
    drop(bloom);
    for iteration in 0..writes {
        sample("build_bloom", iteration, || {
            Erc20EventBloom::build(&partition, &indexes)
        })
        .unwrap();
    }
    println!(
        "index_disk bloom_bytes={}",
        std::fs::metadata(&bloom_path).unwrap().len()
    );
}
