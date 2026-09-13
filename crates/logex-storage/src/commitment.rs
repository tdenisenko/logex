//! Grouping-independent identity of a segment's logical row prefix.
use alloy_primitives::FixedBytes;
use logex_types::LogRow;

pub(crate) type Commitment = FixedBytes<32>;

pub(crate) fn empty(namespace: [u8; 16]) -> Commitment {
    let mut hash = blake3::Hasher::new();
    hash.update(b"logex.logical-prefix.empty.v1\0");
    hash.update(&namespace);
    FixedBytes::from(*hash.finalize().as_bytes())
}

pub(crate) fn extend(mut previous: Commitment, rows: &[LogRow]) -> Commitment {
    for row in rows {
        let mut hash = blake3::Hasher::new();
        hash.update(b"logex.logical-prefix.row.v1\0");
        hash.update(previous.as_slice());
        hash.update(&row.block_number.to_le_bytes());
        hash.update(row.block_hash.as_slice());
        hash.update(&row.timestamp.to_le_bytes());
        hash.update(row.tx_hash.as_slice());
        hash.update(&row.tx_index.to_le_bytes());
        hash.update(&row.log_index.to_le_bytes());
        hash.update(row.address.as_slice());
        for topic in [&row.topic0, &row.topic1, &row.topic2, &row.topic3] {
            match topic {
                Some(topic) => {
                    hash.update(&[1]);
                    hash.update(topic.as_slice());
                }
                None => {
                    hash.update(&[0]);
                }
            }
        }
        hash.update(&row.data_len.to_le_bytes());
        hash.update(&(row.data.len() as u64).to_le_bytes());
        hash.update(&row.data);
        hash.update(&[row.source as u8]);
        previous = FixedBytes::from(*hash.finalize().as_bytes());
    }
    previous
}

pub(crate) fn verify(
    namespace: [u8; 16],
    expected: Option<Commitment>,
    rows: &[LogRow],
) -> std::io::Result<()> {
    if let Some(expected) = expected
        && extend(empty(namespace), rows) != expected
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "committed logical prefix differs from its content commitment",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct AppendRevision {
    pub(crate) previous: Option<Commitment>,
    pub(crate) next: Option<Commitment>,
}

impl AppendRevision {
    pub(crate) fn new(previous: Option<Commitment>, rows: &[LogRow]) -> Self {
        Self {
            previous,
            next: previous.map(|root| extend(root, rows)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes};
    use logex_types::Source;

    fn row() -> LogRow {
        LogRow {
            block_number: 1,
            block_hash: B256::repeat_byte(2),
            timestamp: 3,
            tx_hash: B256::repeat_byte(4),
            tx_index: 5,
            log_index: 6,
            address: Address::repeat_byte(7),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::from_static(b"ab"),
            data_len: 2,
            source: Source::Receipt,
        }
    }

    #[test]
    fn logical_prefix_is_independent_of_append_and_replay_grouping() {
        let mut second = row();
        second.block_number += 1;
        let mut third = row();
        third.source = Source::Trace;
        let rows = [row(), second, third];
        let seed = empty([1; 16]);
        let expected = extend(seed, &rows);
        for split in 0..=rows.len() {
            assert_eq!(
                extend(extend(seed, &rows[..split]), &rows[split..]),
                expected
            );
        }
        assert_ne!(extend(empty([2; 16]), &rows), expected);
        assert_ne!(
            extend(seed, &[rows[1].clone(), rows[0].clone(), rows[2].clone()]),
            expected
        );
        assert_ne!(extend(seed, &rows[..2]), expected);
        assert!(verify([1; 16], Some(expected), &rows).is_ok());
        assert!(verify([1; 16], Some(expected), &rows[..2]).is_err());
    }

    #[test]
    fn logical_encoding_matches_the_documented_flat_bytes() {
        // Independent flat encoding of row(), intentionally not derived through
        // LogRow accessors or the streaming encoder. Guards persisted v1 order.
        let mut encoded = b"logex.logical-prefix.empty.v1\0".to_vec();
        encoded.extend_from_slice(&[1; 16]);
        let seed = *blake3::hash(&encoded).as_bytes();
        let mut encoded = b"logex.logical-prefix.row.v1\0".to_vec();
        encoded.extend_from_slice(&seed);
        encoded.extend_from_slice(&[1, 0, 0, 0, 0, 0, 0, 0]);
        encoded.extend_from_slice(&[2; 32]);
        encoded.extend_from_slice(&[3, 0, 0, 0, 0, 0, 0, 0]);
        encoded.extend_from_slice(&[4; 32]);
        encoded.extend_from_slice(&[5, 0, 0, 0, 6, 0, 0, 0]);
        encoded.extend_from_slice(&[7; 20]);
        encoded.extend_from_slice(&[0, 0, 0, 0]); // absent topics
        encoded.extend_from_slice(&[2, 0, 0, 0]); // declared data length
        encoded.extend_from_slice(&[2, 0, 0, 0, 0, 0, 0, 0]); // actual length
        encoded.extend_from_slice(b"ab\0"); // data, Receipt discriminator
        assert_eq!(
            extend(empty([1; 16]), &[row()]).as_slice(),
            blake3::hash(&encoded).as_bytes()
        );
    }

    #[test]
    fn commitment_covers_every_logical_field_and_topic_presence() {
        let original = row();
        let root = extend(empty([1; 16]), std::slice::from_ref(&original));
        let changes: [fn(&mut LogRow); 14] = [
            |r| r.block_number += 1,
            |r| r.block_hash = B256::ZERO,
            |r| r.timestamp += 1,
            |r| r.tx_hash = B256::ZERO,
            |r| r.tx_index += 1,
            |r| r.log_index += 1,
            |r| r.address = Address::ZERO,
            |r| r.topic0 = Some(B256::ZERO),
            |r| r.topic1 = Some(B256::ZERO),
            |r| r.topic2 = Some(B256::ZERO),
            |r| r.topic3 = Some(B256::ZERO),
            |r| r.data = Bytes::from_static(b"ac"),
            |r| r.data_len += 1,
            |r| r.source = Source::Trace,
        ];
        for (field, change) in changes.iter().enumerate() {
            let mut changed = original.clone();
            change(&mut changed);
            assert_ne!(extend(empty([1; 16]), &[changed]), root, "field {field}");
        }
    }
}
