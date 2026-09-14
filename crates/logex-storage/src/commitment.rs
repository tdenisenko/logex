//! Grouping-independent identity of a segment's logical row prefix.
mod stream;

use alloy_primitives::FixedBytes;
use logex_types::LogRow;
use serde::{Deserialize, Serialize};
use std::io;
use stream::StreamState;

pub(crate) type Commitment = FixedBytes<32>;

/// Bounded append state for the ordered logical rows of one segment.
///
/// Published readers/indexes retain only `commitment()`. Writers and recovery
/// journals retain this state so extending a prefix never rereads old columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PrefixState {
    namespace: FixedBytes<16>,
    rows: u64,
    stream: StreamState,
    #[serde(skip)]
    root: Commitment,
}

impl PrefixState {
    pub(crate) const MAX_ENCODED_BYTES: usize = 24 + stream::MAX_ENCODED_BYTES;

    pub(crate) fn empty(namespace: [u8; 16]) -> Self {
        Self::from_parts(namespace.into(), 0, StreamState::new())
    }

    pub(crate) fn from_rows(namespace: [u8; 16], rows: &[LogRow]) -> io::Result<Self> {
        Self::empty(namespace).extend(rows)
    }

    #[cfg(test)]
    pub(crate) fn row_count(&self) -> u64 {
        self.rows
    }

    pub(crate) fn commitment(&self) -> Commitment {
        self.root
    }

    fn from_parts(namespace: FixedBytes<16>, rows: u64, stream: StreamState) -> Self {
        // Cache the fixed-size published identity when state changes. Catalog
        // validation and query metadata copies must not rehash every segment.
        let mut hash = blake3::Hasher::new();
        hash.update(b"logex.logical-prefix.stream.v2\0");
        hash.update(namespace.as_slice());
        hash.update(&rows.to_le_bytes());
        hash.update(&stream.byte_len().to_le_bytes());
        hash.update(stream.digest().as_slice());
        Self {
            namespace,
            rows,
            stream,
            root: FixedBytes::from(*hash.finalize().as_bytes()),
        }
    }

    pub(crate) fn extend(&self, rows: &[LogRow]) -> io::Result<Self> {
        let row_count = self
            .rows
            .checked_add(u64::try_from(rows.len()).map_err(io::Error::other)?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "logical row count overflow")
            })?;
        let mut encoded = self.stream.clone().writer();
        for row in rows {
            encoded.put(&row.block_number.to_le_bytes())?;
            encoded.put(row.block_hash.as_slice())?;
            encoded.put(&row.timestamp.to_le_bytes())?;
            encoded.put(row.tx_hash.as_slice())?;
            encoded.put(&row.tx_index.to_le_bytes())?;
            encoded.put(&row.log_index.to_le_bytes())?;
            encoded.put(row.address.as_slice())?;
            for topic in [&row.topic0, &row.topic1, &row.topic2, &row.topic3] {
                if let Some(topic) = topic {
                    encoded.put(&[1])?;
                    encoded.put(topic.as_slice())?;
                } else {
                    encoded.put(&[0])?;
                }
            }
            encoded.put(&row.data_len.to_le_bytes())?;
            encoded.put(
                &u64::try_from(row.data.len())
                    .map_err(io::Error::other)?
                    .to_le_bytes(),
            )?;
            encoded.put(&row.data)?;
            encoded.put(&[row.source as u8])?;
        }
        Ok(Self::from_parts(
            self.namespace,
            row_count,
            encoded.finish()?,
        ))
    }

    /// Validate every fallible encoding boundary before a new bundle mutates
    /// files. Only the historical empty-prefix overlap path needs this pass.
    pub(crate) fn validate_new_append(
        &self,
        namespace: [u8; 16],
        root: Commitment,
        rows: &[LogRow],
    ) -> io::Result<()> {
        self.validate(namespace, 0, root)?;
        self.rows
            .checked_add(u64::try_from(rows.len()).map_err(io::Error::other)?)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "logical row count overflow")
            })?;
        let mut bytes = self.stream.byte_len();
        for row in rows {
            let topics = [&row.topic0, &row.topic1, &row.topic2, &row.topic3]
                .into_iter()
                .filter(|topic| topic.is_some())
                .count() as u64;
            bytes = checked_row_end(
                bytes,
                topics,
                u64::try_from(row.data.len()).map_err(io::Error::other)?,
            )?;
        }
        Ok(())
    }

    pub(crate) fn validate(
        &self,
        namespace: [u8; 16],
        rows: u64,
        root: Commitment,
    ) -> io::Result<()> {
        if self.namespace.0 != namespace || self.rows != rows || self.commitment() != root {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "logical append state differs from its published prefix",
            ));
        }
        // Each encoded row contains at least125 bytes. Bounds prevent impossible
        // row/byte combinations even when persisted metadata is self-consistent.
        if (rows == 0) != (self.stream.byte_len() == 0)
            || rows
                .checked_mul(125)
                .is_none_or(|minimum| self.stream.byte_len() < minimum)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid logical append row/byte boundary",
            ));
        }
        Ok(())
    }

    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let stream = self.stream.to_bytes();
        let mut bytes = Vec::with_capacity(24 + stream.len());
        bytes.extend_from_slice(self.namespace.as_slice());
        bytes.extend_from_slice(&self.rows.to_le_bytes());
        bytes.extend_from_slice(&stream);
        bytes
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if !(24..=Self::MAX_ENCODED_BYTES).contains(&bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid logical append state length",
            ));
        }
        let state = Self::from_parts(
            FixedBytes::from_slice(&bytes[..16]),
            u64::from_le_bytes(bytes[16..24].try_into().map_err(io::Error::other)?),
            StreamState::from_bytes(&bytes[24..])?,
        );
        state.validate(state.namespace.0, state.rows, state.root)?;
        Ok(state)
    }
}

fn checked_row_end(previous: u64, topics: u64, payload: u64) -> io::Result<u64> {
    // Private callers count the four topic slots; the fixed part cannot overflow.
    debug_assert!(topics <= 4);
    previous
        .checked_add(125 + 32 * topics)
        .and_then(|bytes| bytes.checked_add(payload))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "logical transcript length overflow",
            )
        })
}

impl<'de> Deserialize<'de> for PrefixState {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            namespace: FixedBytes<16>,
            rows: u64,
            stream: StreamState,
        }
        let wire = Wire::deserialize(deserializer)?;
        let state = Self::from_parts(wire.namespace, wire.rows, wire.stream);
        state
            .validate(state.namespace.0, state.rows, state.root)
            .map_err(serde::de::Error::custom)?;
        Ok(state)
    }
}

pub(crate) fn verify(
    namespace: [u8; 16],
    expected: Option<Commitment>,
    rows: &[LogRow],
) -> std::io::Result<Option<PrefixState>> {
    let Some(expected) = expected else {
        return Ok(None);
    };
    let state = PrefixState::from_rows(namespace, rows)?;
    if state.commitment() != expected {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "committed logical prefix differs from its content commitment",
        ));
    }
    Ok(Some(state))
}

pub(crate) fn validate_state(
    namespace: Option<FixedBytes<16>>,
    rows: u64,
    root: Option<Commitment>,
    state: Option<&PrefixState>,
) -> io::Result<()> {
    match (namespace, root, state) {
        (Some(namespace), Some(root), Some(state)) => state.validate(namespace.0, rows, root),
        (_, None, None) => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "missing or orphan logical append state",
        )),
    }
}

#[derive(Clone)]
pub(crate) struct AppendRevision {
    pub(crate) previous: Option<Commitment>,
    pub(crate) next: Option<Commitment>,
    pub(crate) state: Option<PrefixState>,
}

impl AppendRevision {
    pub(crate) fn new(previous: Option<&PrefixState>, rows: &[LogRow]) -> io::Result<Self> {
        let state = previous.map(|state| state.extend(rows)).transpose()?;
        Ok(Self {
            previous: previous.map(PrefixState::commitment),
            next: state.as_ref().map(PrefixState::commitment),
            state,
        })
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
        let seed = PrefixState::empty([1; 16]);
        let expected = seed.extend(&rows).unwrap();
        for split in 0..=rows.len() {
            assert_eq!(
                seed.extend(&rows[..split])
                    .unwrap()
                    .extend(&rows[split..])
                    .unwrap(),
                expected
            );
        }
        assert_ne!(
            PrefixState::from_rows([2; 16], &rows).unwrap().commitment(),
            expected.commitment()
        );
        assert_ne!(
            seed.extend(&[rows[1].clone(), rows[0].clone(), rows[2].clone()])
                .unwrap(),
            expected
        );
        assert_ne!(seed.extend(&rows[..2]).unwrap(), expected);
        assert!(verify([1; 16], Some(expected.commitment()), &rows).is_ok());
        assert!(verify([1; 16], Some(expected.commitment()), &rows[..2]).is_err());
    }

    #[test]
    fn logical_encoding_matches_the_documented_flat_bytes() {
        // Independent flat logical encoding of row(), intentionally not derived
        // through its accessors or the streaming encoder.
        let mut encoded = Vec::new();
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
        let mut identity = b"logex.logical-prefix.stream.v2\0".to_vec();
        identity.extend_from_slice(&[1; 16]);
        identity.extend_from_slice(&1u64.to_le_bytes());
        identity.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
        identity.extend_from_slice(blake3::hash(&encoded).as_bytes());
        let state = PrefixState::from_rows([1; 16], &[row()]).unwrap();
        assert_eq!(
            state.commitment().as_slice(),
            blake3::hash(&identity).as_bytes()
        );
        assert_eq!(PrefixState::from_bytes(&state.to_bytes()).unwrap(), state);
        state.validate([1; 16], 1, state.commitment()).unwrap();
        assert!(state.validate([2; 16], 1, state.commitment()).is_err());
        assert!(state.validate([1; 16], 2, state.commitment()).is_err());
    }

    #[test]
    fn buffered_bytes_match_flat_hash_across_flush_boundaries() {
        let bytes: Vec<u8> = (0..2 * 64 * 1024 + 1).map(|i| (i % 251) as u8).collect();
        for prefix_len in [0, 1, 1024] {
            for first_len in [0, 1, 32, 1024, 65535, 65536, 65537, 131073] {
                let mut stream = StreamState::new();
                stream.update(&bytes[..prefix_len]).unwrap();
                let mut flat = blake3::Hasher::new();
                flat.update(&bytes[..prefix_len]);
                let mut buffered = stream.writer();
                // Cover empty input, exact fills, split fields and inputs that
                // span several buffers, followed by ordinary small fields.
                for len in [first_len, 0, 1, 4, 8, 20, 32, 65536, 131073, 0, 1] {
                    buffered.put(&bytes[..len]).unwrap();
                    flat.update(&bytes[..len]);
                }
                let stream = buffered.finish().unwrap();
                assert_eq!(stream.digest().as_slice(), flat.finalize().as_bytes());
            }
        }
    }

    #[test]
    fn failed_extension_does_not_change_published_prefix() {
        // A shape-valid opaque frontier near the byte-count limit makes an
        // actual row append overflow without allocating an enormous prefix.
        let mut encoded = vec![1];
        encoded.extend_from_slice(&u64::MAX.to_le_bytes());
        encoded.resize(9 + 54 * 32 + 1023, 0x5a);
        let stream = StreamState::from_bytes(&encoded).unwrap();
        let state = PrefixState::from_parts([1; 16].into(), 1, stream);
        let before = state.to_bytes();
        let root = state.commitment();
        assert!(state.extend(&[row()]).is_err());
        assert_eq!(state.to_bytes(), before);
        assert_eq!(state.commitment(), root);
    }

    #[test]
    fn commitment_covers_every_logical_field_and_topic_presence() {
        let original = row();
        let root = PrefixState::from_rows([1; 16], std::slice::from_ref(&original))
            .unwrap()
            .commitment();
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
            assert_ne!(
                PrefixState::from_rows([1; 16], &[changed])
                    .unwrap()
                    .commitment(),
                root,
                "field {field}"
            );
        }
    }

    #[test]
    fn new_bundle_preflight_checks_empty_origin_and_exact_length_without_restricting_data_len() {
        let empty = PrefixState::empty([1; 16]);
        let mut value = row();
        value.data_len += 1;
        empty
            .validate_new_append([1; 16], empty.commitment(), &[value])
            .unwrap();
        let nonempty = empty.extend(&[row()]).unwrap();
        assert!(
            nonempty
                .validate_new_append([1; 16], nonempty.commitment(), &[])
                .is_err()
        );
        assert!(
            empty
                .validate_new_append([2; 16], empty.commitment(), &[])
                .is_err()
        );
        assert_eq!(checked_row_end(7, 4, 3).unwrap(), 263);
        assert_eq!(checked_row_end(0, 0, u64::MAX - 125).unwrap(), u64::MAX);
        assert!(checked_row_end(0, 0, u64::MAX - 124).is_err());
        assert!(checked_row_end(u64::MAX, 0, 0).is_err());
        let mut wire = vec![1];
        wire.extend_from_slice(&u64::MAX.to_le_bytes());
        wire.resize(9 + 54 * 32 + 1023, 0x5a);
        let forged =
            PrefixState::from_parts([1; 16].into(), 0, StreamState::from_bytes(&wire).unwrap());
        assert!(
            forged
                .validate_new_append([1; 16], forged.commitment(), &[row()])
                .is_err()
        );
    }
}
