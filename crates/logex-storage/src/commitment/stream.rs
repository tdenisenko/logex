//! Bounded, application-owned continuation state for an ordinary BLAKE3 stream.
//!
//! The frontier covers all chunks except the last, in descending power-of-two
//! subtree order. Keeping the last chunk's bytes (including a full final chunk)
//! preserves the root/non-root distinction without serializing Hasher internals.
use std::{fmt, io};

use alloy_primitives::FixedBytes;
use blake3::hazmat::{self, ChainingValue, HasherExt, Mode};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

const CHUNK_BYTES: usize = blake3::CHUNK_LEN;
const VERSION: u8 = 1;
const HEADER_BYTES: usize = 1 + 8;
const MAX_FRONTIER: usize = 54;
pub(crate) const MAX_ENCODED_BYTES: usize = HEADER_BYTES + MAX_FRONTIER * 32 + CHUNK_BYTES;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamState {
    total_bytes: u64,
    frontier: Vec<ChainingValue>,
    tail: Vec<u8>,
}

impl Default for StreamState {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamState {
    pub(crate) fn new() -> Self {
        Self {
            total_bytes: 0,
            frontier: Vec::new(),
            tail: Vec::new(),
        }
    }

    pub(crate) fn byte_len(&self) -> u64 {
        self.total_bytes
    }

    /// Append bytes without changing the state if the u64 byte count overflows.
    pub(crate) fn update(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        let added = u64::try_from(bytes.len()).map_err(|_| invalid("stream input is too long"))?;
        let new_total = self
            .total_bytes
            .checked_add(added)
            .ok_or_else(|| invalid("stream byte count overflows u64"))?;
        if bytes.is_empty() {
            return Ok(());
        }
        let mut completed = completed_chunks(self.total_bytes);
        if !self.tail.is_empty() {
            let take = bytes.len().min(CHUNK_BYTES - self.tail.len());
            self.tail.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if bytes.is_empty() {
                self.total_bytes = new_total;
                return Ok(());
            }
            // A full chunk ceases to be the final chunk only when more input
            // actually follows it. Never discard a possible single-chunk root.
            let cv = subtree_cv(completed, &self.tail);
            self.push_subtree(completed, 1, cv);
            completed += 1;
            self.tail.clear();
        }
        while bytes.len() > CHUNK_BYTES {
            // Leave 1..=CHUNK_BYTES bytes at the right edge. Hash the largest
            // complete power-of-two subtree allowed by both input and GLOBAL
            // offset alignment, so bulk updates can use BLAKE3's SIMD paths.
            let available = (bytes.len() - 1) / CHUNK_BYTES;
            let mut chunks = 1usize << (usize::BITS - 1 - available.leading_zeros());
            if completed != 0 {
                let alignment = 1u64 << completed.trailing_zeros();
                chunks = chunks.min(usize::try_from(alignment).unwrap_or(usize::MAX));
            }
            let len = chunks * CHUNK_BYTES;
            let cv = subtree_cv(completed, &bytes[..len]);
            self.push_subtree(completed, chunks as u64, cv);
            completed += chunks as u64;
            bytes = &bytes[len..];
        }
        self.tail.extend_from_slice(bytes);
        self.total_bytes = new_total;
        debug_assert_eq!(self.frontier.len(), completed.count_ones() as usize);
        debug_assert_eq!(self.tail.len(), tail_len(new_total));
        Ok(())
    }

    // `chunks` is a power of two dividing the starting completed-chunk count.
    // Binary carries merge precisely adjacent, equal-size subtree siblings.
    fn push_subtree(&mut self, completed: u64, chunks: u64, mut cv: ChainingValue) {
        debug_assert!(chunks.is_power_of_two());
        debug_assert_eq!(completed % chunks, 0);
        let mut carries = completed / chunks;
        while carries & 1 != 0 {
            let left = self.frontier.pop().expect("canonical stream frontier");
            cv = hazmat::merge_subtrees_non_root(&left, &cv, Mode::Hash);
            carries >>= 1;
        }
        self.frontier.push(cv);
    }

    pub(crate) fn digest(&self) -> FixedBytes<32> {
        if self.frontier.is_empty() {
            return FixedBytes::from(*blake3::hash(&self.tail).as_bytes());
        }
        let mut right = subtree_cv(completed_chunks(self.total_bytes), &self.tail);
        for left in self.frontier[1..].iter().rev() {
            right = hazmat::merge_subtrees_non_root(left, &right, Mode::Hash);
        }
        FixedBytes::from(
            *hazmat::merge_subtrees_root(&self.frontier[0], &right, Mode::Hash).as_bytes(),
        )
    }

    /// Canonical wire: version, little-endian byte count, descending frontier
    /// CVs, final chunk bytes. Count-derived lengths forbid alternate geometry.
    pub(crate) fn to_bytes(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(HEADER_BYTES + self.frontier.len() * 32 + self.tail.len());
        bytes.push(VERSION);
        bytes.extend_from_slice(&self.total_bytes.to_le_bytes());
        for cv in &self.frontier {
            bytes.extend_from_slice(cv);
        }
        bytes.extend_from_slice(&self.tail);
        bytes
    }

    /// Validate shape before allocating state. CV contents must additionally be
    /// bound to the caller's trusted published digest; past bytes aren't here.
    pub(crate) fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        if !(HEADER_BYTES..=MAX_ENCODED_BYTES).contains(&bytes.len()) {
            return Err(invalid("invalid stream state length"));
        }
        if bytes[0] != VERSION {
            return Err(invalid("unsupported stream state version"));
        }
        let total_bytes = u64::from_le_bytes(bytes[1..HEADER_BYTES].try_into().unwrap());
        let count = completed_chunks(total_bytes).count_ones() as usize;
        let tail_len = tail_len(total_bytes);
        let tail_start = HEADER_BYTES + count * 32;
        if bytes.len() != tail_start + tail_len {
            return Err(invalid(
                "stream byte count, frontier and tail length disagree",
            ));
        }
        let frontier = bytes[HEADER_BYTES..tail_start].as_chunks::<32>().0.to_vec();
        Ok(Self {
            total_bytes,
            frontier,
            tail: bytes[tail_start..].to_vec(),
        })
    }
}

fn completed_chunks(total: u64) -> u64 {
    total.saturating_sub(1) / CHUNK_BYTES as u64
}

fn tail_len(total: u64) -> usize {
    if total == 0 {
        0
    } else {
        ((total - 1) % CHUNK_BYTES as u64) as usize + 1
    }
}

fn subtree_cv(completed: u64, bytes: &[u8]) -> ChainingValue {
    let offset = completed * CHUNK_BYTES as u64;
    debug_assert!(!bytes.is_empty());
    debug_assert!(hazmat::max_subtree_len(offset).is_none_or(|max| bytes.len() as u64 <= max));
    let mut hash = blake3::Hasher::new();
    hash.set_input_offset(offset);
    hash.update(bytes);
    hash.finalize_non_root()
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl Serialize for StreamState {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        let bytes = self.to_bytes();
        let mut hex = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            hex.push(HEX[usize::from(byte >> 4)] as char);
            hex.push(HEX[usize::from(byte & 15)] as char);
        }
        serializer.serialize_str(&hex)
    }
}

impl<'de> Deserialize<'de> for StreamState {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct StateVisitor;
        impl de::Visitor<'_> for StateVisitor {
            type Value = StreamState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a bounded lowercase hexadecimal BLAKE3 stream state")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                if value.len() > MAX_ENCODED_BYTES * 2 || !value.len().is_multiple_of(2) {
                    return Err(E::custom("invalid hexadecimal stream state length"));
                }
                // No input-sized allocation: reject oversized strings before
                // decoding into a fixed buffer or allocating the validated state.
                let mut bytes = [0u8; MAX_ENCODED_BYTES];
                for (index, pair) in value.as_bytes().as_chunks::<2>().0.iter().enumerate() {
                    let nibble = |b| match b {
                        b'0'..=b'9' => Some(b - b'0'),
                        b'a'..=b'f' => Some(b - b'a' + 10),
                        _ => None,
                    };
                    let high = nibble(pair[0]).ok_or_else(|| E::custom("invalid state hex"))?;
                    let low = nibble(pair[1]).ok_or_else(|| E::custom("invalid state hex"))?;
                    bytes[index] = high * 16 + low;
                }
                StreamState::from_bytes(&bytes[..value.len() / 2]).map_err(E::custom)
            }
        }
        deserializer.deserialize_str(StateVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(len: usize) -> Vec<u8> {
        let mut seed = 0x243f_6a88_85a3_08d3u64;
        (0..len)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                seed as u8
            })
            .collect()
    }

    fn assert_oracle(state: &StreamState, bytes: &[u8]) {
        let mut oracle = blake3::Hasher::new();
        oracle.update(bytes);
        assert_eq!(state.byte_len(), bytes.len() as u64);
        assert_eq!(state.digest().as_slice(), oracle.finalize().as_bytes());
        assert_eq!(state.digest(), state.digest());
        assert_eq!(state.tail.len(), tail_len(state.byte_len()));
        assert_eq!(
            state.frontier.len(),
            completed_chunks(state.byte_len()).count_ones() as usize
        );
        let encoded = state.to_bytes();
        assert!(encoded.len() <= MAX_ENCODED_BYTES);
        assert_eq!(StreamState::from_bytes(&encoded).unwrap(), *state);
        let json = serde_json::to_string(state).unwrap();
        assert_eq!(serde_json::from_str::<StreamState>(&json).unwrap(), *state);
    }

    #[test]
    fn ordinary_hasher_matches_chunk_and_power_of_two_boundaries() {
        let bytes = input((1 << 18) + 1);
        let mut lengths: Vec<_> = (0..=2050).collect();
        for power in 0..=18 {
            let boundary = 1usize << power;
            lengths.extend([boundary.saturating_sub(1), boundary, boundary + 1]);
        }
        lengths.sort_unstable();
        lengths.dedup();
        for len in lengths {
            let mut state = StreamState::new();
            state.update(&bytes[..len]).unwrap();
            assert_oracle(&state, &bytes[..len]);
            if len != 0 && len % CHUNK_BYTES == 0 {
                assert_eq!(state.tail.len(), CHUNK_BYTES);
            }
        }
    }

    #[test]
    fn every_small_split_preserves_state_and_resume_digest() {
        let bytes = input(2051);
        let mut whole = StreamState::new();
        whole.update(&bytes).unwrap();
        for split in 0..=bytes.len() {
            let mut state = StreamState::new();
            state.update(&bytes[..split]).unwrap();
            assert_oracle(&state, &bytes[..split]);
            let saved = serde_json::to_string(&state).unwrap();
            state = serde_json::from_str(&saved).unwrap();
            state.update(&[]).unwrap();
            state.update(&bytes[split..]).unwrap();
            assert_eq!(state, whole, "split {split}");
            assert_oracle(&state, &bytes);
        }
    }

    #[test]
    fn aligned_bulk_and_random_splits_reload_at_every_publication() {
        let bytes = input(131_077);
        let mut whole = StreamState::new();
        whole.update(&bytes).unwrap();
        for fixed in [Some(1), Some(1024), Some(16_384), Some(16_385), None] {
            let mut state = StreamState::new();
            let mut offset = 0;
            let mut seed = 0x9e37_79b9u64;
            // The byte-at-a-time case covers enough bytes to cross chunk
            // boundaries without making serde/rehashed-prefix tests quadratic.
            let limit = if fixed == Some(1) { 3073 } else { bytes.len() };
            while offset < limit {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let take = fixed
                    .unwrap_or((seed as usize % 8193) + 1)
                    .min(limit - offset);
                state.update(&bytes[offset..offset + take]).unwrap();
                offset += take;
                assert_oracle(&state, &bytes[..offset]);
                state = StreamState::from_bytes(&state.to_bytes()).unwrap();
            }
            if limit == bytes.len() {
                assert_eq!(state, whole);
            }
        }
    }

    #[test]
    fn rejects_noncanonical_wire_shapes_and_unbounded_hex() {
        assert!(StreamState::from_bytes(&[]).is_err());
        assert!(StreamState::from_bytes(&[0; HEADER_BYTES - 1]).is_err());
        assert!(StreamState::from_bytes(&[0; MAX_ENCODED_BYTES + 1]).is_err());
        let mut wrong_version = StreamState::new().to_bytes();
        wrong_version[0] += 1;
        assert!(StreamState::from_bytes(&wrong_version).is_err());
        let bytes = input(3073);
        for len in [0, 1, 1024, 1025, 2048, 2049, 3072, 3073] {
            let mut state = StreamState::new();
            state.update(&bytes[..len]).unwrap();
            let encoded = state.to_bytes();
            let mut extra = encoded.clone();
            extra.push(0);
            assert!(StreamState::from_bytes(&extra).is_err());
            assert!(StreamState::from_bytes(&encoded[..encoded.len() - 1]).is_err());
            let mut wrong_count = encoded.clone();
            wrong_count[1..HEADER_BYTES].copy_from_slice(&(len as u64 + 1).to_le_bytes());
            assert!(StreamState::from_bytes(&wrong_count).is_err());
            if !state.frontier.is_empty() {
                let mut missing_frontier = encoded.clone();
                missing_frontier.drain(HEADER_BYTES..HEADER_BYTES + 32);
                assert!(StreamState::from_bytes(&missing_frontier).is_err());
            }
        }
        // A byte count claiming a giant tree cannot cause allocation before
        // the exact, bounded frontier/tail encoding is present.
        let mut giant_claim = vec![VERSION];
        giant_claim.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(StreamState::from_bytes(&giant_claim).is_err());
        for json in [
            "null".to_owned(),
            "[]".to_owned(),
            "\"0\"".to_owned(),
            "\"gg\"".to_owned(),
            "\"FF\"".to_owned(),
            format!("\"{}\"", "0".repeat(MAX_ENCODED_BYTES * 2 + 2)),
        ] {
            assert!(serde_json::from_str::<StreamState>(&json).is_err());
        }
    }

    #[test]
    fn maximum_byte_count_is_bounded_and_overflow_is_atomic() {
        // Structural fixtures need no giant input. CV values are opaque;
        // callers must bind decoded state to their trusted published digest.
        let total = u64::MAX - 1;
        let mut encoded = vec![VERSION];
        encoded.extend_from_slice(&total.to_le_bytes());
        encoded.resize(
            HEADER_BYTES + completed_chunks(total).count_ones() as usize * 32 + tail_len(total),
            0x5a,
        );
        let mut state = StreamState::from_bytes(&encoded).unwrap();
        assert_eq!(state.frontier.len(), MAX_FRONTIER);
        state.update(&[1]).unwrap();
        assert_eq!(state.byte_len(), u64::MAX);
        assert!(state.to_bytes().len() <= MAX_ENCODED_BYTES);
        let before = state.clone();
        assert_eq!(
            state.update(&[2]).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(state, before);
        state.update(&[]).unwrap();
        assert_eq!(state, before);
        assert_eq!(StreamState::from_bytes(&state.to_bytes()).unwrap(), state);
        assert_eq!(
            serde_json::from_str::<StreamState>(&serde_json::to_string(&state).unwrap()).unwrap(),
            state
        );
        // Finalization at the largest legal offset must stay inside the public
        // hazmat offset bounds, even for a structurally valid opaque frontier.
        let _ = state.digest();
    }
}
