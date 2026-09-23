// Copyright 2020 Sigma Prime Pty Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use std::collections::{hash_map::Entry, HashMap};

use libp2p_identity::PeerId;
use web_time::Instant;

use crate::{peer_score::RejectReason, MessageId, ValidationError};

/// Tracks recently sent `IWANT` messages and checks if peers respond to them.
pub(crate) struct GossipPromises {
    /// Stores for each tracked message id and peer the instant when this promise expires.
    ///
    /// If the peer didn't respond until then we consider the promise as broken and penalize the
    /// peer.
    promises: HashMap<MessageId, HashMap<PeerId, Instant>>,
    max_entries: usize,
    max_bytes: usize,
    max_peer_associations: usize,
    retained_bytes: usize,
    peer_associations: usize,
}

impl GossipPromises {
    /// Bounds message IDs, their owned bytes, and fixed-size peer/deadline associations.
    pub(crate) fn with_limits(
        max_entries: usize,
        max_bytes: usize,
        max_peer_associations: usize,
    ) -> Self {
        Self {
            promises: HashMap::new(),
            max_entries,
            max_bytes,
            max_peer_associations,
            retained_bytes: 0,
            peer_associations: 0,
        }
    }

    /// Returns true if the message id exists in the promises.
    pub(crate) fn contains(&self, message: &MessageId) -> bool {
        self.promises.contains_key(message)
    }

    /// Get the peers we sent IWANT the input message id.
    pub(crate) fn peers_for_message(&self, message_id: &MessageId) -> Vec<PeerId> {
        self.promises
            .get(message_id)
            .map(|peers| peers.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Track a promise to deliver a message from a list of [`MessageId`]s we are requesting.
    /// Returns only IDs whose peer promise is tracked. Expiry is left to the heartbeat,
    /// so admission pressure never silently discards a pending scoring penalty.
    pub(crate) fn add_promise(
        &mut self,
        peer: PeerId,
        messages: &[MessageId],
        expires: Instant,
    ) -> Vec<MessageId> {
        let mut admitted = Vec::new();
        for message_id in messages {
            if let Some(peers) = self.promises.get_mut(message_id) {
                match peers.entry(peer) {
                    // Keep the first promise deadline, even at capacity.
                    Entry::Occupied(_) => {}
                    Entry::Vacant(entry) if self.peer_associations < self.max_peer_associations => {
                        entry.insert(expires);
                        self.peer_associations += 1;
                    }
                    Entry::Vacant(_) => continue,
                }
                admitted.push(MessageId::new(&message_id.0));
                continue;
            }
            let bytes = message_id.0.len();
            if self.promises.len() >= self.max_entries
                || self.peer_associations >= self.max_peer_associations
                || bytes > self.max_bytes - self.retained_bytes
            {
                continue;
            }
            let mut peers = HashMap::new();
            peers.insert(peer, expires);
            // Copy only the ID's contents, never its caller-provided spare capacity.
            self.promises.insert(MessageId::new(&message_id.0), peers);
            self.retained_bytes += bytes;
            self.peer_associations += 1;
            admitted.push(MessageId::new(&message_id.0));
        }
        admitted
    }

    fn remove_message(&mut self, message_id: &MessageId) {
        if let Some((id, peers)) = self.promises.remove_entry(message_id) {
            self.retained_bytes -= id.0.len();
            self.peer_associations -= peers.len();
        }
    }

    pub(crate) fn message_delivered(&mut self, message_id: &MessageId) {
        // Someone delivered a message, we can stop tracking all promises for it.
        self.remove_message(message_id);
    }

    pub(crate) fn reject_message(&mut self, message_id: &MessageId, reason: &RejectReason) {
        // A message got rejected, so we can stop tracking promises and let the score penalty apply
        // from invalid message delivery.
        // We do take exception and apply promise penalty regardless in the following cases, where
        // the peer delivered an obviously invalid message.
        match reason {
            RejectReason::ValidationError(ValidationError::InvalidSignature) => (),
            RejectReason::SelfOrigin => (),
            _ => {
                self.remove_message(message_id);
            }
        };
    }

    /// Returns the number of broken promises for each peer who didn't follow up on an IWANT
    /// request.
    /// This should be called not too often relative to the expire times, since it iterates over
    /// the whole stored data.
    pub(crate) fn get_broken_promises(&mut self) -> HashMap<PeerId, usize> {
        self.get_broken_promises_at(Instant::now())
    }

    fn get_broken_promises_at(&mut self, now: Instant) -> HashMap<PeerId, usize> {
        let mut result = HashMap::new();
        self.promises.retain(|msg, peers| {
            let previous_len = peers.len();
            peers.retain(|peer_id, expires| {
                if *expires < now {
                    let count = result.entry(*peer_id).or_insert(0);
                    *count += 1;
                    self.peer_associations -= 1;
                    tracing::debug!(
                        peer=%peer_id,
                        message=%msg,
                        "[Penalty] The peer broke the promise to deliver message in time!"
                    );
                    false
                } else {
                    true
                }
            });
            if peers.is_empty() {
                self.retained_bytes -= msg.0.len();
                false
            } else {
                // Return spare peer buckets after partial expiry. Otherwise a
                // surviving promise could retain each map's former high water
                // allocation while the freed association budget is reused.
                if peers.len() < previous_len {
                    peers.shrink_to_fit();
                }
                true
            }
        });
        result
    }
}

#[cfg(test)]
mod logex_capacity_tests {
    use std::time::Duration;

    use super::*;

    fn peer(value: u8) -> PeerId {
        let mut bytes = [value; 34];
        bytes[0] = 0x12;
        bytes[1] = 32;
        PeerId::from_bytes(&bytes).unwrap()
    }

    fn id(value: &[u8]) -> MessageId {
        MessageId::new(value)
    }

    fn assert_counts(promises: &GossipPromises, entries: usize, bytes: usize, pairs: usize) {
        assert_eq!(promises.promises.len(), entries);
        assert_eq!(promises.retained_bytes, bytes);
        assert_eq!(promises.peer_associations, pairs);
        assert_eq!(
            promises.promises.keys().map(|id| id.0.len()).sum::<usize>(),
            bytes
        );
        assert_eq!(
            promises.promises.values().map(HashMap::len).sum::<usize>(),
            pairs
        );
    }

    #[test]
    fn logex_entry_byte_and_pair_bounds_preserve_duplicates_and_deadlines() {
        let now = Instant::now();
        let later = now + Duration::from_secs(10);
        let mut promises = GossipPromises::with_limits(1, 2, 1);
        let first = id(b"ab");
        assert_eq!(
            promises.add_promise(peer(1), std::slice::from_ref(&first), now),
            vec![first.clone()]
        );
        assert_eq!(
            promises.add_promise(peer(1), std::slice::from_ref(&first), later),
            vec![first.clone()]
        );
        assert!(promises
            .add_promise(peer(2), std::slice::from_ref(&first), later)
            .is_empty());
        assert!(promises.add_promise(peer(1), &[id(b"c")], later).is_empty());
        assert_eq!(promises.promises[&first][&peer(1)], now);
        assert_counts(&promises, 1, 2, 1);
        // Preserve the existing strict expiry boundary.
        assert!(promises.get_broken_promises_at(now).is_empty());
        assert_eq!(
            promises.get_broken_promises_at(now + Duration::from_nanos(1)),
            HashMap::from([(peer(1), 1)])
        );
        assert_counts(&promises, 0, 0, 0);
        assert_eq!(
            promises.add_promise(peer(2), std::slice::from_ref(&first), later),
            vec![first]
        );
    }

    #[test]
    fn logex_byte_budget_and_peer_budget_are_independent() {
        let now = Instant::now();
        let mut promises = GossipPromises::with_limits(4, 3, 3);
        let first = id(b"ab");
        assert!(promises
            .add_promise(peer(1), &[id(b"long")], now)
            .is_empty());
        assert_eq!(
            promises
                .add_promise(peer(1), &[first.clone(), id(b"c")], now)
                .len(),
            2
        );
        assert!(promises.add_promise(peer(1), &[id(b"d")], now).is_empty());
        assert_eq!(
            promises.add_promise(peer(2), std::slice::from_ref(&first), now),
            vec![first.clone()]
        );
        assert!(promises.add_promise(peer(3), &[first], now).is_empty());
        assert_counts(&promises, 2, 3, 3);
        let mut pair_limited = GossipPromises::with_limits(4, usize::MAX, 1);
        assert_eq!(
            pair_limited.add_promise(peer(1), &[id(b"a"), id(b"b")], now),
            vec![id(b"a")]
        );
        let mut entry_limited = GossipPromises::with_limits(1, usize::MAX, 4);
        assert_eq!(
            entry_limited.add_promise(peer(1), &[id(b"a"), id(b"b")], now),
            vec![id(b"a")]
        );
    }

    #[test]
    fn logex_partial_expiry_delivery_and_rejection_release_exact_counts() {
        let now = Instant::now();
        let later = now + Duration::from_secs(10);
        let mut promises = GossipPromises::with_limits(1, 2, 2);
        let first = id(b"ab");
        promises.add_promise(peer(1), std::slice::from_ref(&first), now);
        promises.add_promise(peer(2), std::slice::from_ref(&first), later);
        assert_eq!(
            promises.get_broken_promises_at(now + Duration::from_nanos(1)),
            HashMap::from([(peer(1), 1)])
        );
        assert_counts(&promises, 1, 2, 1);
        promises.message_delivered(&first);
        promises.message_delivered(&first);
        assert_counts(&promises, 0, 0, 0);
        assert_eq!(
            promises
                .add_promise(peer(1), std::slice::from_ref(&first), later)
                .len(),
            1
        );
        promises.reject_message(&first, &RejectReason::SelfOrigin);
        promises.reject_message(
            &first,
            &RejectReason::ValidationError(ValidationError::InvalidSignature),
        );
        assert_counts(&promises, 1, 2, 1);
        promises.reject_message(&first, &RejectReason::ValidationFailed);
        assert_counts(&promises, 0, 0, 0);
        assert_eq!(promises.add_promise(peer(2), &[first], later).len(), 1);
    }

    #[test]
    fn logex_full_admission_keeps_expired_penalties_until_heartbeat() {
        let now = Instant::now();
        let mut promises = GossipPromises::with_limits(1, 1, 1);
        promises.add_promise(peer(1), &[id(b"a")], now - Duration::from_secs(1));
        assert!(promises.add_promise(peer(2), &[id(b"b")], now).is_empty());
        assert_eq!(
            promises.get_broken_promises_at(now),
            HashMap::from([(peer(1), 1)])
        );
        assert_eq!(
            promises.add_promise(peer(2), &[id(b"b")], now),
            vec![id(b"b")]
        );
        assert_counts(&promises, 1, 1, 1);
    }
}
