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

use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    fmt,
};

use libp2p_identity::PeerId;

use crate::{
    topic::TopicHash,
    types::{MessageId, RawMessage},
};

/// A history record retains its budget until normal expiry, even after rejection.
#[derive(Debug, Clone)]
pub(crate) struct CacheEntry {
    mid: MessageId,
    topic: TopicHash,
    generation: u64,
    bytes: usize,
}

#[derive(Debug, Clone)]
struct MessageEntry {
    message: RawMessage,
    originating_peers: HashSet<PeerId>,
    iwant_counts: HashMap<PeerId, u32>,
    generation: u64,
    bytes: usize,
}

/// Retains admitted messages until normal expiry; excess admissions are dropped.
/// Dynamic buffers have a byte budget, while history and peer metadata have
/// separate count budgets. These are retained-data bounds, not an RSS estimate.
#[derive(Clone)]
pub(crate) struct MessageCache {
    msgs: HashMap<MessageId, MessageEntry>,
    history: Vec<Vec<CacheEntry>>,
    gossip: usize,
    max_entries: usize,
    max_bytes: usize,
    max_peer_associations: usize,
    history_entries: usize,
    retained_bytes: usize,
    peer_associations: usize,
    next_generation: Option<u64>,
}

impl fmt::Debug for MessageCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageCache")
            .field("msgs", &self.msgs)
            .field("history", &self.history)
            .field("gossip", &self.gossip)
            .finish()
    }
}

impl MessageCache {
    #[cfg(test)]
    pub(crate) fn new(gossip: usize, history_capacity: usize) -> Self {
        Self::with_limits(gossip, history_capacity, usize::MAX, usize::MAX, usize::MAX)
    }

    pub(crate) fn with_limits(
        gossip: usize,
        history_capacity: usize,
        max_entries: usize,
        max_bytes: usize,
        max_peer_associations: usize,
    ) -> Self {
        Self {
            gossip,
            msgs: HashMap::default(),
            history: vec![Vec::new(); history_capacity],
            max_entries,
            max_bytes,
            max_peer_associations,
            history_entries: 0,
            retained_bytes: 0,
            peer_associations: 0,
            next_generation: Some(0),
        }
    }

    // IDs are cloned and topics are normalized to exact-length allocations.
    // Message buffers are moved, so charge capacity rather than just length.
    fn admission_bytes(message_id: &MessageId, msg: &RawMessage) -> Option<(usize, usize)> {
        let history = message_id.0.len().checked_add(msg.topic.as_str().len())?;
        let message = history
            .checked_add(msg.data.capacity())?
            .checked_add(msg.signature.as_ref().map_or(0, Vec::capacity))?
            .checked_add(msg.key.as_ref().map_or(0, Vec::capacity))?;
        Some((history, message))
    }

    #[cfg(test)]
    pub(crate) fn usage(&self) -> (usize, usize, usize) {
        (
            self.history_entries,
            self.retained_bytes,
            self.peer_associations,
        )
    }

    /// Whether a payload still owns this identity in the history window.
    pub(crate) fn contains(&self, message_id: &MessageId) -> bool {
        self.msgs.contains_key(message_id)
    }

    /// Preflight for a synchronous insertion; does not reserve capacity.
    pub(crate) fn can_put(&self, message_id: &MessageId, msg: &RawMessage) -> bool {
        if self.history.is_empty() {
            return true;
        }
        if self.next_generation.is_none()
            || self.msgs.contains_key(message_id)
            || self.history_entries >= self.max_entries
        {
            return false;
        }
        Self::admission_bytes(message_id, msg)
            .and_then(|(history, message)| history.checked_add(message))
            .and_then(|bytes| self.retained_bytes.checked_add(bytes))
            .is_some_and(|bytes| bytes <= self.max_bytes)
    }

    /// Returns true for an admitted message, or for the empty-history no-op.
    pub(crate) fn put(&mut self, message_id: &MessageId, mut msg: RawMessage) -> bool {
        if !self.can_put(message_id, &msg) {
            return false;
        }
        if self.history.is_empty() {
            return true;
        }
        let (history_bytes, message_bytes) =
            Self::admission_bytes(message_id, &msg).expect("admission checked the byte budget");
        msg.topic = TopicHash::from_raw(msg.topic.into_string().into_boxed_str().into_string());
        let generation = self
            .next_generation
            .expect("admission checked generation availability");
        self.next_generation = generation.checked_add(1);
        self.history[0].push(CacheEntry {
            mid: message_id.clone(),
            topic: msg.topic.clone(),
            generation,
            bytes: history_bytes,
        });
        self.msgs.insert(
            message_id.clone(),
            MessageEntry {
                message: msg,
                originating_peers: HashSet::default(),
                iwant_counts: HashMap::default(),
                generation,
                bytes: message_bytes,
            },
        );
        self.history_entries += 1;
        self.retained_bytes += history_bytes + message_bytes;
        tracing::trace!(message=?message_id, "Put message in mcache");
        true
    }

    /// Track duplicate origins only while awaiting validation and budget permits.
    pub(crate) fn observe_duplicate(&mut self, message_id: &MessageId, source: &PeerId) {
        if let Some(entry) = self.msgs.get_mut(message_id) {
            if !entry.message.validated
                && self.peer_associations < self.max_peer_associations
                && entry.originating_peers.insert(*source)
            {
                self.peer_associations += 1;
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn get(&self, message_id: &MessageId) -> Option<&RawMessage> {
        self.msgs.get(message_id).map(|entry| &entry.message)
    }

    /// Existing IWANT counters remain authoritative under capacity pressure.
    /// An untracked peer is not served when its counter cannot be retained.
    pub(crate) fn get_with_iwant_counts(
        &mut self,
        message_id: &MessageId,
        peer: &PeerId,
    ) -> Option<(&RawMessage, u32)> {
        let entry = self.msgs.get_mut(message_id)?;
        if !entry.message.validated {
            return None;
        }
        let count = match entry.iwant_counts.entry(*peer) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                if self.peer_associations >= self.max_peer_associations {
                    return None;
                }
                self.peer_associations += 1;
                entry.insert(0)
            }
        };
        *count = count.saturating_add(1);
        Some((&entry.message, *count))
    }

    /// Validate and release duplicate-origin associations to the caller.
    pub(crate) fn validate(
        &mut self,
        message_id: &MessageId,
    ) -> Option<(&RawMessage, HashSet<PeerId>)> {
        let entry = self.msgs.get_mut(message_id)?;
        entry.message.validated = true;
        let peers = std::mem::take(&mut entry.originating_peers);
        self.peer_associations -= peers.len();
        Some((&entry.message, peers))
    }

    pub(crate) fn get_gossip_message_ids(&self, topic: &TopicHash) -> Vec<MessageId> {
        self.history
            .iter()
            .take(self.gossip)
            .flatten()
            .filter_map(|record| {
                let entry = self.msgs.get(&record.mid)?;
                (&record.topic == topic
                    && entry.message.validated
                    && record.generation == entry.generation)
                    .then(|| record.mid.clone())
            })
            .collect()
    }

    /// Expire one history window, without letting old records delete reinsertions.
    pub(crate) fn shift(&mut self) {
        let Some(expired) = self.history.pop() else {
            return;
        };
        for record in expired {
            self.history_entries -= 1;
            self.retained_bytes -= record.bytes;
            if self
                .msgs
                .get(&record.mid)
                .is_some_and(|entry| record.generation == entry.generation)
            {
                if let Some((msg, _)) = self.remove(&record.mid) {
                    if !msg.validated {
                        tracing::debug!(message=%record.mid,
                            "The message got removed from the cache without being validated.");
                    }
                }
            }
        }
        self.history.insert(0, Vec::new());
    }

    /// Remove payload and peer state. History retains its own bounded allocation
    /// until expiry; generation ownership makes its later removal harmless.
    pub(crate) fn remove(
        &mut self,
        message_id: &MessageId,
    ) -> Option<(RawMessage, HashSet<PeerId>)> {
        let entry = self.msgs.remove(message_id)?;
        self.retained_bytes -= entry.bytes;
        self.peer_associations -= entry.originating_peers.len() + entry.iwant_counts.len();
        Some((entry.message, entry.originating_peers))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IdentTopic as Topic;

    fn gen_testm(x: u64, topic: TopicHash) -> (MessageId, RawMessage) {
        let default_id = |message: &RawMessage| {
            // default message id is: source + sequence number
            let mut source_string = message.source.as_ref().unwrap().to_base58();
            source_string.push_str(&message.sequence_number.unwrap().to_string());
            MessageId::from(source_string)
        };
        let u8x: u8 = x as u8;
        let source = Some(PeerId::random());
        let data: Vec<u8> = vec![u8x];
        let sequence_number = Some(x);

        let m = RawMessage {
            source,
            data,
            sequence_number,
            topic,
            signature: None,
            key: None,
            validated: false,
        };

        let id = default_id(&m);
        (id, m)
    }

    fn new_cache(gossip_size: usize, history: usize) -> MessageCache {
        MessageCache::new(gossip_size, history)
    }

    #[test]
    /// Test that the message cache can be created.
    fn test_new_cache() {
        let x: usize = 3;
        let mc = new_cache(x, 5);

        assert_eq!(mc.gossip, x);
    }

    #[test]
    /// Test you can put one message and get one.
    fn test_put_get_one() {
        let mut mc = new_cache(10, 15);

        let topic1_hash = Topic::new("topic1").hash();
        let (id, m) = gen_testm(10, topic1_hash);

        mc.put(&id, m.clone());

        assert_eq!(mc.history[0].len(), 1);

        let fetched = mc.get(&id);

        assert_eq!(fetched.unwrap(), &m);
    }

    #[test]
    /// Test attempting to 'get' with a wrong id.
    fn test_get_wrong() {
        let mut mc = new_cache(10, 15);

        let topic1_hash = Topic::new("topic1").hash();
        let (id, m) = gen_testm(10, topic1_hash);

        mc.put(&id, m);

        // Try to get an incorrect ID
        let wrong_id = MessageId::new(b"wrongid");
        let fetched = mc.get(&wrong_id);
        assert!(fetched.is_none());
    }

    #[test]
    /// Test attempting to 'get' empty message cache.
    fn test_get_empty() {
        let mc = new_cache(10, 15);

        // Try to get an incorrect ID
        let wrong_string = MessageId::new(b"imempty");
        let fetched = mc.get(&wrong_string);
        assert!(fetched.is_none());
    }

    #[test]
    /// Test shift mechanism.
    fn test_shift() {
        let mut mc = new_cache(1, 5);

        let topic1_hash = Topic::new("topic1").hash();

        // Build the message
        for i in 0..10 {
            let (id, m) = gen_testm(i, topic1_hash.clone());
            mc.put(&id, m.clone());
        }

        mc.shift();

        // Ensure the shift occurred
        assert!(mc.history[0].is_empty());
        assert!(mc.history[1].len() == 10);

        // Make sure no messages deleted
        assert!(mc.msgs.len() == 10);
    }

    #[test]
    /// Test Shift with no additions.
    fn test_empty_shift() {
        let mut mc = new_cache(1, 5);

        let topic1_hash = Topic::new("topic1").hash();

        // Build the message
        for i in 0..10 {
            let (id, m) = gen_testm(i, topic1_hash.clone());
            mc.put(&id, m.clone());
        }

        mc.shift();

        // Ensure the shift occurred
        assert!(mc.history[0].is_empty());
        assert!(mc.history[1].len() == 10);

        mc.shift();

        assert!(mc.history[2].len() == 10);
        assert!(mc.history[1].is_empty());
        assert!(mc.history[0].is_empty());
    }

    #[test]
    /// Test shift to see if the last history messages are removed.
    fn test_remove_last_from_shift() {
        let mut mc = new_cache(4, 5);

        let topic1_hash = Topic::new("topic1").hash();

        // Build the message
        for i in 0..10 {
            let (id, m) = gen_testm(i, topic1_hash.clone());
            mc.put(&id, m.clone());
        }

        // Shift right until deleting messages
        mc.shift();
        mc.shift();
        mc.shift();
        mc.shift();

        assert_eq!(mc.history[mc.history.len() - 1].len(), 10);

        // Shift and delete the messages
        mc.shift();
        assert_eq!(mc.history[mc.history.len() - 1].len(), 0);
        assert_eq!(mc.history[0].len(), 0);
        assert_eq!(mc.msgs.len(), 0);
    }

    fn logex_message(id: u8) -> (MessageId, RawMessage) {
        (
            MessageId::new(&[id]),
            RawMessage {
                source: None,
                data: vec![1],
                sequence_number: None,
                topic: TopicHash::from_raw("t"),
                signature: Some(vec![2]),
                key: Some(vec![3]),
                validated: false,
            },
        )
    }

    #[test]
    fn logex_exact_byte_and_entry_limits_preserve_admitted_messages() {
        let (id, msg) = logex_message(1);
        // Two ID/topic copies and three one-byte message buffers.
        let mut too_small = MessageCache::with_limits(1, 1, 1, 6, 1);
        assert!(!too_small.can_put(&id, &msg));
        assert!(!too_small.put(&id, msg.clone()));
        assert_eq!(too_small.usage(), (0, 0, 0));
        let mut cache = MessageCache::with_limits(1, 1, 1, 7, 1);
        assert!(cache.can_put(&id, &msg));
        assert!(cache.put(&id, msg.clone()));
        assert_eq!(cache.usage(), (1, 7, 0));
        assert!(!cache.put(&id, msg));
        let (other, message) = logex_message(2);
        assert!(!cache.put(&other, message));
        assert!(cache.get(&id).is_some());
        cache.shift();
        assert_eq!(cache.usage(), (0, 0, 0));

        let mut count_limited = MessageCache::with_limits(1, 1, 1, 100, 1);
        let (id, msg) = logex_message(1);
        assert!(count_limited.put(&id, msg));
        let (other, msg) = logex_message(2);
        assert!(!count_limited.put(&other, msg));
    }

    #[test]
    fn logex_buffer_capacity_and_topic_normalization_are_accounted() {
        let (id, mut msg) = logex_message(1);
        msg.data = Vec::with_capacity(8);
        msg.data.push(1);
        let mut topic = String::with_capacity(32);
        topic.push('t');
        msg.topic = TopicHash::from_raw(topic);
        let expected = 6 + msg.data.capacity();
        let mut cache = MessageCache::with_limits(1, 1, 1, expected - 1, 1);
        assert!(!cache.can_put(&id, &msg));
        cache.max_bytes = expected;
        assert!(cache.put(&id, msg));
        assert_eq!(cache.retained_bytes, expected);
        assert_eq!(
            cache
                .get(&id)
                .unwrap()
                .topic
                .clone()
                .into_string()
                .capacity(),
            1
        );
        cache.remove(&id);
        assert_eq!(cache.usage(), (1, 2, 0));
        cache.shift();
        assert_eq!(cache.usage(), (0, 0, 0));
    }

    #[test]
    fn logex_rejection_churn_retains_bounded_history_until_expiry() {
        let mut cache = MessageCache::with_limits(1, 2, 2, 100, 1);
        for n in 1..=2 {
            let (id, msg) = logex_message(n);
            assert!(cache.put(&id, msg));
            assert!(cache.remove(&id).is_some());
        }
        assert_eq!(cache.usage(), (2, 4, 0));
        let (id, msg) = logex_message(3);
        assert!(!cache.put(&id, msg.clone()));
        cache.shift();
        assert!(!cache.put(&id, msg.clone()));
        cache.shift();
        assert_eq!(cache.usage(), (0, 0, 0));
        assert!(cache.put(&id, msg));
    }

    #[test]
    fn logex_old_history_cannot_gossip_or_expire_reinserted_id() {
        let mut cache = MessageCache::with_limits(2, 2, 2, 100, 1);
        let (id, msg) = logex_message(1);
        assert!(cache.put(&id, msg.clone()));
        cache.remove(&id);
        cache.shift();
        assert!(cache.put(&id, msg));
        cache.validate(&id);
        assert_eq!(
            cache.get_gossip_message_ids(&TopicHash::from_raw("t")),
            vec![id.clone()]
        );
        assert_eq!(cache.usage(), (2, 9, 0));
        cache.shift();
        assert!(cache.get(&id).is_some());
        assert_eq!(cache.usage(), (1, 7, 0));
        cache.shift();
        assert!(cache.get(&id).is_none());
        assert_eq!(cache.usage(), (0, 0, 0));
    }

    #[test]
    fn logex_peer_budget_preserves_iwant_counter_continuity() {
        let mut cache = MessageCache::with_limits(1, 1, 2, 100, 1);
        let (id, msg) = logex_message(1);
        cache.put(&id, msg);
        let first = PeerId::random();
        let second = PeerId::random();
        cache.observe_duplicate(&id, &first);
        cache.observe_duplicate(&id, &first);
        cache.observe_duplicate(&id, &second);
        assert_eq!(cache.peer_associations, 1);
        let (_, origins) = cache.validate(&id).unwrap();
        assert_eq!(origins, HashSet::from([first]));
        assert_eq!(cache.peer_associations, 0);
        assert_eq!(cache.get_with_iwant_counts(&id, &first).unwrap().1, 1);
        assert!(cache.get_with_iwant_counts(&id, &second).is_none());
        assert_eq!(cache.get_with_iwant_counts(&id, &first).unwrap().1, 2);
        cache
            .msgs
            .get_mut(&id)
            .unwrap()
            .iwant_counts
            .insert(first, u32::MAX);
        assert_eq!(
            cache.get_with_iwant_counts(&id, &first).unwrap().1,
            u32::MAX
        );
        // A repeated validation must not clear authoritative IWANT counters.
        assert!(cache.validate(&id).unwrap().1.is_empty());
        assert_eq!(cache.peer_associations, 1);
        let mut cloned = cache.clone();
        cloned.shift();
        assert_eq!(cloned.usage(), (0, 0, 0));
        assert_eq!(cache.usage(), (1, 7, 1));
        cache.remove(&id);
        assert_eq!(cache.usage(), (1, 2, 0));
        cache.shift();
        assert_eq!(cache.usage(), (0, 0, 0));
    }

    #[test]
    fn logex_peer_budget_is_shared_between_messages_and_tracking_kinds() {
        let mut cache = MessageCache::with_limits(1, 1, 2, 100, 1);
        let (id, msg) = logex_message(1);
        let (other, message) = logex_message(2);
        cache.put(&id, msg);
        cache.put(&other, message);
        let peer = PeerId::random();
        cache.observe_duplicate(&id, &peer);
        cache.validate(&other);
        assert!(cache.get_with_iwant_counts(&other, &peer).is_none());
        cache.validate(&id);
        assert_eq!(cache.get_with_iwant_counts(&other, &peer).unwrap().1, 1);
        cache.shift();
        assert_eq!(cache.usage(), (0, 0, 0));
    }

    #[test]
    fn logex_empty_history_and_generation_exhaustion() {
        let (id, msg) = logex_message(1);
        let mut empty = MessageCache::with_limits(1, 0, 0, 0, 0);
        assert!(empty.can_put(&id, &msg));
        assert!(empty.put(&id, msg.clone()));
        empty.shift();
        assert!(empty.get_gossip_message_ids(&msg.topic).is_empty());
        assert_eq!(empty.usage(), (0, 0, 0));
        let mut cache = MessageCache::with_limits(1, 1, 1, 100, 1);
        cache.next_generation = Some(u64::MAX);
        assert!(cache.put(&id, msg.clone()));
        cache.shift();
        assert!(!cache.put(&id, msg));
        assert_eq!(cache.usage(), (0, 0, 0));
    }
}
