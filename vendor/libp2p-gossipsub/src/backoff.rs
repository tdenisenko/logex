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

//! Data structure for efficiently storing known back-off's when pruning peers.
use std::{
    collections::{
        hash_map::{Entry, HashMap},
        HashSet,
    },
    time::Duration,
};

use libp2p_identity::PeerId;
use web_time::Instant;

use crate::topic::TopicHash;

#[derive(Copy, Clone)]
struct HeartbeatIndex(usize);

#[derive(Default)]
struct PeerRetention {
    // Includes a provisional handler until Swarm reports admission or failure.
    connected: bool,
    topics: usize,
}

/// Stores backoffs in an efficient manner.
pub(crate) struct BackoffStorage {
    /// One identity reservation for connected peers or retained topic backoffs.
    peers: HashMap<PeerId, PeerRetention>,
    max_peers: usize,
    /// Stores backoffs and the index in backoffs_by_heartbeat per peer per topic.
    backoffs: HashMap<TopicHash, HashMap<PeerId, (Instant, HeartbeatIndex)>>,
    /// Stores peer topic pairs per heartbeat (this is cyclic the current index is
    /// heartbeat_index).
    backoffs_by_heartbeat: Vec<HashSet<(TopicHash, PeerId)>>,
    /// The index in the backoffs_by_heartbeat vector corresponding to the current heartbeat.
    heartbeat_index: HeartbeatIndex,
    /// The heartbeat interval duration from the config.
    heartbeat_interval: Duration,
    /// Backoff slack from the config.
    backoff_slack: u32,
}

impl BackoffStorage {
    fn heartbeats(d: &Duration, heartbeat_interval: &Duration) -> usize {
        d.as_nanos().div_ceil(heartbeat_interval.as_nanos()) as usize
    }

    pub(crate) fn new(
        prune_backoff: &Duration,
        heartbeat_interval: Duration,
        backoff_slack: u32,
        max_peers: usize,
    ) -> BackoffStorage {
        // We add one additional slot for partial heartbeat
        let max_heartbeats =
            Self::heartbeats(prune_backoff, &heartbeat_interval) + backoff_slack as usize + 1;
        BackoffStorage {
            peers: HashMap::new(),
            max_peers,
            backoffs: HashMap::new(),
            backoffs_by_heartbeat: vec![HashSet::new(); max_heartbeats],
            heartbeat_index: HeartbeatIndex(0),
            heartbeat_interval,
            backoff_slack,
        }
    }

    /// Reserve before constructing a handler. Retained identities can reconnect
    /// even at capacity; new identities must wait for ordinary ownership expiry.
    pub(crate) fn reserve_peer(&mut self, peer: &PeerId) -> bool {
        if let Some(retained) = self.peers.get_mut(peer) {
            retained.connected = true;
            return true;
        }
        if self.peers.len() >= self.max_peers {
            return false;
        }
        self.peers.insert(
            *peer,
            PeerRetention {
                connected: true,
                topics: 0,
            },
        );
        true
    }

    pub(crate) fn has_capacity(&self, peer: &PeerId) -> bool {
        self.peers.len() < self.max_peers || self.peers.contains_key(peer)
    }

    /// Release only after the behaviour has removed the final connection ID,
    /// including a provisional connection refused by a sibling behaviour.
    pub(crate) fn release_peer(&mut self, peer: &PeerId) {
        if let Entry::Occupied(mut entry) = self.peers.entry(*peer) {
            entry.get_mut().connected = false;
            if entry.get().topics == 0 {
                entry.remove();
            }
        }
    }

    /// Updates the backoff for a peer (if there is already a more restrictive backoff then this
    /// call doesn't change anything). Only admitted identities can retain state.
    pub(crate) fn update_backoff(&mut self, topic: &TopicHash, peer: &PeerId, time: Duration) {
        self.update_backoff_at(topic, peer, time, Instant::now());
    }

    fn update_backoff_at(
        &mut self,
        topic: &TopicHash,
        peer: &PeerId,
        time: Duration,
        now: Instant,
    ) {
        let Some(retained) = self.peers.get_mut(peer) else {
            tracing::debug!(%peer, "ignoring backoff without connection admission");
            return;
        };
        let Some(instant) = now.checked_add(time) else {
            tracing::warn!("ignoring oversized prune backoff");
            return;
        };

        let insert_into_backoffs_by_heartbeat =
            |heartbeat_index: HeartbeatIndex,
             backoffs_by_heartbeat: &mut Vec<HashSet<_>>,
             heartbeat_interval,
             backoff_slack| {
                let pair = (topic.clone(), *peer);
                let index = (heartbeat_index.0
                    + Self::heartbeats(&time, heartbeat_interval)
                    + backoff_slack as usize)
                    % backoffs_by_heartbeat.len();
                backoffs_by_heartbeat[index].insert(pair);
                HeartbeatIndex(index)
            };
        match self.backoffs.entry(topic.clone()).or_default().entry(*peer) {
            Entry::Occupied(mut o) => {
                let (backoff, index) = o.get();
                if backoff < &instant {
                    let pair = (topic.clone(), *peer);
                    if let Some(s) = self.backoffs_by_heartbeat.get_mut(index.0) {
                        s.remove(&pair);
                    }
                    let index = insert_into_backoffs_by_heartbeat(
                        self.heartbeat_index,
                        &mut self.backoffs_by_heartbeat,
                        &self.heartbeat_interval,
                        self.backoff_slack,
                    );
                    o.insert((instant, index));
                }
            }
            Entry::Vacant(v) => {
                let index = insert_into_backoffs_by_heartbeat(
                    self.heartbeat_index,
                    &mut self.backoffs_by_heartbeat,
                    &self.heartbeat_interval,
                    self.backoff_slack,
                );
                v.insert((instant, index));
                retained.topics += 1;
            }
        };
    }

    /// Checks if a given peer is backoffed for the given topic. This method respects the
    /// configured BACKOFF_SLACK and may return true even if the backoff is already over.
    /// It stays true until heartbeat cleanup observes expiry of the delay and slack.
    ///
    /// This method should be used for deciding if we can already send a GRAFT to a previously
    /// backoffed peer.
    pub(crate) fn is_backoff_with_slack(&self, topic: &TopicHash, peer: &PeerId) -> bool {
        self.backoffs
            .get(topic)
            .is_some_and(|m| m.contains_key(peer))
    }

    pub(crate) fn get_backoff_time(&self, topic: &TopicHash, peer: &PeerId) -> Option<Instant> {
        Self::get_backoff_time_from_backoffs(&self.backoffs, topic, peer)
    }

    fn get_backoff_time_from_backoffs(
        backoffs: &HashMap<TopicHash, HashMap<PeerId, (Instant, HeartbeatIndex)>>,
        topic: &TopicHash,
        peer: &PeerId,
    ) -> Option<Instant> {
        backoffs
            .get(topic)
            .and_then(|m| m.get(peer).map(|(i, _)| *i))
    }

    /// Applies a heartbeat. That should be called regularly in intervals of length
    /// `heartbeat_interval`.
    pub(crate) fn heartbeat(&mut self) {
        self.heartbeat_at(Instant::now());
    }

    fn heartbeat_at(&mut self, now: Instant) {
        // Clean up backoffs_by_heartbeat
        if let Some(s) = self.backoffs_by_heartbeat.get_mut(self.heartbeat_index.0) {
            let backoffs = &mut self.backoffs;
            let peers = &mut self.peers;
            let slack = self.heartbeat_interval * self.backoff_slack;
            s.retain(|(topic, peer)| {
                let keep = match Self::get_backoff_time_from_backoffs(backoffs, topic, peer) {
                    Some(backoff_time) => backoff_time
                        .checked_add(slack)
                        .map(|backoff| backoff > now)
                        .unwrap_or(false),
                    None => false,
                };
                if !keep {
                    // remove from backoffs
                    if let Entry::Occupied(mut m) = backoffs.entry(topic.clone()) {
                        if m.get_mut().remove(peer).is_some() {
                            if let Entry::Occupied(mut retained) = peers.entry(*peer) {
                                retained.get_mut().topics -= 1;
                                if retained.get().topics == 0 && !retained.get().connected {
                                    retained.remove();
                                }
                            }
                            if m.get().is_empty() {
                                m.remove();
                            }
                        }
                    }
                }

                keep
            });
        }

        // Increase heartbeat index
        self.heartbeat_index =
            HeartbeatIndex((self.heartbeat_index.0 + 1) % self.backoffs_by_heartbeat.len());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn storage(capacity: usize) -> BackoffStorage {
        BackoffStorage::new(&Duration::from_secs(4), Duration::from_secs(1), 2, capacity)
    }

    // Drive ordinary one-second heartbeats with an explicit clock. Long delays
    // cross the same wheel buckets repeatedly; no sleeping or early expiry.
    fn advance_to(storage: &mut BackoffStorage, next_tick: &mut Instant, end: Instant) {
        while *next_tick <= end {
            storage.heartbeat_at(*next_tick);
            *next_tick += Duration::from_secs(1);
        }
    }

    #[test]
    fn logex_backoff_identity_admission_and_connection_ownership() {
        let now = Instant::now();
        let peer = PeerId::random();
        let newcomer = PeerId::random();
        let topic = TopicHash::from_raw("allowed");
        let mut zero = storage(0);
        assert!(!zero.reserve_peer(&peer));
        zero.update_backoff_at(&topic, &peer, Duration::from_secs(4), now);
        assert!(zero.peers.is_empty());
        assert!(zero.backoffs.is_empty());

        let mut one = storage(1);
        assert!(one.reserve_peer(&peer));
        assert!(one.reserve_peer(&peer));
        assert!(!one.reserve_peer(&newcomer));
        one.update_backoff_at(&topic, &newcomer, Duration::from_secs(4), now);
        assert!(one.backoffs.is_empty());
        let mut next_tick = now + Duration::from_secs(1);
        advance_to(&mut one, &mut next_tick, now + Duration::from_secs(20));
        assert!(
            !one.reserve_peer(&newcomer),
            "a connected owner does not expire"
        );
        one.release_peer(&peer);
        one.release_peer(&peer);
        assert!(one.reserve_peer(&newcomer));
        assert_eq!(one.peers.len(), 1);
    }

    #[test]
    fn logex_backoff_identity_waits_for_every_topic_and_slack() {
        let now = Instant::now();
        let mut next_tick = now + Duration::from_secs(1);
        let peer = PeerId::random();
        let newcomer = PeerId::random();
        let a = TopicHash::from_raw("a");
        let b = TopicHash::from_raw("b");
        let mut store = storage(1);
        assert!(store.reserve_peer(&peer));
        store.update_backoff_at(&a, &peer, Duration::from_secs(5), now);
        store.update_backoff_at(&b, &peer, Duration::from_secs(12), now);
        store.release_peer(&peer);
        assert_eq!(store.peers[&peer].topics, 2);

        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(6));
        assert!(store.is_backoff_with_slack(&a, &peer));
        assert!(!store.reserve_peer(&newcomer));
        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(10));
        assert!(!store.is_backoff_with_slack(&a, &peer));
        assert!(store.is_backoff_with_slack(&b, &peer));
        assert_eq!(store.peers[&peer].topics, 1);
        assert!(!store.reserve_peer(&newcomer));
        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(13));
        assert!(
            store.is_backoff_with_slack(&b, &peer),
            "retain required slack"
        );
        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(21));
        assert!(store.backoffs.is_empty());
        assert!(store.peers.is_empty());
        assert!(store.backoffs_by_heartbeat.iter().all(HashSet::is_empty));
        assert!(store.reserve_peer(&newcomer));
    }

    #[test]
    fn logex_backoff_extension_and_reconnect_keep_one_reservation() {
        let now = Instant::now();
        let mut next_tick = now + Duration::from_secs(1);
        let peer = PeerId::random();
        let newcomer = PeerId::random();
        let topic = TopicHash::from_raw("allowed");
        let mut store = storage(1);
        assert!(store.reserve_peer(&peer));
        store.update_backoff_at(&topic, &peer, Duration::from_secs(4), now);
        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(2));
        let updated = now + Duration::from_secs(2);
        store.update_backoff_at(&topic, &peer, Duration::from_secs(15), updated);
        store.update_backoff_at(&topic, &peer, Duration::from_secs(1), updated);
        let deadline = now + Duration::from_secs(17);
        assert_eq!(store.get_backoff_time(&topic, &peer), Some(deadline));
        assert_eq!(store.peers[&peer].topics, 1);
        store.release_peer(&peer);
        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(10));
        assert!(!store.reserve_peer(&newcomer));
        assert!(
            store.reserve_peer(&peer),
            "retained identity can reconnect at capacity"
        );
        assert_eq!(store.get_backoff_time(&topic, &peer), Some(deadline));
        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(18));
        assert!(store.is_backoff_with_slack(&topic, &peer));
        advance_to(&mut store, &mut next_tick, now + Duration::from_secs(25));
        assert!(store.backoffs.is_empty());
        assert_eq!(store.peers[&peer].topics, 0);
        assert!(
            !store.reserve_peer(&newcomer),
            "connected identity remains reserved"
        );
        store.release_peer(&peer);
        assert!(store.reserve_peer(&newcomer));
    }
}
