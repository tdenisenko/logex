//! Bounded advisory retention without charging cache-owned message delivery again.
//!
//! Handler keepalive notifications and critical close notifications have separate
//! owners. Message events retain their cache owner until delivery in LogEx's
//! polling/validation flow; optional PX/explicit-peer dial actions are not used
//! by LogEx. This queue's budget covers only best-effort application advisories.

use std::collections::VecDeque;

use libp2p_swarm::ToSwarm;

use crate::{handler::HandlerIn, Event, QueueLimits};

pub(crate) struct EventQueue {
    events: VecDeque<ToSwarm<Event, HandlerIn>>,
    max_advisory_events: usize,
    max_advisory_bytes: usize,
    advisory_events: usize,
    advisory_bytes: usize,
}

impl EventQueue {
    pub(crate) fn new(limits: QueueLimits) -> Self {
        Self {
            events: VecDeque::new(),
            max_advisory_events: limits.max_advisory_events,
            max_advisory_bytes: limits.max_advisory_bytes,
            advisory_events: 0,
            advisory_bytes: 0,
        }
    }

    /// Preserve admitted events and decline excess new advisories. Other event
    /// classes keep their existing ownership and admission paths.
    pub(crate) fn push_back(&mut self, event: ToSwarm<Event, HandlerIn>) -> bool {
        if let Some(bytes) = advisory_bytes(&event) {
            if self.advisory_events >= self.max_advisory_events {
                return false;
            }
            let Some(next_bytes) = self
                .advisory_bytes
                .checked_add(bytes)
                .filter(|next| *next <= self.max_advisory_bytes)
            else {
                return false;
            };
            self.advisory_events += 1;
            self.advisory_bytes = next_bytes;
        }
        self.events.push_back(event);
        true
    }

    pub(crate) fn pop_front(&mut self) -> Option<ToSwarm<Event, HandlerIn>> {
        let event = self.events.pop_front()?;
        if let Some(bytes) = advisory_bytes(&event) {
            self.advisory_events -= 1;
            self.advisory_bytes -= bytes;
        }
        Some(event)
    }

    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn clear(&mut self) {
        self.events.clear();
        self.advisory_events = 0;
        self.advisory_bytes = 0;
    }

    #[cfg(test)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &ToSwarm<Event, HandlerIn>> {
        self.events.iter()
    }
}

#[cfg(test)]
impl std::ops::Index<usize> for EventQueue {
    type Output = ToSwarm<Event, HandlerIn>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.events[index]
    }
}

fn advisory_bytes(event: &ToSwarm<Event, HandlerIn>) -> Option<usize> {
    match event {
        ToSwarm::GenerateEvent(
            Event::Subscribed { topic, .. } | Event::Unsubscribed { topic, .. },
        ) => Some(topic.retained_bytes()),
        ToSwarm::GenerateEvent(Event::GossipsubNotSupported { .. } | Event::SlowPeer { .. }) => {
            Some(0)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use libp2p_identity::PeerId;

    use super::*;
    use crate::TopicHash;

    fn subscribed(topic: String) -> ToSwarm<Event, HandlerIn> {
        ToSwarm::GenerateEvent(Event::Subscribed {
            peer_id: PeerId::random(),
            topic: TopicHash::from_raw(topic),
        })
    }

    #[test]
    fn logex_event_advisory_budget_preserves_order_and_recovers_after_delivery() {
        let mut events = EventQueue::new(QueueLimits {
            max_advisory_events: 2,
            max_advisory_bytes: 2,
            ..Default::default()
        });
        assert!(events.push_back(subscribed("a".into())));
        assert!(events.push_back(subscribed("b".into())));
        assert!(!events.push_back(subscribed("c".into())));
        assert!(matches!(events.pop_front(), Some(ToSwarm::GenerateEvent(
            Event::Subscribed { topic, .. }
        )) if topic.as_str() == "a"));
        assert!(events.push_back(subscribed("c".into())));
        for expected in ["b", "c"] {
            assert!(matches!(events.pop_front(), Some(ToSwarm::GenerateEvent(
                Event::Subscribed { topic, .. }
            )) if topic.as_str() == expected));
        }
        assert_eq!((events.advisory_events, events.advisory_bytes), (0, 0));
    }

    #[test]
    fn logex_event_advisory_bytes_include_spare_topic_capacity() {
        let mut topic = String::with_capacity(32);
        topic.push('a');
        let capacity = topic.capacity();
        let mut events = EventQueue::new(QueueLimits {
            max_advisory_events: 4,
            max_advisory_bytes: capacity,
            ..Default::default()
        });
        assert!(events.push_back(subscribed(topic)));
        assert!(!events.push_back(subscribed("b".into())));
        assert_eq!(
            (events.advisory_events, events.advisory_bytes),
            (1, capacity)
        );
        events.clear();
        assert_eq!((events.advisory_events, events.advisory_bytes), (0, 0));
        assert!(events.push_back(subscribed("b".into())));
    }

    #[test]
    fn logex_event_fixed_advisories_still_consume_entries() {
        let mut events = EventQueue::new(QueueLimits {
            max_advisory_events: 1,
            max_advisory_bytes: 0,
            ..Default::default()
        });
        let advisory = || {
            ToSwarm::GenerateEvent(Event::GossipsubNotSupported {
                peer_id: PeerId::random(),
            })
        };
        assert!(events.push_back(advisory()));
        assert!(!events.push_back(advisory()));
        assert!(events.pop_front().is_some());
        assert!(events.push_back(advisory()));
    }
}
