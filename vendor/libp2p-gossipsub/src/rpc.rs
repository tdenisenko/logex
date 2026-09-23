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
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
};

use futures::{stream::Peekable, Stream, StreamExt};

use crate::{config::QueueLimits, types::RpcOut};

/// A per-peer class budget, shared by all receivers for that peer.
#[derive(Debug)]
struct Budget {
    max_entries: usize,
    max_bytes: usize,
    entries: AtomicUsize,
    bytes: AtomicUsize,
}

impl Budget {
    fn new(max_entries: usize, max_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            max_entries,
            max_bytes,
            entries: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
        })
    }

    fn try_add(counter: &AtomicUsize, amount: usize, limit: usize) -> bool {
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(amount).filter(|next| *next <= limit) else {
                return false;
            };
            match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return true,
                Err(observed) => current = observed,
            }
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> Option<Ticket> {
        if !Self::try_add(&self.entries, 1, self.max_entries) {
            return None;
        }
        if !Self::try_add(&self.bytes, bytes, self.max_bytes) {
            self.entries.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(Ticket {
            budget: self.clone(),
            bytes,
        })
    }
}

/// No per-message allocation: the ticket shares its class budget. Ownership
/// follows the message through both the channel and any receiver's peeked slot.
#[derive(Debug)]
struct Ticket {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::Relaxed);
        self.budget.entries.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct QueuedRpc {
    rpc: RpcOut,
    ticket: Ticket,
}

impl QueuedRpc {
    fn into_rpc(self) -> RpcOut {
        let Self { rpc, ticket } = self;
        drop(ticket);
        rpc
    }
}

type QueueReceiver = Pin<Box<Peekable<async_channel::Receiver<QueuedRpc>>>>;

/// Admission failures distinguish local message sizing and channel lifecycle
/// from aggregate queue pressure, which may indicate a slow consumer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendErrorReason {
    TooLarge,
    Full,
    Closed,
}

#[derive(Debug)]
pub(crate) struct SendError {
    pub(crate) rpc: RpcOut,
    pub(crate) reason: SendErrorReason,
}

/// Priority-aware queued ownership with three independent count/byte budgets.
/// Handler-owned in-flight protobuf/frame data is a separate bounded owner.
#[derive(Debug)]
pub(crate) struct Sender {
    publish_budget: Arc<Budget>,
    control_budget: Arc<Budget>,
    non_priority_budget: Arc<Budget>,
    priority_sender: async_channel::Sender<QueuedRpc>,
    non_priority_sender: async_channel::Sender<QueuedRpc>,
    priority_receiver: async_channel::Receiver<QueuedRpc>,
    non_priority_receiver: async_channel::Receiver<QueuedRpc>,
}

impl Sender {
    pub(crate) fn new(cap: usize, limits: QueueLimits) -> Self {
        // Logical reservations bound both channels plus staged receiver items.
        // Control traffic has its own finite reserve independent of publishes.
        let (priority_sender, priority_receiver) = async_channel::unbounded();
        let (non_priority_sender, non_priority_receiver) = async_channel::unbounded();
        Self {
            publish_budget: Budget::new(cap / 2, limits.max_publish_bytes),
            control_budget: Budget::new(limits.max_control_messages, limits.max_control_bytes),
            non_priority_budget: Budget::new(cap / 2, limits.max_non_priority_bytes),
            priority_sender,
            non_priority_sender,
            priority_receiver,
            non_priority_receiver,
        }
    }

    pub(crate) fn new_receiver(&self) -> Receiver {
        Receiver {
            priority: Box::pin(self.priority_receiver.clone().peekable()),
            non_priority: Box::pin(self.non_priority_receiver.clone().peekable()),
        }
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn send_message(&self, rpc: RpcOut) -> Result<(), SendError> {
        let (budget, sender) = match &rpc {
            RpcOut::Publish { .. } => (&self.publish_budget, &self.priority_sender),
            RpcOut::Graft(_) | RpcOut::Prune(_) | RpcOut::Subscribe(_) | RpcOut::Unsubscribe(_) => {
                (&self.control_budget, &self.priority_sender)
            }
            RpcOut::Forward { .. } | RpcOut::IHave(_) | RpcOut::IWant(_) | RpcOut::IDontWant(_) => {
                (&self.non_priority_budget, &self.non_priority_sender)
            }
        };
        let Some(bytes) = rpc
            .retained_bytes()
            .filter(|bytes| *bytes <= budget.max_bytes)
        else {
            return Err(SendError {
                rpc,
                reason: SendErrorReason::TooLarge,
            });
        };
        if sender.is_closed() {
            return Err(SendError {
                rpc,
                reason: SendErrorReason::Closed,
            });
        }
        let Some(ticket) = budget.reserve(bytes) else {
            return Err(SendError {
                rpc,
                reason: if sender.is_closed() {
                    SendErrorReason::Closed
                } else {
                    SendErrorReason::Full
                },
            });
        };
        sender
            .try_send(QueuedRpc { rpc, ticket })
            .map_err(|err| SendError {
                rpc: err.into_inner().into_rpc(),
                // Both physical lanes are unbounded; only closure can fail here.
                reason: SendErrorReason::Closed,
            })
    }

    /// All priority-lane entries, including items peeked by receivers.
    #[cfg(feature = "metrics")]
    pub(crate) fn priority_queue_len(&self) -> usize {
        self.publish_budget
            .entries
            .load(Ordering::Relaxed)
            .saturating_add(self.control_budget.entries.load(Ordering::Relaxed))
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn non_priority_queue_len(&self) -> usize {
        self.non_priority_budget.entries.load(Ordering::Relaxed)
    }
}

/// A receiver retains budget ownership while inspecting staged queue entries.
#[derive(Debug)]
pub struct Receiver {
    priority: QueueReceiver,
    non_priority: QueueReceiver,
}

impl Receiver {
    /// Poll both lanes, registering wakes for arriving data and active timers.
    /// Closed lanes end only after their staged and channel entries are drained.
    pub(crate) fn poll_stale(&mut self, cx: &mut Context<'_>) -> Poll<Option<RpcOut>> {
        let mut closed = 0;
        for queue in [&mut self.priority, &mut self.non_priority] {
            match queue.as_mut().poll_peek_mut(cx) {
                Poll::Ready(Some(QueuedRpc { rpc, .. })) => {
                    let (RpcOut::Publish { timeout, .. } | RpcOut::Forward { timeout, .. }) = rpc
                    else {
                        continue;
                    };
                    if Pin::new(timeout).poll(cx).is_ready() {
                        let rpc = futures::ready!(queue.poll_next_unpin(cx))
                            .expect("peeked message remains owned by this receiver");
                        return Poll::Ready(Some(rpc.into_rpc()));
                    }
                }
                Poll::Ready(None) => closed += 1,
                Poll::Pending => {}
            }
        }
        if closed == 2 {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }

    /// Pending on an open channel means empty now, and registers the next wake.
    pub(crate) fn poll_is_empty(&mut self, cx: &mut Context<'_>) -> bool {
        let priority = self.priority.as_mut().poll_peek(cx);
        let non_priority = self.non_priority.as_mut().poll_peek(cx);
        !matches!(priority, Poll::Ready(Some(_))) && !matches!(non_priority, Poll::Ready(Some(_)))
    }

    #[cfg(test)]
    pub(crate) fn drain_priority(&mut self) -> Vec<RpcOut> {
        Self::drain(&mut self.priority)
    }

    #[cfg(test)]
    pub(crate) fn drain_non_priority(&mut self) -> Vec<RpcOut> {
        Self::drain(&mut self.non_priority)
    }

    #[cfg(test)]
    fn drain(queue: &mut QueueReceiver) -> Vec<RpcOut> {
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut messages = Vec::new();
        while let Poll::Ready(Some(message)) = queue.poll_next_unpin(&mut cx) {
            messages.push(message.into_rpc());
        }
        messages
    }

    #[cfg(test)]
    pub(crate) fn close_non_priority(&self) {
        self.non_priority.as_ref().get_ref().get_ref().close();
    }
}

impl Stream for Receiver {
    type Item = RpcOut;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<RpcOut>> {
        let priority_closed = match self.priority.poll_next_unpin(cx) {
            Poll::Ready(Some(rpc)) => return Poll::Ready(Some(rpc.into_rpc())),
            Poll::Ready(None) => true,
            Poll::Pending => false,
        };
        match self.non_priority.poll_next_unpin(cx) {
            Poll::Ready(Some(rpc)) => Poll::Ready(Some(rpc.into_rpc())),
            Poll::Ready(None) if priority_closed => Poll::Ready(None),
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod logex_tests {
    use std::time::Duration;

    use futures::{executor::block_on, future::poll_fn, task::noop_waker};
    use futures_timer::Delay;

    use super::*;
    use crate::{types::IDontWant, RawMessage, TopicHash};

    fn publish() -> RpcOut {
        RpcOut::Publish {
            message: RawMessage {
                source: None,
                data: vec![1],
                sequence_number: None,
                topic: TopicHash::from_raw("t"),
                signature: None,
                key: None,
                validated: true,
            },
            timeout: Delay::new(Duration::ZERO),
        }
    }

    #[test]
    fn logex_stale_publish_releases_queue_capacity() {
        let sender = Sender::new(2, QueueLimits::default());
        let mut receiver = sender.new_receiver();
        assert!(sender.send_message(publish()).is_ok());
        let stale = block_on(poll_fn(|cx| receiver.poll_stale(cx)));
        assert!(matches!(stale, Some(RpcOut::Publish { .. })));
        assert!(
            sender.send_message(publish()).is_ok(),
            "removing a stale publish must release its queue slot"
        );
    }

    #[test]
    fn logex_dropped_receiver_releases_peeked_publish_capacity() {
        let sender = Sender::new(2, QueueLimits::default());
        let mut receiver = sender.new_receiver();
        assert!(sender.send_message(publish()).is_ok());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(!receiver.poll_is_empty(&mut cx));
        drop(receiver);
        assert!(
            sender.send_message(publish()).is_ok(),
            "dropping a receiver's peeked publish must release its queue slot"
        );
    }

    #[test]
    fn logex_closed_publish_channel_does_not_charge_failed_send() {
        let sender = Sender::new(2, QueueLimits::default());
        sender.priority_sender.close();
        assert!(sender.send_message(publish()).is_err());
        assert_eq!(
            sender.publish_budget.entries.load(Ordering::Relaxed),
            0,
            "a rejected publish does not occupy a queue slot"
        );
    }

    #[test]
    fn logex_open_empty_queues_are_empty() {
        let sender = Sender::new(2, QueueLimits::default());
        let mut receiver = sender.new_receiver();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(
            receiver.poll_is_empty(&mut cx),
            "an open channel awaiting future messages has no queued messages"
        );
    }

    #[test]
    fn logex_closed_priority_does_not_hide_non_priority_message() {
        let sender = Sender::new(2, QueueLimits::default());
        let mut receiver = sender.new_receiver();
        assert!(sender
            .send_message(RpcOut::IDontWant(IDontWant {
                message_ids: vec![],
            }))
            .is_ok());
        sender.priority_sender.close();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            receiver.poll_next_unpin(&mut cx),
            Poll::Ready(Some(RpcOut::IDontWant(_)))
        ));
    }

    fn tiny_limits() -> QueueLimits {
        QueueLimits {
            max_control_messages: 1,
            max_control_bytes: 8,
            max_publish_bytes: 8,
            max_non_priority_bytes: 8,
            ..Default::default()
        }
    }

    fn non_priority() -> RpcOut {
        RpcOut::IDontWant(IDontWant {
            message_ids: vec![],
        })
    }

    fn usage(budget: &Budget) -> (usize, usize) {
        (
            budget.entries.load(Ordering::Relaxed),
            budget.bytes.load(Ordering::Relaxed),
        )
    }

    #[test]
    fn logex_three_queue_budgets_are_independent_and_include_peeked_items() {
        let sender = Sender::new(2, tiny_limits());
        let mut first = sender.new_receiver();
        let mut second = sender.new_receiver();
        assert!(sender.send_message(publish()).is_ok());
        assert!(sender
            .send_message(RpcOut::Subscribe(TopicHash::from_raw("t")))
            .is_ok());
        assert!(sender.send_message(non_priority()).is_ok());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        // Each receiver can take a private peeked slot, but cannot free budgets.
        assert!(!first.poll_is_empty(&mut cx));
        assert!(!second.poll_is_empty(&mut cx));
        assert!(sender.send_message(publish()).is_err());
        assert!(sender
            .send_message(RpcOut::Unsubscribe(TopicHash::from_raw("t")))
            .is_err());
        assert!(sender.send_message(non_priority()).is_err());
        assert_eq!(usage(&sender.publish_budget), (1, 2));
        assert_eq!(usage(&sender.control_budget), (1, 1));
        assert_eq!(usage(&sender.non_priority_budget), (1, 0));
        drop(first);
        assert_eq!(usage(&sender.publish_budget), (0, 0));
        assert_eq!(usage(&sender.non_priority_budget), (0, 0));
        // The second receiver still owns the control message it peeked.
        assert_eq!(usage(&sender.control_budget), (1, 1));
        assert!(sender.send_message(publish()).is_ok());
        assert!(sender.send_message(non_priority()).is_ok());
        drop(second);
        assert_eq!(usage(&sender.control_budget), (0, 0));
        let mut receiver = sender.new_receiver();
        assert_eq!(receiver.drain_priority().len(), 1);
        assert_eq!(receiver.drain_non_priority().len(), 1);
        assert_eq!(usage(&sender.publish_budget), (0, 0));
        assert_eq!(usage(&sender.non_priority_budget), (0, 0));
    }

    #[test]
    fn logex_queue_bytes_charge_capacity_and_rollback_failed_admission() {
        let limits = QueueLimits {
            max_control_messages: 3,
            max_control_bytes: 8,
            ..tiny_limits()
        };
        let sender = Sender::new(2, limits);
        let mut topic = String::with_capacity(8);
        topic.push('t');
        assert_eq!(topic.capacity(), 8);
        assert!(sender
            .send_message(RpcOut::Subscribe(TopicHash::from_raw(topic)))
            .is_ok());
        assert_eq!(usage(&sender.control_budget), (1, 8));
        assert!(sender
            .send_message(RpcOut::Subscribe(TopicHash::from_raw("x")))
            .is_err());
        assert_eq!(usage(&sender.control_budget), (1, 8));
        let mut receiver = sender.new_receiver();
        assert_eq!(receiver.drain_priority().len(), 1);
        assert_eq!(usage(&sender.control_budget), (0, 0));
        assert!(sender
            .send_message(RpcOut::Unsubscribe(TopicHash::from_raw("x")))
            .is_ok());
        sender.priority_sender.close();
        assert!(sender
            .send_message(RpcOut::Subscribe(TopicHash::from_raw("y")))
            .is_err());
        assert_eq!(usage(&sender.control_budget), (1, 1));
        receiver.drain_priority();
        assert_eq!(usage(&sender.control_budget), (0, 0));
    }

    #[test]
    fn logex_rpc_weight_includes_nested_and_spare_allocations() {
        use crate::types::{Graft, IHave, IWant, MessageId, PeerInfo, Prune};
        let mut ids = Vec::with_capacity(3);
        let mut id = Vec::with_capacity(5);
        id.push(1);
        let expected_ids = ids.capacity() * std::mem::size_of::<MessageId>() + id.capacity();
        ids.push(MessageId(id));
        let mut topic = String::with_capacity(7);
        topic.push('t');
        let topic_bytes = topic.capacity();
        let ihave = RpcOut::IHave(IHave {
            topic_hash: TopicHash::from_raw(topic),
            message_ids: ids,
        });
        assert_eq!(ihave.retained_bytes(), Some(expected_ids + topic_bytes));
        let RpcOut::IHave(ihave) = ihave else {
            unreachable!()
        };
        let iwant = RpcOut::IWant(IWant {
            message_ids: ihave.message_ids,
        });
        assert_eq!(iwant.retained_bytes(), Some(expected_ids));
        let RpcOut::IWant(iwant) = iwant else {
            unreachable!()
        };
        assert_eq!(
            RpcOut::IDontWant(IDontWant {
                message_ids: iwant.message_ids
            })
            .retained_bytes(),
            Some(expected_ids)
        );
        let peers = Vec::<PeerInfo>::with_capacity(2);
        let expected = peers.capacity() * std::mem::size_of::<PeerInfo>() + topic_bytes;
        let prune = RpcOut::Prune(Prune {
            topic_hash: ihave.topic_hash,
            peers,
            backoff: None,
        });
        assert_eq!(prune.retained_bytes(), Some(expected));
        let RpcOut::Prune(prune) = prune else {
            unreachable!()
        };
        assert_eq!(
            RpcOut::Graft(Graft {
                topic_hash: prune.topic_hash
            })
            .retained_bytes(),
            Some(topic_bytes)
        );
        let RpcOut::Publish {
            mut message,
            timeout,
        } = publish()
        else {
            unreachable!()
        };
        message.data = Vec::with_capacity(3);
        message.signature = Some(Vec::with_capacity(2));
        message.key = Some(Vec::with_capacity(4));
        let expected = message.data.capacity()
            + message.topic.retained_bytes()
            + message.signature.as_ref().unwrap().capacity()
            + message.key.as_ref().unwrap().capacity();
        let publish = RpcOut::Publish { message, timeout };
        assert_eq!(publish.retained_bytes(), Some(expected));
        let RpcOut::Publish { message, timeout } = publish else {
            unreachable!()
        };
        assert_eq!(
            RpcOut::Forward { message, timeout }.retained_bytes(),
            Some(expected)
        );
    }

    #[test]
    fn logex_byte_budgets_reject_large_publish_and_non_priority_without_count_leaks() {
        let limits = QueueLimits {
            max_publish_bytes: 1,
            max_non_priority_bytes: 1,
            ..tiny_limits()
        };
        let sender = Sender::new(2, limits);
        assert!(sender.send_message(publish()).is_err());
        assert_eq!(usage(&sender.publish_budget), (0, 0));
        let RpcOut::Publish { message, timeout } = publish() else {
            unreachable!()
        };
        assert!(sender
            .send_message(RpcOut::Forward { message, timeout })
            .is_err());
        assert_eq!(usage(&sender.non_priority_budget), (0, 0));
        assert!(sender.send_message(non_priority()).is_ok());
    }

    #[test]
    fn logex_dropping_last_queue_owner_releases_all_classes() {
        let sender = Sender::new(2, tiny_limits());
        let budgets = [
            sender.publish_budget.clone(),
            sender.control_budget.clone(),
            sender.non_priority_budget.clone(),
        ];
        let mut receiver = sender.new_receiver();
        assert!(sender.send_message(publish()).is_ok());
        assert!(sender
            .send_message(RpcOut::Subscribe(TopicHash::from_raw("t")))
            .is_ok());
        assert!(sender.send_message(non_priority()).is_ok());
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(!receiver.poll_is_empty(&mut cx));
        drop(sender);
        assert!(budgets.iter().all(|budget| usage(budget).0 == 1));
        drop(receiver);
        assert!(budgets.iter().all(|budget| usage(budget) == (0, 0)));
    }

    #[test]
    fn logex_empty_poll_registers_wake_and_stream_waits_for_both_lanes() {
        struct WakeCount(AtomicUsize);
        impl futures::task::ArcWake for WakeCount {
            fn wake_by_ref(arc_self: &Arc<Self>) {
                arc_self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let sender = Sender::new(2, tiny_limits());
        let mut receiver = sender.new_receiver();
        let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
        let waker = futures::task::waker(wakes.clone());
        let mut cx = Context::from_waker(&waker);
        assert!(receiver.poll_is_empty(&mut cx));
        assert!(sender
            .send_message(RpcOut::Subscribe(TopicHash::from_raw("t")))
            .is_ok());
        assert!(wakes.0.load(Ordering::Relaxed) > 0);
        assert!(matches!(
            receiver.poll_next_unpin(&mut cx),
            Poll::Ready(Some(RpcOut::Subscribe(_)))
        ));
        sender.non_priority_sender.close();
        assert!(receiver.poll_next_unpin(&mut cx).is_pending());
        sender.priority_sender.close();
        assert!(matches!(
            receiver.poll_next_unpin(&mut cx),
            Poll::Ready(None)
        ));
    }

    #[test]
    fn logex_checked_budget_reservation_rolls_back_on_overflow() {
        let budget = Budget::new(2, usize::MAX);
        let ticket = budget.reserve(usize::MAX).unwrap();
        assert!(budget.reserve(1).is_none());
        assert_eq!(usage(&budget), (1, usize::MAX));
        drop(ticket);
        assert_eq!(usage(&budget), (0, 0));
        let ticket = budget.reserve(1).unwrap();
        assert_eq!(usage(&budget), (1, 1));
        drop(ticket);
        assert_eq!(usage(&budget), (0, 0));
    }

    #[test]
    fn logex_admission_reasons_distinguish_size_pressure_and_closed_ownership() {
        let sender = Sender::new(
            4,
            QueueLimits {
                max_publish_bytes: 3,
                max_non_priority_bytes: 1,
                ..tiny_limits()
            },
        );
        let mut receiver = sender.new_receiver();
        assert!(sender.send_message(publish()).is_ok());
        let error = sender.send_message(publish()).unwrap_err();
        assert_eq!(error.reason, SendErrorReason::Full);
        assert!(matches!(error.rpc, RpcOut::Publish { .. }));
        assert_eq!(usage(&sender.publish_budget), (1, 2));
        let RpcOut::Publish { message, timeout } = publish() else {
            unreachable!()
        };
        let error = sender
            .send_message(RpcOut::Forward { message, timeout })
            .unwrap_err();
        assert_eq!(error.reason, SendErrorReason::TooLarge);
        assert!(matches!(error.rpc, RpcOut::Forward { .. }));
        assert_eq!(usage(&sender.non_priority_budget), (0, 0));
        sender.priority_sender.close();
        // A closed channel remains a lifecycle failure even when aggregate bytes are full.
        let error = sender.send_message(publish()).unwrap_err();
        assert_eq!(error.reason, SendErrorReason::Closed);
        assert_eq!(usage(&sender.publish_budget), (1, 2));
        receiver.drain_priority();
        assert_eq!(usage(&sender.publish_budget), (0, 0));
        assert_eq!(
            sender.send_message(publish()).unwrap_err().reason,
            SendErrorReason::Closed
        );
        assert_eq!(usage(&sender.publish_budget), (0, 0));
    }

    #[test]
    fn logex_closed_non_priority_preserves_then_releases_queued_ownership() {
        let sender = Sender::new(2, tiny_limits());
        let mut receiver = sender.new_receiver();
        let RpcOut::Publish { message, timeout } = publish() else {
            unreachable!()
        };
        assert!(sender
            .send_message(RpcOut::Forward { message, timeout })
            .is_ok());
        assert_eq!(usage(&sender.non_priority_budget), (1, 2));
        receiver.close_non_priority();
        let error = sender.send_message(non_priority()).unwrap_err();
        assert_eq!(error.reason, SendErrorReason::Closed);
        assert!(matches!(error.rpc, RpcOut::IDontWant(_)));
        assert_eq!(usage(&sender.non_priority_budget), (1, 2));
        let messages = receiver.drain_non_priority();
        assert!(matches!(messages.as_slice(), [RpcOut::Forward { .. }]));
        assert_eq!(usage(&sender.non_priority_budget), (0, 0));
        assert_eq!(
            sender.send_message(non_priority()).unwrap_err().reason,
            SendErrorReason::Closed
        );
        assert_eq!(usage(&sender.non_priority_budget), (0, 0));
    }
}
