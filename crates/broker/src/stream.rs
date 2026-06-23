//! Stream (Plexus) fan-out core, owner-side.
//!
//! Counterpart to the work queue's lease/poll delivery, but for fan-out: every
//! live subscriber sees every matching record as it is published, and a consumer
//! reads the durable log at its own cursor. This module is the pure in-memory
//! heart of one (channel, partition): a newest-X ring of recent records plus a
//! registry of live subscribers. It does no IO. The broker owns persistence
//! (stroma `append_stream_record`), backfill reads (stroma `scan_messages_from`),
//! durable cursor commits, and encoding the wire Deliver frames; this core just
//! decides who gets what, now.
//!
//! Delivery model: a new subscriber attaches at the current tail (its live
//! deliveries flow through an mpsc channel), and is handed a backfill plan for
//! everything between its requested start and that tail. Recent backfill comes
//! straight from the ring; older backfill is a log range the broker reads from
//! stroma. The broker sends the backfill (log first, then ring) before draining
//! the live channel, so the consumer sees a single gap-free, in-order stream.

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use fibril_storage::Offset;
use tokio::sync::mpsc;

/// Per-channel durability tier (a channel-level knob, not a separate channel
/// type). Governs how a publish is persisted and when the producer is confirmed;
/// the fan-out core is the same for all tiers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StreamDurability {
    /// Persist to the log asynchronously, do not gate delivery or confirm. Lowest
    /// latency, weakest guarantee.
    Ephemeral,
    /// Deliver immediately with a speculative marker, defer the producer confirm
    /// until the record is durable (the ghost-flag pattern).
    Speculative,
    /// Persist (and replicate to min-ISR when configured) before confirming.
    #[default]
    Durable,
}

impl StreamDurability {
    pub fn as_u8(self) -> u8 {
        match self {
            StreamDurability::Ephemeral => 0,
            StreamDurability::Speculative => 1,
            StreamDurability::Durable => 2,
        }
    }

    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(StreamDurability::Ephemeral),
            1 => Some(StreamDurability::Speculative),
            2 => Some(StreamDurability::Durable),
            _ => None,
        }
    }
}

/// A single header-match clause's value pattern: a literal that may contain `*`
/// wildcards (each `*` matches any run of characters, including empty). This is
/// the entire grammar - deliberately no regex, character classes, or anchors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WildcardPattern {
    /// The pattern split on `*`. `segments.len() == (number of `*`) + 1`. A
    /// pattern with no `*` is a single segment (exact match).
    segments: Vec<String>,
}

impl WildcardPattern {
    pub fn new(pattern: &str) -> Self {
        WildcardPattern {
            segments: pattern.split('*').map(|s| s.to_string()).collect(),
        }
    }

    /// True when `value` matches the pattern. With one segment this is an exact
    /// match; otherwise the first and last segments must anchor the start and end
    /// and the middle segments must appear in order.
    pub fn matches(&self, value: &str) -> bool {
        if self.segments.len() == 1 {
            return self.segments[0] == value;
        }
        let first = &self.segments[0];
        let last = &self.segments[self.segments.len() - 1];
        if !value.starts_with(first.as_str()) || !value.ends_with(last.as_str()) {
            return false;
        }
        // The start and end anchors must not overlap on a short value.
        if value.len() < first.len() + last.len() {
            return false;
        }
        let mut pos = first.len();
        let end = value.len() - last.len();
        for mid in &self.segments[1..self.segments.len() - 1] {
            if mid.is_empty() {
                continue;
            }
            match value[pos..end].find(mid.as_str()) {
                Some(found) => pos += found + mid.len(),
                None => return false,
            }
        }
        true
    }
}

/// A consumer-side header filter: an AND of `header == pattern` clauses, where a
/// clause matches when the record carries that header and its value matches the
/// wildcard pattern. Stream-only (queues are left untouched). A record with no
/// clauses matches everything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StreamFilter {
    clauses: Vec<(String, WildcardPattern)>,
}

impl StreamFilter {
    pub fn new() -> Self {
        StreamFilter::default()
    }

    /// Build from `(header, value-pattern)` pairs. The value may contain `*`.
    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: AsRef<str>,
    {
        StreamFilter {
            clauses: pairs
                .into_iter()
                .map(|(k, v)| (k.into(), WildcardPattern::new(v.as_ref())))
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty()
    }

    /// True when every clause matches a header on the record.
    pub fn matches(&self, headers: &HashMap<String, String>) -> bool {
        self.clauses.iter().all(|(key, pattern)| {
            headers
                .get(key)
                .map(|value| pattern.matches(value))
                .unwrap_or(false)
        })
    }
}

/// A record as it flows through fan-out. The payload is shared (`Arc`) so a
/// multicast to many subscribers clones a pointer, not the bytes.
#[derive(Debug, Clone)]
pub struct StreamRecord {
    pub offset: Offset,
    pub published: u64,
    pub publish_received: u64,
    pub content_type: Option<String>,
    /// User headers, used for filtering.
    pub headers: HashMap<String, String>,
    pub payload: Arc<[u8]>,
}

struct Subscriber {
    tx: mpsc::Sender<Arc<StreamRecord>>,
    filter: Option<StreamFilter>,
    /// Set when a live send was dropped because the consumer's channel was full.
    /// At-least-once safe: a durable consumer re-reads the gap from its cursor on
    /// reconnect. Surfaced so the broker can flag the consumer as lagging.
    lagged: bool,
}

/// The plan a new subscriber must follow to catch up to the live tail, returned
/// by [`StreamFanout::subscribe`].
pub struct Subscription {
    pub id: u64,
    /// Live records from the tail onward (already filtered).
    pub receiver: mpsc::Receiver<Arc<StreamRecord>>,
    /// Recent records the ring served directly (already filtered), ordered by
    /// offset and all below the live tail. Send these after the log backfill.
    pub ring_backfill: Vec<Arc<StreamRecord>>,
    /// The half-open offset range older than the ring that the broker must read
    /// from the durable log (and filter) before the ring backfill. Empty when the
    /// start was already within the ring or at the tail.
    pub log_backfill: Range<Offset>,
}

/// The in-memory fan-out core for one (channel, partition). Driven by a single
/// owner-side actor, so methods take `&mut self` and need no locking.
pub struct StreamFanout {
    capacity: usize,
    ring: std::collections::VecDeque<Arc<StreamRecord>>,
    /// One past the highest published offset (the next offset to publish).
    next: Offset,
    subscribers: HashMap<u64, Subscriber>,
    next_sub_id: u64,
}

impl StreamFanout {
    /// `capacity` is the newest-X ring size; `next` is the tail at creation
    /// (reconciled from the durable log on materialization).
    pub fn new(capacity: usize, next: Offset) -> Self {
        StreamFanout {
            capacity: capacity.max(1),
            ring: std::collections::VecDeque::new(),
            next,
            subscribers: HashMap::new(),
            next_sub_id: 0,
        }
    }

    /// Lowest offset currently held in the ring (the oldest record it can serve
    /// without a log read). Equals the tail when the ring is empty.
    pub fn ring_base(&self) -> Offset {
        self.ring.front().map(|r| r.offset).unwrap_or(self.next)
    }

    /// One past the highest published offset.
    pub fn tail(&self) -> Offset {
        self.next
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    /// Publish a record to the ring and multicast it to every matching live
    /// subscriber. The record's offset must be the current tail (records are
    /// appended in order); the tail advances past it. A subscriber whose channel
    /// is full is marked lagging and the record is dropped to it (it will re-read
    /// from its durable cursor), and a subscriber whose channel has closed is
    /// dropped from the registry.
    pub fn publish(&mut self, record: StreamRecord) -> Arc<StreamRecord> {
        let record = Arc::new(record);
        self.ring.push_back(record.clone());
        while self.ring.len() > self.capacity {
            self.ring.pop_front();
        }
        self.next = record.offset + 1;

        self.subscribers.retain(|_, sub| {
            let matched = sub
                .filter
                .as_ref()
                .map(|f| f.matches(&record.headers))
                .unwrap_or(true);
            if !matched {
                return true;
            }
            match sub.tx.try_send(record.clone()) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => {
                    sub.lagged = true;
                    true
                }
                Err(mpsc::error::TrySendError::Closed(_)) => false,
            }
        });

        record
    }

    /// Register a live subscriber attached at the current tail and return its
    /// catch-up plan. `start` is the resolved absolute offset (the broker resolves
    /// latest/earliest/N-back/by-time/durable-resume to an offset first), clamped
    /// here to at most the tail. `channel_capacity` bounds the live buffer.
    pub fn subscribe(
        &mut self,
        start: Offset,
        filter: Option<StreamFilter>,
        channel_capacity: usize,
    ) -> Subscription {
        let id = self.next_sub_id;
        self.next_sub_id += 1;
        let (tx, receiver) = mpsc::channel(channel_capacity.max(1));
        self.subscribers.insert(
            id,
            Subscriber {
                tx,
                filter: filter.clone(),
                lagged: false,
            },
        );

        let tail = self.next;
        let start = start.min(tail);
        let ring_base = self.ring_base();

        let filter_ref = filter.as_ref();
        let keep = |r: &Arc<StreamRecord>| filter_ref.map(|f| f.matches(&r.headers)).unwrap_or(true);

        let ring_backfill: Vec<Arc<StreamRecord>> = self
            .ring
            .iter()
            .filter(|r| r.offset >= start && keep(r))
            .cloned()
            .collect();

        let log_backfill = if start < ring_base {
            start..ring_base
        } else {
            tail..tail
        };

        Subscription {
            id,
            receiver,
            ring_backfill,
            log_backfill,
        }
    }

    pub fn unsubscribe(&mut self, id: u64) {
        self.subscribers.remove(&id);
    }

    /// Whether a subscriber has been marked lagging (a live send was dropped).
    pub fn is_lagged(&self, id: u64) -> Option<bool> {
        self.subscribers.get(&id).map(|s| s.lagged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(offset: Offset, headers: &[(&str, &str)]) -> StreamRecord {
        StreamRecord {
            offset,
            published: 0,
            publish_received: 0,
            content_type: None,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            payload: Arc::from(b"x".as_slice()),
        }
    }

    #[test]
    fn wildcard_exact_and_globs() {
        assert!(WildcardPattern::new("eu-west").matches("eu-west"));
        assert!(!WildcardPattern::new("eu-west").matches("eu-east"));
        assert!(WildcardPattern::new("eu-*").matches("eu-west"));
        assert!(WildcardPattern::new("eu-*").matches("eu-"));
        assert!(!WildcardPattern::new("eu-*").matches("us-west"));
        assert!(WildcardPattern::new("*-west").matches("eu-west"));
        assert!(WildcardPattern::new("a*c").matches("abc"));
        assert!(WildcardPattern::new("a*c").matches("ac"));
        assert!(!WildcardPattern::new("a*c").matches("ab"));
        assert!(WildcardPattern::new("*").matches("anything"));
        assert!(WildcardPattern::new("a*b*c").matches("axxbyyc"));
        assert!(!WildcardPattern::new("a*b*c").matches("axxc"));
    }

    #[test]
    fn filter_ands_clauses_and_requires_presence() {
        let f = StreamFilter::from_pairs([("region", "eu-*"), ("kind", "pig")]);
        let mut h: HashMap<String, String> = HashMap::new();
        h.insert("region".into(), "eu-west".into());
        h.insert("kind".into(), "pig".into());
        assert!(f.matches(&h));
        h.insert("kind".into(), "cow".into());
        assert!(!f.matches(&h));
        h.remove("kind");
        assert!(!f.matches(&h));
        // empty filter matches everything
        assert!(StreamFilter::new().matches(&h));
    }

    #[test]
    fn publish_multicasts_to_live_subscribers() {
        let mut fo = StreamFanout::new(8, 0);
        let mut sub = fo.subscribe(0, None, 16);
        assert!(sub.ring_backfill.is_empty());
        assert_eq!(sub.log_backfill, 0..0);

        fo.publish(rec(0, &[]));
        fo.publish(rec(1, &[]));

        assert_eq!(sub.receiver.try_recv().unwrap().offset, 0);
        assert_eq!(sub.receiver.try_recv().unwrap().offset, 1);
    }

    #[test]
    fn live_delivery_respects_filter() {
        let mut fo = StreamFanout::new(8, 0);
        let mut sub = fo.subscribe(0, Some(StreamFilter::from_pairs([("kind", "pig")])), 16);
        fo.publish(rec(0, &[("kind", "cow")]));
        fo.publish(rec(1, &[("kind", "pig")]));
        // only the matching record arrives
        assert_eq!(sub.receiver.try_recv().unwrap().offset, 1);
        assert!(sub.receiver.try_recv().is_err());
    }

    #[test]
    fn subscribe_within_ring_serves_backfill_from_ring() {
        let mut fo = StreamFanout::new(8, 0);
        for i in 0..5 {
            fo.publish(rec(i, &[]));
        }
        // start at 2: records 2,3,4 are in the ring, nothing from the log
        let sub = fo.subscribe(2, None, 16);
        let offs: Vec<Offset> = sub.ring_backfill.iter().map(|r| r.offset).collect();
        assert_eq!(offs, vec![2, 3, 4]);
        assert_eq!(sub.log_backfill, 5..5);
    }

    #[test]
    fn subscribe_below_ring_reports_log_backfill_range() {
        // capacity 3, publish 6 -> ring holds offsets 3,4,5; base = 3
        let mut fo = StreamFanout::new(3, 0);
        for i in 0..6 {
            fo.publish(rec(i, &[]));
        }
        assert_eq!(fo.ring_base(), 3);
        let sub = fo.subscribe(1, None, 16);
        // 1,2 come from the log; 3,4,5 from the ring
        assert_eq!(sub.log_backfill, 1..3);
        let offs: Vec<Offset> = sub.ring_backfill.iter().map(|r| r.offset).collect();
        assert_eq!(offs, vec![3, 4, 5]);
    }

    #[test]
    fn ring_backfill_is_filtered() {
        let mut fo = StreamFanout::new(8, 0);
        fo.publish(rec(0, &[("kind", "cow")]));
        fo.publish(rec(1, &[("kind", "pig")]));
        fo.publish(rec(2, &[("kind", "pig")]));
        let sub = fo.subscribe(0, Some(StreamFilter::from_pairs([("kind", "pig")])), 16);
        let offs: Vec<Offset> = sub.ring_backfill.iter().map(|r| r.offset).collect();
        assert_eq!(offs, vec![1, 2]);
    }

    #[test]
    fn full_channel_marks_lagging_not_blocked() {
        let mut fo = StreamFanout::new(64, 0);
        let sub = fo.subscribe(0, None, 1);
        // channel capacity 1: first send fits, the rest overflow
        for i in 0..5 {
            fo.publish(rec(i, &[]));
        }
        assert_eq!(fo.is_lagged(sub.id), Some(true));
        // hold sub so the receiver is not dropped before this assertion
        drop(sub);
    }

    #[test]
    fn closed_receiver_drops_subscriber() {
        let mut fo = StreamFanout::new(8, 0);
        let sub = fo.subscribe(0, None, 16);
        let id = sub.id;
        drop(sub); // receiver gone
        fo.publish(rec(0, &[]));
        assert_eq!(fo.subscriber_count(), 0);
        assert_eq!(fo.is_lagged(id), None);
    }

    #[test]
    fn subscribe_past_tail_clamps_to_latest() {
        let mut fo = StreamFanout::new(8, 5);
        let sub = fo.subscribe(999, None, 16);
        assert!(sub.ring_backfill.is_empty());
        assert_eq!(sub.log_backfill, 5..5);
    }
}
