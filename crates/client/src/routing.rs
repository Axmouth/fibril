//! Routing / discovery surface, opt in via [`Client::routing`].
//!
//! Built entirely on the public client API (the subscribe builders, the
//! catalogue feed, and `Clone`), so it adds no coupling to the connection core
//! and composes with the delivery-guarantee wrappers: a pattern subscription
//! yields the same message types a single-channel subscription does, and reliable
//! publishing is still reached through the derefed [`Client`].

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};

use crate::{Catalogue, Client, FibrilResult, InflightMessage, Message};

/// Per-channel buffering for the merged pattern stream, multiplied by prefetch.
/// Bounded so a slow consumer backpressures every attached channel rather than
/// letting the client buffer without limit.
const CHANNEL_FANIN_BUFFER: usize = 32;

/// Owned routing/discovery view over a [`Client`].
///
/// Discovery is a first-class but opt-in capability, kept off the default client
/// surface: obtain one with [`Client::routing`]. It is a cheap clone of the
/// connection (the client is reference-counted) and derefs to [`Client`], so
/// every normal operation - publish, reliable publish, explicit subscribe - is
/// available on it too, and routing composes with them rather than replacing
/// them.
#[derive(Debug, Clone)]
pub struct RoutingClient {
    client: Client,
}

impl std::ops::Deref for RoutingClient {
    type Target = Client;
    fn deref(&self) -> &Client {
        &self.client
    }
}

impl Client {
    /// Opt in to the routing/discovery surface. Returns an owned [`RoutingClient`]
    /// sharing this connection. The plain client stays usable.
    pub fn routing(&self) -> RoutingClient {
        RoutingClient {
            client: self.clone(),
        }
    }
}

impl RoutingClient {
    /// Begin a pattern subscription over the work queues whose topic matches
    /// `pattern`.
    ///
    /// `pattern` is a `*`-wildcard glob (each `*` matches any run of characters,
    /// including empty), the same grammar as the per-subscription header filter,
    /// with no regex. `"*"` matches every topic. The subscription fans in across
    /// every currently-matching queue and keeps attaching queues that start
    /// matching later, so newly declared channels are picked up without a
    /// reconnect.
    pub fn subscribe_pattern(&self, pattern: impl AsRef<str>) -> PatternSubscribeBuilder {
        PatternSubscribeBuilder {
            client: self.client.clone(),
            glob: TopicGlob::new(pattern.as_ref()),
            prefetch: 1,
            exclusive: false,
        }
    }

    /// Begin a pattern subscription over the Plexus streams whose topic matches
    /// `pattern`. Same matching and auto-pickup behaviour as
    /// [`subscribe_pattern`](Self::subscribe_pattern), over streams instead of
    /// work queues.
    pub fn subscribe_stream_pattern(
        &self,
        pattern: impl AsRef<str>,
    ) -> StreamPatternSubscribeBuilder {
        StreamPatternSubscribeBuilder {
            client: self.client.clone(),
            glob: TopicGlob::new(pattern.as_ref()),
            prefetch: 16,
            start: StreamStartChoice::Latest,
            filter: Vec::new(),
            durable_name: None,
        }
    }
}

/// The stream start positions a pattern subscription can request. Mirrors the
/// single-stream builder's `from_*` methods (the client does not expose
/// offset-anchored starts).
#[derive(Clone, Copy)]
enum StreamStartChoice {
    Latest,
    Earliest,
    NBack(u64),
    ByTime(u64),
}

/// The channel a pattern-delivered message came from. A pattern fans in across
/// many channels, so the message is paired with its source for routing back to
/// per-topic handling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternSource {
    pub topic: String,
    /// The queue's group namespace, or `None` for the ungrouped default and for
    /// streams (which have no group).
    pub group: Option<String>,
}

/// Builder for a work-queue pattern subscription
/// ([`RoutingClient::subscribe_pattern`]).
pub struct PatternSubscribeBuilder {
    client: Client,
    glob: TopicGlob,
    prefetch: u32,
    exclusive: bool,
}

impl PatternSubscribeBuilder {
    /// Per-channel prefetch, applied to every attached queue (see
    /// [`crate::SubscriptionBuilder::prefetch`]).
    pub fn prefetch(mut self, prefetch: u32) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Consume every matched queue as part of its exclusive cohort (see
    /// [`crate::SubscriptionBuilder::exclusive`]).
    pub fn exclusive(mut self) -> Self {
        self.exclusive = true;
        self
    }

    /// Start with manual acknowledgement. Each delivered [`InflightMessage`] must
    /// be settled, exactly as for a single-channel subscription.
    pub async fn sub(self) -> FibrilResult<PatternSubscription> {
        let PatternSubscribeBuilder {
            client,
            glob,
            prefetch,
            exclusive,
        } = self;
        let (out, rx) = merged_channel(prefetch);
        let attach: AttachFn<InflightMessage> = Arc::new(move |client, source, out| {
            Box::pin(attach_queue_manual(
                client, source, prefetch, exclusive, out,
            ))
        });
        tokio::spawn(run_pattern(client, glob, ChannelKind::Queue, out, attach));
        Ok(PatternSubscription { rx })
    }

    /// Start with client-side automatic acknowledgement, yielding [`Message`].
    pub async fn sub_auto_ack(self) -> FibrilResult<AutoAckPatternSubscription> {
        let PatternSubscribeBuilder {
            client,
            glob,
            prefetch,
            exclusive,
        } = self;
        let (out, rx) = merged_channel(prefetch);
        let attach: AttachFn<Message> = Arc::new(move |client, source, out| {
            Box::pin(attach_queue_auto(client, source, prefetch, exclusive, out))
        });
        tokio::spawn(run_pattern(client, glob, ChannelKind::Queue, out, attach));
        Ok(AutoAckPatternSubscription { rx })
    }
}

/// Builder for a stream pattern subscription
/// ([`RoutingClient::subscribe_stream_pattern`]).
pub struct StreamPatternSubscribeBuilder {
    client: Client,
    glob: TopicGlob,
    prefetch: u32,
    start: StreamStartChoice,
    filter: Vec<(String, String)>,
    durable_name: Option<String>,
}

impl StreamPatternSubscribeBuilder {
    /// Per-channel prefetch, applied to every attached stream.
    pub fn prefetch(mut self, prefetch: u32) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Begin each attached stream at the live tail (the default).
    pub fn from_latest(mut self) -> Self {
        self.start = StreamStartChoice::Latest;
        self
    }

    /// Begin each attached stream at the oldest retained record.
    pub fn from_earliest(mut self) -> Self {
        self.start = StreamStartChoice::Earliest;
        self
    }

    /// Begin `count` records back from each stream's tail.
    pub fn from_last(mut self, count: u64) -> Self {
        self.start = StreamStartChoice::NBack(count);
        self
    }

    /// Begin at the first record at or after this wall-clock time (ms).
    pub fn from_time(mut self, time_ms: u64) -> Self {
        self.start = StreamStartChoice::ByTime(time_ms);
        self
    }

    /// Add a header-match clause applied to every attached stream (see
    /// [`crate::StreamSubscriptionBuilder::filter`]). Repeatable, AND-ed.
    pub fn filter(mut self, header: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.filter.push((header.into(), pattern.into()));
        self
    }

    /// Use a durable broker-side cursor of this name on every attached stream.
    /// each stream tracks its own cursor under the name.
    pub fn durable(mut self, name: impl Into<String>) -> Self {
        self.durable_name = Some(name.into());
        self
    }

    /// Start with manual acknowledgement. Completing a message advances its
    /// stream's durable cursor past the offset.
    pub async fn sub(self) -> FibrilResult<PatternSubscription> {
        let StreamPatternSubscribeBuilder {
            client,
            glob,
            prefetch,
            start,
            filter,
            durable_name,
        } = self;
        let (out, rx) = merged_channel(prefetch);
        let config = StreamAttachConfig {
            prefetch,
            start,
            filter,
            durable_name,
        };
        let attach: AttachFn<InflightMessage> = Arc::new(move |client, source, out| {
            Box::pin(attach_stream_manual(client, source, config.clone(), out))
        });
        tokio::spawn(run_pattern(client, glob, ChannelKind::Stream, out, attach));
        Ok(PatternSubscription { rx })
    }

    /// Start with client-side automatic acknowledgement, yielding [`Message`]. The
    /// broker advances each stream's durable cursor as it delivers.
    pub async fn sub_auto_ack(self) -> FibrilResult<AutoAckPatternSubscription> {
        let StreamPatternSubscribeBuilder {
            client,
            glob,
            prefetch,
            start,
            filter,
            durable_name,
        } = self;
        let (out, rx) = merged_channel(prefetch);
        let config = StreamAttachConfig {
            prefetch,
            start,
            filter,
            durable_name,
        };
        let attach: AttachFn<Message> = Arc::new(move |client, source, out| {
            Box::pin(attach_stream_auto(client, source, config.clone(), out))
        });
        tokio::spawn(run_pattern(client, glob, ChannelKind::Stream, out, attach));
        Ok(AutoAckPatternSubscription { rx })
    }
}

/// A live manual-ack fan-in over every channel matching a glob, with auto-pickup
/// of channels that start matching later. Each item carries its [`PatternSource`]
/// so the caller can route by origin. The [`InflightMessage`] is settled exactly
/// as for a single-channel subscription. Dropping it stops every attached channel
/// and the catalogue watcher.
pub struct PatternSubscription {
    rx: mpsc::Receiver<(PatternSource, InflightMessage)>,
}

impl PatternSubscription {
    /// Receive the next message and the channel it came from, or `None` when
    /// closed.
    pub async fn recv(&mut self) -> Option<(PatternSource, InflightMessage)> {
        self.rx.recv().await
    }

    /// Convert into a stream of `(source, message)` items.
    pub fn into_stream(self) -> impl futures::Stream<Item = (PatternSource, InflightMessage)> {
        futures::stream::unfold(self, |mut s| async move {
            s.rx.recv().await.map(|item| (item, s))
        })
    }
}

/// Auto-ack counterpart of [`PatternSubscription`], yielding settled [`Message`]s.
pub struct AutoAckPatternSubscription {
    rx: mpsc::Receiver<(PatternSource, Message)>,
}

impl AutoAckPatternSubscription {
    /// Receive the next message and the channel it came from, or `None` when
    /// closed.
    pub async fn recv(&mut self) -> Option<(PatternSource, Message)> {
        self.rx.recv().await
    }

    /// Convert into a stream of `(source, message)` items.
    pub fn into_stream(self) -> impl futures::Stream<Item = (PatternSource, Message)> {
        futures::stream::unfold(self, |mut s| async move {
            s.rx.recv().await.map(|item| (item, s))
        })
    }
}

/// Which catalogue list a pattern matches against.
#[derive(Clone, Copy)]
enum ChannelKind {
    Queue,
    Stream,
}

/// Stream-specific attach options, captured so a stream is attached the same way
/// whether it matched at the start or appeared later.
#[derive(Clone)]
struct StreamAttachConfig {
    prefetch: u32,
    start: StreamStartChoice,
    filter: Vec<(String, String)>,
    durable_name: Option<String>,
}

/// Sending and receiving halves of a pattern subscription's merged stream, where
/// each item is a delivery tagged with its [`PatternSource`].
type MergedTx<T> = mpsc::Sender<(PatternSource, T)>;
type MergedRx<T> = mpsc::Receiver<(PatternSource, T)>;

type BoxFuture<O> = Pin<Box<dyn Future<Output = O> + Send>>;

/// Attach one matched channel: subscribe it and forward its messages into the
/// shared pattern stream. Boxed so the queue/stream and manual/auto variants all
/// drive the same orchestration core.
type AttachFn<T> =
    Arc<dyn Fn(Client, PatternSource, MergedTx<T>) -> BoxFuture<FibrilResult<()>> + Send + Sync>;

fn merged_channel<T>(prefetch: u32) -> (MergedTx<T>, MergedRx<T>) {
    mpsc::channel((prefetch as usize).max(1) * CHANNEL_FANIN_BUFFER)
}

/// Drive a pattern subscription: attach the matching channels now and reconcile
/// the attached set against the live catalogue on every change (attaching new
/// matches, forgetting vanished ones). Stops when the pattern stream is dropped.
async fn run_pattern<T: Send + 'static>(
    client: Client,
    glob: TopicGlob,
    kind: ChannelKind,
    out: MergedTx<T>,
    attach: AttachFn<T>,
) {
    let mut known: HashSet<(String, Option<String>)> = HashSet::new();
    reconcile(&client, &glob, kind, &mut known, &out, &attach).await;

    let mut events = client.catalogue_events();
    loop {
        // The feed is lossy. A Lagged error just means the snapshot moved on, so
        // reconcile against the current catalogue either way.
        tokio::select! {
            _ = out.closed() => return,
            ev = events.recv() => match ev {
                Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
        reconcile(&client, &glob, kind, &mut known, &out, &attach).await;
    }
}

/// Attach channels that match but are not yet attached, and forget ones that
/// disappeared (so a later re-declare re-attaches). A per-channel attach failure
/// is left out of the known set so the next reconcile retries it.
async fn reconcile<T: Send + 'static>(
    client: &Client,
    glob: &TopicGlob,
    kind: ChannelKind,
    known: &mut HashSet<(String, Option<String>)>,
    out: &MergedTx<T>,
    attach: &AttachFn<T>,
) {
    let matching = matching_keys(&client.catalogue(), glob, kind);
    let live: HashSet<(String, Option<String>)> = matching.iter().cloned().collect();
    // Forget channels that vanished. Their forwarding tasks end on their own when
    // the broker retires them.
    known.retain(|key| live.contains(key));
    for (topic, group) in matching {
        let key = (topic.clone(), group.clone());
        if known.contains(&key) {
            continue;
        }
        let source = PatternSource { topic, group };
        if attach(client.clone(), source, out.clone()).await.is_ok() {
            known.insert(key);
        }
    }
}

/// The (topic, group) keys of every channel of `kind` whose topic matches.
fn matching_keys(
    catalogue: &Catalogue,
    glob: &TopicGlob,
    kind: ChannelKind,
) -> Vec<(String, Option<String>)> {
    match kind {
        ChannelKind::Queue => catalogue
            .queues
            .iter()
            .filter(|queue| glob.matches(&queue.topic))
            .map(|queue| (queue.topic.clone(), queue.group.clone()))
            .collect(),
        ChannelKind::Stream => catalogue
            .streams
            .iter()
            .filter(|stream| glob.matches(&stream.topic))
            .map(|stream| (stream.topic.clone(), None))
            .collect(),
    }
}

/// Spawn a task forwarding a single channel's manual-ack deliveries into the
/// shared pattern stream, tagged with their source. The task ends when the
/// channel retires or the pattern stream is dropped.
fn forward_manual(
    mut sub: crate::Subscription,
    source: PatternSource,
    out: MergedTx<InflightMessage>,
) {
    tokio::spawn(async move {
        while let Some(msg) = sub.recv().await {
            if out.send((source.clone(), msg)).await.is_err() {
                break;
            }
        }
    });
}

/// Auto-ack counterpart of [`forward_manual`].
fn forward_auto(
    mut sub: crate::AutoAckedSubscription,
    source: PatternSource,
    out: MergedTx<Message>,
) {
    tokio::spawn(async move {
        while let Some(msg) = sub.recv().await {
            if out.send((source.clone(), msg)).await.is_err() {
                break;
            }
        }
    });
}

async fn attach_queue_manual(
    client: Client,
    source: PatternSource,
    prefetch: u32,
    exclusive: bool,
    out: MergedTx<InflightMessage>,
) -> FibrilResult<()> {
    let mut builder = client.subscribe(&source.topic)?;
    if let Some(group) = &source.group {
        builder = builder.group(group)?;
    }
    builder = builder.prefetch(prefetch);
    if exclusive {
        builder = builder.exclusive();
    }
    forward_manual(builder.sub().await?, source, out);
    Ok(())
}

async fn attach_queue_auto(
    client: Client,
    source: PatternSource,
    prefetch: u32,
    exclusive: bool,
    out: MergedTx<Message>,
) -> FibrilResult<()> {
    let mut builder = client.subscribe(&source.topic)?;
    if let Some(group) = &source.group {
        builder = builder.group(group)?;
    }
    builder = builder.prefetch(prefetch);
    if exclusive {
        builder = builder.exclusive();
    }
    forward_auto(builder.sub_auto_ack().await?, source, out);
    Ok(())
}

fn stream_builder<'a>(
    client: &'a Client,
    topic: &str,
    config: &StreamAttachConfig,
) -> FibrilResult<crate::StreamSubscriptionBuilder<'a>> {
    let mut builder = client.stream(topic)?.prefetch(config.prefetch);
    builder = match config.start {
        StreamStartChoice::Latest => builder.from_latest(),
        StreamStartChoice::Earliest => builder.from_earliest(),
        StreamStartChoice::NBack(count) => builder.from_last(count),
        StreamStartChoice::ByTime(time_ms) => builder.from_time(time_ms),
    };
    for (header, pattern) in &config.filter {
        builder = builder.filter(header, pattern);
    }
    if let Some(name) = &config.durable_name {
        builder = builder.durable(name);
    }
    Ok(builder)
}

async fn attach_stream_manual(
    client: Client,
    source: PatternSource,
    config: StreamAttachConfig,
    out: MergedTx<InflightMessage>,
) -> FibrilResult<()> {
    let sub = stream_builder(&client, &source.topic, &config)?
        .sub()
        .await?;
    forward_manual(sub, source, out);
    Ok(())
}

async fn attach_stream_auto(
    client: Client,
    source: PatternSource,
    config: StreamAttachConfig,
    out: MergedTx<Message>,
) -> FibrilResult<()> {
    let sub = stream_builder(&client, &source.topic, &config)?
        .sub_auto_ack()
        .await?;
    forward_auto(sub, source, out);
    Ok(())
}

/// A `*`-wildcard topic matcher. Mirrors the broker's header-value matcher so the
/// discovery glob and the per-subscription filter share one grammar: split on
/// `*`, where each `*` matches any run of characters (including empty), with no
/// regex.
#[derive(Debug, Clone)]
struct TopicGlob {
    segments: Vec<String>,
}

impl TopicGlob {
    fn new(pattern: &str) -> Self {
        TopicGlob {
            segments: pattern.split('*').map(str::to_string).collect(),
        }
    }

    fn matches(&self, value: &str) -> bool {
        if self.segments.len() == 1 {
            return self.segments[0] == value;
        }
        let first = &self.segments[0];
        let last = &self.segments[self.segments.len() - 1];
        if !value.starts_with(first.as_str()) || !value.ends_with(last.as_str()) {
            return false;
        }
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

#[cfg(test)]
mod tests {
    use super::TopicGlob;

    #[test]
    fn glob_matches_prefix_suffix_and_middle() {
        assert!(TopicGlob::new("events.*").matches("events.click"));
        assert!(TopicGlob::new("events.*").matches("events."));
        assert!(!TopicGlob::new("events.*").matches("orders.new"));

        assert!(TopicGlob::new("*.dead").matches("orders.dead"));
        assert!(!TopicGlob::new("*.dead").matches("orders.live"));

        assert!(TopicGlob::new("a*c").matches("abc"));
        assert!(TopicGlob::new("a*c").matches("ac"));
        assert!(!TopicGlob::new("a*c").matches("ab"));

        // No wildcard is an exact match; "*" matches everything.
        assert!(TopicGlob::new("orders").matches("orders"));
        assert!(!TopicGlob::new("orders").matches("orders.new"));
        assert!(TopicGlob::new("*").matches("anything.at.all"));
    }
}
