//! Routing / discovery surface, opt in via [`Client::routing`].
//!
//! Built entirely on the public client API (the subscribe builder, the catalogue
//! feed, and `Clone`), so it adds no coupling to the connection core and composes
//! with the delivery-guarantee wrappers: a pattern subscription yields the same
//! [`InflightMessage`] every subscription does, and reliable publishing is still
//! reached through the derefed [`Client`].

use std::collections::HashSet;

use tokio::sync::{broadcast, mpsc};

use crate::{Catalogue, Client, FibrilResult, InflightMessage};

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
    /// sharing this connection; the plain client stays usable.
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
}

/// The channel a pattern-delivered message came from. A pattern fans in across
/// many channels, so the message is paired with its source for routing back to
/// per-topic handling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternSource {
    pub topic: String,
    /// The queue's group namespace, or `None` for the ungrouped default.
    pub group: Option<String>,
}

/// Builder for a [`RoutingClient::subscribe_pattern`] subscription.
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

    /// Start the subscription with manual acknowledgement. Each delivered
    /// [`InflightMessage`] must be settled, exactly as for a single-channel
    /// subscription. Returns immediately with the live fan-in; channels that
    /// start matching later attach on their own.
    pub async fn sub(self) -> FibrilResult<PatternSubscription> {
        let PatternSubscribeBuilder {
            client,
            glob,
            prefetch,
            exclusive,
        } = self;

        let cap = (prefetch as usize).max(1) * CHANNEL_FANIN_BUFFER;
        let (out, rx) = mpsc::channel(cap);

        // Attach the channels that already match. A per-channel failure is left
        // out of the known set so the watcher retries it on the next catalogue
        // change rather than the whole pattern failing.
        let mut known: HashSet<(String, Option<String>)> = HashSet::new();
        for (topic, group) in matching_queue_keys(&client.catalogue(), &glob) {
            if attach_channel(&client, &topic, group.as_deref(), prefetch, exclusive, &out)
                .await
                .is_ok()
            {
                known.insert((topic, group));
            }
        }

        tokio::spawn(watch_catalogue(client, glob, prefetch, exclusive, known, out));
        Ok(PatternSubscription { rx })
    }
}

/// A live fan-in over every channel matching a glob, with auto-pickup of channels
/// that start matching later. Each item carries its [`PatternSource`] so the
/// caller can route by origin; the [`InflightMessage`] is settled exactly as for
/// a single-channel subscription. Dropping the subscription stops every attached
/// channel and the catalogue watcher.
pub struct PatternSubscription {
    rx: mpsc::Receiver<(PatternSource, InflightMessage)>,
}

impl PatternSubscription {
    /// Receive the next message and the channel it came from. Returns `None` when
    /// the subscription is closed.
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

/// Subscribe to one matched queue and forward its messages into the shared
/// pattern stream, tagged with their source. Reuses the full single-channel
/// subscription (per-partition fan-in, owner failover, live partition grow), so
/// this layer only adds the cross-channel merge. The forwarding task ends when
/// the channel retires or the pattern stream is dropped.
async fn attach_channel(
    client: &Client,
    topic: &str,
    group: Option<&str>,
    prefetch: u32,
    exclusive: bool,
    out: &mpsc::Sender<(PatternSource, InflightMessage)>,
) -> FibrilResult<()> {
    let mut builder = client.subscribe(topic)?;
    if let Some(group) = group {
        builder = builder.group(group)?;
    }
    builder = builder.prefetch(prefetch);
    if exclusive {
        builder = builder.exclusive();
    }
    let mut sub = builder.sub().await?;
    let source = PatternSource {
        topic: topic.to_string(),
        group: group.map(str::to_string),
    };
    let out = out.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.recv().await {
            if out.send((source.clone(), msg)).await.is_err() {
                break;
            }
        }
    });
    Ok(())
}

/// Keep the pattern's attached set reconciled with the live catalogue: attach
/// channels that started matching and forget ones that disappeared (so a later
/// re-declare re-attaches). Stops when the pattern stream is dropped.
async fn watch_catalogue(
    client: Client,
    glob: TopicGlob,
    prefetch: u32,
    exclusive: bool,
    mut known: HashSet<(String, Option<String>)>,
    out: mpsc::Sender<(PatternSource, InflightMessage)>,
) {
    let mut events = client.catalogue_events();
    loop {
        // The feed is lossy, so a Lagged error just means the snapshot moved on;
        // reconcile against the current catalogue in that case rather than waiting
        // for the next change.
        let catalogue = tokio::select! {
            _ = out.closed() => return,
            ev = events.recv() => match ev {
                Ok(catalogue) => catalogue,
                Err(broadcast::error::RecvError::Lagged(_)) => client.catalogue(),
                Err(broadcast::error::RecvError::Closed) => return,
            }
        };

        let matching = matching_queue_keys(&catalogue, &glob);
        let live: HashSet<(String, Option<String>)> = matching.iter().cloned().collect();
        // Forget channels that vanished. Their forwarding tasks end on their own
        // when the broker retires them.
        known.retain(|key| live.contains(key));
        for (topic, group) in matching {
            let key = (topic.clone(), group.clone());
            if known.contains(&key) {
                continue;
            }
            if attach_channel(&client, &topic, group.as_deref(), prefetch, exclusive, &out)
                .await
                .is_ok()
            {
                known.insert(key);
            }
        }
    }
}

/// The (topic, group) keys of every queue in the catalogue whose topic matches.
fn matching_queue_keys(catalogue: &Catalogue, glob: &TopicGlob) -> Vec<(String, Option<String>)> {
    catalogue
        .queues
        .iter()
        .filter(|queue| glob.matches(&queue.topic))
        .map(|queue| (queue.topic.clone(), queue.group.clone()))
        .collect()
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
