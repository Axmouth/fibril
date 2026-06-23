//! Client-side clustering / failover behavior.
//!
//! The retry/redirect classification a caller uses to decide whether to re-issue
//! an operation, the confirmed-publish failover retry state, and the subscription
//! supervisor that migrates a partition stream across an owner failover. Split out
//! of lib.rs (clustering-module separation). Re-exported from the crate root so
//! `fibril_client::RetryAdvice` etc. keep resolving.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;

use fibril_protocol::v1::{ERR_INVALID, ERR_NOT_FOUND, ERR_NOT_OWNER, Partition, Subscribe, SubscribeStream};

use crate::{
    Client, FibrilError, FibrilResult, InflightMessage, Message, subscribe_partition_auto,
    subscribe_partition_manual, subscribe_stream_partition_auto, subscribe_stream_partition_manual,
};

/// A subscribe request the supervisor can migrate across an owner failover. Both
/// the queue [`Subscribe`] and the stream [`SubscribeStream`] expose the routing
/// key (topic, partition, group) the supervisor needs to track ownership.
pub(crate) trait SupervisedReq: Clone + Send + 'static {
    fn topic(&self) -> &str;
    fn partition(&self) -> Partition;
    fn group(&self) -> Option<&str>;
}

impl SupervisedReq for Subscribe {
    fn topic(&self) -> &str {
        &self.topic
    }
    fn partition(&self) -> Partition {
        self.partition
    }
    fn group(&self) -> Option<&str> {
        self.group.as_deref()
    }
}

impl SupervisedReq for SubscribeStream {
    fn topic(&self) -> &str {
        &self.topic
    }
    fn partition(&self) -> Partition {
        self.partition
    }
    fn group(&self) -> Option<&str> {
        None // streams have no group
    }
}

/// Whether and how a caller should retry a failed operation. Get it from
/// [`FibrilError::retry_advice`] (or the [`FibrilError::is_retryable`] shortcut).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryAdvice {
    /// Retry: a transient condition - the broker was briefly unreachable, an owner
    /// is failing over, or the request hit a topology/ownership conflict. Retrying
    /// (against a refreshed owner) is expected to succeed once the cluster settles.
    ///
    /// IMPORTANT for confirmed publishes: a retryable error means the outcome is
    /// UNKNOWN. The broker may already have made the write durable but the confirm
    /// did not get back to you. Re-publishing is therefore at-least-once and can
    /// create a DUPLICATE (and, mid-window, a reorder). Tolerate duplicates, use an
    /// idempotent producer (planned: producer-id + sequence + broker dedup), or
    /// treat the publish as best-effort. The client already auto-retries the
    /// transport-level cases up to `publish_timeout_ms`, so an error you actually
    /// receive has usually outlived that budget.
    Retry,
    /// Do not retry: a request/permanent error retrying will not fix - an unknown
    /// topic/partition, an invalid argument, a bad name, or a payload that failed
    /// to (de)serialize. Fix the request instead.
    DoNotRetry,
}

impl FibrilError {
    /// Whether this error is a transient transport failure (connect/severed
    /// connection). Narrow on purpose: this is the subset the client retries
    /// automatically against a refreshed owner. For the caller-facing "should I
    /// retry?" question use [`is_retryable`](Self::is_retryable), which also covers
    /// topology conflicts and server-transient (5xx) errors.
    pub(crate) fn is_transient(&self) -> bool {
        matches!(
            self,
            FibrilError::Disconnection { .. } | FibrilError::BrokenPipe | FibrilError::Eof
        )
    }

    /// How a caller should treat this error. The intuitive way to decide whether
    /// to re-issue an operation - see [`RetryAdvice`] (note the duplicate caveat
    /// for confirmed publishes).
    pub fn retry_advice(&self) -> RetryAdvice {
        match self {
            // Transport / control-flow: the owner was unreachable or moved.
            FibrilError::Disconnection { .. }
            | FibrilError::BrokenPipe
            | FibrilError::Eof
            | FibrilError::Redirect(_) => RetryAdvice::Retry,
            FibrilError::Failure { code, .. } => match *code {
                // 409 conflict / not-owner: topology moved, a retry re-routes.
                ERR_NOT_OWNER => RetryAdvice::Retry,
                // 404 not-found / 400 invalid: the caller must fix the request.
                ERR_NOT_FOUND | ERR_INVALID => RetryAdvice::DoNotRetry,
                // Server-side (5xx) is transient; other 4xx are caller errors.
                code if code >= 500 => RetryAdvice::Retry,
                _ => RetryAdvice::DoNotRetry,
            },
            // Local request errors: retrying as-is cannot help.
            FibrilError::DeserializationFailure { .. }
            | FibrilError::SerializationFailure { .. }
            | FibrilError::InvalidName { .. }
            | FibrilError::Unexpected { .. } => RetryAdvice::DoNotRetry,
        }
    }

    /// `true` when [`retry_advice`](Self::retry_advice) is [`RetryAdvice::Retry`].
    /// The simple "should I retry this?" check (mind the duplicate caveat for
    /// confirmed publishes - see [`RetryAdvice::Retry`]).
    pub fn is_retryable(&self) -> bool {
        matches!(self.retry_advice(), RetryAdvice::Retry)
    }
}

// TODO(config-knobs, expert tier): the three failover-retry constants below are
// hardcoded for now. Promote them to `ClientOptions` knobs in the later settings
// pass (client-side tunables, expert tier per the settings-tiering follow-up in
// FOLLOWUPS.md), alongside the existing `publish_timeout_ms`. Tracked:
//   - PUBLISH_RETRY_INITIAL_BACKOFF_MS / PUBLISH_RETRY_MAX_BACKOFF_MS
//   - SUBSCRIPTION_OWNER_CHECK_MS

/// Confirmed-publish retry backoff bounds (transient owner-failover retries).
pub(crate) const PUBLISH_RETRY_INITIAL_BACKOFF_MS: u64 = 10;
pub(crate) const PUBLISH_RETRY_MAX_BACKOFF_MS: u64 = 500;

/// How often a supervised subscription re-checks the topology owner to detect a
/// failover (the engine keeps the stream alive across reconnects to the same
/// endpoint, so an owner *move* is not seen via the stream - only via topology).
const SUBSCRIPTION_OWNER_CHECK_MS: u64 = 1_000;

/// One backoff nap: `base_ms` plus up to `base_ms` of jitter, so many publishers
/// retrying a shared failover do not resynchronize into retry storms.
pub(crate) fn publish_retry_nap(base_ms: u64) -> Duration {
    let span = base_ms.max(1);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0)
        % span;
    Duration::from_millis(base_ms + jitter)
}

/// Per-call state for the confirmed-publish failover retry loop.
pub(crate) struct PublishRetryState {
    /// Give-up time for transient retries; `None` when retry is disabled
    /// (`publish_timeout_ms == 0`) so the first transient error fails fast.
    pub(crate) deadline: Option<Instant>,
    pub(crate) redirects: u32,
    pub(crate) backoff_ms: u64,
}

impl PublishRetryState {
    pub(crate) fn new(publish_timeout_ms: u64) -> Self {
        Self {
            deadline: (publish_timeout_ms > 0)
                .then(|| Instant::now() + Duration::from_millis(publish_timeout_ms)),
            redirects: 0,
            backoff_ms: PUBLISH_RETRY_INITIAL_BACKOFF_MS,
        }
    }
}

/// Forward one partition's stream into the merged subscription channel. Ends when
/// the partition stream closes (e.g. the broker retired it during a shrink) or
/// the subscription is dropped.
/// Boxed re-subscribe future for [`supervise_forward`].
type ResubscribeFut<M> =
    std::pin::Pin<Box<dyn std::future::Future<Output = FibrilResult<mpsc::Receiver<M>>> + Send>>;

/// Forward one partition's stream into the fan-in channel, supervising it across
/// an owner failover. When the per-partition stream ends unexpectedly (the owner
/// died), this re-resolves the owner and re-subscribes to the new one, then
/// resumes forwarding - the read-side mirror of the producer publish retry, so a
/// consumer rides through a failover instead of silently losing that partition.
///
/// Stops only when: the consumer drops the subscription (the fan-in sender
/// closes), the client is shutting down, the topic is gone (a refreshed,
/// populated topology no longer knows it), or re-subscribe fails permanently.
/// Unlike the producer there is no fixed deadline: a subscription is long-lived,
/// so it retries (with backoff) for as long as the consumer keeps it open.
fn supervise_forward<R, M, F>(
    client: Client,
    req: R,
    mut part_rx: mpsc::Receiver<M>,
    tx: mpsc::Sender<M>,
    resubscribe: F,
) where
    R: SupervisedReq,
    M: Send + 'static,
    F: Fn(Client, R) -> ResubscribeFut<M> + Send + 'static,
{
    tokio::spawn(async move {
        let check = Duration::from_millis(SUBSCRIPTION_OWNER_CHECK_MS);
        let owner_of = |client: &Client, req: &R| {
            client
                .shared
                .topology
                .load()
                .lookup(req.topic(), req.partition(), req.group())
                .map(|entry| entry.endpoint)
        };
        loop {
            // The owner this stream is bound to. The engine keeps a subscription
            // alive across reconnects to the SAME endpoint (good for blips), so a
            // failover (owner moves to a new node) does NOT close this stream -
            // we detect it by watching the topology owner instead, and migrate
            // once the controller commits the new owner.
            let bound_owner = owner_of(&client, &req);

            // Forward until the stream ends, the consumer leaves, or the owner moves.
            let migrate = loop {
                tokio::select! {
                    msg = part_rx.recv() => match msg {
                        Some(msg) => {
                            if tx.send(msg).await.is_err() {
                                return; // consumer dropped the subscription
                            }
                        }
                        None => break true, // stream closed (owner gone / engine gave up)
                    },
                    _ = tokio::time::sleep(check) => {
                        if tx.is_closed()
                            || client.shared.user_shutdown.load(Ordering::Acquire)
                        {
                            return;
                        }
                        // Refresh (throttled) so the cache reflects a committed
                        // reassignment, then migrate if the owner endpoint changed.
                        client.shared.refresh_topology_throttled().await;
                        let current = owner_of(&client, &req);
                        if current.is_some() && current != bound_owner {
                            tracing::debug!(?bound_owner, ?current, "subscription owner moved, migrating");
                            break true;
                        }
                        // The owner endpoint is unchanged, but its connection may
                        // have dropped (an owner restart-in-place: a bounce faster
                        // than failover, so ownership stays put). Nothing else
                        // reconnects a passive subscription's connection, so re-dial
                        // it here. The reconnect reconciles active subscriptions,
                        // re-establishing this stream (or closing it so we
                        // re-subscribe). Cheap when the connection is healthy.
                        if let Some(owner_addr) = current {
                            match client.shared.engine_slot(owner_addr).await {
                                Ok(slot) => {
                                    if let Err(e) = slot.engine_for_operation().await {
                                        tracing::debug!(%owner_addr, error = ?e, "subscription owner reconnect failed, will retry");
                                    }
                                }
                                Err(e) => {
                                    tracing::debug!(%owner_addr, error = ?e, "subscription owner slot unavailable");
                                }
                            }
                        }
                    }
                }
            };
            if !migrate {
                return;
            }
            if tx.is_closed() || client.shared.user_shutdown.load(Ordering::Acquire) {
                return;
            }

            // Re-resolve and re-subscribe to the new owner, backing off until it
            // comes up, the consumer leaves, or the topic is gone.
            let mut backoff_ms = PUBLISH_RETRY_INITIAL_BACKOFF_MS;
            loop {
                if tx.is_closed() || client.shared.user_shutdown.load(Ordering::Acquire) {
                    return;
                }
                if client.shared.refresh_topology_throttled().await {
                    let topo = client.shared.topology.load();
                    if topo.is_populated() && !topo.knows_topic(req.topic(), req.group()) {
                        return; // topic deleted - stop re-subscribing
                    }
                }
                match resubscribe(client.clone(), req.clone()).await {
                    Ok(new_rx) => {
                        part_rx = new_rx;
                        break;
                    }
                    Err(err) if err.is_transient() => {
                        tokio::time::sleep(publish_retry_nap(backoff_ms)).await;
                        backoff_ms = (backoff_ms * 2).min(PUBLISH_RETRY_MAX_BACKOFF_MS);
                    }
                    Err(_) => return, // permanent (e.g. not-found / max redirects)
                }
            }
        }
    });
}

/// Supervised forward for a manual-ack partition stream.
pub(crate) fn supervise_forward_manual(
    client: Client,
    req: Subscribe,
    part_rx: mpsc::Receiver<InflightMessage>,
    tx: mpsc::Sender<InflightMessage>,
) {
    supervise_forward(client, req, part_rx, tx, |client, req| {
        Box::pin(async move { subscribe_partition_manual(&client, req).await })
    });
}

/// Supervised forward for an auto-ack partition stream.
pub(crate) fn supervise_forward_auto(
    client: Client,
    req: Subscribe,
    part_rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Message>,
) {
    supervise_forward(client, req, part_rx, tx, |client, req| {
        Box::pin(async move { subscribe_partition_auto(&client, req).await })
    });
}

/// Supervised forward for a manual-ack Plexus stream partition.
pub(crate) fn supervise_forward_stream_manual(
    client: Client,
    req: SubscribeStream,
    part_rx: mpsc::Receiver<InflightMessage>,
    tx: mpsc::Sender<InflightMessage>,
) {
    supervise_forward(client, req, part_rx, tx, |client, req| {
        Box::pin(async move { subscribe_stream_partition_manual(&client, req).await })
    });
}

/// Supervised forward for an auto-ack Plexus stream partition.
pub(crate) fn supervise_forward_stream_auto(
    client: Client,
    req: SubscribeStream,
    part_rx: mpsc::Receiver<Message>,
    tx: mpsc::Sender<Message>,
) {
    supervise_forward(client, req, part_rx, tx, |client, req| {
        Box::pin(async move { subscribe_stream_partition_auto(&client, req).await })
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_is_transport_subset() {
        // is_transient is the narrow transport subset the client auto-retries.
        assert!(FibrilError::BrokenPipe.is_transient());
        assert!(FibrilError::Eof.is_transient());
        assert!(
            FibrilError::Disconnection {
                msg: "connection refused".into()
            }
            .is_transient()
        );
        // A topology conflict is retryable to the caller, but not "transient
        // transport" (the client handles it via redirect, not the retry loop).
        assert!(
            !FibrilError::Failure {
                code: ERR_NOT_OWNER,
                msg: "no owner".into()
            }
            .is_transient()
        );
    }

    #[test]
    fn retry_advice_classifies_intuitively() {
        use RetryAdvice::*;
        let retry = |e: FibrilError| assert_eq!(e.retry_advice(), Retry, "{e:?}");
        let no = |e: FibrilError| assert_eq!(e.retry_advice(), DoNotRetry, "{e:?}");

        // Retry: transport, redirect, topology conflict, server-transient.
        retry(FibrilError::BrokenPipe);
        retry(FibrilError::Eof);
        retry(FibrilError::Disconnection {
            msg: "refused".into(),
        });
        retry(FibrilError::Failure {
            code: ERR_NOT_OWNER,
            msg: "moved".into(),
        });
        retry(FibrilError::Failure {
            code: 503,
            msg: "server busy".into(),
        });

        // Do not retry: gone, invalid, and local request errors.
        no(FibrilError::Failure {
            code: ERR_NOT_FOUND,
            msg: "no topic".into(),
        });
        no(FibrilError::Failure {
            code: ERR_INVALID,
            msg: "bad arg".into(),
        });
        no(FibrilError::SerializationFailure {
            msg: "bad payload".into(),
        });
        no(FibrilError::InvalidName {
            kind: "topic",
            name: "BAD".into(),
            msg: "uppercase".into(),
        });

        // is_retryable mirrors retry_advice == Retry.
        assert!(FibrilError::BrokenPipe.is_retryable());
        assert!(
            !FibrilError::Failure {
                code: ERR_NOT_FOUND,
                msg: "x".into()
            }
            .is_retryable()
        );
    }

    #[test]
    fn publish_retry_nap_stays_within_base_and_double() {
        for base in [1u64, 10, 100, 500] {
            for _ in 0..50 {
                let nap = publish_retry_nap(base).as_millis() as u64;
                assert!(nap >= base, "nap {nap} below base {base}");
                assert!(nap < base * 2, "nap {nap} >= 2x base {base}");
            }
        }
    }
}
