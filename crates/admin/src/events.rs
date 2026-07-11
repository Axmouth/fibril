//! Server-sent events for the dashboard. One multiplexed stream per open page
//! pushes full snapshots of the data families that page displays, so the
//! browser stops polling and an idle dashboard costs the broker nothing: each
//! tick serializes a family once no matter how many pages watch it, and a tick
//! with no subscribers does no work at all.

use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use futures::Stream;
use tokio::sync::broadcast;

use crate::routes::check_auth;
use crate::server::AdminServer;

/// Push cadence. Matches the freshness the pages had when they polled.
pub const TICK_INTERVAL: Duration = Duration::from_secs(2);

/// A data family a page can subscribe to. Names double as the query-parameter
/// vocabulary and the keys inside the pushed bundle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Family {
    Overview,
    History,
    Attention,
    QueuesDebug,
    StreamsDebug,
    Streams,
    Connections,
    Subscriptions,
    Cohorts,
    GlobalDlq,
    Audit,
}

impl Family {
    pub const ALL: [Family; 11] = [
        Family::Overview,
        Family::History,
        Family::Attention,
        Family::QueuesDebug,
        Family::StreamsDebug,
        Family::Streams,
        Family::Connections,
        Family::Subscriptions,
        Family::Cohorts,
        Family::GlobalDlq,
        Family::Audit,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Family::Overview => "overview",
            Family::History => "history",
            Family::Attention => "attention",
            Family::QueuesDebug => "queues_debug",
            Family::StreamsDebug => "streams_debug",
            Family::Streams => "streams",
            Family::Connections => "connections",
            Family::Subscriptions => "subscriptions",
            Family::Cohorts => "cohorts",
            Family::GlobalDlq => "global_dlq",
            Family::Audit => "audit",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        Family::ALL.into_iter().find(|f| f.name() == name)
    }
}

/// The fan-out hub. Subscribers register the families they want; the tick
/// task serializes each wanted family once and broadcasts the shared strings.
pub struct EventBroadcaster {
    tx: broadcast::Sender<Arc<HashMap<Family, String>>>,
    wanted: Mutex<HashMap<u64, HashSet<Family>>>,
    next_id: AtomicU64,
}

impl Default for EventBroadcaster {
    fn default() -> Self {
        // A small buffer: a receiver that lags a few ticks just resyncs on the
        // next full snapshot, so there is nothing worth queueing deeply.
        let (tx, _) = broadcast::channel(4);
        Self {
            tx,
            wanted: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(0),
        }
    }
}

impl EventBroadcaster {
    fn register(
        &self,
        families: HashSet<Family>,
    ) -> (u64, broadcast::Receiver<Arc<HashMap<Family, String>>>) {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.wanted
            .lock()
            .expect("event subscriber registry poisoned")
            .insert(id, families);
        (id, self.tx.subscribe())
    }

    fn deregister(&self, id: u64) {
        self.wanted
            .lock()
            .expect("event subscriber registry poisoned")
            .remove(&id);
    }

    /// Families at least one live subscriber wants. Empty means idle.
    fn union(&self) -> HashSet<Family> {
        self.wanted
            .lock()
            .expect("event subscriber registry poisoned")
            .values()
            .flatten()
            .copied()
            .collect()
    }

    /// One tick: produce every family somebody wants and broadcast the shared
    /// serializations. Public as a seam so tests drive ticks deterministically.
    pub async fn tick_once(&self, server: &AdminServer) {
        let union = self.union();
        if union.is_empty() {
            return;
        }
        let mut map = HashMap::with_capacity(union.len());
        for family in union {
            map.insert(family, family_payload(server, family).await.to_string());
        }
        // Send failure just means every receiver vanished mid-tick.
        let _ = self.tx.send(Arc::new(map));
    }
}

/// Produce one family's payload, byte-identical in shape to its GET route.
async fn family_payload(server: &AdminServer, family: Family) -> serde_json::Value {
    match family {
        Family::Overview => {
            serde_json::to_value(crate::routes::overview_payload(server).await).unwrap_or_default()
        }
        Family::History => {
            serde_json::to_value(crate::history::history_payload(server)).unwrap_or_default()
        }
        Family::Attention => {
            serde_json::to_value(crate::attention::attention_payload(server).await)
                .unwrap_or_default()
        }
        Family::QueuesDebug => crate::routes::partition_debug_value(server, false)
            .await
            .unwrap_or(serde_json::Value::Null),
        Family::StreamsDebug => crate::routes::partition_debug_value(server, true)
            .await
            .unwrap_or(serde_json::Value::Null),
        Family::Streams => match &server.streams {
            Some(streams) => {
                let stats = streams.stream_stats().await;
                serde_json::json!({ "streams": serde_json::to_value(stats).unwrap_or_default() })
            }
            None => serde_json::json!({ "streams": [] }),
        },
        Family::Connections => server.metrics.connections().snapshot(),
        Family::Subscriptions => server.metrics.connections().snapshot_subs(),
        Family::Cohorts => {
            let cohorts = match &server.cohorts {
                Some(provider) => provider(),
                None => serde_json::Value::Null,
            };
            serde_json::json!({ "cohorts": cohorts })
        }
        Family::GlobalDlq => match server.storage.global_dlq().await {
            Ok(snapshot) => serde_json::to_value(crate::routes::GlobalDlqView::from(snapshot))
                .unwrap_or_default(),
            Err(_) => serde_json::Value::Null,
        },
        Family::Audit => {
            serde_json::to_value(crate::audit::audit_payload(server)).unwrap_or_default()
        }
    }
}

/// Assemble one subscriber's bundle from the tick's shared per-family strings,
/// in the stable `Family::ALL` order. Pure string concatenation: the families
/// were serialized once for everyone.
fn assemble_bundle(map: &HashMap<Family, String>, families: &HashSet<Family>) -> Option<String> {
    let mut out = String::from("{");
    let mut any = false;
    for family in Family::ALL {
        if !families.contains(&family) {
            continue;
        }
        let Some(payload) = map.get(&family) else {
            continue;
        };
        if any {
            out.push(',');
        }
        out.push('"');
        out.push_str(family.name());
        out.push_str("\":");
        out.push_str(payload);
        any = true;
    }
    out.push('}');
    any.then_some(out)
}

/// Deregisters the subscription when the SSE stream is dropped (client gone).
struct SubscriberGuard {
    server: Arc<AdminServer>,
    id: u64,
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        self.server.events.deregister(self.id);
    }
}

#[derive(serde::Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    pub families: String,
}

/// `GET /admin/api/events?families=a,b,c`: the page's live data stream. Emits
/// a named `tick` event whose data is a JSON object with one key per requested
/// family, first immediately (so the page renders on arrival), then on the
/// broadcaster's cadence.
pub async fn events(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    check_auth(&server, &headers).await?;

    let mut families = HashSet::new();
    for name in query
        .families
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        match Family::parse(name) {
            Some(family) => {
                families.insert(family);
            }
            None => return Err(StatusCode::BAD_REQUEST),
        }
    }
    if families.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    // First bundle inline so the page paints on arrival instead of waiting
    // out a tick.
    let mut first = HashMap::with_capacity(families.len());
    for family in &families {
        first.insert(
            *family,
            family_payload(&server, *family).await.to_string(),
        );
    }
    let first = assemble_bundle(&first, &families);

    let (id, rx) = server.events.register(families.clone());
    let guard = SubscriberGuard {
        server: server.clone(),
        id,
    };

    struct StreamState {
        first: Option<String>,
        rx: broadcast::Receiver<Arc<HashMap<Family, String>>>,
        families: HashSet<Family>,
        _guard: SubscriberGuard,
    }

    let state = StreamState {
        first,
        rx,
        families,
        _guard: guard,
    };

    let stream = futures::stream::unfold(state, |mut state| async move {
        if let Some(bundle) = state.first.take() {
            return Some((Ok(Event::default().event("tick").data(bundle)), state));
        }
        loop {
            match state.rx.recv().await {
                Ok(map) => {
                    let Some(bundle) = assemble_bundle(&map, &state.families) else {
                        continue;
                    };
                    return Some((Ok(Event::default().event("tick").data(bundle)), state));
                }
                // Lagged just means we missed some full snapshots; the next
                // one resyncs us.
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => return None,
            }
        }
    });

    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Drive the broadcaster forever. Spawned next to the history sampler.
pub async fn run_broadcaster(server: Arc<AdminServer>) {
    let mut tick = tokio::time::interval(TICK_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        server.events.tick_once(&server).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn family_names_round_trip() {
        for family in Family::ALL {
            assert_eq!(Family::parse(family.name()), Some(family));
        }
        assert_eq!(Family::parse("nope"), None);
    }

    #[test]
    fn bundle_holds_only_requested_families_in_stable_order() {
        let mut map = HashMap::new();
        map.insert(Family::Overview, "{\"a\":1}".to_string());
        map.insert(Family::History, "{\"b\":2}".to_string());
        map.insert(Family::Attention, "{\"c\":3}".to_string());

        let families: HashSet<Family> =
            [Family::History, Family::Overview].into_iter().collect();
        let bundle = assemble_bundle(&map, &families).expect("two families present");
        assert_eq!(bundle, "{\"overview\":{\"a\":1},\"history\":{\"b\":2}}");

        let missing: HashSet<Family> = [Family::Cohorts].into_iter().collect();
        assert_eq!(assemble_bundle(&map, &missing), None);
    }

    #[test]
    fn union_tracks_registrations() {
        let hub = EventBroadcaster::default();
        assert!(hub.union().is_empty());
        let (id_a, _rx_a) = hub.register([Family::Overview].into_iter().collect());
        let (id_b, _rx_b) =
            hub.register([Family::Overview, Family::History].into_iter().collect());
        assert_eq!(hub.union().len(), 2);
        hub.deregister(id_a);
        assert_eq!(hub.union().len(), 2);
        hub.deregister(id_b);
        assert!(hub.union().is_empty());
    }
}
