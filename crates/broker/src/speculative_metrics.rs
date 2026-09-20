//! Bounded, sampled diagnostics for the isolated experiment. Times are broker-local.
use super::*;
use hdrhistogram::Histogram;
use std::time::Instant;
const LIMIT: usize = 8192;
const MAX_US: u64 = 120_000_000;

#[derive(Debug, Clone, Copy)]
pub(super) enum Fallback {
    StoragePolicy,
    DispatchBusy,
    ByteBudget,
    ItemBudget,
    QueuePolicy,
    NoConsumer,
    NoCredit,
    ChannelUnavailable,
    OlderUndispatched,
    PartialCredit,
    Retired,
}
const REASONS: [&str; 11] = [
    "storage_policy",
    "dispatch_busy",
    "byte_budget",
    "item_budget",
    "queue_policy",
    "no_consumer",
    "no_credit",
    "channel_unavailable",
    "older_undispatched",
    "partial_credit",
    "retired",
];
#[derive(Debug)]
pub(super) struct Telemetry {
    pub cheap: bool,
    pub handoff: bool,
    pub handoff_attempts: AtomicU64,
    pub handoff_acquired: AtomicU64,
    enabled: bool,
    sequence: AtomicU64,
    fallback: [AtomicU64; 11],
    pub copied_bytes: AtomicU64,
    evicted: AtomicU64,
    clipped: AtomicU64,
    pending: Mutex<HashMap<String, Arc<Trace>>>,
    hist: Mutex<HashMap<String, Histogram<u64>>>,
}
impl Telemetry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            handoff: std::env::var("FIBRIL_EXPERIMENTAL_SPEC_HANDOFF").as_deref() == Ok("1"),
            handoff_attempts: AtomicU64::new(0),
            handoff_acquired: AtomicU64::new(0),
            cheap: std::env::var("FIBRIL_EXPERIMENTAL_SPEC_ADMISSION").as_deref() == Ok("cheap"),
            enabled: std::env::var("FIBRIL_EXPERIMENTAL_SPEC_TRACE").as_deref() == Ok("1"),
            sequence: AtomicU64::new(0),
            fallback: std::array::from_fn(|_| AtomicU64::new(0)),
            copied_bytes: AtomicU64::new(0),
            evicted: AtomicU64::new(0),
            clipped: AtomicU64::new(0),
            pending: Mutex::new(HashMap::new()),
            hist: Mutex::new(HashMap::new()),
        })
    }
    pub fn fallback(&self, reason: Fallback, n: usize) {
        self.fallback[reason as usize].fetch_add(n as u64, Ordering::Relaxed);
    }
    pub fn sample(self: &Arc<Self>, id: &str, start: Instant) -> Option<Arc<Trace>> {
        if !self.enabled || self.sequence.fetch_add(1, Ordering::Relaxed) % 64 != 0 {
            return None;
        }
        let trace = Arc::new(Trace {
            start,
            collector: Arc::downgrade(self),
            early: AtomicBool::new(false),
            stage: AtomicU64::new(0),
            event: AtomicU64::new(0),
            dispatch: AtomicU64::new(0),
            id: id.to_owned(),
        });
        let mut pending = self.pending.lock().unwrap();
        if pending.len() >= LIMIT {
            // Diagnostics cannot retain unbounded expired/undelivered messages.
            if let Some(key) = pending.keys().next().cloned() {
                pending.remove(&key);
            }
            self.evicted.fetch_add(1, Ordering::Relaxed);
        }
        pending.insert(id.to_owned(), trace.clone());
        Some(trace)
    }
    pub fn take(&self, id: &str) -> Option<Arc<Trace>> {
        if !self.enabled {
            return None;
        }
        self.pending.lock().unwrap().remove(id)
    }
    fn record(&self, name: &str, us: u64) {
        if us > MAX_US {
            self.clipped.fetch_add(1, Ordering::Relaxed);
        }
        let mut hist = self.hist.lock().unwrap();
        let h = hist
            .entry(name.to_owned())
            .or_insert_with(|| Histogram::new_with_bounds(1, MAX_US, 2).unwrap());
        h.record(us.clamp(1, MAX_US)).unwrap();
    }
    pub fn snapshot(&self) -> serde_json::Value {
        let reasons: serde_json::Map<String, serde_json::Value> = REASONS
            .iter()
            .enumerate()
            .map(|(i, n)| ((*n).into(), self.fallback[i].load(Ordering::Relaxed).into()))
            .collect();
        let hist: serde_json::Map<String, serde_json::Value> = self
            .hist
            .lock()
            .unwrap()
            .iter()
            .map(|(n, h)| {
                (
                    n.clone(),
                    serde_json::json!({"n":h.len(),"p50_us":h.value_at_quantile(0.5),
                "p99_us":h.value_at_quantile(0.99),"max_us":h.max()}),
                )
            })
            .collect();
        serde_json::json!({"handoff":self.handoff,"handoff_attempts":self.handoff_attempts.load(Ordering::Relaxed),
            "handoff_acquired":self.handoff_acquired.load(Ordering::Relaxed),"sample_every":64,"trace_enabled":self.enabled,"cheap":self.cheap,
            "fallback":reasons,"histograms":hist,"pending_samples":self.pending.lock().unwrap().len(),
            "evicted_samples":self.evicted.load(Ordering::Relaxed),"clipped_samples":self.clipped.load(Ordering::Relaxed),
            "copied_payload_bytes":self.copied_bytes.load(Ordering::Relaxed)})
    }
}
#[derive(Debug)]
pub(super) struct Trace {
    start: Instant,
    collector: std::sync::Weak<Telemetry>,
    early: AtomicBool,
    stage: AtomicU64,
    event: AtomicU64,
    dispatch: AtomicU64,
    id: String,
}
impl Trace {
    fn elapsed(&self) -> u64 {
        self.start.elapsed().as_micros().min(u64::MAX as u128 - 1) as u64 + 1
    }
    pub fn stage(&self) {
        self.stage.store(self.elapsed(), Ordering::Relaxed);
    }
    pub fn event(&self) {
        self.event.store(self.elapsed(), Ordering::Relaxed);
    }
    pub fn dispatch(&self, early: bool) {
        let now = self.elapsed();
        if self
            .dispatch
            .compare_exchange(0, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        self.early.store(early, Ordering::Relaxed);
        if let Some(c) = self.collector.upgrade() {
            let path = if early { "early" } else { "fallback" };
            c.record(&format!("{path}.admission_to_dispatch"), now);
            let stage = self.stage.load(Ordering::Relaxed);
            let event = self.event.load(Ordering::Relaxed);
            if stage > 0 {
                c.record(&format!("{path}.admission_to_staged"), stage);
            }
            if event >= stage && stage > 0 {
                c.record(&format!("{path}.staged_to_event_submit"), event - stage);
            }
            if event > 0 {
                c.record(
                    &format!("{path}.event_submit_to_dispatch"),
                    now.saturating_sub(event),
                );
            }
        }
    }
    pub fn record(&self, phase: &str) {
        if let Some(c) = self.collector.upgrade() {
            let path = if self.early.load(Ordering::Relaxed) {
                "early"
            } else {
                "fallback"
            };
            c.record(&format!("{path}.admission_to_{phase}"), self.elapsed());
        }
    }
    pub fn fail(&self) {
        if let Some(c) = self.collector.upgrade() {
            c.take(&self.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn collector() -> Arc<Telemetry> {
        let mut c = Telemetry::new();
        Arc::get_mut(&mut c).unwrap().enabled = true;
        c
    }
    #[test]
    fn undelivered_samples_are_bounded_and_do_not_form_cycles() {
        let c = collector();
        let weak = Arc::downgrade(&c);
        for i in 0..LIMIT + 17 {
            c.sequence.store(0, Ordering::Relaxed);
            c.sample(&i.to_string(), Instant::now()).unwrap();
        }
        assert_eq!(c.pending.lock().unwrap().len(), LIMIT);
        assert_eq!(c.evicted.load(Ordering::Relaxed), 17);
        drop(c);
        assert!(weak.upgrade().is_none());
    }
    #[test]
    fn first_dispatch_is_recorded_once_and_failure_releases_registry() {
        let c = collector();
        let trace = c.sample("a", Instant::now()).unwrap();
        trace.stage();
        trace.event();
        trace.dispatch(true);
        trace.dispatch(false);
        trace.record("confirm");
        trace.record("ack");
        trace.fail();
        let snapshot = c.snapshot();
        assert_eq!(snapshot["pending_samples"], 0);
        assert_eq!(
            snapshot["histograms"]["early.admission_to_dispatch"]["n"],
            1
        );
        assert_eq!(snapshot["histograms"]["early.admission_to_confirm"]["n"], 1);
        assert_eq!(snapshot["histograms"]["early.admission_to_ack"]["n"], 1);
        assert!(
            snapshot["histograms"]
                .get("fallback.admission_to_dispatch")
                .is_none()
        );
    }
}
