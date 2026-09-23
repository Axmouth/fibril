//! Payload-free recovery timing. One event per bounded stage, never per record.
use fibril_coordination_ganglion::promotion::PendingRecovery;
use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

pub(crate) struct RecoveryTiming {
    topic: String,
    partition: u64,
    group: Option<String>,
    epoch: u64,
    transition: String,
    started: Instant,
    sequence: AtomicU64,
    outcome: &'static str,
}
impl RecoveryTiming {
    pub(crate) fn new(pending: &PendingRecovery) -> Result<Self, String> {
        Ok(Self {
            topic: pending.proposed.resource.name.clone(),
            partition: pending.proposed.resource.partition as u64,
            group: pending.proposed.resource.group.clone(),
            epoch: pending.proposed.epoch,
            transition: pending
                .transition_digest()?
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
            started: Instant::now(),
            sequence: AtomicU64::new(0),
            outcome: "cancelled",
        })
    }
    pub(crate) async fn stage<F, T, E>(
        &self,
        stage: &'static str,
        peer: &str,
        future: F,
    ) -> Result<T, E>
    where
        F: Future<Output = Result<T, E>>,
    {
        let mut guard = StageTiming {
            attempt: self,
            stage,
            peer,
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed),
            started: Instant::now(),
            outcome: "cancelled",
        };
        let result = future.await;
        guard.outcome = if result.is_ok() { "ok" } else { "error" };
        result
    }
    pub(crate) fn finish(&mut self, ok: bool) {
        self.outcome = if ok { "ok" } else { "error" };
    }
}
impl Drop for RecoveryTiming {
    fn drop(&mut self) {
        tracing::info!(target: "fibril::recovery_timing", topic=self.topic, partition=self.partition,
            group=?self.group, epoch=self.epoch, transition=self.transition,
            elapsed_us=self.started.elapsed().as_micros() as u64,
            stages=self.sequence.load(Ordering::Relaxed), outcome=self.outcome,
            "recovery attempt timing");
    }
}
struct StageTiming<'a> {
    attempt: &'a RecoveryTiming,
    stage: &'static str,
    peer: &'a str,
    sequence: u64,
    started: Instant,
    outcome: &'static str,
}
impl Drop for StageTiming<'_> {
    fn drop(&mut self) {
        let trace = self.attempt;
        tracing::info!(target: "fibril::recovery_timing", topic=trace.topic, partition=trace.partition,
            group=?trace.group, epoch=trace.epoch, transition=trace.transition,
            stage=self.stage, peer=self.peer, sequence=self.sequence,
            stage_us=self.started.elapsed().as_micros() as u64,
            attempt_us=trace.started.elapsed().as_micros() as u64, outcome=self.outcome,
            "recovery stage timing");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };
    use tracing::{
        Event, Metadata, Subscriber,
        field::{Field, Visit},
        span::{Attributes, Id, Record},
    };
    #[derive(Clone, Default)]
    struct Capture(Arc<Mutex<Vec<BTreeMap<String, String>>>>);
    struct Fields(BTreeMap<String, String>);
    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().into(), format!("{value:?}"));
        }
    }
    impl Subscriber for Capture {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields(BTreeMap::new());
            event.record(&mut fields);
            self.0.lock().unwrap().push(fields.0);
        }
    }
    #[test]
    fn stages_record_success_error_and_cancellation_with_one_identity() {
        let capture = Capture::default();
        tracing::subscriber::with_default(capture.clone(), || {
            let mut timing = RecoveryTiming {
                topic: "q".into(),
                partition: 0,
                group: Some("g".into()),
                epoch: 2,
                transition: "0123".into(),
                started: Instant::now(),
                sequence: AtomicU64::new(0),
                outcome: "cancelled",
            };
            futures::executor::block_on(async {
                assert_eq!(
                    timing.stage("first", "a", async { Ok::<_, ()>(7) }).await,
                    Ok(7)
                );
                assert_eq!(
                    timing.stage("second", "b", async { Err::<(), _>(9) }).await,
                    Err(9)
                );
                let mut cancelled = Box::pin(timing.stage(
                    "third",
                    "c",
                    futures::future::pending::<Result<(), ()>>(),
                ));
                assert!(futures::poll!(cancelled.as_mut()).is_pending());
                drop(cancelled);
            });
            timing.finish(false);
        });
        let events = capture.0.lock().unwrap();
        assert_eq!(events.len(), 4);
        for (index, outcome) in ["ok", "error", "cancelled"].iter().enumerate() {
            assert_eq!(events[index]["outcome"], format!("{outcome:?}"));
            assert_eq!(events[index]["sequence"], index.to_string());
            assert_eq!(events[index]["transition"], "\"0123\"");
            assert!(events[index].contains_key("stage_us"));
        }
        assert_eq!(events[3]["outcome"], "\"error\"");
        assert_eq!(events[3]["stages"], "3");
    }
}
