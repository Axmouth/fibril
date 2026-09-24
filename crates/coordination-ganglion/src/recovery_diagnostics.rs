//! Bounded, process-local observations. These never participate in recovery authority.
use serde::Serialize;
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

const ATTEMPTS: usize = 32;
const STAGES: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Running,
    Ok,
    Error,
    Cancelled,
}

#[derive(Clone, Serialize)]
pub struct Stage {
    pub sequence: u64,
    pub name: String,
    pub peer: String,
    pub start_us: u64,
    pub elapsed_us: u64,
    pub outcome: Outcome,
}
#[derive(Clone, Serialize)]
pub struct Attempt {
    pub id: u64,
    pub topic: String,
    pub partition: u64,
    pub group: Option<String>,
    pub epoch: String,
    pub transition: String,
    pub started_at_ms: u64,
    pub elapsed_us: u64,
    pub outcome: Outcome,
    pub stages: Vec<Stage>,
    pub omitted_stages: u64,
    pub labels_truncated: bool,
}
struct Entry {
    view: Attempt,
    started: Instant,
}
#[derive(Default)]
struct State {
    next: u64,
    entries: VecDeque<Entry>,
    evicted: u64,
}
#[derive(Clone, Default)]
pub struct RecoveryDiagnostics(Arc<Mutex<State>>);
#[derive(Serialize)]
pub struct Snapshot {
    pub scope: &'static str,
    pub capacity: usize,
    pub stage_capacity: usize,
    pub evicted_attempts: u64,
    pub attempts: Vec<Attempt>,
}
fn micros(start: Instant) -> u64 {
    start.elapsed().as_micros().min(u64::MAX as u128) as u64
}
fn clipped(s: &str) -> String {
    s.chars().take(128).collect()
}

impl RecoveryDiagnostics {
    pub fn begin(
        &self,
        topic: &str,
        partition: u64,
        group: Option<&str>,
        epoch: u64,
        transition: &str,
    ) -> u64 {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        state.next = state.next.saturating_add(1);
        let id = state.next;
        if state.entries.len() == ATTEMPTS {
            // Prefer retaining in-progress work. If all slots are active, evict
            // the oldest anyway: diagnostics must never create unbounded work.
            let index = state
                .entries
                .iter()
                .position(|e| e.view.outcome != Outcome::Running)
                .unwrap_or(0);
            state.entries.remove(index);
            state.evicted = state.evicted.saturating_add(1);
        }
        state.entries.push_back(Entry {
            started: Instant::now(),
            view: Attempt {
                id,
                topic: clipped(topic),
                partition,
                group: group.map(clipped),
                epoch: epoch.to_string(),
                transition: clipped(transition),
                started_at_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
                elapsed_us: 0,
                outcome: Outcome::Running,
                stages: Vec::new(),
                omitted_stages: 0,
                labels_truncated: [Some(topic), group, Some(transition)]
                    .into_iter()
                    .flatten()
                    .any(|s| s.chars().count() > 128),
            },
        });
        id
    }
    pub fn start_stage(&self, id: u64, sequence: u64, name: &str, peer: &str) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = state.entries.iter_mut().find(|e| e.view.id == id) else {
            return;
        };
        if entry.view.stages.len() == STAGES {
            entry.view.omitted_stages = entry.view.omitted_stages.saturating_add(1);
            return;
        }
        entry.view.labels_truncated |= name.chars().count() > 128 || peer.chars().count() > 128;
        entry.view.stages.push(Stage {
            sequence,
            name: clipped(name),
            peer: clipped(peer),
            start_us: micros(entry.started),
            elapsed_us: 0,
            outcome: Outcome::Running,
        });
    }
    pub fn finish_stage(&self, id: u64, sequence: u64, outcome: Outcome) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let Some(entry) = state.entries.iter_mut().find(|e| e.view.id == id) else {
            return;
        };
        let now = micros(entry.started);
        if let Some(stage) = entry
            .view
            .stages
            .iter_mut()
            .find(|s| s.sequence == sequence)
        {
            stage.elapsed_us = now.saturating_sub(stage.start_us);
            stage.outcome = outcome;
        }
    }
    pub fn finish(&self, id: u64, outcome: Outcome) {
        let mut state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = state.entries.iter_mut().find(|e| e.view.id == id) {
            entry.view.elapsed_us = micros(entry.started);
            entry.view.outcome = outcome;
        }
    }
    pub fn snapshot(&self) -> Snapshot {
        let state = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let attempts = state
            .entries
            .iter()
            .rev()
            .map(|entry| {
                let mut view = entry.view.clone();
                if view.outcome == Outcome::Running {
                    view.elapsed_us = micros(entry.started);
                }
                for stage in &mut view.stages {
                    if stage.outcome == Outcome::Running {
                        stage.elapsed_us = view.elapsed_us.saturating_sub(stage.start_us);
                    }
                }
                view.stages.sort_by_key(|s| s.sequence);
                view
            })
            .collect();
        Snapshot {
            scope: "local_process",
            capacity: ATTEMPTS,
            stage_capacity: STAGES,
            evicted_attempts: state.evicted,
            attempts,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retention_is_bounded_and_late_completion_cannot_change_another_attempt() {
        let store = RecoveryDiagnostics::default();
        let first = store.begin("q", 0, None, 1, "old");
        for _ in 0..ATTEMPTS {
            store.begin("q", 0, None, 2, "new");
        }
        store.finish(first, Outcome::Error);
        let snapshot = store.snapshot();
        assert_eq!(snapshot.attempts.len(), ATTEMPTS);
        assert_eq!(snapshot.evicted_attempts, 1);
        assert!(snapshot
            .attempts
            .iter()
            .all(|a| a.outcome == Outcome::Running));
        let completed = snapshot.attempts[0].id;
        store.finish(completed, Outcome::Ok);
        store.begin("other", 0, None, 3, "next");
        assert!(store.snapshot().attempts.iter().all(|a| a.id != completed));
    }
    #[test]
    fn live_snapshots_advance_without_mutating_completed_stages() {
        let store = RecoveryDiagnostics::default();
        assert!(store.snapshot().attempts.is_empty());
        let id = store.begin("q", 0, None, 1, "transition");
        // Advance the origin without a sleeping test. Stage callbacks can arrive
        // from concurrent pipelines in a different order from sequence IDs.
        store.0.lock().unwrap().entries[0].started -= std::time::Duration::from_secs(1);
        store.start_stage(id, 4, "copy_pages", "b");
        store.start_stage(id, 2, "copy_pages", "a");
        store.finish_stage(id, 2, Outcome::Ok);
        let first = store.snapshot();
        let second = store.snapshot();
        assert_eq!(first.attempts[0].stages[0].sequence, 2);
        assert!(second.attempts[0].elapsed_us >= first.attempts[0].elapsed_us);
        assert!(first.attempts[0].elapsed_us >= 1_000_000);
        assert_eq!(
            first.attempts[0].stages[0].elapsed_us,
            second.attempts[0].stages[0].elapsed_us
        );
        store.finish_stage(id, 4, Outcome::Cancelled);
        store.finish(id, Outcome::Cancelled);
        let finished = store.snapshot();
        assert_eq!(finished.attempts[0].outcome, Outcome::Cancelled);
        assert_eq!(
            finished.attempts[0].elapsed_us,
            store.snapshot().attempts[0].elapsed_us
        );
    }

    #[test]
    fn stages_and_labels_are_bounded_and_snapshots_are_independent() {
        let store = RecoveryDiagnostics::default();
        let id = store.begin(&"é".repeat(200), 0, None, u64::MAX, "cut");
        for sequence in 0..STAGES as u64 + 2 {
            store.start_stage(id, sequence, "copy_pages", "b");
        }
        store.finish_stage(id, 0, Outcome::Error);
        store.finish_stage(id, 1, Outcome::Cancelled);
        let snapshot = store.snapshot();
        let a = &snapshot.attempts[0];
        assert_eq!(a.topic.chars().count(), 128);
        assert!(a.labels_truncated);
        assert_eq!(a.epoch, u64::MAX.to_string());
        assert_eq!(a.stages.len(), STAGES);
        assert_eq!(a.omitted_stages, 2);
        assert_eq!(a.stages[0].outcome, Outcome::Error);
        store.finish(id, Outcome::Ok);
        assert_eq!(a.outcome, Outcome::Running);
        assert_eq!(store.snapshot().attempts[0].outcome, Outcome::Ok);
    }
}
