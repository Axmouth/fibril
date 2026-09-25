//! On-demand background sample, shared by SSE and HTTP overview readers.
use fibril_broker::queue_engine::{QueueEngine, RecoveryDiskUsage};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Default, Serialize)]
pub struct StorageUsageSnapshot {
    pub sample: Option<RecoveryDiskUsage>,
    pub sampled_at_ms: Option<u64>,
    pub refreshing: bool,
    pub error: Option<String>,
}
#[derive(Default)]
struct State {
    snapshot: StorageUsageSnapshot,
    next_attempt: Option<Instant>,
}
#[derive(Default)]
pub struct StorageUsageCache(Mutex<State>);
impl StorageUsageCache {
    pub fn read(
        self: &Arc<Self>,
        storage: Arc<dyn QueueEngine + Send + Sync>,
    ) -> StorageUsageSnapshot {
        let mut state = self.0.lock().expect("storage usage cache poisoned");
        if !state.snapshot.refreshing
            && state.next_attempt.is_none_or(|next| Instant::now() >= next)
        {
            state.snapshot.refreshing = true;
            let cache = self.clone();
            tokio::spawn(async move {
                let result = storage.recovery_disk_usage().await;
                let mut state = cache.0.lock().expect("storage usage cache poisoned");
                match result {
                    Ok(Some(sample)) => {
                        state.snapshot.sample = Some(sample);
                        state.snapshot.sampled_at_ms = Some(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64,
                        );
                        state.snapshot.error = None;
                    }
                    Ok(None) => {
                        state.snapshot.error =
                            Some("Storage accounting is unavailable for this engine".into())
                    }
                    Err(error) => state.snapshot.error = Some(error.to_string()),
                }
                state.snapshot.refreshing = false;
                state.next_attempt = Some(Instant::now() + Duration::from_secs(30));
            });
        }
        state.snapshot.clone()
    }
}
