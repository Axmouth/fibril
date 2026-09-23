//! Node-local durable storage policy; deliberately separate from the replicated
//! broker runtime-settings document. The update task survives caller cancellation.
use crate::queue_engine::{
    GlobalKey, GlobalStore, LogRuntimeConfig, LogRuntimeSettings, LogRuntimeSnapshot, PutOutcome,
};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Override {
    format_version: u32,
    segment_preallocate_bytes: Option<usize>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalStorageUpdate {
    pub node_id: String,
    pub expected_version: u64,
    /// None removes the override, retaining a versioned reset record.
    pub segment_preallocate_bytes: Option<usize>,
}
#[derive(Debug, Serialize)]
pub struct LocalStorageStatus {
    pub node_id: String,
    pub version: u64,
    pub startup_preallocate_bytes: usize,
    pub override_preallocate_bytes: Option<usize>,
    pub requested_preallocate_bytes: usize,
    pub pending_logs: usize,
    pub failed_logs: usize,
    pub logs: Vec<LocalLogStatus>,
}
#[derive(Debug, Serialize)]
pub struct LocalLogStatus {
    pub path: String,
    pub applied_revision: u64,
    pub active_segment_base: u64,
    pub configured_preallocate_bytes: usize,
    pub effective_preallocate_bytes: u64,
    pub allocation_error: Option<String>,
}
#[derive(Debug, thiserror::Error)]
pub enum LocalStorageError {
    #[error("settings target does not match this node")]
    WrongNode,
    #[error("settings changed; reload before saving")]
    Conflict,
    #[error("invalid local storage settings: {0}")]
    Invalid(String),
    #[error("local storage settings could not be persisted or applied: {0}")]
    Store(String),
}
#[derive(Debug)]
pub struct LocalStorageSettings {
    node_id: String,
    root: PathBuf,
    seed: usize,
    store: Arc<GlobalStore>,
    runtime: Arc<LogRuntimeSettings>,
    state: Mutex<(u64, Option<usize>)>,
}
fn key() -> GlobalKey {
    GlobalKey::new("fibril.node-local", "storage-settings").expect("constant valid key")
}
fn config(bytes: usize) -> Result<LogRuntimeConfig, LocalStorageError> {
    let config = LogRuntimeConfig {
        segment_preallocate_bytes: bytes,
    };
    config
        .validate()
        .map_err(|e| LocalStorageError::Invalid(e.to_string()))?;
    Ok(config)
}
impl LocalStorageSettings {
    pub async fn load(
        node_id: String,
        root: PathBuf,
        seed: usize,
        store: Arc<GlobalStore>,
        runtime: Arc<LogRuntimeSettings>,
    ) -> Result<Arc<Self>, LocalStorageError> {
        let stored = store
            .get(&key())
            .await
            .map_err(|e| LocalStorageError::Store(e.to_string()))?;
        let (version, bytes) = match stored {
            None => (0, None),
            Some(value) => {
                let record: Override = serde_json::from_slice(&value.bytes)
                    .map_err(|e| LocalStorageError::Invalid(e.to_string()))?;
                if record.format_version != 1 {
                    return Err(LocalStorageError::Invalid(
                        "unsupported format version".into(),
                    ));
                }
                let config = config(record.segment_preallocate_bytes.unwrap_or(seed))?;
                runtime
                    .install(
                        0,
                        LogRuntimeSnapshot {
                            revision: value.version,
                            config,
                        },
                    )
                    .map_err(|e| LocalStorageError::Store(e.to_string()))?;
                (value.version, record.segment_preallocate_bytes)
            }
        };
        Ok(Arc::new(Self {
            node_id,
            root,
            seed,
            store,
            runtime,
            state: Mutex::new((version, bytes)),
        }))
    }
    pub async fn status(&self) -> LocalStorageStatus {
        let state = self.state.lock().await;
        self.status_for(*state)
    }
    fn status_for(&self, (version, override_bytes): (u64, Option<usize>)) -> LocalStorageStatus {
        let logs: Vec<_> = self
            .runtime
            .logs()
            .into_iter()
            .map(|log| LocalLogStatus {
                path: log
                    .root
                    .strip_prefix(&self.root)
                    .unwrap_or(&log.root)
                    .display()
                    .to_string(),
                applied_revision: log.applied.revision,
                active_segment_base: log.active_segment_base,
                configured_preallocate_bytes: log.applied.config.segment_preallocate_bytes,
                effective_preallocate_bytes: log.effective_preallocate_bytes,
                allocation_error: log.allocation_error,
            })
            .collect();
        LocalStorageStatus {
            node_id: self.node_id.clone(),
            version,
            startup_preallocate_bytes: self.seed,
            override_preallocate_bytes: override_bytes,
            requested_preallocate_bytes: override_bytes.unwrap_or(self.seed),
            pending_logs: logs
                .iter()
                .filter(|l| l.applied_revision != version)
                .count(),
            failed_logs: logs.iter().filter(|l| l.allocation_error.is_some()).count(),
            logs,
        }
    }
    pub async fn update(
        self: &Arc<Self>,
        request: LocalStorageUpdate,
    ) -> Result<LocalStorageStatus, LocalStorageError> {
        if request.node_id != self.node_id {
            return Err(LocalStorageError::WrongNode);
        }
        let candidate = config(request.segment_preallocate_bytes.unwrap_or(self.seed))?;
        let this = self.clone();
        // Once persistence starts, finish publication even if HTTP disconnects.
        tokio::spawn(async move {
            let mut state = this.state.lock().await;
            if state.0 != request.expected_version {
                return Err(LocalStorageError::Conflict);
            }
            let bytes = serde_json::to_vec(&Override {
                format_version: 1,
                segment_preallocate_bytes: request.segment_preallocate_bytes,
            })
            .map_err(|e| LocalStorageError::Invalid(e.to_string()))?;
            let result = this
                .store
                .put(key(), bytes, Some(state.0))
                .await
                .map_err(|e| LocalStorageError::Store(e.to_string()))?;
            let PutOutcome::Stored { version } = result else {
                return Err(LocalStorageError::Conflict);
            };
            this.runtime
                .install(
                    state.0,
                    LogRuntimeSnapshot {
                        revision: version,
                        config: candidate,
                    },
                )
                .map_err(|e| LocalStorageError::Store(e.to_string()))?;
            *state = (version, request.segment_preallocate_bytes);
            Ok(this.status_for(*state))
        })
        .await
        .map_err(|e| LocalStorageError::Store(e.to_string()))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue_engine::KeratinConfig;
    async fn fixture(
        name: &str,
    ) -> (
        PathBuf,
        Arc<GlobalStore>,
        Arc<LogRuntimeSettings>,
        Arc<LocalStorageSettings>,
    ) {
        let root = std::env::temp_dir().join(format!(
            "fibril-local-settings-{name}-{}",
            uuid::Uuid::now_v7()
        ));
        let store = Arc::new(
            GlobalStore::open(&root, KeratinConfig::test_default())
                .await
                .unwrap(),
        );
        let runtime = Arc::new(LogRuntimeSettings::new(config(0).unwrap()).unwrap());
        let settings = LocalStorageSettings::load(
            "node-a".into(),
            root.clone(),
            0,
            store.clone(),
            runtime.clone(),
        )
        .await
        .unwrap();
        (root, store, runtime, settings)
    }
    fn request(version: u64, bytes: Option<usize>) -> LocalStorageUpdate {
        LocalStorageUpdate {
            node_id: "node-a".into(),
            expected_version: version,
            segment_preallocate_bytes: bytes,
        }
    }
    #[tokio::test]
    async fn local_storage_override_recovers_and_reset_tracks_startup_seed() {
        let (root, store, runtime, settings) = fixture("restart").await;
        settings.update(request(0, Some(4096))).await.unwrap();
        assert_eq!(runtime.current().revision, 1);
        store.shutdown().await.unwrap();
        drop(settings);
        drop(store);
        let store = Arc::new(
            GlobalStore::open(&root, KeratinConfig::test_default())
                .await
                .unwrap(),
        );
        let runtime = Arc::new(LogRuntimeSettings::new(config(8192).unwrap()).unwrap());
        let settings = LocalStorageSettings::load(
            "node-a".into(),
            root.clone(),
            8192,
            store.clone(),
            runtime.clone(),
        )
        .await
        .unwrap();
        assert_eq!(settings.status().await.requested_preallocate_bytes, 4096);
        let reset = settings.update(request(1, None)).await.unwrap();
        assert_eq!(reset.version, 2);
        assert_eq!(reset.requested_preallocate_bytes, 8192);
        store.shutdown().await.unwrap();
        drop(settings);
        drop(store);
        let store = Arc::new(
            GlobalStore::open(&root, KeratinConfig::test_default())
                .await
                .unwrap(),
        );
        let runtime = Arc::new(LogRuntimeSettings::new(config(16384).unwrap()).unwrap());
        let settings = LocalStorageSettings::load(
            "node-a".into(),
            root.clone(),
            16384,
            store.clone(),
            runtime,
        )
        .await
        .unwrap();
        assert_eq!(settings.status().await.requested_preallocate_bytes, 16384);
        assert_eq!(settings.status().await.version, 2);
        store.shutdown().await.unwrap();
        drop(settings);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn local_storage_rejects_wrong_node_and_concurrent_stale_edits() {
        let (root, store, runtime, settings) = fixture("cas").await;
        let mut wrong = request(0, Some(4096));
        wrong.node_id = "node-b".into();
        assert!(matches!(
            settings.update(wrong).await,
            Err(LocalStorageError::WrongNode)
        ));
        let (a, b) = tokio::join!(
            settings.update(request(0, Some(4096))),
            settings.update(request(0, Some(8192)))
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
        assert_eq!(runtime.current().revision, 1);
        if cfg!(target_pointer_width = "64") {
            assert!(matches!(
                settings.update(request(1, Some(usize::MAX))).await,
                Err(LocalStorageError::Invalid(_))
            ));
        }
        assert_eq!(settings.status().await.version, 1);
        store.shutdown().await.unwrap();
        drop(settings);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn local_storage_failed_persistence_does_not_publish_policy() {
        let (root, store, runtime, settings) = fixture("failed").await;
        store.shutdown().await.unwrap();
        assert!(settings.update(request(0, Some(4096))).await.is_err());
        assert_eq!(runtime.current().revision, 0);
        assert_eq!(settings.status().await.version, 0);
        drop(settings);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn local_storage_update_completes_after_caller_cancellation() {
        let (root, store, runtime, settings) = fixture("cancel").await;
        let guard = settings.state.lock().await;
        let mut update = Box::pin(settings.update(request(0, Some(4096))));
        assert!(futures::poll!(update.as_mut()).is_pending());
        drop(update);
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while runtime.current().revision == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(settings.status().await.version, 1);
        assert!(store.get(&key()).await.unwrap().is_some());
        store.shutdown().await.unwrap();
        drop(settings);
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[tokio::test]
    async fn local_storage_changes_are_isolated_between_nodes() {
        let (root_a, store_a, _, a) = fixture("isolation-a").await;
        let (root_b, store_b, runtime_b, b) = fixture("isolation-b").await;
        a.update(request(0, Some(4096))).await.unwrap();
        assert_eq!(runtime_b.current().revision, 0);
        assert_eq!(b.status().await.version, 0);
        assert!(store_b.get(&key()).await.unwrap().is_none());
        store_a.shutdown().await.unwrap();
        store_b.shutdown().await.unwrap();
        drop(a);
        drop(b);
        drop(store_a);
        drop(store_b);
        std::fs::remove_dir_all(root_a).unwrap();
        std::fs::remove_dir_all(root_b).unwrap();
    }
}
