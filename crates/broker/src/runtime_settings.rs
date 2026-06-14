use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use stroma_core::{GlobalKey, GlobalStore, GlobalValue, PutOutcome, StromaError};
use tokio::sync::watch;

use crate::{broker::BrokerConfig, queue_engine::StromaEngine};

const ENVELOPE_VERSION: u16 = 1;
pub const RUNTIME_SETTINGS_NAMESPACE: &str = "fibril.runtime";
pub const RUNTIME_SETTINGS_KEY: &str = "settings";

/// Mutable broker settings loaded from the boot seed and then persisted in Stroma.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSettings {
    pub delivery: DeliveryRuntimeSettings,
    pub idle_queue_cleanup: IdleQueueCleanupRuntimeSettings,
    #[serde(default)]
    pub connection: ConnectionRuntimeSettings,
    #[serde(default)]
    pub replication: ReplicationRuntimeSettings,
    #[serde(default)]
    pub partitioning: PartitioningRuntimeSettings,
    #[serde(default)]
    pub consumer_groups: ConsumerGroupRuntimeSettings,
}

/// Partitioning-related runtime settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PartitioningRuntimeSettings {
    /// Partition count for a queue declared without an explicit count
    /// (Kafka `num.partitions` equivalent).
    pub default_partition_count: u32,
}

impl Default for PartitioningRuntimeSettings {
    fn default() -> Self {
        Self {
            default_partition_count: 1,
        }
    }
}

/// Exclusive consumer-group runtime settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConsumerGroupRuntimeSettings {
    /// Soft target partitions-per-consumer for an exclusive cohort. Exceeding it
    /// flags the cohort under-provisioned (an alert/autoscale signal); coverage
    /// is never reduced. `None` disables the signal.
    pub default_target_per_consumer: Option<usize>,
}

impl RuntimeSettings {
    pub fn validate(&self) -> Result<(), RuntimeSettingsError> {
        if self.delivery.expiry_batch_max == 0 {
            return Err(RuntimeSettingsError::Invalid(
                "delivery.expiry_batch_max must be at least 1".into(),
            ));
        }
        if self.idle_queue_cleanup.sweep_interval_ms == 0 {
            return Err(RuntimeSettingsError::Invalid(
                "idle_queue_cleanup.sweep_interval_ms must be at least 1".into(),
            ));
        }
        if self.replication.caught_up_poll_ms == 0
            || self.replication.retry_poll_ms == 0
            || self.replication.checkpoint_retry_poll_ms == 0
        {
            return Err(RuntimeSettingsError::Invalid(
                "replication poll intervals must be at least 1ms".into(),
            ));
        }
        if self.replication.min_in_sync_replicas == 0 {
            return Err(RuntimeSettingsError::Invalid(
                "replication.min_in_sync_replicas must be at least 1".into(),
            ));
        }
        if self.replication.isr_timeout_ms == 0 {
            return Err(RuntimeSettingsError::Invalid(
                "replication.isr_timeout_ms must be at least 1".into(),
            ));
        }
        if self.partitioning.default_partition_count == 0 {
            return Err(RuntimeSettingsError::Invalid(
                "partitioning.default_partition_count must be at least 1".into(),
            ));
        }
        Ok(())
    }

    fn apply_locks(&mut self, seed: &RuntimeSettings, locks: &RuntimeSettingsLocks) {
        if locks.idle_queue_cleanup {
            self.idle_queue_cleanup = seed.idle_queue_cleanup.clone();
        }
    }
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        Self {
            delivery: DeliveryRuntimeSettings::default(),
            idle_queue_cleanup: IdleQueueCleanupRuntimeSettings::default(),
            connection: ConnectionRuntimeSettings::default(),
            replication: ReplicationRuntimeSettings::default(),
            partitioning: PartitioningRuntimeSettings::default(),
            consumer_groups: ConsumerGroupRuntimeSettings::default(),
        }
    }
}

/// Runtime delivery-loop settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeliveryRuntimeSettings {
    pub inflight_ttl_ms: u64,
    pub expiry_poll_min_ms: u64,
    pub expiry_batch_max: usize,
    pub delivery_poll_max_ms: u64,
}

/// Replication-related runtime settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ReplicationRuntimeSettings {
    /// How long a publish confirm may wait for the assignment's replication
    /// durability policy (replica acks) before failing with a clear error.
    pub confirm_timeout_ms: u64,
    /// Follower pull interval while caught up with the owner. Bounds the
    /// extra confirm latency of replica-durable publishes.
    pub caught_up_poll_ms: u64,
    /// Follower retry interval after a transient error or partial pull.
    pub retry_poll_ms: u64,
    /// Follower retry interval while a checkpoint install is required.
    pub checkpoint_retry_poll_ms: u64,
    /// Minimum in-sync replicas required to accept a replica-durable publish.
    /// 1 disables the floor.
    pub min_in_sync_replicas: usize,
    /// How recently a follower must have reported to count as in-sync.
    pub isr_timeout_ms: u64,
}

impl Default for ReplicationRuntimeSettings {
    fn default() -> Self {
        Self {
            confirm_timeout_ms: 5_000,
            caught_up_poll_ms: 1_000,
            retry_poll_ms: 100,
            checkpoint_retry_poll_ms: 5_000,
            min_in_sync_replicas: 1,
            isr_timeout_ms: 10_000,
        }
    }
}

impl Default for DeliveryRuntimeSettings {
    fn default() -> Self {
        Self {
            inflight_ttl_ms: 30_000,
            expiry_poll_min_ms: 15_000,
            expiry_batch_max: 8192,
            delivery_poll_max_ms: 5_000,
        }
    }
}

/// Runtime queue memory cleanup settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdleQueueCleanupRuntimeSettings {
    pub enabled: bool,
    pub evict_after_ms: u64,
    pub sweep_interval_ms: u64,
    pub publisher_idle_timeout_ms: Option<u64>,
}

impl Default for IdleQueueCleanupRuntimeSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            evict_after_ms: 600_000,
            sweep_interval_ms: 60_000,
            publisher_idle_timeout_ms: None,
        }
    }
}

impl IdleQueueCleanupRuntimeSettings {
    pub fn queue_idle_evict_after_ms(&self) -> Option<u64> {
        self.enabled.then_some(self.evict_after_ms)
    }
}

/// Runtime connection lifecycle settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectionRuntimeSettings {
    pub reconnect_grace_ms: Option<u64>,
}

/// Boot-owned locks that prevent selected runtime sections from being overridden by persisted state.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSettingsLocks {
    pub idle_queue_cleanup: bool,
}

/// Effective runtime settings plus the underlying Stroma global-store version.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSettingsSnapshot {
    pub version: u64,
    pub settings: RuntimeSettings,
}

/// Problem detected while loading persisted runtime settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeSettingsLoadIssue {
    pub version: u64,
    pub message: String,
}

/// Loads, validates, and exposes the effective runtime settings document.
#[derive(Debug)]
pub struct RuntimeSettingsManager {
    store: Arc<GlobalStore>,
    current: watch::Sender<RuntimeSettingsSnapshot>,
    seed: RuntimeSettings,
    locks: RuntimeSettingsLocks,
    load_issue: Arc<Mutex<Option<RuntimeSettingsLoadIssue>>>,
}

impl RuntimeSettingsManager {
    pub async fn load_from_stroma_engine(
        engine: &StromaEngine,
        seed: RuntimeSettings,
        locks: RuntimeSettingsLocks,
    ) -> Result<Self, RuntimeSettingsError> {
        let store = engine.global_store().await?;
        Self::load_from_store(store, seed, locks).await
    }

    pub async fn load_from_store(
        store: Arc<GlobalStore>,
        seed: RuntimeSettings,
        locks: RuntimeSettingsLocks,
    ) -> Result<Self, RuntimeSettingsError> {
        seed.validate()?;
        let loaded = load_or_seed_settings(&store, &seed, &locks).await?;
        if let Some(issue) = &loaded.issue {
            tracing::error!(
                "runtime settings at version {} could not be decoded; using boot seed until replaced: {}",
                issue.version,
                issue.message
            );
        }
        let snapshot = loaded.snapshot;
        let (current, _) = watch::channel(snapshot);
        Ok(Self {
            store,
            current,
            seed,
            locks,
            load_issue: Arc::new(Mutex::new(loaded.issue)),
        })
    }

    pub fn current(&self) -> RuntimeSettingsSnapshot {
        self.current.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<RuntimeSettingsSnapshot> {
        self.current.subscribe()
    }

    pub fn locks(&self) -> &RuntimeSettingsLocks {
        &self.locks
    }

    pub fn store(&self) -> Arc<GlobalStore> {
        self.store.clone()
    }

    pub fn load_issue(&self) -> Option<RuntimeSettingsLoadIssue> {
        load_issue_guard(&self.load_issue).clone()
    }

    pub async fn update(
        &self,
        expected_version: u64,
        settings: RuntimeSettings,
    ) -> Result<RuntimeSettingsUpdateOutcome, RuntimeSettingsError> {
        settings.validate()?;

        let key = runtime_settings_key()?;
        let current = match self.store.get(&key).await? {
            Some(value) => match decode_snapshot(value.clone()) {
                Ok(snapshot) => LoadedRuntimeSettings {
                    snapshot,
                    issue: None,
                },
                Err(err) if self.matches_load_issue(value.version) => LoadedRuntimeSettings {
                    snapshot: RuntimeSettingsSnapshot {
                        version: value.version,
                        settings: self.seed.clone(),
                    },
                    issue: Some(RuntimeSettingsLoadIssue {
                        version: value.version,
                        message: err.to_string(),
                    }),
                },
                Err(err) => return Err(err),
            },
            None => {
                return Err(RuntimeSettingsError::StoreConflict(
                    "runtime settings disappeared before update".into(),
                ));
            }
        };
        let current_effective =
            effective_snapshot(current.snapshot.clone(), &self.seed, &self.locks)?;
        if expected_version != current.snapshot.version {
            return Ok(RuntimeSettingsUpdateOutcome::Conflict(current_effective));
        }

        let mut persisted_settings = settings.clone();
        if self.locks.idle_queue_cleanup {
            if settings.idle_queue_cleanup != current_effective.settings.idle_queue_cleanup {
                return Err(RuntimeSettingsError::Locked(
                    "idle_queue_cleanup is locked by boot config".into(),
                ));
            }
            persisted_settings.idle_queue_cleanup = current.snapshot.settings.idle_queue_cleanup;
        }

        let bytes = encode_settings(&persisted_settings)?;
        match self.store.put(key, bytes, Some(expected_version)).await? {
            PutOutcome::Stored { version } => {
                let snapshot = effective_snapshot(
                    RuntimeSettingsSnapshot {
                        version,
                        settings: persisted_settings,
                    },
                    &self.seed,
                    &self.locks,
                )?;
                self.current.send_replace(snapshot.clone());
                *load_issue_guard(&self.load_issue) = None;
                Ok(RuntimeSettingsUpdateOutcome::Stored(snapshot))
            }
            PutOutcome::Conflict {
                current: Some(value),
            } => {
                let snapshot =
                    effective_snapshot(decode_snapshot(value)?, &self.seed, &self.locks)?;
                Ok(RuntimeSettingsUpdateOutcome::Conflict(snapshot))
            }
            PutOutcome::Conflict { current: None } => Err(RuntimeSettingsError::StoreConflict(
                "runtime settings update conflicted but no current value exists".into(),
            )),
        }
    }

    /// Apply a cluster-authoritative settings document to the local cache.
    ///
    /// Ganglion mode uses the cluster document as the authority, while the
    /// Stroma global store remains this node's durable local cache. The local
    /// store version may differ from the cluster document version, so this
    /// helper retries normal optimistic local updates instead of requiring the
    /// caller to know the current local version.
    pub async fn apply_cluster_settings(
        &self,
        settings: RuntimeSettings,
    ) -> Result<RuntimeSettingsSnapshot, RuntimeSettingsError> {
        settings.validate()?;

        for _ in 0..8 {
            let current = self.current();
            match self.update(current.version, settings.clone()).await? {
                RuntimeSettingsUpdateOutcome::Stored(snapshot) => return Ok(snapshot),
                RuntimeSettingsUpdateOutcome::Conflict(_) => continue,
            }
        }

        Err(RuntimeSettingsError::StoreConflict(
            "cluster runtime settings raced local cache updates repeatedly".into(),
        ))
    }
}

impl BrokerConfig {
    pub fn from_runtime_settings(settings: &RuntimeSettings) -> Self {
        Self {
            inflight_ttl_ms: settings.delivery.inflight_ttl_ms,
            expiry_poll_min_ms: settings.delivery.expiry_poll_min_ms,
            expiry_batch_max: settings.delivery.expiry_batch_max,
            delivery_poll_max_ms: settings.delivery.delivery_poll_max_ms,
            queue_idle_evict_after_ms: settings.idle_queue_cleanup.queue_idle_evict_after_ms(),
            queue_idle_sweep_interval_ms: settings.idle_queue_cleanup.sweep_interval_ms,
            replication_confirm_timeout_ms: settings.replication.confirm_timeout_ms,
            replication_caught_up_poll_ms: settings.replication.caught_up_poll_ms,
            replication_retry_poll_ms: settings.replication.retry_poll_ms,
            replication_checkpoint_retry_poll_ms: settings.replication.checkpoint_retry_poll_ms,
            replication_min_in_sync_replicas: settings.replication.min_in_sync_replicas,
            replication_isr_timeout_ms: settings.replication.isr_timeout_ms,
            default_partition_count: settings.partitioning.default_partition_count,
            default_consumer_target: settings.consumer_groups.default_target_per_consumer,
        }
    }
}

struct LoadedRuntimeSettings {
    snapshot: RuntimeSettingsSnapshot,
    issue: Option<RuntimeSettingsLoadIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RuntimeSettingsEnvelope {
    version: u16,
    settings: RuntimeSettings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeSettingsUpdateOutcome {
    Stored(RuntimeSettingsSnapshot),
    Conflict(RuntimeSettingsSnapshot),
}

#[derive(thiserror::Error, Debug)]
pub enum RuntimeSettingsError {
    #[error("stroma error: {0}")]
    Stroma(#[from] StromaError),

    #[error("encode runtime settings: {0}")]
    Encode(String),

    #[error("decode runtime settings: {0}")]
    Decode(String),

    #[error("invalid runtime settings: {0}")]
    Invalid(String),

    #[error("locked runtime setting: {0}")]
    Locked(String),

    #[error("runtime settings store conflict: {0}")]
    StoreConflict(String),
}

async fn load_or_seed_settings(
    store: &GlobalStore,
    seed: &RuntimeSettings,
    locks: &RuntimeSettingsLocks,
) -> Result<LoadedRuntimeSettings, RuntimeSettingsError> {
    let key = runtime_settings_key()?;
    let loaded = match store.get(&key).await? {
        Some(value) => decode_or_seed_snapshot(value, seed)?,
        None => {
            let bytes = encode_settings(seed)?;
            match store.put(key.clone(), bytes, Some(0)).await? {
                PutOutcome::Stored { version } => LoadedRuntimeSettings {
                    snapshot: RuntimeSettingsSnapshot {
                        version,
                        settings: seed.clone(),
                    },
                    issue: None,
                },
                PutOutcome::Conflict {
                    current: Some(value),
                } => decode_or_seed_snapshot(value, seed)?,
                PutOutcome::Conflict { current: None } => match store.get(&key).await? {
                    Some(value) => decode_or_seed_snapshot(value, seed)?,
                    None => {
                        return Err(RuntimeSettingsError::StoreConflict(
                            "seed write conflicted but no current runtime settings value exists"
                                .into(),
                        ));
                    }
                },
            }
        }
    };

    Ok(LoadedRuntimeSettings {
        snapshot: effective_snapshot(loaded.snapshot, seed, locks)?,
        issue: loaded.issue,
    })
}

fn effective_snapshot(
    mut snapshot: RuntimeSettingsSnapshot,
    seed: &RuntimeSettings,
    locks: &RuntimeSettingsLocks,
) -> Result<RuntimeSettingsSnapshot, RuntimeSettingsError> {
    snapshot.settings.validate()?;
    snapshot.settings.apply_locks(seed, locks);
    snapshot.settings.validate()?;
    Ok(snapshot)
}

fn decode_or_seed_snapshot(
    value: GlobalValue,
    seed: &RuntimeSettings,
) -> Result<LoadedRuntimeSettings, RuntimeSettingsError> {
    let version = value.version;
    match decode_snapshot(value) {
        Ok(snapshot) => Ok(LoadedRuntimeSettings {
            snapshot,
            issue: None,
        }),
        Err(err) => Ok(LoadedRuntimeSettings {
            snapshot: RuntimeSettingsSnapshot {
                version,
                settings: seed.clone(),
            },
            issue: Some(RuntimeSettingsLoadIssue {
                version,
                message: err.to_string(),
            }),
        }),
    }
}

fn load_issue_guard(
    load_issue: &Mutex<Option<RuntimeSettingsLoadIssue>>,
) -> std::sync::MutexGuard<'_, Option<RuntimeSettingsLoadIssue>> {
    load_issue
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl RuntimeSettingsManager {
    fn matches_load_issue(&self, version: u64) -> bool {
        load_issue_guard(&self.load_issue)
            .as_ref()
            .is_some_and(|issue| issue.version == version)
    }
}

fn runtime_settings_key() -> Result<GlobalKey, RuntimeSettingsError> {
    GlobalKey::new(RUNTIME_SETTINGS_NAMESPACE, RUNTIME_SETTINGS_KEY)
        .map_err(RuntimeSettingsError::Stroma)
}

fn encode_settings(settings: &RuntimeSettings) -> Result<Vec<u8>, RuntimeSettingsError> {
    settings.validate()?;
    rmp_serde::to_vec_named(&RuntimeSettingsEnvelope {
        version: ENVELOPE_VERSION,
        settings: settings.clone(),
    })
    .map_err(|err| RuntimeSettingsError::Encode(err.to_string()))
}

fn decode_snapshot(value: GlobalValue) -> Result<RuntimeSettingsSnapshot, RuntimeSettingsError> {
    let envelope: RuntimeSettingsEnvelope = rmp_serde::from_slice(&value.bytes)
        .map_err(|err| RuntimeSettingsError::Decode(err.to_string()))?;
    if envelope.version != ENVELOPE_VERSION {
        return Err(RuntimeSettingsError::Decode(format!(
            "unsupported runtime settings envelope version {}",
            envelope.version
        )));
    }
    envelope.settings.validate()?;
    Ok(RuntimeSettingsSnapshot {
        version: value.version,
        settings: envelope.settings,
    })
}

#[cfg(test)]
mod tests {
    use stroma_core::{KeratinConfig, SnapshotConfig, Stroma, StromaKeratinConfig, test_dir};

    use super::*;

    #[test]
    fn runtime_settings_roundtrip_through_messagepack() {
        let settings = RuntimeSettings {
            delivery: DeliveryRuntimeSettings {
                inflight_ttl_ms: 1,
                expiry_poll_min_ms: 2,
                expiry_batch_max: 3,
                delivery_poll_max_ms: 4,
            },
            idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
                enabled: true,
                evict_after_ms: 5,
                sweep_interval_ms: 6,
                publisher_idle_timeout_ms: Some(7),
            },
            connection: ConnectionRuntimeSettings {
                reconnect_grace_ms: Some(8),
            },
            replication: ReplicationRuntimeSettings::default(),
            partitioning: PartitioningRuntimeSettings::default(),
            consumer_groups: ConsumerGroupRuntimeSettings::default(),
        };

        let decoded = decode_snapshot(GlobalValue {
            version: 9,
            bytes: encode_settings(&settings).unwrap(),
        })
        .unwrap();

        assert_eq!(
            decoded,
            RuntimeSettingsSnapshot {
                version: 9,
                settings,
            }
        );
    }

    #[test]
    fn broker_config_uses_runtime_settings() {
        let settings = RuntimeSettings {
            delivery: DeliveryRuntimeSettings {
                inflight_ttl_ms: 10,
                expiry_poll_min_ms: 11,
                expiry_batch_max: 12,
                delivery_poll_max_ms: 13,
            },
            idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
                enabled: true,
                evict_after_ms: 14,
                sweep_interval_ms: 15,
                publisher_idle_timeout_ms: Some(16),
            },
            connection: ConnectionRuntimeSettings {
                reconnect_grace_ms: Some(17),
            },
            replication: ReplicationRuntimeSettings::default(),
            partitioning: PartitioningRuntimeSettings::default(),
            consumer_groups: ConsumerGroupRuntimeSettings::default(),
        };

        let config = BrokerConfig::from_runtime_settings(&settings);

        assert_eq!(config.inflight_ttl_ms, 10);
        assert_eq!(config.expiry_poll_min_ms, 11);
        assert_eq!(config.expiry_batch_max, 12);
        assert_eq!(config.delivery_poll_max_ms, 13);
        assert_eq!(config.queue_idle_evict_after_ms, Some(14));
        assert_eq!(config.queue_idle_sweep_interval_ms, 15);
    }

    #[test]
    fn runtime_settings_validate_rejects_cluster_runtime_zeroes() {
        let mut settings = RuntimeSettings::default();

        settings.replication.caught_up_poll_ms = 0;
        assert!(matches!(
            settings.validate(),
            Err(RuntimeSettingsError::Invalid(message))
                if message.contains("replication poll intervals")
        ));

        settings = RuntimeSettings::default();
        settings.replication.min_in_sync_replicas = 0;
        assert!(matches!(
            settings.validate(),
            Err(RuntimeSettingsError::Invalid(message))
                if message.contains("min_in_sync_replicas")
        ));

        settings = RuntimeSettings::default();
        settings.replication.isr_timeout_ms = 0;
        assert!(matches!(
            settings.validate(),
            Err(RuntimeSettingsError::Invalid(message))
                if message.contains("isr_timeout_ms")
        ));

        settings = RuntimeSettings::default();
        settings.partitioning.default_partition_count = 0;
        assert!(matches!(
            settings.validate(),
            Err(RuntimeSettingsError::Invalid(message))
                if message.contains("default_partition_count")
        ));
    }

    #[tokio::test]
    async fn manager_seeds_missing_store_and_loads_persisted_settings() {
        let dir = test_dir!("runtime_settings_seed");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();

        let first = RuntimeSettings {
            delivery: DeliveryRuntimeSettings {
                inflight_ttl_ms: 1,
                ..DeliveryRuntimeSettings::default()
            },
            ..RuntimeSettings::default()
        };
        let manager = RuntimeSettingsManager::load_from_store(
            store.clone(),
            first.clone(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(manager.current().version, 1);
        assert_eq!(manager.current().settings, first);

        let second = RuntimeSettings {
            delivery: DeliveryRuntimeSettings {
                inflight_ttl_ms: 999,
                ..DeliveryRuntimeSettings::default()
            },
            ..RuntimeSettings::default()
        };
        let manager = RuntimeSettingsManager::load_from_store(store, second, Default::default())
            .await
            .unwrap();
        assert_eq!(manager.current().version, 1);
        assert_eq!(manager.current().settings, first);
    }

    #[tokio::test]
    async fn manager_uses_seed_when_persisted_settings_are_corrupt() {
        let dir = test_dir!("runtime_settings_corrupt");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();
        store
            .put(runtime_settings_key().unwrap(), vec![0, 1, 2], Some(0))
            .await
            .unwrap();

        let seed = RuntimeSettings {
            delivery: DeliveryRuntimeSettings {
                inflight_ttl_ms: 42,
                ..DeliveryRuntimeSettings::default()
            },
            ..RuntimeSettings::default()
        };
        let manager =
            RuntimeSettingsManager::load_from_store(store, seed.clone(), Default::default())
                .await
                .unwrap();

        assert_eq!(manager.current().version, 1);
        assert_eq!(manager.current().settings, seed);
        let issue = manager.load_issue().expect("corrupt settings are reported");
        assert_eq!(issue.version, 1);
        assert!(issue.message.contains("decode runtime settings"));
    }

    #[tokio::test]
    async fn update_replaces_corrupt_persisted_settings_at_reported_version() {
        let dir = test_dir!("runtime_settings_corrupt_replace");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();
        store
            .put(runtime_settings_key().unwrap(), vec![0, 1, 2], Some(0))
            .await
            .unwrap();

        let manager = RuntimeSettingsManager::load_from_store(
            store.clone(),
            RuntimeSettings::default(),
            Default::default(),
        )
        .await
        .unwrap();
        let mut replacement = manager.current().settings;
        replacement.delivery.inflight_ttl_ms = 12_345;

        let outcome = manager.update(1, replacement.clone()).await.unwrap();

        assert_eq!(
            outcome,
            RuntimeSettingsUpdateOutcome::Stored(RuntimeSettingsSnapshot {
                version: 2,
                settings: replacement.clone(),
            })
        );
        assert_eq!(manager.load_issue(), None);

        let reloaded = RuntimeSettingsManager::load_from_store(
            store,
            RuntimeSettings::default(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(reloaded.current().version, 2);
        assert_eq!(reloaded.current().settings, replacement);
        assert_eq!(reloaded.load_issue(), None);
    }

    #[tokio::test]
    async fn locked_idle_queue_cleanup_uses_seed_value() {
        let dir = test_dir!("runtime_settings_locks");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();

        let persisted = RuntimeSettings {
            idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
                enabled: true,
                evict_after_ms: 1,
                sweep_interval_ms: 2,
                publisher_idle_timeout_ms: Some(3),
            },
            ..RuntimeSettings::default()
        };
        RuntimeSettingsManager::load_from_store(store.clone(), persisted, Default::default())
            .await
            .unwrap();

        let seed = RuntimeSettings {
            idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
                enabled: false,
                evict_after_ms: 10,
                sweep_interval_ms: 20,
                publisher_idle_timeout_ms: None,
            },
            ..RuntimeSettings::default()
        };
        let manager = RuntimeSettingsManager::load_from_store(
            store,
            seed.clone(),
            RuntimeSettingsLocks {
                idle_queue_cleanup: true,
            },
        )
        .await
        .unwrap();

        assert_eq!(
            manager.current().settings.idle_queue_cleanup,
            seed.idle_queue_cleanup
        );
    }

    #[tokio::test]
    async fn update_stores_settings_with_expected_version() {
        let dir = test_dir!("runtime_settings_update");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();
        let manager = RuntimeSettingsManager::load_from_store(
            store,
            RuntimeSettings::default(),
            Default::default(),
        )
        .await
        .unwrap();

        let mut updated = manager.current().settings;
        updated.delivery.inflight_ttl_ms = 123;

        let outcome = manager.update(1, updated.clone()).await.unwrap();

        assert_eq!(
            outcome,
            RuntimeSettingsUpdateOutcome::Stored(RuntimeSettingsSnapshot {
                version: 2,
                settings: updated.clone(),
            })
        );
        assert_eq!(manager.current().version, 2);
        assert_eq!(manager.current().settings, updated);
    }

    #[tokio::test]
    async fn update_conflict_returns_current_effective_settings() {
        let dir = test_dir!("runtime_settings_update_conflict");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();
        let manager = RuntimeSettingsManager::load_from_store(
            store,
            RuntimeSettings::default(),
            Default::default(),
        )
        .await
        .unwrap();

        let mut first_update = manager.current().settings;
        first_update.delivery.inflight_ttl_ms = 111;
        manager.update(1, first_update.clone()).await.unwrap();

        let mut stale_update = first_update.clone();
        stale_update.delivery.inflight_ttl_ms = 222;
        let outcome = manager.update(1, stale_update).await.unwrap();

        assert_eq!(
            outcome,
            RuntimeSettingsUpdateOutcome::Conflict(RuntimeSettingsSnapshot {
                version: 2,
                settings: first_update,
            })
        );
    }

    #[tokio::test]
    async fn apply_cluster_settings_retries_against_local_cache_version() {
        let dir = test_dir!("runtime_settings_apply_cluster");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();
        let manager = RuntimeSettingsManager::load_from_store(
            store,
            RuntimeSettings::default(),
            Default::default(),
        )
        .await
        .unwrap();

        let mut local = manager.current().settings;
        local.delivery.inflight_ttl_ms = 111;
        manager.update(1, local).await.unwrap();

        let mut cluster = RuntimeSettings::default();
        cluster.delivery.inflight_ttl_ms = 222;
        let snapshot = manager
            .apply_cluster_settings(cluster.clone())
            .await
            .unwrap();

        assert_eq!(snapshot.version, 3);
        assert_eq!(snapshot.settings, cluster);
        assert_eq!(manager.current().settings.delivery.inflight_ttl_ms, 222);
    }

    #[tokio::test]
    async fn update_preserves_locked_persisted_sections() {
        let dir = test_dir!("runtime_settings_update_locks");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();

        let persisted = RuntimeSettings {
            idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
                enabled: true,
                evict_after_ms: 1,
                sweep_interval_ms: 2,
                publisher_idle_timeout_ms: Some(3),
            },
            ..RuntimeSettings::default()
        };
        RuntimeSettingsManager::load_from_store(
            store.clone(),
            persisted.clone(),
            Default::default(),
        )
        .await
        .unwrap();

        let seed = RuntimeSettings {
            idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
                enabled: false,
                evict_after_ms: 10,
                sweep_interval_ms: 20,
                publisher_idle_timeout_ms: None,
            },
            ..RuntimeSettings::default()
        };
        let manager = RuntimeSettingsManager::load_from_store(
            store.clone(),
            seed.clone(),
            RuntimeSettingsLocks {
                idle_queue_cleanup: true,
            },
        )
        .await
        .unwrap();

        let mut update = manager.current().settings;
        update.delivery.inflight_ttl_ms = 123;
        let outcome = manager.update(1, update.clone()).await.unwrap();

        assert_eq!(
            outcome,
            RuntimeSettingsUpdateOutcome::Stored(RuntimeSettingsSnapshot {
                version: 2,
                settings: update,
            })
        );

        let unlocked = RuntimeSettingsManager::load_from_store(
            store,
            RuntimeSettings::default(),
            Default::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            unlocked.current().settings.idle_queue_cleanup,
            persisted.idle_queue_cleanup
        );
        assert_eq!(unlocked.current().settings.delivery.inflight_ttl_ms, 123);
    }

    #[tokio::test]
    async fn update_rejects_locked_section_changes() {
        let dir = test_dir!("runtime_settings_update_rejects_locks");
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let store = stroma.global_store().await.unwrap();
        let manager = RuntimeSettingsManager::load_from_store(
            store,
            RuntimeSettings::default(),
            RuntimeSettingsLocks {
                idle_queue_cleanup: true,
            },
        )
        .await
        .unwrap();

        let mut update = manager.current().settings;
        update.idle_queue_cleanup.enabled = true;

        let err = manager.update(1, update).await.unwrap_err();

        assert!(matches!(err, RuntimeSettingsError::Locked(_)));
    }
}
