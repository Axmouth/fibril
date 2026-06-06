use std::sync::Arc;

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

/// Loads, validates, and exposes the effective runtime settings document.
#[derive(Debug)]
pub struct RuntimeSettingsManager {
    store: Arc<GlobalStore>,
    current: watch::Sender<RuntimeSettingsSnapshot>,
    seed: RuntimeSettings,
    locks: RuntimeSettingsLocks,
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
        let snapshot = load_or_seed_settings(&store, &seed, &locks).await?;
        let (current, _) = watch::channel(snapshot);
        Ok(Self {
            store,
            current,
            seed,
            locks,
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

    pub async fn update(
        &self,
        expected_version: u64,
        settings: RuntimeSettings,
    ) -> Result<RuntimeSettingsUpdateOutcome, RuntimeSettingsError> {
        settings.validate()?;

        let key = runtime_settings_key()?;
        let current_raw = match self.store.get(&key).await? {
            Some(value) => decode_snapshot(value)?,
            None => {
                return Err(RuntimeSettingsError::StoreConflict(
                    "runtime settings disappeared before update".into(),
                ));
            }
        };
        let current_effective = effective_snapshot(current_raw.clone(), &self.seed, &self.locks)?;
        if expected_version != current_raw.version {
            return Ok(RuntimeSettingsUpdateOutcome::Conflict(current_effective));
        }

        let mut persisted_settings = settings.clone();
        if self.locks.idle_queue_cleanup {
            if settings.idle_queue_cleanup != current_effective.settings.idle_queue_cleanup {
                return Err(RuntimeSettingsError::Locked(
                    "idle_queue_cleanup is locked by boot config".into(),
                ));
            }
            persisted_settings.idle_queue_cleanup = current_raw.settings.idle_queue_cleanup;
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
        }
    }
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
) -> Result<RuntimeSettingsSnapshot, RuntimeSettingsError> {
    let key = runtime_settings_key()?;
    let snapshot = match store.get(&key).await? {
        Some(value) => decode_snapshot(value)?,
        None => {
            let bytes = encode_settings(seed)?;
            match store.put(key.clone(), bytes, Some(0)).await? {
                PutOutcome::Stored { version } => RuntimeSettingsSnapshot {
                    version,
                    settings: seed.clone(),
                },
                PutOutcome::Conflict {
                    current: Some(value),
                } => decode_snapshot(value)?,
                PutOutcome::Conflict { current: None } => match store.get(&key).await? {
                    Some(value) => decode_snapshot(value)?,
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

    effective_snapshot(snapshot, &seed, locks)
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
    use stroma_core::{KeratinConfig, SnapshotConfig, Stroma, test_dir};

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
        };

        let config = BrokerConfig::from_runtime_settings(&settings);

        assert_eq!(config.inflight_ttl_ms, 10);
        assert_eq!(config.expiry_poll_min_ms, 11);
        assert_eq!(config.expiry_batch_max, 12);
        assert_eq!(config.delivery_poll_max_ms, 13);
        assert_eq!(config.queue_idle_evict_after_ms, Some(14));
        assert_eq!(config.queue_idle_sweep_interval_ms, 15);
    }

    #[tokio::test]
    async fn manager_seeds_missing_store_and_loads_persisted_settings() {
        let dir = test_dir!("runtime_settings_seed");
        let stroma = Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
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
    async fn locked_idle_queue_cleanup_uses_seed_value() {
        let dir = test_dir!("runtime_settings_locks");
        let stroma = Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
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
            KeratinConfig::test_default(),
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
            KeratinConfig::test_default(),
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
    async fn update_preserves_locked_persisted_sections() {
        let dir = test_dir!("runtime_settings_update_locks");
        let stroma = Stroma::open(
            &dir.root,
            KeratinConfig::test_default(),
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
            KeratinConfig::test_default(),
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
