//! Broker credential store.
//!
//! Users are cluster-shared data: one document (name to argon2 hash),
//! persisted in the global store the same way the runtime-settings document
//! is, and replicated through coordination in cluster mode. It is a separate
//! document rather than a settings section so password hashes never ride the
//! settings JSON surface (admin API, dashboard) and a settings update can
//! never clobber users. Node-to-node trust is deliberately NOT derived from
//! this store - nodes authenticate with the cluster secret, users are data
//! that replicates after membership trust exists.

use std::collections::BTreeMap;
use std::sync::Arc;

use argon2::Argon2;
use argon2::password_hash::{
    PasswordHash, PasswordHasher, PasswordVerifier, SaltString, rand_core::OsRng,
};
use serde::{Deserialize, Serialize};
use stroma_core::{GlobalKey, GlobalStore, PutOutcome, StromaError};
use tokio::sync::watch;

use fibril_util::unix_millis;

const USERS_NAMESPACE: &str = "fibril";
const USERS_KEY: &str = "auth_users";
/// Bounded CAS retries for concurrent user edits before reporting conflict.
const UPDATE_RETRIES: usize = 8;

#[derive(Debug, thiserror::Error)]
pub enum UserStoreError {
    #[error("user store storage error: {0}")]
    Storage(#[from] StromaError),
    #[error("user store document could not be decoded: {0}")]
    Decode(String),
    #[error("user store document could not be encoded: {0}")]
    Encode(String),
    #[error("password hashing failed: {0}")]
    Hash(String),
    #[error("user store update conflicted repeatedly, try again")]
    Conflict,
    #[error("unknown user `{0}`")]
    UnknownUser(String),
    #[error("invalid username: {0}")]
    InvalidUsername(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserRecord {
    /// Argon2 hash in PHC string format. Never a plaintext password.
    pub password_hash: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UserDocument {
    pub users: BTreeMap<String, UserRecord>,
}

/// Listing entry for admin surfaces: identity and timestamps, no hash.
#[derive(Debug, Clone, Serialize)]
pub struct UserInfo {
    pub username: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

#[derive(Debug, Clone)]
pub struct UserSnapshot {
    pub version: u64,
    pub document: Arc<UserDocument>,
}

/// Durable user store with a watch snapshot for lock-free verification.
pub struct UserStoreManager {
    store: Arc<GlobalStore>,
    current: watch::Sender<UserSnapshot>,
}

impl UserStoreManager {
    /// Load the user document, seeding it with the given plaintext
    /// credentials only when no document exists yet (first boot). After
    /// that the persisted document owns the users, matching runtime
    /// settings semantics.
    pub async fn load_from_store(
        store: Arc<GlobalStore>,
        seed: &[(String, String)],
    ) -> Result<Self, UserStoreError> {
        let key = users_key();
        let mut loaded = store.get(&key).await?;
        if loaded.is_none() && !seed.is_empty() {
            let mut document = UserDocument::default();
            let now = unix_millis();
            for (username, password) in seed {
                let username = validate_username(username)?;
                document.users.insert(
                    username,
                    UserRecord {
                        password_hash: hash_password(password)?,
                        created_ms: now,
                        updated_ms: now,
                    },
                );
            }
            let bytes = encode_document(&document)?;
            match store.put(key.clone(), bytes, Some(0)).await? {
                PutOutcome::Stored { .. } => {}
                // Another loader seeded concurrently, adopt what it stored.
                PutOutcome::Conflict { .. } => {}
            }
            loaded = store.get(&key).await?;
        }

        let snapshot = match loaded {
            Some(value) => UserSnapshot {
                version: value.version,
                document: Arc::new(decode_document(&value.bytes)?),
            },
            None => UserSnapshot {
                version: 0,
                document: Arc::new(UserDocument::default()),
            },
        };
        let (current, _) = watch::channel(snapshot);
        Ok(Self { store, current })
    }

    pub fn snapshot(&self) -> UserSnapshot {
        self.current.borrow().clone()
    }

    /// Verify credentials against the current document. Argon2 verification
    /// is CPU-bound, so it runs on the blocking pool: authentication is a
    /// per-connection cold path and must not stall the runtime under a
    /// connection storm.
    pub async fn verify(&self, username: &str, password: &str) -> bool {
        let Some(record) = self.current.borrow().document.users.get(username).cloned() else {
            return false;
        };
        let password = password.to_string();
        tokio::task::spawn_blocking(move || {
            PasswordHash::new(&record.password_hash)
                .map(|hash| {
                    Argon2::default()
                        .verify_password(password.as_bytes(), &hash)
                        .is_ok()
                })
                .unwrap_or(false)
        })
        .await
        .unwrap_or(false)
    }

    /// Whether any users exist. Gates behavior like the loopback-only
    /// default-credentials rule.
    pub fn has_users(&self) -> bool {
        !self.current.borrow().document.users.is_empty()
    }

    pub fn list(&self) -> Vec<UserInfo> {
        self.current
            .borrow()
            .document
            .users
            .iter()
            .map(|(username, record)| UserInfo {
                username: username.clone(),
                created_ms: record.created_ms,
                updated_ms: record.updated_ms,
            })
            .collect()
    }

    /// Create or update a user (idempotent on the name, replaces the hash).
    pub async fn upsert_user(&self, username: &str, password: &str) -> Result<(), UserStoreError> {
        let username = validate_username(username)?;
        let password_hash = hash_password(password)?;
        self.mutate(move |document| {
            let now = unix_millis();
            document
                .users
                .entry(username.clone())
                .and_modify(|record| {
                    record.password_hash = password_hash.clone();
                    record.updated_ms = now;
                })
                .or_insert_with(|| UserRecord {
                    password_hash: password_hash.clone(),
                    created_ms: now,
                    updated_ms: now,
                });
            Ok(())
        })
        .await
    }

    pub async fn remove_user(&self, username: &str) -> Result<(), UserStoreError> {
        let username = username.to_string();
        self.mutate(move |document| {
            if document.users.remove(&username).is_none() {
                return Err(UserStoreError::UnknownUser(username.clone()));
            }
            Ok(())
        })
        .await
    }

    /// Apply a document edit under CAS, retrying on concurrent writers.
    async fn mutate<F>(&self, edit: F) -> Result<(), UserStoreError>
    where
        F: Fn(&mut UserDocument) -> Result<(), UserStoreError>,
    {
        let key = users_key();
        for _ in 0..UPDATE_RETRIES {
            let current = self.store.get(&key).await?;
            let (version, mut document) = match &current {
                Some(value) => (value.version, decode_document(&value.bytes)?),
                None => (0, UserDocument::default()),
            };
            edit(&mut document)?;
            let bytes = encode_document(&document)?;
            match self.store.put(key.clone(), bytes, Some(version)).await? {
                PutOutcome::Stored { version } => {
                    self.current.send_replace(UserSnapshot {
                        version,
                        document: Arc::new(document),
                    });
                    return Ok(());
                }
                PutOutcome::Conflict { .. } => continue,
            }
        }
        Err(UserStoreError::Conflict)
    }

    /// Replace the local snapshot with a document decided elsewhere (the
    /// cluster-authoritative copy in coordination), persisting it locally.
    pub async fn adopt_document(
        &self,
        version: u64,
        document: UserDocument,
    ) -> Result<(), UserStoreError> {
        let key = users_key();
        let bytes = encode_document(&document)?;
        // Unconditional put: coordination owns the decision, local storage
        // is a durable cache of it.
        self.store.put(key, bytes, None).await?;
        self.current.send_replace(UserSnapshot {
            version,
            document: Arc::new(document),
        });
        Ok(())
    }
}

fn users_key() -> GlobalKey {
    GlobalKey {
        namespace: USERS_NAMESPACE.to_string(),
        key: USERS_KEY.to_string(),
    }
}

fn encode_document(document: &UserDocument) -> Result<Vec<u8>, UserStoreError> {
    serde_json::to_vec(document).map_err(|err| UserStoreError::Encode(err.to_string()))
}

fn decode_document(bytes: &[u8]) -> Result<UserDocument, UserStoreError> {
    serde_json::from_slice(bytes).map_err(|err| UserStoreError::Decode(err.to_string()))
}

pub fn hash_password(password: &str) -> Result<String, UserStoreError> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(|err| UserStoreError::Hash(err.to_string()))
}

fn validate_username(username: &str) -> Result<String, UserStoreError> {
    let trimmed = username.trim();
    if trimmed.is_empty() {
        return Err(UserStoreError::InvalidUsername(
            "username must not be empty".to_string(),
        ));
    }
    if trimmed.len() > 128 {
        return Err(UserStoreError::InvalidUsername(
            "username must be at most 128 bytes".to_string(),
        ));
    }
    if trimmed.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return Err(UserStoreError::InvalidUsername(
            "username must not contain whitespace or control characters".to_string(),
        ));
    }
    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use stroma_core::{
        KeratinConfig, SnapshotConfig, Stroma, StromaKeratinConfig, TempDir, test_dir,
    };

    async fn open_store(tag: &str) -> (Arc<GlobalStore>, TempDir) {
        let dir = test_dir!(tag);
        let stroma = Stroma::open(
            &dir.root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .expect("open stroma");
        let store = stroma.global_store().await.expect("global store");
        (store, dir)
    }

    #[tokio::test]
    async fn seeds_only_an_empty_store_and_verifies() {
        let (store, _dir) = open_store("auth-seed").await;
        let manager = UserStoreManager::load_from_store(
            store.clone(),
            &[("admin".to_string(), "first-password".to_string())],
        )
        .await
        .expect("load");

        assert!(manager.verify("admin", "first-password").await);
        assert!(!manager.verify("admin", "wrong").await);
        assert!(!manager.verify("ghost", "first-password").await);

        // A reload with a different seed keeps the persisted document.
        let manager = UserStoreManager::load_from_store(
            store,
            &[("admin".to_string(), "other-password".to_string())],
        )
        .await
        .expect("reload");
        assert!(manager.verify("admin", "first-password").await);
        assert!(!manager.verify("admin", "other-password").await);
    }

    #[tokio::test]
    async fn upsert_and_remove_round_trip() {
        let (store, _dir) = open_store("auth-crud").await;
        let manager = UserStoreManager::load_from_store(store, &[])
            .await
            .expect("load");
        assert!(!manager.has_users());

        manager
            .upsert_user("worker", "secret-1")
            .await
            .expect("create");
        assert!(manager.has_users());
        assert!(manager.verify("worker", "secret-1").await);

        manager
            .upsert_user("worker", "secret-2")
            .await
            .expect("rotate");
        assert!(manager.verify("worker", "secret-2").await);
        assert!(!manager.verify("worker", "secret-1").await);
        assert_eq!(manager.list().len(), 1);

        manager.remove_user("worker").await.expect("remove");
        assert!(!manager.verify("worker", "secret-2").await);
        assert!(matches!(
            manager.remove_user("worker").await,
            Err(UserStoreError::UnknownUser(_))
        ));
    }

    #[tokio::test]
    async fn snapshot_and_list_never_expose_hashes() {
        let (store, _dir) = open_store("auth-list").await;
        let manager = UserStoreManager::load_from_store(store, &[])
            .await
            .expect("load");
        manager.upsert_user("ops", "hunter2").await.expect("create");

        let listed = serde_json::to_string(&manager.list()).expect("encode listing");
        assert!(!listed.contains("hash"), "{listed}");
        assert!(!listed.to_lowercase().contains("argon2"), "{listed}");
    }

    #[test]
    fn usernames_are_validated() {
        assert!(validate_username("worker").is_ok());
        assert!(validate_username("  padded  ").is_ok());
        assert!(validate_username("").is_err());
        assert!(validate_username("two words").is_err());
        assert!(validate_username(&"x".repeat(200)).is_err());
    }
}
