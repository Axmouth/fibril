//! Durable resume-session skeletons.
//!
//! A resume session lives in process memory (the `ResumeSessionRegistry`), so a
//! broker restart normally voids every session and a reconnecting client is
//! rejected. This store persists the small IDENTITY of each session - the
//! registry `owner_id`, and per-session `client_id`, `resume_token`, and the
//! subscription set - to the node's durable global store, so a fast restart can
//! honor a resume and let the client reconcile instead of getting a bare
//! rejection. Messages redeliver per at-least-once as they do today, and
//! delivery tags still die with the process (the client marks held deliveries
//! stale on a non-resumed outcome).
//!
//! This is broker-LOCAL durable state, deliberately not coordination
//! replicated: only the owning node can honor these sessions, and a moved
//! partition is already covered by topology-driven resubscribe.
//!
//! Persisted as one document under a single global key (mirroring the user
//! store and runtime settings), so the whole session set is read back at boot
//! and rewritten under CAS on change. Session churn is a cold path relative to
//! message traffic, so the whole-document write is acceptable at the session
//! counts a single node holds.

use std::collections::BTreeMap;
use std::sync::Arc;

use fibril_broker::queue_engine::{GlobalKey, GlobalStore, PutOutcome, StromaEngine, StromaError};
use fibril_wire::ReconcileSubscription;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use uuid::Uuid;

use fibril_util::unix_millis;

const SESSIONS_NAMESPACE: &str = "fibril";
const SESSIONS_KEY: &str = "resume_sessions";
/// Bounded CAS retries for concurrent session edits before giving up (the
/// caller logs and proceeds - a lost skeleton write only costs a resume, never
/// correctness).
const UPDATE_RETRIES: usize = 8;
/// Document envelope version, so the on-disk format can evolve behind a
/// version check the way runtime settings do.
const ENVELOPE_VERSION: u16 = 1;

#[derive(Debug, thiserror::Error)]
pub enum SessionStoreError {
    #[error("session store storage error: {0}")]
    Storage(#[from] StromaError),
    #[error("session store document could not be decoded: {0}")]
    Decode(String),
    #[error("session store document could not be encoded: {0}")]
    Encode(String),
    #[error("session store update conflicted repeatedly")]
    Conflict,
}

/// One persisted session's identity and last-known subscription set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionSkeleton {
    pub resume_token: Uuid,
    /// The session's subscriptions as of the last write. Best-effort: the
    /// client re-drives reconcile on reconnect, so a slightly stale set never
    /// breaks correctness, it just records what the session held.
    #[serde(default)]
    pub subscriptions: Vec<ReconcileSubscription>,
    /// Wall-clock write time, used to expire skeletons past the restart TTL.
    pub updated_ms: u64,
}

/// The whole persisted session set plus the registry owner identity. Storing
/// `owner_id` here is what lets a restarted broker keep its identity, so a
/// client's cached resume identity still matches after a bounce.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SessionDocument {
    owner_id: Uuid,
    sessions: BTreeMap<Uuid, SessionSkeleton>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionEnvelope {
    version: u16,
    document: SessionDocument,
}

/// Durable resume-session skeleton store.
pub struct SessionSkeletonStore {
    store: Arc<GlobalStore>,
    /// Cached view of the persisted document, refreshed on every successful
    /// write. Reads (lookup at resume time) hit this, not storage.
    current: RwLock<Cached>,
    /// The owner identity in effect for this process: loaded from the document
    /// on a restart, or freshly minted and persisted on first boot.
    owner_id: Uuid,
}

#[derive(Debug, Clone)]
struct Cached {
    document: SessionDocument,
}

impl SessionSkeletonStore {
    /// Load the session store from the node's global store, adopting the
    /// persisted `owner_id` if one exists or minting and persisting a fresh one
    /// on first boot.
    pub async fn load_from_stroma_engine(
        engine: &StromaEngine,
        owner_id_if_new: Uuid,
    ) -> Result<Self, SessionStoreError> {
        let store = engine.global_store().await?;
        Self::load_from_store(store, owner_id_if_new).await
    }

    pub async fn load_from_store(
        store: Arc<GlobalStore>,
        owner_id_if_new: Uuid,
    ) -> Result<Self, SessionStoreError> {
        let key = sessions_key();
        let loaded = store.get(&key).await?;
        let document = match loaded {
            Some(value) => decode_document(&value.bytes)?,
            None => {
                // First boot: persist the freshly minted owner identity so a
                // later restart adopts it. An empty session set is fine.
                let document = SessionDocument {
                    owner_id: owner_id_if_new,
                    sessions: BTreeMap::new(),
                };
                let bytes = encode_document(&document)?;
                match store.put(key.clone(), bytes, Some(0)).await? {
                    PutOutcome::Stored { .. } => document,
                    // Another loader seeded concurrently, adopt what it stored.
                    PutOutcome::Conflict { current } => match current {
                        Some(value) => decode_document(&value.bytes)?,
                        None => document,
                    },
                }
            }
        };
        let owner_id = document.owner_id;
        Ok(Self {
            store,
            current: RwLock::new(Cached { document }),
            owner_id,
        })
    }

    /// The registry owner identity this node runs under (persisted, so it
    /// survives a restart).
    pub fn owner_id(&self) -> Uuid {
        self.owner_id
    }

    /// Look up a persisted skeleton by client id, returning it only when the
    /// resume token matches and it is within `ttl_ms` of now. A `ttl_ms` of 0
    /// disables restart resume entirely (no skeleton is ever honored).
    pub async fn lookup(
        &self,
        client_id: &Uuid,
        resume_token: &Uuid,
        ttl_ms: u64,
    ) -> Option<SessionSkeleton> {
        if ttl_ms == 0 {
            return None;
        }
        let cached = self.current.read().await;
        let skeleton = cached.document.sessions.get(client_id)?;
        if skeleton.resume_token != *resume_token {
            return None;
        }
        let now = unix_millis();
        if now.saturating_sub(skeleton.updated_ms) > ttl_ms {
            return None;
        }
        Some(skeleton.clone())
    }

    /// Persist (create or replace) a session skeleton. Best-effort by contract:
    /// the caller treats an error as "the session just will not survive a
    /// restart", never a failure of the underlying subscribe.
    pub async fn upsert(
        &self,
        client_id: Uuid,
        resume_token: Uuid,
        subscriptions: Vec<ReconcileSubscription>,
    ) -> Result<(), SessionStoreError> {
        self.mutate(move |document| {
            document.sessions.insert(
                client_id,
                SessionSkeleton {
                    resume_token,
                    subscriptions: subscriptions.clone(),
                    updated_ms: unix_millis(),
                },
            );
        })
        .await
    }

    /// Drop a session skeleton (clean session end). A no-op when absent.
    pub async fn remove(&self, client_id: &Uuid) -> Result<(), SessionStoreError> {
        let client_id = *client_id;
        self.mutate(move |document| {
            document.sessions.remove(&client_id);
        })
        .await
    }

    /// Apply a document edit under CAS, retrying on a concurrent writer. The
    /// owner identity is preserved across edits (never rewritten here).
    async fn mutate<F>(&self, edit: F) -> Result<(), SessionStoreError>
    where
        F: Fn(&mut SessionDocument),
    {
        let key = sessions_key();
        for _ in 0..UPDATE_RETRIES {
            let current = self.store.get(&key).await?;
            let (version, mut document) = match &current {
                Some(value) => (value.version, decode_document(&value.bytes)?),
                None => (
                    0,
                    SessionDocument {
                        owner_id: self.owner_id,
                        sessions: BTreeMap::new(),
                    },
                ),
            };
            edit(&mut document);
            let bytes = encode_document(&document)?;
            match self.store.put(key.clone(), bytes, Some(version)).await? {
                PutOutcome::Stored { .. } => {
                    *self.current.write().await = Cached { document };
                    return Ok(());
                }
                PutOutcome::Conflict { .. } => continue,
            }
        }
        Err(SessionStoreError::Conflict)
    }
}

fn sessions_key() -> GlobalKey {
    GlobalKey {
        namespace: SESSIONS_NAMESPACE.to_string(),
        key: SESSIONS_KEY.to_string(),
    }
}

fn encode_document(document: &SessionDocument) -> Result<Vec<u8>, SessionStoreError> {
    rmp_serde::to_vec_named(&SessionEnvelope {
        version: ENVELOPE_VERSION,
        document: document.clone(),
    })
    .map_err(|err| SessionStoreError::Encode(err.to_string()))
}

fn decode_document(bytes: &[u8]) -> Result<SessionDocument, SessionStoreError> {
    let envelope: SessionEnvelope =
        rmp_serde::from_slice(bytes).map_err(|err| SessionStoreError::Decode(err.to_string()))?;
    if envelope.version != ENVELOPE_VERSION {
        return Err(SessionStoreError::Decode(format!(
            "unsupported session document version {}",
            envelope.version
        )));
    }
    Ok(envelope.document)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fibril_broker::storage::Partition;
    use stroma_core::{KeratinConfig, SnapshotConfig, Stroma, StromaKeratinConfig, TempDir};

    async fn open_store(tag: &str) -> (Arc<GlobalStore>, TempDir) {
        let dir = TempDir {
            root: std::env::current_dir()
                .unwrap()
                .join("test_data")
                .join(format!("session-store-{tag}-{}", Uuid::now_v7())),
        };
        std::fs::create_dir_all(&dir.root).unwrap();
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

    fn sub(sub_id: u64) -> ReconcileSubscription {
        ReconcileSubscription {
            sub_id,
            topic: "jobs".into(),
            group: None,
            partition: Partition::new(0),
            auto_ack: false,
            prefetch: 1,
            consumer_group: None,
            consumer_target: None,
            member_id: None,
        }
    }

    #[tokio::test]
    async fn owner_id_persists_across_reload() {
        let (store, _dir) = open_store("session-owner").await;
        let minted = Uuid::from_u128(1);
        let first = SessionSkeletonStore::load_from_store(store.clone(), minted)
            .await
            .expect("load");
        assert_eq!(first.owner_id(), minted);

        // A reload with a DIFFERENT freshly minted id keeps the persisted one -
        // this is what lets a restarted broker retain its identity.
        let reloaded = SessionSkeletonStore::load_from_store(store, Uuid::from_u128(2))
            .await
            .expect("reload");
        assert_eq!(reloaded.owner_id(), minted);
    }

    #[tokio::test]
    async fn upsert_lookup_and_ttl() {
        let (store, _dir) = open_store("session-ttl").await;
        let manager = SessionSkeletonStore::load_from_store(store, Uuid::from_u128(1))
            .await
            .expect("load");

        let client = Uuid::from_u128(10);
        let token = Uuid::from_u128(20);
        manager
            .upsert(client, token, vec![sub(1)])
            .await
            .expect("upsert");

        // Present within TTL with the right token.
        let found = manager.lookup(&client, &token, 60_000).await;
        assert_eq!(found.map(|s| s.subscriptions.len()), Some(1));

        // Wrong token never matches.
        assert!(
            manager
                .lookup(&client, &Uuid::from_u128(99), 60_000)
                .await
                .is_none()
        );

        // A zero TTL disables restart resume entirely.
        assert!(manager.lookup(&client, &token, 0).await.is_none());

        // A skeleton older than the TTL window expires: sleep past a small
        // window and confirm the lookup no longer honors it.
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(
            manager.lookup(&client, &token, 5).await.is_none(),
            "a skeleton older than the ttl must not be honored",
        );
        // But a generous window still honors the same skeleton.
        assert!(manager.lookup(&client, &token, 60_000).await.is_some());
    }

    #[tokio::test]
    async fn remove_drops_the_skeleton() {
        let (store, _dir) = open_store("session-remove").await;
        let manager = SessionSkeletonStore::load_from_store(store, Uuid::from_u128(1))
            .await
            .expect("load");
        let client = Uuid::from_u128(10);
        let token = Uuid::from_u128(20);
        manager.upsert(client, token, vec![]).await.expect("upsert");
        assert!(manager.lookup(&client, &token, 60_000).await.is_some());
        manager.remove(&client).await.expect("remove");
        assert!(manager.lookup(&client, &token, 60_000).await.is_none());
        // Removing an absent session is fine.
        manager.remove(&client).await.expect("remove-absent");
    }

    #[tokio::test]
    async fn skeletons_survive_a_reload() {
        let (store, _dir) = open_store("session-reload").await;
        let client = Uuid::from_u128(10);
        let token = Uuid::from_u128(20);
        {
            let manager = SessionSkeletonStore::load_from_store(store.clone(), Uuid::from_u128(1))
                .await
                .expect("load");
            manager
                .upsert(client, token, vec![sub(7)])
                .await
                .expect("upsert");
        }
        // A fresh manager over the same store (a "restart") sees the skeleton.
        let reloaded = SessionSkeletonStore::load_from_store(store, Uuid::from_u128(2))
            .await
            .expect("reload");
        let found = reloaded
            .lookup(&client, &token, 60_000)
            .await
            .expect("present");
        assert_eq!(found.subscriptions[0].sub_id, 7);
    }
}
