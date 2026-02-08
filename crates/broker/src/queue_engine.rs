use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use fibril_storage::Offset;
use fibril_util::UnixMillis;
use hashbrown::HashSet;
use stroma_core::{AppendCompletion, IoError, KeratinConfig, SnapshotConfig, Stroma, StromaError};

pub struct Deliverable {
    pub offset: Offset,
    pub payload: Vec<u8>,
}

pub enum SettleKind {
    Ack,
    Nack { requeue: bool },
}

pub struct SettleRequest {
    pub offset: Offset,
    pub kind: SettleKind,
}

#[async_trait]
pub trait QueueEngine {
    async fn poll_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        max: usize,
        lease_deadline: UnixMillis,
    ) -> Result<Vec<Deliverable>, StromaError>;

    async fn ack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn nack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn settle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        req: SettleRequest,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn publish(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        payload: &[u8],
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    fn next_expiry_hint(&self) -> Result<Option<UnixMillis>, StromaError>;

    async fn requeue_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError>;

    async fn shutdown(&self) -> Result<(), StromaError>;
}

#[derive(Debug, Clone)]
pub struct StromaEngine {
    inner: Stroma,
}

impl StromaEngine {
    pub async fn open(
        root: impl AsRef<Path>,
        keratin_cfg: KeratinConfig,
        snap_cfg: SnapshotConfig,
    ) -> Result<Self, StromaError> {
        let stroma = Stroma::open(root, keratin_cfg, snap_cfg).await?;
        Ok(Self {
            inner: stroma,
        })
    }
}

pub fn make_stroma_engine(root: impl AsRef<Path>) -> Result<StromaEngine, StromaError> {
    let keratin_cfg = KeratinConfig::test_default();
    let snap_cfg = SnapshotConfig::default();
    let stroma = futures::executor::block_on(Stroma::open(root, keratin_cfg, snap_cfg))?;
    Ok(StromaEngine {
        inner: stroma,
    })
}

#[async_trait]
impl QueueEngine for StromaEngine {
    async fn poll_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        max: usize,
        lease_deadline: UnixMillis,
    ) -> Result<Vec<Deliverable>, StromaError> {
        let v = self
            .inner
            .poll_ready(tp, part, group, max, lease_deadline)
            .await?;

        Ok(v.into_iter()
            .map(|(offset, payload)| Deliverable { offset, payload })
            .collect())
    }

    async fn ack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .ack_enqueue(tp, part, group, offset, completion)
            .await
    }

    async fn nack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .nack_enqueue(tp, part, group, offset, requeue, completion)
            .await?;
        Ok(())
    }

    async fn settle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        req: SettleRequest,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        match req.kind {
            SettleKind::Ack => {
                self.inner
                    .ack_enqueue(tp, part, group, req.offset, completion)
                    .await?;
            }
            SettleKind::Nack { requeue } => {
                self.inner
                    .nack_enqueue(tp, part, group, req.offset, requeue, completion)
                    .await?;
            }
        }
        Ok(())
    }

    async fn publish(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        payload: &[u8],
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .append_message(tp, part, group, payload, completion)
            .await?;

        Ok(())
    }

    fn next_expiry_hint(&self) -> Result<Option<UnixMillis>, StromaError> {
        self.inner.next_expiry_hint()
    }

    async fn requeue_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError> {
        self.inner.requeue_expired(now, max).await
    }

    async fn shutdown(&self) -> Result<(), StromaError> {
        self.inner.shutdown()?;

        Ok(())
    }
}
