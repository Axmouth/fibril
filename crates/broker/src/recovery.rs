//! Explicit recovery control operations. No automatic dispatcher is enabled.

use crate::{
    Partition,
    broker::{Broker, BrokerError},
    queue_engine::StromaEngine,
};
use std::sync::{Arc, Mutex};
pub use stroma_core::recovery_budget::{RecoveryBudget, RecoveryBudgetExceeded};
pub use stroma_core::recovery_inspection as inspection;
pub use stroma_core::recovery_journal as journal;
pub use stroma_core::recovery_replay as replay;
pub use stroma_core::{
    QueueLogLayout, RecoveryReadPage, RecoveryReadRequest, RecoveryReadSource, RecoveryRecord,
    RecoverySealRequest, RecoverySequentialRead, RetainedHistoryIdentity, RetainedLog,
    RetainedLogs, SealedReplicaFrontiers,
};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoverySealCommand {
    pub topic: String,
    pub partition: Partition,
    pub group: Option<String>,
    pub stream: bool,
    pub transition: [u8; 32],
    pub fence_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerSealedReplica {
    pub node_id: String,
    pub seal: SealedReplicaFrontiers,
}

type SealResult = Result<BrokerSealedReplica, String>;
pub(crate) struct RecoverySealFlight {
    command: RecoverySealCommand,
    result: watch::Receiver<Option<SealResult>>,
}

struct FlightGuard(
    Arc<Mutex<Option<RecoverySealFlight>>>,
    watch::Receiver<Option<SealResult>>,
    bool,
);
impl Drop for FlightGuard {
    fn drop(&mut self) {
        let mut current = self.0.lock().unwrap();
        if current
            .as_ref()
            .is_some_and(|flight| flight.result.same_channel(&self.1))
            && (!self.2 || !matches!(self.1.borrow().as_ref(), Some(Ok(_))))
        {
            current.take();
        }
    }
}

impl Broker<StromaEngine> {
    /// Reading does not initiate or finish a seal. Require its exact persisted
    /// transition through consensus before entering storage's bounded read slot.
    pub async fn read_sealed_replica(
        &self,
        command: RecoverySealCommand,
        request: RecoveryReadRequest,
    ) -> Result<(String, RecoveryReadPage), BrokerError> {
        let node_id = self.authorize_recovery_read(&command, &request).await?;
        let page = self
            .engine
            .read_sealed_replica(&command, request)
            .await
            .map_err(|err| BrokerError::InvalidArgument(err.to_string()))?;
        Ok((node_id, page))
    }

    /// Tentative pages for a receiver that verifies complete sealed histories.
    /// Fresh authorization is required even when the transport/cursor is reused.
    pub async fn read_sealed_replica_sequential(
        &self,
        command: RecoverySealCommand,
        request: RecoveryReadRequest,
        cursor: Option<RecoverySequentialRead>,
    ) -> Result<(String, RecoveryReadPage, Option<RecoverySequentialRead>), BrokerError> {
        let node_id = self.authorize_recovery_read(&command, &request).await?;
        let (page, cursor) = self
            .engine
            .read_sealed_replica_sequential(&command, request, cursor)
            .await
            .map_err(|err| BrokerError::InvalidArgument(err.to_string()))?;
        Ok((node_id, page, cursor))
    }

    async fn authorize_recovery_read(
        &self,
        command: &RecoverySealCommand,
        request: &RecoveryReadRequest,
    ) -> Result<String, BrokerError> {
        if request.seal.transition != command.transition
            || request.seal.fence_epoch != command.fence_epoch
        {
            return Err(BrokerError::InvalidArgument(
                "recovery read transition mismatch".into(),
            ));
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.ownership.authorize_recovery_seal(command),
        )
        .await
        .map_err(|_| BrokerError::Unknown("recovery authorization timed out".into()))?
        .map_err(BrokerError::InvalidArgument)
    }

    /// The protocol authenticates the peer first. Only one recovery seal runs
    /// per broker; identical concurrent requests share it, conflicting requests
    /// get a busy error. Caller cancellation cannot cancel admitted work. One
    /// completed result is retained for lost replies and freshly reauthorized.
    pub async fn seal_replica_for_recovery(
        &self,
        command: RecoverySealCommand,
    ) -> Result<BrokerSealedReplica, BrokerError> {
        let (mut result, cached) = {
            let mut current = self.recovery_seal_flight.lock().unwrap();
            if let Some(flight) = &*current {
                if flight.command != command && flight.result.borrow().is_none() {
                    return Err(BrokerError::InvalidArgument(
                        "another recovery seal is in progress, retry later".into(),
                    ));
                }
            }
            if current.as_ref().is_some_and(|flight| {
                flight.command != command || matches!(flight.result.borrow().as_ref(), Some(Err(_)))
            }) {
                current.take();
            }
            if let Some(flight) = &*current {
                let cached = matches!(flight.result.borrow().as_ref(), Some(Ok(_)));
                (flight.result.clone(), cached)
            } else {
                let (sender, receiver) = watch::channel(None);
                *current = Some(RecoverySealFlight {
                    command: command.clone(),
                    result: receiver.clone(),
                });
                let ownership = self.ownership.clone();
                let engine = self.engine.clone();
                let flight = self.recovery_seal_flight.clone();
                let guard_result = receiver.clone();
                let command = command.clone();
                tokio::spawn(async move {
                    let _guard = FlightGuard(flight, guard_result, !command.stream);
                    let outcome = async {
                        let node_id = tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            ownership.authorize_recovery_seal(&command),
                        )
                        .await
                        .map_err(|_| "recovery authorization timed out".to_string())??;
                        if !command.stream {
                            let saved_engine = engine.clone();
                            let saved_command = command.clone();
                            let completed = tokio::task::spawn_blocking(move || {
                                saved_engine.completed_queue_recovery_seal(
                                    &saved_command.topic,
                                    saved_command.partition.id(),
                                    saved_command.group.as_deref(),
                                    &RecoverySealRequest {
                                        transition: saved_command.transition,
                                        fence_epoch: saved_command.fence_epoch,
                                    },
                                )
                            })
                            .await
                            .map_err(|e| e.to_string())?
                            .map_err(|e| e.to_string())?;
                            if let Some(seal) = completed {
                                return Ok(BrokerSealedReplica { node_id, seal });
                            }
                        }
                        let seal = engine
                            .seal_replica_for_recovery(
                                &command.topic,
                                command.partition.id(),
                                command.group.as_deref(),
                                command.stream,
                                RecoverySealRequest {
                                    transition: command.transition,
                                    fence_epoch: command.fence_epoch,
                                },
                            )
                            .await
                            .map_err(|err| err.to_string())?;
                        Ok(BrokerSealedReplica { node_id, seal })
                    }
                    .await;
                    sender.send_replace(Some(outcome));
                    drop(_guard);
                });
                (receiver, false)
            }
        };
        if cached {
            // The seal is immutable in this process. A lost response can reuse
            // completed work, but never the old request's consensus authority.
            let node = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                self.ownership.authorize_recovery_seal(&command),
            )
            .await
            .map_err(|_| BrokerError::Unknown("recovery authorization timed out".into()))?
            .map_err(BrokerError::InvalidArgument)?;
            if result
                .borrow()
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .is_none_or(|report| report.node_id != node)
            {
                return Err(BrokerError::InvalidArgument(
                    "cached seal node differs from current authority".into(),
                ));
            }
        }
        loop {
            let completed = result.borrow().clone();
            if let Some(completed) = completed {
                return completed.map_err(BrokerError::InvalidArgument);
            }
            result
                .changed()
                .await
                .map_err(|_| BrokerError::Unknown("recovery seal worker stopped".into()))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{BrokerConfig, QueueOwnership};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Debug, Default)]
    struct Authority {
        calls: AtomicUsize,
        denied: AtomicBool,
        started: Notify,
        release: Notify,
    }
    impl QueueOwnership for Authority {
        fn owns_queue(&self, _: &str, _: Partition, _: Option<&str>) -> bool {
            false
        }
        fn authorize_recovery_seal<'a>(
            &'a self,
            _: &'a RecoverySealCommand,
        ) -> futures::future::BoxFuture<'a, Result<String, String>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                if self.denied.load(Ordering::Relaxed) {
                    return Err("authority revoked".into());
                }
                self.started.notify_one();
                self.release.notified().await;
                Ok("test-replica".into())
            })
        }
    }

    #[tokio::test]
    async fn recovery_seal_coalesces_and_survives_caller_cancellation() {
        let dir = stroma_core::test_dir!("broker_recovery_coalescing");
        let engine = StromaEngine::open(
            &dir.root,
            stroma_core::StromaKeratinConfig::from_message_log(
                stroma_core::KeratinConfig::test_default(),
            ),
            stroma_core::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        engine
            .initialize_empty_storage_history(
                "q",
                0,
                None,
                stroma_core::PartitionKind::Queue,
                stroma_core::StorageHistoryBinding {
                    resource_incarnation: [1; 16],
                    accepted_history: [2; 16],
                    writer_session: [3; 16],
                },
            )
            .await
            .unwrap();
        let authority = Arc::new(Authority::default());
        let broker =
            Broker::new_with_ownership(engine, BrokerConfig::default(), None, authority.clone());
        let command = RecoverySealCommand {
            topic: "q".into(),
            partition: Partition::new(0),
            group: None,
            stream: false,
            transition: [5; 32],
            fence_epoch: 8,
        };
        let call = |command: RecoverySealCommand| {
            let broker = broker.clone();
            tokio::spawn(async move { broker.seal_replica_for_recovery(command).await })
        };
        let first = call(command.clone());
        authority.started.notified().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let mut conflict = command.clone();
        conflict.transition[0] ^= 1;
        assert!(broker.seal_replica_for_recovery(conflict).await.is_err());
        let second = broker.seal_replica_for_recovery(command.clone());
        let third = broker.seal_replica_for_recovery(command.clone());
        tokio::pin!(second);
        tokio::pin!(third);
        assert!(futures::poll!(&mut second).is_pending());
        assert!(futures::poll!(&mut third).is_pending());
        assert_eq!(authority.calls.load(Ordering::Relaxed), 1);
        authority.release.notify_one();
        let (second, third) = tokio::join!(second, third);
        let expected = second.unwrap();
        assert_eq!(expected, third.unwrap());
        assert!(broker.recovery_seal_flight.lock().unwrap().is_some());
        let retry = call(command.clone());
        authority.started.notified().await;
        assert_eq!(authority.calls.load(Ordering::Relaxed), 2);
        authority.release.notify_one();
        assert_eq!(retry.await.unwrap().unwrap(), expected);
        authority.denied.store(true, Ordering::Relaxed);
        assert!(broker.seal_replica_for_recovery(command).await.is_err());
        assert_eq!(authority.calls.load(Ordering::Relaxed), 3);
        broker.shutdown().await;
    }
}

pub use stroma_core::recovery_snapshot as snapshot;

pub use stroma_core::recovery_phase as phase;

pub use stroma_core::recovery_cache as cache;
