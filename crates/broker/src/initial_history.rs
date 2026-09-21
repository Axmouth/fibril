//! Explicit preparation of enrolled empty histories. These operations never
//! activate a writer; callers must authenticate the peer before admitting work.
use crate::{
    broker::{Broker, BrokerError},
    queue_engine::{PartitionKind, PreparedStorageHistory, StorageHistoryBinding, StromaEngine},
    Partition,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialHistoryPrepareCommand {
    pub replica_id: String,
    pub topic: String,
    pub partition: Partition,
    pub group: Option<String>,
    pub stream: bool,
    pub decision: [u8; 32],
    pub binding: StorageHistoryBinding,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialHistoryAuthorization {
    pub node_id: String,
    pub replica_process: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InitialHistoryLocalReceipt {
    pub decision: [u8; 32],
    pub node_id: String,
    pub replica_process: [u8; 16],
    pub storage: PreparedStorageHistory,
}

impl InitialHistoryPrepareCommand {
    pub fn validate_receipt(&self, receipt: &InitialHistoryLocalReceipt) -> Result<(), String> {
        if receipt.node_id != self.replica_id
            || receipt.decision != self.decision
            || receipt.replica_process == [0; 16]
            || receipt.storage.storage_instance == [0; 16]
            || receipt.storage.topic != self.topic
            || receipt.storage.partition != self.partition.id()
            || receipt.storage.group != self.group
            || receipt.storage.stream != self.stream
            || receipt.storage.binding != self.binding
        {
            return Err(
                "preparation receipt differs from the contacted replica or decision".into(),
            );
        }
        Ok(())
    }
}

type PrepareResult = Result<InitialHistoryLocalReceipt, String>;
pub(crate) struct InitialHistoryFlight {
    command: InitialHistoryPrepareCommand,
    result: watch::Receiver<Option<PrepareResult>>,
}
struct FlightGuard(Arc<Mutex<Option<InitialHistoryFlight>>>);
impl Drop for FlightGuard {
    fn drop(&mut self) {
        self.0.lock().unwrap().take();
    }
}

/// Shutdown closes admission before calling this. Already-owned preparation
/// must finish before the engine is stopped, including after caller cancellation.
pub(crate) async fn drain(flight: &Arc<Mutex<Option<InitialHistoryFlight>>>) {
    let pending = flight.lock().unwrap().as_ref().map(|f| f.result.clone());
    if let Some(mut result) = pending {
        while result.borrow().is_none() {
            if result.changed().await.is_err() { break; }
        }
    }
}

impl Broker<StromaEngine> {
    /// Bound admitted preparation to one operation per broker. Identical calls
    /// share work; caller cancellation cannot drop admitted storage work.
    pub async fn prepare_initial_history_replica(
        &self,
        command: InitialHistoryPrepareCommand,
    ) -> Result<InitialHistoryLocalReceipt, BrokerError> {
        let mut result = {
            let mut current = self.initial_history_flight.lock().unwrap();
            if self.is_shutting_down() {
                return Err(BrokerError::InvalidArgument("broker is shutting down".into()));
            }
            if let Some(flight) = &*current {
                if flight.command != command {
                    return Err(BrokerError::InvalidArgument(
                        "another initial history preparation is in progress; retry later".into(),
                    ));
                }
                flight.result.clone()
            } else {
                let (sender, receiver) = watch::channel(None);
                *current = Some(InitialHistoryFlight {
                    command: command.clone(),
                    result: receiver.clone(),
                });
                let ownership = self.ownership.clone();
                let engine = self.engine.clone();
                let flight = self.initial_history_flight.clone();
                tokio::spawn(async move {
                    let guard = FlightGuard(flight);
                    let outcome = async {
                        let authority = tokio::time::timeout(
                            std::time::Duration::from_secs(10),
                            ownership.authorize_initial_history(&command),
                        )
                        .await
                        .map_err(|_| "initial history authorization timed out".to_string())??;
                        if authority.node_id != command.replica_id
                            || authority.replica_process == [0; 16]
                        {
                            return Err(
                                "initial history authorization returned a different replica".into(),
                            );
                        }
                        let storage = engine
                            .resume_empty_storage_history(
                                &command.topic,
                                command.partition.id(),
                                command.group.as_deref(),
                                if command.stream {
                                    PartitionKind::Stream
                                } else {
                                    PartitionKind::Queue
                                },
                                command.binding.clone(),
                            )
                            .await
                            .map_err(|e| e.to_string())?;
                        let receipt = InitialHistoryLocalReceipt {
                            decision: command.decision,
                            node_id: authority.node_id,
                            replica_process: authority.replica_process,
                            storage,
                        };
                        command.validate_receipt(&receipt)?;
                        Ok(receipt)
                    }
                    .await;
                    drop(guard);
                    sender.send_replace(Some(outcome));
                });
                receiver
            }
        };
        loop {
            if let Some(completed) = result.borrow().clone() {
                return completed.map_err(BrokerError::InvalidArgument);
            }
            result
                .changed()
                .await
                .map_err(|_| BrokerError::Unknown("initial history worker stopped".into()))?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{BrokerConfig, QueueOwnership};
    use crate::queue_engine::QueueEngine;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;

    #[derive(Debug, Default)]
    struct Authority {
        calls: AtomicUsize,
        started: Notify,
        release: Notify,
    }
    impl QueueOwnership for Authority {
        fn owns_queue(&self, _: &str, _: Partition, _: Option<&str>) -> bool {
            false
        }
        fn authorize_initial_history<'a>(
            &'a self,
            command: &'a InitialHistoryPrepareCommand,
        ) -> futures::future::BoxFuture<'a, Result<InitialHistoryAuthorization, String>> {
            Box::pin(async move {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.started.notify_one();
                self.release.notified().await;
                Ok(InitialHistoryAuthorization {
                    node_id: command.replica_id.clone(),
                    replica_process: [4; 16],
                })
            })
        }
    }

    #[tokio::test]
    async fn preparation_is_bounded_coalesced_and_survives_caller_cancellation() {
        let dir = stroma_core::test_dir!("broker_initial_history");
        let engine = StromaEngine::open(
            &dir.root,
            stroma_core::StromaKeratinConfig::from_message_log(
                stroma_core::KeratinConfig::test_default(),
            ),
            stroma_core::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let authority = Arc::new(Authority::default());
        let broker = Broker::new_with_ownership(
            engine.clone(),
            BrokerConfig::default(),
            None,
            authority.clone(),
        );
        let command = InitialHistoryPrepareCommand {
            replica_id: "a".into(),
            topic: "q".into(),
            partition: Partition::new(0),
            group: None,
            stream: false,
            decision: [5; 32],
            binding: StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
        };
        let pending = {
            let broker = broker.clone();
            let command = command.clone();
            tokio::spawn(async move { broker.prepare_initial_history_replica(command).await })
        };
        authority.started.notified().await;
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        let mut other = command.clone();
        other.decision[0] ^= 1;
        assert!(broker.prepare_initial_history_replica(other).await.is_err());
        let second = broker.prepare_initial_history_replica(command.clone());
        let third = broker.prepare_initial_history_replica(command.clone());
        tokio::pin!(second);
        tokio::pin!(third);
        assert!(futures::poll!(&mut second).is_pending());
        assert!(futures::poll!(&mut third).is_pending());
        assert_eq!(authority.calls.load(Ordering::Relaxed), 1);
        let shutdown = broker.shutdown();
        tokio::pin!(shutdown);
        assert!(futures::poll!(&mut shutdown).is_pending());
        assert!(broker.prepare_initial_history_replica(command.clone()).await.is_err());
        authority.release.notify_one();
        let (second, third) = tokio::join!(second, third);
        assert_eq!(second.unwrap(), third.unwrap());
        shutdown.await;
        assert!(broker.initial_history_flight.lock().unwrap().is_none());
        assert!(matches!(
            engine.ensure_queue_owner_epoch("q", 0, None, Some(1)).await,
            Err(crate::queue_engine::StromaError::HistoryAdmissionRequired { .. })
        ));
        broker.shutdown().await;
    }
}
