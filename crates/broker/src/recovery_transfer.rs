//! Bounded node-to-node staging and readmission operations. The ownership
//! provider supplies fresh consensus authority for every request.
use crate::{
    broker::{Broker, BrokerError},
    queue_engine::{
        PreparedQueueRecovery, QueueRecoveryStage, QueueRecoveryStageReceipt,
        QueueRecoveryStageSpec, StromaEngine,
    },
    recovery::{RecoveryReadPage, RecoverySealRequest},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryCommand {
    pub replica_id: String,
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
    pub plan: [u8; 32],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryLocalReceipt {
    pub node_id: String,
    pub replica_process: [u8; 16],
    pub installed: PreparedQueueRecovery,
}
#[derive(Debug, Clone)]
pub struct QueueRecoveryAuthorization {
    pub spec: QueueRecoveryStageSpec,
    pub seal: RecoverySealRequest,
    pub replica_process: [u8; 16],
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum QueueRecoveryOperation {
    Begin {
        snapshot: Vec<u8>,
    },
    Resume,
    Append {
        page: RecoveryReadPage,
    },
    Finish,
    Install,
    Snapshot,
    Read {
        from: u64,
        max_records: u32,
        max_bytes: u32,
    },
    Admit,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryRequest {
    pub command: QueueRecoveryCommand,
    pub operation: QueueRecoveryOperation,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueueRecoveryReply {
    Progress(u64),
    Complete(QueueRecoveryStageReceipt),
    Installed(QueueRecoveryLocalReceipt),
    Snapshot(Vec<u8>),
    Page(RecoveryReadPage),
    Admitted(PreparedQueueRecovery),
}
pub(crate) struct RecoveryTransferStage {
    spec: QueueRecoveryStageSpec,
    stage: QueueRecoveryStage,
}
fn error(e: impl ToString) -> BrokerError {
    BrokerError::InvalidArgument(e.to_string())
}
impl Broker<StromaEngine> {
    pub async fn recovery_transfer(
        &self,
        request: QueueRecoveryRequest,
    ) -> Result<QueueRecoveryReply, BrokerError> {
        if self.is_shutting_down() {
            return Err(error("broker is shutting down"));
        }
        let command = request.command;
        if matches!(request.operation, QueueRecoveryOperation::Admit) {
            let prepared = tokio::time::timeout(
                std::time::Duration::from_secs(10),
                self.ownership.authorize_recovery_admission(&command),
            )
            .await
            .map_err(error)?
            .map_err(error)?;
            self.engine
                .admit_prepared_queue_recovery(prepared.clone())
                .await
                .map_err(error)?;
            self.notify_history_admitted();
            return Ok(QueueRecoveryReply::Admitted(prepared));
        }
        let authority = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            self.ownership.authorize_recovery_transfer(&command),
        )
        .await
        .map_err(error)?
        .map_err(error)?;
        if authority.spec.topic != command.topic
            || authority.spec.partition != command.partition
            || authority.spec.group != command.group
            || authority.spec.plan != command.plan
            || authority.replica_process == [0; 16]
        {
            return Err(error("recovery authorization differs from request"));
        }
        let mut current = self
            .recovery_transfer_stage
            .clone()
            .try_lock_owned()
            .map_err(|_| error("another recovery transfer is in progress; retry later"))?;
        if self.is_shutting_down() {
            return Err(error("broker is shutting down"));
        }
        let engine = self.engine.clone();
        let ownership = self.ownership.clone();
        tokio::spawn(async move {
            if current.as_ref().is_some_and(|s| s.spec != authority.spec) {
                current.take();
            }
            if let QueueRecoveryOperation::Begin { snapshot } = &request.operation {
                if snapshot.len() > 16 * 1024 * 1024
                    || *blake3::hash(snapshot).as_bytes() != authority.spec.snapshot_digest
                {
                    return Err(error("recovery snapshot differs from plan"));
                }
            }
            if current.is_none() {
                let stage = match &request.operation {
                    QueueRecoveryOperation::Begin { snapshot } => {
                        engine
                            .open_queue_recovery_stage_reusing(
                                authority.spec.clone(),
                                snapshot.clone(),
                                Default::default(),
                                authority.seal.clone(),
                            )
                            .await
                    }
                    _ => {
                        engine
                            .resume_queue_recovery_stage(authority.spec.clone(), Default::default())
                            .await
                    }
                }
                .map_err(error)?;
                *current = Some(RecoveryTransferStage {
                    spec: authority.spec.clone(),
                    stage,
                });
            }
            let outcome = async {
                let stage = &current.as_ref().unwrap().stage;
                match request.operation {
                    QueueRecoveryOperation::Begin { .. } | QueueRecoveryOperation::Resume => {
                        Ok(QueueRecoveryReply::Progress(stage.next_offset().await))
                    }
                    QueueRecoveryOperation::Append { page } => Ok(QueueRecoveryReply::Progress(
                        stage.append(page).await.map_err(error)?,
                    )),
                    QueueRecoveryOperation::Finish => Ok(QueueRecoveryReply::Complete(
                        stage.finish().await.map_err(error)?,
                    )),
                    QueueRecoveryOperation::Snapshot => Ok(QueueRecoveryReply::Snapshot(
                        stage.completed_snapshot().await.map_err(error)?,
                    )),
                    QueueRecoveryOperation::Read {
                        from,
                        max_records,
                        max_bytes,
                    } => Ok(QueueRecoveryReply::Page(
                        stage
                            .read_completed_messages(from, max_records, max_bytes)
                            .await
                            .map_err(error)?,
                    )),
                    QueueRecoveryOperation::Install => {
                        let installed = engine
                            .install_queue_recovery_stage(authority.spec, authority.seal, stage)
                            .await
                            .map_err(error)?;
                        let report = QueueRecoveryLocalReceipt {
                            node_id: command.replica_id.clone(),
                            replica_process: authority.replica_process,
                            installed,
                        };
                        engine
                            .verify_prepared_queue_recovery(&report.installed)
                            .map_err(error)?;
                        ownership
                            .record_recovery_installation(&command, &report)
                            .await
                            .map_err(error)?;
                        Ok(QueueRecoveryReply::Installed(report))
                    }
                    QueueRecoveryOperation::Admit => unreachable!(),
                }
            }
            .await;
            // An interrupted/failed append marks the storage handle for reopen.
            // Drop the cached handle so the next bounded retry reconstructs its
            // durable progress instead of repeating against a poisoned stage.
            if outcome.is_err() {
                current.take();
            }
            outcome
        })
        .await
        .map_err(error)?
    }
}
