//! Exact installed-quorum activation. Plan, assignment and process/storage
//! identities remain immutable once committed; restart requires another recovery.
use crate::{
    GanglionCoordination, promotion::pending_recovery_key, recovery_plan::QueueRecoveryPlan,
};
use fibril_broker::{
    history_replication::{AcceptedHistory, ReplicaHistoryInstance},
    queue_engine::{PreparedQueueRecovery, QueueRecoveryStage, StromaEngine},
};
use ganglion_core::{CoordinationSnapshot, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const ACTIVE: &str = "fibril/recovered-history/";
const RECEIPT: &str = "fibril/recovery-installed/";
const CERTIFICATE: &str = "fibril/recovery-activation/";
fn error(message: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(message.to_string())
}
fn active_key(plan: &QueueRecoveryPlan) -> Result<String, String> {
    let incarnation = plan
        .pending()
        .resource_incarnation
        .as_ref()
        .ok_or("recovery incarnation missing")?;
    Ok(format!(
        "{ACTIVE}{}",
        serde_json::to_string(incarnation).map_err(|e| e.to_string())?
    ))
}
fn receipt_key(plan: &QueueRecoveryPlan, node: &str) -> Result<String, String> {
    Ok(format!(
        "{RECEIPT}{}/{}",
        blake3::Hash::from_bytes(plan.digest()?),
        serde_json::to_string(node).map_err(|e| e.to_string())?
    ))
}
fn certificate_key(digest: [u8; 32]) -> String {
    format!("{CERTIFICATE}{}", blake3::Hash::from_bytes(digest))
}

pub use fibril_broker::recovery_transfer::QueueRecoveryLocalReceipt;
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueRecoveryActivation {
    version: u32,
    plan: QueueRecoveryPlan,
    assignment: ganglion_core::PartitionAssignment,
    reports: BTreeMap<String, QueueRecoveryLocalReceipt>,
}
impl QueueRecoveryActivation {
    pub fn plan(&self) -> &QueueRecoveryPlan {
        &self.plan
    }
    pub fn reports(&self) -> &BTreeMap<String, QueueRecoveryLocalReceipt> {
        &self.reports
    }
    pub fn digest(&self) -> Result<[u8; 32], String> {
        let mut h = blake3::Hasher::new();
        h.update(b"fibril-recovered-activation-v2\0");
        h.update(&serde_json::to_vec(self).map_err(|e| e.to_string())?);
        Ok(*h.finalize().as_bytes())
    }
    fn validate_receipts(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        let assignment = &self.assignment;
        if crate::recovery_candidate::assignment(snapshot, self.plan.pending())? != *assignment {
            return Err("activation differs from the current recovery candidate".into());
        }
        if self.version != 2
            || self.reports.len() < self.plan.pending().proposed_write_nodes
            || !self.reports.contains_key(&assignment.owner)
        {
            return Err("recovery activation requires the owner and new write quorum".into());
        }
        let spec = self.plan.stage_spec()?;
        for (node, report) in &self.reports {
            let s = &report.installed.storage;
            if node != &report.node_id
                || (node != &assignment.owner && !assignment.followers.contains(node))
                || report.replica_process == [0; 16]
                || s.storage_instance == [0; 16]
                || report.installed.spec != spec
                || s.topic != spec.topic
                || s.partition != spec.partition
                || s.group != spec.group
                || s.stream
                || s.binding != spec.binding
                || snapshot
                    .attributes
                    .get(&receipt_key(&self.plan, node)?)
                    .map(|raw| {
                        serde_json::from_str::<QueueRecoveryLocalReceipt>(raw)
                            .map_err(|e| e.to_string())
                    })
                    .transpose()?
                    .as_ref()
                    != Some(report)
            {
                return Err(
                    "installed receipt differs from exact persisted replica and plan".into(),
                );
            }
        }
        Ok(())
    }
    fn validate_active(&self, snapshot: &CoordinationSnapshot) -> Result<(), String> {
        self.plan.validate_activated(snapshot, &self.assignment)?;
        self.validate_receipts(snapshot)?;
        let raw = serde_json::to_string(self).map_err(|e| e.to_string())?;
        if snapshot.attributes.get(&active_key(&self.plan)?) != Some(&raw)
            || snapshot.attributes.get(&certificate_key(self.digest()?)) != Some(&raw)
        {
            return Err("recovery activation is not the current immutable certificate".into());
        }
        Ok(())
    }
    fn history(&self) -> Result<AcceptedHistory, String> {
        Ok(AcceptedHistory {
            activation: self.digest()?,
            binding: self.plan.binding().clone(),
            owner: self.assignment.owner.clone(),
            blocked_local_replica: None,
            replicas: self
                .reports
                .iter()
                .map(|(node, r)| {
                    (
                        node.clone(),
                        ReplicaHistoryInstance {
                            process: r.replica_process,
                            storage: r.installed.storage.storage_instance,
                        },
                    )
                })
                .collect(),
        })
    }
}

pub(crate) fn accepted(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
) -> Result<Option<AcceptedHistory>, String> {
    let Some(incarnation) = crate::history_identity::resource_incarnation(snapshot, resource)?
    else {
        return Ok(None);
    };
    let key = format!(
        "{ACTIVE}{}",
        serde_json::to_string(&incarnation).map_err(|e| e.to_string())?
    );
    let Some(raw) = snapshot.attributes.get(&key) else {
        return Ok(None);
    };
    let activation: QueueRecoveryActivation =
        serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if &activation.plan.pending().proposed.resource != resource {
        return Err("recovered authority belongs to another resource".into());
    }
    activation.validate_active(snapshot)?;
    activation.history().map(Some)
}
impl GanglionCoordination {
    /// Install then persist this target's exact process/storage receipt. Targets
    /// can refresh an unfinished preparation after restart; activation closes it.
    pub async fn install_local_queue_recovery(
        &self,
        plan: &QueueRecoveryPlan,
        engine: &StromaEngine,
        stage: &QueueRecoveryStage,
    ) -> Result<QueueRecoveryLocalReceipt, OpenraftAdapterError> {
        let spec = self.authorize_local_queue_recovery_stage(plan).await?;
        let installed = engine
            .install_queue_recovery_stage(
                spec,
                fibril_broker::recovery::RecoverySealRequest {
                    transition: plan.pending().transition_digest().map_err(error)?,
                    fence_epoch: plan.pending().proposed.epoch,
                },
                stage,
            )
            .await
            .map_err(error)?;
        let receipt = QueueRecoveryLocalReceipt {
            node_id: self.node_id.clone(),
            replica_process: self.history_process,
            installed,
        };
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            plan.validate_committed(&snapshot).map_err(error)?;
            engine
                .verify_prepared_queue_recovery(&receipt.installed)
                .map_err(error)?;
            let key = receipt_key(plan, &self.node_id).map_err(error)?;
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    expected: snapshot.attributes.get(&key).cloned(),
                    key,
                    value: serde_json::to_string(&receipt).map_err(error)?,
                })
                .await
            {
                Ok(response) => {
                    plan.validate_committed(&response.snapshot).map_err(error)?;
                    return Ok(receipt);
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(e) => return Err(e),
            }
        }
        Err(error(
            "installed receipt raced metadata changes; retry with backoff",
        ))
    }
    pub fn queue_recovery_activation(
        &self,
        plan: &QueueRecoveryPlan,
    ) -> Result<Option<QueueRecoveryActivation>, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        let Some(raw) = snapshot.attributes.get(&active_key(plan).map_err(error)?) else {
            return Ok(None);
        };
        let activation: QueueRecoveryActivation = serde_json::from_str(raw).map_err(error)?;
        if activation.plan != *plan {
            return Ok(None);
        }
        activation.validate_active(&snapshot).map_err(error)?;
        Ok(Some(activation))
    }
    /// Publish assignment, immutable certificate and pending-fence removal in
    /// one guarded consensus operation. Lost replies never trigger reinstall.
    pub async fn activate_queue_recovery(
        &self,
        plan: &QueueRecoveryPlan,
        engine: &StromaEngine,
    ) -> Result<QueueRecoveryActivation, OpenraftAdapterError> {
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            let assignment = crate::recovery_candidate::assignment(&snapshot, plan.pending())
                .map_err(error)?;
            if assignment.owner != self.node_id {
                return Err(error("only the current recovery candidate may activate recovery"));
            }
            if let Some(activation) = self.queue_recovery_activation(plan)? {
                self.verify_local_recovery(&activation, engine)?;
                return Ok(activation);
            }
            plan.validate_committed(&snapshot).map_err(error)?;
            let mut reports = BTreeMap::new();
            for node in std::iter::once(&assignment.owner).chain(assignment.followers.iter()) {
                if let Some(raw) = snapshot
                    .attributes
                    .get(&receipt_key(plan, node).map_err(error)?)
                {
                    reports.insert(node.clone(), serde_json::from_str(raw).map_err(error)?);
                }
            }
            let activation = QueueRecoveryActivation {
                version: 2,
                plan: plan.clone(),
                assignment: assignment.clone(),
                reports,
            };
            activation.validate_receipts(&snapshot).map_err(error)?;
            self.verify_local_recovery(&activation, engine)?;
            let raw = serde_json::to_string(&activation).map_err(error)?;
            let attributes = BTreeMap::from([
                (active_key(plan).map_err(error)?, Some(raw.clone())),
                (
                    certificate_key(activation.digest().map_err(error)?),
                    Some(raw),
                ),
                (pending_recovery_key(&assignment.resource), None),
            ]);
            match self
                .forward_command(MetadataRaftCommand::UpdatePartitionGuarded {
                    expected_generation: snapshot.generation,
                    assignment: assignment.clone(),
                    attributes,
                })
                .await
            {
                Ok(response) => {
                    activation
                        .validate_active(&response.snapshot)
                        .map_err(error)?;
                    return Ok(activation);
                }
                Err(OpenraftAdapterError::GenerationMismatch { .. }) => {
                    tokio::task::yield_now().await
                }
                Err(e) => return Err(e),
            }
        }
        Err(error(
            "recovery activation raced metadata changes; retry with backoff",
        ))
    }
    fn verify_local_recovery<'a>(
        &self,
        activation: &'a QueueRecoveryActivation,
        engine: &StromaEngine,
    ) -> Result<&'a QueueRecoveryLocalReceipt, OpenraftAdapterError> {
        let report = activation
            .reports
            .get(&self.node_id)
            .ok_or_else(|| error("local node is outside recovered quorum"))?;
        if report.replica_process != self.history_process {
            return Err(error("restarted replica requires the next recovery"));
        }
        engine
            .verify_prepared_queue_recovery(&report.installed)
            .map_err(error)?;
        Ok(report)
    }
    pub async fn admit_local_queue_recovery(
        &self,
        activation: &QueueRecoveryActivation,
        engine: &StromaEngine,
    ) -> Result<PreparedQueueRecovery, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        activation.validate_active(&snapshot).map_err(error)?;
        let report = self.verify_local_recovery(activation, engine)?;
        let key = active_key(&activation.plan).map_err(error)?;
        let raw = snapshot.attributes[&key].clone();
        let response = self
            .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                expected_generation: snapshot.generation,
                key,
                expected: Some(raw.clone()),
                value: raw,
            })
            .await?;
        activation
            .validate_active(&response.snapshot)
            .map_err(error)?;
        engine
            .admit_prepared_queue_recovery(report.installed.clone())
            .await
            .map_err(error)?;
        Ok(report.installed.clone())
    }
}

impl GanglionCoordination {
    fn transfer_plan(
        &self,
        command: &fibril_broker::recovery_transfer::QueueRecoveryCommand,
    ) -> Result<QueueRecoveryPlan, String> {
        if command.replica_id != self.node_id {
            return Err("recovery transfer targets another node".into());
        }
        let resource = ResourceIdentity::new(
            crate::QUEUE_NAMESPACE,
            command.topic.clone(),
            u64::from(command.partition),
            command.group.clone(),
        );
        let snapshot = self.node.committed_snapshot();
        let pending: crate::promotion::PendingRecovery = serde_json::from_str(
            snapshot
                .attributes
                .get(&pending_recovery_key(&resource))
                .ok_or("no pending recovery for transfer")?,
        )
        .map_err(|e| e.to_string())?;
        let plan: QueueRecoveryPlan = serde_json::from_str(
            snapshot
                .attributes
                .get(&crate::recovery_plan::key(&pending)?)
                .ok_or("no persisted recovery plan")?,
        )
        .map_err(|e| e.to_string())?;
        plan.validate_committed(&snapshot)?;
        if plan.pending().proposed.resource != resource || plan.digest() != Ok(command.plan) {
            return Err("recovery command differs from the pending plan".into());
        }
        Ok(plan)
    }
    pub(crate) async fn authorize_transfer_command(
        &self,
        command: &fibril_broker::recovery_transfer::QueueRecoveryCommand,
    ) -> Result<fibril_broker::recovery_transfer::QueueRecoveryAuthorization, String> {
        let plan = self.transfer_plan(command)?;
        let spec = self
            .authorize_local_queue_recovery_stage(&plan)
            .await
            .map_err(|e| e.to_string())?;
        Ok(
            fibril_broker::recovery_transfer::QueueRecoveryAuthorization {
                spec,
                seal: fibril_broker::recovery::RecoverySealRequest {
                    transition: plan.pending().transition_digest()?,
                    fence_epoch: plan.pending().proposed.epoch,
                },
                replica_process: self.history_process,
            },
        )
    }
    pub(crate) async fn record_transfer_receipt(
        &self,
        command: &fibril_broker::recovery_transfer::QueueRecoveryCommand,
        receipt: &QueueRecoveryLocalReceipt,
    ) -> Result<(), String> {
        let plan = self.transfer_plan(command)?;
        let s = &receipt.installed.storage;
        if receipt.node_id != self.node_id
            || receipt.replica_process != self.history_process
            || receipt.installed.spec != plan.stage_spec()?
            || s.storage_instance == [0; 16]
            || s.topic != command.topic
            || s.partition != command.partition
            || s.group != command.group
            || s.stream
            || &s.binding != plan.binding()
        {
            return Err("installed receipt differs from this replica and plan".into());
        }
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            plan.validate_committed(&snapshot)?;
            let key = receipt_key(&plan, &self.node_id)?;
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    expected: snapshot.attributes.get(&key).cloned(),
                    key,
                    value: serde_json::to_string(receipt).map_err(|e| e.to_string())?,
                })
                .await
            {
                Ok(response) => {
                    plan.validate_committed(&response.snapshot)?;
                    return Ok(());
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::task::yield_now().await,
                Err(e) => return Err(e.to_string()),
            }
        }
        Err("recovery receipt raced metadata changes; retry with backoff".into())
    }
    pub(crate) async fn authorize_recovery_admission_command(
        &self,
        command: &fibril_broker::recovery_transfer::QueueRecoveryCommand,
    ) -> Result<PreparedQueueRecovery, String> {
        if command.replica_id != self.node_id {
            return Err("recovery admission targets another node".into());
        }
        let resource = ResourceIdentity::new(
            crate::QUEUE_NAMESPACE,
            command.topic.clone(),
            u64::from(command.partition),
            command.group.clone(),
        );
        for _ in 0..8 {
            let snapshot = self.node.committed_snapshot();
            let incarnation = crate::history_identity::resource_incarnation(&snapshot, &resource)?
                .ok_or("recovery incarnation absent")?;
            let key = format!(
                "{ACTIVE}{}",
                serde_json::to_string(&incarnation).map_err(|e| e.to_string())?
            );
            let raw = snapshot
                .attributes
                .get(&key)
                .ok_or("recovery activation absent")?
                .clone();
            let activation: QueueRecoveryActivation =
                serde_json::from_str(&raw).map_err(|e| e.to_string())?;
            activation.validate_active(&snapshot)?;
            let report = activation
                .reports
                .get(&self.node_id)
                .ok_or("replica outside recovered quorum")?;
            if activation.plan.digest() != Ok(command.plan)
                || report.replica_process != self.history_process
            {
                return Err("recovered history or process changed; new recovery required".into());
            }
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
                    expected_generation: snapshot.generation,
                    key,
                    expected: Some(raw.clone()),
                    value: raw,
                })
                .await
            {
                Ok(response) => {
                    activation.validate_active(&response.snapshot)?;
                    return Ok(report.installed.clone());
                }
                Err(
                    OpenraftAdapterError::GenerationMismatch { .. }
                    | OpenraftAdapterError::AttributeMismatch { .. },
                ) => tokio::time::sleep(std::time::Duration::from_millis(5)).await,
                Err(e) => return Err(e.to_string()),
            }
        }
        Err("recovery admission raced metadata changes; retry with backoff".into())
    }
}

impl GanglionCoordination {
    /// Current exact-process admissions that this node may retry after a lost
    /// activation response. A restarted process is omitted and needs recovery.
    pub fn local_queue_recovery_admissions(
        &self,
    ) -> Result<
        Vec<(
            fibril_broker::recovery_transfer::QueueRecoveryCommand,
            PreparedQueueRecovery,
        )>,
        String,
    > {
        let snapshot = self.node.committed_snapshot();
        let mut result = vec![];
        for resource in snapshot
            .assignments
            .keys()
            .filter(|r| r.namespace == crate::QUEUE_NAMESPACE)
        {
            if snapshot
                .attributes
                .contains_key(&pending_recovery_key(resource))
            {
                continue;
            }
            let Some(incarnation) =
                crate::history_identity::resource_incarnation(&snapshot, resource)?
            else {
                continue;
            };
            let key = format!(
                "{ACTIVE}{}",
                serde_json::to_string(&incarnation).map_err(|e| e.to_string())?
            );
            let Some(raw) = snapshot.attributes.get(&key) else {
                continue;
            };
            let activation: QueueRecoveryActivation =
                serde_json::from_str(raw).map_err(|e| e.to_string())?;
            activation.validate_active(&snapshot)?;
            let Some(receipt) = activation.reports.get(&self.node_id) else {
                continue;
            };
            if receipt.replica_process != self.history_process {
                continue;
            }
            result.push((
                fibril_broker::recovery_transfer::QueueRecoveryCommand {
                    replica_id: self.node_id.clone(),
                    topic: resource.name.clone(),
                    partition: u32::try_from(resource.partition).map_err(|e| e.to_string())?,
                    group: resource.group.clone(),
                    plan: activation.plan.digest()?,
                },
                receipt.installed.clone(),
            ));
        }
        Ok(result)
    }
}
