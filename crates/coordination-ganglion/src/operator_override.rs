//! Operator decisions that relax a placement hold. Each one is bound to the
//! exact assignment the operator saw, so a decision about one owner can never
//! release a later, different hold.
use crate::{
    GanglionCoordination,
    promotion::{PendingRecovery, pending_recovery_key},
};
use ganglion_core::{CoordinationSnapshot, PartitionAssignment, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};

pub const ACCEPTED_OWNER_LOSS_PREFIX: &str = "fibril/accepted-owner-loss/";

/// Permission to move a legacy resource off its owner without proof that the
/// new owner holds every confirmed write. Writes only the old owner kept are
/// given up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedOwnerLoss {
    pub version: u32,
    pub resource: ResourceIdentity,
    pub owner: String,
    pub epoch: u64,
}

pub fn accepted_owner_loss_key(resource: &ResourceIdentity) -> String {
    format!(
        "{ACCEPTED_OWNER_LOSS_PREFIX}{}",
        serde_json::to_string(resource).expect("resource identity serializes")
    )
}

/// Coordination identity of a queue or stream partition named by an operator.
pub fn operator_resource(
    stream: bool,
    topic: &str,
    partition: u32,
    group: Option<String>,
) -> Result<ResourceIdentity, String> {
    if stream && group.is_some() {
        return Err("streams have no group".into());
    }
    if group.as_deref().is_some_and(|g| g.is_empty() || g == "default") {
        return Err("the default group is written without a group".into());
    }
    let namespace = if stream {
        crate::STREAM_NAMESPACE
    } else {
        crate::QUEUE_NAMESPACE
    };
    Ok(ResourceIdentity::new(namespace, topic, u64::from(partition), group))
}

/// Whether an operator accepted losing the owner of exactly this assignment.
pub(crate) fn owner_loss_accepted(
    snapshot: &CoordinationSnapshot,
    previous: &PartitionAssignment,
) -> bool {
    snapshot
        .attributes
        .get(&accepted_owner_loss_key(&previous.resource))
        .and_then(|raw| serde_json::from_str::<AcceptedOwnerLoss>(raw).ok())
        .is_some_and(|accepted| {
            accepted.version == 1
                && accepted.resource == previous.resource
                && accepted.owner == previous.owner
                && accepted.epoch == previous.epoch
        })
}

/// Drop acceptances that no longer apply: the assignment changed or is gone,
/// or the owner they gave up on is serving again. A returned owner keeps its
/// data, so a later failure needs a fresh decision.
pub(crate) fn clear_settled_owner_losses(
    snapshot: &mut CoordinationSnapshot,
    owner_serving: impl Fn(&str) -> bool,
) {
    let stale: Vec<String> = snapshot
        .attributes
        .iter()
        .filter(|(key, _)| key.starts_with(ACCEPTED_OWNER_LOSS_PREFIX))
        .filter(|(_, raw)| {
            let Ok(accepted) = serde_json::from_str::<AcceptedOwnerLoss>(raw) else {
                return true;
            };
            match snapshot.assignments.get(&accepted.resource) {
                Some(current) => {
                    !owner_loss_accepted(snapshot, current) || owner_serving(&current.owner)
                }
                None => true,
            }
        })
        .map(|(key, _)| key.clone())
        .collect();
    for key in stale {
        snapshot.attributes.remove(&key);
    }
}

fn error(message: impl ToString) -> OpenraftAdapterError {
    OpenraftAdapterError::Storage(message.to_string())
}

impl GanglionCoordination {
    /// Let the controller move a held legacy resource off its current owner.
    /// Refused for resources with an accepted history (they recover from
    /// sealed evidence instead) and while a recovery request is pending.
    pub async fn accept_owner_loss(
        &self,
        resource: &ResourceIdentity,
    ) -> Result<AcceptedOwnerLoss, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        let current = snapshot
            .assignments
            .get(resource)
            .ok_or_else(|| error("resource has no assignment"))?;
        let incarnation = crate::history_identity::resource_incarnation(&snapshot, resource)
            .map_err(error)?;
        if incarnation.is_some_and(|id| id.version == 2) {
            return Err(error(
                "resource has an accepted history and recovers from sealed replicas, so accepting loss does not apply",
            ));
        }
        if snapshot.attributes.contains_key(&pending_recovery_key(resource)) {
            return Err(error(
                "resource has a pending recovery, withdraw it before accepting loss",
            ));
        }
        let accepted = AcceptedOwnerLoss {
            version: 1,
            resource: resource.clone(),
            owner: current.owner.clone(),
            epoch: current.epoch,
        };
        let key = accepted_owner_loss_key(resource);
        self.forward_command(MetadataRaftCommand::CompareAndSetAttributeGuarded {
            expected_generation: snapshot.generation,
            expected: snapshot.attributes.get(&key).cloned(),
            key,
            value: serde_json::to_string(&accepted).map_err(error)?,
        })
        .await?;
        Ok(accepted)
    }
}

/// Something placement cannot resolve without an operator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecoveryAttention {
    pub resource: ResourceIdentity,
    pub reason: &'static str,
    pub action: &'static str,
}

/// List pending recoveries the automatic driver will never finish and
/// assignments held on a missing owner. Read-only, for status and logs.
pub fn recovery_attention(
    snapshot: &CoordinationSnapshot,
    owner_serving: impl Fn(&str) -> bool,
) -> Vec<RecoveryAttention> {
    let mut out = Vec::new();
    for (key, raw) in &snapshot.attributes {
        if !key.starts_with(crate::promotion::PENDING_RECOVERY_PREFIX) {
            continue;
        }
        let Ok(pending) = serde_json::from_str::<PendingRecovery>(raw) else {
            continue;
        };
        // The driver only recovers queues with an accepted history.
        if pending.previous_activation.is_some() {
            continue;
        }
        let stream = pending.previous.resource.namespace == crate::STREAM_NAMESPACE;
        out.push(RecoveryAttention {
            resource: pending.previous.resource.clone(),
            reason: if stream {
                "stream pending recovery is unsupported for automatic resolution"
            } else {
                "legacy queue pending recovery is unsupported for automatic resolution"
            },
            action: "withdraw it once the owner serves again, or withdraw it and accept owner loss",
        });
    }
    for (resource, current) in &snapshot.assignments {
        if owner_serving(&current.owner)
            || snapshot.attributes.contains_key(&pending_recovery_key(resource))
            || owner_loss_accepted(snapshot, current)
            || crate::promotion::owner_move_loses_nothing(snapshot, resource)
            || crate::promotion::write_requirement(current) != Ok(1)
            || crate::history_identity::resource_incarnation(snapshot, resource)
                .map_or(true, |id| id.is_some_and(|id| id.version == 2))
        {
            continue;
        }
        out.push(RecoveryAttention {
            resource: resource.clone(),
            reason: "only the missing owner holds every confirmed write, so the assignment waits for it",
            action: "bring the owner back, or accept owner loss to move it",
        });
    }
    out.sort_by(|a, b| {
        serde_json::to_string(&a.resource)
            .unwrap_or_default()
            .cmp(&serde_json::to_string(&b.resource).unwrap_or_default())
    });
    out
}

impl GanglionCoordination {
    /// Remove a pending recovery that no automatic stage has acted on, so the
    /// controller can plan the resource again. Refused for queues with an
    /// accepted history: their replicas may already hold durable seals for
    /// the request, which coordination cannot see or undo.
    pub async fn withdraw_pending_recovery(
        &self,
        resource: &ResourceIdentity,
    ) -> Result<PendingRecovery, OpenraftAdapterError> {
        let snapshot = self.node.committed_snapshot();
        let key = pending_recovery_key(resource);
        let raw = snapshot
            .attributes
            .get(&key)
            .ok_or_else(|| error("resource has no pending recovery"))?;
        let pending: PendingRecovery = serde_json::from_str(raw).map_err(error)?;
        if pending.previous_activation.is_some() {
            return Err(error(
                "recovery of an accepted queue history cannot be withdrawn; replicas may already be sealed for it",
            ));
        }
        if snapshot
            .attributes
            .contains_key(&crate::recovery_plan::key(&pending).map_err(error)?)
        {
            return Err(error("recovery already recorded a plan and cannot be withdrawn"));
        }
        let current = snapshot
            .assignments
            .get(resource)
            .ok_or_else(|| error("resource has no assignment"))?
            .clone();
        self.forward_command(MetadataRaftCommand::UpdatePartitionGuarded {
            expected_generation: snapshot.generation,
            assignment: current,
            attributes: std::collections::BTreeMap::from([
                (key, None),
                (crate::recovery_candidate::key(&pending).map_err(error)?, None),
            ]),
        })
        .await?;
        Ok(pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment(owner: &str, epoch: u64) -> PartitionAssignment {
        PartitionAssignment::new(
            ResourceIdentity::new(crate::QUEUE_NAMESPACE, "q", 0, None),
            owner,
            vec![],
            epoch,
        )
    }

    fn accept(snapshot: &mut CoordinationSnapshot, owner: &str, epoch: u64) {
        let a = assignment(owner, epoch);
        let accepted = AcceptedOwnerLoss {
            version: 1,
            resource: a.resource.clone(),
            owner: owner.into(),
            epoch,
        };
        snapshot.attributes.insert(
            accepted_owner_loss_key(&a.resource),
            serde_json::to_string(&accepted).unwrap(),
        );
    }

    #[test]
    fn acceptance_binds_owner_and_epoch() {
        let mut snapshot = CoordinationSnapshot::default();
        accept(&mut snapshot, "a", 3);
        assert!(owner_loss_accepted(&snapshot, &assignment("a", 3)));
        assert!(!owner_loss_accepted(&snapshot, &assignment("a", 4)));
        assert!(!owner_loss_accepted(&snapshot, &assignment("b", 3)));
    }

    #[test]
    fn attention_flags_a_durable_resource_held_on_a_missing_owner() {
        let mut current = assignment("a", 3);
        current.durability = ganglion_core::ReplicationDurabilityPolicy::LocalDurable;
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.assignments.insert(current.resource.clone(), current.clone());
        assert!(recovery_attention(&snapshot, |_| true).is_empty());
        let flagged = recovery_attention(&snapshot, |_| false);
        assert_eq!(flagged.len(), 1);
        assert!(flagged[0].action.contains("accept owner loss"));
        accept(&mut snapshot, "a", 3);
        assert!(recovery_attention(&snapshot, |_| false).is_empty(), "already decided");
    }

    #[test]
    fn settled_acceptances_are_cleared() {
        let current = assignment("a", 3);
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.assignments.insert(current.resource.clone(), current.clone());
        accept(&mut snapshot, "a", 3);
        clear_settled_owner_losses(&mut snapshot, |_| false);
        assert!(owner_loss_accepted(&snapshot, &current), "owner still gone");
        clear_settled_owner_losses(&mut snapshot, |node| node == "a");
        assert!(snapshot.attributes.is_empty(), "owner returned");
        accept(&mut snapshot, "a", 2);
        clear_settled_owner_losses(&mut snapshot, |_| false);
        assert!(snapshot.attributes.is_empty(), "assignment moved on");
    }
}
