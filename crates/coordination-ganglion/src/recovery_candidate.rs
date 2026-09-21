//! Mutable recovery ownership over an immutable sealed history and replica set.
use crate::{GanglionCoordination, promotion::PendingRecovery};
use ganglion_core::{CoordinationSnapshot, PartitionAssignment};

pub(crate) fn key(pending: &PendingRecovery) -> Result<String, String> {
    Ok(format!(
        "fibril/recovery-candidate/{}",
        blake3::Hash::from_bytes(pending.transition_digest()?)
    ))
}

/// Only swap roles inside the fixed proposed replica set. The policy, epoch and
/// storage plan remain unchanged, so every previously completed stage is usable.
pub(crate) fn with_owner(
    pending: &PendingRecovery,
    owner: &str,
) -> Result<PartitionAssignment, String> {
    let mut assignment = pending.proposed.clone();
    if assignment.owner != owner {
        let index = assignment
            .followers
            .iter()
            .position(|node| node == owner)
            .ok_or("recovery candidate is outside the fixed replica set")?;
        assignment.followers[index] = std::mem::replace(&mut assignment.owner, owner.to_owned());
    }
    if crate::promotion::write_requirement(&assignment)? != pending.proposed_write_nodes {
        return Err("recovery candidate changes the write requirement".into());
    }
    Ok(assignment)
}

pub(crate) fn assignment(
    snapshot: &CoordinationSnapshot,
    pending: &PendingRecovery,
) -> Result<PartitionAssignment, String> {
    match snapshot.attributes.get(&key(pending)?) {
        Some(raw) => {
            let candidate: PartitionAssignment =
                serde_json::from_str(raw).map_err(|e| e.to_string())?;
            if with_owner(pending, &candidate.owner)? != candidate {
                return Err("recovery candidate changes the fixed configuration".into());
            }
            Ok(candidate)
        }
        None => with_owner(pending, &pending.proposed.owner),
    }
}

/// Called inside the controller's generation-guarded snapshot update. Liveness
/// chooses who does the work; the unchanged proof still decides whether it is safe.
pub(crate) fn replace_unavailable(
    snapshot: &mut CoordinationSnapshot,
    available: impl Fn(&str) -> bool,
) -> Result<(), String> {
    let requests: Vec<PendingRecovery> = snapshot
        .attributes
        .iter()
        .filter(|(key, _)| key.starts_with(crate::promotion::PENDING_RECOVERY_PREFIX))
        .map(|(_, raw)| serde_json::from_str(raw).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    for pending in requests {
        if pending.proposed.resource.namespace != crate::QUEUE_NAMESPACE
            || pending.previous_activation.is_none()
        {
            continue;
        }
        let current = assignment(snapshot, &pending)?;
        if available(&current.owner) {
            continue;
        }
        // Stable ordering avoids oscillation; a healthy replacement stays chosen
        // even if the original candidate returns.
        let next = std::iter::once(&pending.proposed.owner)
            .chain(pending.proposed.followers.iter())
            .filter(|node| available(node))
            .min();
        let Some(next) = next else { continue };
        let replacement = with_owner(&pending, next)?;
        tracing::info!(topic = pending.proposed.resource.name,
            partition = pending.proposed.resource.partition,
            previous_candidate = current.owner, candidate = next,
            required_write_nodes = pending.proposed_write_nodes,
            transition = %blake3::Hash::from_bytes(pending.transition_digest()?),
            "proposing recovery candidate replacement within the fixed replica set");
        snapshot.attributes.insert(
            key(&pending)?,
            serde_json::to_string(&replacement).map_err(|e| e.to_string())?,
        );
    }
    Ok(())
}

impl GanglionCoordination {
    /// Advisory driver selection. Activation checks this again through a fresh,
    /// generation-guarded consensus write; a cached answer grants no authority.
    pub fn queue_recovery_candidate(
        &self,
        pending: &PendingRecovery,
    ) -> Result<PartitionAssignment, String> {
        let snapshot = self.node.committed_snapshot();
        crate::recovery_witnesses::RecoveryWitnessSet::new(&snapshot, pending)?;
        assignment(&snapshot, pending)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ganglion_core::{ReplicationDurabilityPolicy, ResourceIdentity};

    fn fixture() -> (CoordinationSnapshot, PendingRecovery) {
        let resource = ResourceIdentity::new(crate::QUEUE_NAMESPACE, "q", 0, None::<String>);
        let mut old =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 1);
        old.durability = ReplicationDurabilityPolicy::MajorityDurable;
        let mut proposed = old.clone();
        proposed.owner = "b".into();
        proposed.followers = vec!["a".into(), "c".into()];
        proposed.epoch += 1;
        let pending = PendingRecovery {
            version: 1,
            resource_incarnation: None,
            previous_activation: Some([1; 32]),
            requested_generation: 2,
            previous: old.clone(),
            proposed,
            previous_write_nodes: 2,
            proposed_write_nodes: 2,
            required_old_witnesses: 2,
        };
        let mut snapshot = CoordinationSnapshot::default();
        snapshot.assignments.insert(resource.clone(), old);
        snapshot.attributes.insert(
            crate::promotion::pending_recovery_key(&resource),
            serde_json::to_string(&pending).unwrap(),
        );
        (snapshot, pending)
    }

    #[test]
    fn handoff_preserves_proof_and_is_sticky_until_candidate_is_unavailable() {
        let (mut snapshot, pending) = fixture();
        let old = snapshot.clone();
        replace_unavailable(&mut snapshot, |_| true).unwrap();
        assert_eq!(snapshot, old);
        replace_unavailable(&mut snapshot, |n| n == "c").unwrap();
        assert_eq!(assignment(&snapshot, &pending).unwrap().owner, "c");
        assert_eq!(snapshot.assignments, old.assignments);
        assert_eq!(
            snapshot.attributes
                [&crate::promotion::pending_recovery_key(&pending.previous.resource)],
            old.attributes[&crate::promotion::pending_recovery_key(&pending.previous.resource)]
        );
        let handed = snapshot.clone();
        replace_unavailable(&mut snapshot, |_| true).unwrap();
        assert_eq!(snapshot, handed);
        replace_unavailable(&mut snapshot, |_| false).unwrap();
        assert_eq!(snapshot, handed);
        replace_unavailable(&mut snapshot, |n| n == "outsider").unwrap();
        assert_eq!(snapshot, handed);
        replace_unavailable(&mut snapshot, |n| n == "b").unwrap();
        assert_eq!(assignment(&snapshot, &pending).unwrap(), pending.proposed);
    }

    #[test]
    fn handoff_rejects_membership_policy_epoch_and_resource_changes() {
        let (snapshot, pending) = fixture();
        assert!(with_owner(&pending, "outsider").is_err());
        for field in ["owner", "followers", "epoch", "durability", "resource"] {
            let mut changed = with_owner(&pending, "c").unwrap();
            match field {
                "owner" => changed.owner = "outsider".into(),
                "followers" => changed.followers.clear(),
                "epoch" => changed.epoch += 1,
                "durability" => changed.durability = ReplicationDurabilityPolicy::LocalDurable,
                _ => changed.resource.name = "another".into(),
            }
            let mut bad = snapshot.clone();
            bad.attributes.insert(
                key(&pending).unwrap(),
                serde_json::to_string(&changed).unwrap(),
            );
            assert!(assignment(&bad, &pending).is_err(), "{field}");
        }
    }
}
