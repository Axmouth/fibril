//! Persisted recovery requests. Placement hints cannot activate a new history.
//!
//! The first stage deliberately retains the active assignment until the sealing,
//! history transfer and activation protocol can supply a recovery certificate.
//! This prevents ordinary follower refresh from overwriting surviving evidence.

use ganglion_core::{
    CoordinationSnapshot, PartitionAssignment, ReplicationDurabilityPolicy, ResourceIdentity,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const PENDING_RECOVERY_PREFIX: &str = "fibril/pending-recovery/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingRecovery {
    pub version: u32,
    /// Declaration lifetime, when recorded at creation. Absence preserves legacy
    /// transition serialization and remains an unresolved origin requirement.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_incarnation: Option<crate::history_identity::ResourceIncarnation>,
    pub requested_generation: u64,
    pub previous: PartitionAssignment,
    pub proposed: PartitionAssignment,
    /// Effective write counts, including the stream policy omitted by the
    /// generic assignment projection. Both counts include their owner.
    pub previous_write_nodes: usize,
    pub proposed_write_nodes: usize,
    /// Generic sufficient read/seal quorum: R + W > N for the old configuration.
    /// The count alone does not establish history identity.
    pub required_old_witnesses: usize,
}

pub fn pending_recovery_key(resource: &ResourceIdentity) -> String {
    // A serialized structured identity avoids topic/group separator collisions.
    format!(
        "{PENDING_RECOVERY_PREFIX}{}",
        serde_json::to_string(resource).expect("resource identity serializes")
    )
}

pub(crate) fn write_requirement(assignment: &PartitionAssignment) -> Result<usize, String> {
    let mut nodes = BTreeSet::from([assignment.owner.as_str()]);
    if assignment.followers.iter().any(|id| !nodes.insert(id)) {
        return Err("replica configuration contains duplicate identities".into());
    }
    // Stream assignments use the local-policy field in Ganglion's projection;
    // their broker confirmation rule requires every assigned durable copy.
    let policy = if assignment.resource.namespace == super::STREAM_NAMESPACE
        && !assignment.followers.is_empty()
    {
        ReplicationDurabilityPolicy::ReplicaDurable { nodes: nodes.len() }
    } else {
        assignment.durability
    };
    policy
        .resolve(nodes.len())
        .map(|r| r.nodes)
        .map_err(|e| format!("invalid replication policy: {e:?}"))
}

fn same_configuration(a: &PartitionAssignment, b: &PartitionAssignment) -> bool {
    a.owner == b.owner
        && a.durability == b.durability
        && a.followers.iter().collect::<BTreeSet<_>>()
            == b.followers.iter().collect::<BTreeSet<_>>()
}

fn recovery_witness_requirement(assignment: &PartitionAssignment) -> Result<usize, String> {
    Ok(assignment.replica_set_size() - write_requirement(assignment)? + 1)
}

/// Hold changes affecting a replicated confirmation contract and persist the
/// original configuration beside the proposed replacement in the same CAS.
/// Existing requests are immutable across controller retries and restarts.
pub(crate) fn retain_unproven_assignments(
    committed: &CoordinationSnapshot,
    desired: &mut CoordinationSnapshot,
) -> Result<usize, String> {
    let mut held = 0;
    for (resource, proposed) in &mut desired.assignments {
        let key = pending_recovery_key(resource);
        let Some(previous) = committed.assignments.get(resource) else {
            if committed.attributes.contains_key(&key) {
                return Err(
                    "pending recovery lost its previous assignment; explicit recovery required"
                        .into(),
                );
            }
            continue;
        };
        if let Some(raw) = committed.attributes.get(&key) {
            let pending: PendingRecovery =
                serde_json::from_str(raw).map_err(|e| format!("invalid pending recovery: {e}"))?;
            if pending.version != 1
                || pending.previous != *previous
                || pending.proposed.resource != *resource
                || pending.resource_incarnation
                    != crate::history_identity::resource_incarnation(committed, resource)?
            {
                return Err("pending recovery does not match the active assignment".into());
            }
            *proposed = previous.clone();
            held += 1;
            continue;
        }
        let old_writes = write_requirement(previous)?;
        let new_writes = write_requirement(proposed)?;
        let incarnation = crate::history_identity::resource_incarnation(committed, resource)?;
        let enrolled = incarnation.as_ref().is_some_and(|id| id.version == 2);
        // An enrolled activation binds the exact assignment, even for one local
        // writer. Moving/replacing that writer needs a continuation certificate.
        if if enrolled {
            previous == proposed
        } else {
            same_configuration(previous, proposed) || old_writes == 1 && new_writes == 1
        } {
            continue;
        }
        let mut replacement = proposed.clone();
        replacement.epoch = previous
            .epoch
            .checked_add(1)
            .ok_or("assignment epoch exhausted")?;
        let pending = PendingRecovery {
            version: 1,
            resource_incarnation: incarnation,
            requested_generation: desired.generation,
            previous: previous.clone(),
            proposed: replacement,
            previous_write_nodes: old_writes,
            proposed_write_nodes: new_writes,
            required_old_witnesses: recovery_witness_requirement(previous)?,
        };
        desired.attributes.insert(
            key,
            serde_json::to_string(&pending).map_err(|e| e.to_string())?,
        );
        *proposed = previous.clone();
        held += 1;
    }
    Ok(held)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assignment(owner: &str, followers: &[&str]) -> PartitionAssignment {
        let mut a = PartitionAssignment::new(
            ResourceIdentity::new(super::super::QUEUE_NAMESPACE, "q", 0, None),
            owner,
            followers.iter().map(|x| x.to_string()).collect(),
            3,
        );
        a.durability = ReplicationDurabilityPolicy::MajorityDurable;
        a
    }
    fn snapshot(a: PartitionAssignment) -> CoordinationSnapshot {
        let mut s = CoordinationSnapshot::default();
        s.assignments.insert(a.resource.clone(), a);
        s
    }
    #[test]
    fn pending_recovery_retains_old_membership_and_survives_serialization() {
        let old = snapshot(assignment("a", &["b", "c"]));
        let mut desired = snapshot(assignment("c", &["b"]));
        desired.generation = 7;
        assert_eq!(retain_unproven_assignments(&old, &mut desired).unwrap(), 1);
        assert_eq!(desired.assignments, old.assignments);
        let restored: CoordinationSnapshot =
            serde_json::from_str(&serde_json::to_string(&desired).unwrap()).unwrap();
        let request: PendingRecovery =
            serde_json::from_str(restored.attributes.values().next().unwrap()).unwrap();
        assert_eq!(request.required_old_witnesses, 2);
        assert_eq!(request.proposed.owner, "c");
        assert_eq!(request.proposed.epoch, 4);
        assert_eq!(request.previous.followers, vec!["b", "c"]);
        let mut retry = restored.clone();
        retry.assignments = snapshot(assignment("b", &["c"])).assignments;
        retain_unproven_assignments(&restored, &mut retry).unwrap();
        assert_eq!(retry, restored);
    }
    #[test]
    fn follower_only_and_policy_changes_require_recovery() {
        let old = snapshot(assignment("a", &["b", "c"]));
        for mut changed in [assignment("a", &["b", "d"]), assignment("a", &["b", "c"])] {
            if changed.followers[1] == "c" {
                changed.durability = ReplicationDurabilityPolicy::LocalDurable;
            }
            let mut desired = snapshot(changed);
            assert_eq!(retain_unproven_assignments(&old, &mut desired).unwrap(), 1);
            assert_eq!(desired.assignments, old.assignments);
        }
        let mut reordered = snapshot(assignment("a", &["c", "b"]));
        assert_eq!(
            retain_unproven_assignments(&old, &mut reordered).unwrap(),
            0
        );
    }
    #[test]
    fn enrolled_local_writer_changes_require_recovery() {
        let mut original = assignment("a", &[]);
        original.durability = ReplicationDurabilityPolicy::LocalDurable;
        let resource = original.resource.clone();
        let mut old = snapshot(original.clone());
        let identity = crate::history_identity::ResourceIncarnation {
            version: 2,
            resource: resource.clone(),
            id: [1; 16],
            retired: false,
        };
        old.attributes.insert(
            crate::history_identity::key(&resource),
            serde_json::to_string(&identity).unwrap(),
        );
        let mut changed = original.clone();
        changed.owner = "b".into();
        let mut desired = old.clone();
        desired.assignments.insert(resource.clone(), changed);
        assert_eq!(retain_unproven_assignments(&old, &mut desired).unwrap(), 1);
        assert_eq!(desired.assignments, old.assignments);
        let mut epoch_change = old.clone();
        epoch_change.assignments.get_mut(&resource).unwrap().epoch += 1;
        assert_eq!(
            retain_unproven_assignments(&old, &mut epoch_change).unwrap(),
            1
        );
        let mut identical = old.clone();
        assert_eq!(
            retain_unproven_assignments(&old, &mut identical).unwrap(),
            0
        );
    }

    #[test]
    fn malformed_pending_request_never_unlocks_assignment() {
        let mut old = snapshot(assignment("a", &["b", "c"]));
        let key = pending_recovery_key(old.assignments.keys().next().unwrap());
        old.attributes.insert(key, "{}".into());
        assert!(retain_unproven_assignments(&old, &mut snapshot(assignment("c", &["b"]))).is_err());
    }

    #[test]
    fn duplicate_replica_ids_cannot_shrink_the_recovery_requirement() {
        let duplicate = assignment("a", &["b", "b"]);
        assert!(recovery_witness_requirement(&duplicate).is_err());
        let owner_repeated = assignment("a", &["a", "b"]);
        assert!(recovery_witness_requirement(&owner_repeated).is_err());
    }
    #[test]
    fn stream_replica_changes_are_guarded_despite_projection_policy() {
        let mut a = assignment("a", &["b", "c"]);
        a.resource.namespace = super::super::STREAM_NAMESPACE.into();
        a.durability = ReplicationDurabilityPolicy::LocalDurable;
        let old = snapshot(a.clone());
        a.owner = "b".into();
        a.followers = vec!["c".into()];
        let mut desired = snapshot(a);
        assert_eq!(retain_unproven_assignments(&old, &mut desired).unwrap(), 1);
        assert_eq!(desired.assignments, old.assignments);
        let request: PendingRecovery =
            serde_json::from_str(desired.attributes.values().next().unwrap()).unwrap();
        assert_eq!(request.previous_write_nodes, 3);
        assert_eq!(request.proposed_write_nodes, 2);
        assert_eq!(request.required_old_witnesses, 1);
    }
    #[test]
    fn recovery_threshold_intersects_every_possible_write_quorum() {
        for n in 1usize..=7 {
            for w in 1usize..=n {
                let mut config = assignment("owner", &[]);
                config.followers = (1..n).map(|id| format!("follower-{id}")).collect();
                config.durability = ReplicationDurabilityPolicy::ReplicaDurable { nodes: w };
                let r = recovery_witness_requirement(&config).unwrap();
                for writers in 0u32..1 << n {
                    if writers.count_ones() as usize != w {
                        continue;
                    }
                    for readers in 0u32..1 << n {
                        if readers.count_ones() as usize == r {
                            assert_ne!(writers & readers, 0);
                        }
                    }
                }
            }
        }
    }
}

impl PendingRecovery {
    /// Deterministic identity of the complete transition, including its old
    /// confirmation contract. Follower ordering is retained in this exact record.
    pub fn transition_digest(&self) -> Result<[u8; 32], String> {
        let mut hash = blake3::Hasher::new();
        hash.update(b"fibril-pending-recovery-v1\0");
        hash.update(&serde_json::to_vec(self).map_err(|err| err.to_string())?);
        Ok(*hash.finalize().as_bytes())
    }

    pub fn seal_command(&self) -> Result<fibril_broker::recovery::RecoverySealCommand, String> {
        Ok(fibril_broker::recovery::RecoverySealCommand {
            topic: self.previous.resource.name.clone(),
            partition: fibril_broker::Partition::new(
                u32::try_from(self.previous.resource.partition)
                    .map_err(|_| "recovery partition exceeds wire range")?,
            ),
            group: self.previous.resource.group.clone(),
            stream: self.previous.resource.namespace == super::STREAM_NAMESPACE,
            transition: self.transition_digest()?,
            fence_epoch: self.proposed.epoch,
        })
    }
}

pub(crate) fn validate_seal_command(
    snapshot: &CoordinationSnapshot,
    local_node: &str,
    command: &fibril_broker::recovery::RecoverySealCommand,
) -> Result<(String, String), String> {
    if command
        .group
        .as_deref()
        .is_some_and(|group| group.is_empty() || group == "default")
        || command.stream && command.group.is_some()
    {
        return Err("noncanonical recovery resource identity".into());
    }
    let resource = ResourceIdentity::new(
        if command.stream {
            super::STREAM_NAMESPACE
        } else {
            super::QUEUE_NAMESPACE
        },
        &command.topic,
        u64::from(command.partition.id()),
        command.group.clone(),
    );
    let key = pending_recovery_key(&resource);
    let raw = snapshot
        .attributes
        .get(&key)
        .ok_or("no pending recovery for this resource")?;
    let pending: PendingRecovery =
        serde_json::from_str(raw).map_err(|e| format!("invalid pending recovery: {e}"))?;
    if pending.version != 1
        || pending.requested_generation > snapshot.generation
        || pending.previous.resource != resource
        || pending.proposed.resource != resource
        || pending.resource_incarnation
            != crate::history_identity::resource_incarnation(snapshot, &resource)?
        || snapshot.assignments.get(&resource) != Some(&pending.previous)
        || pending.previous.epoch.checked_add(1) != Some(pending.proposed.epoch)
        || pending.previous_write_nodes != write_requirement(&pending.previous)?
        || pending.proposed_write_nodes != write_requirement(&pending.proposed)?
        || pending.required_old_witnesses != recovery_witness_requirement(&pending.previous)?
        || pending.seal_command()? != *command
    {
        return Err("recovery request does not match the committed transition".into());
    }
    if pending.previous.owner != local_node
        && !pending.previous.followers.iter().any(|n| n == local_node)
    {
        return Err("local node is not an old recovery replica".into());
    }
    Ok((key, raw.clone()))
}

#[cfg(test)]
mod seal_tests {
    use super::*;
    fn pending() -> (CoordinationSnapshot, PendingRecovery) {
        let resource = ResourceIdentity::new(super::super::QUEUE_NAMESPACE, "q", 0, None);
        let mut old =
            PartitionAssignment::new(resource.clone(), "a", vec!["b".into(), "c".into()], 7);
        old.durability = ReplicationDurabilityPolicy::MajorityDurable;
        let mut committed = CoordinationSnapshot::default();
        committed.assignments.insert(resource.clone(), old.clone());
        let mut desired = committed.clone();
        desired.generation = 9;
        let next = desired.assignments.get_mut(&resource).unwrap();
        next.owner = "b".into();
        next.followers = vec!["a".into(), "c".into()];
        retain_unproven_assignments(&committed, &mut desired).unwrap();
        let pending =
            serde_json::from_str(&desired.attributes[&pending_recovery_key(&resource)]).unwrap();
        (desired, pending)
    }

    #[test]
    fn seal_authorization_binds_exact_transition_and_old_membership() {
        let (snapshot, pending) = pending();
        let command = pending.seal_command().unwrap();
        for node in ["a", "b", "c"] {
            validate_seal_command(&snapshot, node, &command).unwrap();
        }
        assert!(validate_seal_command(&snapshot, "outsider", &command).is_err());
        for mutate in 0..6 {
            let mut wrong = command.clone();
            match mutate {
                0 => wrong.transition[0] ^= 1,
                1 => wrong.fence_epoch += 1,
                2 => wrong.stream = true,
                3 => wrong.group = Some("another".into()),
                4 => wrong.partition = fibril_broker::Partition::new(1),
                _ => wrong.topic = "other".into(),
            }
            assert!(validate_seal_command(&snapshot, "b", &wrong).is_err());
        }
        let mut newer = snapshot.clone();
        newer
            .assignments
            .insert(pending.previous.resource.clone(), pending.proposed.clone());
        assert!(validate_seal_command(&newer, "b", &command).is_err());
    }

    #[test]
    fn seal_binds_incarnation_and_preserves_legacy_transition_encoding() {
        let (mut snapshot, mut pending) = pending();
        let legacy = serde_json::to_string(&pending).unwrap();
        assert!(!legacy.contains("resource_incarnation"));
        let decoded: PendingRecovery = serde_json::from_str(&legacy).unwrap();
        assert_eq!(serde_json::to_string(&decoded).unwrap(), legacy);
        let legacy_command = decoded.seal_command().unwrap();
        let resource = pending.previous.resource.clone();
        let identity = crate::history_identity::ResourceIncarnation {
            version: 1,
            resource: resource.clone(),
            id: [1; 16],
            retired: false,
        };
        pending.resource_incarnation = Some(identity.clone());
        snapshot.attributes.insert(
            crate::history_identity::key(&resource),
            serde_json::to_string(&identity).unwrap(),
        );
        snapshot.attributes.insert(
            pending_recovery_key(&resource),
            serde_json::to_string(&pending).unwrap(),
        );
        let command = pending.seal_command().unwrap();
        assert_ne!(command.transition, legacy_command.transition);
        validate_seal_command(&snapshot, "b", &command).unwrap();
        assert!(validate_seal_command(&snapshot, "b", &legacy_command).is_err());
        for value in [
            None,
            Some("{torn".to_string()),
            Some(
                serde_json::to_string(&crate::history_identity::ResourceIncarnation {
                    id: [2; 16],
                    ..identity.clone()
                })
                .unwrap(),
            ),
        ] {
            let mut stale = snapshot.clone();
            let key = crate::history_identity::key(&resource);
            if let Some(value) = value {
                stale.attributes.insert(key, value);
            } else {
                stale.attributes.remove(&key);
            }
            assert!(validate_seal_command(&stale, "b", &command).is_err());
            let mut desired = stale.clone();
            assert!(retain_unproven_assignments(&stale, &mut desired).is_err());
        }
    }

    #[test]
    fn seal_rejects_corrupt_contract_even_with_matching_digest() {
        let (original, pending) = pending();
        for mutation in 0..5 {
            let mut bad = pending.clone();
            match mutation {
                0 => bad.previous_write_nodes = 1,
                1 => bad.required_old_witnesses = 1,
                2 => bad.requested_generation = original.generation + 1,
                3 => bad.proposed.epoch = bad.previous.epoch,
                _ => bad.proposed.followers = vec![bad.proposed.owner.clone()],
            }
            let mut snapshot = original.clone();
            snapshot.attributes.insert(
                pending_recovery_key(&bad.previous.resource),
                serde_json::to_string(&bad).unwrap(),
            );
            assert!(validate_seal_command(&snapshot, "b", &bad.seal_command().unwrap()).is_err());
        }
    }
}
