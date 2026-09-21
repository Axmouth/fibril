//! Catalogue-incarnation identity. This identifies a declaration lifetime, not
//! a trusted storage baseline, writer session, or accepted recovery history.
//! Version 2 additionally records creation under the history-admission protocol:
//! ordinary serving is withheld until that protocol can authorize activation.
use ganglion_core::{CoordinationSnapshot, ResourceIdentity};
use ganglion_openraft::{MetadataRaftCommand, OpenraftAdapterError};
use serde::{Deserialize, Serialize};

const PREFIX: &str = "fibril/resource-incarnation/";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceIncarnation {
    pub version: u32,
    pub resource: ResourceIdentity,
    pub id: [u8; 16],
    /// Retained after enrolled catalogue deletion until placement has retired.
    #[serde(default, skip_serializing_if = "is_false")]
    pub retired: bool,
}

fn is_false(value: &bool) -> bool {
    !value
}

pub(crate) fn key(resource: &ResourceIdentity) -> String {
    format!(
        "{PREFIX}{}",
        serde_json::to_string(resource).expect("resource identity serializes")
    )
}

pub(crate) fn registration(
    resource: ResourceIdentity,
) -> Result<MetadataRaftCommand, OpenraftAdapterError> {
    registration_version(resource, 1)
}

pub(crate) fn enrolled_registration(
    resource: ResourceIdentity,
) -> Result<MetadataRaftCommand, OpenraftAdapterError> {
    registration_version(resource, 2)
}

fn registration_version(
    resource: ResourceIdentity,
    version: u32,
) -> Result<MetadataRaftCommand, OpenraftAdapterError> {
    let identity = ResourceIncarnation {
        version,
        resource: resource.clone(),
        id: *uuid::Uuid::now_v7().as_bytes(),
        retired: false,
    };
    Ok(MetadataRaftCommand::RegisterResourceWithInitialAttribute {
        key: key(&resource),
        resource,
        value: serde_json::to_string(&identity)
            .map_err(|e| OpenraftAdapterError::Storage(e.to_string()))?,
    })
}

/// Missing identity is legacy/unverified. Reading cannot create an origin proof.
pub fn resource_incarnation(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
) -> Result<Option<ResourceIncarnation>, String> {
    let Some(raw) = snapshot.attributes.get(&key(resource)) else {
        return Ok(None);
    };
    let identity: ResourceIncarnation =
        serde_json::from_str(raw).map_err(|e| format!("invalid resource incarnation: {e}"))?;
    if !matches!(identity.version, 1 | 2)
        || identity.resource != *resource
        || identity.id == [0; 16]
        || identity.version == 1 && identity.retired
    {
        return Err("resource incarnation does not identify the requested resource".into());
    }
    Ok(Some(identity))
}

/// Missing identity retains legacy behavior. Malformed or enrolled identities
/// cannot fall back to legacy serving, including before storage is prepared.
pub(crate) fn permits_legacy_serving(
    snapshot: &CoordinationSnapshot,
    resource: &ResourceIdentity,
) -> bool {
    match resource_incarnation(snapshot, resource) {
        Ok(None) => true,
        Ok(Some(identity)) => identity.version == 1,
        Err(error) => {
            tracing::warn!(namespace = resource.namespace, topic = resource.name,
                partition = resource.partition, %error,
                "invalid history incarnation; withholding serving assignment");
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incarnation_parsing_rejects_corruption_and_cross_resource_identity() {
        let resource = ResourceIdentity::new("fibril/queue", "q", 0, Some("g".to_string()));
        let mut snapshot = CoordinationSnapshot::default();
        assert_eq!(resource_incarnation(&snapshot, &resource).unwrap(), None);
        let MetadataRaftCommand::RegisterResourceWithInitialAttribute { key, value, .. } =
            registration(resource.clone()).unwrap()
        else {
            panic!("registration command")
        };
        snapshot.attributes.insert(key.clone(), value.clone());
        let identity = resource_incarnation(&snapshot, &resource).unwrap().unwrap();
        assert!(
            !value.contains("retired"),
            "legacy serialization must stay unchanged"
        );
        for mutation in 0..5 {
            let mut bad = identity.clone();
            match mutation {
                0 => bad.version = 3,
                1 => bad.id = [0; 16],
                2 => {
                    bad.resource =
                        ResourceIdentity::new("fibril/stream", "q", 0, Some("g".to_string()))
                }
                3 => {
                    bad.resource =
                        ResourceIdentity::new("fibril/queue", "q", 1, Some("g".to_string()))
                }
                _ => {
                    bad.resource =
                        ResourceIdentity::new("fibril/queue", "q", 0, Some("other".to_string()))
                }
            }
            snapshot
                .attributes
                .insert(key.clone(), serde_json::to_string(&bad).unwrap());
            assert!(resource_incarnation(&snapshot, &resource).is_err());
        }
        snapshot.attributes.insert(key, "{torn".into());
        assert!(resource_incarnation(&snapshot, &resource).is_err());
    }
}
