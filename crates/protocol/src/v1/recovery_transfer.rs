//! Bounded authenticated recovery RPC. A lost reply can be retried under the
//! same plan: staging is offset-idempotent and installation never resets an
//! already active generation.
use super::{
    Op, RecoveryTransfer,
    helper::try_encode,
    replication::{
        ProtocolOwnerPeerResolverConfig, open_protocol_owner_conn, protocol_error, recv_response,
    },
};
use fibril_broker::{
    broker::BrokerError,
    recovery_transfer::{QueueRecoveryReply, QueueRecoveryRequest},
};
use futures::SinkExt;
use std::time::Duration;
pub async fn request_transfer(
    config: &ProtocolOwnerPeerResolverConfig,
    request: &QueueRecoveryRequest,
    deadline: Duration,
) -> Result<QueueRecoveryReply, BrokerError> {
    if config
        .auth
        .as_ref()
        .is_none_or(|a| a.username != fibril_broker::auth_store::NODE_PRINCIPAL)
    {
        return Err(BrokerError::InvalidArgument(
            "recovery transfer requires cluster peer credentials".into(),
        ));
    }
    let addr = config
        .nodes
        .get(&request.command.replica_id)
        .ok_or_else(|| BrokerError::InvalidArgument("recovery replica has no address".into()))?;
    let body = rmp_serde::to_vec_named(request).map_err(protocol_error)?;
    let frame =
        try_encode(Op::RecoveryTransfer, 3, &RecoveryTransfer { body }).map_err(protocol_error)?;
    tokio::time::timeout(deadline, async {
        let mut conn = open_protocol_owner_conn(
            addr.clone(),
            config.auth.as_ref(),
            config.tls.as_ref(),
            &config.client_name,
            &config.client_version,
            config.owner_connect_timeout_ms,
        )
        .await?;
        conn.send(frame).await.map_err(protocol_error)?;
        let reply: RecoveryTransfer = recv_response(&mut conn, 3, Op::RecoveryTransferOk)
            .await
            .map_err(protocol_error)?;
        decode_body(&reply.body).map_err(protocol_error)
    })
    .await
    .map_err(|_| {
        BrokerError::Unknown("recovery transfer deadline elapsed; retry the same plan".into())
    })?
}

pub(super) fn decode_body<T: serde::de::DeserializeOwned>(body: &[u8]) -> Result<T, String> {
    if body.len() > super::MAX_RECOVERY_TRANSFER_FRAME_BYTES {
        return Err("recovery body exceeds limit".into());
    }
    let mut decoder = rmp_serde::Deserializer::new(std::io::Cursor::new(body));
    let value = T::deserialize(&mut decoder).map_err(|e| e.to_string())?;
    if decoder.position() != body.len() as u64 {
        return Err("trailing recovery control bytes".into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recovery_control_body_rejects_trailing_objects_and_truncation() {
        let bytes = rmp_serde::to_vec_named(&QueueRecoveryReply::Progress(7)).unwrap();
        assert!(matches!(
            decode_body::<QueueRecoveryReply>(&bytes).unwrap(),
            QueueRecoveryReply::Progress(7)
        ));
        for end in 0..bytes.len() {
            assert!(decode_body::<QueueRecoveryReply>(&bytes[..end]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(
            decode_body::<QueueRecoveryReply>(&trailing)
                .unwrap_err()
                .contains("trailing")
        );
    }
}
