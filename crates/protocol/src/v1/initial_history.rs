//! Explicit node-authenticated initial preparation. No automatic fan-out or
//! activation is implied by a successful response.
use super::{
    helper::try_encode,
    replication::{
        open_protocol_owner_conn, protocol_error, recv_response, ProtocolOwnerPeerResolverConfig,
    },
    InitialHistoryPrepare, InitialHistoryPrepareOk, Op,
};
use fibril_broker::{
    broker::BrokerError,
    initial_history::{InitialHistoryLocalReceipt, InitialHistoryPrepareCommand},
    queue_engine::{PreparedStorageHistory, StorageHistoryBinding},
};
use futures::SinkExt;
use std::time::Duration;

pub(super) fn wire_command(command: &InitialHistoryPrepareCommand) -> InitialHistoryPrepare {
    InitialHistoryPrepare {
        replica_id: command.replica_id.clone(),
        topic: command.topic.clone(),
        partition: command.partition,
        group: command.group.clone(),
        stream: command.stream,
        decision: command.decision,
        resource_incarnation: command.binding.resource_incarnation,
        accepted_history: command.binding.accepted_history,
        writer_session: command.binding.writer_session,
    }
}

pub(super) fn broker_command(request: InitialHistoryPrepare) -> InitialHistoryPrepareCommand {
    InitialHistoryPrepareCommand {
        replica_id: request.replica_id,
        topic: request.topic,
        partition: request.partition,
        group: request.group,
        stream: request.stream,
        decision: request.decision,
        binding: StorageHistoryBinding {
            resource_incarnation: request.resource_incarnation,
            accepted_history: request.accepted_history,
            writer_session: request.writer_session,
        },
    }
}

pub(super) fn wire_receipt(receipt: InitialHistoryLocalReceipt) -> InitialHistoryPrepareOk {
    InitialHistoryPrepareOk {
        prepared: InitialHistoryPrepare {
            replica_id: receipt.node_id,
            topic: receipt.storage.topic,
            partition: fibril_broker::Partition::new(receipt.storage.partition),
            group: receipt.storage.group,
            stream: receipt.storage.stream,
            decision: receipt.decision,
            resource_incarnation: receipt.storage.binding.resource_incarnation,
            accepted_history: receipt.storage.binding.accepted_history,
            writer_session: receipt.storage.binding.writer_session,
        },
        replica_process: receipt.replica_process,
        storage_instance: receipt.storage.storage_instance,
    }
}

fn validate_reply(
    command: &InitialHistoryPrepareCommand,
    reply: InitialHistoryPrepareOk,
) -> Result<InitialHistoryLocalReceipt, BrokerError> {
    let received = broker_command(reply.prepared);
    let receipt = InitialHistoryLocalReceipt {
        decision: received.decision,
        node_id: received.replica_id,
        replica_process: reply.replica_process,
        storage: PreparedStorageHistory {
            topic: received.topic,
            partition: received.partition.id(),
            group: received.group,
            stream: received.stream,
            binding: received.binding,
            storage_instance: reply.storage_instance,
        },
    };
    command
        .validate_receipt(&receipt)
        .map_err(BrokerError::InvalidArgument)?;
    Ok(receipt)
}

/// Each attempt opens a fresh authenticated connection. The deadline covers
/// connect, handshake, authorization and the reply. A timeout can leave inert
/// preparation running remotely; retry the same committed command.
pub async fn request_preparation(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &InitialHistoryPrepareCommand,
    deadline: Duration,
) -> Result<InitialHistoryLocalReceipt, BrokerError> {
    if config
        .auth
        .as_ref()
        .is_none_or(|auth| auth.username != fibril_broker::auth_store::NODE_PRINCIPAL)
    {
        return Err(BrokerError::InvalidArgument(
            "initial preparation requires cluster peer credentials".into(),
        ));
    }
    let addr = config.nodes.get(&command.replica_id).ok_or_else(|| {
        BrokerError::InvalidArgument("initial preparation replica has no configured address".into())
    })?;
    let request =
        try_encode(Op::InitialHistoryPrepare, 3, &wire_command(command)).map_err(protocol_error)?;
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
        conn.send(request).await.map_err(protocol_error)?;
        let reply: InitialHistoryPrepareOk =
            recv_response(&mut conn, 3, Op::InitialHistoryPrepareOk)
                .await
                .map_err(protocol_error)?;
        validate_reply(command, reply)
    })
    .await
    .map_err(|_| {
        BrokerError::Unknown(
            "initial preparation deadline elapsed; retry the identical request".into(),
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    fn command() -> InitialHistoryPrepareCommand {
        InitialHistoryPrepareCommand {
            replica_id: "b".into(),
            topic: "q".into(),
            partition: fibril_broker::Partition::new(0),
            group: None,
            stream: false,
            decision: [1; 32],
            binding: StorageHistoryBinding {
                resource_incarnation: [2; 16],
                accepted_history: [3; 16],
                writer_session: [4; 16],
            },
        }
    }
    #[test]
    fn replies_require_exact_target_decision_storage_and_nonzero_instances() {
        let command = command();
        let reply = InitialHistoryPrepareOk {
            prepared: wire_command(&command),
            replica_process: [5; 16],
            storage_instance: [6; 16],
        };
        validate_reply(&command, reply.clone()).unwrap();
        for mutation in 0..11 {
            let mut bad = reply.clone();
            match mutation {
                0 => bad.prepared.replica_id = "a".into(),
                1 => bad.prepared.topic = "other".into(),
                2 => bad.prepared.partition = fibril_broker::Partition::new(1),
                3 => bad.prepared.group = Some("g".into()),
                4 => bad.prepared.stream = true,
                5 => bad.prepared.decision[0] ^= 1,
                6 => bad.prepared.resource_incarnation[0] ^= 1,
                7 => bad.prepared.accepted_history[0] ^= 1,
                8 => bad.prepared.writer_session[0] ^= 1,
                9 => bad.replica_process = [0; 16],
                _ => bad.storage_instance = [0; 16],
            }
            assert!(
                validate_reply(&command, bad).is_err(),
                "mutation {mutation}"
            );
        }
    }
    #[tokio::test]
    async fn setup_and_reply_share_a_bounded_deadline() {
        let listener = fibril_util::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let config = ProtocolOwnerPeerResolverConfig::new(std::collections::HashMap::from([(
            "b".into(),
            listener.local_addr().unwrap().to_string(),
        )]))
        .with_auth("@node", "secret");
        let silent = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            request_preparation(&config, &command(), Duration::from_millis(25)),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().to_string().contains("deadline"));
        silent.abort();
        let unauthenticated = ProtocolOwnerPeerResolverConfig::new(Default::default());
        assert!(
            request_preparation(&unauthenticated, &command(), Duration::from_secs(1))
                .await
                .unwrap_err()
                .to_string()
                .contains("credentials")
        );
    }
}
