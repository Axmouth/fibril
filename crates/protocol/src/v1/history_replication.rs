use super::{HistoryReplication, ReplicationHistoryContext, frame::Frame, wire};
use fibril_broker::{
    Partition,
    broker::{Broker, BrokerError},
    history_replication::{HistoryReplicationSession, ReplicaHistoryInstance},
    queue_engine::{StorageHistoryBinding, StromaEngine},
};

pub fn encode_frame(
    session: &HistoryReplicationSession,
    frame: Frame,
) -> Result<Frame, wire::WireError> {
    wire::encode_history_replication(
        frame.request_id,
        &HistoryReplication {
            history: ReplicationHistoryContext {
                topic: session.topic.clone(),
                partition: session.partition,
                group: session.group.clone(),
                stream: session.stream,
                activation: session.activation,
                resource_incarnation: session.binding.resource_incarnation,
                accepted_history: session.binding.accepted_history,
                writer_session: session.binding.writer_session,
                sender: session.sender.clone(),
                sender_process: session.sender_instance.process,
                sender_storage: session.sender_instance.storage,
                receiver: session.receiver.clone(),
                receiver_process: session.receiver_instance.process,
                receiver_storage: session.receiver_instance.storage,
            },
            opcode: frame.opcode,
            flags: frame.flags,
            body: frame.payload.to_vec(),
        },
    )
}

pub(super) fn decode_frame(
    frame: Frame,
) -> Result<(HistoryReplicationSession, Frame), wire::WireError> {
    let envelope = wire::decode_history_replication(&frame)?;
    let h = envelope.history;
    Ok((
        HistoryReplicationSession {
            topic: h.topic,
            partition: h.partition,
            group: h.group,
            stream: h.stream,
            activation: h.activation,
            binding: StorageHistoryBinding {
                resource_incarnation: h.resource_incarnation,
                accepted_history: h.accepted_history,
                writer_session: h.writer_session,
            },
            sender: h.sender,
            receiver: h.receiver,
            sender_instance: ReplicaHistoryInstance {
                process: h.sender_process,
                storage: h.sender_storage,
            },
            receiver_instance: ReplicaHistoryInstance {
                process: h.receiver_process,
                storage: h.receiver_storage,
            },
        },
        Frame {
            version: frame.version,
            request_id: frame.request_id,
            opcode: envelope.opcode,
            flags: envelope.flags,
            payload: envelope.body.into(),
        },
    ))
}

pub(super) fn check_resource(
    broker: &Broker<StromaEngine>,
    session: Option<&HistoryReplicationSession>,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
    owner_is_receiver: bool,
) -> Result<(), BrokerError> {
    if let Some(session) = session {
        if session.topic != topic
            || session.partition != partition
            || session.group.as_deref() != group
        {
            return Err(BrokerError::InvalidArgument(
                "replication envelope and operation name different resources".into(),
            ));
        }
        broker.authorize_history_replication(session, owner_is_receiver)
    } else {
        broker.authorize_legacy_replication(topic, partition, group)
    }
}

pub(super) fn check_reporter(
    session: Option<&HistoryReplicationSession>,
    reporter: Option<&str>,
) -> Result<(), BrokerError> {
    if let Some(session) = session {
        if reporter.is_some_and(|reporter| reporter != session.sender) {
            return Err(BrokerError::InvalidArgument(
                "replication progress reporter differs from authenticated history sender".into(),
            ));
        }
    }
    Ok(())
}
