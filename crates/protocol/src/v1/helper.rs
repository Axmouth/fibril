use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::v1::{
    frame::{Frame, ProtoCodec},
    wire::{self, WireError},
    *,
};

pub type Conn = Framed<TcpStream, ProtoCodec>;

#[derive(Debug, Clone, Error)]
pub enum ProtocolError {
    #[error("failed to encode protocol frame: {0}")]
    Encode(String),
    #[error("failed to decode protocol frame: {0}")]
    Decode(String),
}

pub type ProtocolResult<T> = Result<T, ProtocolError>;

pub fn try_encode<T: Serialize + Any>(op: Op, req_id: u64, msg: &T) -> ProtocolResult<Frame> {
    match op {
        Op::Hello => encode_typed(msg, "Hello", |msg| wire::encode_hello(req_id, msg)),
        Op::HelloOk => encode_typed(msg, "HelloOk", |msg| wire::encode_hello_ok(req_id, msg)),
        Op::HelloErr => encode_error_like(op, req_id, msg),
        Op::Auth => encode_typed(msg, "Auth", |msg| wire::encode_auth(req_id, msg)),
        Op::AuthOk => encode_unit_like(op, req_id, msg),
        Op::AuthErr => encode_error_like(op, req_id, msg),
        Op::Publish => encode_typed(msg, "Publish", |msg| wire::encode_publish(req_id, msg)),
        Op::PublishDelayed => encode_typed(msg, "PublishDelayed", |msg| {
            wire::encode_publish_delayed(req_id, msg)
        }),
        Op::PublishOk => encode_typed(msg, "PublishOk", |msg| wire::encode_publish_ok(req_id, msg)),
        Op::Subscribe => encode_typed(msg, "Subscribe", |msg| wire::encode_subscribe(req_id, msg)),
        Op::SubscribeOk => encode_typed(msg, "SubscribeOk", |msg| {
            wire::encode_subscribe_ok(req_id, msg)
        }),
        Op::SubscribeErr => encode_error_like(op, req_id, msg),
        Op::Deliver => encode_typed(msg, "Deliver", |msg| wire::encode_deliver(req_id, msg)),
        Op::Ack => encode_typed(msg, "Ack", |msg| wire::encode_ack(req_id, msg)),
        Op::Nack => encode_typed(msg, "Nack", |msg| wire::encode_nack(req_id, msg)),
        Op::AssignmentChanged => encode_typed(msg, "AssignmentChanged", |msg| {
            wire::encode_assignment_changed(req_id, msg)
        }),
        Op::Ping | Op::Pong => encode_unit_like(op, req_id, msg),
        Op::DeclareQueue => encode_typed(msg, "DeclareQueue", |msg| {
            wire::encode_declare_queue(req_id, msg)
        }),
        Op::DeclareQueueOk => encode_typed(msg, "DeclareQueueOk", |msg| {
            wire::encode_declare_queue_ok(req_id, msg)
        }),
        Op::DeclarePlexus => encode_typed(msg, "DeclarePlexus", |msg| {
            wire::encode_declare_plexus(req_id, msg)
        }),
        Op::DeclarePlexusOk => encode_typed(msg, "DeclarePlexusOk", |msg| {
            wire::encode_declare_plexus_ok(req_id, msg)
        }),
        Op::SubscribeStream => encode_typed(msg, "SubscribeStream", |msg| {
            wire::encode_subscribe_stream(req_id, msg)
        }),
        Op::ReconcileClient => encode_typed(msg, "ReconcileClient", |msg| {
            wire::encode_reconcile_client(req_id, msg)
        }),
        Op::ReconcileServer => encode_typed(msg, "ReconcileServer", |msg| {
            wire::encode_reconcile_server(req_id, msg)
        }),
        Op::ReconcileResult => encode_typed(msg, "ReconcileResult", |msg| {
            wire::encode_reconcile_result(req_id, msg)
        }),
        Op::ReplicationRead => encode_typed(msg, "ReplicationRead", |msg| {
            wire::encode_replication_read(req_id, msg)
        }),
        Op::ReplicationReadOk => encode_typed(msg, "ReplicationReadOk", |msg| {
            wire::encode_replication_read_ok(req_id, msg)
        }),
        Op::StreamReplicationRead => encode_typed(msg, "ReplicationRead", |msg| {
            wire::encode_stream_replication_read(req_id, msg)
        }),
        Op::StreamReplicationReadOk => encode_typed(msg, "ReplicationReadOk", |msg| {
            wire::encode_stream_replication_read_ok(req_id, msg)
        }),
        Op::ReplicationApply => encode_typed(msg, "ReplicationApply", |msg| {
            wire::encode_replication_apply(req_id, msg)
        }),
        Op::ReplicationApplyOk => encode_typed(msg, "ReplicationApplyOk", |msg| {
            wire::encode_replication_apply_ok(req_id, msg)
        }),
        Op::ReplicationCheckpointExport => {
            encode_typed(msg, "ReplicationCheckpointExport", |msg| {
                wire::encode_replication_checkpoint_export(req_id, msg)
            })
        }
        Op::ReplicationCheckpointExportOk => {
            encode_typed(msg, "ReplicationCheckpointExportOk", |msg| {
                wire::encode_replication_checkpoint_export_ok(req_id, msg)
            })
        }
        Op::ReplicationCheckpointInstall => {
            encode_typed(msg, "ReplicationCheckpointInstall", |msg| {
                wire::encode_replication_checkpoint_install(req_id, msg)
            })
        }
        Op::ReplicationCheckpointInstallOk => {
            encode_typed(msg, "ReplicationCheckpointInstallOk", |msg| {
                wire::encode_replication_checkpoint_install_ok(req_id, msg)
            })
        }
        Op::Topology => encode_typed(msg, "TopologyRequest", |msg| {
            wire::encode_topology_request(req_id, msg)
        }),
        Op::TopologyOk => encode_typed(msg, "TopologyOk", |msg| {
            wire::encode_topology_ok(req_id, msg)
        }),
        Op::Redirect => encode_typed(msg, "Redirect", |msg| wire::encode_redirect(req_id, msg)),
        Op::ReplicationStreamStart => encode_typed(msg, "ReplicationStreamStart", |msg| {
            wire::encode_replication_stream_start(req_id, msg)
        }),
        Op::ReplicationStreamBatch => encode_typed(msg, "ReplicationStreamBatch", |msg| {
            wire::encode_replication_stream_batch(req_id, msg)
        }),
        Op::ReplicationStreamProgress => encode_typed(msg, "ReplicationStreamProgress", |msg| {
            wire::encode_replication_stream_progress(req_id, msg)
        }),
        Op::ReplicationStreamReset => encode_typed(msg, "ReplicationStreamReset", |msg| {
            wire::encode_replication_stream_reset(req_id, msg)
        }),
        Op::ReplicationStreamStop => encode_unit_like(op, req_id, msg),
        Op::ReplicationStreamEnd => encode_typed(msg, "ReplicationStreamEnd", |msg| {
            wire::encode_replication_stream_end(req_id, msg)
        }),
        Op::Error => encode_error_like(op, req_id, msg),
    }
}

pub fn try_decode<T: for<'de> Deserialize<'de> + Any>(frame: &Frame) -> ProtocolResult<T> {
    match frame.opcode {
        x if x == Op::Publish as u16 => {
            return wire::decode_publish(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded);
        }
        x if x == Op::PublishDelayed as u16 => {
            return wire::decode_publish_delayed(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded);
        }
        x if x == Op::PublishOk as u16 => {
            return wire::decode_publish_ok(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded);
        }
        x if x == Op::Deliver as u16 => {
            return wire::decode_deliver(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded);
        }
        x if x == Op::Ack as u16 => {
            return wire::decode_ack(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded);
        }
        x if x == Op::Nack as u16 => {
            return wire::decode_nack(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded);
        }
        _ => {}
    }

    match frame.opcode {
        x if x == Op::Hello as u16 => wire::decode_hello(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::HelloOk as u16 => wire::decode_hello_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::HelloErr as u16 => decode_error_like(frame),
        x if x == Op::Auth as u16 => wire::decode_auth(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::AuthOk as u16 => decode_unit_like(frame, Op::AuthOk),
        x if x == Op::AuthErr as u16 => decode_error_like(frame),
        x if x == Op::Subscribe as u16 => wire::decode_subscribe(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::SubscribeOk as u16 => wire::decode_subscribe_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::SubscribeErr as u16 => decode_error_like(frame),
        x if x == Op::AssignmentChanged as u16 => wire::decode_assignment_changed(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::Ping as u16 => decode_unit_like(frame, Op::Ping),
        x if x == Op::Pong as u16 => decode_unit_like(frame, Op::Pong),
        x if x == Op::DeclareQueue as u16 => wire::decode_declare_queue(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::DeclareQueueOk as u16 => wire::decode_declare_queue_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::DeclarePlexus as u16 => wire::decode_declare_plexus(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::DeclarePlexusOk as u16 => wire::decode_declare_plexus_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::SubscribeStream as u16 => wire::decode_subscribe_stream(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReconcileClient as u16 => wire::decode_reconcile_client(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReconcileServer as u16 => wire::decode_reconcile_server(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReconcileResult as u16 => wire::decode_reconcile_result(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReplicationRead as u16 => wire::decode_replication_read(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReplicationReadOk as u16 => wire::decode_replication_read_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReplicationApply as u16 => wire::decode_replication_apply(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReplicationApplyOk as u16 => wire::decode_replication_apply_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::ReplicationCheckpointExport as u16 => {
            wire::decode_replication_checkpoint_export(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::ReplicationCheckpointExportOk as u16 => {
            wire::decode_replication_checkpoint_export_ok(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::ReplicationCheckpointInstall as u16 => {
            wire::decode_replication_checkpoint_install(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::ReplicationCheckpointInstallOk as u16 => {
            wire::decode_replication_checkpoint_install_ok(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::Topology as u16 => wire::decode_topology_request(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::TopologyOk as u16 => wire::decode_topology_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::Redirect as u16 => wire::decode_redirect(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::Error as u16 => decode_error_like(frame),
        opcode => Err(ProtocolError::Decode(format!("unknown opcode {opcode}"))),
    }
}

fn encode_typed<T, U, F>(msg: &T, expected: &'static str, encode: F) -> ProtocolResult<Frame>
where
    T: Any,
    U: Any,
    F: FnOnce(&U) -> wire::WireResult<Frame>,
{
    let Some(msg) = (msg as &dyn Any).downcast_ref::<U>() else {
        return Err(ProtocolError::Encode(format!(
            "{expected} opcode requires {expected} payload"
        )));
    };
    encode(msg).map_err(wire_encode_error)
}

fn encode_error_like<T: Any>(op: Op, req_id: u64, msg: &T) -> ProtocolResult<Frame> {
    encode_typed(msg, "ErrorMsg", |msg| {
        wire::encode_error_message(op, req_id, msg)
    })
}

fn encode_unit_like<T: Any>(op: Op, req_id: u64, msg: &T) -> ProtocolResult<Frame> {
    let any = msg as &dyn Any;
    let allowed = match op {
        Op::AuthOk => any.is::<()>(),
        Op::Ping => any.is::<()>() || any.is::<Ping>(),
        Op::Pong => any.is::<()>() || any.is::<Pong>(),
        _ => false,
    };
    if !allowed {
        return Err(ProtocolError::Encode(format!(
            "{op:?} opcode requires an empty payload"
        )));
    }
    wire::encode_unit(op, req_id).map_err(wire_encode_error)
}

fn decode_error_like<T: Any>(frame: &Frame) -> ProtocolResult<T> {
    wire::decode_error_message(frame)
        .map_err(wire_decode_error)
        .and_then(cast_decoded)
}

fn decode_unit_like<T: Any>(frame: &Frame, op: Op) -> ProtocolResult<T> {
    wire::decode_unit(frame, op).map_err(wire_decode_error)?;
    let requested = TypeId::of::<T>();
    if requested == TypeId::of::<()>() {
        return cast_decoded(());
    }

    match op {
        Op::Ping if requested == TypeId::of::<Ping>() => cast_decoded(Ping),
        Op::Pong if requested == TypeId::of::<Pong>() => cast_decoded(Pong),
        _ => Err(ProtocolError::Decode(
            "decoded empty wire payload did not match requested type".into(),
        )),
    }
}

fn cast_decoded<T: Any, U: Any>(value: U) -> ProtocolResult<T> {
    let boxed: Box<dyn Any> = Box::new(value);
    boxed.downcast::<T>().map(|boxed| *boxed).map_err(|_| {
        ProtocolError::Decode("decoded wire payload did not match requested type".into())
    })
}

fn wire_encode_error(err: WireError) -> ProtocolError {
    ProtocolError::Encode(err.to_string())
}

fn wire_decode_error(err: WireError) -> ProtocolError {
    ProtocolError::Decode(err.to_string())
}

pub fn error_frame(req_id: u64, code: u16, message: impl Into<String>) -> ProtocolResult<Frame> {
    try_encode(
        Op::Error,
        req_id,
        &ErrorMsg {
            code,
            message: message.into(),
        },
    )
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use fibril_storage::Partition;
    use uuid::Uuid;

    use super::*;
    use crate::v1::{
        AssignmentChanged, Auth, DeclareQueue, DeclareQueueOk, Hello, HelloOk, QueueDlqPolicy,
        QueueTopologyEntry, ReconcileAction, ReconcileClient, ReconcilePolicy, ReconcileResult,
        ReconcileServer, ReconcileSubscription, ReconcileSubscriptionResult, Redirect,
        ReplicationApply, ReplicationApplyOk, ReplicationCheckpointExport,
        ReplicationCheckpointExportOk, ReplicationCheckpointInstall,
        ReplicationCheckpointInstallOk, ReplicationCheckpointRequired, ReplicationEventApplyBatch,
        ReplicationEventRead, ReplicationEventRecord, ReplicationMessageApplyBatch,
        ReplicationMessageRead, ReplicationMessageRecord, ReplicationRead, ReplicationReadOk,
        ReplicationStateCheckpoint, ResumeIdentity, ResumeOutcome, Subscribe, SubscribeOk,
        TopologyOk,
    };

    #[test]
    fn try_decode_returns_error_for_malformed_payload() {
        let frame = Frame {
            version: PROTOCOL_V1,
            opcode: Op::Hello as u16,
            flags: 0,
            request_id: 1,
            payload: Bytes::from_static(b"not msgpack"),
        };

        assert!(matches!(
            try_decode::<Hello>(&frame),
            Err(ProtocolError::Decode(_))
        ));
    }

    #[test]
    fn unit_frames_have_empty_payloads() {
        let ping = try_encode(Op::Ping, 1, &()).unwrap();
        assert_eq!(ping.opcode, Op::Ping as u16);
        assert!(ping.payload.is_empty());
        try_decode::<()>(&ping).unwrap();

        let pong = try_encode(Op::Pong, 2, &Pong).unwrap();
        assert_eq!(pong.opcode, Op::Pong as u16);
        assert!(pong.payload.is_empty());
        try_decode::<Pong>(&pong).unwrap();

        let auth_ok = try_encode(Op::AuthOk, 3, &()).unwrap();
        assert_eq!(auth_ok.opcode, Op::AuthOk as u16);
        assert!(auth_ok.payload.is_empty());
        try_decode::<()>(&auth_ok).unwrap();
    }

    #[test]
    fn truncated_control_payload_returns_decode_error() {
        let frame = Frame {
            version: PROTOCOL_V1,
            opcode: Op::Subscribe as u16,
            flags: 0,
            request_id: 1,
            payload: Bytes::from_static(b"FSB1"),
        };

        assert!(matches!(
            try_decode::<Subscribe>(&frame),
            Err(ProtocolError::Decode(_))
        ));
    }

    #[test]
    fn handshake_auth_and_error_payloads_roundtrip() {
        let resume = ResumeIdentity {
            owner_id: Uuid::new_v4(),
            client_id: Uuid::new_v4(),
            resume_token: Uuid::new_v4(),
        };
        let hello = Hello {
            client_name: "rust-client".into(),
            client_version: "0.1.0".into(),
            protocol_version: PROTOCOL_V1,
            resume: Some(resume.clone()),
        };
        let frame = try_encode(Op::Hello, 1, &hello).unwrap();
        let got = try_decode::<Hello>(&frame).unwrap();
        assert_eq!(got.client_name, hello.client_name);
        assert_eq!(got.client_version, hello.client_version);
        assert_eq!(got.protocol_version, hello.protocol_version);
        assert_eq!(got.resume.unwrap().client_id, resume.client_id);

        let hello_ok = HelloOk {
            protocol_version: PROTOCOL_V1,
            owner_id: resume.owner_id,
            client_id: resume.client_id,
            resume_token: resume.resume_token,
            resume_outcome: ResumeOutcome::Resumed,
            server_name: "fibril-test".into(),
            compliance: crate::v1::COMPLIANCE_STRING.into(),
        };
        let frame = try_encode(Op::HelloOk, 2, &hello_ok).unwrap();
        let got = try_decode::<HelloOk>(&frame).unwrap();
        assert_eq!(got.owner_id, hello_ok.owner_id);
        assert_eq!(got.resume_outcome, ResumeOutcome::Resumed);
        assert_eq!(got.compliance, hello_ok.compliance);

        let auth = Auth {
            username: "admin".into(),
            password: "secret".into(),
        };
        let frame = try_encode(Op::Auth, 3, &auth).unwrap();
        let got = try_decode::<Auth>(&frame).unwrap();
        assert_eq!(got.username, auth.username);
        assert_eq!(got.password, auth.password);

        for op in [Op::HelloErr, Op::AuthErr, Op::SubscribeErr, Op::Error] {
            let error = ErrorMsg {
                code: 400,
                message: format!("{op:?} failed"),
            };
            let frame = try_encode(op, 4, &error).unwrap();
            assert_eq!(
                try_decode::<ErrorMsg>(&frame).unwrap().message,
                error.message
            );
        }
    }

    #[test]
    fn queue_control_payloads_roundtrip() {
        let declare = DeclareQueue {
            topic: "orders".into(),
            group: Some("workers".into()),
            dlq_policy: Some(QueueDlqPolicy::Custom {
                topic: "orders-dlq".into(),
                group: Some("workers".into()),
            }),
            dlq_max_retries: Some(7),
            partition_count: Some(3),
            default_message_ttl_ms: None,
        };
        let frame = try_encode(Op::DeclareQueue, 5, &declare).unwrap();
        let got = try_decode::<DeclareQueue>(&frame).unwrap();
        assert_eq!(got.topic, declare.topic);
        assert_eq!(got.group, declare.group);
        assert_eq!(got.dlq_max_retries, declare.dlq_max_retries);
        assert_eq!(got.partition_count, declare.partition_count);

        let ok = DeclareQueueOk {
            status: "created".into(),
            partition_count: 3,
        };
        let frame = try_encode(Op::DeclareQueueOk, 6, &ok).unwrap();
        let got = try_decode::<DeclareQueueOk>(&frame).unwrap();
        assert_eq!(got.status, ok.status);
        assert_eq!(got.partition_count, ok.partition_count);

        let member_id = Uuid::new_v4();
        let subscribe = Subscribe {
            topic: "orders".into(),
            partition: Partition::new(2),
            group: Some("workers".into()),
            prefetch: 128,
            auto_ack: false,
            consumer_group: Some("batchers".into()),
            consumer_target: Some(2),
            member_id: Some(member_id),
        };
        let frame = try_encode(Op::Subscribe, 7, &subscribe).unwrap();
        let got = try_decode::<Subscribe>(&frame).unwrap();
        assert_eq!(got.topic, subscribe.topic);
        assert_eq!(got.partition, subscribe.partition);
        assert_eq!(got.consumer_group, subscribe.consumer_group);
        assert_eq!(got.member_id, subscribe.member_id);

        let subscribe_ok = SubscribeOk {
            sub_id: 44,
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(2),
            prefetch: 128,
            consumer_group: Some("batchers".into()),
            consumer_target: Some(2),
            member_id: Some(member_id),
        };
        let frame = try_encode(Op::SubscribeOk, 8, &subscribe_ok).unwrap();
        let got = try_decode::<SubscribeOk>(&frame).unwrap();
        assert_eq!(got.sub_id, subscribe_ok.sub_id);
        assert_eq!(got.partition, subscribe_ok.partition);
        assert_eq!(got.member_id, subscribe_ok.member_id);

        let assignment = AssignmentChanged {
            topic: "orders".into(),
            group: Some("workers".into()),
            consumer_group: "batchers".into(),
            generation: 9,
            assigned: vec![Partition::new(0), Partition::new(2)],
            added: vec![Partition::new(2)],
            revoked: vec![Partition::new(1)],
        };
        let frame = try_encode(Op::AssignmentChanged, 9, &assignment).unwrap();
        assert_eq!(try_decode::<AssignmentChanged>(&frame).unwrap(), assignment);

        let server = ReconcileServer {
            subscriptions: vec![ReconcileSubscription {
                sub_id: 44,
                topic: "orders".into(),
                group: Some("workers".into()),
                partition: Partition::new(2),
                auto_ack: false,
                prefetch: 128,
                consumer_group: Some("batchers".into()),
                consumer_target: Some(2),
                member_id: Some(member_id),
            }],
        };
        let frame = try_encode(Op::ReconcileServer, 10, &server).unwrap();
        assert_eq!(try_decode::<ReconcileServer>(&frame).unwrap(), server);
    }

    #[test]
    fn error_frame_builds_protocol_error_message() {
        let frame = error_frame(7, 400, "bad request").unwrap();
        assert_eq!(frame.opcode, Op::Error as u16);
        assert_eq!(frame.request_id, 7);

        let error: ErrorMsg = try_decode(&frame).unwrap();
        assert_eq!(error.code, 400);
        assert_eq!(error.message, "bad request");
    }

    #[test]
    fn topology_ok_roundtrips() {
        let msg = TopologyOk {
            generation: 7,
            queues: vec![
                QueueTopologyEntry {
                    topic: "orders".into(),
                    partition: Partition::new(0),
                    group: Some("workers".into()),
                    owner_endpoint: Some("127.0.0.1:9000".into()),
                    partitioning_version: 0,
                    partition_count: 1,
                },
                QueueTopologyEntry {
                    topic: "emails".into(),
                    partition: Partition::new(1),
                    group: None,
                    owner_endpoint: None,
                    partitioning_version: 0,
                    partition_count: 1,
                },
            ],
            streams: vec![StreamTopologyEntry {
                topic: "events".into(),
                partition: Partition::new(0),
                owner_endpoint: Some("10.0.0.7:7000".into()),
                partitioning_version: 2,
                partition_count: 4,
            }],
        };

        let frame = try_encode(Op::TopologyOk, 5, &msg).unwrap();
        assert_eq!(frame.opcode, Op::TopologyOk as u16);
        assert_eq!(try_decode::<TopologyOk>(&frame).unwrap(), msg);
    }

    #[test]
    fn redirect_roundtrips() {
        let msg = Redirect {
            topic: "orders".into(),
            partition: Partition::new(2),
            group: Some("workers".into()),
            owner_endpoint: "127.0.0.1:9001".into(),
            partitioning_version: 0,
        };

        let frame = try_encode(Op::Redirect, 8, &msg).unwrap();
        assert_eq!(frame.opcode, Op::Redirect as u16);
        assert_eq!(try_decode::<Redirect>(&frame).unwrap(), msg);
    }

    #[test]
    fn reconcile_client_roundtrips() {
        let msg = ReconcileClient {
            policy: ReconcilePolicy::Conservative,
            subscriptions: vec![ReconcileSubscription {
                sub_id: 9,
                topic: "jobs".into(),
                group: Some("workers".into()),
                partition: Partition::new(0),
                auto_ack: false,
                prefetch: 32,
                consumer_group: None,
                consumer_target: None,
                member_id: None,
            }],
        };

        let frame = try_encode(Op::ReconcileClient, 11, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReconcileClient as u16);
        assert_eq!(try_decode::<ReconcileClient>(&frame).unwrap(), msg);
    }

    #[test]
    fn reconcile_result_roundtrips() {
        let client = ReconcileSubscription {
            sub_id: 9,
            topic: "jobs".into(),
            group: None,
            partition: Partition::new(0),
            auto_ack: false,
            prefetch: 1,
            consumer_group: None,
            consumer_target: None,
            member_id: None,
        };
        let msg = ReconcileResult {
            subscriptions: vec![ReconcileSubscriptionResult {
                client: Some(client),
                server: None,
                action: ReconcileAction::CloseClientSide,
                reason: "server_missing".into(),
            }],
        };

        let frame = try_encode(Op::ReconcileResult, 12, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReconcileResult as u16);
        assert_eq!(try_decode::<ReconcileResult>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_read_roundtrips() {
        let msg = ReplicationRead {
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(3),
            message_from: 11,
            event_from: 17,
            max_messages: 128,
            max_events: 256,
            max_bytes: 1024 * 1024,
            max_wait_ms: 250,
            reporter_node_id: None,
        };

        let frame = try_encode(Op::ReplicationRead, 13, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationRead as u16);
        assert_eq!(try_decode::<ReplicationRead>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_read_ok_roundtrips_batches() {
        let msg = ReplicationReadOk {
            messages: ReplicationMessageRead::Batch {
                epoch: 4,
                requested_offset: 10,
                next_offset: 12,
                records: vec![
                    ReplicationMessageRecord {
                        offset: 10,
                        flags: 0,
                        headers: b"headers-a".to_vec(),
                        payload: b"payload-a".to_vec(),
                    },
                    ReplicationMessageRecord {
                        offset: 11,
                        flags: 0,
                        headers: b"headers-b".to_vec(),
                        payload: b"payload-b".to_vec(),
                    },
                ],
            },
            events: ReplicationEventRead::Batch {
                epoch: 4,
                requested_offset: 20,
                next_offset: 21,
                records: vec![ReplicationEventRecord {
                    offset: 20,
                    payload: b"event".to_vec(),
                }],
            },
        };

        let frame = try_encode(Op::ReplicationReadOk, 14, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationReadOk as u16);
        assert_eq!(try_decode::<ReplicationReadOk>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_read_ok_roundtrips_checkpoint_required() {
        let checkpoint = ReplicationCheckpointRequired {
            epoch: 7,
            requested_offset: 1,
            head_offset: 10,
            next_offset: 20,
        };
        let msg = ReplicationReadOk {
            messages: ReplicationMessageRead::CheckpointRequired(checkpoint.clone()),
            events: ReplicationEventRead::CheckpointRequired(checkpoint),
        };

        let frame = try_encode(Op::ReplicationReadOk, 15, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationReadOk as u16);
        assert_eq!(try_decode::<ReplicationReadOk>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_apply_roundtrips() {
        let msg = ReplicationApply {
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(3),
            messages: Some(ReplicationMessageApplyBatch {
                epoch: 4,
                records: vec![ReplicationMessageRecord {
                    offset: 10,
                    flags: 0,
                    headers: b"headers".to_vec(),
                    payload: b"payload".to_vec(),
                }],
            }),
            events: Some(ReplicationEventApplyBatch {
                epoch: 4,
                records: vec![ReplicationEventRecord {
                    offset: 20,
                    payload: b"event".to_vec(),
                }],
            }),
        };

        let frame = try_encode(Op::ReplicationApply, 16, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationApply as u16);
        assert_eq!(try_decode::<ReplicationApply>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_apply_ok_roundtrips_applied() {
        let msg = ReplicationApplyOk {
            messages_applied: true,
            events_applied: true,
        };

        let frame = try_encode(Op::ReplicationApplyOk, 18, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationApplyOk as u16);
        assert_eq!(try_decode::<ReplicationApplyOk>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_checkpoint_export_roundtrips() {
        let msg = ReplicationCheckpointExport {
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(3),
        };

        let frame = try_encode(Op::ReplicationCheckpointExport, 19, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationCheckpointExport as u16);
        assert_eq!(
            try_decode::<ReplicationCheckpointExport>(&frame).unwrap(),
            msg
        );
    }

    #[test]
    fn replication_checkpoint_export_ok_roundtrips() {
        let msg = ReplicationCheckpointExportOk {
            checkpoint: ReplicationStateCheckpoint {
                message_epoch: 2,
                event_epoch: 3,
                message_checkpoint_offset: 10,
                message_next_offset: 20,
                event_next_offset: 30,
                applied_event_offset: 29,
                state_snapshot: b"snapshot".to_vec(),
            },
        };

        let frame = try_encode(Op::ReplicationCheckpointExportOk, 20, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationCheckpointExportOk as u16);
        assert_eq!(
            try_decode::<ReplicationCheckpointExportOk>(&frame).unwrap(),
            msg
        );
    }

    #[test]
    fn replication_checkpoint_install_roundtrips() {
        let msg = ReplicationCheckpointInstall {
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(3),
            checkpoint: ReplicationStateCheckpoint {
                message_epoch: 2,
                event_epoch: 3,
                message_checkpoint_offset: 10,
                message_next_offset: 20,
                event_next_offset: 30,
                applied_event_offset: 29,
                state_snapshot: b"snapshot".to_vec(),
            },
        };

        let frame = try_encode(Op::ReplicationCheckpointInstall, 21, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationCheckpointInstall as u16);
        assert_eq!(
            try_decode::<ReplicationCheckpointInstall>(&frame).unwrap(),
            msg
        );
    }

    #[test]
    fn replication_checkpoint_install_ok_roundtrips() {
        let msg = ReplicationCheckpointInstallOk {
            message_next_offset: 10,
            event_next_offset: 30,
            applied_event_offset: 29,
        };

        let frame = try_encode(Op::ReplicationCheckpointInstallOk, 22, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationCheckpointInstallOk as u16);
        assert_eq!(
            try_decode::<ReplicationCheckpointInstallOk>(&frame).unwrap(),
            msg
        );
    }
}
