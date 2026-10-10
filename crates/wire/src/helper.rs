use fibril_util::net::TcpStream;
use serde::{Deserialize, Serialize};
use std::any::{Any, TypeId};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_util::codec::Framed;

use crate::{
    frame::{Frame, ProtoCodec},
    wire::{self, WireError},
    *,
};

pub type Conn = Framed<PeerStream, ProtoCodec>;

/// Outbound connection transport for peers and tools: plaintext or TLS over
/// the same net seam. One enum keeps [`Conn`] a single concrete type while
/// the choice stays runtime config.
pub enum PeerStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

/// Wrap a plaintext stream as a [`Conn`].
pub fn plain_conn(stream: TcpStream) -> Conn {
    Framed::new(PeerStream::Plain(stream), ProtoCodec)
}

/// Wrap a client-side TLS stream as a [`Conn`].
pub fn tls_conn(stream: tokio_rustls::client::TlsStream<TcpStream>) -> Conn {
    Framed::new(PeerStream::Tls(Box::new(stream)), ProtoCodec)
}

impl AsyncRead for PeerStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for PeerStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

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
        Op::QueueReplicationRead => encode_typed(msg, "QueueReplicationRead", |msg| {
            wire::encode_queue_replication_read(req_id, msg)
        }),
        Op::QueueReplicationReadOk => encode_typed(msg, "QueueReplicationReadOk", |msg| {
            wire::encode_queue_replication_read_ok(req_id, msg)
        }),
        Op::StreamReplicationRead => encode_typed(msg, "StreamReplicationRead", |msg| {
            wire::encode_stream_replication_read(req_id, msg)
        }),
        Op::StreamReplicationReadOk => encode_typed(msg, "StreamReplicationReadOk", |msg| {
            wire::encode_stream_replication_read_ok(req_id, msg)
        }),
        Op::QueueCheckpointExport => encode_typed(msg, "QueueCheckpointExport", |msg| {
            wire::encode_queue_checkpoint_export(req_id, msg)
        }),
        Op::QueueCheckpointExportOk => encode_typed(msg, "QueueCheckpointExportOk", |msg| {
            wire::encode_queue_checkpoint_export_ok(req_id, msg)
        }),
        Op::Topology => encode_typed(msg, "TopologyRequest", |msg| {
            wire::encode_topology_request(req_id, msg)
        }),
        Op::TopologyOk => encode_typed(msg, "TopologyOk", |msg| {
            wire::encode_topology_ok(req_id, msg)
        }),
        Op::TopologyUpdate => encode_typed(msg, "TopologyOk", |msg| {
            wire::encode_topology_update(req_id, msg)
        }),
        Op::TopologyUpdateAck => encode_typed(msg, "TopologyUpdateAck", |msg| {
            wire::encode_topology_update_ack(req_id, msg)
        }),
        Op::GoingAway => encode_typed(msg, "GoingAway", |msg| wire::encode_going_away(req_id, msg)),
        Op::SubscriptionClosed => encode_typed(msg, "SubscriptionClosed", |msg| {
            wire::encode_subscription_closed(req_id, msg)
        }),
        Op::Redirect => encode_typed(msg, "Redirect", |msg| wire::encode_redirect(req_id, msg)),
        Op::QueuePushStart => encode_typed(msg, "QueuePushStart", |msg| {
            wire::encode_queue_push_start(req_id, msg)
        }),
        Op::QueuePushBatch => encode_typed(msg, "QueueReplicationReadOk", |msg| {
            wire::encode_queue_push_batch(req_id, msg)
        }),
        Op::QueuePushProgress => encode_typed(msg, "QueuePushProgress", |msg| {
            wire::encode_queue_push_progress(req_id, msg)
        }),
        Op::QueuePushReset => encode_typed(msg, "QueuePushReset", |msg| {
            wire::encode_queue_push_reset(req_id, msg)
        }),
        Op::QueuePushStop => encode_unit_like(op, req_id, msg),
        Op::QueuePushEnd => encode_typed(msg, "QueuePushEnd", |msg| {
            wire::encode_queue_push_end(req_id, msg)
        }),
        Op::RecoverySeal => encode_typed(msg, "RecoverySeal", |msg| {
            wire::encode_recovery_seal(req_id, msg)
        }),
        Op::RecoverySealOk => encode_typed(msg, "RecoverySealOk", |msg| {
            wire::encode_recovery_seal_ok(req_id, msg)
        }),
        Op::RecoveryReadSequential => encode_typed(msg, "RecoveryReadSequential", |msg| {
            wire::encode_recovery_read_sequential(req_id, msg)
        }),
        Op::RecoveryRead => encode_typed(msg, "RecoveryRead", |msg| {
            wire::encode_recovery_read(req_id, msg)
        }),
        Op::RecoveryReadOk => encode_typed(msg, "RecoveryReadOk", |msg| {
            wire::encode_recovery_read_ok(req_id, msg)
        }),
        Op::InitialHistoryPrepare => encode_typed(msg, "InitialHistoryPrepare", |msg| {
            wire::encode_initial_history_prepare(req_id, msg)
        }),
        Op::InitialHistoryPrepareOk => encode_typed(msg, "InitialHistoryPrepareOk", |msg| {
            wire::encode_initial_history_prepare_ok(req_id, msg)
        }),
        Op::RecoveryTransfer => encode_typed(msg, "RecoveryTransfer", |msg| {
            wire::encode_recovery_transfer(req_id, msg, false)
        }),
        Op::RecoveryTransferOk => encode_typed(msg, "RecoveryTransfer", |msg| {
            wire::encode_recovery_transfer(req_id, msg, true)
        }),
        Op::HistoryReplication => encode_typed(msg, "HistoryReplication", |msg| {
            wire::encode_history_replication(req_id, msg)
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
        x if x == Op::QueueReplicationRead as u16 => wire::decode_queue_replication_read(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::QueueReplicationReadOk as u16 => {
            wire::decode_queue_replication_read_ok(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::StreamReplicationRead as u16 => wire::decode_stream_replication_read(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::StreamReplicationReadOk as u16 => {
            wire::decode_stream_replication_read_ok(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::QueueCheckpointExport as u16 => wire::decode_queue_checkpoint_export(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::QueueCheckpointExportOk as u16 => {
            wire::decode_queue_checkpoint_export_ok(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::Topology as u16 => wire::decode_topology_request(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::TopologyOk as u16 => wire::decode_topology_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::TopologyUpdate as u16 => wire::decode_topology_update(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::TopologyUpdateAck as u16 => wire::decode_topology_update_ack(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::GoingAway as u16 => wire::decode_going_away(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::SubscriptionClosed as u16 => wire::decode_subscription_closed(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::Redirect as u16 => wire::decode_redirect(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::RecoverySeal as u16 => wire::decode_recovery_seal(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::RecoverySealOk as u16 => wire::decode_recovery_seal_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::RecoveryReadSequential as u16 => wire::decode_recovery_read_sequential(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::RecoveryRead as u16 => wire::decode_recovery_read(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::RecoveryReadOk as u16 => wire::decode_recovery_read_ok(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::InitialHistoryPrepare as u16 => wire::decode_initial_history_prepare(frame)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::InitialHistoryPrepareOk as u16 => {
            wire::decode_initial_history_prepare_ok(frame)
                .map_err(wire_decode_error)
                .and_then(cast_decoded)
        }
        x if x == Op::RecoveryTransfer as u16 => wire::decode_recovery_transfer(frame, false)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::RecoveryTransferOk as u16 => wire::decode_recovery_transfer(frame, true)
            .map_err(wire_decode_error)
            .and_then(cast_decoded),
        x if x == Op::HistoryReplication as u16 => wire::decode_history_replication(frame)
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
    use crate::Partition;
    use bytes::Bytes;
    use uuid::Uuid;

    use super::*;
    use crate::{
        AssignmentChanged, Auth, DeclareQueue, DeclareQueueOk, Hello, HelloOk, QueueDlqPolicy,
        QueueTopologyEntry, ReconcileAction, ReconcileClient, ReconcilePolicy, ReconcileResult,
        ReconcileServer, ReconcileSubscription, ReconcileSubscriptionResult, Redirect,
        QueuePushProgress, QueueReplicationRead, QueueReplicationReadOk,
        QueueCheckpointExport, QueueCheckpointExportOk, ReplicationCheckpointRequired,
        ReplicationEventRead, ReplicationEventRecord, ReplicationMessageRead,
        ReplicationMessageRecord, StreamReplicationRead,
        StreamReplicationReadOk, ResumeIdentity, ResumeOutcome, Subscribe, SubscribeOk,
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
            compliance: crate::COMPLIANCE_STRING.into(),
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
                    owner_endpoints: vec![
                        AdvertisedAddress::parse("127.0.0.1:9000")
                            .expect("valid test owner endpoint"),
                    ],
                    partitioning_version: 0,
                    partition_count: 1,
                },
                QueueTopologyEntry {
                    topic: "emails".into(),
                    partition: Partition::new(1),
                    group: None,
                    owner_endpoints: vec![],
                    partitioning_version: 0,
                    partition_count: 1,
                },
            ],
            streams: vec![StreamTopologyEntry {
                topic: "events".into(),
                partition: Partition::new(0),
                owner_endpoints: vec![
                    AdvertisedAddress::parse("10.0.0.7:7000").expect("valid test owner endpoint"),
                ],
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
            owner_endpoints: vec![
                AdvertisedAddress::parse("127.0.0.1:9001").expect("valid test owner endpoint"),
            ],
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
                code: ReasonCode::ServerMissing,
                reason: "server_missing".into(),
            }],
        };

        let frame = try_encode(Op::ReconcileResult, 12, &msg).unwrap();
        assert_eq!(frame.opcode, Op::ReconcileResult as u16);
        assert_eq!(try_decode::<ReconcileResult>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_read_roundtrips() {
        let msg = QueueReplicationRead {
            reporter_epoch: Some(5),
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(3),
            from: 17,
            max_records: 256,
            max_bytes: 1024 * 1024,
            max_wait_ms: 250,
            reporter_node_id: None,
        };
        let frame = try_encode(Op::QueueReplicationRead, 13, &msg).unwrap();
        assert_eq!(frame.opcode, Op::QueueReplicationRead as u16);
        assert_eq!(try_decode::<QueueReplicationRead>(&frame).unwrap(), msg);

        let msg = StreamReplicationRead {
            reporter_epoch: None,
            topic: "events".into(),
            group: None,
            partition: Partition::new(3),
            message_from: 11,
            event_from: 17,
            max_messages: 128,
            max_events: 256,
            max_bytes: 1024 * 1024,
            max_wait_ms: 250,
            reporter_node_id: None,
        };
        let frame = try_encode(Op::StreamReplicationRead, 13, &msg).unwrap();
        assert_eq!(frame.opcode, Op::StreamReplicationRead as u16);
        assert_eq!(try_decode::<StreamReplicationRead>(&frame).unwrap(), msg);
        // The queue op refuses the stream body type.
        assert!(try_encode(Op::QueueReplicationRead, 13, &msg).is_err());
    }

    #[test]
    fn replication_read_ok_roundtrips_batches() {
        let queue: QueueReplicationReadOk = ReplicationEventRead::Batch {
            epoch: 4,
            requested_offset: 20,
            next_offset: 21,
            records: vec![ReplicationEventRecord {
                offset: 20,
                payload: b"event".to_vec(),
            }],
        };
        let frame = try_encode(Op::QueueReplicationReadOk, 14, &queue).unwrap();
        assert_eq!(frame.opcode, Op::QueueReplicationReadOk as u16);
        assert_eq!(try_decode::<QueueReplicationReadOk>(&frame).unwrap(), queue);

        let msg = StreamReplicationReadOk {
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
            events: queue,
        };
        let frame = try_encode(Op::StreamReplicationReadOk, 14, &msg).unwrap();
        assert_eq!(frame.opcode, Op::StreamReplicationReadOk as u16);
        assert_eq!(try_decode::<StreamReplicationReadOk>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_read_ok_roundtrips_checkpoint_required() {
        let checkpoint = ReplicationCheckpointRequired {
            epoch: 7,
            requested_offset: 1,
            head_offset: 10,
            next_offset: 20,
        };
        let queue: QueueReplicationReadOk =
            ReplicationEventRead::CheckpointRequired(checkpoint.clone());
        let frame = try_encode(Op::QueueReplicationReadOk, 15, &queue).unwrap();
        assert_eq!(try_decode::<QueueReplicationReadOk>(&frame).unwrap(), queue);

        let msg = StreamReplicationReadOk {
            messages: ReplicationMessageRead::CheckpointRequired(checkpoint.clone()),
            events: ReplicationEventRead::CheckpointRequired(checkpoint),
        };
        let frame = try_encode(Op::StreamReplicationReadOk, 15, &msg).unwrap();
        assert_eq!(frame.opcode, Op::StreamReplicationReadOk as u16);
        assert_eq!(try_decode::<StreamReplicationReadOk>(&frame).unwrap(), msg);
    }

    #[test]
    fn queue_push_progress_encodes_through_the_helper() {
        let msg = QueuePushProgress {
            durable_next: 9,
            credit_add_bytes: 100,
        };
        let frame = try_encode(Op::QueuePushProgress, 16, &msg).unwrap();
        assert_eq!(frame.opcode, Op::QueuePushProgress as u16);
        assert_eq!(crate::wire::decode_queue_push_progress(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_checkpoint_export_roundtrips() {
        let msg = QueueCheckpointExport {
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(3),
        };

        let frame = try_encode(Op::QueueCheckpointExport, 19, &msg).unwrap();
        assert_eq!(frame.opcode, Op::QueueCheckpointExport as u16);
        assert_eq!(try_decode::<QueueCheckpointExport>(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_checkpoint_export_ok_roundtrips() {
        let msg = QueueCheckpointExportOk {
            epoch: 3,
            start_offset: 10,
            next_offset: 30,
            state_snapshot: b"snapshot".to_vec(),
        };

        let frame = try_encode(Op::QueueCheckpointExportOk, 20, &msg).unwrap();
        assert_eq!(frame.opcode, Op::QueueCheckpointExportOk as u16);
        assert_eq!(&frame.payload[..4], b"FQC1");
        assert_eq!(try_decode::<QueueCheckpointExportOk>(&frame).unwrap(), msg);
        for length in 0..frame.payload.len() {
            let mut short = frame.clone();
            short.payload = short.payload.slice(..length);
            assert!(try_decode::<QueueCheckpointExportOk>(&short).is_err());
        }

        // A start past the applied boundary is refused.
        let bad = QueueCheckpointExportOk {
            start_offset: 31,
            ..msg
        };
        let frame = try_encode(Op::QueueCheckpointExportOk, 21, &bad).unwrap();
        assert!(try_decode::<QueueCheckpointExportOk>(&frame).is_err());
    }
}

#[cfg(test)]
mod recovery_wire_tests {
    use super::*;
    #[test]
    fn recovery_frames_roundtrip_and_reject_truncation_or_trailing_bytes() {
        let request = RecoverySeal {
            topic: "q".into(),
            group: Some("g".into()),
            partition: Partition::new(3),
            stream: false,
            transition: [19; 32],
            fence_epoch: 8,
        };
        let encoded = try_encode(Op::RecoverySeal, 42, &request).unwrap();
        assert_eq!(try_decode::<RecoverySeal>(&encoded).unwrap(), request);
        for len in 0..encoded.payload.len() {
            let mut broken = encoded.clone();
            broken.payload = broken.payload.slice(..len);
            assert!(try_decode::<RecoverySeal>(&broken).is_err());
        }
        let mut trailing = encoded.clone();
        let mut bytes = trailing.payload.to_vec();
        bytes.push(0);
        trailing.payload = bytes.into();
        assert!(try_decode::<RecoverySeal>(&trailing).is_err());
        for snapshot_digest in [None, Some([22; 32])] {
            let response = RecoverySealOk {
                combined_log: false,
                storage_history: None,
                replica_id: "b".into(),
                transition: [19; 32],
                fence_epoch: 8,
                history_version: 1,
                history_id: [2; 32],
                message_digest: [3; 32],
                event_digest: [4; 32],
                snapshot_digest,
                message_head: 0,
                message_next: 2,
                event_head: 0,
                event_next: 1,
            };
            let encoded = try_encode(Op::RecoverySealOk, 42, &response).unwrap();
            assert_eq!(try_decode::<RecoverySealOk>(&encoded).unwrap(), response);
            for len in 0..encoded.payload.len() {
                let mut broken = encoded.clone();
                broken.payload = broken.payload.slice(..len);
                assert!(try_decode::<RecoverySealOk>(&broken).is_err());
            }
        }
    }
}
