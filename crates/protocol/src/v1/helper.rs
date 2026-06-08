use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

use crate::v1::{
    ErrorMsg, Op, PROTOCOL_V1,
    frame::{Frame, ProtoCodec},
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

pub fn try_encode<T: Serialize>(op: Op, req_id: u64, msg: &T) -> ProtocolResult<Frame> {
    let payload = rmp_serde::to_vec_named(msg)
        .map_err(|err| ProtocolError::Encode(err.to_string()))?
        .into();

    Ok(Frame {
        version: PROTOCOL_V1,
        opcode: op as u16,
        flags: 0,
        request_id: req_id,
        payload,
    })
}

pub fn try_decode<T: for<'de> Deserialize<'de>>(frame: &Frame) -> ProtocolResult<T> {
    rmp_serde::from_slice(&frame.payload).map_err(|err| ProtocolError::Decode(err.to_string()))
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

    use super::*;
    use crate::v1::{
        Hello, ReconcileAction, ReconcileClient, ReconcilePolicy, ReconcileResult,
        ReconcileSubscription, ReconcileSubscriptionResult,
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
    fn error_frame_builds_protocol_error_message() {
        let frame = error_frame(7, 400, "bad request").unwrap();
        assert_eq!(frame.opcode, Op::Error as u16);
        assert_eq!(frame.request_id, 7);

        let error: ErrorMsg = try_decode(&frame).unwrap();
        assert_eq!(error.code, 400);
        assert_eq!(error.message, "bad request");
    }

    #[test]
    fn reconcile_client_roundtrips() {
        let msg = ReconcileClient {
            policy: ReconcilePolicy::Conservative,
            subscriptions: vec![ReconcileSubscription {
                sub_id: 9,
                topic: "jobs".into(),
                group: Some("workers".into()),
                partition: 0,
                auto_ack: false,
                prefetch: 32,
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
            partition: 0,
            auto_ack: false,
            prefetch: 1,
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
}
