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
    use fibril_storage::Partition;

    use super::*;
    use crate::v1::{
        Hello, QueueTopologyEntry, ReconcileAction, ReconcileClient, ReconcilePolicy,
        ReconcileResult, ReconcileSubscription, ReconcileSubscriptionResult, Redirect,
        ReplicationApply, ReplicationApplyOk, ReplicationCheckpointExport,
        ReplicationCheckpointExportOk, ReplicationCheckpointInstall,
        ReplicationCheckpointInstallOk, ReplicationCheckpointRequired, ReplicationEventApplyBatch,
        ReplicationEventRead, ReplicationEventRecord, ReplicationMessageApplyBatch,
        ReplicationMessageRead, ReplicationMessageRecord, ReplicationRead, ReplicationReadOk,
        ReplicationStateCheckpoint, TopologyOk,
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
