use bytes::{BufMut, BytesMut};

use crate::v1::{
    Op, PROTOCOL_V1, ReplicationCheckpointRequired, ReplicationEventRead, ReplicationEventRecord,
    ReplicationMessageRead, ReplicationMessageRecord, ReplicationReadOk,
    frame::Frame,
    helper::{ProtocolError, ProtocolResult},
};

const MAGIC: &[u8; 4] = b"FRR2";
const KIND_BATCH: u8 = 0;
const KIND_CHECKPOINT_REQUIRED: u8 = 1;

pub fn encode_replication_read_ok(req_id: u64, read: &ReplicationReadOk) -> ProtocolResult<Frame> {
    let mut payload = BytesMut::new();
    payload.extend_from_slice(MAGIC);
    encode_message_read(&mut payload, &read.messages)?;
    encode_event_read(&mut payload, &read.events)?;

    Ok(Frame {
        version: PROTOCOL_V1,
        opcode: Op::ReplicationReadOk as u16,
        flags: 0,
        request_id: req_id,
        payload: payload.freeze(),
    })
}

pub fn decode_replication_read_ok(frame: &Frame) -> ProtocolResult<ReplicationReadOk> {
    let mut reader = Reader::new(&frame.payload);
    if reader.take(MAGIC.len())? != MAGIC {
        return Err(decode_error("invalid replication payload magic"));
    }

    let messages = decode_message_read(&mut reader)?;
    let events = decode_event_read(&mut reader)?;
    reader.finish()?;

    Ok(ReplicationReadOk { messages, events })
}

fn encode_message_read(out: &mut BytesMut, read: &ReplicationMessageRead) -> ProtocolResult<()> {
    match read {
        ReplicationMessageRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            out.put_u8(KIND_BATCH);
            out.put_u64(*epoch);
            out.put_u64(*requested_offset);
            out.put_u64(*next_offset);
            out.put_u32(checked_u32(records.len(), "message record count")?);
            for record in records {
                out.put_u64(record.offset);
                out.put_u16(record.flags);
                out.put_u32(checked_u32(record.headers.len(), "message header bytes")?);
                out.put_u32(checked_u32(record.payload.len(), "message payload bytes")?);
                out.extend_from_slice(&record.headers);
                out.extend_from_slice(&record.payload);
            }
        }
        ReplicationMessageRead::CheckpointRequired(required) => {
            encode_checkpoint_required(out, required);
        }
    }
    Ok(())
}

fn encode_event_read(out: &mut BytesMut, read: &ReplicationEventRead) -> ProtocolResult<()> {
    match read {
        ReplicationEventRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            out.put_u8(KIND_BATCH);
            out.put_u64(*epoch);
            out.put_u64(*requested_offset);
            out.put_u64(*next_offset);
            out.put_u32(checked_u32(records.len(), "event record count")?);
            for record in records {
                out.put_u64(record.offset);
                out.put_u32(checked_u32(record.payload.len(), "event payload bytes")?);
                out.extend_from_slice(&record.payload);
            }
        }
        ReplicationEventRead::CheckpointRequired(required) => {
            encode_checkpoint_required(out, required);
        }
    }
    Ok(())
}

fn encode_checkpoint_required(out: &mut BytesMut, required: &ReplicationCheckpointRequired) {
    out.put_u8(KIND_CHECKPOINT_REQUIRED);
    out.put_u64(required.epoch);
    out.put_u64(required.requested_offset);
    out.put_u64(required.head_offset);
    out.put_u64(required.next_offset);
}

fn decode_message_read(reader: &mut Reader<'_>) -> ProtocolResult<ReplicationMessageRead> {
    match reader.u8()? {
        KIND_BATCH => {
            let epoch = reader.u64()?;
            let requested_offset = reader.u64()?;
            let next_offset = reader.u64()?;
            let count = reader.u32()? as usize;

            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                let offset = reader.u64()?;
                let flags = reader.u16()?;
                let header_len = reader.u32()? as usize;
                let payload_len = reader.u32()? as usize;
                let headers = reader.take(header_len)?.to_vec();
                let payload = reader.take(payload_len)?.to_vec();
                records.push(ReplicationMessageRecord {
                    offset,
                    flags,
                    headers,
                    payload,
                });
            }
            validate_message_record_offsets(requested_offset, next_offset, &records)?;

            Ok(ReplicationMessageRead::Batch {
                epoch,
                requested_offset,
                next_offset,
                records,
            })
        }
        KIND_CHECKPOINT_REQUIRED => Ok(ReplicationMessageRead::CheckpointRequired(
            decode_checkpoint_required(reader)?,
        )),
        other => Err(decode_error(format!("unknown message read kind {other}"))),
    }
}

fn decode_event_read(reader: &mut Reader<'_>) -> ProtocolResult<ReplicationEventRead> {
    match reader.u8()? {
        KIND_BATCH => {
            let epoch = reader.u64()?;
            let requested_offset = reader.u64()?;
            let next_offset = reader.u64()?;
            let count = reader.u32()? as usize;

            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                let offset = reader.u64()?;
                let payload_len = reader.u32()? as usize;
                let payload = reader.take(payload_len)?.to_vec();
                records.push(ReplicationEventRecord { offset, payload });
            }
            validate_event_record_offsets(requested_offset, next_offset, &records)?;

            Ok(ReplicationEventRead::Batch {
                epoch,
                requested_offset,
                next_offset,
                records,
            })
        }
        KIND_CHECKPOINT_REQUIRED => Ok(ReplicationEventRead::CheckpointRequired(
            decode_checkpoint_required(reader)?,
        )),
        other => Err(decode_error(format!("unknown event read kind {other}"))),
    }
}

fn decode_checkpoint_required(
    reader: &mut Reader<'_>,
) -> ProtocolResult<ReplicationCheckpointRequired> {
    Ok(ReplicationCheckpointRequired {
        epoch: reader.u64()?,
        requested_offset: reader.u64()?,
        head_offset: reader.u64()?,
        next_offset: reader.u64()?,
    })
}

fn validate_message_record_offsets(
    requested_offset: u64,
    next_offset: u64,
    records: &[ReplicationMessageRecord],
) -> ProtocolResult<()> {
    validate_record_offsets(
        requested_offset,
        next_offset,
        records.iter().map(|record| record.offset),
        "messages",
    )
}

fn validate_event_record_offsets(
    requested_offset: u64,
    next_offset: u64,
    records: &[ReplicationEventRecord],
) -> ProtocolResult<()> {
    validate_record_offsets(
        requested_offset,
        next_offset,
        records.iter().map(|record| record.offset),
        "events",
    )
}

fn validate_record_offsets(
    requested_offset: u64,
    next_offset: u64,
    offsets: impl IntoIterator<Item = u64>,
    label: &str,
) -> ProtocolResult<()> {
    let mut previous: Option<u64> = None;
    for offset in offsets {
        if offset < requested_offset {
            return Err(decode_error(format!(
                "{label} record offset {offset} is before requested offset {requested_offset}"
            )));
        }

        if let Some(previous) = previous {
            let expected = previous
                .checked_add(1)
                .ok_or_else(|| decode_error(format!("{label} record offset overflow")))?;
            if offset != expected {
                return Err(decode_error(format!(
                    "{label} record offset {offset} is not contiguous after {previous}"
                )));
            }
        }

        previous = Some(offset);
    }

    let minimum_next = previous
        .map(|offset| {
            offset
                .checked_add(1)
                .ok_or_else(|| decode_error(format!("{label} next offset overflow")))
        })
        .transpose()?
        .unwrap_or(requested_offset);
    if next_offset < minimum_next {
        return Err(decode_error(format!(
            "{label} next offset {next_offset} is before minimum next offset {minimum_next}"
        )));
    }

    Ok(())
}

fn checked_u32(value: usize, field: &str) -> ProtocolResult<u32> {
    u32::try_from(value).map_err(|_| ProtocolError::Encode(format!("{field} exceeds u32")))
}

struct Reader<'a> {
    payload: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, cursor: 0 }
    }

    fn take(&mut self, len: usize) -> ProtocolResult<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| decode_error("replication payload cursor overflow"))?;
        if end > self.payload.len() {
            return Err(decode_error("truncated replication payload"));
        }
        let bytes = &self.payload[self.cursor..end];
        self.cursor = end;
        Ok(bytes)
    }

    fn finish(&self) -> ProtocolResult<()> {
        if self.cursor != self.payload.len() {
            return Err(decode_error("trailing bytes in replication payload"));
        }
        Ok(())
    }

    fn u8(&mut self) -> ProtocolResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> ProtocolResult<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn u32(&mut self) -> ProtocolResult<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn u64(&mut self) -> ProtocolResult<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }
}

fn decode_error(message: impl Into<String>) -> ProtocolError {
    ProtocolError::Decode(message.into())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::v1::{ReplicationEventRecord, ReplicationMessageRecord};

    fn sample_read_ok() -> ReplicationReadOk {
        ReplicationReadOk {
            messages: ReplicationMessageRead::Batch {
                epoch: 4,
                requested_offset: 10,
                next_offset: 12,
                records: vec![
                    ReplicationMessageRecord {
                        offset: 10,
                        flags: 1,
                        headers: b"headers-a".to_vec(),
                        payload: b"payload-a".to_vec(),
                    },
                    ReplicationMessageRecord {
                        offset: 11,
                        flags: 2,
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
        }
    }

    #[test]
    fn raw_replication_read_ok_roundtrips_batches() {
        let msg = sample_read_ok();
        let frame = encode_replication_read_ok(14, &msg).unwrap();
        assert_eq!(frame.version, PROTOCOL_V1);
        assert_eq!(frame.opcode, Op::ReplicationReadOk as u16);
        assert_eq!(frame.request_id, 14);
        assert_eq!(decode_replication_read_ok(&frame).unwrap(), msg);
    }

    #[test]
    fn raw_replication_read_ok_roundtrips_checkpoint_required() {
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

        let frame = encode_replication_read_ok(15, &msg).unwrap();
        assert_eq!(decode_replication_read_ok(&frame).unwrap(), msg);
    }

    #[test]
    fn raw_replication_read_ok_rejects_msgpack_payload() {
        let msg = sample_read_ok();
        let frame = crate::v1::helper::try_encode(Op::ReplicationReadOk, 14, &msg).unwrap();
        assert!(matches!(
            decode_replication_read_ok(&frame),
            Err(ProtocolError::Decode(_))
        ));
    }

    #[test]
    fn raw_replication_read_ok_rejects_truncated_payload() {
        let frame = Frame {
            version: PROTOCOL_V1,
            opcode: Op::ReplicationReadOk as u16,
            flags: 0,
            request_id: 9,
            payload: Bytes::from_static(b"FRR2\0"),
        };
        assert!(matches!(
            decode_replication_read_ok(&frame),
            Err(ProtocolError::Decode(_))
        ));
    }

    #[test]
    fn raw_replication_read_ok_preserves_tail_next_offset() {
        let mut msg = sample_read_ok();
        if let ReplicationMessageRead::Batch {
            ref mut next_offset,
            ..
        } = msg.messages
        {
            *next_offset = 99;
        }

        let frame = encode_replication_read_ok(14, &msg).unwrap();
        assert_eq!(decode_replication_read_ok(&frame).unwrap(), msg);
    }

    #[test]
    fn raw_replication_read_ok_rejects_offset_before_request() {
        let mut msg = sample_read_ok();
        if let ReplicationMessageRead::Batch {
            ref mut records, ..
        } = msg.messages
        {
            records[0].offset = 9;
        }

        let frame = encode_replication_read_ok(14, &msg).unwrap();
        assert!(matches!(
            decode_replication_read_ok(&frame),
            Err(ProtocolError::Decode(_))
        ));
    }

    #[test]
    fn raw_replication_read_ok_rejects_non_contiguous_records() {
        let mut msg = sample_read_ok();
        if let ReplicationEventRead::Batch {
            ref mut records, ..
        } = msg.events
        {
            records.push(ReplicationEventRecord {
                offset: 22,
                payload: b"gap".to_vec(),
            });
        }

        let frame = encode_replication_read_ok(14, &msg).unwrap();
        assert!(matches!(
            decode_replication_read_ok(&frame),
            Err(ProtocolError::Decode(_))
        ));
    }
}
