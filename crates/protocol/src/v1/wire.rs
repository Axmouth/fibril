use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use thiserror::Error;

use crate::v1::{
    Ack, ContentType, Deliver, DeliveryTag, Nack, Op, PROTOCOL_V1, Partition, Publish,
    PublishDelayed, PublishOk, frame::Frame,
};

pub type WireResult<T> = Result<T, WireError>;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum WireError {
    #[error("unexpected opcode {actual}; expected {expected}")]
    UnexpectedOpcode { expected: u16, actual: u16 },

    #[error("unexpected end of payload")]
    UnexpectedEof,

    #[error("invalid magic for {context}")]
    InvalidMagic { context: &'static str },

    #[error("invalid utf-8 string")]
    InvalidUtf8,

    #[error("invalid bool tag {0}")]
    InvalidBool(u8),

    #[error("invalid option tag {0}")]
    InvalidOption(u8),

    #[error("unknown content type tag {0}")]
    UnknownContentType(u8),

    #[error("payload has trailing bytes: {0}")]
    TrailingBytes(usize),

    #[error("field too large for wire format: {0}")]
    FieldTooLarge(&'static str),
}

pub fn encode_publish(request_id: u64, publish: &Publish) -> WireResult<Frame> {
    let mut out = payload_builder(b"FPB1");
    put_publish_common(&mut out, publish)?;
    Ok(frame(Op::Publish, request_id, out.freeze()))
}

pub fn decode_publish(frame: &Frame) -> WireResult<Publish> {
    expect_op(frame, Op::Publish)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FPB1", "publish")?;
    let value = read_publish_common(&mut reader)?;
    reader.finish()?;
    Ok(value)
}

pub fn encode_publish_delayed(request_id: u64, publish: &PublishDelayed) -> WireResult<Frame> {
    let mut out = payload_builder(b"FPD1");
    put_str(&mut out, &publish.topic)?;
    put_optional_str(&mut out, publish.group.as_deref())?;
    out.put_u32(publish.partition.id());
    put_bool(&mut out, publish.require_confirm);
    out.put_u64(publish.not_before);
    put_content_type(&mut out, publish.content_type.as_ref())?;
    put_headers(&mut out, &publish.headers)?;
    out.put_u64(publish.published);
    put_optional_bytes(&mut out, publish.partition_key.as_deref())?;
    out.put_u64(publish.partitioning_version);
    put_bytes(&mut out, &publish.payload)?;
    Ok(frame(Op::PublishDelayed, request_id, out.freeze()))
}

pub fn decode_publish_delayed(frame: &Frame) -> WireResult<PublishDelayed> {
    expect_op(frame, Op::PublishDelayed)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FPD1", "publish delayed")?;
    let topic = reader.str()?.to_owned();
    let group = reader.optional_str()?.map(ToOwned::to_owned);
    let partition = Partition::new(reader.u32()?);
    let require_confirm = reader.bool()?;
    let not_before = reader.u64()?;
    let content_type = reader.content_type()?;
    let headers = reader.headers()?;
    let published = reader.u64()?;
    let partition_key = reader.optional_bytes()?.map(ToOwned::to_owned);
    let partitioning_version = reader.u64()?;
    let payload = reader.bytes()?.to_vec();
    reader.finish()?;

    Ok(PublishDelayed {
        topic,
        partition,
        group,
        require_confirm,
        not_before,
        content_type,
        headers,
        payload,
        published,
        partition_key,
        partitioning_version,
    })
}

pub fn encode_publish_ok(request_id: u64, publish_ok: &PublishOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FPO1");
    out.put_u64(publish_ok.offset);
    Ok(frame(Op::PublishOk, request_id, out.freeze()))
}

pub fn decode_publish_ok(frame: &Frame) -> WireResult<PublishOk> {
    expect_op(frame, Op::PublishOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FPO1", "publish ok")?;
    let offset = reader.u64()?;
    reader.finish()?;
    Ok(PublishOk { offset })
}

pub fn encode_deliver(request_id: u64, deliver: &Deliver) -> WireResult<Frame> {
    let mut out = payload_builder(b"FDL1");
    out.put_u64(deliver.sub_id);
    put_str(&mut out, &deliver.topic)?;
    put_optional_str(&mut out, deliver.group.as_deref())?;
    out.put_u32(deliver.partition.id());
    out.put_u64(deliver.offset);
    out.put_u64(deliver.delivery_tag.epoch);
    out.put_u64(deliver.published);
    out.put_u64(deliver.publish_received);
    put_content_type(&mut out, deliver.content_type.as_ref())?;
    put_headers(&mut out, &deliver.headers)?;
    put_bytes(&mut out, &deliver.payload)?;
    Ok(frame(Op::Deliver, request_id, out.freeze()))
}

pub fn decode_deliver(frame: &Frame) -> WireResult<Deliver> {
    expect_op(frame, Op::Deliver)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FDL1", "deliver")?;
    let sub_id = reader.u64()?;
    let topic = reader.str()?.to_owned();
    let group = reader.optional_str()?.map(ToOwned::to_owned);
    let partition = Partition::new(reader.u32()?);
    let offset = reader.u64()?;
    let delivery_tag = DeliveryTag {
        epoch: reader.u64()?,
    };
    let published = reader.u64()?;
    let publish_received = reader.u64()?;
    let content_type = reader.content_type()?;
    let headers = reader.headers()?;
    let payload = reader.bytes()?.to_vec();
    reader.finish()?;

    Ok(Deliver {
        sub_id,
        topic,
        group,
        partition,
        offset,
        delivery_tag,
        published,
        publish_received,
        content_type,
        headers,
        payload,
    })
}

pub fn encode_ack(request_id: u64, ack: &Ack) -> WireResult<Frame> {
    let mut out = payload_builder(b"FAK1");
    put_settle_common(
        &mut out,
        &ack.topic,
        ack.group.as_deref(),
        ack.partition,
        &ack.tags,
    )?;
    Ok(frame(Op::Ack, request_id, out.freeze()))
}

pub fn decode_ack(frame: &Frame) -> WireResult<Ack> {
    expect_op(frame, Op::Ack)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FAK1", "ack")?;
    let (topic, group, partition, tags) = reader.settle_common()?;
    reader.finish()?;
    Ok(Ack {
        topic,
        group,
        partition,
        tags,
    })
}

pub fn encode_nack(request_id: u64, nack: &Nack) -> WireResult<Frame> {
    let mut out = payload_builder(b"FNK1");
    put_settle_common(
        &mut out,
        &nack.topic,
        nack.group.as_deref(),
        nack.partition,
        &nack.tags,
    )?;
    put_bool(&mut out, nack.requeue);
    put_optional_u64(&mut out, nack.not_before);
    Ok(frame(Op::Nack, request_id, out.freeze()))
}

pub fn decode_nack(frame: &Frame) -> WireResult<Nack> {
    expect_op(frame, Op::Nack)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FNK1", "nack")?;
    let (topic, group, partition, tags) = reader.settle_common()?;
    let requeue = reader.bool()?;
    let not_before = reader.optional_u64()?;
    reader.finish()?;
    Ok(Nack {
        topic,
        group,
        partition,
        tags,
        requeue,
        not_before,
    })
}

fn put_publish_common(out: &mut BytesMut, publish: &Publish) -> WireResult<()> {
    put_str(out, &publish.topic)?;
    put_optional_str(out, publish.group.as_deref())?;
    out.put_u32(publish.partition.id());
    put_bool(out, publish.require_confirm);
    put_content_type(out, publish.content_type.as_ref())?;
    put_headers(out, &publish.headers)?;
    out.put_u64(publish.published);
    put_optional_bytes(out, publish.partition_key.as_deref())?;
    out.put_u64(publish.partitioning_version);
    put_bytes(out, &publish.payload)?;
    Ok(())
}

fn read_publish_common(reader: &mut Reader<'_>) -> WireResult<Publish> {
    let topic = reader.str()?.to_owned();
    let group = reader.optional_str()?.map(ToOwned::to_owned);
    let partition = Partition::new(reader.u32()?);
    let require_confirm = reader.bool()?;
    let content_type = reader.content_type()?;
    let headers = reader.headers()?;
    let published = reader.u64()?;
    let partition_key = reader.optional_bytes()?.map(ToOwned::to_owned);
    let partitioning_version = reader.u64()?;
    let payload = reader.bytes()?.to_vec();

    Ok(Publish {
        topic,
        partition,
        group,
        require_confirm,
        content_type,
        headers,
        payload,
        published,
        partition_key,
        partitioning_version,
    })
}

fn put_settle_common(
    out: &mut BytesMut,
    topic: &str,
    group: Option<&str>,
    partition: Partition,
    tags: &[DeliveryTag],
) -> WireResult<()> {
    put_str(out, topic)?;
    put_optional_str(out, group)?;
    out.put_u32(partition.id());
    put_len(out, tags.len(), "tags")?;
    for tag in tags {
        out.put_u64(tag.epoch);
    }
    Ok(())
}

fn payload_builder(magic: &[u8; 4]) -> BytesMut {
    let mut out = BytesMut::new();
    out.extend_from_slice(magic);
    out
}

fn frame(op: Op, request_id: u64, payload: Bytes) -> Frame {
    Frame {
        version: PROTOCOL_V1,
        opcode: op as u16,
        flags: 0,
        request_id,
        payload,
    }
}

fn expect_op(frame: &Frame, op: Op) -> WireResult<()> {
    let expected = op as u16;
    if frame.opcode == expected {
        Ok(())
    } else {
        Err(WireError::UnexpectedOpcode {
            expected,
            actual: frame.opcode,
        })
    }
}

fn put_len(out: &mut BytesMut, len: usize, field: &'static str) -> WireResult<()> {
    let len = u32::try_from(len).map_err(|_| WireError::FieldTooLarge(field))?;
    out.put_u32(len);
    Ok(())
}

fn put_bytes(out: &mut BytesMut, bytes: &[u8]) -> WireResult<()> {
    put_len(out, bytes.len(), "bytes")?;
    out.extend_from_slice(bytes);
    Ok(())
}

fn put_str(out: &mut BytesMut, value: &str) -> WireResult<()> {
    put_bytes(out, value.as_bytes())
}

fn put_bool(out: &mut BytesMut, value: bool) {
    out.put_u8(u8::from(value));
}

fn put_optional_bytes(out: &mut BytesMut, value: Option<&[u8]>) -> WireResult<()> {
    match value {
        Some(value) => {
            out.put_u8(1);
            put_bytes(out, value)
        }
        None => {
            out.put_u8(0);
            Ok(())
        }
    }
}

fn put_optional_str(out: &mut BytesMut, value: Option<&str>) -> WireResult<()> {
    match value {
        Some(value) => {
            out.put_u8(1);
            put_str(out, value)
        }
        None => {
            out.put_u8(0);
            Ok(())
        }
    }
}

fn put_optional_u64(out: &mut BytesMut, value: Option<u64>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            out.put_u64(value);
        }
        None => out.put_u8(0),
    }
}

fn put_content_type(out: &mut BytesMut, content_type: Option<&ContentType>) -> WireResult<()> {
    match content_type {
        None => out.put_u8(0),
        Some(ContentType::MsgPack) => out.put_u8(1),
        Some(ContentType::Json) => out.put_u8(2),
        Some(ContentType::Text) => out.put_u8(3),
        Some(ContentType::Custom(value)) => {
            out.put_u8(4);
            put_str(out, value)?;
        }
    }
    Ok(())
}

fn put_headers(out: &mut BytesMut, headers: &HashMap<String, String>) -> WireResult<()> {
    put_len(out, headers.len(), "headers")?;
    for (key, value) in headers {
        put_str(out, key)?;
        put_str(out, value)?;
    }
    Ok(())
}

struct Reader<'a> {
    input: &'a [u8],
    cursor: usize,
}

impl<'a> Reader<'a> {
    fn new(input: &'a [u8]) -> Self {
        Self { input, cursor: 0 }
    }

    fn expect_magic(&mut self, magic: &[u8; 4], context: &'static str) -> WireResult<()> {
        let got = self.take(4)?;
        if got == magic {
            Ok(())
        } else {
            Err(WireError::InvalidMagic { context })
        }
    }

    fn finish(&self) -> WireResult<()> {
        let remaining = self.input.len().saturating_sub(self.cursor);
        if remaining == 0 {
            Ok(())
        } else {
            Err(WireError::TrailingBytes(remaining))
        }
    }

    fn take(&mut self, len: usize) -> WireResult<&'a [u8]> {
        let end = self
            .cursor
            .checked_add(len)
            .ok_or(WireError::UnexpectedEof)?;
        if end > self.input.len() {
            return Err(WireError::UnexpectedEof);
        }
        let bytes = &self.input[self.cursor..end];
        self.cursor = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> WireResult<u8> {
        Ok(*self.take(1)?.first().ok_or(WireError::UnexpectedEof)?)
    }

    fn u32(&mut self) -> WireResult<u32> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| WireError::UnexpectedEof)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> WireResult<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| WireError::UnexpectedEof)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn bool(&mut self) -> WireResult<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(WireError::InvalidBool(value)),
        }
    }

    fn bytes(&mut self) -> WireResult<&'a [u8]> {
        let len = self.u32()? as usize;
        self.take(len)
    }

    fn str(&mut self) -> WireResult<&'a str> {
        std::str::from_utf8(self.bytes()?).map_err(|_| WireError::InvalidUtf8)
    }

    fn optional_bytes(&mut self) -> WireResult<Option<&'a [u8]>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.bytes().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn optional_str(&mut self) -> WireResult<Option<&'a str>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.str().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn optional_u64(&mut self) -> WireResult<Option<u64>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.u64().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn content_type(&mut self) -> WireResult<Option<ContentType>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(ContentType::MsgPack)),
            2 => Ok(Some(ContentType::Json)),
            3 => Ok(Some(ContentType::Text)),
            4 => self
                .str()
                .map(|value| Some(ContentType::Custom(value.to_owned()))),
            value => Err(WireError::UnknownContentType(value)),
        }
    }

    fn headers(&mut self) -> WireResult<HashMap<String, String>> {
        let count = self.u32()? as usize;
        let mut headers = HashMap::with_capacity(count);
        for _ in 0..count {
            let key = self.str()?.to_owned();
            let value = self.str()?.to_owned();
            headers.insert(key, value);
        }
        Ok(headers)
    }

    fn settle_common(
        &mut self,
    ) -> WireResult<(String, Option<String>, Partition, Vec<DeliveryTag>)> {
        let topic = self.str()?.to_owned();
        let group = self.optional_str()?.map(ToOwned::to_owned);
        let partition = Partition::new(self.u32()?);
        let count = self.u32()? as usize;
        let mut tags = Vec::with_capacity(count);
        for _ in 0..count {
            tags.push(DeliveryTag { epoch: self.u64()? });
        }
        Ok((topic, group, partition, tags))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers() -> HashMap<String, String> {
        HashMap::from([
            ("trace".to_string(), "abc".to_string()),
            ("tenant".to_string(), "west".to_string()),
        ])
    }

    fn assert_publish_eq(got: &Publish, expected: &Publish) {
        assert_eq!(got.topic, expected.topic);
        assert_eq!(got.partition, expected.partition);
        assert_eq!(got.group, expected.group);
        assert_eq!(got.require_confirm, expected.require_confirm);
        assert_eq!(
            got.content_type.as_ref().map(ContentType::as_header),
            expected.content_type.as_ref().map(ContentType::as_header)
        );
        assert_eq!(got.headers, expected.headers);
        assert_eq!(got.payload, expected.payload);
        assert_eq!(got.published, expected.published);
        assert_eq!(got.partition_key, expected.partition_key);
        assert_eq!(got.partitioning_version, expected.partitioning_version);
    }

    #[test]
    fn publish_roundtrips() {
        let publish = Publish {
            topic: "orders".into(),
            partition: Partition::new(3),
            group: Some("workers".into()),
            require_confirm: true,
            content_type: Some(ContentType::Json),
            headers: headers(),
            payload: vec![1, 2, 3, 4],
            published: 99,
            partition_key: Some(b"customer-7".to_vec()),
            partitioning_version: 12,
        };

        let frame = encode_publish(42, &publish).unwrap();
        assert_eq!(frame.opcode, Op::Publish as u16);
        assert_eq!(frame.request_id, 42);
        assert_publish_eq(&decode_publish(&frame).unwrap(), &publish);
    }

    #[test]
    fn publish_delayed_roundtrips() {
        let publish = PublishDelayed {
            topic: "jobs".into(),
            partition: Partition::new(1),
            group: None,
            require_confirm: false,
            not_before: 1234,
            content_type: Some(ContentType::Custom("application/x-test".into())),
            headers: headers(),
            payload: vec![9, 8, 7],
            published: 100,
            partition_key: None,
            partitioning_version: 5,
        };

        let frame = encode_publish_delayed(2, &publish).unwrap();
        let got = decode_publish_delayed(&frame).unwrap();
        assert_eq!(got.topic, publish.topic);
        assert_eq!(got.partition, publish.partition);
        assert_eq!(got.group, publish.group);
        assert_eq!(got.require_confirm, publish.require_confirm);
        assert_eq!(got.not_before, publish.not_before);
        assert_eq!(
            got.content_type.as_ref().map(ContentType::as_header),
            publish.content_type.as_ref().map(ContentType::as_header)
        );
        assert_eq!(got.headers, publish.headers);
        assert_eq!(got.payload, publish.payload);
        assert_eq!(got.published, publish.published);
        assert_eq!(got.partition_key, publish.partition_key);
        assert_eq!(got.partitioning_version, publish.partitioning_version);
    }

    #[test]
    fn deliver_roundtrips() {
        let deliver = Deliver {
            sub_id: 11,
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(2),
            offset: 44,
            delivery_tag: DeliveryTag { epoch: 55 },
            published: 66,
            publish_received: 77,
            content_type: Some(ContentType::Text),
            headers: headers(),
            payload: b"hello".to_vec(),
        };

        let frame = encode_deliver(10, &deliver).unwrap();
        let got = decode_deliver(&frame).unwrap();
        assert_eq!(got.sub_id, deliver.sub_id);
        assert_eq!(got.topic, deliver.topic);
        assert_eq!(got.group, deliver.group);
        assert_eq!(got.partition, deliver.partition);
        assert_eq!(got.offset, deliver.offset);
        assert_eq!(got.delivery_tag, deliver.delivery_tag);
        assert_eq!(got.published, deliver.published);
        assert_eq!(got.publish_received, deliver.publish_received);
        assert_eq!(
            got.content_type.as_ref().map(ContentType::as_header),
            deliver.content_type.as_ref().map(ContentType::as_header)
        );
        assert_eq!(got.headers, deliver.headers);
        assert_eq!(got.payload, deliver.payload);
    }

    #[test]
    fn publish_ok_roundtrips() {
        let frame = encode_publish_ok(88, &PublishOk { offset: 123 }).unwrap();
        assert_eq!(decode_publish_ok(&frame).unwrap().offset, 123);
    }

    #[test]
    fn ack_and_nack_roundtrip() {
        let ack = Ack {
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(4),
            tags: vec![DeliveryTag { epoch: 1 }, DeliveryTag { epoch: 2 }],
        };
        let ack_frame = encode_ack(1, &ack).unwrap();
        let got_ack = decode_ack(&ack_frame).unwrap();
        assert_eq!(got_ack.topic, ack.topic);
        assert_eq!(got_ack.group, ack.group);
        assert_eq!(got_ack.partition, ack.partition);
        assert_eq!(got_ack.tags, ack.tags);

        let nack = Nack {
            topic: "orders".into(),
            group: None,
            partition: Partition::new(5),
            tags: vec![DeliveryTag { epoch: 9 }],
            requeue: true,
            not_before: Some(777),
        };
        let nack_frame = encode_nack(2, &nack).unwrap();
        let got_nack = decode_nack(&nack_frame).unwrap();
        assert_eq!(got_nack.topic, nack.topic);
        assert_eq!(got_nack.group, nack.group);
        assert_eq!(got_nack.partition, nack.partition);
        assert_eq!(got_nack.tags, nack.tags);
        assert_eq!(got_nack.requeue, nack.requeue);
        assert_eq!(got_nack.not_before, nack.not_before);
    }

    #[test]
    fn malformed_publish_reports_typed_error() {
        let frame = Frame {
            version: PROTOCOL_V1,
            opcode: Op::Publish as u16,
            flags: 0,
            request_id: 1,
            payload: Bytes::from_static(b"FPB1\x00"),
        };

        assert!(matches!(
            decode_publish(&frame),
            Err(WireError::UnexpectedEof)
        ));
    }

    #[test]
    fn wrong_opcode_reports_typed_error() {
        let frame = Frame {
            version: PROTOCOL_V1,
            opcode: Op::Deliver as u16,
            flags: 0,
            request_id: 1,
            payload: Bytes::new(),
        };

        let err = decode_publish(&frame).unwrap_err();
        assert_eq!(
            err,
            WireError::UnexpectedOpcode {
                expected: Op::Publish as u16,
                actual: Op::Deliver as u16
            }
        );
    }
}
