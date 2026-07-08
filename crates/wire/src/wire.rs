use bytes::{BufMut, Bytes, BytesMut};
use std::collections::HashMap;
use thiserror::Error;

use uuid::Uuid;

use crate::{
    Ack, AdvertisedAddress, AssignmentChanged, Auth, ContentType, DeclarePlexus, DeclarePlexusOk,
    DeclareQueue, DeclareQueueOk, Deliver, DeliveryTag, ErrorMsg, GoingAway, Hello, HelloOk, Nack,
    Op, PROTOCOL_V1, Partition, Publish, PublishDelayed, PublishOk, QueueDlqPolicy,
    QueueTopologyEntry, ReconcileAction, ReconcileClient, ReconcilePolicy, ReconcileResult,
    ReconcileServer, ReconcileSubscription, ReconcileSubscriptionResult, Redirect,
    ReplicationApply, ReplicationApplyOk, ReplicationCheckpointExport,
    ReplicationCheckpointExportOk, ReplicationCheckpointInstall, ReplicationCheckpointInstallOk,
    ReplicationCheckpointRequired, ReplicationEventApplyBatch, ReplicationEventRead,
    ReplicationEventRecord, ReplicationMessageApplyBatch, ReplicationMessageRead,
    ReplicationMessageRecord, ReplicationRead, ReplicationReadOk, ReplicationStateCheckpoint,
    ReplicationStreamEnd, ReplicationStreamProgress, ReplicationStreamReset,
    ReplicationStreamStart, ResumeIdentity, ResumeOutcome, StreamDurability, StreamRetention,
    StreamStart, StreamTopologyEntry, Subscribe, SubscribeOk, SubscribeStream, TopologyOk,
    TopologyRequest, TopologyUpdateAck, frame::Frame,
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

    #[error("invalid uuid bytes")]
    InvalidUuid,

    #[error("invalid bool tag {0}")]
    InvalidBool(u8),

    #[error("invalid option tag {0}")]
    InvalidOption(u8),

    #[error("unknown content type tag {0}")]
    UnknownContentType(u8),

    #[error("unknown {context} tag {value}")]
    UnknownTag { context: &'static str, value: u8 },

    #[error("invalid record sequence for {0}")]
    InvalidRecordSequence(&'static str),

    #[error("payload has trailing bytes: {0}")]
    TrailingBytes(usize),

    #[error("field too large for wire format: {0}")]
    FieldTooLarge(&'static str),
}

pub fn encode_hello(request_id: u64, hello: &Hello) -> WireResult<Frame> {
    let mut out = payload_builder(b"FHL1");
    put_str(&mut out, &hello.client_name)?;
    put_str(&mut out, &hello.client_version)?;
    out.put_u16(hello.protocol_version);
    put_optional_resume_identity(&mut out, hello.resume.as_ref());
    Ok(frame(Op::Hello, request_id, out.freeze()))
}

pub fn decode_hello(frame: &Frame) -> WireResult<Hello> {
    expect_op(frame, Op::Hello)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FHL1", "hello")?;
    let hello = Hello {
        client_name: reader.str()?.to_owned(),
        client_version: reader.str()?.to_owned(),
        protocol_version: reader.u16()?,
        resume: reader.optional_resume_identity()?,
    };
    reader.finish()?;
    Ok(hello)
}

pub fn encode_hello_ok(request_id: u64, hello: &HelloOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FHO1");
    out.put_u16(hello.protocol_version);
    put_uuid(&mut out, hello.owner_id);
    put_uuid(&mut out, hello.client_id);
    put_uuid(&mut out, hello.resume_token);
    put_resume_outcome(&mut out, hello.resume_outcome);
    put_str(&mut out, &hello.server_name)?;
    put_str(&mut out, &hello.compliance)?;
    Ok(frame(Op::HelloOk, request_id, out.freeze()))
}

pub fn decode_hello_ok(frame: &Frame) -> WireResult<HelloOk> {
    expect_op(frame, Op::HelloOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FHO1", "hello ok")?;
    let hello = HelloOk {
        protocol_version: reader.u16()?,
        owner_id: reader.uuid()?,
        client_id: reader.uuid()?,
        resume_token: reader.uuid()?,
        resume_outcome: reader.resume_outcome()?,
        server_name: reader.str()?.to_owned(),
        compliance: reader.str()?.to_owned(),
    };
    reader.finish()?;
    Ok(hello)
}

pub fn encode_auth(request_id: u64, auth: &Auth) -> WireResult<Frame> {
    let mut out = payload_builder(b"FAU1");
    put_str(&mut out, &auth.username)?;
    put_str(&mut out, &auth.password)?;
    Ok(frame(Op::Auth, request_id, out.freeze()))
}

pub fn decode_auth(frame: &Frame) -> WireResult<Auth> {
    expect_op(frame, Op::Auth)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FAU1", "auth")?;
    let auth = Auth {
        username: reader.str()?.to_owned(),
        password: reader.str()?.to_owned(),
    };
    reader.finish()?;
    Ok(auth)
}

pub fn encode_error_message(op: Op, request_id: u64, error: &ErrorMsg) -> WireResult<Frame> {
    expect_error_op(op as u16)?;
    let mut out = payload_builder(b"FER1");
    out.put_u16(error.code);
    put_str(&mut out, &error.message)?;
    Ok(frame(op, request_id, out.freeze()))
}

pub fn decode_error_message(frame: &Frame) -> WireResult<ErrorMsg> {
    expect_error_op(frame.opcode)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FER1", "error")?;
    let error = ErrorMsg {
        code: reader.u16()?,
        message: reader.str()?.to_owned(),
    };
    reader.finish()?;
    Ok(error)
}

pub fn encode_unit(op: Op, request_id: u64) -> WireResult<Frame> {
    match op {
        Op::AuthOk | Op::Ping | Op::Pong => Ok(frame(op, request_id, Bytes::new())),
        _ => Err(WireError::UnexpectedOpcode {
            expected: 0,
            actual: op as u16,
        }),
    }
}

pub fn decode_unit(frame: &Frame, op: Op) -> WireResult<()> {
    expect_op(frame, op)?;
    match op {
        Op::AuthOk | Op::Ping | Op::Pong => {
            let reader = Reader::new(&frame.payload);
            reader.finish()
        }
        _ => Err(WireError::UnexpectedOpcode {
            expected: 0,
            actual: op as u16,
        }),
    }
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

pub fn encode_declare_queue(request_id: u64, declare: &DeclareQueue) -> WireResult<Frame> {
    let mut out = payload_builder(b"FDQ1");
    put_str(&mut out, &declare.topic)?;
    put_optional_str(&mut out, declare.group.as_deref())?;
    put_optional_dlq_policy(&mut out, declare.dlq_policy.as_ref())?;
    put_optional_u32(&mut out, declare.dlq_max_retries);
    put_optional_u32(&mut out, declare.partition_count);
    // Trailing so a peer that omits it still decodes (read as None).
    put_optional_u64(&mut out, declare.default_message_ttl_ms);
    Ok(frame(Op::DeclareQueue, request_id, out.freeze()))
}

pub fn decode_declare_queue(frame: &Frame) -> WireResult<DeclareQueue> {
    expect_op(frame, Op::DeclareQueue)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FDQ1", "declare queue")?;
    let declare = DeclareQueue {
        topic: reader.str()?.to_owned(),
        group: reader.optional_str()?.map(ToOwned::to_owned),
        dlq_policy: reader.optional_dlq_policy()?,
        dlq_max_retries: reader.optional_u32()?,
        partition_count: reader.optional_u32()?,
        default_message_ttl_ms: if reader.remaining() > 0 {
            reader.optional_u64()?
        } else {
            None
        },
    };
    reader.finish()?;
    Ok(declare)
}

pub fn encode_declare_queue_ok(request_id: u64, ok: &DeclareQueueOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FDK1");
    put_str(&mut out, &ok.status)?;
    out.put_u32(ok.partition_count);
    Ok(frame(Op::DeclareQueueOk, request_id, out.freeze()))
}

pub fn decode_declare_queue_ok(frame: &Frame) -> WireResult<DeclareQueueOk> {
    expect_op(frame, Op::DeclareQueueOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FDK1", "declare queue ok")?;
    let ok = DeclareQueueOk {
        status: reader.str()?.to_owned(),
        partition_count: reader.u32()?,
    };
    reader.finish()?;
    Ok(ok)
}

pub fn encode_subscribe(request_id: u64, sub: &Subscribe) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSB1");
    put_queue_key(&mut out, &sub.topic, sub.partition, sub.group.as_deref())?;
    out.put_u32(sub.prefetch);
    put_bool(&mut out, sub.auto_ack);
    put_optional_str(&mut out, sub.consumer_group.as_deref())?;
    put_optional_u32(&mut out, sub.consumer_target);
    put_optional_uuid(&mut out, sub.member_id);
    Ok(frame(Op::Subscribe, request_id, out.freeze()))
}

pub fn decode_subscribe(frame: &Frame) -> WireResult<Subscribe> {
    expect_op(frame, Op::Subscribe)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSB1", "subscribe")?;
    let (topic, partition, group) = reader.queue_key()?;
    let sub = Subscribe {
        topic,
        partition,
        group,
        prefetch: reader.u32()?,
        auto_ack: reader.bool()?,
        consumer_group: reader.optional_str()?.map(ToOwned::to_owned),
        consumer_target: reader.optional_u32()?,
        member_id: reader.optional_uuid()?,
    };
    reader.finish()?;
    Ok(sub)
}

pub fn encode_subscribe_ok(request_id: u64, sub: &SubscribeOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSO1");
    out.put_u64(sub.sub_id);
    put_queue_key(&mut out, &sub.topic, sub.partition, sub.group.as_deref())?;
    out.put_u32(sub.prefetch);
    put_optional_str(&mut out, sub.consumer_group.as_deref())?;
    put_optional_u32(&mut out, sub.consumer_target);
    put_optional_uuid(&mut out, sub.member_id);
    Ok(frame(Op::SubscribeOk, request_id, out.freeze()))
}

pub fn decode_subscribe_ok(frame: &Frame) -> WireResult<SubscribeOk> {
    expect_op(frame, Op::SubscribeOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSO1", "subscribe ok")?;
    let sub_id = reader.u64()?;
    let (topic, partition, group) = reader.queue_key()?;
    let sub = SubscribeOk {
        sub_id,
        topic,
        group,
        partition,
        prefetch: reader.u32()?,
        consumer_group: reader.optional_str()?.map(ToOwned::to_owned),
        consumer_target: reader.optional_u32()?,
        member_id: reader.optional_uuid()?,
    };
    reader.finish()?;
    Ok(sub)
}

pub fn encode_declare_plexus(request_id: u64, declare: &DeclarePlexus) -> WireResult<Frame> {
    let mut out = payload_builder(b"FDP1");
    put_str(&mut out, &declare.topic)?;
    put_optional_u32(&mut out, declare.partition_count);
    out.put_u8(declare.durability.as_u8());
    put_stream_retention(&mut out, &declare.retention);
    put_optional_u32(&mut out, declare.replication_factor);
    Ok(frame(Op::DeclarePlexus, request_id, out.freeze()))
}

pub fn decode_declare_plexus(frame: &Frame) -> WireResult<DeclarePlexus> {
    expect_op(frame, Op::DeclarePlexus)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FDP1", "declare plexus")?;
    let declare = DeclarePlexus {
        topic: reader.str()?.to_owned(),
        partition_count: reader.optional_u32()?,
        durability: reader.stream_durability()?,
        retention: reader.stream_retention()?,
        replication_factor: reader.optional_u32()?,
    };
    reader.finish()?;
    Ok(declare)
}

pub fn encode_declare_plexus_ok(request_id: u64, ok: &DeclarePlexusOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FPK1");
    put_str(&mut out, &ok.status)?;
    out.put_u32(ok.partition_count);
    Ok(frame(Op::DeclarePlexusOk, request_id, out.freeze()))
}

pub fn decode_declare_plexus_ok(frame: &Frame) -> WireResult<DeclarePlexusOk> {
    expect_op(frame, Op::DeclarePlexusOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FPK1", "declare plexus ok")?;
    let ok = DeclarePlexusOk {
        status: reader.str()?.to_owned(),
        partition_count: reader.u32()?,
    };
    reader.finish()?;
    Ok(ok)
}

pub fn encode_subscribe_stream(request_id: u64, sub: &SubscribeStream) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSP1");
    put_str(&mut out, &sub.topic)?;
    put_partition(&mut out, sub.partition);
    put_optional_str(&mut out, sub.durable_name.as_deref())?;
    put_stream_start(&mut out, sub.start);
    put_len(&mut out, sub.filter.len(), "stream filter")?;
    for (key, pattern) in &sub.filter {
        put_str(&mut out, key)?;
        put_str(&mut out, pattern)?;
    }
    out.put_u32(sub.prefetch);
    put_bool(&mut out, sub.auto_ack);
    Ok(frame(Op::SubscribeStream, request_id, out.freeze()))
}

pub fn decode_subscribe_stream(frame: &Frame) -> WireResult<SubscribeStream> {
    expect_op(frame, Op::SubscribeStream)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSP1", "subscribe stream")?;
    let topic = reader.str()?.to_owned();
    let partition = reader.partition()?;
    let durable_name = reader.optional_str()?.map(ToOwned::to_owned);
    let start = reader.stream_start()?;
    let clause_count = reader.u32()? as usize;
    let mut filter = Vec::with_capacity(clause_count);
    for _ in 0..clause_count {
        let key = reader.str()?.to_owned();
        let pattern = reader.str()?.to_owned();
        filter.push((key, pattern));
    }
    let sub = SubscribeStream {
        topic,
        partition,
        durable_name,
        start,
        filter,
        prefetch: reader.u32()?,
        auto_ack: reader.bool()?,
    };
    reader.finish()?;
    Ok(sub)
}

pub fn encode_assignment_changed(
    request_id: u64,
    assignment: &AssignmentChanged,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FAC1");
    put_str(&mut out, &assignment.topic)?;
    put_optional_str(&mut out, assignment.group.as_deref())?;
    put_str(&mut out, &assignment.consumer_group)?;
    out.put_u64(assignment.generation);
    put_partitions(&mut out, &assignment.assigned)?;
    put_partitions(&mut out, &assignment.added)?;
    put_partitions(&mut out, &assignment.revoked)?;
    Ok(frame(Op::AssignmentChanged, request_id, out.freeze()))
}

pub fn decode_assignment_changed(frame: &Frame) -> WireResult<AssignmentChanged> {
    expect_op(frame, Op::AssignmentChanged)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FAC1", "assignment changed")?;
    let assignment = AssignmentChanged {
        topic: reader.str()?.to_owned(),
        group: reader.optional_str()?.map(ToOwned::to_owned),
        consumer_group: reader.str()?.to_owned(),
        generation: reader.u64()?,
        assigned: reader.partitions()?,
        added: reader.partitions()?,
        revoked: reader.partitions()?,
    };
    reader.finish()?;
    Ok(assignment)
}

pub fn encode_topology_request(request_id: u64, request: &TopologyRequest) -> WireResult<Frame> {
    let mut out = payload_builder(b"FTP1");
    put_optional_str(&mut out, request.topic.as_deref())?;
    put_optional_str(&mut out, request.group.as_deref())?;
    Ok(frame(Op::Topology, request_id, out.freeze()))
}

pub fn decode_topology_request(frame: &Frame) -> WireResult<TopologyRequest> {
    expect_op(frame, Op::Topology)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FTP1", "topology")?;
    let request = TopologyRequest {
        topic: reader.optional_str()?.map(ToOwned::to_owned),
        group: reader.optional_str()?.map(ToOwned::to_owned),
    };
    reader.finish()?;
    Ok(request)
}

/// Shared body codec for the topology snapshot, used by both the `TopologyOk`
/// response and the unsolicited `TopologyUpdate` push so they carry identical bytes.
fn put_topology_body(out: &mut BytesMut, topology: &TopologyOk) -> WireResult<()> {
    out.put_u64(topology.generation);
    put_len(out, topology.queues.len(), "topology queues")?;
    for entry in &topology.queues {
        put_topology_entry(out, entry)?;
    }
    put_len(out, topology.streams.len(), "topology streams")?;
    for entry in &topology.streams {
        put_str(out, &entry.topic)?;
        put_partition(out, entry.partition);
        put_advertised_addresses(out, &entry.owner_endpoints)?;
        out.put_u64(entry.partitioning_version);
        out.put_u32(entry.partition_count);
    }
    Ok(())
}

fn read_topology_body(reader: &mut Reader) -> WireResult<TopologyOk> {
    let generation = reader.u64()?;
    let count = reader.u32()? as usize;
    let mut queues = Vec::with_capacity(count);
    for _ in 0..count {
        queues.push(reader.topology_entry()?);
    }
    let stream_count = reader.u32()? as usize;
    let mut streams = Vec::with_capacity(stream_count);
    for _ in 0..stream_count {
        let topic = reader.str()?.to_owned();
        let partition = reader.partition()?;
        let owner_endpoints = reader.advertised_addresses()?;
        let partitioning_version = reader.u64()?;
        let partition_count = reader.u32()?;
        streams.push(StreamTopologyEntry {
            topic,
            partition,
            owner_endpoints,
            partitioning_version,
            partition_count,
        });
    }
    Ok(TopologyOk {
        generation,
        queues,
        streams,
    })
}

pub fn encode_topology_ok(request_id: u64, topology: &TopologyOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FTO1");
    put_topology_body(&mut out, topology)?;
    Ok(frame(Op::TopologyOk, request_id, out.freeze()))
}

pub fn decode_topology_ok(frame: &Frame) -> WireResult<TopologyOk> {
    expect_op(frame, Op::TopologyOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FTO1", "topology ok")?;
    let topology = read_topology_body(&mut reader)?;
    reader.finish()?;
    Ok(topology)
}

/// Unsolicited broker->client topology push. Same body as `TopologyOk` under a
/// distinct op + magic so the client can tell a push from its own request reply.
pub fn encode_topology_update(request_id: u64, topology: &TopologyOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FTU1");
    put_topology_body(&mut out, topology)?;
    Ok(frame(Op::TopologyUpdate, request_id, out.freeze()))
}

pub fn decode_topology_update(frame: &Frame) -> WireResult<TopologyOk> {
    expect_op(frame, Op::TopologyUpdate)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FTU1", "topology update")?;
    let topology = read_topology_body(&mut reader)?;
    reader.finish()?;
    Ok(topology)
}

pub fn encode_topology_update_ack(request_id: u64, ack: &TopologyUpdateAck) -> WireResult<Frame> {
    let mut out = payload_builder(b"FTA1");
    out.put_u64(ack.generation);
    Ok(frame(Op::TopologyUpdateAck, request_id, out.freeze()))
}

pub fn decode_topology_update_ack(frame: &Frame) -> WireResult<TopologyUpdateAck> {
    expect_op(frame, Op::TopologyUpdateAck)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FTA1", "topology update ack")?;
    let generation = reader.u64()?;
    reader.finish()?;
    Ok(TopologyUpdateAck { generation })
}

pub fn encode_going_away(request_id: u64, notice: &GoingAway) -> WireResult<Frame> {
    let mut out = payload_builder(b"FGA1");
    out.put_u64(notice.grace_ms);
    put_str(&mut out, &notice.message)?;
    Ok(frame(Op::GoingAway, request_id, out.freeze()))
}

pub fn decode_going_away(frame: &Frame) -> WireResult<GoingAway> {
    expect_op(frame, Op::GoingAway)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FGA1", "going away")?;
    let grace_ms = reader.u64()?;
    let message = reader.str()?.to_owned();
    reader.finish()?;
    Ok(GoingAway { grace_ms, message })
}

pub fn encode_redirect(request_id: u64, redirect: &Redirect) -> WireResult<Frame> {
    let mut out = payload_builder(b"FRD1");
    put_queue_key(
        &mut out,
        &redirect.topic,
        redirect.partition,
        redirect.group.as_deref(),
    )?;
    put_advertised_addresses(&mut out, &redirect.owner_endpoints)?;
    out.put_u64(redirect.partitioning_version);
    Ok(frame(Op::Redirect, request_id, out.freeze()))
}

pub fn decode_redirect(frame: &Frame) -> WireResult<Redirect> {
    expect_op(frame, Op::Redirect)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FRD1", "redirect")?;
    let (topic, partition, group) = reader.queue_key()?;
    let redirect = Redirect {
        topic,
        partition,
        group,
        owner_endpoints: reader.advertised_addresses()?,
        partitioning_version: reader.u64()?,
    };
    reader.finish()?;
    Ok(redirect)
}

pub fn encode_reconcile_client(request_id: u64, reconcile: &ReconcileClient) -> WireResult<Frame> {
    let mut out = payload_builder(b"FRC1");
    put_reconcile_policy(&mut out, reconcile.policy);
    put_reconcile_subscriptions(&mut out, &reconcile.subscriptions)?;
    Ok(frame(Op::ReconcileClient, request_id, out.freeze()))
}

pub fn decode_reconcile_client(frame: &Frame) -> WireResult<ReconcileClient> {
    expect_op(frame, Op::ReconcileClient)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FRC1", "reconcile client")?;
    let reconcile = ReconcileClient {
        policy: reader.reconcile_policy()?,
        subscriptions: reader.reconcile_subscriptions()?,
    };
    reader.finish()?;
    Ok(reconcile)
}

pub fn encode_reconcile_server(request_id: u64, reconcile: &ReconcileServer) -> WireResult<Frame> {
    let mut out = payload_builder(b"FRS1");
    put_reconcile_subscriptions(&mut out, &reconcile.subscriptions)?;
    Ok(frame(Op::ReconcileServer, request_id, out.freeze()))
}

pub fn decode_reconcile_server(frame: &Frame) -> WireResult<ReconcileServer> {
    expect_op(frame, Op::ReconcileServer)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FRS1", "reconcile server")?;
    let reconcile = ReconcileServer {
        subscriptions: reader.reconcile_subscriptions()?,
    };
    reader.finish()?;
    Ok(reconcile)
}

pub fn encode_reconcile_result(request_id: u64, result: &ReconcileResult) -> WireResult<Frame> {
    let mut out = payload_builder(b"FRR1");
    put_len(
        &mut out,
        result.subscriptions.len(),
        "reconcile result subscriptions",
    )?;
    for sub in &result.subscriptions {
        put_optional_reconcile_subscription(&mut out, sub.client.as_ref())?;
        put_optional_reconcile_subscription(&mut out, sub.server.as_ref())?;
        put_reconcile_action(&mut out, sub.action);
        put_str(&mut out, &sub.reason)?;
    }
    Ok(frame(Op::ReconcileResult, request_id, out.freeze()))
}

pub fn decode_reconcile_result(frame: &Frame) -> WireResult<ReconcileResult> {
    expect_op(frame, Op::ReconcileResult)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FRR1", "reconcile result")?;
    let count = reader.u32()? as usize;
    let mut subscriptions = Vec::with_capacity(count);
    for _ in 0..count {
        subscriptions.push(ReconcileSubscriptionResult {
            client: reader.optional_reconcile_subscription()?,
            server: reader.optional_reconcile_subscription()?,
            action: reader.reconcile_action()?,
            reason: reader.str()?.to_owned(),
        });
    }
    reader.finish()?;
    Ok(ReconcileResult { subscriptions })
}

fn put_replication_read_body(out: &mut BytesMut, read: &ReplicationRead) -> WireResult<()> {
    put_queue_key(out, &read.topic, read.partition, read.group.as_deref())?;
    out.put_u64(read.message_from);
    out.put_u64(read.event_from);
    out.put_u32(read.max_messages);
    out.put_u32(read.max_events);
    out.put_u64(read.max_bytes);
    out.put_u32(read.max_wait_ms);
    put_optional_str(out, read.reporter_node_id.as_deref())?;
    Ok(())
}

fn read_replication_read_body(reader: &mut Reader) -> WireResult<ReplicationRead> {
    let (topic, partition, group) = reader.queue_key()?;
    Ok(ReplicationRead {
        topic,
        group,
        partition,
        message_from: reader.u64()?,
        event_from: reader.u64()?,
        max_messages: reader.u32()?,
        max_events: reader.u32()?,
        max_bytes: reader.u64()?,
        max_wait_ms: reader.u32()?,
        reporter_node_id: reader.optional_str()?.map(ToOwned::to_owned),
    })
}

pub fn encode_replication_read(request_id: u64, read: &ReplicationRead) -> WireResult<Frame> {
    let mut out = payload_builder(b"FRQ1");
    put_replication_read_body(&mut out, read)?;
    Ok(frame(Op::ReplicationRead, request_id, out.freeze()))
}

pub fn decode_replication_read(frame: &Frame) -> WireResult<ReplicationRead> {
    expect_op(frame, Op::ReplicationRead)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FRQ1", "replication read")?;
    let read = read_replication_read_body(&mut reader)?;
    reader.finish()?;
    Ok(read)
}

pub fn encode_replication_read_ok(request_id: u64, read: &ReplicationReadOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FRR2");
    put_replication_message_read(&mut out, &read.messages)?;
    put_replication_event_read(&mut out, &read.events)?;
    Ok(frame(Op::ReplicationReadOk, request_id, out.freeze()))
}

pub fn decode_replication_read_ok(frame: &Frame) -> WireResult<ReplicationReadOk> {
    expect_op(frame, Op::ReplicationReadOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FRR2", "replication read ok")?;
    let messages = reader.replication_message_read()?;
    let events = reader.replication_event_read()?;
    reader.finish()?;
    Ok(ReplicationReadOk { messages, events })
}

// Stream (Plexus) follower replication: a distinct op that reuses the queue
// replication read body (records + cursor-commit events). Distinct magics keep
// the frames self-describing in a capture.
pub fn encode_stream_replication_read(
    request_id: u64,
    read: &ReplicationRead,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSQ1");
    put_replication_read_body(&mut out, read)?;
    Ok(frame(Op::StreamReplicationRead, request_id, out.freeze()))
}

pub fn decode_stream_replication_read(frame: &Frame) -> WireResult<ReplicationRead> {
    expect_op(frame, Op::StreamReplicationRead)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSQ1", "stream replication read")?;
    let read = read_replication_read_body(&mut reader)?;
    reader.finish()?;
    Ok(read)
}

pub fn encode_stream_replication_read_ok(
    request_id: u64,
    read: &ReplicationReadOk,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSR1");
    put_replication_message_read(&mut out, &read.messages)?;
    put_replication_event_read(&mut out, &read.events)?;
    Ok(frame(Op::StreamReplicationReadOk, request_id, out.freeze()))
}

pub fn decode_stream_replication_read_ok(frame: &Frame) -> WireResult<ReplicationReadOk> {
    expect_op(frame, Op::StreamReplicationReadOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSR1", "stream replication read ok")?;
    let messages = reader.replication_message_read()?;
    let events = reader.replication_event_read()?;
    reader.finish()?;
    Ok(ReplicationReadOk { messages, events })
}

pub fn encode_replication_stream_start(
    stream_id: u64,
    start: &ReplicationStreamStart,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSS1");
    put_queue_key(
        &mut out,
        &start.topic,
        start.partition,
        start.group.as_deref(),
    )?;
    out.put_u64(start.message_from);
    out.put_u64(start.event_from);
    out.put_u64(start.credit_bytes);
    put_optional_str(&mut out, start.reporter_node_id.as_deref())?;
    Ok(frame(Op::ReplicationStreamStart, stream_id, out.freeze()))
}

pub fn decode_replication_stream_start(frame: &Frame) -> WireResult<ReplicationStreamStart> {
    expect_op(frame, Op::ReplicationStreamStart)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSS1", "replication stream start")?;
    let (topic, partition, group) = reader.queue_key()?;
    let start = ReplicationStreamStart {
        topic,
        group,
        partition,
        message_from: reader.u64()?,
        event_from: reader.u64()?,
        credit_bytes: reader.u64()?,
        reporter_node_id: reader.optional_str()?.map(ToOwned::to_owned),
    };
    reader.finish()?;
    Ok(start)
}

/// A streamed record batch reuses the `ReplicationReadOk` body verbatim; only the
/// opcode (and the `request_id` as stream id) differ from the pull response.
pub fn encode_replication_stream_batch(
    stream_id: u64,
    batch: &ReplicationReadOk,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSB1");
    put_replication_message_read(&mut out, &batch.messages)?;
    put_replication_event_read(&mut out, &batch.events)?;
    Ok(frame(Op::ReplicationStreamBatch, stream_id, out.freeze()))
}

pub fn decode_replication_stream_batch(frame: &Frame) -> WireResult<ReplicationReadOk> {
    expect_op(frame, Op::ReplicationStreamBatch)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSB1", "replication stream batch")?;
    let messages = reader.replication_message_read()?;
    let events = reader.replication_event_read()?;
    reader.finish()?;
    Ok(ReplicationReadOk { messages, events })
}

pub fn encode_replication_stream_progress(
    stream_id: u64,
    progress: &ReplicationStreamProgress,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSP1");
    out.put_u64(progress.durable_message_next);
    out.put_u64(progress.durable_event_next);
    out.put_u64(progress.credit_add_bytes);
    Ok(frame(
        Op::ReplicationStreamProgress,
        stream_id,
        out.freeze(),
    ))
}

pub fn decode_replication_stream_progress(frame: &Frame) -> WireResult<ReplicationStreamProgress> {
    expect_op(frame, Op::ReplicationStreamProgress)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSP1", "replication stream progress")?;
    let progress = ReplicationStreamProgress {
        durable_message_next: reader.u64()?,
        durable_event_next: reader.u64()?,
        credit_add_bytes: reader.u64()?,
    };
    reader.finish()?;
    Ok(progress)
}

pub fn encode_replication_stream_reset(
    stream_id: u64,
    reset: &ReplicationStreamReset,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSR1");
    out.put_u64(reset.message_from);
    out.put_u64(reset.event_from);
    Ok(frame(Op::ReplicationStreamReset, stream_id, out.freeze()))
}

pub fn decode_replication_stream_reset(frame: &Frame) -> WireResult<ReplicationStreamReset> {
    expect_op(frame, Op::ReplicationStreamReset)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSR1", "replication stream reset")?;
    let reset = ReplicationStreamReset {
        message_from: reader.u64()?,
        event_from: reader.u64()?,
    };
    reader.finish()?;
    Ok(reset)
}

pub fn encode_replication_stream_end(
    stream_id: u64,
    end: &ReplicationStreamEnd,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FSE1");
    out.put_u16(end.code);
    put_str(&mut out, &end.message)?;
    Ok(frame(Op::ReplicationStreamEnd, stream_id, out.freeze()))
}

pub fn decode_replication_stream_end(frame: &Frame) -> WireResult<ReplicationStreamEnd> {
    expect_op(frame, Op::ReplicationStreamEnd)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FSE1", "replication stream end")?;
    let end = ReplicationStreamEnd {
        code: reader.u16()?,
        message: reader.str()?.to_owned(),
    };
    reader.finish()?;
    Ok(end)
}

pub fn encode_replication_apply(request_id: u64, apply: &ReplicationApply) -> WireResult<Frame> {
    let mut out = payload_builder(b"FRA1");
    put_queue_key(
        &mut out,
        &apply.topic,
        apply.partition,
        apply.group.as_deref(),
    )?;
    put_optional_replication_message_apply_batch(&mut out, apply.messages.as_ref())?;
    put_optional_replication_event_apply_batch(&mut out, apply.events.as_ref())?;
    Ok(frame(Op::ReplicationApply, request_id, out.freeze()))
}

pub fn decode_replication_apply(frame: &Frame) -> WireResult<ReplicationApply> {
    expect_op(frame, Op::ReplicationApply)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FRA1", "replication apply")?;
    let (topic, partition, group) = reader.queue_key()?;
    let apply = ReplicationApply {
        topic,
        group,
        partition,
        messages: reader.optional_replication_message_apply_batch()?,
        events: reader.optional_replication_event_apply_batch()?,
    };
    reader.finish()?;
    Ok(apply)
}

pub fn encode_replication_apply_ok(request_id: u64, ok: &ReplicationApplyOk) -> WireResult<Frame> {
    let mut out = payload_builder(b"FAO1");
    put_bool(&mut out, ok.messages_applied);
    put_bool(&mut out, ok.events_applied);
    Ok(frame(Op::ReplicationApplyOk, request_id, out.freeze()))
}

pub fn decode_replication_apply_ok(frame: &Frame) -> WireResult<ReplicationApplyOk> {
    expect_op(frame, Op::ReplicationApplyOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FAO1", "replication apply ok")?;
    let ok = ReplicationApplyOk {
        messages_applied: reader.bool()?,
        events_applied: reader.bool()?,
    };
    reader.finish()?;
    Ok(ok)
}

pub fn encode_replication_checkpoint_export(
    request_id: u64,
    export: &ReplicationCheckpointExport,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FCE1");
    put_queue_key(
        &mut out,
        &export.topic,
        export.partition,
        export.group.as_deref(),
    )?;
    Ok(frame(
        Op::ReplicationCheckpointExport,
        request_id,
        out.freeze(),
    ))
}

pub fn decode_replication_checkpoint_export(
    frame: &Frame,
) -> WireResult<ReplicationCheckpointExport> {
    expect_op(frame, Op::ReplicationCheckpointExport)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FCE1", "replication checkpoint export")?;
    let (topic, partition, group) = reader.queue_key()?;
    reader.finish()?;
    Ok(ReplicationCheckpointExport {
        topic,
        group,
        partition,
    })
}

pub fn encode_replication_checkpoint_export_ok(
    request_id: u64,
    ok: &ReplicationCheckpointExportOk,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FCO1");
    put_replication_state_checkpoint(&mut out, &ok.checkpoint)?;
    Ok(frame(
        Op::ReplicationCheckpointExportOk,
        request_id,
        out.freeze(),
    ))
}

pub fn decode_replication_checkpoint_export_ok(
    frame: &Frame,
) -> WireResult<ReplicationCheckpointExportOk> {
    expect_op(frame, Op::ReplicationCheckpointExportOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FCO1", "replication checkpoint export ok")?;
    let checkpoint = reader.replication_state_checkpoint()?;
    reader.finish()?;
    Ok(ReplicationCheckpointExportOk { checkpoint })
}

pub fn encode_replication_checkpoint_install(
    request_id: u64,
    install: &ReplicationCheckpointInstall,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FCI1");
    put_queue_key(
        &mut out,
        &install.topic,
        install.partition,
        install.group.as_deref(),
    )?;
    put_replication_state_checkpoint(&mut out, &install.checkpoint)?;
    Ok(frame(
        Op::ReplicationCheckpointInstall,
        request_id,
        out.freeze(),
    ))
}

pub fn decode_replication_checkpoint_install(
    frame: &Frame,
) -> WireResult<ReplicationCheckpointInstall> {
    expect_op(frame, Op::ReplicationCheckpointInstall)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FCI1", "replication checkpoint install")?;
    let (topic, partition, group) = reader.queue_key()?;
    let checkpoint = reader.replication_state_checkpoint()?;
    reader.finish()?;
    Ok(ReplicationCheckpointInstall {
        topic,
        group,
        partition,
        checkpoint,
    })
}

pub fn encode_replication_checkpoint_install_ok(
    request_id: u64,
    ok: &ReplicationCheckpointInstallOk,
) -> WireResult<Frame> {
    let mut out = payload_builder(b"FCK1");
    out.put_u64(ok.message_next_offset);
    out.put_u64(ok.event_next_offset);
    out.put_u64(ok.applied_event_offset);
    Ok(frame(
        Op::ReplicationCheckpointInstallOk,
        request_id,
        out.freeze(),
    ))
}

pub fn decode_replication_checkpoint_install_ok(
    frame: &Frame,
) -> WireResult<ReplicationCheckpointInstallOk> {
    expect_op(frame, Op::ReplicationCheckpointInstallOk)?;
    let mut reader = Reader::new(&frame.payload);
    reader.expect_magic(b"FCK1", "replication checkpoint install ok")?;
    let ok = ReplicationCheckpointInstallOk {
        message_next_offset: reader.u64()?,
        event_next_offset: reader.u64()?,
        applied_event_offset: reader.u64()?,
    };
    reader.finish()?;
    Ok(ok)
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
    // Trailing so a peer that does not send it still decodes (read as None).
    put_optional_u64(out, publish.ttl_ms);
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
    // Trailing optional: absent when a peer has not been updated to send it.
    let ttl_ms = if reader.remaining() > 0 {
        reader.optional_u64()?
    } else {
        None
    };

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
        ttl_ms,
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

fn expect_error_op(opcode: u16) -> WireResult<()> {
    match opcode {
        x if x == Op::Error as u16
            || x == Op::HelloErr as u16
            || x == Op::AuthErr as u16
            || x == Op::SubscribeErr as u16 =>
        {
            Ok(())
        }
        actual => Err(WireError::UnexpectedOpcode {
            expected: Op::Error as u16,
            actual,
        }),
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

fn put_uuid(out: &mut BytesMut, value: Uuid) {
    out.extend_from_slice(value.as_bytes());
}

fn put_optional_uuid(out: &mut BytesMut, value: Option<Uuid>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            put_uuid(out, value);
        }
        None => out.put_u8(0),
    }
}

fn put_partition(out: &mut BytesMut, partition: Partition) {
    out.put_u32(partition.id());
}

fn put_queue_key(
    out: &mut BytesMut,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
) -> WireResult<()> {
    put_str(out, topic)?;
    put_partition(out, partition);
    put_optional_str(out, group)?;
    Ok(())
}

fn put_resume_identity(out: &mut BytesMut, resume: &ResumeIdentity) {
    put_uuid(out, resume.owner_id);
    put_uuid(out, resume.client_id);
    put_uuid(out, resume.resume_token);
}

fn put_optional_resume_identity(out: &mut BytesMut, resume: Option<&ResumeIdentity>) {
    match resume {
        Some(resume) => {
            out.put_u8(1);
            put_resume_identity(out, resume);
        }
        None => out.put_u8(0),
    }
}

fn put_resume_outcome(out: &mut BytesMut, outcome: ResumeOutcome) {
    out.put_u8(match outcome {
        ResumeOutcome::New => 0,
        ResumeOutcome::Resumed => 1,
        ResumeOutcome::ResumeNotFound => 2,
        ResumeOutcome::ResumeRejected => 3,
    });
}

fn put_dlq_policy(out: &mut BytesMut, policy: &QueueDlqPolicy) -> WireResult<()> {
    match policy {
        QueueDlqPolicy::Discard => out.put_u8(0),
        QueueDlqPolicy::Global => out.put_u8(1),
        QueueDlqPolicy::Custom { topic, group } => {
            out.put_u8(2);
            put_str(out, topic)?;
            put_optional_str(out, group.as_deref())?;
        }
    }
    Ok(())
}

fn put_optional_dlq_policy(out: &mut BytesMut, policy: Option<&QueueDlqPolicy>) -> WireResult<()> {
    match policy {
        Some(policy) => {
            out.put_u8(1);
            put_dlq_policy(out, policy)
        }
        None => {
            out.put_u8(0);
            Ok(())
        }
    }
}

fn put_reconcile_policy(out: &mut BytesMut, policy: ReconcilePolicy) {
    out.put_u8(match policy {
        ReconcilePolicy::Conservative => 0,
        ReconcilePolicy::Restore => 1,
    });
}

fn put_reconcile_action(out: &mut BytesMut, action: ReconcileAction) {
    out.put_u8(match action {
        ReconcileAction::Keep => 0,
        ReconcileAction::CloseClientSide => 1,
        ReconcileAction::CloseServerSide => 2,
        ReconcileAction::RecreateClientSide => 3,
    });
}

fn put_reconcile_subscription(out: &mut BytesMut, sub: &ReconcileSubscription) -> WireResult<()> {
    out.put_u64(sub.sub_id);
    put_queue_key(out, &sub.topic, sub.partition, sub.group.as_deref())?;
    put_bool(out, sub.auto_ack);
    out.put_u32(sub.prefetch);
    put_optional_str(out, sub.consumer_group.as_deref())?;
    put_optional_u32(out, sub.consumer_target);
    put_optional_uuid(out, sub.member_id);
    Ok(())
}

fn put_optional_reconcile_subscription(
    out: &mut BytesMut,
    sub: Option<&ReconcileSubscription>,
) -> WireResult<()> {
    match sub {
        Some(sub) => {
            out.put_u8(1);
            put_reconcile_subscription(out, sub)
        }
        None => {
            out.put_u8(0);
            Ok(())
        }
    }
}

fn put_reconcile_subscriptions(
    out: &mut BytesMut,
    subs: &[ReconcileSubscription],
) -> WireResult<()> {
    put_len(out, subs.len(), "reconcile subscriptions")?;
    for sub in subs {
        put_reconcile_subscription(out, sub)?;
    }
    Ok(())
}

/// Owner endpoints: a length-prefixed list of `host:port` plus a tag list each,
/// in priority order. Clients try them in order and use the first that connects.
fn put_advertised_addresses(out: &mut BytesMut, addrs: &[AdvertisedAddress]) -> WireResult<()> {
    put_len(out, addrs.len(), "owner endpoints")?;
    for addr in addrs {
        put_str(out, &addr.host)?;
        out.put_u16(addr.port);
        put_len(out, addr.tags.len(), "endpoint tags")?;
        for tag in &addr.tags {
            put_str(out, tag)?;
        }
    }
    Ok(())
}

fn put_topology_entry(out: &mut BytesMut, entry: &QueueTopologyEntry) -> WireResult<()> {
    put_queue_key(out, &entry.topic, entry.partition, entry.group.as_deref())?;
    put_advertised_addresses(out, &entry.owner_endpoints)?;
    out.put_u64(entry.partitioning_version);
    out.put_u32(entry.partition_count);
    Ok(())
}

fn put_replication_checkpoint_required(
    out: &mut BytesMut,
    required: &ReplicationCheckpointRequired,
) {
    out.put_u64(required.epoch);
    out.put_u64(required.requested_offset);
    out.put_u64(required.head_offset);
    out.put_u64(required.next_offset);
}

fn put_replication_message_record(
    out: &mut BytesMut,
    record: &ReplicationMessageRecord,
) -> WireResult<()> {
    out.put_u64(record.offset);
    out.put_u16(record.flags);
    put_bytes(out, &record.headers)?;
    put_bytes(out, &record.payload)?;
    Ok(())
}

fn put_replication_event_record(
    out: &mut BytesMut,
    record: &ReplicationEventRecord,
) -> WireResult<()> {
    out.put_u64(record.offset);
    put_bytes(out, &record.payload)?;
    Ok(())
}

fn put_replication_message_records(
    out: &mut BytesMut,
    records: &[ReplicationMessageRecord],
) -> WireResult<()> {
    put_len(out, records.len(), "replication message records")?;
    for record in records {
        put_replication_message_record(out, record)?;
    }
    Ok(())
}

fn put_replication_event_records(
    out: &mut BytesMut,
    records: &[ReplicationEventRecord],
) -> WireResult<()> {
    put_len(out, records.len(), "replication event records")?;
    for record in records {
        put_replication_event_record(out, record)?;
    }
    Ok(())
}

fn put_replication_message_read(
    out: &mut BytesMut,
    read: &ReplicationMessageRead,
) -> WireResult<()> {
    match read {
        ReplicationMessageRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            out.put_u8(0);
            out.put_u64(*epoch);
            out.put_u64(*requested_offset);
            out.put_u64(*next_offset);
            put_replication_message_records(out, records)?;
        }
        ReplicationMessageRead::CheckpointRequired(required) => {
            out.put_u8(1);
            put_replication_checkpoint_required(out, required);
        }
    }
    Ok(())
}

fn put_replication_event_read(out: &mut BytesMut, read: &ReplicationEventRead) -> WireResult<()> {
    match read {
        ReplicationEventRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            out.put_u8(0);
            out.put_u64(*epoch);
            out.put_u64(*requested_offset);
            out.put_u64(*next_offset);
            put_replication_event_records(out, records)?;
        }
        ReplicationEventRead::CheckpointRequired(required) => {
            out.put_u8(1);
            put_replication_checkpoint_required(out, required);
        }
    }
    Ok(())
}

fn put_optional_replication_message_apply_batch(
    out: &mut BytesMut,
    batch: Option<&ReplicationMessageApplyBatch>,
) -> WireResult<()> {
    match batch {
        Some(batch) => {
            out.put_u8(1);
            out.put_u64(batch.epoch);
            put_replication_message_records(out, &batch.records)?;
        }
        None => out.put_u8(0),
    }
    Ok(())
}

fn put_optional_replication_event_apply_batch(
    out: &mut BytesMut,
    batch: Option<&ReplicationEventApplyBatch>,
) -> WireResult<()> {
    match batch {
        Some(batch) => {
            out.put_u8(1);
            out.put_u64(batch.epoch);
            put_replication_event_records(out, &batch.records)?;
        }
        None => out.put_u8(0),
    }
    Ok(())
}

fn put_replication_state_checkpoint(
    out: &mut BytesMut,
    checkpoint: &ReplicationStateCheckpoint,
) -> WireResult<()> {
    out.put_u64(checkpoint.message_epoch);
    out.put_u64(checkpoint.event_epoch);
    out.put_u64(checkpoint.message_checkpoint_offset);
    out.put_u64(checkpoint.message_next_offset);
    out.put_u64(checkpoint.event_next_offset);
    out.put_u64(checkpoint.applied_event_offset);
    put_bytes(out, &checkpoint.state_snapshot)?;
    Ok(())
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

fn put_optional_u32(out: &mut BytesMut, value: Option<u32>) {
    match value {
        Some(value) => {
            out.put_u8(1);
            out.put_u32(value);
        }
        None => out.put_u8(0),
    }
}

fn put_partitions(out: &mut BytesMut, partitions: &[Partition]) -> WireResult<()> {
    put_len(out, partitions.len(), "partitions")?;
    for partition in partitions {
        put_partition(out, *partition);
    }
    Ok(())
}

fn put_stream_retention(out: &mut BytesMut, retention: &StreamRetention) {
    put_optional_u64(out, retention.max_age_ms);
    put_optional_u64(out, retention.max_bytes);
    put_optional_u64(out, retention.max_records);
}

fn put_stream_start(out: &mut BytesMut, start: StreamStart) {
    match start {
        StreamStart::Latest => out.put_u8(0),
        StreamStart::Earliest => out.put_u8(1),
        StreamStart::Offset { offset } => {
            out.put_u8(2);
            out.put_u64(offset);
        }
        StreamStart::NBack { count } => {
            out.put_u8(3);
            out.put_u64(count);
        }
        StreamStart::ByTime { time_ms } => {
            out.put_u8(4);
            out.put_u64(time_ms);
        }
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

    fn remaining(&self) -> usize {
        self.input.len().saturating_sub(self.cursor)
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

    fn u16(&mut self) -> WireResult<u16> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| WireError::UnexpectedEof)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> WireResult<u64> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| WireError::UnexpectedEof)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn uuid(&mut self) -> WireResult<Uuid> {
        Uuid::from_slice(self.take(16)?).map_err(|_| WireError::InvalidUuid)
    }

    fn optional_uuid(&mut self) -> WireResult<Option<Uuid>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.uuid().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn partition(&mut self) -> WireResult<Partition> {
        self.u32().map(Partition::new)
    }

    fn queue_key(&mut self) -> WireResult<(String, Partition, Option<String>)> {
        let topic = self.str()?.to_owned();
        let partition = self.partition()?;
        let group = self.optional_str()?.map(ToOwned::to_owned);
        Ok((topic, partition, group))
    }

    fn resume_identity(&mut self) -> WireResult<ResumeIdentity> {
        Ok(ResumeIdentity {
            owner_id: self.uuid()?,
            client_id: self.uuid()?,
            resume_token: self.uuid()?,
        })
    }

    fn optional_resume_identity(&mut self) -> WireResult<Option<ResumeIdentity>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.resume_identity().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn resume_outcome(&mut self) -> WireResult<ResumeOutcome> {
        match self.u8()? {
            0 => Ok(ResumeOutcome::New),
            1 => Ok(ResumeOutcome::Resumed),
            2 => Ok(ResumeOutcome::ResumeNotFound),
            3 => Ok(ResumeOutcome::ResumeRejected),
            value => Err(WireError::UnknownTag {
                context: "resume outcome",
                value,
            }),
        }
    }

    fn dlq_policy(&mut self) -> WireResult<QueueDlqPolicy> {
        match self.u8()? {
            0 => Ok(QueueDlqPolicy::Discard),
            1 => Ok(QueueDlqPolicy::Global),
            2 => Ok(QueueDlqPolicy::Custom {
                topic: self.str()?.to_owned(),
                group: self.optional_str()?.map(ToOwned::to_owned),
            }),
            value => Err(WireError::UnknownTag {
                context: "dlq policy",
                value,
            }),
        }
    }

    fn optional_dlq_policy(&mut self) -> WireResult<Option<QueueDlqPolicy>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.dlq_policy().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn reconcile_policy(&mut self) -> WireResult<ReconcilePolicy> {
        match self.u8()? {
            0 => Ok(ReconcilePolicy::Conservative),
            1 => Ok(ReconcilePolicy::Restore),
            value => Err(WireError::UnknownTag {
                context: "reconcile policy",
                value,
            }),
        }
    }

    fn reconcile_action(&mut self) -> WireResult<ReconcileAction> {
        match self.u8()? {
            0 => Ok(ReconcileAction::Keep),
            1 => Ok(ReconcileAction::CloseClientSide),
            2 => Ok(ReconcileAction::CloseServerSide),
            3 => Ok(ReconcileAction::RecreateClientSide),
            value => Err(WireError::UnknownTag {
                context: "reconcile action",
                value,
            }),
        }
    }

    fn reconcile_subscription(&mut self) -> WireResult<ReconcileSubscription> {
        let sub_id = self.u64()?;
        let (topic, partition, group) = self.queue_key()?;
        Ok(ReconcileSubscription {
            sub_id,
            topic,
            group,
            partition,
            auto_ack: self.bool()?,
            prefetch: self.u32()?,
            consumer_group: self.optional_str()?.map(ToOwned::to_owned),
            consumer_target: self.optional_u32()?,
            member_id: self.optional_uuid()?,
        })
    }

    fn optional_reconcile_subscription(&mut self) -> WireResult<Option<ReconcileSubscription>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.reconcile_subscription().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn reconcile_subscriptions(&mut self) -> WireResult<Vec<ReconcileSubscription>> {
        let count = self.u32()? as usize;
        let mut subs = Vec::with_capacity(count);
        for _ in 0..count {
            subs.push(self.reconcile_subscription()?);
        }
        Ok(subs)
    }

    fn advertised_addresses(&mut self) -> WireResult<Vec<AdvertisedAddress>> {
        let count = self.u32()? as usize;
        let mut addrs = Vec::with_capacity(count);
        for _ in 0..count {
            let host = self.str()?.to_owned();
            let port = self.u16()?;
            let tag_count = self.u32()? as usize;
            let mut tags = Vec::with_capacity(tag_count);
            for _ in 0..tag_count {
                tags.push(self.str()?.to_owned());
            }
            addrs.push(AdvertisedAddress { host, port, tags });
        }
        Ok(addrs)
    }

    fn topology_entry(&mut self) -> WireResult<QueueTopologyEntry> {
        let (topic, partition, group) = self.queue_key()?;
        Ok(QueueTopologyEntry {
            topic,
            partition,
            group,
            owner_endpoints: self.advertised_addresses()?,
            partitioning_version: self.u64()?,
            partition_count: self.u32()?,
        })
    }

    fn replication_checkpoint_required(&mut self) -> WireResult<ReplicationCheckpointRequired> {
        Ok(ReplicationCheckpointRequired {
            epoch: self.u64()?,
            requested_offset: self.u64()?,
            head_offset: self.u64()?,
            next_offset: self.u64()?,
        })
    }

    fn replication_message_record(&mut self) -> WireResult<ReplicationMessageRecord> {
        Ok(ReplicationMessageRecord {
            offset: self.u64()?,
            flags: self.u16()?,
            headers: self.bytes()?.to_vec(),
            payload: self.bytes()?.to_vec(),
        })
    }

    fn replication_event_record(&mut self) -> WireResult<ReplicationEventRecord> {
        Ok(ReplicationEventRecord {
            offset: self.u64()?,
            payload: self.bytes()?.to_vec(),
        })
    }

    fn replication_message_records(&mut self) -> WireResult<Vec<ReplicationMessageRecord>> {
        let count = self.u32()? as usize;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(self.replication_message_record()?);
        }
        Ok(records)
    }

    fn replication_event_records(&mut self) -> WireResult<Vec<ReplicationEventRecord>> {
        let count = self.u32()? as usize;
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            records.push(self.replication_event_record()?);
        }
        Ok(records)
    }

    fn replication_message_read(&mut self) -> WireResult<ReplicationMessageRead> {
        match self.u8()? {
            0 => {
                let epoch = self.u64()?;
                let requested_offset = self.u64()?;
                let next_offset = self.u64()?;
                let records = self.replication_message_records()?;
                validate_message_record_offsets(requested_offset, next_offset, &records)?;
                Ok(ReplicationMessageRead::Batch {
                    epoch,
                    requested_offset,
                    next_offset,
                    records,
                })
            }
            1 => Ok(ReplicationMessageRead::CheckpointRequired(
                self.replication_checkpoint_required()?,
            )),
            value => Err(WireError::UnknownTag {
                context: "replication message read",
                value,
            }),
        }
    }

    fn replication_event_read(&mut self) -> WireResult<ReplicationEventRead> {
        match self.u8()? {
            0 => {
                let epoch = self.u64()?;
                let requested_offset = self.u64()?;
                let next_offset = self.u64()?;
                let records = self.replication_event_records()?;
                validate_event_record_offsets(requested_offset, next_offset, &records)?;
                Ok(ReplicationEventRead::Batch {
                    epoch,
                    requested_offset,
                    next_offset,
                    records,
                })
            }
            1 => Ok(ReplicationEventRead::CheckpointRequired(
                self.replication_checkpoint_required()?,
            )),
            value => Err(WireError::UnknownTag {
                context: "replication event read",
                value,
            }),
        }
    }

    fn optional_replication_message_apply_batch(
        &mut self,
    ) -> WireResult<Option<ReplicationMessageApplyBatch>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(ReplicationMessageApplyBatch {
                epoch: self.u64()?,
                records: self.replication_message_records()?,
            })),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn optional_replication_event_apply_batch(
        &mut self,
    ) -> WireResult<Option<ReplicationEventApplyBatch>> {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(ReplicationEventApplyBatch {
                epoch: self.u64()?,
                records: self.replication_event_records()?,
            })),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn replication_state_checkpoint(&mut self) -> WireResult<ReplicationStateCheckpoint> {
        Ok(ReplicationStateCheckpoint {
            message_epoch: self.u64()?,
            event_epoch: self.u64()?,
            message_checkpoint_offset: self.u64()?,
            message_next_offset: self.u64()?,
            event_next_offset: self.u64()?,
            applied_event_offset: self.u64()?,
            state_snapshot: self.bytes()?.to_vec(),
        })
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

    fn stream_durability(&mut self) -> WireResult<StreamDurability> {
        let value = self.u8()?;
        StreamDurability::from_u8(value).ok_or(WireError::UnknownTag {
            context: "stream durability",
            value,
        })
    }

    fn stream_retention(&mut self) -> WireResult<StreamRetention> {
        Ok(StreamRetention {
            max_age_ms: self.optional_u64()?,
            max_bytes: self.optional_u64()?,
            max_records: self.optional_u64()?,
        })
    }

    fn stream_start(&mut self) -> WireResult<StreamStart> {
        match self.u8()? {
            0 => Ok(StreamStart::Latest),
            1 => Ok(StreamStart::Earliest),
            2 => Ok(StreamStart::Offset {
                offset: self.u64()?,
            }),
            3 => Ok(StreamStart::NBack { count: self.u64()? }),
            4 => Ok(StreamStart::ByTime {
                time_ms: self.u64()?,
            }),
            value => Err(WireError::UnknownTag {
                context: "stream start",
                value,
            }),
        }
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

    fn optional_u32(&mut self) -> WireResult<Option<u32>> {
        match self.u8()? {
            0 => Ok(None),
            1 => self.u32().map(Some),
            value => Err(WireError::InvalidOption(value)),
        }
    }

    fn partitions(&mut self) -> WireResult<Vec<Partition>> {
        let count = self.u32()? as usize;
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            partitions.push(self.partition()?);
        }
        Ok(partitions)
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

fn validate_message_record_offsets(
    requested_offset: u64,
    next_offset: u64,
    records: &[ReplicationMessageRecord],
) -> WireResult<()> {
    validate_record_offsets(
        requested_offset,
        next_offset,
        records.iter().map(|record| record.offset),
    )
}

fn validate_event_record_offsets(
    requested_offset: u64,
    next_offset: u64,
    records: &[ReplicationEventRecord],
) -> WireResult<()> {
    validate_record_offsets(
        requested_offset,
        next_offset,
        records.iter().map(|record| record.offset),
    )
}

fn validate_record_offsets(
    requested_offset: u64,
    next_offset: u64,
    offsets: impl IntoIterator<Item = u64>,
) -> WireResult<()> {
    let mut previous: Option<u64> = None;
    for offset in offsets {
        if offset < requested_offset {
            return Err(WireError::InvalidRecordSequence("offset before requested"));
        }

        if let Some(previous) = previous {
            let expected = previous
                .checked_add(1)
                .ok_or(WireError::InvalidRecordSequence("offset overflow"))?;
            if offset != expected {
                return Err(WireError::InvalidRecordSequence("non-contiguous offset"));
            }
        }

        previous = Some(offset);
    }

    let minimum_next = previous
        .map(|offset| {
            offset
                .checked_add(1)
                .ok_or(WireError::InvalidRecordSequence("next offset overflow"))
        })
        .transpose()?
        .unwrap_or(requested_offset);
    if next_offset < minimum_next {
        return Err(WireError::InvalidRecordSequence(
            "next offset before minimum",
        ));
    }

    Ok(())
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

    fn sample_replication_read_ok() -> ReplicationReadOk {
        ReplicationReadOk {
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
        }
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
        assert_eq!(got.ttl_ms, expected.ttl_ms);
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
            ttl_ms: None,
        };

        let frame = encode_publish(42, &publish).unwrap();
        assert_eq!(frame.opcode, Op::Publish as u16);
        assert_eq!(frame.request_id, 42);
        assert_publish_eq(&decode_publish(&frame).unwrap(), &publish);
    }

    #[test]
    fn publish_roundtrips_with_ttl() {
        let publish = Publish {
            topic: "orders".into(),
            partition: Partition::new(0),
            group: None,
            require_confirm: false,
            content_type: None,
            headers: headers(),
            payload: vec![9, 8, 7],
            published: 1,
            partition_key: None,
            partitioning_version: 0,
            ttl_ms: Some(30_000),
        };

        let frame = encode_publish(1, &publish).unwrap();
        let got = decode_publish(&frame).unwrap();
        assert_eq!(got.ttl_ms, Some(30_000));
        assert_publish_eq(&got, &publish);
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
    fn replication_read_ok_roundtrips_batches() {
        let msg = sample_replication_read_ok();
        let frame = encode_replication_read_ok(14, &msg).unwrap();

        assert_eq!(frame.version, PROTOCOL_V1);
        assert_eq!(frame.opcode, Op::ReplicationReadOk as u16);
        assert_eq!(frame.request_id, 14);
        assert_eq!(decode_replication_read_ok(&frame).unwrap(), msg);
    }

    #[test]
    fn stream_replication_read_roundtrips() {
        let msg = ReplicationRead {
            topic: "events".into(),
            group: None,
            partition: Partition::new(2),
            message_from: 10,
            event_from: 4,
            max_messages: 100,
            max_events: 50,
            max_bytes: 1 << 20,
            max_wait_ms: 250,
            reporter_node_id: Some("node-b".into()),
        };
        let frame = encode_stream_replication_read(7, &msg).unwrap();
        assert_eq!(frame.opcode, Op::StreamReplicationRead as u16);
        assert_eq!(decode_stream_replication_read(&frame).unwrap(), msg);
    }

    #[test]
    fn stream_replication_read_ok_roundtrips() {
        let msg = sample_replication_read_ok();
        let frame = encode_stream_replication_read_ok(8, &msg).unwrap();
        assert_eq!(frame.opcode, Op::StreamReplicationReadOk as u16);
        assert_eq!(decode_stream_replication_read_ok(&frame).unwrap(), msg);
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

        let frame = encode_replication_read_ok(15, &msg).unwrap();
        assert_eq!(decode_replication_read_ok(&frame).unwrap(), msg);
    }

    #[test]
    fn replication_read_ok_rejects_truncated_payload() {
        let frame = Frame {
            version: PROTOCOL_V1,
            opcode: Op::ReplicationReadOk as u16,
            flags: 0,
            request_id: 9,
            payload: Bytes::from_static(b"FRR2\0"),
        };

        assert!(matches!(
            decode_replication_read_ok(&frame),
            Err(WireError::UnexpectedEof)
        ));
    }

    #[test]
    fn replication_read_ok_rejects_offset_before_request() {
        let mut msg = sample_replication_read_ok();
        if let ReplicationMessageRead::Batch {
            ref mut records, ..
        } = msg.messages
        {
            records[0].offset = 9;
        }

        let frame = encode_replication_read_ok(14, &msg).unwrap();
        assert!(matches!(
            decode_replication_read_ok(&frame),
            Err(WireError::InvalidRecordSequence(_))
        ));
    }

    #[test]
    fn replication_read_ok_rejects_non_contiguous_records() {
        let mut msg = sample_replication_read_ok();
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
            Err(WireError::InvalidRecordSequence(_))
        ));
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
    fn declare_plexus_roundtrip() {
        for declare in [
            DeclarePlexus {
                topic: "events".into(),
                partition_count: Some(8),
                durability: StreamDurability::Speculative,
                retention: StreamRetention {
                    max_age_ms: Some(60_000),
                    max_bytes: None,
                    max_records: Some(1_000_000),
                },
                replication_factor: Some(2),
            },
            DeclarePlexus {
                topic: "minimal".into(),
                partition_count: None,
                durability: StreamDurability::Durable,
                retention: StreamRetention::default(),
                replication_factor: None,
            },
        ] {
            let frame = encode_declare_plexus(7, &declare).unwrap();
            let got = decode_declare_plexus(&frame).unwrap();
            assert_eq!(got.topic, declare.topic);
            assert_eq!(got.partition_count, declare.partition_count);
            assert_eq!(got.durability, declare.durability);
            assert_eq!(got.retention, declare.retention);
        }

        let ok = DeclarePlexusOk {
            status: "stored".into(),
            partition_count: 8,
        };
        let frame = encode_declare_plexus_ok(7, &ok).unwrap();
        let got = decode_declare_plexus_ok(&frame).unwrap();
        assert_eq!(got.status, ok.status);
        assert_eq!(got.partition_count, ok.partition_count);
    }

    #[test]
    fn topology_update_and_ack_roundtrip() {
        // The push reuses the TopologyOk body under its own op + magic.
        let topology = TopologyOk {
            generation: 42,
            queues: vec![QueueTopologyEntry {
                topic: "t".into(),
                group: None,
                partition: Partition::new(0),
                owner_endpoints: vec![AdvertisedAddress {
                    host: "127.0.0.1".into(),
                    port: 7000,
                    tags: vec![],
                }],
                partitioning_version: 1,
                partition_count: 2,
            }],
            streams: vec![StreamTopologyEntry {
                topic: "s".into(),
                partition: Partition::new(1),
                owner_endpoints: vec![],
                partitioning_version: 3,
                partition_count: 4,
            }],
        };
        let frame = encode_topology_update(9, &topology).unwrap();
        assert_eq!(frame.opcode, Op::TopologyUpdate as u16);
        assert_eq!(decode_topology_update(&frame).unwrap(), topology);

        let ack = TopologyUpdateAck { generation: 42 };
        let frame = encode_topology_update_ack(9, &ack).unwrap();
        assert_eq!(frame.opcode, Op::TopologyUpdateAck as u16);
        assert_eq!(decode_topology_update_ack(&frame).unwrap(), ack);
    }

    #[test]
    fn going_away_roundtrip() {
        let notice = GoingAway {
            grace_ms: 30_000,
            message: "broker restarting for upgrade".into(),
        };
        let frame = encode_going_away(7, &notice).unwrap();
        assert_eq!(frame.opcode, Op::GoingAway as u16);
        assert_eq!(decode_going_away(&frame).unwrap(), notice);
    }

    #[test]
    fn subscribe_stream_roundtrip() {
        for sub in [
            SubscribeStream {
                topic: "events".into(),
                partition: Partition::new(3),
                durable_name: Some("analytics".into()),
                start: StreamStart::ByTime { time_ms: 1234 },
                filter: vec![
                    ("region".into(), "eu-*".into()),
                    ("kind".into(), "order".into()),
                ],
                prefetch: 64,
                auto_ack: true,
            },
            SubscribeStream {
                topic: "events".into(),
                partition: Partition::new(0),
                durable_name: None,
                start: StreamStart::NBack { count: 100 },
                filter: vec![],
                prefetch: 1,
                auto_ack: false,
            },
        ] {
            let frame = encode_subscribe_stream(11, &sub).unwrap();
            let got = decode_subscribe_stream(&frame).unwrap();
            assert_eq!(got.topic, sub.topic);
            assert_eq!(got.partition, sub.partition);
            assert_eq!(got.durable_name, sub.durable_name);
            assert_eq!(got.start, sub.start);
            assert_eq!(got.filter, sub.filter);
            assert_eq!(got.prefetch, sub.prefetch);
            assert_eq!(got.auto_ack, sub.auto_ack);
        }
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

    #[test]
    fn replication_stream_start_roundtrips() {
        let start = ReplicationStreamStart {
            topic: "orders".into(),
            group: Some("workers".into()),
            partition: Partition::new(7),
            message_from: 100,
            event_from: 42,
            credit_bytes: 8 * 1024 * 1024,
            reporter_node_id: Some("broker-2".into()),
        };
        let frame = encode_replication_stream_start(555, &start).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationStreamStart as u16);
        assert_eq!(frame.request_id, 555);
        assert_eq!(decode_replication_stream_start(&frame).unwrap(), start);
    }

    #[test]
    fn replication_stream_batch_roundtrips_reusing_read_ok_body() {
        let batch = sample_replication_read_ok();
        let frame = encode_replication_stream_batch(900, &batch).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationStreamBatch as u16);
        assert_eq!(frame.request_id, 900);
        assert_eq!(decode_replication_stream_batch(&frame).unwrap(), batch);
    }

    #[test]
    fn replication_stream_progress_roundtrips() {
        let progress = ReplicationStreamProgress {
            durable_message_next: 1_000,
            durable_event_next: 250,
            credit_add_bytes: 4 * 1024 * 1024,
        };
        let frame = encode_replication_stream_progress(900, &progress).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationStreamProgress as u16);
        assert_eq!(
            decode_replication_stream_progress(&frame).unwrap(),
            progress
        );
    }

    #[test]
    fn replication_stream_reset_roundtrips() {
        let reset = ReplicationStreamReset {
            message_from: 500,
            event_from: 120,
        };
        let frame = encode_replication_stream_reset(900, &reset).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationStreamReset as u16);
        assert_eq!(decode_replication_stream_reset(&frame).unwrap(), reset);
    }

    #[test]
    fn replication_stream_end_roundtrips() {
        let end = ReplicationStreamEnd {
            code: 1,
            message: "not owner".into(),
        };
        let frame = encode_replication_stream_end(900, &end).unwrap();
        assert_eq!(frame.opcode, Op::ReplicationStreamEnd as u16);
        assert_eq!(decode_replication_stream_end(&frame).unwrap(), end);
    }

    #[test]
    fn replication_stream_batch_rejects_wrong_opcode() {
        let frame = encode_replication_stream_start(
            1,
            &ReplicationStreamStart {
                topic: "t".into(),
                group: None,
                partition: Partition::new(0),
                message_from: 0,
                event_from: 0,
                credit_bytes: 1,
                reporter_node_id: None,
            },
        )
        .unwrap();
        assert!(matches!(
            decode_replication_stream_batch(&frame),
            Err(WireError::UnexpectedOpcode { .. })
        ));
    }
}
