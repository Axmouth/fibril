// Adapter between the engine's protocol.ts struct shapes (snake_case, the legacy
// msgpack field names) and the custom binary wire codec (wire.ts, camelCase).
// codec.ts delegates here so the engine keeps using protocol.ts structs while the
// bytes on the wire are the broker's custom format. Fields the protocol structs
// do not carry yet (partition_key, consumer-group routing, etc.) default to
// null/0 here until those features land client-side.
//
// The mapping is symmetric: encodeBody/decodeBody handle every opcode in both
// directions so the in-process fake broker used by the tests speaks the same
// format as the real broker.

import { Op } from "./protocol.js";
import type {
  AckMsg,
  AuthMsg,
  ContentType,
  DeclareQueueMsg,
  AssignmentChangedMsg,
  DeclareQueueOkMsg,
  DeliverMsg,
  ErrorMsg,
  Hello,
  HelloOk,
  NackMsg,
  PublishDelayedMsg,
  PublishMsg,
  PublishOkMsg,
  QueueDlqPolicy,
  QueueTopologyEntryMsg,
  ReconcileClientMsg,
  ReconcileResultMsg,
  ReconcileServerMsg,
  ReconcileSubscription,
  RedirectMsg,
  ResumeIdentity,
  SubscribeMsg,
  SubscribeOkMsg,
  TopologyOkMsg,
  TopologyRequestMsg,
} from "./protocol.js";
import * as wire from "./wire.js";

const EMPTY = new Uint8Array(0);

function ctToWire(ct: ContentType | null | undefined): wire.ContentType {
  if (ct == null) return null;
  switch (ct.kind) {
    case "msg_pack":
      return "msgpack";
    case "json":
      return "json";
    case "text":
      return "text";
    case "custom":
      return { custom: ct.value };
  }
}

function ctFromWire(ct: wire.ContentType): ContentType | null {
  if (ct == null) return null;
  if (ct === "msgpack") return { kind: "msg_pack" };
  if (ct === "json") return { kind: "json" };
  if (ct === "text") return { kind: "text" };
  return { kind: "custom", value: ct.custom };
}

function dlqToWire(p: QueueDlqPolicy | null | undefined): wire.QueueDlqPolicy | null {
  if (p == null) return null;
  switch (p.kind) {
    case "discard":
      return "discard";
    case "global":
      return "global";
    case "custom":
      return { custom: { topic: p.topic, group: p.group } };
  }
}

function dlqFromWire(p: wire.QueueDlqPolicy | null): QueueDlqPolicy | null {
  if (p == null) return null;
  if (p === "discard") return { kind: "discard" };
  if (p === "global") return { kind: "global" };
  return { kind: "custom", topic: p.custom.topic, group: p.custom.group };
}

function riToWire(ri: ResumeIdentity): wire.ResumeIdentity {
  return {
    ownerId: ri.owner_id as Uint8Array,
    clientId: ri.client_id as Uint8Array,
    resumeToken: ri.resume_token as Uint8Array,
  };
}

function riFromWire(ri: wire.ResumeIdentity): ResumeIdentity {
  return { owner_id: ri.ownerId, client_id: ri.clientId, resume_token: ri.resumeToken };
}

function rsubToWire(s: ReconcileSubscription): wire.ReconcileSubscription {
  return {
    subId: s.sub_id,
    topic: s.topic,
    partition: s.partition,
    group: s.group,
    autoAck: s.auto_ack,
    prefetch: s.prefetch,
    consumerGroup: null,
    consumerTarget: null,
    memberId: null,
  };
}

function rsubFromWire(s: wire.ReconcileSubscription): ReconcileSubscription {
  return {
    sub_id: s.subId,
    topic: s.topic,
    group: s.group,
    partition: s.partition,
    auto_ack: s.autoAck,
    prefetch: s.prefetch,
  };
}

function publishToWire(v: PublishMsg): wire.Publish {
  return {
    topic: v.topic,
    partition: v.partition,
    group: v.group,
    requireConfirm: v.require_confirm,
    contentType: ctToWire(v.content_type),
    headers: v.headers,
    payload: v.payload,
    published: v.published,
    partitionKey: v.partition_key ?? null,
    partitioningVersion: v.partitioning_version ?? 0n,
    ttlMs: v.ttl_ms ?? null,
  };
}

function entryToWire(e: QueueTopologyEntryMsg): wire.QueueTopologyEntry {
  return {
    topic: e.topic,
    partition: e.partition,
    group: e.group,
    ownerEndpoint: e.owner_endpoint,
    partitioningVersion: e.partitioning_version,
    partitionCount: e.partition_count,
  };
}

function entryFromWire(e: wire.QueueTopologyEntry): QueueTopologyEntryMsg {
  return {
    topic: e.topic,
    partition: e.partition,
    group: e.group,
    owner_endpoint: e.ownerEndpoint,
    partitioning_version: e.partitioningVersion,
    partition_count: e.partitionCount,
  };
}

function publishFromWire(w: wire.Publish): PublishMsg {
  return {
    topic: w.topic,
    partition: w.partition,
    group: w.group,
    require_confirm: w.requireConfirm,
    content_type: ctFromWire(w.contentType),
    headers: w.headers,
    payload: w.payload,
    published: w.published,
    partition_key: w.partitionKey,
    partitioning_version: w.partitioningVersion,
    ttl_ms: w.ttlMs,
  };
}

/** Encode a frame body for `op` from the engine's protocol struct. */
export function encodeBody(op: Op, value: unknown): Uint8Array {
  switch (op) {
    case Op.Hello: {
      const v = value as Hello;
      return wire.encodeHelloBody({
        clientName: v.client_name,
        clientVersion: v.client_version,
        protocolVersion: v.protocol_version,
        resume: v.resume ? riToWire(v.resume) : null,
      });
    }
    case Op.HelloOk: {
      const v = value as HelloOk;
      return wire.encodeHelloOkBody({
        protocolVersion: v.protocol_version,
        ownerId: v.owner_id as Uint8Array,
        clientId: v.client_id as Uint8Array,
        resumeToken: v.resume_token as Uint8Array,
        resumeOutcome: v.resume_outcome,
        serverName: v.server_name,
        compliance: v.compliance,
      });
    }
    case Op.Auth: {
      const v = value as AuthMsg;
      return wire.encodeAuthBody({ username: v.username, password: v.password });
    }
    case Op.Publish:
      return wire.encodePublishBody(publishToWire(value as PublishMsg));
    case Op.PublishDelayed: {
      const v = value as PublishDelayedMsg;
      return wire.encodePublishDelayedBody({ ...publishToWire(v), notBefore: v.not_before });
    }
    case Op.PublishOk:
      return wire.encodePublishOkBody({ offset: (value as PublishOkMsg).offset });
    case Op.Subscribe: {
      const v = value as SubscribeMsg;
      return wire.encodeSubscribeBody({
        topic: v.topic,
        partition: v.partition ?? 0,
        group: v.group,
        prefetch: v.prefetch,
        autoAck: v.auto_ack,
        consumerGroup: v.consumer_group ?? null,
        consumerTarget: v.consumer_target ?? null,
        memberId: v.member_id ?? null,
      });
    }
    case Op.SubscribeOk: {
      const v = value as SubscribeOkMsg;
      return wire.encodeSubscribeOkBody({
        subId: v.sub_id,
        topic: v.topic,
        partition: v.partition,
        group: v.group,
        prefetch: v.prefetch,
        consumerGroup: v.consumer_group ?? null,
        consumerTarget: v.consumer_target ?? null,
        memberId: v.member_id ?? null,
      });
    }
    case Op.Deliver: {
      const v = value as DeliverMsg;
      return wire.encodeDeliverBody({
        subId: v.sub_id,
        topic: v.topic,
        group: v.group,
        partition: v.partition,
        offset: v.offset,
        deliveryTag: { epoch: v.delivery_tag.epoch },
        published: v.published,
        publishReceived: v.publish_received,
        contentType: ctToWire(v.content_type),
        headers: v.headers,
        payload: v.payload,
      });
    }
    case Op.Ack: {
      const v = value as AckMsg;
      return wire.encodeAckBody({
        topic: v.topic,
        group: v.group,
        partition: v.partition,
        tags: v.tags.map((t) => ({ epoch: t.epoch })),
      });
    }
    case Op.Nack: {
      const v = value as NackMsg;
      return wire.encodeNackBody({
        topic: v.topic,
        group: v.group,
        partition: v.partition,
        tags: v.tags.map((t) => ({ epoch: t.epoch })),
        requeue: v.requeue,
        notBefore: v.not_before,
      });
    }
    case Op.DeclareQueue: {
      const v = value as DeclareQueueMsg;
      return wire.encodeDeclareQueueBody({
        topic: v.topic,
        group: v.group,
        dlqPolicy: dlqToWire(v.dlq_policy),
        dlqMaxRetries: v.dlq_max_retries,
        partitionCount: v.partition_count ?? null,
        defaultMessageTtlMs: v.default_message_ttl_ms ?? null,
      });
    }
    case Op.DeclareQueueOk: {
      const v = value as DeclareQueueOkMsg;
      return wire.encodeDeclareQueueOkBody({ status: v.status, partitionCount: 0 });
    }
    // Plexus (fan-out stream) ops carry the wire-shaped body directly.
    case Op.DeclarePlexus:
      return wire.encodeDeclarePlexusBody(value as wire.DeclarePlexus);
    case Op.DeclarePlexusOk:
      return wire.encodeDeclarePlexusOkBody(value as wire.DeclarePlexusOk);
    case Op.SubscribeStream:
      return wire.encodeSubscribeStreamBody(value as wire.SubscribeStream);
    case Op.AssignmentChanged: {
      const v = value as AssignmentChangedMsg;
      return wire.encodeAssignmentChangedBody({
        topic: v.topic,
        group: v.group,
        consumerGroup: v.consumer_group,
        generation: v.generation,
        assigned: v.assigned,
        added: v.added,
        revoked: v.revoked,
      });
    }
    case Op.ReconcileClient: {
      const v = value as ReconcileClientMsg;
      return wire.encodeReconcileClientBody({
        policy: v.policy,
        subscriptions: v.subscriptions.map(rsubToWire),
      });
    }
    case Op.ReconcileServer: {
      const v = value as ReconcileServerMsg;
      return wire.encodeReconcileServerBody({
        subscriptions: v.subscriptions.map(rsubToWire),
      });
    }
    case Op.ReconcileResult: {
      const v = value as ReconcileResultMsg;
      return wire.encodeReconcileResultBody({
        subscriptions: v.subscriptions.map((s) => ({
          client: s.client ? rsubToWire(s.client) : null,
          server: s.server ? rsubToWire(s.server) : null,
          action: s.action,
          reason: s.reason,
        })),
      });
    }
    case Op.Topology: {
      const v = value as TopologyRequestMsg;
      return wire.encodeTopologyRequestBody({ topic: v.topic, group: v.group });
    }
    case Op.TopologyOk: {
      const v = value as TopologyOkMsg;
      return wire.encodeTopologyOkBody({
        generation: v.generation,
        queues: v.queues.map(entryToWire),
      });
    }
    case Op.Redirect: {
      const v = value as RedirectMsg;
      return wire.encodeRedirectBody({
        topic: v.topic,
        partition: v.partition,
        group: v.group,
        ownerEndpoint: v.owner_endpoint,
        partitioningVersion: v.partitioning_version,
      });
    }
    case Op.HelloErr:
    case Op.AuthErr:
    case Op.SubscribeErr:
    case Op.Error: {
      const v = value as ErrorMsg;
      return wire.encodeErrorBody({ code: v.code, message: v.message });
    }
    case Op.Ping:
    case Op.Pong:
    case Op.AuthOk:
      return EMPTY;
    default:
      throw new Error(`frames: no encoder for opcode ${op}`);
  }
}

/** Decode a frame body for `op` into the engine's protocol struct. */
export function decodeBody(op: Op, payload: Uint8Array): unknown {
  switch (op) {
    case Op.Hello: {
      const w = wire.decodeHelloBody(payload);
      return {
        client_name: w.clientName,
        client_version: w.clientVersion,
        protocol_version: w.protocolVersion,
        resume: w.resume ? riFromWire(w.resume) : null,
      } satisfies Hello;
    }
    case Op.HelloOk: {
      const w = wire.decodeHelloOkBody(payload);
      return {
        protocol_version: w.protocolVersion,
        owner_id: w.ownerId,
        client_id: w.clientId,
        resume_token: w.resumeToken,
        resume_outcome: w.resumeOutcome,
        server_name: w.serverName,
        compliance: w.compliance,
      } satisfies HelloOk;
    }
    case Op.Auth: {
      const w = wire.decodeAuthBody(payload);
      return { username: w.username, password: w.password } satisfies AuthMsg;
    }
    case Op.Publish:
      return publishFromWire(wire.decodePublishBody(payload));
    case Op.PublishDelayed: {
      const w = wire.decodePublishDelayedBody(payload);
      return {
        ...publishFromWire({ ...w, ttlMs: null }),
        not_before: w.notBefore,
      } satisfies PublishDelayedMsg;
    }
    case Op.PublishOk: {
      const w = wire.decodePublishOkBody(payload);
      return { offset: w.offset } satisfies PublishOkMsg;
    }
    case Op.Subscribe: {
      const w = wire.decodeSubscribeBody(payload);
      return {
        topic: w.topic,
        group: w.group,
        prefetch: w.prefetch,
        auto_ack: w.autoAck,
        partition: w.partition,
        consumer_group: w.consumerGroup,
        consumer_target: w.consumerTarget,
        member_id: w.memberId,
      } satisfies SubscribeMsg;
    }
    case Op.SubscribeOk: {
      const w = wire.decodeSubscribeOkBody(payload);
      return {
        sub_id: w.subId,
        topic: w.topic,
        group: w.group,
        partition: w.partition,
        prefetch: w.prefetch,
        consumer_group: w.consumerGroup,
        consumer_target: w.consumerTarget,
        member_id: w.memberId,
      } satisfies SubscribeOkMsg;
    }
    case Op.Deliver: {
      const w = wire.decodeDeliverBody(payload);
      return {
        sub_id: w.subId,
        topic: w.topic,
        group: w.group,
        partition: w.partition,
        offset: w.offset,
        delivery_tag: { epoch: w.deliveryTag.epoch },
        published: w.published,
        publish_received: w.publishReceived,
        content_type: ctFromWire(w.contentType),
        headers: w.headers,
        payload: w.payload,
      } satisfies DeliverMsg;
    }
    case Op.Ack: {
      const w = wire.decodeAckBody(payload);
      return {
        topic: w.topic,
        group: w.group,
        partition: w.partition,
        tags: w.tags.map((t) => ({ epoch: t.epoch })),
      } satisfies AckMsg;
    }
    case Op.Nack: {
      const w = wire.decodeNackBody(payload);
      return {
        topic: w.topic,
        group: w.group,
        partition: w.partition,
        tags: w.tags.map((t) => ({ epoch: t.epoch })),
        requeue: w.requeue,
        not_before: w.notBefore,
      } satisfies NackMsg;
    }
    case Op.DeclareQueue: {
      const w = wire.decodeDeclareQueueBody(payload);
      return {
        topic: w.topic,
        group: w.group,
        dlq_policy: dlqFromWire(w.dlqPolicy),
        dlq_max_retries: w.dlqMaxRetries,
        partition_count: w.partitionCount,
        default_message_ttl_ms: w.defaultMessageTtlMs,
      } satisfies DeclareQueueMsg;
    }
    case Op.DeclareQueueOk: {
      const w = wire.decodeDeclareQueueOkBody(payload);
      return { status: w.status } satisfies DeclareQueueOkMsg;
    }
    case Op.DeclarePlexus:
      return wire.decodeDeclarePlexusBody(payload);
    case Op.DeclarePlexusOk:
      return wire.decodeDeclarePlexusOkBody(payload);
    case Op.SubscribeStream:
      return wire.decodeSubscribeStreamBody(payload);
    case Op.AssignmentChanged: {
      const w = wire.decodeAssignmentChangedBody(payload);
      return {
        topic: w.topic,
        group: w.group,
        consumer_group: w.consumerGroup,
        generation: w.generation,
        assigned: w.assigned,
        added: w.added,
        revoked: w.revoked,
      } satisfies AssignmentChangedMsg;
    }
    case Op.ReconcileClient: {
      const w = wire.decodeReconcileClientBody(payload);
      return {
        policy: w.policy,
        subscriptions: w.subscriptions.map(rsubFromWire),
      } satisfies ReconcileClientMsg;
    }
    case Op.ReconcileServer: {
      const w = wire.decodeReconcileServerBody(payload);
      return { subscriptions: w.subscriptions.map(rsubFromWire) } satisfies ReconcileServerMsg;
    }
    case Op.ReconcileResult: {
      const w = wire.decodeReconcileResultBody(payload);
      return {
        subscriptions: w.subscriptions.map((s) => ({
          client: s.client ? rsubFromWire(s.client) : null,
          server: s.server ? rsubFromWire(s.server) : null,
          action: s.action,
          reason: s.reason,
        })),
      } satisfies ReconcileResultMsg;
    }
    case Op.Topology: {
      const w = wire.decodeTopologyRequestBody(payload);
      return { topic: w.topic, group: w.group } satisfies TopologyRequestMsg;
    }
    case Op.TopologyOk: {
      const w = wire.decodeTopologyOkBody(payload);
      return {
        generation: w.generation,
        queues: w.queues.map(entryFromWire),
      } satisfies TopologyOkMsg;
    }
    case Op.Redirect: {
      const w = wire.decodeRedirectBody(payload);
      return {
        topic: w.topic,
        partition: w.partition,
        group: w.group,
        owner_endpoint: w.ownerEndpoint,
        partitioning_version: w.partitioningVersion,
      } satisfies RedirectMsg;
    }
    case Op.HelloErr:
    case Op.AuthErr:
    case Op.SubscribeErr:
    case Op.Error: {
      const w = wire.decodeErrorBody(payload);
      return { code: w.code, message: w.message } satisfies ErrorMsg;
    }
    case Op.AuthOk:
    case Op.Pong:
    case Op.Ping:
      return undefined;
    default:
      throw new Error(`frames: no decoder for opcode ${op}`);
  }
}
