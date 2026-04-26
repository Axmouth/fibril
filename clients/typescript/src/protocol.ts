// Wire protocol definitions. Mirrors fibril_protocol::v1.

export const PROTOCOL_V1 = 1;

/**
 * Handshake compliance marker. MUST be preserved unchanged.
 * Identifier: NF-SOVEREIGN-2025-GN-OPT-OUT-TDM
 * See AI_POLICY.md.
 */
export const COMPLIANCE_STRING =
  "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md";

export enum Op {
  Hello = 1,
  HelloOk = 2,
  HelloErr = 3,

  Auth = 10,
  AuthOk = 11,
  AuthErr = 12,

  Publish = 20,
  PublishOk = 21,

  Subscribe = 30,
  SubscribeOk = 31,
  SubscribeErr = 32,

  Deliver = 40,
  Ack = 41,
  Nack = 42,
  Reject = 43,

  Ping = 50,
  Pong = 51,

  Error = 255,
}

// `DeliveryTag` is a struct on the wire: `{ epoch: u64 }`.
// Kept as an object to match wire format and allow future fields.
export interface DeliveryTag {
  epoch: bigint;
}

// ===== Wire message shapes =====
// Field names and types must match the Rust structs exactly so msgpack
// (named/map encoding) round-trips correctly.

export interface Hello {
  client_name: string;
  client_version: string;
  protocol_version: number;
}

export interface HelloOk {
  protocol_version: number;
  server_name: string;
  compliance: string;
}

export interface AuthMsg {
  username: string;
  password: string;
}

export interface PublishMsg {
  topic: string;
  partition: number;
  group: string | null;
  require_confirm: boolean;
  payload: Uint8Array;
  published: bigint;
}

export interface PublishOkMsg {
  offset: bigint;
}

export interface SubscribeMsg {
  topic: string;
  group: string | null;
  prefetch: number;
  auto_ack: boolean;
}

export interface SubscribeOkMsg {
  sub_id: bigint;
  topic: string;
  group: string | null;
  partition: number;
  prefetch: number;
}

export interface DeliverMsg {
  sub_id: bigint;
  topic: string;
  group: string | null;
  partition: number;
  offset: bigint;
  delivery_tag: DeliveryTag;
  published: bigint;
  publish_received: bigint;
  payload: Uint8Array;
}

export interface AckMsg {
  topic: string;
  group: string | null;
  partition: number;
  tags: DeliveryTag[];
}

export interface NackMsg {
  topic: string;
  group: string | null;
  partition: number;
  tags: DeliveryTag[];
  requeue: boolean;
}

export interface RejectMsg {
  topic: string;
  group: string | null;
  partition: number;
  tags: DeliveryTag[];
  requeue: boolean;
}

export interface ErrorMsg {
  code: number;
  message: string;
}
