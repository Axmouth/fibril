// Wire protocol definitions. Mirrors fibril_protocol::v1.
// Most application code should use Client, Publisher, and Subscription instead.

/** Current Fibril TCP protocol version. */
export const PROTOCOL_V1 = 1;

/**
 * Handshake compliance marker. MUST be preserved unchanged.
 * Identifier: NF-SOVEREIGN-2025-GN-OPT-OUT-TDM
 * See AI_POLICY.md.
 */
export const COMPLIANCE_STRING =
  "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md";

/** Numeric operation codes used in protocol frames. */
export enum Op {
  Hello = 1,
  HelloOk = 2,
  HelloErr = 3,

  Auth = 10,
  AuthOk = 11,
  AuthErr = 12,

  Publish = 20,
  PublishDelayed = 21,
  PublishOk = 25,

  Subscribe = 30,
  SubscribeOk = 31,
  SubscribeErr = 32,

  Deliver = 40,
  Ack = 41,
  Nack = 42,

  Ping = 50,
  Pong = 51,

  Error = 255,
}

/**
 * Broker delivery tag used to acknowledge or negatively acknowledge a delivery.
 *
 * This is an object to match the wire format and leave room for future fields.
 */
export interface DeliveryTag {
  /** Delivery epoch assigned by the broker. */
  epoch: bigint;
}

// ===== Wire message shapes =====
// Field names and types must match the Rust structs exactly so msgpack
// (named/map encoding) round-trips correctly.

/** Client handshake frame body. */
export interface Hello {
  client_name: string;
  client_version: string;
  protocol_version: number;
}

/** Successful server handshake response. */
export interface HelloOk {
  protocol_version: number;
  client_id: unknown;
  server_name: string;
  compliance: string;
}

/** Username/password authentication frame body. */
export interface AuthMsg {
  username: string;
  password: string;
}

/** Immediate publish frame body. */
export interface PublishMsg {
  topic: string;
  partition: number;
  group: string | null;
  require_confirm: boolean;
  headers: Record<string, string>;
  payload: Uint8Array;
  published: bigint;
}

/** Delayed publish frame body. */
export interface PublishDelayedMsg extends PublishMsg {
  not_before: bigint;
}

/** Publish confirmation frame body. */
export interface PublishOkMsg {
  offset: bigint;
}

/** Subscribe frame body. */
export interface SubscribeMsg {
  topic: string;
  group: string | null;
  prefetch: number;
  auto_ack: boolean;
}

/** Successful subscribe response body. */
export interface SubscribeOkMsg {
  sub_id: bigint;
  topic: string;
  group: string | null;
  partition: number;
  prefetch: number;
}

/** Broker delivery frame body. */
export interface DeliverMsg {
  sub_id: bigint;
  topic: string;
  group: string | null;
  partition: number;
  offset: bigint;
  delivery_tag: DeliveryTag;
  published: bigint;
  publish_received: bigint;
  headers: Record<string, string>;
  payload: Uint8Array;
}

/** Acknowledgement frame body. */
export interface AckMsg {
  topic: string;
  group: string | null;
  partition: number;
  tags: DeliveryTag[];
}

/** Negative acknowledgement frame body. */
export interface NackMsg {
  topic: string;
  group: string | null;
  partition: number;
  tags: DeliveryTag[];
  requeue: boolean;
}

/** Structured broker error frame body. */
export interface ErrorMsg {
  code: number;
  message: string;
}
