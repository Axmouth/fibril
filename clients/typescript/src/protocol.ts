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

  DeclareQueue = 60,
  DeclareQueueOk = 61,

  ReconcileClient = 70,
  ReconcileServer = 71,
  ReconcileResult = 72,

  Topology = 90,
  TopologyOk = 91,
  Redirect = 92,

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
  resume: ResumeIdentity | null;
}

/** Successful server handshake response. */
export interface HelloOk {
  protocol_version: number;
  owner_id: unknown;
  client_id: unknown;
  resume_token: unknown;
  resume_outcome: ResumeOutcome;
  server_name: string;
  compliance: string;
}

/** Identity returned by the broker and offered on reconnect. */
export interface ResumeIdentity {
  owner_id: unknown;
  client_id: unknown;
  resume_token: unknown;
}

/** Result of a resume attempt during handshake. */
export type ResumeOutcome =
  | "new"
  | "resumed"
  | "resume_not_found"
  | "resume_rejected";

/** Username/password authentication frame body. */
export interface AuthMsg {
  username: string;
  password: string;
}

/** Payload content type metadata carried outside the extra header map. */
export type ContentType =
  | { kind: "msg_pack" }
  | { kind: "json" }
  | { kind: "text" }
  | { kind: "custom"; value: string };

/** Immediate publish frame body. */
export interface PublishMsg {
  topic: string;
  partition: number;
  group: string | null;
  require_confirm: boolean;
  content_type: ContentType | null;
  headers: Record<string, string>;
  payload: Uint8Array;
  published: bigint;
  // Wire fields; default to null / 0 until partitioning lands (brick 3).
  partition_key?: Uint8Array | null;
  partitioning_version?: bigint;
}

/** Delayed publish frame body. */
export interface PublishDelayedMsg extends PublishMsg {
  not_before: bigint;
}

/** Publish confirmation frame body. */
export interface PublishOkMsg {
  offset: bigint;
}

export type QueueDlqPolicy =
  | { kind: "discard" }
  | { kind: "global" }
  | { kind: "custom"; topic: string; group: string | null };

export interface DeclareQueueMsg {
  topic: string;
  group: string | null;
  dlq_policy: QueueDlqPolicy | null;
  dlq_max_retries: number | null;
  // Wire field; default null until repartition declares lands.
  partition_count?: number | null;
}

export interface DeclareQueueOkMsg {
  status: string;
}

/** Subscribe frame body. */
export interface SubscribeMsg {
  topic: string;
  group: string | null;
  prefetch: number;
  auto_ack: boolean;
  // Wire fields; default 0 / null until topology routing + groups land.
  partition?: number;
  consumer_group?: string | null;
  consumer_target?: number | null;
  member_id?: Uint8Array | null;
}

/** Successful subscribe response body. */
export interface SubscribeOkMsg {
  sub_id: bigint;
  topic: string;
  group: string | null;
  partition: number;
  prefetch: number;
}

export interface ReconcileSubscription {
  sub_id: bigint;
  topic: string;
  group: string | null;
  partition: number;
  auto_ack: boolean;
  prefetch: number;
}

export type ReconcilePolicy = "conservative" | "restore_client_subscriptions";

export interface ReconcileClientMsg {
  policy: ReconcilePolicy;
  subscriptions: ReconcileSubscription[];
}

export interface ReconcileServerMsg {
  subscriptions: ReconcileSubscription[];
}

export type ReconcileAction =
  | "keep"
  | "close_client_side"
  | "close_server_side"
  | "recreate_client_side";

export interface ReconcileSubscriptionResult {
  client: ReconcileSubscription | null;
  server: ReconcileSubscription | null;
  action: ReconcileAction;
  reason: string;
}

export interface ReconcileResultMsg {
  subscriptions: ReconcileSubscriptionResult[];
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
  content_type: ContentType | null;
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
  not_before: bigint | null;
}

/** Structured broker error frame body. */
export interface ErrorMsg {
  code: number;
  message: string;
}

// ===== Cluster topology + routing =====

/** Topology query. A null topic asks for the full topology. */
export interface TopologyRequestMsg {
  topic: string | null;
  group: string | null;
}

/** Ownership of one queue partition, as seen by clients for routing. */
export interface QueueTopologyEntryMsg {
  topic: string;
  partition: number;
  group: string | null;
  // Owner broker endpoint, absent when the owner node is not in the registry.
  owner_endpoint: string | null;
  partitioning_version: bigint;
  // Authoritative partition count for the queue, used for key routing.
  partition_count: number;
}

/** Topology snapshot at a coordination generation. */
export interface TopologyOkMsg {
  generation: bigint;
  queues: QueueTopologyEntryMsg[];
}

/**
 * Routing redirect: not an error but a target to retry against. The retry must
 * go to owner_endpoint on a separate connection, so the routing layer (not the
 * per-connection engine) acts on it.
 */
export interface RedirectMsg {
  topic: string;
  partition: number;
  group: string | null;
  owner_endpoint: string;
  partitioning_version: bigint;
}
