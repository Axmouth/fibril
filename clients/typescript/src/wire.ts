// Custom binary wire format for v1 frame bodies, matching the Rust
// `crates/protocol/src/v1/wire.rs`. The broker moved frame bodies from msgpack
// to this format (faster, avoids scheduler starvation); the 20-byte frame header
// (see codec.ts) is unchanged - only the bodies are encoded here.
//
// Layout rules (all big-endian):
//   - integers: u8 / u16 / u32 / u64 big-endian
//   - len-prefixed bytes: u32 length, then the raw bytes
//   - strings: len-prefixed UTF-8
//   - bool: u8 (0 or 1)
//   - uuid: 16 raw bytes (treated as opaque here - the client only echoes them)
//   - option<T>: u8 tag (0 = none, 1 = some), then T when some
//   - each body starts with a 4-byte ASCII magic

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

/** Opaque 16-byte identifier (uuid on the wire). The client never interprets it. */
export type Uuid = Uint8Array;

export interface ResumeIdentity {
  ownerId: Uuid;
  clientId: Uuid;
  resumeToken: Uuid;
}

export type ResumeOutcome = "new" | "resumed" | "resume_not_found" | "resume_rejected";

const RESUME_OUTCOME_TO_U8: Record<ResumeOutcome, number> = {
  new: 0,
  resumed: 1,
  resume_not_found: 2,
  resume_rejected: 3,
};
const RESUME_OUTCOME_FROM_U8: ResumeOutcome[] = [
  "new",
  "resumed",
  "resume_not_found",
  "resume_rejected",
];

export type ReconcilePolicy = "conservative" | "restore_client_subscriptions";
export type ReconcileAction =
  | "keep"
  | "close_client_side"
  | "close_server_side"
  | "recreate_client_side";

const RECONCILE_ACTION_TO_U8: Record<ReconcileAction, number> = {
  keep: 0,
  close_client_side: 1,
  close_server_side: 2,
  recreate_client_side: 3,
};
const RECONCILE_ACTION_FROM_U8: ReconcileAction[] = [
  "keep",
  "close_client_side",
  "close_server_side",
  "recreate_client_side",
];

/** Growable big-endian byte writer. */
export class Writer {
  private buf: Uint8Array;
  private view: DataView;
  private len = 0;

  constructor(initial = 64) {
    this.buf = new Uint8Array(initial);
    this.view = new DataView(this.buf.buffer);
  }

  private ensure(extra: number): void {
    const need = this.len + extra;
    if (need <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < need) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
    this.view = new DataView(this.buf.buffer);
  }

  u8(v: number): void {
    this.ensure(1);
    this.view.setUint8(this.len, v);
    this.len += 1;
  }
  u16(v: number): void {
    this.ensure(2);
    this.view.setUint16(this.len, v, false);
    this.len += 2;
  }
  u32(v: number): void {
    this.ensure(4);
    this.view.setUint32(this.len, v, false);
    this.len += 4;
  }
  u64(v: bigint): void {
    this.ensure(8);
    this.view.setBigUint64(this.len, v, false);
    this.len += 8;
  }
  raw(bytes: Uint8Array): void {
    this.ensure(bytes.length);
    this.buf.set(bytes, this.len);
    this.len += bytes.length;
  }
  bytes(b: Uint8Array): void {
    this.u32(b.length);
    this.raw(b);
  }
  str(s: string): void {
    this.bytes(textEncoder.encode(s));
  }
  bool(b: boolean): void {
    this.u8(b ? 1 : 0);
  }
  uuid(u: Uuid): void {
    if (u.length !== 16) throw new Error(`wire: uuid must be 16 bytes, got ${u.length}`);
    this.raw(u);
  }
  optionalStr(s: string | null | undefined): void {
    if (s == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.str(s);
    }
  }
  optionalUuid(u: Uuid | null | undefined): void {
    if (u == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.uuid(u);
    }
  }
  resumeOutcome(outcome: ResumeOutcome): void {
    this.u8(RESUME_OUTCOME_TO_U8[outcome]);
  }
  resumeIdentity(id: ResumeIdentity): void {
    this.uuid(id.ownerId);
    this.uuid(id.clientId);
    this.uuid(id.resumeToken);
  }
  optionalResumeIdentity(id: ResumeIdentity | null | undefined): void {
    if (id == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.resumeIdentity(id);
    }
  }
  optionalBytes(b: Uint8Array | null | undefined): void {
    if (b == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.bytes(b);
    }
  }
  optionalU32(v: number | null | undefined): void {
    if (v == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.u32(v);
    }
  }
  optionalU64(v: bigint | null | undefined): void {
    if (v == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.u64(v);
    }
  }
  contentType(ct: ContentType): void {
    if (ct == null) {
      this.u8(0);
    } else if (ct === "msgpack") {
      this.u8(1);
    } else if (ct === "json") {
      this.u8(2);
    } else if (ct === "text") {
      this.u8(3);
    } else {
      this.u8(4);
      this.str(ct.custom);
    }
  }
  headers(h: Headers): void {
    const entries = Object.entries(h);
    this.u32(entries.length);
    for (const [k, v] of entries) {
      this.str(k);
      this.str(v);
    }
  }
  queueKey(topic: string, partition: number, group: string | null | undefined): void {
    this.str(topic);
    this.u32(partition);
    this.optionalStr(group);
  }
  settleTags(tags: DeliveryTag[]): void {
    this.u32(tags.length);
    for (const t of tags) {
      this.u64(t.epoch);
    }
  }
  dlqPolicy(p: QueueDlqPolicy): void {
    if (p === "discard") {
      this.u8(0);
    } else if (p === "global") {
      this.u8(1);
    } else {
      this.u8(2);
      this.str(p.custom.topic);
      this.optionalStr(p.custom.group);
    }
  }
  optionalDlqPolicy(p: QueueDlqPolicy | null | undefined): void {
    if (p == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.dlqPolicy(p);
    }
  }
  reconcilePolicy(p: ReconcilePolicy): void {
    this.u8(p === "restore_client_subscriptions" ? 1 : 0);
  }
  reconcileAction(a: ReconcileAction): void {
    this.u8(RECONCILE_ACTION_TO_U8[a]);
  }
  reconcileSubscription(s: ReconcileSubscription): void {
    this.u64(s.subId);
    this.queueKey(s.topic, s.partition, s.group);
    this.bool(s.autoAck);
    this.u32(s.prefetch);
    this.optionalStr(s.consumerGroup);
    this.optionalU32(s.consumerTarget);
    this.optionalUuid(s.memberId);
  }
  optionalReconcileSubscription(s: ReconcileSubscription | null | undefined): void {
    if (s == null) {
      this.u8(0);
    } else {
      this.u8(1);
      this.reconcileSubscription(s);
    }
  }
  reconcileSubscriptions(subs: ReconcileSubscription[]): void {
    this.u32(subs.length);
    for (const s of subs) {
      this.reconcileSubscription(s);
    }
  }
  /** A 4-byte ASCII magic that opens every frame body. */
  magic(m: string): void {
    this.raw(textEncoder.encode(m));
  }
  finish(): Uint8Array {
    return this.buf.slice(0, this.len);
  }
}

/** Big-endian byte reader with strict bounds + trailing-byte checks. */
export class Reader {
  private view: DataView;
  private cursor = 0;

  constructor(private input: Uint8Array) {
    this.view = new DataView(input.buffer, input.byteOffset, input.byteLength);
  }

  private need(n: number): void {
    if (this.cursor + n > this.input.length) {
      throw new Error("wire: unexpected end of input");
    }
  }

  u8(): number {
    this.need(1);
    const v = this.view.getUint8(this.cursor);
    this.cursor += 1;
    return v;
  }
  u16(): number {
    this.need(2);
    const v = this.view.getUint16(this.cursor, false);
    this.cursor += 2;
    return v;
  }
  u32(): number {
    this.need(4);
    const v = this.view.getUint32(this.cursor, false);
    this.cursor += 4;
    return v;
  }
  u64(): bigint {
    this.need(8);
    const v = this.view.getBigUint64(this.cursor, false);
    this.cursor += 8;
    return v;
  }
  raw(n: number): Uint8Array {
    this.need(n);
    const v = this.input.slice(this.cursor, this.cursor + n);
    this.cursor += n;
    return v;
  }
  bytes(): Uint8Array {
    return this.raw(this.u32());
  }
  str(): string {
    return textDecoder.decode(this.bytes());
  }
  bool(): boolean {
    return this.u8() !== 0;
  }
  uuid(): Uuid {
    return this.raw(16);
  }
  optionalStr(): string | null {
    return this.u8() === 1 ? this.str() : null;
  }
  optionalUuid(): Uuid | null {
    return this.u8() === 1 ? this.uuid() : null;
  }
  resumeOutcome(): ResumeOutcome {
    const tag = this.u8();
    const outcome = RESUME_OUTCOME_FROM_U8[tag];
    if (outcome === undefined) throw new Error(`wire: unknown resume outcome ${tag}`);
    return outcome;
  }
  resumeIdentity(): ResumeIdentity {
    return { ownerId: this.uuid(), clientId: this.uuid(), resumeToken: this.uuid() };
  }
  optionalResumeIdentity(): ResumeIdentity | null {
    return this.u8() === 1 ? this.resumeIdentity() : null;
  }
  optionalBytes(): Uint8Array | null {
    return this.u8() === 1 ? this.bytes() : null;
  }
  optionalU32(): number | null {
    return this.u8() === 1 ? this.u32() : null;
  }
  optionalU64(): bigint | null {
    return this.u8() === 1 ? this.u64() : null;
  }
  contentType(): ContentType {
    const tag = this.u8();
    switch (tag) {
      case 0:
        return null;
      case 1:
        return "msgpack";
      case 2:
        return "json";
      case 3:
        return "text";
      case 4:
        return { custom: this.str() };
      default:
        throw new Error(`wire: unknown content type ${tag}`);
    }
  }
  headers(): Headers {
    const n = this.u32();
    const h: Headers = {};
    for (let i = 0; i < n; i += 1) {
      const k = this.str();
      h[k] = this.str();
    }
    return h;
  }
  queueKey(): { topic: string; partition: number; group: string | null } {
    return { topic: this.str(), partition: this.u32(), group: this.optionalStr() };
  }
  settleTags(): DeliveryTag[] {
    const n = this.u32();
    const tags: DeliveryTag[] = [];
    for (let i = 0; i < n; i += 1) {
      tags.push({ epoch: this.u64() });
    }
    return tags;
  }
  dlqPolicy(): QueueDlqPolicy {
    const tag = this.u8();
    switch (tag) {
      case 0:
        return "discard";
      case 1:
        return "global";
      case 2:
        return { custom: { topic: this.str(), group: this.optionalStr() } };
      default:
        throw new Error(`wire: unknown dlq policy ${tag}`);
    }
  }
  optionalDlqPolicy(): QueueDlqPolicy | null {
    return this.u8() === 1 ? this.dlqPolicy() : null;
  }
  reconcilePolicy(): ReconcilePolicy {
    return this.u8() === 1 ? "restore_client_subscriptions" : "conservative";
  }
  reconcileAction(): ReconcileAction {
    const tag = this.u8();
    const a = RECONCILE_ACTION_FROM_U8[tag];
    if (a === undefined) throw new Error(`wire: unknown reconcile action ${tag}`);
    return a;
  }
  reconcileSubscription(): ReconcileSubscription {
    const subId = this.u64();
    const key = this.queueKey();
    const autoAck = this.bool();
    const prefetch = this.u32();
    return {
      subId,
      topic: key.topic,
      partition: key.partition,
      group: key.group,
      autoAck,
      prefetch,
      consumerGroup: this.optionalStr(),
      consumerTarget: this.optionalU32(),
      memberId: this.optionalUuid(),
    };
  }
  optionalReconcileSubscription(): ReconcileSubscription | null {
    return this.u8() === 1 ? this.reconcileSubscription() : null;
  }
  reconcileSubscriptions(): ReconcileSubscription[] {
    const n = this.u32();
    const subs: ReconcileSubscription[] = [];
    for (let i = 0; i < n; i += 1) {
      subs.push(this.reconcileSubscription());
    }
    return subs;
  }
  expectMagic(m: string): void {
    const got = this.raw(4);
    const want = textEncoder.encode(m);
    for (let i = 0; i < 4; i += 1) {
      if (got[i] !== want[i]) throw new Error(`wire: bad magic, expected ${m}`);
    }
  }
  remaining(): number {
    return this.input.length - this.cursor;
  }
  finish(): void {
    if (this.cursor !== this.input.length) {
      throw new Error(`wire: ${this.input.length - this.cursor} trailing byte(s)`);
    }
  }
}

// ---- handshake frame bodies (magic + fields), matching wire.rs ----

export interface Hello {
  clientName: string;
  clientVersion: string;
  protocolVersion: number;
  resume?: ResumeIdentity | null;
}

export function encodeHelloBody(hello: Hello): Uint8Array {
  const w = new Writer();
  w.magic("FHL1");
  w.str(hello.clientName);
  w.str(hello.clientVersion);
  w.u16(hello.protocolVersion);
  w.optionalResumeIdentity(hello.resume);
  return w.finish();
}

export function decodeHelloBody(body: Uint8Array): Hello {
  const r = new Reader(body);
  r.expectMagic("FHL1");
  const hello: Hello = {
    clientName: r.str(),
    clientVersion: r.str(),
    protocolVersion: r.u16(),
    resume: r.optionalResumeIdentity(),
  };
  r.finish();
  return hello;
}

export interface HelloOk {
  protocolVersion: number;
  ownerId: Uuid;
  clientId: Uuid;
  resumeToken: Uuid;
  resumeOutcome: ResumeOutcome;
  serverName: string;
  compliance: string;
}

export function encodeHelloOkBody(hello: HelloOk): Uint8Array {
  const w = new Writer();
  w.magic("FHO1");
  w.u16(hello.protocolVersion);
  w.uuid(hello.ownerId);
  w.uuid(hello.clientId);
  w.uuid(hello.resumeToken);
  w.resumeOutcome(hello.resumeOutcome);
  w.str(hello.serverName);
  w.str(hello.compliance);
  return w.finish();
}

export function decodeHelloOkBody(body: Uint8Array): HelloOk {
  const r = new Reader(body);
  r.expectMagic("FHO1");
  const hello: HelloOk = {
    protocolVersion: r.u16(),
    ownerId: r.uuid(),
    clientId: r.uuid(),
    resumeToken: r.uuid(),
    resumeOutcome: r.resumeOutcome(),
    serverName: r.str(),
    compliance: r.str(),
  };
  r.finish();
  return hello;
}

export interface Auth {
  username: string;
  password: string;
}

export function encodeAuthBody(auth: Auth): Uint8Array {
  const w = new Writer();
  w.magic("FAU1");
  w.str(auth.username);
  w.str(auth.password);
  return w.finish();
}

export function decodeAuthBody(body: Uint8Array): Auth {
  const r = new Reader(body);
  r.expectMagic("FAU1");
  const auth: Auth = { username: r.str(), password: r.str() };
  r.finish();
  return auth;
}

export interface ErrorMsg {
  code: number;
  message: string;
}

export function encodeErrorBody(error: ErrorMsg): Uint8Array {
  const w = new Writer();
  w.magic("FER1");
  w.u16(error.code);
  w.str(error.message);
  return w.finish();
}

export function decodeErrorBody(body: Uint8Array): ErrorMsg {
  const r = new Reader(body);
  r.expectMagic("FER1");
  const error: ErrorMsg = { code: r.u16(), message: r.str() };
  r.finish();
  return error;
}

// ---- shared field types ----

export type Headers = Record<string, string>;

/** Frame content type. `null` = unspecified; `{ custom }` = an arbitrary MIME. */
export type ContentType = null | "msgpack" | "json" | "text" | { custom: string };

export interface DeliveryTag {
  epoch: bigint;
}

/** Dead-letter policy on a queue declaration. */
export type QueueDlqPolicy =
  | "discard"
  | "global"
  | { custom: { topic: string; group: string | null } };

// ---- publish ----

export interface Publish {
  topic: string;
  partition: number;
  group: string | null;
  requireConfirm: boolean;
  contentType: ContentType;
  headers: Headers;
  payload: Uint8Array;
  published: bigint;
  partitionKey: Uint8Array | null;
  partitioningVersion: bigint;
  /** Optional message TTL in milliseconds (relative to publish time). */
  ttlMs: bigint | null;
}

function writePublishCommon(w: Writer, p: Publish): void {
  w.str(p.topic);
  w.optionalStr(p.group);
  w.u32(p.partition);
  w.bool(p.requireConfirm);
  w.contentType(p.contentType);
  w.headers(p.headers);
  w.u64(p.published);
  w.optionalBytes(p.partitionKey);
  w.u64(p.partitioningVersion);
  w.bytes(p.payload);
  // Trailing so a peer that omits it still decodes (read as null).
  w.optionalU64(p.ttlMs);
}

function readPublishCommon(r: Reader): Publish {
  return {
    topic: r.str(),
    group: r.optionalStr(),
    partition: r.u32(),
    requireConfirm: r.bool(),
    contentType: r.contentType(),
    headers: r.headers(),
    published: r.u64(),
    partitionKey: r.optionalBytes(),
    partitioningVersion: r.u64(),
    payload: r.bytes(),
    // Trailing optional: absent when the peer has not been updated to send it.
    ttlMs: r.remaining() > 0 ? r.optionalU64() : null,
  };
}

export function encodePublishBody(p: Publish): Uint8Array {
  const w = new Writer();
  w.magic("FPB1");
  writePublishCommon(w, p);
  return w.finish();
}

export function decodePublishBody(body: Uint8Array): Publish {
  const r = new Reader(body);
  r.expectMagic("FPB1");
  const value = readPublishCommon(r);
  r.finish();
  return value;
}

export interface PublishDelayed {
  topic: string;
  partition: number;
  group: string | null;
  requireConfirm: boolean;
  notBefore: bigint;
  contentType: ContentType;
  headers: Headers;
  payload: Uint8Array;
  published: bigint;
  partitionKey: Uint8Array | null;
  partitioningVersion: bigint;
}

export function encodePublishDelayedBody(p: PublishDelayed): Uint8Array {
  const w = new Writer();
  w.magic("FPD1");
  w.str(p.topic);
  w.optionalStr(p.group);
  w.u32(p.partition);
  w.bool(p.requireConfirm);
  w.u64(p.notBefore);
  w.contentType(p.contentType);
  w.headers(p.headers);
  w.u64(p.published);
  w.optionalBytes(p.partitionKey);
  w.u64(p.partitioningVersion);
  w.bytes(p.payload);
  return w.finish();
}

export function decodePublishDelayedBody(body: Uint8Array): PublishDelayed {
  const r = new Reader(body);
  r.expectMagic("FPD1");
  const value: PublishDelayed = {
    topic: r.str(),
    group: r.optionalStr(),
    partition: r.u32(),
    requireConfirm: r.bool(),
    notBefore: r.u64(),
    contentType: r.contentType(),
    headers: r.headers(),
    published: r.u64(),
    partitionKey: r.optionalBytes(),
    partitioningVersion: r.u64(),
    payload: r.bytes(),
  };
  r.finish();
  return value;
}

export interface PublishOk {
  offset: bigint;
}

export function encodePublishOkBody(ok: PublishOk): Uint8Array {
  const w = new Writer();
  w.magic("FPO1");
  w.u64(ok.offset);
  return w.finish();
}

export function decodePublishOkBody(body: Uint8Array): PublishOk {
  const r = new Reader(body);
  r.expectMagic("FPO1");
  const offset = r.u64();
  r.finish();
  return { offset };
}

// ---- deliver ----

export interface Deliver {
  subId: bigint;
  topic: string;
  group: string | null;
  partition: number;
  offset: bigint;
  deliveryTag: DeliveryTag;
  published: bigint;
  publishReceived: bigint;
  contentType: ContentType;
  headers: Headers;
  payload: Uint8Array;
}

export function encodeDeliverBody(d: Deliver): Uint8Array {
  const w = new Writer();
  w.magic("FDL1");
  w.u64(d.subId);
  w.str(d.topic);
  w.optionalStr(d.group);
  w.u32(d.partition);
  w.u64(d.offset);
  w.u64(d.deliveryTag.epoch);
  w.u64(d.published);
  w.u64(d.publishReceived);
  w.contentType(d.contentType);
  w.headers(d.headers);
  w.bytes(d.payload);
  return w.finish();
}

export function decodeDeliverBody(body: Uint8Array): Deliver {
  const r = new Reader(body);
  r.expectMagic("FDL1");
  const value: Deliver = {
    subId: r.u64(),
    topic: r.str(),
    group: r.optionalStr(),
    partition: r.u32(),
    offset: r.u64(),
    deliveryTag: { epoch: r.u64() },
    published: r.u64(),
    publishReceived: r.u64(),
    contentType: r.contentType(),
    headers: r.headers(),
    payload: r.bytes(),
  };
  r.finish();
  return value;
}

// ---- settle (ack / nack) ----

export interface Ack {
  topic: string;
  group: string | null;
  partition: number;
  tags: DeliveryTag[];
}

export function encodeAckBody(ack: Ack): Uint8Array {
  const w = new Writer();
  w.magic("FAK1");
  w.str(ack.topic);
  w.optionalStr(ack.group);
  w.u32(ack.partition);
  w.settleTags(ack.tags);
  return w.finish();
}

export function decodeAckBody(body: Uint8Array): Ack {
  const r = new Reader(body);
  r.expectMagic("FAK1");
  const value: Ack = {
    topic: r.str(),
    group: r.optionalStr(),
    partition: r.u32(),
    tags: r.settleTags(),
  };
  r.finish();
  return value;
}

export interface Nack {
  topic: string;
  group: string | null;
  partition: number;
  tags: DeliveryTag[];
  requeue: boolean;
  notBefore: bigint | null;
}

export function encodeNackBody(nack: Nack): Uint8Array {
  const w = new Writer();
  w.magic("FNK1");
  w.str(nack.topic);
  w.optionalStr(nack.group);
  w.u32(nack.partition);
  w.settleTags(nack.tags);
  w.bool(nack.requeue);
  w.optionalU64(nack.notBefore);
  return w.finish();
}

export function decodeNackBody(body: Uint8Array): Nack {
  const r = new Reader(body);
  r.expectMagic("FNK1");
  const value: Nack = {
    topic: r.str(),
    group: r.optionalStr(),
    partition: r.u32(),
    tags: r.settleTags(),
    requeue: r.bool(),
    notBefore: r.optionalU64(),
  };
  r.finish();
  return value;
}

// ---- declare queue ----

export interface DeclareQueue {
  topic: string;
  group: string | null;
  dlqPolicy: QueueDlqPolicy | null;
  dlqMaxRetries: number | null;
  partitionCount: number | null;
  /** Default message TTL (ms) for the queue. Not queue expiration. */
  defaultMessageTtlMs: bigint | null;
}

export function encodeDeclareQueueBody(d: DeclareQueue): Uint8Array {
  const w = new Writer();
  w.magic("FDQ1");
  w.str(d.topic);
  w.optionalStr(d.group);
  w.optionalDlqPolicy(d.dlqPolicy);
  w.optionalU32(d.dlqMaxRetries);
  w.optionalU32(d.partitionCount);
  // Trailing so a peer that omits it still decodes (read as null).
  w.optionalU64(d.defaultMessageTtlMs);
  return w.finish();
}

export function decodeDeclareQueueBody(body: Uint8Array): DeclareQueue {
  const r = new Reader(body);
  r.expectMagic("FDQ1");
  const value: DeclareQueue = {
    topic: r.str(),
    group: r.optionalStr(),
    dlqPolicy: r.optionalDlqPolicy(),
    dlqMaxRetries: r.optionalU32(),
    partitionCount: r.optionalU32(),
    defaultMessageTtlMs: r.remaining() > 0 ? r.optionalU64() : null,
  };
  r.finish();
  return value;
}

export interface DeclareQueueOk {
  status: string;
  partitionCount: number;
}

export function encodeDeclareQueueOkBody(ok: DeclareQueueOk): Uint8Array {
  const w = new Writer();
  w.magic("FDK1");
  w.str(ok.status);
  w.u32(ok.partitionCount);
  return w.finish();
}

export function decodeDeclareQueueOkBody(body: Uint8Array): DeclareQueueOk {
  const r = new Reader(body);
  r.expectMagic("FDK1");
  const value: DeclareQueueOk = { status: r.str(), partitionCount: r.u32() };
  r.finish();
  return value;
}

// ---- subscribe ----

export interface Subscribe {
  topic: string;
  partition: number;
  group: string | null;
  prefetch: number;
  autoAck: boolean;
  consumerGroup: string | null;
  consumerTarget: number | null;
  memberId: Uuid | null;
}

export function encodeSubscribeBody(s: Subscribe): Uint8Array {
  const w = new Writer();
  w.magic("FSB1");
  w.queueKey(s.topic, s.partition, s.group);
  w.u32(s.prefetch);
  w.bool(s.autoAck);
  w.optionalStr(s.consumerGroup);
  w.optionalU32(s.consumerTarget);
  w.optionalUuid(s.memberId);
  return w.finish();
}

export function decodeSubscribeBody(body: Uint8Array): Subscribe {
  const r = new Reader(body);
  r.expectMagic("FSB1");
  const key = r.queueKey();
  const value: Subscribe = {
    topic: key.topic,
    partition: key.partition,
    group: key.group,
    prefetch: r.u32(),
    autoAck: r.bool(),
    consumerGroup: r.optionalStr(),
    consumerTarget: r.optionalU32(),
    memberId: r.optionalUuid(),
  };
  r.finish();
  return value;
}

export interface SubscribeOk {
  subId: bigint;
  topic: string;
  partition: number;
  group: string | null;
  prefetch: number;
  consumerGroup: string | null;
  consumerTarget: number | null;
  memberId: Uuid | null;
}

export function encodeSubscribeOkBody(s: SubscribeOk): Uint8Array {
  const w = new Writer();
  w.magic("FSO1");
  w.u64(s.subId);
  w.queueKey(s.topic, s.partition, s.group);
  w.u32(s.prefetch);
  w.optionalStr(s.consumerGroup);
  w.optionalU32(s.consumerTarget);
  w.optionalUuid(s.memberId);
  return w.finish();
}

export function decodeSubscribeOkBody(body: Uint8Array): SubscribeOk {
  const r = new Reader(body);
  r.expectMagic("FSO1");
  const subId = r.u64();
  const key = r.queueKey();
  const value: SubscribeOk = {
    subId,
    topic: key.topic,
    partition: key.partition,
    group: key.group,
    prefetch: r.u32(),
    consumerGroup: r.optionalStr(),
    consumerTarget: r.optionalU32(),
    memberId: r.optionalUuid(),
  };
  r.finish();
  return value;
}

// ---- reconcile ----

export interface ReconcileSubscription {
  subId: bigint;
  topic: string;
  partition: number;
  group: string | null;
  autoAck: boolean;
  prefetch: number;
  consumerGroup: string | null;
  consumerTarget: number | null;
  memberId: Uuid | null;
}

export interface ReconcileSubscriptionResult {
  client: ReconcileSubscription | null;
  server: ReconcileSubscription | null;
  action: ReconcileAction;
  reason: string;
}

export interface ReconcileClient {
  policy: ReconcilePolicy;
  subscriptions: ReconcileSubscription[];
}

export interface ReconcileServer {
  subscriptions: ReconcileSubscription[];
}

export interface ReconcileResult {
  subscriptions: ReconcileSubscriptionResult[];
}

export function encodeReconcileClientBody(rc: ReconcileClient): Uint8Array {
  const w = new Writer();
  w.magic("FRC1");
  w.reconcilePolicy(rc.policy);
  w.reconcileSubscriptions(rc.subscriptions);
  return w.finish();
}

export function decodeReconcileClientBody(body: Uint8Array): ReconcileClient {
  const r = new Reader(body);
  r.expectMagic("FRC1");
  const value: ReconcileClient = {
    policy: r.reconcilePolicy(),
    subscriptions: r.reconcileSubscriptions(),
  };
  r.finish();
  return value;
}

export function encodeReconcileServerBody(rs: ReconcileServer): Uint8Array {
  const w = new Writer();
  w.magic("FRS1");
  w.reconcileSubscriptions(rs.subscriptions);
  return w.finish();
}

export function decodeReconcileServerBody(body: Uint8Array): ReconcileServer {
  const r = new Reader(body);
  r.expectMagic("FRS1");
  const value: ReconcileServer = { subscriptions: r.reconcileSubscriptions() };
  r.finish();
  return value;
}

export function encodeReconcileResultBody(rr: ReconcileResult): Uint8Array {
  const w = new Writer();
  w.magic("FRR1");
  w.u32(rr.subscriptions.length);
  for (const s of rr.subscriptions) {
    w.optionalReconcileSubscription(s.client);
    w.optionalReconcileSubscription(s.server);
    w.reconcileAction(s.action);
    w.str(s.reason);
  }
  return w.finish();
}

export function decodeReconcileResultBody(body: Uint8Array): ReconcileResult {
  const r = new Reader(body);
  r.expectMagic("FRR1");
  const n = r.u32();
  const subscriptions: ReconcileSubscriptionResult[] = [];
  for (let i = 0; i < n; i += 1) {
    subscriptions.push({
      client: r.optionalReconcileSubscription(),
      server: r.optionalReconcileSubscription(),
      action: r.reconcileAction(),
      reason: r.str(),
    });
  }
  r.finish();
  return { subscriptions };
}

// ---- topology + redirect (cluster routing) ----

export interface TopologyRequest {
  topic: string | null;
  group: string | null;
}

/** One queue partition's ownership as seen by clients for routing. */
export interface QueueTopologyEntry {
  topic: string;
  partition: number;
  group: string | null;
  // Owner broker endpoint, absent when the owner node is not in the registry.
  ownerEndpoint: string | null;
  partitioningVersion: bigint;
  // Authoritative partition count for the queue, used for key routing.
  partitionCount: number;
}

export interface TopologyOk {
  generation: bigint;
  queues: QueueTopologyEntry[];
}

/**
 * Routing redirect: not an error but a target to retry against. It must be
 * retried on a connection to ownerEndpoint, so the routing layer handles it.
 */
export interface Redirect {
  topic: string;
  partition: number;
  group: string | null;
  ownerEndpoint: string;
  partitioningVersion: bigint;
}

export function encodeTopologyRequestBody(req: TopologyRequest): Uint8Array {
  const w = new Writer();
  w.magic("FTP1");
  w.optionalStr(req.topic);
  w.optionalStr(req.group);
  return w.finish();
}

export function decodeTopologyRequestBody(body: Uint8Array): TopologyRequest {
  const r = new Reader(body);
  r.expectMagic("FTP1");
  const req: TopologyRequest = { topic: r.optionalStr(), group: r.optionalStr() };
  r.finish();
  return req;
}

export function encodeTopologyOkBody(topology: TopologyOk): Uint8Array {
  const w = new Writer();
  w.magic("FTO1");
  w.u64(topology.generation);
  w.u32(topology.queues.length);
  for (const e of topology.queues) {
    w.queueKey(e.topic, e.partition, e.group);
    w.optionalStr(e.ownerEndpoint);
    w.u64(e.partitioningVersion);
    w.u32(e.partitionCount);
  }
  return w.finish();
}

export function decodeTopologyOkBody(body: Uint8Array): TopologyOk {
  const r = new Reader(body);
  r.expectMagic("FTO1");
  const generation = r.u64();
  const n = r.u32();
  const queues: QueueTopologyEntry[] = [];
  for (let i = 0; i < n; i += 1) {
    const key = r.queueKey();
    queues.push({
      topic: key.topic,
      partition: key.partition,
      group: key.group,
      ownerEndpoint: r.optionalStr(),
      partitioningVersion: r.u64(),
      partitionCount: r.u32(),
    });
  }
  r.finish();
  return { generation, queues };
}

export function encodeRedirectBody(redirect: Redirect): Uint8Array {
  const w = new Writer();
  w.magic("FRD1");
  w.queueKey(redirect.topic, redirect.partition, redirect.group);
  w.str(redirect.ownerEndpoint);
  w.u64(redirect.partitioningVersion);
  return w.finish();
}

export function decodeRedirectBody(body: Uint8Array): Redirect {
  const r = new Reader(body);
  r.expectMagic("FRD1");
  const key = r.queueKey();
  const redirect: Redirect = {
    topic: key.topic,
    partition: key.partition,
    group: key.group,
    ownerEndpoint: r.str(),
    partitioningVersion: r.u64(),
  };
  r.finish();
  return redirect;
}
