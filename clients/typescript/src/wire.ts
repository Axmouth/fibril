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
  expectMagic(m: string): void {
    const got = this.raw(4);
    const want = textEncoder.encode(m);
    for (let i = 0; i < 4; i += 1) {
      if (got[i] !== want[i]) throw new Error(`wire: bad magic, expected ${m}`);
    }
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
