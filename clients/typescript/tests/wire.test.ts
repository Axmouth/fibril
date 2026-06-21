import { test } from "node:test";
import assert from "node:assert/strict";

import {
  Reader,
  Writer,
  encodeHelloBody,
  decodeHelloBody,
  encodeHelloOkBody,
  decodeHelloOkBody,
  encodeAuthBody,
  decodeAuthBody,
  encodeErrorBody,
  decodeErrorBody,
  type ResumeIdentity,
} from "../src/wire.js";

const uuid = (fill: number): Uint8Array => new Uint8Array(16).fill(fill);

test("primitives round-trip", () => {
  const w = new Writer();
  w.u8(0xab);
  w.u16(0x1234);
  w.u32(0xdeadbeef);
  w.u64(0x0102030405060708n);
  w.str("héllo 🌍");
  w.bool(true);
  w.bool(false);
  w.uuid(uuid(7));
  w.optionalStr(null);
  w.optionalStr("present");
  w.optionalUuid(uuid(9));
  w.optionalUuid(null);

  const r = new Reader(w.finish());
  assert.equal(r.u8(), 0xab);
  assert.equal(r.u16(), 0x1234);
  assert.equal(r.u32(), 0xdeadbeef);
  assert.equal(r.u64(), 0x0102030405060708n);
  assert.equal(r.str(), "héllo 🌍");
  assert.equal(r.bool(), true);
  assert.equal(r.bool(), false);
  assert.deepEqual(r.uuid(), uuid(7));
  assert.equal(r.optionalStr(), null);
  assert.equal(r.optionalStr(), "present");
  assert.deepEqual(r.optionalUuid(), uuid(9));
  assert.equal(r.optionalUuid(), null);
  r.finish();
});

test("big-endian byte layout matches the wire spec", () => {
  const w = new Writer();
  w.u32(0x01020304);
  assert.deepEqual(w.finish(), new Uint8Array([0x01, 0x02, 0x03, 0x04]));

  const s = new Writer();
  s.str("AB"); // u32 len = 2, then 'A','B'
  assert.deepEqual(s.finish(), new Uint8Array([0, 0, 0, 2, 0x41, 0x42]));
});

test("Reader.finish rejects trailing bytes", () => {
  const w = new Writer();
  w.u8(1);
  w.u8(2);
  const r = new Reader(w.finish());
  assert.equal(r.u8(), 1);
  assert.throws(() => r.finish(), /trailing/);
});

test("hello body round-trips with and without resume", () => {
  const noResume = { clientName: "fibril-ts", clientVersion: "0.1.0", protocolVersion: 1 };
  assert.deepEqual(decodeHelloBody(encodeHelloBody(noResume)), {
    ...noResume,
    resume: null,
  });

  const resume: ResumeIdentity = { ownerId: uuid(1), clientId: uuid(2), resumeToken: uuid(3) };
  const withResume = { clientName: "c", clientVersion: "v", protocolVersion: 1, resume };
  assert.deepEqual(decodeHelloBody(encodeHelloBody(withResume)), withResume);
});

test("hello_ok body round-trips across resume outcomes", () => {
  for (const outcome of ["new", "resumed", "resume_not_found", "resume_rejected"] as const) {
    const hello = {
      protocolVersion: 1,
      ownerId: uuid(4),
      clientId: uuid(5),
      resumeToken: uuid(6),
      resumeOutcome: outcome,
      serverName: "fibril",
      compliance: "ok",
    };
    assert.deepEqual(decodeHelloOkBody(encodeHelloOkBody(hello)), hello);
  }
});

test("auth and error bodies round-trip", () => {
  const auth = { username: "fibril", password: "secret" };
  assert.deepEqual(decodeAuthBody(encodeAuthBody(auth)), auth);

  const err = { code: 409, message: "not the owner" };
  assert.deepEqual(decodeErrorBody(encodeErrorBody(err)), err);
});

test("magic mismatch is rejected", () => {
  const body = encodeAuthBody({ username: "a", password: "b" });
  assert.throws(() => decodeHelloBody(body), /magic/);
});
