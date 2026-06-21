import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildFrame,
  decodeFrameBody,
  encodeFrame,
  HEADER_SIZE,
  tryDecodeFrame,
} from "../src/codec.js";
import { Op, PROTOCOL_V1, type Hello } from "../src/protocol.js";

// These tests cover the 20-byte frame header and stream framing. Body encoding
// is exercised against real protocol structs (the only thing the wire codec
// accepts) rather than ad-hoc objects.

const hello = (name = "test"): Hello => ({
  client_name: name,
  client_version: "0.0.1",
  protocol_version: PROTOCOL_V1,
  resume: null,
});

test("frame round-trip carries header fields and body", () => {
  const original = buildFrame(Op.Hello, 42n, hello());

  const bytes = encodeFrame(original);
  assert.equal(bytes.byteLength, HEADER_SIZE + original.payload.byteLength);

  const decoded = tryDecodeFrame(bytes);
  assert.ok(decoded);
  assert.equal(decoded.consumed, bytes.byteLength);
  assert.equal(decoded.frame.opcode, Op.Hello);
  assert.equal(decoded.frame.requestId, 42n);
  assert.equal(decoded.frame.version, PROTOCOL_V1);
  assert.equal(decoded.frame.flags, 0);

  const body = decodeFrameBody<Hello>(decoded.frame);
  assert.equal(body.client_name, "test");
  assert.equal(body.client_version, "0.0.1");
  assert.equal(body.protocol_version, PROTOCOL_V1);
});

test("frame with empty payload", () => {
  const original = buildFrame(Op.Ping, 7n, null);
  assert.equal(original.payload.byteLength, 0);

  const bytes = encodeFrame(original);
  assert.equal(bytes.byteLength, HEADER_SIZE);

  const decoded = tryDecodeFrame(bytes);
  assert.ok(decoded);
  assert.equal(decoded.frame.opcode, Op.Ping);
  assert.equal(decoded.frame.payload.byteLength, 0);
});

test("partial decode returns null", () => {
  const frame = buildFrame(Op.Hello, 1n, hello());
  const bytes = encodeFrame(frame);

  // Less than header
  assert.equal(tryDecodeFrame(bytes.subarray(0, 5)), null);
  // Header but partial payload
  assert.equal(tryDecodeFrame(bytes.subarray(0, HEADER_SIZE + 1)), null);
  // Exactly header but payload still due
  assert.equal(tryDecodeFrame(bytes.subarray(0, HEADER_SIZE)), null);
});

test("decode handles two frames concatenated", () => {
  const a = buildFrame(Op.Ping, 1n, null);
  const b = buildFrame(Op.Pong, 2n, null);
  const merged = new Uint8Array(encodeFrame(a).byteLength + encodeFrame(b).byteLength);
  merged.set(encodeFrame(a), 0);
  merged.set(encodeFrame(b), encodeFrame(a).byteLength);

  const first = tryDecodeFrame(merged);
  assert.ok(first);
  assert.equal(first.frame.opcode, Op.Ping);

  const second = tryDecodeFrame(merged.subarray(first.consumed));
  assert.ok(second);
  assert.equal(second.frame.opcode, Op.Pong);
});

test("u64 body field round-trips near the max value", () => {
  const big = 0xfffffffffffffffen; // u64 near max
  const frame = buildFrame(Op.PublishOk, 99n, { offset: big });
  const bytes = encodeFrame(frame);
  const decoded = tryDecodeFrame(bytes);
  assert.ok(decoded);
  const body = decodeFrameBody<{ offset: bigint }>(decoded.frame);
  assert.equal(body.offset, big);
  assert.equal(typeof body.offset, "bigint");
});

test("request id big-endian encoding", () => {
  const frame = buildFrame(Op.Ping, 0x0102030405060708n, null);
  const bytes = encodeFrame(frame);
  // Bytes 12..20 should be the request id, BE.
  assert.equal(bytes[12], 0x01);
  assert.equal(bytes[13], 0x02);
  assert.equal(bytes[14], 0x03);
  assert.equal(bytes[15], 0x04);
  assert.equal(bytes[16], 0x05);
  assert.equal(bytes[17], 0x06);
  assert.equal(bytes[18], 0x07);
  assert.equal(bytes[19], 0x08);
});

test("payload length big-endian", () => {
  // A 300-char client_name forces a payload over 256 bytes.
  const frame = buildFrame(Op.Hello, 1n, hello("x".repeat(300)));
  const bytes = encodeFrame(frame);
  const len = (bytes[0]! << 24) | (bytes[1]! << 16) | (bytes[2]! << 8) | bytes[3]!;
  assert.equal(len, frame.payload.byteLength);
  assert.ok(len > 256);
});
