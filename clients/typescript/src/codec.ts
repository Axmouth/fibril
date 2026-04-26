import { encode as msgpackEncode, decode as msgpackDecode } from "@msgpack/msgpack";
import type { Op } from "./protocol.js";
import { PROTOCOL_V1 } from "./protocol.js";

// Wire frame layout (big-endian):
//   u32 payload_len
//   u16 version
//   u16 opcode
//   u32 flags
//   u64 request_id
//   bytes payload[payload_len]
// Total header size: 20 bytes.

export const HEADER_SIZE = 20;

export interface Frame {
  version: number;
  opcode: number;
  flags: number;
  requestId: bigint;
  payload: Uint8Array;
}

/**
 * Serialize a value as msgpack with map/named-field encoding for objects
 * and array encoding for sequences. Matches `rmp_serde::to_vec_named`
 * for protocol frames.
 *
 * NOTE: `@msgpack/msgpack` encodes plain JS objects as maps with string
 * keys and arrays as arrays. That matches `to_vec_named` for the
 * struct-with-named-fields case. User payloads in the Rust client are
 * encoded with `to_vec` (array form for structs); for TS we only
 * serialize protocol frames here, so map encoding is correct.
 */
export function encodeMsgpack(value: unknown): Uint8Array {
  // useBigInt64 ensures bigint values round-trip as msgpack int64.
  return msgpackEncode(value, { useBigInt64: true });
}

/**
 * Deserialize msgpack bytes. Configured to produce bigints for 64-bit
 * integers (matching wire u64 fields like sub_id, offset, delivery_tag.epoch).
 */
export function decodeMsgpack<T>(bytes: Uint8Array): T {
  return msgpackDecode(bytes, { useBigInt64: true }) as T;
}

/**
 * Build a frame for a given opcode and payload value. The payload is
 * msgpack-encoded with map/named-field encoding.
 */
export function buildFrame(op: Op, requestId: bigint, payload: unknown): Frame {
  const bytes =
    payload === null || payload === undefined
      ? new Uint8Array(0)
      : encodeMsgpack(payload);
  return {
    version: PROTOCOL_V1,
    opcode: op,
    flags: 0,
    requestId,
    payload: bytes,
  };
}

/**
 * Encode a frame to its on-wire byte representation.
 */
export function encodeFrame(frame: Frame): Uint8Array {
  const out = new Uint8Array(HEADER_SIZE + frame.payload.byteLength);
  const view = new DataView(out.buffer, out.byteOffset, out.byteLength);
  view.setUint32(0, frame.payload.byteLength, false);
  view.setUint16(4, frame.version, false);
  view.setUint16(6, frame.opcode, false);
  view.setUint32(8, frame.flags, false);
  view.setBigUint64(12, frame.requestId, false);
  out.set(frame.payload, HEADER_SIZE);
  return out;
}

/**
 * Try to decode one frame from the head of a buffer.
 * Returns `null` if not enough bytes are available yet.
 *
 * On success returns the frame and the number of bytes consumed.
 */
export function tryDecodeFrame(
  buf: Uint8Array,
): { frame: Frame; consumed: number } | null {
  if (buf.byteLength < HEADER_SIZE) return null;

  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const payloadLen = view.getUint32(0, false);
  const totalLen = HEADER_SIZE + payloadLen;
  if (buf.byteLength < totalLen) return null;

  const version = view.getUint16(4, false);
  const opcode = view.getUint16(6, false);
  const flags = view.getUint32(8, false);
  const requestId = view.getBigUint64(12, false);

  // Slice owns its own bytes view; copy to detach from the input buffer
  // so the caller can free/reuse `buf` safely.
  const payload = new Uint8Array(payloadLen);
  payload.set(buf.subarray(HEADER_SIZE, totalLen));

  return {
    frame: { version, opcode, flags, requestId, payload },
    consumed: totalLen,
  };
}

/**
 * Decode the typed body of a frame.
 */
export function decodeFrameBody<T>(frame: Frame): T {
  return decodeMsgpack<T>(frame.payload);
}
