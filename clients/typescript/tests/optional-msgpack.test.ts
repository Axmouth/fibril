import { test } from "node:test";
import assert from "node:assert/strict";

import { encodeMsgpack, decodeMsgpack } from "../src/codec.js";
import { NewMessage } from "../src/message.js";

// msgpack is an optional dependency loaded lazily (createRequire). When present,
// the codec must round-trip; the raw/text paths must not touch it at all.

test("msgpack codec lazily loads and round-trips (bigints included)", () => {
  const value = { id: 1n, name: "x", flag: true };
  const bytes = encodeMsgpack(value);
  assert.deepEqual(decodeMsgpack(bytes), value);
});

test("raw and text messages need no msgpack", () => {
  assert.deepEqual(NewMessage.raw(new Uint8Array([1, 2, 3])).payload, new Uint8Array([1, 2, 3]));
  const text = NewMessage.content("hi");
  assert.equal(new TextDecoder().decode(text.payload), "hi");
});
