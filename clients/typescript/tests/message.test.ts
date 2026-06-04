import { test } from "node:test";
import assert from "node:assert/strict";
import {
  deserializeByContentType,
  NewMessage,
} from "../src/message.js";
import { DeserializationError } from "../src/errors.js";

test("NewMessage msgpack sets content type and deserializes by default", () => {
  const message = NewMessage.msgpack({ value: 7 });

  assert.equal(message.headers["content-type"], "application/msgpack");
  assert.deepEqual(
    deserializeByContentType<{ value: number }>(undefined, message.payload),
    { value: 7 },
  );
});

test("NewMessage json sets content type and deserializes as json", () => {
  const message = NewMessage.json({ ok: true });

  assert.equal(message.headers["content-type"], "application/json");
  assert.deepEqual(
    deserializeByContentType<{ ok: boolean }>(
      message.headers["content-type"],
      message.payload,
    ),
    { ok: true },
  );
});

test("NewMessage raw has no implicit headers", () => {
  const message = NewMessage.raw(new Uint8Array([1, 2, 3]));

  assert.deepEqual(message.headers, {});
  assert.deepEqual([...message.payload], [1, 2, 3]);
});

test("unsupported content type fails explicitly", () => {
  assert.throws(
    () => deserializeByContentType("application/custom", new Uint8Array()),
    DeserializationError,
  );
});
