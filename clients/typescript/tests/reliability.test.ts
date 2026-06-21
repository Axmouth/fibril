import { test } from "node:test";
import assert from "node:assert/strict";

import {
  BrokenPipeError,
  DeserializationError,
  DisconnectionError,
  EofError,
  RedirectError,
  SerializationError,
  ServerError,
  UnexpectedError,
  isRetryable,
  retryAdvice,
} from "../src/errors.js";
import { NewMessage, isReservedHeaderKey } from "../src/message.js";

test("retryAdvice classifies errors intuitively", () => {
  const redirect = new RedirectError({
    topic: "t",
    partition: 0,
    group: null,
    owner_endpoint: "127.0.0.1:1",
    partitioning_version: 0n,
  });
  // Transport, redirect, topology conflict (409), and server-transient (5xx) retry.
  for (const err of [
    new DisconnectionError("x"),
    new BrokenPipeError(),
    new EofError(),
    redirect,
    new ServerError(409, "owner moved"),
    new ServerError(503, "busy"),
  ]) {
    assert.equal(retryAdvice(err), "retry", err.name);
    assert.equal(isRetryable(err), true, err.name);
  }
  // Not-found, invalid, and local request errors do not retry.
  for (const err of [
    new ServerError(404, "gone"),
    new ServerError(400, "bad"),
    new DeserializationError("x"),
    new SerializationError("x"),
    new UnexpectedError("x"),
  ]) {
    assert.equal(retryAdvice(err), "do_not_retry", err.name);
    assert.equal(isRetryable(err), false, err.name);
  }
});

test("reserved header keys are guarded; the carve-out is recognized", () => {
  assert.equal(isReservedHeaderKey("fibril.client.producer_id"), true);
  assert.equal(isReservedHeaderKey("stroma.region"), true);
  assert.equal(isReservedHeaderKey("x-trace"), false);

  // User code cannot set reserved keys.
  assert.throws(() => NewMessage.json({}).header("fibril.foo", "v"), /reserved/);
  assert.throws(() => NewMessage.json({}).header("stroma.bar", "v"), /reserved/);

  // Ordinary headers are fine.
  const m = NewMessage.json({}).header("x-trace", "abc");
  assert.equal(m.headers["x-trace"], "abc");
});
