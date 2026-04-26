import { test } from "node:test";
import assert from "node:assert/strict";
import { BoundedQueue } from "../src/internal/bounded-queue.js";

test("send and recv in order", async () => {
  const q = new BoundedQueue<number>(4);
  await q.send(1);
  await q.send(2);
  await q.send(3);
  assert.equal(await q.recv(), 1);
  assert.equal(await q.recv(), 2);
  assert.equal(await q.recv(), 3);
});

test("recv waits for send", async () => {
  const q = new BoundedQueue<number>(4);
  const recvP = q.recv();
  await q.send(42);
  assert.equal(await recvP, 42);
});

test("send blocks when full and unblocks on recv", async () => {
  const q = new BoundedQueue<number>(2);
  await q.send(1);
  await q.send(2);

  let sent = false;
  const sendP = q.send(3).then(() => {
    sent = true;
  });

  // Give the event loop a chance to run; sendP should still be pending.
  await new Promise((r) => setImmediate(r));
  assert.equal(sent, false);

  assert.equal(await q.recv(), 1);
  await sendP;
  assert.equal(sent, true);
  assert.equal(await q.recv(), 2);
  assert.equal(await q.recv(), 3);
});

test("close resolves recv with null after drain", async () => {
  const q = new BoundedQueue<number>(2);
  await q.send(1);
  q.close();
  assert.equal(await q.recv(), 1);
  assert.equal(await q.recv(), null);
});

test("close rejects pending senders", async () => {
  const q = new BoundedQueue<number>(1);
  await q.send(1);
  const sendP = q.send(2);
  q.close(new Error("byebye"));
  await assert.rejects(sendP, /byebye/);
});

test("close terminates pending recv if buffer empty", async () => {
  const q = new BoundedQueue<number>(2);
  const recvP = q.recv();
  q.close();
  assert.equal(await recvP, null);
});

test("async iteration", async () => {
  const q = new BoundedQueue<number>(4);
  const collected: number[] = [];
  const consumer = (async () => {
    for await (const v of q) collected.push(v);
  })();

  await q.send(1);
  await q.send(2);
  await q.send(3);
  q.close();
  await consumer;
  assert.deepEqual(collected, [1, 2, 3]);
});

test("send to closed queue rejects", async () => {
  const q = new BoundedQueue<number>(1);
  q.close();
  await assert.rejects(q.send(1));
});
