import type { Socket } from "node:net";
import {
  buildFrame,
  decodeFrameBody,
  encodeFrame,
  type Frame,
} from "./codec.js";
import {
  BrokenPipeError,
  DisconnectionError,
  EofError,
  FibrilError,
  ServerError,
  UnexpectedError,
} from "./errors.js";
import {
  COMPLIANCE_STRING,
  Op,
  PROTOCOL_V1,
  type AckMsg,
  type AuthMsg,
  type DeliverMsg,
  type DeliveryTag,
  type ErrorMsg,
  type Hello,
  type HelloOk,
  type NackMsg,
  type PublishMsg,
  type PublishOkMsg,
  type RejectMsg,
  type SubscribeMsg,
  type SubscribeOkMsg,
} from "./protocol.js";
import { BoundedQueue } from "./internal/bounded-queue.js";
import { deferred, type Deferred } from "./internal/deferred.js";
import { FrameReader } from "./internal/frame-reader.js";
import type { ClientOptions } from "./client.js";

// ===== Internal types =====

export type SubDelivery =
  | { kind: "manual"; queue: BoundedQueue<InternalInflight> }
  | { kind: "auto"; queue: BoundedQueue<InternalDelivered> };

export interface SubState {
  topic: string;
  group: string | null;
  partition: number;
  delivery: SubDelivery;
}

export interface InternalDelivered {
  delivery_tag: DeliveryTag;
  payload: Uint8Array;
  published: bigint;
  publish_received: bigint;
  offset: bigint;
}

export interface InternalInflight extends InternalDelivered {
  /** Frame request id from the original Deliver frame. */
  deliver_request_id: bigint;
  /** Sub id this delivery belongs to (for routing settle commands). */
  sub_id: bigint;
}

// Outstanding request waiters, keyed by request_id.
type Waiter =
  | { kind: "publish"; deferred: Deferred<bigint> }
  | { kind: "subscribeManual"; deferred: Deferred<BoundedQueue<InternalInflight>> }
  | { kind: "subscribeAuto"; deferred: Deferred<BoundedQueue<InternalDelivered>> };

// Commands the public API submits to the engine.
export type Command =
  | { type: "publishUnconfirmed"; topic: string; group: string | null; payload: Uint8Array; published: bigint }
  | { type: "publishConfirmed"; topic: string; group: string | null; payload: Uint8Array; published: bigint; reply: Deferred<bigint> }
  | { type: "subscribe"; req: SubscribeMsg; reply: Deferred<BoundedQueue<InternalInflight>> }
  | { type: "subscribeAutoAck"; req: SubscribeMsg; reply: Deferred<BoundedQueue<InternalDelivered>> }
  | { type: "ack"; sub_id: bigint; tag: DeliveryTag; request_id: bigint; reply: Deferred<void> }
  | { type: "nack"; sub_id: bigint; tag: DeliveryTag; requeue: boolean; request_id: bigint; reply: Deferred<void> }
  | { type: "reject"; sub_id: bigint; tag: DeliveryTag; requeue: boolean; request_id: bigint; reply: Deferred<void> };

// ===== Constants =====

const DEFAULT_HEARTBEAT_INTERVAL_S = 5;
const COMMAND_QUEUE_CAPACITY = 8192;
const HANDSHAKE_REQUEST_ID = 1n;
const AUTH_REQUEST_ID = 2n;

// ===== Public engine handle =====

export class Engine {
  readonly #commandQueue: BoundedQueue<Command>;
  readonly #socket: Socket;
  #shutdownInitiated = false;
  /** Resolved (with the fatal error if any) when the engine task exits. */
  readonly #completed: Promise<void>;

  private constructor(
    commandQueue: BoundedQueue<Command>,
    socket: Socket,
    completed: Promise<void>,
  ) {
    this.#commandQueue = commandQueue;
    this.#socket = socket;
    this.#completed = completed;
  }

  /**
   * Connect, perform handshake (and optional auth), and start the engine task.
   */
  static async start(socket: Socket, opts: ClientOptions): Promise<Engine> {
    const reader = new FrameReader(socket);
    const iter = reader[Symbol.asyncIterator]();

    // ---- HELLO ----
    await writeFrame(
      socket,
      buildFrame(Op.Hello, HANDSHAKE_REQUEST_ID, {
        client_name: opts.clientName,
        client_version: opts.clientVersion,
        protocol_version: PROTOCOL_V1,
      } satisfies Hello),
    );

    const helloFrame = await nextFrameOrEof(iter);
    if (helloFrame.opcode === Op.HelloErr) {
      const err = decodeFrameBody<ErrorMsg>(helloFrame);
      throw new ServerError(err.code, err.message);
    }
    if (helloFrame.opcode !== Op.HelloOk) {
      throw new UnexpectedError(`Unexpected frame opcode ${helloFrame.opcode} during HELLO`);
    }
    const hello = decodeFrameBody<HelloOk>(helloFrame);
    if (hello.compliance !== COMPLIANCE_STRING) {
      throw new DisconnectionError("Protocol compliance marker mismatch");
    }
    if (hello.protocol_version !== PROTOCOL_V1) {
      throw new DisconnectionError(
        `Protocol version mismatch: expected ${PROTOCOL_V1}, got ${hello.protocol_version}`,
      );
    }

    // ---- AUTH (optional) ----
    if (opts.auth) {
      await writeFrame(
        socket,
        buildFrame(Op.Auth, AUTH_REQUEST_ID, opts.auth satisfies AuthMsg),
      );
      const authFrame = await nextFrameOrEof(iter);
      if (authFrame.opcode === Op.AuthErr) {
        const err = decodeFrameBody<ErrorMsg>(authFrame);
        throw new ServerError(err.code, err.message);
      }
      if (authFrame.opcode !== Op.AuthOk) {
        throw new UnexpectedError(
          `Unexpected frame opcode ${authFrame.opcode} during AUTH`,
        );
      }
    }

    // ---- Start engine loop ----
    const commandQueue = new BoundedQueue<Command>(COMMAND_QUEUE_CAPACITY);
    const heartbeatInterval =
      opts.heartbeatIntervalSeconds ?? DEFAULT_HEARTBEAT_INTERVAL_S;

    const completed = runEngineLoop({
      socket,
      reader,
      commandQueue,
      heartbeatIntervalSeconds: heartbeatInterval,
    });

    return new Engine(commandQueue, socket, completed);
  }

  /**
   * Submit a command to the engine. Awaits until the command is enqueued.
   * Throws `BrokenPipeError` if the engine has shut down.
   */
  async submit(cmd: Command): Promise<void> {
    if (this.#shutdownInitiated) {
      throw new BrokenPipeError();
    }
    try {
      await this.#commandQueue.send(cmd);
    } catch {
      throw new BrokenPipeError();
    }
  }

  /**
   * Trigger graceful shutdown. The engine task will close the socket and
   * fail all pending operations.
   */
  shutdown(): void {
    if (this.#shutdownInitiated) return;
    this.#shutdownInitiated = true;
    // Close the command queue: command consumer will exit cleanly.
    this.#commandQueue.close(new BrokenPipeError("engine shutdown"));
    // Destroy the socket: frame producer's async iterator will end.
    if (!this.#socket.destroyed) {
      this.#socket.destroy();
    }
  }

  /** Resolves when the engine task has fully exited. */
  whenComplete(): Promise<void> {
    return this.#completed;
  }
}

// ===== Engine main loop =====

interface EngineLoopArgs {
  socket: Socket;
  reader: FrameReader;
  commandQueue: BoundedQueue<Command>;
  heartbeatIntervalSeconds: number;
}

async function runEngineLoop(args: EngineLoopArgs): Promise<void> {
  const { socket, reader, commandQueue, heartbeatIntervalSeconds } = args;

  const subs = new Map<bigint, SubState>();
  const waiters = new Map<bigint, Waiter>();
  let nextRequestId = 3n; // 1 = HELLO, 2 = AUTH; user requests start at 3.
  const nextReqId = (): bigint => {
    const id = nextRequestId;
    // u64 wraparound; will not happen in practice but stays consistent
    // with the Rust client's wrapping_add.
    nextRequestId = (nextRequestId + 1n) & 0xffff_ffff_ffff_ffffn;
    return id;
  };

  let lastSeen = Date.now();
  const timeoutMs = heartbeatIntervalSeconds * 3 * 1000;
  let fatalError: FibrilError | null = null;

  // ---- write helper that classifies write failures as fatal ----
  let socketDead = false;
  /**
   * Mark the connection dead. Idempotent. Closes the command queue and
   * destroys the socket so both producer and consumer tasks unblock.
   */
  const markDead = (err: FibrilError): void => {
    if (socketDead) return;
    socketDead = true;
    if (!fatalError) fatalError = err;
    // Unblock command consumer.
    commandQueue.close(err);
    // Unblock frame producer.
    if (!socket.destroyed) socket.destroy();
  };

  const sendOrDie = async (frame: Frame): Promise<boolean> => {
    if (socketDead) return false;
    try {
      await writeFrame(socket, frame);
      return true;
    } catch (err) {
      markDead(
        new DisconnectionError(
          `Socket write failed: ${(err as Error).message}`,
        ),
      );
      return false;
    }
  };

  // ---- heartbeat ----
  const heartbeatHandle = setInterval(() => {
    if (socketDead) return;
    void heartbeatTick();
  }, heartbeatIntervalSeconds * 1000);

  const heartbeatTick = async (): Promise<void> => {
    if (Date.now() - lastSeen > timeoutMs) {
      markDead(new DisconnectionError("Heartbeat timeout"));
      return;
    }
    await sendOrDie(buildFrame(Op.Ping, nextReqId(), null));
  };

  // ---- frame producer (reads from socket, pushes onto an internal queue) ----
  // Note: We don't merge command and frame streams via Promise.race because
  // race leaves losers pending and can leak. Instead, the command consumer
  // and frame consumer run concurrently as separate "tasks" that share state
  // through the closures above; ordering between them is not critical because
  // all state mutations happen on the same JS thread anyway.

  const frameTask = (async (): Promise<void> => {
    try {
      for await (const frame of reader) {
        lastSeen = Date.now();
        await handleIncomingFrame(frame);
        if (socketDead) break;
      }
      // Reader ended cleanly (EOF). If we weren't shutting down, that's
      // still a disconnection from the user's perspective.
      if (!socketDead) {
        markDead(new DisconnectionError("Connection closed by peer"));
      }
    } catch (err) {
      markDead(
        new DisconnectionError(
          `Read failed: ${(err as Error).message}`,
        ),
      );
    }
  })();

  const commandTask = (async (): Promise<void> => {
    while (!socketDead) {
      const cmd = await commandQueue.recv();
      if (cmd === null) break; // queue closed (shutdown)
      await handleCommand(cmd);
      if (socketDead) break;
    }
  })();

  async function handleCommand(cmd: Command): Promise<void> {
    switch (cmd.type) {
      case "publishUnconfirmed": {
        const reqId = nextReqId();
        const msg: PublishMsg = {
          topic: cmd.topic,
          group: cmd.group,
          partition: 0,
          require_confirm: false,
          payload: cmd.payload,
          published: cmd.published,
        };
        await sendOrDie(buildFrame(Op.Publish, reqId, msg));
        return;
      }
      case "publishConfirmed": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "publish", deferred: cmd.reply });
        const msg: PublishMsg = {
          topic: cmd.topic,
          group: cmd.group,
          partition: 0,
          require_confirm: true,
          payload: cmd.payload,
          published: cmd.published,
        };
        if (!(await sendOrDie(buildFrame(Op.Publish, reqId, msg)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "subscribe": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "subscribeManual", deferred: cmd.reply });
        if (!(await sendOrDie(buildFrame(Op.Subscribe, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "subscribeAutoAck": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "subscribeAuto", deferred: cmd.reply });
        if (!(await sendOrDie(buildFrame(Op.Subscribe, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "ack": {
        const sub = subs.get(cmd.sub_id);
        if (!sub) {
          // Sub no longer exists; resolve so caller doesn't hang.
          cmd.reply.resolve();
          return;
        }
        const msg: AckMsg = {
          topic: sub.topic,
          group: sub.group,
          partition: sub.partition,
          tags: [cmd.tag],
        };
        const ok = await sendOrDie(buildFrame(Op.Ack, cmd.request_id, msg));
        if (ok) cmd.reply.resolve();
        else cmd.reply.reject(new BrokenPipeError());
        return;
      }
      case "nack": {
        const sub = subs.get(cmd.sub_id);
        if (!sub) {
          cmd.reply.resolve();
          return;
        }
        const msg: NackMsg = {
          topic: sub.topic,
          group: sub.group,
          partition: sub.partition,
          tags: [cmd.tag],
          requeue: cmd.requeue,
        };
        const ok = await sendOrDie(buildFrame(Op.Nack, cmd.request_id, msg));
        if (ok) cmd.reply.resolve();
        else cmd.reply.reject(new BrokenPipeError());
        return;
      }
      case "reject": {
        const sub = subs.get(cmd.sub_id);
        if (!sub) {
          cmd.reply.resolve();
          return;
        }
        const msg: RejectMsg = {
          topic: sub.topic,
          group: sub.group,
          partition: sub.partition,
          tags: [cmd.tag],
          requeue: cmd.requeue,
        };
        const ok = await sendOrDie(buildFrame(Op.Reject, cmd.request_id, msg));
        if (ok) cmd.reply.resolve();
        else cmd.reply.reject(new BrokenPipeError());
        return;
      }
    }
  }

  async function handleIncomingFrame(frame: Frame): Promise<void> {
    switch (frame.opcode) {
      case Op.PublishOk: {
        const ok = decodeFrameBody<PublishOkMsg>(frame);
        const w = waiters.get(frame.requestId);
        if (w?.kind === "publish") {
          waiters.delete(frame.requestId);
          w.deferred.resolve(ok.offset);
        } else if (w) {
          // protocol violation: log + drop
          // (waiter exists but is the wrong kind)
        }
        return;
      }

      case Op.SubscribeOk: {
        const ok = decodeFrameBody<SubscribeOkMsg>(frame);
        const w = waiters.get(frame.requestId);
        if (!w) return;
        waiters.delete(frame.requestId);

        const prefetch = Math.max(1, ok.prefetch);
        if (w.kind === "subscribeManual") {
          const queue = new BoundedQueue<InternalInflight>(prefetch);
          subs.set(ok.sub_id, {
            topic: ok.topic,
            group: ok.group,
            partition: ok.partition,
            delivery: { kind: "manual", queue },
          });
          w.deferred.resolve(queue);
        } else if (w.kind === "subscribeAuto") {
          const queue = new BoundedQueue<InternalDelivered>(prefetch);
          subs.set(ok.sub_id, {
            topic: ok.topic,
            group: ok.group,
            partition: ok.partition,
            delivery: { kind: "auto", queue },
          });
          w.deferred.resolve(queue);
        } else {
          // wrong kind; nothing to do
        }
        return;
      }

      case Op.Deliver: {
        const d = decodeFrameBody<DeliverMsg>(frame);
        const sub = subs.get(d.sub_id);
        if (!sub) return;

        const base: InternalDelivered = {
          delivery_tag: d.delivery_tag,
          payload: d.payload,
          published: d.published,
          publish_received: d.publish_received,
          offset: d.offset,
        };

        if (sub.delivery.kind === "manual") {
          const inflight: InternalInflight = {
            ...base,
            deliver_request_id: frame.requestId,
            sub_id: d.sub_id,
          };
          // Backpressure: this awaits if user code isn't keeping up. That's
          // the prefetch contract.
          try {
            await sub.delivery.queue.send(inflight);
          } catch {
            // queue closed — sub is dead, drop silently.
          }
        } else {
          try {
            await sub.delivery.queue.send(base);
          } catch {
            // queue closed
          }
        }
        return;
      }

      case Op.Ping: {
        await sendOrDie(buildFrame(Op.Pong, frame.requestId, null));
        return;
      }

      case Op.Pong:
        return;

      case Op.Error: {
        const err = decodeFrameBody<ErrorMsg>(frame);
        const w = waiters.get(frame.requestId);
        if (w) {
          waiters.delete(frame.requestId);
          failWaiter(w, new ServerError(err.code, err.message));
        } else {
          // connection-level error: fatal
          markDead(
            new DisconnectionError(
              `Server connection error ${err.code}: ${err.message}`,
            ),
          );
        }
        return;
      }

      case Op.SubscribeErr: {
        // Server-side subscribe failure: treat like an Error frame for
        // the matching request id.
        const err = decodeFrameBody<ErrorMsg>(frame);
        const w = waiters.get(frame.requestId);
        if (w) {
          waiters.delete(frame.requestId);
          failWaiter(w, new ServerError(err.code, err.message));
        }
        return;
      }

      default:
        // Unknown opcode: ignore. Server may have added new ops we don't
        // understand yet; logging would happen here in a richer build.
        return;
    }
  }

  // ---- wait for both producer and consumer to drain ----
  await Promise.all([frameTask, commandTask]);

  // ---- cleanup ----
  clearInterval(heartbeatHandle);

  const cleanupError =
    fatalError ?? new DisconnectionError("engine shutdown");

  // Fail all waiters.
  for (const w of waiters.values()) {
    failWaiter(w, cleanupError);
  }
  waiters.clear();

  // Close all subscription queues with the failure cause so
  // subscription async iterators throw rather than ending silently.
  for (const sub of subs.values()) {
    sub.delivery.queue.close(cleanupError);
  }
  subs.clear();

  // Close the command queue (idempotent) and drain any commands that were
  // buffered but never processed. Reject their reply deferreds so user
  // code doesn't hang.
  commandQueue.close(cleanupError);
  for (const cmd of commandQueue.drain()) {
    switch (cmd.type) {
      case "publishConfirmed":
        cmd.reply.reject(cleanupError);
        break;
      case "subscribe":
      case "subscribeAutoAck":
        cmd.reply.reject(cleanupError);
        break;
      case "ack":
      case "nack":
      case "reject":
        cmd.reply.reject(cleanupError);
        break;
      case "publishUnconfirmed":
        // No reply to fail.
        break;
    }
  }

  // Make sure the socket is gone.
  if (!socket.destroyed) {
    socket.destroy();
  }
}

// ===== helpers =====

function failWaiter(w: Waiter, err: FibrilError): void {
  switch (w.kind) {
    case "publish":
      w.deferred.reject(err);
      return;
    case "subscribeManual":
    case "subscribeAuto":
      w.deferred.reject(err);
      return;
  }
}

function writeFrame(socket: Socket, frame: Frame): Promise<void> {
  const bytes = encodeFrame(frame);
  return new Promise<void>((resolve, reject) => {
    const ok = socket.write(bytes, (err) => {
      if (err) reject(err);
    });
    if (ok) {
      // Resolve on next microtask so callers can chain awaits without
      // racing the error callback.
      queueMicrotask(resolve);
    } else {
      socket.once("drain", resolve);
    }
  });
}

async function nextFrameOrEof(
  iter: AsyncIterator<Frame>,
): Promise<Frame> {
  const result = await iter.next();
  if (result.done) {
    throw new EofError("Connection closed before expected frame");
  }
  return result.value;
}

// Re-export to break a circular import in tests / external use.
export { deferred };
export type { Deferred };
