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
  RedirectError,
  ServerError,
  UnexpectedError,
} from "./errors.js";
import {
  COMPLIANCE_STRING,
  Op,
  PROTOCOL_V1,
  type AckMsg,
  type ContentType,
  type AuthMsg,
  type DeclareQueueMsg,
  type DeclareQueueOkMsg,
  type DeliverMsg,
  type DeliveryTag,
  type ErrorMsg,
  type Hello,
  type HelloOk,
  type NackMsg,
  type PublishDelayedMsg,
  type PublishMsg,
  type PublishOkMsg,
  type ReconcileClientMsg,
  type ReconcileResultMsg,
  type ReconcileSubscription,
  type RedirectMsg,
  type ResumeIdentity,
  type SubscribeMsg,
  type SubscribeOkMsg,
  type TopologyOkMsg,
  type TopologyRequestMsg,
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
  content_type: ContentType | null;
  headers: Record<string, string>;
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
  | { kind: "declareQueue"; deferred: Deferred<void> }
  | { kind: "subscribeManual"; deferred: Deferred<SubscribeResult<InternalInflight>>; supervised: boolean }
  | { kind: "subscribeAuto"; deferred: Deferred<SubscribeResult<InternalDelivered>>; supervised: boolean }
  | { kind: "topology"; deferred: Deferred<TopologyOkMsg> };

// Commands the public API submits to the engine.
export type Command =
  | { type: "publishUnconfirmed"; topic: string; group: string | null; partition: number; partition_key: Uint8Array | null; partitioning_version: bigint; content_type: ContentType | null; headers: Record<string, string>; payload: Uint8Array; published: bigint }
  | { type: "publishConfirmed"; topic: string; group: string | null; partition: number; partition_key: Uint8Array | null; partitioning_version: bigint; content_type: ContentType | null; headers: Record<string, string>; payload: Uint8Array; published: bigint; reply: Deferred<bigint> }
  | { type: "publishDelayedUnconfirmed"; topic: string; group: string | null; partition: number; partition_key: Uint8Array | null; partitioning_version: bigint; content_type: ContentType | null; headers: Record<string, string>; payload: Uint8Array; published: bigint; not_before: bigint }
  | { type: "publishDelayedConfirmed"; topic: string; group: string | null; partition: number; partition_key: Uint8Array | null; partitioning_version: bigint; content_type: ContentType | null; headers: Record<string, string>; payload: Uint8Array; published: bigint; not_before: bigint; reply: Deferred<bigint> }
  | { type: "declareQueue"; req: DeclareQueueMsg; reply: Deferred<void> }
  | { type: "subscribe"; req: SubscribeMsg; supervised: boolean; reply: Deferred<SubscribeResult<InternalInflight>> }
  | { type: "subscribeAutoAck"; req: SubscribeMsg; supervised: boolean; reply: Deferred<SubscribeResult<InternalDelivered>> }
  | { type: "ack"; sub_id: bigint; tag: DeliveryTag; request_id: bigint; reply: Deferred<void> }
  | { type: "nack"; sub_id: bigint; tag: DeliveryTag; requeue: boolean; not_before: bigint | null; request_id: bigint; reply: Deferred<void> }
  | { type: "topology"; topic: string | null; group: string | null; reply: Deferred<TopologyOkMsg> };

// ===== Constants =====

const DEFAULT_HEARTBEAT_INTERVAL_S = 5;
const COMMAND_QUEUE_CAPACITY = 8192;
const HANDSHAKE_REQUEST_ID = 1n;
const AUTH_REQUEST_ID = 2n;
const RECONCILE_REQUEST_ID = 3n;

export interface RegisteredSubscription {
  reconcile: ReconcileSubscription;
  delivery: SubDelivery;
}

/** Result of a subscribe: the delivery queue plus the server-echoed member id. */
export interface SubscribeResult<R> {
  queue: BoundedQueue<R>;
  memberId: Uint8Array | null;
}

export type SubscriptionRegistry = Map<bigint, RegisteredSubscription>;

interface ShutdownMode {
  preserveSubscriptions: boolean;
}

function normalizePayload(payload: DeliverMsg["payload"] | number[]): Uint8Array {
  if (payload instanceof Uint8Array) return payload;
  return Uint8Array.from(payload);
}

function applyReconcileResult(
  registry: SubscriptionRegistry,
  result: ReconcileResultMsg,
): Map<bigint, SubState> {
  const restored = new Map<bigint, SubState>();
  for (const item of result.subscriptions) {
    const client = item.client;
    if (!client) continue;

    if (item.action !== "keep") {
      const registered = registry.get(client.sub_id);
      registered?.delivery.queue.close(
        new DisconnectionError(`subscription was not kept after reconnect: ${item.reason}`),
      );
      registry.delete(client.sub_id);
      continue;
    }

    const registered = registry.get(client.sub_id);
    if (!registered) continue;
    const server = item.server ?? client;
    registered.reconcile = server;
    registry.delete(client.sub_id);
    registry.set(server.sub_id, registered);
    restored.set(server.sub_id, {
      topic: server.topic,
      group: server.group,
      partition: server.partition,
      delivery: registered.delivery,
    });
  }
  return restored;
}

// ===== Public engine handle =====

export class Engine {
  readonly #commandQueue: BoundedQueue<Command>;
  readonly #socket: Socket;
  readonly #shutdownMode: ShutdownMode;
  readonly resumeIdentity: ResumeIdentity;
  readonly resumeOutcome: HelloOk["resume_outcome"];
  #shutdownInitiated = false;
  #closed = false;
  /** Resolved (with the fatal error if any) when the engine task exits. */
  readonly #completed: Promise<void>;

  private constructor(
    commandQueue: BoundedQueue<Command>,
    socket: Socket,
    completed: Promise<void>,
    shutdownMode: ShutdownMode,
    resumeIdentity: ResumeIdentity,
    resumeOutcome: HelloOk["resume_outcome"],
  ) {
    this.#commandQueue = commandQueue;
    this.#socket = socket;
    this.#shutdownMode = shutdownMode;
    this.#completed = completed.finally(() => {
      this.#closed = true;
    });
    this.resumeIdentity = resumeIdentity;
    this.resumeOutcome = resumeOutcome;
  }

  /**
   * Connect, perform handshake (and optional auth), and start the engine task.
   */
  static async start(
    socket: Socket,
    opts: ClientOptions,
    subscriptionRegistry: SubscriptionRegistry = new Map(),
  ): Promise<Engine> {
    const reader = new FrameReader(socket);
    const iter = reader[Symbol.asyncIterator]();

    // ---- HELLO ----
    await writeFrame(
      socket,
      buildFrame(Op.Hello, HANDSHAKE_REQUEST_ID, {
        client_name: opts.clientName,
        client_version: opts.clientVersion,
        protocol_version: PROTOCOL_V1,
        resume: opts.resumeIdentity ?? null,
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
    const resumeIdentity: ResumeIdentity = {
      owner_id: hello.owner_id,
      client_id: hello.client_id,
      resume_token: hello.resume_token,
    };

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

    let restoredSubscriptions = new Map<bigint, SubState>();
    // Reconcile on ANY reconnect that has active subscriptions, not only a
    // resumed session. When the owner broker restarts in place (a bounce faster
    // than failure detection, so ownership stays and there is no failover) the
    // client reconnects into a FRESH session (resume_rejected/new) that has no
    // memory of the subscriptions. Reconciling lets the broker either restore
    // them (restore_client_subscriptions) or report them missing so the dead
    // streams close. Without this a bounced owner leaves the streams open but
    // unfed and the consumer silently stops receiving.
    const reconcileSubs = [...subscriptionRegistry.values()].map((sub) => sub.reconcile);
    if (reconcileSubs.length > 0) {
      const reconcile: ReconcileClientMsg = {
        policy: opts.reconnectReconcilePolicy,
        subscriptions: reconcileSubs,
      };
      await writeFrame(
        socket,
        buildFrame(Op.ReconcileClient, RECONCILE_REQUEST_ID, reconcile),
      );
      const reconcileFrame = await nextFrameOrEof(iter);
      if (reconcileFrame.opcode === Op.Error) {
        const err = decodeFrameBody<ErrorMsg>(reconcileFrame);
        throw new ServerError(err.code, err.message);
      }
      if (reconcileFrame.opcode !== Op.ReconcileResult) {
        throw new UnexpectedError(
          `Unexpected frame opcode ${reconcileFrame.opcode} during reconciliation`,
        );
      }
      restoredSubscriptions = applyReconcileResult(
        subscriptionRegistry,
        decodeFrameBody<ReconcileResultMsg>(reconcileFrame),
      );
    }

    // ---- Start engine loop ----
    const commandQueue = new BoundedQueue<Command>(COMMAND_QUEUE_CAPACITY);
    const heartbeatInterval =
      opts.heartbeatIntervalSeconds ?? DEFAULT_HEARTBEAT_INTERVAL_S;

    const shutdownMode = { preserveSubscriptions: false };
    const completed = runEngineLoop({
      socket,
      reader,
      commandQueue,
      heartbeatIntervalSeconds: heartbeatInterval,
      subscriptionRegistry,
      initialSubscriptions: restoredSubscriptions,
      shutdownMode,
    });

    return new Engine(commandQueue, socket, completed, shutdownMode, resumeIdentity, hello.resume_outcome);
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
   * Fetch the cluster topology from the broker. A null topic asks for the full
   * topology. Standalone brokers return an empty topology.
   */
  async fetchTopology(
    filter: { topic?: string | null; group?: string | null } = {},
  ): Promise<TopologyOkMsg> {
    const reply = deferred<TopologyOkMsg>();
    await this.submit({
      type: "topology",
      topic: filter.topic ?? null,
      group: filter.group ?? null,
      reply,
    });
    return reply.promise;
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

  /** @internal Preserve restored subscription queues while replacing engines. */
  shutdownForReconnect(): void {
    this.#shutdownMode.preserveSubscriptions = true;
    this.shutdown();
  }

  /** Resolves when the engine task has fully exited. */
  whenComplete(): Promise<void> {
    return this.#completed;
  }

  /** True when this engine cannot accept new commands. */
  isClosed(): boolean {
    return this.#closed || this.#shutdownInitiated || this.#socket.destroyed;
  }
}

// ===== Engine main loop =====

interface EngineLoopArgs {
  socket: Socket;
  reader: FrameReader;
  commandQueue: BoundedQueue<Command>;
  heartbeatIntervalSeconds: number;
  subscriptionRegistry: SubscriptionRegistry;
  initialSubscriptions: Map<bigint, SubState>;
  shutdownMode: ShutdownMode;
}

async function runEngineLoop(args: EngineLoopArgs): Promise<void> {
  const {
    socket,
    reader,
    commandQueue,
    heartbeatIntervalSeconds,
    subscriptionRegistry,
    initialSubscriptions,
    shutdownMode,
  } = args;

  const subs = new Map<bigint, SubState>(initialSubscriptions);
  const waiters = new Map<bigint, Waiter>();
  let nextRequestId = 4n; // 1 = HELLO, 2 = AUTH, 3 = optional reconcile.
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
          partition: cmd.partition,
          partition_key: cmd.partition_key,
          partitioning_version: cmd.partitioning_version,
          require_confirm: false,
          content_type: cmd.content_type,
          headers: cmd.headers,
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
          partition: cmd.partition,
          partition_key: cmd.partition_key,
          partitioning_version: cmd.partitioning_version,
          require_confirm: true,
          content_type: cmd.content_type,
          headers: cmd.headers,
          payload: cmd.payload,
          published: cmd.published,
        };
        if (!(await sendOrDie(buildFrame(Op.Publish, reqId, msg)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "publishDelayedUnconfirmed": {
        const reqId = nextReqId();
        const msg: PublishDelayedMsg = {
          topic: cmd.topic,
          group: cmd.group,
          partition: cmd.partition,
          partition_key: cmd.partition_key,
          partitioning_version: cmd.partitioning_version,
          require_confirm: false,
          not_before: cmd.not_before,
          content_type: cmd.content_type,
          headers: cmd.headers,
          payload: cmd.payload,
          published: cmd.published,
        };
        await sendOrDie(buildFrame(Op.PublishDelayed, reqId, msg));
        return;
      }
      case "publishDelayedConfirmed": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "publish", deferred: cmd.reply });
        const msg: PublishDelayedMsg = {
          topic: cmd.topic,
          group: cmd.group,
          partition: cmd.partition,
          partition_key: cmd.partition_key,
          partitioning_version: cmd.partitioning_version,
          require_confirm: true,
          not_before: cmd.not_before,
          content_type: cmd.content_type,
          headers: cmd.headers,
          payload: cmd.payload,
          published: cmd.published,
        };
        if (!(await sendOrDie(buildFrame(Op.PublishDelayed, reqId, msg)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "subscribe": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "subscribeManual", deferred: cmd.reply, supervised: cmd.supervised });
        if (!(await sendOrDie(buildFrame(Op.Subscribe, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "subscribeAutoAck": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "subscribeAuto", deferred: cmd.reply, supervised: cmd.supervised });
        if (!(await sendOrDie(buildFrame(Op.Subscribe, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "declareQueue": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "declareQueue", deferred: cmd.reply });
        if (!(await sendOrDie(buildFrame(Op.DeclareQueue, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "topology": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "topology", deferred: cmd.reply });
        const req: TopologyRequestMsg = { topic: cmd.topic, group: cmd.group };
        if (!(await sendOrDie(buildFrame(Op.Topology, reqId, req)))) {
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
          not_before: cmd.not_before,
        };
        const ok = await sendOrDie(buildFrame(Op.Nack, cmd.request_id, msg));
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
          const delivery: SubDelivery = { kind: "manual", queue };
          subs.set(ok.sub_id, {
            topic: ok.topic,
            group: ok.group,
            partition: ok.partition,
            delivery,
          });
          // Supervised subscriptions own their own continuity (the client
          // re-subscribes on failover), so they stay out of the reconcile
          // registry: they are not preserved or reconciled on reconnect.
          if (!w.supervised) {
            subscriptionRegistry.set(ok.sub_id, {
              reconcile: {
                sub_id: ok.sub_id,
                topic: ok.topic,
                group: ok.group,
                partition: ok.partition,
                auto_ack: false,
                prefetch: ok.prefetch,
              },
              delivery,
            });
          }
          w.deferred.resolve({ queue, memberId: ok.member_id ?? null });
        } else if (w.kind === "subscribeAuto") {
          const queue = new BoundedQueue<InternalDelivered>(prefetch);
          const delivery: SubDelivery = { kind: "auto", queue };
          subs.set(ok.sub_id, {
            topic: ok.topic,
            group: ok.group,
            partition: ok.partition,
            delivery,
          });
          if (!w.supervised) {
            subscriptionRegistry.set(ok.sub_id, {
              reconcile: {
                sub_id: ok.sub_id,
                topic: ok.topic,
                group: ok.group,
                partition: ok.partition,
                auto_ack: true,
                prefetch: ok.prefetch,
              },
              delivery,
            });
          }
          w.deferred.resolve({ queue, memberId: ok.member_id ?? null });
        } else {
          // wrong kind; nothing to do
        }
        return;
      }

      case Op.DeclareQueueOk: {
        decodeFrameBody<DeclareQueueOkMsg>(frame);
        const w = waiters.get(frame.requestId);
        if (w?.kind === "declareQueue") {
          waiters.delete(frame.requestId);
          w.deferred.resolve();
        }
        return;
      }

      case Op.TopologyOk: {
        const topology = decodeFrameBody<TopologyOkMsg>(frame);
        const w = waiters.get(frame.requestId);
        if (w?.kind === "topology") {
          waiters.delete(frame.requestId);
          w.deferred.resolve(topology);
        }
        return;
      }

      case Op.Redirect: {
        // Not a failure: the broker is naming the current owner for a misrouted
        // op. Fail the matching waiter with a typed error so the routing layer
        // can apply the redirect and retry on the owner's connection. Without a
        // waiter there is nothing to retry, so drop it.
        const redirect = decodeFrameBody<RedirectMsg>(frame);
        const w = waiters.get(frame.requestId);
        if (w) {
          waiters.delete(frame.requestId);
          failWaiter(w, new RedirectError(redirect));
        }
        return;
      }

      case Op.Deliver: {
        const d = decodeFrameBody<DeliverMsg>(frame);
        const sub = subs.get(d.sub_id);
        if (!sub) return;

        const base: InternalDelivered = {
          delivery_tag: d.delivery_tag,
          payload: normalizePayload(d.payload),
          content_type: d.content_type,
          headers: d.headers,
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
            subs.delete(d.sub_id);
            subscriptionRegistry.delete(d.sub_id);
          }
        } else {
          try {
            await sub.delivery.queue.send(base);
          } catch {
            subs.delete(d.sub_id);
            subscriptionRegistry.delete(d.sub_id);
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
  for (const [subId, sub] of subs) {
    if (shutdownMode.preserveSubscriptions) {
      const stillRegistered = [...subscriptionRegistry.values()].some(
        (registered) => registered.delivery === sub.delivery,
      );
      if (stillRegistered) continue;
    }
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
      case "publishDelayedConfirmed":
        cmd.reply.reject(cleanupError);
        break;
      case "declareQueue":
      case "subscribe":
      case "subscribeAutoAck":
        cmd.reply.reject(cleanupError);
        break;
      case "ack":
      case "nack":
      case "topology":
        cmd.reply.reject(cleanupError);
        break;
      case "publishDelayedUnconfirmed":
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
    case "declareQueue":
      w.deferred.reject(err);
      return;
    case "subscribeManual":
    case "subscribeAuto":
      w.deferred.reject(err);
      return;
    case "topology":
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
