import type { Socket } from "node:net";
import { buildFrame, decodeFrameBody, encodeFrame, type Frame } from "./codec.js";
import {
  BrokenPipeError,
  DisconnectionError,
  EofError,
  ERR_TLS_REQUIRED,
  FibrilError,
  RedirectError,
  retryAdvice,
  ServerError,
  StaleDeliveryError,
  SubscriptionClosedError,
  TlsRequiredByBrokerError,
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
  type AssignmentChangedMsg,
  type DeliverMsg,
  type DeliveryTag,
  type ErrorMsg,
  type GoingAwayMsg,
  type SubscriptionClosedMsg,
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
  type TopologyUpdateAckMsg,
} from "./protocol.js";
import type { DeclarePlexus, SubscribeStream } from "./wire.js";
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
  /**
   * Durable settle coordinates, captured at delivery so settling no longer
   * depends on the origin engine's sub map. A reconnect that re-keyed the
   * subscription (new server sub id) still settles correctly.
   */
  topic: string;
  group: string | null;
  partition: number;
  /**
   * The connection incarnation this delivery arrived on, stamped once by the
   * engine that received it. A non-resumed reconnect bumps the live incarnation,
   * so a delivery held across it settles to `StaleDeliveryError`.
   */
  incarnation: bigint;
}

// Outstanding request waiters, keyed by request_id.
type Waiter =
  | { kind: "publish"; deferred: Deferred<bigint> }
  | { kind: "declareQueue"; deferred: Deferred<void> }
  | {
      kind: "subscribeManual";
      deferred: Deferred<SubscribeResult<InternalInflight>>;
      supervised: boolean;
    }
  | {
      kind: "subscribeAuto";
      deferred: Deferred<SubscribeResult<InternalDelivered>>;
      supervised: boolean;
    }
  | { kind: "topology"; deferred: Deferred<TopologyOkMsg> };

// Commands the public API submits to the engine.
export type Command =
  | {
      type: "publishUnconfirmed";
      topic: string;
      group: string | null;
      partition: number;
      partition_key: Uint8Array | null;
      partitioning_version: bigint;
      content_type: ContentType | null;
      headers: Record<string, string>;
      payload: Uint8Array;
      published: bigint;
      ttl_ms: bigint | null;
    }
  | {
      type: "publishConfirmed";
      topic: string;
      group: string | null;
      partition: number;
      partition_key: Uint8Array | null;
      partitioning_version: bigint;
      content_type: ContentType | null;
      headers: Record<string, string>;
      payload: Uint8Array;
      published: bigint;
      ttl_ms: bigint | null;
      reply: Deferred<bigint>;
    }
  | {
      type: "publishDelayedUnconfirmed";
      topic: string;
      group: string | null;
      partition: number;
      partition_key: Uint8Array | null;
      partitioning_version: bigint;
      content_type: ContentType | null;
      headers: Record<string, string>;
      payload: Uint8Array;
      published: bigint;
      not_before: bigint;
    }
  | {
      type: "publishDelayedConfirmed";
      topic: string;
      group: string | null;
      partition: number;
      partition_key: Uint8Array | null;
      partitioning_version: bigint;
      content_type: ContentType | null;
      headers: Record<string, string>;
      payload: Uint8Array;
      published: bigint;
      not_before: bigint;
      reply: Deferred<bigint>;
    }
  | { type: "declareQueue"; req: DeclareQueueMsg; reply: Deferred<void> }
  | { type: "declarePlexus"; req: DeclarePlexus; reply: Deferred<void> }
  | {
      type: "subscribe";
      req: SubscribeMsg;
      supervised: boolean;
      reply: Deferred<SubscribeResult<InternalInflight>>;
    }
  | {
      type: "subscribeAutoAck";
      req: SubscribeMsg;
      supervised: boolean;
      reply: Deferred<SubscribeResult<InternalDelivered>>;
    }
  | {
      type: "subscribeStream";
      req: SubscribeStream;
      reply: Deferred<SubscribeResult<InternalInflight>>;
    }
  | {
      type: "subscribeStreamAutoAck";
      req: SubscribeStream;
      reply: Deferred<SubscribeResult<InternalDelivered>>;
    }
  | {
      type: "ack";
      topic: string;
      group: string | null;
      partition: number;
      tag: DeliveryTag;
      request_id: bigint;
      reply: Deferred<void>;
    }
  | {
      type: "nack";
      topic: string;
      group: string | null;
      partition: number;
      tag: DeliveryTag;
      requeue: boolean;
      not_before: bigint | null;
      request_id: bigint;
      reply: Deferred<void>;
    }
  | {
      type: "topology";
      topic: string | null;
      group: string | null;
      reply: Deferred<TopologyOkMsg>;
    };

// ===== Constants =====

const DEFAULT_HEARTBEAT_INTERVAL_S = 5;
const COMMAND_QUEUE_CAPACITY = 8192;
const HANDSHAKE_REQUEST_ID = 1n;
const AUTH_REQUEST_ID = 2n;
const RECONCILE_REQUEST_ID = 3n;

// Coalesce fire-and-forget writes and flush in one socket write. Node does one
// write syscall per socket.write, so an unconfirmed-publish burst is otherwise
// one syscall per message. Three triggers flush the buffer, whichever comes
// first: a byte cap, a frame-count cap, and a short time window. Reply-bearing
// and control frames flush immediately, so coalescing only ever delays
// fire-and-forget frames, and never past the window. Mirrors the Python client;
// see its engine.py for the sizing rationale (memory/latency ceiling, not a
// throughput knob).
export const WRITE_COALESCE_BYTES = 128 * 1024;
export const WRITE_COALESCE_COUNT = 128;
export const WRITE_COALESCE_WINDOW_MS = 0.5;

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
  // Set by the engine loop: best-effort synchronous flush of buffered
  // fire-and-forget frames (coalesced acks and publishes), called on a graceful
  // shutdown before the socket closes so an ack-then-close does not drop the ack.
  flush?: () => void;
}

// Shared mutable holder for the engine's terminal error, set as the loop tears
// down and read by the reconnect path. A holder (rather than a return value)
// lets the reconnect gate read the reason the instant the engine closes.
interface CloseReason {
  reason: FibrilError | null;
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
      // Carry the typed reason to the delivery leg. A supervised subscription
      // treats a recreate (and other non-terminal reasons) as a cue to
      // re-subscribe; a terminal reason surfaces to the consumer.
      const registered = registry.get(client.sub_id);
      registered?.delivery.queue.close(new SubscriptionClosedError(item.code, item.reason));
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

// ===== Settlement routing =====

/**
 * Routes a manual settle (ack/nack) to whatever engine is currently live for a
 * connection, keyed by the connection incarnation the delivery arrived on.
 * One per persistent connection (the bootstrap owner and each pooled owner),
 * outliving every engine it swaps through, so a delivery held across a reconnect
 * settles against the current engine rather than the dead one it arrived on.
 *
 * A non-resumed reconnect allocates a fresh incarnation, so a delivery from the
 * replaced session settles to {@link StaleDeliveryError}. A resumed reconnect
 * keeps the incarnation, so a held delivery still settles through the new engine.
 * Mirrors the reference client's `SettleContext` (crates/client).
 */
export class SettleContext {
  #current: { incarnation: bigint; engine: Engine } | null = null;
  #nextIncarnation = 0n;

  /**
   * Reserve the incarnation a (re)connecting engine will stamp on its
   * deliveries, BEFORE its read loop can deliver anything. A fresh session (a
   * non-resumed reconnect, or the first connect) claims a new incarnation so
   * deliveries held from the replaced session go stale; a resumed session reuses
   * the current one so they still settle. The engine captures the returned value
   * once and never re-reads it, so a late delivery on a superseded engine keeps
   * its own (now stale) stamp.
   */
  reserve(freshSession: boolean): bigint {
    if (freshSession || this.#current === null) {
      const incarnation = this.#nextIncarnation;
      this.#nextIncarnation += 1n;
      return incarnation;
    }
    return this.#current.incarnation;
  }

  /** Point settlement at an engine once it exists, under its reserved incarnation. */
  bind(incarnation: bigint, engine: Engine): void {
    this.#current = { incarnation, engine };
  }

  /**
   * Settle a delivery stamped at `incarnation`. If the incarnation has moved on,
   * the delivery is stale (its session was replaced) and this throws
   * {@link StaleDeliveryError} with no frame sent; otherwise the settle routes to
   * the live engine (which may still throw {@link BrokenPipeError} if it too is
   * down).
   */
  async settle(incarnation: bigint, cmd: Command): Promise<void> {
    const current = this.#current;
    if (current === null) throw new BrokenPipeError();
    if (incarnation !== current.incarnation) throw new StaleDeliveryError();
    await current.engine.submit(cmd);
  }
}

// ===== Public engine handle =====

export class Engine {
  readonly #commandQueue: BoundedQueue<Command>;
  readonly #socket: Socket;
  readonly #shutdownMode: ShutdownMode;
  // Why this engine's connection ended, set once as it tears down. Read by the
  // reconnect path so a non-retryable close (bad credentials, forbidden) is
  // surfaced instead of storming the broker with the same doomed handshake.
  readonly #closeReason: CloseReason;
  readonly resumeIdentity: ResumeIdentity;
  readonly resumeOutcome: HelloOk["resume_outcome"];
  /** The connection's persistent settle router; settlement routes through it. */
  readonly settleContext: SettleContext;
  #shutdownInitiated = false;
  #closed = false;
  /** Resolved (with the fatal error if any) when the engine task exits. */
  readonly #completed: Promise<void>;

  private constructor(
    commandQueue: BoundedQueue<Command>,
    socket: Socket,
    completed: Promise<void>,
    shutdownMode: ShutdownMode,
    closeReason: CloseReason,
    resumeIdentity: ResumeIdentity,
    resumeOutcome: HelloOk["resume_outcome"],
    settleContext: SettleContext,
  ) {
    this.#commandQueue = commandQueue;
    this.#socket = socket;
    this.#shutdownMode = shutdownMode;
    this.#closeReason = closeReason;
    this.#completed = completed.finally(() => {
      this.#closed = true;
    });
    this.resumeIdentity = resumeIdentity;
    this.resumeOutcome = resumeOutcome;
    this.settleContext = settleContext;
  }

  /**
   * Connect, perform handshake (and optional auth), and start the engine task.
   */
  static async start(
    socket: Socket,
    opts: ClientOptions,
    settleContext: SettleContext,
    subscriptionRegistry: SubscriptionRegistry = new Map(),
    onAssignmentChanged?: (msg: AssignmentChangedMsg) => void,
    onTopologyUpdate?: (topology: TopologyOkMsg) => bigint,
    onGoingAway?: (notice: GoingAwayMsg) => void,
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
    // A TLS listener answers a plaintext HELLO with a plaintext error frame
    // carrying ERR_TLS_REQUIRED, so the mismatch surfaces as its own typed
    // error rather than a generic failure.
    if (helloFrame.opcode === Op.Error) {
      const err = decodeFrameBody<ErrorMsg>(helloFrame);
      if (err.code === ERR_TLS_REQUIRED) throw new TlsRequiredByBrokerError();
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
      await writeFrame(socket, buildFrame(Op.Auth, AUTH_REQUEST_ID, opts.auth satisfies AuthMsg));
      const authFrame = await nextFrameOrEof(iter);
      if (authFrame.opcode === Op.AuthErr) {
        const err = decodeFrameBody<ErrorMsg>(authFrame);
        throw new ServerError(err.code, err.message);
      }
      if (authFrame.opcode !== Op.AuthOk) {
        throw new UnexpectedError(`Unexpected frame opcode ${authFrame.opcode} during AUTH`);
      }
    }

    let restoredSubscriptions = new Map<bigint, SubState>();
    // Reconcile on ANY reconnect that has active subscriptions, not only a
    // resumed session. When the owner broker restarts in place (a bounce faster
    // than failure detection, so ownership stays and there is no failover) the
    // client reconnects into a FRESH session (resume_rejected/new) that has no
    // memory of the subscriptions. Reconciling lets the broker either restore
    // them (restore) or report them missing so the dead
    // streams close. Without this a bounced owner leaves the streams open but
    // unfed and the consumer silently stops receiving.
    const reconcileSubs = [...subscriptionRegistry.values()].map((sub) => sub.reconcile);
    if (reconcileSubs.length > 0) {
      const reconcile: ReconcileClientMsg = {
        policy: opts.reconnectReconcilePolicy,
        subscriptions: reconcileSubs,
      };
      await writeFrame(socket, buildFrame(Op.ReconcileClient, RECONCILE_REQUEST_ID, reconcile));
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
    const heartbeatInterval = opts.heartbeatIntervalSeconds ?? DEFAULT_HEARTBEAT_INTERVAL_S;

    // Reserve this engine's incarnation before the loop can deliver: a resumed
    // session keeps the current one so held deliveries still settle, any other
    // outcome takes a fresh one so they go stale. The loop stamps this exact
    // value on every delivery (never a live re-read of the shared context).
    const incarnation = settleContext.reserve(hello.resume_outcome !== "resumed");

    const shutdownMode = { preserveSubscriptions: false };
    const closeReason: CloseReason = { reason: null };
    const completed = runEngineLoop({
      socket,
      reader,
      commandQueue,
      heartbeatIntervalSeconds: heartbeatInterval,
      subscriptionRegistry,
      initialSubscriptions: restoredSubscriptions,
      incarnation,
      shutdownMode,
      closeReason,
      writeCoalesceBytes: opts.writeCoalesceBytes,
      writeCoalesceCount: opts.writeCoalesceCount,
      writeCoalesceWindowMs: opts.writeCoalesceWindowMs,
      onAssignmentChanged,
      onTopologyUpdate,
      onGoingAway,
    });

    const engine = new Engine(
      commandQueue,
      socket,
      completed,
      shutdownMode,
      closeReason,
      resumeIdentity,
      hello.resume_outcome,
      settleContext,
    );
    // Bind before returning (and before the loop, suspended at its first read,
    // can process a delivery) so settlement routes to this live engine.
    settleContext.bind(incarnation, engine);
    return engine;
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
    // Push any buffered fire-and-forget frames (coalesced acks/publishes) to the
    // socket before it closes, so an ack-then-shutdown does not drop the ack.
    this.#shutdownMode.flush?.();
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

  /**
   * Why this connection ended, or null while it is still open. The reconnect
   * path reads it to tell a transient transport drop from a fatal rejection
   * (bad credentials, forbidden) that would only fail again on reconnect.
   */
  closeReason(): FibrilError | null {
    return this.#closeReason.reason;
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
  /** The incarnation this engine stamps on every manual delivery it hands out. */
  incarnation: bigint;
  shutdownMode: ShutdownMode;
  closeReason: CloseReason;
  writeCoalesceBytes: number;
  writeCoalesceCount: number;
  writeCoalesceWindowMs: number;
  onAssignmentChanged?: (msg: AssignmentChangedMsg) => void;
  onTopologyUpdate?: (topology: TopologyOkMsg) => bigint;
  onGoingAway?: (notice: GoingAwayMsg) => void;
}

async function runEngineLoop(args: EngineLoopArgs): Promise<void> {
  const {
    socket,
    reader,
    commandQueue,
    heartbeatIntervalSeconds,
    subscriptionRegistry,
    initialSubscriptions,
    incarnation,
    shutdownMode,
    closeReason,
    writeCoalesceBytes,
    writeCoalesceCount,
    writeCoalesceWindowMs,
    onAssignmentChanged,
    onTopologyUpdate,
    onGoingAway,
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
    // Record the terminal reason before the socket is destroyed, so the
    // reconnect gate can read it the instant isClosed() flips.
    closeReason.reason = fatalError;
    // Drop any buffered fire-and-forget frames the dead socket can't take.
    cancelScheduledFlush();
    pending = [];
    pendingBytes = 0;
    pendingCount = 0;
    // Unblock command consumer.
    commandQueue.close(err);
    // Unblock frame producer.
    if (!socket.destroyed) socket.destroy();
  };

  // ---- write coalescing (shared by the command and frame tasks) ----
  // Both tasks append to one pending buffer, so global write order is preserved
  // even when a reply-bearing frame from one task flushes fire-and-forget frames
  // buffered by the other. A single in-flight flush drains the buffer (awaiting
  // socket drain for backpressure) and never overlaps itself.
  let pending: Uint8Array[] = [];
  let pendingBytes = 0;
  let pendingCount = 0;
  let flushTimer: ReturnType<typeof setTimeout> | null = null;
  let flushInFlight: Promise<void> | null = null;
  // Window measured from the last flush, not the first buffered frame, so a lone
  // low-rate frame after an idle stretch flushes promptly. Seeded in the past so
  // the first frame flushes promptly too.
  let lastFlush = 0;

  const cancelScheduledFlush = (): void => {
    if (flushTimer !== null) {
      clearTimeout(flushTimer);
      flushTimer = null;
    }
  };

  const takePending = (): Uint8Array => {
    const data = concatChunks(pending, pendingBytes);
    pending = [];
    pendingBytes = 0;
    pendingCount = 0;
    lastFlush = performance.now();
    return data;
  };

  // Drain the buffer to the socket, awaiting drain between writes so a slow
  // consumer applies backpressure. Only one runs at a time; concurrent callers
  // await the same in-flight flush, which keeps going while frames remain.
  const flush = (): Promise<void> => {
    if (flushInFlight) return flushInFlight;
    cancelScheduledFlush();
    flushInFlight = (async () => {
      try {
        while (!socketDead && pending.length > 0) {
          await writeBytes(socket, takePending());
        }
      } catch (err) {
        markDead(new DisconnectionError(`Socket write failed: ${(err as Error).message}`));
      } finally {
        flushInFlight = null;
      }
    })();
    return flushInFlight;
  };

  const scheduleFlush = (): void => {
    if (flushTimer !== null || flushInFlight !== null || socketDead) return;
    const delayMs = Math.max(0, lastFlush + writeCoalesceWindowMs - performance.now());
    flushTimer = setTimeout(() => {
      flushTimer = null;
      void flush();
    }, delayMs);
  };

  const bufferFrame = (frame: Frame): void => {
    const bytes = encodeFrame(frame);
    pending.push(bytes);
    pendingBytes += bytes.byteLength;
    pendingCount += 1;
  };

  // Reply-bearing and control frames flush immediately, in order behind any
  // buffered fire-and-forget frames, because a reply is about to be awaited.
  const sendOrDie = async (frame: Frame): Promise<boolean> => {
    if (socketDead) return false;
    bufferFrame(frame);
    await flush();
    return !socketDead;
  };

  // Fire-and-forget frames coalesce: buffer, and flush only on a cap (which also
  // applies backpressure), else leave it to the window. A saturating publish loop
  // keeps the queue near empty, so the window is what batches the send syscalls.
  const sendBuffered = async (frame: Frame): Promise<boolean> => {
    if (socketDead) return false;
    bufferFrame(frame);
    if (pendingBytes >= writeCoalesceBytes || pendingCount >= writeCoalesceCount) {
      await flush();
    } else {
      scheduleFlush();
    }
    return !socketDead;
  };

  // Graceful-shutdown hook: push buffered fire-and-forget frames to the socket
  // before it closes. libuv attempts a synchronous write and the kernel flushes
  // its send buffer on the FIN, so a small trailing ack still reaches the broker.
  shutdownMode.flush = (): void => {
    if (socketDead || pending.length === 0) return;
    cancelScheduledFlush();
    try {
      socket.write(takePending());
    } catch {
      // Best effort: a dead socket just drops it, which redelivers.
    }
  };

  // ---- heartbeat ----
  const heartbeatHandle = setInterval(() => {
    if (socketDead) return;
    void heartbeatTick();
  }, heartbeatIntervalSeconds * 1000);

  const heartbeatTick = async (): Promise<void> => {
    if (Date.now() - lastSeen > timeoutMs) {
      markDead(
        new DisconnectionError(
          `heartbeat timeout: no response from the broker for over ${timeoutMs / 1000}s. ` +
            `This usually means a network stall or an overloaded or stopped broker rather ` +
            `than a client bug - check broker reachability and health. The client will ` +
            `attempt to reconnect if auto-reconnect is enabled`,
        ),
      );
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
      markDead(new DisconnectionError(`Read failed: ${(err as Error).message}`));
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
          ttl_ms: cmd.ttl_ms,
        };
        await sendBuffered(buildFrame(Op.Publish, reqId, msg));
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
          ttl_ms: cmd.ttl_ms,
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
        await sendBuffered(buildFrame(Op.PublishDelayed, reqId, msg));
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
        waiters.set(reqId, {
          kind: "subscribeManual",
          deferred: cmd.reply,
          supervised: cmd.supervised,
        });
        if (!(await sendOrDie(buildFrame(Op.Subscribe, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "subscribeAutoAck": {
        const reqId = nextReqId();
        waiters.set(reqId, {
          kind: "subscribeAuto",
          deferred: cmd.reply,
          supervised: cmd.supervised,
        });
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
      case "declarePlexus": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "declareQueue", deferred: cmd.reply });
        if (!(await sendOrDie(buildFrame(Op.DeclarePlexus, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      // Stream subscriptions reuse the manual/auto subscribe waiters but are
      // always supervised, so they register delivery state yet stay out of the
      // reconcile registry (the stream fan-in owns re-subscribe + cursor resume).
      case "subscribeStream": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "subscribeManual", deferred: cmd.reply, supervised: true });
        if (!(await sendOrDie(buildFrame(Op.SubscribeStream, reqId, cmd.req)))) {
          waiters.delete(reqId);
        }
        return;
      }
      case "subscribeStreamAutoAck": {
        const reqId = nextReqId();
        waiters.set(reqId, { kind: "subscribeAuto", deferred: cmd.reply, supervised: true });
        if (!(await sendOrDie(buildFrame(Op.SubscribeStream, reqId, cmd.req)))) {
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
        // Built from the delivery's own durable coordinates (carried on the
        // command), not a sub-map lookup - so a reconnect that re-keyed the
        // subscription still settles correctly.
        const msg: AckMsg = {
          topic: cmd.topic,
          group: cmd.group,
          partition: cmd.partition,
          tags: [cmd.tag],
        };
        // Acks are fire-and-forget (no reply awaited), so they coalesce like
        // unconfirmed publishes. A lost ack on a severed connection just
        // redelivers, which is already the at-least-once guarantee.
        const ok = await sendBuffered(buildFrame(Op.Ack, cmd.request_id, msg));
        if (ok) cmd.reply.resolve();
        else cmd.reply.reject(new BrokenPipeError());
        return;
      }
      case "nack": {
        const msg: NackMsg = {
          topic: cmd.topic,
          group: cmd.group,
          partition: cmd.partition,
          tags: [cmd.tag],
          requeue: cmd.requeue,
          not_before: cmd.not_before,
        };
        // Fire-and-forget like ack (see there).
        const ok = await sendBuffered(buildFrame(Op.Nack, cmd.request_id, msg));
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

      case Op.DeclarePlexusOk: {
        decodeFrameBody(frame);
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
            // Durable settle coordinates + this engine's incarnation, captured
            // now so a later settle routes to the current engine (or goes stale)
            // without depending on this engine's sub map surviving a reconnect.
            topic: sub.topic,
            group: sub.group,
            partition: sub.partition,
            incarnation,
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

      case Op.AssignmentChanged: {
        // Server push (no request id). Observability for exclusive cohorts;
        // exclusivity is enforced by the broker gate regardless.
        if (onAssignmentChanged) {
          onAssignmentChanged(decodeFrameBody<AssignmentChangedMsg>(frame));
        }
        return;
      }

      case Op.TopologyUpdate: {
        // Broker-pushed routing refresh (generation changed). Apply it to the
        // shared routing cache so subsequent ops route to the new owners, then
        // ack the generation now reflected so the broker can fence a cutover.
        if (onTopologyUpdate) {
          const topology = decodeFrameBody<TopologyOkMsg>(frame);
          const generation = onTopologyUpdate(topology);
          await sendOrDie(
            buildFrame(Op.TopologyUpdateAck, frame.requestId, {
              generation,
            } satisfies TopologyUpdateAckMsg),
          );
        }
        return;
      }

      case Op.GoingAway: {
        // The broker is draining for a planned shutdown or upgrade. Surface it
        // to the app so it can wind down; when the socket then closes, the
        // existing reconnect path redirects to the post-drain owner.
        const notice = decodeFrameBody<GoingAwayMsg>(frame);
        if (onGoingAway) {
          try {
            onGoingAway(notice);
          } catch {
            // A listener throwing must not break frame processing.
          }
        }
        return;
      }

      case Op.SubscriptionClosed: {
        // The broker ended one subscription (topic deleted, ownership moved,
        // cohort removal) while the connection stays up. Close its delivery
        // leg with the typed reason so the stream never just goes silent - a
        // supervised subscription re-subscribes on a non-terminal reason, a
        // terminal one surfaces to the consumer.
        const closed = decodeFrameBody<SubscriptionClosedMsg>(frame);
        const sub = subs.get(closed.sub_id);
        if (sub) {
          sub.delivery.queue.close(new SubscriptionClosedError(closed.code, closed.message));
          subs.delete(closed.sub_id);
          subscriptionRegistry.delete(closed.sub_id);
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
          // Connection-level error (no correlated request). Preserve the broker
          // code so a non-retryable rejection (bad credentials, forbidden,
          // malformed) is not mistaken for a transient disconnect and stormed on
          // reconnect. A retryable code (owner moved, 5xx) still closes as a
          // transient disconnect, so the reconnect/failover path is unchanged.
          const failure = new ServerError(err.code, err.message);
          markDead(
            retryAdvice(failure) === "do_not_retry"
              ? failure
              : new DisconnectionError(`Server connection error ${err.code}: ${err.message}`),
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

  const cleanupError = fatalError ?? new DisconnectionError("engine shutdown");

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
      case "declarePlexus":
      case "subscribe":
      case "subscribeAutoAck":
      case "subscribeStream":
      case "subscribeStreamAutoAck":
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
  return writeBytes(socket, encodeFrame(frame));
}

function writeBytes(socket: Socket, bytes: Uint8Array): Promise<void> {
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

function concatChunks(chunks: Uint8Array[], totalBytes: number): Uint8Array {
  if (chunks.length === 1) return chunks[0]!;
  const out = new Uint8Array(totalBytes);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.byteLength;
  }
  return out;
}

async function nextFrameOrEof(iter: AsyncIterator<Frame>): Promise<Frame> {
  const result = await iter.next();
  if (result.done) {
    throw new EofError("Connection closed before expected frame");
  }
  return result.value;
}

// Re-export to break a circular import in tests / external use.
export { deferred };
export type { Deferred };
