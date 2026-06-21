import { connect as netConnect, type Socket } from "node:net";
import { Engine, type SubscriptionRegistry } from "./engine.js";
import { BrokenPipeError, DisconnectionError, FibrilError } from "./errors.js";
import { deferred } from "./internal/deferred.js";
import { Publisher } from "./publisher.js";
import { SubscriptionBuilder } from "./subscription.js";
import { TopologyCache, routePartition, type Route, type RoundRobin } from "./internal/topology.js";
import type {
  AuthMsg,
  DeclareQueueMsg,
  QueueDlqPolicy,
  ReconcilePolicy,
  RedirectMsg,
  ResumeIdentity,
  ResumeOutcome,
  TopologyOkMsg,
} from "./protocol.js";

function normalizeGroup(group: string | null): string | null {
  const trimmed = group?.trim();
  if (!trimmed || trimmed === "default") return null;
  return trimmed;
}

/**
 * Options used during client startup and protocol handshake.
 *
 * @example
 * ```ts
 * const opts = new ClientOptions({
 *   clientName: "worker",
 * }).withAuth("fibril", "fibril");
 * ```
 */
export interface ClientOptionsInit {
  /** Name sent to the broker during handshake. */
  clientName?: string;
  /** Version sent to the broker during handshake. */
  clientVersion?: string;
  /** Optional username/password authentication. */
  auth?: AuthMsg;
  /** Heartbeat interval in seconds. Server-side timeout is 3x this value. */
  heartbeatIntervalSeconds?: number;
  /** Optional resume identity from a previous connection. */
  resumeIdentity?: ResumeIdentity;
  /** Automatic reconnect attempts before a new operation when the engine is closed. */
  autoReconnectAttempts?: number;
  /** Subscription reconciliation policy used after a resumed reconnect. */
  reconnectReconcilePolicy?: ReconcilePolicy;
  /** Maximum owner redirects to follow for a single confirmed publish. */
  maxRedirects?: number;
  /**
   * Budget in milliseconds for retrying a confirmed publish across a transient
   * owner failover. 0 disables retry so the first transport failure fails fast.
   */
  publishTimeoutMs?: number;
  /** Minimum gap between throttled topology refreshes during retries. */
  topologyRefreshCooldownMs?: number;
}

const DEFAULT_CLIENT_NAME = "Fibril TS Client";
const DEFAULT_CLIENT_VERSION = "0.1.0";

/**
 * Immutable connection option builder.
 *
 * Use `new ClientOptions().withAuth(...).connect(...)` for the common path.
 */
export class ClientOptions {
  readonly clientName: string;
  readonly clientVersion: string;
  readonly auth: AuthMsg | undefined;
  readonly heartbeatIntervalSeconds: number | undefined;
  readonly resumeIdentity: ResumeIdentity | undefined;
  readonly autoReconnectAttempts: number;
  readonly reconnectReconcilePolicy: ReconcilePolicy;
  readonly maxRedirects: number;
  readonly publishTimeoutMs: number;
  readonly topologyRefreshCooldownMs: number;

  constructor(init: ClientOptionsInit = {}) {
    this.clientName = init.clientName ?? DEFAULT_CLIENT_NAME;
    this.clientVersion = init.clientVersion ?? DEFAULT_CLIENT_VERSION;
    this.auth = init.auth;
    this.heartbeatIntervalSeconds = init.heartbeatIntervalSeconds;
    this.resumeIdentity = init.resumeIdentity;
    this.autoReconnectAttempts = init.autoReconnectAttempts ?? 1;
    this.reconnectReconcilePolicy = init.reconnectReconcilePolicy ?? "conservative";
    this.maxRedirects = init.maxRedirects ?? 3;
    this.publishTimeoutMs = init.publishTimeoutMs ?? 30_000;
    this.topologyRefreshCooldownMs = init.topologyRefreshCooldownMs ?? 1_000;
  }

  /** Return a copy with the given fields overridden. */
  #copy(overrides: ClientOptionsInit): ClientOptions {
    return new ClientOptions({
      clientName: this.clientName,
      clientVersion: this.clientVersion,
      auth: this.auth,
      heartbeatIntervalSeconds: this.heartbeatIntervalSeconds,
      resumeIdentity: this.resumeIdentity,
      autoReconnectAttempts: this.autoReconnectAttempts,
      reconnectReconcilePolicy: this.reconnectReconcilePolicy,
      maxRedirects: this.maxRedirects,
      publishTimeoutMs: this.publishTimeoutMs,
      topologyRefreshCooldownMs: this.topologyRefreshCooldownMs,
      ...overrides,
    });
  }

  /**
   * Return a copy with username/password authentication configured.
   */
  withAuth(username: string, password: string): ClientOptions {
    return this.#copy({ auth: { username, password } });
  }

  /**
   * Return a copy with heartbeat interval configured in seconds.
   */
  withHeartbeatInterval(seconds: number): ClientOptions {
    return this.#copy({ heartbeatIntervalSeconds: seconds });
  }

  /**
   * Return a copy configured to attempt resuming a previous connection.
   */
  withResumeIdentity(resumeIdentity: ResumeIdentity): ClientOptions {
    return this.#copy({ resumeIdentity });
  }

  /**
   * Return a copy with automatic reconnect disabled.
   */
  disableAutoReconnect(): ClientOptions {
    return this.withAutoReconnectAttempts(0);
  }

  /**
   * Return a copy with a custom automatic reconnect attempt limit.
   */
  withAutoReconnectAttempts(maxAttempts: number): ClientOptions {
    if (!Number.isInteger(maxAttempts) || maxAttempts < 0) {
      throw new Error("maxAttempts must be a non-negative integer");
    }
    return this.#copy({ autoReconnectAttempts: maxAttempts });
  }

  /**
   * Return a copy with a custom reconnect subscription reconciliation policy.
   */
  withReconnectReconcilePolicy(policy: ReconcilePolicy): ClientOptions {
    return this.#copy({ reconnectReconcilePolicy: policy });
  }

  /**
   * Return a copy with a custom limit on owner redirects followed per publish.
   */
  withMaxRedirects(maxRedirects: number): ClientOptions {
    if (!Number.isInteger(maxRedirects) || maxRedirects < 0) {
      throw new Error("maxRedirects must be a non-negative integer");
    }
    return this.#copy({ maxRedirects });
  }

  /**
   * Return a copy with a custom confirmed-publish failover retry budget in
   * milliseconds. 0 disables retry (the first transport failure fails fast).
   */
  withPublishTimeout(timeoutMs: number): ClientOptions {
    if (!Number.isInteger(timeoutMs) || timeoutMs < 0) {
      throw new Error("timeoutMs must be a non-negative integer");
    }
    return this.#copy({ publishTimeoutMs: timeoutMs });
  }

  /**
   * Connect to a broker with these options.
   *
   * @example
   * ```ts
   * const client = await new ClientOptions()
   *   .withAuth("fibril", "fibril")
   *   .connect("127.0.0.1:9876");
   * ```
   */
  connect(address: string | { host: string; port: number }): Promise<Client> {
    return Client.connect(address, this);
  }
}

/**
 * Queue declaration for retry and dead-letter behavior.
 *
 * Declarations apply to a topic plus optional group. Partition selection is
 * internal.
 */
export class QueueConfig {
  readonly topic: string;
  readonly groupName: string | null;
  readonly dlqPolicy: QueueDlqPolicy | null;
  readonly dlqMaxRetries: number | null;

  constructor(
    topic: string,
    groupName: string | null = null,
    dlqPolicy: QueueDlqPolicy | null = null,
    dlqMaxRetries: number | null = null,
  ) {
    this.topic = topic;
    this.groupName = normalizeGroup(groupName);
    this.dlqPolicy = dlqPolicy;
    this.dlqMaxRetries = dlqMaxRetries;
  }

  group(group: string): QueueConfig {
    return new QueueConfig(
      this.topic,
      normalizeGroup(group),
      this.dlqPolicy,
      this.dlqMaxRetries,
    );
  }

  maxRetries(maxRetries: number): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      this.dlqPolicy,
      maxRetries,
    );
  }

  discardDeadLetters(): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      { kind: "discard" },
      this.dlqMaxRetries,
    );
  }

  useGlobalDeadLetterQueue(): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      { kind: "global" },
      this.dlqMaxRetries,
    );
  }

  customDeadLetterQueue(topic: string, group: string | null = null): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      { kind: "custom", topic, group: normalizeGroup(group) },
      this.dlqMaxRetries,
    );
  }

  toWire(): DeclareQueueMsg {
    return {
      topic: this.topic,
      group: this.groupName,
      dlq_policy: this.dlqPolicy,
      dlq_max_retries: this.dlqMaxRetries,
    };
  }
}

export interface ReconnectOutcome {
  resumeOutcome: ResumeOutcome;
}

function parseAddress(
  address: string | { host: string; port: number },
): { host: string; port: number } {
  if (typeof address === "object") return address;
  // Accept "host:port" or "[ipv6]:port".
  const ipv6Match = /^\[([^\]]+)\]:(\d+)$/.exec(address);
  if (ipv6Match) {
    return { host: ipv6Match[1]!, port: parseInt(ipv6Match[2]!, 10) };
  }
  const idx = address.lastIndexOf(":");
  if (idx === -1) {
    throw new DisconnectionError(`Invalid address (no port): ${address}`);
  }
  const host = address.slice(0, idx);
  const port = parseInt(address.slice(idx + 1), 10);
  if (!Number.isFinite(port)) {
    throw new DisconnectionError(`Invalid port in address: ${address}`);
  }
  return { host, port };
}

// Canonical pool key for a host/port. The broker reports owner endpoints as
// strings, so this stays a plain string compare rather than parsing to an addr.
function endpointKey(host: string, port: number): string {
  return `${host}:${port}`;
}

function openSocket(host: string, port: number): Promise<Socket> {
  return new Promise<Socket>((resolve, reject) => {
    const socket = netConnect({ host, port });
    let settled = false;
    socket.once("connect", () => {
      if (settled) return;
      settled = true;
      // Disable Nagle for lower latency on small frames; matches typical
      // RPC/messaging clients.
      socket.setNoDelay(true);
      resolve(socket);
    });
    socket.once("error", (err) => {
      if (settled) return;
      settled = true;
      reject(
        new DisconnectionError(
          `Failed to connect to ${host}:${port}: ${err.message}`,
        ),
      );
    });
  });
}

class EngineSlot {
  #engine: Engine;

  constructor(engine: Engine) {
    this.#engine = engine;
  }

  current(): Engine {
    return this.#engine;
  }

  replace(engine: Engine): Engine {
    const old = this.#engine;
    this.#engine = engine;
    return old;
  }
}

/**
 * A lazily connected pooled connection to one non-bootstrap owner. Used for
 * routed publishes. Each is its own session (no resume of the bootstrap
 * identity) and reconnects on demand if the engine has closed.
 */
class PooledConnection {
  #engine: Engine | null = null;
  #connecting: Promise<Engine> | null = null;

  constructor(
    private readonly host: string,
    private readonly port: number,
    private readonly opts: ClientOptions,
  ) {}

  async engineForOperation(): Promise<Engine> {
    if (this.#engine && !this.#engine.isClosed()) return this.#engine;
    if (this.#connecting) return this.#connecting;
    this.#connecting = (async () => {
      const socket = await openSocket(this.host, this.port);
      try {
        const engine = await Engine.start(socket, this.opts, new Map());
        this.#engine = engine;
        return engine;
      } catch (err) {
        socket.destroy();
        if (err instanceof FibrilError) throw err;
        throw new DisconnectionError(`Engine failed to start: ${(err as Error).message}`);
      } finally {
        this.#connecting = null;
      }
    })();
    return this.#connecting;
  }

  shutdown(): void {
    this.#engine?.shutdown();
  }
}

/**
 * Fibril broker client. Manages a single connection and dispatches
 * publish/subscribe operations through an internal engine.
 *
 * @example
 * ```ts
 * const client = await Client.connect("127.0.0.1:9876");
 * const publisher = client.publisher("jobs");
 * await publisher.publish({ id: 1 });
 * await client.shutdown();
 * ```
 */
export class Client {
  readonly #address: { host: string; port: number };
  readonly #opts: ClientOptions;
  #engine: EngineSlot;
  #reconnectPromise: Promise<void> | null = null;
  #userShutdown = false;
  readonly #subscriptions: SubscriptionRegistry;
  // Routing cache, warmed by fetchTopology and point-updated by redirects. Empty
  // means "no routing info" (standalone broker or cold client).
  readonly #topology = new TopologyCache();
  // Connections to non-bootstrap owners, keyed by "host:port". The bootstrap
  // connection is the EngineSlot above and is never pooled here.
  readonly #pool = new Map<string, PooledConnection>();
  // Cursor for keyless round-robin partition spread.
  readonly #roundRobin: RoundRobin = { next: 0 };
  readonly #bootstrapEndpoint: string;

  private constructor(
    address: { host: string; port: number },
    opts: ClientOptions,
    engine: Engine,
    subscriptions: SubscriptionRegistry,
  ) {
    this.#address = address;
    this.#opts = opts;
    this.#engine = new EngineSlot(engine);
    this.#subscriptions = subscriptions;
    this.#bootstrapEndpoint = endpointKey(address.host, address.port);
  }

  /**
   * Connect to a broker.
   *
   * The address can be `"host:port"`, `"[ipv6]:port"`, or an object with
   * `{ host, port }`.
   */
  static async connect(
    address: string | { host: string; port: number },
    opts: ClientOptions = new ClientOptions(),
  ): Promise<Client> {
    const addr = parseAddress(address);
    const socket = await openSocket(addr.host, addr.port);
    const subscriptions: SubscriptionRegistry = new Map();
    let engine: Engine;
    try {
      engine = await Engine.start(socket, opts, subscriptions);
    } catch (err) {
      socket.destroy();
      if (err instanceof FibrilError) throw err;
      throw new DisconnectionError(
        `Engine failed to start: ${(err as Error).message}`,
      );
    }
    return new Client(addr, opts, engine, subscriptions);
  }

  /**
   * Replace the internal engine with a new connection.
   *
   * Existing publishers created from this client use the new engine after this
   * returns. Existing active subscriptions remain attached to their original
   * stream until subscription reconciliation is implemented.
   */
  async reconnect(): Promise<ReconnectOutcome> {
    if (this.#reconnectPromise) {
      await this.#reconnectPromise;
    }
    return this.#reconnectOnce();
  }

  async #reconnectOnce(): Promise<ReconnectOutcome> {
    const oldEngine = this.#engine.current();
    const socket = await openSocket(this.#address.host, this.#address.port);
    let engine: Engine;
    try {
      engine = await Engine.start(
        socket,
        this.#opts.withResumeIdentity(oldEngine.resumeIdentity),
        this.#subscriptions,
      );
    } catch (err) {
      socket.destroy();
      if (err instanceof FibrilError) throw err;
      throw new DisconnectionError(
        `Engine failed to start: ${(err as Error).message}`,
      );
    }
    this.#engine.replace(engine);
    this.#userShutdown = false;
    oldEngine.shutdownForReconnect();
    return { resumeOutcome: engine.resumeOutcome };
  }

  async #reconnectIfClosed(): Promise<void> {
    if (!this.#engine.current().isClosed()) return;
    if (this.#userShutdown) {
      throw new BrokenPipeError();
    }
    if (this.#opts.autoReconnectAttempts === 0) {
      throw new BrokenPipeError();
    }

    if (!this.#reconnectPromise) {
      this.#reconnectPromise = (async () => {
        let lastErr: unknown = null;
        for (let attempt = 0; attempt < this.#opts.autoReconnectAttempts; attempt += 1) {
          try {
            await this.#reconnectOnce();
            return;
          } catch (err) {
            lastErr = err;
          }
        }
        if (lastErr instanceof Error) throw lastErr;
        throw new BrokenPipeError();
      })().finally(() => {
        this.#reconnectPromise = null;
      });
    }

    await this.#reconnectPromise;
  }

  /** @internal Used by handles before starting a new operation. */
  async _engineForOperation(): Promise<Engine> {
    await this.#reconnectIfClosed();
    return this.#engine.current();
  }

  /**
   * Get a publisher handle for a topic with no group.
   */
  publisher(topic: string): Publisher {
    return new Publisher(this, topic, null);
  }

  /**
   * Get a publisher handle for a grouped queue.
   *
   * Grouping writes to an optional queue namespace under the topic.
   */
  publisherGrouped(topic: string, group: string): Publisher {
    return new Publisher(this, topic, normalizeGroup(group));
  }

  /**
   * Build a subscription request.
   *
   * Chain `.group(...)` and `.prefetch(...)`, then call `.subManualAck()` or
   * `.subAutoAck()`.
   */
  subscribe(topic: string): SubscriptionBuilder {
    return new SubscriptionBuilder(this, topic);
  }

  /**
   * Fetch the cluster topology and refresh the routing cache. Returns the
   * snapshot. In standalone mode the broker returns an empty topology and the
   * client keeps using its direct connection.
   */
  async fetchTopology(filter: { topic?: string | null; group?: string | null } = {}): Promise<TopologyOkMsg> {
    const topology = await (await this._engineForOperation()).fetchTopology(filter);
    this.#topology.replace(topology);
    this.#topology.lastRefreshMs = Date.now();
    // A full refresh is authoritative: drop pooled connections to endpoints that
    // no longer own anything so a failed-over owner's stale connection is gone.
    const live = this.#topology.endpoints();
    for (const [endpoint, conn] of this.#pool) {
      if (!live.has(endpoint)) {
        conn.shutdown();
        this.#pool.delete(endpoint);
      }
    }
    return topology;
  }

  /** @internal The routing cache, exposed for the pool/router layer and tests. */
  _topology(): TopologyCache {
    return this.#topology;
  }

  /** @internal Choose the partition (and version) for a publish. */
  _route(topic: string, group: string | null, key: Uint8Array | null): Route {
    return routePartition(this.#topology, topic, group, key, this.#roundRobin);
  }

  /** @internal Maximum owner redirects to follow per confirmed publish. */
  _maxRedirects(): number {
    return this.#opts.maxRedirects;
  }

  /** @internal Failover retry budget for a confirmed publish, in milliseconds. */
  _publishTimeoutMs(): number {
    return this.#opts.publishTimeoutMs;
  }

  /**
   * @internal Refresh topology from a reachable node, throttled by the refresh
   * cooldown. Returns true if a refresh actually happened. Used by the publish
   * failover retry to re-resolve the new owner after a transient failure.
   */
  async _refreshTopologyThrottled(): Promise<boolean> {
    const now = Date.now();
    if (now - this.#topology.lastRefreshMs < this.#opts.topologyRefreshCooldownMs) {
      return false;
    }
    try {
      await this.fetchTopology();
      return true;
    } catch {
      return false;
    }
  }

  /** @internal Point-update routing from a redirect (after a misroute). */
  _applyRedirect(redirect: RedirectMsg): void {
    this.#topology.applyRedirect(redirect);
  }

  /**
   * @internal True when the cache holds a real cluster view that does not
   * include this queue, so a publish should fail fast as not-found rather than
   * burning the failover retry budget on a topic that was never declared.
   */
  _isTopicMissing(topic: string, group: string | null): boolean {
    return this.#topology.isPopulated() && !this.#topology.knowsTopic(topic, group);
  }

  /**
   * @internal Resolve the engine that should serve a queue partition. Routing is
   * reactive: a cache hit routes to the owner's pooled connection, a miss (cold
   * cache or standalone) falls back to the bootstrap connection. The cache is
   * filled by redirects and by fetchTopology, never on this hot path.
   */
  async _engineFor(topic: string, partition: number, group: string | null): Promise<Engine> {
    const owner = this.#topology.lookup(topic, partition, group);
    if (!owner || owner.endpoint === this.#bootstrapEndpoint) {
      return this._engineForOperation();
    }
    let conn = this.#pool.get(owner.endpoint);
    if (!conn) {
      const addr = parseAddress(owner.endpoint);
      conn = new PooledConnection(addr.host, addr.port, this.#opts);
      this.#pool.set(owner.endpoint, conn);
    }
    return conn.engineForOperation();
  }

  /**
   * Declare queue retry and dead-letter behavior.
   */
  async declareQueue(config: QueueConfig): Promise<void> {
    const reply = deferred<void>();
    await (await this._engineForOperation()).submit({
      type: "declareQueue",
      req: config.toWire(),
      reply,
    });
    await reply.promise;
  }

  /**
   * Gracefully shut down the client. Pending operations fail with
   * `BrokenPipeError`; subscription iterators throw or terminate.
   */
  async shutdown(): Promise<void> {
    this.#userShutdown = true;
    for (const conn of this.#pool.values()) conn.shutdown();
    this.#pool.clear();
    const engine = this.#engine.current();
    engine.shutdown();
    await engine.whenComplete();
  }

  /** @internal Used by active delivery handles. */
  _engine(): Engine {
    return this.#engine.current();
  }
}
