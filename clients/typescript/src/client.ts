import { connect as netConnect, type Socket } from "node:net";
import { Engine } from "./engine.js";
import { DisconnectionError, FibrilError } from "./errors.js";
import { Publisher } from "./publisher.js";
import { SubscriptionBuilder } from "./subscription.js";
import type { AuthMsg } from "./protocol.js";

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

  constructor(init: ClientOptionsInit = {}) {
    this.clientName = init.clientName ?? DEFAULT_CLIENT_NAME;
    this.clientVersion = init.clientVersion ?? DEFAULT_CLIENT_VERSION;
    this.auth = init.auth;
    this.heartbeatIntervalSeconds = init.heartbeatIntervalSeconds;
  }

  /**
   * Return a copy with username/password authentication configured.
   */
  withAuth(username: string, password: string): ClientOptions {
    return new ClientOptions({
      clientName: this.clientName,
      clientVersion: this.clientVersion,
      auth: { username, password },
      heartbeatIntervalSeconds: this.heartbeatIntervalSeconds,
    });
  }

  /**
   * Return a copy with heartbeat interval configured in seconds.
   */
  withHeartbeatInterval(seconds: number): ClientOptions {
    return new ClientOptions({
      clientName: this.clientName,
      clientVersion: this.clientVersion,
      auth: this.auth,
      heartbeatIntervalSeconds: seconds,
    });
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
  #engine: Engine;

  private constructor(
    address: { host: string; port: number },
    opts: ClientOptions,
    engine: Engine,
  ) {
    this.#address = address;
    this.#opts = opts;
    this.#engine = engine;
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
    let engine: Engine;
    try {
      engine = await Engine.start(socket, opts);
    } catch (err) {
      socket.destroy();
      if (err instanceof FibrilError) throw err;
      throw new DisconnectionError(
        `Engine failed to start: ${(err as Error).message}`,
      );
    }
    return new Client(addr, opts, engine);
  }

  /**
   * Replace the internal engine with a new connection. Existing publishers
   * and subscriptions from the old connection will fail with `BrokenPipeError`.
   */
  async reconnect(): Promise<void> {
    const oldEngine = this.#engine;
    const socket = await openSocket(this.#address.host, this.#address.port);
    let engine: Engine;
    try {
      engine = await Engine.start(socket, this.#opts);
    } catch (err) {
      socket.destroy();
      if (err instanceof FibrilError) throw err;
      throw new DisconnectionError(
        `Engine failed to start: ${(err as Error).message}`,
      );
    }
    this.#engine = engine;
    oldEngine.shutdown();
  }

  /**
   * Get a publisher handle for a topic with no group.
   */
  publisher(topic: string): Publisher {
    return new Publisher(this.#engine, topic, null);
  }

  /**
   * Get a publisher handle for a grouped queue.
   *
   * Grouping is part of the queue identity, alongside topic and partition.
   */
  publisherGrouped(topic: string, group: string): Publisher {
    return new Publisher(this.#engine, topic, group);
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
   * Gracefully shut down the client. Pending operations fail with
   * `BrokenPipeError`; subscription iterators throw or terminate.
   */
  async shutdown(): Promise<void> {
    this.#engine.shutdown();
    await this.#engine.whenComplete();
  }

  /** @internal Used by the SubscriptionBuilder. */
  _engine(): Engine {
    return this.#engine;
  }
}
