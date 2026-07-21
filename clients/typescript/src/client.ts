import { connect as netConnect, isIP, type Socket } from "node:net";
import { connect as tlsConnect, type DetailedPeerCertificate, type TLSSocket } from "node:tls";
import { readFileSync } from "node:fs";
import { createHash, X509Certificate } from "node:crypto";
import {
  Engine,
  SettleContext,
  WRITE_COALESCE_BYTES,
  WRITE_COALESCE_COUNT,
  WRITE_COALESCE_WINDOW_MS,
  type InternalDelivered,
  type InternalInflight,
  type SubscribeResult,
  type SubscriptionRegistry,
} from "./engine.js";
import {
  BrokenPipeError,
  DisconnectionError,
  FibrilError,
  retryAdvice,
  TlsCertificateUntrustedError,
  TlsConfigError,
  TlsHandshakeError,
  TlsClientCertificateRequiredError,
  TlsNotSupportedByBrokerError,
} from "./errors.js";
import { deferred } from "./internal/deferred.js";
import type { BoundedQueue } from "./internal/bounded-queue.js";
import { Publisher } from "./publisher.js";
import { SubscriptionBuilder, StreamSubscriptionBuilder } from "./subscription.js";
import { RoutingClient } from "./routing.js";
import { TopologyCache, routePartition, type Route, type RoundRobin } from "./internal/topology.js";
import type { DeclarePlexus, StreamDurability, StreamRetention, SubscribeStream } from "./wire.js";
import type {
  AssignmentChangedMsg,
  AuthMsg,
  DeclareQueueMsg,
  GoingAwayMsg,
  QueueDlqPolicy,
  ReconcilePolicy,
  RedirectMsg,
  ResumeIdentity,
  ResumeOutcome,
  SubscribeMsg,
  TopologyOkMsg,
} from "./protocol.js";

/** A routed subscription connection: the engine plus its delivery queue. */
export interface SubscribeHandle<R> {
  engine: Engine;
  queue: BoundedQueue<R>;
}

function normalizeGroup(group: string | null): string | null {
  const trimmed = group?.trim();
  if (!trimmed || trimmed === "default") return null;
  return trimmed;
}

/**
 * TLS options for connecting to a TLS-enabled broker.
 *
 * Trust resolution order: `caFingerprint` pin if set, else `caPath` roots,
 * else the OS trust store (for brokers with publicly issued certificates).
 */
export interface TlsOptions {
  /**
   * PEM file with the CA certificate(s) to trust, e.g. the broker's
   * generated `<data_dir>/tls/ca.pem`.
   */
  caPath?: string;
  /**
   * SHA-256 fingerprint of the broker CA (or server) certificate, as
   * printed in the broker startup log. Hex digits, colons optional.
   * Pinning replaces the CA trust store: the broker is trusted only when the
   * pinned certificate is its leaf, or is a CA that genuinely signed the
   * presented leaf (so a CA pin survives leaf rotation). Hostname verification
   * is skipped because the pin, not a name, is the trust root.
   */
  caFingerprint?: string;
  /**
   * Name verified against the certificate (and sent as SNI). Defaults to
   * the host part of the connect address.
   */
  serverName?: string;
  /**
   * PEM client certificate chain presented to the broker, for
   * `tls.client_auth` deployments. Set together with `keyPath`.
   */
  certPath?: string;
  /** PEM private key for the client certificate. */
  keyPath?: string;
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
  /** TLS options. Absent connects plaintext. */
  tls?: TlsOptions;
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
  /**
   * Whether a supervised subscription silently re-subscribes when the broker
   * advises a safe recreate (the default). Off, a recreate ends the
   * subscription with the typed close reason instead.
   */
  autoResubscribe?: boolean;
  /** Maximum owner redirects to follow for a single confirmed publish. */
  maxRedirects?: number;
  /**
   * Budget in milliseconds for retrying a confirmed publish across a transient
   * owner failover. 0 disables retry so the first transport failure fails fast.
   */
  publishTimeoutMs?: number;
  /** Minimum gap between throttled topology refreshes during retries. */
  topologyRefreshCooldownMs?: number;
  /**
   * Whether subscriptions ride through an owner failover by re-subscribing to
   * the new owner. null disables supervision (a dropped stream just ends).
   */
  superviseSubscriptions?: boolean;
  /**
   * How often (ms) a supervised subscription re-checks the topology owner to
   * detect a graceful owner move that did not drop the connection.
   */
  subscriptionSuperviseIntervalMs?: number;
  /**
   * Coalesce fire-and-forget writes (unconfirmed publishes) until this many
   * buffered bytes, then flush in one socket write. Tune with
   * {@link ClientOptions.withWriteCoalescing}.
   */
  writeCoalesceBytes?: number;
  /** Coalesce fire-and-forget writes until this many buffered frames. */
  writeCoalesceCount?: number;
  /** Flush coalesced fire-and-forget writes within this many ms of the last flush. */
  writeCoalesceWindowMs?: number;
}

const DEFAULT_CLIENT_NAME = "Fibril TS Client";
const DEFAULT_CLIENT_VERSION = "0.4.0";

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
  readonly autoResubscribe: boolean;
  readonly maxRedirects: number;
  readonly publishTimeoutMs: number;
  readonly topologyRefreshCooldownMs: number;
  readonly superviseSubscriptions: boolean;
  readonly subscriptionSuperviseIntervalMs: number;
  readonly tls: TlsOptions | undefined;
  readonly writeCoalesceBytes: number;
  readonly writeCoalesceCount: number;
  readonly writeCoalesceWindowMs: number;

  constructor(init: ClientOptionsInit = {}) {
    this.tls = init.tls;
    this.clientName = init.clientName ?? DEFAULT_CLIENT_NAME;
    this.clientVersion = init.clientVersion ?? DEFAULT_CLIENT_VERSION;
    this.auth = init.auth;
    this.heartbeatIntervalSeconds = init.heartbeatIntervalSeconds;
    this.resumeIdentity = init.resumeIdentity;
    this.autoReconnectAttempts = init.autoReconnectAttempts ?? 1;
    this.reconnectReconcilePolicy = init.reconnectReconcilePolicy ?? "conservative";
    this.autoResubscribe = init.autoResubscribe ?? true;
    this.maxRedirects = init.maxRedirects ?? 3;
    this.publishTimeoutMs = init.publishTimeoutMs ?? 30_000;
    this.topologyRefreshCooldownMs = init.topologyRefreshCooldownMs ?? 1_000;
    this.superviseSubscriptions = init.superviseSubscriptions ?? true;
    this.subscriptionSuperviseIntervalMs = init.subscriptionSuperviseIntervalMs ?? 1_000;
    this.writeCoalesceBytes = init.writeCoalesceBytes ?? WRITE_COALESCE_BYTES;
    this.writeCoalesceCount = init.writeCoalesceCount ?? WRITE_COALESCE_COUNT;
    this.writeCoalesceWindowMs = init.writeCoalesceWindowMs ?? WRITE_COALESCE_WINDOW_MS;
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
      autoResubscribe: this.autoResubscribe,
      maxRedirects: this.maxRedirects,
      publishTimeoutMs: this.publishTimeoutMs,
      topologyRefreshCooldownMs: this.topologyRefreshCooldownMs,
      superviseSubscriptions: this.superviseSubscriptions,
      subscriptionSuperviseIntervalMs: this.subscriptionSuperviseIntervalMs,
      tls: this.tls,
      writeCoalesceBytes: this.writeCoalesceBytes,
      writeCoalesceCount: this.writeCoalesceCount,
      writeCoalesceWindowMs: this.writeCoalesceWindowMs,
      ...overrides,
    });
  }

  /**
   * Return a copy with fire-and-forget write coalescing tuned. Unconfirmed
   * publishes are buffered and sent in one socket write, flushed on whichever
   * limit is reached first: `maxBytes` buffered, `maxFrames` buffered, or
   * `windowMs` since the last flush. Reply-bearing frames (confirmed publishes,
   * acks, requests) always flush immediately. Larger limits trade a little
   * latency for fewer syscalls; the defaults already sit at the throughput
   * plateau, so this is mainly for tightening latency or memory, or disabling
   * coalescing (`maxFrames: 1`). Only the limits passed change.
   */
  withWriteCoalescing(limits: {
    maxBytes?: number;
    maxFrames?: number;
    windowMs?: number;
  }): ClientOptions {
    const { maxBytes, maxFrames, windowMs } = limits;
    if (maxBytes !== undefined && (!Number.isFinite(maxBytes) || maxBytes < 1)) {
      throw new Error("maxBytes must be a positive number");
    }
    if (maxFrames !== undefined && (!Number.isInteger(maxFrames) || maxFrames < 1)) {
      throw new Error("maxFrames must be a positive integer");
    }
    if (windowMs !== undefined && (!Number.isFinite(windowMs) || windowMs < 0)) {
      throw new Error("windowMs must be a non-negative number");
    }
    return this.#copy({
      ...(maxBytes !== undefined ? { writeCoalesceBytes: maxBytes } : {}),
      ...(maxFrames !== undefined ? { writeCoalesceCount: maxFrames } : {}),
      ...(windowMs !== undefined ? { writeCoalesceWindowMs: windowMs } : {}),
    });
  }

  /**
   * Return a copy with username/password authentication configured.
   */
  withAuth(username: string, password: string): ClientOptions {
    return this.#copy({ auth: { username, password } });
  }

  /**
   * Return a copy with TLS enabled using the OS trust store, for brokers
   * with publicly issued certificates.
   */
  withTls(): ClientOptions {
    return this.#copy({ tls: { ...(this.tls ?? {}) } });
  }

  /**
   * Return a copy with TLS enabled, trusting the CA certificate(s) in a PEM
   * file, e.g. the broker's generated `<data_dir>/tls/ca.pem`.
   */
  withTlsCaPath(caPath: string): ClientOptions {
    return this.#copy({ tls: { ...(this.tls ?? {}), caPath } });
  }

  /**
   * Return a copy with TLS enabled, pinning the broker certificate by the
   * SHA-256 fingerprint printed in the broker startup log (colons optional).
   * Pinning replaces the CA trust store: the broker is trusted only when the
   * pinned certificate is its leaf, or is a CA that genuinely signed the
   * presented leaf (so a CA pin survives leaf rotation).
   */
  withTlsCaFingerprint(caFingerprint: string): ClientOptions {
    return this.#copy({ tls: { ...(this.tls ?? {}), caFingerprint } });
  }

  /**
   * Return a copy with TLS enabled and an explicit certificate name to
   * verify (and send as SNI), when the connect address is not the name on
   * the certificate.
   */
  withTlsServerName(serverName: string): ClientOptions {
    return this.#copy({ tls: { ...(this.tls ?? {}), serverName } });
  }

  /**
   * Present a client certificate (PEM chain and key) to the broker, for
   * `tls.client_auth` deployments. Enables TLS if not already enabled.
   */
  withTlsClientCert(certPath: string, keyPath: string): ClientOptions {
    return this.#copy({ tls: { ...(this.tls ?? {}), certPath, keyPath } });
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
   * Return a copy toggling whether a supervised subscription silently
   * re-subscribes on a recreate verdict (default on). Off, a recreate ends
   * the subscription with the typed close reason.
   */
  withAutoResubscribe(enabled: boolean): ClientOptions {
    return this.#copy({ autoResubscribe: enabled });
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
  readonly defaultMessageTtlMs: bigint | null;

  constructor(
    topic: string,
    groupName: string | null = null,
    dlqPolicy: QueueDlqPolicy | null = null,
    dlqMaxRetries: number | null = null,
    defaultMessageTtlMs: bigint | null = null,
  ) {
    this.topic = topic;
    this.groupName = normalizeGroup(groupName);
    this.dlqPolicy = dlqPolicy;
    this.dlqMaxRetries = dlqMaxRetries;
    this.defaultMessageTtlMs = defaultMessageTtlMs;
  }

  group(group: string): QueueConfig {
    return new QueueConfig(
      this.topic,
      normalizeGroup(group),
      this.dlqPolicy,
      this.dlqMaxRetries,
      this.defaultMessageTtlMs,
    );
  }

  maxRetries(maxRetries: number): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      this.dlqPolicy,
      maxRetries,
      this.defaultMessageTtlMs,
    );
  }

  /**
   * Set a default message TTL (milliseconds) for this queue: messages published
   * without their own TTL drop after this age. Per-message expiry, not queue
   * expiration (auto-deleting an idle queue).
   */
  defaultMessageTtl(ttlMs: number): QueueConfig {
    if (!Number.isFinite(ttlMs) || ttlMs < 0) {
      throw new Error("ttl must be a non-negative millisecond value");
    }
    return new QueueConfig(
      this.topic,
      this.groupName,
      this.dlqPolicy,
      this.dlqMaxRetries,
      BigInt(Math.trunc(ttlMs)),
    );
  }

  discardDeadLetters(): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      { kind: "discard" },
      this.dlqMaxRetries,
      this.defaultMessageTtlMs,
    );
  }

  useGlobalDeadLetterQueue(): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      { kind: "global" },
      this.dlqMaxRetries,
      this.defaultMessageTtlMs,
    );
  }

  customDeadLetterQueue(topic: string, group: string | null = null): QueueConfig {
    return new QueueConfig(
      this.topic,
      this.groupName,
      { kind: "custom", topic, group: normalizeGroup(group) },
      this.dlqMaxRetries,
      this.defaultMessageTtlMs,
    );
  }

  toWire(): DeclareQueueMsg {
    return {
      topic: this.topic,
      group: this.groupName,
      dlq_policy: this.dlqPolicy,
      dlq_max_retries: this.dlqMaxRetries,
      default_message_ttl_ms: this.defaultMessageTtlMs,
    };
  }
}

/**
 * Declaration for a Plexus (fan-out stream) channel. Immutable builder: each
 * method returns a new config. A stream delivers every record to every consumer.
 *
 * Durability tiers trade latency for durability: `durable` fsyncs before
 * delivering and confirming, `speculative` delivers early and defers the confirm
 * until durable, and `ephemeral` delivers and confirms without an fsync.
 */
export class StreamConfig {
  readonly topic: string;
  readonly partitionCount: number | null;
  readonly durability: StreamDurability;
  readonly retention: StreamRetention;
  readonly replicationFactorValue: number | null;

  constructor(
    topic: string,
    partitionCount: number | null = null,
    durability: StreamDurability = "durable",
    retention: StreamRetention = { maxAgeMs: null, maxBytes: null, retainRecords: null },
    replicationFactor: number | null = null,
  ) {
    this.topic = topic;
    this.partitionCount = partitionCount;
    this.durability = durability;
    this.retention = retention;
    this.replicationFactorValue = replicationFactor;
  }

  #with(
    patch: Partial<{
      partitionCount: number | null;
      durability: StreamDurability;
      retention: StreamRetention;
      replicationFactor: number | null;
    }>,
  ): StreamConfig {
    return new StreamConfig(
      this.topic,
      patch.partitionCount ?? this.partitionCount,
      patch.durability ?? this.durability,
      patch.retention ?? this.retention,
      patch.replicationFactor ?? this.replicationFactorValue,
    );
  }

  /** Request a partition count. When unset, the cluster default applies. */
  partitions(count: number): StreamConfig {
    if (count < 1 || !Number.isInteger(count)) {
      throw new Error("partitions must be a positive integer");
    }
    return this.#with({ partitionCount: count });
  }

  /** Persist asynchronously without gating delivery or confirm (lowest latency). */
  ephemeral(): StreamConfig {
    return this.#with({ durability: "ephemeral" });
  }

  /** Deliver immediately with a speculative marker, defer the confirm. */
  speculative(): StreamConfig {
    return this.#with({ durability: "speculative" });
  }

  /** Persist before confirming (the default). */
  durable(): StreamConfig {
    return this.#with({ durability: "durable" });
  }

  /** Drop records older than this age in milliseconds. */
  retainForMs(ms: number): StreamConfig {
    return this.#with({ retention: { ...this.retention, maxAgeMs: BigInt(Math.trunc(ms)) } });
  }

  /** Keep at most this many bytes of retained records. */
  retainBytes(bytes: number | bigint): StreamConfig {
    return this.#with({ retention: { ...this.retention, maxBytes: BigInt(bytes) } });
  }

  /** Keep at most this many records. */
  retainRecords(records: number | bigint): StreamConfig {
    return this.#with({ retention: { ...this.retention, retainRecords: BigInt(records) } });
  }

  /**
   * Per-stream durable-tier replication factor (follower count). When unset, the
   * cluster default applies. Only the durable tier replicates; the express tiers
   * stay owner-only. A value of 0 makes a durable stream owner-only.
   */
  replicationFactor(replicas: number): StreamConfig {
    if (replicas < 0 || !Number.isInteger(replicas)) {
      throw new Error("replicationFactor must be a non-negative integer");
    }
    return this.#with({ replicationFactor: replicas });
  }

  toWire(): DeclarePlexus {
    return {
      topic: this.topic,
      partitionCount: this.partitionCount,
      durability: this.durability,
      retention: this.retention,
      replicationFactor: this.replicationFactorValue,
    };
  }
}

export interface ReconnectOutcome {
  resumeOutcome: ResumeOutcome;
}

function parseAddress(address: string | { host: string; port: number }): {
  host: string;
  port: number;
} {
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

function openSocket(host: string, port: number, tls?: TlsOptions): Promise<Socket> {
  if (tls) return openTlsSocket(host, port, tls);
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
      // A refused connection is the most common first-run stumble: name the
      // two checks that resolve almost all of them.
      const detail =
        (err as NodeJS.ErrnoException).code === "ECONNREFUSED"
          ? `connection refused by ${host}:${port}. Is the broker running and reachable ` +
            `there? Clients connect to the broker port (default 9876), not the admin API ` +
            `or dashboard port (default 8081)`
          : `Failed to connect to ${host}:${port}: ${err.message}`;
      reject(new DisconnectionError(detail));
    });
  });
}

function normalizeFingerprint(raw: string): string {
  const hex = raw.replace(/[:\s]/g, "").toLowerCase();
  if (!/^[0-9a-f]{64}$/.test(hex)) {
    throw new TlsConfigError("tls caFingerprint must be 64 hex digits (SHA-256, colons optional)");
  }
  return hex;
}

/**
 * The certificates the broker presented, leaf first, parsed from the socket.
 * Walks issuerCertificate links, which end in a self-referential root.
 */
function presentedChain(socket: TLSSocket): X509Certificate[] {
  const out: X509Certificate[] = [];
  const seen = new Set<string>();
  let cert: DetailedPeerCertificate | null = socket.getPeerCertificate(true);
  while (cert && cert.raw) {
    const parsed = new X509Certificate(cert.raw);
    if (seen.has(parsed.fingerprint256)) break;
    seen.add(parsed.fingerprint256);
    out.push(parsed);
    cert = cert.issuerCertificate ?? null;
  }
  return out;
}

/**
 * Enforces a SHA-256 certificate pin against the presented chain (ordered leaf
 * first). A fingerprint can pin either the leaf or an issuer:
 *
 *   - Leaf pin: the pin matches the presented leaf. The handshake already proved
 *     possession of the leaf's key, so the match stands on its own.
 *   - CA pin: the pin matches an issuer (so the leaf can rotate under the same
 *     CA). The pinned certificate merely appearing in the chain proves nothing,
 *     because the real CA certificate is public and a man-in-the-middle can
 *     staple it beside a rogue leaf it controls. So the leaf is path-validated
 *     against the pinned certificate as the sole trust root, and accepted only
 *     if an unbroken, signature-verified path from the leaf reaches it.
 */
export function verifyPinnedChain(chain: X509Certificate[], pin: string): boolean {
  const sha256Hex = (der: Buffer) => createHash("sha256").update(der).digest("hex");
  const now = Date.now();
  const valid = (cert: X509Certificate) =>
    now >= new Date(cert.validFrom).getTime() && now <= new Date(cert.validTo).getTime();

  let child: X509Certificate | undefined;
  for (const cert of chain) {
    if (child === undefined) {
      // The leaf. A leaf pin matches here; the handshake proved its key.
      if (sha256Hex(cert.raw) === pin) return true;
    } else {
      // For the path to hold, `cert` must be the in-validity issuer that signed
      // the previous certificate. A CA pin is accepted once such a validated
      // path reaches the pinned certificate.
      if (!valid(child) || !child.checkIssued(cert) || !child.verify(cert.publicKey)) {
        return false;
      }
      if (valid(cert) && sha256Hex(cert.raw) === pin) return true;
    }
    child = cert;
  }
  return false;
}

// Node OpenSSL verification codes that mean the certificate could not be
// trusted, as opposed to a transport-level handshake failure.
const CERT_ERROR_CODES = new Set([
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
  "SELF_SIGNED_CERT_IN_CHAIN",
  "DEPTH_ZERO_SELF_SIGNED_CERT",
  "UNABLE_TO_GET_ISSUER_CERT",
  "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
  "CERT_HAS_EXPIRED",
  "CERT_NOT_YET_VALID",
  "CERT_UNTRUSTED",
  "CERT_SIGNATURE_FAILURE",
  "ERR_TLS_CERT_ALTNAME_INVALID",
]);

/**
 * Sort a failed TLS handshake into the taxonomy: an early end of the
 * connection is almost always a plaintext broker (the plaintext listener
 * closes on sighting a ClientHello), certificate failures are trust
 * configuration, everything else stays a generic handshake error.
 */
function classifyTlsError(err: NodeJS.ErrnoException, host: string, port: number): FibrilError {
  const code = err.code ?? "";
  if (isCertificateRequired(err)) {
    return new TlsClientCertificateRequiredError(
      `the broker at ${host}:${port} requires a client certificate: provide one \
with withTlsClientCert(certPath, keyPath). Deployment-CA certificates are \
issued with fibrilctl cert issue`,
    );
  }
  if (code === "ECONNRESET" || /disconnected before secure TLS connection/i.test(err.message)) {
    return new TlsNotSupportedByBrokerError(`${host}:${port}`);
  }
  if (CERT_ERROR_CODES.has(code)) {
    return new TlsCertificateUntrustedError(err.message);
  }
  return new TlsHandshakeError(`TLS handshake failed: ${err.message}`);
}

/**
 * With TLS 1.3 the client side of the handshake completes before the
 * broker's client-certificate verdict, so a `require` rejection can land on
 * the first read after connect instead of in the handshake itself.
 */
function isCertificateRequired(err: Error): boolean {
  const code = (err as NodeJS.ErrnoException).code ?? "";
  return (
    code === "ERR_SSL_TLSV13_ALERT_CERTIFICATE_REQUIRED" ||
    /certificate required/i.test(err.message)
  );
}

function openTlsSocket(host: string, port: number, tls: TlsOptions): Promise<Socket> {
  let ca: Buffer | undefined;
  if (tls.caPath !== undefined) {
    try {
      ca = readFileSync(tls.caPath);
    } catch (err) {
      return Promise.reject(
        new TlsConfigError(`failed to read tls caPath ${tls.caPath}: ${(err as Error).message}`),
      );
    }
  }
  let pin: string | undefined;
  if (tls.caFingerprint !== undefined) {
    try {
      pin = normalizeFingerprint(tls.caFingerprint);
    } catch (err) {
      return Promise.reject(err);
    }
  }
  if ((tls.certPath === undefined) !== (tls.keyPath === undefined)) {
    return Promise.reject(
      new TlsConfigError(
        "client certificate options must be set together: both certPath and keyPath",
      ),
    );
  }
  let cert: Buffer | undefined;
  let key: Buffer | undefined;
  if (tls.certPath !== undefined && tls.keyPath !== undefined) {
    try {
      cert = readFileSync(tls.certPath);
      key = readFileSync(tls.keyPath);
    } catch (err) {
      return Promise.reject(
        new TlsConfigError(
          `failed to read tls client certificate material: ${(err as Error).message}`,
        ),
      );
    }
  }
  const servername = tls.serverName ?? (isIP(host) ? undefined : host);
  return new Promise<Socket>((resolve, reject) => {
    const socket = tlsConnect({
      host,
      port,
      ca,
      cert,
      key,
      servername,
      // A pin replaces the CA trust store; the leaf is path-validated against
      // the pinned certificate on secureConnect below. Hostname is not the trust
      // basis here (the pin is), so checkServerIdentity is disabled.
      rejectUnauthorized: pin === undefined,
      ...(pin !== undefined ? { checkServerIdentity: () => undefined } : {}),
    });
    let settled = false;
    socket.once("secureConnect", () => {
      if (settled) return;
      settled = true;
      if (pin !== undefined && !verifyPinnedChain(presentedChain(socket), pin)) {
        socket.destroy();
        reject(
          new TlsCertificateUntrustedError(
            "the presented certificate is not the pinned certificate, nor signed by the pinned CA",
          ),
        );
        return;
      }
      socket.setNoDelay(true);
      resolve(socket);
    });
    socket.once("error", (err) => {
      if (settled) return;
      settled = true;
      reject(classifyTlsError(err, host, port));
    });
  });
}

class EngineSlot {
  #engine: Engine;
  /** The bootstrap connection's persistent settle router, shared across every
   * engine this slot swaps through so a held delivery settles against the
   * current one. Handed to each bootstrap `Engine.start`. */
  readonly settle: SettleContext;

  constructor(engine: Engine, settle: SettleContext) {
    this.#engine = engine;
    this.settle = settle;
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
  // This pooled owner's persistent settle router, shared across its reconnects
  // so a held delivery settles against the current engine (or goes stale).
  readonly #settle = new SettleContext();

  constructor(
    private readonly host: string,
    private readonly port: number,
    private readonly opts: ClientOptions,
    private readonly onAssignmentChanged?: (event: AssignmentChangedMsg) => void,
    private readonly onTopologyUpdate?: (topology: TopologyOkMsg) => bigint,
    private readonly onGoingAway?: (notice: GoingAwayMsg) => void,
  ) {}

  async engineForOperation(): Promise<Engine> {
    if (this.#engine && !this.#engine.isClosed()) return this.#engine;
    if (this.#connecting) return this.#connecting;
    this.#connecting = (async () => {
      const socket = await openSocket(this.host, this.port, this.opts.tls);
      try {
        const engine = await Engine.start(
          socket,
          this.opts,
          this.#settle,
          new Map(),
          this.onAssignmentChanged,
          this.onTopologyUpdate,
          this.onGoingAway,
        );
        this.#engine = engine;
        return engine;
      } catch (err) {
        socket.destroy();
        if (this.opts.tls && isCertificateRequired(err as Error)) {
          throw new TlsClientCertificateRequiredError(
            `the broker at ${this.host}:${this.port} requires a client certificate: \
provide one with withTlsClientCert(certPath, keyPath)`,
          );
        }
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
 * Drop pooled connections to endpoints that no longer own anything, so a
 * failed-over owner's stale connection is gone. A full topology view (refresh or
 * push) is authoritative about the live owner set.
 */
function prunePoolToTopology(cache: TopologyCache, pool: Map<string, PooledConnection>): void {
  const live = cache.endpoints();
  for (const [endpoint, conn] of pool) {
    if (!live.has(endpoint)) {
      conn.shutdown();
      pool.delete(endpoint);
    }
  }
}

/**
 * Apply a broker-pushed topology snapshot to the routing cache and prune the
 * pool, mirroring fetchTopology's apply path. A push only moves the cache
 * forward: a stale push (older generation than the cache already holds) is
 * ignored so an out-of-order delivery cannot regress routing. Returns the
 * generation the cache reflects after the call, which the engine acks.
 */
function applyPushedTopology(
  topology: TopologyOkMsg,
  cache: TopologyCache,
  pool: Map<string, PooledConnection>,
): bigint {
  if (topology.generation > cache.generation) {
    cache.replace(topology);
    cache.lastRefreshMs = Date.now();
    prunePoolToTopology(cache, pool);
  }
  return cache.generation;
}

/** A declared queue as seen in the cluster {@link Catalogue}. */
export interface QueueInfo {
  topic: string;
  /** The queue's group namespace, or null for the ungrouped default. */
  group: string | null;
  partitionCount: number;
}

/** A declared Plexus stream as seen in the cluster {@link Catalogue}. */
export interface StreamInfo {
  topic: string;
  partitionCount: number;
}

/**
 * A snapshot of the channels declared in the cluster: every queue and Plexus
 * stream the client currently knows about, with partition counts. Derived from
 * the topology and kept live by topology pushes, so it needs no extra
 * round-trips. `queues` and `streams` are sorted (by topic, then group) for a
 * stable order. Read the current snapshot with {@link Client.catalogue} or
 * subscribe to changes with {@link Client.onCatalogueChange}.
 */
export interface Catalogue {
  queues: QueueInfo[];
  streams: StreamInfo[];
  generation: bigint;
}

/** Shared catalogue state: the latest snapshot plus its change listeners. Created
 * in connect() before the bootstrap engine starts, so a push has somewhere to
 * land with no wiring race (same reasoning as the routing cache and pool). */
interface CatalogueState {
  current: Catalogue;
  listeners: Set<(catalogue: Catalogue) => void>;
}

function emptyCatalogue(): Catalogue {
  return { queues: [], streams: [], generation: 0n };
}

/** Derive the catalogue from a topology snapshot. The topology lists one entry
 * per partition, so queues dedupe by (topic, group) and streams by topic; both
 * are returned sorted for a deterministic order. */
function catalogueFromTopology(topology: TopologyOkMsg): Catalogue {
  const queues = new Map<string, QueueInfo>();
  for (const e of topology.queues) {
    // "/" is rejected in topic and group names, so it is a safe dedupe-key join.
    queues.set(e.topic + "/" + (e.group ?? ""), {
      topic: e.topic,
      group: e.group,
      partitionCount: Math.max(e.partition_count, 1),
    });
  }
  const streams = new Map<string, StreamInfo>();
  for (const e of topology.streams ?? []) {
    streams.set(e.topic, {
      topic: e.topic,
      partitionCount: Math.max(e.partition_count, 1),
    });
  }
  return {
    queues: [...queues.values()].sort((a, b) =>
      a.topic === b.topic
        ? (a.group ?? "").localeCompare(b.group ?? "")
        : a.topic.localeCompare(b.topic),
    ),
    streams: [...streams.values()].sort((a, b) => a.topic.localeCompare(b.topic)),
    generation: topology.generation,
  };
}

function cataloguesEqual(a: Catalogue, b: Catalogue): boolean {
  if (a.queues.length !== b.queues.length || a.streams.length !== b.streams.length) {
    return false;
  }
  return (
    a.queues.every((x, i) => {
      const y = b.queues[i]!;
      return x.topic === y.topic && x.group === y.group && x.partitionCount === y.partitionCount;
    }) &&
    a.streams.every((x, i) => {
      const y = b.streams[i]!;
      return x.topic === y.topic && x.partitionCount === y.partitionCount;
    })
  );
}

/**
 * Refresh the catalogue snapshot from a full topology and notify listeners if the
 * set of declared queues or streams changed. Monotonic and self-guarding: a stale
 * topology (generation not newer than the snapshot already held) is ignored.
 * Owner-only churn updates the stored generation but fires no listener.
 */
function refreshCatalogue(topology: TopologyOkMsg, state: CatalogueState): void {
  const next = catalogueFromTopology(topology);
  const prev = state.current;
  if (next.generation <= prev.generation && prev.generation !== 0n) {
    return;
  }
  const changed = !cataloguesEqual(prev, next);
  state.current = next;
  if (changed) {
    for (const listener of [...state.listeners]) {
      try {
        listener(next);
      } catch {
        // A listener throwing must not break frame processing.
      }
    }
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
  // Routing cache, warmed by fetchTopology, point-updated by redirects, and
  // replaced by broker topology pushes. Empty means "no routing info" (standalone
  // broker or cold client). Created in connect() before the bootstrap engine
  // starts so a topology push has somewhere to land with no wiring race.
  readonly #topology: TopologyCache;
  // Connections to non-bootstrap owners, keyed by "host:port". The bootstrap
  // connection is the EngineSlot above and is never pooled here.
  readonly #pool: Map<string, PooledConnection>;
  // Latest catalogue snapshot plus its change listeners. Created in connect()
  // before the bootstrap engine starts (same race reasoning as the routing cache).
  readonly #catalogue: CatalogueState;
  // Cursor for keyless round-robin partition spread.
  readonly #roundRobin: RoundRobin = { next: 0 };
  readonly #bootstrapEndpoint: string;
  // Cluster-scoped cohort member id, minted by the server on the first exclusive
  // subscribe and carried on every later one (across partitions, reconnects, and
  // brokers) so the cohort recognizes this client as one member.
  #cohortMemberId: Uint8Array | null = null;
  // Client-level fan-out of exclusive-cohort assignment changes. Shared across
  // every engine (bootstrap, reconnect, pool) so the stream survives reconnects
  // and owner moves. Lossy broadcast, like the Rust client.
  readonly #assignmentListeners: Set<(event: AssignmentChangedMsg) => void>;
  // Client-level fan-out of broker drain notices, shared across every engine so
  // the stream survives reconnects. Lossy, like the assignment stream.
  readonly #goingAwayListeners: Set<(notice: GoingAwayMsg) => void>;

  // Bound so it can be handed to every Engine.start as the assignment callback.
  readonly #emitAssignment = (event: AssignmentChangedMsg): void => {
    for (const listener of [...this.#assignmentListeners]) {
      try {
        listener(event);
      } catch {
        // A listener throwing must not break frame processing.
      }
    }
  };

  // Bound so it can be handed to every Engine.start as the drain callback.
  readonly #emitGoingAway = (notice: GoingAwayMsg): void => {
    for (const listener of [...this.#goingAwayListeners]) {
      try {
        listener(notice);
      } catch {
        // A listener throwing must not break frame processing.
      }
    }
  };

  // Bound so it can be handed to every Engine.start as the topology-push
  // callback: apply the pushed routing snapshot and return the generation to ack.
  readonly #onTopologyUpdate = (topology: TopologyOkMsg): bigint =>
    this._applyPushedTopology(topology);

  private constructor(
    address: { host: string; port: number },
    opts: ClientOptions,
    engine: Engine,
    settle: SettleContext,
    subscriptions: SubscriptionRegistry,
    assignmentListeners: Set<(event: AssignmentChangedMsg) => void>,
    goingAwayListeners: Set<(notice: GoingAwayMsg) => void>,
    topology: TopologyCache,
    pool: Map<string, PooledConnection>,
    catalogue: CatalogueState,
  ) {
    this.#address = address;
    this.#opts = opts;
    this.#engine = new EngineSlot(engine, settle);
    this.#subscriptions = subscriptions;
    this.#assignmentListeners = assignmentListeners;
    this.#goingAwayListeners = goingAwayListeners;
    this.#topology = topology;
    this.#pool = pool;
    this.#catalogue = catalogue;
    this.#bootstrapEndpoint = endpointKey(address.host, address.port);
  }

  /**
   * The current cluster catalogue: every queue and Plexus stream this client
   * knows about, with partition counts. Read straight from the cached topology -
   * no round-trip. Empty on a cold or standalone client; kept live by broker
   * topology pushes.
   */
  catalogue(): Catalogue {
    return this.#catalogue.current;
  }

  /**
   * Observe cluster catalogue changes. The handler fires with a full
   * {@link Catalogue} snapshot when the set of declared queues or streams changes
   * (a channel added or removed, or a partition count change). Owner-only
   * failover churn does not fire. Returns an unsubscribe function.
   */
  onCatalogueChange(handler: (catalogue: Catalogue) => void): () => void {
    this.#catalogue.listeners.add(handler);
    return () => {
      this.#catalogue.listeners.delete(handler);
    };
  }

  /**
   * Observe exclusive-cohort assignment changes for this client's subscriptions.
   * The handler fires when the broker reports this client's partition set
   * changed for a cohort it joined via `SubscriptionBuilder.consumerGroup`.
   * Purely informational - the broker's per-partition gate enforces exclusivity
   * regardless. Returns an unsubscribe function. The stream survives reconnects.
   */
  onAssignmentChange(handler: (event: AssignmentChangedMsg) => void): () => void {
    this.#assignmentListeners.add(handler);
    return () => {
      this.#assignmentListeners.delete(handler);
    };
  }

  /**
   * Observe broker drain notices ({@link GoingAwayMsg}). The handler fires when
   * the broker announces it is draining for a planned shutdown or upgrade, so the
   * app can stop producing or finish in-flight work before the connection drops.
   * Returns an unsubscribe function. The stream survives reconnects.
   */
  onGoingAway(handler: (notice: GoingAwayMsg) => void): () => void {
    this.#goingAwayListeners.add(handler);
    return () => {
      this.#goingAwayListeners.delete(handler);
    };
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
    const socket = await openSocket(addr.host, addr.port, opts.tls);
    const subscriptions: SubscriptionRegistry = new Map();
    // The listener set is shared with the Client instance (and so with every
    // engine it later creates), so the bootstrap engine routes into the same set.
    const assignmentListeners = new Set<(event: AssignmentChangedMsg) => void>();
    const emitAssignment = (event: AssignmentChangedMsg): void => {
      for (const listener of [...assignmentListeners]) {
        try {
          listener(event);
        } catch {
          // ignore listener errors
        }
      }
    };
    const goingAwayListeners = new Set<(notice: GoingAwayMsg) => void>();
    const emitGoingAway = (notice: GoingAwayMsg): void => {
      for (const listener of [...goingAwayListeners]) {
        try {
          listener(notice);
        } catch {
          // ignore listener errors
        }
      }
    };
    // The routing cache and pool are created here, before the bootstrap engine
    // starts, so a topology push the broker sends right after HELLO has somewhere
    // to land with no wiring race. The Client below shares these exact objects.
    const topology = new TopologyCache();
    const pool = new Map<string, PooledConnection>();
    const catalogueState: CatalogueState = {
      current: emptyCatalogue(),
      listeners: new Set(),
    };
    const onTopologyUpdate = (t: TopologyOkMsg): bigint => {
      const generation = applyPushedTopology(t, topology, pool);
      refreshCatalogue(t, catalogueState);
      return generation;
    };
    const settle = new SettleContext();
    let engine: Engine;
    try {
      engine = await Engine.start(
        socket,
        opts,
        settle,
        subscriptions,
        emitAssignment,
        onTopologyUpdate,
        emitGoingAway,
      );
    } catch (err) {
      socket.destroy();
      if (opts.tls && isCertificateRequired(err as Error)) {
        throw new TlsClientCertificateRequiredError(
          `the broker at ${addr.host}:${addr.port} requires a client certificate: \
provide one with withTlsClientCert(certPath, keyPath)`,
        );
      }
      if (err instanceof FibrilError) throw err;
      throw new DisconnectionError(`Engine failed to start: ${(err as Error).message}`);
    }
    return new Client(
      addr,
      opts,
      engine,
      settle,
      subscriptions,
      assignmentListeners,
      goingAwayListeners,
      topology,
      pool,
      catalogueState,
    );
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
    const socket = await openSocket(this.#address.host, this.#address.port, this.#opts.tls);
    let engine: Engine;
    try {
      engine = await Engine.start(
        socket,
        this.#opts.withResumeIdentity(oldEngine.resumeIdentity),
        this.#engine.settle,
        this.#subscriptions,
        this.#emitAssignment,
        this.#onTopologyUpdate,
        this.#emitGoingAway,
      );
    } catch (err) {
      socket.destroy();
      if (err instanceof FibrilError) throw err;
      throw new DisconnectionError(`Engine failed to start: ${(err as Error).message}`);
    }
    this.#engine.replace(engine);
    this.#userShutdown = false;
    oldEngine.shutdownForReconnect();
    return { resumeOutcome: engine.resumeOutcome };
  }

  async #reconnectIfClosed(): Promise<void> {
    const current = this.#engine.current();
    if (!current.isClosed()) return;
    if (this.#userShutdown) {
      throw new BrokenPipeError();
    }
    if (this.#opts.autoReconnectAttempts === 0) {
      throw new BrokenPipeError();
    }
    // A non-retryable close (bad credentials, forbidden, malformed request)
    // will only fail again on reconnect, so surface it instead of storming the
    // broker with doomed handshakes while the real error never reaches the caller.
    const reason = current.closeReason();
    if (reason && retryAdvice(reason) === "do_not_retry") {
      throw reason;
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
   * Chain `.group(...)` and `.prefetch(...)`, then call `.sub()` or
   * `.subAutoAck()`.
   */
  subscribe(topic: string): SubscriptionBuilder {
    return new SubscriptionBuilder(this, topic);
  }

  /**
   * Begin a Plexus (fan-out stream) subscription. Every consumer sees every
   * record. Chain `.durable(...)`, `.fromEarliest()`, `.filter(...)`, then call
   * `.sub()` or `.subAutoAck()`.
   */
  stream(topic: string): StreamSubscriptionBuilder {
    return new StreamSubscriptionBuilder(this, topic);
  }

  /**
   * Opt in to the routing/discovery surface. Returns a {@link RoutingClient}
   * sharing this connection; the plain client stays usable. Pattern subscribe
   * and auto-pickup of matching channels live there, off the default surface.
   */
  routing(): RoutingClient {
    return new RoutingClient(this);
  }

  /**
   * Fetch the cluster topology and refresh the routing cache. Returns the
   * snapshot. In standalone mode the broker returns an empty topology and the
   * client keeps using its direct connection.
   */
  async fetchTopology(
    filter: { topic?: string | null; group?: string | null } = {},
  ): Promise<TopologyOkMsg> {
    const topology = await (await this._engineForOperation()).fetchTopology(filter);
    this.#topology.replace(topology);
    this.#topology.lastRefreshMs = Date.now();
    prunePoolToTopology(this.#topology, this.#pool);
    refreshCatalogue(topology, this.#catalogue);
    return topology;
  }

  /**
   * @internal Apply a broker-pushed topology snapshot to the routing cache and
   * prune the pool. Used by the bootstrap engine's push callback (via the shared
   * objects) and by reconnect/pooled engines through `#onTopologyUpdate`.
   */
  _applyPushedTopology(topology: TopologyOkMsg): bigint {
    return applyPushedTopology(topology, this.#topology, this.#pool);
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
      conn = new PooledConnection(
        addr.host,
        addr.port,
        this.#opts,
        this.#emitAssignment,
        this.#onTopologyUpdate,
        this.#emitGoingAway,
      );
      this.#pool.set(owner.endpoint, conn);
    }
    return conn.engineForOperation();
  }

  /** @internal Whether the user has asked the client to shut down. */
  _isShuttingDown(): boolean {
    return this.#userShutdown;
  }

  /** @internal Whether subscriptions should ride through a failover. */
  _superviseSubscriptions(): boolean {
    return this.#opts.superviseSubscriptions;
  }

  /** @internal Whether a supervised subscription auto-resubscribes on recreate. */
  _autoResubscribe(): boolean {
    return this.#opts.autoResubscribe;
  }

  /** @internal How often a supervised subscription re-checks its owner. */
  _superviseIntervalMs(): number {
    return this.#opts.subscriptionSuperviseIntervalMs;
  }

  /**
   * @internal Partitions a subscription fans in over for (topic, group). The
   * count comes from the topology cache. An unknown count (cold cache or
   * standalone) yields just partition 0, the single-partition path. Cache-only
   * by design: subscribe never fetches topology on its own.
   */
  _partitionSet(topic: string, group: string | null): number[] {
    const partitioning = this.#topology.partitioning(topic, group);
    const count = Math.max(partitioning?.count ?? 1, 1);
    return Array.from({ length: count }, (_, i) => i);
  }

  /** @internal Current owner endpoint for a partition, or null if unresolved. */
  _ownerEndpoint(topic: string, partition: number, group: string | null): string | null {
    return this.#topology.lookup(topic, partition, group)?.endpoint ?? null;
  }

  /**
   * @internal Subscribe once to a partition owner with manual ack, returning the
   * engine and its delivery queue. Routes via the topology cache (owner's pooled
   * connection, or bootstrap on a miss). The supervisor calls this to (re)attach
   * a subscription to its current owner.
   */
  async _subscribeManualOnce(req: SubscribeMsg): Promise<SubscribeHandle<InternalInflight>> {
    const reply = deferred<SubscribeResult<InternalInflight>>();
    const fullReq = this.#withCohortMember(req);
    const engine = await this._engineFor(fullReq.topic, fullReq.partition ?? 0, fullReq.group);
    await engine.submit({
      type: "subscribe",
      req: fullReq,
      supervised: this.#opts.superviseSubscriptions,
      reply,
    });
    const result = await reply.promise;
    this.#captureCohortMember(fullReq, result.memberId);
    return { engine, queue: result.queue };
  }

  /** @internal Auto-ack counterpart of _subscribeManualOnce. */
  async _subscribeAutoOnce(req: SubscribeMsg): Promise<SubscribeHandle<InternalDelivered>> {
    const reply = deferred<SubscribeResult<InternalDelivered>>();
    const fullReq = this.#withCohortMember(req);
    const engine = await this._engineFor(fullReq.topic, fullReq.partition ?? 0, fullReq.group);
    await engine.submit({
      type: "subscribeAutoAck",
      req: fullReq,
      supervised: this.#opts.superviseSubscriptions,
      reply,
    });
    const result = await reply.promise;
    this.#captureCohortMember(fullReq, result.memberId);
    return { engine, queue: result.queue };
  }

  // Stamp the current cohort member id onto an exclusive subscribe so every
  // subscribe from this client joins the cohort as the same member.
  #withCohortMember(req: SubscribeMsg): SubscribeMsg {
    if (req.consumer_group == null) return req;
    return { ...req, member_id: req.member_id ?? this.#cohortMemberId };
  }

  // Latch the server-minted member id from the first exclusive subscribe.
  #captureCohortMember(req: SubscribeMsg, memberId: Uint8Array | null): void {
    if (req.consumer_group != null && memberId != null && this.#cohortMemberId == null) {
      this.#cohortMemberId = memberId;
    }
  }

  /**
   * Declare queue retry and dead-letter behavior.
   */
  async declareQueue(config: QueueConfig): Promise<void> {
    const reply = deferred<void>();
    await (
      await this._engineForOperation()
    ).submit({
      type: "declareQueue",
      req: config.toWire(),
      reply,
    });
    await reply.promise;
  }

  /**
   * Declare a Plexus (fan-out stream) channel. See {@link StreamConfig} for
   * partitioning, durability, and retention.
   */
  async declarePlexus(config: StreamConfig): Promise<void> {
    const reply = deferred<void>();
    await (
      await this._engineForOperation()
    ).submit({
      type: "declarePlexus",
      req: config.toWire(),
      reply,
    });
    await reply.promise;
  }

  /**
   * @internal Subscribe once to a stream partition (manual ack). The supervisor
   * calls this to (re)attach to the partition's current owner.
   */
  async _subscribeStreamManualOnce(
    req: SubscribeStream,
  ): Promise<SubscribeHandle<InternalInflight>> {
    const reply = deferred<SubscribeResult<InternalInflight>>();
    const engine = await this._engineFor(req.topic, req.partition, null);
    await engine.submit({ type: "subscribeStream", req, reply });
    const result = await reply.promise;
    return { engine, queue: result.queue };
  }

  /** @internal Auto-ack counterpart of _subscribeStreamManualOnce. */
  async _subscribeStreamAutoOnce(
    req: SubscribeStream,
  ): Promise<SubscribeHandle<InternalDelivered>> {
    const reply = deferred<SubscribeResult<InternalDelivered>>();
    const engine = await this._engineFor(req.topic, req.partition, null);
    await engine.submit({ type: "subscribeStreamAutoAck", req, reply });
    const result = await reply.promise;
    return { engine, queue: result.queue };
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
