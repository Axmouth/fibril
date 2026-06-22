import {
  BrokenPipeError,
  RedirectError,
  ServerError,
  isRetryable,
  isTransientError,
} from "./errors.js";
import type { Command, Engine } from "./engine.js";
import { deferred, type Deferred } from "./internal/deferred.js";
import type { Route } from "./internal/topology.js";
import {
  PUBLISH_RETRY_INITIAL_BACKOFF_MS,
  bumpBackoff,
  newPublishRetryState,
  publishRetryNap,
  sleep,
  type PublishRetryState,
} from "./internal/retry.js";
import {
  HEADER_PRODUCER_ID,
  HEADER_PRODUCER_SEQ,
  intoMessage,
  NewMessage,
  type Publishable,
} from "./message.js";
import type { ContentType, RedirectMsg } from "./protocol.js";

// Broker error code for a queue that is not declared in the cluster. Matches the
// server ERR_NOT_FOUND used to fail a publish to an unknown topic fast.
const ERR_NOT_FOUND = 404;

/**
 * Routing surface the Publisher needs from the Client. The Client owns the
 * topology cache and connection pool. The Publisher only asks it where each
 * publish should go and how to react to a redirect or transient failover.
 */
interface RoutingClient {
  _route(topic: string, group: string | null, key: Uint8Array | null): Route;
  _engineFor(topic: string, partition: number, group: string | null): Promise<Engine>;
  _applyRedirect(redirect: RedirectMsg): void;
  _maxRedirects(): number;
  _publishTimeoutMs(): number;
  _refreshTopologyThrottled(): Promise<boolean>;
  _isTopicMissing(topic: string, group: string | null): boolean;
}

/**
 * Delay accepted by delayed publish methods.
 *
 * A number is a relative delay in milliseconds. A `Date` is treated as an
 * absolute Unix-millisecond deadline.
 */
export type DelayInput = number | Date;

export function deadlineFromDelay(delay: DelayInput): bigint {
  if (delay instanceof Date) return BigInt(delay.getTime());
  if (!Number.isFinite(delay) || delay < 0) {
    throw new Error("delay must be a non-negative millisecond value");
  }
  return BigInt(Date.now() + Math.trunc(delay));
}

// The content and routing inputs shared by every publish variant.
interface SendSpec {
  contentType: ContentType | null;
  headers: Record<string, string>;
  payload: Uint8Array;
  key: Uint8Array | null;
  notBefore: bigint | null;
}

/**
 * Broker confirmation for a publish request.
 *
 * Await `confirmed()` when you need the broker-assigned offset without
 * serializing every publish on the confirmation round trip.
 */
export class PublishConfirmation {
  readonly #promise: Promise<bigint>;

  /** @internal */
  constructor(promise: Promise<bigint>) {
    this.#promise = promise;
  }

  /**
   * Wait for the broker-assigned topic offset.
   */
  async confirmed(): Promise<bigint> {
    try {
      return await this.#promise;
    } catch (err) {
      if (err instanceof Error) throw err;
      throw new BrokenPipeError();
    }
  }
}

/**
 * Publish messages to a specific topic (and optional group).
 *
 * Construct via `client.publisher(topic)` or `client.publisherGrouped(topic, group)`.
 *
 * @example
 * ```ts
 * const publisher = client.publisher("email.send");
 *
 * await publisher.publish({ to: "user@example.com" });
 * const offset = await publisher.publishConfirmed("needs an offset");
 * ```
 */
export class Publisher {
  readonly #client: RoutingClient;
  readonly #topic: string;
  readonly #group: string | null;
  readonly #defaultTtlMs: bigint | null;

  /** @internal */
  constructor(
    client: RoutingClient,
    topic: string,
    group: string | null,
    defaultTtlMs: bigint | null = null,
  ) {
    this.#client = client;
    this.#topic = topic;
    this.#group = group;
    this.#defaultTtlMs = defaultTtlMs;
  }

  /**
   * Topic this publisher writes to.
   */
  get topic(): string {
    return this.#topic;
  }

  /**
   * Optional queue group this publisher writes to.
   */
  get group(): string | null {
    return this.#group;
  }

  /**
   * Publish without waiting for server confirmation. Plain values are
   * msgpack-encoded and tagged as `application/msgpack`.
   *
   * This only waits for the command to be accepted by the local client engine.
   * Use `publishConfirmed` when you need the broker-assigned offset.
   */
  async publish<T>(payload: Publishable<T>): Promise<void> {
    await this.#sendUnconfirmed(specFromMessage(intoMessage(payload), null));
  }

  /**
   * Publish and wait for the broker's confirmation. Resolves with the
   * topic offset assigned to the message.
   */
  async publishConfirmed<T>(payload: Publishable<T>): Promise<bigint> {
    return this.#sendConfirmed(specFromMessage(intoMessage(payload), null));
  }

  /**
   * Publish and return a handle that can be awaited for broker confirmation.
   *
   * Use this to pipeline multiple confirmed publishes while still retaining
   * each broker-assigned offset.
   */
  async publishWithConfirmation<T>(
    payload: Publishable<T>,
  ): Promise<PublishConfirmation> {
    return this.#sendWithConfirmation(specFromMessage(intoMessage(payload), null));
  }

  /**
   * Publish a raw byte payload without msgpack wrapping or content-type.
   */
  async publishBytes(payload: Uint8Array): Promise<void> {
    await this.#sendUnconfirmed(rawSpec(payload, null));
  }

  /**
   * Publish a raw byte payload and wait for broker confirmation.
   */
  async publishBytesConfirmed(payload: Uint8Array): Promise<bigint> {
    return this.#sendConfirmed(rawSpec(payload, null));
  }

  /**
   * Publish a raw byte payload and return a broker-confirmation handle.
   */
  async publishBytesWithConfirmation(
    payload: Uint8Array,
  ): Promise<PublishConfirmation> {
    return this.#sendWithConfirmation(rawSpec(payload, null));
  }

  /**
   * Publish after a delay without waiting for server confirmation.
   * Numeric delays are milliseconds. A `Date` is treated as an absolute deadline.
   *
   * @example
   * ```ts
   * await publisher.publishDelayed("retry later", 30_000);
   * ```
   */
  async publishDelayed<T>(
    payload: Publishable<T>,
    delay: DelayInput,
  ): Promise<void> {
    await this.#sendUnconfirmed(specFromMessage(intoMessage(payload), deadlineFromDelay(delay)));
  }

  /**
   * Publish after a delay and wait for broker confirmation.
   * Numeric delays are milliseconds. A `Date` is treated as an absolute deadline.
   *
   * @example
   * ```ts
   * const offset = await publisher.publishDelayedConfirmed(
   *   { id: 42 },
   *   new Date(Date.now() + 30_000),
   * );
   * ```
   */
  async publishDelayedConfirmed<T>(
    payload: Publishable<T>,
    delay: DelayInput,
  ): Promise<bigint> {
    return this.#sendConfirmed(specFromMessage(intoMessage(payload), deadlineFromDelay(delay)));
  }

  /**
   * Publish after a delay and return a broker-confirmation handle.
   * Numeric delays are milliseconds. A `Date` is treated as an absolute deadline.
   */
  async publishDelayedWithConfirmation<T>(
    payload: Publishable<T>,
    delay: DelayInput,
  ): Promise<PublishConfirmation> {
    return this.#sendWithConfirmation(specFromMessage(intoMessage(payload), deadlineFromDelay(delay)));
  }

  // ---- routed send paths ----

  // Fire-and-forget: route by the cache and send. Without a confirmation there
  // is no reply to carry a redirect, so a misroute is corrected on the next
  // confirmed publish rather than here.
  async #sendUnconfirmed(spec: SendSpec): Promise<void> {
    const route = this.#client._route(this.#topic, this.#group, spec.key);
    const engine = await this.#client._engineFor(this.#topic, route.partition, this.#group);
    await engine.submit(this.#command(spec, route, null));
  }

  // Confirmed publish that awaits the offset. One attempt resolves the owner,
  // sends, and awaits the confirm, so a transport failure at any step is caught
  // and retried together. Two retry kinds, both bounded:
  //   - redirect: not a failure, names the new owner -> point-update and retry.
  //   - transient (owner unreachable mid-failover): refresh topology so the next
  //     attempt re-resolves the new owner, back off, retry until the deadline.
  async #sendConfirmed(spec: SendSpec): Promise<bigint> {
    const state = newPublishRetryState(this.#client._publishTimeoutMs());
    for (;;) {
      try {
        const route = this.#client._route(this.#topic, this.#group, spec.key);
        const engine = await this.#client._engineFor(this.#topic, route.partition, this.#group);
        const reply = deferred<bigint>();
        await engine.submit(this.#command(spec, route, reply));
        return await reply.promise;
      } catch (err) {
        await this.#afterPublishFailure(err, state);
      }
    }
  }

  // Decide whether to retry after a failed confirmed-publish attempt. Returns to
  // retry the loop, throws to give up with the surfaced error.
  async #afterPublishFailure(err: unknown, state: PublishRetryState): Promise<void> {
    if (err instanceof RedirectError) {
      if (state.redirects >= this.#client._maxRedirects()) {
        throw err;
      }
      this.#client._applyRedirect(err.redirect);
      state.redirects += 1;
      return;
    }
    if (isTransientError(err)) {
      if (state.deadline === null || Date.now() >= state.deadline) {
        throw err;
      }
      // Re-resolve the new owner before the next attempt. If a refresh lands a
      // populated cluster view that does not know this queue, fail fast rather
      // than burning the whole budget on a topic that is not declared.
      if (await this.#client._refreshTopologyThrottled()) {
        if (this.#client._isTopicMissing(this.#topic, this.#group)) {
          throw new ServerError(ERR_NOT_FOUND, `${this.#topic} is not declared in the cluster`);
        }
      }
      const remaining = state.deadline - Date.now();
      await sleep(Math.min(publishRetryNap(state.backoffMs), Math.max(remaining, 0)));
      bumpBackoff(state);
      return;
    }
    throw err;
  }

  // Pipelined confirmed publish: route and send once, hand back the handle
  // before the confirm lands. A redirect on the reply rejects the handle (no
  // auto-retry) to preserve send ordering across pipelined calls.
  async #sendWithConfirmation(spec: SendSpec): Promise<PublishConfirmation> {
    const route = this.#client._route(this.#topic, this.#group, spec.key);
    const engine = await this.#client._engineFor(this.#topic, route.partition, this.#group);
    const reply = deferred<bigint>();
    await engine.submit(this.#command(spec, route, reply));
    return new PublishConfirmation(reply.promise);
  }

  #command(spec: SendSpec, route: Route, reply: Deferred<bigint> | null): Command {
    const common = {
      topic: this.#topic,
      group: this.#group,
      partition: route.partition,
      partition_key: spec.key,
      partitioning_version: route.partitioningVersion,
      content_type: spec.contentType,
      headers: spec.headers,
      payload: spec.payload,
      published: BigInt(Date.now()),
      ttl_ms: this.#defaultTtlMs,
    };
    if (spec.notBefore === null) {
      return reply
        ? { type: "publishConfirmed", ...common, reply }
        : { type: "publishUnconfirmed", ...common };
    }
    return reply
      ? { type: "publishDelayedConfirmed", ...common, not_before: spec.notBefore, reply }
      : { type: "publishDelayedUnconfirmed", ...common, not_before: spec.notBefore };
  }

  /**
   * Return a publisher that stamps a default message TTL (in milliseconds) on
   * every immediate publish: the broker drops the message if it is not consumed
   * within the interval. Milliseconds, matching this client's delayed-publish
   * convention. Applies to the immediate publish paths (delayed publishes do not
   * carry a TTL yet).
   *
   * @example
   * ```ts
   * const ephemeral = client.publisher("rpc.reply").expiring(30_000);
   * await ephemeral.publish(result);
   * ```
   */
  expiring(ttlMs: number): Publisher {
    if (!Number.isFinite(ttlMs) || ttlMs < 0) {
      throw new Error("ttl must be a non-negative millisecond value");
    }
    return new Publisher(this.#client, this.#topic, this.#group, BigInt(Math.trunc(ttlMs)));
  }

  /**
   * Wrap this publisher in a {@link ReliablePublisher} that stamps producer
   * dedup ids and retries until the message is durably confirmed.
   */
  reliable(): ReliablePublisher {
    return new ReliablePublisher(this);
  }
}

/**
 * A publisher that keeps retrying a confirmed publish until it is durably
 * confirmed (or hits a permanent error or the attempt cap), stamping each
 * message with a stable producer id and a monotonic per-producer sequence under
 * the library-owned `fibril.client.*` header carve-out.
 *
 * The broker ignores those headers today, so this stays at-least-once: a retry
 * after a lost confirmation may duplicate. Once the broker dedups on these keys
 * the same helper becomes effectively-once with no API change.
 */
export class ReliablePublisher {
  readonly #publisher: Publisher;
  readonly #producerId: string;
  #seq = 0n;
  #maxAttempts = 0;

  /** @internal Use {@link Publisher.reliable}. */
  constructor(publisher: Publisher, producerId: string = crypto.randomUUID()) {
    this.#publisher = publisher;
    this.#producerId = producerId;
  }

  /** The stable producer id stamped on every message from this publisher. */
  get producerId(): string {
    return this.#producerId;
  }

  /**
   * Cap the number of publish attempts. 0 (the default) retries until the
   * message is durably confirmed or a permanent error.
   */
  maxAttempts(maxAttempts: number): this {
    if (maxAttempts < 0 || !Number.isInteger(maxAttempts)) {
      throw new Error("maxAttempts must be a non-negative integer");
    }
    this.#maxAttempts = maxAttempts;
    return this;
  }

  /**
   * Publish and keep retrying until durably confirmed. Resolves with the
   * broker-assigned offset. A retry re-sends the SAME sequence number.
   */
  async publish<T>(payload: Publishable<T>): Promise<bigint> {
    const seq = this.#seq;
    this.#seq += 1n;
    const message = intoMessage(payload)
      .systemHeader(HEADER_PRODUCER_ID, this.#producerId)
      .systemHeader(HEADER_PRODUCER_SEQ, seq.toString());

    let attempts = 0;
    for (;;) {
      try {
        return await this.#publisher.publishConfirmed(message);
      } catch (err) {
        // Retryable means the outcome is unknown (the inner publish already
        // spent its transport retry budget). Keep going unless the cap is hit.
        if (isRetryable(err)) {
          attempts += 1;
          if (this.#maxAttempts !== 0 && attempts >= this.#maxAttempts) throw err;
          await sleep(publishRetryNap(PUBLISH_RETRY_INITIAL_BACKOFF_MS));
          continue;
        }
        throw err; // permanent
      }
    }
  }
}

function specFromMessage(message: NewMessage, notBefore: bigint | null): SendSpec {
  return {
    contentType: message.contentTypeValue,
    headers: message.headers,
    payload: message.payload,
    key: message.partitionKeyValue,
    notBefore,
  };
}

function rawSpec(payload: Uint8Array, notBefore: bigint | null): SendSpec {
  return { contentType: null, headers: {}, payload, key: null, notBefore };
}
