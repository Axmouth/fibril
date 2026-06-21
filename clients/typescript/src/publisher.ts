import { BrokenPipeError, RedirectError } from "./errors.js";
import type { Command, Engine } from "./engine.js";
import { deferred, type Deferred } from "./internal/deferred.js";
import type { Route } from "./internal/topology.js";
import { intoMessage, NewMessage, type Publishable } from "./message.js";
import type { ContentType, RedirectMsg } from "./protocol.js";

/**
 * Routing surface the Publisher needs from the Client. The Client owns the
 * topology cache and connection pool; the Publisher only asks it where each
 * publish should go and how to react to a redirect.
 */
interface RoutingClient {
  _route(topic: string, group: string | null, key: Uint8Array | null): Route;
  _engineFor(topic: string, partition: number, group: string | null): Promise<Engine>;
  _applyRedirect(redirect: RedirectMsg): void;
  _maxRedirects(): number;
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

  /** @internal */
  constructor(client: RoutingClient, topic: string, group: string | null) {
    this.#client = client;
    this.#topic = topic;
    this.#group = group;
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
   * Numeric delays are milliseconds; `Date` is treated as an absolute deadline.
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
   * Numeric delays are milliseconds; `Date` is treated as an absolute deadline.
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
   * Numeric delays are milliseconds; `Date` is treated as an absolute deadline.
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

  // Confirmed publish that awaits the offset. Follows owner redirects up to the
  // configured limit: a redirect is not a failure, it names the new owner, so
  // we point-update routing and retry the whole attempt against it.
  async #sendConfirmed(spec: SendSpec): Promise<bigint> {
    let redirects = 0;
    for (;;) {
      const route = this.#client._route(this.#topic, this.#group, spec.key);
      const engine = await this.#client._engineFor(this.#topic, route.partition, this.#group);
      const reply = deferred<bigint>();
      await engine.submit(this.#command(spec, route, reply));
      try {
        return await reply.promise;
      } catch (err) {
        if (err instanceof RedirectError && redirects < this.#client._maxRedirects()) {
          this.#client._applyRedirect(err.redirect);
          redirects += 1;
          continue;
        }
        throw err;
      }
    }
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
