import { BrokenPipeError } from "./errors.js";
import type { Engine } from "./engine.js";
import { deferred } from "./internal/deferred.js";
import { intoMessage, type Publishable } from "./message.js";

/**
 * Delay accepted by delayed publish methods.
 *
 * A number is a relative delay in milliseconds. A `Date` is treated as an
 * absolute Unix-millisecond deadline.
 */
export type DelayInput = number | Date;

function deadlineFromDelay(delay: DelayInput): bigint {
  if (delay instanceof Date) return BigInt(delay.getTime());
  if (!Number.isFinite(delay) || delay < 0) {
    throw new Error("delay must be a non-negative millisecond value");
  }
  return BigInt(Date.now() + Math.trunc(delay));
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
 * const offset = await publisher.publish({ to: "user@example.com" });
 * await publisher.publishUnconfirmed("fire and forget");
 * ```
 */
export class Publisher {
  readonly #engine: Engine;
  readonly #topic: string;
  readonly #group: string | null;

  /** @internal */
  constructor(engine: Engine, topic: string, group: string | null) {
    this.#engine = engine;
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
   * Use `publish` when you need the broker-assigned offset.
   */
  async publishUnconfirmed<T>(payload: Publishable<T>): Promise<void> {
    const message = intoMessage(payload);
    await this.#engine.submit({
      type: "publishUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      headers: message.headers,
      payload: message.payload,
      published: BigInt(Date.now()),
    });
  }

  /**
   * Publish and wait for the broker's confirmation. Resolves with the
   * topic offset assigned to the message.
   */
  async publish<T>(payload: Publishable<T>): Promise<bigint> {
    const message = intoMessage(payload);
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishConfirmed",
      topic: this.#topic,
      group: this.#group,
      headers: message.headers,
      payload: message.payload,
      published: BigInt(Date.now()),
      reply,
    });
    try {
      return await reply.promise;
    } catch (err) {
      if (err instanceof Error) throw err;
      throw new BrokenPipeError();
    }
  }

  /**
   * Publish a raw byte payload (no msgpack wrapping and no content-type).
   */
  async publishBytes(payload: Uint8Array): Promise<bigint> {
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishConfirmed",
      topic: this.#topic,
      group: this.#group,
      headers: {},
      payload,
      published: BigInt(Date.now()),
      reply,
    });
    return reply.promise;
  }

  /**
   * Publish a raw byte payload without waiting for broker confirmation.
   */
  async publishBytesUnconfirmed(payload: Uint8Array): Promise<void> {
    await this.#engine.submit({
      type: "publishUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      headers: {},
      payload,
      published: BigInt(Date.now()),
    });
  }

  /**
   * Publish after a delay without waiting for server confirmation.
   * Numeric delays are milliseconds; `Date` is treated as an absolute deadline.
   *
   * @example
   * ```ts
   * await publisher.publishUnconfirmedDelayed("retry later", 30_000);
   * ```
   */
  async publishUnconfirmedDelayed<T>(
    payload: Publishable<T>,
    delay: DelayInput,
  ): Promise<void> {
    const message = intoMessage(payload);
    await this.#engine.submit({
      type: "publishDelayedUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      headers: message.headers,
      payload: message.payload,
      published: BigInt(Date.now()),
      not_before: deadlineFromDelay(delay),
    });
  }

  /**
   * Publish after a delay and wait for broker confirmation.
   * Numeric delays are milliseconds; `Date` is treated as an absolute deadline.
   *
   * @example
   * ```ts
   * const offset = await publisher.publishDelayed(
   *   { id: 42 },
   *   new Date(Date.now() + 30_000),
   * );
   * ```
   */
  async publishDelayed<T>(
    payload: Publishable<T>,
    delay: DelayInput,
  ): Promise<bigint> {
    const message = intoMessage(payload);
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishDelayedConfirmed",
      topic: this.#topic,
      group: this.#group,
      headers: message.headers,
      payload: message.payload,
      published: BigInt(Date.now()),
      not_before: deadlineFromDelay(delay),
      reply,
    });
    try {
      return await reply.promise;
    } catch (err) {
      if (err instanceof Error) throw err;
      throw new BrokenPipeError();
    }
  }

  /**
   * Alias for `publishDelayed`.
   */
  async publishWithDelayed<T>(
    payload: Publishable<T>,
    delay: DelayInput,
  ): Promise<bigint> {
    return this.publishDelayed(payload, delay);
  }
}
