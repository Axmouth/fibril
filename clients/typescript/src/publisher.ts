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

export function deadlineFromDelay(delay: DelayInput): bigint {
  if (delay instanceof Date) return BigInt(delay.getTime());
  if (!Number.isFinite(delay) || delay < 0) {
    throw new Error("delay must be a non-negative millisecond value");
  }
  return BigInt(Date.now() + Math.trunc(delay));
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
   * Use `publishConfirmed` when you need the broker-assigned offset.
   */
  async publish<T>(payload: Publishable<T>): Promise<void> {
    const message = intoMessage(payload);
    await this.#engine.submit({
      type: "publishUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      content_type: message.contentTypeValue,
      headers: message.headers,
      payload: message.payload,
      published: BigInt(Date.now()),
    });
  }

  /**
   * Publish and wait for the broker's confirmation. Resolves with the
   * topic offset assigned to the message.
   */
  async publishConfirmed<T>(payload: Publishable<T>): Promise<bigint> {
    return (await this.publishWithConfirmation(payload)).confirmed();
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
    const message = intoMessage(payload);
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishConfirmed",
      topic: this.#topic,
      group: this.#group,
      content_type: message.contentTypeValue,
      headers: message.headers,
      payload: message.payload,
      published: BigInt(Date.now()),
      reply,
    });
    return new PublishConfirmation(reply.promise);
  }

  /**
   * Publish a raw byte payload without msgpack wrapping or content-type.
   */
  async publishBytes(payload: Uint8Array): Promise<void> {
    await this.#engine.submit({
      type: "publishUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      content_type: null,
      headers: {},
      payload,
      published: BigInt(Date.now()),
    });
  }

  /**
   * Publish a raw byte payload and wait for broker confirmation.
   */
  async publishBytesConfirmed(payload: Uint8Array): Promise<bigint> {
    return (await this.publishBytesWithConfirmation(payload)).confirmed();
  }

  /**
   * Publish a raw byte payload and return a broker-confirmation handle.
   */
  async publishBytesWithConfirmation(
    payload: Uint8Array,
  ): Promise<PublishConfirmation> {
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishConfirmed",
      topic: this.#topic,
      group: this.#group,
      content_type: null,
      headers: {},
      payload,
      published: BigInt(Date.now()),
      reply,
    });
    return new PublishConfirmation(reply.promise);
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
    const message = intoMessage(payload);
    await this.#engine.submit({
      type: "publishDelayedUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      content_type: message.contentTypeValue,
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
    return (await this.publishDelayedWithConfirmation(payload, delay)).confirmed();
  }

  /**
   * Publish after a delay and return a broker-confirmation handle.
   * Numeric delays are milliseconds; `Date` is treated as an absolute deadline.
   */
  async publishDelayedWithConfirmation<T>(
    payload: Publishable<T>,
    delay: DelayInput,
  ): Promise<PublishConfirmation> {
    const message = intoMessage(payload);
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishDelayedConfirmed",
      topic: this.#topic,
      group: this.#group,
      content_type: message.contentTypeValue,
      headers: message.headers,
      payload: message.payload,
      published: BigInt(Date.now()),
      not_before: deadlineFromDelay(delay),
      reply,
    });
    return new PublishConfirmation(reply.promise);
  }
}
