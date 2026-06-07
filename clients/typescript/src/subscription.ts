import {
  BrokenPipeError,
  DeserializationError,
  FibrilError,
} from "./errors.js";
import { contentTypeHeader, deserializeByContentType } from "./message.js";
import type { DeliveryTag } from "./protocol.js";
import type {
  Engine,
  InternalDelivered,
  InternalInflight,
} from "./engine.js";
import { deferred } from "./internal/deferred.js";
import type { BoundedQueue } from "./internal/bounded-queue.js";
import type { Client } from "./client.js";
import { deadlineFromDelay, type DelayInput } from "./publisher.js";

/**
 * A delivered message in auto-ack mode (no settle action required).
 *
 * `deserialize()` chooses a decoder from content type metadata; missing
 * content type defaults to msgpack.
 */
export class Message {
  readonly deliveryTag: DeliveryTag;
  readonly payload: Uint8Array;
  readonly contentTypeValue: InternalDelivered["content_type"];
  readonly headers: Record<string, string>;
  /** Unix milliseconds when the publisher claimed to have published. */
  readonly published: number;
  /** Unix milliseconds when the broker received the publish. */
  readonly publishReceived: number;
  /** Topic offset assigned by the broker. */
  readonly offset: bigint;

  constructor(d: InternalDelivered) {
    this.deliveryTag = d.delivery_tag;
    this.payload = d.payload;
    this.contentTypeValue = d.content_type;
    this.headers = d.headers;
    this.published = Number(d.published);
    this.publishReceived = Number(d.publish_received);
    this.offset = d.offset;
  }

  /**
   * Decode by content type metadata. Missing content type defaults to msgpack.
   *
   * @example
   * ```ts
   * const job = msg.deserialize<{ id: number }>();
   * ```
   */
  deserialize<T>(): T {
    try {
      return deserializeByContentType<T>(this.contentType(), this.payload);
    } catch (err) {
      if (err instanceof DeserializationError) throw err;
      throw new DeserializationError(
        `Failed to deserialize payload: ${(err as Error).message}`,
      );
    }
  }

  /**
   * Return the message content type, if present.
   */
  contentType(): string | undefined {
    return contentTypeHeader(this.contentTypeValue);
  }

  /**
   * Decode the payload as msgpack, ignoring content type metadata.
   */
  msgpack<T>(): T {
    return deserializeByContentType<T>("application/msgpack", this.payload);
  }

  /**
   * Decode the payload as JSON, ignoring content type metadata.
   */
  json<T>(): T {
    return deserializeByContentType<T>("application/json", this.payload);
  }

  /**
   * Return the raw payload bytes.
   */
  raw(): Uint8Array {
    return this.payload;
  }

  /**
   * Decode the payload as UTF-8 text.
   */
  content(): string {
    try {
      return new TextDecoder().decode(this.payload);
    } catch (err) {
      throw new DeserializationError(
        `Failed to decode text payload: ${(err as Error).message}`,
      );
    }
  }
}

type SettleState =
  | { kind: "open" }
  | { kind: "settled" };

/**
 * A delivered message in manual-ack mode. Must be settled with one of
 * `complete()`, `fail()`, `retry()`, or `retryAfter()`. Calling more than
 * once throws.
 *
 * Dropping an `InflightMessage` without settling it does not acknowledge the
 * message.
 */
export class InflightMessage {
  readonly deliveryTag: DeliveryTag;
  readonly payload: Uint8Array;
  readonly contentTypeValue: InternalInflight["content_type"];
  readonly headers: Record<string, string>;
  readonly published: number;
  readonly publishReceived: number;
  readonly offset: bigint;

  readonly #engine: Engine;
  readonly #subId: bigint;
  readonly #deliverRequestId: bigint;
  #state: SettleState = { kind: "open" };

  constructor(engine: Engine, d: InternalInflight) {
    this.deliveryTag = d.delivery_tag;
    this.payload = d.payload;
    this.contentTypeValue = d.content_type;
    this.headers = d.headers;
    this.published = Number(d.published);
    this.publishReceived = Number(d.publish_received);
    this.offset = d.offset;
    this.#engine = engine;
    this.#subId = d.sub_id;
    this.#deliverRequestId = d.deliver_request_id;
  }

  /**
   * Decode by content type metadata. Missing content type defaults to msgpack.
   */
  deserialize<T>(): T {
    try {
      return deserializeByContentType<T>(this.contentType(), this.payload);
    } catch (err) {
      if (err instanceof DeserializationError) throw err;
      throw new DeserializationError(
        `Failed to deserialize payload: ${(err as Error).message}`,
      );
    }
  }

  /**
   * Return the message content type, if present.
   */
  contentType(): string | undefined {
    return contentTypeHeader(this.contentTypeValue);
  }

  /**
   * Decode the payload as msgpack, ignoring content type metadata.
   */
  msgpack<T>(): T {
    return deserializeByContentType<T>("application/msgpack", this.payload);
  }

  /**
   * Decode the payload as JSON, ignoring content type metadata.
   */
  json<T>(): T {
    return deserializeByContentType<T>("application/json", this.payload);
  }

  /**
   * Return the raw payload bytes.
   */
  raw(): Uint8Array {
    return this.payload;
  }

  /**
   * Decode the payload as UTF-8 text.
   */
  content(): string {
    try {
      return new TextDecoder().decode(this.payload);
    } catch (err) {
      throw new DeserializationError(
        `Failed to decode text payload: ${(err as Error).message}`,
      );
    }
  }

  /**
   * Acknowledge successful processing.
   *
   * Resolves with a settled `Message` view of the same payload and metadata.
   */
  async complete(): Promise<Message> {
    this.#assertOpen();
    this.#state = { kind: "settled" };
    const reply = deferred<void>();
    await this.#engine.submit({
      type: "ack",
      sub_id: this.#subId,
      tag: this.deliveryTag,
      request_id: this.#deliverRequestId,
      reply,
    });
    await reply.promise;
    return this.#asMessage();
  }

  /**
   * Negatively acknowledge without requeue.
   *
   * Depending on queue configuration, the broker may drop or dead-letter the
   * message.
   */
  async fail(): Promise<Message> {
    this.#assertOpen();
    this.#state = { kind: "settled" };
    const reply = deferred<void>();
    await this.#engine.submit({
      type: "nack",
      sub_id: this.#subId,
      tag: this.deliveryTag,
      requeue: false,
      not_before: null,
      request_id: this.#deliverRequestId,
      reply,
    });
    await reply.promise;
    return this.#asMessage();
  }

  /**
   * Negatively acknowledge with requeue.
   *
   * The message becomes eligible for redelivery.
   */
  async retry(): Promise<Message> {
    this.#assertOpen();
    this.#state = { kind: "settled" };
    const reply = deferred<void>();
    await this.#engine.submit({
      type: "nack",
      sub_id: this.#subId,
      tag: this.deliveryTag,
      requeue: true,
      not_before: null,
      request_id: this.#deliverRequestId,
      reply,
    });
    await reply.promise;
    return this.#asMessage();
  }

  /**
   * Negatively acknowledge with delayed requeue.
   *
   * A number is a relative delay in milliseconds. A `Date` is treated as an
   * absolute Unix-millisecond retry deadline.
   */
  async retryAfter(delay: DelayInput): Promise<Message> {
    this.#assertOpen();
    this.#state = { kind: "settled" };
    const reply = deferred<void>();
    await this.#engine.submit({
      type: "nack",
      sub_id: this.#subId,
      tag: this.deliveryTag,
      requeue: true,
      not_before: deadlineFromDelay(delay),
      request_id: this.#deliverRequestId,
      reply,
    });
    await reply.promise;
    return this.#asMessage();
  }

  #assertOpen(): void {
    if (this.#state.kind === "settled") {
      throw new FibrilError("InflightMessage already settled");
    }
  }

  #asMessage(): Message {
    return new Message({
      delivery_tag: this.deliveryTag,
      payload: this.payload,
      content_type: this.contentTypeValue,
      headers: this.headers,
      published: BigInt(this.published),
      publish_received: BigInt(this.publishReceived),
      offset: this.offset,
    });
  }
}

/**
 * Subscription with manual acknowledgement. Iterate with `for await`.
 * Each delivered message must be settled by the user via
 * `complete()` / `fail()` / `retry()` / `retryAfter()` on the
 * `InflightMessage`.
 *
 * Iteration ends cleanly when the subscription is closed by the user
 * (via `close()`); it throws `DisconnectionError` (or other `FibrilError`)
 * if the engine dies.
 *
 * @example
 * ```ts
 * const sub = await client
 *   .subscribe("email.send")
 *   .group("workers")
 *   .prefetch(32)
 *   .subManualAck();
 *
 * for await (const msg of sub) {
 *   try {
 *     await process(msg.deserialize<Job>());
 *     await msg.complete();
 *   } catch {
 *     await msg.retry();
 *   }
 * }
 * ```
 */
export class Subscription implements AsyncIterable<InflightMessage> {
  readonly #engine: Engine;
  readonly #queue: BoundedQueue<InternalInflight>;
  #closed = false;

  /** @internal */
  constructor(engine: Engine, queue: BoundedQueue<InternalInflight>) {
    this.#engine = engine;
    this.#queue = queue;
  }

  /** Receive the next message, or `null` if the subscription is closed cleanly. */
  async recv(): Promise<InflightMessage | null> {
    const v = await this.#queue.recv();
    if (v === null) return null;
    return new InflightMessage(this.#engine, v);
  }

  /** Close this subscription. The client engine continues running. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#queue.close();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<InflightMessage> {
    while (true) {
      const msg = await this.recv();
      if (msg === null) return;
      yield msg;
    }
  }
}

/**
 * Subscription with automatic (client-side) acknowledgement. The user
 * receives `Message` directly with no settle action required.
 *
 * Prefer manual acknowledgements when processing correctness matters.
 */
export class AutoAckedSubscription implements AsyncIterable<Message> {
  readonly #queue: BoundedQueue<InternalDelivered>;
  #closed = false;

  /** @internal */
  constructor(queue: BoundedQueue<InternalDelivered>) {
    this.#queue = queue;
  }

  /** Receive the next message, or `null` if the subscription is closed cleanly. */
  async recv(): Promise<Message | null> {
    const v = await this.#queue.recv();
    if (v === null) return null;
    return new Message(v);
  }

  /** Close this subscription. The client engine continues running. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#queue.close();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<Message> {
    while (true) {
      const msg = await this.recv();
      if (msg === null) return;
      yield msg;
    }
  }
}

/**
 * Builder for subscription requests.
 *
 * Construct with `client.subscribe(topic)`, then call `subManualAck()` or
 * `subAutoAck()`.
 */
export class SubscriptionBuilder {
  readonly #client: Client;
  #topic: string;
  #group: string | null = null;
  #prefetch = 1;

  /** @internal */
  constructor(client: Client, topic: string) {
    this.#client = client;
    this.#topic = topic;
  }

  /**
   * Set the queue group.
   *
   * A group is an optional queue namespace under the topic.
   */
  group(group: string): this {
    this.#group = group;
    return this;
  }

  /**
   * Set the maximum number of messages the broker may lease ahead.
   *
   * Higher values improve throughput but increase the number of messages that
   * may need redelivery if the client disconnects before settling them.
   */
  prefetch(prefetch: number): this {
    if (prefetch < 1 || !Number.isInteger(prefetch)) {
      throw new Error("prefetch must be a positive integer");
    }
    this.#prefetch = prefetch;
    return this;
  }

  /**
   * Subscribe with manual acknowledgement.
   *
   * Each received `InflightMessage` must be settled by the user.
   */
  async subManualAck(): Promise<Subscription> {
    const reply = deferred<BoundedQueue<InternalInflight>>();
    const engine = this.#client._engine();
    await engine.submit({
      type: "subscribe",
      req: {
        topic: this.#topic,
        group: this.#group,
        prefetch: this.#prefetch,
        auto_ack: false,
      },
      reply,
    });
    let queue: BoundedQueue<InternalInflight>;
    try {
      queue = await reply.promise;
    } catch (err) {
      if (err instanceof FibrilError) throw err;
      throw new BrokenPipeError();
    }
    return new Subscription(engine, queue);
  }

  /**
   * Subscribe with client-side automatic acknowledgement.
   */
  async subAutoAck(): Promise<AutoAckedSubscription> {
    const reply = deferred<BoundedQueue<InternalDelivered>>();
    const engine = this.#client._engine();
    await engine.submit({
      type: "subscribeAutoAck",
      req: {
        topic: this.#topic,
        group: this.#group,
        prefetch: this.#prefetch,
        auto_ack: false, // matches Rust client: auto-ack is client-side
      },
      reply,
    });
    let queue: BoundedQueue<InternalDelivered>;
    try {
      queue = await reply.promise;
    } catch (err) {
      if (err instanceof FibrilError) throw err;
      throw new BrokenPipeError();
    }
    return new AutoAckedSubscription(queue);
  }
}
