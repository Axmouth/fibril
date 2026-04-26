import { decodeMsgpack } from "./codec.js";
import {
  BrokenPipeError,
  DeserializationError,
  FibrilError,
} from "./errors.js";
import type { DeliveryTag } from "./protocol.js";
import type {
  Engine,
  InternalDelivered,
  InternalInflight,
} from "./engine.js";
import { deferred } from "./internal/deferred.js";
import type { BoundedQueue } from "./internal/bounded-queue.js";
import type { Client } from "./client.js";

/**
 * A delivered message in auto-ack mode (no settle action required).
 */
export class Message {
  readonly deliveryTag: DeliveryTag;
  readonly payload: Uint8Array;
  /** Unix milliseconds when the publisher claimed to have published. */
  readonly published: number;
  /** Unix milliseconds when the broker received the publish. */
  readonly publishReceived: number;
  /** Topic offset assigned by the broker. */
  readonly offset: bigint;

  constructor(d: InternalDelivered) {
    this.deliveryTag = d.delivery_tag;
    this.payload = d.payload;
    this.published = Number(d.published);
    this.publishReceived = Number(d.publish_received);
    this.offset = d.offset;
  }

  /**
   * Decode the payload as msgpack.
   * Throws `DeserializationError` if the payload is not valid msgpack.
   */
  deserialize<T>(): T {
    try {
      return decodeMsgpack<T>(this.payload);
    } catch (err) {
      throw new DeserializationError(
        `Failed to deserialize payload: ${(err as Error).message}`,
      );
    }
  }
}

type SettleState =
  | { kind: "open" }
  | { kind: "settled" };

/**
 * A delivered message in manual-ack mode. Must be settled with one of
 * `complete()`, `fail()`, or `retry()`. Calling more than once throws.
 */
export class InflightMessage {
  readonly deliveryTag: DeliveryTag;
  readonly payload: Uint8Array;
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
    this.published = Number(d.published);
    this.publishReceived = Number(d.publish_received);
    this.offset = d.offset;
    this.#engine = engine;
    this.#subId = d.sub_id;
    this.#deliverRequestId = d.deliver_request_id;
  }

  deserialize<T>(): T {
    try {
      return decodeMsgpack<T>(this.payload);
    } catch (err) {
      throw new DeserializationError(
        `Failed to deserialize payload: ${(err as Error).message}`,
      );
    }
  }

  /** Acknowledge successful processing. */
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

  /** Negatively acknowledge without requeue (message is dropped/dead-lettered server-side). */
  async fail(): Promise<Message> {
    this.#assertOpen();
    this.#state = { kind: "settled" };
    const reply = deferred<void>();
    await this.#engine.submit({
      type: "nack",
      sub_id: this.#subId,
      tag: this.deliveryTag,
      requeue: false,
      request_id: this.#deliverRequestId,
      reply,
    });
    await reply.promise;
    return this.#asMessage();
  }

  /** Negatively acknowledge with requeue (message will be redelivered). */
  async retry(): Promise<Message> {
    this.#assertOpen();
    this.#state = { kind: "settled" };
    const reply = deferred<void>();
    await this.#engine.submit({
      type: "nack",
      sub_id: this.#subId,
      tag: this.deliveryTag,
      requeue: true,
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
      published: BigInt(this.published),
      publish_received: BigInt(this.publishReceived),
      offset: this.offset,
    });
  }
}

/**
 * Subscription with manual acknowledgement. Iterate with `for await`.
 * Each delivered message must be settled by the user via
 * `complete()` / `fail()` / `retry()` on the `InflightMessage`.
 *
 * Iteration ends cleanly when the subscription is closed by the user
 * (via `close()`); it throws `DisconnectionError` (or other `FibrilError`)
 * if the engine dies.
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

  /** Close this subscription. The engine continues running. */
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
 */
export class AutoAckedSubscription implements AsyncIterable<Message> {
  readonly #queue: BoundedQueue<InternalDelivered>;
  #closed = false;

  /** @internal */
  constructor(queue: BoundedQueue<InternalDelivered>) {
    this.#queue = queue;
  }

  async recv(): Promise<Message | null> {
    const v = await this.#queue.recv();
    if (v === null) return null;
    return new Message(v);
  }

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

  group(group: string): this {
    this.#group = group;
    return this;
  }

  prefetch(prefetch: number): this {
    if (prefetch < 1 || !Number.isInteger(prefetch)) {
      throw new Error("prefetch must be a positive integer");
    }
    this.#prefetch = prefetch;
    return this;
  }

  /** Subscribe with manual acknowledgement (must call complete/fail/retry). */
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

  /** Subscribe with client-side automatic acknowledgement. */
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
