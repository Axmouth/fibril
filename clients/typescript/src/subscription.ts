import {
  BrokenPipeError,
  DeserializationError,
  FibrilError,
  isTransientError,
} from "./errors.js";
import { contentTypeHeader, deserializeByContentType } from "./message.js";
import type { DeliveryTag, SubscribeMsg } from "./protocol.js";
import type {
  Engine,
  InternalDelivered,
  InternalInflight,
} from "./engine.js";
import { deferred } from "./internal/deferred.js";
import { BoundedQueue } from "./internal/bounded-queue.js";
import {
  PUBLISH_RETRY_INITIAL_BACKOFF_MS,
  PUBLISH_RETRY_MAX_BACKOFF_MS,
  publishRetryNap,
  sleep,
} from "./internal/retry.js";
import type { Client, SubscribeHandle } from "./client.js";
import { deadlineFromDelay, type DelayInput } from "./publisher.js";

function normalizeGroup(group: string | null): string | null {
  const trimmed = group?.trim();
  if (!trimmed || trimmed === "default") return null;
  return trimmed;
}

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

/** A delivery tagged with the engine it arrived on, so settle routes correctly. */
type Tagged<R> = { engine: Engine; raw: R };

/** The routing surface a supervised subscription needs from the Client. */
interface SupervisorClient {
  _isShuttingDown(): boolean;
  _superviseSubscriptions(): boolean;
  _refreshTopologyThrottled(): Promise<boolean>;
  _isTopicMissing(topic: string, group: string | null): boolean;
}

/**
 * Keeps a subscription's stream alive across an owner failover or restart. The
 * public subscription reads from a merged queue that this fills from the current
 * owner's per-connection delivery queue. When that stream ends and the consumer
 * has not closed, it re-subscribes to the current owner (re-resolved from a
 * refreshed topology) with backoff, then resumes - the read-side mirror of the
 * publish failover retry. Each delivery carries the engine it arrived on so a
 * manual ack settles against the right connection.
 *
 * Scope: single-partition (partition 0). Owner moves are detected when the
 * stream closes (owner death / restart). A graceful owner reassignment that
 * leaves the old connection up, and multi-partition fan-in, are follow-ups.
 */
class SubscriptionSupervisor<R> {
  readonly merged: BoundedQueue<Tagged<R>>;
  #engine: Engine;
  #partQueue: BoundedQueue<R>;
  #userClosed = false;

  constructor(
    private readonly client: SupervisorClient,
    private readonly req: SubscribeMsg,
    private readonly resubscribe: (req: SubscribeMsg) => Promise<SubscribeHandle<R>>,
    first: SubscribeHandle<R>,
    prefetch: number,
  ) {
    this.#engine = first.engine;
    this.#partQueue = first.queue;
    this.merged = new BoundedQueue<Tagged<R>>(Math.max(prefetch, 1));
    void this.#run();
  }

  async recv(): Promise<Tagged<R> | null> {
    return this.merged.recv();
  }

  close(): void {
    if (this.#userClosed) return;
    this.#userClosed = true;
    // Close the current owner stream to unblock the forwarder, and the merged
    // queue to end the consumer.
    this.#partQueue.close();
    this.merged.close();
  }

  async #run(): Promise<void> {
    for (;;) {
      // Forward the current owner's stream into the merged queue.
      for (;;) {
        const raw = await this.#partQueue.recv();
        if (raw === null) break;
        try {
          await this.merged.send({ engine: this.#engine, raw });
        } catch {
          // merged closed by close(): the consumer is gone.
          this.#userClosed = true;
          break;
        }
      }
      // The owner stream ended. Stop on user close, client shutdown, or when
      // supervision is disabled; otherwise re-subscribe to the current owner.
      if (
        this.#userClosed ||
        this.client._isShuttingDown() ||
        !this.client._superviseSubscriptions()
      ) {
        this.merged.close();
        return;
      }
      if (!(await this.#resubscribeWithBackoff())) {
        this.merged.close();
        return;
      }
    }
  }

  async #resubscribeWithBackoff(): Promise<boolean> {
    let backoffMs = PUBLISH_RETRY_INITIAL_BACKOFF_MS;
    for (;;) {
      if (this.#userClosed || this.client._isShuttingDown()) return false;
      // Re-resolve the owner from a refreshed topology. If a populated view no
      // longer knows the topic, stop re-subscribing (it was deleted).
      if (await this.client._refreshTopologyThrottled()) {
        if (this.client._isTopicMissing(this.req.topic, this.req.group)) return false;
      }
      try {
        const next = await this.resubscribe(this.req);
        if (this.#userClosed || this.client._isShuttingDown()) {
          next.queue.close();
          return false;
        }
        this.#engine = next.engine;
        this.#partQueue = next.queue;
        return true;
      } catch (err) {
        if (isTransientError(err)) {
          await sleep(publishRetryNap(backoffMs));
          backoffMs = Math.min(backoffMs * 2, PUBLISH_RETRY_MAX_BACKOFF_MS);
          continue;
        }
        return false; // permanent: subscribe rejected, max redirects, etc.
      }
    }
  }
}

/**
 * Subscription with manual acknowledgement. Iterate with `for await`.
 * Each delivered message must be settled by the user via
 * `complete()` / `fail()` / `retry()` / `retryAfter()` on the
 * `InflightMessage`.
 *
 * Iteration ends cleanly when the subscription is closed by the user (via
 * `close()`) or the client shuts down. With supervision enabled (the default)
 * it rides through an owner failover by re-subscribing rather than ending.
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
  readonly #sup: SubscriptionSupervisor<InternalInflight>;

  /** @internal */
  constructor(sup: SubscriptionSupervisor<InternalInflight>) {
    this.#sup = sup;
  }

  /** Receive the next message, or `null` if the subscription is closed cleanly. */
  async recv(): Promise<InflightMessage | null> {
    const item = await this.#sup.recv();
    if (item === null) return null;
    return new InflightMessage(item.engine, item.raw);
  }

  /** Close this subscription. The client engine continues running. */
  close(): void {
    this.#sup.close();
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
  readonly #sup: SubscriptionSupervisor<InternalDelivered>;

  /** @internal */
  constructor(sup: SubscriptionSupervisor<InternalDelivered>) {
    this.#sup = sup;
  }

  /** Receive the next message, or `null` if the subscription is closed cleanly. */
  async recv(): Promise<Message | null> {
    const item = await this.#sup.recv();
    if (item === null) return null;
    return new Message(item.raw);
  }

  /** Close this subscription. The client engine continues running. */
  close(): void {
    this.#sup.close();
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
    this.#group = normalizeGroup(group);
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
    const req: SubscribeMsg = {
      topic: this.#topic,
      group: this.#group,
      prefetch: this.#prefetch,
      auto_ack: false,
      partition: 0,
    };
    let first: SubscribeHandle<InternalInflight>;
    try {
      first = await this.#client._subscribeManualOnce(req);
    } catch (err) {
      if (err instanceof FibrilError) throw err;
      throw new BrokenPipeError();
    }
    const sup = new SubscriptionSupervisor<InternalInflight>(
      this.#client,
      req,
      (r) => this.#client._subscribeManualOnce(r),
      first,
      this.#prefetch,
    );
    return new Subscription(sup);
  }

  /**
   * Subscribe with client-side automatic acknowledgement.
   */
  async subAutoAck(): Promise<AutoAckedSubscription> {
    const req: SubscribeMsg = {
      topic: this.#topic,
      group: this.#group,
      prefetch: this.#prefetch,
      auto_ack: false, // matches Rust client: auto-ack is client-side
      partition: 0,
    };
    let first: SubscribeHandle<InternalDelivered>;
    try {
      first = await this.#client._subscribeAutoOnce(req);
    } catch (err) {
      if (err instanceof FibrilError) throw err;
      throw new BrokenPipeError();
    }
    const sup = new SubscriptionSupervisor<InternalDelivered>(
      this.#client,
      req,
      (r) => this.#client._subscribeAutoOnce(r),
      first,
      this.#prefetch,
    );
    return new AutoAckedSubscription(sup);
  }
}
