import {
  BrokenPipeError,
  DeserializationError,
  FibrilError,
  isTransientError,
} from "./errors.js";
import { contentTypeHeader, deserializeByContentType } from "./message.js";
import type { DeliveryTag, SubscribeMsg } from "./protocol.js";
import type { StreamStart, SubscribeStream } from "./wire.js";
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
  _superviseIntervalMs(): number;
  _refreshTopologyThrottled(): Promise<boolean>;
  _isTopicMissing(topic: string, group: string | null): boolean;
  _ownerEndpoint(topic: string, partition: number, group: string | null): string | null;
  _partitionSet(topic: string, group: string | null): number[];
}

/**
 * Supervises one partition's stream. Forwards it into the shared merged queue,
 * re-subscribes to the current owner when the stream closes (owner death or
 * restart), and watches the topology for a graceful owner move that did not drop
 * the connection. Each delivery carries the engine it arrived on so a manual ack
 * settles against the right connection.
 */
class PartitionSupervisor<R> {
  #engine: Engine;
  #partQueue: BoundedQueue<R>;
  #stopped = false;
  #boundOwner: string | null;
  #timer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly client: SupervisorClient,
    private readonly req: SubscribeMsg,
    private readonly merged: BoundedQueue<Tagged<R>>,
    private readonly resubscribe: (req: SubscribeMsg) => Promise<SubscribeHandle<R>>,
    first: SubscribeHandle<R>,
    onStopped: () => void,
  ) {
    this.#engine = first.engine;
    this.#partQueue = first.queue;
    this.#boundOwner = this.#ownerNow();
    // The merged queue is shared, so this partition closing must not end the others.
    // Notify the fan-in so it can close the merged queue once all have stopped.
    void this.#run().finally(onStopped);
    if (this.client._superviseSubscriptions()) this.#startOwnerCheck();
  }

  stop(): void {
    if (this.#stopped) return;
    this.#stopped = true;
    this.#clearTimer();
    this.#partQueue.close();
  }

  #ownerNow(): string | null {
    return this.client._ownerEndpoint(this.req.topic, this.req.partition ?? 0, this.req.group);
  }

  // Periodically detect a graceful owner move (ownership changed but the old
  // connection stayed up, so the stream did not close). Closing the stream forces
  // a re-subscribe to the new owner. Unref'd so it never keeps the process alive.
  #startOwnerCheck(): void {
    let checking = false;
    this.#timer = setInterval(() => {
      if (this.#stopped || this.client._isShuttingDown()) {
        this.#clearTimer();
        return;
      }
      if (checking) return;
      checking = true;
      void (async () => {
        try {
          await this.client._refreshTopologyThrottled();
          const current = this.#ownerNow();
          if (current !== null && current !== this.#boundOwner) {
            this.#boundOwner = current;
            this.#partQueue.close();
          }
        } finally {
          checking = false;
        }
      })();
    }, Math.max(this.client._superviseIntervalMs(), 1));
    this.#timer.unref?.();
  }

  #clearTimer(): void {
    if (this.#timer) {
      clearInterval(this.#timer);
      this.#timer = null;
    }
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
          this.#stopped = true; // merged closed: the consumer is gone
          break;
        }
      }
      if (
        this.#stopped ||
        this.client._isShuttingDown() ||
        !this.client._superviseSubscriptions()
      ) {
        this.#clearTimer();
        return;
      }
      if (!(await this.#resubscribeWithBackoff())) {
        this.#clearTimer();
        return;
      }
    }
  }

  async #resubscribeWithBackoff(): Promise<boolean> {
    let backoffMs = PUBLISH_RETRY_INITIAL_BACKOFF_MS;
    for (;;) {
      if (this.#stopped || this.client._isShuttingDown()) return false;
      // Re-resolve the owner from a refreshed topology. If a populated view no
      // longer knows the topic, stop re-subscribing (it was deleted).
      if (await this.client._refreshTopologyThrottled()) {
        if (this.client._isTopicMissing(this.req.topic, this.req.group)) return false;
      }
      try {
        const next = await this.resubscribe(this.req);
        if (this.#stopped || this.client._isShuttingDown()) {
          next.queue.close();
          return false;
        }
        this.#engine = next.engine;
        this.#partQueue = next.queue;
        this.#boundOwner = this.#ownerNow();
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
 * Fans one logical subscription in over its partitions. Each partition has its
 * own supervisor feeding a single merged queue that the public subscription
 * reads, so per-partition ordering is preserved and the partitions interleave.
 *
 * Partitions added by a live grow are picked up by a supervised poll that
 * refreshes topology and subscribes the new partitions, matching the Rust
 * client. Shrink (retiring a partition) is left to the per-partition supervisor.
 */
class FanIn<R> {
  readonly #merged: BoundedQueue<Tagged<R>>;
  readonly #partitions: PartitionSupervisor<R>[] = [];
  readonly #covered = new Set<number>();
  #active: number;
  #closed = false;
  #growthTimer: ReturnType<typeof setInterval> | null = null;

  constructor(
    private readonly client: SupervisorClient,
    private readonly baseReq: SubscribeMsg,
    private readonly resubscribe: (req: SubscribeMsg) => Promise<SubscribeHandle<R>>,
    initial: Array<{ partition: number; handle: SubscribeHandle<R> }>,
    prefetch: number,
  ) {
    const cap = Math.max(prefetch, 1) * Math.max(initial.length, 1);
    this.#merged = new BoundedQueue<Tagged<R>>(cap);
    this.#active = initial.length;
    for (const { partition, handle } of initial) {
      this.#covered.add(partition);
      this.#partitions.push(this.#supervise(partition, handle));
    }
    if (this.client._superviseSubscriptions()) this.#startGrowthPoll();
  }

  #supervise(partition: number, handle: SubscribeHandle<R>): PartitionSupervisor<R> {
    return new PartitionSupervisor(
      this.client,
      { ...this.baseReq, partition },
      this.#merged,
      this.resubscribe,
      handle,
      () => this.#onPartitionStopped(),
    );
  }

  async recv(): Promise<Tagged<R> | null> {
    return this.#merged.recv();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#clearGrowthTimer();
    for (const sup of this.#partitions) sup.stop();
    this.#merged.close();
  }

  // Periodically pick up partitions added by a live grow. Unref'd so it never
  // keeps the process alive. Refreshes topology, then subscribes any partition in
  // the current set we are not already covering.
  #startGrowthPoll(): void {
    let polling = false;
    this.#growthTimer = setInterval(() => {
      if (this.#closed || this.client._isShuttingDown()) {
        this.#clearGrowthTimer();
        return;
      }
      if (polling) return;
      polling = true;
      void this.#pickUpNewPartitions().finally(() => {
        polling = false;
      });
    }, this.client._superviseIntervalMs());
    this.#growthTimer.unref?.();
  }

  async #pickUpNewPartitions(): Promise<void> {
    await this.client._refreshTopologyThrottled();
    if (this.#closed) return;
    const set = this.client._partitionSet(this.baseReq.topic, this.baseReq.group);
    for (const partition of set) {
      if (this.#covered.has(partition)) continue;
      try {
        const handle = await this.resubscribe({ ...this.baseReq, partition });
        if (this.#closed) {
          handle.queue.close();
          return;
        }
        this.#covered.add(partition);
        this.#active += 1;
        this.#partitions.push(this.#supervise(partition, handle));
      } catch {
        // Owner not ready yet; retry on the next poll. Do not mark covered.
      }
    }
  }

  #clearGrowthTimer(): void {
    if (this.#growthTimer) {
      clearInterval(this.#growthTimer);
      this.#growthTimer = null;
    }
  }

  // When the last partition permanently stops, end the merged stream so the
  // consumer's iteration completes rather than blocking forever.
  #onPartitionStopped(): void {
    this.#active -= 1;
    if (this.#active <= 0 && !this.#closed) {
      this.#closed = true;
      this.#clearGrowthTimer();
      this.#merged.close();
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
  readonly #fanIn: FanIn<InternalInflight>;

  /** @internal */
  constructor(fanIn: FanIn<InternalInflight>) {
    this.#fanIn = fanIn;
  }

  /** Receive the next message, or `null` if the subscription is closed cleanly. */
  async recv(): Promise<InflightMessage | null> {
    const item = await this.#fanIn.recv();
    if (item === null) return null;
    return new InflightMessage(item.engine, item.raw);
  }

  /** Close this subscription. The client engine continues running. */
  close(): void {
    this.#fanIn.close();
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
  readonly #fanIn: FanIn<InternalDelivered>;

  /** @internal */
  constructor(fanIn: FanIn<InternalDelivered>) {
    this.#fanIn = fanIn;
  }

  /** Receive the next message, or `null` if the subscription is closed cleanly. */
  async recv(): Promise<Message | null> {
    const item = await this.#fanIn.recv();
    if (item === null) return null;
    return new Message(item.raw);
  }

  /** Close this subscription. The client engine continues running. */
  close(): void {
    this.#fanIn.close();
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
  #consumerGroup: string | null = null;
  #consumerTarget: number | null = null;

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
   * Join an exclusive consumer cohort. Members of the same cohort share the
   * topic's partitions: the broker assigns each partition to one member, so the
   * cohort consumes the partitioned topic in order with free failover. Without
   * this, the subscription is a plain competing consumer.
   */
  consumerGroup(consumerGroup: string): this {
    this.#consumerGroup = consumerGroup;
    return this;
  }

  /**
   * Hint how many members the cohort should spread partitions across. This is a
   * capacity signal, not a coverage cap. Only meaningful with a consumer group.
   */
  consumerTarget(target: number): this {
    if (target < 1 || !Number.isInteger(target)) {
      throw new Error("consumerTarget must be a positive integer");
    }
    this.#consumerTarget = target;
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
    const baseReq: SubscribeMsg = {
      topic: this.#topic,
      group: this.#group,
      prefetch: this.#prefetch,
      auto_ack: false,
      consumer_group: this.#consumerGroup,
      consumer_target: this.#consumerTarget,
    };
    const initial = await this.#fanInInitial(baseReq, (r) =>
      this.#client._subscribeManualOnce(r),
    );
    const fanIn = new FanIn<InternalInflight>(
      this.#client,
      baseReq,
      (r) => this.#client._subscribeManualOnce(r),
      initial,
      this.#prefetch,
    );
    return new Subscription(fanIn);
  }

  /**
   * Subscribe with automatic acknowledgement.
   */
  async subAutoAck(): Promise<AutoAckedSubscription> {
    const baseReq: SubscribeMsg = {
      topic: this.#topic,
      group: this.#group,
      prefetch: this.#prefetch,
      // Auto-ack can be done two ways: server-side, by setting auto_ack on the
      // wire so the broker settles each delivery as it sends it; or client-side,
      // by leaving it false and having the client ack after yielding. We use the
      // server-side path (matches the Rust client), so deliveries arrive already
      // settled and the subscription yields plain Messages with nothing to ack.
      auto_ack: true,
      consumer_group: this.#consumerGroup,
      consumer_target: this.#consumerTarget,
    };
    const initial = await this.#fanInInitial(baseReq, (r) =>
      this.#client._subscribeAutoOnce(r),
    );
    const fanIn = new FanIn<InternalDelivered>(
      this.#client,
      baseReq,
      (r) => this.#client._subscribeAutoOnce(r),
      initial,
      this.#prefetch,
    );
    return new AutoAckedSubscription(fanIn);
  }

  // Subscribe to each partition in the topic's partition set. On a partial
  // failure, close the handles already acquired so we leak no connections.
  async #fanInInitial<R>(
    baseReq: SubscribeMsg,
    subscribeOne: (req: SubscribeMsg) => Promise<SubscribeHandle<R>>,
  ): Promise<Array<{ partition: number; handle: SubscribeHandle<R> }>> {
    const partitions = this.#client._partitionSet(this.#topic, this.#group);
    const initial: Array<{ partition: number; handle: SubscribeHandle<R> }> = [];
    try {
      for (const partition of partitions) {
        const handle = await subscribeOne({ ...baseReq, partition });
        initial.push({ partition, handle });
      }
    } catch (err) {
      for (const { handle } of initial) handle.queue.close();
      if (err instanceof FibrilError) throw err;
      throw new BrokenPipeError();
    }
    return initial;
  }
}

/**
 * Builder for a Plexus (fan-out stream) subscription.
 *
 * Construct with `client.stream(topic)`, optionally set a durable name, start
 * position, header filter, and prefetch, then choose manual or auto ack. The
 * subscription reads every partition and fans them in; the SAME durable name
 * tracks an independent cursor per partition.
 *
 * @example
 * ```ts
 * const sub = await client
 *   .stream("events")
 *   .durable("analytics")
 *   .filter("region", "eu-*")
 *   .subManualAck();
 * for await (const msg of sub) await msg.complete();
 * ```
 */
export class StreamSubscriptionBuilder {
  readonly #client: Client;
  #topic: string;
  #partitionCount: number | null = null;
  #durableName: string | null = null;
  #start: StreamStart = { kind: "latest" };
  #filter: [string, string][] = [];
  #prefetch = 16;

  /** @internal */
  constructor(client: Client, topic: string) {
    this.#client = client;
    this.#topic = topic;
  }

  /**
   * Fan in over exactly this many partitions (0..count). When unset, the topology
   * cache supplies the count (just partition 0 in standalone / cold cache). Set
   * this for a multi-partition stream when the cache does not carry it yet.
   */
  partitions(count: number): this {
    if (count < 1 || !Number.isInteger(count)) {
      throw new Error("partitions must be a positive integer");
    }
    this.#partitionCount = count;
    return this;
  }

  /**
   * Use a durable broker-side cursor: the subscription resumes from the committed
   * position (earliest on a fresh name) and acks advance it. Without a durable
   * name the subscription is ephemeral and `start` governs the position.
   */
  durable(name: string): this {
    this.#durableName = name;
    return this;
  }

  /** Begin at the live tail (only records published from now on). The default. */
  fromLatest(): this {
    this.#start = { kind: "latest" };
    return this;
  }

  /** Begin at the oldest retained record. */
  fromEarliest(): this {
    this.#start = { kind: "earliest" };
    return this;
  }

  /** Begin at a specific offset (clamped into the retained window). */
  fromOffset(offset: bigint): this {
    this.#start = { kind: "offset", value: offset };
    return this;
  }

  /** Begin `count` records back from the tail. */
  fromLast(count: bigint): this {
    this.#start = { kind: "nback", value: count };
    return this;
  }

  /** Begin at the first record at or after this wall-clock time (ms). */
  fromTime(timeMs: bigint): this {
    this.#start = { kind: "bytime", value: timeMs };
    return this;
  }

  /**
   * Add a header-match clause: deliver only records whose `header` value matches
   * `pattern` (a literal that may contain `*` wildcards). Repeatable; AND-ed.
   */
  filter(header: string, pattern: string): this {
    this.#filter.push([header, pattern]);
    return this;
  }

  /** Set the maximum number of records the broker may push ahead per partition. */
  prefetch(prefetch: number): this {
    if (prefetch < 1 || !Number.isInteger(prefetch)) {
      throw new Error("prefetch must be a positive integer");
    }
    this.#prefetch = prefetch;
    return this;
  }

  #partitionList(): number[] {
    if (this.#partitionCount != null) {
      return Array.from({ length: this.#partitionCount }, (_, i) => i);
    }
    return this.#client._partitionSet(this.#topic, null);
  }

  #toStreamReq(partition: number, autoAck: boolean): SubscribeStream {
    return {
      topic: this.#topic,
      partition,
      durableName: this.#durableName,
      start: this.#start,
      filter: this.#filter.map((c) => [...c] as [string, string]),
      prefetch: this.#prefetch,
      autoAck,
    };
  }

  // Stream routing reuses the queue fan-in (FanIn/PartitionSupervisor), which work
  // off a SubscribeMsg-shaped routing request; the resubscribe closure translates
  // it to a SubscribeStream carrying this builder's stream options.
  #baseReq(autoAck: boolean): SubscribeMsg {
    return {
      topic: this.#topic,
      group: null,
      prefetch: this.#prefetch,
      auto_ack: autoAck,
      consumer_group: null,
      consumer_target: null,
    };
  }

  async #initial<R>(
    subscribeOne: (req: SubscribeMsg) => Promise<SubscribeHandle<R>>,
    baseReq: SubscribeMsg,
  ): Promise<Array<{ partition: number; handle: SubscribeHandle<R> }>> {
    const initial: Array<{ partition: number; handle: SubscribeHandle<R> }> = [];
    try {
      for (const partition of this.#partitionList()) {
        const handle = await subscribeOne({ ...baseReq, partition });
        initial.push({ partition, handle });
      }
    } catch (err) {
      for (const { handle } of initial) handle.queue.close();
      if (err instanceof FibrilError) throw err;
      throw new BrokenPipeError();
    }
    return initial;
  }

  /** Subscribe with manual acknowledgement; completing a message advances the cursor. */
  async subManualAck(): Promise<Subscription> {
    const baseReq = this.#baseReq(false);
    const subscribeOne = (r: SubscribeMsg) =>
      this.#client._subscribeStreamManualOnce(this.#toStreamReq(r.partition ?? 0, false));
    const initial = await this.#initial(subscribeOne, baseReq);
    const fanIn = new FanIn<InternalInflight>(this.#client, baseReq, subscribeOne, initial, this.#prefetch);
    return new Subscription(fanIn);
  }

  /** Subscribe with automatic acknowledgement; the broker advances the cursor as it delivers. */
  async subAutoAck(): Promise<AutoAckedSubscription> {
    const baseReq = this.#baseReq(true);
    const subscribeOne = (r: SubscribeMsg) =>
      this.#client._subscribeStreamAutoOnce(this.#toStreamReq(r.partition ?? 0, true));
    const initial = await this.#initial(subscribeOne, baseReq);
    const fanIn = new FanIn<InternalDelivered>(this.#client, baseReq, subscribeOne, initial, this.#prefetch);
    return new AutoAckedSubscription(fanIn);
  }
}
