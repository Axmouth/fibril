/**
 * Routing / discovery surface, opt in via {@link Client.routing}.
 *
 * Built entirely on the public client API (the subscribe builders, the catalogue
 * feed, and the connection it already holds), so it adds no coupling to the
 * connection core and composes with the delivery-guarantee wrappers: a pattern
 * subscription yields the same message types a single-channel subscription does,
 * and reliable publishing is still reached through the same client.
 *
 * Mirrors the Rust client's `RoutingClient`.
 */

import type { Catalogue, Client } from "./client.js";
import { BoundedQueue } from "./internal/bounded-queue.js";
import type {
  AutoAckedSubscription,
  InflightMessage,
  Message,
  Subscription,
} from "./subscription.js";

/** Per-channel buffering for the merged pattern stream, multiplied by prefetch. */
const CHANNEL_FANIN_BUFFER = 32;

/**
 * The channel a pattern-delivered message came from. A pattern fans in across
 * many channels, so each message is paired with its source for routing back to
 * per-topic handling.
 */
export interface PatternSource {
  topic: string;
  /** The queue's group namespace, or null for the ungrouped default and for
   * streams (which have no group). */
  group: string | null;
}

/** A pattern delivery: the message plus the channel it came from. */
export interface PatternMessage<M> {
  source: PatternSource;
  message: M;
}

/**
 * Owned routing/discovery view over a {@link Client}.
 *
 * Discovery is a first-class but opt-in capability, kept off the default client
 * surface: obtain one with {@link Client.routing}. It shares the client's
 * connection and re-exposes the normal operations (publish, subscribe, stream),
 * so routing composes with them rather than replacing them.
 */
export class RoutingClient {
  readonly #client: Client;

  constructor(client: Client) {
    this.#client = client;
  }

  /** The underlying client, for any operation not surfaced here directly. */
  get client(): Client {
    return this.#client;
  }

  publisher(topic: string) {
    return this.#client.publisher(topic);
  }

  subscribe(topic: string) {
    return this.#client.subscribe(topic);
  }

  stream(topic: string) {
    return this.#client.stream(topic);
  }

  catalogue(): Catalogue {
    return this.#client.catalogue();
  }

  onCatalogueChange(handler: (catalogue: Catalogue) => void): () => void {
    return this.#client.onCatalogueChange(handler);
  }

  /**
   * Begin a pattern subscription over the work queues whose topic matches
   * `pattern`.
   *
   * `pattern` is a `*`-wildcard glob (each `*` matches any run of characters,
   * including empty), the same grammar as the per-subscription header filter,
   * with no regex. `"*"` matches every topic. The subscription fans in across
   * every currently-matching queue and keeps attaching queues that start matching
   * later, so newly declared channels are picked up without a reconnect.
   */
  subscribePattern(pattern: string): PatternSubscribeBuilder {
    return new PatternSubscribeBuilder(this.#client, new TopicGlob(pattern));
  }

  /**
   * Begin a pattern subscription over the Plexus streams whose topic matches
   * `pattern`. Same matching and auto-pickup behaviour as
   * {@link subscribePattern}, over streams instead of work queues.
   */
  subscribeStreamPattern(pattern: string): StreamPatternSubscribeBuilder {
    return new StreamPatternSubscribeBuilder(this.#client, new TopicGlob(pattern));
  }
}

/** Builder for a work-queue pattern subscription. */
export class PatternSubscribeBuilder {
  #prefetch = 1;
  #consumerGroup: string | null = null;

  constructor(
    private readonly client: Client,
    private readonly glob: TopicGlob,
  ) {}

  /** Per-channel prefetch, applied to every attached queue. */
  prefetch(prefetch: number): this {
    if (prefetch < 1 || !Number.isInteger(prefetch)) {
      throw new Error("prefetch must be a positive integer");
    }
    this.#prefetch = prefetch;
    return this;
  }

  /** Consume every matched queue as part of the named exclusive cohort (see
   * {@link SubscriptionBuilder.consumerGroup}). */
  consumerGroup(id: string): this {
    this.#consumerGroup = id;
    return this;
  }

  /** Start with manual acknowledgement; each delivered message must be settled. */
  async sub(): Promise<PatternSubscription<InflightMessage>> {
    const prefetch = this.#prefetch;
    const consumerGroup = this.#consumerGroup;
    const out = mergedQueue<InflightMessage>(prefetch);
    const fan = new PatternFanIn(this.client, this.glob, "queue", out, (client, source, o) =>
      attachManual(subscribeQueue(client, source, prefetch, consumerGroup), source, o),
    );
    await fan.start();
    return new PatternSubscription(fan, out);
  }

  /** Start with client-side automatic acknowledgement, yielding settled messages. */
  async subAutoAck(): Promise<PatternSubscription<Message>> {
    const prefetch = this.#prefetch;
    const consumerGroup = this.#consumerGroup;
    const out = mergedQueue<Message>(prefetch);
    const fan = new PatternFanIn(this.client, this.glob, "queue", out, (client, source, o) =>
      attachAuto(subscribeQueueAuto(client, source, prefetch, consumerGroup), source, o),
    );
    await fan.start();
    return new PatternSubscription(fan, out);
  }
}

/** Builder for a stream pattern subscription. */
export class StreamPatternSubscribeBuilder {
  #prefetch = 16;
  #start: StreamStartChoice = { kind: "latest" };
  #filter: Array<[string, string]> = [];
  #durableName: string | null = null;

  constructor(
    private readonly client: Client,
    private readonly glob: TopicGlob,
  ) {}

  /** Per-channel prefetch, applied to every attached stream. */
  prefetch(prefetch: number): this {
    if (prefetch < 1 || !Number.isInteger(prefetch)) {
      throw new Error("prefetch must be a positive integer");
    }
    this.#prefetch = prefetch;
    return this;
  }

  /** Begin each attached stream at the live tail (the default). */
  fromLatest(): this {
    this.#start = { kind: "latest" };
    return this;
  }

  /** Begin each attached stream at the oldest retained record. */
  fromEarliest(): this {
    this.#start = { kind: "earliest" };
    return this;
  }

  /** Begin `count` records back from each stream's tail. */
  fromLast(count: bigint): this {
    this.#start = { kind: "nback", count };
    return this;
  }

  /** Begin at the first record at or after this wall-clock time (ms). */
  fromTime(timeMs: bigint): this {
    this.#start = { kind: "bytime", timeMs };
    return this;
  }

  /** Add a header-match clause applied to every attached stream. Repeatable, AND-ed. */
  filter(header: string, pattern: string): this {
    this.#filter.push([header, pattern]);
    return this;
  }

  /** Use a durable broker-side cursor of this name on every attached stream; each
   * stream tracks its own cursor under the name. */
  durable(name: string): this {
    this.#durableName = name;
    return this;
  }

  /** Start with manual acknowledgement; completing a message advances its stream's
   * durable cursor. */
  async sub(): Promise<PatternSubscription<InflightMessage>> {
    const config = this.#config();
    const out = mergedQueue<InflightMessage>(config.prefetch);
    const fan = new PatternFanIn(this.client, this.glob, "stream", out, (client, source, o) =>
      attachManual(subscribeStream(client, source, config), source, o),
    );
    await fan.start();
    return new PatternSubscription(fan, out);
  }

  /** Start with client-side automatic acknowledgement, yielding settled messages. */
  async subAutoAck(): Promise<PatternSubscription<Message>> {
    const config = this.#config();
    const out = mergedQueue<Message>(config.prefetch);
    const fan = new PatternFanIn(this.client, this.glob, "stream", out, (client, source, o) =>
      attachAuto(subscribeStreamAuto(client, source, config), source, o),
    );
    await fan.start();
    return new PatternSubscription(fan, out);
  }

  #config(): StreamAttachConfig {
    return {
      prefetch: this.#prefetch,
      start: this.#start,
      filter: this.#filter.slice(),
      durableName: this.#durableName,
    };
  }
}

/**
 * A live fan-in over every channel matching a glob, with auto-pickup of channels
 * that start matching later. Each item carries its {@link PatternSource}. Iterate
 * with `for await`; call `close()` to stop every attached channel and the
 * catalogue watcher.
 */
export class PatternSubscription<M> implements AsyncIterable<PatternMessage<M>> {
  constructor(
    private readonly fan: PatternFanIn<M>,
    private readonly out: BoundedQueue<PatternMessage<M>>,
  ) {}

  async *[Symbol.asyncIterator](): AsyncIterator<PatternMessage<M>> {
    for await (const item of this.out) {
      yield item;
    }
  }

  /** Receive the next message and the channel it came from, or null when closed. */
  async recv(): Promise<PatternMessage<M> | null> {
    return this.out.recv();
  }

  /** Stop the subscription: every attached channel and the catalogue watcher. */
  close(): void {
    this.fan.close();
  }
}

// ---- internal orchestration -------------------------------------------------

type ChannelKind = "queue" | "stream";

type StreamStartChoice =
  | { kind: "latest" }
  | { kind: "earliest" }
  | { kind: "nback"; count: bigint }
  | { kind: "bytime"; timeMs: bigint };

interface StreamAttachConfig {
  prefetch: number;
  start: StreamStartChoice;
  filter: Array<[string, string]>;
  durableName: string | null;
}

/** A subscribed channel forwarding into the merged stream, plus how to stop it. */
interface ActiveChannel {
  close(): void;
}

/** Attach one matched channel into the merged stream. */
type AttachFn<M> = (
  client: Client,
  source: PatternSource,
  out: BoundedQueue<PatternMessage<M>>,
) => Promise<ActiveChannel>;

function mergedQueue<M>(prefetch: number): BoundedQueue<PatternMessage<M>> {
  return new BoundedQueue<PatternMessage<M>>(Math.max(prefetch, 1) * CHANNEL_FANIN_BUFFER);
}

function channelKey(topic: string, group: string | null): string {
  return `${topic} ${group ?? ""}`;
}

/**
 * Drives a pattern subscription: attach the matching channels now and reconcile
 * the attached set against the live catalogue on every change (attaching new
 * matches, closing vanished ones). Stops on `close()`.
 */
class PatternFanIn<M> {
  readonly #active = new Map<string, ActiveChannel>();
  #unsubscribe: (() => void) | null = null;
  #closed = false;

  constructor(
    private readonly client: Client,
    private readonly glob: TopicGlob,
    private readonly kind: ChannelKind,
    private readonly out: BoundedQueue<PatternMessage<M>>,
    private readonly attach: AttachFn<M>,
  ) {}

  async start(): Promise<void> {
    await this.#reconcile();
    if (this.#closed) {
      return;
    }
    // The catalogue feed fires on topology pushes, so new channels are picked up
    // without polling.
    this.#unsubscribe = this.client.onCatalogueChange(() => {
      void this.#reconcile();
    });
  }

  close(): void {
    if (this.#closed) {
      return;
    }
    this.#closed = true;
    this.#unsubscribe?.();
    for (const channel of this.#active.values()) {
      channel.close();
    }
    this.#active.clear();
    this.out.close();
  }

  async #reconcile(): Promise<void> {
    if (this.#closed) {
      return;
    }
    const matching = matchingKeys(this.client.catalogue(), this.glob, this.kind);
    const live = new Set(matching.map((s) => channelKey(s.topic, s.group)));

    // Close channels that vanished so a later re-declare re-attaches.
    for (const [key, channel] of this.#active) {
      if (!live.has(key)) {
        channel.close();
        this.#active.delete(key);
      }
    }

    for (const source of matching) {
      const key = channelKey(source.topic, source.group);
      if (this.#active.has(key)) {
        continue;
      }
      try {
        const channel = await this.attach(this.client, source, this.out);
        if (this.#closed) {
          channel.close();
          return;
        }
        this.#active.set(key, channel);
      } catch {
        // Best effort: a per-channel attach failure is retried on the next
        // catalogue change rather than failing the whole pattern.
      }
    }
  }
}

function matchingKeys(catalogue: Catalogue, glob: TopicGlob, kind: ChannelKind): PatternSource[] {
  if (kind === "queue") {
    return catalogue.queues
      .filter((queue) => glob.matches(queue.topic))
      .map((queue) => ({ topic: queue.topic, group: queue.group }));
  }
  return catalogue.streams
    .filter((stream) => glob.matches(stream.topic))
    .map((stream) => ({ topic: stream.topic, group: null }));
}

function queueBuilder(
  client: Client,
  source: PatternSource,
  prefetch: number,
  consumerGroup: string | null,
) {
  let builder = client.subscribe(source.topic);
  if (source.group != null) {
    builder = builder.group(source.group);
  }
  builder = builder.prefetch(prefetch);
  if (consumerGroup != null) {
    builder = builder.consumerGroup(consumerGroup);
  }
  return builder;
}

function subscribeQueue(
  client: Client,
  source: PatternSource,
  prefetch: number,
  consumerGroup: string | null,
): Promise<Subscription> {
  return queueBuilder(client, source, prefetch, consumerGroup).sub();
}

function subscribeQueueAuto(
  client: Client,
  source: PatternSource,
  prefetch: number,
  consumerGroup: string | null,
): Promise<AutoAckedSubscription> {
  return queueBuilder(client, source, prefetch, consumerGroup).subAutoAck();
}

function streamBuilder(client: Client, source: PatternSource, config: StreamAttachConfig) {
  let builder = client.stream(source.topic).prefetch(config.prefetch);
  switch (config.start.kind) {
    case "latest":
      builder = builder.fromLatest();
      break;
    case "earliest":
      builder = builder.fromEarliest();
      break;
    case "nback":
      builder = builder.fromLast(config.start.count);
      break;
    case "bytime":
      builder = builder.fromTime(config.start.timeMs);
      break;
  }
  for (const [header, pattern] of config.filter) {
    builder = builder.filter(header, pattern);
  }
  if (config.durableName != null) {
    builder = builder.durable(config.durableName);
  }
  return builder;
}

function subscribeStream(
  client: Client,
  source: PatternSource,
  config: StreamAttachConfig,
): Promise<Subscription> {
  return streamBuilder(client, source, config).sub();
}

function subscribeStreamAuto(
  client: Client,
  source: PatternSource,
  config: StreamAttachConfig,
): Promise<AutoAckedSubscription> {
  return streamBuilder(client, source, config).subAutoAck();
}

/** Subscribe (manual ack) and pump the channel's messages into the merged stream. */
async function attachManual<M extends InflightMessage>(
  subPromise: Promise<AsyncIterable<M> & { close(): void }>,
  source: PatternSource,
  out: BoundedQueue<PatternMessage<M>>,
): Promise<ActiveChannel> {
  const sub = await subPromise;
  pump(sub, source, out);
  return { close: () => sub.close() };
}

/** Auto-ack counterpart of {@link attachManual}. */
async function attachAuto<M extends Message>(
  subPromise: Promise<AsyncIterable<M> & { close(): void }>,
  source: PatternSource,
  out: BoundedQueue<PatternMessage<M>>,
): Promise<ActiveChannel> {
  const sub = await subPromise;
  pump(sub, source, out);
  return { close: () => sub.close() };
}

/** Forward a channel's deliveries into the merged stream, tagged with the source.
 * Backpressure flows back to the channel because `send` awaits when the merged
 * queue is full. Ends when the channel ends or the merged queue closes. */
function pump<M>(
  sub: AsyncIterable<M>,
  source: PatternSource,
  out: BoundedQueue<PatternMessage<M>>,
): void {
  void (async () => {
    try {
      for await (const message of sub) {
        await out.send({ source, message });
      }
    } catch {
      // The merged queue closed (pattern subscription dropped) or the channel
      // errored; either way this forwarder is done.
    }
  })();
}

/**
 * A `*`-wildcard topic matcher. Mirrors the broker's header-value matcher so the
 * discovery glob and the per-subscription filter share one grammar: split on
 * `*`, where each `*` matches any run of characters (including empty), no regex.
 */
class TopicGlob {
  readonly #segments: string[];

  constructor(pattern: string) {
    this.#segments = pattern.split("*");
  }

  matches(value: string): boolean {
    const segments = this.#segments;
    if (segments.length === 1) {
      return segments[0] === value;
    }
    const first = segments[0] ?? "";
    const last = segments[segments.length - 1] ?? "";
    if (!value.startsWith(first) || !value.endsWith(last)) {
      return false;
    }
    if (value.length < first.length + last.length) {
      return false;
    }
    let pos = first.length;
    const end = value.length - last.length;
    for (let i = 1; i < segments.length - 1; i++) {
      const mid = segments[i];
      if (mid === undefined || mid.length === 0) {
        continue;
      }
      const found = value.indexOf(mid, pos);
      if (found === -1 || found > end - mid.length) {
        return false;
      }
      pos = found + mid.length;
    }
    return true;
  }
}

export { TopicGlob as _TopicGlobForTests };
