import { encodeMsgpack } from "./codec.js";
import { BrokenPipeError, SerializationError } from "./errors.js";
import type { Engine } from "./engine.js";
import { deferred } from "./internal/deferred.js";

/**
 * Publish messages to a specific topic (and optional group).
 *
 * Construct via `client.publisher(topic)` or `client.publisherGrouped(topic, group)`.
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

  get topic(): string {
    return this.#topic;
  }

  get group(): string | null {
    return this.#group;
  }

  /**
   * Publish without waiting for server confirmation. Resolves once the
   * frame has been queued for the engine.
   *
   * The payload is msgpack-encoded.
   */
  async publishUnconfirmed<T>(payload: T): Promise<void> {
    const bytes = this.#serialize(payload);
    await this.#engine.submit({
      type: "publishUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      payload: bytes,
      published: BigInt(Date.now()),
    });
  }

  /**
   * Publish and wait for the broker's confirmation. Resolves with the
   * topic offset assigned to the message.
   */
  async publish<T>(payload: T): Promise<bigint> {
    const bytes = this.#serialize(payload);
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishConfirmed",
      topic: this.#topic,
      group: this.#group,
      payload: bytes,
      published: BigInt(Date.now()),
      reply,
    });
    try {
      return await reply.promise;
    } catch (err) {
      // Engine rejected our reply with an error type; surface it.
      if (err instanceof Error) throw err;
      throw new BrokenPipeError();
    }
  }

  /**
   * Publish a raw byte payload (no msgpack wrapping). Useful when you
   * already have an encoded payload.
   */
  async publishBytes(payload: Uint8Array): Promise<bigint> {
    const reply = deferred<bigint>();
    await this.#engine.submit({
      type: "publishConfirmed",
      topic: this.#topic,
      group: this.#group,
      payload,
      published: BigInt(Date.now()),
      reply,
    });
    return reply.promise;
  }

  async publishBytesUnconfirmed(payload: Uint8Array): Promise<void> {
    await this.#engine.submit({
      type: "publishUnconfirmed",
      topic: this.#topic,
      group: this.#group,
      payload,
      published: BigInt(Date.now()),
    });
  }

  #serialize<T>(payload: T): Uint8Array {
    try {
      return encodeMsgpack(payload);
    } catch (err) {
      throw new SerializationError(
        `Failed to serialize payload: ${(err as Error).message}`,
      );
    }
  }
}
