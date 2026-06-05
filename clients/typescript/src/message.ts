import { decodeMsgpack, encodeMsgpack } from "./codec.js";
import {
  DeserializationError,
  SerializationError,
} from "./errors.js";

/**
 * Message headers sent to the broker.
 *
 * Fibril reserves `fibril.*` headers for broker-owned metadata. User code
 * should avoid that prefix.
 */
export type HeadersInit = Record<string, string>;

const MSGPACK_CONTENT_TYPE = "application/msgpack";
const JSON_CONTENT_TYPE = "application/json";
const TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";

/**
 * Explicit publish message.
 *
 * Plain values passed to `Publisher.publish(...)` are msgpack encoded by
 * default. Use `NewMessage` when you want JSON, text, raw bytes, or custom
 * headers.
 *
 * @example
 * ```ts
 * await publisher.publish(
 *   NewMessage.json({ id: 42 })
 *     .header("x-trace-id", "abc123"),
 * );
 *
 * await publisher.publish(NewMessage.raw(new Uint8Array([1, 2, 3])));
 * ```
 */
export class NewMessage {
  /** Encoded payload bytes sent to the broker. */
  readonly payload: Uint8Array;
  /** Headers sent with the payload. */
  readonly headers: Record<string, string>;

  private constructor(payload: Uint8Array, headers: Record<string, string>) {
    this.payload = payload;
    this.headers = { ...headers };
  }

  /**
   * Encode a value as msgpack and set `application/msgpack`.
   */
  static msgpack<T>(payload: T): NewMessage {
    try {
      return NewMessage.withContentType(
        encodeMsgpack(payload),
        MSGPACK_CONTENT_TYPE,
      );
    } catch (err) {
      throw new SerializationError(
        `Failed to serialize payload: ${(err as Error).message}`,
      );
    }
  }

  /**
   * Encode a value as JSON and set `application/json`.
   */
  static json<T>(payload: T): NewMessage {
    try {
      return NewMessage.withContentType(
        new TextEncoder().encode(JSON.stringify(payload)),
        JSON_CONTENT_TYPE,
      );
    } catch (err) {
      throw new SerializationError(
        `Failed to serialize payload: ${(err as Error).message}`,
      );
    }
  }

  /**
   * Publish raw bytes without setting a content type.
   */
  static raw(payload: Uint8Array): NewMessage {
    return new NewMessage(payload, {});
  }

  /**
   * Publish UTF-8 text and set `text/plain; charset=utf-8`.
   */
  static content(payload: string): NewMessage {
    return NewMessage.withContentType(
      new TextEncoder().encode(payload),
      TEXT_CONTENT_TYPE,
    );
  }

  /**
   * Return a copy with an added or replaced header.
   */
  header(key: string, value: string): NewMessage {
    return new NewMessage(this.payload, { ...this.headers, [key]: value });
  }

  /**
   * Return a copy with the `content-type` header set.
   */
  contentType(contentType: string): NewMessage {
    return this.header("content-type", contentType);
  }

  private static withContentType(
    payload: Uint8Array,
    contentType: string,
  ): NewMessage {
    return new NewMessage(payload, { "content-type": contentType });
  }
}

/**
 * Payload accepted by publisher methods.
 *
 * `NewMessage` preserves explicit headers and payload bytes. Any other value
 * is msgpack encoded and tagged as `application/msgpack`.
 */
export type Publishable<T = unknown> = T | NewMessage;

/**
 * Convert a publishable payload into an explicit message.
 */
export function intoMessage<T>(payload: Publishable<T>): NewMessage {
  if (payload instanceof NewMessage) return payload;
  return NewMessage.msgpack(payload);
}

/**
 * Decode payload bytes according to `content-type`.
 *
 * Missing or empty content type defaults to msgpack. JSON is supported for
 * `application/json`; unsupported types throw `DeserializationError`.
 */
export function deserializeByContentType<T>(
  contentType: string | undefined,
  payload: Uint8Array,
): T {
  const normalized = contentType?.split(";")[0]?.trim();
  if (!normalized || normalized === MSGPACK_CONTENT_TYPE) {
    return decodeMsgpack<T>(payload);
  }
  if (normalized === JSON_CONTENT_TYPE) {
    try {
      return JSON.parse(new TextDecoder().decode(payload)) as T;
    } catch (err) {
      throw new DeserializationError(
        `Failed to deserialize payload: ${(err as Error).message}`,
      );
    }
  }
  throw new DeserializationError(
    `Unsupported content-type \`${normalized}\``,
  );
}
