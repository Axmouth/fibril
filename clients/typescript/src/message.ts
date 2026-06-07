import { decodeMsgpack, encodeMsgpack } from "./codec.js";
import {
  DeserializationError,
  SerializationError,
} from "./errors.js";
import type { ContentType } from "./protocol.js";

/**
 * Extra message headers sent to the broker.
 *
 * Fibril reserves `fibril.*` and `stroma.*` headers for system metadata. User
 * code should avoid those prefixes.
 */
export type HeadersInit = Record<string, string>;

const MSGPACK_CONTENT_TYPE = "application/msgpack";
const JSON_CONTENT_TYPE = "application/json";
const TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";

export function contentTypeFromHeader(value: string): ContentType {
  const normalized = value.split(";")[0]?.trim();
  if (normalized === MSGPACK_CONTENT_TYPE) return { kind: "msg_pack" };
  if (normalized === JSON_CONTENT_TYPE) return { kind: "json" };
  if (normalized === "text/plain" && value === TEXT_CONTENT_TYPE) {
    return { kind: "text" };
  }
  return { kind: "custom", value };
}

export function contentTypeHeader(
  contentType: ContentType | null | undefined,
): string | undefined {
  if (!contentType) return undefined;
  switch (contentType.kind) {
    case "msg_pack":
      return MSGPACK_CONTENT_TYPE;
    case "json":
      return JSON_CONTENT_TYPE;
    case "text":
      return TEXT_CONTENT_TYPE;
    case "custom":
      return contentType.value;
  }
}

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
  /** Payload content type metadata sent outside the extra header map. */
  readonly contentTypeValue: ContentType | null;
  /** Extra headers sent with the payload. */
  readonly headers: Record<string, string>;

  private constructor(
    payload: Uint8Array,
    contentTypeValue: ContentType | null,
    headers: Record<string, string>,
  ) {
    this.payload = payload;
    this.contentTypeValue = contentTypeValue;
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
    return new NewMessage(payload, null, {});
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
    if (key.toLowerCase() === "content-type") {
      return new NewMessage(
        this.payload,
        contentTypeFromHeader(value),
        this.headers,
      );
    }
    return new NewMessage(this.payload, this.contentTypeValue, {
      ...this.headers,
      [key]: value,
    });
  }

  /**
   * Return a copy with content type metadata set.
   */
  contentType(contentType: string): NewMessage {
    return this.header("content-type", contentType);
  }

  /**
   * Return the content type header value that will be sent as metadata.
   */
  contentTypeHeader(): string | undefined {
    return contentTypeHeader(this.contentTypeValue);
  }

  private static withContentType(
    payload: Uint8Array,
    contentType: string,
  ): NewMessage {
    return new NewMessage(payload, contentTypeFromHeader(contentType), {});
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
