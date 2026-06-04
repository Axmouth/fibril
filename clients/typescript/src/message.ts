import { decodeMsgpack, encodeMsgpack } from "./codec.js";
import {
  DeserializationError,
  SerializationError,
} from "./errors.js";

export type HeadersInit = Record<string, string>;

const MSGPACK_CONTENT_TYPE = "application/msgpack";
const JSON_CONTENT_TYPE = "application/json";
const TEXT_CONTENT_TYPE = "text/plain; charset=utf-8";

export class NewMessage {
  readonly payload: Uint8Array;
  readonly headers: Record<string, string>;

  private constructor(payload: Uint8Array, headers: Record<string, string>) {
    this.payload = payload;
    this.headers = { ...headers };
  }

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

  static raw(payload: Uint8Array): NewMessage {
    return new NewMessage(payload, {});
  }

  static content(payload: string): NewMessage {
    return NewMessage.withContentType(
      new TextEncoder().encode(payload),
      TEXT_CONTENT_TYPE,
    );
  }

  header(key: string, value: string): NewMessage {
    return new NewMessage(this.payload, { ...this.headers, [key]: value });
  }

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

export type Publishable<T = unknown> = T | NewMessage;

export function intoMessage<T>(payload: Publishable<T>): NewMessage {
  if (payload instanceof NewMessage) return payload;
  return NewMessage.msgpack(payload);
}

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
