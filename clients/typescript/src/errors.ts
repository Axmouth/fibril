/**
 * Base class for all errors thrown by the Fibril client.
 *
 * @example
 * ```ts
 * try {
 *   await publisher.publish({ id: 1 });
 * } catch (err) {
 *   if (err instanceof FibrilError) {
 *     console.error(err.name, err.message);
 *   }
 * }
 * ```
 */
export class FibrilError extends Error {
  override readonly name: string = "FibrilError";
  constructor(message: string) {
    super(message);
  }
}

/**
 * The client could not establish or maintain a connection to the server.
 */
export class DisconnectionError extends FibrilError {
  override readonly name = "DisconnectionError";
}

/**
 * Failed to deserialize a payload (typically `Message.deserialize`).
 */
export class DeserializationError extends FibrilError {
  override readonly name = "DeserializationError";
}

/**
 * Failed to serialize a payload before publishing.
 */
export class SerializationError extends FibrilError {
  override readonly name = "SerializationError";
}

/**
 * Internal pipe between user-facing handle and engine is broken.
 * Typically means the engine has shut down or is shutting down.
 * Reconnection is advised.
 */
export class BrokenPipeError extends FibrilError {
  override readonly name = "BrokenPipeError";
  constructor(message = "Broken pipe; engine has shut down") {
    super(message);
  }
}

/**
 * Server returned a structured error in response to a request.
 */
export class ServerError extends FibrilError {
  override readonly name = "ServerError";
  /** Numeric broker error code. */
  readonly code: number;
  constructor(code: number, message: string) {
    super(`Server error ${code}: ${message}`);
    this.code = code;
  }
}

/**
 * The broker told the client to retry this op against a different owner. Not a
 * failure: it carries a routing target and must be retried on a connection to
 * that owner. The routing layer (not the per-connection engine) acts on it, so
 * the engine surfaces it as this typed error rather than a generic ServerError.
 */
export class RedirectError extends FibrilError {
  override readonly name = "RedirectError";
  readonly redirect: import("./protocol.js").RedirectMsg;
  constructor(redirect: import("./protocol.js").RedirectMsg) {
    super(
      `redirected to owner ${redirect.owner_endpoint} for ${redirect.topic}/${redirect.partition}`,
    );
    this.redirect = redirect;
  }
}

/**
 * Connection ended before completing a handshake or expected exchange.
 */
export class EofError extends FibrilError {
  override readonly name = "EofError";
  constructor(message = "Unexpected EOF") {
    super(message);
  }
}

/**
 * Catch-all for protocol violations or unexpected states.
 */
export class UnexpectedError extends FibrilError {
  override readonly name = "UnexpectedError";
}

/**
 * Whether an error is a transient transport failure: a connect or severed
 * connection. Narrow on purpose - this is the subset the client retries
 * automatically against a refreshed owner during a failover.
 */
export function isTransientError(err: unknown): boolean {
  return (
    err instanceof DisconnectionError ||
    err instanceof BrokenPipeError ||
    err instanceof EofError
  );
}
