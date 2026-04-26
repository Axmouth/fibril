/**
 * Base class for all errors thrown by the Fibril client.
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
  readonly code: number;
  constructor(code: number, message: string) {
    super(`Server error ${code}: ${message}`);
    this.code = code;
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
