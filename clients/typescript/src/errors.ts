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
 * Discriminants for {@link WireError}, mirroring the Rust client's typed wire
 * errors so callers can branch on the failure kind rather than parse messages.
 */
export type WireErrorKind =
  | "unexpected_eof"
  | "invalid_magic"
  | "trailing_bytes"
  | "invalid_uuid"
  | "unknown_content_type"
  | "unknown_tag";

/**
 * A frame body could not be decoded: truncated, wrong magic, trailing bytes, or
 * an unknown tag. `kind` is a stable discriminant; `message` is human-readable.
 */
export class WireError extends FibrilError {
  override readonly name = "WireError";
  constructor(
    readonly kind: WireErrorKind,
    message: string,
  ) {
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
 * A subscription ended with a typed reason instead of a silent stop. Thrown
 * from the subscription's iterator (or `recv`) when the broker or a reconcile
 * verdict closes it - a topic deletion, an ownership move that could not be
 * migrated, a broker shutdown, a recreate the client opted out of, or a
 * server error. A clean user-initiated `close()` ends the iteration without
 * this error.
 */
export class SubscriptionClosedError extends FibrilError {
  override readonly name = "SubscriptionClosedError";
  /** Machine-readable reason code (see `REASON_CODES`). */
  readonly code: number;
  constructor(code: number, message: string) {
    super(message);
    this.code = code;
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
      `redirected to owner ${redirect.owner_endpoints
        .map((a) => `${a.host}:${a.port}`)
        .join(", ")} for ${redirect.topic}/${redirect.partition}`,
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
 * Broker error code for a plaintext connection to a TLS listener. The broker
 * replies with this before closing, so the mismatch is definitive.
 */
export const ERR_TLS_REQUIRED = 426;

/**
 * The broker requires TLS but this client connected plaintext. Reported by
 * the broker itself, so it is definitive rather than inferred.
 */
export class TlsRequiredByBrokerError extends FibrilError {
  override readonly name = "TlsRequiredByBrokerError";
  constructor() {
    super(
      "the broker requires TLS. Enable client TLS: withTlsCaPath(...) or " +
        "withTlsCaFingerprint(...) to trust self-signed broker material, or " +
        "bare withTls() for a publicly issued certificate",
    );
  }
}

/**
 * TLS is enabled but the handshake ended before completing, which usually
 * means the broker listener speaks plaintext.
 */
export class TlsNotSupportedByBrokerError extends FibrilError {
  override readonly name = "TlsNotSupportedByBrokerError";
  constructor(address: string) {
    super(
      `TLS handshake with ${address} ended early, the broker listener is ` +
        "probably plaintext. Disable TLS in the client options, or set " +
        "tls.enabled = true on the broker",
    );
  }
}

/**
 * The broker certificate failed verification. A trust configuration problem,
 * distinct from a transport mismatch.
 */
export class TlsCertificateUntrustedError extends FibrilError {
  override readonly name = "TlsCertificateUntrustedError";
  constructor(detail: string) {
    super(
      `broker certificate verification failed: ${detail}. Trust the broker ` +
        "CA via withTlsCaPath(...) (generated deployments write " +
        "<data_dir>/tls/ca.pem) or pin withTlsCaFingerprint(...) from the " +
        "broker startup log",
    );
  }
}

/**
 * Client-side TLS configuration problem: unreadable caPath, malformed
 * fingerprint, or an invalid server name.
 */
export class TlsConfigError extends FibrilError {
  override readonly name = "TlsConfigError";
}

/**
 * Any other TLS handshake failure.
 */
export class TlsHandshakeError extends FibrilError {
  override readonly name = "TlsHandshakeError";
}

/**
 * The broker requires a client certificate (`tls.client_auth = require`)
 * and this client presented none. Provide one with
 * `withTlsClientCert(certPath, keyPath)`; deployment-CA certificates are
 * issued with `fibrilctl cert issue`.
 */
export class TlsClientCertificateRequiredError extends FibrilError {
  override readonly name = "TlsClientCertificateRequiredError";
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

/** How a caller should treat an error when deciding whether to re-issue an op. */
export type RetryAdvice = "retry" | "do_not_retry";

// Broker error codes that change the retry decision.
const ERR_INVALID = 400; // malformed request: fix it, do not retry
const ERR_NOT_FOUND = 404; // topic/partition not in the cluster: do not retry
const ERR_NOT_OWNER = 409; // topology conflict: a retry re-routes

/**
 * How a caller should treat an error. Transport failures, redirects, topology
 * conflicts, and server-transient (5xx) errors are worth retrying. Not-found,
 * invalid, and local request errors are not. Note the duplicate-publish caveat:
 * a confirmed publish that fails after the broker may have accepted it can
 * duplicate on retry until owner-side dedup ships.
 */
export function retryAdvice(err: unknown): RetryAdvice {
  if (
    err instanceof DisconnectionError ||
    err instanceof BrokenPipeError ||
    err instanceof EofError ||
    err instanceof RedirectError
  ) {
    return "retry";
  }
  if (err instanceof ServerError) {
    if (err.code === ERR_NOT_OWNER) return "retry";
    if (err.code === ERR_NOT_FOUND || err.code === ERR_INVALID) return "do_not_retry";
    if (err.code >= 500) return "retry";
    return "do_not_retry";
  }
  // Local request errors (serialize/deserialize/unexpected) and anything else.
  return "do_not_retry";
}

/** The simple "should I retry this?" check: retryAdvice(err) === "retry". */
export function isRetryable(err: unknown): boolean {
  return retryAdvice(err) === "retry";
}
