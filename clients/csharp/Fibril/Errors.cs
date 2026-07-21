namespace Fibril;

/// <summary>Base type for every error the Fibril client raises, so callers can catch one type.</summary>
public abstract class FibrilException : Exception
{
    protected FibrilException(string message) : base(message)
    {
    }
}

/// <summary>A structured error the broker returned in response to a request.</summary>
public sealed class ServerException : FibrilException
{
    public ushort Code { get; }

    public ServerException(ushort code, string message) : base($"server error {code}: {message}")
    {
        Code = code;
    }
}

/// <summary>
/// A subscription ended with a typed reason instead of a silent stop. Thrown
/// from the subscription's <c>Deliveries()</c> enumeration when the broker or a
/// reconcile verdict closes it - a topic deletion, an ownership move that could
/// not be migrated, a broker shutdown, a recreate the client opted out of, or a
/// server error. A clean user close ends the enumeration without this exception.
/// </summary>
public sealed class SubscriptionClosedException : FibrilException
{
    /// <summary>Machine-readable reason code.</summary>
    public ReasonCode Code { get; }

    public SubscriptionClosedException(ReasonCode code, string message) : base(message)
    {
        Code = code;
    }
}

/// <summary>The client could not establish or maintain a connection to the broker.</summary>
public sealed class DisconnectionException : FibrilException
{
    public DisconnectionException(string message) : base(message)
    {
    }
}

/// <summary>The engine has shut down (or is shutting down), so the operation could not be delivered.</summary>
public sealed class BrokenPipeException : FibrilException
{
    public BrokenPipeException(string? message = null) : base(message ?? "broken pipe: engine has shut down")
    {
    }
}

/// <summary>
/// Settling a delivery whose connection was replaced by a non-resumed reconnect
/// (or a broker restart). The delivery tag was minted by the superseded session
/// and is dead server-side, so no settle frame is sent. The message is not lost:
/// it redelivers on the current subscription per at-least-once. Distinct from
/// <see cref="BrokenPipeException"/> (the connection is down, retry) - a stale
/// delivery must NOT be retried, since re-settling would only report stale again
/// while the message arrives afresh.
/// </summary>
public sealed class StaleDeliveryException : FibrilException
{
    public StaleDeliveryException()
        : base("delivery is stale: its connection was replaced, the message will redeliver")
    {
    }
}

/// <summary>A protocol violation or an otherwise unexpected state.</summary>
public sealed class UnexpectedException : FibrilException
{
    public UnexpectedException(string message) : base(message)
    {
    }
}

/// <summary>A message could not be prepared for sending (a bad value to encode, or a reserved header key).</summary>
public sealed class SerializationException : FibrilException
{
    public SerializationException(string message) : base(message)
    {
    }
}

/// <summary>A delivered payload could not be decoded.</summary>
public sealed class DeserializationException : FibrilException
{
    public DeserializationException(string message) : base(message)
    {
    }
}

/// <summary>
/// The broker requires TLS but this client connected plaintext. Reported by the
/// broker itself (error code 426), so it is definitive.
/// </summary>
public sealed class TlsRequiredByBrokerException : FibrilException
{
    public TlsRequiredByBrokerException()
        : base("the broker requires TLS: set TLS options (trust a CA file, pin a fingerprint, or use the OS roots)")
    {
    }
}

/// <summary>
/// The broker certificate failed verification (chain or pin). A trust-configuration
/// problem, distinct from a transport error.
/// </summary>
public sealed class TlsCertificateUntrustedException : FibrilException
{
    public TlsCertificateUntrustedException(string detail) : base("broker certificate verification failed: " + detail)
    {
    }
}

/// <summary>
/// A client-side TLS configuration problem: an unreadable CA file, a malformed
/// fingerprint, or missing client-cert material.
/// </summary>
public sealed class TlsConfigException : FibrilException
{
    public TlsConfigException(string message) : base(message)
    {
    }
}

/// <summary>
/// TLS is enabled on the client but the handshake ended before completing, which
/// usually means the broker listener speaks plaintext.
/// </summary>
public sealed class TlsNotSupportedByBrokerException : FibrilException
{
    public TlsNotSupportedByBrokerException(string addr)
        : base("TLS handshake with " + addr + " ended early, the broker listener is probably plaintext. Disable TLS in the client options, or enable TLS on the broker")
    {
    }
}

/// <summary>Any other TLS handshake failure.</summary>
public sealed class TlsHandshakeException : FibrilException
{
    public TlsHandshakeException(string message) : base(message)
    {
    }
}

/// <summary>
/// The broker requires a client certificate (mTLS) but this client presented none.
/// Set ClientCertFile and ClientKeyFile in the TLS options to authenticate.
/// </summary>
public sealed class TlsClientCertificateRequiredException : FibrilException
{
    public TlsClientCertificateRequiredException()
        : base("the broker requires a client certificate (mTLS) but none was configured. Set ClientCertFile and ClientKeyFile in the TLS options")
    {
    }
}

/// <summary>
/// The broker told the client to retry this op against a different owner. Not a
/// failure: the routing layer applies the target and retries, so the
/// per-connection engine surfaces it as this typed error.
/// </summary>
public sealed class RedirectException : FibrilException
{
    internal Redirect Redirect { get; }

    /// <summary>The topic the redirect applies to.</summary>
    public string Topic => Redirect.Topic;

    internal RedirectException(Redirect redirect) : base("redirected to a different owner for " + redirect.Topic)
    {
        Redirect = redirect;
    }
}

/// <summary>The broker error code for a plaintext connection to a TLS listener.</summary>
internal static class BrokerErrorCode
{
    public const ushort TlsRequired = 426;
    public const ushort Invalid = 400; // malformed request: fix it, do not retry
    public const ushort NotFound = 404; // topic/partition not in the cluster: do not retry
    public const ushort NotOwner = 409; // topology conflict: a retry re-routes
}

/// <summary>Whether re-issuing an operation is worthwhile.</summary>
public enum RetryAdvice
{
    Retry,
    DoNotRetry,
}

/// <summary>Retry classification for failed operations.</summary>
public static class Retry
{
    /// <summary>
    /// Reports how a caller should treat an error when deciding whether to re-issue
    /// an operation. Transport failures, redirects, topology conflicts, and
    /// server-transient (5xx) errors are worth retrying. Not-found, invalid, and
    /// local request errors are not. A confirmed publish that fails after the
    /// broker may have accepted it can duplicate on retry until owner-side dedup
    /// ships.
    /// </summary>
    public static RetryAdvice Advise(Exception err)
    {
        if (FirstOfType<DisconnectionException>(err) is not null
            || FirstOfType<BrokenPipeException>(err) is not null
            || FirstOfType<RedirectException>(err) is not null)
        {
            return RetryAdvice.Retry;
        }
        if (FirstOfType<ServerException>(err) is { } se)
        {
            if (se.Code == BrokerErrorCode.NotOwner)
            {
                return RetryAdvice.Retry;
            }
            if (se.Code is BrokerErrorCode.NotFound or BrokerErrorCode.Invalid)
            {
                return RetryAdvice.DoNotRetry;
            }
            if (se.Code >= 500)
            {
                return RetryAdvice.Retry;
            }
            return RetryAdvice.DoNotRetry;
        }
        // A stale delivery redelivers on its own; retrying the settle is pointless.
        if (FirstOfType<StaleDeliveryException>(err) is not null)
        {
            return RetryAdvice.DoNotRetry;
        }
        return RetryAdvice.DoNotRetry;
    }

    /// <summary>The simple "should I retry this?" check.</summary>
    public static bool IsRetryable(Exception err) => Advise(err) == RetryAdvice.Retry;

    // Walks the InnerException chain so a wrapped transport error is still classified.
    private static T? FirstOfType<T>(Exception err) where T : Exception
    {
        for (Exception? e = err; e is not null; e = e.InnerException)
        {
            if (e is T match)
            {
                return match;
            }
        }
        return null;
    }
}

/// <summary>
/// Client-local error wording pinned by the shared clients/error_guides.json (the
/// error-message analogue of the wire vectors). The test suite asserts each message
/// carries every required substring, keeping the guidance in parity.
/// </summary>
internal static class Guides
{
    public static string ConnectionRefused(string addr) =>
        "connection refused by " + addr +
        ". Is the broker running and reachable there? Clients connect to the broker port " +
        "(default 9876), not the admin API or dashboard port (default 8081)";

    public const string HeartbeatTimeout =
        "heartbeat timeout: no response from the broker within the timeout window. " +
        "This usually means a network stall or an overloaded or stopped broker rather than a client bug. " +
        "The client will attempt to reconnect if auto-reconnect is enabled.";
}
