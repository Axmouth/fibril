namespace Fibril;

/// <summary>
/// The kind of a wire parse failure. Mirrors the discriminant every client
/// reports so a malformed frame is classified identically across languages.
/// </summary>
public enum WireErrorKind
{
    UnexpectedEof,
    InvalidMagic,
    TrailingBytes,
    InvalidUuid,
    UnknownContentType,
    UnknownTag,
}

/// <summary>
/// Raised when a frame body cannot be decoded from the wire. Carries a typed
/// <see cref="Kind"/> so callers can branch without string matching.
/// </summary>
public sealed class WireException : Exception
{
    public WireErrorKind Kind { get; }

    public WireException(WireErrorKind kind, string message) : base(message)
    {
        Kind = kind;
    }
}
