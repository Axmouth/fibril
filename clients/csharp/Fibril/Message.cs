using System.Text;
using System.Text.Json;

namespace Fibril;

/// <summary>
/// An outgoing message: a payload plus a content type, optional headers, an
/// optional partition key, and an optional per-message TTL. Build it with
/// Raw/Text/Json and the fluent With helpers.
/// </summary>
public sealed record Message
{
    public byte[] Payload { get; init; } = Array.Empty<byte>();
    public ContentType ContentType { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public byte[]? PartitionKey { get; init; }

    /// <summary>Per-message time to live. Zero means no TTL.</summary>
    public TimeSpan Ttl { get; init; }

    /// <summary>Builds a message from opaque bytes with no content type.</summary>
    public static Message Raw(byte[] payload) => new() { Payload = payload };

    /// <summary>Builds a text/plain message.</summary>
    public static Message Text(string s) => new() { Payload = Encoding.UTF8.GetBytes(s), ContentType = new ContentType(ContentKind.Text) };

    /// <summary>Builds a message by JSON-encoding <paramref name="value"/>.</summary>
    public static Message Json<T>(T value)
    {
        try
        {
            return new Message { Payload = JsonSerializer.SerializeToUtf8Bytes(value), ContentType = new ContentType(ContentKind.Json) };
        }
        catch (JsonException ex)
        {
            throw new SerializationException("json encode: " + ex.Message);
        }
    }

    /// <summary>
    /// Tags already-encoded MessagePack bytes with the msgpack content type.
    /// Encoding is left to the caller, so the client needs no serialization dependency.
    /// </summary>
    public static Message Msgpack(byte[] payload) => new() { Payload = payload, ContentType = new ContentType(ContentKind.Msgpack) };

    /// <summary>Builds a message with a custom MIME content type over opaque bytes.</summary>
    public static Message Custom(string contentType, byte[] payload) => new() { Payload = payload, ContentType = ContentType.CustomType(contentType) };

    /// <summary>Sets the partition key (routing hashes it to a partition).</summary>
    public Message Keyed(byte[] key) => this with { PartitionKey = key };

    /// <summary>Sets a per-message time to live.</summary>
    public Message WithTtl(TimeSpan d) => this with { Ttl = d };

    /// <summary>
    /// Returns a copy with a header set. Reserved keys are rejected at publish time,
    /// not here, so the fluent chain stays exception-free.
    /// </summary>
    public Message WithHeader(string key, string value)
    {
        var h = Headers is null ? new Dictionary<string, string>() : new Dictionary<string, string>(Headers);
        h[key] = value;
        return this with { Headers = h };
    }

    // Reserved header namespaces. User code cannot set fibril.* or stroma.* headers.
    // The library-owned fibril.client.* keys (producer dedup) are set only by the
    // client itself (see the reliable publisher).
    internal const string HeaderProducerId = "fibril.client.producer_id";
    internal const string HeaderProducerSeq = "fibril.client.producer_seq";

    internal static bool IsReservedHeaderKey(string k) => k.StartsWith("fibril.", StringComparison.Ordinal) || k.StartsWith("stroma.", StringComparison.Ordinal);

    // Validates user headers and lowers the message onto the wire publish frame.
    internal PublishFrame ToPublish(string topic, string? group)
    {
        ValidateHeaders();
        return new PublishFrame
        {
            Topic = topic,
            Group = group,
            ContentType = ContentType,
            Headers = Headers,
            Payload = Payload,
            PartitionKey = PartitionKey,
            TtlMs = TtlMillis(),
        };
    }

    internal PublishDelayedFrame ToPublishDelayed(string topic, string? group, ulong notBefore)
    {
        ValidateHeaders();
        return new PublishDelayedFrame
        {
            Topic = topic,
            Group = group,
            NotBefore = notBefore,
            ContentType = ContentType,
            Headers = Headers,
            Payload = Payload,
            PartitionKey = PartitionKey,
        };
    }

    private void ValidateHeaders()
    {
        if (Headers is null)
        {
            return;
        }
        foreach (var key in Headers.Keys)
        {
            if (IsReservedHeaderKey(key))
            {
                throw new SerializationException("header key is reserved and cannot be set by user code: " + key);
            }
        }
    }

    private ulong? TtlMillis() => Ttl > TimeSpan.Zero ? (ulong)Ttl.TotalMilliseconds : null;
}
