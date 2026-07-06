using System.Security.Cryptography;

namespace Fibril;

// Topic-bound publisher handles over the client's routed publish. A plain Publisher
// sends a Message; a ReliablePublisher additionally stamps producer-dedup headers so
// a retried publish can be deduplicated by the broker. The frame is built here (and
// decorated by subclasses) before it reaches the routed publish, so the reliable
// stamp lands after user-header validation.

/// <summary>Publishes messages to one topic (and optional group).</summary>
public class Publisher
{
    private readonly Client _client;
    private readonly string _topic;
    private readonly string? _group;
    private readonly TimeSpan _ttl; // default per-message TTL for immediate publishes

    internal Publisher(Client client, string topic, string? group, TimeSpan ttl = default)
    {
        _client = client;
        _topic = topic;
        _group = group;
        _ttl = ttl;
    }

    /// <summary>
    /// Returns a publisher that stamps a default TTL on each immediate publish that
    /// carries no TTL of its own. Applies to immediate publishes only; a per-message
    /// TTL still wins.
    /// </summary>
    public Publisher Expiring(TimeSpan ttl) => new(_client, _topic, _group, ttl);

    /// <summary>Sends a fire-and-forget publish.</summary>
    public Task PublishAsync(Message message, CancellationToken ct = default)
        => _client.PublishUnconfirmedAsync(BuildImmediate(message), ct);

    /// <summary>Sends a publish and waits for the broker-assigned offset.</summary>
    public Task<ulong> PublishConfirmedAsync(Message message, CancellationToken ct = default)
        => _client.PublishConfirmedAsync(BuildImmediate(message), ct);

    /// <summary>
    /// Sends a confirmed publish and returns a handle for its offset, without blocking
    /// on the confirm. Fire several, then await their handles, to pipeline.
    /// </summary>
    public Task<PublishConfirmation> PublishWithConfirmationAsync(Message message, CancellationToken ct = default)
        => _client.PublishWithConfirmationAsync(BuildImmediate(message), ct);

    /// <summary>Sends a fire-and-forget publish scheduled to become visible after <paramref name="delay"/>.</summary>
    public Task PublishDelayedAsync(Message message, TimeSpan delay, CancellationToken ct = default)
        => _client.PublishDelayedUnconfirmedAsync(BuildDelayed(message, delay), ct);

    /// <summary>Sends a delayed publish and waits for the broker-assigned offset.</summary>
    public Task<ulong> PublishDelayedConfirmedAsync(Message message, TimeSpan delay, CancellationToken ct = default)
        => _client.PublishDelayedConfirmedAsync(BuildDelayed(message, delay), ct);

    /// <summary>Sends a delayed confirmed publish and returns a handle for its offset, without blocking.</summary>
    public Task<PublishConfirmation> PublishDelayedWithConfirmationAsync(Message message, TimeSpan delay, CancellationToken ct = default)
        => _client.PublishDelayedWithConfirmationAsync(BuildDelayed(message, delay), ct);

    // Builds the wire publish for an immediate send, applying the publisher's default
    // TTL when the message sets none. Subclasses override to decorate the frame.
    private protected virtual PublishFrame BuildImmediate(Message message)
    {
        var m = message.Ttl == TimeSpan.Zero && _ttl > TimeSpan.Zero ? message.WithTtl(_ttl) : message;
        return m.ToPublish(_topic, _group);
    }

    private protected virtual PublishDelayedFrame BuildDelayed(Message message, TimeSpan delay)
        => message.ToPublishDelayed(_topic, _group, NotBefore(delay));

    // not_before is an absolute unix-ms timestamp, so a relative delay is converted
    // against the current clock.
    private protected static ulong NotBefore(TimeSpan delay)
    {
        var ms = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + (long)delay.TotalMilliseconds;
        return ms < 0 ? 0 : (ulong)ms;
    }
}

/// <summary>
/// Stamps each message with a stable producer id and a monotonic sequence, so a
/// publish retried after a transient failure carries the same identity and the broker
/// can deduplicate it. The client always stamps; owner-side dedup is a broker feature.
/// </summary>
public sealed class ReliablePublisher : Publisher
{
    private readonly string _producerId = NewProducerId();
    private long _seq = -1; // so the first stamp is sequence 0

    internal ReliablePublisher(Client client, string topic, string? group) : base(client, topic, group)
    {
    }

    private protected override PublishFrame BuildImmediate(Message message)
    {
        var frame = base.BuildImmediate(message);
        return frame with { Headers = Stamp(frame.Headers) };
    }

    private protected override PublishDelayedFrame BuildDelayed(Message message, TimeSpan delay)
    {
        var frame = base.BuildDelayed(message, delay);
        return frame with { Headers = Stamp(frame.Headers) };
    }

    // Adds the library-owned producer-dedup headers directly, bypassing the user-header
    // validation that forbids the reserved fibril.* namespace.
    private Dictionary<string, string> Stamp(IReadOnlyDictionary<string, string>? existing)
    {
        var headers = existing is null ? new Dictionary<string, string>() : new Dictionary<string, string>(existing);
        headers[Message.HeaderProducerId] = _producerId;
        headers[Message.HeaderProducerSeq] = ((ulong)Interlocked.Increment(ref _seq)).ToString();
        return headers;
    }

    private static string NewProducerId()
    {
        var bytes = RandomNumberGenerator.GetBytes(16);
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }
}
