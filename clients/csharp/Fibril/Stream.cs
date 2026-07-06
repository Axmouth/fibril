namespace Fibril;

// Plexus (fan-out stream) subscription surface. Every consumer sees every record,
// so a whole-stream subscription reads all partitions and fans them in. These are
// the public option types over the internal stream wire DTOs.

/// <summary>Where a stream subscription begins reading.</summary>
public readonly record struct StreamStartPosition
{
    internal StreamStartKind Kind { get; }
    internal ulong Value { get; }

    private StreamStartPosition(StreamStartKind kind, ulong value)
    {
        Kind = kind;
        Value = value;
    }

    /// <summary>Start at the newest record (only records published from now on).</summary>
    public static StreamStartPosition Latest => new(StreamStartKind.Latest, 0);

    /// <summary>Start at the oldest retained record.</summary>
    public static StreamStartPosition Earliest => new(StreamStartKind.Earliest, 0);

    /// <summary>Start at a specific offset.</summary>
    public static StreamStartPosition Offset(ulong offset) => new(StreamStartKind.Offset, offset);

    /// <summary>Start <paramref name="count"/> records back from the newest.</summary>
    public static StreamStartPosition FromBack(ulong count) => new(StreamStartKind.NBack, count);

    /// <summary>Start at the first record published at or after <paramref name="time"/>.</summary>
    public static StreamStartPosition FromTime(DateTimeOffset time) => new(StreamStartKind.ByTime, (ulong)Math.Max(time.ToUnixTimeMilliseconds(), 0));

    internal StreamStart ToWire() => new(Kind, Value);
}

/// <summary>One header key/pattern predicate on a stream subscription (<c>*</c> is a glob).</summary>
public readonly record struct StreamHeaderFilter(string Key, string Pattern)
{
    internal StreamFilter ToWire() => new(Key, Pattern);
}

/// <summary>Options for a whole-stream (Plexus) subscription.</summary>
public sealed record StreamSubscribeOptions
{
    public StreamStartPosition Start { get; init; } = StreamStartPosition.Latest;
    public IReadOnlyList<StreamHeaderFilter> Filter { get; init; } = Array.Empty<StreamHeaderFilter>();
    public string? DurableName { get; init; }
    public uint Prefetch { get; init; } = 32;
    public bool AutoAck { get; init; }
}

public sealed partial class Client
{
    /// <summary>
    /// Subscribes to one partition of a Plexus stream, routed to its owner and
    /// following owner redirects, and returns the live subscription.
    /// </summary>
    public Task<Subscription> SubscribeStreamAsync(string topic, uint partition, StreamSubscribeOptions? options = null, CancellationToken ct = default)
    {
        options ??= new StreamSubscribeOptions();
        return SubscribeStreamRoutedAsync(BuildStreamFrame(topic, partition, options), ct);
    }

    /// <summary>
    /// Subscribes to every partition of a Plexus stream and fans the records into one
    /// channel, supervising each partition and picking up partitions added by a live
    /// repartition grow. Every consumer sees every record.
    /// </summary>
    public Task<FanIn> SubscribeStreamTopicAsync(string topic, StreamSubscribeOptions? options = null, CancellationToken ct = default)
    {
        var opts = options ?? new StreamSubscribeOptions();
        return NewFanInAsync(topic, null, opts.Prefetch, (c, p) =>
            SuperviseAttachAsync(opts.Prefetch, cc => SubscribeStreamRoutedAsync(BuildStreamFrame(topic, p, opts), cc), topic, null, c), ct);
    }

    private static SubscribeStream BuildStreamFrame(string topic, uint partition, StreamSubscribeOptions options) => new()
    {
        Topic = topic,
        Partition = partition,
        DurableName = options.DurableName,
        Start = options.Start.ToWire(),
        Filter = options.Filter.Select(f => f.ToWire()).ToList(),
        Prefetch = options.Prefetch,
        AutoAck = options.AutoAck,
    };
}
