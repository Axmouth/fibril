namespace Fibril;

// Public declaration options over the internal declare wire DTOs: dead-letter and
// TTL for queues, durability and retention for Plexus streams.

/// <summary>Dead-letter routing for a queue's exhausted or rejected messages.</summary>
public sealed record DeadLetterPolicy
{
    internal DlqKind Kind { get; private init; }
    internal string DlqTopic { get; private init; } = "";
    internal string? DlqGroup { get; private init; }

    /// <summary>Drop dead-lettered messages.</summary>
    public static DeadLetterPolicy Discard => new() { Kind = DlqKind.Discard };

    /// <summary>Route dead-lettered messages to the cluster's global dead-letter queue.</summary>
    public static DeadLetterPolicy Global => new() { Kind = DlqKind.Global };

    /// <summary>Route dead-lettered messages to a specific topic (and optional group).</summary>
    public static DeadLetterPolicy ToTopic(string topic, string? group = null)
        => new() { Kind = DlqKind.Custom, DlqTopic = topic, DlqGroup = group };

    internal DlqPolicy ToWire() => new() { Kind = Kind, Topic = DlqTopic, Group = DlqGroup };
}

/// <summary>Queue declaration config.</summary>
public sealed record QueueConfig
{
    public int? PartitionCount { get; init; }

    /// <summary>The group namespace this queue is declared within.</summary>
    public string? Group { get; init; }

    public DeadLetterPolicy? DeadLetter { get; init; }
    public int? MaxRetries { get; init; }

    /// <summary>Default per-message time to live for messages published without their own TTL.</summary>
    public TimeSpan? DefaultMessageTtl { get; init; }

    /// <summary>
    /// How the queue confirms and delivers. Null leaves it to the broker. Only
    /// the first declare of a queue sets its mode. A later declare naming
    /// another mode fails with error code 409. Brokers that predate the mode
    /// refuse a declare that names one.
    /// </summary>
    public QueueDurability? Durability { get; init; }
}

/// <summary>How a queue confirms and delivers.</summary>
public enum QueueDurability : byte
{
    /// <summary>
    /// Confirm and deliver only once each required copy has synced a message
    /// to disk. New queues get this unless the cluster is configured with
    /// <c>replica_buffered</c>.
    /// </summary>
    Durable = 0,

    /// <summary>
    /// Confirm and deliver once each required copy has written a message to the
    /// OS, before it is synced to disk. Lower latency, but power loss or a
    /// storage failure on every required node at once can lose recent
    /// confirmed messages.
    /// </summary>
    Buffered = 1,
}

/// <summary>A Plexus stream's durability tier.</summary>
public enum StreamDurabilityTier : byte
{
    Ephemeral = 0,
    Speculative = 1,
    Durable = 2,
}

/// <summary>Bounds on how much of a stream is retained. Each limit is optional (null = unbounded on that axis).</summary>
public sealed record StreamRetentionPolicy
{
    public TimeSpan? MaxAge { get; init; }
    public long? MaxBytes { get; init; }
    public long? RetainRecords { get; init; }
}

/// <summary>Plexus (fan-out stream) declaration config.</summary>
public sealed record StreamConfig
{
    public int? PartitionCount { get; init; }
    public StreamDurabilityTier Durability { get; init; } = StreamDurabilityTier.Durable;
    public StreamRetentionPolicy? Retention { get; init; }
    public int? ReplicationFactor { get; init; }
}

public sealed partial class Client
{
    /// <summary>
    /// Declares a queue with dead-letter, retry, partition, TTL and durability
    /// options. When the declare names a mode, the outcome carries the mode in
    /// effect and any broker warnings.
    /// </summary>
    public async Task<DeclareOutcome> DeclareQueueAsync(string topic, QueueConfig options, CancellationToken ct = default)
    {
        var ok = await BootstrapOpAsync(eng => eng.DeclareQueueAsync(new DeclareQueueFrame
        {
            Topic = topic,
            Group = options.Group,
            DlqPolicy = options.DeadLetter?.ToWire(),
            DlqMaxRetries = options.MaxRetries is null ? null : (uint)options.MaxRetries.Value,
            PartitionCount = options.PartitionCount is null ? null : (uint)options.PartitionCount.Value,
            DefaultMessageTtlMs = options.DefaultMessageTtl is { } ttl ? (ulong)ttl.TotalMilliseconds : null,
            Durability = options.Durability,
        }, ct), ct).ConfigureAwait(false);
        return new DeclareOutcome(ok.Status, (int)ok.PartitionCount)
        {
            Durability = ok.Durability,
            Warnings = ok.Warnings,
        };
    }

    /// <summary>Declares a Plexus stream with partition, durability, retention, and replication options.</summary>
    public async Task<DeclareOutcome> DeclarePlexusAsync(string topic, StreamConfig options, CancellationToken ct = default)
    {
        var retention = options.Retention is { } r
            ? new StreamRetention
            {
                MaxAgeMs = r.MaxAge is { } age ? (ulong)age.TotalMilliseconds : null,
                MaxBytes = r.MaxBytes is null ? null : (ulong)r.MaxBytes.Value,
                MaxRecords = r.RetainRecords is null ? null : (ulong)r.RetainRecords.Value,
            }
            : new StreamRetention();

        var ok = await BootstrapOpAsync(eng => eng.DeclarePlexusAsync(new DeclarePlexusFrame
        {
            Topic = topic,
            PartitionCount = options.PartitionCount is null ? null : (uint)options.PartitionCount.Value,
            Durability = (StreamDurability)(byte)options.Durability,
            Retention = retention,
            ReplicationFactor = options.ReplicationFactor is null ? null : (uint)options.ReplicationFactor.Value,
        }, ct), ct).ConfigureAwait(false);
        return new DeclareOutcome(ok.Status, (int)ok.PartitionCount);
    }
}
