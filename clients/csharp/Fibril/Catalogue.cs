namespace Fibril;

// The catalogue is a snapshot of the channels declared in the cluster: every queue
// and Plexus stream the client currently knows about, with partition counts.
// Derived from topology and kept live by topology pushes, so it needs no extra
// round-trips. Queues and streams are sorted (by topic, then group) for a stable
// order.

/// <summary>One declared work queue in the catalogue.</summary>
public sealed record QueueInfo(string Topic, string? Group, int PartitionCount);

/// <summary>One declared Plexus stream in the catalogue.</summary>
public sealed record StreamInfo(string Topic, int PartitionCount);

/// <summary>A snapshot of the declared queues and streams in the cluster.</summary>
public sealed record Catalogue
{
    public IReadOnlyList<QueueInfo> Queues { get; init; } = Array.Empty<QueueInfo>();
    public IReadOnlyList<StreamInfo> Streams { get; init; } = Array.Empty<StreamInfo>();
    public ulong Generation { get; init; }

    // Reports whether two catalogues list the same set of queues and streams
    // (ignoring generation), so an owner-only topology churn fires no listener.
    internal bool SameChannels(Catalogue other)
    {
        if (Queues.Count != other.Queues.Count || Streams.Count != other.Streams.Count)
        {
            return false;
        }
        for (var i = 0; i < Queues.Count; i++)
        {
            var a = Queues[i];
            var b = other.Queues[i];
            if (a.Topic != b.Topic || (a.Group ?? "") != (b.Group ?? "") || a.PartitionCount != b.PartitionCount)
            {
                return false;
            }
        }
        for (var i = 0; i < Streams.Count; i++)
        {
            if (Streams[i] != other.Streams[i])
            {
                return false;
            }
        }
        return true;
    }
}

internal static class CatalogueBuilder
{
    // Derives the catalogue from a topology snapshot. Topology lists one entry per
    // partition, so queues dedupe by (topic, group) and streams by topic; both come
    // back sorted for a deterministic order.
    public static Catalogue FromTopology(TopologyOk topo)
    {
        var queues = new Dictionary<string, QueueInfo>();
        foreach (var q in topo.Queues)
        {
            var count = (int)Math.Max(q.PartitionCount, 1);
            queues[(q.Topic) + "\0" + (q.Group ?? "")] = new QueueInfo(q.Topic, q.Group, count);
        }
        var streams = new Dictionary<string, StreamInfo>();
        foreach (var s in topo.Streams)
        {
            var count = (int)Math.Max(s.PartitionCount, 1);
            streams[s.Topic] = new StreamInfo(s.Topic, count);
        }

        var orderedQueues = queues.Values
            .OrderBy(q => q.Topic, StringComparer.Ordinal)
            .ThenBy(q => q.Group ?? "", StringComparer.Ordinal)
            .ToList();
        var orderedStreams = streams.Values
            .OrderBy(s => s.Topic, StringComparer.Ordinal)
            .ToList();

        return new Catalogue { Generation = topo.Generation, Queues = orderedQueues, Streams = orderedStreams };
    }
}
