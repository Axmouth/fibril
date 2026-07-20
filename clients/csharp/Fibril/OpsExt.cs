namespace Fibril;

// Cluster, declaration, and stream ops: the second half of the wire surface.
// Same byte-exact rules as Ops.cs, pinned by clients/wire_vectors.json.

/// <summary>A broker endpoint the client can connect to, with optional routing tags.</summary>
internal readonly record struct AdvertisedAddress
{
    public AdvertisedAddress() { }
    public string Host { get; init; } = "";
    public ushort Port { get; init; }
    public IReadOnlyList<string> Tags { get; init; } = Array.Empty<string>();
}

/// <summary>The dead-letter routing policy tag.</summary>
internal enum DlqKind : byte
{
    Discard = 0,
    Global = 1,
    Custom = 2,
}

/// <summary>A queue's dead-letter policy. Topic and Group apply only when Kind is Custom.</summary>
internal readonly record struct DlqPolicy
{
    public DlqPolicy() { }
    public DlqKind Kind { get; init; }
    public string Topic { get; init; } = "";
    public string? Group { get; init; }
}

/// <summary>Declares a queue with optional dead-letter, retry, partition, and per-message TTL settings.</summary>
internal readonly record struct DeclareQueueFrame
{
    public DeclareQueueFrame() { }
    public string Topic { get; init; } = "";
    public string? Group { get; init; }
    public DlqPolicy? DlqPolicy { get; init; }
    public uint? DlqMaxRetries { get; init; }
    public uint? PartitionCount { get; init; }
    public ulong? DefaultMessageTtlMs { get; init; }
}

/// <summary>The broker's declare reply.</summary>
internal readonly record struct DeclareQueueOk(string Status, uint PartitionCount);

/// <summary>A Plexus stream's durability tier.</summary>
internal enum StreamDurability : byte
{
    Ephemeral = 0,
    Speculative = 1,
    Durable = 2,
}

/// <summary>Bounds how much of a stream is retained. Each limit is optional (null = unbounded on that axis).</summary>
internal readonly record struct StreamRetention
{
    public StreamRetention() { }
    public ulong? MaxAgeMs { get; init; }
    public ulong? MaxBytes { get; init; }
    public ulong? MaxRecords { get; init; }
}

/// <summary>Declares a Plexus (fan-out stream) channel.</summary>
internal readonly record struct DeclarePlexusFrame
{
    public DeclarePlexusFrame() { }
    public string Topic { get; init; } = "";
    public uint? PartitionCount { get; init; }

    // The zero value defaults to the durable tier, matching the reference client.
    public StreamDurability Durability { get; init; } = StreamDurability.Durable;
    public StreamRetention Retention { get; init; } = new();
    public uint? ReplicationFactor { get; init; }
}

/// <summary>The broker's stream-declare reply.</summary>
internal readonly record struct DeclarePlexusOk(string Status, uint PartitionCount);

/// <summary>One partition's ownership in the topology.</summary>
internal readonly record struct QueueTopologyEntry
{
    public QueueTopologyEntry() { }
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? Group { get; init; }
    public IReadOnlyList<AdvertisedAddress> OwnerEndpoints { get; init; } = Array.Empty<AdvertisedAddress>();
    public ulong PartitioningVersion { get; init; }
    public uint PartitionCount { get; init; }
}

/// <summary>One stream partition's ownership in the topology.</summary>
internal readonly record struct StreamTopologyEntry
{
    public StreamTopologyEntry() { }
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public IReadOnlyList<AdvertisedAddress> OwnerEndpoints { get; init; } = Array.Empty<AdvertisedAddress>();
    public ulong PartitioningVersion { get; init; }
    public uint PartitionCount { get; init; }
}

/// <summary>
/// A topology snapshot: the ownership of every queue and stream partition the
/// broker knows about, at a generation.
/// </summary>
internal readonly record struct TopologyOk
{
    public TopologyOk() { }
    public ulong Generation { get; init; }
    public IReadOnlyList<QueueTopologyEntry> Queues { get; init; } = Array.Empty<QueueTopologyEntry>();
    public IReadOnlyList<StreamTopologyEntry> Streams { get; init; } = Array.Empty<StreamTopologyEntry>();
}

/// <summary>Asks for the topology, optionally filtered to one topic/group.</summary>
internal readonly record struct TopologyRequest
{
    public TopologyRequest() { }
    public string? Topic { get; init; }
    public string? Group { get; init; }
}

/// <summary>Acknowledges the generation a client now reflects, so the broker can fence a repartition cutover.</summary>
internal readonly record struct TopologyUpdateAck(ulong Generation);

/// <summary>Governs how the broker reconciles a client's subscriptions after a reconnect.</summary>
/// <summary>Governs how the broker reconciles a client's subscriptions after a reconnect.</summary>
public enum ReconcilePolicy : byte
{
    /// <summary>Keep matching subscriptions, close ones the broker cannot prove valid, drop server-side extras.</summary>
    Conservative = 0,

    /// <summary>Additionally ask the broker to recreate client-owned subscriptions missing after a resume.</summary>
    Restore = 1,
}

/// <summary>Describes a subscription a reconnecting client wants the broker to restore.</summary>
internal readonly record struct ReconcileSubscription
{
    public ReconcileSubscription() { }
    public ulong SubId { get; init; }
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? Group { get; init; }
    public bool AutoAck { get; init; }
    public uint Prefetch { get; init; }
    public string? ConsumerGroup { get; init; }
    public uint? ConsumerTarget { get; init; }
    public Uuid? MemberId { get; init; }
}

/// <summary>Asks the broker to reconcile the listed subscriptions under a policy after a reconnect.</summary>
internal readonly record struct ReconcileClient
{
    public ReconcileClient() { }
    public ReconcilePolicy Policy { get; init; }
    public IReadOnlyList<ReconcileSubscription> Subscriptions { get; init; } = Array.Empty<ReconcileSubscription>();
}

/// <summary>The broker's verdict for one reconciled subscription.</summary>
internal enum ReconcileAction : byte
{
    Keep = 0,
    CloseClientSide = 1,
    CloseServerSide = 2,
    RecreateClient = 3,
}

/// <summary>
/// The machine-readable reason taxonomy shared by reconcile results and the
/// SubscriptionClosed push. A raw ushort on the wire, so a code minted by a
/// newer broker survives decode verbatim (C# enums admit any underlying value).
/// </summary>
internal enum ReasonCode : ushort
{
    Unspecified = 0,
    Matched = 1,
    ServerIdChanged = 2,
    ServerRestored = 3,
    ServerMismatch = 4,
    ServerRestoreFailed = 5,
    ServerMissing = 6,
    ClientMissing = 7,
    Recreate = 8,
    TopicDeleted = 20,
    OwnerMoved = 21,
    BrokerShutdown = 22,
    CohortRemoved = 23,
    Lagged = 24,
    ServerError = 40,
}

/// <summary>
/// The broker's verdict for one subscription a reconnecting client asked it to
/// reconcile: the client's view, the server's restored view (if any), the
/// action, and a tagged code beside a human-readable reason.
/// </summary>
internal readonly record struct ReconcileSubscriptionResult
{
    public ReconcileSubscriptionResult() { }
    public ReconcileSubscription? Client { get; init; }
    public ReconcileSubscription? Server { get; init; }
    public ReconcileAction Action { get; init; }
    public ReasonCode Code { get; init; }
    public string Reason { get; init; } = "";
}

/// <summary>An unsolicited broker to client push of the subscriptions the broker believes a client holds.</summary>
internal readonly record struct ReconcileServer
{
    public ReconcileServer() { }
    public IReadOnlyList<ReconcileSubscription> Subscriptions { get; init; } = Array.Empty<ReconcileSubscription>();
}

/// <summary>The broker's reply to a RECONCILE_CLIENT: one verdict per reconciled subscription.</summary>
internal readonly record struct ReconcileResult
{
    public ReconcileResult() { }
    public IReadOnlyList<ReconcileSubscriptionResult> Subscriptions { get; init; } = Array.Empty<ReconcileSubscriptionResult>();
}

/// <summary>Tells the client to retry an op against a different owner.</summary>
internal readonly record struct Redirect
{
    public Redirect() { }
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? Group { get; init; }
    public IReadOnlyList<AdvertisedAddress> OwnerEndpoints { get; init; } = Array.Empty<AdvertisedAddress>();
    public ulong PartitioningVersion { get; init; }
}

/// <summary>Notifies an exclusive-cohort member of its new partition assignment.</summary>
internal readonly record struct AssignmentChanged
{
    public AssignmentChanged() { }
    public string Topic { get; init; } = "";
    public string? Group { get; init; }
    public string ConsumerGroup { get; init; } = "";
    public ulong Generation { get; init; }
    public IReadOnlyList<uint> Assigned { get; init; } = Array.Empty<uint>();
    public IReadOnlyList<uint> Added { get; init; } = Array.Empty<uint>();
    public IReadOnlyList<uint> Revoked { get; init; } = Array.Empty<uint>();
}

/// <summary>The broker's drain notice ahead of a planned shutdown or upgrade.</summary>
internal readonly record struct GoingAway(ulong GraceMs, string Message);

/// <summary>
/// The broker's push that one subscription ended outside a reconcile exchange,
/// so a delivery stream never just goes silent while the connection is up.
/// </summary>
internal readonly record struct SubscriptionClosed(ulong SubId, ReasonCode Code, string Message);

/// <summary>Where a stream subscription begins reading.</summary>
internal enum StreamStartKind : byte
{
    Latest = 0,
    Earliest = 1,
    Offset = 2,
    NBack = 3,
    ByTime = 4,
}

/// <summary>A stream read start position. Value applies only to the offset/nback/bytime kinds.</summary>
internal readonly record struct StreamStart(StreamStartKind Kind, ulong Value = 0);

/// <summary>
/// One header key/pattern predicate on a stream subscription. Filters are an
/// ordered list, not a map, so order is significant on the wire.
/// </summary>
internal readonly record struct StreamFilter(string Key, string Pattern);

/// <summary>Requests a Plexus (fan-out stream) subscription of one partition.</summary>
internal readonly record struct SubscribeStream
{
    public SubscribeStream() { }
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? DurableName { get; init; }
    public StreamStart Start { get; init; }
    public IReadOnlyList<StreamFilter> Filter { get; init; } = Array.Empty<StreamFilter>();
    public uint Prefetch { get; init; }
    public bool AutoAck { get; init; }
}

/// <summary>Composite helpers for the cluster/declaration/stream ops.</summary>
internal static class WireCompositesExt
{
    public static void AdvertisedAddresses(this WireWriter w, IReadOnlyList<AdvertisedAddress> addrs)
    {
        w.U32((uint)addrs.Count);
        foreach (var a in addrs)
        {
            w.WriteStr(a.Host);
            w.U16(a.Port);
            w.U32((uint)a.Tags.Count);
            foreach (var t in a.Tags)
            {
                w.WriteStr(t);
            }
        }
    }

    public static IReadOnlyList<AdvertisedAddress> AdvertisedAddresses(this WireReader r)
    {
        var n = r.U32();
        var outList = new List<AdvertisedAddress>((int)n);
        for (uint i = 0; i < n; i++)
        {
            var host = r.ReadStr();
            var port = r.U16();
            var m = r.U32();
            var tags = new List<string>((int)m);
            for (uint j = 0; j < m; j++)
            {
                tags.Add(r.ReadStr());
            }
            outList.Add(new AdvertisedAddress { Host = host, Port = port, Tags = tags });
        }
        return outList;
    }

    public static void PartitionList(this WireWriter w, IReadOnlyList<uint> ps)
    {
        w.U32((uint)ps.Count);
        foreach (var p in ps)
        {
            w.U32(p);
        }
    }

    public static IReadOnlyList<uint> PartitionList(this WireReader r)
    {
        var n = r.U32();
        var outList = new List<uint>((int)n);
        for (uint i = 0; i < n; i++)
        {
            outList.Add(r.U32());
        }
        return outList;
    }

    public static void OptionalDlqPolicy(this WireWriter w, DlqPolicy? p)
    {
        if (p is null)
        {
            w.U8(0);
            return;
        }
        w.U8(1);
        w.U8((byte)p.Value.Kind);
        if (p.Value.Kind == DlqKind.Custom)
        {
            w.WriteStr(p.Value.Topic);
            w.OptionalStr(p.Value.Group);
        }
    }

    public static DlqPolicy? OptionalDlqPolicy(this WireReader r)
    {
        if (r.U8() != 1)
        {
            return null;
        }
        var kind = (DlqKind)r.U8();
        if ((byte)kind > (byte)DlqKind.Custom)
        {
            throw new WireException(WireErrorKind.UnknownTag, "wire: unknown dlq policy");
        }
        if (kind == DlqKind.Custom)
        {
            return new DlqPolicy { Kind = kind, Topic = r.ReadStr(), Group = r.OptionalStr() };
        }
        return new DlqPolicy { Kind = kind };
    }

    public static void Durability(this WireWriter w, StreamDurability d) => w.U8((byte)d);

    public static StreamDurability Durability(this WireReader r)
    {
        var tag = r.U8();
        if (tag > (byte)StreamDurability.Durable)
        {
            throw new WireException(WireErrorKind.UnknownTag, "wire: unknown durability");
        }
        return (StreamDurability)tag;
    }

    public static void StreamStart(this WireWriter w, StreamStart s)
    {
        w.U8((byte)s.Kind);
        if (s.Kind >= StreamStartKind.Offset)
        {
            w.U64(s.Value);
        }
    }

    public static StreamStart StreamStart(this WireReader r)
    {
        var kind = (StreamStartKind)r.U8();
        if ((byte)kind > (byte)StreamStartKind.ByTime)
        {
            throw new WireException(WireErrorKind.UnknownTag, "wire: unknown stream start");
        }
        var value = kind >= StreamStartKind.Offset ? r.U64() : 0;
        return new StreamStart(kind, value);
    }

    public static void ReconcilePolicy(this WireWriter w, ReconcilePolicy p) => w.U8((byte)p);

    public static ReconcilePolicy ReconcilePolicy(this WireReader r) => r.U8() == 1 ? Fibril.ReconcilePolicy.Restore : Fibril.ReconcilePolicy.Conservative;

    public static void ReconcileSubscription(this WireWriter w, ReconcileSubscription s)
    {
        w.U64(s.SubId);
        w.QueueKey(s.Topic, s.Partition, s.Group);
        w.WriteBool(s.AutoAck);
        w.U32(s.Prefetch);
        w.OptionalStr(s.ConsumerGroup);
        w.OptionalU32(s.ConsumerTarget);
        w.OptionalUuid(s.MemberId);
    }

    public static ReconcileSubscription ReconcileSubscription(this WireReader r)
    {
        var subId = r.U64();
        var (topic, partition, group) = r.QueueKey();
        var autoAck = r.ReadBool();
        var prefetch = r.U32();
        var consumerGroup = r.OptionalStr();
        var consumerTarget = r.OptionalU32();
        var memberId = r.OptionalUuid();
        return new ReconcileSubscription
        {
            SubId = subId,
            Topic = topic,
            Partition = partition,
            Group = group,
            AutoAck = autoAck,
            Prefetch = prefetch,
            ConsumerGroup = consumerGroup,
            ConsumerTarget = consumerTarget,
            MemberId = memberId,
        };
    }

    public static void OptionalReconcileSubscription(this WireWriter w, ReconcileSubscription? s)
    {
        if (s is null)
        {
            w.U8(0);
            return;
        }
        w.U8(1);
        w.ReconcileSubscription(s.Value);
    }

    public static ReconcileSubscription? OptionalReconcileSubscription(this WireReader r)
        => r.U8() == 1 ? r.ReconcileSubscription() : null;
}

internal static partial class WireOps
{
    // ---- declare queue ----

    public static byte[] EncodeDeclareQueue(DeclareQueueFrame d)
    {
        var w = new WireWriter();
        w.Magic("FDQ1");
        w.WriteStr(d.Topic);
        w.OptionalStr(d.Group);
        w.OptionalDlqPolicy(d.DlqPolicy);
        w.OptionalU32(d.DlqMaxRetries);
        w.OptionalU32(d.PartitionCount);
        // Trailing so a peer that omits it still decodes (read as null).
        w.OptionalU64(d.DefaultMessageTtlMs);
        return w.ToArray();
    }

    public static DeclareQueueFrame DecodeDeclareQueue(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FDQ1");
        var topic = r.ReadStr();
        var group = r.OptionalStr();
        var dlq = r.OptionalDlqPolicy();
        var dlqMaxRetries = r.OptionalU32();
        var partitionCount = r.OptionalU32();
        var ttl = r.Remaining > 0 ? r.OptionalU64() : null;
        r.Finish();
        return new DeclareQueueFrame
        {
            Topic = topic,
            Group = group,
            DlqPolicy = dlq,
            DlqMaxRetries = dlqMaxRetries,
            PartitionCount = partitionCount,
            DefaultMessageTtlMs = ttl,
        };
    }

    public static byte[] EncodeDeclareQueueOk(DeclareQueueOk o)
    {
        var w = new WireWriter();
        w.Magic("FDK1");
        w.WriteStr(o.Status);
        w.U32(o.PartitionCount);
        return w.ToArray();
    }

    public static DeclareQueueOk DecodeDeclareQueueOk(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FDK1");
        var o = new DeclareQueueOk(r.ReadStr(), r.U32());
        r.Finish();
        return o;
    }

    // ---- declare plexus ----

    public static byte[] EncodeDeclarePlexus(DeclarePlexusFrame d)
    {
        var w = new WireWriter();
        w.Magic("FDP1");
        w.WriteStr(d.Topic);
        w.OptionalU32(d.PartitionCount);
        w.Durability(d.Durability);
        w.OptionalU64(d.Retention.MaxAgeMs);
        w.OptionalU64(d.Retention.MaxBytes);
        w.OptionalU64(d.Retention.MaxRecords);
        w.OptionalU32(d.ReplicationFactor);
        return w.ToArray();
    }

    public static DeclarePlexusFrame DecodeDeclarePlexus(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FDP1");
        var topic = r.ReadStr();
        var partitionCount = r.OptionalU32();
        var durability = r.Durability();
        var retention = new StreamRetention { MaxAgeMs = r.OptionalU64(), MaxBytes = r.OptionalU64(), MaxRecords = r.OptionalU64() };
        var replicationFactor = r.OptionalU32();
        r.Finish();
        return new DeclarePlexusFrame
        {
            Topic = topic,
            PartitionCount = partitionCount,
            Durability = durability,
            Retention = retention,
            ReplicationFactor = replicationFactor,
        };
    }

    public static byte[] EncodeDeclarePlexusOk(DeclarePlexusOk o)
    {
        var w = new WireWriter();
        w.Magic("FPK1");
        w.WriteStr(o.Status);
        w.U32(o.PartitionCount);
        return w.ToArray();
    }

    public static DeclarePlexusOk DecodeDeclarePlexusOk(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FPK1");
        var o = new DeclarePlexusOk(r.ReadStr(), r.U32());
        r.Finish();
        return o;
    }

    // ---- topology ----

    private static void TopologyBody(WireWriter w, TopologyOk t)
    {
        w.U64(t.Generation);
        w.U32((uint)t.Queues.Count);
        foreach (var e in t.Queues)
        {
            w.QueueKey(e.Topic, e.Partition, e.Group);
            w.AdvertisedAddresses(e.OwnerEndpoints);
            w.U64(e.PartitioningVersion);
            w.U32(e.PartitionCount);
        }
        w.U32((uint)t.Streams.Count);
        foreach (var s in t.Streams)
        {
            w.WriteStr(s.Topic);
            w.U32(s.Partition);
            w.AdvertisedAddresses(s.OwnerEndpoints);
            w.U64(s.PartitioningVersion);
            w.U32(s.PartitionCount);
        }
    }

    private static TopologyOk TopologyBody(WireReader r)
    {
        var generation = r.U64();
        var nq = r.U32();
        var queues = new List<QueueTopologyEntry>((int)nq);
        for (uint i = 0; i < nq; i++)
        {
            var (topic, partition, group) = r.QueueKey();
            var endpoints = r.AdvertisedAddresses();
            var pv = r.U64();
            var pc = r.U32();
            queues.Add(new QueueTopologyEntry
            {
                Topic = topic,
                Partition = partition,
                Group = group,
                OwnerEndpoints = endpoints,
                PartitioningVersion = pv,
                PartitionCount = pc,
            });
        }
        var ns = r.U32();
        var streams = new List<StreamTopologyEntry>((int)ns);
        for (uint i = 0; i < ns; i++)
        {
            var topic = r.ReadStr();
            var partition = r.U32();
            var endpoints = r.AdvertisedAddresses();
            var pv = r.U64();
            var pc = r.U32();
            streams.Add(new StreamTopologyEntry
            {
                Topic = topic,
                Partition = partition,
                OwnerEndpoints = endpoints,
                PartitioningVersion = pv,
                PartitionCount = pc,
            });
        }
        return new TopologyOk { Generation = generation, Queues = queues, Streams = streams };
    }

    public static byte[] EncodeTopologyOk(TopologyOk t)
    {
        var w = new WireWriter();
        w.Magic("FTO1");
        TopologyBody(w, t);
        return w.ToArray();
    }

    public static TopologyOk DecodeTopologyOk(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FTO1");
        var t = TopologyBody(r);
        r.Finish();
        return t;
    }

    // Unsolicited broker->client topology push: the same body as TopologyOk under
    // a distinct magic so a push is distinguishable from a request reply.
    public static byte[] EncodeTopologyUpdate(TopologyOk t)
    {
        var w = new WireWriter();
        w.Magic("FTU1");
        TopologyBody(w, t);
        return w.ToArray();
    }

    public static TopologyOk DecodeTopologyUpdate(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FTU1");
        var t = TopologyBody(r);
        r.Finish();
        return t;
    }

    public static byte[] EncodeTopologyRequest(TopologyRequest req)
    {
        var w = new WireWriter();
        w.Magic("FTP1");
        w.OptionalStr(req.Topic);
        w.OptionalStr(req.Group);
        return w.ToArray();
    }

    public static TopologyRequest DecodeTopologyRequest(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FTP1");
        var req = new TopologyRequest { Topic = r.OptionalStr(), Group = r.OptionalStr() };
        r.Finish();
        return req;
    }

    public static byte[] EncodeTopologyUpdateAck(TopologyUpdateAck a)
    {
        var w = new WireWriter();
        w.Magic("FTA1");
        w.U64(a.Generation);
        return w.ToArray();
    }

    public static TopologyUpdateAck DecodeTopologyUpdateAck(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FTA1");
        var a = new TopologyUpdateAck(r.U64());
        r.Finish();
        return a;
    }

    // ---- reconcile ----

    public static byte[] EncodeReconcileClient(ReconcileClient rc)
    {
        var w = new WireWriter();
        w.Magic("FRC1");
        w.ReconcilePolicy(rc.Policy);
        w.U32((uint)rc.Subscriptions.Count);
        foreach (var s in rc.Subscriptions)
        {
            w.ReconcileSubscription(s);
        }
        return w.ToArray();
    }

    public static ReconcileClient DecodeReconcileClient(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FRC1");
        var policy = r.ReconcilePolicy();
        var n = r.U32();
        var subs = new List<ReconcileSubscription>((int)n);
        for (uint i = 0; i < n; i++)
        {
            subs.Add(r.ReconcileSubscription());
        }
        r.Finish();
        return new ReconcileClient { Policy = policy, Subscriptions = subs };
    }

    public static byte[] EncodeReconcileServer(ReconcileServer rs)
    {
        var w = new WireWriter();
        w.Magic("FRS1");
        w.U32((uint)rs.Subscriptions.Count);
        foreach (var s in rs.Subscriptions)
        {
            w.ReconcileSubscription(s);
        }
        return w.ToArray();
    }

    public static ReconcileServer DecodeReconcileServer(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FRS1");
        var n = r.U32();
        var subs = new List<ReconcileSubscription>((int)n);
        for (uint i = 0; i < n; i++)
        {
            subs.Add(r.ReconcileSubscription());
        }
        r.Finish();
        return new ReconcileServer { Subscriptions = subs };
    }

    public static byte[] EncodeReconcileResult(ReconcileResult rr)
    {
        var w = new WireWriter();
        w.Magic("FRR1");
        w.U32((uint)rr.Subscriptions.Count);
        foreach (var s in rr.Subscriptions)
        {
            w.OptionalReconcileSubscription(s.Client);
            w.OptionalReconcileSubscription(s.Server);
            w.U8((byte)s.Action);
            w.WriteStr(s.Reason);
        }
        // Tagged codes ride as a tail array parallel to the subscriptions, one
        // u16 each (absent on brokers from before the taxonomy).
        foreach (var s in rr.Subscriptions)
        {
            w.U16((ushort)s.Code);
        }
        return w.ToArray();
    }

    public static ReconcileResult DecodeReconcileResult(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FRR1");
        var n = r.U32();
        var subs = new List<ReconcileSubscriptionResult>((int)n);
        for (uint i = 0; i < n; i++)
        {
            subs.Add(new ReconcileSubscriptionResult
            {
                Client = r.OptionalReconcileSubscription(),
                Server = r.OptionalReconcileSubscription(),
                Action = (ReconcileAction)r.U8(),
                Reason = r.ReadStr(),
            });
        }
        // A broker from before the taxonomy sends no code tail, every code
        // stays Unspecified.
        if (r.Remaining > 0)
        {
            for (var i = 0; i < subs.Count; i++)
            {
                subs[i] = subs[i] with { Code = (ReasonCode)r.U16() };
            }
        }
        r.Finish();
        return new ReconcileResult { Subscriptions = subs };
    }

    // ---- redirect ----

    public static byte[] EncodeRedirect(Redirect rd)
    {
        var w = new WireWriter();
        w.Magic("FRD1");
        w.QueueKey(rd.Topic, rd.Partition, rd.Group);
        w.AdvertisedAddresses(rd.OwnerEndpoints);
        w.U64(rd.PartitioningVersion);
        return w.ToArray();
    }

    public static Redirect DecodeRedirect(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FRD1");
        var (topic, partition, group) = r.QueueKey();
        var endpoints = r.AdvertisedAddresses();
        var pv = r.U64();
        r.Finish();
        return new Redirect { Topic = topic, Partition = partition, Group = group, OwnerEndpoints = endpoints, PartitioningVersion = pv };
    }

    // ---- assignment ----

    public static byte[] EncodeAssignmentChanged(AssignmentChanged a)
    {
        var w = new WireWriter();
        w.Magic("FAC1");
        w.WriteStr(a.Topic);
        w.OptionalStr(a.Group);
        w.WriteStr(a.ConsumerGroup);
        w.U64(a.Generation);
        w.PartitionList(a.Assigned);
        w.PartitionList(a.Added);
        w.PartitionList(a.Revoked);
        return w.ToArray();
    }

    public static AssignmentChanged DecodeAssignmentChanged(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FAC1");
        var topic = r.ReadStr();
        var group = r.OptionalStr();
        var consumerGroup = r.ReadStr();
        var generation = r.U64();
        var assigned = r.PartitionList();
        var added = r.PartitionList();
        var revoked = r.PartitionList();
        r.Finish();
        return new AssignmentChanged
        {
            Topic = topic,
            Group = group,
            ConsumerGroup = consumerGroup,
            Generation = generation,
            Assigned = assigned,
            Added = added,
            Revoked = revoked,
        };
    }

    // ---- going away ----

    public static byte[] EncodeGoingAway(GoingAway g)
    {
        var w = new WireWriter();
        w.Magic("FGA1");
        w.U64(g.GraceMs);
        w.WriteStr(g.Message);
        return w.ToArray();
    }

    public static GoingAway DecodeGoingAway(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FGA1");
        var g = new GoingAway(r.U64(), r.ReadStr());
        r.Finish();
        return g;
    }

    // ---- subscription closed ----

    public static byte[] EncodeSubscriptionClosed(SubscriptionClosed sc)
    {
        var w = new WireWriter();
        w.Magic("FSC1");
        w.U64(sc.SubId);
        w.U16((ushort)sc.Code);
        w.WriteStr(sc.Message);
        return w.ToArray();
    }

    public static SubscriptionClosed DecodeSubscriptionClosed(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FSC1");
        var sc = new SubscriptionClosed(r.U64(), (ReasonCode)r.U16(), r.ReadStr());
        r.Finish();
        return sc;
    }

    // ---- subscribe stream ----

    public static byte[] EncodeSubscribeStream(SubscribeStream s)
    {
        var w = new WireWriter();
        w.Magic("FSP1");
        w.WriteStr(s.Topic);
        w.U32(s.Partition);
        w.OptionalStr(s.DurableName);
        w.StreamStart(s.Start);
        w.U32((uint)s.Filter.Count);
        foreach (var f in s.Filter)
        {
            w.WriteStr(f.Key);
            w.WriteStr(f.Pattern);
        }
        w.U32(s.Prefetch);
        w.WriteBool(s.AutoAck);
        return w.ToArray();
    }

    public static SubscribeStream DecodeSubscribeStream(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FSP1");
        var topic = r.ReadStr();
        var partition = r.U32();
        var durableName = r.OptionalStr();
        var start = r.StreamStart();
        var n = r.U32();
        var filter = new List<StreamFilter>((int)n);
        for (uint i = 0; i < n; i++)
        {
            filter.Add(new StreamFilter(r.ReadStr(), r.ReadStr()));
        }
        var prefetch = r.U32();
        var autoAck = r.ReadBool();
        r.Finish();
        return new SubscribeStream
        {
            Topic = topic,
            Partition = partition,
            DurableName = durableName,
            Start = start,
            Filter = filter,
            Prefetch = prefetch,
            AutoAck = autoAck,
        };
    }
}
