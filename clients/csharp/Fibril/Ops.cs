namespace Fibril;

// This file maps the idiomatic message types to and from the wire codec, in both
// directions for every op, so the field order lives in exactly one place. Byte
// layouts are byte-for-byte pinned by the shared vectors in
// clients/wire_vectors.json. Option fields are C# nullables (null = none).

/// <summary>The handshake outcome the broker reports.</summary>
internal enum ResumeOutcome : byte
{
    New = 0,
    Resumed = 1,
    NotFound = 2,
    Rejected = 3,
}

/// <summary>Identity the broker returns and the client offers on reconnect to resume a session.</summary>
internal sealed record ResumeIdentity(Uuid OwnerId, Uuid ClientId, Uuid ResumeToken);

/// <summary>The client's opening handshake frame.</summary>
internal sealed record Hello(string ClientName, string ClientVersion, ushort ProtocolVersion, ResumeIdentity? Resume);

/// <summary>The broker's handshake reply.</summary>
internal sealed record HelloOk(
    ushort ProtocolVersion,
    Uuid OwnerId,
    Uuid ClientId,
    Uuid ResumeToken,
    ResumeOutcome Outcome,
    string ServerName,
    string Compliance);

/// <summary>A username/password authentication frame.</summary>
internal sealed record AuthFrame(string Username, string Password);

/// <summary>A structured broker error.</summary>
internal sealed record ErrorMsg(ushort Code, string Message);

/// <summary>Identifies a delivery to ack or nack.</summary>
internal readonly record struct DeliveryTag(ulong Epoch);

/// <summary>A publish request. Group, PartitionKey, and TtlMs are optional (null = absent).</summary>
internal sealed record PublishFrame
{
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? Group { get; init; }
    public bool RequireConfirm { get; init; }
    public ContentType ContentType { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public byte[] Payload { get; init; } = Array.Empty<byte>();
    public ulong Published { get; init; }
    public byte[]? PartitionKey { get; init; }
    public ulong PartitioningVersion { get; init; }
    public ulong? TtlMs { get; init; }
}

/// <summary>A publish scheduled to become visible at NotBefore.</summary>
internal sealed record PublishDelayedFrame
{
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? Group { get; init; }
    public bool RequireConfirm { get; init; }
    public ulong NotBefore { get; init; }
    public ContentType ContentType { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public byte[] Payload { get; init; } = Array.Empty<byte>();
    public ulong Published { get; init; }
    public byte[]? PartitionKey { get; init; }
    public ulong PartitioningVersion { get; init; }
}

/// <summary>The broker's confirmed-publish reply carrying the assigned offset.</summary>
internal sealed record PublishOk(ulong Offset);

/// <summary>Settles one or more deliveries as processed.</summary>
internal sealed record AckFrame
{
    public string Topic { get; init; } = "";
    public string? Group { get; init; }
    public uint Partition { get; init; }
    public IReadOnlyList<DeliveryTag> Tags { get; init; } = Array.Empty<DeliveryTag>();
}

/// <summary>Returns deliveries unprocessed, optionally requeuing them (NotBefore delays the requeue).</summary>
internal sealed record NackFrame
{
    public string Topic { get; init; } = "";
    public string? Group { get; init; }
    public uint Partition { get; init; }
    public IReadOnlyList<DeliveryTag> Tags { get; init; } = Array.Empty<DeliveryTag>();
    public bool Requeue { get; init; }
    public ulong? NotBefore { get; init; }
}

/// <summary>Requests delivery of one partition of a topic.</summary>
internal sealed record SubscribeFrame
{
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? Group { get; init; }
    public uint Prefetch { get; init; }
    public bool AutoAck { get; init; }
    public string? ConsumerGroup { get; init; }
    public uint? ConsumerTarget { get; init; }
    public Uuid? MemberId { get; init; }
}

/// <summary>The broker's subscribe reply, echoing the assignment and the server-chosen sub id.</summary>
internal sealed record SubscribeOk
{
    public ulong SubId { get; init; }
    public string Topic { get; init; } = "";
    public uint Partition { get; init; }
    public string? Group { get; init; }
    public uint Prefetch { get; init; }
    public string? ConsumerGroup { get; init; }
    public uint? ConsumerTarget { get; init; }
    public Uuid? MemberId { get; init; }
}

/// <summary>A broker-pushed message delivery.</summary>
internal sealed record Deliver
{
    public ulong SubId { get; init; }
    public string Topic { get; init; } = "";
    public string? Group { get; init; }
    public uint Partition { get; init; }
    public ulong Offset { get; init; }
    public DeliveryTag DeliveryTag { get; init; }
    public ulong Published { get; init; }
    public ulong PublishReceived { get; init; }
    public ContentType ContentType { get; init; }
    public IReadOnlyDictionary<string, string>? Headers { get; init; }
    public byte[] Payload { get; init; } = Array.Empty<byte>();
}

/// <summary>
/// Shared field-group helpers layered on the primitive wire reader/writer. These
/// are domain composites (queue keys, settle tags, resume identity), kept out of
/// the primitive codec so Wire.cs stays purely about bytes.
/// </summary>
internal static class WireComposites
{
    public static void QueueKey(this WireWriter w, string topic, uint partition, string? group)
    {
        w.WriteStr(topic);
        w.U32(partition);
        w.OptionalStr(group);
    }

    public static (string Topic, uint Partition, string? Group) QueueKey(this WireReader r)
        => (r.ReadStr(), r.U32(), r.OptionalStr());

    public static void SettleTags(this WireWriter w, IReadOnlyList<DeliveryTag> tags)
    {
        w.U32((uint)tags.Count);
        foreach (var t in tags)
        {
            w.U64(t.Epoch);
        }
    }

    public static IReadOnlyList<DeliveryTag> SettleTags(this WireReader r)
    {
        var n = r.U32();
        var tags = new List<DeliveryTag>((int)n);
        for (uint i = 0; i < n; i++)
        {
            tags.Add(new DeliveryTag(r.U64()));
        }
        return tags;
    }

    public static void ResumeOutcome(this WireWriter w, ResumeOutcome o) => w.U8((byte)o);

    public static ResumeOutcome ResumeOutcome(this WireReader r)
    {
        var tag = r.U8();
        if (tag > (byte)Fibril.ResumeOutcome.Rejected)
        {
            throw new WireException(WireErrorKind.UnknownTag, "wire: unknown resume outcome");
        }
        return (ResumeOutcome)tag;
    }

    public static void OptionalResumeIdentity(this WireWriter w, ResumeIdentity? ri)
    {
        if (ri is null)
        {
            w.U8(0);
            return;
        }
        w.U8(1);
        w.WriteUuid(ri.OwnerId);
        w.WriteUuid(ri.ClientId);
        w.WriteUuid(ri.ResumeToken);
    }

    public static ResumeIdentity? OptionalResumeIdentity(this WireReader r)
        => r.U8() == 1 ? new ResumeIdentity(r.ReadUuid(), r.ReadUuid(), r.ReadUuid()) : null;
}

/// <summary>Byte-exact encode/decode for every v1 op body. Split across Ops.cs and OpsExt.cs.</summary>
internal static partial class WireOps
{
    // ---- handshake ----

    public static byte[] EncodeHello(Hello h)
    {
        var w = new WireWriter();
        w.Magic("FHL1");
        w.WriteStr(h.ClientName);
        w.WriteStr(h.ClientVersion);
        w.U16(h.ProtocolVersion);
        w.OptionalResumeIdentity(h.Resume);
        return w.ToArray();
    }

    public static Hello DecodeHello(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FHL1");
        var h = new Hello(r.ReadStr(), r.ReadStr(), r.U16(), r.OptionalResumeIdentity());
        r.Finish();
        return h;
    }

    public static byte[] EncodeHelloOk(HelloOk h)
    {
        var w = new WireWriter();
        w.Magic("FHO1");
        w.U16(h.ProtocolVersion);
        w.WriteUuid(h.OwnerId);
        w.WriteUuid(h.ClientId);
        w.WriteUuid(h.ResumeToken);
        w.ResumeOutcome(h.Outcome);
        w.WriteStr(h.ServerName);
        w.WriteStr(h.Compliance);
        return w.ToArray();
    }

    public static HelloOk DecodeHelloOk(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FHO1");
        var h = new HelloOk(r.U16(), r.ReadUuid(), r.ReadUuid(), r.ReadUuid(), r.ResumeOutcome(), r.ReadStr(), r.ReadStr());
        r.Finish();
        return h;
    }

    public static byte[] EncodeAuth(AuthFrame a)
    {
        var w = new WireWriter();
        w.Magic("FAU1");
        w.WriteStr(a.Username);
        w.WriteStr(a.Password);
        return w.ToArray();
    }

    public static AuthFrame DecodeAuth(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FAU1");
        var a = new AuthFrame(r.ReadStr(), r.ReadStr());
        r.Finish();
        return a;
    }

    public static byte[] EncodeError(ErrorMsg e)
    {
        var w = new WireWriter();
        w.Magic("FER1");
        w.U16(e.Code);
        w.WriteStr(e.Message);
        return w.ToArray();
    }

    public static ErrorMsg DecodeError(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FER1");
        var e = new ErrorMsg(r.U16(), r.ReadStr());
        r.Finish();
        return e;
    }

    // ---- publish ----

    private static void PublishCommon(WireWriter w, PublishFrame p)
    {
        w.WriteStr(p.Topic);
        w.OptionalStr(p.Group);
        w.U32(p.Partition);
        w.WriteBool(p.RequireConfirm);
        w.WriteContentType(p.ContentType);
        w.WriteHeaders(p.Headers);
        w.U64(p.Published);
        w.OptionalBytes(p.PartitionKey);
        w.U64(p.PartitioningVersion);
        w.WriteBytes(p.Payload);
    }

    public static byte[] EncodePublish(PublishFrame p)
    {
        var w = new WireWriter();
        w.Magic("FPB1");
        PublishCommon(w, p);
        // Trailing so a peer that omits it still decodes (read as null).
        w.OptionalU64(p.TtlMs);
        return w.ToArray();
    }

    public static PublishFrame DecodePublish(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FPB1");
        var topic = r.ReadStr();
        var group = r.OptionalStr();
        var partition = r.U32();
        var requireConfirm = r.ReadBool();
        var ct = r.ReadContentType();
        var headers = r.ReadHeaders();
        var published = r.U64();
        var partitionKey = r.OptionalBytes();
        var partitioningVersion = r.U64();
        var payload = r.ReadBytes();
        // Trailing optional: absent when the peer has not been updated to send it.
        var ttl = r.Remaining > 0 ? r.OptionalU64() : null;
        r.Finish();
        return new PublishFrame
        {
            Topic = topic,
            Group = group,
            Partition = partition,
            RequireConfirm = requireConfirm,
            ContentType = ct,
            Headers = headers,
            Published = published,
            PartitionKey = partitionKey,
            PartitioningVersion = partitioningVersion,
            Payload = payload,
            TtlMs = ttl,
        };
    }

    public static byte[] EncodePublishDelayed(PublishDelayedFrame p)
    {
        var w = new WireWriter();
        w.Magic("FPD1");
        w.WriteStr(p.Topic);
        w.OptionalStr(p.Group);
        w.U32(p.Partition);
        w.WriteBool(p.RequireConfirm);
        w.U64(p.NotBefore);
        w.WriteContentType(p.ContentType);
        w.WriteHeaders(p.Headers);
        w.U64(p.Published);
        w.OptionalBytes(p.PartitionKey);
        w.U64(p.PartitioningVersion);
        w.WriteBytes(p.Payload);
        return w.ToArray();
    }

    public static PublishDelayedFrame DecodePublishDelayed(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FPD1");
        var topic = r.ReadStr();
        var group = r.OptionalStr();
        var partition = r.U32();
        var requireConfirm = r.ReadBool();
        var notBefore = r.U64();
        var ct = r.ReadContentType();
        var headers = r.ReadHeaders();
        var published = r.U64();
        var partitionKey = r.OptionalBytes();
        var partitioningVersion = r.U64();
        var payload = r.ReadBytes();
        r.Finish();
        return new PublishDelayedFrame
        {
            Topic = topic,
            Group = group,
            Partition = partition,
            RequireConfirm = requireConfirm,
            NotBefore = notBefore,
            ContentType = ct,
            Headers = headers,
            Published = published,
            PartitionKey = partitionKey,
            PartitioningVersion = partitioningVersion,
            Payload = payload,
        };
    }

    public static byte[] EncodePublishOk(PublishOk o)
    {
        var w = new WireWriter();
        w.Magic("FPO1");
        w.U64(o.Offset);
        return w.ToArray();
    }

    public static PublishOk DecodePublishOk(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FPO1");
        var o = new PublishOk(r.U64());
        r.Finish();
        return o;
    }

    // ---- settle ----

    public static byte[] EncodeAck(AckFrame a)
    {
        var w = new WireWriter();
        w.Magic("FAK1");
        w.WriteStr(a.Topic);
        w.OptionalStr(a.Group);
        w.U32(a.Partition);
        w.SettleTags(a.Tags);
        return w.ToArray();
    }

    public static AckFrame DecodeAck(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FAK1");
        var a = new AckFrame { Topic = r.ReadStr(), Group = r.OptionalStr(), Partition = r.U32(), Tags = r.SettleTags() };
        r.Finish();
        return a;
    }

    public static byte[] EncodeNack(NackFrame n)
    {
        var w = new WireWriter();
        w.Magic("FNK1");
        w.WriteStr(n.Topic);
        w.OptionalStr(n.Group);
        w.U32(n.Partition);
        w.SettleTags(n.Tags);
        w.WriteBool(n.Requeue);
        w.OptionalU64(n.NotBefore);
        return w.ToArray();
    }

    public static NackFrame DecodeNack(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FNK1");
        var topic = r.ReadStr();
        var group = r.OptionalStr();
        var partition = r.U32();
        var tags = r.SettleTags();
        var requeue = r.ReadBool();
        var notBefore = r.OptionalU64();
        r.Finish();
        return new NackFrame { Topic = topic, Group = group, Partition = partition, Tags = tags, Requeue = requeue, NotBefore = notBefore };
    }

    // ---- subscribe ----

    public static byte[] EncodeSubscribe(SubscribeFrame s)
    {
        var w = new WireWriter();
        w.Magic("FSB1");
        w.QueueKey(s.Topic, s.Partition, s.Group);
        w.U32(s.Prefetch);
        w.WriteBool(s.AutoAck);
        w.OptionalStr(s.ConsumerGroup);
        w.OptionalU32(s.ConsumerTarget);
        w.OptionalUuid(s.MemberId);
        return w.ToArray();
    }

    public static SubscribeFrame DecodeSubscribe(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FSB1");
        var (topic, partition, group) = r.QueueKey();
        var prefetch = r.U32();
        var autoAck = r.ReadBool();
        var consumerGroup = r.OptionalStr();
        var consumerTarget = r.OptionalU32();
        var memberId = r.OptionalUuid();
        r.Finish();
        return new SubscribeFrame
        {
            Topic = topic,
            Partition = partition,
            Group = group,
            Prefetch = prefetch,
            AutoAck = autoAck,
            ConsumerGroup = consumerGroup,
            ConsumerTarget = consumerTarget,
            MemberId = memberId,
        };
    }

    public static byte[] EncodeSubscribeOk(SubscribeOk s)
    {
        var w = new WireWriter();
        w.Magic("FSO1");
        w.U64(s.SubId);
        w.QueueKey(s.Topic, s.Partition, s.Group);
        w.U32(s.Prefetch);
        w.OptionalStr(s.ConsumerGroup);
        w.OptionalU32(s.ConsumerTarget);
        w.OptionalUuid(s.MemberId);
        return w.ToArray();
    }

    public static SubscribeOk DecodeSubscribeOk(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FSO1");
        var subId = r.U64();
        var (topic, partition, group) = r.QueueKey();
        var prefetch = r.U32();
        var consumerGroup = r.OptionalStr();
        var consumerTarget = r.OptionalU32();
        var memberId = r.OptionalUuid();
        r.Finish();
        return new SubscribeOk
        {
            SubId = subId,
            Topic = topic,
            Partition = partition,
            Group = group,
            Prefetch = prefetch,
            ConsumerGroup = consumerGroup,
            ConsumerTarget = consumerTarget,
            MemberId = memberId,
        };
    }

    // ---- deliver ----

    public static byte[] EncodeDeliver(Deliver d)
    {
        var w = new WireWriter();
        w.Magic("FDL1");
        w.U64(d.SubId);
        w.WriteStr(d.Topic);
        w.OptionalStr(d.Group);
        w.U32(d.Partition);
        w.U64(d.Offset);
        w.U64(d.DeliveryTag.Epoch);
        w.U64(d.Published);
        w.U64(d.PublishReceived);
        w.WriteContentType(d.ContentType);
        w.WriteHeaders(d.Headers);
        w.WriteBytes(d.Payload);
        return w.ToArray();
    }

    public static Deliver DecodeDeliver(byte[] body)
    {
        var r = new WireReader(body);
        r.ExpectMagic("FDL1");
        var subId = r.U64();
        var topic = r.ReadStr();
        var group = r.OptionalStr();
        var partition = r.U32();
        var offset = r.U64();
        var tag = new DeliveryTag(r.U64());
        var published = r.U64();
        var publishReceived = r.U64();
        var ct = r.ReadContentType();
        var headers = r.ReadHeaders();
        var payload = r.ReadBytes();
        r.Finish();
        return new Deliver
        {
            SubId = subId,
            Topic = topic,
            Group = group,
            Partition = partition,
            Offset = offset,
            DeliveryTag = tag,
            Published = published,
            PublishReceived = publishReceived,
            ContentType = ct,
            Headers = headers,
            Payload = payload,
        };
    }
}
