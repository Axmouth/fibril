using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Fibril;

namespace Fibril.Tests;

public class EngineTest
{
    private static EngineOptions Opts() => new()
    {
        ClientName = "csharp-test",
        HeartbeatInterval = TimeSpan.FromHours(1), // no heartbeat noise in tests
    };

    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    [Fact]
    public async Task ConnectPerformsHandshake()
    {
        await using var broker = new FakeBroker();
        await using var engine = await Engine.ConnectAsync(broker.Address, Opts(), Timeout());

        Assert.Equal(ResumeOutcome.New, engine.ResumeOutcome);
        Assert.False(engine.IsClosed);
        Assert.Equal(Uuid.Fill(9), engine.ResumeIdentity.OwnerId);
    }

    [Fact]
    public async Task PublishConfirmedReturnsOffset()
    {
        await using var broker = new FakeBroker();
        await using var engine = await Engine.ConnectAsync(broker.Address, Opts(), Timeout());

        var offset = await engine.PublishConfirmedAsync(new PublishFrame { Topic = "t", Payload = new byte[] { 1 } }, Timeout());
        Assert.Equal(FakeBroker.FirstOffset, offset);
    }

    [Fact]
    public async Task SubscribeDeliversAndAcks()
    {
        await using var broker = new FakeBroker { PushDeliveryOnSubscribe = true };
        await using var engine = await Engine.ConnectAsync(broker.Address, Opts(), Timeout());

        var sub = await engine.SubscribeAsync(new SubscribeFrame { Topic = "t", Prefetch = 8 }, noReconcile: true, Timeout());

        Delivery? got = null;
        await foreach (var d in sub.Deliveries(Timeout()))
        {
            got = d;
            d.Complete();
            break;
        }

        Assert.NotNull(got);
        Assert.Equal("hello", got!.Value.Text());
        Assert.True(await broker.WaitForAsync(Op.Ack, Timeout()));
    }

    [Fact]
    public async Task SubscriptionClosedFrameSurfacesTypedReason()
    {
        await using var broker = new FakeBroker { PushDeliveryOnSubscribe = true, PushCloseOnSubscribe = true };
        await using var engine = await Engine.ConnectAsync(broker.Address, Opts(), Timeout());

        var sub = await engine.SubscribeAsync(new SubscribeFrame { Topic = "restart.jobs", Prefetch = 8 }, noReconcile: true, Timeout());

        // The buffered delivery arrives first, then the enumeration throws the
        // typed terminal close.
        var got = new List<string>();
        var ex = await Assert.ThrowsAsync<SubscriptionClosedException>(async () =>
        {
            await foreach (var d in sub.Deliveries(Timeout()))
            {
                got.Add(d.Text());
                d.Complete();
            }
        });

        Assert.Equal(new[] { "hello" }, got);
        Assert.Equal(ReasonCode.TopicDeleted, ex.Code);
    }

    [Fact]
    public async Task ShutdownClosesEngine()
    {
        await using var broker = new FakeBroker();
        var engine = await Engine.ConnectAsync(broker.Address, Opts(), Timeout());
        await engine.ShutdownAsync();
        Assert.True(engine.IsClosed);
    }

    [Fact]
    public async Task ConnectionRefusedCarriesGuide()
    {
        // Bind an ephemeral port then free it, so nothing is listening there.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var addr = ((IPEndPoint)listener.LocalEndpoint).ToString();
        listener.Stop();

        var ex = await Assert.ThrowsAsync<DisconnectionException>(
            () => Engine.ConnectAsync(addr, Opts(), Timeout()));
        foreach (var sub in new[] { "connection refused", "9876", "8081" })
        {
            Assert.Contains(sub, ex.Message.ToLowerInvariant());
        }
    }
}

/// <summary>
/// An in-memory TCP broker that scripts the handshake and op replies, so the engine
/// can be exercised without a real server.
/// </summary>
internal sealed class FakeBroker : IAsyncDisposable
{
    public const ulong FirstOffset = 42;

    private readonly TcpListener _listener;
    private readonly Task _serve;
    private readonly ConcurrentQueue<Op> _received = new();
    private readonly CancellationTokenSource _cts = new();

    public string Address { get; }
    public bool PushDeliveryOnSubscribe { get; init; }
    public bool PushCloseOnSubscribe { get; init; }

    /// <summary>When set, the first publish is answered with a redirect to this endpoint.</summary>
    public string? RedirectPublishTo { get; init; }

    /// <summary>When true, the connection is dropped after answering the first publish.</summary>
    public bool DropAfterFirstPublish { get; init; }

    /// <summary>When set, TOPOLOGY returns the requested topic as a queue with this many partitions.</summary>
    public uint TopologyQueuePartitions { get; init; }

    /// <summary>When set, TOPOLOGY returns the requested topic as a stream with this many partitions.</summary>
    public uint TopologyStreamPartitions { get; init; }

    /// <summary>When set, connections are wrapped in server-side TLS with this certificate.</summary>
    public X509Certificate2? ServerCertificate { get; init; }

    /// <summary>When true, the TLS handshake requires the client to present a certificate (mTLS).</summary>
    public bool RequireClientCertificate { get; init; }

    /// <summary>Topics an unfiltered TOPOLOGY advertises as single-partition queues, for discovery.</summary>
    public IReadOnlyList<string> CatalogueTopics { get; init; } = Array.Empty<string>();

    /// <summary>
    /// When true, the first subscribe delivers then drops; the next connection answers
    /// RECONCILE_CLIENT (keep, re-keyed) and pushes a second delivery on the restored sub.
    /// </summary>
    public bool ReconcileOnReconnect { get; init; }

    private bool _reconcileDropped;

    /// <summary>The member id the broker mints for the first cohort subscribe.</summary>
    public static Uuid CohortMemberId => Uuid.Fill(3);

    /// <summary>Every SUBSCRIBE frame the broker received, for assertions.</summary>
    public ConcurrentQueue<SubscribeFrame> Subscribes { get; } = new();

    /// <summary>Every PUBLISH frame the broker received, for assertions.</summary>
    public ConcurrentQueue<PublishFrame> Publishes { get; } = new();

    /// <summary>Every DECLARE_QUEUE frame the broker received, for assertions.</summary>
    public ConcurrentQueue<DeclareQueueFrame> QueueDeclares { get; } = new();

    /// <summary>Every DECLARE_PLEXUS frame the broker received, for assertions.</summary>
    public ConcurrentQueue<DeclarePlexusFrame> PlexusDeclares { get; } = new();

    private bool _redirected;
    private bool _dropped;
    private ulong _subId;

    public FakeBroker()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Address = ((IPEndPoint)_listener.LocalEndpoint).ToString();
        _serve = ServeAsync(_cts.Token);
    }

    public async Task<bool> WaitForAsync(Op op, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            if (_received.Contains(op))
            {
                return true;
            }
            await Task.Delay(10, ct).ContinueWith(_ => { }, TaskScheduler.Default);
        }
        return false;
    }

    private async Task ServeAsync(CancellationToken ct)
    {
        try
        {
            // Accept in a loop so a reconnect after a drop is served on a fresh connection.
            while (!ct.IsCancellationRequested)
            {
                using var client = await _listener.AcceptTcpClientAsync(ct);
                Stream stream = client.GetStream();
                if (ServerCertificate is not null)
                {
                    var ssl = new SslStream(stream, leaveInnerStreamOpen: false);
                    await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
                    {
                        ServerCertificate = ServerCertificate,
                        ClientCertificateRequired = RequireClientCertificate,
                        // Accept any presented client cert (the mTLS tests only assert
                        // presence). When a client cert is required, reject a missing one so a
                        // certless client fails; otherwise accept the plain-TLS case.
                        RemoteCertificateValidationCallback = (_, cert, _, _) => !RequireClientCertificate || cert is not null,
                        CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
                    });
                    stream = ssl;
                }
                using (stream)
                {
                    await HandleConnectionAsync(stream, ct);
                }
            }
        }
        catch (Exception)
        {
            // Listener stopped or client gone: the test is finishing.
        }
    }

    private async Task HandleConnectionAsync(Stream stream, CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var f = await ReadFrameAsync(stream, ct);
            _received.Enqueue(f.Opcode);
            switch (f.Opcode)
            {
                case Op.Hello:
                    await WriteAsync(stream, Op.HelloOk, f.RequestId, WireOps.EncodeHelloOk(new HelloOk(
                        Protocol.V1, Uuid.Fill(9), Uuid.Fill(8), Uuid.Fill(7), ResumeOutcome.New, "fake", Protocol.ComplianceString)), ct);
                    break;
                case Op.Auth:
                    await WriteAsync(stream, Op.AuthOk, f.RequestId, Array.Empty<byte>(), ct);
                    break;
                case Op.Topology:
                    await WriteAsync(stream, Op.TopologyOk, f.RequestId, WireOps.EncodeTopologyOk(BuildTopology(f)), ct);
                    break;
                case Op.Publish:
                    Publishes.Enqueue(WireOps.DecodePublish(f.Payload));
                    if (RedirectPublishTo is { } target && !_redirected)
                    {
                        _redirected = true;
                        var p = WireOps.DecodePublish(f.Payload);
                        var (host, port) = SplitAddress(target);
                        await WriteAsync(stream, Op.Redirect, f.RequestId, WireOps.EncodeRedirect(new Redirect
                        {
                            Topic = p.Topic,
                            Partition = p.Partition,
                            Group = p.Group,
                            OwnerEndpoints = new[] { new AdvertisedAddress { Host = host, Port = port } },
                        }), ct);
                        break;
                    }
                    await WriteAsync(stream, Op.PublishOk, f.RequestId, WireOps.EncodePublishOk(new PublishOk(FirstOffset)), ct);
                    if (DropAfterFirstPublish && !_dropped)
                    {
                        _dropped = true;
                        return; // close the connection to force a reconnect
                    }
                    break;
                case Op.Subscribe:
                    var sub = WireOps.DecodeSubscribe(f.Payload);
                    Subscribes.Enqueue(sub);
                    var subId = ++_subId;
                    // Mint a member id for the first cohort subscribe, echo the carried one after.
                    var memberId = sub.ConsumerGroup is null ? (Uuid?)null : (sub.MemberId ?? CohortMemberId);
                    await WriteAsync(stream, Op.SubscribeOk, f.RequestId, WireOps.EncodeSubscribeOk(new SubscribeOk
                    {
                        SubId = subId,
                        Topic = sub.Topic,
                        Partition = sub.Partition,
                        Group = sub.Group,
                        Prefetch = sub.Prefetch,
                        ConsumerGroup = sub.ConsumerGroup,
                        MemberId = memberId,
                    }), ct);
                    if (PushDeliveryOnSubscribe)
                    {
                        await WriteAsync(stream, Op.Deliver, 1000, WireOps.EncodeDeliver(new Deliver
                        {
                            SubId = subId,
                            Topic = sub.Topic,
                            Partition = sub.Partition,
                            Offset = 1,
                            DeliveryTag = new DeliveryTag(7),
                            ContentType = new ContentType(ContentKind.Text),
                            Payload = System.Text.Encoding.UTF8.GetBytes("hello"),
                        }), ct);
                    }
                    if (PushCloseOnSubscribe)
                    {
                        await WriteAsync(stream, Op.SubscriptionClosed, 0, WireOps.EncodeSubscriptionClosed(new SubscriptionClosed(
                            subId, ReasonCode.TopicDeleted, "the queue was deleted")), ct);
                    }
                    if (ReconcileOnReconnect && !_reconcileDropped)
                    {
                        _reconcileDropped = true;
                        return; // drop so the next connection exercises RECONCILE_CLIENT
                    }
                    break;
                case Op.ReconcileClient:
                    var rc = WireOps.DecodeReconcileClient(f.Payload);
                    var verdicts = rc.Subscriptions.Select(s => new ReconcileSubscriptionResult
                    {
                        Client = s,
                        Server = s with { SubId = s.SubId + 100 }, // re-key to a fresh server sub id
                        Action = ReconcileAction.Keep,
                        Reason = "kept",
                    }).ToList();
                    await WriteAsync(stream, Op.ReconcileResult, f.RequestId, WireOps.EncodeReconcileResult(new ReconcileResult { Subscriptions = verdicts }), ct);
                    foreach (var restored in rc.Subscriptions)
                    {
                        await WriteAsync(stream, Op.Deliver, 2000, WireOps.EncodeDeliver(new Deliver
                        {
                            SubId = restored.SubId + 100,
                            Topic = restored.Topic,
                            Partition = restored.Partition,
                            Offset = 2,
                            DeliveryTag = new DeliveryTag(8),
                            ContentType = new ContentType(ContentKind.Text),
                            Payload = System.Text.Encoding.UTF8.GetBytes("restored"),
                        }), ct);
                    }
                    break;
                case Op.SubscribeStream:
                    var ss = WireOps.DecodeSubscribeStream(f.Payload);
                    var streamSubId = ++_subId;
                    await WriteAsync(stream, Op.SubscribeOk, f.RequestId, WireOps.EncodeSubscribeOk(new SubscribeOk
                    {
                        SubId = streamSubId,
                        Topic = ss.Topic,
                        Partition = ss.Partition,
                        Prefetch = ss.Prefetch,
                    }), ct);
                    if (PushDeliveryOnSubscribe)
                    {
                        await WriteAsync(stream, Op.Deliver, 1000, WireOps.EncodeDeliver(new Deliver
                        {
                            SubId = streamSubId,
                            Topic = ss.Topic,
                            Partition = ss.Partition,
                            Offset = 1,
                            DeliveryTag = new DeliveryTag(7),
                            ContentType = new ContentType(ContentKind.Text),
                            Payload = System.Text.Encoding.UTF8.GetBytes("record"),
                        }), ct);
                    }
                    break;
                case Op.DeclareQueue:
                    QueueDeclares.Enqueue(WireOps.DecodeDeclareQueue(f.Payload));
                    await WriteAsync(stream, Op.DeclareQueueOk, f.RequestId, WireOps.EncodeDeclareQueueOk(new DeclareQueueOk("created", 1)), ct);
                    break;
                case Op.DeclarePlexus:
                    PlexusDeclares.Enqueue(WireOps.DecodeDeclarePlexus(f.Payload));
                    await WriteAsync(stream, Op.DeclarePlexusOk, f.RequestId, WireOps.EncodeDeclarePlexusOk(new DeclarePlexusOk("created", 1)), ct);
                    break;
                case Op.Ping:
                    await WriteAsync(stream, Op.Pong, f.RequestId, Array.Empty<byte>(), ct);
                    break;
            }
        }
    }

    private TopologyOk BuildTopology(Frame f)
    {
        var req = WireOps.DecodeTopologyRequest(f.Payload);
        // An unfiltered fetch advertises the whole catalogue (one partition each).
        if (req.Topic is null && CatalogueTopics.Count > 0)
        {
            var catalogue = CatalogueTopics
                .Select(t => new QueueTopologyEntry { Topic = t, Partition = 0, PartitionCount = 1 })
                .ToList();
            return new TopologyOk { Generation = 1, Queues = catalogue };
        }
        if (TopologyQueuePartitions == 0 && TopologyStreamPartitions == 0)
        {
            return new TopologyOk { Generation = 1 };
        }
        var topic = req.Topic ?? "orders";
        // No owner endpoints, so routing stays on the bootstrap connection.
        var queues = new List<QueueTopologyEntry>();
        for (uint p = 0; p < TopologyQueuePartitions; p++)
        {
            queues.Add(new QueueTopologyEntry { Topic = topic, Partition = p, PartitionCount = TopologyQueuePartitions });
        }
        var streams = new List<StreamTopologyEntry>();
        for (uint p = 0; p < TopologyStreamPartitions; p++)
        {
            streams.Add(new StreamTopologyEntry { Topic = topic, Partition = p, PartitionCount = TopologyStreamPartitions });
        }
        return new TopologyOk { Generation = 1, Queues = queues, Streams = streams };
    }

    private static (string Host, ushort Port) SplitAddress(string addr)
    {
        var idx = addr.LastIndexOf(':');
        return (addr[..idx], ushort.Parse(addr[(idx + 1)..]));
    }

    private static async Task WriteAsync(Stream s, Op op, ulong requestId, byte[] body, CancellationToken ct)
        => await s.WriteAsync(Frame.Build(op, requestId, body).Encode(), ct);

    private static async Task<Frame> ReadFrameAsync(Stream s, CancellationToken ct)
    {
        var header = new byte[Frame.HeaderSize];
        await s.ReadExactlyAsync(header, ct);
        var payloadLen = BinaryPrimitives.ReadUInt32BigEndian(header);
        var payload = payloadLen > 0 ? new byte[payloadLen] : Array.Empty<byte>();
        if (payloadLen > 0)
        {
            await s.ReadExactlyAsync(payload, ct);
        }
        var full = new byte[Frame.HeaderSize + payload.Length];
        header.CopyTo(full, 0);
        payload.CopyTo(full, Frame.HeaderSize);
        Frame.TryDecode(full, out var frame, out _);
        return frame;
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        try
        {
            await _serve;
        }
        catch
        {
        }
        _cts.Dispose();
    }
}
