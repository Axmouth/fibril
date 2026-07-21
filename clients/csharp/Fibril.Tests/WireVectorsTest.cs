using System.Text.Json;
using Fibril;

namespace Fibril.Tests;

public class WireVectorsTest
{
    // A conformance case: the struct-encoded bytes, and a decode+re-encode of the
    // canonical bytes, both of which must equal the shared vector.
    private sealed record VCase(string Name, byte[] Encoded, Func<byte[], byte[]> RoundTrip);

    private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    private static Dictionary<string, string> Vector(string k, string v) => new() { [k] = v };

    private static List<VCase> Cases()
    {
        var one = new Dictionary<string, string> { ["x-a"] = "1" };
        return new List<VCase>
        {
            new("hello", WireOps.EncodeHello(new Hello("py-client", "0.1.0", 1, new ResumeIdentity(Uuid.Fill(1), Uuid.Fill(2), Uuid.Fill(3)))),
                b => WireOps.EncodeHello(WireOps.DecodeHello(b))),
            new("hello_no_resume", WireOps.EncodeHello(new Hello("c", "v", 1, null)),
                b => WireOps.EncodeHello(WireOps.DecodeHello(b))),
            new("hello_ok", WireOps.EncodeHelloOk(new HelloOk(1, Uuid.Fill(9), Uuid.Fill(8), Uuid.Fill(7), ResumeOutcome.Resumed, "srv", "v=1;x")),
                b => WireOps.EncodeHelloOk(WireOps.DecodeHelloOk(b))),
            new("auth", WireOps.EncodeAuth(new AuthFrame("u", "p")),
                b => WireOps.EncodeAuth(WireOps.DecodeAuth(b))),
            new("error", WireOps.EncodeError(new ErrorMsg(409, "not owner")),
                b => WireOps.EncodeError(WireOps.DecodeError(b))),
            new("publish", WireOps.EncodePublish(new PublishFrame
            {
                Topic = "orders", Partition = 3, Group = "g", RequireConfirm = true,
                ContentType = new ContentType(ContentKind.Json), Headers = new Dictionary<string, string> { ["x-a"] = "1" },
                Payload = new byte[] { 1, 2, 3, 4 }, Published = 1234567890, PartitionKey = new byte[] { 9, 9 },
                PartitioningVersion = 5, TtlMs = 60000,
            }), b => WireOps.EncodePublish(WireOps.DecodePublish(b))),
            new("publish_no_ttl", WireOps.EncodePublish(new PublishFrame { Topic = "t", ContentType = new ContentType(ContentKind.None) }),
                b => WireOps.EncodePublish(WireOps.DecodePublish(b))),
            new("publish_custom_ct", WireOps.EncodePublish(new PublishFrame
            {
                Topic = "t", ContentType = ContentType.CustomType("application/x-thing"), Payload = new byte[] { 7 }, Published = 1,
            }), b => WireOps.EncodePublish(WireOps.DecodePublish(b))),
            new("publish_delayed", WireOps.EncodePublishDelayed(new PublishDelayedFrame
            {
                Topic = "t", Partition = 1, RequireConfirm = true, NotBefore = 999,
                ContentType = new ContentType(ContentKind.Text), Headers = new Dictionary<string, string> { ["k"] = "v" },
                Payload = new byte[] { 5, 6 }, Published = 42, PartitioningVersion = 2,
            }), b => WireOps.EncodePublishDelayed(WireOps.DecodePublishDelayed(b))),
            new("publish_ok", WireOps.EncodePublishOk(new PublishOk(777)),
                b => WireOps.EncodePublishOk(WireOps.DecodePublishOk(b))),
            new("deliver", WireOps.EncodeDeliver(new Deliver
            {
                SubId = 11, Topic = "t", Group = "g", Partition = 2, Offset = 100,
                DeliveryTag = new DeliveryTag(5), Published = 7, PublishReceived = 8,
                ContentType = new ContentType(ContentKind.Msgpack), Headers = new Dictionary<string, string> { ["h"] = "1" },
                Payload = new byte[] { 3, 2, 1 },
            }), b => WireOps.EncodeDeliver(WireOps.DecodeDeliver(b))),
            new("ack", WireOps.EncodeAck(new AckFrame { Topic = "t", Tags = new[] { new DeliveryTag(1), new DeliveryTag(2) } }),
                b => WireOps.EncodeAck(WireOps.DecodeAck(b))),
            new("nack", WireOps.EncodeNack(new NackFrame { Topic = "t", Group = "g", Partition = 1, Tags = new[] { new DeliveryTag(9) }, Requeue = true, NotBefore = 5000 }),
                b => WireOps.EncodeNack(WireOps.DecodeNack(b))),
            new("nack_no_nb", WireOps.EncodeNack(new NackFrame { Topic = "t", Tags = Array.Empty<DeliveryTag>() }),
                b => WireOps.EncodeNack(WireOps.DecodeNack(b))),
            new("subscribe", WireOps.EncodeSubscribe(new SubscribeFrame
            {
                Topic = "t", Partition = 1, Group = "g", Prefetch = 32, AutoAck = false,
                ConsumerGroup = "cg", ConsumerTarget = 2, MemberId = Uuid.Fill(4),
            }), b => WireOps.EncodeSubscribe(WireOps.DecodeSubscribe(b))),
            new("subscribe_min", WireOps.EncodeSubscribe(new SubscribeFrame { Topic = "t", AutoAck = true }),
                b => WireOps.EncodeSubscribe(WireOps.DecodeSubscribe(b))),
            new("subscribe_ok", WireOps.EncodeSubscribeOk(new SubscribeOk
            {
                SubId = 5, Topic = "t", Partition = 1, Group = "g", Prefetch = 16, ConsumerGroup = "cg", MemberId = Uuid.Fill(4),
            }), b => WireOps.EncodeSubscribeOk(WireOps.DecodeSubscribeOk(b))),
            new("declare", WireOps.EncodeDeclareQueue(new DeclareQueueFrame
            {
                Topic = "t", Group = "g", DlqPolicy = new DlqPolicy { Kind = DlqKind.Custom, Topic = "dlq" },
                DlqMaxRetries = 3, PartitionCount = 4, DefaultMessageTtlMs = 30000,
            }), b => WireOps.EncodeDeclareQueue(WireOps.DecodeDeclareQueue(b))),
            new("declare_min", WireOps.EncodeDeclareQueue(new DeclareQueueFrame { Topic = "t" }),
                b => WireOps.EncodeDeclareQueue(WireOps.DecodeDeclareQueue(b))),
            new("declare_ok", WireOps.EncodeDeclareQueueOk(new DeclareQueueOk("created", 4)),
                b => WireOps.EncodeDeclareQueueOk(WireOps.DecodeDeclareQueueOk(b))),
            new("declare_plexus", WireOps.EncodeDeclarePlexus(new DeclarePlexusFrame
            {
                Topic = "t", PartitionCount = 4, Durability = StreamDurability.Speculative,
                Retention = new StreamRetention { MaxAgeMs = 60000, MaxRecords = 1000000 }, ReplicationFactor = 2,
            }), b => WireOps.EncodeDeclarePlexus(WireOps.DecodeDeclarePlexus(b))),
            new("declare_plexus_min", WireOps.EncodeDeclarePlexus(new DeclarePlexusFrame { Topic = "t" }),
                b => WireOps.EncodeDeclarePlexus(WireOps.DecodeDeclarePlexus(b))),
            new("declare_plexus_ok", WireOps.EncodeDeclarePlexusOk(new DeclarePlexusOk("created", 4)),
                b => WireOps.EncodeDeclarePlexusOk(WireOps.DecodeDeclarePlexusOk(b))),
            new("topology_ok", WireOps.EncodeTopologyOk(new TopologyOk
            {
                Generation = 12,
                Queues = new[]
                {
                    new QueueTopologyEntry { Topic = "t", Partition = 0, OwnerEndpoints = new[] { new AdvertisedAddress { Host = "127.0.0.1", Port = 7000 } }, PartitioningVersion = 1, PartitionCount = 2 },
                    new QueueTopologyEntry { Topic = "t", Partition = 1, PartitioningVersion = 1, PartitionCount = 2 },
                },
                Streams = new[] { new StreamTopologyEntry { Topic = "s", Partition = 2, OwnerEndpoints = new[] { new AdvertisedAddress { Host = "10.0.0.9", Port = 7100 } }, PartitioningVersion = 4, PartitionCount = 3 } },
            }), b => WireOps.EncodeTopologyOk(WireOps.DecodeTopologyOk(b))),
            new("topology_req", WireOps.EncodeTopologyRequest(new TopologyRequest { Topic = "t" }),
                b => WireOps.EncodeTopologyRequest(WireOps.DecodeTopologyRequest(b))),
            new("topology_update", WireOps.EncodeTopologyUpdate(new TopologyOk
            {
                Generation = 12,
                Queues = new[] { new QueueTopologyEntry { Topic = "t", Partition = 0, OwnerEndpoints = new[] { new AdvertisedAddress { Host = "127.0.0.1", Port = 7000 } }, PartitioningVersion = 1, PartitionCount = 2 } },
                Streams = new[] { new StreamTopologyEntry { Topic = "s", Partition = 2, OwnerEndpoints = new[] { new AdvertisedAddress { Host = "10.0.0.9", Port = 7100 } }, PartitioningVersion = 4, PartitionCount = 3 } },
            }), b => WireOps.EncodeTopologyUpdate(WireOps.DecodeTopologyUpdate(b))),
            new("topology_update_ack", WireOps.EncodeTopologyUpdateAck(new TopologyUpdateAck(12)),
                b => WireOps.EncodeTopologyUpdateAck(WireOps.DecodeTopologyUpdateAck(b))),
            new("reconcile_client", WireOps.EncodeReconcileClient(new ReconcileClient
            {
                Policy = ReconcilePolicy.Restore,
                Subscriptions = new[] { new ReconcileSubscription { SubId = 1, Topic = "t", Partition = 0, AutoAck = false, Prefetch = 8 } },
            }), b => WireOps.EncodeReconcileClient(WireOps.DecodeReconcileClient(b))),
            new("redirect", WireOps.EncodeRedirect(new Redirect { Topic = "t", Partition = 1, Group = "g", OwnerEndpoints = new[] { new AdvertisedAddress { Host = "h", Port = 1 } }, PartitioningVersion = 3 }),
                b => WireOps.EncodeRedirect(WireOps.DecodeRedirect(b))),
            new("assignment", WireOps.EncodeAssignmentChanged(new AssignmentChanged
            {
                Topic = "t", ConsumerGroup = "cg", Generation = 6, Assigned = new uint[] { 0, 1, 2 }, Added = new uint[] { 2 }, Revoked = Array.Empty<uint>(),
            }), b => WireOps.EncodeAssignmentChanged(WireOps.DecodeAssignmentChanged(b))),
            new("going_away", WireOps.EncodeGoingAway(new GoingAway(30000, "broker restarting for upgrade")),
                b => WireOps.EncodeGoingAway(WireOps.DecodeGoingAway(b))),
            new("reconcile_result", WireOps.EncodeReconcileResult(new ReconcileResult
            {
                Subscriptions = new[]
                {
                    new ReconcileSubscriptionResult
                    {
                        Client = new ReconcileSubscription { SubId = 1, Topic = "t", Partition = 0, AutoAck = false, Prefetch = 8 },
                        Action = ReconcileAction.CloseClientSide, Code = ReasonCode.ServerMissing, Reason = "server_missing",
                    },
                    new ReconcileSubscriptionResult
                    {
                        Server = new ReconcileSubscription { SubId = 2, Topic = "t", Group = "g", Partition = 1, AutoAck = true, Prefetch = 4 },
                        Action = ReconcileAction.CloseServerSide, Code = ReasonCode.ClientMissing, Reason = "client_missing",
                    },
                },
            }), b => WireOps.EncodeReconcileResult(WireOps.DecodeReconcileResult(b))),
            new("subscription_closed", WireOps.EncodeSubscriptionClosed(new SubscriptionClosed(7, ReasonCode.OwnerMoved, "partition moved")),
                b => WireOps.EncodeSubscriptionClosed(WireOps.DecodeSubscriptionClosed(b))),
            new("hello_ok_resumed_after_restart", WireOps.EncodeHelloOk(new HelloOk(1, Uuid.Fill(9), Uuid.Fill(8), Uuid.Fill(7), ResumeOutcome.ResumedAfterRestart, "srv", "v=1;x")),
                b => WireOps.EncodeHelloOk(WireOps.DecodeHelloOk(b))),
            new("subscribe_stream", WireOps.EncodeSubscribeStream(new SubscribeStream
            {
                Topic = "t", Partition = 1, DurableName = "c1", Start = new StreamStart(StreamStartKind.ByTime, 1234),
                Filter = new[] { new StreamFilter("region", "eu-*"), new StreamFilter("kind", "order") }, Prefetch = 16, AutoAck = false,
            }), b => WireOps.EncodeSubscribeStream(WireOps.DecodeSubscribeStream(b))),
            new("subscribe_stream_min", WireOps.EncodeSubscribeStream(new SubscribeStream { Topic = "t", Start = new StreamStart(StreamStartKind.Latest), AutoAck = true }),
                b => WireOps.EncodeSubscribeStream(WireOps.DecodeSubscribeStream(b))),
        };
    }

    private static Dictionary<string, string> LoadVectors()
    {
        // The shared cross-client fixture is copied next to the test assembly.
        var path = Path.Combine(AppContext.BaseDirectory, "wire_vectors.json");
        var raw = File.ReadAllText(path);
        return JsonSerializer.Deserialize<Dictionary<string, string>>(raw)
            ?? throw new InvalidOperationException("wire_vectors.json parsed to null");
    }

    [Fact]
    public void EncodeMatchesSharedVectors()
    {
        var vectors = LoadVectors();
        foreach (var c in Cases())
        {
            Assert.True(vectors.TryGetValue(c.Name, out var want), $"{c.Name}: no shared vector");
            Assert.Equal(want, Hex(c.Encoded));
        }
    }

    [Fact]
    public void DecodeRoundTripsToVector()
    {
        var vectors = LoadVectors();
        foreach (var c in Cases())
        {
            var want = vectors[c.Name];
            var raw = Convert.FromHexString(want);
            var reencoded = c.RoundTrip(raw);
            Assert.Equal(want, Hex(reencoded));
        }
    }

    // Fails if a shared vector has no conformance case, so a new op added to
    // wire_vectors.json cannot silently skip the C# codec.
    [Fact]
    public void AllVectorsCovered()
    {
        var vectors = LoadVectors();
        var covered = Cases().Select(c => c.Name).ToHashSet();
        foreach (var name in vectors.Keys)
        {
            Assert.True(covered.Contains(name), $"shared vector \"{name}\" has no C# conformance case");
        }
    }
}
