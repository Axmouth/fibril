using Fibril;

namespace Fibril.Tests;

public class DeclareTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts() => new() { ClientName = "t", HeartbeatInterval = TimeSpan.FromHours(1) };

    [Fact]
    public async Task DeclareQueueSendsDlqAndTtlOptions()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        var outcome = await client.DeclareQueueAsync("orders", new QueueConfig
        {
            PartitionCount = 4,
            DeadLetter = DeadLetterPolicy.ToTopic("orders.dlq"),
            MaxRetries = 5,
            DefaultMessageTtl = TimeSpan.FromSeconds(30),
        }, Timeout());

        Assert.Equal("created", outcome.Status);
        var declare = broker.QueueDeclares.Single();
        Assert.Equal("orders", declare.Topic);
        Assert.Equal(4u, declare.PartitionCount);
        Assert.Equal(5u, declare.DlqMaxRetries);
        Assert.Equal(30000ul, declare.DefaultMessageTtlMs);
        Assert.Equal(DlqKind.Custom, declare.DlqPolicy!.Value.Kind);
        Assert.Equal("orders.dlq", declare.DlqPolicy.Value.Topic);
        Assert.Null(declare.Durability);
        Assert.Null(outcome.Durability);
        Assert.Empty(outcome.Warnings);
    }

    [Fact]
    public async Task DeclareQueueSendsModeAndReturnsModeAndWarnings()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        var buffered = await client.DeclareQueueAsync("jobs", new QueueConfig { Durability = QueueDurability.Buffered }, Timeout());
        Assert.Equal(QueueDurability.Buffered, buffered.Durability);
        Assert.Equal(["buffered queue can lose recent confirmed messages"], buffered.Warnings);

        var durable = await client.DeclareQueueAsync("audit", new QueueConfig { Durability = QueueDurability.Durable }, Timeout());
        Assert.Equal(QueueDurability.Durable, durable.Durability);
        Assert.Empty(durable.Warnings);

        Assert.Equal(
            [QueueDurability.Buffered, QueueDurability.Durable],
            broker.QueueDeclares.Select(d => d.Durability!.Value).ToArray());
    }

    [Fact]
    public async Task DeclarePlexusSendsDurabilityAndRetention()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        await client.DeclarePlexusAsync("events", new StreamConfig
        {
            PartitionCount = 3,
            Durability = StreamDurabilityTier.Speculative,
            Retention = new StreamRetentionPolicy { MaxAge = TimeSpan.FromMinutes(1), RetainRecords = 1_000_000 },
            ReplicationFactor = 2,
        }, Timeout());

        var declare = broker.PlexusDeclares.Single();
        Assert.Equal("events", declare.Topic);
        Assert.Equal(3u, declare.PartitionCount);
        Assert.Equal(StreamDurability.Speculative, declare.Durability);
        Assert.Equal(60000ul, declare.Retention.MaxAgeMs);
        Assert.Equal(1_000_000ul, declare.Retention.MaxRecords);
        Assert.Equal(2u, declare.ReplicationFactor);
    }

    private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    // declare_min from the shared vectors: topic "t", every optional unset.
    private const string DeclareMin = "4644513100000001740000000000";

    // FDK2 head: status "stored", 2 partitions. The mode and warnings follow.
    private const string Fdk2Head = "46444b320000000673746f72656400000002";

    [Fact]
    public void DeclareWithoutModeIsUnchanged()
    {
        var body = WireOps.EncodeDeclareQueue(new DeclareQueueFrame { Topic = "t" });
        Assert.Equal(DeclareMin, Hex(body));
        Assert.Null(WireOps.DecodeDeclareQueue(body).Durability);
    }

    [Theory]
    [InlineData(QueueDurability.Durable, "00")]
    [InlineData(QueueDurability.Buffered, "01")]
    public void DeclareModeIsOneTrailingByte(QueueDurability mode, string tag)
    {
        var body = WireOps.EncodeDeclareQueue(new DeclareQueueFrame { Topic = "t", Durability = mode });
        // The ttl optional is still written before the mode byte.
        Assert.Equal(DeclareMin + tag, Hex(body));
        Assert.Equal(mode, WireOps.DecodeDeclareQueue(body).Durability);
    }

    [Theory]
    [InlineData(DeclareMin + "02", WireErrorKind.UnknownTag)]
    [InlineData(DeclareMin + "0100", WireErrorKind.TrailingBytes)]
    public void DeclareRejectsBadMode(string hex, WireErrorKind kind)
    {
        var ex = Assert.Throws<WireException>(() => WireOps.DecodeDeclareQueue(Convert.FromHexString(hex)));
        Assert.Equal(kind, ex.Kind);
    }

    [Fact]
    public void DeclareOkDecodesFdk1()
    {
        const string fdk1 = "46444b31000000076372656174656400000004";
        var ok = WireOps.DecodeDeclareQueueOk(Convert.FromHexString(fdk1));
        Assert.Equal("created", ok.Status);
        Assert.Equal(4u, ok.PartitionCount);
        Assert.Null(ok.Durability);
        Assert.Empty(ok.Warnings);
        Assert.Equal(fdk1, Hex(WireOps.EncodeDeclareQueueOk(ok)));
    }

    [Fact]
    public void DeclareOkDecodesFdk2WithModeAndWarnings()
    {
        var fdk2 = Fdk2Head + "0101" + "00000001" + "0000000177";
        var ok = WireOps.DecodeDeclareQueueOk(Convert.FromHexString(fdk2));
        Assert.Equal("stored", ok.Status);
        Assert.Equal(2u, ok.PartitionCount);
        Assert.Equal(QueueDurability.Buffered, ok.Durability);
        Assert.Equal(["w"], ok.Warnings);
        Assert.Equal(fdk2, Hex(WireOps.EncodeDeclareQueueOk(ok)));

        var bare = WireOps.DecodeDeclareQueueOk(Convert.FromHexString(Fdk2Head + "00" + "00000000"));
        Assert.Null(bare.Durability);
        Assert.Empty(bare.Warnings);
    }

    [Theory]
    [InlineData(Fdk2Head + "0107" + "00000000", WireErrorKind.UnknownTag)]
    [InlineData(Fdk2Head + "00" + "00000000" + "00", WireErrorKind.TrailingBytes)]
    [InlineData(Fdk2Head + "00" + "ffffffff", WireErrorKind.UnexpectedEof)]
    public void DeclareOkRejectsBadFdk2(string hex, WireErrorKind kind)
    {
        var ex = Assert.Throws<WireException>(() => WireOps.DecodeDeclareQueueOk(Convert.FromHexString(hex)));
        Assert.Equal(kind, ex.Kind);
    }
}
