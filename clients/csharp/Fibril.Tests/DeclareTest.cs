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

        var outcome = await client.DeclareQueueAsync("orders", new QueueDeclareOptions
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
    }

    [Fact]
    public async Task DeclarePlexusSendsDurabilityAndRetention()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        await client.DeclarePlexusAsync("events", new PlexusDeclareOptions
        {
            PartitionCount = 3,
            Durability = StreamDurabilityTier.Speculative,
            Retention = new StreamRetentionPolicy { MaxAge = TimeSpan.FromMinutes(1), MaxRecords = 1_000_000 },
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
}
