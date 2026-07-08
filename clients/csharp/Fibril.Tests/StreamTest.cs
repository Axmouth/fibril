using Fibril;

namespace Fibril.Tests;

public class StreamTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts() => new()
    {
        ClientName = "csharp-test",
        HeartbeatInterval = TimeSpan.FromHours(1),
        RetryBackoff = TimeSpan.FromMilliseconds(5),
        RepartitionPollInterval = TimeSpan.FromHours(1),
    };

    [Fact]
    public async Task StreamFanInReadsEveryPartition()
    {
        await using var broker = new FakeBroker { TopologyStreamPartitions = 3, PushDeliveryOnSubscribe = true };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        await using var fan = await client.SubscribeStreamTopicAsync("events", new StreamSubscribeOptions
        {
            Start = StreamStartPosition.Earliest,
            Prefetch = 4,
        }, Timeout());

        var received = 0;
        await foreach (var record in fan.Deliveries(Timeout()))
        {
            Assert.Equal("record", record.Text());
            record.Complete();
            if (++received == 3)
            {
                break;
            }
        }
        Assert.Equal(3, received);
    }

    [Fact]
    public async Task SingleStreamPartitionSubscribes()
    {
        await using var broker = new FakeBroker { TopologyStreamPartitions = 1, PushDeliveryOnSubscribe = true };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        var sub = await client.SubscribeStreamAsync("events", 0, new StreamSubscribeOptions
        {
            Start = StreamStartPosition.FromTime(DateTimeOffset.UtcNow),
            Filter = new[] { new StreamHeaderFilter("region", "eu-*") },
        }, Timeout());

        await foreach (var record in sub.Deliveries(Timeout()))
        {
            Assert.Equal("record", record.Text());
            break;
        }
    }
}
