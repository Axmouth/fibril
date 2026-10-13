using Fibril;

namespace Fibril.Tests;

public class FanInTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts() => new()
    {
        ClientName = "csharp-test",
        HeartbeatInterval = TimeSpan.FromHours(1),
        RetryBackoff = TimeSpan.FromMilliseconds(5),
        RepartitionPollInterval = TimeSpan.FromHours(1), // no growth churn during the test
    };

    [Fact]
    public async Task FansInEveryPartition()
    {
        // A topic with three partitions: the fan-in subscribes all three and merges
        // one delivery from each.
        await using var broker = new FakeBroker { TopologyQueuePartitions = 3, PushDeliveryOnSubscribe = true };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        await using var fan = await client.SubscribeTopicAsync("orders", prefetch: 4, ct: Timeout());

        var received = 0;
        var ct = Timeout();
        await foreach (var msg in fan.Deliveries(ct))
        {
            Assert.Equal("hello", msg.Text());
            msg.Complete();
            if (++received == 3)
            {
                break;
            }
        }
        Assert.Equal(3, received);
    }

    [Fact]
    public async Task TerminalPartitionCloseEndsTheFanIn()
    {
        // Mirrors the Rust client's supervised_subscription_surfaces_a_terminal_close.
        // With the growth loop running and another partition still live, a terminal
        // close of one partition ends the whole fan-in with its typed reason instead
        // of leaving it open and silent.
        await using var broker = new FakeBroker { TopologyQueuePartitions = 2, PushCloseOnSubscribe = true, PushClosePartition = 0 };
        await using var client = await Client.ConnectAsync(broker.Address,
            Opts() with { RepartitionPollInterval = TimeSpan.FromMilliseconds(20) }, Timeout());

        await using var fan = await client.SubscribeTopicAsync("jobs", prefetch: 4, ct: Timeout());

        var ex = await Assert.ThrowsAsync<SubscriptionClosedException>(async () =>
        {
            await foreach (var msg in fan.Deliveries(Timeout()))
            {
                msg.Complete();
            }
        });
        Assert.Equal(ReasonCode.TopicDeleted, ex.Code);
    }

    [Fact]
    public async Task FailedResubscribeSurfacesTypedReason()
    {
        // The connection drops and the re-subscribe is rejected for good. The
        // supervised subscription ends with a typed server error instead of looking
        // like a clean local close.
        await using var broker = new FakeBroker { RejectResubscribe = true };
        await using var client = await Client.ConnectAsync(broker.Address,
            Opts() with { SuperviseBackoff = TimeSpan.FromMilliseconds(20) }, Timeout());

        await using var sub = await client.SuperviseSubscribeAsync("jobs", prefetch: 4, ct: Timeout());

        var ex = await Assert.ThrowsAsync<SubscriptionClosedException>(async () =>
        {
            await foreach (var msg in sub.Deliveries(Timeout()))
            {
                msg.Complete();
            }
        });
        Assert.Equal(ReasonCode.ServerError, ex.Code);
    }

    [Fact]
    public async Task ExclusiveCohortCarriesMemberIdAcrossPartitions()
    {
        // Two partitions joined as the default exclusive cohort. The broker mints a
        // member id on the first subscribe; the client must present that same id on
        // the sibling partition's subscribe.
        await using var broker = new FakeBroker { TopologyQueuePartitions = 2, PushDeliveryOnSubscribe = true };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        await using var fan = await client.SubscribeTopicExclusiveAsync("orders", prefetch: 4, ct: Timeout());

        // Drain both partitions so both subscribes have certainly happened.
        var received = 0;
        await foreach (var msg in fan.Deliveries(Timeout()))
        {
            msg.Complete();
            if (++received == 2)
            {
                break;
            }
        }

        var subscribes = broker.Subscribes.ToList();
        Assert.Equal(2, subscribes.Count);
        Assert.All(subscribes, s => Assert.Equal(Client.DefaultCohortId, s.ConsumerGroup));
        // The first subscribe carries no member id; every later one carries the minted id.
        Assert.Null(subscribes[0].MemberId);
        Assert.Equal(FakeBroker.CohortMemberId, subscribes[1].MemberId);
    }
}
