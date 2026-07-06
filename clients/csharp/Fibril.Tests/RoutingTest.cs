using Fibril;

namespace Fibril.Tests;

public class RoutingTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts() => new()
    {
        ClientName = "csharp-test",
        HeartbeatInterval = TimeSpan.FromHours(1),
        RetryBackoff = TimeSpan.FromMilliseconds(5),
    };

    [Fact]
    public async Task FollowsRedirectToOwner()
    {
        // The bootstrap redirects the first publish to a second broker, which owns
        // the partition and confirms it.
        await using var owner = new FakeBroker();
        await using var bootstrap = new FakeBroker { RedirectPublishTo = owner.Address };
        await using var client = await Client.ConnectAsync(bootstrap.Address, Opts(), Timeout());

        var offset = await client.Publisher("orders").PublishConfirmedAsync(Message.Text("x"), Timeout());

        Assert.Equal(FakeBroker.FirstOffset, offset);
        // The owner connection saw the retried publish after the redirect.
        Assert.True(await owner.WaitForAsync(Op.Publish, Timeout()));
    }

    [Fact]
    public async Task KeylessPublishesRoundRobinKeyedPublishesStick()
    {
        // A three-partition topology: keyless publishes spread evenly across the
        // partitions (round-robin), while publishes carrying the same key all land on
        // one partition (FNV-1a of the key). All partitions route to the bootstrap
        // here, so the fake broker records the routed partition of each publish.
        await using var broker = new FakeBroker { TopologyQueuePartitions = 3 };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());
        await client.FetchTopologyAsync("orders", Timeout());

        var publisher = client.Publisher("orders");
        for (var i = 0; i < 6; i++)
        {
            await publisher.PublishConfirmedAsync(Message.Text("x"), Timeout());
        }
        var keyless = broker.Publishes.Select(p => p.Partition).ToList();
        Assert.Equal(new[] { 0u, 1u, 2u }, keyless.Distinct().OrderBy(x => x).ToArray());
        // Six sends over three partitions land two on each (even round-robin spread).
        Assert.All(Enumerable.Range(0, 3), p => Assert.Equal(2, keyless.Count(x => x == (uint)p)));

        broker.Publishes.Clear();
        var key = new byte[] { 7, 7, 7 };
        for (var i = 0; i < 3; i++)
        {
            await publisher.PublishConfirmedAsync(Message.Text("x").Keyed(key), Timeout());
        }
        // Every keyed publish hashes to the same partition.
        Assert.Single(broker.Publishes.Select(p => p.Partition).Distinct());
    }

    [Fact]
    public async Task ReconnectsAfterConnectionDrop()
    {
        // The broker drops the connection after the first confirmed publish. The
        // client must transparently reconnect for the second one.
        await using var broker = new FakeBroker { DropAfterFirstPublish = true };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());
        var publisher = client.Publisher("t");

        var first = await publisher.PublishConfirmedAsync(Message.Text("a"), Timeout());
        Assert.Equal(FakeBroker.FirstOffset, first);

        var second = await publisher.PublishConfirmedAsync(Message.Text("b"), Timeout());
        Assert.Equal(FakeBroker.FirstOffset, second);
    }
}
