using Fibril;

namespace Fibril.Tests;

public class PublisherTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts() => new() { ClientName = "t", HeartbeatInterval = TimeSpan.FromHours(1) };

    [Fact]
    public async Task ReliablePublisherStampsProducerIdAndSequence()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        var publisher = client.ReliablePublisher("orders");
        await publisher.PublishConfirmedAsync(Message.Text("a"), Timeout());
        await publisher.PublishConfirmedAsync(Message.Text("b"), Timeout());

        var published = broker.Publishes.ToList();
        Assert.Equal(2, published.Count);

        var id0 = published[0].Headers!["fibril.client.producer_id"];
        var id1 = published[1].Headers!["fibril.client.producer_id"];
        Assert.Equal(id0, id1); // stable producer id across messages
        Assert.Equal(32, id0.Length); // 16 random bytes, hex

        Assert.Equal("0", published[0].Headers!["fibril.client.producer_seq"]);
        Assert.Equal("1", published[1].Headers!["fibril.client.producer_seq"]);
    }

    [Fact]
    public async Task ExpiringPublisherStampsDefaultTtl()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        var publisher = client.Publisher("orders").Expiring(TimeSpan.FromSeconds(30));
        await publisher.PublishConfirmedAsync(Message.Text("a"), Timeout());

        var published = broker.Publishes.Single();
        Assert.Equal(30000ul, published.TtlMs);
    }

    [Fact]
    public async Task GroupedPublisherRoutesWithGroup()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        var publisher = client.PublisherGrouped("orders", "eu");
        await publisher.PublishConfirmedAsync(Message.Text("a"), Timeout());

        Assert.Equal("eu", broker.Publishes.Single().Group);
    }
}
