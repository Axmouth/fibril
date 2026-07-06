using Fibril;

namespace Fibril.Tests;

public class PatternTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts() => new()
    {
        ClientName = "csharp-test",
        HeartbeatInterval = TimeSpan.FromHours(1),
        RetryBackoff = TimeSpan.FromMilliseconds(5),
        RepartitionPollInterval = TimeSpan.FromHours(1),
    };

    [Theory]
    [InlineData("orders.*", "orders.eu", true)]
    [InlineData("orders.*", "orders.us", true)]
    [InlineData("orders.*", "billing", false)]
    [InlineData("*", "anything", true)]
    [InlineData("exact", "exact", true)]
    [InlineData("exact", "other", false)]
    [InlineData("a*c", "abc", true)]
    [InlineData("a*c", "ac", true)]
    [InlineData("a*c", "abd", false)]
    [InlineData("*.dlq", "orders.dlq", true)]
    [InlineData("*.dlq", "orders.eu", false)]
    public void GlobMatches(string pattern, string value, bool expected)
    {
        Assert.Equal(expected, new TopicGlob(pattern).Matches(value));
    }

    [Fact]
    public async Task PatternSubscribeFansInMatchingQueues()
    {
        await using var broker = new FakeBroker
        {
            CatalogueTopics = new[] { "orders.eu", "orders.us", "billing" },
            PushDeliveryOnSubscribe = true,
        };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        await using var sub = await client.Routing.SubscribePatternAsync("orders.*", new PatternSubscribeOptions { Prefetch = 4 }, Timeout());

        var topics = new HashSet<string>();
        await foreach (var msg in sub.Deliveries(Timeout()))
        {
            topics.Add(msg.Topic);
            msg.Ack();
            if (topics.Count == 2)
            {
                break;
            }
        }
        Assert.Equal(new HashSet<string> { "orders.eu", "orders.us" }, topics);
    }

    [Fact]
    public async Task CatalogueReflectsDeclaredChannels()
    {
        await using var broker = new FakeBroker { CatalogueTopics = new[] { "a", "b" } };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        await client.FetchTopologyAsync(ct: Timeout());
        var catalogue = client.Routing.Catalogue();

        Assert.Equal(new[] { "a", "b" }, catalogue.Queues.Select(q => q.Topic).ToArray());
    }
}
