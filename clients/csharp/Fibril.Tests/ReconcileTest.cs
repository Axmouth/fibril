using Fibril;

namespace Fibril.Tests;

public class ReconcileTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts() => new()
    {
        ClientName = "csharp-test",
        HeartbeatInterval = TimeSpan.FromHours(1),
        RetryBackoff = TimeSpan.FromMilliseconds(5),
    };

    [Fact]
    public async Task PlainSubscriptionSurvivesReconnectViaReconcile()
    {
        // The broker delivers once then drops. A later routed op reconnects, the client
        // re-announces its remembered subscription (RECONCILE_CLIENT), and the restored
        // sub keeps yielding on the same channel the caller already holds.
        await using var broker = new FakeBroker
        {
            TopologyQueuePartitions = 1,
            PushDeliveryOnSubscribe = true,
            ReconcileOnReconnect = true,
        };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(), Timeout());

        var sub = await client.SubscribeAsync("t", ct: Timeout());
        await using var deliveries = sub.Deliveries(Timeout()).GetAsyncEnumerator();

        // First delivery, then the broker drops the connection.
        Assert.True(await deliveries.MoveNextAsync());
        var first = deliveries.Current.Text();
        deliveries.Current.Complete();

        // A routed op forces the reconnect that carries RECONCILE_CLIENT.
        await client.Publisher("t").PublishConfirmedAsync(Message.Text("trigger"), Timeout());

        // The restored subscription yields the post-reconnect delivery on the same channel.
        Assert.True(await deliveries.MoveNextAsync());
        var second = deliveries.Current.Text();
        deliveries.Current.Complete();

        Assert.Equal("hello", first);
        Assert.Equal("restored", second);
    }
}
