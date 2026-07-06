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
