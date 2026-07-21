using Fibril;

namespace Fibril.Tests;

public class ClientTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    [Fact]
    public async Task PublisherConfirmsThroughPublicApi()
    {
        await using var broker = new FakeBroker();
        await using var client = await Client.ConnectAsync(broker.Address, new ClientOptions { ClientName = "t", HeartbeatInterval = TimeSpan.FromHours(1) }, Timeout());

        var publisher = client.Publisher("email.send");
        var offset = await publisher.PublishConfirmedAsync(Message.Text("hello"), Timeout());
        Assert.Equal(FakeBroker.FirstOffset, offset);
    }

    [Fact]
    public async Task SubscribeYieldsDeliveries()
    {
        await using var broker = new FakeBroker { PushDeliveryOnSubscribe = true };
        await using var client = await Client.ConnectAsync(broker.Address, new ClientOptions { HeartbeatInterval = TimeSpan.FromHours(1) }, Timeout());

        var sub = await client.SubscribeAsync("email.send", prefetch: 8, ct: Timeout());
        await foreach (var msg in sub.Deliveries(Timeout()))
        {
            Assert.Equal("hello", msg.Text());
            msg.Complete();
            break;
        }
    }

    [Fact]
    public void ReservedHeaderRejectedAtPublishTime()
    {
        // The fluent chain stays exception-free; lowering to the wire validates.
        var message = Message.Text("x").WithHeader("fibril.client.producer_id", "p");
        Assert.Throws<SerializationException>(() => message.ToPublish("t", null));
    }

    [Fact]
    public void JsonRoundTripsThroughContentType()
    {
        var message = Message.Json(new Point { X = 1, Y = 2 });
        Assert.Equal(ContentKind.Json, message.ContentType.Kind);

        // A delivery carrying those bytes decodes back to the record.
        var deliver = new Deliver { Topic = "t", Payload = message.Payload, ContentType = message.ContentType };
        var delivery = new Delivery(deliver, 0, false, null!, 0);
        var point = delivery.Json<Point>();
        Assert.Equal(1, point!.X);
        Assert.Equal(2, point.Y);
    }

    private sealed record Point
    {
        public int X { get; init; }
        public int Y { get; init; }
    }
}
