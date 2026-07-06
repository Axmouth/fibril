using Fibril;

namespace Fibril.Examples;

internal sealed record Payload(string Hello, int N);

// Each example is a small self-validating end-to-end exchange against a real broker.
internal static class Examples
{
    // Publish one JSON message and receive it back, verifying the payload round-trips.
    public static async Task RoundtripAsync()
    {
        await using var client = await Harness.ConnectAsync("example-roundtrip");
        var topic = Harness.UniqueTopic("roundtrip");

        await using var fan = await client.SubscribeTopicAsync(topic, prefetch: 1, autoAck: true, ct: Harness.Timeout(10));

        var offset = await client.Publisher(topic).PublishConfirmedAsync(Message.Json(new Payload("world", 42)), Harness.Timeout(10));
        Harness.Check(offset >= 0, "confirmed publish returns a broker offset");

        var delivery = await Harness.RecvAsync(fan.Deliveries(), TimeSpan.FromSeconds(5), "the round-trip message");
        var got = delivery.Json<Payload>();
        Harness.AssertEq(got!.Hello, "world", "payload.hello");
        Harness.AssertEq(got.N, 42, "payload.n");
    }

    // Publish a delayed message and confirm it arrives only after the delay window opens.
    public static async Task ConfirmedDelayedAsync()
    {
        await using var client = await Harness.ConnectAsync("example-confirmed-delayed");
        var topic = Harness.UniqueTopic("confirmed-delayed");

        await using var fan = await client.SubscribeTopicAsync(topic, prefetch: 1, autoAck: true, ct: Harness.Timeout(10));

        var offset = await client.Publisher(topic).PublishDelayedConfirmedAsync(Message.Text("later"), TimeSpan.FromMilliseconds(500), Harness.Timeout(10));
        Harness.Check(offset >= 0, "delayed confirmed publish returns an offset");

        var delivery = await Harness.RecvAsync(fan.Deliveries(), TimeSpan.FromSeconds(5), "the delayed message");
        Harness.AssertEq(delivery.Text(), "later", "delayed payload");
    }

    // Publish one task, nack it for retry, receive the redelivery, then ack it.
    public static async Task ManualAckRetryAsync()
    {
        await using var client = await Harness.ConnectAsync("example-manual-ack");
        var topic = Harness.UniqueTopic("manual-ack");
        await client.DeclareQueueAsync(topic, partitionCount: 1, ct: Harness.Timeout(10));

        var sub = await client.SubscribeAsync(topic, prefetch: 8, autoAck: false, ct: Harness.Timeout(10));
        await using var deliveries = sub.Deliveries(Harness.Timeout(15)).GetAsyncEnumerator();

        await client.Publisher(topic).PublishConfirmedAsync(Message.Text("task"), Harness.Timeout(10));

        Harness.Check(await deliveries.MoveNextAsync(), "first delivery");
        Harness.AssertEq(deliveries.Current.Text(), "task", "task payload");
        deliveries.Current.Retry(); // requeue for redelivery

        Harness.Check(await deliveries.MoveNextAsync(), "redelivery after retry");
        Harness.AssertEq(deliveries.Current.Text(), "task", "redelivered task payload");
        deliveries.Current.Ack();
    }

    // Declare a Plexus stream, subscribe, publish a record, and read it back.
    public static async Task StreamAsync()
    {
        await using var client = await Harness.ConnectAsync("example-stream");
        var topic = Harness.UniqueTopic("stream");
        await client.DeclarePlexusAsync(topic, partitionCount: 1, ct: Harness.Timeout(10));

        await using var fan = await client.SubscribeStreamTopicAsync(topic, new StreamSubscribeOptions
        {
            Start = StreamStartPosition.Latest,
            AutoAck = true,
        }, Harness.Timeout(10));

        await client.Publisher(topic).PublishConfirmedAsync(Message.Text("record"), Harness.Timeout(10));

        var record = await Harness.RecvAsync(fan.Deliveries(), TimeSpan.FromSeconds(5), "the stream record");
        Harness.AssertEq(record.Text(), "record", "stream record payload");
    }

    // Demo (not self-validating): pattern (discovery) subscribe. Fan in across every
    // work queue whose topic matches a glob, with new matching queues attaching
    // automatically. Discovery is driven by cluster topology, so this needs a
    // multi-node broker that advertises topology (a single-node broker advertises
    // none). Runs until interrupted, so it is excluded from run-all; pattern subscribe
    // is validated in CI by the fake-broker PatternTest instead.
    public static async Task PatternAsync()
    {
        await using var client = await Harness.ConnectAsync("example-pattern");
        await using var sub = await client.Routing.SubscribePatternAsync(
            "events.*",
            new PatternSubscribeOptions { Prefetch = 16, AutoAck = true });

        Console.WriteLine("listening for events.* (new matching queues attach automatically; Ctrl-C to stop)");
        await foreach (var d in sub.Deliveries())
        {
            Console.WriteLine($"{d.Topic}: {d.Text()}");
        }
    }
}
