# Fibril .NET client

C# client for the [Fibril](https://fibril.sh) message broker. It follows the
Rust reference client (`crates/client`) where that fits .NET, and uses .NET
idioms where they read better: `async` methods with `CancellationToken`,
`IAsyncEnumerable` for deliveries, and `IAsyncDisposable` for clients and
subscriptions.

## Status

Pre-alpha and not yet published. The API tracks the source tree.

Until the package is on NuGet, reference the project from a checkout:

```xml
<ProjectReference Include="path/to/fibril/clients/csharp/Fibril/Fibril.csproj" />
```

Once published, it will install with:

```sh
dotnet add package Fibril
```

The library targets .NET 8, so .NET 8 and newer apps can use it. There are no
third-party dependencies.

## Quick start

```csharp
using Fibril;

await using var client = await Client.ConnectAsync("localhost:9876", new ClientOptions
{
    ClientName = "orders-worker",
    Credentials = new Credentials("user", "pass"),
});

// Declare a queue. Only the first declare sets its options.
await client.DeclareQueueAsync("orders", new QueueConfig { Durability = QueueDurability.Durable });

// Subscribe with manual ack across every partition of the topic.
await using var sub = await client.SubscribeTopicAsync("orders", prefetch: 100);

// Publish and wait for the broker to confirm the offset.
var offset = await client.Publisher("orders").PublishConfirmedAsync(Message.Json(new Order(42, "widget")));
Console.WriteLine($"published at offset {offset}");

await foreach (var delivery in sub.Deliveries())
{
    var order = delivery.Json<Order>();
    if (order is null)
    {
        delivery.Fail(); // not requeued: dead-lettered or dropped per queue policy
        continue;
    }
    try
    {
        await ProcessAsync(order);
        delivery.Complete();
    }
    catch
    {
        delivery.Retry(); // requeue for redelivery
    }
}

static Task ProcessAsync(Order order) => Task.CompletedTask;

record Order(int Id, string Item);
```

Pass `autoAck: true` to `SubscribeTopicAsync` when deliveries need no
settlement. `RetryAfter(delay)` requeues a delivery no sooner than `delay` from
now.

## Queue durability

`QueueConfig.Durability` picks how a queue confirms and delivers.
`QueueDurability.Durable` waits until each required copy has synced a message to
disk. `QueueDurability.Buffered` waits only until each required copy has written
it to the OS, before fsync, so power loss or a storage failure on every required
node at once can lose recent confirmed messages. Leaving it null keeps the
broker default.

Only the first declare of a queue sets its mode. A later declare naming another
mode fails with error code 409.

## Layout

- `Fibril/` is the library, packed as the `Fibril` NuGet package.
- `Fibril.Tests/` holds the xunit tests.
- `Fibril.Examples/` holds runnable examples. Each connects to a real broker,
  exercises one feature, and exits non-zero on failure. `Fibril.Examples/run-all.sh`
  starts a broker, runs them all, and tears it down.
- `Fibril.Bench/` holds load generators.

## Development

```sh
cd clients/csharp
dotnet build
dotnet test
```

## More

- [Client architecture](https://github.com/Axmouth/fibril/blob/main/clients/ARCHITECTURE.md), the layering and invariants shared by every client
- [Feature matrix](https://github.com/Axmouth/fibril/blob/main/clients/FEATURE_MATRIX.md), what each client supports
- [Documentation](https://fibril.sh)

## License

MIT. See [AI_POLICY.md](https://github.com/Axmouth/fibril/blob/main/AI_POLICY.md)
for the text and data mining reservation that comes with this package.
