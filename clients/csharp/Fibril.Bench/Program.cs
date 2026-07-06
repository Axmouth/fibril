using System.Diagnostics;
using Fibril;

// Plexus (fan-out stream) throughput benchmark, mirroring the Go/Python/TS
// bench-plexus. A saturating publisher feeds a stream and CONSUMERS independent
// stream subscribers each receive every record (fan-out), so deliver throughput
// is aggregated across all of them. Run together (MODE=both) or alone
// (MODE=pub / MODE=sub) so producer and consumers can run as separate processes.
//
// Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE, DURATION_S, WARMUP_S, TOPIC,
// PARTITIONS, PREFETCH, MODE (both|pub|sub), CONSUMERS, DURABILITY
// (ephemeral|speculative|durable).

static string Env(string key, string def) => Environment.GetEnvironmentVariable(key) is { Length: > 0 } v ? v : def;
static double EnvNum(string key, double def) => double.TryParse(Environment.GetEnvironmentVariable(key), out var n) ? n : def;

var addr = Env("FIBRIL_ADDR", "127.0.0.1:9876");
var size = (int)EnvNum("SIZE", 1024);
var duration = TimeSpan.FromSeconds(EnvNum("DURATION_S", 8));
var warmup = TimeSpan.FromSeconds(EnvNum("WARMUP_S", 2));
var topic = Env("TOPIC", "benchplexus");
var partitions = (int)EnvNum("PARTITIONS", 1);
var prefetch = (uint)EnvNum("PREFETCH", 4096);
var mode = Env("MODE", "both");
var consumers = (int)EnvNum("CONSUMERS", 1);
var durability = Enum.Parse<StreamDurabilityTier>(Env("DURABILITY", "durable"), ignoreCase: true);
var kind = Env("KIND", "stream"); // stream = Plexus fan-out, queue = work queue (consumed once)
var isQueue = kind == "queue";
var doPub = mode is "both" or "pub";
var doSub = mode is "both" or "sub";

async Task<Client> DialAsync()
{
    try
    {
        return await Client.ConnectAsync(addr, new ClientOptions
        {
            ClientName = "bench",
            Auth = new Credentials(Env("FIBRIL_USER", "fibril"), Env("FIBRIL_PASS", "fibril")),
        }, new CancellationTokenSource(TimeSpan.FromSeconds(10)).Token);
    }
    catch (Exception ex)
    {
        Console.WriteLine("connect: " + ex.Message);
        Environment.Exit(1);
        throw;
    }
}

// Declare the stream once up front (on a throwaway connection) so both roles can
// run as separate processes.
var admin = await DialAsync();
if (isQueue)
{
    await admin.DeclareQueueAsync(topic, partitions);
}
else
{
    await admin.DeclarePlexusAsync(topic, new PlexusDeclareOptions { PartitionCount = partitions, Durability = durability });
}
await admin.DisposeAsync();

long published = 0, delivered = 0;
var running = true;
var clients = new List<Client>();

if (doSub)
{
    for (var i = 0; i < consumers; i++)
    {
        var consumer = await DialAsync();
        clients.Add(consumer);
        var fan = isQueue
            ? await consumer.SubscribeTopicAsync(topic, prefetch: prefetch, autoAck: true)
            : await consumer.SubscribeStreamTopicAsync(topic, new StreamSubscribeOptions
            {
                Start = StreamStartPosition.Latest,
                Prefetch = prefetch,
                AutoAck = true,
            });
        _ = Task.Run(async () =>
        {
            await foreach (var _ in fan.Deliveries())
            {
                Interlocked.Increment(ref delivered);
            }
        });
    }
}

if (doPub)
{
    var producer = await DialAsync();
    clients.Add(producer);
    var pub = producer.Publisher(topic);
    var payload = new byte[size];
    _ = Task.Run(async () =>
    {
        while (Volatile.Read(ref running))
        {
            await pub.PublishAsync(Message.Raw(payload));
            Interlocked.Increment(ref published);
        }
    });
}

await Task.Delay(warmup);
var p0 = Interlocked.Read(ref published);
var d0 = Interlocked.Read(ref delivered);
var sw = Stopwatch.StartNew();
await Task.Delay(duration);
var elapsed = sw.Elapsed.TotalSeconds;
var p1 = Interlocked.Read(ref published);
var d1 = Interlocked.Read(ref delivered);
Volatile.Write(ref running, false);

var outLine = $"mode={mode} kind={kind} durability={durability.ToString().ToLowerInvariant()}";
if (doPub)
{
    outLine += $"  publish={(p1 - p0) / elapsed:F0}";
}
if (doSub)
{
    var rate = (d1 - d0) / elapsed;
    outLine += $"  deliver={rate:F0} (fanout={consumers}, per_consumer={rate / consumers:F0})";
}
outLine += $"  msgs/s size={size} elapsed={elapsed:F1}s";
Console.WriteLine(outLine);

foreach (var c in clients)
{
    await c.DisposeAsync();
}
