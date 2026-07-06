using Fibril;

namespace Fibril.Examples;

// Shared helpers for the self-validating examples. Each example connects to a real
// broker (FIBRIL_ADDR, default 127.0.0.1:9876), does a small end-to-end exchange,
// and asserts the outcome, so it doubles as a light integration test.
internal static class Harness
{
    public static string Address => Environment.GetEnvironmentVariable("FIBRIL_ADDR") ?? "127.0.0.1:9876";

    public static CancellationToken Timeout(int seconds) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    // The built-in fibril/fibril credentials are accepted from loopback only, which
    // is where the examples connect.
    public static Task<Client> ConnectAsync(string name) => Client.ConnectAsync(Address, new ClientOptions
    {
        ClientName = name,
        Auth = new Credentials("fibril", "fibril"),
    }, Timeout(10));

    public static string UniqueTopic(string prefix) => $"{prefix}.{Guid.NewGuid():N}";

    public static void Check(bool condition, string what)
    {
        if (!condition)
        {
            throw new Exception("check failed: " + what);
        }
    }

    public static void AssertEq<T>(T got, T want, string what)
    {
        if (!Equals(got, want))
        {
            throw new Exception($"{what}: got {got}, want {want}");
        }
    }

    // Reads one delivery from a merged stream, failing if none arrives within the timeout.
    public static async Task<Delivery> RecvAsync(IAsyncEnumerable<Delivery> deliveries, TimeSpan timeout, string what)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await foreach (var d in deliveries.WithCancellation(cts.Token))
            {
                return d;
            }
        }
        catch (OperationCanceledException)
        {
        }
        throw new Exception("timed out waiting for " + what);
    }
}
