using Fibril.Examples;

// Dispatches to one self-validating example by name, exiting non-zero on failure so
// run-all.sh can treat each as a light integration test.
//
//   FIBRIL_ADDR=127.0.0.1:9876 dotnet run --project Fibril.Examples -- roundtrip

var examples = new Dictionary<string, Func<Task>>(StringComparer.Ordinal)
{
    ["roundtrip"] = Examples.RoundtripAsync,
    ["confirmed-delayed"] = Examples.ConfirmedDelayedAsync,
    ["manual-ack-retry"] = Examples.ManualAckRetryAsync,
    ["stream"] = Examples.StreamAsync,
};

var name = args.FirstOrDefault(a => !a.StartsWith("--", StringComparison.Ordinal));
if (name is null || !examples.TryGetValue(name, out var run))
{
    Console.Error.WriteLine("usage: fibril-examples <" + string.Join("|", examples.Keys) + "> [--check]");
    return 2;
}

try
{
    await run();
    Console.WriteLine($"[{name}] OK");
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"[{name}] FAILED: {ex.Message}");
    return 1;
}
