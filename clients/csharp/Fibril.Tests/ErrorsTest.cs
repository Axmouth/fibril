using System.Text.Json;
using Fibril;

namespace Fibril.Tests;

public class ErrorsTest
{
    [Theory]
    [InlineData(409, RetryAdvice.Retry)]   // not owner: a retry re-routes
    [InlineData(404, RetryAdvice.DoNotRetry)] // not found: do not retry
    [InlineData(400, RetryAdvice.DoNotRetry)] // invalid: do not retry
    [InlineData(500, RetryAdvice.Retry)]   // server transient
    [InlineData(503, RetryAdvice.Retry)]
    [InlineData(403, RetryAdvice.DoNotRetry)] // other 4xx
    public void ServerErrorRetryClassification(int code, RetryAdvice want)
    {
        var err = new ServerException((ushort)code, "x");
        Assert.Equal(want, Retry.Advise(err));
        Assert.Equal(want == RetryAdvice.Retry, Retry.IsRetryable(err));
    }

    [Fact]
    public void TransportAndRedirectErrorsRetry()
    {
        Assert.True(Retry.IsRetryable(new DisconnectionException("down")));
        Assert.True(Retry.IsRetryable(new BrokenPipeException()));
    }

    [Fact]
    public void LocalErrorsDoNotRetry()
    {
        Assert.False(Retry.IsRetryable(new SerializationException("bad header")));
        Assert.False(Retry.IsRetryable(new UnexpectedException("protocol violation")));
    }

    // A stale delivery redelivers on its own, so retrying the settle is pointless.
    [Fact]
    public void StaleDeliveryDoesNotRetry()
    {
        Assert.Equal(RetryAdvice.DoNotRetry, Retry.Advise(new StaleDeliveryException()));
        Assert.False(Retry.IsRetryable(new StaleDeliveryException()));
    }

    [Fact]
    public void RetryWalksInnerExceptionChain()
    {
        // A disconnection wrapped in a generic exception is still retryable.
        var wrapped = new InvalidOperationException("outer", new DisconnectionException("inner"));
        Assert.True(Retry.IsRetryable(wrapped));
    }

    // The shared clients/error_guides.json is the single source of truth for the
    // wording every client must carry for the errors it raises itself.
    private static List<string> MustContain(string name)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "error_guides.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        var entry = doc.RootElement.GetProperty(name).GetProperty("must_contain");
        return entry.EnumerateArray().Select(e => e.GetString()!).ToList();
    }

    private static void AssertContainsAll(string message, IEnumerable<string> want)
    {
        var lower = message.ToLowerInvariant();
        foreach (var sub in want)
        {
            Assert.Contains(sub.ToLowerInvariant(), lower);
        }
    }

    [Fact]
    public void ConnectionRefusedCarriesGuide()
    {
        var message = new DisconnectionException(Guides.ConnectionRefused("127.0.0.1:9876")).Message;
        AssertContainsAll(message, MustContain("connection_refused"));
    }

    [Fact]
    public void HeartbeatTimeoutCarriesGuide()
    {
        AssertContainsAll(Guides.HeartbeatTimeout, MustContain("heartbeat_timeout"));
    }

    [Fact]
    public void DecodeMalformedBodyCarriesGuide()
    {
        var delivery = new Delivery(
            new Deliver { Payload = System.Text.Encoding.UTF8.GetBytes("not json") },
            0UL, false, null!, 0);
        var ex = Assert.Throws<DeserializationException>(
            () => delivery.Json<Dictionary<string, object>>());
        AssertContainsAll(ex.Message, MustContain("decode_malformed_body"));
    }
}
