using Fibril;

namespace Fibril.Tests;

public class ProtocolTest
{
    [Fact]
    public void ComplianceMarkerIsByteExact()
    {
        Assert.Equal("v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md", Protocol.ComplianceString);
        Assert.Equal(1, Protocol.V1);
    }

    [Fact]
    public void SharedFixturesAreOnDisk()
    {
        // The conformance fixtures must be copied next to the test assembly so the
        // vector-driven tests can pin the codec against the shared cross-client bytes.
        var dir = AppContext.BaseDirectory;
        Assert.True(File.Exists(Path.Combine(dir, "wire_vectors.json")), "wire_vectors.json not copied to test output");
        Assert.True(File.Exists(Path.Combine(dir, "error_guides.json")), "error_guides.json not copied to test output");
    }
}
