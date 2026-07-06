using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Fibril;

namespace Fibril.Tests;

public class TlsTest
{
    private static CancellationToken Timeout(int seconds = 5) => new CancellationTokenSource(TimeSpan.FromSeconds(seconds)).Token;

    private static ClientOptions Opts(TlsOptions tls) => new()
    {
        ClientName = "csharp-test",
        HeartbeatInterval = TimeSpan.FromHours(1),
        Tls = tls,
    };

    private static X509Certificate2 SelfSigned()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=localhost", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        // Re-import through PKCS#12 so the private key is usable server-side on any platform.
        return X509CertificateLoader.LoadPkcs12(cert.Export(X509ContentType.Pfx), null);
    }

    private static string Fingerprint(X509Certificate2 cert) => Convert.ToHexString(SHA256.HashData(cert.RawData));

    [Fact]
    public async Task PinnedFingerprintConnectsAndPublishes()
    {
        using var cert = SelfSigned();
        await using var broker = new FakeBroker { ServerCertificate = cert };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(new TlsOptions { CaFingerprint = Fingerprint(cert) }), Timeout());

        var offset = await client.Publisher("t").PublishConfirmedAsync(Message.Text("hello"), Timeout());
        Assert.Equal(FakeBroker.FirstOffset, offset);
    }

    [Fact]
    public async Task WrongPinIsRejectedAsUntrusted()
    {
        using var cert = SelfSigned();
        await using var broker = new FakeBroker { ServerCertificate = cert };
        var wrongPin = new string('0', 64);

        await Assert.ThrowsAsync<TlsCertificateUntrustedException>(
            () => Client.ConnectAsync(broker.Address, Opts(new TlsOptions { CaFingerprint = wrongPin }), Timeout()));
    }

    [Fact]
    public async Task TlsToPlaintextBrokerReportsNotSupported()
    {
        // A listener that accepts then immediately closes, the way a plaintext broker
        // looks to a TLS handshake: the ServerHello never arrives and the read hits EOF.
        var listener = new System.Net.Sockets.TcpListener(System.Net.IPAddress.Loopback, 0);
        listener.Start();
        var addr = ((System.Net.IPEndPoint)listener.LocalEndpoint).ToString();
        var accept = Task.Run(async () =>
        {
            try
            {
                using var c = await listener.AcceptTcpClientAsync();
            }
            catch
            {
            }
        });

        try
        {
            await Assert.ThrowsAsync<TlsNotSupportedByBrokerException>(
                () => Client.ConnectAsync(addr, Opts(new TlsOptions { CaFingerprint = new string('a', 64) }), Timeout()));
        }
        finally
        {
            listener.Stop();
            await accept;
        }
    }

    [Fact]
    public async Task MalformedFingerprintIsAConfigError()
    {
        await using var broker = new FakeBroker();
        await Assert.ThrowsAsync<TlsConfigException>(
            () => Client.ConnectAsync(broker.Address, Opts(new TlsOptions { CaFingerprint = "not-hex" }), Timeout()));
    }

    [Fact]
    public async Task HalfSetClientCertIsAConfigError()
    {
        await using var broker = new FakeBroker();
        await Assert.ThrowsAsync<TlsConfigException>(
            () => Client.ConnectAsync(broker.Address, Opts(new TlsOptions { CaFingerprint = new string('a', 64), ClientCertFile = "cert.pem" }), Timeout()));
    }
}
