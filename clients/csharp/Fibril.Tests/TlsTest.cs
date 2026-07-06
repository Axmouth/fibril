using System.IO;
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

    // Writes a fresh self-signed client certificate and its key to temp PEM files,
    // whose paths the client feeds to ClientCertFile / ClientKeyFile.
    private static (string CertPath, string KeyPath) ClientCertPem()
    {
        using var rsa = RSA.Create(2048);
        var req = new CertificateRequest("CN=fibril-client", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        using var cert = req.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var dir = Directory.CreateTempSubdirectory().FullName;
        var certPath = Path.Combine(dir, "client.crt");
        var keyPath = Path.Combine(dir, "client.key");
        File.WriteAllText(certPath, cert.ExportCertificatePem());
        File.WriteAllText(keyPath, rsa.ExportPkcs8PrivateKeyPem());
        return (certPath, keyPath);
    }

    // A self-signed CA plus a server certificate it signs, with a "localhost" DNS
    // SAN so hostname verification passes. Returns the CA (public, to trust via a CA
    // file) and the server certificate with its key (for the broker to present).
    private static (X509Certificate2 Ca, X509Certificate2 Server) CaAndServerCert()
    {
        using var caRsa = RSA.Create(2048);
        var caReq = new CertificateRequest("CN=fibril-test-ca", caRsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        caReq.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: true, hasPathLengthConstraint: false, pathLengthConstraint: 0, critical: true));
        using var ca = caReq.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));

        using var srvRsa = RSA.Create(2048);
        var srvReq = new CertificateRequest("CN=localhost", srvRsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName("localhost");
        srvReq.CertificateExtensions.Add(san.Build());
        using var srvCert = srvReq.Create(ca, DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1), new byte[] { 1, 2, 3, 4 });
        using var withKey = srvCert.CopyWithPrivateKey(srvRsa);
        // Re-import through PKCS#12 so the private key is usable server-side on any platform.
        var server = X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pfx), null);
        var caPublic = X509CertificateLoader.LoadCertificate(ca.Export(X509ContentType.Cert));
        return (caPublic, server);
    }

    [Fact]
    public async Task CaFileTrustCompletesHandshake()
    {
        var (ca, server) = CaAndServerCert();
        using (ca)
        using (server)
        {
            var caPath = Path.Combine(Directory.CreateTempSubdirectory().FullName, "ca.crt");
            File.WriteAllText(caPath, ca.ExportCertificatePem());
            await using var broker = new FakeBroker { ServerCertificate = server };
            // Trust the CA (no fingerprint pin), and verify the "localhost" SAN.
            await using var client = await Client.ConnectAsync(broker.Address, Opts(new TlsOptions { CaFile = caPath, ServerName = "localhost" }), Timeout());

            var offset = await client.Publisher("t").PublishConfirmedAsync(Message.Text("hello"), Timeout());
            Assert.Equal(FakeBroker.FirstOffset, offset);
        }
    }

    [Fact]
    public async Task ClientCertificatePassesRequiringBroker()
    {
        using var cert = SelfSigned();
        var (certPath, keyPath) = ClientCertPem();
        await using var broker = new FakeBroker { ServerCertificate = cert, RequireClientCertificate = true };
        await using var client = await Client.ConnectAsync(broker.Address, Opts(new TlsOptions
        {
            CaFingerprint = Fingerprint(cert),
            ClientCertFile = certPath,
            ClientKeyFile = keyPath,
        }), Timeout());

        var offset = await client.Publisher("t").PublishConfirmedAsync(Message.Text("hello"), Timeout());
        Assert.Equal(FakeBroker.FirstOffset, offset);
    }

    [Fact]
    public async Task CertlessClientAgainstRequiringBrokerGetsTypedError()
    {
        using var cert = SelfSigned();
        await using var broker = new FakeBroker { ServerCertificate = cert, RequireClientCertificate = true };
        await Assert.ThrowsAsync<TlsClientCertificateRequiredException>(
            () => Client.ConnectAsync(broker.Address, Opts(new TlsOptions { CaFingerprint = Fingerprint(cert) }), Timeout()));
    }
}
