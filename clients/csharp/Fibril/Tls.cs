using System.Collections.Generic;
using System.IO;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Fibril;

/// <summary>
/// TLS connection settings. Trust resolves in order: a fingerprint pin if set,
/// else a CA file, else the OS roots. An optional client certificate enables mTLS.
/// </summary>
public sealed record TlsOptions
{
    /// <summary>A PEM file of CA certificate(s) to trust. Empty uses the OS trust store.</summary>
    public string? CaFile { get; init; }

    /// <summary>
    /// Pins the broker certificate by its SHA-256 fingerprint (hex, colons optional).
    /// Pinning replaces the CA trust store: the broker is trusted only when the pinned
    /// certificate is its leaf, or is a CA that genuinely signed the presented leaf (so
    /// a CA pin survives leaf rotation). Hostname verification is skipped because the
    /// pin is the trust root. Takes precedence over <see cref="CaFile"/>.
    /// </summary>
    public string? CaFingerprint { get; init; }

    /// <summary>The name verified against the certificate and sent as SNI. Empty defaults to the dial host.</summary>
    public string? ServerName { get; init; }

    /// <summary>Presents a client certificate for mTLS deployments. Set both or neither.</summary>
    public string? ClientCertFile { get; init; }

    public string? ClientKeyFile { get; init; }
}

// Builds an authenticated SslStream over the raw connection using the trust
// settings, and maps handshake failures onto the typed TLS error taxonomy.
internal static class Tls
{
    // Captures a certificate-rejection reason from the validation callback so the
    // opaque AuthenticationException can be turned into a typed, specific error.
    private sealed class Rejection
    {
        public string? Detail;
    }

    public static async Task<Stream> WrapAsync(Stream inner, string addr, string host, TlsOptions opts, CancellationToken ct)
    {
        var pin = opts.CaFingerprint is { Length: > 0 } fp ? ParseFingerprint(fp) : null;
        var caCerts = pin is null && opts.CaFile is { Length: > 0 } caFile ? LoadCa(caFile) : null;
        var clientCerts = LoadClientCert(opts);
        var rejection = new Rejection();

        var ssl = new SslStream(inner, leaveInnerStreamOpen: false, (_, cert, chain, errors) =>
            Validate(cert, chain, errors, pin, caCerts, rejection));

        var authOptions = new SslClientAuthenticationOptions
        {
            TargetHost = opts.ServerName is { Length: > 0 } sni ? sni : host,
            EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13,
            ClientCertificates = clientCerts,
        };

        try
        {
            await ssl.AuthenticateAsClientAsync(authOptions, ct).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            await ssl.DisposeAsync().ConfigureAwait(false);
            throw Classify(ex, addr, rejection.Detail);
        }
        return ssl;
    }

    private static bool Validate(X509Certificate? cert, X509Chain? chain, SslPolicyErrors errors, byte[]? pin, X509Certificate2Collection? caCerts, Rejection rejection)
    {
        if (cert is null)
        {
            rejection.Detail = "the broker presented no certificate";
            return false;
        }

        if (pin is not null)
        {
            // The pin replaces the CA trust store and hostname verification. The
            // handshake still proves possession of the leaf key.
            if (ValidatePinnedChain(PresentedChain(cert, chain), pin))
            {
                return true;
            }
            rejection.Detail = "the presented certificate is not the pinned certificate, nor signed by the pinned CA";
            return false;
        }

        if (caCerts is not null)
        {
            if ((errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
            {
                rejection.Detail = "certificate name does not match the server name";
                return false;
            }
            using var built = new X509Chain();
            built.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            built.ChainPolicy.CustomTrustStore.AddRange(caCerts);
            built.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            if (built.Build(new X509Certificate2(cert.GetRawCertData())))
            {
                return true;
            }
            rejection.Detail = "certificate does not chain to the trusted CA file";
            return false;
        }

        // OS roots: full default verification.
        if (errors == SslPolicyErrors.None)
        {
            return true;
        }
        rejection.Detail = errors.ToString();
        return false;
    }

    // The certificates the broker presented, leaf first, as the chain engine saw
    // them. Used as the input to pin validation.
    private static IReadOnlyList<X509Certificate2> PresentedChain(X509Certificate leaf, X509Chain? chain)
    {
        if (chain is not null && chain.ChainElements.Count > 0)
        {
            var certs = new List<X509Certificate2>(chain.ChainElements.Count);
            foreach (var element in chain.ChainElements)
            {
                certs.Add(element.Certificate);
            }
            return certs;
        }
        return new List<X509Certificate2> { new X509Certificate2(leaf.GetRawCertData()) };
    }

    // Enforces a SHA-256 certificate pin against the presented chain (ordered
    // leaf first). A fingerprint can pin either the leaf or an issuer:
    //
    //   - Leaf pin: the pin matches the presented leaf. The handshake already
    //     proved possession of the leaf's key, so the match stands on its own.
    //   - CA pin: the pin matches an issuer (so the leaf can rotate under the same
    //     CA). The pinned certificate merely appearing in the chain proves nothing,
    //     because the real CA certificate is public and a man-in-the-middle can
    //     staple it beside a rogue leaf it controls. So the leaf is path-validated
    //     against the pinned certificate as the sole trust root, and accepted only
    //     if it is genuinely signed by it.
    internal static bool ValidatePinnedChain(IReadOnlyList<X509Certificate2> presented, byte[] pin)
    {
        if (presented.Count == 0)
        {
            return false;
        }
        var leaf = presented[0];
        if (Sha256Matches(leaf, pin))
        {
            return true;
        }
        for (var i = 1; i < presented.Count; i++)
        {
            if (Sha256Matches(presented[i], pin) && LeafSignedByPinnedAnchor(leaf, presented[i], presented))
            {
                return true;
            }
        }
        return false;
    }

    private static bool Sha256Matches(X509Certificate2 cert, byte[] pin) =>
        SHA256.HashData(cert.RawData).AsSpan().SequenceEqual(pin);

    private static bool LeafSignedByPinnedAnchor(X509Certificate2 leaf, X509Certificate2 anchor, IReadOnlyList<X509Certificate2> presented)
    {
        using var built = new X509Chain();
        built.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
        built.ChainPolicy.CustomTrustStore.Add(anchor);
        built.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
        foreach (var cert in presented)
        {
            built.ChainPolicy.ExtraStore.Add(cert);
        }
        // No hostname check: the pin, not a name, is the trust root.
        return built.Build(leaf);
    }

    private static byte[] ParseFingerprint(string raw)
    {
        var hex = raw.Replace(":", "").Replace(" ", "").ToLowerInvariant();
        try
        {
            var bytes = Convert.FromHexString(hex);
            if (bytes.Length != SHA256.HashSizeInBytes)
            {
                throw new FormatException();
            }
            return bytes;
        }
        catch (FormatException)
        {
            throw new TlsConfigException("tls CaFingerprint must be 64 hex digits (SHA-256, colons optional)");
        }
    }

    private static X509Certificate2Collection LoadCa(string caFile)
    {
        var collection = new X509Certificate2Collection();
        try
        {
            collection.ImportFromPemFile(caFile);
        }
        catch (Exception ex)
        {
            throw new TlsConfigException("failed to read tls CaFile " + caFile + ": " + ex.Message);
        }
        if (collection.Count == 0)
        {
            throw new TlsConfigException("tls CaFile " + caFile + " contains no usable certificates");
        }
        return collection;
    }

    private static X509CertificateCollection? LoadClientCert(TlsOptions opts)
    {
        if ((opts.ClientCertFile is null) != (opts.ClientKeyFile is null))
        {
            throw new TlsConfigException("client certificate options must be set together: both the certificate and its key");
        }
        if (opts.ClientCertFile is null || opts.ClientKeyFile is null)
        {
            return null;
        }
        try
        {
            using var pem = X509Certificate2.CreateFromPemFile(opts.ClientCertFile, opts.ClientKeyFile);
            // Re-import through PKCS#12 so the private key is usable by the TLS stack
            // on every platform (a bare PEM-loaded key is ephemeral on Windows).
            var cert = new X509Certificate2(pem.Export(X509ContentType.Pfx));
            return new X509CertificateCollection { cert };
        }
        catch (Exception ex) when (ex is not TlsConfigException)
        {
            throw new TlsConfigException("failed to load tls client certificate: " + ex.Message);
        }
    }

    private static Exception Classify(Exception ex, string addr, string? rejectionDetail)
    {
        if (rejectionDetail is not null)
        {
            return new TlsCertificateUntrustedException(rejectionDetail);
        }
        // A handshake that ends early, or non-TLS bytes where the ServerHello should
        // be, means the broker listener is almost certainly speaking plaintext.
        var message = (ex.InnerException?.Message ?? "") + " " + ex.Message;
        if (ex is IOException || message.Contains("unexpected", StringComparison.OrdinalIgnoreCase)
            || message.Contains("EOF", StringComparison.OrdinalIgnoreCase)
            || message.Contains("could not be established", StringComparison.OrdinalIgnoreCase))
        {
            return new TlsNotSupportedByBrokerException(addr);
        }
        if (ex is AuthenticationException)
        {
            return new TlsCertificateUntrustedException(ex.Message);
        }
        return new TlsHandshakeException("TLS handshake with " + addr + " failed: " + ex.Message);
    }
}
