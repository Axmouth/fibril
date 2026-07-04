//! Client-side TLS: trust configuration, the connector, and error
//! classification that keeps transport mismatches apart from certificate
//! trust failures, because their fixes differ.

use std::io;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::client::danger::{
    HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
};
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::{self, DigitallySignedStruct, SignatureScheme};

use crate::{FibrilError, FibrilResult};
use fibril_util::net::TcpStream;

/// TLS options for connecting to a TLS-enabled broker.
///
/// Trust resolution order: `ca_fingerprint` pin if set, else `ca_path`
/// roots, else the OS trust store (for brokers with publicly issued
/// certificates).
#[derive(Debug, Clone, Default)]
pub struct TlsClientOptions {
    /// PEM file with the CA certificate(s) to trust, e.g. the broker's
    /// generated `<data_dir>/tls/ca.pem`.
    pub ca_path: Option<PathBuf>,
    /// SHA-256 fingerprint of the broker CA (or server) certificate, as
    /// printed in the broker startup log. Hex digits, colons optional.
    /// Pinning replaces chain-of-trust verification, the handshake still
    /// proves possession of the certificate key.
    pub ca_fingerprint: Option<String>,
    /// Name verified against the certificate (and sent as SNI). Defaults to
    /// the host part of the connect address.
    pub server_name: Option<String>,
    /// PEM client certificate chain presented to the broker, for
    /// `tls.client_auth` deployments. Set together with `client_key_path`.
    pub client_cert_path: Option<PathBuf>,
    /// PEM private key for the client certificate.
    pub client_key_path: Option<PathBuf>,
}

/// The connection transport: plaintext or TLS over the same net seam. One
/// enum keeps the engine monomorphic while the choice stays runtime config.
pub(crate) enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_flush(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Open the transport to `address`: TCP, then TLS on top when configured.
pub(crate) async fn establish_stream(
    address: &str,
    tls: Option<&TlsClientOptions>,
) -> FibrilResult<MaybeTlsStream> {
    let stream = TcpStream::connect(address)
        .await
        .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
    stream
        .set_nodelay(true)
        .map_err(|e| FibrilError::Disconnection {
            msg: format!("failed to set TCP_NODELAY: {e}"),
        })?;
    let Some(tls) = tls else {
        return Ok(MaybeTlsStream::Plain(stream));
    };
    let connector = build_connector(tls)?;
    let name = resolve_server_name(address, tls)?;
    match connector.connect(name, stream).await {
        Ok(stream) => Ok(MaybeTlsStream::Tls(Box::new(stream))),
        Err(err) => Err(classify_tls_connect_error(err, address)),
    }
}

/// Sort a failed TLS handshake into the taxonomy: an early end of the
/// connection is almost always a plaintext broker (the plaintext listener
/// closes on sighting a ClientHello), certificate failures are trust
/// configuration, everything else stays a generic handshake error.
fn classify_tls_connect_error(err: io::Error, address: &str) -> FibrilError {
    if matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    ) {
        return FibrilError::TlsNotSupportedByBroker {
            address: address.to_string(),
        };
    }
    if let Some(inner) = err.get_ref()
        && let Some(tls_err) = inner.downcast_ref::<rustls::Error>()
    {
        if matches!(
            tls_err,
            rustls::Error::AlertReceived(rustls::AlertDescription::CertificateRequired)
        ) {
            return FibrilError::TlsClientCertificateRequired {
                address: address.to_string(),
            };
        }
        if matches!(tls_err, rustls::Error::InvalidCertificate(_)) {
            return FibrilError::TlsCertificateUntrusted {
                msg: tls_err.to_string(),
            };
        }
        return FibrilError::TlsHandshake {
            msg: tls_err.to_string(),
        };
    }
    FibrilError::TlsHandshake {
        msg: err.to_string(),
    }
}

/// With TLS 1.3 the client side of the handshake completes before the
/// broker's client-certificate verdict, so a `require` rejection lands on
/// the first read after connect instead of in the handshake itself. The
/// alert is sometimes readable there (the typed lane) and sometimes
/// flattened into a bare stream end by connection teardown, so a certless
/// TLS session that dies during the HELLO exchange is attributed to the
/// certificate requirement - the one broker behavior with exactly that
/// shape.
pub(crate) fn refine_post_connect_error(
    err: FibrilError,
    tls: Option<&TlsClientOptions>,
    address: &str,
) -> FibrilError {
    let Some(tls) = tls else {
        return err;
    };
    let required = || FibrilError::TlsClientCertificateRequired {
        address: address.to_string(),
    };
    match &err {
        FibrilError::Disconnection { msg } if msg.contains("CertificateRequired") => required(),
        FibrilError::Eof | FibrilError::BrokenPipe | FibrilError::Disconnection { .. }
            if tls.client_cert_path.is_none() =>
        {
            required()
        }
        _ => err,
    }
}

fn build_connector(tls: &TlsClientOptions) -> FibrilResult<TlsConnector> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| FibrilError::TlsConfig { msg: e.to_string() })?;

    let client_identity = match (&tls.client_cert_path, &tls.client_key_path) {
        (Some(cert_path), Some(key_path)) => Some(load_client_identity(cert_path, key_path)?),
        (None, None) => None,
        _ => {
            return Err(FibrilError::TlsConfig {
                msg: "client certificate options must be set together: both the                       certificate and its key"
                    .into(),
            });
        }
    };
    let with_identity = |built: rustls::ConfigBuilder<
        rustls::ClientConfig,
        rustls::client::WantsClientCert,
    >|
     -> FibrilResult<rustls::ClientConfig> {
        match client_identity {
            None => Ok(built.with_no_client_auth()),
            Some((certs, key)) => {
                built
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| FibrilError::TlsConfig {
                        msg: format!("client certificate rejected: {e}"),
                    })
            }
        }
    };

    let config = if let Some(raw) = &tls.ca_fingerprint {
        let pin = parse_fingerprint(raw)?;
        with_identity(
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(FingerprintVerifier { pin, provider })),
        )?
    } else {
        let mut roots = rustls::RootCertStore::empty();
        if let Some(path) = &tls.ca_path {
            let file = std::fs::File::open(path).map_err(|e| FibrilError::TlsConfig {
                msg: format!("failed to open tls ca_path {}: {e}", path.display()),
            })?;
            let mut added = 0usize;
            for cert in rustls_pemfile::certs(&mut io::BufReader::new(file)) {
                let cert = cert.map_err(|e| FibrilError::TlsConfig {
                    msg: format!("failed to parse tls ca_path {}: {e}", path.display()),
                })?;
                roots.add(cert).map_err(|e| FibrilError::TlsConfig {
                    msg: format!("rejected CA certificate in {}: {e}", path.display()),
                })?;
                added += 1;
            }
            if added == 0 {
                return Err(FibrilError::TlsConfig {
                    msg: format!("no certificates found in tls ca_path {}", path.display()),
                });
            }
        } else {
            for cert in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(cert);
            }
            if roots.is_empty() {
                return Err(FibrilError::TlsConfig {
                    msg: "no OS trust roots available, set tls ca_path or ca_fingerprint".into(),
                });
            }
        }
        with_identity(builder.with_root_certificates(roots))?
    };
    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_client_identity(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
) -> FibrilResult<(
    Vec<CertificateDer<'static>>,
    rustls::pki_types::PrivateKeyDer<'static>,
)> {
    let open = |path: &std::path::Path| {
        std::fs::File::open(path).map_err(|e| FibrilError::TlsConfig {
            msg: format!("failed to open {}: {e}", path.display()),
        })
    };
    let certs = rustls_pemfile::certs(&mut io::BufReader::new(open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| FibrilError::TlsConfig {
            msg: format!(
                "failed to parse client certificate {}: {e}",
                cert_path.display()
            ),
        })?;
    if certs.is_empty() {
        return Err(FibrilError::TlsConfig {
            msg: format!("no certificates found in {}", cert_path.display()),
        });
    }
    let key = rustls_pemfile::private_key(&mut io::BufReader::new(open(key_path)?))
        .map_err(|e| FibrilError::TlsConfig {
            msg: format!("failed to parse client key {}: {e}", key_path.display()),
        })?
        .ok_or_else(|| FibrilError::TlsConfig {
            msg: format!("no private key found in {}", key_path.display()),
        })?;
    Ok((certs, key))
}

fn resolve_server_name(address: &str, tls: &TlsClientOptions) -> FibrilResult<ServerName<'static>> {
    let host = match &tls.server_name {
        Some(name) => name.clone(),
        None => address
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(address)
            .trim_matches(['[', ']'])
            .to_string(),
    };
    ServerName::try_from(host).map_err(|e| FibrilError::TlsConfig {
        msg: format!("invalid TLS server name derived from `{address}`: {e}"),
    })
}

fn parse_fingerprint(raw: &str) -> FibrilResult<Vec<u8>> {
    let hex: String = raw
        .chars()
        .filter(|c| !matches!(c, ':' | ' '))
        .collect::<String>()
        .to_ascii_lowercase();
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(FibrilError::TlsConfig {
            msg: format!(
                "tls ca_fingerprint must be 64 hex digits (SHA-256, colons optional), \
                 got {} usable digits",
                hex.len()
            ),
        });
    }
    Ok((0..32)
        .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0))
        .collect())
}

/// Accepts a presented chain when any certificate in it matches the pinned
/// SHA-256 fingerprint. Handshake signatures are still verified, so a peer
/// must hold the pinned certificate's key, not merely replay its bytes.
#[derive(Debug)]
struct FingerprintVerifier {
    pin: Vec<u8>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for FingerprintVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        for cert in std::iter::once(end_entity).chain(intermediates.iter()) {
            if Sha256::digest(cert.as_ref()).as_slice() == self.pin.as_slice() {
                return Ok(ServerCertVerified::assertion());
            }
        }
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::ApplicationVerificationFailure,
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_parses_colon_hex_and_bare_hex() {
        let colon = "AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99:\
                     AA:BB:CC:DD:EE:FF:00:11:22:33:44:55:66:77:88:99";
        let bare = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899";
        assert_eq!(
            parse_fingerprint(colon).expect("colon form"),
            parse_fingerprint(bare).expect("bare form")
        );
    }

    #[test]
    fn fingerprint_rejects_wrong_length_and_non_hex() {
        assert!(parse_fingerprint("abcd").is_err());
        assert!(
            parse_fingerprint("zzbbccddeeff00112233445566778899aabbccddeeff00112233445566778899")
                .is_err()
        );
    }

    #[test]
    fn server_name_derives_from_address_and_respects_override() {
        let default_tls = TlsClientOptions::default();
        assert!(resolve_server_name("broker-1:9876", &default_tls).is_ok());
        assert!(resolve_server_name("127.0.0.1:9876", &default_tls).is_ok());
        assert!(resolve_server_name("[::1]:9876", &default_tls).is_ok());

        let overridden = TlsClientOptions {
            server_name: Some("broker.internal".into()),
            ..Default::default()
        };
        let name = resolve_server_name("127.0.0.1:9876", &overridden).expect("override");
        assert!(format!("{name:?}").contains("broker.internal"));
    }
}
