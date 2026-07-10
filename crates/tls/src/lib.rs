//! Broker TLS material: operator-supplied PEM files or per-deployment
//! material generated under the data dir.
//!
//! Fibril never ships certificates. The generated path follows the
//! Elasticsearch 8 pattern: a per-deployment CA and server certificate are
//! created on first boot and the CA fingerprint is printed so clients can
//! pin or trust it.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use sha2::{Digest, Sha256};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::sign::CertifiedKey;
use tokio_rustls::rustls::{self, ServerConfig as RustlsServerConfig};

pub use fibril_config::{ClientAuthMode, TlsMode};
pub use tokio_rustls;
pub use tokio_rustls::{TlsAcceptor as RustlsAcceptor, TlsConnector};

/// Directory under the data dir holding generated material.
pub const GENERATED_TLS_DIR: &str = "tls";
/// Directory under the data dir holding operator material uploaded through
/// first-boot setup, kept apart from the generated dir so the
/// partial-material check there stays meaningful.
pub const PROVIDED_TLS_DIR: &str = "tls-provided";

#[derive(Debug, thiserror::Error)]
pub enum TlsSetupError {
    #[error(
        "failed to read TLS material at {path}: {source}. Check that tls.cert_path and \
         tls.key_path point at readable PEM files"
    )]
    ReadMaterial {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("no certificates found in {path}: the file must hold a PEM certificate chain")]
    EmptyCertChain { path: PathBuf },
    #[error(
        "no private key found in {path}: the file must hold a PEM private key \
         (PKCS#8, PKCS#1 or SEC1)"
    )]
    NoPrivateKey { path: PathBuf },
    #[error("TLS configuration rejected: {0}")]
    Rustls(#[from] rustls::Error),
    #[error("failed to generate per-deployment TLS material: {0}")]
    Generate(#[from] rcgen::Error),
    #[error("failed to write generated TLS material at {path}: {source}")]
    WriteMaterial {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error(
        "generated TLS material under {dir} is incomplete: expected ca.pem, ca.key, \
         server.pem and server.key together, or only the ca.pem + ca.key pair to mint \
         this node's server certificate from a shared CA. Remove the directory to \
         regenerate, or supply tls.cert_path and tls.key_path instead"
    )]
    PartialGeneratedMaterial { dir: PathBuf },
    #[error("uploaded TLS material rejected: {detail}")]
    InvalidUploadedMaterial { detail: String },
}

/// The acceptor plus where its material came from, for startup reporting.
/// `server_config` is shared so the admin HTTP server can serve HTTPS from
/// the same material. The certificate is served through a swappable
/// resolver, so [`ServerTls::reload`] rotates the leaf without touching the
/// config or any live connection.
pub struct ServerTls {
    pub acceptor: TlsAcceptor,
    pub server_config: Arc<RustlsServerConfig>,
    pub source: TlsMaterialSource,
    resolver: Arc<ReloadableCertResolver>,
    material: MaterialPaths,
}

pub enum TlsMaterialSource {
    Provided,
    Generated {
        dir: PathBuf,
        ca_fingerprint: String,
    },
}

/// Where the serving material lives on disk, so a reload re-reads the same
/// source the boot did. `chain_ca` is appended to the served chain (the
/// generated CA), so fingerprint-pinning clients keep seeing the CA.
#[derive(Debug, Clone)]
struct MaterialPaths {
    cert_path: PathBuf,
    key_path: PathBuf,
    chain_ca: Option<PathBuf>,
}

impl ServerTls {
    /// Re-read the certificate and key from their configured paths,
    /// validate that they form a working pair, and swap them in. New
    /// handshakes present the new certificate, connections already
    /// established keep the one they negotiated. Nothing changes when
    /// validation fails: the old material keeps serving.
    ///
    /// Returns the fingerprint of the new leaf certificate.
    pub fn reload(&self) -> Result<String, TlsSetupError> {
        let certified = certified_key_from_paths(&self.material)?;
        let fingerprint = fingerprint_der(&certified.cert[0]);
        self.resolver.current.store(Arc::new(certified));
        Ok(fingerprint)
    }

    /// Fingerprint of the leaf certificate currently being served to new
    /// handshakes.
    pub fn leaf_fingerprint(&self) -> String {
        fingerprint_der(&self.resolver.current.load().cert[0])
    }

    /// Presentation metadata for the leaf certificate currently being served:
    /// its fingerprint, validity window (Unix seconds), and subject. Feeds the
    /// admin dashboard's certificate view and its expiry warning.
    pub fn leaf_metadata(&self) -> CertMetadata {
        let certified = self.resolver.current.load();
        cert_metadata(&certified.cert[0])
    }
}

/// Presentation metadata parsed from a leaf certificate. Absent validity fields
/// mean the certificate did not parse; the fingerprint is always available.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertMetadata {
    pub fingerprint: String,
    pub not_before_unix: Option<i64>,
    pub not_after_unix: Option<i64>,
    pub subject: Option<String>,
}

fn cert_metadata(cert: &CertificateDer<'_>) -> CertMetadata {
    let fingerprint = fingerprint_der(cert);
    match x509_parser::parse_x509_certificate(cert.as_ref()) {
        Ok((_, parsed)) => CertMetadata {
            fingerprint,
            not_before_unix: Some(parsed.validity().not_before.timestamp()),
            not_after_unix: Some(parsed.validity().not_after.timestamp()),
            subject: Some(parsed.subject().to_string()),
        },
        Err(_) => CertMetadata {
            fingerprint,
            not_before_unix: None,
            not_after_unix: None,
            subject: None,
        },
    }
}

/// Serves the current certificate to every new handshake, swappable at
/// runtime for live rotation.
struct ReloadableCertResolver {
    current: ArcSwap<CertifiedKey>,
}

impl fmt::Debug for ReloadableCertResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReloadableCertResolver")
    }
}

impl rustls::server::ResolvesServerCert for ReloadableCertResolver {
    fn resolve(&self, _hello: rustls::server::ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current.load_full())
    }
}

/// Client-certificate policy for the server build: the mode plus where the
/// client CA lives. `ca_path` unset falls back to the generated
/// `<data_dir>/tls/ca.pem`.
#[derive(Debug, Clone, Default)]
pub struct ClientAuthPolicy {
    pub mode: ClientAuthMode,
    pub ca_path: Option<PathBuf>,
}

/// Build the broker TLS acceptor for the configured mode. `extra_sans` adds
/// hostnames or IPs (typically the advertise hosts) to a generated server
/// certificate on top of the localhost set. Client certificates are not
/// requested; use [`build_server_tls_with_client_auth`] for that.
pub fn build_server_tls(
    mode: &TlsMode,
    data_dir: &Path,
    extra_sans: &[String],
) -> Result<Option<ServerTls>, TlsSetupError> {
    build_server_tls_with_client_auth(mode, data_dir, extra_sans, &ClientAuthPolicy::default())
}

/// [`build_server_tls`] with a client-certificate policy: `request` verifies
/// a certificate when one is presented while still admitting certless
/// clients, `require` rejects certless clients in the handshake.
pub fn build_server_tls_with_client_auth(
    mode: &TlsMode,
    data_dir: &Path,
    extra_sans: &[String],
    client_auth: &ClientAuthPolicy,
) -> Result<Option<ServerTls>, TlsSetupError> {
    let (material, source) = match mode {
        TlsMode::Disabled => return Ok(None),
        TlsMode::Provided {
            cert_path,
            key_path,
        } => (
            MaterialPaths {
                cert_path: cert_path.clone(),
                key_path: key_path.clone(),
                chain_ca: None,
            },
            TlsMaterialSource::Provided,
        ),
        TlsMode::AutoSelfSigned => {
            let dir = data_dir.join(GENERATED_TLS_DIR);
            ensure_generated_material(&dir, extra_sans)?;
            let files = GeneratedFiles::new(&dir);
            let ca_fingerprint = ca_fingerprint(&files.ca_pem)?;
            (
                MaterialPaths {
                    cert_path: files.server_pem,
                    key_path: files.server_key,
                    // Serve leaf + CA so a client pinning the CA
                    // fingerprint can match it against the presented chain.
                    chain_ca: Some(files.ca_pem),
                },
                TlsMaterialSource::Generated {
                    dir,
                    ca_fingerprint,
                },
            )
        }
    };

    let certified = certified_key_from_paths(&material)?;
    let resolver = Arc::new(ReloadableCertResolver {
        current: ArcSwap::from_pointee(certified),
    });
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = RustlsServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?;
    let config = match client_verifier(client_auth, data_dir)? {
        Some(verifier) => builder.with_client_cert_verifier(verifier),
        None => builder.with_no_client_auth(),
    }
    .with_cert_resolver(resolver.clone());
    let server_config = Arc::new(config);
    Ok(Some(ServerTls {
        acceptor: TlsAcceptor::from(server_config.clone()),
        server_config,
        source,
        resolver,
        material,
    }))
}

/// Build the client-certificate verifier for the policy, or `None` when
/// client auth is off. Client certificates only make sense against a
/// deployment-controlled CA, so unlike server trust there is no OS-roots
/// fallback: no explicit `client_ca_path` and no generated CA is a guided
/// error.
fn client_verifier(
    policy: &ClientAuthPolicy,
    data_dir: &Path,
) -> Result<Option<Arc<dyn rustls::server::danger::ClientCertVerifier>>, TlsSetupError> {
    if policy.mode == ClientAuthMode::Off {
        return Ok(None);
    }
    let generated_ca = data_dir.join(GENERATED_TLS_DIR).join("ca.pem");
    let ca_path = match &policy.ca_path {
        Some(path) => path.clone(),
        None if generated_ca.exists() => generated_ca,
        None => {
            return Err(TlsSetupError::InvalidUploadedMaterial {
                detail: "tls.client_auth needs a client CA: set tls.client_ca_path or use \
                         generated material so <data_dir>/tls/ca.pem exists"
                    .to_string(),
            });
        }
    };
    let mut roots = rustls::RootCertStore::empty();
    for cert in load_certs(&ca_path)? {
        roots
            .add(cert)
            .map_err(|err| TlsSetupError::InvalidUploadedMaterial {
                detail: format!(
                    "rejected client CA certificate in {}: {err}",
                    ca_path.display()
                ),
            })?;
    }
    let builder = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    );
    let builder = match policy.mode {
        ClientAuthMode::Request => builder.allow_unauthenticated(),
        _ => builder,
    };
    let verifier = builder
        .build()
        .map_err(|err| TlsSetupError::InvalidUploadedMaterial {
            detail: format!("client certificate verifier rejected: {err}"),
        })?;
    Ok(Some(verifier))
}

/// Identity a verified client certificate asserts. See
/// [`fibril_util::client_identity_from_der`]; this is the certificate-typed
/// convenience over the same contract.
pub fn client_identity_from_cert(cert: &CertificateDer<'_>) -> Option<String> {
    fibril_util::client_identity_from_der(cert.as_ref())
}

/// Issue a client certificate for `identity` from the deployment CA under
/// `tls_dir`, returning the certificate and key as PEM. The identity rides
/// as the DNS subject alternative name and common name, matching what the
/// broker extracts on a verified connection.
pub fn issue_client_certificate(
    tls_dir: &Path,
    identity: &str,
) -> Result<(String, String), TlsSetupError> {
    if identity.is_empty() || identity.starts_with('@') {
        return Err(TlsSetupError::InvalidUploadedMaterial {
            detail: format!(
                "client certificate identity `{identity}` is not usable: it must be                  non-empty and outside the @ node namespace"
            ),
        });
    }
    let files = GeneratedFiles::new(tls_dir);
    let ca_pem =
        fs::read_to_string(&files.ca_pem).map_err(|source| TlsSetupError::ReadMaterial {
            path: files.ca_pem.clone(),
            source,
        })?;
    let ca_key_pem =
        fs::read_to_string(&files.ca_key).map_err(|source| TlsSetupError::ReadMaterial {
            path: files.ca_key.clone(),
            source,
        })?;
    let ca_key = rcgen::KeyPair::from_pem(&ca_key_pem)?;
    let ca_cert = rcgen::CertificateParams::from_ca_cert_pem(&ca_pem)?.self_signed(&ca_key)?;

    let client_key = rcgen::KeyPair::generate()?;
    let mut params = rcgen::CertificateParams::new(vec![identity.to_string()])?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, identity);
    let cert = params.signed_by(&client_key, &ca_cert, &ca_key)?;
    Ok((cert.pem(), client_key.serialize_pem()))
}

/// Load and validate the serving pair from disk: the chain (leaf plus the
/// generated CA when present) and a key proven to match the leaf.
fn certified_key_from_paths(material: &MaterialPaths) -> Result<CertifiedKey, TlsSetupError> {
    let mut certs = load_certs(&material.cert_path)?;
    if let Some(chain_ca) = &material.chain_ca {
        certs.extend(load_certs(chain_ca)?);
    }
    let key = load_key(&material.key_path)?;
    let provider = rustls::crypto::ring::default_provider();
    Ok(CertifiedKey::from_der(certs, key, &provider)?)
}

/// SHA-256 fingerprint of a DER certificate, colon-hex.
fn fingerprint_der(cert: &CertificateDer<'_>) -> String {
    let digest = Sha256::digest(cert.as_ref());
    digest
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// SHA-256 fingerprint of the first certificate in a PEM file, colon-hex.
pub fn ca_fingerprint(ca_pem: &Path) -> Result<String, TlsSetupError> {
    let certs = load_certs(ca_pem)?;
    Ok(fingerprint_der(&certs[0]))
}

/// Extract SAN-usable hosts from advertise `host:port` entries. Brackets are
/// stripped so an IPv6 literal becomes a plain address SAN.
pub fn san_hosts_from_advertise(advertise: &[String]) -> Vec<String> {
    advertise
        .iter()
        .filter_map(|entry| entry.rsplit_once(':').map(|(host, _)| host))
        .map(|host| host.trim_matches(['[', ']']).to_string())
        .filter(|host| !host.is_empty())
        .collect()
}

struct GeneratedFiles {
    ca_pem: PathBuf,
    ca_key: PathBuf,
    server_pem: PathBuf,
    server_key: PathBuf,
}

impl GeneratedFiles {
    fn new(dir: &Path) -> Self {
        Self {
            ca_pem: dir.join("ca.pem"),
            ca_key: dir.join("ca.key"),
            server_pem: dir.join("server.pem"),
            server_key: dir.join("server.key"),
        }
    }
}

/// What to do with the generated-material directory, decided from which of
/// the four files are present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeneratedPlan {
    /// All four files exist: load them as they are.
    Reuse,
    /// Nothing exists: mint a CA and a server certificate.
    GenerateAll,
    /// Only the CA pair exists - the shared-CA cluster lane, where the
    /// operator copied `ca.pem` and `ca.key` from another node. Mint only
    /// this node's server certificate from that CA.
    MintServerFromCa,
}

/// The full presence matrix. Every other combination is partial material
/// that gets refused rather than guessed at: a lone key cannot be paired,
/// and a lone certificate cannot be served.
fn generated_plan(
    ca_pem: bool,
    ca_key: bool,
    server_pem: bool,
    server_key: bool,
) -> Option<GeneratedPlan> {
    match (ca_pem, ca_key, server_pem, server_key) {
        (true, true, true, true) => Some(GeneratedPlan::Reuse),
        (false, false, false, false) => Some(GeneratedPlan::GenerateAll),
        (true, true, false, false) => Some(GeneratedPlan::MintServerFromCa),
        _ => None,
    }
}

/// Generate the CA and server certificate if the directory holds none, mint
/// a server certificate when only a shared CA pair is present, and reuse
/// complete material as is. Partial material is refused rather than guessed
/// at.
pub fn ensure_generated_material(dir: &Path, extra_sans: &[String]) -> Result<(), TlsSetupError> {
    let files = GeneratedFiles::new(dir);
    let plan = generated_plan(
        files.ca_pem.exists(),
        files.ca_key.exists(),
        files.server_pem.exists(),
        files.server_key.exists(),
    )
    .ok_or_else(|| TlsSetupError::PartialGeneratedMaterial {
        dir: dir.to_path_buf(),
    })?;

    let (ca_cert, ca_key) = match plan {
        GeneratedPlan::Reuse => return Ok(()),
        GeneratedPlan::GenerateAll => {
            let ca_key = rcgen::KeyPair::generate()?;
            let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new())?;
            ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
            ca_params
                .distinguished_name
                .push(rcgen::DnType::CommonName, "Fibril per-deployment CA");
            let ca_cert = ca_params.self_signed(&ca_key)?;

            fs::create_dir_all(dir).map_err(|source| TlsSetupError::WriteMaterial {
                path: dir.to_path_buf(),
                source,
            })?;
            write_public(&files.ca_pem, ca_cert.pem().as_bytes())?;
            write_secret(&files.ca_key, ca_key.serialize_pem().as_bytes())?;
            (ca_cert, ca_key)
        }
        GeneratedPlan::MintServerFromCa => {
            let ca_pem = fs::read_to_string(&files.ca_pem).map_err(|source| {
                TlsSetupError::ReadMaterial {
                    path: files.ca_pem.clone(),
                    source,
                }
            })?;
            let ca_key_pem = fs::read_to_string(&files.ca_key).map_err(|source| {
                TlsSetupError::ReadMaterial {
                    path: files.ca_key.clone(),
                    source,
                }
            })?;
            let ca_key = rcgen::KeyPair::from_pem(&ca_key_pem)?;
            // Rebuild an issuer from the stored CA. Only the issuer name and
            // signing key matter for the minted certificate; the stored
            // ca.pem keeps being the one served and fingerprinted.
            let ca_cert =
                rcgen::CertificateParams::from_ca_cert_pem(&ca_pem)?.self_signed(&ca_key)?;
            (ca_cert, ca_key)
        }
    };

    let mut sans = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    for san in extra_sans {
        if !sans.contains(san) {
            sans.push(san.clone());
        }
    }
    let server_key = rcgen::KeyPair::generate()?;
    let mut server_params = rcgen::CertificateParams::new(sans)?;
    server_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "fibril-broker");
    let server_cert = server_params.signed_by(&server_key, &ca_cert, &ca_key)?;

    write_public(&files.server_pem, server_cert.pem().as_bytes())?;
    write_secret(&files.server_key, server_key.serialize_pem().as_bytes())?;
    Ok(())
}

fn write_public(path: &Path, contents: &[u8]) -> Result<(), TlsSetupError> {
    fs::write(path, contents).map_err(|source| TlsSetupError::WriteMaterial {
        path: path.to_path_buf(),
        source,
    })
}

fn write_secret(path: &Path, contents: &[u8]) -> Result<(), TlsSetupError> {
    write_public(path, contents)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|source| {
            TlsSetupError::WriteMaterial {
                path: path.to_path_buf(),
                source,
            }
        })?;
    }
    Ok(())
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsSetupError> {
    let file = fs::File::open(path).map_err(|source| TlsSetupError::ReadMaterial {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = io::BufReader::new(file);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsSetupError::ReadMaterial {
            path: path.to_path_buf(),
            source,
        })?;
    if certs.is_empty() {
        return Err(TlsSetupError::EmptyCertChain {
            path: path.to_path_buf(),
        });
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsSetupError> {
    let file = fs::File::open(path).map_err(|source| TlsSetupError::ReadMaterial {
        path: path.to_path_buf(),
        source,
    })?;
    let mut reader = io::BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|source| TlsSetupError::ReadMaterial {
            path: path.to_path_buf(),
            source,
        })?
        .ok_or_else(|| TlsSetupError::NoPrivateKey {
            path: path.to_path_buf(),
        })
}

/// Build the TLS connector for outbound peer connections (replication and
/// coordination dials). Trust resolution: an explicit peer CA file, else the
/// deployment's generated `<data_dir>/tls/ca.pem` when present, else OS
/// roots. Peer certificates are verified against the peer's host name, the
/// standard server verification - node membership is authenticated
/// separately by the cluster secret inside the session.
pub fn build_peer_connector(
    peer_ca_path: Option<&Path>,
    data_dir: &Path,
) -> Result<tokio_rustls::TlsConnector, TlsSetupError> {
    build_peer_connector_with_identity(peer_ca_path, data_dir, None)
}

/// [`build_peer_connector`] presenting a client certificate, for dialing
/// peers that require client auth. `identity` is the certificate chain and
/// key paths, typically the node's own server leaf.
pub fn build_peer_connector_with_identity(
    peer_ca_path: Option<&Path>,
    data_dir: &Path,
    identity: Option<(&Path, &Path)>,
) -> Result<tokio_rustls::TlsConnector, TlsSetupError> {
    let generated_ca = data_dir.join(GENERATED_TLS_DIR).join("ca.pem");
    let ca_path = match peer_ca_path {
        Some(path) => Some(path.to_path_buf()),
        None if generated_ca.exists() => Some(generated_ca),
        None => None,
    };

    let mut roots = rustls::RootCertStore::empty();
    if let Some(ca_path) = &ca_path {
        for cert in load_certs(ca_path)? {
            roots
                .add(cert)
                .map_err(|err| TlsSetupError::InvalidUploadedMaterial {
                    detail: format!("rejected CA certificate in {}: {err}", ca_path.display()),
                })?;
        }
    } else {
        for cert in rustls_native_certs::load_native_certs().certs {
            let _ = roots.add(cert);
        }
        if roots.is_empty() {
            return Err(TlsSetupError::InvalidUploadedMaterial {
                detail: "no OS trust roots available for peer TLS, set tls.peer_ca_path"
                    .to_string(),
            });
        }
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots);
    let config = match identity {
        None => config.with_no_client_auth(),
        Some((cert_path, key_path)) => {
            let certs = load_certs(cert_path)?;
            let key = load_key(key_path)?;
            config.with_client_auth_cert(certs, key)?
        }
    };
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// Validate uploaded PEM text (a certificate chain and its private key) and
/// store it under `<data_dir>/tls-provided` with a 0600 key. The pair is
/// proven to load as a working rustls config before anything is written.
pub fn store_provided_material(
    data_dir: &Path,
    cert_pem: &str,
    key_pem: &str,
) -> Result<(PathBuf, PathBuf), TlsSetupError> {
    let certs = rustls_pemfile::certs(&mut io::Cursor::new(cert_pem.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| TlsSetupError::InvalidUploadedMaterial {
            detail: format!("certificate is not valid PEM: {err}"),
        })?;
    if certs.is_empty() {
        return Err(TlsSetupError::InvalidUploadedMaterial {
            detail: "no certificates found in the uploaded chain".to_string(),
        });
    }
    let key = rustls_pemfile::private_key(&mut io::Cursor::new(key_pem.as_bytes()))
        .map_err(|err| TlsSetupError::InvalidUploadedMaterial {
            detail: format!("key is not valid PEM: {err}"),
        })?
        .ok_or_else(|| TlsSetupError::InvalidUploadedMaterial {
            detail: "no private key found in the uploaded PEM".to_string(),
        })?;
    server_config_from(certs, key).map_err(|err| TlsSetupError::InvalidUploadedMaterial {
        detail: format!("certificate and key do not form a working TLS config: {err}"),
    })?;

    let dir = data_dir.join(PROVIDED_TLS_DIR);
    fs::create_dir_all(&dir).map_err(|source| TlsSetupError::WriteMaterial {
        path: dir.clone(),
        source,
    })?;
    let cert_path = dir.join("server.pem");
    let key_path = dir.join("server.key");
    write_public(&cert_path, cert_pem.as_bytes())?;
    write_secret(&key_path, key_pem.as_bytes())?;
    Ok((cert_path, key_path))
}

fn server_config_from(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<Arc<RustlsServerConfig>, TlsSetupError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = RustlsServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    Ok(Arc::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fibril-tls-{tag}-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        fs::create_dir_all(&dir).expect("temp dir");
        dir
    }

    #[test]
    fn auto_self_signed_generates_material_and_reuses_it() {
        let root = temp_dir("auto");
        let first = build_server_tls(&TlsMode::AutoSelfSigned, &root, &["broker-1".into()])
            .expect("generate")
            .expect("enabled");
        let TlsMaterialSource::Generated {
            dir,
            ca_fingerprint,
        } = &first.source
        else {
            panic!("expected generated source");
        };
        assert!(dir.join("ca.pem").exists());
        assert!(dir.join("server.key").exists());
        assert_eq!(ca_fingerprint.len(), 32 * 3 - 1);

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(dir.join("server.key"))
                .expect("key metadata")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // A second boot loads the same material instead of regenerating.
        let second = build_server_tls(&TlsMode::AutoSelfSigned, &root, &[])
            .expect("reload")
            .expect("enabled");
        let TlsMaterialSource::Generated {
            ca_fingerprint: second_fp,
            ..
        } = &second.source
        else {
            panic!("expected generated source");
        };
        assert_eq!(ca_fingerprint, second_fp);
    }

    #[test]
    fn partial_generated_material_is_refused() {
        let root = temp_dir("partial");
        let dir = root.join(GENERATED_TLS_DIR);
        fs::create_dir_all(&dir).expect("dir");
        fs::write(dir.join("ca.pem"), "not a cert").expect("stray file");

        let Err(err) = build_server_tls(&TlsMode::AutoSelfSigned, &root, &[]) else {
            panic!("partial material must be refused");
        };
        assert!(err.to_string().contains("incomplete"), "{err}");
    }

    #[test]
    fn provided_mode_names_the_missing_file() {
        let root = temp_dir("missing");
        let Err(err) = build_server_tls(
            &TlsMode::Provided {
                cert_path: root.join("nope.pem"),
                key_path: root.join("nope.key"),
            },
            &root,
            &[],
        ) else {
            panic!("missing PEM files must be refused");
        };
        assert!(err.to_string().contains("tls.cert_path"), "{err}");
    }

    #[test]
    fn provided_mode_serves_generated_style_material() {
        // Generate material, then load it back through the Provided path the
        // way an operator supplying PEM files would.
        let root = temp_dir("roundtrip");
        build_server_tls(&TlsMode::AutoSelfSigned, &root, &[])
            .expect("generate")
            .expect("enabled");
        let dir = root.join(GENERATED_TLS_DIR);
        let provided = build_server_tls(
            &TlsMode::Provided {
                cert_path: dir.join("server.pem"),
                key_path: dir.join("server.key"),
            },
            &root,
            &[],
        )
        .expect("load")
        .expect("enabled");
        assert!(matches!(provided.source, TlsMaterialSource::Provided));
    }

    #[test]
    fn generation_plan_matrix_is_total() {
        // (ca.pem, ca.key, server.pem, server.key) -> outcome, all 16.
        let cases = [
            (
                (false, false, false, false),
                Some(GeneratedPlan::GenerateAll),
            ),
            ((false, false, false, true), None),
            ((false, false, true, false), None),
            ((false, false, true, true), None),
            ((false, true, false, false), None),
            ((false, true, false, true), None),
            ((false, true, true, false), None),
            ((false, true, true, true), None),
            ((true, false, false, false), None),
            ((true, false, false, true), None),
            ((true, false, true, false), None),
            ((true, false, true, true), None),
            (
                (true, true, false, false),
                Some(GeneratedPlan::MintServerFromCa),
            ),
            ((true, true, false, true), None),
            ((true, true, true, false), None),
            ((true, true, true, true), Some(GeneratedPlan::Reuse)),
        ];
        for ((ca_pem, ca_key, server_pem, server_key), expected) in cases {
            assert_eq!(
                generated_plan(ca_pem, ca_key, server_pem, server_key),
                expected,
                "matrix case ({ca_pem}, {ca_key}, {server_pem}, {server_key})"
            );
        }
    }

    #[test]
    fn shared_ca_pair_mints_a_server_certificate() {
        // Node A generates the deployment CA; node B receives only the CA
        // pair (the shared-CA cluster lane) and must mint its own leaf.
        let node_a = temp_dir("mint-a");
        build_server_tls(&TlsMode::AutoSelfSigned, &node_a, &["broker-a".into()])
            .expect("generate on a")
            .expect("enabled");
        let a_dir = node_a.join(GENERATED_TLS_DIR);

        let node_b = temp_dir("mint-b");
        let b_dir = node_b.join(GENERATED_TLS_DIR);
        fs::create_dir_all(&b_dir).expect("b dir");
        fs::copy(a_dir.join("ca.pem"), b_dir.join("ca.pem")).expect("copy ca.pem");
        fs::copy(a_dir.join("ca.key"), b_dir.join("ca.key")).expect("copy ca.key");

        let b = build_server_tls(&TlsMode::AutoSelfSigned, &node_b, &["broker-b".into()])
            .expect("mint on b")
            .expect("enabled");

        // Same CA, distinct leaf.
        let a_fp = ca_fingerprint(&a_dir.join("ca.pem")).expect("a fp");
        let TlsMaterialSource::Generated {
            ca_fingerprint: b_fp,
            ..
        } = &b.source
        else {
            panic!("expected generated source");
        };
        assert_eq!(&a_fp, b_fp);
        assert_ne!(
            fs::read(a_dir.join("server.pem")).expect("a leaf"),
            fs::read(b_dir.join("server.pem")).expect("b leaf"),
        );

        // B's minted chain verifies against the shared CA root.
        let mut roots = rustls::RootCertStore::empty();
        for cert in load_certs(&b_dir.join("ca.pem")).expect("ca certs") {
            roots.add(cert).expect("add root");
        }
        let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::new(rustls::crypto::ring::default_provider()),
        )
        .build()
        .expect("verifier");
        let chain = load_certs(&b_dir.join("server.pem")).expect("b chain");
        use rustls::client::danger::ServerCertVerifier as _;
        verifier
            .verify_server_cert(
                &chain[0],
                &chain[1..],
                &rustls::pki_types::ServerName::try_from("broker-b").expect("name"),
                &[],
                rustls::pki_types::UnixTime::now(),
            )
            .expect("minted chain must verify against the shared CA");
    }

    #[test]
    fn reload_swaps_the_leaf_and_rejects_bad_material() {
        // Two generated material sets stand in for a certificate rotation.
        let first = temp_dir("rotate-first");
        build_server_tls(&TlsMode::AutoSelfSigned, &first, &[])
            .expect("first material")
            .expect("enabled");
        let second = temp_dir("rotate-second");
        build_server_tls(&TlsMode::AutoSelfSigned, &second, &[])
            .expect("second material")
            .expect("enabled");
        let first_dir = first.join(GENERATED_TLS_DIR);
        let second_dir = second.join(GENERATED_TLS_DIR);

        // Serve the first set through Provided mode paths in a live dir.
        let live = temp_dir("rotate-live");
        let cert_path = live.join("server.pem");
        let key_path = live.join("server.key");
        fs::copy(first_dir.join("server.pem"), &cert_path).expect("stage cert");
        fs::copy(first_dir.join("server.key"), &key_path).expect("stage key");
        let tls = build_server_tls(
            &TlsMode::Provided {
                cert_path: cert_path.clone(),
                key_path: key_path.clone(),
            },
            &live,
            &[],
        )
        .expect("serve")
        .expect("enabled");
        let before = tls.leaf_fingerprint();

        // Rotate: replace the files, reload, observe the new leaf.
        fs::copy(second_dir.join("server.pem"), &cert_path).expect("rotate cert");
        fs::copy(second_dir.join("server.key"), &key_path).expect("rotate key");
        let reported = tls.reload().expect("reload");
        assert_ne!(before, reported);
        assert_eq!(reported, tls.leaf_fingerprint());

        // A mismatched pair (new cert, garbage key) is rejected and the
        // previous material keeps serving.
        fs::write(&key_path, "not a key").expect("break key");
        let err = tls.reload().expect_err("bad material must be rejected");
        assert!(err.to_string().contains("private key"), "{err}");
        assert_eq!(reported, tls.leaf_fingerprint());

        // A key that parses but does not match the certificate is also
        // rejected without a swap.
        fs::copy(first_dir.join("server.key"), &key_path).expect("mismatched key");
        assert!(tls.reload().is_err(), "mismatched pair must be rejected");
        assert_eq!(reported, tls.leaf_fingerprint());
    }

    #[test]
    fn client_identity_prefers_dns_san_then_cn_and_never_maps_node() {
        let key = rcgen::KeyPair::generate().expect("key");

        let mut with_san =
            rcgen::CertificateParams::new(vec!["orders-service".to_string()]).expect("params");
        with_san
            .distinguished_name
            .push(rcgen::DnType::CommonName, "cn-name");
        let cert = with_san.self_signed(&key).expect("cert");
        assert_eq!(
            client_identity_from_cert(cert.der()),
            Some("orders-service".to_string())
        );

        let mut cn_only = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        cn_only
            .distinguished_name
            .push(rcgen::DnType::CommonName, "worker-7");
        let cert = cn_only.self_signed(&key).expect("cert");
        assert_eq!(
            client_identity_from_cert(cert.der()),
            Some("worker-7".to_string())
        );

        let mut node_claim = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        node_claim
            .distinguished_name
            .push(rcgen::DnType::CommonName, "@node");
        let cert = node_claim.self_signed(&key).expect("cert");
        assert_eq!(client_identity_from_cert(cert.der()), None);

        let mut unnamed = rcgen::CertificateParams::new(Vec::<String>::new()).expect("params");
        unnamed.distinguished_name = rcgen::DistinguishedName::new();
        let cert = unnamed.self_signed(&key).expect("cert");
        assert_eq!(client_identity_from_cert(cert.der()), None);
    }

    async fn tls_round_trip(
        connector: &tokio_rustls::TlsConnector,
        addr: std::net::SocketAddr,
    ) -> io::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let tcp = tokio::net::TcpStream::connect(addr).await?;
        let name = rustls::pki_types::ServerName::try_from("localhost").expect("name");
        let mut stream = connector.connect(name, tcp).await?;
        stream.write_all(b"x").await?;
        stream.flush().await?;
        let mut buf = [0u8; 1];
        stream.read_exact(&mut buf).await?;
        Ok(())
    }

    async fn echo_server(acceptor: TlsAcceptor) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    break;
                };
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(mut stream) = acceptor.accept(tcp).await {
                        let mut buf = [0u8; 1];
                        if stream.read_exact(&mut buf).await.is_ok() {
                            let _ = stream.write_all(&buf).await;
                            let _ = stream.flush().await;
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn client_auth_modes_gate_the_handshake() {
        let root = temp_dir("client-auth");
        let require = build_server_tls_with_client_auth(
            &TlsMode::AutoSelfSigned,
            &root,
            &[],
            &ClientAuthPolicy {
                mode: ClientAuthMode::Require,
                ca_path: None,
            },
        )
        .expect("require build")
        .expect("enabled");
        let require_addr = echo_server(require.acceptor.clone()).await;

        let certless = build_peer_connector(None, &root).expect("certless connector");
        assert!(
            tls_round_trip(&certless, require_addr).await.is_err(),
            "require mode must reject a certless client"
        );

        let tls_dir = root.join(GENERATED_TLS_DIR);
        let with_cert = build_peer_connector_with_identity(
            None,
            &root,
            Some((&tls_dir.join("server.pem"), &tls_dir.join("server.key"))),
        )
        .expect("cert connector");
        tls_round_trip(&with_cert, require_addr)
            .await
            .expect("a deployment-CA certificate passes require mode");

        // Request mode admits both lanes.
        let request = build_server_tls_with_client_auth(
            &TlsMode::AutoSelfSigned,
            &root,
            &[],
            &ClientAuthPolicy {
                mode: ClientAuthMode::Request,
                ca_path: None,
            },
        )
        .expect("request build")
        .expect("enabled");
        let request_addr = echo_server(request.acceptor.clone()).await;
        tls_round_trip(&certless, request_addr)
            .await
            .expect("request mode admits certless clients");
        tls_round_trip(&with_cert, request_addr)
            .await
            .expect("request mode admits cert-bearing clients");
    }

    #[test]
    fn client_auth_without_any_ca_is_a_guided_error() {
        let root = temp_dir("client-auth-no-ca");
        // Provided-mode material without generated files: no client CA to
        // fall back to.
        let material = temp_dir("client-auth-material");
        build_server_tls(&TlsMode::AutoSelfSigned, &material, &[])
            .expect("material")
            .expect("enabled");
        let material_dir = material.join(GENERATED_TLS_DIR);
        let Err(err) = build_server_tls_with_client_auth(
            &TlsMode::Provided {
                cert_path: material_dir.join("server.pem"),
                key_path: material_dir.join("server.key"),
            },
            &root,
            &[],
            &ClientAuthPolicy {
                mode: ClientAuthMode::Require,
                ca_path: None,
            },
        ) else {
            panic!("client auth without a CA must be refused");
        };
        assert!(err.to_string().contains("client_ca_path"), "{err}");
    }

    #[test]
    fn san_hosts_strip_ports_and_brackets() {
        let hosts = san_hosts_from_advertise(&[
            "broker-1:9876".to_string(),
            "[::1]:9876".to_string(),
            "10.0.0.5:9876".to_string(),
        ]);
        assert_eq!(hosts, vec!["broker-1", "::1", "10.0.0.5"]);
    }
}
