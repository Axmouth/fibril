//! Broker TLS material: operator-supplied PEM files or per-deployment
//! material generated under the data dir.
//!
//! Fibril never ships certificates. The generated path follows the
//! Elasticsearch 8 pattern: a per-deployment CA and server certificate are
//! created on first boot and the CA fingerprint is printed so clients can
//! pin or trust it.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fibril_config::TlsMode;
use sha2::{Digest, Sha256};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{self, ServerConfig as RustlsServerConfig};

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
         server.pem and server.key together. Remove the directory to regenerate, or \
         supply tls.cert_path and tls.key_path instead"
    )]
    PartialGeneratedMaterial { dir: PathBuf },
    #[error("uploaded TLS material rejected: {detail}")]
    InvalidUploadedMaterial { detail: String },
}

/// The acceptor plus where its material came from, for startup reporting.
/// `server_config` is shared so the admin HTTP server can serve HTTPS from
/// the same material.
pub struct ServerTls {
    pub acceptor: TlsAcceptor,
    pub server_config: Arc<RustlsServerConfig>,
    pub source: TlsMaterialSource,
}

pub enum TlsMaterialSource {
    Provided,
    Generated {
        dir: PathBuf,
        ca_fingerprint: String,
    },
}

/// Build the broker TLS acceptor for the configured mode. `extra_sans` adds
/// hostnames or IPs (typically the advertise hosts) to a generated server
/// certificate on top of the localhost set.
pub fn build_server_tls(
    mode: &TlsMode,
    data_dir: &Path,
    extra_sans: &[String],
) -> Result<Option<ServerTls>, TlsSetupError> {
    match mode {
        TlsMode::Disabled => Ok(None),
        TlsMode::Provided {
            cert_path,
            key_path,
        } => {
            let certs = load_certs(cert_path)?;
            let key = load_key(key_path)?;
            let server_config = server_config_from(certs, key)?;
            Ok(Some(ServerTls {
                acceptor: TlsAcceptor::from(server_config.clone()),
                server_config,
                source: TlsMaterialSource::Provided,
            }))
        }
        TlsMode::AutoSelfSigned => {
            let dir = data_dir.join(GENERATED_TLS_DIR);
            ensure_generated_material(&dir, extra_sans)?;
            let files = GeneratedFiles::new(&dir);
            // Serve leaf + CA so a client pinning the CA fingerprint can
            // match it against the presented chain.
            let mut certs = load_certs(&files.server_pem)?;
            certs.extend(load_certs(&files.ca_pem)?);
            let key = load_key(&files.server_key)?;
            let ca_fingerprint = ca_fingerprint(&files.ca_pem)?;
            let server_config = server_config_from(certs, key)?;
            Ok(Some(ServerTls {
                acceptor: TlsAcceptor::from(server_config.clone()),
                server_config,
                source: TlsMaterialSource::Generated {
                    dir,
                    ca_fingerprint,
                },
            }))
        }
    }
}

/// SHA-256 fingerprint of the first certificate in a PEM file, colon-hex.
pub fn ca_fingerprint(ca_pem: &Path) -> Result<String, TlsSetupError> {
    let certs = load_certs(ca_pem)?;
    let digest = Sha256::digest(certs[0].as_ref());
    Ok(digest
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect::<Vec<_>>()
        .join(":"))
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

/// Generate the CA and server certificate if the directory holds none.
/// Complete material is reused as is, partial material is refused rather
/// than guessed at.
fn ensure_generated_material(dir: &Path, extra_sans: &[String]) -> Result<(), TlsSetupError> {
    let files = GeneratedFiles::new(dir);
    let present = [
        files.ca_pem.exists(),
        files.ca_key.exists(),
        files.server_pem.exists(),
        files.server_key.exists(),
    ];
    if present.iter().all(|p| *p) {
        return Ok(());
    }
    if present.iter().any(|p| *p) {
        return Err(TlsSetupError::PartialGeneratedMaterial {
            dir: dir.to_path_buf(),
        });
    }

    let ca_key = rcgen::KeyPair::generate()?;
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Fibril per-deployment CA");
    let ca_cert = ca_params.self_signed(&ca_key)?;

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

    fs::create_dir_all(dir).map_err(|source| TlsSetupError::WriteMaterial {
        path: dir.to_path_buf(),
        source,
    })?;
    write_public(&files.ca_pem, ca_cert.pem().as_bytes())?;
    write_secret(&files.ca_key, ca_key.serialize_pem().as_bytes())?;
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
    fn san_hosts_strip_ports_and_brackets() {
        let hosts = san_hosts_from_advertise(&[
            "broker-1:9876".to_string(),
            "[::1]:9876".to_string(),
            "10.0.0.5:9876".to_string(),
        ]);
        assert_eq!(hosts, vec!["broker-1", "::1", "10.0.0.5"]);
    }
}
