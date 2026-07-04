//! TLS transport for the embedded coordinator's raft channel: the dialer
//! and acceptor injected into ganglion's transport seams, built from the
//! broker's own TLS material. Ganglion itself stays TLS-free.

use std::io;

use fibril_tls::tokio_rustls::{self, TlsAcceptor, TlsConnector};
use ganglion::{RaftAcceptor, RaftDialer};
use tokio::net::TcpStream;

/// Dials raft peers over TLS, verifying their certificate against the
/// peer-CA trust the connector was built with.
pub struct TlsRaftDialer {
    connector: TlsConnector,
}

impl TlsRaftDialer {
    pub fn new(connector: TlsConnector) -> Self {
        Self { connector }
    }
}

impl RaftDialer for TlsRaftDialer {
    type Stream = tokio_rustls::client::TlsStream<TcpStream>;

    async fn dial(&self, addr: &str) -> io::Result<Self::Stream> {
        let tcp = TcpStream::connect(addr).await?;
        let host = addr
            .rsplit_once(':')
            .map(|(host, _)| host)
            .unwrap_or(addr)
            .trim_matches(['[', ']'])
            .to_string();
        let name = tokio_rustls::rustls::pki_types::ServerName::try_from(host).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid TLS server name from coordination peer address `{addr}`: {err}"),
            )
        })?;
        self.connector
            .connect(name, tcp)
            .await
            .map_err(|err| name_peer_handshake_error(err, addr))
    }
}

/// Name a failed peer handshake so the raft dial log guides the fix instead
/// of surfacing an opaque IO error.
fn name_peer_handshake_error(err: io::Error, addr: &str) -> io::Error {
    if matches!(
        err.kind(),
        io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
    ) {
        return io::Error::new(
            err.kind(),
            format!(
                "coordination peer at {addr} closed the connection during the TLS handshake: \
                 it is likely serving plaintext. Enable tls on that node or set \
                 tls.inter_broker = false"
            ),
        );
    }
    io::Error::new(
        err.kind(),
        format!("coordination peer TLS handshake with {addr} failed: {err}"),
    )
}

/// Runs the server-side TLS handshake on accepted raft connections, serving
/// the broker's certificate.
pub struct TlsRaftAcceptor {
    acceptor: TlsAcceptor,
}

impl TlsRaftAcceptor {
    pub fn new(acceptor: TlsAcceptor) -> Self {
        Self { acceptor }
    }
}

impl RaftAcceptor for TlsRaftAcceptor {
    type Stream = tokio_rustls::server::TlsStream<TcpStream>;

    async fn accept(&self, stream: TcpStream) -> io::Result<Self::Stream> {
        self.acceptor.accept(stream).await.map_err(|err| {
            // Ganglion drops the connection silently on a failed wrap, so
            // this is where the operator sees why (a plaintext peer or an
            // untrusted certificate).
            tracing::warn!(
                "coordination TLS accept failed: {err}. A peer is likely serving plaintext \
                 (tls.inter_broker mismatch) or presents a certificate outside the peer CA"
            );
            err
        })
    }
}
