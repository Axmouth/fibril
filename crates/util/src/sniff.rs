//! First-bytes connection sniffing.
//!
//! A listener that serves the wrong transport fails badly by default: a TLS
//! ClientHello read by the plaintext frame codec parses its leading bytes as
//! a huge payload length and waits forever, and a plaintext frame sent to a
//! TLS acceptor fails as an opaque handshake error. Sniffing the first two
//! bytes lets the accept path name the mismatch in both directions before
//! handing the stream (with nothing consumed) to the real transport.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};

/// A TLS ClientHello record starts with content-type 0x16 (handshake) and
/// record version major 0x03.
pub fn looks_like_tls_client_hello(prefix: &[u8]) -> bool {
    prefix.len() >= 2 && prefix[0] == 0x16 && prefix[1] == 0x03
}

/// A plaintext protocol frame starts with a u32 payload length. The first
/// frame on a connection is a small HELLO, so its two leading length bytes
/// are always zero, which no TLS record starts with.
pub fn looks_like_plaintext_frame(prefix: &[u8]) -> bool {
    prefix.len() >= 2 && prefix[0] == 0x00 && prefix[1] == 0x00
}

/// Read up to `N` bytes from the stream, returning fewer only on EOF.
pub async fn sniff_first_bytes<S: AsyncRead + Unpin, const N: usize>(
    stream: &mut S,
) -> io::Result<(usize, [u8; N])> {
    let mut buf = [0u8; N];
    let mut filled = 0;
    while filled < N {
        let n = stream.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok((filled, buf))
}

/// A stream that replays sniffed prefix bytes before reading from the inner
/// stream, so sniffing consumes nothing from the protocol's point of view.
pub struct PrefixedStream<S> {
    prefix: Vec<u8>,
    pos: usize,
    inner: S,
}

impl<S> PrefixedStream<S> {
    pub fn new(prefix: Vec<u8>, inner: S) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PrefixedStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if this.pos < this.prefix.len() {
            let remaining = &this.prefix[this.pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.pos += n;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PrefixedStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn prefixed_stream_replays_prefix_then_reads_inner() {
        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(b"cd").await.unwrap();

        let mut stream = PrefixedStream::new(b"ab".to_vec(), server);
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"abcd");
    }

    #[tokio::test]
    async fn prefixed_stream_serves_prefix_across_small_reads() {
        let (mut client, server) = tokio::io::duplex(64);
        client.write_all(b"c").await.unwrap();

        let mut stream = PrefixedStream::new(b"ab".to_vec(), server);
        let mut one = [0u8; 1];
        stream.read_exact(&mut one).await.unwrap();
        assert_eq!(&one, b"a");
        stream.read_exact(&mut one).await.unwrap();
        assert_eq!(&one, b"b");
        stream.read_exact(&mut one).await.unwrap();
        assert_eq!(&one, b"c");
    }

    #[tokio::test]
    async fn prefixed_stream_writes_pass_through() {
        let (mut client, server) = tokio::io::duplex(64);
        let mut stream = PrefixedStream::new(b"xy".to_vec(), server);
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();
        let mut buf = [0u8; 4];
        client.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
    }

    #[tokio::test]
    async fn sniff_reads_n_bytes_or_stops_at_eof() {
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(b"ab").await.unwrap();
        drop(client);
        let (n, buf) = sniff_first_bytes::<_, 4>(&mut server).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..n], b"ab");
    }

    #[test]
    fn classifies_client_hello_and_plaintext_frames() {
        assert!(looks_like_tls_client_hello(&[0x16, 0x03, 0x01]));
        assert!(!looks_like_tls_client_hello(&[0x00, 0x00]));
        assert!(looks_like_plaintext_frame(&[0x00, 0x00, 0x00, 0x2a]));
        assert!(!looks_like_plaintext_frame(&[0x16, 0x03]));
        assert!(!looks_like_plaintext_frame(&[0x00]));
    }
}
