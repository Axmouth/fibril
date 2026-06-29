//! Network type seam for deterministic simulation (task #97).
//!
//! Normal builds re-export tokio's TCP types. Under the `simulation` feature they
//! flip to turmoil's simulated equivalents, which expose the same API for
//! everything the broker uses (`connect`, `bind`, `accept`, `set_nodelay` as a
//! no-op, `peer_addr`/`local_addr`, `into_split`). Because the swap lives in this
//! one re-export, call sites import from here and need no per-site `cfg` - the
//! whole stack can be built inside a turmoil `Sim` by enabling one feature.

#[cfg(not(feature = "simulation"))]
pub use tokio::net::{
    TcpListener, TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};

#[cfg(feature = "simulation")]
pub use turmoil::net::{
    TcpListener, TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};

#[cfg(test)]
mod tests {
    use super::{TcpListener, TcpStream};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Default (tokio) path of the seam: a real loopback exchange compiles and
    /// runs through `fibril_util::net` types.
    #[cfg(not(feature = "simulation"))]
    #[tokio::test]
    async fn seam_tokio_loopback_roundtrips() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await.unwrap();
            s.write_all(&buf).await.unwrap();
        });
        let mut c = TcpStream::connect(addr).await.unwrap();
        c.set_nodelay(true).unwrap();
        c.write_all(b"ping").await.unwrap();
        let mut buf = [0u8; 4];
        c.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"ping");
        server.await.unwrap();
    }

    /// Simulation (turmoil) path of the seam: the same `fibril_util::net` types,
    /// inside a turmoil Sim, exchange a frame deterministically.
    #[cfg(feature = "simulation")]
    #[test]
    fn seam_turmoil_sim_roundtrips() {
        let mut sim = turmoil::Builder::new().build();
        sim.host("server", || async {
            let listener = TcpListener::bind(("0.0.0.0", 9999)).await?;
            let (mut s, _) = listener.accept().await?;
            let mut buf = [0u8; 4];
            s.read_exact(&mut buf).await?;
            s.write_all(&buf).await?;
            Ok(())
        });
        sim.client("client", async {
            let mut c = TcpStream::connect(("server", 9999)).await?;
            c.set_nodelay(true)?;
            c.write_all(b"ping").await?;
            let mut buf = [0u8; 4];
            c.read_exact(&mut buf).await?;
            assert_eq!(&buf, b"ping");
            Ok(())
        });
        sim.run().expect("simulation runs to completion");
    }
}
