//! Deterministic-simulation smoke test (task #97, plan step 1): confirm turmoil
//! builds and runs in our toolchain before any broker net-seam work. Two
//! simulated hosts exchange a frame over `turmoil::net`. This is the foothold the
//! cluster failure-path scenarios grow from; see the deterministic-simulation
//! development note.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use turmoil::net::{TcpListener, TcpStream};

#[test]
fn turmoil_two_hosts_exchange_a_frame() {
    let mut sim = turmoil::Builder::new().build();

    sim.host("server", || async {
        let listener = TcpListener::bind(("0.0.0.0", 9999)).await?;
        let (mut stream, _peer) = listener.accept().await?;
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await?;
        stream.write_all(&buf).await?;
        Ok(())
    });

    sim.client("client", async {
        let mut stream = TcpStream::connect(("server", 9999)).await?;
        stream.write_all(b"ping").await?;
        let mut buf = [0u8; 4];
        stream.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"ping");
        Ok(())
    });

    sim.run().expect("simulation runs to completion");
}
