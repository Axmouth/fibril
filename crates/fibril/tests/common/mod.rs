//! Shared broker-boot support for the integration tests. Boots here are
//! hardened against the freed-ephemeral-port race: the tests pick ports by
//! binding :0 and releasing, so a parallel test (or another test binary in a
//! workspace run) can grab the port before the server binds it. The server
//! then dies with AddrInUse - which used to be swallowed and surface as a 30s
//! "did not come up" flake. Boots retry with fresh ports on that collision
//! and surface any other boot error immediately.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use fibril::run_server_from_config;
use fibril_config::ServerConfig;
use tokio::net::TcpStream;

pub fn temp_root(prefix: &str, tag: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!(
        "{prefix}-{tag}-{}-{}",
        std::process::id(),
        fastrand::u64(..)
    ));
    std::fs::create_dir_all(&root).expect("temp root");
    root
}

pub fn free_loopback_addr() -> std::net::SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    listener.local_addr().expect("local addr")
}

/// Cap on concurrently-booting servers per test binary, so a parallel suite
/// does not churn loopback ports (more churn, more collisions to retry) or
/// oversubscribe the machine.
pub static BOOT_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

pub struct BootedServer {
    pub broker_addr: std::net::SocketAddr,
    pub admin_addr: std::net::SocketAddr,
    pub data_dir: PathBuf,
    pub handle: tokio::task::JoinHandle<()>,
    // Held for the whole test so the concurrent-boot cap covers the server's
    // lifetime, not just its startup.
    pub boot_permit: tokio::sync::SemaphorePermit<'static>,
}

/// Boot a standalone broker, retrying with fresh ports when a parallel test
/// grabbed one first. `wait_admin` also gates readiness on the admin
/// listener answering.
pub async fn boot_server(
    prefix: &str,
    tag: &str,
    wait_admin: bool,
    configure: impl Fn(&mut ServerConfig),
) -> BootedServer {
    let boot_permit = BOOT_PERMITS.acquire().await.expect("boot permit");
    for _ in 0..12 {
        let root = temp_root(prefix, tag);
        let mut config = ServerConfig::default();
        config.server.data_dir = root.join("data");
        config.broker.listener.bind = free_loopback_addr();
        config.admin.listener.bind = free_loopback_addr();
        configure(&mut config);
        let broker_addr = config.broker.listener.bind;
        let admin_addr = config.admin.listener.bind;
        let data_dir = config.server.data_dir.clone();

        let boot_error = Arc::new(std::sync::Mutex::new(None));
        let sink = boot_error.clone();
        let handle = tokio::spawn(async move {
            if let Err(e) = run_server_from_config(config).await {
                *sink.lock().unwrap() = Some(format!("{e:?}"));
            }
        });

        let mut ready = false;
        for _ in 0..1200 {
            if handle.is_finished() {
                break;
            }
            let broker_up = TcpStream::connect(broker_addr).await.is_ok();
            let admin_up = !wait_admin || TcpStream::connect(admin_addr).await.is_ok();
            if broker_up && admin_up {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        if ready {
            return BootedServer {
                broker_addr,
                admin_addr,
                data_dir,
                handle,
                boot_permit,
            };
        }
        if handle.is_finished() {
            let err = boot_error.lock().unwrap().take().unwrap_or_default();
            if err.contains("AddrInUse") {
                continue;
            }
            panic!("server exited during boot: {err}");
        }
        handle.abort();
        panic!("broker listener did not come up within the readiness budget");
    }
    panic!("broker could not bind free ports after repeated collisions");
}
