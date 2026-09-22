use fibril::run_server_from_config;
use fibril_config::ServerConfig;
use fibril_util::init_tracing;
#[cfg(not(feature = "system-allocator-experiment"))]
use mimalloc::MiMalloc;

#[cfg(not(feature = "system-allocator-experiment"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    #[cfg(feature = "system-allocator-experiment")]
    tracing::warn!("ALLOCATOR_SYSTEM_CONTROL active");
    #[cfg(all(unix, feature = "mimalloc-collection-experiment"))]
    install_collection_probe()?;
    Ok(run_server_from_config(ServerConfig::load()?).await?)
}

#[cfg(all(
    feature = "mimalloc-collection-experiment",
    feature = "system-allocator-experiment"
))]
compile_error!("mimalloc collection requires the mimalloc global allocator");

#[cfg(all(unix, feature = "mimalloc-collection-experiment"))]
fn install_collection_probe() -> std::io::Result<()> {
    use fibril_broker::queue_engine::allocator_probe;
    use tokio::signal::unix::{SignalKind, signal};
    if !allocator_probe::enabled() {
        return Ok(());
    }
    let mut central = signal(SignalKind::user_defined1())?;
    let mut writers = signal(SignalKind::user_defined2())?;
    tokio::spawn(async move {
        while central.recv().await.is_some() {
            tokio::task::spawn_blocking(|| allocator_probe::collect_current_thread("central"))
                .await
                .expect("collection probe panicked");
        }
    });
    tokio::spawn(async move {
        while writers.recv().await.is_some() {
            allocator_probe::request_writer_collection();
        }
    });
    Ok(())
}
