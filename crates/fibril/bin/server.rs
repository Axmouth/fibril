use fibril::run_server_from_config;
use fibril_config::ServerConfig;
use fibril_util::init_tracing;
#[cfg(not(any(
    feature = "system-allocator-experiment",
    feature = "jemalloc-allocator-experiment"
)))]
use mimalloc::MiMalloc;

#[cfg(not(any(
    feature = "system-allocator-experiment",
    feature = "jemalloc-allocator-experiment"
)))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "jemalloc-allocator-experiment")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    #[cfg(feature = "jemalloc-allocator-experiment")]
    install_jemalloc_probe()?;
    #[cfg(feature = "system-allocator-experiment")]
    tracing::warn!("ALLOCATOR_SYSTEM_CONTROL active");
    #[cfg(all(unix, feature = "mimalloc-collection-experiment"))]
    install_collection_probe()?;
    Ok(run_server_from_config(ServerConfig::load()?).await?)
}

#[cfg(all(
    feature = "mimalloc-collection-experiment",
    any(
        feature = "system-allocator-experiment",
        feature = "jemalloc-allocator-experiment"
    )
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

#[cfg(all(
    feature = "system-allocator-experiment",
    feature = "jemalloc-allocator-experiment"
))]
compile_error!("select one diagnostic allocator");

#[cfg(feature = "jemalloc-allocator-experiment")]
fn install_jemalloc_probe() -> anyhow::Result<()> {
    use tikv_jemalloc_ctl::{background_thread, opt, version};
    let background = std::env::var("FIBRIL_JEMALLOC_BACKGROUND").as_deref() == Ok("1");
    background_thread::write(background)
        .map_err(|e| anyhow::anyhow!("jemalloc background control: {e}"))?;
    let actual =
        background_thread::read().map_err(|e| anyhow::anyhow!("jemalloc background read: {e}"))?;
    anyhow::ensure!(actual == background, "jemalloc background state mismatch");
    tracing::warn!(
        "ALLOCATOR_JEMALLOC_CONTROL active background={} version={:?} narenas={:?}",
        actual,
        version::read(),
        opt::narenas::read()
    );
    Ok(())
}
