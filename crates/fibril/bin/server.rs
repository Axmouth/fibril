use fibril::run_server_from_config;
use fibril_config::ServerConfig;
use fibril_util::init_tracing;
#[cfg(not(feature = "heap-profile"))]
use mimalloc::MiMalloc;

#[cfg(all(tokio_unstable, feature = "task-polls"))]
mod task_polls;

#[cfg(not(feature = "heap-profile"))]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(feature = "heap-profile")]
#[global_allocator]
static GLOBAL: dhat::Alloc = dhat::Alloc;

/// Diagnostic build: same server under the dhat allocator. SIGTERM writes
/// the heap profile, with peak live bytes by allocation site, to
/// `FIBRIL_HEAP_PROFILE_PATH` (default `dhat-heap.json`) and exits.
#[cfg(feature = "heap-profile")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let path = std::env::var("FIBRIL_HEAP_PROFILE_PATH").unwrap_or_else(|_| "dhat-heap.json".into());
    let profiler = dhat::Profiler::builder().file_name(path).build();
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        result = run_server_from_config(ServerConfig::load()?) => result?,
        _ = term.recv() => {}
    }
    drop(profiler);
    std::process::exit(0)
}

#[cfg(not(any(all(tokio_unstable, feature = "task-polls"), feature = "heap-profile")))]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    Ok(run_server_from_config(ServerConfig::load()?).await?)
}

/// Diagnostic build: same server, with task polls counted by spawn site.
#[cfg(all(tokio_unstable, feature = "task-polls"))]
fn main() -> anyhow::Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .on_before_task_poll(task_polls::count)
        .on_after_task_poll(task_polls::finish)
        .build()?;
    task_polls::start_writer();
    runtime.block_on(async {
        init_tracing();
        Ok(run_server_from_config(ServerConfig::load()?).await?)
    })
}
