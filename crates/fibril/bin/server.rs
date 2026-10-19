use fibril::run_server_from_config;
use fibril_config::ServerConfig;
use fibril_util::init_tracing;
use mimalloc::MiMalloc;

#[cfg(all(tokio_unstable, feature = "task-polls"))]
mod task_polls;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[cfg(not(all(tokio_unstable, feature = "task-polls")))]
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
        .build()?;
    task_polls::start_writer();
    runtime.block_on(async {
        init_tracing();
        Ok(run_server_from_config(ServerConfig::load()?).await?)
    })
}
