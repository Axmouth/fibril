use fibril::run_server_from_config;
use fibril_config::ServerConfig;
use fibril_util::init_tracing;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    Ok(run_server_from_config(ServerConfig::load()?).await?)
}
