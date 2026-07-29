use std::sync::Arc;

use rudis::config::Config;
use rudis::runtime::bootstrap;
use rudis::runtime::shutdown;
use rudis::telemetry;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let config = match Config::load() {
        Ok(c) => Arc::new(c),
        Err(e) => {
            eprintln!("failed to load config: {e}");
            std::process::exit(1);
        }
    };

    telemetry::init_tracing(&config);

    let handle = match bootstrap::start(config.clone()).await {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("bootstrap failed: {e}");
            std::process::exit(1);
        }
    };

    tracing::info!(
        "rudis listening on {}:{} (workers={}, shards={})",
        config.bind,
        config.port,
        config.workers,
        config.shards
    );

    if let Err(e) = shutdown::wait_for_signal().await {
        tracing::error!("signal handler error: {e}");
    }

    tracing::info!("shutdown initiated");
    if let Err(e) = handle.shutdown().await {
        tracing::error!("shutdown error: {e}");
    }
    tracing::info!("rudis stopped");
}
