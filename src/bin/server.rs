use anyhow::Result;
use tracing::{info, error};
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    info!("RustKV Server is starting...");

    if let Err(e) = run() {
        error!("Fatal error: {}", e);
    }

    Ok(())
}

fn run() -> Result<()> {
    info!("RustKV Server is running...");
    Ok(())
}