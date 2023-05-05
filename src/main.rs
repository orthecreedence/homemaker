use anyhow::{Context, Result};
use tracing::log::{debug, info, warn, error};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

pub mod channel;
pub mod job;
pub mod queue;
pub mod store;

fn setup_logger() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(
            EnvFilter::try_from_default_env()
                .or_else(|_| EnvFilter::try_new("info"))
                .context("error parsing RUST_LOG env")?
        )
        .init();
    Ok(())
}

fn main() -> Result<()> {
    setup_logger()
        .context("Failed to setup logging")?;
    let queue 
    Ok(())
}

