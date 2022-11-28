use clap::Parser;
use piper::{Args, PiperError, PiperService};

use tracing::{info, metadata::LevelFilter};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), PiperError> {
    dotenv::dotenv().ok();

    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("LOG_LEVEL")
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("Piper is starting...");
    let args = Args::parse();

    let svc = PiperService::new(args).await?;

    svc.start().await
}
