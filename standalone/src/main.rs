use std::pin::Pin;

use clap::Parser;
use futures::{Future, TryFutureExt};
use piper::{Args, PiperError, PiperService};

use tracing::{info, metadata::LevelFilter};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), PiperError> {
    dotenvy::dotenv().ok();
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .with_env_var("LOG_LEVEL")
        .from_env_lossy();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    info!("Piper is starting...");
    let args = Args::parse();

    let mut svc = PiperService::new(args).await?;

    let ctrl_c: Pin<Box<dyn Future<Output = Result<(), PiperError>>>> =
        Box::pin(tokio::signal::ctrl_c().map_err(|e| PiperError::Unknown(e.to_string())));
    let task: Pin<Box<dyn Future<Output = Result<(), PiperError>>>> = Box::pin(svc.start());

    futures::future::select(ctrl_c, task).await;
    Ok(())
}
