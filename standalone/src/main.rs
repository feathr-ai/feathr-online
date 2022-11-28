use std::collections::HashMap;

use clap::Parser;
use once_cell::sync::OnceCell;
use piper::{Appliable, Args, Logged, Piper, PiperError};
use poem::{
    error::BadRequest,
    get, handler,
    listener::TcpListener,
    middleware::{Cors, TokioMetrics, Tracing},
    post,
    web::Json,
    EndpointExt, Route, Server,
};
use tracing::{info, metadata::LevelFilter};
use tracing_subscriber::EnvFilter;

static PIPER: OnceCell<Piper> = OnceCell::new();

#[handler]
fn get_version() -> Json<HashMap<String, String>> {
    let mut version = HashMap::new();
    version.insert(
        "version".to_string(),
        option_env!("CARGO_PKG_VERSION")
            .unwrap_or_default()
            .to_string(),
    );
    Json(version)
}

#[handler]
async fn health_check() -> String {
    if PIPER.get().unwrap().health_check().await {
        "OK".to_string()
    } else {
        "ERROR".to_string()
    }
}

#[handler]
fn get_pipelines() -> Json<HashMap<String, serde_json::Value>> {
    Json(PIPER.get().unwrap().get_pipelines())
}

#[handler]
fn get_lookup_sources() -> Json<serde_json::Value> {
    Json(PIPER.get().unwrap().get_lookup_sources())
}

#[handler]
async fn process(req: Json<piper::Request>) -> poem::Result<Json<piper::Response>> {
    Ok(Json(
        PIPER
            .get()
            .unwrap()
            .process(req.0)
            .await
            .map_err(BadRequest)?,
    ))
}

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

    let piper = Piper::new(args.clone()).await?;
    PIPER.set(piper).unwrap();

    let metrics_process = TokioMetrics::new();

    let app = Route::new()
        .at("/version", get(get_version))
        .at("/metrics", metrics_process.exporter())
        .at("/process", post(process).with(metrics_process))
        .at("/healthz", get(health_check))
        .at("/pipelines", get(get_pipelines))
        .at("/lookup-sources", get(get_lookup_sources))
        .with(Cors::new())
        .with(Tracing);

    info!("Piper started, listening on {}:{}", args.address, args.port);
    Server::new(TcpListener::bind(format!("{}:{}", args.address, args.port)))
        .run(app)
        .await
        .log()
        .map_err(|e| PiperError::Unknown(e.to_string()))
        .then(|_| info!("Exiting..."))
}
