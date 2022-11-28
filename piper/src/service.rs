use std::{collections::HashMap, sync::Arc};

use clap::Parser;
use poem::{
    error::BadRequest,
    handler,
    web::{Data, Json}, middleware::{TokioMetrics, Cors, Tracing}, get, post, Route, EndpointExt, Server, listener::TcpListener,
};
use tracing::info;

use crate::{Piper, PiperError, Request, Response, Logged, Appliable};

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Pipeline definition file name
    #[arg(short, long, env = "PIPELINE_DEFINITION_FILE")]
    pub pipeline: String,

    /// Lookup source definition file name
    #[arg(short, long, env = "LOOKUP_DEFINITION_FILE")]
    pub lookup: String,

    #[arg(long, default_value = "0.0.0.0", env = "LISTENING_ADDRESS")]
    pub address: String,

    #[arg(long, default_value_t = 8000, env = "LISTENING_PORT")]
    pub port: u16,

    #[arg(long, default_value_t = false, env = "ENABLE_MANAGED_IDENTITY")]
    pub enable_managed_identity: bool,
}

pub struct PiperService {
    arg: Args,
    piper: Arc<Piper>,
}

impl PiperService {
    pub async fn new(arg: Args) -> Result<Self, PiperError> {
        let piper =
            Arc::new(Piper::new(&arg.pipeline, &arg.lookup, arg.enable_managed_identity).await?);
        Ok(Self { arg, piper })
    }

    pub async fn start(&self) -> Result<(), PiperError> {
        let metrics_process = TokioMetrics::new();

        let app = Route::new()
            .at("/version", get(get_version))
            .at("/metrics", metrics_process.exporter())
            .at("/process", post(process).with(metrics_process))
            .at("/healthz", get(health_check))
            .at("/pipelines", get(get_pipelines))
            .at("/lookup-sources", get(get_lookup_sources))
            .with(Cors::new())
            .with(Tracing)
            .data(self.piper.clone());
    
        info!("Piper started, listening on {}:{}", self.arg.address, self.arg.port);
        Server::new(TcpListener::bind(format!("{}:{}", self.arg.address, self.arg.port)))
            .run(app)
            .await
            .log()
            .map_err(|e| PiperError::Unknown(e.to_string()))
            .then(|_| info!("Exiting..."))    
    }
}

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
async fn health_check(piper: Data<&Arc<Piper>>) -> String {
    if piper.0.health_check().await {
        "OK".to_string()
    } else {
        "ERROR".to_string()
    }
}

#[handler]
fn get_pipelines(piper: Data<&Arc<Piper>>) -> Json<HashMap<String, serde_json::Value>> {
    Json(piper.0.get_pipelines())
}

#[handler]
fn get_lookup_sources(piper: Data<&Arc<Piper>>) -> Json<serde_json::Value> {
    Json(piper.0.get_lookup_sources())
}

#[handler]
async fn process(piper: Data<&Arc<Piper>>, req: Json<Request>) -> poem::Result<Json<Response>> {
    Ok(Json(piper.0.process(req.0).await.map_err(BadRequest)?))
}
