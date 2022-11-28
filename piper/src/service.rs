use std::{collections::HashMap, sync::Arc};

use azure_core::auth::TokenCredential;
use azure_identity::{DefaultAzureCredential, DefaultAzureCredentialBuilder};
use clap::Parser;
use poem::{
    error::BadRequest,
    get, handler,
    listener::TcpListener,
    middleware::{Cors, TokioMetrics, Tracing},
    post,
    web::{Data, Json},
    EndpointExt, Route, Server,
};
use tracing::{debug, info};

use crate::{Appliable, Function, Logged, Piper, PiperError, Request, Response};

#[derive(Parser, Debug, Clone, Default)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Pipeline definition file name
    #[arg(short, long, env = "PIPELINE_DEFINITION_FILE")]
    pub pipeline: String,

    #[arg(hide = true, long, default_value = "None")]
    pub pipeline_definition: Option<String>,

    /// Lookup source definition file name
    #[arg(short, long, env = "LOOKUP_DEFINITION_FILE")]
    pub lookup: String,

    #[arg(hide = true, long, default_value = "None")]
    pub lookup_definition: Option<String>,

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
        let pipeline_def = match &arg.pipeline_definition {
            Some(def) => def.clone(),
            None => load_file(&arg.pipeline, arg.enable_managed_identity).await?,
        };

        let lookup_def = match &arg.lookup_definition {
            Some(def) => def.clone(),
            None => load_file(&arg.lookup, arg.enable_managed_identity).await?,
        };

        let piper = Arc::new(Piper::new(&pipeline_def, &lookup_def)?);
        Ok(Self { arg, piper })
    }

    pub async fn with_udf(
        arg: Args,
        udf: HashMap<String, Box<dyn Function>>,
    ) -> Result<Self, PiperError> {
        let pipeline_def = load_file(&arg.pipeline, arg.enable_managed_identity).await?;
        let lookup_def = load_file(&arg.lookup, arg.enable_managed_identity).await?;

        let piper = Arc::new(Piper::new_with_udf(&pipeline_def, &lookup_def, udf)?);
        Ok(Self { arg, piper })
    }

    pub async fn start(&self) -> Result<(), PiperError> {
        self.start_at(&self.arg.address, self.arg.port).await
    }

    pub fn create(pipelines: &str, lookups: &str, udf: HashMap<String, Box<dyn Function>>) -> Self {
        let piper = Piper::new_with_udf(pipelines, lookups, udf).unwrap();
        Self {
            arg: Default::default(),
            piper: Arc::new(piper),
        }
    }

    pub async fn start_at(&self, address: &str, port: u16) -> Result<(), PiperError> {
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

        info!("Piper started, listening on {}:{}", address, port);
        Server::new(TcpListener::bind(format!("{}:{}", address, port)))
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

async fn make_request(
    url: &str,
    enable_managed_identity: bool,
) -> Result<reqwest::RequestBuilder, PiperError> {
    let client = reqwest::Client::new();
    if url.starts_with("https://") && url.contains(".blob.core.windows.net/") {
        // It's on Azure Storage Blob
        let credential = if enable_managed_identity {
            DefaultAzureCredential::default()
        } else {
            DefaultAzureCredentialBuilder::new()
                .exclude_managed_identity_credential()
                .build()
        };
        let token = credential
            .get_token("https://storage.azure.com/")
            .await
            .log()
            .map_err(|e| PiperError::AuthError(format!("{:?}", e)))
            .map(|t| t.token.secret().to_string())
            .ok();
        match token {
            // Acquired token and use it
            Some(t) => Ok(client
                .get(url)
                // @see: https://learn.microsoft.com/en-us/azure/storage/common/storage-auth-aad-app?tabs=dotnet#create-a-block-blob
                .header("x-ms-version", "2017-11-09")
                .bearer_auth(t)),
            // We don't have token, assume it's public accessible
            None => Ok(client.get(url)),
        }
    } else {
        Ok(client.get(url))
    }
}

async fn load_file(path: &str, enable_managed_identity: bool) -> Result<String, PiperError> {
    debug!("Reading file at {}", path);
    Ok(if path.starts_with("http:") || path.starts_with("https:") {
        let resp = make_request(path, enable_managed_identity)
            .await?
            .send()
            .await
            .log()
            .map_err(|e| PiperError::Unknown(e.to_string()))?;
        resp.text()
            .await
            .log()
            .map_err(|e| PiperError::Unknown(e.to_string()))
    } else {
        tokio::fs::read_to_string(path)
            .await
            .log()
            .map_err(|e| PiperError::Unknown(e.to_string()))
    }?
    .then(|s| {
        debug!(
            "Successfully read file at {}, file length is {}",
            path,
            s.len()
        );
    }))
}
