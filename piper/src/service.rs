use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use azure_core::auth::TokenCredential;
use azure_identity::{DefaultAzureCredential, DefaultAzureCredentialBuilder};
use clap::Parser;
use futures::{pin_mut, Future};
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

use crate::{Appliable, Function, Logged, LookupSource, Piper, PiperError, Request, Response};

#[derive(Parser, Debug, Clone, Default)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// Pipeline definition file name
    #[arg(short, long, env = "PIPELINE_DEFINITION_FILE")]
    pub pipeline: String,

    #[arg(hide = true, long)]
    pub pipeline_definition: Option<String>,

    /// Lookup source definition file name
    #[arg(short, long, env = "LOOKUP_DEFINITION_FILE")]
    pub lookup: String,

    #[arg(hide = true, long)]
    pub lookup_definition: Option<String>,

    #[arg(long, default_value = "0.0.0.0", env = "LISTENING_ADDRESS")]
    pub address: String,

    #[arg(long, default_value_t = 8000, env = "LISTENING_PORT")]
    pub port: u16,

    #[arg(long, default_value_t = false, env = "ENABLE_MANAGED_IDENTITY")]
    pub enable_managed_identity: bool,
}

/**
 * The pipeline service
 */
pub struct PiperService {
    arg: Args,
    piper: Arc<Piper>,

    should_stop: AtomicBool,
}

#[derive(Debug, Clone)]
pub struct HandlerData {
    piper: Arc<Piper>,
    #[cfg(feature = "python")]
    locals: Option<pyo3_asyncio::TaskLocals>,
}

impl PiperService {
    /**
     * Create a new pipeline service
     */
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
        Ok(Self {
            arg,
            piper,
            should_stop: AtomicBool::new(false),
        })
    }

    /**
     * Create a new pipeline service with UDF
     */
    pub async fn with_udf(
        arg: Args,
        udf: HashMap<String, Box<dyn Function>>,
    ) -> Result<Self, PiperError> {
        let pipeline_def = load_file(&arg.pipeline, arg.enable_managed_identity).await?;
        let lookup_def = load_file(&arg.lookup, arg.enable_managed_identity).await?;

        let piper = Arc::new(Piper::new_with_udf(&pipeline_def, &lookup_def, udf)?);
        Ok(Self {
            arg,
            piper,
            should_stop: AtomicBool::new(false),
        })
    }

    /**
     * Create a new pipeline service with lookup source in JSON and UDF
     */
    pub fn create(pipelines: &str, lookups: &str, udf: HashMap<String, Box<dyn Function>>) -> Self {
        let piper = Piper::new_with_udf(pipelines, lookups, udf).unwrap();
        Self {
            arg: Default::default(),
            piper: Arc::new(piper),
            should_stop: AtomicBool::new(false),
        }
    }

    /**
     * Create a new pipeline service with lookup source map in JSON and UDF
     */
    pub fn create_with_lookup_udf(
        pipelines: &str,
        lookups: HashMap<String, Arc<dyn LookupSource>>,
        udf: HashMap<String, Box<dyn Function>>,
    ) -> Self {
        let piper = Piper::new_with_lookup_udf(pipelines, lookups, udf).unwrap();
        Self {
            arg: Default::default(),
            piper: Arc::new(piper),
            should_stop: AtomicBool::new(false),
        }
    }

    /**
     * Start the pipeline service
     */
    pub async fn start(
        &mut self,
        #[cfg(feature = "python")] use_py_async: bool,
    ) -> Result<(), PiperError> {
        let address = self.arg.address.clone();
        self.start_at(
            &address,
            self.arg.port,
            #[cfg(feature = "python")]
            use_py_async,
        )
        .await
    }

    /**
     * Start the service at the given address and port
     */
    pub async fn start_at(
        &mut self,
        address: &str,
        port: u16,
        #[cfg(feature = "python")] use_py_async: bool,
    ) -> Result<(), PiperError> {
        self.should_stop.store(false, Ordering::Relaxed);
        let metrics_process = TokioMetrics::new();

        let data = HandlerData {
            piper: self.piper.clone(),
            #[cfg(feature = "python")]
            locals: if use_py_async {
                Some(
                    pyo3::Python::with_gil(pyo3_asyncio::tokio::get_current_locals)
                        .map_err(|e| PiperError::ExternalError(e.to_string()))?,
                )
            } else {
                None
            },
        };

        let app = Route::new()
            .at("/version", get(get_version))
            .at("/metrics", metrics_process.exporter())
            .at("/process", post(process).with(metrics_process))
            .at("/lookup", post(lookup_feature))
            .at("/healthz", get(health_check))
            .at("/pipelines", get(get_pipelines))
            .at("/lookup-sources", get(get_lookup_sources))
            .with(Cors::new())
            .with(Tracing)
            .data(data);

        info!("Piper started, listening on {}:{}", address, port);
        self.cancelable_wait(async {
            Server::new(TcpListener::bind(format!("{}:{}", address, port)))
                .run(app)
                .await
                .log()
                .map_err(|e| PiperError::Unknown(e.to_string()))
                .then(|_| info!("Exiting..."))
        })
        .await
    }

    /**
     * Stop the pipeline service
     */
    pub fn stop(&mut self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }

    /**
     * Check CTRL-C every 100ms, cancel the future if pressed and return Interrupted error
     */
    async fn cancelable_wait<F, T: Send>(&self, f: F) -> Result<T, PiperError>
    where
        F: Future<Output = Result<T, PiperError>>,
    {
        // Future needs to be pinned then its mutable ref can be awaited multiple times.
        pin_mut!(f);
        loop {
            match tokio::time::timeout(std::time::Duration::from_millis(100), &mut f).await {
                Ok(v) => {
                    return v;
                }
                Err(_) => {
                    // Timeout, check if CTRL-C is pressed
                    if self.should_stop.load(Ordering::Relaxed) {
                        return Err(PiperError::Interrupted);
                    }
                }
            }
        }
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
async fn health_check(data: Data<&HandlerData>) -> String {
    if data.0.piper.health_check().await {
        "OK".to_string()
    } else {
        "ERROR".to_string()
    }
}

#[handler]
fn get_pipelines(data: Data<&HandlerData>) -> Json<HashMap<String, serde_json::Value>> {
    Json(data.0.piper.get_pipelines())
}

#[handler]
fn get_lookup_sources(data: Data<&HandlerData>) -> Json<serde_json::Value> {
    Json(data.0.piper.get_lookup_sources())
}

#[cfg(feature = "python")]
#[handler]
async fn process(data: Data<&HandlerData>, req: Json<Request>) -> poem::Result<Json<Response>> {
    let data = data.0.clone();
    match data.locals.clone() {
        Some(locals) => {
            pyo3_asyncio::tokio::scope(locals, async move {
                Ok(Json(data.piper.process(req.0).await.map_err(BadRequest)?))
            })
            .await
        }
        None => Ok(Json(data.piper.process(req.0).await.map_err(BadRequest)?)),
    }
}

#[cfg(not(feature = "python"))]
#[handler]
async fn lookup_feature(
    data: Data<&HandlerData>,
    req: Json<crate::LookupRequest>,
) -> poem::Result<Json<crate::LookupResponse>> {
    Ok(Json(data.0.piper.lookup(req.0).await.map_err(BadRequest)?))
}

#[cfg(feature = "python")]
#[handler]
async fn lookup_feature(
    data: Data<&HandlerData>,
    req: Json<crate::LookupRequest>,
) -> poem::Result<Json<crate::LookupResponse>> {
    let data = data.0.clone();
    match data.locals.clone() {
        Some(locals) => {
            pyo3_asyncio::tokio::scope(locals, async move {
                Ok(Json(data.piper.lookup(req.0).await.map_err(BadRequest)?))
            })
            .await
        }
        None => Ok(Json(data.piper.lookup(req.0).await.map_err(BadRequest)?)),
    }
}

#[cfg(not(feature = "python"))]
#[handler]
async fn process(data: Data<&HandlerData>, req: Json<Request>) -> poem::Result<Json<Response>> {
    Ok(Json(data.0.piper.process(req.0).await.map_err(BadRequest)?))
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;

    use crate::pipeline::init_lookup_sources;

    #[tokio::test]
    async fn test_load_file() {
        dotenvy::dotenv().ok();
        let content = super::load_file(
            "https://xchfeathrtest4sto.blob.core.windows.net/xchfeathrtest4fs/lookup.json",
            false,
        )
        .await
        .unwrap();
        serde_json::from_str::<serde_json::Value>(&content).unwrap();
    }

    #[tokio::test]
    async fn test_svc() {
        dotenvy::dotenv().ok();
        let lookup_src = if let Ok(s) = super::load_file("../conf/lookup.json", false).await {
            s
        } else {
            super::load_file("conf/lookup.json", false).await.unwrap()
        };
        let lookups = init_lookup_sources(&lookup_src).unwrap();
        tokio::spawn(async {
            let pipelines = "t(x) | project y=x+42, z=x-42;";
            let mut svc =
                super::PiperService::create_with_lookup_udf(pipelines, lookups, Default::default());
            svc.start_at("127.0.0.1", 38080).await.unwrap();
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let client = reqwest::Client::new();
        let resp = client
            // Devskim: ignore DS137138
            .get("http://127.0.0.1:38080/healthz")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            // Devskim: ignore DS137138
            .get("http://127.0.0.1:38080/lookup-sources")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            // Devskim: ignore DS137138
            .get("http://127.0.0.1:38080/pipelines")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            // Devskim: ignore DS137138
            .get("http://127.0.0.1:38080/metrics")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp = client
            // Devskim: ignore DS137138
            .get("http://127.0.0.1:38080/version")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let resp: serde_json::Value = client
            // Devskim: ignore DS137138
            .post("http://127.0.0.1:38080/process")
            .json(&json!({ "requests": [{ "pipeline": "t", "data": [{"x": 57}] }] }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resp["results"][0]["data"][0]["y"], 99);
        assert_eq!(resp["results"][0]["data"][0]["z"], 15);

        let resp: serde_json::Value = client
            // Devskim: ignore DS137138
            .post("http://127.0.0.1:38080/lookup")
            .json(&json!({
                "source": "join_test_mssql",
                "keys": ["1", "2"],
                "features": ["name", "age"]
            }))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(resp["data"][0]["name"], "Jack");
        assert_eq!(resp["data"][0]["age"], 30);
        assert_eq!(resp["data"][1]["name"], "Jill");
        assert_eq!(resp["data"][1]["age"], 33);
        println!("{:?}", resp);
    }
}
