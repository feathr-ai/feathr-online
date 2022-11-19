use std::{collections::HashMap, fmt::Debug, time::Instant};

use clap::Parser;
use futures::future::join_all;
use once_cell::sync::OnceCell;
use pipeline::{Pipeline, PiperError};
use poem::{
    get, handler,
    listener::TcpListener,
    middleware::{Cors, TokioMetrics, Tracing},
    post,
    web::Json,
    EndpointExt, Route, Server,
};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, instrument, metadata::LevelFilter};
use tracing_subscriber::EnvFilter;

use crate::pipeline::{ValidationMode, Value};

mod common;
mod pipeline;

pub use common::{Appliable, Logged};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Pipeline definition file name
    #[arg(short, long, env = "PIPELINE_DEFINITION_FILE")]
    pipeline: String,

    /// Lookup source definition file name
    #[arg(short, long, env = "LOOKUP_DEFINITION_FILE")]
    lookup: String,

    #[arg(long, default_value = "0.0.0.0", env = "LISTENING_ADDRESS")]
    address: String,

    #[arg(long, default_value_t = 8000, env = "LISTENING_PORT")]
    port: u16,
}

static PIPELINES: OnceCell<HashMap<String, Pipeline>> = OnceCell::new();

#[handler]
async fn health_check() -> &'static str {
    let (_, ret) = PIPELINES
        .get()
        .unwrap()
        .get("%health")
        .unwrap()
        .process_row(vec![Value::Int(57)], ValidationMode::Strict)
        .unwrap()
        .eval()
        .await;
    if ret.len() == 1 {
        match &ret[0] {
            Ok(row) => {
                if row.len() == 2 {
                    match row[1] {
                        Value::Int(99) => "OK",
                        _ => "ERROR",
                    }
                } else {
                    "ERROR"
                }
            }
            Err(_) => "ERROR",
        }
    } else {
        "ERROR"
    }
}

#[handler]
fn dump_pipelines() -> Json<HashMap<String, serde_json::Value>> {
    Json(
        PIPELINES
            .get()
            .unwrap()
            .values()
            .map(|p| (p.name.clone(), p.to_json()))
            .collect(),
    )
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SingleRequest {
    pipeline: String,
    #[serde(default)]
    validation_mode: ValidationMode,
    data: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    requests: Vec<SingleRequest>,
}

#[derive(Debug, Serialize)]
struct SingleResponse {
    pipeline: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Vec<HashMap<String, serde_json::Value>>>,
}

#[derive(Debug, Serialize)]
struct Response {
    results: Vec<SingleResponse>,
}

#[handler]
#[instrument(level = "trace")]
async fn process(req: Json<Request>) -> poem::Result<Json<Response>> {
    debug!(
        "Received request, contains {} sub-requests",
        req.requests.len()
    );
    let futures: Vec<_> = req
        .0
        .requests
        .into_iter()
        .map(|r| async {
            let pipeline = r.pipeline.clone();
            let r = process_single_request(r).await;
            match r {
                Ok(r) => r,
                Err(e) => SingleResponse {
                    pipeline,
                    status: format!("ERROR: {:?}", e),
                    time: None,
                    count: None,
                    data: None,
                },
            }
        })
        .collect();
    let results = join_all(futures).await;
    Ok(Json(Response { results }))
}

#[instrument(level = "trace")]
async fn process_single_request(req: SingleRequest) -> Result<SingleResponse, PiperError> {
    let pipeline = PIPELINES
        .get()
        .ok_or(PiperError::PipelineNotFound(req.pipeline.clone()))?
        .get(&req.pipeline)
        .ok_or(PiperError::PipelineNotFound(req.pipeline.clone()))?;
    debug!("Processing request to pipeline {}", pipeline.name);

    let schema = &pipeline.input_schema;

    let row: Vec<Value> = schema
        .columns
        .iter()
        .map(|c| {
            req.data
                .get(c.name.as_str())
                .map(|v| Value::from(v.clone()))
                .unwrap_or_default()
        })
        .collect();

    let now = Instant::now();
    let (schema, ret) = pipeline.process_row(row, req.validation_mode)?.eval().await;
    let ret: Vec<HashMap<String, serde_json::Value>> = ret
        .into_iter()
        .map(|r| {
            r.map(|v| {
                v.into_iter()
                    .zip(schema.columns.iter())
                    .map(|(v, c)| (c.name.clone(), v.into()))
                    .collect()
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SingleResponse {
        pipeline: req.pipeline,
        status: "OK".to_owned(),
        time: Some((now.elapsed().as_micros() as f64) / 1000f64),
        count: Some(ret.len()),
        data: Some(ret),
    })
}

async fn load_file(path: &str) -> Result<String, PiperError> {
    debug!("Reading file at {}", path);
    Ok(if path.starts_with("http:") || path.starts_with("https:") {
        let resp = reqwest::get(path)
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

    let pipeline_def = load_file(&args.pipeline).await?;
    let lookup_def = load_file(&args.lookup).await?;

    let mut pipelines = Pipeline::load(&pipeline_def, &lookup_def).log()?;
    // Use invalid identifier as the name, avoid clashes with user-defined pipelines
    pipelines.insert("%health".to_string(), Pipeline::get_health_checker());

    PIPELINES.set(pipelines).unwrap();

    let metrics_process = TokioMetrics::new();

    let app = Route::new()
        .at("/version", get(get_version))
        .at("/metrics", metrics_process.exporter())
        .at("/process", post(process).with(metrics_process))
        .at("/healthz", get(health_check))
        .at("/pipelines", get(dump_pipelines))
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
