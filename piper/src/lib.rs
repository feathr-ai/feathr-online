use std::{collections::HashMap, fmt::Debug};

use clap::Parser;
use pipeline::{ErrorCollectingMode, ErrorRecord};
use serde::{Deserialize, Serialize};

mod common;
mod pipeline;
mod piper;

pub use common::{Appliable, Logged};
pub use pipeline::{Function, PiperError, Value, ValueType};
pub use piper::Piper;

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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SingleRequest {
    pipeline: String,
    data: HashMap<String, serde_json::Value>,
    #[serde(default)]
    validate: bool,
    #[serde(default)]
    errors: ErrorCollectingMode,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    requests: Vec<SingleRequest>,
}

#[derive(Debug, Serialize)]
pub struct SingleResponse {
    pipeline: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Vec<HashMap<String, serde_json::Value>>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<ErrorRecord>,
}

#[derive(Debug, Serialize)]
pub struct Response {
    results: Vec<SingleResponse>,
}
