use std::{collections::HashMap, fmt::Debug};

use pipeline::{ErrorCollectingMode, ErrorRecord};
use serde::{Deserialize, Serialize};

mod common;
mod pipeline;
mod piper;
mod service;

pub use common::{Appliable, Logged};
pub use pipeline::{Function, PiperError, Value, ValueType};
pub use piper::Piper;
pub use service::{Args, PiperService};

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
