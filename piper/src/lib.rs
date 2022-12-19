use std::{collections::HashMap, fmt::Debug};

use serde::{Deserialize, Serialize};

mod common;
mod pipeline;
mod piper;
mod service;

pub use crate::piper::Piper;
pub use common::{Appliable, Logged};
pub use pipeline::{
    load_lookup_source, ErrorCollectingMode, ErrorRecord, Function, IntoValue, LookupSource, PiperError,
    Value, ValueType,
};
pub use service::{Args, PiperService};

#[derive(Clone, Debug, Deserialize)]
#[serde(untagged)]
pub enum RequestData {
    Single(HashMap<String, serde_json::Value>),
    Multi(Vec<HashMap<String, serde_json::Value>>),
}

impl Default for RequestData {
    fn default() -> Self {
        Self::Single(HashMap::new())
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupRequest {
    pub source: String,
    pub keys: Vec<serde_json::Value>,
    pub features: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LookupResponse {
    pub data: Vec<HashMap<String, serde_json::Value>>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SingleRequest {
    pub pipeline: String,
    pub data: RequestData,
    #[serde(default)]
    pub validate: bool,
    #[serde(default)]
    pub errors: ErrorCollectingMode,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Request {
    pub requests: Vec<SingleRequest>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SingleResponse {
    pub pipeline: String,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Vec<HashMap<String, serde_json::Value>>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<ErrorRecord>,
}

#[derive(Debug, Serialize)]
pub struct Response {
    pub results: Vec<SingleResponse>,
}
