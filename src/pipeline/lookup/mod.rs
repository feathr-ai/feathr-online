use std::sync::Arc;
use std::{collections::HashMap, env};
use std::fmt::Debug;

use async_trait::async_trait;
use once_cell::sync::OnceCell;
use regex::Regex;
use serde::{Deserialize, Serialize};

use super::{PiperError, Value};

mod feathr_online_store;
mod http_json_api;

use feathr_online_store::FeathrOnlineStore;
use http_json_api::HttpJsonApi;

#[async_trait]
pub trait LookupSource: Sync + Send + Debug {
    async fn lookup(&self, key: &Value, fields: &Vec<String>) -> Result<Vec<Value>, PiperError>;

    fn dump(&self) -> serde_json::Value;
}

/**
 * Get a lookup source by name.
 */
pub fn get_lookup_source(name: &str) -> Result<Arc<dyn LookupSource>, PiperError> {
    let repo = LOOKUP_SOURCE_REPO
        .get_or_init(|| init_lookup_source_repo(None))
        .get(name)
        .cloned()
        .ok_or(PiperError::LookupSourceNotFound(name.to_string()))?;
    Ok(repo)
}

/**
 * This must be called with valid config before any lookup source is used.
 */
pub fn init_lookup_sources(cfg: &str) -> Result<usize, PiperError> {
    #[derive(Debug, Deserialize, Serialize)]
    struct LookupSources {
        sources: Vec<LookupSourceEntry>,
    }

    let cfg: HashMap<String, Arc<LookupSourceType>> = serde_json::from_str::<LookupSources>(cfg)
        .map_err(|e| PiperError::Unknown(format!("Failed to parse lookup source config: {}", e)))?
        .sources
        .into_iter()
        .map(|e| (e.name, Arc::new(e.source)))
        .collect();

    let ret = cfg.len();

    LOOKUP_SOURCE_REPO.get_or_init(|| init_lookup_source_repo(Some(cfg)));
    Ok(ret)
}

pub fn dump_lookup_sources() -> serde_json::Value {
    LOOKUP_SOURCE_REPO
        .get_or_init(|| init_lookup_source_repo(None))
        .iter()
        .map(|(k, v)| (k, v.dump()))
        .collect()
}

static LOOKUP_SOURCE_REPO: OnceCell<HashMap<String, Arc<LookupSourceType>>> = OnceCell::new();

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "class")]
enum LookupSourceType {
    #[serde(alias = "HttpJsonApiSource", alias = "http")]
    HttpJsonApi(HttpJsonApi),
    #[serde(alias = "FeathrRedisSource", alias = "feathr")]
    FeathrOnlineStore(FeathrOnlineStore),
    // TODO: Add more lookup sources here
    // CosmosDb(CosmosDb),
    // MongoDb(MongoDb),
}

#[async_trait]
impl LookupSource for LookupSourceType {
    async fn lookup(&self, key: &Value, fields: &Vec<String>) -> Result<Vec<Value>, PiperError> {
        match self {
            LookupSourceType::HttpJsonApi(s) => s.lookup(key, fields).await,
            LookupSourceType::FeathrOnlineStore(s) => s.lookup(key, fields).await,
        }
    }

    fn dump(&self) -> serde_json::Value {
        match self {
            LookupSourceType::HttpJsonApi(s) => s.dump(),
            LookupSourceType::FeathrOnlineStore(s) => s.dump(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct LookupSourceEntry {
    name: String,
    #[serde(flatten)]
    source: LookupSourceType,
}

fn init_lookup_source_repo(
    cfg: Option<HashMap<String, Arc<LookupSourceType>>>,
) -> HashMap<String, Arc<LookupSourceType>> {
    cfg.expect("Internal error: lookup source repo is not initialized")
}

pub fn get_secret<T>(secret: Option<T>) -> Option<String>
where
    T: AsRef<str>,
{
    match secret {
        Some(p) => {
            let re = Regex::new(r"^\$\{([^}]+)\}$").unwrap();
            match re.captures(p.as_ref()) {
                Some(cap) => env::var(cap.get(1).unwrap().as_str())
                    .map(|s| s.to_string())
                    .ok(),
                None => Some(p.as_ref().to_string()),
            }
        }
        None => None,
    }
}
