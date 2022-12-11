use std::fmt::Debug;
use std::sync::Arc;
use std::{collections::HashMap, env};

use async_trait::async_trait;
use regex::Regex;
use serde::{de, Deserialize, Serialize};

use super::{PiperError, Value};

mod cosmosdb;
mod feathr_online_store;
mod http_json_api;
mod mssql;
mod sqlite;

use feathr_online_store::FeathrOnlineStore;
use http_json_api::HttpJsonApi;

// Disable batch by default
const DEFAULT_CONCURRENCY: usize = 1;

#[async_trait]
pub trait LookupSource: Sync + Send + Debug {
    fn batch_size(&self) -> usize {
        DEFAULT_CONCURRENCY
    }

    /**
     * Return single row for one key
     */
    async fn lookup(&self, key: &Value, fields: &[String]) -> Vec<Value> {
        self.join(key, fields)
            .await
            .get(0)
            .cloned()
            .unwrap_or_else(|| vec![Value::Null; fields.len()])
    }

    /**
     * It can return multiple rows in a join operation, if the lookup source supports it.
     */
    async fn join(&self, key: &Value, fields: &[String]) -> Vec<Vec<Value>> {
        vec![self.lookup(key, fields).await]
    }

    fn dump(&self) -> serde_json::Value;
}

/**
 * This must be called with valid config before any lookup source is used.
 */
pub fn init_lookup_sources(
    cfg: &str,
) -> Result<HashMap<String, Arc<dyn LookupSource>>, PiperError> {
    #[derive(Debug, Deserialize, Serialize)]
    struct LookupSources {
        #[serde(default)]
        sources: Vec<LookupSourceEntry>,
    }

    let cfg = if cfg.is_empty() { "{}" } else { cfg };

    let cfg: HashMap<String, Arc<dyn LookupSource>> = serde_json::from_str::<LookupSources>(cfg)
        .map_err(|e| PiperError::Unknown(format!("Failed to parse lookup source config: {}", e)))?
        .sources
        .into_iter()
        .map(|e| (e.name, Arc::new(e.source) as Arc<dyn LookupSource>))
        .collect();
    Ok(cfg)
}

pub fn load_lookup_source(json_str: &str) -> Result<Arc<dyn LookupSource>, PiperError> {
    let entry: LookupSourceEntry = serde_json::from_str(json_str)
        .map_err(|e| PiperError::Unknown(format!("Failed to parse lookup source config: {}", e)))?;
    Ok(Arc::new(entry.source))
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "class")]
enum LookupSourceType {
    #[serde(alias = "HttpJsonApiSource", alias = "http")]
    HttpJsonApi(HttpJsonApi),
    #[serde(alias = "FeathrRedisSource", alias = "feathr")]
    FeathrOnlineStore(FeathrOnlineStore),
    #[serde(alias = "MsSqlSource", alias = "mssql")]
    MsSqlLSource(mssql::MsSqlLookupSource),
    #[serde(alias = "SqliteSource", alias = "sqlite")]
    SqliteLSource(sqlite::SqliteLookupSource),
    #[serde(alias = "cosmosdb", alias = "cosmos")]
    CosmosDb(cosmosdb::CosmosDbSource),
    // TODO: Add more lookup sources here
    // MongoDb(MongoDb),
}

#[async_trait]
impl LookupSource for LookupSourceType {
    async fn lookup(&self, key: &Value, fields: &[String]) -> Vec<Value> {
        match self {
            LookupSourceType::HttpJsonApi(s) => s.lookup(key, fields).await,
            LookupSourceType::FeathrOnlineStore(s) => s.lookup(key, fields).await,
            LookupSourceType::MsSqlLSource(s) => s.lookup(key, fields).await,
            LookupSourceType::SqliteLSource(s) => s.lookup(key, fields).await,
            LookupSourceType::CosmosDb(s) => s.lookup(key, fields).await,
        }
    }

    async fn join(&self, key: &Value, fields: &[String]) -> Vec<Vec<Value>> {
        match self {
            LookupSourceType::HttpJsonApi(s) => s.join(key, fields).await,
            LookupSourceType::FeathrOnlineStore(s) => s.join(key, fields).await,
            LookupSourceType::MsSqlLSource(s) => s.join(key, fields).await,
            LookupSourceType::SqliteLSource(s) => s.join(key, fields).await,
            LookupSourceType::CosmosDb(s) => s.join(key, fields).await,
        }
    }

    fn dump(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct LookupSourceEntry {
    name: String,
    #[serde(flatten)]
    source: LookupSourceType,
}

pub fn get_secret<T>(secret: Option<T>) -> Result<String, PiperError>
where
    T: AsRef<str>,
{
    match secret {
        Some(p) => {
            let re = Regex::new(r"^\$\{([^}]+)\}$").unwrap();
            match re.captures(p.as_ref()) {
                Some(cap) => Ok(env::var(cap.get(1).unwrap().as_str()).map_err(|_| {
                    PiperError::EnvVarNotSet(cap.get(1).unwrap().as_str().to_string())
                })?),
                None => Ok(p.as_ref().to_string()),
            }
        }
        None => Ok(Default::default()),
    }
}

pub fn deserialize_field_list<'de, D>(deserializer: D) -> Result<HashMap<String, usize>, D::Error>
where
    D: de::Deserializer<'de>,
{
    let v: Vec<String> = de::Deserialize::deserialize(deserializer)?;
    Ok(v.into_iter().enumerate().map(|(i, f)| (f, i)).collect())
}

pub fn serialize_field_list(fields: &HashMap<String, usize>) -> Vec<&String> {
    let mut entries = fields.iter().map(|(k, v)| (k, *v)).collect::<Vec<_>>();
    entries.sort_by_key(|(_, v)| *v);
    entries.into_iter().map(|(k, _)| k).collect::<Vec<_>>()
}
