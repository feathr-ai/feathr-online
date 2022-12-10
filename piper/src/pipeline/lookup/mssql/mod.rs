use std::collections::HashMap;

use bb8::{Pool, PooledConnection};
use bb8_tiberius::ConnectionManager;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::OnceCell;

use crate::{common::IgnoreDebug, Logged, LookupSource, PiperError, Value};

use super::get_secret;

mod db_conv;

use db_conv::row_to_values;

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MsSqlLookupSource {
    connection_string: String,
    sql_template: String,
    available_fields: Vec<String>,
    #[serde(skip)]
    pool: IgnoreDebug<OnceCell<bb8::Pool<bb8_tiberius::ConnectionManager>>>,
}

impl MsSqlLookupSource {
    async fn get_pool(&self) -> Result<&Pool<ConnectionManager>, PiperError> {
        self.pool
            .inner
            .get_or_try_init(|| async {
                let mgr = bb8_tiberius::ConnectionManager::build(
                    get_secret(Some(&self.connection_string))?.as_str(),
                )
                .log()
                .map_err(|e| PiperError::ExternalError(e.to_string()))?;
                bb8::Pool::builder()
                    .max_size(2)
                    .build(mgr)
                    .await
                    .log()
                    .map_err(|e| PiperError::ExternalError(e.to_string()))
            })
            .await
    }

    async fn get_connection(
        &self,
    ) -> Result<PooledConnection<'static, bb8_tiberius::ConnectionManager>, PiperError> {
        self.get_pool()
            .await?
            .get_owned()
            .await
            .log()
            .map_err(|e| PiperError::ExternalError(e.to_string()))
    }

    async fn make_query(&self, key: &Value) -> Result<Vec<Vec<Value>>, PiperError> {
        self.get_connection()
            .await?
            .query(&self.sql_template, &[key])
            .await
            .log()
            .map_err(|e| PiperError::ExternalError(e.to_string()))?
            .into_first_result()
            .await
            .log()
            .map_err(|e| PiperError::ExternalError(e.to_string()))
            .map(|rows| rows.into_iter().map(row_to_values).collect())
    }
}

#[async_trait::async_trait]
impl LookupSource for MsSqlLookupSource {
    async fn lookup(&self, key: &Value, fields: &[String]) -> Vec<Value> {
        self.join(key, fields)
            .await
            .get(0)
            .cloned()
            .unwrap_or_else(|| vec![Value::Null; fields.len()])
    }

    async fn join(&self, key: &Value, fields: &[String]) -> Vec<Vec<Value>> {
        // Propagate error
        if matches!(key, Value::Error(_)) {
            return vec![vec![key.clone(); fields.len()]];
        }

        // Null key
        if matches!(key, Value::Null) {
            return vec![vec![Value::Null; fields.len()]];
        }

        // Unsupported key type
        if matches!(key, Value::Array(_) | Value::Object(_)) {
            return vec![vec![
                Value::Error(PiperError::InvalidValue(format!(
                    "Unsupported key type: {:?}",
                    key.value_type()
                )));
                fields.len()
            ]];
        }

        let idx_map: HashMap<String, usize> = self
            .available_fields
            .iter()
            .enumerate()
            .map(|(i, f)| (f.clone(), i))
            .collect();

        let rows = self.make_query(key).await;
        match rows {
            Ok(v) => v
                .into_iter()
                .map(|row| {
                    fields
                        .iter()
                        .map(|f| {
                            idx_map
                                .get(f)
                                .and_then(|idx| row.get(*idx).cloned())
                                .unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .collect(),
            Err(e) => {
                vec![vec![e.into(); fields.len()]]
            }
        }
    }

    fn dump(&self) -> serde_json::Value {
        json!(
            {
                "sql_template": self.sql_template,
                "available_fields": self.available_fields,
            }
        )
    }
}
