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
    #[serde(deserialize_with = "super::deserialize_field_list")]
    available_fields: HashMap<String, usize>,
    #[serde(skip)]
    pool: IgnoreDebug<OnceCell<bb8::Pool<bb8_tiberius::ConnectionManager>>>,
}

impl MsSqlLookupSource {
    async fn get_pool(&self) -> Result<&Pool<ConnectionManager>, PiperError> {
        self.pool
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

        let rows = self.make_query(key).await;
        match rows {
            Ok(v) => v
                .into_iter()
                .map(|row| {
                    fields
                        .iter()
                        .map(|f| {
                            self.available_fields
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
                "available_fields": super::serialize_field_list(&self.available_fields),
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LookupSource, Value};

    #[tokio::test]
    async fn test_sqlite_lookup() {
        dotenvy::dotenv().ok();
        let s: MsSqlLookupSource = serde_json::from_str(r#"{
            "connectionString": "${CONN_STR}",
            "sqlTemplate": "select name, age from join_test where group_id = @P1",
            "availableFields": [
              "name",
              "age"
            ]
        }"#).unwrap();
        let l = Box::new(s);
        let result = l
            .join(&Value::Int(2), &["name".to_string(), "age".to_string()])
            .await;
        assert_eq!(
            result,
            vec![
                vec![Value::String("Jill".into()), Value::Int(33)],
                vec![Value::String("Jose".into()), Value::Int(34)]
            ]
        );
    }
}
