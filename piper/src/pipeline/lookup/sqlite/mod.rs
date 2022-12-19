use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use futures::TryFutureExt;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{PiperError, Value};

use self::db_conv::row_to_values;

mod db_conv;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqliteLookupSource {
    db_path: String,
    sql_template: String,
    #[serde(deserialize_with = "super::deserialize_field_list")]
    available_fields: HashMap<String, usize>,
    #[serde(skip)]
    client: OnceCell<Arc<Mutex<rusqlite::Connection>>>,
}

impl SqliteLookupSource {
    fn make_query_sync(
        conn: Arc<Mutex<rusqlite::Connection>>,
        sql_template: String,
        key: Value,
    ) -> Result<Vec<Vec<Value>>, PiperError> {
        let conn = conn.lock().unwrap();
        let mut stmt = conn.prepare(&sql_template).map_err(Into::into)?;
        let rows = stmt
            .query_map(&[(":key", &key)], |row| Ok(row_to_values(row)))
            .map_err(Into::into)?;
        let mut result = Vec::new();
        for row in rows {
            result.push(row.map_err(Into::into)?);
        }
        Ok(result)
    }

    async fn make_query(&self, key: &Value) -> Result<Vec<Vec<Value>>, PiperError> {
        let conn = self
            .client
            .get_or_try_init(|| {
                let conn = rusqlite::Connection::open(&self.db_path)?;
                Ok(Arc::new(Mutex::new(conn)))
            })
            .map_err(|e: rusqlite::Error| {
                PiperError::ExternalError(format!("Failed to spawn blocking task: {}", e))
            })?
            .clone();
        let sql_template = self.sql_template.clone();
        let key = key.clone();
        tokio::runtime::Handle::current()
            .spawn_blocking(move || Self::make_query_sync(conn, sql_template, key))
            .map_err(|e| PiperError::ExternalError(format!("Failed to spawn blocking task: {}", e)))
            .await?
    }
}

#[async_trait::async_trait]
impl super::LookupSource for SqliteLookupSource {
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
        let db_path = if !std::path::Path::new("test-data/test.db").exists() {
            "../test-data/test.db"
        } else {
            "test-data/test.db"
        };
        let s = format!(
            r#"
        {{
            "name": "join_test",
            "dbPath": "{}",
            "sqlTemplate": "select name, age from join_test where group_id = :key",
            "availableFields": [
              "name",
              "age"
            ]
        }}
        "#,
            db_path
        );
        let s: SqliteLookupSource = serde_json::from_str(&s).unwrap();
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
