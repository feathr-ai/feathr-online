use async_trait::async_trait;
use bb8::Pool;
use bb8_redis::RedisConnectionManager;
use protobuf::Message;
use redis::cmd;
use serde::{Deserialize, Serialize};
use tokio::sync::OnceCell;
use tracing::{debug, error, instrument};

use crate::{
    pipeline::{PiperError, Value, ValueType},
    Logged,
};

use self::generated::feathr::FeatureValue;

use super::{get_secret, LookupSource};

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/generated/mod.rs"));
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FeathrOnlineStore {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    concurrency: Option<usize>,

    host: String,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    password: Option<String>,
    #[serde(default)]
    ssl: bool,
    table: String,

    #[serde(skip, default)]
    client: OnceCell<RedisConnectionPool>,
}

#[derive(Clone, Debug)]
struct RedisConnectionPool {
    pool: Pool<RedisConnectionManager>,
}

impl RedisConnectionPool {
    #[instrument(level = "trace", skip(password))]
    async fn new(
        url: &str,
        user: &Option<String>,
        password: &Option<String>,
        ssl: bool,
    ) -> Result<RedisConnectionPool, PiperError> {
        debug!("Creating new Redis connection pool for {}", url);
        let proto = if ssl { "rediss" } else { "redis" };
        let user = get_secret(user.as_ref())?;
        let pwd = get_secret(password.as_ref())?;
        let url = if pwd.is_empty() && user.is_empty() {
            format!("{}://{}", proto, get_secret(Some(url).as_ref())?)
        } else {
            format!(
                "{}://{}:{}@{}",
                proto,
                user,
                pwd,
                get_secret(Some(url).as_ref())?
            )
        };
        let manager =
            RedisConnectionManager::new(url).map_err(|e| PiperError::RedisError(e.to_string()))?;

        let pool = Pool::builder()
            .build(manager)
            .await
            .map_err(|e| PiperError::RedisError(e.to_string()))?;
        debug!("New Redis connection pool created");
        Ok(RedisConnectionPool { pool })
    }
}

impl FeathrOnlineStore {
    #[instrument(level = "trace", skip(self))]
    async fn do_lookup(&self, key: &Value, fields: &[String]) -> Result<Vec<Value>, PiperError> {
        let client = self
            .client
            .get_or_try_init(|| async {
                RedisConnectionPool::new(&self.host, &self.user, &self.password, self.ssl).await
            })
            .await?;

        debug!("Getting connection from the Redis connection pool");
        let mut conn = client
            .pool
            .get()
            .await
            .map_err(|e| PiperError::RedisError(e.to_string()))?;

        let mut cmd = cmd("HMGET");
        // Key format is "table:key"
        cmd.arg(format!(
            "{}:{}",
            get_secret(Some(&self.table))?,
            key.clone().convert_to(ValueType::String).get_string()?
        ));
        for f in fields {
            cmd.arg(f);
        }

        debug!("Executing HMGET command");
        let resp: Vec<String> = cmd
            .query_async(&mut *conn)
            .await
            .log()
            .map_err(|e| PiperError::RedisError(e.to_string()))?;

        let ret: Vec<_> = resp
            .into_iter()
            .map(|s| {
                base64::decode(s)
                    .map_err(|e| PiperError::Base64Error(e.to_string()))
                    .and_then(|v| {
                        FeatureValue::parse_from_bytes(&v)
                            .log()
                            .map_err(|e| PiperError::ProtobufError(e.to_string()))
                    })
            })
            .map(|f| f.map(feature_to_value))
            .map(Into::into)
            .collect();
        Ok(ret)
    }
}

#[async_trait]
impl LookupSource for FeathrOnlineStore {
    async fn lookup(&self, key: &Value, fields: &[String]) -> Vec<Value> {
        match self.do_lookup(key, fields).await {
            Ok(v) => v,
            Err(e) => {
                vec![e.into(); fields.len()]
            }
        }
    }

    fn dump(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn batch_size(&self) -> usize {
        self.concurrency.unwrap_or(super::DEFAULT_CONCURRENCY)
    }
}

fn feature_to_value(f: FeatureValue) -> Value {
    // TODO: Sparse arrays
    if f.has_boolean_value() {
        f.boolean_value().into()
    } else if f.has_int_value() {
        f.int_value().into()
    } else if f.has_long_value() {
        f.long_value().into()
    } else if f.has_float_value() {
        f.float_value().into()
    } else if f.has_double_value() {
        f.double_value().into()
    } else if f.has_string_value() {
        f.string_value().to_string().into()
    } else if f.has_boolean_array() {
        f.boolean_array().booleans.clone().into()
    } else if f.has_int_array() {
        f.int_array().integers.clone().into()
    } else if f.has_long_array() {
        f.long_array().longs.clone().into()
    } else if f.has_float_array() {
        f.float_array().floats.clone().into()
    } else if f.has_double_array() {
        f.double_array().doubles.clone().into()
    } else if f.has_string_array() {
        f.string_array().strings.clone().into()
    } else {
        error!("Unsupported feature type");
        Value::Error(PiperError::RedisError(
            "Unsupported feature type".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{lookup::LookupSource, Value};

    use super::FeathrOnlineStore;

    #[tokio::test]
    async fn test_lookup() {
        dotenvy::dotenv().ok();
        let s = r#"
        {
            "host": "${REDIS_HOST}",
            "password": "${REDIS_PASSWORD}",
            "table": "${REDIS_TABLE}",
            "ssl": true
        }
        "#;
        let s: FeathrOnlineStore = serde_json::from_str(s).unwrap();
        let l = Box::new(s);
        let k: Value = 107.into();
        let fields = vec![
            "f_location_avg_fare".to_string(),
            "f_location_max_fare".to_string(),
        ];
        let ret = l.lookup(&k, &fields).await;
        println!("{:?}", ret);
        assert_eq!(ret.len(), 2);
        assert_eq!(ret[0].clone().get_int().unwrap(), 23);
        assert_eq!(ret[1].clone().get_int().unwrap(), 78);
    }
}
