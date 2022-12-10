use std::collections::HashMap;

use async_trait::async_trait;
use once_cell::sync::OnceCell;
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument};

use crate::{
    pipeline::{PiperError, Value, ValueType},
    Logged,
};

use super::{
    super::{get_secret, LookupSource},
    Auth,
};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpJsonApi {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    concurrency: Option<usize>,
    // Fixed part
    url_base: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    method: Option<String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    additional_headers: HashMap<String, String>,
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    additional_query_params: HashMap<String, String>,

    #[serde(default)]
    auth: Auth,

    // Key in URL
    #[serde(skip_serializing_if = "Option::is_none")]
    key_url_template: Option<String>,
    // Key in header
    #[serde(skip_serializing_if = "Option::is_none")]
    key_header: Option<String>,
    // Key in query param
    #[serde(skip_serializing_if = "Option::is_none")]
    key_query_param: Option<String>,

    // Key in request body
    // The template of request body, if key_path is also specified, the element at the path will be replaced with the key value
    #[serde(skip_serializing_if = "Option::is_none")]
    request_template: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key_path: Option<String>,

    // Defines the result fields and their JSON paths in the response
    // For now only supports extraction from response body only.
    // TODO: Support extraction from response headers
    result_path: HashMap<String, String>,

    #[serde(skip, default)]
    client: OnceCell<Client>,
    // TODO: Support auth, for now only static key in header or query param is supported
}

impl HttpJsonApi {
    async fn do_lookup(&self, k: &Value, fields: &[String]) -> Result<Vec<Vec<Value>>, PiperError> {
        // The key string will be used in url, header, and query param, but not in request body.
        let key = k
            .clone()
            .convert_to(ValueType::String)
            .get_string()?
            .into_owned();
        let url = match &self.key_url_template {
            Some(s) => format!(
                "{}{}",
                get_secret(Some(&self.url_base)).unwrap_or_default(),
                s.to_owned().replace('$', &key)
            ),
            None => get_secret(Some(&self.url_base)).unwrap_or_default(),
        };
        let m = self.method.clone().unwrap_or_else(|| "GET".to_string());
        let method = Method::from_bytes(m.to_uppercase().as_bytes())
            .log()
            .map_err(|_| PiperError::InvalidMethod(m))?;
        let client = self.client.get_or_init(Client::new);
        let req = self.auth.auth(client.request(method, url)).await?;
        let req = self.additional_headers.iter().fold(req, |req, (k, v)| {
            // Use `get_secret` in case there something like API key in the header.
            req.header(k, get_secret(Some(v)).unwrap_or_default())
        });
        let req = match &self.key_header {
            Some(k) => req.header(k, &key),
            None => req,
        };
        let req = match self.key_query_param {
            Some(ref k) => req.query(&[(k, &key)]),
            None => req,
        };
        let req = self
            .additional_query_params
            .iter()
            .fold(req, |req, (k, v)| {
                // Use `get_secret` in case there something like API key in the query param.
                req.query(&[(k, &get_secret(Some(v)).unwrap_or_default())])
            });
        let req = match self.request_template {
            Some(ref t) => match self.key_path {
                Some(ref p) => {
                    let t = t.clone();
                    // We use original key value here, not the stringified one.
                    let t = jsonpath_lib::replace_with(t, p, &mut |_| Some(k.clone().into()))
                        .log()
                        .map_err(|e| PiperError::InvalidJsonPath(e.to_string()))?;
                    req.json(&t)
                }
                None => req.json(&t),
            },
            None => req,
        };
        let resp = req
            .send()
            .await
            .log()
            .map_err(|e| PiperError::HttpError(e.to_string()))?
            .error_for_status()
            .log()
            .map_err(|e| PiperError::HttpError(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .log()
            .map_err(|e| PiperError::HttpError(e.to_string()))?;
        // Each element in this vector is a column of field values, so we need to transpose before returning.
        let fields_data: Vec<Vec<serde_json::Value>> = fields
            .iter()
            .map(|f| {
                let path = self
                    .result_path
                    .get(f)
                    .ok_or_else(|| PiperError::ColumnNotFound(f.clone()))?;
                let v = jsonpath_lib::select(&resp, path)
                    .log()
                    .map_err(|e| PiperError::InvalidJsonPath(e.to_string()))?;
                if v.is_empty() {
                    debug!("JSONPath selected no elements");
                    Ok(vec![])
                } else {
                    debug!("JSONPath selected array with {} elements", v.len());
                    Ok(v.into_iter().cloned().collect())
                }
            })
            .collect::<Result<_, _>>()?;
        // Transpose the result
        let mut ret = vec![];
        for i in 0..fields_data[0].len() {
            let mut row = vec![];
            (0..fields_data.len()).for_each(|j| {
                row.push(Value::from(fields_data.get(j).and_then(|v| v.get(i))));
            });
            ret.push(row);
        }
        Ok(ret)
    }
}

#[async_trait]
impl LookupSource for HttpJsonApi {
    #[instrument(level = "trace", skip(self))]
    async fn lookup(&self, k: &Value, fields: &[String]) -> Vec<Value> {
        let ret = self.do_lookup(k, fields).await;
        match ret {
            Ok(v) => v
                .get(0)
                .cloned()
                .unwrap_or_else(|| vec![Value::Null; fields.len()]),
            Err(e) => {
                vec![e.into(); fields.len()]
            }
        }
    }

    #[instrument(level = "trace", skip(self))]
    async fn join(&self, k: &Value, fields: &[String]) -> Vec<Vec<Value>> {
        let ret = self.do_lookup(k, fields).await;
        match ret {
            Ok(v) => v,
            Err(e) => {
                vec![vec![e.into(); fields.len()]]
            }
        }
    }

    fn dump(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap()
    }

    fn batch_size(&self) -> usize {
        self.concurrency
            .unwrap_or(super::super::DEFAULT_CONCURRENCY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::Value;

    #[tokio::test]
    async fn test_http_json_api() {
        let src = r#"
        {
            "urlBase": "https://locsvc.azurewebsites.net",
            "keyUrlTemplate": "/locations/$",
            "resultPath": {
              "id": "$.id",
              "name": "$.name"
            }
        }
        "#;
        let source: HttpJsonApi = serde_json::from_str(src).unwrap();
        let result = source
            .lookup(&Value::Int(107), &["name".to_owned(), "id".to_owned()])
            .await;
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            Value::String("577 Lakewood Dr., Bronx, NY 10473".into())
        );
        assert_eq!(result[1], Value::Long(107));
    }
}
