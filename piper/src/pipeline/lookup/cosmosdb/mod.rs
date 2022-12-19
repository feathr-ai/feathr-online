use std::collections::HashMap;

use async_trait::async_trait;
use azure_core::request_options::MaxItemCount;
use azure_data_cosmos::prelude::{
    AuthorizationToken, CollectionClient, CosmosClient, GetDocumentResponse, Param,
    Query,
};
use futures::StreamExt;
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{IntoValue, LookupSource, PiperError, Value, Appliable};

use super::get_secret;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CosmosDbSource {
    account: String,
    api_key: String,
    database: String,
    collection: String,
    #[serde(default)]
    query: Option<String>,
    #[serde(default)]
    max_item_count: Option<i32>,

    #[serde(skip, default)]
    client: OnceCell<CollectionClient>,
}

impl CosmosDbSource {
    fn get_client(&self) -> Result<CollectionClient, PiperError> {
        let account = get_secret(Some(&self.account))?;
        let api_key = get_secret(Some(&self.api_key))?;
        let database = get_secret(Some(&self.database))?;
        let collection = get_secret(Some(&self.collection))?;
        self.client
            .get_or_try_init(move || {
                let authorization_token = AuthorizationToken::primary_from_base64(&api_key)
                    .map_err(|e| PiperError::ExternalError(e.to_string()))?;
                let client = CosmosClient::new(account, authorization_token);
                let database_client = client.database_client(database);
                Ok(database_client.collection_client(collection))
            })
            .cloned()
    }

    async fn get_doc_from_query(
        &self,
        key: &Value,
        fields: &[String],
        query: String,
    ) -> Vec<Vec<Value>> {
        let q = Query::with_params(
            query,
            vec![Param::new(
                "@key".to_string(),
                serde_json::Value::from(key.clone()),
            )],
        );
        match self.get_client() {
            Err(e) => {
                vec![vec![Value::Error(e); fields.len()]]
            }
            Ok(client) => {
                let mut resp = client
                    .query_documents(q)
                    .query_cross_partition(true)
                    .max_item_count(
                        self.max_item_count
                            .map(MaxItemCount::new)
                            .unwrap_or_default(),
                    )
                    .into_stream::<serde_json::Value>();
                let mut result = Vec::new();
                while let Some(page) = resp.next().await {
                    match page.apply(|v| {
                        println!("{:?}", v);
                        v
                    }) {
                        Ok(page) => {
                            for (doc, _) in page.results {
                                let mut row = Vec::new();
                                for field in fields {
                                    let value = match doc.get(field) {
                                        Some(v) => v.clone().into_value(),
                                        None => Value::Null,
                                    };
                                    row.push(value);
                                }
                                result.push(row);
                            }
                        }
                        Err(e) => {
                            return vec![vec![
                                Value::Error(PiperError::ExternalError(
                                    e.to_string()
                                ));
                                fields.len()
                            ]];
                        }
                    }
                }
                result
            }
        }
    }

    async fn get_doc_from_collection(&self, key: &Value, fields: &[String]) -> Vec<Vec<Value>> {
        let key = key.clone().convert_to(crate::ValueType::String);
        let doc_id = match key.get_string() {
            Ok(v) => v,
            Err(e) => {
                return vec![vec![Value::Error(e); fields.len()]];
            }
        };
        match self.get_client() {
            Err(e) => {
                vec![vec![Value::Error(e); fields.len()]]
            }
            Ok(client) => {
                let doc_client = match client.document_client(doc_id.clone(), &doc_id) {
                    Ok(v) => v,
                    Err(e) => {
                        return vec![vec![
                            Value::Error(PiperError::ExternalError(e.to_string()));
                            fields.len()
                        ]];
                    }
                };
                let resp = match doc_client
                    .get_document::<serde_json::Value>()
                    .into_future()
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        return vec![vec![
                            Value::Error(PiperError::ExternalError(e.to_string()));
                            fields.len()
                        ]];
                    }
                };
                match resp {
                    GetDocumentResponse::Found(doc) => {
                        let doc = doc.document.document;
                        let m = match doc {
                            serde_json::Value::Object(m) => m
                                .into_iter()
                                .map(|(k, v)| (k, v.into_value()))
                                .collect::<HashMap<_, _>>(),
                            _ => todo!(),
                        };
                        vec![fields
                            .iter()
                            .map(|f| m.get(f).cloned().unwrap_or_default())
                            .collect()]
                    }
                    GetDocumentResponse::NotFound(_) => {
                        vec![vec![Value::Null; fields.len()]]
                    }
                }
            }
        }
    }
}

#[async_trait]
impl LookupSource for CosmosDbSource {
    async fn join(&self, key: &Value, fields: &[String]) -> Vec<Vec<Value>> {
        match &self.query {
            Some(q) => self.get_doc_from_query(key, fields, q.clone()).await,
            None => self.get_doc_from_collection(key, fields).await,
        }
    }

    fn dump(&self) -> serde_json::Value {
        json!({
            "account": self.account,
            "database": self.database,
            "collection": self.collection,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{Value, LookupSource};

    use super::CosmosDbSource;

    #[tokio::test]
    async fn test_lookup_by_key() {
        dotenvy::dotenv().ok();
        if std::env::var("COSMOS_ACCOUNT").is_err() {
            println!("CosmosDb is not configured, skip the test.");
            return;
        }
        let s = r#"
        {
            "account": "${COSMOS_ACCOUNT}",
            "apiKey": "${COSMOS_API_KEY}",
            "database": "${COSMOS_DATABASE}",
            "collection": "${COSMOS_COLLECTION}"
        }
        "#;
        let s: CosmosDbSource = serde_json::from_str(s).unwrap();
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


    #[tokio::test]
    async fn test_lookup_by_query() {
        dotenvy::dotenv().ok();
        if std::env::var("COSMOS_ACCOUNT").is_err() {
            println!("CosmosDb is not configured, skip the test.");
            return;
        }
        let s = r#"
        {
            "account": "${COSMOS_ACCOUNT}",
            "apiKey": "${COSMOS_API_KEY}",
            "database": "${COSMOS_DATABASE}",
            "collection": "${COSMOS_COLLECTION}",
            "query": "SELECT * FROM table1 c WHERE c.key0 = @key"
        }
        "#;
        let s: CosmosDbSource = serde_json::from_str(s).unwrap();
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