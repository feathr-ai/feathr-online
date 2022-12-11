use std::collections::HashMap;

use async_trait::async_trait;
use azure_data_cosmos::prelude::{
    AuthorizationToken, CollectionClient, CosmosClient, GetDocumentResponse,
};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{IntoValue, LookupSource, PiperError, Value};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CosmosDbSource {
    account: String,
    api_key: String,
    #[serde(default)]
    database: String,
    #[serde(default)]
    collection: String,

    #[serde(skip, default)]
    client: OnceCell<CollectionClient>,
}

impl CosmosDbSource {
    fn get_client(&self) -> Result<CollectionClient, PiperError> {
        let api_key = self.api_key.clone();
        let database = self.database.clone();
        let collection = self.collection.clone();
        self.client
            .get_or_try_init(move || {
                let authorization_token = AuthorizationToken::primary_from_base64(&api_key)
                    .map_err(|e| PiperError::ExternalError(e.to_string()))?;
                let client = CosmosClient::new(&self.account, authorization_token);
                let database_client = client.database_client(database);
                Ok(database_client.collection_client(collection))
            })
            .cloned()
    }
}

#[async_trait]
impl LookupSource for CosmosDbSource {
    async fn join(&self, key: &Value, fields: &[String]) -> Vec<Vec<Value>> {
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

    fn dump(&self) -> serde_json::Value {
        json!({
            "account": self.account,
            "database": self.database,
            "collection": self.collection,
        })
    }
}
