use std::{fmt::Debug, sync::Arc};

use azure_core::auth::TokenCredential;
use azure_identity::{AutoRefreshingTokenCredential, DefaultAzureCredential};
use once_cell::sync::OnceCell;
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};

use crate::{pipeline::{lookup::get_secret, PiperError}, common::IgnoreDebug};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Auth {
    None,
    Basic {
        username: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        password: Option<String>,
    },
    Header {
        key: String,
        #[serde(skip_serializing_if = "Option::is_none", default)]
        value: Option<String>,
    },
    Bearer {
        token: String,
    },
    Aad {
        resource: String,
        #[serde(skip, default)]
        credential: OnceCell<IgnoreDebug<AutoRefreshingTokenCredential>>,
    },
}

impl Default for Auth {
    fn default() -> Self {
        Self::None
    }
}

impl Auth {
    pub async fn auth(&self, request: RequestBuilder) -> Result<RequestBuilder, PiperError> {
        Ok(match self {
            Auth::None => request,
            Auth::Basic { username, password } => match get_secret(Some(username)) {
                Some(username) => request.basic_auth(username, get_secret(password.as_ref())),
                None => request,
            },
            Auth::Header { key, value } => match get_secret(Some(key)) {
                Some(key) => match get_secret(value.as_ref()) {
                    Some(value) => request.header(key, value),
                    None => request,
                },
                None => request,
            },
            Auth::Bearer { token } => match get_secret(Some(token)) {
                Some(token) => request.bearer_auth(token),
                None => request,
            },
            Auth::Aad {
                resource,
                credential,
            } => {
                let resource =
                    get_secret(Some(resource).as_ref()).unwrap_or_else(|| resource.to_string());
                let credential = credential.get_or_init(|| IgnoreDebug {
                    inner: AutoRefreshingTokenCredential::new(Arc::new(
                        DefaultAzureCredential::default(),
                    )),
                });
                let token =
                    credential.inner.get_token(&resource).await.map_err(|e| {
                        PiperError::AuthError(format!("Failed to get token: {}", e))
                    })?;
                request.bearer_auth(token.token.secret())
            }
        })
    }
}
