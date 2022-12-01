use std::{fmt::Debug, sync::Arc};

use azure_core::auth::TokenCredential;
use azure_identity::{AutoRefreshingTokenCredential, DefaultAzureCredential};
use once_cell::sync::OnceCell;
use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};

use crate::{
    common::IgnoreDebug,
    pipeline::{lookup::get_secret, PiperError},
};

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
            Auth::Basic { username, password } => request.basic_auth(
                get_secret(Some(username))?,
                Some(get_secret(password.as_ref())?),
            ),
            Auth::Header { key, value } => {
                request.header(get_secret(Some(key))?, get_secret(value.as_ref())?)
            }
            Auth::Bearer { token } => request.bearer_auth(get_secret(Some(token))?),
            Auth::Aad {
                resource,
                credential,
            } => {
                let resource = get_secret(Some(resource))?;
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
