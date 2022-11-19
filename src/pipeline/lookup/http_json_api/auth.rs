use reqwest::RequestBuilder;
use serde::{Deserialize, Serialize};

use crate::pipeline::{lookup::get_secret, PiperError};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum Auth {
    None,
    Basic { username: String, password: String },
    Bearer { token: String },
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
                Some(username) => request.basic_auth(username, get_secret(Some(password))),
                None => request,
            },
            Auth::Bearer { token } => match get_secret(Some(token)) {
                Some(token) => request.bearer_auth(token),
                None => request,
            },
        })
    }
}
