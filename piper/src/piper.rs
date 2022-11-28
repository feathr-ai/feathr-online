use std::{collections::HashMap, time::Instant};

use azure_core::auth::TokenCredential;
use azure_identity::{DefaultAzureCredential, DefaultAzureCredentialBuilder};
use futures::future::join_all;
use tracing::{debug, instrument};

use crate::{
    common::IgnoreDebug,
    pipeline::{
        BuildContext, ErrorCollector, Pipeline,
        PiperError, ValidationMode, Value,
    },
    Appliable, Args, Logged, Request, Response, SingleRequest, SingleResponse,
};

#[derive(Debug)]
pub struct Piper {
    pub(crate) pipelines: HashMap<String, Pipeline>,
    pub(crate) ctx: IgnoreDebug<BuildContext>,
}

impl Piper {
    pub async fn new(args: Args) -> Result<Self, PiperError> {
        let pipeline_def = load_file(&args.pipeline, args.enable_managed_identity).await?;
        let lookup_def = load_file(&args.lookup, args.enable_managed_identity).await?;

        let ctx = BuildContext::from_config(&lookup_def)?;

        let mut pipelines = Pipeline::load(&pipeline_def, &ctx).log()?;
        // Use invalid identifier as the name, avoid clashes with user-defined pipelines
        pipelines.insert("%health".to_string(), Pipeline::get_health_checker());
        Ok(Self {
            pipelines,
            ctx: IgnoreDebug { inner: ctx },
        })
    }

    pub async fn health_check(&self) -> bool {
        let (_, ret) = self
            .pipelines
            .get("%health")
            .unwrap()
            .process_row(vec![Value::Int(57)], ValidationMode::Strict)
            .unwrap()
            .eval()
            .await;
        if (ret.len() == 1) && (ret[0].len() == 2) {
            matches!(ret[0][1], Value::Int(99))
        } else {
            false
        }
    }

    pub fn get_pipelines(&self) -> HashMap<String, serde_json::Value> {
        self.pipelines
            .values()
            .map(|p| (p.name.clone(), p.to_json()))
            .collect()
    }

    pub fn get_lookup_sources(&self) -> serde_json::Value {
        self.ctx.inner.dump_lookup_sources()
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn process(&self, req: Request) -> Result<Response, PiperError> {
        debug!(
            "Received request, contains {} sub-requests",
            req.requests.len()
        );
        let futures: Vec<_> = req
            .requests
            .into_iter()
            .map(|r| async {
                let pipeline = r.pipeline.clone();
                let r = self.process_single_request(r).await;
                match r {
                    Ok(r) => r,
                    Err(e) => SingleResponse {
                        pipeline,
                        status: format!("ERROR: {}", e),
                        time: None,
                        count: None,
                        data: None,
                        errors: vec![],
                    },
                }
            })
            .collect();
        let results = join_all(futures).await;
        Ok(Response { results })
    }

    #[instrument(level = "trace", skip(self))]
    async fn process_single_request(
        &self,
        req: SingleRequest,
    ) -> Result<SingleResponse, PiperError> {
        let pipeline = self
            .pipelines
            .get(&req.pipeline)
            .ok_or_else(|| PiperError::PipelineNotFound(req.pipeline.clone()))?;
        debug!("Processing request to pipeline {}", pipeline.name);

        let schema = &pipeline.input_schema;

        let row: Vec<Value> = schema
            .columns
            .iter()
            .map(|c| {
                req.data
                    .get(c.name.as_str())
                    .map(|v| Value::from(v.clone()))
                    .unwrap_or_default()
            })
            .collect();

        let now = Instant::now();
        let (ret, errors) = pipeline
            .process_row(
                row,
                if req.validate {
                    ValidationMode::Strict
                } else {
                    ValidationMode::Lenient
                },
            )?
            .eval()
            .await
            .collect_into_json(req.errors);
        Ok(SingleResponse {
            pipeline: req.pipeline,
            status: "OK".to_owned(),
            time: Some((now.elapsed().as_micros() as f64) / 1000f64),
            count: Some(ret.len()),
            data: Some(ret),
            errors,
        })
    }
}

async fn make_request(
    url: &str,
    enable_managed_identity: bool,
) -> Result<reqwest::RequestBuilder, PiperError> {
    let client = reqwest::Client::new();
    if url.starts_with("https://") && url.contains(".blob.core.windows.net/") {
        // It's on Azure Storage Blob
        let credential = if enable_managed_identity {
            DefaultAzureCredential::default()
        } else {
            DefaultAzureCredentialBuilder::new()
                .exclude_managed_identity_credential()
                .build()
        };
        let token = credential
            .get_token("https://storage.azure.com/")
            .await
            .log()
            .map_err(|e| PiperError::AuthError(format!("{:?}", e)))
            .map(|t| t.token.secret().to_string())
            .ok();
        match token {
            // Acquired token and use it
            Some(t) => Ok(client
                .get(url)
                // @see: https://learn.microsoft.com/en-us/azure/storage/common/storage-auth-aad-app?tabs=dotnet#create-a-block-blob
                .header("x-ms-version", "2017-11-09")
                .bearer_auth(t)),
            // We don't have token, assume it's public accessible
            None => Ok(client.get(url)),
        }
    } else {
        Ok(client.get(url))
    }
}

async fn load_file(path: &str, enable_managed_identity: bool) -> Result<String, PiperError> {
    debug!("Reading file at {}", path);
    Ok(if path.starts_with("http:") || path.starts_with("https:") {
        let resp = make_request(path, enable_managed_identity)
            .await?
            .send()
            .await
            .log()
            .map_err(|e| PiperError::Unknown(e.to_string()))?;
        resp.text()
            .await
            .log()
            .map_err(|e| PiperError::Unknown(e.to_string()))
    } else {
        tokio::fs::read_to_string(path)
            .await
            .log()
            .map_err(|e| PiperError::Unknown(e.to_string()))
    }?
    .then(|s| {
        debug!(
            "Successfully read file at {}, file length is {}",
            path,
            s.len()
        );
    }))
}
