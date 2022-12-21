use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use futures::future::join_all;
use tracing::{debug, instrument};

use crate::{
    common::IgnoreDebug,
    pipeline::{
        BuildContext, DataSetCreator, ErrorCollector, Pipeline, PiperError, ValidationMode, Value,
    },
    Function, Logged, LookupRequest, LookupResponse, LookupSource, Request, Response,
    SingleRequest, SingleResponse,
};

#[derive(Debug)]
pub struct Piper {
    pub pipelines: HashMap<String, Pipeline>,
    pub ctx: IgnoreDebug<BuildContext>,
}

impl Piper {
    /**
     * Create a new Piper instance from a pipeline definition and a lookup source definition.
     */
    pub fn new(pipeline_def: &str, lookup_def: &str) -> Result<Self, PiperError> {
        let ctx = BuildContext::from_config(lookup_def)?;

        let mut pipelines = Pipeline::load(pipeline_def, &ctx).log()?;
        // Use invalid identifier as the name, avoid clashes with user-defined pipelines
        pipelines.insert("%health".to_string(), Pipeline::get_health_checker());
        Ok(Self {
            pipelines,
            ctx: IgnoreDebug::new(ctx),
        })
    }

    /**
     * Create a new Piper instance from a pipeline definition and a lookup source definition.
     */
    pub fn new_with_udf(
        pipeline_def: &str,
        lookup_def: &str,
        udf: HashMap<String, Box<dyn Function>>,
    ) -> Result<Self, PiperError> {
        let ctx = BuildContext::from_config_with_udf(lookup_def, udf)?;

        let mut pipelines = Pipeline::load(pipeline_def, &ctx).log()?;
        // Use invalid identifier as the name, avoid clashes with user-defined pipelines
        pipelines.insert("%health".to_string(), Pipeline::get_health_checker());
        Ok(Self {
            pipelines,
            ctx: IgnoreDebug::new(ctx),
        })
    }

    /**
     * Create a new Piper instance from a pipeline definition and a lookup source map and UDF map.
     */
    pub fn new_with_lookup_udf(
        pipeline_def: &str,
        lookup: HashMap<String, Arc<dyn LookupSource>>,
        udf: HashMap<String, Box<dyn Function>>,
    ) -> Result<Self, PiperError> {
        let ctx = BuildContext::new_with_lookup_udf(lookup, udf)?;

        let mut pipelines = Pipeline::load(pipeline_def, &ctx).log()?;
        // Use invalid identifier as the name, avoid clashes with user-defined pipelines
        pipelines.insert("%health".to_string(), Pipeline::get_health_checker());
        Ok(Self {
            pipelines,
            ctx: IgnoreDebug::new(ctx),
        })
    }

    /**
     * Get a list of all predefined functions, include UDF.
     */
    pub fn get_functions(&self) -> HashSet<String> {
        self.ctx.functions.keys().cloned().collect()
    }

    /**
     *  Run basic health check on the pipeline.
     */
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

    /**
     * Return string representations of all pipelines.
     */
    pub fn get_pipelines(&self) -> HashMap<String, serde_json::Value> {
        self.pipelines
            .values()
            .map(|p| (p.name.clone(), p.to_json()))
            .collect()
    }

    /**
     * Return a JSON representation of all lookup sources.
     */
    pub fn get_lookup_sources(&self) -> serde_json::Value {
        self.ctx.dump_lookup_sources()
    }

    /**
     * Lookup a single key.
     */
    #[instrument(level = "trace", skip(self))]
    pub async fn lookup(&self, req: LookupRequest) -> Result<LookupResponse, PiperError> {
        let src = self
            .ctx
            .lookup_sources
            .get(&req.source)
            .ok_or_else(|| PiperError::LookupSourceNotFound(req.source.clone()))?;
        let mut data: Vec<HashMap<String, serde_json::Value>> = vec![];
        for key in req.keys.iter() {
            let features = src.lookup(&Value::from(key), &req.features).await;
            data.push(
                req.features
                    .iter()
                    .zip(features.into_iter())
                    .map(|(f, v)| (f.clone(), v.into()))
                    .collect(),
            );
        }
        Ok(LookupResponse { data })
    }

    /**
     * Process a composited request.
     */
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

    /**
     * Process a single request.
     */
    #[instrument(level = "trace", skip(self))]
    pub async fn process_single_request(
        &self,
        req: SingleRequest,
    ) -> Result<SingleResponse, PiperError> {
        let pipeline = self
            .pipelines
            .get(&req.pipeline)
            .ok_or_else(|| PiperError::PipelineNotFound(req.pipeline.clone()))?;
        debug!("Processing request to pipeline {}", pipeline.name);

        let schema = &pipeline.input_schema;

        let now = Instant::now();
        let (ret, errors) = match req.data {
            crate::RequestData::Single(data) => {
                let row = schema
                    .columns
                    .iter()
                    .map(|c| {
                        data.get(c.name.as_str())
                            .map(|v| Value::from(v.clone()))
                            .unwrap_or_default()
                    })
                    .collect();
                pipeline.process_row(
                    row,
                    if req.validate {
                        ValidationMode::Strict
                    } else {
                        ValidationMode::Lenient
                    },
                )
            }
            crate::RequestData::Multi(data) => {
                let rows = data.into_iter().map(|row| {
                    schema
                        .columns
                        .iter()
                        .map(|c| {
                            row.get(c.name.as_str())
                                .map(|v| Value::from(v.clone()))
                                .unwrap_or_default()
                        })
                        .collect()
                });
                let dataset = DataSetCreator::eager(schema.clone(), rows);
                pipeline.process(
                    dataset,
                    if req.validate {
                        ValidationMode::Strict
                    } else {
                        ValidationMode::Lenient
                    },
                )
            }
        }?
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::{Function, Piper, PiperError, Request};

    #[test]
    fn test_dup_columns() {
        let p = Piper::new("test_pipeline(a,b) | lookup b from src on a ;", "");
        // Try to define a udf with the same name as a built-in function
        assert!(matches!(p, Err(PiperError::ColumnAlreadyExists(_))));

        let p = Piper::new("test_pipeline(a,b) | project-rename b=a ;", "");
        // Try to define a udf with the same name as a built-in function
        assert!(matches!(p, Err(PiperError::ColumnAlreadyExists(_))));

        let p = Piper::new("test_pipeline(a,b) | project b=sqrt(a) ;", "");
        // Try to define a udf with the same name as a built-in function
        assert!(matches!(p, Err(PiperError::ColumnAlreadyExists(_))));

        let p = Piper::new("test_pipeline(a) | project b=sqrt(a) ;", "");
        // Try to define a udf with the same name as a built-in function
        assert!(p.is_ok());
    }

    #[test]
    fn test_with_udf() {
        let udf = crate::pipeline::unary_fn(f64::sqrt) as Box<dyn Function>;
        let p = Piper::new_with_udf(
            "test_pipeline(a) | project b=sqrt(a) ;",
            "",
            vec![("sqrt".to_string(), udf)].into_iter().collect(),
        );
        // Try to define a udf with the same name as a built-in function
        assert!(matches!(p, Err(PiperError::FunctionAlreadyDefined(_))));
    }

    #[tokio::test]
    async fn test_piper() {
        let p = Piper::new("test_pipeline(a) | project b=a+42, c=a-42 ;", "").unwrap();
        let r = p
            .process_single_request(crate::SingleRequest {
                pipeline: "test_pipeline".to_string(),
                data: crate::RequestData::Multi(vec![
                    vec![("a".to_string(), json!(1))].into_iter().collect(),
                    vec![("a".to_string(), json!(2))].into_iter().collect(),
                    vec![("a".to_string(), json!(3))].into_iter().collect(),
                ]),
                validate: false,
                errors: crate::ErrorCollectingMode::On,
            })
            .await
            .unwrap();
        assert_eq!(r.data.as_ref().unwrap().len(), 3);
        assert_eq!(r.data.as_ref().unwrap()[0]["b"], json!(43));
        assert_eq!(r.data.as_ref().unwrap()[0]["c"], json!(-41));
        assert_eq!(r.data.as_ref().unwrap()[1]["b"], json!(44));
        assert_eq!(r.data.as_ref().unwrap()[1]["c"], json!(-40));
        assert_eq!(r.data.as_ref().unwrap()[2]["b"], json!(45));
        assert_eq!(r.data.as_ref().unwrap()[2]["c"], json!(-39));

        let req = crate::SingleRequest {
            pipeline: "test_pipeline".to_string(),
            data: crate::RequestData::Multi(vec![
                vec![("a".to_string(), json!(1))].into_iter().collect(),
                vec![("a".to_string(), json!(2))].into_iter().collect(),
                vec![("a".to_string(), json!(3))].into_iter().collect(),
            ]),
            validate: false,
            errors: crate::ErrorCollectingMode::On,
        };

        let r = p.process_single_request(req.clone()).await.unwrap();
        assert_eq!(r.data.as_ref().unwrap().len(), 3);
        assert_eq!(r.data.as_ref().unwrap()[0]["b"], json!(43));
        assert_eq!(r.data.as_ref().unwrap()[0]["c"], json!(-41));
        assert_eq!(r.data.as_ref().unwrap()[1]["b"], json!(44));
        assert_eq!(r.data.as_ref().unwrap()[1]["c"], json!(-40));
        assert_eq!(r.data.as_ref().unwrap()[2]["b"], json!(45));
        assert_eq!(r.data.as_ref().unwrap()[2]["c"], json!(-39));

        let req = Request {
            requests: vec![req],
        };
        let mut r = p.process(req).await.unwrap();
        assert_eq!(r.results.len(), 1);
        let r = r.results.remove(0);

        assert_eq!(r.data.as_ref().unwrap().len(), 3);
        assert_eq!(r.data.as_ref().unwrap()[0]["b"], json!(43));
        assert_eq!(r.data.as_ref().unwrap()[0]["c"], json!(-41));
        assert_eq!(r.data.as_ref().unwrap()[1]["b"], json!(44));
        assert_eq!(r.data.as_ref().unwrap()[1]["c"], json!(-40));
        assert_eq!(r.data.as_ref().unwrap()[2]["b"], json!(45));
        assert_eq!(r.data.as_ref().unwrap()[2]["c"], json!(-39));

        assert!(p.health_check().await);
    }
}
