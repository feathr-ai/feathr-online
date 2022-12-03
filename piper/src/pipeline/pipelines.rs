use std::{collections::HashMap, sync::Arc};

use serde_json::json;
use tracing::debug;

use crate::{Appliable, Function};

use super::{
    expression::{ColumnExpression, Expression, LiteralExpression, OperatorExpression},
    init_built_in_functions,
    lookup::{init_lookup_sources, LookupSource},
    operator::PlusOperator,
    parser::{parse_pipeline, parse_script},
    transformation::{ProjectTransformation, Transformation},
    Column, DataSet, DataSetCreator, PiperError, Schema, Validated, ValidationMode, Value,
    ValueType,
};

pub struct BuildContext {
    pub functions: HashMap<String, Box<dyn Function>>,
    pub lookup_sources: HashMap<String, Arc<dyn LookupSource>>,
}

impl BuildContext {
    pub fn from_config(lookup_source_def: &str) -> Result<Self, PiperError> {
        Ok(Self {
            functions: init_built_in_functions(),
            lookup_sources: init_lookup_sources(lookup_source_def)?
                .then(|s| debug!("{} lookup data sources loaded", s.len())),
        })
    }

    pub fn from_config_with_udf(
        lookup_source_def: &str,
        udf: HashMap<String, Box<dyn Function>>,
    ) -> Result<Self, PiperError> {
        let mut functions = init_built_in_functions();
        for (name, func) in udf {
            if functions.contains_key(&name) {
                return Err(PiperError::FunctionAlreadyDefined(name));
            }
            functions.insert(name, func);
        }
        Ok(Self {
            functions,
            lookup_sources: init_lookup_sources(lookup_source_def)?
                .then(|s| debug!("{} lookup data sources loaded", s.len())),
        })
    }

    pub fn new_with_lookup_udf(
        lookup: HashMap<String, Arc<dyn LookupSource>>,
        udf: HashMap<String, Box<dyn Function>>,
    ) -> Result<Self, PiperError> {
        let mut functions = init_built_in_functions();
        for (name, func) in udf {
            if functions.contains_key(&name) {
                return Err(PiperError::FunctionAlreadyDefined(name));
            }
            functions.insert(name, func);
        }
        Ok(Self {
            functions,
            lookup_sources: lookup,
        })
    }

    pub fn dump_lookup_sources(&self) -> serde_json::Value {
        json!(self
            .lookup_sources
            .iter()
            .map(|(k, v)| (k, v.dump()))
            .collect::<HashMap<_, _>>())
    }

    pub fn get_lookup_source(&self, name: &str) -> Result<Arc<dyn LookupSource>, PiperError> {
        self.lookup_sources
            .get(name)
            .cloned()
            .ok_or_else(|| PiperError::LookupSourceNotFound(name.to_owned()))
    }
}

impl Default for BuildContext {
    fn default() -> Self {
        Self {
            functions: init_built_in_functions(),
            lookup_sources: HashMap::new(),
        }
    }
}

/**
 * One transformation stage
 */
#[derive(Debug)]
pub struct Stage {
    /**
     * The input schema of this stage
     */
    pub input_schema: Schema,

    /**
     * The output schema of this stage
     */
    pub output_schema: Schema,

    /**
     * The transformation that transforms the input data set to the output data set
     */
    pub transformation: Box<dyn Transformation>,
}

/**
 * A transformation pipeline
 */
#[derive(Debug)]
pub struct Pipeline {
    /**
     * The name of the pipeline
     */
    pub name: String,

    /**
     * The input schema of the pipeline
     */
    pub input_schema: Schema,

    /**
     * The output schema of the pipeline
     */
    pub output_schema: Schema,

    /**
     * The transformation stages
     */
    pub transformations: Vec<Stage>,
}

impl Stage {
    pub fn new(input_schema: Schema, transformation: Box<dyn Transformation>) -> Self {
        let output_schema = transformation.get_output_schema(&input_schema);
        Self {
            input_schema,
            output_schema,
            transformation,
        }
    }
}

impl Pipeline {
    /**
     * Load DSL script and lookup source config to build name/pipeline map.
     */
    pub fn load(script: &str, ctx: &BuildContext) -> Result<HashMap<String, Self>, PiperError> {
        debug!("Loading lookup data sources");
        debug!("Loading pipeline definitions");
        Ok(parse_script(script, ctx)?.then(|p| {
            debug!("{} pipeline definitions loaded", p.len());
        }))
    }

    /**
     * Load a pipeline from the DSL definition.
     */
    #[allow(dead_code)]
    pub fn parse(input: &str, ctx: &BuildContext) -> Result<Self, PiperError> {
        parse_pipeline(input, ctx)
    }

    /// Returns a health checking pipeline, which does `a as int + 42`
    pub fn get_health_checker() -> Pipeline {
        let input_schema = Schema::from(vec![Column::new("a", ValueType::Int)]);
        let transformation = ProjectTransformation::create(
            &input_schema,
            vec![(
                "b".to_string(),
                Box::new(OperatorExpression {
                    operator: Box::new(PlusOperator),
                    arguments: vec![
                        Box::new(ColumnExpression {
                            column_name: "a".to_string(),
                            column_index: 0,
                        }) as Box<dyn Expression>,
                        Box::new(LiteralExpression {
                            value: Value::Int(42),
                        }) as Box<dyn Expression>,
                    ],
                }),
            )],
        )
        .unwrap();
        let output_schema = transformation.get_output_schema(&input_schema);
        let stage = Stage::new(input_schema.clone(), transformation);
        Self {
            name: "%health".to_string(),
            input_schema,
            output_schema,
            transformations: vec![stage],
        }
    }

    /**
     * Dump the pipeline to JSON.
     */
    #[allow(dead_code)]
    pub fn to_json(&self) -> serde_json::Value {
        let input_schema = serde_json::to_value(
            &self
                .input_schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.column_type.to_string()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap();
        let output_schema = serde_json::to_value(
            &self
                .output_schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.column_type.to_string()))
                .collect::<HashMap<_, _>>(),
        )
        .unwrap();
        let definition = self.dump();
        json!({
            "name": self.name,
            "inputSchema": input_schema,
            "outputSchema": output_schema,
            "definition": definition,
        })
    }

    /**
     * Process a single record of the input data
     */
    pub fn process_row(
        &self,
        input: Vec<Value>,
        validation_mode: ValidationMode,
    ) -> Result<Box<dyn DataSet>, PiperError> {
        self.process(
            DataSetCreator::eager(self.input_schema.clone(), vec![input]),
            validation_mode,
        )
    }

    /**
     * Process a dataset.
     */
    pub fn process(
        &self,
        input: Box<dyn DataSet>,
        validation_mode: ValidationMode,
    ) -> Result<Box<dyn DataSet>, PiperError> {
        self.transformations
            .iter()
            .try_fold(input.validated(validation_mode), |input, stage| {
                stage
                    .transformation
                    .transform(input)
                    .map(|output| output.validated(validation_mode))
            })
    }

    /**
     * Dump the pipeline source.
     */
    pub fn dump(&self) -> String {
        let mut ret = format!("{}({})\n", self.name, self.input_schema.dump());
        for stage in &self.transformations {
            ret.push_str(&format!("| {}\n", stage.transformation.dump()));
        }
        ret.push(';');
        ret
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{pipelines::BuildContext, DataSetCreator, Value};

    #[tokio::test]
    async fn test_explode() {
        let pipeline = super::Pipeline::parse(
            "test_pipeline(a as int, b as array)
            | explode b as int
            ;",
            &BuildContext::default(),
        )
        .unwrap();
        let ds = DataSetCreator::eager(
            pipeline.input_schema.clone(),
            vec![
                vec![Value::from(10), Value::from(vec![1, 2, 3])],
                vec![Value::from(20), Value::from(Vec::<i32>::new())],
                vec![Value::from(30), Value::from(Vec::<i32>::new())],
                vec![Value::from(40), Value::from(vec![400])],
                vec![Value::from(50), Value::from(vec![4, 5, 6])],
                vec![Value::from(60), Value::from(vec![600])],
                vec![Value::from(70), Value::from(Vec::<i32>::new())],
                vec![Value::from(80), Value::from(vec![800])],
            ],
        );
        let (schema, rows) = pipeline
            .process(ds, crate::pipeline::ValidationMode::Strict)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema, pipeline.output_schema);
        assert_eq!(rows.len(), 9);
        assert_eq!(rows[0][0], 10.into());
        assert_eq!(rows[0][1], 1.into());
        assert_eq!(rows[1][0], 10.into());
        assert_eq!(rows[1][1], 2.into());
        assert_eq!(rows[2][0], 10.into());
        assert_eq!(rows[2][1], 3.into());
        assert_eq!(rows[3][0], 40.into());
        assert_eq!(rows[3][1], 400.into());
        assert_eq!(rows[4][0], 50.into());
        assert_eq!(rows[4][1], 4.into());
        assert_eq!(rows[5][0], 50.into());
        assert_eq!(rows[5][1], 5.into());
        assert_eq!(rows[6][0], 50.into());
        assert_eq!(rows[6][1], 6.into());
        assert_eq!(rows[7][0], 60.into());
        assert_eq!(rows[7][1], 600.into());
        assert_eq!(rows[8][0], 80.into());
        assert_eq!(rows[8][1], 800.into());
    }
}
