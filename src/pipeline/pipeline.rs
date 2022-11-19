use std::collections::HashMap;

use serde_json::json;
use tracing::debug;

use crate::Appliable;

use super::{
    expression::{ColumnExpression, Expression, LiteralExpression, OperatorExpression},
    lookup::init_lookup_sources,
    operator::PlusOperator,
    parser::{parse_pipeline, parse_script},
    transformation::{ProjectTransformation, Transformation},
    Column, DataSet, DataSetCreator, DataSetValidator, PiperError, Schema, ValidationMode, Value,
    ValueType,
};

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
    pub fn load(script: &str, lookup_def: &str) -> Result<HashMap<String, Self>, PiperError> {
        debug!("Loading lookup data sources");
        init_lookup_sources(lookup_def)?.then(|s| debug!("{} lookup data sources loaded", s));
        debug!("Loading pipeline definitions");
        Ok(parse_script(script)?.then(|p| {
            debug!("{} pipeline definitions loaded", p.len());
        }))
    }

    /**
     * Load a pipeline from the DSL definition.
     */
    #[allow(dead_code)]
    pub fn parse(input: &str) -> Result<Self, PiperError> {
        parse_pipeline(input)
    }

    /// Returns a health checking pipeline, which does `a as int + 42`
    pub fn get_health_checker() -> Pipeline {
        let input_schema = Schema::from(vec![Column::new("a", ValueType::Int)]);
        let transformation = ProjectTransformation::new(
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
            .try_fold(input, |input, stage| {
                stage
                    .transformation
                    .transform(input.validated(validation_mode))
            })
            .map(|dataset| dataset.validated(validation_mode))
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
    use crate::pipeline::{DataSet, DataSetCreator, Value};

    use super::Pipeline;

    fn gen_ds(pipeline: &Pipeline) -> Box<dyn DataSet> {
        DataSetCreator::eager(
            pipeline.input_schema.clone(),
            vec![
                vec![Value::from(10), Value::from(100), Value::from(true)],
                vec![Value::from(20), Value::from(200), Value::from(true)],
                vec![Value::from(30), Value::from(300), Value::from(false)],
                vec![Value::from(40), Value::from(400), Value::from(false)],
                vec![Value::from(50), Value::from(500), Value::from("false")],
                vec![Value::from(60), Value::from("600"), Value::from(false)],
                vec![Value::from(70), Value::from(700), Value::from(false)],
                vec![Value::from(80), Value::from(800), Value::from(true)],
            ],
        )
    }

    #[tokio::test]
    async fn test_pipeline() {
        let pipeline = super::Pipeline::parse(
            "test_pipeline(a as int, b as int, c as bool)
            | where not c and (a>42)
            ;",
        )
        .unwrap();

        println!("{}", pipeline.dump());

        // Strict mode should output 1 row and 2 errors
        let (schema, rows) = pipeline
            .process(gen_ds(&pipeline), crate::pipeline::ValidationMode::Strict)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema, pipeline.output_schema);
        assert_eq!(rows.len(), 3);
        println!("{:?}", rows);
        assert!(rows[0].is_err());
        assert!(rows[1].is_err());
        // Succeeded row has a==70
        assert_eq!(rows[2].as_ref().unwrap()[0], 70.into());

        // Skip mode should output 1 rows, 2 error rows are skipped
        let (schema, rows) = pipeline
            .process(gen_ds(&pipeline), crate::pipeline::ValidationMode::Skip)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema, pipeline.output_schema);
        println!("{:?}", rows);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].as_ref().unwrap()[0], 70.into());

        // Lenient mode should output 2 rows, the 1st has field `b` as null because of the type cast failure
        let (schema, rows) = pipeline
            .process(gen_ds(&pipeline), crate::pipeline::ValidationMode::Lenient)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema, pipeline.output_schema);
        println!("{:?}", rows);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].as_ref().unwrap()[0], 60.into());
        assert_eq!(rows[0].as_ref().unwrap()[1], Value::Null);
        assert_eq!(rows[1].as_ref().unwrap()[0], 70.into());

        // Convert mode should output 3 rows with converted values
        let (schema, rows) = pipeline
            .process(gen_ds(&pipeline), crate::pipeline::ValidationMode::Convert)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema, pipeline.output_schema);
        println!("{:?}", rows);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].as_ref().unwrap()[0], 50.into());
        assert_eq!(rows[0].as_ref().unwrap()[2], false.into());
        assert_eq!(rows[1].as_ref().unwrap()[0], 60.into());
        assert_eq!(rows[1].as_ref().unwrap()[1], 600.into());
        assert_eq!(rows[2].as_ref().unwrap()[0], 70.into());
    }

    #[tokio::test]
    async fn test_explode() {
        let pipeline = super::Pipeline::parse(
            "test_pipeline(a as int, b as array)
            | explode b as int
            ;",
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
            .process(ds, crate::pipeline::ValidationMode::Convert)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema, pipeline.output_schema);
        assert_eq!(rows.len(), 9);
        assert_eq!(rows[0].as_ref().unwrap()[0], 10.into());
        assert_eq!(rows[0].as_ref().unwrap()[1], 1.into());
        assert_eq!(rows[1].as_ref().unwrap()[0], 10.into());
        assert_eq!(rows[1].as_ref().unwrap()[1], 2.into());
        assert_eq!(rows[2].as_ref().unwrap()[0], 10.into());
        assert_eq!(rows[2].as_ref().unwrap()[1], 3.into());
        assert_eq!(rows[3].as_ref().unwrap()[0], 40.into());
        assert_eq!(rows[3].as_ref().unwrap()[1], 400.into());
        assert_eq!(rows[4].as_ref().unwrap()[0], 50.into());
        assert_eq!(rows[4].as_ref().unwrap()[1], 4.into());
        assert_eq!(rows[5].as_ref().unwrap()[0], 50.into());
        assert_eq!(rows[5].as_ref().unwrap()[1], 5.into());
        assert_eq!(rows[6].as_ref().unwrap()[0], 50.into());
        assert_eq!(rows[6].as_ref().unwrap()[1], 6.into());
        assert_eq!(rows[7].as_ref().unwrap()[0], 60.into());
        assert_eq!(rows[7].as_ref().unwrap()[1], 600.into());
        assert_eq!(rows[8].as_ref().unwrap()[0], 80.into());
        assert_eq!(rows[8].as_ref().unwrap()[1], 800.into());
    }
}
