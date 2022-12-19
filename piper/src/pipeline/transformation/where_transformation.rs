use std::sync::Arc;

use async_trait::async_trait;

use crate::pipeline::{expression::Expression, DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Debug)]
pub struct WhereTransformation {
    pub predicate: Arc<dyn Expression>,
}

impl Transformation for WhereTransformation {
    fn get_output_schema(&self, input_schema: &Schema) -> Schema {
        input_schema.clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(WhereDataSet {
            input: dataset,
            predicate: self.predicate.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!("where {}", self.predicate.dump())
    }
}

struct WhereDataSet {
    input: Box<dyn DataSet>,
    predicate: Arc<dyn Expression>,
}

#[async_trait]
impl DataSet for WhereDataSet {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        loop {
            let row = self.input.next().await?;
            let predicate = self.predicate.eval(&row);
            match predicate.get_bool() {
                Ok(true) => return Some(row),
                // Filtered out
                Ok(false) => continue,
                // Skip predicate error
                Err(_) => continue,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{pipeline::{pipelines::BuildContext, DataSetCreator, Value, Pipeline}, PiperError};

    #[tokio::test]
    async fn test_explode() {
        let pipeline = Pipeline::parse(
            "test_pipeline(a as int, b as array)
            | where a > 20
            ;",
            &BuildContext::default(),
        )
        .unwrap();
        let ds = DataSetCreator::eager(
            pipeline.input_schema.clone(),
            vec![
                vec![Value::from(10), Value::from(vec![1, 2, 3])],
                vec![Value::from(10), Value::from(Vec::<i32>::new())],
                vec![Value::from(20), Value::from(Vec::<i32>::new())],
                vec![Value::from(20), Value::from(vec![400])],
                vec![Value::from(30), Value::Error(PiperError::Unknown("test".to_string()))],
                vec![Value::from(30), Value::from(vec![600])],
                vec![Value::from(40), Value::from(vec![800])],
            ],
        );
        let (schema, rows) = pipeline
            .process(ds, crate::pipeline::ValidationMode::Strict)
            .unwrap()
            .eval()
            .await;
        assert_eq!(schema, pipeline.output_schema);
        println!("pipelines: {}", pipeline.dump());
        println!("{:?}", rows);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0], vec![Value::from(30), Value::Error(PiperError::Unknown("test".to_string()))]);
        assert_eq!(rows[1], vec![Value::from(30), Value::from(vec![600])]);
        assert_eq!(rows[2], vec![Value::from(40), Value::from(vec![800])]);
    }
}