use async_trait::async_trait;

use crate::pipeline::{Value, PiperError, DataSet, Schema};

use super::Transformation;

#[derive(Debug)]
pub struct IgnoreErrorTransformation;

impl Transformation for IgnoreErrorTransformation {
    fn get_output_schema(&self, input_schema: &Schema) -> Schema {
        input_schema.clone()
    }

    fn transform(
        &self,
        dataset: Box<dyn DataSet>,
    ) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(IgnoreErrorDataSet {
            input: dataset,
        }))
    }

    fn dump(&self) -> String {
        "ignore-error".to_string()
    }
}

struct IgnoreErrorDataSet {
    input: Box<dyn DataSet>,
}

#[async_trait]
impl DataSet for IgnoreErrorDataSet {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        loop {
            match self.input.next().await {
                Some(row) => {
                    let mut has_error = false;
                    for v in &row {
                        if v.is_error() {
                            has_error = true;
                            break;
                        }
                    }
                    if has_error {
                        continue;
                    } else {
                        return Some(row);
                    }
                }
                None => return None,
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
            | ignore-error
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
                vec![Value::Error(PiperError::Unknown("test".to_string())), Value::from(Vec::<i32>::new())],
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
        assert_eq!(rows.len(), 6);
        assert_eq!(rows[0], vec![Value::from(10), Value::from(vec![1, 2, 3])]);
        assert_eq!(rows[1], vec![Value::from(10), Value::from(Vec::<i32>::new())]);
        assert_eq!(rows[2], vec![Value::from(20), Value::from(Vec::<i32>::new())]);
        assert_eq!(rows[3], vec![Value::from(20), Value::from(vec![400])]);
        assert_eq!(rows[4], vec![Value::from(30), Value::from(vec![600])]);
        assert_eq!(rows[5], vec![Value::from(40), Value::from(vec![800])]);
    }
}