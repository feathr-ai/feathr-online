use async_trait::async_trait;

use crate::pipeline::{Schema, DataSet, Value, PiperError};

use super::Transformation;

#[derive(Clone, Debug)]
pub struct TakeTransformation {
    pub count: usize,
}

impl Transformation for TakeTransformation {
    fn get_output_schema(&self, input_schema: &Schema) -> Schema {
        input_schema.clone()
    }

    fn transform(
        &self,
        dataset: Box<dyn DataSet>,
    ) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(TakeDataSet {
            input: dataset,
            count: self.count,
        }))
    }

    fn dump(&self) -> String {
        format!("take {}", self.count)
    }
}

struct TakeDataSet {
    input: Box<dyn DataSet>,
    count: usize,
}

#[async_trait]
impl DataSet for TakeDataSet {
    fn schema(&self) -> &Schema {
        self.input.schema()
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        if self.count == 0 {
            None
        } else {
            self.count -= 1;
            self.input.next().await
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
            | take 3
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
        assert_eq!(rows[0], vec![Value::from(10), Value::from(vec![1, 2, 3])]);
        assert_eq!(rows[1], vec![Value::from(10), Value::from(Vec::<i32>::new())]);
        assert_eq!(rows[2], vec![Value::from(20), Value::from(Vec::<i32>::new())]);
    }
}