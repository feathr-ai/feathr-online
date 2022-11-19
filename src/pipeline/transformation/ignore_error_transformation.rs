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
        "ignore_error".to_string()
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

    async fn next(&mut self) -> Option<Result<Vec<Value>, PiperError>> {
        loop {
            match self.input.next().await {
                Some(Ok(row)) => return Some(Ok(row)),
                Some(Err(_)) => continue,
                None => return None,
            }
        }
        
    }
}