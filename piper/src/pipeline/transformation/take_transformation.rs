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
