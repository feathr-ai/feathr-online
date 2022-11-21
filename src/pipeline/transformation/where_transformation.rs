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
