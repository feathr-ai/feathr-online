use std::{collections::VecDeque, sync::Arc};

use async_trait::async_trait;
use tracing::{instrument, trace};

use crate::pipeline::{DataSet, PiperError, Schema, Value, ValueType};

use super::Transformation;

#[derive(Clone, Debug)]
pub struct ExplodeTransformation {
    column_idx: usize,
    exploded_type: ValueType,
    output_schema: Arc<Schema>,
}

impl ExplodeTransformation {
    pub fn create(
        input_schema: &Schema,
        column_idx: usize,
        exploded_type: ValueType,
    ) -> Box<dyn Transformation> {
        let mut output_schema = input_schema.clone();
        output_schema.columns[column_idx].column_type = exploded_type;
        Box::new(Self {
            column_idx,
            exploded_type,
            output_schema: Arc::new(output_schema),
        })
    }
}

impl Transformation for ExplodeTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(ExplodedDataSet {
            input: dataset,
            output_schema: self.output_schema.clone(),
            column_idx: self.column_idx,
            exploded_type: self.exploded_type,
            current_row: None,
            current_exploded_column: Default::default(),
        }))
    }

    fn dump(&self) -> String {
        format!(
            "explode {} as {}",
            self.output_schema.columns[self.column_idx].name, self.exploded_type
        )
    }
}

struct ExplodedDataSet {
    input: Box<dyn DataSet>,
    output_schema: Arc<Schema>,
    column_idx: usize,
    exploded_type: ValueType,
    current_row: Option<Vec<Value>>,
    current_exploded_column: VecDeque<Value>,
}

#[async_trait]
impl DataSet for ExplodedDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    #[instrument(level = "trace", skip(self))]
    async fn next(&mut self) -> Option<Vec<Value>> {
        while self.current_exploded_column.is_empty() {
            trace!("current_exploded_column is empty, fetching next row from upstream");
            match self.get_next_row().await {
                Some(_) => {
                    // We do have a new row, but loop again to check if the array is empty
                    // We should skip such rows
                }
                None => {
                    trace!("Upstream returned None");
                    return None;
                }
            }
        }

        let mut row = match &self.current_row {
            Some(row) => row.clone(),
            None => {
                trace!("Data set is exhausted");
                return None;
            }
        };
        row[self.column_idx] = self
            .current_exploded_column
            .pop_front()
            .unwrap() // This won't fail as `get_next_row` ensures that the deque is not empty
            .cast_to(self.exploded_type);
        Some(row)
    }
}

impl ExplodedDataSet {
    #[instrument(level = "trace", skip(self))]
    async fn get_next_row(&mut self) -> Option<Vec<Value>> {
        while let Some(row) = self.input.next().await {
            trace!("Fetched 1 row from upstream");
            self.current_row = Some(row.clone());
            self.current_exploded_column = match row[self.column_idx].get_array() {
                Ok(array) => array.clone().into_iter().collect(),
                // Keep an error row when the exploded column is not an array so downstream can know what happened
                Err(e) => return Some(vec![Value::Error(e)]),
            };
            trace!(
                "Exploded column has {} elements",
                self.current_exploded_column.len()
            );
            if self.current_exploded_column.is_empty() {
                trace!("Exploded column is empty, fetching next row from upstream");
                continue;
            } else {
                return Some(row);
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::pipeline::{pipelines::BuildContext, DataSetCreator, Value, Pipeline};

    #[tokio::test]
    async fn test_explode() {
        let pipeline = Pipeline::parse(
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
                vec![Value::from(10), Value::from(Vec::<i32>::new())],
                vec![Value::from(20), Value::from(Vec::<i32>::new())],
                vec![Value::from(20), Value::from(vec![400])],
                vec![Value::from(30), Value::from(vec![4, 5, 6])],
                vec![Value::from(30), Value::from(vec![600])],
                vec![Value::from(40), Value::from(Vec::<i32>::new())],
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
        assert_eq!(rows.len(), 9);
        assert_eq!(rows[0], vec![Value::from(10), Value::from(1)]);
        assert_eq!(rows[1], vec![Value::from(10), Value::from(2)]);
        assert_eq!(rows[2], vec![Value::from(10), Value::from(3)]);
        assert_eq!(rows[3], vec![Value::from(20), Value::from(400)]);
        assert_eq!(rows[4], vec![Value::from(30), Value::from(4)]);
        assert_eq!(rows[5], vec![Value::from(30), Value::from(5)]);
        assert_eq!(rows[6], vec![Value::from(30), Value::from(6)]);
        assert_eq!(rows[7], vec![Value::from(30), Value::from(600)]);
        assert_eq!(rows[8], vec![Value::from(40), Value::from(800)]);
    }
}