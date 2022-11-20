use std::collections::VecDeque;

use async_trait::async_trait;
use tracing::{debug, instrument};

use crate::pipeline::{DataSet, PiperError, Schema, Value, ValueType};

use super::Transformation;

#[derive(Clone, Debug)]
pub struct ExplodeTransformation {
    column_idx: usize,
    exploded_type: ValueType,
    output_schema: Schema,
}

impl ExplodeTransformation {
    pub fn new(
        input_schema: &Schema,
        column_idx: usize,
        exploded_type: ValueType,
    ) -> Box<dyn Transformation> {
        let mut output_schema = input_schema.clone();
        output_schema.columns[column_idx].column_type = exploded_type;
        Box::new(Self {
            column_idx,
            exploded_type,
            output_schema,
        })
    }
}

impl Transformation for ExplodeTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.clone()
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
    output_schema: Schema,
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
            debug!("current_exploded_column is empty, fetching next row from upstream");
            match self.get_next_row().await {
                Some(_) => {
                    // We do have a new row, but loop again to check if the array is empty
                    // We should skip such rows
                }
                None => {
                    debug!("Upstream returned None");
                    return None;
                }
            }
        }

        if self.current_row.is_none() {
            debug!("Data set is exhausted");
            return None;
        }
        let mut row = self.current_row.clone().unwrap();
        match self
            .current_exploded_column
            .pop_front()
            .unwrap()
            .try_into(self.exploded_type)
        {
            Ok(v) => {
                row[self.column_idx] = v;
                Some(row)
            }
            Err(e) => {
                row[self.column_idx] = e.into();
                Some(row)
            }
        }
    }
}

impl ExplodedDataSet {
    #[instrument(level = "trace", skip(self))]
    async fn get_next_row(&mut self) -> Option<Vec<Value>> {
        if let Some(row) = self.input.next().await {
            debug!("Fetched 1 row from upstream");
            self.current_row = Some(row.clone());
            self.current_exploded_column = match row[self.column_idx].get_array() {
                Ok(array) => array.clone().into_iter().collect(),
                Err(e) => return Some(vec![Value::Error(e); self.output_schema.columns.len()]),
            };
            debug!(
                "Exploded column has {} elements",
                self.current_exploded_column.len()
            );
            Some(row)
        } else {
            return None;
        }
    }
}
