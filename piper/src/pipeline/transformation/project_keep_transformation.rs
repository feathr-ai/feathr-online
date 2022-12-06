use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;

use crate::pipeline::{DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Debug)]
pub struct ProjectKeepTransformation {
    output_schema: Arc<Schema>,
    kept_columns: Vec<String>,
    keep_set: HashSet<usize>,
}

impl ProjectKeepTransformation {
    pub fn create(
        input_schema: &Schema,
        kept_columns: Vec<String>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let mut keep_set = HashSet::new();
        let mut columns = vec![];
        for column in &kept_columns {
            let index = input_schema
                .get_column_index(column)
                .ok_or_else(|| PiperError::ColumnNotFound(column.to_string()))?;
            keep_set.insert(index);
        }
        let mut indices: Vec<usize> = keep_set.iter().copied().collect();
        indices.sort();
        for index in indices {
            columns.push(input_schema.columns[index].clone());
        }
        Ok(Box::new(ProjectKeepTransformation {
            output_schema: Arc::new(Schema::from(columns)),
            kept_columns,
            keep_set,
        }))
    }
}

impl Transformation for ProjectKeepTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(ProjectKeepDataSet {
            input: dataset,
            output_schema: self.output_schema.clone(),
            keep_set: self.keep_set.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!("project-keep {}", self.kept_columns.join(", "))
    }
}

struct ProjectKeepDataSet {
    input: Box<dyn DataSet>,
    output_schema: Arc<Schema>,
    keep_set: HashSet<usize>,
}

#[async_trait]
impl DataSet for ProjectKeepDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    /**
     * Get the next row of the data set, returns None if there is no more row
     */
    async fn next(&mut self) -> Option<Vec<Value>> {
        let row = self.input.next().await?;
        Some(
            row.into_iter()
                .enumerate()
                .filter(|(idx, _)| self.keep_set.contains(idx))
                .map(|(_, value)| value)
                .collect(),
        )
    }
}
