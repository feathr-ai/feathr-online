use std::collections::HashSet;

use async_trait::async_trait;

use crate::pipeline::{DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Debug)]
pub struct ProjectKeepTransformation {
    output_schema: Schema,
    kept_columns: Vec<String>,
    keep_set: HashSet<usize>,
}

impl ProjectKeepTransformation {
    pub fn create(input_schema: &Schema, kept_columns: Vec<String>) -> Box<dyn Transformation> {
        let mut keep_set = HashSet::new();
        let mut columns = vec![];
        for column in &kept_columns {
            let index = input_schema.get_column_index(column).unwrap();
            keep_set.insert(index);
            columns.push(input_schema.columns[index].clone());
        }
        Box::new(ProjectKeepTransformation {
            output_schema: Schema::from(columns),
            kept_columns,
            keep_set,
        })
    }
}

impl Transformation for ProjectKeepTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.clone()
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
    output_schema: Schema,
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
                .collect()
        )
    }
}
