use std::{collections::HashSet, sync::Arc};

use async_trait::async_trait;

use crate::pipeline::{DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Clone, Debug)]
pub struct ProjectRemoveTransformation {
    output_schema: Arc<Schema>,
    removed_columns: Vec<String>,
    remove_set: HashSet<usize>,
}

impl ProjectRemoveTransformation {
    pub fn create(
        input_schema: &Schema,
        columns: Vec<String>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let output_schema = Arc::new(input_schema
            .columns
            .iter()
            .filter(|c| !columns.contains(&c.name))
            .cloned()
            .collect());
        let remove_set = input_schema
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| columns.contains(&c.name))
            .map(|(i, _)| i)
            .collect();
        Ok(Box::new(Self {
            output_schema,
            removed_columns: columns,
            remove_set,
        }))
    }
}

impl Transformation for ProjectRemoveTransformation {
    fn get_output_schema(&self, _input_schema: &Schema) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(&self, dataset: Box<dyn DataSet>) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(ProjectRemovedDataSet {
            input: dataset,
            output_schema: self.output_schema.clone(),
            remove_set: self.remove_set.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!("project-remove {}", self.removed_columns.join(", "))
    }
}

struct ProjectRemovedDataSet {
    output_schema: Arc<Schema>,
    input: Box<dyn DataSet>,
    remove_set: HashSet<usize>,
}

#[async_trait]
impl DataSet for ProjectRemovedDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }

    async fn next(&mut self) -> Option<Vec<Value>> {
        self.input.next().await.map(|row| {
            row.into_iter()
                .enumerate()
                .filter(|(i, _)| !self.remove_set.contains(i))
                .map(|(_, v)| v)
                .collect()
        })
    }
}
