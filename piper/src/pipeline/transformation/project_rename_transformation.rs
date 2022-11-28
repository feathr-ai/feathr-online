use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

use crate::pipeline::{Column, DataSet, PiperError, Schema, Value};

use super::Transformation;

#[derive(Debug, Clone)]
pub struct ProjectRenameTransformation {
    output_schema: Arc<Schema>,
    renames: HashMap<String, String>,
}

impl ProjectRenameTransformation {
    pub fn create(
        input_schema: &Schema,
        renames: HashMap<String, String>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(Box::new(Self {
            output_schema: Arc::new(input_schema
                .columns
                .iter()
                .map(|c| Column {
                    name: renames.get(&c.name).unwrap_or(&c.name).to_owned(),
                    column_type: c.column_type,
                })
                .collect()),
            renames,
        }))
    }
}

impl Transformation for ProjectRenameTransformation {
    fn get_output_schema(
        &self,
        _input_schema: &Schema,
    ) -> Schema {
        self.output_schema.as_ref().clone()
    }

    fn transform(
        &self,
        dataset: Box<dyn DataSet>,
    ) -> Result<Box<dyn DataSet>, PiperError> {
        Ok(Box::new(ProjectRenamedDataSet {
            input: dataset,
            output_schema: self.output_schema.clone(),
        }))
    }

    fn dump(&self) -> String {
        format!(
            "project-rename {}",
            self.renames
                .iter()
                .map(|(old, new)| format!("{} = {}", old, new))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

struct ProjectRenamedDataSet {
    output_schema: Arc<Schema>,
    input: Box<dyn DataSet>,
}

#[async_trait]
impl DataSet for ProjectRenamedDataSet {
    fn schema(&self) -> &Schema {
        &self.output_schema
    }
    async fn next(&mut self) -> Option<Vec<Value>> {
        self.input.next().await
    }
}
