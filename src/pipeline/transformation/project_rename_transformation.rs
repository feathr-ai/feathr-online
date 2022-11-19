use std::collections::HashMap;

use async_trait::async_trait;

use crate::pipeline::{Column, DataSet, PiperError, Value};

use super::Transformation;

#[derive(Debug, Clone)]
pub struct ProjectRenameTransformation {
    output_schema: crate::pipeline::Schema,
    renames: HashMap<String, String>,
}

impl ProjectRenameTransformation {
    pub fn new(
        input_schema: &crate::pipeline::Schema,
        renames: HashMap<String, String>,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(Box::new(Self {
            output_schema: input_schema
                .columns
                .iter()
                .map(|c| Column {
                    name: renames.get(&c.name).unwrap_or(&c.name).to_owned(),
                    column_type: c.column_type,
                })
                .collect(),
            renames,
        }))
    }
}

impl Transformation for ProjectRenameTransformation {
    fn get_output_schema(
        &self,
        _input_schema: &crate::pipeline::Schema,
    ) -> crate::pipeline::Schema {
        self.output_schema.clone()
    }

    fn transform(
        &self,
        dataset: Box<dyn crate::pipeline::DataSet>,
    ) -> Result<Box<dyn crate::pipeline::DataSet>, PiperError> {
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
    output_schema: crate::pipeline::Schema,
    input: Box<dyn crate::pipeline::DataSet>,
}

#[async_trait]
impl DataSet for ProjectRenamedDataSet {
    fn schema(&self) -> &crate::pipeline::Schema {
        &self.output_schema
    }
    async fn next(&mut self) -> Option<Result<Vec<Value>, PiperError>> {
        self.input.next().await
    }
}
