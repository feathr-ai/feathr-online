use std::collections::HashMap;

use crate::pipeline::{transformation::{ProjectRenameTransformation, Transformation}, Schema, PiperError, pipelines::BuildContext};

use super::TransformationBuilder;

pub struct ProjectRenameTransformationBuilder {
    pub renames: HashMap<String, String>,
}

impl ProjectRenameTransformationBuilder {
    pub fn create(renames: Vec<(String, String)>) -> Box<dyn TransformationBuilder> {
        Box::new(Self {
            renames: renames.into_iter().collect(),
        })
    }
}

impl TransformationBuilder for ProjectRenameTransformationBuilder {
    fn build(&self, input_schema: &Schema, _ctx: &BuildContext) -> Result<Box<dyn Transformation>, PiperError> {
        for name in self.renames.values() {
            if input_schema.get_column_index(name).is_some() {
                return Err(PiperError::ColumnAlreadyExists(name.clone()));
            }
        }
        ProjectRenameTransformation::create(input_schema, self.renames.clone())
    }
}
