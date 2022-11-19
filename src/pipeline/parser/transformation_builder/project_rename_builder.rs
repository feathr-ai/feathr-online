use std::collections::HashMap;

use crate::pipeline::{transformation::{ProjectRenameTransformation, Transformation}, Schema, PiperError};

use super::TransformationBuilder;

pub struct ProjectRenameTransformationBuilder {
    pub renames: HashMap<String, String>,
}

impl ProjectRenameTransformationBuilder {
    pub fn new(renames: Vec<(String, String)>) -> Box<dyn TransformationBuilder> {
        Box::new(Self {
            renames: renames.into_iter().collect(),
        })
    }
}

impl TransformationBuilder for ProjectRenameTransformationBuilder {
    fn build(&self, input_schema: &Schema) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(ProjectRenameTransformation::new(input_schema, self.renames.clone())?)
    }
}
