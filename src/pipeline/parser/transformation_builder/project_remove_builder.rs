use crate::pipeline::{
    transformation::{ProjectRemoveTransformation, Transformation},
    PiperError, Schema,
};

use super::TransformationBuilder;

pub struct ProjectRemoveTransformationBuilder {
    pub removes: Vec<String>,
}

impl ProjectRemoveTransformationBuilder {
    pub fn new(removes: Vec<String>) -> Box<dyn TransformationBuilder> {
        Box::new(Self { removes })
    }
}

impl TransformationBuilder for ProjectRemoveTransformationBuilder {
    fn build(&self, input_schema: &Schema) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(ProjectRemoveTransformation::new(
            input_schema,
            self.removes.clone(),
        )?)
    }
}
