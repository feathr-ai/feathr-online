use crate::pipeline::{
    transformation::{ProjectKeepTransformation, Transformation},
    PiperError, Schema, pipelines::BuildContext,
};

use super::TransformationBuilder;

pub struct ProjectKeepTransformationBuilder {
    pub keeps: Vec<String>,
}

impl ProjectKeepTransformationBuilder {
    pub fn create(keeps: Vec<String>) -> Box<dyn TransformationBuilder> {
        Box::new(Self { keeps })
    }
}

impl TransformationBuilder for ProjectKeepTransformationBuilder {
    fn build(&self, input_schema: &Schema, _ctx: &BuildContext) -> Result<Box<dyn Transformation>, PiperError> {
        ProjectKeepTransformation::create(input_schema, self.keeps.clone())
    }
}
