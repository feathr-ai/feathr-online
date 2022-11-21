use crate::pipeline::{transformation::{Transformation, IgnoreErrorTransformation}, PiperError, Schema};

use super::TransformationBuilder;

pub struct IgnoreErrorTransformationBuilder;

impl TransformationBuilder for IgnoreErrorTransformationBuilder {
    fn build(&self, _input_schema: &Schema) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(Box::new(IgnoreErrorTransformation))
    }
}

impl IgnoreErrorTransformationBuilder {
    pub fn create() -> Box<dyn TransformationBuilder> {
        Box::new(Self)
    }
}
