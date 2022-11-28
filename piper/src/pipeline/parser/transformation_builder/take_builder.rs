use crate::pipeline::{Schema, transformation::{Transformation, TakeTransformation}, PiperError, pipelines::BuildContext};

use super::TransformationBuilder;

pub struct TakeTransformationBuilder {
    pub count: usize,
}

impl TakeTransformationBuilder {
    pub fn create(count: usize) -> Box<dyn TransformationBuilder> {
        Box::new(Self { count })
    }
}

impl TransformationBuilder for TakeTransformationBuilder {
    fn build(&self, _input_schema: &Schema, _ctx: &BuildContext) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(Box::new(TakeTransformation {
            count: self.count,
        }))
    }
}
