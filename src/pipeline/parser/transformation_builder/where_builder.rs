use crate::pipeline::{transformation::{Transformation, WhereTransformation}, PiperError, Schema, parser::expression_builders::ExpressionBuilder};

use super::TransformationBuilder;


pub struct WhereTransformationBuilder {
    pub expression: Box<dyn ExpressionBuilder>,
}

impl WhereTransformationBuilder {
    pub fn create(expression: Box<dyn ExpressionBuilder>) -> Box<dyn TransformationBuilder> {
        Box::new(Self { expression })
    }
}

impl TransformationBuilder for WhereTransformationBuilder {
    fn build(&self, input_schema: &Schema) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(Box::new(WhereTransformation {
            predicate: self.expression.build(input_schema)?,
        }))
    }
}