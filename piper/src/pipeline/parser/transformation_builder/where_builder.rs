use crate::pipeline::{
    parser::expression_builders::ExpressionBuilder,
    transformation::{Transformation, WhereTransformation},
    PiperError, Schema, pipelines::BuildContext,
};

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
    fn build(&self, input_schema: &Schema, ctx: &BuildContext) -> Result<Box<dyn Transformation>, PiperError> {
        Ok(Box::new(WhereTransformation {
            predicate: self.expression.build(input_schema, ctx)?.into(),
        }))
    }
}
