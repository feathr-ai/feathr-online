use crate::pipeline::{
    parser::expression_builders::ExpressionBuilder,
    pipelines::BuildContext,
    transformation::{ProjectTransformation, Transformation},
    PiperError, Schema,
};

use super::TransformationBuilder;

pub struct ProjectTransformationBuilder {
    pub columns: Vec<(String, Box<dyn ExpressionBuilder>)>,
}

impl ProjectTransformationBuilder {
    pub fn create(
        columns: Vec<(String, Box<dyn ExpressionBuilder>)>,
    ) -> Box<dyn TransformationBuilder> {
        Box::new(Self { columns })
    }
}

impl TransformationBuilder for ProjectTransformationBuilder {
    fn build(
        &self,
        input_schema: &Schema,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        ProjectTransformation::create(
            input_schema,
            self.columns
                .iter()
                .map(|(name, exp)| exp.build(input_schema, ctx).map(|e| (name.to_owned(), e)))
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}
