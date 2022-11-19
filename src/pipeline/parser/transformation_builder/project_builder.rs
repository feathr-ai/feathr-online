use crate::pipeline::{
    parser::expression_builders::ExpressionBuilder, transformation::ProjectTransformation,
};

use super::TransformationBuilder;

pub struct ProjectTransformationBuilder {
    pub columns: Vec<(String, Box<dyn ExpressionBuilder>)>,
}

impl ProjectTransformationBuilder {
    pub fn new(
        columns: Vec<(String, Box<dyn ExpressionBuilder>)>,
    ) -> Box<dyn TransformationBuilder> {
        Box::new(Self { columns })
    }
}

impl TransformationBuilder for ProjectTransformationBuilder {
    fn build(
        &self,
        input_schema: &crate::pipeline::Schema,
    ) -> Result<Box<dyn crate::pipeline::transformation::Transformation>, crate::pipeline::PiperError>
    {
        ProjectTransformation::new(
            input_schema,
            self.columns
                .iter()
                .map(|(name, exp)| exp.build(input_schema).map(|e| (name.to_owned(), e)))
                .collect::<Result<Vec<_>, _>>()?,
        )
    }
}
