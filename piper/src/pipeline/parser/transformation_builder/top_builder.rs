use crate::pipeline::{
    parser::expression_builders::ExpressionBuilder,
    transformation::{NullPos, SortOrder, TopTransformation}, pipelines::BuildContext,
};

use super::TransformationBuilder;

pub struct TopTransformationBuilder {
    count: usize,
    criteria: Box<dyn ExpressionBuilder>,
    sort_order: Option<SortOrder>,
    null_pos: Option<NullPos>,
}

impl TopTransformationBuilder {
    pub fn create(
        count: usize,
        criteria: Box<dyn ExpressionBuilder>,
        sort_order: Option<SortOrder>,
        null_pos: Option<NullPos>,
    ) -> Box<dyn TransformationBuilder> {
        Box::new(Self {
            count,
            criteria,
            sort_order,
            null_pos,
        })
    }
}

impl TransformationBuilder for TopTransformationBuilder {
    fn build(
        &self,
        input_schema: &crate::pipeline::Schema,
        ctx: &BuildContext,
    ) -> Result<Box<dyn crate::pipeline::transformation::Transformation>, crate::pipeline::PiperError>
    {
        Ok(TopTransformation::new(
            self.count,
            self.criteria.build(input_schema, ctx)?,
            self.sort_order,
            self.null_pos,
        ))
    }
}
