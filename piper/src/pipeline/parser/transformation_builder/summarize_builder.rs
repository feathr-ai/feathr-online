use crate::{
    pipeline::{
        parser::aggregation_builder::AggregationBuilder,
        parser::expression_builders::ExpressionBuilder,
        transformation::{SummarizeTransformation, Transformation},
        BuildContext, Schema,
    },
    PiperError,
};

use super::TransformationBuilder;

#[derive(Debug)]
pub struct SummarizeTransformationBuilder {
    pub aggregations: Vec<(String, AggregationBuilder)>,
    pub group_by: Vec<(String, Option<Box<dyn ExpressionBuilder>>)>,
}

impl TransformationBuilder for SummarizeTransformationBuilder {
    fn build(
        &self,
        input_schema: &Schema,
        ctx: &BuildContext,
    ) -> Result<Box<dyn Transformation>, PiperError> {
        let aggregations = self
            .aggregations
            .iter()
            .map(|(name, agg)| {
                let agg = agg.build(input_schema, ctx)?;
                Ok((name.clone(), agg))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let keys = self
            .group_by
            .iter()
            .map(|(name, expr)| {
                let expr = expr
                    .as_ref()
                    .map(|e| e.build(input_schema, ctx))
                    .transpose()?;
                Ok((name.clone(), expr))
            })
            .collect::<Result<Vec<_>, _>>()?;
        SummarizeTransformation::create(input_schema, aggregations, keys)
    }
}
