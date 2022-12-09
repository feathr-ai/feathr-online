use crate::{
    pipeline::{aggregation::Aggregation, BuildContext, Schema},
    PiperError,
};

use super::expression_builders::ExpressionBuilder;

#[derive(Debug)]
pub struct AggregationBuilder {
    pub aggregation_name: String,
    pub aggregation_args: Vec<Box<dyn ExpressionBuilder>>,
}

impl AggregationBuilder {
    pub fn build(&self, schema: &Schema, ctx: &BuildContext) -> Result<Aggregation, PiperError> {
        let agg = ctx
            .agg_functions
            .get(&self.aggregation_name)
            .ok_or_else(|| PiperError::UnknownFunction(self.aggregation_name.clone()))?;
        let args = self
            .aggregation_args
            .iter()
            .map(|e| e.build(schema, ctx))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Aggregation::new(agg.clone(), args))
    }
}
